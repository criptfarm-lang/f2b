"""
Планировщик задач бота F2B PRO
- Утренняя сводка в 9:00
- Напоминание о дедлайнах в 10:00
- Стареющие клиенты в 12:00
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from telegram.ext import Application
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger

from claude_ai import generate_morning_summary

logger = logging.getLogger(__name__)

MSK = ZoneInfo("Europe/Moscow")

def get_group_ids():
    raw = os.getenv("GROUP_CHAT_IDS", "")
    return [int(x.strip()) for x in raw.split(",") if x.strip()]

def get_group_chat_id():
    val = os.getenv("GROUP_CHAT_ID", "")
    return int(val) if val else None

def setup_scheduler(app: Application, db):
    """Настраивает и запускает все запланированные задачи.

    ВАЖНО: у каждого CronTrigger timezone задан явно. AsyncIOScheduler(timezone=...)
    на CronTrigger без явной таймзоны НЕ распространяется — он берёт локальную TZ
    контейнера (UTC на Railway), и часы съезжают на +3.
    """
    scheduler = AsyncIOScheduler(timezone=MSK)

    # 09:00 МСК — утренняя сводка
    scheduler.add_job(
        morning_summary,
        CronTrigger(hour=9, minute=0, timezone=MSK),
        args=[app, db],
        id="morning_summary"
    )

    # 10:00 МСК — напоминание о задачах на сегодня
    scheduler.add_job(
        remind_today_tasks,
        CronTrigger(hour=10, minute=0, timezone=MSK),
        args=[app, db],
        id="remind_today_tasks"
    )

    # 12:00 МСК — проверка стареющих клиентов
    scheduler.add_job(
        check_aging_clients,
        CronTrigger(hour=12, minute=0, timezone=MSK),
        args=[app],
        id="aging_clients"
    )

    # 03:00 МСК — очистка старых задач
    scheduler.add_job(
        cleanup_done_tasks,
        CronTrigger(hour=3, minute=0, timezone=MSK),
        id="cleanup_done_tasks"
    )

    # 02:00 МСК — синхронизация менеджеров в wazzup_contact_map
    scheduler.add_job(
        sync_managers_job,
        CronTrigger(hour=2, minute=0, timezone=MSK),
        args=[app],
        id="sync_managers"
    )

    # Светофор техопераций (каждые 30 мин) — ПЕРЕНЕСЁН в PTB JobQueue (bot.py,
    # _processing_svetofor_wrapper) 2026-07-03. В этом AsyncIOScheduler джоба ни
    # разу не исполнялась: interval-тик дропался (тот же баг, что market_intel/
    # payment_planned, вынесенные в JobQueue ранее). План: светофор-техопераций-эф.

    # 13:55 и 14:00 МСК — снимок состояния заказов для ПДЗ-автоматики
    # (план 2026-05-20, Фаза 2). Два запуска: до банк-cut-off (13:55) и
    # после разнесения банка (14:00). Логика срывов сравнивает 14:00-снимки.
    # misfire_grace_time=3600 + coalesce=True: если scheduler работал, но не
    # успел дёрнуть job (тяжёлая очередь и т.п.), tick догонится в течение часа.
    # Если же контейнер был перезапущен ПОСЛЕ tick'а — misfire не помогает
    # (AsyncIOScheduler без persistent jobstore не знает о пропуске). Этот
    # сценарий покрыт pdz_catch_up_missed_jobs(), вызывается из bot.py при старте.
    # Сдвиг всей цепочки на час раньше (решение собственника 2026-05-26):
    # digest менеджерам в 13:00 МСК (вместо 14:10), дедлайн менеджерам 15:00
    # (вместо 16:00), пинг собственнику 15:05 (вместо 16:05). Регламент банка
    # — разноска до 12:45 МСК (переподтвердить с Малышкиным).
    scheduler.add_job(
        _pdz_run_and_record,
        CronTrigger(hour=12, minute=45, timezone=MSK),
        args=["pdz_snapshot_1245", pdz_take_snapshot_job, app, db],
        id="pdz_snapshot_1245",
        misfire_grace_time=3600, coalesce=True,
    )
    scheduler.add_job(
        _pdz_run_and_record,
        CronTrigger(hour=12, minute=50, timezone=MSK),
        args=["pdz_snapshot_1250", pdz_take_snapshot_job, app, db],
        id="pdz_snapshot_1250",
        misfire_grace_time=3600, coalesce=True,
    )

    # 12:52 МСК — обработка событий обещаний (Фаза 3). Между snapshot 12:50
    # и дайджестом менеджерам 13:00. Сравнивает сегодняшний и вчерашний
    # snapshot, эскалирует по неплатежам и шлёт TG-алерт собственнику
    # при изменении ppm_initial («Дата планируемой оплаты», менять нельзя).
    scheduler.add_job(
        _pdz_run_and_record,
        CronTrigger(hour=12, minute=52, timezone=MSK),
        args=["pdz_process_events_1252", pdz_process_events_job, app, db],
        id="pdz_process_events_1252",
        misfire_grace_time=3600, coalesce=True,
    )

    # 13:00 МСК — TG-дайджест каждому менеджеру с просрочками (Фаза 4).
    # Если просрочек нет — тишина.
    scheduler.add_job(
        _pdz_run_and_record,
        CronTrigger(hour=13, minute=0, timezone=MSK),
        args=["pdz_send_digests_1300", pdz_send_digests_job, app, db],
        id="pdz_send_digests_1300",
        misfire_grace_time=3600, coalesce=True,
    )

    # 15:05 МСК — пинг собственнику по необработанным после дедлайна 15:00 (Фаза 4).
    scheduler.add_job(
        _pdz_run_and_record,
        CronTrigger(hour=15, minute=5, timezone=MSK),
        args=["pdz_send_owner_pending_1505", pdz_send_owner_pending_job, app, db],
        id="pdz_send_owner_pending_1505",
        misfire_grace_time=3600, coalesce=True,
    )

    # 13:05 МСК — регенерация HTML-отчёта «Дебиторка» (Фаза 5).
    # Идёт после 12:52 (эскалация) и 13:00 (дайджесты
    # менеджерам). Источник — БД (pdz_snapshots + pdz_payment_state), МС API не
    # дёргается. Шлёт собственнику ссылку с токеном TTL 24ч.
    scheduler.add_job(
        _pdz_run_and_record,
        CronTrigger(hour=13, minute=5, timezone=MSK),
        args=["pdz_generate_html_1305", pdz_generate_html_job, app, db],
        id="pdz_generate_html_1305",
        misfire_grace_time=3600, coalesce=True,
    )

    # 16:00 МСК — сводка по листу контроля дебиторки собственнику в личку
    # (план 2026-09-09). После 15:05 owner_pending, чтобы не сталкиваться с
    # ПДЗ-потоком, и до конца банковского дня 16:00 — приходы за день уже видны.
    scheduler.add_job(
        control_list_daily_job,
        CronTrigger(hour=16, minute=0, timezone=MSK),
        args=[app, db],
        id="control_list_daily_1600",
        misfire_grace_time=3600, coalesce=True,
    )

    # ПТ 08:00 МСК — пересчёт «% работы на новых клиентах» (MTD) и запись
    # снимка в bot_settings.op_new_share_snapshot. Виджет в /op_report читает
    # его независимо от report_cache. План:
    # 2026-05-21-виджет-процент-новых-в-отчете-оп.md, Фаза 5.
    scheduler.add_job(
        op_new_share_snapshot_job,
        CronTrigger(day_of_week='fri', hour=8, minute=0, timezone=MSK),
        args=[app, db],
        id="op_new_share_snapshot_fri_08"
    )

    # ПН 09:00 МСК — еженедельная прайс-рассылка DashaMail.
    # План: 2026-06-22-email-pipeline-fix, Фаза C.2. Скрипт через DashaMail
    # API копирует последнюю SENT прайс-кампанию, патчит PDF/href/дату/subject,
    # шлёт Виктору TG-сообщение с inline-кнопкой «Запланировать» которая идёт
    # в handle_dashamail_callback (bot.py).
    scheduler.add_job(
        dashamail_weekly_send_job,
        CronTrigger(day_of_week='mon', hour=9, minute=0, timezone=MSK),
        args=[app, db],
        id="dashamail_weekly_send_mon_09",
        misfire_grace_time=3600, coalesce=True,
    )

    # ПН 09:30 МСК — еженедельная сводка по рекламе Я.Директ в ЛС собственнику.
    # План: 2026-08-10-еженедельная-сводка-директа-в-боте.md, Фаза 4.
    # 09:30, а не 09:00 — в 09:00 уже уходит dashamail_weekly_send_job, две
    # рассылки в одну минуту сливаются в кашу.
    scheduler.add_job(
        direct_weekly_report_job,
        CronTrigger(day_of_week='mon', hour=9, minute=30, timezone=MSK),
        args=[app],
        id="direct_weekly_report_mon_0930",
        misfire_grace_time=3600, coalesce=True,
    )

    # Автоподстановка «Дата планируемой оплаты» вынесена в PTB JobQueue в bot.py
    # (16.06.2026): AsyncIOScheduler пропускал tick 15:56 МСК даже при next_run_time
    # в job-листинге — тот же паттерн, что с market_intel 28-29.05.

    # Каждые 4 ч — warm-up кэша отчёта ОП (TTL в БД 5 ч). Без прогрева первый
    # /op_report после простоя падал в 504 «upstream request timeout» от Amvera
    # ingress: handle_web_report пытался синхронно собрать данные из МС+amoCRM
    # и не укладывался в таймаут. Первый прогон через 60 с после старта —
    # «горячий» кэш после деплоя.
    scheduler.add_job(
        refresh_op_report_cache_job,
        IntervalTrigger(hours=4, timezone=MSK),
        args=[app, db],
        id="refresh_op_report_cache",
        next_run_time=datetime.now(MSK) + timedelta(seconds=60),
        misfire_grace_time=3600, coalesce=True,
    )

    # Каждые 30 мин — обработка прайс-листов из канала «Мониторинг».
    # ВАЖНО (29.05): market_intel вынесен из AsyncIOScheduler в PTB JobQueue
    # (см. bot.py app.job_queue.run_repeating). Причина: AsyncIOScheduler
    # 28.05 завис на tick 18:04 (job больше не триггерился 15 часов),
    # 29.05 после рестарта повторил то же — выполнил 1 tick и забыл job.
    # CronTrigger вариант (hour='9-19', minute='*/30') ранее тоже не дёргал
    # 12:00-16:30 МСК. PTB JobQueue работает стабильно (retry_pending_idents
    # каждый час подтверждён). market_intel_cron_job импортируется и вызывается
    # из bot.py wrapper'а.

    # 2026-07-08 10:00 МСК — разовое напоминание собственнику: контроль Я.Директ
    # после перенастройки РК 01.07 (план f2b-second-brain/plans/2026-07-01-
    # яндекс-директ-перенастройка-рк.md). DateTrigger + guard: если дата уже
    # прошла (перезапуск контейнера после 08.07) — джоб не добавляем.
    _direct_reminder_dt = datetime(2026, 7, 8, 10, 0, tzinfo=MSK)
    if _direct_reminder_dt > datetime.now(MSK):
        scheduler.add_job(
            direct_check_reminder,
            DateTrigger(run_date=_direct_reminder_dt),
            args=[app],
            id="direct_check_reminder_20260708",
            misfire_grace_time=3600, coalesce=True,
        )

    # ПН 05:00 МСК — недельный светофор надёжности контрагентов (отгрузки за 3 мес)
    # План: f2b-second-brain/plans/2026-07-29-svetofor-nadezhnosti-kontragenta.md (Фаза 3)
    scheduler.add_job(
        counterparty_svetofor_weekly,
        CronTrigger(day_of_week='mon', hour=5, minute=0, timezone=MSK),
        args=[app, db],
        id="counterparty_svetofor_weekly",
        misfire_grace_time=3600, coalesce=True,
    )

    # ВС 15:00 МСК — напоминание менеджерам ОП изучить новые вопросы FISHки недели.
    # Слать в ЛС каждому из 5 менеджеров (PDZ_MANAGER_TG_IDS — тот же реестр состава,
    # что у ПДЗ-дайджестов). Воскресенье — день ротации недели квиза, набор уже активен.
    # План: f2b-second-brain/plans/2026-07-30-fishki-воскресное-напоминание-менеджерам.md
    scheduler.add_job(
        fishki_reminder_job,
        CronTrigger(day_of_week='sun', hour=15, minute=0, timezone=MSK),
        args=[app, db],
        id="fishki_reminder_sun_15",
        misfire_grace_time=3600, coalesce=True,
    )

    scheduler.start()
    logger.info("✅ Планировщик запущен")
    for job in scheduler.get_jobs():
        nxt = job.next_run_time.astimezone(MSK).strftime("%Y-%m-%d %H:%M %Z") if job.next_run_time else "?"
        logger.info(f"  job={job.id} next_run={nxt}")

async def fishki_reminder_job(app: Application, db) -> dict:
    """ВС 15:00 МСК — напоминание изучить новые вопросы FISHки недели.

    Получатели в ЛС:
      - 5 менеджеров ОП (PDZ_MANAGER_TG_IDS; fallback — manager_chats по имени).
        Текст со ссылкой на блок дашборда «FISHки → изучить вопросы».
      - 2 закупщика (request_handler.ASSIGNEE_TG belyakova/kristina). У них нет
        менеджерского дашборда, поэтому вариант текста без него — только learn-ссылка.
      - собственник (OWNER_CHAT_ID) + партнёр-логистика (PARTNER_CHAT_ID), env,
        тот же текст без дашборда.
    Возвращает сводку {получатель: "sent"|"no_chat_id"|"error"} для теста."""
    from moysklad import PDZ_MANAGER_TAG_MAP, PDZ_MANAGER_TG_IDS
    from request_handler import ASSIGNEE_TG

    text_managers = (
        "Воскресное напоминание. С сегодняшнего дня в FISHки новый набор "
        "вопросов на неделю. Загляните в свой дашборд → блок "
        "«FISHки → изучить вопросы», чтобы знать, что на этой неделе "
        "спрашивают у клиентов. Прямой просмотр: https://fishki.f2b.group/?learn=1"
    )
    text_buyers = (
        "Воскресное напоминание. С сегодняшнего дня в FISHки новый набор "
        "вопросов на неделю. Полистай, чтобы быть в теме, что на этой неделе "
        "спрашивают у клиентов: https://fishki.f2b.group/?learn=1"
    )

    logger.info(f"fishki_reminder_job стартовала в {datetime.now(MSK):%Y-%m-%d %H:%M %Z}")
    report: dict = {}

    async def _send(key: str, chat_id, text: str):
        if not chat_id:
            logger.warning(f"fishki_reminder_job: {key} — chat_id не найден")
            report[key] = "no_chat_id"
            return
        try:
            await app.bot.send_message(
                chat_id=chat_id, text=text, disable_web_page_preview=True,
            )
            report[key] = "sent"
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.error(f"fishki_reminder_job: {key} send_message → {chat_id}: {e}")
            report[key] = "error"

    # Менеджеры ОП
    for tag, manager_name in PDZ_MANAGER_TAG_MAP.items():
        chat_id = PDZ_MANAGER_TG_IDS.get(tag)
        if not chat_id:
            first_name = manager_name.split()[0] if manager_name else ""
            chat_id = db.get_manager_chat_id(first_name) if first_name else None
        await _send(tag, chat_id, text_managers)

    # Закупщики (Виктор в ASSIGNEE_TG — это собственник, отправляем ниже отдельно)
    for buyer in ("belyakova", "kristina"):
        await _send(buyer, ASSIGNEE_TG.get(buyer), text_buyers)

    # Собственник + партнёр (логистика) — тот же текст без дашборда. id из env.
    _owner = os.getenv("OWNER_CHAT_ID")
    _partner = os.getenv("PARTNER_CHAT_ID")
    await _send("viktor", int(_owner) if _owner else None, text_buyers)
    await _send("malanchuk", int(_partner) if _partner else None, text_buyers)

    logger.info(f"fishki_reminder_job завершена: {report}")
    return report


async def counterparty_svetofor_weekly(app: Application, db):
    """Недельный прогон светофора надёжности по всем контрагентам с отгрузкой за 3 мес."""
    try:
        from counterparty_svetofor import weekly_batch_job
        await weekly_batch_job(app, db)
    except Exception as e:
        logger.error(f"counterparty_svetofor_weekly → {e}")


async def direct_check_reminder(app: Application):
    """Разовое напоминание собственнику о контроле Я.Директ (08.07.2026 10:00 МСК)."""
    owner_raw = os.getenv("OWNER_CHAT_ID")
    if not owner_raw:
        logger.warning("direct_check_reminder: OWNER_CHAT_ID не задан, пропуск")
        return
    text = (
        "Напоминание: контроль Я.Директ после перенастройки 01.07.\n\n"
        "В Claude Code скажи «запусти директ анализ». Проверить:\n"
        "– CPA ХоРеКа (тянуть к 1000–1500, стартовали с 2000)\n"
        "– объём ХоРеКа после отсечки выходных — не задушило ли\n"
        "– какие из 4 объявлений на кампанию выиграли (слабые отключить)\n"
        "– общий баланс и расход"
    )
    try:
        await app.bot.send_message(chat_id=int(owner_raw), text=text)
        logger.info("direct_check_reminder: отправлено собственнику")
    except Exception as e:
        logger.error(f"direct_check_reminder: ошибка отправки: {e}")


async def morning_summary(app: Application, db):
    """Отправляет утреннюю сводку в группы."""
    logger.info(f"morning_summary стартовала в {datetime.now(MSK):%Y-%m-%d %H:%M %Z}")
    group_ids = get_group_ids()
    if not group_ids:
        return

    tasks_today = db.get_tasks_due_today()
    tasks_overdue = db.get_overdue_tasks()

    if not tasks_today and not tasks_overdue:
        return

    text = await generate_morning_summary(tasks_today, tasks_overdue)

    for chat_id in group_ids:
        try:
            await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
            logger.info(f"Утренняя сводка отправлена в {chat_id}")
        except Exception as e:
            logger.error(f"Не удалось отправить сводку в {chat_id}: {e}")

async def remind_today_tasks(app: Application, db):
    """Напоминает о задачах на сегодня."""
    logger.info(f"remind_today_tasks стартовала в {datetime.now(MSK):%Y-%m-%d %H:%M %Z}")
    group_ids = get_group_ids()
    tasks = db.get_tasks_due_today()

    if not tasks or not group_ids:
        return

    lines = ["⏰ *Напоминание: задачи на сегодня*\n"]
    for t in tasks:
        exe = t.get('executor', 'Команда')
        lines.append(f"• *{exe}*: {t['text']}")

    text = "\n".join(lines)
    for chat_id in group_ids:
        try:
            await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Ошибка напоминания в {chat_id}: {e}")

def cleanup_done_tasks():
    """Удаляет выполненные задачи старше 24 часов."""
    logger.info(f"cleanup_done_tasks стартовала в {datetime.now(MSK):%Y-%m-%d %H:%M %Z}")
    try:
        from database import Database
        db = Database()
        db.cleanup_done_tasks()
        logger.info("cleanup_done_tasks: старые выполненные задачи удалены")
    except Exception as e:
        logger.error(f"cleanup_done_tasks: {e}")

async def sync_managers_job(app: Application):
    """Ночная синхронизация менеджеров в wazzup_contact_map."""
    logger.info(f"sync_managers_job стартовала в {datetime.now(MSK):%Y-%m-%d %H:%M %Z}")
    try:
        from bot import sync_contact_managers
        updated = await sync_contact_managers()
        logger.info(f"sync_managers_job: обновлено {updated} контактов")
    except Exception as e:
        logger.error(f"sync_managers_job: {e}", exc_info=True)

async def check_aging_clients(app: Application):
    """Ежедневно в 12:00 — алерт по новым стареющим клиентам (40+ дней без отгрузок)."""
    logger.info(f"check_aging_clients стартовала в {datetime.now(MSK):%Y-%m-%d %H:%M %Z}")
    from moysklad import get_aging_clients
    from database import Database

    chat_id = get_group_chat_id()
    if not chat_id:
        return

    db = Database()

    try:
        clients = await get_aging_clients(days=40)
        if not clients:
            logger.info("check_aging_clients: стареющих клиентов нет")
            return

        # Фильтруем — только новые (кому ещё не отправляли алерт)
        already_alerted = db.get_aging_alerted()
        new_clients = [c for c in clients if c["id"] not in already_alerted]

        logger.info(f"check_aging_clients: всего={len(clients)} новых={len(new_clients)}")

        if not new_clients:
            logger.info("check_aging_clients: новых стареющих нет")
            return

        MANAGER_TAG_MAP = {
            "баласанян": "Карина Баласанян",
            "мерзлякова": "Елена Мерзлякова",
            "скляр": "Инесса Скляр",
            "дьяченко": "Ирина Дьяченко",
            "коликов": "Денис Коликов",
            "кормилицын": "Антон Кормилицын",
        }

        import asyncio
        for i, client in enumerate(new_clients):
            name = client["name"]
            tags = client.get("tags", [])
            last_date = client["last_demand_date"]
            days = client.get("days", 40)

            manager_name = "Без менеджера"
            manager_tag = None
            for tag in tags:
                if tag.lower() in MANAGER_TAG_MAP:
                    manager_name = MANAGER_TAG_MAP[tag.lower()]
                    manager_tag = tag.lower()
                    break

            # Алерт в группу
            await app.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"⚠️ *Стареющий клиент*\n\n"
                    f"👤 *{name}*\n"
                    f"📅 Последняя отгрузка: {last_date} ({days} дн. назад)\n"
                    f"👔 Менеджер: {manager_name}"
                ),
                parse_mode="Markdown"
            )

            # Задача менеджеру в личку
            if manager_tag:
                mgr_chat_id = db.get_manager_chat_id(manager_name.split()[0])
                if mgr_chat_id:
                    try:
                        await app.bot.send_message(
                            chat_id=mgr_chat_id,
                            text=(
                                f"📋 *Стареющий клиент*\n\n"
                                f"👤 *{name}* — нет отгрузок {days} дней.\n"
                                f"Последняя: {last_date}\n\n"
                                f"Свяжись и сделай спецпредложение."
                            ),
                            parse_mode="Markdown"
                        )
                    except Exception as e:
                        logger.warning(f"check_aging_clients: {manager_name}: {e}")

            # Сохраняем что алерт отправлен
            db.save_aging_alert(client["id"], name)
            logger.info(f"check_aging_clients: {name} → {manager_name}")

            if i < len(new_clients) - 1:
                await asyncio.sleep(30)

    except Exception as e:
        logger.error(f"check_aging_clients: {e}", exc_info=True)


async def _pdz_run_and_record(job_id: str, func, app: Application, db):
    """Обёртка для всех PDZ-cron-job'ов: вызывает func(app, db) и при успехе
    записывает дату последнего выполнения в bot_settings.pdz_job_last_run_{id}.

    Нужно для pdz_catch_up_missed_jobs() — он смотрит last_run при старте
    бота, чтобы понять, был ли job сегодня запущен. Если упал — last_run
    не пишем, тогда catch-up попробует ещё раз при следующем старте.
    """
    try:
        result = await func(app, db)
        try:
            db.set_pdz_job_last_run(job_id, datetime.now(MSK).date())
        except Exception as ee:
            logger.warning(f"_pdz_run_and_record({job_id}): set last_run failed: {ee}")
        return result
    except Exception:
        logger.error(f"_pdz_run_and_record({job_id}): job упал", exc_info=True)
        raise


async def pdz_catch_up_missed_jobs(app: Application, db):
    """Вызывается ОДИН раз при старте бота, после setup_scheduler.

    Для каждого PDZ-cron-job: если сегодняшнее cron-время уже прошло
    И job сегодня ещё не выполнялся (last_run != today) → запускаем сейчас.

    Закрывает сценарий: контейнер Amvera пересобирался ПОСЛЕ cron-tick'а,
    AsyncIOScheduler без persistent jobstore не знает о пропуске. Без catch-up
    дайджесты пропадают, как было 2026-05-22 (14:10) и 2026-05-25 (14:10).
    """
    import asyncio as _asyncio
    now = datetime.now(MSK)
    today = now.date()

    # Порядок намеренно НЕ хронологический. catch_up идёт fire-and-forget
    # (см. bot.py _catchup_in_bg). При частых rebuild Amvera SIGTERM может
    # оборвать catch_up в середине. Поэтому быстрые TG-сводки (digest ~5с,
    # owner_pending ~2с) - впереди медленных snapshot (70с МС API), даже
    # если хронологически snapshot раньше. Регрессия 2026-06-04: 3-4 дня
    # подряд catch_up успевал только до snapshot и обрывался, дайджесты
    # пропадали. См. retro 2026-06-04.
    pdz_jobs = [
        ("pdz_send_digests_1300",        13,  0, pdz_send_digests_job),
        ("pdz_send_owner_pending_1505",  15,  5, pdz_send_owner_pending_job),
        ("pdz_snapshot_1245",            12, 45, pdz_take_snapshot_job),
        ("pdz_snapshot_1250",            12, 50, pdz_take_snapshot_job),
        ("pdz_process_events_1252",      12, 52, pdz_process_events_job),
        ("pdz_generate_html_1305",       13,  5, pdz_generate_html_job),
    ]

    caught = 0
    for job_id, h, m, func in pdz_jobs:
        cron_time_today = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if now < cron_time_today:
            continue  # cron-время ещё впереди — scheduler сам дёрнет
        try:
            last_run = db.get_pdz_job_last_run(job_id)
        except Exception as e:
            logger.warning(f"pdz_catch_up: get_pdz_job_last_run({job_id}) failed: {e}")
            last_run = None
        if last_run == today:
            continue  # уже отрабатывал сегодня — не дублировать

        logger.warning(
            f"pdz_catch_up: {job_id} был пропущен (cron {h:02d}:{m:02d} МСК, "
            f"last_run={last_run}, today={today}). Запускаю сейчас."
        )
        try:
            await _pdz_run_and_record(job_id, func, app, db)
            caught += 1
        except Exception:
            pass  # уже залогировано внутри обёртки
        await _asyncio.sleep(2)  # не нагружать МС API подряд

    logger.info(f"pdz_catch_up: догнано {caught} job'ов")


async def pdz_take_snapshot_job(app: Application, db):
    """Снимок состояния customerorder для ПДЗ-автоматики.

    Запускается дважды в день — 13:55 и 14:00 МСК. Тянет все заказы с
    заполненным `Дата планируемой оплаты`, пишет в `pdz_snapshots`.
    Логика срывов (Фаза 3) будет сравнивать вчерашний 14:00 со сегодняшним.
    """
    logger.info(
        f"pdz_take_snapshot_job стартовала в {datetime.now(MSK):%Y-%m-%d %H:%M %Z}"
    )
    try:
        from moysklad import pdz_take_snapshot
        rows = await pdz_take_snapshot()
        inserted = db.save_pdz_snapshot(rows)
        logger.info(f"pdz_take_snapshot_job: вставлено {inserted} строк")
    except Exception as e:
        logger.error(f"pdz_take_snapshot_job: {e}", exc_info=True)
        return

    # Состояние платежей клиентов — источник для эскалации и сводок вместо
    # отменённой 25.09.2026 «НОВОЙ даты оплаты». Отдельный try: снимок заказов
    # уже сохранён, падение здесь не должно его обесценить.
    try:
        from moysklad import fetch_payments_by_agent, PDZ_PAYMENT_WINDOW_DAYS
        from datetime import datetime as _dt
        from zoneinfo import ZoneInfo as _ZI

        payments = await fetch_payments_by_agent()
        snap_date = _dt.now(_ZI("Europe/Moscow")).date()
        saved = db.save_pdz_payment_state(snap_date, payments, PDZ_PAYMENT_WINDOW_DAYS)
        logger.info(f"pdz_take_snapshot_job: состояние платежей по {saved} контрагентам")
    except Exception as e:
        logger.error(f"pdz_take_snapshot_job: состояние платежей: {e}", exc_info=True)


# ─── ПДЗ Фаза 3: обработка событий + аудит исходной даты ─────────────────

def _md_escape(s) -> str:
    """Markdown-экранирование специальных символов в TG-сообщениях.

    parse_mode=Markdown в PTB: спецсимволы для нашего use-case — `*`, `_`,
    `` ` ``, `[`, `]`. Имена клиентов часто содержат `_`/`-`/кавычки — это ок,
    защищаем именно от ломающей разметки.
    """
    if s is None:
        return ""
    text = str(s)
    for ch in ("\\", "*", "_", "`", "[", "]"):
        text = text.replace(ch, "\\" + ch)
    return text


def _fmt_date(d) -> str:
    """Дата → DD.MM.YYYY. Принимает date/datetime/None/str."""
    if not d:
        return "—"
    try:
        return d.strftime("%d.%m.%Y")
    except Exception:
        return str(d)


async def pdz_process_events_job(app: Application, db):
    """Cron 14:02 МСК — аудит даты оплаты и эскалация по неплатежам.

    Шаги:
      a. today  = pdz_take_snapshot()  (свежий API-запрос; мы сразу после 14:00
         и хотим максимально актуальное состояние).
      b. yesterday = db.get_last_snapshot_before(today_date).
      c. эскалация по связке «просрочка + отсутствие платежей» (см.
         _pdz_escalate_by_payment_gap).
      d. initial_changes = await audit_ppm_initial_changes(today, yesterday).
      e. Для каждого initial_change — TG-сообщение собственнику OWNER_CHAT_ID.

    Сообщения группируются по 10 на одно TG-message (защита от rate-limit).

    25.09.2026: журнал обещаний (set/moved/broken) убран вместе с полем
    «НОВАЯ дата оплаты» — см. plans/2026-09-25-отмена-новой-даты-оплаты.md.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _ZI

    logger.info(
        f"pdz_process_events_job стартовала в {_dt.now(MSK):%Y-%m-%d %H:%M %Z}"
    )
    try:
        from moysklad import (
            pdz_take_snapshot,
            audit_ppm_initial_changes,
        )

        today_rows = await pdz_take_snapshot()
        today_date = _dt.now(_ZI("Europe/Moscow")).date()
        yesterday_rows = db.get_last_snapshot_before(today_date)

        # Сохраняем сегодняшний срез сюда же — чтобы ручной прогон был
        # самодостаточным (даже если 14:00-cron почему-то не отработал).
        try:
            inserted_now = db.save_pdz_snapshot(today_rows)
            logger.info(f"pdz_process_events_job: snapshot up-sert {inserted_now} строк")
        except Exception as e:
            logger.warning(f"pdz_process_events_job: повторный save_pdz_snapshot: {e}")

        # ── Эскалация по неплатежам (правило от 25.09.2026) ─────────────
        # Смотрим связку «глубина просрочки + отсутствие приходов денег»:
        #   ≥14 дн просрочки и ≥14 дн без платежей → задача менеджеру в МС.
        #   ≥14 дн просрочки и ≥21 дн без платежей → СТОП отгрузок + алерт.
        #   ≥30 дн просрочки и ≥21 дн без платежей → только предоплата + алерт.
        # Дедуп: db.is_pdz_escalation_done(agent_id, level) → не повторять.
        try:
            escalation_summary = await _pdz_escalate_by_payment_gap(app, db, today_rows)
            logger.info(
                f"pdz_process_events_job: escalations="
                f"tasks_created={escalation_summary['tasks_created']}, "
                f"stop_shipments={escalation_summary['stop_shipments']}, "
                f"prepayment_only={escalation_summary['prepayment_only']}, "
                f"skipped_done={escalation_summary['skipped_done']}"
            )
        except Exception as e:
            logger.error(f"pdz_process_events_job: escalation step failed: {e}", exc_info=True)
            escalation_summary = {
                "tasks_created": 0,
                "stop_shipments": 0,
                "prepayment_only": 0,
                "skipped_done": 0,
                "errors": [str(e)],
            }

        # Аудит исходной даты
        initial_changes = await audit_ppm_initial_changes(today_rows, yesterday_rows)
        logger.info(f"pdz_process_events_job: ppm_initial изменений: {len(initial_changes)}")

        if initial_changes:
            owner_raw = os.getenv("OWNER_CHAT_ID")
            if not owner_raw:
                logger.warning("pdz_process_events_job: OWNER_CHAT_ID не задан, алерты не отправлены")
            else:
                owner_id = int(owner_raw)
                BATCH = 10
                for i in range(0, len(initial_changes), BATCH):
                    chunk = initial_changes[i : i + BATCH]
                    lines = []
                    for c in chunk:
                        order_id = c.get("order_id") or ""
                        order_name = _md_escape(c.get("order_name") or "—")
                        agent_name = _md_escape(c.get("agent_name") or "—")
                        manager_tag = _md_escape(c.get("manager_tag") or "—")
                        changed_by = _md_escape(c.get("changed_by") or "не определено")
                        old_d = _md_escape(_fmt_date(c.get("old_ppm_initial")))
                        new_d = _md_escape(_fmt_date(c.get("new_ppm_initial")))
                        ms_url = (
                            f"https://online.moysklad.ru/app/#customerorder/edit?id={order_id}"
                        )
                        lines.append(
                            f"⚠️ *Изменена ИСХОДНАЯ дата оплаты*\n"
                            f"[{agent_name}]({ms_url}) · {order_name}\n"
                            f"Было: {old_d} → Стало: {new_d}\n"
                            f"Менеджер: {manager_tag}\n"
                            f"Кто менял: {changed_by}"
                        )
                    text = "\n\n".join(lines)
                    try:
                        await app.bot.send_message(
                            chat_id=owner_id,
                            text=text,
                            parse_mode="Markdown",
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        logger.error(f"pdz_process_events_job: TG-алерт собственнику: {e}")

        return {
            "initial_changes": len(initial_changes),
            "escalation": escalation_summary,
        }
    except Exception as e:
        logger.error(f"pdz_process_events_job: {e}", exc_info=True)
        return {"initial_changes": 0, "error": str(e)}


# Пороги эскалации (25.09.2026). Считаем не «сколько раз менеджер перенёс
# обещание» (поле отменено), а факт: клиент в просрочке и денег от него нет.
# Порог по числу срывов не годится — у клиента с десятками отгрузок срывов
# всегда много, и под стоп попадали бы крупнейшие плательщики (замер 25.09:
# правило «3 срыва за 90 дней» дало бы 74 клиента, включая Биг Маму и Фугу).
PDZ_ESC_TASK_OVERDUE_DAYS = 14      # задача менеджеру
PDZ_ESC_TASK_NOPAY_DAYS = 14
PDZ_ESC_STOP_OVERDUE_DAYS = 14      # стоп отгрузок
PDZ_ESC_STOP_NOPAY_DAYS = 21
PDZ_ESC_PREPAY_OVERDUE_DAYS = 30    # только предоплата
PDZ_ESC_PREPAY_NOPAY_DAYS = 21


async def _pdz_escalate_by_payment_gap(app: Application, db, today_rows: list) -> dict:
    """Эскалация по связке «глубина просрочки + отсутствие приходов денег».

    Для каждого контрагента из снимка считаем:
      - max_days_overdue — по заказам, где ppm_initial + GRACE прошла и заказ
        не оплачен, у клиента отрицательное сальдо;
      - days_no_pay — дней с последнего прихода денег (paymentin/cashin) по
        данным `pdz_payment_state`; клиента нет в таблице → платежей за всё
        окно не было, берём длину окна.

    Уровни (дедуп через db.is_pdz_escalation_done):
      2 → задача менеджеру в МС.
      3 → set_client_stop_flag(stop_shipments) + TG-алерт собственнику.
      4 → set_client_stop_flag(prepayment_only) + TG-алерт.

    Долг в алерте — по сальдо контрагента, а не по сумме неоплаченных заказов:
    payedSum в МС не отражает неразнесённые платежи (см. память
    reference_f2b_ms_payedSum_unreliable).

    Возвращает сводку (для логирования и теста).
    """
    from moysklad import _pdz_classify, PDZ_PAYMENT_WINDOW_DAYS, _to_date

    summary = {
        "tasks_created": 0,
        "stop_shipments": 0,
        "prepayment_only": 0,
        "skipped_done": 0,
        "candidates": 0,
        "errors": [],
    }

    today = datetime.now(MSK).date()

    try:
        pay_state = db.get_pdz_payment_state() or {}
    except Exception as e:
        logger.error(f"_pdz_escalate_by_payment_gap: get_pdz_payment_state: {e}")
        return summary
    if not pay_state:
        # Состояния платежей ещё нет (первый прогон после деплоя) — без него
        # правило неполное, молча выходим: лучше не эскалировать, чем ошибочно.
        logger.warning("_pdz_escalate_by_payment_gap: pdz_payment_state пуст, пропуск")
        return summary

    # Шаг 1: собрать по клиенту максимальную глубину просрочки и сальдо.
    agents: dict = {}
    for r in today_rows or []:
        aid = r.get("agent_id") or ""
        if not aid:
            continue
        bal_raw = r.get("agent_balance")
        try:
            balance = float(bal_raw) if bal_raw is not None else None
        except Exception:
            balance = None
        if balance is None or balance >= 0:
            continue  # клиент не должен — не эскалируем
        try:
            total = float(r.get("total_sum") or 0)
            payed = float(r.get("payed_sum") or 0)
        except Exception:
            continue
        if payed >= total:
            continue
        status, _effective, days_overdue = _pdz_classify(_to_date(r.get("ppm_initial")), today)
        if status != "overdue":
            continue
        a = agents.setdefault(aid, {
            "agent_name": r.get("agent_name") or "—",
            "manager_tag": (r.get("manager_tag") or "").lower(),
            "debt": abs(balance),
            "max_days_overdue": 0,
        })
        if days_overdue > a["max_days_overdue"]:
            a["max_days_overdue"] = days_overdue

    # Шаг 2: добавить разрыв по платежам.
    for aid, a in agents.items():
        st = pay_state.get(aid) or {}
        last = st.get("last_payment_date")
        last = _to_date(last) if last else None
        a["days_no_pay"] = (today - last).days if last else PDZ_PAYMENT_WINDOW_DAYS
        a["last_payment_date"] = last

    owner_raw = os.getenv("OWNER_CHAT_ID")
    owner_id = int(owner_raw) if owner_raw else None

    from moysklad import (
        find_employee_id_by_tag,
        create_task as ms_create_task,
        fmt_money,
        PDZ_MANAGER_TAG_MAP,
    )
    from datetime import timedelta

    # Алерты собственнику копим и отправляем ОДНИМ сообщением в конце:
    # раньше на каждого клиента уходил отдельный пуш, и при пачке эскалаций
    # собственник получал «разом много сообщений» (правка 25.09.2026).
    stop_alerts: list = []
    prepay_alerts: list = []

    def _alert_line(a_name: str, m_tag: str, a_id: str, reason: str, debt: float) -> str:
        tag_display = PDZ_MANAGER_TAG_MAP.get(m_tag, m_tag or "—")
        safe_name = (a_name or "—").replace("*", "").replace("_", "")
        safe_tag = (tag_display or "—").replace("*", "").replace("_", "")
        safe_aid = (a_id or "—").replace("*", "").replace("_", "")
        return (
            f"• {safe_name} — {fmt_money(debt)} · {reason} · {safe_tag}\n"
            f"  `/snimi_stop {safe_aid}`"
        )

    for aid, info in agents.items():
        overdue_days = info["max_days_overdue"]
        no_pay = info["days_no_pay"]
        agent_name = info["agent_name"]
        manager_tag = info["manager_tag"]
        total_unpaid = info["debt"]

        hit_task = (overdue_days >= PDZ_ESC_TASK_OVERDUE_DAYS
                    and no_pay >= PDZ_ESC_TASK_NOPAY_DAYS)
        hit_stop = (overdue_days >= PDZ_ESC_STOP_OVERDUE_DAYS
                    and no_pay >= PDZ_ESC_STOP_NOPAY_DAYS)
        hit_prepay = (overdue_days >= PDZ_ESC_PREPAY_OVERDUE_DAYS
                      and no_pay >= PDZ_ESC_PREPAY_NOPAY_DAYS)
        if not hit_task:
            continue
        summary["candidates"] += 1
        reason_txt = f"просрочка {overdue_days} дн, без платежей {no_pay} дн"

        # ── Уровень 2: задача менеджеру в МС ───────────────────────────
        if hit_task and not db.is_pdz_escalation_done(aid, 2):
            try:
                assignee_id = await find_employee_id_by_tag(manager_tag)
                if not assignee_id:
                    logger.warning(
                        f"_pdz_escalate_by_payment_gap: не нашли сотрудника МС по тегу '{manager_tag}'"
                        f" (клиент {agent_name}); задача не поставлена"
                    )
                else:
                    due_msk = datetime.now(MSK) + timedelta(days=2)
                    desc = (
                        f"Клиент {agent_name}: {reason_txt}. "
                        f"Связаться и зафиксировать, когда будет оплата."
                    )
                    await ms_create_task(assignee_id, desc, due_msk)
                    summary["tasks_created"] += 1
                db.mark_pdz_escalation_done(aid, 2)
            except Exception as e:
                logger.error(
                    f"_pdz_escalate_by_payment_gap: create_task для {agent_name} ({manager_tag}): {e}"
                )
                summary["errors"].append(f"task:{aid}:{e}")
        elif hit_task and db.is_pdz_escalation_done(aid, 2):
            summary["skipped_done"] += 1

        # ── Уровень 3: stop_shipments + TG-алерт собственнику ──────────
        if hit_stop and not db.is_pdz_escalation_done(aid, 3):
            try:
                changed = db.set_client_stop_flag(
                    agent_id=aid,
                    agent_name=agent_name,
                    status="stop_shipments",
                    reason=reason_txt,
                    set_by="auto",
                )
                if changed:
                    summary["stop_shipments"] += 1
                db.mark_pdz_escalation_done(aid, 3)
                if owner_id:
                    stop_alerts.append(
                        _alert_line(agent_name, manager_tag, aid, reason_txt, total_unpaid)
                    )
            except Exception as e:
                logger.error(f"_pdz_escalate_by_payment_gap: set_stop_shipments для {agent_name}: {e}")
                summary["errors"].append(f"stop3:{aid}:{e}")

        # ── Уровень 4+: prepayment_only + TG-алерт собственнику ────────
        if hit_prepay and not db.is_pdz_escalation_done(aid, 4):
            try:
                changed = db.set_client_stop_flag(
                    agent_id=aid,
                    agent_name=agent_name,
                    status="prepayment_only",
                    reason=reason_txt,
                    set_by="auto",
                )
                if changed:
                    summary["prepayment_only"] += 1
                db.mark_pdz_escalation_done(aid, 4)
                if owner_id:
                    prepay_alerts.append(
                        _alert_line(agent_name, manager_tag, aid, reason_txt, total_unpaid)
                    )
            except Exception as e:
                logger.error(f"_pdz_escalate_by_payment_gap: set_prepayment_only для {agent_name}: {e}")
                summary["errors"].append(f"prepay:{aid}:{e}")

    # ── Одно сводное сообщение собственнику по всем новым автостопам ────
    if owner_id and (stop_alerts or prepay_alerts):
        parts: list = ["🚫 *Автостоп по ПДЗ*"]
        if prepay_alerts:
            parts.append(f"\n*Только предоплата* ({len(prepay_alerts)}):")
            parts.extend(prepay_alerts)
        if stop_alerts:
            parts.append(f"\n*Стоп отгрузок* ({len(stop_alerts)}):")
            parts.extend(stop_alerts)
        chunks: list = []
        cur = ""
        for block in parts:
            piece = block if not cur else f"{cur}\n{block}"
            if len(piece) > 3800 and cur:
                chunks.append(cur)
                cur = block
            else:
                cur = piece
        if cur:
            chunks.append(cur)
        for chunk in chunks:
            try:
                await app.bot.send_message(
                    chat_id=owner_id,
                    text=chunk,
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
            except Exception as e:
                logger.error(
                    f"_pdz_escalate_by_payment_gap: сводный TG-алерт собственнику: {e}"
                )

    return summary


# ─── ПДЗ Фаза 4: TG-дайджесты ────────────────────────────────────────────


async def pdz_send_digests_job(app: Application, db) -> dict:
    """Cron 14:10 МСК — рассылка дайджестов просрочек менеджерам в личку.

    Для каждого тега из PDZ_MANAGER_TAG_MAP:
      - получает items через pdz_overdue_for_manager(tag, db=db);
      - если пусто — пропускает (тишина = всё хорошо);
      - ищет chat_id менеджера в БД (по первому имени из имени менеджера);
      - формирует список TG-сообщений (каждое ≤3500 симв.);
      - отправляет последовательно, sleep 0.3с между сообщениями (TG rate limit).

    Возвращает сводку для теста: {tag: {"status": ..., "messages_sent": N}}.
    """
    from moysklad import (
        PDZ_MANAGER_TAG_MAP,
        PDZ_MANAGER_TG_IDS,
        pdz_overdue_for_manager,
        pdz_send_manager_digest_text,
    )

    logger.info(
        f"pdz_send_digests_job стартовала в {datetime.now(MSK):%Y-%m-%d %H:%M %Z}"
    )

    # Вчерашний снимок — для дельты «за сутки» в шапке сводки.
    try:
        from datetime import datetime as _dt
        from zoneinfo import ZoneInfo as _ZI
        yesterday_rows = db.get_last_snapshot_before(_dt.now(_ZI("Europe/Moscow")).date())
    except Exception as e:
        logger.warning(f"pdz_send_digests_job: вчерашний снимок недоступен: {e}")
        yesterday_rows = None

    report: dict = {}
    for tag, manager_name in PDZ_MANAGER_TAG_MAP.items():
        try:
            items = await pdz_overdue_for_manager(tag, db=db)
        except Exception as e:
            logger.error(f"pdz_send_digests_job: {tag} pdz_overdue_for_manager: {e}", exc_info=True)
            report[tag] = {"status": "error_fetch", "error": str(e), "messages_sent": 0}
            continue

        if not items:
            logger.info(f"pdz_send_digests_job: {tag} — просрочек нет, пропуск")
            report[tag] = {"status": "no_overdue", "messages_sent": 0, "clients": 0}
            continue

        chat_id = PDZ_MANAGER_TG_IDS.get(tag)
        if not chat_id:
            first_name = manager_name.split()[0] if manager_name else ""
            chat_id = db.get_manager_chat_id(first_name) if first_name else None
        if not chat_id:
            logger.warning(
                f"pdz_send_digests_job: {tag} ({manager_name}) — chat_id не найден в manager_chats"
            )
            report[tag] = {
                "status": "no_chat_id",
                "manager": manager_name,
                "messages_sent": 0,
                "clients": len(items),
            }
            continue

        delta = None
        if yesterday_rows:
            try:
                prev = await pdz_overdue_for_manager(tag, db=db, rows=yesterday_rows)
                delta = round(
                    sum(float(i.get("total_unpaid", 0) or 0) for i in items)
                    - sum(float(p.get("total_unpaid", 0) or 0) for p in prev), 2
                )
            except Exception as e:
                logger.warning(f"pdz_send_digests_job: дельта для {tag}: {e}")

        messages = pdz_send_manager_digest_text(items, manager_name, delta=delta)
        sent = 0
        for i, msg in enumerate(messages):
            try:
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=msg,
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
                sent += 1
                if i < len(messages) - 1:
                    await asyncio.sleep(0.3)
            except Exception as e:
                logger.error(
                    f"pdz_send_digests_job: {tag} send_message → {chat_id}: {e}"
                )
                break

        logger.info(
            f"pdz_send_digests_job: {tag} ({manager_name}) — {len(items)} клиентов, "
            f"{sent}/{len(messages)} сообщений отправлено"
        )
        report[tag] = {
            "status": "ok",
            "manager": manager_name,
            "chat_id": chat_id,
            "clients": len(items),
            "messages_sent": sent,
            "messages_total": len(messages),
        }

    return report


async def pdz_send_owner_pending_job(app: Application, db) -> dict:
    """Cron 16:05 МСК — вечерняя сводка собственнику по просрочке менеджеров.

    По каждому менеджеру: сумма просрочки, число клиентов, дельта за сутки,
    худший клиент по дням. Ниже — список его клиентов.

    Если просрочки нет ни у кого — отправляет «✅ Просрочек нет».

    25.09.2026: раньше это был пинг «кто не проставил НОВУЮ дату оплаты».
    Поле отменено, признак «обработал» больше не существует — см.
    plans/2026-09-25-отмена-новой-даты-оплаты.md.
    """
    from moysklad import (
        PDZ_MANAGER_TAG_MAP,
        pdz_unprocessed_for_owner,
        agent_ids_with_tag_live,
        fmt_money,
    )

    logger.info(
        f"pdz_send_owner_pending_job стартовала в {datetime.now(MSK):%Y-%m-%d %H:%M %Z}"
    )

    owner_raw = os.getenv("OWNER_CHAT_ID")
    if not owner_raw:
        logger.warning("pdz_send_owner_pending_job: OWNER_CHAT_ID не задан, пропуск")
        return {"status": "no_owner_chat_id"}
    owner_id = int(owner_raw)

    try:
        # Живой agent→менеджер map по текущим тегам МС (привязка не залипает при смене тега).
        live_map = {}
        for _mtag in PDZ_MANAGER_TAG_MAP:
            for _aid in await agent_ids_with_tag_live(_mtag):
                live_map[_aid] = _mtag
        by_tag = pdz_unprocessed_for_owner(db, live_map=live_map or None)
    except Exception as e:
        logger.error(f"pdz_send_owner_pending_job: {e}", exc_info=True)
        try:
            await app.bot.send_message(
                chat_id=owner_id,
                text=f"⚠️ pdz_send_owner_pending_job упал: {e}",
            )
        except Exception:
            pass
        return {"status": "error", "error": str(e)}

    if not by_tag:
        text = "✅ Просрочек нет ни у одного менеджера"
        try:
            await app.bot.send_message(chat_id=owner_id, text=text)
        except Exception as e:
            logger.error(f"pdz_send_owner_pending_job: send_message OK: {e}")
        return {"status": "all_clear", "managers": 0}

    # Дельта за сутки по каждому менеджеру — из вчерашнего снимка теми же правилами.
    prev_totals: dict = {}
    try:
        yesterday_rows = db.get_last_snapshot_before(datetime.now(MSK).date())
        if yesterday_rows:
            from moysklad import pdz_overdue_for_manager
            for _tag in PDZ_MANAGER_TAG_MAP:
                prev_items = await pdz_overdue_for_manager(_tag, db=db, rows=yesterday_rows)
                prev_totals[_tag] = sum(float(i.get("total_unpaid", 0) or 0) for i in prev_items)
    except Exception as e:
        logger.warning(f"pdz_send_owner_pending_job: дельта за сутки недоступна: {e}")

    # Формируем сводку. Группировка по менеджеру, внутри — клиенты по
    # total_unpaid убыванию (уже отсортированы pdz_unprocessed_for_owner).
    grand_total = sum(
        sum(a["total_unpaid"] for a in agents) for agents in by_tag.values()
    )
    lines: list[str] = [f"📊 *Просрочка на 16:00 — {fmt_money(grand_total)}*", ""]

    # Сортируем менеджеров по сумме просрочки (убывание).
    tag_totals = [
        (tag, sum(a["total_unpaid"] for a in agents), agents)
        for tag, agents in by_tag.items()
    ]
    tag_totals.sort(key=lambda x: x[1], reverse=True)

    for tag, total, agents in tag_totals:
        manager_name = PDZ_MANAGER_TAG_MAP.get(tag, tag)
        surname = manager_name.split()[-1] if manager_name else tag
        cnt = len(agents)
        word = "клиент" if cnt % 10 == 1 and cnt % 100 != 11 else (
            "клиента" if cnt % 10 in (2, 3, 4) and cnt % 100 not in (12, 13, 14) else "клиентов"
        )
        head = f"*{surname}* — {cnt} {word} · {fmt_money(total)}"
        prev = prev_totals.get(tag)
        if prev is not None:
            d = round(total - prev, 2)
            if abs(d) >= 1:
                head += f" · за сутки {'🔺 +' if d > 0 else '🔻 '}{fmt_money(abs(d))}"
        worst = max(agents, key=lambda a: int(a.get("max_days_overdue", 0) or 0))
        worst_name = (worst.get("agent_name") or "—").replace("*", "").replace("_", "")
        head += f"\nДольше всех: {worst_name} — {int(worst.get('max_days_overdue', 0) or 0)} дн"
        lines.append(head)
        for a in agents:
            name = (a.get("agent_name") or "—").replace("*", "").replace("_", "")
            url = a.get("ms_url_first_order") or "#"
            no_pay = a.get("days_no_pay")
            # Фаза 6: префикс стоп-флага.
            stop_status = a.get("stop_status")
            if stop_status == "stop_shipments":
                stop_prefix = "🚫 СТОП "
            elif stop_status == "prepayment_only":
                stop_prefix = "🚫 ПРЕДОПЛАТА "
            else:
                stop_prefix = ""
            prefix = stop_prefix + ("🔴 " if (no_pay or 0) >= 21 else "")
            suffix = f" · без платежей {int(no_pay)} дн" if no_pay else ""
            lines.append(
                f"• {prefix}[{name}]({url}) · {a.get('max_days_overdue', 0)} дн · "
                f"{fmt_money(a.get('total_unpaid', 0))}{suffix}"
            )
        lines.append("")

    text = "\n".join(lines).rstrip()
    # Разбиваем на чанки ≤3500 символов (как в дайджесте).
    chunks: list[str] = []
    buf: list[str] = []
    cur_len = 0
    for ln in text.split("\n"):
        if cur_len + len(ln) + 1 > 3500 and buf:
            chunks.append("\n".join(buf))
            buf = []
            cur_len = 0
        buf.append(ln)
        cur_len += len(ln) + 1
    if buf:
        chunks.append("\n".join(buf))

    sent = 0
    for i, chunk in enumerate(chunks):
        try:
            await app.bot.send_message(
                chat_id=owner_id,
                text=chunk,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
            sent += 1
            if i < len(chunks) - 1:
                await asyncio.sleep(0.3)
        except Exception as e:
            logger.error(f"pdz_send_owner_pending_job: send_message → {owner_id}: {e}")
            break

    logger.info(
        f"pdz_send_owner_pending_job: {len(by_tag)} менеджеров с необработанными, "
        f"{sent}/{len(chunks)} сообщений отправлено"
    )
    return {
        "status": "sent",
        "managers": len(by_tag),
        "messages_sent": sent,
        "messages_total": len(chunks),
    }


async def pdz_generate_html_job(app: Application, db) -> dict:
    """Cron 14:15 МСК — регенерация HTML-отчёта «Дебиторка» (Фаза 5).

    Шаги:
      1. render_pdz_html_from_db(db) — собирает HTML из последнего snapshot
         и pdz_payment_state (МС API не дёргается).
      2. db.set_pdz_html_cache(html) — атомарно перезаписывает кэш.
      3. db.create_report_link(mgr_filter='pdz', ttl_minutes=24*60) — токен на 24ч.
      4. Шлёт собственнику в ЛС ссылку `https://<host>/pdz?token=...`.

    Возвращает dict для тестов: {status, html_size, token, url}.
    """
    logger.info(
        f"pdz_generate_html_job стартовала в {datetime.now(MSK):%Y-%m-%d %H:%M %Z}"
    )

    owner_raw = os.getenv("OWNER_CHAT_ID")
    if not owner_raw:
        logger.warning("pdz_generate_html_job: OWNER_CHAT_ID не задан")
        return {"status": "no_owner_chat_id"}
    owner_id = int(owner_raw)

    try:
        from pdz_report_html import render_pdz_html_from_db
        html_text = render_pdz_html_from_db(db)
    except Exception as e:
        logger.error(f"pdz_generate_html_job: render: {e}", exc_info=True)
        try:
            await app.bot.send_message(
                chat_id=owner_id,
                text=f"⚠️ pdz_generate_html_job упал на рендере: {e}",
            )
        except Exception:
            pass
        return {"status": "error_render", "error": str(e)}

    try:
        db.set_pdz_html_cache(html_text)
    except Exception as e:
        logger.error(f"pdz_generate_html_job: set_pdz_html_cache: {e}", exc_info=True)
        return {"status": "error_save", "error": str(e)}

    # Шаг 4 плана plans/2026-05-21-единый-дашборд-f2b.md: вместо одноразового
    # /pdz?token=… ссылаемся на единый дашборд, вкладку «Дебиторка». Дашборд
    # тянет HTML server-side через /pdz/embed?secret=… (постоянный), TTL-токен
    # для отчёта больше не нужен. Basic Auth на /dashboard — admin / Archor973.
    dashboard_base = os.getenv("DASHBOARD_URL", "https://f2b-fishki-victor03.amvera.io").rstrip("/")
    url = f"{dashboard_base}/dashboard?tab=pdz"

    try:
        await app.bot.send_message(
            chat_id=owner_id,
            text=f"📊 Дебиторка обновлена: {url}",
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error(f"pdz_generate_html_job: send_message: {e}")
        return {"status": "error_send", "error": str(e), "url": url}

    logger.info(
        f"pdz_generate_html_job: html={len(html_text)} симв, url={url}"
    )
    return {
        "status": "ok",
        "html_size": len(html_text),
        "url": url,
    }


async def op_new_share_snapshot_job(app: Application, db):
    """Пересчитывает «% работы на новых клиентах» за MTD и пишет снимок в БД.

    Cron: ПТ 08:00 МСК. План:
    2026-05-21-виджет-процент-новых-в-отчете-оп.md, Фаза 5.

    Виджет в /op_report читает снимок через db.get_new_share_snapshot()
    ОТДЕЛЬНО от report_cache (TTL 5 ч), поэтому свежий снимок виден сразу.

    При исключении в расчёте — TG-алерт собственнику; старый снимок не
    перезаписывается, в UI он покажется как «устарело · DD.MM» через >9 дней.
    """
    logger.info(
        f"op_new_share_snapshot_job стартовала в "
        f"{datetime.now(MSK):%Y-%m-%d %H:%M %Z}"
    )

    try:
        from op_new_share import compute_new_client_share
        from datetime import date
        today = datetime.now(MSK).date()
        start = today.replace(day=1)
        result = await compute_new_client_share(start, today)
        db.set_new_share_snapshot(result)
        logger.info(
            f"op_new_share_snapshot_job OK: "
            f"company={result['company_pct']}% "
            f"by_manager={result['by_manager']}"
        )
        return {"status": "ok", "company_pct": result["company_pct"]}
    except Exception as e:
        logger.error(f"op_new_share_snapshot_job: {e}", exc_info=True)
        owner_raw = os.getenv("OWNER_CHAT_ID")
        if owner_raw:
            try:
                await app.bot.send_message(
                    chat_id=int(owner_raw),
                    text=f"⚠️ op_new_share_snapshot_job упал: {e}\n"
                         f"Снимок не обновлён. Старая цифра остаётся в /op_report; "
                         f"если она старше 9 дней — в UI появится «устарело».",
                )
            except Exception as send_err:
                logger.error(
                    f"op_new_share_snapshot_job: TG-алерт не доставлен: {send_err}"
                )
        return {"status": "error", "error": str(e)}


async def direct_weekly_report_job(app: Application):
    """Шлёт собственнику сводку по Я.Директ. Cron: ПН 09:30 МСК.

    План: 2026-08-10-еженедельная-сводка-директа-в-боте.md, Фаза 4.
    Раньше это была cloud-routine, которая коммитила файл-маркер в git —
    до собственника не доходило ничего, и сводку не делали неделями.

    Ошибку не глотаем: если Директ не отдал данные или протух токен,
    собственник должен увидеть это текстом, а не тишину вместо отчёта.
    """
    logger.info(
        f"direct_weekly_report_job стартовала в "
        f"{datetime.now(MSK):%Y-%m-%d %H:%M %Z}"
    )
    owner_raw = os.getenv("OWNER_CHAT_ID")
    if not owner_raw:
        logger.error("direct_weekly_report_job: OWNER_CHAT_ID не задан")
        return {"status": "error", "error": "no OWNER_CHAT_ID"}
    owner = int(owner_raw)

    try:
        from direct_report import build_report, split_for_telegram
        text = await build_report()
        for chunk in split_for_telegram(text):
            await app.bot.send_message(chat_id=owner, text=chunk,
                                       disable_web_page_preview=True)
        logger.info("direct_weekly_report_job OK")
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"direct_weekly_report_job: {e}", exc_info=True)
        try:
            await app.bot.send_message(
                chat_id=owner,
                text=f"⚠️ Сводка Я.Директ не собралась: {e}\n"
                     f"Попробовать вручную — команда /direct_report.",
            )
        except Exception as send_err:
            logger.error(
                f"direct_weekly_report_job: TG-алерт не доставлен: {send_err}")
        return {"status": "error", "error": str(e)}


async def refresh_op_report_cache_job(app: Application, db):
    """Прогревает кэш отчёта ОП каждые 4 ч.

    TTL в БД — 5 ч (database.get_report_cache, INTERVAL '300 minutes'),
    запас 1 ч. Зачем: handle_web_report при пустом кэше пытается синхронно
    собрать данные из МС + amoCRM и не укладывается в таймаут Amvera ingress
    (504 «upstream request timeout»). _refresh_report_cache живёт в bot.py —
    late import чтобы избежать циркулярки scheduler ↔ bot.
    """
    logger.info(
        f"refresh_op_report_cache_job стартовала в "
        f"{datetime.now(MSK):%Y-%m-%d %H:%M %Z}"
    )
    try:
        from bot import _refresh_report_cache
        await _refresh_report_cache()
        logger.info("refresh_op_report_cache_job OK")
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"refresh_op_report_cache_job: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


# ─── Автоподстановка «Дата планируемой оплаты» (план 2026-05-20-автоподстановка) ──
# Cron-tick, primary mechanism. Activation gated by PAYMENT_PLANNED_AUTOFILL_ENABLED
# env-var ("1" / "true"). Регистрация cron'а сейчас закомментирована в setup_scheduler.

async def payment_planned_autofill_job(app: Application, db) -> dict:
    """Cron-tick автоподстановки «Даты планируемой оплаты» + TG-алерты собственнику.

    Включается env-флагом PAYMENT_PLANNED_AUTOFILL_ENABLED. Если выключен — silently skip.
    Шлёт собственнику один TG-message со списком zero-delay+большая сумма (триггер на
    ревизию договора с клиентом).
    """
    enabled = (os.getenv("PAYMENT_PLANNED_AUTOFILL_ENABLED", "") or "").strip().lower() in {"1", "true", "yes"}
    if not enabled:
        logger.info("payment_planned_autofill_job: disabled by env (PAYMENT_PLANNED_AUTOFILL_ENABLED!=1), skip")
        return {"status": "disabled"}

    logger.info(
        f"payment_planned_autofill_job стартовала в {datetime.now(MSK):%Y-%m-%d %H:%M %Z}"
    )
    try:
        from moysklad import payment_planned_autofill_tick
        res = await payment_planned_autofill_tick(db, hours_back=24)
        try:
            db.cleanup_bot_self_writes(older_than_seconds=300)
        except Exception as ex:
            logger.warning(f"cleanup_bot_self_writes failed: {ex}")
        logger.info(f"payment_planned_autofill_job: {res}")
    except Exception as e:
        logger.error(f"payment_planned_autofill_job: tick failed: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}

    # TG-алерт собственнику по zero_alerts (delay=0 + сумма > порога)
    try:
        owner_id_raw = os.getenv("OWNER_CHAT_ID", "")
        owner_id = int(owner_id_raw) if owner_id_raw else None
    except ValueError:
        owner_id = None

    alerts = res.get("zero_alerts") or []
    if owner_id and alerts:
        lines = [
            f"Автоподстановка даты оплаты: {len(alerts)} заказ(ов) с пустой отсрочкой при сумме > {int(50_000):,} ₽".replace(",", " "),
            "",
            "Возможно у клиента есть отсрочка по договору, но не проставлена в карточке — это сигнал на ревизию.",
            "",
        ]
        for a in alerts[:15]:
            name = _md_escape(a.get("agent_name") or a.get("order_name") or "—")
            href = a.get("order_href") or ""
            sum_rub = a.get("sum_rub") or 0
            lines.append(f"• [{name}]({href}) — {sum_rub:,.0f} ₽".replace(",", " "))
        if len(alerts) > 15:
            lines.append(f"… и ещё {len(alerts) - 15}")
        text = "\n".join(lines)
        try:
            await app.bot.send_message(
                chat_id=owner_id, text=text, parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        except Exception as e:
            logger.warning(f"payment_planned_autofill_job: TG send failed: {e}")

    return {"status": "ok", **res}


async def dashamail_weekly_send_job(app: Application, db) -> None:
    """ПН 09:00 МСК — еженедельная прайс-рассылка DashaMail.

    Запускает scripts/dashamail_weekly_send.py как subprocess (там Playwright
    sync — не миксуется с asyncio loop). Скрипт сам шлёт уведомление Виктору
    в TG через прямой Bot API. План: 2026-06-22-email-pipeline-fix, Фаза C.
    """
    import sys
    from pathlib import Path

    logger.info(f"dashamail_weekly_send стартовала в {datetime.now(MSK):%Y-%m-%d %H:%M %Z}")
    script = Path(__file__).resolve().parent / "scripts" / "dashamail_weekly_send.py"
    if not script.is_file():
        logger.error(f"dashamail_weekly_send_job: script not found: {script}")
        return
    # Передаём env с TG_BOT_TOKEN (бот хранит токен как TELEGRAM_BOT_TOKEN).
    env = dict(os.environ)
    if "TELEGRAM_BOT_TOKEN" in env and "TG_BOT_TOKEN" not in env:
        env["TG_BOT_TOKEN"] = env["TELEGRAM_BOT_TOKEN"]
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(script),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=env,
    )
    try:
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=900)
    except asyncio.TimeoutError:
        proc.kill()
        logger.error("dashamail_weekly_send_job: timeout 900s — script killed")
        return
    out = stdout.decode("utf-8", errors="replace")
    logger.info(f"dashamail_weekly_send_job rc={proc.returncode}\n{out[-3000:]}")


async def control_list_daily_job(app: Application, db):
    """16:00 МСК — сводка по листу контроля дебиторки собственнику.

    Лист заводится в БД (`control_list`), а не в коде: состав меняется чаще,
    чем выкатывается бот. Сид стартового состава — `control_list.seed_control_list`,
    вызывается один раз при старте, дальше состав правится командами.
    """
    logger.info(f"control_list_daily_job стартовала в {datetime.now(MSK):%Y-%m-%d %H:%M %Z}")
    owner_raw = os.getenv("OWNER_CHAT_ID")
    if not owner_raw:
        logger.warning("control_list_daily_job: OWNER_CHAT_ID не задан, пропуск")
        return
    try:
        from control_list import send_daily_summary
        await send_daily_summary(app.bot, db, int(owner_raw))
    except Exception as e:
        logger.error(f"control_list_daily_job: {e}", exc_info=True)
