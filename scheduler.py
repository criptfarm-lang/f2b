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
    # snapshot, пишет события в promise_log и шлёт TG-алерт собственнику
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
    # Идёт после 12:52 (фиксация срывов в promise_log) и 13:00 (дайджесты
    # менеджерам). Источник — БД (pdz_snapshots + promise_log), МС API не
    # дёргается. Шлёт собственнику ссылку с токеном TTL 24ч.
    scheduler.add_job(
        _pdz_run_and_record,
        CronTrigger(hour=13, minute=5, timezone=MSK),
        args=["pdz_generate_html_1305", pdz_generate_html_job, app, db],
        id="pdz_generate_html_1305",
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

    scheduler.start()
    logger.info("✅ Планировщик запущен")
    for job in scheduler.get_jobs():
        nxt = job.next_run_time.astimezone(MSK).strftime("%Y-%m-%d %H:%M %Z") if job.next_run_time else "?"
        logger.info(f"  job={job.id} next_run={nxt}")

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
    """Cron 14:02 МСК — основная обработка событий обещаний (Фаза 3).

    Шаги:
      a. today  = pdz_take_snapshot()  (свежий API-запрос; мы сразу после 14:00
         и хотим максимально актуальное состояние для сравнения).
      b. yesterday = db.get_last_snapshot_before(today_date).
      c. events = compute_promise_events(today, yesterday) → save_promise_events.
      d. initial_changes = await audit_ppm_initial_changes(today, yesterday).
      e. Для каждого initial_change — TG-сообщение собственнику OWNER_CHAT_ID.

    Сообщения группируются по 10 на одно TG-message (защита от rate-limit).
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _ZI

    logger.info(
        f"pdz_process_events_job стартовала в {_dt.now(MSK):%Y-%m-%d %H:%M %Z}"
    )
    try:
        from moysklad import (
            pdz_take_snapshot,
            compute_promise_events,
            audit_ppm_initial_changes,
        )

        today_rows = await pdz_take_snapshot()
        today_date = _dt.now(_ZI("Europe/Moscow")).date()
        yesterday_rows = db.get_last_snapshot_before(today_date)

        # Сохраняем сегодняшний срез сюда же — чтобы /pdz_events_test был
        # самодостаточным (даже если 14:00-cron почему-то не отработал).
        # Если cron 14:00 уже отписал тот же snap_date — будет дубль, и это
        # нормально (для compute_promise_events используется именно `today_rows`,
        # а get_last_snapshot_before берёт ПРОШЛЫЕ даты). На текущий день
        # дублирующая запись не мешает.
        try:
            inserted_now = db.save_pdz_snapshot(today_rows)
            logger.info(f"pdz_process_events_job: snapshot up-sert {inserted_now} строк")
        except Exception as e:
            logger.warning(f"pdz_process_events_job: повторный save_pdz_snapshot: {e}")

        events = compute_promise_events(today_rows, yesterday_rows)
        saved = db.save_promise_events(events)
        sets = sum(1 for e in events if e["event_type"] == "set")
        moved = sum(1 for e in events if e["event_type"] == "moved")
        broken = sum(1 for e in events if e["event_type"] == "broken")
        logger.info(
            f"pdz_process_events_job: events={len(events)} (set={sets}, moved={moved}, broken={broken}); saved={saved}"
        )

        # ── Фаза 6: эскалация по числу срывов ───────────────────────────
        # Для каждого broken-события считаем число срывов за 90д у этого
        # контрагента и реагируем:
        #   2 → задача менеджеру в МС (через create_task).
        #   3 → set_client_stop_flag(stop_shipments) + TG-алерт собственнику.
        #   4+ → set_client_stop_flag(prepayment_only) + TG-алерт.
        # Дедуп: db.is_pdz_escalation_done(agent_id, level) → не повторять.
        try:
            escalation_summary = await _pdz_escalate_broken_events(app, db, events)
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
            "events_total": len(events),
            "set": sets,
            "moved": moved,
            "broken": broken,
            "initial_changes": len(initial_changes),
            "escalation": escalation_summary,
        }
    except Exception as e:
        logger.error(f"pdz_process_events_job: {e}", exc_info=True)
        return {"events_total": 0, "set": 0, "moved": 0, "broken": 0, "initial_changes": 0, "error": str(e)}


async def _pdz_escalate_broken_events(app: Application, db, events: list) -> dict:
    """Реакция на broken-события (Фаза 6).

    Для каждого уникального agent_id, по которому в `events` есть >=1 broken:
      - получаем breaks_count = db.get_promise_breaks_count за 90д;
      - выбираем максимальный уровень эскалации (2/3/4+), который ещё не
        выполнен (db.is_pdz_escalation_done);
      - выполняем соответствующее действие и помечаем его как done.

    Возвращает сводку (для логирования и теста).
    """
    summary = {
        "tasks_created": 0,
        "stop_shipments": 0,
        "prepayment_only": 0,
        "skipped_done": 0,
        "errors": [],
    }

    # Собираем уникальные клиенты, которые ИМЕННО СЕГОДНЯ сорвали обещание.
    broken_agents: dict = {}  # agent_id → {agent_name, manager_tag, order_id, order_name}
    for ev in events or []:
        if ev.get("event_type") != "broken":
            continue
        aid = ev.get("agent_id")
        if not aid or aid in broken_agents:
            continue
        broken_agents[aid] = {
            "agent_name": ev.get("agent_name"),
            "manager_tag": ev.get("manager_tag"),
            "order_id": ev.get("order_id"),
            "order_name": ev.get("order_name"),
        }

    if not broken_agents:
        return summary

    # breaks_count за 90д батчем для всех затронутых клиентов.
    try:
        breaks_map = db.get_promise_breaks_count(list(broken_agents.keys()), days_window=90)
    except Exception as e:
        logger.error(f"_pdz_escalate_broken_events: get_promise_breaks_count failed: {e}")
        breaks_map = {}

    owner_raw = os.getenv("OWNER_CHAT_ID")
    owner_id = int(owner_raw) if owner_raw else None

    # Импортируем тут, чтобы не плодить циклические импорты при загрузке модуля.
    from moysklad import (
        find_employee_id_by_tag,
        create_task as ms_create_task,
        fmt_money,
        PDZ_MANAGER_TAG_MAP,
    )
    from datetime import timedelta

    for aid, info in broken_agents.items():
        count = int(breaks_map.get(aid, 0))
        if count < 2:
            # 1 срыв = молча по плану.
            continue
        agent_name = info.get("agent_name") or "—"
        manager_tag = (info.get("manager_tag") or "").lower()
        order_id = info.get("order_id")
        order_name = info.get("order_name")

        # Считаем общий долг клиента по последнему снимку — для алерта собственнику.
        total_unpaid = 0.0
        try:
            latest = db.get_latest_snapshot() or []
            for r in latest:
                if (r.get("agent_id") or "") != aid:
                    continue
                bal_raw = r.get("agent_balance")
                try:
                    balance = float(bal_raw) if bal_raw is not None else None
                except Exception:
                    balance = None
                if balance is not None and balance >= 0:
                    continue
                try:
                    t = float(r.get("total_sum") or 0)
                    p = float(r.get("payed_sum") or 0)
                except Exception:
                    t, p = 0.0, 0.0
                if p < t:
                    total_unpaid += (t - p)
        except Exception as e:
            logger.warning(f"_pdz_escalate_broken_events: total_unpaid for {aid} failed: {e}")

        # ── Уровень 2: задача менеджеру в МС ───────────────────────────
        if count >= 2 and not db.is_pdz_escalation_done(aid, 2):
            try:
                assignee_id = await find_employee_id_by_tag(manager_tag)
                if not assignee_id:
                    logger.warning(
                        f"_pdz_escalate_broken_events: не нашли сотрудника МС по тегу '{manager_tag}'"
                        f" (клиент {agent_name}); задача не поставлена"
                    )
                else:
                    due_msk = datetime.now(MSK) + timedelta(days=2)
                    desc = (
                        f"2-й перенос обещания у клиента {agent_name}. "
                        f"Согласовать с собственником при необходимости."
                    )
                    await ms_create_task(assignee_id, desc, due_msk)
                    summary["tasks_created"] += 1
                db.mark_pdz_escalation_done(aid, 2)
            except Exception as e:
                logger.error(
                    f"_pdz_escalate_broken_events: create_task для {agent_name} ({manager_tag}): {e}"
                )
                summary["errors"].append(f"task:{aid}:{e}")
        elif count >= 2 and db.is_pdz_escalation_done(aid, 2):
            summary["skipped_done"] += 1

        # ── Уровень 3: stop_shipments + TG-алерт собственнику ──────────
        if count >= 3 and not db.is_pdz_escalation_done(aid, 3):
            try:
                changed = db.set_client_stop_flag(
                    agent_id=aid,
                    agent_name=agent_name,
                    status="stop_shipments",
                    reason="3-й перенос обещания",
                    set_by="auto",
                )
                if changed:
                    summary["stop_shipments"] += 1
                db.mark_pdz_escalation_done(aid, 3)
                if owner_id:
                    tag_display = PDZ_MANAGER_TAG_MAP.get(manager_tag, manager_tag or "—")
                    safe_name = (agent_name or "—").replace("*", "").replace("_", "")
                    safe_tag = (tag_display or "—").replace("*", "").replace("_", "")
                    safe_aid = (aid or "—").replace("*", "").replace("_", "")
                    text = (
                        f"🚫 *СТОП ОТГРУЗОК:* {safe_name}\n"
                        f"Причина: 3-й перенос обещания\n"
                        f"Менеджер: {safe_tag}\n"
                        f"Долг: {fmt_money(total_unpaid)}\n\n"
                        f"Для снятия: `/snimi_stop {safe_aid}`"
                    )
                    try:
                        await app.bot.send_message(
                            chat_id=owner_id,
                            text=text,
                            parse_mode="Markdown",
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        logger.error(
                            f"_pdz_escalate_broken_events: TG-алерт собственнику (stop): {e}"
                        )
            except Exception as e:
                logger.error(f"_pdz_escalate_broken_events: set_stop_shipments для {agent_name}: {e}")
                summary["errors"].append(f"stop3:{aid}:{e}")

        # ── Уровень 4+: prepayment_only + TG-алерт собственнику ────────
        if count >= 4 and not db.is_pdz_escalation_done(aid, 4):
            try:
                changed = db.set_client_stop_flag(
                    agent_id=aid,
                    agent_name=agent_name,
                    status="prepayment_only",
                    reason=f"{count}-й перенос обещания (4+)",
                    set_by="auto",
                )
                if changed:
                    summary["prepayment_only"] += 1
                db.mark_pdz_escalation_done(aid, 4)
                if owner_id:
                    tag_display = PDZ_MANAGER_TAG_MAP.get(manager_tag, manager_tag or "—")
                    safe_name = (agent_name or "—").replace("*", "").replace("_", "")
                    safe_tag = (tag_display or "—").replace("*", "").replace("_", "")
                    safe_aid = (aid or "—").replace("*", "").replace("_", "")
                    text = (
                        f"🚫 *ТОЛЬКО ПРЕДОПЛАТА:* {safe_name}\n"
                        f"Причина: {count}-й перенос обещания\n"
                        f"Менеджер: {safe_tag}\n"
                        f"Долг: {fmt_money(total_unpaid)}\n\n"
                        f"Для снятия: `/snimi_stop {safe_aid}`"
                    )
                    try:
                        await app.bot.send_message(
                            chat_id=owner_id,
                            text=text,
                            parse_mode="Markdown",
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        logger.error(
                            f"_pdz_escalate_broken_events: TG-алерт собственнику (prepay): {e}"
                        )
            except Exception as e:
                logger.error(f"_pdz_escalate_broken_events: set_prepayment_only для {agent_name}: {e}")
                summary["errors"].append(f"prepay:{aid}:{e}")

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

        messages = pdz_send_manager_digest_text(items, manager_name)
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
    """Cron 16:05 МСК — пинг собственнику по необработанным после 16:00.

    Собирает по менеджерам клиентов, где менеджер не пересогласовал
    `ppm_new` в будущее (см. pdz_unprocessed_for_owner).

    Если у всех всё проставлено — отправляет «✅ Все менеджеры обработали
    просрочки до 16:00».
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
        text = "✅ Все менеджеры обработали просрочки до 16:00"
        try:
            await app.bot.send_message(chat_id=owner_id, text=text)
        except Exception as e:
            logger.error(f"pdz_send_owner_pending_job: send_message OK: {e}")
        return {"status": "all_clear", "managers": 0}

    # Формируем сводку. Группировка по менеджеру, внутри — клиенты по
    # total_unpaid убыванию (уже отсортированы pdz_unprocessed_for_owner).
    lines: list[str] = ["⚠️ *16:00 — не проставлены новые даты:*", ""]

    # Сортируем менеджеров по сумме просрочки (убывание).
    tag_totals = [
        (tag, sum(a["total_unpaid"] for a in agents), agents)
        for tag, agents in by_tag.items()
    ]
    tag_totals.sort(key=lambda x: x[1], reverse=True)

    def _breaks_word(cnt: int) -> str:
        if cnt % 10 == 1 and cnt % 100 != 11:
            return "срыв"
        if cnt % 10 in (2, 3, 4) and cnt % 100 not in (12, 13, 14):
            return "срыва"
        return "срывов"

    for tag, total, agents in tag_totals:
        manager_name = PDZ_MANAGER_TAG_MAP.get(tag, tag)
        # Берём фамилию (первое слово после имени) для заголовка — в плане
        # пример "Скляр — 3 клиента · 280 000 ₽".
        surname = manager_name.split()[-1] if manager_name else tag
        cnt = len(agents)
        word = "клиент" if cnt % 10 == 1 and cnt % 100 != 11 else (
            "клиента" if cnt % 10 in (2, 3, 4) and cnt % 100 not in (12, 13, 14) else "клиентов"
        )
        lines.append(f"*{surname}* — {cnt} {word} · {fmt_money(total)}")
        for a in agents:
            name = (a.get("agent_name") or "—").replace("*", "").replace("_", "")
            url = a.get("ms_url_first_order") or "#"
            breaks = int(a.get("breaks_count", 0) or 0)
            # Фаза 6: префикс стоп-флага.
            stop_status = a.get("stop_status")
            if stop_status == "stop_shipments":
                stop_prefix = "🚫 СТОП "
            elif stop_status == "prepayment_only":
                stop_prefix = "🚫 ПРЕДОПЛАТА "
            else:
                stop_prefix = ""
            prefix = stop_prefix + ("🔴 " if breaks > 0 else "")
            suffix = f" ({breaks} {_breaks_word(breaks)} за 90д)" if breaks > 0 else ""
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
         и promise_log (МС API не дёргается).
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
