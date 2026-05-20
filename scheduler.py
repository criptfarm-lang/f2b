"""
Планировщик задач бота F2B PRO
- Утренняя сводка в 9:00
- Напоминание о дедлайнах в 10:00
- Стареющие клиенты в 12:00
"""

import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from telegram.ext import Application
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

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
    scheduler.add_job(
        pdz_take_snapshot_job,
        CronTrigger(hour=13, minute=55, timezone=MSK),
        args=[app, db],
        id="pdz_snapshot_1355"
    )
    scheduler.add_job(
        pdz_take_snapshot_job,
        CronTrigger(hour=14, minute=0, timezone=MSK),
        args=[app, db],
        id="pdz_snapshot_1400"
    )

    # 14:02 МСК — обработка событий обещаний (Фаза 3). Между snapshot 14:00
    # и дайджестом менеджерам 14:10. Сравнивает сегодняшний и вчерашний
    # snapshot, пишет события в promise_log и шлёт TG-алерт собственнику
    # при изменении ppm_initial («Дата планируемой оплаты», менять нельзя).
    scheduler.add_job(
        pdz_process_events_job,
        CronTrigger(hour=14, minute=2, timezone=MSK),
        args=[app, db],
        id="pdz_process_events_1402"
    )

    scheduler.start()
    logger.info("✅ Планировщик запущен")
    for job in scheduler.get_jobs():
        nxt = job.next_run_time.astimezone(MSK).strftime("%Y-%m-%d %H:%M %Z") if job.next_run_time else "?"
        logger.info(f"  job={job.id} next_run={nxt}")

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
        }
    except Exception as e:
        logger.error(f"pdz_process_events_job: {e}", exc_info=True)
        return {"events_total": 0, "set": 0, "moved": 0, "broken": 0, "initial_changes": 0, "error": str(e)}
