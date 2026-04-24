"""
Планировщик задач бота F2B PRO
- Утренняя сводка в 9:00
- Напоминание о дедлайнах в 10:00
- Сводка ПДЗ вызывается вручную кнопкой "Результат ПДЗ" или командой /pdz_results
- ПДЗ по командам через /pdz — НЕ автоматически
"""

import logging
import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

from telegram.ext import Application
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from claude_ai import generate_morning_summary
from moysklad import get_overdue_demands, format_overdue_summary

logger = logging.getLogger(__name__)

MSK = ZoneInfo("Europe/Moscow")

def get_group_ids():
    raw = os.getenv("GROUP_CHAT_IDS", "")
    return [int(x.strip()) for x in raw.split(",") if x.strip()]

def get_group_chat_id():
    val = os.getenv("GROUP_CHAT_ID", "")
    return int(val) if val else None

# Менеджеры ПДЗ — порядок определяет очерёдность отправки (2 мин между каждым)
# chat_id подтягивается из БД (менеджер пишет /mychatid боту)
PDZ_MANAGERS = [
    {"name": "Карина",   "tag": "баласанян",  "name_fragment": "Баласанян"},
    {"name": "Елена",    "tag": "мерзлякова", "name_fragment": "Мерзлякова"},
    {"name": "Инесса",   "tag": "скляр",      "name_fragment": "Скляр"},
    {"name": "Алексей",  "tag": "леонтьев",   "name_fragment": "Леонтьев"},
    {"name": "Денис",    "tag": "коликов",    "name_fragment": "Коликов"},
]

# Флаг — был ли запущен /pdz сегодня (для вечерней сводки)
pdz_launched_today: set = set()  # хранит даты когда был запуск

# Хранилище сообщений группы за текущий день ПДЗ
pdz_day_messages: dict = {}

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

async def pdz_morning_task(app: Application, mgr: dict):
    """
    Отправляет ПДЗ по менеджеру в группу.
    Затем шлёт менеджеру в личку задачу проработать дебиторку.
    """
    chat_id = get_group_chat_id()
    if not chat_id:
        return

    today = date.today().isoformat()
    if today not in pdz_day_messages:
        pdz_day_messages[today] = {}
    if mgr["tag"] not in pdz_day_messages[today]:
        pdz_day_messages[today][mgr["tag"]] = []

    try:
        items = await get_overdue_demands(tag=mgr["tag"])

        if not items:
            logger.info(f"pdz_morning_task: нет просрочки у {mgr['name']}")
            # В личку — нет долгов
            mgr_chat_id = mgr.get("chat_id")
            if mgr_chat_id:
                try:
                    await app.bot.send_message(
                        chat_id=mgr_chat_id,
                        text=f"✅ {mgr['name']}, у твоих клиентов сегодня нет просроченной дебиторки. Хорошая работа!"
                    )
                except Exception as e:
                    logger.warning(f"Не удалось написать {mgr['name']} в личку: {e}")
            return

        pdz_text = format_overdue_summary(items)

        # 1. Отправляем ПДЗ в группу
        group_msg = await app.bot.send_message(
            chat_id=chat_id,
            text=(
                f"📋 *Дебиторка — {mgr['name']}*\n\n"
                f"{pdz_text}"
            ),
            parse_mode="Markdown"
        )
        logger.info(f"pdz_morning_task: ПДЗ {mgr['name']} отправлена в группу")

        # 2. Пишем менеджеру в личку
        mgr_chat_id = mgr.get("chat_id")
        if mgr_chat_id:
            try:
                await app.bot.send_message(
                    chat_id=mgr_chat_id,
                    text=(
                        f"📋 *{mgr['name']}, задача по дебиторке*\n\n"
                        f"Твоя просроченная дебиторка опубликована в группе.\n"
                        f"Свяжись с каждым клиентом и пиши результаты *мне в личку* — "
                        f"кто и когда оплатит.\n\n"
                        f"Срок отчёта — до 17:00.\n\n"
                        f"Пример: _«Атмосфера — оплатит в пятницу 21.03»_"
                    ),
                    parse_mode="Markdown"
                )
                logger.info(f"pdz_morning_task: личка {mgr['name']} отправлена")
            except Exception as e:
                logger.warning(f"Не удалось написать {mgr['name']} в личку: {e}")

    except Exception as e:
        logger.error(f"Ошибка pdz_morning_task для {mgr['name']}: {e}", exc_info=True)

def record_group_message(sender_name: str, tag: str, text: str):
    """Записывает сообщение из группы в хранилище текущего дня ПДЗ."""
    today = date.today().isoformat()
    if today not in pdz_day_messages:
        pdz_day_messages[today] = {}
    if tag not in pdz_day_messages[today]:
        pdz_day_messages[today][tag] = []
    pdz_day_messages[today][tag].append(f"{sender_name}: {text}")

async def pdz_evening_summary(app: Application):
    """В 17:00 собирает результаты из БД и отправляет сводку по каждому менеджеру."""
    import asyncio

    today = date.today().isoformat()

    if today not in pdz_launched_today:
        logger.info("pdz_evening_summary: /pdz не запускался сегодня, пропускаем")
        return

    chat_id = get_group_chat_id()
    if not chat_id:
        return

    try:
        from database import Database
        db_local = Database()
        results = db_local.get_pdz_results_today()

        # Группируем по менеджеру
        by_manager = {}
        for r in results:
            name = r.get("manager_name", "Неизвестный")
            if name not in by_manager:
                by_manager[name] = []
            by_manager[name].append(r.get("result_text", ""))

        # Отправляем по каждому менеджеру с паузой 2 мин
        for i, mgr in enumerate(PDZ_MANAGERS):
            mgr_name = mgr["name"]
            # Ищем результаты по фрагменту имени
            mgr_results = []
            frag = mgr.get("name_fragment", mgr_name).lower()
            for full_name, msgs in by_manager.items():
                if frag in full_name.lower():
                    mgr_results = msgs
                    break

            # Текущая просрочка
            items = await get_overdue_demands(tag=mgr["tag"])
            from moysklad import format_overdue_demands
            pdz_text = format_overdue_demands(items) if items else "✅ Просрочек нет"

            if mgr_results:
                results_text = "\n".join(f"  — {r}" for r in mgr_results)
            else:
                results_text = "  — ответов не поступало"

            text = (
                f"📊 *Итог дня — {mgr_name}*\n\n"
                f"{pdz_text}\n\n"
                f"💬 *Результаты работы:*\n{results_text}"
            )

            await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
            logger.info(f"pdz_evening_summary отправлена для {mgr_name}")

            if i < len(PDZ_MANAGERS) - 1:
                await asyncio.sleep(120)

    except Exception as e:
        logger.error(f"Ошибка pdz_evening_summary: {e}", exc_info=True)

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
            "леонтьев": "Алексей Леонтьев",
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
