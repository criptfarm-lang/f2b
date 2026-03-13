"""
Планировщик задач бота F2B PRO
- Утренняя сводка в 9:00
- Напоминание о дедлайнах в 10:00
- Вечерний срез по дебиторке в 18:00
- Пн/Ср: утренние задачи по ПДЗ менеджерам в 9:30–9:38
- Пн/Ср: вечерняя сводка по ПДЗ в 16:00
"""

import logging
import os
from datetime import date

from telegram.ext import Application
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from claude_ai import generate_morning_summary
from moysklad import get_overdue_demands, format_overdue_summary

logger = logging.getLogger(__name__)


def get_group_ids():
    raw = os.getenv("GROUP_CHAT_IDS", "")
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def get_group_chat_id():
    val = os.getenv("GROUP_CHAT_ID", "")
    return int(val) if val else None


# Менеджеры ПДЗ: имя, тег МойСклад, время отправки (мск)
PDZ_MANAGERS = [
    {"name": "Карина",  "tag": "баласанян",  "hour": 9, "minute": 30},
    {"name": "Елена",   "tag": "мерзлякова", "hour": 9, "minute": 32},
    {"name": "Инесса",  "tag": "скляр",      "hour": 9, "minute": 34},
    {"name": "Татьяна", "tag": "голубева",   "hour": 9, "minute": 36},
    {"name": "Алексей", "tag": "леонтьев",   "hour": 9, "minute": 38},
]

# Хранилище сообщений группы за текущий день ПДЗ (в памяти)
# { "2026-03-10": { "баласанян": ["Карина: ИП Орехов оплатит 12.03", ...], ... } }
pdz_day_messages: dict = {}


def setup_scheduler(app: Application, db):
    """Настраивает и запускает все запланированные задачи."""
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

    # 09:00 — утренняя сводка
    scheduler.add_job(
        morning_summary,
        CronTrigger(hour=9, minute=0),
        args=[app, db],
        id="morning_summary"
    )

    # 10:00 — напоминание о задачах на сегодня
    scheduler.add_job(
        remind_today_tasks,
        CronTrigger(hour=10, minute=0),
        args=[app, db],
        id="remind_today"
    )

    # Пн/Ср — утренние задачи по ПДЗ каждому менеджеру
    for mgr in PDZ_MANAGERS:
        scheduler.add_job(
            pdz_morning_task,
            CronTrigger(day_of_week="mon,wed", hour=mgr["hour"], minute=mgr["minute"]),
            args=[app, mgr],
            id=f"pdz_task_{mgr['tag']}"
        )

    # Пн/Ср 16:00 — вечерняя сводка по ПДЗ
    scheduler.add_job(
        pdz_evening_summary,
        CronTrigger(day_of_week="mon,wed", hour=16, minute=0),
        args=[app],
        id="pdz_evening_summary"
    )

    scheduler.add_job(
        cleanup_done_tasks,
        CronTrigger(hour=3, minute=0),
        id="cleanup_done_tasks"
    )

    scheduler.start()
    logger.info("✅ Планировщик запущен")


async def morning_summary(app: Application, db):
    """Отправляет утреннюю сводку в группы."""
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


async def registry_reminder(app: Application, db):
    """Напоминает Беляковой прислать реестр."""
    group_ids = get_group_ids()
    if not group_ids:
        return

    text = (
        "📋 *Белякова Александра*, не забудь:\n"
        "Согласуй реестр и отправь в склад ✅"
    )

    for chat_id in group_ids:
        try:
            await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Ошибка registry_reminder в {chat_id}: {e}")


async def pdz_morning_task(app: Application, mgr: dict):
    """Отправляет задачу по ПДЗ конкретному менеджеру в группу."""
    chat_id = get_group_chat_id()
    if not chat_id:
        logger.warning("GROUP_CHAT_ID не задан, пропускаем pdz_morning_task")
        return

    today = date.today().isoformat()

    # Инициализируем хранилище на сегодня
    if today not in pdz_day_messages:
        pdz_day_messages[today] = {}
    if mgr["tag"] not in pdz_day_messages[today]:
        pdz_day_messages[today][mgr["tag"]] = []

    try:
        # Получаем просрочку по менеджеру
        items = await get_overdue_demands(tag=mgr["tag"])
        if not items:
            logger.info(f"pdz_morning_task: нет просрочки у {mgr['name']}")
            return

        pdz_text = format_overdue_summary(items)

        text = (
            f"📋 *{mgr['name']}*, задача на сегодня:\n\n"
            f"Свяжись с клиентами по просроченной задолженности и напиши "
            f"в группу кто и когда оплатит. Срок — до 16:00.\n\n"
            f"{pdz_text}"
        )

        await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        logger.info(f"pdz_morning_task отправлена для {mgr['name']}")

    except Exception as e:
        logger.error(f"Ошибка pdz_morning_task для {mgr['name']}: {e}", exc_info=True)


def record_group_message(sender_name: str, tag: str, text: str):
    """
    Записывает сообщение из группы в хранилище текущего дня ПДЗ.
    Вызывается из bot.py при каждом сообщении в группе в дни ПДЗ.
    """
    today = date.today().isoformat()
    if today not in pdz_day_messages:
        pdz_day_messages[today] = {}
    if tag not in pdz_day_messages[today]:
        pdz_day_messages[today][tag] = []
    pdz_day_messages[today][tag].append(f"{sender_name}: {text}")


async def pdz_evening_summary(app: Application):
    """В 16:00 анализирует переписку группы и отправляет сводку по ПДЗ."""
    from claude_ai import analyze_pdz_responses

    chat_id = get_group_chat_id()
    if not chat_id:
        logger.warning("GROUP_CHAT_ID не задан, пропускаем pdz_evening_summary")
        return

    today = date.today().isoformat()
    day_data = pdz_day_messages.get(today, {})

    try:
        # Для каждого менеджера получаем актуальный список клиентов
        results = {}
        for mgr in PDZ_MANAGERS:
            tag = mgr["tag"]
            items = await get_overdue_demands(tag=tag)
            messages = day_data.get(tag, [])
            results[mgr["name"]] = {
                "items": items,
                "messages": messages,
            }

        summary = await analyze_pdz_responses(results)

        await app.bot.send_message(
            chat_id=chat_id,
            text=f"📊 *Сводка по работе с ПДЗ*\n\n{summary}",
            parse_mode="Markdown"
        )
        logger.info("pdz_evening_summary отправлена")

    except Exception as e:
        logger.error(f"Ошибка pdz_evening_summary: {e}", exc_info=True)


def cleanup_done_tasks():
    """Удаляет выполненные задачи старше 24 часов. Запускается в 3:00."""
    try:
        from database import Database
        db = Database()
        db.cleanup_done_tasks()
        logger.info("cleanup_done_tasks: старые выполненные задачи удалены")
    except Exception as e:
        logger.error(f"cleanup_done_tasks: {e}")
