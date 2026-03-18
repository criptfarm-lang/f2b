"""
Планировщик задач бота F2B PRO
- Утренняя сводка в 9:00
- Напоминание о дедлайнах в 10:00
- Вечерняя сводка ПДЗ в 17:00 (только если был запущен /pdz)
- ПДЗ по командам через /pdz — НЕ автоматически
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


# Менеджеры ПДЗ — порядок определяет очерёдность отправки (2 мин между каждым)
PDZ_MANAGERS = [
    {"name": "Карина",   "tag": "баласанян"},
    {"name": "Елена",    "tag": "мерзлякова"},
    {"name": "Инесса",   "tag": "скляр"},
    {"name": "Татьяна",  "tag": "голубева"},
    {"name": "Алексей",  "tag": "леонтьев"},
    {"name": "Сергей",   "tag": "черентаев"},
]

# Флаг — был ли запущен /pdz сегодня (для вечерней сводки)
pdz_launched_today: set = set()  # хранит даты когда был запуск

# Хранилище сообщений группы за текущий день ПДЗ
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
        id="remind_today_tasks"
    )

    # 17:00 — вечерняя сводка ПДЗ (только если /pdz был запущен сегодня)
    scheduler.add_job(
        pdz_evening_summary,
        CronTrigger(hour=17, minute=0),
        args=[app],
        id="pdz_evening_summary"
    )

    # 03:00 — очистка старых задач
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


async def pdz_morning_task(app: Application, mgr: dict):
    """Отправляет задачу по ПДЗ конкретному менеджеру. Вызывается из /pdz."""
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
            await app.bot.send_message(
                chat_id=chat_id,
                text=f"✅ *{mgr['name']}* — просроченных долгов нет.",
                parse_mode="Markdown"
            )
            return

        pdz_text = format_overdue_summary(items)
        text = (
            f"📋 *{mgr['name']}*, задача на сегодня:\n\n"
            f"Свяжись с клиентами по просроченной задолженности и напиши "
            f"в группу кто и когда оплатит. Срок — до 17:00.\n\n"
            f"{pdz_text}"
        )

        await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        logger.info(f"pdz_morning_task отправлена для {mgr['name']}")

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
    """В 17:00 анализирует ответы менеджеров и отправляет сводку по ПДЗ."""
    from claude_ai import analyze_pdz_responses
    import asyncio

    today = date.today().isoformat()

    # Отправляем только если /pdz был запущен сегодня
    if today not in pdz_launched_today:
        logger.info("pdz_evening_summary: /pdz не запускался сегодня, пропускаем")
        return

    chat_id = get_group_chat_id()
    if not chat_id:
        return

    day_data = pdz_day_messages.get(today, {})

    try:
        results = {}
        for mgr in PDZ_MANAGERS:
            tag = mgr["tag"]
            items = await get_overdue_demands(tag=tag)
            messages = day_data.get(tag, [])
            messages += day_data.get("_all", [])
            results[mgr["name"]] = {
                "items": items or [],
                "messages": messages,
            }

        summary = await analyze_pdz_responses(results)

        # Отправляем по каждому менеджеру с паузой 2 мин
        for i, mgr in enumerate(PDZ_MANAGERS):
            mgr_name = mgr["name"]
            mgr_result = results.get(mgr_name, {})
            items = mgr_result.get("items") or []
            msgs = mgr_result.get("messages", [])

            if not items:
                continue

            from moysklad import format_overdue_demands
            pdz_text = format_overdue_demands(items)
            msgs_text = "\n".join(f"  — {m}" for m in msgs[-5:]) if msgs else "  — нет ответов"

            text = (
                f"📊 *Итог дня — {mgr_name}*\n\n"
                f"{pdz_text}\n\n"
                f"💬 Ответы за день:\n{msgs_text}"
            )

            await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
            logger.info(f"pdz_evening_summary отправлена для {mgr_name}")

            if i < len(PDZ_MANAGERS) - 1:
                await asyncio.sleep(120)  # 2 минуты между менеджерами

    except Exception as e:
        logger.error(f"Ошибка pdz_evening_summary: {e}", exc_info=True)


def cleanup_done_tasks():
    """Удаляет выполненные задачи старше 24 часов."""
    try:
        from database import Database
        db = Database()
        db.cleanup_done_tasks()
        logger.info("cleanup_done_tasks: старые выполненные задачи удалены")
    except Exception as e:
        logger.error(f"cleanup_done_tasks: {e}")
