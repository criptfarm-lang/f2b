"""
F2B PRO — Telegram Bot
Ассистент отдела продаж: задачи, фото, прайсы, дебиторка
"""

import asyncio
import logging
import os
import re
from datetime import datetime

from telegram import Update, Message
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    TypeHandler,
    ContextTypes,
    filters,
)

from database import Database
from scheduler import setup_scheduler, record_group_message, PDZ_MANAGERS, get_group_chat_id
from claude_ai import dispatch, smart_answer, extract_tasks_from_message, detect_task_completion, parse_product_query
from amocrm import find_contacts_for_broadcast, broadcast_to_leads, check_connection as amo_check
from moysklad import (search_products, search_products_filtered, get_price_list, format_products,
    format_price_list, get_product_image, download_image, get_image_download_url,
    get_counterparty_balance, get_all_debtors, format_debtors_ms, format_counterparty_balance,
    find_counterparty_info, format_counterparty_info,
    get_debtors_by_tag, get_clients_by_tag, resolve_tag,
    format_debtors_by_tag, format_clients_by_tag,
    get_overdue_demands, format_overdue_demands, format_overdue_summary,
    format_reminders_for_manager, format_debt_reminder, fmt_money)

# ─── Словарь сотрудников — варианты имён и склонений ─────────────────────────
EMPLOYEES = {
    "Белякова Александра": [
        "александра", "александры", "александре", "александру",
        "белякова", "беляковой", "белякову",
        "саша", "саши", "саше", "сашу",
    ],
    "Алексей Леонтьев": [
        "алексей", "алексея", "алексею", "алексеем",
        "леонтьев", "леонтьева", "леонтьеву",
        "лёша", "лёши", "лёше", "леша", "леши", "лёшу",
    ],
    "Ярослав": [
        "ярослав", "ярослава", "ярославу", "ярославом",
        "ярик", "ярика", "ярику",
    ],
    "Андрей Иванов": [
        "андрей", "андрея", "андрею", "андреем",
        "иванов", "иванова", "иванову",
    ],
    "Инесса Скляр": [
        "инесса", "инессы", "инессе", "инессу", "инессой",
        "скляр",
    ],
    "Маланчук Александр": [
        "маланчук", "маланчука", "маланчуку",
    ],
    "Карина Баласанян": [
        "карина", "карины", "карине", "карину", "кариной",
        "баласанян",
    ],
    "Елена Мерзлякова": [
        "елена", "елены", "елене", "елену", "еленой",
        "мерзлякова", "мерзляковой", "мерзлякову",
        "марзлякова", "марзляковой",
        "лена", "лены", "лене", "лену", "леной",
    ],
    "Татьяна Голубева": [
        "татьяна", "татьяны", "татьяне", "татьяну", "татьяной",
        "голубева", "голубевой", "голубеву",
        "таня", "тани", "тане", "таню", "таней",
    ],
}

def find_employee(query: str) -> str | None:
    """Ищет сотрудника по любому варианту имени/фамилии в запросе."""
    query_lower = query.lower()
    # Сначала ищем точное совпадение слова
    for full_name, variants in EMPLOYEES.items():
        for variant in variants:
            # Проверяем что вариант встречается как отдельное слово
            import re as _re
            if _re.search(r"\b" + _re.escape(variant) + r"\b", query_lower):
                return full_name
    return None



# ─── Логирование ────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Инициализация БД ────────────────────────────────────────────────────────
db = Database()

# ─── Определяем обращение к боту ─────────────────────────────────────────────
BOT_TRIGGERS = ["эф,", "эф ", "бот,", "бот ", "@эф", "bot,", "bot ", "@f2b_assistant_bot", "@f2b_assistant"]


def is_bot_addressed(text: str) -> bool:
    """Проверяет, обращаются ли к боту."""
    if not text:
        return False
    text_lower = text.lower().strip()
    # Реагируем на обращение в начале или @mention в любом месте
    if any(text_lower.startswith(t) for t in BOT_TRIGGERS):
        return True
    # @mention может быть в любом месте сообщения
    if "@f2b_assistant" in text_lower or "эф," in text_lower or text_lower.startswith("эф "):
        return True
    return False


def clean_query(text: str) -> str:
    """Убирает обращение к боту из текста."""
    text_lower = text.lower()
    for trigger in BOT_TRIGGERS:
        if text_lower.startswith(trigger):
            return text[len(trigger):].strip()
    return text.strip()


# ─── Команды ─────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Привет! Я Эф — ассистент F2B PRO.\n\n"
        "Обращайся ко мне: *Эф, [вопрос]*\n\n"
        "Примеры:\n"
        "• Эф, пришли фото тунца\n"
        "• Эф, какая цена на лосось?\n"
        "• Эф, задачи Карины\n"
        "• Эф, кто нам должен?\n\n"
        "Команды:\n"
        "/tasks — мои задачи\n"
        "/report — отчёт\n"
        "/help — все команды",
        parse_mode="Markdown"
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📋 *Все команды:*\n\n"
        "*Задачи:*\n"
        "/tasks — мои задачи\n"
        "/all_tasks — все задачи команды\n"
        "/overdue — просроченные задачи\n\n"
        "*Отчёты:*\n"
        "/report — недельный отчёт\n"
        "/дебиторка — срез по дебиторке\n\n"
        "*База знаний:*\n"
        "/фото [товар] — найти фото\n"
        "/прайс — актуальный прайс\n"
        "/контакт [имя] — найти контакт\n\n"
        "*Обращение в свободной форме:*\n"
        "бот, [любой вопрос]",
        parse_mode="Markdown"
    )


async def cmd_my_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает задачи текущего пользователя."""
    user = update.effective_user
    name = user.full_name
    tasks = db.get_tasks_for_user(name)

    if not tasks:
        await update.message.reply_text(f"✅ {name}, у тебя нет открытых задач!")
        return

    lines = [f"📋 *Задачи для {name}:*\n"]
    for t in tasks:
        deadline_str = f" — до {t['deadline']}" if t.get('deadline') else ""
        status_icon = "🔴" if t.get('overdue') else "🟡"
        lines.append(f"{status_icon} {t['text']}{deadline_str}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_all_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Все открытые задачи команды."""
    tasks = db.get_all_open_tasks()
    if not tasks:
        await update.message.reply_text("✅ Нет открытых задач!")
        return

    # Группируем по исполнителю
    by_user = {}
    for t in tasks:
        exe = t.get('executor', 'Неизвестно')
        by_user.setdefault(exe, []).append(t)

    lines = ["📋 *Все открытые задачи:*\n"]
    for user, utasks in by_user.items():
        lines.append(f"*{user}* ({len(utasks)}):")
        for t in utasks:
            icon = "🔴" if t.get('overdue') else "🟡"
            deadline_str = f" [{t['deadline']}]" if t.get('deadline') else ""
            lines.append(f"  {icon} {t['text']}{deadline_str}")
        lines.append("")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_overdue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просроченные задачи."""
    tasks = db.get_overdue_tasks()
    if not tasks:
        await update.message.reply_text("✅ Просроченных задач нет!")
        return

    lines = [f"🔴 *Просроченные задачи ({len(tasks)}):*\n"]
    for t in tasks:
        lines.append(f"• *{t.get('executor', '?')}*: {t['text']} [срок: {t.get('deadline', '?')}]")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Недельный отчёт по команде."""
    stats = db.get_weekly_stats()

    lines = ["📊 *Отчёт за неделю:*\n"]
    for user, s in stats.items():
        pct = int(s['done'] / s['total'] * 100) if s['total'] > 0 else 0
        bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
        lines.append(
            f"*{user}*\n"
            f"  {bar} {pct}%\n"
            f"  ✅ {s['done']} выполнено  🔴 {s['overdue']} просрочено  📋 {s['total']} всего\n"
        )

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_debtors(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Срез по дебиторке."""
    debtors = db.get_debtors()
    if not debtors:
        await update.message.reply_text("✅ Просроченной дебиторки нет!")
        return

    lines = ["💰 *Дебиторка — требуют внимания:*\n"]
    for d in debtors:
        lines.append(f"• {d['client']} → *{d['manager']}* [{d['days']} дн.]")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Поиск фото по команде /фото [товар]."""
    query = " ".join(context.args) if context.args else ""
    if not query:
        await update.message.reply_text("Укажи товар: /photo тунец")
        return
    await search_and_send_photo(update, context, query)


async def cmd_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Последний актуальный прайс."""
    price = db.get_latest_price()
    if price:
        await update.message.reply_document(
            document=price['file_id'],
            caption=f"📄 Прайс от {price['date']}"
        )
    else:
        await update.message.reply_text("Прайс пока не загружен в базу. Скинь прайс в чат и я его сохраню!")


async def cmd_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Поиск контакта."""
    query = " ".join(context.args) if context.args else ""
    if not query:
        await update.message.reply_text("Укажи имя: /contact Малахов")
        return

    contacts = db.search_contacts(query)
    if not contacts:
        await update.message.reply_text(f"Контакт '{query}' не найден в базе.")
        return

    lines = [f"📞 *Контакты по запросу '{query}':*\n"]
    for c in contacts:
        lines.append(f"• *{c['name']}* — {c['phone']} ({c.get('company', '')})")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ─── Обработка обычных сообщений ──────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главный обработчик всех сообщений."""
    message = update.message
    if not message:
        return

    chat_id = message.chat_id
    user = message.from_user
    text = message.text or message.caption or ""

    # Сохраняем все сообщения в историю чата
    if text and user:
        db.save_message(
            chat_id=chat_id,
            user_id=user.id,
            user_name=user.full_name,
            text=text[:1000],  # обрезаем очень длинные
        )


    # Записываем сообщения группы для анализа ПДЗ (пн/ср)
    group_chat_id = get_group_chat_id()
    if text and user and group_chat_id and chat_id == group_chat_id:
        from datetime import date as _date
        if _date.today().weekday() in (0, 2):  # 0=пн, 2=ср
            sender_lower = user.full_name.lower()
            matched = False
            for mgr in PDZ_MANAGERS:
                if mgr["name"].lower() in sender_lower or mgr["tag"] in sender_lower:
                    record_group_message(user.full_name, mgr["tag"], text)
                    matched = True
                    break
            if not matched:
                record_group_message(user.full_name, "_all", text)
    # 1. Сохраняем документы в базу (фото берём из МойСклад)
    if message.document:
        fname = message.document.file_name or ""
        if any(fname.lower().endswith(ext) for ext in [".pdf", ".xlsx", ".xls", ".docx"]):
            await save_media(message, "document")
            # Если это прайс — помечаем отдельно
            if any(w in fname.lower() for w in ["прайс", "price", "price-list"]):
                db.save_price(
                    file_id=message.document.file_id,
                    filename=fname,
                    chat_id=chat_id,
                    uploader=user.full_name
                )
                await message.reply_text("✅ Прайс сохранён в базу!")

    if not text:
        return

    # 2. Автоматическое извлечение задач (анализируем ВСЕ сообщения руководителя)
    # Список ID руководителей — добавь в .env
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]

    # Логируем ID для диагностики
    logger.info(f"Message from user.id={user.id}, name={user.full_name}, chat_id={message.chat_id}, manager_ids={manager_ids}")

    if user.id in manager_ids and len(text) > 15:
        # Не обрабатываем обращения к боту как задачи
        if not is_bot_addressed(text):
            tasks = await extract_tasks_from_message(text, user.full_name)
            saved_count = 0
            task_lines = []
            for task in tasks:
                if task.get("task"):
                    db.save_task(
                        text=task["task"],
                        executor=task.get("executor", ""),
                        deadline=task.get("deadline"),
                        source_chat=chat_id,
                        source_message_id=message.message_id,
                        created_by=user.full_name
                    )
                    saved_count += 1
                    executor = task.get("executor", "—")
                    deadline = task.get("deadline")
                    deadline_str = f" · до {deadline}" if deadline else ""
                    task_lines.append(f"👤 *{executor}*{deadline_str}: {task['task']}")
                    logger.info(f"Задача: {executor} → {task['task']}")

            if saved_count > 0:
                lines = [f"📌 Зафиксировано задач: {saved_count}\n"] + task_lines
                await message.reply_text("\n".join(lines), parse_mode="Markdown")

    # 3. Автозакрытие задач — Claude анализирует контекст без списков слов
    if not is_bot_addressed(text) and len(text) > 5:
        open_tasks = db.get_all_open_tasks()
        if open_tasks:
            completed_ids = await detect_task_completion(text, open_tasks)
            if completed_ids:
                closed = []
                for task_id in completed_ids:
                    task = next((t for t in open_tasks if t['id'] == task_id), None)
                    if task:
                        db.complete_task(task_id)
                        executor = task.get('executor', '')
                        closed.append(f"✅ *{executor}*: {task['text']}")
                        logger.info(f"Автозакрытие задачи {task_id}: {task['text']}")
                if closed:
                    lines = ["🤖 Эф зафиксировал выполнение:\n"] + closed
                    await message.reply_text("\n".join(lines), parse_mode="Markdown")

    # 4. Реагируем на обращение к боту
    # Автоматически реагируем на IT-проблемы даже без обращения "Эф,"
    IT_KEYWORDS = [
        "телеграм не", "telegram не", "амо не", "амосрм", "amocrm",
        "crm не", "срм не", "не отправляется", "не загружается",
        "не проходят звонки", "звонки не", "почта не", "не приходит письмо",
        "не работает телеграм", "не работает амо", "не работает crm",
        "слетела интеграция", "нет сообщений в амо", "не открывается амо",
    ]
    text_lower_it = text.lower()
    if not is_bot_addressed(text) and any(kw in text_lower_it for kw in IT_KEYWORDS):
        await message.reply_text(
            "По техническим вопросам (Telegram, amoCRM, звонки, почта) "
            "пишите в группу **IT8 & ОП ФИШ ТУ БИЗНЕС** 🛠",
            parse_mode="Markdown"
        )
        return

    # Проверяем подтверждение рассылки
    if text.lower().strip() in ("да, рассылай", "да рассылай", "рассылай", "подтверждаю"):
        pending = context.user_data.get("pending_broadcast")
        if pending:
            lead_ids = pending["lead_ids"]
            broadcast_text = pending["text"]
            product = pending["product"]
            count = pending["count"]
            context.user_data.pop("pending_broadcast", None)

            await message.reply_text(
                f"🚀 Начинаю рассылку по *{product}*\n"
                f"📨 {count} получателей · ~{count} мин\n"
                f"Отчёт пришлю по завершении.",
                parse_mode="Markdown"
            )

            async def run_broadcast():
                from amocrm import broadcast_to_leads as _broadcast
                stats = await _broadcast(lead_ids, broadcast_text, delay_seconds=60)
                result_text = (
                    f"✅ *Рассылка завершена!*\n"
                    f"📨 Отправлено: {stats['sent']}/{count}\n"
                )
                if stats["failed"]:
                    result_text += f"❌ Ошибок: {stats['failed']}\n"
                await context.bot.send_message(chat_id=chat_id, text=result_text, parse_mode="Markdown")

            asyncio.create_task(run_broadcast())
            return

    if not is_bot_addressed(text):
        return
    query_lower = query.lower()

    # ── Всё через Claude — он сам разбирается что нужно ──
    await message.reply_chat_action("typing")
    context_data = db.get_context_summary()
    chat_history = db.format_history(chat_id, limit=40)
    memories = db.format_memories()

    logger.info(f"Dispatching query='{query}' from '{user.full_name}'")
    result = await dispatch(query, user.full_name, context_data,
                            chat_history=chat_history, memories=memories)
    logger.info(f"Dispatch result: {result}")
    action = result.get("action", "answer")
    params = result.get("params", {})

    if action == "get_tasks":
        employee = params.get("employee")
        if employee:
            tasks = db.get_tasks_for_user(employee)
            if not tasks:
                await message.reply_text(f"✅ У *{employee}* нет открытых задач.", parse_mode="Markdown")
            else:
                lines = [f"📋 *Задачи — {employee}:*\n"]
                for t in tasks:
                    deadline_str = f" — до {t['deadline']}" if t.get("deadline") else ""
                    icon = "🔴" if t.get("overdue") else "🟡"
                    lines.append(f"{icon} {t['text']}{deadline_str}")
                await message.reply_text("\n".join(lines), parse_mode="Markdown")
        else:
            await cmd_my_tasks(update, context)

    elif action == "get_all_tasks":
        await cmd_all_tasks(update, context)

    elif action == "get_report":
        await cmd_report(update, context)

    elif action == "get_debtors":
        await message.reply_chat_action("typing")
        debtors = await get_all_debtors()
        text = format_debtors_ms(debtors)
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "get_debt":
        debt_query = params.get("query", "")
        await message.reply_chat_action("typing")
        counterparties = await get_counterparty_balance(debt_query)
        text = format_counterparty_balance(counterparties, debt_query)
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "find_counterparty":
        cp_query = params.get("query", "")
        await message.reply_chat_action("typing")
        counterparties = await find_counterparty_info(cp_query)
        text = format_counterparty_info(counterparties, cp_query)
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "get_group_debts":
        raw_tag = params.get("tag", "")
        tag = resolve_tag(raw_tag)
        await message.reply_chat_action("typing")
        items = await get_debtors_by_tag(tag)
        text = format_debtors_by_tag(items, tag)
        if len(text) > 4000:
            text = text[:3900] + "\n\n_...уточни запрос_"
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "get_group_clients":
        raw_tag = params.get("tag", "")
        tag = resolve_tag(raw_tag)
        await message.reply_chat_action("typing")
        items = await get_clients_by_tag(tag)
        text = format_clients_by_tag(items, tag)
        if len(text) > 4000:
            text = text[:3900] + "\n\n_...слишком много, уточни_"
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "get_overdue_debt":
        raw_tag = params.get("tag", "")
        raw_query = params.get("query", "")
        brief = params.get("brief", False)
        tag = resolve_tag(raw_tag) if raw_tag else None
        await message.reply_chat_action("typing")
        items = await get_overdue_demands(tag=tag, query=raw_query)
        label = raw_query or (tag or None)
        if brief and not raw_query:
            text = format_overdue_summary(items)
        else:
            text = format_overdue_demands(items, tag=label)
        if len(text) > 4000:
            text = text[:3900] + "\n\n_...уточни запрос_"
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "prepare_reminders":
        USER_MANAGER_TAGS = {
            "карина": "баласанян", "баласанян": "баласанян",
            "инесса": "скляр", "скляр": "скляр",
            "елена": "мерзлякова", "мерзлякова": "мерзлякова",
            "татьяна": "голубева", "голубева": "голубева",
            "алексей": "леонтьев", "леонтьев": "леонтьев",
        }
        USER_MANAGER_DISPLAY = {
            "баласанян": "Карина Баласанян",
            "скляр": "Инесса Скляр",
            "мерзлякова": "Елена Мерзлякова",
            "голубева": "Татьяна Голубева",
            "леонтьев": "Алексей Леонтьев",
        }
        full_name_lower = user.full_name.lower()
        manager_tag = None
        manager_display = user.full_name
        for key, tag in USER_MANAGER_TAGS.items():
            if key in full_name_lower:
                manager_tag = tag
                manager_display = USER_MANAGER_DISPLAY.get(tag, user.full_name)
                break
        # Руководитель может указать тег явно
        if not manager_tag and params.get("tag"):
            manager_tag = resolve_tag(params["tag"])
            manager_display = USER_MANAGER_DISPLAY.get(manager_tag, params["tag"].capitalize())
        raw_query = params.get("query", "")
        await message.reply_chat_action("typing")
        items = await get_overdue_demands(tag=manager_tag, query=raw_query)
        if not items:
            await message.reply_text("✅ Просроченных клиентов нет — напоминания не нужны.")
        else:
            header = (
                f"📋 *Напоминания об оплате — {manager_display}*\n"
                f"{len(items)} клиентов · скопируй и отправь каждому"
            )
            await message.reply_text(header, parse_mode="Markdown")
            for c in sorted(items, key=lambda x: x["overdue_sum"], reverse=True):
                reminder = format_debt_reminder(c)
                label = f"💬 {c['name']} — {fmt_money(c['overdue_sum'])}\n\n{reminder}"
                await message.reply_text(label)

    elif action == "find_photo":
        photo_query = params.get("query", query)
        await search_and_send_photo(update, context, photo_query)

    elif action == "get_price":
        # Сначала пробуем МойСклад
        ms_token = os.getenv("MOYSKLAD_TOKEN")
        if ms_token:
            await message.reply_chat_action("typing")
            products = await get_price_list(limit=50)
            if products:
                text = format_price_list(products)
                # Telegram ограничивает 4096 символов
                if len(text) > 4000:
                    text = text[:3900] + "\n\n_...показаны первые позиции_"
                await message.reply_text(text, parse_mode="Markdown")
                return
        await cmd_price(update, context)

    elif action == "ms_search":
        # Поиск товара в МойСклад — Claude разбирает запрос на фильтры
        ms_query = params.get("query", query)
        await message.reply_chat_action("typing")
        parsed = await parse_product_query(ms_query)
        logger.info(f"parse_product_query result: {parsed}")
        
        # Принудительный in_stock если пользователь явно спросил "в наличии" / "есть на складе"
        stock_keywords = ["в наличии", "на складе", "есть ли", "что есть", "имеется"]
        if any(kw in ms_query.lower() for kw in stock_keywords):
            parsed.setdefault("filters", {})["in_stock"] = True
            logger.info("Forced in_stock=True based on query keywords")
        
        products = await search_products_filtered(parsed)
        if not products:
            products = await search_products(ms_query)
        text = format_products(products, ms_query)
        if len(text) > 4000:
            text = text[:3900] + "\n\n_...слишком много результатов, уточни запрос_"
        await message.reply_text(text, parse_mode="Markdown")

        # Если один товар и есть фото — пробуем прислать
        if len(products) == 1 and products[0].get("image_href"):
            try:
                img_bytes = await download_image(products[0]["image_href"])
                if img_bytes:
                    import io as _io
                    await message.reply_photo(
                        photo=_io.BytesIO(img_bytes),
                        caption=f"📸 {products[0]['name']}"
                    )
            except Exception as e:
                logger.warning(f"Не удалось отправить фото из МойСклад: {e}")

    elif action == "broadcast":
        product = params.get("product", "")
        broadcast_text = params.get("message", "")

        if not product or not broadcast_text:
            await message.reply_text("❌ Не указан товар или текст сообщения.")
            return

        await message.reply_chat_action("typing")
        await message.reply_text(f"🔍 Ищу клиентов которые покупали *{product}* в МойСклад...", parse_mode="Markdown")

        # 1. Находим контрагентов в МойСклад по товару
        from moysklad import get_counterparties_by_product
        counterparty_names = await get_counterparties_by_product(product)

        if not counterparty_names:
            await message.reply_text(f"❌ Не найдено клиентов покупавших *{product}* в МойСклад.", parse_mode="Markdown")
            return

        await message.reply_text(f"📋 Найдено {len(counterparty_names)} клиентов в МойСклад.\n🔍 Ищу их в amoCRM...", parse_mode="Markdown")

        # 2. Находим их в amoCRM
        contacts = await find_contacts_for_broadcast(counterparty_names)

        if not contacts:
            await message.reply_text(
                f"❌ Клиенты найдены в МойСклад, но не найдены в amoCRM.\n"
                f"Клиенты МойСклад: {', '.join(counterparty_names[:5])}{'...' if len(counterparty_names) > 5 else ''}",
                parse_mode="Markdown"
            )
            return

        lead_ids = [c["lead_id"] for c in contacts]
        duration_min = len(lead_ids)

        # 3. Показываем список и просим подтверждение
        names_preview = "\n".join(f"• {c['amo_name']}" for c in contacts[:10])
        if len(contacts) > 10:
            names_preview += f"\n_...и ещё {len(contacts) - 10}_"

        confirm_text = (
            f"📣 *Рассылка готова*\n\n"
            f"*Товар:* {product}\n"
            f"*Текст:* _{broadcast_text}_\n\n"
            f"*Получатели ({len(contacts)}):*\n{names_preview}\n\n"
            f"⏱ Рассылка займёт ~{duration_min} мин (1 сообщение в минуту)\n\n"
            f"Для подтверждения напиши: *да, рассылай*"
        )
        await message.reply_text(confirm_text, parse_mode="Markdown")

        # Сохраняем данные рассылки в память бота для подтверждения
        context.user_data["pending_broadcast"] = {
            "lead_ids": lead_ids,
            "text": broadcast_text,
            "product": product,
            "count": len(lead_ids),
        }

    elif action == "find_contact":
        contact_query = params.get("query", "")
        contacts = db.search_contacts(contact_query)
        if contacts:
            lines = [f"📞 *{c['name']}* — {c['phone']} ({c.get('company', '')})" for c in contacts]
            await message.reply_text("\n".join(lines), parse_mode="Markdown")
        else:
            await message.reply_text(f"Контакт '{contact_query}' не найден в базе.")

    elif action == "answer":
        text = params.get("text")
        if text:
            await message.reply_text(text)
        else:
            # Claude не дал готовый ответ — спрашиваем отдельно
            response = await smart_answer(query, user.full_name, context_data)
            await message.reply_text(response)

    else:
        # Неизвестное действие — текстовый ответ
        response = await smart_answer(query, user.full_name, context_data)
        await message.reply_text(response)


async def save_media(message: Message, media_type: str):
    """Сохраняет фото/документ в базу с тегами из подписи."""
    caption = message.caption or ""
    chat_id = message.chat_id
    user = message.from_user.full_name if message.from_user else "unknown"

    if media_type == "photo":
        file_id = message.photo[-1].file_id  # берём наибольшее разрешение
        if not caption:
            # Уведомляем что фото сохранено без тега
            await message.reply_text(
                "Фото сохранено в базу без подписи.\n"
                "Чтобы его можно было найти, напиши следующим сообщением название товара — например: форель трим С",
            )
    else:
        file_id = message.document.file_id
        caption = caption or message.document.file_name or ""

    db.save_media(
        file_id=file_id,
        media_type=media_type,
        caption=caption,
        chat_id=chat_id,
        uploader=user,
        date=datetime.now().isoformat()
    )


async def search_photo_in_content_channel(context: ContextTypes.DEFAULT_TYPE, query: str) -> list:
    """Ищет фото в канале Контент F2B по ключевым словам в подписи.
    Возвращает список (file_id, caption) подходящих фото.
    """
    content_chat_id = int(os.getenv("CONTENT_CHAT_ID", "-1001433042091"))
    query_lower = query.lower()
    results = []

    # Ищем в локальной БД (фото из канала сохраняются при поступлении)
    photos = db.search_media(query_lower, media_type="photo")
    for p in photos:
        if p.get("chat_id") == content_chat_id:
            results.append({"file_id": p["file_id"], "caption": p.get("caption", "")})

    return results


async def search_and_send_photo(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str):
    """Ищет фото товара: сначала в канале Контент, потом в МойСклад."""
    import io as _io
    await update.message.reply_chat_action("upload_photo")

    # 1. Ищем в канале Контент F2B
    content_photos = await search_photo_in_content_channel(context, query)
    if content_photos:
        sent = 0
        for p in content_photos[:3]:
            try:
                await update.message.reply_photo(
                    photo=p["file_id"],
                    caption=f"📸 {p['caption']}" if p["caption"] else f"📸 {query}"
                )
                sent += 1
            except Exception as e:
                logger.warning(f"Не удалось отправить фото из Контент: {e}")
        if sent > 0:
            return

    # 2. Fallback — ищем в МойСклад
    products = await search_products(query)
    with_photo = [p for p in products if p.get("image_href")]

    if not with_photo:
        if products:
            text = format_products(products, query)
            await update.message.reply_text(
                f"😕 Фото не найдено, но вот что есть:\n\n{text}",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(f"😕 Фото '{query}' не найдено ни в канале Контент, ни в МойСклад.")
        return

    sent = 0
    for product in with_photo[:3]:
        try:
            img_bytes = await download_image(product["image_href"])
            if img_bytes:
                name = product.get("name", query)
                price = product.get("sale_price", 0)
                stock = product.get("stock", 0)
                caption = f"📸 {name}"
                if price:
                    caption += f"\n💰 {price} руб/кг"
                if stock and stock > 0:
                    caption += f"\n📦 В наличии: {stock} кг"
                await update.message.reply_photo(
                    photo=_io.BytesIO(img_bytes),
                    caption=caption
                )
                sent += 1
        except Exception as e:
            logger.warning(f"Не удалось отправить фото {product.get('name')}: {e}")

    if sent == 0:
        await update.message.reply_text(f"😕 Фото '{query}' есть в МойСклад, но не удалось загрузить.")


async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает посты из канала Контент F2B — сохраняет фото в БД."""
    message = update.channel_post
    if not message:
        return

    # Логируем chat_id для диагностики (можно удалить после настройки)
    logger.info(f"channel_post from chat_id={message.chat_id}, title='{message.chat.title}', caption='{message.caption or message.text or ''}'")

    content_chat_id = int(os.getenv("CONTENT_CHAT_ID", "-1001433042091"))
    if message.chat_id != content_chat_id:
        logger.info(f"channel_post: chat_id {message.chat_id} != CONTENT_CHAT_ID {content_chat_id}, пропускаем")
        return

    caption = message.caption or message.text or ""

    if message.photo:
        file_id = message.photo[-1].file_id
        db.save_media(
            file_id=file_id,
            media_type="photo",
            caption=caption,
            chat_id=message.chat_id,
            uploader="Контент F2B",
            date=datetime.now().isoformat()
        )
        logger.info(f"Сохранено фото из канала Контент: '{caption}' file_id={file_id}")

    elif message.document and message.document.mime_type and message.document.mime_type.startswith("image/"):
        file_id = message.document.file_id
        db.save_media(
            file_id=file_id,
            media_type="photo",
            caption=caption or message.document.file_name or "",
            chat_id=message.chat_id,
            uploader="Контент F2B",
            date=datetime.now().isoformat()
        )
        logger.info(f"Сохранено фото-документ из канала Контент: '{caption}'")


# ─── Запуск ──────────────────────────────────────────────────────────────────

async def cmd_memory(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает что Эф помнит."""
    memories = db.get_all_memories()
    if not memories:
        await update.message.reply_text("🧠 Долгосрочная память пуста.")
        return
    lines = ["🧠 *Что я помню:*\n"]
    for m in memories[:20]:
        lines.append(f"• *{m['key']}*: {m['value']}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_remember(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Явно запомнить факт: /remember ключ: значение"""
    args = " ".join(context.args) if context.args else ""
    if ":" not in args:
        await update.message.reply_text(
            "Формат: /remember ключ: значение\n"
            "Например: /remember скидка Иванову: 5%"
        )
        return
    key, value = args.split(":", 1)
    db.remember(key.strip(), value.strip())
    await update.message.reply_text(f"✅ Запомнил: *{key.strip()}* → {value.strip()}", parse_mode="Markdown")


async def cmd_pdz_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестовый запуск утренних задач ПДЗ. /pdz_test [имя|all]"""
    user = update.message.from_user
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]
    if user.id not in manager_ids:
        await update.message.reply_text("⛔ Только для руководителей.")
        return

    arg = (context.args[0].lower() if context.args else "all")

    from scheduler import pdz_morning_task, PDZ_MANAGERS
    app = update.get_bot()  # используем контекст

    targets = PDZ_MANAGERS if arg == "all" else [
        m for m in PDZ_MANAGERS if m["name"].lower() == arg or m["tag"] == arg
    ]

    if not targets:
        names = ", ".join(m["name"].lower() for m in PDZ_MANAGERS)
        await update.message.reply_text(f"Не найдено. Варианты: all, {names}")
        return

    await update.message.reply_text(
        f"🧪 Запускаю тест ПДЗ для: {', '.join(m['name'] for m in targets)}..."
    )

    for mgr in targets:
        try:
            await pdz_morning_task(context.application, mgr)
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка для {mgr['name']}: {e}")


async def cmd_pdz_evening_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестовый запуск вечерней сводки ПДЗ. /pdz_evening"""
    user = update.message.from_user
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]
    if user.id not in manager_ids:
        await update.message.reply_text("⛔ Только для руководителей.")
        return

    await update.message.reply_text("🧪 Запускаю тест вечерней сводки ПДЗ...")
    from scheduler import pdz_evening_summary
    try:
        await pdz_evening_summary(context.application)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("Не задан TELEGRAM_BOT_TOKEN в переменных окружения!")

    app = Application.builder().token(token).build()

    # Команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("tasks", cmd_my_tasks))
    app.add_handler(CommandHandler("all_tasks", cmd_all_tasks))
    app.add_handler(CommandHandler("overdue", cmd_overdue))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("debts", cmd_debtors))
    app.add_handler(CommandHandler("photo", cmd_photo))
    app.add_handler(CommandHandler("price", cmd_price))
    app.add_handler(CommandHandler("contact", cmd_contact))

    # Все сообщения (текст + медиа)
    app.add_handler(CommandHandler("memory", cmd_memory))
    app.add_handler(CommandHandler("remember", cmd_remember))
    app.add_handler(CommandHandler("memory", cmd_memory))
    app.add_handler(CommandHandler("remember", cmd_remember))
    app.add_handler(CommandHandler("pdz_test", cmd_pdz_test))
    app.add_handler(CommandHandler("pdz_evening", cmd_pdz_evening_test))
    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POSTS, handle_channel_post))
    app.add_handler(MessageHandler(filters.ALL & ~filters.UpdateType.CHANNEL_POSTS, handle_message))

    # Планировщик (утренние сводки, напоминания)
    setup_scheduler(app, db)

    logger.info("🤖 Бот запущен!")
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "channel_post", "edited_message", "edited_channel_post"]
    )


if __name__ == "__main__":
    main()
