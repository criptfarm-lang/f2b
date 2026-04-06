"""
F2B PRO — Telegram Bot
Ассистент отдела продаж: задачи, фото, прайсы, дебиторка
"""

import asyncio
import logging
import os
import re
from datetime import datetime

from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    TypeHandler,
    ContextTypes,
    filters,
)

from database import Database
from scheduler import setup_scheduler, record_group_message, PDZ_MANAGERS, get_group_chat_id
from claude_ai import dispatch, smart_answer, extract_tasks_from_message, detect_task_completion, parse_product_query
from amocrm import check_connection as amo_check  # оставляем для совместимости
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
}

# Менеджеры отдела продаж — "всем менеджерам"
MOP_MANAGERS = [
    "Карина Баласанян",
    "Елена Мерзлякова",
    "Инесса Скляр",
    "Алексей Леонтьев",
]

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

# ─── Квиз — URL сервиса викторины ────────────────────────────────────────────
QUIZ_BASE_URL = os.getenv("QUIZ_BASE_URL", "")  # https://викторина-xxxx.up.railway.app

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
    user = update.effective_user
    chat_id = update.effective_chat.id
    if user and chat_id == user.id:
        db.save_manager_chat_id(user.id, user.full_name)
        logger.info(f"cmd_start: сохранён chat_id={chat_id} name={user.full_name}")
    await update.message.reply_text(
        f"👋 Привет, *{user.full_name if user else 'друг'}*! Я Эф — ассистент F2B PRO.\n\n"
        f"Используй меню ниже или обращайся: *Эф, [вопрос]*",
        parse_mode="Markdown",
        reply_markup=_user_menu_keyboard()
    )

def _user_menu_keyboard() -> InlineKeyboardMarkup:
    """Общее меню для всех пользователей."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📸 Запросить фото товара", callback_data="user_photo"),
            InlineKeyboardButton("💰 ПДЗ клиента", callback_data="user_pdz_client"),
        ],
        [
            InlineKeyboardButton("📄 Сформировать договор", callback_data="user_contract"),
            InlineKeyboardButton("📋 Мои задачи", callback_data="user_my_tasks"),
        ],
        [
            InlineKeyboardButton("📊 Отчёт ОП", callback_data="user_op_report"),
        ],
    ])

async def cmd_user_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/usermenu — общее меню."""
    await update.message.reply_text(
        "Выбери действие:",
        reply_markup=_user_menu_keyboard()
    )

async def handle_user_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик общего меню пользователей."""
    query = update.callback_query
    await query.answer()
    user = query.from_user
    action = query.data

    if action == "user_photo":
        await query.message.reply_text(
            "📸 Напиши название товара — пришлю фото.\n"
            "Например: _форель охл трим С_",
            parse_mode="Markdown"
        )
        _user_awaiting[query.from_user.id] = "photo"

    elif action == "user_pdz_client":
        await query.message.reply_text(
            "💰 Напиши название клиента — покажу его дебиторку.\n"
            "Например: _Атмосфера_ или _ИТФИШ_",
            parse_mode="Markdown"
        )
        _user_awaiting[query.from_user.id] = "pdz_client"

    elif action == "user_contract":
        await query.message.reply_text(
            "📄 Напиши название компании — сформирую договор поставки.\n"
            "Например: _Атмосфера_ или _ИТФИШ_",
            parse_mode="Markdown"
        )
        _user_awaiting[query.from_user.id] = "contract"

    elif action == "user_op_report":
        await cmd_op_report(update, context)

    elif action == "user_my_tasks":
        tasks = db.get_tasks_by_executor(user.full_name)
        if not tasks:
            await query.message.reply_text("✅ У тебя нет открытых задач.")
        else:
            lines = [f"📋 *Твои задачи ({len(tasks)}):*\n"]
            for t in tasks:
                deadline = t.get("deadline")
                if deadline:
                    from datetime import date as _d
                    MONTHS = ["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"]
                    try:
                        d = _d.fromisoformat(str(deadline)[:10])
                        dl = f" · до {d.day} {MONTHS[d.month-1]}"
                    except Exception:
                        dl = f" · до {deadline}"
                else:
                    dl = ""
                lines.append(f"• {t.get('text','')}{dl}")
            await query.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_mychatid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает chat_id пользователя."""
    user = update.effective_user
    chat_id = update.effective_chat.id
    if user and chat_id == user.id:
        db.save_manager_chat_id(user.id, user.full_name)
    await update.message.reply_text(
        f"👤 *{user.full_name if user else 'Неизвестный'}*\n"
        f"Твой chat_id: `{chat_id}`",
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
        "*Управление:*\n"
        "/menu — панель управления\n"
        "/pdz — запустить работу с дебиторкой\n"
        "/mychatid — мой chat ID\n"
        "/clearall — очистить открытые задачи\n"
        "/cleartasksall — очистить все задачи",
        parse_mode="Markdown"
    )

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Панель управления — только для руководителя."""
    user = update.effective_user
    if not user or user.id != 360092495:
        # Для остальных — общее меню
        await update.message.reply_text(
            "Выбери действие:",
            reply_markup=_user_menu_keyboard()
        )
        return

    keyboard = InlineKeyboardMarkup([
        # ── Доступно всем ──────────────────────────────────────────
        [InlineKeyboardButton("── Общие функции ──", callback_data="menu_noop")],
        [
            InlineKeyboardButton("📸 Фото товара", callback_data="user_photo"),
            InlineKeyboardButton("💰 ПДЗ клиента", callback_data="user_pdz_client"),
        ],
        [
            InlineKeyboardButton("📄 Сформировать договор", callback_data="user_contract"),
            InlineKeyboardButton("📋 Мои задачи", callback_data="user_my_tasks"),
        ],
        [
            InlineKeyboardButton("📊 Отчёт ОП", callback_data="menu_evening"),
        ],
        # ── Только руководитель ────────────────────────────────────
        [InlineKeyboardButton("── Только для меня ──", callback_data="menu_noop")],
        [
            InlineKeyboardButton("📋 Все задачи", callback_data="menu_all_tasks"),
            InlineKeyboardButton("⏰ Просроченные", callback_data="menu_overdue"),
        ],
        [
            InlineKeyboardButton("🚀 Запустить /pdz", callback_data="menu_pdz_run"),
            InlineKeyboardButton("📊 Результат ПДЗ", callback_data="menu_pdz_results"),
        ],
        [
            InlineKeyboardButton("📈 Активность", callback_data="menu_activity"),
            InlineKeyboardButton("⏳ Стареющие", callback_data="menu_aging"),
        ],
        [
            InlineKeyboardButton("📊 Статистика бота", callback_data="menu_stats"),
            InlineKeyboardButton("🔍 Диагностика", callback_data="menu_test"),
        ],
        [
            InlineKeyboardButton("📢 Промо хорека", callback_data="menu_promo_horeka"),
            InlineKeyboardButton("📢 Промо опт", callback_data="menu_promo_opt"),
        ],
        [
            InlineKeyboardButton("💣 Очистить ВСЕ", callback_data="menu_clearall"),
        ],
    ])

    await update.message.reply_text(
        "🎛 *Панель управления Эф*\n\nВыбери действие:",
        parse_mode="Markdown",
        reply_markup=keyboard
    )

async def handle_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопок панели управления."""
    query = update.callback_query
    await query.answer()

    if not query.from_user or query.from_user.id != 360092495:
        await query.answer("⛔ Только для руководителя.", show_alert=True)
        return

    action = query.data

    if action == "menu_noop":
        return  # разделитель — ничего не делаем

    elif action == "user_photo":
        await query.message.reply_text(
            "📸 Напиши название товара — пришлю фото.\nНапример: _форель охл трим С_",
            parse_mode="Markdown"
        )
        _user_awaiting[query.from_user.id] = "photo"

    elif action == "user_pdz_client":
        await query.message.reply_text(
            "💰 Напиши название клиента — покажу его дебиторку.\nНапример: _Атмосфера_",
            parse_mode="Markdown"
        )
        _user_awaiting[query.from_user.id] = "pdz_client"

    elif action == "menu_all_tasks":
        from database import Database
        tasks = db.get_all_open_tasks()
        if not tasks:
            await query.message.reply_text("✅ Открытых задач нет.")
        else:
            lines = [f"📋 *Все открытые задачи ({len(tasks)}):*\n"]
            for t in tasks[:30]:
                lines.append(f"• *{t.get('executor','')}*: {t.get('text','')[:60]}")
            await query.message.reply_text("\n".join(lines), parse_mode="Markdown")

    elif action == "menu_overdue":
        tasks = db.get_overdue_tasks()
        if not tasks:
            await query.message.reply_text("✅ Просроченных задач нет.")
        else:
            lines = [f"⏰ *Просроченные задачи ({len(tasks)}):*\n"]
            for t in tasks[:20]:
                lines.append(f"• *{t.get('executor','')}*: {t.get('text','')[:60]}")
            await query.message.reply_text("\n".join(lines), parse_mode="Markdown")

    elif action == "menu_pdz_all":
        await query.message.reply_text("⏳ Запрашиваю ПДЗ...")
        from moysklad import get_overdue_demands
        items = await get_overdue_demands()
        if not items:
            await query.message.reply_text("✅ Просроченных долгов нет.")
        else:
            text = format_overdue_demands(items)
            await query.message.reply_text(text, parse_mode="Markdown")

    elif action == "menu_pdz_run":
        await query.message.reply_text(
            "🚀 Запускаю ПДЗ задачи?\nЭто разошлёт задачи всем менеджерам.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Да, запустить", callback_data="menu_pdz_confirm"),
                InlineKeyboardButton("❌ Отмена", callback_data="menu_cancel"),
            ]])
        )

    elif action == "menu_pdz_confirm":
        import asyncio
        from scheduler import pdz_morning_task, pdz_launched_today
        from datetime import date as _date
        today = _date.today().isoformat()
        pdz_launched_today.add(today)
        await query.message.reply_text(f"📋 Запускаю для {len(PDZ_MANAGERS)} менеджеров (2 мин интервал)...")
        for i, mgr in enumerate(PDZ_MANAGERS):
            try:
                await pdz_morning_task(context.application, mgr)
            except Exception as e:
                await query.message.reply_text(f"❌ {mgr['name']}: {e}")
            if i < len(PDZ_MANAGERS) - 1:
                await asyncio.sleep(120)
        await query.message.reply_text("✅ Готово. Сводка придёт в 17:00.")

    elif action == "menu_pdz_results":
        await cmd_pdz_results(update, context)

    elif action == "menu_evening":
        await cmd_op_report(update, context)

    elif action == "menu_promo_horeka":
        current = db.get_promo("хорека")
        await query.message.reply_text(
            f"📢 *Промо хорека:*\n\n{current or '— не задан'}\n\n"
            f"Чтобы изменить: `/set_promo хорека [текст]`",
            parse_mode="Markdown"
        )

    elif action == "menu_promo_opt":
        current = db.get_promo("опт")
        await query.message.reply_text(
            f"📢 *Промо опт:*\n\n{current or '— не задан'}\n\n"
            f"Чтобы изменить: `/set_promo опт [текст]`",
            parse_mode="Markdown"
        )

    elif action == "menu_aging":
        await cmd_aging(update, context)

    elif action == "menu_activity":
        await query.message.reply_text(
            "📊 Активность менеджеров:\nНапиши *Эф, активность за 7 дней*",
            parse_mode="Markdown"
        )

    elif action == "menu_report":
        await query.message.reply_text("⏳ Формирую отчёт...")
        context2 = db.get_context_summary()
        from claude_ai import smart_answer
        text = await smart_answer("Дай краткий недельный отчёт по задачам команды", context2)
        await query.message.reply_text(text, parse_mode="Markdown")

    elif action == "menu_clearopen":
        await query.message.reply_text(
            "🗑 Очистить все ОТКРЫТЫЕ задачи?",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Да", callback_data="menu_clearopen_confirm"),
                InlineKeyboardButton("❌ Нет", callback_data="menu_cancel"),
            ]])
        )

    elif action == "menu_clearopen_confirm":
        db._ensure_connection()
        with db.conn.cursor() as cur:
            cur.execute("UPDATE tasks SET status='done', completed_at=NOW(), result='Удалено руководителем' WHERE status='open'")
            count = cur.rowcount
        db.conn.commit()
        await query.message.reply_text(f"✅ Очищено открытых задач: {count}")

    elif action == "menu_clearall":
        await query.message.reply_text(
            "💣 Удалить ВСЕ задачи включая выполненные?\nЭто нельзя отменить!",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("💣 Да, удалить всё", callback_data="menu_clearall_confirm"),
                InlineKeyboardButton("❌ Отмена", callback_data="menu_cancel"),
            ]])
        )

    elif action == "menu_clearall_confirm":
        db._ensure_connection()
        with db.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM tasks")
            row = cur.fetchone()
            total = row["cnt"] if row else 0
            cur.execute("TRUNCATE TABLE tasks RESTART IDENTITY")
        db.conn.commit()
        await query.message.reply_text(f"💣 Удалено задач: {total}. Таблица очищена.")

    elif action == "menu_test":
        await query.message.reply_text("🔍 Запускаю диагностику...")
        fake_update = update
        await cmd_test(fake_update, context)

    elif action == "menu_contacts":
        lines = ["📞 *Контакты менеджеров:*\n"]
        for name, contact in MANAGERS_CONTACTS.items():
            lines.append(f"• {name}: {contact}")
        await query.message.reply_text("\n".join(lines), parse_mode="Markdown")

    elif action == "menu_stats":
        stats = db.get_usage_stats()
        if not stats:
            await query.message.reply_text("📭 Статистики пока нет.")
        else:
            lines = ["📊 *Статистика использования бота*\n"]
            for s in stats:
                blocked = " 🔒" if s.get("is_blocked") else ""
                last = s.get("last_seen")
                last_str = last.strftime("%d.%m %H:%M") if last else "—"
                lines.append(
                    f"👤 *{s.get('full_name','?')}*{blocked}\n"
                    f"   `{s.get('user_id')}` · {s.get('request_count',0)} зап. · {last_str}"
                )
            await query.message.reply_text("\n".join(lines), parse_mode="Markdown")

    elif action == "menu_block":
        stats = db.get_usage_stats()
        if not stats:
            await query.message.reply_text("Нет пользователей.")
            return
        buttons = []
        for s in stats:
            if s.get("user_id") == 360092495:
                continue
            status = "🔒" if s.get("is_blocked") else "✅"
            cb = f"menu_toggleblock|{s['user_id']}"
            buttons.append([InlineKeyboardButton(
                f"{status} {s.get('full_name','?')} ({s.get('request_count',0)} зап.)",
                callback_data=cb
            )])
        buttons.append([InlineKeyboardButton("◀️ Назад", callback_data="menu_cancel")])
        await query.message.reply_text(
            "🔒 *Управление доступом*\nНажми на пользователя чтобы заблокировать/разблокировать:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif action.startswith("menu_toggleblock|"):
        uid = int(action.split("|")[1])
        row = db._fetchone("SELECT full_name, is_blocked FROM manager_chats WHERE user_id=%s", (uid,))
        if row:
            if row.get("is_blocked"):
                db.unblock_user(uid)
                await query.answer(f"🔓 {row['full_name']} разблокирован")
            else:
                db.block_user(uid)
                await query.answer(f"🔒 {row['full_name']} заблокирован")
        await query.message.delete()

    elif action == "menu_cancel":
        await query.message.delete()

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

# Кэш уже отправленных уведомлений об идентификации — chat_id → True
_wazzup_notified: set = set()

_pending_links: dict = {}
_pending_task_results: dict = {}  # user_id → {task_id, task_text, executor}
_user_awaiting: dict = {}  # user_id → "photo" | "pdz_client"
_pending_price_comments: dict = {}  # manager_user_id → {alert_id, order_id, mgr_name}

async def handle_wazzup_ignore_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Помечает контакт как 'не наш клиент' — больше не присылать уведомления."""
    query = update.callback_query
    await query.answer()
    parts = query.data.split("|")
    chat_id_val = parts[1] if len(parts) > 1 else ""
    if chat_id_val:
        db.link_wazzup_contact(
            chat_id=chat_id_val,
            chat_type="telegram",
            channel_id="",
            company_name="__ignore__",
            wazzup_name="",
            role="игнор",
        )
    await query.message.edit_text("🚫 Контакт помечен как 'не наш клиент'. Уведомления больше не придут.")

async def handle_wazzup_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатие кнопки привязки Telegram контакта к компании."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split("|")

    # Выбор компании из списка похожих: wazzup_pick|index|link_key
    if parts[0] == "wazzup_pick":
        idx = int(parts[1])
        link_key = parts[2]
        pending = _pending_links.get(link_key) or _pending_links.get(query.from_user.id)
        if not pending:
            await query.message.edit_text("❌ Сессия истекла, попробуй снова.")
            return
        suggestions = pending.get("suggestions", [])
        if idx >= len(suggestions):
            await query.message.edit_text("❌ Ошибка выбора, попробуй снова.")
            return
        cp_name = suggestions[idx]
        _pending_links.pop(link_key, None)
        db.delete_pending_link(link_key)
        _pending_links.pop(query.from_user.id, None)
        ok = db.link_wazzup_contact(
            chat_id=pending["chat_id"],
            chat_type=pending["chat_type"],
            channel_id=pending["channel_id"],
            company_name=cp_name,
            wazzup_name=pending["wazzup_name"],
            role="рассылка",
        )
        if ok:
            _wazzup_notified.discard(pending["chat_id"])
            try:
                from moysklad import find_counterparty_info
                cp_list = await find_counterparty_info(cp_name)
                if cp_list:
                    cp_data = cp_list[0]
                    db.update_wazzup_contact_tags(
                        chat_id=pending["chat_id"],
                        tags=cp_data.get("tags", []),
                        manager=cp_data.get("manager", ""),
                        segment=cp_data.get("buyer_type", ""),
                    )
            except Exception as e:
                logger.warning(f"Теги МойСклад: {e}")
            await query.message.edit_text(
                f"✅ *{pending['wazzup_name']}* → *{cp_name}*\nЭф запомнил!",
                parse_mode="Markdown"
            )
            await asyncio.sleep(3)
            try:
                await query.message.delete()
            except Exception:
                pass
        return

    # Выбор сегмента: wazzup_seg|сегмент|link_key
    if parts[0] == "wazzup_seg":
        segment = parts[1]
        link_key = parts[2]
        pending = _pending_links.get(link_key)
        if not pending:
            for uid, v in _pending_links.items():
                if isinstance(uid, int) and v.get("link_key") == link_key:
                    pending = v
                    break
        if not pending:
            await query.message.edit_text("❌ Сессия истекла, попробуй снова.")
            return
        pending["segment"] = segment
        # Спрашиваем менеджера
        MANAGERS = ["Баласанян К.", "Леонтьев А.", "Мерзлякова Е.", "Скляр И.", "Иванов А."]
        buttons = [[InlineKeyboardButton(m, callback_data=f"wazzup_mgr|{m}|{link_key}")] for m in MANAGERS]
        await query.message.edit_text(
            f"✅ Сегмент: *{segment}*\n\nОтветственный менеджер?",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

    # Выбор менеджера: wazzup_mgr|менеджер|link_key
    if parts[0] == "wazzup_mgr":
        manager = parts[1]
        link_key = parts[2]
        pending = _pending_links.get(link_key)
        if not pending:
            for uid, v in _pending_links.items():
                if isinstance(uid, int) and v.get("link_key") == link_key:
                    pending = v
                    break
        if not pending or "company_name" not in pending:
            await query.message.edit_text("❌ Сессия истекла, попробуй снова.")
            return
        # Очищаем
        _pending_links.pop(link_key, None)
        db.delete_pending_link(link_key)
        for uid in [k for k, v in list(_pending_links.items()) if isinstance(k, int) and v.get("link_key") == link_key]:
            _pending_links.pop(uid, None)
        ok = db.link_wazzup_contact(
            chat_id=pending["chat_id"],
            chat_type=pending["chat_type"],
            channel_id=pending["channel_id"],
            company_name=pending["company_name"],
            wazzup_name=pending["wazzup_name"],
            role=role,
        )
        if ok:
            # Подтягиваем теги из МойСклад
            try:
                from moysklad import find_counterparty_info
                cp_list = await find_counterparty_info(pending["company_name"])
                if cp_list:
                    cp = cp_list[0]
                    tags = cp.get("tags", [])
                    manager_tag = cp.get("manager", "")
                    buyer_type = cp.get("buyer_type", "")
                    db.update_wazzup_contact_tags(
                        chat_id=pending["chat_id"],
                        tags=tags,
                        manager=manager_tag,
                        segment=buyer_type,
                    )
            except Exception as e:
                logger.warning(f"Не удалось подтянуть теги из МойСклад: {e}")
        if ok:
            _wazzup_notified.discard(pending["chat_id"])
            await query.message.edit_text(
                f"✅ *{pending['wazzup_name']}* → *{pending['company_name']}* ({role})\nЭф запомнил!",
                parse_mode="Markdown"
            )
            await asyncio.sleep(3)
            try:
                await query.message.delete()
            except Exception:
                pass
        return

    # Выбор роли: wazzup_role|роль|link_key
    if parts[0] == "wazzup_role":
        role = parts[1]
        link_key = parts[2]

        # Отмена
        if role == "отмена":
            pending_data = _pending_links.get(link_key, {})
            _wazzup_notified.discard(pending_data.get("chat_id", ""))
            _pending_links.pop(link_key, None)
            db.delete_pending_link(link_key)
            for uid in [k for k, v in list(_pending_links.items()) if v.get("link_key") == link_key]:
                _pending_links.pop(uid, None)
            await query.message.delete()
            return
        pending = _pending_links.get(link_key)
        # Если не нашли по link_key — ищем по user_id среди всех pending
        if not pending or "company_name" not in pending:
            for uid, v in _pending_links.items():
                if isinstance(uid, int) and v.get("link_key") == link_key and "company_name" in v:
                    pending = v
                    break
        if not pending or "company_name" not in pending:
            await query.message.edit_text("❌ Сессия истекла, попробуй снова.")
            return
        # После выбора роли — спрашиваем сегмент
        pending["role"] = role
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🍣 Хорека", callback_data=f"wazzup_seg|хорека|{link_key}"),
            InlineKeyboardButton("📦 Опт", callback_data=f"wazzup_seg|опт|{link_key}"),
            InlineKeyboardButton("🚚 Поставщик", callback_data=f"wazzup_seg|поставщик|{link_key}"),
        ]])
        await query.message.edit_text(
            f"✅ Роль: *{role}*\n\nСегмент клиента?",
            parse_mode="Markdown",
            reply_markup=keyboard
        )
        return

    # Первое нажатие — запрашиваем название компании: wazzup_link|link_key
    if len(parts) < 2:
        return
    link_key = parts[1]
    pending = _pending_links.get(link_key)
    if not pending:
        await query.message.edit_text("❌ Сессия истекла, попробуй снова.")
        return

    # Помечаем что ждём ввода от этого пользователя — добавляем в стек
    if query.from_user.id not in _pending_links:
        _pending_links[query.from_user.id] = []
    # Если уже есть такой link_key — не дублируем
    existing_keys = [p.get("link_key") for p in _pending_links[query.from_user.id]] if isinstance(_pending_links.get(query.from_user.id), list) else []
    if link_key not in existing_keys:
        entry = {**pending, "link_key": link_key}
        if isinstance(_pending_links[query.from_user.id], list):
            _pending_links[query.from_user.id].append(entry)
        else:
            _pending_links[query.from_user.id] = [entry]
    _pending_links[link_key] = {**pending, "link_key": link_key}

    # Берём последний ожидающий контакт
    current = _pending_links[query.from_user.id][-1] if isinstance(_pending_links[query.from_user.id], list) else _pending_links[query.from_user.id]

    await query.message.edit_text(
        f"👤 Контакт: *{current['wazzup_name']}*\n\n"
        f"Как этот клиент называется в МойСклад?\n"
        f"_(напиши название или часть названия)_",
        parse_mode="Markdown"
    )

async def cmd_clear_wazzup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет запись из wazzup_contact_map по chat_id. Только для руководителя."""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return
    chat_id_val = context.args[0] if context.args else ""
    if not chat_id_val:
        await update.message.reply_text("Укажи chat_id: /clearwazzup 360092495")
        return
    try:
        with db.conn.cursor() as cur:
            cur.execute("DELETE FROM wazzup_contact_map WHERE chat_id=%s", (chat_id_val,))
            cur.execute("DELETE FROM wazzup_contacts WHERE chat_id=%s", (chat_id_val,))
        db.conn.commit()
        await update.message.reply_text(f"✅ Запись {chat_id_val} удалена.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

# Ожидающие данные для создания договора — user_id → {data, missing, missing_idx}
_pending_contracts: dict = {}

async def _create_and_send_contract(contract_data: dict, created_by: str,
                                    message, context, force_number: str = None):
    """Генерирует договор PDF и отправляет в группу."""
    import io, sys, os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    try:
        from contract_generator import generate_contract_pdf, get_contract_number
        from datetime import datetime

        today = datetime.now()

        if force_number:
            contract_number = force_number
        else:
            contract_number = get_contract_number(today, db)

        MONTHS_RU = ["января","февраля","марта","апреля","мая","июня",
                     "июля","августа","сентября","октября","ноября","декабря"]
        contract_data["contract_number"] = contract_number
        contract_data["contract_date"] = f"{today.day} {MONTHS_RU[today.month-1]} {today.year} г."

        pdf_bytes = generate_contract_pdf(contract_data)

        # Сохраняем в БД с полными реквизитами
        db.save_contract(contract_number, contract_data["buyer_name"], created_by,
                         buyer_data=contract_data)

        # Отправляем в группу
        group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
        target = group_chat_id or message.chat_id
        caption = (
            f"📄 *Договор поставки № {contract_number}*\n"
            f"📅 {contract_data['contract_date']}\n"
            f"🏢 {contract_data['buyer_name']}\n"
            f"👤 Создал: {created_by}"
        )
        await context.bot.send_document(
            chat_id=target,
            document=io.BytesIO(pdf_bytes),
            filename=f"Договор_{contract_number}_{contract_data['buyer_name'][:30]}.pdf",
            caption=caption,
            parse_mode="Markdown"
        )
        if target != message.chat_id:
            await message.reply_text(f"✅ Договор № {contract_number} отправлен в группу.", parse_mode="Markdown")

    except Exception as e:
        logger.error(f"_create_and_send_contract: {e}", exc_info=True)
        await message.reply_text(f"❌ Ошибка генерации договора: {e}")

async def cmd_wazzup_enrich(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обогащает базу контактов тегами из МойСклад. /wazzup_enrich"""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return

    rows = db._fetchall("SELECT chat_id, company_name FROM wazzup_contact_map WHERE company_name IS NOT NULL AND company_name != '__ignore__'")
    if not rows:
        await update.message.reply_text("База контактов пуста.")
        return

    await update.message.reply_text(f"🔍 Обогащаю {len(rows)} контактов из МойСклад...")
    from moysklad import find_counterparty_info
    updated = 0
    for r in rows:
        try:
            cp_list = await find_counterparty_info(r["company_name"])
            if cp_list:
                cp = cp_list[0]
                db.update_wazzup_contact_tags(
                    chat_id=r["chat_id"],
                    tags=cp.get("tags", []),
                    manager=cp.get("manager", ""),
                    segment=cp.get("buyer_type", ""),
                )
                updated += 1
        except Exception as e:
            logger.warning(f"wazzup_enrich {r['company_name']}: {e}")

    await update.message.reply_text(f"✅ Обновлено: {updated}/{len(rows)} контактов.")

async def cmd_wazzup_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выгружает базу идентифицированных контактов в Excel. /wazzup_export"""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return

    rows = db._fetchall("SELECT company_name, wazzup_name, chat_type, manager, segment, tags, created_at FROM wazzup_contact_map WHERE company_name != '__ignore__' ORDER BY company_name")

    if not rows:
        await update.message.reply_text("База контактов пуста.")
        return

    import io, csv
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Компания", "Имя в мессенджере", "Канал", "Менеджер", "Сегмент", "Теги", "Дата"])
    for r in rows:
        writer.writerow([
            r.get("company_name", ""),
            r.get("wazzup_name", ""),
            r.get("chat_type", ""),
            r.get("manager", ""),
            r.get("segment", ""),
            r.get("tags", ""),
            str(r.get("created_at", ""))[:10],
        ])

    output.seek(0)
    await update.message.reply_document(
        document=io.BytesIO(output.getvalue().encode("utf-8-sig")),
        filename="wazzup_contacts.csv",
        caption=f"📋 База контактов — {len(rows)} записей"
    )

async def cmd_wazzup_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбрасывает привязку Wazzup контакта. /wazzup_reset <chat_id>"""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return
    args = context.args
    if not args:
        await update.message.reply_text("Использование: /wazzup_reset <chat_id>")
        return
    chat_id_val = args[0]
    try:
        with db.conn.cursor() as cur:
            cur.execute("DELETE FROM wazzup_contact_map WHERE chat_id = %s", (chat_id_val,))
        db.conn.commit()
        await update.message.reply_text(f"✅ Привязка для `{chat_id_val}` сброшена.", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def cmd_wazzup_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список каналов Wazzup с их ID."""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return

    api_key = os.getenv("WAZZUP_API_KEY", "")
    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://api.wazzup24.com/v3/channels",
            headers={"Authorization": f"Bearer {api_key}"}
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                await update.message.reply_text(f"❌ Ошибка: {resp.status} {text[:200]}")
                return
            data = await resp.json()

    channels = data.get("channels", data) if isinstance(data, dict) else data
    if not channels:
        await update.message.reply_text("Каналов не найдено.")
        return

    lines = ["📡 *Каналы Wazzup:*\n"]
    for ch in channels if isinstance(channels, list) else [channels]:
        ch_id = ch.get("id", ch.get("channelId", "?"))
        name = ch.get("name", "")
        transport = ch.get("transport", "")
        status = ch.get("state", ch.get("status", ""))
        lines.append(f"• `{ch_id}`\n  transport: *{transport}* name: {name} status: {status}\n")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_wazzup_setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Настраивает вебхук Wazzup. /wazzup_setup"""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return

    api_key = os.getenv("WAZZUP_API_KEY", "")
    if not api_key:
        await update.message.reply_text("❌ WAZZUP_API_KEY не задан в Railway.")
        return

    import aiohttp
    webhook_url = "https://f2b-production.up.railway.app/webhook/wazzup"

    async with aiohttp.ClientSession() as session:
        # Устанавливаем вебхук
        async with session.patch(
            "https://api.wazzup24.com/v3/webhooks",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"webhooksUri": webhook_url, "subscriptions": {"messagesAndStatuses": True}}
        ) as resp:
            if resp.status == 200:
                await update.message.reply_text(
                    f"✅ Wazzup вебхук настроен!\n"
                    f"📡 URL: `{webhook_url}`\n\n"
                    f"Теперь все сообщения менеджеров будут сохраняться автоматически.",
                    parse_mode="Markdown"
                )
            else:
                text = await resp.text()
                await update.message.reply_text(f"❌ Ошибка: {resp.status} {text[:200]}")

async def cmd_clear_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет ВСЕ открытые задачи. Только для руководителя."""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return
    try:
        with db.conn.cursor() as cur:
            cur.execute("UPDATE tasks SET status='done', completed_at=NOW(), result='Удалено руководителем' WHERE status='open'")
        db.conn.commit()
        await update.message.reply_text("✅ Все открытые задачи очищены.")
    except Exception as e:
        logger.error(f"cmd_clear_all error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def cmd_clear_tasks_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет ВСЕ задачи включая выполненные. Только для руководителя."""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return
    try:
        db._ensure_connection()
        with db.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM tasks")
            row = cur.fetchone()
            total = row["cnt"] if row else 0
            cur.execute("TRUNCATE TABLE tasks RESTART IDENTITY")
        db.conn.commit()
        await update.message.reply_text(f"🗑 Удалено задач: {total}. Таблица очищена полностью.")
    except Exception as e:
        logger.error(f"cmd_clear_tasks_all error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверяет все основные функции Эфа. Только для руководителя."""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return

    await update.message.reply_text("🔍 Запускаю диагностику...", parse_mode="Markdown")

    results = []

    async def check(name: str, coro, timeout: int = 8):
        try:
            result = await asyncio.wait_for(coro, timeout=timeout)
            if result:
                results.append(f"✅ {name}")
            else:
                results.append(f"⚠️ {name} — пустой результат")
        except asyncio.TimeoutError:
            results.append(f"⚠️ {name} — таймаут (>{timeout}с)")
        except Exception as e:
            results.append(f"❌ {name} — {str(e)[:60]}")

    # 1. БД — задачи
    try:
        tasks = db.get_all_open_tasks()
        results.append(f"✅ База данных — {len(tasks)} открытых задач")
    except Exception as e:
        results.append(f"❌ База данных — {e}")

    # 2. МойСклад — токен и базовый запрос
    async def test_ms():
        from moysklad import get_headers, MS_BASE
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{MS_BASE}/entity/organization", headers=get_headers()) as resp:
                return resp.status == 200
    await check("МойСклад API", test_ms())

    # 3. МойСклад — поиск товара
    async def test_ms_search():
        from moysklad import search_products
        rows = await search_products("лосось")
        return len(rows) > 0
    await check("МойСклад поиск товаров", test_ms_search())

    # 4. МойСклад — баланс контрагента
    async def test_ms_balance():
        from moysklad import get_counterparty_balance
        rows = await get_counterparty_balance("джи")
        return len(rows) > 0
    await check("МойСклад баланс контрагента", test_ms_balance())

    # 5. МойСклад — ПДЗ (лёгкая проверка — просто один запрос)
    async def test_pdz():
        from moysklad import get_headers, MS_BASE
        import aiohttp
        async with aiohttp.ClientSession() as session:
            url = f"{MS_BASE}/entity/customerorder?limit=1&expand=attributes"
            async with session.get(url, headers=get_headers()) as resp:
                return resp.status == 200
    await check("МойСклад ПДЗ (заказы)", test_pdz())

    # 6. Claude API — диспетчер
    async def test_claude():
        from claude_ai import dispatch
        result = await dispatch("привет", "Test")
        return result.get("action") is not None
    await check("Claude AI диспетчер", test_claude())

    # 7. Поиск фото
    async def test_photo():
        photos = db.search_media("лосось", media_type="photo")
        return True  # просто проверяем что БД отвечает
    await check("Поиск фото (канал Контент)", test_photo())

    # 8. Геокодер
    async def test_geocoder():
        from moysklad import geocode_address
        coords = await geocode_address("Истра, Московская область")
        return coords is not None
    await check("Яндекс геокодер", test_geocoder())

    # 9. Webhook сервер
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get("https://f2b-production.up.railway.app/health") as resp:
                if resp.status == 200:
                    results.append("✅ Webhook сервер")
                else:
                    results.append(f"⚠️ Webhook сервер — статус {resp.status}")
    except Exception as e:
        results.append(f"❌ Webhook сервер — {e}")

    # Итог
    ok = sum(1 for r in results if r.startswith("✅"))
    warn = sum(1 for r in results if r.startswith("⚠️"))
    err = sum(1 for r in results if r.startswith("❌"))

    header = f"📊 Диагностика Эфа\n✅ {ok} ок  ⚠️ {warn} предупреждений  ❌ {err} ошибок\n\n"
    await update.message.reply_text(header + "\n".join(results))

async def cmd_del_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет задачу по ID. /deltask 5"""
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return
    if not context.args:
        await update.message.reply_text("Использование: /deltask <ID>\nСписок ID: /cleartasks")
        return
    try:
        task_id = int(context.args[0])
        with db.conn.cursor() as cur:
            cur.execute(
                "UPDATE tasks SET status='done', completed_at=NOW(), result='Удалено руководителем' WHERE id=%s AND status='open'",
                (task_id,)
            )
            deleted = cur.rowcount
        db.conn.commit()
        if deleted:
            await update.message.reply_text(f"✅ Задача #{task_id} удалена.")
        else:
            await update.message.reply_text(f"❌ Задача #{task_id} не найдена или уже закрыта.")
    except ValueError:
        await update.message.reply_text("❌ Укажи числовой ID задачи.")

async def handle_contract_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопок подтверждения создания договора."""
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "contract_cancel":
        await query.message.edit_text("❌ Создание договора отменено.")
        return

    if data.startswith("contract_force|"):
        cp_id = data.split("|")[1]
        from moysklad import get_counterparty_requisites
        await query.message.edit_text("🔍 Читаю реквизиты...")
        reqs = await get_counterparty_requisites(cp_id)
        contract_data = {
            "buyer_name": reqs.get("buyer_legal_title") or reqs.get("buyer_name", ""),
            "buyer_inn": reqs.get("buyer_inn", ""),
            "buyer_ogrn": reqs.get("buyer_ogrn", ""),
            "buyer_address": reqs.get("buyer_address", ""),
            "buyer_bank": reqs.get("buyer_bank", ""),
            "buyer_rs": reqs.get("buyer_rs", ""),
            "buyer_bik": reqs.get("buyer_bik", ""),
            "buyer_ks": reqs.get("buyer_ks", ""),
            "buyer_phone": reqs.get("buyer_phone", ""),
            "buyer_email": reqs.get("buyer_email", ""),
            "buyer_representative": reqs.get("buyer_representative", ""),
            "buyer_director_name": reqs.get("buyer_director_name", ""),
            "buyer_basis": "Устава",
        }
        # Для ИП: собираем полное ФИО если в buyer_name нет фамилии
        _ip_last = reqs.get("buyer_last_name", "")
        _ip_first = reqs.get("buyer_first_name", "")
        _ip_mid = reqs.get("buyer_middle_name", "")
        if _ip_last and _ip_first and _ip_last not in contract_data["buyer_name"]:
            _full_fio = " ".join(filter(None, [_ip_last, _ip_first, _ip_mid]))
            if contract_data["buyer_name"]:
                contract_data["buyer_name"] = contract_data["buyer_name"] + " " + _full_fio
            else:
                contract_data["buyer_name"] = _full_fio
        # Проверяем недостающие поля
        ASK_REQUIRED = {
            "buyer_representative": "должность и полное ФИО директора (напр. 'генерального директора Иванову Марию Алексеевну' — обязательно с фамилией!)",
            "buyer_basis": "основание полномочий",
        }
        INFO_REQUIRED = {
            "buyer_inn": "ИНН", "buyer_ogrn": "ОГРН",
            "buyer_address": "юридический адрес",
            "buyer_rs": "расчётный счёт (р/с)", "buyer_bik": "БИК банка",
            "buyer_bank": "название банка", "buyer_ks": "корреспондентский счёт (к/с)",
        }
        missing_ask = [(k, v) for k, v in ASK_REQUIRED.items() if not contract_data.get(k)]
        missing_info = [(k, v) for k, v in INFO_REQUIRED.items() if not contract_data.get(k)]
        user = query.from_user

        # Если не хватает данных из МойСклад — останавливаемся
        if missing_info:
            names = "\n".join(f"   • {v}" for _, v in missing_info)
            await query.message.edit_text(
                f"📄 *{contract_data['buyer_name']}*\n\n"
                f"❌ *Договор не сформирован* — в МойСклад отсутствуют:\n{names}\n\n"
                f"Попроси клиента предоставить данные и внеси их в карточку МойСклад, "
                f"затем запроси договор повторно.",
                parse_mode="Markdown"
            )
            return

        msg = f"📄 *{contract_data['buyer_name']}*\n"

        if missing_ask:
            _pending_contracts[user.id] = {
                "data": contract_data,
                "missing_keys": [m[0] for m in missing_ask],
                "missing_labels": [m[1] for m in missing_ask],
                "missing_idx": 0,
            }
            try:
                import json as _j
                db._execute("INSERT INTO pending_contracts (user_id, data) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET data=%s,created_at=NOW()", (user.id, _j.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str), _j.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str)))
            except Exception: pass
            msg += f"*{missing_ask[0][1]}*?"
            await query.message.edit_text(msg, parse_mode="Markdown")
        else:
            await _create_and_send_contract(
                contract_data, user.full_name, query.message, context
            )

async def handle_task_done_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки 'Выполнено' — запрашивает результат."""
    query = update.callback_query
    await query.answer()
    parts = query.data.split("|")
    task_id = int(parts[1]) if len(parts) > 1 else 0

    task = db._fetchone("SELECT * FROM tasks WHERE id=%s", (task_id,))
    if not task:
        await query.edit_message_text("❌ Задача не найдена.")
        return

    if task.get("status") == "done":
        await query.edit_message_text(
            query.message.text + "\n\n✅ *Уже выполнено*",
            parse_mode="Markdown"
        )
        return

    # Сохраняем ожидание результата
    _pending_task_results[query.from_user.id] = {
        "task_id": task_id,
        "task_text": task.get("text", ""),
        "executor": task.get("executor", ""),
        "msg_id": query.message.message_id,
    }

    await query.edit_message_text(
        query.message.text + "\n\n✏️ *Напиши результат выполнения:*",
        parse_mode="Markdown"
    )

async def cmd_deltask_by_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удалить задачу ответом 'удали' на сообщение бота с задачей."""
    msg = update.message
    if not msg or not msg.reply_to_message:
        return
    user = msg.from_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if not user or user.id not in manager_ids:
        return
    replied = msg.reply_to_message
    # Проверяем что это сообщение бота
    if not replied.from_user or not replied.from_user.is_bot:
        return
    bot_msg_id = replied.message_id
    chat_id = msg.chat_id
    deleted = db.delete_tasks_by_bot_message_id(bot_msg_id, chat_id)
    if deleted:
        names = [f"• *{t['executor']}*: {t['text']}" for t in deleted]
        await msg.reply_text(
            f"🗑 Задач удалено: {len(deleted)}\n" + "\n".join(names),
            parse_mode="Markdown"
        )
        # Удаляем само сообщение бота о задачах
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=bot_msg_id)
        except Exception:
            pass
    else:
        await msg.reply_text("Задачи не найдены или уже закрыты.")

async def cmd_clear_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет все открытые задачи кроме указанных ID. Только для руководителя."""
    logger.info(f"cmd_clear_tasks вызван от {update.effective_user.id} args={context.args}")
    user = update.effective_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        return
    try:
        args = context.args
        if args and args[0] == "keep":
            keep_ids = [int(x) for x in args[1:] if x.isdigit()]
            db = Database()
            with db.conn.cursor() as cur:
                if keep_ids:
                    placeholders = ",".join(["%s"] * len(keep_ids))
                    cur.execute(f"UPDATE tasks SET status='done', completed_at=NOW(), result='Удалено руководителем' WHERE status='open' AND id NOT IN ({placeholders})", keep_ids)
                else:
                    cur.execute("UPDATE tasks SET status='done', completed_at=NOW(), result='Удалено руководителем' WHERE status='open'")
            db.conn.commit()
            db.conn.close()
            await update.message.reply_text(f"✅ Все задачи очищены." if not keep_ids else f"✅ Задачи очищены. Оставлены ID: {keep_ids}")
        else:
            db = Database()
            tasks = db.get_all_open_tasks()
            if not tasks:
                await update.message.reply_text("Нет открытых задач.")
                return
            lines = [f"ID {t['id']}: {t.get('executor','—')} — {t.get('text','')}" for t in tasks]
            lines.append("\nЧтобы оставить только нужные: /cleartasks keep 5 12")
            await update.message.reply_text("\n".join(lines))
    except Exception as e:
        logger.error(f"cmd_clear_tasks error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {e}")

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
    """Все открытые и недавно выполненные задачи команды."""
    tasks = db.get_all_open_tasks()
    done_tasks = db.get_recently_done(hours=24)

    if not tasks and not done_tasks:
        await update.message.reply_text("✅ Нет открытых задач!")
        return

    lines = []

    if tasks:
        # Группируем открытые по исполнителю
        by_user = {}
        for t in tasks:
            exe = t.get('executor') or 'Неизвестно'
            by_user.setdefault(exe, []).append(t)

        lines.append("📋 *Открытые задачи:*\n")
        MONTHS = ["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"]
        for user, utasks in by_user.items():
            lines.append(f"*{user}* ({len(utasks)}):")
            for t in utasks:
                icon = "🔴" if t.get('overdue') else "🟡"
                dl = t.get('deadline')
                if dl:
                    try:
                        from datetime import date as _date
                        d = _date.fromisoformat(str(dl)[:10])
                        deadline_str = f" · до {d.day} {MONTHS[d.month-1]}"
                    except Exception:
                        deadline_str = f" · до {dl}"
                else:
                    deadline_str = ""
                lines.append(f"  {icon} {t['text']}{deadline_str}")
            lines.append("")

    if done_tasks:
        lines.append("✅ *Выполнено за 24 часа:*\n")
        for t in done_tasks:
            exe = t.get('executor') or ''
            result = t.get('result') or ''
            result_str = f" — {result}" if result else ""
            lines.append(f"  ✅ *{exe}*: {t['text']}{result_str}")

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

async def process_sipuni_call(call_id: str, src_num: str, dst_num: str,
                              short_dst: str, tree_name: str, record_link: str,
                              call_start: str, call_answer: str, bot):
    """Скачивает запись звонка и транскрибирует через Whisper. Сохраняет в БД."""
    import aiohttp, tempfile, os as _os

    openai_key = _os.getenv("OPENAI_API_KEY", "")
    if not openai_key:
        logger.warning("process_sipuni_call: OPENAI_API_KEY не задан")
        return

    try:
        manager_name = tree_name or short_dst or dst_num

        # Длительность
        duration_sec = 0
        try:
            if call_start and call_answer and call_answer != "0":
                duration_sec = int(call_answer) - int(call_start)
                if duration_sec < 0:
                    duration_sec = 0
        except Exception:
            pass

        logger.info(f"Sipuni: транскрибирую звонок {call_id} от {src_num} ({duration_sec}с)")

        # 1. Скачиваем запись
        async with aiohttp.ClientSession() as session:
            async with session.get(record_link) as resp:
                if resp.status != 200:
                    logger.warning(f"Sipuni: не удалось скачать запись {resp.status}")
                    return
                audio_data = await resp.read()

        if len(audio_data) < 1000:
            logger.warning(f"Sipuni: запись слишком короткая ({len(audio_data)} байт)")
            return

        # 2. Транскрипция через OpenAI Whisper
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
            tmp.write(audio_data)
            tmp_path = tmp.name

        try:
            async with aiohttp.ClientSession() as session:
                with open(tmp_path, "rb") as f:
                    form = aiohttp.FormData()
                    form.add_field("file", f, filename="call.mp3", content_type="audio/mpeg")
                    form.add_field("model", "whisper-1")
                    form.add_field("language", "ru")
                    async with session.post(
                        "https://api.openai.com/v1/audio/transcriptions",
                        headers={"Authorization": f"Bearer {openai_key}"},
                        data=form
                    ) as resp:
                        if resp.status != 200:
                            body = await resp.text()
                            logger.warning(f"Whisper error {resp.status}: {body[:200]}")
                            return
                        result = await resp.json()
                        transcript = result.get("text", "")
        finally:
            _os.unlink(tmp_path)

        if not transcript:
            logger.warning("Sipuni: транскрипция пустая")
            return

        # 3. Сохраняем в БД
        saved = db.save_call_transcript(
            call_id=call_id,
            src_num=src_num,
            dst_num=dst_num,
            manager_name=manager_name,
            tree_name=tree_name,
            transcript=transcript,
            duration_sec=duration_sec,
        )
        logger.info(f"Sipuni: транскрипция сохранена call_id={call_id} saved={saved} ({len(transcript)} символов)")

    except Exception as e:
        logger.error(f"process_sipuni_call: {e}", exc_info=True)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главный обработчик всех сообщений."""
    message = update.message
    if not message:
        return

    async def safe_reply(text, **kwargs):
        """Отправляет ответ, при ошибке цитаты — без неё."""
        try:
            return await message.reply_text(text, **kwargs)
        except Exception:
            try:
                return await context.bot.send_message(
                    chat_id=message.chat_id, text=text,
                    parse_mode=kwargs.get("parse_mode"),
                    reply_markup=kwargs.get("reply_markup")
                )
            except Exception as e:
                logger.warning(f"safe_reply failed: {e}")

    # Команды обрабатываются отдельными CommandHandler — пропускаем
    if message.text and message.text.startswith("/"):
        return

    chat_id = message.chat_id
    user = message.from_user
    text = message.text or message.caption or ""

    # Проверяем блокировку
    if user and db.is_user_blocked(user.id):
        await message.reply_text("⛔ Доступ ограничен. Обратитесь к руководителю.")
        return

    # Логируем запрос
    if user and text:
        db.log_usage(user.id, user.full_name, text[:100], chat_id)

    # Обработка ожидаемого ввода из меню (фото / ПДЗ клиента)
    awaiting = _user_awaiting.get(user.id) if user else None
    if awaiting and user and text and not text.startswith("/"):
        _user_awaiting.pop(user.id, None)

        if awaiting == "photo":
            await message.reply_chat_action("upload_photo")
            await search_and_send_photo(update, context, text)
            await message.reply_text("Выбери действие:", reply_markup=_user_menu_keyboard())
            return

        elif awaiting and awaiting.startswith("promo_"):
            segment = awaiting.replace("promo_", "")
            db.set_promo(text, segment)
            await message.reply_text(
                f"✅ Промо для *{segment}* сохранён:\n\n{text}",
                parse_mode="Markdown"
            )
            return

        elif awaiting == "contract":
            await message.reply_chat_action("typing")
            buyer_query = text
            from moysklad import get_counterparty_requisites
            from datetime import date as _date, datetime as _datetime
            counterparties = await get_counterparty_balance(buyer_query)
            if not counterparties:
                await message.reply_text(
                    f"❌ Компания *{buyer_query}* не найдена в МойСклад.",
                    parse_mode="Markdown"
                )
                await message.reply_text("Выбери действие:", reply_markup=_user_menu_keyboard())
                return
            cp = counterparties[0]
            cp_id = cp.get("id", "")
            cp_name = cp.get("name", buyer_query)

            # Проверяем дату создания клиента — если до 20.03.2026, договор не создаём
            created_str = cp.get("created", "")
            CUTOFF = _datetime(2026, 3, 20)
            is_old_client = False
            if created_str:
                try:
                    created_dt = _datetime.fromisoformat(created_str[:19])
                    if created_dt < CUTOFF:
                        is_old_client = True
                except Exception:
                    pass

            if is_old_client:
                await message.reply_text(
                    f"⚠️ *{cp_name}* — действующий клиент.\n\n"
                    f"Договор по этому клиенту уже существует или ведётся в работе.\n"
                    f"По вопросам договора обратитесь к *Юлии Гераськиной*.",
                    parse_mode="Markdown",
                    reply_markup=_user_menu_keyboard()
                )
                return

            existing = db.find_contract_by_buyer(cp_name)
            if existing:
                await message.reply_text(
                    f"📄 Договор с *{cp_name}* уже был сформирован.\n"
                    f"Номер: *{existing['contract_number']}* от {existing['created_at'].strftime('%d.%m.%Y')}\n\n"
                    f"Повторное формирование невозможно.\n"
                    f"По вопросам договора обратитесь к *Юлии Гераськиной*.",
                    parse_mode="Markdown"
                )
                await message.reply_text("Выбери действие:", reply_markup=_user_menu_keyboard())
                return
            # Проверяем — клиент уже существует в МойСклад?
            cp_updated = cp.get("updated") or cp.get("created") or ""
            from datetime import date as _date2
            today_str2 = _date2.today().isoformat()
            if cp_updated and cp_updated[:10] < today_str2:
                await message.reply_text(
                    f"⚠️ Клиент *{cp_name}* уже существует в МойСклад.\n\n"
                    f"Договор с давними клиентами не формируется через бота.\n"
                    f"По вопросам договора обратитесь к *Юлии Гераськиной*.",
                    parse_mode="Markdown"
                )
                await message.reply_text("Выбери действие:", reply_markup=_user_menu_keyboard())
                return
            await message.reply_text(f"🔍 Читаю реквизиты *{cp_name}*...", parse_mode="Markdown")
            reqs = await get_counterparty_requisites(cp_id)
            contract_data = {
                "buyer_name": reqs.get("buyer_legal_title") or reqs.get("buyer_name", buyer_query),
                "buyer_inn": reqs.get("buyer_inn", ""),
                "buyer_ogrn": reqs.get("buyer_ogrn", ""),
                "buyer_address": reqs.get("buyer_address", ""),
                "buyer_bank": reqs.get("buyer_bank", ""),
                "buyer_rs": reqs.get("buyer_rs", ""),
                "buyer_bik": reqs.get("buyer_bik", ""),
                "buyer_ks": reqs.get("buyer_ks", ""),
                "buyer_phone": reqs.get("buyer_phone", ""),
                "buyer_email": reqs.get("buyer_email", ""),
                "buyer_representative": reqs.get("buyer_representative", ""),
                "buyer_director_name": reqs.get("buyer_director_name", ""),
                "buyer_basis": "Устава",
            }
            # Для ИП: собираем полное ФИО если в buyer_name нет фамилии
            _ip_last = reqs.get("buyer_last_name", "")
            _ip_first = reqs.get("buyer_first_name", "")
            _ip_mid = reqs.get("buyer_middle_name", "")
            if _ip_last and _ip_first and _ip_last not in contract_data["buyer_name"]:
                _full_fio = " ".join(filter(None, [_ip_last, _ip_first, _ip_mid]))
                if contract_data["buyer_name"]:
                    contract_data["buyer_name"] = contract_data["buyer_name"] + " " + _full_fio
                else:
                    contract_data["buyer_name"] = _full_fio
            INFO_REQUIRED = {
                "buyer_inn": "ИНН", "buyer_ogrn": "ОГРН",
                "buyer_address": "юридический адрес",
                "buyer_rs": "расчётный счёт (р/с)", "buyer_bik": "БИК банка",
                "buyer_bank": "название банка", "buyer_ks": "корреспондентский счёт (к/с)",
            }
            ASK_REQUIRED = {
                "buyer_representative": "должность и полное ФИО директора (напр. 'генерального директора Иванову Марию Алексеевну' — обязательно с фамилией!)",
                "buyer_basis": "основание полномочий",
            }
            missing_info = [(k, v) for k, v in INFO_REQUIRED.items() if not contract_data.get(k)]
            missing_ask = [(k, v) for k, v in ASK_REQUIRED.items() if not contract_data.get(k)]

            if missing_info:
                names = "\n".join(f"   • {v}" for _, v in missing_info)
                await message.reply_text(
                    f"📄 *{contract_data['buyer_name']}*\n\n"
                    f"❌ *Договор не сформирован* — в МойСклад отсутствуют:\n{names}\n\n"
                    f"Внеси данные в карточку МойСклад и запроси договор повторно.",
                    parse_mode="Markdown",
                    reply_markup=_user_menu_keyboard()
                )
                return
            if missing_ask:
                _pending_contracts[user.id] = {
                    "data": contract_data,
                    "missing_keys": [m[0] for m in missing_ask],
                    "missing_labels": [m[1] for m in missing_ask],
                    "missing_idx": 0,
                }
                await message.reply_text(
                    f"📄 *{contract_data['buyer_name']}*\n\nУточни:\n*{missing_ask[0][1]}*?",
                    parse_mode="Markdown"
                )
            else:
                await _create_and_send_contract(contract_data, user.full_name, message, context)
            return

        elif awaiting == "pdz_client":
            await message.reply_chat_action("typing")
            counterparties = await get_counterparty_balance(text)
            if not counterparties:
                await message.reply_text(f"❌ Клиент *{text}* не найден.", parse_mode="Markdown")
                await message.reply_text("Выбери действие:", reply_markup=_user_menu_keyboard())
                return

            cp = counterparties[0]
            cp_name = cp.get("name", text)
            balance = cp.get("balance", 0)
            tags = cp.get("tags", [])

            MANAGER_TAG_MAP = {
                "баласанян": "Карина Баласанян", "мерзлякова": "Елена Мерзлякова",
                "скляр": "Инесса Скляр", "леонтьев": "Алексей Леонтьев",
            }
            manager = "Не назначен"
            for tag in tags:
                if tag.lower() in MANAGER_TAG_MAP:
                    manager = MANAGER_TAG_MAP[tag.lower()]
                    break

            overdue_items = await get_overdue_demands(query=cp_name)
            overdue_sum = sum(i.get("overdue_sum", 0) for i in overdue_items) if overdue_items else 0
            overdue_lines = []
            if overdue_items:
                for item in overdue_items:
                    for d in item.get("demands", []):
                        overdue_lines.append(
                            f"   └ {d.get('name','')} · {d.get('due','')} · "
                            f"{d.get('unpaid',0):,.2f} руб. · {d.get('days',0)} дн."
                        )

            lines = [
                f"📊 *{cp_name}*\n",
                f"👤 Менеджер: *{manager}*",
                f"🏷 Теги: {', '.join(tags) if tags else '—'}",
                f"💵 Общий долг: *{abs(balance):,.2f} руб.*",
            ]
            if overdue_sum > 0:
                lines.append(f"🔴 Просрочено: *{overdue_sum:,.2f} руб.*")
                if overdue_lines:
                    lines.append("\n*Просроченные заказы:*")
                    lines.extend(overdue_lines[:5])
            else:
                lines.append("✅ Просроченных долгов нет")

            await message.reply_text("\n".join(lines), parse_mode="Markdown",
                                     reply_markup=_user_menu_keyboard())
            return

    # Ответ менеджера на алерт цены в личке — пересылаем Виктору
    OWNER_ID = 360092495
    if user and chat_id == user.id and chat_id != OWNER_ID and text:

        # 1. Ожидаем результат выполнения задачи
        if user.id in _pending_task_results:
            pending_tr = _pending_task_results.pop(user.id)
            task_id = pending_tr["task_id"]
            task_text = pending_tr["task_text"]
            executor = pending_tr["executor"]
            db.complete_task(task_id, result=text, completed_by=user.full_name)
            # Уведомляем Виктора
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=(
                    f"✅ *Задача выполнена*\n"
                    f"👤 *{executor}*\n"
                    f"📋 {task_text}\n\n"
                    f"💬 Результат: {text}"
                ),
                parse_mode="Markdown"
            )
            # Обновляем в группе PRO
            group_chat_id_tr = int(os.getenv("GROUP_CHAT_ID", "0"))
            if group_chat_id_tr:
                await context.bot.send_message(
                    chat_id=group_chat_id_tr,
                    text=(
                        f"✅ *{executor}* выполнил задачу:\n"
                        f"_{task_text}_\n\n"
                        f"💬 {text}"
                    ),
                    parse_mode="Markdown"
                )
            await message.reply_text("✅ Результат сохранён, руководитель уведомлён.")
            return

        # 2. Ответ на запрос комментария по цене
        if user.id in _pending_price_comments:
            pending = _pending_price_comments.pop(user.id)
            alert_id = pending.get("alert_id", 0)
            order_id = pending.get("order_id", "")
            mgr_name = pending.get("mgr_name", user.full_name)
            if alert_id:
                db.close_price_alert(alert_id, text)
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=(
                    f"💬 *Комментарий по занижению цены*\n"
                    f"👤 Менеджер: *{mgr_name}*\n\n"
                    f"{text}"
                ),
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Согласовано", callback_data=f"price_ok|{order_id}"),
                ]])
            )
            await message.reply_text("✅ Комментарий отправлен руководителю.")
            return

        # 3. Ответ на алерт цены через reply
        replied = message.reply_to_message
        if replied and replied.text and "#price_alert_" in replied.text:
            import re
            m = re.search(r"#price_alert_(\d+)", replied.text)
            if m:
                alert_id = int(m.group(1))
                alert_data = db.get_price_alert(alert_id)
                if alert_data:
                    db.close_price_alert(alert_id, text)
                    order_name = alert_data.get("order_name", "")
                    mgr_name = alert_data.get("manager_name", user.full_name)
                    await context.bot.send_message(
                        chat_id=OWNER_ID,
                        text=(
                            f"💬 *Ответ по алерту цены*\n"
                            f"👤 Менеджер: *{mgr_name}*\n"
                            f"📋 Заказ: {order_name or 'см. выше'}\n\n"
                            f"{text}"
                        ),
                        parse_mode="Markdown"
                    )
                    await message.reply_text("✅ Ответ отправлен руководителю.")
                    return

        # 3. Результат по ПДЗ — менеджер пишет в личку боту
        from scheduler import pdz_launched_today
        from datetime import date as _date
        today_str = _date.today().isoformat()
        if today_str in pdz_launched_today:
            text_lower = text.lower()

            # Фильтруем — НЕ сохраняем если это запрос к боту или нерелевантное сообщение
            is_bot_request = (
                text_lower.startswith("эф") or
                text_lower.startswith("пришли фото") or
                text_lower.startswith("фото ") or
                "пдз пробка" in text_lower or
                "/pdz" in text_lower or
                len(text.strip()) < 5
            )

            # Признаки ПДЗ ответа — упоминание оплаты/суммы/клиента/даты
            pdz_keywords = [
                "оплат", "перевод", "платёж", "платеж", "платежк",
                "тыс", "руб", "завтра", "пятниц", "понедельник",
                "вторник", "среда", "четверг", "задолженност",
                "ип ", "ооо ", "пришлёт", "пришлет", "перечислит",
                "отсрочк", "просрочк", "долг"
            ]
            is_pdz_result = any(kw in text_lower for kw in pdz_keywords)

            if not is_bot_request and is_pdz_result:
                db.save_pdz_result(
                    manager_name=user.full_name,
                    manager_user_id=user.id,
                    result_text=text
                )
                from scheduler import pdz_day_messages
                if today_str not in pdz_day_messages:
                    pdz_day_messages[today_str] = {}
                mgr_tag = "_all"
                for mgr in PDZ_MANAGERS:
                    frag = mgr.get("name_fragment", mgr["name"]).lower()
                    if frag in user.full_name.lower():
                        mgr_tag = mgr["tag"]
                        break
                if mgr_tag not in pdz_day_messages[today_str]:
                    pdz_day_messages[today_str][mgr_tag] = []
                pdz_day_messages[today_str][mgr_tag].append(f"{user.full_name}: {text}")
                await message.reply_text("✅ Принял, записал для отчёта.")
            elif is_bot_request:
                # Это запрос к боту — не отвечаем (обработается ниже)
                pass
            else:
                await message.reply_text(
                    "ℹ️ Не похоже на ответ по дебиторке.\n"
                    "Напиши например: _«Атмосфера оплатит в пятницу»_",
                    parse_mode="Markdown"
                )
            return

        # 4. Прочие сообщения в личке — бот не реагирует
        return

    if message.forward_origin and chat_id == (user.id if user else None):
        origin = message.forward_origin
        logger.info(f"forward_origin type={type(origin).__name__} data={origin}")

        fwd_id = None
        fwd_name = None
        chat_type = "telegram"

        # Обычный пользователь с открытым профилем
        fwd_user = getattr(origin, "sender_user", None)
        if fwd_user:
            fwd_id = str(fwd_user.id)
            fwd_name = fwd_user.full_name or str(fwd_user.id)

        # Скрытый пользователь — нет ID, писать нельзя
        if not fwd_id:
            fwd_name = getattr(origin, "sender_user_name", None)
            if fwd_name:
                await message.reply_text(
                    f"😕 У контакта *{fwd_name}* закрыт профиль в Telegram.\n"
                    f"Написать ему через бота не получится.",
                    parse_mode="Markdown"
                )
            return

        # Канал или чат
        fwd_chat = getattr(origin, "sender_chat", None) or getattr(origin, "chat", None)
        if not fwd_id and fwd_chat:
            fwd_id = str(fwd_chat.id)
            fwd_name = fwd_chat.title or fwd_chat.username or str(fwd_chat.id)

        if not fwd_id or not fwd_name:
            await message.reply_text("😕 Не удалось определить контакт — возможно у него закрыта приватность.")
            return

        if db.is_wazzup_contact_known(fwd_id):
            rows = db._fetchall("SELECT company_name FROM wazzup_contact_map WHERE chat_id=%s", (fwd_id,))
            if rows:
                await message.reply_text(f"✅ Контакт *{fwd_name}* уже привязан к *{rows[0]['company_name']}*", parse_mode="Markdown")
            return

        import uuid as _uuid_fwd
        link_key = str(_uuid_fwd.uuid4())[:8]
        entry = {
            "chat_id": fwd_id,
            "channel_id": "ddd24a95-9304-4098-a320-3e47fcd1020a",
            "wazzup_name": fwd_name,
            "chat_type": chat_type,
            "link_key": link_key,
        }
        _pending_links[link_key] = entry
        # Используем стек как и везде
        if user.id not in _pending_links or not isinstance(_pending_links.get(user.id), list):
            _pending_links[user.id] = []
        _pending_links[user.id].append(entry)
        await message.reply_text(
            f"👤 Контакт: *{fwd_name}*\n\nКак этот клиент называется в МойСклад?\n_(напиши название или часть названия)_",
            parse_mode="Markdown"
        )
        return

    # В группе ИДЕНТИФИКАЦИЯ — обрабатываем ввод названия компании
    wazzup_id_chat = int(os.getenv("WAZZUP_ID_CHAT_ID", "0"))
    if wazzup_id_chat and chat_id == wazzup_id_chat:
        if not text or text.startswith("/"):
            return
        # Удаляем сообщение менеджера чтобы не засорять группу
        try:
            await message.delete()
        except Exception:
            pass
        # Если у этого пользователя нет pending — ищем любой активный pending в группе
        if user and user.id not in _pending_links:
            for uid, pl in list(_pending_links.items()):
                if isinstance(uid, int) and isinstance(pl, list) and pl:
                    _pending_links[user.id] = pl
                    break
                elif isinstance(uid, int) and isinstance(pl, dict) and "wazzup_name" in pl:
                    _pending_links[user.id] = [pl]
                    break
        logger.info(f"ИДЕНТИФИКАЦИЯ: user={user.id if user else None} pending={user.id in _pending_links if user else False} all_keys={[k for k in _pending_links.keys() if isinstance(k, int)]}")
        if user and user.id not in _pending_links:
            return

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

    if user.id in manager_ids and len(text) > 5:
        text_lower = text.lower()

        # Задачи фиксируем ТОЛЬКО если в тексте есть слово "задача"
        if "задач" in text_lower:
            DATA_QUERY_KEYWORDS = [
                "пдз", "долг", "дебитор", "остатк", "отчёт", "отчет",
                "сводк", "покажи", "дай", "сколько", "кто",
                "активност", "упоминани", "договор", "баланс", "прайс",
                "задолженност", "статистик", "аналитик", "кратко", "итог",
                "просрочка", "просроченн", "должник",
            ]
            if not any(kw in text_lower for kw in DATA_QUERY_KEYWORDS):
                tasks = await extract_tasks_from_message(text, user.full_name)
                group_chat_id_for_tasks = int(os.getenv("GROUP_CHAT_ID", "0"))

                for task in tasks:
                    executor = task.get("executor", "")
                    if not task.get("task") or not executor:
                        continue

                    # Если "всем менеджерам" — разворачиваем в список МОП
                    executors = []
                    exec_lower = executor.lower()
                    if any(w in exec_lower for w in ["всем", "все менеджер", "мop", "мoп", "отдел продаж", "команда"]):
                        executors = MOP_MANAGERS
                    else:
                        executors = [executor]

                    for exec_name in executors:
                        task_id = db.save_task(
                            text=task["task"],
                            executor=exec_name,
                            deadline=task.get("deadline"),
                            source_chat=chat_id,
                            source_message_id=message.message_id,
                            created_by=user.full_name
                        )

                        deadline = task.get("deadline")
                        if deadline:
                            from datetime import date as _date
                            MONTHS = ["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"]
                            try:
                                d = _date.fromisoformat(deadline)
                                deadline_str = f" · до {d.day} {MONTHS[d.month-1]}"
                            except Exception:
                                deadline_str = f" · до {deadline}"
                        else:
                            deadline_str = ""

                        # 1. Публикуем в группу PRO без кнопок
                        if group_chat_id_for_tasks:
                            pub_text = (
                                f"📌 *Задача*\n"
                                f"👤 *{exec_name}*{deadline_str}\n"
                                f"{task['task']}\n\n"
                                f"_От: {user.full_name}_"
                            )
                            sent = await context.bot.send_message(
                                chat_id=group_chat_id_for_tasks,
                                text=pub_text,
                                parse_mode="Markdown"
                            )
                            if sent:
                                db.set_task_bot_message_id(task_id, sent.message_id)

                        # 2. Дублируем исполнителю в личку с кнопкой
                        mgr_chat_id = db.get_manager_chat_id(exec_name.split()[0])
                        if mgr_chat_id:
                            personal_text = (
                                f"📋 *Тебе задача*{deadline_str}:\n\n"
                                f"{task['task']}\n\n"
                                f"_От: {user.full_name}_"
                            )
                            await context.bot.send_message(
                                chat_id=mgr_chat_id,
                                text=personal_text,
                                parse_mode="Markdown",
                                reply_markup=InlineKeyboardMarkup([[
                                    InlineKeyboardButton("✅ Выполнено", callback_data=f"task_done|{task_id}")
                                ]])
                            )

                        logger.info(f"Задача #{task_id}: {exec_name} → {task['task']}")

    # 3. Автозакрытие задач — Claude анализирует контекст
    sender_name = update.effective_user.full_name if update.effective_user else ""
    text_lower_ac = text.lower().strip()
    is_task_assignment = (
        text_lower_ac.startswith("задача ") or
        text_lower_ac.startswith("задачи ") or
        "задач" in text_lower_ac and any(
            name in text_lower_ac for name in
            ["карин", "инесс", "скляр", "алексей", "леонтьев", "мерзляков", "баласанян"]
        )
    )
    if not is_bot_addressed(text) and len(text) > 5 and not is_task_assignment:
        open_tasks = db.get_all_open_tasks()
        if open_tasks:
            completed_items = await detect_task_completion(text, open_tasks, author=sender_name)
            if completed_items:
                closed = []
                for item in completed_items:
                    task_id = item["id"]
                    result = item.get("result", "")
                    task = next((t for t in open_tasks if t['id'] == task_id), None)
                    if task:
                        db.complete_task(task_id, result=result, completed_by=sender_name)
                        executor = task.get('executor', '')
                        result_str = f" — {result}" if result else ""
                        closed.append(f"✅ *{executor}*: {task['text']}{result_str}")
                        logger.info(f"Автозакрытие задачи {task_id}: {task['text']} | результат: {result}")
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

    # Проверяем ожидание привязки Wazzup контакта — из группы ИДЕНТИФИКАЦИИ или лички бота
    wazzup_id_chat_for_ident = int(os.getenv("WAZZUP_ID_CHAT_ID", "0"))
    is_ident_chat = (chat_id == wazzup_id_chat_for_ident)
    is_private_chat = (user and chat_id == user.id)
    if user and user.id in _pending_links and not is_bot_addressed(text) and (is_ident_chat or is_private_chat):
        _pl = _pending_links[user.id]
        pending_link = _pl[0] if isinstance(_pl, list) and _pl else (_pl if isinstance(_pl, dict) else None)
        if not pending_link:
            _pending_links.pop(user.id, None)
        else:
            logger.info(f"pending_links: user={user.id} contact={pending_link.get('wazzup_name')} text={text[:30]}")
            if "company_name" not in pending_link:
                # Если уже показали варианты — просим нажать кнопку
                if pending_link.get("suggestions") and text.strip() == pending_link.get("last_query", ""):
                    await safe_reply("👆 Выбери компанию из списка выше или нажми «Не привязывать».")
                    return
                # Сбрасываем старые варианты при новом вводе
                pending_link.pop("suggestions", None)
                pending_link["last_query"] = text.strip()
                # Ищем компанию в МойСклад
                company_query = text.strip()
                counterparties = await get_counterparty_balance(company_query)
                if not counterparties:
                    words = company_query.split()
                    suggestions = []
                    for word in words:
                        if len(word) >= 3:
                            found = await get_counterparty_balance(word)
                            for c in found:
                                if c not in suggestions:
                                    suggestions.append(c)
                    if suggestions:
                        link_key = pending_link.get("link_key", str(user.id))
                        pending_link["suggestions"] = [c.get("name","") for c in suggestions[:5]]
                        buttons = []
                        for i, c in enumerate(suggestions[:5]):
                            cp_name = c.get("name", "")
                            buttons.append([InlineKeyboardButton(
                                cp_name[:40],
                                callback_data=f"wazzup_pick|{i}|{link_key}"
                            )])
                        buttons.append([InlineKeyboardButton(
                            "🚫 Не привязывать",
                            callback_data=f"wazzup_role|отмена|{link_key}"
                        )])
                        await safe_reply(
                            f"❓ *{company_query}* не найдена точно.\n\nВозможно имеется в виду:",
                            parse_mode="Markdown",
                            reply_markup=InlineKeyboardMarkup(buttons)
                        )
                    else:
                        keyboard = InlineKeyboardMarkup([[
                            InlineKeyboardButton("🚫 Не привязывать", callback_data=f"wazzup_role|отмена|{pending_link.get('link_key', str(user.id))}")
                        ]])
                        await safe_reply(
                            f"❌ Компания *{company_query}* не найдена в МойСклад.\n"
                            f"Попробуй написать название точнее или отмени привязку.",
                            parse_mode="Markdown",
                            reply_markup=keyboard
                        )
                    return
                cp = counterparties[0]
                cp_name = cp.get("name", company_query)
                pending_link["company_name"] = cp_name
                link_key = pending_link.get("link_key", str(user.id))
                # Удаляем из стека
                if isinstance(_pending_links.get(user.id), list):
                    _pending_links[user.id] = [p for p in _pending_links[user.id] if p.get("link_key") != link_key]
                    if not _pending_links[user.id]:
                        _pending_links.pop(user.id, None)
                else:
                    _pending_links.pop(user.id, None)
                _pending_links.pop(link_key, None)
                db.delete_pending_link(link_key)
                ok = db.link_wazzup_contact(
                    chat_id=pending_link["chat_id"],
                    chat_type=pending_link["chat_type"],
                    channel_id=pending_link["channel_id"],
                    company_name=cp_name,
                    wazzup_name=pending_link["wazzup_name"],
                    role="рассылка",
                )
                if ok:
                    _wazzup_notified.discard(pending_link["chat_id"])
                    try:
                        from moysklad import find_counterparty_info
                        cp_list = await find_counterparty_info(cp_name)
                        if cp_list:
                            cp_data = cp_list[0]
                            db.update_wazzup_contact_tags(
                                chat_id=pending_link["chat_id"],
                                tags=cp_data.get("tags", []),
                                manager=cp_data.get("manager", ""),
                                segment=cp_data.get("buyer_type", ""),
                            )
                    except Exception as e:
                        logger.warning(f"Теги МойСклад: {e}")
                    await safe_reply(
                        f"✅ *{pending_link['wazzup_name']}* → *{cp_name}*\nЭф запомнил!",
                        parse_mode="Markdown"
                    )
                # Если есть ещё ожидающие в стеке — спрашиваем следующего
                next_pl = _pending_links.get(user.id)
                next_pending = next_pl[0] if isinstance(next_pl, list) and next_pl else None
                if next_pending:
                    await safe_reply(
                        f"👤 Следующий контакт: *{next_pending['wazzup_name']}*\n\n"
                        f"Как этот клиент называется в МойСклад?\n"
                        f"_(напиши название или часть названия)_",
                        parse_mode="Markdown"
                    )
            return

    # Проверяем ожидание данных для договора (ПОСЛЕ идентификации)
    if user and user.id in _pending_contracts:
        pending_c = _pending_contracts[user.id]
        keys = pending_c["missing_keys"]
        labels = pending_c["missing_labels"]
        idx = pending_c["missing_idx"]
        data = pending_c["data"]

        # Чистим текст от обращения "Эф," если есть
        answer_text = text.strip()
        for prefix in ["эф,", "эф ", "ef,", "ef "]:
            if answer_text.lower().startswith(prefix):
                answer_text = answer_text[len(prefix):].strip()
                break

        field_key = keys[idx]
        data[field_key] = answer_text

        if field_key == "buyer_representative":
            parts = answer_text.split()
            # Слова которые являются частью должности (даже с заглавной буквы)
            POSITION_WORDS = {
                "генерального", "генеральный", "директора", "директор",
                "индивидуального", "индивидуальный", "предпринимателя", "предприниматель",
                "исполнительного", "исполнительный", "коммерческого", "коммерческий",
                "финансового", "финансовый", "управляющего", "управляющий",
                "президента", "президент", "председателя", "председатель",
            }
            # Ищем первое слово с заглавной буквы которое НЕ является словом должности
            fio_start = 0
            for i, w in enumerate(parts):
                cleaned = w.strip(".,").lower()
                if w.strip(".,") and w.strip(".,")[0].isupper() and i > 0 and cleaned not in POSITION_WORDS:
                    fio_start = i
                    break
            if fio_start > 0:
                data["buyer_director_name"] = " ".join(parts[fio_start:])
            elif len(parts) >= 2:
                data["buyer_director_name"] = " ".join(parts[-3:]) if len(parts) >= 3 else " ".join(parts)

        idx += 1
        pending_c["missing_idx"] = idx

        if idx < len(keys):
            await message.reply_text(f"✅ Принято.\n\n*{labels[idx]}*?", parse_mode="Markdown")
        else:
            _pending_contracts.pop(user.id, None)
            db._execute("DELETE FROM pending_contracts WHERE user_id=%s", (user.id,))

            # Режим refresh — генерируем с сохранением номера и даты
            if pending_c.get("mode") == "refresh":
                await message.reply_text("✅ Данные получены. Генерирую договор...")
                try:
                    from contract_generator import generate_contract_pdf
                    import io as _io_refresh
                    pdf_bytes = generate_contract_pdf(data)
                    db.save_contract(data["contract_number"], data["buyer_name"], user.full_name, buyer_data=data)
                    group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
                    target = group_chat_id or message.chat_id
                    await context.bot.send_document(
                        chat_id=target,
                        document=_io_refresh.BytesIO(pdf_bytes),
                        filename=f"Договор_{data['contract_number']}_{data['buyer_name'][:30]}.pdf",
                        caption=(
                            f"📄 *Договор поставки № {data['contract_number']}* (переиздан, данные обновлены)\n"
                            f"📅 {data['contract_date']}\n"
                            f"🏢 {data['buyer_name']}\n"
                            f"👤 {user.full_name}"
                        ),
                        parse_mode="Markdown"
                    )
                    if target != message.chat_id:
                        await message.reply_text(f"✅ Договор № {data['contract_number']} переиздан и отправлен в группу.")
                except Exception as _e:
                    logger.error(f"refresh pending generate: {_e}", exc_info=True)
                    await message.reply_text(f"❌ Ошибка генерации: {_e}")
            else:
                await message.reply_text("✅ Все данные получены. Генерирую договор...")
                await _create_and_send_contract(data, user.full_name, message, context)
        return

    if not is_bot_addressed(text):
        # Автореакция на ПДЗ-запросы без обращения "Эф,"
        text_lower_pdz = text.lower().strip()
        PDZ_TRIGGER = ["просрочка", "пдз", "должник", "дебиторка"]
        if any(kw in text_lower_pdz for kw in PDZ_TRIGGER) and user and user.id in manager_ids:
            pass  # продолжаем — обработаем как PDZ запрос
        else:
            return

    query = clean_query(text)

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
            done = [t for t in db.get_recently_done(hours=24)
                    if employee.lower() in (t.get('executor') or '').lower()]
            if not tasks and not done:
                await message.reply_text(f"✅ У *{employee}* нет задач.", parse_mode="Markdown")
            else:
                lines = [f"📋 *Задачи — {employee}:*\n"]
                for t in tasks:
                    deadline_str = f" — до {t['deadline']}" if t.get("deadline") else ""
                    icon = "🔴" if t.get("overdue") else "🟡"
                    lines.append(f"{icon} {t['text']}{deadline_str}")
                if done:
                    lines.append("")
                    for t in done:
                        result_str = f" — {t['result']}" if t.get('result') else ""
                        lines.append(f"✅ {t['text']}{result_str}")
                await message.reply_text("\n".join(lines), parse_mode="Markdown")
        else:
            await cmd_all_tasks(update, context)

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

        # None означает что клиент не найден в МойСклад
        if items is None:
            await message.reply_text(
                f"❌ Клиент *{raw_query}* не найден в МойСклад.\n"
                f"Уточни название — например, часть названия компании.",
                parse_mode="Markdown"
            )
            return

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
                        "алексей": "леонтьев", "леонтьев": "леонтьев",
        }
        USER_MANAGER_DISPLAY = {
            "баласанян": "Карина Баласанян",
            "скляр": "Инесса Скляр",
            "мерзлякова": "Елена Мерзлякова",
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

    elif action == "find_buyers":
        product = params.get("product", "")
        period_days = params.get("period_days", 30)
        if not product:
            await message.reply_text("❌ Не указан товар.")
            return
        await message.reply_chat_action("typing")
        await message.reply_text(f"🔍 Ищу покупателей *{product}* за последние {period_days} дней...", parse_mode="Markdown")
        from moysklad import get_buyers_by_product
        result = await get_buyers_by_product(product, period_days=period_days)
        buyers = result.get("buyers", []) if isinstance(result, dict) else result
        found_name = result.get("product_name", product) if isinstance(result, dict) else product
        if not buyers:
            await message.reply_text(
                f"❌ Покупателей *{found_name}* за последние {period_days} дней не найдено.\n"
                f"_Искал товар: {found_name}_", parse_mode="Markdown")
            return
        lines = [f"👥 *Покупатели {found_name}* за {period_days} дней ({len(buyers)}):\n"]

        MANAGER_TAG_MAP = {
            "баласанян": "Карина Баласанян", "мерзлякова": "Елена Мерзлякова",
            "скляр": "Инесса Скляр", "леонтьев": "Алексей Леонтьев",
        }
        SPEC_TAGS = {"опт", "хорека", "розница"}

        for b in buyers:
            tags = b.get("tags", [])
            tags_lower = [t.lower() for t in tags]
            spec = next((t.capitalize() for t in tags_lower if t in SPEC_TAGS), "—")
            manager = next((MANAGER_TAG_MAP[t] for t in tags_lower if t in MANAGER_TAG_MAP), "Не назначен")
            lines.append(
                f"👤 *{b['name']}*\n"
                f"   👔 Менеджер: {manager}\n"
                f"   🏷 Категория: {spec}"
            )
        text = "\n\n".join(lines)
        if len(text) > 4000:
            text = text[:3900] + "\n\n_...и ещё_"
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "get_delivery_days":
        address = params.get("address", "")
        if not address:
            await message.reply_text("❌ Укажи адрес или город.")
            return
        await message.reply_chat_action("typing")
        from moysklad import check_delivery_schedule, DELIVERY_CITIES_COORDS, _CITY_INDEX, WEEKDAYS_RU, geocode_address, _haversine

        # Текстовый поиск по городу
        address_lower = address.lower()
        found_keyword = None
        for keyword in sorted(_CITY_INDEX.keys(), key=len, reverse=True):
            if keyword in address_lower:
                found_keyword = keyword
                break

        if found_keyword:
            info = _CITY_INDEX[found_keyword]
            canonical = info["canonical"]
            days = [WEEKDAYS_RU[d] for d in sorted(info["days"])]
            days_str = ", ".join(days)
            await message.reply_text(
                f"🚛 *{canonical}*\n📅 Дни доставки: *{days_str}*",
                parse_mode="Markdown"
            )
            return

        # Московский адрес?
        if "москва" in address_lower or "moscow" in address_lower:
            await message.reply_text("🚛 *Москва* — доставляем в любой рабочий день.", parse_mode="Markdown")
            return

        # Геокодируем
        coords = await geocode_address(address)
        if not coords:
            await message.reply_text(f"😕 Не удалось определить направление для адреса: {address}")
            return

        lat, lon = coords
        dist_from_moscow = _haversine(lat, lon, 55.7558, 37.6173)
        if dist_from_moscow < 35:
            await message.reply_text("🚛 Адрес в московской агломерации — доставляем в любой рабочий день.", parse_mode="Markdown")
            return

        # Ищем ближайший город
        nearest_city = None
        nearest_dist = float("inf")
        for city, (clat, clon) in DELIVERY_CITIES_COORDS.items():
            dist = _haversine(lat, lon, clat, clon)
            if dist < nearest_dist:
                nearest_dist = dist
                nearest_city = city

        if nearest_dist > 25 or not nearest_city:
            await message.reply_text(
                f"😕 Адрес *{address}* не входит ни в одно наше направление МО.\n"
                f"Уточни у руководителя.",
                parse_mode="Markdown"
            )
            return

        # Нашли ближайший город — берём его дни
        days = []
        for keyword, info in _CITY_INDEX.items():
            if info["canonical"] == nearest_city:
                days = [WEEKDAYS_RU[d] for d in sorted(info["days"])]
                break

        days_str = ", ".join(days) if days else "уточни у руководителя"
        await message.reply_text(
            f"🚛 Адрес близко к *{nearest_city}* ({round(nearest_dist)} км)\n"
            f"📅 Дни доставки: *{days_str}*",
            parse_mode="Markdown"
        )

    elif action == "send_message_to_client":
        client_query = params.get("client", "")
        msg_text = params.get("message", "")

        if not client_query:
            await message.reply_text("❌ Укажи клиента.")
            return

        await message.reply_chat_action("typing")

        # Находим контрагента в МойСклад
        from moysklad import get_counterparty_phones
        counterparties = await get_counterparty_balance(client_query)
        if not counterparties:
            await message.reply_text(f"❌ Клиент *{client_query}* не найден в МойСклад.", parse_mode="Markdown")
            return

        cp = counterparties[0]
        cp_name = cp.get("name", client_query)

        # Берём телефон
        phones = await get_counterparty_phones([{"id": cp.get("id",""), "name": cp_name, "href": cp.get("href","")}])
        phone = phones[0].get("phone") if phones else None

        if not phone:
            await message.reply_text(f"❌ У клиента *{cp_name}* нет телефона в МойСклад.", parse_mode="Markdown")
            return

        # Если текст не задан — формируем напоминание об оплате
        if not msg_text:
            balance = cp.get("balance", 0)
            debt = abs(balance) if balance < 0 else 0
            if debt > 0:
                from moysklad import fmt_money
                msg_text = f"Добрый день! Напоминаем о задолженности перед компанией F2B в размере {fmt_money(debt)}. Просьба произвести оплату. Спасибо!"
            else:
                await message.reply_text(f"❌ У *{cp_name}* нет долга. Укажи текст сообщения явно.", parse_mode="Markdown")
                return

        # Определяем каналы в порядке приоритета TG → Max → WhatsApp
        CHANNEL_MAP = {
            "telegram": "ddd24a95-9304-4098-a320-3e47fcd1020a",
            "tgapi":    "ddd24a95-9304-4098-a320-3e47fcd1020a",
            "max":      "1d5bc70a-7ca6-4895-8d1f-9690cf448214",
            "whatsapp": "e180aa1d-dc48-4d0a-bec3-fc0afc53cf03",
        }
        PRIORITY = ["telegram", "tgapi", "max", "whatsapp"]

        # Ищем известные каналы клиента из вебхуков — по имени или телефону
        known = db.get_wazzup_contacts(cp_name)
        # Также ищем по номеру телефона
        if phone and not known:
            known = db.get_wazzup_contacts(phone[-10:])  # последние 10 цифр
        channels_to_try = []
        for p in PRIORITY:
            for k in known:
                if k.get("chat_type") in (p,):
                    channels_to_try.append({
                        "channel_id": k["channel_id"],
                        "chat_type": k["chat_type"],
                        "chat_id": k["chat_id"],
                    })
                    break

        # Fallback — WhatsApp по номеру телефона если нет известных каналов
        if not any(c["chat_type"] in ("whatsapp",) for c in channels_to_try):
            channels_to_try.append({
                "channel_id": CHANNEL_MAP["whatsapp"],
                "chat_type": "whatsapp",
                "chat_id": phone,
            })

        # Показываем превью с кнопками — ждём подтверждения
        import uuid as _uuid
        msg_key = str(_uuid.uuid4())[:8]
        _pending_sends[msg_key] = {
            "channels": channels_to_try,
            "name": cp_name,
            "text": msg_text,
        }

        group_chat_id = get_group_chat_id()
        target_chat = group_chat_id or chat_id

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Отправить", callback_data=f"send_confirm|{msg_key}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"send_cancel|{msg_key}"),
        ]])
        await context.bot.send_message(
            chat_id=target_chat,
            text=(
                f"📤 *Сообщение клиенту*\n\n"
                f"👤 *{cp_name}*\n"
                f"📱 {phone}\n\n"
                f"💬 _{msg_text}_"
            ),
            parse_mode="Markdown",
            reply_markup=keyboard
        )

    elif action == "reconciliation":
        buyer_query = params.get("buyer", "")
        date_from = params.get("date_from", f"{datetime.now().year}-01-01")
        date_to = params.get("date_to", datetime.now().strftime("%Y-%m-%d"))

        await message.reply_chat_action("typing")

        # Ищем контрагента
        counterparties = await get_counterparty_balance(buyer_query)
        if not counterparties:
            await message.reply_text(
                f"❌ Компания *{buyer_query}* не найдена в МойСклад.",
                parse_mode="Markdown"
            )
            return

        cp = counterparties[0]
        cp_id = cp.get("id", "")
        cp_name = cp.get("name", buyer_query)

        await message.reply_text(
            f"📊 Формирую акт сверки *{cp_name}*\n"
            f"📅 Период: {date_from} — {date_to}\n"
            f"⏳ Запрашиваю данные из МойСклад...",
            parse_mode="Markdown"
        )

        try:
            import io as _io
            from reconciliation_generator import generate_reconciliation_pdf
            from moysklad import get_reconciliation_data

            # Получаем данные
            rec_data = await get_reconciliation_data(cp_id, date_from, date_to)

            if not rec_data or not rec_data.get("rows"):
                await message.reply_text(
                    f"😕 За период {date_from} — {date_to} операций с *{cp_name}* не найдено.",
                    parse_mode="Markdown"
                )
                return

            pdf_bytes = generate_reconciliation_pdf(rec_data)

            # Отправляем в группу
            group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
            target = group_chat_id or message.chat_id
            closing = rec_data.get("closing_balance", 0)
            if closing > 0:
                balance_str = f"💰 Долг клиента: *{closing:,.2f} руб.*"
            elif closing < 0:
                balance_str = f"💰 Переплата: *{abs(closing):,.2f} руб.*"
            else:
                balance_str = "✅ Взаиморасчёты согласованы"
            caption = (
                f"📊 *Акт сверки — {cp_name}*\n"
                f"📅 {date_from} — {date_to}\n"
                f"{balance_str}"
            )
            await context.bot.send_document(
                chat_id=target,
                document=_io.BytesIO(pdf_bytes),
                filename=f"Акт_сверки_{cp_name[:30]}_{date_to}.pdf",
                caption=caption,
                parse_mode="Markdown"
            )
            if target != message.chat_id:
                await message.reply_text("✅ Акт сверки отправлен в группу.")

        except Exception as e:
            logger.error(f"reconciliation error: {e}", exc_info=True)
            await message.reply_text(f"❌ Ошибка формирования акта: {e}")
        return

    elif action == "generate_contract":
        buyer_query = params.get("buyer", "")
        await message.reply_chat_action("typing")

        from moysklad import get_counterparty_requisites
        from datetime import date as _date, datetime as _datetime

        counterparties = await get_counterparty_balance(buyer_query)
        if not counterparties:
            await message.reply_text(
                f"❌ Компания *{buyer_query}* не найдена в МойСклад.",
                parse_mode="Markdown"
            )
            return

        cp = counterparties[0]
        cp_id = cp.get("id", "")
        cp_name = cp.get("name", buyer_query)

        # Запрет на договор для клиентов созданных до 20.03.2026
        created_str = cp.get("created", "")
        CUTOFF = _datetime(2026, 3, 20)
        if created_str:
            try:
                if _datetime.fromisoformat(created_str[:19]) < CUTOFF:
                    await message.reply_text(
                        f"⚠️ *{cp_name}* — действующий клиент.\n\n"
                        f"Договор по этому клиенту уже существует или ведётся в работе.\n"
                        f"По вопросам договора обратитесь к *Юлии Гераськиной*.",
                        parse_mode="Markdown"
                    )
                    return
            except Exception:
                pass

        # Проверяем — есть ли уже договор созданный Эфом
        existing = db.find_contract_by_buyer(cp_name)
        if existing:
            await message.reply_text(
                f"📄 Договор с *{cp_name}* уже был сформирован.\n"
                f"Номер: *{existing['contract_number']}* от {existing['created_at'].strftime('%d.%m.%Y')}\n\n"
                f"Повторное формирование договора невозможно.\n"
                f"По вопросам договора обратитесь к *Юлии Гераськиной*.",
                parse_mode="Markdown"
            )
            return

        # 2. Проверяем дату создания клиента в МойСклад
        # Если клиент создан ДО сегодня — договор уже должен существовать
        cp_updated = cp.get("updated") or cp.get("created") or ""
        today_str = _date.today().isoformat()
        if cp_updated and cp_updated[:10] < today_str:
            await message.reply_text(
                f"⚠️ Клиент *{cp_name}* уже существует в МойСклад.\n\n"
                f"Договор с давними клиентами не формируется через бота.\n"
                f"По вопросам договора обратитесь к *Юлии Гераськиной*.",
                parse_mode="Markdown"
            )
            return

        # Читаем полные реквизиты
        await message.reply_text(f"🔍 Читаю реквизиты *{cp.get('name','')}*...", parse_mode="Markdown")
        reqs = await get_counterparty_requisites(cp_id)

        contract_data = {
            "buyer_name": reqs.get("buyer_legal_title") or reqs.get("buyer_name", buyer_query),
            "buyer_inn": reqs.get("buyer_inn", ""),
            "buyer_ogrn": reqs.get("buyer_ogrn", ""),
            "buyer_address": reqs.get("buyer_address", ""),
            "buyer_bank": reqs.get("buyer_bank", ""),
            "buyer_rs": reqs.get("buyer_rs", ""),
            "buyer_bik": reqs.get("buyer_bik", ""),
            "buyer_ks": reqs.get("buyer_ks", ""),
            "buyer_phone": reqs.get("buyer_phone", ""),
            "buyer_email": reqs.get("buyer_email", ""),
            "buyer_representative": reqs.get("buyer_representative", ""),
            "buyer_director_name": reqs.get("buyer_director_name", ""),
            "buyer_basis": "Устава",  # по умолчанию
        }
        # Для ИП: собираем полное ФИО если в buyer_name нет фамилии
        _ip_last = reqs.get("buyer_last_name", "")
        _ip_first = reqs.get("buyer_first_name", "")
        _ip_mid = reqs.get("buyer_middle_name", "")
        if _ip_last and _ip_first and _ip_last not in contract_data["buyer_name"]:
            _full_fio = " ".join(filter(None, [_ip_last, _ip_first, _ip_mid]))
            if contract_data["buyer_name"]:
                contract_data["buyer_name"] = contract_data["buyer_name"] + " " + _full_fio
            else:
                contract_data["buyer_name"] = _full_fio

        # Проверяем чего не хватает
        # Разделяем: что спрашиваем у менеджера, что просто сообщаем как отсутствующее
        ASK_REQUIRED = {
            "buyer_representative": "должность и полное ФИО директора (напр. 'генерального директора Иванову Марию Алексеевну' — обязательно с фамилией!)",
            "buyer_basis": "основание полномочий (напр. 'Устава' или 'доверенности № 1 от 01.01.2026')",
        }
        INFO_REQUIRED = {
            "buyer_inn": "ИНН",
            "buyer_ogrn": "ОГРН",
            "buyer_address": "юридический адрес",
            "buyer_rs": "расчётный счёт (р/с)",
            "buyer_bik": "БИК банка",
            "buyer_bank": "название банка",
            "buyer_ks": "корреспондентский счёт (к/с)",
        }

        missing_ask = [(k, v) for k, v in ASK_REQUIRED.items() if not contract_data.get(k)]
        missing_info = [(k, v) for k, v in INFO_REQUIRED.items() if not contract_data.get(k)]

        if missing_ask or missing_info:
            _pending_contracts[user.id] = {
                "data": contract_data,
                "missing_keys": [m[0] for m in missing_ask],
                "missing_labels": [m[1] for m in missing_ask],
                "missing_idx": 0,
            }
            try:
                import json as _j
                db._execute("INSERT INTO pending_contracts (user_id, data) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET data=%s,created_at=NOW()", (user.id, _j.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str), _j.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str)))
            except Exception: pass
            found_info = []
            if contract_data["buyer_inn"]: found_info.append(f"ИНН: {contract_data['buyer_inn']}")
            if contract_data["buyer_ogrn"]: found_info.append(f"ОГРН: {contract_data['buyer_ogrn']}")
            if contract_data["buyer_bank"]: found_info.append(f"Банк: {contract_data['buyer_bank']}")
            found_str = " · ".join(found_info) if found_info else "реквизиты не найдены"

            msg = f"📄 *{contract_data['buyer_name']}*\n_{found_str}_\n\n"

            if missing_info:
                names = "\n".join(f"   • {v}" for _, v in missing_info)
                await message.reply_text(
                    f"📄 *{contract_data['buyer_name']}*\n\n"
                    f"❌ *Договор не сформирован* — в МойСклад отсутствуют:\n{names}\n\n"
                    f"Внеси данные в карточку МойСклад и запроси договор повторно.",
                    parse_mode="Markdown"
                )
                return

            if missing_ask:
                msg += f"*{missing_ask[0][1]}*?"
                await message.reply_text(msg, parse_mode="Markdown")
                return

        # Все данные есть — генерируем сразу
        await message.reply_text("✅ Все реквизиты найдены. Генерирую договор...")
        await _create_and_send_contract(contract_data, user.full_name, message, context)

    elif action == "manager_activity":
        days = int(params.get("days", 7))
        manager_filter = params.get("manager", "")
        if manager_filter.lower() in ("все", "all", ""):
            manager_filter = None

        # Маппинг имён → фамилий для поиска в БД
        NAME_MAP = {
            "инесса": "скляр", "скляр": "скляр", "скляр инесса ионасовна": "скляр",
            "карина": "баласанян", "баласанян": "баласанян", "баласанян карина владимировна": "баласанян",
                        "алексей": "леонтьев", "леонтьев": "леонтьев", "леонтьев алексей вадимович": "леонтьев",
            "елена": "мерзлякова", "лена": "мерзлякова", "мерзлякова": "мерзлякова", "мерзлякова елена владимировна": "мерзлякова",
        }
        if manager_filter:
            manager_filter = NAME_MAP.get(manager_filter.lower(), manager_filter.split()[0].lower() if manager_filter else manager_filter)

        await message.reply_chat_action("typing")
        rows = db.get_manager_activity(days=days, manager_name=manager_filter)

        if not rows:
            await message.reply_text(f"😕 Нет данных за последние {days} дней.")
            return

        from moysklad import get_manager_stats_ms, MANAGER_TAGS

        lines = [f"📊 *Активность менеджеров за {days} дней*\n"]
        for r in rows:
            mgr = r["manager"]
            msg_count = r.get("msg_count", 0)
            msg_clients = r.get("msg_clients", 0)
            call_count = r.get("call_count", 0)
            call_clients = r.get("call_clients", 0)
            avg_dur = r.get("avg_duration", 0)
            avg_str = f" · ср. {avg_dur//60}:{avg_dur%60:02d}" if avg_dur else ""

            # Уникальные клиенты по всем каналам
            total_unique = len(set(list(range(msg_clients)) + list(range(msg_clients, msg_clients + call_clients))))
            # Упрощённо: берём максимум из двух (точнее не посчитать без JOIN)
            total_unique = max(msg_clients, call_clients)

            # Ищем тег менеджера для запроса в МойСклад
            ms_tag = None
            for tag_key, tag_name in MANAGER_TAGS.items():
                if tag_key.lower() in mgr.lower() or mgr.lower() in tag_name.lower():
                    ms_tag = tag_key
                    break

            lines.append(f"👤 *{mgr}*")
            if msg_count:
                lines.append(f"  💬 Сообщений: {msg_count} ({msg_clients} клиентов)")
            if call_count:
                lines.append(f"  📞 Звонков: {call_count} ({call_clients} клиентов){avg_str}")
            lines.append(f"  🤝 Всего контактов за период: {total_unique}")

            # Данные из МойСклад
            if ms_tag:
                try:
                    ms_stats = await get_manager_stats_ms(ms_tag)
                    lines.append(f"  📋 База МойСклад: {ms_stats['total']} компаний")
                    lines.append(f"  🔥 Активных (60 дн): {ms_stats['active']} компаний")
                except Exception:
                    pass
            lines.append("")

        await message.reply_text("\n".join(lines), parse_mode="Markdown")

    elif action == "op_report":
        await cmd_op_report(update, context)
        return

    elif action == "aging":
        await cmd_aging(update, context)
        return

    elif action == "search_mentions":
        product = params.get("product", "")
        days = int(params.get("days", 7))
        manager_filter = params.get("manager", "")

        if not product:
            await message.reply_text("❌ Укажи товар для поиска.")
            return

        await message.reply_chat_action("typing")

        # Разбиваем на несколько товаров если через запятую
        # Добавляем словоформы — обрезаем до основы (первые 5+ символов)
        raw_keywords = [p.strip().lower() for p in product.replace(" и ", ",").split(",") if p.strip()]
        keywords = []
        for kw in raw_keywords:
            keywords.append(kw)
            # Добавляем основу слова если длиннее 5 символов
            if len(kw) > 5:
                keywords.append(kw[:5])
        keywords = list(set(keywords))

        rows = db.search_wazzup_mentions(keywords, days=days, manager_name=manager_filter or None)
        call_rows = db.search_call_mentions(keywords, days=days, manager_name=manager_filter or None)

        if not rows and not call_rows:
            await message.reply_text(
                f"😕 Упоминаний *{product}* за последние {days} дней не найдено.\n"
                f"_Данные накапливаются с момента подключения._",
                parse_mode="Markdown"
            )
            return

        lines = [f"🔍 *Упоминания «{product}»* за {days} дней\n"]

        # Переписки
        if rows:
            by_manager = {}
            unknown = []
            for row in rows:
                mgr = row.get("manager_name") or "Неизвестно"
                if mgr == "Неизвестно":
                    unknown.append(row)
                else:
                    by_manager.setdefault(mgr, []).append(row)

            lines.append("💬 *Переписки:*")
            for mgr, msgs in sorted(by_manager.items()):
                clients = list({r.get("client_name") or r.get("contact_name", "") for r in msgs if r.get("client_name") or r.get("contact_name")})
                lines.append(f"👤 *{mgr}* — {len(msgs)} сообщений, {len(clients)} клиентов:")
                for c in clients[:10]:
                    lines.append(f"  • {c}")
                if len(clients) > 10:
                    lines.append(f"  _...и ещё {len(clients)-10}_")
                lines.append("")

            if unknown:
                clients_unk = list({r.get("client_name") or r.get("contact_name", "") for r in unknown if r.get("client_name") or r.get("contact_name")})
                lines.append(f"❓ *Не идентифицированы* — {len(unknown)} сообщений, {len(clients_unk)} контактов:")
                for c in clients_unk[:10]:
                    lines.append(f"  • {c}")
                if len(clients_unk) > 10:
                    lines.append(f"  _...и ещё {len(clients_unk)-10}_")
                lines.append("")

        # Звонки
        if call_rows:
            by_manager_calls = {}
            for row in call_rows:
                mgr = row.get("manager_name") or "Неизвестно"
                by_manager_calls.setdefault(mgr, []).append(row)

            lines.append("📞 *Звонки:*")
            for mgr, calls in sorted(by_manager_calls.items()):
                clients = list({r.get("src_num", "") for r in calls if r.get("src_num")})
                lines.append(f"👤 *{mgr}* — {len(calls)} звонков, {len(clients)} клиентов:")
                for c in clients[:10]:
                    lines.append(f"  • {c}")
                if len(clients) > 10:
                    lines.append(f"  _...и ещё {len(clients)-10}_")
                lines.append("")

        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:3900] + "\n\n_...уточни запрос_"
        await message.reply_text(text, parse_mode="Markdown")

    elif action == "broadcast":
        product = params.get("product", "")
        broadcast_text = params.get("message", "")
        manager_filter = params.get("manager", "")

        if not product or not broadcast_text:
            await message.reply_text("❌ Не указан товар или текст сообщения.")
            return

        await message.reply_chat_action("typing")
        period_days = params.get("period_days", 180)
        await message.reply_text(
            f"🔍 Ищу клиентов которые покупали *{product}* за последние {period_days} дней...",
            parse_mode="Markdown"
        )

        # 1. Находим покупателей через МойСклад
        from moysklad import get_buyers_by_product, get_counterparty_phones
        result = await get_buyers_by_product(product, period_days=period_days)
        buyers = result.get("buyers", []) if isinstance(result, dict) else result
        found_name = result.get("product_name", product) if isinstance(result, dict) else product

        if not buyers:
            await message.reply_text(f"❌ Не найдено покупателей *{found_name}* за последние {period_days} дней.", parse_mode="Markdown")
            return

        await message.reply_text(f"📋 Найдено {len(buyers)} покупателей. Получаю телефоны...", parse_mode="Markdown")

        # 2. Получаем телефоны из МойСклад
        contacts = await get_counterparty_phones(buyers)
        with_phone = [c for c in contacts if c.get("phone")]
        no_phone = [c for c in contacts if not c.get("phone")]

        if not with_phone:
            await message.reply_text("❌ Ни у одного клиента нет телефона в МойСклад.")
            return

        # 3. Показываем список и просим подтверждение
        duration_min = len(with_phone)
        names_preview = "\n".join(f"• {c['name']} ({c['phone']})" for c in with_phone[:10])
        if len(with_phone) > 10:
            names_preview += f"\n_...и ещё {len(with_phone) - 10}_"

        no_phone_note = f"\n⚠️ Без телефона ({len(no_phone)}): {', '.join(c['name'] for c in no_phone[:5])}" if no_phone else ""

        confirm_text = (
            f"📣 *Рассылка готова*\n\n"
            f"*Товар:* {found_name}\n"
            f"*Текст:* _{broadcast_text}_\n\n"
            f"*Получатели ({len(with_phone)}):*\n{names_preview}{no_phone_note}\n\n"
            f"⏱ Рассылка займёт ~{duration_min} мин (1 сообщение в минуту)\n\n"
            f"Для подтверждения напиши: *да, рассылай*"
        )
        await message.reply_text(confirm_text, parse_mode="Markdown")

        # Сохраняем и показываем кнопку подтверждения
        import uuid as _uuid
        broadcast_key = str(_uuid.uuid4())[:8]
        _pending_sends[f"broadcast_{broadcast_key}"] = {
            "contacts": with_phone,
            "text": broadcast_text,
            "product": found_name,
            "is_broadcast": True,
        }

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Начать рассылку", callback_data=f"send_confirm|broadcast_{broadcast_key}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"send_cancel|broadcast_{broadcast_key}"),
        ]])
        await message.reply_text(confirm_text, parse_mode="Markdown", reply_markup=keyboard)

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
    """Ищет фото в канале Контент F2B по ключевым словам."""
    content_chat_id = int(os.getenv("CONTENT_CHAT_ID", "-1001433042091"))
    query_lower = query.lower()
    results = []
    seen = set()

    # Сначала ищем по полному запросу
    photos = db.search_media(query_lower, media_type="photo")
    logger.info(f"search_photo: query='{query_lower}' total_in_db={len(photos)}")
    for p in photos:
        logger.info(f"search_photo: chat_id={p.get('chat_id')} expected={content_chat_id} caption='{(p.get('caption') or '')[:40]}'")
        if p.get("chat_id") == content_chat_id and p["file_id"] not in seen:
            results.append({"file_id": p["file_id"], "caption": p.get("caption", "")})
            seen.add(p["file_id"])

    # Если не нашли — ищем по каждому значимому слову
    if not results:
        words = [w for w in query_lower.split() if len(w) >= 4]
        for word in words:
            photos = db.search_media(word, media_type="photo")
            logger.info(f"search_photo: word='{word}' found={len(photos)}")
            for p in photos:
                if p.get("chat_id") == content_chat_id and p["file_id"] not in seen:
                    results.append({"file_id": p["file_id"], "caption": p.get("caption", "")})
                    seen.add(p["file_id"])
            if results:
                break

    logger.info(f"search_photo: returning {len(results)} for '{query}'")
    return results

async def search_and_send_photo(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str):
    """Ищет фото товара в канале Контент F2B."""
    await update.message.reply_chat_action("upload_photo")

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

    await update.message.reply_text(f"😕 Фото *{query}* не найдено в канале Контент.", parse_mode="Markdown")

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

async def cmd_add_webhook(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Создаёт вебхуки в МойСклад. /add_webhook"""
    user = update.message.from_user
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]
    if user.id not in manager_ids:
        await update.message.reply_text("⛔ Только для руководителей.")
        return

    import aiohttp
    token = os.getenv("MOYSKLAD_TOKEN")
    webhook_url = "https://f2b-production.up.railway.app/webhook/moysklad"
    api_url = "https://api.moysklad.ru/api/remap/1.2/entity/webhook"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    results = []
    async with aiohttp.ClientSession() as session:
        for action, extra in [("CREATE", {}), ("UPDATE", {"diffType": "NONE"})]:
            payload = {"url": webhook_url, "action": action, "entityType": "customerorder", **extra}
            async with session.post(api_url, headers=headers, json=payload) as resp:
                data = await resp.json()
                if resp.status in (200, 201):
                    results.append(f"✅ {action}: id={data.get('id')}")
                else:
                    results.append(f"❌ {action}: {data}")

    await update.message.reply_text("Вебхуки МойСклад:\n" + "\n".join(results))
    """Тестовый запуск утренних задач ПДЗ. /pdz_test [имя|all]"""
    user = update.message.from_user
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]
    if user.id not in manager_ids:
        await update.message.reply_text("⛔ Только для руководителей.")
        return

    arg = (context.args[0].lower() if context.args else "all")

    from scheduler import pdz_morning_task
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

# Хранилище ожидающих отправки сообщений — message_key → {phone, name, text, chat_type}
_pending_sends: dict = {}

async def handle_send_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатие кнопки Отправить / Отменить для сообщений клиентам."""
    query = update.callback_query
    await query.answer()

    user = query.from_user
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    if user.id not in manager_ids:
        await query.answer("⛔ Только для руководителей.", show_alert=True)
        return

    parts = query.data.split("|")
    action = parts[0]
    msg_key = parts[1] if len(parts) > 1 else ""

    if action == "send_cancel":
        _pending_sends.pop(msg_key, None)
        await query.message.edit_text("❌ Отправка отменена.")
        return

    if action != "send_confirm":
        return

    pending = _pending_sends.pop(msg_key, None)
    if not pending:
        await query.message.edit_text("❌ Сообщение устарело — попробуй снова.")
        return

    api_key = os.getenv("WAZZUP_API_KEY", "")
    import aiohttp, uuid as _uuid

    # Каналы в порядке приоритета: WhatsApp → Max → Telegram
    CHANNEL_PRIORITY = [
        {"id": "e180aa1d-dc48-4d0a-bec3-fc0afc53cf03", "type": "whatsapp"},
        {"id": "1d5bc70a-7ca6-4895-8d1f-9690cf448214", "type": "max"},
        {"id": "ddd24a95-9304-4098-a320-3e47fcd1020a", "type": "telegram"},
    ]

    # Рассылка (несколько клиентов)
    if pending.get("is_broadcast"):
        contacts = pending["contacts"]
        product = pending["product"]
        broadcast_text = pending["text"]
        count = len(contacts)
        await query.message.edit_text(
            f"🚀 Начинаю рассылку по *{product}*\n📨 {count} получателей · ~{count} мин",
            parse_mode="Markdown"
        )

        async def run_wazzup_broadcast():
            sent, failed = 0, 0
            async with aiohttp.ClientSession() as session:
                for c in contacts:
                    phone = c.get("phone", "")
                    if not phone:
                        failed += 1
                        continue
                    try:
                        async with session.post(
                            "https://api.wazzup24.com/v3/message",
                            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                            json={
                                "channelId": channel_id,
                                "chatType": c.get("chat_type", "whatsapp"),
                                "chatId": phone,
                                "crmMessageId": str(_uuid.uuid4()),
                                "text": broadcast_text,
                            }
                        ) as resp:
                            if resp.status in (200, 201):
                                sent += 1
                            else:
                                failed += 1
                    except Exception:
                        failed += 1
                    await asyncio.sleep(60)

            result_text = f"✅ *Рассылка завершена!*\n📨 Отправлено: {sent}/{count}\n"
            if failed:
                result_text += f"❌ Не отправлено: {failed}\n"
            group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
            await context.bot.send_message(chat_id=group_chat_id or query.message.chat_id, text=result_text, parse_mode="Markdown")

        asyncio.create_task(run_wazzup_broadcast())
        return

    try:
        async with aiohttp.ClientSession() as session:
            sent_channel = None
            last_error = ""
            channels = pending.get("channels") or []
            for ch in channels:
                try:
                    async with session.post(
                        "https://api.wazzup24.com/v3/message",
                        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                        json={
                            "channelId": ch["channel_id"],
                            "chatType": ch["chat_type"],
                            "chatId": ch["chat_id"],
                            "crmMessageId": str(_uuid.uuid4()),
                            "text": pending["text"],
                        }
                    ) as resp:
                        if resp.status in (200, 201):
                            sent_channel = ch["chat_type"]
                            break
                        else:
                            body = await resp.text()
                            last_error = f"{resp.status}: {body[:100]}"
                            logger.warning(f"Wazzup {ch['chat_type']} failed: {last_error}")
                except Exception as e:
                    last_error = str(e)
                    logger.warning(f"Wazzup {ch['chat_type']} exception: {e}")

            if sent_channel:
                await query.message.edit_text(
                    f"✅ Сообщение отправлено *{pending['name']}* через {sent_channel}",
                    parse_mode="Markdown"
                )
                logger.info(f"Wazzup: отправлено {pending['name']} ({pending['phone']}) через {sent_channel}")
            else:
                await query.message.edit_text(f"❌ Не удалось отправить ни через один канал.\nПоследняя ошибка: {last_error}")
    except Exception as e:
        await query.message.edit_text(f"❌ Ошибка: {e}")

async def cmd_pdz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/pdz — ПДЗ в группу + задача менеджерам в личку."""
    user = update.message.from_user
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]
    if user.id not in manager_ids:
        await update.message.reply_text("⛔ Только для руководителей.")
        return

    from scheduler import pdz_morning_task, pdz_launched_today
    import asyncio
    from datetime import date as _date

    today = _date.today().isoformat()
    pdz_launched_today.add(today)

    # Подтягиваем chat_id менеджеров из БД
    managers_with_ids = []
    for mgr in PDZ_MANAGERS:
        mgr_copy = dict(mgr)
        cid = db.get_manager_chat_id(mgr.get("name_fragment", mgr["name"]))
        mgr_copy["chat_id"] = cid
        if not cid:
            logger.warning(f"cmd_pdz: нет chat_id для {mgr['name']}")
        managers_with_ids.append(mgr_copy)

    await update.message.reply_text(
        f"📋 Запускаю ПДЗ для {len(managers_with_ids)} менеджеров.\n"
        f"Интервал: 2 минуты. Результаты — боту в личку."
    )

    for i, mgr in enumerate(managers_with_ids):
        try:
            await pdz_morning_task(context.application, mgr)
        except Exception as e:
            logger.error(f"cmd_pdz ошибка для {mgr['name']}: {e}")
        if i < len(managers_with_ids) - 1:
            await asyncio.sleep(120)

    await update.message.reply_text("✅ Готово. Сводка результатов придёт в 17:00.")

async def cmd_delete_executor_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/del_tasks [имя] — удалить все открытые задачи конкретного исполнителя."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    if not context.args:
        return
    name = " ".join(context.args)
    db._ensure_connection()
    with db.conn.cursor() as cur:
        cur.execute(
            "UPDATE tasks SET status='done', completed_at=NOW(), result='Удалено — сотрудник уволен' "
            "WHERE LOWER(executor) LIKE LOWER(%s) AND status='open'",
            (f"%{name}%",)
        )
        count = cur.rowcount
    db.conn.commit()
    await update.message.reply_text(f"✅ Закрыто задач для *{name}*: {count}", parse_mode="Markdown")

async def cmd_test_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/test_group [название группы] — сумма продаж по группе товаров за текущий месяц по менеджерам."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return

    import aiohttp
    from datetime import date
    from moysklad import get_headers
    MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

    group_name_query = " ".join(context.args) if context.args else "ПРИВЛЕЧЕННЫЕ ТОВАРЫ"
    await update.message.reply_text(f"🔍 Ищу группу товаров {group_name_query}...")

    month_start = date.today().replace(day=1).isoformat()
    month_end   = date.today().isoformat()

    TAGS = {
        "скляр":      "Инесса",
        "мерзлякова": "Елена",
        "баласанян":  "Карина",
        "леонтьев":   "Алексей",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/productfolder",
                headers=get_headers(),
                params={"filter": f"name={group_name_query}", "limit": 10}
            ) as r:
                data = await r.json()

            folders = data.get("rows", [])
            if not folders:
                async with session.get(f"{MS_BASE}/entity/productfolder", headers=get_headers(), params={"limit": 100}) as r:
                    data = await r.json()
                all_folders = [f.get("name","") for f in data.get("rows",[])]
                await update.message.reply_text(f"Группа не найдена. Все группы:\n" + "\n".join(all_folders[:20]))
                return

            folder = folders[0]
            folder_href = folder.get("meta", {}).get("href", "")
            folder_name = folder.get("name", "")
            await update.message.reply_text(f"Группа найдена: {folder_name}\nСчитаю по менеджерам...")

            tag_to_ids = {}
            for tag in TAGS:
                ids = set()
                off = 0
                while True:
                    async with session.get(f"{MS_BASE}/entity/counterparty", headers=get_headers(),
                                           params={"filter": f"tags={tag}", "limit": 100, "offset": off}) as r:
                        d = await r.json()
                    rows = d.get("rows", [])
                    for cp in rows:
                        ids.add(cp.get("id", ""))
                    if len(rows) < 100:
                        break
                    off += 100
                tag_to_ids[tag] = ids

            mgr_sums = {tag: 0.0 for tag in TAGS}
            total_sum = 0.0
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/report/profit/bycounterparty",
                    headers=get_headers(),
                    params={"momentFrom": f"{month_start} 00:00:00", "momentTo": f"{month_end} 23:59:59",
                            "filter": f"productFolder={folder_href}", "limit": 200, "offset": offset}
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    cp_href = row.get("counterparty", {}).get("meta", {}).get("href", "")
                    cp_id = cp_href.split("/")[-1] if cp_href else ""
                    sell_sum = (row.get("sellSum", 0) or 0) / 100
                    total_sum += sell_sum
                    for tag, ids in tag_to_ids.items():
                        if cp_id in ids:
                            mgr_sums[tag] += sell_sum
                            break
                total = data.get("meta", {}).get("size", 0)
                offset += len(rows)
                if offset >= total or len(rows) < 200:
                    break

            lines = [f"📊 *{folder_name}* · март\n", f"Итого: *{total_sum:,.0f} руб.*\n"]
            for tag, short in TAGS.items():
                lines.append(f"  {short}: *{mgr_sums[tag]:,.0f}* руб.")
            await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def cmd_op_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/op_report — временная ссылка на интерактивный отчёт ОП."""
    token = db.create_report_link(mgr_filter=None, ttl_minutes=60)
    base = os.getenv("RAILWAY_PUBLIC_DOMAIN", "f2b-production.up.railway.app")
    url = f"https://{base}/report?token={token}"
    msg = update.effective_message
    if msg:
        await msg.reply_text(f"📊 Отчёт ОП (действует 1 час):\n{url}")
    # Обновляем кэш в фоне если он устарел
    if not db.get_report_cache():
        import asyncio
        asyncio.ensure_future(_refresh_report_cache())

async def cmd_test_fact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/test_fact [тег] — тест получения отгрузок по тегу за текущий месяц."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    tag = " ".join(context.args).lower() if context.args else "скляр"
    await update.message.reply_text(f"🔍 Считаю отгрузки за март по тегу {tag}...")

    import aiohttp
    from datetime import date
    from moysklad import get_headers
    MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

    month_start = date.today().replace(day=1).isoformat()
    month_end = date.today().isoformat()

    try:
        async with aiohttp.ClientSession() as session:
            cp_ids = set()
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/counterparty",
                    headers=get_headers(),
                    params={"filter": f"tags={tag}", "limit": 100, "offset": offset}
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for cp in rows:
                    cp_ids.add(cp.get("id", ""))
                if len(rows) < 100:
                    break
                offset += 100

            await update.message.reply_text(f"Контрагентов с тегом {tag}: {len(cp_ids)}")

            shipments = 0
            revenue = 0.0
            clients = set()
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"moment>={month_start} 00:00:00;moment<={month_end} 23:59:59",
                        "expand": "agent",
                        "limit": 200,
                        "offset": offset,
                    }
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    agent_href = row.get("agent", {}).get("meta", {}).get("href", "")
                    agent_id = agent_href.split("/")[-1] if agent_href else ""
                    if agent_id in cp_ids:
                        shipments += 1
                        revenue += (row.get("sum", 0) or 0) / 100
                        clients.add(agent_id)
                if len(rows) < 200:
                    break
                offset += 200

            # Имена клиентов
            client_names = []
            for cid in clients:
                async with session.get(
                    f"{MS_BASE}/entity/counterparty/{cid}",
                    headers=get_headers()
                ) as r:
                    cp = await r.json()
                    client_names.append(cp.get("name", cid))
            client_names.sort()

            await update.message.reply_text(
                f"Итог {tag} за март:\nОтгрузок: {shipments}\nВыручка: {revenue:,.0f} руб.\nКлиентов: {len(clients)}\n\n" +
                "\n".join(f"{i+1}. {n}" for i, n in enumerate(client_names))
            )
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

    import aiohttp
    from datetime import date
    from moysklad import get_headers
    MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

    month_start = date.today().replace(day=1).isoformat()
    month_end = date.today().isoformat()

    try:
        async with aiohttp.ClientSession() as session:
            # 1. Все контрагенты с тегом менеджера
            cp_ids = set()
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/counterparty",
                    headers=get_headers(),
                    params={"filter": f"tags={tag}", "limit": 100, "offset": offset}
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for cp in rows:
                    cp_ids.add(cp.get("id", ""))
                if len(rows) < 100:
                    break
                offset += 100

            await update.message.reply_text(f"Контрагентов с тегом *{tag}*: {len(cp_ids)}", parse_mode="Markdown")

            # 2. Все отгрузки за период — фильтруем по agent из cp_ids
            shipments = 0
            revenue = 0.0
            clients = set()
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"moment>={month_start} 00:00:00;moment<={month_end} 23:59:59",
                        "expand": "agent",
                        "limit": 200,
                        "offset": offset,
                    }
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    agent = row.get("agent", {})
                    agent_href = agent.get("meta", {}).get("href", "")
                    agent_id = agent_href.split("/")[-1] if agent_href else ""
                    if agent_id in cp_ids:
                        shipments += 1
                        revenue += (row.get("sum", 0) or 0) / 100
                        clients.add(agent_id)
                if len(rows) < 200:
                    break
                offset += 200

            await update.message.reply_text(
                f"📊 *Итог по тегу {tag} за март:*\n"
                f"🚚 Отгрузок: *{shipments}*\n"
                f"💰 Выручка: *{revenue:,.0f} руб.*\n"
                f"👥 Клиентов: *{len(clients)}*",
                parse_mode="Markdown"
            )
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def cmd_set_promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_promo [хорека|опт] [текст] — задать рекламный текст по сегменту."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return

    # Берём сырой текст сообщения чтобы не потерять URL и спецсимволы
    raw = update.message.text or ""
    # Убираем команду /set_promo
    raw_args = raw.split(" ", 1)[1].strip() if " " in raw else ""

    if not raw_args:
        хорека = db.get_promo("хорека")
        опт = db.get_promo("опт")
        общий = db.get_promo()
        msg = (
            "📢 *Текущие промо-тексты:*\n\n"
            f"*Хорека:*\n{хорека or '— не задан'}\n\n"
            f"*Опт:*\n{опт or '— не задан'}\n\n"
            f"*Общий:*\n{общий or '— не задан'}\n\n"
            "Чтобы изменить:\n"
            "`/set_promo хорека [текст]`\n"
            "`/set_promo опт [текст]`\n"
            "`/set_promo [текст]` — общий"
        )
        await update.message.reply_text(msg, parse_mode="Markdown")
        return

    first_word = raw_args.split(" ", 1)[0].lower()
    if first_word in ("хорека", "horeka", "опт", "opt"):
        segment = "хорека" if first_word in ("хорека", "horeka") else "опт"
        text = raw_args.split(" ", 1)[1].strip() if " " in raw_args else ""
        if not text:
            # Текст не указан — просим прислать следующим сообщением
            _user_awaiting[user.id] = f"promo_{segment}"
            await update.message.reply_text(
                f"📢 Отправь текст промо для *{segment}* следующим сообщением.\n"
                f"Можно использовать переносы строк, эмодзи и ссылки.",
                parse_mode="Markdown"
            )
            return
        db.set_promo(text, segment)
        await update.message.reply_text(f"✅ Промо для *{segment}* сохранён:\n\n{text}", parse_mode="Markdown")
    else:
        db.set_promo(raw_args)
        await update.message.reply_text(f"✅ Общий промо сохранён:\n\n{raw_args}")

# Временные токены для веб-отчёта: token → {expires, user_id, mgr_filter}
_report_tokens: dict = {}


async def cmd_refresh_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/refresh_history — принудительно обновить историю менеджеров."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    await update.message.reply_text("🔄 Собираю историю по месяцам, это займёт несколько минут...")
    try:
        from moysklad import get_manager_monthly_history
        TAGS = {
            "скляр": "Инесса Скляр",
            "мерзлякова": "Елена Мерзлякова",
            "баласанян": "Карина Баласанян",
            "леонтьев": "Алексей Леонтьев",
        }
        for tag, mgr_name in TAGS.items():
            hist = await get_manager_monthly_history(tag, mgr_name)
            db.set_mgr_history_cache(tag, hist)
            await update.message.reply_text(f"✅ {mgr_name}: {len(hist)} месяцев")
        # Сбрасываем кэш отчёта чтобы следующий запрос подхватил историю
        db._execute("DELETE FROM report_cache")
        await update.message.reply_text("✅ История обновлена. Запроси /web_report для нового отчёта.")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def cmd_reissue_contract(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/reissue_contract [название компании] — перегенерировать договор с теми же номером и датой."""
    user = update.effective_user
    OWNER_ID = 360092495
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    allowed_ids = manager_ids + [OWNER_ID]
    if not user or user.id not in allowed_ids:
        await update.message.reply_text("❌ У вас нет доступа к этой команде.")
        return
    if not context.args:
        await update.message.reply_text("Использование: /reissue_contract [название компании]")
        return

    buyer_query = " ".join(context.args)
    existing = db.find_contract_by_buyer(buyer_query)
    if not existing:
        await update.message.reply_text(f"❌ Договор с '{buyer_query}' не найден в базе.")
        return

    contract_number = existing["contract_number"]
    created_at = existing["created_at"]
    buyer_data = existing.get("buyer_data") or {}

    await update.message.reply_text(
        f"📄 Найден договор №{contract_number} от {created_at.strftime('%d.%m.%Y')}\n"
        f"🏢 {existing['buyer_name']}\n\n"
        f"Перегенерирую PDF с теми же номером и датой..."
    )

    try:
        from contract_generator import generate_contract_pdf
        import io

        # Восстанавливаем данные из БД
        if isinstance(buyer_data, str):
            import json
            buyer_data = json.loads(buyer_data)

        MONTHS_RU = ["января","февраля","марта","апреля","мая","июня",
                     "июля","августа","сентября","октября","ноября","декабря"]
        buyer_data["contract_number"] = contract_number
        buyer_data["contract_date"] = f"{created_at.day} {MONTHS_RU[created_at.month-1]} {created_at.year} г."

        pdf_bytes = generate_contract_pdf(buyer_data)

        group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
        target = group_chat_id or update.message.chat_id
        await context.bot.send_document(
            chat_id=target,
            document=io.BytesIO(pdf_bytes),
            filename=f"Договор_{contract_number}_{existing['buyer_name'][:30]}.pdf",
            caption=(
                f"📄 *Договор поставки № {contract_number}* (переизданный)\n"
                f"📅 {buyer_data['contract_date']}\n"
                f"🏢 {existing['buyer_name']}"
            ),
            parse_mode="Markdown"
        )
        if target != update.message.chat_id:
            await update.message.reply_text(f"✅ Договор № {contract_number} переиздан и отправлен в группу.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_refresh_contract(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/refresh_contract [название компании] — обновить реквизиты из МойСклад и переиздать договор с тем же номером и датой."""
    user = update.effective_user
    OWNER_ID = 360092495
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    allowed_ids = manager_ids + [OWNER_ID]
    if not user or user.id not in allowed_ids:
        await update.message.reply_text("❌ У вас нет доступа к этой команде.")
        return
    if not context.args:
        await update.message.reply_text(
            "Использование: /refresh_contract [название компании]\n"
            "Команда заново запросит реквизиты из МойСклад и переиздаст договор с сохранением номера и даты."
        )
        return

    buyer_query = " ".join(context.args)
    existing = db.find_contract_by_buyer(buyer_query)
    if not existing:
        await update.message.reply_text(f"❌ Договор с '{buyer_query}' не найден в базе.")
        return

    contract_number = existing["contract_number"]
    created_at = existing["created_at"]

    await update.message.reply_text(
        f"🔍 Найден договор №{contract_number} от {created_at.strftime('%d.%m.%Y')}\n"
        f"🏢 {existing['buyer_name']}\n\n"
        f"Запрашиваю актуальные реквизиты из МойСклад..."
    )

    try:
        from moysklad import get_counterparty_requisites, get_counterparty_balance
        # Используем get_counterparty_balance для поиска контрагента по имени
        _found_list = await get_counterparty_balance(buyer_query)
        if not _found_list:
            await update.message.reply_text(f"❌ Компания '{buyer_query}' не найдена в МойСклад.")
            return
        cp_id = _found_list[0]["id"]
        reqs = await get_counterparty_requisites(cp_id)

        MONTHS_RU_RC = ["января","февраля","марта","апреля","мая","июня",
                        "июля","августа","сентября","октября","ноября","декабря"]

        # Старые данные из БД — используем как fallback для полей которых нет в МойСклад
        old_data = existing.get("buyer_data") or {}
        if isinstance(old_data, str):
            import json as _j2
            old_data = _j2.loads(old_data)

        contract_data = {
            "buyer_name": reqs.get("buyer_legal_title") or reqs.get("buyer_name", buyer_query),
            "buyer_legal_title": reqs.get("buyer_legal_title") or reqs.get("buyer_name", buyer_query),
            "buyer_inn": reqs.get("buyer_inn", "") or old_data.get("buyer_inn", ""),
            "buyer_ogrn": reqs.get("buyer_ogrn", "") or old_data.get("buyer_ogrn", ""),
            "buyer_address": reqs.get("buyer_address", "") or old_data.get("buyer_address", ""),
            "buyer_bank": reqs.get("buyer_bank", "") or old_data.get("buyer_bank", ""),
            "buyer_rs": reqs.get("buyer_rs", "") or old_data.get("buyer_rs", ""),
            "buyer_bik": reqs.get("buyer_bik", "") or old_data.get("buyer_bik", ""),
            "buyer_ks": reqs.get("buyer_ks", "") or old_data.get("buyer_ks", ""),
            "buyer_phone": reqs.get("buyer_phone", "") or old_data.get("buyer_phone", ""),
            "buyer_email": reqs.get("buyer_email", "") or old_data.get("buyer_email", ""),
            # ФИО директора — ТОЛЬКО из МойСклад, не из старых данных
            # (старые данные могут содержать неполное ФИО без фамилии)
            "buyer_representative": reqs.get("buyer_representative", ""),
            "buyer_director_name": reqs.get("buyer_director_name", ""),
            "buyer_basis": reqs.get("buyer_basis", "") or old_data.get("buyer_basis", "Устава"),
            # Сохраняем ОРИГИНАЛЬНЫЕ номер и дату
            "contract_number": contract_number,
            "contract_date": f"{created_at.day} {MONTHS_RU_RC[created_at.month-1]} {created_at.year} г.",
        }

        # Для ИП: собираем полное ФИО если в buyer_name нет фамилии
        _ip_last = reqs.get("buyer_last_name", "")
        _ip_first = reqs.get("buyer_first_name", "")
        _ip_mid = reqs.get("buyer_middle_name", "")
        if _ip_last and _ip_first and _ip_last not in contract_data["buyer_name"]:
            _full_fio = " ".join(filter(None, [_ip_last, _ip_first, _ip_mid]))
            contract_data["buyer_name"] = (contract_data["buyer_name"] + " " + _full_fio).strip() if contract_data["buyer_name"] else _full_fio

        # Если ФИО директора не нашли или нет фамилии — спрашиваем вручную
        _director_ok = (
            contract_data.get("buyer_director_name") and
            len(contract_data["buyer_director_name"].split()) >= 2 and
            contract_data.get("buyer_representative")
        )
        if not _director_ok:
            # Сначала очищаем любую старую запись
            _pending_contracts.pop(user.id, None)
            db._execute("DELETE FROM pending_contracts WHERE user_id=%s", (user.id,))

            _pending_contracts[user.id] = {
                "data": contract_data,
                "missing_keys": ["buyer_representative"],
                "missing_labels": ["должность и полное ФИО директора (напр. 'генерального директора Иванову Марию Алексеевну' — обязательно с фамилией!)"],
                "missing_idx": 0,
                "mode": "refresh",
            }
            import json as _j_rc
            db._execute(
                "INSERT INTO pending_contracts (user_id, data) VALUES (%s,%s) ON CONFLICT (user_id) DO UPDATE SET data=%s,created_at=NOW()",
                (user.id, _j_rc.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str),
                 _j_rc.dumps(_pending_contracts[user.id], ensure_ascii=False, default=str))
            )
            await update.message.reply_text(
                f"📄 *{contract_data['buyer_name']}*\n\n"
                f"В МойСклад не заполнено ФИО директора.\n\n"
                f"*Напиши должность и полное ФИО директора*\n"
                f"_(напр. 'генерального директора Иванову Марию Алексеевну')_",
                parse_mode="Markdown"
            )
            return

        from contract_generator import generate_contract_pdf
        import io as _io_rc

        pdf_bytes = generate_contract_pdf(contract_data)

        # Обновляем данные в БД
        import json as _j3
        db.save_contract(contract_number, contract_data["buyer_name"], user.full_name, buyer_data=contract_data)

        group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
        target = group_chat_id or update.message.chat_id
        await context.bot.send_document(
            chat_id=target,
            document=_io_rc.BytesIO(pdf_bytes),
            filename=f"Договор_{contract_number}_{contract_data['buyer_name'][:30]}.pdf",
            caption=(
                f"📄 *Договор поставки № {contract_number}* (переиздан, данные обновлены)\n"
                f"📅 {contract_data['contract_date']}\n"
                f"🏢 {contract_data['buyer_name']}\n"
                f"👤 {user.full_name}"
            ),
            parse_mode="Markdown"
        )
        if target != update.message.chat_id:
            await update.message.reply_text(f"✅ Договор № {contract_number} переиздан с актуальными данными и отправлен в группу.")
    except Exception as e:
        logger.error(f"cmd_refresh_contract: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_refresh_cache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/refresh_cache — принудительно обновить кэш отчёта ОП."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    await update.message.reply_text("🔄 Запускаю обновление кэша... (займёт несколько минут)")
    import asyncio
    asyncio.ensure_future(_refresh_report_cache())


async def cmd_web_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/web_report — временная ссылка на интерактивный отчёт ОП (1 час)."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    token = db.create_report_link(mgr_filter=None, ttl_minutes=60)
    base = os.getenv("RAILWAY_PUBLIC_DOMAIN", "f2b-production.up.railway.app")
    url = f"https://{base}/report?token={token}"
    await update.effective_message.reply_text(
        f"📊 Отчёт ОП (ссылка действует 1 час):\n{url}"
    )


async def cmd_lost_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/lost_clients — клиенты которые грузились в прошлом месяце но не грузились в этом."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return

    await update.message.reply_text("🔍 Ищу выбывших клиентов...")

    import aiohttp
    from datetime import date
    from moysklad import get_headers, MS_BASE

    today = date.today()
    # Текущий месяц
    month_start = today.replace(day=1).isoformat()
    # Прошлый месяц
    if today.month == 1:
        prev_month_start = date(today.year - 1, 12, 1).isoformat()
        prev_month_end = date(today.year, 1, 1).isoformat()
    else:
        prev_month_start = date(today.year, today.month - 1, 1).isoformat()
        prev_month_end = month_start

    MANAGERS = {
        "скляр":      "Инесса",
        "мерзлякова": "Елена",
        "баласанян":  "Карина",
        "леонтьев":   "Алексей",
    }

    try:
        async with aiohttp.ClientSession() as session:
            # Контрагенты каждого менеджера
            tag_to_ids = {}
            for tag in MANAGERS:
                ids = set()
                off = 0
                while True:
                    async with session.get(
                        f"{MS_BASE}/entity/counterparty",
                        headers=get_headers(),
                        params={"filter": f"tags={tag}", "limit": 100, "offset": off}
                    ) as r:
                        data = await r.json()
                    rows = data.get("rows", [])
                    for cp in rows:
                        ids.add(cp.get("id", ""))
                    if len(rows) < 100:
                        break
                    off += 100
                tag_to_ids[tag] = ids

            all_mgr_ids = set().union(*tag_to_ids.values())

            # Клиенты у кого были отгрузки в ПРОШЛОМ месяце
            prev_clients = {}  # agent_id → href
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"moment>={prev_month_start} 00:00:00;moment<{prev_month_end} 00:00:00",
                        "expand": "agent",
                        "limit": 200,
                        "offset": offset,
                    }
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    agent_href = row.get("agent", {}).get("meta", {}).get("href", "")
                    agent_id = agent_href.split("/")[-1] if agent_href else ""
                    if agent_id and agent_id in all_mgr_ids:
                        prev_clients[agent_id] = agent_href
                if len(rows) < 200:
                    break
                offset += 200

            # Клиенты у кого были отгрузки в ТЕКУЩЕМ месяце
            curr_clients = set()
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"moment>={month_start} 00:00:00;moment<={today.isoformat()} 23:59:59",
                        "expand": "agent",
                        "limit": 200,
                        "offset": offset,
                    }
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    agent_href = row.get("agent", {}).get("meta", {}).get("href", "")
                    agent_id = agent_href.split("/")[-1] if agent_href else ""
                    if agent_id:
                        curr_clients.add(agent_id)
                if len(rows) < 200:
                    break
                offset += 200

            # Выбывшие = были в прошлом месяце но нет в текущем
            lost_ids = {aid: href for aid, href in prev_clients.items() if aid not in curr_clients}

            if not lost_ids:
                await update.message.reply_text("✅ Выбывших клиентов нет.")
                return

            # Получаем имена и группируем по менеджеру
            by_mgr = {}
            for agent_id, agent_href in lost_ids.items():
                mgr = "Без менеджера"
                for tag, short in MANAGERS.items():
                    if agent_id in tag_to_ids.get(tag, set()):
                        mgr = short
                        break
                async with session.get(agent_href, headers=get_headers()) as r:
                    cp = await r.json()
                name = cp.get("name", agent_id)
                by_mgr.setdefault(mgr, []).append(name)

            total = sum(len(v) for v in by_mgr.values())
            prev_label = date(today.year, today.month - 1 if today.month > 1 else 12, 1).strftime("%B")
            lines = [f"📉 *Выбывшие клиенты* (грузились в {prev_label}, нет в марте) — {total} чел.\n"]
            for mgr, names in sorted(by_mgr.items()):
                lines.append(f"*{mgr}* ({len(names)}):")
                for n in sorted(names):
                    lines.append(f"  • {n}")
                lines.append("")

            await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def cmd_new_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/new_clients — список новых клиентов текущего месяца."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return

    await update.message.reply_text("🔍 Ищу новых клиентов за текущий месяц...")

    import aiohttp
    from datetime import date
    from moysklad import get_headers, MS_BASE

    today = date.today()
    month_start = today.replace(day=1).isoformat()

    MANAGERS = {
        "скляр":      "Инесса",
        "мерзлякова": "Елена",
        "баласанян":  "Карина",
        "леонтьев":   "Алексей",
    }

    try:
        async with aiohttp.ClientSession() as session:
            # Собираем контрагентов каждого менеджера
            tag_to_ids = {}
            for tag in MANAGERS:
                ids = set()
                off = 0
                while True:
                    async with session.get(
                        f"{MS_BASE}/entity/counterparty",
                        headers=get_headers(),
                        params={"filter": f"tags={tag}", "limit": 100, "offset": off}
                    ) as r:
                        data = await r.json()
                    rows = data.get("rows", [])
                    for cp in rows:
                        ids.add(cp.get("id", ""))
                    if len(rows) < 100:
                        break
                    off += 100
                tag_to_ids[tag] = ids

            # Клиенты у кого были отгрузки в текущем месяце
            month_clients = {}  # agent_id → agent_href
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"moment>={month_start} 00:00:00;moment<={today.isoformat()} 23:59:59",
                        "expand": "agent",
                        "limit": 200,
                        "offset": offset,
                    }
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    agent_href = row.get("agent", {}).get("meta", {}).get("href", "")
                    agent_id = agent_href.split("/")[-1] if agent_href else ""
                    if agent_id:
                        month_clients[agent_id] = agent_href
                if len(rows) < 200:
                    break
                offset += 200

            # Проверяем у кого не было отгрузок до месяца
            by_mgr = {}
            for agent_id, agent_href in month_clients.items():
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"agent={agent_href};moment<{month_start} 00:00:00",
                        "limit": 1,
                    }
                ) as r:
                    prev = await r.json()
                if prev.get("meta", {}).get("size", 0) == 0:
                    # Новый клиент — определяем менеджера
                    mgr = "Без менеджера"
                    for tag, short in MANAGERS.items():
                        if agent_id in tag_to_ids.get(tag, set()):
                            mgr = short
                            break
                    # Получаем имя
                    async with session.get(agent_href, headers=get_headers()) as r2:
                        cp = await r2.json()
                    name = cp.get("name", agent_id)
                    by_mgr.setdefault(mgr, []).append(name)

            if not by_mgr:
                await update.message.reply_text("Новых клиентов не найдено.")
                return

            total = sum(len(v) for v in by_mgr.values())
            lines = [f"🆕 *Новые клиенты за {today.strftime('%B')}* — {total} чел.\n"]
            for mgr, names in sorted(by_mgr.items()):
                lines.append(f"*{mgr}* ({len(names)}):")
                for n in sorted(names):
                    lines.append(f"  • {n}")
                lines.append("")

            await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def cmd_test_publink(update, context):
    """/test_publink — создаём публичную ссылку на PDF заказа."""
    import aiohttp
    from moysklad import get_headers, MS_BASE
    from telegram.ext import ContextTypes
    from telegram import Update

    user = update.effective_user
    if not user or user.id != 360092495:
        return

    order_id = context.args[0] if context.args else "ff0588e7-2766-11f1-0a80-01a1000aa0eb"
    await update.message.reply_text("🔍 Создаю публичную ссылку на PDF заказа...")

    try:
        async with aiohttp.ClientSession() as session:
            tmpl_href = f"{MS_BASE}/entity/customerorder/metadata/embeddedtemplate/ff0ad2ff-1883-4bc2-ba2f-6fe91686fc1b"

            async with session.post(
                f"{MS_BASE}/entity/customerorder/{order_id}/publications",
                headers=get_headers(),
                json={"template": {"meta": {
                    "href": tmpl_href,
                    "type": "embeddedtemplate",
                    "mediaType": "application/json"
                }}}
            ) as r:
                status = r.status
                body = await r.json() if r.status in (200, 201) else await r.text()

            await update.message.reply_text(f"Статус: {status}\n{str(body)[:500]}")

            if status in (200, 201) and isinstance(body, dict):
                pub_href = body.get("href", "")
                await update.message.reply_text(f"Публичная ссылка:\n{pub_href}")

    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def cmd_unlink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/unlink [chat_id или имя контакта] — удаляет контакт из базы идентификации."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    if not context.args:
        await update.message.reply_text("Использование: /unlink [chat_id]\nПример: /unlink 1293122998")
        return

    query_val = " ".join(context.args)

    # Ищем по chat_id, contact_name, wazzup_name, company_name
    rows = db._fetchall(
        """SELECT chat_id, wazzup_name, company_name 
           FROM wazzup_contact_map 
           WHERE chat_id::text = %s 
              OR LOWER(wazzup_name) LIKE LOWER(%s)
              OR LOWER(company_name) LIKE LOWER(%s)""",
        (query_val, f"%{query_val}%", f"%{query_val}%")
    )
    if not rows:
        # Показываем последние 5 записей для диагностики
        recent = db._fetchall(
            "SELECT chat_id, wazzup_name, company_name FROM wazzup_contact_map ORDER BY created_at DESC LIMIT 5"
        )
        hint = "\n".join([f"• {r['wazzup_name']} / {r['company_name']} ({r['chat_id']})" for r in recent]) if recent else "—"
        await update.message.reply_text(
            f"❌ Контакт не найден: {query_val}\n\n"
            f"Последние 5 записей в базе:\n{hint}"
        )
        return

    for r in rows:
        db._execute("DELETE FROM wazzup_contact_map WHERE chat_id=%s", (r["chat_id"],))
        await update.message.reply_text(
            f"✅ Удалён: {r['wazzup_name']} / {r['company_name']} (chat_id: {r['chat_id']})"
        )

async def cmd_relink(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/relink [chat_id] [новое имя компании] — перепривязывает контакт к другой компании."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    if len(context.args) < 2:
        await update.message.reply_text(
            "Использование: /relink [chat_id] [название компании]\n"
            "Пример: /relink 1293122998 ИП Махнев"
        )
        return

    chat_id = context.args[0]
    company_name = " ".join(context.args[1:])

    # Обновляем company_name в wazzup_contact_map
    db._execute(
        "UPDATE wazzup_contact_map SET company_name=%s WHERE chat_id=%s",
        (company_name, chat_id)
    )
    rows = db._fetchall(
        "SELECT chat_id, contact_name, company_name, manager FROM wazzup_contact_map WHERE chat_id=%s",
        (chat_id,)
    )
    if rows:
        r = rows[0]
        await update.message.reply_text(
            f"✅ Контакт перепривязан:\n"
            f"chat_id: {r['chat_id']}\n"
            f"Контакт: {r['wazzup_name']}\n"
            f"Компания: {r['company_name']}\n"
            f"Менеджер: {r['manager']}"
        )
    else:
        await update.message.reply_text(f"❌ Контакт с chat_id={chat_id} не найден")

async def cmd_sync_managers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/sync_managers — обновляет менеджеров в базе по тегам МойСклад."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return

    await update.message.reply_text("🔄 Синхронизирую менеджеров...")
    updated = await sync_contact_managers()
    await update.message.reply_text(f"✅ Обновлено контактов: {updated}")

async def sync_contact_managers() -> int:
    """Обновляет поле manager в wazzup_contact_map по тегам из МойСклад."""
    import aiohttp
    from moysklad import get_headers, MS_BASE

    TAG_TO_NAME = {
        "скляр":      "Инесса Скляр",
        "мерзлякова": "Елена Мерзлякова",
        "баласанян":  "Карина Баласанян",
        "леонтьев":   "Алексей Леонтьев",
    }

    updated = 0
    try:
        # Берём все контакты из нашей БД
        contacts = db.get_all_contacts_with_company()
        if not contacts:
            return 0

        async with aiohttp.ClientSession() as session:
            for contact in contacts:
                company_name = contact.get("company_name", "")
                if not company_name:
                    continue
                # Ищем в МойСклад
                async with session.get(
                    f"{MS_BASE}/entity/counterparty",
                    headers=get_headers(),
                    params={"search": company_name, "limit": 1}
                ) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
                rows = data.get("rows", [])
                if not rows:
                    continue
                tags = [t.lower() for t in rows[0].get("tags", [])]
                mgr = next((TAG_TO_NAME[t] for t in tags if t in TAG_TO_NAME), None)
                if mgr and mgr != contact.get("manager"):
                    db.update_contact_manager(company_name, mgr)
                    updated += 1
                    logger.info(f"sync_managers: {company_name} → {mgr}")

    except Exception as e:
        logger.error(f"sync_contact_managers: {e}", exc_info=True)

    return updated

async def cmd_search_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/search_msg [слово] — ищет сообщения в БД без ограничения по дате."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    query = " ".join(context.args).lower() if context.args else ""
    if not query:
        await update.message.reply_text("Использование: /search_msg [слово]")
        return

    rows = db._fetchall(
        """SELECT manager_name, contact_name, text, sent_at, is_outbound
           FROM wazzup_messages
           WHERE LOWER(text) LIKE %s
           ORDER BY sent_at DESC LIMIT 10""",
        (f"%{query}%",)
    )
    if not rows:
        await update.message.reply_text(f"В БД нет сообщений со словом '{query}'")
        return

    lines = [f"Найдено {len(rows)} сообщений со словом '{query}':\n"]
    for r in rows:
        direction = "→" if r["is_outbound"] else "←"
        lines.append(f"{direction} {r['manager_name'] or '?'} / {r['wazzup_name']} [{r['sent_at']}]\n  {r['text'][:80]}")
    await update.message.reply_text("\n".join(lines))

async def cmd_aging(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/aging — показать список стареющих клиентов (40+ дней без отгрузок)."""
    msg = update.effective_message
    if not msg:
        return

    await msg.reply_text("🔍 Ищу стареющих клиентов...")

    from moysklad import get_aging_clients
    MANAGER_TAG_MAP = {
        "баласанян": "Карина", "мерзлякова": "Елена",
        "скляр": "Инесса", "леонтьев": "Алексей",
    }

    try:
        clients = await get_aging_clients(days=40)
        if not clients:
            await msg.reply_text("✅ Стареющих клиентов нет.")
            return

        # Группируем по менеджеру
        by_mgr = {}
        for c in clients:
            mgr = "Без менеджера"
            for tag in c.get("tags", []):
                if tag.lower() in MANAGER_TAG_MAP:
                    mgr = MANAGER_TAG_MAP[tag.lower()]
                    break
            by_mgr.setdefault(mgr, []).append(c)

        lines = [f"⚠️ *Стареющие клиенты (40+ дней)* — {len(clients)} чел.\n"]
        for mgr, cl_list in sorted(by_mgr.items()):
            lines.append(f"*{mgr}* ({len(cl_list)}):")
            for c in sorted(cl_list, key=lambda x: -x.get("days", 0)):
                days = c.get("days", c.get("days_ago", "?"))
                lines.append(f"  • {c['name']} — {days} дн.")
            lines.append("")

        text = "\n".join(lines)
        if len(text) > 4000:
            for i in range(0, len(text), 4000):
                await msg.reply_text(text[i:i+4000], parse_mode="Markdown")
        else:
            await msg.reply_text(text, parse_mode="Markdown")

    except Exception as e:
        await msg.reply_text(f"Ошибка: {e}")


async def cmd_set_attestation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_attestation [имя] [общая|акб] [процент] — задать аттестацию менеджера."""
    if not update.effective_user or update.effective_user.id != 360092495:
        return
    if len(context.args) < 3:
        await update.message.reply_text(
            "Использование: /set_attestation [имя] [общая|акб] [процент]\n"
            "Пример: /set_attestation Карина общая 35"
        )
        return
    name_part = context.args[0].lower()
    kind = context.args[1].lower()
    try:
        value = int(context.args[2])
    except ValueError:
        await update.message.reply_text("❌ Процент должен быть целым числом")
        return
    NAME_MAP = {
        "инесса": "Инесса Скляр", "скляр": "Инесса Скляр",
        "карина": "Карина Баласанян", "баласанян": "Карина Баласанян",
        "елена": "Елена Мерзлякова", "лена": "Елена Мерзлякова", "мерзлякова": "Елена Мерзлякова",
        "алексей": "Алексей Леонтьев", "леонтьев": "Алексей Леонтьев",
        "денис": "Денис Коликов", "коликов": "Денис Коликов",
    }
    mgr_name = NAME_MAP.get(name_part)
    if not mgr_name:
        await update.message.reply_text(f"❌ Менеджер '{context.args[0]}' не найден.")
        return
    if kind in ("общая", "general"):
        key = f"attestation_general_{mgr_name}"
        label = "общая"
    elif kind in ("акб", "akb"):
        key = f"attestation_akb_{mgr_name}"
        label = "АКБ"
    else:
        await update.message.reply_text("❌ Тип должен быть 'общая' или 'акб'")
        return
    db._execute(
        "INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=%s",
        (key, str(value), str(value))
    )
    await update.message.reply_text(f"✅ Аттестация {label} для {mgr_name}: {value}%")


def get_weekly_targets(mgr_name: str) -> dict:
    """Возвращает накопительные недельные цели менеджера из БД."""
    result = {}
    for metric in ["revenue", "shipments", "clients", "new_clients", "attracted"]:
        try:
            row = db._fetchone("SELECT value FROM bot_settings WHERE key=%s", (f"weekly_target_{mgr_name}_{metric}",))
            if row:
                result[metric] = float(row["value"])
        except Exception:
            pass
    return result


async def cmd_set_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_weekly [имя] [показатель] [значение] — задать накопительный недельный план."""
    if not update.effective_user or update.effective_user.id != 360092495:
        return
    if len(context.args) < 3:
        await update.message.reply_text(
            "Использование: /set_weekly [имя] [показатель] [значение]\n"
            "Показатели: выручка, отгрузки, акб, новые, привл\n"
            "Пример: /set_weekly Инесса выручка 4000000"
        )
        return
    name_part = context.args[0].lower()
    metric_part = context.args[1].lower()
    try:
        value = float(context.args[2].replace(',', '.'))
    except ValueError:
        await update.message.reply_text("❌ Значение должно быть числом")
        return
    NAME_MAP = {
        "инесса": "Инесса Скляр", "скляр": "Инесса Скляр",
        "карина": "Карина Баласанян", "баласанян": "Карина Баласанян",
        "елена": "Елена Мерзлякова", "лена": "Елена Мерзлякова", "мерзлякова": "Елена Мерзлякова",
        "алексей": "Алексей Леонтьев", "леонтьев": "Алексей Леонтьев",
        "денис": "Денис Коликов", "коликов": "Денис Коликов",
    }
    METRIC_MAP = {
        "выручка": "revenue", "revenue": "revenue",
        "отгрузки": "shipments", "отгрузка": "shipments", "shipments": "shipments",
        "акб": "clients", "клиенты": "clients", "clients": "clients",
        "новые": "new_clients", "новых": "new_clients", "new_clients": "new_clients",
        "привл": "attracted", "привлеченные": "attracted", "attracted": "attracted",
    }
    mgr_name = NAME_MAP.get(name_part)
    if not mgr_name:
        await update.message.reply_text(f"❌ Менеджер '{context.args[0]}' не найден.")
        return
    metric_key = METRIC_MAP.get(metric_part)
    if not metric_key:
        await update.message.reply_text(f"❌ Показатель '{context.args[1]}' не найден.\nДоступные: выручка, отгрузки, акб, новые, привл")
        return
    key = f"weekly_target_{mgr_name}_{metric_key}"
    db._execute(
        "INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=%s",
        (key, str(value), str(value))
    )
    METRIC_LABELS = {"revenue": "Выручка", "shipments": "Отгрузки", "clients": "АКБ", "new_clients": "Новые", "attracted": "Привл. товары"}
    label = METRIC_LABELS.get(metric_key, metric_key)
    await update.message.reply_text(f"✅ Недельный план *{label}* для *{mgr_name}*: {value:,.0f}", parse_mode="Markdown")



async def cmd_check_webhooks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/check_webhooks — проверить вебхуки МойСклад."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    try:
        import aiohttp
        from moysklad import get_headers, MS_BASE
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{MS_BASE}/entity/webhook", headers=get_headers()) as resp:
                if resp.status != 200:
                    await update.message.reply_text(f"❌ Ошибка запроса: {resp.status}")
                    return
                data = await resp.json()
        rows = data.get("rows", [])
        if not rows:
            await update.message.reply_text("❌ Вебхуки не найдены в МойСклад!")
            return
        lines = [f"📋 Вебхуки МойСклад ({len(rows)} шт):\n"]
        for w in rows:
            enabled = "✅" if w.get("enabled") else "❌"
            url = w.get("url", "")
            action = w.get("action", "")
            entity = w.get("entityType", "")
            lines.append(f"{enabled} {entity} / {action}\n   {url}")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_managers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/managers — показать всех зарегистрированных менеджеров и их chat_id."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    rows = db._fetchall(
        "SELECT user_id, full_name, is_blocked FROM manager_chats ORDER BY full_name"
    )
    if not rows:
        await update.message.reply_text("❌ Менеджеры не найдены.")
        return
    lines = ["👥 *Зарегистрированные менеджеры:*\n"]
    for r in rows:
        blocked = " 🚫" if r.get("is_blocked") else ""
        lines.append(f"• {r['full_name']}{blocked}\n  ID: `{r['user_id']}`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_reset_agreed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/reset_agreed [order_id или номер заказа] — сбросить флаг отправки уведомления."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    if not context.args:
        await update.message.reply_text("Использование: /reset_agreed [order_id или номер, например 01559]")
        return
    query = " ".join(context.args)
    if len(query) > 20 and "-" in query:
        db._execute("DELETE FROM agreed_notifications WHERE order_id=%s", (query,))
        await update.message.reply_text(f"✅ Флаг сброшен для {query}")
        return
    try:
        import aiohttp
        from moysklad import get_headers, MS_BASE
        await update.message.reply_text(f"🔍 Ищу заказ №{query}...")
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/customerorder",
                headers=get_headers(),
                params={"filter": f"name~{query}", "limit": 5}
            ) as resp:
                data = await resp.json()
        rows = data.get("rows", [])
        if not rows:
            await update.message.reply_text(f"❌ Заказ №{query} не найден в МойСклад.")
            return
        results = []
        for row in rows:
            order_id = row["id"]
            order_name = row.get("name", "")
            agent_name = row.get("agent", {}).get("name", "")
            db._execute("DELETE FROM agreed_notifications WHERE order_id=%s", (order_id,))
            results.append(f"✅ №{order_name} — {agent_name}\n   ID: `{order_id}`")
        await update.message.reply_text(
            f"Флаги сброшены:\n\n" + "\n".join(results) +
            "\n\nТеперь смени статус заказа на «Согласовано» — уведомление отправится.",
            parse_mode="Markdown"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_fix_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/fix_channels — найти и исправить записи с пустым channel_id."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    TELEGRAM_CHANNEL_ID = "ddd24a95-9304-4098-a320-3e47fcd1020a"
    rows = db._fetchall(
        "SELECT chat_id, company_name, chat_type FROM wazzup_contact_map WHERE channel_id='' OR channel_id IS NULL"
    )
    if not rows:
        await update.message.reply_text("✅ Все записи имеют channel_id — всё в порядке.")
        return
    db._execute(
        "UPDATE wazzup_contact_map SET channel_id=%s WHERE channel_id='' OR channel_id IS NULL",
        (TELEGRAM_CHANNEL_ID,)
    )
    lines = [f"✅ Исправлено {len(rows)} записей с пустым channel_id:\n"]
    for r in rows:
        lines.append(f"• {r['company_name']} ({r['chat_type']}, {r['chat_id']})")
    await update.message.reply_text("\n".join(lines))


async def cmd_relink_max(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/relink_max [chat_id] [компания] — привязать MAX контакт."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    if len(context.args) < 2:
        await update.message.reply_text("Использование: /relink_max [phone] [название компании]")
        return
    chat_id = context.args[0]
    company_name = " ".join(context.args[1:])
    MAX_CHANNEL_ID = "1d5bc70a-7ca6-4895-8d1f-9690cf448214"
    db._execute(
        """INSERT INTO wazzup_contact_map (chat_id, company_name, chat_type, channel_id, wazzup_name)
           VALUES (%s, %s, 'max', %s, %s)
           ON CONFLICT (chat_id) DO UPDATE SET company_name=%s, channel_id=%s, chat_type='max'""",
        (chat_id, company_name, MAX_CHANNEL_ID, company_name, company_name, MAX_CHANNEL_ID)
    )
    await update.message.reply_text(
        f"✅ MAX контакт привязан:\n"
        f"chat_id: {chat_id}\n"
        f"Компания: {company_name}\n"
        f"Канал: MAX"
    )


async def cmd_relink_wa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/relink_wa [phone] [компания] — привязать WhatsApp контакт."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return
    if len(context.args) < 2:
        await update.message.reply_text("Использование: /relink_wa [телефон] [название компании]")
        return
    chat_id = context.args[0]
    company_name = " ".join(context.args[1:])
    WA_CHANNEL_ID = "e180aa1d-dc48-4d0a-bec3-fc0afc53cf03"
    db._execute(
        """INSERT INTO wazzup_contact_map (chat_id, company_name, chat_type, channel_id, wazzup_name)
           VALUES (%s, %s, 'whatsapp', %s, %s)
           ON CONFLICT (chat_id) DO UPDATE SET company_name=%s, channel_id=%s, chat_type='whatsapp'""",
        (chat_id, company_name, WA_CHANNEL_ID, company_name, company_name, WA_CHANNEL_ID)
    )
    await update.message.reply_text(
        f"✅ WhatsApp контакт привязан:\n"
        f"Телефон: {chat_id}\n"
        f"Компания: {company_name}"
    )


async def cmd_block_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/block [user_id] — заблокировать пользователя."""
    if not update.effective_user or update.effective_user.id != 360092495:
        return
    if not context.args:
        await update.message.reply_text("Использование: /block [user_id]")
        return
    try:
        uid = int(context.args[0])
        db.block_user(uid)
        await update.message.reply_text(f"🔒 Пользователь {uid} заблокирован.")
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")

async def cmd_unblock_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/unblock [user_id] — разблокировать пользователя."""
    if not update.effective_user or update.effective_user.id != 360092495:
        return
    if not context.args:
        await update.message.reply_text("Использование: /unblock [user_id]")
        return
    try:
        uid = int(context.args[0])
        db.unblock_user(uid)
        await update.message.reply_text(f"🔓 Пользователь {uid} разблокирован.")
    except Exception as e:
        await update.message.reply_text(f"❌ {e}")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/stats — статистика использования бота."""
    if not update.effective_user or update.effective_user.id != 360092495:
        return
    stats = db.get_usage_stats()
    if not stats:
        await update.message.reply_text("📭 Статистики пока нет.")
        return
    lines = ["📊 *Статистика использования бота*\n"]
    for s in stats:
        blocked = " 🔒" if s.get("is_blocked") else ""
        last = s.get("last_seen")
        last_str = last.strftime("%d.%m %H:%M") if last else "—"
        lines.append(
            f"👤 *{s.get('full_name','?')}*{blocked}\n"
            f"   ID: `{s.get('user_id')}` · Запросов: {s.get('request_count',0)} · "
            f"Был: {last_str}"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
    """/pdz_results — результаты ПДЗ в личку по каждому менеджеру."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return

    OWNER_ID = 360092495
    from moysklad import get_overdue_demands

    # Берём сегодняшние результаты, если нет — последние доступные
    today_results = db.get_pdz_results_today()
    if today_results:
        all_results = today_results
        date_label = "сегодня"
    else:
        last_date, all_results = db.get_pdz_results_last()
        if not all_results:
            await context.bot.send_message(chat_id=OWNER_ID, text="📭 Результатов по ПДЗ пока нет.")
            return
        date_label = last_date.strftime("%d.%m.%Y") if hasattr(last_date, "strftime") else str(last_date)

    sent_any = False
    for mgr in PDZ_MANAGERS:
        frag = mgr.get("name_fragment", mgr["name"]).lower()
        mgr_results = [
            r.get("result_text", "") for r in all_results
            if frag in r.get("manager_name", "").lower()
        ]

        # Текущая просрочка
        items = await get_overdue_demands(tag=mgr["tag"])
        overdue_clients = [i.get("name", "") for i in items] if items else []

        # Клиенты с ответами
        answered_clients = []
        for res_text in mgr_results:
            res_lower = res_text.lower()
            for client in overdue_clients:
                if any(w.lower() in res_lower for w in client.split() if len(w) >= 4):
                    if client not in answered_clients:
                        answered_clients.append(client)

        unanswered = [c for c in overdue_clients if c not in answered_clients]

        lines = [f"📊 *{mgr['name']} — ПДЗ* ({date_label})\n"]

        if not overdue_clients:
            lines.append("✅ Просроченных долгов нет")
        else:
            if mgr_results:
                lines.append("💬 *Ответы менеджера:*")
                for r in mgr_results:
                    lines.append(f"   — {r}")
                lines.append("")
            if unanswered:
                lines.append("❓ *Без ответа:*")
                for c in unanswered:
                    lines.append(f"   • {c}")
            else:
                lines.append("✅ По всем клиентам есть ответы")

        text = "\n".join(lines)

        keyboard = None
        if unanswered:
            mgr_chat_id = db.get_manager_chat_id(mgr.get("name_fragment", mgr["name"]))
            if mgr_chat_id:
                alert_id = db.save_price_alert(
                    order_id=f"pdz_{mgr['tag']}",
                    order_name=", ".join(unanswered[:3]),
                    client_name=", ".join(unanswered[:3]),
                    manager_name=mgr["name"],
                    manager_user_id=mgr_chat_id,
                    alert_text=text
                )
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("📨 Запросить комментарии",
                                         callback_data=f"pdz_request|{alert_id}")
                ]])

        await context.bot.send_message(
            chat_id=OWNER_ID, text=text,
            parse_mode="Markdown", reply_markup=keyboard
        )
        sent_any = True

    if not sent_any:
        await context.bot.send_message(chat_id=OWNER_ID, text="📭 Просроченных долгов нет.")

async def cmd_pdz_results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/pdz_results — результаты ПДЗ в личку по каждому менеджеру."""
    user = update.effective_user
    if not user or user.id != 360092495:
        return

    OWNER_ID = 360092495
    from moysklad import get_overdue_demands

    today_results = db.get_pdz_results_today()
    if today_results:
        all_results = today_results
        date_label = "сегодня"
    else:
        last_date, all_results = db.get_pdz_results_last()
        if not all_results:
            await context.bot.send_message(chat_id=OWNER_ID, text="📭 Результатов по ПДЗ пока нет.")
            return
        date_label = last_date.strftime("%d.%m.%Y") if hasattr(last_date, "strftime") else str(last_date)

    sent_any = False
    for mgr in PDZ_MANAGERS:
        frag = mgr.get("name_fragment", mgr["name"]).lower()
        mgr_results = [
            r.get("result_text", "") for r in all_results
            if frag in r.get("manager_name", "").lower()
        ]

        items = await get_overdue_demands(tag=mgr["tag"])
        overdue_clients = [i.get("name", "") for i in items] if items else []

        answered_clients = []
        for res_text in mgr_results:
            res_lower = res_text.lower()
            for client in overdue_clients:
                if any(w.lower() in res_lower for w in client.split() if len(w) >= 4):
                    if client not in answered_clients:
                        answered_clients.append(client)

        unanswered = [c for c in overdue_clients if c not in answered_clients]

        lines = [f"📊 *{mgr['name']} — ПДЗ* ({date_label})\n"]
        if not overdue_clients:
            lines.append("✅ Просроченных долгов нет")
        else:
            if mgr_results:
                lines.append("💬 *Ответы менеджера:*")
                for r in mgr_results:
                    lines.append(f"   — {r}")
                lines.append("")
            if unanswered:
                lines.append("❓ *Без ответа:*")
                for c in unanswered:
                    lines.append(f"   • {c}")
            else:
                lines.append("✅ По всем клиентам есть ответы")

        text = "\n".join(lines)
        keyboard = None
        if unanswered:
            mgr_chat_id = db.get_manager_chat_id(mgr.get("name_fragment", mgr["name"]))
            if mgr_chat_id:
                alert_id = db.save_price_alert(
                    order_id=f"pdz_{mgr['tag']}",
                    order_name=", ".join(unanswered[:3]),
                    client_name=", ".join(unanswered[:3]),
                    manager_name=mgr["name"],
                    manager_user_id=mgr_chat_id,
                    alert_text=text
                )
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("📨 Запросить комментарии",
                                         callback_data=f"pdz_request|{alert_id}")
                ]])

        await context.bot.send_message(
            chat_id=OWNER_ID, text=text,
            parse_mode="Markdown", reply_markup=keyboard
        )
        sent_any = True

    if not sent_any:
        await context.bot.send_message(chat_id=OWNER_ID, text="📭 Просроченных долгов нет.")

async def cmd_pdz_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестовый запуск утренних задач ПДЗ. /pdz_test [имя|all]"""
    user = update.message.from_user
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]
    if user.id not in manager_ids:
        await update.message.reply_text("⛔ Только для руководителей.")
        return

    arg = (context.args[0].lower() if context.args else "all")

    from scheduler import pdz_morning_task
    app = update.get_bot()

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

async def handle_price_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатия кнопок на алерте о цене."""
    query = update.callback_query
    await query.answer()

    user = query.from_user

    # Только руководители могут нажимать
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]
    if user.id not in manager_ids:
        await query.answer("⛔ Только для руководителей.", show_alert=True)
        return

    parts = query.data.split("|")
    action = parts[0]
    order_href = parts[1] if len(parts) > 1 else ""

    # Имя менеджера берём из текста сообщения (строка "Менеджер: ...")
    manager_name = ""
    for line in query.message.text.split("\n"):
        if line.startswith("Менеджер:"):
            manager_name = line.replace("Менеджер:", "").strip()
            break

    group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))

    MS_STATE_AGREED = "005f3651-9a9a-11f0-0a80-03a900027474"

    if action == "price_ok":
        from moysklad import set_order_state
        order_id = parts[1] if len(parts) > 1 else ""
        if order_id:
            await set_order_state(order_id, MS_STATE_AGREED)
        await query.message.delete()

    elif action == "price_comment":
        order_id_val = parts[1] if len(parts) > 1 else ""
        alert_id = int(parts[2]) if len(parts) > 2 else 0
        alert_data = db.get_price_alert(alert_id) if alert_id else {}
        mgr_name = alert_data.get("manager_name", "") if alert_data else manager_name

        # Ищем chat_id менеджера
        mgr_chat_id = None
        if mgr_name:
            # Пробуем найти по фамилии
            for part in mgr_name.split():
                cid = db.get_manager_chat_id(part)
                if cid:
                    mgr_chat_id = cid
                    break

        alert_text = query.message.text

        if mgr_chat_id:
            await context.bot.send_message(
                chat_id=mgr_chat_id,
                text=(
                    f"⚠️ *Виктор просит пояснить занижение цены:*\n\n"
                    f"{alert_text}\n\n"
                    f"Напиши пояснение — оно уйдёт Виктору."
                ),
                parse_mode="Markdown"
            )
            # Сохраняем ожидание ответа
            _pending_price_comments[mgr_chat_id] = {
                "alert_id": alert_id,
                "order_id": order_id_val,
                "mgr_name": mgr_name,
            }
            await query.edit_message_text(
                query.message.text + f"\n\n💬 *Запрошен комментарий у {mgr_name}*",
                parse_mode="Markdown"
            )
        else:
            # Нет chat_id — пишем в группу
            group_chat_id_val = int(os.getenv("GROUP_CHAT_ID", "0"))
            if group_chat_id_val:
                contact = MANAGERS_CONTACTS.get(mgr_name, f"*{mgr_name}*")
                await context.bot.send_message(
                    chat_id=group_chat_id_val,
                    text=f"{contact}, Виктор просит пояснить занижение цены по заказу.",
                    parse_mode="Markdown"
                )
            await query.edit_message_text(
                query.message.text + f"\n\n💬 *Запрошен комментарий у {mgr_name}*",
                parse_mode="Markdown"
            )

    elif action == "pdz_ok":
        from moysklad import set_order_state
        order_id = parts[1] if len(parts) > 1 else ""
        logger.info(f"pdz_ok: order_id={order_id}")
        if order_id:
            success = await set_order_state(order_id, MS_STATE_AGREED)
            logger.info(f"pdz_ok: set_order_state result={success}")
        await query.answer("✅ Принято")
        await query.message.delete()

    elif action == "pdz_request":
        # Запрашиваем комментарии у менеджера по клиентам без ответа
        alert_id = int(parts[1]) if len(parts) > 1 else 0
        alert_data = db.get_price_alert(alert_id) if alert_id else {}
        if not alert_data:
            await query.answer("Данные не найдены.", show_alert=True)
            return

        mgr_chat_id = alert_data.get("manager_user_id")
        mgr_name = alert_data.get("manager_name", "")
        clients_str = alert_data.get("client_name", "")

        if not mgr_chat_id:
            await query.answer("Нет chat_id менеджера — пусть напишет /mychatid боту.", show_alert=True)
            return

        clients_list = [c.strip() for c in clients_str.split(",") if c.strip()]
        clients_text = "\n".join(f"• {c}" for c in clients_list)

        try:
            await context.bot.send_message(
                chat_id=mgr_chat_id,
                text=(
                    f"📋 *{mgr_name}, нужны комментарии по дебиторке*\n\n"
                    f"По следующим клиентам пока нет информации:\n"
                    f"{clients_text}\n\n"
                    f"Напиши боту в личку — кто и когда оплатит."
                ),
                parse_mode="Markdown"
            )
            await query.edit_message_reply_markup(reply_markup=None)
            await query.answer("✅ Запрос отправлен менеджеру.")
            await context.bot.send_message(
                chat_id=360092495,
                text=f"✅ Запрос комментариев отправлен *{mgr_name}*.",
                parse_mode="Markdown"
            )
        except Exception as e:
            await query.answer(f"Ошибка: {e}", show_alert=True)

    elif action == "pdz_comment":
        order_id = parts[1] if len(parts) > 1 else ""
        pdz_data = _pdz_alert_data.get(order_id, {})
        client = pdz_data.get("client", "")
        manager_name_pdz = pdz_data.get("manager", manager_name)
        order_name_pdz = pdz_data.get("order_name", "")
        debt_amount = pdz_data.get("debt_amount", 0)
        debt_days = pdz_data.get("debt_days", 0)

        # Сохраняем в БД
        db.save_pdz_comment(
            client=client,
            manager=manager_name_pdz,
            order_name=order_name_pdz,
            debt_amount=debt_amount,
            debt_days=debt_days,
            comment="Запрошен комментарий руководителем",
            commented_by=user.first_name,
        )

        new_text = query.message.text + f"\n\n💬 *{user.first_name} ждёт комментарий менеджера*"
        await query.edit_message_text(new_text, parse_mode="Markdown")

        if group_chat_id:
            contact = MANAGERS_CONTACTS.get(manager_name_pdz)
            mgr_mention = contact if contact else f"*{manager_name_pdz}*" if manager_name_pdz else "Менеджер"
            await context.bot.send_message(
                chat_id=group_chat_id,
                text=f"{mgr_mention}, дай комментарий по заказу *{order_name_pdz}* — у клиента просрочка {debt_days} дней.",
                parse_mode="Markdown"
            )

async def _refresh_report_cache():
    """Фоновое обновление кэша отчёта."""
    try:
        logger.info("_refresh_report_cache: начинаю сбор данных...")
        data = await _build_report_data()
        db.set_report_cache(data)
        logger.info("_refresh_report_cache: кэш обновлён")
    except Exception as e:
        logger.error(f"_refresh_report_cache: {e}", exc_info=True)


async def _build_report_data() -> dict:
    """Собирает все данные для отчёта ОП из МойСклад."""
    from datetime import date
    from moysklad import get_manager_shipments, get_attracted_goods_by_manager, get_lost_clients_by_manager, get_headers, MS_BASE
    import aiohttp

    today = date.today()
    month_start = today.replace(day=1).isoformat()
    month_end = today.isoformat()

    facts = await get_manager_shipments(month_start, month_end)
    attracted = await get_attracted_goods_by_manager(month_start, month_end)
    lost = await get_lost_clients_by_manager(month_start, month_end)

    for mgr_name in facts:
        facts[mgr_name]["attracted"] = attracted.get(mgr_name, 0.0)
        facts[mgr_name]["lost_clients"] = lost.get(mgr_name, 0)

    TAGS = {"скляр":"Инесса Скляр","мерзлякова":"Елена Мерзлякова","баласанян":"Карина Баласанян","леонтьев":"Алексей Леонтьев","коликов":"Денис Коликов"}

    # История по месяцам (кэшируется раз в месяц)
    from moysklad import get_manager_monthly_history
    mgr_history = {}
    for tag, mgr_name in TAGS.items():
        cached_hist = db.get_mgr_history_cache(tag)
        if cached_hist is not None:
            mgr_history[mgr_name] = cached_hist
            logger.info(f"_build_report_data: история {mgr_name} из кэша")
        else:
            logger.info(f"_build_report_data: считаю историю {mgr_name}...")
            hist = await get_manager_monthly_history(tag, mgr_name)
            db.set_mgr_history_cache(tag, hist)
            mgr_history[mgr_name] = hist

    tag_to_ids = {}
    async with aiohttp.ClientSession() as session:
        for tag, mgr in TAGS.items():
            ids = set()
            off = 0
            while True:
                async with session.get(f"{MS_BASE}/entity/counterparty", headers=get_headers(), params={"filter":f"tags={tag}","limit":100,"offset":off}) as r:
                    d = await r.json()
                rows = d.get("rows",[])
                for cp in rows: ids.add(cp.get("id",""))
                if len(rows)<100: break
                off+=100
            tag_to_ids[mgr] = ids

        curr_ids = {}
        off = 0
        while True:
            async with session.get(f"{MS_BASE}/entity/demand", headers=get_headers(), params={"filter":f"moment>={month_start} 00:00:00;moment<={month_end} 23:59:59","expand":"agent","limit":200,"offset":off}) as r:
                d = await r.json()
            rows = d.get("rows",[])
            for row in rows:
                href = row.get("agent",{}).get("meta",{}).get("href","")
                aid = href.split("/")[-1] if href else ""
                if aid: curr_ids[aid] = href
            if len(rows)<200: break
            off+=200

        new_client_names = {}
        for mgr, ids in tag_to_ids.items():
            for aid in ids:
                if aid not in curr_ids: continue
                async with session.get(f"{MS_BASE}/entity/demand", headers=get_headers(), params={"filter":f"agent={MS_BASE}/entity/counterparty/{aid};moment<{month_start} 00:00:00","limit":1}) as r:
                    prev = await r.json()
                if prev.get("meta",{}).get("size",0)==0:
                    async with session.get(f"{MS_BASE}/entity/counterparty/{aid}", headers=get_headers()) as r2:
                        cp = await r2.json()
                    new_client_names.setdefault(mgr,[]).append(cp.get("name",aid))

        if today.month==1:
            prev_start = f"{today.year-1}-12-01"
        else:
            prev_start = f"{today.year}-{today.month-1:02d}-01"
        prev_ids = set()
        all_mgr_ids = set().union(*tag_to_ids.values())
        off = 0
        while True:
            async with session.get(f"{MS_BASE}/entity/demand", headers=get_headers(), params={"filter":f"moment>={prev_start} 00:00:00;moment<{month_start} 00:00:00","expand":"agent","limit":200,"offset":off}) as r:
                d = await r.json()
            rows = d.get("rows",[])
            for row in rows:
                href = row.get("agent",{}).get("meta",{}).get("href","")
                aid = href.split("/")[-1] if href else ""
                if aid and aid in all_mgr_ids: prev_ids.add(aid)
            if len(rows)<200: break
            off+=200

        EXCLUDED_STATUSES = {"закрылся", "переименован"}
        lost_client_names = {}
        for mgr, ids in tag_to_ids.items():
            for aid in (ids & prev_ids - set(curr_ids.keys())):
                async with session.get(f"{MS_BASE}/entity/counterparty/{aid}", headers=get_headers()) as r:
                    cp = await r.json()
                # Исключаем закрытых и переименованных
                cp_status = (cp.get("state") or {}).get("name", "").lower().strip()
                if cp_status in EXCLUDED_STATUSES:
                    logger.info(f"lost_clients: исключён {cp.get('name')} — статус '{cp_status}'")
                    continue
                # Проверяем что клиент всё ещё принадлежит этому менеджеру по тегу
                # Если тег сменился — клиент не выбывший, просто перешёл к другому менеджеру
                cp_tags = [t.lower() for t in cp.get("tags", [])]
                mgr_tag = next((t for t, m in TAGS.items() if m == mgr), None)
                if mgr_tag and mgr_tag not in cp_tags:
                    logger.info(f"Skipping lost client {cp.get('name')} for {mgr} — tag changed to {cp_tags}")
                    continue
                lost_client_names.setdefault(mgr, []).append(cp.get("name", aid))

    PLANS = {
        "Инесса Скляр":     {"shipments": 250, "revenue": 26_500_000, "clients": 30, "new_clients": 5, "attracted": 1_000_000},
        "Карина Баласанян": {"shipments": 170, "revenue": 6_000_000,  "clients": 44, "new_clients": 5, "attracted": 1_100_000},
        "Елена Мерзлякова": {"shipments": 80,  "revenue": 6_700_000,  "clients": 30, "new_clients": 5, "attracted": 300_000},
        "Алексей Леонтьев": {"shipments": 17,  "revenue": 500_000,    "clients": 8,  "new_clients": 7, "attracted": 300_000},
        "Денис Коликов":    {"shipments": 3,   "revenue": 900_000,    "clients": 3,  "new_clients": 3, "attracted": 100_000},
    }
    WEEKLY_PLANS = {
        "Инесса Скляр":     {"shipments": 25,  "revenue": 2_000_000,  "clients": 10, "new_clients": 1, "attracted": 250_000},
        "Карина Баласанян": {"shipments": 40,  "revenue": 1_200_000,  "clients": 16, "new_clients": 1, "attracted": 275_000},
        "Елена Мерзлякова": {"shipments": 10,  "revenue": 1_000_000,  "clients": 5,  "new_clients": 1, "attracted": 75_000},
        "Алексей Леонтьев": {"shipments": 3,   "revenue": 100_000,    "clients": 3,  "new_clients": 1, "attracted": 75_000},
        "Денис Коликов":    {"shipments": 1,   "revenue": 225_000,    "clients": 1,  "new_clients": 1, "attracted": 25_000},
    }
    SHORT_NAMES = {"Инесса Скляр":"Инесса","Карина Баласанян":"Карина","Елена Мерзлякова":"Елена","Алексей Леонтьев":"Алексей","Денис Коликов":"Денис"}

    # Синхронизируем счётчики с реальными списками имён
    for mgr_name in facts:
        facts[mgr_name]["new_clients"] = len(new_client_names.get(mgr_name, []))
        facts[mgr_name]["lost_clients"] = len(lost_client_names.get(mgr_name, []))

    # Загружаем аттестацию из БД
    attestation = {}
    for mgr_name in PLANS:
        att = {}
        for kind, suffix in [("general", "general"), ("akb", "akb")]:
            try:
                row = db._fetchone("SELECT value FROM bot_settings WHERE key=%s", (f"attestation_{suffix}_{mgr_name}",))
                if row:
                    att[kind] = int(row["value"])
            except Exception:
                pass
        if att:
            attestation[mgr_name] = att
            logger.info(f"_build_report_data: аттестация {mgr_name} = {att}")

    # Загружаем накопительные недельные цели из БД
    weekly_targets = {}
    for mgr_name in PLANS:
        wt = get_weekly_targets(mgr_name)
        wp = WEEKLY_PLANS.get(mgr_name, {})
        for metric in ["revenue", "shipments", "clients", "new_clients", "attracted"]:
            if metric not in wt and metric in wp:
                wt[metric] = wp[metric]
        weekly_targets[mgr_name] = wt
        logger.info(f"_build_report_data: weekly_targets {mgr_name} = {wt}")

    return {
        "date": today.strftime("%d.%m.%Y"),
        "facts": facts,
        "plans": PLANS,
        "weekly_plans": WEEKLY_PLANS,
        "weekly_targets": weekly_targets,
        "short_names": SHORT_NAMES,
        "new_client_names": new_client_names,
        "lost_client_names": lost_client_names,
        "mgr_history": mgr_history,
        "attestation": attestation,
    }


def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("Не задан TELEGRAM_BOT_TOKEN в переменных окружения!")

    app = Application.builder().token(token).build()

    # Восстанавливаем ожидающие идентификации из БД после рестарта
    try:
        pending_rows = db.get_pending_idents()
        for row in pending_rows:
            lk = row["link_key"]
            _pending_links[lk] = {
                "chat_id": row["chat_id"],
                "channel_id": row["channel_id"],
                "wazzup_name": row["wazzup_name"],
                "chat_type": row["chat_type"],
                "link_key": lk,
            }
        if pending_rows:
            logger.info(f"Восстановлено {len(pending_rows)} ожидающих идентификаций из БД")
    except Exception as e:
        logger.warning(f"Не удалось восстановить pending_idents: {e}")

    # Команды
    app.add_handler(CallbackQueryHandler(handle_task_done_callback, pattern="^task_done\\|"))
    app.add_handler(CommandHandler("del_tasks", cmd_delete_executor_tasks))
    app.add_handler(CommandHandler("op_report", cmd_op_report))
    app.add_handler(CommandHandler("test_group", cmd_test_group))
    app.add_handler(CommandHandler("test_fact", cmd_test_fact))
    app.add_handler(CommandHandler("set_promo", cmd_set_promo))
    app.add_handler(CommandHandler("refresh_history", cmd_refresh_history))
    app.add_handler(CommandHandler("reissue_contract", cmd_reissue_contract))
    app.add_handler(CommandHandler("refresh_contract", cmd_refresh_contract))
    app.add_handler(CommandHandler("refresh_cache", cmd_refresh_cache))
    app.add_handler(CommandHandler("web_report", cmd_web_report))
    app.add_handler(CommandHandler("lost_clients", cmd_lost_clients))
    app.add_handler(CommandHandler("new_clients", cmd_new_clients))
    app.add_handler(CommandHandler("test_publink", cmd_test_publink))
    app.add_handler(CommandHandler("unlink", cmd_unlink))
    app.add_handler(CommandHandler("relink", cmd_relink))
    app.add_handler(CommandHandler("sync_managers", cmd_sync_managers))
    app.add_handler(CommandHandler("search_msg", cmd_search_msg))
    app.add_handler(CommandHandler("aging", cmd_aging))
    app.add_handler(CommandHandler("check_webhooks", cmd_check_webhooks))
    app.add_handler(CommandHandler("managers", cmd_managers))
    app.add_handler(CommandHandler("reset_agreed", cmd_reset_agreed))
    app.add_handler(CommandHandler("fix_channels", cmd_fix_channels))
    app.add_handler(CommandHandler("relink_max", cmd_relink_max))
    app.add_handler(CommandHandler("relink_wa", cmd_relink_wa))
    app.add_handler(CommandHandler("set_attestation", cmd_set_attestation))
    app.add_handler(CommandHandler("set_weekly", cmd_set_weekly))
    app.add_handler(CommandHandler("block", cmd_block_user))
    app.add_handler(CommandHandler("unblock", cmd_unblock_user))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("usermenu", cmd_user_menu))
    app.add_handler(CallbackQueryHandler(handle_user_menu_callback, pattern="^user_"))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CallbackQueryHandler(handle_menu_callback, pattern="^menu_"))
    app.add_handler(CommandHandler("mychatid", cmd_mychatid))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("clearwazzup", cmd_clear_wazzup))
    app.add_handler(CommandHandler("wazzup_enrich", cmd_wazzup_enrich))
    app.add_handler(CommandHandler("wazzup_export", cmd_wazzup_export))
    app.add_handler(CommandHandler("wazzup_reset", cmd_wazzup_reset))
    app.add_handler(CommandHandler("wazzup_channels", cmd_wazzup_channels))
    app.add_handler(CommandHandler("wazzup_setup", cmd_wazzup_setup))
    app.add_handler(CommandHandler("clearall", cmd_clear_all))
    app.add_handler(CommandHandler("cleartasksall", cmd_clear_tasks_all))
    app.add_handler(CommandHandler("test", cmd_test))
    app.add_handler(CommandHandler("deltask", cmd_del_task))
    app.add_handler(MessageHandler(
        filters.REPLY & filters.Regex(r"(?i)^(удали|удалить|отмени|отменить|убери)"),
        cmd_deltask_by_reply
    ))
    app.add_handler(MessageHandler(filters.StatusUpdate.MESSAGE_AUTO_DELETE_TIMER_CHANGED, handle_message))
    app.add_handler(CommandHandler("cleartasks", cmd_clear_tasks))
    app.add_handler(CommandHandler("all_tasks", cmd_all_tasks))
    app.add_handler(CommandHandler("overdue", cmd_overdue))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("debts", cmd_debtors))
    app.add_handler(CommandHandler("photo", cmd_photo))
    app.add_handler(CommandHandler("price", cmd_price))
    app.add_handler(CommandHandler("contact", cmd_contact))
    app.add_handler(CommandHandler("memory", cmd_memory))
    app.add_handler(CommandHandler("remember", cmd_remember))
    app.add_handler(CommandHandler("add_webhook", cmd_add_webhook))
    app.add_handler(CommandHandler("pdz_results", cmd_pdz_results))
    app.add_handler(CommandHandler("pdz", cmd_pdz))
    app.add_handler(CommandHandler("pdz_test", cmd_pdz_test))
    app.add_handler(CommandHandler("pdz_evening", cmd_pdz_evening_test))
    app.add_handler(CallbackQueryHandler(handle_contract_callback, pattern="^contract_"))
    app.add_handler(CallbackQueryHandler(handle_price_callback, pattern="^(price_|pdz_)"))
    app.add_handler(CallbackQueryHandler(handle_send_callback, pattern="^send_"))
    app.add_handler(CallbackQueryHandler(handle_wazzup_link_callback, pattern="^(wazzup_link|wazzup_role|wazzup_pick|wazzup_seg|wazzup_mgr)"))
    app.add_handler(CallbackQueryHandler(handle_wazzup_ignore_callback, pattern="^wazzup_ignore"))
    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POSTS, handle_channel_post))
    app.add_handler(MessageHandler(filters.ALL & ~filters.UpdateType.CHANNEL_POSTS, handle_message))

    # Планировщик
    setup_scheduler(app, db)

    # Запускаем webhook-сервер и polling параллельно
    import aiohttp.web as web

    async def handle_sipuni_webhook(request):
        """Принимает события от Sipuni АТС."""
        try:
            params = dict(request.rel_url.query)
            event = params.get("event", "")
            call_id = params.get("call_id", "")
            src_num = params.get("src_num", "")
            dst_num = params.get("dst_num", "")
            short_dst = params.get("short_dst_num", "")
            tree_name = params.get("treeName", "")
            status = params.get("status", "")
            record_link = params.get("call_record_link", "")
            call_start = params.get("call_start_timestamp", "")
            call_answer = params.get("call_answer_timestamp", "0")

            logger.info(f"Sipuni event={event} call_id={call_id} src={src_num} dst={dst_num} tree={tree_name} status={status}")

            # event=2 — звонок завершён
            if event == "2" and record_link and status == "ANSWER":
                asyncio.create_task(process_sipuni_call(
                    call_id=call_id,
                    src_num=src_num,
                    dst_num=dst_num,
                    short_dst=short_dst,
                    tree_name=tree_name,
                    record_link=record_link,
                    call_start=call_start,
                    call_answer=call_answer,
                    bot=app.bot,
                ))

            return web.Response(text="ok")
        except Exception as e:
            logger.error(f"Sipuni webhook error: {e}")
            return web.Response(text="error", status=500)

    async def handle_wazzup_webhook(request):
        """Принимает webhook от Wazzup — сохраняет сообщения и chatId клиентов."""
        try:
            data = await request.json()
            messages = data.get("messages", [])
            saved = 0
            for msg in messages:
                text = msg.get("text", "")
                chat_type = msg.get("chatType", "")
                chat_id_val = msg.get("chatId", "")
                channel_id_val = msg.get("channelId", "")
                contact = msg.get("contact", {})
                contact_name = contact.get("name", chat_id_val)
                is_outbound = msg.get("isEcho", False)
                manager_id = msg.get("crmUserId", "")
                manager_name = WAZZUP_MANAGERS.get(manager_id, manager_id)
                raw_dt = msg.get("dateTime", "")
                try:
                    from datetime import datetime, timezone
                    if raw_dt and str(raw_dt).isdigit():
                        sent_at = datetime.fromtimestamp(int(raw_dt), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        sent_at = str(raw_dt)
                except Exception:
                    sent_at = str(raw_dt)

                logger.info(f"Wazzup msg: isEcho={is_outbound} channel={channel_id_val} chatType={chat_type} chatId={chat_id_val} contact='{contact_name}' text='{text[:60]}'")

                # Сохраняем маппинг контакта → chatId/channel для последующей отправки
                if chat_id_val and contact_name and not is_outbound:
                    db.save_wazzup_contact(
                        contact_name=contact_name,
                        chat_id=chat_id_val,
                        chat_type=chat_type,
                        channel_id=channel_id_val,
                    )

                # Для исходящих — тоже проверяем идентификацию
                if chat_id_val and contact_name and is_outbound:
                    db.save_wazzup_contact(
                        contact_name=contact_name,
                        chat_id=chat_id_val,
                        chat_type=chat_type,
                        channel_id=channel_id_val,
                    )

                # Уведомляем если контакт неизвестен (для любых сообщений)
                if chat_id_val and contact_name:
                    is_known = db.is_wazzup_contact_known(chat_id_val)
                    logger.info(f"Wazzup: chat_id={chat_id_val} is_known={is_known}")
                    if chat_type in ("telegram", "tgapi", "max") and not is_known and chat_id_val not in _wazzup_notified:
                        # Проверяем что контакт не помечен как игнорируемый
                        ignored = db._fetchone(
                            "SELECT id FROM wazzup_contact_map WHERE chat_id=%s AND company_name='__ignore__'",
                            (chat_id_val,)
                        )
                        if ignored:
                            continue
                        group_chat_id = int(os.getenv("WAZZUP_ID_CHAT_ID", "0"))
                        logger.info(f"Wazzup: отправляю уведомление в группу {group_chat_id}")
                        if group_chat_id:
                            try:
                                import uuid as _uuid2
                                link_key = str(_uuid2.uuid4())[:8]
                                _pending_links[link_key] = {
                                    "chat_id": chat_id_val,
                                    "channel_id": channel_id_val,
                                    "wazzup_name": contact_name,
                                    "chat_type": chat_type,
                                }
                                # Сохраняем в БД чтобы пережить рестарт
                                db.save_pending_link(
                                    link_key=link_key,
                                    chat_id=chat_id_val,
                                    channel_id=channel_id_val,
                                    wazzup_name=contact_name,
                                    chat_type=chat_type,
                                )
                                keyboard = InlineKeyboardMarkup([[
                                    InlineKeyboardButton("🏢 Привязать компанию", callback_data=f"wazzup_link|{link_key}"),
                                    InlineKeyboardButton("🚫 Не привязывать", callback_data=f"wazzup_ignore|{chat_id_val}")
                                ]])
                                preview = (text or "").replace("\n", " ").strip()
                                if len(preview) > 120:
                                    preview = preview[:120] + "..."
                                CHANNEL_NAMES = {"telegram": "Telegram", "tgapi": "Telegram", "max": "Max", "whatsapp": "WhatsApp"}
                                channel_label = CHANNEL_NAMES.get(chat_type, chat_type)
                                await app.bot.send_message(
                                    chat_id=group_chat_id,
                                    text=(
                                        f"📩 *Новый неизвестный контакт — {channel_label}*\n\n"
                                        f"👤 Имя: *{contact_name}*\n"
                                        f"💬 _{preview}_\n\n"
                                        f"Чей клиент? Нажми и напиши как он называется в МойСклад"
                                    ),
                                    parse_mode="Markdown",
                                    reply_markup=keyboard
                                )
                                _wazzup_notified.add(chat_id_val)
                            except Exception as e:
                                logger.error(f"Не удалось отправить уведомление в группу: {e}", exc_info=True)

                if not text:
                    continue
                ok = db.save_wazzup_message(
                    message_id=msg.get("messageId", ""),
                    channel_id=channel_id_val,
                    chat_type=chat_type,
                    chat_id=chat_id_val,
                    contact_name=contact_name,
                    manager_id=manager_id,
                    manager_name=manager_name,
                    text=text,
                    is_outbound=is_outbound,
                    sent_at=sent_at,
                )
                if ok:
                    saved += 1
            logger.info(f"Wazzup webhook: получено {len(messages)} сообщений, сохранено {saved}")
            return web.Response(text="ok")
        except Exception as e:
            logger.error(f"Wazzup webhook error: {e}")
            return web.Response(text="error", status=500)

    async def handle_ms_webhook(request):
        """Принимает webhook от МойСклад — новые/обновлённые заказы."""
        try:
            data = await request.json()
            asyncio.create_task(process_ms_webhook(data, app.bot))
            return web.Response(text="ok")
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            return web.Response(text="error", status=500)

    async def handle_health(request):
        return web.Response(text="ok")

    async def handle_web_report(request):
        """Интерактивный веб-отчёт ОП."""
        token = request.query.get("token", "")
        link = db.get_report_link(token)
        if not link:
            return web.Response(
                text="<html><body style='font-family:sans-serif;padding:40px;color:#6b7280'><h2>Ссылка истекла или недействительна</h2><p>Запроси новую через /web_report в боте.</p></body></html>",
                content_type="text/html", status=403
            )

        try:
            import json, pathlib
            from datetime import date

            # Проверяем кэш
            cached = db.get_report_cache()
            if cached:
                report_data = cached
                logger.info("handle_web_report: отдаём из кэша")
            else:
                report_data = await _build_report_data()
                db.set_report_cache(report_data)

            tpl_path = pathlib.Path(__file__).parent / "report_template.html"
            if tpl_path.exists():
                html = tpl_path.read_text(encoding="utf-8")
            else:
                from report_html import REPORT_HTML_TEMPLATE
                html = REPORT_HTML_TEMPLATE
            html = html.replace("__REPORT_DATA__", json.dumps(report_data, ensure_ascii=False))
            return web.Response(text=html, content_type="text/html", charset="utf-8")

        except Exception as e:
            logger.error(f"handle_web_report: {e}", exc_info=True)
            return web.Response(text=f"Ошибка: {e}", status=500)
    async def run_web():
        web_app = web.Application()
        web_app.router.add_post("/webhook/moysklad", handle_ms_webhook)
        web_app.router.add_post("/webhook/wazzup", handle_wazzup_webhook)
        web_app.router.add_get("/webhook/sipuni", handle_sipuni_webhook)
        web_app.router.add_post("/webhook/sipuni", handle_sipuni_webhook)
        web_app.router.add_get("/health", handle_health)
        web_app.router.add_get("/report", handle_web_report)
        port = int(os.getenv("PORT", "8080"))
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logger.info(f"🌐 Webhook сервер запущен на порту {port}")

    async def run_all():
        await run_web()
        await app.initialize()
        await app.start()

        # Ждём завершения старого инстанса и принудительно сбрасываем webhook
        import asyncio as _asyncio
        for attempt in range(5):
            try:
                await app.bot.delete_webhook(drop_pending_updates=True)
                break
            except Exception as e:
                logger.warning(f"delete_webhook attempt {attempt+1}: {e}")
                await _asyncio.sleep(2)

        await app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=["message", "channel_post", "edited_message", "edited_channel_post", "callback_query"]
        )
        logger.info("🤖 Бот запущен!")
        # Загружаем менеджеров Wazzup
        await load_wazzup_managers()
        # Восстанавливаем pending_contracts из БД
        try:
            import json as _json
            rows = db._fetchall("SELECT user_id, data FROM pending_contracts WHERE created_at > NOW() - INTERVAL '24 hours'")
            for row in rows:
                _pending_contracts[row["user_id"]] = _json.loads(row["data"])
            if rows:
                logger.info(f"Восстановлено {len(rows)} pending_contracts из БД")
        except Exception as e:
            logger.warning(f"Не удалось восстановить pending_contracts: {e}")
        # Восстанавливаем pending_links из БД после рестарта
        try:
            pending_rows = db.load_pending_links()
            for row in pending_rows:
                lk = row["link_key"]
                entry = {
                    "link_key": lk,
                    "chat_id": row["chat_id"],
                    "channel_id": row["channel_id"],
                    "wazzup_name": row["wazzup_name"],
                    "chat_type": row["chat_type"],
                }
                _pending_links[lk] = entry
            if pending_rows:
                logger.info(f"Восстановлено {len(pending_rows)} pending_links из БД")
        except Exception as e:
            logger.warning(f"load_pending_links: {e}")
        # Держим бота запущенным
        try:
            import signal
            loop = asyncio.get_event_loop()
            stop = loop.create_future()
            loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
            loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
            await stop
        finally:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()

    asyncio.run(run_all())

# Маппинг менеджеров МойСклад → Telegram (username или телефон)
MANAGERS_CONTACTS = {
    "Леонтьев Алексей Вадимович":      "@EL_Aliexbox",
    "Мерзлякова Елена Владимировна":   "+79920035102",
    "Баласанян Карина Владимировна":   "@fatbob183",
    "Скляр Инесса Ионасовна":          "+79622522903",
}

# Маппинг crmUserId Wazzup → имя менеджера (заполним после первых вебхуков)
WAZZUP_MANAGERS: dict = {}

async def load_wazzup_managers():
    """Загружает список менеджеров из Wazzup API."""
    import aiohttp
    api_key = os.getenv("WAZZUP_API_KEY", "")
    if not api_key:
        return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.wazzup24.com/v3/users",
                headers={"Authorization": f"Bearer {api_key}"}
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    for u in data.get("users", []):
                        uid = u.get("userId") or u.get("id", "")
                        name = u.get("name", "") or f"{u.get('firstName','')} {u.get('lastName','')}".strip()
                        if uid and name:
                            WAZZUP_MANAGERS[str(uid)] = name
                    logger.info(f"Wazzup managers loaded: {len(WAZZUP_MANAGERS)} — {list(WAZZUP_MANAGERS.values())}")
    except Exception as e:
        logger.warning(f"load_wazzup_managers: {e}")
# Кэш для дедупликации webhook — order_id → timestamp последней проверки
_price_check_cache: dict = {}
# Хранилище данных алертов ПДЗ — order_id → {client, manager, debt_amount, debt_days, order_name}
_pdz_alert_data: dict = {}

async def _is_company_excluded(company_name: str) -> bool:
    """Проверяет — находится ли компания в списке исключений квиза."""
    import aiohttp as _aio_excl
    if not QUIZ_BASE_URL:
        return False
    try:
        async with _aio_excl.ClientSession() as session:
            async with session.get(
                f"{QUIZ_BASE_URL}/api/check-exclusion",
                params={"company_name": company_name},
                timeout=_aio_excl.ClientTimeout(total=5)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    return data.get("excluded", False)
    except Exception as e:
        logger.warning(f"_is_company_excluded: не удалось проверить ({e}), считаем ИСКЛЮЧЁННОЙ (fail-safe)")
    return True


async def _is_company_whitelisted(company_name: str) -> bool:
    """Проверяет — в белом списке ли компания (опт, которому разрешён квиз)."""
    import aiohttp as _aio_wl
    if not QUIZ_BASE_URL:
        return False
    try:
        async with _aio_wl.ClientSession() as session:
            async with session.get(
                f"{QUIZ_BASE_URL}/api/check-whitelist",
                params={"company_name": company_name},
                timeout=_aio_wl.ClientTimeout(total=5)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    return data.get("whitelisted", False)
    except Exception as e:
        logger.warning(f"_is_company_whitelisted: ошибка ({e}), считаем не в белом списке")
    return False


async def check_order_agreed(order_href: str, bot):
    """При смене статуса заказа на Согласовано — отправляем клиенту в мессенджер."""
    MS_STATE_AGREED = "005f3651-9a9a-11f0-0a80-03a900027474"
    try:
        import aiohttp
        from moysklad import get_headers

        async with aiohttp.ClientSession() as session:
            async with session.get(
                order_href,
                headers=get_headers(),
                params={"expand": "agent,state,owner"}
            ) as r:
                if r.status != 200:
                    return
                order = await r.json()

        state = order.get("state", {})
        state_id = state.get("meta", {}).get("href", "").split("/")[-1]

        if state_id != MS_STATE_AGREED:
            return

        # Проверяем не отправляли ли уже
        order_id_check = order_href.split("/")[-1].split("?")[0]
        if db.is_agreed_notified(order_id_check):
            logger.info(f"check_order_agreed: заказ {order_id_check} — уведомление уже отправлялось, пропускаем")
            return

        order_name = order.get("name", "")
        order_sum = (order.get("sum", 0) or 0) / 100
        agent = order.get("agent", {})
        agent_name = agent.get("name", "")
        agent_id = agent.get("meta", {}).get("href", "").split("/")[-1]
        owner = order.get("owner", {})
        owner_name = owner.get("name", "")

        # Пропускаем розничного покупателя
        if agent_name.lower().strip() == "розничный покупатель":
            logger.info(f"check_order_agreed: пропускаем розничного покупателя")
            db.save_agreed_notification(order_id_check)
            return

        logger.info(f"check_order_agreed: заказ {order_name} клиент={agent_name} статус=Согласовано")

        # Загружаем позиции заказа
        order_id = order_href.split("/")[-1].split("?")[0]
        positions_text = ""
        try:
            async with aiohttp.ClientSession() as session2:
                async with session2.get(
                    f"https://api.moysklad.ru/api/remap/1.2/entity/customerorder/{order_id}/positions",
                    headers=get_headers(),
                    params={"expand": "assortment", "limit": 100}
                ) as r2:
                    if r2.status == 200:
                        pos_data = await r2.json()
                        lines = []
                        for pos in pos_data.get("rows", []):
                            name = pos.get("assortment", {}).get("name", "?")
                            qty = int(pos.get("quantity", 0))
                            price = (pos.get("price", 0) or 0) / 100
                            total = qty * price
                            lines.append(f"  • {name} × {qty} = {total:,.0f} руб.")
                        if lines:
                            positions_text = "\n".join(lines)
        except Exception as e:
            logger.warning(f"check_order_agreed: не удалось загрузить позиции: {e}")

        # Формируем сообщение
        delivery = order.get("deliveryPlannedMoment", "")[:10] if order.get("deliveryPlannedMoment") else ""

        # Определяем сегмент клиента по тегам (хорека / опт)
        segment = None
        tags = agent.get("tags", [])
        for tag in tags:
            if tag.lower() in ("хорека", "horeka"):
                segment = "хорека"
                break
            elif tag.lower() == "опт":
                segment = "опт"
                break
        # Если теги не пришли в expand — загружаем карточку
        if not tags and agent_id:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.get(
                        f"https://api.moysklad.ru/api/remap/1.2/entity/counterparty/{agent_id}",
                        headers=get_headers()
                    ) as r:
                        if r.status == 200:
                            cp = await r.json()
                            for tag in cp.get("tags", []):
                                if tag.lower() in ("хорека", "horeka"):
                                    segment = "хорека"
                                    break
                                elif tag.lower() == "опт":
                                    segment = "опт"
                                    break
            except Exception:
                pass

        msg = f"📋 Пожалуйста, проверьте заказ {order_name}\n\n"
        if positions_text:
            msg += f"📦 Состав:\n{positions_text}\n\n"
        msg += f"💰 Итого: {order_sum:,.0f} руб.\n"
        if delivery:
            msg += f"📅 Плановая дата отгрузки: {delivery}\n"

        # ── Определяем отправлять ли квиз ───────────────────────────────────
        # ── Квиз всем кроме исключений ───────────────────────────────────────
        if QUIZ_BASE_URL:
            excluded = await _is_company_excluded(agent_name)
            if not excluded:
                quiz_url = (
                    f"{QUIZ_BASE_URL}"
                    f"/?order={order_id}"
                    f"&client_id={agent_id}"
                    f"&amount={int(order_sum)}"
                )
                msg += (
                    f"\n\n🐟 Хотите бесплатный пласт форели? Сыграйте в нашу викторину FISHки! 🎣\n"
                    f"{quiz_url}"
                )
                logger.info(f"check_order_agreed: квиз добавлен для {agent_name}")
            else:
                logger.info(f"check_order_agreed: {agent_name} в исключениях, квиз не отправляем")
        # ─────────────────────────────────────────────────────────────────────

        # Ищем мессенджер клиента
        contact = db._fetchone(
            """SELECT chat_id, channel_id, chat_type, manager
               FROM wazzup_contact_map
               WHERE LOWER(company_name) LIKE LOWER(%s)
               LIMIT 1""",
            (f"%{agent_name}%",)
        )

        if not contact:
            logger.info(f"check_order_agreed: мессенджер {agent_name} не найден, пропускаем")
            db.save_agreed_notification(order_id_check)
            return
        # Отправляем через Wazzup
        import aiohttp as _aio
        api_key = os.getenv("WAZZUP_API_KEY", "")
        async with _aio.ClientSession() as session:
            payload = {
                "channelId": contact["channel_id"],
                "chatType": contact["chat_type"],
                "chatId": contact["chat_id"],
                "text": msg,
            }
            async with session.post(
                "https://api.wazzup24.com/v3/message",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload
            ) as r:
                if r.status in (200, 201):
                    logger.info(f"check_order_agreed: отправлено {agent_name} в {contact['chat_type']}")
                    db.save_agreed_notification(order_id_check)
                else:
                    body = await r.text()
                    logger.error(f"check_order_agreed: ошибка отправки {r.status} {body[:100]}")

    except Exception as e:
        logger.error(f"check_order_agreed: {e}", exc_info=True)

async def check_debtor_alert(order_href: str, bot, group_chat_id: int):
    """Проверяет есть ли у клиента просрочка > 5 дней при новом заказе."""
    try:
        import aiohttp
        from moysklad import get_headers, MS_BASE
        from datetime import date

        async with aiohttp.ClientSession() as session:
            async with session.get(
                order_href, headers=get_headers(),
                params={"expand": "agent,owner"}
            ) as resp:
                if resp.status != 200:
                    return
                order = await resp.json()

        agent = order.get("agent", {})
        agent_meta = agent.get("meta", {})
        agent_href = agent_meta.get("href", "")
        agent_id = agent.get("id") or (agent_href.split("/")[-1] if agent_href else "")
        agent_name = agent.get("name", "")
        order_name = order.get("name", "")
        owner = order.get("owner", {})
        manager_name = owner.get("name", "не указан")

        logger.info(f"check_debtor_alert: agent_id={agent_id} agent_name={agent_name} order={order_name}")

        if not agent_id:
            logger.warning("check_debtor_alert: agent_id пустой, пропускаем")
            return

        # Проверяем долг и просрочку через заказы контрагента
        from moysklad import get_counterparty_debt
        logger.info(f"check_debtor_alert: запрашиваю долг для {agent_id}")
        debt_info = await get_counterparty_debt(agent_id)
        logger.info(f"check_debtor_alert: debt_info={debt_info}")

        if not debt_info:
            logger.info("check_debtor_alert: debt_info пустой — нет долга или ошибка")
            return

        debt_amount = debt_info.get("debt", 0)
        debt_days = debt_info.get("overdue_days", 0)
        logger.info(f"check_debtor_alert: debt={debt_amount} days={debt_days}")

        if debt_days <= 5 or debt_amount <= 0:
            logger.info(f"check_debtor_alert: просрочка {debt_days} дней — ниже порога или долга нет")
            return

        order_id = order_href.split("/")[-1]
        _pdz_alert_data[order_id] = {
            "client": agent_name,
            "manager": manager_name,
            "order_name": order_name,
            "debt_amount": debt_amount,
            "debt_days": debt_days,
        }

        text = (
            f"🔴 *Новый заказ от клиента с просрочкой!*\n\n"
            f"*{agent_name}* | Заказ *{order_name}*\n"
            f"Менеджер: {manager_name}\n\n"
            f"Просрочка: *{debt_days} дней* | Сумма: *{debt_amount:,.0f} руб*"
        )
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Согласовано", callback_data=f"pdz_ok|{order_id}"),
                InlineKeyboardButton("💬 Требуется комментарий", callback_data=f"pdz_comment|{order_id}"),
            ]
        ])
        await bot.send_message(
            chat_id=group_chat_id,
            text=text,
            parse_mode="Markdown",
            reply_markup=keyboard
        )
        logger.info(f"ПДЗ алерт: {agent_name}, просрочка {debt_days} дней, заказ {order_name}")

    except Exception as e:
        logger.error(f"check_debtor_alert: {e}")
# Кэш позиций заказа — order_id → frozenset(позиций) для отслеживания изменений цен/номенклатуры
_order_positions_cache: dict = {}

async def process_ms_webhook(data: dict, bot):
    """Обрабатывает webhook от МойСклад — проверяет цены в заказе."""
    import time
    try:
        from moysklad import check_order_prices
        group_chat_id = int(os.getenv("GROUP_CHAT_ID", "0"))
        if not group_chat_id:
            return

        events = data.get("events", [])
        for event in events:
            meta = event.get("meta", {})
            entity_type = meta.get("type", "")
            if entity_type != "customerorder":
                continue

            order_href = meta.get("href", "")
            if not order_href:
                continue

            # Дедупликация — один заказ не чаще раза в 10 секунд
            order_id = order_href.split("/")[-1]
            now = time.time()
            last_check = _price_check_cache.get(order_id, 0)
            already_checked = now - last_check < 10
            _price_check_cache[order_id] = now

            action = event.get("action", "")
            logger.info(f"Webhook: заказ {order_id} action={action} already_checked={already_checked}")

            # Смена статуса на "Согласовано" — отправляем клиенту
            if action == "UPDATE" and not already_checked:
                await check_order_agreed(order_href, bot)

            # ПДЗ алерт — только для новых заказов, только один раз
            if action == "CREATE" and not already_checked:
                await check_debtor_alert(order_href, bot, group_chat_id)

            if already_checked:
                logger.info(f"Webhook: заказ {order_id} уже проверялся, пропускаем цены/логистику")
                continue

            # Получаем снапшот позиций (товар + цена) и сравниваем с предыдущим
            from moysklad import get_order_positions_snapshot
            snapshot = await get_order_positions_snapshot(order_href)
            prev_snapshot = _order_positions_cache.get(order_id)
            _order_positions_cache[order_id] = snapshot

            if prev_snapshot is not None and snapshot == prev_snapshot:
                logger.info(f"Webhook: заказ {order_id} — цены/номенклатура не изменились, пропускаем")
                continue

            logger.info(f"Webhook: проверяю цены заказа {order_id}")
            alerts = await check_order_prices(order_href)

            if alerts:
                owner_chat_id = 360092495  # Виктор Васильев
                text = "⚠️ *Цена ниже минимальной!*\n\n" + "\n\n".join(alerts)

                # Получаем имя менеджера из данных заказа
                mgr_name = ""
                for a in alerts:
                    for line in a.split("\n"):
                        if "Менеджер:" in line:
                            mgr_name = line.replace("Менеджер:", "").strip()
                            break

                # Сохраняем алерт в БД
                alert_id = db.save_price_alert(
                    order_id=order_id,
                    order_name="",
                    client_name="",
                    manager_name=mgr_name,
                    manager_user_id=0,
                    alert_text=text
                )

                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Согласовано", callback_data=f"price_ok|{order_id}"),
                    InlineKeyboardButton("💬 Комментарий менеджеру", callback_data=f"price_comment|{order_id}|{alert_id}"),
                ]])
                await bot.send_message(
                    chat_id=owner_chat_id,
                    text=text,
                    parse_mode="Markdown",
                    reply_markup=keyboard
                )

            # Проверяем логистику — только при создании заказа
            if action == "CREATE":
                await check_logistics_alert(order_href, bot, group_chat_id)

    except Exception as e:
        logger.error(f"process_ms_webhook: {e}")

async def check_logistics_alert(order_href: str, bot, group_chat_id: int):
    """Проверяет адрес доставки заказа на соответствие расписанию логистики."""
    try:
        from moysklad import check_delivery_schedule, get_headers, MS_BASE
        import aiohttp

        async with aiohttp.ClientSession() as session:
            url = order_href.split("?")[0]
            async with session.get(url, headers=get_headers()) as resp:
                if resp.status != 200:
                    return
                order = await resp.json()

        address = order.get("shipmentAddress", "")
        delivery_date = order.get("deliveryPlannedMoment", "")
        order_name = order.get("name", "")

        logger.info(f"check_logistics_alert: заказ={order_name} address='{address}' delivery_date='{delivery_date}'")

        if not address or not delivery_date:
            logger.info(f"check_logistics_alert: заказ {order_name} — нет адреса или даты, пропускаем (address={bool(address)}, date={bool(delivery_date)})")
            return

        # Не алертим старые заказы (старше 3 дней)
        from datetime import datetime, timezone, timedelta
        try:
            delivery_dt = datetime.fromisoformat(delivery_date.replace("Z", "+00:00"))
            if delivery_dt.tzinfo is None:
                delivery_dt = delivery_dt.replace(tzinfo=timezone.utc)
            if delivery_dt < datetime.now(timezone.utc) - timedelta(days=3):
                logger.info(f"check_logistics_alert: заказ {order_name} слишком старый ({delivery_date}), пропускаем")
                return
        except Exception:
            pass

        result = await check_delivery_schedule(address, delivery_date)
        if result.get("ok"):
            return

        # Получаем имя клиента и менеджера
        agent_href = order.get("agent", {}).get("meta", {}).get("href", "")
        owner_href = order.get("owner", {}).get("meta", {}).get("href", "")
        client_name = ""
        manager_name = ""

        async with aiohttp.ClientSession() as session:
            from moysklad import get_headers
            if agent_href:
                async with session.get(agent_href, headers=get_headers()) as r:
                    if r.status == 200:
                        d = await r.json()
                        client_name = d.get("name", "")
            if owner_href:
                async with session.get(owner_href, headers=get_headers()) as r:
                    if r.status == 200:
                        d = await r.json()
                        manager_name = d.get("name", "")

        city = result["city"].capitalize()
        weekday = result["weekday"]  # строка: "среда", "пятница" и т.д.
        allowed = ", ".join(result["allowed_days"]) or "не запланирован"

        # Винительный падеж для "не едем в ..."
        WEEKDAY_ACCUSATIVE = {
            "понедельник": "понедельник",
            "вторник": "вторник",
            "среда": "среду",
            "четверг": "четверг",
            "пятница": "пятницу",
            "суббота": "субботу",
            "воскресенье": "воскресенье",
        }
        weekday_acc = WEEKDAY_ACCUSATIVE.get(weekday.lower(), weekday)
        from datetime import date
        MONTHS = ["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"]
        try:
            d = date.fromisoformat(result["date"])
            date_str = f"{d.day} {MONTHS[d.month-1]}"
        except Exception:
            date_str = result["date"]

        text = (
            f"🚛 *Несоответствие логистики*\n\n"
            f"👤 {client_name} | Заказ №{order_name}\n"
            f"👔 Менеджер: {manager_name}\n"
            f"📍 Адрес: {address}\n\n"
            f"📅 Дата отгрузки: *{date_str} ({weekday})*\n"
            f"❌ В {city} мы не едем в {weekday_acc}\n"
            f"✅ {city} доступен: *{allowed}*"
        )

        await bot.send_message(
            chat_id=group_chat_id,
            text=text,
            parse_mode="Markdown"
        )
        logger.info(f"Логистика алерт: заказ {order_name}, {city}, {weekday}")

    except Exception as e:
        logger.error(f"check_logistics_alert: {e}", exc_info=True)

if __name__ == "__main__":
    main()
