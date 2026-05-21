"""
F2B PRO — Telegram Bot
Ассистент отдела продаж: фото, прайсы, задачи
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
    BusinessConnectionHandler,
    filters,
)

from database import Database
from notifier import check_order_agreed  # рассылка при согласовании — не трогать!
from scheduler import setup_scheduler, get_group_chat_id
from claude_ai import dispatch, smart_answer, parse_product_query
from amocrm import check_connection as amo_check  # оставляем для совместимости
from amo_alarms import (
    handle_amo_webhook,
    handle_take_callback,
    handle_amo_link_callback,
    cmd_myamoid,
    cmd_amo_setup,
)
from moysklad import (search_products, search_products_filtered, get_price_list, format_products,
    format_price_list, get_product_image, download_image, get_image_download_url,
    get_counterparty_balance, get_all_debtors, format_debtors_ms, format_counterparty_balance,
    find_counterparty_info, format_counterparty_info,
    get_debtors_by_tag, get_clients_by_tag, resolve_tag,
    format_debtors_by_tag, format_clients_by_tag,
    get_overdue_demands, format_overdue_demands, format_overdue_summary,
    format_reminders_for_manager, format_debt_reminder, fmt_money,
    list_employees, create_task, invalidate_employees_cache)
from claude_ai import parse_task_draft

# ─── Владелец бота — TG-id, гейтит админские команды и адрес утренних сводок ─
_owner_chat_id_raw = os.getenv("OWNER_CHAT_ID")
if not _owner_chat_id_raw:
    raise RuntimeError(
        "OWNER_CHAT_ID env not set. "
        "Задай числовой Telegram-id владельца в Railway → Variables."
    )
OWNER_CHAT_ID = int(_owner_chat_id_raw)

# ─── Партнёр — TG-id, whitelist для /задача (второй из двух постановщиков) ───
_partner_chat_id_raw = os.getenv("PARTNER_CHAT_ID")
if not _partner_chat_id_raw:
    raise RuntimeError(
        "PARTNER_CHAT_ID env not set. "
        "Задай TG-id партнёра (Александр) в Railway → Variables."
    )
PARTNER_CHAT_ID = int(_partner_chat_id_raw)

# ─── Словарь сотрудников — варианты имён и склонений ─────────────────────────
EMPLOYEES = {
    "Белякова Александра": [
        "александра", "александры", "александре", "александру",
        "белякова", "беляковой", "белякову",
        "саша", "саши", "саше", "сашу",
    ],
    "Ирина Дьяченко": [
        "ирина", "ирины", "ирине", "ирину", "ириной",
        "дьяченко",
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
    "Денис Коликов": [
        "денис", "дениса", "денису", "денисом",
        "коликов", "коликова", "коликову",
    ],
}

# Менеджеры отдела продаж — "всем менеджерам"
MOP_MANAGERS = [
    "Карина Баласанян",
    "Елена Мерзлякова",
    "Инесса Скляр",
    "Ирина Дьяченко",
    "Денис Коликов",
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
    is_author = user is not None and _is_task_author(user.id)
    await update.message.reply_text(
        f"👋 Привет, *{user.full_name if user else 'друг'}*! Я Эф — ассистент F2B PRO.\n\n"
        f"Используй меню ниже или обращайся: *Эф, [вопрос]*",
        parse_mode="Markdown",
        reply_markup=_user_menu_keyboard(include_task_button=is_author)
    )

def _user_menu_keyboard(include_task_button: bool = False) -> InlineKeyboardMarkup:
    """Общее меню для всех пользователей.

    Если include_task_button=True (только для whitelist Виктор/Александр) —
    добавляется кнопка «📋 Поставить задачу».
    """
    rows = [
        [
            InlineKeyboardButton("📸 Запросить фото товара", callback_data="user_photo"),
        ],
        [
            InlineKeyboardButton("📄 Сформировать договор", callback_data="user_contract"),
        ],
        [
            InlineKeyboardButton("📊 Отчёт ОП", callback_data="user_op_report"),
        ],
    ]
    if include_task_button:
        rows.append([InlineKeyboardButton("📋 Поставить задачу", callback_data="menu_task")])
    return InlineKeyboardMarkup(rows)

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

    elif action == "user_contract":
        await query.message.reply_text(
            "📄 Напиши название компании — сформирую договор поставки.\n"
            "Например: _Атмосфера_ или _ИТФИШ_",
            parse_mode="Markdown"
        )
        _user_awaiting[query.from_user.id] = "contract"

    elif action == "user_op_report":
        await cmd_op_report(update, context)

    elif action == "user_reconciliation":
        await query.message.reply_text(
            "📊 Напиши название компании — сформирую акт сверки.\n"
            "Например: _Атмосфера_ или _ИТФИШ_",
            parse_mode="Markdown"
        )
        _user_awaiting[query.from_user.id] = "reconciliation"


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
        "*Отчёты:*\n"
        "/report — недельный отчёт\n\n"
        "*Управление:*\n"
        "/menu — панель управления\n"
        "/mychatid — мой chat ID",
        parse_mode="Markdown"
    )

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Панель управления.

    - Виктор (OWNER): полное меню + секция «Постановка задач».
    - Александр (PARTNER): общее меню + кнопка «Поставить задачу».
    - Остальные: общее меню.
    """
    user = update.effective_user
    if not user:
        return

    # Не-руководитель — общее меню. Кнопка задач только для whitelist (Александр).
    if user.id != OWNER_CHAT_ID:
        await update.message.reply_text(
            "Выбери действие:",
            reply_markup=_user_menu_keyboard(include_task_button=_is_task_author(user.id))
        )
        return

    # OWNER — расширенное меню с кнопкой задач в персональном блоке.
    keyboard = InlineKeyboardMarkup([
        # ── Доступно всем ──────────────────────────────────────────
        [InlineKeyboardButton("── Общие функции ──", callback_data="menu_noop")],
        [
            InlineKeyboardButton("📸 Фото товара", callback_data="user_photo"),
        ],
        [
            InlineKeyboardButton("📄 Сформировать договор", callback_data="user_contract"),
        ],
        [
            InlineKeyboardButton("📊 Отчёт ОП", callback_data="menu_evening"),
        ],
        # ── Только руководитель ────────────────────────────────────
        [InlineKeyboardButton("── Только для меня ──", callback_data="menu_noop")],
        [
            InlineKeyboardButton("⏳ Стареющие", callback_data="menu_aging"),
        ],
        # ── Постановка задач в МойСклад ────────────────────────────
        [InlineKeyboardButton("── Постановка задач ──", callback_data="menu_noop")],
        [InlineKeyboardButton("📋 Поставить задачу", callback_data="menu_task")],
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

    if not query.from_user or query.from_user.id != OWNER_CHAT_ID:
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

    elif action == "menu_evening":
        await cmd_op_report(update, context)

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
            if s.get("user_id") == OWNER_CHAT_ID:
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
        "*Отчёты:*\n"
        "/report — недельный отчёт\n\n"
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
_user_awaiting: dict = {}  # user_id → "photo" | "contract" | "reconciliation"
_pending_price_comments: dict = {}  # manager_user_id → {alert_id, order_id, mgr_name, alert_type?, approver_chat_id?}
_pending_approver_input: dict = {}  # approver_chat_id → {alert_id, order_name, client_name, manager_name, manager_user_id}

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

# UUID полей контрагентов в МойСклад для рассылок
MS_ATTR_TELEGRAM = "15052610-34d7-11f1-0a80-1489000ec44a"
MS_ATTR_WHATSAPP = "1505270f-34d7-11f1-0a80-1489000ec44b"
MS_ATTR_MAX      = "1505236e-34d7-11f1-0a80-1489000ec449"
MS_ATTR_BY_TYPE  = {
    "telegram": MS_ATTR_TELEGRAM,
    "tgapi":    MS_ATTR_TELEGRAM,
    "whatsapp": MS_ATTR_WHATSAPP,
    "max":      MS_ATTR_MAX,
}

async def _write_contact_to_ms(agent_href: str, chat_type: str, chat_id: str) -> bool:
    """Записывает chat_id в поле контрагента в МойСклад."""
    import aiohttp
    from moysklad import get_headers
    attr_id = MS_ATTR_BY_TYPE.get(chat_type.lower())
    if not attr_id:
        return False
    MS_BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
    payload = {"attributes": [{"meta": {
        "href": f"{MS_BASE_URL}/entity/counterparty/metadata/attributes/{attr_id}",
        "type": "attributemetadata",
        "mediaType": "application/json"
    }, "value": str(chat_id)}]}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(agent_href, headers=get_headers(), json=payload) as r:
                return r.status in (200, 201)
    except Exception as e:
        logger.warning(f"_write_contact_to_ms: {e}")
        return False


async def handle_wazzup_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатие кнопки привязки Telegram контакта к компании."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split("|")

    # Новый контакт для рассылок: wazzup_mailing|link_key
    if parts[0] == "wazzup_mailing":
        if len(parts) < 2:
            return
        link_key = parts[1]
        pending = _pending_links.get(link_key)
        if not pending:
            await query.message.edit_text("❌ Сессия истекла, попробуй снова.")
            return
        # Помечаем как "для рассылок"
        pending["for_mailing"] = True
        _pending_links[link_key] = pending
        # Добавляем в стек пользователя
        if query.from_user.id not in _pending_links:
            _pending_links[query.from_user.id] = []
        existing_keys = [p.get("link_key") for p in _pending_links[query.from_user.id]] if isinstance(_pending_links.get(query.from_user.id), list) else []
        if link_key not in existing_keys:
            entry = {**pending, "link_key": link_key}
            if isinstance(_pending_links[query.from_user.id], list):
                _pending_links[query.from_user.id].append(entry)
            else:
                _pending_links[query.from_user.id] = [entry]
        CHANNEL_NAMES = {"telegram": "Telegram", "tgapi": "Telegram", "max": "Max", "whatsapp": "WhatsApp"}
        channel_label = CHANNEL_NAMES.get(pending.get("chat_type", ""), "")
        await query.message.edit_text(
            f"✅ Контакт для рассылок ({channel_label})\n\n"
            f"👤 *{pending['wazzup_name']}*\n\n"
            f"Как называется компания в МойСклад?\n"
            f"_(напиши название или часть)_",
            parse_mode="Markdown"
        )
        return

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
                    # Записываем chat_id в МойСклад
                    agent_href = cp_data.get("href", "")
                    if agent_href and pending.get("chat_type"):
                        ms_ok = await _write_contact_to_ms(
                            agent_href=agent_href,
                            chat_type=pending["chat_type"],
                            chat_id=pending["chat_id"],
                        )
                        if ms_ok:
                            logger.info(f"_write_contact_to_ms: {pending['chat_id']} ({pending['chat_type']}) → {cp_name}")
                        else:
                            logger.warning(f"_write_contact_to_ms: не удалось записать {pending['chat_id']} → {cp_name}")
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
        MANAGERS = ["Баласанян К.", "Дьяченко И.", "Мерзлякова Е.", "Скляр И.", "Иванов А."]
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

    # Откладываем идентификацию на 24 часа: wazzup_later|link_key
    if parts[0] == "wazzup_later":
        link_key = parts[1] if len(parts) > 1 else ""
        if link_key:
            db.postpone_pending_ident(link_key, hours=24)
            _wazzup_notified.discard(_pending_links.get(link_key, {}).get("chat_id", ""))
        await query.message.edit_text("⏰ Напомню через 24 часа.")
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
        await update.message.reply_text("Укажи chat_id: /clearwazzup <id>")
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
    _public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "f2b-production.up.railway.app")
    webhook_url = f"https://{_public_domain}/webhook/wazzup"

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

    # 1. БД — базовая проверка
    try:
        db._ensure_connection()
        results.append("✅ База данных — соединение OK")
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
            _public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "f2b-production.up.railway.app")
            async with session.get(f"https://{_public_domain}/health") as resp:
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

    # Диагностика входящих апдейтов: тип + chat + user
    _u = message.from_user
    _mt = ("video" if message.video else "video-doc" if (message.document and (message.document.mime_type or "").startswith("video/")) else "photo" if message.photo else "doc" if message.document else "text" if message.text else "other")
    logger.info(f"HM-IN: chat={message.chat_id} type={_mt} user={_u.id if _u else None} user_name={_u.full_name if _u else '-'}")

    # Видео от собственника/партнёра в личке → сохраняем в media до всех остальных return'ов
    if _u and _u.id in {OWNER_CHAT_ID, PARTNER_CHAT_ID}:
        if message.video:
            await save_media(message, "video")
            return
        if message.document and (message.document.mime_type or "").startswith("video/"):
            # видео отправленное как файл (большое или несжатое)
            file_id = message.document.file_id
            caption = message.caption or message.document.file_name or ""
            db.save_media(
                file_id=file_id,
                media_type="video",
                caption=caption,
                chat_id=message.chat_id,
                uploader=_u.full_name,
                date=datetime.now().isoformat()
            )
            await message.reply_text("Видео-файл сохранён в базу.")
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


        elif awaiting == "reconciliation":
            await message.reply_chat_action("typing")
            from datetime import datetime as _datetime
            buyer_query = text
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
            date_from = f"{_datetime.now().year}-01-01"
            date_to = _datetime.now().strftime("%Y-%m-%d")
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
                rec_data = await get_reconciliation_data(cp_id, date_from, date_to)
                if not rec_data or not rec_data.get("rows"):
                    await message.reply_text(
                        f"😕 За период {date_from} — {date_to} операций с *{cp_name}* не найдено.",
                        parse_mode="Markdown"
                    )
                    return
                pdf_bytes = generate_reconciliation_pdf(rec_data)
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
                await message.reply_document(
                    document=_io.BytesIO(pdf_bytes),
                    filename=f"Акт_сверки_{cp_name[:30]}_{date_to}.pdf",
                    caption=caption,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"reconciliation awaiting error: {e}", exc_info=True)
                await message.reply_text(f"❌ Ошибка формирования акта: {e}")
            await message.reply_text("Выбери действие:", reply_markup=_user_menu_keyboard())
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
            # (проверка по дате создания убрана — достаточно проверки CUTOFF выше)
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

    # Ответ менеджера на алерт цены в личке — пересылаем Виктору
    if user and chat_id == user.id and chat_id != OWNER_CHAT_ID and text:

        # 1. Approver написал текст комментария для менеджера (approval-алерт)
        if user.id in _pending_approver_input:
            pending = _pending_approver_input.pop(user.id)
            mgr_uid = pending.get("manager_user_id") or 0
            alert_id = pending["alert_id"]
            order_name = pending.get("order_name", "")
            client_name = pending.get("client_name", "")
            mgr_name = pending.get("manager_name", "?")

            if mgr_uid:
                try:
                    await context.bot.send_message(
                        chat_id=mgr_uid,
                        text=(
                            f"💬 *Вопрос по заказу {order_name} ({client_name})*\n\n"
                            f"{text}\n\n"
                            f"_Ответь в этот чат — ответ уйдёт обратно._"
                        ),
                        parse_mode="Markdown",
                    )
                    # Ждём reply менеджера → отправим обратно approver'у через _pending_price_comments
                    _pending_price_comments[mgr_uid] = {
                        "alert_id": alert_id,
                        "alert_type": "approval",
                        "approver_chat_id": user.id,
                        "order_id": pending.get("order_name", ""),
                        "order_name": order_name,
                        "client_name": client_name,
                        "mgr_name": mgr_name,
                    }
                    await message.reply_text("✅ Уйдёт менеджеру; жду его ответ.")
                except Exception as e:
                    logger.error(f"appr_comment forward: {e}")
                    await message.reply_text(f"❌ Не удалось отправить: {e}")
            else:
                await message.reply_text(
                    f"❌ Chat_id менеджера {mgr_name} не известен боту. "
                    f"Напишите ему напрямую."
                )
            return

        # 2. Ответ на запрос комментария по цене (price_*) ИЛИ от менеджера на approval
        if user.id in _pending_price_comments:
            pending = _pending_price_comments.pop(user.id)
            alert_id = pending.get("alert_id", 0)
            order_id = pending.get("order_id", "")
            order_name = pending.get("order_name", "")
            client_name = pending.get("client_name", "")
            mgr_name = pending.get("mgr_name", user.full_name)
            alert_type = pending.get("alert_type", "price")

            if alert_type == "approval":
                # Reply менеджера на approval-комментарий → шлём approver'у с шапкой заказа
                approver_chat_id = pending.get("approver_chat_id", OWNER_CHAT_ID)
                if alert_id:
                    db.close_approval_alert(alert_id, closed_by=user.id, comment=text)
                await context.bot.send_message(
                    chat_id=approver_chat_id,
                    text=(
                        f"💬 *Ответ по заказу {order_name} ({client_name})*\n"
                        f"👤 Менеджер: *{mgr_name}*\n\n"
                        f"{text}"
                    ),
                    parse_mode="Markdown",
                )
                await message.reply_text("✅ Ответ передан.")
                return

            # Старый flow для price_alerts
            if alert_id:
                db.close_price_alert(alert_id, text)
            await context.bot.send_message(
                chat_id=OWNER_CHAT_ID,
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
                        chat_id=OWNER_CHAT_ID,
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

        # Прочие сообщения в личке — бот не реагирует
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

    # Видео-форварды от собственника/партнёра → сохраняем как контент
    if message.video and user and user.id in {OWNER_CHAT_ID, PARTNER_CHAT_ID}:
        await save_media(message, "video")

    if not text:
        return

    # 2. Автоматическое извлечение задач (анализируем ВСЕ сообщения руководителя)
    # Список ID руководителей — добавь в .env
    manager_ids_str = os.getenv("MANAGER_IDS", "")
    manager_ids = [int(x) for x in manager_ids_str.split(",") if x.strip()]

    # Логируем ID для диагностики
    logger.info(f"Message from user.id={user.id}, name={user.full_name}, chat_id={message.chat_id}, manager_ids={manager_ids}")

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

    if action == "get_report":
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
                        "ирина": "дьяченко", "дьяченко": "дьяченко",
        }
        USER_MANAGER_DISPLAY = {
            "баласанян": "Карина Баласанян",
            "скляр": "Инесса Скляр",
            "мерзлякова": "Елена Мерзлякова",
            "дьяченко": "Ирина Дьяченко",
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
            "скляр": "Инесса Скляр", "дьяченко": "Ирина Дьяченко",
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

        # (проверка по дате создания убрана — достаточно проверки CUTOFF выше)
        if False:
            await message.reply_text("placeholder")
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
                        "ирина": "дьяченко", "дьяченко": "дьяченко",
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

# ─── Канал «Мониторинг» — закупочные прайсы поставщиков ────────────────────
# План фичи: F2B второй мозг/plans/2026-05-15-tg-канал-мониторинг-цен-поставщиков.md
# Обработка vision'ом — на стороне Claude Code (скилл update-market-intel),
# бот только сохраняет сырьё в БД и медиа на persistent volume Amvera.
MARKET_INTEL_CHAT_ID = int(os.getenv("MARKET_INTEL_CHAT_ID", "-1002964644525"))
MARKET_INTEL_DIR = os.getenv("MARKET_INTEL_DIR", "/data/market-intel")


async def handle_market_intel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохраняет channel_post из канала «Мониторинг»: текст + медиа."""
    msg = update.channel_post or update.edited_channel_post
    if not msg:
        return
    # Diagnostic: всегда логируем chat_id, чтобы видеть, какие каналы вообще достигают handler'а.
    # При несовпадении с whitelist — выходим, но запись в логе остаётся.
    logger.info(
        f"market_intel: channel_post received chat_id={msg.chat_id} "
        f"chat_title={getattr(msg.chat, 'title', '?')} "
        f"msg_id={msg.message_id} expected_chat_id={MARKET_INTEL_CHAT_ID}"
    )
    if msg.chat_id != MARKET_INTEL_CHAT_ID:
        return  # whitelist по chat_id

    # Тип сообщения и file_id (если есть)
    file_id = None
    file_ext = None
    if msg.photo:
        file_id = msg.photo[-1].file_id  # наибольшее разрешение
        file_ext = "jpg"
        msg_type = "photo"
    elif msg.document:
        file_id = msg.document.file_id
        # выбираем расширение по mime/имени
        fname = msg.document.file_name or ""
        if "." in fname:
            file_ext = fname.rsplit(".", 1)[-1].lower()[:8]
        else:
            mime = (msg.document.mime_type or "").lower()
            file_ext = "pdf" if "pdf" in mime else "bin"
        msg_type = "document"
    else:
        msg_type = "text"

    text_raw = msg.text or msg.caption or ""
    # python-telegram-bot 21+: forward_from_chat / forward_sender_name удалены,
    # вместо них единое поле forward_origin (MessageOrigin*).
    forward_from = None
    origin = getattr(msg, "forward_origin", None)
    if origin is not None:
        chat = getattr(origin, "chat", None)
        if chat is not None:
            forward_from = chat.title or chat.username or ""
        else:
            sender_user = getattr(origin, "sender_user", None)
            if sender_user is not None:
                forward_from = sender_user.full_name or sender_user.username or ""
            else:
                forward_from = getattr(origin, "sender_user_name", None) or getattr(origin, "sender_chat", None)
                if hasattr(forward_from, "title"):
                    forward_from = forward_from.title

    # Скачиваем медиа на persistent volume (если есть)
    file_path = None
    if file_id:
        try:
            ym = msg.date.strftime("%Y-%m") if msg.date else datetime.utcnow().strftime("%Y-%m")
            month_dir = os.path.join(MARKET_INTEL_DIR, ym)
            os.makedirs(month_dir, exist_ok=True)
            file_path = os.path.join(month_dir, f"{msg.message_id}.{file_ext}")
            tg_file = await context.bot.get_file(file_id)
            await tg_file.download_to_drive(file_path)
        except Exception as e:
            logger.error(f"market_intel: download failed for msg {msg.message_id}: {e}")
            file_path = None  # запись в БД всё равно создаём, но без файла

    saved_id = db.save_market_intel_message(
        tg_msg_id=msg.message_id,
        chat_id=msg.chat_id,
        posted_at=msg.date or datetime.utcnow(),
        msg_type=msg_type,
        text_raw=text_raw,
        file_path=file_path,
        file_ext=file_ext,
        forward_from=forward_from,
    )
    if saved_id:
        logger.info(
            f"market_intel: saved id={saved_id} tg_msg={msg.message_id} type={msg_type} "
            f"file={'yes' if file_path else 'no'} fwd={forward_from or '-'}"
        )
    else:
        logger.info(f"market_intel: duplicate tg_msg={msg.message_id} skipped")


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
    elif media_type == "video":
        file_id = message.video.file_id
        if not caption:
            await message.reply_text(
                "Видео сохранено в базу без подписи.\n"
                "Чтобы его можно было найти, напиши следующим сообщением название товара.",
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

    elif message.video:
        file_id = message.video.file_id
        db.save_media(
            file_id=file_id,
            media_type="video",
            caption=caption,
            chat_id=message.chat_id,
            uploader="Контент F2B",
            date=datetime.now().isoformat()
        )
        logger.info(f"Сохранено видео из канала Контент: '{caption}' file_id={file_id}")

    elif message.document and message.document.mime_type and message.document.mime_type.startswith("video/"):
        file_id = message.document.file_id
        db.save_media(
            file_id=file_id,
            media_type="video",
            caption=caption or message.document.file_name or "",
            chat_id=message.chat_id,
            uploader="Контент F2B",
            date=datetime.now().isoformat()
        )
        logger.info(f"Сохранено видео-документ из канала Контент: '{caption}'")

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
    _public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "f2b-production.up.railway.app")
    webhook_url = f"https://{_public_domain}/webhook/moysklad"
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

async def cmd_pdz_disabled(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Заглушка для всех старых команд по дебиторке (/pdz, /pdz_results, /pdz_test,
    /pdz_evening, /дебиторка, /debtors). Новая механика — без команд, см. объявление в группе ОП."""
    if update.message:
        await update.message.reply_text(
            "Команда временно отключена. Скоро запустим новую механику работы с дебиторкой — "
            "регламент будет в группе ОП."
        )


async def cmd_pdz_token_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Диагностика: проверка какой токен МС в env + жив ли он + есть ли права."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    tok = os.getenv("MOYSKLAD_TOKEN", "")
    if not tok:
        await update.message.reply_text("❌ MOYSKLAD_TOKEN в env не задан!")
        return

    lines = [
        f"🔑 Токен в env: len={len(tok)}",
        f"   prefix={tok[:4]}  suffix={tok[-4:]}",
        "",
    ]

    import aiohttp
    from moysklad import MS_BASE, get_headers
    try:
        async with aiohttp.ClientSession() as session:
            # 1) /context/employee — проверка живого токена
            async with session.get(f"{MS_BASE}/context/employee", headers=get_headers()) as r:
                lines.append(f"/context/employee → {r.status}")
                if r.status == 200:
                    data = await r.json()
                    lines.append(f"   me: {data.get('name', '?')} ({data.get('email', '?')})")

            # 2) /report/counterparty — берём один agent_id и тестируем
            async with session.get(f"{MS_BASE}/entity/customerorder?limit=1", headers=get_headers()) as r:
                if r.status == 200:
                    d = await r.json()
                    if d.get("rows"):
                        href = d["rows"][0]["agent"]["meta"]["href"]
                        aid = href.split("/")[-1]
                        async with session.get(f"{MS_BASE}/report/counterparty/{aid}", headers=get_headers()) as r2:
                            lines.append(f"/report/counterparty/{aid[:8]} → {r2.status}")
                            if r2.status == 200:
                                rd = await r2.json()
                                lines.append(f"   balance: {rd.get('balance')}")
                            else:
                                body = await r2.text()
                                lines.append(f"   body: {body[:200]}")
    except Exception as e:
        lines.append(f"❌ {type(e).__name__}: {e}")

    await update.message.reply_text("\n".join(lines))


async def cmd_pdz_snapshot_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Диагностика Фазы 2 ПДЗ-автоматики: руками запустить снимок и вернуть
    в ЛС количество записей + 3 примера строк. Доступ — только собственник."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    await update.message.reply_text("⏳ Тяну снимок customerorder из МойСклад...")
    try:
        from moysklad import pdz_take_snapshot
        rows = await pdz_take_snapshot()
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка снимка: {e}")
        return

    try:
        inserted = db.save_pdz_snapshot(rows)
    except Exception as e:
        await update.message.reply_text(
            f"⚠️ Снимок получен ({len(rows)} строк), но запись в БД упала: {e}"
        )
        return

    lines = [f"📸 *Снимок ПДЗ*", f"Получено: {len(rows)} · Записано: {inserted}", ""]
    if rows:
        lines.append("*Примеры (первые 3):*")
        for r in rows[:3]:
            bal_raw = r.get("agent_balance")
            bal_str = "—" if bal_raw is None else f"{bal_raw}"
            lines.append(
                f"• `{r.get('order_name','?')}` · {r.get('agent_name','?')} · "
                f"тег={r.get('manager_tag') or '—'} · "
                f"исх={r.get('ppm_initial')} · нов={r.get('ppm_new') or '—'} · "
                f"reason={r.get('reason_id') or '—'} · "
                f"{r.get('payed_sum')}/{r.get('total_sum')} · "
                f"balance={bal_str}"
            )
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_pdz_events_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Диагностика Фазы 3 ПДЗ-автоматики: руками запускает pdz_process_events_job.
    Отвечает короткой сводкой по событиям и аудиту. Доступ — только собственник."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    await update.message.reply_text("⏳ Запускаю обработку событий обещаний...")
    try:
        from scheduler import pdz_process_events_job
        result = await pdz_process_events_job(context.application, db)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    if result is None:
        await update.message.reply_text("⚠️ pdz_process_events_job вернула None")
        return

    err = result.get("error")
    if err:
        await update.message.reply_text(f"⚠️ Job упала: {err}")
        return

    text = (
        f"✅ События: {result.get('events_total', 0)} "
        f"(set:{result.get('set', 0)}, moved:{result.get('moved', 0)}, broken:{result.get('broken', 0)}). "
        f"Аудит ppm_initial: {result.get('initial_changes', 0)} алертов"
    )
    await update.message.reply_text(text)


async def cmd_pdz_html(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/pdz_html` — ручной запуск регенерации HTML-отчёта «Дебиторка» (Фаза 5).

    Только собственник (OWNER_CHAT_ID). Запускает pdz_generate_html_job —
    тот рендерит HTML из БД (snapshot + promise_log, без МС API), кладёт в
    кэш и шлёт собственнику ссылку с токеном TTL 24ч. Команда отвечает
    краткой сводкой результата."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        if update.message:
            await update.message.reply_text("Нет доступа")
        return
    if not update.message:
        return

    await update.message.reply_text("⏳ Регенерирую HTML-отчёт «Дебиторка»...")
    try:
        from scheduler import pdz_generate_html_job
        result = await pdz_generate_html_job(context.application, db)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    status = (result or {}).get("status")
    if status == "ok":
        url = result.get("url") or "—"
        size_kb = round(int(result.get("html_size") or 0) / 1024, 1)
        await update.message.reply_text(
            f"✅ Отчёт обновлён ({size_kb} KB)\n{url}",
            disable_web_page_preview=True,
        )
    else:
        err = (result or {}).get("error") or "—"
        await update.message.reply_text(
            f"⚠️ Статус: {status}. Ошибка: {err}"
        )


async def cmd_pdz_overdue_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/pdz_overdue_test <тег>` — печатает первые 5 строк просрочки менеджера.
    Тег — фамилия в нижнем регистре (скляр, баласанян, мерзлякова, дьяченко, коликов).
    Доступ — только собственник."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    if not context.args:
        await update.message.reply_text(
            "Укажи тег менеджера: `/pdz_overdue_test скляр`",
            parse_mode="Markdown",
        )
        return
    tag = context.args[0].strip().lower()

    await update.message.reply_text(f"⏳ Считаю просрочки для тега «{tag}»...")
    try:
        from moysklad import pdz_overdue_for_manager, fmt_money
        items = await pdz_overdue_for_manager(tag, db=db)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    if not items:
        await update.message.reply_text(f"✅ По тегу «{tag}» просрочек нет.")
        return

    # items — список dict, сгруппированный по контрагенту (group_by_agent=True
    # по умолчанию). Показываем ВСЕХ клиентов, разбивая на несколько TG-сообщений
    # по лимиту 3500 символов (запас от лимита Telegram 4096).
    header = f"📋 *Просрочки {tag}* — клиентов: {len(items)}"
    chunks: list[list[str]] = [[header, ""]]
    current_len = len(header) + 2

    def order_word(cnt: int) -> str:
        if cnt % 10 == 1 and cnt % 100 != 11:
            return "заказ"
        if cnt % 10 in (2, 3, 4) and cnt % 100 not in (12, 13, 14):
            return "заказа"
        return "заказов"

    def breaks_word(cnt: int) -> str:
        if cnt % 10 == 1 and cnt % 100 != 11:
            return "срыв"
        if cnt % 10 in (2, 3, 4) and cnt % 100 not in (12, 13, 14):
            return "срыва"
        return "срывов"

    for it in items:
        name = (it.get("agent_name") or "—").replace("*", "").replace("_", "")
        url = it.get("ms_url_first_order") or "#"
        cnt = it.get("orders_count", 0)
        breaks = int(it.get("breaks_count", 0) or 0)
        prefix = "🔴 " if breaks > 0 else ""
        suffix = f" ({breaks} {breaks_word(breaks)} за 90д)" if breaks > 0 else ""
        line = (
            f"{prefix}[{name}]({url}) · {cnt} {order_word(cnt)} · "
            f"{it.get('max_days_overdue', 0)} дн · "
            f"{fmt_money(it.get('total_unpaid', 0))}{suffix}"
        )
        if current_len + len(line) + 1 > 3500:
            chunks.append([])
            current_len = 0
        chunks[-1].append(line)
        current_len += len(line) + 1

    for chunk in chunks:
        if not chunk:
            continue
        await update.message.reply_text(
            "\n".join(chunk),
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )


# ─── ПДЗ Фаза 4: тестовые команды ────────────────────────────────────────

# In-memory токен подтверждения для «боевой» рассылки дайджестов.
# Проставляется в cmd_pdz_send_digests_test, сбрасывается через 10 минут
# либо после успешного запуска /pdz_send_digests_test_now.
_PDZ_DIGESTS_CONFIRM_TS = {"ts": None}


async def cmd_pdz_send_digests_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Диагностика Фазы 4: подтверждает намерение разослать боевые дайджесты.
    Ничего не шлёт — только включает окно подтверждения на 10 минут.
    Реальная рассылка — /pdz_send_digests_test_now. Доступ — только собственник."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    from datetime import datetime as _dt
    _PDZ_DIGESTS_CONFIRM_TS["ts"] = _dt.utcnow()
    await update.message.reply_text(
        "⚠️ Сейчас разошлю боевые дайджесты 5 менеджерам. "
        "Подтверди — отправь ещё раз `/pdz_send_digests_test_now`",
        parse_mode="Markdown",
    )


async def cmd_pdz_send_digests_test_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Реально запускает pdz_send_digests_job — рассылает боевые дайджесты
    менеджерам в личку. Требует, чтобы перед этим был вызван
    /pdz_send_digests_test (окно подтверждения 10 минут).
    Доступ — только собственник."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    from datetime import datetime as _dt, timedelta as _td
    ts = _PDZ_DIGESTS_CONFIRM_TS.get("ts")
    if not ts or (_dt.utcnow() - ts) > _td(minutes=10):
        await update.message.reply_text(
            "❌ Нет действующего подтверждения. Сначала отправь `/pdz_send_digests_test`",
            parse_mode="Markdown",
        )
        return
    # Сбрасываем токен — повторный вызов потребует нового /pdz_send_digests_test.
    _PDZ_DIGESTS_CONFIRM_TS["ts"] = None

    await update.message.reply_text("⏳ Шлю боевые дайджесты менеджерам...")
    try:
        from scheduler import pdz_send_digests_job
        report = await pdz_send_digests_job(context.application, db)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    if not report:
        await update.message.reply_text("⚠️ pdz_send_digests_job вернула пусто")
        return

    lines = ["✅ Дайджесты разосланы. Сводка:"]
    for tag, info in report.items():
        status = info.get("status")
        if status == "ok":
            lines.append(
                f"• {tag} ({info.get('manager','?')}) — {info.get('clients',0)} клиентов, "
                f"{info.get('messages_sent',0)}/{info.get('messages_total',0)} сообщений"
            )
        elif status == "no_overdue":
            lines.append(f"• {tag} — просрочек нет")
        elif status == "no_chat_id":
            lines.append(f"• {tag} ({info.get('manager','?')}) — ⚠️ chat_id не найден ({info.get('clients',0)} клиентов)")
        elif status == "error_fetch":
            lines.append(f"• {tag} — ❌ ошибка получения: {info.get('error','?')}")
        else:
            lines.append(f"• {tag} — {status}")
    await update.message.reply_text("\n".join(lines))


async def cmd_pdz_send_owner_pending_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Диагностика Фазы 4: руками запускает pdz_send_owner_pending_job.
    Результат собственник видит у себя же (job шлёт OWNER_CHAT_ID).
    Доступ — только собственник."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    await update.message.reply_text("⏳ Считаю необработанных для пинга собственнику...")
    try:
        from scheduler import pdz_send_owner_pending_job
        result = await pdz_send_owner_pending_job(context.application, db)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    if not result:
        await update.message.reply_text("⚠️ pdz_send_owner_pending_job вернула пусто")
        return

    status = result.get("status")
    if status == "all_clear":
        await update.message.reply_text("ℹ️ Сводка: все менеджеры обработали (сообщение пошло собственнику).")
    elif status == "sent":
        await update.message.reply_text(
            f"ℹ️ Сводка: {result.get('managers',0)} менеджеров с необработанными, "
            f"{result.get('messages_sent',0)}/{result.get('messages_total',0)} сообщений отправлено."
        )
    elif status == "no_owner_chat_id":
        await update.message.reply_text("⚠️ OWNER_CHAT_ID не задан в env.")
    elif status == "error":
        await update.message.reply_text(f"❌ Job упал: {result.get('error','?')}")
    else:
        await update.message.reply_text(f"ℹ️ status={status}")


async def cmd_pdz_breaks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """`/pdz_breaks` — топ-30 клиентов по числу срывов обещаний за 90 дней.

    Источник — таблица `promise_log` (event_type='broken'). Если в окне нет
    зафиксированных срывов (а так будет в первые дни после старта Фазы 4) —
    выводим «📭 За 90 дней нет зафиксированных срывов». Доступ — только
    собственник (OWNER_CHAT_ID).
    """
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not update.message:
        return

    try:
        rows = db.get_promise_breaks_top(limit=30, days_window=90)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    if not rows:
        await update.message.reply_text("📭 За 90 дней нет зафиксированных срывов")
        return

    def breaks_word(cnt: int) -> str:
        if cnt % 10 == 1 and cnt % 100 != 11:
            return "срыв"
        if cnt % 10 in (2, 3, 4) and cnt % 100 not in (12, 13, 14):
            return "срыва"
        return "срывов"

    # URL первого заказа клиента — берём любой свежий заказ из последнего
    # snapshot по этому agent_id (используем уже хранящийся в БД ms_url).
    try:
        latest_snap = db.get_latest_snapshot() or []
    except Exception as e:
        latest_snap = []
        logger.warning(f"cmd_pdz_breaks: get_latest_snapshot failed: {e}")
    agent_url_map: dict = {}
    for r in latest_snap:
        aid = r.get("agent_id") or ""
        if not aid or aid in agent_url_map:
            continue
        order_id = r.get("order_id") or ""
        if order_id:
            agent_url_map[aid] = f"https://online.moysklad.ru/app/#customerorder/edit?id={order_id}"

    from datetime import datetime as _dt, timezone as _tz
    now_utc = _dt.now(_tz.utc)

    header = "🔴 *Срывы обещаний за 90 дней — топ-30*"
    chunks: list[list[str]] = [[header, ""]]
    cur_len = len(header) + 2

    for r in rows:
        aid = r.get("agent_id") or ""
        name = (r.get("agent_name") or "—").replace("*", "").replace("_", "")
        cnt = int(r.get("breaks_count", 0) or 0)
        tag = r.get("manager_tag") or "—"
        url = agent_url_map.get(aid)

        last_at = r.get("last_break_at")
        days_ago_str = "?"
        if last_at is not None:
            try:
                if isinstance(last_at, _dt):
                    if last_at.tzinfo is None:
                        last_at = last_at.replace(tzinfo=_tz.utc)
                    delta_days = (now_utc - last_at).days
                    days_ago_str = str(max(0, delta_days))
            except Exception:
                days_ago_str = "?"

        client_label = f"[{name}]({url})" if url else name
        line = (
            f"{client_label} · {cnt} {breaks_word(cnt)} · "
            f"последний {days_ago_str} дн назад · менеджер: {tag}"
        )
        if cur_len + len(line) + 1 > 3500:
            chunks.append([])
            cur_len = 0
        chunks[-1].append(line)
        cur_len += len(line) + 1

    for chunk in chunks:
        if not chunk:
            continue
        await update.message.reply_text(
            "\n".join(chunk),
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )


async def cmd_test_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/test_group [название группы] — сумма продаж по группе товаров за текущий месяц по менеджерам."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
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
        "дьяченко":  "Ирина",
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
    if not user or user.id != OWNER_CHAT_ID:
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


# Временные токены для веб-отчёта: token → {expires, user_id, mgr_filter}
_report_tokens: dict = {}


async def cmd_refresh_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/refresh_history — принудительно обновить историю менеджеров."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text("🔄 Собираю историю по месяцам, это займёт несколько минут...")
    try:
        from moysklad import get_manager_monthly_history
        TAGS = {
            "скляр": "Инесса Скляр",
            "мерзлякова": "Елена Мерзлякова",
            "баласанян": "Карина Баласанян",
            "дьяченко": "Ирина Дьяченко",
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
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    allowed_ids = manager_ids + [OWNER_CHAT_ID]
    if not user or user.id not in allowed_ids:
        await update.message.reply_text("❌ У вас нет доступа к этой команде.")
        return
    if not context.args:
        rows = db._fetchall(
            "SELECT buyer_name, contract_number, created_at FROM contracts ORDER BY created_at DESC LIMIT 10"
        )
        if not rows:
            await update.message.reply_text("📭 В базе нет сохранённых договоров.")
            return
        lines = ["📄 *Последние договоры в базе:*\n"]
        for r in rows:
            dt = r["created_at"].strftime("%d.%m.%Y") if r.get("created_at") else "—"
            lines.append(f"• {r['buyer_name']} №{r['contract_number']} от {dt}")
        lines.append("\nИспользование: /reissue_contract [название компании]")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    buyer_query = " ".join(context.args)
    existing = db.find_contract_by_buyer(buyer_query)
    if not existing:
        # Пробуем частичный поиск через LIKE
        rows = db._fetchall(
            "SELECT * FROM contracts WHERE LOWER(buyer_name) LIKE LOWER(%s) ORDER BY created_at DESC LIMIT 5",
            (f"%{buyer_query}%",)
        )
        if not rows:
            await update.message.reply_text(
                f"❌ Договор с '{buyer_query}' не найден в базе.\n"
                f"Попробуй часть названия, например: /reissue_contract ПРОДУКТЫ"
            )
            return
        if len(rows) == 1:
            existing = rows[0]
        else:
            lines = ["🔍 Найдено несколько договоров — уточни название:\n"]
            for r in rows:
                lines.append(f"• {r['buyer_name']} (№{r['contract_number']})")
            await update.message.reply_text("\n".join(lines))
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
    manager_ids = [int(x) for x in os.getenv("MANAGER_IDS", "").split(",") if x.strip()]
    allowed_ids = manager_ids + [OWNER_CHAT_ID]
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
    if not user or user.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text("🔄 Собираю данные из МойСклад... (3-5 мин)")
    try:
        # Сначала удаляем старый кэш
        db._execute("DELETE FROM report_cache")
        # Собираем синхронно — ждём результата
        data = await _build_report_data()
        db.set_report_cache(data)
        mgrs = list(data.get("facts", {}).keys())
        att = data.get("attestation", {})
        wt = data.get("weekly_targets", {})
        lines = ["✅ Кэш обновлён!\n"]
        for mg in mgrs:
            short = data.get("short_names", {}).get(mg, mg)
            a = att.get(mg, {})
            w = wt.get(mg, {})
            att_str = f"общая {a.get('general','—')}% / АКБ {a.get('akb','—')}%" if a else "аттестация не задана"
            lines.append(f"*{short}*: {att_str}")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"cmd_refresh_cache: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_web_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/web_report — временная ссылка на интерактивный отчёт ОП (1 час)."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
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
    if not user or user.id != OWNER_CHAT_ID:
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
        "дьяченко":  "Ирина",
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
    if not user or user.id != OWNER_CHAT_ID:
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
        "дьяченко":  "Ирина",
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
    if not user or user.id != OWNER_CHAT_ID:
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
    if not user or user.id != OWNER_CHAT_ID:
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
    if not user or user.id != OWNER_CHAT_ID:
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
    if not user or user.id != OWNER_CHAT_ID:
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
        "дьяченко":   "Ирина Дьяченко",
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
    if not user or user.id != OWNER_CHAT_ID:
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
        "скляр": "Инесса", "дьяченко": "Ирина",
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

async def cmd_block_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/block [user_id] — заблокировать пользователя."""
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
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
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
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
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
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

async def handle_approval_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает нажатия на кнопки объединённого алерта «На согласовании / ЗА ЛИМИТОМ».
    Callbacks: appr_ok | appr_confirm | appr_cancel | appr_comment.
    План: 2026-05-21-объединённый-алерт-на-согласование.md, Фаза 3.
    """
    query = update.callback_query
    await query.answer()
    user = query.from_user

    parts = query.data.split("|")
    action = parts[0]
    try:
        alert_id = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, IndexError):
        alert_id = 0
    if not alert_id:
        return

    # Авторизация: пользователь должен быть в APPROVERS_CHAT_IDS (или дефолтный OWNER)
    raw_approvers = os.getenv("APPROVERS_CHAT_IDS", "").strip()
    if raw_approvers:
        approvers = [int(x.strip()) for x in raw_approvers.split(",")
                     if x.strip().lstrip("-").isdigit()]
    else:
        approvers = [OWNER_CHAT_ID]
    if user.id not in approvers:
        await query.answer("⛔ Только для согласующих.", show_alert=True)
        return

    alert_data = db.get_approval_alert(alert_id)
    if not alert_data:
        await query.answer("Алерт не найден или закрыт.", show_alert=True)
        return

    order_id = alert_data["order_id"]
    colors = alert_data.get("colors_json") or {}
    if isinstance(colors, str):
        import json as _json
        try:
            colors = _json.loads(colors)
        except Exception:
            colors = {}

    MS_STATE_AGREED = "005f3651-9a9a-11f0-0a80-03a900027474"

    # --- appr_ok ---
    if action == "appr_ok":
        # Если уже закрыт — молча подтверждаем
        if alert_data.get("closed_at"):
            await query.answer("Уже согласовано ✓", show_alert=False)
            return
        has_non_green = any(c != "green" for c in colors.values())
        if has_non_green:
            n_red = sum(1 for c in colors.values() if c == "red")
            n_yellow = sum(1 for c in colors.values() if c == "yellow")
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Да, согласовать", callback_data=f"appr_confirm|{alert_id}"),
                InlineKeyboardButton("❌ Отмена",          callback_data=f"appr_cancel|{alert_id}"),
            ]])
            await query.message.reply_text(
                f"⚠ Точно согласовать? В светофоре {n_red} красных, {n_yellow} жёлтых.",
                reply_markup=kb,
            )
            return
        # all-green → действие сразу (fall-through к appr_confirm)
        action = "appr_confirm"

    # --- appr_confirm ---
    if action == "appr_confirm":
        from moysklad import set_order_state, get_headers, MS_BASE
        import aiohttp

        # Idempotency: GET state перед PATCH
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{MS_BASE}/entity/customerorder/{order_id}",
                    headers=get_headers(),
                    params={"expand": "state"},
                ) as r:
                    if r.status == 200:
                        ord_data = await r.json()
                        cur_state_id = ord_data.get("state", {}).get("meta", {}).get(
                            "href", "").split("/")[-1]
                        if cur_state_id == MS_STATE_AGREED:
                            db.close_approval_alert(alert_id, closed_by=user.id)
                            await query.answer("Уже согласовано ✓", show_alert=False)
                            return
        except Exception as e:
            logger.warning(f"appr_confirm idempotency check: {e}")

        ok = await set_order_state(order_id, MS_STATE_AGREED)
        if ok:
            from datetime import datetime, timezone, timedelta
            now_msk = datetime.now(timezone(timedelta(hours=3)))
            db.close_approval_alert(alert_id, closed_by=user.id)
            try:
                await query.edit_message_text(
                    query.message.text + f"\n\n✅ Согласовал: {user.full_name} в {now_msk.strftime('%H:%M')}",
                    parse_mode="Markdown",
                )
            except Exception:
                # Если это confirmation-сообщение (без кнопок ниже) — просто отвечаем
                await query.message.reply_text("✅ Согласовано")
        else:
            await query.answer("❌ Ошибка смены статуса в МС", show_alert=True)
        return

    # --- appr_cancel ---
    if action == "appr_cancel":
        try:
            await query.message.delete()
        except Exception:
            pass
        return

    # --- appr_comment ---
    if action == "appr_comment":
        if alert_data.get("closed_at"):
            await query.answer("Алерт уже закрыт.", show_alert=True)
            return
        _pending_approver_input[user.id] = {
            "alert_id": alert_id,
            "order_name": alert_data.get("order_name", ""),
            "client_name": alert_data.get("client_name", ""),
            "manager_name": alert_data.get("manager_name", ""),
            "manager_user_id": alert_data.get("manager_user_id", 0),
        }
        mgr_name = alert_data.get("manager_name", "?")
        await query.message.reply_text(
            f"💬 Напишите вопрос/инструкцию менеджеру *{mgr_name}* одним сообщением. "
            f"Уйдёт ему в личку.",
            parse_mode="Markdown",
        )
        return


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

    TAGS = {"скляр":"Инесса Скляр","мерзлякова":"Елена Мерзлякова","баласанян":"Карина Баласанян","дьяченко":"Ирина Дьяченко","коликов":"Денис Коликов"}

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

    MONTHLY_PLANS = {
        "2026-05": {
            "Инесса Скляр":     {"shipments": 210, "revenue": 22_000_000, "clients": 28, "new_clients": 5, "attracted": 1_000_000},
            "Карина Баласанян": {"shipments": 180, "revenue": 6_000_000,  "clients": 44, "new_clients": 5, "attracted": 1_100_000},
            "Елена Мерзлякова": {"shipments": 67,  "revenue": 6_500_000,  "clients": 23, "new_clients": 5, "attracted": 300_000},
            "Ирина Дьяченко":   {"shipments": 4,   "revenue": 500_000,    "clients": 2,  "new_clients": 1, "attracted": 12_500},
            "Денис Коликов":    {"shipments": 15,  "revenue": 1_000_000,  "clients": 12, "new_clients": 5, "attracted": 50_000},
        },
    }
    current_month_key = today.strftime("%Y-%m")
    if current_month_key in MONTHLY_PLANS:
        PLANS = MONTHLY_PLANS[current_month_key]
    else:
        latest_month = max(MONTHLY_PLANS.keys())
        logger.warning(f"План на {current_month_key} не задан в MONTHLY_PLANS, использую {latest_month}")
        PLANS = MONTHLY_PLANS[latest_month]
    WEEKLY_PLANS = {
        "Инесса Скляр":     {"shipments": 25,  "revenue": 2_000_000,  "clients": 10, "new_clients": 1, "attracted": 250_000},
        "Карина Баласанян": {"shipments": 40,  "revenue": 1_200_000,  "clients": 16, "new_clients": 1, "attracted": 275_000},
        "Елена Мерзлякова": {"shipments": 10,  "revenue": 1_000_000,  "clients": 5,  "new_clients": 1, "attracted": 75_000},
        "Ирина Дьяченко":   {"shipments": 1,   "revenue": 50_000,     "clients": 1,  "new_clients": 1, "attracted": 12_500},
        "Денис Коликов":    {"shipments": 1,   "revenue": 50_000,     "clients": 1,  "new_clients": 1, "attracted": 12_500},
    }
    SHORT_NAMES = {"Инесса Скляр":"Инесса","Карина Баласанян":"Карина","Елена Мерзлякова":"Елена","Ирина Дьяченко":"Ирина","Денис Коликов":"Денис"}

    # Загружаем накопительные недельные цели из БД (set_weekly) — перекрывают WEEKLY_PLANS
    weekly_targets = {}
    METRICS_KEYS = ["revenue", "shipments", "clients", "new_clients", "attracted"]
    all_mgr_names = list(PLANS.keys())
    for mgr_name in all_mgr_names:
        targets = {}
        for metric in METRICS_KEYS:
            try:
                row = db._fetchone(
                    "SELECT value FROM bot_settings WHERE key=%s",
                    (f"weekly_target_{mgr_name}_{metric}",)
                )
                if row:
                    targets[metric] = float(row["value"])
            except Exception:
                pass
        # Fallback на WEEKLY_PLANS если в БД нет данных
        if targets:
            weekly_targets[mgr_name] = targets
        else:
            weekly_targets[mgr_name] = WEEKLY_PLANS.get(mgr_name, {})

    # Аттестация из БД
    attestation = {}
    for mgr_name in all_mgr_names:
        att = {}
        for kind in ["general", "akb"]:
            try:
                row = db._fetchone(
                    "SELECT value FROM bot_settings WHERE key=%s",
                    (f"attestation_{kind}_{mgr_name}",)
                )
                if row:
                    att[kind] = int(row["value"])
            except Exception:
                pass
        if att:
            attestation[mgr_name] = att

    # Синхронизируем счётчики с реальными списками имён
    for mgr_name in facts:
        facts[mgr_name]["new_clients"] = len(new_client_names.get(mgr_name, []))
        facts[mgr_name]["lost_clients"] = len(lost_client_names.get(mgr_name, []))

    return {
        "date": today.strftime("%d.%m.%Y"),
        "facts": facts,
        "plans": PLANS,
        "weekly_plans": WEEKLY_PLANS,
        "weekly_targets": weekly_targets,
        "attestation": attestation,
        "short_names": SHORT_NAMES,
        "new_client_names": new_client_names,
        "lost_client_names": lost_client_names,
        "mgr_history": mgr_history,
    }


async def cmd_reset_agreed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/reset_agreed [номер заказа] — сбросить флаг отправки уведомления."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /reset_agreed [номер заказа, например 01645]")
        return
    query_val = " ".join(context.args)
    if len(query_val) > 20 and "-" in query_val:
        db._execute("DELETE FROM agreed_notifications WHERE order_id=%s", (query_val,))
        await update.message.reply_text(f"✅ Флаг сброшен для `{query_val}`", parse_mode="Markdown")
        return
    try:
        import aiohttp
        from moysklad import get_headers, MS_BASE
        await update.message.reply_text(f"🔍 Ищу заказ №{query_val}...")
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/customerorder",
                headers=get_headers(),
                params={"filter": f"name~{query_val}", "limit": 5}
            ) as resp:
                data = await resp.json()
        rows = data.get("rows", [])
        if not rows:
            await update.message.reply_text(f"❌ Заказ №{query_val} не найден в МойСклад.")
            return
        results = []
        for row in rows:
            order_id = row["id"]
            order_name = row.get("name", "")
            agent_name = row.get("agent", {}).get("name", "")
            db._execute("DELETE FROM agreed_notifications WHERE order_id=%s", (order_id,))
            results.append(f"✅ №{order_name} — {agent_name}\n   ID: `{order_id}`")
        await update.message.reply_text(
            "Флаги сброшены:\n\n" + "\n".join(results) +
            "\n\nТеперь смени статус заказа на «Согласовано» — уведомление отправится.",
            parse_mode="Markdown"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_notifier_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/notifier_status — заказы за сегодня которым не ушла рассылка."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text("🔍 Проверяю заказы за сегодня...")
    try:
        import aiohttp
        from moysklad import get_headers, MS_BASE
        from datetime import date
        today = date.today().isoformat()
        MS_STATE_AGREED = "005f3651-9a9a-11f0-0a80-03a900027474"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/customerorder",
                headers=get_headers(),
                params={
                    "filter": f"state={MS_BASE}/entity/customerorderstatus/{MS_STATE_AGREED};moment>={today} 00:00:00",
                    "expand": "agent,state",
                    "limit": 100,
                }
            ) as resp:
                data = await resp.json()
        rows = data.get("rows", [])
        if not rows:
            await update.message.reply_text("✅ Сегодня согласованных заказов нет.")
            return
        sent, not_sent = [], []
        for row in rows:
            order_id = row["id"]
            order_name = row.get("name", "")
            agent_name = row.get("agent", {}).get("name", "")
            notified = db._fetchone("SELECT order_id FROM agreed_notifications WHERE order_id=%s", (order_id,))
            if notified:
                sent.append(f"  ✅ №{order_name} — {agent_name}")
            else:
                contact = db._fetchone(
                    "SELECT chat_id FROM wazzup_contact_map WHERE LOWER(company_name) LIKE LOWER(%s) LIMIT 1",
                    (f"%{agent_name}%",)
                )
                reason = "нет контакта в базе" if not contact else "неизвестно"
                not_sent.append(f"  ❌ №{order_name} — {agent_name} ({reason})")
        lines = [f"📊 Рассылка за {today} — всего {len(rows)} заказов\n"]
        if not_sent:
            lines.append(f"Не получили ({len(not_sent)}):")
            lines.extend(not_sent)
            lines.append("\nЧтобы дослать: /reset_agreed [номер заказа]")
        else:
            lines.append("✅ Все получили рассылку!")
        if sent:
            lines.append(f"\nПолучили ({len(sent)}):")
            lines.extend(sent)
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        logger.error(f"cmd_notifier_status: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_set_attestation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_attestation [имя] [общая|акб] [процент] — задать аттестацию менеджера."""
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
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
        "ирина": "Ирина Дьяченко", "дьяченко": "Ирина Дьяченко",
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


# ─── Карты менеджеров и метрик для /set_weekly и /set_weekly_bulk ────────────
_MGR_NAME_MAP = {
    "инесса": "Инесса Скляр", "скляр": "Инесса Скляр",
    "карина": "Карина Баласанян", "баласанян": "Карина Баласанян",
    "елена": "Елена Мерзлякова", "лена": "Елена Мерзлякова", "мерзлякова": "Елена Мерзлякова",
    "ирина": "Ирина Дьяченко", "дьяченко": "Ирина Дьяченко",
    "денис": "Денис Коликов", "коликов": "Денис Коликов",
}
_WEEKLY_METRIC_MAP = {
    "выручка": "revenue", "выручки": "revenue", "выручку": "revenue", "revenue": "revenue",
    "отгрузки": "shipments", "отгрузка": "shipments", "отгрузок": "shipments", "отгрузке": "shipments", "shipments": "shipments",
    "акб": "clients", "клиенты": "clients", "клиент": "clients", "клиента": "clients", "клиентов": "clients", "clients": "clients",
    "новые": "new_clients", "новых": "new_clients", "новый": "new_clients", "новых_клиентов": "new_clients", "new_clients": "new_clients",
    "привл": "attracted", "привлеченные": "attracted", "привлеч": "attracted", "привлеченных": "attracted", "attracted": "attracted",
}
_WEEKLY_METRIC_LABELS = {
    "revenue": "Выручка",
    "shipments": "Отгрузки",
    "clients": "АКБ",
    "new_clients": "Новые",
    "attracted": "Привл. товары",
}


def _parse_weekly_value(s: str) -> float:
    """Парсит '12500000', '12 500 000', '12,5 млн', '860 тыс', '500к' → float."""
    s = s.strip().lower().replace('₽', '').replace('руб', '').replace('р.', '').strip()
    s = s.replace(' ', '').replace(' ', '').replace(',', '.')
    m = re.match(r'^([\d.]+)(млн|млрд|тыс|m|k|кк)?$', s)
    if not m:
        raise ValueError(f"некорректное число «{s}»")
    v = float(m.group(1))
    suffix = m.group(2)
    if suffix in ('млн', 'm', 'кк'):
        v *= 1_000_000
    elif suffix == 'млрд':
        v *= 1_000_000_000
    elif suffix in ('тыс', 'k'):
        v *= 1_000
    return v


def _parse_weekly_chunk(chunk: str) -> tuple[str, float]:
    """Парсит 'выручка 12,5 млн' либо '12,5 млн выручка' → (metric_key, value)."""
    tokens = chunk.split()
    if len(tokens) < 2:
        raise ValueError(f"ожидается «метрика значение», получено «{chunk}»")
    metric_idx = None
    for i, tok in enumerate(tokens):
        if tok.lower() in _WEEKLY_METRIC_MAP:
            metric_idx = i
            break
    if metric_idx is None:
        raise ValueError(f"метрика не найдена в «{chunk}»")
    metric_key = _WEEKLY_METRIC_MAP[tokens[metric_idx].lower()]
    value_tokens = tokens[:metric_idx] + tokens[metric_idx + 1:]
    if not value_tokens:
        raise ValueError(f"нет числа в «{chunk}»")
    return metric_key, _parse_weekly_value(' '.join(value_tokens))


def _parse_weekly_bulk(text: str) -> tuple[list, list]:
    """Разбирает многострочный ввод. Возвращает (успехи, ошибки).

    Успехи: [(mgr_name, metric_key, value), ...]
    Ошибки: [(контекст, сообщение), ...]

    Поддерживает:
    - «Имя:» или просто «Имя» в начале строки — старт блока менеджера.
    - В блоке: пары «метрика значение» через запятую, точку с запятой или « — ».
    - Числа: «12500000», «12 500 000», «12,5 млн», «860 тыс», «500к».
    - Склонения: «отгрузок», «клиентов» и т.п.
    - Пустые строки и строки с «#» в начале игнорируются.
    """
    successes: list = []
    errors: list = []
    blocks: list = []
    current_mgr = None
    current_data: list = []

    for raw_line in text.split('\n'):
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        before, sep, after = line.partition(':')
        if sep and _MGR_NAME_MAP.get(before.strip().lower()):
            if current_mgr is not None:
                blocks.append((current_mgr, ' , '.join(current_data)))
            current_mgr = _MGR_NAME_MAP[before.strip().lower()]
            current_data = [after.strip()] if after.strip() else []
            continue
        if _MGR_NAME_MAP.get(line.lower()):
            if current_mgr is not None:
                blocks.append((current_mgr, ' , '.join(current_data)))
            current_mgr = _MGR_NAME_MAP[line.lower()]
            current_data = []
            continue
        if current_mgr is None:
            errors.append((line, "ожидался «Имя:» или строка только с именем менеджера"))
            continue
        current_data.append(line)
    if current_mgr is not None:
        blocks.append((current_mgr, ' , '.join(current_data)))

    for mgr_name, data_str in blocks:
        if not data_str.strip():
            errors.append((mgr_name, "нет данных по менеджеру"))
            continue
        normalized = re.sub(r'\s+[-—–;]\s+', ' , ', data_str)
        normalized = re.sub(r'(\d),(\d)', r'\1.\2', normalized)
        for chunk in normalized.split(','):
            chunk = chunk.strip()
            if not chunk:
                continue
            try:
                metric_key, value = _parse_weekly_chunk(chunk)
                successes.append((mgr_name, metric_key, value))
            except ValueError as e:
                errors.append((f"{mgr_name}: {chunk}", str(e)))

    return successes, errors


async def cmd_set_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_weekly [имя] [показатель] [значение] — задать накопительный недельный план."""
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
        return
    if len(context.args) < 3:
        await update.message.reply_text(
            "Использование: /set_weekly [имя] [показатель] [значение]\n"
            "Показатели: выручка, отгрузки, акб, новые, привл\n"
            "Пример: /set_weekly Инесса выручка 4000000\n\n"
            "Для нескольких значений сразу — /set_weekly_bulk"
        )
        return
    name_part = context.args[0].lower()
    metric_part = context.args[1].lower()
    try:
        value = float(context.args[2].replace(',', '.'))
    except ValueError:
        await update.message.reply_text("❌ Значение должно быть числом")
        return
    mgr_name = _MGR_NAME_MAP.get(name_part)
    if not mgr_name:
        await update.message.reply_text(f"❌ Менеджер '{context.args[0]}' не найден.")
        return
    metric_key = _WEEKLY_METRIC_MAP.get(metric_part)
    if not metric_key:
        await update.message.reply_text(f"❌ Показатель '{context.args[1]}' не найден.\nДоступные: выручка, отгрузки, акб, новые, привл")
        return
    key = f"weekly_target_{mgr_name}_{metric_key}"
    db._execute(
        "INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=%s",
        (key, str(value), str(value))
    )
    label = _WEEKLY_METRIC_LABELS.get(metric_key, metric_key)
    await update.message.reply_text(f"✅ Недельный план *{label}* для *{mgr_name}*: {value:,.0f}", parse_mode="Markdown")


async def cmd_set_weekly_bulk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/set_weekly_bulk — задать накопительные недельные планы списком (многострочно).

    Форматы (можно смешивать):
        /set_weekly_bulk
        Инесса: выручка 12,5 млн, отгрузки 110, акб 26
        Лена: 3 млн выручка, 32 отгрузок, 20 клиентов

    или с именем на отдельной строке:
        /set_weekly_bulk
        Инесса
        12,5 млн выручка — 110 отгрузок — 26 клиентов

    Числа: «12500000», «12 500 000», «12,5 млн», «860 тыс», «500к».
    Метрики: выручка, отгрузки, акб, новые, привл (склонения распознаются).
    """
    if not update.effective_user or update.effective_user.id != OWNER_CHAT_ID:
        return
    raw_text = update.message.text or ''
    lines = raw_text.split('\n')
    first_stripped = re.sub(r'^/\S+\s*', '', lines[0]) if lines else ''
    body = '\n'.join(([first_stripped] if first_stripped.strip() else []) + lines[1:])

    if not body.strip():
        await update.message.reply_text(
            "Использование: пришли команду + список менеджеров (одна строка на менеджера).\n\n"
            "Пример:\n"
            "/set_weekly_bulk\n"
            "Инесса: выручка 12,5 млн, отгрузки 110, акб 26\n"
            "Лена: выручка 3 млн, отгрузки 32, акб 20\n"
            "Карина: выручка 3,8 млн, отгрузки 100, акб 36\n"
            "Ирина: выручка 500 тыс, отгрузки 2, акб 2\n"
            "Денис: выручка 860 тыс, отгрузки 6, акб 6\n\n"
            "Метрики: выручка, отгрузки, акб, новые, привл.\n"
            "Числа: 12500000, 12,5 млн, 860 тыс — все ок."
        )
        return

    successes, errors = _parse_weekly_bulk(body)

    applied: list = []
    for mgr_name, metric_key, value in successes:
        try:
            db._execute(
                "INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=%s",
                (f"weekly_target_{mgr_name}_{metric_key}", str(value), str(value))
            )
            applied.append((mgr_name, metric_key, value))
        except Exception as e:
            errors.append((f"{mgr_name} {metric_key}={value}", f"DB-ошибка: {e}"))

    msg_parts: list = []
    if applied:
        from collections import OrderedDict
        by_mgr: "OrderedDict[str, list]" = OrderedDict()
        for mgr, metric, val in applied:
            by_mgr.setdefault(mgr, []).append((metric, val))
        msg_parts.append(f"✅ Обновлено целей: {len(applied)}")
        for mgr, items in by_mgr.items():
            parts = []
            for metric, val in items:
                label = _WEEKLY_METRIC_LABELS.get(metric, metric)
                num = f"{val:,.0f}".replace(',', ' ')
                parts.append(f"{label} {num}")
            msg_parts.append(f"• {mgr}: {', '.join(parts)}")
    if errors:
        msg_parts.append(f"\n❌ Ошибок: {len(errors)}")
        for ctx_str, err in errors:
            msg_parts.append(f"• {ctx_str} → {err}")
    if not msg_parts:
        msg_parts = ["Нечего обновлять."]
    await update.message.reply_text('\n'.join(msg_parts))




async def cmd_managers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/managers — показать всех зарегистрированных пользователей и их chat_id."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    rows = db._fetchall(
        "SELECT user_id, full_name, is_blocked FROM manager_chats ORDER BY full_name"
    )
    if not rows:
        await update.message.reply_text("❌ Пользователи не найдены.")
        return
    lines = ["👥 *Зарегистрированные пользователи:*\n"]
    for r in rows:
        blocked = " 🚫" if r.get("is_blocked") else ""
        lines.append(f"• {r['full_name']}{blocked}\n  ID: `{r['user_id']}`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_ms_attributes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/ms_attributes — показать UUID дополнительных полей контрагентов."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    import aiohttp
    from moysklad import get_headers
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://api.moysklad.ru/api/remap/1.2/entity/counterparty/metadata/attributes",
            headers=get_headers()
        ) as r:
            data = await r.json()
    rows = data.get("rows", [])
    lines = ["Дополнительные поля контрагентов:\n"]
    for row in rows:
        lines.append(f"{row.get('name')} — {row.get('id')}")
    await update.message.reply_text("\n".join(lines))


async def cmd_del_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/del_user [user_id] — полностью удалить пользователя из базы."""
    user = update.effective_user
    if not user or user.id != OWNER_CHAT_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /del_user [user_id]\nПример: /del_user 1337598287")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ user_id должен быть числом.")
        return
    try:
        row = db._fetchone("SELECT full_name FROM manager_chats WHERE user_id=%s", (target_id,))
        if not row:
            await update.message.reply_text(f"❌ Пользователь {target_id} не найден в базе.")
            return
        name = row["full_name"]
        db._execute("DELETE FROM manager_chats WHERE user_id=%s", (target_id,))
        await update.message.reply_text(f"✅ Удалён: {name} (ID: {target_id})")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


# ============================================================================
# Постановка задач в МойСклад через /задача (whitelist: Виктор + Александр)
# ============================================================================

from zoneinfo import ZoneInfo
_MSK = ZoneInfo("Europe/Moscow")

_TASK_CONV_TIMEOUT_SEC = 600  # 10 минут

# Стадии conversation для /задача
_STAGE_WAITING_DESCRIPTION = "WAITING_DESCRIPTION"       # ждём первое сообщение с задачей
_STAGE_DRAFT = "DRAFT"                                   # показан драфт, ждём ✅/✏️/❌
_STAGE_WAITING_CLARIFICATION = "WAITING_CLARIFICATION"   # ждём уточнения исполнителя
_STAGE_WAITING_REWRITE = "WAITING_REWRITE"               # ждём новое сообщение после ✏️

# In-memory state: {chat_id: {"stage", "draft", "draft_message_id", "started_at"}}
_task_conversations: dict = {}


def _is_task_author(chat_id: int) -> bool:
    """Whitelist для /задача — только Виктор и Александр."""
    return chat_id in {OWNER_CHAT_ID, PARTNER_CHAT_ID}


def _task_actor_name(chat_id: int) -> str:
    """Имя инициатора для логов."""
    if chat_id == OWNER_CHAT_ID:
        return "Виктор"
    if chat_id == PARTNER_CHAT_ID:
        return "Александр"
    return f"chat_id={chat_id}"


def _task_conv_expired(conv: dict) -> bool:
    """True, если с момента started_at прошло больше таймаута."""
    started = conv.get("started_at")
    if not started:
        return True
    return (datetime.now(_MSK) - started).total_seconds() > _TASK_CONV_TIMEOUT_SEC


async def _task_strip_buttons(chat_id: int, message_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Снять inline-кнопки у драфт-сообщения (если ещё живо)."""
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=None,
        )
    except Exception as e:
        logger.debug(f"_task_strip_buttons: {e}")


async def _task_cancel_existing(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Закрыть активную conversation: снять кнопки, очистить state."""
    conv = _task_conversations.pop(chat_id, None)
    if conv:
        msg_id = conv.get("draft_message_id")
        if msg_id:
            await _task_strip_buttons(chat_id, msg_id, context)


async def cmd_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/задача /поставить /task — старт постановки задачи в МойСклад.

    Доступно только Виктору и Александру в личном чате с ботом.
    В группе от whitelist → подсказка «работает только в личке».
    Не-whitelist → тихое игнорирование.
    """
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        return

    # Группа + whitelist → подсказка. Группа + не-whitelist → молчим.
    if chat.type != "private":
        if _is_task_author(user.id):
            await update.message.reply_text(
                "ℹ️ Постановка задач работает только в личном чате со мной."
            )
        return

    # Личка + не-whitelist → молчим.
    if not _is_task_author(user.id):
        return

    # Если уже есть активная conversation у этого пользователя — отменяем старую.
    if user.id in _task_conversations:
        await _task_cancel_existing(user.id, context)
        await update.message.reply_text("⚠️ Предыдущая задача отменена.")

    _task_conversations[user.id] = {
        "stage": _STAGE_WAITING_DESCRIPTION,
        "draft": None,
        "draft_message_id": None,
        "started_at": datetime.now(_MSK),
    }
    logger.info(
        f"metric.task_bot.conv_started chat_id={user.id} actor={_task_actor_name(user.id)}"
    )
    await update.message.reply_text(
        "✍️ Опиши задачу одним сообщением — кому, что сделать, к какому сроку."
    )


async def handle_menu_task_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Кнопка «📋 Поставить задачу» из /menu.

    Эквивалент /задача, но стартует через callback. Доступна только whitelist.
    """
    query = update.callback_query
    user = query.from_user
    if not user:
        return
    # Гейт: только Виктор и Александр
    if not _is_task_author(user.id):
        await query.answer("⛔ Доступно только Виктору и Александру", show_alert=True)
        return
    chat = query.message.chat if query.message else None
    if not chat or chat.type != "private":
        await query.answer("ℹ️ Работает только в личном чате со мной", show_alert=True)
        return
    await query.answer()
    # Если активная conv — закрываем старую, начинаем новую
    if user.id in _task_conversations:
        await _task_cancel_existing(user.id, context)
    _task_conversations[user.id] = {
        "stage": _STAGE_WAITING_DESCRIPTION,
        "draft": None,
        "draft_message_id": None,
        "started_at": datetime.now(_MSK),
    }
    logger.info(
        f"metric.task_bot.conv_started chat_id={user.id} "
        f"actor={_task_actor_name(user.id)} via=menu"
    )
    await context.bot.send_message(
        chat_id=user.id,
        text="✍️ Опиши задачу одним сообщением — кому, что сделать, к какому сроку.",
    )


async def cmd_task_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/отмена — отменить активную постановку задачи."""
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    if not _is_task_author(user.id):
        return
    if user.id not in _task_conversations:
        # Нечего отменять — тихо игнорируем, чтобы не реагировать на /отмена вне контекста.
        return
    await _task_cancel_existing(user.id, context)
    logger.info(f"metric.task_bot.task_cancelled chat_id={user.id}")
    await update.message.reply_text(
        "❌ Отменил. Чтобы начать заново — /задача."
    )


# ─── Рендеринг драфта и клавиатур ───────────────────────────────────────────

def _render_task_draft_text(draft: dict) -> str:
    assignee_name = draft.get("assignee_name") or "—"
    description = draft.get("description") or "—"
    due_str = draft.get("due_msk")
    due_pretty = f"{due_str} МСК" if due_str else "не указан"
    return (
        "📋 Задача в МойСклад:\n"
        f"• Исполнитель: {assignee_name}\n"
        f"• Описание: {description}\n"
        f"• Дедлайн: {due_pretty}"
    )


def _task_draft_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Создать", callback_data="task_confirm"),
        InlineKeyboardButton("✏️ Переписать", callback_data="task_rewrite"),
        InlineKeyboardButton("❌ Отмена", callback_data="task_draft_cancel"),
    ]])


def _task_candidates_keyboard(candidates: list) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(c["name"], callback_data=f"task_cand:{c['id']}")]
            for c in candidates]
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="task_draft_cancel")])
    return InlineKeyboardMarkup(rows)


async def _task_send_draft(chat_id: int, context: ContextTypes.DEFAULT_TYPE, draft: dict) -> None:
    """Отправляет новое сообщение с драфтом + кнопками, обновляет state."""
    text = _render_task_draft_text(draft)
    msg = await context.bot.send_message(
        chat_id=chat_id, text=text, reply_markup=_task_draft_keyboard()
    )
    conv = _task_conversations.get(chat_id)
    if conv is not None:
        conv["stage"] = _STAGE_DRAFT
        conv["draft"] = draft
        conv["draft_message_id"] = msg.message_id
        conv["started_at"] = datetime.now(_MSK)


# ─── Парсинг и маршрутизация драфта ─────────────────────────────────────────

async def _task_parse_and_route(chat_id: int, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    """Парсит текст через Claude, валидирует id, решает: драфт / кандидаты / переспрос."""
    conv = _task_conversations.get(chat_id)
    if not conv:
        return
    try:
        employees = await list_employees()
    except Exception as e:
        logger.error(f"list_employees failed: {e}")
        logger.info(f"metric.task_bot.task_error_ms_employees chat_id={chat_id}")
        await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ МойСклад временно недоступен. /отмена и попробуй позже.",
        )
        return
    valid_ids = {e["id"] for e in employees}
    id_to_name = {e["id"]: e["name"] for e in employees}
    now_msk = datetime.now(_MSK)
    try:
        draft = await parse_task_draft(text, employees, now_msk)
    except Exception as e:
        logger.error(f"parse_task_draft failed: {e}", exc_info=True)
        logger.info(f"metric.task_bot.task_error_claude chat_id={chat_id}")
        await context.bot.send_message(
            chat_id=chat_id, text="⚠️ Не получилось распарсить, попробуй ещё раз."
        )
        return
    # Валидация assignee_id — отсекаем галлюцинации Claude
    aid = draft.get("assignee_id")
    if aid and aid not in valid_ids:
        logger.warning(f"parse_task_draft: assignee_id={aid} не в списке — отбрасываем")
        aid = None
    cands = [c for c in (draft.get("assignee_candidates") or []) if c.get("id") in valid_ids]
    desc = (draft.get("description") or "").strip()
    if len(desc) > 200:
        desc = desc[:199] + "…"

    draft = {
        "assignee_id": aid,
        "assignee_name": id_to_name.get(aid) if aid else None,
        "assignee_candidates": cands if cands else None,
        "description": desc,
        "due_msk": draft.get("due_msk"),
    }

    # Есть однозначный исполнитель → сразу драфт
    if aid:
        await _task_send_draft(chat_id, context, draft)
        return
    # 2–5 кандидатов → кнопки выбора
    if cands and 2 <= len(cands) <= 5:
        conv["stage"] = _STAGE_WAITING_CLARIFICATION
        conv["draft"] = draft
        conv["draft_message_id"] = None
        conv["started_at"] = datetime.now(_MSK)
        await context.bot.send_message(
            chat_id=chat_id,
            text="🤔 Кого из них ты имел в виду?",
            reply_markup=_task_candidates_keyboard(cands),
        )
        return
    # 0 или >5 кандидатов → переспрос фамилией
    conv["stage"] = _STAGE_WAITING_CLARIFICATION
    conv["draft"] = draft
    conv["draft_message_id"] = None
    conv["started_at"] = datetime.now(_MSK)
    await context.bot.send_message(
        chat_id=chat_id, text="🤔 Кому ставим? Напиши фамилию."
    )


# ─── MessageHandler для текста в активной conversation ──────────────────────

class _TaskConvActiveFilter(filters.MessageFilter):
    """Срабатывает только для whitelist-пользователей в личке с активной conv."""
    def filter(self, message):
        user = getattr(message, "from_user", None)
        chat = getattr(message, "chat", None)
        if not user or not chat or chat.type != "private":
            return False
        active = user.id in _task_conversations
        # Диагностика: логируем попытку для whitelist-юзеров, чтобы увидеть,
        # почему filter не совпадает, если пользователь в conv ожидает.
        if _is_task_author(user.id):
            logger.info(
                f"metric.task_bot.filter_check user_id={user.id} "
                f"active={active} conv_keys={list(_task_conversations.keys())} "
                f"text={(message.text or '')[:60]!r}"
            )
        return active


_task_conv_active_filter = _TaskConvActiveFilter()


async def handle_task_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Текстовое сообщение пользователя в активной /задача-conversation."""
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    logger.info(
        f"metric.task_bot.message_received user_id={user.id} "
        f"in_conv={user.id in _task_conversations} "
        f"stage={_task_conversations.get(user.id, {}).get('stage')}"
    )
    if user.id not in _task_conversations:
        return
    conv = _task_conversations[user.id]
    if _task_conv_expired(conv):
        msg_id = conv.get("draft_message_id")
        if msg_id:
            await _task_strip_buttons(user.id, msg_id, context)
        _task_conversations.pop(user.id, None)
        logger.info(f"metric.task_bot.task_timeout chat_id={user.id}")
        await update.message.reply_text("⏳ Сессия устарела, нажми /задача заново.")
        return

    text = (update.message.text or "").strip()
    if not text:
        return
    stage = conv["stage"]
    if stage == _STAGE_WAITING_DESCRIPTION:
        await _task_parse_and_route(user.id, context, text)
    elif stage == _STAGE_WAITING_CLARIFICATION:
        # Склеиваем старое описание с уточнением-фамилией, чтобы парсер увидел оба факта.
        old_desc = (conv.get("draft") or {}).get("description") or ""
        combined = f"{text}, {old_desc}" if old_desc else text
        await _task_parse_and_route(user.id, context, combined)
    elif stage == _STAGE_WAITING_REWRITE:
        await _task_parse_and_route(user.id, context, text)
    # DRAFT — пользователь пишет текст, не нажимая кнопку. Игнорируем (ждём кнопку или /отмена).


# ─── Callback кнопок драфта и кандидатов ────────────────────────────────────

# Идемпотентность ✅ Создать: набор draft_message_id, по которым уже создаём задачу.
_task_creating: set = set()


async def handle_task_draft_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """✅ Создать / ✏️ Переписать / ❌ Отмена."""
    query = update.callback_query
    user = query.from_user
    if not _is_task_author(user.id):
        await query.answer("Недоступно", show_alert=False)
        return
    conv = _task_conversations.get(user.id)
    if not conv or _task_conv_expired(conv):
        await query.answer("⏳ Сессия устарела, нажми /задача заново.", show_alert=True)
        if conv:
            await _task_cancel_existing(user.id, context)
        return
    data = query.data
    if data == "task_draft_cancel":
        await query.answer("Отменил")
        await _task_cancel_existing(user.id, context)
        logger.info(f"metric.task_bot.task_cancelled chat_id={user.id}")
        await context.bot.send_message(
            chat_id=user.id,
            text="❌ Отменил. Чтобы начать заново — /задача.",
        )
        return
    if data == "task_rewrite":
        await query.answer()
        await _task_strip_buttons(user.id, query.message.message_id, context)
        conv["stage"] = _STAGE_WAITING_REWRITE
        conv["draft_message_id"] = None
        conv["started_at"] = datetime.now(_MSK)
        await context.bot.send_message(
            chat_id=user.id, text="✍️ Опиши задачу заново одним сообщением."
        )
        return
    if data == "task_confirm":
        await _task_handle_confirm(update, context)
        return


async def handle_task_candidate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор кандидата исполнителя из inline-кнопок."""
    query = update.callback_query
    user = query.from_user
    if not _is_task_author(user.id):
        await query.answer("Недоступно", show_alert=False)
        return
    conv = _task_conversations.get(user.id)
    if not conv or _task_conv_expired(conv):
        await query.answer("⏳ Сессия устарела, нажми /задача заново.", show_alert=True)
        if conv:
            await _task_cancel_existing(user.id, context)
        return
    _, _, emp_id = query.data.partition(":")
    try:
        employees = await list_employees()
    except Exception as e:
        logger.error(f"list_employees failed: {e}")
        await query.answer("⚠️ МойСклад временно недоступен", show_alert=True)
        return
    id_to_name = {e["id"]: e["name"] for e in employees}
    if emp_id not in id_to_name:
        await query.answer("Сотрудник не найден, начни заново /задача", show_alert=True)
        await _task_cancel_existing(user.id, context)
        return
    draft = dict(conv.get("draft") or {})
    draft["assignee_id"] = emp_id
    draft["assignee_name"] = id_to_name[emp_id]
    draft["assignee_candidates"] = None
    await _task_strip_buttons(user.id, query.message.message_id, context)
    await query.answer()
    await _task_send_draft(user.id, context, draft)


async def _task_handle_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """✅ Создать: создаём задачу в МС с идемпотентностью и обработкой ошибок."""
    import aiohttp as _aiohttp
    query = update.callback_query
    user_id = query.from_user.id
    conv = _task_conversations.get(user_id)
    if not conv:
        await query.answer("⏳ Сессия устарела, нажми /задача заново.", show_alert=True)
        return
    msg_id = query.message.message_id

    # Идемпотентность: второй клик по той же кнопке — отвечаем «уже создаю».
    if msg_id in _task_creating:
        await query.answer("Уже создаю…")
        return
    _task_creating.add(msg_id)

    try:
        await query.answer("Создаю…")
        await _task_strip_buttons(user_id, msg_id, context)

        draft = conv.get("draft") or {}
        aid = draft.get("assignee_id")
        desc = draft.get("description") or ""
        due_str = draft.get("due_msk")
        due_dt = None
        if due_str:
            try:
                due_dt = datetime.strptime(due_str, "%Y-%m-%d %H:%M")
            except ValueError:
                logger.warning(f"task_confirm: не распарсил due_msk='{due_str}', создаю без дедлайна")

        async def _call_create():
            return await create_task(aid, desc, due_dt)

        try:
            result = await _call_create()
        except _aiohttp.ClientResponseError as e:
            status = e.status
            logger.warning(f"create_task HTTP {status}: {e}")
            logger.info(f"metric.task_bot.task_error_{status} chat_id={user_id}")
            if status == 404:
                invalidate_employees_cache()
                try:
                    result = await _call_create()
                except Exception as e2:
                    logger.warning(f"create_task retry after 404 failed: {e2}")
                    await context.bot.send_message(
                        chat_id=user_id,
                        text="⚠️ Сотрудник не найден, возможно уволен. Выбери через ✏️ Переписать или ❌ Отмена.",
                    )
                    await _task_send_draft(user_id, context, draft)
                    return
            elif status in (401, 403):
                await context.bot.send_message(
                    chat_id=user_id,
                    text="⚠️ У «Эф» недостаточно прав в МойСкладе. Сообщи Виктору.",
                )
                await _task_send_draft(user_id, context, draft)
                return
            elif status >= 500:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="⚠️ МойСклад временно недоступен. Жми ✅ ещё раз или ❌ Отмена.",
                )
                await _task_send_draft(user_id, context, draft)
                return
            else:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"⚠️ Ошибка МС ({status}). Жми ✅ ещё раз или ❌ Отмена.",
                )
                await _task_send_draft(user_id, context, draft)
                return
        except Exception as e:
            logger.exception(f"create_task unexpected error: {e}")
            logger.info(f"metric.task_bot.task_error_exception chat_id={user_id}")
            await context.bot.send_message(
                chat_id=user_id,
                text="⚠️ Не удалось создать задачу. Жми ✅ ещё раз или ❌ Отмена.",
            )
            await _task_send_draft(user_id, context, draft)
            return

        # Успех
        task_id = result["id"]
        task_url = result["url"]
        assignee_name = draft.get("assignee_name") or "—"
        due_pretty = f"{due_str} МСК" if due_str else "не указан"
        logger.info(f"metric.task_bot.task_created chat_id={user_id} task_id={task_id}")
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                "✅ Задача создана\n"
                f"Исполнитель: {assignee_name}\n"
                f"Дедлайн: {due_pretty}\n"
                f"→ [Открыть в МС]({task_url})"
            ),
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
        _task_conversations.pop(user_id, None)
    finally:
        _task_creating.discard(msg_id)


# ─── Telegram Business: приём BusinessConnection (для TG Stories Publisher) ───
async def handle_business_connection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Ловит подключение/отключение бота в Telegram Business → Chatbots.
    Сохраняет business_connection_id в bot_settings и шлёт алерт собственнику.
    План: plans/2026-05-14-tg-stories-publisher-mvp.md (Фаза 0/1).
    """
    bc = update.business_connection
    if bc is None:
        return

    rights_repr = None
    try:
        if getattr(bc, "rights", None) is not None:
            rights_repr = str(bc.rights.to_dict()) if hasattr(bc.rights, "to_dict") else str(bc.rights)
    except Exception:
        rights_repr = "<unparseable>"

    payload = {
        "id": bc.id,
        "user_chat_id": bc.user_chat_id,
        "user_id": bc.user.id if bc.user else None,
        "is_enabled": bc.is_enabled,
        "rights": rights_repr,
        "date": bc.date.isoformat() if bc.date else None,
    }
    logger.info(f"📣 BusinessConnection update: {payload}")

    try:
        db._execute(
            """INSERT INTO bot_settings (key, value) VALUES (%s, %s)
               ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""",
            ("business_connection_id_ftb_mob", bc.id if bc.is_enabled else "")
        )
        db._execute(
            """INSERT INTO bot_settings (key, value) VALUES (%s, %s)
               ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""",
            ("business_connection_payload_ftb_mob", str(payload))
        )
    except Exception as e:
        logger.error(f"Не удалось сохранить business_connection в bot_settings: {e}")

    try:
        status_emoji = "🟢" if bc.is_enabled else "🔴"
        text = (
            f"{status_emoji} Business Connection {'подключён' if bc.is_enabled else 'отключён'}\n"
            f"business_connection_id: {bc.id}\n"
            f"От user: {payload['user_id']}\n"
            f"Права: {rights_repr}"
        )
        await context.bot.send_message(chat_id=OWNER_CHAT_ID, text=text)
    except Exception as e:
        logger.error(f"Не удалось отправить алерт собственнику: {e}")


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

    # Telegram Business: подключение бота к бизнес-аккаунту (Stories Publisher)
    app.add_handler(BusinessConnectionHandler(handle_business_connection))

    # ─── Канал «Мониторинг» — закупочные прайсы поставщиков ─────────────────
    # Регистрируем ДО любых других handler'ов на channel_post, чтобы whitelist
    # по chat_id отработал первым. В функции стоит ранний return, если чат не наш.
    app.add_handler(MessageHandler(
        filters.UpdateType.CHANNEL_POST | filters.UpdateType.EDITED_CHANNEL_POST,
        handle_market_intel_post,
    ))

    # Команды
    app.add_handler(CommandHandler("op_report", cmd_op_report))
    app.add_handler(CommandHandler("test_group", cmd_test_group))
    app.add_handler(CommandHandler("test_fact", cmd_test_fact))
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
    app.add_handler(CommandHandler("managers", cmd_managers))
    app.add_handler(CommandHandler("search_msg", cmd_search_msg))
    app.add_handler(CommandHandler("aging", cmd_aging))
    app.add_handler(CommandHandler("block", cmd_block_user))
    app.add_handler(CommandHandler("unblock", cmd_unblock_user))
    app.add_handler(CommandHandler("del_user", cmd_del_user))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("usermenu", cmd_user_menu))
    app.add_handler(CallbackQueryHandler(handle_user_menu_callback, pattern="^user_"))
    app.add_handler(CommandHandler("menu", cmd_menu))
    # Кнопка «📋 Поставить задачу» в /menu — whitelist-гейт внутри; регистрируется ДО общего handle_menu_callback
    app.add_handler(CallbackQueryHandler(handle_menu_task_button, pattern=r"^menu_task$"))
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
    app.add_handler(CommandHandler("test", cmd_test))
    app.add_handler(MessageHandler(filters.StatusUpdate.MESSAGE_AUTO_DELETE_TIMER_CHANGED, handle_message))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("photo", cmd_photo))
    app.add_handler(CommandHandler("price", cmd_price))
    app.add_handler(CommandHandler("contact", cmd_contact))
    app.add_handler(CommandHandler("memory", cmd_memory))
    app.add_handler(CommandHandler("remember", cmd_remember))
    app.add_handler(CommandHandler("add_webhook", cmd_add_webhook))
    # ── Заглушки для старых ПДЗ-команд (Фаза 1: чистка старой механики) ──
    # Команды отключены, новая механика дебиторки описывается в группе ОП.
    app.add_handler(CommandHandler("pdz", cmd_pdz_disabled))
    app.add_handler(CommandHandler("pdz_results", cmd_pdz_disabled))
    app.add_handler(CommandHandler("pdz_test", cmd_pdz_disabled))
    app.add_handler(CommandHandler("pdz_evening", cmd_pdz_disabled))
    app.add_handler(CommandHandler("debtors", cmd_pdz_disabled))
    app.add_handler(CommandHandler("debts", cmd_pdz_disabled))
    app.add_handler(MessageHandler(
        filters.Regex(r"^/дебиторка(@\w+)?(\s|$)"),
        cmd_pdz_disabled,
    ))
    # Диагностика Фазы 2 ПДЗ-автоматики (только собственник).
    app.add_handler(CommandHandler("pdz_snapshot_test", cmd_pdz_snapshot_test))
    app.add_handler(CommandHandler("pdz_token_check", cmd_pdz_token_check))
    # Диагностика Фазы 3 (только собственник).
    app.add_handler(CommandHandler("pdz_events_test", cmd_pdz_events_test))
    app.add_handler(CommandHandler("pdz_overdue_test", cmd_pdz_overdue_test))
    # Диагностика Фазы 4: TG-дайджесты менеджерам + пинг собственнику.
    app.add_handler(CommandHandler("pdz_send_digests_test", cmd_pdz_send_digests_test))
    app.add_handler(CommandHandler("pdz_send_digests_test_now", cmd_pdz_send_digests_test_now))
    app.add_handler(CommandHandler("pdz_send_owner_pending_test", cmd_pdz_send_owner_pending_test))
    # Фаза 4.5: топ-30 клиентов по срывам обещаний за 90 дней (только собственник).
    app.add_handler(CommandHandler("pdz_breaks", cmd_pdz_breaks))
    # Фаза 5: HTML-отчёт «Дебиторка» — ручной запуск регенерации (только собственник).
    app.add_handler(CommandHandler("pdz_html", cmd_pdz_html))
    app.add_handler(CommandHandler("set_attestation", cmd_set_attestation))
    app.add_handler(CommandHandler("set_weekly", cmd_set_weekly))
    app.add_handler(CommandHandler("set_weekly_bulk", cmd_set_weekly_bulk))
    app.add_handler(CommandHandler("reset_agreed", cmd_reset_agreed))
    app.add_handler(CommandHandler("ms_attributes", cmd_ms_attributes))
    app.add_handler(CommandHandler("notifier_status", cmd_notifier_status))
    app.add_handler(CallbackQueryHandler(handle_contract_callback, pattern="^contract_"))
    app.add_handler(CallbackQueryHandler(handle_price_callback, pattern="^(price_|pdz_)"))
    app.add_handler(CallbackQueryHandler(handle_approval_callback, pattern="^appr_"))
    app.add_handler(CallbackQueryHandler(handle_send_callback, pattern="^send_"))
    app.add_handler(CallbackQueryHandler(handle_wazzup_link_callback, pattern="^(wazzup_link|wazzup_role|wazzup_pick|wazzup_seg|wazzup_mgr|wazzup_mailing|wazzup_later)"))
    app.add_handler(CallbackQueryHandler(handle_wazzup_ignore_callback, pattern="^wazzup_ignore"))
    # ─── Алармы amoCRM ───────────────────────────────────────────────────────
    app.add_handler(CommandHandler("myamoid", lambda u, c: cmd_myamoid(u, c, db)))
    app.add_handler(CommandHandler("amo_setup", cmd_amo_setup))
    app.add_handler(CallbackQueryHandler(
        lambda u, c: handle_take_callback(u.callback_query, db),
        pattern="^amo_alarm_take\\|"
    ))
    app.add_handler(CallbackQueryHandler(
        lambda u, c: handle_amo_link_callback(u.callback_query, db),
        pattern="^amo_link\\|"
    ))
    # ─── Постановка задач в МойСклад (/задача) ───────────────────────────────
    # PTB не принимает кириллицу в CommandHandler (ValueError: not a valid bot command),
    # поэтому маршрутизируем /задача /поставить /отмена через MessageHandler + Regex.
    # /task — ASCII alias, можно зарегистрировать BotFather'ом в меню команд.
    app.add_handler(CommandHandler("task", cmd_task))
    app.add_handler(MessageHandler(
        filters.Regex(r"^/(задача|поставить)(@\w+)?(\s|$)"),
        cmd_task,
    ))
    app.add_handler(MessageHandler(
        filters.Regex(r"^/отмена(@\w+)?(\s|$)"),
        cmd_task_cancel,
    ))
    # Текст в активной /задача-conversation (только для whitelist в личке) — ДО общего handle_message
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & _task_conv_active_filter,
        handle_task_message,
    ))
    app.add_handler(CallbackQueryHandler(
        handle_task_draft_callback,
        pattern=r"^task_(confirm|rewrite|draft_cancel)$",
    ))
    app.add_handler(CallbackQueryHandler(
        handle_task_candidate_callback,
        pattern=r"^task_cand:",
    ))

    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POSTS, handle_channel_post))
    app.add_handler(MessageHandler(filters.ALL & ~filters.UpdateType.CHANNEL_POSTS, handle_message))

    # Планировщик
    setup_scheduler(app, db)

    async def retry_pending_idents(context):
        """Каждый час проверяем отложенные идентификации и повторяем запрос."""
        try:
            rows = db.get_retry_idents()
            if not rows:
                return
            group_chat_id = int(os.getenv("WAZZUP_ID_CHAT_ID", "0"))
            if not group_chat_id:
                return
            import uuid as _uuid3
            for row in rows:
                old_link_key = row["link_key"]
                chat_id_val = row["chat_id"]
                channel_id_val = row.get("channel_id", "")
                wazzup_name = row.get("wazzup_name", "")
                chat_type = row.get("chat_type", "telegram")

                # Создаём новый link_key
                link_key = str(_uuid3.uuid4())[:8]
                _pending_links[link_key] = {
                    "chat_id": chat_id_val,
                    "channel_id": channel_id_val,
                    "wazzup_name": wazzup_name,
                    "chat_type": chat_type,
                }
                db.save_pending_link(link_key, chat_id_val, channel_id_val, wazzup_name, chat_type)
                db.delete_pending_ident(old_link_key)

                CHANNEL_NAMES = {"telegram": "Telegram", "tgapi": "Telegram", "max": "Max", "whatsapp": "WhatsApp"}
                channel_label = CHANNEL_NAMES.get(chat_type, chat_type)
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("✅ Для рассылок", callback_data=f"wazzup_mailing|{link_key}"),
                        InlineKeyboardButton("👤 Просто контакт", callback_data=f"wazzup_link|{link_key}"),
                    ],
                    [
                        InlineKeyboardButton("⏰ Привязать позже", callback_data=f"wazzup_later|{link_key}"),
                    ]
                ])
                await context.bot.send_message(
                    chat_id=group_chat_id,
                    text=(
                        f"🔔 *Напоминание — {channel_label}*\n\n"
                        f"👤 *{wazzup_name}* ещё не привязан к компании.\n\n"
                        f"Этот контакт для рассылок при согласовании заказов?"
                    ),
                    parse_mode="Markdown",
                    reply_markup=keyboard,
                )
                logger.info(f"retry_pending_idents: повторный запрос для {wazzup_name} ({chat_id_val})")
        except Exception as e:
            logger.error(f"retry_pending_idents: {e}", exc_info=True)

    app.job_queue.run_repeating(retry_pending_idents, interval=3600, first=300)

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
                        # Проверяем есть ли уже chat_id в МойСклад — если да, идентификация не нужна
                        try:
                            from moysklad import get_headers
                            import aiohttp as _aiohttp
                            _attr_id = MS_ATTR_BY_TYPE.get(chat_type.lower(), "")
                            if _attr_id:
                                _ms_url = f"https://api.moysklad.ru/api/remap/1.2/entity/counterparty"
                                async with _aiohttp.ClientSession() as _s:
                                    async with _s.get(
                                        _ms_url,
                                        headers=get_headers(),
                                        params={"filter": f"attributes.{_attr_id}={chat_id_val}", "limit": 1}
                                    ) as _r:
                                        if _r.status == 200:
                                            _data = await _r.json()
                                            if _data.get("rows"):
                                                _cp_name = _data["rows"][0].get("name", "")
                                                logger.info(f"Wazzup: {chat_id_val} уже в МойСклад ({_cp_name}), идентификация не нужна")
                                                db.link_wazzup_contact(
                                                    chat_id=chat_id_val,
                                                    chat_type=chat_type,
                                                    channel_id=channel_id_val,
                                                    company_name=_cp_name,
                                                    wazzup_name=contact_name,
                                                    role="рассылка",
                                                )
                                                continue
                        except Exception as _e:
                            logger.warning(f"Wazzup: проверка МойСклад не удалась: {_e}")
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
                                keyboard = InlineKeyboardMarkup([
                                    [
                                        InlineKeyboardButton("✅ Для рассылок", callback_data=f"wazzup_mailing|{link_key}"),
                                        InlineKeyboardButton("👤 Просто контакт", callback_data=f"wazzup_link|{link_key}"),
                                    ],
                                    [
                                        InlineKeyboardButton("⏰ Привязать позже", callback_data=f"wazzup_later|{link_key}"),
                                    ]
                                ])
                                preview = (text or "").replace("\n", " ").strip()
                                if len(preview) > 120:
                                    preview = preview[:120] + "..."
                                CHANNEL_NAMES = {"telegram": "Telegram", "tgapi": "Telegram", "max": "Max", "whatsapp": "WhatsApp"}
                                channel_label = CHANNEL_NAMES.get(chat_type, chat_type)
                                await app.bot.send_message(
                                    chat_id=group_chat_id,
                                    text=(
                                        f"📩 *Новый контакт — {channel_label}*\n\n"
                                        f"👤 Имя: *{contact_name}*\n"
                                        f"💬 _{preview}_\n\n"
                                        f"Этот контакт для рассылок при согласовании заказов?"
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

            # Снимок «% работы на новых» (op_new_share) читается ОТДЕЛЬНО,
            # вне report_cache (TTL 5 ч). План: 2026-05-21-виджет-..., Фаза 3.
            # Пишется cron-job пятница 08:00 МСК + CLI --backfill.
            report_data["new_share"] = db.get_new_share_snapshot()

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

    async def handle_pdz_html(request):
        """HTML-отчёт «Дебиторка» (Фаза 5 плана 2026-05-20-пдз-автоматика).

        Доступ — только по токену из `report_links` с mgr_filter='pdz'.
        Отдаёт закэшированный HTML (cron 14:15 МСК пишет в bot_settings.pdz_html_cache).
        Если кэш пуст — рендерит на лету из последнего snapshot + promise_log.
        НЕ дёргает МойСклад API.
        """
        token = request.query.get("token", "")
        link = db.get_report_link(token)
        if not link or (link.get("mgr_filter") or "") != "pdz":
            return web.Response(
                text="<html><body style='font-family:sans-serif;padding:40px;color:#6b7280'>"
                     "<h2>Ссылка истекла или недействительна</h2>"
                     "<p>Запроси новую через /pdz_html в боте.</p></body></html>",
                content_type="text/html", status=403, charset="utf-8",
            )
        try:
            cached = db.get_pdz_html_cache()
            if cached:
                html_text = cached
                logger.info("handle_pdz_html: отдаём из кэша")
            else:
                from pdz_report_html import render_pdz_html_from_db
                html_text = render_pdz_html_from_db(db)
                try:
                    db.set_pdz_html_cache(html_text)
                except Exception as e:
                    logger.warning(f"handle_pdz_html: set_pdz_html_cache: {e}")
            return web.Response(text=html_text, content_type="text/html", charset="utf-8")
        except Exception as e:
            logger.error(f"handle_pdz_html: {e}", exc_info=True)
            return web.Response(text=f"Ошибка: {e}", status=500, charset="utf-8")

    async def run_web():
        web_app = web.Application()
        web_app.router.add_post("/webhook/moysklad", handle_ms_webhook)
        web_app.router.add_post("/webhook/wazzup", handle_wazzup_webhook)
        web_app.router.add_get("/webhook/sipuni", handle_sipuni_webhook)
        web_app.router.add_post("/webhook/sipuni", handle_sipuni_webhook)
        web_app.router.add_get("/health", handle_health)
        web_app.router.add_get("/report", handle_web_report)
        web_app.router.add_get("/pdz", handle_pdz_html)
        # ─── amoCRM алармы ───────────────────────────────────────────────────
        async def handle_amo_webhook_route(request):
            await handle_amo_webhook(request, app, db)
            return web.Response(text="ok")
        web_app.router.add_post("/webhook/amocrm", handle_amo_webhook_route)

        # ─── Market Intel: эндпоинты для скилла update-market-intel ──────────
        market_intel_token = os.getenv("MARKET_INTEL_TOKEN", "")

        def _check_market_intel_auth(request) -> bool:
            if not market_intel_token:
                return False
            auth = request.headers.get("Authorization", "")
            return auth == f"Bearer {market_intel_token}"

        async def handle_market_intel_file(request):
            if not _check_market_intel_auth(request):
                return web.Response(text="forbidden", status=403)
            try:
                msg_id = int(request.match_info["id"])
            except (KeyError, ValueError):
                return web.Response(text="bad id", status=400)
            row = db.get_market_intel_message(msg_id)
            if not row or not row.get("file_path"):
                return web.Response(text="not found", status=404)
            path = row["file_path"]
            if not os.path.exists(path):
                return web.Response(text="file missing on disk", status=410)
            return web.FileResponse(path)

        async def handle_market_intel_processed(request):
            if not _check_market_intel_auth(request):
                return web.Response(text="forbidden", status=403)
            try:
                data = await request.json()
                msg_id = int(data["id"])
            except Exception:
                return web.Response(text="bad body", status=400)
            db.mark_market_intel_processed(msg_id)
            return web.json_response({"ok": True, "id": msg_id})

        async def handle_market_intel_unprocessed(request):
            """Возвращает список необработанных сообщений из канала «Мониторинг»
            для скилла update-market-intel и cloud-routine."""
            if not _check_market_intel_auth(request):
                return web.Response(text="forbidden", status=403)
            try:
                limit = int(request.query.get("limit", "100"))
            except ValueError:
                limit = 100
            rows = db.get_unprocessed_market_intel(limit=limit)
            # сериализуем datetime в isoformat для JSON
            for r in rows:
                for k in ("posted_at", "created_at"):
                    v = r.get(k)
                    if v is not None and hasattr(v, "isoformat"):
                        r[k] = v.isoformat()
            counts = db.get_market_intel_count()
            return web.json_response({
                "ok": True,
                "limit": limit,
                "returned": len(rows),
                "total": counts.get("total", 0),
                "unprocessed_total": counts.get("unprocessed", 0),
                "messages": rows,
            })

        async def handle_market_intel_alert(request):
            """Принимает {"text": "..."} и шлёт его в OWNER_CHAT_ID (Виктор)
            от лица бота «Эф» — для алертов «кандидат на замену сырья»."""
            if not _check_market_intel_auth(request):
                return web.Response(text="forbidden", status=403)
            try:
                data = await request.json()
                text = (data.get("text") or "").strip()
            except Exception:
                return web.Response(text="bad body", status=400)
            if not text:
                return web.Response(text="empty text", status=400)
            try:
                sent = await app.bot.send_message(
                    chat_id=OWNER_CHAT_ID,
                    text=text,
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
            except Exception as e:
                logger.error(f"market_intel_alert send failed: {e}")
                # fallback без Markdown — в случае ошибки парсинга
                try:
                    sent = await app.bot.send_message(
                        chat_id=OWNER_CHAT_ID,
                        text=text,
                        disable_web_page_preview=True,
                    )
                except Exception as e2:
                    return web.json_response({"ok": False, "error": str(e2)}, status=500)
            return web.json_response({"ok": True, "message_id": sent.message_id})

        web_app.router.add_get("/market-intel/files/{id}", handle_market_intel_file)
        web_app.router.add_get("/market-intel/unprocessed", handle_market_intel_unprocessed)
        web_app.router.add_post("/market-intel/processed", handle_market_intel_processed)
        web_app.router.add_post("/market-intel/alert", handle_market_intel_alert)

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
            allowed_updates=["message", "channel_post", "edited_message", "edited_channel_post", "callback_query", "business_connection"]
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

        text = (
            f"🔴 *Новый заказ от клиента с просрочкой!*\n\n"
            f"*{agent_name}* | Заказ *{order_name}*\n"
            f"Менеджер: {manager_name}\n\n"
            f"Просрочка: *{debt_days} дней* | Сумма: *{debt_amount:,.0f} руб*"
        )
        await bot.send_message(
            chat_id=group_chat_id,
            text=text,
            parse_mode="Markdown",
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

            # Смена статуса на "Согласовано" (UPDATE) или создание сразу
            # в этом статусе (CREATE — например, через API/импорт/Salesbot)
            # → отправляем клиенту. Notifier проверяет state внутри и делает
            # атомарный claim через agreed_notifications.
            if action in ("UPDATE", "CREATE"):
                await check_order_agreed(order_href, bot, db)

            # Объединённый алерт «На согласовании» / «ЗА ЛИМИТОМ» — собственнику
            # (план 2026-05-21, Фаза 3). Notifier сам проверяет state и делает
            # атомарный дедуп через pending_approval_alerts (UNIQUE order_id+sum_hash).
            if action in ("UPDATE", "CREATE"):
                from notifier import check_approval_needed
                await check_approval_needed(order_href, bot, db)

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
                    chat_id=OWNER_CHAT_ID,
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
