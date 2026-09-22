"""Ценовое согласование из дашборда менеджера: кнопки светофора «ЗАПРОС ЦЕНЫ».

План: «F2B второй мозг»/plans/2026-09-22-ценовое-согласование-вкладка-и-светофор.md.

Запрос заводит менеджер во вкладке «Ценовое согласование» (quiz-game, fishki.f2b.group),
там же считается себестоимость и рекомендуемая цена, и собственнику в личку уходит
светофор с кнопками. Бот ловит кнопки (общая таблица price_requests):
  • «✅ Согласовано N» – согласована цена запроса менеджера;
  • «✏️ Другая цена» – бот ждёт от собственника число и согласует его.
После решения менеджеру уходит пуш, в дашборде статус меняется сам.
Согласованная цена действует на один заказ в течение 14 дней (решение собственника 22.09.2026).
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from database import CONNECT_TIMEOUT_SEC, STATEMENT_TIMEOUT_MS

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))
VALID_DAYS = 14
AWAIT_MINUTES = 60          # «Другая цена» ждёт ввода не дольше часа
_PRICE_RE = re.compile(r"^\s*(\d[\d\s]{0,8}(?:[.,]\d{1,2})?)\s*(?:₽|р\.?|руб\.?)?\s*$", re.IGNORECASE)

_conn = None


def _db():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(os.environ["DATABASE_URL"],
                                 cursor_factory=psycopg2.extras.RealDictCursor,
                                 connect_timeout=CONNECT_TIMEOUT_SEC,
                                 options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS}")
        _conn.autocommit = True
    return _conn


def _one(sql: str, params=()) -> dict | None:
    try:
        with _db().cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone() if cur.description else None
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        global _conn
        _conn = None
        with _db().cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone() if cur.description else None


def _owner_id() -> int:
    v = (os.getenv("OWNER_CHAT_ID") or "").strip()
    return int(v) if v.lstrip("-").isdigit() else 0


def _rub(x) -> str:
    return f"{float(x):,.0f}".replace(",", " ") if x is not None else "—"


def _markup(row) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(f"✅ Согласовано {_rub(row['price_requested'])}", callback_data=f"preq_ok:{row['id']}"),
        InlineKeyboardButton("✏️ Другая цена", callback_data=f"preq_other:{row['id']}"),
    ]])


def _manager_chat(tag: str | None) -> int | None:
    from moysklad import PDZ_MANAGER_TG_IDS
    return PDZ_MANAGER_TG_IDS.get((tag or "").lower())


def _sku(row) -> str:
    return f"{row['sku_code']} {row.get('sku_name') or ''}".strip()


async def _drop_buttons(bot, row) -> None:
    msgs = row.get("tg_messages") or []
    if isinstance(msgs, str):
        import json
        try:
            msgs = json.loads(msgs)
        except Exception:
            msgs = []
    for m in msgs:
        try:
            await bot.edit_message_reply_markup(chat_id=m["chat_id"], message_id=m["message_id"], reply_markup=None)
        except Exception:
            pass


async def _approve(bot, req_id: int, price: float, from_status: tuple[str, ...]) -> dict | None:
    """Согласовать цену: статус, срок, снять кнопки, пуш менеджеру. None – уже обработано."""
    valid_until = datetime.now(MSK).date() + timedelta(days=VALID_DAYS)
    row = _one(
        """UPDATE price_requests SET status='approved', approved_price=%s, decided_by='Виктор',
                  decided_at=NOW(), valid_until=%s, awaiting_since=NULL
           WHERE id=%s AND status = ANY(%s) RETURNING *""",
        (price, valid_until, req_id, list(from_status)))
    if not row:
        return None
    await _drop_buttons(bot, row)
    chat = _manager_chat(row["manager_tag"])
    if chat:
        asked = float(row["price_requested"])
        other = f" (вы просили {_rub(asked)})" if abs(float(price) - asked) >= 0.5 else ""
        qty = f", {_rub(row['qty_kg'])} кг" if row.get("qty_kg") else ""
        text = (f"✅ Цена согласована – запрос №{row['id']}\n\n"
                f"Клиент: {row['client_name']}\n"
                f"Позиция: {_sku(row)}{qty}\n"
                f"Цена: {_rub(price)} ₽/кг{other}\n\n"
                f"Действует до {valid_until:%d.%m} на один заказ. Оформите заказ в МойСкладе – "
                f"при согласовании он пройдёт с пометкой «согласовано в дашборде».")
        try:
            await bot.send_message(chat_id=chat, text=text)
        except Exception as e:
            logger.warning("price request %s: не уведомил менеджера %s: %r", req_id, row["manager_tag"], e)
    else:
        logger.warning("price request %s: chat_id менеджера '%s' не найден – пуш пропущен",
                       req_id, row["manager_tag"])
    return row


async def handle_price_request_callback(update, context):
    """Кнопки светофора «ЗАПРОС ЦЕНЫ»: preq_ok:<id> / preq_other:<id>."""
    query = update.callback_query
    uid = update.effective_user.id if update.effective_user else 0
    if uid != _owner_id():
        await query.answer("Только для собственника.", show_alert=True)
        return
    try:
        action, sid = (query.data or "").split(":", 1)
        req_id = int(sid)
    except (ValueError, TypeError):
        await query.answer()
        return

    row = _one("SELECT * FROM price_requests WHERE id=%s", (req_id,))
    if not row:
        await query.answer("Запрос не найден.", show_alert=True)
        return

    if action == "preq_ok":
        done = await _approve(context.bot, req_id, float(row["price_requested"]), ("pending", "awaiting_price"))
        if not done:
            await query.answer(f"Уже обработано: {row['status']}", show_alert=True)
            return
        await query.answer("Согласовано")
        await query.message.reply_text(
            f"✅ №{req_id} согласовано: {_rub(done['approved_price'])} ₽/кг, "
            f"до {done['valid_until']:%d.%m} на один заказ. Менеджер уведомлён.")
        return

    if action == "preq_other":
        # один ввод за раз: предыдущий незаконченный – обратно на кнопки
        _one("""UPDATE price_requests SET status='pending', awaiting_since=NULL
                WHERE status='awaiting_price' AND id<>%s""", (req_id,))
        upd = _one("""UPDATE price_requests SET status='awaiting_price', awaiting_since=NOW()
                      WHERE id=%s AND status IN ('pending','awaiting_price') RETURNING *""", (req_id,))
        if not upd:
            await query.answer(f"Уже обработано: {row['status']}", show_alert=True)
            return
        await query.answer()
        await query.message.reply_text(
            f"✏️ Запрос №{req_id} – {row['client_name']}, {_sku(row)}.\n"
            f"Напишите цену за кг числом, например {_rub(row.get('recommended') or row['price_requested'])}. "
            f"«отмена» – вернуться к кнопкам.")


async def owner_price_input(message, context) -> bool:
    """Собственник пишет цену после «Другая цена». True – сообщение обработано."""
    text = (message.text or "").strip()
    row = _one("""SELECT * FROM price_requests WHERE status='awaiting_price'
                  AND awaiting_since > NOW() - (%s || ' minutes')::interval
                  ORDER BY awaiting_since DESC LIMIT 1""", (str(AWAIT_MINUTES),))
    if not row:
        return False
    if text.lower() in ("отмена", "отменить", "cancel"):
        _one("UPDATE price_requests SET status='pending', awaiting_since=NULL WHERE id=%s", (row["id"],))
        await message.reply_text(f"Запрос №{row['id']}: ввод цены отменён, кнопки остались в сообщении.")
        return True
    m = _PRICE_RE.match(text)
    if not m:
        return False                       # не число – это не ответ на запрос, пусть обработает бот дальше
    price = float(m.group(1).replace(" ", "").replace(",", "."))
    if price <= 0:
        return False
    done = await _approve(context.bot, row["id"], price, ("awaiting_price",))
    if not done:
        await message.reply_text(f"Запрос №{row['id']} уже обработан.")
        return True
    rec = float(row["recommended"]) if row.get("recommended") is not None else None
    below = f" Ниже рекомендованной ({_rub(rec)})." if rec and price < rec - 0.5 else ""
    await message.reply_text(
        f"✅ №{row['id']} согласовано: {_rub(price)} ₽/кг, до {done['valid_until']:%d.%m} на один заказ.{below} "
        f"Менеджер уведомлён.")
    return True
