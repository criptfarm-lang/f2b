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
VOLUME_TOLERANCE = 1.2      # согласованная цена держит объём до +20 % от запрошенного
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


def _all(sql: str, params=()) -> list[dict]:
    try:
        with _db().cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        global _conn
        _conn = None
        with _db().cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())


def apply_dashboard_approvals(price: dict, order_name: str) -> dict:
    """Позиции заказа ниже прайса, цену которых уже согласовали в дашборде («ЗАПРОС ЦЕНЫ»).

    Совпадение: тот же клиент (id МойСклада, а для «нового» – ИНН), та же позиция,
    согласование ещё действует (14 дней) и не использовано в другом заказе.
    Цена в заказе не ниже согласованной – позиция уходит из красной строки (и из
    блока закупщика для привлечённых) в пометку «согласовано в дашборде», запрос
    помечается использованным этим заказом. Ниже согласованной – остаётся на месте
    с подсказкой, что было согласовано.
    """
    items = list(price.get("items") or [])
    attracted = list(price.get("attracted_items") or [])
    agent_id, agent_inn = price.get("agent_id"), price.get("agent_inn")
    codes = [it.get("code") for it in items + attracted if it.get("code")]
    if not codes or not (agent_id or agent_inn):
        return price
    today = datetime.now(MSK).date()
    rows = _all(
        """SELECT id, sku_code, approved_price, qty_kg FROM price_requests
           WHERE status='approved' AND valid_until >= %s
             AND (used_order IS NULL OR used_order = %s)
             AND sku_code = ANY(%s)
             AND (client_ms_id = %s OR (client_inn IS NOT NULL AND client_inn <> '' AND client_inn = %s))
           ORDER BY decided_at DESC""",
        (today, order_name, codes, agent_id or "", agent_inn or ""))
    if not rows:
        return price
    by_code: dict[str, dict] = {}
    for r in rows:
        by_code.setdefault(r["sku_code"], r)
    ok: list[dict] = []

    def split(lst: list[dict]) -> list[dict]:
        keep = []
        for it in lst:
            r = by_code.get(it.get("code"))
            if not r:
                keep.append(it)
                continue
            approved = float(r["approved_price"])
            agreed_qty = float(r["qty_kg"]) if r.get("qty_kg") else None
            info = {"dashboard_id": r["id"], "dashboard_price": approved, "dashboard_qty": agreed_qty}
            price_ok = it["order_price"] >= approved - 0.5
            # скидку согласовывали на объём – в заказе больше +20 % уже не тот запрос
            qty_ok = not agreed_qty or not it.get("qty") or it["qty"] <= agreed_qty * VOLUME_TOLERANCE + 0.5
            if price_ok and qty_ok:
                ok.append({**it, **info})
                _one("UPDATE price_requests SET used_order=%s WHERE id=%s", (order_name, r["id"]))
            else:
                reason = "объём больше согласованного" if price_ok else "цена ниже согласованной"
                keep.append({**it, **info, "dashboard_reason": reason})
        return keep

    red = split(items)
    attracted_left = split(attracted)
    return {**price, "items": red, "attracted_items": attracted_left, "dashboard_items": ok,
            "color": "red" if red else "green"}


def _owner_id() -> int:
    v = (os.getenv("OWNER_CHAT_ID") or "").strip()
    return int(v) if v.lstrip("-").isdigit() else 0


def approver_ids() -> set[int]:
    """Кто решает по «ЗАПРОС ЦЕНЫ»: собственник и закупщик (привлечённые товары – Кристина)."""
    ids = {_owner_id()}
    try:
        from notifier import _attracted_approver_chat_id
        buyer = _attracted_approver_chat_id()
        if buyer:
            ids.add(int(buyer))
    except Exception:
        pass
    ids.discard(0)
    return ids


def _decider_name(uid: int) -> str:
    return "Виктор" if uid == _owner_id() else "Кристина"


def _can_decide(uid: int, row: dict) -> bool:
    """Собственник – по любому запросу; закупщик – по тем, что ушли ему."""
    return uid == _owner_id() or (row.get("approver_chat") and uid == int(row["approver_chat"]))


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


async def _approve(bot, req_id: int, price: float, from_status: tuple[str, ...],
                   decided_by: str = "Виктор") -> dict | None:
    """Согласовать цену: статус, срок, снять кнопки, пуш менеджеру. None – уже обработано."""
    valid_until = datetime.now(MSK).date() + timedelta(days=VALID_DAYS)
    row = _one(
        """UPDATE price_requests SET status='approved', approved_price=%s, decided_by=%s,
                  decided_at=NOW(), valid_until=%s, awaiting_since=NULL, awaiting_by=NULL
           WHERE id=%s AND status = ANY(%s) RETURNING *""",
        (price, decided_by, valid_until, req_id, list(from_status)))
    if not row:
        return None
    await _drop_buttons(bot, row)
    chat = _manager_chat(row["manager_tag"])
    if chat:
        asked = float(row["price_requested"])
        other = f" (вы просили {_rub(asked)})" if abs(float(price) - asked) >= 0.5 else ""
        qty = f", {_rub(row['qty_kg'])} кг" if row.get("qty_kg") else ""
        who = "" if decided_by == "Виктор" else f" ({decided_by})"
        text = (f"✅ Цена согласована{who} – запрос №{row['id']}\n\n"
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
    if not _can_decide(uid, row):
        await query.answer("Решает тот, кому пришёл запрос.", show_alert=True)
        return

    if action == "preq_ok":
        done = await _approve(context.bot, req_id, float(row["price_requested"]), ("pending", "awaiting_price"),
                              _decider_name(uid))
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
        _one("""UPDATE price_requests SET status='pending', awaiting_since=NULL, awaiting_by=NULL
                WHERE status='awaiting_price' AND awaiting_by=%s AND id<>%s""", (uid, req_id))
        upd = _one("""UPDATE price_requests SET status='awaiting_price', awaiting_since=NOW(), awaiting_by=%s
                      WHERE id=%s AND status IN ('pending','awaiting_price') RETURNING *""", (uid, req_id))
        if not upd:
            await query.answer(f"Уже обработано: {row['status']}", show_alert=True)
            return
        await query.answer()
        await query.message.reply_text(
            f"✏️ Запрос №{req_id} – {row['client_name']}, {_sku(row)}.\n"
            f"Напишите цену за кг числом, например {_rub(row.get('recommended') or row['price_requested'])}. "
            f"«отмена» – вернуться к кнопкам.")


async def owner_price_input(message, context) -> bool:
    """Согласующий (собственник или закупщик) пишет цену после «Другая цена».
    True – сообщение обработано, дальше его не разбирать."""
    text = (message.text or "").strip()
    uid = message.from_user.id if message.from_user else 0
    row = _one("""SELECT * FROM price_requests WHERE status='awaiting_price' AND awaiting_by=%s
                  AND awaiting_since > NOW() - (%s || ' minutes')::interval
                  ORDER BY awaiting_since DESC LIMIT 1""", (uid, str(AWAIT_MINUTES)))
    if not row:
        return False
    if text.lower() in ("отмена", "отменить", "cancel"):
        _one("UPDATE price_requests SET status='pending', awaiting_since=NULL, awaiting_by=NULL WHERE id=%s",
             (row["id"],))
        await message.reply_text(f"Запрос №{row['id']}: ввод цены отменён, кнопки остались в сообщении.")
        return True
    m = _PRICE_RE.match(text)
    if not m:
        return False                       # не число – это не ответ на запрос, пусть обработает бот дальше
    price = float(m.group(1).replace(" ", "").replace(",", "."))
    if price <= 0:
        return False
    done = await _approve(context.bot, row["id"], price, ("awaiting_price",), _decider_name(uid))
    if not done:
        await message.reply_text(f"Запрос №{row['id']} уже обработан.")
        return True
    rec = float(row["recommended"]) if row.get("recommended") is not None else None
    below = f" Ниже рекомендованной ({_rub(rec)})." if rec and price < rec - 0.5 else ""
    await message.reply_text(
        f"✅ №{row['id']} согласовано: {_rub(price)} ₽/кг, до {done['valid_until']:%d.%m} на один заказ.{below} "
        f"Менеджер уведомлён.")
    return True
