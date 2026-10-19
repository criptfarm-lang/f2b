"""Сверка 18:00: заказы на завтра в группу F2B PRO (Ф8 плана 2026-10-16-логистика-всё-в-развозке).

Раньше логист в 18:00 слала в группу скрины заказов на завтра, менеджеры смотрели,
всё ли попало и согласовано. Теперь бот сам: по каждому заказу нашей развозки на
завтра – чего не хватает: не согласован, нет окна приёмки, нет контакта приёмки
(телефона в комментарии к адресу), нет «Названия ресторана», нет точки или точка
примерная (из «Развозки»). Контакт приёмки менеджер подтверждает кнопкой раз в
CONFIRM_DAYS дней – шеф мог смениться (решения собственника 19.10.2026: группа
F2B PRO, раз в 4 недели; пинг «не проставлено окно» остаётся как есть).
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time
from datetime import date, datetime, time as dtime, timedelta, timezone

import aiohttp
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

import delivery_window_check as dwc
import route_registry as rr

logger = logging.getLogger(__name__)
_MSK = timezone(timedelta(hours=3))

GROUP_CHAT_ID = int(os.getenv("RAZVOZKA_EVENING_CHAT_ID", "-4824850517"))   # F2B PRO
CONFIRM_DAYS = 28
RAZVOZKA_URL = os.getenv("RAZVOZKA_INTERNAL_URL", "http://127.0.0.1:8087")
# Статусы, в которых заказ «согласован» – как у «Развозки» (config.ORDER_STATES_SYNC моста).
OK_STATES = {"Согласован", "Собирается", "Собран без охл", "Собран", "Документы готовы",
             "ИЗМЕНЕН", "Отгружен"}
SKIP_STATES = {"Возврат", "Отменен"}
ATTR_RESTAURANT = "Название ресторана"
WEEKDAYS = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
_MAX_BUTTONS = 30

_DB = None


def ensure_schema(db) -> None:
    db._execute("""
        CREATE TABLE IF NOT EXISTS contact_confirm (
            agent_id      TEXT PRIMARY KEY,
            agent_name    TEXT,
            confirmed_at  TIMESTAMPTZ,
            confirmed_by  TEXT
        )
    """)


def _confirmed(agent_ids) -> dict:
    """{agent_id: datetime подтверждения контакта}."""
    if _DB is None or not agent_ids:
        return {}
    try:
        rows = _DB._fetchall("SELECT agent_id, confirmed_at FROM contact_confirm WHERE agent_id = ANY(%s)",
                             (list(agent_ids),))
    except Exception as e:
        logger.warning("contact_confirm: %s", e)
        return {}
    return {r["agent_id"]: r["confirmed_at"] for r in rows or []}


def _token() -> str | None:
    secret = os.getenv("DISPATCH_SSO_SECRET", "")
    if not secret:
        return None
    msg = f"{int(time.time()) + 600}.owner"
    return msg + "." + hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()


async def _razvozka_geo(day: date) -> dict:
    """{№ заказа: «нет точки …» | «точка примерная»} из плана «Развозки» на дату."""
    tok = _token()
    if not tok:
        return {}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{RAZVOZKA_URL}/api/plan", params={"date": day.isoformat(), "k": tok},
                             timeout=aiohttp.ClientTimeout(total=180)) as r:
                plan = await r.json(content_type=None)
    except Exception as e:
        logger.warning("сверка 18:00: «Развозка» не ответила: %s", e)
        return {}
    out = {}
    stops = [x for u in plan.get("units") or [] for x in u.get("stops") or []]
    for b in ("hired", "taxi", "unassigned"):
        stops += plan.get(b) or []
    for st in stops:
        no = str(st.get("order_no") or "")
        if st.get("lat") is None:
            out[no] = "нет точки на карте" + (f" ({st['hold_reason']})" if st.get("hold_reason") else "")
        elif st.get("geo_rough"):
            out[no] = "точка на карте примерная"
    return out


def _is_retail(o: dict) -> bool:
    return ((o.get("agent") or {}).get("name") or "").lower().startswith("розничный покупатель")


def _restaurant(o: dict) -> str:
    attrs = {a.get("name"): a.get("value") for a in ((o.get("agent") or {}).get("attributes") or [])}
    v = str(attrs.get(ATTR_RESTAURANT) or "").strip()
    return "" if v.lower() in ("нет", "-", "—", "–") else v


def order_issues(o: dict, geo: dict, confirmed: dict, now: datetime) -> tuple[list[str], bool]:
    """(список проблем заказа, нужна ли кнопка «контакт актуален»). Неподтверждённый
    контакт – не проблема заказа, а отдельная кнопка (иначе первые недели в списке все)."""
    issues = []
    retail = _is_retail(o)
    state = ((o.get("state") or {}).get("name") or "").strip()
    if state not in OK_STATES:
        issues.append(f"не согласован ({state or 'без статуса'})")
    gap = dwc.window_gap(o)
    if gap == ["с", "до"]:
        issues.append("нет окна приёмки")
    elif gap:
        issues.append(f"нет окна «{gap[0]}»")
    comment = ((o.get("shipmentAddressFull") or {}).get("comment") or "")
    need_confirm = False
    if not rr._phone_from_text(comment):
        issues.append("нет контакта приёмки (телефона в комментарии к адресу)")
    elif not retail:
        at = confirmed.get((o.get("agent") or {}).get("id"))
        need_confirm = at is None or (now - at).days >= CONFIRM_DAYS
    if not retail and not _restaurant(o):
        issues.append("нет «Названия ресторана»")
    g = geo.get(str(o.get("name") or ""))
    if g:
        issues.append(g)
    return issues, need_confirm


def _who(o: dict) -> str:
    rest = _restaurant(o)
    name = (o.get("agent") or {}).get("name") or "—"
    return f"{rest} ({name})" if rest else name


async def build(day: date) -> tuple[list[str], InlineKeyboardMarkup | None, int]:
    """(части текста ≤ 3800 симв., кнопки, число заказов). Пусто – заказов нет."""
    async with aiohttp.ClientSession() as session:
        orders = await dwc._fetch_orders(session, day, day)
    orders = [o for o in orders
              if ((o.get("state") or {}).get("name") or "") not in SKIP_STATES
              and dwc.is_our_delivery(o.get("shipmentAddress") or "")]
    if not orders:
        return [], None, 0
    geo = await _razvozka_geo(day)
    agent_ids = {(o.get("agent") or {}).get("id") for o in orders} - {None}
    confirmed = _confirmed(agent_ids)
    now = datetime.now(_MSK)
    by_mgr: dict[str, list[str]] = {}
    buttons, seen_agents, n_bad = [], set(), 0
    for o in sorted(orders, key=lambda x: x.get("name") or ""):
        issues, need_confirm = order_issues(o, geo, confirmed, now)
        if issues:
            n_bad += 1
            mgr = ((o.get("owner") or {}).get("name") or "без менеджера").split()[0]
            by_mgr.setdefault(mgr, []).append(f"• №{o.get('name')} {_who(o)[:60]}: " + "; ".join(issues))
        agent = o.get("agent") or {}
        if need_confirm and agent.get("id") and agent["id"] not in seen_agents \
                and len(buttons) < _MAX_BUTTONS:
            seen_agents.add(agent["id"])
            buttons.append([InlineKeyboardButton(f"✓ Контакт актуален: {_who(o)[:40]}",
                                                 callback_data=f"rcc:{agent['id']}")])
    head = (f"🗓 Сверка на завтра, {day:%d.%m} ({WEEKDAYS[day.weekday()]}): заказов нашей развозки "
            f"{len(orders)}, с вопросами {n_bad}.")
    blocks = [head]
    if n_bad:
        blocks.append("Менеджеры, проверьте и поправьте в МойСклад до утра:")
        for mgr in sorted(by_mgr):
            blocks.append(f"\n{mgr}:\n" + "\n".join(by_mgr[mgr]))
    else:
        blocks.append("Всё согласовано, окна, контакты и точки на месте.")
    if buttons:
        blocks.append(f"\nКонтакт приёмки по этим клиентам не подтверждали {CONFIRM_DAYS // 7} недели "
                      "или ни разу. Шеф не сменился, телефон верный – нажмите кнопку клиента.")
    parts, cur = [], ""
    for b in blocks:
        if len(cur) + len(b) + 1 > 3800:
            parts.append(cur)
            cur = ""
        cur = (cur + "\n" + b) if cur else b
    parts.append(cur)
    return parts, (InlineKeyboardMarkup(buttons) if buttons else None), len(orders)


async def send(bot, day: date, chat_id: int = GROUP_CHAT_ID) -> str:
    parts, kb, n = await build(day)
    if not parts:
        return f"на {day:%d.%m} заказов нашей развозки нет – сверку не шлю"
    for i, p in enumerate(parts):
        await bot.send_message(chat_id, p, reply_markup=kb if i == len(parts) - 1 else None)
    return f"сверка на {day:%d.%m}: заказов {n}, сообщений {len(parts)}"


async def _job(context: ContextTypes.DEFAULT_TYPE):
    day = datetime.now(_MSK).date() + timedelta(days=1)
    try:
        logger.info("сверка 18:00: %s", await send(context.bot, day))
    except Exception as e:
        logger.exception("сверка 18:00: %s", e)


async def cb_contact_confirmed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Кнопка «✓ Контакт актуален» под сверкой: запоминаем за контрагентом на CONFIRM_DAYS."""
    q = update.callback_query
    if q.message is None or q.message.chat.id != GROUP_CHAT_ID:
        await q.answer()
        return
    agent_id = q.data.split(":", 1)[1]
    label = next((b.text for r in (q.message.reply_markup.inline_keyboard if q.message.reply_markup else [])
                  for b in r if b.callback_data == q.data), "")
    try:
        _DB._execute(
            "INSERT INTO contact_confirm (agent_id, agent_name, confirmed_at, confirmed_by) "
            "VALUES (%s,%s,now(),%s) ON CONFLICT (agent_id) DO UPDATE SET confirmed_at=now(), "
            "confirmed_by=EXCLUDED.confirmed_by, agent_name=EXCLUDED.agent_name",
            (agent_id, label.replace("✓ Контакт актуален: ", "")[:200], q.from_user.full_name))
    except Exception as e:
        logger.warning("cb_contact_confirmed: %s", e)
        await q.answer("Не сохранилось, попробуйте ещё раз", show_alert=True)
        return
    await q.answer("Записал: контакт актуален")
    try:
        left = [r for r in q.message.reply_markup.inline_keyboard if r[0].callback_data != q.data]
        await q.edit_message_reply_markup(InlineKeyboardMarkup(left) if left else None)
    except Exception as e:
        logger.warning("cb_contact_confirmed edit: %s", e)


def register(app: Application, db):
    global _DB
    _DB = db
    ensure_schema(db)
    app.add_handler(CallbackQueryHandler(cb_contact_confirmed, pattern=r"^rcc:"))
    if app.job_queue:
        app.job_queue.run_daily(_job, time=dtime(hour=18, minute=0, tzinfo=_MSK), name="razvozka_evening")
    logger.info("razvozka_evening: register (чат %s)", GROUP_CHAT_ID)
