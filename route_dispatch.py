"""
Подтверждение маршрутов логистом и пуш водителям.
План: F2B второй мозг/plans/2026-07-16-подтверждение-маршрутов-логистом-и-пуш-водителям.md

Фаза 1: Белякова/владелец жмёт «Собрать маршруты» → бот читает раскладку из
Wialon (route_registry), по каждой машине формирует сводку (водитель + точки в
порядке выгрузки) + PDF-реестр и шлёт логисту с кнопками [Подтвердить]/[Пересобрать].
Состояние — таблица route_dispatch (draft → confirmed). Пуш водителю/складу — Фаза 2.
"""
import io
import os
import json
import logging
from datetime import datetime, timezone, timedelta

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (Application, CommandHandler, MessageHandler,
                          CallbackQueryHandler, ContextTypes, filters)

import route_registry as rr

logger = logging.getLogger(__name__)
_MSK = timezone(timedelta(hours=3))
_DB = None


def _logist_chat_id() -> int:
    return int(os.getenv("LOGIST_CHAT_ID", "8267564735") or 0)


def _owner_chat_id() -> int:
    return int(os.getenv("OWNER_CHAT_ID", "0") or 0)


def _allowed(chat_id: int) -> bool:
    return chat_id in (_logist_chat_id(), _owner_chat_id())


# ─── Схема ───────────────────────────────────────────────────────────────────

def ensure_schema(db):
    db._execute("""
        CREATE TABLE IF NOT EXISTS route_dispatch (
            snap_date       DATE,
            unit_id         BIGINT,
            driver_chat_id  BIGINT,
            status          TEXT DEFAULT 'draft',
            stops           JSONB,
            driver_msg_id   BIGINT,
            confirmed_at    TIMESTAMPTZ,
            updated_at      TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (snap_date, unit_id)
        )
    """)
    logger.info("route_dispatch: схема готова")


def _driver_for_unit(unit_id):
    """(chat_id, name) водителя, закреплённого за юнитом (drivers.unit_id)."""
    if _DB is None:
        return (None, None)
    try:
        r = _DB._fetchone(
            "SELECT chat_id, name FROM drivers WHERE unit_id=%s AND active LIMIT 1", (unit_id,))
        if r:
            return (int(r["chat_id"]), r.get("name"))
    except Exception as e:
        logger.warning("_driver_for_unit: %s", e)
    return (None, None)


def _upsert_draft(snap_date, unit_id, driver_id, snap):
    if _DB is None:
        return
    try:
        _DB._execute("""
            INSERT INTO route_dispatch (snap_date, unit_id, driver_chat_id, status, stops, updated_at)
            VALUES (%s, %s, %s, 'draft', %s, now())
            ON CONFLICT (snap_date, unit_id) DO UPDATE SET
              driver_chat_id = EXCLUDED.driver_chat_id, status = 'draft',
              stops = EXCLUDED.stops, driver_msg_id = NULL,
              confirmed_at = NULL, updated_at = now()
        """, (snap_date, unit_id, driver_id, json.dumps(snap, ensure_ascii=False)))
    except Exception as e:
        logger.warning("_upsert_draft: %s", e)


def _stop_on_date(s, d) -> bool:
    ref = s.get("tf") or s.get("vt") or s.get("tt")
    if not ref:
        return False
    return datetime.fromtimestamp(ref, _MSK).date() == d


# ─── Сбор маршрутов → логисту ────────────────────────────────────────────────

async def cmd_routes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    if not _allowed(user.id):
        await update.message.reply_text("⛔ Доступно логисту и владельцу.")
        return
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Сегодня", callback_data="rd:col:0"),
        InlineKeyboardButton("Завтра", callback_data="rd:col:1"),
    ]])
    await update.message.reply_text(
        "Собрать маршруты из Логистики на подтверждение — на какой день?",
        reply_markup=kb)


async def cb_collect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Собираю…")
    day_off = int(q.data.split(":")[2])
    target = (datetime.now(_MSK) + timedelta(days=day_off)).date()
    await _collect_and_send(context, target, to_chat=q.from_user.id)


async def _collect_and_send(context, target_date, to_chat):
    try:
        routes = await rr.fetch_routes()
    except Exception as e:
        logger.exception("collect fetch_routes: %s", e)
        await context.bot.send_message(to_chat, "Не удалось прочитать маршруты Wialon. Проверь доступ.")
        return
    me = await context.bot.get_me()
    date_str = target_date.strftime("%d.%m.%Y")
    day_off = (target_date - datetime.now(_MSK).date()).days
    sent = 0
    for uid in rr.UNITS:
        stops = sorted(
            [s for s in (routes.get(uid) or []) if _stop_on_date(s, target_date)],
            key=lambda s: (s.get("seq") if s.get("seq") is not None else 999))
        if not stops:
            continue
        driver_id, driver_name = _driver_for_unit(uid)
        order_numbers = [s["order_no"] for s in stops]
        ms_extra = await rr._ms_extra_by_order(order_numbers)
        pdf = rr._build_registry_pdf({uid: stops}, ms_extra, me.username, date_str)
        snap = [{"order_no": s["order_no"], "client": s.get("client"), "seq": s.get("seq")}
                for s in stops]
        _upsert_draft(target_date, uid, driver_id, snap)

        who = driver_name or "⚠️ водитель не закреплён"
        lines = [f"{i}. {(s.get('client') or s.get('order_no') or '?')[:35]} ({rr._hm(s.get('vt'))})"
                 for i, s in enumerate(stops, 1)]
        caption = (f"🚚 {rr.UNITS[uid]} — водитель: {who}\n"
                   f"Маршрут на {date_str} — {len(stops)} точек (порядок выгрузки):\n"
                   + "\n".join(lines))
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить",
                                  callback_data=f"rd:conf:{target_date.isoformat()}:{uid}")],
            [InlineKeyboardButton("🔄 Пересобрать", callback_data=f"rd:col:{day_off}")],
        ])
        await context.bot.send_document(
            to_chat, document=io.BytesIO(pdf),
            filename=f"reestr_{uid}_{target_date.isoformat()}.pdf",
            caption=caption[:1024], reply_markup=kb)
        sent += 1
    if sent == 0:
        await context.bot.send_message(
            to_chat, f"На {date_str} маршрутов в Логистике нет (не построены).")


async def cb_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Фаза 1: фиксируем подтверждение. Пуш водителю + склад — Фаза 2."""
    q = update.callback_query
    await q.answer()
    parts = q.data.split(":")  # rd:conf:<date>:<uid>
    dstr, uid = parts[2], int(parts[3])
    if _DB is not None:
        try:
            _DB._execute(
                "UPDATE route_dispatch SET status='confirmed', confirmed_at=now(), "
                "updated_at=now() WHERE snap_date=%s AND unit_id=%s", (dstr, uid))
        except Exception as e:
            logger.warning("cb_confirm: %s", e)
    try:
        await q.edit_message_caption(
            caption=(q.message.caption or "") + "\n\n✅ Подтверждено (пуш водителю — следующий срез).")
    except Exception as e:
        logger.warning("cb_confirm edit: %s", e)


def register(app: Application, db):
    global _DB
    _DB = db
    ensure_schema(db)
    app.add_handler(CommandHandler("routes", cmd_routes))
    app.add_handler(MessageHandler(filters.Regex(r"^/маршруты(@\w+)?(\s|$)"), cmd_routes))
    app.add_handler(CallbackQueryHandler(cb_collect, pattern=r"^rd:col:"))
    app.add_handler(CallbackQueryHandler(cb_confirm, pattern=r"^rd:conf:"))
    logger.info("route_dispatch: хендлеры зарегистрированы")
