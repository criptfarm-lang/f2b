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
import re
import json
import math
import asyncio
import logging
from datetime import datetime, date, timezone, timedelta

import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (Application, CommandHandler, MessageHandler,
                          CallbackQueryHandler, ContextTypes, filters)

import route_registry as rr
from moysklad import MS_BASE, get_headers

logger = logging.getLogger(__name__)
_MSK = timezone(timedelta(hours=3))
_DB = None


# Логисты (равный доступ ко всему логистскому функционалу и рассылкам):
# 8267564735 — Белякова, 1689203038 — Петровский Владимир. Список — env
# LOGIST_CHAT_IDS (через запятую) с дефолтом обоих; добавить логиста = дописать id.
def _logist_chat_ids() -> list:
    raw = os.getenv("LOGIST_CHAT_IDS") or "8267564735,1689203038"
    out = []
    for p in raw.split(","):
        p = p.strip()
        if p:
            try:
                out.append(int(p))
            except ValueError:
                pass
    return out


def _logist_chat_id() -> int:
    ids = _logist_chat_ids()
    return ids[0] if ids else 0


def _owner_chat_id() -> int:
    return int(os.getenv("OWNER_CHAT_ID", "0") or 0)


def _allowed(chat_id: int) -> bool:
    return chat_id in _logist_chat_ids() or chat_id == _owner_chat_id()


def _sklad_chat_id() -> int:
    return int(os.getenv("SKLAD_CHAT_ID", "-4750423130") or 0)  # группа «Склад»


# ─── Кубатура / вместимость машин ────────────────────────────────────────────
# Готовая продукция + мелкая привлечёнка едут в одинаковых гофрокоробах 600×250×200 мм.
# Объём машины = сумма «Количество мест» по точкам × объём короба. Согласовано 2026-07-22.
_BOX_VOLUME_M3 = 0.6 * 0.25 * 0.2  # 0.03 м³ на одно «место»
_BOX_WEIGHT_KG = 10  # средний вес короба (пляшет 8–12) — для прогноза мест по весу; собственник 2026-07-24
_CAP_BOXES = {26210: 190, 26209: 165}  # К459 Porter / В970 KIA — «руками» (0 паллетов)
# Вместимость коробов ЗАВИСИТ от числа паллетов (паллет грузится неэффективно).
# Калибровка собственника 2026-07-24 (memory reference_f2b_truck_capacity_by_pallets).
_PALLET_KG = 360        # ~вес одного паллета (≈30 коробов) — для расчёта числа паллетов заказа
_CAP_BY_PALLETS = {
    26210: {0: 190, 1: 157, 2: 130, 3: 102},  # К459 Porter
    26209: {0: 165, 1: 140, 2: 121, 3: 102},  # В970 KIA
}
# Клиенты паллетной загрузки (имена как в МС, через «;»). Собственник даёт список.
# Пусто → паллетов 0 → вместимость «руками» (прежнее поведение).
_PALLET_CLIENTS = {c.strip().lower() for c in os.getenv("PALLET_CLIENTS", "").split(";") if c.strip()}


def _pallets_for(stops, ms_extra) -> int:
    """Число паллетов в загрузке = по заказам клиентов из _PALLET_CLIENTS:
    вес ÷ 360 кг, округление вверх (обычно 1–2). Без веса — минимум 1 паллет."""
    pallets = 0
    for s in stops:
        if (s.get("client") or "").strip().lower() in _PALLET_CLIENTS:
            w = (ms_extra.get(s["order_no"]) or {}).get("weight") or 0
            pallets += max(1, math.ceil(w / _PALLET_KG)) if w > 0 else 1
    return pallets


def _cap_boxes_for(uid, pallets: int):
    """Вместимость коробов машины с учётом числа паллетов (0–3)."""
    tbl = _CAP_BY_PALLETS.get(uid)
    if not tbl:
        return _CAP_BOXES.get(uid)
    return tbl[min(pallets, 3)]


def _total_boxes(stops, ms_extra):
    """Сумма «Количество мест» по точкам. Если поле не заполнено — прогнозируем места
    по весу (наша коробка ~12 кг). Возвращает (boxes, n_missing, n_estimated):
    n_estimated — сколько точек оценено по весу, n_missing — сколько осталось без мест
    И без веса (их объём не учтён)."""
    total = missing = estimated = 0
    for s in stops:
        ex = ms_extra.get(s["order_no"]) or {}
        pl = ex.get("places")
        try:
            n = int(pl) if pl not in (None, "") else 0
        except (ValueError, TypeError):
            n = 0
        if n <= 0:
            w = ex.get("weight") or 0
            if w > 0:
                n = max(1, math.ceil(w / _BOX_WEIGHT_KG))  # прогноз по весу
                estimated += 1
            else:
                missing += 1
        total += n
    return total, missing, estimated


def _volume_note(uid, stops, ms_extra, km=None) -> str:
    """Заметка по объёму загрузки машины — в САМО сообщение (не в реестр-PDF),
    видят все: логист, водитель, склад. Предупреждает о перегрузе. km — плановый
    пробег маршрута (Wialon), если известен."""
    boxes, missing, estimated = _total_boxes(stops, ms_extra)
    vol = round(boxes * _BOX_VOLUME_M3, 1)
    pallets = _pallets_for(stops, ms_extra)
    cap_boxes = _cap_boxes_for(uid, pallets)
    plt = f", {pallets} паллет" if pallets else ""
    if not cap_boxes:
        note = f"📦 Объём: ~{vol} м³ ({boxes} мест{plt})"
    else:
        cap_vol = round(cap_boxes * _BOX_VOLUME_M3, 1)
        if boxes > cap_boxes:
            note = (f"📦 ⚠️ ПЕРЕГРУЗ: ~{vol} м³ / лимит {cap_vol} м³ "
                    f"({boxes} из {cap_boxes} мест{plt}) — не влезает, сними точки или дай наём")
        else:
            pct = round(boxes / cap_boxes * 100)
            note = f"📦 Объём: ~{vol} м³ / {cap_vol} м³ ({boxes}/{cap_boxes} мест{plt}, {pct}%)"
    if km:
        note += f"\n🛣 Пробег маршрута: ~{km} км"
    if estimated:
        note += (f"\n📐 места не заполнены у {estimated} точ. — оценил по весу "
                 f"(коробка ~{_BOX_WEIGHT_KG} кг)")
    if missing:
        note += f"\n⚠️ у {missing} точ. нет ни мест, ни веса — объём занижен"
    return note


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
    db._execute("ALTER TABLE route_dispatch ADD COLUMN IF NOT EXISTS done JSONB DEFAULT '[]'::jsonb")
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


async def _safe_answer(q, text=None):
    """q.answer() — первый исходящий вызов в колбэке. При деградации связи Amvera↔Telegram
    он падает по TimedOut и БЕЗ обёртки ронял весь хендлер (сбор/подтверждение маршрута) ещё
    до полезной работы: draft не писался, PDF не уходил — «нажала, бот не сработал»
    (см. project_f2b_bot_telegram_timeout_outage_2026_07_22). Спиннер кнопки не критичен —
    глушим сетевую ошибку и доводим сбор до конца."""
    try:
        await q.answer(text)
    except Exception as e:
        logger.warning("q.answer проигнорирован (Telegram лагает?): %s", e)


async def cb_collect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _safe_answer(q, "Собираю…")
    day_off = int(q.data.split(":")[2])
    target = (datetime.now(_MSK) + timedelta(days=day_off)).date()
    await _collect_and_send(context, target, to_chat=q.from_user.id)


async def _unit_package(routes, uid, target_date, bot_username):
    """Пакет по машине на дату: {stops, pdf, driver_id, driver_name} или None (нет точек)."""
    stops = sorted(
        [s for s in (routes.get(uid) or []) if _stop_on_date(s, target_date)],
        key=lambda s: (s.get("seq") if s.get("seq") is not None else 999))
    if not stops:
        return None
    driver_id, driver_name = _driver_for_unit(uid)
    ms_extra = await rr._ms_extra_by_order([s["order_no"] for s in stops])
    pdf = await asyncio.to_thread(rr._build_registry_pdf, {uid: stops}, ms_extra,
                                  bot_username, target_date.strftime("%d.%m.%Y"))  # reportlab CPU-sync → поток
    return {"stops": stops, "pdf": pdf, "driver_id": driver_id, "driver_name": driver_name,
            "ms_extra": ms_extra}


_DOC_RE = re.compile(r"\d{2,}")


def _doc_no(order_no):
    """МС-номер документа из текста точки: первая числовая группа.
    Логист дописывает в поле № заказа адрес/заметки («00371 (Истринский…)») —
    их нельзя тащить в callback_data (лимит Telegram 64 байта: длинный текст
    роняет ВСЮ клавиатуру). В callback и в матчинге done↔stops используем только
    номер документа. Нет цифр (pickup-заметка, «Белякова») → усечь до безопасной длины."""
    s = (order_no or "").strip()
    m = _DOC_RE.search(s)
    if m:
        return m.group(0)
    return s.encode("utf-8")[:48].decode("utf-8", "ignore")  # ≤48б → callback ≤55б < 64


def _short_retail(name):
    """Сжать generic-имя розницы «Розничный покупатель*** (Инесса)» → «Розн. Инесса».
    Отличающий контакт стоит в конце длинного имени и срезался бы лимитом кнопки;
    вытаскиваем его вперёд. Именованные ООО/ИП возвращаем как есть."""
    name = (name or "?").strip()
    if name.startswith("Рознич"):
        m = re.search(r"\(([^)]+)\)", name)
        return f"Розн. {m.group(1)}" if m else "Розн."
    return name


def _btn_label(name, num):
    """Подпись кнопки точки: имя клиента + № заказа (совпадает с УПД в руках водителя).
    Два одинаковых розничных контакта различаются по №. Имя усечено так, чтобы № влез."""
    name = _short_retail(name)
    suffix = f" №{num}" if num else ""
    return f"{name[:40 - len(suffix)]}{suffix}"


def _stop_label(s):
    """Подпись точки маршрута из stop-словаря (client + № документа)."""
    return _btn_label(s.get("client") or s.get("order_no"), _doc_no(s.get("order_no")))


def _driver_kb(stops, done=None):
    """Кнопки маршрута водителю: точка → drv:rp:<№документа> (обрабатывает driver_checklist).
    done — множество № документов, уже закрытых (не показываем; Фаза 3)."""
    done = done or set()
    rows = []
    for i, s in enumerate(stops, 1):
        if _doc_no(s.get("order_no")) in done:
            continue
        rows.append([InlineKeyboardButton(f"📍 {i}. {_stop_label(s)}",
                                          callback_data=f"drv:rp:{_doc_no(s.get('order_no'))}")])
    return InlineKeyboardMarkup(rows) if rows else None


def is_confirmed_today(unit_id) -> bool:
    """Подтверждён ли маршрут этой машины на сегодня (для гейта /рейс)."""
    if _DB is None:
        return False
    try:
        today = datetime.now(_MSK).date()
        r = _DB._fetchone(
            "SELECT 1 FROM route_dispatch WHERE snap_date=%s AND unit_id=%s AND status='confirmed'",
            (today, unit_id))
        return bool(r)
    except Exception as e:
        logger.warning("is_confirmed_today: %s", e)
        return False


def _existing_done(snap_date, unit_id):
    """Множество уже закрытых № заказов (для сохранения прогресса при пересборе/переподтверждении)."""
    if _DB is None:
        return set()
    try:
        r = _DB._fetchone("SELECT done FROM route_dispatch WHERE snap_date=%s AND unit_id=%s",
                          (snap_date, unit_id))
        if r and r.get("done"):
            d = r["done"] if isinstance(r["done"], list) else json.loads(r["done"])
            return set(d)
    except Exception as e:
        logger.warning("_existing_done: %s", e)
    return set()


def mark_done_by_unit(unit_id, order_no):
    """Пометить точку закрытой в route_dispatch.done по МАШИНЕ (без TG-контекста).
    Для веб-приёмки: у веба нет driver_chat_id/driver_msg_id, зато есть unit_id из ссылки.
    Только БД — TG-сообщение не трогаем (веб работает мимо Telegram)."""
    if _DB is None:
        return
    try:
        today = datetime.now(_MSK).date()
        row = _DB._fetchone(
            "SELECT stops, done FROM route_dispatch "
            "WHERE snap_date=%s AND unit_id=%s AND status='confirmed'", (today, unit_id))
        if not row:
            return
        doc = _doc_no(order_no)
        stops = row.get("stops") or []
        done = row.get("done") or []
        if isinstance(stops, str):
            stops = json.loads(stops)
        if isinstance(done, str):
            done = json.loads(done)
        if doc not in [_doc_no(s.get("order_no")) for s in stops]:
            return  # точка не из этого маршрута
        if doc not in done:
            done.append(doc)
            _DB._execute("UPDATE route_dispatch SET done=%s, updated_at=now() WHERE snap_date=%s AND unit_id=%s",
                         (json.dumps(done, ensure_ascii=False), today, unit_id))
    except Exception as e:
        logger.warning("mark_done_by_unit: %s", e)


async def _demand_order_no(demand_id):
    """demand → номер его заказа (customerOrder.name) — чтобы сматчить закрытую точку с маршрутом."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{MS_BASE}/entity/demand/{demand_id}?expand=customerOrder",
                             headers=get_headers()) as r:
                if r.status != 200:
                    return None
                d = await r.json()
        return (d.get("customerOrder") or {}).get("name")
    except Exception as e:
        logger.warning("_demand_order_no: %s", e)
        return None


async def mark_done_and_refresh(context, driver_chat_id, demand_id):
    """Точка закрыта водителем → убираем её кнопку из сообщения-списка; последняя → «завершён» (Фаза 3)."""
    if _DB is None:
        return
    try:
        today = datetime.now(_MSK).date()
        row = _DB._fetchone(
            "SELECT unit_id, stops, done, driver_msg_id FROM route_dispatch "
            "WHERE snap_date=%s AND driver_chat_id=%s AND status='confirmed'",
            (today, driver_chat_id))
        if not row or not row.get("driver_msg_id"):
            return
        order_no = await _demand_order_no(demand_id)
        if not order_no:
            return
        doc = _doc_no(order_no)
        stops = row.get("stops") or []
        done = row.get("done") or []
        if isinstance(stops, str):
            stops = json.loads(stops)
        if isinstance(done, str):
            done = json.loads(done)
        if doc not in [_doc_no(s.get("order_no")) for s in stops]:
            return  # точка не из этого маршрута
        if doc not in done:
            done.append(doc)
        _DB._execute("UPDATE route_dispatch SET done=%s, updated_at=now() WHERE snap_date=%s AND unit_id=%s",
                     (json.dumps(done, ensure_ascii=False), today, row["unit_id"]))
        kb = _driver_kb(stops, done=set(done))
        if kb is None:
            await context.bot.edit_message_text(
                chat_id=driver_chat_id, message_id=row["driver_msg_id"],
                text="✅ Маршрут завершён — все точки закрыты. Спасибо!")
        else:
            await context.bot.edit_message_reply_markup(
                chat_id=driver_chat_id, message_id=row["driver_msg_id"], reply_markup=kb)
    except Exception as e:
        logger.warning("mark_done_and_refresh: %s", e)


async def _collect_and_send(context, target_date, to_chat):
    try:
        routes, order_routes = await rr.fetch_routes(with_meta=True)
    except Exception as e:
        logger.exception("collect fetch_routes: %s", e)
        await context.bot.send_message(to_chat, "Не удалось прочитать маршруты Wialon. Проверь доступ.")
        return
    me = await context.bot.get_me()
    date_str = target_date.strftime("%d.%m.%Y")
    day_off = (target_date - datetime.now(_MSK).date()).days
    sent = 0
    for uid in rr.UNITS:
        pkg = await _unit_package(routes, uid, target_date, me.username)
        if not pkg:
            continue
        stops = pkg["stops"]
        snap = [{"order_no": s["order_no"], "client": s.get("client"), "seq": s.get("seq")}
                for s in stops]
        _upsert_draft(target_date, uid, pkg["driver_id"], snap)

        who = pkg["driver_name"] or "⚠️ водитель не закреплён"
        km = rr.mileage_km(order_routes, uid, [s.get("oid") for s in stops])
        note = _volume_note(uid, stops, pkg["ms_extra"], km=km)
        lines = [f"{i}. {(s.get('client') or s.get('order_no') or '?')[:35]} ({rr._hm(s.get('vt'))})"
                 for i, s in enumerate(stops, 1)]
        caption = (f"🚚 {rr.UNITS[uid]} — водитель: {who}\n"
                   f"{note}\n"
                   f"Маршрут на {date_str} — {len(stops)} точек (порядок выгрузки):\n"
                   + "\n".join(lines))
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить",
                                  callback_data=f"rd:conf:{target_date.isoformat()}:{uid}")],
            [InlineKeyboardButton("🔄 Пересобрать", callback_data=f"rd:col:{day_off}")],
        ])
        await context.bot.send_document(
            to_chat, document=io.BytesIO(pkg["pdf"]),
            filename=f"reestr_{uid}_{target_date.isoformat()}.pdf",
            caption=caption[:1024], reply_markup=kb)
        sent += 1
    if sent == 0:
        await context.bot.send_message(
            to_chat, f"На {date_str} маршрутов в Логистике нет (не построены).")


async def cb_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение → status=confirmed + пуш водителю (PDF + кнопки) + PDF в склад-группу."""
    q = update.callback_query
    await _safe_answer(q, "Отправляю…")
    parts = q.data.split(":")  # rd:conf:<date>:<uid>
    target_date = date.fromisoformat(parts[2])
    uid = int(parts[3])
    dstr = parts[2]
    try:
        routes, order_routes = await rr.fetch_routes(with_meta=True)
        me = await context.bot.get_me()
        pkg = await _unit_package(routes, uid, target_date, me.username)
    except Exception as e:
        logger.exception("cb_confirm fetch: %s", e)
        await context.bot.send_message(q.from_user.id, "Не удалось перечитать маршрут Wialon для отправки.")
        return
    if not pkg:
        await context.bot.send_message(q.from_user.id, f"Маршрут {rr.UNITS.get(uid)} пуст — отправлять нечего.")
        return

    stops = pkg["stops"]
    driver_id = pkg["driver_id"]
    date_str = target_date.strftime("%d.%m.%Y")
    unit_name = rr.UNITS.get(uid, str(uid))
    km = rr.mileage_km(order_routes, uid, [s.get("oid") for s in stops])
    note = _volume_note(uid, stops, pkg["ms_extra"], km=km)  # заметка по объёму — во все сообщения
    # Навигация в Яндекс.Картах (база Ильинский → точки по порядку). На «густой» день
    # точек больше лимита Яндекса — тогда ya_block содержит несколько ссылок-частей.
    ya_urls = rr.yandex_route_urls(stops)
    if not ya_urls:
        ya_block = ""
    elif len(ya_urls) == 1:
        ya_block = f"\n🧭 Маршрут в Яндекс.Картах: {ya_urls[0]}"
    else:
        ya_block = "\n" + "\n".join(
            f"🧭 Яндекс.Карты, часть {i + 1}/{len(ya_urls)}: {u}" for i, u in enumerate(ya_urls))
    snap = [{"order_no": s["order_no"], "client": s.get("client"), "seq": s.get("seq")} for s in stops]
    if _DB is not None:
        try:
            _DB._execute(
                "UPDATE route_dispatch SET status='confirmed', stops=%s, confirmed_at=now(), "
                "updated_at=now() WHERE snap_date=%s AND unit_id=%s",
                (json.dumps(snap, ensure_ascii=False), dstr, uid))
        except Exception as e:
            logger.warning("cb_confirm update: %s", e)

    # 1) Пуш водителю: PDF + кнопки маршрута
    driver_note = ""
    if driver_id:
        try:
            await context.bot.send_document(
                driver_id, io.BytesIO(pkg["pdf"]),
                filename=f"reestr_{uid}_{target_date.isoformat()}.pdf",
                caption=f"🚚 Твой маршрут на {date_str} — {unit_name}. Реестр во вложении.{ya_block}")
            # Сохраняем прогресс: при пересборе/переподтверждении уже закрытые точки не показываем.
            done_prev = _existing_done(dstr, uid)
            kb = _driver_kb(stops, done=done_prev)
            if kb is None:
                msg = await context.bot.send_message(
                    driver_id, f"🚚 Маршрут на {date_str} — {unit_name}: все точки уже закрыты. ✅")
            else:
                left = len([s for s in stops if s["order_no"] not in done_prev])
                try:
                    import route_web
                    live = f"\n🌐 Живой маршрут (всегда актуальный): {route_web.route_url(uid, dstr)}"
                except Exception:
                    live = ""
                msg = await context.bot.send_message(
                    driver_id,
                    f"🚚 Маршрут на {date_str} — {left} из {len(stops)} точек (порядок выгрузки).\n"
                    f"{note}\n"
                    "Приехал на точку — жми её, закрывай сдачу:"
                    f"{ya_block}{live}",
                    reply_markup=kb)
            if _DB is not None:
                _DB._execute("UPDATE route_dispatch SET driver_msg_id=%s WHERE snap_date=%s AND unit_id=%s",
                             (msg.message_id, dstr, uid))
            driver_note = f"водитель {pkg['driver_name'] or driver_id}"
        except Exception as e:
            logger.warning("cb_confirm push driver: %s", e)
            driver_note = "⚠️ водителю НЕ доставлено (не нажимал /start у бота?)"
    else:
        driver_note = f"⚠️ за {unit_name} не закреплён водитель — не отправлено"

    # 2) PDF в склад-группу (лист загрузки)
    sklad_note = ""
    sklad = _sklad_chat_id()
    if sklad:
        try:
            await context.bot.send_document(
                sklad, io.BytesIO(pkg["pdf"]),
                filename=f"reestr_{uid}_{target_date.isoformat()}.pdf",
                caption=(f"📦 Лист загрузки — {unit_name}, {date_str}. Грузить по порядку "
                         f"(первая точка к дверям).\n{note}"
                         f"\n\n✍️ Перед выездом водитель подписывает копию реестра и отдаёт "
                         f"оператору склада."))
            sklad_note = "склад ✓"
        except Exception as e:
            logger.warning("cb_confirm push sklad: %s", e)
            sklad_note = "⚠️ склад НЕ доставлено"

    try:
        await q.edit_message_caption(
            caption=(q.message.caption or "") + f"\n\n✅ Подтверждено → {driver_note}; {sklad_note}.")
    except Exception as e:
        logger.warning("cb_confirm edit: %s", e)


async def cmd_progress(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Свод хода развоза логисту: по каждой машине сколько точек закрыто + что осталось."""
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    if not _allowed(user.id):
        await update.message.reply_text("⛔ Доступно логисту и владельцу.")
        return
    if _DB is None:
        await update.message.reply_text("БД недоступна.")
        return
    today = datetime.now(_MSK).date()
    rows = _DB._fetchall(
        "SELECT unit_id, stops, done FROM route_dispatch "
        "WHERE snap_date=%s AND status='confirmed' ORDER BY unit_id", (today,))
    if not rows:
        await update.message.reply_text("На сегодня подтверждённых маршрутов нет.")
        return
    blocks = []
    for r in rows:
        uid = r["unit_id"]
        stops = r.get("stops") or []
        done = r.get("done") or []
        if isinstance(stops, str):
            stops = json.loads(stops)
        if isinstance(done, str):
            done = json.loads(done)
        done_set = set(done)
        total = len(stops)
        remaining = [s for s in stops if s.get("order_no") not in done_set]
        ndone = total - len(remaining)
        _, drv = _driver_for_unit(uid)
        head = f"🚚 {rr.UNITS.get(uid, uid)} — {drv or 'без водителя'}: {ndone}/{total} закрыто"
        if remaining:
            rem = "\n".join(f"  • {(s.get('client') or s.get('order_no') or '?')[:35]}"
                            for s in remaining[:12])
            if len(remaining) > 12:
                rem += f"\n  …и ещё {len(remaining) - 12}"
            head += "\nОсталось:\n" + rem
        else:
            head += " ✅ завершён"
        blocks.append(head)
    await update.message.reply_text("Ход развоза на сегодня:\n\n" + "\n\n".join(blocks))


def register(app: Application, db):
    global _DB
    _DB = db
    app.add_handler(CommandHandler("routes", cmd_routes))
    app.add_handler(MessageHandler(filters.Regex(r"^/маршруты(@\w+)?(\s|$)"), cmd_routes))
    app.add_handler(CommandHandler("progress", cmd_progress))
    app.add_handler(MessageHandler(filters.Regex(r"^/ход(@\w+)?(\s|$)"), cmd_progress))
    app.add_handler(CallbackQueryHandler(cb_collect, pattern=r"^rd:col:"))
    app.add_handler(CallbackQueryHandler(cb_confirm, pattern=r"^rd:conf:"))
    # ensure_schema — после хендлеров и best-effort: сбой БД на старте не должен ронять
    # register и глушить кнопки маршрута (та же защита, что в driver_checklist).
    try:
        ensure_schema(db)
    except Exception as e:
        logger.exception("route_dispatch.ensure_schema отложено (БД не готова?): %s", e)
    logger.info("route_dispatch: хендлеры зарегистрированы")
