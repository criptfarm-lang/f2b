"""
Реестр развоза (Фаза 5 плана 2026-07-14-чеклист-водителя-приёмка-на-точке).

После того как логист (Белякова) раскладывает заказы по машинам в Wialon Logistics,
эта штука читает маршрут (машина + порядок + плановое время) через Wialon Remote API,
подтягивает мест/комментарий из МойСклад по № заказа и собирает:
  - реестр водителю: точки его машины по порядку ВЫГРУЗКИ (1→N) + QR на каждую;
  - лист загрузки складу: те же точки в обратном порядке (LIFO — первый на выгрузку
    грузится последним, к дверям).

Команда /реестр (+ /registry) — PDF (обе машины). Доступ: логист/водители/владелец.

Wialon: ресурс заявок 26208, машины 26209 (В 970 СВ 797) / 26210 (К 459 ХК 797).
Порядок точки — p.r.i, плановое время — p.r.vt, № заказа МС — поле n.
"""

import os
import io
import json
import logging
from datetime import datetime, timezone, timedelta

import aiohttp
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

from moysklad import MS_BASE, get_headers

logger = logging.getLogger(__name__)
_MSK = timezone(timedelta(hours=3))

WIALON_BASE = "https://app.gpsnetwork.ru/wialon/ajax.html"
RESOURCE_ID = 26208
UNITS = {26209: "В 970 СВ 797", 26210: "К 459 ХК 797"}


# ─── Доступ (переиспользуем whitelist из driver_checklist) ───────────────────

def _allowed(chat_id: int) -> bool:
    import driver_checklist as dc
    return (dc._is_driver(chat_id)
            or chat_id == dc._owner_chat_id()
            or chat_id == dc._logist_chat_id())


# ─── Wialon: чтение маршрута ──────────────────────────────────────────────────

async def _wialon_call(session, svc, params, sid=None):
    data = {"svc": svc, "params": json.dumps(params)}
    if sid:
        data["sid"] = sid
    async with session.post(WIALON_BASE, data=data) as r:
        return await r.json(content_type=None)


async def fetch_routes() -> dict:
    """Возвращает {unit_id: [stop,...]} по машинам, точки отсортированы по порядку выгрузки.
    stop = {seq, vt, tf, tt, client, address, phone, order_no}."""
    token = os.getenv("WIALON_TOKEN")
    if not token:
        raise RuntimeError("WIALON_TOKEN не задан")
    async with aiohttp.ClientSession() as session:
        login = await _wialon_call(session, "token/login", {"token": token})
        sid = login.get("eid")
        if not sid:
            raise RuntimeError(f"Wialon login fail: {login}")
        res = await _wialon_call(session, "core/search_item",
                                 {"id": RESOURCE_ID, "flags": 0xffffff}, sid)
        orders = (res.get("item") or {}).get("orders") or {}

    routes = {uid: [] for uid in UNITS}
    for o in orders.values():
        uid = o.get("u")
        if uid not in UNITS:
            continue
        p = o.get("p") or {}
        r = p.get("r") or {}
        routes[uid].append({
            "seq": r.get("i"),
            "vt": r.get("vt"),
            "tf": o.get("tf"),
            "tt": o.get("tt"),
            "client": p.get("n") or "?",
            "address": p.get("a") or "",
            "phone": p.get("p") or "",
            "order_no": o.get("n"),
            "lat": o.get("y"),
            "lon": o.get("x"),
        })
    for uid in routes:
        routes[uid].sort(key=lambda s: (s["seq"] if s["seq"] is not None else 999))
    return routes


# ─── МойСклад: мест/комментарий по № заказа ──────────────────────────────────

async def _ms_extra_by_order(order_numbers) -> dict:
    """{order_no: {places, comment}} — из заказов МС по номеру."""
    out = {}
    if not order_numbers:
        return out
    headers = get_headers()
    import urllib.parse
    async with aiohttp.ClientSession() as session:
        for no in order_numbers:
            try:
                f = urllib.parse.quote(f"name={no}")
                url = f"{MS_BASE}/entity/customerorder?filter={f}&limit=1"
                async with session.get(url, headers=headers) as r:
                    rows = (await r.json()).get("rows", []) if r.status == 200 else []
                if not rows:
                    continue
                co = rows[0]
                places = None
                for a in co.get("attributes", []) or []:
                    if a.get("name") == "Количество мест":
                        places = a.get("value")
                out[no] = {"places": places, "comment": (co.get("description") or "").strip()}
            except Exception as e:
                logger.warning("_ms_extra_by_order %s: %s", no, e)
    return out


# ─── PDF реестра ─────────────────────────────────────────────────────────────

def _hm(ts):
    if not ts:
        return "—"
    try:
        return datetime.fromtimestamp(ts, _MSK).strftime("%H:%M")
    except Exception:
        return "—"


def _build_registry_pdf(routes, ms_extra, bot_username, date_str) -> bytes:
    from contract_generator import FONT_NORMAL, FONT_BOLD
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle, Paragraph,
                                    Spacer, PageBreak)
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.graphics.barcode.qr import QrCodeWidget
    from reportlab.graphics.shapes import Drawing

    def qr_flow(data, size=20 * mm):
        qr = QrCodeWidget(data)
        b = qr.getBounds()
        w = b[2] - b[0]
        h = b[3] - b[1]
        d = Drawing(size, size, transform=[size / w, 0, 0, size / h, 0, 0])
        d.add(qr)
        return d

    cell = ParagraphStyle("c", fontName=FONT_NORMAL, fontSize=8, leading=10)
    h2 = ParagraphStyle("h2", fontName=FONT_BOLD, fontSize=12, leading=15, spaceBefore=6, spaceAfter=4)
    title = ParagraphStyle("t", fontName=FONT_BOLD, fontSize=14, leading=18, spaceAfter=6)
    small = ParagraphStyle("s", fontName=FONT_NORMAL, fontSize=8, leading=11)
    load = ParagraphStyle("l", fontName=FONT_BOLD, fontSize=9, leading=13)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=12 * mm, bottomMargin=10 * mm,
                            leftMargin=10 * mm, rightMargin=10 * mm)
    flow = []
    first = True
    for uid, name in UNITS.items():
        stops = routes.get(uid) or []
        if not first:
            flow.append(PageBreak())
        first = False
        flow.append(Paragraph(f"Реестр развоза — {date_str}", title))
        flow.append(Paragraph(f"Машина {name} — {len(stops)} точек (порядок выгрузки)", h2))
        if not stops:
            flow.append(Paragraph("Маршрут по этой машине не построен.", small))
            continue

        header = [Paragraph(x, ParagraphStyle("hd", fontName=FONT_BOLD, fontSize=8, leading=10))
                  for x in ["#", "План", "Окно", "Клиент / адрес", "Мест", "QR"]]
        rows = [header]
        for idx, s in enumerate(stops, 1):
            ex = ms_extra.get(s["order_no"], {})
            places = ex.get("places")
            places = str(places) if places not in (None, "") else "—"
            info = (f"<b>{s['client'][:40]}</b> (№{s['order_no']})<br/>{(s['address'] or '')[:70]}")
            cm = (ex.get("comment") or "").replace("\n", " ")
            if cm:
                info += f"<br/><font size=7 color='#666666'>{cm[:70]}</font>"
            link = f"https://t.me/{bot_username}?start=chk_{s['order_no']}"
            rows.append([
                Paragraph(str(idx), cell),
                Paragraph(_hm(s["vt"]), cell),
                Paragraph(f"{_hm(s['tf'])}–{_hm(s['tt'])}", cell),
                Paragraph(info, cell),
                Paragraph(places, cell),
                qr_flow(link),
            ])
        t = Table(rows, colWidths=[7 * mm, 15 * mm, 22 * mm, 111 * mm, 14 * mm, 21 * mm])
        t.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#cccccc")),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eef2f5")),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        flow.append(t)

        # Лист загрузки (LIFO): грузим с последней точки, первая — к дверям
        flow.append(Spacer(1, 4 * mm))
        flow.append(Paragraph("Лист загрузки (грузить в этом порядке — первая точка к дверям):", load))
        loading = []
        for idx in range(len(stops), 0, -1):
            s = stops[idx - 1]
            ex = ms_extra.get(s["order_no"], {})
            pl = ex.get("places")
            pl = f" — {pl} мест" if pl not in (None, "") else ""
            loading.append(f"{idx}. {s['client'][:38]} (№{s['order_no']}){pl}")
        flow.append(Paragraph("<br/>".join(loading), small))
    doc.build(flow)
    return buf.getvalue()


# ─── Команда ─────────────────────────────────────────────────────────────────

async def cmd_registry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    if not _allowed(user.id):
        await update.message.reply_text("⛔ Реестр доступен логисту и водителям.")
        return
    await update.message.reply_text("Собираю реестр развоза из Wialon…")
    try:
        routes = await fetch_routes()
        total = sum(len(v) for v in routes.values())
        if total == 0:
            await update.message.reply_text(
                "Маршрут ещё не построен — логист не разложил заказы по машинам в Wialon.")
            return
        order_numbers = [s["order_no"] for v in routes.values() for s in v]
        ms_extra = await _ms_extra_by_order(order_numbers)
        me = await context.bot.get_me()
        now = datetime.now(_MSK)
        pdf = _build_registry_pdf(routes, ms_extra, me.username, now.strftime("%d.%m.%Y"))
        parts = " / ".join(f"{UNITS[u]}: {len(routes[u])}" for u in UNITS)
        await context.bot.send_document(
            chat_id=user.id, document=io.BytesIO(pdf),
            filename=f"reestr_{now.strftime('%Y-%m-%d')}.pdf",
            caption=(f"Реестр развоза {now.strftime('%d.%m.%Y')} — {parts}.\n"
                     "У каждой машины: порядок выгрузки (сверху) + лист загрузки LIFO (снизу). "
                     "QR по каждой точке → скан открывает сдачу груза."),
        )
    except Exception as e:
        logger.exception("cmd_registry: %s", e)
        await update.message.reply_text("Не удалось собрать реестр. Проверь WIALON_TOKEN / доступ.")


def register(app: Application):
    app.add_handler(CommandHandler("registry", cmd_registry))
    app.add_handler(MessageHandler(filters.Regex(r"^/реестр(@\w+)?(\s|$)"), cmd_registry))
    logger.info("route_registry: хендлеры зарегистрированы")
