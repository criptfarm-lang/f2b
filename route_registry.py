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
            "has_cid": bool(p.get("cid")),  # cid → заявку создал мост; без cid → ручная в Логистике
            "lat": o.get("y"),
            "lon": o.get("x"),
        })

    # (c) Гард дублей. Мост всегда ставит cid = id заказа МС. Если у № есть мостовая
    # заявка (с cid) на какой-то машине, то ручные копии того же № БЕЗ cid — дубли
    # (риск двойной доставки). Отбрасываем их, оставляя мостовую как источник правды.
    cid_numbers = {s["order_no"] for v in routes.values() for s in v if s.get("has_cid")}
    for uid in routes:
        kept = []
        for s in routes[uid]:
            if (not s.get("has_cid")) and s.get("order_no") in cid_numbers:
                logger.warning("fetch_routes: отброшен дубль-сирота №%s на unit %s (есть мостовая копия)",
                               s.get("order_no"), uid)
                continue
            kept.append(s)
        routes[uid] = kept

    # (a) Фолбэк имени клиента из МС для оставшихся точек без имени ('?') — это ручные
    # заявки логиста без cid (мост имя проставляет всегда), не дубли.
    unnamed = sorted({s["order_no"] for v in routes.values() for s in v
                      if s.get("order_no") and (not s.get("client") or s["client"] == "?")})
    if unnamed:
        try:
            names = await _ms_names_by_order(unnamed)
            for v in routes.values():
                for s in v:
                    if (not s.get("client") or s["client"] == "?") and names.get(s.get("order_no")):
                        s["client"] = names[s["order_no"]]
        except Exception as e:
            logger.warning("fetch_routes: фолбэк имён из МС упал: %s", e)

    # (b) Порядок выгрузки — по времени визита vt (монотонно, надёжно), а не по seq:
    # seq у сирот из разных раскладок коллизирует (несколько seq=0) и сбивает нумерацию.
    # После сортировки перенумеровываем seq 0..N — канон для списка/реестра/LIFO.
    def _order_key(s):
        if s.get("vt") is not None:
            return s["vt"]
        if s.get("tf") is not None:
            return s["tf"]
        return 1 << 62
    for uid in routes:
        routes[uid].sort(key=_order_key)
        for i, s in enumerate(routes[uid]):
            s["seq"] = i
    return routes


# ─── МойСклад: мест/комментарий по № заказа ──────────────────────────────────

# Теги-фамилии менеджеров ОП → отображаемое имя (для колонки «Ответственный»).
_MANAGER_TAG_NAMES = {
    "скляр": "Скляр Инесса",
    "мерзлякова": "Мерзлякова Елена",
    "баласанян": "Баласанян Карина",
    "коликов": "Коликов",
    "дьяченко": "Дьяченко Ирина",
}


def _fmt_weight(w) -> str:
    """5.415 → '5.415', 12.0 → '12'. Товар считается в кг, вес = сумма количеств позиций."""
    if not w:
        return ""
    return f"{w:.3f}".rstrip("0").rstrip(".")


async def _ms_names_by_order(order_numbers) -> dict:
    """{order_no: agent_name} — лёгкий резолв имени клиента по № заказа.
    Нужен для точек, где имя в Wialon пустое ('?'): логист не подписал точку в Логистике."""
    out = {}
    if not order_numbers:
        return out
    headers = get_headers()
    import urllib.parse
    async with aiohttp.ClientSession() as session:
        for no in order_numbers:
            try:
                f = urllib.parse.quote(f"name={no}")
                url = f"{MS_BASE}/entity/customerorder?filter={f}&expand=agent&limit=1"
                async with session.get(url, headers=headers) as r:
                    rows = (await r.json()).get("rows", []) if r.status == 200 else []
                if rows:
                    nm = ((rows[0].get("agent") or {}).get("name") or "").strip()
                    if nm:
                        out[no] = nm
            except Exception as e:
                logger.warning("_ms_names_by_order %s: %s", no, e)
    return out


async def _ms_extra_by_order(order_numbers) -> dict:
    """{order_no: {places, comment, weight, manager, phone, zdraste}} — из заказов МС по номеру.
    weight — суммарный вес (кг = сумма количеств позиций); manager — из тега контрагента;
    phone/zdraste — из карточки контрагента (телефон и флаг-тег «здрасте»)."""
    out = {}
    if not order_numbers:
        return out
    headers = get_headers()
    import urllib.parse
    async with aiohttp.ClientSession() as session:
        for no in order_numbers:
            try:
                f = urllib.parse.quote(f"name={no}")
                url = (f"{MS_BASE}/entity/customerorder?filter={f}"
                       f"&expand=positions.assortment,agent&limit=1")
                async with session.get(url, headers=headers) as r:
                    rows = (await r.json()).get("rows", []) if r.status == 200 else []
                if not rows:
                    continue
                co = rows[0]
                places = None
                for a in co.get("attributes", []) or []:
                    if a.get("name") == "Количество мест":
                        places = a.get("value")
                # вес = сумма количеств позиций (рыба/морепродукты продаются в кг)
                weight = 0.0
                for p in (co.get("positions") or {}).get("rows", []):
                    weight += p.get("quantity") or 0
                # менеджер / телефон / «здрасте» из контрагента
                agent = co.get("agent") or {}
                tags = [str(t).strip().lower() for t in (agent.get("tags") or [])]
                manager = next((_MANAGER_TAG_NAMES[t] for t in tags if t in _MANAGER_TAG_NAMES), "")
                out[no] = {
                    "places": places,
                    "comment": (co.get("description") or "").strip(),
                    "weight": weight,
                    "manager": manager,
                    "phone": (agent.get("phone") or "").strip(),
                    "zdraste": "здрасте" in tags,
                }
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
    # Итерируем по переданным машинам (одна или обе) — позволяет собрать PDF на одну машину.
    for uid in routes:
        name = UNITS.get(uid, str(uid))
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
                  for x in ["#", "План", "Окно", "Клиент / адрес", "Вес / Мест", "QR"]]
        rows = [header]
        for idx, s in enumerate(stops, 1):
            ex = ms_extra.get(s["order_no"], {})
            places = ex.get("places")
            places = str(places) if places not in (None, "") else "—"
            wt = _fmt_weight(ex.get("weight"))
            wcell = (f"<b>{wt} кг</b><br/>" if wt else "") + f"{places} мест"
            info = (f"<b>{s['client'][:40]}</b> (№{s['order_no']})<br/>{(s['address'] or '')[:70]}")
            # Ответственный + телефон (из карточки контрагента)
            resp = ex.get("manager") or ""
            phone = ex.get("phone") or s.get("phone") or ""
            meta_line = " · ".join(x for x in (
                (f"Отв: {resp}" if resp else ""),
                (f"тел: {phone}" if phone else ""),
                ("Здрасте" if ex.get("zdraste") else ""),
            ) if x)
            if meta_line:
                info += f"<br/><font size=7 color='#444444'>{meta_line[:80]}</font>"
            cm = (ex.get("comment") or "").replace("\n", " ")
            if cm:
                info += f"<br/><font size=7 color='#888888'>{cm[:70]}</font>"
            link = f"https://t.me/{bot_username}?start=chk_{s['order_no']}"
            rows.append([
                Paragraph(str(idx), cell),
                Paragraph(_hm(s["vt"]), cell),
                Paragraph(f"{_hm(s['tf'])}–{_hm(s['tt'])}", cell),
                Paragraph(info, cell),
                Paragraph(wcell, cell),
                qr_flow(link),
            ])
        t = Table(rows, colWidths=[7 * mm, 15 * mm, 22 * mm, 99 * mm, 26 * mm, 21 * mm])
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
            wt = _fmt_weight(ex.get("weight"))
            bits = " — " + ", ".join(x for x in (
                (f"{wt} кг" if wt else ""),
                (f"{pl} мест" if pl not in (None, "") else ""),
            ) if x) if (wt or pl not in (None, "")) else ""
            loading.append(f"{idx}. {s['client'][:38]} (№{s['order_no']}){bits}")
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
