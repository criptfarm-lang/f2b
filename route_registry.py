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
import asyncio
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
            or chat_id in dc._logist_chat_ids())


# ─── Wialon: чтение маршрута ──────────────────────────────────────────────────

async def _wialon_call(session, svc, params, sid=None):
    data = {"svc": svc, "params": json.dumps(params)}
    if sid:
        data["sid"] = sid
    async with session.post(WIALON_BASE, data=data) as r:
        return await r.json(content_type=None)


async def fetch_routes(with_meta: bool = False):
    """Возвращает {unit_id: [stop,...]} по машинам, точки отсортированы по порядку выгрузки.
    stop = {seq, vt, tf, tt, client, address, phone, order_no, oid, ...}.
    with_meta=True → кортеж (routes, order_routes) — order_routes нужен для пробега (mileage_km)."""
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
        item = res.get("item") or {}
        orders = item.get("orders") or {}
        order_routes = item.get("order_routes") or {}

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
            "oid": o.get("uid"),  # уникальный id заявки Wialon — для матча с order_routes.ord
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
    if with_meta:
        return routes, order_routes
    return routes


def mileage_km(order_routes, uid, stop_oids):
    """Плановый пробег маршрута машины (км) из Wialon order_routes.summary.mileage.
    Матчим по пересечению id заявок (order_routes[*].ord = список oid), а не по дате —
    у машины бывает несколько исторических раскладок; берём ту, что реально накрывает
    текущие точки. Возвращает округлённые км или None."""
    if not order_routes:
        return None
    stop_set = {o for o in (stop_oids or []) if o}
    if not stop_set:
        return None
    best, best_ov = None, 0
    routes = order_routes.values() if isinstance(order_routes, dict) else order_routes
    for r in routes:
        if (r.get("st") or {}).get("u") != uid:
            continue
        ov = len(set(r.get("ord") or []) & stop_set)
        if ov > best_ov:
            best_ov, best = ov, r
    if not best:
        return None
    m = (best.get("summary") or {}).get("mileage")
    return round(m / 1000.0, 1) if m else None


# ─── Навигационная ссылка Яндекс.Карт ─────────────────────────────────────────

# База Ильинский (производство, Рабочая ул. 48/8) — старт развоза.
# Координата сверена геокодером DaData 2026-07-29 (совпала с точкой из ручного
# маршрута Беляковой 55.633302,38.104360). (lat, lon).
BASE_ILINSKY = (55.633297, 38.104351)


# Практический лимит точек в ОДНОЙ ссылке Яндекс.Карт. Веб/приложение Карт держит
# ~10 точек (ручные маршруты Беляковой — 10, работали); на большем Яндекс молча режет
# хвост. Больше — дробим на части с перекрытием.
YA_MAX_WAYPOINTS = 10


def _route_points(stops, with_base: bool):
    """Список координат маршрута (lat, lon) по порядку выгрузки. База Ильинский первой
    (with_base). Подряд идущие одинаковые координаты схлопываются — несколько заказов
    одному клиенту (один адрес) не должны занимать несколько точек в ссылке."""
    pts = []
    if with_base:
        pts.append(BASE_ILINSKY)
    for s in stops or []:
        lat, lon = s.get("lat"), s.get("lon")
        if lat in (None, "", 0) or lon in (None, "", 0):
            continue
        c = (round(float(lat), 6), round(float(lon), 6))
        if pts and pts[-1] == c:  # тот же адрес подряд
            continue
        pts.append(c)
    return pts


def _ya_url(pts) -> str:
    rtext = "~".join(f"{lat:.6f},{lon:.6f}" for lat, lon in pts)
    return f"https://yandex.ru/maps/?mode=routes&rtext={rtext}&rtt=auto"


def yandex_route_urls(stops, with_base: bool = True, max_wp: int = YA_MAX_WAYPOINTS):
    """Ссылки на маршрут в Яндекс.Картах (старт база Ильинский → точки по порядку выгрузки).
    Если точек больше лимита Яндекса — дробит на минимально нужное число частей РАВНОМЕРНО
    (напр. 11 точек → 6+6, а не 10+2), с перекрытием в 1 точку между частями, чтобы они
    стыковались (последняя точка части = первая точка следующей). Части идут последовательно.
    Возвращает список URL (обычно 1) или [] если валидных точек < 2."""
    pts = _route_points(stops, with_base)
    n = len(pts)
    if n < 2:
        return []
    if n <= max_wp:
        return [_ya_url(pts)]
    # k = минимум частей: каждая ≤ max_wp точек при перекрытии в 1 точку.
    k = (n - 2) // (max_wp - 1) + 1  # = ceil((n-1)/(max_wp-1))
    edges = n - 1                    # рёбер между точками — делим их поровну по частям
    base, rem = divmod(edges, k)
    urls, start = [], 0
    for i in range(k):
        cnt = base + (1 if i < rem else 0)  # рёбер (следовательно cnt+1 точек) в этой части
        end = start + cnt
        urls.append(_ya_url(pts[start:end + 1]))
        start = end                         # следующая часть стартует с последней точки текущей
    return urls


def yandex_route_url(stops, with_base: bool = True) -> str | None:
    """Первая (обычно единственная) ссылка маршрута — тонкая обёртка над yandex_route_urls."""
    urls = yandex_route_urls(stops, with_base=with_base)
    return urls[0] if urls else None


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

    def _parse(co):
        places = None
        win_from = win_to = ""
        for a in co.get("attributes", []) or []:
            nm = a.get("name")
            if nm == "Количество мест":
                places = a.get("value")
            elif nm == ATTR_WINDOW_FROM:
                win_from = _attr_time(a.get("value"))
            elif nm == ATTR_WINDOW_TO:
                win_to = _attr_time(a.get("value"))
        # вес = сумма количеств позиций (рыба/морепродукты продаются в кг)
        weight = 0.0
        for p in (co.get("positions") or {}).get("rows", []):
            weight += p.get("quantity") or 0
        # менеджер / телефон / «здрасте» из контрагента
        agent = co.get("agent") or {}
        tags = [str(t).strip().lower() for t in (agent.get("tags") or [])]
        manager = next((_MANAGER_TAG_NAMES[t] for t in tags if t in _MANAGER_TAG_NAMES), "")
        return {
            "places": places,
            "comment": (co.get("description") or "").strip(),
            "weight": weight,
            "manager": manager,
            # Телефон приёмки водитель берёт из Комментария заказа (менеджеры вписывают
            # контакт туда). Поле «Телефон» карточки контрагента больше НЕ тянем.
            "win_from": win_from,   # «Окно доставки с (время)» → HH:MM
            "win_to": win_to,       # «Окно доставки до (время)» → HH:MM
            "zdraste": "здрасте" in tags,
        }

    # Параллельно, но с семафором — иначе N одновременных GET упрутся в rate-limit МС.
    sem = asyncio.Semaphore(6)

    async def _one(session, no):
        async with sem:
            try:
                f = urllib.parse.quote(f"name={no}")
                url = (f"{MS_BASE}/entity/customerorder?filter={f}"
                       f"&expand=positions.assortment,agent&limit=1")
                async with session.get(url, headers=headers) as r:
                    rows = (await r.json()).get("rows", []) if r.status == 200 else []
                return no, (_parse(rows[0]) if rows else None)
            except Exception as e:
                logger.warning("_ms_extra_by_order %s: %s", no, e)
                return no, None

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*(_one(session, no) for no in order_numbers))
    for no, data in results:
        if data is not None:
            out[no] = data
    return out


# ─── PDF реестра ─────────────────────────────────────────────────────────────

def _hm(ts):
    if not ts:
        return "—"
    try:
        return datetime.fromtimestamp(ts, _MSK).strftime("%H:%M")
    except Exception:
        return "—"


# Имена доп.полей заказа МС с окном приёмки (тип «дата-время», значащая часть — ВРЕМЯ).
ATTR_WINDOW_FROM = "Окно доставки с (время)"
ATTR_WINDOW_TO = "Окно доставки до (время)"

import re as _re_rr


def _attr_time(val) -> str:
    """«2026-07-29 09:30:00.000» → «09:30». Пусто → ''. Дата в поле ненадёжна, берём только время."""
    if not val:
        return ""
    s = str(val).strip()
    m = _re_rr.search(r"\b(\d{1,2}):(\d{2})\b", s)
    return f"{int(m.group(1)):02d}:{m.group(2)}" if m else ""


def _fmt_window(win_from: str, win_to: str, tf=None, tt=None) -> str:
    """Окно приёмки для листа: приоритет — поля заказа МС «Окно доставки с/до (время)».
    Обе стороны → «09:00–09:30»; только до → «до 09:30»; только с → «с 14:00 »;
    ни одной → фолбэк на окно заявки Wialon (tf/tt)."""
    wf, wt = (win_from or "").strip(), (win_to or "").strip()
    if wf and wt:
        return f"{wf}–{wt}"
    if wt:
        return f"до {wt}"
    if wf:
        return f"с {wf}"
    return f"{_hm(tf)}–{_hm(tt)}"


# Телефон приёмки менеджеры пишут в Комментарии заказа в разном виде (+7 901…, 8 (901)…).
_PHONE_RE = _re_rr.compile(r"(?:\+7|8|7)[\s\-()]*\d{3}[\s\-()]*\d{3}[\s\-()]*\d{2}[\s\-()]*\d{2}")


def _phone_from_text(text: str) -> str:
    """Первый телефон из текста → нормализованный +7XXXXXXXXXX для tel:. Пусто, если нет."""
    if not text:
        return ""
    m = _PHONE_RE.search(text)
    if not m:
        return ""
    d = _re_rr.sub(r"\D", "", m.group(0))
    if len(d) == 11 and d[0] in "78":
        return "+7" + d[1:]
    return "+" + d if d else ""


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
    sub = ParagraphStyle("sub", fontName=FONT_NORMAL, fontSize=8, leading=11, spaceAfter=4)
    legal = ParagraphStyle("lg", fontName=FONT_NORMAL, fontSize=8, leading=11, spaceBefore=4)

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
        flow.append(Paragraph(
            "Приёмо-сдаточный реестр к договору о материальной ответственности "
            "водителя-экспедитора.", sub))
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
            # Ответственный менеджер (из тега контрагента). Телефон приёмки — в Комментарии.
            resp = ex.get("manager") or ""
            meta_line = " · ".join(x for x in (
                (f"Отв: {resp}" if resp else ""),
                ("Здрасте" if ex.get("zdraste") else ""),
            ) if x)
            if meta_line:
                info += f"<br/><font size=7 color='#444444'>{meta_line[:80]}</font>"
            # Комментарий = контакт/условия приёмки (менеджеры пишут телефон сюда).
            cm = (ex.get("comment") or "").replace("\n", " ")
            # Телефон приёмки — из комментария, кликабельной tel:-ссылкой (звонок из PDF).
            tel = _phone_from_text(cm)
            if tel:
                info += (f"<br/><font size=8><a href='tel:{tel}' color='#0645ad'>"
                         f"тел. {tel}</a></font>")
            if cm:
                # Лимит выше (140), чтобы не срезать инструкции/номер. Paragraph переносит текст.
                info += f"<br/><font size=7 color='#888888'>{cm[:140]}</font>"
            link = f"https://t.me/{bot_username}?start=chk_{s['order_no']}"
            rows.append([
                Paragraph(str(idx), cell),
                Paragraph(_hm(s["vt"]), cell),
                Paragraph(_fmt_window(ex.get("win_from"), ex.get("win_to"), s["tf"], s["tt"]), cell),
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

        # Юридический блок: приёмка (подпись) + порядок сдачи (QR = ПЭП)
        flow.append(Spacer(1, 5 * mm))
        flow.append(Paragraph(
            "<b>Приёмка.</b> Груз по реестру принял к перевозке в полном объёме и надлежащем "
            "качестве. Расхождения при приёмке (при наличии): "
            "______________________________________", legal))
        flow.append(Paragraph(
            "Водитель-экспедитор: _____________ / _____________________ "
            "(подпись, дата, время)&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
            "Передал (склад): _____________ / _____________________", legal))
        flow.append(Paragraph(
            "<b>Сдача.</b> По каждой точке подтверждается сканированием QR-кода строки — бот в "
            "мессенджере или веб-страница по ссылке (простая электронная подпись, ст. 5–6 "
            "Федерального закона № 63-ФЗ; фиксируется в системе). При недоступности системы — "
            "подпись водителя на реестре с указанием № точки. Подпись грузополучателя не "
            "требуется.", legal))
        flow.append(Paragraph(
            "<b>Платные дороги.</b> Организация оплачивает только СОГЛАСОВАННЫЕ проезды по "
            "платным дорогам (перечень согласованных маршрутов — у логиста). Несогласованный "
            "проезд водитель оплачивает самостоятельно.", legal))
        flow.append(Paragraph(
            "<b>ПДД.</b> Нарушения Правил дорожного движения (штрафы) — ответственность "
            "водителя.", legal))
        flow.append(Paragraph(
            "<b>Манёвры.</b> Поворот и разворот налево выполнять только с крайней левой "
            "полосы (п. 8.5 ПДД). Нарушение — ответственность водителя.", legal))
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
