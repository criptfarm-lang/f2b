"""
Живая HTML-страница маршрута водителю.
План: F2B второй мозг/plans/2026-07-23-html-маршрут-водителю.md

Водитель открывает ссылку `/route/<uid>/<date>?t=<token>` → всегда актуальный
маршрут своей машины, прочитанный из Wialon в реальном времени (в отличие от
статичного PDF-реестра и снимка `route_dispatch.stops`, которые протухают, если
логист правит раскладку после подтверждения).

Read-only: приёмка на точке НЕ дублируется — кнопка «Открыть сдачу» ведёт
deep-link'ом `t.me/<bot>?start=chk_<№заказа>` в существующий driver_checklist.
"""
import os
import hmac
import hashlib
import logging
import html as _html
import urllib.parse
from datetime import datetime, date, timezone, timedelta

from aiohttp import web

import route_registry as rr
import route_dispatch as rd

logger = logging.getLogger(__name__)
_MSK = timezone(timedelta(hours=3))
_bot_username_cache = None


# ─── Токен (152-ФЗ: страница с адресами/телефонами не может быть публичной) ───

def _secret() -> bytes:
    # Отдельный секрет, иначе — токен бота (он уже в env и надёжен).
    s = os.getenv("ROUTE_LINK_SECRET") or os.getenv("TELEGRAM_BOT_TOKEN") or "f2b-route"
    return s.encode("utf-8")


def make_token(uid, date_str: str) -> str:
    msg = f"{uid}|{date_str}".encode("utf-8")
    return hmac.new(_secret(), msg, hashlib.sha256).hexdigest()[:16]


def verify_token(uid, date_str: str, t: str) -> bool:
    return hmac.compare_digest(make_token(uid, date_str), t or "")


def route_url(uid, date_str: str) -> str:
    """Абсолютная ссылка на живой маршрут (для пуша водителю).
    Базовый URL — из env PUBLIC_BASE_URL, дефолт — внешний домен бота на Amvera."""
    base = (os.getenv("PUBLIC_BASE_URL") or "https://f2b-bot-victor03.amvera.io").rstrip("/")
    return f"{base}/route/{uid}/{date_str}?t={make_token(uid, date_str)}"


# ─── Рендер ──────────────────────────────────────────────────────────────────

def _maps_link(address: str) -> str:
    return "https://yandex.ru/maps/?text=" + urllib.parse.quote(address)


def _e(s) -> str:
    return _html.escape(str(s or ""))


_PAGE_CSS = """
*{box-sizing:border-box}
body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:0;background:#f2f4f7;color:#111}
.wrap{max-width:640px;margin:0 auto;padding:12px}
.head{background:#0d2b45;color:#fff;border-radius:12px;padding:14px 16px;margin-bottom:12px}
.head h1{font-size:18px;margin:0 0 4px}
.head .sub{font-size:13px;opacity:.85}
.head .vol{font-size:13px;margin-top:8px;background:rgba(255,255,255,.12);border-radius:8px;padding:6px 10px}
.pt{background:#fff;border-radius:12px;padding:12px 14px;margin-bottom:10px;box-shadow:0 1px 3px rgba(0,0,0,.06)}
.pt.done{opacity:.5}
.pt .top{display:flex;align-items:baseline;gap:8px}
.pt .num{font-weight:700;font-size:16px;color:#0d2b45;min-width:24px}
.pt .cli{font-weight:700;font-size:15px;flex:1}
.pt .win{font-size:12px;color:#556;white-space:nowrap}
.pt .row{font-size:13px;color:#334;margin-top:4px}
.pt .row a{color:#0a58ca;text-decoration:none}
.pt .meta{font-size:12px;color:#778;margin-top:4px}
.pt .done-badge{color:#1a7f37;font-weight:700;font-size:13px}
.pt .btn{display:inline-block;margin-top:10px;background:#0d2b45;color:#fff;text-decoration:none;
  padding:9px 14px;border-radius:9px;font-size:14px;font-weight:600}
.foot{text-align:center;color:#889;font-size:12px;padding:8px 0 24px}
.empty{background:#fff;border-radius:12px;padding:24px;text-align:center;color:#667}
"""


async def _bot_username(bot) -> str:
    """Имя бота для deep-link'ов. Кешируем ТОЛЬКО успех — иначе транзиентный
    сбой get_me (Telegram-лаг) навсегда убил бы кнопки «Открыть сдачу»."""
    global _bot_username_cache
    if _bot_username_cache:
        return _bot_username_cache
    try:
        me = await bot.get_me()
        if me and me.username:
            _bot_username_cache = me.username
    except Exception as e:
        logger.warning("route_web: get_me не удался: %s", e)
    return _bot_username_cache or ""


async def render_page(uid: int, target: date, db, bot) -> str:
    unit_name = rr.UNITS.get(uid, str(uid))
    date_str = target.strftime("%d.%m.%Y")

    routes = await rr.fetch_routes()
    stops = [s for s in (routes.get(uid) or []) if rd._stop_on_date(s, target)]

    # Имя водителя + закрытые точки
    driver_name = ""
    done_set = set()
    if db is not None:
        try:
            r = db._fetchone("SELECT name FROM drivers WHERE unit_id=%s AND active LIMIT 1", (uid,))
            if r:
                driver_name = r.get("name") or ""
        except Exception as e:
            logger.warning("route_web driver: %s", e)
        try:
            import json
            row = db._fetchone("SELECT done FROM route_dispatch WHERE snap_date=%s AND unit_id=%s",
                               (target.isoformat(), uid))
            if row and row.get("done"):
                d = row["done"] if isinstance(row["done"], list) else json.loads(row["done"])
                done_set = set(d)
        except Exception as e:
            logger.warning("route_web done: %s", e)

    head = (f"<div class='head'><h1>🚚 {_e(unit_name)}</h1>"
            f"<div class='sub'>Водитель: {_e(driver_name or '—')} · {date_str} · "
            f"{len(stops)} точек (порядок выгрузки)</div>")

    if stops:
        ms_extra = await rr._ms_extra_by_order([s["order_no"] for s in stops])
        head += f"<div class='vol'>{_e(rd._volume_note(uid, stops, ms_extra))}</div>"
    head += "</div>"

    if not stops:
        body = "<div class='empty'>Маршрут по этой машине ещё не построен в Логистике.</div>"
        return _wrap(unit_name, head + body)

    username = await _bot_username(bot)
    cards = []
    for i, s in enumerate(stops, 1):
        order_no = s.get("order_no")
        ex = ms_extra.get(order_no, {})
        is_done = rd._doc_no(order_no) in done_set
        cls = "pt done" if is_done else "pt"

        client = s.get("client") or order_no or "?"
        win = f"{rr._hm(s.get('tf'))}–{rr._hm(s.get('tt'))}"
        plan = rr._hm(s.get("vt"))
        address = s.get("address") or ""
        phone = (ex.get("phone") or s.get("phone") or "").strip()
        wt = rr._fmt_weight(ex.get("weight"))
        places = ex.get("places")
        places = str(places) if places not in (None, "") else "—"
        resp = ex.get("manager") or ""
        comment = (ex.get("comment") or "").replace("\n", " ").strip()

        rows = []
        rows.append(f"<div class='top'><span class='num'>{i}.</span>"
                    f"<span class='cli'>{_e(client)}</span>"
                    f"<span class='win'>🕒 {_e(plan)} · {_e(win)}</span></div>")
        if address:
            rows.append(f"<div class='row'>📍 <a href='{_maps_link(address)}' target='_blank'>{_e(address)}</a></div>")
        if phone:
            rows.append(f"<div class='row'>📞 <a href='tel:{_e(phone)}'>{_e(phone)}</a></div>")
        wbits = " · ".join(x for x in ((f"{wt} кг" if wt else ""), f"{places} мест") if x)
        meta = " · ".join(x for x in (wbits, (f"Отв: {_e(resp)}" if resp else "")) if x)
        if meta:
            rows.append(f"<div class='meta'>📦 {meta}</div>")
        if comment:
            rows.append(f"<div class='meta'>💬 {_e(comment)}</div>")
        rows.append(f"<div class='meta'>№ заказа {_e(order_no)}</div>")

        if is_done:
            rows.append("<div class='done-badge'>✅ Сдан</div>")
        elif username:
            rows.append(f"<a class='btn' href='https://t.me/{username}?start=chk_{_e(order_no)}'>Открыть сдачу</a>")

        cards.append(f"<div class='{cls}'>" + "".join(rows) + "</div>")

    body = "".join(cards)
    return _wrap(unit_name, head + body)


def _wrap(title: str, inner: str) -> str:
    return (f"<!doctype html><html lang='ru'><head><meta charset='utf-8'>"
            f"<meta name='viewport' content='width=device-width,initial-scale=1'>"
            f"<meta http-equiv='refresh' content='60'>"
            f"<title>Маршрут {_e(title)}</title><style>{_PAGE_CSS}</style></head>"
            f"<body><div class='wrap'>{inner}"
            f"<div class='foot'>Обновляется автоматически каждую минуту · данные из Логистики</div>"
            f"</div></body></html>")


# ─── aiohttp-хендлер ─────────────────────────────────────────────────────────

def _err(text: str, status: int) -> web.Response:
    return web.Response(
        text=f"<html><body style='font-family:sans-serif;padding:40px;color:#6b7280'>"
             f"<h2>{_html.escape(text)}</h2></body></html>",
        content_type="text/html", charset="utf-8", status=status)


async def handle(request, db, bot) -> web.Response:
    try:
        uid = int(request.match_info["uid"])
        date_str = request.match_info["date"]
        target = date.fromisoformat(date_str)
    except (ValueError, KeyError):
        return _err("Некорректная ссылка", 400)
    if uid not in rr.UNITS:
        return _err("Неизвестная машина", 404)
    if not verify_token(uid, date_str, request.query.get("t", "")):
        return _err("Ссылка недействительна", 403)
    try:
        html_text = await render_page(uid, target, db, bot)
        return web.Response(text=html_text, content_type="text/html", charset="utf-8")
    except Exception as e:
        logger.error("route_web.handle: %s", e, exc_info=True)
        return _err("Не удалось собрать маршрут. Проверь доступ к Логистике.", 500)
