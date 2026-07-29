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
import re
import time
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


# Телефон приёмки менеджеры пишут в Комментарии заказа в разном виде (+7 901…, 8 (901)…).
_PHONE_RE = re.compile(r"(?:\+7|8|7)[\s\-()]*\d{3}[\s\-()]*\d{3}[\s\-()]*\d{2}[\s\-()]*\d{2}")


def _phone_from_text(text: str) -> str:
    """Первый телефон из текста → нормализованный +7XXXXXXXXXX для tel:-ссылки. Пусто, если нет."""
    if not text:
        return ""
    m = _PHONE_RE.search(text)
    if not m:
        return ""
    digits = re.sub(r"\D", "", m.group(0))
    if len(digits) == 11 and digits[0] in "78":
        return "+7" + digits[1:]
    return "+" + digits if digits else ""


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


_BOT_USERNAME_DEFAULT = "F2B_assistant_bot"


async def _bot_username(bot) -> str:
    """Имя бота для deep-link'ов «Открыть сдачу».

    НЕ полагаемся на get_me в момент запроса — он флапает ровно из-за той же
    деградации Telegram API, что и весь инцидент 22.07 (get_me таймаутит →
    кнопки пропадают). Цепочка: env BOT_USERNAME → успешный get_me (кешируем
    только успех) → статичный дефолт. Имя бота стабильно, дефолт безопасен."""
    global _bot_username_cache
    env = os.getenv("BOT_USERNAME")
    if env:
        return env.lstrip("@")
    if _bot_username_cache:
        return _bot_username_cache
    try:
        me = await bot.get_me()
        if me and me.username:
            _bot_username_cache = me.username
            return _bot_username_cache
    except Exception as e:
        logger.warning("route_web: get_me не удался, беру дефолт: %s", e)
    return _BOT_USERNAME_DEFAULT


_ROUTE_CACHE = {}   # (uid, date_iso) -> (mono_ts, stops, ms_extra, order_routes)
_ROUTE_TTL = 45     # сек: авто-refresh и повторные заходы бьют кэш, а не Wialon+МС


async def _route_data(uid: int, target: date):
    """(stops, ms_extra, order_routes) с коротким TTL-кэшем — снимает тяжёлый живой
    фетч Wialon+МС при авто-обновлении и повторных заходах. Закрытость точек (done) и
    имя водителя берутся мимо кэша (дёшево, из БД) → статус сдачи всегда свежий."""
    key = (uid, target.isoformat())
    now = time.monotonic()
    hit = _ROUTE_CACHE.get(key)
    if hit and (now - hit[0]) < _ROUTE_TTL:
        return hit[1], hit[2], hit[3]
    routes, order_routes = await rr.fetch_routes(with_meta=True)
    stops = [s for s in (routes.get(uid) or []) if rd._stop_on_date(s, target)]
    ms_extra = await rr._ms_extra_by_order([s["order_no"] for s in stops]) if stops else {}
    _ROUTE_CACHE[key] = (now, stops, ms_extra, order_routes)
    return stops, ms_extra, order_routes


async def render_page(uid: int, target: date, db, bot) -> str:
    unit_name = rr.UNITS.get(uid, str(uid))
    date_str = target.strftime("%d.%m.%Y")
    date_iso = target.isoformat()
    token = make_token(uid, date_iso)

    stops, ms_extra, order_routes = await _route_data(uid, target)

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
        km = rr.mileage_km(order_routes, uid, [s.get("oid") for s in stops])
        head += f"<div class='vol'>{_e(rd._volume_note(uid, stops, ms_extra, km=km))}</div>"
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
        # Окно приёмки — из полей заказа МС «Окно доставки с/до (время)», фолбэк на заявку.
        win = rr._fmt_window(ex.get("win_from"), ex.get("win_to"), s.get("tf"), s.get("tt"))
        plan = rr._hm(s.get("vt"))
        address = s.get("address") or ""
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
        # Телефон приёмки — из Комментария заказа (менеджеры вписывают контакт туда),
        # карточку контрагента больше не читаем. Номер из комментария делаем кликабельным.
        tel = _phone_from_text(comment)
        if tel:
            rows.append(f"<div class='row'>📞 <a href='tel:{_e(tel)}'>{_e(tel)}</a></div>")
        wbits = " · ".join(x for x in ((f"{wt} кг" if wt else ""), f"{places} мест") if x)
        meta = " · ".join(x for x in (wbits, (f"Отв: {_e(resp)}" if resp else "")) if x)
        if meta:
            rows.append(f"<div class='meta'>📦 {meta}</div>")
        if comment:
            rows.append(f"<div class='meta'>💬 {_e(comment)}</div>")
        rows.append(f"<div class='meta'>№ заказа {_e(order_no)}</div>")

        if is_done:
            rows.append("<div class='done-badge'>✅ Сдан</div>")
        else:
            is_retail = (client or "").startswith("Рознич")
            money_q = ("<div class='q'>💵 Деньги приняты?"
                       "<label><input type='radio' name='money' value='yes'>Да</label>"
                       "<label><input type='radio' name='money' value='no'>Нет</label></div>") if is_retail else ""
            bot_link = (f"<a class='blink' href='https://t.me/{username}?start=chk_{_e(order_no)}'>или через бот</a>"
                        if username else "")
            rows.append(
                f"<form class='sd' data-uid='{uid}' data-date='{date_iso}' "
                f"data-order='{_e(order_no)}' data-token='{_e(token)}'>"
                f"{money_q}"
                "<div class='q'>✍️ Документ подписан?"
                "<label><input type='radio' name='doc' value='yes'>Да</label>"
                "<label><input type='radio' name='doc' value='no'>Нет</label></div>"
                "<div class='q'>📦 Итог:"
                "<label><input type='radio' name='accepted' value='ok'>Сдано</label>"
                "<label><input type='radio' name='accepted' value='claim'>Претензия</label></div>"
                "<textarea name='claim_text' class='claim' placeholder='Опиши претензию' hidden></textarea>"
                "<button type='button' class='btn' onclick='sendSd(this.form)'>Отправить сдачу</button>"
                f"{bot_link}<div class='msg'></div></form>")

        cards.append(f"<div class='{cls}'>" + "".join(rows) + "</div>")

    body = "".join(cards)
    return _wrap(unit_name, head + body)


_FORM_CSS = (".sd{margin-top:10px;padding-top:8px;border-top:1px solid #eee}"
             ".q{margin:6px 0;font-size:15px}.q label{margin-left:10px;white-space:nowrap}"
             ".claim{width:100%;box-sizing:border-box;margin:6px 0;min-height:54px}"
             ".msg{color:#b91c1c;font-size:14px;margin-top:6px}"
             ".blink{display:inline-block;margin-left:12px;color:#6b7280;font-size:13px}")

_FORM_JS = (
    "function sendSd(f){var acc=(f.accepted&&f.accepted.value)||'';"
    "if(!acc){alert('Отметь: Сдано или Претензия');return;}"
    "var body={order_no:f.dataset.order,items:{}};"
    "new FormData(f).forEach(function(v,k){if(k.indexOf('item_')===0){body.items[k.slice(5)]=v;}else{body[k]=v;}});"
    "if(acc==='claim'&&!((body.claim_text||'').trim())){alert('Опиши претензию');return;}"
    "var url='/route/'+f.dataset.uid+'/'+f.dataset.date+'/submit?t='+encodeURIComponent(f.dataset.token);"
    "var btn=f.querySelector('button');btn.disabled=true;btn.textContent='Отправляю…';"
    "function fail(t){f.querySelector('.msg').textContent=t;btn.disabled=false;btn.textContent='Отправить сдачу';}"
    "fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})"
    ".then(function(r){return r.text().then(function(txt){return{status:r.status,txt:txt};});})"
    ".then(function(res){var d=null;try{d=JSON.parse(res.txt);}catch(_){}"
    "if(d&&d.ok){var c=f.closest('.pt');if(c)c.className='pt done';"
    "f.outerHTML=\"<div class='done-badge'>\"+(d.msg||'✅ Закрыто')+\"</div>\";}"
    "else if(d){fail(d.msg||('Ошибка '+res.status));}"
    "else{fail('HTTP '+res.status+': '+((res.txt||'нет ответа').replace(/<[^>]*>/g,' ').trim().slice(0,140)));}})"
    ".catch(function(e){fail('Сбой запроса: '+((e&&e.message)||e));});}"
    "document.addEventListener('change',function(e){if(e.target&&e.target.name==='accepted'){"
    "var t=e.target.form.querySelector('.claim');if(t)t.hidden=(e.target.value!=='claim');}});")


def _wrap(title: str, inner: str) -> str:
    return (f"<!doctype html><html lang='ru'><head><meta charset='utf-8'>"
            f"<meta name='viewport' content='width=device-width,initial-scale=1'>"
            f"<title>Маршрут {_e(title)}</title><style>{_PAGE_CSS}{_FORM_CSS}</style></head>"
            f"<body><div class='wrap'>{inner}"
            f"<div class='foot'>Сдача точки — прямо здесь. Обнови страницу, если логист поменял маршрут.</div>"
            f"</div><script>{_FORM_JS}</script></body></html>")


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


async def handle_submit(request, db, bot) -> web.Response:
    """POST-приёмка точки с веб-страницы (мимо Telegram). Тот же HMAC-токен, что на GET."""
    try:
        uid = int(request.match_info["uid"])
        date_str = request.match_info["date"]
        date.fromisoformat(date_str)
    except (ValueError, KeyError):
        return web.json_response({"ok": False, "msg": "Некорректная ссылка"}, status=400)
    if uid not in rr.UNITS:
        return web.json_response({"ok": False, "msg": "Неизвестная машина"}, status=404)
    if not verify_token(uid, date_str, request.query.get("t", "")):
        return web.json_response({"ok": False, "msg": "Ссылка недействительна"}, status=403)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "msg": "Некорректные данные"}, status=400)
    order_no = str(body.get("order_no") or "").strip()
    if not order_no:
        return web.json_response({"ok": False, "msg": "Не указана точка"}, status=400)
    try:
        import driver_checklist as dc
        ok, msg = await dc.web_submit(
            db, bot, uid, order_no,
            money=body.get("money"), doc=body.get("doc"),
            items=body.get("items") or {}, accepted=(body.get("accepted") or ""),
            claim_text=body.get("claim_text") or "")
        return web.json_response({"ok": ok, "msg": msg})
    except Exception as e:
        logger.error("route_web.handle_submit: %s", e, exc_info=True)
        return web.json_response({"ok": False, "msg": "Ошибка сохранения, повтори"}, status=500)
