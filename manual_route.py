"""
Аварийная раскладка развоза без Wialon.

План: F2B второй мозг/plans/2026-09-09-аварийная-раскладка-без-wialon.md

Повод: 08.09.2026 провайдер GPS Network закрыл доступ к Wialon (и диспетчерская, и API
отвечают ACCESS_DENIED_BY_SITENAME) → route_registry.fetch_routes падает, водителям не
уходит реестр, статусы отгрузок в МойСклад не меняются.

Wialon в цепочке отвечал ровно за одно — «какой заказ на какой машине и в каком порядке».
Здесь это вводит логист на веб-странице /dispatch/<дата>, а раскладка отдаётся в формате
route_registry.fetch_routes. Ниже по течению не меняется ничего: /маршруты, подтверждение,
PDF складу, ссылка водителю и веб-приёмка работают как раньше.
"""
import os
import re
import json
import time
import hmac
import hashlib
import logging
import asyncio
import urllib.parse
import html as _html_mod
from datetime import datetime, date, timedelta, timezone

import aiohttp
from aiohttp import web
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

import route_registry as rr
from moysklad import MS_BASE, get_headers

logger = logging.getLogger(__name__)
_MSK = timezone(timedelta(hours=3))
_DB = None

# Статусы заказа, при которых точка в развоз не идёт. «Отгружен» здесь НЕТ намеренно:
# в день доставки это оформленный документ, а не уехавший товар (та же логика, что в
# мосте f2b-logistics-bridge/config.py::ORDER_STATE_SHIPPED).
STATES_SKIP = {"Отменен", "Возврат", "НЕ СОГЛАСОВАН", "ЗА ЛИМИТОМ", "На согласовании"}

_CACHE = {}          # day.isoformat() -> (mono_ts, [order,...])
_CACHE_TTL = 60      # сек


# ─── Схема ───────────────────────────────────────────────────────────────────

def ensure_schema(db):
    db._execute("""
        CREATE TABLE IF NOT EXISTS manual_route_stops (
            day        DATE,
            order_no   TEXT,
            unit_id    BIGINT,
            seq        INT,
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (day, order_no)
        )
    """)
    logger.info("manual_route: схема готова")


def set_db(db):
    global _DB
    _DB = db


# ─── Токен страницы (152-ФЗ: адреса и телефоны клиентов не отдаём публично) ───

def _secret() -> bytes:
    s = os.getenv("ROUTE_LINK_SECRET") or os.getenv("TELEGRAM_BOT_TOKEN") or "f2b-route"
    return s.encode("utf-8")


def make_token(date_str: str) -> str:
    msg = f"dispatch|{date_str}".encode("utf-8")
    return hmac.new(_secret(), msg, hashlib.sha256).hexdigest()[:16]


def verify_token(date_str: str, t: str) -> bool:
    return hmac.compare_digest(make_token(date_str), t or "")


def dispatch_url(date_str: str) -> str:
    base = (os.getenv("PUBLIC_BASE_URL") or "https://f2b-bot-victor03.amvera.io").rstrip("/")
    return f"{base}/dispatch/{date_str}?t={make_token(date_str)}"


# ─── Заказы дня из МойСклад ──────────────────────────────────────────────────

def _attr(order: dict, name: str):
    for a in order.get("attributes", []) or []:
        if a.get("name") == name:
            return a.get("value")
    return None


def _parse_ms_dt(s):
    if not s:
        return None
    try:
        return datetime.strptime(str(s).split(".")[0], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _is_selfpickup(addr) -> bool:
    """Самовывоз менеджеры помечают словом «самовывоз» в адресе доставки — такой заказ
    машина не везёт (правило моста f2b-logistics-bridge::is_selfpickup)."""
    return "самовыв" in (addr or "").lower()


async def ms_orders_for_day(day: date, force: bool = False) -> list:
    """Заказы МС с плановой доставкой в этот день — кандидаты в раскладку.

    Фильтруем по deliveryPlannedMoment прямо в API (день = 1 запрос), а не тянем 600
    свежих заказов пагинацией, как мост: страница логиста дёргается часто.
    """
    key = day.isoformat()
    hit = _CACHE.get(key)
    if hit and not force and (time.monotonic() - hit[0]) < _CACHE_TTL:
        return hit[1]

    f = (f"deliveryPlannedMoment>={key} 00:00:00;"
         f"deliveryPlannedMoment<={key} 23:59:59")
    rows = []
    async with aiohttp.ClientSession(headers=get_headers()) as session:
        offset = 0
        while True:
            url = (f"{MS_BASE}/entity/customerorder?limit=100&offset={offset}"
                   f"&expand=agent,state&filter={urllib.parse.quote(f, safe='')}")
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:200]
                    raise RuntimeError(f"МойСклад HTTP {resp.status}: {body}")
                page = (await resp.json()).get("rows", [])
            rows.extend(page)
            if len(page) < 100:
                break
            offset += 100

    out = []
    for o in rows:
        addr = o.get("shipmentAddress")
        if _is_selfpickup(addr):
            continue
        state = (o.get("state") or {}).get("name") or ""
        if state in STATES_SKIP:
            continue
        planned = _parse_ms_dt(o.get("deliveryPlannedMoment"))
        out.append({
            "order_no": o.get("name"),
            "client": (o.get("agent") or {}).get("name") or "?",
            "address": addr or "",
            "state": state,
            "planned": planned,
            # Из доп.полей берём только ВРЕМЯ суток: дата в окне у менеджеров часто
            # остаётся от прошлой правки (готча моста, кейс 03151).
            "win_from": rr._attr_time(_attr(o, rr.ATTR_WINDOW_FROM)),
            "win_to": rr._attr_time(_attr(o, rr.ATTR_WINDOW_TO)),
            "comment": ((o.get("shipmentAddressFull") or {}).get("comment") or "").strip(),
            "sum_rub": (o.get("sum") or 0) / 100.0,
        })
    out.sort(key=lambda r: (r["win_from"] or "99:99", r["order_no"] or ""))
    _CACHE[key] = (time.monotonic(), out)
    return out


# ─── Назначения логиста ──────────────────────────────────────────────────────

def assignments(day: date) -> dict:
    """{order_no: {"unit_id": int, "seq": int}} — что логист расставил на этот день."""
    if _DB is None:
        return {}
    try:
        rows = _DB._fetchall(
            "SELECT order_no, unit_id, seq FROM manual_route_stops WHERE day=%s", (day,))
    except Exception as e:
        logger.warning("manual_route.assignments: %s", e)
        return {}
    return {r["order_no"]: {"unit_id": int(r["unit_id"]), "seq": r.get("seq") or 0}
            for r in rows or []}


def save_assignments(day: date, mapping: dict) -> int:
    """mapping = {order_no: (unit_id|None, seq)}. unit_id None → точка снимается с раскладки."""
    if _DB is None:
        return 0
    n = 0
    for order_no, (unit_id, seq) in mapping.items():
        try:
            if unit_id is None:
                _DB._execute("DELETE FROM manual_route_stops WHERE day=%s AND order_no=%s",
                             (day, order_no))
            else:
                _DB._execute("""
                    INSERT INTO manual_route_stops (day, order_no, unit_id, seq, updated_at)
                    VALUES (%s, %s, %s, %s, now())
                    ON CONFLICT (day, order_no) DO UPDATE SET
                      unit_id = EXCLUDED.unit_id, seq = EXCLUDED.seq, updated_at = now()
                """, (day, order_no, unit_id, seq))
                n += 1
        except Exception as e:
            logger.warning("manual_route.save_assignments %s: %s", order_no, e)
    return n


def days_with_assignments(days) -> set:
    """Из набора дней — те, где раскладка вообще заведена (дешёвая проверка одним запросом)."""
    if _DB is None or not days:
        return set()
    try:
        rows = _DB._fetchall(
            "SELECT DISTINCT day FROM manual_route_stops WHERE day = ANY(%s)", (list(days),))
    except Exception as e:
        logger.warning("manual_route.days_with_assignments: %s", e)
        return set()
    return {r["day"] for r in rows or []}


# ─── Сборка точек в формате route_registry ───────────────────────────────────

def _ts(day: date, hm: str, fallback: datetime = None):
    """«09:30» на день day → unix ts (МСК). Пусто → fallback (planned) → полдень дня."""
    m = re.match(r"^(\d{1,2}):(\d{2})$", hm or "")
    if m:
        dt = datetime(day.year, day.month, day.day, int(m.group(1)), int(m.group(2)), tzinfo=_MSK)
    elif fallback:
        dt = fallback.replace(tzinfo=_MSK)
    else:
        dt = datetime(day.year, day.month, day.day, 12, 0, tzinfo=_MSK)
    return int(dt.timestamp())


async def build_routes(day: date) -> dict:
    """{unit_id: [stop,...]} за день из ручной раскладки. Формат точки — как в
    rr.fetch_routes: seq/vt/tf/tt/client/address/phone/order_no/oid/has_cid/lat/lon.

    vt (плановое время визита) не ставим: его считал Wialon, а синтетическое время
    водителя обманывает. Порядок точек — тот, что задал логист (seq), поэтому ручные
    маршруты собираются здесь целиком и мимо сортировки fetch_routes по vt/tf.
    """
    assign = assignments(day)
    if not assign:
        return {}
    orders = {o["order_no"]: o for o in await ms_orders_for_day(day)}
    routes = {uid: [] for uid in rr.UNITS}
    for order_no, a in assign.items():
        uid = a["unit_id"]
        if uid not in routes:
            continue
        o = orders.get(order_no)
        if not o:
            # Заказ отменили/перенесли после раскладки — точку не выдаём, но след оставляем.
            logger.info("manual_route: №%s из раскладки %s больше не в заказах дня", order_no, day)
            continue
        routes[uid].append({
            "seq": a["seq"],
            "vt": None,
            "tf": _ts(day, o["win_from"], o["planned"]),
            "tt": _ts(day, o["win_to"], o["planned"]),
            "client": o["client"],
            "address": o["address"],
            "phone": "",
            "order_no": order_no,
            "oid": None,
            # Ручные точки помечаем «мостовыми», чтобы гарды дублей fetch_routes (они
            # ищут сирот без cid) их не отбрасывали при возврате Wialon.
            "has_cid": True,
            "lat": None,
            "lon": None,
            "manual": True,
        })
    for uid in routes:
        routes[uid].sort(key=lambda s: (s.get("seq") if s.get("seq") is not None else 999,
                                        s.get("tf") or 0))
        for i, s in enumerate(routes[uid]):
            s["seq"] = i
    return routes


async def build_routes_window(days=None) -> dict:
    """Раскладка за окно дней (вчера…послезавтра) — fetch_routes отдаёт все дни разом,
    а потребители сами фильтруют точки по своей дате."""
    today = datetime.now(_MSK).date()
    days = days or [today + timedelta(days=d) for d in (-1, 0, 1, 2)]
    days = [d for d in days if d in days_with_assignments(days)]
    routes = {uid: [] for uid in rr.UNITS}
    for d in sorted(days):
        try:
            part = await build_routes(d)
        except Exception as e:
            logger.warning("manual_route.build_routes_window %s: %s", d, e)
            continue
        for uid, stops in part.items():
            routes.setdefault(uid, []).extend(stops)
    return routes


# ─── Веб-страница логиста ────────────────────────────────────────────────────

def _e(s) -> str:
    return _html_mod.escape(str(s or ""))


_PAGE_CSS = """
*{box-sizing:border-box}
body{margin:0;font:15px/1.4 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
     background:#f2f4f7;color:#14202e}
header{position:sticky;top:0;z-index:5;background:#0b3d5c;color:#fff;padding:12px 14px;
       box-shadow:0 2px 8px rgba(0,0,0,.18)}
header h1{margin:0;font-size:17px}
header .sub{opacity:.85;font-size:13px;margin-top:3px}
.wrap{padding:12px 10px 100px}
.note{background:#fff6e5;border:1px solid #f0d08a;border-radius:10px;padding:10px 12px;
      margin-bottom:12px;font-size:13px}
.card{background:#fff;border-radius:12px;padding:12px;margin-bottom:10px;
      box-shadow:0 1px 3px rgba(16,32,48,.10)}
.card.on{border-left:5px solid #1d7a46}
.top{display:flex;justify-content:space-between;gap:10px;align-items:baseline}
.no{font-weight:700}
.client{font-weight:600;margin:3px 0}
.addr{color:#4a5b6d;font-size:13px}
.meta{color:#6b7b8c;font-size:12px;margin-top:4px}
.row{display:flex;gap:8px;margin-top:10px}
select,input[type=number]{font-size:16px;padding:9px;border:1px solid #c7d2dd;border-radius:9px;
     background:#fff;color:#14202e}
select{flex:1}
input[type=number]{width:86px}
.bar{position:fixed;left:0;right:0;bottom:0;background:#fff;border-top:1px solid #dde4ec;
     padding:10px 12px;display:flex;gap:10px;align-items:center;
     box-shadow:0 -2px 10px rgba(16,32,48,.10)}
.bar .sum{flex:1;font-size:13px;color:#4a5b6d}
button{font-size:16px;font-weight:600;padding:12px 18px;border:0;border-radius:10px;
       background:#1d7a46;color:#fff}
.ok{background:#e6f5ec;border:1px solid #9ed0b3;border-radius:10px;padding:10px 12px;
    margin-bottom:12px;font-size:14px}
"""


def _card(o, cur, units) -> str:
    no = o["order_no"]
    sel = cur.get(no) or {}
    uid_cur = sel.get("unit_id")
    seq_cur = sel.get("seq")
    opts = ['<option value="">— не везём —</option>']
    for uid, name in units.items():
        s = " selected" if uid_cur == uid else ""
        opts.append(f'<option value="{uid}"{s}>{_e(name)}</option>')
    win = rr._fmt_window(o["win_from"], o["win_to"])
    meta = f"{_e(o['state'])} · окно {_e(win)}"
    if o.get("sum_rub"):
        meta += f" · {o['sum_rub']:,.0f} ₽".replace(",", " ")
    cls = "card on" if uid_cur else "card"
    return (
        f'<div class="{cls}">'
        f'<div class="top"><span class="no">{_e(no)}</span></div>'
        f'<div class="client">{_e(o["client"])}</div>'
        f'<div class="addr">{_e(o["address"])}</div>'
        f'<div class="meta">{meta}</div>'
        f'<div class="row">'
        f'<select name="u_{_e(no)}">{"".join(opts)}</select>'
        f'<input type="number" name="s_{_e(no)}" min="1" max="99" placeholder="№"'
        f' value="{"" if seq_cur is None else seq_cur + 1}">'
        f'</div></div>'
    )


async def render_page(day: date, saved: int = None) -> str:
    orders = await ms_orders_for_day(day)
    cur = assignments(day)
    units = rr.UNITS
    counts = {}
    for a in cur.values():
        counts[a["unit_id"]] = counts.get(a["unit_id"], 0) + 1
    sum_txt = " · ".join(f"{units.get(u, u)}: {n}" for u, n in sorted(counts.items())) or "не расставлено"
    ok = f'<div class="ok">Сохранено: {saved} точек. Дальше в боте: /маршруты → Подтвердить.</div>' if saved is not None else ""
    body = "".join(_card(o, cur, units) for o in orders)
    if not orders:
        body = '<div class="card">На этот день заказов с доставкой нет.</div>'
    return f"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Раскладка {day.strftime('%d.%m')}</title><style>{_PAGE_CSS}</style></head><body>
<header><h1>Раскладка развоза — {day.strftime('%d.%m.%Y')}</h1>
<div class="sub">Заказов на день: {len(orders)}</div></header>
<form method="post" class="wrap">
{ok}
<div class="note">Логистическая программа недоступна — раскладку ведём здесь.
Поставь машину каждой точке и номер по порядку выгрузки (пусто — порядок по окну).
После «Сохранить» в боте: /маршруты → Подтвердить, водителям уйдёт реестр.</div>
{body}
<div class="bar"><span class="sum">{_e(sum_txt)}</span><button type="submit">Сохранить</button></div>
</form></body></html>"""


async def handle(request) -> web.Response:
    try:
        day = date.fromisoformat(request.match_info["date"])
    except (ValueError, KeyError):
        return web.Response(text="Некорректная ссылка", status=400)
    if not verify_token(day.isoformat(), request.query.get("t", "")):
        return web.Response(text="Ссылка недействительна", status=403)
    try:
        html_text = await render_page(day)
    except Exception as e:
        logger.error("manual_route.handle: %s", e, exc_info=True)
        return web.Response(text="Не удалось собрать список заказов, обнови страницу", status=500)
    return web.Response(text=html_text, content_type="text/html", charset="utf-8")


async def handle_save(request) -> web.Response:
    try:
        day = date.fromisoformat(request.match_info["date"])
    except (ValueError, KeyError):
        return web.Response(text="Некорректная ссылка", status=400)
    if not verify_token(day.isoformat(), request.query.get("t", "")):
        return web.Response(text="Ссылка недействительна", status=403)
    data = await request.post()
    # Порядок: что логист проставила руками — как есть; остальным точкам машины
    # раздаём порядок по времени окна (сортировка ms_orders_for_day), продолжая нумерацию.
    picked = {}
    for k, v in data.items():
        if not k.startswith("u_"):
            continue
        order_no = k[2:]
        uid = int(v) if str(v).strip() else None
        seq_raw = str(data.get(f"s_{order_no}") or "").strip()
        seq = int(seq_raw) - 1 if seq_raw.isdigit() and int(seq_raw) > 0 else None
        picked[order_no] = (uid, seq)
    try:
        orders = await ms_orders_for_day(day)
    except Exception:
        orders = []
    order_pos = {o["order_no"]: i for i, o in enumerate(orders)}
    mapping = {no: (None, 0) for no, (uid, _) in picked.items() if uid is None}
    by_unit = {}
    for order_no, (uid, seq) in picked.items():
        if uid is not None:
            by_unit.setdefault(uid, []).append((order_no, seq))
    for uid, items in by_unit.items():
        numbered = [(no, s) for no, s in items if s is not None]
        rest = sorted((no for no, s in items if s is None),
                      key=lambda n: order_pos.get(n, 999))
        for no, s in numbered:
            mapping[no] = (uid, s)
        base = max([s for _, s in numbered] or [-1]) + 1
        for i, no in enumerate(rest):
            mapping[no] = (uid, base + i)
    saved = save_assignments(day, mapping)
    logger.info("manual_route: раскладка на %s сохранена, точек %s", day, saved)
    try:
        html_text = await render_page(day, saved=saved)
    except Exception as e:
        logger.error("manual_route.handle_save render: %s", e, exc_info=True)
        return web.Response(text="Сохранено, но страница не перерисовалась — обнови", status=500)
    return web.Response(text=html_text, content_type="text/html", charset="utf-8")


# ─── Команда бота ────────────────────────────────────────────────────────────

async def cmd_dispatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not rr._allowed(chat_id):
        return
    arg = (context.args[0] if context.args else "").strip().lower()
    today = datetime.now(_MSK).date()
    day = today + timedelta(days=1) if arg in ("завтра", "tomorrow") else today
    await update.message.reply_text(
        f"Раскладка на {day.strftime('%d.%m')} (аварийный режим, без Логистики):\n"
        f"{dispatch_url(day.isoformat())}\n\n"
        "Расставь машины и порядок → Сохранить → тут в боте /маршруты → Подтвердить.\n"
        "На завтра: /раскладка завтра",
        disable_web_page_preview=True)


def register(app: Application, db):
    set_db(db)
    app.add_handler(CommandHandler("dispatch", cmd_dispatch))
    app.add_handler(MessageHandler(filters.Regex(r"^/раскладка(@\w+)?(\s|$)"), cmd_dispatch))
    try:
        ensure_schema(db)
    except Exception as e:
        logger.exception("manual_route.ensure_schema отложено (БД не готова?): %s", e)
    logger.info("manual_route: хендлеры зарегистрированы")
