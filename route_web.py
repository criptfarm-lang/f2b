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
import asyncio
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


def _norm_tel(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits[0] in "78":
        return "+7" + digits[1:]
    return "+" + digits if digits else ""


def _phone_from_text(text: str) -> str:
    """Первый телефон из текста → нормализованный +7XXXXXXXXXX для tel:-ссылки. Пусто, если нет."""
    m = _PHONE_RE.search(text or "")
    return _norm_tel(m.group(0)) if m else ""


def _phones_from_text(text: str) -> list[str]:
    """ВСЕ телефоны из текста, по порядку, без дублей.

    Менеджер пишет под адресом несколько контактов приёмки («Катя +7926…, склад
    +7495…»), а водителю показывался только первый — если он не отвечает, точка
    встаёт. Поручение совещания по логистике 24.08.2026."""
    out = []
    for m in _PHONE_RE.finditer(text or ""):
        tel = _norm_tel(m.group(0))
        if tel and tel not in out:
            out.append(tel)
    return out


def _e(s) -> str:
    return _html.escape(str(s or ""))


# Забор товара «руками»: точка Логистики без документа в МС, названная «ЗАБРАТЬ ПОДДОНЫ»,
# «забрать тунец» и т.п. Заборы по заказу поставщику ловятся точнее — по is_pickup из МС.
_PICKUP_RE = re.compile(r"забра|забор", re.IGNORECASE)


def _looks_like_pickup(order_no, client) -> bool:
    return bool(_PICKUP_RE.search(f"{order_no or ''} {client or ''}"))


# Бесплатные образцы клиенту проводят по 1 ₽ (зеркало driver_checklist._SAMPLE_MAX_PRICE_RUB).
_SAMPLE_MAX_PRICE_RUB = 1.0


def _rub(v) -> str:
    """12480.5 → «12 480 ₽». Копейки водителю не нужны — деньги он берёт купюрами."""
    try:
        return f"{round(float(v)):,} ₽".replace(",", " ")
    except (TypeError, ValueError):
        return ""


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
/* Забор товара — оранжевый акцент, как строка забора в PDF-реестре: водитель должен
   с первого взгляда отличать «приехал забрать» от «приехал сдать». */
.pt.pickup{background:#fff7ed;border-left:5px solid #f59e0b}
.pt.pickup .num{color:#b45309}
.tag-pickup{display:inline-block;margin-left:8px;padding:1px 7px;border-radius:6px;
  background:#f59e0b;color:#fff;font-size:11px;font-weight:700;vertical-align:middle}
.pt .meta.sample{color:#b45309;font-weight:600}
/* Адрес сменили в МС после сборки маршрута – водитель едет по новому (кейс 04572). */
.addr-moved{margin:4px 0;padding:6px 8px;border-radius:8px;background:#fdecea;color:#c62828;font-size:13px;font-weight:600}
/* Подсветка точки, на которую привёл QR из реестра (якорь #o<№ документа>).
   scroll-margin — чтобы карточка не прилипала к верхней кромке экрана. */
.pt:target{box-shadow:0 0 0 3px #f59e0b,0 1px 3px rgba(0,0,0,.06);scroll-margin-top:12px}
.pt:target .num{color:#b45309}
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
_ROUTE_TASKS = {}   # (uid, date_iso) -> asyncio.Task построения снимка (single-flight)
_ROUTE_TTL = 90     # сек: снимок свежий — отдаём как есть
_COLD_WAIT = 25     # сек: сколько ждём ПЕРВОЕ построение, если снимка ещё нет вообще


class LogisticsUnavailable(Exception):
    """Логистика (Wialon) не ответила, и ручная раскладка по машине пуста."""


async def _build_route_data(uid: int, target: date):
    """Тяжёлый живой фетч Wialon + МойСклад. Горячий путь HTTP-запроса его НЕ ждёт
    (кроме самого первого раза) — см. `_route_data`."""
    routes, order_routes, wialon_ok = await rr.fetch_routes_checked()
    stops = [s for s in (routes.get(uid) or []) if rd._stop_on_date(s, target)]
    if not stops and not wialon_ok:
        # Пустоту при упавшей Логистике в снимок не кладём: прежний снимок (если был)
        # продолжит отдаваться, а без него водитель увидит «Логистика не ответила».
        raise LogisticsUnavailable("Логистика не ответила, точек по машине нет")
    # names — чтобы заборы (заказы поставщику) матчились ещё и по названию поставщика,
    # а не только по номеру ЗП: иначе точка забора приезжает в веб как обычная доставка.
    ms_extra = await rr._ms_extra_by_order(
        [s["order_no"] for s in stops],
        names={s["order_no"]: s.get("client") for s in stops},
    ) if stops else {}
    return stops, ms_extra, order_routes


async def _refresh(key, uid: int, target: date):
    """Обновить снимок маршрута в кэше. Единственное место, которое ходит в Wialon/МС."""
    t0 = time.monotonic()
    try:
        data = await _build_route_data(uid, target)
        _ROUTE_CACHE[key] = (time.monotonic(),) + data
        logger.info("route_web: снимок %s обновлён за %.1f с (%d точек)",
                    key, time.monotonic() - t0, len(data[0]))
        return data
    except Exception as e:
        logger.warning("route_web: снимок %s не собрался за %.1f с: %s", key, time.monotonic() - t0, e)
        raise
    finally:
        _ROUTE_TASKS.pop(key, None)


def _ensure_refresh(key, uid: int, target: date):
    """Запустить обновление снимка, если оно ещё не идёт (single-flight: три водителя
    на одной машине не должны запускать три одинаковых тяжёлых фетча)."""
    task = _ROUTE_TASKS.get(key)
    if task is None or task.done():
        task = asyncio.ensure_future(_refresh(key, uid, target))
        _ROUTE_TASKS[key] = task
    return task


async def _route_data(uid: int, target: date):
    """(stops, ms_extra, order_routes, age_sec) из фонового снимка.

    Страница водителя не должна зависеть от скорости МойСклад: при 429 глобальный
    троттл (`moysklad.py`) уходит в 30-секундный sleep, и синхронный рендер висел
    десятки секунд — телефон рвал соединение (07.08.2026: `200` с телом 0 байт).
    Поэтому: свежий снимок отдаём сразу; протухший — тоже сразу, а обновление
    крутится в фоне; ждём построение только если снимка нет вообще.
    Закрытость точек (done) читается мимо снимка, из БД → статус сдачи всегда свежий."""
    key = (uid, target.isoformat())
    hit = _ROUTE_CACHE.get(key)
    age = (time.monotonic() - hit[0]) if hit else None
    if hit and age < _ROUTE_TTL:
        return hit[1], hit[2], hit[3], age
    task = _ensure_refresh(key, uid, target)
    if hit:
        return hit[1], hit[2], hit[3], age   # stale-while-revalidate
    # Холодный старт: подождём построение, но задачу не отменяем — доедет для следующего захода.
    await asyncio.wait([task], timeout=_COLD_WAIT)
    hit = _ROUTE_CACHE.get(key)
    if not hit:
        if task.done() and isinstance(task.exception(), LogisticsUnavailable):
            raise task.exception()
        raise TimeoutError("снимок маршрута ещё не готов")
    return hit[1], hit[2], hit[3], time.monotonic() - hit[0]


async def warm_all(target: date = None):
    """Прогрев снимков всех машин — из фоновой джобы бота (bot.py). Водитель заходит
    на готовое, а не запускает тяжёлый фетч своим кликом."""
    target = target or datetime.now(_MSK).date()
    for uid in rr.UNITS:
        key = (uid, target.isoformat())
        try:
            await _refresh(key, uid, target)
        except Exception:
            pass   # причина уже в логе _refresh; остальные машины греем дальше


def _age_note(age: float) -> str:
    """Возраст снимка человеческим языком: «1 мин назад»."""
    mins = int(age // 60)
    return f"{mins} мин назад" if mins else f"{int(age)} сек назад"


def _q_block(question: str, name: str, options) -> str:
    """Один вопрос чеклиста колонкой: заголовок сверху, варианты — крупными строками.

    Вариант — это <label> во всю ширину: попасть пальцем можно в любую его точку,
    а не только в кружок радиокнопки (жалоба водителей на «строчный» вариант).
    """
    opts = "".join(
        f"<label class='opt'><input type='radio' name='{_e(name)}' value='{_e(v)}'>"
        f"<span>{_e(t)}</span></label>" for v, t in options)
    return (f"<div class='q'><div class='qt'>{_e(question)}</div>"
            f"<div class='opts'>{opts}</div></div>")


async def _tasks_for_stops(stops, ms_extra, done_set, db) -> dict:
    """{order_no: {"tasks": [поручение, …], "notes": [пометка, …]}} для незакрытых точек сдачи.

    notes — то, что менеджер написал в поле справкой, без действия («рест Ухват», телефон):
    показываем строкой, чекбокса не даём. Раньше модель дописывала к таким пометкам глагол
    и водитель получал выдуманное поручение «Забери заказ для ресторана …» (14.08.2026).

    money_needed считаем ровно как в карточке — этот флаг входит в ключ кэша и в промпт
    (для розницы денежный пункт не дублируем, его закрывает шаг «Забери деньги»)."""
    import driver_checklist as dc
    targets = []
    for s in stops:
        order_no = s.get("order_no")
        ex = ms_extra.get(order_no, {})
        raw = (ex.get("checklist_raw") or "").strip()
        if not raw or rd._doc_no(order_no) in done_set:
            continue
        if ex.get("is_pickup") or _looks_like_pickup(order_no, s.get("client")):
            continue   # у забора документа в МС нет → поручений заказа тоже нет
        client = s.get("client") or ""
        _mp = ex.get("max_price_rub")
        is_sample = (_mp is not None and (ex.get("weight") or 0) > 0
                     and _mp <= _SAMPLE_MAX_PRICE_RUB)
        targets.append((order_no, raw, client.startswith("Рознич") and not is_sample))
    if not targets:
        return {}
    res = await asyncio.gather(
        *(dc.driver_tasks_ex(db, raw, money) for _, raw, money in targets),
        return_exceptions=True)
    out = {}
    for (order_no, _, _), r in zip(targets, res):
        if isinstance(r, Exception):
            logger.warning("route_web tasks %s: %s", order_no, r)
            continue
        if r and (r.get("tasks") or r.get("notes")):
            out[order_no] = r
    return out


async def render_page(uid: int, target: date, db, bot) -> str:
    unit_name = rr.UNITS.get(uid, str(uid))
    date_str = target.strftime("%d.%m.%Y")
    date_iso = target.isoformat()
    token = make_token(uid, date_iso)

    stops, ms_extra, order_routes, age = await _route_data(uid, target)

    # Закрытые точки. Имя водителя на странице больше не показываем (собственник,
    # 2026-08-04): маршрут привязан к машине, водители забирают свой рейс сами.
    done_set = set()
    if db is not None:
        try:
            import json
            row = db._fetchone("SELECT done FROM route_dispatch WHERE snap_date=%s AND unit_id=%s",
                               (target.isoformat(), uid))
            if row and row.get("done"):
                d = row["done"] if isinstance(row["done"], list) else json.loads(row["done"])
                done_set = set(d)
        except Exception as e:
            logger.warning("route_web done: %s", e)

    # Возраст снимка показываем честно: страница отдаётся из фонового снимка (мгновенно),
    # а не строится в момент захода — значит состав маршрута может отставать на минуту-другую.
    stale = f" · данные {_age_note(age)}" if age and age >= _ROUTE_TTL else ""
    head = (f"<div class='head'><h1>🚚 {_e(rr.unit_title(uid))}</h1>"
            f"<div class='sub'>{date_str} · {len(stops)} точек (порядок выгрузки){stale}</div>")

    if stops:
        km = rr.mileage_km(order_routes, uid, [s.get("oid") for s in stops])
        head += f"<div class='vol'>{_e(rd._volume_note(uid, stops, ms_extra, km=km))}</div>"
    head += "</div>"

    if not stops:
        body = "<div class='empty'>Маршрут по этой машине ещё не построен в Логистике.</div>"
        return _wrap(unit_name, head + body)

    # Поручения из доп.поля заказа «Чек-лист водителя» — считаем ОДНИМ заходом на всю
    # страницу и параллельно: на кэш-промахе каждый текст идёт в модель, по очереди это
    # были бы секунды ожидания у водителя. Незакрытые точки — только они и рисуют форму.
    tasks_by_order = await _tasks_for_stops(stops, ms_extra, done_set, db)

    cards = []
    for i, s in enumerate(stops, 1):
        order_no = s.get("order_no")
        ex = ms_extra.get(order_no, {})
        is_done = rd._doc_no(order_no) in done_set
        # Забор товара: заказ поставщику с нашей машиной (is_pickup из МС) ЛИБО ручная
        # точка Логистики, названная «забрать …» (документа в МС у неё нет вообще).
        is_pickup = bool(ex.get("is_pickup")) or _looks_like_pickup(order_no, s.get("client"))
        # Своя точка «Развозки» («ТОЧКА 7»): документа в МС нет – закрывается ручной формой
        # «Выполнил / Проблема», а не забором с документами поставщика.
        is_free = bool(s.get("is_free")) or rr.is_free_point(order_no)
        if is_free:
            is_pickup = False
        # Ручная точка Логистики без документа МС («Рыба на Анализы»): в поле № заказа нет
        # номера — отгрузки быть не может, обычная сдача ответила бы «не отгружена» навсегда
        # (кейс 20.10.2026). Закрывается своей формой: выполнил / проблема.
        is_manual = is_free or (not is_pickup and not rd._DOC_RE.search(order_no or ""))
        cls = "pt" + (" done" if is_done else "") + (" pickup" if is_pickup else "")

        # Для забора имя = поставщик, адрес = «Адрес забора» из заказа поставщику (как в PDF-реестре).
        client = (ex.get("client") if is_pickup and ex.get("client") else s.get("client")) or order_no or "?"
        # Окно приёмки — из полей заказа МС «Окно доставки с/до (время)», фолбэк на заявку.
        win = rr._fmt_window(ex.get("win_from"), ex.get("win_to"), s.get("tf"), s.get("tt"))
        plan = rr._hm(s.get("vt") or s.get("vt_plan"))
        address = (ex.get("address") if is_pickup and ex.get("address") else s.get("address")) or ""
        wt = rr._fmt_weight(ex.get("weight"))
        places = ex.get("places")
        places = str(places) if places not in (None, "") else "—"
        resp = ex.get("manager") or ""
        comment = (ex.get("comment") or "").replace("\n", " ").strip()

        rows = []
        badge = "<span class='tag-pickup'>ЗАБОР</span>" if is_pickup else ""
        rest = "" if is_pickup else (ex.get("restaurant") or "")
        client = rr.split_restaurant(client, rest)
        rest_html = f" <small style='font-weight:400;opacity:.75'>{_e(rest)}</small>" if rest else ""
        rows.append(f"<div class='top'><span class='num'>{i}.</span>"
                    f"<span class='cli'>{_e(client)}{rest_html}{badge}</span>"
                    f"<span class='win'>🕒 {_e(plan)} · {_e(win)}</span></div>")
        if address:
            rows.append(f"<div class='row'>📍 <a href='{_maps_link(address)}' target='_blank'>{_e(address)}</a></div>")
        # Поручения из «Чек-листа водителя» — сразу под клиентом, а не только в форме
        # сдачи внизу: «пропуск заказать за час» надо увидеть ДО приезда (кейс 04734).
        chk = (ex.get("checklist_raw") or "").strip()
        if chk and not is_done and not is_pickup:
            chk_html = "<br>".join(_e(x.strip()) for x in chk.splitlines() if x.strip())
            rows.append(f"<div class='addr-moved'>📋 Чек-лист: {chk_html}</div>")
        if s.get("addr_changed_from") and not is_pickup:
            rows.append(f"<div class='addr-moved'>⚠ Адрес изменён менеджером. В маршруте было: "
                        f"{_e(s['addr_changed_from'])}</div>")
        # Телефон приёмки — из Комментария заказа (менеджеры вписывают контакт туда),
        # карточку контрагента больше не читаем. Номер из комментария делаем кликабельным.
        tels = _phones_from_text(comment)
        if tels:
            links = " · ".join(f"<a href='tel:{_e(t)}'>{_e(t)}</a>" for t in tels)
            rows.append(f"<div class='row'>📞 {links}</div>")
        wbits = " · ".join(x for x in ((f"{wt} кг" if wt else ""), f"{places} мест") if x)
        meta = " · ".join(x for x in (wbits, (f"Отв: {_e(resp)}" if resp else "")) if x)
        if meta:
            rows.append(f"<div class='meta'>📦 {meta}</div>")
        if comment:
            rows.append(f"<div class='meta'>💬 {_e(comment)}</div>")
        # Приписка логиста в заявке Логистики (пропуск, номер авианакладной, условия
        # въезда) — в МойСклад её нет, водителю она нужна на точке. Источник помечаем.
        mnote = rr.manual_note(s.get("note"), comment, ex.get("descr"))
        if mnote:
            rows.append(f"<div class='meta'>📋 От логиста: {_e(mnote)}</div>")
        rows.append(f"<div class='meta'>№ заказа {_e(order_no)}</div>")

        if is_done:
            rows.append(f"<div class='done-badge'>{'✅ Забрано' if is_pickup else ('✅ Выполнено' if is_manual else '✅ Сдан')}</div>")
        elif is_manual:
            rows.append(
                f"<form class='sd' data-uid='{uid}' data-date='{date_iso}' data-kind='manual' "
                f"data-order='{_e(order_no)}' data-token='{_e(token)}'>"
                + _q_block("📦 Итог по точке:", "accepted",
                           [("ok", "Выполнил"), ("claim", "Проблема")])
                + "<textarea name='claim_text' class='claim' placeholder='Что не так' hidden></textarea>"
                  "<button type='button' class='btn' onclick='sendSd(this.form)'>Отправить</button>"
                  "<div class='msg'></div></form>")
        elif is_pickup:
            # Забор: денег и «сдачи» нет — водитель забирает товар и документы у поставщика.
            rows.append(
                f"<form class='sd' data-uid='{uid}' data-date='{date_iso}' data-kind='pickup' "
                f"data-order='{_e(order_no)}' data-token='{_e(token)}'>"
                + _q_block("✍️ Забери документы у поставщика", "doc",
                           [("yes", "Забрал"), ("no", "Не забрал")])
                + _q_block("📦 Итог забора:", "accepted",
                           [("ok", "Забрал"), ("claim", "Проблема")])
                + "<textarea name='claim_text' class='claim' placeholder='Что не так: чего не хватило, что не отдали' hidden></textarea>"
                  "<button type='button' class='btn' onclick='sendSd(this.form)'>Отправить забор</button>"
                  "<div class='msg'></div></form>")
        else:
            is_retail = (client or "").startswith("Рознич")
            # Бесплатные образцы клиенту проводят по 1 ₽ за позицию — а иногда и по 0 ₽
            # (заказ 03420, собственник 05.08.2026), поэтому нижней границы нет: образец —
            # это «дороже 1 ₽ в заказе ничего нет». Денег с такой точки водитель не берёт.
            # Позиции при этом должны быть (вес > 0) — иначе это заказ без данных, а не образцы.
            _mp = ex.get("max_price_rub")
            is_sample = (_mp is not None and (ex.get("weight") or 0) > 0
                         and _mp <= _SAMPLE_MAX_PRICE_RUB)
            money_needed = is_retail and not is_sample
            # Чеклист — колонкой: пункт строкой, ответы под ним (собственник, 2026-08-04).
            # В строку не помещалось на телефоне и водитель промахивался по варианту.
            # Формулировка — РАСПОРЯЖЕНИЕ, а не вопрос (собственник, 2026-08-14): «Забрал
            # деньги?» водитель читал как опрос, а не как задачу на точке. Сумму заказа
            # подставляем — водитель видит, сколько ждать с розничной точки.
            _sum = ex.get("sum_rub") or 0
            money_q = _q_block(
                "💵 Забери деньги" + (f" — {_rub(_sum)}" if _sum else ""), "money",
                [("yes", "Забрал"), ("no", "Не забрал")]) if money_needed else ""
            if is_sample:
                rows.append("<div class='meta sample'>🎁 Образцы — деньги не берём</div>")
            # Поручения менеджера из доп.поля заказа «Чек-лист водителя» — отдельными
            # пунктами. Разбор по смыслу и кэш — driver_checklist.driver_tasks; приёмка
            # берёт тот же список тем же вызовом, ответы сопоставляются по индексу.
            # Пометки без действия («рест Ухват», телефон приёмки) — строкой над формой:
            # отмечать там нечего, а раньше модель дописывала им глагол и получалось
            # выдуманное поручение (собственник, 14.08.2026).
            _tn = tasks_by_order.get(order_no) or {}
            for ntext in (_tn.get("notes") or []):
                rows.append(f"<div class='meta'>📋 {_e(ntext)}</div>")
            tasks_q = ""
            for ti, ttext in enumerate(_tn.get("tasks") or []):
                tasks_q += _q_block(f"📋 {ttext}", f"item_{ti}",
                                    [("yes", "Сделал"), ("no", "Не сделал")])
            # Запасной вход «или через бот» убран (собственник, 2026-08-04): кнопки в
            # Telegram не нажимались/подвисали, сдача закрывается только здесь.
            rows.append(
                f"<form class='sd' data-uid='{uid}' data-date='{date_iso}' "
                f"data-order='{_e(order_no)}' data-token='{_e(token)}'>"
                f"{money_q}{tasks_q}"
                + _q_block("✍️ Подпиши документ и забери свой экземпляр", "doc",
                           [("yes", "Подписал"), ("no", "Не подписал")])
                + _q_block("📦 Итог сдачи:", "accepted",
                           [("ok", "Сдано"), ("claim", "Претензия")])
                + "<textarea name='claim_text' class='claim' placeholder='Опиши претензию' hidden></textarea>"
                  "<button type='button' class='btn' onclick='sendSd(this.form)'>Отправить сдачу</button>"
                  "<div class='msg'></div></form>")

        # «Клиент вернул» (план 2026-10-20-возвраты-гп): и до, и после сдачи – возврат бывает
        # по прошлым накладным. Форма грузит накладные клиента по запросу, а не с маршрутом.
        # Кнопка скрыта до обсуждения инструкции командой (собственник 20.10.2026):
        # включить – RETURNS_FORM_ON=1 в env бота и перезапуск.
        if not is_pickup and not is_manual and os.getenv("RETURNS_FORM_ON") == "1":
            rows.append(f"<div class='ret' data-uid='{uid}' data-date='{date_iso}' "
                        f"data-order='{_e(order_no)}' data-token='{_e(token)}'>"
                        "<button type='button' class='btn2' onclick='retOpen(this)'>↩️ Клиент вернул</button>"
                        "<div class='retbox' hidden></div></div>")

        # id карточки = № документа: QR в PDF-реестре ведёт сюда якорем (#o<№>),
        # и точка подсвечивается рамкой — водителю видно, какую именно он сканировал.
        cards.append(f"<div class='{cls}' id='o{_e(rd._doc_no(order_no))}'>"
                     + "".join(rows) + "</div>")

    body = "".join(cards)
    return _wrap(unit_name, head + body)


# Чеклист колонкой: вопрос — строкой, под ним варианты во всю ширину (палец не
# промахивается). Раньше вопрос и ответы шли в одну строку и на телефоне ломались.
_FORM_CSS = (".sd{margin-top:10px;padding-top:10px;border-top:1px solid #eee}"
             ".q{margin:12px 0}"
             ".qt{font-size:15px;font-weight:600;margin-bottom:6px}"
             ".opts{display:flex;flex-direction:column;gap:6px}"
             ".opt{display:flex;align-items:center;gap:10px;padding:11px 12px;"
             "border:1px solid #d7dbe0;border-radius:10px;font-size:15px;background:#fff}"
             ".opt input{width:20px;height:20px;margin:0;flex:none}"
             ".opt:has(input:checked),.opt.sel{border-color:#0d2b45;background:#eef2f7;font-weight:600}"
             ".claim{width:100%;box-sizing:border-box;margin:6px 0;min-height:54px;"
             "font-size:15px;padding:8px;border:1px solid #d7dbe0;border-radius:10px}"
             ".sd .btn{display:block;width:100%;margin-top:12px;border:0;cursor:pointer}"
             # Неотвеченный пункт при попытке отправить сдачу.
             ".q.unans .qt{color:#b91c1c}"
             ".q.unans .opt{border-color:#b91c1c}"
             ".msg{color:#b91c1c;font-size:14px;margin-top:6px}"
             # Возврат у клиента
             ".ret{margin-top:10px}"
             ".btn2{display:block;width:100%;padding:11px;border:1px solid #0d2b45;border-radius:10px;"
             "background:#fff;color:#0d2b45;font-size:15px;font-weight:600;cursor:pointer}"
             ".retbox{margin-top:8px;padding:10px;border:1px dashed #0d2b45;border-radius:10px;background:#f8fafc}"
             ".retbox h4{margin:10px 0 6px;font-size:14px}"
             ".retbox video{width:100%;max-height:220px;background:#000;border-radius:8px}"
             ".rpos{display:flex;align-items:center;gap:8px;padding:8px 0;border-bottom:1px solid #e6e7ee;font-size:14px}"
             ".rpos .nm{flex:1}.rpos small{color:#667}"
             ".rpos button{border:1px solid #d7dbe0;border-radius:8px;background:#fff;padding:6px 10px;font-size:14px}"
             ".rpos input{width:70px;padding:6px;font-size:15px;border:1px solid #d7dbe0;border-radius:8px;text-align:right}"
             ".retbox select,.retbox textarea,.retbox .code{width:100%;box-sizing:border-box;padding:8px;font-size:15px;"
             "border:1px solid #d7dbe0;border-radius:10px;margin:4px 0}"
             ".rph{text-align:center;box-sizing:border-box}"
             ".rthumbs{display:flex;flex-wrap:wrap;gap:6px;align-items:center;margin:6px 0}"
             ".rth{position:relative}.rth img{width:64px;height:64px;object-fit:cover;border-radius:8px;display:block}"
             ".rth button{position:absolute;top:-6px;right:-6px;border:0;border-radius:50%;width:22px;height:22px;"
             "background:#b91c1c;color:#fff;font-size:12px;padding:0}"
             ".rtoast{font-size:14px;color:#166534;min-height:18px;margin:4px 0}"
             ".rtoast.err{color:#b91c1c}")

_FORM_JS = (
    # Ни один пункт нельзя пропустить: раньше проверялся только «Итог сдачи», и поручения
    # менеджера водитель мог оставить неотвеченными. Незакрытый пункт подсвечиваем красным
    # и подкручиваем к нему экран, а не просто ругаемся alert'ом в пустоту.
    "function needAll(f){var rs=f.querySelectorAll('input[type=radio]');var seen=[],g={};"
    "for(var i=0;i<rs.length;i++){var n=rs[i].name;if(!g[n]){g[n]=[];seen.push(n);}g[n].push(rs[i]);}"
    "for(var k=0;k<seen.length;k++){var arr=g[seen[k]],ok=false;"
    "for(var j=0;j<arr.length;j++){if(arr[j].checked)ok=true;}"
    "if(!ok){var q=arr[0].closest('.q');if(q){q.classList.add('unans');"
    "q.scrollIntoView({block:'center',behavior:'smooth'});"
    "var t=q.querySelector('.qt');alert('Отметь пункт: '+((t&&t.textContent)||''));}return false;}}"
    "return true;}"
    "function sendSd(f){var kd=f.dataset.kind||'';var pk=(kd==='pickup'||kd==='manual');"
    "if(!needAll(f))return;"
    "var acc=(f.accepted&&f.accepted.value)||'';"
    "var body={order_no:f.dataset.order,items:{}};"
    "if(pk){body.kind=kd;body.title=(f.closest('.pt').querySelector('.cli')||{}).textContent||'';}"
    "new FormData(f).forEach(function(v,k){if(k.indexOf('item_')===0){body.items[k.slice(5)]=v;}else{body[k]=v;}});"
    "if(acc==='claim'&&!((body.claim_text||'').trim())){alert(pk?'Опиши, что не так':'Опиши претензию');return;}"
    "var url='/route/'+f.dataset.uid+'/'+f.dataset.date+'/submit?t='+encodeURIComponent(f.dataset.token);"
    "var btn=f.querySelector('button');var lbl=btn.textContent;btn.disabled=true;btn.textContent='Отправляю…';"
    "function fail(t){f.querySelector('.msg').textContent=t;btn.disabled=false;btn.textContent=lbl;}"
    "fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})"
    ".then(function(r){return r.text().then(function(txt){return{status:r.status,txt:txt};});})"
    ".then(function(res){var d=null;try{d=JSON.parse(res.txt);}catch(_){}"
    "if(d&&d.ok){var c=f.closest('.pt');if(c)c.className='pt done'+(kd==='pickup'?' pickup':'');"
    "f.outerHTML=\"<div class='done-badge'>\"+(d.msg||'✅ Закрыто')+\"</div>\";}"
    "else if(d){fail(d.msg||('Ошибка '+res.status));}"
    "else{fail('HTTP '+res.status+': '+((res.txt||'нет ответа').replace(/<[^>]*>/g,' ').trim().slice(0,140)));}})"
    ".catch(function(e){fail('Сбой запроса: '+((e&&e.message)||e));});}"
    "document.addEventListener('change',function(e){var el=e.target;if(!el||el.type!=='radio')return;"
    "if(el.name==='accepted'){var t=el.form.querySelector('.claim');if(t)t.hidden=(el.value!=='claim');}"
    # Подсветка выбранного варианта — на случай браузера без поддержки CSS :has().
    "var grp=el.form.querySelectorAll(\"input[name='\"+el.name+\"']\");"
    "for(var i=0;i<grp.length;i++){var lab=grp[i].closest('.opt');"
    "if(lab)lab.className=grp[i].checked?'opt sel':'opt';}"
    "var q=el.closest('.q');if(q)q.classList.remove('unans');});")


# Форма «Клиент вернул». Скан – камерой (BarcodeDetector, иначе библиотека ZXing, как в ревизии
# цеха) или вводом цифр штрихкода. Весовая этикетка «2…»: знаки 3–7 – код товара, 8–12 – граммы;
# GS1-128 – GTIN и вес. Позиция ищется в накладной точки, потом в 3 прошлых накладных клиента;
# не нашлась – всё равно принимается («накладную определить»). Привлечённая – только фото этикеток:
# водитель не выбирает ни позицию, ни накладную (собственник 23.10.2026, после жалобы водителей).
_RET_JS = r"""
var RET={};
function retEsc(s){return String(s==null?'':s).replace(/[&<>"']/g,function(c){return{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
function retUrl(w,path){return '/route/'+w.dataset.uid+'/'+w.dataset.date+'/returns'+(path||'')+'?t='+encodeURIComponent(w.dataset.token);}
function retOpen(btn){var w=btn.closest('.ret'),box=w.querySelector('.retbox');
  if(!box.hidden){box.hidden=true;retStop(w);return;}
  box.hidden=false;if(RET[w.dataset.order]){return;}
  box.innerHTML='Загружаю накладные клиента…';
  fetch(retUrl(w)+'&order='+encodeURIComponent(w.dataset.order)).then(function(r){return r.json();}).then(function(d){
    if(!d.ok){box.innerHTML='<div class="rtoast err">'+retEsc(d.msg||'Не загрузилось')+'</div>';return;}
    RET[w.dataset.order]={d:d,items:[],photos:[],last:{}};retDraw(w);
  }).catch(function(e){box.innerHTML='<div class="rtoast err">Сбой: '+retEsc(e.message||e)+'</div>';});}
function retDraw(w){var st=RET[w.dataset.order],d=st.d,box=w.querySelector('.retbox');
  box.innerHTML='<h4>Наша продукция – сканируй этикетку</h4>'
   +'<button type="button" class="btn2 rcam" onclick="retCam(this)">📷 Сканировать</button>'
   +'<video class="rv" hidden playsinline muted></video>'
   +'<input class="code" inputmode="numeric" placeholder="или цифры штрихкода" onkeydown="if(event.key===\'Enter\'){retCode(this.closest(\'.ret\'),this.value);this.value=\'\';}">'
   +'<div class="rtoast"></div>'
   +'<h4>Возвращают</h4><div class="ritems"></div>'
   +'<h4>Привлечённая – сфоткай этикетку</h4>'
   +'<label class="btn2 rph">📸 Фото этикетки<input type="file" accept="image/*" capture="environment" hidden onchange="retPhoto(this)"></label>'
   +'<div class="rthumbs"></div>'
   +'<h4>Причина</h4><select class="rreason"><option value="">– выбери –</option>'
   +d.reasons.map(function(r){return '<option>'+retEsc(r)+'</option>';}).join('')+'</select>'
   +'<textarea class="rcomm" placeholder="Комментарий: что не так"></textarea>'
   +'<button type="button" class="btn" onclick="retSend(this.closest(\'.ret\'))">Оформить возврат</button><div class="msg"></div>';
  retItems(w);retThumbs(w);}
function retItems(w){var st=RET[w.dataset.order],h='';
  if(!st.items.length)h='<div class="rpos"><small>пока ничего</small></div>';
  st.items.forEach(function(it,k){h+='<div class="rpos"><span class="nm">'+retEsc(it.code)+' '+retEsc(it.name)+' <small>'+(it.demand_name?'накл. №'+retEsc(it.demand_name):'накладную определят в офисе')+'</small></span>'
    +'<input inputmode="decimal" value="'+it.qty+'" onchange="retQty(this.closest(\'.ret\'),'+k+',this.value)"><small>'+retEsc(it.uom)+'</small>'
    +'<button type="button" onclick="retDel(this.closest(\'.ret\'),'+k+')">✕</button></div>';});
  w.querySelector('.ritems').innerHTML=h;}
function retToast(w,t,err){var e=w.querySelector('.rtoast');if(e){e.textContent=t;e.className='rtoast'+(err?' err':'');}}
function retAdd(w,did,i,qty,src,raw){var st=RET[w.dataset.order];
  var dm=st.d.demands.filter(function(x){return x.id===did;})[0],p=dm.positions[i];
  var q=qty==null?(p.uom.indexOf('кг')===0?0:1):qty;
  var ex=st.items.filter(function(x){return x.demand_id===did&&x.product_id===p.product_id;})[0];
  if(ex&&src==='scan'){ex.qty=Math.round((ex.qty+q)*1000)/1000;ex.raw=(ex.raw?ex.raw+' ':'')+raw;}
  else if(!ex){st.items.push({demand_id:did,demand_name:dm.name,product_id:p.product_id,type:p.type,code:p.code,name:p.name,qty:q,uom:p.uom,source:src,raw:raw||''});}
  retItems(w);
  if(src==='scan')retToast(w,'Добавлено: '+p.code+' '+p.name+' +'+q+' '+p.uom);
  else if(!q){var ins=w.querySelectorAll('.ritems input');if(ins.length)ins[ins.length-1].focus();}}
function retQty(w,k,v){RET[w.dataset.order].items[k].qty=parseFloat(String(v).replace(',','.'))||0;}
function retDel(w,k){RET[w.dataset.order].items.splice(k,1);retItems(w);}
function ean13ok(d){var s=0;for(var i=0;i<12;i++)s+=(+d[i])*(i%2?3:1);return (10-s%10)%10===+d[12];}
function retParse(raw){var c=String(raw||'').trim();
  if(/^2\d{12}$/.test(c)&&ean13ok(c)&&c.slice(2,7)!=='00000')return{code:c.slice(2,7),kg:parseInt(c.slice(7,12),10)/1000};
  var t=c.replace(/^\]C1/,'').replace(/[()]/g,''),m=t.match(/01(\d{14})/),wt=t.match(/310(\d)(\d{6})/);
  if(m)return{gtin:m[1],kg:wt?parseInt(wt[2],10)/Math.pow(10,+wt[1]):null};
  if(/^\d{4,6}$/.test(c))return{code:c,kg:null};
  return null;}
function retCode(w,raw){var st=RET[w.dataset.order],now=Date.now();
  if(st.last[raw]&&now-st.last[raw]<3000)return;st.last[raw]=now;
  var l=retParse(raw);if(!l){retToast(w,'Штрихкод не распознан: '+raw,true);return;}
  var hit=null;st.d.demands.some(function(dm){return dm.positions.some(function(p,i){
    var ok=l.code?(p.code===l.code||p.code.replace(/^0+/,'')===l.code.replace(/^0+/,'')):(p.gtins||[]).indexOf(l.gtin)>=0;
    if(ok){hit=[dm.id,i];}return ok;});});
  if(!hit){if(!l.code){retToast(w,'Этикетка не наша – сфоткай её ниже',true);return;}
    var ex=st.items.filter(function(x){return !x.product_id&&x.code===l.code;})[0],q=l.kg||0;
    if(ex){ex.qty=Math.round((ex.qty+q)*1000)/1000;ex.raw+=' '+raw;}
    else st.items.push({demand_id:null,demand_name:'',product_id:null,code:l.code,name:'',qty:q,uom:'кг',source:'scan',raw:raw});
    retItems(w);retToast(w,'Добавлено: код '+l.code+(q?' +'+q+' кг':''));return;}
  retAdd(w,hit[0],hit[1],l.kg,'scan',raw);}
function retThumbs(w){var st=RET[w.dataset.order],h='';
  st.photos.forEach(function(p,k){h+='<span class="rth"><img src="'+p+'"><button type="button" onclick="retPhDel(this.closest(\'.ret\'),'+k+')">✕</button></span>';});
  w.querySelector('.rthumbs').innerHTML=h+(st.photos.length?'<small>фото: '+st.photos.length+'</small>':'');}
function retPhDel(w,k){RET[w.dataset.order].photos.splice(k,1);retThumbs(w);}
function retPhoto(inp){var w=inp.closest('.ret'),st=RET[w.dataset.order],f=inp.files&&inp.files[0];inp.value='';if(!f)return;
  if(st.photos.length>=10){retToast(w,'Не больше 10 фото',true);return;}
  var img=new Image(),u=URL.createObjectURL(f);img.onload=function(){var k=Math.min(1,1280/Math.max(img.width,img.height));
    var c=document.createElement('canvas');c.width=Math.round(img.width*k);c.height=Math.round(img.height*k);
    c.getContext('2d').drawImage(img,0,0,c.width,c.height);URL.revokeObjectURL(u);
    st.photos.push(c.toDataURL('image/jpeg',0.7));retThumbs(w);};
  img.onerror=function(){retToast(w,'Фото не открылось, сфоткай ещё раз',true);};img.src=u;}
function retStop(w){var v=w.querySelector('video.rv');if(w._rs){w._rs.getTracks().forEach(function(t){t.stop();});w._rs=null;}
  if(w._rz){try{w._rz.reset();}catch(e){}w._rz=null;}w._scan=false;if(v)v.hidden=true;}
function retLoadZx(cb){if(window.ZXing)return cb();var s=document.createElement('script');s.src='/route-static/zxing.min.js';s.onload=cb;
  s.onerror=function(){cb('нет библиотеки');};document.head.appendChild(s);}
function retCam(btn){var w=btn.closest('.ret'),v=w.querySelector('video.rv');
  if(w._scan){retStop(w);btn.textContent='📷 Сканировать';return;}
  btn.textContent='⏹ Остановить камеру';
  if(!navigator.mediaDevices||!navigator.mediaDevices.getUserMedia){retToast(w,'Браузер не даёт камеру – введи цифры штрихкода',true);return;}
  var go=function(stream){w._rs=stream;w._scan=true;v.hidden=false;v.srcObject=stream;v.play().catch(function(){});
    if('BarcodeDetector' in window){var det=new BarcodeDetector({formats:['ean_13','code_128']});
      (function loop(){if(!w._scan)return;det.detect(v).then(function(cs){cs.forEach(function(c){retCode(w,c.rawValue);});}).catch(function(){}).then(function(){setTimeout(loop,200);});})();}
    else retLoadZx(function(err){if(err){retToast(w,'Сканер не загрузился – введи цифры',true);return;}
      var hints=new Map();hints.set(ZXing.DecodeHintType.POSSIBLE_FORMATS,[ZXing.BarcodeFormat.EAN_13,ZXing.BarcodeFormat.CODE_128]);
      w._rz=new ZXing.BrowserMultiFormatReader(hints,200);w._rz.decodeFromStream(stream,v,function(res){if(res)retCode(w,res.getText());});});};
  navigator.mediaDevices.getUserMedia({video:{facingMode:{ideal:'environment'}},audio:false}).then(go)
    .catch(function(e){retToast(w,'Камера не открылась: '+(e.name||e)+' – введи цифры штрихкода',true);});}
function retSend(w){var st=RET[w.dataset.order],b0=w.querySelector('.retbox');var m=b0.querySelector('.msg');
  st.reason=b0.querySelector('.rreason').value;st.comment=b0.querySelector('.rcomm').value;
  if(!st.items.length&&!st.photos.length){m.textContent='Отсканируй нашу этикетку или сфоткай привлечённую';return;}
  if(st.items.some(function(x){return x.product_id&&!(x.qty>0);})){m.textContent='Укажи количество у каждой позиции';return;}
  if(!st.reason){m.textContent='Выбери причину';return;}
  var b=w.querySelector('.retbox .btn');b.disabled=true;b.textContent='Оформляю…';
  fetch(retUrl(w),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({order_no:w.dataset.order,
    agent_id:st.d.agent_id,agent_name:st.d.agent_name,items:st.items,photos:st.photos,
    point_demand_id:((st.d.demands.filter(function(x){return x.today;})[0])||{}).id||'',reason:st.reason,comment:st.comment||''})})
   .then(function(r){return r.json();}).then(function(d){
     if(d.ok){retStop(w);delete RET[w.dataset.order];w.querySelector('.retbox').innerHTML='<div class="done-badge">'+retEsc(d.msg)+'</div>';}
     else{m.textContent=d.msg||'Ошибка';b.disabled=false;b.textContent='Оформить возврат';}})
   .catch(function(e){m.textContent='Сбой: '+(e.message||e);b.disabled=false;b.textContent='Оформить возврат';});}
"""


def _wrap(title: str, inner: str) -> str:
    return (f"<!doctype html><html lang='ru'><head><meta charset='utf-8'>"
            f"<meta name='viewport' content='width=device-width,initial-scale=1'>"
            f"<title>Маршрут {_e(title)}</title><style>{_PAGE_CSS}{_FORM_CSS}</style></head>"
            f"<body><div class='wrap'>{inner}"
            f"<div class='foot'>Сдача точки — прямо здесь. Обнови страницу, если логист поменял маршрут.</div>"
            f"</div><script>{_FORM_JS}{_RET_JS}</script></body></html>")


# ─── aiohttp-хендлер ─────────────────────────────────────────────────────────

def _logistics_down_page() -> web.Response:
    """Логистика не ответила — честно говорим водителю и сами перезагружаемся через 30 с."""
    return web.Response(
        text="<!doctype html><html lang='ru'><head><meta charset='utf-8'>"
             "<meta name='viewport' content='width=device-width,initial-scale=1'>"
             "<meta http-equiv='refresh' content='30'><title>Логистика не ответила</title>"
             "<style>body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;background:#f2f4f7;"
             "color:#334;margin:0;padding:48px 24px;text-align:center}"
             "h2{font-size:19px;margin:0 0 8px}p{font-size:15px;color:#667}</style></head>"
             "<body><h2>Логистика не ответила</h2>"
             "<p>Маршрут сейчас не загрузить. Страница обновится сама через 30 секунд, "
             "или обнови её через минуту.</p></body></html>",
        content_type="text/html", charset="utf-8")


def _waiting_page() -> web.Response:
    """Маршрут ещё строится (холодный старт + тормоза МС) — страница сама перезагрузится."""
    return web.Response(
        text="<!doctype html><html lang='ru'><head><meta charset='utf-8'>"
             "<meta name='viewport' content='width=device-width,initial-scale=1'>"
             "<meta http-equiv='refresh' content='10'><title>Маршрут обновляется</title>"
             "<style>body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;background:#f2f4f7;"
             "color:#334;margin:0;padding:48px 24px;text-align:center}"
             "h2{font-size:19px;margin:0 0 8px}p{font-size:15px;color:#667}</style></head>"
             "<body><h2>Маршрут обновляется…</h2>"
             "<p>Страница откроется сама через несколько секунд.</p></body></html>",
        content_type="text/html", charset="utf-8")


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
    t0 = time.monotonic()
    try:
        html_text = await render_page(uid, target, db, bot)
        logger.info("route_web: страница uid=%s отдана за %.1f с", uid, time.monotonic() - t0)
        return web.Response(text=html_text, content_type="text/html", charset="utf-8")
    except LogisticsUnavailable:
        logger.warning("route_web: Логистика не ответила, снимка uid=%s нет — отдаю страницу «не ответила»", uid)
        return _logistics_down_page()
    except TimeoutError:
        # Снимка ещё нет, а построение упёрлось в тормоза МС/Wialon. Отдаём лёгкую
        # страницу с авто-перезагрузкой — водитель не смотрит в «вечную загрузку».
        logger.warning("route_web: снимок uid=%s не готов за %.0f с — отдаю страницу ожидания",
                       uid, time.monotonic() - t0)
        return _waiting_page()
    except Exception as e:
        logger.error("route_web.handle: %s", e, exc_info=True)
        return _err("Не удалось собрать маршрут. Проверь доступ к Логистике.", 500)


async def handle_submit(request, db, bot) -> web.Response:
    """POST-приёмка точки с веб-страницы (мимо Telegram). Тот же HMAC-токен, что на GET."""
    try:
        uid = int(request.match_info["uid"])
        date_str = request.match_info["date"]
        day = date.fromisoformat(date_str)
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
        kind = body.get("kind") or ""
        if kind in ("pickup", "manual"):
            # Забор и ручная точка: отгрузки в МС нет — своя ветка (гасим точку + уведомляем логистов).
            ok, msg = await dc.web_pickup_submit(
                db, bot, uid, order_no,
                doc=body.get("doc"), accepted=(body.get("accepted") or ""),
                claim_text=body.get("claim_text") or "", title=(body.get("title") or "").strip(),
                day=day, kind=kind)
            return web.json_response({"ok": ok, "msg": msg})
        # day — дата ИЗ ССЫЛКИ, а не «сегодня»: сдача после полуночи должна гасить точку
        # своего маршрута, а не следующего дня.
        ok, msg = await dc.web_submit(
            db, bot, uid, order_no,
            money=body.get("money"), doc=body.get("doc"),
            items=body.get("items") or {}, accepted=(body.get("accepted") or ""),
            claim_text=body.get("claim_text") or "", day=day)
        return web.json_response({"ok": ok, "msg": msg})
    except Exception as e:
        logger.error("route_web.handle_submit: %s", e, exc_info=True)
        return web.json_response({"ok": False, "msg": "Ошибка сохранения, повтори"}, status=500)


# ─── возврат у клиента (client_returns.py) ───────────────────────────────────

def _check_link(request):
    try:
        uid = int(request.match_info["uid"])
        date_str = request.match_info["date"]
        day = date.fromisoformat(date_str)
    except (ValueError, KeyError):
        return None, None, web.json_response({"ok": False, "msg": "Некорректная ссылка"}, status=400)
    if uid not in rr.UNITS or not verify_token(uid, date_str, request.query.get("t", "")):
        return None, None, web.json_response({"ok": False, "msg": "Ссылка недействительна"}, status=403)
    return uid, day, None


async def handle_returns_options(request, db, bot) -> web.Response:
    uid, day, err = _check_link(request)
    if err:
        return err
    import client_returns
    try:
        return web.json_response(await client_returns.options(request.query.get("order", "")))
    except Exception as e:
        logger.error("returns options: %s", e, exc_info=True)
        return web.json_response({"ok": False, "msg": "МойСклад не ответил, попробуй ещё раз"})


async def handle_returns_submit(request, db, bot) -> web.Response:
    uid, day, err = _check_link(request)
    if err:
        return err
    if os.getenv("RETURNS_FORM_ON") != "1":
        return web.json_response({"ok": False, "msg": "Оформление возвратов пока выключено"}, status=403)
    import client_returns
    try:
        body = await request.json()
        ok, msg = await client_returns.submit(db, bot, uid, day, str(body.get("order_no") or "").strip(), body)
        return web.json_response({"ok": ok, "msg": msg})
    except Exception as e:
        logger.error("returns submit: %s", e, exc_info=True)
        return web.json_response({"ok": False, "msg": "Ошибка сохранения, повтори"}, status=500)


async def handle_zxing(request) -> web.Response:
    import pathlib
    return web.FileResponse(pathlib.Path(__file__).with_name("static") / "zxing.min.js",
                            headers={"Cache-Control": "public, max-age=604800"})
