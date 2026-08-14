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


async def _build_route_data(uid: int, target: date):
    """Тяжёлый живой фетч Wialon + МойСклад. Горячий путь HTTP-запроса его НЕ ждёт
    (кроме самого первого раза) — см. `_route_data`."""
    routes, order_routes = await rr.fetch_routes(with_meta=True)
    stops = [s for s in (routes.get(uid) or []) if rd._stop_on_date(s, target)]
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
    """{order_no: [поручение, …]} для незакрытых точек сдачи.

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
        *(dc.driver_tasks(db, raw, money) for _, raw, money in targets),
        return_exceptions=True)
    out = {}
    for (order_no, _, _), r in zip(targets, res):
        if isinstance(r, Exception):
            logger.warning("route_web tasks %s: %s", order_no, r)
            continue
        if r:
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
        cls = "pt" + (" done" if is_done else "") + (" pickup" if is_pickup else "")

        # Для забора имя = поставщик, адрес = «Адрес забора» из заказа поставщику (как в PDF-реестре).
        client = (ex.get("client") if is_pickup and ex.get("client") else s.get("client")) or order_no or "?"
        # Окно приёмки — из полей заказа МС «Окно доставки с/до (время)», фолбэк на заявку.
        win = rr._fmt_window(ex.get("win_from"), ex.get("win_to"), s.get("tf"), s.get("tt"))
        plan = rr._hm(s.get("vt"))
        address = (ex.get("address") if is_pickup and ex.get("address") else s.get("address")) or ""
        wt = rr._fmt_weight(ex.get("weight"))
        places = ex.get("places")
        places = str(places) if places not in (None, "") else "—"
        resp = ex.get("manager") or ""
        comment = (ex.get("comment") or "").replace("\n", " ").strip()

        rows = []
        badge = "<span class='tag-pickup'>ЗАБОР</span>" if is_pickup else ""
        rows.append(f"<div class='top'><span class='num'>{i}.</span>"
                    f"<span class='cli'>{_e(client)}{badge}</span>"
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
            rows.append(f"<div class='done-badge'>{'✅ Забрано' if is_pickup else '✅ Сдан'}</div>")
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
            tasks_q = ""
            for ti, ttext in enumerate(tasks_by_order.get(order_no) or []):
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
             ".msg{color:#b91c1c;font-size:14px;margin-top:6px}")

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
    "function sendSd(f){var pk=f.dataset.kind==='pickup';"
    "if(!needAll(f))return;"
    "var acc=(f.accepted&&f.accepted.value)||'';"
    "var body={order_no:f.dataset.order,items:{}};"
    "if(pk){body.kind='pickup';body.title=(f.closest('.pt').querySelector('.cli')||{}).textContent||'';}"
    "new FormData(f).forEach(function(v,k){if(k.indexOf('item_')===0){body.items[k.slice(5)]=v;}else{body[k]=v;}});"
    "if(acc==='claim'&&!((body.claim_text||'').trim())){alert(pk?'Опиши, что не так':'Опиши претензию');return;}"
    "var url='/route/'+f.dataset.uid+'/'+f.dataset.date+'/submit?t='+encodeURIComponent(f.dataset.token);"
    "var btn=f.querySelector('button');var lbl=btn.textContent;btn.disabled=true;btn.textContent='Отправляю…';"
    "function fail(t){f.querySelector('.msg').textContent=t;btn.disabled=false;btn.textContent=lbl;}"
    "fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})"
    ".then(function(r){return r.text().then(function(txt){return{status:r.status,txt:txt};});})"
    ".then(function(res){var d=null;try{d=JSON.parse(res.txt);}catch(_){}"
    "if(d&&d.ok){var c=f.closest('.pt');if(c)c.className='pt done'+(pk?' pickup':'');"
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


def _wrap(title: str, inner: str) -> str:
    return (f"<!doctype html><html lang='ru'><head><meta charset='utf-8'>"
            f"<meta name='viewport' content='width=device-width,initial-scale=1'>"
            f"<title>Маршрут {_e(title)}</title><style>{_PAGE_CSS}{_FORM_CSS}</style></head>"
            f"<body><div class='wrap'>{inner}"
            f"<div class='foot'>Сдача точки — прямо здесь. Обнови страницу, если логист поменял маршрут.</div>"
            f"</div><script>{_FORM_JS}</script></body></html>")


# ─── aiohttp-хендлер ─────────────────────────────────────────────────────────

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
        if (body.get("kind") or "") == "pickup":
            # Забор: отгрузки в МС нет — своя ветка (гасим точку + уведомляем логистов).
            ok, msg = await dc.web_pickup_submit(
                db, bot, uid, order_no,
                doc=body.get("doc"), accepted=(body.get("accepted") or ""),
                claim_text=body.get("claim_text") or "", title=(body.get("title") or "").strip(),
                day=day)
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
