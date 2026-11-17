"""Акционные объявления по вторникам: ОПТ (2 карточки) и HoReCa (4 карточки) собственнику на утверждение.

Решение собственника 17.11.2026 (план «второго мозга» plans/2026-11-17-акционные-объявления-по-вторникам.md):
- ОПТ: филе лосося/форели/кеты с/с, х/к, с/м (все разделки), масляная, тунец, нарезки, скумбрия.
  Цена – «Цена опт». «Опубликовать» – в канал «FISH TO BUSINESS opt» (где прайсы).
- HoReCa: 1 – зависший товар (продажи за 30 дн. упали к прошлым 60 дн.), 2 – охлаждённая рыба
  (лосось/форель ОХЛ, по кругу), 3 – привлечённый товар, 4 – товар ГП. Цена – «Цена продажи».
  «Опубликовать» – в канал «FISH TO BUSINESS horeca».
- Наименование и артикул – из МойСклада. На всех карточках «Цена действует 3 дня с момента публикации».
- Фото: свежие посты «Контент F2B» (publishing.story_jobs) с подписью про этот товар, иначе фото с сайта.
- Кнопки: «Другая цена» (собственник отвечает числом), «Другая» (следующий кандидат), «Другое фото»,
  «Опубликовать». Ничего не нажал – карточка просто остаётся.

Запуск: вторник 10:00 МСК (проверка раз в 10 минут). Внеочередной прогон – bot_settings
promo_offers_run_now = '1' (сбрасывается после прогона).
"""
from __future__ import annotations

import asyncio
import datetime as dt
import io
import json
import logging
import os
import re

import requests
from telegram import ForceReply, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.ext import ApplicationHandlerStop, CallbackQueryHandler, MessageHandler, filters

logger = logging.getLogger(__name__)

MSK = dt.timezone(dt.timedelta(hours=3))
MS = "https://api.moysklad.ru/api/remap/1.2"
OPT_CHANNEL_ID = int(os.getenv("PROMO_OPT_CHANNEL_ID", "-1002344232944"))      # FISH TO BUSINESS opt
HORECA_CHANNEL_ID = int(os.getenv("PROMO_HORECA_CHANNEL_ID", "-1002433362110"))  # FISH TO BUSINESS horeca
CONTENT_CHAT_ID = int(os.getenv("CONTENT_CHAT_ID", "-1001433042091"))
STORE = "Основной склад"
RUN_HOUR = 10
REPEAT_WEEKS = 3            # товар, предложенный недавно, не повторяем столько недель
SEND_GAP_SEC = 60

MAX_URL = "https://max.ru/u/f9LHodD0cOJK0mNXf5ra8sG0gRn_OteEQFMrTDbiOXsAXwQS1zDkIMO46-0"
FOOTER = (f'Заказать: <a href="{MAX_URL}">MAX</a> · <a href="https://t.me/fishtobiz">Telegram</a> · '
          '<a href="https://wa.me/79937007087">WhatsApp</a>\n8 800 700-27-03')
TERM = "Цена действует 3 дня с момента публикации."
SLOT_TITLE = {("opt", 1): "ОПТ 1", ("opt", 2): "ОПТ 2", ("horeca", 1): "HoReCa · зависший",
              ("horeca", 2): "HoReCa · охлаждёнка", ("horeca", 3): "HoReCa · привлечённый",
              ("horeca", 4): "HoReCa · ГП"}

SCHEMA = """
CREATE SCHEMA IF NOT EXISTS publishing;
CREATE TABLE IF NOT EXISTS publishing.promo_offers (
    id serial PRIMARY KEY,
    week date NOT NULL,
    segment text NOT NULL,
    slot int NOT NULL,
    candidates jsonb NOT NULL DEFAULT '[]',
    idx int NOT NULL DEFAULT 0,
    price numeric,
    photos jsonb NOT NULL DEFAULT '[]',
    photo_idx int NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'pending',
    chat_id bigint,
    message_id bigint,
    prompt_message_id bigint,
    published_message_id bigint,
    published_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (week, segment, slot)
);
"""


def _owner_id() -> int:
    return int(os.environ["OWNER_CHAT_ID"])


# ── МойСклад ─────────────────────────────────────────────────────────────────

def _ms_get(path, params=None):
    tok = os.environ["MOYSKLAD_TOKEN"]
    for attempt in range(3):
        r = requests.get(MS + path, params=params, timeout=60,
                         headers={"Authorization": f"Bearer {tok}", "Accept-Encoding": "gzip"})
        if r.status_code == 429:
            import time
            time.sleep(2 + attempt * 3)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()


def _ms_all(path, params):
    rows, off = [], 0
    while True:
        d = _ms_get(path, {**params, "limit": 1000, "offset": off})
        rows += d.get("rows", [])
        if len(d.get("rows", [])) < 1000:
            return rows
        off += 1000


def _sales(date_from, date_to):
    out = {}
    for r in _ms_all("/report/profit/byproduct",
                     {"momentFrom": f"{date_from} 00:00:00", "momentTo": f"{date_to} 23:59:59"}):
        code = (r.get("assortment") or {}).get("code")
        if code:
            out[code] = out.get(code, 0) + (r.get("sellQuantity") or 0)
    return out


def _prices(code):
    """Цены, единица и папка товара по артикулу."""
    rows = _ms_get("/entity/product", {"filter": f"code={code}", "expand": "uom", "limit": 1}).get("rows", [])
    if not rows:
        return None
    p = rows[0]
    prices = {x["priceType"]["name"]: x["value"] / 100 for x in p.get("salePrices", []) if x.get("value")}
    return {"name": p["name"], "unit": ((p.get("uom") or {}).get("name") or "кг"), "prices": prices,
            "archived": p.get("archived", False)}


OPT_RE = re.compile(r"(лосось.*филе|форель филе|кета.*филе|масляная.*филе|тунец филе|ломтики|скумбрия)", re.I)
SKIP_RE = re.compile(r"(отход|суповой|сырье под|набор для пива|кубик|хребт)", re.I)


def _collect(today):
    """Остатки «Основного склада» + продажи 30 дн. и 60 дн. до них."""
    store = _ms_get("/entity/store", {"filter": f"name={STORE}"})["rows"][0]["meta"]["href"]
    stock = _ms_all("/report/stock/all", {"filter": f"store={store}"})
    d30 = today - dt.timedelta(days=30)
    recent = _sales(d30.isoformat(), (today - dt.timedelta(days=1)).isoformat())
    early = _sales((d30 - dt.timedelta(days=60)).isoformat(), (d30 - dt.timedelta(days=1)).isoformat())
    items = []
    for r in stock:
        code = r.get("code")
        qty = r.get("stock") or 0
        if not code or qty <= 0:
            continue
        f = r.get("folder") or {}
        folder = f"{f.get('pathName', '')}/{f.get('name', '')}"
        avail = qty - (r.get("reserve") or 0)
        q30 = recent.get(code, 0)
        prev_month = early.get(code, 0) / 2
        items.append({"code": code, "name": r["name"], "folder": folder, "avail": avail,
                      "value": avail * (r.get("salePrice") or 0) / 100,
                      "q30": q30, "prev_month": prev_month,
                      "days": avail / (q30 / 30) if q30 > 0 else 9999})
    return items


def _recent_codes(db, week):
    rows = db._fetchall(
        "SELECT candidates, idx FROM publishing.promo_offers WHERE week > %s AND week < %s",
        (week - dt.timedelta(weeks=REPEAT_WEEKS), week))
    used = set()
    for r in rows:
        c = r["candidates"] or []
        if 0 <= r["idx"] < len(c):
            used.add(c[r["idx"]]["code"])
    return used


def _pick(items, used, cond, key, limit=8, exclude=()):
    out = []
    for it in sorted((i for i in items if cond(i)), key=key):
        if it["code"] in used or it["code"] in exclude or SKIP_RE.search(it["name"]):
            continue
        out.append(it)
        if len(out) >= limit:
            break
    return out


def _chilled_candidates(db, week):
    """Охлаждённое филе/рыба лосося и форели из ГП – по кругу: давно не предлагали – первыми."""
    rows = _ms_all("/entity/product", {"filter": "pathName~ГОТОВАЯ ПРОДУКЦИЯ;archived=false"})
    out = []
    for p in rows:
        n = p["name"]
        if "ОХЛ" in n.upper() and re.search(r"(лосось|форель)", n, re.I) and p.get("code"):
            out.append({"code": p["code"], "name": n})
    last = {}
    for r in db._fetchall("SELECT week, candidates, idx FROM publishing.promo_offers "
                          "WHERE segment = 'horeca' AND slot = 2 AND status = 'published'"):
        c = r["candidates"] or []
        if 0 <= r["idx"] < len(c):
            last[c[r["idx"]]["code"]] = r["week"]
    return sorted(out, key=lambda x: (last.get(x["code"], dt.date(2000, 1, 1)), x["code"]))


MIN_VALUE = 20000          # остаток дешевле 20 тыс. ₽ (по цене продажи) в акцию не ставим


def build_candidates(db, week, today):
    items = _collect(today)
    used = _recent_codes(db, week)
    gp = lambda i: i["folder"].startswith("ГОТОВАЯ ПРОДУКЦИЯ")
    big = lambda i: i["value"] >= MIN_VALUE and (i["q30"] > 0 or i["prev_month"] > 0)
    slow = lambda i: (i["days"] < 45, -i["value"])   # запас больше 45 дней продаж, крупный – первым
    taken = set()                                    # первые кандидаты слотов не повторяются между слотами

    def pick(cond, key, limit=8):
        out = _pick(items, used, lambda i: big(i) and cond(i), key, limit, exclude=set(taken))
        taken.update(x["code"] for x in out[:2])
        return out

    opt = pick(lambda i: gp(i) and "ОХЛ" not in i["name"].upper() and OPT_RE.search(i["name"]), slow, limit=12)
    taken.update(x["code"] for x in opt[:4])
    stuck = pick(lambda i: (gp(i) or i["folder"].startswith("ПРИВЛЕЧ")) and i["prev_month"] > 0
                 and "ОХЛ" not in i["name"].upper()
                 and i["q30"] < i["prev_month"] * 0.5, lambda i: -(i["value"] * (1 - i["q30"] / i["prev_month"])))
    attracted = pick(lambda i: i["folder"].startswith("ПРИВЛЕЧ"), slow)
    gp_items = pick(lambda i: gp(i) and "Акционный" not in i["folder"] and "ОХЛ" not in i["name"].upper(), slow)
    chilled = _chilled_candidates(db, week)
    return {("opt", 1): opt[0::2], ("opt", 2): opt[1::2], ("horeca", 1): stuck, ("horeca", 2): chilled,
            ("horeca", 3): attracted, ("horeca", 4): gp_items}


# ── Фото ─────────────────────────────────────────────────────────────────────

_SPECIES = [("лангуст", r"лангуст|аргентин"), ("ваннам", r"ванн?ам"), ("саку", r"саку"),  # уточнённые – первыми
            ("лосос", r"лосос|сёмг|семг"), ("форел", r"форел"), ("кета", r"кет[аыу]"), ("масл", r"масля"),
            ("тунец", r"тун[еца]"), ("скумбри", r"скумбри"), ("кальмар", r"кальмар"), ("кревет", r"кревет"),
            ("гребеш", r"гребеш"), ("угор", r"угор"), ]


def _kind(name):
    n = name.lower()
    if "х/к" in n or "хк" in n.split() or "копч" in n:
        return r"копч|х/к|хк"
    if "с/с" in n or "солен" in n:
        return r"солен|солё|с/с|посол"
    if "охл" in n:
        return r"охл"
    return None


def _content_photos(db, name):
    """Фото из «Контент F2B», где в подписи есть и вид рыбы, и способ обработки (если он есть у товара)."""
    n = name.lower()
    sp = next((rx for _, rx in _SPECIES if re.search(rx, n)), None)
    if not sp:
        return []
    kind = _kind(name)
    if " - ломтики" in n or "ломтик" in n:
        kind = r"нарезк|ломтик|слайс"
    if (not kind or "ОХЛ" in name.upper()) and not re.search(r"саку|кальмар|кревет|гребеш|угор|лангуст", n):
        return []          # филе с/м и охл.: разделку (Трим) по подписи не узнать – только фото с сайта (собственник 17.11)
    rows = db._fetchall(
        "SELECT id, caption, media FROM publishing.story_jobs WHERE source_chat_id = %s OR chat_id = %s "
        "ORDER BY id DESC LIMIT 200", (CONTENT_CHAT_ID, CONTENT_CHAT_ID))
    out = []
    slice_item = bool(re.search(r"ломтик|нарезк|слайс", n))
    for r in rows:
        cap = (r["caption"] or "").lower()
        if not re.search(sp, cap) or re.search(
                r"суповой|набор|отход|хребт|голов|приём|прием|партия|короб|паллет|отгруз|отправ|камер|инвентар", cap):
            # на таких фото коробки и паллеты, а не рыба (собственник 17.11)
            continue
        if sum(1 for _, rx in _SPECIES[3:9] if re.search(rx, cap)) > 1:
            continue                                   # «нарезки из лосося и форели» – не про один товар
        if bool(re.search(r"нарезк|ломтик|слайс", cap)) != slice_item:
            continue                                   # нарезка на фото ≠ филе в карточке
        if kind and not re.search(kind, cap):
            continue
        if not kind and re.search(r"копч|х/?к\b|солен|солё|с/с|охл", cap):
            continue                                   # товар с/м, а на фото копчёная или солёная
        for m in r["media"] or []:
            if m.get("kind") == "photo" and m.get("file_id"):
                out.append({"type": "file_id", "v": m["file_id"], "src": f"Контент №{r['id']}: {r['caption'][:60]}"})
    return out[:4]


_site_cache = {"at": None, "map": {}}


def _site_images():
    now = dt.datetime.now()
    if _site_cache["at"] and now - _site_cache["at"] < dt.timedelta(hours=6):
        return _site_cache["map"]
    q = "query products($data: GetProductsInput){products(data:$data){edges{node{name images{url}}}}}"
    r = requests.post("https://f2b.group/graphql", timeout=60,
                      json={"query": q, "variables": {"data": {"first": 1000}}, "operationName": "products"})
    r.raise_for_status()
    m = {}
    for e in r.json()["data"]["products"]["edges"]:
        imgs = [i["url"] for i in e["node"].get("images") or [] if i.get("url")]
        if imgs:
            m[_norm(e["node"]["name"])] = imgs
    _site_cache.update(at=now, map=m)
    return m


def _norm(s):
    return re.sub(r"\s+", " ", s.replace("ё", "е")).strip().lower()


def _photos_for(db, name):
    photos = _content_photos(db, name)
    try:
        for url in _site_images().get(_norm(name), [])[:2]:
            photos.append({"type": "url", "v": url, "src": "сайт f2b.group"})
    except Exception:
        logger.exception("weekly_promo: не получил фото с сайта")
    return photos


def _download_jpeg(url):
    from PIL import Image
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    im = Image.open(io.BytesIO(r.content)).convert("RGB")
    im.thumbnail((1600, 1600))
    buf = io.BytesIO()
    im.save(buf, "JPEG", quality=88)
    buf.seek(0)
    return buf


# ── Карточка ─────────────────────────────────────────────────────────────────

def _fmt_price(v):
    v = float(v)
    s = f"{v:,.0f}" if v == int(v) else f"{v:,.2f}"
    return s.replace(",", " ").replace(".", ",")


def _caption(segment, cand, price):
    s = f"<b>{cand['name']}</b>\nАрт. {cand['code']}\n<b>{_fmt_price(price)} ₽/{cand.get('unit', 'кг')}</b>\n"
    if segment == "horeca":
        s += "Доставка по Москве бесплатно.\n"
    return s + f"{TERM}\n\n{FOOTER}"


def _keyboard(oid):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Другая цена", callback_data=f"po:price:{oid}"),
         InlineKeyboardButton("Другая", callback_data=f"po:next:{oid}")],
        [InlineKeyboardButton("Другое фото", callback_data=f"po:photo:{oid}"),
         InlineKeyboardButton("Отмена", callback_data=f"po:cancel:{oid}")],
        [InlineKeyboardButton("Опубликовать", callback_data=f"po:pub:{oid}")],
    ])


def _owner_caption(row, cand):
    photos = row["photos"] or []
    src = photos[row["photo_idx"]]["src"] if photos else "фото нет"
    head = f"{SLOT_TITLE.get((row['segment'], row['slot']), 'HoReCa · акция из сторис')} · фото: {src}"
    return f"<i>{head}</i>\n\n" + _caption(row["segment"], cand, row["price"])


async def _media(row):
    photos = row["photos"] or []
    if not photos:
        return None
    p = photos[row["photo_idx"] % len(photos)]
    if p["type"] == "file_id":
        return p["v"]
    if p["type"] == "path":
        return open(p["v"], "rb")
    return await asyncio.to_thread(_download_jpeg, p["v"])


def _price_for(segment, info):
    return info["prices"].get("Цена опт" if segment == "opt" else "Цена продажи")


async def _resolve(db, segment, cands, start):
    """С позиции start – первый кандидат с ценой. Возвращает (idx, cand, price, photos) или None."""
    for i in range(start, len(cands)):
        info = await asyncio.to_thread(_prices, cands[i]["code"])
        if not info or info["archived"]:
            continue
        price = _price_for(segment, info)
        if not price:
            continue
        cands[i].update(name=info["name"], unit=info["unit"])
        photos = await asyncio.to_thread(_photos_for, db, info["name"])
        return i, cands[i], price, photos
    return None


async def _send_card(bot, db, row):
    cand = row["candidates"][row["idx"]]
    media = await _media(row)
    if media is None:
        msg = await bot.send_message(row["chat_id"], _owner_caption(row, cand) + "\n\n(фото не нашёл – «Другое фото» не поможет)",
                                     parse_mode="HTML", reply_markup=_keyboard(row["id"]))
    else:
        msg = await bot.send_photo(row["chat_id"], media, caption=_owner_caption(row, cand), parse_mode="HTML",
                                   reply_markup=_keyboard(row["id"]))
    db._execute("UPDATE publishing.promo_offers SET message_id = %s, updated_at = now() WHERE id = %s",
                (msg.message_id, row["id"]))


def _row(db, oid):
    return db._fetchone("SELECT * FROM publishing.promo_offers WHERE id = %s", (oid,))


async def run_week(bot, db, today=None):
    today = today or dt.datetime.now(MSK).date()
    week = today - dt.timedelta(days=today.weekday())
    exists = db._fetchone("SELECT count(*) AS n FROM publishing.promo_offers WHERE week = %s", (week,))
    if exists and exists["n"]:
        logger.info("weekly_promo: неделя %s уже отправлена", week)
        return False
    pools = await asyncio.to_thread(build_candidates, db, week, today)
    owner = _owner_id()
    await bot.send_message(owner, "Акционные объявления недели на утверждение: ОПТ – 2, HoReCa – 4.\n"
                                  "«Другая цена» – ответьте числом, «Другая» – следующий товар, "
                                  "«Опубликовать» – в канал, «Отмена» – убрать. Не нажали – не публикуется.\n"
                                  "Карточки придут по одной в минуту.")
    first = True
    for (segment, slot), cands in pools.items():
        if not first:
            await asyncio.sleep(SEND_GAP_SEC)          # не скопом – по карточке в минуту (собственник 17.11)
        first = False
        cands = [dict(c) for c in cands]
        got = await _resolve(db, segment, cands, 0)
        if not got:
            await bot.send_message(owner, f"{SLOT_TITLE[(segment, slot)]}: подходящего товара не нашёл.")
            continue
        idx, cand, price, photos = got
        row = db._fetchone(
            "INSERT INTO publishing.promo_offers (week, segment, slot, candidates, idx, price, photos, chat_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (week, segment, slot) DO NOTHING RETURNING *",
            (week, segment, slot, json.dumps(cands, ensure_ascii=False), idx, price,
             json.dumps(photos, ensure_ascii=False), owner))
        if row:
            await _send_card(bot, db, row)
    return True


# ── Кнопки ───────────────────────────────────────────────────────────────────

_CB = re.compile(r"^po:(price|next|photo|pub|cancel):(\d+)$")


async def _refresh(q, row):
    cand = row["candidates"][row["idx"]]
    media = await _media(row)
    if media is None:
        await q.edit_message_caption(caption=_owner_caption(row, cand), parse_mode="HTML",
                                     reply_markup=_keyboard(row["id"]))
        return
    await q.edit_message_media(InputMediaPhoto(media, caption=_owner_caption(row, cand), parse_mode="HTML"),
                               reply_markup=_keyboard(row["id"]))


async def _cleanup(bot, q, row):
    """Опубликованная или отменённая карточка стирается из лички вместе с вопросом о цене."""
    for mid in (row.get("prompt_message_id"), q.message.message_id if q.message else None):
        if not mid:
            continue
        try:
            await bot.delete_message(row["chat_id"], mid)
        except Exception:
            try:
                if mid == q.message.message_id:
                    await q.edit_message_reply_markup(reply_markup=None)   # старше 48 ч удалить нельзя
            except Exception:
                pass


def _make_callback(db):
    async def handle(update, context):
        q = update.callback_query
        if not q.from_user or q.from_user.id != _owner_id():
            await q.answer("Только для руководителя.", show_alert=True)
            return
        m = _CB.match(q.data or "")
        if not m:
            await q.answer()
            return
        action, oid = m.group(1), int(m.group(2))
        row = _row(db, oid)
        if not row or row["status"] in ("published", "cancelled"):
            await q.answer({"published": "Уже опубликовано.", "cancelled": "Отменено."}.get(
                row["status"] if row else "", "Карточка не найдена."), show_alert=True)
            try:
                await q.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            return
        try:
            if action == "cancel":
                row = db._fetchone("UPDATE publishing.promo_offers SET status = 'cancelled', updated_at = now() "
                                   "WHERE id = %s AND status = 'pending' RETURNING *", (oid,))
                await q.answer("Отменено")
                if row:
                    await _cleanup(context.bot, q, row)
                return
            if action == "price":
                await q.answer()
                cand = row["candidates"][row["idx"]]
                prompt = await q.message.reply_text(
                    f"Цена для «{cand['name']}» (арт. {cand['code']}), ₽ – ответьте числом.",
                    reply_markup=ForceReply(input_field_placeholder="Например 1990"))
                db._execute("UPDATE publishing.promo_offers SET prompt_message_id = %s, updated_at = now() "
                            "WHERE id = %s", (prompt.message_id, oid))
            elif action == "next":
                await q.answer("Ищу другой товар…")
                cands = row["candidates"]
                got = await _resolve(db, row["segment"], cands, row["idx"] + 1)
                if not got and row["idx"] > 0:
                    got = await _resolve(db, row["segment"], cands, 0)      # по кругу
                if not got:
                    await q.message.reply_text("Других кандидатов нет.")
                    return
                idx, cand, price, photos = got
                row = db._fetchone(
                    "UPDATE publishing.promo_offers SET candidates = %s, idx = %s, price = %s, photos = %s, "
                    "photo_idx = 0, updated_at = now() WHERE id = %s RETURNING *",
                    (json.dumps(cands, ensure_ascii=False), idx, price, json.dumps(photos, ensure_ascii=False), oid))
                await _refresh(q, row)
            elif action == "photo":
                n = len(row["photos"] or [])
                if n < 2:
                    await q.answer("Другого фото нет.", show_alert=True)
                    return
                await q.answer()
                row = db._fetchone("UPDATE publishing.promo_offers SET photo_idx = (photo_idx + 1) %% %s, "
                                   "updated_at = now() WHERE id = %s RETURNING *", (n, oid))
                await _refresh(q, row)
            elif action == "pub":
                row = db._fetchone("UPDATE publishing.promo_offers SET status = 'publishing', updated_at = now() "
                                   "WHERE id = %s AND status = 'pending' RETURNING *", (oid,))
                if not row:
                    await q.answer("Уже публикую.", show_alert=True)
                    return
                await q.answer("Публикую в канал " + ("ОПТ" if row["segment"] == "opt" else "HoReCa"))
                cand = row["candidates"][row["idx"]]
                chan = OPT_CHANNEL_ID if row["segment"] == "opt" else HORECA_CHANNEL_ID
                media = await _media(row)
                try:
                    if media is None:
                        msg = await context.bot.send_message(chan, _caption(row["segment"], cand, row["price"]),
                                                             parse_mode="HTML")
                    else:
                        msg = await context.bot.send_photo(chan, media, caption=_caption(row["segment"], cand, row["price"]),
                                                           parse_mode="HTML")
                except Exception:
                    db._execute("UPDATE publishing.promo_offers SET status = 'pending' WHERE id = %s", (oid,))
                    raise
                db._execute("UPDATE publishing.promo_offers SET status = 'published', published_message_id = %s, "
                            "published_at = now(), updated_at = now() WHERE id = %s", (msg.message_id, oid))
                await _cleanup(context.bot, q, row)
        except Exception as e:
            logger.exception("weekly_promo: кнопка %s №%s", action, oid)
            try:
                await q.message.reply_text(f"Не получилось: {e}")
            except Exception:
                pass

    return handle


_NUM = re.compile(r"^\s*(\d[\d\s]*(?:[.,]\d{1,2})?)\s*(₽|р\.?|руб\.?)?\s*$", re.I)


class _PriceReply(filters.MessageFilter):
    def __init__(self, db):
        super().__init__(name="PromoPrice")
        self.db = db

    def filter(self, message) -> bool:
        r = message.reply_to_message
        if not r or not message.text or not message.from_user or message.from_user.id != _owner_id():
            return False
        try:
            return bool(self.db._fetchone(
                "SELECT 1 FROM publishing.promo_offers WHERE prompt_message_id = %s AND chat_id = %s",
                (r.message_id, message.chat_id)))
        except Exception:
            return False


def _make_price_handler(db):
    async def handle(update, context):
        msg = update.effective_message
        row = db._fetchone("SELECT * FROM publishing.promo_offers WHERE prompt_message_id = %s AND chat_id = %s",
                           (msg.reply_to_message.message_id, msg.chat_id))
        m = _NUM.match(msg.text or "")
        if not row:
            raise ApplicationHandlerStop
        if not m:
            await msg.reply_text("Нужно число, например 1990.")
            raise ApplicationHandlerStop
        if row["status"] == "published":
            await msg.reply_text("Эта карточка уже опубликована – цену не меняю.")
            raise ApplicationHandlerStop
        price = float(m.group(1).replace(" ", "").replace(",", "."))
        row = db._fetchone("UPDATE publishing.promo_offers SET price = %s, updated_at = now() WHERE id = %s "
                           "RETURNING *", (price, row["id"]))
        cand = row["candidates"][row["idx"]]
        try:
            await context.bot.edit_message_caption(chat_id=row["chat_id"], message_id=row["message_id"],
                                                   caption=_owner_caption(row, cand), parse_mode="HTML",
                                                   reply_markup=_keyboard(row["id"]))
        except Exception:
            logger.exception("weekly_promo: не обновил карточку №%s", row["id"])
        for mid in (msg.message_id, msg.reply_to_message.message_id):   # вопрос и ответ не копим в личке
            try:
                await context.bot.delete_message(msg.chat_id, mid)
            except Exception:
                pass
        db._execute("UPDATE publishing.promo_offers SET prompt_message_id = NULL WHERE id = %s", (row["id"],))
        raise ApplicationHandlerStop

    return handle


# ── Расписание ───────────────────────────────────────────────────────────────

def _make_job(db):
    async def job(context):
        try:
            now = dt.datetime.now(MSK)
            flag = db._fetchone("SELECT value FROM bot_settings WHERE key = 'promo_offers_run_now'")
            forced = bool(flag and (flag["value"] or "").strip() == "1")
            if forced:
                db._execute("UPDATE bot_settings SET value = '0' WHERE key = 'promo_offers_run_now'")
                if not await run_week(context.bot, db):
                    await context.bot.send_message(_owner_id(), "Акционные объявления этой недели уже отправлены.")
            elif now.weekday() == 1 and now.hour >= RUN_HOUR:
                await run_week(context.bot, db)
        except Exception:
            logger.exception("weekly_promo: прогон упал")

    return job


def register(app, db):
    db._execute(SCHEMA)
    app.add_handler(MessageHandler(filters.TEXT & filters.REPLY & _PriceReply(db), _make_price_handler(db)), group=-1)
    app.add_handler(CallbackQueryHandler(_make_callback(db), pattern=r"^po:(price|next|photo|pub|cancel):"))
    app.job_queue.run_repeating(_make_job(db), interval=600, first=90, name="weekly_promo")
    logger.info("weekly_promo: зарегистрирован")
