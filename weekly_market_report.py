"""F2B — недельный обзор рынка в канал «Мониторинг».

План: plans/2026-09-29-недельный-обзор-рынка-в-мониторинг.md (репо «F2B второй мозг»).

Пятница 16:00 МСК: собираем за 7 дней всё, что Кристина и Александра принесли в
«Мониторинг» (market_intel_messages + разобранные лоты procurement.lots), список
привлечённых товаров с остатком > 0 из МойСклад, статьи и цены Фишнета/Фишретейла,
модель пишет короткий отчёт, бот публикует его в тот же канал.

Фаза 1 (этот файл сейчас): сбор внутренних данных. Цифры считаем здесь, в коде –
модель потом только пересказывает готовые числа, сама ничего не считает.
"""

from __future__ import annotations

import asyncio
import html as _html
import re
import statistics
from dataclasses import dataclass, field
from datetime import date, timedelta

import aiohttp

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
ATTRACTED_FOLDER = "ПРИВЛЕЧЕННЫЕ ТОВАРЫ"
BASELINE_DAYS = 28          # с чем сравниваем неделю
TOP_LOTS = 3                # сколько самых дешёвых предложений показываем модели

# Целые формы рыбы (сырьё под разделку). Филе, трим, стейки в «сырьё» не идут.
WHOLE = ("ПСГ", "ПБГ", "НПСГ", "Б/Г", "НР", "HGT", "тушка")


@dataclass
class Category:
    key: str
    title: str
    species: str
    regions: tuple[str, ...] = ()        # пусто = любой регион
    states: tuple[str, ...] = ()         # пусто = любое состояние
    not_states: tuple[str, ...] = ()
    processing: tuple[str, ...] = WHOLE
    weight: str | None = None            # калибр, напр. "4-5" (для Чили)


# Порядок и состав – решение собственника 29.09.2026.
CATEGORIES: list[Category] = [
    Category("salmon_mrm", "Лосось охл Мурманск", "лосось", ("Мурманск",), ("охл",)),
    Category("trout_tr", "Форель охл Турция", "форель", ("Турция-озеро", "Турция-море"), ("охл",)),
    Category("trout_os", "Форель охл Осетия", "форель", ("Северная Осетия",), ("охл",)),
    Category("trout_am", "Форель охл Армения", "форель", ("Армения",), ("охл",)),
    Category("trout_ka", "Форель охл Карелия", "форель", ("Карелия",), ("охл",)),
    Category("trout_ir", "Форель с/м Иран", "форель", ("Иран",), (), ("охл",)),
    Category("salmon_cl45", "Лосось Чили 4-5", "лосось", ("Чили",), (), ("охл",), weight="4-5"),
    Category("salmon_cl56", "Лосось Чили 5-6", "лосось", ("Чили",), (), ("охл",), weight="5-6"),
    Category("keta", "Кета", "кета"),
    Category("butter", "Масляная", "масляная", processing=()),
    # «Атлантическая (Фареры и проч.)»: Фареры, Норвегия, Мурманск – северная Атлантика.
    # Китай/Корея/Аргентина – другая скумбрия, не берём.
    Category("mackerel", "Скумбрия атлантическая", "скумбрия", ("Фарерские о-ва", "Норвегия", "Мурманск")),
    Category("tuna_loin", "Тунец лойн", "тунец", processing=("лоин",)),
]


def _sizes(text: str | None) -> set[tuple[str, str]]:
    """Пары калибра «4-5», «4/5», «4–5 кг», «16-20» → {("4","5")}. Запятые → точки."""
    if not text:
        return set()
    t = text.replace(",", ".")
    return {(a, b) for a, b in re.findall(r"(\d+(?:\.\d+)?)\s*[-/–]\s*(\d+(?:\.\d+)?)", t)}


def _median(xs: list[float]) -> float | None:
    return round(statistics.median(xs)) if xs else None


def _pct(new: float | None, old: float | None) -> float | None:
    if not new or not old:
        return None
    return round((new - old) / old * 100, 1)


# ─── Доступ к базе ──────────────────────────────────────────────────────────
# fetchall(sql, params) -> list[dict], SQL в стиле psycopg2 (%s). В боте это
# db._fetchall, в локальном прогоне – адаптер над asyncpg (см. __main__).

LOTS_SQL = """
SELECT l.species::text AS species, l.region::text AS region, l.state::text AS state,
       l.processing::text AS processing, l.product_form::text AS form,
       l.weight_class, l.price_rub_kg::float AS price, l.received_at,
       coalesce(s.name, l.supplier_id) AS supplier,
       left(coalesce(l.raw_text, ''), 90) AS raw
FROM procurement.lots l
LEFT JOIN procurement.suppliers s ON s.slug = l.supplier_id
WHERE l.received_at >= %s AND l.received_at <= %s
  AND l.price_rub_kg > 0
  AND l.species::text = ANY(%s)
"""


def _lot_matches(lot: dict, c: Category) -> bool:
    if lot["species"] != c.species or lot["form"] != "сырьё":
        return False
    if c.regions and lot["region"] not in c.regions:
        return False
    if c.states and lot["state"] not in c.states:
        return False
    if c.not_states and lot["state"] in c.not_states:
        return False
    if c.processing and lot["processing"] not in c.processing:
        return False
    if c.weight and tuple(c.weight.split("-")) not in _sizes(lot["weight_class"]):
        return False
    return True


def _short_lot(lot: dict) -> dict:
    return {
        "цена": round(lot["price"]),
        "поставщик": lot["supplier"],
        "регион": lot["region"],
        "разделка": lot["processing"],
        "состояние": lot["state"],
        "калибр": lot["weight_class"],
        "дата": lot["received_at"].isoformat(),
    }


def _stats(week: list[dict], base: list[dict]) -> dict:
    wp = [l["price"] for l in week]
    bp = [l["price"] for l in base]
    w_med, b_med = _median(wp), _median(bp)
    cheapest = sorted(week, key=lambda l: l["price"])[:TOP_LOTS]
    return {
        "предложений_за_неделю": len(week),
        "поставщиков_за_неделю": len({l["supplier"] for l in week}),
        "мин_неделя": round(min(wp)) if wp else None,
        "медиана_неделя": w_med,
        "медиана_4_недели_до": b_med,
        "изменение_медианы_%": _pct(w_med, b_med),
        "предложений_4_недели_до": len(base),
        "дешевле_всего": [_short_lot(l) for l in cheapest],
    }


def collect_raw_categories(fetchall, week_end: date) -> list[dict]:
    week_start = week_end - timedelta(days=6)
    base_start = week_start - timedelta(days=BASELINE_DAYS)
    species = sorted({c.species for c in CATEGORIES})
    lots = fetchall(LOTS_SQL, (base_start, week_end, species))
    out = []
    for c in CATEGORIES:
        mine = [l for l in lots if _lot_matches(l, c)]
        week = [l for l in mine if l["received_at"] >= week_start]
        base = [l for l in mine if l["received_at"] < week_start]
        out.append({"категория": c.title, **_stats(week, base)})
    return out


MESSAGES_SQL = """
SELECT m.id, m.tg_msg_id, m.posted_at, m.msg_type, coalesce(m.author_signature, '') AS author,
       coalesce(m.text_raw, '') AS text, coalesce(m.original_filename, '') AS filename,
       coalesce(m.forward_from, '') AS forward_from,
       (SELECT count(*) FROM procurement.lots l WHERE l.msg_id = m.tg_msg_id) AS lots
FROM market_intel_messages m
WHERE m.posted_at >= %s AND m.posted_at < %s
ORDER BY m.posted_at
"""


def collect_messages(fetchall, week_end: date) -> list[dict]:
    """Все сообщения канала за неделю: текст/подпись целиком, для файлов – имя и сколько
    лотов из него разобрано. Цены из файлов модель получает через лоты, не через текст."""
    week_start = week_end - timedelta(days=6)
    rows = fetchall(MESSAGES_SQL, (week_start, week_end + timedelta(days=1)))
    return [{
        "дата": r["posted_at"].strftime("%d.%m %H:%M"),
        "автор": r["author"] or "без подписи",
        "тип": r["msg_type"],
        "текст": r["text"][:1500],
        "файл": r["filename"],
        "переслано_из": r["forward_from"],
        "разобрано_цен": r["lots"],
    } for r in rows]


# ─── Привлечённые товары ────────────────────────────────────────────────────

# Ключевое слово в названии SKU → вид в procurement.lots. «Креветка (Лангустин) L1» –
# аргентинская красная креветка: в лотах она лежит то как креветка, то как лангустин.
SKU_SPECIES = [
    ("лангустин", ("креветка", "лангустин")), ("креветк", ("креветка",)),
    ("кальмар", ("кальмар",)), ("мидии", ("мидия",)), ("мидий", ("мидия",)),
    ("гребешок", ("гребешок",)), ("осьминог", ("осьминог",)), ("угорь", ("угорь",)),
    ("тунец", ("тунец",)), ("треск", ("треска",)), ("судак", ("судак",)),
    ("минтай", ("минтай",)), ("камбала", ("камбала",)), ("сибас", ("сибас",)),
    ("дорадо", ("дорадо",)),
]
SKU_REGIONS = {"индия": "Индия", "эквадор": "Эквадор", "китай": "Китай", "вьетнам": "Вьетнам"}
FILLET = ("филе б/к", "филе н/к", "филе с/к", "лоин")


@dataclass
class SkuRule:
    species: tuple[str, ...]
    region: str | None = None
    sizes: set = field(default_factory=set)
    processing: tuple[str, ...] = ()     # пусто = любая разделка
    forms: tuple[str, ...] = ("сырьё",)
    raw_any: tuple[str, ...] = ()        # хоть одна подстрока в названии лота


def _sku_rule(name: str) -> SkuRule | None:
    """Разбор названия нашего SKU в фильтр по лотам. Только явные признаки из
    названия; если признака нет – фильтр по нему не ставим."""
    low = name.lower()
    species = next((sp for kw, sp in SKU_SPECIES if kw in low), None)
    if not species:
        return None
    r = SkuRule(species, next((rg for kw, rg in SKU_REGIONS.items() if kw in low), None), _sizes(name))
    if "панировк" in low or "кляр" in low:
        r.forms, r.raw_any = (), ("панир", "кляр")
    elif "жарен" in low:
        r.forms = ("жаренный", "с соусом", "г/к")
        r.sizes = set()                  # у нас граммы, у поставщиков унции – не сопоставимо
    elif "саку" in low:
        r.processing = ("Saku", "филе б/к")
    elif "лойн" in low or "лоин" in low or "спинки" in low:
        r.processing = ("лоин",)
    elif "мясо мидий" in low:
        r.raw_any = ("мяс",)
    elif "половин" in low:
        r.raw_any = ("половин",)
    elif "очищ" in low and "креветка" in species:
        r.processing = ("PDTO", "PDTL")
    elif ("в панцире" in low or "б/г" in low) and "креветка" in species:
        r.processing = ("HLSO", "Б/Г")
    elif "филе" in low:
        r.processing = FILLET
    elif "тушка" in low:
        r.processing = ("тушка",)
    elif "н/р" in low:
        r.processing = ("НР",)
    elif "б/г" in low:
        r.processing = ("Б/Г", "ПБГ")
    if " l1" in low:
        r.raw_any = ("l1", "l 1")
    return r


def _sku_lot_ok(lot: dict, r: SkuRule, use_sizes: bool) -> bool:
    raw = (lot["raw"] or "").lower()
    return (lot["species"] in r.species
            and (not r.region or lot["region"] == r.region)
            and (not r.processing or lot["processing"] in r.processing)
            and (not r.forms or lot["form"] in r.forms)
            and (not r.raw_any or any(x in raw or x in (lot["weight_class"] or "").lower() for x in r.raw_any))
            and (not use_sizes or not r.sizes or r.sizes & _sizes(lot["weight_class"])))


async def fetch_attracted_stock(session: aiohttp.ClientSession, headers: dict) -> list[dict]:
    """Позиции папки «ПРИВЛЕЧЕННЫЕ ТОВАРЫ» с остатком > 0, без образцов."""
    async with session.get(f"{MS_BASE}/entity/productfolder", headers=headers,
                           params={"filter": f"name={ATTRACTED_FOLDER}"}) as r:
        r.raise_for_status()
        folders = (await r.json())["rows"]
    if not folders:
        raise RuntimeError(f"в МойСклад нет папки «{ATTRACTED_FOLDER}»")
    href = folders[0]["meta"]["href"]
    rows, offset = [], 0
    while True:
        async with session.get(f"{MS_BASE}/report/stock/all", headers=headers, params={
            "filter": f"productFolder={href};stockMode=positiveOnly",
            "limit": 1000, "offset": offset,
        }) as r:
            r.raise_for_status()
            page = (await r.json())["rows"]
        rows += page
        if len(page) < 1000:
            break
        offset += 1000
    out = []
    for p in rows:
        if "образец" in p["name"].lower() or "образцов" in p["name"].lower():
            continue
        out.append({
            "name": p["name"],
            "stock": round(p.get("stock", 0), 2),
            "uom": (p.get("uom") or {}).get("name", ""),
            "cost": round(p.get("price", 0) / 100),        # себестоимость на складе
            "sale": round(p.get("salePrice", 0) / 100),
        })
    return out


def _attracted_species(skus: list[dict]) -> list[str]:
    return sorted({sp for x in skus if (r := _sku_rule(x["name"])) for sp in r.species})


def match_attracted(fetchall, skus: list[dict], week_end: date) -> list[dict]:
    """К каждой позиции подбираем предложения того же вида, разделки, региона и калибра
    (что есть в названии) за неделю и за 4 недели до. Если по калибру не нашлось ничего,
    сравниваем без калибра и честно помечаем это. Нерыбные позиции (рис, имбирь,
    наггетсы) лотов не имеют – по ним модель смотрит только текст сообщений."""
    week_start = week_end - timedelta(days=6)
    base_start = week_start - timedelta(days=BASELINE_DAYS)
    wanted = _attracted_species(skus)
    lots = fetchall(LOTS_SQL, (base_start, week_end, wanted)) if wanted else []
    out = []
    for sku in skus:
        item = {"позиция": sku["name"], "остаток": f'{sku["stock"]} {sku["uom"]}',
                "наша_себестоимость": sku["cost"] or None, "наша_цена_продажи": sku["sale"] or None}
        r = _sku_rule(sku["name"])
        if r:
            mine = [l for l in lots if _sku_lot_ok(l, r, use_sizes=True)]
            if r.sizes and not mine:
                mine = [l for l in lots if _sku_lot_ok(l, r, use_sizes=False)]
                item["калибр_не_сопоставлен"] = True
            week = [l for l in mine if l["received_at"] >= week_start]
            base = [l for l in mine if l["received_at"] < week_start]
            item.update(_stats(week, base))
            # Разница считается в коде, чтобы модель не делала арифметику сама.
            item["рынок_мин_к_нашей_себестоимости_%"] = _pct(item["мин_неделя"], sku["cost"])
            for l, full in zip(item["дешевле_всего"], sorted(week, key=lambda l: l["price"])):
                l["как_у_поставщика"] = full["raw"]
        out.append(item)
    return out


# ─── Внешние источники: Фишнет, Фишретейл ───────────────────────────────────

# Без Accept Фишретейл держит соединение до таймаута (проверено 29.09.2026).
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120 Safari/537.36",
      "Accept": "text/html,application/xhtml+xml,*/*", "Accept-Language": "ru"}
FISHNET = "https://www.fishnet.ru"
FISHNET_SECTIONS = ["rynok", "syrievaya_baza", "aquaculture_news", "promysel_i_pererabotka"]
ARTICLE_MAX = 1500          # обычная статья – начало текста
PRICE_ARTICLE_MAX = 5000    # статья с таблицей цен – строки по Москве и заголовки
# Статьи, которые точно нужны модели, и темы, по которым статью стоит читать целиком.
KEYWORDS = ("лосос", "форел", "кет", "скумбр", "тунец", "тунц", "маслян", "креветк",
            "кальмар", "мид", "гребеш", "осьмин", "угор", "треск", "судак", "минтай",
            "камбал", "сибас", "дорадо", "лангустин", "икр", "чили", "норвег", "фарер",
            "иран", "турц", "карел", "мурманск", "цен", "рын", "обзор", "импорт", "пошлин",
            "пелаги", "семг", "путин")


def _html_to_text(fragment: str) -> str:
    t = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", fragment, flags=re.S)
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"</t[dh]>", " | ", t)
    t = re.sub(r"</(p|tr|div|h\d|li)>|<br\s*/?>", "\n", t)
    t = _html.unescape(re.sub(r"<[^>]+>", "", t)).replace("\xa0", " ")
    t = re.sub(r"[ \t]+", " ", t)
    return re.sub(r"\n\s*\n+", "\n", t).strip()


async def _get(session: aiohttp.ClientSession, url: str, tries: int = 3, timeout: int = 40) -> str:
    last = None
    for attempt in range(tries):
        try:
            async with session.get(url, headers=UA, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status >= 500:
                    raise RuntimeError(f"HTTP {r.status}")
                r.raise_for_status()
                return await r.text(errors="ignore")
        except Exception as e:           # сеть/502 – повторяем с паузой
            last = e
            await asyncio.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{url}: {last}")


def _fishnet_listing(page: str) -> list[tuple[str, str, date]]:
    items = re.findall(r'<a class="h3" href="([^"]+)">\s*(.*?)\s*</a>.*?<time datetime="(\d{4}-\d\d-\d\d)',
                       page, flags=re.S)
    return [(u if u.startswith("http") else FISHNET + u, _html.unescape(re.sub(r"\s+", " ", t)).strip(),
             date.fromisoformat(d)) for u, t, d in items]


def _fishnet_article(page: str, is_price: bool) -> tuple[str, bool]:
    i = page.find("block_news_detail")
    j = page.find("Вернуться", i)
    text = _html_to_text(page[i:j if j > 0 else i + 60000])
    text = text.split("\n", 1)[1] if text.startswith("block_news_detail") else text
    paid = "Платные новости доступны только подписчикам" in text
    if is_price:
        # Таблица на 10–15 тыс. знаков: берём вводный абзац, заголовки разделов и строки по Москве.
        lines = text.split("\n")
        keep = lines[:6] + [l for l in lines[6:] if "Москва" in l or ("|" not in l and len(l) < 60)]
        return "\n".join(keep)[:PRICE_ARTICLE_MAX], paid
    return text[:ARTICLE_MAX], paid


async def collect_fishnet(session: aiohttp.ClientSession, week_end: date) -> dict:
    week_start = week_end - timedelta(days=6)
    seen, items, errors = set(), [], []
    for sec in FISHNET_SECTIONS:
        for n in (1, 2):
            url = f"{FISHNET}/news/{sec}/" + (f"?page={n}" if n > 1 else "")
            try:
                listing = _fishnet_listing(await _get(session, url))
            except Exception as e:
                errors.append(str(e))
                break
            fresh = [x for x in listing if week_start <= x[2] <= week_end]
            for u, title, d in fresh:
                if u not in seen:
                    seen.add(u)
                    items.append({"раздел": sec, "дата": d.isoformat(), "заголовок": title, "ссылка": u})
            if not listing or min(x[2] for x in listing) < week_start:
                break                     # дальше страницы старше недели
    for it in items:
        low = it["заголовок"].lower()
        if not any(k in low for k in KEYWORDS):
            continue                      # по заголовку не про наш ассортимент – только заголовок
        is_price = "оптовые цены" in low or "обзор оптовых цен" in low
        try:
            it["текст"], paid = _fishnet_article(await _get(session, it["ссылка"]), is_price)
            if paid:
                it["текст"] = "платная статья, текст недоступен"
        except Exception as e:
            errors.append(str(e))
    return {"статьи": items, "ошибки": errors}


FISHRETAIL = "https://fishretail.ru"
# Мониторинг цен у Фишретейла платный (без подписки отдаёт пустой шаблон), доска
# объявлений – без цен. Поэтому берём только новости: «Главные» и «Обзоры отрасли».
FISHRETAIL_SECTIONS = ["general", "branch"]
MONTHS = {m: i for i, m in enumerate(["января", "февраля", "марта", "апреля", "мая", "июня", "июля",
                                      "августа", "сентября", "октября", "ноября", "декабря"], 1)}


def _fishretail_listing(page: str, year: int) -> list[dict]:
    """Лента идёт блоками: <h3 class="news-list__title">25 сентября</h3>, затем статьи дня."""
    out, day = [], None
    for m in re.finditer(r'<h3 class="news-list__title">\s*(.*?)\s*</h3>|'
                         r'<a class="news-item__content" href="([^"]+)">\s*<h4 class="news-item__title">\s*(.*?)\s*</h4>'
                         r'.*?<p class="news-item__text">\s*(.*?)\s*</p>', page, flags=re.S):
        if m.group(1):
            d = re.search(r"(\d{1,2}) ([а-я]+)", m.group(1))
            day = date(year, MONTHS[d.group(2)], int(d.group(1))) if d and d.group(2) in MONTHS else None
        elif day:
            url = m.group(2)
            out.append({"дата": day, "заголовок": _html.unescape(m.group(3)).strip(),
                        "текст": _html.unescape(re.sub(r"\s+", " ", m.group(4))).strip(),
                        "ссылка": "https:" + url if url.startswith("//") else url})
    return out


async def collect_fishretail(session: aiohttp.ClientSession, week_end: date) -> dict:
    week_start = week_end - timedelta(days=6)
    seen, items, errors = set(), [], []
    for sec in FISHRETAIL_SECTIONS:
        for n in range(1, 6):
            url = f"{FISHRETAIL}/news/{sec}" + (f"?page={n}" if n > 1 else "")
            try:
                listing = _fishretail_listing(await _get(session, url), week_end.year)
            except Exception as e:
                errors.append(str(e))
                break
            for it in listing:
                if week_start <= it["дата"] <= week_end and it["ссылка"] not in seen:
                    seen.add(it["ссылка"])
                    low = (it["заголовок"] + " " + it["текст"]).lower()
                    items.append({"раздел": sec, "дата": it["дата"].isoformat(), "заголовок": it["заголовок"],
                                  "ссылка": it["ссылка"],
                                  **({"текст": it["текст"]} if any(k in low for k in KEYWORDS) else {})})
            if not listing or min(x["дата"] for x in listing) < week_start:
                break
    return {"статьи": items, "ошибки": errors}


async def collect_external(week_end: date) -> dict:
    async with aiohttp.ClientSession() as s:
        fishnet, fishretail = await asyncio.gather(collect_fishnet(s, week_end),
                                                   collect_fishretail(s, week_end))
    return {"fishnet": fishnet, "fishretail": fishretail}


# ─── Текст отчёта ───────────────────────────────────────────────────────────

import json
import logging
import os

logger = logging.getLogger(__name__)

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
MODEL = os.getenv("WEEKLY_MARKET_MODEL", "claude-opus-5")
MAX_REPORT = 3500            # знаков видимого текста; лимит Telegram 4096

SYSTEM = """Ты аналитик закупок F2B (производство рыбного филе + поставщик HoReCa Москвы).
Пишешь пятничный обзор рынка для закупщиков в Telegram-канал. Читатели – закупщики
Кристина и Александра и собственник. Им нужно за минуту понять, что изменилось за неделю.

Данные (JSON):
- «сырьё» – 12 категорий сырья. Цены в ₽/кг уже посчитаны: минимум и медиана за неделю,
  медиана за 4 недели до, изменение в %, число предложений и поставщиков, 3 самых дешёвых.
- «привлечённые» – наши товары на перепродажу с остатком на складе: себестоимость,
  цена продажи и предложения поставщиков того же вида за неделю. Поле «как_у_поставщика» –
  исходное название лота. «рынок_мин_к_нашей_себестоимости_%» уже посчитано.
- «сообщения» – что закупщики принесли в канал «Мониторинг» (тексты, имена файлов).
- «fishnet», «fishretail» – статьи отраслевых сайтов за неделю (заголовок, иногда текст, ссылка).

ЖЁСТКИЕ ПРАВИЛА
1. Каждое число бери только из данных. Ничего не считай сам и не округляй по-своему.
   Нет числа в данных – пиши словами, без цифр.
2. Предложение поставщика сравнивай с нашей позицией только если по «как_у_поставщика»
   это тот же товар (вид, разделка, калибр, форма). «Филе гигантского кальмара» – не наш
   «командорский кальмар», наггетсы из креветки – не наша креветка в панировке. Сомневаешься – не сравнивай.
3. Если предложений за неделю не было, пиши «за неделю предложений в Мониторинге не было».
   Это не дефицит на рынке. Дефицит пиши только если о нём прямо сказано в сообщениях или статьях.
4. Коротко. Одна строка – одна мысль, до 20 слов. Без вводных, без выводов «в целом»,
   без воды и без советов, которых не просили.
5. «изменение_медианы_%» – это медиана недели к медиане 4 недель до неё. Пиши «к прошлому
   месяцу», не «за неделю» и не «за четыре недели».
6. Текст после тире начинай со строчной буквы.
7. Только русский язык, без английских слов и жаргона. Только короткое тире «–», никогда «—».
   Без эмодзи. Не используй конструкции «не X, а Y».

ЧТО ВЕРНУТЬ – строго JSON без пояснений:
{
 "главное": "1–2 предложения: самое важное за неделю для закупки",
 "привлечённые": ["строка", ...],   // до 8 строк, только позиции, по которым есть что сказать:
                                     // рынок заметно дешевле/дороже нашей себестоимости (от 5%),
                                     // цена выросла/упала за неделю, пропали предложения, важная новость.
                                     // Формат строки: «<позиция коротко> – <что произошло с цифрой>».
 "сырьё": [{"категория": "<точно как в данных>", "текст": "<одна строка>"}, ...],  // все 12, по порядку
 "рынок": [{"текст": "<одна строка: что случилось и чем важно для нас>", "источник": "Фишнет|Фишретейл", "ссылка": "<url из данных>"}, ...]
                                     // 2–5 строк, только то, что влияет на наши 12 категорий
                                     // или привлечённые товары: цены, путина, квоты, импорт, пошлины.
}"""


def answer_text(data: dict) -> str:
    # У моделей с размышлением первым идёт блок thinking – берём первый текстовый.
    return next((b["text"] for b in data.get("content", []) if b.get("type") == "text"), "")


def _parse_json(raw: str) -> dict:
    raw = raw.strip()
    raw = re.sub(r"^```(?:json)?|```$", "", raw, flags=re.M).strip()
    return json.loads(raw[raw.find("{"): raw.rfind("}") + 1])


async def ask_model(data: dict) -> dict:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY не задан")
    payload = {"model": MODEL, "max_tokens": 8000, "system": SYSTEM,
               "messages": [{"role": "user", "content": json.dumps(data, ensure_ascii=False, default=str)}]}
    headers = {"x-api-key": api_key, "anthropic-version": ANTHROPIC_VERSION,
               "content-type": "application/json"}
    async with aiohttp.ClientSession() as session:
        async with session.post(ANTHROPIC_URL, json=payload, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=300)) as r:
            if r.status != 200:
                raise RuntimeError(f"Anthropic {r.status}: {(await r.text())[:300]}")
            return _parse_json(answer_text(await r.json()))


def unknown_numbers(report: dict, data: dict) -> list[str]:
    """Числа от 100 и выше (цены, объёмы), которых нет во входных данных, – признак,
    что модель что-то посчитала или придумала сама. Проценты и калибры мелкие – их не ловим."""
    src = json.dumps(data, ensure_ascii=False, default=str).replace("\u00a0", "")
    known = set(re.findall(r"\d+", src.replace(" ", "")))
    text = json.dumps(report, ensure_ascii=False)
    found = {n.replace(" ", "") for n in re.findall(r"\d[\d ]*\d|\d", text)}
    return sorted(n for n in found if n.isdigit() and int(n) >= 100 and n not in known)


def _esc(t: str) -> str:
    return _html.escape(t.replace("—", "–"), quote=False)


def render(report: dict, week_label: str) -> str:
    lines = [f"<b>Обзор рынка за {week_label}</b>", "", _esc(report.get("главное", ""))]
    if report.get("привлечённые"):
        lines += ["", "<b>Привлечённые товары</b>"] + [f"• {_esc(x)}" for x in report["привлечённые"]]
    lines += ["", "<b>Сырьё</b>"] + [f"• <b>{_esc(x['категория'])}</b> – {_esc(x['текст'])}"
                                     for x in report.get("сырьё", [])]
    if report.get("рынок"):
        lines += ["", "<b>Рынок</b>"]
        for x in report["рынок"]:
            src = _esc(x.get("источник", ""))
            url = x.get("ссылка") or ""
            tail = f' (<a href="{_html.escape(url)}">{src}</a>)' if url.startswith("http") else (f" ({src})" if src else "")
            lines.append(f"• {_esc(x['текст'])}{tail}")
    return "\n".join(lines)


def visible_len(html_text: str) -> int:
    return len(_html.unescape(re.sub(r"<[^>]+>", "", html_text)))


async def send_telegram(token: str, chat_id: int | str, html_text: str) -> None:
    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{token}/sendMessage", json={
            "chat_id": chat_id, "text": html_text, "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=aiohttp.ClientTimeout(total=60)) as r:
            body = await r.json()
            if not body.get("ok"):
                raise RuntimeError(f"Telegram: {body}")


def build_data(fetchall, skus: list[dict], external: dict, week_end: date) -> dict:
    ws = week_end - timedelta(days=6)
    return {
        "неделя": f"{ws:%d.%m}–{week_end:%d.%m.%Y}",
        "сырьё": collect_raw_categories(fetchall, week_end),
        "привлечённые": match_attracted(fetchall, skus, week_end),
        "сообщения": collect_messages(fetchall, week_end),
        **external,
    }


async def make_report(data: dict, week_end: date) -> tuple[str, list[str]]:
    """Отчёт в HTML для Telegram + список подозрительных чисел. Одна повторная попытка,
    если модель вышла за длину или принесла чужие числа."""
    ws = week_end - timedelta(days=6)
    label = f"{ws:%d.%m}–{week_end:%d.%m}"
    report = await ask_model(data)
    text, bad = render(report, label), unknown_numbers(report, data)
    if bad or visible_len(text) > MAX_REPORT:
        logger.warning("weekly_market: повтор, чужие числа %s, длина %s", bad, visible_len(text))
        report = await ask_model(data)
        text, bad = render(report, label), unknown_numbers(report, data)
    return text, bad


# ─── Задача в боте ──────────────────────────────────────────────────────────
# bot.py регистрирует run_job раз в 30 минут. Шлём в пятницу с 16:00 МСК, один раз за
# ISO-неделю (отметка в bot_settings). Если бот перезапустился в 16:00 – догонит
# следующим тиком. Упало – пишем собственнику и пробуем на следующем тике, но не
# больше MAX_ATTEMPTS раз за неделю, чтобы не долбить модель и сайты.

from datetime import datetime, timezone

MSK = timezone(timedelta(hours=3))
SENT_KEY = "weekly_market_sent"          # value = "2026-W40"
FAIL_KEY = "weekly_market_fail"          # value = "2026-W40:<число попыток>"
MAX_ATTEMPTS = 3


def _setting(db, key: str) -> str:
    row = db._fetchone("SELECT value FROM bot_settings WHERE key=%s", (key,))
    return (row or {}).get("value") or ""


def _set_setting(db, key: str, value: str) -> None:
    db._execute("INSERT INTO bot_settings (key, value) VALUES (%s, %s) "
                "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", (key, value))


def _unavailable_note(external: dict) -> str:
    notes = []
    for key, name in (("fishnet", "Фишнет"), ("fishretail", "Фишретейл")):
        part = external.get(key) or {}
        if part.get("ошибки") and not part.get("статьи"):
            notes.append(name)
    return f"\n\n<i>{' и '.join(notes)} не ответил{'и' if len(notes) > 1 else ''} – статьи без них.</i>" if notes else ""


async def run_job(app, db, channel_id: int, owner_chat_id: int, force: bool = False) -> str | None:
    now = datetime.now(MSK)
    week = f"{now:%G-W%V}"
    fail = "" if force else _setting(db, FAIL_KEY)
    if not force:
        if now.weekday() != 4 or now.hour < 16:
            return None
        if _setting(db, SENT_KEY) == week:
            return None
        if fail.startswith(week + ":") and int(fail.split(":")[1]) >= MAX_ATTEMPTS:
            return None
    week_end = now.date()
    try:
        from moysklad import get_headers
        async with aiohttp.ClientSession() as s:
            skus = await fetch_attracted_stock(s, get_headers())
        external = await collect_external(week_end)
        data = build_data(db._fetchall, skus, external, week_end)
        text, bad = await make_report(data, week_end)
        text += _unavailable_note(external)
        await app.bot.send_message(channel_id, text, parse_mode="HTML", disable_web_page_preview=True)
        if not force:
            _set_setting(db, SENT_KEY, week)
        if bad:
            await app.bot.send_message(owner_chat_id, "Обзор рынка ушёл в «Мониторинг», но в нём есть "
                                       f"числа, которых нет в данных: {', '.join(bad)}. Проверьте.")
        logger.info("weekly_market: отправлен за %s, знаков %s", week, visible_len(text))
        return week
    except Exception as e:
        logger.error("weekly_market: %s", e, exc_info=True)
        n = int(fail.split(":")[1]) + 1 if fail.startswith(week + ":") else 1
        if not force:
            _set_setting(db, FAIL_KEY, f"{week}:{n}")
        try:
            await app.bot.send_message(owner_chat_id, f"Обзор рынка в «Мониторинг» не собрался "
                                       f"(попытка {n} из {MAX_ATTEMPTS}): {type(e).__name__}: {str(e)[:300]}")
        except Exception:
            logger.exception("weekly_market: не смог сообщить собственнику")
        return None


# ─── Локальный прогон ───────────────────────────────────────────────────────
# python3 weekly_market_report.py [YYYY-MM-DD] [--data | --print | --send CHAT_ID]
#   --data   только собранные данные (JSON), без модели
#   --print  отчёт в консоль
#   --send   отчёт в Telegram указанному чату (для проверки – личка собственника)
# Env: DATABASE_URL, MOYSKLAD_TOKEN, ANTHROPIC_API_KEY, TG_BOT_TOKEN (для --send).
if __name__ == "__main__":
    import ssl
    import sys

    import asyncpg

    async def _main():
        args = [a for a in sys.argv[1:] if not a.startswith("--")]
        week_end = date.fromisoformat(args[0]) if args and "-" in args[0] else date.today()
        # Mac: asyncpg без ALPN (psycopg2 с Mac к Amvera не подключается).
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        conn = await asyncpg.connect(os.environ["DATABASE_URL"].split("?")[0], ssl=ctx)
        cache: dict = {}

        async def prefetch(sql, params):
            n = iter(range(1, 100))
            q = re.sub(r"%s", lambda _: f"${next(n)}", sql)
            cache[(sql, tuple(map(str, params)))] = [dict(r) for r in await conn.fetch(q, *params)]

        # Синхронный интерфейс как у db._fetchall: запросы сделаны заранее, отдаём по ключу.
        def fetchall(sql, params):
            return cache[(sql, tuple(map(str, params)))]

        # moysklad.get_headers локально не импортируется (модуль требует ключ геокодера).
        headers = {"Authorization": f"Bearer {os.environ['MOYSKLAD_TOKEN']}", "Accept-Encoding": "gzip"}
        async with aiohttp.ClientSession() as s:
            skus = await fetch_attracted_stock(s, headers)
        ws = week_end - timedelta(days=6)
        bs = ws - timedelta(days=BASELINE_DAYS)
        await prefetch(LOTS_SQL, (bs, week_end, sorted({c.species for c in CATEGORIES})))
        await prefetch(LOTS_SQL, (bs, week_end, _attracted_species(skus)))
        await prefetch(MESSAGES_SQL, (ws, week_end + timedelta(days=1)))
        await conn.close()

        data = build_data(fetchall, skus, await collect_external(week_end), week_end)
        if "--data" in sys.argv:
            print(json.dumps(data, ensure_ascii=False, indent=1, default=str))
            return
        text, bad = await make_report(data, week_end)
        print(text)
        print(f"\n--- видимых знаков: {visible_len(text)}; чисел не из данных: {bad or 'нет'}")
        if "--send" in sys.argv:
            chat = sys.argv[sys.argv.index("--send") + 1]
            await send_telegram(os.environ["TG_BOT_TOKEN"], chat, text)
            print(f"отправлено в {chat}")

    asyncio.run(_main())
