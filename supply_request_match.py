"""
Блок «Заявка» для светофора Заказа поставщику (бот «Эф»).

План (второй мозг): plans/2026-09-24-заявка-в-светофоре-заказа-поставщику.md.
Повод — разбор закупок привлечёнки 24.09.2026: 63,6% закупок шли без заявки
менеджера, объём заказа не сверялся с заявленной потребностью, на складе
зависло 1,36 млн ₽ (кальмар 830 кг при заявке на 60 кг/мес и т.п.).

Что делает: для позиции Заказа поставщику из папки «ПРИВЛЕЧЕННЫЕ ТОВАРЫ»
ищет заявки менеджеров в procurement.requests (вид + калибр, окно 90 дней)
и считает три проверки — наличие заявки, объём, наценка к цене клиента.

Пороги (решения собственника 24.09.2026):
  наличие: есть 🟢 / есть по виду, но калибр другой 🟡 / нет 🔴
  объём:   ≤1× заявленного 🟢 / 1–2× 🟡 / >2× 🔴
  цена:    наценка ≥20% 🟢 / 10–20% 🟡 / <10% 🔴
Цвет блока — худший из трёх. Заявкой считается только поданная менеджером
(created_by_tg <> OWNER_CHAT_ID); запросы клиентов из Wazzup не в счёт.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

MATCH_WINDOW_DAYS = 90
MARKUP_GREEN_PCT = 20.0
MARKUP_YELLOW_PCT = 10.0
VOLUME_GREEN_RATIO = 1.0
VOLUME_YELLOW_RATIO = 2.0

ATTRACTED_PREFIX = "ПРИВЛЕЧЕННЫЕ ТОВАРЫ"

# ── словарь видов ────────────────────────────────────────────────────────────
# (подстрока в нижнем регистре, канон). Порядок важен: специфичное — раньше
# общего («лангустин» до «кревет», «крабовые палочки» до «краб»).
_CANON_KEYS: list[tuple[str, str]] = [
    ("крабов", "сурими"), ("снежный краб", "сурими"), ("сурими", "сурими"),
    ("лангустин", "лангустин"),
    ("кревет", "креветка"), ("ваннамей", "креветка"), ("ванамей", "креветка"),
    ("каракатиц", "каракатица"),
    ("кальмар", "кальмар"),
    ("осьминог", "осьминог"),
    ("гребеш", "гребешок"),
    ("мидии", "мидия"), ("мидий", "мидия"), ("мидия", "мидия"),
    ("устриц", "устрица"),
    ("краб", "краб"),
    ("тунец", "тунец"), ("саку", "тунец"),
    ("судак", "судак"),
    ("сибас", "сибас"),
    ("дорадо", "дорадо"),
    ("минтай", "минтай"),
    ("треск", "треска"),
    ("пикш", "пикша"),
    ("камбал", "камбала"),
    ("палтус", "палтус"),
    ("угольн", "угольная"),
    ("навага", "навага"),
    ("зубатк", "зубатка"),
    ("угорь", "угорь"), ("унаги", "угорь"),
    ("масляная", "масляная"), ("эсколар", "масляная"),
    ("тилапия", "тилапия"),
    ("пангасиус", "пангасиус"),
    ("скумбри", "скумбрия"),
    ("сельдь", "сельдь"),
    ("горбуш", "горбуша"),
    ("нерка", "нерка"),
    ("кижуч", "кижуч"),
    ("кета", "кета"),
    ("форел", "форель"),
    ("лосос", "лосось"), ("сёмга", "лосось"), ("семга", "лосось"),
    ("масаго", "икра"), ("тобико", "икра"), ("икра", "икра"),
    # сопутка HoReCa
    ("чука", "водоросли"), ("нори", "водоросли"), ("вакаме", "водоросли"),
    ("рис ", "рис"), ("рис,", "рис"), ("рис для суши", "рис"),
    ("имбир", "имбирь"),
    ("васаби", "васаби"),
    ("соевый соус", "соевый-соус"), ("соевый-соус", "соевый-соус"),
    ("уксус", "уксус"), ("мирин", "мирин"),
    ("моцарелл", "сыр"), ("творожн", "сыр"), ("сыр", "сыр"),
    ("крем со вкусом сыра", "сыр"), ("милетто", "сыр"),
    ("масло подсолнеч", "масло-растительное"), ("масло растительн", "масло-растительное"),
    ("олейна", "масло-растительное"), ("фритюр", "масло-растительное"),
    ("наггетс", "птица"), ("стрипс", "птица"), ("куриц", "птица"),
    ("куриное", "птица"), ("куриные", "птица"), ("грудк", "птица"), ("бедр", "птица"),
    ("картоф", "картофель"),
    ("панко", "панировка"), ("панировк", "панировка"),
    ("палочки бамбук", "бамбук"), ("бамбук", "бамбук"),
]

_CALIBER_RE = re.compile(r"(\d{1,4})\s*[-/]\s*(\d{1,4})")
_CALIBER_L_RE = re.compile(r"\bl\s?([1-9])\b")

# Разделка и обработка — две независимые оси. Сравниваем только внутри своей оси:
# «филе» (разделка) и «очищенная» (обработка) друг другу не противоречат, а вот
# «филе» vs «тушка» или «в панцире» vs «очищенная» — противоречат.
_CUT_KEYS: list[tuple[str, str]] = [
    ("лойн", "лойн"), ("лоин", "лойн"), ("спинк", "лойн"),
    ("филе", "филе"),
    ("стейк", "стейк"),
    ("кольц", "кольца"),
    ("щупальц", "щупальца"),
    ("тушка", "тушка"), ("тушки", "тушка"),
    ("н/р", "н/р"), ("непотрош", "н/р"),
]
_TREAT_KEYS: list[tuple[str, str]] = [
    ("очищ", "очищенная"), ("б/п", "очищенная"),
    ("панцир", "в панцире"),
]


_CANON_VALUES = {canon for _, canon in _CANON_KEYS}


def detect_canon(*parts: str | None) -> str | None:
    """Канонический вид по любому тексту (название МС, species+subspecies заявки)."""
    text = " ".join(p for p in parts if p).lower().strip()
    if not text:
        return None
    # species заявки часто уже равен канону («рис», «минтай») — подстроки не нужны.
    for part in parts:
        v = (part or "").strip().lower()
        if v in _CANON_VALUES:
            return v
    for key, canon in _CANON_KEYS:
        if key in text:
            return canon
    return None


def _canon_mentioned(canon: str, text: str | None) -> bool:
    """Упомянут ли вид в тексте хоть одним из своих ключей."""
    t = (text or "").lower()
    if not t:
        return False
    if canon in t:
        return True
    return any(key in t for key, c in _CANON_KEYS if c == canon)


def detect_cut(*parts: str | None) -> str | None:
    """Разделка: филе / лойн / тушка / стейк / кольца / щупальца / н/р."""
    text = " ".join(p for p in parts if p).lower()
    for key, cut in _CUT_KEYS:
        if key in text:
            return cut
    return None


def detect_treat(*parts: str | None) -> str | None:
    """Обработка: очищенная / в панцире."""
    text = " ".join(p for p in parts if p).lower()
    for key, treat in _TREAT_KEYS:
        if key in text:
            return treat
    return None


def norm_caliber(*parts: str | None) -> str | None:
    """Калибр к виду «16-20» / «l1». Берём первый диапазон в тексте."""
    text = " ".join(p for p in parts if p).lower()
    m = _CALIBER_L_RE.search(text)
    if m:
        return f"l{m.group(1)}"
    m = _CALIBER_RE.search(text)
    if m:
        return f"{int(m.group(1))}-{int(m.group(2))}"
    return None


def is_attracted(path: str | None) -> bool:
    return bool(path) and path.startswith(ATTRACTED_PREFIX)


# ── заявки ───────────────────────────────────────────────────────────────────
def load_recent_requests(conn, days: int = MATCH_WINDOW_DAYS) -> list[dict]:
    """Заявки менеджеров за окно (без отменённых). Один запрос на весь заказ."""
    owner = (os.getenv("OWNER_CHAT_ID") or "0").strip()
    owner_id = int(owner) if owner.lstrip("-").isdigit() else 0
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with conn.cursor() as cur:
        cur.execute("""
            select request_id, created_at, created_by_name, species, subspecies,
                   weight_class, volume_kg, target_price_rub_kg, client_name, raw_text
              from procurement.requests
             where created_at >= %s
               and coalesce(created_by_tg, 0) <> %s
               and coalesce(status, '') <> 'отменено'
             order by created_at desc
        """, (since, owner_id))
        rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        enrich_request(r)
    return rows


def enrich_request(r: dict) -> dict:
    """Канон/калибр/форма заявки. Вид берём из species+subspecies; raw_text —
    только если species пуст или «прочее» (иначе заявка на горбушу с упоминанием
    минтая в тексте прилипает к минтаю)."""
    species = (r.get("species") or "").strip().lower()
    raw = r.get("raw_text") or ""
    if species and species != "прочее":
        canon = detect_canon(species, r.get("subspecies"))
        # Парсер иногда подставляет не тот вид (каракатицу записал осьминогом).
        # Если текст заявки этот вид вообще не упоминает, а даёт другой — верим тексту.
        canon_txt = detect_canon(raw)
        if canon and canon_txt and canon_txt != canon and not _canon_mentioned(canon, raw):
            canon = canon_txt
        r["canon"] = canon
    else:
        r["canon"] = detect_canon(species, r.get("subspecies"), raw)
    # Имитация краба — отдельный товар, а не краб.
    if r["canon"] == "краб" and any(w in raw.lower() for w in ("палочк", "имитац", "сурими")):
        r["canon"] = "сурими"
    r["caliber"] = norm_caliber(r.get("weight_class")) or norm_caliber(r.get("raw_text"))
    r["cut"] = detect_cut(r.get("processing"), r.get("raw_text"))
    r["treat"] = detect_treat(r.get("processing"), r.get("raw_text"))
    return r


def _short_name(full: str | None) -> str:
    """«Карина Баласанян 🦭» → «Баласанян». Фамилия — второе слово, если есть."""
    parts = [p for p in re.sub(r"[^\w\s\-]", " ", (full or "")).split() if p]
    return parts[1] if len(parts) >= 2 else (parts[0] if parts else "—")


def match_position(name: str, qty: float, price: float, requests: list[dict],
                   uom: str | None = None) -> dict:
    """Три проверки по позиции. Возвращает dict с color/line-данными.

    color: green/yellow/red/white (white — вид не распознан, считать нечем).
    """
    canon = detect_canon(name)
    if not canon:
        return {"color": "white", "reason": "вид не распознан", "matched": []}

    caliber = norm_caliber(name)
    cut = detect_cut(name)
    treat = detect_treat(name)
    same_species = [r for r in requests if r.get("canon") == canon]
    if not same_species:
        return {"color": "red", "reason": f"заявок на «{canon}» за {MATCH_WINDOW_DAYS} дн нет",
                "matched": [], "canon": canon}

    # Признак заявки, который не задан, конфликтом не считается.
    def _fits(r: dict) -> bool:
        if caliber and r.get("caliber") and r["caliber"] != caliber:
            return False
        if cut and r.get("cut") and r["cut"] != cut:
            return False
        if treat and r.get("treat") and r["treat"] != treat:
            return False
        return True

    matched = [r for r in same_species if _fits(r)]
    if not matched:
        # Заявки на вид есть, но на другой калибр или другую разделку — по факту
        # заказанный товар никто не просил (кейсы осьминога 20/40 и угря 420-500).
        others = []
        for r in same_species[:3]:
            tag = " / ".join(x for x in [r.get("caliber"), r.get("cut"), r.get("treat")] if x)
            others.append(f"#{r['request_id']} {tag or '—'}")
        want = " / ".join(x for x in [caliber, cut, treat] if x) or "—"
        return {"color": "red", "canon": canon, "matched": [], "near": same_species,
                "reason": f"на «{canon}» просили другое ({', '.join(others)}), в заказе {want}"}

    # Заявки с совпавшим калибром — вперёд: они точнее описывают заказанное.
    matched.sort(key=lambda r: (r.get("caliber") != caliber, r.get("created_at")), reverse=False)

    volume_req = sum(float(r["volume_kg"]) for r in matched if r.get("volume_kg"))
    prices = [float(r["target_price_rub_kg"]) for r in matched if r.get("target_price_rub_kg")]
    price_target = max(prices) if prices else None
    price_min = min(prices) if prices else None

    colors = ["green"]
    non_kg = bool(uom) and uom.lower() not in ("кг", "kg")

    # Объём (только если единица измерения — кг и в заявках есть объём)
    volume_ratio = None
    volume_note = ""
    if non_kg:
        volume_note = f"ед. «{uom}» — объём и цену сверить вручную"
    elif volume_req > 0:
        volume_ratio = (qty or 0) / volume_req
        if volume_ratio <= VOLUME_GREEN_RATIO:
            colors.append("green")
        elif volume_ratio <= VOLUME_YELLOW_RATIO:
            colors.append("yellow")
        else:
            colors.append("red")
    else:
        volume_note = "объём в заявке не указан"

    # Цена
    markup_pct = None
    if non_kg:
        price_target = price_min = None
        price_note = ""
    elif price_target and price and price > 0:
        markup_pct = (price_target - price) / price * 100
        if markup_pct >= MARKUP_GREEN_PCT:
            colors.append("green")
        elif markup_pct >= MARKUP_YELLOW_PCT:
            colors.append("yellow")
        else:
            colors.append("red")
    if non_kg:
        price_note = ""
    else:
        price_note = "" if price_target else "цены в заявке нет"

    rank = {"red": 3, "yellow": 2, "green": 1, "white": 0}
    color = max(colors, key=lambda c: rank[c])

    return {
        "color": color,
        "canon": canon,
        "caliber": caliber,
        "cut": cut,
        "treat": treat,
        "matched": matched,
        "volume_req": volume_req,
        "volume_ratio": volume_ratio,
        "volume_note": volume_note,
        "price_target": price_target,
        "price_min": price_min,
        "markup_pct": markup_pct,
        "price_note": price_note,
    }


def format_line(res: dict, icon) -> str:
    """Строка блока для алерта (без иконки цвета — её ставит вызывающий)."""
    if res["color"] == "white":
        return f"   Заявка: ⚪ {res.get('reason', 'не определить')}"
    if not res.get("matched"):
        return f"   Заявка: {icon('red')} {res.get('reason', 'заявок нет')}"

    matched = res["matched"]
    head = matched[0]
    who = _short_name(head.get("created_by_name"))
    ids = ", ".join(f"#{r['request_id']}" for r in matched[:3])
    if len(matched) > 3:
        ids += f" и ещё {len(matched) - 3}"
    if len(matched) > 1:
        others = {_short_name(r.get("created_by_name")) for r in matched[1:4]} - {who}
        if others:
            who += " + " + ", ".join(sorted(others))

    parts = [f"{who} {ids}"]
    if res.get("volume_req") and not res.get("volume_note"):
        vr = res.get("volume_ratio")
        vol = f"просили {res['volume_req']:.0f} кг"
        if vr is not None and vr > VOLUME_GREEN_RATIO:
            vol += f", заказ {vr:.1f}×"
        parts.append(vol)
    elif res.get("volume_note"):
        parts.append(res["volume_note"])
    if res.get("price_target"):
        pr = f"клиенту {res['price_target']:.0f} ₽"
        if res.get("price_min") and res["price_min"] != res["price_target"]:
            pr = f"клиенту {res['price_min']:.0f}–{res['price_target']:.0f} ₽"
        if res.get("markup_pct") is not None:
            pr += f" → {res['markup_pct']:+.0f}%"
        parts.append(pr)
    elif res.get("price_note"):
        parts.append(res["price_note"])

    return f"   Заявка: {icon(res['color'])} " + " · ".join(parts)
