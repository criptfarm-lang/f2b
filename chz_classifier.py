"""
Честный знак: к какой товарной группе маркировки относится товар (бот «Эф»).
План (второй мозг): plans/2026-10-12-чз-новый-товар-в-светофоре-закупки.md — Фаза 2.

В МойСкладе признаков маркировки нет (trackingType, GTIN, ТН ВЭД на 12.10.2026 пустые
почти у всех товаров), поэтому группу определяем по названию и папке:
  1) правила (_RULES) — покрывают весь текущий ассортимент;
  2) что правила не узнали — классификатор ТН ВЭД: Claude Haiku определяет код ТН ВЭД
     и признаки товара (масса, тара, стерилизация), группу вычисляет chz_tnved по
     перечням кодов из постановлений, а не модель (решение собственника 12.10.2026);
  3) результат кэшируется в public.chz_product_class по товару + названию.

Справочник групп (договор с ЧЗ есть/нет, статус) живёт в БД — public.chz_groups.
Сид ниже вставляется один раз (on conflict do nothing), дальше правится в БД:
появился договор — `update chz_groups set has_contract=true where key='oil'`.
Источник статусов — сверка 12.10.2026, .business/company/chestny-znak-markirovka-ryby.md.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import date

import aiohttp

import chz_tnved

logger = logging.getLogger(__name__)

# status: mandatory — обязательна для опта сейчас; upcoming — станет обязательной
# (mandatory_from); experiment — добровольный эксперимент; none — не маркируется.
CHZ_GROUPS: dict[str, dict] = {
    "dairy":   {"title": "Молочная продукция", "status": "mandatory",
                "mandatory_from": None, "has_contract": True, "act": "ПП 2099"},
    "oil":     {"title": "Растительные масла", "status": "mandatory",
                "mandatory_from": None, "has_contract": False, "act": "ПП 676"},
    "sauce":   {"title": "Бакалея: соусы, специи, уксусы, снеки", "status": "mandatory",
                "mandatory_from": None, "has_contract": True, "act": "ПП 1682"},
    # Нанесение кодов: макароны и мёд с 01.09.2026, крупы и рис с 01.12.2026;
    # для опта (ЭДО) — с 01.11.2027.
    "grain":   {"title": "Бакалея: крупы, рис, макароны, мёд, водоросли", "status": "upcoming",
                "mandatory_from": date(2027, 11, 1), "has_contract": True,
                "act": "ПП 1682 в ред. ПП 186"},
    "canned":  {"title": "Консервы", "status": "mandatory",
                "mandatory_from": None, "has_contract": False, "act": "ПП 677"},
    "water":   {"title": "Упакованная вода", "status": "mandatory",
                "mandatory_from": None, "has_contract": False, "act": "ПП 841"},
    "drinks":  {"title": "Безалкогольные напитки и соки", "status": "mandatory",
                "mandatory_from": None, "has_contract": False, "act": "ПП 887"},
    "beer":    {"title": "Пиво и слабоалкогольные напитки", "status": "mandatory",
                "mandatory_from": None, "has_contract": False, "act": "ПП 2173"},
    "meat":    {"title": "Мясные изделия", "status": "upcoming",
                "mandatory_from": date(2027, 6, 1), "has_contract": False, "act": "ПП 205"},
    "frozen":  {"title": "Заморозка и полуфабрикаты", "status": "experiment",
                "mandatory_from": date(2027, 3, 1), "has_contract": False,
                "act": "ПП 1823 в ред. ПП 1099"},
    # Код ТН ВЭД определить не удалось — считаем маркируемым, пока не выяснено.
    "unknown": {"title": "Код ТН ВЭД не определён", "status": "mandatory",
                "mandatory_from": None, "has_contract": False, "act": ""},
    "none":    {"title": "Не маркируется", "status": "none",
                "mandatory_from": None, "has_contract": True, "act": ""},
}

# Папки МС, где лежат хозяйственные закупки и материалы цеха, а не товар на перепродажу.
_OWN_NEEDS_PATHS = ("яПРОЧЕЕ", "УПАКОВКА", "ПИЩЕВЫЕ ДОБАВКИ")
# Наша рыба (охл., с/с, х/к, пресная) и сырьё для цеха — не в потребительской
# заморозке; заморозка в этих папках ловится по слову «заморож»/«с/м».
_OWN_FISH_PATHS = ("ГОТОВАЯ ПРОДУКЦИЯ/ФИЛЕ", "ГОТОВАЯ ПРОДУКЦИЯ/КУСОК",
                   "ГОТОВАЯ ПРОДУКЦИЯ/ПРОЧЕЕ ПРОИЗВОДСТВО", "СЫРЬЕ")
_FROZEN_PATHS = ("ПРИВЛЕЧЕННЫЕ ТОВАРЫ/Рыба", "ПРИВЛЕЧЕННЫЕ ТОВАРЫ/Морепродукты",
                 "ПРИВЛЕЧЕННЫЕ ТОВАРЫ/Полуфабрикаты", "ПРИВЛЕЧЕННЫЕ ТОВАРЫ/Мясо",
                 "ПРИВЛЕЧЕННЫЕ ТОВАРЫ/Икра")

_FROZEN_RE = re.compile(r"с/м|з/м|в/м|заморож|мороже")
_SMALL_PACK_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:гр|г|мл)\b")

# (ключ группы, регэксп по названию, исключения). Только однозначные случаи;
# всё спорное (сыры с ЗМЖ, имбирь, водоросли, уксус, специи) идёт через ТН ВЭД.
# Порядок важен: первое совпадение.
_RULES: list[tuple[str, re.Pattern, re.Pattern | None]] = [
    ("oil", re.compile(r"масло\s+(подсолн|растит|фритюр|оливк|рапс|кукуруз|соев|льнян)|фритюрн"), None),
    ("water", re.compile(r"\bвода\b"), None),
    ("grain", re.compile(r"\bрис\b|рис для|крупа|гречк|пшено|булгур|макарон|спагетти"), None),
    ("sauce", re.compile(r"(^|\s)соус(?!е)|майонез|кетчуп"), None),
]


# Коды, сверенные по первоисточникам 12.10.2026 (разбор спорных позиций, см. план).
# Модель на таких названиях путается от прогона к прогону — фиксируем код и признаки.
# (регэксп по названию, код ТН ВЭД, признаки для chz_tnved.group_by_tnved)
_KNOWN_TNVED: list[tuple[re.Pattern, str, dict]] = [
    (re.compile(r"кокосов\w* молоко"), "2106909808", {"frozen": False}),  # решение ЕЭК № 171
    (re.compile(r"со вкусом сыр|вкусом сыра"), "2106909808",
     {"frozen": False, "milk_fat_replaced": True}),
    (re.compile(r"кунжут"), "1207400000", {}),
    (re.compile(r"панировочн|панко|panko"), "1905909000", {"breadcrumbs": True}),
    (re.compile(r"уксус"), "2209009100", {}),
    (re.compile(r"\bнори\b"), "1212210000", {}),
    (re.compile(r"имбир\w* маринов"), "2001909709", {}),
    (re.compile(r"паста\s+том\s*ям"), "2103909009", {}),
    (re.compile(r"васаби"), "2103909009", {}),
]


def _norm(s: str) -> str:
    return (s or "").lower().replace("ё", "е")


def _small_sauce(name_l: str) -> bool:
    """Соусы массой нетто ≤ 30 г ЧЗ не маркирует (FAQ ЧЗ по бакалее)."""
    m = _SMALL_PACK_RE.search(name_l)
    if not m or "кг" in name_l:
        return False
    return float(m.group(1).replace(",", ".")) <= 30


def classify_by_rules(name: str, path: str) -> tuple[str, str] | None:
    """(ключ группы, причина) или None — правила товар не узнали, нужен Claude."""
    n = _norm(name)
    path = path or ""
    if path.startswith(_OWN_NEEDS_PATHS):
        return "none", "хозяйственная закупка, не на перепродажу"
    # Полуфабрикаты в панировке/кляре (в т.ч. сырные палочки) — заморозка, не молочка.
    if path.startswith("ПРИВЛЕЧЕННЫЕ ТОВАРЫ/Полуфабрикаты"):
        return "frozen", "папка «Полуфабрикаты»"
    # Наша рыба: папка важнее названия («юкола в специях» – не специи).
    if path.startswith(_OWN_FISH_PATHS):
        if _FROZEN_RE.search(n):
            return "frozen", "наша рыба, заморозка"
        return "none", "рыба охл./с/с/х/к – не маркируется"
    # Масаго/тобико (икра летучей рыбы) в группу «икра» не входит, продаётся с/м.
    if re.search(r"масаго|тобико", n):
        return "frozen", "икра летучей рыбы, заморозка"
    if "набор порционных образцов" in n:
        return "none", "наш набор образцов, не товар поставщика"
    for rx, code, flags in _KNOWN_TNVED:
        if rx.search(n):
            m = _SMALL_PACK_RE.search(n)
            w = None if (not m or "кг" in n) else float(m.group(1).replace(",", "."))
            key, why = chz_tnved.group_by_tnved(code, net_weight_g=w, **flags)
            return key, why + " (код сверен вручную)"
    for key, rx, exclude in _RULES:
        if rx.search(n) and not (exclude and exclude.search(n)):
            if key == "sauce" and re.search(r"соус", n) and _small_sauce(n):
                return "none", "соус в упаковке ≤ 30 г – не маркируется"
            return key, f"по названию ({rx.search(n).group(0).strip()})"
    if _FROZEN_RE.search(n) or path.startswith(_FROZEN_PATHS):
        return "frozen", "заморозка (по названию/папке)"
    return None


# ── классификатор ТН ВЭД для того, что правила не узнали ─────────────────────
_LLM_MODEL = "claude-haiku-4-5-20251001"
_LLM_PROMPT = """Ты таможенный классификатор. Определи код ТН ВЭД ЕАЭС (10 знаков) для товара,
который оптовая компания закупает у поставщика и перепродаёт ресторанам (Россия).
Товар: «{name}»
Папка в учётной системе: «{path}»
Учитывай: продукты с заменителем молочного жира – не глава 04 (обычно 2106 или 1901);
приправленные водоросли – глава 20; сушёные листы нори – 1212; маринованные овощи – 2001.
Замороженный товар не бывает стерилизованным: hermetic_sterilized=false.
Ответ – только JSON:
{{"tnved": "<10 цифр>", "net_weight_g": <масса нетто одной упаковки в граммах или null>,
  "hermetic_sterilized": <true если герметичная тара и продукт стерилизован/пастеризован, false если нет, null если неизвестно>,
  "breadcrumbs": <true если это панировочные сухари>,
  "milk_fat_replaced": <true если молочный жир заменён растительным>,
  "frozen": <true если товар продаётся замороженным или это полуфабрикат>,
  "reason": "<до 12 слов>"}}"""


async def classify_by_llm(name: str, path: str) -> tuple[str, str]:
    """Код ТН ВЭД от модели → группа по перечням chz_tnved.
    Прямой HTTP к API (не SDK: на Amvera SDK заморожен на 0.40.0).
    При сбое — «unknown»: считаем маркируемым, пока не выяснено."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return "unknown", "классификатор недоступен (нет ключа)"
    body = {"model": _LLM_MODEL, "max_tokens": 200, "temperature": 0,
            "messages": [{"role": "user", "content": _LLM_PROMPT.format(name=name, path=path)}]}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post("https://api.anthropic.com/v1/messages", json=body,
                              headers={"x-api-key": api_key, "anthropic-version": "2023-06-01"},
                              timeout=aiohttp.ClientTimeout(total=30)) as r:
                data = await r.json()
                if r.status != 200:
                    raise RuntimeError(f"{r.status}: {str(data)[:200]}")
        text = next(b["text"] for b in data["content"] if b.get("type") == "text")
        m = re.search(r"\{.*\}", text, re.S)
        out = json.loads(m.group(0)) if m else {}
    except Exception as e:
        logger.warning(f"chz_classifier: LLM fail for {name!r}: {e!r}")
        return "unknown", "классификатор не ответил"
    key, why = chz_tnved.group_by_tnved(
        str(out.get("tnved") or ""),
        net_weight_g=out.get("net_weight_g"),
        hermetic_sterilized=out.get("hermetic_sterilized"),
        breadcrumbs=bool(out.get("breadcrumbs")),
        milk_fat_replaced=bool(out.get("milk_fat_replaced")),
        frozen=out.get("frozen"),
    )
    return key, f"{why}; {(out.get('reason') or '')[:80]}"


async def classify(name: str, path: str) -> tuple[str, str, str]:
    """(ключ группы, причина, источник rules|llm)."""
    r = classify_by_rules(name, path)
    if r:
        return r[0], r[1], "rules"
    key, reason = await classify_by_llm(name, path)
    return key, reason, "llm"


# ── БД: справочник групп и кэш классификации ────────────────────────────────
DDL = """
create table if not exists public.chz_groups (
    key            text primary key,
    title          text not null,
    status         text not null,
    mandatory_from date,
    has_contract   boolean not null default false,
    act            text,
    updated_at     timestamptz default now()
);
create table if not exists public.chz_product_class (
    product_id  uuid primary key,
    name        text not null,
    path        text,
    group_key   text not null,
    reason      text,
    source      text,
    decided_at  timestamptz default now()
);
"""


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
        for key, g in CHZ_GROUPS.items():
            cur.execute("""
                insert into public.chz_groups (key, title, status, mandatory_from, has_contract, act)
                values (%s, %s, %s, %s, %s, %s) on conflict (key) do nothing
            """, (key, g["title"], g["status"], g["mandatory_from"], g["has_contract"], g["act"]))


def load_groups(conn) -> dict[str, dict]:
    with conn.cursor() as cur:
        cur.execute("select * from public.chz_groups")
        return {r["key"]: dict(r) for r in cur.fetchall()}


async def classify_cached(conn, product_id: str, name: str, path: str) -> dict:
    """Группа товара из кэша; если товара нет или его переименовали — классифицируем заново."""
    with conn.cursor() as cur:
        cur.execute("select group_key, reason, source, name from public.chz_product_class "
                    "where product_id=%s", (product_id,))
        row = cur.fetchone()
    if row and row["name"] == name:
        return {"key": row["group_key"], "reason": row["reason"], "source": row["source"]}
    key, reason, source = await classify(name, path)
    with conn.cursor() as cur:
        cur.execute("""
            insert into public.chz_product_class (product_id, name, path, group_key, reason, source, decided_at)
            values (%s, %s, %s, %s, %s, %s, now())
            on conflict (product_id) do update set name=excluded.name, path=excluded.path,
              group_key=excluded.group_key, reason=excluded.reason, source=excluded.source,
              decided_at=now()
        """, (product_id, name, path, key, reason, source))
    return {"key": key, "reason": reason, "source": source}
