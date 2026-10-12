"""
Честный знак: к какой товарной группе маркировки относится товар (бот «Эф»).
План (второй мозг): plans/2026-10-12-чз-новый-товар-в-светофоре-закупки.md — Фаза 2.

В МойСкладе признаков маркировки нет (trackingType, GTIN, ТН ВЭД на 12.10.2026 пустые
почти у всех товаров), поэтому группу определяем по названию и папке:
  1) правила (_RULES) — покрывают весь текущий ассортимент;
  2) что правила не узнали — Claude Haiku, ответ строго одним ключом из CHZ_GROUPS;
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

logger = logging.getLogger(__name__)

# status: mandatory — обязательна для опта сейчас; upcoming — станет обязательной
# (mandatory_from); experiment — добровольный эксперимент; none — не маркируется.
CHZ_GROUPS: dict[str, dict] = {
    "dairy":   {"title": "Молочная продукция", "status": "mandatory",
                "mandatory_from": None, "has_contract": True, "act": "ПП 2099"},
    "oil":     {"title": "Растительные масла", "status": "mandatory",
                "mandatory_from": None, "has_contract": False, "act": "ПП 676"},
    "sauce":   {"title": "Бакалея: соусы и специи", "status": "mandatory",
                "mandatory_from": None, "has_contract": True, "act": "ПП 1682"},
    "grain":   {"title": "Бакалея: крупы, макароны, мёд", "status": "upcoming",
                "mandatory_from": date(2026, 12, 1), "has_contract": True,
                "act": "ПП 1682 в ред. ПП 186"},
    "canned":  {"title": "Консервы", "status": "mandatory",
                "mandatory_from": None, "has_contract": False, "act": "ПП 677"},
    "water":   {"title": "Упакованная вода", "status": "mandatory",
                "mandatory_from": None, "has_contract": False, "act": "ПП 841"},
    "meat":    {"title": "Мясные изделия", "status": "upcoming",
                "mandatory_from": date(2027, 6, 1), "has_contract": False, "act": "ПП 205"},
    "frozen":  {"title": "Заморозка и полуфабрикаты", "status": "experiment",
                "mandatory_from": date(2027, 3, 1), "has_contract": False,
                "act": "ПП 1823 в ред. ПП 1099"},
    # Модель считает, что товар маркируется, но не в группах выше — проверяет Малышкин.
    "unknown": {"title": "Возможно маркируется, группа не определена", "status": "mandatory",
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

_FROZEN_RE = re.compile(r"с/м|з/м|заморож|мороже")
_SMALL_PACK_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:гр|г|мл)\b")

# (ключ группы, регэксп по названию, исключения). Порядок важен: первое совпадение.
_RULES: list[tuple[str, re.Pattern, re.Pattern | None]] = [
    ("oil", re.compile(r"масло\s+(подсолн|растит|фритюр|оливк|кунжутн|рапс|кукуруз|соев|льнян)"
                       r"|фритюрн|маргарин|спред"), None),
    ("canned", re.compile(r"консерв|\bж/б\b|в собственном соку|шпрот|тушенк"), None),
    ("water", re.compile(r"\bвода\b"), None),
    ("dairy", re.compile(r"\bсыр(\b|ы\b|а\b|н|ом\b)|творож|сливк|сливочн|молок|молочн|сметан"
                         r"|йогурт|кефир|маскарпоне|моцарел|пармез|филадельф|cream cheese"),
     re.compile(r"кокос|сливочно-|в панировке")),
    ("grain", re.compile(r"\bрис\b|рис для|крупа|гречк|пшено|булгур|кускус|киноа|макарон|спагетти"
                         r"|лапша|\bмед\b|мёд|овсян|перлов"), None),
    ("sauce", re.compile(r"(^|\s)соус(?!е)|майонез|кетчуп|горчиц|специ|приправ|перец\s+(черн|молот|красн)"
                         r"|паприк|кориандр|васаби|карри|корица|куркум|паста\s+том\s*ям"), None),
    ("meat", re.compile(r"колбас|сосиск|сардельк|ветчин|бекон|окорок|карбонад"), None),
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
    for key, rx, exclude in _RULES:
        if rx.search(n) and not (exclude and exclude.search(n)):
            if key == "sauce" and re.search(r"соус", n) and _small_sauce(n):
                return "none", "соус в упаковке ≤ 30 г – не маркируется"
            return key, f"по названию ({rx.search(n).group(0).strip()})"
    if _FROZEN_RE.search(n) or path.startswith(_FROZEN_PATHS):
        return "frozen", "заморозка (по названию/папке)"
    return None


# ── Claude для того, что правила не узнали ───────────────────────────────────
_LLM_MODEL = "claude-haiku-4-5-20251001"
_LLM_PROMPT = """Ты помогаешь бухгалтерии оптовой компании (рыба и продукты для ресторанов, Россия, октябрь 2026).
Определи, к какой товарной группе системы маркировки «Честный знак» относится товар.
Группы (ответь ровно одним ключом):
- dairy: молочная продукция, сыры, в т.ч. сырные продукты с заменителем молочного жира
- oil: растительные масла и масложировая продукция
- sauce: соусы (2103), специи и пряности (0904–0910)
- grain: крупы, рис, макароны, лапша, мёд
- canned: консервы (рыбные, мясные и т.п., в герметичной таре)
- water: упакованная питьевая вода
- meat: готовые мясные изделия (колбасы, сосиски, ветчина)
- frozen: замороженная продукция и полуфабрикаты (рыба, морепродукты, мясо, овощи)
- unknown: товар, вероятно, маркируется, но не входит ни в одну группу выше (напитки, пиво, табак, лекарства и т.п.)
- none: не маркируется
Если сомневаешься между none и группой – выбирай группу.
Товар: «{name}»
Папка в учётной системе: «{path}»
Ответ – только JSON: {{"key": "<ключ>", "reason": "<до 12 слов>"}}"""


async def classify_by_llm(name: str, path: str) -> tuple[str, str]:
    """Прямой HTTP к API (не SDK: на Amvera SDK заморожен на 0.40.0).
    При сбое — «unknown»: пусть лучше посмотрит Малышкин, чем пропустим."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return "unknown", "модель недоступна (нет ключа) – проверить вручную"
    body = {"model": _LLM_MODEL, "max_tokens": 120,
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
        key = out.get("key")
        if key not in CHZ_GROUPS:
            return "unknown", f"модель ответила непонятно: {text[:60]}"
        return key, "модель: " + (out.get("reason") or "")[:80]
    except Exception as e:
        logger.warning(f"chz_classifier: LLM fail for {name!r}: {e!r}")
        return "unknown", "модель не ответила – проверить вручную"


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
