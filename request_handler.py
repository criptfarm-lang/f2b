"""Заявки на закупку — handler для бота «Эф» (Phase 3.2).

План: plans/2026-05-26-procurement-requests.md в репо f2b-second-brain.

Менеджер жмёт «📝 Новая заявка» → вставляет текст → LLM парсит → preview →
подтверждение → INSERT в procurement.requests + TG-нотификация закупщику.

Зеркало parser/router из procurement_app/lib — копия sync с dictionaries.md.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# ─── ENUM (sync с .business/raw-materials/dictionaries.md) ─────────────────

SPECIES_ENUM = {
    # Рыба
    "лосось", "форель", "кета", "горбуша", "нерка", "кижуч", "чавыча",
    "треска", "пикша", "минтай", "судак", "сибас", "дорадо", "палтус",
    "зубатка", "масляная", "тилапия", "пангасиус",
    "угорь", "тунец", "скумбрия", "сельдь",
    # Морепродукты
    "креветка", "кальмар", "осьминог", "гребешок", "мидия", "устрица",
    "краб", "омар", "лангустин",
    # Икра
    "икра",
    # Сопутка для HoReCa (HoReCa-one-stop-shop, решение собственника 2026-05-27)
    "сыр", "молочка", "масло-сливочное", "масло-растительное",
    "овощи", "фрукты", "зелень", "специи", "соусы",
    "рис", "крупы", "мука", "тесто",
    "мясо", "птица", "яйцо",
    "водоросли", "упаковка",
    "прочее",
}
PROCESSING_ENUM = {
    "НПСГ", "ПСГ", "ПБГ", "Б/Г",
    "Trim PR", "Trim A", "Trim B", "Trim C", "Trim D",
    "стейк", "кусок", "тушка", "хвост", "unspecified",
}
STATE_ENUM = {"охл", "IQF", "блок", "глубокая-заморозка", "сушёный", "unspecified"}
PRODUCT_FORM_ENUM = {
    "сырьё", "слабосоль", "сильносоль", "х/к", "г/к",
    "сухой-засол", "вяленое", "консервы",
}
REGION_ENUM = {
    "Чёрное море РФ", "Мурманск", "Карелия", "Северная Осетия",
    "Северо-Запад РФ", "Дальний Восток РФ", "Каспий",
    "Армения", "Беларусь", "Казахстан",
    "Иран", "Турция-озеро", "Турция-море", "Грузия", "Норвегия",
    "Фарерские о-ва", "Чили", "Аргентина", "Эквадор", "Перу",
    "Китай", "Вьетнам", "Таиланд", "Индия", "Индонезия", "Бангладеш",
}

# ─── Маппинг закупщиков (sync с dictionaries.md::species_to_owner) ─────────

BELYAKOVA_SPECIES = {"лосось", "форель", "масляная"}
KRISTINA_GO_LIVE = date(2026, 6, 2)

# TG chat_id закупщиков (sync с manager_chats в БД, см. memory). При смене
# состава — править здесь + в dictionaries.md.
ASSIGNEE_TG = {
    "belyakova": 8267564735,
    "kristina":  8185545246,
    "victor":    360092495,
}

# Виктор как fallback при species_unclassified (получает все 3 одновременно).
UNCLASSIFIED_NOTIFY_TG = [
    ASSIGNEE_TG["belyakova"],
    ASSIGNEE_TG["kristina"],
    ASSIGNEE_TG["victor"],
]


def route_request(species: Optional[str], today: Optional[date] = None) -> Optional[str]:
    today = today or date.today()
    if species is None:
        return None
    species = species.strip().lower()
    if species in BELYAKOVA_SPECIES:
        return "belyakova"
    if today < KRISTINA_GO_LIVE:
        return "victor"
    return "kristina"


# ─── LLM-парсер ────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Ты — парсер заявок на закупку рыбы и морепродуктов для компании F2B.
Менеджер вставляет свободный текст запроса от клиента. Твоя задача — извлечь
структурированные поля и вернуть JSON. Никаких комментариев, никакого markdown.

ENUM-значения (используй ТОЛЬКО эти, если не совпадает — возвращай null):

species: F2B — поставщик «одного окна» для HoReCa. Принимаем заявки и на рыбу,
и на сопутку (сыр для роллов, овощи, соусы, рис и т.д.).

КОНКРЕТНЫЙ продукт идёт в species, а не в subspecies. Примеры:
«майонез Печагин» → species=майонез, brand=Печагин.
«сыр творожный» → species=сыр, subspecies=творожный.
«кетчуп Heinz» → species=кетчуп, brand=Heinz.
«сметана 20%» → species=сметана.
«огурцы свежие» → species=огурцы.

Рыба: лосось, форель, кета, горбуша, нерка, кижуч, чавыча, треска, пикша,
минтай, судак, сибас, дорадо, палтус, зубатка, масляная, тилапия, пангасиус,
угорь, тунец, скумбрия, сельдь.
Морепродукты: креветка, кальмар, осьминог, гребешок, мидия, устрица, краб,
омар, лангустин.
Икра: икра (для всех видов, subspecies = лососёвая/кетовая/щучья/форелевая).

Сопутка для HoReCa — конкретные позиции в species:
- Сыры: сыр (subspecies: творожный/сливочный/моцарелла/пармезан/фета/...).
- Молочка: сметана, сливки, йогурт, ряженка, кефир.
- Масла: масло-сливочное, масло-растительное (оливковое/подсолнечное в subspecies).
- Соусы конкретно: майонез, кетчуп, горчица, соевый-соус, мирин, унаги, спайси,
  васаби, ткемали. ВАЖНО: «майонез» — это species, а не subspecies «соусы».
- Овощи, фрукты, зелень: можно как обобщённо «овощи», так и конкретно
  «огурцы», «помидоры», «лимоны», «авокадо», «руккола».
- Крупы и мучное: рис, гречка, мука, тесто, лапша.
- Мясное: мясо (subspecies: говядина/свинина), птица (subspecies: курица/утка), яйцо.
- Водоросли (нори, вакаме в subspecies).
- Упаковка: контейнеры, плёнка, лотки.
- прочее — только если ни одна категория и конкретный продукт не подходят.

Если уверенность в распознавании конкретного продукта < 0.7 — НЕ объединяй в обобщённую
категорию, оставь species=null и низкий confidence. Менеджер дополнит.

processing: НПСГ, ПСГ, ПБГ, Б/Г, Trim PR, Trim A, Trim B, Trim C, Trim D,
стейк, кусок, тушка, хвост, unspecified.

state: охл (охлаждённое), IQF, блок, глубокая-заморозка, сушёный, unspecified.
«с/м», «свежемороженое», «зам», «замороженное», «з/м» → глубокая-заморозка.
ВАЖНО: «х/к», «г/к», «слабосоль», «копчёное» — это НЕ state, а product_form.

product_form: сырьё, слабосоль, сильносоль, х/к (холодного копчения),
г/к (горячего копчения), сухой-засол, вяленое, консервы. По умолчанию «сырьё».

weight_class: свободный текст («5-6», «4+», «1.5-2.0», «2-3», «3.5+»).

volume_kg: МЕСЯЧНАЯ потребность клиента в килограммах — сколько ему нужно
товара за месяц. «200 кг» → 200. «1 тонна» → 1000. «1 т в месяц» → 1000.
ВАЖНО: если число рядом с упаковкой («бочка 100 кг», «канистра 5л», «пакет 1 кг»)
— это размер тары, идёт в package, а НЕ в volume_kg.
В volume_kg идёт ОТДЕЛЬНОЕ число общего месячного потребления.
Если непонятно (одиночное число без контекста) — оставь volume_kg=null.

package: вид упаковки / тара (свободный текст). Примеры:
«бочка 100 кг», «канистра 5 л», «лоток 250 г», «пакет 1 кг»,
«коробка 6 шт по 1 л», «короб 10 кг». Иначе null.

target_price_rub_kg: ЦЕНА ПРОДАЖИ КЛИЕНТУ в рублях за кг, не бюджет закупки.
Менеджер пишет «продаю по 720 ₽», «отдадим клиенту за 800» — это сюда.
«До 720 ₽», «не больше 720» — тоже сюда (потолок цены для клиента).

target_date: ISO дата СТРОГО ПОСЛЕ сегодня (`{today}`, день недели: `{today_dow}`).
ПРАВИЛО для «к {{dow}}»: ближайший будущий {{dow}}, СТРОГО позднее today.
Сегодня == целевой dow → +7 дней.
Примеры (today=2026-05-26 вторник): «к понедельнику» → 2026-06-01;
«к четвергу» → 2026-05-28; «к вторнику» → 2026-06-02; «завтра» → 2026-05-27;
«на следующей неделе» → 2026-06-01.

subspecies: ваннамеи/тигровая/северная/гребенчатая для креветки;
лососёвая/кетовая/щучья/форелевая для икры. Регион — отдельное поле.

region: «чёрноморская» → Чёрное море РФ; «карельская» → Карелия;
«мурманский/-ая» → Мурманск; «осетинская» → Северная Осетия;
«армянская» → Армения; «иранская» → Иран; «чилийский» → Чили;
«норвежский» → Норвегия. ENUM (используй ТОЛЬКО эти):
Чёрное море РФ, Мурманск, Карелия, Северная Осетия, Северо-Запад РФ,
Дальний Восток РФ, Каспий, Армения, Беларусь, Казахстан, Иран,
Турция-озеро, Турция-море, Грузия, Норвегия, Фарерские о-ва, Чили,
Аргентина, Эквадор, Перу, Китай, Вьетнам, Таиланд, Индия, Индонезия, Бангладеш.

client_hint: имя клиента — ресторан / сеть / ООО / ИП / название заведения.
ВАЖНО: фамилия или название рядом с продуктом — это обычно БРЕНД, не клиент.
Примеры: «майонез Печагин» → brand=Печагин, client_hint=null.
«хлеб Riga Pekarnia» → brand=Riga Pekarnia, client_hint=null.
«Лосось 5-6 для ресторана Сахалин» → client_hint=Сахалин, brand=null.
Клиент почти всегда явно упомянут как «для X», «клиент Y», «ресторан Z», «ООО».

brand: бренд / производитель продукта (не магазин-клиент). Пример:
«майонез Печагин» → brand=Печагин. «Сыр Hochland» → brand=Hochland.
Иначе null.

confidence: 0.00-1.00. < 0.50 если основные поля (species + processing) не извлекаются.

Возвращай СТРОГО JSON со всеми полями.

Пример рыба: «Лосось ПБГ 5-6 охл 200 кг к четвергу, клиенту по 720 ₽» →
{{"species": "лосось", "subspecies": null, "brand": null, "region": null,
 "weight_class": "5-6", "processing": "ПБГ", "state": "охл",
 "product_form": "сырьё", "package": null, "volume_kg": 200,
 "target_price_rub_kg": 720, "target_date": "2026-05-28",
 "client_hint": null, "confidence": 0.95}}

Пример сопутка: «майонез Печагин 100 кг 240 ₽» →
{{"species": "майонез", "subspecies": null, "brand": "Печагин", "region": null,
 "weight_class": null, "processing": null, "state": null,
 "product_form": null, "package": null, "volume_kg": 100,
 "target_price_rub_kg": 240, "target_date": null,
 "client_hint": null, "confidence": 0.95}}

Пример сопутка с упаковкой: «майонез Mr.Ricco в канистрах 5 кг» →
{{"species": "майонез", "subspecies": null, "brand": "Mr.Ricco", "region": null,
 "weight_class": null, "processing": null, "state": null,
 "product_form": null, "package": "канистра 5 кг", "volume_kg": null,
 "target_price_rub_kg": null, "target_date": null,
 "client_hint": null, "confidence": 0.90}}
"""


@dataclass
class ParsedRequest:
    species: Optional[str]
    subspecies: Optional[str]
    brand: Optional[str]
    region: Optional[str]
    weight_class: Optional[str]
    processing: Optional[str]
    state: Optional[str]
    product_form: Optional[str]
    package: Optional[str]
    volume_kg: Optional[float]
    target_price_rub_kg: Optional[float]
    target_date: Optional[str]    # ISO string для JSON-сериализации в draft
    client_hint: Optional[str]
    confidence: float
    raw_text: str = ""


def _validate_enum(v: Optional[str], allowed: set) -> Optional[str]:
    if v is None:
        return None
    return v if v in allowed else None


async def parse_request_text(text: str, today: Optional[date] = None) -> ParsedRequest:
    """Дёргает Claude Haiku 4.5 + валидирует ENUM."""
    from anthropic import AsyncAnthropic

    today = today or date.today()
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY не задан")
    client = AsyncAnthropic(api_key=api_key)

    dows = ["понедельник", "вторник", "среда", "четверг",
            "пятница", "суббота", "воскресенье"]
    system = SYSTEM_PROMPT.format(
        today=today.isoformat(), today_dow=dows[today.weekday()]
    )

    msg = await client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=512,
        system=[{"type": "text", "text": system,
                 "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": text.strip()}],
    )
    raw = msg.content[0].text.strip()
    json_match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not json_match:
        return ParsedRequest(
            species=None, subspecies=None, brand=None, region=None, weight_class=None,
            processing=None, state=None, product_form=None, package=None,
            volume_kg=None, target_price_rub_kg=None, target_date=None, client_hint=None,
            confidence=0.0, raw_text=text,
        )
    try:
        data = json.loads(json_match.group(0))
    except json.JSONDecodeError:
        return ParsedRequest(
            species=None, subspecies=None, brand=None, region=None, weight_class=None,
            processing=None, state=None, product_form=None, package=None,
            volume_kg=None, target_price_rub_kg=None, target_date=None, client_hint=None,
            confidence=0.0, raw_text=text,
        )

    # species: enum-валидацию НЕ применяем — для HoReCa-сопутки LLM может вернуть
    # «майонез», «кетчуп», «зелёный лук» и др., которых в enum нет. Принимаем как есть.
    species_raw = data.get("species")
    species = species_raw.strip().lower() if species_raw else None
    processing = _validate_enum(data.get("processing"), PROCESSING_ENUM)
    state = _validate_enum(data.get("state"), STATE_ENUM)
    product_form = _validate_enum(data.get("product_form"), PRODUCT_FORM_ENUM)
    region = _validate_enum(data.get("region"), REGION_ENUM)

    td_raw = data.get("target_date")
    td = None
    if td_raw:
        try:
            d = datetime.strptime(td_raw, "%Y-%m-%d").date()
            if today <= d <= today + timedelta(days=90):
                td = d.isoformat()
        except ValueError:
            pass

    def _num(v):
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    return ParsedRequest(
        species=species,
        subspecies=data.get("subspecies"),
        brand=data.get("brand"),
        region=region,
        weight_class=data.get("weight_class"),
        processing=processing,
        state=state,
        product_form=product_form,
        package=data.get("package"),
        volume_kg=_num(data.get("volume_kg")),
        target_price_rub_kg=_num(data.get("target_price_rub_kg")),
        target_date=td,
        client_hint=data.get("client_hint"),
        confidence=float(data.get("confidence") or 0.0),
        raw_text=text,
    )


# ─── DB ────────────────────────────────────────────────────────────────────

def insert_request(db, parsed: ParsedRequest, created_by_tg: int,
                   created_by_name: str, assigned_to: Optional[str]) -> int:
    """INSERT в procurement.requests + event 'created'. Возвращает request_id."""
    with db.conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO procurement.requests(
                created_by_tg, created_by_name, raw_text,
                species, subspecies, brand, region, weight_class,
                processing, state, product_form, package,
                volume_kg, target_price_rub_kg, target_date,
                client_name, assigned_to, llm_confidence
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
            RETURNING request_id
            """,
            (
                created_by_tg, created_by_name, parsed.raw_text,
                parsed.species, parsed.subspecies, parsed.brand, parsed.region,
                parsed.weight_class, parsed.processing, parsed.state,
                parsed.product_form, parsed.package,
                parsed.volume_kg, parsed.target_price_rub_kg,
                parsed.target_date,
                parsed.client_hint, assigned_to, parsed.confidence,
            ),
        )
        request_id = cur.fetchone()["request_id"]
        cur.execute(
            """
            INSERT INTO procurement.request_events(request_id, actor, event_type, payload)
            VALUES (%s, %s, %s, %s)
            """,
            (request_id, f"manager:{created_by_tg}", "created",
             json.dumps(asdict(parsed), ensure_ascii=False)),
        )
        db.conn.commit()
    return request_id


# ─── Валидация полноты заявки ──────────────────────────────────────────────

# Обязательные поля: без них «Подтвердить» недоступен. Решено собственником
# 2026-05-26: «не пропускаем без цены как минимум» + species + объём как фундамент.
REQUIRED_FIELDS = [
    ("species",              "вид/категория"),
    ("volume_kg",             "месячная потребность (кг)"),
    ("target_price_rub_kg",   "цена для клиента (₽/кг)"),
]

# Желательные: при отсутствии — preview покажет рекомендацию, но «Подтвердить»
# доступно (для морепродуктов калибр не всегда применим).
RECOMMENDED_FIELDS = [
    ("weight_class",   "навеска"),
]


def validate_request(parsed: "ParsedRequest") -> tuple[list[str], list[str]]:
    """Возвращает (missing_required, missing_recommended)."""
    missing_req = []
    for field, label in REQUIRED_FIELDS:
        if getattr(parsed, field) in (None, "", "unspecified"):
            missing_req.append(label)
    missing_rec = []
    for field, label in RECOMMENDED_FIELDS:
        if getattr(parsed, field) in (None, "", "unspecified"):
            missing_rec.append(label)
    return missing_req, missing_rec


# ─── Форматирование preview / нотификации ─────────────────────────────────

def _fmt_field(label: str, value, suffix: str = "") -> str:
    if value is None or value == "" or value == "unspecified":
        return f"  • {label}: —"
    return f"  • {label}: *{value}*{suffix}"


# Категории species, относящиеся к РЫБЕ/МОРЕПРОДУКТАМ. Для них preview
# показывает «рыбные» поля (processing, state, region, weight_class).
# Для сопутки эти поля скрываются как нерелевантные.
FISH_SPECIES = SPECIES_ENUM - {
    "сыр", "молочка", "масло-сливочное", "масло-растительное",
    "овощи", "фрукты", "зелень", "специи", "соусы",
    "рис", "крупы", "мука", "тесто",
    "мясо", "птица", "яйцо", "водоросли", "упаковка", "прочее",
}


def format_preview(parsed: ParsedRequest, assigned_to: Optional[str],
                   missing_req: list[str] = None,
                   missing_rec: list[str] = None) -> str:
    missing_req = missing_req or []
    missing_rec = missing_rec or []

    is_fish = (parsed.species or "").lower() in FISH_SPECIES

    target_str = None
    if parsed.target_date:
        try:
            d = datetime.strptime(parsed.target_date, "%Y-%m-%d").date()
            dows = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
            target_str = f"{d.strftime('%d.%m')} ({dows[d.weekday()]})"
        except ValueError:
            target_str = parsed.target_date

    confidence_warn = ""
    if parsed.confidence < 0.8:
        confidence_warn = f"\n⚠️ Уверенность парсера: *{parsed.confidence:.0%}* — проверь поля"

    assignee_disp = {
        "belyakova": "Александре Беляковой",
        "kristina":  "Кристине Павленко",
        "victor":    "Виктору",
        None:        "общая очередь (вид не определён)",
    }.get(assigned_to, f"`{assigned_to}`")

    # Блок «не хватает» сверху — менеджер сразу видит, чего не хватает.
    header = ""
    if missing_req:
        header += (
            f"❌ *Не хватает обязательного:* {', '.join(missing_req)}\n"
            f"_Жми «➕ Дополнить» и пришли это в одном сообщении._\n\n"
        )
    elif missing_rec:
        header += (
            f"💡 *Желательно добавить:* {', '.join(missing_rec)}\n"
            f"_Можно подтвердить и без — но закупщику пригодится._\n\n"
        )

    # Динамическая сборка — показываем только заполненные поля.
    # Для сопутки скрываем «рыбные» поля (разделка/состояние/регион/подвид).
    lines = ["📋 *Понял заявку так:*", ""]
    lines.append(_fmt_field("Вид", parsed.species))
    if parsed.brand:
        lines.append(_fmt_field("Бренд", parsed.brand))
    if is_fish and parsed.subspecies:
        lines.append(_fmt_field("Подвид", parsed.subspecies))
    if is_fish and parsed.region:
        lines.append(_fmt_field("Регион", parsed.region))
    if parsed.weight_class:
        lines.append(_fmt_field("Навеска", parsed.weight_class))
    if is_fish and parsed.processing and parsed.processing != "unspecified":
        lines.append(_fmt_field("Разделка", parsed.processing))
    if is_fish and parsed.state and parsed.state != "unspecified":
        lines.append(_fmt_field("Состояние", parsed.state))
    if parsed.product_form and parsed.product_form != "сырьё":
        lines.append(_fmt_field("Форма", parsed.product_form))
    lines.append(_fmt_field("Месячная потребность", parsed.volume_kg, " кг"))
    if parsed.package:
        lines.append(_fmt_field("Упаковка", parsed.package))
    lines.append(_fmt_field("Цена для клиента", parsed.target_price_rub_kg, " ₽/кг"))
    if target_str:
        lines.append(_fmt_field("Дедлайн", target_str))
    if parsed.client_hint:
        lines.append(_fmt_field("Клиент", parsed.client_hint))

    return (
        header
        + "\n".join(lines)
        + f"\n\n🛒 Закупщик: *{assignee_disp}*"
        + confidence_warn
    )


def format_assignee_notification(request_id: int, parsed: ParsedRequest,
                                 created_by_name: str) -> str:
    is_fish = (parsed.species or "").lower() in FISH_SPECIES
    parts = []
    if parsed.species:
        parts.append(parsed.species)
    if parsed.brand:
        parts.append(parsed.brand)
    if is_fish and parsed.subspecies:
        parts.append(parsed.subspecies)
    if is_fish and parsed.region:
        parts.append(parsed.region)
    if is_fish and parsed.processing and parsed.processing != "unspecified":
        parts.append(parsed.processing)
    if parsed.weight_class:
        parts.append(parsed.weight_class)
    if is_fish and parsed.state and parsed.state != "unspecified":
        parts.append(parsed.state)
    if parsed.product_form and parsed.product_form != "сырьё":
        parts.append(parsed.product_form)
    if parsed.volume_kg:
        parts.append(f"{parsed.volume_kg:g} кг")
    if parsed.package:
        parts.append(parsed.package)
    summary = " · ".join(parts) if parts else "(детали в карточке)"

    target = ""
    if parsed.target_date:
        try:
            d = datetime.strptime(parsed.target_date, "%Y-%m-%d").date()
            target = f"\nК {d.strftime('%d.%m')}"
        except ValueError:
            pass
    if parsed.target_price_rub_kg:
        target += f"\nЦена клиенту: {parsed.target_price_rub_kg:g} ₽/кг"

    base = os.getenv("PROCUREMENT_APP_URL", "https://f2b-procurement-victor03.amvera.io")
    url = f"{base}/requests/{request_id}"

    return (
        f"🆕 *Новая заявка №{request_id}*\n"
        f"От: {created_by_name}\n\n"
        f"{summary}"
        f"{target}\n\n"
        f"[Открыть карточку]({url})"
    )
