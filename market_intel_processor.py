"""F2B — обработчик сообщений канала «Мониторинг» в procurement.lots.

План 2026-05-22, Фаза 1.4 (закрыта 2026-05-26). Cron-задача в боте «Эф»:
каждые 30 минут 9-19 МСК читает unprocessed сообщения из market_intel_messages,
парсит через Anthropic SDK (text/photo через Claude haiku-4-5),
записывает в procurement.lots, помечает обработанным.

PDF на этом этапе SKIPPED (требует chunking из-за лимита output tokens на больших
прайс-листах типа Мореодор 300+ позиций). Vision-обработка PDF планируется в
следующей итерации; пока для PDF используется ручной запуск скилла update-market-intel.
"""

import base64
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from claude_ai import get_client

logger = logging.getLogger(__name__)

ANTHROPIC_MODEL = os.getenv("MARKET_INTEL_MODEL", "claude-haiku-4-5-20251001")
# Отдельная модель для PDF — Sonnet 4.6 видит native PDF, понимает таблицы.
# Haiku обрабатывает text/photo (дешевле). См. план 2026-05-28-автопарсер-pdf-прайсов.md.
ANTHROPIC_PDF_MODEL = os.getenv("MARKET_INTEL_PDF_MODEL", "claude-sonnet-4-5-20250929")

# ─── ENUM-перечни (зеркало procurement_app/migrations/001+002.sql) ─────────
SPECIES_ENUM = [
    # лососёвые
    "лосось", "форель", "кета", "горбуша", "нерка", "кижуч", "чавыча",
    # белая рыба
    "треска", "пикша", "минтай", "судак", "сибас", "дорадо", "палтус",
    "зубатка", "масляная", "тилапия", "пангасиус",
    # прочая рыба
    "угорь", "тунец", "скумбрия", "сельдь",
    # морепродукты
    "креветка", "кальмар", "осьминог", "гребешок", "мидия", "устрица",
    "краб", "омар", "лангустин",
    # икра / прочее
    "икра", "прочее",
    # расширение из 002 (Moreodor)
    "осётр", "белуга", "стерлядь", "баррамунди", "барабулька", "ледяная",
    "нототения", "окунь", "эсколар", "кобия", "клыкач", "угольная",
    "хек", "хоки", "мойва", "навага", "корюшка", "омуль", "камбала",
    "сайда", "терпуг", "щука", "марлин", "рыба-меч", "тилапия-галилео",
]

REGION_ENUM = [
    "Чёрное море РФ", "Мурманск", "Карелия", "Северная Осетия",
    "Северо-Запад РФ", "Дальний Восток РФ", "Каспий",
    "Армения", "Беларусь", "Казахстан",
    "Иран", "Турция-озеро", "Турция-море", "Грузия", "Норвегия",
    "Фарерские о-ва", "Чили", "Аргентина", "Эквадор", "Перу",
    "Китай", "Вьетнам", "Таиланд", "Индия", "Индонезия", "Бангладеш",
    "прочее",
    # расширение из 002
    "Уругвай", "Корея", "Мавритания", "Египет", "РФ", "импорт",
    "Вьетнам/Китай",
]

PROCESSING_ENUM = [
    "НПСГ", "ПСГ", "ПБГ", "Б/Г",
    "Trim PR", "Trim A", "Trim B", "Trim C", "Trim D",
    "стейк", "кусок", "тушка", "хвост", "unspecified",
    # расширение из 002
    "НР", "HGT", "HLSO", "HOSO", "PDTO", "PDTL",
    "филе б/к", "филе н/к", "филе с/к", "кубик", "фарш",
    "лоин", "голова", "суповой набор", "J-CUT", "бабочка",
    "ломтики", "Saku",
]

STATE_ENUM = [
    "охл", "IQF", "блок", "глубокая-заморозка", "сушёный",
    "морская заморозка", "береговая заморозка", "полублок",
]

PRODUCT_FORM_ENUM = [
    "сырьё", "слабосоль", "сильносоль", "х/к", "г/к",
    "сухой-засол", "вяленое", "консервы",
    "жаренный", "с соусом",
]

# Маппинг подсказок поставщиков → slug в procurement.suppliers.
# Расширяется по мере появления новых поставщиков в канале.
SUPPLIER_HINT_MAP = {
    "мореодор": "moreodor",
    "moreodor": "moreodor",
    "мурман экспорт": "fish2o-murman-export",
    "fish2o": "fish2o-murman-export",
    "inarctica": "fish2o-murman-export",
    "лаки фиш": "luckyfish",
    "luckyfish": "luckyfish",
    "тем групп": "tem-grupp-artur",
    "tem grupp": "tem-grupp-artur",
    "артур": "tem-grupp-artur",
    "федоренко": "fedorenko-karelia",
    "карелия федоренко": "fedorenko-karelia",
    "смарт": "smart-chm",
    "smart": "smart-chm",
    "хотенко": "khotenko-andrey",
    "волна": "volna-vladimir",
    "ирна": "irna",
    "эрам": "eram-iran",
    "юнифрост": "junifrost-iran",
    "сити-ритейл": "sk-retail",
    "ск ритейл": "sk-retail",
    "дмитрий": "dmitriy-turkey-georgia",
    "sky fish": "sky-fish",
    "skyfish": "sky-fish",
}


def _build_system_prompt() -> str:
    return f"""Ты парсер прайс-листа поставщика рыбы / морепродуктов для F2B (АО ФИШ ТУ БИЗНЕС).

Из текста / фото / PDF извлеки СПИСОК лотов. Один лот = одна товарная позиция одной ступени цены.

Закрытые ENUM-перечни (используй ТОЛЬКО эти значения; если нет подходящего — `прочее`/`unspecified` с пометкой `confidence_self: needs-review`):

species: {", ".join(SPECIES_ENUM)}
regions: {", ".join(REGION_ENUM)}
processing: {", ".join(PROCESSING_ENUM)}
state: {", ".join(STATE_ENUM)}
product_form: {", ".join(PRODUCT_FORM_ENUM)}

ПОДСКАЗКИ для маппинга:
- HG / HGT (head gutted) → processing="ПБГ" или "HGT" если упоминается tail-off
- HON (head-on) → processing="ПСГ"
- HLSO/HOSO/PDTO/PDTL — для креветок, как processing
- ОХЛ / охлаждённое → state="охл"
- IQF / инд. заморозка / индивидуальная заморозка → state="IQF"
- block / блочная → state="блок"
- 21/25, 30/40 и т.п. для креветок — это weight_class (счёт штук на фунт), НЕ вес упаковки
- Вес тары («~30 кг», «8х1 кг», «12*1 кг») — НИКОГДА не уходит в weight_class. Только в conditions.
- «2-3 кг», «3,6-4,5», «1.5-2.0» — это weight_class рыбы
- «PREM» в названии — пометка премиум-сорта; включи в notes, не в species
- Регион «РФ» = Россия общая (если конкретного региона нет)
- «ДВ» = "Дальний Восток РФ"
- Сёмга / Семга = лосось (subspecies="атлантический")

ПРАВИЛА ЦЕНЫ (КРИТИЧНО — F2B работает в МСК):
1. Если в строке прайса несколько городов (СПб/МСК/Липецк/ДВ) → берём цену для МСК. В conditions запиши "МСК".
2. Если цена одна без указания города → берём её, в conditions локацию не указываем.
3. Если в строке несколько цен по условиям оплаты (предоплата/отсрочка) → берём ПРЕДОПЛАТУ как опт. В conditions запиши "предоплата".
4. Если в Caption (text_raw из TG) есть модификатор цены ("+7 руб для МСК", "к цене +7", "по Москве дороже на N", "акция -10%") — ПРИМЕНИ к цене для МСК и зафиксируй факт в notes (например "+7 ₽/кг для МСК из caption").
5. Если в прайсе несколько городов и для МСК ПУСТО (нет цены, нет ● маркера) — строку ПРОПУСКАЕМ (не записываем в lots).
6. ИГНОРИРУЕМ: вес тары/упаковки, период вылова, штрих-коды, артикулы (это либо в notes, либо вообще не записываем).

Формат ответа — СТРОГО JSON-объект, никакого текста до или после:

{{
  "supplier_hint": "название поставщика как видишь (для маппинга на slug)",
  "received_at": "YYYY-MM-DD дата прайса из заголовка или null",
  "lots": [
    {{
      "species": "из enum",
      "subspecies": "опционально — атлантический/ваннамеи/чёрноморская/радужная/...",
      "region": "из enum или null",
      "weight_class": "точно из прайса, нормализуй запятые в точки",
      "processing": "из enum",
      "state": "из enum",
      "product_form": "из enum (default 'сырьё')",
      "price_rub_kg": число,
      "volume_tier": "от 100 кг / от 1.5 т / null",
      "conditions": "склад, упаковка — опционально",
      "raw_text": "исходный текст позиции из прайса",
      "confidence_self": "confirmed | needs-review",
      "notes": "PREM сорт, % аллокации и т.п."
    }}
  ]
}}

Если в сообщении не прайс-лист (короткая заметка, шум) — верни {{"supplier_hint": null, "received_at": null, "lots": []}}.
Если сообщение — длинный многопозиционный прайс (50+ позиций), верни все позиции максимально полно.
"""


async def _call_claude(content, max_tokens: int = 8192, model: Optional[str] = None) -> dict:
    """Универсальный вызов Anthropic API. content — список content-blocks.
    model — если None, используется ANTHROPIC_MODEL (Haiku по умолчанию)."""
    client = get_client()
    response = await client.messages.create(
        model=model or ANTHROPIC_MODEL,
        max_tokens=max_tokens,
        system=_build_system_prompt(),
        messages=[{"role": "user", "content": content}],
    )
    text = response.content[0].text
    # Иногда Claude добавляет ```json ... ``` обёртки — снимаем
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text
        text = text.rsplit("```", 1)[0].strip()
        if text.startswith("json"):
            text = text[4:].lstrip()
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(f"market_intel: JSON decode failed: {e}\ntext={text[:500]}")
        return {"supplier_hint": None, "received_at": None, "lots": []}


async def parse_text_message(text: str) -> dict:
    return await _call_claude([
        {"type": "text", "text": f"Текст сообщения из канала «Мониторинг»:\n\n{text}"}
    ])


async def parse_photo_message(image_path: str, caption: str = "") -> dict:
    img_b64 = base64.b64encode(Path(image_path).read_bytes()).decode()
    media_type = "image/jpeg" if image_path.lower().endswith((".jpg", ".jpeg")) else "image/png"
    return await _call_claude([
        {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": img_b64}},
        {"type": "text", "text": f"Фото прайс-листа из канала «Мониторинг».\nCaption (если есть): {caption or '(нет)'}\nИзвлеки все товарные позиции."},
    ])


async def parse_pdf_message(pdf_path: str, caption: str = "") -> dict:
    """PDF → native document block в Claude Sonnet 4.6.

    План 2026-05-28: одна ступень — Sonnet видит PDF + caption (text_raw из TG)
    одним вызовом, разбирает шапки таблиц, объединённые ячейки, многоколоночность,
    применяет модификаторы цены из caption ("+7 для МСК"). Стоимость ~5 ₽/PDF.
    Предыдущая итерация (pdfplumber → плоский текст → Haiku) на табличных прайсах
    типа Мореодор давала lots=[] — Haiku не справлялся с грязным текстовым месивом.
    """
    pdf_bytes = Path(pdf_path).read_bytes()
    if not pdf_bytes:
        logger.warning(f"market_intel: PDF {pdf_path} пустой")
        return {"supplier_hint": None, "received_at": None, "lots": []}
    pdf_b64 = base64.b64encode(pdf_bytes).decode()

    content = [
        {
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": pdf_b64,
            },
        },
        {
            "type": "text",
            "text": (
                f"PDF-прайс из канала «Мониторинг».\n"
                f"Caption из TG (text_raw — может содержать модификатор цены типа '+7 для МСК', "
                f"информацию о локации, акциях): {caption or '(нет caption)'}\n\n"
                f"Извлеки ВСЕ товарные позиции прайса. Применяй правила цены МСК / предоплата / "
                f"caption-модификаторы из system prompt. Шапку поставщика, реквизиты, "
                f"секции-заголовки CAPS — игнорируй (используй как контекст species, но не "
                f"записывай в lots)."
            ),
        },
    ]
    return await _call_claude(content, max_tokens=32768, model=ANTHROPIC_PDF_MODEL)


def _supplier_slug_from_hint(hint: str) -> tuple[Optional[str], bool]:
    """Возвращает (slug, is_known). is_known=True если матч с SUPPLIER_HINT_MAP,
    False если slug сгенерирован из hint (новый/неопознанный поставщик).
    Лоты от is_known=False помечаются confidence=needs-review."""
    if not hint:
        return None, False
    h = hint.lower().strip()
    for needle, slug in SUPPLIER_HINT_MAP.items():
        if needle in h:
            return slug, True
    # Hint не в карте — генерим slug. Берём первое осмысленное слово, чистим.
    import re as _re
    # Игнорируем мусорные фразы из канала-обёртки.
    GARBAGE = (
        "мониторинг", "ооо фиш ту бизнес", "ооо «фиш ту бизнес»",
        "важно", "не забывайте", "что-то от поставщика",
        "кристина, сюда", "сюда пересылаем", "пересылаем",
    )
    for g in GARBAGE:
        if g in h:
            return None, False
    # Берём первые 30 символов, оставляем только буквы/цифры/пробелы/дефисы.
    cleaned = _re.sub(r"[^a-zа-я0-9\s\-]", "", h[:30]).strip()
    cleaned = _re.sub(r"\s+", "-", cleaned)
    if len(cleaned) < 2:
        return None, False
    return cleaned, False


def _insert_lots(db, lots: list[dict], supplier_id: str, msg_id: int,
                 source_file: str, received_at: str) -> tuple[int, int]:
    """INSERT в procurement.lots. Возвращает (inserted, skipped)."""
    try:
        valid_until = (datetime.strptime(received_at, "%Y-%m-%d").date()
                       + timedelta(days=14)).isoformat()
    except (ValueError, TypeError):
        # fallback: сегодня + 14 дней
        valid_until = (datetime.now().date() + timedelta(days=14)).isoformat()
        received_at = datetime.now().date().isoformat()

    db._ensure_connection() if hasattr(db, "_ensure_connection") else None
    conn = db.conn
    inserted, skipped = 0, 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO procurement.suppliers (slug, name) VALUES (%s, %s) ON CONFLICT (slug) DO NOTHING",
                (supplier_id, supplier_id),
            )
            for lot in lots:
                # валидация ENUM на стороне Python: если значение не в списке — needs-review
                species = lot.get("species")
                if species not in SPECIES_ENUM:
                    species, lot["confidence_self"] = "прочее", "needs-review"
                region = lot.get("region")
                if region and region not in REGION_ENUM:
                    region = None
                processing = lot.get("processing")
                if processing not in PROCESSING_ENUM:
                    processing = "unspecified"
                state = lot.get("state")
                if state not in STATE_ENUM:
                    state = "глубокая-заморозка"
                product_form = lot.get("product_form", "сырьё")
                if product_form not in PRODUCT_FORM_ENUM:
                    product_form = "сырьё"
                confidence = lot.get("confidence_self", "confirmed")
                if confidence not in ("confirmed", "pending", "needs-review"):
                    confidence = "confirmed"
                try:
                    price = float(lot["price_rub_kg"])
                except (KeyError, TypeError, ValueError):
                    skipped += 1
                    continue
                try:
                    cur.execute(
                        """INSERT INTO procurement.lots (
                            species, subspecies, region, weight_class, processing, state,
                            product_form, price_rub_kg, volume_tier, conditions, supplier_id,
                            received_at, valid_until, msg_id, source_file, raw_text,
                            confidence, notes
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (supplier_id, COALESCE(msg_id, 0), species,
                                     COALESCE(region, 'прочее'::procurement.region_enum),
                                     weight_class, processing, state, price_rub_kg,
                                     COALESCE(volume_tier, ''), COALESCE(conditions, ''))
                        WHERE superseded_by_lot_id IS NULL DO NOTHING""",
                        (
                            species, lot.get("subspecies"), region,
                            lot.get("weight_class", "unspecified"),
                            processing, state, product_form, price,
                            lot.get("volume_tier"), lot.get("conditions"),
                            supplier_id, received_at, valid_until, msg_id,
                            source_file, lot.get("raw_text"),
                            confidence, lot.get("notes"),
                        ),
                    )
                    if cur.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1
                except Exception as e:
                    logger.error(f"market_intel: insert lot failed: {e!r}; lot={lot}")
                    skipped += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return inserted, skipped


async def process_pending(db, limit: int = 20) -> dict:
    """Главная функция — обработать unprocessed сообщения."""
    pending = db.get_unprocessed_market_intel(limit=limit)
    stats = {"checked": len(pending), "processed": 0, "failed": 0,
             "skipped_pdf": 0, "inserted_total": 0}
    for msg in pending:
        msg_id = msg["id"]
        mt = msg.get("msg_type")
        caption = msg.get("text_raw") or ""
        forward_from = msg.get("forward_from") or ""
        file_path = msg.get("file_path")
        try:
            if mt == "text":
                if not caption.strip():
                    logger.info(f"market_intel: msg {msg_id} empty text, skip")
                    db.mark_market_intel_processed(msg_id)
                    stats["processed"] += 1
                    continue
                result = await parse_text_message(caption)
            elif mt == "photo" and file_path and os.path.exists(file_path):
                result = await parse_photo_message(file_path, caption)
            elif mt == "document" and msg.get("file_ext") == "pdf":
                if not file_path or not os.path.exists(file_path):
                    logger.warning(f"market_intel: msg {msg_id} PDF file_path не найден ({file_path}), помечаю processed")
                    db.mark_market_intel_processed(msg_id)
                    stats["failed"] += 1
                    continue
                try:
                    result = await parse_pdf_message(file_path, caption)
                except Exception as e:
                    logger.exception(f"market_intel: msg {msg_id} PDF parse failed: {e!r}")
                    # НЕ помечаем processed — попробуем на следующем тике (вдруг временный сбой).
                    stats["failed"] += 1
                    continue
            else:
                logger.warning(f"market_intel: msg {msg_id} type={mt} unknown, skip")
                db.mark_market_intel_processed(msg_id)
                stats["processed"] += 1
                continue

            # Маппинг supplier
            hint = result.get("supplier_hint") or forward_from or caption[:50]
            slug, is_known = _supplier_slug_from_hint(hint)
            if not slug:
                logger.warning(f"market_intel: msg {msg_id} — slug пустой (hint={hint!r}); пропускаю needs-review")
                db.mark_market_intel_processed(msg_id)
                stats["failed"] += 1
                continue

            received_at = result.get("received_at")
            if not received_at:
                posted_at = msg.get("posted_at")
                received_at = posted_at.strftime("%Y-%m-%d") if posted_at else datetime.now().strftime("%Y-%m-%d")

            # Авто-slug (не из known map) → лоты помечаются needs-review.
            lots_list = result.get("lots", [])
            if not is_known:
                for lot in lots_list:
                    lot["confidence_self"] = "needs-review"
                    lot.setdefault("notes", "")
                    lot["notes"] = (lot["notes"] + f" [auto-slug from hint='{hint[:40]}']").strip()

            ins, skp = _insert_lots(db, lots_list, slug,
                                     msg_id, file_path or f"msg_{msg_id}", received_at)
            db.mark_market_intel_processed(msg_id)
            stats["processed"] += 1
            stats["inserted_total"] += ins
            logger.info(f"market_intel: msg {msg_id} → supplier={slug} (known={is_known}) lots_inserted={ins} skipped={skp}")
        except Exception as e:
            logger.exception(f"market_intel: msg {msg_id} failed: {e!r}")
            stats["failed"] += 1
    return stats


def _record_tick(db, status: str, detail: str = ""):
    """Heartbeat в bot_settings. Чтобы видеть из БД, что cron живой и когда
    он последний раз стартовал/завершился (без доступа к Amvera-логам)."""
    try:
        from zoneinfo import ZoneInfo
        now_iso = datetime.now(ZoneInfo("Europe/Moscow")).isoformat()
        db._execute(
            "INSERT INTO bot_settings(key, value) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (f"market_intel_{status}_at", f"{now_iso} {detail}".strip()),
        )
    except Exception as e:
        logger.warning(f"_record_tick: {e}")


async def market_intel_cron_job(app, db):
    """APScheduler entry-point. Вызывается каждые 30 мин 24/7."""
    logger.info("market_intel cron tick started")
    _record_tick(db, "tick_started")
    try:
        stats = await process_pending(db)
        logger.info(f"market_intel cron: {stats}")
        _record_tick(db, "tick_done", detail=str(stats))
    except Exception as e:
        logger.exception("market_intel cron crashed")
        _record_tick(db, "tick_crashed", detail=str(e))
