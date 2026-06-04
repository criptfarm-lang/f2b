"""Wazzup AI-классификатор: запросы клиентов по номенклатуре.

Фаза 3 плана 2026-05-25. Micro-batch worker раз в 15 мин в окне 09-19 МСК Пн-Пт.
Читает новые входящие из wazzup_messages → классифицирует через Haiku 4.5 →
INSERT в wazzup_classifications + UPDATE classified_at в messages.

Offline-эксперимент (2026-06-04, /tmp/wazzup_classifier_exp.py): F1=0.968 на 172
примерах, precision=1.0 (0 ложных алертов).

Фазы 4-6 (TG-алерты закупщику/менеджеру + дневная сводка собственнику + кнопки
действия в TG) — отдельный заход после Фазы 0 (юр-проверка 152-ФЗ + согласование
с командой ОП). Сейчас только копим данные.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta

from anthropic import AsyncAnthropic

logger = logging.getLogger(__name__)

# Версия промпта = git-sha коммита. При обновлении промпта меняем — это
# инвалидирует UNIQUE(message_id, prompt_version) и старые сообщения
# переклассифицируются.
PROMPT_VERSION = "v1-2026-06-04"

MODEL = "claude-haiku-4-5-20251001"

SYSTEM_PROMPT = """Ты — классификатор B2B-переписок компании F2B (Fish to Business),
оптовая поставка рыбы и морепродуктов ресторанам Москвы и опт по РФ.

Менеджеры F2B (5 человек: Карина, Инесса, Елена, Ирина, Денис) переписываются
с клиентами через Wazzup24 (WhatsApp/Telegram/MAX). Тебе на вход подаётся
ОДНО ВХОДЯЩЕЕ сообщение от клиента (НЕ от менеджера).

Твоя задача — определить, является ли это сообщение «запросом клиента по
номенклатуре». Это значит: клиент СПРАШИВАЕТ конкретный товар (название
рыбы/морепродукта, размер, разделку, цену, наличие). Примеры:
✅ «Есть кижуч?»
✅ «Дайте цену на форель ПСГ 1.5-2 кг»
✅ «Срочно нужно 50 кг трески филе»
✅ «Сёмга охл. трим Б 1.6-2 кг — какая цена и сроки?»
✅ «Прайс на креветку 21/25 в наличии?»

НЕ ЯВЛЯЕТСЯ запросом по номенклатуре:
❌ Приветствия, благодарности, общая болтовня («Доброе утро», «Спасибо»)
❌ Уточнения по логистике/оплате/документам («Когда оплата?», «Реквизиты пришлите»)
❌ Сообщения от закупщиков/коллег (внутренние, не клиент)
❌ Ответы менеджеров клиенту (это исходящее — сейчас не должно попадаться)
❌ Сухие технические сообщения (телефоны, номера, «оплачено», «принято»)

Если есть сомнение — лучше пометь is_nomenclature_request=false, чем
давать ложно-положительный сигнал закупщику.

Ответ — СТРОГО JSON одной строкой без обёрток ```json:
{
  "is_nomenclature_request": bool,
  "sku_or_description": "что просят, краткое описание" | null,
  "species_normalized": "одно из: лосось/форель/треска/судак/окунь/.../другое" | null,
  "urgency": "срочно" | "уточнение" | "общий",
  "confidence": float от 0 до 1,
  "reason": "1-фраза почему"
}"""

FEWSHOT = [
    {"role": "user", "content": "Сообщение от клиента «Ольга»: «Доброе утро! Есть форель ПСГ 1.5-2 кг? Срочно нужно 30 кг.»"},
    {"role": "assistant", "content": '{"is_nomenclature_request": true, "sku_or_description": "Форель ПСГ 1.5-2 кг, 30 кг", "species_normalized": "форель", "urgency": "срочно", "confidence": 0.97, "reason": "Явный запрос конкретной номенклатуры с количеством"}'},
    {"role": "user", "content": "Сообщение от клиента «Андрей»: «Завтра оплата будет, не переживайте»"},
    {"role": "assistant", "content": '{"is_nomenclature_request": false, "sku_or_description": null, "species_normalized": null, "urgency": "общий", "confidence": 0.95, "reason": "Не запрос — финансовое уведомление"}'},
    {"role": "user", "content": "Сообщение от клиента «Дмитрий»: «А скумбрия с/м есть в наличии? И цена»"},
    {"role": "assistant", "content": '{"is_nomenclature_request": true, "sku_or_description": "Скумбрия с/м, наличие и цена", "species_normalized": "скумбрия", "urgency": "уточнение", "confidence": 0.95, "reason": "Запрос наличия и цены конкретной рыбы"}'},
    {"role": "user", "content": "Сообщение от клиента «Татьяна»: «Спасибо!»"},
    {"role": "assistant", "content": '{"is_nomenclature_request": false, "sku_or_description": null, "species_normalized": null, "urgency": "общий", "confidence": 0.99, "reason": "Благодарность, не запрос"}'},
]


def _get_client() -> AsyncAnthropic:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set!")
    return AsyncAnthropic(api_key=api_key)


def _is_working_hours_msk() -> bool:
    """Окно 09-19 МСК Пн-Пт. Сб-Вс — отдых."""
    now = datetime.now(timezone(timedelta(hours=3)))
    if now.weekday() >= 5:
        return False
    return 9 <= now.hour < 19


async def _classify_one(client: AsyncAnthropic, contact: str, text: str) -> dict | None:
    user_content = f"Сообщение от клиента «{contact or '?'}»: «{text}»"
    msgs = list(FEWSHOT) + [{"role": "user", "content": user_content}]
    try:
        resp = await client.messages.create(
            model=MODEL,
            max_tokens=300,
            system=SYSTEM_PROMPT,
            messages=msgs,
        )
        txt = resp.content[0].text.strip()
        if txt.startswith("```"):
            txt = txt.split("\n", 1)[1] if "\n" in txt else txt
            txt = txt.rsplit("```", 1)[0].strip()
            if txt.startswith("json"):
                txt = txt[4:].lstrip()
        return json.loads(txt)
    except Exception as e:
        logger.warning(f"wazzup_classifier: classify failed: {type(e).__name__}: {e}")
        return None


async def _check_freshness(db) -> bool:
    """Health-check: max(sent_at) > now() - 24h. Если БД отстаёт → пропуск
    батча. См. memory feedback_chat_extract_freshness_check."""
    row = db._fetchone("SELECT MAX(sent_at) AS last FROM wazzup_messages")
    last = row.get("last") if row else None
    if last is None:
        return False
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    gap_h = (datetime.now(timezone.utc) - last).total_seconds() / 3600
    return gap_h < 24


async def run_classification_batch(db, force: bool = False) -> dict:
    """Один проход: выбирает до 100 необработанных входящих за последние 2ч,
    классифицирует, сохраняет результаты. Идемпотентно через UNIQUE(message_id,
    prompt_version) + ON CONFLICT DO NOTHING.

    Возвращает {processed, requests_found, skipped, errors}.
    """
    if not force and not _is_working_hours_msk():
        return {"skipped_reason": "out_of_hours", "processed": 0}

    if not await _check_freshness(db):
        logger.warning("wazzup_classifier: БД отстаёт >24ч, пропуск батча")
        return {"skipped_reason": "stale_db", "processed": 0}

    # Берём входящие за последние 2ч которые ещё не классифицированы
    # с этой версией промпта.
    rows = db._fetchall(
        """SELECT m.message_id, m.chat_id, m.contact_name, m.text
        FROM wazzup_messages m
        LEFT JOIN wazzup_classifications c
               ON c.message_id = m.message_id
              AND c.prompt_version = %s
        WHERE m.is_outbound = FALSE
          AND c.id IS NULL
          AND m.classified_at IS NULL
          AND m.text IS NOT NULL AND length(m.text) > 5
          AND m.sent_at > NOW() - INTERVAL '2 hours'
        ORDER BY m.sent_at
        LIMIT 100""",
        (PROMPT_VERSION,),
    )

    if not rows:
        return {"processed": 0, "requests_found": 0}

    client = _get_client()
    sem = asyncio.Semaphore(8)  # Anthropic tier-1 RPM=50, 8 параллелей безопасно
    stats = {"processed": 0, "requests_found": 0, "errors": 0}

    async def _worker(m: dict):
        async with sem:
            result = await _classify_one(client, m.get("contact_name") or "", m["text"])
        if result is None:
            stats["errors"] += 1
            return
        try:
            is_req = bool(result.get("is_nomenclature_request"))
            db._execute(
                """INSERT INTO wazzup_classifications
                   (message_id, is_nomenclature_request, sku_or_description,
                    species_normalized, urgency, confidence, reason,
                    raw_response, model, prompt_version)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (message_id, prompt_version) DO NOTHING""",
                (
                    m["message_id"], is_req,
                    result.get("sku_or_description"),
                    result.get("species_normalized"),
                    result.get("urgency"),
                    result.get("confidence"),
                    result.get("reason"),
                    json.dumps(result, ensure_ascii=False),
                    MODEL, PROMPT_VERSION,
                ),
            )
            db._execute(
                "UPDATE wazzup_messages SET classified_at = NOW() WHERE message_id = %s",
                (m["message_id"],),
            )
            stats["processed"] += 1
            if is_req:
                stats["requests_found"] += 1
        except Exception as e:
            logger.warning(f"wazzup_classifier: db save failed for {m['message_id']}: {e}")
            stats["errors"] += 1

    await asyncio.gather(*(_worker(dict(r)) for r in rows))
    logger.info(
        f"wazzup_classifier: batch done — processed={stats['processed']}, "
        f"requests={stats['requests_found']}, errors={stats['errors']}"
    )
    return stats
