"""Конверсия оферов по ссылке → amoCRM.

На странице офера (fishki.f2b.group/o/opt, /o/horeca – репо quiz-game) кнопки
«Написать» открывают MAX/Telegram/WhatsApp с готовым текстом, в котором стоит
код акции. Когда клиент отправляет такое сообщение, вебхук Wazzup приносит его
сюда: сохраняем попадание в promo_hits и ставим тег акции на сделку клиента в
amoCRM. Сделку ищем через ~2 минуты – входящее от нового человека интеграция
Wazzup сначала сама заводит контактом и сделкой в «Привлечении».

Один чат × одна акция = одно попадание: повторные сообщения тег не дёргают.
Пушей менеджерам нет (feedback_retention_no_manager_tg_pushes) – конверсию
видно по тегу в amoCRM и в promo_hits.

План: «F2B второй мозг»/plans/2026-10-09-оферы-по-ссылке.md
"""
import asyncio
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# Код акции из готового текста кнопки → тег сделки. Тексты кнопок – в quiz-game
# main.py, OFFERS[...]["promo_text"]: код менять синхронно.
PROMOS = {
    "ФТ-ОПТ": "акция форель опт",
    "ФТ-РЕСТ": "акция форель хорека",
}
# Клиент стёр код, но оставил суть – тег без направления.
FALLBACK_TAG = "акция форель"
CODE_RE = re.compile(r"ФТ\s*[-–]\s*(ОПТ|РЕСТ)", re.I)

# Поля контакта amoCRM, куда Wazzup пишет id чата (reference_f2b_wazzup_channels_and_fields)
CHAT_ID_FIELDS = {2244321, 2224427, 2245217, 2245219}
CLOSED_STATUSES = (142, 143)
RESOLVE_DELAYS = (120, 600)  # сек: после первой попытки ещё одна через 10 мин
_table_ready = False


def detect(text: str) -> Optional[str]:
    """Тег акции по тексту входящего или None."""
    if not text:
        return None
    m = CODE_RE.search(text)
    if m:
        return PROMOS["ФТ-" + m.group(1).upper()]
    low = text.lower()
    if "по акции" in low and "форел" in low:
        return FALLBACK_TAG
    return None


def ensure_table(db) -> None:
    db._execute("""
        CREATE TABLE IF NOT EXISTS promo_hits (
            id           SERIAL PRIMARY KEY,
            tag          TEXT NOT NULL,
            chat_type    TEXT,
            chat_id      TEXT NOT NULL,
            contact_name TEXT,
            message_id   TEXT,
            text         TEXT,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            lead_id      BIGINT,
            tagged_at    TIMESTAMPTZ,
            error        TEXT,
            UNIQUE (chat_id, tag)
        )""")


def on_message(db, chat_type: str, chat_id: str, contact_name: str, text: str,
               message_id: str) -> None:
    """Вызывается из вебхука Wazzup для входящего. Не блокирует: amoCRM – в фоне."""
    tag = detect(text)
    if not tag or not chat_id:
        return
    global _table_ready
    try:
        if not _table_ready:
            ensure_table(db)
            _table_ready = True
        row = db._fetchone(
            """INSERT INTO promo_hits (tag, chat_type, chat_id, contact_name, message_id, text)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (chat_id, tag) DO NOTHING RETURNING id""",
            (tag, chat_type, chat_id, contact_name, message_id, (text or "")[:1000]))
    except Exception as e:
        logger.error("promo_offers: запись попадания не удалась: %s", e)
        return
    if not row:
        return  # этот чат уже писал по этой акции
    logger.info("promo_offers: %s – %s (%s %s)", tag, contact_name, chat_type, chat_id)
    asyncio.get_running_loop().create_task(
        _tag_lead(db, row["id"], tag, chat_type, chat_id, contact_name))


async def _find_lead(chat_id: str, contact_name: str) -> Optional[int]:
    """Сделка клиента: контакт по id чата в полях Wazzup (точное совпадение),
    иначе общий резолв по телефону / точному имени. Из сделок контакта – открытая,
    самая свежая; если открытых нет – самая свежая."""
    from amocrm import amo_get
    from wazzup_classifier import _resolve_amocrm_lead_id

    found = await amo_get("/contacts", params={"query": chat_id, "with": "leads", "limit": 50})
    contacts = [c for c in ((found or {}).get("_embedded") or {}).get("contacts", [])
                if any(f.get("field_id") in CHAT_ID_FIELDS
                       and any(str(v.get("value")) == str(chat_id) for v in f.get("values") or [])
                       for f in c.get("custom_fields_values") or [])]
    lead_ids = [l["id"] for c in contacts for l in ((c.get("_embedded") or {}).get("leads") or [])]
    if not lead_ids:
        one = await _resolve_amocrm_lead_id(chat_id, contact_name)
        lead_ids = [one] if one else []
    if not lead_ids:
        return None
    leads = []
    for lid in dict.fromkeys(lead_ids):
        lead = await amo_get(f"/leads/{lid}")
        if lead:
            leads.append(lead)
    if not leads:
        return None
    leads.sort(key=lambda l: (l.get("status_id") not in CLOSED_STATUSES, l.get("updated_at") or 0),
               reverse=True)
    return int(leads[0]["id"])


async def _tag_lead(db, hit_id: int, tag: str, chat_type: str, chat_id: str,
                    contact_name: str) -> None:
    from amocrm import amo_get, amo_post
    from amo_alarms import _amo_patch

    err = "сделка не найдена"
    for delay in RESOLVE_DELAYS:
        await asyncio.sleep(delay)
        try:
            lead_id = await _find_lead(chat_id, contact_name)
            if not lead_id:
                continue
            lead = await amo_get(f"/leads/{lead_id}") or {}
            tags = [{"id": t["id"]} for t in (lead.get("_embedded") or {}).get("tags") or []]
            names = {t.get("name") for t in (lead.get("_embedded") or {}).get("tags") or []}
            if tag not in names:
                # PATCH заменяет набор тегов целиком – передаём старые вместе с новым
                ok = await _amo_patch(f"/leads/{lead_id}",
                                      {"_embedded": {"tags": tags + [{"name": tag}]}})
                if not ok:
                    err = "amoCRM не принял тег"
                    continue
            await amo_post(f"/leads/{lead_id}/notes", [{
                "note_type": "common",
                "params": {"text": f"Клиент написал по акции со страницы офера ({tag})."}}])
            db._execute("UPDATE promo_hits SET lead_id=%s, tagged_at=NOW(), error=NULL WHERE id=%s",
                        (lead_id, hit_id))
            logger.info("promo_offers: тег «%s» на сделке %s", tag, lead_id)
            return
        except Exception as e:
            err = str(e)[:300]
            logger.warning("promo_offers: попытка не удалась (%s): %s", chat_id, e)
    try:
        db._execute("UPDATE promo_hits SET error=%s WHERE id=%s", (err, hit_id))
    except Exception:
        pass
    logger.warning("promo_offers: %s – тег не поставлен: %s", chat_id, err)
