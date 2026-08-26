"""Анализ переписок — классификатор проблемных сигналов за сутки (Ф2).

План `2026-04-29-анализ-переписок-фундамент.md`, реализация 2026-07-14.

Приватность (152-ФЗ): в Claude уходит ТОЛЬКО обезличенный текст (`chat_anonymizer`)
+ опаковый id диалога (d1, d2, ...). Привязку id → реальный менеджер/клиент/чат
держим локально (`id_map`) и приклеиваем к сигналам ПОСЛЕ ответа модели.
Дайджест Виктору может содержать реальные имена — он оператор данных; ограничение
касается только передачи в Anthropic.

6 категорий сигналов (приоритет при конфликте — сверху вниз):
  our_failure > complaint > sla_miss > missed_request > payment_issue > competitor_or_pricing
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re

from anthropic import AsyncAnthropic

from chat_anonymizer import anonymize

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"
MAX_DIALOGUES_PER_BATCH = 20
CONFIDENCE_THRESHOLD = 0.7
PROMPT_VERSION = "digest-v1"

AMO_SUBDOMAIN = os.getenv("AMO_SUBDOMAIN", "victorfishtobiz")
_LEAD_URL = "https://" + AMO_SUBDOMAIN + ".amocrm.ru/leads/detail/{}"

SIGNAL_TYPES = [
    "sla_miss",
    "missed_request",
    "payment_issue",
    "complaint",
    "our_failure",
    "competitor_or_pricing",
]

SYSTEM_PROMPT = """Ты — аналитик отдела продаж рыбной компании F2B. На входе —
обезличенные диалоги менеджеров с клиентами за сутки (Wazzup). Твоя задача —
найти ПРОБЛЕМНЫЕ сигналы, на которые собственнику нужно среагировать. Обычные
рабочие диалоги (оформили заказ, поблагодарили, согласовали) — НЕ сигнал.

Категории (в скобках — приоритет при конфликте, бери ОДНУ, самую важную):
1. our_failure (1) — НАШ факап: опоздание/перенос доставки, недовоз, пересорт,
   потерянные документы, брак с нашей стороны, техсбой в заказе.
2. complaint (2) — претензия/негатив клиента: жалоба на качество/сервис,
   эмоции («?!?», «сколько можно», угроза уйти).
3. sla_miss (3) — клиент написал в рабочее время, а ответа менеджера долго нет
   или нет вовсе (ориентир: >2 часов в рабочее время / ответ на следующий день /
   вопрос повис). Смотри на времена [HH:MM] и на то, кто написал последним.
4. missed_request (4) — ГЛАВНЫЙ ДЕНЕЖНЫЙ СИГНАЛ. Клиент попросил товар/размер/срок,
   а получил «нет», «не сможем», размытый ответ или тишину — без альтернативы.
   Здесь recall важнее точности: лучше отметить сомнительный, чем упустить.
5. payment_issue (5) — проблемы оплат: просрочка, перенос оплаты, финансовый
   стресс клиента, спор по сумме.
6. competitor_or_pricing (6) — упомянут конкурент / клиент сравнил цену / ушёл к
   другому / «у других дешевле».

Правила:
- Верни сигнал ТОЛЬКО если уверен (confidence 0..1). Порог отсечения применит код.
- summary — КОРОТКИЙ тезис по-русски, максимум 70 знаков, без вводных слов и без
  повтора имени клиента/менеджера. Только суть: «недовоз льда, повторно»,
  «форель сдвинули на чт, клиента не предупредили», «документы не дошли, оплата стоит». Тире только короткое (\u2013).
- quote — короткая обезличенная цитата (как в диалоге, с масками [VOLUME]/[AMOUNT]/…),
  подтверждающая сигнал (хранится в базе, в дайджест не выводится).
- Один диалог может дать несколько сигналов разных типов, но не дублируй один и тот
  же смысл. Если диалог штатный — не возвращай по нему ничего.
- Не выдумывай: опирайся только на текст диалога."""

TOOL = {
    "name": "report_signals",
    "description": "Вернуть найденные проблемные сигналы в переписках за день.",
    "input_schema": {
        "type": "object",
        "properties": {
            "signals": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "dialogue_id": {"type": "string", "description": "id вида d1, d2"},
                        "type": {"type": "string", "enum": SIGNAL_TYPES},
                        "severity": {"type": "string", "enum": ["high", "medium", "low"]},
                        "quote": {"type": "string"},
                        "summary": {"type": "string"},
                        "confidence": {"type": "number"},
                    },
                    "required": ["dialogue_id", "type", "severity", "summary", "confidence"],
                },
            }
        },
        "required": ["signals"],
    },
}


def _get_client() -> AsyncAnthropic:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY env not set")
    return AsyncAnthropic(api_key=api_key)


def fetch_dialogues(db, hours: int = 24) -> list[dict]:
    """Собрать диалоги за последние `hours` часов, сгруппированные по чату.

    Возвращает список dict: {chat_id, contact_name, manager_name, messages:[...]}.
    messages — по возрастанию времени, каждое {role: client|manager, ts, text}.
    Берём и входящие, и исходящие — исходящие нужны для детекта SLA/ответа.
    """
    rows = db._fetchall(
        """SELECT m.chat_id, m.contact_name, m.manager_name, m.text,
                  m.is_outbound, m.sent_at,
                  cm.company_name AS cm_company, cm.manager AS cm_manager
           FROM wazzup_messages m
           LEFT JOIN wazzup_contact_map cm ON cm.chat_id = m.chat_id
           WHERE m.text IS NOT NULL AND length(m.text) > 0
             AND m.sent_at > NOW() - (%s || ' hours')::interval
             AND (cm.role IS NULL OR cm.role <> 'игнор')
           ORDER BY m.chat_id, m.sent_at""",
        (str(hours),),
    )
    by_chat: dict[str, dict] = {}
    for r in rows:
        cid = str(r["chat_id"])
        d = by_chat.setdefault(
            cid,
            {
                "chat_id": cid,
                "contact_name": _clean_name(r.get("contact_name")),
                # менеджер: сначала из ручной разметки чата, потом из сообщения
                "manager_name": _clean_name(r.get("cm_manager") or r.get("manager_name")),
                "company_name": _clean_name(r.get("cm_company")),
                "messages": [],
            },
        )
        if not d["contact_name"]:
            d["contact_name"] = _clean_name(r.get("contact_name"))
        if not d["manager_name"]:
            d["manager_name"] = _clean_name(r.get("cm_manager") or r.get("manager_name"))
        if not d["company_name"]:
            d["company_name"] = _clean_name(r.get("cm_company"))
        d["messages"].append(
            {
                "role": "manager" if r.get("is_outbound") else "client",
                "ts": r.get("sent_at"),
                "text": r["text"],
            }
        )
    # только диалоги, где есть хоть одно сообщение клиента
    return [
        d for d in by_chat.values()
        if any(m["role"] == "client" for m in d["messages"])
    ]


def _clean_name(name: str | None) -> str | None:
    """Отбросить служебные плейсхолдеры имён/компаний."""
    if not name:
        return None
    n = name.strip()
    if n.lower() in {"__ignore__", "ignore", "-", "—", "?"} or not n:
        return None
    return n


def _fmt_ts(ts) -> str:
    try:
        return ts.strftime("%H:%M")
    except Exception:
        return "--:--"


def _render_dialogue(did: str, dlg: dict) -> str:
    """Обезличенный текстовый блок диалога для промпта."""
    lines = [f"[{did}]"]
    for m in dlg["messages"]:
        anon = anonymize(
            m["text"],
            contact_name=dlg.get("contact_name"),
            manager_name=dlg.get("manager_name"),
        )
        who = "клиент" if m["role"] == "client" else "менеджер"
        lines.append(f"  [{_fmt_ts(m['ts'])}] {who}: {anon}")
    return "\n".join(lines)


def _build_batch(dialogues: list[dict]) -> tuple[str, dict]:
    """Собрать user-текст батча + id_map {did: dialogue}."""
    blocks, id_map = [], {}
    for i, dlg in enumerate(dialogues, 1):
        did = f"d{i}"
        id_map[did] = dlg
        blocks.append(_render_dialogue(did, dlg))
    user = "Диалоги за сутки:\n\n" + "\n\n".join(blocks)
    return user, id_map


async def _classify_batch(client: AsyncAnthropic, dialogues: list[dict]) -> list[dict]:
    user, id_map = _build_batch(dialogues)
    try:
        resp = await client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=[{"type": "text", "text": SYSTEM_PROMPT,
                     "cache_control": {"type": "ephemeral"}}],
            tools=[TOOL],
            tool_choice={"type": "tool", "name": "report_signals"},
            messages=[{"role": "user", "content": user}],
        )
    except Exception as e:
        logger.warning("chat_digest: батч упал: %s", e)
        return []

    signals: list[dict] = []
    for block in resp.content:
        if getattr(block, "type", None) != "tool_use":
            continue
        for s in block.input.get("signals", []):
            dlg = id_map.get(s.get("dialogue_id"))
            if not dlg:
                continue
            if s.get("type") not in SIGNAL_TYPES:
                continue
            if float(s.get("confidence") or 0) < CONFIDENCE_THRESHOLD:
                continue
            signals.append(
                {
                    "type": s["type"],
                    "severity": s.get("severity", "medium"),
                    "quote": s.get("quote", ""),
                    "summary": s.get("summary", ""),
                    "confidence": float(s.get("confidence")),
                    # локальная привязка (в Claude не уходила)
                    "chat_id": dlg["chat_id"],
                    "manager_name": dlg.get("manager_name"),
                    "contact_name": dlg.get("contact_name"),
                    "company_name": dlg.get("company_name"),
                }
            )
    return signals


async def _enrich_with_amocrm(signals: list[dict]) -> None:
    """Дорезолвить по каждому чату сделку в amoCRM и ответственного менеджера.

    Менеджер в Wazzup-сообщениях часто пуст (webhook для входящих не присылает
    crmUserId — см. wazzup_classifier), поэтому «менеджер неизв.» в дайджесте —
    это дыра разметки, а не отсутствие менеджера. Источник правды — кто
    ОТВЕТСТВЕННЫЙ за сделку в amoCRM. Резолвим chat_id → contact → lead →
    responsible и заодно кладём lead_id, чтобы дайджест давал ссылку в сделку.

    Мутируем сигналы на месте: проставляем `lead_id`, и если manager_name пуст —
    заполняем именем ответственного. Резолв — один раз на чат (кэш), не на сигнал.
    """
    try:
        from wazzup_classifier import (
            _resolve_amocrm_lead_id,
            _resolve_amocrm_responsible,
        )
    except Exception as e:  # модуль/зависимость недоступны — дайджест без ссылок
        logger.info("chat_digest: amoCRM-обогащение пропущено: %s", e)
        return

    # уникальные чаты + любое известное имя контакта для fallback-резолва по имени
    chats: dict[str, str | None] = {}
    for s in signals:
        cid = s.get("chat_id")
        if not cid:
            continue
        if cid not in chats or (not chats[cid] and s.get("contact_name")):
            chats[cid] = s.get("contact_name")

    async def _resolve(cid: str, contact_name: str | None):
        lead_id = await _resolve_amocrm_lead_id(cid, contact_name)
        _, resp_name = await _resolve_amocrm_responsible(lead_id)
        return cid, lead_id, resp_name

    resolved = await asyncio.gather(
        *[_resolve(cid, name) for cid, name in chats.items()],
        return_exceptions=True,
    )
    by_chat: dict[str, tuple] = {}
    for r in resolved:
        if isinstance(r, Exception):
            continue
        cid, lead_id, resp_name = r
        by_chat[cid] = (lead_id, resp_name)

    for s in signals:
        lead_id, resp_name = by_chat.get(s.get("chat_id"), (None, None))
        if lead_id:
            s["lead_id"] = lead_id
        if not s.get("manager_name") and resp_name:
            s["manager_name"] = resp_name


async def analyze(db, hours: int = 24) -> list[dict]:
    """Прогнать сутки переписок → список обогащённых сигналов."""
    dialogues = fetch_dialogues(db, hours=hours)
    if not dialogues:
        return []
    batches = [
        dialogues[i:i + MAX_DIALOGUES_PER_BATCH]
        for i in range(0, len(dialogues), MAX_DIALOGUES_PER_BATCH)
    ]
    client = _get_client()
    sem = asyncio.Semaphore(4)

    async def _run(batch):
        async with sem:
            return await _classify_batch(client, batch)

    results = await asyncio.gather(*[_run(b) for b in batches])
    signals = [s for batch in results for s in batch]
    await _enrich_with_amocrm(signals)
    logger.info(
        "chat_digest: %d диалогов, %d батчей, %d сигналов",
        len(dialogues), len(batches), len(signals),
    )
    return signals


# ── Хранилище ─────────────────────────────────────────────────────────────────
CREATE_SIGNALS_TABLE = """
CREATE TABLE IF NOT EXISTS chat_signals (
    id SERIAL PRIMARY KEY,
    chat_id TEXT,
    signal_type TEXT NOT NULL,
    severity TEXT,
    summary TEXT,
    quote TEXT,
    manager TEXT,
    company TEXT,
    contact_name TEXT,
    confidence REAL,
    lead_id BIGINT,
    occurred_day DATE NOT NULL DEFAULT (NOW() AT TIME ZONE 'Europe/Moscow')::date,
    semantic_key TEXT,
    prompt_version TEXT,
    model_id TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    delivered_at TIMESTAMP,
    UNIQUE (chat_id, signal_type, occurred_day, semantic_key)
);
"""


def _semantic_key(sig: dict) -> str:
    """Хэш нормализованного смысла — чтобы повтор прогона не плодил дубли."""
    base = re.sub(r"\s+", " ", (sig.get("summary") or sig.get("quote") or "")).lower().strip()
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def save_signals(db, signals: list[dict]) -> int:
    """Записать сигналы с дедупом. Вернуть число новых строк."""
    if not signals:
        return 0
    db._execute(CREATE_SIGNALS_TABLE)
    db._execute("ALTER TABLE chat_signals ADD COLUMN IF NOT EXISTS lead_id BIGINT")
    inserted = 0
    for s in signals:
        res = db._execute(
            """INSERT INTO chat_signals
                 (chat_id, signal_type, severity, summary, quote, manager,
                  company, contact_name, confidence, lead_id, semantic_key,
                  prompt_version, model_id)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (chat_id, signal_type, occurred_day, semantic_key)
               DO NOTHING""",
            (
                s.get("chat_id"), s["type"], s.get("severity"), s.get("summary"),
                s.get("quote"), s.get("manager_name"), s.get("company_name"),
                s.get("contact_name"), s.get("confidence"), s.get("lead_id"),
                _semantic_key(s), PROMPT_VERSION, MODEL,
            ),
        )
        # psycopg2 rowcount доступен через _execute? если нет — считаем оптимистично
        inserted += 1
    return inserted


# ── Форматтер дайджеста ───────────────────────────────────────────────────────
_TYPE_LABELS = {
    "our_failure": "🔴 Наши факапы",
    "complaint": "🟠 Претензии и негатив",
    "sla_miss": "🟡 Долго не отвечали (SLA)",
    "missed_request": "💸 Упущенные запросы",
    "payment_issue": "💰 Проблемы оплат",
    "competitor_or_pricing": "⚔️ Конкуренты и цены",
}
_TYPE_ORDER = ["our_failure", "complaint", "sla_miss", "missed_request",
               "payment_issue", "competitor_or_pricing"]
_SEV_RANK = {"high": 0, "medium": 1, "low": 2}


MAX_PER_TYPE = 8
_HEAD_LIMIT = 30
_SUMMARY_LIMIT = 80


def _short(text: str, limit: int) -> str:
    """Обрезать по границе слова, без хвостовой пунктуации."""
    text = " ".join((text or "").replace("\u2014", "\u2013").split())
    if len(text) <= limit:
        return text.rstrip(" .,;:")
    cut = text[:limit]
    if " " in cut:
        cut = cut[:cut.rindex(" ")]
    return cut.rstrip(" .,;:") + "…"


def render_digest(signals: list[dict], day_label: str = "за сутки") -> str:
    """Сжатый markdown-дайджест: одна строка на сигнал — тезис + ссылка на сделку."""
    if not signals:
        return f"📊 *Анализ переписок {day_label}*\n\nПроблемных сигналов не найдено — всё штатно."

    by_type: dict[str, list[dict]] = {}
    for s in signals:
        by_type.setdefault(s["type"], []).append(s)

    lines = [f"📊 *Анализ переписок {day_label}* \u2013 сигналов: {len(signals)}", ""]
    for t in _TYPE_ORDER:
        group = by_type.get(t)
        if not group:
            continue
        group.sort(key=lambda x: (_SEV_RANK.get(x.get("severity"), 1), -(x.get("confidence") or 0)))
        lines.append(f"*{_TYPE_LABELS[t]}* ({len(group)})")
        for s in group[:MAX_PER_TYPE]:
            who = _short(s.get("company_name") or s.get("contact_name") or "клиент неизв.", _HEAD_LIMIT)
            thesis = _short(s.get("summary", ""), _SUMMARY_LIMIT)
            lead_id = s.get("lead_id")
            head = f"[{who}]({_LEAD_URL.format(lead_id)})" if lead_id else who
            mgr = s.get("manager_name")
            tail = f" · {_short(mgr, 20)}" if mgr else ""
            lines.append(f"• {head} \u2013 {thesis}{tail}")
        if len(group) > MAX_PER_TYPE:
            lines.append(f"  …ещё {len(group) - MAX_PER_TYPE} того же типа")
        lines.append("")
    return "\n".join(lines).strip()
