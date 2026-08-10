"""
Контрольные задачи претензионной работы: карточка на доске YouGile (проект
«Претензии») при одобрении претензии по оплате.

Почему карточка, а не задача в МойСкладе напрямую: коннектор `f2b-publisher/
src/yougile_sync.py` создаёт задачи только в направлении YouGile → МойСклад.
Карточка со стикером «Исполнитель» и сроком на ближайшем тике (30 мин)
превращается в задачу МС на менеджера и связывается с карточкой. Так одна
сущность видна в двух окнах: собственнику — на доске, менеджеру — в МойСкладе.
Задача, заведённая прямо в МС, на доске не появится.

План: plans/2026-08-10-регламент-претензионной-работы-по-оплатам.md, Фаза 4.
"""

import os
import logging
from datetime import datetime, timedelta, timezone

import httpx

logger = logging.getLogger(__name__)

YOUGILE_BASE = "https://ru.yougile.com/api-v2"
ASSIGNEE_STICKER_ID = "d0ef0de9-0baf-4a74-8047-fe71de6d267e"
CLAIMS_BOARD_TITLE = "Задачи"
CLAIMS_PROJECT_TITLE = "Претензии"
INBOX_COLUMN = "входящие"
MSK = timezone(timedelta(hours=3))

# Сроки из текста писем: письмо №1 — 5 рабочих дней на погашение,
# письмо №2 — 3 рабочих дня. Решение собственника 2026-08-10.
DEADLINE_WORKDAYS = {"claim_payment": 5, "claim_payment_final": 3}


# Порог ст. 91.1 Основ о нотариате: уведомление о долге должно быть направлено
# не менее чем за 14 дней до обращения к нотариусу. Раньше эскалировать нельзя.
NOTARY_THRESHOLD_DAYS = 15


def _key() -> str | None:
    return os.getenv("YOUGILE_KEY")


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS claim_control (
    doc_id        INTEGER PRIMARY KEY,
    agent_id      TEXT,
    counterparty  TEXT,
    letter_type   TEXT,
    manager_tag   TEXT,
    card_id       TEXT,
    debt          NUMERIC,
    deadline      TIMESTAMPTZ,
    approved_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    escalated_at  TIMESTAMPTZ,
    closed_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS claim_control_open_idx ON claim_control (closed_at, agent_id);
"""


def ensure_schema(db) -> None:
    for stmt in filter(None, (s.strip() for s in SCHEMA_SQL.split(";"))):
        db._execute(stmt)


def workday_deadline(workdays: int, start: datetime | None = None) -> datetime:
    """Конец рабочего дня через N рабочих дней (суббота и воскресенье не считаются)."""
    cur = (start or datetime.now(MSK)).replace(hour=18, minute=0, second=0, microsecond=0)
    left = workdays
    while left > 0:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            left -= 1
    return cur


def _pick_assignee_state(states: list, manager_tag: str) -> str | None:
    """Состояние стикера по тегу-фамилии из карточки МС («баласанян» → «Баласанян К.»)."""
    tag = (manager_tag or "").strip().lower()
    if not tag:
        return None
    for st in states:
        surname = (st.get("name") or "").split()[0].strip().lower().rstrip(".")
        if surname == tag:
            return st.get("id")
    return None


async def create_claim_control_card(letter_type: str, counterparty: str, manager_tag: str,
                                    debt: float, ms_url: str = "", details: str = "") -> dict:
    """Ставит контрольную карточку в проект «Претензии». Возвращает {ok, id|error}.

    Ошибки не поднимаются наверх: не поставленная задача не должна ронять
    согласование документа — согласование важнее, о сбое сообщаем текстом.
    """
    key = _key()
    if not key:
        return {"ok": False, "error": "YOUGILE_KEY не настроен"}

    workdays = DEADLINE_WORKDAYS.get(letter_type)
    if not workdays:
        return {"ok": False, "error": f"тип {letter_type} не требует контрольной задачи"}

    is_final = letter_type == "claim_payment_final"
    deadline = workday_deadline(workdays)
    title = f"Претензия {counterparty} – проверить оплату {debt:,.2f} ₽".replace(",", " ")
    action = ("Прихода нет – передать на взыскание: суд либо исполнительная надпись нотариуса."
              if is_final else
              "Прихода нет – сформировать вторую форму письма (повторная претензия).")
    description = (
        f"<p>{'Повторная (окончательная) претензия' if is_final else 'Претензия по оплате'} "
        f"согласована {datetime.now(MSK):%d.%m.%Y}. Срок по письму – "
        f"{workdays} рабочих дн., до {deadline:%d.%m.%Y}.</p>"
        f"<p><b>Проверить поступление денег.</b> Пришли – закрыть задачу. {action}</p>"
        + (f"<p>{details}</p>" if details else "")
        + (f'<p><a href="{ms_url}">Карточка контрагента в МойСклад</a></p>' if ms_url else "")
    )

    try:
        async with httpx.AsyncClient(base_url=YOUGILE_BASE, timeout=30,
                                     headers={"Authorization": f"Bearer {key}"}) as yg:
            projects = (await yg.get("/projects", params={"limit": 100})).json()["content"]
            proj = next((p for p in projects if (p.get("title") or "").strip().lower()
                         == CLAIMS_PROJECT_TITLE.lower()), None)
            if not proj:
                return {"ok": False, "error": f"проект «{CLAIMS_PROJECT_TITLE}» не найден"}

            boards = (await yg.get("/boards", params={"limit": 100})).json()["content"]
            board = next((b for b in boards if b.get("projectId") == proj["id"]), None)
            if not board:
                return {"ok": False, "error": "доска проекта «Претензии» не найдена"}

            columns = (await yg.get("/columns", params={"limit": 1000})).json()["content"]
            col = next((c for c in columns if c.get("boardId") == board["id"]
                        and (c.get("title") or "").strip().lower() == INBOX_COLUMN), None)
            if not col:
                return {"ok": False, "error": "колонка «Входящие» не найдена"}

            body = {
                "title": title,
                "columnId": col["id"],
                "description": description,
                "deadline": {"deadline": int(deadline.timestamp() * 1000)},
            }
            sticker = (await yg.get(f"/string-stickers/{ASSIGNEE_STICKER_ID}")).json()
            state_id = _pick_assignee_state(sticker.get("states") or [], manager_tag)
            if state_id:
                body["stickers"] = {ASSIGNEE_STICKER_ID: state_id}
            else:
                logger.warning("claim task: исполнитель по тегу «%s» не найден — "
                               "карточка встанет во «Входящие» без исполнителя", manager_tag)

            resp = await yg.post("/tasks", json=body)
            if resp.status_code >= 400:
                return {"ok": False, "error": f"YouGile {resp.status_code}: {resp.text[:200]}"}
            return {"ok": True, "id": resp.json().get("id"),
                    "deadline": deadline, "assigned": bool(state_id)}
    except Exception as e:
        logger.error("claim task: создание карточки не удалось: %s", e)
        return {"ok": False, "error": str(e)}


# ── Цикл претензионной работы ────────────────────────────────────────────────

async def _agent_debt(agent_id: str) -> float:
    """Текущий долг контрагента по взаиморасчётам МС (отрицательный баланс = нам должны)."""
    import aiohttp
    import moysklad
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{moysklad.MS_BASE}/report/counterparty/{agent_id}",
                         headers=moysklad.get_headers()) as r:
            if r.status != 200:
                raise RuntimeError(f"МС {r.status}")
            bal = ((await r.json()).get("balance") or 0) / 100
    return -bal if bal < 0 else 0.0


async def _close_linked_ms_task(db, card_id: str) -> bool:
    """Закрывает задачу МойСклада, связанную с карточкой доски.

    Мастерство по «выполнено» — за МойСкладом: коннектор поднимает done из МС
    в YouGile и двигает карточку в «На проверке». Поэтому закрываем именно
    задачу МС, а карточку не трогаем."""
    row = db._fetchone("SELECT task_id::text AS task_id FROM board.card_task_map WHERE card_id=%s::uuid",
                       (card_id,))
    if not row or not row.get("task_id"):
        return False
    import aiohttp
    import moysklad
    async with aiohttp.ClientSession() as s:
        async with s.put(f"{moysklad.MS_BASE}/entity/task/{row['task_id']}",
                         headers=moysklad.get_headers(), json={"done": True}) as r:
            return r.status < 400


async def poll_job(app, db) -> None:
    """Обходит открытые претензии: гасит цепочку при погашении долга и
    эскалирует на собственника, когда сроки вышли.

    План: plans/2026-08-10-регламент-претензионной-работы-по-оплатам.md, Фазы 5–6.
    """
    ensure_schema(db)
    rows = db._fetchall("SELECT * FROM claim_control WHERE closed_at IS NULL ORDER BY doc_id")
    if not rows:
        return

    owner = int(os.getenv("OWNER_CHAT_ID") or 0)
    now = datetime.now(MSK)

    for row in rows:
        agent_id = row.get("agent_id")
        if not agent_id:
            continue
        try:
            debt = await _agent_debt(agent_id)
        except Exception as e:
            logger.error("claim poll: долг по %s не получен: %s", row.get("counterparty"), e)
            continue

        # 1. Долг погашен — закрываем цепочку.
        if debt <= 0:
            closed = False
            if row.get("card_id"):
                try:
                    closed = await _close_linked_ms_task(db, row["card_id"])
                except Exception as e:
                    logger.error("claim poll: закрытие задачи не удалось: %s", e)
            db._execute("UPDATE claim_control SET closed_at=NOW() WHERE doc_id=%s", (row["doc_id"],))
            if owner:
                note = "" if closed else " Задачу в МойСкладе закрой вручную — связь с карточкой не найдена."
                await app.bot.send_message(
                    owner,
                    f"✅ {row.get('counterparty')} погасил задолженность — претензионная работа закрыта."
                    + note)
            continue

        # 2. Срок вышел и пройден порог ст. 91.1 — эскалация на собственника, один раз.
        deadline = row.get("deadline")
        approved = row.get("approved_at")
        if row.get("escalated_at") or not deadline or not approved:
            continue
        days_since = (now - approved).days
        if now < deadline or days_since < NOTARY_THRESHOLD_DAYS:
            continue

        db._execute("UPDATE claim_control SET escalated_at=NOW() WHERE doc_id=%s", (row["doc_id"],))
        card = await create_claim_control_card(
            "claim_payment_final", row.get("counterparty") or "—", "васильев", float(debt),
            details="Сроки по претензиям вышли, оплата не поступила. Решение: суд либо "
                    "исполнительная надпись нотариуса (п. 7.10 Договора).")
        if owner:
            await app.bot.send_message(
                owner,
                f"⚖️ {row.get('counterparty')}: сроки по претензии вышли, долг "
                f"{debt:,.2f} ₽ не погашен.".replace(",", " ")
                + f"\nПрошло {days_since} дн. с согласования — порог ст. 91.1 (14 дн.) пройден, "
                  f"можно к нотариусу или в суд."
                + ("\n🗂 Карточка на доске «Претензии» поставлена." if card.get("ok")
                   else f"\n⚠️ Карточку поставить не удалось: {card.get('error')}"))
