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


def _money(v: float) -> str:
    """Русский формат суммы: 12 345,67 (пробел разделяет тысячи, запятая — копейки)."""
    return f"{v:,.2f}".replace(",", "\u00a0").replace(".", ",").replace("\u00a0", " ")


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
ALTER TABLE claim_control ADD COLUMN IF NOT EXISTS chain_started_at TIMESTAMPTZ;
ALTER TABLE claim_control ADD COLUMN IF NOT EXISTS final_requested_at TIMESTAMPTZ;
ALTER TABLE claim_control ADD COLUMN IF NOT EXISTS lawyer_sent_at TIMESTAMPTZ;
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
    title = f"Претензия {counterparty} – проверить оплату {_money(debt)} ₽"
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

async def _overdue_debt(agent_id: str) -> float:
    """Просроченная часть долга контрагента (сервис «Документы», docs_overdue).

    Именно просрочка, а не сальдо: по сальдо новая поставка держала бы цепочку
    открытой после погашения просроченных накладных, а аванс по новой сделке —
    закрывал бы её при неоплаченной старой. Логика та же, что у ПДЗ."""
    base = (os.getenv("QUIZ_BASE_URL") or "").rstrip("/")
    if not base:
        raise RuntimeError("QUIZ_BASE_URL не настроен")
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.get(f"{base}/api/docs/claim/overdue/{agent_id}")
        if r.status_code != 200:
            raise RuntimeError(f"сервис документов {r.status_code}")
        return float((r.json() or {}).get("overdue_sum") or 0)


async def _send_to_lawyer(agent_id: str, counterparty: str) -> dict:
    """Просит сервис «Документы» собрать комплект и отправить юристу письмом.

    Юрист контрактный: в сотрудниках МойСклад и в стикере «Исполнитель» доски
    его нет, задачу поставить некому — поэтому почта, а контроль остаётся
    карточкой на собственника."""
    base = (os.getenv("QUIZ_BASE_URL") or "").rstrip("/")
    if not base:
        return {"ok": False, "error": "QUIZ_BASE_URL не настроен"}
    try:
        async with httpx.AsyncClient(timeout=300) as c:
            r = await c.post(f"{base}/api/docs/claim/to-lawyer",
                             json={"agent_id": agent_id, "counterparty_name": counterparty})
            if r.status_code >= 400:
                return {"ok": False, "error": f"{r.status_code}: {r.text[:150]}"}
            return r.json() or {"ok": False, "error": "пустой ответ сервиса"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def _request_final_claim(agent_id: str, counterparty: str) -> dict:
    """Просит сервис «Документы» собрать письмо №2 и отдать собственнику на
    согласование. Сборка идёт там фоном (чтение сканов договора до 150 с)."""
    base = (os.getenv("QUIZ_BASE_URL") or "").rstrip("/")
    if not base:
        return {"ok": False, "error": "QUIZ_BASE_URL не настроен"}
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(f"{base}/api/docs/claim/auto-final",
                             json={"agent_id": agent_id, "counterparty_name": counterparty})
            if r.status_code >= 400:
                return {"ok": False, "error": f"{r.status_code}: {r.text[:150]}"}
            return {"ok": True, **(r.json() or {})}
    except Exception as e:
        return {"ok": False, "error": str(e)}


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


async def _notify_manager(app, manager_tag: str, text: str) -> None:
    """Пуш менеджеру по тегу контрагента. Тег неизвестен — молча пропускаем:
    собственник о том же событии узнаёт своим каналом."""
    try:
        from moysklad import PDZ_MANAGER_TG_IDS
        chat_id = PDZ_MANAGER_TG_IDS.get((manager_tag or "").strip().lower())
        if chat_id:
            await app.bot.send_message(chat_id, text)
    except Exception as e:
        logger.error("claim poll: пуш менеджеру «%s» не ушёл: %s", manager_tag, e)


async def _close_chain(app, db, chain: list, owner: int) -> None:
    """Просрочка погашена — закрываем все письма цепочки разом.

    Именно все, а не только последнее: письма №1 и №2 живут отдельными строками
    по одному контрагенту, и обход их поодиночке слал бы дубли уведомлений."""
    manual = []
    for row in chain:
        if row.get("card_id"):
            try:
                if not await _close_linked_ms_task(db, row["card_id"]):
                    manual.append(row["doc_id"])
            except Exception as e:
                logger.error("claim poll: закрытие задачи не удалось: %s", e)
                manual.append(row["doc_id"])
    db._execute("UPDATE claim_control SET closed_at=NOW() WHERE doc_id = ANY(%s)",
                ([r["doc_id"] for r in chain],))
    cp = chain[-1].get("counterparty")
    # Менеджеру не пишем: событие штатное, а его задача закроется сама
    # (правило «TG-пуш менеджеру — только по тревоге»).
    if owner:
        note = "" if not manual else " Задачи в МойСкладе закрой вручную — связь с карточкой не найдена."
        await app.bot.send_message(
            owner, f"✅ {cp} погасил просроченную задолженность — претензионная работа закрыта." + note)


async def poll_job(app, db) -> None:
    """Ведёт цепочки претензионной работы: письмо №1 → автосборка письма №2 по
    сроку контроля → передача комплекта юристу; в любой момент — закрытие при
    погашении просрочки.

    Обход идёт по контрагентам, а не по письмам: у одной цепочки бывает две
    открытые строки (письма №1 и №2), и независимый обход слал бы дубли.

    План: plans/2026-08-10-регламент-претензионной-работы-по-оплатам.md, Фазы 5–9.
    """
    ensure_schema(db)
    rows = db._fetchall("SELECT * FROM claim_control WHERE closed_at IS NULL "
                        "ORDER BY agent_id, doc_id")
    if not rows:
        return

    owner = int(os.getenv("OWNER_CHAT_ID") or 0)
    now = datetime.now(MSK)

    chains: dict = {}
    for row in rows:
        if row.get("agent_id"):
            chains.setdefault(row["agent_id"], []).append(row)

    for agent_id, chain in chains.items():
        active = chain[-1]                       # последнее письмо цепочки
        cp = active.get("counterparty") or "—"
        # Порог ст. 91.1 отсчитывается от ПЕРВОЙ претензии, а не от последнего письма.
        started = min((r.get("chain_started_at") or r.get("approved_at")) for r in chain)
        try:
            debt = await _overdue_debt(agent_id)
        except Exception as e:
            logger.error("claim poll: просрочка по %s не получена: %s", cp, e)
            continue

        # 1. Просрочка погашена — закрываем цепочку целиком.
        if debt <= 0:
            await _close_chain(app, db, chain, owner)
            continue

        deadline = active.get("deadline")
        if not deadline or now < deadline:
            continue

        # 2. Срок по письму №1 вышел, денег нет — система сама собирает письмо №2.
        if active.get("letter_type") == "claim_payment":
            if active.get("final_requested_at"):
                continue
            res = await _request_final_claim(agent_id, cp)
            if not res.get("ok"):
                logger.error("claim poll: автосборка письма №2 по %s не удалась: %s", cp, res.get("error"))
                if owner:
                    await app.bot.send_message(
                        owner, f"⚠️ {cp}: срок по претензии вышел, но собрать повторную "
                               f"не удалось ({res.get('error')}). Собери вручную в «Документах».")
                continue
            db._execute("UPDATE claim_control SET final_requested_at=NOW() WHERE doc_id=%s",
                        (active["doc_id"],))
            if res.get("queued") is False:
                continue                          # письмо №2 уже ждёт решения
            if owner:
                await app.bot.send_message(
                    owner, f"📄 {cp}: срок по претензии вышел, просрочка {_money(debt)} ₽ не погашена. "
                           f"Собираю повторную претензию — придёт на согласование через пару минут.")
            await _notify_manager(app, active.get("manager_tag"),
                                  f"📄 {cp}: оплата по претензии не поступила, просрочка "
                                  f"{_money(debt)} ₽. Собрана повторная (окончательная) претензия, "
                                  f"ждёт согласования собственника — после одобрения отправь клиенту.")
            continue

        # 3. Срок по письму №2 вышел — комплект юристу, но не раньше порога ст. 91.1.
        if active.get("letter_type") == "claim_payment_final":
            if active.get("escalated_at"):
                continue
            days_since = (now - started).days
            if days_since < NOTARY_THRESHOLD_DAYS:
                continue
            db._execute("UPDATE claim_control SET escalated_at=NOW() WHERE doc_id=%s",
                        (active["doc_id"],))
            # Комплект юристу — до карточки: в сообщении собственнику должен быть
            # виден результат отправки, а не обещание.
            sent = await _send_to_lawyer(agent_id, cp)
            if sent.get("ok"):
                db._execute("UPDATE claim_control SET lawyer_sent_at=NOW() WHERE doc_id=%s",
                            (active["doc_id"],))
            card = await create_claim_control_card(
                "claim_payment_final", cp, "васильев", float(debt),
                details="Сроки по обеим претензиям вышли, оплата не поступила. Комплект документов "
                        "передан юристу. Решение: суд либо исполнительная надпись нотариуса "
                        "(п. 7.10 Договора).")
            if owner:
                lawyer_line = (f"\n📮 Комплект ушёл юристу ({sent.get('to')}): "
                               f"{sent.get('claims')} претензии, {sent.get('attachments')} вложений."
                               if sent.get("ok")
                               else f"\n⚠️ Комплект юристу НЕ ушёл: {sent.get('error')}. Отправь вручную.")
                await app.bot.send_message(
                    owner,
                    f"⚖️ {cp}: сроки по повторной претензии вышли, просрочка {_money(debt)} ₽ не погашена."
                    f"\nПрошло {days_since} дн. с первой претензии — порог ст. 91.1 (14 дн.) пройден, "
                    f"можно к нотариусу или в суд."
                    + lawyer_line
                    + ("\n🗂 Карточка на доске «Претензии» поставлена." if card.get("ok")
                       else f"\n⚠️ Карточку поставить не удалось: {card.get('error')}"))
