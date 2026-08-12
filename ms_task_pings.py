"""Пинг исполнителю в Telegram по просроченным задачам МойСклад.

План: plans/2026-08-12-пинг-просроченных-задач-моисклад.md (репо «второй мозг»).

Задачи ставятся в МС (через MCP и бота), но исполнитель узнаёт о наступившем
дедлайне, только если сам зайдёт в МойСклад. Джоба раз в день утром Пн–Пт шлёт
исполнителю личный список просрочек со ссылками на карточки, а задачи с
просрочкой дольше ESCALATE_DAYS дублирует собственнику.

Только чтение МС: ничего не закрывает и не правит (см. правило «no writes to
MoySklad» — задачи закрывает человек в МС).
"""
import logging
import os
from datetime import datetime, timedelta, timezone

import moysklad

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))

# Карта «сотрудник МС → чат в Telegram»: "employee_id:chat_id" через запятую.
# Дефолт — Белякова А. (закупки), решение собственника 12.08.2026: пингуем
# пока только её, расширение карты — отдельным решением.
_DEFAULT_PING_MAP = "45a43dab-a05b-11f0-0a80-0d1c0024b24f:8267564735"

# Задача без дедлайна считается зависшей через столько дней от создания.
NO_DUE_DAYS = int(os.getenv("MS_TASK_PING_NO_DUE_DAYS", "14"))
# С какой просрочки копия уходит собственнику.
ESCALATE_DAYS = int(os.getenv("MS_TASK_PING_ESCALATE_DAYS", "3"))

_TITLE_LIMIT = 80


def ping_map() -> dict:
    """{ms_employee_id: tg_chat_id} из env MS_TASK_PING_MAP."""
    raw = os.getenv("MS_TASK_PING_MAP") or _DEFAULT_PING_MAP
    result = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair or ":" not in pair:
            continue
        emp_id, _, chat = pair.partition(":")
        emp_id, chat = emp_id.strip(), chat.strip()
        if not emp_id or not chat.lstrip("-").isdigit():
            logger.warning("ms_task_pings: пропущена пара «%s» в MS_TASK_PING_MAP", pair)
            continue
        result[emp_id] = int(chat)
    return result


def _title(task: dict) -> str:
    """Первая строка описания задачи, обрезанная под лимит."""
    text = (task.get("description") or "").strip()
    first = text.split("\n", 1)[0].strip() or "(без описания)"
    if len(first) > _TITLE_LIMIT:
        first = first[: _TITLE_LIMIT - 1].rstrip() + "…"
    return first


def _plural_days(n: int) -> str:
    """«1 день», «2 дня», «5 дней» — именительный падеж."""
    if 11 <= n % 100 <= 14:
        return "дней"
    return {1: "день", 2: "дня", 3: "дня", 4: "дня"}.get(n % 10, "дней")


def _plural_days_gen(n: int) -> str:
    """«от 1 дня», «от 3 дней» — родительный падеж."""
    if n % 10 == 1 and n % 100 != 11:
        return "дня"
    return "дней"


def select_overdue(tasks: list, now: datetime | None = None) -> list:
    """Отбирает просроченные задачи и считает возраст просрочки.

    Возвращает список словарей задачи + ключи:
      overdue_days — календарных дней с даты дедлайна (для задач без дедлайна —
                     с даты создания); 0 = срок вышел сегодня;
      no_due       — True, если у задачи не было дедлайна.
    Сортировка: самая старая просрочка первой.

    Дни считаются по календарю, а не по 24-часовым интервалам: вчерашний срок —
    это «просрочка 1 день», даже если прошло 18 часов.
    """
    now = now or datetime.now(MSK).replace(tzinfo=None)
    overdue = []
    for task in tasks:
        due, created = task.get("due"), task.get("created")
        if due is not None:
            if due >= now:
                continue
            days = (now.date() - due.date()).days
            no_due = False
        else:
            if created is None:
                continue
            days = (now.date() - created.date()).days
            if days < NO_DUE_DAYS:
                continue
            no_due = True
        overdue.append({**task, "overdue_days": days, "no_due": no_due})
    overdue.sort(key=lambda t: t["overdue_days"], reverse=True)
    return overdue


def render_for_assignee(overdue: list) -> str:
    """Сообщение исполнителю. HTML — описания задач из МС ломают Markdown."""
    from html import escape

    count = len(overdue)
    head = f"⏰ Просроченные задачи в МойСклад: <b>{count}</b>\n"
    lines = [head]
    for task in overdue:
        days = task["overdue_days"]
        if task["no_due"]:
            tail = f"без срока, висит {days} {_plural_days(days)}"
        elif days == 0:
            tail = f"срок вышел сегодня в {task['due']:%H:%M}"
        else:
            tail = f"срок {task['due']:%d.%m}, просрочка {days} {_plural_days(days)}"
        lines.append(f'• <a href="{task["url"]}">{escape(_title(task))}</a>\n  {tail}')
    lines.append("\nЗакрой в МойСклад или перенеси срок — тогда напоминание уйдёт.")
    return "\n".join(lines)


def render_for_owner(name: str, overdue: list) -> str:
    """Сообщение собственнику: только затяжные просрочки."""
    from html import escape

    lines = [f"🔴 <b>{escape(name)}</b> — затянувшиеся просрочки "
             f"(от {ESCALATE_DAYS} {_plural_days_gen(ESCALATE_DAYS)}):\n"]
    for task in overdue:
        days = task["overdue_days"]
        mark = "без срока, висит" if task["no_due"] else "просрочка"
        lines.append(f'• <a href="{task["url"]}">{escape(_title(task))}</a>'
                     f' — {mark} {days} {_plural_days(days)}')
    return "\n".join(lines)


async def _employee_name(employee_id: str) -> str:
    try:
        for emp in await moysklad.list_employees():
            if emp.get("id") == employee_id:
                return emp.get("name") or employee_id
    except Exception as e:
        logger.warning("ms_task_pings: имя сотрудника %s не получено: %s", employee_id, e)
    return employee_id


async def run(app, owner_chat_id: int | None = None, dry_run: bool = False) -> dict:
    """Один прогон: собрать просрочки по карте и разослать пинги.

    Возвращает сводку {employee_id: {"overdue": n, "escalated": m, "sent": bool}}.
    При dry_run сообщения не отправляются — только логируются (для проверки
    текста на живых данных).
    """
    summary = {}
    escalations = []

    for employee_id, chat_id in ping_map().items():
        try:
            tasks = await moysklad.list_open_tasks(employee_id)
        except Exception as e:
            logger.error("ms_task_pings: не прочитаны задачи %s: %s", employee_id, e)
            summary[employee_id] = {"overdue": 0, "escalated": 0, "sent": False,
                                    "error": str(e)}
            continue

        overdue = select_overdue(tasks)
        stale = [t for t in overdue if t["overdue_days"] >= ESCALATE_DAYS]
        summary[employee_id] = {"overdue": len(overdue), "escalated": len(stale),
                                "sent": False}
        if not overdue:
            logger.info("ms_task_pings: %s — просрочек нет", employee_id)
            continue

        text = render_for_assignee(overdue)
        if dry_run:
            logger.info("ms_task_pings DRY-RUN → chat %s:\n%s", chat_id, text)
        else:
            try:
                await app.bot.send_message(
                    chat_id=chat_id, text=text, parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                summary[employee_id]["sent"] = True
            except Exception as e:
                logger.error("ms_task_pings: пинг %s в чат %s не ушёл: %s",
                             employee_id, chat_id, e)
        if stale:
            escalations.append((await _employee_name(employee_id), stale))

    if escalations and owner_chat_id:
        for name, stale in escalations:
            text = render_for_owner(name, stale)
            if dry_run:
                logger.info("ms_task_pings DRY-RUN → owner:\n%s", text)
                continue
            try:
                await app.bot.send_message(
                    chat_id=owner_chat_id, text=text, parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception as e:
                logger.error("ms_task_pings: эскалация по %s не ушла: %s", name, e)

    return summary


async def poll_job(app, owner_chat_id: int | None = None) -> dict:
    """Обёртка для JobQueue: молчим в выходные, ошибки не роняют джобу."""
    now = datetime.now(MSK)
    if now.weekday() >= 5:
        return {}
    try:
        return await run(app, owner_chat_id=owner_chat_id)
    except Exception as e:
        logger.error("ms_task_pings poll_job: %s", e, exc_info=True)
        return {}
