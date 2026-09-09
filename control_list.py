"""
Лист контроля дебиторки — план 2026-09-09.

Две функции:
  1. Метка «клиент из листа контроля» в светофоре согласований (notifier.py).
  2. Ежедневная сводка собственнику в 16:00 МСК: общий долг, дельта за сутки,
     за счёт кого изменение.

Просрочка считается через `moysklad.compute_overdue_color` — тем же пайплайном,
что ПДЗ-дайджест (demand-FIFO приходов, при недоступности — ppm+LIFO). Свой
расчёт здесь заводить нельзя: разошедшиеся цифры в двух отчётах про один долг
обесценивают оба (см. feedback «notifier PDZ must match digest»).
"""

import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

MSK = ZoneInfo("Europe/Moscow")

# Стартовый состав листа (тяжёлая дебиторка Карины Баласанян на 09.09.2026).
# Карточки МС; group_key = ИНН, поэтому три дубля ООО «ПЕЧИ» в сводке идут
# одной строкой — решение собственника 09.09.2026.
SEED = [
    ("e959b800-a11d-11f0-0a80-056600237880", "ИП Буртаев Александр Александрович", "130701320272", "ИП Буртаев А.А.", "стоп отгрузок"),
    ("60edd292-89a9-11f1-0a80-09fd003579c0", 'ООО "ПЕЧИ"', "7703416044", "ООО «ПЕЧИ»", "сторнирование долга"),
    ("684c9f48-adae-11f0-0a80-0c0400343403", 'ООО "ПЕЧИ"', "7703416044", "ООО «ПЕЧИ»", "сторнирование долга"),
    ("92e8f4c6-89a5-11f1-0a80-1062003390cd", 'ООО "ПЕЧИ"', "7703416044", "ООО «ПЕЧИ»", "сторнирование долга"),
    ("e6d82c98-a11d-11f0-0a80-056600237512", 'ООО "ЧАЙНА НЬЮС"', "7728875843", "ООО «ЧАЙНА НЬЮС»", "стоп отгрузок"),
    ("e74c596e-a11d-11f0-0a80-0566002375bb", 'ООО "ЧАЙНА НЬЮС АРБАТ"', "7728337122", "ООО «ЧАЙНА НЬЮС АРБАТ»", "стоп отгрузок"),
    ("e9a2a82f-a11d-11f0-0a80-0566002378f2", "ИП Гайтукиев Сайдмагомед Русланович", "060308801523", "ИП Гайтукиев С.Р.", "недоплата"),
    ("dd724eae-7aac-11f1-0a80-1e290009d53e", "ИП Григорян Камета Самалудиевна", "561009786216", "ИП Григорян Камета С.", "претензия"),
    ("02b89a53-7aac-11f1-0a80-0ed90008b52e", "ИП Григорян Арсен Ленордович", "561004435060", "ИП Григорян Арсен Л.", "претензия"),
    ("f0b6c25e-0029-11f1-0a80-029d0034548a", "ИП Карабаев Арзымат Кадырбекович", "332201176580", "ИП Карабаев А.К.", "претензия №2 от 10.08"),
    ("6cc28727-fc2b-11f0-0a80-0d6e001f2f3c", "ИП Губа Дарья Алексеевна", "770770580890", "ИП Губа Д.А.", "стоп отгрузок"),
    ("e8754e90-a11d-11f0-0a80-0566002376f2", "ИП Иванов Артем Юрьевич", "503420653308", "ИП Иванов А.Ю.", "претензия"),
    ("5e86af21-d8e0-11f0-0a80-166200208d0f", "ИП Маркин Анатолий Олегович", "772173304749", "ИП Маркин А.О.", "претензия"),
    ("9b6bb588-75e2-11f1-0a80-19c4001a7e59", 'ООО "ТОПАЗ ХОРЕКА"', "9723260590", "ООО «ТОПАЗ ХОРЕКА»", "претензия"),
    ("e8096fa4-0734-11f1-0a80-183f000a102e", "ИП Мушаилова Юлия Николаевна", "131900510985", "ИП Мушаилова Ю.Н.", "претензия"),
    ("fde49bd3-3404-11f1-0a80-09ec000d8a2b", "ИП Моргачев Иван Олегович", "503614517583", "ИП Моргачев И.О.", ""),
    ("616de88a-536b-11f1-0a80-145e000be555", 'ООО "ХИНКАЛЬНАЯ МЕТРОПОЛИС"', "9704173535", "ООО «ХИНКАЛЬНАЯ МЕТРОПОЛИС»", "претензия №29 от 22.07"),
]


def seed_control_list(db) -> int:
    """Заливает стартовый состав. Идемпотентно — можно звать при каждом старте."""
    n = 0
    for agent_id, name, inn, group_name, note in SEED:
        try:
            db.add_to_control_list(
                agent_id=agent_id, agent_name=name, inn=inn,
                group_key=inn, group_name=group_name,
                manager_tag="баласанян", note=note,
            )
            n += 1
        except Exception as e:
            logger.warning(f"seed_control_list {name}: {e}")
    return n


def _fmt(amount: float) -> str:
    """1394283.77 → «1 394 284». Копейки в сводке не нужны."""
    return f"{round(amount):,.0f}".replace(",", " ")


def _signed(amount: float) -> str:
    return ("+" if amount > 0 else "−") + _fmt(abs(amount))


async def _fetch_one(agent_id: str) -> dict:
    """Сальдо + просрочка по одной карточке МС."""
    import aiohttp
    from moysklad import MS_BASE, get_headers, compute_overdue_color

    balance = 0.0
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/report/counterparty/{agent_id}",
                headers=get_headers(),
                timeout=aiohttp.ClientTimeout(total=20),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    balance = (data.get("balance", 0) or 0) / 100
                else:
                    logger.warning(f"control_list: report/counterparty {agent_id[:8]} → {r.status}")
    except Exception as e:
        logger.warning(f"control_list: balance {agent_id[:8]} → {e}")

    overdue, days = 0.0, 0
    try:
        ov = await compute_overdue_color(agent_id)
        overdue = float(ov.get("debt", 0) or 0)
        days = int(ov.get("days", 0) or 0)
    except Exception as e:
        logger.warning(f"control_list: overdue {agent_id[:8]} → {e}")

    return {"agent_id": agent_id, "balance": balance, "overdue": overdue, "max_days": days}


async def collect_snapshot(db) -> list:
    """Текущая картина по всему листу. Последовательно, не пачкой:
    compute_overdue_color сам тянет все заказы контрагента, параллельный запуск
    на 17 карточек упирается в rate-limit МС (см. reference ms_api_rate_limit)."""
    rows = db.get_control_list()
    out = []
    for row in rows:
        snap = await _fetch_one(row["agent_id"])
        snap["agent_name"] = row["agent_name"]
        snap["group_key"] = row.get("group_key") or row["agent_id"]
        snap["group_name"] = row.get("group_name") or row["agent_name"]
        out.append(snap)
        await asyncio.sleep(0.3)
    return out


def _group(rows: list) -> dict:
    """Карточки → клиенты. Долг = −balance (положительное число = должен нам)."""
    groups = {}
    for r in rows:
        key = r["group_key"]
        g = groups.setdefault(key, {"name": r["group_name"], "debt": 0.0,
                                    "overdue": 0.0, "max_days": 0})
        g["debt"] += -float(r["balance"])
        g["overdue"] += float(r["overdue"])
        g["max_days"] = max(g["max_days"], int(r["max_days"]))
    # Просрочка не может превышать долг клиента. У ЮЛ с дублями карточек
    # (ООО «ПЕЧИ»: минус на одной, переплата на другой) сумма просрочек по
    # карточкам обгоняет схлопнутое сальдо — показывать «просрочено 173 340
    # при долге 112 055» нельзя, это читается как ошибка.
    for g in groups.values():
        g["overdue"] = min(g["overdue"], max(0.0, g["debt"]))
    return groups


def build_summary(rows: list, prev: dict) -> str:
    """rows — сегодняшний снимок, prev — {agent_id: строка прошлого снимка}."""
    today = datetime.now(MSK)
    groups = _group(rows)

    prev_rows = []
    for r in rows:
        p = prev.get(r["agent_id"])
        if p is None:
            continue
        prev_rows.append({**r, "balance": float(p["balance"]),
                          "overdue": float(p["overdue"]), "max_days": int(p["max_days"])})
    prev_groups = _group(prev_rows) if prev_rows else {}

    total = sum(g["debt"] for g in groups.values())
    total_overdue = sum(g["overdue"] for g in groups.values())

    head = f"📋 *Лист контроля* · {today:%d.%m}\n"
    if prev_groups:
        prev_total = sum(g["debt"] for g in prev_groups.values())
        delta = total - prev_total
        head += f"Долг *{_fmt(total)} ₽*"
        head += f" ({_signed(delta)} ₽ за сутки)\n" if abs(delta) >= 1 else " (без изменений)\n"
    else:
        head += f"Долг *{_fmt(total)} ₽* (первый замер, сравнивать не с чем)\n"

    down, up, new = [], [], []
    for key, g in groups.items():
        p = prev_groups.get(key)
        if p is None:
            # В прошлом снимке группы не было — значит клиента добавили в лист
            # сегодня. На первом замере prev_groups пуст, тогда «новых» нет.
            if prev_groups:
                new.append(g)
            continue
        d = g["debt"] - p["debt"]
        if d <= -1:
            down.append((d, g))
        elif d >= 1:
            up.append((d, g))

    body = ""
    if down:
        body += "\n*Уменьшили:*\n"
        for d, g in sorted(down, key=lambda x: x[0]):
            body += f"  {g['name']} — {_signed(d)} → {_fmt(g['debt'])}\n"
    if up:
        body += "\n*Выросли:*\n"
        for d, g in sorted(up, key=lambda x: -x[0]):
            body += f"  {g['name']} — {_signed(d)} → {_fmt(g['debt'])}\n"
    if new:
        body += "\n*Новые в листе:*\n"
        for g in new:
            body += f"  {g['name']} — {_fmt(g['debt'])}\n"
    if not body and prev_groups:
        body = "\nНи по одному клиенту движения нет.\n"

    tail = f"\nПросрочено *{_fmt(total_overdue)} ₽*"
    worst = max(groups.values(), key=lambda g: g["max_days"], default=None)
    if worst and worst["max_days"] > 0:
        tail += f" · дольше всех {worst['name']} ({worst['max_days']} дн)"
    tail += f"\nКлиентов в листе: {len(groups)}"
    return head + body + tail


async def send_daily_summary(bot, db, chat_id: int) -> None:
    """Джоба 16:00 МСК. Снимок → сравнение с прошлым → сообщение → запись снимка.

    Снимок пишется ПОСЛЕ отправки: если МС недоступен и цифры кривые, лучше не
    зафиксировать день, чем считать завтрашнюю дельту от мусора.
    """
    today = datetime.now(MSK).date()
    rows = await collect_snapshot(db)
    if not rows:
        logger.info("control_list: лист пуст, сводка не отправлена")
        return
    prev = db.get_last_control_snapshot(today)
    text = build_summary(rows, prev)
    await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
    db.save_control_snapshot(today, rows)
    logger.info(f"control_list: сводка отправлена, {len(rows)} карточек")
