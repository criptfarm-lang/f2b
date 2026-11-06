"""Решения по возвратам и уценкам из претензий менеджеров (дашборд → «Претензии»).

План: plans/2026-11-06-претензии-возврат-уценка.md (решения собственника 06.11.2026).
- Возврат: «Принимаем» – менеджеру пуш, в группу «F2B СКЛАД» и Рудневой в личку:
  ожидаем возврат по УПД, принять и передать Рудневой для вычерка.
  «Не принимаем» – только менеджеру (склад не трогаем).
- Уценка: «Согласовано» – цена клиента по позициям; «Другая цена» – нажавший пишет
  цену ответом. Итог – менеджеру, в «F2B СКЛАД» и Рудневой в личку.
Протокол совещания читает manager_claims сам (f2b-publisher/src/claims_protocol.py).
Таблицу создаёт дашборд (quiz-game), здесь только свои колонки ожидания цены.
"""
import json
import logging
import re

logger = logging.getLogger(__name__)

AWAIT_MINUTES = 30
_NUM_RE = re.compile(r"\d+(?:[.,]\d+)?")  # цены без пробелов-разделителей: пробел делит позиции

STATUS_NOTE = {
    "accepted": "Возврат принимаем. Склад предупреждён – договоритесь с клиентом о возврате.",
    "declined": "Возврат не принимаем. Объясните клиенту решение.",
    "approved": "Уценка согласована по цене клиента. Склад и Руднева уведомлены.",
    "counter": "Уценка согласована по другой цене (см. позиции). Донесите цену до клиента.",
}


def ensure_schema(db) -> None:
    db._execute("ALTER TABLE manager_claims ADD COLUMN IF NOT EXISTS awaiting_by BIGINT")
    db._execute("ALTER TABLE manager_claims ADD COLUMN IF NOT EXISTS awaiting_since TIMESTAMPTZ")


def _items(row) -> list:
    v = row.get("items") or []
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            v = []
    return v if isinstance(v, list) else []


def _num(v) -> str:
    try:
        f = float(v or 0)
    except (TypeError, ValueError):
        return str(v or "")
    return f"{f:,.3f}".rstrip("0").rstrip(".").replace(",", " ").replace(".", ",")


def _upd(row) -> str:
    return f"УПД № {row.get('upd_name') or '—'} от {row.get('upd_date') or '—'}"


def _pos_name(it) -> str:
    return f"{it.get('code') or ''} {it.get('name') or ''}".strip()


def _return_lines(row) -> str:
    return "\n".join(f"• {_pos_name(it)} – {_num(it.get('qty'))} {it.get('uom') or ''}".rstrip()
                     for it in _items(row))


def _price_lines(row, key: str) -> str:
    return "\n".join(f"• {_pos_name(it)}: было {_num(it.get('price'))} ₽ → {_num(it.get(key))} ₽"
                     for it in _items(row))


async def _send(bot, chat_id, text) -> None:
    if not chat_id:
        return
    try:
        await bot.send_message(chat_id=chat_id, text=text)
    except Exception as e:
        logger.warning("manager claim: не отправлено в %s: %r", chat_id, e)


async def _to_warehouse_and_rudneva(bot, wh_text: str, rud_text: str) -> None:
    from notifier import _warehouse_chat_id
    from edo_unsigned import _rudneva_chat_id
    await _send(bot, _warehouse_chat_id(), wh_text)
    await _send(bot, _rudneva_chat_id(), rud_text)


async def _finish(bot, db, row, status: str, decided_by: str, items=None) -> dict | None:
    """Ставит итоговый статус (только из pending/awaiting_price) и шлёт всем по решению.
    None – решение уже принято раньше (второй копией или двойным нажатием)."""
    params = [status, STATUS_NOTE[status], decided_by]
    items_sql = ""
    if items is not None:
        items_sql = ", items=%s::jsonb"
        params.append(json.dumps(items, ensure_ascii=False))
    params.append(row["id"])
    done = db._fetchone(
        f"""UPDATE manager_claims SET status=%s, decision_note=%s, decided_by=%s, decided_at=NOW(),
                   awaiting_by=NULL, awaiting_since=NULL{items_sql}
             WHERE id=%s AND status IN ('pending','awaiting_price') RETURNING *""",
        tuple(params))
    if not done:
        return None

    cli = done.get("client_name") or "—"
    cid = done["id"]
    head = f"клиент «{cli}», {_upd(done)}"
    if status == "accepted":
        pkg = "вскрыта" if done.get("package_opened") else "не вскрыта"
        await _to_warehouse_and_rudneva(
            bot,
            f"📦 Ожидаем возврат: {head}.\n{_return_lines(done)}\nУпаковка: {pkg}.\n\n"
            f"Необходимо принять (по правилам возвратов – сразу в цех, не на Основной склад) "
            f"и передать информацию Евгении Рудневой для вычерка из {_upd(done)}.\n"
            f"Решение: {decided_by}, претензия №{cid}.",
            f"📦 Согласован возврат: {head}.\n{_return_lines(done)}\n\n"
            f"После приёмки складом – вычерк этих позиций из {_upd(done)}. Претензия №{cid}.",
        )
    elif status in ("approved", "counter"):
        lines = _price_lines(done, "final_price")
        await _to_warehouse_and_rudneva(
            bot,
            f"💲 Уценка согласована: {head}.\n{lines}\n\n"
            f"Информация для Евгении Рудневой: исправить цену в {_upd(done)}.\n"
            f"Решение: {decided_by}, претензия №{cid}.",
            f"💲 Уценка согласована: {head}.\n{lines}\n\n"
            f"Исправить цену в {_upd(done)}. Претензия №{cid}.",
        )
    return done


def manager_push(row) -> str:
    """Пуш менеджеру по итогу решения."""
    cid, cli, st = row["id"], row.get("client_name") or "—", row.get("status")
    if st == "accepted":
        return (f"✅ Возврат №{cid} (клиент «{cli}», {_upd(row)}) принимаем.\n{_return_lines(row)}\n\n"
                f"Склад предупреждён. Договоритесь с клиентом о возврате.")
    if st == "declined":
        return (f"❌ Возврат №{cid} (клиент «{cli}», {_upd(row)}) не принимаем.\n\n"
                f"Объясните клиенту решение.")
    if st == "approved":
        return (f"✅ Уценка №{cid} (клиент «{cli}», {_upd(row)}) согласована по цене клиента:\n"
                f"{_price_lines(row, 'final_price')}")
    if st == "counter":
        return (f"✏️ Уценка №{cid} (клиент «{cli}», {_upd(row)}): согласована другая цена:\n"
                f"{_price_lines(row, 'final_price')}\n\nДонесите цену до клиента.")
    return ""


async def decide(bot, db, row, action: str, uid: int, decided_by: str) -> tuple[str, dict | None]:
    """Кнопка решения. Возвращает (ответ нажавшему, итоговая строка или None).
    Итоговая строка есть – кнопки снимаем и пушим менеджеру (это делает вызывающий)."""
    ensure_schema(db)
    kind = row.get("kind")
    if action in ("mclaim_accept", "mclaim_decline"):
        if kind != "return":
            return "Кнопка не подходит к этому обращению.", None
        st = "accepted" if action == "mclaim_accept" else "declined"
        done = await _finish(bot, db, row, st, decided_by)
        if not done:
            return "Уже решено раньше.", None
        what = "принимаем" if st == "accepted" else "не принимаем"
        tail = " Склад и Руднева уведомлены." if st == "accepted" else ""
        return f"Возврат №{row['id']} ({row.get('client_name')}) – {what}. Решил: {decided_by}.{tail}", done

    if kind != "markdown":
        return "Кнопка не подходит к этому обращению.", None
    if action == "mclaim_approve":
        items = [dict(it, final_price=it.get("req_price")) for it in _items(row)]
        done = await _finish(bot, db, row, "approved", decided_by, items)
        if not done:
            return "Уже решено раньше.", None
        return (f"Уценка №{row['id']} ({row.get('client_name')}) согласована. Решил: {decided_by}. "
                f"Склад и Руднева уведомлены."), done

    # «Другая цена»: ждём число (или по числу на позицию) ответом в личке.
    db._execute("""UPDATE manager_claims SET awaiting_by=NULL, awaiting_since=NULL, status='pending'
                    WHERE status='awaiting_price' AND awaiting_by=%s AND id<>%s""", (uid, row["id"]))
    upd = db._fetchone("""UPDATE manager_claims SET status='awaiting_price', awaiting_by=%s,
                                 awaiting_since=NOW()
                           WHERE id=%s AND status IN ('pending','awaiting_price') RETURNING *""",
                       (uid, row["id"]))
    if not upd:
        return "Уже решено раньше.", None
    items = _items(upd)
    if len(items) == 1:
        ask = f"Напишите цену за {items[0].get('uom') or 'ед.'} одним числом"
    else:
        ask = ("Напишите цены по позициям в том же порядке, через пробел или с новой строки:\n"
               + "\n".join(f"{i}. {_pos_name(it)} (было {_num(it.get('price'))}, просят "
                           f"{_num(it.get('req_price'))})" for i, it in enumerate(items, 1)))
    return (f"✏️ Уценка №{upd['id']} ({upd.get('client_name')}). {ask}\n"
            f"«отмена» – вернуть кнопки. Жду {AWAIT_MINUTES} мин."), None


async def price_input(message, context, db, decided_by: str) -> tuple[bool, dict | None]:
    """Руководитель пишет цену после «Другая цена». (обработано?, итоговая строка)."""
    ensure_schema(db)
    uid = message.from_user.id if message.from_user else 0
    row = db._fetchone(
        """SELECT * FROM manager_claims WHERE status='awaiting_price' AND awaiting_by=%s
             AND awaiting_since > NOW() - (%s || ' minutes')::interval
           ORDER BY awaiting_since DESC LIMIT 1""", (uid, str(AWAIT_MINUTES)))
    if not row:
        return False, None
    text = (message.text or "").strip()
    if text.lower() in ("отмена", "отменить"):
        db._execute("""UPDATE manager_claims SET status='pending', awaiting_by=NULL, awaiting_since=NULL
                        WHERE id=%s AND status='awaiting_price'""", (row["id"],))
        await message.reply_text(f"Уценка №{row['id']}: ввод цены отменён, кнопки остались в сообщении.")
        return True, None
    items = _items(row)
    nums = [float(m.replace(",", ".")) for m in _NUM_RE.findall(text)]
    if not nums:
        return False, None              # не число – не ответ на запрос, разбирает бот дальше
    if len(nums) != len(items) or any(n <= 0 for n in nums):
        await message.reply_text(
            f"Нужно цен: {len(items)} (по одной на позицию, больше нуля), "
            f"получено: {len(nums)}. Напишите ещё раз или «отмена».")
        return True, None
    new_items = [dict(it, final_price=round(n, 2)) for it, n in zip(items, nums)]
    done = await _finish(context.bot, db, row, "counter", decided_by, new_items)
    if not done:
        await message.reply_text(f"Уценка №{row['id']} уже решена.")
        return True, None
    await message.reply_text(
        f"✅ Уценка №{row['id']} ({row.get('client_name')}) согласована по вашей цене:\n"
        f"{_price_lines(done, 'final_price')}\nМенеджер, склад и Руднева уведомлены.")
    return True, done
