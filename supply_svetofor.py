"""
Светофор Заказа поставщику (бот «Эф»).
План (второй мозг): plans/2026-07-09-светофор-заказа-поставщику.md — Фаза 2.

При попадании Заказа поставщику (purchaseorder) в статус «На согласовании» бот шлёт
Виктору (OWNER_CHAT_ID) алерт из 4 блоков:
  • Оборот   — дни запаса = (остаток + кол-во в заказе) / суточный расход (outcome).
  • Цена     — цена в заказе vs цена последнего поступления SKU.
  • Карточка  — контакты поставщика (max/telegram) 🟢/🔴.
  • Даты     — план. приёмка vs план. оплата → предоплата 🔴 / отсрочка 🟢.

Триггер — polling через PTB JobQueue (webhook по purchaseorder МС не шлёт — итог
разведки Фазы 0). Образец — processing_svetofor.py. Дедуп по (order_id, sum_hash):
повторный алерт по тому же заказу с той же суммой не шлётся; изменение состава/суммы
(re-submission) — шлёт заново. Защита от «потопа» на первом прогоне (лог пуст → сид без рассылки).

Расчётные функции живут в moysklad.py (compute_supply_*). Импорт moysklad ок: бот его
и так грузит, YANDEX_GEOCODER_KEY в проде задан; модуль импортируется лениво из bot.py.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta

import aiohttp
import psycopg2
import psycopg2.extras
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from moysklad import (
    MS_BASE, get_headers,
    SUPPLY_STATE_ON_APPROVAL, SUPPLY_STATE_AGREED,
    is_chilled_position,
    compute_supply_turnover_color,
    compute_supply_price_color,
    load_supplier_card,
    compute_supply_dates,
)

logger = logging.getLogger(__name__)

# Товарные позиции = product/variant. Услуги/логистику по обороту/цене не считаем.
_GOODS_TYPES = ("product", "variant")
_MAX_POS_SHOWN = 12          # сколько позиций показываем в теле (остальные — счётчиком)
_TURNOVER_WINDOW_DAYS = 60


LOG_DDL = """
create table if not exists public.supply_svetofor_log (
    order_id   uuid primary key,
    order_name text,
    last_state text,
    sum_hash   bigint,
    alerted_at timestamptz default now(),
    updated_at timestamptz default now()
);
create table if not exists public.supply_svetofor_state (
    id        int primary key default 1,
    seeded_at timestamptz
);
"""

# ── DB (свой коннект, autocommit — как в processing_svetofor) ────────────────
_conn = None


def _db():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(os.environ["DATABASE_URL"],
                                 cursor_factory=psycopg2.extras.RealDictCursor)
        _conn.autocommit = True
        with _conn.cursor() as cur:
            cur.execute(LOG_DDL)
    return _conn


def _log_get_all() -> dict[str, dict]:
    with _db().cursor() as cur:
        cur.execute("select order_id, sum_hash, last_state from public.supply_svetofor_log")
        return {str(r["order_id"]): r for r in cur.fetchall()}


def _is_seeded() -> bool:
    """Инициализирован ли светофор (первичный сид уже прошёл). Маркер не зависит
    от того, пуст ли лог — иначе при пустом логе первый реальный заказ был бы
    молча засижен вместо алерта."""
    with _db().cursor() as cur:
        cur.execute("select seeded_at from public.supply_svetofor_state where id=1")
        row = cur.fetchone()
        return bool(row and row.get("seeded_at"))


def _mark_seeded():
    with _db().cursor() as cur:
        cur.execute("""
            insert into public.supply_svetofor_state (id, seeded_at)
            values (1, now())
            on conflict (id) do update set seeded_at = now()
        """)


def _log_upsert(order_id: str, order_name: str, state: str, sum_hash: int):
    with _db().cursor() as cur:
        cur.execute("""
            insert into public.supply_svetofor_log
              (order_id, order_name, last_state, sum_hash, alerted_at, updated_at)
            values (%s, %s, %s, %s, now(), now())
            on conflict (order_id) do update set
              order_name = excluded.order_name,
              last_state = excluded.last_state,
              sum_hash   = excluded.sum_hash,
              alerted_at = now(),
              updated_at = now()
        """, (order_id, order_name, state, sum_hash))


# ── получатели ───────────────────────────────────────────────────────────────
def _recipients() -> list[int]:
    v = (os.getenv("OWNER_CHAT_ID") or "").strip()
    return [int(v)] if v.lstrip("-").isdigit() else []


# ── MS ─────────────────────────────────────────────────────────────────────
async def _fetch_on_approval() -> list[dict]:
    """Заказы поставщику, сейчас в статусе «На согласовании» (с позициями и атрибутами)."""
    state_href = f"{MS_BASE}/entity/purchaseorder/metadata/states/{SUPPLY_STATE_ON_APPROVAL}"
    rows, offset = [], 0
    async with aiohttp.ClientSession() as session:
        while True:
            async with session.get(
                f"{MS_BASE}/entity/purchaseorder",
                headers=get_headers(),
                params={"filter": f"state={state_href}", "limit": 50, "offset": offset,
                        "order": "moment,desc",
                        "expand": "agent,state,positions.assortment,attributes"},
                timeout=aiohttp.ClientTimeout(total=40),
            ) as r:
                if r.status != 200:
                    logger.warning(f"supply_svetofor: fetch on_approval {r.status}")
                    break
                data = await r.json()
            chunk = data.get("rows", [])
            rows.extend(chunk)
            if len(chunk) < 50:
                break
            offset += 50
    return rows


def _has_goods(order: dict) -> bool:
    """Есть ли в заказе хоть одна товарная позиция (product/variant).
    Чисто-логистические/сервисные ЗП (только «Услуги …») пропускаем — решение
    Виктора 2026-07-09: светофор только по товарным закупкам."""
    for p in (order.get("positions") or {}).get("rows", []) or []:
        if (p.get("assortment") or {}).get("meta", {}).get("type") in _GOODS_TYPES:
            return True
    return False


# ── форматирование ───────────────────────────────────────────────────────────
def _icon(color: str) -> str:
    return {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(color, "⚪")


def _fmt_money(amount: float) -> str:
    return f"{amount:,.0f}".replace(",", " ")


def _fmt_qty(q: float) -> str:
    return (f"{q:.0f}" if abs(q - round(q)) < 0.01 else f"{q:.1f}")


def _worst(colors: list[str]) -> str:
    rank = {"red": 3, "yellow": 2, "green": 1, "white": 0}
    present = [c for c in colors if c in rank]
    return max(present, key=lambda c: rank[c]) if present else "white"


async def _compute_position_blocks(order: dict) -> list[dict]:
    """По каждой товарной позиции — оборот и цена (параллельно, с общим таймаутом)."""
    positions = (order.get("positions") or {}).get("rows", []) or []
    goods = []
    for p in positions:
        a = p.get("assortment") or {}
        if a.get("meta", {}).get("type") not in _GOODS_TYPES:
            continue
        goods.append({
            "pid": a.get("id"),
            "name": a.get("name", ""),
            "qty": p.get("quantity") or 0,
            "price": (p.get("price", 0) or 0) / 100,
        })

    async def _one(g):
        turn, price = await asyncio.gather(
            compute_supply_turnover_color(g["pid"], g["name"], g["qty"],
                                          window_days=_TURNOVER_WINDOW_DAYS),
            compute_supply_price_color(g["pid"], g["price"]),
            return_exceptions=True,
        )
        if isinstance(turn, Exception):
            logger.warning(f"turnover fail {g['name']}: {turn!r}")
            turn = {"color": "white", "days": None}
        if isinstance(price, Exception):
            logger.warning(f"price fail {g['name']}: {price!r}")
            price = {"color": "yellow", "found": False, "last_price": None,
                     "diff_rub": 0, "diff_pct": 0}
        # g уже содержит числовой "price" (цена в заказе) — ценовой блок кладём отдельным ключом.
        return {**g, "turn": turn, "price_block": price}

    try:
        results = await asyncio.wait_for(
            asyncio.gather(*[_one(g) for g in goods]), timeout=90.0)
    except asyncio.TimeoutError:
        logger.warning(f"supply_svetofor: timeout расчёта позиций для {order.get('name')}")
        results = []
    return results


def _build_supplier_card_lines(card: dict) -> list[str]:
    """Инфо-блок «Карточка поставщика» — без цвета (решение Виктора 2026-07-10).
    Показываем поля как есть, чтобы оценить полноту глазами."""
    def _v(x):
        return x if x else "—"
    contacts = []
    if card.get("max"):      contacts.append(f"Max {card['max']}")
    if card.get("telegram"): contacts.append(f"TG {card['telegram']}")
    if card.get("whatsapp"): contacts.append(f"WA {card['whatsapp']}")
    contacts_str = " · ".join(contacts) if contacts else "—"

    dogovor = "подписан" if card.get("contract_signed") else "не подписан"
    if card.get("contract_number"):
        dogovor += f" · № {card['contract_number']}"
    signer = " · ".join(p for p in [card.get("signer_name"), card.get("signer_role")] if p) or "—"

    return [
        "\n*Карточка поставщика:*",
        f"   Контакт: {contacts_str}",
        f"   Контактное лицо: {_v(card.get('contact_person'))}",
        f"   Сайт: {_v((card.get('site') or '')[:60] + ('…' if len(card.get('site') or '') > 60 else ''))}",
        f"   Договор: {dogovor}",
        f"   Подписант: {signer}",
    ]


def _build_supply_alert_text(order: dict, pos_blocks: list[dict],
                             card: dict, dates: dict) -> str:
    now_msk = datetime.now(timezone(timedelta(hours=3))).strftime("%H:%M")
    order_name = order.get("name", "")
    agent = order.get("agent") or {}
    agent_name = agent.get("name", "—")
    order_sum = (order.get("sum", 0) or 0) / 100

    # Общий цвет — по Оборот/Цена/Даты. «Карточка» — инфо-блок без цвета (решение Виктора).
    block_colors = [dates["color"]]
    for b in pos_blocks:
        block_colors += [b["turn"]["color"], b["price_block"]["color"]]
    overall = _worst(block_colors)

    header = (
        f"{_icon(overall)} Заказ поставщику *{order_name}* · {_fmt_money(order_sum)} ₽\n"
        f"🏭 {agent_name} · 🕐 {now_msk}\n"
    )

    lines = [""]
    # Даты
    lines.append(
        f"{_icon(dates['color'])} *Даты:* приёмка {dates['receipt_str']} → "
        f"оплата {dates['payment_str']} · {dates['kind']}"
    )

    # Карточка поставщика — без цвета, просто показываем поля для оценки глазами.
    lines += _build_supplier_card_lines(card)

    # Позиции
    if not pos_blocks:
        lines.append("\n_Товарных позиций нет (услуги/логистика)._")
    else:
        lines.append("\n*Позиции:*")
        for b in pos_blocks[:_MAX_POS_SHOWN]:
            t = b["turn"]; pr = b["price_block"]
            chilled = " ❄️" if is_chilled_position(b["name"]) else ""
            name = (b["name"] or "")[:52]
            lines.append(f"{_icon(_worst([t['color'], pr['color']]))} {name}{chilled} — {_fmt_qty(b['qty'])} кг")
            # Оборот
            if t.get("days") is None:
                lines.append("   Оборот: ⚪ нет расхода за 60 дн — смотреть вручную")
            else:
                lines.append(
                    f"   Оборот: {_icon(t['color'])} {t['days']:.0f} дн "
                    f"(ост {_fmt_qty(t['stock'])} + заказ {_fmt_qty(b['qty'])}, "
                    f"расход {_fmt_qty(t['per_day'])}/сут)"
                )
            # Цена
            if not pr.get("found"):
                lines.append("   Цена: 🟡 нет данных о поступлении")
            else:
                sign = "+" if pr["diff_rub"] > 0 else ""
                lines.append(
                    f"   Цена: {_icon(pr['color'])} {_fmt_money(b['price'])} ₽ "
                    f"({sign}{pr['diff_pct']:.1f}% к посл. пост. {_fmt_money(pr['last_price'])} ₽)"
                )
        extra = len(pos_blocks) - _MAX_POS_SHOWN
        if extra > 0:
            lines.append(f"   … и ещё {extra} позиц.")

    return header + "\n".join(lines)


# ── кнопка «✅ Согласован» (Фаза 3, только Виктору) ──────────────────────────
def keyboard(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Согласован", callback_data=f"sappr:{order_id}"),
    ]])


async def _patch_supply_state(order_id: str, state_id: str):
    """PUT нативного статуса purchaseorder. Единственная разрешённая запись в МС
    (owner-действие по кнопке — memory feedback_no_writes_to_moysklad)."""
    state_href = f"{MS_BASE}/entity/purchaseorder/metadata/states/{state_id}"
    async with aiohttp.ClientSession() as session:
        async with session.put(
            f"{MS_BASE}/entity/purchaseorder/{order_id}",
            headers=get_headers(),
            json={"state": {"meta": {"href": state_href, "type": "state",
                                     "mediaType": "application/json"}}},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            if r.status not in (200, 201):
                body = (await r.text())[:200]
                raise RuntimeError(f"PUT статуса {r.status}: {body}")


async def handle_supply_approval_callback(update, context):
    """Callback кнопки «✅ Согласован» (pattern ^sappr:). Только Виктор.
    q.answer() вызываем СРАЗУ (урок светофора техопераций — иначе Telegram timeout)."""
    q = update.callback_query
    try:
        _, order_id = (q.data or "").split(":", 1)
    except ValueError:
        await q.answer()
        return

    owner = (os.getenv("OWNER_CHAT_ID") or "").strip()
    if owner.isdigit() and update.effective_user and update.effective_user.id != int(owner):
        await q.answer("Нет прав")
        return

    await q.answer("Отмечаю…")
    try:
        await _patch_supply_state(order_id, SUPPLY_STATE_AGREED)
        base = q.message.text or ""
        # Идемпотентность: повторный клик просто снова выставит тот же статус и уберёт кнопку.
        await q.edit_message_text(f"{base}\n\n→ ✅ Согласован", reply_markup=None)
        logger.info(f"supply_svetofor: заказ {order_id} → Согласован (Виктор)")
    except Exception as e:
        logger.error(f"supply_svetofor callback {order_id}: {e}")
        try:
            await context.bot.send_message(
                chat_id=q.message.chat_id,
                text=f"⚠️ Светофор ЗП: не удалось выставить «Согласован» — {e}. "
                     f"Кнопка на месте, попробуй ещё раз.")
        except Exception:
            pass


# ── детект + отправка ────────────────────────────────────────────────────────
async def check_supply_approval_needed(order: dict, app) -> bool:
    """Считает 4 блока по заказу, шлёт алерт получателям. True — если отправлено."""
    order_id = order.get("id") or (order.get("meta", {}).get("href", "").split("/")[-1])
    order_name = order.get("name", "")
    agent = order.get("agent") or {}
    agent_id = agent.get("id") or agent.get("meta", {}).get("href", "").split("/")[-1]

    card = (await load_supplier_card(agent_id)) if agent_id else \
        {"contact_person": "", "max": "", "telegram": "", "whatsapp": "", "site": "",
         "contract_signed": False, "contract_number": "", "signer_role": "", "signer_name": ""}
    dates = compute_supply_dates(order)
    pos_blocks = await _compute_position_blocks(order)

    text = _build_supply_alert_text(order, pos_blocks, card, dates)

    recipients = _recipients()
    if not recipients:
        logger.error("supply_svetofor: OWNER_CHAT_ID не задан")
        return False
    kb = keyboard(order_id)
    delivered = 0
    for chat_id in recipients:
        try:
            await app.bot.send_message(chat_id=chat_id, text=text,
                                       parse_mode="Markdown", reply_markup=kb)
            delivered += 1
        except Exception as e:
            # Markdown мог сломаться на «грязном» имени товара/поставщика из МС —
            # пробуем добить обычным текстом (без parse_mode), чтобы алерт всё же дошёл.
            logger.warning(f"supply_svetofor: Markdown send to {chat_id} failed ({e}); retry plain")
            try:
                await app.bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)
                delivered += 1
            except Exception as e2:
                logger.error(f"supply_svetofor: plain send to {chat_id} failed: {e2}")
    # Возвращаем True ТОЛЬКО если реально доставили хотя бы одному получателю —
    # иначе poll_job не пишет лог и повторит на следующем тике (self-heal против
    # транзиентных сбоев Telegram/сети).
    if delivered:
        logger.info(f"supply_svetofor: алерт по {order_name} доставлен ({delivered}/{len(recipients)})")
        return True
    logger.error(f"supply_svetofor: алерт по {order_name} НЕ доставлен — повтор на следующем тике")
    return False


async def poll_job(app, db=None):
    """PTB JobQueue-джоба: заказы поставщику в «На согласовании» → алерт Виктору.

    Дедуп по (order_id, sum_hash). Первый прогон (лог пуст) — только сид без рассылки.
    """
    try:
        orders = await _fetch_on_approval()
    except Exception as e:
        logger.error(f"supply_svetofor poll: ошибка МС: {e}")
        return

    # Только товарные заказы — логистику/услуги пропускаем полностью.
    orders = [o for o in orders if _has_goods(o)]

    # Защита от «потопа» на ПЕРВОМ прогоне после деплоя: помечаем текущий бэклог
    # заказов как виденный (без рассылки) и ставим маркер seeded. Дальше маркер
    # не даёт этой ветке срабатывать при временно пустом логе.
    if not _is_seeded():
        for o in orders:
            _log_upsert(o.get("id"), o.get("name"), SUPPLY_STATE_ON_APPROVAL,
                        round((o.get("sum", 0) or 0) / 100))
        _mark_seeded()
        logger.info(f"supply_svetofor: первичный сид — {len(orders)} заказов, рассылки нет")
        return

    log = _log_get_all()
    sent = 0
    for o in orders:
        oid = o.get("id")
        sum_hash = round((o.get("sum", 0) or 0) / 100)
        prev = log.get(oid)
        # Уже алертили по этому заказу с той же суммой — пропускаем.
        if prev is not None and prev.get("sum_hash") == sum_hash:
            continue
        try:
            ok = await check_supply_approval_needed(o, app)
            if ok:
                _log_upsert(oid, o.get("name"), SUPPLY_STATE_ON_APPROVAL, sum_hash)
                sent += 1
        except Exception as e:
            logger.error(f"supply_svetofor: {o.get('name')} ошибка: {e}", exc_info=True)
    if sent:
        logger.info(f"supply_svetofor poll: отправлено {sent}")
