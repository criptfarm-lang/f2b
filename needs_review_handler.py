"""F2B — UI подтверждения needs-review лотов через бота «Эф».

План 2026-05-28-автопарсер-pdf-прайсов.md, Под-фазы 2 + 2.x.

Двухэтажный UI:
1. Auto-slug поставщики (сгенерированные из caption, не в SUPPLIER_HINT_MAP)
   — групповая операция: 1 клик подтверждает всех лотов под этим slug'ом сразу.
2. Лоты в needs-review для confirmed-поставщиков — по одному.
   Inline-правка species/processing/state прямо в карточке (Под-фаза 2.x):
   текущее значение помечено ✓, top-2 наиболее частых из confirmed-лотов
   + кнопка «Другое...» открывает picker со всеми ENUM-значениями.

Per-zone routing временно отключён (для bench Виктор видит всё). Включится через
env NR_OWNER_ZONE=belyakova|kristina, когда добавим chat_id остальных в БД.
"""

from __future__ import annotations

import html
import logging
import re
from typing import Optional


def _h(s) -> str:
    """HTML-экранирование. None → пустая строка."""
    return html.escape(str(s)) if s is not None else ""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from market_intel_processor import SUPPLIER_HINT_MAP

logger = logging.getLogger(__name__)

# Множество КАНОНИЧЕСКИХ slug'ов поставщиков (значения SUPPLIER_HINT_MAP).
# Если supplier_id лота не в этом множестве — это auto-slug, требует подтверждения.
_KNOWN_SLUGS = set(SUPPLIER_HINT_MAP.values())


# ─── Транслитерация ru→lat для предложения slug ─────────────────────────────
_RU_TO_LAT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "yo",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}


def _translit_slug(text: str) -> str:
    """ооо-альфа-марин → ooo-alfa-marin. Урезаем стоп-префиксы (ооо, ип)."""
    s = (text or "").lower().strip()
    # Удаляем юридические префиксы.
    for prefix in ("ооо-", "ип-", "ао-", "оао-", "зао-", "пао-"):
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    # Транслитерируем.
    out = []
    for ch in s:
        out.append(_RU_TO_LAT.get(ch, ch))
    s = "".join(out)
    s = re.sub(r"[^a-z0-9\-]", "", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:40] or "supplier"


# ─── Запросы к БД ───────────────────────────────────────────────────────────


def _get_known_slugs(db) -> set[str]:
    """KNOWN = статическая карта SUPPLIER_HINT_MAP + все slug'и из procurement.suppliers.

    Поставщик считается подтверждённым (НЕ показываем как auto-slug в /needs_review),
    если он уже есть в таблице procurement.suppliers — это либо bootstrap из known map,
    либо результат предыдущего подтверждения через эту команду.
    """
    db_slugs = {r["slug"] for r in db._fetchall("SELECT slug FROM procurement.suppliers")}
    return _KNOWN_SLUGS | db_slugs


def _get_auto_slug_suppliers(db) -> list[dict]:
    """Поставщики с needs-review лотами, чей slug ещё НЕ подтверждён."""
    known = _get_known_slugs(db)
    rows = db._fetchall(
        """SELECT supplier_id, COUNT(*) AS lots_n,
                  ARRAY_AGG(DISTINCT species::text) AS species_kinds
           FROM procurement.lots
           WHERE confidence = 'needs-review'
             AND superseded_by_lot_id IS NULL
           GROUP BY supplier_id
           ORDER BY lots_n DESC""",
    )
    return [r for r in rows if r["supplier_id"] not in known]


def _get_needs_review_lots(db, limit: int = 1) -> list[dict]:
    """Лоты needs-review для УЖЕ подтверждённых поставщиков (slug в procurement.suppliers + SUPPLIER_HINT_MAP)."""
    known = _get_known_slugs(db)
    if not known:
        return []
    placeholders = ",".join(["%s"] * len(known))
    rows = db._fetchall(
        f"""SELECT lot_id, supplier_id, species::text AS species,
                   subspecies, region::text AS region, weight_class,
                   processing::text AS processing, state::text AS state,
                   product_form::text AS product_form, price_rub_kg,
                   raw_text, notes, conditions
            FROM procurement.lots
            WHERE confidence = 'needs-review'
              AND superseded_by_lot_id IS NULL
              AND supplier_id IN ({placeholders})
            ORDER BY created_at ASC
            LIMIT %s""",
        list(known) + [limit],
    )
    return rows


def _confirm_supplier(db, auto_slug: str, new_slug: str) -> int:
    """Создаёт supplier (если нет) + переносит все лоты на новый slug + confirmed."""
    db._execute(
        """INSERT INTO procurement.suppliers (slug, name)
           VALUES (%s, %s) ON CONFLICT (slug) DO NOTHING""",
        (new_slug, new_slug),
    )
    # перенос лотов + auto-confirm. Лоты, у которых species='прочее' или processing='unspecified',
    # оставляем в needs-review (там реальные проблемы нормализации, не slug).
    # CASE → TEXT, нужен explicit cast в procurement.lot_confidence_enum.
    cur = db._execute(
        """UPDATE procurement.lots
           SET supplier_id = %s,
               confidence = (CASE
                 WHEN species = 'прочее' OR processing = 'unspecified' THEN 'needs-review'
                 ELSE 'confirmed'
               END)::procurement.lot_confidence_enum
           WHERE supplier_id = %s""",
        (new_slug, auto_slug),
    )
    return cur.rowcount or 0


def _drop_supplier_lots(db, auto_slug: str) -> int:
    """Отбрасывает все лоты с этим auto-slug (не поставщик, мусор)."""
    cur = db._execute(
        "DELETE FROM procurement.lots WHERE supplier_id = %s", (auto_slug,)
    )
    return cur.rowcount or 0


def _confirm_lot(db, lot_id: str) -> bool:
    cur = db._execute(
        "UPDATE procurement.lots SET confidence = 'confirmed'::procurement.lot_confidence_enum "
        "WHERE lot_id = %s",
        (lot_id,),
    )
    return (cur.rowcount or 0) > 0


def _drop_lot(db, lot_id: str) -> bool:
    cur = db._execute(
        "DELETE FROM procurement.lots WHERE lot_id = %s", (lot_id,)
    )
    return (cur.rowcount or 0) > 0


# ─── ENUM-правка в карточке (Под-фаза 2.x) ──────────────────────────────────

# Маппинг короткого кода поля → (имя колонки в БД, имя ENUM-типа).
# Коды короткие, чтобы влезать в callback_data (64 байта Telegram-лимит).
_FIELD_MAP = {
    "sp": ("species", "procurement.species_enum"),
    "pr": ("processing", "procurement.processing_enum"),
    "st": ("state", "procurement.state_enum"),
}

_ENUM_VALUES_CACHE: dict[str, list[str]] = {}


def _get_enum_values(db, enum_type: str) -> list[str]:
    """Все значения ENUM в порядке определения. Кэшируем in-memory."""
    if enum_type in _ENUM_VALUES_CACHE:
        return _ENUM_VALUES_CACHE[enum_type]
    schema, _, name = enum_type.partition(".")
    rows = db._fetchall(
        """SELECT e.enumlabel
           FROM pg_type t
           JOIN pg_enum e ON e.enumtypid = t.oid
           JOIN pg_namespace n ON n.oid = t.typnamespace
           WHERE n.nspname = %s AND t.typname = %s
           ORDER BY e.enumsortorder""",
        (schema, name),
    )
    values = [r["enumlabel"] for r in rows]
    _ENUM_VALUES_CACHE[enum_type] = values
    return values


def _get_top_values(db, column: str, current_value: str, limit: int = 2) -> list[str]:
    """Top-N наиболее частых значений колонки среди confirmed-лотов, исключая текущее."""
    rows = db._fetchall(
        f"""SELECT {column}::text AS v, COUNT(*) AS n
            FROM procurement.lots
            WHERE confidence = 'confirmed'
              AND superseded_by_lot_id IS NULL
              AND {column}::text <> %s
            GROUP BY {column}
            ORDER BY n DESC
            LIMIT %s""",
        (current_value, limit),
    )
    return [r["v"] for r in rows]


def _resolve_lot_by_short_id(db, short_id: str) -> Optional[dict]:
    """Восстанавливает полный лот по первым 8 chars UUID. При коллизии — первый."""
    rows = db._fetchall(
        """SELECT lot_id, supplier_id, species::text AS species,
                  subspecies, region::text AS region, weight_class,
                  processing::text AS processing, state::text AS state,
                  product_form::text AS product_form, price_rub_kg,
                  raw_text, notes, conditions, confidence::text AS confidence
           FROM procurement.lots
           WHERE lot_id::text LIKE %s
           LIMIT 1""",
        (short_id + "%",),
    )
    return rows[0] if rows else None


def _set_lot_field(db, lot_id: str, column: str, enum_type: str, value: str) -> bool:
    """UPDATE одной ENUM-колонки лота. Возвращает True если строка обновлена."""
    cur = db._execute(
        f"UPDATE procurement.lots SET {column} = %s::{enum_type} WHERE lot_id = %s",
        (value, lot_id),
    )
    return (cur.rowcount or 0) > 0


# ─── Карточки UI ────────────────────────────────────────────────────────────


def _format_supplier_card(supplier: dict) -> tuple[str, InlineKeyboardMarkup]:
    auto = supplier["supplier_id"]
    suggested = _translit_slug(auto)
    species = supplier.get("species_kinds") or []
    species_str = ", ".join(sorted(s for s in species if s)[:6]) or "—"
    text = (
        f"🆕 <b>Новый поставщик в канале</b>\n\n"
        f"<code>{_h(auto)}</code>\n"
        f"лотов: <b>{supplier['lots_n']}</b>\n"
        f"species: {_h(species_str)}\n\n"
        f"Предлагаемый slug: <code>{_h(suggested)}</code>"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ Подтвердить как {suggested}",
                              callback_data=f"nr_sok|{auto}")],
        [InlineKeyboardButton("❌ Шум, отбросить лоты",
                              callback_data=f"nr_sdrop|{auto}")],
    ])
    return text, kb


def _format_lot_card(lot: dict, db) -> tuple[str, InlineKeyboardMarkup]:
    parts = [
        f"<b>{_h(lot['supplier_id'])}</b>",
        f"{_h(lot['species'])}"
        + (f"/{_h(lot['subspecies'])}" if lot.get("subspecies") else "")
        + (f" • {_h(lot['region'])}" if lot.get("region") else ""),
        f"{_h(lot['processing'])} / {_h(lot['state'])} / {_h(lot.get('product_form') or 'сырьё')}",
        f"вес: <b>{_h(lot['weight_class'])}</b>  цена: <b>{_h(lot['price_rub_kg'])} ₽/кг</b>",
    ]
    if lot.get("conditions"):
        parts.append(f"<i>условия:</i> {_h(lot['conditions'])}")
    if lot.get("notes"):
        parts.append(f"<i>notes:</i> {_h(lot['notes'][:80])}")
    if lot.get("raw_text"):
        parts.append(f"\n<code>{_h(lot['raw_text'][:120])}</code>")
    text = "\n".join(parts)

    lot_id = str(lot["lot_id"])
    sid = lot_id[:8]
    rows = []
    # Три ряда ENUM-правки. Каждый ряд: [текущее ✓] [top1] [top2] [Другое...]
    for code, (column, enum_type) in _FIELD_MAP.items():
        current = lot[column] or "—"
        tops = _get_top_values(db, column, current, limit=2)
        row = [InlineKeyboardButton(f"✓ {current}", callback_data="nr_nop")]
        for v in tops:
            row.append(InlineKeyboardButton(v, callback_data=f"nr_s|{sid}|{code}|{v}"))
        row.append(InlineKeyboardButton("Другое…", callback_data=f"nr_m|{sid}|{code}"))
        rows.append(row)
    rows.append([
        InlineKeyboardButton("✅ Подтвердить", callback_data=f"nr_lok|{lot_id}"),
        InlineKeyboardButton("❌ Отбросить", callback_data=f"nr_ldrop|{lot_id}"),
    ])
    return text, InlineKeyboardMarkup(rows)


def _format_enum_picker(db, lot: dict, code: str) -> tuple[str, InlineKeyboardMarkup]:
    """Карточка выбора значения ENUM из полного списка."""
    column, enum_type = _FIELD_MAP[code]
    current = lot[column] or "—"
    lot_id = str(lot["lot_id"])
    sid = lot_id[:8]
    label = {"sp": "species", "pr": "processing", "st": "state"}[code]
    text = (
        f"<b>{_h(lot['supplier_id'])}</b>\n"
        f"Выбор <code>{label}</code> (сейчас: <b>{_h(current)}</b>)\n\n"
        f"<code>{_h((lot.get('raw_text') or '')[:120])}</code>"
    )
    values = _get_enum_values(db, enum_type)
    # 3 кнопки в ряд, текущее значение исключаем
    buttons = [v for v in values if v != current]
    rows = []
    row = []
    for v in buttons:
        row.append(InlineKeyboardButton(v, callback_data=f"nr_s|{sid}|{code}|{v}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("← Назад", callback_data=f"nr_b|{sid}")])
    return text, InlineKeyboardMarkup(rows)


# ─── Главные точки входа ────────────────────────────────────────────────────


async def cmd_needs_review(update: Update, context: ContextTypes.DEFAULT_TYPE, db) -> None:
    """Точка входа: команда /needs_review. Шлёт первую карточку или 'всё подтверждено'."""
    await _send_next(update.message.reply_text, db, is_callback=False)


async def cb_needs_review(update: Update, context: ContextTypes.DEFAULT_TYPE, db) -> None:
    """CallbackQueryHandler для всех nr_* кнопок."""
    query = update.callback_query
    data = query.data or ""

    # nr_nop — уже выбранное значение ENUM, no-op
    if data == "nr_nop":
        await query.answer("Уже выбрано")
        return

    await query.answer()
    parts = data.split("|")
    action = parts[0]
    advance_to_next = True  # для большинства действий — сразу следующая карточка

    try:
        if action == "nr_sok":
            auto_slug = parts[1]
            # Idempotency: если auto_slug уже в procurement.suppliers (повторный
            # клик по старой карточке в истории чата), не выполняем UPDATE.
            existing = db._fetchone(
                "SELECT 1 FROM procurement.suppliers WHERE slug = %s", (auto_slug,)
            )
            if existing:
                await query.edit_message_text(
                    f"ℹ Поставщик <code>{_h(auto_slug)}</code> уже подтверждён ранее.",
                    parse_mode="HTML",
                )
            else:
                new_slug = _translit_slug(auto_slug)
                moved = _confirm_supplier(db, auto_slug, new_slug)
                await query.edit_message_text(
                    f"✅ Поставщик подтверждён: <code>{_h(auto_slug)}</code> → <code>{_h(new_slug)}</code>. "
                    f"Перенесено {moved} лотов.",
                    parse_mode="HTML",
                )
        elif action == "nr_sdrop":
            auto_slug = parts[1]
            n = _drop_supplier_lots(db, auto_slug)
            if n == 0:
                await query.edit_message_text(
                    f"ℹ Лотов от <code>{_h(auto_slug)}</code> уже нет (обработан ранее).",
                    parse_mode="HTML",
                )
            else:
                await query.edit_message_text(
                    f"❌ Отброшено {n} лотов от <code>{_h(auto_slug)}</code> (шум).",
                    parse_mode="HTML",
                )
        elif action == "nr_lok":
            lot_id = parts[1]
            if _confirm_lot(db, lot_id):
                await query.edit_message_text("✅ Лот подтверждён.")
            else:
                await query.edit_message_text("ℹ Лот уже обработан или удалён.")
        elif action == "nr_ldrop":
            lot_id = parts[1]
            if _drop_lot(db, lot_id):
                await query.edit_message_text("❌ Лот отброшен.")
            else:
                await query.edit_message_text("ℹ Лот уже обработан или удалён.")
        elif action == "nr_s":
            # nr_s|<sid>|<code>|<value> — UPDATE одной колонки + refresh карточки
            sid, code, value = parts[1], parts[2], parts[3]
            if code not in _FIELD_MAP:
                await query.edit_message_text(f"⚠ Неизвестное поле: {code}")
                return
            lot = _resolve_lot_by_short_id(db, sid)
            if not lot or lot.get("confidence") != "needs-review":
                await query.edit_message_text("ℹ Лот уже обработан или удалён.")
                return
            column, enum_type = _FIELD_MAP[code]
            _set_lot_field(db, str(lot["lot_id"]), column, enum_type, value)
            # refresh: подтягиваем обновлённый лот и редактируем карточку in-place
            lot = _resolve_lot_by_short_id(db, sid)
            text, kb = _format_lot_card(lot, db)
            await query.edit_message_text(text, reply_markup=kb, parse_mode="HTML")
            advance_to_next = False
        elif action == "nr_m":
            # nr_m|<sid>|<code> — открыть picker полного списка
            sid, code = parts[1], parts[2]
            if code not in _FIELD_MAP:
                await query.edit_message_text(f"⚠ Неизвестное поле: {code}")
                return
            lot = _resolve_lot_by_short_id(db, sid)
            if not lot or lot.get("confidence") != "needs-review":
                await query.edit_message_text("ℹ Лот уже обработан или удалён.")
                return
            text, kb = _format_enum_picker(db, lot, code)
            await query.edit_message_text(text, reply_markup=kb, parse_mode="HTML")
            advance_to_next = False
        elif action == "nr_b":
            # nr_b|<sid> — назад к карточке лота из picker
            sid = parts[1]
            lot = _resolve_lot_by_short_id(db, sid)
            if not lot or lot.get("confidence") != "needs-review":
                await query.edit_message_text("ℹ Лот уже обработан или удалён.")
                return
            text, kb = _format_lot_card(lot, db)
            await query.edit_message_text(text, reply_markup=kb, parse_mode="HTML")
            advance_to_next = False
        else:
            await query.edit_message_text(f"⚠ Неизвестное действие: {action}")
            return
    except Exception as e:
        logger.exception("needs_review callback failed")
        await query.edit_message_text(f"⚠ Ошибка: {e!r}")
        return

    if advance_to_next:
        await _send_next(query.message.reply_text, db, is_callback=True)


async def _send_next(send_func, db, is_callback: bool) -> None:
    """Шлёт следующую карточку: сначала auto-slug поставщик, потом лот."""
    suppliers = _get_auto_slug_suppliers(db)
    if suppliers:
        text, kb = _format_supplier_card(suppliers[0])
        await send_func(text, reply_markup=kb, parse_mode="HTML")
        return

    lots = _get_needs_review_lots(db, limit=1)
    if lots:
        text, kb = _format_lot_card(lots[0], db)
        await send_func(text, reply_markup=kb, parse_mode="HTML")
        return

    # Пусто — всё подтверждено
    await send_func("✅ Все needs-review лоты разобраны. /search покажет всё в confirmed.")
