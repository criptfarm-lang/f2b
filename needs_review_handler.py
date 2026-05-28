"""F2B — UI подтверждения needs-review лотов через бота «Эф».

План 2026-05-28-автопарсер-pdf-прайсов.md, Под-фаза 2 (минимальная итерация).

Двухэтажный UI:
1. Auto-slug поставщики (сгенерированные из caption, не в SUPPLIER_HINT_MAP)
   — групповая операция: 1 клик подтверждает всех лотов под этим slug'ом сразу.
2. Лоты в needs-review для confirmed-поставщиков — по одному.

В минимальной версии каждая карточка имеет 2 кнопки: Подтвердить / Отбросить.
Inline-правка species/processing/state — следующая итерация (2.x).

Per-zone routing временно отключён (для bench Виктор видит всё). Включится через
env NR_OWNER_ZONE=belyakova|kristina, когда добавим chat_id остальных в БД.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

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


# ─── Карточки UI ────────────────────────────────────────────────────────────


def _format_supplier_card(supplier: dict) -> tuple[str, InlineKeyboardMarkup]:
    auto = supplier["supplier_id"]
    suggested = _translit_slug(auto)
    species = supplier.get("species_kinds") or []
    species_str = ", ".join(sorted(s for s in species if s)[:6]) or "—"
    text = (
        f"🆕 *Новый поставщик в канале*\n\n"
        f"`{auto}`\n"
        f"лотов: *{supplier['lots_n']}*\n"
        f"species: {species_str}\n\n"
        f"Предлагаемый slug: `{suggested}`"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ Подтвердить как {suggested}",
                              callback_data=f"nr_sok|{auto}")],
        [InlineKeyboardButton("❌ Шум, отбросить лоты",
                              callback_data=f"nr_sdrop|{auto}")],
    ])
    return text, kb


def _format_lot_card(lot: dict) -> tuple[str, InlineKeyboardMarkup]:
    parts = [
        f"*{lot['supplier_id']}*",
        f"{lot['species']}"
        + (f"/{lot['subspecies']}" if lot.get("subspecies") else "")
        + (f" • {lot['region']}" if lot.get("region") else ""),
        f"{lot['processing']} / {lot['state']} / {lot.get('product_form') or 'сырьё'}",
        f"вес: *{lot['weight_class']}*  цена: *{lot['price_rub_kg']} ₽/кг*",
    ]
    if lot.get("conditions"):
        parts.append(f"_условия:_ {lot['conditions']}")
    if lot.get("notes"):
        parts.append(f"_notes:_ {lot['notes'][:80]}")
    if lot.get("raw_text"):
        parts.append(f"\n`{lot['raw_text'][:120]}`")
    text = "\n".join(parts)

    lot_id = lot["lot_id"]
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Подтвердить", callback_data=f"nr_lok|{lot_id}"),
        InlineKeyboardButton("❌ Отбросить", callback_data=f"nr_ldrop|{lot_id}"),
    ]])
    return text, kb


# ─── Главные точки входа ────────────────────────────────────────────────────


async def cmd_needs_review(update: Update, context: ContextTypes.DEFAULT_TYPE, db) -> None:
    """Точка входа: команда /needs_review. Шлёт первую карточку или 'всё подтверждено'."""
    await _send_next(update.message.reply_text, db, is_callback=False)


async def cb_needs_review(update: Update, context: ContextTypes.DEFAULT_TYPE, db) -> None:
    """CallbackQueryHandler для всех nr_* кнопок."""
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    action, _, payload = data.partition("|")

    try:
        if action == "nr_sok":
            auto_slug = payload
            new_slug = _translit_slug(auto_slug)
            moved = _confirm_supplier(db, auto_slug, new_slug)
            await query.edit_message_text(
                f"✅ Поставщик подтверждён: `{auto_slug}` → `{new_slug}`. "
                f"Перенесено {moved} лотов.",
                parse_mode="Markdown",
            )
        elif action == "nr_sdrop":
            auto_slug = payload
            n = _drop_supplier_lots(db, auto_slug)
            await query.edit_message_text(
                f"❌ Отброшено {n} лотов от `{auto_slug}` (шум).",
                parse_mode="Markdown",
            )
        elif action == "nr_lok":
            lot_id = payload
            if _confirm_lot(db, lot_id):
                await query.edit_message_text("✅ Лот подтверждён.")
            else:
                await query.edit_message_text("⚠ Лот не найден (уже обработан?).")
        elif action == "nr_ldrop":
            lot_id = payload
            if _drop_lot(db, lot_id):
                await query.edit_message_text("❌ Лот отброшен.")
            else:
                await query.edit_message_text("⚠ Лот не найден.")
        else:
            await query.edit_message_text(f"⚠ Неизвестное действие: {action}")
            return
    except Exception as e:
        logger.exception("needs_review callback failed")
        await query.edit_message_text(f"⚠ Ошибка: {e!r}")
        return

    # Сразу следующая карточка
    await _send_next(query.message.reply_text, db, is_callback=True)


async def _send_next(send_func, db, is_callback: bool) -> None:
    """Шлёт следующую карточку: сначала auto-slug поставщик, потом лот."""
    suppliers = _get_auto_slug_suppliers(db)
    if suppliers:
        text, kb = _format_supplier_card(suppliers[0])
        await send_func(text, reply_markup=kb, parse_mode="Markdown")
        return

    lots = _get_needs_review_lots(db, limit=1)
    if lots:
        text, kb = _format_lot_card(lots[0])
        await send_func(text, reply_markup=kb, parse_mode="Markdown")
        return

    # Пусто — всё подтверждено
    await send_func("✅ Все needs-review лоты разобраны. /search покажет всё в confirmed.")
