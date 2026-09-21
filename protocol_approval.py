"""Кнопки «Опубликовать» / «Доработка» под недельными протоколами.

С 21.09.2026 протоколы производства (пн 08:30) и логистики (пн 08:35) крон
f2b-publisher шлёт не в группы, а собственнику в личку (`src/approval.py` в
репо f2b-publisher). Здесь — обработка кнопок под этим PDF:

- «Опубликовать»: тот же файл по file_id с той же подписью уходит в группы
  протокола, неделя отмечается в `publishing.publish_log` (тип
  `claims_protocol` / `logistics_protocol`). Отметку ставим ДО отправки и через
  `ON CONFLICT DO NOTHING` — двойное нажатие второй раз в группы не шлёт.
- «Доработка»: в группы ничего не уходит, кнопки снимаются. Исправленную
  версию собирает рабочая сессия с агентом и отправляет сама.

План: plans/2026-09-21-протоколы-на-утверждение-собственнику.md (репо «второй мозг»).
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone

from telegram.ext import CallbackQueryHandler

logger = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))

# Группы протоколов. Те же id, что у f2b-publisher (`claims_protocol.PRODUCTION_CHAT_ID`,
# `logistics_protocol.LOGI_CHAT_ID` / `SKLAD_CHAT_ID`): сменилась группа — правятся оба репо.
GROUPS = {
    "claims": ((-1002460651890, "F2B ПРОИЗВОДСТВО"),),
    "logistics": ((-5154587608, "ЛОГИСТИКА Ф2Б"), (-4750423130, "F2B СКЛАД")),
}
LOG_TYPE = {"claims": "claims_protocol", "logistics": "logistics_protocol"}

_DATA = re.compile(r"^proto:(pub|fix):(claims|logistics):(\d{4})-(\d{2})$")


def _owner_id() -> int:
    return int(os.environ["OWNER_CHAT_ID"])


def _make_handler(db):
    async def handle(update, context):
        q = update.callback_query
        if not q.from_user or q.from_user.id != _owner_id():
            await q.answer("⛔ Только для руководителя.", show_alert=True)
            return
        m = _DATA.match(q.data or "")
        msg = q.message
        if not m or not msg:
            await q.answer("Не разобрал кнопку")
            return
        action, kind, year, week = m.group(1), m.group(2), int(m.group(3)), int(m.group(4))
        caption = msg.caption or ""

        if action == "fix":
            await q.edit_message_caption(
                caption=caption + "\n\nНа доработке – в группы не отправлено.",
                reply_markup=None)
            await q.answer("Не публикуем")
            return

        if not msg.document:
            await q.answer("В сообщении нет файла протокола", show_alert=True)
            return
        try:
            fresh = db._fetchone(
                "INSERT INTO publishing.publish_log (type, iso_year, iso_week) "
                "VALUES (%s, %s, %s) ON CONFLICT (type, iso_year, iso_week) DO NOTHING "
                "RETURNING type",
                (LOG_TYPE[kind], year, week),
            )
        except Exception as exc:
            logger.exception("протокол: не записал отметку публикации")
            await q.answer(f"Не записал отметку: {exc}", show_alert=True)
            return
        if not fresh:
            await q.edit_message_reply_markup(reply_markup=None)
            await q.answer("Этот протокол уже опубликован", show_alert=True)
            return

        sent = []
        for chat_id, title in GROUPS[kind]:
            try:
                await context.bot.send_document(chat_id, msg.document.file_id, caption=caption)
                sent.append(title)
            except Exception as exc:
                logger.exception("протокол %s: не ушёл в %s", kind, title)
                if not sent:
                    # Никуда не ушло — снимаем отметку, чтобы можно было нажать ещё раз.
                    db._execute(
                        "DELETE FROM publishing.publish_log "
                        "WHERE type = %s AND iso_year = %s AND iso_week = %s",
                        (LOG_TYPE[kind], year, week),
                    )
                    await q.answer(f"Не отправил в «{title}»: {exc}", show_alert=True)
                    return
                await q.edit_message_caption(
                    caption=caption + f"\n\nОпубликовано в «{'», «'.join(sent)}». "
                                      f"В «{title}» не ушло: {exc}",
                    reply_markup=None)
                await q.answer("Отправлено не во все группы", show_alert=True)
                return

        at = datetime.now(MSK).strftime("%H:%M")
        await q.edit_message_caption(
            caption=caption + f"\n\nОпубликовано в {at} – «{'», «'.join(sent)}».",
            reply_markup=None)
        await q.answer("Опубликовано")

    return handle


def register(app, db):
    app.add_handler(CallbackQueryHandler(_make_handler(db), pattern=r"^proto:(pub|fix):"))
    logger.info("protocol_approval: зарегистрирован")
