"""
notifier.py — Рассылка клиентам при согласовании заказа в МойСклад.

Этот файл полностью независим от bot.py.
Любые правки в bot.py НЕ затрагивают рассылку.

Используется в bot.py только через один импорт:
    from notifier import check_order_agreed
"""

import logging
import os

logger = logging.getLogger(__name__)

QUIZ_BASE_URL = os.getenv("QUIZ_BASE_URL", "")

MS_STATE_AGREED = "005f3651-9a9a-11f0-0a80-03a900027474"
WAZZUP_API_URL = "https://api.wazzup24.com/v3/message"
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"
# UUID дополнительных полей контрагентов в МойСклад
ATTR_TELEGRAM = "15052610-34d7-11f1-0a80-1489000ec44a"
ATTR_WHATSAPP = "1505270f-34d7-11f1-0a80-1489000ec44b"
ATTR_MAX      = "1505236e-34d7-11f1-0a80-1489000ec449"

# Каналы Wazzup
CHANNEL_TELEGRAM = "ddd24a95-9304-4098-a320-3e47fcd1020a"
CHANNEL_WHATSAPP = "e180aa1d-dc48-4d0a-bec3-fc0afc53cf03"
CHANNEL_MAX      = "1d5bc70a-7ca6-4895-8d1f-9690cf448214"


MONTHS_RU = [
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря"
]


async def _is_company_excluded(company_name: str) -> bool:
    """Проверяет находится ли компания в списке исключений квиза.
    При недоступном API — возвращает False (квиз отправляем, не блокируем)."""
    import aiohttp
    if not QUIZ_BASE_URL:
        return False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{QUIZ_BASE_URL}/api/check-exclusion",
                params={"company_name": company_name},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    return data.get("excluded", False)
    except Exception as e:
        logger.warning(f"_is_company_excluded: API недоступен ({e}), квиз отправляем")
    return False


async def _load_order_positions(order_id: str, headers: dict) -> str:
    """Загружает позиции заказа и возвращает отформатированный текст."""
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/customerorder/{order_id}/positions",
                headers=headers,
                params={"expand": "assortment", "limit": 100}
            ) as r:
                if r.status != 200:
                    return ""
                data = await r.json()
                lines = []
                for pos in data.get("rows", []):
                    name = pos.get("assortment", {}).get("name", "?")
                    qty = int(pos.get("quantity", 0))
                    price = (pos.get("price", 0) or 0) / 100
                    total = qty * price
                    lines.append(f"  • {name} — {qty} кг × {price:,.0f} руб/кг = {total:,.0f} руб.")
                return "\n".join(lines)
    except Exception as e:
        logger.warning(f"_load_order_positions: {e}")
        return ""


async def _get_agent_tags(agent_id: str, headers: dict) -> list:
    """Загружает теги контрагента из МойСклад."""
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/counterparty/{agent_id}",
                headers=headers
            ) as r:
                if r.status == 200:
                    cp = await r.json()
                    return cp.get("tags", [])
    except Exception as e:
        logger.warning(f"_get_agent_tags: {e}")
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Синхронизация Wazzup-полей в amoCRM-контакт ПЕРЕД рассылкой.
#
# Зачем: при отправке через Wazzup `POST /v3/message` Wazzup-amoCRM-интеграция
# параллельно пытается найти amoCRM-контакт с тем же chatId/username в кастомных
# полях TelegramId_WZ / MaxId_WZ / WhatsappUsername_WZ. Если не находит —
# создаёт новый контакт + новую сделку в воронке Привлечение. Получаем
# фантом-сделки на каждую рассылку существующим клиентам.
#
# Решение: перед `POST /v3/message` найти amoCRM-контакт по phone клиента
# (из МСК) и обновить ему соответствующее Wazzup-поле тем же значением,
# которое мы шлём через Wazzup. Тогда Wazzup-интеграция найдёт контакт
# и положит беседу к нему — без создания фантома.
#
# Включается флагом AMOCRM_SYNC_ENABLED=1 (по умолчанию выключен).
# Любая ошибка → логируется WARN и не блокирует рассылку.
# ─────────────────────────────────────────────────────────────────────────────

# IDs кастомных полей контакта в amoCRM F2B (`victorfishtobiz`)
AMO_FIELD_MAX_ID = 2244321          # MaxId_WZ
AMO_FIELD_TG_ID = 2224427           # TelegramId_WZ
AMO_FIELD_TG_USERNAME = 2224425     # TelegramUsername_WZ
AMO_FIELD_WA_USERNAME = 2245217     # WhatsappUsername_WZ


async def _get_agent_phone(agent_id: str, headers: dict) -> str | None:
    """Читает phone контрагента из МойСклад. Возвращает digits-only или None."""
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/counterparty/{agent_id}",
                headers=headers
            ) as r:
                if r.status != 200:
                    return None
                cp = await r.json()
        phone = cp.get("phone") or ""
        digits = "".join(ch for ch in phone if ch.isdigit())
        if len(digits) == 10:
            digits = "7" + digits
        elif len(digits) == 11 and digits.startswith("8"):
            digits = "7" + digits[1:]
        return digits or None
    except Exception as e:
        logger.warning(f"_get_agent_phone: {e}")
        return None


async def _sync_amocrm_contact(agent_phone: str | None, contact_data: dict) -> int | None:
    """Найти amoCRM-контакт по phone и обновить Wazzup-поле под канал отправки.

    Возвращает amocrm contact_id если контакт найден (даже если не пришлось
    обновлять поля), иначе None. Не падает на ошибках — только логирует.
    """
    if not agent_phone:
        return None
    if os.getenv("AMOCRM_SYNC_ENABLED", "0").lower() not in ("1", "true", "yes"):
        return None

    try:
        from amocrm import find_contact_by_phone, AMO_BASE_URL, get_headers
    except Exception as e:
        logger.warning(f"_sync_amocrm_contact: amocrm import failed: {e}")
        return None

    try:
        contact = await find_contact_by_phone(agent_phone)
    except Exception as e:
        logger.warning(f"_sync_amocrm_contact: search by phone {agent_phone} failed: {e}")
        return None
    if not contact:
        logger.info(f"_sync_amocrm_contact: amoCRM-контакт не найден по phone={agent_phone}")
        return None

    contact_id = contact["id"]
    chat_type = contact_data.get("chat_type", "")
    chat_id = contact_data.get("chat_id", "")

    # Определяем целевое поле и значение
    if chat_type == "max":
        field_id, value = AMO_FIELD_MAX_ID, chat_id
    elif chat_type == "telegram":
        if chat_id.startswith("@"):
            field_id, value = AMO_FIELD_TG_USERNAME, chat_id
        else:
            field_id, value = AMO_FIELD_TG_ID, chat_id
    elif chat_type == "whatsapp":
        field_id, value = AMO_FIELD_WA_USERNAME, chat_id
    else:
        return contact_id

    # Не пишем если уже совпадает
    existing_cf = next(
        (cf for cf in (contact.get("custom_fields_values") or [])
         if cf.get("field_id") == field_id),
        None
    )
    if existing_cf:
        existing_val = existing_cf.get("values", [{}])[0].get("value") if existing_cf.get("values") else None
        if existing_val == value:
            logger.info(
                f"_sync_amocrm_contact: contact={contact_id} field={field_id} уже = {value}, skip"
            )
            return contact_id

    # PATCH — добавить/обновить кастомное поле
    import aiohttp
    payload = {
        "custom_fields_values": [
            {"field_id": field_id, "values": [{"value": value}]}
        ]
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.patch(
                f"{AMO_BASE_URL}/contacts/{contact_id}",
                headers=get_headers(),
                json=payload,
            ) as r:
                if r.status == 200:
                    logger.info(
                        f"_sync_amocrm_contact: ✅ contact={contact_id} field={field_id} = {value}"
                    )
                else:
                    body = await r.text()
                    logger.warning(
                        f"_sync_amocrm_contact: PATCH {r.status} contact={contact_id}: {body[:200]}"
                    )
    except Exception as e:
        logger.warning(f"_sync_amocrm_contact: PATCH error: {e}")
    return contact_id


async def _get_contact_from_ms(agent_id: str, headers: dict) -> dict | None:
    """Читает контакт для рассылки из дополнительных полей контрагента в МойСклад.
    Возвращает {chat_id, chat_type, channel_id} или None."""
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/counterparty/{agent_id}",
                headers=headers,
                params={"expand": "attributes"}
            ) as r:
                if r.status != 200:
                    return None
                cp = await r.json()

        attributes = cp.get("attributes", [])
        telegram_id = None
        whatsapp_id = None
        max_id = None

        for attr in attributes:
            attr_href = attr.get("meta", {}).get("href", "")
            value = attr.get("value", "")
            if not value:
                continue
            if ATTR_TELEGRAM in attr_href:
                telegram_id = str(value)
            elif ATTR_WHATSAPP in attr_href:
                whatsapp_id = str(value)
            elif ATTR_MAX in attr_href:
                max_id = str(value)

        # Приоритет: Telegram → Max → WhatsApp
        if telegram_id:
            return {"chat_id": telegram_id, "chat_type": "telegram", "channel_id": CHANNEL_TELEGRAM}
        if max_id:
            return {"chat_id": max_id, "chat_type": "max", "channel_id": CHANNEL_MAX}
        if whatsapp_id:
            return {"chat_id": whatsapp_id, "chat_type": "whatsapp", "channel_id": CHANNEL_WHATSAPP}

    except Exception as e:
        logger.warning(f"_get_contact_from_ms: {e}")
    return None


async def check_order_agreed(order_href: str, bot, db):
    """При смене статуса заказа на Согласовано — отправляем клиенту в мессенджер.

    Аргументы:
        order_href — ссылка на заказ из МойСклад webhook
        bot        — telegram bot instance (не используется напрямую, для совместимости)
        db         — экземпляр Database
    """
    try:
        import aiohttp
        from moysklad import get_headers

        headers = get_headers()

        # ── 1. Загружаем заказ ────────────────────────────────────────────
        async with aiohttp.ClientSession() as session:
            async with session.get(
                order_href,
                headers=headers,
                params={"expand": "agent,state,owner"}
            ) as r:
                if r.status != 200:
                    return
                order = await r.json()

        # ── 2. Проверяем статус "Согласовано" ─────────────────────────────
        state_id = order.get("state", {}).get("meta", {}).get("href", "").split("/")[-1]
        if state_id != MS_STATE_AGREED:
            return

        order_id = order_href.split("/")[-1].split("?")[0]

        # ── 3. Быстрая проверка дедупа — оптимизация, чтобы не дёргать API ─
        # Финальный атомарный claim делается прямо перед POST в Wazzup ниже.
        if db.is_agreed_notified(order_id):
            logger.info(f"notifier: заказ {order_id} уже уведомлялся, пропускаем")
            return

        # ── 4. Данные заказа ──────────────────────────────────────────────
        order_name = order.get("name", "")
        order_sum  = (order.get("sum", 0) or 0) / 100
        agent      = order.get("agent", {})
        agent_name = agent.get("name", "")
        agent_id   = agent.get("meta", {}).get("href", "").split("/")[-1]

        # Пропускаем розничного покупателя
        if agent_name.lower().strip() == "розничный покупатель":
            db.save_agreed_notification(order_id)
            return

        logger.info(f"notifier: заказ {order_name} клиент={agent_name}")

        # ── 5. Проверка исключений — исключённые не получают ничего ──────
        excluded = await _is_company_excluded(agent_name) if QUIZ_BASE_URL else False
        if excluded:
            logger.info(f"notifier: {agent_name} в исключениях, рассылка не отправляется")
            db.save_agreed_notification(order_id)
            return

        # ── 6. Ищем контакт в МойСклад ──────────────────────────────────
        contact = await _get_contact_from_ms(agent_id, headers)
        if contact:
            logger.info(f"notifier: контакт {agent_name} найден в МойСклад → {contact['chat_type']}")
        else:
            logger.info(f"notifier: контакт {agent_name} не найден в МойСклад, пропускаем")
            # НЕ сохраняем флаг — заполни поля Telegram/Max/WhatsApp в карточке контрагента
            return

        # ── 7. Позиции заказа ─────────────────────────────────────────────
        positions_text = await _load_order_positions(order_id, headers)

        # ── 8. Дата отгрузки ──────────────────────────────────────────────
        delivery_raw = order.get("deliveryPlannedMoment", "")
        delivery_fmt = ""
        if delivery_raw:
            try:
                from datetime import date as _d
                d = _d.fromisoformat(delivery_raw[:10])
                delivery_fmt = f"{d.day} {MONTHS_RU[d.month - 1]}"
            except Exception:
                delivery_fmt = delivery_raw[:10]

        # ── 9. Формируем сообщение ────────────────────────────────────────
        msg = f"📋 Пожалуйста, проверьте заказ {order_name}\n\n"
        if positions_text:
            msg += f"📦 Состав:\n{positions_text}\n\n"
        msg += f"💰 Итого: {order_sum:,.0f} руб.\n"
        if delivery_fmt:
            msg += f"📅 Плановая дата отгрузки: {delivery_fmt}\n"

        # ── 10. Квиз (всем кроме исключённых, исключённые уже отфильтрованы) ──
        if QUIZ_BASE_URL:
            import urllib.parse, hashlib, aiohttp as _ah
            # Создаём короткую ссылку через квиз-сервер
            short_code = hashlib.md5(f"{order_id}{agent_id}".encode()).hexdigest()[:8]
            quiz_url = f"{QUIZ_BASE_URL}/q/{short_code}"
            try:
                async with _ah.ClientSession() as _qs:
                    await _qs.post(
                        f"{QUIZ_BASE_URL}/api/short-link",
                        params={
                            "order_id": order_id,
                            "client_id": agent_id,
                            "amount": int(order_sum),
                            "company_name": agent_name,
                        },
                        timeout=_ah.ClientTimeout(total=5)
                    )
            except Exception as _e:
                # Если не удалось создать короткую ссылку — используем длинную
                quiz_url = (
                    f"{QUIZ_BASE_URL}"
                    f"/?order={order_id}"
                    f"&client_id={agent_id}"
                    f"&amount={int(order_sum)}"
                    f"&company={urllib.parse.quote(agent_name)}"
                )
                logger.warning(f"notifier: short-link failed ({_e}), используем длинную")
            msg += (
                f"\n\n🎣 Дарим 300 кг филе форели! Играйте в викторину, копите FISHки и обменивайте на филе!\n"
                f"{quiz_url}"
            )
            logger.info(f"notifier: квиз добавлен для {agent_name}")

        # ── 10.5. Синхронизация amoCRM-контакта (защита от дублей сделок) ─
        # Заполняем у существующего amoCRM-контакта Wazzup-поле тем же значением,
        # которое отправляем через Wazzup. Это позволяет Wazzup-amoCRM-интеграции
        # сматчить исходящее с существующим контактом и не создавать новую сделку.
        # Управляется флагом AMOCRM_SYNC_ENABLED (по умолчанию выключено).
        try:
            agent_phone = await _get_agent_phone(agent_id, headers)
            await _sync_amocrm_contact(agent_phone, contact)
        except Exception as _e:
            logger.warning(f"notifier: amocrm sync failed (non-blocking): {_e}")

        # ── 11. Атомарный claim прямо перед отправкой ─────────────────────
        # INSERT ... ON CONFLICT DO NOTHING RETURNING — один из параллельных
        # webhook'ов получит True и отправит, остальные получат False и выйдут.
        # Если флаг стоит — НЕ откатываем даже при ошибке Wazzup: требование
        # «строго один POST на заказ». Для повторной попытки — /reset_agreed.
        if not db.try_claim_agreed_notification(order_id):
            logger.info(f"notifier: заказ {order_id} уже отправлен параллельным вызовом, пропускаем")
            return

        # ── 12. Отправляем через Wazzup ───────────────────────────────────
        api_key = os.getenv("WAZZUP_API_KEY", "")
        # Определяем chatId или username
        chat_id_value = contact["chat_id"]
        wazzup_payload = {
            "channelId": contact["channel_id"],
            "chatType":  contact["chat_type"],
            "text":      msg,
        }
        if chat_id_value.startswith("@"):
            # Username — передаём без @
            wazzup_payload["username"] = chat_id_value.lstrip("@")
        else:
            wazzup_payload["chatId"] = chat_id_value

        async with aiohttp.ClientSession() as session:
            async with session.post(
                WAZZUP_API_URL,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=wazzup_payload,
            ) as r:
                if r.status in (200, 201):
                    logger.info(f"notifier: ✅ отправлено {agent_name} → {contact['chat_type']}")
                    # Логируем рассылку в квиз-сервис для статистики
                    logger.info(f"notifier: QUIZ_BASE_URL={QUIZ_BASE_URL!r}")
                    if QUIZ_BASE_URL:
                        try:
                            async with aiohttp.ClientSession() as _s:
                                await _s.post(
                                    f"{QUIZ_BASE_URL}/api/log-mailing",
                                    json={"order_id": order_id, "client_id": agent_id, "company_name": agent_name},
                                    timeout=aiohttp.ClientTimeout(total=5)
                                )
                        except Exception as _e:
                            logger.warning(f"notifier: log-mailing failed ({_e})")
                else:
                    body = await r.text()
                    logger.error(f"notifier: ❌ ошибка {r.status} для {agent_name}: {body[:200]}")

    except Exception as e:
        logger.error(f"notifier.check_order_agreed: {e}", exc_info=True)


# ============================================================================
# Объединённый алерт «На согласовании» / «ЗА ЛИМИТОМ»
# План: 2026-05-21-объединённый-алерт-на-согласование.md, Фаза 3.
# Светофор: Лимит → Просрочка → ДДС → Цена → Дата оплаты → Сайт → Контакты.
# ============================================================================

MS_STATE_ON_APPROVAL = "005f34bf-9a9a-11f0-0a80-03a900027473"
MS_STATE_OVER_LIMIT  = "462ee41b-b554-11f0-0a80-15a000036d2c"


def _parse_approvers_chat_ids() -> list[int]:
    """
    APPROVERS_CHAT_IDS — env, список chat_id через запятую.
    Дефолт: только OWNER_CHAT_ID (если задан).
    """
    raw = os.getenv("APPROVERS_CHAT_IDS", "").strip()
    if not raw:
        owner = os.getenv("OWNER_CHAT_ID", "").strip()
        return [int(owner)] if owner.isdigit() else []
    out = []
    for x in raw.split(","):
        x = x.strip()
        if x.lstrip("-").isdigit():
            out.append(int(x))
    return out


def _icon(color: str) -> str:
    return {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(color, "⚪")


def _fmt_money(amount: float) -> str:
    return f"{amount:,.0f}".replace(",", " ")


def _build_approval_text(
    order_name: str, order_sum: float, state_name: str,
    client_name: str, manager_name: str,
    credit: dict, overdue: dict, cashflow: dict, price: dict,
    site: dict, contacts: dict,
) -> str:
    """
    Шаблон алерта. Порядок строк (по убыванию важности):
      Лимит → Просрочка → ДДС → Сайт → Контакты → Цена (внизу — длинный список).
    all-green → одна сводная строка; иначе — 6 строк светофора.
    """
    from datetime import datetime, timezone, timedelta
    now_msk = datetime.now(timezone(timedelta(hours=3)))
    sent_at = now_msk.strftime("%H:%M")

    colors = [credit["color"], overdue["color"], cashflow["color"],
              price["color"], site["color"], contacts["color"]]
    all_green = all(c == "green" for c in colors)

    header = (
        f"🔔 *{client_name}* · {_fmt_money(order_sum)} ₽\n"
        f"Заказ {order_name} · _{state_name}_\n"
        f"👔 {manager_name} · 🕐 {sent_at}\n"
    )

    if all_green:
        limit_pct = 0
        if credit.get("limit", 0) > 0:
            limit_pct = int(credit["effective_debt"] / credit["limit"] * 100)
        body = (
            f"\n🟢 Все 6 проверок ОК "
            f"(лимит {limit_pct}% · ДДС {cashflow.get('n_days', 0)}д · "
            f"долг {_fmt_money(credit.get('current_debt', 0))} _на {sent_at}_ · "
            f"сайт · контакты · цена)\n"
        )
        return header + body

    lines = ["\n"]

    # 1. Лимит
    # snapshot-маркер: долг = balance на момент webhook'а. К моменту, когда
    # согласующий откроет UI «Взаиморасчёты», цифра уже может вырасти из-за
    # новых отгрузок — это не баг, см. plans/2026-05-21, Фаза 6.
    if credit["color"] == "yellow":
        lines.append(f"🟡 *Лимит:* не задан — заполните в карточке МС")
    else:
        lines.append(
            f"{_icon(credit['color'])} *Лимит:* долг {_fmt_money(credit['current_debt'])} "
            f"+ заказ {_fmt_money(credit['order_sum'])} "
            f"= {_fmt_money(credit['effective_debt'])} ₽ "
            f"из {_fmt_money(credit['limit'])} ₽ "
            f"_(на {sent_at})_"
        )

    # 2. Просрочка
    if overdue["color"] == "red":
        lines.append(
            f"🔴 *Просрочка:* {overdue.get('days', 0)} дн "
            f"/ {_fmt_money(overdue.get('debt', 0))} ₽"
        )
    else:
        lines.append(f"🟢 *Просрочка:* нет")

    # 3. ДДС
    explain = cashflow.get("explain", "")
    lines.append(f"{_icon(cashflow['color'])} *ДДС:* {explain}")

    # 4. Сайт
    if site["color"] == "green":
        lines.append(f"🟢 *Сайт:* {site['raw_value'][:60]}")
    else:
        lines.append(f"🔴 *Сайт:* {site.get('raw_value', '') or 'не заполнен'}")

    # 5. Контакты
    mx = contacts.get("max", "")
    tg = contacts.get("telegram", "")
    if contacts["color"] == "green":
        parts_c = []
        if contacts.get("max_valid"):
            parts_c.append(f"Max {mx}")
        if contacts.get("tg_valid"):
            parts_c.append(f"TG {tg}")
        lines.append(f"🟢 *Контакты:* " + " · ".join(parts_c))
    else:
        lines.append(f"🔴 *Контакты:* Max={mx or '—'} / TG={tg or '—'}")

    # 6. Цена — внизу, длинный список не мешает читать остальные строки
    items = price.get("items", [])
    if items:
        n = len(items)
        lines.append(f"🔴 *Цена ниже минимальной* — {n} {'позиция' if n == 1 else 'позиций'}:")
        for it in items[:5]:
            name = (it.get("name") or "")[:48]
            lines.append(
                f"   • {name}: {_fmt_money(it['order_price'])} ₽ "
                f"при минимуме {_fmt_money(it['min_price'])} ₽ "
                f"(−{it['diff_pct']:.1f}%, −{_fmt_money(it['diff_rub'])} ₽)"
            )
        if n > 5:
            lines.append(f"   • … и ещё {n - 5} {'позиция' if n - 5 == 1 else 'позиций'}")
    else:
        lines.append("🟢 *Цена:* в норме")

    return header + "\n".join(lines)


async def check_approval_needed(order_href: str, bot, db):
    """
    Объединённый алерт согласующим (OWNER + Маланчук, env APPROVERS_CHAT_IDS) при
    попадании заказа в статус «На согласовании» или «ЗА ЛИМИТОМ».
    Дедуп — атомарный INSERT с UNIQUE(order_id, sum_hash) в pending_approval_alerts.
    """
    try:
        import asyncio
        import aiohttp
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        from moysklad import (
            get_headers, MS_BASE,
            load_counterparty_attrs,
            compute_credit_color, compute_overdue_color, compute_cashflow_color,
            compute_price_color,
            compute_site_color, compute_contacts_color,
        )

        # 1. Загружаем заказ с атрибутами и расширениями
        async with aiohttp.ClientSession() as session:
            async with session.get(
                order_href, headers=get_headers(),
                params={"expand": "agent,state,owner,attributes"}
            ) as r:
                if r.status != 200:
                    logger.warning(f"check_approval_needed: GET order {r.status}")
                    return
                order = await r.json()

        state = order.get("state") or {}
        state_id = state.get("meta", {}).get("href", "").split("/")[-1]
        if state_id not in (MS_STATE_ON_APPROVAL, MS_STATE_OVER_LIMIT):
            return

        state_name = state.get("name", "?")
        order_id = order_href.split("/")[-1].split("?")[0]
        order_name = order.get("name", "")
        order_sum = (order.get("sum", 0) or 0) / 100
        agent = order.get("agent") or {}
        agent_name = agent.get("name", "")
        agent_id = agent.get("id") or agent.get("meta", {}).get("href", "").split("/")[-1]
        owner = order.get("owner") or {}
        manager_name = owner.get("name", "не указан")

        if not agent_id:
            logger.warning(f"check_approval_needed: agent_id пустой для {order_name}")
            return

        logger.info(
            f"check_approval_needed: {order_name} ({state_name}) клиент={agent_name} "
            f"менеджер={manager_name} sum={order_sum:,.0f}"
        )

        # 2. Атрибуты counterparty (1 GET, передаётся в 3 sync helper'а)
        cp_attrs = await load_counterparty_attrs(agent_id)

        # 3. Параллельно: cashflow, overdue, price (+ timeout 20с с fallback)
        try:
            cashflow, overdue, price = await asyncio.wait_for(
                asyncio.gather(
                    compute_cashflow_color(agent_id),
                    compute_overdue_color(agent_id),
                    compute_price_color(order_href),
                    return_exceptions=True,
                ),
                timeout=20.0,
            )
        except asyncio.TimeoutError:
            logger.warning(f"check_approval_needed: timeout для {order_name}")
            cashflow = overdue = price = None

        # Нормализуем результаты (Exception/None → yellow с пометкой)
        def _norm(v, default):
            if isinstance(v, Exception) or v is None:
                logger.warning(f"check_approval_needed: helper failed → {v!r}")
                return default
            return v

        cashflow = _norm(cashflow, {"color": "yellow", "n_days": None, "explain": "?",
                                    "payments_sum": 0, "current_debt": 0.0})
        overdue = _norm(overdue, {"color": "yellow", "days": 0, "debt": 0})
        price = _norm(price, {"color": "yellow", "items": []})

        # 4. Sync helper'ы.
        # current_debt берём из /report (cashflow), а НЕ из overdue.debt — overdue
        # отдаёт только просроченную часть, а для лимита нужна вся текущая дебиторка.
        current_debt = cashflow.get("current_debt", 0) or 0
        credit = compute_credit_color(cp_attrs, current_debt=current_debt, order_sum=order_sum)
        site = compute_site_color(cp_attrs)
        contacts = compute_contacts_color(cp_attrs)

        # 5. Сборка текста + colors_json для confirmation flow
        colors_json = {
            "credit": credit["color"],
            "overdue": overdue["color"],
            "cashflow": cashflow["color"],
            "price": price["color"],
            "site": site["color"],
            "contacts": contacts["color"],
        }
        alert_text = _build_approval_text(
            order_name=order_name, order_sum=order_sum, state_name=state_name,
            client_name=agent_name, manager_name=manager_name,
            credit=credit, overdue=overdue, cashflow=cashflow, price=price,
            site=site, contacts=contacts,
        )

        # 6. Дедуп: sum_hash = округлённая сумма в ₽
        sum_hash = round(order_sum)

        # Резолвим manager_user_id (для callback `appr_comment`)
        mgr_user_id = 0
        if manager_name:
            for part in manager_name.split():
                cid = db.get_manager_chat_id(part)
                if cid:
                    mgr_user_id = cid
                    break

        # 7. Атомарная вставка с дедупом
        alert_id = db.try_insert_approval_alert(
            order_id=order_id, sum_hash=sum_hash,
            alert_text=alert_text, colors_json=colors_json,
            order_name=order_name, client_name=agent_name,
            manager_name=manager_name, manager_user_id=mgr_user_id,
        )
        if alert_id is None:
            logger.info(f"check_approval_needed: дедуп — {order_name} (sum_hash={sum_hash}) уже алертили")
            return

        logger.info(f"check_approval_needed: alert_id={alert_id} для {order_name}")

        # 8. Клавиатура + fan-out
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Согласовано", callback_data=f"appr_ok|{alert_id}"),
            InlineKeyboardButton("💬 Комментарий", callback_data=f"appr_comment|{alert_id}"),
        ]])

        approvers = _parse_approvers_chat_ids()
        if not approvers:
            logger.error("check_approval_needed: APPROVERS_CHAT_IDS / OWNER_CHAT_ID не задан")
            return

        for chat_id in approvers:
            try:
                await bot.send_message(
                    chat_id=chat_id, text=alert_text,
                    parse_mode="Markdown", reply_markup=keyboard
                )
            except Exception as e:
                logger.error(f"check_approval_needed: send to {chat_id} failed: {e}")

    except Exception as e:
        logger.error(f"notifier.check_approval_needed: {e}", exc_info=True)
