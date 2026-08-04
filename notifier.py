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
# Статус «НЕ СОГЛАСОВАН» — заказ отклонён/не прошёл согласование, менеджеру
# нужно его доработать. Сверено с МС 2026-07-16 (GET /entity/customerorder/metadata).
# План: 2026-07-16-алерт-заказ-не-согласован.md
MS_STATE_NOT_AGREED = "f7e3f71d-6b0b-11f1-0a80-1a5900237f4c"
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

# ─── Алерт «крупный заказ готовой продукции» ────────────────────────────────
# План: plans/2026-07-15-алерт-крупный-заказ-готовая-продукция.md
# Группа товаров собственного производства в МойСклад (по pathName-префиксу).
BULK_GROUP_PREFIX = "ГОТОВАЯ ПРОДУКЦИЯ"
# Пороги по одной позиции: в кг ≥ 300, в штуках ≥ 200.
BULK_THRESHOLD_KG = 300
BULK_THRESHOLD_PCS = 200


def _bulk_alert_chat_id() -> int | None:
    """Кому шлём алерт о крупном заказе.
    BULK_ORDER_ALERT_CHAT_ID (env) → иначе PARTNER_CHAT_ID (Маланчук)."""
    raw = os.getenv("BULK_ORDER_ALERT_CHAT_ID", "").strip() or \
          os.getenv("PARTNER_CHAT_ID", "").strip()
    return int(raw) if raw.lstrip("-").isdigit() else None


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


async def _get_contacts_from_ms(agent_id: str, headers: dict) -> list[dict]:
    """Читает ВСЕ каналы для рассылки из доп.полей контрагента в МойСклад.

    Возвращает список {chat_id, chat_type, channel_id} в порядке приоритета
    Telegram → Max → WhatsApp. Пустой список — ни один канал не заполнен.
    Список нужен для фолбэка: если отправка по первому каналу падает
    (напр. Telegram username не найден), пробуем следующий."""
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/counterparty/{agent_id}",
                headers=headers,
                params={"expand": "attributes"}
            ) as r:
                if r.status != 200:
                    return []
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
        contacts = []
        if telegram_id:
            contacts.append({"chat_id": telegram_id, "chat_type": "telegram", "channel_id": CHANNEL_TELEGRAM})
        if max_id:
            contacts.append({"chat_id": max_id, "chat_type": "max", "channel_id": CHANNEL_MAX})
        if whatsapp_id:
            contacts.append({"chat_id": whatsapp_id, "chat_type": "whatsapp", "channel_id": CHANNEL_WHATSAPP})
        return contacts

    except Exception as e:
        logger.warning(f"_get_contacts_from_ms: {e}")
    return []


async def _get_contact_from_ms(agent_id: str, headers: dict) -> dict | None:
    """Первый по приоритету канал контрагента (совместимость со старыми вызовами)."""
    contacts = await _get_contacts_from_ms(agent_id, headers)
    return contacts[0] if contacts else None


async def _load_order(order_href: str, headers: dict) -> dict | None:
    """Загружает заказ из МойСклад с раскрытием agent/state/owner. None при ошибке."""
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                order_href,
                headers=headers,
                params={"expand": "agent,state,owner"}
            ) as r:
                if r.status != 200:
                    return None
                return await r.json()
    except Exception as e:
        logger.warning(f"_load_order: {e}")
        return None


async def _send_fishki_mailing(order: dict, db) -> tuple[bool, str]:
    """Строит и шлёт FISHки-рассылку клиенту по уже загруженному заказу.

    Не проверяет статус — это ответственность вызывающего (webhook-путь проверяет
    «Согласовано», ручной cron-путь шлёт по любому статусу ≥ «Согласовано»).

    Возвращает (sent_to_client, reason):
        (True,  "sent")            — отправлено в Wazzup, запись в agreed_notifications
        (False, "retail")          — Розничный покупатель, флаг сохранён, рассылка не нужна
        (False, "excluded")        — в exclusions, флаг сохранён, рассылка не нужна
        (False, "no_contact")      — нет Telegram/WhatsApp/Max в карточке МС
        (False, "already_claimed") — кто-то уже отправил параллельно
        (False, f"wazzup_err:...") — POST в Wazzup упал; флаг ОСТАЁТСЯ (один POST на заказ)
        (False, "load_failed")     — не удалось дотянуть данные заказа
    """
    import aiohttp
    from moysklad import get_headers

    headers = get_headers()
    order_id   = order.get("id", "")
    order_name = order.get("name", "")
    order_sum  = (order.get("sum", 0) or 0) / 100
    agent      = order.get("agent", {})
    agent_name = agent.get("name", "")
    agent_id   = agent.get("meta", {}).get("href", "").split("/")[-1]

    if not order_id or not agent_id:
        return (False, "load_failed")

    # Розничный покупатель — флаг ставим, рассылку не делаем
    if agent_name.lower().strip() == "розничный покупатель":
        db.save_agreed_notification(order_id)
        return (False, "retail")

    logger.info(f"notifier: заказ {order_name} клиент={agent_name}")

    # Исключения — флаг ставим, рассылку не делаем
    excluded = await _is_company_excluded(agent_name) if QUIZ_BASE_URL else False
    if excluded:
        logger.info(f"notifier: {agent_name} в исключениях, рассылка не отправляется")
        db.save_agreed_notification(order_id)
        return (False, "excluded")

    # Контакты в МойСклад (все каналы по приоритету, для фолбэка при отказе)
    contacts = await _get_contacts_from_ms(agent_id, headers)
    if contacts:
        logger.info(
            f"notifier: контакт {agent_name} найден в МойСклад → "
            f"{', '.join(c['chat_type'] for c in contacts)}"
        )
    else:
        logger.info(f"notifier: контакт {agent_name} не найден в МойСклад, пропускаем")
        # НЕ сохраняем флаг — заполни поля Telegram/Max/WhatsApp в карточке контрагента
        return (False, "no_contact")

    # Позиции заказа
    positions_text = await _load_order_positions(order_id, headers)

    # Дата отгрузки
    delivery_raw = order.get("deliveryPlannedMoment", "")
    delivery_fmt = ""
    if delivery_raw:
        try:
            from datetime import date as _d
            d = _d.fromisoformat(delivery_raw[:10])
            delivery_fmt = f"{d.day} {MONTHS_RU[d.month - 1]}"
        except Exception:
            delivery_fmt = delivery_raw[:10]

    # Формируем сообщение
    msg = f"📋 Пожалуйста, проверьте заказ {order_name}\n\n"
    if positions_text:
        msg += f"📦 Состав:\n{positions_text}\n\n"
    msg += f"💰 Итого: {order_sum:,.0f} руб.\n"
    if delivery_fmt:
        msg += f"📅 Плановая дата отгрузки: {delivery_fmt}\n"

    # Квиз
    if QUIZ_BASE_URL:
        import urllib.parse, hashlib, aiohttp as _ah
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
            quiz_url = (
                f"{QUIZ_BASE_URL}"
                f"/?order={order_id}"
                f"&client_id={agent_id}"
                f"&amount={int(order_sum)}"
                f"&company={urllib.parse.quote(agent_name)}"
            )
            logger.warning(f"notifier: short-link failed ({_e}), используем длинную")
        msg += (
            f"\n\nКое-что приготовили для вас в благодарность за заказ:\n"
            f"{quiz_url}"
        )
        logger.info(f"notifier: квиз добавлен для {agent_name}")

    # Синхронизация amoCRM-контакта (защита от дублей сделок)
    try:
        agent_phone = await _get_agent_phone(agent_id, headers)
        await _sync_amocrm_contact(agent_phone, contacts[0])
    except Exception as _e:
        logger.warning(f"notifier: amocrm sync failed (non-blocking): {_e}")

    # Атомарный claim прямо перед отправкой.
    # INSERT ... ON CONFLICT DO NOTHING RETURNING — победитель шлёт, остальные выходят.
    # При полном провале (все каналы отвергли) claim ОТКАТЫВАЕМ, чтобы sweep дошлёт.
    if not db.try_claim_agreed_notification(order_id):
        logger.info(f"notifier: заказ {order_id} уже отправлен параллельным вызовом, пропускаем")
        return (False, "already_claimed")

    # POST в Wazzup с фолбэком по каналам: Telegram → Max → WhatsApp.
    # Частый кейс — CHANNEL_TGAPI_CONTACT_NOT_FOUND_BY_USERNAME: Wazzup не может
    # инициировать Telegram-диалог по @username, если клиент туда не писал.
    # Тогда пробуем следующий заполненный канал (Max/WhatsApp).
    # Сетевой сбой (Wazzup недоступен, таймаут) — такой же провал канала, как и
    # отказ 4xx: ловим внутри цикла, иначе исключение улетает наружу и claim
    # остаётся навсегда → sweep не дошлёт (кейс ООО «ЛСК» 03.08.2026,
    # Cannot connect to host api.wazzup24.com). Откат claim — в finally, чтобы
    # сработал при любом исходе, кроме успешной отправки.
    api_key = os.getenv("WAZZUP_API_KEY", "")
    last_err = "no_channels"
    sent = False
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            for contact in contacts:
                chat_id_value = contact["chat_id"]
                wazzup_payload = {
                    "channelId": contact["channel_id"],
                    "chatType":  contact["chat_type"],
                    "text":      msg,
                }
                if chat_id_value.startswith("@"):
                    wazzup_payload["username"] = chat_id_value.lstrip("@")
                else:
                    wazzup_payload["chatId"] = chat_id_value

                try:
                    async with session.post(
                        WAZZUP_API_URL,
                        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                        json=wazzup_payload,
                    ) as r:
                        if r.status in (200, 201):
                            sent = True
                            db.clear_fishki_failure(order_id)
                            logger.info(f"notifier: ✅ отправлено {agent_name} → {contact['chat_type']}")
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
                            return (True, "sent")
                        else:
                            body = await r.text()
                            last_err = f"wazzup_err:{r.status}:{body[:200]}"
                            logger.warning(
                                f"notifier: канал {contact['chat_type']} отверг {agent_name} "
                                f"({r.status}): {body[:200]}"
                            )
                except Exception as _net:
                    last_err = f"net_err:{type(_net).__name__}:{str(_net)[:200]}"
                    logger.warning(
                        f"notifier: канал {contact['chat_type']} недоступен для {agent_name}: {_net}"
                    )
    except Exception as _outer:
        last_err = f"net_err:{type(_outer).__name__}:{str(_outer)[:200]}"
        logger.warning(f"notifier: отправка {agent_name} прервана: {_outer}")
    finally:
        # Все каналы провалились — откатываем claim, чтобы sweep-крон дошлёт позже,
        # и считаем попытку: на второй неудаче sweep предупредит собственника.
        # План: plans/2026-08-04-алерт-рассылка-фишки-не-доставлена.md
        if not sent:
            db.release_agreed_notification(order_id)
            try:
                db.record_fishki_failure(order_id, order_name, agent_name, last_err)
            except Exception as _db_e:
                logger.warning(f"notifier: record_fishki_failure failed ({_db_e})")
            logger.error(
                f"notifier: ❌ все каналы провалились для {agent_name}, claim откачен: {last_err}"
            )

    return (False, last_err)


async def check_order_agreed(order_href: str, bot, db):
    """При смене статуса заказа на Согласовано — отправляем клиенту в мессенджер.

    Аргументы:
        order_href — ссылка на заказ из МойСклад webhook
        bot        — telegram bot instance (не используется напрямую, для совместимости)
        db         — экземпляр Database
    """
    try:
        from moysklad import get_headers
        headers = get_headers()

        order = await _load_order(order_href, headers)
        if not order:
            return

        # Триггер только на статус «Согласован»
        state_id = order.get("state", {}).get("meta", {}).get("href", "").split("/")[-1]
        if state_id != MS_STATE_AGREED:
            return

        order_id = order.get("id") or order_href.split("/")[-1].split("?")[0]

        # Быстрая проверка дедупа — оптимизация. Финальный атомарный claim внутри _send_fishki_mailing.
        if db.is_agreed_notified(order_id):
            logger.info(f"notifier: заказ {order_id} уже уведомлялся, пропускаем")
            return

        await _send_fishki_mailing(order, db)

    except Exception as e:
        logger.error(f"notifier.check_order_agreed: {e}", exc_info=True)


async def _load_bulk_positions(order_id: str, headers: dict) -> list[dict]:
    """Позиции заказа с раскрытием товара и единицы измерения.

    Возвращает список dict: {name, qty, uom, path} — только те, что нужны
    для проверки порога крупного заказа. uom берётся из assortment.uom.name;
    для вариантов (нет uom на самом variant) — из родительского product.
    """
    import aiohttp
    rows_out: list[dict] = []
    prod_uom_cache: dict[str, str] = {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{MS_BASE}/entity/customerorder/{order_id}/positions",
                headers=headers,
                params={"expand": "assortment.uom,assortment.product.uom", "limit": "100"},
            ) as r:
                if r.status != 200:
                    logger.warning(f"_load_bulk_positions: status {r.status}")
                    return []
                data = await r.json()

            for pos in data.get("rows", []):
                a = pos.get("assortment", {}) or {}
                name = a.get("name", "?")
                path = a.get("pathName") or ""
                qty = pos.get("quantity", 0) or 0

                uom = a.get("uom") or {}
                uom_name = uom.get("name") if isinstance(uom, dict) else None
                # variant → uom родительского product
                if not uom_name:
                    prod = a.get("product") or {}
                    puom = prod.get("uom") if isinstance(prod, dict) else None
                    if isinstance(puom, dict):
                        uom_name = puom.get("name")

                rows_out.append({
                    "name": name, "qty": qty,
                    "uom": (uom_name or "").strip(), "path": path,
                })
    except Exception as e:
        logger.warning(f"_load_bulk_positions: {e}")
        return []
    return rows_out


def _bulk_positions_over_threshold(positions: list[dict]) -> list[dict]:
    """Отбирает позиции из группы готовой продукции, превысившие порог.

    кг: qty ≥ BULK_THRESHOLD_KG; шт: qty ≥ BULK_THRESHOLD_PCS.
    """
    hits = []
    for p in positions:
        path = p.get("path") or ""
        if not (path == BULK_GROUP_PREFIX or path.startswith(BULK_GROUP_PREFIX + "/")):
            continue
        uom = (p.get("uom") or "").lower()
        qty = p.get("qty", 0) or 0
        if uom == "кг" and qty >= BULK_THRESHOLD_KG:
            hits.append(p)
        elif uom == "шт" and qty >= BULK_THRESHOLD_PCS:
            hits.append(p)
    return hits


async def check_bulk_production_order(order_href: str, bot, db):
    """При согласовании заказа с крупной позицией готовой продукции — алерт Маланчуку.

    Триггер: статус «Согласован» + хотя бы одна позиция из группы «ГОТОВАЯ
    ПРОДУКЦИЯ» c qty ≥ 300 кг (или ≥ 200 шт). Дедуп — атомарный claim
    bulk_order_notifications, один заказ = одно уведомление.
    """
    try:
        from moysklad import get_headers
        headers = get_headers()

        order = await _load_order(order_href, headers)
        if not order:
            return

        state_id = order.get("state", {}).get("meta", {}).get("href", "").split("/")[-1]
        if state_id != MS_STATE_AGREED:
            return

        order_id = order.get("id") or order_href.split("/")[-1].split("?")[0]

        positions = await _load_bulk_positions(order_id, headers)
        hits = _bulk_positions_over_threshold(positions)
        if not hits:
            return

        chat_id = _bulk_alert_chat_id()
        if not chat_id:
            logger.warning("check_bulk_production_order: не задан BULK_ORDER_ALERT_CHAT_ID/PARTNER_CHAT_ID")
            return

        # Атомарный дедуп — только первый webhook отправляет.
        if not db.try_claim_bulk_notification(order_id):
            logger.info(f"bulk-alert: заказ {order_id} уже уведомлялся, пропуск")
            return

        order_name = order.get("name", order_id)
        agent_name = order.get("agent", {}).get("name", "—")

        def _fmt_qty(p):
            q = p["qty"]
            q_str = f"{q:g}"
            return f"  • {p['name']} — {q_str} {p['uom']}"

        lines = "\n".join(_fmt_qty(p) for p in hits)
        text = (
            f"🏭 Крупный заказ готовой продукции\n\n"
            f"Заказ: {order_name}\n"
            f"Клиент: {agent_name}\n\n"
            f"Позиции ≥ порога (кг≥{BULK_THRESHOLD_KG} / шт≥{BULK_THRESHOLD_PCS}):\n"
            f"{lines}"
        )

        try:
            await bot.send_message(chat_id=chat_id, text=text)
            logger.info(f"bulk-alert: заказ {order_name} → chat {chat_id}, позиций {len(hits)}")
        except Exception as e:
            logger.error(f"bulk-alert: отправка упала для {order_id}: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"notifier.check_bulk_production_order: {e}", exc_info=True)


async def check_order_not_agreed(order_href: str, bot, db):
    """При переводе заказа в статус «НЕ СОГЛАСОВАН» — пинг ответственному менеджеру.

    Триггер: state == MS_STATE_NOT_AGREED. Получатель — менеджер (owner заказа,
    правило: owner заказа = реальный менеджер). Резолв tg-id как в
    check_approval_needed (PDZ_MANAGER_TG_IDS → get_manager_chat_id). Если не
    резолвится — уходит собственнику (OWNER_CHAT_ID) с пометкой, сигнал не теряем.
    Дедуп — атомарный claim not_agreed_notifications по (order_id, sum_hash):
    повторный алерт только если сумма заказа изменилась (менеджер доработал →
    снова не согласовали). План: 2026-07-16-алерт-заказ-не-согласован.md
    """
    try:
        from moysklad import get_headers, PDZ_MANAGER_TG_IDS
        headers = get_headers()

        order = await _load_order(order_href, headers)
        if not order:
            return

        state_id = order.get("state", {}).get("meta", {}).get("href", "").split("/")[-1]
        if state_id != MS_STATE_NOT_AGREED:
            return

        order_id = order.get("id") or order_href.split("/")[-1].split("?")[0]
        order_name = order.get("name", order_id)
        order_sum = (order.get("sum", 0) or 0) / 100
        agent_name = order.get("agent", {}).get("name", "—")
        manager_name = (order.get("owner") or {}).get("name", "")

        # Резолвим tg-id менеджера (тот же приём, что в check_approval_needed).
        mgr_user_id = 0
        if manager_name:
            for part in manager_name.split():
                key = part.lower().strip(".,").rstrip()
                if key in PDZ_MANAGER_TG_IDS:
                    mgr_user_id = PDZ_MANAGER_TG_IDS[key]
                    break
            if not mgr_user_id:
                for part in manager_name.split():
                    cid = db.get_manager_chat_id(part)
                    if cid:
                        mgr_user_id = cid
                        break

        note = ""
        chat_id = mgr_user_id
        if not chat_id:
            owner_raw = os.getenv("OWNER_CHAT_ID", "").strip()
            chat_id = int(owner_raw) if owner_raw.lstrip("-").isdigit() else 0
            note = f"\n⚠️ Не нашёл TG менеджера «{manager_name or '—'}» — отправлено вам."
            logger.warning(
                f"check_order_not_agreed: не нашёл tg_id для менеджера "
                f"'{manager_name}' по заказу {order_name} → fallback OWNER_CHAT_ID"
            )
        if not chat_id:
            logger.error(f"check_order_not_agreed: некому слать по {order_name} (нет менеджера и OWNER_CHAT_ID)")
            return

        # Атомарный дедуп по (order_id, sum_hash) — повтор только при смене суммы.
        sum_hash = round(order_sum)
        if not db.try_claim_not_agreed_notification(order_id, sum_hash):
            logger.info(f"not-agreed: заказ {order_id} (sum_hash={sum_hash}) уже уведомлялся, пропуск")
            return

        text = (
            f"⚠️ Заказ {order_name} — НЕ СОГЛАСОВАН\n"
            f"🏢 {agent_name} · {_fmt_money(order_sum)} ₽\n"
            f"Требует внимания — проверьте и доработайте."
            f"{note}"
        )

        try:
            await bot.send_message(chat_id=chat_id, text=text)
            logger.info(f"not-agreed: заказ {order_name} → chat {chat_id} (менеджер '{manager_name}')")
        except Exception as e:
            logger.error(f"not-agreed: отправка упала для {order_id}: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"notifier.check_order_not_agreed: {e}", exc_info=True)


async def manual_send_fishki(order_id: str, db) -> tuple[bool, str]:
    """Ручная отправка FISHки-рассылки по order_id (cron-fallback кнопка в TG).

    В отличие от check_order_agreed не проверяет статус заказа — вызывается
    по найденному cron'ом пропуску (state ≥ «Согласован», нет в agreed_notifications).
    """
    try:
        from moysklad import get_headers
        headers = get_headers()
        order_href = f"{MS_BASE}/entity/customerorder/{order_id}"
        order = await _load_order(order_href, headers)
        if not order:
            return (False, "load_failed")

        if db.is_agreed_notified(order_id):
            return (False, "already_claimed")

        return await _send_fishki_mailing(order, db)
    except Exception as e:
        logger.error(f"notifier.manual_send_fishki: {e}", exc_info=True)
        return (False, f"exception:{e}")


# ============================================================================
# Дослыка FISHки-рассылки по пропущенным согласованным заказам.
# План: plans/2026-07-29-дослыка-fishki-по-пропущенным-согласованным.md
#
# Зачем: webhook «Согласован» иногда теряется или обрабатывается уже после
# того, как заказ ушёл дальше по конвейеру («Собирается»/«Отгружен») → живой
# статус ≠ «Согласован» → check_order_agreed выходит, единственная отправка не
# происходит. Sweep ловит такие пропуски по факту «заказ дошёл до Согласован».
# Дедуп по order_id (try_claim_agreed_notification) → строго одна отправка,
# цикл «Согласован → откат → снова Согласован» не задваивается.
# ============================================================================

# Статусы конвейера ≥ «Согласован» (по имени — устойчиво к смене UUID).
# «На согласовании», «НЕ СОГЛАСОВАН», «ЗА ЛИМИТОМ», «Возврат» и черновики — НЕ здесь.
FISHKI_SWEEP_STATES = {
    "Согласован", "Собирается", "Собран", "Документы готовы", "Отгружен",
}


def _order_is_fresh(order: dict, grace_days: int) -> bool:
    """Заказ «свежий» — плановая отгрузка не раньше, чем grace_days назад (или в
    будущем). Защита от «зомби»: у старых заказов updated подскакивает от
    привязки отгрузки/возврата, но слать «проверьте заказ» по заказу
    двухмесячной давности нельзя. Если даты отгрузки нет — берём created.
    """
    from datetime import date
    raw = order.get("deliveryPlannedMoment") or order.get("created") or ""
    if not raw:
        return False
    try:
        d = date.fromisoformat(raw[:10])
    except Exception:
        return False
    from datetime import datetime, timedelta
    return d >= (datetime.now().date() - timedelta(days=grace_days))


async def sweep_missed_fishki(bot, db) -> dict:
    """Находит согласованные заказы без рассылки и дошлёт по каждому один раз.

    Окно — заказы с updated за последние FISHKI_SWEEP_HOURS часов (дефолт 48).
    Кандидат = текущий статус в FISHKI_SWEEP_STATES и order_id нет в
    agreed_notifications. Отправку/фильтры (розница, исключения, нет контакта,
    дедуп) делает manual_send_fishki → _send_fishki_mailing.
    """
    import aiohttp
    from datetime import datetime, timedelta
    from moysklad import get_headers

    hours = int(os.getenv("FISHKI_SWEEP_HOURS", "48") or "48")
    cap   = int(os.getenv("FISHKI_SWEEP_MAX", "40") or "40")
    grace = int(os.getenv("FISHKI_SWEEP_GRACE_DAYS", "4") or "4")
    since = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    headers = get_headers()

    stats = {"checked": 0, "candidates": 0, "sent": 0,
             "retail": 0, "excluded": 0, "no_contact": 0,
             "already": 0, "stale": 0, "errors": 0, "alerted": 0}

    # 1) Тянем заказы, обновлённые за окно (пагинация).
    orders: list[dict] = []
    offset = 0
    try:
        async with aiohttp.ClientSession() as session:
            while True:
                params = {
                    "filter": f"updated>={since}",
                    "expand": "agent,state",
                    "limit": 100,
                    "offset": offset,
                }
                async with session.get(
                    f"{MS_BASE}/entity/customerorder", headers=headers, params=params
                ) as r:
                    if r.status != 200:
                        logger.error(f"fishki-sweep: МС {r.status}: {(await r.text())[:200]}")
                        stats["errors"] += 1
                        break
                    data = await r.json()
                rows = data.get("rows", [])
                orders.extend(rows)
                size = data.get("meta", {}).get("size", 0)
                offset += 100
                if offset >= size or not rows:
                    break
    except Exception as e:
        logger.error(f"fishki-sweep: ошибка загрузки заказов: {e}", exc_info=True)
        stats["errors"] += 1
        return stats

    stats["checked"] = len(orders)

    # 2) Отбираем кандидатов и дошлём.
    for o in orders:
        state_name = (o.get("state") or {}).get("name", "")
        if state_name not in FISHKI_SWEEP_STATES:
            continue
        order_id = o.get("id", "")
        if not order_id or db.is_agreed_notified(order_id):
            continue
        if not _order_is_fresh(o, grace):
            stats["stale"] += 1
            continue
        stats["candidates"] += 1
        if stats["sent"] + stats["already"] >= cap:
            logger.warning(f"fishki-sweep: достигнут кап {cap}, остаток отложен до след. тика")
            break
        sent, reason = await manual_send_fishki(order_id, db)
        if sent:
            stats["sent"] += 1
            logger.info(f"fishki-sweep: ✅ дослано {o.get('name','')} ({o.get('agent',{}).get('name','')})")
        elif reason == "retail":
            stats["retail"] += 1
        elif reason == "excluded":
            stats["excluded"] += 1
        elif reason == "no_contact":
            stats["no_contact"] += 1
        elif reason == "already_claimed":
            stats["already"] += 1
        else:
            stats["errors"] += 1
            logger.info(f"fishki-sweep: {o.get('name','')} не отправлено: {reason}")

    logger.info(
        f"fishki-sweep итог: проверено={stats['checked']} кандидатов={stats['candidates']} "
        f"дослано={stats['sent']} розница={stats['retail']} исключ={stats['excluded']} "
        f"нет_контакта={stats['no_contact']} старых={stats['stale']} ошибок={stats['errors']}"
    )

    # 3) Алерт собственнику по заказам, которые не доходят до клиента.
    stats["alerted"] = await _alert_stuck_fishki(bot, db)
    return stats


FISHKI_ALERT_MIN_ATTEMPTS = 2


async def _alert_stuck_fishki(bot, db) -> int:
    """Предупреждает собственника о заказах, по которым рассылка не уходит.

    Порог — FISHKI_ALERT_MIN_ATTEMPTS попыток: первая обычно вебхук «Согласован»,
    вторая — ближайший тик sweep. Разовый сбой Wazzup лечится дослыкой сам и
    собственника не трогает; два провала подряд — уже повод посмотреть руками.
    Алерт по заказу одноразовый (alerted_at), повторно не тревожим.
    План: plans/2026-08-04-алерт-рассылка-фишки-не-доставлена.md
    """
    try:
        db.purge_old_fishki_failures(7)
        stuck = db.fishki_failures_to_alert(FISHKI_ALERT_MIN_ATTEMPTS)
    except Exception as e:
        logger.warning(f"fishki-sweep: не смог прочитать неудачи рассылки: {e}")
        return 0

    if not stuck:
        return 0

    owner_raw = os.getenv("OWNER_CHAT_ID", "").strip()
    if not owner_raw.lstrip("-").isdigit():
        logger.error("fishki-sweep: OWNER_CHAT_ID не задан, алерт о застрявшей рассылке некому слать")
        return 0

    lines = ["❗️ Рассылка «проверьте заказ + фишки» не доходит до клиента:", ""]
    for row in stuck:
        err = (row.get("last_error") or "")[:160]
        lines.append(
            f"• №{row.get('order_name') or '?'} — {row.get('agent_name') or '?'}\n"
            f"  попыток: {row.get('attempts')}, последняя ошибка: {err}"
        )
    lines.append("")
    lines.append("Дослать после починки: /reset_agreed [номер заказа]")

    try:
        await bot.send_message(chat_id=int(owner_raw), text="\n".join(lines))
    except Exception as e:
        logger.error(f"fishki-sweep: алерт собственнику не ушёл: {e}")
        return 0

    db.mark_fishki_failures_alerted([r["order_id"] for r in stuck])
    logger.info(f"fishki-sweep: алерт собственнику по {len(stuck)} заказам отправлен")
    return len(stuck)


# ============================================================================
# Объединённый алерт «На согласовании» / «ЗА ЛИМИТОМ»
# План: 2026-05-21-объединённый-алерт-на-согласование.md, Фаза 3.
# Светофор: Лимит → Договор → Просрочка → ДДС → Сайт → Контакты → Цена.
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
    credit: dict, contract: dict, overdue: dict, cashflow: dict, price: dict,
    site: dict, contacts: dict, upd_debt: dict = None,
    address: dict = None,
    payment_planned_date: str = "",
    reliability: dict = None,
) -> str:
    """
    Шаблон алерта. Порядок строк (по убыванию важности):
      Лимит → Договор → Просрочка → ДДС → УПД → Сайт → Контакты → Адрес → Цена.
    all-green → одна сводная строка; иначе — строки светофора.
    """
    from datetime import datetime, timezone, timedelta
    now_msk = datetime.now(timezone(timedelta(hours=3)))
    sent_at = now_msk.strftime("%H:%M")

    if upd_debt is None:
        upd_debt = {"color": "green", "count": 0, "sum": 0}
    if address is None:
        address = {"color": "green", "addr": "", "reason": ""}

    payment_color = "green" if payment_planned_date else "red"
    colors = [credit["color"], contract["color"], overdue["color"], cashflow["color"],
              price["color"], site["color"], contacts["color"], payment_color,
              upd_debt["color"], address["color"]]
    # Надёжность контрагента (ЕГРЮЛ+финансы) — отдельная строка. unknown («не
    # проверено») не рушит all_green; yellow/red — рушат, чтобы строка показалась.
    rel_color, rel_line = (None, None)
    if reliability is not None:
        from counterparty_svetofor import format_reliability_line
        rel_color, rel_line = format_reliability_line(reliability)
    all_green = all(c == "green" for c in colors) and rel_color not in ("yellow", "red")

    header = (
        f"🔔 *{client_name}* · {_fmt_money(order_sum)} ₽\n"
        f"Заказ {order_name}\n"
        f"👔 {manager_name} · 🕐 {sent_at}\n"
    )

    if all_green:
        limit_pct = 0
        if credit.get("limit", 0) > 0:
            limit_pct = int(credit["effective_debt"] / credit["limit"] * 100)
        rel_suffix = ""
        if rel_color == "green":
            rel_suffix = " · надёжность ЕГРЮЛ"
        elif rel_color == "unknown":
            rel_suffix = " · надёжность не пров."
        body = (
            f"\n🟢 Все проверки ОК "
            f"(лимит {limit_pct}% · договор · ДДС {cashflow.get('n_days', 0)}д · "
            f"долг {_fmt_money(credit.get('current_debt', 0))} _на {sent_at}_ · "
            f"УПД · сайт · контакты · адрес · цена · оплата {payment_planned_date}{rel_suffix})\n"
        )
        return header + body

    lines = ["\n"]
    if rel_line:
        lines.append(rel_line)

    # 1. Лимит
    # snapshot-маркер: долг = balance на момент webhook'а. К моменту, когда
    # согласующий откроет UI «Взаиморасчёты», цифра уже может вырасти из-за
    # новых отгрузок — это не баг, см. plans/2026-05-21, Фаза 6.
    if credit["color"] == "yellow":
        cd = credit.get("current_debt", 0) or 0
        lines.append(
            f"🟡 *Лимит:* не задан — заполните в карточке МС "
            f"· текущий долг {_fmt_money(cd)} ₽ _(на {sent_at})_"
        )
    else:
        lines.append(
            f"{_icon(credit['color'])} *Лимит:* долг {_fmt_money(credit['current_debt'])} "
            f"+ заказ {_fmt_money(credit['order_sum'])} "
            f"= {_fmt_money(credit['effective_debt'])} ₽ "
            f"из {_fmt_money(credit['limit'])} ₽ "
            f"_(на {sent_at})_"
        )

    # 2. Договор
    # Цвет: 🟢 подписан / 🔴 не подписан. № договора и дни отсрочки — справка,
    # на цвет не влияют (только подсказывают согласующему условия работы).
    if contract["color"] == "green":
        parts_c = ["подписан"]
        if contract.get("number"):
            parts_c.append(f"№ {contract['number']}")
        if contract.get("days", 0) > 0:
            parts_c.append(f"отсрочка {contract['days']} дн")
        else:
            parts_c.append("отсрочка не указана")
        lines.append(f"🟢 *Договор:* " + " · ".join(parts_c))
    else:
        lines.append(f"🔴 *Договор:* не подписан")

    # 3. Просрочка
    if overdue["color"] == "red":
        lines.append(
            f"🔴 *Просрочка:* {overdue.get('days', 0)} дн "
            f"/ {_fmt_money(overdue.get('debt', 0))} ₽"
        )
    else:
        lines.append(f"🟢 *Просрочка:* нет")

    # 3a. Дата планируемой оплаты — то, что бот проставил автоматом
    # из План.даты отгрузки + дней отсрочки (план 2026-05-20-автоподстановка).
    # 🔴 если пустая — сигнал: контрагент без отсрочки или autofill не отработал.
    if payment_planned_date:
        lines.append(f"🟢 *Оплата:* {payment_planned_date}")
    else:
        lines.append(f"🔴 *Оплата:* не задана")

    # 4. ДДС
    explain = cashflow.get("explain", "")
    lines.append(f"{_icon(cashflow['color'])} *ДДС:* {explain}")

    # 4a. Долг по УПД — есть отгрузка(и) клиента в статусе «Долг по УПД» → 🔴.
    # Источник — статус отгрузки в МС (бухгалтерия ставит вручную), не payedSum.
    if upd_debt["color"] == "red":
        n = upd_debt.get("count", 0)
        word = "отгрузка" if n == 1 else "отгрузки" if 2 <= n <= 4 else "отгрузок"
        s = upd_debt.get("sum", 0) or 0
        tail = f" / {_fmt_money(s)} ₽" if s > 0 else ""
        lines.append(f"🔴 *Долг по УПД:* {n} {word}{tail}")
    elif upd_debt["color"] == "yellow":
        lines.append(f"🟡 *Долг по УПД:* не проверено")
    else:
        lines.append(f"🟢 *Долг по УПД:* нет")

    # 5. Сайт
    if site["color"] == "green":
        lines.append(f"🟢 *Сайт:* {site['raw_value'][:60]}")
    else:
        lines.append(f"🔴 *Сайт:* {site.get('raw_value', '') or 'не заполнен'}")

    # 6. Контакты
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

    # 6a. Адрес доставки — заполнен и чистый (только адрес, без телефонов/заметок)?
    # Нужно, чтобы адрес геокодился для логистики (мост МС→Wialon Logistics).
    if address["color"] == "green":
        lines.append(f"🟢 *Адрес:* {address['addr'][:60]}")
    else:
        reason = address.get("reason", "")
        if address.get("addr"):
            lines.append(f"🔴 *Адрес:* {reason} — {address['addr'][:55]}")
        else:
            lines.append(f"🔴 *Адрес:* {reason}")

    # 7. Цена — внизу, длинный список не мешает читать остальные строки
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
            compute_credit_color, compute_contract_color,
            compute_overdue_color, compute_cashflow_color,
            compute_price_color,
            compute_site_color, compute_contacts_color,
            compute_upd_debt_color, compute_address_color,
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

        # 2a. Надёжность контрагента (ЕГРЮЛ+финансы по ИНН) — мягкая деградация:
        # при таймауте/ошибке reliability=None → блок «не проверено», алерт уходит как обычно.
        reliability = None
        cp_inn = (cp_attrs.get("inn") or "").strip()
        if cp_inn:
            try:
                from counterparty_svetofor import check_counterparty
                reliability = await asyncio.wait_for(
                    check_counterparty(cp_inn, save=True), timeout=15)
            except Exception as e:
                logger.warning(f"check_approval_needed: надёжность {cp_inn} → {e}")

        # 3. Параллельно: cashflow, overdue, price, upd_debt (+ timeout 20с с fallback)
        try:
            cashflow, overdue, price, upd_debt = await asyncio.wait_for(
                asyncio.gather(
                    compute_cashflow_color(agent_id),
                    compute_overdue_color(agent_id),
                    compute_price_color(order_href),
                    compute_upd_debt_color(agent_id),
                    return_exceptions=True,
                ),
                timeout=20.0,
            )
        except asyncio.TimeoutError:
            logger.warning(f"check_approval_needed: timeout для {order_name}")
            cashflow = overdue = price = upd_debt = None

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
        upd_debt = _norm(upd_debt, {"color": "yellow", "count": 0, "sum": 0})

        # 4. Sync helper'ы.
        # current_debt берём из /report (cashflow), а НЕ из overdue.debt — overdue
        # отдаёт только просроченную часть, а для лимита нужна вся текущая дебиторка.
        current_debt = cashflow.get("current_debt", 0) or 0
        credit = compute_credit_color(cp_attrs, current_debt=current_debt, order_sum=order_sum)
        contract = compute_contract_color(cp_attrs)
        site = compute_site_color(cp_attrs)
        contacts = compute_contacts_color(cp_attrs)
        address = compute_address_color(order.get("shipmentAddress"))

        # 5. Сборка текста + colors_json для confirmation flow
        colors_json = {
            "credit": credit["color"],
            "contract": contract["color"],
            "overdue": overdue["color"],
            "cashflow": cashflow["color"],
            "price": price["color"],
            "site": site["color"],
            "contacts": contacts["color"],
            "upd_debt": upd_debt["color"],
            "address": address["color"],
            "reliability": (reliability or {}).get("color", "unknown"),
        }
        # Дата планируемой оплаты — то что выставил webhook-autofill за 60с до
        # этого алерта. Берём из attributes заказа, форматируем DD.MM.YYYY.
        ppm_str = ""
        for a in order.get("attributes", []) or []:
            if a.get("name") == "Дата планируемой оплаты":
                v = a.get("value")
                if v:
                    try:
                        from datetime import datetime as _dt
                        ppm_str = _dt.strptime(str(v)[:19], "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y")
                    except Exception:
                        ppm_str = str(v)[:10]
                break

        alert_text = _build_approval_text(
            order_name=order_name, order_sum=order_sum, state_name=state_name,
            client_name=agent_name, manager_name=manager_name,
            credit=credit, contract=contract, overdue=overdue, cashflow=cashflow,
            price=price, site=site, contacts=contacts, upd_debt=upd_debt,
            address=address,
            payment_planned_date=ppm_str,
            reliability=reliability,
        )

        # 6. Дедуп: sum_hash = округлённая сумма в ₽
        sum_hash = round(order_sum)

        # Резолвим manager_user_id (для callback `appr_comment`).
        # 1) Primary — захардкоженный маппинг PDZ_MANAGER_TG_IDS (lowercased
        #    фамилия → tg user_id). Тот же фикс, что в PDZ-flow (коммит 49bb318):
        #    db.get_manager_chat_id спотыкается о невидимые символы и
        #    регистр в /managers.
        # 2) Fallback — db.get_manager_chat_id (вдруг новый менеджер не
        #    проставлен в PDZ_MANAGER_TG_IDS, но засинкан в managers).
        from moysklad import PDZ_MANAGER_TG_IDS
        mgr_user_id = 0
        if manager_name:
            for part in manager_name.split():
                key = part.lower().strip(".,").rstrip()
                if key in PDZ_MANAGER_TG_IDS:
                    mgr_user_id = PDZ_MANAGER_TG_IDS[key]
                    break
            if not mgr_user_id:
                for part in manager_name.split():
                    cid = db.get_manager_chat_id(part)
                    if cid:
                        mgr_user_id = cid
                        break
        if not mgr_user_id and manager_name:
            logger.warning(
                f"check_approval_needed: не нашёл tg_id для менеджера "
                f"'{manager_name}' (ни в PDZ_MANAGER_TG_IDS, ни в /managers). "
                f"Reply-flow по этому заказу работать не будет."
            )

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
