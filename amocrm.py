"""
amoCRM API — интеграция для F2B PRO бота
Функции: поиск контактов, сделок, отправка сообщений в чат сделки
"""

import os
import logging
import asyncio
import aiohttp
from typing import Optional

logger = logging.getLogger(__name__)

AMO_SUBDOMAIN = os.getenv("AMO_SUBDOMAIN", "victorfishtobiz")
AMO_BASE_URL = f"https://{AMO_SUBDOMAIN}.amocrm.ru/api/v4"


def get_headers() -> dict:
    token = os.getenv("AMO_ACCESS_TOKEN", "")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


# ─── Базовый запрос ───────────────────────────────────────────────────────────

async def amo_get(path: str, params: dict = None) -> Optional[dict]:
    """GET запрос к amoCRM API."""
    url = f"{AMO_BASE_URL}{path}"
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=get_headers(), params=params) as resp:
                if resp.status == 200:
                    raw = await resp.read()
                    try:
                        text = raw.decode("utf-8")
                    except UnicodeDecodeError:
                        text = raw.decode("windows-1251")
                    import json
                    return json.loads(text)
                elif resp.status == 401:
                    logger.error("amoCRM: токен истёк, нужно обновить AMO_ACCESS_TOKEN")
                    return None
                else:
                    text = await resp.text()
                    logger.error(f"amoCRM GET {path}: {resp.status} {text[:200]}")
                    return None
    except asyncio.TimeoutError:
        logger.error(f"amoCRM GET {path}: таймаут 10 сек")
        return None
    except Exception as e:
        logger.error(f"amoCRM GET {path}: {e}")
        return None


async def amo_post(path: str, data: dict) -> Optional[dict]:
    """POST запрос к amoCRM API."""
    url = f"{AMO_BASE_URL}{path}"
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=get_headers(), json=data) as resp:
                if resp.status in (200, 201):
                    return await resp.json()
                else:
                    text = await resp.text()
                    logger.error(f"amoCRM POST {path}: {resp.status} {text[:200]}")
                    return None
    except asyncio.TimeoutError:
        logger.error(f"amoCRM POST {path}: таймаут 10 сек")
        return None
    except Exception as e:
        logger.error(f"amoCRM POST {path}: {e}")
        return None


# ─── Контакты и компании ──────────────────────────────────────────────────────

async def find_contact_by_name(name: str) -> list:
    """Ищет контакты по имени или названию компании."""
    result = await amo_get("/contacts", params={"query": name, "limit": 10})
    if not result:
        return []
    return result.get("_embedded", {}).get("contacts", [])


async def find_company_by_name(name: str) -> list:
    """Ищет компании по названию."""
    result = await amo_get("/companies", params={"query": name, "limit": 10})
    if not result:
        return []
    return result.get("_embedded", {}).get("companies", [])


# ─── Сделки ──────────────────────────────────────────────────────────────────

async def get_leads_by_contact(contact_id: int) -> list:
    """Получает сделки контакта."""
    result = await amo_get(f"/contacts/{contact_id}/leads")
    if not result:
        return []
    return result.get("_embedded", {}).get("leads", [])


async def get_lead(lead_id: int) -> Optional[dict]:
    """Получает сделку по ID."""
    return await amo_get(f"/leads/{lead_id}")


async def get_active_leads(limit: int = 50, page: int = 1) -> list:
    """Получает активные сделки (не закрытые)."""
    result = await amo_get("/leads", params={
        "limit": limit,
        "page": page,
        "filter[statuses][0][pipeline_id]": "",  # все воронки
    })
    if not result:
        return []
    return result.get("_embedded", {}).get("leads", [])


# ─── Поиск контрагентов по товару (через МойСклад + amoCRM) ──────────────────

def normalize_name(name: str) -> str:
    """Убирает ООО, ИП, кавычки для нечёткого сравнения."""
    import re
    name = name.upper()
    name = re.sub(r'\b(ООО|ОАО|ЗАО|ИП|АО|ПАО|НКО)\b', '', name)
    name = re.sub(r'["\'\«\»]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def normalize_phone(phone: str) -> str:
    """Нормализует телефон — только цифры, 11-значный формат."""
    import re
    digits = re.sub(r'\D', '', phone)
    if len(digits) == 10:
        digits = '7' + digits
    elif len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    return digits


async def find_contact_by_phone(phone: str) -> Optional[dict]:
    """Ищет контакт в amoCRM по номеру телефона."""
    normalized = normalize_phone(phone)
    result = await amo_get("/contacts", params={"query": normalized, "limit": 5})
    if not result:
        return None
    contacts = result.get("_embedded", {}).get("contacts", [])
    return contacts[0] if contacts else None


async def find_contacts_for_broadcast(counterparties: list) -> list:
    """
    По списку контрагентов из МойСклад находит контакты в amoCRM.
    counterparties: [{"name": ..., "phone": ...}] или ["название", ...]
    Возвращает список: [{"name": ..., "amo_name": ..., "lead_id": ...}]
    """
    found = []
    for cp in counterparties:
        name = cp.get("name", "") if isinstance(cp, dict) else cp
        phone = cp.get("phone") if isinstance(cp, dict) else None

        contact = None

        # 1. Ищем по телефону — самый надёжный способ
        if phone:
            contact = await find_contact_by_phone(phone)
            if contact:
                logger.info(f"Найдено по телефону: '{name}' → '{contact.get('name')}' (тел: {phone})")

        # 2. Ищем по нормализованному названию компании
        if not contact:
            search_query = normalize_name(name)
            companies = await find_company_by_name(search_query)
            if not companies and search_query != name:
                companies = await find_company_by_name(name)
            if companies:
                contact = companies[0]
                logger.info(f"Найдено по компании: '{name}' → '{contact.get('name')}'")

        # 3. Ищем как контакт
        if not contact:
            contacts = await find_contact_by_name(normalize_name(name))
            if contacts:
                contact = contacts[0]
                logger.info(f"Найдено как контакт: '{name}' → '{contact.get('name')}'")

        if not contact:
            logger.warning(f"Не найдено в amoCRM: '{name}' (тел: {phone})")
            await asyncio.sleep(0.1)
            continue

        # Получаем сделки
        contact_id = contact["id"]
        leads_data = await amo_get(f"/contacts/{contact_id}/leads")
        if not leads_data or not leads_data.get("_embedded", {}).get("leads"):
            leads_data = await amo_get(f"/companies/{contact_id}/leads")

        leads = leads_data.get("_embedded", {}).get("leads", []) if leads_data else []
        if leads:
            lead_id = leads[0]["id"]
            found.append({
                "name": name,
                "amo_name": contact.get("name", name),
                "contact_id": contact_id,
                "lead_id": lead_id,
            })
        else:
            logger.warning(f"Нет сделок для '{name}' (contact_id={contact_id})")

        await asyncio.sleep(0.1)

    logger.info(f"find_contacts_for_broadcast: найдено {len(found)} из {len(counterparties)}")
    return found
    """
    По списку названий контрагентов из МойСклад находит контакты/компании в amoCRM.
    Возвращает список: [{"name": ..., "amo_name": ..., "company_id": ..., "lead_id": ...}]
    """
    found = []
    for name in counterparty_names:
        search_query = normalize_name(name)
        if len(search_query) < 2:
            search_query = name

        logger.info(f"amoCRM поиск: '{name}' → normalized='{search_query}'")

        # Ищем как компанию
        companies = await find_company_by_name(search_query)
        logger.info(f"amoCRM companies по '{search_query}': {[c.get('name') for c in companies]}")
        if not companies:
            companies = await find_company_by_name(name)
            logger.info(f"amoCRM companies по оригиналу '{name}': {[c.get('name') for c in companies]}")

        if companies:
            company = companies[0]
            company_id = company["id"]
            leads_data = await amo_get(f"/companies/{company_id}/leads")
            leads = leads_data.get("_embedded", {}).get("leads", []) if leads_data else []
            if leads:
                lead_id = leads[0]["id"]
                found.append({
                    "name": name,
                    "amo_name": company.get("name", name),
                    "company_id": company_id,
                    "lead_id": lead_id,
                })
                logger.info(f"Найдено в amoCRM: '{name}' → '{company.get('name')}' lead={lead_id}")
                await asyncio.sleep(0.1)
                continue

        # Ищем как контакт
        contacts = await find_contact_by_name(search_query)
        if contacts:
            contact = contacts[0]
            contact_id = contact["id"]
            leads = await get_leads_by_contact(contact_id)
            if leads:
                lead_id = leads[0]["id"]
                found.append({
                    "name": name,
                    "amo_name": contact.get("name", name),
                    "contact_id": contact_id,
                    "lead_id": lead_id,
                })
                logger.info(f"Найдено как контакт в amoCRM: '{name}' lead={lead_id}")

        await asyncio.sleep(0.1)

    logger.info(f"find_contacts_for_broadcast: найдено {len(found)} из {len(counterparty_names)}")
    return found


# ─── Отправка сообщений ───────────────────────────────────────────────────────

async def send_message_to_lead(lead_id: int, text: str) -> bool:
    """
    Отправляет сообщение в чат сделки (через встроенные мессенджеры amoCRM).
    Сообщение уйдёт клиенту через тот мессенджер который он использует.
    """
    # Получаем talks (чаты) сделки
    talks_data = await amo_get(f"/leads/{lead_id}/talks")
    if not talks_data:
        logger.warning(f"Нет чатов для сделки {lead_id}")
        return False

    talks = talks_data.get("_embedded", {}).get("talks", [])
    if not talks:
        logger.warning(f"Сделка {lead_id}: нет активных чатов")
        return False

    # Берём последний активный чат
    talk_id = talks[0]["id"]

    result = await amo_post(f"/talks/{talk_id}/messages", {
        "text": text
    })

    if result:
        logger.info(f"Сообщение отправлено в сделку {lead_id}, чат {talk_id}")
        return True
    return False


# ─── Рассылка ────────────────────────────────────────────────────────────────

async def broadcast_to_leads(lead_ids: list, text: str, delay_seconds: int = 60) -> dict:
    """
    Медленная рассылка по списку сделок.
    delay_seconds — пауза между сообщениями (по умолчанию 1 минута).
    Возвращает статистику: {"sent": N, "failed": N, "errors": [...]}
    """
    sent = 0
    failed = 0
    errors = []

    for i, lead_id in enumerate(lead_ids):
        try:
            success = await send_message_to_lead(lead_id, text)
            if success:
                sent += 1
                logger.info(f"Рассылка [{i+1}/{len(lead_ids)}]: ✅ сделка {lead_id}")
            else:
                failed += 1
                errors.append(lead_id)
                logger.warning(f"Рассылка [{i+1}/{len(lead_ids)}]: ❌ сделка {lead_id}")
        except Exception as e:
            failed += 1
            errors.append(lead_id)
            logger.error(f"Рассылка [{i+1}/{len(lead_ids)}]: ошибка {e}")

        # Пауза между сообщениями (кроме последнего)
        if i < len(lead_ids) - 1:
            await asyncio.sleep(delay_seconds)

    return {"sent": sent, "failed": failed, "errors": errors}


# ─── Проверка подключения ─────────────────────────────────────────────────────

async def check_connection() -> bool:
    """Проверяет что токен работает."""
    result = await amo_get("/account")
    if result:
        logger.info(f"amoCRM подключён: {result.get('name')} (id={result.get('id')})")
        return True
    return False
