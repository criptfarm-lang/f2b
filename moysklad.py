"""
Интеграция с МойСклад API
- Остатки товаров
- Цены
- Характеристики
- Фото из карточек
"""

import os
import logging
import aiohttp
from typing import Optional

logger = logging.getLogger(__name__)

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"


def get_headers():
    token = os.getenv("MOYSKLAD_TOKEN")
    if not token:
        raise ValueError("MOYSKLAD_TOKEN не задан!")
    return {
        "Authorization": f"Bearer {token}",
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json",
    }


async def search_products(query: str, limit: int = 10) -> list:
    """Ищет товары по названию, возвращает список с остатками и ценами."""
    try:
        async with aiohttp.ClientSession() as session:

            # 1. Ищем товары
            url = f"{MS_BASE}/entity/product"
            params = {
                "filter": f"name~{query}",
                "limit": limit,
                "expand": "productFolder",
            }
            async with session.get(url, headers=get_headers(), params=params) as resp:
                text = await resp.text()
                logger.info(f"МойСклад search status={resp.status}, body={text[:500]}")
                if resp.status != 200:
                    logger.error(f"МойСклад search error {resp.status}: {text}")
                    return []
                import json as _json
                data = _json.loads(text)

            products = data.get("rows", [])
            logger.info(f"МойСклад found {len(products)} products for query='{query}'")
            if not products:
                # Попробуем поиск без фильтра чтобы проверить что API вообще работает
                async with session.get(url, headers=get_headers(), params={"limit": 3}) as resp2:
                    data2 = await resp2.json()
                    sample = [r.get("name") for r in data2.get("rows", [])]
                    logger.info(f"МойСклад sample products (no filter): {sample}")
                return []

            # 2. Получаем остатки одним запросом
            product_ids = [p["id"] for p in products]
            stocks = await get_stocks(session, product_ids)

            # 3. Собираем результат
            result = []
            for p in products:
                pid = p["id"]
                stock_info = stocks.get(pid, {})

                # Достаём цену продажи
                sale_price = None
                for price in p.get("salePrices", []):
                    if price.get("value", 0) > 0:
                        sale_price = price["value"] / 100  # МойСклад хранит в копейках
                        break

                result.append({
                    "id": pid,
                    "name": p.get("name", ""),
                    "code": p.get("code", ""),
                    "article": p.get("article", ""),
                    "folder": p.get("productFolder", {}).get("name", "") if p.get("productFolder") else "",
                    "stock": stock_info.get("stock", 0),
                    "reserve": stock_info.get("reserve", 0),
                    "in_transit": stock_info.get("inTransit", 0),
                    "price": sale_price,
                    "unit": p.get("uom", {}).get("name", "кг") if p.get("uom") else "кг",
                    "description": p.get("description", ""),
                    "weight": p.get("weight"),
                    "image_href": p.get("images", {}).get("meta", {}).get("href") if p.get("images") else None,
                })

            return result

    except Exception as e:
        logger.error(f"МойСклад search_products error: {e}")
        return []


async def get_stocks(session: aiohttp.ClientSession, product_ids: list) -> dict:
    """Получает остатки для списка товаров."""
    try:
        url = f"{MS_BASE}/report/stock/all/current"
        # Формируем фильтр по product ids
        filter_str = ";".join([
            f"assortmentId={pid}" for pid in product_ids[:50]
        ])
        params = {"filter": filter_str}

        async with session.get(url, headers=get_headers(), params=params) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()

        stocks = {}
        for row in data:
            pid = row.get("assortmentId")
            if pid:
                stocks[pid] = row
        return stocks

    except Exception as e:
        logger.error(f"get_stocks error: {e}")
        return {}


async def get_product_image(product_id: str) -> Optional[str]:
    """Получает URL первого фото товара."""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{MS_BASE}/entity/product/{product_id}/images"
            async with session.get(url, headers=get_headers()) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()

            rows = data.get("rows", [])
            if rows:
                # Возвращаем miniature URL
                meta = rows[0].get("meta", {})
                return meta.get("downloadHref") or meta.get("href")
            return None

    except Exception as e:
        logger.error(f"get_product_image error: {e}")
        return None


async def download_image(url: str) -> Optional[bytes]:
    """Скачивает фото товара из МойСклад."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=get_headers()) as resp:
                if resp.status == 200:
                    return await resp.read()
        return None
    except Exception as e:
        logger.error(f"download_image error: {e}")
        return None


async def get_price_list(limit: int = 100) -> list:
    """Получает прайс-лист — все товары с ценами и остатками."""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{MS_BASE}/entity/product"
            params = {"limit": limit, "filter": "archived=false"}

            async with session.get(url, headers=get_headers(), params=params) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()

            products = data.get("rows", [])
            product_ids = [p["id"] for p in products]
            stocks = await get_stocks(session, product_ids)

            result = []
            for p in products:
                pid = p["id"]
                stock_info = stocks.get(pid, {})
                sale_price = None
                for price in p.get("salePrices", []):
                    if price.get("value", 0) > 0:
                        sale_price = price["value"] / 100
                        break

                if sale_price or stock_info.get("stock", 0) > 0:
                    result.append({
                        "name": p.get("name", ""),
                        "price": sale_price,
                        "stock": stock_info.get("stock", 0),
                        "unit": "кг",
                    })

            return sorted(result, key=lambda x: x["name"])

    except Exception as e:
        logger.error(f"get_price_list error: {e}")
        return []


def format_products(products: list, query: str = "") -> str:
    """Форматирует список товаров для отправки в Telegram."""
    if not products:
        return f"Товары по запросу «{query}» не найдены в МойСклад."

    lines = [f"📦 *Найдено в МойСклад: {len(products)} товар(ов)*\n"]

    for p in products:
        name = p["name"]
        stock = p.get("stock", 0)
        price = p.get("price")
        reserve = p.get("reserve", 0)

        # Статус наличия
        if stock > 0:
            stock_icon = "🟢"
            stock_str = f"{stock:,.1f} {p.get('unit', 'кг')}"
        elif p.get("in_transit", 0) > 0:
            stock_icon = "🟡"
            stock_str = f"в пути: {p['in_transit']:,.1f} {p.get('unit', 'кг')}"
        else:
            stock_icon = "🔴"
            stock_str = "нет в наличии"

        price_str = f" · {price:,.0f} руб/{p.get('unit', 'кг')}" if price else ""
        reserve_str = f" (резерв: {reserve:,.1f})" if reserve > 0 else ""

        lines.append(f"{stock_icon} *{name}*{price_str}")
        lines.append(f"   {stock_str}{reserve_str}")

        if p.get("article"):
            lines[-1] += f" · арт. {p['article']}"

    return "\n".join(lines)


def format_price_list(products: list) -> str:
    """Форматирует прайс-лист."""
    if not products:
        return "Прайс-лист пуст."

    lines = ["📋 *Актуальный прайс-лист МойСклад*\n"]
    for p in products:
        stock = p.get("stock", 0)
        price = p.get("price")
        icon = "🟢" if stock > 0 else "🔴"
        price_str = f"{price:,.0f} руб" if price else "цена не указана"
        lines.append(f"{icon} {p['name']} — {price_str}")

    return "\n".join(lines)
