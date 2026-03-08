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
import asyncio
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


async def search_products(query: str, limit: int = 20) -> list:
    """Ищет товары по названию, возвращает список с остатками и ценами."""
    try:
        async with aiohttp.ClientSession() as session:

            # Разбиваем запрос на ключевые слова и ищем по каждому
            # Берём самое длинное слово как основной фильтр (лучшая селективность)
            stop_words = {"с", "в", "на", "по", "из", "от", "до", "и", "а", "кг", "см", "г"}
            words = [
                w.lower() for w in query.split()
                if w not in stop_words and (
                    len(w) > 2 or  # обычные слова
                    w.isupper()    # аббревиатуры: ПР, СС, ОХЛ и т.д.
                )
            ]

            all_products = []
            seen_ids = set()

            url = f"{MS_BASE}/entity/product"

            # Ищем по каждому слову отдельно
            search_terms = words[:3] if words else [query]
            for term in search_terms:
                params = {
                    "filter": f"name~{term}",
                    "limit": limit,
                }
                async with session.get(url, headers=get_headers(), params=params) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        logger.error(f"МойСклад search error {resp.status}: {text[:200]}")
                        continue
                    data = await resp.json()

                for p in data.get("rows", []):
                    if p["id"] not in seen_ids:
                        seen_ids.add(p["id"])
                        all_products.append(p)

            # Нормализуем окончания для нечёткого совпадения (черную → черн)
            def normalize(word):
                return word[:-2] if len(word) > 5 else word

            def score(p):
                name = p.get("name", "").lower()
                return sum(1 for w in words
                           if w in name or normalize(w) in name)

            if words:
                # Строгий фильтр — все слова (с нормализацией)
                strict = [p for p in all_products if score(p) == len(words)]
                if strict:
                    products = strict[:limit]
                else:
                    # Мягкий — хотя бы половина слов, сортируем по релевантности
                    threshold = max(1, len(words) // 2)
                    soft = [p for p in all_products if score(p) >= threshold]
                    soft.sort(key=score, reverse=True)
                    products = soft[:limit]
            else:
                products = all_products[:limit]

            logger.info(f"МойСклад found {len(products)} products for query='{query}' (words={words})")
            if not products:
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


async def get_image_download_url(url: str) -> Optional[str]:
    """Возвращает прямую ссылку на скачивание фото из МойСклад."""
    try:
        logger.info(f"get_image_download_url: url={url}")
        async with aiohttp.ClientSession() as session:
            if "/images" in url and "downloadHref" not in url:
                async with session.get(url, headers=get_headers()) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                rows = data.get("rows", [])
                if not rows:
                    return None
                meta = rows[0].get("meta", {})
                download_url = meta.get("downloadHref") or meta.get("href")
                logger.info(f"get_image_download_url: resolved={download_url}")
                return download_url
            return url
    except Exception as e:
        logger.error(f"get_image_download_url error: {e}")
        return None


async def download_image(url: str) -> Optional[bytes]:
    """Скачивает фото товара из МойСклад.
    МойСклад /download/ делает редирект на CDN — скачиваем в два шага.
    """
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        # Шаг 1: получаем список изображений если нужно
        if "/images" in url and "download" not in url:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=get_headers()) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
            rows = data.get("rows", [])
            if not rows:
                return None
            meta = rows[0].get("meta", {})
            url = meta.get("downloadHref") or meta.get("href")
            if not url:
                return None

        # Шаг 2: используем miniature (превью) — идёт через порт 443, не CDN :8080
        # Преобразуем /download/UUID → /entity/product/.../images/UUID/miniature
        # Или используем прямой GET с параметром miniature через основной API
        miniature_url = url.replace("/download/", "/entity/product/") 
        # Попробуем получить миниатюру через images endpoint
        # url вида: https://api.moysklad.ru/api/remap/1.2/download/UUID
        # miniature доступна через: GET images endpoint с Accept: image/*
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Пробуем скачать через основной API с заголовком Accept image
            headers_img = dict(get_headers())
            headers_img["Accept"] = "image/png, image/jpeg, image/*, */*"
            async with session.get(url, headers=headers_img,
                                   allow_redirects=True) as resp:
                logger.info(f"download_image: direct status={resp.status} content-type={resp.content_type}")
                if resp.status == 200 and resp.content_type.startswith("image"):
                    data = await resp.read()
                    logger.info(f"download_image: got {len(data)} bytes direct")
                    return data
                elif resp.status in (301, 302, 303, 307, 308):
                    cdn_url = resp.headers.get("Location", "")
                    logger.info(f"download_image: redirect to {cdn_url}")
                    # Если CDN на нестандартном порту — пробуем заменить порт на 443
                    if ":8080" in cdn_url:
                        cdn_url_443 = cdn_url.replace(":8080", "")
                        logger.info(f"download_image: trying port 443 version: {cdn_url_443[:80]}...")
                        try:
                            async with session.get(cdn_url_443) as r2:
                                logger.info(f"download_image: port443 status={r2.status}")
                                if r2.status == 200:
                                    data = await r2.read()
                                    logger.info(f"download_image: got {len(data)} bytes via port443")
                                    return data
                        except Exception as e:
                            logger.error(f"download_image: port443 error {e}")
                    # Пробуем CDN как есть (вдруг Railway разрешает)
                    try:
                        cdn_timeout = aiohttp.ClientTimeout(total=10)
                        async with aiohttp.ClientSession(timeout=cdn_timeout) as cdn_s:
                            async with cdn_s.get(cdn_url) as r3:
                                logger.info(f"download_image: CDN direct status={r3.status}")
                                if r3.status == 200:
                                    data = await r3.read()
                                    logger.info(f"download_image: got {len(data)} bytes from CDN")
                                    return data
                    except asyncio.TimeoutError:
                        logger.error(f"download_image: CDN:8080 TIMEOUT — port blocked")
                    except Exception as e:
                        logger.error(f"download_image: CDN error {e}")
                else:
                    body = await resp.text()
                    logger.error(f"download_image: error status={resp.status} body={body[:200]}")
        return None
    except asyncio.TimeoutError:
        logger.error(f"download_image: TIMEOUT url={url}")
        return None
    except Exception as e:
        logger.error(f"download_image error: {e}", exc_info=True)
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
