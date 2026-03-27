"""
Интеграция с МойСклад API
- Остатки товаров
- Цены
- Характеристики
- Фото из карточек
"""

import os
import logging
import re
import aiohttp
import asyncio
from typing import Optional

logger = logging.getLogger(__name__)

YANDEX_GEOCODER_KEY = os.getenv("YANDEX_GEOCODER_KEY", "5a133f74-30f1-4296-9dc4-a780332987cc")

# Координаты центров направлений (lat, lon)
DELIVERY_CITIES_COORDS = {
    "Звенигород":       (55.7324, 36.8519),
    "Истра":            (55.9167, 36.8667),
    "Солнечногорск":    (56.1833, 36.9833),
    "Королёв":          (55.9167, 37.8333),
    "Мытищи":           (55.9108, 37.7297),
    "Одинцово":         (55.6833, 37.2833),
    "Подольск":         (55.4167, 37.5500),
    "Серпухов":         (54.9167, 37.4000),
    "Чехов":            (55.1500, 37.4667),
    "Щелково":          (55.9167, 38.0167),
    "Домодедово":       (55.4333, 37.7667),
    "Орехово-Зуево":    (55.8000, 38.9833),
    "Павловский Посад": (55.7833, 38.6500),
    "Сергиев Посад":    (56.3000, 38.1333),
    "Красноармейск":    (56.1000, 38.1500),
    "Пушкино":          (56.0167, 37.8500),
    "Апрелевка":        (55.5500, 37.0667),
    "Наро-Фоминск":     (55.3833, 36.7333),
    "Егорьевск":        (55.3833, 39.0333),
    "Воскресенск":      (55.3167, 38.6667),
    "Каширское шоссе":  (55.3000, 37.6167),
}

# Радиус (км) в котором адрес считается относящимся к направлению
DELIVERY_RADIUS_KM = 25

def _haversine(lat1, lon1, lat2, lon2) -> float:
    """Расстояние между двумя точками в км."""
    import math
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

async def geocode_address(address: str) -> tuple:
    """Геокодирует адрес через Яндекс. Возвращает (lat, lon) или None."""
    try:
        import urllib.parse
        params = urllib.parse.urlencode({
            "apikey": YANDEX_GEOCODER_KEY,
            "geocode": address,
            "format": "json",
            "results": 1,
            "ll": "37.6173,55.7558",
            "spn": "2.0,2.0",
        })
        url = f"https://geocode-maps.yandex.ru/1.x/?{params}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
        pos = (data["response"]["GeoObjectCollection"]
               ["featureMember"][0]["GeoObject"]["Point"]["pos"])
        lon, lat = map(float, pos.split())
        return lat, lon
    except Exception as e:
        logger.warning(f"geocode_address error: {e}")
        return None

def fmt_money(amount: float) -> str:
    """Форматирует сумму в рублях: 192 850,45 руб."""
    return f"{amount:,.2f}".replace(",", " ").replace(".", ",").rstrip("0").rstrip(",") + " руб."

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
    """Ищет товары по названию с поддержкой сокращений и синонимов."""
    try:
        # ── Словарь сокращений ──────────────────────────────────────────
        # Термин обработки / состояния
        ABBR = {
            "хк":      ["х/к", "холодн"],
            "х/к":     ["х/к", "холодн"],
            "гк":      ["г/к", "горяч"],
            "г/к":     ["г/к", "горяч"],
            "сс":      ["с/с", "слабосол"],
            "с/с":     ["с/с", "слабосол"],
            "охл":     ["охл"],
            "зам":     ["заморож"],
            "заморож": ["заморож"],
            # СМ = сырой мороженый: есть заморож, нет х/к и с/с
            # Обрабатывается отдельно в score()
            "см":      ["__СМ__"],
            "с/м":     ["__СМ__"],
            
            "мрм":     ["мурманск", "мурм", "мрм"],
            "мурманск": ["мурманск"],
            # Вид разделки
            "пр":      ["пр"],
            "тримпр":  ["трим пр"],
            "трим":    ["трим"],
            # Виды разделки (буква) — только "трим X"
            "а":       ["трим а"],
            "б":       ["трим б"],
            "д":       ["трим д"],
            "е":       ["трим е"],
            "с":       ["трим с"],
        }
        # Синонимы названий рыб
        SYNONYMS = {
            "семга":   "лосось",
            "сёмга":   "лосось",
            "сёмга":   "лосось",
            "форель":  "форель",
            "масляная": "масляная",
            "маслян":  "масляная",
            "угорь":   "угорь",
            "осьминог": "осьминог",
            "палтус":  "палтус",
            "треска":  "треска",
            "минтай":  "минтай",
            "горбуша": "горбуша",
            "кета":    "кета",
            "чавыча":  "чавыча",
            "кижуч":   "кижуч",
            "нерка":   "нерка",
            "сибас":   "сибас",
            "дорада":  "дорада",
            "тунец":   "тунец",
            "скумбрия": "скумбрия",
            "сельдь":  "сельдь",
            "мойва":   "мойва",
            "краб":    "краб",
            "креветка": "крев",
            "крев":    "крев",
            "кальмар": "кальмар",
        }

        stop_words = {"с", "в", "на", "по", "из", "от", "до", "и", "а", "кг", "гр", "г", "филе"}

        raw_words = query.lower().split()

        # Нормализуем каждое слово
        search_tokens = []   # что ищем в МойСклад (для API запроса — основное слово)
        match_tokens  = []   # что проверяем в названии (может быть несколько вариантов)

        for w in raw_words:
            w = w.strip(".,;:()/-")
            if not w or w in stop_words:
                continue

            # Числа-диапазоны (1.6-2.0) — пропускаем
            if re.match(r'^[0-9.,\-]+$', w):
                continue

            # Синоним
            canon = SYNONYMS.get(w, w)

            # Сокращение → варианты для матчинга
            if w in ABBR:
                variants = ABBR[w]
                match_tokens.append(variants)
                # Не добавляем в API поиск — аббревиатура не поможет
            else:
                match_tokens.append([canon])
                if len(canon) > 2 or canon.isupper():
                    search_tokens.append(canon)

        # Если search_tokens пустые — берём первые слова из match_tokens
        if not search_tokens:
            for mt in match_tokens:
                if len(mt[0]) > 2:
                    search_tokens.append(mt[0])
                    break

        logger.info(f"search_products: query='{query}' search_tokens={search_tokens} match_tokens={match_tokens}")

        async with aiohttp.ClientSession() as session:
            all_products = []
            seen_ids = set()
            url = f"{MS_BASE}/entity/product"

            # Ищем по первым 2 токенам
            for term in search_tokens[:2]:
                params = {"filter": f"name~{term}", "limit": 50}
                async with session.get(url, headers=get_headers(), params=params) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                for p in data.get("rows", []):
                    if p["id"] not in seen_ids:
                        seen_ids.add(p["id"])
                        all_products.append(p)

            # Скоринг: считаем сколько match_tokens встречается в названии
            def score(p):
                name = p.get("name", "").lower()
                def norm(w): return w[:-2] if len(w) > 5 else w
                hits = 0
                for variants in match_tokens:
                    if "__СМ__" in variants:
                        # СМ = заморож + НЕ х/к + НЕ с/с
                        if "заморож" in name and "х/к" not in name and "с/с" not in name:
                            hits += 1
                    elif any(norm(v) in name or v in name for v in variants):
                        hits += 1
                return hits

            total = len(match_tokens)
            if total == 0:
                products = all_products[:limit]
            else:
                # Строгий: все токены совпали
                strict = [p for p in all_products if score(p) == total]
                def sort_key(p):
                    # Сначала те что в наличии, потом по релевантности
                    in_stock = 1 if p.get("stock", 0) > 0 else 0
                    return (in_stock, score(p))

                if strict:
                    products = sorted(strict, key=sort_key, reverse=True)[:limit]
                else:
                    # Мягкий: хотя бы половина
                    threshold = max(1, total // 2)
                    soft = [p for p in all_products if score(p) >= threshold]
                    products = sorted(soft, key=sort_key, reverse=True)[:limit]

            logger.info(f"МойСклад found {len(products)} products for query='{query}' tokens={search_tokens}")
            if not products:
                return []

            # Получаем остатки
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
                result.append({
                    "id": pid,
                    "name": p.get("name", ""),
                    "sale_price": sale_price,
                    "stock": stock_info.get("stock", 0),
                    "reserve": stock_info.get("reserve", 0),
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
    """Скачивает фото товара из МойСклад."""
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:

            # Шаг 1: получаем href первого изображения если передан /images URL
            if "/images" in url and "download" not in url and "miniature" not in url:
                async with session.get(url, headers=get_headers()) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                rows = data.get("rows", [])
                if not rows:
                    return None
                img_href = rows[0].get("meta", {}).get("href")
                if not img_href:
                    return None
                logger.info(f"download_image: img_href={img_href}")
            else:
                img_href = url

            # Шаг 2: пробуем /download (полный размер)
            for suffix in ["/download", "/miniature"]:
                try_url = img_href + suffix
                logger.info(f"download_image: trying {try_url}")
                async with session.get(try_url, headers=get_headers(),
                                       allow_redirects=True) as resp:
                    logger.info(f"download_image: {suffix} status={resp.status} type={resp.content_type}")
                    if resp.status == 200 and "image" in (resp.content_type or ""):
                        data = await resp.read()
                        logger.info(f"download_image: got {len(data)} bytes via {suffix}")
                        return data

        logger.error("download_image: все способы не сработали")
        return None
    except asyncio.TimeoutError:
        logger.error(f"download_image: TIMEOUT url={url}")
        return None
    except Exception as e:
        logger.error(f"download_image error: {e}", exc_info=True)
        return None

async def search_products_filtered(parsed: dict, limit: int = 20) -> list:
    """Поиск товаров используя разобранные Claude фильтры."""
    search_term = parsed.get("search_term", "")
    filters = parsed.get("filters", {})
    raw_tokens = parsed.get("raw_tokens", [])
    
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{MS_BASE}/entity/product"
            params = {"filter": f"name~{search_term}", "limit": 100}
            
            async with session.get(url, headers=get_headers(), params=params) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
            
            # Исключаем нерыбные товары (красители, упаковка и т.п.)
            NON_FISH = ["краситель", "упаковк", "пакет", "контейнер", "лоток", "соус", "маринад"]
            all_products = [
                p for p in data.get("rows", [])
                if not any(kw in p.get("name", "").lower() for kw in NON_FISH)
            ]
            
            def matches(p):
                name = p.get("name", "").lower()

                # Исключаем нерыбные товары
                junk_words = ["краситель", "упаковк", "пакет", "лоток", "соус", "маринад"]
                if any(j in name for j in junk_words):
                    return False

                # Тип разделки (тушка/филе)
                cut = filters.get("cut")
                if cut == "псг":
                    if "псг" not in name:
                        return False
                elif cut == "филе":
                    if "филе" not in name or "псг" in name:
                        return False

                # Вид разделки
                trim = filters.get("trim")
                if trim:
                    if f"трим {trim}" not in name:
                        return False
                
                # Обработка
                processing = filters.get("processing")
                if processing == "хк":
                    if "х/к" not in name:
                        return False
                elif processing == "гк":
                    if "г/к" not in name:
                        return False
                elif processing == "сс":
                    if "с/с" not in name:
                        return False
                elif processing == "см":
                    # Сырой мороженый — нет копчения, нет засолки
                    if "х/к" in name or "г/к" in name or "с/с" in name:
                        return False
                    if "заморож" not in name:
                        return False
                
                # Состояние
                state = filters.get("state")
                if state == "охл":
                    if "охл" not in name:
                        return False
                elif state == "заморож":
                    if "заморож" not in name:
                        return False
                
                # Регион
                region = filters.get("region")
                if region == "мурманск":
                    if "мурманск" not in name and "мрм" not in name:
                        return False
                elif region == "чили":
                    if "чили" not in name:
                        return False
                    if "чили" not in name:
                        return False
                
                # Калибр
                caliber = filters.get("caliber")
                if caliber and caliber not in name:
                    return False
                
                return True
            
            products = [p for p in all_products if matches(p)]
            
            if not products:
                logger.info(f"search_products_filtered: no strict matches, falling back")
                products = [p for p in all_products if not any(
                    j in p.get("name","").lower() for j in ["краситель","упаковк","пакет","лоток"]
                )]
            
            products = products[:limit]
            logger.info(f"search_products_filtered: '{search_term}' filters={filters} → {len(products)} products")
            
            if not products:
                return []
            
            # Получаем остатки
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
                result.append({
                    "id": pid,
                    "name": p.get("name", ""),
                    "sale_price": sale_price,
                    "stock": stock_info.get("stock", 0),
                    "reserve": stock_info.get("reserve", 0),
                    "image_href": p.get("images", {}).get("meta", {}).get("href") if p.get("images") else None,
                })
            
            # Фильтр "только в наличии"
            if filters.get("in_stock"):
                result = [r for r in result if r["stock"] > 0]

            # Сортируем: в наличии первыми
            result.sort(key=lambda x: (1 if x["stock"] > 0 else 0), reverse=True)
            return result
            
    except Exception as e:
        logger.error(f"search_products_filtered error: {e}")
        return []

async def get_counterparty_balance(query: str) -> list:
    """Ищет контрагента по имени и возвращает баланс через /report/counterparty."""
    import re as _re

    def _strip_legal(q: str) -> str:
        return _re.sub(r'^\s*(ооо|ип|зао|ао|пао|оао|нко|снт)\s+', '', q.strip(), flags=_re.IGNORECASE).strip()

    try:
        async with aiohttp.ClientSession() as session:
            url = f"{MS_BASE}/entity/counterparty"
            rows = []
            stripped = _strip_legal(query)
            queries = [query, query.upper(), query.lower(), query.capitalize(),
                       query.replace(" ", "-"), query.replace(" ", "-").upper()]
            if stripped and stripped.lower() != query.lower():
                queries += [stripped, stripped.upper(), stripped.lower(),
                            stripped.replace(" ", "-"), stripped.replace(" ", "-").upper()]
            # Также пробуем по первому значимому слову
            words = [w for w in query.split() if len(w) >= 4]
            queries += words

            for q in queries:
                params = {"filter": f"name~{q}", "limit": 10}
                async with session.get(url, headers=get_headers(), params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rows = data.get("rows", [])
                        if rows:
                            logger.info(f"counterparty found with query variant '{q}'")
                            break
            if not rows:
                logger.info(f"counterparty not found for query='{query}'")
                return []

            # Шаг 2: для каждого контрагента получить баланс через report
            result = []
            for c in rows:
                cid = c["id"]
                report_url = f"{MS_BASE}/report/counterparty/{cid}"
                async with session.get(report_url, headers=get_headers()) as resp2:
                    if resp2.status != 200:
                        body = await resp2.text()
                        logger.error(f"counterparty report {resp2.status}: {body[:200]}")
                        balance = 0
                    else:
                        rdata = await resp2.json()
                        # МойСклад хранит деньги в копейках — делим на 100
                        raw_balance = rdata.get("balance", 0) or 0
                        balance = raw_balance / 100

                # Для покупателей: баланс < 0 = нам должны, баланс > 0 = мы должны
                debt = -balance if balance < 0 else 0
                result.append({
                    "id": cid,
                    "name": c.get("name", ""),
                    "balance": balance,
                    "debt": debt,
                    "tags": c.get("tags", []),
                    "created": c.get("created", ""),
                    "href": c.get("meta", {}).get("href", f"{MS_BASE}/entity/counterparty/{cid}"),
                })
                logger.info(f"get_counterparty_balance: {c.get('name')} tags={c.get('tags', [])}")
            return result

    except Exception as e:
        logger.error(f"get_counterparty_balance error: {e}", exc_info=True)
        return []

async def get_all_debtors() -> list:
    """Получает всех контрагентов с долгами через /report/counterparty."""
    try:
        async with aiohttp.ClientSession() as session:
            # /report/counterparty возвращает список с балансами
            url = f"{MS_BASE}/report/counterparty"
            params = {"limit": 100}
            async with session.get(url, headers=get_headers(), params=params) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(f"report/counterparty {resp.status}: {body[:200]}")
                    return []
                data = await resp.json()

            result = []
            for c in data.get("rows", []):
                balance = (c.get("balance", 0) or 0) / 100
                if balance < 0:  # отрицательный = нам должны (покупатели)
                    name = c.get("counterparty", {}).get("name", c.get("name", ""))
                    result.append({
                        "id": c.get("counterparty", {}).get("id", ""),
                        "name": name,
                        "debt": -balance,
                    })

            result.sort(key=lambda x: x["debt"], reverse=True)
            logger.info(f"get_all_debtors: found {len(result)} debtors")
            return result

    except Exception as e:
        logger.error(f"get_all_debtors error: {e}")
        return []

def format_debtors_ms(debtors: list) -> str:
    """Форматирует список должников из МойСклад."""
    if not debtors:
        return "\u2705 \u0414\u0435\u0431\u0438\u0442\u043e\u0440\u0441\u043a\u043e\u0439 \u0437\u0430\u0434\u043e\u043b\u0436\u0435\u043d\u043d\u043e\u0441\u0442\u0438 \u043d\u0435\u0442."

    total = sum(d["debt"] for d in debtors)
    lines = [
        f"\U0001f4b0 *\u0414\u0435\u0431\u0438\u0442\u043e\u0440\u0441\u043a\u0430\u044f \u0437\u0430\u0434\u043e\u043b\u0436\u0435\u043d\u043d\u043e\u0441\u0442\u044c \u2014 {len(debtors)} \u043a\u043b\u0438\u0435\u043d\u0442\u043e\u0432*",
        f"\u0418\u0442\u043e\u0433\u043e: *{fmt_money(total)}*\n",
    ]
    for d in debtors:
        lines.append(f"\u2022 {d['name']} \u2014 *{fmt_money(d['debt'])}*")

    return "\n".join(lines)

def format_counterparty_balance(counterparties: list, query: str) -> str:
    """Форматирует баланс конкретного контрагента."""
    if not counterparties:
        return f"\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442 \u00ab{query}\u00bb \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d \u0432 \u041c\u043e\u0439\u0421\u043a\u043b\u0430\u0434."

    lines = []
    for c in counterparties:
        balance = c["balance"]
        name = c["name"]
        if balance < 0:
            lines.append(f"\U0001f534 *{name}*\n\u0414\u043e\u043b\u0433 \u043f\u0435\u0440\u0435\u0434 \u043d\u0430\u043c\u0438: *{fmt_money(-balance)}*")
        elif balance > 0:
            lines.append(f"\U0001f7e2 *{name}*\n\u041c\u044b \u0434\u043e\u043b\u0436\u043d\u044b \u0438\u043c: *{fmt_money(balance)}*")
        else:
            lines.append(f"\u2705 *{name}*\n\u0411\u0430\u043b\u0430\u043d\u0441 \u043d\u0443\u043b\u0435\u0432\u043e\u0439, \u0434\u043e\u043b\u0433\u043e\u0432 \u043d\u0435\u0442.")

    return "\n\n".join(lines)

# Карта тегов → менеджер
MANAGER_TAGS = {
    "баласанян": "Карина Баласанян",
    "леонтьев":  "Алексей Леонтьев",
    "мерзлякова": "Елена Мерзлякова",
    "скляр":     "Инесса Скляр",
}

# Тип покупателя
BUYER_TYPE_TAGS = {
    "хорека": "ХОРЕКА (рестораны)",
    "опт":    "ОПТ (оптовые покупатели)",
    "покупатели": "Покупатель",
}

async def get_manager_stats_ms(manager_tag: str, active_days: int = 60) -> dict:
    """
    Статистика менеджера из МойСклад:
    - total: всего компаний с тегом менеджера
    - active: уникальные компании с заказами за active_days дней
    """
    import aiohttp
    from datetime import datetime, timedelta

    try:
        async with aiohttp.ClientSession() as session:
            # 1. Всего компаний с тегом менеджера
            url = f"{MS_BASE}/entity/counterparty"
            async with session.get(url, headers=get_headers(),
                                   params={"filter": f"tags={manager_tag}", "limit": 1}) as resp:
                if resp.status != 200:
                    return {"total": 0, "active": 0}
                data = await resp.json()
                total = data.get("meta", {}).get("size", 0)

            if not total:
                return {"total": 0, "active": 0}

            # 2. Получаем ID всех контрагентов с тегом
            cp_ids = set()
            offset = 0
            while True:
                async with session.get(url, headers=get_headers(),
                                       params={"filter": f"tags={manager_tag}", "limit": 100, "offset": offset}) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()
                    rows = data.get("rows", [])
                    for r in rows:
                        cp_ids.add(r["id"])
                    if len(rows) < 100:
                        break
                    offset += 100

            logger.info(f"get_manager_stats_ms: tag={manager_tag} total={total} cp_ids={len(cp_ids)}")

            # 3. Для каждого клиента проверяем был ли заказ за period
            since = (datetime.now() - timedelta(days=active_days)).strftime("%Y-%m-%d %H:%M:%S")
            active_ids = set()
            orders_url = f"{MS_BASE}/entity/customerorder"

            # Батчами по 5 параллельных запросов
            import asyncio as _asyncio
            cp_list = list(cp_ids)

            async def check_cp(cp_id):
                filter_str = (
                    f"agent=https://api.moysklad.ru/api/remap/1.2/entity/counterparty/{cp_id}"
                    f";moment>{since}"
                )
                async with aiohttp.ClientSession() as s:
                    async with s.get(orders_url, headers=get_headers(),
                                     params={"filter": filter_str, "limit": 1}) as r:
                        if r.status == 200:
                            d = await r.json()
                            if d.get("meta", {}).get("size", 0) > 0:
                                return cp_id
                return None

            # Запускаем батчами по 10
            for i in range(0, len(cp_list), 10):
                batch = cp_list[i:i+10]
                results = await _asyncio.gather(*[check_cp(cp_id) for cp_id in batch])
                for r in results:
                    if r:
                        active_ids.add(r)

            logger.info(f"get_manager_stats_ms: active={len(active_ids)}")
            return {"total": total, "active": len(active_ids)}

    except Exception as e:
        logger.error(f"get_manager_stats_ms: {e}")
        return {"total": 0, "active": 0}

async def search_counterparty_by_name(query: str) -> dict | None:
    """
    Ищет контрагента по имени в МойСклад.
    Возвращает {"id": ..., "name": ...} первого совпадения или None.
    """
    import re as _re
    import aiohttp

    def _strip_legal(q: str) -> str:
        return _re.sub(r'^\s*(ооо|ип|зао|ао|пао|оао|нко|снт)\s+', '', q.strip(), flags=_re.IGNORECASE).strip()

    try:
        stripped = _strip_legal(query)
        # Перебираем варианты написания
        variants = [query, query.upper(), query.lower(), query.capitalize()]
        if stripped and stripped.lower() != query.lower():
            variants += [stripped, stripped.upper(), stripped.lower(), stripped.capitalize()]
        # Также пробуем по первому значимому слову
        words = [w for w in query.split() if len(w) >= 4]
        variants += words

        async with aiohttp.ClientSession() as session:
            url = f"{MS_BASE}/entity/counterparty"
            for v in variants:
                params = {"filter": f"name~{v}", "limit": 5}
                async with session.get(url, headers=get_headers(), params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rows = data.get("rows", [])
                        if rows:
                            logger.info(f"search_counterparty_by_name: found '{rows[0].get('name')}' for query='{query}'")
                            return {"id": rows[0]["id"], "name": rows[0].get("name", "")}
        logger.info(f"search_counterparty_by_name: not found for query='{query}'")
        return None
    except Exception as e:
        logger.error(f"search_counterparty_by_name: {e}", exc_info=True)
        return None


async def get_counterparty_requisites(counterparty_id: str) -> dict:
    """
    Читает полные реквизиты контрагента из МойСклад:
    ИНН, ОГРН, адрес, банковские реквизиты, телефон, email, директор.
    """
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            # Основные реквизиты
            url = f"{MS_BASE}/entity/counterparty/{counterparty_id}"
            async with session.get(url, headers=get_headers()) as resp:
                if resp.status != 200:
                    return {}
                cp = await resp.json()

            # Банковские реквизиты
            accounts_url = f"{MS_BASE}/entity/counterparty/{counterparty_id}/accounts"
            accounts = []
            async with session.get(accounts_url, headers=get_headers()) as resp:
                if resp.status == 200:
                    adata = await resp.json()
                    accounts = adata.get("rows", [])

        # Основной банковский счёт
        bank_data = {}
        for acc in accounts:
            if acc.get("isDefault") or not bank_data:
                bank_data = {
                    "buyer_rs": acc.get("accountNumber", ""),
                    "buyer_bank": acc.get("bankName", ""),
                    "buyer_bik": acc.get("bic", ""),
                    "buyer_ks": acc.get("correspondentAccount", ""),
                }
                if acc.get("isDefault"):
                    break

        # Адрес — сначала юридический, потом фактический
        legal = cp.get("legalAddress", "") or ""
        actual = cp.get("actualAddress", "") or ""

        # Директор из legalFirstName + legalLastName или contactPersons
        director = ""
        lf = cp.get("legalFirstName", "") or ""
        lm = cp.get("legalMiddleName", "") or ""
        ll = cp.get("legalLastName", "") or ""
        if ll:
            # Формируем "Иванов И.И."
            initials = ""
            if lf:
                initials += lf[0] + "."
            if lm:
                initials += lm[0] + "."
            director = f"{ll} {initials}".strip()

        cp_type = cp.get("companyType", "")

        # Для ИП: полное ФИО = legalLastName + legalFirstName + legalMiddleName
        full_fio_ip = ""
        if cp_type == "entrepreneur" and ll:
            full_fio_ip = " ".join(filter(None, [ll, lf, lm]))

        # buyer_legal_title: для ИП берём полное ФИО если legalTitle пустой
        legal_title = cp.get("legalTitle", "") or ""
        if not legal_title and cp_type == "entrepreneur" and full_fio_ip:
            legal_title = f"ИП {full_fio_ip}"
        if not legal_title:
            legal_title = cp.get("name", "")

        result = {
            "buyer_inn": cp.get("inn", "") or "",
            "buyer_ogrn": cp.get("ogrn", "") or cp.get("ogrnip", "") or "",
            "buyer_address": legal or actual,
            "buyer_phone": cp.get("phone", "") or "",
            "buyer_email": cp.get("email", "") or "",
            "buyer_director_name": director,
            "buyer_name": cp.get("name", ""),
            "buyer_legal_title": legal_title,
            # Передаём отдельные поля ФИО — используются в bot.py для сборки реквизитов ИП
            "buyer_last_name": ll,
            "buyer_first_name": lf,
            "buyer_middle_name": lm,
            "href": f"{MS_BASE}/entity/counterparty/{counterparty_id}",
            "id": counterparty_id,
        }
        result.update(bank_data)

        # Представитель для договора
        if director:
            # Определяем должность по типу контрагента
            if cp_type == "entrepreneur":
                result["buyer_representative"] = f"индивидуального предпринимателя {director}"
            else:
                result["buyer_representative"] = f"генерального директора {director}"

        logger.info(f"get_counterparty_requisites: {cp.get('name')} inn={result['buyer_inn']} bank={result.get('buyer_bank','')[:20]}")
        return result

    except Exception as e:
        logger.error(f"get_counterparty_requisites: {e}", exc_info=True)
        return {}

async def find_counterparty_info(query: str) -> list:
    """Находит контрагента и возвращает его теги, менеджера, тип покупателя и баланс."""
    import re as _re

    def _strip_legal(q: str) -> str:
        """Убирает юр.форму из запроса: ООО, ИП, ЗАО, АО, ПАО и т.д."""
        return _re.sub(r'^\s*(ооо|ип|зао|ао|пао|оао|нко|снт)\s+', '', q.strip(), flags=_re.IGNORECASE).strip()

    try:
        async with aiohttp.ClientSession() as session:
            url = f"{MS_BASE}/entity/counterparty"
            rows = []
            stripped = _strip_legal(query)
            queries = [query, query.upper(), query.lower(), query.capitalize()]
            # Добавляем вариант без юр.формы если он отличается
            if stripped and stripped.lower() != query.lower():
                queries += [stripped, stripped.upper(), stripped.lower()]

            for q in queries:
                params = {"filter": f"name~{q}", "limit": 10}
                async with session.get(url, headers=get_headers(), params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rows = data.get("rows", [])
                        if rows:
                            break

            result = []
            for c in rows:
                tags = [t.lower() for t in c.get("tags", [])]

                # Определяем менеджера по тегам
                manager = None
                for tag in tags:
                    for key, name in MANAGER_TAGS.items():
                        if key in tag:
                            manager = name
                            break

                # Определяем тип покупателя
                buyer_type = None
                for tag in tags:
                    for key, label in BUYER_TYPE_TAGS.items():
                        if key in tag:
                            buyer_type = label
                            break

                # Получаем баланс через report
                balance = 0
                try:
                    report_url = f"{MS_BASE}/report/counterparty/{c['id']}"
                    async with session.get(report_url, headers=get_headers()) as r2:
                        if r2.status == 200:
                            rdata = await r2.json()
                            balance = (rdata.get("balance", 0) or 0) / 100
                except Exception:
                    pass

                result.append({
                    "id": c["id"],
                    "name": c.get("name", ""),
                    "tags": c.get("tags", []),
                    "manager": manager,
                    "buyer_type": buyer_type,
                    "balance": balance,
                })
            return result

    except Exception as e:
        logger.error(f"find_counterparty_info error: {e}", exc_info=True)
        return []

def format_counterparty_info(counterparties: list, query: str) -> str:
    """Форматирует информацию о контрагенте."""
    if not counterparties:
        return f"Контрагент «{query}» не найден в МойСклад."

    lines = []
    for c in counterparties:
        name = c["name"]
        parts = [f"*{name}*"]

        if c.get("buyer_type"):
            parts.append(f"Тип: {c['buyer_type']}")

        if c.get("manager"):
            parts.append(f"Менеджер: {c['manager']}")
        else:
            parts.append("Менеджер: не указан")

        balance = c["balance"]
        if balance < 0:
            parts.append(f"Долг перед нами: *{fmt_money(-balance)}*")
        elif balance > 0:
            parts.append(f"Мы должны им: *{fmt_money(balance)}*")
        else:
            parts.append("Баланс нулевой")

        if c.get("tags"):
            parts.append(f"Теги: {', '.join(c['tags'])}")

        lines.append("\n".join(parts))

    return "\n\n".join(lines)

async def get_debtors_by_tag(tag: str, limit: int = 100) -> list:
    """Возвращает должников с определённым тегом (менеджер, хорека, опт и т.д.)"""
    try:
        async with aiohttp.ClientSession() as session:
            # МойСклад не поддерживает filter=tag — грузим всех, фильтруем локально
            url = f"{MS_BASE}/entity/counterparty"
            all_rows = []
            offset = 0
            while True:
                params = {"limit": 100, "offset": offset}
                async with session.get(url, headers=get_headers(), params=params) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()
                    batch = data.get("rows", [])
                    all_rows.extend(batch)
                    if len(batch) < 100:
                        break
                    offset += 100
                    if offset >= 1000:
                        break

            tag_lower = tag.lower()
            rows = [c for c in all_rows if any(tag_lower in t.lower() for t in c.get("tags", []))]
            logger.info(f"get_debtors_by_tag tag='{tag}': {len(rows)}/{len(all_rows)} counterparties match")

            # Получаем балансы через report параллельно
            result = []
            for c in rows:
                try:
                    report_url = f"{MS_BASE}/report/counterparty/{c['id']}"
                    async with session.get(report_url, headers=get_headers()) as r2:
                        balance = 0
                        if r2.status == 200:
                            rdata = await r2.json()
                            balance = (rdata.get("balance", 0) or 0) / 100
                except Exception:
                    balance = 0

                result.append({
                    "id": c["id"],
                    "name": c.get("name", ""),
                    "tags": c.get("tags", []),
                    "balance": balance,
                    "debt": -balance if balance < 0 else 0,
                })

            return result

    except Exception as e:
        logger.error(f"get_debtors_by_tag error: {e}", exc_info=True)
        return []

async def get_clients_by_tag(tag: str, limit: int = 1000) -> list:
    """Возвращает всех контрагентов с тегом (список клиентов менеджера).
    МойСклад не поддерживает filter=tag, поэтому грузим всех и фильтруем локально.
    """
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{MS_BASE}/entity/counterparty"
            all_rows = []
            offset = 0
            while True:
                params = {"limit": 100, "offset": offset}
                async with session.get(url, headers=get_headers(), params=params) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()
                    rows = data.get("rows", [])
                    all_rows.extend(rows)
                    if len(rows) < 100:
                        break
                    offset += 100
                    if offset >= limit:
                        break

            logger.info(f"get_clients_by_tag: loaded {len(all_rows)} total, filtering by tag='{tag}'")
            tag_lower = tag.lower()
            result = []
            for c in all_rows:
                tags = [t.lower() for t in c.get("tags", [])]
                if any(tag_lower in t for t in tags):
                    result.append({
                        "id": c["id"],
                        "name": c.get("name", ""),
                        "tags": c.get("tags", []),
                    })
            logger.info(f"get_clients_by_tag: {len(result)} matching tag='{tag}'")
            return result

    except Exception as e:
        logger.error(f"get_clients_by_tag error: {e}")
        return []

def resolve_tag(query: str) -> str:
    """Определяет тег МойСклад по запросу пользователя."""
    q = query.lower().strip()
    # Менеджеры
    manager_map = {
        "баласанян": "баласанян",
        "карина": "баласанян",
        "леонтьев": "леонтьев",
        "алексей": "леонтьев",
        "мерзлякова": "мерзлякова",
        "елена": "мерзлякова",
        "лена": "мерзлякова",
        "скляр": "скляр",
        "инесса": "скляр",
    }
    # Типы
    type_map = {
        "хорека": "хорека",
        "рестораны": "хорека",
        "ресторан": "хорека",
        "опт": "опт",
        "оптовые": "опт",
        "покупатели": "покупатели",
    }
    for key, tag in {**manager_map, **type_map}.items():
        if key in q:
            return tag
    return q  # вернуть как есть

def format_debtors_by_tag(items: list, tag: str) -> str:
    """Форматирует долги по группе/менеджеру."""
    debtors = [i for i in items if i["debt"] > 0]
    tag_label = tag.capitalize()

    if not debtors:
        return f"✅ По группе *{tag_label}* долгов нет."

    total = sum(d["debt"] for d in debtors)
    lines = [
        f"💰 *Долги по группе {tag_label}* — {len(debtors)} клиентов",
        f"Итого: *{fmt_money(total)}*\n",
    ]
    for d in sorted(debtors, key=lambda x: x["debt"], reverse=True):
        lines.append(f"• {d['name']} — *{fmt_money(d['debt'])}*")
    return "\n".join(lines)

def format_clients_by_tag(items: list, tag: str) -> str:
    """Форматирует список клиентов группы."""
    tag_label = tag.capitalize()
    if not items:
        return f"По группе *{tag_label}* клиентов не найдено."

    lines = [f"📋 *Клиенты группы {tag_label}* — {len(items)} шт.\n"]
    for c in items:
        lines.append(f"• {c['name']}")
    return "\n".join(lines)

async def get_overdue_demands(tag: str = None, query: str = None) -> list:
    """Просроченная дебиторка через Заказы покупателей.
    Грузим все заказы (или конкретного агента), фильтруем локально:
    - paymentPlannedMoment < сегодня
    - payedSum < sum (не оплачен)
    """
    try:
        from datetime import datetime, timezone
        today_dt = datetime.now(timezone.utc)

        async with aiohttp.ClientSession() as session:

            # Если query — найдём href контрагента для фильтра
            agent_filter = ""
            if query:
                cp_url = f"{MS_BASE}/entity/counterparty"
                found_cp = False
                # Варианты запроса: оригинал, с дефисом, без пробелов, части слов
                queries = [query, query.upper(), query.lower(), query.capitalize(),
                           query.replace(" ", "-"), query.replace(" ", "")]
                # Добавляем отдельные слова для поиска
                words = [w for w in query.split() if len(w) >= 3]
                queries.extend(words)

                for q in queries:
                    async with session.get(cp_url, headers=get_headers(),
                                           params={"filter": f"name~{q}", "limit": 5}) as cr:
                        if cr.status == 200:
                            cp_rows = (await cr.json()).get("rows", [])
                            if cp_rows:
                                agent_href = cp_rows[0].get("meta", {}).get("href", "")
                                agent_name_found = cp_rows[0].get("name", "")
                                if agent_href:
                                    agent_filter = f";agent={agent_href}"
                                    found_cp = True
                                    logger.info(f"get_overdue_demands: найден контрагент '{agent_name_found}' по запросу '{q}'")
                                    break
                    if found_cp:
                        break

                if not found_cp:
                    logger.info(f"get_overdue_demands: контрагент '{query}' не найден")
                    return None

            # Грузим ЗАКАЗЫ покупателей — дата оплаты стоит именно в заказе
            url = f"{MS_BASE}/entity/customerorder"
            all_orders = []
            offset = 0

            # Если query задан но контрагент не найден — возвращаем None
            if query and not agent_filter:
                logger.info(f"get_overdue_demands: '{query}' не найден, прерываем")
                return None

            while True:
                params = {
                    "limit": 100,
                    "offset": offset,
                    "expand": "agent,attributes",
                    "order": "moment,asc",
                }
                if agent_filter:
                    params["filter"] = agent_filter.lstrip(";")
                async with session.get(url, headers=get_headers(), params=params) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error(f"customerorder {resp.status}: {body[:200]}")
                        break
                    data = await resp.json()
                    batch = data.get("rows", [])
                    all_orders.extend(batch)
                    logger.info(f"customerorder loaded {len(all_orders)} orders (offset={offset})")
                    if len(batch) < 100:
                        break
                    offset += 100

            logger.info(f"get_overdue_demands: {len(all_orders)} заказов загружено")

            by_agent = {}
            agent_not_overdue = {}

            for order in all_orders:
                # Дата планируемой оплаты — кастомный атрибут заказа
                ppm = ""
                for attr in order.get("attributes", []):
                    if attr.get("name") == "Дата планируемой оплаты":
                        ppm = attr.get("value", "")
                        break
                if not ppm:
                    continue

                try:
                    due_dt = datetime.fromisoformat(ppm.replace(".000", "").replace("Z", ""))
                    if due_dt.tzinfo is None:
                        due_dt = due_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    continue

                total_sum = (order.get("sum", 0) or 0) / 100
                payed_sum = (order.get("payedSum", 0) or 0) / 100
                unpaid = round(max(0, total_sum - payed_sum), 2)
                if unpaid <= 0:
                    continue

                agent = order.get("agent", {})
                agent_id = agent.get("id", "")
                agent_name = agent.get("name", "неизвестно")
                agent_tags = agent.get("tags", [])

                if not agent_id:
                    continue

                # Пропускаем розничных покупателей
                if "розничный покупатель" in agent_name.lower():
                    continue

                # Фильтр по тегу
                if tag:
                    tags_lower = [t.lower() for t in agent_tags]
                    if not any(tag.lower() in t for t in tags_lower):
                        continue

                # Непросроченные — откладываем отдельно
                if due_dt >= today_dt:
                    agent_not_overdue[agent_id] = agent_not_overdue.get(agent_id, 0) + total_sum
                    continue

                # Просроченный заказ
                days_overdue = (today_dt - due_dt).days

                # Определяем менеджера по тегам
                MANAGER_TAG_MAP = {
                    "баласанян": "Карина Баласанян",
                    "скляр": "Инесса Скляр",
                    "мерзлякова": "Елена Мерзлякова",
                            "леонтьев": "Алексей Леонтьев",
                            }
                manager_name = "Без менеджера"
                for t in agent_tags:
                    if t.lower() in MANAGER_TAG_MAP:
                        manager_name = MANAGER_TAG_MAP[t.lower()]
                        break

                if agent_id not in by_agent:
                    by_agent[agent_id] = {
                        "id": agent_id,
                        "name": agent_name,
                        "overdue_sum": 0,
                        "max_days": 0,
                        "demands": [],
                        "manager": manager_name,
                    }
                by_agent[agent_id]["overdue_sum"] += unpaid
                by_agent[agent_id]["max_days"] = max(by_agent[agent_id]["max_days"], days_overdue)
                by_agent[agent_id]["demands"].append({
                    "name": order.get("name", ""),
                    "due": ppm[:10],
                    "unpaid": unpaid,
                    "days": days_overdue,
                })

            result = list(by_agent.values())

            # Пересчитываем просрочку с учётом реального баланса
            # Логика: оплаты покрывают свежие заказы первыми
            filtered = []
            for agent in result:
                try:
                    report_url = f"{MS_BASE}/report/counterparty/{agent['id']}"
                    async with session.get(report_url, headers=get_headers()) as rr:
                        if rr.status == 200:
                            rdata = await rr.json()
                            real_balance = (rdata.get("balance", 0) or 0) / 100
                            real_debt = abs(min(real_balance, 0))
                        else:
                            real_debt = agent["overdue_sum"]

                    if real_debt <= 0:
                        logger.info(f"Excluding {agent['name']}: no real debt")
                        continue

                    # Правильная логика:
                    # просрочка = реальный долг - сумма НЕпросроченных заказов
                    # Непросроченные заказы уже "зарезервированы" под будущие оплаты
                    not_overdue_sum = agent_not_overdue.get(agent["id"], 0)
                    # Из просроченных берём только то что покрыто реальным долгом
                    # после вычета непросроченных
                    effective_overdue = max(0, real_debt - not_overdue_sum)

                    if effective_overdue <= 0:
                        logger.info(f"Excluding {agent['name']}: covered by non-overdue {not_overdue_sum:.2f}")
                        continue

                    # Распределяем effective_overdue по просроченным заказам (старые первые)
                    overdue_only = sorted(
                        [d for d in agent["demands"] if d.get("days", 0) > 0],
                        key=lambda x: x.get("due", "")
                    )
                    remaining = effective_overdue
                    overdue_demands = []
                    overdue_sum = 0
                    for d in overdue_only:
                        if remaining <= 0:
                            break
                        amount = min(remaining, d["unpaid"])
                        remaining -= amount
                        overdue_demands.append({**d, "unpaid": round(amount, 2)})
                        overdue_sum += amount

                    overdue_sum = round(overdue_sum, 2)
                    if overdue_sum <= 0:
                        logger.info(f"Excluding {agent['name']}: overdue covered by payments real_debt={real_debt:.2f}")
                        continue

                    agent["overdue_sum"] = overdue_sum
                    agent["demands"] = overdue_demands
                    agent["max_days"] = max((d["days"] for d in overdue_demands), default=0)
                    filtered.append(agent)
                    logger.info(f"{agent['name']}: real_debt={real_debt:.2f} overdue={overdue_sum:.2f}")

                except Exception as e:
                    logger.warning(f"balance check failed for {agent.get('name')}: {e}")
                    filtered.append(agent)

            filtered.sort(key=lambda x: x["overdue_sum"], reverse=True)
            logger.info(f"get_overdue_demands: {len(filtered)} agents with overdue debt (after balance check)")
            return filtered

    except Exception as e:
        logger.error(f"get_overdue_demands error: {e}", exc_info=True)
        return []

def format_overdue_summary(items: list) -> str:
    """Краткий формат ПДЗ: итог + по менеджерам со списком клиентов."""
    if not items:
        return "✅ Просроченных долгов нет."

    total_all = sum(c["overdue_sum"] for c in items)
    lines = [
        f"⚠️ *Просроченная дебиторка* — {len(items)} клиентов · *{fmt_money(total_all)}*\n"
    ]

    by_manager = {}
    for c in items:
        manager = c.get("manager", "Без менеджера")
        if manager not in by_manager:
            by_manager[manager] = {"total": 0, "clients": []}
        by_manager[manager]["total"] += c["overdue_sum"]
        by_manager[manager]["clients"].append(c)

    for manager, data in sorted(by_manager.items(), key=lambda x: x[1]["total"], reverse=True):
        lines.append(f"👤 *{manager}* — {fmt_money(data['total'])}")
        for c in sorted(data["clients"], key=lambda x: x["overdue_sum"], reverse=True):
            lines.append(f"   • {c['name']} — {fmt_money(c['overdue_sum'])}")
        lines.append("")

    return "\n".join(lines).rstrip()

def format_overdue_demands(items: list, tag: str = None) -> str:
    """Форматирует просроченную дебиторку."""
    if not items:
        label = f" по *{tag.capitalize()}*" if tag else ""
        return f"✅ Просроченных долгов{label} нет."

    total = sum(i["overdue_sum"] for i in items)
    label = f" — {tag.capitalize()}" if tag else ""
    lines = [
        f"⚠️ *Просроченная дебиторка{label}*",
        f"{len(items)} клиентов · Итого: *{fmt_money(total)}*\n",
    ]
    for c in items:
        header = f"🔴 *{c['name']}* — {fmt_money(c['overdue_sum'])}"
        lines.append(header)
        demands = c.get("demands", [])
        for d in demands:
            due_fmt = '.'.join(reversed(d['due'].split('-'))) if d['due'] else d['due']
            days = d.get("days", 0)
            days_str = f" · {days} дн." if days > 0 else ""
            lines.append(f"   └ {d['name']} · {due_fmt} · {fmt_money(d['unpaid'])}{days_str}")
        lines.append("")

    return "\n".join(lines).rstrip()

def format_debt_reminder(client: dict) -> str:
    """Готовит текст напоминания клиенту об оплате."""
    demands = client.get("demands", [])
    lines = [
        "Добрый день!",
        "",
        'Напоминаем о наличии просроченной задолженности перед компанией АО "ФИШ ТУ БИЗНЕС":',
        "",
    ]
    for d in demands:
        due_fmt = '.'.join(reversed(d['due'].split('-'))) if d['due'] else d['due']
        lines.append(f"• Заказ {d['name']} от {due_fmt} — {fmt_money(d['unpaid'])}")
    lines += [
        "",
        f"Итого к оплате: {fmt_money(client['overdue_sum'])}",
        "",
        "Просим произвести оплату в ближайшее время.",
    ]
    return "\n".join(lines)

def format_reminders_for_manager(items: list, manager_display: str) -> str:
    """Форматирует пакет напоминаний для менеджера — по одному на клиента."""
    if not items:
        return "✅ Просроченных клиентов нет — напоминания не нужны."

    lines = [
        f"📋 *Напоминания об оплате — {manager_display}*",
        f"{len(items)} клиентов · скопируй и отправь каждому\n",
    ]
    for c in sorted(items, key=lambda x: x["overdue_sum"], reverse=True):
        lines.append(f"━━━━━━━━━━━━━━━━━━━━")
        lines.append(f"*{c['name']}* — {fmt_money(c['overdue_sum'])}")
        lines.append("```")
        lines.append(format_debt_reminder(c))
        lines.append("```")
    return "\n".join(lines)

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
        price = p.get("sale_price") or p.get("price")
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
        price = p.get("sale_price") or p.get("price")
        icon = "🟢" if stock > 0 else "🔴"
        price_str = f"{price:,.0f} руб" if price else "цена не указана"
        lines.append(f"{icon} {p['name']} — {price_str}")

    return "\n".join(lines)

async def get_counterparties_by_product(product_query: str, period_days: int = 180) -> list:
    """
    Находит всех контрагентов которые покупали товар по названию.
    Возвращает список: [{"id": ..., "name": ..., "phone": ...}]
    """
    import aiohttp
    from datetime import datetime, timedelta

    product_lower = product_query.lower()
    found = {}  # id -> {name, phone}
    date_from = (datetime.now() - timedelta(days=period_days)).strftime("%Y-%m-%d %H:%M:%S")

    try:
        async with aiohttp.ClientSession() as session:
            offset = 0
            limit = 100
            while True:
                url = f"{MS_BASE}/entity/customerorder"
                params = {
                    "limit": limit,
                    "offset": offset,
                    "expand": "agent,positions.assortment",
                    "filter": f"moment>{date_from}",
                }
                async with session.get(url, headers=get_headers(), params=params) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()

                rows = data.get("rows", [])
                if not rows:
                    break

                for order in rows:
                    agent = order.get("agent", {})
                    agent_id = agent.get("id", "")
                    agent_name = agent.get("name", "")

                    if not agent_id or not agent_name:
                        continue
                    if "розничный покупатель" in agent_name.lower():
                        continue
                    if agent_id in found:
                        continue

                    positions = order.get("positions", {})
                    pos_rows = positions.get("rows", []) if isinstance(positions, dict) else []

                    for pos in pos_rows:
                        assortment = pos.get("assortment", {})
                        pos_name = assortment.get("name", "").lower()
                        if product_lower in pos_name:
                            found[agent_id] = {
                                "name": agent_name,
                                "phone": None,
                                "tags": agent.get("tags", []),
                            }
                            break

                offset += limit
                if len(rows) < limit:
                    break

            # Загружаем телефоны контрагентов
            for agent_id in list(found.keys()):
                try:
                    url = f"{MS_BASE}/entity/counterparty/{agent_id}"
                    async with session.get(url, headers=get_headers()) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            # Телефон может быть в phone или actualAddress
                            phone = data.get("phone", "")
                            if not phone:
                                # Ищем в контактах
                                contacts_data = data.get("contactpersons", {})
                                if isinstance(contacts_data, dict):
                                    cp_rows = contacts_data.get("rows", [])
                                    for cp in cp_rows:
                                        if cp.get("phone"):
                                            phone = cp["phone"]
                                            break
                            if phone:
                                found[agent_id]["phone"] = phone
                except Exception:
                    pass

        logger.info(f"get_counterparties_by_product: '{product_query}' за {period_days} дней → {len(found)} контрагентов")

    except Exception as e:
        logger.error(f"get_counterparties_by_product: {e}")

    return list(found.values())

async def get_buyers_by_product(product_query: str, period_days: int = 180) -> list:
    """
    Быстрый поиск покупателей через отчёт "Прибыльность по покупателям".
    Фильтрует по товару и основному складу за указанный период.
    Возвращает список: [{"id": ..., "name": ..., "href": ...}]
    """
    import aiohttp
    from datetime import datetime, timedelta

    STORE_ID = os.getenv("MS_STORE_ID", "0044d71e-9a9a-11f0-0a80-03a90002743d")
    STORE_HREF = f"{MS_BASE}/entity/store/{STORE_ID}"

    date_to = datetime.now()
    date_from = date_to - timedelta(days=period_days)
    moment_from = date_from.strftime("%Y-%m-%d %H:%M:%S")
    moment_to = date_to.strftime("%Y-%m-%d %H:%M:%S")

    # 1. Ищем товар по названию
    products = await search_products(product_query, limit=5)
    if not products:
        logger.warning(f"get_buyers_by_product: товар '{product_query}' не найден")
        return []

    # Берём первый подходящий товар
    product = products[0]
    product_id = product.get("id")
    product_name = product.get("name", product_query)
    if not product_id:
        logger.warning(f"get_buyers_by_product: нет ID у товара '{product_name}'")
        return []

    product_href = f"{MS_BASE}/entity/product/{product_id}"
    logger.info(f"get_buyers_by_product: товар '{product_name}' id={product_id}")

    # 2. Запрашиваем отчёт прибыльности по покупателям
    buyers = []
    offset = 0
    limit = 100

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                url = f"{MS_BASE}/report/profit/bycounterparty"
                params = {
                    "momentFrom": moment_from,
                    "momentTo": moment_to,
                    "filter": f"store={STORE_HREF};product={product_href}",
                    "limit": limit,
                    "offset": offset,
                }
                async with session.get(url, headers=get_headers(), params=params) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        logger.error(f"get_buyers_by_product: {resp.status} {text[:200]}")
                        break
                    data = await resp.json()

                rows = data.get("rows", [])
                total = data.get("meta", {}).get("size", 0)

                for row in rows:
                    cp = row.get("counterparty", {})
                    cp_name = cp.get("name", "")
                    cp_href = cp.get("meta", {}).get("href", "")
                    cp_id = cp_href.split("/")[-1] if cp_href else ""
                    if cp_name and cp_id:
                        buyers.append({
                            "id": cp_id,
                            "name": cp_name,
                            "href": cp_href,
                        })

                offset += limit
                if offset >= total or len(rows) < limit:
                    break

        logger.info(f"get_buyers_by_product: '{product_name}' за {period_days} дней → {len(buyers)} покупателей")

        # Загружаем теги для каждого покупателя
        async with aiohttp.ClientSession() as session:
            for b in buyers:
                try:
                    href = b.get("href", "")
                    if href:
                        async with session.get(href, headers=get_headers()) as r:
                            if r.status == 200:
                                cp_data = await r.json()
                                b["tags"] = cp_data.get("tags", [])
                except Exception:
                    b["tags"] = []

    except Exception as e:
        logger.error(f"get_buyers_by_product: {e}")

    return {"buyers": buyers, "product_name": product_name}

async def get_counterparty_phones(buyers: list) -> list:
    """
    Получает телефоны контрагентов из МойСклад.
    buyers — список dict с полями id, name, href.
    Возвращает список {name, phone, id}.
    """
    result = []
    try:
        async with aiohttp.ClientSession() as session:
            for b in buyers:
                href = b.get("href", "")
                if not href:
                    result.append({"name": b.get("name", ""), "phone": None, "id": b.get("id", "")})
                    continue
                try:
                    async with session.get(href, headers=get_headers()) as resp:
                        if resp.status != 200:
                            result.append({"name": b.get("name", ""), "phone": None, "id": b.get("id", "")})
                            continue
                        data = await resp.json()
                    # Телефон в поле phone (строка) или в массиве phones
                    phone = data.get("phone", "") or ""
                    if not phone:
                        phones = data.get("phones", [])
                        if phones:
                            phone = phones[0].get("value", "")
                    logger.info(f"get_counterparty_phones: {data.get('name')} raw_phone='{phone}'")
                    # Нормализуем — оставляем только цифры
                    phone_clean = "".join(c for c in phone if c.isdigit())
                    if len(phone_clean) == 11 and phone_clean.startswith("8"):
                        phone_clean = "7" + phone_clean[1:]
                    elif len(phone_clean) == 10:
                        phone_clean = "7" + phone_clean
                    elif len(phone_clean) == 11 and phone_clean.startswith("7"):
                        pass  # уже правильный формат
                    else:
                        phone_clean = None  # неизвестный формат
                    result.append({
                        "name": data.get("name", b.get("name", "")),
                        "phone": phone_clean if phone_clean else None,
                        "id": b.get("id", ""),
                        "chat_type": "whatsapp",
                    })
                except Exception as e:
                    logger.warning(f"get_counterparty_phones: {b.get('name')} error: {e}")
                    result.append({"name": b.get("name", ""), "phone": None, "id": b.get("id", "")})
    except Exception as e:
        logger.error(f"get_counterparty_phones: {e}")
    return result

async def check_order_prices(order_href: str) -> list:
    """
    Проверяет цены в заказе покупателя.
    Пропускает заказы в финальных статусах.
    """
    SKIP_STATES = {
        "005f3651-9a9a-11f0-0a80-03a900027474",  # Согласован
        "267fdfbc-a2a7-11f0-0a80-0f640047fcaa",  # Собирается
        "70999fb0-a2b6-11f0-0a80-1c830049f367",  # Собран без охл
        "005f376a-9a9a-11f0-0a80-03a900027475",  # Собран
        "ee088f23-df45-11f0-0a80-1670003a954a",  # ИЗМЕНЕН
        "6edbfa00-dfdb-11f0-0a80-104e0008a4d4",  # Документы готовы
        "005f383a-9a9a-11f0-0a80-03a900027476",  # Отгружен
        "005f3938-9a9a-11f0-0a80-03a900027478",  # Возврат
        "005f398e-9a9a-11f0-0a80-03a900027479",  # Отменен
    }
    import aiohttp
    alerts = []

    try:
        async with aiohttp.ClientSession() as session:

            # 1. Загружаем заказ с позициями и контрагентом
            async with session.get(
                order_href,
                headers=get_headers(),
                params={"expand": "agent,positions.assortment,owner,state"}
            ) as resp:
                if resp.status != 200:
                    logger.error(f"check_order_prices: не удалось загрузить заказ {order_href}")
                    return []
                order = await resp.json()

            # Пропускаем если заказ в финальном статусе
            state = order.get("state", {})
            if state.get("id") in SKIP_STATES:
                logger.info(f"check_order_prices: заказ в статусе '{state.get('name', '')}' — пропускаем")
                return []

            agent = order.get("agent", {})
            agent_name = agent.get("name", "неизвестно")
            agent_id = agent.get("id", "")
            order_name = order.get("name", "")

            # Менеджер (владелец заказа)
            owner = order.get("owner", {})
            manager_name = owner.get("name", "не указан")

            # 2. Определяем тег контрагента (хорека или опт)
            agent_tags = agent.get("tags", [])
            tags_lower = [t.lower() for t in agent_tags]

            if "хорека" in tags_lower:
                price_type_name = "Цена продажи"
                client_type = "хорека"
            elif "опт" in tags_lower:
                price_type_name = "Цена опт"
                client_type = "опт"
            else:
                # Нет тега — не проверяем
                logger.info(f"check_order_prices: контрагент '{agent_name}' без тега хорека/опт — пропускаем")
                return []

            logger.info(f"check_order_prices: заказ {order_name}, клиент '{agent_name}' ({client_type}), тип цены: {price_type_name}")

            # 3. Проверяем позиции заказа
            positions = order.get("positions", {})
            pos_rows = positions.get("rows", []) if isinstance(positions, dict) else []

            for pos in pos_rows:
                assortment = pos.get("assortment", {})
                product_name = assortment.get("name", "")
                product_id = assortment.get("id", "")
                order_price = pos.get("price", 0) / 100  # цена в копейках

                if not product_id or order_price <= 0:
                    continue

                # 4. Загружаем эталонную цену из карточки товара
                product_url = f"{MS_BASE}/entity/product/{product_id}"
                async with session.get(product_url, headers=get_headers()) as resp:
                    if resp.status != 200:
                        continue
                    product_data = await resp.json()

                # Ищем нужный тип цены
                sale_prices = product_data.get("salePrices", [])
                min_price = None
                for sp in sale_prices:
                    pt = sp.get("priceType", {})
                    if pt.get("name", "") == price_type_name:
                        min_price = sp.get("value", 0) / 100
                        break

                if min_price is None or min_price <= 0:
                    continue  # Цена не установлена — пропускаем

                # 5. Сравниваем
                if order_price < min_price:
                    diff = min_price - order_price
                    alerts.append(
                        f"📦 *{agent_name}* | Заказ *{order_name}*\n"
                        f"Менеджер: {manager_name}\n\n"
                        f"*{product_name}*\n"
                        f"Цена в заказе: {order_price:,.0f} руб | Минимальная ({client_type}): {min_price:,.0f} руб\n"
                        f"*Занижена на: {diff:,.0f} руб*"
                    )

    except Exception as e:
        logger.error(f"check_order_prices: {e}")

    return alerts

async def get_order_manager(order_href: str) -> dict:
    """
    Возвращает имя и Telegram ID менеджера-владельца заказа.
    Маппинг имён на Telegram ID берётся из переменной окружения MANAGER_TG_IDS
    формат: "Иванов Андрей:123456789,Баласанян Карина:987654321"
    """
    import aiohttp
    import re

    manager_info = {"name": "", "telegram_id": None}

    # Маппинг имя → telegram_id из переменной окружения
    mapping_str = os.getenv("MANAGER_TG_IDS", "")
    mapping = {}
    for item in mapping_str.split(","):
        item = item.strip()
        if ":" in item:
            name, tg_id = item.rsplit(":", 1)
            try:
                mapping[name.strip().lower()] = int(tg_id.strip())
            except ValueError:
                pass

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(order_href, headers=get_headers(), params={"expand": "owner"}) as resp:
                if resp.status != 200:
                    return manager_info
                order = await resp.json()

        owner = order.get("owner", {})
        owner_name = owner.get("name", "")
        manager_info["name"] = owner_name

        # Ищем telegram_id по имени (частичное совпадение)
        owner_lower = owner_name.lower()
        for mapped_name, tg_id in mapping.items():
            if mapped_name in owner_lower or owner_lower in mapped_name:
                manager_info["telegram_id"] = tg_id
                break

    except Exception as e:
        logger.error(f"get_order_manager: {e}")

    return manager_info

async def get_order_positions_snapshot(order_href: str) -> frozenset:
    """
    Возвращает frozenset позиций заказа в виде (product_id, price).
    Используется для отслеживания изменений цен и номенклатуры.
    """
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                order_href,
                headers=get_headers(),
                params={"expand": "positions.assortment"}
            ) as resp:
                if resp.status != 200:
                    return frozenset()
                order = await resp.json()

        positions = order.get("positions", {})
        pos_rows = positions.get("rows", []) if isinstance(positions, dict) else []

        snapshot = frozenset(
            (
                pos.get("assortment", {}).get("id", ""),
                pos.get("price", 0),
                pos.get("quantity", 0),
            )
            for pos in pos_rows
        )
        return snapshot

    except Exception as e:
        logger.error(f"get_order_positions_snapshot: {e}")
        return frozenset()

async def get_counterparty_debt(counterparty_id: str) -> dict:
    """
    Просрочка контрагента: правильная логика через текущий баланс.
    1. Берём текущий долг (balance) из отчёта МойСклад
    2. Берём все заказы с датой оплаты, сортируем от свежих к старым
    3. Вычитаем долг из заказов начиная со свежих — они покрыты последними оплатами
    4. Просроченные = только заказы у которых дата прошла И долг не покрыт
    """
    import aiohttp
    from datetime import datetime, timezone

    today_dt = datetime.now(timezone.utc)

    try:
        async with aiohttp.ClientSession() as session:
            cp_href = f"{MS_BASE}/entity/counterparty/{counterparty_id}"

            # 1. Текущий баланс (отрицательный = клиент должен нам)
            async with session.get(
                f"{MS_BASE}/report/counterparty/{counterparty_id}",
                headers=get_headers()
            ) as resp:
                if resp.status != 200:
                    return {}
                rdata = await resp.json()

            balance = (rdata.get("balance", 0) or 0) / 100
            # balance < 0 означает что клиент должен нам
            total_debt = abs(min(balance, 0))
            logger.info(f"get_counterparty_debt: id={counterparty_id} balance={balance} total_debt={total_debt}")

            if total_debt <= 0:
                return {}

            # 2. Все заказы с датой планируемой оплаты
            async with session.get(
                f"{MS_BASE}/entity/customerorder",
                headers=get_headers(),
                params={
                    "filter": f"agent={cp_href}",
                    "expand": "attributes",
                    "limit": 200,
                    "order": "moment,desc",  # от свежих к старым
                }
            ) as resp:
                if resp.status != 200:
                    return {"debt": total_debt, "overdue_days": 0}
                data = await resp.json()

        orders = data.get("rows", [])

        # 3. Собираем заказы с датой оплаты и неоплаченным остатком
        order_list = []
        for order in orders:
            ppm = ""
            for attr in order.get("attributes", []):
                if attr.get("name") == "Дата планируемой оплаты":
                    ppm = attr.get("value", "")
                    break
            if not ppm:
                continue

            try:
                due_dt = datetime.fromisoformat(ppm.replace(".000", "").replace("Z", ""))
                if due_dt.tzinfo is None:
                    due_dt = due_dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue

            total_sum = (order.get("sum", 0) or 0) / 100
            payed_sum = (order.get("payedSum", 0) or 0) / 100
            unpaid = round(total_sum - payed_sum, 2)
            if unpaid <= 0:
                continue

            order_list.append({
                "name": order.get("name", ""),
                "due_dt": due_dt,
                "unpaid": unpaid,
                "overdue": due_dt < today_dt,
            })

        # Сортируем: свежие (большая дата оплаты) первыми
        order_list.sort(key=lambda x: x["due_dt"], reverse=True)

        # 4. Вычитаем текущий долг начиная со свежих заказов
        # Логика: оплаты покрывают самые свежие заказы первыми
        remaining_debt = total_debt
        overdue_sum = 0
        max_overdue_days = 0

        for o in order_list:
            if remaining_debt <= 0:
                break
            covered = min(remaining_debt, o["unpaid"])
            remaining_debt -= covered
            uncovered = round(o["unpaid"] - covered, 2)

            if uncovered > 0 and o["overdue"]:
                days = (today_dt - o["due_dt"]).days
                overdue_sum += uncovered
                if days > max_overdue_days:
                    max_overdue_days = days
                logger.info(f"get_counterparty_debt: просрочен {o['name']} due={o['due_dt'].date()} uncovered={uncovered} days={days}")

        overdue_sum = round(overdue_sum, 2)
        logger.info(f"get_counterparty_debt: overdue_sum={overdue_sum} max_days={max_overdue_days}")

        if overdue_sum <= 0:
            return {}

        return {"debt": overdue_sum, "overdue_days": max_overdue_days}

    except Exception as e:
        logger.error(f"get_counterparty_debt: {e}", exc_info=True)
        return {}

async def set_order_state(order_id: str, state_id: str) -> bool:
    """Меняет статус заказа покупателя."""
    import aiohttp
    url = f"{MS_BASE}/entity/customerorder/{order_id}"
    payload = {
        "state": {
            "meta": {
                "href": f"{MS_BASE}/entity/customerorder/metadata/states/{state_id}",
                "type": "state",
                "mediaType": "application/json"
            }
        }
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(url, headers=get_headers(), json=payload) as resp:
                if resp.status == 200:
                    logger.info(f"Статус заказа {order_id} изменён на {state_id}")
                    return True
                else:
                    text = await resp.text()
                    logger.error(f"set_order_state: {resp.status} {text[:200]}")
                    return False
    except Exception as e:
        logger.error(f"set_order_state: {e}")
        return False

# Расписание доставки по МО
# Каждый город — список вариантов написания (все в нижнем регистре)
DELIVERY_SCHEDULE_RAW = {
    0: {  # Понедельник
        "Звенигород": ["звенигород", "звенигородский"],
        "Истра": ["истра", "истринский"],
        "Солнечногорск": ["солнечногорск", "солнечногорский"],
    },
    1: {  # Вторник
        "Королёв": ["королёв", "королев", "королевский"],
        "Мытищи": ["мытищи", "мытищинский"],
        "Одинцово": ["одинцово", "одинцовский"],
        "Подольск": ["подольск", "подольский"],
        "Серпухов": ["серпухов", "серпуховский"],
        "Чехов": ["чехов", "чеховский"],
        "Щелково": ["щелково", "щёлково", "щелковский", "щёлковский"],
    },
    2: {  # Среда
        "Домодедово": ["домодедово", "домодедовский"],
        "Королёв": ["королёв", "королев", "королевский"],
        "Мытищи": ["мытищи", "мытищинский"],
        "Орехово-Зуево": ["орехово-зуево", "орехово зуево", "ореховозуево", "орехово-зуевский"],
        "Павловский Посад": ["павловский посад", "павлово-посадский", "павловопосадский"],
        "Сергиев Посад": ["сергиев посад", "сергиево-посадский", "сергиевопосадский"],
        "Щелково": ["щелково", "щёлково", "щелковский", "щёлковский"],
        "Красноармейск": ["красноармейск"],
        "Пушкино": ["пушкино", "пушкинский"],
    },
    3: {  # Четверг
        "Апрелевка": ["апрелевка", "апрелевский"],
        "Королёв": ["королёв", "королев", "королевский"],
        "Мытищи": ["мытищи", "мытищинский"],
        "Наро-Фоминск": ["наро-фоминск", "наро фоминск", "нарофоминск", "наро-фоминский"],
        "Щелково": ["щелково", "щёлково", "щелковский", "щёлковский"],
    },
    4: {  # Пятница
        "Егорьевск": ["егорьевск", "егорьевский"],
        "Воскресенск": ["воскресенск", "воскресенский"],
        "Королёв": ["королёв", "королев", "королевский"],
        "Мытищи": ["мытищи", "мытищинский"],
        "Щелково": ["щелково", "щёлково", "щелковский", "щёлковский"],
        "Каширское шоссе": ["каширское шоссе", "кашира", "каширский"],
    },
}

WEEKDAYS_RU = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
WEEKDAYS_RU_IN = ["понедельник", "вторник", "среду", "четверг", "пятницу", "субботу", "воскресенье"]

# Плоский словарь: вариант написания → (канонический город, список дней)
def _build_city_index():
    index = {}  # keyword → {"canonical": str, "days": set}
    for day, cities in DELIVERY_SCHEDULE_RAW.items():
        for canonical, variants in cities.items():
            for v in variants:
                if v not in index:
                    index[v] = {"canonical": canonical, "days": set()}
                index[v]["days"].add(day)
    return index

_CITY_INDEX = _build_city_index()
# Все города МО из расписания (все варианты написания)
ALL_MO_CITIES = list(_CITY_INDEX.keys())

async def check_delivery_schedule(address: str, delivery_date_str: str) -> dict:
    """
    Проверяет соответствие адреса доставки и дня недели расписанию.
    Сначала текстовый поиск, потом геокодирование через Яндекс.
    Московские адреса всегда OK.
    """
    if not address or not delivery_date_str:
        return {"ok": True}

    address_lower = address.lower()

    # Московские адреса — не проверяем
    if "москва" in address_lower or "moscow" in address_lower:
        return {"ok": True}

    # Определяем день недели даты отгрузки
    try:
        from datetime import datetime
        dt = datetime.fromisoformat(delivery_date_str[:10])
        weekday = dt.weekday()
    except Exception:
        return {"ok": True}

    # Шаг 1: текстовый поиск по известным городам
    found_keyword = None
    for keyword in sorted(_CITY_INDEX.keys(), key=len, reverse=True):
        if keyword in address_lower:
            found_keyword = keyword
            break

    if found_keyword:
        city_info = _CITY_INDEX[found_keyword]
        canonical = city_info["canonical"]
        allowed_days_nums = city_info["days"]
        if weekday in allowed_days_nums:
            return {"ok": True}
        allowed_days = [WEEKDAYS_RU[d] for d in sorted(allowed_days_nums)]
        return {
            "ok": False,
            "city": canonical,
            "date": delivery_date_str[:10],
            "weekday": WEEKDAYS_RU[weekday],
            "allowed_days": allowed_days,
        }

    # Шаг 2: геокодирование — ищем ближайший город из расписания
    coords = await geocode_address(address)
    if not coords:
        return {"ok": True}  # Не смогли геокодировать — не блокируем

    lat, lon = coords

    # Адреса ближе 35 км от центра Москвы — возим в любой день
    dist_from_moscow = _haversine(lat, lon, 55.7558, 37.6173)
    if dist_from_moscow < 35:
        return {"ok": True}

    # Ищем ближайший город в радиусе DELIVERY_RADIUS_KM
    nearest_city = None
    nearest_dist = float("inf")
    for city, (clat, clon) in DELIVERY_CITIES_COORDS.items():
        dist = _haversine(lat, lon, clat, clon)
        if dist < nearest_dist:
            nearest_dist = dist
            nearest_city = city

    if nearest_dist > DELIVERY_RADIUS_KM:
        return {"ok": True}  # Далеко от всех наших направлений

    # Нашли ближайший город — ищем его в индексе
    found_keyword = None
    for keyword, info in _CITY_INDEX.items():
        if info["canonical"] == nearest_city:
            found_keyword = keyword
            break

    if not found_keyword:
        return {"ok": True}

    city_info = _CITY_INDEX[found_keyword]
    allowed_days_nums = city_info["days"]
    if weekday in allowed_days_nums:
        return {"ok": True}

    allowed_days = [WEEKDAYS_RU[d] for d in sorted(allowed_days_nums)]
    return {
        "ok": False,
        "city": nearest_city,
        "date": delivery_date_str[:10],
        "weekday": WEEKDAYS_RU[weekday],
        "allowed_days": allowed_days,
        "distance_km": round(nearest_dist, 1),
    }

async def get_reconciliation_data(counterparty_id: str, date_from: str, date_to: str) -> dict:
    """
    Получает данные для акта сверки по контрагенту за период.
    date_from, date_to: 'YYYY-MM-DD'
    Возвращает: {
        counterparty_name, opening_balance,
        rows: [{date, doc_type, doc_number, debit, credit}],
        closing_balance
    }
    """
    import aiohttp
    from datetime import datetime

    dt_from = f"{date_from} 00:00:00"
    dt_to   = f"{date_to} 23:59:59"
    cp_href = f"{MS_BASE}/entity/counterparty/{counterparty_id}"

    try:
        async with aiohttp.ClientSession() as session:
            # Имя контрагента
            async with session.get(f"{cp_href}", headers=get_headers()) as r:
                cp = await r.json() if r.status == 200 else {}
            cp_name = cp.get("name", "")

            rows = []

            async def fetch_docs(entity, label, is_payment=False, is_return=False):
                url = f"{MS_BASE}/entity/{entity}"
                params = {
                    "filter": f"agent={cp_href};moment>={dt_from};moment<={dt_to}",
                    "limit": 200,
                    "order": "moment,asc",
                }
                async with session.get(url, headers=get_headers(), params=params) as r:
                    if r.status != 200:
                        return
                    data = await r.json()
                    for doc in data.get("rows", []):
                        date_str = doc.get("moment", "")[:10]
                        num = doc.get("name", doc.get("id", ""))
                        amount = round((doc.get("sum", 0) or 0) / 100, 2)
                        if amount <= 0:
                            continue
                        rows.append({
                            "date": date_str,
                            "doc_type": label,
                            "doc_number": num,
                            "amount": amount,
                            # is_payment=True → кредит (уменьшает долг клиента)
                            # is_return=True → тоже кредит (возврат уменьшает долг)
                            "is_payment": is_payment or is_return,
                        })

            # Дебет покупателя (он должен нам)
            await fetch_docs("demand",    "Отгрузка",      is_payment=False)
            await fetch_docs("invoiceout","Счёт",           is_payment=False)

            # Кредит покупателя (он платит или мы возвращаем)
            await fetch_docs("paymentin", "Оплата б/н",    is_payment=True)
            await fetch_docs("cashin",    "Оплата нал.",   is_payment=True)
            await fetch_docs("salesreturn","Возврат",      is_return=True)
            await fetch_docs("paymentout","Выплата",       is_payment=True)
            await fetch_docs("cashout",   "Выплата нал.",  is_payment=True)

            # Убираем счета из расчёта сальдо (они информационные)
            rows_for_calc = [r for r in rows if r["doc_type"] != "Счёт"]

            # Сортируем по дате
            rows.sort(key=lambda x: x["date"])

            # Считаем сальдо
            debit_total  = round(sum(r["amount"] for r in rows_for_calc if not r["is_payment"]), 2)
            credit_total = round(sum(r["amount"] for r in rows_for_calc if r["is_payment"]), 2)

            # Сверяем с реальным балансом МойСклад
            async with session.get(
                f"{MS_BASE}/report/counterparty/{counterparty_id}",
                headers=get_headers()
            ) as rb:
                if rb.status == 200:
                    rb_data = await rb.json()
                    real_balance = (rb_data.get("balance", 0) or 0) / 100
                    # balance < 0 → клиент должен нам
                    real_debt = round(abs(min(real_balance, 0)), 2)
                else:
                    real_debt = round(debit_total - credit_total, 2)

            closing = real_debt  # используем реальный баланс из МойСклад

            # ИНН и адрес контрагента
            cp_inn = cp.get("inn", "")
            legal_addr = cp.get("legalAddress", "") or ""
            actual_addr = cp.get("actualAddress", "") or ""
            cp_address = legal_addr or actual_addr

            return {
                "counterparty_name": cp_name,
                "buyer_inn": cp_inn,
                "buyer_address": cp_address,
                "date_from": date_from,
                "date_to": date_to,
                "rows": rows,
                "debit_total": debit_total,
                "credit_total": credit_total,
                "closing_balance": closing,
            }

    except Exception as e:
        logger.error(f"get_reconciliation_data: {e}", exc_info=True)
        return {}

async def get_aging_clients(days: int = 50) -> list:
    """
    Возвращает клиентов у которых последняя отгрузка была 50+ дней назад.
    Логика: берём все отгрузки за последние 50 дней — у кого нет = стареющий.
    """
    import aiohttp
    from datetime import datetime, timezone, timedelta

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = today - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%d 00:00:00")
    today_str = today.strftime("%Y-%m-%d 23:59:59")

    # Дата начала истории (берём последние 2 года для нахождения активных клиентов)
    history_from = (today - timedelta(days=730)).strftime("%Y-%m-%d 00:00:00")

    try:
        async with aiohttp.ClientSession() as session:

            # 1. Клиенты у которых ЕСТЬ отгрузки за последние 50 дней — исключаем их
            recent_params = {
                "filter": f"moment>={cutoff_str};moment<={today_str}",
                "expand": "agent",
                "limit": 1000,
            }
            async with session.get(f"{MS_BASE}/entity/demand",
                                   headers=get_headers(), params=recent_params) as resp:
                if resp.status != 200:
                    return []
                recent_data = await resp.json()

            active_ids = set()
            for doc in recent_data.get("rows", []):
                agent = doc.get("agent", {})
                agent_href = agent.get("meta", {}).get("href", "")
                agent_id = agent_href.split("/")[-1] if agent_href else agent.get("id", "")
                if agent_id:
                    active_ids.add(agent_id)

            logger.info(f"get_aging_clients: активных за {days} дней = {len(active_ids)}")

            # 2. Все клиенты с отгрузками за последние 2 года
            all_agents = {}
            offset = 0
            while True:
                hist_params = {
                    "filter": f"moment>={history_from};moment<={cutoff_str}",
                    "expand": "agent",
                    "limit": 200,
                    "offset": offset,
                    "order": "moment,desc",
                }
                async with session.get(f"{MS_BASE}/entity/demand",
                                       headers=get_headers(), params=hist_params) as resp:
                    if resp.status != 200:
                        break
                    hist_data = await resp.json()
                    rows = hist_data.get("rows", [])
                    for doc in rows:
                        agent = doc.get("agent", {})
                        agent_href = agent.get("meta", {}).get("href", "")
                        agent_id = agent_href.split("/")[-1] if agent_href else agent.get("id", "")
                        if not agent_id or agent_id in active_ids:
                            continue
                        if "розничный покупатель" in agent.get("name", "").lower():
                            continue
                        if agent_id not in all_agents:
                            doc_date = doc.get("moment", "")[:10]
                            days_ago = (today.date() - datetime.strptime(doc_date, "%Y-%m-%d").date()).days
                            all_agents[agent_id] = {
                                "id": agent_id,
                                "name": agent.get("name", ""),
                                "tags": agent.get("tags", []),
                                "last_demand_date": doc_date,
                                "days": days_ago,
                            }
                    if len(rows) < 200:
                        break
                    offset += 200

            result = list(all_agents.values())
            result.sort(key=lambda x: x["days"], reverse=True)

            # Дозагружаем имена и теги через карточки контрагентов
            async with aiohttp.ClientSession() as session2:
                for client in result:
                    try:
                        async with session2.get(
                            f"{MS_BASE}/entity/counterparty/{client['id']}",
                            headers=get_headers()
                        ) as r:
                            if r.status == 200:
                                cp = await r.json()
                                client["name"] = cp.get("name", client["name"])
                                client["tags"] = [t.lower() for t in cp.get("tags", [])]
                    except Exception:
                        pass

            logger.info(f"get_aging_clients: стареющих клиентов = {len(result)}")
            return result

    except Exception as e:
        logger.error(f"get_aging_clients: {e}", exc_info=True)
        return []

async def get_manager_shipments(date_from: str, date_to: str) -> dict:
    """
    Берёт отгрузки за период для всех менеджеров ОП.
    Для каждого менеджера: кол-во отгрузок, выручка, кол-во клиентов.
    """
    import aiohttp

    MANAGERS = {
        "скляр":      "Инесса Скляр",
        "мерзлякова": "Елена Мерзлякова",
        "баласанян":  "Карина Баласанян",
        "леонтьев":   "Алексей Леонтьев",
    }

    result = {name: {"shipments": 0, "revenue": 0.0, "clients": set(), "new_clients": 0}
              for name in MANAGERS.values()}

    try:
        async with aiohttp.ClientSession() as session:

            # 1. Загружаем всех контрагентов каждого менеджера
            tag_to_ids = {}
            for tag in MANAGERS:
                ids = set()
                offset = 0
                while True:
                    async with session.get(
                        f"{MS_BASE}/entity/counterparty",
                        headers=get_headers(),
                        params={"filter": f"tags={tag}", "limit": 100, "offset": offset}
                    ) as r:
                        data = await r.json()
                    rows = data.get("rows", [])
                    for cp in rows:
                        ids.add(cp.get("id", ""))
                    if len(rows) < 100:
                        break
                    offset += 100
                tag_to_ids[tag] = ids
                logger.info(f"get_manager_shipments: {tag} — {len(ids)} контрагентов")

            # 2. Все отгрузки за период
            offset = 0
            while True:
                params = {
                    "filter": f"moment>={date_from} 00:00:00;moment<={date_to} 23:59:59",
                    "expand": "agent",
                    "limit": 200,
                    "offset": offset,
                }
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(), params=params
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    agent_href = row.get("agent", {}).get("meta", {}).get("href", "")
                    agent_id = agent_href.split("/")[-1] if agent_href else ""
                    revenue = (row.get("sum", 0) or 0) / 100
                    for tag, mgr_name in MANAGERS.items():
                        if agent_id in tag_to_ids.get(tag, set()):
                            result[mgr_name]["shipments"] += 1
                            result[mgr_name]["revenue"] += revenue
                            result[mgr_name]["clients"].add(agent_id)
                            break
                if len(rows) < 200:
                    break
                offset += 200

            # 3. Новые клиенты — у кого не было отгрузок до date_from
            for mgr_name, data_mgr in result.items():
                new_clients = set()
                for agent_id in data_mgr["clients"]:
                    # Проверяем есть ли отгрузки до начала периода
                    async with session.get(
                        f"{MS_BASE}/entity/demand",
                        headers=get_headers(),
                        params={
                            "filter": f"agent={MS_BASE}/entity/counterparty/{agent_id};moment<{date_from} 00:00:00",
                            "limit": 1,
                        }
                    ) as r:
                        prev = await r.json()
                    if prev.get("meta", {}).get("size", 0) == 0:
                        new_clients.add(agent_id)
                result[mgr_name]["new_clients"] = len(new_clients)
                logger.info(f"get_manager_shipments {mgr_name}: new_clients={len(new_clients)}")

    except Exception as e:
        logger.error(f"get_manager_shipments: {e}", exc_info=True)

    for name in result:
        result[name]["clients"] = len(result[name]["clients"])
        logger.info(f"get_manager_shipments {name}: ship={result[name]['shipments']} rev={result[name]['revenue']:.0f} cl={result[name]['clients']}")

    return result

async def get_attracted_goods_by_manager(date_from: str, date_to: str) -> dict:
    """
    Сумма продаж групп 'ПРИВЛЕЧЕННЫЕ ТОВАРЫ' + 'Акционный прайс-лист' по менеджерам.
    """
    import aiohttp

    TAGS = {
        "скляр":      "Инесса Скляр",
        "мерзлякова": "Елена Мерзлякова",
        "баласанян":  "Карина Баласанян",
        "леонтьев":   "Алексей Леонтьев",
    }
    GROUP_NAMES = ["ПРИВЛЕЧЕННЫЕ ТОВАРЫ", "Акционный прайс-лист"]

    result = {name: 0.0 for name in TAGS.values()}

    try:
        async with aiohttp.ClientSession() as session:

            # 1. Находим href групп товаров
            folder_hrefs = []
            for gname in GROUP_NAMES:
                async with session.get(
                    f"{MS_BASE}/entity/productfolder",
                    headers=get_headers(),
                    params={"filter": f"name={gname}", "limit": 5}
                ) as r:
                    data = await r.json()
                for f in data.get("rows", []):
                    folder_hrefs.append(f.get("meta", {}).get("href", ""))
                    logger.info(f"get_attracted_goods: группа '{gname}' найдена")

            if not folder_hrefs:
                logger.warning("get_attracted_goods: группы не найдены")
                return result

            # 2. Контрагенты каждого менеджера
            tag_to_ids = {}
            for tag in TAGS:
                ids = set()
                off = 0
                while True:
                    async with session.get(
                        f"{MS_BASE}/entity/counterparty",
                        headers=get_headers(),
                        params={"filter": f"tags={tag}", "limit": 100, "offset": off}
                    ) as r:
                        data = await r.json()
                    rows = data.get("rows", [])
                    for cp in rows:
                        ids.add(cp.get("id", ""))
                    if len(rows) < 100:
                        break
                    off += 100
                tag_to_ids[tag] = ids

            # 3. Отчёт прибыльности по покупателям для каждой группы
            for folder_href in folder_hrefs:
                offset = 0
                while True:
                    async with session.get(
                        f"{MS_BASE}/report/profit/bycounterparty",
                        headers=get_headers(),
                        params={
                            "momentFrom": f"{date_from} 00:00:00",
                            "momentTo":   f"{date_to} 23:59:59",
                            "filter":     f"productFolder={folder_href}",
                            "limit": 200,
                            "offset": offset,
                        }
                    ) as r:
                        data = await r.json()
                    rows = data.get("rows", [])
                    for row in rows:
                        cp_href = row.get("counterparty", {}).get("meta", {}).get("href", "")
                        cp_id = cp_href.split("/")[-1] if cp_href else ""
                        sell_sum = (row.get("sellSum", 0) or 0) / 100
                        for tag, mgr_name in TAGS.items():
                            if cp_id in tag_to_ids.get(tag, set()):
                                result[mgr_name] += sell_sum
                                break
                    total = data.get("meta", {}).get("size", 0)
                    offset += len(rows)
                    if offset >= total or len(rows) < 200:
                        break

    except Exception as e:
        logger.error(f"get_attracted_goods_by_manager: {e}", exc_info=True)

    for name, val in result.items():
        logger.info(f"attracted_goods {name}: {val:.0f}")

    return result


async def get_lost_clients_by_manager(date_from: str, date_to: str) -> dict:
    """
    Клиенты которые грузились в прошлом месяце но не грузились в текущем.
    date_from/date_to — текущий период.
    """
    import aiohttp
    from datetime import datetime

    MANAGERS = {
        "скляр":      "Инесса Скляр",
        "мерзлякова": "Елена Мерзлякова",
        "баласанян":  "Карина Баласанян",
        "леонтьев":   "Алексей Леонтьев",
    }

    result = {name: 0 for name in MANAGERS.values()}

    try:
        # Прошлый месяц
        dt = datetime.strptime(date_from, "%Y-%m-%d")
        if dt.month == 1:
            prev_start = f"{dt.year-1}-12-01"
            prev_end = date_from
        else:
            prev_start = f"{dt.year}-{dt.month-1:02d}-01"
            prev_end = date_from

        async with aiohttp.ClientSession() as session:
            # Контрагенты каждого менеджера
            tag_to_ids = {}
            for tag in MANAGERS:
                ids = set()
                off = 0
                while True:
                    async with session.get(
                        f"{MS_BASE}/entity/counterparty",
                        headers=get_headers(),
                        params={"filter": f"tags={tag}", "limit": 100, "offset": off}
                    ) as r:
                        data = await r.json()
                    rows = data.get("rows", [])
                    for cp in rows:
                        ids.add(cp.get("id", ""))
                    if len(rows) < 100:
                        break
                    off += 100
                tag_to_ids[tag] = ids

            all_mgr_ids = set().union(*tag_to_ids.values())

            # Клиенты прошлого месяца (только менеджеров ОП)
            prev_clients = set()
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"moment>={prev_start} 00:00:00;moment<{prev_end} 00:00:00",
                        "expand": "agent",
                        "limit": 200,
                        "offset": offset,
                    }
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    agent_href = row.get("agent", {}).get("meta", {}).get("href", "")
                    agent_id = agent_href.split("/")[-1] if agent_href else ""
                    if agent_id and agent_id in all_mgr_ids:
                        prev_clients.add(agent_id)
                if len(rows) < 200:
                    break
                offset += 200

            # Клиенты текущего месяца
            curr_clients = set()
            offset = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={
                        "filter": f"moment>={date_from} 00:00:00;moment<={date_to} 23:59:59",
                        "expand": "agent",
                        "limit": 200,
                        "offset": offset,
                    }
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for row in rows:
                    agent_href = row.get("agent", {}).get("meta", {}).get("href", "")
                    agent_id = agent_href.split("/")[-1] if agent_href else ""
                    if agent_id:
                        curr_clients.add(agent_id)
                if len(rows) < 200:
                    break
                offset += 200

            # Выбывшие по менеджерам
            lost_ids = prev_clients - curr_clients
            for agent_id in lost_ids:
                for tag, mgr_name in MANAGERS.items():
                    if agent_id in tag_to_ids.get(tag, set()):
                        result[mgr_name] += 1
                        break

    except Exception as e:
        logger.error(f"get_lost_clients_by_manager: {e}", exc_info=True)

    for name, val in result.items():
        logger.info(f"lost_clients {name}: {val}")

    return result


async def get_manager_monthly_history(tag: str, mgr_name: str) -> list:
    """
    История отгрузок менеджера по месяцам с первого месяца до текущего.
    Возвращает список: [{"month": "2024-01", "revenue": ..., "shipments": ..., "clients": ...}]
    """
    import aiohttp
    from datetime import date, datetime

    try:
        async with aiohttp.ClientSession() as session:
            # 1. Контрагенты менеджера
            cp_ids = set()
            off = 0
            while True:
                async with session.get(
                    f"{MS_BASE}/entity/counterparty",
                    headers=get_headers(),
                    params={"filter": f"tags={tag}", "limit": 100, "offset": off}
                ) as r:
                    data = await r.json()
                rows = data.get("rows", [])
                for cp in rows:
                    cp_ids.add(cp.get("id", ""))
                if len(rows) < 100:
                    break
                off += 100

            if not cp_ids:
                return []

            # 2. Первая отгрузка любого клиента менеджера
            first_month = None
            all_mgr_hrefs = [f"{MS_BASE}/entity/counterparty/{cid}" for cid in list(cp_ids)[:5]]
            for href in all_mgr_hrefs:
                async with session.get(
                    f"{MS_BASE}/entity/demand",
                    headers=get_headers(),
                    params={"filter": f"agent={href}", "limit": 1, "order": "moment,asc"}
                ) as r:
                    d = await r.json()
                rows = d.get("rows", [])
                if rows:
                    moment = rows[0].get("moment", "")[:7]  # YYYY-MM
                    if moment and (first_month is None or moment < first_month):
                        first_month = moment

            if not first_month:
                # Пробуем через все отгрузки с фильтром
                for cid in list(cp_ids):
                    async with session.get(
                        f"{MS_BASE}/entity/demand",
                        headers=get_headers(),
                        params={"filter": f"agent={MS_BASE}/entity/counterparty/{cid}", "limit": 1, "order": "moment,asc"}
                    ) as r:
                        d = await r.json()
                    rows = d.get("rows", [])
                    if rows:
                        moment = rows[0].get("moment", "")[:7]
                        if moment and (first_month is None or moment < first_month):
                            first_month = moment
                    if first_month:
                        break

            if not first_month:
                return []

            # 3. Собираем данные по каждому месяцу
            today = date.today()
            result = []
            year, month = int(first_month[:4]), int(first_month[5:7])

            while True:
                month_start = f"{year}-{month:02d}-01"
                if month == 12:
                    month_end = f"{year+1}-01-01"
                else:
                    month_end = f"{year}-{month+1:02d}-01"

                # Все отгрузки за месяц
                revenue = 0.0
                shipments = 0
                clients = set()
                offset = 0
                while True:
                    async with session.get(
                        f"{MS_BASE}/entity/demand",
                        headers=get_headers(),
                        params={
                            "filter": f"moment>={month_start} 00:00:00;moment<{month_end} 00:00:00",
                            "expand": "agent",
                            "limit": 200,
                            "offset": offset,
                        }
                    ) as r:
                        d = await r.json()
                    rows = d.get("rows", [])
                    for row in rows:
                        agent_href = row.get("agent", {}).get("meta", {}).get("href", "")
                        agent_id = agent_href.split("/")[-1] if agent_href else ""
                        if agent_id in cp_ids:
                            shipments += 1
                            revenue += (row.get("sum", 0) or 0) / 100
                            clients.add(agent_id)
                    if len(rows) < 200:
                        break
                    offset += 200

                result.append({
                    "month": f"{year}-{month:02d}",
                    "revenue": revenue,
                    "shipments": shipments,
                    "clients": len(clients),
                })

                # Переходим к следующему месяцу
                if month == 12:
                    year += 1
                    month = 1
                else:
                    month += 1

                # Останавливаемся на текущем месяце
                if year > today.year or (year == today.year and month > today.month):
                    break

            logger.info(f"get_manager_monthly_history {mgr_name}: {len(result)} месяцев с {first_month}")
            return result

    except Exception as e:
        logger.error(f"get_manager_monthly_history {mgr_name}: {e}", exc_info=True)
        return []
