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

YANDEX_GEOCODER_KEY = os.getenv("YANDEX_GEOCODER_KEY")
if not YANDEX_GEOCODER_KEY:
    raise RuntimeError(
        "YANDEX_GEOCODER_KEY env not set. "
        "Выпустить ключ в Яндекс.Разработчики и задать в Railway → Variables."
    )

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
    "дьяченко":  "Ирина Дьяченко",
    "коликов":   "Денис Коликов",
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

        result = {
            "buyer_inn": cp.get("inn", "") or "",
            "buyer_ogrn": cp.get("ogrn", "") or cp.get("ogrnip", "") or "",
            "buyer_address": legal or actual,
            "buyer_phone": cp.get("phone", "") or "",
            "buyer_email": cp.get("email", "") or "",
            "buyer_director_name": director,
            "buyer_name": cp.get("name", ""),
            "buyer_legal_title": cp.get("legalTitle", "") or cp.get("name", ""),
            "href": f"{MS_BASE}/entity/counterparty/{counterparty_id}",
            "id": counterparty_id,
        }
        result.update(bank_data)

        # Представитель для договора
        if director:
            # Определяем должность по типу контрагента
            cp_type = cp.get("companyType", "")
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
        "дьяченко": "дьяченко",
        "ирина": "дьяченко",
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
                            "дьяченко": "Ирина Дьяченко",
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


# ─── ПДЗ-автоматика (план 2026-05-20, Фаза 2) ─────────────────────────────
# Маппинг тег контрагента → имя менеджера. Полная версия (5 ОП). Скопировано
# из удалённого PDZ_MANAGERS в scheduler.py при Фазе 1.
PDZ_MANAGER_TAG_MAP = {
    "баласанян": "Карина Баласанян",
    "скляр": "Инесса Скляр",
    "мерзлякова": "Елена Мерзлякова",
    "дьяченко": "Ирина Дьяченко",
    "коликов": "Денис Коликов",
}

# Прямой маппинг tag → telegram user_id. Использовать вместо поиска по имени
# через get_manager_chat_id (тот спотыкается о невидимые символы / регистр /
# порядок «Имя Фамилия» в TG-профиле). Источник — `/managers` 2026-05-22.
PDZ_MANAGER_TG_IDS = {
    "баласанян": 595181729,
    "скляр":     1435133158,
    "мерзлякова": 8021969241,
    "дьяченко":  649712597,
    "коликов":   683079752,
}


def _parse_ms_date(value):
    """МС отдаёт даты в виде 'YYYY-MM-DD HH:MM:SS.SSS'. Возвращает date или None."""
    if not value:
        return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # отрезаем время если есть
        s10 = s[:10]
        try:
            from datetime import date as _date
            y, m, d = s10.split("-")
            return _date(int(y), int(m), int(d))
        except Exception:
            return None
    return None


async def pdz_take_snapshot() -> list:
    """Снимок состояния всех customerorder с заполненным `ppm_initial`.

    Тянет все заказы (пагинация по 100, expand=agent,attributes), для каждого
    собирает: исходную дату оплаты (ppm_initial), новую дату оплаты (ppm_new),
    id причины переноса (reason_id, из customentity), менеджера (по тегу
    контрагента), payed_sum/total_sum (в рублях, не копейках).

    Заказы без `ppm_initial` (старые до введения поля) пропускаются.
    Розничный покупатель исключается.

    Возвращает список dict, готовый для `Database.save_pdz_snapshot()`.
    """
    from datetime import datetime, timezone
    snap_at = datetime.now(timezone.utc)
    # Используем дату МСК для snap_date — снимок логически принадлежит МСК-дню.
    try:
        from zoneinfo import ZoneInfo as _ZI
        snap_date = snap_at.astimezone(_ZI("Europe/Moscow")).date()
    except Exception:
        snap_date = snap_at.date()

    rows: list = []
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{MS_BASE}/entity/customerorder"
            all_orders = []
            offset = 0
            while True:
                params = {
                    "limit": 100,
                    "offset": offset,
                    "expand": "agent,attributes",
                    "order": "moment,asc",
                }
                async with session.get(url, headers=get_headers(), params=params) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error(f"pdz_take_snapshot customerorder {resp.status}: {body[:200]}")
                        break
                    data = await resp.json()
                    batch = data.get("rows", [])
                    all_orders.extend(batch)
                    if len(batch) < 100:
                        break
                    offset += 100

            logger.info(f"pdz_take_snapshot: {len(all_orders)} заказов загружено из МС")

            for order in all_orders:
                # Атрибуты — список с {name, value, ...}. customentity-значение
                # = вложенный объект с meta.href и id.
                ppm_initial_raw = None
                ppm_new_raw = None
                reason_id = None
                for attr in order.get("attributes", []) or []:
                    name = attr.get("name")
                    if name == "Дата планируемой оплаты":
                        ppm_initial_raw = attr.get("value")
                    elif name == "НОВАЯ дата оплаты":
                        ppm_new_raw = attr.get("value")
                    elif name == "Причина переноса":
                        v = attr.get("value")
                        if isinstance(v, dict):
                            # customentity: бери id напрямую, иначе вытаскивай
                            # из meta.href последний сегмент
                            reason_id = v.get("id")
                            if not reason_id:
                                href = (v.get("meta") or {}).get("href") or ""
                                if href:
                                    reason_id = href.rstrip("/").split("/")[-1] or None

                ppm_initial = _parse_ms_date(ppm_initial_raw)
                if not ppm_initial:
                    # Пустые ppm_initial не пишем — старые заказы до введения поля.
                    continue
                ppm_new = _parse_ms_date(ppm_new_raw)

                agent = order.get("agent", {}) or {}
                agent_id = agent.get("id") or ""
                agent_name = agent.get("name") or "неизвестно"
                agent_tags = agent.get("tags", []) or []

                if not agent_id:
                    continue

                # Розничного покупателя исключаем.
                if "розничный покупатель" in agent_name.lower():
                    continue

                manager_tag = None
                for t in agent_tags:
                    if isinstance(t, str) and t.lower() in PDZ_MANAGER_TAG_MAP:
                        manager_tag = t.lower()
                        break

                total_sum = round((order.get("sum", 0) or 0) / 100, 2)
                payed_sum = round((order.get("payedSum", 0) or 0) / 100, 2)

                rows.append({
                    "snap_date": snap_date,
                    "order_id": order.get("id") or "",
                    "order_name": order.get("name") or "",
                    "agent_id": agent_id,
                    "agent_name": agent_name,
                    "manager_tag": manager_tag,
                    "ppm_initial": ppm_initial,
                    "ppm_new": ppm_new,
                    "reason_id": reason_id,
                    "payed_sum": payed_sum,
                    "total_sum": total_sum,
                    "agent_balance": None,  # обогащается ниже
                })

            # ── Обогащение balance контрагента (отсекаем false-positive PDZ) ──
            # Логика: МойСклад в customerorder.payedSum хранит только разнесённые
            # на этот заказ оплаты. Если бухгалтерия закрыла долг общей платёжкой
            # без разноски — payedSum < sum, но реальный долг = 0. Фильтр по
            # /report/counterparty/{id}.balance (в копейках, делим на 100):
            #   balance < 0 = клиент должен НАМ (есть реальный долг)
            #   balance >= 0 = клиент НЕ должен (нам должны ему / в ноль)
            unique_agents = list({r["agent_id"] for r in rows if r.get("agent_id")})
            logger.info(
                f"pdz_take_snapshot: тяну balance для {len(unique_agents)} уникальных контрагентов"
            )

            balance_map: dict = {}

            async def _fetch_balance(aid: str, attempt: int = 1):
                report_url = f"{MS_BASE}/report/counterparty/{aid}"
                try:
                    timeout = aiohttp.ClientTimeout(total=30)
                    async with session.get(report_url, headers=get_headers(), timeout=timeout) as resp_b:
                        if resp_b.status == 429 and attempt == 1:
                            # Rate-limit — подождать секунду и попробовать ещё раз.
                            await asyncio.sleep(1.0)
                            return await _fetch_balance(aid, attempt=2)
                        if resp_b.status != 200:
                            body = await resp_b.text()
                            logger.warning(
                                f"pdz_take_snapshot balance {aid[:8]} STATUS={resp_b.status} body={body[:200]}"
                            )
                            return aid, None
                        rdata = await resp_b.json()
                        raw_balance = rdata.get("balance", 0) or 0
                        return aid, round(raw_balance / 100, 2)
                except (asyncio.TimeoutError, aiohttp.ClientError) as ex_b:
                    if attempt == 1:
                        await asyncio.sleep(0.5)
                        return await _fetch_balance(aid, attempt=2)
                    logger.warning(
                        f"pdz_take_snapshot balance {aid[:8]} {type(ex_b).__name__}: {ex_b}"
                    )
                    return aid, None
                except Exception as ex_b:
                    logger.warning(
                        f"pdz_take_snapshot balance {aid[:8]} unexpected: {type(ex_b).__name__}: {ex_b}"
                    )
                    return aid, None

            # Батчи по 2 параллельно + 200ms пауза между батчами — защита от
            # rate-limit МС. Для ~700 контрагентов ~70 сек, приемлемо для cron.
            BATCH = 2
            for i in range(0, len(unique_agents), BATCH):
                chunk = unique_agents[i:i + BATCH]
                results = await asyncio.gather(
                    *(_fetch_balance(aid) for aid in chunk),
                    return_exceptions=False,
                )
                for aid, bal in results:
                    balance_map[aid] = bal
                # Тротлинг для МС — иначе на больших объёмах бот ловит 429/timeout.
                if i + BATCH < len(unique_agents):
                    await asyncio.sleep(0.2)

            # Раскладываем balance по строкам.
            for r in rows:
                r["agent_balance"] = balance_map.get(r.get("agent_id"))

            # Статистика для логов: сколько agents с balance>=0 (= не должны).
            non_debtors = sum(
                1 for aid, b in balance_map.items() if b is not None and b >= 0
            )
            failed = sum(1 for b in balance_map.values() if b is None)
            logger.info(
                f"pdz_take_snapshot: balance получен для {len(balance_map) - failed}/{len(balance_map)} "
                f"контрагентов; balance>=0 (не должны): {non_debtors}; запросы упали: {failed}"
            )

            logger.info(f"pdz_take_snapshot: {len(rows)} строк готово к записи (с ppm_initial, не розница)")
            return rows
    except Exception as e:
        logger.error(f"pdz_take_snapshot error: {e}", exc_info=True)
        return rows


# ─── ПДЗ Фаза 3: логика событий обещаний + аудит ppm_initial ───────────────

# Защита от шторма: если в одном проходе изменения ppm_initial оказались
# массовыми (например, кто-то «починил» поле через импорт) — обрезаем список,
# чтобы не запросить /audit на сотни заказов и не залить TG-чат.
PDZ_AUDIT_PPM_INITIAL_LIMIT = 200


def _to_date(value):
    """Принимает date | datetime | None | str — возвращает date или None."""
    if value is None:
        return None
    # date / datetime
    try:
        from datetime import date as _date, datetime as _dt
        if isinstance(value, _dt):
            return value.date()
        if isinstance(value, _date):
            return value
    except Exception:
        pass
    # строка
    if isinstance(value, str):
        return _parse_ms_date(value)
    return None


def compute_promise_events(today_rows: list, yesterday_rows: list) -> list:
    """Сравнивает текущий снимок (today_rows) со вчерашним (yesterday_rows)
    и возвращает список событий обещаний для записи в promise_log.

    Логика по каждому заказу из today_rows:
      - `event_type='set'`     — у заказа `old.ppm_new IS NULL`, `new.ppm_new IS NOT NULL`
        → обещание поставлено впервые.
      - `event_type='moved'`   — `old.ppm_new IS NOT NULL`, `new.ppm_new IS NOT NULL`
        и они РАЗНЫЕ даты → обещание перенесено.
      - `event_type='broken'`  — `new.ppm_new IS NOT NULL`, `new.ppm_new < today`,
        `new.payed_sum < new.total_sum`, `old.ppm_new == new.ppm_new` (т.е. за день
        ничего не пересогласовали и дата прошла без оплаты).

    Возвращаемые dict'ы готовы к Database.save_promise_events().
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo as _ZI
    today = datetime.now(_ZI("Europe/Moscow")).date()

    yesterday_by_id = {r.get("order_id"): r for r in (yesterday_rows or []) if r.get("order_id")}

    events: list = []
    for new_row in today_rows or []:
        order_id = new_row.get("order_id")
        if not order_id:
            continue
        old_row = yesterday_by_id.get(order_id)

        new_ppm = _to_date(new_row.get("ppm_new"))
        old_ppm = _to_date(old_row.get("ppm_new")) if old_row else None

        new_payed = new_row.get("payed_sum") or 0
        new_total = new_row.get("total_sum") or 0

        event_type = None
        # set: было пусто (или строки вчера не было), сейчас стоит
        if new_ppm is not None and old_ppm is None:
            event_type = "set"
        # moved: было непусто, сейчас непусто, и они РАЗНЫЕ
        elif new_ppm is not None and old_ppm is not None and new_ppm != old_ppm:
            event_type = "moved"
        # broken: за день ничего не пересогласовали (даты одинаковые),
        # дата уже в прошлом, и не оплачено
        elif (
            new_ppm is not None
            and old_ppm is not None
            and new_ppm == old_ppm
            and new_ppm < today
            and float(new_payed) < float(new_total)
        ):
            event_type = "broken"

        if not event_type:
            continue

        events.append({
            "order_id": order_id,
            "order_name": new_row.get("order_name"),
            "agent_id": new_row.get("agent_id"),
            "agent_name": new_row.get("agent_name"),
            "manager_tag": new_row.get("manager_tag"),
            "event_type": event_type,
            "old_ppm_new": old_ppm,
            "new_ppm_new": new_ppm,
            "reason_id": new_row.get("reason_id"),
        })

    return events


async def _audit_who_changed_order(order_id: str) -> Optional[str]:
    """GET `/audit?filter=entity=customerorder;hrefId={order_id}&limit=10`.

    Возвращает имя сотрудника из последней записи, где это можно определить,
    либо None если МС не отдал данные/ничего не найдено. Сетевые ошибки —
    тихо проглатываем (логируем), чтобы не ронять весь job.
    """
    if not order_id:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{MS_BASE}/audit"
            params = {
                # МС audit-фильтр: entity=customerorder;hrefId=<uuid>.
                "filter": f"entity=customerorder;hrefId={order_id}",
                "limit": 10,
            }
            async with session.get(
                url,
                headers=get_headers(),
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(
                        f"_audit_who_changed_order {order_id} {resp.status}: {body[:200]}"
                    )
                    return None
                data = await resp.json()
        # rows — список audit-событий; у каждого есть employee {name, ...}.
        # Берём ПОСЛЕДНЕЕ событие (события возвращаются от свежих к старым).
        rows = data.get("rows") or []
        for ev in rows:
            emp = ev.get("employee") or {}
            name = emp.get("name")
            if name:
                return name
        return None
    except Exception as e:
        logger.warning(f"_audit_who_changed_order {order_id}: {e}")
        return None


async def audit_ppm_initial_changes(today_rows: list, yesterday_rows: list) -> list:
    """Сравнивает ppm_initial вчера vs сегодня. Любое изменение поля
    «Дата планируемой оплаты» (которая по регламенту менять НЕЛЬЗЯ) — алерт.

    Для каждого изменения, по возможности, через `/audit` определяет, кто менял
    (employee.name). Лимит на ОДИН проход — PDZ_AUDIT_PPM_INITIAL_LIMIT заказов
    (защита от шторма). Если в одном дне массовое изменение — берём первые N
    по сортировке order_name + флаг в логе.

    Возвращает список:
      {order_id, order_name, agent_name, manager_tag,
       old_ppm_initial, new_ppm_initial, changed_by (Optional[str])}
    """
    yesterday_by_id = {r.get("order_id"): r for r in (yesterday_rows or []) if r.get("order_id")}

    # Сначала собираем кандидатов без сетевых запросов
    candidates: list = []
    for new_row in today_rows or []:
        order_id = new_row.get("order_id")
        if not order_id:
            continue
        old_row = yesterday_by_id.get(order_id)
        if not old_row:
            # Новый заказ — это не «изменение исходной даты», пропускаем.
            continue
        old_ppm = _to_date(old_row.get("ppm_initial"))
        new_ppm = _to_date(new_row.get("ppm_initial"))
        if not old_ppm or not new_ppm:
            continue
        if old_ppm == new_ppm:
            continue

        candidates.append({
            "order_id": order_id,
            "order_name": new_row.get("order_name"),
            "agent_name": new_row.get("agent_name"),
            "manager_tag": new_row.get("manager_tag"),
            "old_ppm_initial": old_ppm,
            "new_ppm_initial": new_ppm,
            "changed_by": None,
        })

    if not candidates:
        return []

    if len(candidates) > PDZ_AUDIT_PPM_INITIAL_LIMIT:
        logger.warning(
            "audit_ppm_initial_changes: получено %d изменений, обрезаем до %d (защита от шторма)",
            len(candidates), PDZ_AUDIT_PPM_INITIAL_LIMIT,
        )
        candidates.sort(key=lambda c: c.get("order_name") or "")
        candidates = candidates[:PDZ_AUDIT_PPM_INITIAL_LIMIT]

    # Теперь к каждому — точечный GET /audit. Сетевые ошибки уже подавлены внутри
    # _audit_who_changed_order, она вернёт None.
    for c in candidates:
        changed_by = await _audit_who_changed_order(c["order_id"])
        c["changed_by"] = changed_by

    return candidates


async def pdz_overdue_for_manager(manager_tag: str, db=None, group_by_agent: bool = True) -> list:
    """Список просроченных заказов конкретного менеджера для TG-дайджеста.

    Источник данных:
      - Если передан `db` — читает последний снимок из БД (`db.get_latest_snapshot()`).
        Мгновенно. Это основной путь для cron 14:10 и тестов.
      - Если `db=None` — fallback на свежий `pdz_take_snapshot()` (~30 сек,
        для совместимости и ручных вызовов).

    Критерий «просрочен»:
      - effective_due_date = ppm_new if ppm_new is not None else ppm_initial
      - effective_due_date < сегодня (МСК)
      - payed_sum < total_sum
      - manager_tag совпадает (case-insensitive)
      - agent_balance < 0 ИЛИ agent_balance is None (фильтр против ложных
        срабатываний: balance≥0 = клиент не должен → пропускаем; None = запрос
        упал → оставляем как подозрительного).

    Возврат:
      - group_by_agent=True (default): список dict по контрагентам, отсортирован
        по total_unpaid убыванию, лимит топ-30. Поля:
          {agent_id, agent_name, total_unpaid, max_days_overdue, orders_count,
           ms_url_first_order, agent_balance}
      - group_by_agent=False: плоский список заказов, сортировка по days_overdue.
        Поля: {order_id, order_name, agent_id, agent_name, effective_due_date,
        days_overdue, unpaid_sum, ms_url, agent_balance}
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo as _ZI
    today = datetime.now(_ZI("Europe/Moscow")).date()

    if not manager_tag:
        return []
    tag_lower = manager_tag.lower()

    if db is not None:
        rows = db.get_latest_snapshot()
    else:
        rows = await pdz_take_snapshot()

    orders: list = []
    skipped_balance_ok = 0
    for r in rows:
        row_tag = (r.get("manager_tag") or "").lower()
        if row_tag != tag_lower:
            continue
        ppm_new = _to_date(r.get("ppm_new"))
        ppm_initial = _to_date(r.get("ppm_initial"))
        effective = ppm_new if ppm_new is not None else ppm_initial
        if not effective or effective >= today:
            continue
        payed = float(r.get("payed_sum") or 0)
        total = float(r.get("total_sum") or 0)
        if payed >= total:
            continue
        # Фильтр по реальному балансу контрагента.
        bal_raw = r.get("agent_balance")
        agent_balance = float(bal_raw) if bal_raw is not None else None
        if agent_balance is not None and agent_balance >= 0:
            # Клиент не должен — заказ выглядит как просрочка только из-за
            # неразнесённой оплаты в бухгалтерии. Пропускаем.
            skipped_balance_ok += 1
            continue

        days_overdue = (today - effective).days
        unpaid = round(total - payed, 2)
        order_id = r.get("order_id") or ""
        orders.append({
            "order_id": order_id,
            "order_name": r.get("order_name"),
            "agent_id": r.get("agent_id"),
            "agent_name": r.get("agent_name"),
            "effective_due_date": effective,
            "days_overdue": days_overdue,
            "unpaid_sum": unpaid,
            "ms_url": f"https://online.moysklad.ru/app/#customerorder/edit?id={order_id}",
            "agent_balance": agent_balance,
        })

    if skipped_balance_ok:
        logger.info(
            f"pdz_overdue_for_manager({manager_tag}): пропущено {skipped_balance_ok} "
            f"заказов с balance>=0 (клиент не должен)"
        )

    if not group_by_agent:
        orders.sort(key=lambda x: x["days_overdue"], reverse=True)
        return orders

    # ── Группировка по контрагенту ────────────────────────────────────────
    by_agent: dict = {}
    for o in orders:
        aid = o.get("agent_id") or ""
        if aid not in by_agent:
            by_agent[aid] = {
                "agent_id": aid,
                "agent_name": o.get("agent_name"),
                "agent_balance": o.get("agent_balance"),
                "orders": [],
            }
        by_agent[aid]["orders"].append(o)

    grouped: list = []
    for aid, data in by_agent.items():
        agent_orders = data["orders"]
        # Самый старый просроченный заказ — по возрастанию effective_due_date.
        oldest = min(agent_orders, key=lambda x: x["effective_due_date"])
        total_unpaid = round(sum(x["unpaid_sum"] for x in agent_orders), 2)
        max_days = max(x["days_overdue"] for x in agent_orders)
        grouped.append({
            "agent_id": aid,
            "agent_name": data["agent_name"],
            "agent_balance": data["agent_balance"],
            "total_unpaid": total_unpaid,
            "max_days_overdue": max_days,
            "orders_count": len(agent_orders),
            "ms_url_first_order": oldest["ms_url"],
        })

    # Обогащение счётчиком срывов за 90 дней (Фаза 4.5).
    if db is not None and grouped:
        try:
            ids = [g.get("agent_id") for g in grouped if g.get("agent_id")]
            breaks_map = db.get_promise_breaks_count(ids, days_window=90)
        except Exception as e:
            logger.warning(f"pdz_overdue_for_manager({manager_tag}): breaks_count failed: {e}")
            breaks_map = {}
        # Обогащение стоп-флагами (Фаза 6).
        stop_map: dict = {}
        if hasattr(db, "get_stop_flag_map"):
            try:
                stop_map = db.get_stop_flag_map(ids) or {}
            except Exception as e:
                logger.warning(f"pdz_overdue_for_manager({manager_tag}): stop_flag_map failed: {e}")
                stop_map = {}
        for g in grouped:
            aid = g.get("agent_id") or ""
            g["breaks_count"] = int(breaks_map.get(aid, 0))
            g["stop_status"] = stop_map.get(aid)
    else:
        for g in grouped:
            g.setdefault("breaks_count", 0)
            g.setdefault("stop_status", None)

    grouped.sort(key=lambda x: x["total_unpaid"], reverse=True)
    return grouped


# ─── ПДЗ Фаза 4: TG-дайджесты ────────────────────────────────────────────

def _order_word_ru(cnt: int) -> str:
    """Русское склонение «заказ/заказа/заказов» по числу."""
    if cnt % 10 == 1 and cnt % 100 != 11:
        return "заказ"
    if cnt % 10 in (2, 3, 4) and cnt % 100 not in (12, 13, 14):
        return "заказа"
    return "заказов"


def _breaks_word_ru(cnt: int) -> str:
    """Русское склонение «срыв/срыва/срывов» по числу."""
    if cnt % 10 == 1 and cnt % 100 != 11:
        return "срыв"
    if cnt % 10 in (2, 3, 4) and cnt % 100 not in (12, 13, 14):
        return "срыва"
    return "срывов"


def pdz_send_manager_digest_text(items: list, manager_name: str = None) -> list:
    """Формирует список TG-сообщений (Markdown) с дайджестом просрочек для одного
    менеджера. Каждое сообщение ≤3500 символов (запас от лимита TG 4096).

    items — результат pdz_overdue_for_manager(tag, db=db) (grouped, по клиентам,
    отсортирован по total_unpaid убыванию).

    Формат строки клиента:
        [Имя клиента](ms_url_first_order) · K заказа · M дн · S руб.

    Заголовок (одно сообщение в начале):
        📋 Просрочки — клиентов: N

    Возвращает [] если items пуст (тишина = всё хорошо).

    manager_name — оставлен в сигнатуре для совместимости / диагностики; в текст
    не подмешиваем, потому что сообщение приходит менеджеру в личку и он знает,
    что это про него (требование плана 2026-05-20 Фазы 4).
    """
    if not items:
        return []

    header = f"📋 *Просрочки* — клиентов: {len(items)}"
    chunks: list[list[str]] = [[header, ""]]
    current_len = len(header) + 2

    for it in items:
        name = (it.get("agent_name") or "—").replace("*", "").replace("_", "")
        url = it.get("ms_url_first_order") or "#"
        cnt = int(it.get("orders_count", 0) or 0)
        breaks = int(it.get("breaks_count", 0) or 0)
        # Фаза 6: префикс стоп-флага (🚫 СТОП / 🚫 ПРЕДОПЛАТА). Может идти
        # вместе с 🔴 (срывами). Пример: «🚫 СТОП 🔴 [Клиент] ...».
        stop_status = it.get("stop_status")
        stop_prefix = ""
        if stop_status == "stop_shipments":
            stop_prefix = "🚫 СТОП "
        elif stop_status == "prepayment_only":
            stop_prefix = "🚫 ПРЕДОПЛАТА "
        prefix = stop_prefix + ("🔴 " if breaks > 0 else "")
        suffix = f" ({breaks} {_breaks_word_ru(breaks)} за 90д)" if breaks > 0 else ""
        line = (
            f"{prefix}[{name}]({url}) · {cnt} {_order_word_ru(cnt)} · "
            f"{it.get('max_days_overdue', 0)} дн · "
            f"{fmt_money(it.get('total_unpaid', 0))}{suffix}"
        )
        if current_len + len(line) + 1 > 3500:
            chunks.append([])
            current_len = 0
        chunks[-1].append(line)
        current_len += len(line) + 1

    return ["\n".join(c) for c in chunks if c]


def pdz_unprocessed_for_owner(db) -> dict:
    """Для пинга собственнику в 16:05 МСК. Группирует «необработанных» клиентов
    по тегу менеджера.

    «Необработанный заказ» (из последнего snapshot):
      - manager_tag входит в PDZ_MANAGER_TAG_MAP
      - effective_due_date = ppm_new if ppm_new else ppm_initial; effective < today
      - payed_sum < total_sum
      - ppm_new is None ИЛИ ppm_new < today (= менеджер НЕ пересогласовал в будущее)
      - agent_balance < 0 ИЛИ agent_balance is None (как в pdz_overdue_for_manager)

    Возврат: {manager_tag: [agent_dict, ...]}, где agent_dict — то же поле, что
    отдаёт pdz_overdue_for_manager(group_by_agent=True), отсортирован по
    total_unpaid убыванию.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo as _ZI
    today = datetime.now(_ZI("Europe/Moscow")).date()

    rows = db.get_latest_snapshot() if db is not None else []

    # tag → agent_id → {agent_name, agent_balance, orders: [...]}
    by_tag: dict = {}

    for r in rows:
        row_tag = (r.get("manager_tag") or "").lower()
        if not row_tag or row_tag not in PDZ_MANAGER_TAG_MAP:
            continue

        ppm_new = _to_date(r.get("ppm_new"))
        ppm_initial = _to_date(r.get("ppm_initial"))
        effective = ppm_new if ppm_new is not None else ppm_initial
        if not effective or effective >= today:
            continue

        payed = float(r.get("payed_sum") or 0)
        total = float(r.get("total_sum") or 0)
        if payed >= total:
            continue

        # Фильтр против ложных просрочек (см. pdz_overdue_for_manager).
        bal_raw = r.get("agent_balance")
        agent_balance = float(bal_raw) if bal_raw is not None else None
        if agent_balance is not None and agent_balance >= 0:
            continue

        # «Необработан»: ppm_new пустой ИЛИ ppm_new в прошлом/сегодня
        # (= не пересогласовал в будущее).
        if ppm_new is not None and ppm_new >= today:
            continue

        days_overdue = (today - effective).days
        unpaid = round(total - payed, 2)
        order_id = r.get("order_id") or ""
        order = {
            "order_id": order_id,
            "order_name": r.get("order_name"),
            "agent_id": r.get("agent_id"),
            "agent_name": r.get("agent_name"),
            "effective_due_date": effective,
            "days_overdue": days_overdue,
            "unpaid_sum": unpaid,
            "ms_url": f"https://online.moysklad.ru/app/#customerorder/edit?id={order_id}",
            "agent_balance": agent_balance,
        }

        agent_bucket = by_tag.setdefault(row_tag, {}).setdefault(
            order["agent_id"] or "",
            {
                "agent_id": order["agent_id"],
                "agent_name": order["agent_name"],
                "agent_balance": agent_balance,
                "orders": [],
            },
        )
        agent_bucket["orders"].append(order)

    # Сводим order→agent в формат как у pdz_overdue_for_manager.
    result: dict = {}
    all_ids: list = []
    for tag, agents in by_tag.items():
        grouped = []
        for aid, data in agents.items():
            orders = data["orders"]
            oldest = min(orders, key=lambda x: x["effective_due_date"])
            total_unpaid = round(sum(x["unpaid_sum"] for x in orders), 2)
            max_days = max(x["days_overdue"] for x in orders)
            grouped.append({
                "agent_id": data["agent_id"],
                "agent_name": data["agent_name"],
                "agent_balance": data["agent_balance"],
                "total_unpaid": total_unpaid,
                "max_days_overdue": max_days,
                "orders_count": len(orders),
                "ms_url_first_order": oldest["ms_url"],
            })
            if data.get("agent_id"):
                all_ids.append(data["agent_id"])
        grouped.sort(key=lambda x: x["total_unpaid"], reverse=True)
        if grouped:
            result[tag] = grouped

    # Обогащение счётчиком срывов за 90 дней (Фаза 4.5).
    breaks_map: dict = {}
    if db is not None and all_ids:
        try:
            breaks_map = db.get_promise_breaks_count(all_ids, days_window=90)
        except Exception as e:
            logger.warning(f"pdz_unprocessed_for_owner: breaks_count failed: {e}")
            breaks_map = {}
    # Обогащение стоп-флагами (Фаза 6).
    stop_map: dict = {}
    if db is not None and all_ids and hasattr(db, "get_stop_flag_map"):
        try:
            stop_map = db.get_stop_flag_map(all_ids) or {}
        except Exception as e:
            logger.warning(f"pdz_unprocessed_for_owner: stop_flag_map failed: {e}")
            stop_map = {}
    for tag, grouped in result.items():
        for g in grouped:
            aid = g.get("agent_id") or ""
            g["breaks_count"] = int(breaks_map.get(aid, 0))
            g["stop_status"] = stop_map.get(aid)

    return result


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
        "дьяченко":   "Ирина Дьяченко",
        "коликов":    "Денис Коликов",
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
        "дьяченко":   "Ирина Дьяченко",
        "коликов":    "Денис Коликов",
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
        "дьяченко":   "Ирина Дьяченко",
        "коликов":    "Денис Коликов",
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


# ============================================================================
# Задачи и сотрудники (фича: постановка задач через /задача в TG-боте)
# ============================================================================

MS_TASK_EDIT_URL = "https://online.moysklad.ru/app/#task/edit?id={task_id}"
TASK_DESCRIPTION_SOFT_LIMIT = 200

_employees_cache: Optional[list] = None
_employees_cached_at: Optional[float] = None
_EMPLOYEES_CACHE_TTL_SECONDS = 3600


async def list_employees(force_refresh: bool = False) -> list:
    """Список активных сотрудников МойСклада с модульным кешем на 1 ч.

    Возвращает список словарей вида {"id", "name", "email"}.
    Кеш используется как closed-list для промпта Claude и для валидации
    assignee_id в Python до вызова create_task.
    """
    global _employees_cache, _employees_cached_at
    import time
    now_ts = time.time()
    if (not force_refresh
            and _employees_cache is not None
            and _employees_cached_at is not None
            and now_ts - _employees_cached_at < _EMPLOYEES_CACHE_TTL_SECONDS):
        return _employees_cache

    url = f"{MS_BASE}/entity/employee?limit=100"
    async with aiohttp.ClientSession() as session:
        async with session.get(
            url,
            headers=get_headers(),
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

    employees = []
    for emp in data.get("rows", []):
        if emp.get("archived"):
            continue
        employees.append({
            "id": emp.get("id"),
            "name": emp.get("shortFio") or emp.get("name") or emp.get("fullName") or "",
            "email": emp.get("email") or "",
        })
    _employees_cache = employees
    _employees_cached_at = now_ts
    logger.info(f"list_employees: {len(employees)} активных сотрудников (кеш обновлён)")
    return employees


def invalidate_employees_cache() -> None:
    """Сбросить кеш сотрудников. Вызывать после 404/403 на create_task."""
    global _employees_cache, _employees_cached_at
    _employees_cache = None
    _employees_cached_at = None
    logger.info("invalidate_employees_cache: кеш сотрудников сброшен")


async def create_task(assignee_id: str, description: str, due_msk) -> dict:
    """Создать задачу в МойСкладе на конкретного сотрудника.

    Args:
        assignee_id: UUID сотрудника (из list_employees).
        description: текст задачи. Если > TASK_DESCRIPTION_SOFT_LIMIT (200) —
            обрезается до 199 + '…'.
        due_msk: datetime в МСК (naive или с tzinfo). МС трактует dueToDate
            без TZ как МСК. Если None — задача без дедлайна.

    Returns:
        {"id": str, "url": str} — id задачи и URL карточки в веб-МС.

    Raises:
        ValueError: пустой assignee_id или description.
        aiohttp.ClientResponseError: HTTP-ошибка от МС (status 4xx/5xx).
        RuntimeError: МС вернул успешный ответ без id.
    """
    if not assignee_id:
        raise ValueError("create_task: пустой assignee_id")
    text = (description or "").strip()
    if not text:
        raise ValueError("create_task: пустой description")
    if len(text) > TASK_DESCRIPTION_SOFT_LIMIT:
        text = text[: TASK_DESCRIPTION_SOFT_LIMIT - 1] + "…"

    payload = {
        "description": text,
        "assignee": {
            "meta": {
                "href": f"{MS_BASE}/entity/employee/{assignee_id}",
                "type": "employee",
                "mediaType": "application/json",
            }
        },
    }
    if due_msk is not None:
        payload["dueToDate"] = due_msk.strftime("%Y-%m-%d %H:%M:%S.000")

    url = f"{MS_BASE}/entity/task"
    async with aiohttp.ClientSession() as session:
        async with session.post(
            url,
            headers=get_headers(),
            json=payload,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

    task_id = data.get("id")
    if not task_id:
        raise RuntimeError(f"create_task: МС вернул ответ без id: {data}")
    task_url = MS_TASK_EDIT_URL.format(task_id=task_id)
    logger.info(f"create_task: создана задача {task_id} для assignee={assignee_id}")
    return {"id": task_id, "url": task_url}


# ─── ПДЗ Фаза 6: поиск сотрудника МС по тегу-фамилии ──────────────────────
async def find_employee_id_by_tag(tag: str) -> Optional[str]:
    """Возвращает UUID сотрудника МойСклада по фамилии-тегу контрагента.

    Использует кеш list_employees(). Сопоставляет:
      1) PDZ_MANAGER_TAG_MAP[tag] → полное ФИО сотрудника F2B → match по shortFio.
      2) Если в карте нет — пробует найти сотрудника, в имени которого есть тег.

    Сравнение case-insensitive. Возвращает None, если не нашли.
    """
    if not tag:
        return None
    t = tag.strip().lower()
    target_full = PDZ_MANAGER_TAG_MAP.get(t)
    target_full_l = target_full.lower() if target_full else None

    try:
        employees = await list_employees()
    except Exception as e:
        logger.warning(f"find_employee_id_by_tag: list_employees failed: {e}")
        return None

    # 1) Прямой match по полному ФИО.
    if target_full_l:
        for emp in employees:
            name = (emp.get("name") or "").lower()
            if not name:
                continue
            if name == target_full_l:
                return emp.get("id")
        # 2) Match по фамилии (последнее слово ФИО).
        for emp in employees:
            name = (emp.get("name") or "").lower()
            if not name:
                continue
            words = [w for w in name.split() if w]
            if words and words[-1] == t:
                return emp.get("id")

    # 3) Substring-fallback: tag входит в name (например, «коликов» → «Коликов Д. Н.»).
    for emp in employees:
        name = (emp.get("name") or "").lower()
        if name and t in name:
            return emp.get("id")

    return None


# ============================================================================
# Helper'ы для объединённого алерта «На согласовании» / «ЗА ЛИМИТОМ»
# План: 2026-05-21-объединённый-алерт-на-согласование.md, Фаза 2.
# Семипунктовый светофор: Лимит → Просрочка → ДДС → Цена → Дата оплаты → Сайт → Контакты.
# ============================================================================

# UUID статусов customerorder (триггеры алерта; fallback на случай недоступности metadata)
APPROVAL_STATE_ON_APPROVAL = "005f34bf-9a9a-11f0-0a80-03a900027473"  # «На согласовании»
APPROVAL_STATE_OVER_LIMIT  = "462ee41b-b554-11f0-0a80-15a000036d2c"  # «ЗА ЛИМИТОМ»
APPROVAL_STATE_AGREED      = "005f3651-9a9a-11f0-0a80-03a900027474"  # «Согласован»

# UUID кастомных атрибутов counterparty
ATTR_CP_CREDIT_LIMIT = "fd6e0220-b553-11f0-0a80-114f000373a7"  # long, required (₽)
ATTR_CP_SITE         = "9e4fb8db-b55f-11f0-0a80-196e000604d3"  # link, required
ATTR_CP_MAX          = "1505236e-34d7-11f1-0a80-1489000ec449"  # string
ATTR_CP_TELEGRAM     = "15052610-34d7-11f1-0a80-1489000ec44a"  # string

# UUID кастомного атрибута customerorder (Дата плановой оплаты)
ATTR_CO_PAYMENT_PLANNED = "327940fd-b54e-11f0-0a80-0066000d5578"  # time

# Регулярки валидации
_RE_SITE_HAS_LATIN = re.compile(r"[a-zA-Z]{3,}")
_RE_PHONE_RU       = re.compile(r"^[78]\d{10}$")
_RE_TG_USERNAME    = re.compile(r"^@\S{5,}$")
_RE_TG_CHAT_ID     = re.compile(r"^\d+$")

# Москва — фиксированный UTC+3
_MSK_TZ = None  # lazy init в _now_msk()

# Кеш статусов customerorder (single-process, перетягиваем раз в час)
_co_states_cache: dict = {"data": None, "fetched_at": 0.0}
_CO_STATES_TTL_SEC = 3600


def _now_msk():
    """Возвращает текущее время в МСК (UTC+3)."""
    from datetime import datetime, timezone, timedelta
    global _MSK_TZ
    if _MSK_TZ is None:
        _MSK_TZ = timezone(timedelta(hours=3))
    return datetime.now(_MSK_TZ)


async def get_customerorder_state_uuid(name: str) -> Optional[str]:
    """
    Возвращает UUID статуса customerorder по имени (как в МС UI).
    Тянет /entity/customerorder/metadata раз в час (кеш в _co_states_cache).
    При недоступности metadata — fallback на захардкоженные UUID известных статусов.
    """
    import time as _time
    now = _time.time()
    cached = _co_states_cache.get("data")
    if cached is None or (now - _co_states_cache.get("fetched_at", 0)) > _CO_STATES_TTL_SEC:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{MS_BASE}/entity/customerorder/metadata",
                    headers=get_headers(),
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        states = data.get("states", []) or []
                        cached = {s.get("name", "").strip().lower(): s.get("id") for s in states}
                        _co_states_cache["data"] = cached
                        _co_states_cache["fetched_at"] = now
        except Exception as e:
            logger.warning(f"get_customerorder_state_uuid: metadata недоступна ({e}), fallback")
    if cached:
        sid = cached.get(name.strip().lower())
        if sid:
            return sid
    # Fallback
    fallback = {
        "на согласовании": APPROVAL_STATE_ON_APPROVAL,
        "за лимитом":      APPROVAL_STATE_OVER_LIMIT,
        "согласован":      APPROVAL_STATE_AGREED,
    }
    return fallback.get(name.strip().lower())


def _extract_attr_value(attrs: list, attr_id: str):
    """attrs = order['attributes'] или counterparty['attributes']."""
    for a in attrs or []:
        if a.get("id") == attr_id:
            return a.get("value")
    return None


async def load_counterparty_attrs(agent_id: str) -> dict:
    """
    Один GET counterparty с expand=attributes. Возвращает dict с готовыми ключами:
      {site, max, telegram, credit_limit, _raw_attrs}
    _raw_attrs нужен на случай, если потом понадобится прочитать ещё атрибут (например,
    из Wazzup-блока) без второго GET.
    """
    url = f"{MS_BASE}/entity/counterparty/{agent_id}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=get_headers(),
                params={"expand": "attributes"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    logger.error(f"load_counterparty_attrs: {resp.status} для {agent_id}")
                    return {"site": "", "max": "", "telegram": "", "credit_limit": 0, "_raw_attrs": []}
                cp = await resp.json()
    except Exception as e:
        logger.error(f"load_counterparty_attrs: {e}")
        return {"site": "", "max": "", "telegram": "", "credit_limit": 0, "_raw_attrs": []}

    attrs = cp.get("attributes", []) or []
    return {
        "site":         (_extract_attr_value(attrs, ATTR_CP_SITE) or "").strip(),
        "max":          (_extract_attr_value(attrs, ATTR_CP_MAX) or "").strip(),
        "telegram":     (_extract_attr_value(attrs, ATTR_CP_TELEGRAM) or "").strip(),
        "credit_limit": _extract_attr_value(attrs, ATTR_CP_CREDIT_LIMIT) or 0,
        "_raw_attrs":   attrs,
    }


def compute_credit_color(cp_attrs: dict, current_debt: float, order_sum: float) -> dict:
    """
    Блок 1 светофора. cp_attrs = результат load_counterparty_attrs().
    current_debt — текущая дебиторка клиента (₽), order_sum — сумма согласовываемого заказа (₽).
    Возвращает {color, limit, current_debt, order_sum, effective_debt}.
    Цвет:
      🟡 если limit = 0/null («лимит не задан»);
      🟢 если effective_debt ≤ limit;
      🔴 если effective_debt > limit.
    """
    limit_raw = cp_attrs.get("credit_limit") or 0
    try:
        limit = float(limit_raw)
    except (TypeError, ValueError):
        limit = 0.0
    effective_debt = (current_debt or 0.0) + (order_sum or 0.0)
    if limit <= 0:
        color = "yellow"
    elif effective_debt <= limit:
        color = "green"
    else:
        color = "red"
    return {
        "color": color,
        "limit": limit,
        "current_debt": current_debt or 0.0,
        "order_sum": order_sum or 0.0,
        "effective_debt": effective_debt,
    }


async def compute_overdue_color(agent_id: str) -> dict:
    """
    Блок 2 светофора. Просрочка >3 дн → красный.
    Обёртка над существующей get_counterparty_debt: переиспользуем её алгоритм
    (vычитание текущего долга из свежих заказов с datepayplanned).
    Возвращает {color, days, debt}.
    """
    OVERDUE_THRESHOLD_DAYS = 3
    info = await get_counterparty_debt(agent_id)
    debt = info.get("debt", 0) if info else 0
    days = info.get("overdue_days", 0) if info else 0
    if debt > 0 and days > OVERDUE_THRESHOLD_DAYS:
        color = "red"
    else:
        color = "green"
    return {"color": color, "days": days, "debt": debt}


# Кеш текущего сальдо контрагента (используется внутри compute_cashflow_color
# и может пригодиться вызывающей стороне для пересчёта).
async def _fetch_counterparty_report(agent_id: str) -> dict:
    """GET /report/counterparty/{id}. Возвращает сырой JSON ({} при ошибке)."""
    url = f"{MS_BASE}/report/counterparty/{agent_id}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers=get_headers(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    return {}
                return await resp.json()
    except Exception as e:
        logger.error(f"_fetch_counterparty_report: {e}")
        return {}


async def _fetch_incoming_payments(agent_id: str, since_iso: str) -> list[dict]:
    """
    Тянет paymentin + cashin контрагента, фильтрует по moment >= since_iso в Python.
    Пагинация — через meta.nextHref (МС-рекомендуемый способ).
    """
    agent_href = f"{MS_BASE}/entity/counterparty/{agent_id}"
    results: list[dict] = []
    for entity in ("paymentin", "cashin"):
        next_url = (
            f"{MS_BASE}/entity/{entity}?filter=agent={agent_href}"
        )
        try:
            async with aiohttp.ClientSession() as session:
                for _ in range(20):  # safety bound
                    async with session.get(
                        next_url, headers=get_headers(),
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        if resp.status != 200:
                            logger.warning(
                                f"_fetch_incoming_payments[{entity}]: {resp.status}"
                            )
                            break
                        data = await resp.json()
                    for row in data.get("rows", []) or []:
                        m = row.get("moment", "")
                        if m and m >= since_iso:
                            results.append({
                                "moment": m,
                                "sum": (row.get("sum", 0) or 0) / 100,
                                "type": entity,
                            })
                    next_url = data.get("meta", {}).get("nextHref")
                    if not next_url:
                        break
        except Exception as e:
            logger.error(f"_fetch_incoming_payments[{entity}]: {e}")
    return results


async def compute_cashflow_color(agent_id: str, today=None, window_days: int = 30) -> dict:
    """
    Блок 3 светофора (ДДС). Полная формула — в плане 2026-05-21, раздел Фазы 1.

    Логика:
      1. debt_today = -balance / 100 (если balance < 0); иначе клиент в авансе → 🟢.
      2. demands_30d, payments_30d, returns_30d (последние — из агрегата отчёта).
      3. opening_balance(T-30) ≈ debt_today − net_movement, где net = demands − returns − payments.
      4. Сортируем платежи по убыв. даты, копим кумулятив; первое N (дн назад), при котором
         cumulative ≥ opening_balance, — искомое.
      5. Цвет: <20 🟢, 20–29 🟡, ≥30 (или платежи не покрывают) 🔴.
      6. Доп. правило банк-cutoff: если сейчас в МСК до 14:00 — поднимаем красный в жёлтый
         с пометкой bank_pending=True (банк ещё разносится).
    """
    from datetime import datetime, timedelta, timezone

    if today is None:
        today = _now_msk().date()

    since_dt = datetime.combine(today, datetime.min.time()) - timedelta(days=window_days)
    since_iso = since_dt.strftime("%Y-%m-%d %H:%M:%S")

    # 1. Сальдо и агрегаты
    report = await _fetch_counterparty_report(agent_id)
    if not report:
        return {"color": "yellow", "n_days": None, "opening_balance": 0,
                "payments_sum": 0, "current_debt": 0.0, "explain": "отчёт МС недоступен"}
    balance = (report.get("balance", 0) or 0) / 100
    debt_today = -balance if balance < 0 else 0.0

    # 2. Платежи + отгрузки за окно
    payments = await _fetch_incoming_payments(agent_id, since_iso)
    payments_sum = sum(p["sum"] for p in payments)

    demands_sum_30d = 0.0
    try:
        agent_href = f"{MS_BASE}/entity/counterparty/{agent_id}"
        next_url = f"{MS_BASE}/entity/demand?filter=agent={agent_href}"
        async with aiohttp.ClientSession() as session:
            for _ in range(20):
                async with session.get(
                    next_url, headers=get_headers(),
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()
                for row in data.get("rows", []) or []:
                    m = row.get("moment", "")
                    if m and m >= since_iso:
                        demands_sum_30d += (row.get("sum", 0) or 0) / 100
                next_url = data.get("meta", {}).get("nextHref")
                if not next_url:
                    break
    except Exception as e:
        logger.warning(f"compute_cashflow_color: demands {e}")

    # 3. Opening balance (T-window): debt_today − net_movement
    net_movement = demands_sum_30d - payments_sum
    opening = debt_today - net_movement

    # Клиент был в авансе или без долга 30 дней назад — точно 🟢
    if opening <= 0:
        return {"color": "green", "n_days": 0, "opening_balance": opening,
                "payments_sum": payments_sum, "current_debt": debt_today,
                "explain": "клиент в авансе"}

    # 4. Подбираем N: сортируем платежи по дате убыв., копим кумулятив
    today_dt = datetime.combine(today, datetime.min.time())
    by_day: dict[int, float] = {}
    for p in payments:
        m = p["moment"]
        try:
            pdt = datetime.fromisoformat(m.replace(" ", "T").replace(".000", "").replace("Z", ""))
        except Exception:
            continue
        n = (today_dt.date() - pdt.date()).days
        if n < 0:
            n = 0
        by_day[n] = by_day.get(n, 0.0) + p["sum"]

    cum = 0.0
    n_match = None
    for n in sorted(by_day.keys()):
        cum += by_day[n]
        if cum >= opening:
            n_match = n
            break

    # Bank-cutoff: до 14:00 МСК банк ещё может довезти платежи дня
    now_msk = _now_msk()
    bank_pending = now_msk.hour < 14

    if n_match is None:
        color = "yellow" if bank_pending else "red"
        explain = (
            f"платежи за {window_days}д ({payments_sum:,.0f}) не покрывают долг "
            f"({opening:,.0f})"
        )
    elif n_match < 20:
        color = "green"
        explain = f"закрывает долг за {n_match} дн"
    elif n_match < 30:
        color = "yellow"
        explain = f"закрывает долг за {n_match} дн — медленно"
    else:
        color = "yellow" if bank_pending else "red"
        explain = f"закрывает долг за {n_match} дн"

    if bank_pending and color == "yellow" and "медленно" not in explain:
        explain += " (⏳ банк ещё разносится)"

    return {
        "color": color,
        "n_days": n_match,
        "opening_balance": opening,
        "payments_sum": payments_sum,
        "demands_sum": demands_sum_30d,
        "current_debt": debt_today,
        "explain": explain,
        "bank_pending": bank_pending,
    }


async def compute_price_color(order_href: str) -> dict:
    """
    Блок цены в светофоре. Возвращает структурированный список заниженных позиций,
    а не строки (для красивого форматирования в шаблоне алерта).
    Возвращает {color, items: [{name, order_price, min_price, diff_rub, diff_pct, client_type}]}.
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
    items: list[dict] = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                order_href, headers=get_headers(),
                params={"expand": "agent,positions.assortment,state"},
            ) as resp:
                if resp.status != 200:
                    return {"color": "green", "items": []}
                order = await resp.json()

            state_id = (order.get("state") or {}).get("id")
            if state_id in SKIP_STATES:
                return {"color": "green", "items": []}

            agent = order.get("agent") or {}
            tags_lower = [t.lower() for t in (agent.get("tags") or [])]
            if "хорека" in tags_lower:
                price_type_name = "Цена продажи"
                client_type = "хорека"
            elif "опт" in tags_lower:
                price_type_name = "Цена опт"
                client_type = "опт"
            else:
                return {"color": "green", "items": []}

            positions = order.get("positions", {}) or {}
            for pos in (positions.get("rows", []) if isinstance(positions, dict) else []):
                assortment = pos.get("assortment", {}) or {}
                product_id = assortment.get("id", "")
                product_name = assortment.get("name", "")
                order_price = (pos.get("price", 0) or 0) / 100
                if not product_id or order_price <= 0:
                    continue
                async with session.get(
                    f"{MS_BASE}/entity/product/{product_id}", headers=get_headers(),
                ) as r2:
                    if r2.status != 200:
                        continue
                    product_data = await r2.json()
                min_price = None
                for sp in (product_data.get("salePrices", []) or []):
                    if (sp.get("priceType") or {}).get("name", "") == price_type_name:
                        min_price = (sp.get("value", 0) or 0) / 100
                        break
                if not min_price or min_price <= 0:
                    continue
                if order_price < min_price:
                    diff_rub = min_price - order_price
                    diff_pct = diff_rub / min_price * 100
                    items.append({
                        "name": product_name,
                        "order_price": order_price,
                        "min_price": min_price,
                        "diff_rub": diff_rub,
                        "diff_pct": diff_pct,
                        "client_type": client_type,
                    })
    except Exception as e:
        logger.error(f"compute_price_color: {e}")
    return {"color": "red" if items else "green", "items": items}


def compute_payment_date_color(order: dict) -> dict:
    """
    Блок 5 светофора. Дата планируемой оплаты — атрибут на customerorder (НЕ counterparty).
    order — JSON загруженного заказа с expand=attributes.
    Возвращает {color, date_str}.
    """
    from datetime import datetime

    attrs = order.get("attributes", []) or []
    raw = _extract_attr_value(attrs, ATTR_CO_PAYMENT_PLANNED)
    if not raw:
        return {"color": "red", "date_str": ""}
    try:
        # МС отдаёт формат "2026-05-25 00:00:00.000"
        dt = datetime.fromisoformat(str(raw).replace(" ", "T").replace(".000", "").replace("Z", ""))
    except Exception:
        return {"color": "red", "date_str": str(raw)}
    today = _now_msk().date()
    return {
        "color": "green" if dt.date() >= today else "red",
        "date_str": dt.date().strftime("%d.%m.%Y"),
    }


def compute_site_color(cp_attrs: dict) -> dict:
    """
    Блок 6 светофора. Сайт валиден, если содержит ≥3 латинских букв подряд
    (отсекает «нет», «—», «н/д» в кириллице).
    Возвращает {color, raw_value}.
    """
    raw = (cp_attrs.get("site") or "").strip()
    if raw and _RE_SITE_HAS_LATIN.search(raw):
        return {"color": "green", "raw_value": raw}
    return {"color": "red", "raw_value": raw}


def _normalize_phone(raw: str) -> str:
    """+7 (926) 583-10-26 → 79265831026. Превращает префикс +7 в 7."""
    digits = re.sub(r"[^\d]", "", raw or "")
    if digits.startswith("8") and len(digits) == 11:
        return digits
    if digits.startswith("7") and len(digits) == 11:
        return digits
    # +7XXXXXXXXXX → 7XXXXXXXXXX уже сохранён (regex срезал +)
    return digits


def compute_contacts_color(cp_attrs: dict) -> dict:
    """
    Блок 7 светофора. Контакт валиден, если ХОТЯ БЫ ОДНО из двух:
      Max:      11 цифр, начинается с 7 или 8 (формат +7... тоже принимаем — нормализуем).
      Telegram: ^@\\S{5,}$  ИЛИ  ^\\d+$ (но НЕ совпадает с шаблоном телефона)
    Wazzup-сгенерированные атрибуты НЕ учитываем (решение Виктора 2026-05-21).
    Возвращает {color, max, telegram, max_valid, tg_valid}.
    """
    mx_raw = (cp_attrs.get("max") or "").strip()
    tg = (cp_attrs.get("telegram") or "").strip()

    mx_normalized = _normalize_phone(mx_raw)
    max_valid = bool(_RE_PHONE_RU.match(mx_normalized))

    tg_valid = False
    if tg:
        if _RE_TG_USERNAME.match(tg):
            tg_valid = True
        elif _RE_TG_CHAT_ID.match(tg) and not _RE_PHONE_RU.match(tg):
            tg_valid = True

    return {
        "color": "green" if (max_valid or tg_valid) else "red",
        "max": mx_raw, "telegram": tg,
        "max_valid": max_valid, "tg_valid": tg_valid,
    }
