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
    """Скачивает миниатюру фото товара из МойСклад.
    Использует /miniature endpoint — работает через порт 443, без CDN.
    """
    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Шаг 1: получаем href первого изображения
            if "/images" in url and "download" not in url:
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

            # Шаг 2: miniature — превью через основной API (порт 443, без CDN)
            miniature_url = img_href + "/miniature"
            logger.info(f"download_image: fetching {miniature_url}")
            async with session.get(miniature_url, headers=get_headers(),
                                   allow_redirects=True) as resp:
                logger.info(f"download_image: status={resp.status} type={resp.content_type}")
                if resp.status == 200 and "image" in resp.content_type:
                    data = await resp.read()
                    logger.info(f"download_image: got {len(data)} bytes")
                    return data
                else:
                    body = await resp.text()
                    logger.error(f"download_image: error body={body[:300]}")
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
    try:
        async with aiohttp.ClientSession() as session:
            # Шаг 1: найти контрагента по имени — пробуем оригинал и uppercase
            url = f"{MS_BASE}/entity/counterparty"
            rows = []
            for q in [query, query.upper(), query.lower(), query.capitalize()]:
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
                })
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
    "голубева":  "Татьяна Голубева",
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


async def find_counterparty_info(query: str) -> list:
    """Находит контрагента и возвращает его теги, менеджера, тип покупателя и баланс."""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{MS_BASE}/entity/counterparty"
            rows = []
            for q in [query, query.upper(), query.lower(), query.capitalize()]:
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
        "голубева": "голубева",
        "татьяна": "голубева",
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
            agent_name_filter = ""
            if query:
                cp_url = f"{MS_BASE}/entity/counterparty"
                for q in [query, query.upper(), query.lower(), query.capitalize()]:
                    async with session.get(cp_url, headers=get_headers(), params={"filter": f"name~{q}", "limit": 5}) as cr:
                        if cr.status == 200:
                            cp_rows = (await cr.json()).get("rows", [])
                            if cp_rows:
                                agent_href = cp_rows[0].get("meta", {}).get("href", "")
                                if agent_href:
                                    agent_filter = f";agent={agent_href}"
                                break

            # Грузим заказы покупателей постранично
            url = f"{MS_BASE}/entity/customerorder"
            all_orders = []
            offset = 0
            while True:
                params = {
                    "limit": 100,
                    "offset": offset,
                    "expand": "agent",
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

            logger.info(f"get_overdue_demands: {len(all_orders)} total orders loaded")



            by_agent = {}
            for order in all_orders:
                # Дата планируемой оплаты — кастомный атрибут
                ppm = ""
                for attr in order.get("attributes", []):
                    if attr.get("name") == "Дата планируемой оплаты":
                        ppm = attr.get("value", "")
                        break
                if not ppm:
                    continue

                try:
                    due_dt = datetime.fromisoformat(ppm.replace(".000", "").replace("Z", ""))
                    # Добавляем UTC если нет timezone
                    if due_dt.tzinfo is None:
                        due_dt = due_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    continue

                # Просрочена?
                if due_dt >= today_dt:
                    continue

                # Не оплачена?
                total_sum = (order.get("sum", 0) or 0) / 100
                payed_sum = (order.get("payedSum", 0) or 0) / 100
                unpaid = total_sum - payed_sum
                if unpaid <= 0:
                    continue

                agent = order.get("agent", {})
                agent_id = agent.get("id", "")
                agent_name = agent.get("name", "неизвестно")
                agent_tags = agent.get("tags", [])

                # Фильтр по тегу
                if tag:
                    tags_lower = [t.lower() for t in agent_tags]
                    if not any(tag.lower() in t for t in tags_lower):
                        continue

                # Пропускаем розничных покупателей
                if "розничный покупатель" in agent_name.lower():
                    continue

                days_overdue = (today_dt - due_dt).days

                # Пропускаем розничных покупателей
                if "розничный покупатель" in agent_name.lower():
                    continue

                # Определяем менеджера по тегам
                MANAGER_TAG_MAP = {
                    "баласанян": "Карина Баласанян",
                    "скляр": "Инесса Скляр",
                    "мерзлякова": "Елена Мерзлякова",
                    "голубева": "Татьяна Голубева",
                    "леонтьев": "Алексей Леонтьев",
                }
                manager_name = "Без менеджера"
                for t in agent_tags:
                    if t.lower() in MANAGER_TAG_MAP:
                        manager_name = MANAGER_TAG_MAP[t.lower()]
                        break

                if agent_id not in by_agent:
                    by_agent[agent_id] = {
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
            result.sort(key=lambda x: x["overdue_sum"], reverse=True)
            logger.info(f"get_overdue_demands: {len(result)} agents with overdue debt")
            return result

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
        days = c.get("max_days", 0)
        days_str = f"{days} дн." if days > 0 else ""
        header = f"🔴 *{c['name']}* — {fmt_money(c['overdue_sum'])}"
        if days_str:
            header += f" · просрочка {days_str}"
        lines.append(header)
        # Детализация по всем просроченным заказам
        demands = c.get("demands", [])
        if len(demands) > 1:
            for d in demands:
                lines.append(f"   └ {d['name']} · {d['due']} · {fmt_money(d['unpaid'])}")
        lines.append("")

    return "\n".join(lines).rstrip()


def format_debt_reminder(client: dict) -> str:
    """Готовит текст напоминания клиенту об оплате."""
    demands = client.get("demands", [])
    lines = [
        "Добрый день!",
        "",
        "Напоминаем о наличии просроченной задолженности перед компанией F2B PRO:",
        "",
    ]
    for d in demands:
        lines.append(f"• Заказ {d['name']} от {d['due']} — {fmt_money(d['unpaid'])}")
    lines += [
        "",
        f"Итого к оплате: {fmt_money(client['overdue_sum'])}",
        "",
        "Просим произвести оплату в ближайшее время.",
        "По вопросам свяжитесь с вашим менеджером.",
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
