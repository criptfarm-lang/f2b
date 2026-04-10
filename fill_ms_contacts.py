"""
fill_ms_contacts.py
Заполняет доп. поля контрагентов в МойСклад из базы wazzup_contact_map.

Доп. поля (attributeMeta):
  Telegram  : 15052610-34d7-11f1-0a80-1489000ec44a
  WhatsApp  : 1505270f-34d7-11f1-0a80-1489000ec44b
  Max       : 1505236e-34d7-11f1-0a80-1489000ec449

Запуск:
  MS_TOKEN=<токен> python fill_ms_contacts.py [--dry-run] [--company "ООО Пример"]

  --dry-run   Показывает что будет обновлено, не делает запросов к МойСклад
  --company   Обновить только одну компанию (для проверки)
"""

import os
import sys
import csv
import json
import time
import argparse
import urllib.request
import urllib.parse

# ─── Конфигурация ──────────────────────────────────────────────────────────────

MS_TOKEN = os.environ.get("MOYSKLAD_TOKEN", "")
MS_API   = "https://api.moysklad.ru/api/remap/1.2"

ATTR_TELEGRAM  = "15052610-34d7-11f1-0a80-1489000ec44a"
ATTR_WHATSAPP  = "1505270f-34d7-11f1-0a80-1489000ec44b"
ATTR_MAX       = "1505236e-34d7-11f1-0a80-1489000ec449"

CSV_FILE = os.path.join(os.path.dirname(__file__), "contact_decisions.csv")

# ─── Спорные случаи: ручные решения ────────────────────────────────────────────
# Для компаний с разными людьми в разных каналах.
# Здесь выбираем КОГО записать. Если нужно записать ОБОИХ — укажи оба.
# Ключ: точное название компании из МойСклад.
# Значение: {'max': 'имя', 'telegram': 'имя'}  (пустая строка = не записывать)

MANUAL_OVERRIDES = {
    'ООО "БИГ МАМА"': {
        'max': '79510216186',
        'telegram': 'Александр',          # основной закупщик
    },
    'ООО "НОРМА-ПАК"': {
        'max': 'Юлия',
        'telegram': '79258627823',
    },
    'ООО "ПТК ЭКОР-ФИШ"': {
        'max': 'Алексей',
        'telegram': 'Lexa Котов',          # вероятно тот же Алексей
    },
    'ООО "РЫБКА.РУ"': {
        'max': '79158766934',
        'telegram': 'Мария Холод',
    },
    'ООО "СКАЙ-Ф" МРК': {
        'max': 'Анна Ч',
        'telegram': 'Фиш Рай',
    },
    'ООО "ХОЛОД"': {
        'max': 'Дмитрий',
        'telegram': 'Сергей телеграм',
    },
    'ООО "ЧАЙНА НЬЮС АРБАТ"': {
        'max': 'Роман',
        'telegram': 'Анна ЧН Арбат',
    },
    'ООО "ЭРАМ"': {
        'max': '79690729289',
        'telegram': 'Моин',
    },
    'ТОРА ООО': {
        'max': 'Лиза Тора',               # Лиза = Елизавета, один человек
        'telegram': 'Елизавета Тора',
    },
}

# ─── Вспомогательные функции ──────────────────────────────────────────────────

def ms_get(path: str) -> dict:
    url = f"{MS_API}/{path}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {MS_TOKEN}",
        "Accept-Encoding": "gzip",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = resp.read()
        # urllib не декодирует gzip автоматически, но МойСклад отдаёт без него
        # если заголовок всё же пришёл сжатый — раскрываем
        if resp.info().get("Content-Encoding") == "gzip":
            import gzip
            data = gzip.decompress(data)
        return json.loads(data)


def ms_put(path: str, body: dict) -> dict:
    url = f"{MS_API}/{path}"
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=payload, method="PUT", headers={
        "Authorization": f"Bearer {MS_TOKEN}",
        "Content-Type": "application/json",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def find_counterparty(name: str) -> dict | None:
    """Ищет контрагента в МойСклад по точному имени."""
    encoded = urllib.parse.quote(f'name="{name}"')
    result = ms_get(f"entity/counterparty?filter={encoded}&limit=5")
    rows = result.get("rows", [])
    if not rows:
        return None
    # Берём точное совпадение
    for row in rows:
        if row.get("name", "").strip() == name.strip():
            return row
    return rows[0]  # fallback: первый результат


def build_attributes_patch(current_attrs: list, max_val: str, tg_val: str, wa_val: str) -> list:
    """
    Возвращает список атрибутов для PATCH.
    МойСклад требует передавать ВСЕ доп.поля при обновлении — иначе затрёт остальные.
    """
    attr_map = {a["id"]: a for a in current_attrs}

    def make(attr_id: str, value: str) -> dict:
        entry = {"meta": {"href": f"{MS_API}/entity/counterparty/metadata/attributes/{attr_id}",
                          "type": "attributemetadata",
                          "mediaType": "application/json"}}
        if attr_id in attr_map:
            entry["id"] = attr_id
        entry["value"] = value
        return entry

    patches = []
    # Сохраняем существующие атрибуты, которые мы не трогаем
    for a in current_attrs:
        if a["id"] not in (ATTR_MAX, ATTR_TELEGRAM, ATTR_WHATSAPP):
            patches.append(a)

    if max_val:
        patches.append(make(ATTR_MAX, max_val))
    if tg_val:
        patches.append(make(ATTR_TELEGRAM, tg_val))
    if wa_val:
        patches.append(make(ATTR_WHATSAPP, wa_val))

    return patches


# ─── Основная логика ───────────────────────────────────────────────────────────

def load_decisions(csv_path: str) -> list[dict]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def run(dry_run: bool = False, only_company: str | None = None):
    if not MS_TOKEN and not dry_run:
        print("ERROR: Укажи токен МойСклад: export MS_TOKEN=xxxx")
        sys.exit(1)

    decisions = load_decisions(CSV_FILE)

    ok = 0
    skipped = 0
    errors = []

    for row in decisions:
        company = row["company"]

        if only_company and company != only_company:
            continue

        # Применяем ручные переопределения для спорных случаев
        if company in MANUAL_OVERRIDES:
            override = MANUAL_OVERRIDES[company]
            max_val = override.get("max", row["max_contact"])
            tg_val  = override.get("telegram", row["telegram_contact"])
            wa_val  = override.get("whatsapp", row.get("whatsapp_contact", ""))
        else:
            max_val = row["max_contact"]
            tg_val  = row["telegram_contact"]
            wa_val  = row.get("whatsapp_contact", "")

        if not max_val and not tg_val and not wa_val:
            skipped += 1
            continue

        print(f"\n{'[DRY]' if dry_run else '[UPD]'} {company}")
        print(f"  Max: {max_val or '—'}  |  TG: {tg_val or '—'}  |  WA: {wa_val or '—'}")

        if dry_run:
            ok += 1
            continue

        try:
            cp = find_counterparty(company)
            if not cp:
                print(f"  ⚠️  Контрагент не найден в МойСклад: {company!r}")
                errors.append(company)
                continue

            cp_id = cp["id"]
            current_attrs = cp.get("attributes", [])

            new_attrs = build_attributes_patch(current_attrs, max_val, tg_val, wa_val)
            ms_put(f"entity/counterparty/{cp_id}", {"attributes": new_attrs})
            print(f"  ✅ Обновлено (id={cp_id})")
            ok += 1
            time.sleep(0.2)  # не долбим API

        except Exception as e:
            print(f"  ❌ Ошибка: {e}")
            errors.append(company)

    print(f"\n{'='*50}")
    print(f"Обновлено: {ok}  |  Пропущено: {skipped}  |  Ошибок: {len(errors)}")
    if errors:
        print("Ошибки:")
        for e in errors:
            print(f"  - {e}")


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Заполнить доп. поля контрагентов в МойСклад")
    parser.add_argument("--dry-run", action="store_true", help="Только показать, ничего не писать")
    parser.add_argument("--company", type=str, default=None, help="Обновить только одну компанию")
    args = parser.parse_args()
    run(dry_run=args.dry_run, only_company=args.company)
