#!/usr/bin/env python3
"""Валидатор email-адресов для DashaMail-пайплайна F2B.

Применять ОБЯЗАТЕЛЬНО перед записью контактов в любой DashaMail-список:
- импорт из amoCRM/МойСклад
- нарезка партий re-permission
- любой ручной CSV-импорт

Правила сформулированы 22.06.2026 после инцидента Партии 7 (11.5% hard-bounce,
BLOCKED-SPAM). 7.7% master 9007 — мусор по синтаксису, ещё ~14% — мёртвые ящики
(видно только через bounce-репорты).

Использование как библиотеки:
    from dashamail_validate import is_valid_email, clean_list

    keep, removed = clean_list(rows, email_key='email')
    # removed = [(row, reason), ...]

Использование из CLI:
    python3 scripts/dashamail_validate.py path/to/contacts.csv
    python3 scripts/dashamail_validate.py path/to/contacts.csv --email-col email
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from typing import Iterable

# Локальная часть — телефонный номер в формате +7XXXXXXXXXX или 8XXXXXXXXXX.
_PHONE_LOCAL = re.compile(r"^\+?[78]\d{10}$")
# Локальная часть — только цифры (00531@, 12345@).
_DIGITS_ONLY = re.compile(r"^\d+$")
# Хотя бы одна буква (латиница или кириллица).
_HAS_LETTER = re.compile(r"[a-zA-Zа-яА-Я]")
# Локальная часть слишком короткая (<3 символов).
_SHORT_LOCAL_THRESHOLD = 2  # ≤2 = слишком коротко


def is_valid_email(addr: str) -> tuple[bool, str | None]:
    """Возвращает (valid, reason_if_invalid).

    Правила (по приоритету):
    1. no_at — нет '@' или пусто
    2. domain_no_dot — в домене нет точки (mailru, yandexru)
    3. phone_local — локалка = телефон
    4. digits_only — локалка = только цифры
    5. no_letters — в локальной части ни одной буквы (+++@, --@)

    `local_lt3` НЕ режется автоматом: «ak@», «zy@» могут быть валидными
    короткими ником ресторана. После A.3 если выяснится много bounce — добавим.
    """
    e = (addr or "").strip().lower()
    if not e or "@" not in e:
        return False, "no_at"
    local, _, domain = e.rpartition("@")
    if "." not in domain:
        return False, "domain_no_dot"
    if _PHONE_LOCAL.match(local):
        return False, "phone_local"
    if _DIGITS_ONLY.match(local):
        return False, "digits_only"
    if not _HAS_LETTER.search(local):
        return False, "no_letters"
    return True, None


def clean_list(
    rows: Iterable[dict],
    email_key: str = "email",
) -> tuple[list[dict], list[tuple[dict, str]]]:
    """Делит rows на keep + removed (с причиной)."""
    keep: list[dict] = []
    removed: list[tuple[dict, str]] = []
    for r in rows:
        ok, reason = is_valid_email(r.get(email_key, ""))
        if ok:
            keep.append(r)
        else:
            removed.append((r, reason or "unknown"))
    return keep, removed


def _cli() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("csv_path")
    p.add_argument("--email-col", default="email")
    p.add_argument("--out-keep", default=None)
    p.add_argument("--out-remove", default=None)
    args = p.parse_args()

    with open(args.csv_path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    keep, removed = clean_list(rows, email_key=args.email_col)

    print(f"всего:   {len(rows)}")
    print(f"keep:    {len(keep)}")
    print(f"remove:  {len(removed)}")

    from collections import Counter
    cnt = Counter(reason for _, reason in removed)
    for reason, n in cnt.most_common():
        print(f"  {reason:20s} {n}")

    if args.out_keep:
        with open(args.out_keep, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(keep)
    if args.out_remove:
        cols = list(rows[0].keys()) + ["_reason"]
        with open(args.out_remove, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r, reason in removed:
                w.writerow({**r, "_reason": reason})


if __name__ == "__main__":
    _cli()
