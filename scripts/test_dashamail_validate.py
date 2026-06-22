"""Юнит-тесты валидатора email для DashaMail-пайплайна.

Запуск: python3 -m pytest scripts/test_dashamail_validate.py -v
"""
from __future__ import annotations

import pytest

from dashamail_validate import clean_list, is_valid_email


# (email, ожидается valid, ожидаемая причина)
CASES = [
    # — валидные —
    ("chef@vkusnyaev.ru", True, None),
    ("zakaz@restoran-msk.com", True, None),
    ("ak@tabletalk.ru", True, None),  # local_lt3 не режем
    ("a1@bk.ru", True, None),  # есть буква, короткая локалка
    ("info@horeca.spb.ru", True, None),  # многоточечный домен
    ("Иван@yandex.ru", True, None),  # кириллица в локалке
    # — синтаксис —
    ("", False, "no_at"),
    ("no-at-here", False, "no_at"),
    ("local@nodot", False, "domain_no_dot"),
    # — телефон —
    ("+79153504603@mail.ru", False, "phone_local"),
    ("89153504603@yandex.ru", False, "phone_local"),
    ("79153504603@bk.ru", False, "phone_local"),
    # — цифры —
    ("0005333@mail.ru", False, "digits_only"),
    ("12345@gmail.com", False, "digits_only"),
    # — без букв —
    ("+++++@mail.ru", False, "no_letters"),
    ("---@mail.ru", False, "no_letters"),
    ("+-+@mail.ru", False, "no_letters"),
]


@pytest.mark.parametrize("addr,expected_valid,expected_reason", CASES)
def test_is_valid_email(addr, expected_valid, expected_reason):
    ok, reason = is_valid_email(addr)
    assert ok == expected_valid, f"{addr} → valid={ok} (ждали {expected_valid})"
    if not expected_valid:
        assert reason == expected_reason, (
            f"{addr} → reason={reason} (ждали {expected_reason})"
        )


def test_clean_list_splits():
    rows = [
        {"email": "good@mail.ru", "name": "ресторан А"},
        {"email": "+79991112233@mail.ru", "name": "телефон"},
        {"email": "12345@gmail.com", "name": "цифры"},
        {"email": "another@yandex.ru", "name": "ресторан Б"},
    ]
    keep, removed = clean_list(rows)
    assert len(keep) == 2
    assert {r["email"] for r in keep} == {"good@mail.ru", "another@yandex.ru"}
    assert len(removed) == 2
    reasons = {reason for _, reason in removed}
    assert reasons == {"phone_local", "digits_only"}


def test_clean_list_respects_custom_key():
    rows = [
        {"E-mail": "good@mail.ru"},
        {"E-mail": "+78001234567@bk.ru"},
    ]
    keep, removed = clean_list(rows, email_key="E-mail")
    assert len(keep) == 1
    assert len(removed) == 1


def test_case_insensitive():
    # E-mail может прийти в верхнем регистре
    ok, _ = is_valid_email("CHEF@VKUSNYAEV.RU")
    assert ok is True


def test_whitespace_strip():
    ok, _ = is_valid_email("  chef@mail.ru  ")
    assert ok is True
