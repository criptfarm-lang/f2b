"""Тесты реактивации сайт-лидов: окно, ротация текстов, payload, сверка цен, стоп-статусы.

План: plans/2026-09-17-реактивация-сайт-лидов-спеццена-мессенджеры.md (репо «второй мозг»).

Запуск: python3 -m pytest test_reactivation_campaign.py -q
"""
from datetime import datetime

import pytest

from reactivation_campaign import (MSK, block_index, build_payload, in_window, lead_blocked,
                                   price_mismatches, render)

CFG = {"days": [0, 1, 2, 3, 4], "start": "10:00", "end": "17:00"}


@pytest.mark.parametrize("dt,expected", [
    (datetime(2026, 9, 21, 10, 0, tzinfo=MSK), True),    # пн, ровно начало
    (datetime(2026, 9, 21, 9, 59, tzinfo=MSK), False),   # пн, до окна
    (datetime(2026, 9, 21, 16, 59, tzinfo=MSK), True),   # пн, последняя минута
    (datetime(2026, 9, 21, 17, 0, tzinfo=MSK), False),   # пн, конец окна не включён
    (datetime(2026, 9, 19, 12, 0, tzinfo=MSK), False),   # сб
    (datetime(2026, 9, 20, 12, 0, tzinfo=MSK), False),   # вс
])
def test_in_window(dt, expected):
    assert in_window(dt, CFG) is expected


def test_block_changes_every_five_sends():
    blocks = [block_index(n, 5, 17) for n in range(83)]
    assert blocks[:5] == [0] * 5
    assert blocks[5:10] == [1] * 5
    assert blocks[80:83] == [16] * 3
    assert max(blocks) == 16


def test_block_wraps_when_texts_run_out():
    assert block_index(85, 5, 17) == 0


def test_render_manager_name():
    assert render("Здравствуйте! {manager}, F2B.", "Карина") == "Здравствуйте! Карина, F2B."


def test_payload_max_with_crm_user():
    row = {"chat_type": "max", "chat_id": "4083305", "responsible_user_id": 12625622}
    p = build_payload(row, "текст", "responsible")
    assert p == {"channelId": "1d5bc70a-7ca6-4895-8d1f-9690cf448214", "chatType": "max",
                 "chatId": "4083305", "text": "текст", "crmUserId": "12625622"}


def test_payload_telegram_without_crm_user():
    row = {"chat_type": "telegram", "chat_id": 5183347829, "responsible_user_id": 12625622}
    p = build_payload(row, "текст", "none")
    assert p["channelId"] == "ddd24a95-9304-4098-a320-3e47fcd1020a"
    assert p["chatId"] == "5183347829" and "crmUserId" not in p


def test_prices_match():
    exp = {"14001": {"spec": 890, "opt": 1050}}
    assert price_mismatches(exp, {"14001": {"spec": 890.0, "opt": 1050.0}}) == []


def test_prices_changed_or_missing():
    exp = {"14001": {"spec": 890, "opt": 1050}, "18001": {"spec": 1100, "opt": 1350}}
    bad = price_mismatches(exp, {"14001": {"spec": 920.0, "opt": 1050.0}})
    assert any("14001 spec" in b for b in bad)
    assert any("18001" in b and "не найден" in b for b in bad)


@pytest.mark.parametrize("lead,blocked", [
    ({"status_id": 142}, True),
    ({"status_id": 143, "loss_reason_id": 23426482}, True),     # Не целевой
    ({"status_id": 143, "loss_reason_id": 23426478}, True),     # Логистика невозможна
    ({"status_id": 143, "loss_reason_id": 23433142}, False),    # Нет движения 7 дней
    ({"status_id": 85554806, "custom_fields_values": [
        {"field_id": 2246177, "values": [{"value": "Логистика невозможна"}]}]}, True),
    ({"status_id": 85554806, "custom_fields_values": [
        {"field_id": 2246177, "values": [{"value": "Дорого"}]}]}, False),
])
def test_lead_blocked(lead, blocked):
    assert (lead_blocked(lead) is not None) is blocked
