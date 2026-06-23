"""Планирование DashaMail-кампании через web-UI (DashaMail JSON API не имеет campaigns.schedule).

Вызывается из bot.py при нажатии inline-кнопки «Запланировать <kind>» в TG-уведомлении
от scripts/dashamail_weekly_send.py.

Принцип:
1. Playwright-сессия с логином victor@f2b.group (env DASHAMAIL_PASSWORD).
2. Переход на wizard.php?id={cid}, открытие шага планирования.
3. Установка времени = ближайший ПН 11:00 МСК через `newprepareSchedule()` JS-вызов.
4. Финальное "Завершить" → подтверждение.

В случае ошибки возвращает {"ok": False, "err": "<reason>", "url": "<wizard_url>"} —
бот шлёт это Виктору, он добивает руками.
"""
from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta
from typing import Any

try:
    from playwright.sync_api import sync_playwright
except ImportError:  # на dev-машинах без playwright
    sync_playwright = None  # type: ignore


def _next_monday_11msk() -> datetime:
    """Ближайший ПН в 11:00 МСК (UTC+3). Если запуск в ПН до 11 — этот же ПН."""
    now = datetime.utcnow()
    # МСК = UTC + 3
    msk = now + timedelta(hours=3)
    days_until_mon = (7 - msk.weekday()) % 7
    if days_until_mon == 0 and msk.hour >= 11:
        days_until_mon = 7
    target_msk = (msk + timedelta(days=days_until_mon)).replace(hour=11, minute=0, second=0, microsecond=0)
    return target_msk


def schedule_campaign(cid: int) -> dict[str, Any]:
    """Планирует DashaMail-кампанию на ближайший ПН 11:00 МСК.

    Возвращает: {"ok": bool, "scheduled_at": "...", "err": "...", "wizard_url": "..."}.
    """
    if sync_playwright is None:
        return {"ok": False, "err": "playwright not installed", "wizard_url": f"https://lk.dashamail.ru/wizard.php?id={cid}"}

    raw = os.environ.get("DASHAMAIL_PASSWORD")
    if not raw:
        return {"ok": False, "err": "DASHAMAIL_PASSWORD env not set"}
    if raw.startswith("b64:"):
        import base64
        password = base64.b64decode(raw[4:]).decode("utf-8")
    else:
        password = raw

    target = _next_monday_11msk()
    # формат для UI: "DD.MM.YYYY HH:MM"
    target_str = target.strftime("%d.%m.%Y %H:%M")
    wizard_url = f"https://lk.dashamail.ru/wizard.php?id={cid}"

    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        ctx = b.new_context(locale="ru-RU")
        page = ctx.new_page()

        # login
        page.goto("https://lk.dashamail.ru/login.php", wait_until="domcontentloaded", timeout=30000)
        time.sleep(2)
        page.fill('input[name="username"]', "victor@f2b.group")
        page.fill('input[name="password"]', password)
        page.click('button[type="submit"]'); time.sleep(7)
        if "login" in page.url.lower():
            b.close()
            return {"ok": False, "err": f"login failed: {page.url}", "wizard_url": wizard_url}

        # wizard
        page.goto(wizard_url, wait_until="domcontentloaded", timeout=45000)
        time.sleep(5)

        # Вызов newprepareSchedule() — функция UI которая открывает форму планирования
        # и принимает date+time через диалог. Используем низкоуровневый AJAX вызов.
        # Известный endpoint из SKILL.md: /wizard/action/save-schedule.php
        result = page.evaluate(
            """async ({cid, dt}) => {
                const body = new URLSearchParams({
                    cid: String(cid),
                    schedule_date: dt,
                    schedule_type: 'once',
                });
                const r = await fetch('/wizard/action/save-schedule.php', {
                    method:'POST',
                    credentials:'include',
                    headers:{'Content-Type':'application/x-www-form-urlencoded'},
                    body: body.toString(),
                });
                return {status: r.status, text: (await r.text()).slice(0, 200)};
            }""",
            {"cid": cid, "dt": target_str},
        )
        b.close()

    ok = bool(result and result.get("status") == 200 and "ok" in (result.get("text", "").lower()))
    return {
        "ok": ok,
        "scheduled_at": target_str + " МСК",
        "err": None if ok else f"save-schedule returned {result}",
        "wizard_url": wizard_url,
    }
