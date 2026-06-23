#!/usr/bin/env python3
"""F2B еженедельная прайс-рассылка — автомат для Amvera Cron.

Цель: убрать 2-3 ч ручной подготовки в понедельник до 5 мин sanity-check.

Что делает в одном проходе:
1. Считает даты следующей рассылочной недели (ПН-ВС от сегодня + 0..6 дней).
2. Открывает 4 draft-кампании (2 прайса + 2 re-perm) — campaign_ids из конфига.
3. Через Playwright-session:
   - подменяет PDF-attachment свежим из DashaMail storage (по stem-маске имени файла)
   - патчит в HTML <a href="*.pdf"> на новый URL
   - патчит в HTML дату шапки `DD.MM–DD.MM`
   - патчит тему письма subject (через API campaigns.update)
4. Sanity-check: после save повторно качает get_html.php и grep'ает:
   - старый URL PDF: должно быть 0 совпадений
   - старая дата шапки: должно быть 0 совпадений
   - новая дата: ≥1 совпадение
5. Отправляет тестовое письмо на victor@f2b.group для каждого draft.
6. Пишет JSON-отчёт в `.business/operations/dashamail/weekly-sends/YYYY-MM-DD.json`.
7. Шлёт уведомление в TG-бот «Эф» (если задана env BOT_NOTIFY_URL):
   - status_summary, ссылки на preview, кнопка «отправить».

Что НЕ делает:
- НЕ загружает PDF в storage. Загружают `build_pricelist*.py` через filemanager — это
  отдельный конвейер, его автоматизация = отдельная фича.
- НЕ планирует кампании автоматически. DashaMail JSON API не имеет `campaigns.schedule`;
  планирование требует web-UI (DashaMail wizard / newprepareSchedule). Виктор нажимает
  «отправить» в TG-боте → бот вызывает наш wrapper, который через Playwright
  планирует. Это часть C.2 (Amvera Cron + TG-callback).

Запуск локально для проверки:
    python3 scripts/dashamail_weekly_send.py --dry-run
    python3 scripts/dashamail_weekly_send.py  # боевой
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEST_EMAIL = "victor@f2b.group"
# На Amvera /data — persistent mount; локально пишем в репо.
_DEFAULT_OUT = Path("/data/dashamail/weekly-sends") if Path("/data").is_dir() else (
    ROOT / ".business" / "operations" / "dashamail" / "weekly-sends"
)
OUT_DIR = Path(os.environ.get("DASHAMAIL_OUT_DIR", str(_DEFAULT_OUT)))


# campaign_id берём из последней SENT прайс-кампании в DashaMail (auto-discovery).
# Re-perm после Партий 8/9 (NEED-FIX) — выключены до возобновления Виктором,
# поэтому в авто-цикле остаются только 2 прайс-кампании.
PRICE_KINDS = [
    {"key": "opt", "stem": "price_Opt", "subject_match": "F2B ОПТ", "label": "ОПТ-прайс"},
    {"key": "horeca", "stem": "price_HoReCa", "subject_match": "F2B HoReCa", "label": "HoReCa-прайс"},
]


def keychain(s: str, a: str | None = None) -> str:
    """Сначала env (Amvera прод), затем macOS Keychain (локальная отладка).

    Amvera запрещает `!` и кавычки в значениях env. Префикс `b64:` распаковывается.
    """
    import base64
    env_map = {
        "f2b-dashamail-api-key": "DASHAMAIL_API_KEY",
        "f2b-dashamail": "DASHAMAIL_PASSWORD",
    }
    env_name = env_map.get(s, s.upper().replace("-", "_"))
    v = os.environ.get(env_name)
    if v:
        if v.startswith("b64:"):
            return base64.b64decode(v[4:]).decode("utf-8")
        return v
    cmd = ["security", "find-generic-password", "-s", s, "-w"]
    if a:
        cmd[3:3] = ["-a", a]
    return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip()


def dasha_api_key() -> str:
    return keychain("f2b-dashamail-api-key")


def dasha_call(method: str, **params) -> dict:
    params.update(method=method, api_key=dasha_api_key(), format="json")
    url = "https://api.dashamail.com/?" + urllib.parse.urlencode(params)
    return json.loads(urllib.request.urlopen(url, timeout=30).read().decode())


def next_send_week(today: date | None = None) -> tuple[date, date]:
    """Возвращает (ПН, ВС) ближайшей рассылочной недели.

    Если запуск в ПН — это та же неделя. Если в любой другой день — следующая ПН.
    """
    today = today or date.today()
    days_ahead = (7 - today.weekday()) % 7  # 0=пн, 1=вт, ..., 6=вс
    if days_ahead == 0 and today.weekday() != 0:
        days_ahead = 7
    monday = today + timedelta(days=days_ahead)
    sunday = monday + timedelta(days=6)
    return monday, sunday


def format_date_header(mon: date, sun: date) -> str:
    """`22.06–28.06` — короткое тире, не длинное (правило проекта)."""
    return f"{mon:%d.%m}–{sun:%d.%m}"


def find_latest_sent_prices() -> dict[str, dict]:
    """Возвращает {kind: campaign_dict} для последней SENT каждого типа."""
    r = dasha_call("campaigns.get", state="SENT", limit=50, sort="send_date", order="desc")
    data = (r.get("response") or {}).get("data") or []
    out: dict[str, dict] = {}
    for kind in PRICE_KINDS:
        for c in data:
            name = (c.get("name") or "")
            if kind["subject_match"] in name and "прайс" in name.lower() and kind["key"] not in out:
                out[kind["key"]] = c
                break
    return out


def copy_campaign_for_kind(kind: dict, source_cid: int) -> int | None:
    """Делает campaigns.copy и возвращает id новой draft-копии."""
    r = dasha_call("campaigns.copy", campaign_id=source_cid)
    msg = (r.get("response") or {}).get("msg") or {}
    if msg.get("err_code") != 0:
        print(f"  ERR copy {source_cid}: {msg}")
        return None
    time.sleep(2)
    # Поиск свежей draft
    r2 = dasha_call("campaigns.get", state="DRAFT", limit=10, sort="id", order="desc")
    items = (r2.get("response") or {}).get("data") or []
    items.sort(key=lambda c: int(c.get("id", 0)), reverse=True)
    if not items:
        return None
    return int(items[0]["id"])


def patch_subject(cid: int, kind: dict, week_label: str) -> bool:
    """Меняет subject draft-копии (DashaMail НЕ обновляет subject при копии)."""
    new_subject = f"F2B {'ОПТ' if kind['key']=='opt' else 'HoReCa'} — прайс {week_label}.2026"
    r = dasha_call("campaigns.update", campaign_id=cid, subject=new_subject)
    err = (r.get("response") or {}).get("msg", {}).get("err_code")
    return err == 0


def get_pricelist_pdf_from_postgres(kind_key: str) -> dict | None:
    """Опционально: возвращает {filename, sha256, url} свежего PDF из Amvera Postgres
    `publishing.pricelists`. Используется для верификации даты PDF.

    Сейчас просто returns None — функция-заглушка для C.1. В реальном проде
    PDF уже в DashaMail storage (загружен через build_pricelist_opt / build_pricelist).
    Эта функция полезна для матчинга stem→URL когда нужна точная привязка к версии.
    """
    return None


def run_playwright_patches(plan: dict, dry_run: bool) -> dict:
    """Запускает Playwright-сессию, патчит каждую draft-копию.

    Возвращает report dict.
    """
    if dry_run:
        return {"dry_run": True, "skipped_playwright": True}

    # Импорт ленивый, чтобы --dry-run работал без playwright
    from playwright.sync_api import sync_playwright

    pwd = keychain("f2b-dashamail", "victor@f2b.group")
    report: dict = {"drafts": {}}

    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        ctx = b.new_context(locale="ru-RU")
        page = ctx.new_page()

        page.goto("https://lk.dashamail.ru/login.php", wait_until="domcontentloaded", timeout=30000)
        time.sleep(2)
        page.fill('input[name="username"]', "victor@f2b.group")
        page.fill('input[name="password"]', pwd)
        page.click('button[type="submit"]'); time.sleep(7)
        if "login" in page.url.lower():
            raise RuntimeError(f"login failed: {page.url}")

        for kind in PRICE_KINDS:
            cid = plan["drafts"].get(kind["key"], {}).get("new_cid")
            if not cid:
                continue
            print(f"\n=== {kind['label']} CID={cid} ===")

            # 1. attach свежий PDF из storage
            page.goto(f"https://lk.dashamail.ru/wizard.php?id={cid}", wait_until="domcontentloaded", timeout=45000)
            time.sleep(5)
            page.evaluate("() => dijit.byId('attach-dialog').show()"); time.sleep(2)
            page.evaluate("""() => {
                $('#filemanager-container').css({opacity:'1','z-index':'1000000'}).fadeIn();
                document.getElementById('filemanager-container').contentWindow.SearchImages('attachments');
            }"""); time.sleep(5)
            files = page.evaluate("""() => {
                const ifr = document.getElementById('filemanager-container');
                const doc = ifr.contentDocument;
                const items = doc.querySelectorAll('[id^="gridImage-"]');
                return Array.from(items).map(el => {
                    const text = (el.innerText||'').trim();
                    const fname = (text.match(/[^\\s]+\\.pdf/i)||[null])[0];
                    return {id: el.id, fname};
                });
            }""")
            matches = [f for f in files if f["fname"] and kind["stem"] in f["fname"]]
            if not matches:
                report["drafts"][kind["key"]] = {"ok": False, "err": f"no PDF stem={kind['stem']} in storage"}
                continue
            picked = matches[0]
            print(f"  pick: {picked}")
            page.evaluate("""(grid_id) => {
                const ifr = document.getElementById('filemanager-container');
                const win = ifr.contentWindow;
                const doc = ifr.contentDocument;
                const el = doc.getElementById(grid_id);
                const cb = el.querySelector('input[type="checkbox"]') ||
                           el.querySelector('label[for]') ||
                           el.querySelector('[class*="checkbox"]');
                if (cb) cb.click(); else el.click();
                if (typeof win.useImage === 'function') win.useImage();
            }""", picked["id"])
            time.sleep(4)
            new_url = f"https://storage.dashamail.ru/files/356881/attachments/{picked['fname']}"

            # 2. удалить ВСЕ старые attachments
            page.evaluate("() => dijit.byId('attach-dialog').show()"); time.sleep(2)
            page.evaluate("""(keep) => {
                const dlg = document.getElementById('attach-dialog');
                const links = Array.from(dlg.querySelectorAll('a[href*=".pdf"]'));
                for (const a of links) {
                    if (a.href === keep) continue;
                    const row = a.closest('tr,div,li') || a.parentElement;
                    const delBtn = row && row.querySelector('[onclick*="delete_att"]');
                    if (delBtn) {
                        const m = (delBtn.getAttribute('onclick')||'').match(/delete_att\\(([^,]+),([^\\)]+)\\)/);
                        if (m) { try { delete_att(m[1].trim(), m[2].trim()); } catch(e){} }
                    }
                }
            }""", new_url)
            time.sleep(2)

            # 3. patch HTML — href + дата + title
            html = page.evaluate(
                """async (cid) => (await fetch(`/wizard/action/get_html.php?cid=${cid}`, {credentials:'include'})).text()""",
                cid,
            )
            broad = re.compile(r"https://storage\.dashamail\.ru/files/356881/attachments/[^\"']+\.pdf", re.I)
            before_urls = set(broad.findall(html))
            for u in before_urls:
                if u != new_url:
                    html = html.replace(u, new_url)
            # дата (любой вид тире)
            date_pat = re.compile(r"\d{2}\.\d{2}\s*[–—\-]\s*\d{2}\.\d{2}")
            old_dates = set(date_pat.findall(html))
            for d in old_dates:
                html = html.replace(d, plan["week_label"])

            save = page.evaluate(
                """async ({cid, html}) => {
                    const body = 'edit_html=' + encodeURIComponent(html);
                    const r = await fetch(`/wizard/action/html-save.php?cid=${cid}`, {
                        method:'POST', credentials:'include',
                        headers:{'Content-Type':'application/x-www-form-urlencoded'},
                        body
                    });
                    return {status: r.status, text: (await r.text()).slice(0,80)};
                }""",
                {"cid": cid, "html": html},
            )

            # 4. sanity-check
            html2 = page.evaluate(
                """async (cid) => (await fetch(`/wizard/action/get_html.php?cid=${cid}`, {credentials:'include'})).text()""",
                cid,
            )
            stale_urls = [u for u in broad.findall(html2) if u != new_url]
            stale_dates = [d for d in date_pat.findall(html2) if d != plan["week_label"]]
            new_date_count = html2.count(plan["week_label"])
            sanity_ok = (len(stale_urls) == 0) and (len(stale_dates) == 0) and (new_date_count >= 1)

            # 5. подменить subject
            subject_ok = patch_subject(cid, kind, plan["week_label"])

            # 6. test-mail
            r_test = page.evaluate(
                """async (u) => { const r = await fetch(u,{credentials:'include'}); return {status:r.status, text:(await r.text()).slice(0,80)}; }""",
                f"https://lk.dashamail.ru/wizard/action/send-test.php?test-email={TEST_EMAIL}&cid={cid}",
            )

            report["drafts"][kind["key"]] = {
                "cid": cid,
                "kind": kind["label"],
                "pdf_file": picked["fname"],
                "new_url": new_url,
                "stale_urls_after_save": stale_urls,
                "stale_dates_after_save": stale_dates,
                "new_date_count": new_date_count,
                "sanity_ok": sanity_ok,
                "subject_ok": subject_ok,
                "save_status": save.get("status"),
                "test_email_sent_to": TEST_EMAIL,
                "test_email_result": r_test,
                "preview_url": f"https://lk.dashamail.ru/wizard.php?id={cid}",
            }
        b.close()
    return report


def notify_telegram(plan: dict, report: dict) -> None:
    """Шлёт уведомление Виктору через прямой Telegram Bot API.

    Env: `TG_BOT_TOKEN` (тот же что у бота «Эф»), `OWNER_CHAT_ID`.
    Бот «Эф» получит callback_query через свой long-polling и обработает
    через CallbackQueryHandler(pattern="^dashamail:").
    """
    token = os.environ.get("TG_BOT_TOKEN")
    owner = os.environ.get("OWNER_CHAT_ID")
    if not token or not owner:
        print("[telegram] TG_BOT_TOKEN или OWNER_CHAT_ID не заданы — пропускаю")
        return
    drafts = report.get("drafts", {})
    sanity_all = drafts and all(d.get("sanity_ok") and d.get("subject_ok") for d in drafts.values())
    title = f"📧 Прайс-рассылка {plan['week_label']} — {'готова' if sanity_all else 'нужна правка'}"
    lines = [title, ""]
    for key, d in drafts.items():
        flag = "✅" if d.get("sanity_ok") and d.get("subject_ok") else "⚠️"
        lines.append(f"{flag} {d['kind']}")
        lines.append(f"   {d.get('preview_url', '')}")
        if not d.get("sanity_ok"):
            if d.get("stale_urls_after_save"):
                lines.append(f"   ⚠ stale PDF urls: {len(d['stale_urls_after_save'])}")
            if d.get("stale_dates_after_save"):
                lines.append(f"   ⚠ stale dates: {d['stale_dates_after_save']}")
    text = "\n".join(lines)

    inline_kb: list[list[dict]] = []
    if sanity_all:
        for key, d in drafts.items():
            inline_kb.append([{
                "text": f"📤 Запланировать {d['kind']}",
                "callback_data": f"dashamail:schedule:{d['cid']}",
            }])

    payload = {
        "chat_id": int(owner),
        "text": text,
        "disable_web_page_preview": True,
    }
    if inline_kb:
        payload["reply_markup"] = json.dumps({"inline_keyboard": inline_kb})

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    req = urllib.request.Request(
        url,
        data=urllib.parse.urlencode(payload).encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        resp = urllib.request.urlopen(req, timeout=20).read().decode()
        ok = json.loads(resp).get("ok")
        print(f"[telegram] sendMessage ok={ok}")
    except Exception as e:
        print(f"[telegram] sendMessage FAILED: {e}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="не вызывать playwright, не редактировать draft, только напечатать план")
    ap.add_argument("--date", help="дата запуска в формате YYYY-MM-DD (для тестов)")
    args = ap.parse_args()

    today = datetime.fromisoformat(args.date).date() if args.date else date.today()
    mon, sun = next_send_week(today)
    week_label = format_date_header(mon, sun)
    print(f"=== F2B weekly send. today={today.isoformat()} target week={mon}…{sun} label='{week_label}' ===")

    # 1. найти последние SENT прайс-кампании
    latest = find_latest_sent_prices()
    if len(latest) < 2:
        print(f"ERR: найдено {len(latest)}/2 SENT прайс-кампаний — нечего копировать. abort.")
        sys.exit(2)

    plan: dict = {
        "today": today.isoformat(),
        "week_label": week_label,
        "monday": mon.isoformat(),
        "sunday": sun.isoformat(),
        "drafts": {},
    }
    for kind in PRICE_KINDS:
        c = latest.get(kind["key"])
        if not c:
            print(f"  {kind['label']}: не нашёл SENT источник")
            continue
        print(f"  {kind['label']}: source CID={c['id']} name={(c.get('name') or '')[:80]}")
        if args.dry_run:
            plan["drafts"][kind["key"]] = {
                "source_cid": int(c["id"]),
                "new_cid": None,
                "dry_run": True,
            }
            continue
        new_cid = copy_campaign_for_kind(kind, int(c["id"]))
        plan["drafts"][kind["key"]] = {"source_cid": int(c["id"]), "new_cid": new_cid}
        print(f"    new draft CID={new_cid}")

    # 2. playwright patches
    report = run_playwright_patches(plan, dry_run=args.dry_run)

    # 3. сохранить отчёт
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"{today.isoformat()}.json"
    out_path.write_text(json.dumps({"plan": plan, "report": report}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nreport -> {out_path}")

    # 4. telegram notify (no-op если BOT_NOTIFY_URL не задан)
    if not args.dry_run:
        notify_telegram(plan, report)


if __name__ == "__main__":
    main()
