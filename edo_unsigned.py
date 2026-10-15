"""
Неподписанные документы ЭДО (Диадок) — строка «ЭДО» в светофоре заказа и сводка Рудневой.
План (второй мозг): plans/2026-10-15-чз-эдо-неподписанные-документы.md.

Решения собственника 15.10.2026: долг — все документы, которые ждут подписи клиента
(УПД, акты сверки, договоры, неформализованные); в светофоре — отдельная строка «ЭДО»
с красным кружком, без блокировки; Руднева получает сводку для работы через менеджеров.

Доступ к Диадоку — своё приложение в Кабинете интегратора (clientId ci_FISh-TU-BIZNES),
вход Контур.ID. refresh_token РОТИРУЕТСЯ при каждом продлении: храним его только в
public.diadoc_auth и продлеваем под advisory-lock, иначе вторая копия протухнет.
"""
from __future__ import annotations

import asyncio
import collections
import logging
import os
from datetime import date, datetime, timedelta, timezone

import aiohttp
import psycopg2
import psycopg2.extras

from database import CONNECT_TIMEOUT_SEC, STATEMENT_TIMEOUT_MS

logger = logging.getLogger(__name__)

IDENTITY_URL = "https://identity.kontur.ru/connect/token"
API = "https://diadoc-api.kontur.ru"
BOX_ID = os.getenv("DIADOC_BOX_ID", "967e1549f39744df82c37254b5b50393@diadoc.ru")
CLIENT_ID = os.getenv("DIADOC_CLIENT_ID", "ci_FISh-TU-BIZNES")
CATEGORY = "Any.OutboundWaitingForRecipientSignature"
# Не долг клиента: документ отзываем мы сами.
_SKIP_STATUS = ("Ожидается аннулирование",)
_LOCK_KEY = 815_2026_10  # pg_advisory_xact_lock для продления токена

# Менеджеры ОП — теги контрагента в МС (как в moysklad.PDZ_MANAGER_TG_IDS).
_MANAGER_TAGS = ("баласанян", "скляр", "белов", "дьяченко", "коликов", "кормилицын",
                 "мерзлякова", "фугу")

DDL = """
create table if not exists public.diadoc_auth (
    id            int primary key default 1,
    refresh_token text not null,
    updated_at    timestamptz default now()
);
create table if not exists public.edo_orgs (
    box_id     text primary key,
    inn        text,
    kpp        text,
    name       text,
    updated_at timestamptz default now()
);
create table if not exists public.edo_unsigned (
    entity_id  text primary key,
    box_id     text not null,
    inn        text,
    cp_name    text,
    doc_type   text,
    doc_number text,
    doc_date   date,
    total_sum  numeric,
    status     text,
    sent_at    timestamptz,
    synced_at  timestamptz default now()
);
create index if not exists edo_unsigned_inn on public.edo_unsigned (inn);
create table if not exists public.edo_state (
    id        int primary key default 1,
    synced_at timestamptz,
    total     int
);
"""

_conn = None


def _db():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(os.environ["DATABASE_URL"],
                                 cursor_factory=psycopg2.extras.RealDictCursor,
                                 connect_timeout=CONNECT_TIMEOUT_SEC,
                                 options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS}")
        _conn.autocommit = True
        with _conn.cursor() as cur:
            cur.execute(DDL)
    return _conn


# ── токен ────────────────────────────────────────────────────────────────────
_access: dict = {"token": None, "exp": datetime.min.replace(tzinfo=timezone.utc)}


async def _access_token() -> str:
    """Access-токен Диадока. Продление — в транзакции под advisory-lock: читаем
    refresh_token, меняем на новый, сохраняем до коммита (ротация)."""
    now = datetime.now(timezone.utc)
    if _access["token"] and _access["exp"] > now + timedelta(minutes=5):
        return _access["token"]
    secret = os.getenv("DIADOC_CLIENT_SECRET")
    if not secret:
        raise RuntimeError("DIADOC_CLIENT_SECRET не задан")
    _db()  # схема
    # Отдельное соединение: внутри транзакции есть await (HTTP), общий _conn в это
    # время могут использовать другие задачи бота.
    conn = psycopg2.connect(os.environ["DATABASE_URL"],
                            cursor_factory=psycopg2.extras.RealDictCursor,
                            connect_timeout=CONNECT_TIMEOUT_SEC)
    try:
        with conn.cursor() as cur:
            cur.execute("select pg_advisory_xact_lock(%s)", (_LOCK_KEY,))
            cur.execute("select refresh_token from public.diadoc_auth where id=1")
            row = cur.fetchone()
            if not row:
                raise RuntimeError("нет refresh_token Диадока в diadoc_auth")
            async with aiohttp.ClientSession() as s:
                async with s.post(IDENTITY_URL, data={
                    "grant_type": "refresh_token", "client_id": CLIENT_ID,
                    "client_secret": secret, "refresh_token": row["refresh_token"],
                }, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    data = await r.json(content_type=None)
                    if r.status != 200:
                        raise RuntimeError(f"Контур.ID {r.status}: {str(data)[:200]}")
            if data.get("refresh_token"):
                cur.execute("update public.diadoc_auth set refresh_token=%s, updated_at=now() "
                            "where id=1", (data["refresh_token"],))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    _access["token"] = data["access_token"]
    _access["exp"] = now + timedelta(seconds=int(data.get("expires_in") or 3600))
    return _access["token"]


async def _get(session, path: str, params: dict) -> dict:
    tok = await _access_token()
    async with session.get(f"{API}{path}", params=params,
                           headers={"Authorization": f"Bearer {tok}", "Accept": "application/json"},
                           timeout=aiohttp.ClientTimeout(total=60)) as r:
        if r.status != 200:
            raise RuntimeError(f"Диадок {path} {r.status}: {(await r.text())[:200]}")
        return await r.json(content_type=None)


# ── выгрузка ─────────────────────────────────────────────────────────────────
def _meta(d: dict, key: str):
    return next((m.get("Value") for m in d.get("Metadata") or [] if m.get("Key") == key), None)


def _status(d: dict) -> str:
    return ((d.get("DocflowStatus") or {}).get("PrimaryStatus") or {}).get("StatusText") or ""


def _dedupe(docs: list[dict]) -> list[dict]:
    """Один и тот же документ (контрагент + тип + номер + дата) отправляли повторно —
    оставляем последнюю отправку. Без номера (PDF и т.п.) — каждый документ отдельно."""
    best: dict = {}
    for d in docs:
        if _status(d).startswith(_SKIP_STATUS):
            continue
        num, dt = d.get("DocumentNumber"), d.get("DocumentDate")
        key = (d["CounteragentBoxId"], d.get("DocumentType"), num, dt) if num and dt else d["EntityId"]
        if key not in best or (d.get("CreationTimestamp") or "") > (best[key].get("CreationTimestamp") or ""):
            best[key] = d
    return list(best.values())


async def sync() -> int:
    """Полная выгрузка «ждёт подписи получателя» → public.edo_unsigned (замена снимка)."""
    docs: dict = {}
    after = None
    async with aiohttp.ClientSession() as s:
        for _ in range(200):
            params = {"boxId": BOX_ID, "filterCategory": CATEGORY}
            if after:
                params["afterIndexKey"] = after
            page = (await _get(s, "/V3/GetDocuments", params)).get("Documents") or []
            new = 0
            for d in page:
                if d["EntityId"] not in docs:
                    docs[d["EntityId"]] = d
                    new += 1
            if len(page) < 100 or not new:
                break
            after = page[-1].get("IndexKey")
        rows = _dedupe(list(docs.values()))

        conn = _db()
        with conn.cursor() as cur:
            cur.execute("select box_id from public.edo_orgs")
            known = {r["box_id"] for r in cur.fetchall()}
        for box in {d["CounteragentBoxId"] for d in rows} - known:
            try:
                o = await _get(s, "/GetOrganization", {"boxId": box})
                with conn.cursor() as cur:
                    cur.execute("""insert into public.edo_orgs (box_id, inn, kpp, name)
                                   values (%s, %s, %s, %s) on conflict (box_id) do nothing""",
                                (box, o.get("Inn"), o.get("Kpp"),
                                 o.get("ShortName") or o.get("FullName")))
            except Exception as e:
                logger.warning(f"edo_unsigned: организация {box}: {e}")

    def _date(v):
        try:
            return datetime.strptime(v, "%d.%m.%Y").date()
        except Exception:
            return None

    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("select box_id, inn, name from public.edo_orgs")
            orgs = {r["box_id"]: r for r in cur.fetchall()}
            cur.execute("delete from public.edo_unsigned")
            vals = []
            for d in rows:
                o = orgs.get(d["CounteragentBoxId"]) or {}
                try:
                    total = float(_meta(d, "TotalSum")) if _meta(d, "TotalSum") else None
                except ValueError:
                    total = None
                vals.append((d["EntityId"], d["CounteragentBoxId"], o.get("inn"), o.get("name"),
                             d.get("DocumentType"), d.get("DocumentNumber"),
                             _date(d.get("DocumentDate")), total, _status(d),
                             d.get("CreationTimestamp")))
            # одной пачкой — синхронный psycopg2 не должен надолго держать event loop
            psycopg2.extras.execute_values(cur, """insert into public.edo_unsigned
                (entity_id, box_id, inn, cp_name, doc_type, doc_number, doc_date,
                 total_sum, status, sent_at) values %s""", vals, page_size=500)
            cur.execute("""insert into public.edo_state (id, synced_at, total) values (1, now(), %s)
                           on conflict (id) do update set synced_at=now(), total=excluded.total""",
                        (len(rows),))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = True
    logger.info(f"edo_unsigned: синхронизировано {len(rows)} документов")
    return len(rows)


# ── светофор заказа ──────────────────────────────────────────────────────────
_TYPE_LABEL = {"UniversalTransferDocument": "УПД", "ReconciliationAct": "акт сверки",
               "Contract": "договор", "SupplementaryAgreement": "допсоглашение"}


def _kind(doc_type: str, number: str | None) -> str:
    if doc_type in _TYPE_LABEL:
        return _TYPE_LABEL[doc_type]
    return "прочие"


def client_debt(inn: str) -> dict | None:
    """Неподписанные документы клиента по ИНН. None — данных ещё нет (не синхронизировано)."""
    conn = _db()
    with conn.cursor() as cur:
        cur.execute("select synced_at from public.edo_state where id=1")
        st = cur.fetchone()
        if not st or not st["synced_at"]:
            return None
        cur.execute("""select doc_type, doc_number, doc_date, total_sum, sent_at
                       from public.edo_unsigned where inn=%s""", (inn,))
        rows = cur.fetchall()
    kinds = collections.Counter(_kind(r["doc_type"], r["doc_number"]) for r in rows)
    dates = [r["doc_date"] or (r["sent_at"].date() if r["sent_at"] else None) for r in rows]
    dates = [x for x in dates if x]
    return {"count": len(rows), "kinds": kinds,
            "sum": sum(float(r["total_sum"] or 0) for r in rows if r["doc_type"] == "UniversalTransferDocument"),
            "oldest": min(dates) if dates else None, "synced_at": st["synced_at"]}


def _fmt_money(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ")


def format_line(info: dict | None) -> tuple[str | None, str | None]:
    """(цвет, строка) для светофора. None — строку не показываем."""
    if info is None:
        return None, None
    if not info["count"]:
        return "green", "🟢 *ЭДО:* всё подписано"
    parts = [f"{k} {n}" for k, n in info["kinds"].most_common()]
    tail = f" с {info['oldest']:%d.%m.%Y}" if info.get("oldest") else ""
    money = f", УПД на {_fmt_money(info['sum'])} ₽" if info.get("sum") else ""
    return "red", f"🔴 *ЭДО:* не подписано {info['count']} док. ({', '.join(parts)}){tail}{money}"


async def chz_positions(order_href: str) -> list[str]:
    """Позиции заказа покупателя, которые маркируются ЧЗ (обязательная группа или
    «консервы при условии»). Классификатор и кэш — chz_classifier (план 2026-10-12)."""
    import chz_classifier as chz
    from moysklad import get_headers
    async with aiohttp.ClientSession() as s:
        async with s.get(order_href, headers=get_headers(),
                         params={"expand": "positions.assortment"},
                         timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                raise RuntimeError(f"заказ {r.status}")
            order = await r.json()
    conn = _db()
    chz.ensure_schema(conn)
    groups = chz.load_groups(conn)
    out = []
    for p in (order.get("positions") or {}).get("rows", []) or []:
        a = p.get("assortment") or {}
        if a.get("meta", {}).get("type") not in ("product", "variant"):
            continue
        cls = await chz.classify_cached(conn, a.get("id"), a.get("name", ""), a.get("pathName") or "")
        g = groups.get(cls["key"]) or {}
        conditional = "если тара герметичная" in (cls.get("reason") or "")
        if g.get("status") == "mandatory" or conditional:
            out.append(a.get("name", ""))
    return out


# ── сводка Рудневой ──────────────────────────────────────────────────────────
def _rudneva_chat_id() -> int | None:
    v = (os.getenv("EDO_REPORT_CHAT_ID") or "2131540964").strip()
    return int(v) if v.lstrip("-").isdigit() else None


async def _managers_by_inn(inns: list[str]) -> dict[str, str]:
    """ИНН → менеджер по тегам МС (несколько карточек на ИНН — объединяем теги)."""
    from moysklad import MS_BASE, get_headers
    out: dict[str, str] = {}
    sem = asyncio.Semaphore(2)

    async def one(session, inn):
        rows, ok = [], False
        async with sem:
            for attempt in range(4):
                try:
                    async with session.get(f"{MS_BASE}/entity/counterparty", headers=get_headers(),
                                           params={"filter": f"inn={inn}", "limit": 20},
                                           timeout=aiohttp.ClientTimeout(total=30)) as r:
                        if r.status == 429:
                            await asyncio.sleep(1.5 * (attempt + 1))
                            continue
                        if r.status == 200:
                            rows, ok = (await r.json()).get("rows", []), True
                        break
                except Exception:
                    await asyncio.sleep(1)
        if not ok:
            out[inn] = "не проверено"
            return
        if not rows:
            out[inn] = "нет в МС"
            return
        tags = {t for c in rows for t in (c.get("tags") or [])}
        mgr = [t for t in _MANAGER_TAGS if t in tags]
        out[inn] = ", ".join(mgr) if mgr else "без менеджера"

    async with aiohttp.ClientSession() as s:
        await asyncio.gather(*[one(s, i) for i in inns])
    return out


async def build_report() -> tuple[str, bytes, str]:
    """(текст сообщения, xlsx, имя файла). Только клиенты с тегом менеджера —
    контрагенты без менеджера (в основном поставщики с актами сверки) не входят."""
    import io
    from openpyxl import Workbook
    from openpyxl.styles import Font

    conn = _db()
    with conn.cursor() as cur:
        cur.execute("""select inn, max(cp_name) cp_name, count(*) n,
                              count(*) filter (where doc_type='UniversalTransferDocument') n_upd,
                              coalesce(sum(total_sum) filter (where doc_type='UniversalTransferDocument'),0) s,
                              min(coalesce(doc_date, sent_at::date)) oldest
                       from public.edo_unsigned group by inn""")
        rows = cur.fetchall()
        cur.execute("select synced_at from public.edo_state where id=1")
        st = cur.fetchone()
    mgr = await _managers_by_inn([r["inn"] for r in rows if r["inn"]])
    today = date.today()
    clients = []
    for r in rows:
        m = mgr.get(r["inn"], "нет в МС")
        if m == "без менеджера":
            continue
        clients.append({**r, "manager": m, "age": (today - r["oldest"]).days if r["oldest"] else 0})
    clients.sort(key=lambda r: -r["age"])

    by_m = collections.defaultdict(lambda: [0, 0, 0.0])
    for r in clients:
        x = by_m[r["manager"]]
        x[0] += 1; x[1] += r["n"]; x[2] += float(r["s"])
    total_n = sum(r["n"] for r in clients)
    total_s = sum(float(r["s"]) for r in clients)
    lines = [f"ЭДО: клиенты не подписали документы – на {today:%d.%m.%Y}",
             f"{total_n} док. у {len(clients)} клиентов, УПД на {_fmt_money(total_s)} ₽.", "",
             "По менеджерам:"]
    for m, (c, n, sm) in sorted(by_m.items(), key=lambda kv: -kv[1][1]):
        lines.append(f"• {m[:1].upper() + m[1:]}: {n} док., {c} клиент., УПД на {_fmt_money(sm)} ₽")
    lines += ["", "Дольше всего без подписи:"]
    for r in clients[:10]:
        lines.append(f"• {r['cp_name'] or r['inn']} ({r['manager']}) – {r['n']} док., {r['age']} дн.")
    lines += ["", "Полный список – в файле. Данные Диадока на "
              f"{st['synced_at'] + timedelta(hours=3):%d.%m %H:%M} МСК."]

    wb = Workbook()
    ws = wb.active
    ws.title = "Без подписи"
    head = ["Менеджер", "Клиент", "ИНН", "Документов", "Из них УПД", "Сумма УПД, ₽",
            "Самый старый", "Дней"]
    ws.append(head)
    for c in ws[1]:
        c.font = Font(bold=True)
    for r in sorted(clients, key=lambda r: (r["manager"], -r["age"])):
        ws.append([r["manager"], r["cp_name"], r["inn"], r["n"], r["n_upd"], float(r["s"]),
                   r["oldest"], r["age"]])
        ws.cell(ws.max_row, 7).number_format = "DD.MM.YYYY"
        ws.cell(ws.max_row, 6).number_format = "# ##0"
    for col, w in zip("ABCDEFGH", (16, 42, 14, 12, 11, 14, 13, 7)):
        ws.column_dimensions[col].width = w
    ws.freeze_panes = "A2"
    buf = io.BytesIO()
    wb.save(buf)
    return "\n".join(lines), buf.getvalue(), f"ЭДО без подписи {today:%Y-%m-%d}.xlsx"


async def weekly_job(app):
    """Синхронизация + сводка Рудневой (по понедельникам, решение собственника 15.10). Ошибка Диадока — сообщение собственнику."""
    try:
        await sync()
    except Exception as e:
        logger.error(f"edo_unsigned: синхронизация упала: {e}", exc_info=True)
        owner = (os.getenv("OWNER_CHAT_ID") or "").strip()
        if owner.isdigit():
            try:
                await app.bot.send_message(int(owner), f"⚠️ ЭДО: не удалось забрать данные из Диадока – {e}")
            except Exception:
                pass
        return
    chat = _rudneva_chat_id()
    if not chat:
        return
    text, xlsx, fname = await build_report()
    await app.bot.send_message(chat, text)
    await app.bot.send_document(chat, document=xlsx, filename=fname)


async def sync_job(app):
    try:
        await sync()
    except Exception as e:
        logger.warning(f"edo_unsigned: промежуточная синхронизация: {e}")
