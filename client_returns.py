"""Возврат ГП у клиента – форма водителя «Клиент вернул» (план 2026-10-20-возвраты-гп-от-водителя-до-плана.md).

Решения собственника 20.10.2026:
- возврат бывает по прошлым накладным: нашу ГП водитель сканирует (весовая этикетка или этикетка
  коробки), накладную форма находит сама; привлечённую – фото этикетки, позиции и накладную по фото
  определяют менеджер и бухгалтер (23.10.2026, после жалобы водителей: «как я должен знать, с какого
  заказа этот возврат»). Скан, которого нет в последних накладных, тоже принимается – «накладную определить»;
- форма создаёт в МС НЕпроведённый «Возврат покупателя» (разрешено собственником) на склад
  «Производство»: на Основной склад возврат не принимают, он сразу идёт в цех на анализ;
- менеджеру клиента, логистам и Рудневой – «едет возврат»: решить с клиентом, это вычерк из
  отгрузки или официальный возврат с корректировочной. Вычерк делает Руднева.
"""
import asyncio
import base64
import io
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import aiohttp
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

from moysklad import MS_BASE, get_headers

logger = logging.getLogger(__name__)
MSK = timezone(timedelta(hours=3))

STORE_PRODUCTION_ID = os.getenv("MOYSKLAD_STORE_PRODUCTION_ID") or "7f3534c1-9dca-11f0-0a80-0510000585d3"
EARLIER_DEMANDS = 3
MAX_PHOTOS = 10
REASONS = ["брак или качество", "не заказывали", "пересорт", "срок годности", "другое"]
DECISIONS = {"strike": "вычерк из отгрузки", "official": "официальный возврат с корректировочной"}
_DB = None


def ensure_schema(db) -> None:
    db._execute("""
        CREATE TABLE IF NOT EXISTS client_returns (
            id           SERIAL PRIMARY KEY,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            unit_id      INT,
            route_day    DATE,
            order_no     TEXT,
            agent_id     TEXT,
            agent_name   TEXT,
            manager_tag  TEXT,
            reason       TEXT,
            comment      TEXT,
            items        JSONB NOT NULL,          -- [{demand_id, demand_name, product_id, code, name, qty, uom, source, raw}]
            ms_returns   JSONB,                   -- [{id, name, demand_name, error?}]
            decision     TEXT,                    -- strike | official
            decided_by   TEXT,
            decided_at   TIMESTAMPTZ
        )""")
    db._execute("ALTER TABLE client_returns ADD COLUMN IF NOT EXISTS loose JSONB")    # [{code, kg, raw}] – скан не нашёлся в накладных
    db._execute("ALTER TABLE client_returns ADD COLUMN IF NOT EXISTS photos JSONB")   # [file_id] – фото этикеток привлечённой


# ─── этикетка ───

def _ean13ok(d: str) -> bool:
    s = sum(int(d[i]) * (3 if i % 2 else 1) for i in range(12))
    return (10 - s % 10) % 10 == int(d[12])


def parse_label(raw: str) -> dict | None:
    """Весовой EAN-13 на «2»: знаки 3–7 – код товара МС, 8–12 – вес в граммах (как в ревизии
    охл. ГП, f2b-production/chilled_inventory.py). GS1-128: AI 01 – GTIN, AI 310n – вес."""
    c = str(raw or "").strip()
    if re.fullmatch(r"2\d{12}", c) and _ean13ok(c) and c[2:7] != "00000":
        return {"code": c[2:7], "kg": int(c[7:12]) / 1000}
    t = re.sub(r"^\]C1", "", c).replace("(", "\x1d").replace(")", "")
    m = re.search(r"01(\d{14})", t)
    w = re.search(r"310(\d)(\d{6})", t)
    if m:
        return {"gtin": m.group(1), "kg": int(w.group(2)) / 10 ** int(w.group(1)) if w else None}
    return None


# ─── отгрузки клиента ───

async def _get(session, path, **params):
    for attempt in range(4):
        async with session.get(f"{MS_BASE}/{path}", headers=get_headers(), params=params,
                               timeout=aiohttp.ClientTimeout(total=40)) as r:
            if r.status == 429:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            r.raise_for_status()
            return await r.json()
    raise RuntimeError("МойСклад: 429")


def _demand_view(d: dict, today_id: str | None) -> dict:
    pos = []
    for p in ((d.get("positions") or {}).get("rows") or []):
        a = p.get("assortment") or {}
        if a.get("meta", {}).get("type") not in ("product", "variant"):
            continue                          # услуги (доставка) не возвращают
        pos.append({
            "product_id": a.get("id"), "type": a["meta"]["type"], "code": str(a.get("code") or ""),
            "name": a.get("name") or "", "qty": p.get("quantity") or 0,
            "uom": ((a.get("uom") or {}).get("name") or "").lower(),
            "gtins": [b.get("gtin") for b in a.get("barcodes") or [] if b.get("gtin")],
        })
    return {"id": d["id"], "name": d.get("name"), "moment": (d.get("moment") or "")[:10],
            "today": d["id"] == today_id, "positions": pos}


async def options(order_no: str) -> dict:
    """Накладная этой точки + 3 прошлые накладные клиента с позициями – для формы водителя."""
    import driver_checklist as dc
    import route_dispatch
    kind, did = await dc._resolve_stop_doc(route_dispatch._doc_no(order_no))
    if kind != "demand" or not did:
        return {"ok": False, "msg": "Точка ещё не отгружена складом – возврат оформить не к чему."}
    async with aiohttp.ClientSession() as s:
        cur = await _get(s, f"entity/demand/{did}", expand="agent")
        agent = cur.get("agent") or {}
        rows = (await _get(s, "entity/demand", filter=f"agent={agent['meta']['href']}",
                           order="moment,desc", limit=EARLIER_DEMANDS + 2,
                           expand="positions.assortment.uom")).get("rows", [])
    if not any(r["id"] == did for r in rows):
        async with aiohttp.ClientSession() as s:
            rows.insert(0, await _get(s, f"entity/demand/{did}", expand="positions.assortment.uom"))
    today = [_demand_view(r, did) for r in rows if r["id"] == did]
    earlier = [_demand_view(r, did) for r in rows if r["id"] != did][:EARLIER_DEMANDS]
    return {"ok": True, "agent_id": agent.get("id"), "agent_name": agent.get("name"),
            "demands": today + earlier, "reasons": REASONS}


# ─── оформление ───

def _fmt_qty(q, uom):
    return f"{q:g} {uom or ''}".strip()


async def _create_ms_return(session, demand_id: str, items: list[dict], note: str) -> dict:
    """Непроведённый «Возврат покупателя» по отгрузке: склад «Производство», цены из отгрузки."""
    d = await _get(session, f"entity/demand/{demand_id}", expand="positions")
    price = {}
    for p in (d.get("positions") or {}).get("rows") or []:
        aid = p["assortment"]["meta"]["href"].rstrip("/").split("/")[-1]
        price.setdefault(aid, p)
    positions = []
    for it in items:
        src = price.get(it["product_id"])
        positions.append({
            "quantity": float(it["qty"]),
            "price": (src or {}).get("price", 0),
            "vat": (src or {}).get("vat", 0),
            "assortment": (src or {}).get("assortment") or {"meta": {
                "href": f"{MS_BASE}/entity/{it.get('type') or 'product'}/{it['product_id']}",
                "type": it.get("type") or "product", "mediaType": "application/json"}},
        })
    body = {
        "organization": d["organization"], "agent": d["agent"],
        "store": {"meta": {"href": f"{MS_BASE}/entity/store/{STORE_PRODUCTION_ID}",
                           "type": "store", "mediaType": "application/json"}},
        "demand": {"meta": d["meta"]}, "applicable": False, "description": note, "positions": positions,
    }
    if d.get("contract"):
        body["contract"] = d["contract"]
    async with session.post(f"{MS_BASE}/entity/salesreturn", headers=get_headers(), json=body,
                            timeout=aiohttp.ClientTimeout(total=60)) as r:
        js = await r.json()
        if r.status >= 300:
            raise RuntimeError("; ".join(e.get("error", "") for e in js.get("errors", [])) or str(r.status))
        return {"id": js["id"], "name": js.get("name"), "demand_name": d.get("name")}


def _photo_bytes(data_url: str) -> bytes | None:
    try:
        b = base64.b64decode(str(data_url).split(",", 1)[-1], validate=False)
        return b if 1000 < len(b) < 5 * 1024 * 1024 else None
    except Exception:
        return None


async def submit(db, bot, uid: int, day, order_no: str, body: dict) -> tuple[bool, str]:
    items = [it for it in (body.get("items") or [])
             if it.get("product_id") and it.get("demand_id") and float(it.get("qty") or 0) > 0]
    loose = [{"code": str(it.get("code") or ""), "kg": it.get("qty"), "raw": it.get("raw") or ""}
             for it in (body.get("items") or []) if not it.get("product_id") and it.get("code")]
    photos = [b for b in (_photo_bytes(x) for x in (body.get("photos") or [])[:MAX_PHOTOS]) if b]
    if not items and not loose and not photos:
        return False, "Отсканируй этикетку нашей продукции или сфоткай этикетку привлечённой."
    reason = (body.get("reason") or "").strip()
    if reason not in REASONS:
        return False, "Выбери причину возврата."
    comment = (body.get("comment") or "").strip()
    import driver_checklist as dc
    agent_id = body.get("agent_id") or ""
    mtag, mchat = await dc._resolve_manager_chat(agent_id) if agent_id else (None, None)
    row = db._fetchone("""
        INSERT INTO client_returns (unit_id, route_day, order_no, agent_id, agent_name, manager_tag,
                                    reason, comment, items, loose)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
        (uid, day, order_no, agent_id, body.get("agent_name"), mtag, reason, comment or None,
         json.dumps(items, ensure_ascii=False), json.dumps(loose, ensure_ascii=False)))
    rid = row["id"]
    asyncio.create_task(_finalize(db, bot, rid, uid, order_no, items, reason, comment,
                                  body.get("agent_name") or "", mtag, mchat,
                                  loose=loose, photos=photos, point_demand_id=body.get("point_demand_id") or ""))
    return True, "↩️ Возврат записан. Менеджер, логист и бухгалтер получат сообщение. Товар вези в цех."


async def _product_names(codes: list[str]) -> dict:
    out = {}
    try:
        async with aiohttp.ClientSession() as s:
            for c in codes[:10]:
                rows = (await _get(s, "entity/product", filter=f"code={c}", limit=1)).get("rows", [])
                if rows:
                    out[c] = rows[0].get("name") or ""
    except Exception as e:
        logger.warning("client_returns: имена по кодам %s: %s", codes, e)
    return out


async def _finalize(db, bot, rid, uid, order_no, items, reason, comment, agent_name, mtag, mchat,
                    loose=None, photos=None, point_demand_id=""):
    import driver_checklist as dc
    import route_registry as rr
    unit = rr.UNITS.get(uid) if hasattr(rr, "UNITS") else None
    unit_name = (unit or {}).get("name") if isinstance(unit, dict) else (unit or uid)
    note = (f"Возврат от клиента, водитель (машина {unit_name}), точка {order_no}. Причина: {reason}."
            + (f" {comment}" if comment else "") + " Создано формой водителя – не проводить до приёмки в цехе.")
    loose, photos = loose or [], photos or []
    names = await _product_names(sorted({x["code"] for x in loose})) if loose else {}
    by_demand: dict[str, list] = {}
    for it in items:
        by_demand.setdefault(it["demand_id"], []).append(it)
    # Привлечённая по фото и сканы вне накладных: пустой черновик по накладной точки – позиции и
    # накладную по фото уточняют менеджер и бухгалтер; с позициями он встанет в очередь цеха.
    todo_note = ""
    if (loose or photos) and point_demand_id:
        todo_note = (note.replace(" Создано формой", " ПОЗИЦИИ УТОЧНИТЬ по фото этикеток в Telegram"
                                  + (" и сканам: " + ", ".join(f"код {x['code']} {x['kg'] or ''} кг" for x in loose)
                                     if loose else "") + ". Создано формой"))
        by_demand.setdefault(point_demand_id, [])
    made = []
    if os.getenv("RETURNS_MS_WRITE", "1") == "1":
        async with aiohttp.ClientSession() as s:
            for did, its in by_demand.items():
                try:
                    is_todo = bool(todo_note) and did == point_demand_id
                    m = await _create_ms_return(s, did, its, todo_note if is_todo else note)
                    m["todo"] = is_todo
                    made.append(m)
                except Exception as e:
                    logger.warning("client_returns %s: возврат по %s не создан: %s", rid, did, e)
                    made.append({"id": None, "demand_name": (its[0].get("demand_name") if its else "точки"),
                                 "error": str(e)[:200]})
    db._execute("UPDATE client_returns SET ms_returns=%s WHERE id=%s", (json.dumps(made, ensure_ascii=False), rid))

    lines = []
    for did, its in by_demand.items():
        if not its:
            continue
        lines.append(f"Накладная {its[0].get('demand_name') or '?'}:")
        lines += [f"  • {it.get('code') or ''} {it.get('name') or ''} – {_fmt_qty(float(it['qty']), it.get('uom'))}"
                  for it in its]
    if loose:
        lines.append("Сканы не нашлись в последних накладных клиента – накладную определить:")
        lines += [f"  • код {x['code']} {names.get(x['code'], '')} – {x['kg'] or '?'} кг" for x in loose]
    if photos:
        lines.append(f"Привлечённая: {len(photos)} фото этикеток – позиции определить по фото.")
    todo = [m for m in made if m.get("id") and m.get("todo")]
    if todo:
        lines.append(f"В черновик возврата №{todo[0]['name']} (накладная {todo[0].get('demand_name')}) добавьте "
                     "позиции по фото – с ними возврат встанет в очередь цеха.")
    docs = ", ".join(f"№{m['name']}" for m in made if m.get("id"))
    fails = [m for m in made if not m.get("id")]
    text = (f"↩️ Едет возврат\nКлиент: {agent_name}\nМашина {unit_name}, точка {order_no}\n"
            + "\n".join(lines) + f"\nПричина: {reason}" + (f"\nКомментарий: {comment}" if comment else "")
            + (f"\n\nЧерновик «Возврат покупателя» в МС: {docs} (не проведён, склад «Производство»)" if docs else "")
            + ("\n⚠️ Черновик в МС не создался: " + "; ".join(f"{m.get('demand_name')}: {m.get('error')}" for m in fails)
               if fails else "")
            + (f"\nМенеджер: {mtag}" if mtag else "")
            + "\n\nРешите с клиентом: вычерк из отгрузки или официальный возврат с корректировочной.")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Вычерк из отгрузки", callback_data=f"cret:{rid}:strike")],
                               [InlineKeyboardButton("Официальный возврат", callback_data=f"cret:{rid}:official")]])
    import edo_unsigned
    file_ids: list[str] = []
    for cid in _recipients(mchat, edo_unsigned._rudneva_chat_id(), dc._logist_chat_ids()):
        try:
            if photos:
                src = file_ids or [io.BytesIO(b) for b in photos]
                if len(src) == 1:                # альбом в Telegram – от 2 фото
                    sent = [await bot.send_photo(chat_id=cid, photo=src[0])]
                else:
                    sent = await bot.send_media_group(chat_id=cid, media=[InputMediaPhoto(x) for x in src])
                if not file_ids:
                    file_ids = [m.photo[-1].file_id for m in sent if m.photo]
            await bot.send_message(chat_id=cid, text=text, reply_markup=kb)
        except Exception as e:
            logger.warning("client_returns %s → %s: %s", rid, cid, e)
    if file_ids:
        db._execute("UPDATE client_returns SET photos=%s WHERE id=%s", (json.dumps(file_ids), rid))


def _recipients(*parts) -> list:
    out = []
    for p in parts:
        for cid in (p if isinstance(p, (list, tuple)) else [p]):
            if cid and cid not in out:
                out.append(cid)
    return out


async def cb_decision(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    _, rid, dec = q.data.split(":")
    db = context.bot_data.get("db") or _DB
    who = q.from_user.full_name or str(q.from_user.id)
    row = db._fetchone("SELECT * FROM client_returns WHERE id=%s", (int(rid),))
    if not row:
        await q.answer("Возврат не найден", show_alert=True)
        return
    if row.get("decision"):
        await q.answer(f"Уже решено: {DECISIONS.get(row['decision'])} ({row.get('decided_by')})", show_alert=True)
        return
    db._execute("UPDATE client_returns SET decision=%s, decided_by=%s, decided_at=now() WHERE id=%s",
                (dec, who, int(rid)))
    await q.answer("Записал")
    try:
        await q.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass
    tail = ("Евгения Руднева правит отгрузку и удаляет черновик возврата." if dec == "strike"
            else "Возврат оформляется официально: цех принимает, бухгалтерия – корректировочная.")
    import driver_checklist as dc
    import edo_unsigned
    text = f"↩️ Возврат {row.get('agent_name') or ''} (точка {row.get('order_no')}): {DECISIONS[dec]} – решил(а) {who}.\n{tail}"
    _, mchat = await dc._resolve_manager_chat(row["agent_id"]) if row.get("agent_id") else (None, None)
    for cid in _recipients(mchat, edo_unsigned._rudneva_chat_id(), dc._logist_chat_ids()):
        try:
            await context.bot.send_message(chat_id=cid, text=text)
        except Exception as e:
            logger.warning("client_returns decision → %s: %s", cid, e)


async def job_remind(context: ContextTypes.DEFAULT_TYPE):
    """Возврат без решения дольше 2 рабочих дней – напоминание менеджеру и Рудневой (раз в день, будни)."""
    db = context.bot_data.get("db") or _DB
    rows = db._fetchall("""
        SELECT * FROM client_returns
         WHERE decision IS NULL AND created_at >= now() - interval '30 days'
           AND (SELECT count(*) FROM generate_series(created_at::date + 1, (now() AT TIME ZONE 'Europe/Moscow')::date, '1 day') d
                 WHERE extract(isodow FROM d) < 6) > 2""")
    if not rows:
        return
    import driver_checklist as dc
    import edo_unsigned
    for r in rows:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Вычерк из отгрузки", callback_data=f"cret:{r['id']}:strike")],
                                   [InlineKeyboardButton("Официальный возврат", callback_data=f"cret:{r['id']}:official")]])
        text = (f"⏰ Возврат без решения с {r['created_at'].astimezone(MSK):%d.%m}: {r.get('agent_name') or ''}, "
                f"точка {r.get('order_no')}. Решите с клиентом – вычерк или официальный возврат.")
        _, mchat = await dc._resolve_manager_chat(r["agent_id"]) if r.get("agent_id") else (None, None)
        for cid in _recipients(mchat, edo_unsigned._rudneva_chat_id()):
            try:
                await context.bot.send_message(chat_id=cid, text=text, reply_markup=kb)
            except Exception as e:
                logger.warning("client_returns remind → %s: %s", cid, e)


def register(app: Application, db) -> None:
    global _DB
    _DB = db
    app.add_handler(CallbackQueryHandler(cb_decision, pattern=r"^cret:\d+:(strike|official)$"))
    # будни 10:00 МСК
    from datetime import time as _t
    app.job_queue.run_daily(job_remind, time=_t(10, 0, tzinfo=MSK), days=(1, 2, 3, 4, 5), name="client_returns_remind")
    try:
        ensure_schema(db)
    except Exception as e:
        logger.exception("client_returns.ensure_schema отложено: %s", e)
