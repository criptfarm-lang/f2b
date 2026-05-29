"""
HTML-отчёт «Дебиторка» — Фаза 5 плана 2026-05-20-пдз-автоматика.

Источник данных:
  - последний snapshot из `pdz_snapshots` (db.get_latest_snapshot)
  - журнал срывов из `promise_log` (db.get_promise_breaks_top + breaks_count)

НЕ дёргает МойСклад API — генерация чисто из БД (~мгновенно).

Доступ — только собственник через защищённую ссылку с TTL (`report_links`).
"""

from __future__ import annotations

import html
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from moysklad import PDZ_GRACE_DAYS, _pdz_classify

MSK = ZoneInfo("Europe/Moscow")


# ── helpers ──────────────────────────────────────────────────────────────────


def _to_date(v):
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        s = v[:10]
        try:
            y, m, d = s.split("-")
            return date(int(y), int(m), int(d))
        except Exception:
            return None
    return None


def _fmt_money(v) -> str:
    try:
        n = float(v or 0)
    except Exception:
        return "—"
    return f"{n:,.0f}".replace(",", " ") + " ₽"


def _fmt_dt(dt) -> str:
    if not dt:
        return "—"
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except Exception:
            return dt
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MSK)
        else:
            dt = dt.astimezone(MSK)
        return dt.strftime("%Y-%m-%d %H:%M МСК")
    except Exception:
        return str(dt)


def _esc(s) -> str:
    return html.escape("" if s is None else str(s), quote=True)


def _ms_url(order_id: str) -> str:
    return f"https://online.moysklad.ru/app/#customerorder/edit?id={order_id or ''}"


# ── data assembly ────────────────────────────────────────────────────────────


def build_pdz_payload(db) -> dict:
    """Собирает данные для HTML-отчёта из БД.

    Возвращает dict с ключами:
      - updated_at (datetime|None) — самая свежая snap_at
      - kpi: total_debt, debtors_count, broken_90d, oldest_overdue_days
      - debtors: список dict {agent_id, agent_name, manager_tag, orders_count,
                              days_overdue, breaks_count, total_unpaid, ms_url}
      - managers: список dict {manager_tag, debtors_count, breaks_count, total_debt}
      - top_violators: список dict {agent_id, agent_name, breaks_count,
                                    days_since_last_break, manager_tag}
    """
    rows = db.get_latest_snapshot() or []
    today = datetime.now(MSK).date()

    # snap_at MAX
    updated_at = None
    for r in rows:
        sa = r.get("snap_at")
        if sa is None:
            continue
        if updated_at is None or sa > updated_at:
            updated_at = sa

    # Per-contractor FIFO + balance=None skip + лаг PDZ_GRACE_DAYS дней от
    # ppm_initial (та же логика что в moysklad.pdz_overdue_for_manager и
    # pdz_unprocessed_for_owner).
    # Шаг 1: собрать per-agent overdue + in_сroк_unpaid + work-метрики.
    #
    # work_total = заказы где ppm_initial + GRACE < today И payed < sum
    #              (формально просрочено с учётом лага — работа менеджера).
    # work_touched = из work_total те где ppm_new is not null (менеджер
    #                открыл карточку и поставил новую дату — неважно в прошлое
    #                или будущее).
    # Заказы в grace-окне не считаются ни «к работе», ни «просроченными» —
    # их защищает технический лаг от первой обещанной даты.
    by_agent_raw: dict[str, dict] = {}
    for r in rows:
        bal_raw = r.get("agent_balance")
        try:
            balance = float(bal_raw) if bal_raw is not None else None
        except Exception:
            balance = None
        try:
            total = float(r.get("total_sum") or 0)
            payed = float(r.get("payed_sum") or 0)
        except Exception:
            total, payed = 0.0, 0.0
        if payed >= total:
            continue
        ppm_new = _to_date(r.get("ppm_new"))
        ppm_initial = _to_date(r.get("ppm_initial"))
        status, effective, days_overdue = _pdz_classify(ppm_initial, ppm_new, today)
        if status == "skip":
            continue
        aid = r.get("agent_id") or ""
        if not aid:
            continue
        unpaid = round(total - payed, 2)
        bucket = by_agent_raw.setdefault(aid, {
            "agent_id": aid,
            "agent_name": r.get("agent_name") or "—",
            "manager_tag": r.get("manager_tag") or "",
            "agent_balance": balance,
            "overdue": [],
            "in_сroк_unpaid_total": 0.0,
            "work_total": 0,
            "work_touched": 0,
        })
        # Работа менеджера — по ppm_initial + GRACE (синхронно с дайджестом).
        if ppm_initial is not None and today > ppm_initial + timedelta(days=PDZ_GRACE_DAYS):
            bucket["work_total"] += 1
            if ppm_new is not None:
                bucket["work_touched"] += 1
        if status in ("in_срок", "in_grace"):
            bucket["in_сroк_unpaid_total"] = round(bucket["in_сroк_unpaid_total"] + unpaid, 2)
            continue
        bucket["overdue"].append({
            "order_id": r.get("order_id") or "",
            "days_overdue": days_overdue,
            "unpaid": unpaid,
        })

    # Шаг 2: применить FIFO + balance=None skip. total_unpaid = real_overdue.
    debtors: list[dict] = []
    for aid, data in by_agent_raw.items():
        if not data["overdue"]:
            continue
        bal = data["agent_balance"]
        if bal is None:
            continue  # balance не подтянулся в snapshot — пропускаем
        if bal >= 0:
            continue
        bal_abs = abs(bal)
        in_сroк = data["in_сroк_unpaid_total"]
        real_overdue = max(0.0, round(bal_abs - in_сroк, 2))
        if real_overdue < 0.01:
            continue  # FIFO-перекрытие
        # Самый старый просроченный заказ — для days_overdue и ссылки.
        oldest = max(data["overdue"], key=lambda x: x["days_overdue"])
        debtors.append({
            "agent_id": aid,
            "agent_name": data["agent_name"],
            "manager_tag": data["manager_tag"],
            "orders_count": data["work_total"],          # всего заказов клиента с просроч. ppm_initial
            "orders_touched": data["work_touched"],      # из них с заполн. «Новой датой оплаты»
            "days_overdue": oldest["days_overdue"],
            "total_unpaid": real_overdue,
            "ms_url": _ms_url(oldest["order_id"]),
        })

    # Обогащение срывами за 90 дней
    breaks_map: dict[str, int] = {}
    try:
        ids = [g["agent_id"] for g in debtors if g.get("agent_id")]
        if ids and hasattr(db, "get_promise_breaks_count"):
            breaks_map = db.get_promise_breaks_count(ids, days_window=90) or {}
    except Exception:
        breaks_map = {}
    # Фаза 6: обогащение стоп-флагами для колонки «Статус».
    stop_map: dict[str, str] = {}
    try:
        ids = [g["agent_id"] for g in debtors if g.get("agent_id")]
        if ids and hasattr(db, "get_stop_flag_map"):
            stop_map = db.get_stop_flag_map(ids) or {}
    except Exception:
        stop_map = {}
    for g in debtors:
        aid = g.get("agent_id") or ""
        g["breaks_count"] = int(breaks_map.get(aid, 0))
        g["stop_status"] = stop_map.get(aid)

    debtors.sort(key=lambda x: x["total_unpaid"], reverse=True)
    debtors_top = debtors[:50]

    # KPI
    total_debt = round(sum(d["total_unpaid"] for d in debtors), 2)
    debtors_count = len(debtors)
    oldest_overdue_days = max((d["days_overdue"] for d in debtors), default=0)

    # Срывов за 90д всего — из promise_log: возьмём top без лимита и просуммируем
    # (на топ-1000 точно хватит — масштаб ОП на 5 менеджеров).
    broken_90d = 0
    try:
        if hasattr(db, "get_promise_breaks_top"):
            top_all = db.get_promise_breaks_top(limit=1000, days_window=90) or []
            broken_90d = sum(int(r.get("breaks_count") or 0) for r in top_all)
    except Exception:
        broken_90d = 0

    # По менеджерам.
    # debtors_count / total_debt берём из FIFO-отфильтрованных debtors.
    # А orders_total / orders_touched (метрика «отработано») считаем НЕЗАВИСИМО
    # от FIFO: по всем заказам в snapshot с ppm_initial < today И payed < sum.
    # Иначе если клиент ушёл из FIFO (хвосты перекрыты приходами), вся работа
    # менеджера по нему теряется — плитка показывает 0% даже когда менеджер
    # реально отработал десятки заказов.
    by_mgr: dict[str, dict] = {}
    for d in debtors:
        tag = (d.get("manager_tag") or "—").lower() or "—"
        m = by_mgr.get(tag)
        if m is None:
            by_mgr[tag] = {
                "manager_tag": tag,
                "debtors_count": 1,
                "breaks_count": int(d.get("breaks_count") or 0),
                "total_debt": d["total_unpaid"],
                "orders_total": 0,    # заполним ниже из всех snapshot-rows
                "orders_touched": 0,
            }
        else:
            m["debtors_count"] += 1
            m["breaks_count"] += int(d.get("breaks_count") or 0)
            m["total_debt"] = round(m["total_debt"] + d["total_unpaid"], 2)

    # Независимый pass по snapshot: считаем работу менеджера по ВСЕМ заказам
    # с просроченной исходной датой (без FIFO).
    for r in rows:
        tag = (r.get("manager_tag") or "—").lower() or "—"
        try:
            total = float(r.get("total_sum") or 0)
            payed = float(r.get("payed_sum") or 0)
        except Exception:
            continue
        if payed >= total:
            continue
        ppm_initial = _to_date(r.get("ppm_initial"))
        # ppm_initial <= today: включаем заказы, у которых срок истекает
        # сегодня — менеджер мог проактивно открыть и поставить «Новую дату».
        if not ppm_initial or ppm_initial > today:
            continue
        ppm_new = _to_date(r.get("ppm_new"))
        m = by_mgr.setdefault(tag, {
            "manager_tag": tag,
            "debtors_count": 0,
            "breaks_count": 0,
            "total_debt": 0,
            "orders_total": 0,
            "orders_touched": 0,
        })
        m["orders_total"] += 1
        if ppm_new is not None:
            m["orders_touched"] += 1

    managers = sorted(by_mgr.values(), key=lambda x: x["total_debt"], reverse=True)

    # Топ-нарушители за 90д
    top_violators: list[dict] = []
    try:
        if hasattr(db, "get_promise_breaks_top"):
            raw_top = db.get_promise_breaks_top(limit=20, days_window=90) or []
            for r in raw_top:
                last = r.get("last_break_at")
                days_since = None
                if last:
                    try:
                        if isinstance(last, str):
                            last_dt = datetime.fromisoformat(last)
                        else:
                            last_dt = last
                        if last_dt.tzinfo is None:
                            last_dt = last_dt.replace(tzinfo=MSK)
                        delta = datetime.now(MSK) - last_dt.astimezone(MSK)
                        days_since = max(0, int(delta.total_seconds() // 86400))
                    except Exception:
                        days_since = None
                top_violators.append({
                    "agent_id": r.get("agent_id") or "",
                    "agent_name": r.get("agent_name") or "—",
                    "breaks_count": int(r.get("breaks_count") or 0),
                    "days_since_last_break": days_since,
                    "manager_tag": r.get("manager_tag") or "—",
                })
    except Exception:
        top_violators = []

    return {
        "updated_at": updated_at,
        "kpi": {
            "total_debt": total_debt,
            "debtors_count": debtors_count,
            "broken_90d": broken_90d,
            "oldest_overdue_days": oldest_overdue_days,
        },
        "debtors": debtors_top,
        "managers": managers,
        "top_violators": top_violators,
    }


# ── HTML rendering ───────────────────────────────────────────────────────────


_PAGE_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #f8f7ff; color: #1e1b4b; }
.header { background: #fff; border-bottom: 1px solid #e8e5f5; padding: 14px 24px;
          display: flex; align-items: center; justify-content: space-between; }
.header h1 { font-size: 17px; font-weight: 600; }
.header .date { font-size: 13px; color: #6b7280; }
.container { max-width: 1200px; margin: 0 auto; padding: 20px 16px; }
.summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
           gap: 10px; margin-bottom: 20px; }
.s-card { background: #fff; border-radius: 10px; border: 1px solid #e8e5f5;
          padding: 12px 14px; }
.s-card .label { font-size: 11px; color: #6b7280; text-transform: uppercase;
                 letter-spacing: 0.4px; margin-bottom: 5px; }
.s-card .value { font-size: 22px; font-weight: 600; }
.section { background: #fff; border-radius: 10px; border: 1px solid #e8e5f5;
           padding: 18px 20px; margin-bottom: 20px; }
.section-title { font-size: 13px; font-weight: 600; color: #1e1b4b;
                 text-transform: uppercase; letter-spacing: 0.4px;
                 margin-bottom: 14px; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; padding: 8px 10px; font-weight: 600; color: #6b7280;
     border-bottom: 1px solid #e8e5f5; font-size: 11px;
     text-transform: uppercase; letter-spacing: 0.4px; }
td { padding: 8px 10px; border-bottom: 1px solid #f0effe; vertical-align: middle; }
tr:last-child td { border-bottom: none; }
td a { color: #7c3aed; text-decoration: none; }
td a:hover { text-decoration: underline; }
.num { text-align: right; white-space: nowrap; font-variant-numeric: tabular-nums; }
.bad-break { color: #c00; font-weight: bold; }
tr.row-old-overdue td { background: #fef2f2; }
/* Фаза 6: фон строк со стоп-флагом. ПРЕДОПЛАТА жёстче — красный, СТОП — жёлтый. */
tr.row-stop-shipments td { background: #fff3cd; }
tr.row-prepayment-only td { background: #fee2e2; }
.stop-badge { display: inline-block; padding: 2px 6px; border-radius: 4px;
              font-size: 11px; font-weight: 600; }
.stop-badge.stop { background: #fde68a; color: #7c2d12; }
.stop-badge.prepay { background: #fecaca; color: #7f1d1d; }
.muted { color: #9ca3af; }

/* Плитки по менеджерам — сетка с прогресс-баром «обработано». */
.mgr-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 14px; }
.mgr-card { background: #fafaff; border: 1px solid #e8e5f5; border-radius: 10px;
            padding: 14px 16px; }
.mgr-name { font-size: 14px; font-weight: 600; color: #1e1b4b; margin-bottom: 4px; }
.mgr-debt { font-size: 22px; font-weight: 700; color: #1e1b4b; margin-bottom: 8px;
            font-variant-numeric: tabular-nums; }
.mgr-meta { font-size: 12px; color: #6b7280; display: flex; gap: 6px; flex-wrap: wrap;
            margin-bottom: 10px; }
.mgr-meta .mgr-breaks { color: #c00; font-weight: 600; }
.mgr-progress-label { display: flex; justify-content: space-between; font-size: 12px;
                      color: #4b5563; margin-bottom: 4px; }
.mgr-progress-bar { background: #e8e5f5; border-radius: 4px; height: 6px; overflow: hidden; }
.mgr-progress-fill { height: 100%; transition: width 0.3s; }
.empty { color: #6b7280; font-size: 13px; padding: 20px; text-align: center; }
"""


def _render_kpi(kpi: dict) -> str:
    parts = [
        ("Общий долг", _fmt_money(kpi["total_debt"])),
        ("Клиентов в просрочке", str(kpi["debtors_count"])),
        ("Срывов за 90 дней", str(kpi["broken_90d"])),
        ("Самая старая просрочка",
         (f"{kpi['oldest_overdue_days']} дн" if kpi["oldest_overdue_days"] else "—")),
    ]
    cards = "".join(
        f'<div class="s-card"><div class="label">{_esc(lbl)}</div>'
        f'<div class="value">{_esc(val)}</div></div>'
        for lbl, val in parts
    )
    return f'<div class="summary">{cards}</div>'


def _render_debtors(debtors: list) -> str:
    if not debtors:
        return ('<div class="section">'
                '<div class="section-title">Клиенты-должники</div>'
                '<div class="empty">Должников нет</div></div>')
    rows = []
    for d in debtors:
        breaks = int(d.get("breaks_count") or 0)
        days = int(d.get("days_overdue") or 0)
        stop_status = d.get("stop_status")
        breaks_cell = (
            f'<span class="bad-break">{breaks}</span>' if breaks > 0
            else f'<span class="muted">0</span>'
        )
        # Фаза 6: класс строки и значок статуса. Приоритет — стоп-флаг.
        if stop_status == "prepayment_only":
            row_cls = ' class="row-prepayment-only"'
            status_cell = '<span class="stop-badge prepay">🚫 ПРЕДОПЛАТА</span>'
        elif stop_status == "stop_shipments":
            row_cls = ' class="row-stop-shipments"'
            status_cell = '<span class="stop-badge stop">🚫 СТОП</span>'
        else:
            row_cls = ' class="row-old-overdue"' if days > 90 else ''
            status_cell = '<span class="muted">—</span>'

        # Обработано = сколько просроченных заказов клиента имеют заполненную
        # «Новую дату оплаты». Логика 2026-05-27: если менеджер физически
        # открыл заказ в МС и поставил ppm_new — это значит «начал отрабатывать»
        # (даже если эта новая дата уже в прошлом = срыв обещания).
        orders_total = int(d.get("orders_count") or 0)
        orders_touched = int(d.get("orders_touched") or 0)
        if orders_total == 0:
            touched_cell = '<span class="muted">—</span>'
        elif orders_touched == 0:
            touched_cell = f'<span class="bad-break">0/{orders_total}</span>'
        elif orders_touched < orders_total:
            touched_cell = f'<span style="color:#d97706">{orders_touched}/{orders_total}</span>'
        else:
            touched_cell = f'<span style="color:#059669">{orders_touched}/{orders_total}</span>'

        rows.append(
            f'<tr{row_cls}>'
            f'<td><a href="{_esc(d.get("ms_url"))}" target="_blank" rel="noopener">'
            f'{_esc(d.get("agent_name"))}</a></td>'
            f'<td>{_esc(d.get("manager_tag") or "—")}</td>'
            f'<td class="num">{orders_total}</td>'
            f'<td class="num">{touched_cell}</td>'
            f'<td class="num">{days}</td>'
            f'<td class="num">{breaks_cell}</td>'
            f'<td>{status_cell}</td>'
            f'<td class="num">{_esc(_fmt_money(d.get("total_unpaid")))}</td>'
            f'</tr>'
        )
    table = (
        '<table>'
        '<thead><tr>'
        '<th>Клиент</th><th>Менеджер</th>'
        '<th class="num">Заказов</th>'
        '<th class="num" title="Сколько заказов клиента менеджер уже открыл и проставил «Новую дату оплаты»">Обработано</th>'
        '<th class="num">Дней просрочки</th>'
        '<th class="num">Срывов 90д</th><th>Статус</th><th class="num">Долг</th>'
        '</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table>'
    )
    return (
        '<div class="section">'
        '<div class="section-title">Клиенты-должники (топ-50 по сумме долга)</div>'
        f'{table}</div>'
    )


def _render_managers(managers: list) -> str:
    if not managers:
        return ''

    # Маппинг тегов на отображаемые имена (как в moysklad.PDZ_MANAGER_TAG_MAP).
    DISPLAY = {
        "баласанян":  "Карина Баласанян",
        "скляр":      "Инесса Скляр",
        "мерзлякова": "Елена Мерзлякова",
        "дьяченко":   "Ирина Дьяченко",
        "коликов":    "Денис Коликов",
    }

    cards = []
    for m in managers:
        tag = (m.get("manager_tag") or "—").lower()
        name = DISPLAY.get(tag, m.get("manager_tag") or "—")
        debtors_n = int(m.get("debtors_count") or 0)
        orders_total = int(m.get("orders_total") or 0)
        orders_touched = int(m.get("orders_touched") or 0)
        breaks = int(m.get("breaks_count") or 0)
        debt = m.get("total_debt") or 0

        pct = (orders_touched * 100 // orders_total) if orders_total > 0 else 0

        if orders_total == 0:
            bar_color = "#9ca3af"
        elif pct == 0:
            bar_color = "#dc2626"  # красный
        elif pct < 100:
            bar_color = "#d97706"  # оранжевый
        else:
            bar_color = "#059669"  # зелёный

        cards.append(f'''
        <div class="mgr-card">
          <div class="mgr-name">{_esc(name)}</div>
          <div class="mgr-debt">{_esc(_fmt_money(debt))}</div>
          <div class="mgr-meta">
            <span>{debtors_n} клиент{'ов' if debtors_n%10 not in (1,2,3,4) or debtors_n%100 in (11,12,13,14) else ('а' if debtors_n%10 in (2,3,4) else '')}</span>
            <span>•</span>
            <span>{orders_total} заказ{'ов' if orders_total%10 not in (1,2,3,4) or orders_total%100 in (11,12,13,14) else ('а' if orders_total%10 in (2,3,4) else '')}</span>
            {f'<span>•</span><span class="mgr-breaks">{breaks} срыв{"ов" if breaks%10 not in (1,2,3,4) or breaks%100 in (11,12,13,14) else ("а" if breaks%10 in (2,3,4) else "")} 90д</span>' if breaks > 0 else ''}
          </div>
          <div class="mgr-progress-label">
            <span>Отработано: <strong>{orders_touched}/{orders_total}</strong></span>
            <span style="color:{bar_color}"><strong>{pct}%</strong></span>
          </div>
          <div class="mgr-progress-bar">
            <div class="mgr-progress-fill" style="width:{pct}%;background:{bar_color}"></div>
          </div>
        </div>
        ''')

    return (
        '<div class="section">'
        '<div class="section-title">По менеджерам</div>'
        f'<div class="mgr-grid">{"".join(cards)}</div>'
        '</div>'
    )


def _render_violators(top_violators: list) -> str:
    if not top_violators:
        return (
            '<div class="section">'
            '<div class="section-title">Топ-нарушители за 90 дней</div>'
            '<div class="empty">Срывов обещаний за 90 дней нет</div>'
            '</div>'
        )
    rows = []
    for v in top_violators:
        days_since = v.get("days_since_last_break")
        last_str = "—" if days_since is None else f"{int(days_since)} дн назад"
        rows.append(
            f'<tr>'
            f'<td>{_esc(v.get("agent_name"))}</td>'
            f'<td class="num"><span class="bad-break">{int(v.get("breaks_count") or 0)}</span></td>'
            f'<td>{_esc(last_str)}</td>'
            f'<td>{_esc(v.get("manager_tag") or "—")}</td>'
            f'</tr>'
        )
    table = (
        '<table>'
        '<thead><tr>'
        '<th>Клиент</th>'
        '<th class="num">Срывов</th>'
        '<th>Последний срыв</th>'
        '<th>Менеджер</th>'
        '</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table>'
    )
    return (
        '<div class="section">'
        '<div class="section-title">Топ-нарушители за 90 дней</div>'
        f'{table}</div>'
    )


def build_pdz_html(payload: dict) -> str:
    """Собирает финальный HTML-документ из payload (build_pdz_payload)."""
    upd_str = _fmt_dt(payload.get("updated_at"))
    body = (
        _render_kpi(payload.get("kpi") or {})
        + _render_debtors(payload.get("debtors") or [])
        + _render_managers(payload.get("managers") or [])
        + _render_violators(payload.get("top_violators") or [])
    )
    return (
        '<!DOCTYPE html>'
        '<html lang="ru"><head>'
        '<meta charset="UTF-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
        '<title>Дебиторка — F2B</title>'
        f'<style>{_PAGE_CSS}</style>'
        '</head><body>'
        '<div class="header">'
        f'<h1>Дебиторка · обновлено {_esc(upd_str)}</h1>'
        '<span class="date">F2B PRO</span>'
        '</div>'
        f'<div class="container">{body}</div>'
        '</body></html>'
    )


def render_pdz_html_from_db(db) -> str:
    """Удобная одношаговая функция: данные из БД → HTML-строка."""
    payload = build_pdz_payload(db)
    return build_pdz_html(payload)
