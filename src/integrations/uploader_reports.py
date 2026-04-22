"""Per-uploader aggregation reports.

The CPA queue has a filter (Item 1 of the polish pass) — this module
adds the matching *report*: a table showing every uploader's volume,
amount, dates, and status breakdown, scoped by date range + optional
client.

Exposes three helpers:

- ``aggregate(db, firm_code, start, end, client_code=None,
  sort_by='count', sort_dir='desc')`` → list[dict]
- ``top_uploaders_this_week(db, firm_code, limit=5)`` → list[dict]
  (small summary for the dashboard home widget)
- ``documents_for_uploader(db, firm_code, uploader_key, start, end)``
  → list[dict] (drill-down)
- ``render_csv(rows)`` → bytes (CSV export for the button)

Every query filters rows to the caller's firm via
``client_code IN (SELECT client_code FROM clients WHERE firm_code=?)``
so firm_admin users never see another firm's uploaders. Owner (None
firm_code) sees across all firms.

Anonymous rows (``uploader_email IS NULL OR ''``) are excluded by
default — the spec says ``excludes_anonymous_from_report`` — but
``include_anonymous=True`` surfaces them for debugging.

Suspended users ARE included: historical uploads belong to the
person who made them regardless of the user's current status.
"""
from __future__ import annotations

import csv
import html as _html
import io
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


log = logging.getLogger(__name__)


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


VALID_SORT_COLUMNS = {
    'uploader_email', 'uploader_name', 'client_code',
    'document_count', 'total_amount', 'first_upload', 'last_upload',
    'pending_count', 'approved_count', 'rejected_count',
}


def aggregate(
    db_path: Path | str, *,
    firm_code: str | None = None,
    client_code: str | None = None,
    start: str | None = None,
    end: str | None = None,
    include_anonymous: bool = False,
    sort_by: str = 'document_count',
    sort_dir: str = 'desc',
) -> list[dict[str, Any]]:
    """Per-uploader aggregation.

    ``start``/``end`` are ISO date strings (YYYY-MM-DD). When both
    blank, defaults to the current month. ``firm_code=None`` means
    owner scope (all firms).
    """
    if sort_by not in VALID_SORT_COLUMNS:
        raise ValueError(f'invalid sort_by {sort_by!r}')
    if sort_dir.lower() not in ('asc', 'desc'):
        raise ValueError(f'invalid sort_dir {sort_dir!r}')

    # Default to current month
    if not start or not end:
        today = datetime.now(timezone.utc).date()
        month_start = today.replace(day=1).isoformat()
        # End of month: easier — first-of-next-month
        if today.month == 12:
            next_month = today.replace(year=today.year + 1,
                                         month=1, day=1)
        else:
            next_month = today.replace(month=today.month + 1, day=1)
        month_end = (next_month - timedelta(days=1)).isoformat()
        start = start or month_start
        end = end or month_end

    where: list[str] = []
    params: list[Any] = []
    # Firm scoping
    if firm_code:
        where.append(
            "d.client_code IN "
            "(SELECT client_code FROM clients WHERE firm_code=?)"
        )
        params.append(firm_code)
    if client_code:
        where.append("d.client_code = ?")
        params.append(client_code)
    # Date range on document_date (falls back to uploaded_at when the
    # invoice date isn't set; kept simple here — document_date is the
    # canonical field).
    where.append(
        "COALESCE(d.document_date,'')  >= ? "
        "AND COALESCE(d.document_date,'') <= ?"
    )
    params.append(start)
    params.append(end)
    if not include_anonymous:
        where.append(
            "COALESCE(d.uploader_email,'') != ''"
        )
    where_sql = 'WHERE ' + ' AND '.join(where) if where else ''

    # Detect whether the optional channel column exists so older DBs
    # (pre-migration) still produce a report — they just won't show a
    # channel breakdown. Same idea as the drift guard: degrade, don't
    # crash.
    with _open(db_path) as _probe:
        _doc_cols = {
            r['name'] for r in
            _probe.execute("PRAGMA table_info(documents)").fetchall()
        }
    if 'uploaded_via_channel' in _doc_cols:
        channel_exprs = (
            ",\n          SUM(CASE WHEN LOWER(COALESCE("
            "d.uploaded_via_channel,'portal')) "
            "= 'portal' THEN 1 ELSE 0 END) AS portal_count"
            ",\n          SUM(CASE WHEN LOWER(COALESCE("
            "d.uploaded_via_channel,'')) "
            "= 'whatsapp' THEN 1 ELSE 0 END) AS whatsapp_count"
            ",\n          SUM(CASE WHEN LOWER(COALESCE("
            "d.uploaded_via_channel,'')) "
            "= 'email' THEN 1 ELSE 0 END) AS email_count"
        )
    else:
        # Fallback: every row counts as 'portal' for reporting.
        channel_exprs = (
            ",\n          COUNT(*) AS portal_count"
            ",\n          0 AS whatsapp_count"
            ",\n          0 AS email_count"
        )
    # We need:
    # - uploader identity (email + name)
    # - client_code (the uploader can belong to multiple clients; group
    #   on (email, client_code) so the report row reads cleanly)
    # - role (looked up from client_portal_users when present)
    # - count, sum, first/last date
    # - status breakdown
    # - channel breakdown (portal vs whatsapp vs email etc.)
    sql = f"""
        SELECT
          COALESCE(d.uploader_email,'__anonymous__') AS uploader_email,
          COALESCE(d.uploader_name, d.uploader_email, 'Anonymous') AS uploader_name,
          d.client_code AS client_code,
          COALESCE(cpu.role, 'external') AS role,
          COALESCE(cpu.status, 'unknown') AS user_status,
          COUNT(*) AS document_count,
          COALESCE(SUM(d.amount), 0) AS total_amount,
          MIN(COALESCE(d.uploaded_at, d.document_date, d.created_at)) AS first_upload,
          MAX(COALESCE(d.uploaded_at, d.document_date, d.created_at)) AS last_upload,
          SUM(CASE WHEN LOWER(COALESCE(d.review_status,'')) IN
                    ('needsreview','new','processing','submitted') THEN 1 ELSE 0 END) AS pending_count,
          SUM(CASE WHEN LOWER(COALESCE(d.review_status,'')) IN
                    ('posted','approved','ready') THEN 1 ELSE 0 END) AS approved_count,
          SUM(CASE WHEN LOWER(COALESCE(d.review_status,'')) IN
                    ('rejected','ignored','deleted') THEN 1 ELSE 0 END) AS rejected_count
          {channel_exprs}
        FROM documents d
        LEFT JOIN client_portal_users cpu
          ON cpu.client_code = d.client_code
         AND LOWER(cpu.email) = LOWER(d.uploader_email)
        {where_sql}
        GROUP BY uploader_email, d.client_code
        ORDER BY {sort_by} {sort_dir.upper()}
    """
    with _open(db_path) as conn:
        try:
            rows = conn.execute(sql, tuple(params)).fetchall()
        except sqlite3.OperationalError as exc:
            log.warning('uploader aggregate failed: %s', exc)
            return []
    out = []
    for r in rows:
        d = dict(r)
        d['total_amount'] = round(float(d['total_amount'] or 0.0), 2)
        out.append(d)
    return out


def top_uploaders_this_week(
    db_path: Path | str, *,
    firm_code: str | None = None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Home-widget summary: top-N uploaders by count for the last 7
    days. Excludes anonymous rows."""
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=7)).isoformat()
    end = today.isoformat()
    rows = aggregate(
        db_path, firm_code=firm_code, start=start, end=end,
        sort_by='document_count', sort_dir='desc',
    )
    return rows[:max(1, limit)]


def documents_for_uploader(
    db_path: Path | str, *,
    firm_code: str | None = None,
    uploader_email: str,
    client_code: str | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Drill-down: every document from a single uploader in range."""
    where = ["LOWER(COALESCE(d.uploader_email,'')) = LOWER(?)"]
    params: list[Any] = [uploader_email or '']
    if firm_code:
        where.append(
            "d.client_code IN "
            "(SELECT client_code FROM clients WHERE firm_code=?)"
        )
        params.append(firm_code)
    if client_code:
        where.append("d.client_code = ?")
        params.append(client_code)
    if start:
        where.append("COALESCE(d.document_date,'') >= ?")
        params.append(start)
    if end:
        where.append("COALESCE(d.document_date,'') <= ?")
        params.append(end)
    sql = (
        "SELECT d.document_id, d.client_code, d.vendor, d.amount, "
        "       d.document_date, d.review_status, d.uploaded_at, "
        "       d.uploader_name, d.uploader_email "
        "FROM documents d "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY COALESCE(d.uploaded_at, d.document_date) DESC "
        "LIMIT ?"
    )
    params.append(limit)
    with _open(db_path) as conn:
        try:
            rows = conn.execute(sql, tuple(params)).fetchall()
        except sqlite3.OperationalError as exc:
            log.warning('uploader drilldown failed: %s', exc)
            return []
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Export + render
# ---------------------------------------------------------------------------


CSV_COLUMNS = [
    ('uploader_name', 'Uploader'),
    ('uploader_email', 'Email'),
    ('client_code', 'Client'),
    ('role', 'Role'),
    ('document_count', 'Documents'),
    ('total_amount', 'Total amount (CAD)'),
    ('pending_count', 'Pending'),
    ('approved_count', 'Approved'),
    ('rejected_count', 'Rejected'),
    ('first_upload', 'First upload'),
    ('last_upload', 'Last upload'),
]


def render_csv(rows: Iterable[dict[str, Any]]) -> bytes:
    """Return CSV bytes (UTF-8, Excel-safe) with header row first."""
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    writer.writerow([label for _, label in CSV_COLUMNS])
    for r in rows:
        writer.writerow([r.get(key, '') for key, _ in CSV_COLUMNS])
    return buf.getvalue().encode('utf-8-sig')  # -sig for Excel BOM


def _esc(v: Any) -> str:
    return _html.escape(str(v if v is not None else ''))


def render_report_page(
    *,
    rows: list[dict[str, Any]],
    start: str, end: str,
    firm_code: str | None,
    client_code: str | None,
    clients_available: list[dict[str, Any]] | None = None,
    sort_by: str = 'document_count',
    sort_dir: str = 'desc',
    flash: str = '', flash_error: str = '',
) -> str:
    """CPA report page HTML."""
    # Client picker
    client_opts = '<option value="">— all clients —</option>'
    for c in (clients_available or []):
        sel = ' selected' if c['client_code'] == client_code else ''
        client_opts += (
            f'<option value="{_esc(c["client_code"])}"{sel}>'
            f'{_esc(c["client_code"])} — {_esc(c.get("client_name") or "")}'
            '</option>'
        )

    flash_html = ''
    if flash:
        flash_html = f'<div class="flash success">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{_esc(flash_error)}</div>'

    def _sort_link(col: str, label: str) -> str:
        new_dir = 'asc' if sort_by == col and sort_dir == 'desc' else 'desc'
        arrow = ''
        if sort_by == col:
            arrow = ' &uarr;' if sort_dir == 'asc' else ' &darr;'
        return (
            f'<a href="?start={_esc(start)}&amp;end={_esc(end)}'
            f'&amp;client_code={_esc(client_code or "")}'
            f'&amp;sort_by={col}&amp;sort_dir={new_dir}" '
            f'style="color:inherit;text-decoration:none;">'
            f'{_esc(label)}{arrow}</a>'
        )

    row_html = ''
    for r in rows:
        uploader_key = r['uploader_email']
        # Channel breakdown: "45 portal / 23 WhatsApp" — omit zeros
        # so a single-channel uploader doesn't get a noisy cell.
        channel_parts = []
        if int(r.get('portal_count') or 0):
            channel_parts.append(f'{int(r["portal_count"])} portal')
        if int(r.get('whatsapp_count') or 0):
            channel_parts.append(
                f'{int(r["whatsapp_count"])} WhatsApp'
            )
        if int(r.get('email_count') or 0):
            channel_parts.append(f'{int(r["email_count"])} email')
        channel_cell = _esc(' / '.join(channel_parts) or '—')
        row_html += (
            '<tr>'
            f'<td>{_esc(r["uploader_name"])}</td>'
            f'<td>{_esc(r["uploader_email"])}</td>'
            f'<td>{_esc(r["client_code"])}</td>'
            f'<td><span style="background:#e5e7eb;padding:2px 8px;'
            f'border-radius:10px;font-size:12px;">{_esc(r["role"])}</span></td>'
            f'<td style="text-align:right;">{r["document_count"]}</td>'
            f'<td style="font-size:12px;color:#374151;">{channel_cell}</td>'
            f'<td style="text-align:right;">${r["total_amount"]:,.2f}</td>'
            f'<td style="text-align:right;color:#6b7280;">{r["pending_count"]}</td>'
            f'<td style="text-align:right;color:#16a34a;">{r["approved_count"]}</td>'
            f'<td style="text-align:right;color:#dc2626;">{r["rejected_count"]}</td>'
            f'<td>{_esc(r["first_upload"])}</td>'
            f'<td>{_esc(r["last_upload"])}</td>'
            f'<td><a href="/reports/by_uploader/drill?'
            f'uploader_email={_esc(uploader_key)}'
            f'&amp;client_code={_esc(r["client_code"])}'
            f'&amp;start={_esc(start)}&amp;end={_esc(end)}">View &rarr;</a></td>'
            '</tr>'
        )
    if not row_html:
        row_html = '<tr><td colspan="13" style="text-align:center;color:#6b7280;">No uploads in this range.</td></tr>'

    csv_href = (
        f'/reports/by_uploader.csv?start={_esc(start)}'
        f'&amp;end={_esc(end)}&amp;client_code={_esc(client_code or "")}'
        f'&amp;sort_by={_esc(sort_by)}&amp;sort_dir={_esc(sort_dir)}'
    )

    return (
        '<div class="card" style="max-width:1100px;margin:1rem auto;">'
        '<h2>Uploader activity</h2>'
        f'{flash_html}'
        '<form method="GET" action="/reports/by_uploader" '
        'style="display:flex;gap:8px;align-items:center;'
        'margin-bottom:12px;flex-wrap:wrap;">'
        f'<label>From <input type="date" name="start" value="{_esc(start)}"></label>'
        f'<label>To <input type="date" name="end" value="{_esc(end)}"></label>'
        f'<label>Client <select name="client_code">{client_opts}</select></label>'
        '<button type="submit" class="primary" '
        'style="background:#1e40af;color:white;padding:6px 12px;">Apply</button>'
        f'<a href="{csv_href}" class="button-link" '
        'style="margin-left:auto;background:#059669;color:white;'
        'padding:6px 12px;border-radius:4px;text-decoration:none;">'
        'Download CSV</a>'
        '</form>'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr>'
        f'<th>{_sort_link("uploader_name", "Name")}</th>'
        f'<th>{_sort_link("uploader_email", "Email")}</th>'
        f'<th>{_sort_link("client_code", "Client")}</th>'
        '<th>Role</th>'
        f'<th>{_sort_link("document_count", "Docs")}</th>'
        '<th>Channel</th>'
        f'<th>{_sort_link("total_amount", "Total")}</th>'
        f'<th>{_sort_link("pending_count", "Pending")}</th>'
        f'<th>{_sort_link("approved_count", "Approved")}</th>'
        f'<th>{_sort_link("rejected_count", "Rejected")}</th>'
        f'<th>{_sort_link("first_upload", "First")}</th>'
        f'<th>{_sort_link("last_upload", "Last")}</th>'
        '<th></th></tr></thead>'
        f'<tbody>{row_html}</tbody></table>'
        '</div>'
    )


def render_top_uploaders_widget(
    rows: list[dict[str, Any]], *, lang: str = 'en',
) -> str:
    """Compact widget for the home dashboard sidebar."""
    title = 'Top uploaders this week' if lang == 'en' else 'Téléverseurs — cette semaine'
    if not rows:
        return (
            '<div class="card" style="padding:12px;margin-bottom:12px;'
            'background:#f9fafb;border:1px solid #e5e7eb;">'
            f'<h3 style="margin:0 0 6px;font-size:14px;">{_esc(title)}</h3>'
            '<p style="color:#6b7280;font-size:13px;margin:0;">'
            'No uploads in the last 7 days.</p></div>'
        )
    items = ''
    for r in rows:
        items += (
            f'<li style="padding:4px 0;display:flex;justify-content:space-between;">'
            f'<span>{_esc(r["uploader_name"])} '
            f'<span style="color:#9ca3af;">({_esc(r["client_code"])})</span>'
            '</span>'
            f'<span style="color:#374151;font-weight:600;">{r["document_count"]}</span>'
            '</li>'
        )
    link = 'See full report' if lang == 'en' else 'Voir le rapport complet'
    return (
        '<div class="card" style="padding:12px;margin-bottom:12px;'
        'background:#fffef5;border:1px solid #d4cfa8;">'
        f'<h3 style="margin:0 0 6px;font-size:14px;">{_esc(title)}</h3>'
        f'<ul style="margin:0;padding:0;list-style:none;">{items}</ul>'
        '<p style="margin:6px 0 0;font-size:13px;">'
        f'<a href="/reports/by_uploader">{_esc(link)} &rarr;</a></p>'
        '</div>'
    )
