"""HTTP route handlers for Gap 1-5 surfaces.

This module contains renderers + dispatch functions so the 24k-line
dashboard monolith only needs thin hooks in `do_GET` / `do_POST` that
delegate here. Each handler:

1. Receives the session user + an open handler reference (for cookies/
   redirects), so it can call the shared _send_html / _redirect helpers.
2. Calls the pure engine functions in src/integrations for state.
3. Returns True when it handled the request (caller returns without
   touching the default 404 fallback).

Grouped by gap:

    Gap 1 — /onboarding*, /tour, /onboarding/checklist, welcome modal
    Gap 2 — /my_tasks, /review_queue, /document/{id}/*
    Gap 3 — /owner/dashboard*, /owner/firms/*, /owner/impersonate, feedback
    Gap 4 — /close/wizard*
    Gap 5 — /c/{token}/status, /c/{token}/activity, /c/{token}/messages*

The impersonation block is enforced here — any path that mutates state
checks `impersonation.active_session()` and 403s when one exists.
"""
from __future__ import annotations

import html as _html
import json
import logging
import sqlite3
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.integrations import (
    client_status as _cs,
    impersonation as _imp,
    month_end_close as _close,
    notification_sender as _notify,
    onboarding_checklist as _ob,
    owner_dashboard as _od,
    review_workflow as _rw,
)

log = logging.getLogger(__name__)


def _esc(v: Any) -> str:
    return _html.escape(str(v if v is not None else ''))


def _rget(row: Any, key: str, default: Any = None) -> Any:
    """Get a value from either a dict or a sqlite3.Row safely."""
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (IndexError, KeyError):
        return default


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Schema bootstrap (called once from review_dashboard.bootstrap_schema)
# ---------------------------------------------------------------------------


def ensure_all_gap_schemas(db_path: Path | str) -> None:
    _ob.ensure_onboarding_schema(db_path)
    _rw.ensure_review_schema(db_path)
    _close.ensure_close_schema(db_path)
    _cs.ensure_client_status_schema(db_path)
    _od  # re-export marker
    _imp.ensure_impersonation_schema(db_path)
    _notify.ensure_sender_schema(db_path)
    _ensure_feedback_schema(db_path)


def _ensure_feedback_schema(db_path: Path | str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT,
                submitter_email TEXT,
                subject TEXT,
                body TEXT,
                response_body TEXT,
                responded_by TEXT,
                responded_at TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()


# ---------------------------------------------------------------------------
# Impersonation cookie helpers
# ---------------------------------------------------------------------------


IMP_COOKIE = 'otocpa_imp_sid'


def read_imp_cookie(handler) -> str:
    cookie = handler.headers.get('Cookie', '') or ''
    for part in cookie.split(';'):
        part = part.strip()
        if part.startswith(f'{IMP_COOKIE}='):
            return part[len(f'{IMP_COOKIE}='):]
    return ''


def set_imp_cookie(sid: str) -> tuple[str, str]:
    return (
        'Set-Cookie',
        f'{IMP_COOKIE}={sid}; HttpOnly; SameSite=Lax; Path=/; Max-Age={12*3600}',
    )


def clear_imp_cookie() -> tuple[str, str]:
    return (
        'Set-Cookie',
        f'{IMP_COOKIE}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0',
    )


def current_impersonation(db_path: Path | str, handler) -> dict[str, Any] | None:
    sid = read_imp_cookie(handler)
    if not sid:
        return None
    return _imp.active_session(db_path, session_id=sid)


def effective_firm_code(db_path: Path | str, handler, ctx: dict[str, Any]) -> str:
    sess = current_impersonation(db_path, handler)
    if sess:
        return sess.get('impersonated_firm_code') or ctx.get('firm_code', '')
    return ctx.get('firm_code', '')


def block_write_if_impersonating(
    db_path: Path | str, handler, *, action: str, path: str, method: str,
) -> bool:
    """Returns True when the request must be blocked. Also audit-logs."""
    sess = current_impersonation(db_path, handler)
    if not sess:
        return False
    _imp.log_action(
        db_path, session_id=sess['session_id'],
        original_user_email=sess['original_user_email'],
        firm_code=sess['impersonated_firm_code'],
        action=action, path=path, method=method, blocked=True,
    )
    return True


# ---------------------------------------------------------------------------
# Gap 1 — onboarding / tour / checklist
# ---------------------------------------------------------------------------


def render_onboarding_quick_setup(
    db_path: Path | str, *, firm_code: str, lang: str = 'en',
    flash: str = '', flash_error: str = '',
) -> str:
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM firms WHERE firm_code=?", (firm_code,),
        ).fetchone()
    firm = dict(row) if row else {}
    name = _esc(firm.get('name') or firm.get('firm_name') or '')
    address = _esc(firm.get('address') or '')
    phone = _esc(firm.get('phone') or '')
    default_lang = _esc(firm.get('default_lang') or 'en')
    fye = _esc(firm.get('fiscal_year_end') or '12-31')
    flash_html = ''
    if flash:
        flash_html = f'<div class="flash success">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{_esc(flash_error)}</div>'
    return (
        '<div class="card" style="max-width:600px;margin:1rem auto;">'
        '<h2>Quick setup</h2>'
        '<p>Complete your firm profile to unlock the rest of the checklist.</p>'
        f'{flash_html}'
        '<form method="POST" action="/onboarding/save" style="display:grid;gap:10px;">'
        '<label>Firm name<br>'
        f'<input type="text" name="name" value="{name}" required '
        'style="width:100%;padding:8px;"></label>'
        '<label>Address<br>'
        f'<input type="text" name="address" value="{address}" required '
        'style="width:100%;padding:8px;"></label>'
        '<label>Phone<br>'
        f'<input type="text" name="phone" value="{phone}" required '
        'style="width:100%;padding:8px;"></label>'
        '<label>Default language<br>'
        '<select name="default_lang" style="padding:8px;">'
        f'<option value="en"{" selected" if default_lang=="en" else ""}>English</option>'
        f'<option value="fr"{" selected" if default_lang=="fr" else ""}>Fran&ccedil;ais</option>'
        '</select></label>'
        '<label>Fiscal year end (MM-DD)<br>'
        f'<input type="text" name="fiscal_year_end" value="{fye}" '
        'placeholder="12-31" style="padding:8px;"></label>'
        '<button type="submit" class="primary" style="padding:10px 16px;">Save</button>'
        '</form>'
        '<p style="margin-top:12px;"><a href="/">Back to dashboard</a></p>'
        '</div>'
    )


def render_tour_screens(step: int, lang: str = 'en') -> str:
    steps = [
        ('Welcome to your queue',
         'The home page lists documents awaiting action. Filter by status, '
         'assign to staff, or submit for review — all in one place.'),
        ('Upload receipts',
         'Drop files on the upload page, email them to your ingest address, '
         'or let your clients upload directly through the portal link.'),
        ('Review + approve',
         'Employees submit finished work; owners / firm admins approve or '
         'reject with notes. Approved items post straight to QuickBooks.'),
        ('Close the month',
         'The close wizard walks through six checks and locks the period. '
         'Save and continue later — state persists between sessions.'),
        ('You are all set',
         'Head to the getting-started checklist on the right of every page. '
         'Each item ticks itself when the underlying action is complete.'),
    ]
    total = len(steps)
    step = max(1, min(total, step))
    title, body = steps[step - 1]
    next_href = f'/tour?step={step+1}' if step < total else '/tour/complete'
    next_label = 'Next' if step < total else 'Finish'
    prev_html = ''
    if step > 1:
        prev_html = f'<a href="/tour?step={step-1}" style="margin-right:8px;">&larr; Back</a>'
    skip_html = '<a href="/tour/complete" style="color:#888;margin-left:12px;">Skip tour</a>'
    finish_submit = ''
    if next_label == 'Finish':
        finish_submit = (
            '<form method="POST" action="/tour/complete" style="display:inline;">'
            '<button type="submit" class="primary" style="padding:8px 20px;">Finish</button>'
            '</form>'
        )
        next_btn = finish_submit
    else:
        next_btn = (
            f'<a href="{next_href}" class="primary" '
            'style="background:#1e40af;color:white;padding:8px 20px;'
            'border-radius:4px;text-decoration:none;">Next &rarr;</a>'
        )
    return (
        '<div class="card" style="max-width:600px;margin:2rem auto;text-align:center;">'
        f'<div style="color:#888;">Step {step} of {total}</div>'
        f'<h2>{_esc(title)}</h2>'
        f'<p style="line-height:1.6;font-size:16px;">{_esc(body)}</p>'
        '<div style="margin-top:2rem;">'
        f'{prev_html}{next_btn}{skip_html}'
        '</div></div>'
    )


# ---------------------------------------------------------------------------
# Gap 2 — review queue + my tasks
# ---------------------------------------------------------------------------


def render_my_tasks(
    db_path: Path | str, *, assignee_email: str, lang: str = 'en',
    flash: str = '', flash_error: str = '',
) -> str:
    tasks = _rw.my_tasks(db_path, assignee_email=assignee_email)
    flash_html = ''
    if flash:
        flash_html = f'<div class="flash success">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{_esc(flash_error)}</div>'
    if not tasks:
        return (
            '<div class="card">'
            '<h2>My Tasks</h2>'
            f'{flash_html}'
            '<p>Nothing assigned to you right now. When an owner assigns a '
            'document / journal entry for your review, it will appear here.</p>'
            '</div>'
        )
    rows = []
    for t in tasks:
        eid = _esc(t['entity_id'])
        etype = _esc(t['entity_type'])
        pri = _esc(t.get('priority') or 'normal')
        status = _esc(t.get('status') or '')
        assigned = _esc(t.get('assigned_at') or '')
        submit_form = (
            f'<form method="POST" action="/document/{eid}/submit_for_review" '
            'style="display:inline;">'
            '<button type="submit" class="primary">Submit for review</button>'
            '</form>'
        )
        escalate_form = (
            f'<form method="POST" action="/document/{eid}/escalate" '
            'style="display:inline;margin-left:6px;">'
            '<button type="submit">Escalate</button>'
            '</form>'
        )
        rows.append(
            '<tr>'
            f'<td>{etype}</td>'
            f'<td><a href="/document?id={eid}">{eid}</a></td>'
            f'<td><span style="font-weight:bold;color:'
            f'{"#b91c1c" if pri == "urgent" else "#333"}">{pri}</span></td>'
            f'<td>{status}</td>'
            f'<td>{assigned}</td>'
            f'<td>{submit_form}{escalate_form}</td>'
            '</tr>'
        )
    return (
        '<div class="card">'
        f'<h2>My Tasks <span style="color:#888;">({len(tasks)})</span></h2>'
        f'{flash_html}'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr>'
        '<th>Type</th><th>ID</th><th>Priority</th>'
        '<th>Status</th><th>Assigned</th><th>Actions</th>'
        '</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table></div>'
    )


def render_review_queue(
    db_path: Path | str, *, firm_code: str, lang: str = 'en',
    flash: str = '', flash_error: str = '',
) -> str:
    pending = _rw.pending_reviews(db_path, firm_code=firm_code)
    flash_html = ''
    if flash:
        flash_html = f'<div class="flash success">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{_esc(flash_error)}</div>'
    if not pending:
        return (
            '<div class="card">'
            '<h2>Review Queue</h2>'
            f'{flash_html}'
            '<p>No items waiting for review. When an employee submits a '
            'document you will see it here.</p></div>'
        )
    rows = []
    for p in pending:
        eid = _esc(p['entity_id'])
        etype = _esc(p['entity_type'])
        pri = _esc(p.get('priority') or 'normal')
        status = _esc(p.get('status') or '')
        submitted_at = _esc(p.get('submitted_at') or '')
        by = _esc(p.get('submitted_by_email') or '')
        approve_form = (
            f'<form method="POST" action="/document/{eid}/approve" '
            'style="display:inline;">'
            '<button type="submit" class="primary" '
            'style="background:#16C172;color:black;">Approve</button>'
            '</form>'
        )
        reject_form = (
            f'<form method="POST" action="/document/{eid}/reject" '
            'style="display:inline;margin-left:6px;">'
            '<input type="text" name="reason" placeholder="Reason (required)" '
            'style="padding:4px;">'
            '<button type="submit" style="background:#dc2626;color:white;">'
            'Reject</button>'
            '</form>'
        )
        rows.append(
            '<tr>'
            f'<td><input type="checkbox" name="entity_ids" value="{eid}"></td>'
            f'<td>{etype}</td>'
            f'<td><a href="/document?id={eid}">{eid}</a></td>'
            f'<td>{pri}</td>'
            f'<td>{status}</td>'
            f'<td>{by}</td>'
            f'<td>{submitted_at}</td>'
            f'<td>{approve_form}{reject_form}</td>'
            '</tr>'
        )
    return (
        '<div class="card">'
        f'<h2>Review Queue <span style="color:#888;">({len(pending)})</span></h2>'
        f'{flash_html}'
        '<form method="POST" action="/review_queue/bulk_approve">'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr>'
        '<th></th><th>Type</th><th>ID</th><th>Priority</th>'
        '<th>Status</th><th>Submitted by</th><th>At</th><th>Actions</th>'
        '</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table>'
        '<button type="submit" class="primary" '
        'style="margin-top:1rem;background:#16C172;color:black;">'
        'Bulk approve selected</button>'
        '</form></div>'
    )


# ---------------------------------------------------------------------------
# Gap 3 — owner dashboard + impersonation
# ---------------------------------------------------------------------------


def render_owner_dashboard(
    db_path: Path | str, *, flash: str = '', flash_error: str = '',
) -> str:
    bundle = _od.build_dashboard(db_path)
    rev = bundle['revenue']
    firms = bundle['firms']
    sys_h = bundle['system']
    support = bundle['support']
    alerts = bundle['alerts']
    feedback = bundle['feedback']
    drilldown = bundle['drilldown']

    widget = (
        '<div class="owner-widget" style="background:white;border:1px solid #ddd;'
        'padding:14px;border-radius:6px;margin-bottom:12px;">'
    )
    rev_html = (
        f'{widget}<h3 style="margin-top:0;">Revenue</h3>'
        f'<div style="font-size:28px;font-weight:bold;">'
        f'${rev["mrr_cad"]:,.2f}<span style="font-size:14px;color:#888;"> MRR</span>'
        '</div>'
        f'<div>Failed payments (7d): {rev["failed_payments_7d"]}</div>'
        f'<div>At-risk subscriptions: {rev["at_risk_count"]}</div></div>'
    )
    firms_html = (
        f'{widget}<h3 style="margin-top:0;">Firms</h3>'
        f'<div>Total: {firms["total_firms"]}</div>'
        f'<div>Active this week: {firms["active_this_week"]}</div>'
        f'<div>Never logged in: {firms["never_logged_in"]}</div></div>'
    )
    sys_html = (
        f'{widget}<h3 style="margin-top:0;">System</h3>'
        f'<div>DB: {sys_h["db_size_mb"]} MB</div>'
        f'<div>Disk used: {sys_h["disk_used_percent"]}%</div>'
        f'<div>RSS: {sys_h["rss_mb"]} MB</div>'
        f'<div>Last QBO sync: {_esc(sys_h.get("last_qbo_sync_success") or "never")}</div></div>'
    )
    alerts_rows = ''.join(
        f'<li><strong style="color:#b91c1c;">[{_esc(a["severity"])}]</strong> {_esc(a["message"])}</li>'
        for a in alerts
    ) or '<li>No alerts.</li>'
    alerts_html = (
        f'{widget}<h3 style="margin-top:0;">Alerts</h3>'
        f'<ul>{alerts_rows}</ul></div>'
    )
    feedback_rows = ''.join(
        f'<li>{_esc(f.get("subject") or "(no subject)")} &mdash; '
        f'<em>{_esc(f.get("submitter_email") or "anonymous")}</em></li>'
        for f in feedback
    ) or '<li>No feedback yet.</li>'
    feedback_html = (
        f'{widget}<h3 style="margin-top:0;">Recent feedback '
        f'<a href="/owner/feedback" style="font-size:13px;">(all)</a></h3>'
        f'<ul>{feedback_rows}</ul></div>'
    )
    support_html = (
        f'{widget}<h3 style="margin-top:0;">Support queue</h3>'
        f'<div>Open feedback: {support["open_feedback"]}</div>'
        f'<div>Firms with errors (24h): '
        f'{len(support["firms_with_errors_24h"])}</div></div>'
    )
    drilldown_rows = ''.join(
        '<tr>'
        f'<td><a href="/owner/firms/{_esc(f["firm_code"])}">{_esc(f["firm_code"])}</a></td>'
        f'<td>{_esc(f.get("name") or "")}</td>'
        f'<td>{_esc(f.get("plan") or "")}</td>'
        f'<td>{_esc(f.get("last_login") or "never")}</td>'
        f'<td>{f.get("doc_count", 0)}</td>'
        f'<td>${f.get("mrr_cad", 0):.2f}</td>'
        f'<td><a href="/owner/firms/{_esc(f["firm_code"])}/impersonate" '
        'style="color:#856404;">Impersonate</a></td>'
        '</tr>'
        for f in drilldown
    )
    drilldown_html = (
        '<div class="owner-widget" style="background:white;border:1px solid #ddd;'
        'padding:14px;border-radius:6px;grid-column:1/-1;">'
        '<h3 style="margin-top:0;">Per-firm drilldown</h3>'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr><th>Firm</th><th>Name</th><th>Plan</th>'
        '<th>Last login</th><th>Docs</th><th>MRR</th><th></th></tr></thead>'
        f'<tbody>{drilldown_rows}</tbody></table></div>'
    )

    flash_html = ''
    if flash:
        flash_html = f'<div class="flash success">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{_esc(flash_error)}</div>'

    return (
        '<div style="padding:1rem;">'
        '<h1>Owner dashboard</h1>'
        f'{flash_html}'
        '<div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));'
        'gap:12px;margin-bottom:12px;" id="owner-metrics-grid">'
        f'{rev_html}{firms_html}{sys_html}'
        f'{alerts_html}{feedback_html}{support_html}'
        f'{drilldown_html}'
        '</div>'
        '<script>'
        'setInterval(async()=>{try{'
        'const r=await fetch("/owner/dashboard/metrics");'
        'if(!r.ok)return;await r.json();'
        '}catch(e){}},60000);'
        '</script></div>'
    )


def render_firm_drilldown(
    db_path: Path | str, *, firm_code: str,
) -> str:
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        firm = conn.execute(
            "SELECT * FROM firms WHERE firm_code=?", (firm_code,),
        ).fetchone()
        clients = conn.execute(
            "SELECT * FROM clients WHERE firm_code=?", (firm_code,),
        ).fetchall()
        users = conn.execute(
            "SELECT username, role, first_login_at "
            "FROM dashboard_users WHERE firm_code=?", (firm_code,),
        ).fetchall()
        doc_n = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE firm_code=?",
            (firm_code,),
        ).fetchone()[0]
    if not firm:
        return '<div class="card"><h2>Firm not found</h2></div>'
    client_rows = ''.join(
        f'<tr><td>{_esc(c["client_code"])}</td>'
        f'<td>{_esc(_rget(c, "client_name") or "")}</td>'
        f'<td>{_esc(_rget(c, "portal_token") or "")[:12] or "—"}</td></tr>'
        for c in clients
    ) or '<tr><td colspan="3">No clients yet.</td></tr>'
    user_rows = ''.join(
        f'<tr><td>{_esc(u["username"])}</td>'
        f'<td>{_esc(u["role"])}</td>'
        f'<td>{_esc(_rget(u, "first_login_at") or "never")}</td></tr>'
        for u in users
    ) or '<tr><td colspan="3">No users.</td></tr>'
    return (
        '<div class="card">'
        f'<h2>{_esc(firm["firm_code"])} &mdash; {_esc(_rget(firm, "name") or "(unnamed)")}</h2>'
        f'<p>Plan: <strong>{_esc(_rget(firm, "plan") or "")}</strong></p>'
        f'<p>Documents: {doc_n}</p>'
        f'<form method="POST" action="/owner/firms/{_esc(firm_code)}/impersonate">'
        '<button type="submit" class="primary" '
        'style="background:#856404;color:white;">Impersonate (read-only)</button>'
        '</form>'
        f'<h3>Clients ({len(clients)})</h3>'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr><th>Code</th><th>Name</th><th>Portal token</th></tr></thead>'
        f'<tbody>{client_rows}</tbody></table>'
        f'<h3>Users ({len(users)})</h3>'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr><th>User</th><th>Role</th><th>First login</th></tr></thead>'
        f'<tbody>{user_rows}</tbody></table>'
        '<p style="margin-top:1rem;">'
        '<a href="/owner/dashboard">&larr; Back to owner dashboard</a></p>'
        '</div>'
    )


def render_feedback_queue(db_path: Path | str) -> str:
    _ensure_feedback_schema(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM feedback ORDER BY "
            "CASE WHEN responded_at IS NULL OR responded_at='' THEN 0 ELSE 1 END, "
            "created_at DESC LIMIT 100"
        ).fetchall()
    if not rows:
        return '<div class="card"><h2>Feedback queue</h2><p>No feedback yet.</p></div>'
    tiles = []
    for r in rows:
        r = dict(r)
        responded = bool(r.get('responded_at'))
        bg = '#e8f5e9' if responded else '#fff3cd'
        form = ''
        if not responded:
            form = (
                f'<form method="POST" action="/owner/feedback/{r["id"]}/respond" '
                'style="margin-top:10px;">'
                '<textarea name="response_body" required rows="3" '
                'style="width:100%;padding:6px;"></textarea>'
                '<button type="submit" class="primary" '
                'style="margin-top:6px;">Send response</button>'
                '</form>'
            )
        else:
            form = (
                f'<div style="margin-top:8px;color:#155724;">'
                f'<strong>Responded by {_esc(r.get("responded_by") or "")}:</strong> '
                f'{_esc(r.get("response_body") or "")}</div>'
            )
        tiles.append(
            f'<div style="background:{bg};padding:12px;border-radius:6px;'
            'margin-bottom:10px;">'
            f'<div><strong>#{r["id"]}</strong> — '
            f'{_esc(r.get("subject") or "(no subject)")}</div>'
            f'<div style="color:#666;font-size:13px;">'
            f'{_esc(r.get("submitter_email") or "anonymous")} / '
            f'{_esc(r.get("firm_code") or "")} / {_esc(r.get("created_at") or "")}'
            '</div>'
            f'<div style="margin-top:6px;">{_esc(r.get("body") or "")}</div>'
            f'{form}</div>'
        )
    return (
        '<div class="card"><h2>Feedback queue</h2>'
        + ''.join(tiles)
        + '</div>'
    )


# ---------------------------------------------------------------------------
# Gap 4 — close wizard
# ---------------------------------------------------------------------------


def render_close_wizard(
    db_path: Path | str, *, firm_code: str, client_code: str = '',
    period: str = '', step_n: int = 1,
    flash: str = '', flash_error: str = '',
) -> str:
    step_n = max(1, min(6, step_n))
    state = None
    if client_code and period:
        state = _close.get_state(
            db_path, firm_code=firm_code, client_code=client_code,
            period=period,
        )

    flash_html = ''
    if flash:
        flash_html = f'<div class="flash success">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{_esc(flash_error)}</div>'

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        clients = conn.execute(
            "SELECT client_code, client_name FROM clients WHERE firm_code=? "
            "ORDER BY client_code", (firm_code,),
        ).fetchall()

    if not client_code or not period:
        # Step 1: picker
        options = ''.join(
            f'<option value="{_esc(c["client_code"])}">{_esc(c["client_code"])} — '
            f'{_esc(_rget(c, "client_name") or "")}</option>'
            for c in clients
        )
        return (
            '<div class="card" style="max-width:600px;margin:1rem auto;">'
            '<h2>Month-end close wizard</h2>'
            f'{flash_html}'
            '<p>Select a client and the period you want to close.</p>'
            '<form method="POST" action="/close/wizard/advance" '
            'style="display:grid;gap:10px;">'
            '<input type="hidden" name="step" value="1">'
            '<label>Client<br>'
            f'<select name="client_code" required style="padding:8px;">'
            '<option value="">— select —</option>'
            f'{options}</select></label>'
            '<label>Period (YYYY-MM)<br>'
            '<input type="text" name="period" pattern="[0-9]{4}-[0-9]{2}" '
            'placeholder="2026-04" required style="padding:8px;"></label>'
            '<button type="submit" class="primary" style="padding:10px 16px;">'
            'Begin close &rarr;</button>'
            '</form></div>'
        )

    steps = state['steps']
    current = state['current']

    # Render stepper
    bar = '<div style="display:flex;gap:4px;margin:1rem 0;">'
    for i, s in enumerate(steps, 1):
        color = '#16C172' if s['step_status'] == 'done' else (
            '#1e40af' if s['step'] == current else '#ddd')
        label = s['step'].replace('_', ' ')
        bar += (
            f'<div style="flex:1;padding:8px;text-align:center;'
            f'background:{color};color:white;border-radius:4px;'
            'font-size:12px;">'
            f'{i}. {label}</div>'
        )
    bar += '</div>'

    # Step-specific body
    body = _render_wizard_step(db_path, firm_code=firm_code,
                                 client_code=client_code, period=period,
                                 step_n=step_n, state=state)

    back_hidden = (
        f'<input type="hidden" name="client_code" value="{_esc(client_code)}">'
        f'<input type="hidden" name="period" value="{_esc(period)}">'
        f'<input type="hidden" name="step" value="{step_n}">'
    )
    back_form = ''
    if step_n > 1:
        back_form = (
            '<form method="POST" action="/close/wizard/back" '
            'style="display:inline;">'
            f'{back_hidden}'
            '<button type="submit">&larr; Back</button></form>'
        )
    save_form = (
        '<form method="POST" action="/close/wizard/save_progress" '
        'style="display:inline;margin-left:10px;">'
        f'{back_hidden}'
        '<button type="submit">Save and exit</button></form>'
    )

    return (
        '<div class="card" style="max-width:800px;margin:1rem auto;">'
        f'<h2>Close {_esc(client_code)} — {_esc(period)}</h2>'
        f'{flash_html}{bar}{body}'
        f'<div style="margin-top:1rem;">{back_form}{save_form}</div>'
        '</div>'
    )


_STEP_NAMES = {
    1: 'select_period', 2: 'process_documents', 3: 'reconcile_bank',
    4: 'accruals', 5: 'statements', 6: 'lock',
}


def _render_wizard_step(
    db_path: Path | str, *,
    firm_code: str, client_code: str, period: str, step_n: int,
    state: dict[str, Any],
) -> str:
    hidden = (
        f'<input type="hidden" name="client_code" value="{_esc(client_code)}">'
        f'<input type="hidden" name="period" value="{_esc(period)}">'
        f'<input type="hidden" name="step" value="{step_n}">'
    )
    step_row = next((s for s in state['steps']
                      if s['step'] == _STEP_NAMES.get(step_n)), None)
    done = step_row and step_row.get('step_status') == 'done'

    if step_n == 1:
        return (
            '<h3>Step 1 — confirm period</h3>'
            f'<p>Period <strong>{_esc(period)}</strong> selected. '
            'We will refuse to close this period if any earlier period is '
            'still open.</p>'
            + _advance_button(hidden, done=done, label='Confirm period')
        )
    if step_n == 2:
        return (
            '<h3>Step 2 — process documents</h3>'
            '<p>Every document dated in the period must be posted, ignored, '
            'or deleted before we can continue.</p>'
            + _advance_button(hidden, done=done, label='Mark documents processed')
        )
    if step_n == 3:
        return (
            '<h3>Step 3 — reconcile bank</h3>'
            '<p>Bank transactions must be matched to receipts (or marked as '
            'hidden duplicates). Tick the acknowledge box to bypass any '
            'remaining unmatched rows with an audit note.</p>'
            '<form method="POST" action="/close/wizard/advance" '
            'style="display:inline;">'
            f'{hidden}'
            '<label><input type="checkbox" name="acknowledge_unreconciled" '
            'value="1"> Acknowledge remaining unmatched</label>'
            '<button type="submit" class="primary" '
            'style="display:block;margin-top:10px;">Continue &rarr;</button>'
            '</form>'
        )
    if step_n == 4:
        suggestions = _close.suggest_accruals(
            db_path, firm_code=firm_code, client_code=client_code,
            period=period,
        )
        rows = ''.join(
            '<tr>'
            f'<td><input type="checkbox" name="accepted_kinds" value="{_esc(s["kind"])}"'
            f'{" checked" if float(s.get("amount_cad") or 0)>0 else ""}></td>'
            f'<td>{_esc(s["kind"])}</td>'
            f'<td>{_esc(s["description"])}</td>'
            f'<td>${float(s.get("amount_cad") or 0):,.2f}</td>'
            f'<td style="color:#888;font-size:12px;">{_esc(s.get("amount_hint") or "")}</td>'
            '</tr>'
            for s in suggestions
        )
        return (
            '<h3>Step 4 — accruals</h3>'
            '<p>Tick the accruals you want posted as draft JEs. '
            'Amounts are computed from your current data — zero means '
            'not enough history to suggest one.</p>'
            '<form method="POST" action="/close/wizard/advance">'
            f'{hidden}'
            '<table style="width:100%;border-collapse:collapse;">'
            '<thead><tr><th></th><th>Kind</th><th>Description</th>'
            '<th>Amount (CAD)</th><th>Source</th></tr></thead>'
            f'<tbody>{rows}</tbody></table>'
            '<button type="submit" class="primary" style="margin-top:10px;">'
            'Post all suggested accruals &rarr;</button>'
            '</form>'
        )
    if step_n == 5:
        return (
            '<h3>Step 5 — financial statements</h3>'
            '<p>Generate trial balance, income statement, and balance sheet. '
            'Any unbalanced statement surfaces as a warning.</p>'
            + _advance_button(hidden, done=done, label='Generate statements')
        )
    if step_n == 6:
        # Summary + final lock
        summary_rows = ''.join(
            f'<li>{i}. {s["step"]} — <strong>{_esc(s["step_status"])}</strong></li>'
            for i, s in enumerate(state['steps'], 1)
        )
        return (
            '<h3>Step 6 — lock period</h3>'
            f'<ul>{summary_rows}</ul>'
            '<p>Locking this period prevents further posts. You can still '
            'view reports but edits are refused. This is the final step.</p>'
            '<form method="POST" action="/close/wizard/finalize">'
            f'{hidden}'
            '<button type="submit" class="primary" '
            'style="background:#dc2626;color:white;padding:10px 20px;">'
            'Lock period</button></form>'
        )
    return '<p>Unknown step.</p>'


def _advance_button(hidden: str, *, done: bool, label: str) -> str:
    if done:
        return (
            '<p style="color:#155724;"><strong>&#10003; Already done.</strong> '
            'Click next to continue.</p>'
            '<form method="POST" action="/close/wizard/advance">'
            f'{hidden}'
            f'<button type="submit" class="primary">Next &rarr;</button></form>'
        )
    return (
        '<form method="POST" action="/close/wizard/advance">'
        f'{hidden}'
        f'<button type="submit" class="primary">{_esc(label)}</button></form>'
    )


# ---------------------------------------------------------------------------
# Gap 5 — client portal status + activity + messages
# ---------------------------------------------------------------------------


def render_portal_status_page(
    db_path: Path | str, *, client: dict[str, Any], token: str,
) -> str:
    code = client['client_code']
    bundle = _cs.build_client_status(db_path, client_code=code)
    up = bundle['upload_status']
    ytd = bundle['ytd_summary']
    notif = bundle['unread_notifications']
    threads = bundle['threads']
    act = bundle['recent_activity']

    tiles = (
        '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));'
        'gap:12px;">'
        f'<div class="tile"><div style="font-size:28px;font-weight:bold;">{up["this_month"]}</div>'
        '<div>Uploads this month</div></div>'
        f'<div class="tile"><div style="font-size:28px;font-weight:bold;">{up["processing"]}</div>'
        '<div>Being processed</div></div>'
        f'<div class="tile"><div style="font-size:28px;font-weight:bold;">{up["reviewed"]}</div>'
        '<div>Reviewed</div></div>'
        f'<div class="tile"><div style="font-size:28px;font-weight:bold;">{notif}</div>'
        '<div>Unread notifications</div></div>'
        '</div>'
    )

    act_rows = ''.join(
        f'<li><span style="color:#888;">{_esc(a.get("ts") or "")}</span> — '
        f'{_esc(a.get("summary") or "")}</li>'
        for a in act[:20]
    ) or '<li>No activity yet.</li>'

    thread_rows = ''
    for th in threads[:10]:
        tid = th['id']
        unread = th.get('unread_from_cpa') or 0
        badge = (f'<span style="background:#dc2626;color:white;'
                  f'padding:2px 6px;border-radius:10px;font-size:11px;">'
                  f'{unread}</span>') if unread else ''
        thread_rows += (
            f'<li><a href="/c/{_esc(token)}/messages?thread={tid}">'
            f'{_esc(th.get("subject") or "(no subject)")}</a> {badge}</li>'
        )
    if not thread_rows:
        thread_rows = '<li>No message threads yet.</li>'

    return (
        '<!DOCTYPE html><html><head>'
        '<meta charset="utf-8"><title>Your portal</title>'
        '<style>'
        'body{font-family:system-ui,Arial;max-width:900px;margin:2rem auto;padding:1rem;}'
        '.tile{background:#f3f4f6;padding:14px;border-radius:6px;text-align:center;}'
        'h2,h3{margin-top:1.4rem;}a{color:#1e40af;}'
        '</style></head><body>'
        f'<h1>{_esc(client.get("client_name") or code)}</h1>'
        '<p><a href="/c/' + _esc(token) + '/upload">Upload receipts</a> &middot; '
        '<a href="/c/' + _esc(token) + '/messages">Messages</a></p>'
        f'{tiles}'
        f'<h3>Recent activity</h3><ul>{act_rows}</ul>'
        '<p><a href="#" onclick="refreshActivity();return false;">Refresh</a></p>'
        f'<h3>YTD summary ({ytd["year"]})</h3>'
        f'<p>Total receipts: <strong>{ytd["total_receipts"]}</strong> &middot; '
        f'Expenses: <strong>${ytd["total_expenses_cad"]:,.2f}</strong> &middot; '
        f'This month: {ytd["this_month"]} &middot; '
        f'Prior month: {ytd["prior_month"]}</p>'
        f'<h3>Message threads</h3><ul>{thread_rows}</ul>'
        '<script>'
        'function refreshActivity(){'
        'fetch("/c/' + _esc(token) + '/activity").then(r=>r.json()).then(()=>{'
        'location.reload();});}'
        'setInterval(()=>fetch("/c/' + _esc(token) + '/activity").catch(()=>{}), 60000);'
        '</script></body></html>'
    )


def render_portal_messages_page(
    db_path: Path | str, *, client: dict[str, Any], token: str,
    thread_id: int | None = None,
    flash: str = '', flash_error: str = '',
) -> str:
    code = client['client_code']
    threads = _cs.list_threads(db_path, client_code=code)
    flash_html = ''
    if flash:
        flash_html = f'<div style="background:#d4edda;padding:8px;">{_esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div style="background:#f8d7da;padding:8px;">{_esc(flash_error)}</div>'

    thread_sidebar = ''.join(
        '<li>'
        f'<a href="/c/{_esc(token)}/messages?thread={t["id"]}">'
        f'{_esc(t.get("subject") or "(no subject)")}</a>'
        f' <span style="color:#888;font-size:12px;">({t.get("unread_from_cpa") or 0} unread)</span>'
        '</li>'
        for t in threads
    ) or '<li>No threads yet.</li>'

    thread_body = ''
    if thread_id:
        tdata = _cs.get_thread(db_path, thread_id=thread_id,
                                 mark_read_as='client')
        header = tdata.get('header') or {}
        if header and header.get('client_code') == code:
            posts = tdata.get('posts') or []
            post_rows = ''.join(
                f'<div style="margin-bottom:8px;padding:8px;background:'
                f'{"#e8f0fe" if p["sender_type"] == "cpa" else "#f3f4f6"};'
                'border-radius:6px;">'
                f'<div style="font-size:12px;color:#555;">{_esc(p["sender_type"])} '
                f'— {_esc(p.get("created_at") or "")}</div>'
                f'<div>{_esc(p.get("body") or "")}</div></div>'
                for p in posts
            )
            thread_body = (
                f'<h3>{_esc(header.get("subject") or "")}</h3>'
                f'{post_rows}'
                '<form method="POST" '
                f'action="/c/{_esc(token)}/messages/send" '
                'style="margin-top:1rem;">'
                f'<input type="hidden" name="thread_id" value="{thread_id}">'
                '<textarea name="body" rows="3" required '
                'style="width:100%;padding:6px;"></textarea>'
                '<button type="submit" class="primary" style="margin-top:6px;">'
                'Send</button></form>'
            )
        else:
            thread_body = '<p>Thread not found.</p>'

    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<title>Messages</title>'
        '<style>body{font-family:system-ui,Arial;max-width:900px;'
        'margin:2rem auto;padding:1rem;}a{color:#1e40af;}</style></head><body>'
        f'<p><a href="/c/{_esc(token)}/status">&larr; Back to status</a></p>'
        f'<h2>Messages</h2>{flash_html}'
        '<div style="display:grid;grid-template-columns:220px 1fr;gap:20px;">'
        f'<aside><ul>{thread_sidebar}</ul>'
        '<form method="POST" '
        f'action="/c/{_esc(token)}/messages/send" '
        'style="margin-top:1rem;padding-top:1rem;border-top:1px solid #eee;">'
        '<input type="hidden" name="new_thread" value="1">'
        '<input type="text" name="subject" placeholder="Subject" required '
        'style="width:100%;padding:6px;margin-bottom:6px;">'
        '<textarea name="body" rows="2" placeholder="Start a new thread..." '
        'required style="width:100%;padding:6px;"></textarea>'
        '<button type="submit" style="margin-top:6px;">New thread</button>'
        '</form>'
        '</aside>'
        f'<main>{thread_body or "<p>Select a thread.</p>"}</main>'
        '</div></body></html>'
    )
