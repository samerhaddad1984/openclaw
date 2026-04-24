"""Outstanding CPA requests tracker.

The CPA frequently needs specific things from a client ("please send
your March bank statement", "confirm the Costco receipt is personal
vs business"). Without tracking, these lose context quickly. This
module provides:

  - ``create_request()`` — CPA posts a titled/described request,
    optional due-date, optional target portal user.
  - ``list_open_for_client()`` — all open/overdue requests for a
    client (CPA view + admin portal view).
  - ``list_open_for_user()`` — what's on *my* plate as a portal user
    (targeted to me + untargeted).
  - ``mark_completed()`` — called when the client marks done OR
    uploads a document linked to the request.
  - ``overdue_requests()`` — for cron-driven reminders.

Schema lives in ``ensure_schema(db_path)``; modules are expected to
call this before first use (portal routes do so on demand).

A rendered HTML page ``render_client_tasks_page()`` is included so
``scripts/review_dashboard.py`` can surface the /cp/{token}/tasks view
without importing Jinja.
"""
from __future__ import annotations

import html as _html
import logging
import sqlite3
from datetime import datetime, timezone, date as _date, timedelta
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


STATUS_OPEN = 'open'
STATUS_COMPLETED = 'completed'
STATUS_CANCELLED = 'cancelled'


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def _esc(s: Any) -> str:
    return _html.escape(str(s or ""))


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def ensure_schema(db_path: Path | str) -> None:
    """Create ``client_requests`` if it doesn't exist (idempotent)."""
    with _open(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS client_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                client_code TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                due_date TEXT,
                target_portal_user_id INTEGER,
                status TEXT NOT NULL DEFAULT 'open',
                created_by_email TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                completed_at TEXT,
                completed_by_portal_user_id INTEGER,
                fulfillment_document_id TEXT,
                last_reminder_at TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_client_requests_firm_client "
            "ON client_requests(firm_code, client_code, status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_client_requests_target "
            "ON client_requests(target_portal_user_id, status)"
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Create / list / mutate
# ---------------------------------------------------------------------------


def create_request(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
    title: str, description: str = '',
    due_date: str | None = None,
    target_portal_user_id: int | None = None,
    created_by_email: str = '',
) -> int:
    """Insert a new open request. Returns the new row id."""
    if not title.strip():
        raise ValueError("request title is required")
    ensure_schema(db_path)
    with _open(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO client_requests (firm_code, client_code, title, "
            "description, due_date, target_portal_user_id, status, "
            "created_by_email, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (firm_code, client_code, title.strip(), description.strip(),
             due_date, target_portal_user_id, STATUS_OPEN,
             created_by_email, _iso_now()),
        )
        conn.commit()
        return int(cur.lastrowid)


def list_open_for_client(
    db_path: Path | str, *,
    firm_code: str, client_code: str, include_completed: bool = False,
) -> list[dict[str, Any]]:
    ensure_schema(db_path)
    with _open(db_path) as conn:
        if include_completed:
            rows = conn.execute(
                "SELECT * FROM client_requests "
                "WHERE firm_code=? AND client_code=? "
                "ORDER BY status='open' DESC, "
                "  COALESCE(due_date,'9999-12-31') ASC, created_at DESC",
                (firm_code, client_code),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM client_requests "
                "WHERE firm_code=? AND client_code=? AND status='open' "
                "ORDER BY COALESCE(due_date,'9999-12-31') ASC, "
                "created_at DESC",
                (firm_code, client_code),
            ).fetchall()
    return [dict(r) for r in rows]


def list_open_for_user(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
    portal_user_id: int,
) -> list[dict[str, Any]]:
    """Open requests targeted at this user OR untargeted (whole team)."""
    ensure_schema(db_path)
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM client_requests "
            "WHERE firm_code=? AND client_code=? AND status='open' "
            "  AND (target_portal_user_id IS NULL "
            "       OR target_portal_user_id = ?) "
            "ORDER BY COALESCE(due_date,'9999-12-31') ASC, "
            "created_at DESC",
            (firm_code, client_code, int(portal_user_id)),
        ).fetchall()
    return [dict(r) for r in rows]


def get_request(
    db_path: Path | str, *, request_id: int,
) -> dict[str, Any] | None:
    ensure_schema(db_path)
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM client_requests WHERE id=?",
            (int(request_id),),
        ).fetchone()
    return dict(row) if row else None


def mark_completed(
    db_path: Path | str, *,
    request_id: int,
    completed_by_portal_user_id: int | None = None,
    fulfillment_document_id: str | None = None,
) -> dict[str, Any] | None:
    """Mark a request completed. Idempotent; returns the updated row."""
    ensure_schema(db_path)
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM client_requests WHERE id=?",
            (int(request_id),),
        ).fetchone()
        if row is None:
            return None
        if row['status'] == STATUS_COMPLETED:
            # Already done — idempotent no-op.
            return dict(row)
        conn.execute(
            "UPDATE client_requests SET status=?, completed_at=?, "
            "completed_by_portal_user_id=?, fulfillment_document_id=? "
            "WHERE id=?",
            (STATUS_COMPLETED, _iso_now(),
             completed_by_portal_user_id, fulfillment_document_id,
             int(request_id)),
        )
        conn.commit()
        updated = conn.execute(
            "SELECT * FROM client_requests WHERE id=?",
            (int(request_id),),
        ).fetchone()

    # Notify CPA (best-effort).
    try:
        from src.integrations import notification_sender as _ns
        body_fr = (
            f"La demande « {updated['title']} » a été marquée complétée."
        )
        body_en = (
            f'Request "{updated["title"]}" was marked complete.'
        )
        if fulfillment_document_id:
            body_fr += f"\n(Document téléversé: {fulfillment_document_id})"
            body_en += f"\n(Uploaded document: {fulfillment_document_id})"
        _ns.enqueue(
            db_path,
            client_code=updated['client_code'],
            kind='client_request_fulfilled',
            title=f"Request fulfilled: {updated['title']}",
            body=f"{body_fr}\n\n{body_en}",
            recipient_email=updated['created_by_email'] or None,
            subject=f"[OtoCPA] {updated['title']}",
            priority=5,
        )
    except Exception:
        log.exception("fulfillment notification enqueue failed")

    return dict(updated) if updated else None


def cancel_request(
    db_path: Path | str, *, request_id: int,
) -> dict[str, Any] | None:
    ensure_schema(db_path)
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE client_requests SET status=? WHERE id=?",
            (STATUS_CANCELLED, int(request_id)),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM client_requests WHERE id=?",
            (int(request_id),),
        ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Overdue detection (for cron + reminders)
# ---------------------------------------------------------------------------


def overdue_requests(
    db_path: Path | str, *,
    firm_code: str | None = None,
    as_of: str | None = None,
) -> list[dict[str, Any]]:
    """Open requests whose due_date is before ``as_of`` (default: today)."""
    ensure_schema(db_path)
    as_of = as_of or _date.today().isoformat()
    with _open(db_path) as conn:
        if firm_code:
            rows = conn.execute(
                "SELECT * FROM client_requests "
                "WHERE status='open' AND firm_code=? "
                "  AND due_date IS NOT NULL AND due_date < ? "
                "ORDER BY due_date ASC",
                (firm_code, as_of),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM client_requests "
                "WHERE status='open' "
                "  AND due_date IS NOT NULL AND due_date < ? "
                "ORDER BY due_date ASC",
                (as_of,),
            ).fetchall()
    return [dict(r) for r in rows]


def send_overdue_reminders(
    db_path: Path | str, *,
    firm_code: str | None = None,
    cooldown_hours: int = 24,
) -> int:
    """Enqueue a reminder email for each overdue request.

    ``cooldown_hours`` prevents spamming — each request gets at most
    one reminder per cooldown window. Returns the count enqueued.
    """
    ensure_schema(db_path)
    overdue = overdue_requests(db_path, firm_code=firm_code)
    now_iso = _iso_now()
    cutoff = (datetime.now(timezone.utc)
              - timedelta(hours=cooldown_hours)).isoformat(timespec='seconds')
    sent = 0
    for req in overdue:
        last = req.get('last_reminder_at') or ''
        if last and last >= cutoff:
            continue  # still inside cooldown
        try:
            from src.integrations import notification_sender as _ns
            body = (
                f"Rappel / Reminder: {req['title']} "
                f"(échéance / due {req.get('due_date')})"
            )
            _ns.enqueue(
                db_path,
                client_code=req['client_code'],
                kind='client_request_overdue',
                title=f"Overdue: {req['title']}",
                body=body,
                recipient_email=req.get('created_by_email') or None,
                subject=f"[OtoCPA] Overdue: {req['title']}",
                priority=2,
            )
            with _open(db_path) as conn:
                conn.execute(
                    "UPDATE client_requests SET last_reminder_at=? "
                    "WHERE id=?",
                    (now_iso, int(req['id'])),
                )
                conn.commit()
            sent += 1
        except Exception:
            log.exception("overdue reminder enqueue failed")
    return sent


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------


def render_client_tasks_page(
    *, client: dict[str, Any], user_token: str,
    portal_user: dict[str, Any],
    requests: list[dict[str, Any]],
    flash: str = '', flash_error: str = '',
    nav_html: str = '',
) -> str:
    """Render /cp/{user_token}/tasks — the client-side task inbox."""
    name = _esc(client.get("client_name") or client.get("client_code") or "")
    flash_html = ""
    if flash:
        flash_html += (
            f'<div style="background:#d4edda;padding:8px;margin-bottom:10px;">'
            f'{_esc(flash)}</div>'
        )
    if flash_error:
        flash_html += (
            f'<div style="background:#f8d7da;padding:8px;margin-bottom:10px;">'
            f'{_esc(flash_error)}</div>'
        )

    today = _date.today().isoformat()
    rows_html = ""
    for r in requests:
        due = r.get('due_date') or ''
        overdue = bool(due and due < today)
        row_bg = '#fff3cd' if overdue else 'transparent'
        target = (
            ''  # team-wide
            if not r.get('target_portal_user_id')
            else '<span class="muted">(moi / me)</span>'
        )
        due_html = ''
        if due:
            tag = 'color:#b91c1c;font-weight:600;' if overdue else ''
            due_html = (
                f'<div style="{tag}">Échéance / Due: {_esc(due)}</div>'
            )
        rows_html += (
            f'<tr style="background:{row_bg};">'
            f'<td><strong>{_esc(r["title"])}</strong> {target}'
            f'<div class="muted">{_esc(r.get("description") or "")}</div>'
            f'{due_html}</td>'
            f'<td><form method="POST" '
            f'action="/cp/{_esc(user_token)}/tasks/{int(r["id"])}/complete" '
            'style="display:inline;">'
            '<button type="submit">'
            'Marquer complété / Mark complete</button></form></td>'
            '</tr>'
        )
    if not rows_html:
        table_html = (
            '<p class="muted"><em>Aucune demande en cours. / '
            'No open requests.</em></p>'
        )
    else:
        table_html = (
            '<table><thead><tr>'
            '<th>Demande / Request</th><th></th>'
            '</tr></thead>'
            f'<tbody>{rows_html}</tbody></table>'
        )

    back = '' if nav_html else (
        f'<p><a href="/cp/{_esc(user_token)}/upload">'
        '&larr; Retour / Back</a></p>'
    )
    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<title>Tâches / Tasks</title>'
        '<style>body{font-family:system-ui,Arial;max-width:900px;'
        'margin:2rem auto;padding:1rem;}'
        'table{width:100%;border-collapse:collapse;}'
        'th,td{border-bottom:1px solid #eee;padding:10px;text-align:left;'
        'vertical-align:top;font-size:14px;}'
        '.card{background:#f9fafb;border:1px solid #e5e7eb;padding:1rem;'
        'border-radius:6px;margin-bottom:1rem;}'
        '.muted{color:#6b7280;font-size:12px;}'
        '.tabs{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:1rem;}'
        '.tabs a{padding:8px 14px;background:#f3f4f6;color:#111827;'
        'border-radius:8px 8px 0 0;text-decoration:none;font-size:14px;}'
        '.tabs a.active{background:#2a8759;color:white;}'
        '</style></head><body>'
        f'{nav_html}{back}'
        f'<h1>{name} — Tâches / Tasks</h1>'
        f'{flash_html}'
        '<div class="card">'
        f'{table_html}</div>'
        '</body></html>'
    )


def render_cpa_requests_page(
    *, firm_code: str, client_code: str, client_name: str,
    requests: list[dict[str, Any]],
    portal_users: list[dict[str, Any]] | None = None,
    flash: str = '', flash_error: str = '',
) -> str:
    """Render /clients/{code}/requests (CPA side)."""
    flash_html = ""
    if flash:
        flash_html += (
            f'<div style="background:#d4edda;padding:8px;margin-bottom:10px;">'
            f'{_esc(flash)}</div>'
        )
    if flash_error:
        flash_html += (
            f'<div style="background:#f8d7da;padding:8px;margin-bottom:10px;">'
            f'{_esc(flash_error)}</div>'
        )

    today = _date.today().isoformat()
    rows_html = ''
    for r in requests:
        due = r.get('due_date') or ''
        overdue = bool(due and due < today and r.get('status') == 'open')
        status = r.get('status') or 'open'
        badge_color = {
            'open': '#2563eb',
            'completed': '#166534',
            'cancelled': '#6b7280',
        }.get(status, '#6b7280')
        if overdue:
            badge_color = '#b91c1c'
            status_label = f'overdue ({due})'
        else:
            status_label = status
        target_label = ''
        if r.get('target_portal_user_id'):
            target_label = f'#{r["target_portal_user_id"]}'
        rows_html += (
            '<tr>'
            f'<td>{_esc(r.get("title"))}</td>'
            f'<td>{_esc(r.get("due_date") or "")}</td>'
            f'<td>{_esc(target_label)}</td>'
            f'<td><span style="color:{badge_color};font-weight:600;">'
            f'{_esc(status_label)}</span></td>'
            f'<td class="muted">{_esc(r.get("description") or "")}</td>'
            '</tr>'
        )
    if not rows_html:
        table_html = '<p class="muted"><em>No requests yet.</em></p>'
    else:
        table_html = (
            '<table><thead><tr>'
            '<th>Title</th><th>Due</th>'
            '<th>Target</th><th>Status</th><th>Description</th>'
            '</tr></thead>'
            f'<tbody>{rows_html}</tbody></table>'
        )

    options = '<option value="">(team-wide)</option>'
    for u in (portal_users or []):
        uid = int(u['id'])
        label = (
            f'{_esc(u.get("full_name") or u.get("email"))}'
        )
        options += f'<option value="{uid}">{label}</option>'

    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        f'<title>Requests — {_esc(client_name)}</title>'
        '<style>body{font-family:system-ui,Arial;max-width:1000px;'
        'margin:2rem auto;padding:1rem;}'
        'table{width:100%;border-collapse:collapse;margin:1rem 0;}'
        'th,td{border-bottom:1px solid #eee;padding:8px;text-align:left;'
        'vertical-align:top;font-size:14px;}'
        '.card{background:#f9fafb;border:1px solid #e5e7eb;padding:1rem;'
        'border-radius:6px;margin-bottom:1rem;}'
        '.muted{color:#6b7280;font-size:12px;}'
        '</style></head><body>'
        f'<h1>{_esc(client_name)} — Requests</h1>'
        f'{flash_html}'
        '<div class="card"><h2>New request</h2>'
        '<form method="POST" action="/clients/requests" '
        'style="display:grid;gap:6px;grid-template-columns:2fr 1fr 1fr auto;">'
        f'<input type="hidden" name="client_code" value="{_esc(client_code)}">'
        '<input type="text" name="title" placeholder="Title" required>'
        '<input type="date" name="due_date">'
        f'<select name="target_portal_user_id">{options}</select>'
        '<button type="submit">Create</button>'
        '<textarea name="description" placeholder="Description (optional)" '
        'rows="2" style="grid-column:1/-1;"></textarea>'
        '</form></div>'
        '<div class="card">'
        f'{table_html}</div>'
        '</body></html>'
    )
