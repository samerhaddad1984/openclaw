"""Owner-scoped read-only impersonation.

An owner can impersonate any firm to see exactly what their firm_admin
/ manager / employee would see. Every write/mutation path must check
``is_impersonating(ctx)`` and refuse when True — this module is the only
authority on that flag. Exiting impersonation restores the original
session; no user data is destroyed.

State lives in a dedicated ``impersonation_sessions`` table (not in the
HTTP cookie) so the audit trail is durable and an owner can't forge a
firm scope by editing cookies. The cookie just names the active
impersonation id; all firm-scoping happens server-side.
"""
from __future__ import annotations

import secrets
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def ensure_impersonation_schema(db_path: Path | str) -> None:
    with _open(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS impersonation_sessions (
                session_id TEXT PRIMARY KEY,
                original_user_email TEXT NOT NULL,
                impersonated_firm_code TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL DEFAULT 'active'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS impersonation_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                original_user_email TEXT NOT NULL,
                impersonated_firm_code TEXT NOT NULL,
                action TEXT NOT NULL,
                path TEXT,
                method TEXT,
                blocked INTEGER DEFAULT 0,
                at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_impersonation_audit_session "
            "ON impersonation_audit(session_id, at)"
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Start / stop
# ---------------------------------------------------------------------------


def start(
    db_path: Path | str, *,
    original_user_email: str, firm_code: str,
) -> str:
    """Open an impersonation row; returns the session_id cookie value."""
    ensure_impersonation_schema(db_path)
    sid = secrets.token_hex(16)
    with _open(db_path) as conn:
        conn.execute(
            "INSERT INTO impersonation_sessions "
            "(session_id, original_user_email, impersonated_firm_code, "
            " started_at, status) VALUES (?,?,?,?, 'active')",
            (sid, original_user_email, firm_code, _iso_now()),
        )
        conn.execute(
            "INSERT INTO impersonation_audit "
            "(session_id, original_user_email, impersonated_firm_code, "
            " action, blocked) VALUES (?,?,?,?,?)",
            (sid, original_user_email, firm_code, 'start', 0),
        )
        conn.commit()
    return sid


def stop(db_path: Path | str, *, session_id: str) -> None:
    if not session_id:
        return
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT original_user_email, impersonated_firm_code "
            "FROM impersonation_sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
        conn.execute(
            "UPDATE impersonation_sessions SET ended_at=?, status='ended' "
            "WHERE session_id=?",
            (_iso_now(), session_id),
        )
        if row:
            conn.execute(
                "INSERT INTO impersonation_audit "
                "(session_id, original_user_email, impersonated_firm_code, "
                " action, blocked) VALUES (?,?,?,?,?)",
                (session_id, row['original_user_email'],
                 row['impersonated_firm_code'], 'stop', 0),
            )
        conn.commit()


# ---------------------------------------------------------------------------
# State lookup
# ---------------------------------------------------------------------------


def active_session(
    db_path: Path | str, *, session_id: str,
) -> dict[str, Any] | None:
    if not session_id:
        return None
    ensure_impersonation_schema(db_path)
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM impersonation_sessions "
            "WHERE session_id=? AND status='active'",
            (session_id,),
        ).fetchone()
    return dict(row) if row else None


def log_action(
    db_path: Path | str, *, session_id: str,
    original_user_email: str, firm_code: str,
    action: str, path: str = '', method: str = '',
    blocked: bool = False,
) -> None:
    ensure_impersonation_schema(db_path)
    with _open(db_path) as conn:
        conn.execute(
            "INSERT INTO impersonation_audit "
            "(session_id, original_user_email, impersonated_firm_code, "
            " action, path, method, blocked) VALUES (?,?,?,?,?,?,?)",
            (session_id, original_user_email, firm_code, action, path,
             method, 1 if blocked else 0),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------


def render_banner(firm_code: str, firm_name: str | None = None) -> str:
    """Top-of-page read-only banner shown on every page while impersonating."""
    import html as _html
    name = _html.escape(firm_name or firm_code or 'unknown')
    code = _html.escape(firm_code or '')
    return (
        '<div style="background:#fff3cd;border:2px solid #856404;'
        'padding:10px 14px;margin-bottom:12px;border-radius:6px;'
        'display:flex;justify-content:space-between;align-items:center;'
        'font-family:system-ui,Arial,sans-serif;">'
        '<div style="font-size:14px;color:#856404;">'
        f'&#9888;&#65039; <strong>IMPERSONATING {name}</strong> '
        f'({code}) &mdash; read-only. All writes are blocked.'
        '</div>'
        '<form method="POST" action="/owner/impersonate/stop" '
        'style="margin:0;">'
        '<button type="submit" '
        'style="background:#856404;color:white;border:none;padding:6px 12px;'
        'border-radius:4px;cursor:pointer;font-weight:bold;">'
        'Stop impersonating</button></form></div>'
    )


def forbidden_write_response_html() -> str:
    return (
        '<div class="card" style="margin:1rem;">'
        '<h2>Action blocked</h2>'
        '<p>Writes are disabled while impersonating a firm. '
        '<a href="/owner/dashboard">Exit impersonation</a> and try again '
        'as your owner account.</p></div>'
    )
