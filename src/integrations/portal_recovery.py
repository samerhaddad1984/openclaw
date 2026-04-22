"""src/integrations/portal_recovery.py — self-service portal link recovery.

Two flows:

1. ``rotate_my_token(user_id)`` — a logged-in portal user rotates their
   own token. Wraps ``multi_user_portal.rotate_user_token`` with an
   additional *notify* step so the user gets the new URL by email.

2. ``request_recovery(email, firm_code, client_code)`` — *public*
   endpoint for users who lost their link. Looks up the active user
   by email under the named firm+client, rate-limits to 1/hour per
   email, writes an audit row, and enqueues an email with the existing
   portal URL. Contributors' recovery requests additionally notify
   every active admin under the same client.

The rate-limit state is stored in a ``portal_recovery_throttle`` table
(not in memory) so the limit survives dashboard restarts.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.integrations import multi_user_portal as _mup

log = logging.getLogger(__name__)

RECOVERY_THROTTLE_SECONDS = 3600  # 1 per hour per email


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portal_recovery_throttle (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            firm_code TEXT,
            client_code TEXT,
            requested_at TEXT NOT NULL,
            ip TEXT
        )
        """,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_recov_email_time "
        "ON portal_recovery_throttle(email, requested_at)",
    )
    conn.commit()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# 1. Rotate my own token
# ---------------------------------------------------------------------------


def rotate_my_token(
    db_path: Path | str, *,
    user_id: int,
    base_url: str = "",
    notify: bool = True,
) -> dict[str, Any]:
    """Rotate a portal user's token and optionally enqueue a notification.

    Returns ``{"new_token": ..., "user": {...}}``. Raises ``LookupError``
    if the user doesn't exist.
    """
    user = _mup.get_user(db_path, user_id=user_id)
    if user is None:
        raise LookupError(f"user_id={user_id} not found")
    new_token = _mup.rotate_user_token(
        db_path,
        firm_code=user['firm_code'],
        client_code=user['client_code'],
        user_id=user_id,
        actor_email=user['email'],
    )
    if notify and user.get('email'):
        try:
            from src.integrations import notification_sender as _ns
            link = f"{base_url.rstrip('/')}/cp/{new_token}/upload" if base_url \
                else f"/cp/{new_token}/upload"
            _ns.enqueue(
                db_path,
                client_code=user['client_code'],
                kind='portal_recovery',
                title="Votre nouveau lien d'accès / Your new access link",
                body=(
                    f"Bonjour {user.get('full_name') or ''},\n\n"
                    f"Votre lien d'accès au portail a été renouvelé. "
                    f"Utilisez le nouveau lien ci-dessous.\n\n"
                    f"Hello {user.get('full_name') or ''},\n\n"
                    f"Your portal access link was rotated. Use the "
                    f"new link below.\n\n"
                    f"{link}\n"
                ),
                recipient_email=user['email'],
                subject="Votre lien d'accès / Your access link",
                priority=3,
            )
        except Exception:
            log.exception("rotate_my_token notification enqueue failed")
    return {"new_token": new_token, "user": user}


# ---------------------------------------------------------------------------
# 2. Public "forgot my link" recovery
# ---------------------------------------------------------------------------


def _too_recent(conn: sqlite3.Connection, email: str) -> bool:
    cutoff = _now() - timedelta(seconds=RECOVERY_THROTTLE_SECONDS)
    row = conn.execute(
        "SELECT 1 FROM portal_recovery_throttle "
        "WHERE LOWER(email) = LOWER(?) AND requested_at > ? "
        "LIMIT 1",
        (email, _iso(cutoff)),
    ).fetchone()
    return row is not None


def _record_attempt(conn: sqlite3.Connection, *, email: str,
                     firm_code: str, client_code: str, ip: str) -> None:
    conn.execute(
        "INSERT INTO portal_recovery_throttle "
        "(email, firm_code, client_code, requested_at, ip) "
        "VALUES (?, ?, ?, ?, ?)",
        (email, firm_code, client_code, _iso(_now()), ip or ''),
    )
    conn.commit()


def request_recovery(
    db_path: Path | str, *,
    email: str, firm_code: str, client_code: str,
    base_url: str = "",
    ip: str = "",
) -> dict[str, Any]:
    """Process a public "forgot my link" request.

    Returns ``{"ok": bool, "reason": str}``. ``ok=False`` + reason is
    *always* the same short string regardless of whether the user exists,
    so the public endpoint can't be used as an email-enumeration oracle.
    The caller should surface a generic "If an account exists, you'll
    receive an email" message.
    """
    email = (email or '').strip().lower()
    if not email or '@' not in email:
        return {"ok": False, "reason": "invalid_email"}

    with _open(db_path) as conn:
        _ensure_schema(conn)
        if _too_recent(conn, email):
            # Still record the attempt (for abuse detection) but don't
            # enqueue a second email within the window.
            _record_attempt(
                conn, email=email, firm_code=firm_code,
                client_code=client_code, ip=ip,
            )
            return {"ok": False, "reason": "rate_limited"}

        # Look up the user. We ALWAYS record an attempt (so repeated
        # guesses get rate-limited even when the account doesn't exist).
        user = _mup.get_user(
            db_path, firm_code=firm_code, client_code=client_code, email=email,
        )
        _record_attempt(
            conn, email=email, firm_code=firm_code,
            client_code=client_code, ip=ip,
        )

    if user is None or user.get('status') != 'active':
        # Don't leak existence — return the same shape the caller sees
        # on success so the HTTP response is identical.
        return {"ok": True, "reason": "processed"}

    # Enqueue the recovery email. The user keeps their current token —
    # we just re-send the URL. If the CPA wants to force a rotation,
    # they can do it from the admin portal.
    try:
        from src.integrations import notification_sender as _ns
        link = (
            f"{base_url.rstrip('/')}/cp/{user['user_token']}/upload"
            if base_url else f"/cp/{user['user_token']}/upload"
        )
        _ns.enqueue(
            db_path,
            client_code=client_code,
            kind='portal_recovery_link',
            title="Votre lien d'accès / Your access link",
            body=(
                f"Bonjour {user.get('full_name') or ''},\n\n"
                "Vous avez demandé votre lien d'accès au portail. "
                "Cliquez sur le lien ci-dessous.\n\n"
                f"Hello {user.get('full_name') or ''},\n\n"
                "You requested your portal access link. Click the link "
                "below.\n\n"
                f"{link}\n"
            ),
            recipient_email=user['email'],
            subject="Votre lien d'accès / Your access link",
            priority=3,
        )
    except Exception:
        log.exception("recovery email enqueue failed")

    # Audit.
    try:
        with _open(db_path) as conn2:
            _mup._audit(
                conn2,
                firm_code=firm_code, client_code=client_code,
                actor_email=email, action='recovery_requested',
                portal_user_id=user['id'], detail=f'ip={ip}',
            )
            conn2.commit()
    except Exception:
        log.exception("recovery audit failed")

    # Notify admins when a contributor (non-admin) requests recovery.
    if user.get('role') != 'admin':
        try:
            admins = [
                u for u in _mup.list_users(
                    db_path, firm_code=firm_code, client_code=client_code,
                )
                if u.get('role') == 'admin' and u.get('status') == 'active'
            ]
            from src.integrations import notification_sender as _ns
            for adm in admins:
                _ns.enqueue(
                    db_path,
                    client_code=client_code,
                    kind='portal_recovery_admin_notify',
                    title=(
                        f"{user['email']} a demandé une récupération de lien / "
                        f"requested link recovery"
                    ),
                    body=(
                        f"Pour votre information : {user['email']} a demandé "
                        "son lien d'accès au portail.\n\n"
                        f"FYI: {user['email']} requested their portal access "
                        "link.\n"
                    ),
                    recipient_email=adm['email'],
                    priority=5,
                )
        except Exception:
            log.exception("admin notify on recovery failed")

    return {"ok": True, "reason": "processed"}
