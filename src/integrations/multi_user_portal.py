"""Multi-user client portal helpers.

State
-----
- `clients.portal_mode` selects `'single'` (default, anonymous QR link)
  or `'multi'` (each human gets a personal invite-issued token).
- `client_portal_users` holds active / invited / suspended / removed
  user rows per (firm_code, client_code, email).
- `client_portal_invitations` holds pending invite tokens; once
  accepted, a users row is created and the invitation row flips to
  ``status='accepted'``.

Token resolution
----------------
`resolve_portal_access(token)` checks both namespaces:

1. `clients.portal_token` (legacy single-link path). If found and
   the client is in single mode, returns ``('single', client, None)``;
   if in multi mode, returns ``('multi_redirect', client, None)`` so
   the caller can render "use your personal link".
2. `client_portal_users.user_token` (personal path). If found and the
   user is `'active'`, returns ``('multi', client, portal_user)``.

Anything else returns ``(None, None, None)`` — the caller renders the
same invalid-link page used for legacy portals.
"""
from __future__ import annotations

import logging
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _iso_in(days: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)
            ).replace(microsecond=0).isoformat()


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _rowdict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


# ---------------------------------------------------------------------------
# Mode toggle
# ---------------------------------------------------------------------------


VALID_MODES = ('single', 'multi')
VALID_ROLES = ('admin', 'contributor')
VALID_STATUSES = ('invited', 'active', 'suspended', 'removed')


def set_portal_mode(
    db_path: Path | str, *, firm_code: str, client_code: str,
    mode: str, actor_email: str,
) -> dict[str, Any]:
    if mode not in VALID_MODES:
        raise ValueError(f"invalid mode {mode!r}")
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT firm_code FROM clients WHERE client_code=?",
            (client_code,),
        ).fetchone()
        if not row:
            raise LookupError(f"unknown client_code {client_code!r}")
        if row['firm_code'] != firm_code:
            raise PermissionError(
                f"client_code {client_code!r} is not in firm {firm_code!r}"
            )
        conn.execute(
            "UPDATE clients SET portal_mode=? WHERE client_code=?",
            (mode, client_code),
        )
        conn.execute(
            "INSERT INTO client_portal_user_audit "
            "(firm_code, client_code, actor_email, action, detail) "
            "VALUES (?,?,?,?,?)",
            (firm_code, client_code, actor_email, 'portal_mode_changed',
             f'mode={mode}'),
        )
        conn.commit()
    return {'ok': True, 'mode': mode}


def get_client(
    db_path: Path | str, *, firm_code: str | None = None, client_code: str,
) -> dict[str, Any] | None:
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM clients WHERE client_code=?", (client_code,),
        ).fetchone()
    c = _rowdict(row)
    if c is None:
        return None
    if firm_code is not None and c.get('firm_code') != firm_code:
        return None
    return c


# ---------------------------------------------------------------------------
# Token resolution
# ---------------------------------------------------------------------------


def _find_client_by_token(
    conn: sqlite3.Connection, token: str,
) -> dict[str, Any] | None:
    if not token or len(token) < 30:
        return None
    row = conn.execute(
        "SELECT * FROM clients WHERE portal_token=? "
        "AND COALESCE(active,1)=1",
        (token,),
    ).fetchone()
    return _rowdict(row)


def _find_user_by_token(
    conn: sqlite3.Connection, token: str,
) -> dict[str, Any] | None:
    if not token or len(token) < 30:
        return None
    row = conn.execute(
        "SELECT * FROM client_portal_users WHERE user_token=?",
        (token,),
    ).fetchone()
    return _rowdict(row)


def resolve_portal_access(
    db_path: Path | str, *, token: str,
) -> tuple[str | None, dict[str, Any] | None, dict[str, Any] | None]:
    """Return (mode, client, portal_user) where mode is one of:

    - 'single'          — client is in single mode and token is its portal_token
    - 'multi'           — token is a valid active portal_user token
    - 'multi_redirect'  — client is in multi mode but caller used the client
                          portal_token; we ask them to use their personal link
    - None              — token didn't resolve to anything usable

    `portal_user` is None except on 'multi'.
    """
    with _open(db_path) as conn:
        client = _find_client_by_token(conn, token)
        if client is not None:
            mode = (client.get('portal_mode') or 'single').lower()
            if mode == 'multi':
                return ('multi_redirect', client, None)
            return ('single', client, None)
        user = _find_user_by_token(conn, token)
        if user is None:
            return (None, None, None)
        if (user.get('status') or 'invited') != 'active':
            # invited/suspended/removed all reject at the door.
            return (None, None, None)
        client = _rowdict(conn.execute(
            "SELECT * FROM clients WHERE client_code=? AND firm_code=?",
            (user['client_code'], user['firm_code']),
        ).fetchone())
        if not client:
            return (None, None, None)
        return ('multi', client, user)


# ---------------------------------------------------------------------------
# Users CRUD
# ---------------------------------------------------------------------------


def _new_user_token() -> str:
    return secrets.token_urlsafe(32)


def list_users(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
    include_removed: bool = False,
) -> list[dict[str, Any]]:
    with _open(db_path) as conn:
        if include_removed:
            rows = conn.execute(
                "SELECT * FROM client_portal_users "
                "WHERE firm_code=? AND client_code=? "
                "ORDER BY role DESC, email",
                (firm_code, client_code),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM client_portal_users "
                "WHERE firm_code=? AND client_code=? "
                "AND COALESCE(status,'invited') != 'removed' "
                "ORDER BY role DESC, email",
                (firm_code, client_code),
            ).fetchall()
    return [dict(r) for r in rows]


def get_user(
    db_path: Path | str, *, user_id: int | None = None,
    user_token: str | None = None,
    firm_code: str | None = None, client_code: str | None = None,
    email: str | None = None,
) -> dict[str, Any] | None:
    with _open(db_path) as conn:
        if user_id is not None:
            r = conn.execute(
                "SELECT * FROM client_portal_users WHERE id=?",
                (user_id,),
            ).fetchone()
        elif user_token is not None:
            r = conn.execute(
                "SELECT * FROM client_portal_users WHERE user_token=?",
                (user_token,),
            ).fetchone()
        elif email is not None and firm_code and client_code:
            r = conn.execute(
                "SELECT * FROM client_portal_users "
                "WHERE firm_code=? AND client_code=? AND LOWER(email)=LOWER(?)",
                (firm_code, client_code, email),
            ).fetchone()
        else:
            return None
    return _rowdict(r)


def _audit(
    conn: sqlite3.Connection, *,
    firm_code: str, client_code: str, actor_email: str,
    action: str, portal_user_id: int | None = None,
    detail: str = '', ip: str = '', user_agent: str = '',
) -> None:
    conn.execute(
        "INSERT INTO client_portal_user_audit "
        "(portal_user_id, firm_code, client_code, actor_email, action, "
        " detail, ip, user_agent) VALUES (?,?,?,?,?,?,?,?)",
        (portal_user_id, firm_code, client_code, actor_email, action,
         detail, ip, user_agent),
    )


def create_user_direct(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
    email: str, full_name: str, role: str,
    invited_by: str, status: str = 'active',
) -> dict[str, Any]:
    """Insert a user row without going through the invitation flow.

    Used by:
    - CPA switching a client to multi-mode (promotes the current
      single-portal contact to admin),
    - Acceptance of an invitation (the invitation row writes directly
      here with status='active').

    The ``email`` uniqueness is enforced by the (firm_code, client_code,
    email) UNIQUE constraint.
    """
    if role not in VALID_ROLES:
        raise ValueError(f"invalid role {role!r}")
    if status not in VALID_STATUSES:
        raise ValueError(f"invalid status {status!r}")
    token = _new_user_token()
    now = _iso_now()
    with _open(db_path) as conn:
        existing = conn.execute(
            "SELECT id, status FROM client_portal_users "
            "WHERE firm_code=? AND client_code=? AND LOWER(email)=LOWER(?)",
            (firm_code, client_code, email),
        ).fetchone()
        if existing and existing['status'] != 'removed':
            # Re-activate a suspended user rather than erroring
            conn.execute(
                "UPDATE client_portal_users SET status=?, user_token=?, "
                "accepted_at=COALESCE(accepted_at, ?) WHERE id=?",
                (status, token, now, existing['id']),
            )
            _audit(conn, firm_code=firm_code, client_code=client_code,
                    actor_email=invited_by, action='user_reactivated',
                    portal_user_id=existing['id'],
                    detail=f'role={role} status={status}')
            uid = existing['id']
        elif existing and existing['status'] == 'removed':
            # Reuse the row but issue a fresh token.
            conn.execute(
                "UPDATE client_portal_users SET user_token=?, status=?, "
                "role=?, full_name=?, invited_by=?, invited_at=?, "
                "accepted_at=?, removed_at=NULL WHERE id=?",
                (token, status, role, full_name, invited_by, now, now,
                 existing['id']),
            )
            _audit(conn, firm_code=firm_code, client_code=client_code,
                    actor_email=invited_by, action='user_readded',
                    portal_user_id=existing['id'],
                    detail=f'role={role}')
            uid = existing['id']
        else:
            cur = conn.execute(
                "INSERT INTO client_portal_users "
                "(firm_code, client_code, email, full_name, role, "
                " user_token, status, invited_by, invited_at, "
                " accepted_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (firm_code, client_code, email, full_name, role, token,
                 status, invited_by, now,
                 now if status == 'active' else None),
            )
            uid = int(cur.lastrowid)
            _audit(conn, firm_code=firm_code, client_code=client_code,
                    actor_email=invited_by, action='user_created',
                    portal_user_id=uid,
                    detail=f'role={role} status={status}')
        conn.commit()
    return get_user(db_path, user_id=uid) or {}


def set_user_status(
    db_path: Path | str, *,
    firm_code: str, client_code: str, user_id: int,
    status: str, actor_email: str,
) -> dict[str, Any]:
    if status not in VALID_STATUSES:
        raise ValueError(f"invalid status {status!r}")
    now = _iso_now()
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM client_portal_users WHERE id=? "
            "AND firm_code=? AND client_code=?",
            (user_id, firm_code, client_code),
        ).fetchone()
        if not row:
            raise LookupError("user not found in this firm/client scope")
        extra = ''
        if status == 'suspended':
            extra = ', suspended_at=?'
            args = (status, now, user_id)
        elif status == 'removed':
            # Invalidate the token so a stashed cookie can't resume.
            conn.execute(
                "UPDATE client_portal_users SET user_token=?, removed_at=? "
                "WHERE id=?",
                (f'__removed_{secrets.token_hex(8)}_' + _new_user_token(),
                 now, user_id),
            )
            args = (status, user_id)
            extra = ''
        else:
            args = (status, user_id)
        conn.execute(
            f"UPDATE client_portal_users SET status=?{extra} WHERE id=?",
            args,
        )
        _audit(conn, firm_code=firm_code, client_code=client_code,
                actor_email=actor_email,
                action=f'user_status_{status}', portal_user_id=user_id,
                detail='')
        conn.commit()
    return get_user(db_path, user_id=user_id) or {}


def set_user_role(
    db_path: Path | str, *,
    firm_code: str, client_code: str, user_id: int,
    role: str, actor_email: str,
) -> dict[str, Any]:
    if role not in VALID_ROLES:
        raise ValueError(f"invalid role {role!r}")
    # Self-demote guard: prevent the last admin from demoting themselves.
    actor_row = get_user(db_path, firm_code=firm_code,
                           client_code=client_code, email=actor_email)
    target = get_user(db_path, user_id=user_id)
    if target is None:
        raise LookupError("user not found")
    if (actor_row and actor_row.get('id') == user_id and role != 'admin'):
        admins = [u for u in list_users(db_path, firm_code=firm_code,
                                           client_code=client_code)
                  if u.get('role') == 'admin'
                  and u.get('status') == 'active']
        if len(admins) <= 1:
            raise PermissionError(
                "cannot demote yourself when you are the only admin"
            )
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE client_portal_users SET role=? WHERE id=?",
            (role, user_id),
        )
        _audit(conn, firm_code=firm_code, client_code=client_code,
                actor_email=actor_email, action='user_role_changed',
                portal_user_id=user_id, detail=f'role={role}')
        conn.commit()
    return get_user(db_path, user_id=user_id) or {}


def rotate_user_token(
    db_path: Path | str, *, firm_code: str, client_code: str,
    user_id: int, actor_email: str,
) -> str:
    token = _new_user_token()
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE client_portal_users SET user_token=? WHERE id=? "
            "AND firm_code=? AND client_code=?",
            (token, user_id, firm_code, client_code),
        )
        _audit(conn, firm_code=firm_code, client_code=client_code,
                actor_email=actor_email, action='user_token_rotated',
                portal_user_id=user_id, detail='')
        conn.commit()
    return token


def increment_upload_count(
    db_path: Path | str, *, user_id: int, n: int = 1,
) -> None:
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE client_portal_users SET upload_count=upload_count+?, "
            "last_active_at=? WHERE id=?",
            (n, _iso_now(), user_id),
        )
        conn.commit()


def mark_active(
    db_path: Path | str, *, user_id: int,
) -> None:
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE client_portal_users SET last_active_at=? WHERE id=?",
            (_iso_now(), user_id),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Invitation lifecycle
# ---------------------------------------------------------------------------


INVITE_EXPIRY_DAYS = 14


def _new_invite_token() -> str:
    return secrets.token_urlsafe(32)


def create_invitation(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
    email: str, full_name: str, role: str,
    invited_by: str,
) -> dict[str, Any]:
    """Create or replace a pending invitation for (client, email).

    Role must be 'admin' or 'contributor'. If an older pending
    invitation exists for the same email it gets superseded (status
    flips to 'cancelled') so resending doesn't orphan invite tokens.
    """
    if role not in VALID_ROLES:
        raise ValueError(f"invalid role {role!r}")
    if not email or '@' not in email:
        raise ValueError(f"invalid email {email!r}")
    token = _new_invite_token()
    now = _iso_now()
    expires = _iso_in(INVITE_EXPIRY_DAYS)
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE client_portal_invitations SET status='cancelled' "
            "WHERE firm_code=? AND client_code=? "
            "AND LOWER(email)=LOWER(?) AND status='pending'",
            (firm_code, client_code, email),
        )
        cur = conn.execute(
            "INSERT INTO client_portal_invitations "
            "(firm_code, client_code, email, full_name, invited_role, "
            " invitation_token, invited_by, invited_at, expires_at, status) "
            "VALUES (?,?,?,?,?,?,?,?,?, 'pending')",
            (firm_code, client_code, email, full_name, role, token,
             invited_by, now, expires),
        )
        inv_id = int(cur.lastrowid)
        _audit(conn, firm_code=firm_code, client_code=client_code,
                actor_email=invited_by, action='invitation_created',
                detail=f'email={email} role={role}')
        conn.commit()
    return {'id': inv_id, 'token': token, 'email': email,
             'role': role, 'expires_at': expires}


def get_invitation(
    db_path: Path | str, *, token: str,
) -> dict[str, Any] | None:
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM client_portal_invitations "
            "WHERE invitation_token=?", (token,),
        ).fetchone()
    return _rowdict(row)


def list_invitations(
    db_path: Path | str, *, firm_code: str, client_code: str,
    status: str | None = 'pending',
) -> list[dict[str, Any]]:
    with _open(db_path) as conn:
        if status is not None:
            rows = conn.execute(
                "SELECT * FROM client_portal_invitations "
                "WHERE firm_code=? AND client_code=? AND status=? "
                "ORDER BY invited_at DESC",
                (firm_code, client_code, status),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM client_portal_invitations "
                "WHERE firm_code=? AND client_code=? "
                "ORDER BY invited_at DESC",
                (firm_code, client_code),
            ).fetchall()
    return [dict(r) for r in rows]


def _invite_expired(inv: dict[str, Any]) -> bool:
    exp = inv.get('expires_at') or ''
    return bool(exp and exp < _iso_now())


def accept_invitation(
    db_path: Path | str, *, token: str,
) -> dict[str, Any]:
    inv = get_invitation(db_path, token=token)
    if inv is None:
        return {'ok': False, 'error': 'invalid_token'}
    if inv['status'] == 'accepted':
        return {'ok': False, 'error': 'already_accepted'}
    if inv['status'] == 'cancelled':
        return {'ok': False, 'error': 'cancelled'}
    if _invite_expired(inv):
        with _open(db_path) as conn:
            conn.execute(
                "UPDATE client_portal_invitations SET status='expired' "
                "WHERE id=?", (inv['id'],),
            )
            conn.commit()
        return {'ok': False, 'error': 'expired'}
    user = create_user_direct(
        db_path, firm_code=inv['firm_code'], client_code=inv['client_code'],
        email=inv['email'], full_name=inv['full_name'] or inv['email'],
        role=inv['invited_role'], invited_by=inv['invited_by'] or 'cpa',
        status='active',
    )
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE client_portal_invitations SET status='accepted', "
            "accepted_at=? WHERE id=?",
            (_iso_now(), inv['id']),
        )
        _audit(conn, firm_code=inv['firm_code'],
                client_code=inv['client_code'],
                actor_email=inv['email'], action='invitation_accepted',
                portal_user_id=user.get('id'),
                detail=f'invitation_id={inv["id"]}')
        conn.commit()
    return {'ok': True, 'user': user, 'invitation': inv}


def cancel_invitation(
    db_path: Path | str, *, invitation_id: int, actor_email: str,
) -> None:
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT firm_code, client_code FROM client_portal_invitations "
            "WHERE id=?", (invitation_id,),
        ).fetchone()
        if not row:
            return
        conn.execute(
            "UPDATE client_portal_invitations SET status='cancelled' "
            "WHERE id=?", (invitation_id,),
        )
        _audit(conn, firm_code=row['firm_code'],
                client_code=row['client_code'],
                actor_email=actor_email, action='invitation_cancelled',
                detail=f'invitation_id={invitation_id}')
        conn.commit()


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------


def audit_log(
    db_path: Path | str, *, firm_code: str, client_code: str,
    actor_email: str, action: str, portal_user_id: int | None = None,
    detail: str = '', ip: str = '', user_agent: str = '',
) -> None:
    with _open(db_path) as conn:
        _audit(conn, firm_code=firm_code, client_code=client_code,
                actor_email=actor_email, action=action,
                portal_user_id=portal_user_id, detail=detail,
                ip=ip, user_agent=user_agent)
        conn.commit()


def recent_audit(
    db_path: Path | str, *, firm_code: str, client_code: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM client_portal_user_audit "
            "WHERE firm_code=? AND client_code=? "
            "ORDER BY created_at DESC, id DESC LIMIT ?",
            (firm_code, client_code, limit),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Rate-limit bookkeeping (Phase 7)
# ---------------------------------------------------------------------------


_PER_USER_UPLOADS_PER_MIN = 30


def _per_user_log_lock():
    # threading.local isn't safe across processes, but the dashboard is a
    # single ThreadingHTTPServer instance, so an in-memory log with a
    # lock is adequate. Persisted attempts would go to audit table.
    import threading
    if not hasattr(_per_user_log_lock, "_lock"):
        _per_user_log_lock._lock = threading.Lock()
    return _per_user_log_lock._lock


_per_user_uploads: dict[int, list[float]] = {}


def upload_rate_allowed(user_id: int) -> bool:
    """True when the user is under the 30-upload/min limit.

    Rate windows are 60 seconds; older entries are pruned on every
    call so the in-memory log doesn't grow unboundedly."""
    import time as _t
    now = _t.time()
    with _per_user_log_lock():
        log = _per_user_uploads.setdefault(user_id, [])
        cutoff = now - 60.0
        log[:] = [t for t in log if t >= cutoff]
        if len(log) >= _PER_USER_UPLOADS_PER_MIN:
            return False
        log.append(now)
    return True


def reset_rate_limits() -> None:
    """Test helper: clear the per-user log between tests."""
    with _per_user_log_lock():
        _per_user_uploads.clear()


# ---------------------------------------------------------------------------
# Suspicious-activity detection (Phase 7)
# ---------------------------------------------------------------------------


SUSPICIOUS_RAPID_UPLOADS = 40    # per minute
SUSPICIOUS_IP_COUNT = 3          # distinct IPs in a 1-hour window
SUSPICIOUS_FAILED_ATTEMPTS = 5   # failed accesses in 10 minutes


def log_access_attempt(
    db_path: Path | str, *, firm_code: str, client_code: str,
    portal_user_id: int | None, actor_email: str, action: str,
    ip: str = '', user_agent: str = '', detail: str = '',
) -> None:
    """Generic audit writer used by the dashboard for 'login' attempts
    (i.e. any access of a /cp/ route whether it succeeded or not)."""
    audit_log(
        db_path, firm_code=firm_code, client_code=client_code,
        actor_email=actor_email, action=action,
        portal_user_id=portal_user_id, detail=detail,
        ip=ip, user_agent=user_agent,
    )


def detect_suspicious_activity(
    db_path: Path | str, *, portal_user_id: int,
    window_hours: int = 1,
) -> list[dict[str, Any]]:
    """Scan the audit log for this user over the last `window_hours`.

    Emits one dict per signal; empty list when nothing unusual found.
    Runs in O(rows-in-window) — cheap enough for on-demand admin views."""
    alerts: list[dict[str, Any]] = []
    with _open(db_path) as conn:
        rows = conn.execute(
            "SELECT action, ip, created_at, detail FROM client_portal_user_audit "
            "WHERE portal_user_id=? "
            "AND datetime(created_at) >= datetime('now', ?)",
            (portal_user_id, f'-{window_hours} hours'),
        ).fetchall()
    rows = [dict(r) for r in rows]
    if not rows:
        return alerts

    # Distinct IPs
    ips = {r.get('ip') for r in rows if r.get('ip')}
    if len(ips) >= SUSPICIOUS_IP_COUNT:
        alerts.append({
            'kind': 'multi_ip',
            'detail': f'{len(ips)} distinct IPs in last {window_hours}h',
            'ips': sorted(ips),
        })
    # Rapid uploads (>40 / minute)
    import collections
    per_min = collections.Counter()
    for r in rows:
        if r.get('action') != 'upload':
            continue
        ts = (r.get('created_at') or '')[:16]  # YYYY-MM-DDTHH:MM
        per_min[ts] += 1
    for minute, count in per_min.items():
        # Each upload row records count=N in detail; sum that too.
        if count >= 5:
            alerts.append({
                'kind': 'rapid_uploads',
                'detail': (f'{count} upload events in minute {minute}'),
            })
    # Failed access attempts — already scoped to the window by the SQL.
    failed = [r for r in rows if r.get('action') == 'access_rejected']
    if len(failed) >= SUSPICIOUS_FAILED_ATTEMPTS:
        alerts.append({
            'kind': 'failed_access_burst',
            'detail': f'{len(failed)} rejected accesses in last 10 min',
        })
    return alerts


def suspicious_summary(
    db_path: Path | str, *, firm_code: str, client_code: str,
) -> list[dict[str, Any]]:
    """Scan every active user in the client and return any detected
    signals. Used by admin dashboard widget."""
    out = []
    for u in list_users(db_path, firm_code=firm_code,
                           client_code=client_code):
        alerts = detect_suspicious_activity(db_path, portal_user_id=u['id'])
        if alerts:
            out.append({'user_id': u['id'],
                         'email': u['email'], 'alerts': alerts})
    return out


# ---------------------------------------------------------------------------
# Render helpers (self-contained so gap_routes/dispatch can just emit HTML)
# ---------------------------------------------------------------------------


import html as _html


def _esc(v: Any) -> str:
    return _html.escape(str(v if v is not None else ''))


def render_use_personal_link(client: dict[str, Any]) -> str:
    name = _esc(client.get('client_name') or client.get('client_code') or '')
    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<title>Use your personal link</title>'
        '<style>body{font-family:system-ui,Arial;max-width:560px;'
        'margin:4rem auto;padding:1rem;text-align:center;color:#333;}</style>'
        '</head><body>'
        f'<h1>{name} uses personal links</h1>'
        '<p>This organisation switched to multi-user mode, so the shared '
        'QR / email link is no longer the right one.</p>'
        '<p>Ask your administrator to <strong>invite you</strong>; you will '
        'get your own email with a personal upload link.</p>'
        '<p>If you <em>are</em> the administrator, please use the admin '
        'link your CPA sent when multi-user mode was enabled.</p>'
        '</body></html>'
    )


def render_invalid_token() -> str:
    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<title>Invalid link</title>'
        '<style>body{font-family:system-ui,Arial;max-width:560px;'
        'margin:4rem auto;padding:1rem;text-align:center;color:#333;}</style>'
        '</head><body>'
        '<h1>This link is not valid</h1>'
        '<p>It may have been rotated, the invitation expired, or your '
        'account was suspended. Contact your administrator or your CPA.</p>'
        '</body></html>'
    )


def render_cpa_portal_users(
    *, client: dict[str, Any],
    users: list[dict[str, Any]],
    invitations: list[dict[str, Any]] | None = None,
    flash: str = '', flash_error: str = '',
) -> str:
    """CPA-side view of the client's portal users. Read + mutate."""
    code = _esc(client.get('client_code') or '')
    name = _esc(client.get('client_name') or code)
    mode = _esc(client.get('portal_mode') or 'single')
    flash_html = ''
    if flash:
        flash_html = (
            f'<div class="flash success">{_esc(flash)}</div>'
        )
    if flash_error:
        flash_html += (
            f'<div class="flash error">{_esc(flash_error)}</div>'
        )

    rows = ''
    for u in users:
        role = _esc(u.get('role') or '')
        status = _esc(u.get('status') or '')
        actions = ''
        if status != 'removed':
            actions = (
                f'<form method="POST" action="/clients/portal_users/remove" '
                'style="display:inline;" '
                'onsubmit="return confirm(\'Force-remove this user? Their uploads are preserved but token stops working.\');">'
                f'<input type="hidden" name="client_code" value="{code}">'
                f'<input type="hidden" name="user_id" value="{u["id"]}">'
                '<button type="submit" '
                'style="background:#dc2626;color:white;">Force remove</button>'
                '</form>'
            )
        rows += (
            f'<tr><td>{_esc(u.get("full_name") or "")}</td>'
            f'<td>{_esc(u.get("email") or "")}</td>'
            f'<td>{role}</td><td>{status}</td>'
            f'<td>{int(u.get("upload_count") or 0)}</td>'
            f'<td>{_esc(u.get("last_active_at") or "never")}</td>'
            f'<td>{actions}</td></tr>'
        )

    inv_rows = ''
    for inv in (invitations or []):
        inv_rows += (
            f'<tr><td>{_esc(inv.get("email") or "")}</td>'
            f'<td>{_esc(inv.get("invited_role") or "")}</td>'
            f'<td>{_esc(inv.get("status") or "")}</td>'
            f'<td>{_esc(inv.get("invited_by") or "")}</td>'
            f'<td>{_esc(inv.get("expires_at") or "")}</td></tr>'
        )

    mode_switch = (
        '<form method="POST" action="/clients/portal_mode" '
        'style="display:inline;">'
        f'<input type="hidden" name="client_code" value="{code}">'
        f'<input type="hidden" name="mode" value="'
        f'{"single" if mode == "multi" else "multi"}">'
        '<button type="submit">'
        f'Switch to {"single" if mode == "multi" else "multi"}-user mode'
        '</button></form>'
    )

    return (
        '<div class="card" style="max-width:900px;margin:1rem auto;">'
        f'<h2>{name} — portal users</h2>'
        f'<p>Current mode: <strong>{mode}</strong> {mode_switch}</p>'
        f'{flash_html}'
        '<h3>Users</h3>'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr><th>Name</th><th>Email</th><th>Role</th>'
        '<th>Status</th><th>Uploads</th><th>Last active</th>'
        '<th>CPA override</th></tr></thead>'
        f'<tbody>{rows or "<tr><td colspan=7>No users yet.</td></tr>"}</tbody>'
        '</table>'
        '<h3>Pending invitations</h3>'
        '<table style="width:100%;border-collapse:collapse;">'
        '<thead><tr><th>Email</th><th>Role</th><th>Status</th>'
        '<th>Invited by</th><th>Expires</th></tr></thead>'
        f'<tbody>{inv_rows or "<tr><td colspan=5>None.</td></tr>"}</tbody>'
        '</table>'
        '<p><a href="/clients">&larr; Back to clients</a></p>'
        '</div>'
    )


def render_user_portal_admin(
    *, client: dict[str, Any], user_token: str,
    users: list[dict[str, Any]],
    invitations: list[dict[str, Any]] | None = None,
    flash: str = '', flash_error: str = '',
) -> str:
    name = _esc(client.get('client_name') or client.get('client_code') or '')
    flash_html = ''
    if flash:
        flash_html = (
            f'<div style="background:#d4edda;padding:8px;'
            f'margin-bottom:10px;">{_esc(flash)}</div>'
        )
    if flash_error:
        flash_html += (
            f'<div style="background:#f8d7da;padding:8px;'
            f'margin-bottom:10px;">{_esc(flash_error)}</div>'
        )

    user_rows = ''
    for u in users:
        uid = u['id']
        role = _esc(u.get('role') or '')
        status = _esc(u.get('status') or '')
        email = _esc(u.get('email') or '')
        full_name = _esc(u.get('full_name') or '')
        uploads = int(u.get('upload_count') or 0)
        last = _esc(u.get('last_active_at') or 'never')
        actions = ''
        if status == 'active':
            actions += (
                f'<form method="POST" '
                f'action="/cp/{_esc(user_token)}/users/{uid}/suspend" '
                'style="display:inline;">'
                '<button type="submit">Suspend</button></form> '
            )
        if status == 'suspended':
            actions += (
                f'<form method="POST" '
                f'action="/cp/{_esc(user_token)}/users/{uid}/reactivate" '
                'style="display:inline;">'
                '<button type="submit">Reactivate</button></form> '
            )
        if status != 'removed':
            actions += (
                f'<form method="POST" '
                f'action="/cp/{_esc(user_token)}/users/{uid}/remove" '
                'style="display:inline;" '
                'onsubmit="return confirm(\'Remove this user? Their token stops working.\');">'
                '<button type="submit" style="background:#dc2626;color:white;">Remove</button></form> '
            )
            if role == 'contributor':
                actions += (
                    f'<form method="POST" '
                    f'action="/cp/{_esc(user_token)}/users/{uid}/make_admin" '
                    'style="display:inline;">'
                    '<button type="submit">Make admin</button></form> '
                )
            elif role == 'admin':
                actions += (
                    f'<form method="POST" '
                    f'action="/cp/{_esc(user_token)}/users/{uid}/make_contributor" '
                    'style="display:inline;">'
                    '<button type="submit">Make contributor</button></form> '
                )
        user_rows += (
            f'<tr><td>{full_name}</td><td>{email}</td>'
            f'<td>{role}</td><td>{status}</td>'
            f'<td>{uploads}</td><td>{last}</td>'
            f'<td>{actions}</td></tr>'
        )

    invite_rows = ''
    for inv in invitations or []:
        status = _esc(inv.get('status') or '')
        invite_rows += (
            f'<tr><td>{_esc(inv.get("email") or "")}</td>'
            f'<td>{_esc(inv.get("invited_role") or "")}</td>'
            f'<td>{status}</td>'
            f'<td>{_esc(inv.get("expires_at") or "")}</td></tr>'
        )

    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<title>Team admin</title>'
        '<style>body{font-family:system-ui,Arial;max-width:900px;'
        'margin:2rem auto;padding:1rem;}'
        'table{width:100%;border-collapse:collapse;margin:1rem 0;}'
        'th,td{border-bottom:1px solid #eee;padding:8px;text-align:left;}'
        'form.inline{display:inline;}'
        '.card{background:#f9fafb;border:1px solid #e5e7eb;padding:1rem;'
        'border-radius:6px;margin-bottom:1rem;}'
        '</style></head><body>'
        f'<p><a href="/cp/{_esc(user_token)}/upload">&larr; Back to upload</a></p>'
        f'<h1>{name} — team admin</h1>'
        f'{flash_html}'
        '<div class="card"><h2>Invite someone</h2>'
        f'<form method="POST" action="/cp/{_esc(user_token)}/invite" '
        'style="display:grid;grid-template-columns:1fr 1fr 120px auto;gap:8px;">'
        '<input type="email" name="email" placeholder="Email" required>'
        '<input type="text" name="full_name" placeholder="Full name">'
        '<select name="role">'
        '<option value="contributor">Contributor</option>'
        '<option value="admin">Admin</option>'
        '</select>'
        '<button type="submit" class="primary" '
        'style="background:#1e40af;color:white;padding:8px 14px;border:none;">'
        'Send invitation</button></form></div>'
        '<h2>Team members</h2>'
        '<table><thead><tr><th>Name</th><th>Email</th><th>Role</th>'
        '<th>Status</th><th>Uploads</th><th>Last active</th><th>Actions</th></tr></thead>'
        f'<tbody>{user_rows or "<tr><td colspan=7>No members yet.</td></tr>"}</tbody>'
        '</table>'
        '<h2>Pending invitations</h2>'
        '<table><thead><tr><th>Email</th><th>Role</th>'
        '<th>Status</th><th>Expires</th></tr></thead>'
        f'<tbody>{invite_rows or "<tr><td colspan=4>None.</td></tr>"}</tbody>'
        '</table>'
        '</body></html>'
    )


def render_invitation_email(
    *, recipient_name: str, inviter_name: str,
    client_display: str, accept_url: str, lang: str = 'en',
) -> tuple[str, str]:
    """Return (subject, html_body) for the invitation email.

    Bilingual — explicit lang wins; callers should derive lang from the
    session (portal_user.language), an explicit ``lang=`` form field,
    or a fallback to the request's Accept-Language header."""
    lang_key = 'fr' if (lang or '').lower().startswith('fr') else 'en'
    safe_recipient = _esc(recipient_name or 'there')
    safe_inviter = _esc(inviter_name or 'A colleague')
    safe_client = _esc(client_display or '')
    safe_url = _esc(accept_url or '')
    if lang_key == 'fr':
        subject = (
            f'{inviter_name or "Un collègue"} vous invite à soumettre '
            'des reçus sur OtoCPA'
        )
        body = (
            f'<p>Bonjour {safe_recipient},</p>'
            f'<p><strong>{safe_inviter}</strong> vous a invité(e) à soumettre '
            f'reçus et factures pour <strong>{safe_client}</strong> '
            'sur OtoCPA.</p>'
            '<p>Acceptez l\'invitation (expire dans 14 jours) :</p>'
            f'<p><a href="{safe_url}">{safe_url}</a></p>'
            '<p style="color:#6b7280;font-size:12px;margin-top:2rem;">'
            'Si vous n\'attendiez pas ce courriel, ignorez-le.</p>'
        )
    else:
        subject = (
            f'{inviter_name or "A colleague"} invited you to submit '
            'receipts on OtoCPA'
        )
        body = (
            f'<p>Hi {safe_recipient},</p>'
            f'<p><strong>{safe_inviter}</strong> has invited you to submit '
            f'receipts and invoices for <strong>{safe_client}</strong> '
            'on OtoCPA.</p>'
            '<p>Accept the invitation (expires in 14 days):</p>'
            f'<p><a href="{safe_url}">{safe_url}</a></p>'
            '<p style="color:#6b7280;font-size:12px;margin-top:2rem;">'
            'If you were not expecting this email, you can ignore it.</p>'
        )
    return subject, body


def render_accept_invitation_page(
    inv: dict[str, Any], *, client_name: str,
    firm_name: str = '',
) -> str:
    name = _esc(inv.get('full_name') or inv.get('email') or '')
    role = _esc(inv.get('invited_role') or 'contributor')
    client = _esc(client_name or inv.get('client_code') or '')
    firm = _esc(firm_name or inv.get('firm_code') or '')
    tok = _esc(inv.get('invitation_token') or '')
    expires = _esc(inv.get('expires_at') or '')
    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<title>Accept invitation</title>'
        '<style>body{font-family:system-ui,Arial;max-width:560px;'
        'margin:3rem auto;padding:1rem;}'
        '.card{background:#f9fafb;border:1px solid #e5e7eb;padding:1.5rem;'
        'border-radius:8px;}'
        'button.primary{background:#1e40af;color:white;padding:12px 24px;'
        'border:none;border-radius:6px;font-size:16px;cursor:pointer;}'
        '</style></head><body>'
        f'<div class="card">'
        f'<h1>You are invited to upload receipts to {firm}</h1>'
        f'<p>Hi {name} — <strong>{client}</strong> uses OtoCPA to submit '
        f'receipts and invoices to <strong>{firm}</strong>. You have been '
        f'invited as <strong>{role}</strong>.</p>'
        f'<p>Accept the invitation to get your personal upload link. '
        f'This invitation expires on {expires}.</p>'
        f'<form method="POST" action="/invite/{tok}/accept">'
        f'<button class="primary" type="submit">Accept invitation</button>'
        f'</form></div>'
        '</body></html>'
    )
