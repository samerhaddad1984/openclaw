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
# WhatsApp identity
# ---------------------------------------------------------------------------
#
# A portal user can register at most one WhatsApp number; that number
# is globally unique because the Twilio webhook has no other way to
# disambiguate two people who happen to register the same handset
# across firms. We still enforce firm-scoped uniqueness explicitly
# even though the DB index covers it, so the error message we show
# can distinguish "already used in this firm" from "used at a
# different firm" (the admin cares about the former, the CPA about
# the latter).


def validate_whatsapp_number(
    db_path: Path | str, *,
    raw_number: str,
    firm_code: str,
    client_code: str,
    current_user_id: int | None = None,
) -> dict[str, Any]:
    """Check if *raw_number* can be saved for this user.

    Returns a dict::

        {
            'valid': bool,
            'normalized': str | None,
            'error': str | None,
            'already_used': bool,
            'used_in_firm': bool,  # vs. another firm
        }

    ``current_user_id`` lets edit flows accept the number they
    already own without flagging a "duplicate".
    """
    from src.integrations.phone_normalizer import normalize_phone
    normalized = normalize_phone(raw_number)
    if normalized is None:
        return {
            'valid': False,
            'normalized': None,
            'error': 'invalid_format',
            'already_used': False,
            'used_in_firm': False,
        }
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT id, firm_code, client_code FROM client_portal_users "
            "WHERE whatsapp_number=? "
            "AND COALESCE(status,'invited') != 'removed'",
            (normalized,),
        ).fetchone()
    if row and row['id'] != current_user_id:
        return {
            'valid': False,
            'normalized': normalized,
            'error': 'already_used',
            'already_used': True,
            'used_in_firm': row['firm_code'] == firm_code,
        }
    return {
        'valid': True,
        'normalized': normalized,
        'error': None,
        'already_used': False,
        'used_in_firm': False,
    }


def set_user_whatsapp_number(
    db_path: Path | str, *,
    firm_code: str, client_code: str, user_id: int,
    raw_number: str | None, actor_email: str,
) -> dict[str, Any]:
    """Attach (or clear) a WhatsApp number on a portal user row.

    Pass ``raw_number=None`` (or empty) to clear. Raises ``ValueError``
    on invalid format and on uniqueness collisions — callers should
    surface the message to the UI.
    """
    if raw_number is None or not str(raw_number).strip():
        with _open(db_path) as conn:
            conn.execute(
                "UPDATE client_portal_users SET whatsapp_number=NULL, "
                "whatsapp_verified=0, whatsapp_verified_at=NULL "
                "WHERE id=? AND firm_code=? AND client_code=?",
                (user_id, firm_code, client_code),
            )
            _audit(conn, firm_code=firm_code, client_code=client_code,
                    actor_email=actor_email,
                    action='whatsapp_number_cleared',
                    portal_user_id=user_id, detail='')
            conn.commit()
        return {'normalized': None}

    check = validate_whatsapp_number(
        db_path, raw_number=raw_number,
        firm_code=firm_code, client_code=client_code,
        current_user_id=user_id,
    )
    if not check['valid']:
        if check['error'] == 'invalid_format':
            raise ValueError("Invalid WhatsApp number format")
        raise ValueError("WhatsApp number already registered to another user")
    normalized = check['normalized']
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE client_portal_users SET whatsapp_number=?, "
            "whatsapp_verified=1, whatsapp_verified_at=? "
            "WHERE id=? AND firm_code=? AND client_code=?",
            (normalized, _iso_now(), user_id, firm_code, client_code),
        )
        _audit(conn, firm_code=firm_code, client_code=client_code,
                actor_email=actor_email,
                action='whatsapp_number_set',
                portal_user_id=user_id, detail=normalized)
        conn.commit()
    return {'normalized': normalized}


# ---------------------------------------------------------------------------
# Invitation lifecycle
# ---------------------------------------------------------------------------


INVITE_EXPIRY_DAYS = 14


def _new_invite_token() -> str:
    return secrets.token_urlsafe(32)


def get_invitation_by_request_id(
    db_path: Path | str, *,
    firm_code: str, client_code: str, client_request_id: str,
) -> dict[str, Any] | None:
    """Look up a prior invitation by its client_request_id.

    Used by the invite-POST handler to de-dupe double-clicks: if the
    second POST shares a request_id with an already-created row, the
    handler short-circuits and returns the cached invitation."""
    if not client_request_id:
        return None
    with _open(db_path) as conn:
        try:
            row = conn.execute(
                "SELECT * FROM client_portal_invitations "
                "WHERE firm_code=? AND client_code=? AND client_request_id=?",
                (firm_code, client_code, client_request_id),
            ).fetchone()
        except sqlite3.OperationalError:
            return None
    return _rowdict(row)


def create_invitation(
    db_path: Path | str, *,
    firm_code: str, client_code: str,
    email: str, full_name: str, role: str,
    invited_by: str, lang: str | None = None,
    client_request_id: str | None = None,
    whatsapp_number: str | None = None,
) -> dict[str, Any]:
    """Create or replace a pending invitation for (client, email).

    Role must be 'admin' or 'contributor'. If an older pending
    invitation exists for the same email it gets superseded (status
    flips to 'cancelled') so resending doesn't orphan invite tokens.
    ``lang='fr'|'en'`` is remembered on the row so the accept page +
    email render in the same language the inviter chose.

    ``whatsapp_number`` (optional, any NANP-ish shape) is normalized
    to E.164 and validated for uniqueness up front. Storing it on
    the invitation row lets ``accept_invitation`` copy it onto the
    user without a separate round-trip.
    """
    if role not in VALID_ROLES:
        raise ValueError(f"invalid role {role!r}")
    if not email or '@' not in email:
        raise ValueError(f"invalid email {email!r}")
    # Validate + normalize the WhatsApp number (if any) before we
    # mint the invitation token so typos surface at the admin
    # without needing a second submit.
    normalized_wa: str | None = None
    if whatsapp_number and whatsapp_number.strip():
        check = validate_whatsapp_number(
            db_path, raw_number=whatsapp_number,
            firm_code=firm_code, client_code=client_code,
        )
        if not check['valid']:
            if check['error'] == 'invalid_format':
                raise ValueError(
                    "Invalid WhatsApp number format / "
                    "Format de numéro WhatsApp invalide"
                )
            raise ValueError(
                "WhatsApp number already registered to another user / "
                "Numéro WhatsApp déjà enregistré à un autre utilisateur"
            )
        normalized_wa = check['normalized']

    # Item 5: idempotency — if this exact (firm, client, request_id)
    # triple already has an invitation, return it unchanged instead of
    # minting a second token + cancelling the first.
    if client_request_id:
        existing = get_invitation_by_request_id(
            db_path, firm_code=firm_code, client_code=client_code,
            client_request_id=client_request_id,
        )
        if existing is not None:
            return {
                'id': existing['id'],
                'token': existing['invitation_token'],
                'email': existing['email'],
                'role': existing['invited_role'],
                'expires_at': existing['expires_at'],
                'idempotent_replay': True,
            }

    token = _new_invite_token()
    now = _iso_now()
    expires = _iso_in(INVITE_EXPIRY_DAYS)
    invited_language = (lang or '').lower()
    if invited_language not in ('fr', 'en'):
        invited_language = None
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE client_portal_invitations SET status='cancelled' "
            "WHERE firm_code=? AND client_code=? "
            "AND LOWER(email)=LOWER(?) AND status='pending'",
            (firm_code, client_code, email),
        )
        # Inspect which optional columns this DB has so we can tailor
        # the INSERT. Reading PRAGMA once avoids try/except cascades
        # that accidentally drop invited_language when only
        # client_request_id is missing.
        _cpi_cols = {r['name'] for r in conn.execute(
            "PRAGMA table_info(client_portal_invitations)").fetchall()}
        has_wa_col = 'whatsapp_number' in _cpi_cols
        try:
            if ('client_request_id' in _cpi_cols
                    and 'invited_language' in _cpi_cols
                    and has_wa_col):
                cur = conn.execute(
                    "INSERT INTO client_portal_invitations "
                    "(firm_code, client_code, email, full_name, invited_role, "
                    " invitation_token, invited_by, invited_at, expires_at, "
                    " status, invited_language, client_request_id, "
                    " whatsapp_number) "
                    "VALUES (?,?,?,?,?,?,?,?,?, 'pending', ?, ?, ?)",
                    (firm_code, client_code, email, full_name, role, token,
                     invited_by, now, expires, invited_language,
                     client_request_id, normalized_wa),
                )
            elif ('client_request_id' in _cpi_cols
                    and 'invited_language' in _cpi_cols):
                cur = conn.execute(
                    "INSERT INTO client_portal_invitations "
                    "(firm_code, client_code, email, full_name, invited_role, "
                    " invitation_token, invited_by, invited_at, expires_at, "
                    " status, invited_language, client_request_id) "
                    "VALUES (?,?,?,?,?,?,?,?,?, 'pending', ?, ?)",
                    (firm_code, client_code, email, full_name, role, token,
                     invited_by, now, expires, invited_language,
                     client_request_id),
                )
            elif 'invited_language' in _cpi_cols:
                cur = conn.execute(
                    "INSERT INTO client_portal_invitations "
                    "(firm_code, client_code, email, full_name, invited_role, "
                    " invitation_token, invited_by, invited_at, expires_at, "
                    " status, invited_language) "
                    "VALUES (?,?,?,?,?,?,?,?,?, 'pending', ?)",
                    (firm_code, client_code, email, full_name, role, token,
                     invited_by, now, expires, invited_language),
                )
            else:
                cur = conn.execute(
                    "INSERT INTO client_portal_invitations "
                    "(firm_code, client_code, email, full_name, invited_role, "
                    " invitation_token, invited_by, invited_at, expires_at, status) "
                    "VALUES (?,?,?,?,?,?,?,?,?, 'pending')",
                    (firm_code, client_code, email, full_name, role, token,
                     invited_by, now, expires),
                )
        except sqlite3.IntegrityError:
            # Concurrent double-click race: a sibling request with the
            # same client_request_id won the INSERT. Replay that row.
            existing = get_invitation_by_request_id(
                db_path, firm_code=firm_code, client_code=client_code,
                client_request_id=client_request_id or '',
            )
            if existing:
                conn.rollback()
                return {
                    'id': existing['id'],
                    'token': existing['invitation_token'],
                    'email': existing['email'],
                    'role': existing['invited_role'],
                    'expires_at': existing['expires_at'],
                    'idempotent_replay': True,
                }
            raise
        inv_id = int(cur.lastrowid)
        _audit(conn, firm_code=firm_code, client_code=client_code,
                actor_email=invited_by, action='invitation_created',
                detail=f'email={email} role={role} lang={invited_language or "auto"}')
        conn.commit()
    return {'id': inv_id, 'token': token, 'email': email,
             'role': role, 'expires_at': expires,
             'idempotent_replay': False}


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
    # If the invite staged a WhatsApp number, promote it onto the
    # new user row. Re-validates in case the number was claimed by
    # someone else between invite creation + acceptance.
    invited_wa = inv.get('whatsapp_number')
    if invited_wa:
        try:
            set_user_whatsapp_number(
                db_path, firm_code=inv['firm_code'],
                client_code=inv['client_code'],
                user_id=user['id'],
                raw_number=invited_wa,
                actor_email=inv.get('invited_by') or 'cpa',
            )
            user = get_user(db_path, user_id=user['id']) or user
        except ValueError:
            # Collision at accept time → leave the user row
            # without a WhatsApp number; admin can reassign.
            pass
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


# Item 5: per-admin invitation rate limit (10 invites / 60s / admin).
_PER_ADMIN_INVITES_PER_MIN = 10
_per_admin_invites: dict[int, list[float]] = {}


def invite_rate_allowed(admin_user_id: int) -> bool:
    """True when the portal admin is under 10 invites / minute."""
    import time as _t
    now = _t.time()
    with _per_user_log_lock():
        log = _per_admin_invites.setdefault(admin_user_id, [])
        cutoff = now - 60.0
        log[:] = [t for t in log if t >= cutoff]
        if len(log) >= _PER_ADMIN_INVITES_PER_MIN:
            return False
        log.append(now)
    return True


def reset_invite_rate_limits() -> None:
    with _per_user_log_lock():
        _per_admin_invites.clear()


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

    from src.integrations.phone_normalizer import format_display
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
        wa_raw = u.get('whatsapp_number') or ''
        wa_display = _esc(format_display(wa_raw)) if wa_raw else (
            '<em style="color:#6b7280;">Non enregistré / '
            'Not registered</em>'
        )
        wa_form = (
            '<form method="POST" '
            'action="/clients/portal_users/whatsapp" '
            'style="display:inline-flex;gap:4px;margin-top:4px;">'
            f'<input type="hidden" name="client_code" value="{code}">'
            f'<input type="hidden" name="user_id" value="{u["id"]}">'
            f'<input type="text" name="whatsapp_number" '
            f'value="{_esc(wa_raw)}" '
            'placeholder="+1 (514) 555-0100" '
            'style="width:150px;font-size:12px;">'
            '<button type="submit" style="font-size:12px;">'
            'Override</button></form>'
        )
        wa_cell = f'<div style="font-size:12px;">{wa_display}</div>{wa_form}'
        rows += (
            f'<tr><td>{_esc(u.get("full_name") or "")}</td>'
            f'<td>{_esc(u.get("email") or "")}</td>'
            f'<td>{role}</td><td>{status}</td>'
            f'<td>{wa_cell}</td>'
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
        '<th>Status</th><th>WhatsApp</th>'
        '<th>Uploads</th><th>Last active</th>'
        '<th>CPA override</th></tr></thead>'
        f'<tbody>{rows or "<tr><td colspan=8>No users yet.</td></tr>"}</tbody>'
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


def _invite_request_id() -> str:
    """Per-form-render random request id (hex-32). The backend uses
    this to de-dupe double-click POSTs (Item 5)."""
    return 'inv_' + secrets.token_hex(16)


def render_user_portal_admin(
    *, client: dict[str, Any], user_token: str,
    users: list[dict[str, Any]],
    invitations: list[dict[str, Any]] | None = None,
    audit_entries: list[dict[str, Any]] | None = None,
    flash: str = '', flash_error: str = '',
    nav_html: str = '',
) -> str:
    from src.integrations.phone_normalizer import format_display
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
        wa_raw = u.get('whatsapp_number') or ''
        if wa_raw:
            wa_display_html = (
                f'<div style="font-size:12px;color:#374151;">'
                f'{_esc(format_display(wa_raw))}</div>'
            )
        else:
            wa_display_html = (
                '<div class="muted">'
                '<em>Non enregistré / Not registered</em></div>'
            )
        wa_cell = wa_display_html + (
            f'<form method="POST" '
            f'action="/cp/{_esc(user_token)}/users/{uid}/whatsapp" '
            'style="display:inline-flex;gap:4px;align-items:center;">'
            f'<input type="text" name="whatsapp_number" '
            f'value="{_esc(wa_raw)}" placeholder="+1 (514) 555-0100" '
            'style="width:150px;" data-wa-field="1">'
            '<button type="submit" style="font-size:12px;">'
            'Enregistrer / Save</button>'
            '</form>'
        )
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
                f'action="/cp/{_esc(user_token)}/users/{uid}/rotate_token" '
                'style="display:inline;" '
                'onsubmit="return confirm(\'Rotate this user\\\'s access link? Their old link stops working immediately. / Renouveler ce lien? L\\\'ancien cessera de fonctionner immédiatement.\');">'
                '<button type="submit">Rotate link / Renouveler</button></form> '
            )
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
            f'<td>{wa_cell}</td>'
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
        '<style>body{font-family:system-ui,Arial;max-width:1000px;'
        'margin:2rem auto;padding:1rem;}'
        'table{width:100%;border-collapse:collapse;margin:1rem 0;}'
        'th,td{border-bottom:1px solid #eee;padding:8px;text-align:left;'
        'vertical-align:top;font-size:14px;}'
        'form.inline{display:inline;}'
        '.tabs{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:1rem;}'
        '.tabs a{padding:8px 14px;background:#f3f4f6;color:#111827;'
        'border-radius:8px 8px 0 0;text-decoration:none;font-size:14px;}'
        '.tabs a.active{background:#2a8759;color:white;}'
        '.card{background:#f9fafb;border:1px solid #e5e7eb;padding:1rem;'
        'border-radius:6px;margin-bottom:1rem;}'
        '.muted{color:#6b7280;font-size:12px;}'
        '.wa-ok{color:#166534;font-size:12px;}'
        '.wa-bad{color:#b91c1c;font-size:12px;}'
        '</style></head><body>'
        f'{nav_html}'
        + ('' if nav_html else
           f'<p><a href="/cp/{_esc(user_token)}/upload">'
           '&larr; Back to upload</a></p>') +
        f'<h1>{name} — team admin</h1>'
        f'{flash_html}'
        '<div class="card"><h2>Invite someone / Inviter quelqu\'un</h2>'
        f'<form method="POST" action="/cp/{_esc(user_token)}/invite" '
        'id="portal-invite-form" '
        'onsubmit="return _inviteSubmit(this);" '
        'style="display:grid;grid-template-columns:1fr 1fr 180px 120px auto;'
        'gap:8px;align-items:start;">'
        # Item 5: client_request_id minted per form render; double-
        # click submits the same id twice, backend replays cached row.
        f'<input type="hidden" name="client_request_id" value="'
        f'{_esc(_invite_request_id())}">'
        '<input type="email" name="email" placeholder="Email" required>'
        '<input type="text" name="full_name" placeholder="Full name / Nom">'
        '<input type="text" name="whatsapp_number" id="invite-wa-field" '
        'placeholder="WhatsApp (optional / optionnel)" '
        'data-wa-field="1">'
        '<select name="role">'
        '<option value="contributor">Contributor</option>'
        '<option value="admin">Admin</option>'
        '</select>'
        '<button type="submit" id="portal-invite-btn" class="primary" '
        'style="background:#1e40af;color:white;padding:8px 14px;border:none;">'
        'Send invitation</button>'
        '<div id="invite-wa-hint" class="muted" '
        'style="grid-column:1 / -1;">'
        "Les utilisateurs enregistrés peuvent envoyer des reçus par "
        "WhatsApp depuis ce numéro. / Registered users can send "
        "receipts via WhatsApp from this number."
        '</div>'
        '</form>'
        '<script>'
        'function _inviteSubmit(f){'
        'var b=document.getElementById("portal-invite-btn");'
        'if(b && b.dataset.submitting==="1"){return false;}'
        'if(b){b.dataset.submitting="1";b.disabled=true;'
        'b.style.background="#6b7280";b.style.cursor="wait";'
        'b.textContent="Sending...";'
        'setTimeout(function(){b.disabled=false;b.dataset.submitting="";'
        'b.style.background="#1e40af";b.style.cursor="pointer";'
        'b.textContent="Send invitation";}, 30000);}'
        'return true;}'
        # Live WhatsApp validation on any input with data-wa-field.
        # Calls /cp/<tok>/validate_whatsapp and renders a hint beside
        # the field. Debounced so we don't hammer the endpoint on
        # every keystroke.
        '(function(){'
        f'var base="/cp/{_esc(user_token)}/validate_whatsapp";'
        'function attach(inp){'
        'var hint=document.createElement("span");'
        'hint.className="muted";hint.style.marginLeft="6px";'
        'inp.parentNode.insertBefore(hint, inp.nextSibling);'
        'var t=null;'
        'inp.addEventListener("input", function(){'
        'clearTimeout(t);var v=inp.value.trim();'
        'if(!v){hint.textContent="";return;}'
        't=setTimeout(function(){'
        'var fd=new FormData();fd.append("number", v);'
        'if(inp.dataset.userId){fd.append("user_id", inp.dataset.userId);}'
        'fetch(base,{method:"POST",body:fd}).then(function(r){return r.json();})'
        '.then(function(j){'
        'if(j.valid){hint.className="wa-ok";'
        'hint.textContent="✓ " + (j.normalized || "OK");}'
        'else if(j.error==="already_used"){hint.className="wa-bad";'
        'hint.textContent=j.used_in_firm ? '
        '"Already registered / Déjà enregistré (firm)" : '
        '"Already registered at another firm";}'
        'else{hint.className="wa-bad";'
        'hint.textContent="Invalid format / Format invalide";}'
        '}).catch(function(){});'
        '}, 350);'
        '});}'
        'document.querySelectorAll("[data-wa-field]").forEach(attach);'
        '})();'
        '</script>'
        '</div>'
        '<h2>Team members / Membres</h2>'
        '<table><thead><tr><th>Name</th><th>Email</th><th>Role</th>'
        '<th>Status</th><th>WhatsApp</th>'
        '<th>Uploads</th><th>Last active</th><th>Actions</th></tr></thead>'
        f'<tbody>{user_rows or "<tr><td colspan=8>No members yet.</td></tr>"}</tbody>'
        '</table>'
        '<h2>Pending invitations</h2>'
        '<table><thead><tr><th>Email</th><th>Role</th>'
        '<th>Status</th><th>Expires</th></tr></thead>'
        f'<tbody>{invite_rows or "<tr><td colspan=4>None.</td></tr>"}</tbody>'
        '</table>'
        + _render_portal_audit_section(audit_entries or [])
        + '</body></html>'
    )


def _render_portal_audit_section(entries: list[dict[str, Any]]) -> str:
    """Render the client-admin view of the portal audit trail.

    Bilingual FR/EN throughout — CPAs and client admins share the same
    screen, so we never rely on a session lang flag.
    """
    if not entries:
        return (
            '<h2>Historique / Activity log</h2>'
            '<p class="muted"><em>Aucune activité. / No activity yet.</em></p>'
        )
    rows = ''
    for e in entries:
        when = _esc(e.get('created_at') or '')
        actor = _esc(e.get('actor_email') or '')
        action = _esc(e.get('action') or '')
        detail = _esc(e.get('detail') or '')
        rows += (
            f'<tr><td style="white-space:nowrap;">{when}</td>'
            f'<td>{actor}</td><td>{action}</td>'
            f'<td class="muted">{detail}</td></tr>'
        )
    return (
        '<h2>Historique / Activity log</h2>'
        '<table><thead><tr>'
        '<th>Quand / When</th><th>Par / By</th>'
        '<th>Action</th><th>Détail / Detail</th>'
        '</tr></thead>'
        f'<tbody>{rows}</tbody>'
        '</table>'
    )


PORTAL_USER_TOUR_TOTAL_CONTRIBUTOR = 3
PORTAL_USER_TOUR_TOTAL_ADMIN = 4

# Backwards-compat: callers that don't know the role yet default to
# the contributor length. `render_portal_user_tour` now accepts
# `role=...` and injects the admin screen when role=='admin'.
PORTAL_USER_TOUR_TOTAL = PORTAL_USER_TOUR_TOTAL_CONTRIBUTOR


_PORTAL_USER_TOUR_ADMIN_SCREEN = {
    'en': {
        'title': 'Manage your team',
        'subtitle': 'You can invite colleagues and set their role.',
        'body': (
            "As an admin for {firm_client}, the admin page "
            "(/cp/.../admin) lets you invite bookkeepers, the office "
            "manager, or any other uploader. Each invitee gets a "
            "personal email with a 14-day link. You can suspend, "
            "reactivate, or remove someone later if needed — their "
            "past uploads stay attributed to them."
        ),
    },
    'fr': {
        'title': 'Gérez votre équipe',
        'subtitle': "Vous pouvez inviter des collègues et leur attribuer un rôle.",
        'body': (
            "En tant qu'admin pour {firm_client}, la page admin "
            "(/cp/.../admin) vous permet d'inviter comptables, adjoint(e)s "
            "administratif(ves), ou tout autre utilisateur. Chaque "
            "invité reçoit un courriel personnel avec un lien de 14 "
            "jours. Vous pouvez suspendre, réactiver ou retirer "
            "quelqu'un plus tard si nécessaire — ses anciens "
            "téléversements lui restent attribués."
        ),
    },
}


_PORTAL_USER_TOUR_CONTENT = [
    {
        'en': {
            'title': 'Welcome, {name}!',
            'subtitle': 'You were invited to submit receipts to {firm}.',
            'body': (
                "This portal is where you upload receipts and invoices "
                "so your accountant can record them. Your uploads are "
                "tagged with your name so {firm} always knows who sent "
                "what."
            ),
        },
        'fr': {
            'title': 'Bienvenue, {name} !',
            'subtitle': 'Vous avez été invité(e) à soumettre des reçus à {firm}.',
            'body': (
                "Ce portail vous permet de téléverser reçus et factures "
                "pour que votre comptable puisse les enregistrer. Vos "
                "téléversements portent votre nom pour que {firm} sache "
                "toujours qui a envoyé quoi."
            ),
        },
    },
    {
        'en': {
            'title': 'How to upload',
            'subtitle': 'Two taps and it is on the way.',
            'body': (
                "Take a photo with your phone (or pick a PDF), tap "
                "'Upload'. We run OCR on it, {firm} reviews, and "
                "the number lands in their books. You can upload as "
                "many at a time as you want."
            ),
        },
        'fr': {
            'title': 'Comment téléverser',
            'subtitle': 'Deux tapes et c\'est parti.',
            'body': (
                "Prenez une photo avec votre téléphone (ou choisissez "
                "un PDF), tapez « Téléverser ». Nous effectuons l'OCR, "
                "{firm} révise, et le montant est enregistré. Vous "
                "pouvez téléverser autant de documents que nécessaire "
                "à la fois."
            ),
        },
    },
    {
        'en': {
            'title': 'Messages + status',
            'subtitle': 'Know where things stand.',
            'body': (
                "The Status page shows what has been processed, what "
                "is still in review, and what was approved. If {firm} "
                "has a question about a receipt, they'll message you "
                "here and you can reply without leaving the portal."
            ),
        },
        'fr': {
            'title': 'Messages et statut',
            'subtitle': 'Sachez où en sont vos documents.',
            'body': (
                "La page Statut montre ce qui a été traité, ce qui "
                "est en révision et ce qui a été approuvé. Si {firm} "
                "a une question sur un reçu, ils vous enverront un "
                "message ici et vous pourrez répondre sans quitter "
                "le portail."
            ),
        },
    },
]


def _portal_tour_labels(lang: str) -> dict[str, str]:
    fr = {
        'step_of': 'Étape {n} sur {total}',
        'back': '&larr; Retour',
        'next': 'Suivant &rarr;',
        'finish': 'Commencer',
        'skip': 'Ignorer la visite',
        'other': 'English',
    }
    en = {
        'step_of': 'Step {n} of {total}',
        'back': '&larr; Back',
        'next': 'Next &rarr;',
        'finish': 'Get started',
        'skip': 'Skip tour',
        'other': 'Français',
    }
    return fr if lang == 'fr' else en


def portal_tour_total_for_role(role: str) -> int:
    """Admins see the team-management screen; contributors don't."""
    return (PORTAL_USER_TOUR_TOTAL_ADMIN if (role or '').lower() == 'admin'
            else PORTAL_USER_TOUR_TOTAL_CONTRIBUTOR)


def _portal_tour_screen_for_step(step: int, role: str) -> dict:
    """Resolve which screen content-block a given 1-based step maps to.

    Contributor: [welcome, upload, messages] = 3 screens in order.
    Admin: [welcome, upload, messages, manage-team] = 4 screens;
    the manage-team block is a separate dict rather than an extra
    entry in _PORTAL_USER_TOUR_CONTENT so contributor paths stay
    untouched."""
    is_admin = (role or '').lower() == 'admin'
    total = portal_tour_total_for_role(role)
    step = max(1, min(total, step))
    # For both roles, steps 1..3 come from _PORTAL_USER_TOUR_CONTENT.
    if step <= 3:
        return {'source': 'base', 'block': _PORTAL_USER_TOUR_CONTENT[step - 1],
                'step': step, 'total': total, 'admin_screen': False}
    # step == 4 only exists for admin.
    return {'source': 'admin', 'block': _PORTAL_USER_TOUR_ADMIN_SCREEN,
            'step': step, 'total': total, 'admin_screen': True}


def render_portal_user_tour(
    step: int, *,
    user_name: str,
    firm_name: str,
    user_token: str,
    lang: str = 'en',
    role: str = 'contributor',
    firm_client_display: str | None = None,
) -> str:
    """Bilingual tour shown on first login after invite.

    Contributors get 3 screens; admins get 4 (the extra one covers
    managing teammates). ``role`` is the portal user's role;
    ``firm_client_display`` is shown in the admin screen body to
    ground them (e.g. 'Construction Tremblay at Sam CPA')."""
    lang_key = 'fr' if (lang or '').lower().startswith('fr') else 'en'
    resolved = _portal_tour_screen_for_step(step, role)
    total = resolved['total']
    step = resolved['step']
    screen = resolved['block'][lang_key]
    labels = _portal_tour_labels(lang_key)

    title_kwargs = {'name': _esc(user_name or '')}
    subtitle_kwargs = {'firm': _esc(firm_name or '')}
    body_kwargs = {'firm': _esc(firm_name or ''),
                    'firm_client': _esc(firm_client_display or firm_name or '')}

    title = screen['title'].format(**{**title_kwargs, **body_kwargs})
    subtitle = screen['subtitle'].format(**{**subtitle_kwargs, **body_kwargs})
    body_text = screen['body'].format(**body_kwargs)
    step_label = labels['step_of'].format(n=step, total=total)

    prev_html = ''
    if step > 1:
        prev_html = (
            f'<a href="/cp/{_esc(user_token)}/tour/{step-1}?lang={lang_key}" '
            'style="margin-right:12px;color:#6b7280;">'
            f'{labels["back"]}</a>'
        )

    if step < total:
        next_btn = (
            f'<a href="/cp/{_esc(user_token)}/tour/{step+1}?lang={lang_key}" '
            'style="background:#1e40af;color:white;padding:10px 22px;'
            'border-radius:4px;text-decoration:none;font-weight:bold;">'
            f'{labels["next"]}</a>'
        )
    else:
        next_btn = (
            '<form method="POST" '
            f'action="/cp/{_esc(user_token)}/tour/complete" '
            'style="display:inline;">'
            f'<input type="hidden" name="lang" value="{lang_key}">'
            '<button type="submit" '
            'style="background:#16C172;color:black;padding:10px 22px;'
            'border:none;border-radius:4px;font-weight:bold;cursor:pointer;">'
            f'{labels["finish"]}</button></form>'
        )
    skip = (
        '<form method="POST" '
        f'action="/cp/{_esc(user_token)}/tour/complete" '
        'style="display:inline;margin-left:16px;">'
        f'<input type="hidden" name="lang" value="{lang_key}">'
        '<button type="submit" '
        'style="background:none;border:none;color:#9ca3af;'
        'cursor:pointer;text-decoration:underline;padding:0;">'
        f'{labels["skip"]}</button></form>'
    )

    other_lang = 'en' if lang_key == 'fr' else 'fr'
    switcher = (
        f'<a href="/cp/{_esc(user_token)}/tour/{step}?lang={other_lang}" '
        'style="position:absolute;top:10px;right:14px;color:#9ca3af;'
        'font-size:13px;">'
        f'{_esc(labels["other"])}</a>'
    )

    # Admin-only CTA: deep-link to the invite page so the admin can
    # invite their first colleague without leaving the tour.
    invite_cta = ''
    if resolved['admin_screen']:
        invite_label_en = 'Invite your first colleague'
        invite_label_fr = 'Inviter votre premier(ère) collègue'
        invite_label = invite_label_fr if lang_key == 'fr' else invite_label_en
        invite_cta = (
            f'<p style="margin-top:1.2rem;">'
            f'<a href="/cp/{_esc(user_token)}/admin" '
            'class="tour-invite-cta" '
            'style="display:inline-block;background:#f3f4f6;'
            'border:1px solid #1e40af;color:#1e40af;padding:10px 20px;'
            'border-radius:4px;text-decoration:none;font-weight:bold;">'
            f'{_esc(invite_label)} &rarr;</a></p>'
        )

    return (
        '<!DOCTYPE html><html lang="' + lang_key + '">'
        '<head><meta charset="utf-8">'
        f'<title>{_esc(title)}</title>'
        '<style>body{font-family:system-ui,Arial;max-width:640px;'
        'margin:2rem auto;padding:1rem;}'
        '.card{background:white;border:1px solid #e5e7eb;padding:2rem;'
        'border-radius:8px;position:relative;}</style></head><body>'
        '<div class="card" data-tour-screen="portal_user" '
        f'data-tour-step="{step}" data-tour-total="{total}" '
        f'data-tour-role="{_esc(role)}" '
        f'data-tour-lang="{lang_key}">'
        f'{switcher}'
        f'<div style="color:#9ca3af;font-size:13px;">{step_label}</div>'
        f'<h2 style="margin:6px 0 4px;">{title}</h2>'
        f'<div style="color:#6b7280;margin-bottom:1rem;">{subtitle}</div>'
        f'<p style="line-height:1.6;">{body_text}</p>'
        f'{invite_cta}'
        '<div style="margin-top:2rem;text-align:right;">'
        f'{prev_html}{next_btn}{skip}'
        '</div></div></body></html>'
    )


def portal_user_tour_completed(
    db_path: Path | str, *, user_id: int,
) -> bool:
    """Return True when this portal user has seen (or skipped) the tour."""
    with _open(db_path) as conn:
        row = conn.execute(
            "SELECT first_tour_completed_at FROM client_portal_users "
            "WHERE id=?",
            (user_id,),
        ).fetchone()
    if not row:
        return False
    return bool(row['first_tour_completed_at'])


def mark_portal_user_tour_completed(
    db_path: Path | str, *, user_id: int,
) -> None:
    with _open(db_path) as conn:
        conn.execute(
            "UPDATE client_portal_users "
            "SET first_tour_completed_at=COALESCE(first_tour_completed_at, ?) "
            "WHERE id=?",
            (_iso_now(), user_id),
        )
        conn.commit()


def render_target_user_dropdown(
    users: list[dict[str, Any]], *,
    selected_id: int | None = None,
    broadcast_label: str = 'All (broadcast)',
) -> str:
    """Return the HTML ``<select>`` for the CPA message-send form.

    Expects an already-filtered list (caller applied ``include_removed=False``).
    Active users are selectable; suspended users are shown greyed-out
    (disabled); removed users are filtered out before they reach here."""
    if not users:
        return ''
    options = [
        f'<option value=""{"" if selected_id else " selected"}>'
        f'&mdash; {_esc(broadcast_label)} &mdash;</option>'
    ]
    for u in users:
        uid = u.get('id')
        status = (u.get('status') or '').lower()
        if status == 'removed':
            continue
        role = (u.get('role') or '').capitalize()
        label_name = u.get('full_name') or u.get('email') or ''
        last_active = u.get('last_active_at') or 'never'
        disabled = ''
        prefix = ''
        if status == 'suspended':
            disabled = ' disabled'
            prefix = '[suspended] '
        sel = ' selected' if selected_id and uid == selected_id else ''
        options.append(
            f'<option value="{uid}"{disabled}{sel} '
            f'title="last active {_esc(last_active)}">'
            f'{_esc(prefix)}{_esc(label_name)} ({_esc(role)})'
            '</option>'
        )
    return (
        '<select name="target_portal_user_id">'
        + ''.join(options)
        + '</select>'
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


def resolve_invite_lang(
    *,
    qs_lang: str | None = None,
    accept_language_header: str | None = None,
    invitation_lang: str | None = None,
) -> str:
    """Decide which language to render the /invite/... page in.

    Precedence (highest wins):
    1. explicit ?lang=fr|en query string,
    2. invitation row's stored ``invited_language`` when set,
    3. the browser's first-preferred ``Accept-Language`` fragment,
    4. English fallback.
    """
    if qs_lang and qs_lang.lower() in ('fr', 'en'):
        return qs_lang.lower()
    if invitation_lang and invitation_lang.lower() in ('fr', 'en'):
        return invitation_lang.lower()
    hdr = (accept_language_header or '').strip().lower()
    if hdr:
        first = hdr.split(',', 1)[0].split(';', 1)[0].strip()
        if first.startswith('fr'):
            return 'fr'
        if first.startswith('en'):
            return 'en'
    return 'en'


_INVITE_STRINGS = {
    'en': {
        'title': 'Accept invitation',
        'heading': 'You are invited to upload receipts to {firm}',
        'intro': (
            'Hi {name} — <strong>{client}</strong> uses OtoCPA to submit '
            'receipts and invoices to <strong>{firm}</strong>. You have '
            'been invited as <strong>{role}</strong>.'
        ),
        'expiry': 'Accept the invitation to get your personal upload link. '
                   'This invitation expires on {expires}.',
        'button': 'Accept invitation',
        'other_lang_label': 'Français',
        'footer': (
            "If you were not expecting this invitation, you can safely "
            "ignore this page."
        ),
    },
    'fr': {
        'title': "Accepter l'invitation",
        'heading': 'Vous êtes invité(e) à téléverser des reçus pour {firm}',
        'intro': (
            'Bonjour {name} — <strong>{client}</strong> utilise OtoCPA pour '
            'soumettre reçus et factures à <strong>{firm}</strong>. Vous '
            'avez été invité(e) en tant que <strong>{role}</strong>.'
        ),
        'expiry': 'Acceptez l\'invitation pour obtenir votre lien personnel '
                   "de téléversement. Cette invitation expire le {expires}.",
        'button': "Accepter l'invitation",
        'other_lang_label': 'English',
        'footer': (
            "Si vous n'attendiez pas cette invitation, vous pouvez ignorer "
            "cette page sans risque."
        ),
    },
}


def render_accept_invitation_page(
    inv: dict[str, Any], *, client_name: str,
    firm_name: str = '', lang: str = 'en',
) -> str:
    """Render the public /invite/{token} acceptance page in ``lang``.

    Shows a language toggle top-right so a recipient can flip locales
    without re-requesting with a new ?lang= arg (the link preserves the
    invitation_token query)."""
    lang_key = 'fr' if (lang or '').lower().startswith('fr') else 'en'
    strings = _INVITE_STRINGS[lang_key]
    other_lang = 'en' if lang_key == 'fr' else 'fr'

    name = _esc(inv.get('full_name') or inv.get('email') or '')
    role = _esc(inv.get('invited_role') or 'contributor')
    client = _esc(client_name or inv.get('client_code') or '')
    firm = _esc(firm_name or inv.get('firm_code') or '')
    tok = _esc(inv.get('invitation_token') or '')
    expires = _esc(inv.get('expires_at') or '')

    heading = strings['heading'].format(firm=firm)
    intro = strings['intro'].format(name=name, client=client, firm=firm,
                                       role=role)
    expiry = strings['expiry'].format(expires=expires)
    title = strings['title']
    button = strings['button']
    other_lang_label = strings['other_lang_label']
    footer = strings['footer']

    return (
        '<!DOCTYPE html><html lang="' + lang_key + '">'
        '<head><meta charset="utf-8">'
        f'<title>{_esc(title)}</title>'
        '<style>body{font-family:system-ui,Arial;max-width:560px;'
        'margin:3rem auto;padding:1rem;position:relative;}'
        '.card{background:#f9fafb;border:1px solid #e5e7eb;padding:1.5rem;'
        'border-radius:8px;}'
        'button.primary{background:#1e40af;color:white;padding:12px 24px;'
        'border:none;border-radius:6px;font-size:16px;cursor:pointer;}'
        '.lang-toggle{position:absolute;top:8px;right:12px;'
        'color:#6b7280;font-size:13px;text-decoration:none;}'
        '.lang-toggle:hover{color:#1e40af;}'
        '</style></head><body>'
        '<a class="lang-toggle" '
        f'href="/invite/{tok}?lang={other_lang}" '
        f'data-testid="lang-toggle">{_esc(other_lang_label)}</a>'
        '<div class="card" data-tour-lang="' + lang_key + '">'
        f'<h1>{heading}</h1>'
        f'<p>{intro}</p>'
        f'<p>{expiry}</p>'
        f'<form method="POST" action="/invite/{tok}/accept">'
        f'<input type="hidden" name="lang" value="{lang_key}">'
        f'<button class="primary" type="submit">{_esc(button)}</button>'
        '</form>'
        f'<p style="color:#6b7280;font-size:12px;margin-top:1.5rem;">{footer}</p>'
        '</div></body></html>'
    )
