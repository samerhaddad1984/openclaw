"""
QuickBooks Online OAuth helpers.

Sprint 3 (multi-client): tokens are stored per (firm_code, client_code) in the
``qbo_connections`` table so CPA firms can manage many QBO realms — one per
client — from a single OtoCPA instance.

The legacy single-connection ``data/qbo_config.json`` remains readable so old
deploys don't crash on import, but it is no longer written to. Operators must
reconnect each client via /qbo/connect?client_code=X. See review_dashboard.py
bootstrap for the migration warning emitted when the legacy file still exists.
"""
from __future__ import annotations

import os
import json
import time
import base64
import sqlite3
import urllib.request
import urllib.parse
import urllib.error
import secrets
import logging
from pathlib import Path
from typing import Any, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

# Kept for backwards compatibility with code that still reads from the legacy
# single-realm config. New code must NOT write to this file.
CONFIG_PATH = ROOT_DIR / "data" / "qbo_config.json"

INTUIT_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"


# ---------------------------------------------------------------------------
# Legacy single-connection config (read-only in Sprint 3+)
# ---------------------------------------------------------------------------

def load_config() -> dict[str, Any]:
    """Read the legacy data/qbo_config.json if present.

    .. deprecated:: Sprint 3
        Tokens are now stored per-client in the ``qbo_connections`` table.
        This helper remains only so pre-Sprint-3 code paths keep importing.
    """
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def legacy_config_exists() -> bool:
    """True when the pre-Sprint-3 single-realm config file is still on disk."""
    return CONFIG_PATH.exists()


# ---------------------------------------------------------------------------
# DB helper
# ---------------------------------------------------------------------------

def _open_db(db_path: Path | str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_table(db_path: Path | str | None = None) -> None:
    """Guarantee qbo_connections exists (tests use fresh DBs)."""
    with _open_db(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS qbo_connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT NOT NULL,
                client_code TEXT NOT NULL,
                realm_id TEXT NOT NULL,
                access_token TEXT NOT NULL,
                refresh_token TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                connected_at TEXT DEFAULT (datetime('now')),
                connected_by TEXT,
                last_refreshed_at TEXT,
                last_error TEXT,
                status TEXT DEFAULT 'active',
                UNIQUE(firm_code, client_code)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_qbo_firm_client "
            "ON qbo_connections(firm_code, client_code)"
        )
        conn.commit()


# ---------------------------------------------------------------------------
# OAuth URL + CSRF state helpers
# ---------------------------------------------------------------------------

def build_state(client_code: str, csrf_nonce: str) -> str:
    """Pack (client_code, csrf_nonce) into the opaque Intuit ``state`` param.

    Intuit echoes ``state`` back on the callback; the callback decodes it to
    recover which client_code this OAuth round-trip was started for and to
    check the CSRF nonce against the session cookie.
    """
    raw = f"{client_code}.{csrf_nonce}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_state(state: str) -> tuple[Optional[str], Optional[str]]:
    """Inverse of :func:`build_state`. Returns ``(None, None)`` on any error."""
    if not state:
        return None, None
    try:
        padded = state + "=" * (-len(state) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        client_code, _, csrf_nonce = raw.partition(".")
        if not client_code or not csrf_nonce:
            return None, None
        return client_code, csrf_nonce
    except Exception:
        return None, None


def get_authorize_url(client_code: str, csrf_nonce: Optional[str] = None) -> tuple[str, str]:
    """Return ``(auth_url, csrf_nonce)``. Caller stores ``csrf_nonce`` in its
    session cookie and verifies it on the callback."""
    client_id = os.environ.get('QBO_CLIENT_ID', '')
    redirect_uri = os.environ.get('QBO_REDIRECT_URI', 'https://app.otocpa.com/qbo/callback')
    nonce = csrf_nonce or secrets.token_urlsafe(24)
    state = build_state(client_code, nonce)

    params = {
        'client_id': client_id,
        'response_type': 'code',
        'scope': 'com.intuit.quickbooks.accounting',
        'redirect_uri': redirect_uri,
        'state': state,
    }
    url = 'https://appcenter.intuit.com/connect/oauth2?' + urllib.parse.urlencode(params)
    return url, nonce


# ---------------------------------------------------------------------------
# Intuit token endpoint (separated for easy mocking in tests)
# ---------------------------------------------------------------------------

def _post_to_intuit_token_endpoint(form_fields: dict[str, str]) -> dict[str, Any]:
    client_id = os.environ.get('QBO_CLIENT_ID', '')
    client_secret = os.environ.get('QBO_CLIENT_SECRET', '')
    credentials = base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode()

    data = urllib.parse.urlencode(form_fields).encode()
    req = urllib.request.Request(
        INTUIT_TOKEN_URL,
        data=data,
        headers={
            'Authorization': f'Basic {credentials}',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def exchange_code_for_token(code: str, redirect_uri: Optional[str] = None) -> dict[str, Any]:
    """Exchange an OAuth ``code`` for a token bundle. Does NOT persist."""
    return _post_to_intuit_token_endpoint({
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': redirect_uri or os.environ.get(
            'QBO_REDIRECT_URI', 'https://app.otocpa.com/qbo/callback'
        ),
    })


# ---------------------------------------------------------------------------
# Per-client token storage (Sprint 3)
# ---------------------------------------------------------------------------

def store_qbo_tokens(
    firm_code: str,
    client_code: str,
    realm_id: str,
    access_token: str,
    refresh_token: str,
    expires_in: int,
    connected_by: Optional[str] = None,
    db_path: Path | str | None = None,
) -> None:
    """UPSERT tokens for (firm_code, client_code).

    ``expires_in`` is seconds-from-now (Intuit returns this as-is). Stored as an
    absolute epoch-second ``expires_at`` so refresh logic is a pure comparison.
    """
    _ensure_table(db_path)
    expires_at = int(time.time()) + int(expires_in or 0)
    with _open_db(db_path) as conn:
        conn.execute(
            """
            INSERT INTO qbo_connections
                (firm_code, client_code, realm_id, access_token,
                 refresh_token, expires_at, connected_by, status, last_error)
            VALUES (?,?,?,?,?,?,?, 'active', NULL)
            ON CONFLICT(firm_code, client_code) DO UPDATE SET
                realm_id       = excluded.realm_id,
                access_token   = excluded.access_token,
                refresh_token  = excluded.refresh_token,
                expires_at     = excluded.expires_at,
                connected_by   = excluded.connected_by,
                connected_at   = datetime('now'),
                status         = 'active',
                last_error     = NULL
            """,
            (firm_code, client_code, realm_id, access_token,
             refresh_token, expires_at, connected_by),
        )
        conn.commit()
    logging.info("QBO tokens stored for firm=%s client=%s realm=%s",
                 firm_code, client_code, realm_id)


def _mark_expired(firm_code: str, client_code: str, error_text: str,
                  db_path: Path | str | None = None) -> None:
    with _open_db(db_path) as conn:
        conn.execute(
            "UPDATE qbo_connections SET status='expired', last_error=? "
            "WHERE firm_code=? AND client_code=?",
            (error_text[:500], firm_code, client_code),
        )
        conn.commit()


def refresh_access_token(
    firm_code: str,
    client_code: str,
    db_path: Path | str | None = None,
) -> Optional[dict[str, Any]]:
    """Use the refresh token to mint a new access token.

    Returns the updated row-as-dict on success, or ``None`` if there is no
    connection / refresh fails (in which case the row is marked ``expired``
    and ``last_error`` is populated).
    """
    _ensure_table(db_path)
    with _open_db(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM qbo_connections WHERE firm_code=? AND client_code=?",
            (firm_code, client_code),
        ).fetchone()
    if row is None:
        return None

    try:
        token_data = _post_to_intuit_token_endpoint({
            'grant_type': 'refresh_token',
            'refresh_token': row['refresh_token'],
        })
    except Exception as exc:
        _mark_expired(firm_code, client_code, f"refresh_failed: {exc}", db_path)
        logging.warning("QBO refresh failed firm=%s client=%s: %s",
                        firm_code, client_code, exc)
        return None

    new_access = token_data.get('access_token')
    if not new_access:
        _mark_expired(firm_code, client_code,
                      f"refresh_bad_response: {token_data}", db_path)
        return None

    new_refresh = token_data.get('refresh_token') or row['refresh_token']
    expires_at = int(time.time()) + int(token_data.get('expires_in', 3600))
    with _open_db(db_path) as conn:
        conn.execute(
            "UPDATE qbo_connections SET access_token=?, refresh_token=?, "
            "expires_at=?, last_refreshed_at=datetime('now'), "
            "status='active', last_error=NULL "
            "WHERE firm_code=? AND client_code=?",
            (new_access, new_refresh, expires_at, firm_code, client_code),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM qbo_connections WHERE firm_code=? AND client_code=?",
            (firm_code, client_code),
        ).fetchone()
    return dict(row) if row else None


def get_qbo_tokens(
    firm_code: str,
    client_code: str,
    db_path: Path | str | None = None,
    refresh_window_seconds: int = 300,
) -> Optional[dict[str, Any]]:
    """Return a dict with fresh tokens for the given (firm, client), or None.

    Auto-refreshes when ``expires_at`` is within ``refresh_window_seconds`` of
    now. If the row is already ``status != 'active'`` we still return it so
    callers can surface the error state, but without refreshing.
    """
    _ensure_table(db_path)
    with _open_db(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM qbo_connections WHERE firm_code=? AND client_code=?",
            (firm_code, client_code),
        ).fetchone()
    if row is None:
        return None

    data = dict(row)
    if data.get('status') != 'active':
        return data

    now = int(time.time())
    if int(data.get('expires_at') or 0) - now < refresh_window_seconds:
        refreshed = refresh_access_token(firm_code, client_code, db_path)
        if refreshed is None:
            # Refresh failed — re-read so caller sees the updated status
            with _open_db(db_path) as conn:
                row = conn.execute(
                    "SELECT * FROM qbo_connections WHERE firm_code=? AND client_code=?",
                    (firm_code, client_code),
                ).fetchone()
            return dict(row) if row else None
        return refreshed
    return data


def disconnect_qbo(
    firm_code: str,
    client_code: str,
    db_path: Path | str | None = None,
) -> bool:
    """Remove the connection. Returns True if a row was deleted."""
    _ensure_table(db_path)
    with _open_db(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM qbo_connections WHERE firm_code=? AND client_code=?",
            (firm_code, client_code),
        )
        conn.commit()
        return cur.rowcount > 0


def list_qbo_connections(
    firm_code: Optional[str] = None,
    db_path: Path | str | None = None,
) -> list[dict[str, Any]]:
    """List connections for ``firm_code``; owner may pass None to see all.

    Each row is joined against ``clients`` so the UI can show
    ``client_name`` without a second query.
    """
    _ensure_table(db_path)
    with _open_db(db_path) as conn:
        if firm_code is None:
            rows = conn.execute(
                "SELECT qc.*, c.client_name AS client_name "
                "FROM qbo_connections qc "
                "LEFT JOIN clients c ON c.client_code = qc.client_code "
                "ORDER BY qc.firm_code, qc.client_code"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT qc.*, c.client_name AS client_name "
                "FROM qbo_connections qc "
                "LEFT JOIN clients c ON c.client_code = qc.client_code "
                "WHERE qc.firm_code=? ORDER BY qc.client_code",
                (firm_code,),
            ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Legacy callers (kept so pre-Sprint-3 code doesn't crash at import time)
# ---------------------------------------------------------------------------

def is_connected() -> bool:
    """Legacy helper: True if the legacy config has tokens or any per-client
    connection exists at all. New code should call :func:`get_qbo_tokens`."""
    config = load_config()
    if config.get('access_token') and config.get('realm_id'):
        return True
    try:
        _ensure_table()
        with _open_db() as conn:
            row = conn.execute(
                "SELECT 1 FROM qbo_connections WHERE status='active' LIMIT 1"
            ).fetchone()
        return row is not None
    except Exception:
        return False
