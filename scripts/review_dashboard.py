from __future__ import annotations

import hashlib
import html
import bcrypt
import json
import secrets
import sqlite3
import sys
import time
import traceback
import urllib.parse
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.learning_memory_store import LearningMemoryStore
from src.agents.core.learning_suggestion_engine import LearningSuggestionEngine
from src.agents.tools.explain_decision_formatter import build_human_decision_summary
from src.agents.tools.posting_builder import (
    approve_posting_job,
    build_posting_job,
    retry_posting_job,
)
from src.agents.tools.qbo_online_adapter import post_one_ready_job as qbo_post_one_ready_job
from src.agents.core.period_close import (
    ensure_period_close_tables,
    get_document_period,
    get_lock_info,
    get_or_create_period_checklist,
    generate_period_close_pdf,
    is_period_complete,
    is_period_locked,
    lock_period,
    update_checklist_item,
)
from src.engines.tax_engine import calculate_gst_qst, generate_filing_summary, validate_tax_code
from src.agents.core.time_tracker import (
    ensure_time_tables,
    get_time_summary,
    start_time_entry,
    stop_time_entry,
)
from src.agents.core.invoice_generator import generate_invoice_number, generate_invoice_pdf
from src.agents.core.revenu_quebec import (
    compute_prefill,
    ensure_client_config_table,
    get_client_config,
    generate_revenu_quebec_pdf,
    set_client_config,
)
from src.engines.bank_parser import (
    apply_manual_match as bank_apply_manual_match,
    import_statement as bank_import_statement,
)
from src.i18n import t
from src.agents.core import client_comms as _client_comms
from src.agents.core.filing_calendar import (
    ensure_filing_tables as _ensure_filing_tables,
    get_upcoming_deadlines as _get_upcoming_deadlines,
    mark_as_filed as _mark_as_filed,
    period_label_to_dates as _period_label_to_dates,
)
import src.engines.audit_engine as _audit
from src.engines.license_engine import get_license_status, save_license_to_config, check_limits, get_signing_secret, TIER_DEFAULTS


DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
LOG_PATH = ROOT_DIR / "data" / "ledgerlink.log"
HOST = "127.0.0.1"
PORT = 8787
DEFAULT_REVIEWER = "Sam"
SESSION_DURATION_HOURS = 12

_SERVICE_START = datetime.now(timezone.utc)

learning_store = LearningMemoryStore()
suggestion_engine = LearningSuggestionEngine(DB_PATH)


# ---------------------------------------------------------------------------
# Role config (permissions only — clients come from DB)
# ---------------------------------------------------------------------------

ROLE_CONFIG: dict[str, dict[str, Any]] = {
    "owner": {
        "can_view_all_clients": True,
        "can_view_all_assignments": True,
        "can_assign": True,
        "can_post": True,
        "can_manage_team": True,
    },
    "manager": {
        "can_view_all_clients": True,
        "can_view_all_assignments": True,
        "can_assign": True,
        "can_post": True,
        "can_manage_team": True,
    },
    "employee": {
        "can_view_all_clients": False,
        "can_view_all_assignments": False,
        "can_assign": False,
        "can_post": False,
        "can_manage_team": False,
    },
}


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().replace(microsecond=0).isoformat()


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_optional_text(value: Any) -> str | None:
    text = normalize_text(value)
    return text if text else None


def normalize_amount_input(value: Any) -> float | None:
    text = normalize_text(value)
    if not text:
        return None
    try:
        return round(float(text.replace(",", "")), 2)
    except Exception:
        return None


def normalize_key(value: Any) -> str:
    return " ".join(normalize_text(value).casefold().split())


def get_user_lang(user: dict[str, Any] | None) -> str:
    """Return the user's preferred language (``'fr'`` or ``'en'``).

    Falls back to ``'fr'`` (Quebec default) for unauthenticated requests or
    rows that have no ``language`` column yet.
    """
    if not user:
        return "fr"
    lang = normalize_text(user.get("language") or "fr")
    return lang if lang in ("fr", "en") else "fr"


# Mapping from internal status strings → i18n keys (for display only;
# the stored/URL values stay in English so filters keep working).
_STATUS_LABEL_KEYS: dict[str, str] = {
    "Needs Review":  "stat_needs_review",
    "On Hold":       "stat_on_hold",
    "Ready to Post": "stat_ready_to_post",
    "Posted":        "stat_posted",
    "Ignored":       "filter_ignored",
}

_NEXT_ACTION_KEYS: dict[str, str] = {
    "None":         "action_none",
    "Claim":        "action_claim",
    "View":         "action_view",
    "Review":       "action_review",
    "Resolve Hold": "action_resolve_hold",
    "Post":         "action_post",
    "Approve":      "action_approve",
}


def safe_json_loads(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        loaded = json.loads(str(value))
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def urlquote(value: Any) -> str:
    return urllib.parse.quote("" if value is None else str(value), safe="")


def parse_form_body(raw: bytes) -> dict[str, str]:
    parsed = urllib.parse.parse_qs(raw.decode("utf-8"), keep_blank_values=True)
    return {k: v[0] if v else "" for k, v in parsed.items()}


def _parse_multipart_simple(
    raw: bytes, content_type: str
) -> tuple[dict[str, str], bytes, str]:
    """Minimal multipart/form-data parser. Returns (fields, file_bytes, filename)."""
    fields: dict[str, str] = {}
    file_bytes = b""
    filename = ""

    boundary_str = ""
    for part in content_type.split(";"):
        part = part.strip()
        if part.startswith("boundary="):
            boundary_str = part[len("boundary="):].strip().strip('"')
            break
    if not boundary_str:
        return fields, file_bytes, filename

    boundary = ("--" + boundary_str).encode()
    parts = raw.split(boundary)

    for chunk in parts[1:]:
        if chunk in (b"--\r\n", b"--", b"--\r\n--"):
            continue
        if chunk.startswith(b"--"):
            continue
        if b"\r\n\r\n" not in chunk:
            continue
        header_block, body = chunk.split(b"\r\n\r\n", 1)
        if body.endswith(b"\r\n"):
            body = body[:-2]

        headers_raw = header_block.decode("utf-8", errors="replace")
        disp = ""
        for line in headers_raw.splitlines():
            if line.lower().startswith("content-disposition"):
                disp = line
        if 'filename="' in disp:
            fn_start = disp.index('filename="') + len('filename="')
            fn_end = disp.index('"', fn_start)
            filename = disp[fn_start:fn_end]
            file_bytes = body
        elif 'name="' in disp:
            n_start = disp.index('name="') + len('name="')
            n_end = disp.index('"', n_start)
            name = disp[n_start:n_end]
            fields[name] = body.decode("utf-8", errors="replace")

    return fields, file_bytes, filename



def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify password against stored hash. Supports bcrypt and legacy SHA-256."""
    if stored_hash.startswith(("$2b$", "$2a$", "$2y$")):
        return bcrypt.checkpw(password.encode(), stored_hash.encode())
    # Legacy SHA-256 fallback (transition only)
    return hashlib.sha256(password.encode()).hexdigest() == stored_hash


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------

def bootstrap_schema() -> None:
    with open_db() as conn:
        # Auth tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_users (
                username      TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                role          TEXT NOT NULL DEFAULT 'employee',
                display_name  TEXT,
                active        INTEGER NOT NULL DEFAULT 1,
                language      TEXT NOT NULL DEFAULT 'fr',
                created_at    TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_sessions (
                session_token TEXT PRIMARY KEY,
                username      TEXT NOT NULL,
                expires_at    TEXT NOT NULL,
                created_at    TEXT
            )
        """)
        # Document assignment tracking
        conn.execute("""
            CREATE TABLE IF NOT EXISTS document_assignments (
                document_id TEXT PRIMARY KEY,
                assigned_to TEXT,
                assigned_by TEXT,
                assigned_at TEXT,
                updated_at  TEXT,
                note        TEXT
            )
        """)
        # Portfolio assignments
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_portfolios (
                username    TEXT NOT NULL,
                client_code TEXT NOT NULL,
                assigned_by TEXT,
                assigned_at TEXT,
                PRIMARY KEY (username, client_code)
            )
        """)
        conn.commit()

        # Add columns to documents if missing
        existing = {row["name"] for row in conn.execute("PRAGMA table_info(documents)").fetchall()}
        for col in ["assigned_to", "manual_hold_reason", "manual_hold_by", "manual_hold_at"]:
            if col not in existing:
                conn.execute(f"ALTER TABLE documents ADD COLUMN {col} TEXT")
        conn.commit()

        # Add must_reset_password / language columns to dashboard_users if missing
        user_cols = {row["name"] for row in conn.execute("PRAGMA table_info(dashboard_users)").fetchall()}
        if "language" not in user_cols:
            conn.execute("ALTER TABLE dashboard_users ADD COLUMN language TEXT NOT NULL DEFAULT 'fr'")
            conn.commit()
        if "must_reset_password" not in user_cols:
            conn.execute("ALTER TABLE dashboard_users ADD COLUMN must_reset_password INTEGER NOT NULL DEFAULT 0")
            # Force all existing users to reset — their hashes may be legacy SHA-256
            conn.execute("UPDATE dashboard_users SET must_reset_password = 1")
            conn.commit()
            print("  [bootstrap] must_reset_password=1 set for all existing users (bcrypt migration)")

        # Seed a default admin account if no users exist
        count = conn.execute("SELECT COUNT(*) FROM dashboard_users").fetchone()[0]
        if count == 0:
            conn.execute(
                "INSERT INTO dashboard_users (username, password_hash, role, display_name, active, must_reset_password, created_at) VALUES (?,?,?,?,1,1,?)",
                ("sam", hash_password("admin123"), "owner", "Sam", utc_now_iso()),
            )
            conn.commit()
            print("  [bootstrap] Default user created: sam / admin123  <-- CHANGE THIS PASSWORD")

        # Login attempt tracking (brute-force protection)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS login_attempts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ip_address   TEXT NOT NULL,
                username     TEXT NOT NULL,
                attempted_at TEXT NOT NULL,
                success      INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.commit()

        # Client communications table
        _client_comms.ensure_comms_table(conn)

        # Audit tables (working papers, evidence, trial balance, engagements)
        _audit.ensure_audit_tables(conn)
        _audit.seed_chart_of_accounts(conn)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def create_session(username: str) -> str:
    token = secrets.token_hex(32)
    expires = (utc_now() + timedelta(hours=SESSION_DURATION_HOURS)).isoformat()
    with open_db() as conn:
        conn.execute(
            "INSERT INTO dashboard_sessions (session_token, username, expires_at, created_at) VALUES (?,?,?,?)",
            (token, username, expires, utc_now_iso()),
        )
        conn.commit()
    return token


def delete_session(token: str) -> None:
    with open_db() as conn:
        conn.execute("DELETE FROM dashboard_sessions WHERE session_token = ?", (token,))
        conn.commit()


# ---------------------------------------------------------------------------
# Brute-force / rate-limit helpers
# ---------------------------------------------------------------------------

_RATE_LIMIT_WINDOW_MIN   = 15
_RATE_LIMIT_MAX_FAILURES = 5
_RATE_LIMIT_MSG = (
    "Trop de tentatives. Réessayez dans 15 minutes / "
    "Too many attempts. Try again in 15 minutes."
)


def _get_client_ip(handler: BaseHTTPRequestHandler) -> str:
    """Return the real client IP, honouring CF-Connecting-IP / X-Forwarded-For."""
    for hdr in ("CF-Connecting-IP", "X-Forwarded-For"):
        val = handler.headers.get(hdr, "").split(",")[0].strip()
        if val:
            return val
    return handler.client_address[0]


def _is_https(handler: BaseHTTPRequestHandler) -> bool:
    """Detect HTTPS via X-Forwarded-Proto (set by Cloudflare)."""
    return handler.headers.get("X-Forwarded-Proto", "").lower() == "https"


def _session_cookie_attrs(handler: BaseHTTPRequestHandler) -> str:
    """Return cookie security attributes appropriate for the current context."""
    if _is_https(handler):
        return "Secure; SameSite=Strict"
    return "SameSite=Lax"


def record_login_attempt(ip: str, username: str, success: bool) -> None:
    """Record a login attempt in the login_attempts table."""
    with open_db() as conn:
        conn.execute(
            "INSERT INTO login_attempts (ip_address, username, attempted_at, success)"
            " VALUES (?,?,?,?)",
            (ip, username, utc_now_iso(), 1 if success else 0),
        )
        conn.commit()


def is_rate_limited(ip: str) -> bool:
    """Return True if this IP has ≥5 failed attempts in the last 15 minutes."""
    window_start = (
        utc_now() - timedelta(minutes=_RATE_LIMIT_WINDOW_MIN)
    ).replace(microsecond=0).isoformat()
    with open_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM login_attempts"
            " WHERE ip_address=? AND success=0 AND attempted_at>=?",
            (ip, window_start),
        ).fetchone()
    return (row[0] if row else 0) >= _RATE_LIMIT_MAX_FAILURES


def get_session_user(handler: BaseHTTPRequestHandler) -> dict[str, Any] | None:
    cookie = handler.headers.get("Cookie", "")
    token = None
    for part in cookie.split(";"):
        part = part.strip()
        if part.startswith("session_token="):
            token = part[len("session_token="):]
            break
    if not token:
        return None

    with open_db() as conn:
        session = conn.execute(
            "SELECT * FROM dashboard_sessions WHERE session_token = ?", (token,)
        ).fetchone()
        if not session:
            return None
        if session["expires_at"] < utc_now_iso():
            return None
        user = conn.execute(
            "SELECT * FROM dashboard_users WHERE username = ? AND active = 1",
            (session["username"],),
        ).fetchone()
        return dict(user) if user else None


def get_token_from_cookie(handler: BaseHTTPRequestHandler) -> str:
    cookie = handler.headers.get("Cookie", "")
    for part in cookie.split(";"):
        part = part.strip()
        if part.startswith("session_token="):
            return part[len("session_token="):]
    return ""


def get_all_active_users() -> list[dict[str, Any]]:
    with open_db() as conn:
        rows = conn.execute(
            "SELECT username, role, display_name FROM dashboard_users WHERE active = 1 ORDER BY username"
        ).fetchall()
    return [dict(r) for r in rows]


def get_available_usernames() -> list[str]:
    return [u["username"] for u in get_all_active_users()]


# ---------------------------------------------------------------------------
# Portfolio & user context
# ---------------------------------------------------------------------------

def get_portfolio_clients(username: str) -> list[str]:
    try:
        with open_db() as conn:
            rows = conn.execute(
                "SELECT client_code FROM user_portfolios WHERE username = ? ORDER BY client_code",
                (normalize_key(username),),
            ).fetchall()
        return [r["client_code"] for r in rows]
    except Exception:
        return []


def get_all_portfolios() -> dict[str, list[str]]:
    try:
        with open_db() as conn:
            rows = conn.execute(
                "SELECT username, client_code FROM user_portfolios ORDER BY username, client_code"
            ).fetchall()
        result: dict[str, list[str]] = {}
        for r in rows:
            result.setdefault(r["username"], []).append(r["client_code"])
        return result
    except Exception:
        return {}


def get_all_client_codes() -> list[str]:
    try:
        with open_db() as conn:
            rows = conn.execute(
                "SELECT DISTINCT client_code FROM documents WHERE client_code IS NOT NULL AND client_code != '' ORDER BY client_code"
            ).fetchall()
        return [r["client_code"] for r in rows]
    except Exception:
        return []


def assign_client_to_user(client_code: str, username: str, assigned_by: str) -> None:
    with open_db() as conn:
        conn.execute(
            """
            INSERT INTO user_portfolios (username, client_code, assigned_by, assigned_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(username, client_code) DO UPDATE SET
                assigned_by = excluded.assigned_by, assigned_at = excluded.assigned_at
            """,
            (normalize_key(username), client_code, assigned_by, utc_now_iso()),
        )
        conn.commit()


def remove_client_from_user(client_code: str, username: str) -> None:
    with open_db() as conn:
        conn.execute(
            "DELETE FROM user_portfolios WHERE username = ? AND client_code = ?",
            (normalize_key(username), client_code),
        )
        conn.commit()


def move_client_to_user(client_code: str, from_user: str, to_user: str, assigned_by: str) -> None:
    remove_client_from_user(client_code, from_user)
    if normalize_key(to_user) != "unassigned":
        assign_client_to_user(client_code, to_user, assigned_by)


def build_user_context(user: dict[str, Any]) -> dict[str, Any]:
    role = normalize_key(user.get("role") or "employee")
    if role not in ROLE_CONFIG:
        role = "employee"
    base = ROLE_CONFIG[role]
    username = normalize_key(user.get("username") or "")

    db_clients = get_portfolio_clients(username) if role == "employee" else []

    return {
        "username": username,
        "display_name": normalize_text(user.get("display_name") or user.get("username")),
        "role": role,
        "allowed_clients": db_clients,
        "can_view_all_clients": bool(base["can_view_all_clients"]),
        "can_view_all_assignments": bool(base["can_view_all_assignments"]),
        "can_assign": bool(base["can_assign"]),
        "can_post": bool(base["can_post"]),
        "can_manage_team": bool(base["can_manage_team"]),
    }


# ---------------------------------------------------------------------------
# Badge helpers
# ---------------------------------------------------------------------------

def review_status_badge(status: str) -> str:
    s = normalize_text(status)
    css = "badge badge-muted"
    if s in {"Ready", "Ready to Post"}:         css = "badge badge-ready"
    elif s in {"NeedsReview", "Needs Review"}:  css = "badge badge-needsreview"
    elif s in {"On Hold", "Hold"}:              css = "badge badge-hold"
    elif s == "Ignored":                        css = "badge badge-ignored"
    elif s == "Posted":                         css = "badge badge-posted"
    elif s == "Exception":                      css = "badge badge-exception"
    return f'<span class="{css}">{esc(s or "Unknown")}</span>'


def posting_status_badge(status: str) -> str:
    s = normalize_text(status)
    css = "badge badge-muted"
    if s == "posted":         css = "badge badge-posted"
    elif s == "ready_to_post": css = "badge badge-ready"
    elif s == "post_failed":  css = "badge badge-exception"
    elif s == "draft":        css = "badge badge-hold"
    return f'<span class="{css}">{esc(s or "none")}</span>'


def approval_state_badge(status: str) -> str:
    s = normalize_text(status)
    css = "badge badge-muted"
    if s == "approved_for_posting":    css = "badge badge-ready"
    elif s == "pending_human_approval": css = "badge badge-needsreview"
    elif s == "rejected":               css = "badge badge-exception"
    return f'<span class="{css}">{esc(s or "none")}</span>'


def bool_badge(flag: bool, true_text: str = "Yes", false_text: str = "No") -> str:
    if flag:
        return f'<span class="badge badge-ready">{esc(true_text)}</span>'
    return f'<span class="badge badge-muted">{esc(false_text)}</span>'


# ---------------------------------------------------------------------------
# Business logic
# ---------------------------------------------------------------------------

def compute_blocking_issues(row: sqlite3.Row) -> list[str]:
    required = ["vendor", "amount", "document_date", "gl_account", "tax_code", "category", "client_code"]
    return [f for f in required if row[f] is None or str(row[f]).strip() == ""]


def get_accounting_status(row: sqlite3.Row) -> str:
    review_status  = normalize_text(row["review_status"])
    posting_status = normalize_text(row["posting_status"])
    approval_state = normalize_text(row["approval_state"])
    external_id    = normalize_text(row["external_id"])
    hold_reason    = normalize_text(row["manual_hold_reason"])

    if review_status == "Ignored":                                              return "Ignored"
    if external_id or posting_status == "posted":                               return "Posted"
    if hold_reason or approval_state == "pending_human_approval":               return "On Hold"
    if posting_status == "ready_to_post" or approval_state == "approved_for_posting": return "Ready to Post"
    if review_status in {"NeedsReview", "Exception"}:                          return "Needs Review"
    if review_status == "Ready":                                                return "Ready"
    return review_status or "New"


def get_plain_review_reason(row: sqlite3.Row) -> str:
    raw = safe_json_loads(row["raw_result"])
    duplicate    = raw.get("duplicate_result", {})
    vendor_mem   = raw.get("vendor_memory_enrichment", {})
    exc_router   = raw.get("exception_router_result", {})
    auto_app     = raw.get("auto_approval_result", {})

    blocking = compute_blocking_issues(row)
    if blocking:
        return f"Missing required fields: {', '.join(blocking)}"
    if normalize_text(row["manual_hold_reason"]):
        return normalize_text(row["manual_hold_reason"])
    if isinstance(duplicate, dict) and normalize_key(duplicate.get("risk_level")) in {"medium", "high"}:
        return "Possible duplicate"
    if isinstance(exc_router, dict):
        reasons = exc_router.get("reasons", [])
        if "missing_vendor" in reasons:   return "Missing vendor"
        if "missing_amount" in reasons:   return "Missing amount"
        if "duplicate_medium_risk" in reasons: return "Possible duplicate"
    if isinstance(vendor_mem, dict) and vendor_mem.get("flagged_for_review"):
        rr = vendor_mem.get("review_reasons", [])
        if rr:
            first = normalize_text(rr[0])
            return "Unusual amount, review needed" if ("amount_above" in first or "amount_below" in first) else first.replace("_", " ")
    if isinstance(auto_app, dict) and normalize_text(auto_app.get("reason")) == "approve_but_hold":
        return "Approved by system but held for review"
    reviewer = normalize_text(row["posting_reviewer"])
    if reviewer == "ExceptionRouter":
        return "Waiting accounting review"
    if normalize_text(row["approval_state"]) == "pending_human_approval":
        return "Waiting approval"
    return "Review needed"


def get_next_action(row: sqlite3.Row, ctx: dict[str, Any]) -> str:
    status = get_accounting_status(row)
    assigned = normalize_text(row["assigned_to"])
    is_mine = normalize_key(assigned) == normalize_key(ctx["username"])

    if status == "Posted":                                        return "None"
    if not assigned:                                              return "Claim"
    if not is_mine and not ctx["can_assign"]:                     return "View"
    if status == "Needs Review":                                  return "Review"
    if status == "On Hold":                                       return "Resolve Hold"
    if status == "Ready to Post":   return "Post" if ctx["can_post"] else "Approve"
    if status == "Ready":                                         return "Approve"
    return "Review"


def get_status_counts(ctx: dict[str, Any]) -> dict[str, int]:
    rows = get_documents(ctx=ctx, include_ignored=True, limit=5000)
    counts = {"Needs Review": 0, "On Hold": 0, "Ready to Post": 0, "Posted": 0, "Ignored": 0}
    for row in rows:
        s = get_accounting_status(row)
        if s in counts:
            counts[s] += 1
    return counts


# ---------------------------------------------------------------------------
# DB queries
# ---------------------------------------------------------------------------

def get_documents(
    *,
    ctx: dict[str, Any],
    status: str = "",
    q: str = "",
    include_ignored: bool = False,
    only_my_queue: bool = False,
    only_unassigned: bool = False,
    limit: int = 500,
) -> list[sqlite3.Row]:
    where: list[str] = []
    params: list[Any] = []

    if not include_ignored:
        where.append("(d.review_status IS NULL OR d.review_status != 'Ignored')")

    if q:
        like = f"%{q}%"
        where.append("(d.file_name LIKE ? OR d.vendor LIKE ? OR d.client_code LIKE ? OR d.gl_account LIKE ? OR d.category LIKE ?)")
        params.extend([like] * 5)

    if not ctx["can_view_all_clients"]:
        allowed = ctx.get("allowed_clients", [])
        if not allowed:
            return []
        placeholders = ",".join("?" for _ in allowed)
        where.append(f"COALESCE(d.client_code, '') IN ({placeholders})")
        params.extend(allowed)

    if only_my_queue:
        where.append("COALESCE(da.assigned_to, d.assigned_to, '') = ?")
        params.append(ctx["username"])
    elif only_unassigned:
        where.append("COALESCE(da.assigned_to, d.assigned_to, '') = ''")
    elif not ctx["can_view_all_assignments"]:
        where.append("(COALESCE(da.assigned_to, d.assigned_to, '') = '' OR COALESCE(da.assigned_to, d.assigned_to, '') = ?)")
        params.append(ctx["username"])

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT
            d.document_id, d.file_name, d.file_path, d.client_code, d.vendor,
            d.doc_type, d.amount, d.document_date, d.gl_account, d.tax_code,
            d.category, d.review_status, d.confidence, d.raw_result,
            d.created_at, d.updated_at,
            d.assigned_to AS document_assigned_to,
            d.manual_hold_reason, d.manual_hold_by, d.manual_hold_at,
            COALESCE(da.assigned_to, d.assigned_to, '') AS assigned_to,
            da.assigned_by, da.assigned_at, da.note AS assignment_note,
            pj.posting_id, pj.posting_status, pj.approval_state,
            pj.reviewer AS posting_reviewer, pj.external_id,
            pj.payload_json AS posting_payload_json,
            pj.error_text AS posting_error_text
        FROM documents d
        LEFT JOIN document_assignments da ON da.document_id = d.document_id
        LEFT JOIN posting_jobs pj ON pj.document_id = d.document_id
            AND pj.rowid = (
                SELECT pj2.rowid FROM posting_jobs pj2
                WHERE pj2.document_id = d.document_id
                ORDER BY COALESCE(pj2.updated_at, pj2.created_at) DESC, pj2.rowid DESC LIMIT 1
            )
        {where_sql}
        ORDER BY COALESCE(d.updated_at, d.created_at) DESC, d.file_name ASC
        LIMIT ?
    """
    params.append(limit)

    with open_db() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()

    wanted = normalize_key(status)
    if not wanted:
        return list(rows)
    return [r for r in rows if normalize_key(get_accounting_status(r)) == wanted]


def get_document(document_id: str) -> sqlite3.Row | None:
    with open_db() as conn:
        return conn.execute(
            """
            SELECT
                d.document_id, d.file_name, d.file_path, d.client_code, d.vendor,
                d.doc_type, d.amount, d.document_date, d.gl_account, d.tax_code,
                d.category, d.review_status, d.confidence, d.raw_result,
                d.created_at, d.updated_at,
                d.assigned_to AS document_assigned_to,
                d.manual_hold_reason, d.manual_hold_by, d.manual_hold_at,
                COALESCE(d.hallucination_suspected, 0) AS hallucination_suspected,
                d.raw_ocr_text,
                COALESCE(d.correction_count, 0) AS correction_count,
                COALESCE(da.assigned_to, d.assigned_to, '') AS assigned_to,
                da.assigned_by, da.assigned_at, da.note AS assignment_note,
                pj.posting_id, pj.posting_status, pj.approval_state,
                pj.reviewer AS posting_reviewer, pj.external_id,
                pj.payload_json AS posting_payload_json,
                pj.error_text AS posting_error_text
            FROM documents d
            LEFT JOIN document_assignments da ON da.document_id = d.document_id
            LEFT JOIN posting_jobs pj ON pj.document_id = d.document_id
                AND pj.rowid = (
                    SELECT pj2.rowid FROM posting_jobs pj2
                    WHERE pj2.document_id = d.document_id
                    ORDER BY COALESCE(pj2.updated_at, pj2.created_at) DESC, pj2.rowid DESC LIMIT 1
                )
            WHERE d.document_id = ?
            """,
            (document_id,),
        ).fetchone()


def update_document_fields(document_id: str, fields: dict[str, Any]) -> None:
    allowed = {"vendor","client_code","doc_type","amount","document_date","gl_account",
               "tax_code","category","review_status","manual_hold_reason","manual_hold_by","manual_hold_at"}
    updates, params = [], []
    for k, v in fields.items():
        if k not in allowed:
            continue
        params.append(normalize_amount_input(v) if k == "amount" else normalize_optional_text(v))
        updates.append(f"{k} = ?")
    if not updates:
        return
    updates.append("updated_at = ?")
    params.extend([utc_now_iso(), document_id])
    with open_db() as conn:
        conn.execute(f"UPDATE documents SET {', '.join(updates)} WHERE document_id = ?", tuple(params))
        conn.commit()


def set_document_status(document_id: str, review_status: str) -> None:
    with open_db() as conn:
        conn.execute("UPDATE documents SET review_status = ?, updated_at = ? WHERE document_id = ?",
                     (review_status, utc_now_iso(), document_id))
        conn.commit()


def set_manual_hold(document_id: str, hold_reason: str, username: str) -> None:
    with open_db() as conn:
        conn.execute(
            "UPDATE documents SET review_status='NeedsReview', manual_hold_reason=?, manual_hold_by=?, manual_hold_at=?, updated_at=? WHERE document_id=?",
            (normalize_optional_text(hold_reason), normalize_optional_text(username), utc_now_iso(), utc_now_iso(), document_id),
        )
        conn.commit()


def clear_manual_hold(document_id: str) -> None:
    with open_db() as conn:
        conn.execute(
            "UPDATE documents SET manual_hold_reason=NULL, manual_hold_by=NULL, manual_hold_at=NULL, updated_at=? WHERE document_id=?",
            (utc_now_iso(), document_id),
        )
        conn.commit()


def assign_document(document_id: str, assigned_to: str, assigned_by: str, note: str = "") -> None:
    clean = normalize_optional_text(assigned_to)
    now = utc_now_iso()
    with open_db() as conn:
        conn.execute(
            """INSERT INTO document_assignments (document_id, assigned_to, assigned_by, assigned_at, updated_at, note)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(document_id) DO UPDATE SET
                   assigned_to=excluded.assigned_to, assigned_by=excluded.assigned_by,
                   assigned_at=excluded.assigned_at, updated_at=excluded.updated_at, note=excluded.note""",
            (document_id, clean, normalize_optional_text(assigned_by), now, now, normalize_optional_text(note)),
        )
        conn.execute("UPDATE documents SET assigned_to=?, updated_at=? WHERE document_id=?", (clean, now, document_id))
        conn.commit()


def get_qbo_posting_job(document_id: str) -> sqlite3.Row | None:
    with open_db() as conn:
        return conn.execute(
            "SELECT * FROM posting_jobs WHERE document_id=? AND target_system='qbo' ORDER BY COALESCE(updated_at,created_at) DESC, rowid DESC LIMIT 1",
            (document_id,),
        ).fetchone()


def record_learning_corrections(document_id: str, before_row: sqlite3.Row, updated_fields: dict[str, Any]) -> None:
    tracked = {"vendor","client_code","doc_type","amount","document_date","gl_account","tax_code","category","review_status"}
    for field, submitted in updated_fields.items():
        if field not in tracked:
            continue
        old = before_row[field] if field in before_row.keys() else None
        new = normalize_amount_input(submitted) if field == "amount" else normalize_optional_text(submitted)
        if (str(old) if old is not None else "") == (str(new) if new is not None else ""):
            continue
        try:
            learning_store.record_correction(
                document_id=document_id, field_name=field, old_value=old,
                new_value=new if new is not None else "",
                reviewer=DEFAULT_REVIEWER,
                correction_context={"source": "review_dashboard", "document_id": document_id},
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Learning history (safe fallback)
# ---------------------------------------------------------------------------

def get_learning_history(document_id: str) -> list[dict[str, Any]]:
    try:
        result = learning_store.get_document_learning_history(document_id)
        if isinstance(result, list):
            return result
    except Exception:
        pass
    for table in ("learning_corrections", "gl_corrections", "field_corrections", "corrections"):
        try:
            with open_db() as conn:
                rows = conn.execute(
                    f"SELECT field_name, old_value, new_value, reviewer, COALESCE(created_at, updated_at, '') AS created_at FROM {table} WHERE document_id=? ORDER BY created_at DESC LIMIT 50",
                    (document_id,),
                ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            continue
    return []


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------

def render_learning_history(document_id: str, lang: str = "fr") -> str:
    history = get_learning_history(document_id)
    title = t("section_learning_history", lang)
    if not history:
        return f'<div class="card"><h3>{esc(title)}</h3><p class="muted">{esc(t("no_corrections", lang))}</p></div>'
    rows_html = "".join(
        f"<tr><td>{esc(i.get('field_name'))}</td><td>{esc(i.get('old_value'))}</td>"
        f"<td>{esc(i.get('new_value'))}</td><td>{esc(i.get('reviewer'))}</td><td>{esc(i.get('created_at'))}</td></tr>"
        for i in history
    )
    th = (f"<tr><th>{esc(t('col_field', lang))}</th><th>{esc(t('col_old', lang))}</th>"
          f"<th>{esc(t('col_new', lang))}</th><th>{esc(t('col_reviewer', lang))}</th>"
          f"<th>{esc(t('col_date', lang))}</th></tr>")
    return f'<div class="card"><h3>{esc(title)}</h3><table><thead>{th}</thead><tbody>{rows_html}</tbody></table></div>'


def render_learning_suggestions(document_id: str, row: sqlite3.Row, username: str,
                                 lang: str = "fr") -> str:
    title = t("section_learning_suggestions", lang)
    try:
        suggestions = suggestion_engine.suggestions_for_document(
            client_code=row["client_code"], vendor=row["vendor"], doc_type=row["doc_type"], limit_per_field=5)
    except Exception:
        suggestions = {}
    if not suggestions:
        return f'<div class="card"><h3>{esc(title)}</h3><p class="muted">{esc(t("no_suggestions", lang))}</p></div>'

    current = {k: normalize_text(row[k]) for k in ["vendor","client_code","doc_type","gl_account","tax_code","category","review_status"]}
    rows_html: list[str] = []
    for field, options in suggestions.items():
        for opt in options:
            if current.get(field, "") == normalize_text(opt["value"]):
                continue
            rows_html.append(f"""<tr><td>{esc(field)}</td><td>{esc(opt["value"])}</td>
                <td>{esc(opt["support"])}</td><td>{esc(opt["confidence"])}</td><td>{esc(opt["source"])}</td>
                <td><form method="POST" action="/apply_suggestion">
                    <input type="hidden" name="document_id" value="{esc(document_id)}">
                    <input type="hidden" name="field" value="{esc(field)}">
                    <input type="hidden" name="value" value="{esc(opt['value'])}">
                    <button class="btn-primary" type="submit">{esc(t("btn_apply", lang))}</button>
                </form></td></tr>""")

    if not rows_html:
        return f'<div class="card"><h3>{esc(title)}</h3><p class="muted">{esc(t("no_suggestions_remaining", lang))}</p></div>'
    th = (f"<tr><th>{esc(t('col_field', lang))}</th><th>{esc(t('col_suggested', lang))}</th>"
          f"<th>{esc(t('col_support', lang))}</th><th>{esc(t('col_confidence', lang))}</th>"
          f"<th>{esc(t('col_source', lang))}</th><th></th></tr>")
    return f'<div class="card"><h3>{esc(title)}</h3><table><thead>{th}</thead><tbody>{"".join(rows_html)}</tbody></table></div>'


def render_posting_readiness(row: sqlite3.Row, lang: str = "fr") -> str:
    blocking = compute_blocking_issues(row)
    title = t("section_posting_readiness", lang)
    if blocking:
        badge = f'<span class="badge badge-exception">{esc(t("badge_blocked", lang))}</span>'
    else:
        badge = f'<span class="badge badge-ready">{esc(t("badge_ready_for_posting", lang))}</span>'
    issues = "".join(f"<li>{esc(x)}</li>" for x in blocking) or f"<li>{esc(t('badge_none', lang))}</li>"
    return f'<div class="card"><h3>{esc(title)}</h3><p>{badge}</p><ul>{issues}</ul></div>'


def render_vendor_memory(raw_result: dict[str, Any], lang: str = "fr") -> str:
    enrichment = raw_result.get("vendor_memory_enrichment")
    title = t("section_vendor_memory", lang)
    if not isinstance(enrichment, dict) or not enrichment:
        return f'<div class="card"><h3>{esc(title)}</h3><p class="muted">{esc(t("no_vendor_memory", lang))}</p></div>'
    flagged = bool(enrichment.get("flagged_for_review"))
    if flagged:
        badge = f'<span class="badge badge-exception">{esc(t("badge_flagged", lang))}</span>'
    else:
        badge = f'<span class="badge badge-ready">{esc(t("badge_ok", lang))}</span>'
    reasons = enrichment.get("review_reasons") or []
    reasons_html = "".join(f"<li>{esc(r)}</li>" for r in reasons) or f"<li>{esc(t('vendor_reasons_none', lang))}</li>"
    show_json = esc(t("show_json", lang))
    return f'<div class="card"><h3>{esc(title)}</h3><p>{badge}</p><ul>{reasons_html}</ul><details><summary>{show_json}</summary><textarea readonly>{esc(json.dumps(enrichment, indent=2))}</textarea></details></div>'


def render_auto_approval(raw_result: dict[str, Any], lang: str = "fr") -> str:
    approval = raw_result.get("auto_approval_result")
    title = t("section_auto_approval", lang)
    if not isinstance(approval, dict) or not approval:
        return f'<div class="card"><h3>{esc(title)}</h3><p class="muted">{esc(t("no_auto_approval", lang))}</p></div>'
    auto_approved = bool(approval.get("auto_approved"))
    if auto_approved:
        badge = f'<span class="badge badge-ready">{esc(t("badge_auto_approved", lang))}</span>'
    else:
        badge = f'<span class="badge badge-needsreview">{esc(t("badge_manual_required", lang))}</span>'
    show_json = esc(t("show_json", lang))
    return f'<div class="card"><h3>{esc(title)}</h3><p>{badge} Score: {esc(approval.get("approval_score"))} — {esc(approval.get("reason"))}</p><details><summary>{show_json}</summary><textarea readonly>{esc(json.dumps(approval, indent=2))}</textarea></details></div>'


def _fraud_severity_badge(severity: str) -> str:
    css_map = {
        "critical": "badge badge-exception",
        "high":     "badge badge-exception",
        "medium":   "badge badge-hold",
        "low":      "badge badge-needsreview",
    }
    css = css_map.get(severity.lower(), "badge badge-muted")
    sev_key = f"fraud_severity_{severity.lower()}"
    label = t(sev_key, "en")  # severity label is always short — resolved below at call site
    return f'<span class="{css}">{esc(label)}</span>'


def render_fraud_flags(row: Any, lang: str = "fr") -> str:
    """Render the fraud flags card for a document detail page.

    *row* is a sqlite3.Row (or dict-like) from the documents table.
    Fraud flags are stored as a JSON list in the ``fraud_flags`` column.
    The function uses t() for every user-visible string so both FR and EN work.
    """
    title = t("fraud_section_title", lang)

    raw_flags: str | None = None
    try:
        raw_flags = row["fraud_flags"]
    except (KeyError, TypeError):
        pass

    if not raw_flags:
        return ""  # nothing to show — no flags, no card

    try:
        flags: list[dict[str, Any]] = json.loads(raw_flags)
    except Exception:
        flags = []

    if not flags:
        return ""  # empty list → no card

    rows_html = ""
    for flag in flags:
        severity   = str(flag.get("severity", "low"))
        i18n_key   = str(flag.get("i18n_key", ""))
        params     = flag.get("params") or {}
        explanation = t(i18n_key, lang, **params)
        sev_key    = f"fraud_severity_{severity.lower()}"
        sev_label  = t(sev_key, lang)
        badge_css  = {
            "critical": "badge badge-exception",
            "high":     "badge badge-exception",
            "medium":   "badge badge-hold",
            "low":      "badge badge-needsreview",
        }.get(severity.lower(), "badge badge-muted")
        rows_html += (
            f'<tr>'
            f'<td><span class="{badge_css}">{esc(sev_label)}</span></td>'
            f'<td>{esc(explanation)}</td>'
            f'</tr>'
        )

    return (
        f'<div class="card" style="border-left: 4px solid #e53e3e;">'
        f'<h3 style="color:#c53030">{esc(title)}</h3>'
        f'<table style="width:100%;border-collapse:collapse;">'
        f'<thead><tr>'
        f'<th style="text-align:left;padding:4px 8px;width:90px">'
        f'{esc(t("col_status", lang))}</th>'
        f'<th style="text-align:left;padding:4px 8px">'
        f'{esc(t("col_reason", lang))}</th>'
        f'</tr></thead>'
        f'<tbody>{rows_html}</tbody>'
        f'</table>'
        f'</div>'
    )


# ---------------------------------------------------------------------------
# Troubleshoot helpers
# ---------------------------------------------------------------------------

def _format_uptime(start: datetime) -> str:
    delta = datetime.now(timezone.utc) - start
    total_seconds = int(delta.total_seconds())
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def _ping_provider(base_url: str, timeout: float = 3.0) -> dict[str, Any]:
    """HTTP GET to base_url root to check reachability; returns latency in ms."""
    import urllib.request as _ureq
    t0 = time.monotonic()
    try:
        req = _ureq.Request(base_url, method="GET")
        with _ureq.urlopen(req, timeout=timeout):
            pass
        return {"ok": True, "latency_ms": int((time.monotonic() - t0) * 1000), "error": ""}
    except Exception as exc:
        return {"ok": False, "latency_ms": int((time.monotonic() - t0) * 1000), "error": str(exc)}


def render_filing_summary(
    ctx: dict[str, Any],
    user: dict[str, Any],
    client_code: str,
    period_start: str,
    period_end: str,
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    """Render the GST/QST filing summary page (manager/owner only)."""
    summary: dict[str, Any] = {}
    error_msg = ""

    if client_code and period_start and period_end:
        summary = generate_filing_summary(client_code, period_start, period_end)
        if "error" in summary:
            error_msg = summary["error"]

    # Build the filter form
    filter_form = f"""
<div class="card">
  <h2>{esc(t("filing_title", lang))}</h2>
  <form method="GET" action="/filing_summary">
    <div class="grid-2">
      <label>{esc(t("filing_client_code", lang))}
        <input type="text" name="client_code" value="{esc(client_code)}" placeholder="{esc(t("filing_client_ph", lang))}" required>
      </label>
      <label>{esc(t("filing_period_start", lang))}
        <input type="date" name="period_start" value="{esc(period_start)}">
      </label>
      <label>{esc(t("filing_period_end", lang))}
        <input type="date" name="period_end" value="{esc(period_end)}">
      </label>
    </div>
    <button class="btn-primary" type="submit">{esc(t("btn_generate", lang))}</button>
  </form>
</div>"""

    if error_msg:
        body = filter_form + f'<div class="card"><p class="error">{esc(error_msg)}</p></div>'
        return page_layout(t("filing_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)

    if not summary:
        return page_layout(t("filing_title", lang), filter_form, user=user, flash=flash, flash_error=flash_error, lang=lang)

    # Summary totals card
    gst_col = summary["gst_collected"]
    qst_col = summary["qst_collected"]
    itc     = summary["itc_available"]
    itr     = summary["itr_available"]
    net_gst = summary["net_gst_payable"]
    net_qst = summary["net_qst_payable"]
    posted  = summary["documents_posted"]
    pending = summary["documents_pending"]
    total   = summary["documents_total"]

    totals_card = f"""
<div class="card">
  <h3>{esc(t("doc_section_summary", lang))} — {esc(client_code)} &nbsp; {esc(period_start)} to {esc(period_end)}</h3>
  <table>
    <tr><td><strong>{esc(t("filing_docs_summary", lang))}</strong></td>
        <td>{posted} / {pending} / {total}</td></tr>
    <tr><td><strong>{esc(t("filing_gst_collected", lang))}</strong></td>    <td>${gst_col}</td></tr>
    <tr><td><strong>{esc(t("filing_qst_collected", lang))}</strong></td>    <td>${qst_col}</td></tr>
    <tr><td><strong>{esc(t("filing_itc", lang))}</strong></td><td>${itc}</td></tr>
    <tr><td><strong>{esc(t("filing_itr", lang))}</strong></td><td>${itr}</td></tr>
    <tr><td><strong>{esc(t("filing_net_gst", lang))}</strong></td>           <td>${net_gst}</td></tr>
    <tr><td><strong>{esc(t("filing_net_qst", lang))}</strong></td>           <td>${net_qst}</td></tr>
  </table>
</div>"""

    # Line items table
    if summary["line_items"]:
        rows_html = ""
        for item in summary["line_items"]:
            if item["is_posted"]:
                status_badge = f'<span class="badge badge-ready">{esc(t("badge_posted_filing", lang))}</span>'
            else:
                status_badge = f'<span class="badge badge-exception">{esc(t("badge_pending", lang))}</span>'
            rows_html += (
                f"<tr>"
                f"<td>{esc(item['document_id'])}</td>"
                f"<td>{esc(item['vendor'])}</td>"
                f"<td>{esc(item['document_date'])}</td>"
                f"<td>{esc(item['tax_code'])}</td>"
                f"<td>${item['amount']}</td>"
                f"<td>${item['gst_recoverable']}</td>"
                f"<td>${item['qst_recoverable']}</td>"
                f"<td>${item['hst_recoverable']}</td>"
                f"<td>{status_badge}</td>"
                f"</tr>"
            )
        line_items_card = f"""
<div class="card">
  <h3>{esc(t("line_items", lang))}</h3>
  <table>
    <thead><tr>
      <th>{esc(t("col_document_id", lang))}</th><th>{esc(t("col_vendor", lang))}</th>
      <th>{esc(t("col_date", lang))}</th><th>Tax Code</th>
      <th>{esc(t("col_amount", lang))}</th><th>{esc(t("col_gst_rec", lang))}</th>
      <th>{esc(t("col_qst_rec", lang))}</th><th>{esc(t("col_hst_rec", lang))}</th>
      <th>{esc(t("col_status", lang))}</th>
    </tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>"""
    else:
        line_items_card = f'<div class="card"><p>{esc(t("filing_no_docs", lang))}</p></div>'

    body = filter_form + totals_card + line_items_card
    return page_layout(t("filing_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Revenu Québec pre-fill
# ---------------------------------------------------------------------------

def render_revenu_quebec(
    ctx: dict[str, Any],
    user: dict[str, Any],
    client_code: str,
    period_start: str,
    period_end: str,
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    """Render the Revenu Québec pre-fill page (owner only)."""
    prefill: dict[str, Any] = {}
    error_msg = ""
    client_cfg: dict[str, Any] = {"quick_method": 0, "quick_method_type": "retail"}

    # Always load the current client config (for the config form)
    if client_code:
        try:
            with open_db() as conn:
                ensure_client_config_table(conn)
                client_cfg = get_client_config(conn, client_code)
        except Exception:
            pass

    if client_code and period_start and period_end:
        try:
            with open_db() as conn:
                ensure_client_config_table(conn)
                prefill = compute_prefill(client_code, period_start, period_end, conn)
                if prefill.get("error"):
                    error_msg = str(prefill["error"])
        except Exception as exc:
            error_msg = str(exc)

    # ---- Filter form ----
    filter_form = f"""
<div class="card">
  <h2>{esc(t("rq_title", lang))}</h2>
  <form method="GET" action="/revenu_quebec">
    <div class="grid-3">
      <label>{esc(t("rq_client_code", lang))}
        <input type="text" name="client_code" value="{esc(client_code)}"
               placeholder="{esc(t("rq_client_ph", lang))}" required>
      </label>
      <label>{esc(t("rq_period_start", lang))}
        <input type="date" name="period_start" value="{esc(period_start)}">
      </label>
      <label>{esc(t("rq_period_end", lang))}
        <input type="date" name="period_end" value="{esc(period_end)}">
      </label>
    </div>
    <button class="btn-primary" type="submit">{esc(t("rq_btn_load", lang))}</button>
  </form>
</div>"""

    # ---- Warning banner (bilingual, always visible) ----
    warning_banner = f"""
<div style="background:#fff8e1;border:2px solid #f59e0b;border-radius:8px;
            padding:14px 18px;margin-bottom:16px;">
  <strong style="color:#92400e;">{esc(t("rq_warning_title", lang))}</strong><br>
  <span style="color:#78350f;font-size:14px;">
    <strong>FR :</strong> {esc(t("rq_warning_text", "fr"))}<br>
    <strong>EN :</strong> {esc(t("rq_warning_text", "en"))}
  </span>
</div>"""

    if error_msg:
        body = filter_form + warning_banner + \
               f'<div class="card"><p class="error">{esc(error_msg)}</p></div>'
        return page_layout(
            t("rq_title", lang), body, user=user,
            flash=flash, flash_error=flash_error, lang=lang,
        )

    if not prefill:
        # Show filter form + config form (if client selected) + warning
        config_card = _render_rq_config_card(client_code, client_cfg, lang)
        body = filter_form + warning_banner + config_card
        return page_layout(
            t("rq_title", lang), body, user=user,
            flash=flash, flash_error=flash_error, lang=lang,
        )

    # ---- Quick Method info card ----
    qm_active = prefill.get("quick_method", False)
    qm_type   = prefill.get("quick_method_type") or "retail"
    if qm_active:
        if qm_type == "services":
            rates_fr = t("rq_quick_method_services", "fr")
            rates_en = t("rq_quick_method_services", "en")
        else:
            rates_fr = t("rq_quick_method_retail", "fr")
            rates_en = t("rq_quick_method_retail", "en")
        qm_card = f"""
<div class="card" style="border-left:4px solid #16a34a;">
  <strong style="color:#15803d;">{esc(t("rq_quick_method_active", lang))}</strong><br>
  <small>FR : {esc(rates_fr)}</small><br>
  <small>EN : {esc(rates_en)}</small><br>
  <small style="color:#6b7280;">{esc(t("rq_quick_method_no_itc", lang))}</small>
</div>"""
    else:
        qm_card = ""

    # ---- Lines table — bilingual labels side by side ----
    def _row(line_num: str, key_fr: str, key_en: str, value: Any, note: str = "") -> str:
        fr_label = t(f"rq_line_{line_num}", "fr")
        en_label = t(f"rq_line_{line_num}", "en")
        note_html = f'<br><small style="color:#6b7280;">{esc(note)}</small>' if note else ""
        return (
            f"<tr>"
            f"<td style='white-space:nowrap;'><strong>{esc(fr_label)}</strong><br>"
            f"<span style='color:#6b7280;font-size:12px;'>{esc(en_label)}</span>"
            f"{note_html}</td>"
            f"<td style='text-align:right;font-family:monospace;font-size:14px;'>"
            f"${float(value):,.2f}</td>"
            f"</tr>"
        )

    note_101 = t("rq_note_sales_not_tracked", lang)
    gst_rows = (
        _row("101", "rq_line_101", "rq_line_101", prefill["line_101"], note=note_101)
        + _row("103", "rq_line_103", "rq_line_103", prefill["line_103"])
        + _row("106", "rq_line_106", "rq_line_106", prefill["line_106"])
        + _row("108", "rq_line_108", "rq_line_108", prefill["line_108"])
    )
    qst_rows = (
        _row("205", "rq_line_205", "rq_line_205", prefill["line_205"])
        + _row("207", "rq_line_207", "rq_line_207", prefill["line_207"])
        + _row("209", "rq_line_209", "rq_line_209", prefill["line_209"])
    )

    gst_section_title = t("rq_gst_section", lang)
    qst_section_title = t("rq_qst_section", lang)

    lines_card = f"""
<div class="card">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
    <h3>{esc(client_code)} — {esc(period_start)} → {esc(period_end)}</h3>
    <a href="/revenu_quebec/pdf?client_code={urlquote(client_code)}&period_start={urlquote(period_start)}&period_end={urlquote(period_end)}"
       class="btn-secondary" style="padding:8px 16px;">
      {esc(t("rq_btn_download_pdf", lang))}
    </a>
  </div>

  <h4 style="color:#1e40af;margin-top:16px;">{esc(gst_section_title)}</h4>
  <table style="width:100%;">
    <tbody>{gst_rows}</tbody>
  </table>

  <h4 style="color:#1e40af;margin-top:16px;">{esc(qst_section_title)}</h4>
  <table style="width:100%;">
    <tbody>{qst_rows}</tbody>
  </table>

  <hr style="margin:16px 0;border:none;border-top:1px solid #e5e7eb;">
  <small style="color:#6b7280;">
    {esc(t("rq_docs_posted", lang))}: {prefill["documents_posted"]} &nbsp;|&nbsp;
    {esc(t("rq_docs_pending", lang))}: {prefill["documents_pending"]} &nbsp;|&nbsp;
    {esc(t("rq_docs_total", lang))}: {prefill["documents_total"]}
  </small>
</div>"""

    config_card = _render_rq_config_card(client_code, client_cfg, lang)
    body = filter_form + warning_banner + qm_card + lines_card + config_card
    return page_layout(
        t("rq_title", lang), body, user=user,
        flash=flash, flash_error=flash_error, lang=lang,
    )


def _render_rq_config_card(
    client_code: str,
    client_cfg: dict[str, Any],
    lang: str,
) -> str:
    """Render the Quick Method configuration card (owner only — page is already restricted)."""
    if not client_code:
        return ""
    qm_checked  = 'checked' if client_cfg.get("quick_method") else ""
    retail_sel  = 'selected' if (client_cfg.get("quick_method_type") or "retail") == "retail" else ""
    svc_sel     = 'selected' if client_cfg.get("quick_method_type") == "services" else ""
    return f"""
<div class="card" style="border-top:3px solid #6366f1;">
  <h4>{esc(t("rq_quick_method", lang))} — {esc(client_code)}</h4>
  <form method="POST" action="/revenu_quebec/set_config">
    <input type="hidden" name="client_code" value="{esc(client_code)}">
    <div style="display:flex;gap:20px;align-items:center;flex-wrap:wrap;">
      <label style="display:flex;gap:8px;align-items:center;">
        <input type="checkbox" name="quick_method" value="1" {qm_checked}>
        {esc(t("rq_quick_method", lang))}
      </label>
      <label>{esc(t("rq_quick_method_type", lang))}
        <select name="quick_method_type">
          <option value="retail" {retail_sel}>{esc(t("rq_type_retail", lang))}</option>
          <option value="services" {svc_sel}>{esc(t("rq_type_services", lang))}</option>
        </select>
      </label>
      <button class="btn-secondary" type="submit">{esc(t("rq_btn_save_config", lang))}</button>
    </div>
    <small style="color:#6b7280;margin-top:8px;display:block;">
      {esc(t("rq_quick_method_retail", lang))} &nbsp;|&nbsp;
      {esc(t("rq_quick_method_services", lang))}
    </small>
  </form>
</div>"""


# ---------------------------------------------------------------------------
# Time tracking summary
# ---------------------------------------------------------------------------

def render_time_summary(
    ctx: dict[str, Any],
    user: dict[str, Any],
    client_code: str,
    period_start: str,
    period_end: str,
    hourly_rate: str,
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    """Render the time tracking summary page (manager/owner only)."""
    summary: dict[str, Any] = {}

    if client_code and period_start and period_end:
        with open_db() as conn:
            summary = get_time_summary(conn, client_code, period_start, period_end)

    rate_val = esc(hourly_rate or "")

    filter_form = f"""
<div class="card">
  <h2>{esc(t("time_title", lang))}</h2>
  <form method="GET" action="/time">
    <div class="grid-2">
      <label>{esc(t("time_client_code", lang))}
        <input type="text" name="client_code" value="{esc(client_code)}" placeholder="{esc(t("pc_client_ph", lang))}">
      </label>
      <label>{esc(t("time_period_start", lang))}
        <input type="date" name="period_start" value="{esc(period_start)}">
      </label>
      <label>{esc(t("time_period_end", lang))}
        <input type="date" name="period_end" value="{esc(period_end)}">
      </label>
      <label>{esc(t("time_hourly_rate", lang))} ($/h)
        <input type="number" step="0.01" min="0" name="hourly_rate" value="{rate_val}" placeholder="150.00">
      </label>
    </div>
    <button class="btn-primary" type="submit">{esc(t("btn_generate", lang))}</button>
  </form>
</div>"""

    if not summary:
        return page_layout(t("time_title", lang), filter_form,
                           user=user, flash=flash, flash_error=flash_error, lang=lang)

    if summary["entry_count"] == 0:
        no_data = f'<div class="card"><p class="muted">{esc(t("time_no_entries", lang))}</p></div>'
        return page_layout(t("time_title", lang), filter_form + no_data,
                           user=user, flash=flash, flash_error=flash_error, lang=lang)

    try:
        rate_decimal = Decimal(str(hourly_rate or "0"))
    except Exception:
        rate_decimal = Decimal("0")

    # Per-user summary table
    by_user_rows = ""
    for uname, stats in sorted(summary["by_user"].items()):
        total_h = round(stats["total_minutes"] / 60, 2)
        billable_h = round(stats["billable_minutes"] / 60, 2)
        est_fee = rate_decimal * Decimal(str(billable_h))
        by_user_rows += (
            f"<tr>"
            f"<td>{esc(uname)}</td>"
            f"<td>{total_h:.2f}</td>"
            f"<td>{billable_h:.2f}</td>"
            f"<td>${est_fee:.2f}</td>"
            f"</tr>"
        )

    total_fee = rate_decimal * Decimal(str(summary["billable_hours"]))
    by_user_rows += (
        f'<tr style="font-weight:600;border-top:2px solid #e5e7eb;">'
        f'<td>{esc(t("inv_total", lang))}</td>'
        f'<td>{summary["total_hours"]:.2f}</td>'
        f'<td>{summary["billable_hours"]:.2f}</td>'
        f'<td>${total_fee:.2f}</td>'
        f'</tr>'
    )

    summary_card = f"""
<div class="card">
  <h3>{esc(client_code)} &mdash; {esc(period_start)} &ndash; {esc(period_end)}</h3>
  <table>
    <thead><tr>
      <th>{esc(t("time_user", lang))}</th>
      <th>{esc(t("time_total_hours", lang))}</th>
      <th>{esc(t("time_billable_hours", lang))}</th>
      <th>{esc(t("time_estimated_fee", lang))} (@ ${rate_decimal:.2f}/h)</th>
    </tr></thead>
    <tbody>{by_user_rows}</tbody>
  </table>
</div>"""

    # Invoice generation form
    invoice_form = f"""
<div class="card">
  <h3>{esc(t("time_generate_invoice", lang))}</h3>
  <form method="POST" action="/invoice/generate">
    <input type="hidden" name="client_code" value="{esc(client_code)}">
    <input type="hidden" name="period_start" value="{esc(period_start)}">
    <input type="hidden" name="period_end" value="{esc(period_end)}">
    <div class="grid-2">
      <label>{esc(t("time_firm_name", lang))}
        <input type="text" name="firm_name" required placeholder="Fiducie Comptable inc.">
      </label>
      <label>{esc(t("time_client_name", lang))}
        <input type="text" name="client_name" required value="{esc(client_code)}">
      </label>
      <label>{esc(t("time_gst_number", lang))}
        <input type="text" name="gst_number" required placeholder="123 456 789 RT 0001">
      </label>
      <label>{esc(t("time_qst_number", lang))}
        <input type="text" name="qst_number" required placeholder="1234567890 TQ 0001">
      </label>
      <label>{esc(t("time_hourly_rate", lang))} ($/h)
        <input type="number" step="0.01" min="0" name="hourly_rate"
               value="{rate_val or '150.00'}" required>
      </label>
    </div>
    <button class="btn-primary" type="submit">{esc(t("time_generate_invoice", lang))}</button>
  </form>
</div>"""

    body = filter_form + summary_card + invoice_form
    return page_layout(t("time_title", lang), body,
                       user=user, flash=flash, flash_error=flash_error, lang=lang)


def _render_folder_watcher_status(lang: str = "fr") -> str:
    """Return an HTML snippet showing the folder watcher status for /troubleshoot."""
    try:
        from scripts.folder_watcher import get_watcher_status
        ws = get_watcher_status()
    except Exception:
        ws = {
            "enabled": False, "inbox_folder": "", "processed_today": 0,
            "last_file": "", "last_file_at": "", "errors": [],
        }

    if ws["enabled"]:
        status_badge = f'<span class="badge badge-ready">{esc(t("fw_status_enabled", lang))}</span>'
    else:
        status_badge = f'<span class="badge badge-exception">{esc(t("fw_status_disabled", lang))}</span>'

    errors_html = ""
    if ws["errors"]:
        items = "".join(
            f"<li><code>{esc(e)}</code></li>" for e in ws["errors"][-5:]
        )
        errors_html = f'<p><strong>{esc(t("fw_recent_errors", lang))}</strong></p><ul>{items}</ul>'

    last_file_str = ws["last_file"] or "—"
    last_at_str   = ws["last_file_at"] or "—"
    folder_str    = ws["inbox_folder"] or t("fw_not_configured", lang)

    return f"""
<div class="grid-2">
  <div>
    <p><strong>{esc(t("fw_status", lang))}</strong> {status_badge}</p>
    <p><strong>{esc(t("fw_folder", lang))}</strong><br><code>{esc(folder_str)}</code></p>
    <p><strong>{esc(t("fw_processed_today", lang))}</strong> {ws["processed_today"]}</p>
  </div>
  <div>
    <p><strong>{esc(t("fw_last_file", lang))}</strong> {esc(last_file_str)}</p>
    <p><strong>{esc(t("fw_last_at", lang))}</strong> {esc(last_at_str)}</p>
    {errors_html}
  </div>
</div>"""


def _render_cloudflare_tunnel_status(lang: str = "fr") -> str:
    """Return an HTML snippet showing Cloudflare tunnel status for /troubleshoot."""
    import subprocess as _sp

    # Service status via sc query (Windows)
    connected = False
    try:
        result = _sp.run(
            ["sc", "query", "cloudflared"],
            capture_output=True, text=True, timeout=5,
        )
        connected = "RUNNING" in result.stdout
    except Exception:
        pass

    # Public URL from config
    public_url = ""
    try:
        cfg_path = ROOT_DIR / "ledgerlink.config.json"
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        public_url = cfg.get("public_portal_url", "")
    except Exception:
        pass

    # Requests today from cloudflared log (best-effort)
    requests_today = "N/A"
    try:
        cf_log = ROOT_DIR / "cloudflare" / "cloudflared.log"
        if cf_log.exists():
            today = datetime.now().strftime("%Y-%m-%d")
            count = sum(
                1 for line in cf_log.read_text(encoding="utf-8", errors="replace").splitlines()
                if today in line and "INF" in line and "request" in line.lower()
            )
            requests_today = str(count)
    except Exception:
        pass

    status_label = t("cf_tunnel_connected", lang) if connected else t("cf_tunnel_disconnected", lang)
    status_css   = "badge-ready" if connected else "badge-exception"
    url_display  = esc(public_url) if public_url else esc(t("cf_tunnel_not_configured", lang))

    return f"""
<div style="display:grid;grid-template-columns:auto 1fr;gap:6px 16px;align-items:center;font-size:14px;">
  <strong>{esc(t("cf_tunnel_status", lang))}</strong>
  <span><span class="badge {status_css}">{esc(status_label)}</span></span>
  <strong>{esc(t("cf_tunnel_url", lang))}</strong>
  <span><code>{url_display}</code></span>
  <strong>{esc(t("cf_tunnel_requests_today", lang))}</strong>
  <span>{esc(requests_today)}</span>
</div>"""


def render_troubleshoot(ctx: dict[str, Any], user: dict[str, Any],
                        flash: str = "", flash_error: str = "", lang: str = "fr") -> str:
    uptime_str = _format_uptime(_SERVICE_START)

    db_size_str = "N/A"
    if DB_PATH.exists():
        size_bytes = DB_PATH.stat().st_size
        if size_bytes < 1024 * 1024:
            db_size_str = f"{size_bytes / 1024:.1f} KB"
        else:
            db_size_str = f"{size_bytes / (1024 * 1024):.2f} MB"

    log_lines = "(log file not found)"
    if LOG_PATH.exists():
        try:
            all_lines = LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
            log_lines = "\n".join(all_lines[-50:]) or "(log file is empty)"
        except Exception as exc:
            log_lines = f"(error reading log: {exc})"

    routine_url = ""
    premium_url = ""
    try:
        cfg_path = ROOT_DIR / "ledgerlink.config.json"
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        routine_url = cfg.get("ai_router", {}).get("routine_provider", {}).get("base_url", "")
        premium_url = cfg.get("ai_router", {}).get("premium_provider", {}).get("base_url", "")
    except Exception:
        pass

    not_configured = {"ok": False, "latency_ms": 0, "error": "not configured"}
    routine_ping = _ping_provider(routine_url) if routine_url else not_configured
    premium_ping = _ping_provider(premium_url) if premium_url else not_configured

    def ping_row(label: str, url: str, result: dict[str, Any]) -> str:
        if result["ok"]:
            badge = f'<span class="badge badge-ready">OK &mdash; {result["latency_ms"]}ms</span>'
        else:
            badge = f'<span class="badge badge-exception">FAIL &mdash; {esc(result["error"])}</span>'
        return (
            f"<tr><td><strong>{esc(label)}</strong></td>"
            f"<td><code>{esc(url or '(not set)')}</code></td>"
            f"<td>{badge}</td></tr>"
        )

    body = f"""
<div class="card">
  <h2>{esc(t("diag_title", lang))}</h2>
  <div class="grid-2">
    <div>
      <p><strong>{esc(t("diag_service_uptime", lang))}</strong> {esc(uptime_str)}</p>
      <p><strong>{esc(t("diag_db_path", lang))}</strong><br><code>{esc(str(DB_PATH))}</code></p>
      <p><strong>{esc(t("diag_db_size", lang))}</strong> {esc(db_size_str)}</p>
    </div>
    <div>
      <p><strong>{esc(t("diag_ai_status", lang))}</strong></p>
      <table>
        <thead><tr><th>{esc(t("diag_col_provider", lang))}</th><th>{esc(t("diag_col_url", lang))}</th><th>{esc(t("diag_col_ping", lang))}</th></tr></thead>
        <tbody>
          {ping_row(t("diag_routine", lang), routine_url, routine_ping)}
          {ping_row(t("diag_premium", lang), premium_url, premium_ping)}
        </tbody>
      </table>
    </div>
  </div>
</div>
<div class="card">
  <h2>{esc(t("diag_actions", lang))}</h2>
  <div class="actions">
    <a href="/troubleshoot/backup" class="button-link btn-primary">{esc(t("btn_backup", lang))}</a>
    <form method="POST" action="/troubleshoot/restart" style="display:inline;"
          onsubmit="return confirm('{esc(t("restart_confirm", lang))}');">
      <button class="btn-danger">{esc(t("btn_restart", lang))}</button>
    </form>
    <a href="/troubleshoot" class="button-link btn-secondary">{esc(t("btn_refresh", lang))}</a>
  </div>
</div>
<div class="card">
  <h2>{esc(t("diag_folder_watcher", lang))}</h2>
  {_render_folder_watcher_status(lang)}
</div>
<div class="card">
  <h2>{esc(t("cf_tunnel_title", lang))}</h2>
  {_render_cloudflare_tunnel_status(lang)}
</div>
<div class="card">
  <h2>{esc(t("diag_log_lines", lang))}</h2>
  <textarea readonly style="height:420px;font-size:12px;">{esc(log_lines)}</textarea>
</div>"""

    return page_layout(t("dashboard_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Period close checklist page
# ---------------------------------------------------------------------------

def render_period_close(
    ctx: dict[str, Any],
    user: dict[str, Any],
    client_code: str,
    period: str,
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    """Render the month-end close checklist page (manager/owner only)."""
    body_parts: list[str] = []

    body_parts.append(f"""<div class="card">
  <h2>{esc(t("pc_title", lang))}</h2>
  <form method="GET" action="/period_close">
    <div class="grid-2">
      <label>{esc(t("doc_field_client", lang))}
        <input type="text" name="client_code" value="{esc(client_code)}"
               placeholder="{esc(t("pc_client_ph", lang))}" required>
      </label>
      <label>{esc(t("pc_period", lang))}
        <input type="month" name="period" value="{esc(period)}">
      </label>
    </div>
    <div style="margin-top:12px;">
      <button class="btn-primary" type="submit">{esc(t("pc_btn_load", lang))}</button>
      <a class="button-link btn-secondary" href="/" style="margin-left:8px;">{esc(t("btn_back_to_queue", lang))}</a>
    </div>
  </form>
</div>""")

    if not (client_code and period):
        return page_layout(t("pc_title", lang), "\n".join(body_parts),
                           user=user, flash=flash, flash_error=flash_error, lang=lang)

    with open_db() as conn:
        items = list(get_or_create_period_checklist(conn, client_code, period))
        complete = is_period_complete(conn, client_code, period)
        locked = is_period_locked(conn, client_code, period)
        lock_info = get_lock_info(conn, client_code, period)

    if locked and lock_info:
        body_parts.append(
            f'<div class="flash success">'
            f'<strong>{esc(t("pc_period_locked", lang))}</strong> &mdash; '
            f'{esc(t("pc_locked_by", lang))}: {esc(lock_info.get("locked_by") or "")} &mdash; '
            f'{esc(t("pc_locked_at", lang))}: {esc(lock_info.get("locked_at") or "")}'
            f'</div>'
        )
        body_parts.append(
            f'<div class="card"><a class="button-link btn-primary" '
            f'href="/period_close/pdf?client_code={urlquote(client_code)}&period={urlquote(period)}">'
            f'{esc(t("pc_btn_download_pdf", lang))}</a></div>'
        )
    elif complete:
        body_parts.append(
            f'<div class="flash success">{esc(t("pc_all_complete_msg", lang))}</div>'
        )

    open_count = sum(1 for it in items if (it["status"] or "open") == "open")
    status_badge_cls = {
        "complete": "badge-ready",
        "waived":   "badge-muted",
        "open":     "badge-needsreview",
    }

    rows_html: list[str] = []
    for item in items:
        iid     = item["id"]
        label   = t(item["checklist_item"], lang)
        status  = item["status"] or "open"
        badge   = status_badge_cls.get(status, "badge-muted")
        st_lbl  = t(f"pc_status_{status}", lang)
        cby     = esc(item["completed_by"] or "")
        cat     = esc(item["completed_at"] or "")
        notes_v = esc(item["notes"] or "")
        resp_v  = esc(item["responsible_user"] or "")

        # Common hidden inputs shared by all forms in this row
        hid = (
            f'<input type="hidden" name="item_id" value="{iid}">'
            f'<input type="hidden" name="client_code" value="{esc(client_code)}">'
            f'<input type="hidden" name="period" value="{esc(period)}">'
        )

        if locked:
            resp_cell    = resp_v
            notes_cell   = notes_v
            actions_cell = ""
        else:
            # Responsible / due-date save form
            resp_cell = (
                f'<form method="POST" action="/period_close/check_item">'
                f'{hid}'
                f'<input type="hidden" name="status" value="{esc(status)}">'
                f'<input type="text" name="responsible_user" value="{resp_v}" '
                f'placeholder="{esc(t("pc_responsible", lang))}" style="width:90px;font-size:12px;">'
                f'<input type="date" name="due_date" value="{esc(item["due_date"] or "")}" '
                f'style="font-size:12px;">'
                f'<button class="btn-secondary" type="submit" '
                f'style="padding:5px 9px;font-size:12px;">{esc(t("btn_save", lang))}</button>'
                f'</form>'
            )

            # Notes save form
            notes_cell = (
                f'<form method="POST" action="/period_close/check_item">'
                f'{hid}'
                f'<input type="hidden" name="status" value="{esc(status)}">'
                f'<input type="text" name="notes" value="{notes_v}" '
                f'placeholder="{esc(t("pc_notes_ph", lang))}" style="width:140px;font-size:12px;">'
                f'<button class="btn-secondary" type="submit" '
                f'style="padding:5px 9px;font-size:12px;">{esc(t("btn_save", lang))}</button>'
                f'</form>'
            )

            def _action_btn(new_status: str, label_key: str, css: str) -> str:
                return (
                    f'<form method="POST" action="/period_close/check_item" style="display:inline;">'
                    f'{hid}'
                    f'<input type="hidden" name="status" value="{new_status}">'
                    f'<button class="{css}" type="submit" style="padding:5px 9px;font-size:12px;">'
                    f'{esc(t(label_key, lang))}</button>'
                    f'</form> '
                )

            actions_cell = ""
            if status != "complete":
                actions_cell += _action_btn("complete", "pc_btn_complete", "btn-success")
            if status != "waived":
                actions_cell += _action_btn("waived", "pc_btn_waive", "btn-warning")
            if status != "open":
                actions_cell += _action_btn("open", "pc_btn_reopen", "btn-secondary")

        rows_html.append(
            f'<tr>'
            f'<td>{esc(label)}</td>'
            f'<td><span class="badge {badge}">{esc(st_lbl)}</span></td>'
            f'<td>{resp_cell}</td>'
            f'<td>{cby}</td>'
            f'<td>{cat}</td>'
            f'<td>{notes_cell}</td>'
            f'<td class="actions">{actions_cell}</td>'
            f'</tr>'
        )

    status_msg = (
        f'<p class="muted">{open_count} {esc(t("pc_items_remaining", lang))}</p>'
        if not locked and open_count > 0 else ""
    )

    close_btn = ""
    if not locked and complete:
        close_btn = (
            f'<div style="margin-top:16px;">'
            f'<form method="POST" action="/period_close/lock">'
            f'<input type="hidden" name="client_code" value="{esc(client_code)}">'
            f'<input type="hidden" name="period" value="{esc(period)}">'
            f'<button class="btn-danger" type="submit" '
            f'onclick="return confirm(\'{esc(t("pc_close_period_confirm", lang))}\')">'
            f'{esc(t("pc_btn_close_period", lang))}</button>'
            f'</form></div>'
        )

    th_labels = [
        t("pc_col_item", lang),
        t("pc_col_status", lang),
        f'{t("pc_responsible", lang)} / {t("pc_col_due_date", lang)}',
        t("pc_completed_by", lang),
        t("pc_completed_at", lang),
        t("col_note", lang),
        t("col_actions_header", lang),
    ]
    headers = "".join(f"<th>{esc(h)}</th>" for h in th_labels)

    body_parts.append(
        f'<div class="card">'
        f'<h3>{esc(client_code)} &mdash; {esc(period)}</h3>'
        f'{status_msg}'
        f'<table><thead><tr>{headers}</tr></thead>'
        f'<tbody>{"".join(rows_html)}</tbody>'
        f'</table>'
        f'{close_btn}'
        f'</div>'
    )

    return page_layout(t("pc_title", lang), "\n".join(body_parts),
                       user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Page layout
# ---------------------------------------------------------------------------

CSS = """
body{font-family:Arial,Helvetica,sans-serif;margin:0;background:#f5f7fb;color:#111827}
header{background:#111827;color:white;padding:16px 24px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px}
header h1{margin:0;font-size:20px}
.user-pill{font-size:13px;color:#cbd5e1}
main{max-width:1500px;margin:0 auto;padding:20px 24px 40px}
.card{background:white;border:1px solid #e5e7eb;border-radius:10px;padding:16px;margin-bottom:16px;box-shadow:0 1px 2px rgba(0,0,0,.04)}
h1,h2,h3{margin-top:0}
a{color:#1d4ed8;text-decoration:none}
a:hover{text-decoration:underline}
.flash{padding:12px 14px;border-radius:8px;margin-bottom:16px;font-weight:700}
.flash.success{background:#ecfdf5;border:1px solid #a7f3d0;color:#065f46}
.flash.error{background:#fef2f2;border:1px solid #fecaca;color:#991b1b}
.topbar{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;align-items:center}
.stats{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:16px}
.stat{background:white;border:1px solid #e5e7eb;border-radius:10px;padding:12px 14px;min-width:140px}
.small{font-size:12px}
.muted{color:#6b7280}
.filters{display:grid;grid-template-columns:repeat(5,minmax(140px,1fr));gap:12px;align-items:end}
.grid-2{display:grid;grid-template-columns:repeat(2,1fr);gap:16px}
.grid-3{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}
.grid-4{display:grid;grid-template-columns:repeat(4,1fr);gap:16px}
.field{margin-bottom:12px}
label{display:block;margin-bottom:4px;font-size:12px;font-weight:700;color:#374151}
input[type=text],input[type=password],select,textarea{width:100%;box-sizing:border-box;padding:10px 12px;border:1px solid #d1d5db;border-radius:8px;background:white}
textarea{min-height:200px;font-family:Consolas,Monaco,monospace;font-size:12px;overflow:auto}
.summary-box{white-space:pre-wrap;line-height:1.55;background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:14px}
button,.button-link{border:0;border-radius:8px;padding:10px 14px;font-weight:700;cursor:pointer;display:inline-block;text-decoration:none}
.btn-primary{background:#2563eb;color:white}
.btn-success{background:#059669;color:white}
.btn-warning{background:#d97706;color:white}
.btn-danger{background:#dc2626;color:white}
.btn-secondary{background:#e5e7eb;color:#111827}
.btn-dark{background:#111827;color:white}
.actions{display:flex;gap:8px;flex-wrap:wrap}
.inline-form{display:inline-flex;gap:8px;align-items:center;flex-wrap:wrap}
table{width:100%;border-collapse:collapse}
th,td{padding:10px 8px;border-bottom:1px solid #e5e7eb;text-align:left;vertical-align:top;font-size:14px}
th{background:#f9fafb}
.badge{display:inline-block;padding:6px 10px;border-radius:999px;font-size:12px;font-weight:700}
.badge-ready{background:#dcfce7;color:#166534}
.badge-needsreview{background:#fef3c7;color:#92400e}
.badge-hold{background:#fde68a;color:#92400e}
.badge-ignored{background:#e5e7eb;color:#374151}
.badge-exception{background:#fee2e2;color:#991b1b}
.badge-posted{background:#d1fae5;color:#065f46}
.badge-muted{background:#f3f4f6;color:#374151}
.badge-unread{background:#dc2626;color:white;border-radius:999px;padding:2px 8px;font-size:11px;font-weight:700;display:inline-block;vertical-align:middle}
.queue-table td{white-space:nowrap}
.queue-table td.file-cell,.queue-table td.reason-cell{white-space:normal}
details{border:1px solid #e5e7eb;border-radius:8px;padding:10px 12px;background:#fff}
summary{cursor:pointer;font-weight:700}
ul{margin-top:6px}
@media(max-width:1100px){.filters{grid-template-columns:repeat(3,minmax(140px,1fr))}}
@media(max-width:900px){.grid-2,.grid-3,.grid-4,.filters{grid-template-columns:1fr}}
"""


def render_bank_import(
    ctx: dict[str, Any],
    user: dict[str, Any],
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
    result: dict[str, Any] | None = None,
) -> str:
    """Bank statement import page: upload form + results table."""

    # ------------------------------------------------------------------ #
    # Upload form
    # ------------------------------------------------------------------ #
    upload_form = f"""
<div class="card">
  <h2>{esc(t("bank_import_title", lang))}</h2>
  <p style="color:#6b7280;margin-bottom:1rem;">{esc(t("bank_import_upload_hint", lang))}</p>
  <form method="POST" action="/bank_import" enctype="multipart/form-data">
    <div class="grid-2">
      <div class="field">
        <label>{esc(t("bank_import_client_code", lang))}</label>
        <input type="text" name="client_code" placeholder="e.g. ACME" required>
      </div>
      <div class="field">
        <label>{esc(t("bank_import_file", lang))}</label>
        <input type="file" name="statement_file" accept=".pdf,.csv,.txt" required>
      </div>
    </div>
    <button class="btn-primary" type="submit">{esc(t("bank_import_btn_import", lang))}</button>
  </form>
</div>"""

    results_html = ""

    if result is not None:
        if not result.get("transactions"):
            results_html = f'<div class="card"><p>{esc(t("bank_import_no_txns", lang))}</p></div>'
        else:
            bank_name = esc(result.get("bank_name") or "Unknown")
            total = result.get("transaction_count", 0)
            matched = result.get("matched_count", 0)
            unmatched = result.get("unmatched_count", 0)

            # Summary bar
            summary_bar = f"""
<div class="card">
  <div class="grid-3" style="gap:0.5rem;">
    <div class="stat-box"><div class="stat-label">{esc(t("bank_import_bank_name", lang))}</div>
      <div class="stat-value">{bank_name}</div></div>
    <div class="stat-box"><div class="stat-label">{esc(t("bank_import_total", lang))}</div>
      <div class="stat-value">{total}</div></div>
    <div class="stat-box"><div class="stat-label">{esc(t("bank_import_matched", lang))}</div>
      <div class="stat-value" style="color:#16a34a;">{matched}</div></div>
  </div>
</div>"""

            # Errors
            errors = result.get("errors") or []
            errors_html = ""
            if errors:
                err_items = "".join(f"<li>{esc(e)}</li>" for e in errors)
                errors_html = f'<div class="flash error"><ul style="margin:0;padding-left:1.2em;">{err_items}</ul></div>'

            # Transactions table
            rows_html = ""
            for txn in result["transactions"]:
                status = txn.get("review_status", "")
                if status == "Ready":
                    badge = f'<span class="badge badge-green">{esc(t("bank_import_status_ready", lang))}</span>'
                else:
                    badge = f'<span class="badge badge-yellow">{esc(t("bank_import_status_needs_review", lang))}</span>'

                conf = txn.get("match_confidence")
                conf_str = f"{conf:.0%}" if conf else "—"
                matched_doc = txn.get("matched_document_id") or ""
                matched_doc_link = (
                    f'<a href="/document?id={urlquote(matched_doc)}">{esc(matched_doc[:16])}…</a>'
                    if matched_doc else "—"
                )
                reason = esc(txn.get("match_reason") or "")

                debit = txn.get("debit")
                credit = txn.get("credit")
                balance = txn.get("balance")
                debit_str = f"${debit:,.2f}" if debit is not None else ""
                credit_str = f"${credit:,.2f}" if credit is not None else ""
                balance_str = f"${balance:,.2f}" if balance is not None else ""

                doc_id = esc(txn.get("document_id", ""))

                # Manual match form (only for unmatched)
                manual_form = ""
                if status != "Ready":
                    manual_form = f"""
<form method="POST" action="/bank_import/match" style="display:flex;gap:6px;align-items:center;margin-top:4px;">
  <input type="hidden" name="bank_document_id" value="{doc_id}">
  <input type="text" name="invoice_document_id" placeholder="{esc(t("bank_import_manual_match_doc", lang))}"
         style="font-size:12px;padding:3px 6px;width:160px;">
  <button class="btn-secondary" style="padding:3px 8px;font-size:12px;"
          type="submit">{esc(t("bank_import_btn_match", lang))}</button>
</form>"""

                rows_html += f"""<tr>
  <td>{esc(txn.get("txn_date") or "")}</td>
  <td>{esc(txn.get("description") or "")}</td>
  <td style="text-align:right;color:#dc2626;">{debit_str}</td>
  <td style="text-align:right;color:#16a34a;">{credit_str}</td>
  <td style="text-align:right;color:#6b7280;">{balance_str}</td>
  <td>{badge}</td>
  <td style="text-align:right;">{conf_str}</td>
  <td>{matched_doc_link}{manual_form}</td>
  <td style="color:#9ca3af;font-size:12px;">{reason}</td>
</tr>"""

            table = f"""
<div class="card" style="overflow-x:auto;">
  <h3>{esc(t("bank_import_transactions", lang))}
    <span style="font-weight:400;font-size:14px;color:#6b7280;">
      — {unmatched} {esc(t("bank_import_unmatched", lang))}
    </span>
  </h3>
  <table style="width:100%;border-collapse:collapse;font-size:13px;">
    <thead>
      <tr style="background:#f9fafb;">
        <th style="text-align:left;padding:6px 8px;">{esc(t("bank_import_col_date", lang))}</th>
        <th style="text-align:left;padding:6px 8px;">{esc(t("bank_import_col_desc", lang))}</th>
        <th style="text-align:right;padding:6px 8px;">{esc(t("bank_import_col_debit", lang))}</th>
        <th style="text-align:right;padding:6px 8px;">{esc(t("bank_import_col_credit", lang))}</th>
        <th style="text-align:right;padding:6px 8px;">{esc(t("bank_import_col_balance", lang))}</th>
        <th style="padding:6px 8px;">{esc(t("bank_import_col_status", lang))}</th>
        <th style="text-align:right;padding:6px 8px;">{esc(t("bank_import_col_confidence", lang))}</th>
        <th style="padding:6px 8px;">{esc(t("bank_import_col_matched_doc", lang))}</th>
        <th style="padding:6px 8px;">{esc(t("bank_import_col_reason", lang))}</th>
      </tr>
    </thead>
    <tbody>
      {rows_html}
    </tbody>
  </table>
</div>"""

            results_html = errors_html + summary_bar + table

    body = upload_form + results_html
    return page_layout(
        t("bank_import_title", lang), body,
        user=user, flash=flash, flash_error=flash_error, lang=lang,
    )


# ---------------------------------------------------------------------------
# Analytics – data helpers (owner-only, no new tables needed)
# ---------------------------------------------------------------------------

def _analytics_staff_productivity(
    conn: sqlite3.Connection, week_start: str, month_start: str
) -> list[dict[str, Any]]:
    """Per-accountant: docs reviewed this week/month, avg review time, hold/approval rates."""
    staff: dict[str, dict[str, Any]] = {}

    for r in conn.execute(
        "SELECT assigned_to, COUNT(*) AS cnt FROM documents "
        "WHERE assigned_to IS NOT NULL AND assigned_to != '' AND updated_at >= ? "
        "GROUP BY assigned_to",
        (week_start,),
    ).fetchall():
        staff.setdefault(r["assigned_to"], {})["docs_week"] = r["cnt"]

    for r in conn.execute(
        "SELECT assigned_to, COUNT(*) AS cnt FROM documents "
        "WHERE assigned_to IS NOT NULL AND assigned_to != '' AND updated_at >= ? "
        "GROUP BY assigned_to",
        (month_start,),
    ).fetchall():
        staff.setdefault(r["assigned_to"], {})["docs_month"] = r["cnt"]

    try:
        for r in conn.execute(
            "SELECT username, AVG(duration_minutes) AS avg_min FROM time_entries "
            "WHERE ended_at IS NOT NULL AND duration_minutes IS NOT NULL AND duration_minutes > 0 "
            "GROUP BY username"
        ).fetchall():
            staff.setdefault(r["username"], {})["avg_minutes"] = r["avg_min"]
    except sqlite3.OperationalError:
        pass

    for r in conn.execute(
        "SELECT assigned_to, "
        "  SUM(CASE WHEN review_status = 'On Hold' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS hold_rate, "
        "  SUM(CASE WHEN review_status IN ('Ready','Posted') THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS approval_rate "
        "FROM documents "
        "WHERE assigned_to IS NOT NULL AND assigned_to != '' "
        "GROUP BY assigned_to"
    ).fetchall():
        uname = r["assigned_to"]
        staff.setdefault(uname, {})
        staff[uname]["hold_rate"] = r["hold_rate"]
        staff[uname]["approval_rate"] = r["approval_rate"]

    result = []
    for uname, d in staff.items():
        result.append({
            "username": uname,
            "docs_week": d.get("docs_week", 0),
            "docs_month": d.get("docs_month", 0),
            "avg_minutes": d.get("avg_minutes"),
            "hold_rate": d.get("hold_rate"),
            "approval_rate": d.get("approval_rate"),
        })
    result.sort(key=lambda x: x["docs_month"], reverse=True)
    return result


def _analytics_client_complexity(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Per-client: avg docs/month, avg review time, hold rate, most common hold reason, est. fee."""
    clients: dict[str, dict[str, Any]] = {}

    for r in conn.execute(
        "SELECT client_code, COUNT(*) AS total, "
        "  CAST((julianday('now') - julianday(MIN(created_at))) / 30.0 AS REAL) AS elapsed_months, "
        "  SUM(CASE WHEN review_status = 'On Hold' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS hold_rate "
        "FROM documents "
        "WHERE client_code IS NOT NULL AND client_code != '' "
        "GROUP BY client_code"
    ).fetchall():
        code = r["client_code"]
        elapsed = max(1.0, r["elapsed_months"] or 1.0)
        clients[code] = {
            "avg_docs_month": round(r["total"] / elapsed, 1),
            "hold_rate": r["hold_rate"],
        }

    seen_reasons: set[str] = set()
    for r in conn.execute(
        "SELECT client_code, manual_hold_reason, COUNT(*) AS cnt FROM documents "
        "WHERE client_code IS NOT NULL AND client_code != '' "
        "  AND manual_hold_reason IS NOT NULL AND manual_hold_reason != '' "
        "GROUP BY client_code, manual_hold_reason ORDER BY cnt DESC"
    ).fetchall():
        code = r["client_code"]
        if code in clients and code not in seen_reasons:
            clients[code]["common_reason"] = r["manual_hold_reason"]
            seen_reasons.add(code)

    try:
        for r in conn.execute(
            "SELECT client_code, AVG(duration_minutes) AS avg_min, AVG(hourly_rate) AS avg_rate "
            "FROM time_entries "
            "WHERE client_code IS NOT NULL AND client_code != '' "
            "  AND ended_at IS NOT NULL AND duration_minutes IS NOT NULL AND duration_minutes > 0 "
            "GROUP BY client_code"
        ).fetchall():
            code = r["client_code"]
            if code not in clients:
                continue
            clients[code]["avg_minutes"] = r["avg_min"]
            rate = r["avg_rate"] or 0.0
            if rate > 0:
                avg_monthly_docs = clients[code].get("avg_docs_month", 0.0)
                avg_min_per_doc = r["avg_min"] or 0.0
                monthly_hours = avg_monthly_docs * avg_min_per_doc / 60.0
                clients[code]["est_fee"] = monthly_hours * rate
    except sqlite3.OperationalError:
        pass

    result = []
    for code, d in clients.items():
        result.append({
            "client_code": code,
            "avg_docs_month": d.get("avg_docs_month", 0.0),
            "avg_minutes": d.get("avg_minutes"),
            "hold_rate": d.get("hold_rate"),
            "common_reason": d.get("common_reason"),
            "est_fee": d.get("est_fee"),
        })
    result.sort(key=lambda x: x["avg_docs_month"], reverse=True)
    return result


def _analytics_monthly_trends(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Document counts per month for the last 12 months."""
    rows = conn.execute(
        "SELECT strftime('%Y-%m', created_at) AS month, COUNT(*) AS cnt "
        "FROM documents "
        "WHERE created_at >= date('now', '-12 months') "
        "GROUP BY month ORDER BY month"
    ).fetchall()
    return [{"month": r["month"], "count": r["cnt"]} for r in rows]


def _analytics_fraud_summary(conn: sqlite3.Connection, month_start: str) -> dict[str, int]:
    """Count fraud flags for documents created this month by severity."""
    counts: dict[str, int] = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    try:
        rows = conn.execute(
            "SELECT fraud_flags FROM documents "
            "WHERE fraud_flags IS NOT NULL AND fraud_flags NOT IN ('', 'null', '[]', '{}') "
            "  AND created_at >= ?",
            (month_start,),
        ).fetchall()
        for r in rows:
            try:
                flags = json.loads(r["fraud_flags"])
                if isinstance(flags, list):
                    for f in flags:
                        sev = str(f.get("severity", "")).lower()
                        if sev in counts:
                            counts[sev] += 1
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass
    except sqlite3.OperationalError:
        pass
    return counts


def _analytics_deadlines_at_risk(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Clients with checklist due dates in the next 14 days who still have open documents."""
    result = []
    try:
        rows = conn.execute(
            "SELECT pc.client_code, pc.period, MIN(pc.due_date) AS earliest_deadline, "
            "  COUNT(DISTINCT d.document_id) AS open_docs "
            "FROM period_close pc "
            "JOIN documents d ON d.client_code = pc.client_code "
            "  AND d.review_status IN ('NeedsReview', 'Needs Review', 'On Hold', 'Exception') "
            "WHERE pc.due_date >= date('now') AND pc.due_date <= date('now', '+14 days') "
            "  AND pc.status = 'open' "
            "GROUP BY pc.client_code, pc.period "
            "ORDER BY earliest_deadline"
        ).fetchall()
        for r in rows:
            result.append({
                "client_code": r["client_code"],
                "period": r["period"],
                "earliest_deadline": r["earliest_deadline"],
                "open_docs": r["open_docs"],
            })
    except sqlite3.OperationalError:
        pass
    return result


# ---------------------------------------------------------------------------
# Analytics page renderer
# ---------------------------------------------------------------------------

def render_analytics(
    ctx: dict[str, Any],
    user: dict[str, Any],
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    """Owner-only firm performance analytics dashboard."""
    now = datetime.now(timezone.utc)
    month_start = now.strftime("%Y-%m-01")
    week_start = (now - timedelta(days=7)).strftime("%Y-%m-%d")

    with open_db() as conn:
        staff = _analytics_staff_productivity(conn, week_start, month_start)
        clients_data = _analytics_client_complexity(conn)
        trends = _analytics_monthly_trends(conn)
        fraud = _analytics_fraud_summary(conn, month_start)
        deadlines = _analytics_deadlines_at_risk(conn)

    # ------------------------------------------------------------------ #
    # Section 1: Staff Productivity
    # ------------------------------------------------------------------ #
    if staff:
        staff_rows_html = ""
        for s in staff:
            avg_min_str = (
                f"{s['avg_minutes']:.1f}\u00a0{esc(t('analytics_minutes_abbr', lang))}"
                if s["avg_minutes"] is not None
                else esc(t("analytics_na", lang))
            )
            hold_str = f"{s['hold_rate']:.1f}%" if s["hold_rate"] is not None else esc(t("analytics_na", lang))
            appr_str = f"{s['approval_rate']:.1f}%" if s["approval_rate"] is not None else esc(t("analytics_na", lang))
            staff_rows_html += (
                f"<tr>"
                f"<td><strong>{esc(s['username'])}</strong></td>"
                f"<td style='text-align:right;'>{s['docs_week']}</td>"
                f"<td style='text-align:right;'>{s['docs_month']}</td>"
                f"<td style='text-align:right;'>{avg_min_str}</td>"
                f"<td style='text-align:right;'>{hold_str}</td>"
                f"<td style='text-align:right;'>{appr_str}</td>"
                f"</tr>\n"
            )
        staff_section = (
            f'<div class="card">'
            f'<h2>{esc(t("analytics_staff_title", lang))}</h2>'
            f'<div style="overflow-x:auto;">'
            f'<table>'
            f'<thead><tr>'
            f'<th>{esc(t("analytics_col_accountant", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("analytics_col_docs_week", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("analytics_col_docs_month", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("analytics_col_avg_review_min", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("analytics_col_hold_rate", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("analytics_col_approval_rate", lang))}</th>'
            f'</tr></thead>'
            f'<tbody>{staff_rows_html}</tbody>'
            f'</table></div></div>'
        )
    else:
        staff_section = (
            f'<div class="card"><h2>{esc(t("analytics_staff_title", lang))}</h2>'
            f'<p class="muted">{esc(t("analytics_no_staff", lang))}</p></div>'
        )

    # ------------------------------------------------------------------ #
    # Section 2: Client Complexity
    # ------------------------------------------------------------------ #
    if clients_data:
        client_rows_html = ""
        for c in clients_data:
            avg_min_str = f"{c['avg_minutes']:.1f}" if c["avg_minutes"] is not None else esc(t("analytics_na", lang))
            hold_str = f"{c['hold_rate']:.1f}%" if c["hold_rate"] is not None else esc(t("analytics_na", lang))
            fee_str = f"${c['est_fee']:,.2f}" if c.get("est_fee") is not None else esc(t("analytics_na", lang))
            reason_str = esc(c["common_reason"]) if c.get("common_reason") else '<span class="muted">\u2014</span>'
            client_rows_html += (
                f"<tr>"
                f"<td><strong>{esc(c['client_code'])}</strong></td>"
                f"<td style='text-align:right;'>{c['avg_docs_month']}</td>"
                f"<td style='text-align:right;'>{avg_min_str}</td>"
                f"<td style='text-align:right;'>{hold_str}</td>"
                f"<td>{reason_str}</td>"
                f"<td style='text-align:right;'>{fee_str}</td>"
                f"</tr>\n"
            )
        client_section = (
            f'<div class="card">'
            f'<h2>{esc(t("analytics_client_title", lang))}</h2>'
            f'<div style="overflow-x:auto;">'
            f'<table>'
            f'<thead><tr>'
            f'<th>{esc(t("analytics_col_client", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("analytics_col_avg_docs_month", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("analytics_col_avg_review_min_client", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("analytics_col_hold_rate_client", lang))}</th>'
            f'<th>{esc(t("analytics_col_common_reason", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("analytics_col_est_fee", lang))}</th>'
            f'</tr></thead>'
            f'<tbody>{client_rows_html}</tbody>'
            f'</table></div></div>'
        )
    else:
        client_section = (
            f'<div class="card"><h2>{esc(t("analytics_client_title", lang))}</h2>'
            f'<p class="muted">{esc(t("analytics_no_clients", lang))}</p></div>'
        )

    # ------------------------------------------------------------------ #
    # Section 3: Monthly Trends (Chart.js bar chart)
    # ------------------------------------------------------------------ #
    trend_labels = json.dumps([r["month"] for r in trends])
    trend_data = json.dumps([r["count"] for r in trends])
    chart_label = esc(t("analytics_trends_title", lang)).replace("'", "\\'")
    trends_section = (
        f'<div class="card">'
        f'<h2>{esc(t("analytics_trends_title", lang))}</h2>'
        f'<canvas id="analytics-monthly-chart" style="max-height:320px;"></canvas>'
        f'<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"'
        f' crossorigin="anonymous" referrerpolicy="no-referrer"></script>'
        f'<script>'
        f'(function(){{'
        f'  var el=document.getElementById("analytics-monthly-chart");'
        f'  new Chart(el,{{'
        f'    type:"bar",'
        f'    data:{{'
        f'      labels:{trend_labels},'
        f'      datasets:[{{'
        f'        label:"{chart_label}",'
        f'        data:{trend_data},'
        f'        backgroundColor:"#2563eb",'
        f'        borderRadius:4'
        f'      }}]'
        f'    }},'
        f'    options:{{'
        f'      responsive:true,'
        f'      plugins:{{legend:{{display:false}}}},'
        f'      scales:{{y:{{beginAtZero:true,ticks:{{stepSize:1}}}}}}'
        f'    }}'
        f'  }});'
        f'}})();'
        f'</script>'
        f'</div>'
    )

    # ------------------------------------------------------------------ #
    # Section 4: Fraud Summary
    # ------------------------------------------------------------------ #
    severity_order = ["critical", "high", "medium", "low"]
    severity_colors = {
        "critical": "#dc2626",
        "high": "#d97706",
        "medium": "#2563eb",
        "low": "#6b7280",
    }
    total_fraud = sum(fraud.values())
    if total_fraud > 0:
        fraud_rows_html = ""
        for sev in severity_order:
            cnt = fraud.get(sev, 0)
            color = severity_colors[sev]
            label_key = f"fraud_severity_{sev}"
            fraud_rows_html += (
                f"<tr>"
                f'<td><span class="badge" style="background:{color}20;color:{color};">'
                f"{esc(t(label_key, lang))}</span></td>"
                f"<td style='text-align:right;font-weight:700;'>{cnt}</td>"
                f"</tr>\n"
            )
        fraud_section = (
            f'<div class="card">'
            f'<h2>{esc(t("analytics_fraud_title", lang))}</h2>'
            f'<table style="max-width:340px;">'
            f'<thead><tr>'
            f'<th>{esc(t("analytics_col_severity", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("analytics_col_count", lang))}</th>'
            f'</tr></thead>'
            f'<tbody>{fraud_rows_html}</tbody>'
            f'</table></div>'
        )
    else:
        fraud_section = (
            f'<div class="card"><h2>{esc(t("analytics_fraud_title", lang))}</h2>'
            f'<p class="muted">{esc(t("analytics_no_fraud", lang))}</p></div>'
        )

    # ------------------------------------------------------------------ #
    # Section 5: Filing Deadlines at Risk
    # ------------------------------------------------------------------ #
    if deadlines:
        deadline_rows_html = ""
        for d in deadlines:
            deadline_rows_html += (
                f"<tr>"
                f"<td><strong>{esc(d['client_code'])}</strong></td>"
                f"<td>{esc(d['period'] or '')}</td>"
                f"<td>{esc(d['earliest_deadline'] or '')}</td>"
                f"<td style='text-align:right;'>{d['open_docs']}</td>"
                f"</tr>\n"
            )
        deadlines_section = (
            f'<div class="card">'
            f'<h2>{esc(t("analytics_deadlines_title", lang))}</h2>'
            f'<div style="overflow-x:auto;">'
            f'<table>'
            f'<thead><tr>'
            f'<th>{esc(t("analytics_col_client", lang))}</th>'
            f'<th>{esc(t("analytics_col_period", lang))}</th>'
            f'<th>{esc(t("analytics_col_deadline", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("analytics_col_open_docs", lang))}</th>'
            f'</tr></thead>'
            f'<tbody>{deadline_rows_html}</tbody>'
            f'</table></div></div>'
        )
    else:
        deadlines_section = (
            f'<div class="card"><h2>{esc(t("analytics_deadlines_title", lang))}</h2>'
            f'<p class="muted">{esc(t("analytics_no_deadlines", lang))}</p></div>'
        )

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("analytics_h1", lang))}</h2>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n'
        f'{staff_section}\n'
        f'{client_section}\n'
        f'{trends_section}\n'
        f'<div class="grid-2">{fraud_section}{deadlines_section}</div>'
    )
    return page_layout(
        t("analytics_title", lang), body,
        user=user, flash=flash, flash_error=flash_error, lang=lang,
    )


# ---------------------------------------------------------------------------
# Filing Calendar
# ---------------------------------------------------------------------------

def render_calendar(
    ctx: dict[str, Any],
    user: dict[str, Any],
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    from datetime import date as _date

    with open_db() as conn:
        _ensure_filing_tables(conn)
        deadlines = _get_upcoming_deadlines(conn, as_of=_date.today(), days_ahead=90)

    freq_labels = {
        "monthly":   t("cal_freq_monthly",   lang),
        "quarterly": t("cal_freq_quarterly",  lang),
        "annual":    t("cal_freq_annual",     lang),
    }

    rows_html = ""
    for d in deadlines:
        days   = d["days_until"]
        is_filed = d["is_filed"]

        # Row background colour
        if is_filed:
            bg = "background:#f3f4f6;"   # grey
        elif days < 14:
            bg = "background:#fef2f2;"   # red
        elif days <= 30:
            bg = "background:#fffbeb;"   # yellow
        else:
            bg = "background:#f0fdf4;"   # green

        # Badge
        if is_filed:
            badge = (
                f'<span style="background:#e5e7eb;color:#374151;padding:2px 8px;'
                f'border-radius:10px;font-size:12px;">'
                f'{esc(t("cal_status_filed", lang))}</span>'
            )
        elif days < 14:
            badge = (
                f'<span style="background:#fee2e2;color:#991b1b;padding:2px 8px;'
                f'border-radius:10px;font-size:12px;">'
                f'{days}d</span>'
            )
        elif days <= 30:
            badge = (
                f'<span style="background:#fef3c7;color:#92400e;padding:2px 8px;'
                f'border-radius:10px;font-size:12px;">'
                f'{days}d</span>'
            )
        else:
            badge = (
                f'<span style="background:#dcfce7;color:#166534;padding:2px 8px;'
                f'border-radius:10px;font-size:12px;">'
                f'{days}d</span>'
            )

        # Pull GST/QST ready amounts + pending docs count for unfiled rows
        gst_ready = qst_ready = ""
        docs_pending_count = ""
        if not is_filed:
            try:
                period_start, period_end = _period_label_to_dates(
                    d["period_label"], d.get("fiscal_year_end", "12-31")
                )
                summary = generate_filing_summary(
                    d["client_code"], period_start, period_end, DB_PATH
                )
                gst_ready = f'${float(summary.get("itc_available", 0)):,.2f}'
                qst_ready = f'${float(summary.get("itr_available", 0)):,.2f}'
                docs_pending_count = str(summary.get("documents_pending", 0))
            except Exception:
                gst_ready = qst_ready = docs_pending_count = "—"

        # Status / filed-at label
        if is_filed and d.get("filed_at"):
            status_html = (
                f'<small class="muted">'
                f'{esc(t("cal_filed_at", lang))}: {esc(d["filed_at"][:10])}'
                f'</small>'
            )
        else:
            status_html = badge

        # Action cell
        if is_filed:
            action_html = ""
        else:
            action_html = (
                f'<form method="POST" action="/calendar/mark_filed" style="display:inline;">'
                f'<input type="hidden" name="client_code"  value="{esc(d["client_code"])}">'
                f'<input type="hidden" name="period_label" value="{esc(d["period_label"])}">'
                f'<input type="hidden" name="deadline"     value="{esc(d["deadline"])}">'
                f'<button class="btn-secondary" style="padding:4px 10px;font-size:12px;">'
                f'{esc(t("cal_mark_filed", lang))}</button>'
                f'</form>'
            )

        freq_lbl = esc(freq_labels.get(d["frequency"], d["frequency"]))

        rows_html += (
            f'<tr style="{bg}">'
            f'<td><strong>{esc(d["client_code"])}</strong></td>'
            f'<td>{esc(d["period_label"])}</td>'
            f'<td>{freq_lbl}</td>'
            f'<td>{esc(d["deadline"])}</td>'
            f'<td style="text-align:center;">{status_html}</td>'
            f'<td style="text-align:right;">{esc(gst_ready)}</td>'
            f'<td style="text-align:right;">{esc(qst_ready)}</td>'
            f'<td style="text-align:right;">{esc(docs_pending_count)}</td>'
            f'<td>{action_html}</td>'
            f'</tr>\n'
        )

    if rows_html:
        table_html = (
            f'<div style="overflow-x:auto;">'
            f'<table>'
            f'<thead><tr>'
            f'<th>{esc(t("cal_col_client",      lang))}</th>'
            f'<th>{esc(t("cal_col_period",       lang))}</th>'
            f'<th>{esc(t("cal_col_frequency",    lang))}</th>'
            f'<th>{esc(t("cal_col_deadline",     lang))}</th>'
            f'<th style="text-align:center;">{esc(t("cal_col_status",      lang))}</th>'
            f'<th style="text-align:right;">{esc(t("cal_col_gst_amount",  lang))}</th>'
            f'<th style="text-align:right;">{esc(t("cal_col_qst_amount",  lang))}</th>'
            f'<th style="text-align:right;">{esc(t("cal_col_docs_pending",lang))}</th>'
            f'<th>{esc(t("cal_col_action",       lang))}</th>'
            f'</tr></thead>'
            f'<tbody>{rows_html}</tbody>'
            f'</table></div>'
        )
    else:
        table_html = f'<p class="muted">{esc(t("cal_no_deadlines", lang))}</p>'

    # Legend
    legend_html = (
        f'<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:16px;font-size:13px;">'
        f'<span style="background:#f0fdf4;border:1px solid #bbf7d0;padding:2px 10px;border-radius:8px;">'
        f'&#9632; {esc(t("cal_legend_green",  lang))}</span>'
        f'<span style="background:#fffbeb;border:1px solid #fde68a;padding:2px 10px;border-radius:8px;">'
        f'&#9632; {esc(t("cal_legend_yellow", lang))}</span>'
        f'<span style="background:#fef2f2;border:1px solid #fecaca;padding:2px 10px;border-radius:8px;">'
        f'&#9632; {esc(t("cal_legend_red",    lang))}</span>'
        f'<span style="background:#f3f4f6;border:1px solid #d1d5db;padding:2px 10px;border-radius:8px;">'
        f'&#9632; {esc(t("cal_legend_grey",   lang))}</span>'
        f'</div>'
    )

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<div>'
        f'<h2 style="margin:0;">{esc(t("cal_h1", lang))}</h2>'
        f'<p class="muted" style="margin:4px 0 0;">{esc(t("cal_subtitle", lang))}</p>'
        f'</div>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n'
        f'<div class="card">'
        f'{legend_html}'
        f'{table_html}'
        f'</div>'
    )
    return page_layout(
        t("cal_title", lang), body,
        user=user, flash=flash, flash_error=flash_error, lang=lang,
    )


# ---------------------------------------------------------------------------
# Audit Module — Working Papers
# ---------------------------------------------------------------------------

def render_working_papers(
    ctx: dict[str, Any],
    user: dict[str, Any],
    client_code: str,
    period: str,
    engagement_type: str,
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    engagement_type = engagement_type or "audit"

    filter_form = (
        f'<div class="card">'
        f'<form method="GET" action="/working_papers" style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("col_client", lang))}</label><br>'
        f'<input type="text" name="client_code" value="{esc(client_code)}" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("wp_period", lang))}</label><br>'
        f'<input type="month" name="period" value="{esc(period)}" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("wp_engagement_type", lang))}</label><br>'
        f'<select name="engagement_type" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;">'
        f'<option value="audit" {"selected" if engagement_type == "audit" else ""}>Audit</option>'
        f'<option value="review" {"selected" if engagement_type == "review" else ""}>Review</option>'
        f'<option value="compilation" {"selected" if engagement_type == "compilation" else ""}>Compilation</option>'
        f'</select></div>'
        f'<div><button type="submit" class="btn-primary" style="padding:7px 16px;">{esc(t("btn_save", lang))}</button></div>'
        f'</form>'
        f'</div>\n'
    )

    papers_html = ""
    if client_code and period:
        with open_db() as conn:
            papers = _audit.get_working_papers(conn, client_code, period, engagement_type or None)

        if papers:
            def _status_badge(s: str) -> str:
                if s == "complete":
                    return f'<span style="background:#dcfce7;color:#166534;padding:2px 8px;border-radius:10px;font-size:12px;">{esc(t("wp_status_complete", lang))}</span>'
                elif s == "exception":
                    return f'<span style="background:#fee2e2;color:#991b1b;padding:2px 8px;border-radius:10px;font-size:12px;">{esc(t("wp_status_exception", lang))}</span>'
                else:
                    return f'<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:10px;font-size:12px;">{esc(t("wp_status_open", lang))}</span>'

            rows_html = ""
            for p in papers:
                bal_books = f"${float(p['balance_per_books']):,.2f}" if p.get("balance_per_books") is not None else '<span class="muted">\u2014</span>'
                bal_conf  = f"${float(p['balance_confirmed']):,.2f}" if p.get("balance_confirmed") is not None else '<span class="muted">\u2014</span>'
                diff_val  = (float(p.get("balance_per_books") or 0) - float(p.get("balance_confirmed") or 0)) if (p.get("balance_per_books") is not None and p.get("balance_confirmed") is not None) else None
                diff_html = f"${diff_val:,.2f}" if diff_val is not None else '<span class="muted">\u2014</span>'
                tested_by = esc(p.get("tested_by") or "")
                reviewed  = esc(p.get("reviewed_by") or "")
                rows_html += (
                    f"<tr>"
                    f"<td><strong>{esc(p.get('account_code', ''))}</strong></td>"
                    f"<td>{esc(p.get('account_name', ''))}</td>"
                    f"<td style='text-align:right;'>{bal_books}</td>"
                    f"<td style='text-align:right;'>{bal_conf}</td>"
                    f"<td style='text-align:right;'>{diff_html}</td>"
                    f"<td>{_status_badge(p.get('status', 'open'))}</td>"
                    f"<td>{reviewed}</td>"
                    f"<td>{tested_by}</td>"
                    f"<td>"
                    f"<form method='POST' action='/working_papers/signoff' style='display:inline;'>"
                    f"<input type='hidden' name='paper_id' value='{esc(str(p['id']))}'>"
                    f"<input type='hidden' name='client_code' value='{esc(client_code)}'>"
                    f"<input type='hidden' name='period' value='{esc(period)}'>"
                    f"<input type='hidden' name='engagement_type' value='{esc(engagement_type)}'>"
                    f"<button type='submit' class='btn-primary' style='padding:4px 10px;font-size:12px;'>"
                    f"{esc(t('wp_sign_off', lang))}</button></form>"
                    f"</td>"
                    f"</tr>\n"
                )

            pdf_url = f"/working_papers/pdf?client_code={urlquote(client_code)}&period={urlquote(period)}&engagement_type={urlquote(engagement_type)}"
            papers_html = (
                f'<div class="card">'
                f'<div class="topbar" style="margin-bottom:12px;">'
                f'<h3 style="margin:0;">{esc(t("wp_lead_sheet", lang))}</h3>'
                f'<a href="{pdf_url}" class="btn-secondary button-link" style="font-size:13px;">{esc(t("wp_export_pdf", lang))}</a>'
                f'</div>'
                f'<div style="overflow-x:auto;">'
                f'<table>'
                f'<thead><tr>'
                f'<th>Code</th>'
                f'<th>{esc(t("wp_lead_sheet", lang))}</th>'
                f'<th style="text-align:right;">{esc(t("wp_balance_books", lang))}</th>'
                f'<th style="text-align:right;">{esc(t("wp_balance_confirmed", lang))}</th>'
                f'<th style="text-align:right;">{esc(t("wp_difference", lang))}</th>'
                f'<th>{esc(t("col_status", lang))}</th>'
                f'<th>{esc(t("wp_reviewed_by", lang))}</th>'
                f'<th>{esc(t("wp_prepared_by", lang))}</th>'
                f'<th>{esc(t("col_actions_header", lang))}</th>'
                f'</tr></thead>'
                f'<tbody>{rows_html}</tbody>'
                f'</table></div></div>\n'
            )
        else:
            coa_form = (
                f'<form method="POST" action="/working_papers/create_from_coa" style="display:inline;">'
                f'<input type="hidden" name="client_code" value="{esc(client_code)}">'
                f'<input type="hidden" name="period" value="{esc(period)}">'
                f'<input type="hidden" name="engagement_type" value="{esc(engagement_type)}">'
                f'<button type="submit" class="btn-primary">{esc(t("wp_new_paper", lang))}</button>'
                f'</form>'
            )
            papers_html = (
                f'<div class="card"><p class="muted">{esc(t("wp_no_papers", lang))}</p>'
                f'{coa_form}</div>\n'
            )

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("wp_title", lang))}</h2>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n'
        f'{filter_form}'
        f'{papers_html}'
    )
    return page_layout(t("wp_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Audit Module — Evidence
# ---------------------------------------------------------------------------

def render_audit_evidence(
    ctx: dict[str, Any],
    user: dict[str, Any],
    client_code: str,
    period: str,
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    filter_form = (
        f'<div class="card">'
        f'<form method="GET" action="/audit/evidence" style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("col_client", lang))}</label><br>'
        f'<input type="text" name="client_code" value="{esc(client_code)}" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("wp_period", lang))}</label><br>'
        f'<input type="month" name="period" value="{esc(period)}" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><button type="submit" class="btn-primary" style="padding:7px 16px;">{esc(t("btn_save", lang))}</button></div>'
        f'</form>'
        f'</div>\n'
    )

    evidence_html = ""
    if client_code and period:
        with open_db() as conn:
            _audit.check_and_update_evidence_for_period(conn, client_code, period)
            chains = _audit.get_evidence_chains(conn, client_code, period)

        def _ev_type_badge(ev_type: str) -> str:
            colors = {
                "po":      ("background:#ede9fe;color:#5b21b6;", t("ev_po", lang)),
                "invoice": ("background:#dbeafe;color:#1e40af;", t("ev_invoice", lang)),
                "payment": ("background:#dcfce7;color:#166534;", t("ev_payment", lang)),
            }
            style, label = colors.get(ev_type, ("background:#f3f4f6;color:#374151;", ev_type))
            return f'<span style="{style}padding:2px 8px;border-radius:10px;font-size:12px;">{esc(label)}</span>'

        def _match_badge(status: str) -> str:
            if status == "complete":
                return f'<span style="background:#dcfce7;color:#166534;padding:2px 8px;border-radius:10px;font-size:12px;">{esc(t("ev_match_complete", lang))}</span>'
            elif status == "partial":
                return f'<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:10px;font-size:12px;">{esc(t("ev_match_partial", lang))}</span>'
            else:
                return f'<span style="background:#fee2e2;color:#991b1b;padding:2px 8px;border-radius:10px;font-size:12px;">{esc(t("ev_match_missing", lang))}</span>'

        if chains:
            rows_html = ""
            for ev in chains:
                linked_count = len(ev.get("linked_document_ids") or [])
                ev_id_esc = esc(str(ev.get("id", "")))
                rows_html += (
                    f"<tr>"
                    f"<td><code>{esc(str(ev.get('document_id', '')))}</code></td>"
                    f"<td>{esc(str(ev.get('vendor', '')))}</td>"
                    f"<td>{esc(str(ev.get('document_date', '')))}</td>"
                    f"<td style='text-align:right;'>{esc(str(ev.get('amount', '')))}</td>"
                    f"<td>{_ev_type_badge(ev.get('evidence_type', ''))}</td>"
                    f"<td>{_match_badge(ev.get('match_status', ''))}</td>"
                    f"<td style='text-align:center;'>{linked_count}</td>"
                    f"<td>"
                    f"<details><summary style='cursor:pointer;font-size:12px;color:#6366f1;'>{esc(t('ev_link_docs', lang))}</summary>"
                    f"<form method='POST' action='/audit/evidence/link' style='margin-top:8px;display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end;'>"
                    f"<input type='hidden' name='evidence_id' value='{ev_id_esc}'>"
                    f"<input type='hidden' name='client_code' value='{esc(client_code)}'>"
                    f"<input type='hidden' name='period' value='{esc(period)}'>"
                    f"<input type='text' name='linked_doc_ids' placeholder='doc_id1,doc_id2' style='padding:4px 8px;border:1px solid #d1d5db;border-radius:4px;font-size:12px;width:200px;'>"
                    f"<button type='submit' class='btn-primary' style='padding:4px 10px;font-size:12px;'>{esc(t('btn_save', lang))}</button>"
                    f"</form></details>"
                    f"</td>"
                    f"</tr>\n"
                )
            evidence_html = (
                f'<div class="card">'
                f'<div style="overflow-x:auto;">'
                f'<table>'
                f'<thead><tr>'
                f'<th>ID</th>'
                f'<th>Vendor</th>'
                f'<th>{esc(t("col_date", lang))}</th>'
                f'<th style="text-align:right;">{esc(t("col_amount", lang))}</th>'
                f'<th>{esc(t("ev_po", lang))}/{esc(t("ev_invoice", lang))}/{esc(t("ev_payment", lang))}</th>'
                f'<th>{esc(t("ev_match_status", lang))}</th>'
                f'<th style="text-align:center;">Linked</th>'
                f'<th>{esc(t("col_actions_header", lang))}</th>'
                f'</tr></thead>'
                f'<tbody>{rows_html}</tbody>'
                f'</table></div></div>\n'
            )
        else:
            evidence_html = f'<div class="card"><p class="muted">{esc(t("ev_no_evidence", lang))}</p></div>\n'

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("ev_title", lang))}</h2>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n'
        f'{filter_form}'
        f'{evidence_html}'
    )
    return page_layout(t("ev_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Audit Module — Sampling
# ---------------------------------------------------------------------------

def render_audit_sample(
    ctx: dict[str, Any],
    user: dict[str, Any],
    client_code: str,
    period: str,
    account_code: str,
    sample_size_str: str,
    paper_id: str,
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    try:
        sample_size = int(sample_size_str) if sample_size_str else 10
    except ValueError:
        sample_size = 10

    filter_form = (
        f'<div class="card">'
        f'<form method="GET" action="/audit/sample" style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("col_client", lang))}</label><br>'
        f'<input type="text" name="client_code" value="{esc(client_code)}" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("wp_period", lang))}</label><br>'
        f'<input type="month" name="period" value="{esc(period)}" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("samp_account", lang))}</label><br>'
        f'<input type="text" name="account_code" value="{esc(account_code)}" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("samp_size", lang))}</label><br>'
        f'<input type="number" name="sample_size" value="{sample_size}" min="1" max="100" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;width:80px;"></div>'
        f'{"<input type=hidden name=paper_id value=" + repr(esc(paper_id)) + ">" if paper_id else ""}'
        f'<div><button type="submit" class="btn-primary" style="padding:7px 16px;">{esc(t("btn_save", lang))}</button></div>'
        f'</form>'
        f'</div>\n'
    )

    sample_html = ""
    if client_code and period:
        with open_db() as conn:
            docs = _audit.get_sample(conn, client_code, period, account_code, sample_size, paper_id or None)
            if paper_id:
                progress = _audit.get_sample_status(conn, paper_id)
            else:
                progress = None

        if progress:
            pct = int(progress.get("pct", 0))
            tested   = progress.get("tested", 0)
            total    = progress.get("total", 0)
            exceptions_count = progress.get("exceptions", 0)
            sample_html += (
                f'<div class="card">'
                f'<p style="margin-bottom:6px;">{esc(t("samp_progress", lang))}: {tested}/{total} ({pct}%)'
                f'{"  ⚠ " + str(exceptions_count) + " exceptions" if exceptions_count else ""}</p>'
                f'<div style="background:#e5e7eb;border-radius:6px;height:10px;">'
                f'<div style="background:#6366f1;border-radius:6px;height:10px;width:{pct}%;"></div>'
                f'</div>'
                f'</div>\n'
            )

        if docs:
            def _tick_badge(tick: str) -> str:
                if tick == "tested":
                    return f'<span style="background:#dcfce7;color:#166534;padding:2px 8px;border-radius:10px;font-size:12px;">{esc(t("wp_tick_tested", lang))}</span>'
                elif tick == "exception":
                    return f'<span style="background:#fee2e2;color:#991b1b;padding:2px 8px;border-radius:10px;font-size:12px;">{esc(t("wp_tick_exception", lang))}</span>'
                elif tick == "na":
                    return f'<span style="background:#f3f4f6;color:#374151;padding:2px 8px;border-radius:10px;font-size:12px;">{esc(t("wp_tick_na", lang))}</span>'
                else:
                    return f'<span class="muted">\u2014</span>'

            rows_html = ""
            _samp_mark_labels = [("tested", "samp_mark_tested"), ("exception", "samp_mark_exception"), ("na", "samp_mark_na")]
            _paper_id_esc = esc(str(paper_id or ""))
            for doc in docs:
                current_tick = doc.get("tick_mark", "")
                _doc_id_esc = esc(str(doc.get("id") or doc.get("document_id") or ""))
                _mark_btns = "".join(
                    f"<form method='POST' action='/audit/sample/mark' style='display:inline;margin-right:4px;'>"
                    f"<input type='hidden' name='paper_id' value='{_paper_id_esc}'>"
                    f"<input type='hidden' name='document_id' value='{_doc_id_esc}'>"
                    f"<input type='hidden' name='tick_mark' value='{mark}'>"
                    f"<input type='hidden' name='client_code' value='{esc(client_code)}'>"
                    f"<input type='hidden' name='period' value='{esc(period)}'>"
                    f"<input type='hidden' name='account_code' value='{esc(account_code)}'>"
                    f"<input type='hidden' name='sample_size' value='{sample_size}'>"
                    f"<button type='submit' class='btn-secondary' style='padding:3px 8px;font-size:11px;'>"
                    f"{esc(t(label_key, lang))}</button></form>"
                    for mark, label_key in _samp_mark_labels
                )
                rows_html += (
                    f"<tr>"
                    f"<td>{esc(str(doc.get('file_name', '')))}</td>"
                    f"<td>{esc(str(doc.get('vendor', '')))}</td>"
                    f"<td style='text-align:right;'>{esc(str(doc.get('amount', '')))}</td>"
                    f"<td>{esc(str(doc.get('document_date', '')))}</td>"
                    f"<td>{_tick_badge(current_tick)}</td>"
                    f"<td style='white-space:nowrap;'>{_mark_btns}</td>"
                    f"</tr>\n"
                )
            sample_html += (
                f'<div class="card">'
                f'<div style="overflow-x:auto;">'
                f'<table>'
                f'<thead><tr>'
                f'<th>File</th>'
                f'<th>Vendor</th>'
                f'<th style="text-align:right;">{esc(t("col_amount", lang))}</th>'
                f'<th>{esc(t("col_date", lang))}</th>'
                f'<th>Tick</th>'
                f'<th>{esc(t("col_actions_header", lang))}</th>'
                f'</tr></thead>'
                f'<tbody>{rows_html}</tbody>'
                f'</table></div></div>\n'
            )
        else:
            sample_html += f'<div class="card"><p class="muted">{esc(t("samp_no_docs", lang))}</p></div>\n'

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("samp_title", lang))}</h2>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n'
        f'{filter_form}'
        f'{sample_html}'
    )
    return page_layout(t("samp_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Audit Module — Financial Statements
# ---------------------------------------------------------------------------

def render_financial_statements_page(
    ctx: dict[str, Any],
    user: dict[str, Any],
    client_code: str,
    period: str,
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    filter_form = (
        f'<div class="card">'
        f'<form method="GET" action="/financial_statements" style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("col_client", lang))}</label><br>'
        f'<input type="text" name="client_code" value="{esc(client_code)}" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("wp_period", lang))}</label><br>'
        f'<input type="month" name="period" value="{esc(period)}" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><button type="submit" class="btn-primary" style="padding:7px 16px;">{esc(t("btn_generate", lang))}</button></div>'
        f'</form>'
        f'</div>\n'
    )

    stmts_html = ""
    if client_code and period:
        with open_db() as conn:
            stmts = _audit.generate_financial_statements(conn, client_code, period)

        def _fmt(val: object) -> str:
            if val is None:
                return '<span class="muted">\u2014</span>'
            try:
                return f"${float(val):,.2f}"
            except (TypeError, ValueError):
                return esc(str(val))

        bs = stmts.get("balance_sheet", {})
        inc = stmts.get("income_statement", {})
        pdf_url = f"/financial_statements/pdf?client_code={urlquote(client_code)}&period={urlquote(period)}"

        stmts_html = (
            f'<div class="topbar" style="margin-bottom:8px;">'
            f'<h3 style="margin:0;">{esc(t("fs_balance_sheet", lang))}</h3>'
            f'<a href="{pdf_url}" class="btn-secondary button-link" style="font-size:13px;">{esc(t("fs_export_pdf", lang))}</a>'
            f'</div>'
            f'<div class="grid-2">'
            # Assets
            f'<div class="card">'
            f'<h4 style="margin-top:0;">{esc(t("fs_current_assets", lang))}</h4>'
            + "".join(
                f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #f3f4f6;">'
                f'<span>{esc(str(k))}</span><span>{_fmt(v)}</span></div>'
                for k, v in (bs.get("current_assets") or {}).items()
            )
            + f'<div style="display:flex;justify-content:space-between;padding:6px 0;font-weight:700;">'
            f'<span>{esc(t("fs_total_assets", lang))}</span><span>{_fmt(bs.get("total_current_assets"))}</span></div>'
            f'<h4>{esc(t("fs_non_current_assets", lang))}</h4>'
            + "".join(
                f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #f3f4f6;">'
                f'<span>{esc(str(k))}</span><span>{_fmt(v)}</span></div>'
                for k, v in (bs.get("non_current_assets") or {}).items()
            )
            + f'<div style="display:flex;justify-content:space-between;padding:6px 0;font-weight:700;">'
            f'<span>{esc(t("fs_total_assets", lang))}</span><span>{_fmt(bs.get("total_assets"))}</span></div>'
            f'</div>'
            # Liabilities + Equity
            f'<div class="card">'
            f'<h4 style="margin-top:0;">{esc(t("fs_current_liabilities", lang))}</h4>'
            + "".join(
                f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #f3f4f6;">'
                f'<span>{esc(str(k))}</span><span>{_fmt(v)}</span></div>'
                for k, v in (bs.get("current_liabilities") or {}).items()
            )
            + f'<div style="display:flex;justify-content:space-between;padding:6px 0;font-weight:700;">'
            f'<span>{esc(t("fs_total_liabilities", lang))}</span><span>{_fmt(bs.get("total_current_liabilities"))}</span></div>'
            f'<h4>{esc(t("fs_long_term_liabilities", lang))}</h4>'
            + "".join(
                f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #f3f4f6;">'
                f'<span>{esc(str(k))}</span><span>{_fmt(v)}</span></div>'
                for k, v in (bs.get("long_term_liabilities") or {}).items()
            )
            + f'<div style="display:flex;justify-content:space-between;padding:6px 0;font-weight:700;">'
            f'<span>{esc(t("fs_total_liabilities", lang))}</span><span>{_fmt(bs.get("total_liabilities"))}</span></div>'
            f'<h4>{esc(t("fs_equity", lang))}</h4>'
            + "".join(
                f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #f3f4f6;">'
                f'<span>{esc(str(k))}</span><span>{_fmt(v)}</span></div>'
                for k, v in (bs.get("equity") or {}).items()
            )
            + f'<div style="display:flex;justify-content:space-between;padding:6px 0;font-weight:700;">'
            f'<span>{esc(t("fs_total_equity", lang))}</span><span>{_fmt(bs.get("total_equity"))}</span></div>'
            f'</div>'
            f'</div>'
            # Income Statement
            f'<div class="card" style="margin-top:16px;">'
            f'<h3 style="margin-top:0;">{esc(t("fs_income_statement", lang))}</h3>'
            f'<h4>{esc(t("fs_revenue", lang))}</h4>'
            + "".join(
                f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #f3f4f6;">'
                f'<span>{esc(str(k))}</span><span>{_fmt(v)}</span></div>'
                for k, v in (inc.get("revenue") or {}).items()
            )
            + f'<div style="display:flex;justify-content:space-between;padding:6px 0;font-weight:700;">'
            f'<span>{esc(t("fs_total_revenue", lang))}</span><span>{_fmt(inc.get("total_revenue"))}</span></div>'
            f'<h4>{esc(t("fs_expenses", lang))}</h4>'
            + "".join(
                f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #f3f4f6;">'
                f'<span>{esc(str(k))}</span><span>{_fmt(v)}</span></div>'
                for k, v in (inc.get("expenses") or {}).items()
            )
            + f'<div style="display:flex;justify-content:space-between;padding:6px 0;font-weight:700;">'
            f'<span>{esc(t("fs_total_expenses", lang))}</span><span>{_fmt(inc.get("total_expenses"))}</span></div>'
            f'<div style="display:flex;justify-content:space-between;padding:8px 0;font-size:16px;font-weight:700;border-top:2px solid #374151;">'
            f'<span>{esc(t("fs_net_income", lang))}</span><span>{_fmt(inc.get("net_income"))}</span></div>'
            f'</div>\n'
        )

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("fs_title", lang))}</h2>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n'
        f'{filter_form}'
        f'{stmts_html}'
    )
    return page_layout(t("fs_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Audit Module — Analytical Procedures
# ---------------------------------------------------------------------------

def render_analytical(
    ctx: dict[str, Any],
    user: dict[str, Any],
    client_code: str,
    period: str,
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    filter_form = (
        f'<div class="card">'
        f'<form method="GET" action="/audit/analytical" style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("col_client", lang))}</label><br>'
        f'<input type="text" name="client_code" value="{esc(client_code)}" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("wp_period", lang))}</label><br>'
        f'<input type="month" name="period" value="{esc(period)}" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><button type="submit" class="btn-primary" style="padding:7px 16px;">{esc(t("btn_generate", lang))}</button></div>'
        f'</form>'
        f'</div>\n'
    )

    results_html = ""
    if client_code and period:
        with open_db() as conn:
            results = _audit.run_analytical_procedures(conn, client_code, period)

        def _pct(v: object) -> str:
            if v is None:
                return esc(t("analytics_na", lang))
            try:
                return f"{float(v):.1f}%"
            except (TypeError, ValueError):
                return esc(str(v))

        def _fmt(v: object) -> str:
            if v is None:
                return esc(t("analytics_na", lang))
            try:
                return f"{float(v):,.2f}"
            except (TypeError, ValueError):
                return esc(str(v))

        ratios = results.get("ratios", {})
        variances = results.get("variances") or []
        pdf_url = f"/audit/analytical/pdf?client_code={urlquote(client_code)}&period={urlquote(period)}"

        ratio_rows = (
            f'<div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #f3f4f6;"><span>{esc(t("anal_current_ratio", lang))}</span><span>{_fmt(ratios.get("current_ratio"))}</span></div>'
            f'<div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #f3f4f6;"><span>{esc(t("anal_quick_ratio", lang))}</span><span>{_fmt(ratios.get("quick_ratio"))}</span></div>'
            f'<div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #f3f4f6;"><span>{esc(t("anal_gross_margin", lang))}</span><span>{_pct(ratios.get("gross_margin_pct"))}</span></div>'
            f'<div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #f3f4f6;"><span>{esc(t("anal_net_margin", lang))}</span><span>{_pct(ratios.get("net_margin_pct"))}</span></div>'
            f'<div style="display:flex;justify-content:space-between;padding:6px 0;"><span>{esc(t("anal_ap_days", lang))}</span><span>{_fmt(ratios.get("ap_days"))}</span></div>'
        )

        variance_rows_html = ""
        for v in variances:
            flagged = v.get("flagged", False)
            row_style = "background:#fef2f2;" if flagged else ""
            pct_change = _pct(v.get("pct_change"))
            flag_icon = ' <span style="color:#991b1b;font-size:14px;">&#9888;</span>' if flagged else ""
            variance_rows_html += (
                f"<tr style='{row_style}'>"
                f"<td>{esc(str(v.get('account', '')))}</td>"
                f"<td style='text-align:right;'>{_fmt(v.get('current'))}</td>"
                f"<td style='text-align:right;'>{_fmt(v.get('prior'))}</td>"
                f"<td style='text-align:right;'>{_fmt(v.get('difference'))}</td>"
                f"<td style='text-align:right;'>{pct_change}{flag_icon}</td>"
                f"</tr>\n"
            )

        results_html = (
            f'<div class="topbar" style="margin-bottom:8px;">'
            f'<h3 style="margin:0;">{esc(t("anal_title", lang))}</h3>'
            f'<a href="{pdf_url}" class="btn-secondary button-link" style="font-size:13px;">{esc(t("anal_export_pdf", lang))}</a>'
            f'</div>'
            f'<div class="grid-2">'
            f'<div class="card"><h4 style="margin-top:0;">{esc(t("anal_ratios", lang))}</h4>{ratio_rows}</div>'
            f'<div class="card">'
            f'<h4 style="margin-top:0;">{esc(t("anal_variance_title", lang))}</h4>'
            + (
                f'<div style="overflow-x:auto;"><table>'
                f'<thead><tr>'
                f'<th>Account</th>'
                f'<th style="text-align:right;">{esc(t("anal_current", lang))}</th>'
                f'<th style="text-align:right;">{esc(t("anal_prior", lang))}</th>'
                f'<th style="text-align:right;">{esc(t("anal_difference", lang))}</th>'
                f'<th style="text-align:right;">%</th>'
                f'</tr></thead>'
                f'<tbody>{variance_rows_html}</tbody>'
                f'</table></div>'
                if variances else
                f'<p class="muted">{esc(t("anal_no_data", lang))}</p>'
            )
            + f'</div></div>\n'
        )

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("anal_title", lang))}</h2>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n'
        f'{filter_form}'
        f'{results_html}'
    )
    return page_layout(t("anal_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Audit Module — Engagements
# ---------------------------------------------------------------------------

def render_engagements(
    ctx: dict[str, Any],
    user: dict[str, Any],
    client_code_filter: str,
    status_filter: str,
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    status_opts = "".join(
        f'<option value="{v}" {"selected" if status_filter == v else ""}>{esc(v.capitalize())}</option>'
        for v in ["", "planning", "fieldwork", "review", "complete", "issued"]
    )

    filter_form = (
        f'<div class="card">'
        f'<form method="GET" action="/engagements" style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("col_client", lang))}</label><br>'
        f'<input type="text" name="client_code" value="{esc(client_code_filter)}" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("col_status", lang))}</label><br>'
        f'<select name="status" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;">{status_opts}</select></div>'
        f'<div><button type="submit" class="btn-primary" style="padding:7px 16px;">{esc(t("btn_save", lang))}</button></div>'
        f'</form>'
        f'</div>\n'
    )

    with open_db() as conn:
        engagements = _audit.get_engagements(
            conn,
            client_code=client_code_filter or None,
            status=status_filter or None,
        )

    def _eng_status_badge(s: str) -> str:
        colors = {
            "planning":  ("background:#dbeafe;color:#1e40af;", s),
            "fieldwork": ("background:#fef3c7;color:#92400e;", s),
            "review":    ("background:#ede9fe;color:#5b21b6;", s),
            "complete":  ("background:#dcfce7;color:#166534;", s),
            "issued":    ("background:#f3f4f6;color:#374151;", s),
        }
        style, label = colors.get(s, ("background:#f3f4f6;color:#374151;", s))
        return f'<span style="{style}padding:2px 8px;border-radius:10px;font-size:12px;">{esc(label.capitalize())}</span>'

    new_form = (
        f'<div class="card" style="margin-bottom:12px;">'
        f'<h3 style="margin-top:0;">{esc(t("eng_new", lang))}</h3>'
        f'<form method="POST" action="/engagements/create" style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("col_client", lang))}</label><br>'
        f'<input type="text" name="client_code" value="{esc(client_code_filter)}" required style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("wp_period", lang))}</label><br>'
        f'<input type="month" name="period" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("wp_engagement_type", lang))}</label><br>'
        f'<select name="engagement_type" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;">'
        f'<option value="audit">Audit</option><option value="review">Review</option><option value="compilation">Compilation</option>'
        f'</select></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("eng_partner", lang))}</label><br>'
        f'<input type="text" name="partner" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("eng_manager", lang))}</label><br>'
        f'<input type="text" name="manager" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("eng_planned_hours", lang))}</label><br>'
        f'<input type="number" name="planned_hours" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;width:100px;"></div>'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("eng_fee", lang))}</label><br>'
        f'<input type="number" name="fee" step="0.01" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;width:120px;"></div>'
        f'<div><button type="submit" class="btn-primary" style="padding:7px 16px;">{esc(t("eng_new", lang))}</button></div>'
        f'</form></div>\n'
    )

    if engagements:
        rows_html = ""
        for eng in engagements:
            with open_db() as conn:
                prog = _audit.get_engagement_progress(conn, eng["id"])
            pct = int(prog.get("pct", 0))
            prog_html = (
                f'<div style="background:#e5e7eb;border-radius:4px;height:6px;width:80px;display:inline-block;vertical-align:middle;">'
                f'<div style="background:#6366f1;border-radius:4px;height:6px;width:{pct}%;"></div></div>'
                f'&nbsp;{pct}%'
            )
            _eng_id_q = urlquote(str(eng.get("id", "")))
            rows_html += (
                f"<tr>"
                f"<td><strong>{esc(str(eng.get('client_code', '')))}</strong></td>"
                f"<td>{esc(str(eng.get('period', '')))}</td>"
                f"<td>{esc(str(eng.get('engagement_type', '')))}</td>"
                f"<td>{_eng_status_badge(eng.get('status', ''))}</td>"
                f"<td>{esc(str(eng.get('partner', '')))}</td>"
                f"<td>{esc(str(eng.get('manager', '')))}</td>"
                f"<td style='text-align:right;'>{esc(str(eng.get('planned_hours', '') or ''))}</td>"
                f"<td style='text-align:right;'>{esc(str(eng.get('actual_hours', '') or ''))}</td>"
                f"<td>{prog_html}</td>"
                f"<td><a href='/engagements/detail?id={_eng_id_q}' class='btn-secondary button-link' style='padding:3px 10px;font-size:12px;'>{esc(t('eng_detail', lang))}</a></td>"
                f"</tr>\n"
            )
        eng_table = (
            f'<div class="card">'
            f'<div style="overflow-x:auto;">'
            f'<table>'
            f'<thead><tr>'
            f'<th>{esc(t("col_client", lang))}</th>'
            f'<th>{esc(t("wp_period", lang))}</th>'
            f'<th>{esc(t("wp_engagement_type", lang))}</th>'
            f'<th>{esc(t("col_status", lang))}</th>'
            f'<th>{esc(t("eng_partner", lang))}</th>'
            f'<th>{esc(t("eng_manager", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("eng_planned_hours", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("eng_actual_hours", lang))}</th>'
            f'<th>{esc(t("eng_progress", lang))}</th>'
            f'<th>{esc(t("col_actions_header", lang))}</th>'
            f'</tr></thead>'
            f'<tbody>{rows_html}</tbody>'
            f'</table></div></div>\n'
        )
    else:
        eng_table = f'<div class="card"><p class="muted">{esc(t("eng_no_engagements", lang))}</p></div>\n'

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("eng_title", lang))}</h2>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n'
        f'{filter_form}'
        f'{new_form}'
        f'{eng_table}'
    )
    return page_layout(t("eng_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Audit Module — Engagement Detail
# ---------------------------------------------------------------------------

def render_engagement_detail(
    ctx: dict[str, Any],
    user: dict[str, Any],
    engagement_id: str,
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    with open_db() as conn:
        eng = _audit.get_engagement(conn, engagement_id)
        if not eng:
            return page_layout(
                t("err_eng_not_found", lang),
                f'<div class="card"><h2>{esc(t("err_eng_not_found", lang))}</h2>'
                f'<p><a href="/engagements">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                user=user, lang=lang,
            )
        prog = _audit.get_engagement_progress(conn, engagement_id)
        papers = _audit.get_working_papers(conn, eng["client_code"], eng["period"], eng.get("engagement_type"))

    pct = int(prog.get("pct", 0))
    signed_off = prog.get("signed_off", 0)
    total      = prog.get("total", 0)
    exceptions = prog.get("open_exceptions", 0)

    def _fmt_field(label: str, val: object) -> str:
        display = esc(str(val)) if val is not None else '<span class="muted">\u2014</span>'
        return (
            f'<div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #f3f4f6;">'
            f'<span style="font-weight:600;font-size:13px;">{esc(label)}</span><span>{display}</span></div>'
        )

    status_opts = "".join(
        f'<option value="{v}" {"selected" if eng.get("status") == v else ""}>{esc(v.capitalize())}</option>'
        for v in ["planning", "fieldwork", "review", "complete", "issued"]
    )

    issue_btn = ""
    if eng.get("status") != "issued":
        issue_btn = (
            f'<form method="POST" action="/engagements/issue" style="display:inline;margin-left:8px;">'
            f'<input type="hidden" name="engagement_id" value="{esc(str(engagement_id))}">'
            f'<button type="submit" class="btn-primary" style="background:#dc2626;" '
            f'onclick="return confirm(\'{esc(t("eng_issue", lang) if False else "Issue engagement?")}\')">'
            f'{esc(t("eng_issued_at", lang) if False else "Issue Report")}</button></form>'
        )

    wp_rows = ""
    for p in papers:
        wp_rows += (
            f"<tr><td>{esc(p.get('account_code', ''))}</td>"
            f"<td>{esc(p.get('account_name', ''))}</td>"
            f"<td>{esc(p.get('status', ''))}</td></tr>\n"
        )

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("eng_detail", lang))}</h2>'
        f'<a href="/engagements" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n'
        f'<div class="grid-2">'
        f'<div class="card">'
        + _fmt_field(t("col_client", lang), eng.get("client_code"))
        + _fmt_field(t("wp_period", lang), eng.get("period"))
        + _fmt_field(t("wp_engagement_type", lang), eng.get("engagement_type"))
        + _fmt_field(t("col_status", lang), eng.get("status"))
        + _fmt_field(t("eng_partner", lang), eng.get("partner"))
        + _fmt_field(t("eng_manager", lang), eng.get("manager"))
        + _fmt_field(t("eng_staff", lang), eng.get("staff"))
        + _fmt_field(t("eng_planned_hours", lang), eng.get("planned_hours"))
        + _fmt_field(t("eng_actual_hours", lang), eng.get("actual_hours"))
        + _fmt_field(t("eng_budget", lang), eng.get("budget"))
        + _fmt_field(t("eng_fee", lang), eng.get("fee"))
        + f'</div>'
        f'<div class="card">'
        f'<h4 style="margin-top:0;">{esc(t("eng_progress", lang))}</h4>'
        f'<p>{signed_off}/{total} papers signed off — {exceptions} open exceptions</p>'
        f'<div style="background:#e5e7eb;border-radius:6px;height:12px;">'
        f'<div style="background:#6366f1;border-radius:6px;height:12px;width:{pct}%;"></div></div>'
        f'<p style="margin-top:6px;font-size:13px;color:#6b7280;">{pct}%</p>'
        f'<hr style="border:none;border-top:1px solid #e5e7eb;margin:12px 0;">'
        f'<h4>{esc(t("eng_title", lang))} — Edit</h4>'
        f'<form method="POST" action="/engagements/update" style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;">'
        f'<input type="hidden" name="engagement_id" value="{esc(str(engagement_id))}">'
        f'<div><label style="font-size:12px;font-weight:600;">{esc(t("col_status", lang))}</label><br>'
        f'<select name="status" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;">{status_opts}</select></div>'
        f'<div><label style="font-size:12px;font-weight:600;">{esc(t("eng_partner", lang))}</label><br>'
        f'<input type="text" name="partner" value="{esc(str(eng.get("partner") or ""))}" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;"></div>'
        f'<div><label style="font-size:12px;font-weight:600;">{esc(t("eng_manager", lang))}</label><br>'
        f'<input type="text" name="manager" value="{esc(str(eng.get("manager") or ""))}" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;"></div>'
        f'<div><label style="font-size:12px;font-weight:600;">{esc(t("eng_staff", lang))}</label><br>'
        f'<input type="text" name="staff" value="{esc(str(eng.get("staff") or ""))}" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;"></div>'
        f'<div><label style="font-size:12px;font-weight:600;">{esc(t("eng_planned_hours", lang))}</label><br>'
        f'<input type="number" name="planned_hours" value="{esc(str(eng.get("planned_hours") or ""))}" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;width:90px;"></div>'
        f'<div><label style="font-size:12px;font-weight:600;">{esc(t("eng_actual_hours", lang))}</label><br>'
        f'<input type="number" name="actual_hours" value="{esc(str(eng.get("actual_hours") or ""))}" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;width:90px;"></div>'
        f'<div><label style="font-size:12px;font-weight:600;">{esc(t("eng_fee", lang))}</label><br>'
        f'<input type="number" name="fee" step="0.01" value="{esc(str(eng.get("fee") or ""))}" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;width:110px;"></div>'
        f'<div><button type="submit" class="btn-primary" style="padding:6px 14px;">{esc(t("btn_save", lang))}</button></div>'
        f'</form>'
        f'{issue_btn}'
        f'</div>'
        f'</div>'
        + (
            f'<div class="card" style="margin-top:16px;">'
            f'<h4 style="margin-top:0;">{esc(t("wp_title", lang))}</h4>'
            f'<div style="overflow-x:auto;"><table>'
            f'<thead><tr><th>Code</th><th>Account</th><th>{esc(t("col_status", lang))}</th></tr></thead>'
            f'<tbody>{wp_rows}</tbody></table></div></div>'
            if papers else
            f'<div class="card" style="margin-top:16px;"><p class="muted">{esc(t("wp_no_papers", lang))}</p></div>'
        )
    )
    return page_layout(t("eng_detail", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


def render_license_page(
    ctx: dict[str, Any],
    user: dict[str, Any],
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    """Render the /license management page (owner only)."""
    status = get_license_status()
    with open_db() as conn:
        limits = check_limits(conn)

    tier = status.get("tier", "none")
    valid = status.get("valid", False)
    firm_name = status.get("firm_name", "")
    expiry_date = status.get("expiry_date", "")
    days_remaining = status.get("days_remaining", 0)
    features = status.get("features", [])
    error = status.get("error", "")

    # Tier badge color
    tier_colors = {
        "essentiel":     "#6b7280",
        "professionnel": "#2563eb",
        "cabinet":       "#7c3aed",
        "entreprise":    "#b45309",
    }
    tier_color = tier_colors.get(tier, "#6b7280")
    tier_label_key = f"lic_tier_{tier}" if tier != "none" else "lic_status_none"
    tier_label = t(tier_label_key, lang)

    # Status badge
    if valid:
        status_label = t("lic_status_valid", lang)
        status_color = "#16a34a"
    elif tier == "none":
        status_label = t("lic_status_none", lang)
        status_color = "#6b7280"
    else:
        status_label = t("lic_status_expired", lang)
        status_color = "#dc2626"

    # Expiry color
    if valid:
        if days_remaining < 30:
            expiry_color = "#dc2626"
        elif days_remaining < 90:
            expiry_color = "#d97706"
        else:
            expiry_color = "#16a34a"
    else:
        expiry_color = "#6b7280"

    # Usage bars
    def _usage_bar(used: int, max_val: int, label: str, ok: bool) -> str:
        if max_val <= 0:
            pct = 0
        else:
            pct = min(100, int(used * 100 / max_val))
        bar_color = "#16a34a" if ok else "#dc2626"
        return (
            f'<div style="margin-bottom:10px;">'
            f'<div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:4px;">'
            f'<span style="font-weight:600;">{esc(label)}</span>'
            f'<span>{used} / {max_val}</span></div>'
            f'<div style="background:#e5e7eb;border-radius:4px;height:8px;">'
            f'<div style="background:{bar_color};border-radius:4px;height:8px;width:{pct}%;"></div>'
            f'</div></div>'
        )

    usage_html = (
        _usage_bar(limits["client_count"], limits["max_clients"], t("lic_clients_used", lang), limits["clients_ok"])
        + _usage_bar(limits["user_count"], limits["max_users"], t("lic_users_used", lang), limits["users_ok"])
    )

    # Feature checklist — all possible features in order
    all_features = [
        "basic_review", "basic_posting", "ai_router", "bank_parser",
        "fraud_detection", "revenu_quebec", "time_tracking", "month_end",
        "analytics", "microsoft365", "filing_calendar", "client_comms",
        "audit_module", "financial_statements", "sampling", "api_access",
    ]
    feature_rows = ""
    for feat in all_features:
        enabled = feat in features
        icon = "&#10003;" if enabled else "&#10007;"
        color = "#16a34a" if enabled else "#9ca3af"
        label_key = f"lic_feature_{feat}"
        feature_rows += (
            f'<div style="display:flex;align-items:center;gap:8px;padding:5px 0;'
            f'border-bottom:1px solid #f3f4f6;">'
            f'<span style="color:{color};font-weight:700;font-size:15px;">{icon}</span>'
            f'<span style="font-size:13px;color:{"#374151" if enabled else "#9ca3af"};">'
            f'{esc(t(label_key, lang))}</span></div>\n'
        )

    # Status info card
    info_html = (
        f'<div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;">'
        f'<span style="background:{tier_color};color:white;padding:4px 12px;border-radius:12px;font-size:13px;font-weight:600;">'
        f'{esc(tier_label)}</span>'
        f'<span style="background:{status_color};color:white;padding:4px 12px;border-radius:12px;font-size:13px;font-weight:600;">'
        f'{esc(status_label)}</span>'
        f'</div>'
    )
    if firm_name:
        info_html += (
            f'<div style="font-size:13px;margin-bottom:6px;">'
            f'<strong>{esc(t("lic_firm", lang))}:</strong> {esc(firm_name)}</div>'
        )
    if expiry_date:
        info_html += (
            f'<div style="font-size:13px;margin-bottom:6px;">'
            f'<strong>{esc(t("lic_expiry", lang))}:</strong> '
            f'<span style="color:{expiry_color};font-weight:600;">{esc(expiry_date)}</span>'
            f' &mdash; '
            f'<span style="color:{expiry_color};">{days_remaining} {esc(t("lic_days_remaining", lang))}</span>'
            f'</div>'
        )
    if error and not valid:
        info_html += (
            f'<div class="flash error" style="margin-top:8px;">{esc(error)}</div>'
        )

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("lic_title", lang))}</h2>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n'
        f'<div class="grid-2">'

        # Left column: status + usage
        f'<div>'
        f'<div class="card" style="margin-bottom:16px;">'
        f'<h4 style="margin-top:0;">{esc(t("lic_tier", lang))}</h4>'
        + info_html
        + f'</div>'

        f'<div class="card" style="margin-bottom:16px;">'
        f'<h4 style="margin-top:0;">{esc(t("lic_usage_title", lang))}</h4>'
        + usage_html
        + f'</div>'

        # Activate form
        f'<div class="card">'
        f'<h4 style="margin-top:0;">{esc(t("lic_activate_title", lang))}</h4>'
        f'<form method="POST" action="/license/activate">'
        f'<div class="field">'
        f'<label>{esc(t("lic_key_label", lang))}</label>'
        f'<textarea name="license_key" rows="3" placeholder="{esc(t("lic_key_ph", lang))}" '
        f'style="width:100%;font-family:monospace;font-size:12px;padding:8px;border:1px solid #d1d5db;border-radius:4px;resize:vertical;"></textarea>'
        f'</div>'
        f'<button type="submit" class="btn-primary">{esc(t("lic_btn_activate", lang))}</button>'
        f'</form>'
        f'</div>'
        f'</div>'

        # Right column: features
        f'<div class="card">'
        f'<h4 style="margin-top:0;">{esc(t("lic_features_title", lang))}</h4>'
        + feature_rows
        + f'</div>'

        f'</div>'
    )
    return page_layout(t("lic_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


def page_layout(title: str, body_html: str, user: dict[str, Any] | None = None,
                flash: str = "", flash_error: str = "", lang: str = "fr") -> str:
    flash_html = ""
    if flash:
        flash_html = f'<div class="flash success">{esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{esc(flash_error)}</div>'

    user_pill = ""
    right_controls = ""
    if user:
        display = esc(user.get("display_name") or user.get("username") or "")
        role = esc(user.get("role") or "")
        user_pill = f'<span class="user-pill">{display} &mdash; {role}</span>'
        toggle_label = esc(t("switch_lang", lang))
        toggle_lang = "en" if lang == "fr" else "fr"
        lang_toggle = (
            f'<form method="POST" action="/set_language" style="display:inline;">'
            f'<input type="hidden" name="lang" value="{toggle_lang}">'
            f'<button class="btn-secondary" style="padding:6px 12px;font-size:13px;">'
            f'{toggle_label}</button></form>'
        )
        logout_label = esc(t("logout_btn", lang))
        logout_btn = (
            f'<form method="POST" action="/logout" style="display:inline;">'
            f'<button class="btn-secondary" style="padding:6px 12px;font-size:13px;">'
            f'{logout_label}</button></form>'
        )
        comm_link_html = ""
        if user.get("role") in ("manager", "owner"):
            try:
                with open_db() as _conn:
                    _unread = _client_comms.get_unread_count(_conn)
            except Exception:
                _unread = 0
            _badge = (
                f'<span class="badge-unread" style="margin-left:4px;">{_unread}</span>'
                if _unread > 0 else ""
            )
            comm_link_html = (
                f'<a href="/communications" style="color:#cbd5e1;font-size:13px;'
                f'text-decoration:none;white-space:nowrap;">'
                f'{esc(t("comm_nav_link", lang))}{_badge}</a>'
            )
        right_controls = f'{user_pill} {comm_link_html} {lang_toggle} {logout_btn}'

    # Audit navigation strip — visible to manager/owner only
    audit_nav_html = ""
    if user and user.get("role") in ("manager", "owner"):
        def _anav(href: str, label_key: str) -> str:
            return (
                f'<a href="{href}" style="color:#e2e8f0;font-size:12px;font-weight:500;'
                f'text-decoration:none;padding:4px 10px;border-radius:12px;'
                f'background:rgba(255,255,255,0.08);white-space:nowrap;">'
                f'{esc(t(label_key, lang))}</a>'
            )
        _lic_link = ""
        if user.get("role") == "owner":
            _lic_link = _anav("/license", "lic_nav_link")
        audit_nav_html = (
            f'<nav style="background:#1f2937;padding:6px 24px;display:flex;gap:4px;flex-wrap:wrap;">'
            + _anav("/working_papers", "wp_nav_link")
            + _anav("/audit/evidence", "ev_title")
            + _anav("/audit/sample", "samp_title")
            + _anav("/financial_statements", "fs_title")
            + _anav("/audit/analytical", "anal_title")
            + _anav("/engagements", "eng_title")
            + _lic_link
            + f'</nav>'
        )

    return f"""<!doctype html>
<html lang="{lang}">
<head>
<meta charset="utf-8">
<title>{esc(title)}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}</style>
</head>
<body>
<header>
    <h1>{esc(t("dashboard_header", lang))}</h1>
    <div style="display:flex;gap:12px;align-items:center;">{right_controls}</div>
</header>
{audit_nav_html}
<main>
    {flash_html}
    {body_html}
</main>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Login page
# ---------------------------------------------------------------------------

def render_login(flash_error: str = "", lang: str = "fr") -> str:
    err = f'<div class="flash error">{esc(flash_error)}</div>' if flash_error else ""
    switch_lang = "en" if lang == "fr" else "fr"
    switch_label = "English" if lang == "fr" else "Français"
    return f"""<!doctype html>
<html lang="{lang}">
<head><meta charset="utf-8"><title>{esc(t("login_page_title", lang))}</title><style>{CSS}
.login-wrap{{min-height:100vh;display:flex;align-items:center;justify-content:center;background:#f5f7fb}}
.login-box{{background:white;border:1px solid #e5e7eb;border-radius:12px;padding:2rem 2.5rem;min-width:320px;box-shadow:0 2px 8px rgba(0,0,0,.06)}}
.login-lang{{text-align:center;margin-top:14px;font-size:12px;color:#6b7280}}
.login-lang a{{color:#2563eb;text-decoration:none}}
</style></head>
<body>
<div class="login-wrap">
    <div class="login-box">
        <h2 style="margin-bottom:1.5rem;">LedgerLink</h2>
        {err}
        <form method="POST" action="/login">
            <input type="hidden" name="lang" value="{lang}">
            <div class="field"><label>{esc(t("username", lang))}</label><input type="text" name="username" autofocus></div>
            <div class="field"><label>{esc(t("password", lang))}</label><input type="password" name="password"></div>
            <button class="btn-primary" type="submit" style="width:100%;padding:12px;">{esc(t("login_btn", lang))}</button>
        </form>
        <div class="login-lang"><a href="/login?lang={switch_lang}">{switch_label}</a></div>
    </div>
</div>
</body></html>"""


# ---------------------------------------------------------------------------
# Change password page (shown after bcrypt migration or must_reset_password=1)
# ---------------------------------------------------------------------------

def render_change_password(user: dict[str, Any] | None = None, flash_error: str = "",
                           lang: str = "fr") -> str:
    err = f'<div class="flash error">{esc(flash_error)}</div>' if flash_error else ""
    display_raw = (user.get("display_name") or user.get("username") or "") if user else ""
    display = esc(display_raw)
    intro = esc(t("change_pw_intro", lang, name=display_raw))
    return f"""<!doctype html>
<html lang="{lang}">
<head><meta charset="utf-8"><title>{esc(t("change_pw_title", lang))}</title><style>{CSS}
.login-wrap{{min-height:100vh;display:flex;align-items:center;justify-content:center;background:#f5f7fb}}
.login-box{{background:white;border:1px solid #e5e7eb;border-radius:12px;padding:2rem 2.5rem;min-width:340px;box-shadow:0 2px 8px rgba(0,0,0,.06)}}
</style></head>
<body>
<div class="login-wrap">
    <div class="login-box">
        <h2 style="margin-bottom:.5rem;">{esc(t("change_pw_heading", lang))}</h2>
        <p class="muted" style="margin-bottom:1.5rem;">{intro}</p>
        {err}
        <form method="POST" action="/change_password">
            <div class="field"><label>{esc(t("change_pw_new", lang))}</label><input type="password" name="new_password" autofocus minlength="8"></div>
            <div class="field"><label>{esc(t("change_pw_confirm", lang))}</label><input type="password" name="confirm_password" minlength="8"></div>
            <button class="btn-primary" type="submit" style="width:100%;padding:12px;">{esc(t("change_pw_save", lang))}</button>
        </form>
    </div>
</div>
</body></html>"""


# ---------------------------------------------------------------------------
# Portfolio page
# ---------------------------------------------------------------------------

def render_portfolios(ctx: dict[str, Any], user: dict[str, Any], flash: str,
                      flash_error: str, lang: str = "fr") -> str:
    if not ctx["can_manage_team"]:
        return page_layout(
            t("err_access_denied", lang),
            f'<div class="card"><h2>{esc(t("err_access_denied", lang))}</h2>'
            f'<p>{esc(t("portfolio_access_denied", lang))}</p></div>',
            user=user, flash=flash, flash_error=flash_error, lang=lang)

    all_clients = get_all_client_codes()
    portfolios = get_all_portfolios()
    all_users = get_all_active_users()
    employees = [u for u in all_users if u["role"] == "employee"]

    assigned_clients: set[str] = {c for clients in portfolios.values() for c in clients}
    unassigned = [c for c in all_clients if c not in assigned_clients]

    cards = ""
    for emp in employees:
        emp_name = emp["username"]
        emp_clients = portfolios.get(normalize_key(emp_name), [])
        other_emps = [e["username"] for e in employees if e["username"] != emp_name]

        client_rows = ""
        for cc in emp_clients:
            move_opts = "".join(f'<option value="{esc(e)}">{esc(e)}</option>' for e in other_emps)
            move_opts += f'<option value="unassigned">{esc(t("option_unassigned", lang))}</option>'
            client_rows += f"""<tr>
                <td style="font-size:13px;">{esc(cc)}</td>
                <td>
                    <form method="POST" action="/portfolios/move" style="display:inline-flex;gap:6px;align-items:center;margin-right:6px;">
                        <input type="hidden" name="client_code" value="{esc(cc)}">
                        <input type="hidden" name="from_user" value="{esc(emp_name)}">
                        <select name="to_user" style="padding:4px 8px;font-size:12px;border:1px solid #d1d5db;border-radius:6px;">{move_opts}</select>
                        <button class="btn-secondary" type="submit" style="padding:4px 10px;font-size:12px;">{esc(t("btn_move", lang))}</button>
                    </form>
                    <form method="POST" action="/portfolios/remove" style="display:inline;">
                        <input type="hidden" name="client_code" value="{esc(cc)}">
                        <input type="hidden" name="username_target" value="{esc(emp_name)}">
                        <button class="btn-danger" type="submit" style="padding:4px 10px;font-size:12px;">{esc(t("btn_remove", lang))}</button>
                    </form>
                </td></tr>"""

        no_cli = f'<tr><td colspan=2 class=muted>{esc(t("no_clients_yet", lang))}</td></tr>'
        client_table = (
            f'<table style="margin-bottom:10px;"><thead><tr>'
            f'<th>{esc(t("col_client_header", lang))}</th>'
            f'<th>{esc(t("col_actions_header", lang))}</th>'
            f'</tr></thead><tbody>{client_rows if client_rows else no_cli}</tbody></table>'
        )

        assign_form = ""
        if unassigned:
            opts = "".join(f'<option value="{esc(c)}">{esc(c)}</option>' for c in unassigned)
            assign_form = f"""<form method="POST" action="/portfolios/assign" style="display:flex;gap:8px;align-items:center;margin-top:10px;flex-wrap:wrap;">
                <input type="hidden" name="username_target" value="{esc(emp_name)}">
                <select name="client_code" style="padding:6px 10px;font-size:13px;border:1px solid #d1d5db;border-radius:8px;">{opts}</select>
                <button class="btn-primary" type="submit" style="padding:7px 14px;font-size:13px;">{esc(t("btn_add_client", lang))}</button>
            </form>"""

        n = len(emp_clients)
        cli_word = t("client_plural", lang) if n != 1 else t("client_singular", lang)
        count_badge = f'<span class="badge badge-muted" style="font-size:11px;padding:3px 8px;">{n} {esc(cli_word)}</span>'
        cards += f'<div class="card"><h3>{esc(emp_name)} {count_badge}</h3>{client_table}{assign_form}</div>'

    unassigned_html = ""
    if unassigned:
        pills = " ".join(f'<span class="badge badge-muted" style="margin:2px;">{esc(c)}</span>' for c in unassigned)
        unassigned_html = (
            f'<div class="card"><h3>{esc(t("unassigned_clients", lang))}</h3>'
            f'<p class="muted small">{esc(t("unassigned_clients_hint", lang))}</p>'
            f'<div>{pills}</div></div>'
        )

    no_emp = f'<div class="card"><p class="muted">{esc(t("no_employees", lang))}</p></div>'
    body = f"""
    <div class="card"><div class="actions" style="margin-bottom:12px;"><a href="/">{esc(t("btn_back_to_queue", lang))}</a></div>
        <h2 style="margin-bottom:6px;">{esc(t("portfolio_h2", lang))}</h2>
        <p class="muted">{esc(t("portfolio_desc", lang))}</p>
    </div>
    {unassigned_html}
    {cards if cards else no_emp}
    """
    return page_layout(t("portfolio_title", lang), body,
                       user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# User management page (owner only)
# ---------------------------------------------------------------------------

def render_user_management(ctx: dict[str, Any], user: dict[str, Any], flash: str,
                           flash_error: str, lang: str = "fr") -> str:
    if ctx["role"] != "owner":
        return page_layout(
            t("err_access_denied", lang),
            f'<div class="card"><h2>{esc(t("user_access_denied", lang))}</h2></div>',
            user=user, lang=lang)

    with open_db() as conn:
        users = conn.execute("SELECT * FROM dashboard_users ORDER BY username").fetchall()

    rows_html = "".join(f"""
        <tr>
            <td>{esc(u["username"])}</td>
            <td>{esc(u["display_name"])}</td>
            <td>{esc(u["role"])}</td>
            <td>{esc(t("status_active", lang)) if u["active"] else esc(t("status_inactive", lang))}</td>
            <td>
                <form method="POST" action="/users/set_password" style="display:inline-flex;gap:6px;align-items:center;margin-right:6px;">
                    <input type="hidden" name="username_target" value="{esc(u["username"])}">
                    <input type="text" name="new_password" placeholder="{esc(t("pw_placeholder", lang))}" style="width:160px;padding:4px 8px;font-size:12px;">
                    <button class="btn-secondary" type="submit" style="padding:4px 10px;font-size:12px;">{esc(t("btn_set_pw", lang))}</button>
                </form>
            </td>
        </tr>""" for u in users)

    body = f"""
    <div class="card"><div class="actions" style="margin-bottom:12px;"><a href="/">{esc(t("btn_back_to_queue", lang))}</a></div>
        <h2>{esc(t("user_mgmt_title", lang))}</h2>
    </div>
    <div class="card">
        <h3>{esc(t("existing_users", lang))}</h3>
        <table><thead><tr>
            <th>{esc(t("col_username", lang))}</th>
            <th>{esc(t("col_display_name", lang))}</th>
            <th>{esc(t("col_role", lang))}</th>
            <th>{esc(t("col_status_u", lang))}</th>
            <th>{esc(t("col_password_header", lang))}</th>
        </tr></thead>
        <tbody>{rows_html}</tbody></table>
    </div>
    <div class="card">
        <h3>{esc(t("add_user_title", lang))}</h3>
        <form method="POST" action="/users/add">
            <div class="grid-3">
                <div class="field"><label>{esc(t("col_username", lang))}</label><input type="text" name="username"></div>
                <div class="field"><label>{esc(t("col_display_name", lang))}</label><input type="text" name="display_name"></div>
                <div class="field"><label>{esc(t("col_password_header", lang))}</label><input type="password" name="password"></div>
                <div class="field"><label>{esc(t("col_role", lang))}</label>
                    <select name="role">
                        <option value="employee">{esc(t("role_employee", lang))}</option>
                        <option value="manager">{esc(t("role_manager", lang))}</option>
                        <option value="owner">{esc(t("role_owner", lang))}</option>
                    </select>
                </div>
            </div>
            <button class="btn-primary" type="submit">{esc(t("btn_add_user", lang))}</button>
        </form>
    </div>"""
    return page_layout(t("user_mgmt_title", lang), body,
                       user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Home page
# ---------------------------------------------------------------------------

def render_doc_communications(
    document_id: str, row: sqlite3.Row, ctx: dict[str, Any], lang: str = "fr"
) -> str:
    """Render the client-communications card on a document detail page.

    Shows a *Draft Message* button when the document is on hold, plus a table
    of all existing drafts and sent messages for this document.
    """
    hold_reason = normalize_text(row["manual_hold_reason"])

    try:
        with open_db() as conn:
            comms = _client_comms.get_document_comms(conn, document_id)
    except Exception:
        comms = []

    if not hold_reason and not comms:
        return ""

    title = t("comm_title", lang)

    draft_btn = ""
    if hold_reason:
        draft_btn = (
            f'<form method="POST" action="/communications/draft" style="display:inline;">'
            f'<input type="hidden" name="document_id" value="{esc(document_id)}">'
            f'<input type="hidden" name="lang" value="{esc(lang)}">'
            f'<button class="btn-secondary" type="submit">{esc(t("comm_draft_btn", lang))}</button>'
            f'</form>'
        )

    comm_rows_html = ""
    for c in comms:
        sent_at = c.get("sent_at") or ""
        direction = c.get("direction") or "outbound"
        direction_label = t(f"comm_direction_{direction}", lang)
        is_draft = not sent_at

        if is_draft:
            status_cell = f'<span class="badge badge-hold">{esc(t("comm_draft_unsent", lang))}</span>'
            msg_cell = (
                f'<form method="POST" action="/communications/send" style="margin:0;">'
                f'<input type="hidden" name="document_id" value="{esc(document_id)}">'
                f'<input type="hidden" name="comm_id" value="{esc(c["comm_id"])}">'
                f'<div style="margin-bottom:6px;">'
                f'<textarea name="message" rows="4" style="width:100%;box-sizing:border-box;'
                f'padding:8px;border:1px solid #d1d5db;border-radius:6px;font-size:13px;">'
                f'{esc(c.get("message") or "")}</textarea>'
                f'</div>'
                f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:4px;">'
                f'<input type="text" name="to_email"'
                f' placeholder="{esc(t("comm_to_email_ph", lang))}"'
                f' style="flex:1;min-width:160px;padding:7px;border:1px solid #d1d5db;'
                f'border-radius:6px;font-size:13px;">'
                f'<input type="text" name="subject"'
                f' placeholder="{esc(t("comm_subject_ph", lang))}"'
                f' style="flex:1;min-width:160px;padding:7px;border:1px solid #d1d5db;'
                f'border-radius:6px;font-size:13px;">'
                f'<button class="btn-success" type="submit">{esc(t("comm_send_btn", lang))}</button>'
                f'</div>'
                f'<p class="small muted">{esc(t("comm_edit_hint", lang))}</p>'
                f'</form>'
            )
            date_cell = f'<span class="muted small">{esc(t("comm_draft_unsent", lang))}</span>'
        else:
            status_cell = f'<span class="badge badge-ready">{esc(t("comm_send_btn", lang))}</span>'
            raw_msg = c.get("message") or ""
            msg_preview = raw_msg[:300] + ("\u2026" if len(raw_msg) > 300 else "")
            msg_cell = f'<div style="white-space:pre-wrap;font-size:13px;">{esc(msg_preview)}</div>'
            date_cell = f'<span class="muted small">{esc(sent_at)}</span>'

        comm_rows_html += (
            f'<tr>'
            f'<td style="width:80px;vertical-align:top;padding-top:12px;">{status_cell}</td>'
            f'<td style="width:80px;vertical-align:top;padding-top:12px;">{esc(direction_label)}</td>'
            f'<td>{msg_cell}</td>'
            f'<td style="width:90px;vertical-align:top;padding-top:12px;">'
            f'{esc(c.get("sent_by") or "")}</td>'
            f'<td style="width:130px;vertical-align:top;padding-top:12px;">{date_cell}</td>'
            f'</tr>'
        )

    no_comms = (
        f'<p class="muted">{esc(t("comm_no_comms", lang))}</p>'
        if not comm_rows_html else ""
    )
    table = ""
    if comm_rows_html:
        th = (
            f'<tr>'
            f'<th style="width:80px;">{esc(t("col_status", lang))}</th>'
            f'<th style="width:80px;">{esc(t("comm_col_direction", lang))}</th>'
            f'<th>{esc(t("comm_col_message", lang))}</th>'
            f'<th style="width:90px;">{esc(t("comm_col_by", lang))}</th>'
            f'<th style="width:130px;">{esc(t("comm_col_date", lang))}</th>'
            f'</tr>'
        )
        table = f'<table style="width:100%;"><thead>{th}</thead><tbody>{comm_rows_html}</tbody></table>'

    return (
        f'<div class="card">'
        f'<h3>{esc(title)}</h3>'
        f'<div style="margin-bottom:12px;">{draft_btn}</div>'
        f'{no_comms}{table}'
        f'</div>'
    )


def render_communications(
    ctx: dict[str, Any],
    user: dict[str, Any],
    client_code: str = "",
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    """Full communications log page (manager/owner only).

    Marks all unread messages as read when the page is viewed.
    """
    try:
        with open_db() as conn:
            _client_comms.mark_all_read(conn)
            if client_code:
                comms = _client_comms.get_client_comms(conn, client_code)
            else:
                comms = _client_comms.get_all_comms(conn)
    except Exception:
        comms = []

    title = t("comm_title", lang)

    filter_form = f"""<div class="card">
  <h2>{esc(title)}</h2>
  <div class="actions" style="margin-bottom:12px;">
    <a href="/">{esc(t("btn_back_to_queue", lang))}</a>
  </div>
  <form method="GET" action="/communications">
    <div style="display:flex;gap:12px;align-items:flex-end;flex-wrap:wrap;">
      <div class="field" style="margin-bottom:0;">
        <label>{esc(t("doc_field_client", lang))}</label>
        <input type="text" name="client_code" value="{esc(client_code)}"
               placeholder="{esc(t("comm_filter_client_ph", lang))}">
      </div>
      <div style="padding-bottom:1px;">
        <button class="btn-primary" type="submit">{esc(t("btn_filter", lang))}</button>
      </div>
    </div>
  </form>
</div>"""

    if not comms:
        body = (
            filter_form
            + f'<div class="card"><p class="muted">{esc(t("comm_no_comms", lang))}</p></div>'
        )
        return page_layout(title, body, user=user, flash=flash, flash_error=flash_error, lang=lang)

    rows_html = ""
    for c in comms:
        sent_at = c.get("sent_at") or ""
        is_draft = not sent_at
        direction = c.get("direction") or "outbound"
        direction_label = t(f"comm_direction_{direction}", lang)

        if is_draft:
            status_cell = f'<span class="badge badge-hold">{esc(t("comm_draft_unsent", lang))}</span>'
            date_str = "\u2014"
        else:
            status_cell = f'<span class="badge badge-ready">{esc(t("comm_send_btn", lang))}</span>'
            date_str = esc(sent_at)

        doc_id = c.get("document_id") or ""
        file_name = c.get("file_name") or doc_id
        doc_link = (
            f'<a href="/document?id={urlquote(doc_id)}">{esc(file_name)}</a>'
            if doc_id else "\u2014"
        )

        raw_msg = c.get("message") or ""
        msg_preview = raw_msg[:200] + ("\u2026" if len(raw_msg) > 200 else "")

        rows_html += (
            f'<tr>'
            f'<td style="width:80px;">{status_cell}</td>'
            f'<td style="width:90px;">{esc(c.get("client_code") or "")}</td>'
            f'<td>{doc_link}</td>'
            f'<td style="width:80px;">{esc(direction_label)}</td>'
            f'<td style="white-space:pre-wrap;font-size:13px;">{esc(msg_preview)}</td>'
            f'<td style="width:90px;">{esc(c.get("sent_by") or "")}</td>'
            f'<td style="width:130px;" class="small muted">{date_str}</td>'
            f'</tr>'
        )

    th = (
        f'<tr>'
        f'<th style="width:80px;">{esc(t("col_status", lang))}</th>'
        f'<th style="width:90px;">{esc(t("col_client", lang))}</th>'
        f'<th>{esc(t("comm_col_document", lang))}</th>'
        f'<th style="width:80px;">{esc(t("comm_col_direction", lang))}</th>'
        f'<th>{esc(t("comm_col_message", lang))}</th>'
        f'<th style="width:90px;">{esc(t("comm_col_by", lang))}</th>'
        f'<th style="width:130px;">{esc(t("comm_col_date", lang))}</th>'
        f'</tr>'
    )
    table = f'<div class="card"><table><thead>{th}</thead><tbody>{rows_html}</tbody></table></div>'
    body = filter_form + table
    return page_layout(title, body, user=user, flash=flash, flash_error=flash_error, lang=lang)


def render_home(ctx: dict[str, Any], user: dict[str, Any], status: str, q: str,
                flash: str, flash_error: str, include_ignored: bool,
                only_my_queue: bool, only_unassigned: bool, lang: str = "fr") -> str:
    rows = get_documents(ctx=ctx, status=status, q=q, include_ignored=include_ignored,
                         only_my_queue=only_my_queue, only_unassigned=only_unassigned)
    counts = get_status_counts(ctx)

    portfolio_btn = (
        f'<a class="button-link btn-dark" href="/portfolios">{esc(t("btn_manage_portfolios", lang))}</a>'
        if ctx["can_manage_team"] else ""
    )
    users_btn = (
        f'<a class="button-link btn-secondary" href="/users">{esc(t("btn_users", lang))}</a>'
        if ctx["role"] == "owner" else ""
    )
    period_close_btn = (
        f'<a class="button-link btn-secondary" href="/period_close">{esc(t("pc_nav_link", lang))}</a>'
        if ctx["can_manage_team"] else ""
    )
    time_btn = (
        f'<a class="button-link btn-secondary" href="/time">{esc(t("time_nav_link", lang))}</a>'
        if ctx["can_manage_team"] else ""
    )
    comms_btn = (
        f'<a class="button-link btn-secondary" href="/communications">{esc(t("comm_nav_link", lang))}</a>'
        if ctx["can_manage_team"] else ""
    )
    wp_btn = (
        f'<a class="button-link btn-secondary" href="/working_papers">{esc(t("wp_nav_link", lang))}</a>'
        if ctx["can_manage_team"] else ""
    )
    engagements_btn = (
        f'<a class="button-link btn-secondary" href="/engagements">{esc(t("eng_title", lang))}</a>'
        if ctx["can_manage_team"] else ""
    )

    stats_html = f"""
    <div class="card">
        <div class="topbar">
            <div><h2 style="margin-bottom:4px;">{esc(t("queue_title", lang))}</h2></div>
            <div class="actions">{portfolio_btn}{users_btn}{period_close_btn}{time_btn}{comms_btn}{wp_btn}{engagements_btn}<a class="button-link btn-secondary" href="/">{esc(t("btn_reset", lang))}</a></div>
        </div>
    </div>
    <div class="stats">
        <div class="stat"><div class="small muted">{esc(t("stat_needs_review", lang))}</div><div><strong>{counts.get("Needs Review",0)}</strong></div></div>
        <div class="stat"><div class="small muted">{esc(t("stat_on_hold", lang))}</div><div><strong>{counts.get("On Hold",0)}</strong></div></div>
        <div class="stat"><div class="small muted">{esc(t("stat_ready_to_post", lang))}</div><div><strong>{counts.get("Ready to Post",0)}</strong></div></div>
        <div class="stat"><div class="small muted">{esc(t("stat_posted", lang))}</div><div><strong>{counts.get("Posted",0)}</strong></div></div>
        <div class="stat"><div class="small muted">{esc(t("stat_visible", lang))}</div><div><strong>{len(rows)}</strong></div></div>
    </div>"""

    status_opts = "".join(
        f'<option value="{v}" {"selected" if status==v else ""}>'
        f'{esc(t(_STATUS_LABEL_KEYS.get(v, v), lang))}</option>'
        for v in ["Needs Review", "On Hold", "Ready to Post", "Posted", "Ignored"]
    )
    filters_html = f"""
    <div class="card">
        <form method="GET" action="/">
            <div class="filters">
                <div class="field"><label>{esc(t("filter_status", lang))}</label><select name="status">
                    <option value="">{esc(t("queue_all_visible", lang))}</option>
                    {status_opts}
                </select></div>
                <div class="field"><label>{esc(t("filter_search", lang))}</label><input type="text" name="q" value="{esc(q)}" placeholder="{esc(t("filter_search_ph", lang))}"></div>
                <div class="field"><label>{esc(t("filter_queue_view", lang))}</label><select name="queue_mode">
                    <option value="all" {"selected" if not only_my_queue and not only_unassigned else ""}>{esc(t("queue_all_visible", lang))}</option>
                    <option value="mine" {"selected" if only_my_queue else ""}>{esc(t("queue_mine", lang))}</option>
                    <option value="unassigned" {"selected" if only_unassigned else ""}>{esc(t("queue_unassigned", lang))}</option>
                </select></div>
                <div class="field"><label>{esc(t("filter_ignored", lang))}</label><select name="include_ignored">
                    <option value="0" {"selected" if not include_ignored else ""}>{esc(t("ignored_hide", lang))}</option>
                    <option value="1" {"selected" if include_ignored else ""}>{esc(t("ignored_show", lang))}</option>
                </select></div>
                <div class="field" style="align-self:end;"><button class="btn-primary" type="submit">{esc(t("btn_filter", lang))}</button></div>
            </div>
        </form>
    </div>"""

    all_usernames = get_available_usernames()
    row_html: list[str] = []
    for row in rows:
        assigned = normalize_text(row["assigned_to"])
        is_mine = normalize_key(assigned) == normalize_key(ctx["username"])
        status_display = get_accounting_status(row)
        reason = get_plain_review_reason(row)
        next_action = get_next_action(row, ctx)
        next_action_display = esc(t(_NEXT_ACTION_KEYS.get(next_action, next_action), lang))

        if ctx["can_assign"]:
            opts = "".join(f'<option value="{esc(u)}" {"selected" if normalize_key(assigned)==normalize_key(u) else ""}>{esc(u)}</option>' for u in all_usernames)
            assign_ctrl = f"""<form method="POST" action="/assign" class="inline-form">
                <input type="hidden" name="document_id" value="{esc(row["document_id"])}">
                <input type="hidden" name="redirect_to" value="home">
                <select name="assigned_to" style="min-width:110px;"><option value="">{esc(t("option_unassigned", lang))}</option>{opts}</select>
                <button class="btn-secondary" type="submit">{esc(t("btn_assign", lang))}</button></form>"""
        elif not assigned:
            assign_ctrl = f"""<form method="POST" action="/claim" class="inline-form">
                <input type="hidden" name="document_id" value="{esc(row["document_id"])}">
                <input type="hidden" name="redirect_to" value="home">
                <button class="btn-secondary" type="submit">{esc(t("btn_claim", lang))}</button></form>"""
        else:
            assign_ctrl = esc(assigned or t("unassigned_label", lang))

        row_html.append(f"""<tr>
            <td class="file-cell"><a href="/document?id={urlquote(row["document_id"])}">{esc(row["file_name"])}</a>
                <div class="small muted">{esc(row["document_id"])}</div></td>
            <td>{esc(row["client_code"])}</td><td>{esc(row["vendor"])}</td>
            <td>{esc(row["amount"])}</td><td>{esc(row["document_date"])}</td>
            <td>{esc(row["category"])}</td><td>{esc(row["gl_account"])}</td>
            <td>{review_status_badge(status_display)}</td>
            <td>{assign_ctrl}</td>
            <td class="reason-cell">{esc(reason)}</td>
            <td>{next_action_display}</td></tr>""")

    no_docs_cell = f'<tr><td colspan=11 class=muted>{esc(t("no_documents_found", lang))}</td></tr>'
    table_html = f"""<div class="card"><table class="queue-table">
        <thead><tr>
            <th>{esc(t("col_document", lang))}</th><th>{esc(t("col_client", lang))}</th>
            <th>{esc(t("col_vendor", lang))}</th><th>{esc(t("col_amount", lang))}</th>
            <th>{esc(t("col_date", lang))}</th><th>{esc(t("col_category", lang))}</th>
            <th>{esc(t("col_gl_account", lang))}</th><th>{esc(t("col_status", lang))}</th>
            <th>{esc(t("col_assigned", lang))}</th><th>{esc(t("col_reason", lang))}</th>
            <th>{esc(t("col_action", lang))}</th>
        </tr></thead>
        <tbody>{"".join(row_html) if row_html else no_docs_cell}</tbody>
    </table></div>"""

    return page_layout(t("dashboard_title", lang), stats_html + filters_html + table_html,
                       user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Document detail page
# ---------------------------------------------------------------------------

def render_document(document_id: str, ctx: dict[str, Any], user: dict[str, Any],
                    flash: str, flash_error: str, lang: str = "fr") -> str:
    row = get_document(document_id)
    if row is None:
        return page_layout(
            t("err_doc_not_found", lang),
            f'<div class="card"><h2>{esc(t("err_doc_not_found", lang))}</h2><p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
            user=user, lang=lang)

    # Access control
    if not ctx["can_view_all_clients"]:
        allowed_keys = {normalize_key(c) for c in ctx.get("allowed_clients", [])}
        if normalize_key(row["client_code"]) not in allowed_keys:
            return page_layout(
                t("err_access_denied", lang),
                f'<div class="card"><h2>{esc(t("err_access_denied", lang))}</h2><p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                user=user, lang=lang)

    raw_result = safe_json_loads(row["raw_result"])
    accounting_status = get_accounting_status(row)
    review_reason = get_plain_review_reason(row)
    blocking_issues = compute_blocking_issues(row)
    blocking_html = "".join(f"<li>{esc(x)}</li>" for x in blocking_issues) or f"<li>{esc(t('badge_none', lang))}</li>"
    assigned = normalize_text(row["assigned_to"])

    try:
        human_summary = build_human_decision_summary(raw_result)
    except Exception:
        human_summary = "Could not generate summary."

    all_usernames = get_available_usernames()

    assign_title = t("doc_section_assignment", lang)
    if ctx["can_assign"]:
        opts = "".join(f'<option value="{esc(u)}" {"selected" if normalize_key(assigned)==normalize_key(u) else ""}>{esc(u)}</option>' for u in all_usernames)
        assign_card = f"""<div class="card"><h3>{esc(assign_title)}</h3>
            <form method="POST" action="/assign" class="inline-form">
                <input type="hidden" name="document_id" value="{esc(document_id)}">
                <input type="hidden" name="redirect_to" value="document">
                <select name="assigned_to" style="min-width:200px;"><option value="">{esc(t("option_unassigned", lang))}</option>{opts}</select>
                <button class="btn-secondary" type="submit">{esc(t("btn_save", lang))}</button>
            </form></div>"""
    elif not assigned:
        assign_card = f"""<div class="card"><h3>{esc(assign_title)}</h3>
            <form method="POST" action="/claim" class="inline-form">
                <input type="hidden" name="document_id" value="{esc(document_id)}">
                <input type="hidden" name="redirect_to" value="document">
                <button class="btn-secondary" type="submit">{esc(t("btn_claim_item", lang))}</button>
            </form></div>"""
    else:
        assign_card = f'<div class="card"><h3>{esc(assign_title)}</h3><p><strong>{esc(t("doc_assigned_to_label", lang))}</strong> {esc(assigned)}</p></div>'

    qbo_actions = ""
    if ctx["can_post"]:
        qbo_actions = f"""<div class="card"><h3>{esc(t("doc_section_accounting_actions", lang))}</h3><div class="actions">
            <form method="POST" action="/qbo/build"><input type="hidden" name="document_id" value="{esc(document_id)}"><button class="btn-primary" type="submit">{esc(t("btn_create_posting_job", lang))}</button></form>
            <form method="POST" action="/qbo/approve"><input type="hidden" name="document_id" value="{esc(document_id)}"><button class="btn-success" type="submit">{esc(t("btn_approve", lang))}</button></form>
            <form method="POST" action="/qbo/post"><input type="hidden" name="document_id" value="{esc(document_id)}"><button class="btn-dark" type="submit">{esc(t("btn_post_to_qbo", lang))}</button></form>
            <form method="POST" action="/qbo/retry"><input type="hidden" name="document_id" value="{esc(document_id)}"><button class="btn-warning" type="submit">{esc(t("btn_retry", lang))}</button></form>
        </div></div>"""

    status_options = "".join(f'<option value="{v}" {"selected" if normalize_text(row["review_status"])==v else ""}>{v}</option>'
                             for v in ["Ready","NeedsReview","Ignored","Exception"])

    file_path = normalize_text(row["file_path"])
    pdf_viewer_html = ""
    if file_path:
        suffix = Path(file_path).suffix.lower()
        pdf_url = f"/pdf?id={urlquote(document_id)}"
        preview_title = esc(t("doc_section_preview", lang))
        if suffix == ".pdf":
            pdf_viewer_html = f"""<div class="card"><h3>{preview_title}</h3>
                <iframe src="{pdf_url}" style="width:100%;height:800px;border:1px solid #e5e7eb;border-radius:8px;" title="{preview_title}"></iframe>
            </div>"""
        elif suffix in {".png", ".jpg", ".jpeg"}:
            pdf_viewer_html = f"""<div class="card"><h3>{preview_title}</h3>
                <img src="{pdf_url}" style="max-width:100%;border:1px solid #e5e7eb;border-radius:8px;" alt="Document image">
            </div>"""

    _doc_client_code = esc(normalize_text(row.get("client_code", "") or ""))
    _doc_id_esc = esc(document_id)
    timer_badge = (
        '<div style="margin-top:6px;">'
        '<span id="doc-timer" style="background:#dbeafe;color:#1e40af;'
        'padding:2px 10px;border-radius:999px;font-size:12px;font-weight:600;">'
        '\u23f1 0:00</span></div>'
    )
    timer_js = f"""<script>
(function(){{
    var _t0 = Date.now();
    var _el = document.getElementById('doc-timer');
    var _eid = null;
    setInterval(function(){{
        var s = Math.floor((Date.now() - _t0) / 1000);
        if (_el) _el.textContent = '\u23f1 ' + Math.floor(s / 60) + ':' + ('0' + s % 60).slice(-2);
    }}, 1000);
    fetch('/time/start', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/x-www-form-urlencoded'}},
        body: 'document_id=' + encodeURIComponent('{_doc_id_esc}') + '&client_code=' + encodeURIComponent('{_doc_client_code}')
    }}).then(function(r) {{ return r.json(); }}).then(function(d) {{ _eid = d.entry_id; }}).catch(function() {{}});
    window.addEventListener('beforeunload', function() {{
        if (!_eid) return;
        var mins = ((Date.now() - _t0) / 60000).toFixed(3);
        if (navigator.sendBeacon) {{
            navigator.sendBeacon('/time/stop', new URLSearchParams('entry_id=' + _eid + '&duration_minutes=' + mins));
        }}
    }});
}})();
</script>"""

    # Hallucination warning banner
    _hallucination_suspected = int(row["hallucination_suspected"] or 0)
    _confidence_val = float(row["confidence"] or 0.0)
    _show_hallucination_warning = _hallucination_suspected or _confidence_val < 0.7
    hallucination_banner = ""
    if _show_hallucination_warning:
        hallucination_banner = (
            f'<div style="background:#fef9c3;border:2px solid #ca8a04;border-radius:8px;'
            f'padding:14px 20px;margin-bottom:16px;font-weight:600;color:#713f12;">'
            f'\u26a0\ufe0f {esc(t("hallucination_warning", lang))}'
            f'</div>'
        )

    # Raw OCR text collapsible
    _raw_ocr = normalize_text(row["raw_ocr_text"]) if row["raw_ocr_text"] else ""
    raw_ocr_section = ""
    if _raw_ocr:
        raw_ocr_section = (
            f'<details style="margin-top:0;">'
            f'<summary style="cursor:pointer;font-weight:600;">'
            f'{esc(t("section_raw_ocr", lang))}</summary>'
            f'<div class="card" style="margin-top:8px;">'
            f'<p class="small muted" style="margin-bottom:8px;">'
            f'{esc(t("section_raw_ocr_hint", lang))}</p>'
            f'<textarea readonly style="width:100%;min-height:200px;font-family:monospace;font-size:12px;">'
            f'{esc(_raw_ocr)}</textarea>'
            f'</div></details>'
        )

    body = f"""
    <div class="card"><div class="actions" style="margin-bottom:12px;"><a href="/">{esc(t("btn_back_to_queue", lang))}</a></div>
        <h2 style="margin-bottom:8px;">{esc(row["file_name"])}</h2>
        <div class="small muted">{esc(t("doc_field_id", lang))} {esc(row["document_id"])}</div>
        {timer_badge}
    </div>
    {hallucination_banner}
    {pdf_viewer_html}
    <div class="card"><h3>{esc(t("doc_section_summary", lang))}</h3>
        <div class="grid-4">
            <div><strong>{esc(t("doc_field_status", lang))}</strong><div>{review_status_badge(accounting_status)}</div></div>
            <div><strong>{esc(t("doc_field_client", lang))}</strong><div>{esc(row["client_code"])}</div></div>
            <div><strong>{esc(t("doc_field_vendor", lang))}</strong><div>{esc(row["vendor"])}</div></div>
            <div><strong>{esc(t("doc_field_assigned_to", lang))}</strong><div>{esc(assigned or t("unassigned_label", lang))}</div></div>
            <div><strong>{esc(t("doc_field_amount", lang))}</strong><div>{esc(row["amount"])}</div></div>
            <div><strong>{esc(t("doc_field_date", lang))}</strong><div>{esc(row["document_date"])}</div></div>
            <div><strong>{esc(t("doc_field_category", lang))}</strong><div>{esc(row["category"])}</div></div>
            <div><strong>{esc(t("doc_field_gl_account", lang))}</strong><div>{esc(row["gl_account"])}</div></div>
        </div>
    </div>
    <div class="card"><h3>{esc(t("doc_section_attention", lang))}</h3>
        <div class="field"><label>{esc(t("doc_field_reason", lang))}</label><div class="summary-box">{esc(review_reason)}</div></div>
        <div class="field"><label>{esc(t("doc_field_blocking", lang))}</label><ul>{blocking_html}</ul></div>
    </div>
    {assign_card}
    <div class="card"><h3>{esc(t("doc_section_edit", lang))}</h3>
        <form method="POST" action="/document/update">
            <input type="hidden" name="document_id" value="{esc(document_id)}">
            <div class="grid-3">
                <div class="field"><label>{esc(t("field_vendor", lang))}</label><input type="text" name="vendor" value="{esc(row["vendor"])}"></div>
                <div class="field"><label>{esc(t("field_client_code", lang))}</label><input type="text" name="client_code" value="{esc(row["client_code"])}"></div>
                <div class="field"><label>{esc(t("field_doc_type", lang))}</label><input type="text" name="doc_type" value="{esc(row["doc_type"])}"></div>
                <div class="field"><label>{esc(t("field_amount", lang))}</label><input type="text" name="amount" value="{esc(row["amount"])}"></div>
                <div class="field"><label>{esc(t("field_document_date", lang))}</label><input type="text" name="document_date" value="{esc(row["document_date"])}"></div>
                <div class="field"><label>{esc(t("col_gl_account", lang))}</label><input type="text" name="gl_account" value="{esc(row["gl_account"])}"></div>
                <div class="field"><label>Tax Code</label><input type="text" name="tax_code" value="{esc(row["tax_code"])}"></div>
                <div class="field"><label>{esc(t("col_category", lang))}</label><input type="text" name="category" value="{esc(row["category"])}"></div>
                <div class="field"><label>{esc(t("field_review_status", lang))}</label><select name="review_status">{status_options}</select></div>
            </div>
            <button class="btn-primary" type="submit">{esc(t("btn_save_changes", lang))}</button>
        </form>
    </div>
    <div class="card"><h3>{esc(t("doc_section_hold", lang))}</h3><div class="grid-2">
        <form method="POST" action="/document/hold">
            <input type="hidden" name="document_id" value="{esc(document_id)}">
            <div class="field"><label>{esc(t("field_hold_reason", lang))}</label><input type="text" name="hold_reason" value="{esc(normalize_text(row["manual_hold_reason"]))}" placeholder="{esc(t("hold_reason_ph", lang))}"></div>
            <button class="btn-warning" type="submit">{esc(t("btn_put_on_hold", lang))}</button>
        </form>
        <form method="POST" action="/document/return_ready">
            <input type="hidden" name="document_id" value="{esc(document_id)}">
            <div class="field"><label>{esc(t("field_return_action", lang))}</label><div class="small muted" style="padding-top:10px;">{esc(t("return_action_hint", lang))}</div></div>
            <button class="btn-success" type="submit">{esc(t("btn_return_to_ready", lang))}</button>
        </form>
    </div></div>
    <div class="card"><h3>{esc(t("doc_section_posting", lang))}</h3><div class="grid-4">
        <div><strong>{esc(t("field_approval", lang))}</strong><div>{approval_state_badge(normalize_text(row["approval_state"]))}</div></div>
        <div><strong>{esc(t("field_posting", lang))}</strong><div>{posting_status_badge(normalize_text(row["posting_status"]))}</div></div>
        <div><strong>{esc(t("field_reviewer", lang))}</strong><div>{esc(row["posting_reviewer"])}</div></div>
        <div><strong>{esc(t("field_external_id", lang))}</strong><div>{esc(row["external_id"])}</div></div>
    </div></div>
    {qbo_actions}
    {render_doc_communications(document_id, row, ctx, lang)}
    {render_fraud_flags(row, lang)}
    {raw_ocr_section}
    <details><summary>{esc(t("doc_section_advanced", lang))}</summary><div style="margin-top:16px;">
        {render_posting_readiness(row, lang)}
        {render_vendor_memory(raw_result, lang)}
        {render_auto_approval(raw_result, lang)}
        {render_learning_suggestions(document_id, row, ctx["username"], lang)}
        {render_learning_history(document_id, lang)}
        <div class="card"><h3>{esc(t("section_explain", lang))}</h3>
            <div class="field"><label>{esc(t("section_summary_lbl", lang))}</label><div class="summary-box">{esc(human_summary)}</div></div>
            <details><summary>{esc(t("section_raw_json", lang))}</summary><div class="field" style="margin-top:12px;"><textarea readonly>{esc(json.dumps(raw_result, indent=2))}</textarea></div></details>
        </div>
        <div class="card"><h3>{esc(t("section_technical", lang))}</h3><div class="grid-3">
            <div><strong>{esc(t("field_confidence", lang))}</strong><div>{esc(row["confidence"])}</div></div>
            <div><strong>{esc(t("field_hold_by", lang))}</strong><div>{esc(row["manual_hold_by"])}</div></div>
            <div><strong>{esc(t("field_hold_at", lang))}</strong><div>{esc(row["manual_hold_at"])}</div></div>
            <div><strong>{esc(t("field_assigned_by", lang))}</strong><div>{esc(row["assigned_by"])}</div></div>
            <div><strong>{esc(t("field_assigned_at", lang))}</strong><div>{esc(row["assigned_at"])}</div></div>
            <div><strong>{esc(t("field_file_path", lang))}</strong><div>{esc(row["file_path"])}</div></div>
        </div></div>
    </div></details>
    {timer_js}"""

    return page_layout(f"Document — {normalize_text(row['file_name'])}", body,
                       user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Period-lock guard (called from document-mutation POST handlers)
# ---------------------------------------------------------------------------

def _check_period_not_locked_for_doc(document_id: str, lang: str = "fr") -> None:
    """Raise ValueError if the document's accounting period is locked."""
    doc = get_document(document_id)
    if not doc:
        return
    client_code = normalize_text(doc["client_code"])
    period = get_document_period(normalize_text(doc["document_date"] or ""))
    if not client_code or not period:
        return
    with open_db() as conn:
        if is_period_locked(conn, client_code, period):
            raise ValueError(t("err_pc_period_locked", lang))


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class ReviewDashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_html(self, content: str, status: int = 200, extra_headers: list[tuple[str, str]] | None = None) -> None:
        body = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if extra_headers:
            for k, v in extra_headers:
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, data: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_db_backup(self) -> None:
        """Stream the SQLite database file as a binary download."""
        if not DB_PATH.exists():
            self._send_html(page_layout("Error", '<div class="card"><h2>Database not found</h2></div>'), status=404)
            return
        data = DB_PATH.read_bytes()
        filename = f"ledgerlink_backup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.db"
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _serve_pdf(self, document_id: str, user: dict[str, Any]) -> None:
        """Serve the raw PDF file for a document, gated by session auth."""
        if not document_id:
            self._send_html(page_layout("Bad Request", '<div class="card"><h2>Missing document id</h2></div>', user=user), status=400)
            return
        row = get_document(document_id)
        if row is None:
            self._send_html(page_layout("Not Found", '<div class="card"><h2>Document not found</h2></div>', user=user), status=404)
            return
        # Access control
        ctx = build_user_context(user)
        if not ctx["can_view_all_clients"]:
            allowed_keys = {normalize_key(c) for c in ctx.get("allowed_clients", [])}
            if normalize_key(row["client_code"]) not in allowed_keys:
                self._send_html(page_layout("Access Denied", '<div class="card"><h2>Access denied</h2></div>', user=user), status=403)
                return
        file_path = normalize_text(row["file_path"])
        if not file_path:
            self._send_html(page_layout("Not Found", '<div class="card"><h2>No file path recorded for this document</h2></div>', user=user), status=404)
            return
        path_obj = Path(file_path)
        # Resolve and safety-check — must be an existing file
        try:
            resolved = path_obj.resolve(strict=True)
        except (OSError, RuntimeError):
            self._send_html(page_layout("Not Found", f'<div class="card"><h2>File not found on disk</h2><p class="muted">{esc(file_path)}</p></div>', user=user), status=404)
            return
        suffix = resolved.suffix.lower()
        if suffix == ".pdf":
            content_type = "application/pdf"
        elif suffix in {".png", ".jpg", ".jpeg"}:
            content_type = f"image/{'jpeg' if suffix in {'.jpg', '.jpeg'} else 'png'}"
        else:
            self._send_html(page_layout("Unsupported", '<div class="card"><h2>Preview not supported for this file type</h2></div>', user=user), status=415)
            return
        data = resolved.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Content-Disposition", f'inline; filename="{esc(resolved.name)}"')
        self.end_headers()
        self.wfile.write(data)

    def _serve_period_close_pdf(
        self, client_code: str, period: str, user: dict[str, Any], lang: str
    ) -> None:
        """Generate and stream the period-close PDF summary."""
        if not client_code or not period:
            self._send_html(page_layout(
                "Bad Request",
                '<div class="card"><h2>Client code and period are required</h2></div>',
                user=user), status=400)
            return
        with open_db() as conn:
            pdf_bytes = generate_period_close_pdf(conn, client_code, period, lang=lang)
        filename = f"period_close_{client_code}_{period}.pdf"
        self.send_response(200)
        self.send_header("Content-Type", "application/pdf")
        self.send_header("Content-Length", str(len(pdf_bytes)))
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(pdf_bytes)

    def _serve_revenu_quebec_pdf(
        self,
        client_code: str,
        period_start: str,
        period_end: str,
        user: dict[str, Any],
        lang: str,
    ) -> None:
        """Generate and stream the Revenu Québec pre-fill summary PDF."""
        if not client_code or not period_start or not period_end:
            self._send_html(page_layout(
                "Bad Request",
                '<div class="card"><h2>Client code and period are required</h2></div>',
                user=user), status=400)
            return
        with open_db() as conn:
            ensure_client_config_table(conn)
            prefill = compute_prefill(client_code, period_start, period_end, conn)
        generated_at = utc_now().strftime("%Y-%m-%d %H:%M UTC")
        pdf_bytes = generate_revenu_quebec_pdf(
            client_code=client_code,
            period_start=period_start,
            period_end=period_end,
            prefill=prefill,
            generated_at=generated_at,
        )
        filename = f"revenu_quebec_{client_code}_{period_start}_{period_end}.pdf"
        self.send_response(200)
        self.send_header("Content-Type", "application/pdf")
        self.send_header("Content-Length", str(len(pdf_bytes)))
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(pdf_bytes)

    def _redirect(self, location: str, extra_headers: list[tuple[str, str]] | None = None) -> None:
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
        if extra_headers:
            for k, v in extra_headers:
                self.send_header(k, v)
        self.end_headers()

    def _get_qs(self) -> dict[str, list[str]]:
        return urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query, keep_blank_values=True)

    def _flash_redirect(self, location: str, flash: str = "", error: str = "") -> None:
        sep = "&" if "?" in location else "?"
        if flash:
            location += sep + "flash=" + urlquote(flash)
            sep = "&"
        if error:
            location += sep + "error=" + urlquote(error)
        self._redirect(location)

    def _get_lang_from_cookie(self) -> str:
        cookie = self.headers.get("Cookie", "")
        for part in cookie.split(";"):
            part = part.strip()
            if part.startswith("dashboard_lang="):
                val = part[len("dashboard_lang="):]
                return val if val in ("fr", "en") else "fr"
        return "fr"

    def do_GET(self) -> None:
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            flash = qs.get("flash", [""])[0]
            flash_error = qs.get("error", [""])[0]

            if path == "/login":
                lang_qs = qs.get("lang", [""])[0]
                lang = lang_qs if lang_qs in ("fr", "en") else self._get_lang_from_cookie()
                self._send_html(render_login(flash_error, lang=lang))
                return

            user = get_session_user(self)
            if not user:
                self._redirect("/login")
                return

            lang = get_user_lang(user)
            ctx = build_user_context(user)

            if path == "/":
                status = qs.get("status", [""])[0]
                q = qs.get("q", [""])[0]
                include_ignored = qs.get("include_ignored", ["0"])[0] == "1"
                queue_mode = qs.get("queue_mode", ["all"])[0]
                self._send_html(render_home(ctx, user, status, q, flash, flash_error,
                                            include_ignored, queue_mode == "mine",
                                            queue_mode == "unassigned", lang=lang))
                return

            if path == "/change_password":
                self._send_html(render_change_password(user, flash_error=flash_error, lang=lang))
                return

            if path == "/pdf":
                document_id = qs.get("id", [""])[0]
                self._serve_pdf(document_id, user)
                return

            if path == "/document":
                document_id = qs.get("id", [""])[0]
                self._send_html(render_document(document_id, ctx, user, flash, flash_error, lang=lang))
                return

            if path == "/portfolios":
                self._send_html(render_portfolios(ctx, user, flash, flash_error, lang=lang))
                return

            if path == "/users":
                self._send_html(render_user_management(ctx, user, flash, flash_error, lang=lang))
                return

            if path == "/troubleshoot":
                if user.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._send_html(render_troubleshoot(ctx, user, flash, flash_error, lang=lang))
                return

            if path == "/troubleshoot/backup":
                if user.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_owner_required", lang))}</p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._serve_db_backup()
                return

            if path == "/filing_summary":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                client_code  = qs.get("client_code",  [""])[0].strip()
                period_start = qs.get("period_start",  [""])[0].strip()
                period_end   = qs.get("period_end",    [""])[0].strip()
                self._send_html(render_filing_summary(
                    ctx, user, client_code, period_start, period_end, flash, flash_error, lang=lang))
                return

            if path in ("/revenu_quebec", "/revenu_quebec/pdf"):
                if user.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                rq_client = qs.get("client_code",  [""])[0].strip()
                rq_start  = qs.get("period_start", [""])[0].strip()
                rq_end    = qs.get("period_end",   [""])[0].strip()
                if path == "/revenu_quebec/pdf":
                    self._serve_revenu_quebec_pdf(rq_client, rq_start, rq_end, user, lang)
                else:
                    self._send_html(render_revenu_quebec(
                        ctx, user, rq_client, rq_start, rq_end,
                        flash, flash_error, lang=lang))
                return

            if path == "/period_close":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                pc_client = qs.get("client_code", [""])[0].strip()
                pc_period = qs.get("period", [""])[0].strip()
                self._send_html(render_period_close(
                    ctx, user, pc_client, pc_period, flash, flash_error, lang=lang))
                return

            if path == "/period_close/pdf":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p></div>',
                        user=user, lang=lang), status=403)
                    return
                pc_client = qs.get("client_code", [""])[0].strip()
                pc_period = qs.get("period", [""])[0].strip()
                self._serve_period_close_pdf(pc_client, pc_period, user, lang)
                return

            if path == "/time":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                tc_client = qs.get("client_code", [""])[0].strip()
                tc_start  = qs.get("period_start",  [""])[0].strip()
                tc_end    = qs.get("period_end",    [""])[0].strip()
                tc_rate   = qs.get("hourly_rate",   [""])[0].strip()
                self._send_html(render_time_summary(
                    ctx, user, tc_client, tc_start, tc_end, tc_rate,
                    flash, flash_error, lang=lang))
                return

            if path == "/bank_import":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._send_html(render_bank_import(ctx, user, flash, flash_error, lang=lang))
                return

            if path == "/communications":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                comm_client = qs.get("client_code", [""])[0].strip()
                self._send_html(render_communications(
                    ctx, user, comm_client, flash, flash_error, lang=lang))
                return

            if path == "/analytics":
                if user.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._send_html(render_analytics(ctx, user, flash, flash_error, lang=lang))
                return

            if path == "/calendar":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_cal_forbidden", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._send_html(render_calendar(ctx, user, flash, flash_error, lang=lang))
                return

            # ------------------------------------------------------------------
            # Audit module — GET routes
            # ------------------------------------------------------------------

            if path == "/working_papers":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                wp_client = qs.get("client_code", [""])[0].strip()
                wp_period = qs.get("period", [""])[0].strip()
                wp_type   = qs.get("engagement_type", ["audit"])[0].strip()
                self._send_html(render_working_papers(ctx, user, wp_client, wp_period, wp_type, flash, flash_error, lang=lang))
                return

            if path == "/working_papers/pdf":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                wp_client = qs.get("client_code", [""])[0].strip()
                wp_period = qs.get("period", [""])[0].strip()
                wp_type   = qs.get("engagement_type", ["audit"])[0].strip()
                with open_db() as conn:
                    pdf_bytes = _audit.generate_lead_sheet_pdf(conn, wp_client, wp_period, wp_type, lang=lang)
                filename = f"lead_sheet_{wp_client}_{wp_period}.pdf"
                self.send_response(200)
                self.send_header("Content-Type", "application/pdf")
                self.send_header("Content-Length", str(len(pdf_bytes)))
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.end_headers()
                self.wfile.write(pdf_bytes)
                return

            if path == "/audit/evidence":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                ev_client = qs.get("client_code", [""])[0].strip()
                ev_period = qs.get("period", [""])[0].strip()
                self._send_html(render_audit_evidence(ctx, user, ev_client, ev_period, flash, flash_error, lang=lang))
                return

            if path == "/audit/sample":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                samp_client  = qs.get("client_code", [""])[0].strip()
                samp_period  = qs.get("period", [""])[0].strip()
                samp_account = qs.get("account_code", [""])[0].strip()
                samp_size    = qs.get("sample_size", ["10"])[0].strip()
                samp_paper   = qs.get("paper_id", [""])[0].strip()
                self._send_html(render_audit_sample(ctx, user, samp_client, samp_period, samp_account, samp_size, samp_paper, flash, flash_error, lang=lang))
                return

            if path == "/financial_statements":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                fs_client = qs.get("client_code", [""])[0].strip()
                fs_period = qs.get("period", [""])[0].strip()
                self._send_html(render_financial_statements_page(ctx, user, fs_client, fs_period, flash, flash_error, lang=lang))
                return

            if path == "/financial_statements/pdf":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                fs_client = qs.get("client_code", [""])[0].strip()
                fs_period = qs.get("period", [""])[0].strip()
                with open_db() as conn:
                    pdf_bytes = _audit.generate_financial_statements_pdf(conn, fs_client, fs_period, lang=lang)
                filename = f"financial_statements_{fs_client}_{fs_period}.pdf"
                self.send_response(200)
                self.send_header("Content-Type", "application/pdf")
                self.send_header("Content-Length", str(len(pdf_bytes)))
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.end_headers()
                self.wfile.write(pdf_bytes)
                return

            if path == "/audit/analytical":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                anal_client = qs.get("client_code", [""])[0].strip()
                anal_period = qs.get("period", [""])[0].strip()
                self._send_html(render_analytical(ctx, user, anal_client, anal_period, flash, flash_error, lang=lang))
                return

            if path == "/audit/analytical/pdf":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                anal_client = qs.get("client_code", [""])[0].strip()
                anal_period = qs.get("period", [""])[0].strip()
                with open_db() as conn:
                    pdf_bytes = _audit.generate_analytical_report_pdf(conn, anal_client, anal_period, lang=lang)
                filename = f"analytical_{anal_client}_{anal_period}.pdf"
                self.send_response(200)
                self.send_header("Content-Type", "application/pdf")
                self.send_header("Content-Length", str(len(pdf_bytes)))
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.end_headers()
                self.wfile.write(pdf_bytes)
                return

            if path == "/engagements":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                eng_client = qs.get("client_code", [""])[0].strip()
                eng_status = qs.get("status", [""])[0].strip()
                self._send_html(render_engagements(ctx, user, eng_client, eng_status, flash, flash_error, lang=lang))
                return

            if path == "/engagements/detail":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                eng_id = qs.get("id", [""])[0].strip()
                self._send_html(render_engagement_detail(ctx, user, eng_id, flash, flash_error, lang=lang))
                return

            if path == "/license":
                if user.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_lic_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_lic_forbidden", lang))}</h2>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._send_html(render_license_page(ctx, user, flash, flash_error, lang=lang))
                return

            self._send_html(page_layout(
                t("err_not_found", lang),
                f'<div class="card"><h2>{esc(t("err_not_found", lang))}</h2>'
                f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                user=user, lang=lang), status=404)

        except Exception:
            self._send_html(page_layout("Error",
                f'<div class="card"><h2>Unhandled Error</h2><pre>{esc(traceback.format_exc())}</pre></div>'), status=500)

    def do_POST(self) -> None:
        document_id = ""
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            form = parse_form_body(raw)
            path = urllib.parse.urlparse(self.path).path
            document_id = form.get("document_id", "")

            # --- Login (no auth required) ---
            if path == "/login":
                lang = normalize_text(form.get("lang") or "fr")
                if lang not in ("fr", "en"):
                    lang = "fr"
                username = form.get("username", "").strip()
                password = form.get("password", "")
                ip = _get_client_ip(self)

                # Rate-limit: 5 failures per IP in 15 minutes → HTTP 429
                if is_rate_limited(ip):
                    record_login_attempt(ip, username, False)
                    body = render_login(_RATE_LIMIT_MSG, lang=lang).encode("utf-8")
                    self.send_response(429)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Retry-After", "900")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                with open_db() as conn:
                    user_row = conn.execute(
                        "SELECT * FROM dashboard_users WHERE username=? AND active=1", (username,)
                    ).fetchone()
                # Unified error message for both "user not found" and "wrong password"
                if not user_row or not verify_password(password, user_row["password_hash"]):
                    record_login_attempt(ip, username, False)
                    self._send_html(render_login(t("login_invalid", lang), lang=lang))
                    return
                # Upgrade legacy SHA-256 hash to bcrypt on successful login
                stored = user_row["password_hash"]
                is_legacy = not stored.startswith(("$2b$", "$2a$", "$2y$"))
                record_login_attempt(ip, username, True)
                token = create_session(username)
                sec = _session_cookie_attrs(self)
                if is_legacy or user_row["must_reset_password"]:
                    if is_legacy:
                        # Re-hash with bcrypt immediately
                        with open_db() as conn:
                            conn.execute(
                                "UPDATE dashboard_users SET password_hash=?, must_reset_password=1 WHERE username=?",
                                (hash_password(password), username),
                            )
                            conn.commit()
                    self._redirect("/change_password", extra_headers=[
                        ("Set-Cookie", f"session_token={token}; HttpOnly; {sec}; Path=/"),
                        ("Set-Cookie", f"dashboard_lang={lang}; {sec}; Path=/"),
                    ])
                else:
                    self._redirect("/", extra_headers=[
                        ("Set-Cookie", f"session_token={token}; HttpOnly; {sec}; Path=/"),
                        ("Set-Cookie", f"dashboard_lang={lang}; {sec}; Path=/"),
                    ])
                return

            # --- Logout (no auth required) ---
            if path == "/logout":
                token = get_token_from_cookie(self)
                if token:
                    delete_session(token)
                sec = _session_cookie_attrs(self)
                self._redirect("/login", extra_headers=[
                    ("Set-Cookie", f"session_token=; HttpOnly; {sec}; Path=/; Max-Age=0")
                ])
                return

            # --- Change password (requires active session, no must_reset check) ---
            if path == "/change_password":
                user = get_session_user(self)
                if not user:
                    self._redirect("/login")
                    return
                lang = get_user_lang(user)
                new_pw = form.get("new_password", "")
                confirm_pw = form.get("confirm_password", "")
                if not new_pw or len(new_pw) < 8:
                    self._send_html(render_change_password(user, flash_error=t("change_pw_too_short", lang), lang=lang))
                    return
                if new_pw != confirm_pw:
                    self._send_html(render_change_password(user, flash_error=t("change_pw_mismatch", lang), lang=lang))
                    return
                with open_db() as conn:
                    conn.execute(
                        "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 WHERE username=?",
                        (hash_password(new_pw), user["username"]),
                    )
                    conn.commit()
                self._flash_redirect("/", flash=t("flash_pw_updated", lang))
                return

            # All other POSTs require auth
            user = get_session_user(self)
            if not user:
                self._redirect("/login")
                return
            lang = get_user_lang(user)
            ctx = build_user_context(user)
            redirect_to = form.get("redirect_to", "document")

            # --- Set language ---
            if path == "/set_language":
                new_lang = normalize_text(form.get("lang", "fr"))
                if new_lang not in ("fr", "en"):
                    new_lang = "fr"
                with open_db() as conn:
                    conn.execute(
                        "UPDATE dashboard_users SET language=? WHERE username=?",
                        (new_lang, user["username"]),
                    )
                    conn.commit()
                referer = self.headers.get("Referer", "/")
                sec = _session_cookie_attrs(self)
                self._redirect(referer, extra_headers=[
                    ("Set-Cookie", f"dashboard_lang={new_lang}; {sec}; Path=/"),
                ])
                return

            if path == "/document/update":
                _check_period_not_locked_for_doc(document_id, lang)
                before_row = get_document(document_id)
                if before_row is None:
                    raise ValueError(t("err_doc_not_found", lang))
                submitted = {k: form.get(k, "") for k in ["vendor","client_code","doc_type","amount","document_date","gl_account","tax_code","category","review_status"]}
                update_document_fields(document_id, submitted)
                record_learning_corrections(document_id, before_row, submitted)
                try:
                    from src.agents.core.hallucination_guard import track_correction_count
                    track_correction_count(document_id, before_row, submitted, db_path=DB_PATH)
                except Exception:
                    pass
                self._flash_redirect(f"/document?id={urlquote(document_id)}", flash=t("flash_doc_updated", lang))
                return

            if path == "/document/hold":
                _check_period_not_locked_for_doc(document_id, lang)
                hold_reason = form.get("hold_reason", "")
                if not normalize_text(hold_reason):
                    raise ValueError(t("err_hold_required", lang))
                set_manual_hold(document_id, hold_reason, ctx["username"])
                self._flash_redirect(f"/document?id={urlquote(document_id)}", flash=t("flash_on_hold", lang))
                return

            if path == "/document/return_ready":
                _check_period_not_locked_for_doc(document_id, lang)
                clear_manual_hold(document_id)
                set_document_status(document_id, "Ready")
                self._flash_redirect(f"/document?id={urlquote(document_id)}", flash=t("flash_return_ready", lang))
                return

            if path == "/assign":
                assign_document(document_id, form.get("assigned_to", ""), ctx["username"])
                dest = "/" if redirect_to == "home" else f"/document?id={urlquote(document_id)}"
                self._flash_redirect(dest, flash=t("flash_assignment_updated", lang))
                return

            if path == "/claim":
                assign_document(document_id, ctx["username"], ctx["username"], note="claimed from dashboard")
                dest = "/" if redirect_to == "home" else f"/document?id={urlquote(document_id)}"
                self._flash_redirect(dest, flash=t("flash_item_claimed", lang))
                return

            if path == "/apply_suggestion":
                before_row = get_document(document_id)
                if before_row is None:
                    raise ValueError(t("err_doc_not_found", lang))
                update_document_fields(document_id, {form.get("field",""): form.get("value","")})
                record_learning_corrections(document_id, before_row, {form.get("field",""): form.get("value","")})
                self._flash_redirect(f"/document?id={urlquote(document_id)}", flash=t("flash_suggestion_applied", lang))
                return

            if path == "/qbo/build":
                payload = build_posting_job(document_id=document_id, target_system="qbo", entry_kind="expense", db_path=DB_PATH)
                self._flash_redirect(f"/document?id={urlquote(document_id)}",
                                     flash=t("flash_posting_job_created", lang) + ": " + normalize_text(payload.posting_id))
                return

            if path == "/qbo/approve":
                doc_row = get_document(document_id)
                if doc_row is not None:
                    raw_doc = safe_json_loads(doc_row["raw_result"])
                    vendor_province = str(raw_doc.get("vendor_province", "") or "").strip()
                    tax_check = validate_tax_code(doc_row["gl_account"], doc_row["tax_code"], vendor_province)
                    if not tax_check["valid"]:
                        self._flash_redirect(
                            f"/document?id={urlquote(document_id)}",
                            error=t("err_tax_validation", lang) + ": " + "; ".join(tax_check["warnings"]),
                        )
                        return
                    try:
                        from src.agents.core.hallucination_guard import (
                            verify_numeric_totals, record_math_mismatch,
                        )
                        math_check = verify_numeric_totals(raw_doc)
                        if not math_check["ok"] and not math_check.get("skipped"):
                            record_math_mismatch(
                                document_id,
                                math_check["delta"],
                                math_check["computed"],
                                math_check["claimed_total"],
                                db_path=DB_PATH,
                            )
                            self._flash_redirect(
                                f"/document?id={urlquote(document_id)}",
                                error=t("err_math_mismatch", lang),
                            )
                            return
                    except Exception:
                        pass
                posting = get_qbo_posting_job(document_id)
                if posting is None:
                    build_posting_job(document_id=document_id, target_system="qbo", entry_kind="expense", db_path=DB_PATH)
                    posting = get_qbo_posting_job(document_id)
                if posting is None:
                    raise ValueError("Could not create posting job")
                payload = approve_posting_job(posting_id=normalize_text(posting["posting_id"]), reviewer=DEFAULT_REVIEWER, db_path=DB_PATH)
                clear_manual_hold(document_id)
                set_document_status(document_id, "Ready")
                self._flash_redirect(f"/document?id={urlquote(document_id)}",
                                     flash=t("flash_approved", lang) + ": " + normalize_text(payload.posting_id))
                return

            if path == "/qbo/post":
                posting = get_qbo_posting_job(document_id)
                if posting is None:
                    build_posting_job(document_id=document_id, target_system="qbo", entry_kind="expense", db_path=DB_PATH)
                    posting = get_qbo_posting_job(document_id)
                if posting is None:
                    raise ValueError("Could not create posting job")
                posting_id = normalize_text(posting["posting_id"])
                if normalize_text(posting["posting_status"]) == "post_failed":
                    retry_posting_job(posting_id=posting_id, reviewer=DEFAULT_REVIEWER, note="retry from dashboard", db_path=DB_PATH)
                elif normalize_text(posting["approval_state"]) != "approved_for_posting" or normalize_text(posting["posting_status"]) != "ready_to_post":
                    approve_posting_job(posting_id=posting_id, reviewer=DEFAULT_REVIEWER, db_path=DB_PATH)
                result = qbo_post_one_ready_job(posting_id, db_path=DB_PATH)
                if normalize_text(result.get("status")) == "posted":
                    set_document_status(document_id, "Ready")
                    clear_manual_hold(document_id)
                    msg = t("flash_posted_to_qbo", lang)
                    ext = normalize_text(result.get("external_id"))
                    if ext:
                        msg += f". {t('flash_ext_id', lang)}: {ext}"
                    self._flash_redirect(f"/document?id={urlquote(document_id)}", flash=msg)
                else:
                    self._flash_redirect(f"/document?id={urlquote(document_id)}",
                                         error=normalize_text(result.get("error")) or t("err_qbo_failed", lang))
                return

            if path == "/qbo/retry":
                posting = get_qbo_posting_job(document_id)
                if posting is None:
                    raise ValueError("No posting job exists for this document")
                payload = retry_posting_job(posting_id=normalize_text(posting["posting_id"]), reviewer=DEFAULT_REVIEWER, note="retry from dashboard", db_path=DB_PATH)
                self._flash_redirect(f"/document?id={urlquote(document_id)}",
                                     flash=t("flash_retry_prepared", lang) + ": " + normalize_text(payload.posting_id))
                return

            # Portfolio routes
            if path == "/portfolios/assign":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                username_target = form.get("username_target", "")
                client_code = form.get("client_code", "")
                if username_target and client_code:
                    assign_client_to_user(client_code, username_target, ctx["username"])
                self._flash_redirect("/portfolios",
                    flash=f"{client_code} {t('flash_assigned_to', lang)} {username_target}")
                return

            if path == "/portfolios/remove":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                username_target = form.get("username_target", "")
                client_code = form.get("client_code", "")
                if username_target and client_code:
                    remove_client_from_user(client_code, username_target)
                self._flash_redirect("/portfolios",
                    flash=f"{client_code} {t('flash_removed_from', lang)} {username_target}")
                return

            if path == "/portfolios/move":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                from_user = form.get("from_user", "")
                to_user = form.get("to_user", "")
                client_code = form.get("client_code", "")
                if from_user and to_user and client_code:
                    move_client_to_user(client_code, from_user, to_user, ctx["username"])
                self._flash_redirect("/portfolios",
                    flash=f"{client_code} {t('flash_moved_to', lang)} {to_user}")
                return

            # User management routes
            if path == "/users/add":
                if ctx["role"] != "owner":
                    raise ValueError("Only owners can add users")
                username = normalize_text(form.get("username", ""))
                password = form.get("password", "")
                role = normalize_text(form.get("role", "employee"))
                display_name = normalize_text(form.get("display_name", "")) or username
                if not username or not password:
                    raise ValueError("Username and password are required")
                with open_db() as conn:
                    conn.execute(
                        "INSERT INTO dashboard_users (username, password_hash, role, display_name, active, created_at) VALUES (?,?,?,?,1,?)",
                        (username, hash_password(password), role, display_name, utc_now_iso()),
                    )
                    conn.commit()
                self._flash_redirect("/users",
                    flash=f"{t('flash_user_created', lang)}: {username}")
                return

            if path == "/users/set_password":
                if ctx["role"] != "owner":
                    raise ValueError("Only owners can change passwords")
                username_target = normalize_text(form.get("username_target", ""))
                new_password = form.get("new_password", "")
                if not username_target or not new_password:
                    raise ValueError("Username and new password are required")
                with open_db() as conn:
                    conn.execute(
                        "UPDATE dashboard_users SET password_hash=? WHERE username=?",
                        (hash_password(new_password), username_target),
                    )
                    conn.commit()
                self._flash_redirect("/users",
                    flash=f"{t('flash_pw_updated_for', lang)} {username_target}")
                return

            if path == "/troubleshoot/restart":
                if user.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2></div>',
                        user=user, lang=lang), status=403)
                    return
                self._flash_redirect("/troubleshoot", flash=t("flash_service_restart", lang))
                import os as _os
                _os.execv(sys.executable, [sys.executable] + sys.argv)
                return  # unreachable; satisfies linters

            if path == "/period_close/check_item":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                item_id_str = normalize_text(form.get("item_id", ""))
                if not item_id_str.isdigit():
                    raise ValueError("Invalid item_id")
                item_id_int = int(item_id_str)
                pc_status = normalize_text(form.get("status", "open"))
                pc_notes = normalize_text(form.get("notes", ""))
                pc_resp = normalize_text(form.get("responsible_user", ""))
                pc_due = normalize_text(form.get("due_date", ""))
                pc_cc = normalize_text(form.get("client_code", ""))
                pc_per = normalize_text(form.get("period", ""))
                with open_db() as conn:
                    update_checklist_item(
                        conn, item_id_int, pc_status,
                        completed_by=ctx["username"],
                        notes=pc_notes,
                        responsible_user=pc_resp,
                        due_date=pc_due,
                    )
                dest = f"/period_close?client_code={urlquote(pc_cc)}&period={urlquote(pc_per)}"
                self._flash_redirect(dest, flash=t("flash_pc_item_updated", lang))
                return

            if path == "/period_close/lock":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                pc_cc = normalize_text(form.get("client_code", ""))
                pc_per = normalize_text(form.get("period", ""))
                if not pc_cc or not pc_per:
                    raise ValueError(t("err_pc_client_period_required", lang))
                with open_db() as conn:
                    if is_period_locked(conn, pc_cc, pc_per):
                        raise ValueError(t("err_pc_period_locked", lang))
                    if not is_period_complete(conn, pc_cc, pc_per):
                        raise ValueError(t("err_pc_items_open", lang))
                    lock_period(conn, pc_cc, pc_per, ctx["username"])
                dest = f"/period_close?client_code={urlquote(pc_cc)}&period={urlquote(pc_per)}"
                self._flash_redirect(dest, flash=t("flash_pc_period_locked", lang))
                return

            if path == "/time/start":
                doc_id_t  = normalize_text(form.get("document_id", ""))
                client_t  = normalize_text(form.get("client_code", ""))
                with open_db() as conn:
                    entry_id = start_time_entry(
                        conn, ctx["username"], client_t, doc_id_t or None)
                self._send_json({"entry_id": entry_id})
                return

            if path == "/time/stop":
                entry_id_str = normalize_text(form.get("entry_id", ""))
                duration_str = normalize_text(form.get("duration_minutes", "0"))
                if entry_id_str.isdigit():
                    try:
                        dur = float(duration_str)
                    except ValueError:
                        dur = 0.0
                    with open_db() as conn:
                        stop_time_entry(conn, int(entry_id_str), dur)
                self.send_response(204)
                self.end_headers()
                return

            if path == "/invoice/generate":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                inv_client      = normalize_text(form.get("client_code", ""))
                inv_start       = normalize_text(form.get("period_start", ""))
                inv_end         = normalize_text(form.get("period_end", ""))
                inv_rate_str    = normalize_text(form.get("hourly_rate", "0"))
                inv_firm        = normalize_text(form.get("firm_name", ""))
                inv_client_name = normalize_text(form.get("client_name", ""))
                inv_gst_num     = normalize_text(form.get("gst_number", ""))
                inv_qst_num     = normalize_text(form.get("qst_number", ""))
                if not all([inv_client, inv_start, inv_end, inv_firm,
                            inv_client_name, inv_gst_num, inv_qst_num]):
                    raise ValueError("All invoice fields are required.")
                try:
                    hourly_rate = Decimal(inv_rate_str)
                except Exception:
                    raise ValueError("Invalid hourly rate.")
                with open_db() as conn:
                    summary = get_time_summary(conn, inv_client, inv_start, inv_end)
                    entries = summary["entries"]
                    billable_hours = Decimal(str(summary["billable_hours"]))
                    if billable_hours <= 0:
                        raise ValueError(t("time_no_entries", lang))
                    subtotal = (hourly_rate * billable_hours).quantize(Decimal("0.01"))
                    tax = calculate_gst_qst(subtotal)
                    inv_number = generate_invoice_number()
                    inv_date   = utc_now().strftime("%Y-%m-%d")
                    ensure_time_tables(conn)
                    conn.execute(
                        """
                        INSERT INTO invoices
                            (invoice_id, client_code, period_start, period_end,
                             generated_by, generated_at, hourly_rate, subtotal,
                             gst_amount, qst_amount, total_amount, entry_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (inv_number, inv_client, inv_start, inv_end,
                         ctx["username"], inv_date, float(hourly_rate),
                         float(subtotal), float(tax["gst"]),
                         float(tax["qst"]), float(tax["total_with_tax"]),
                         len(entries)),
                    )
                    conn.commit()
                pdf_bytes = generate_invoice_pdf(
                    invoice_number=inv_number,
                    invoice_date=inv_date,
                    firm_name=inv_firm,
                    gst_number=inv_gst_num,
                    qst_number=inv_qst_num,
                    client_name=inv_client_name,
                    client_code=inv_client,
                    period_start=inv_start,
                    period_end=inv_end,
                    hourly_rate=hourly_rate,
                    billable_hours=billable_hours,
                    entries=entries,
                    lang=lang,
                )
                filename = f"invoice_{inv_client}_{inv_start}_{inv_end}_{inv_number}.pdf"
                self.send_response(200)
                self.send_header("Content-Type", "application/pdf")
                self.send_header("Content-Length", str(len(pdf_bytes)))
                self.send_header("Content-Disposition",
                                 f'attachment; filename="{filename}"')
                self.end_headers()
                self.wfile.write(pdf_bytes)
                return

            if path == "/revenu_quebec/set_config":
                if user.get("role") != "owner":
                    raise ValueError(t("err_owner_required", lang))
                rq_cc   = normalize_text(form.get("client_code", ""))
                rq_qm   = form.get("quick_method", "") == "1"
                rq_type = normalize_text(form.get("quick_method_type", "retail"))
                if rq_type not in ("retail", "services"):
                    rq_type = "retail"
                with open_db() as conn:
                    set_client_config(conn, rq_cc, rq_qm, rq_type, utc_now_iso())
                dest = f"/revenu_quebec?client_code={urlquote(rq_cc)}"
                self._flash_redirect(dest, flash=t("flash_rq_config_saved", lang))
                return

            # --- Bank statement import ---
            if path == "/bank_import":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                ct = self.headers.get("Content-Type", "")
                if "multipart/form-data" not in ct:
                    raise ValueError(t("err_bank_no_file", lang))
                fields, file_bytes, filename = _parse_multipart_simple(raw, ct)
                if not file_bytes or not filename:
                    raise ValueError(t("err_bank_no_file", lang))
                bi_client = normalize_text(fields.get("client_code", ""))
                if not bi_client:
                    raise ValueError(t("bank_import_client_code", lang) + " required")
                result = bank_import_statement(
                    file_bytes=file_bytes,
                    filename=filename,
                    client_code=bi_client,
                    imported_by=ctx["username"],
                    db_path=DB_PATH,
                )
                if result["transaction_count"] == 0:
                    self._send_html(render_bank_import(
                        ctx, user, flash_error=t("err_bank_no_transactions", lang),
                        lang=lang, result=result))
                    return
                self._send_html(render_bank_import(
                    ctx, user,
                    flash=t("flash_bank_imported", lang),
                    lang=lang, result=result))
                return

            # --- Manual bank transaction matching ---
            if path == "/bank_import/match":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                bank_doc_id = normalize_text(form.get("bank_document_id", ""))
                inv_doc_id = normalize_text(form.get("invoice_document_id", ""))
                if not bank_doc_id or not inv_doc_id:
                    raise ValueError("Both bank_document_id and invoice_document_id are required")
                bank_apply_manual_match(
                    bank_document_id=bank_doc_id,
                    invoice_document_id=inv_doc_id,
                    db_path=DB_PATH,
                )
                self._flash_redirect("/bank_import", flash=t("flash_bank_match_applied", lang))
                return

            # --- Draft client message (AI-generated, saved as unsent draft) ---
            if path == "/communications/draft":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                doc_id = normalize_text(form.get("document_id", ""))
                if not doc_id:
                    raise ValueError(t("err_doc_not_found", lang))
                doc_row = get_document(doc_id)
                if doc_row is None:
                    raise ValueError(t("err_doc_not_found", lang))
                vendor = normalize_text(doc_row["vendor"] or "")
                amount = normalize_text(doc_row["amount"] or "") or None
                client_code_d = normalize_text(doc_row["client_code"] or "")
                draft_lang = normalize_text(form.get("lang", "") or lang)
                if draft_lang not in ("fr", "en"):
                    draft_lang = lang
                draft_text = _client_comms.draft_message(
                    document_id=doc_id,
                    vendor=vendor,
                    amount=amount,
                    client_code=client_code_d,
                    lang=draft_lang,
                    username=ctx["username"],
                )
                with open_db() as conn:
                    _client_comms.save_draft(
                        conn,
                        document_id=doc_id,
                        client_code=client_code_d,
                        message=draft_text,
                        sent_by=ctx["username"],
                    )
                self._flash_redirect(
                    f"/document?id={urlquote(doc_id)}",
                    flash=t("flash_comm_draft_created", lang),
                )
                return

            # --- Send a drafted client message via SMTP ---
            if path == "/communications/send":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                doc_id = normalize_text(form.get("document_id", ""))
                comm_id_s = normalize_text(form.get("comm_id", ""))
                to_email_s = normalize_text(form.get("to_email", ""))
                subject_s = normalize_text(form.get("subject", ""))
                message_s = normalize_text(form.get("message", ""))
                if not comm_id_s:
                    raise ValueError(t("err_comm_not_found", lang))
                if not message_s:
                    raise ValueError(t("err_comm_message_required", lang))
                with open_db() as conn:
                    _client_comms.update_draft(conn, comm_id_s, message_s)
                    _client_comms.send_comm(
                        conn,
                        comm_id=comm_id_s,
                        to_email=to_email_s,
                        subject=subject_s,
                    )
                dest = f"/document?id={urlquote(doc_id)}" if doc_id else "/"
                self._flash_redirect(dest, flash=t("flash_comm_sent", lang))
                return

            # --- Filing calendar: mark period as filed ---
            if path == "/calendar/mark_filed":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_cal_forbidden", lang))
                cal_cc     = normalize_text(form.get("client_code",  ""))
                cal_period = normalize_text(form.get("period_label", ""))
                cal_dl     = normalize_text(form.get("deadline",     ""))
                if not cal_cc or not cal_period:
                    raise ValueError("client_code and period_label are required")
                with open_db() as conn:
                    _mark_as_filed(
                        conn,
                        client_code=cal_cc,
                        period_label=cal_period,
                        deadline=cal_dl,
                        filed_by=ctx["username"],
                        filed_at=utc_now_iso(),
                    )
                self._flash_redirect("/calendar", flash=t("flash_cal_filed", lang))
                return

            # --- Filing calendar: save per-client filing config ---
            if path == "/calendar/save_config":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_cal_forbidden", lang))
                cfg_cc    = normalize_text(form.get("client_code",             ""))
                cfg_freq  = normalize_text(form.get("filing_frequency",        "monthly"))
                cfg_gst   = normalize_text(form.get("gst_registration_number", ""))
                cfg_qst   = normalize_text(form.get("qst_registration_number", ""))
                cfg_fye   = normalize_text(form.get("fiscal_year_end",         "12-31"))
                if cfg_freq not in ("monthly", "quarterly", "annual"):
                    cfg_freq = "monthly"
                if not cfg_cc:
                    raise ValueError("client_code is required")
                with open_db() as conn:
                    ensure_client_config_table(conn)
                    conn.execute(
                        """
                        INSERT INTO client_config
                            (client_code, filing_frequency, gst_registration_number,
                             qst_registration_number, fiscal_year_end, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(client_code) DO UPDATE SET
                            filing_frequency          = excluded.filing_frequency,
                            gst_registration_number   = excluded.gst_registration_number,
                            qst_registration_number   = excluded.qst_registration_number,
                            fiscal_year_end           = excluded.fiscal_year_end,
                            updated_at                = excluded.updated_at
                        """,
                        (cfg_cc, cfg_freq, cfg_gst, cfg_qst, cfg_fye, utc_now_iso()),
                    )
                    conn.commit()
                self._flash_redirect("/calendar", flash=t("flash_cal_config_saved", lang))
                return

            # ------------------------------------------------------------------
            # Audit module — POST routes
            # ------------------------------------------------------------------

            if path == "/working_papers/signoff":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                paper_id    = normalize_text(form.get("paper_id", ""))
                wp_client   = normalize_text(form.get("client_code", ""))
                wp_period   = normalize_text(form.get("period", ""))
                wp_type     = normalize_text(form.get("engagement_type", "audit"))
                bal_conf_str = normalize_text(form.get("balance_confirmed", ""))
                if not paper_id:
                    raise ValueError("paper_id is required")
                kwargs: dict[str, Any] = {
                    "reviewed_by": user["username"],
                    "status": "complete",
                }
                if bal_conf_str:
                    try:
                        kwargs["balance_confirmed"] = float(bal_conf_str)
                    except ValueError:
                        pass
                with open_db() as conn:
                    _audit.update_working_paper(conn, paper_id, **kwargs)
                self._flash_redirect(
                    f"/working_papers?client_code={urlquote(wp_client)}&period={urlquote(wp_period)}&engagement_type={urlquote(wp_type)}",
                    flash=t("flash_wp_saved", lang),
                )
                return

            if path == "/working_papers/create_from_coa":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                wp_client = normalize_text(form.get("client_code", ""))
                wp_period = normalize_text(form.get("period", ""))
                wp_type   = normalize_text(form.get("engagement_type", "audit"))
                if not wp_client or not wp_period:
                    raise ValueError("client_code and period are required")
                with open_db() as conn:
                    accounts = conn.execute("SELECT * FROM chart_of_accounts ORDER BY account_code").fetchall()
                    for acct in accounts:
                        _audit.get_or_create_working_paper(
                            conn,
                            wp_client,
                            wp_period,
                            wp_type,
                            acct["account_code"],
                            acct["account_name"],
                        )
                self._flash_redirect(
                    f"/working_papers?client_code={urlquote(wp_client)}&period={urlquote(wp_period)}&engagement_type={urlquote(wp_type)}",
                    flash=t("flash_wp_saved", lang),
                )
                return

            if path == "/audit/evidence/link":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                evidence_id   = normalize_text(form.get("evidence_id", ""))
                linked_raw    = normalize_text(form.get("linked_doc_ids", ""))
                ev_client     = normalize_text(form.get("client_code", ""))
                ev_period     = normalize_text(form.get("period", ""))
                if not evidence_id:
                    raise ValueError("evidence_id is required")
                linked_ids = [x.strip() for x in linked_raw.split(",") if x.strip()]
                with open_db() as conn:
                    _audit.link_evidence_documents(conn, evidence_id, linked_ids)
                self._flash_redirect(
                    f"/audit/evidence?client_code={urlquote(ev_client)}&period={urlquote(ev_period)}",
                    flash=t("flash_ev_linked", lang),
                )
                return

            if path == "/audit/sample/mark":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                samp_paper    = normalize_text(form.get("paper_id", ""))
                samp_doc      = normalize_text(form.get("document_id", ""))
                tick_mark     = normalize_text(form.get("tick_mark", "tested"))
                samp_client   = normalize_text(form.get("client_code", ""))
                samp_period   = normalize_text(form.get("period", ""))
                samp_account  = normalize_text(form.get("account_code", ""))
                samp_size_str = normalize_text(form.get("sample_size", "10"))
                if not samp_doc:
                    raise ValueError("document_id is required")
                with open_db() as conn:
                    _audit.add_working_paper_item(conn, samp_paper or None, samp_doc, tick_mark, notes="", tested_by=user["username"])
                redirect_qs = (
                    f"?client_code={urlquote(samp_client)}&period={urlquote(samp_period)}"
                    f"&account_code={urlquote(samp_account)}&sample_size={urlquote(samp_size_str)}"
                    + (f"&paper_id={urlquote(samp_paper)}" if samp_paper else "")
                )
                self._flash_redirect(f"/audit/sample{redirect_qs}", flash=t("flash_samp_marked", lang))
                return

            if path == "/engagements/create":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                eng_client   = normalize_text(form.get("client_code", ""))
                eng_period   = normalize_text(form.get("period", ""))
                eng_type     = normalize_text(form.get("engagement_type", "audit"))
                eng_partner  = normalize_text(form.get("partner", ""))
                eng_manager  = normalize_text(form.get("manager", ""))
                eng_staff    = normalize_text(form.get("staff", ""))
                ph_str       = normalize_text(form.get("planned_hours", ""))
                budget_str   = normalize_text(form.get("budget", ""))
                fee_str      = normalize_text(form.get("fee", ""))
                if not eng_client or not eng_period:
                    raise ValueError("client_code and period are required")
                if eng_type not in _audit.VALID_ENGAGEMENT_TYPES:
                    eng_type = "audit"
                planned_hours = float(ph_str) if ph_str else None
                budget        = float(budget_str) if budget_str else None
                fee           = float(fee_str) if fee_str else None
                with open_db() as conn:
                    _audit.create_engagement(
                        conn, eng_client, eng_period,
                        engagement_type=eng_type,
                        partner=eng_partner, manager=eng_manager, staff=eng_staff,
                        planned_hours=planned_hours, budget=budget, fee=fee,
                    )
                self._flash_redirect("/engagements", flash=t("flash_eng_created", lang))
                return

            if path == "/engagements/update":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                eng_id       = normalize_text(form.get("engagement_id", ""))
                eng_status   = normalize_text(form.get("status", ""))
                eng_partner  = normalize_text(form.get("partner", ""))
                eng_manager  = normalize_text(form.get("manager", ""))
                eng_staff    = normalize_text(form.get("staff", ""))
                ph_str       = normalize_text(form.get("planned_hours", ""))
                ah_str       = normalize_text(form.get("actual_hours", ""))
                fee_str      = normalize_text(form.get("fee", ""))
                if not eng_id:
                    raise ValueError("engagement_id is required")
                upd_kwargs: dict[str, Any] = {}
                if eng_status and eng_status in _audit.VALID_ENGAGEMENT_STATUSES:
                    upd_kwargs["status"] = eng_status
                if eng_partner:
                    upd_kwargs["partner"] = eng_partner
                if eng_manager:
                    upd_kwargs["manager"] = eng_manager
                if eng_staff:
                    upd_kwargs["staff"] = eng_staff
                if ph_str:
                    try:
                        upd_kwargs["planned_hours"] = float(ph_str)
                    except ValueError:
                        pass
                if ah_str:
                    try:
                        upd_kwargs["actual_hours"] = float(ah_str)
                    except ValueError:
                        pass
                if fee_str:
                    try:
                        upd_kwargs["fee"] = float(fee_str)
                    except ValueError:
                        pass
                with open_db() as conn:
                    _audit.update_engagement(conn, eng_id, **upd_kwargs)
                self._flash_redirect(f"/engagements/detail?id={urlquote(eng_id)}", flash=t("flash_eng_updated", lang))
                return

            if path == "/engagements/issue":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                eng_id = normalize_text(form.get("engagement_id", ""))
                if not eng_id:
                    raise ValueError("engagement_id is required")
                with open_db() as conn:
                    pdf_bytes = _audit.issue_engagement(conn, eng_id, issued_by=user["username"], lang=lang)
                filename = f"engagement_{eng_id}.pdf"
                self.send_response(200)
                self.send_header("Content-Type", "application/pdf")
                self.send_header("Content-Length", str(len(pdf_bytes)))
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.end_headers()
                self.wfile.write(pdf_bytes)
                return

            if path == "/license/activate":
                if user.get("role") != "owner":
                    self._flash_redirect("/license", error=t("err_lic_forbidden", lang))
                    return
                license_key = form.get("license_key", "").strip()
                secret = get_signing_secret()
                try:
                    save_license_to_config(license_key, secret)
                    self._flash_redirect("/license", flash=t("flash_lic_activated", lang))
                except ValueError as exc:
                    self._flash_redirect("/license", error=f"{t('err_lic_invalid', lang)}: {exc}")
                return

            self._send_html(page_layout("Unknown Route", '<div class="card"><h2>Unknown route</h2><p><a href="/">Back</a></p></div>', user=user), status=404)

        except Exception as exc:
            dest = f"/document?id={urlquote(document_id)}" if document_id else "/"
            self._flash_redirect(dest, error=str(exc))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    bootstrap_schema()
    print()
    print("LEDGERLINK ACCOUNTING QUEUE")
    print("=" * 80)
    print(f"Database : {DB_PATH}")
    print(f"URL      : http://{HOST}:{PORT}/")
    print(f"Login    : sam / admin123  (change this!)")
    print()

    # Start the folder watcher if configured
    try:
        _fw_cfg = json.loads((ROOT_DIR / "ledgerlink.config.json").read_text(encoding="utf-8"))
        if _fw_cfg.get("folder_watcher_enabled") and _fw_cfg.get("inbox_folder"):
            from scripts.folder_watcher import start_folder_watcher as _start_fw
            _start_fw()
            print(f"Folder watcher : {_fw_cfg['inbox_folder']}")
            print()
    except Exception as _fw_exc:
        print(f"Folder watcher : failed to start — {_fw_exc}")
        print()

    server = ThreadingHTTPServer((HOST, PORT), ReviewDashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())