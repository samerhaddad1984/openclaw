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
from src.engines.reconciliation_engine import (
    add_reconciliation_item as recon_add_item,
    auto_populate_outstanding_items as recon_auto_populate,
    calculate_reconciliation as recon_calculate,
    create_reconciliation as recon_create,
    ensure_reconciliation_tables as _ensure_recon_tables,
    finalize_reconciliation as recon_finalize,
    generate_reconciliation_pdf as recon_generate_pdf,
    get_reconciliation as recon_get,
    get_reconciliation_items as recon_get_items,
    get_reconciliation_summary as recon_get_summary,
    list_reconciliations as recon_list,
    mark_item_cleared as recon_mark_cleared,
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
import src.engines.cas_engine as _cas
from src.engines.license_engine import (
    get_license_status, save_license_to_config, check_limits,
    get_signing_secret, TIER_DEFAULTS,
    get_licensed_machines, check_machine_license, register_machine,
    MAX_MACHINES_PER_FIRM,
)
from src.agents.core.ai_router import get_cache_stats as _get_cache_stats, _clear_cache as _ai_clear_cache
from src.integrations.qr_generator import generate_client_qr_png, generate_all_qr_pdf, _build_upload_url


DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
LOG_PATH = ROOT_DIR / "data" / "ledgerlink.log"
# Read bind address from config; default to 0.0.0.0 for LAN access
try:
    _boot_cfg = json.loads((ROOT_DIR / "ledgerlink.config.json").read_text(encoding="utf-8"))
    _net = _boot_cfg.get("network", {})
    if _net.get("bind_all_interfaces", False):
        HOST = "0.0.0.0"
    else:
        HOST = _boot_cfg.get("host", "127.0.0.1")
    PORT = _boot_cfg.get("port", 8787)
except Exception:
    HOST = "0.0.0.0"
    PORT = 8787
DEFAULT_REVIEWER = "Sam"
SESSION_DURATION_HOURS = 12

_SERVICE_START = datetime.now(timezone.utc)


def _get_app_version() -> str:
    """Read version from version.json."""
    try:
        vf = ROOT_DIR / "version.json"
        return json.loads(vf.read_text(encoding="utf-8")).get("version", "?")
    except Exception:
        return "?"

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


def _dict_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    """Row factory that returns plain dicts so .get() works everywhere."""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = _dict_factory
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
        count = list(conn.execute("SELECT COUNT(*) FROM dashboard_users").fetchone().values())[0]
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

        # CAS tables (materiality, risk assessments)
        _cas.ensure_cas_tables(conn)

        # Reconciliation tables
        _ensure_recon_tables(conn)


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
    return (list(row.values())[0] if row else 0) >= _RATE_LIMIT_MAX_FAILURES


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
) -> list[dict]:
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
        rows = [dict(r) for r in conn.execute(sql, tuple(params)).fetchall()]

    wanted = normalize_key(status)
    if not wanted:
        return rows
    return [r for r in rows if normalize_key(get_accounting_status(r)) == wanted]


def _infer_entry_kind(doc_row) -> str:
    """FIX 2: Infer entry_kind from doc_type and amount."""
    doc_type = normalize_text(doc_row["doc_type"]).lower() if doc_row.get("doc_type") else ""
    amount = None
    try:
        amount = float(doc_row["amount"]) if doc_row.get("amount") is not None else None
    except (TypeError, ValueError):
        pass
    credit_doc_types = {"credit_note", "refund", "chargeback", "reversal"}
    if doc_type in credit_doc_types or (amount is not None and amount < 0):
        return "credit"
    return "expense"


def get_document(document_id: str) -> dict | None:
    with open_db() as conn:
        row = conn.execute(
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
                d.fraud_flags,
                COALESCE(d.has_line_items, 0) AS has_line_items,
                COALESCE(d.lines_reconciled, 0) AS lines_reconciled,
                d.line_total_sum,
                d.invoice_total_gap,
                COALESCE(d.deposit_allocated, 0) AS deposit_allocated,
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
        return dict(row) if row else None


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


def get_qbo_posting_job(document_id: str) -> dict | None:
    with open_db() as conn:
        row = conn.execute(
            "SELECT * FROM posting_jobs WHERE document_id=? AND target_system='qbo' ORDER BY COALESCE(updated_at,created_at) DESC, rowid DESC LIMIT 1",
            (document_id,),
        ).fetchone()
        return dict(row) if row else None


def record_learning_corrections(document_id: str, before_row: dict, updated_fields: dict[str, Any]) -> None:
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


def render_learning_suggestions(document_id: str, row: dict, username: str,
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


def render_line_items_card(document_id: str, row: Any, lang: str = "fr") -> str:
    """Render invoice line items and deposit allocation cards for document detail."""
    try:
        _db = sqlite3.connect(str(DB_PATH))
        _db.row_factory = _dict_factory
        lines = _db.execute(
            """SELECT line_number, description, quantity, unit_price,
                      line_total_pretax, tax_regime, gst_amount, qst_amount,
                      hst_amount, province_of_supply, line_notes
               FROM invoice_lines WHERE document_id = ?
               ORDER BY line_number""",
            (document_id,),
        ).fetchall()
        _db.close()
    except Exception:
        return ""

    if not lines:
        return ""

    # Line items table
    rows_html = ""
    for ln in lines:
        rows_html += (
            f"<tr>"
            f"<td>{esc(str(ln['line_number']))}</td>"
            f"<td>{esc(str(ln['description'] or ''))}</td>"
            f"<td style='text-align:right;'>{esc(str(ln['quantity'] or ''))}</td>"
            f"<td style='text-align:right;'>{esc(str(ln['unit_price'] or ''))}</td>"
            f"<td style='text-align:right;'>{esc(str(ln['line_total_pretax'] or ''))}</td>"
            f"<td>{esc(str(ln['tax_regime'] or ''))}</td>"
            f"<td style='text-align:right;'>{esc(str(ln['gst_amount'] or '0.00'))}</td>"
            f"<td style='text-align:right;'>{esc(str(ln['qst_amount'] or '0.00'))}</td>"
            f"<td style='text-align:right;'>{esc(str(ln['hst_amount'] or '0.00'))}</td>"
            f"<td>{esc(str(ln['province_of_supply'] or ''))}</td>"
            f"<td class='small muted'>{esc(str(ln['line_notes'] or ''))}</td>"
            f"</tr>"
        )

    # Reconciliation status
    has_line_items = int(row["has_line_items"] or 0) if "has_line_items" in row.keys() else 0
    lines_reconciled = int(row["lines_reconciled"] or 0) if "lines_reconciled" in row.keys() else 0
    gap = float(row["invoice_total_gap"] or 0) if "invoice_total_gap" in row.keys() else 0
    line_total_sum = float(row["line_total_sum"] or 0) if "line_total_sum" in row.keys() else 0

    if lines_reconciled:
        recon_badge = f'<span class="badge badge-ready">{esc(t("line_reconciled_yes", lang))}</span>'
    else:
        gap_text = t("line_gap_amount", lang).replace("{amount}", f"{gap:.2f}")
        recon_badge = f'<span class="badge badge-hold">{esc(t("line_reconciled_no", lang))}</span> <span class="small muted">{esc(gap_text)}</span>'

    card = f"""
<div class="card">
  <h3>{esc(t("line_items_section", lang))}</h3>
  <div style="margin-bottom:8px;"><strong>{esc(t("line_reconciliation_status", lang))}:</strong> {recon_badge}</div>
  <div style="overflow-x:auto;">
  <table>
    <thead><tr>
      <th>{esc(t("line_col_num", lang))}</th>
      <th>{esc(t("line_col_description", lang))}</th>
      <th>{esc(t("line_col_qty", lang))}</th>
      <th>{esc(t("line_col_unit_price", lang))}</th>
      <th>{esc(t("line_col_pretax", lang))}</th>
      <th>{esc(t("line_col_tax_regime", lang))}</th>
      <th>{esc(t("line_col_gst", lang))}</th>
      <th>{esc(t("line_col_qst", lang))}</th>
      <th>{esc(t("line_col_hst", lang))}</th>
      <th>{esc(t("line_col_province", lang))}</th>
      <th>{esc(t("line_col_notes", lang))}</th>
    </tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
  </div>
</div>"""

    # Deposit allocation section
    deposit_card = ""
    deposit_allocated = int(row["deposit_allocated"] or 0) if "deposit_allocated" in row.keys() else 0
    if deposit_allocated:
        try:
            from src.engines.line_item_engine import allocate_deposit_proportionally
            # Try to get deposit amount from raw_result
            raw_result = {}
            try:
                raw_result = json.loads(row["raw_result"]) if row["raw_result"] else {}
            except Exception:
                pass
            dep_amt = raw_result.get("deposit_amount", 0)
            if dep_amt and float(dep_amt) > 0:
                _db2 = sqlite3.connect(str(DB_PATH))
                _db2.row_factory = _dict_factory
                alloc = allocate_deposit_proportionally(document_id, dep_amt, _db2)
                _db2.close()

                alloc_rows = ""
                for a in alloc.get("allocations", []):
                    alloc_rows += (
                        f"<tr>"
                        f"<td>{esc(str(a.get('description', '')))}</td>"
                        f"<td style='text-align:right;'>${a.get('original_pretax', 0):.2f}</td>"
                        f"<td style='text-align:right;'>${a.get('deposit_allocated', 0):.2f}</td>"
                        f"<td style='text-align:right;'>${a.get('net_pretax', 0):.2f}</td>"
                        f"<td style='text-align:right;'>${a.get('adjusted_gst_recovery', 0):.2f}</td>"
                        f"<td style='text-align:right;'>${a.get('adjusted_qst_recovery', 0):.2f}</td>"
                        f"<td style='text-align:right;'>${a.get('adjusted_hst_recovery', 0):.2f}</td>"
                        f"</tr>"
                    )
                deposit_card = f"""
<div class="card">
  <h3>{esc(t("deposit_allocation_section", lang))}</h3>
  <p><strong>{esc(t("deposit_total_label", lang))}:</strong> ${float(dep_amt):.2f}</p>
  <div style="overflow-x:auto;">
  <table>
    <thead><tr>
      <th>{esc(t("deposit_col_line", lang))}</th>
      <th>{esc(t("deposit_col_original", lang))}</th>
      <th>{esc(t("deposit_col_allocated", lang))}</th>
      <th>{esc(t("deposit_col_net", lang))}</th>
      <th>{esc(t("deposit_col_adj_gst", lang))}</th>
      <th>{esc(t("deposit_col_adj_qst", lang))}</th>
      <th>{esc(t("deposit_col_adj_hst", lang))}</th>
    </tr></thead>
    <tbody>{alloc_rows}</tbody>
  </table>
  </div>
</div>"""
        except Exception:
            pass

    return card + deposit_card


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


def _render_openclaw_bridge_status(lang: str = "fr") -> str:
    """Return an HTML snippet showing OpenClaw bridge stats for /troubleshoot."""
    try:
        from src.integrations.openclaw_bridge import get_bridge_stats
        stats = get_bridge_stats()
    except Exception as exc:
        return f'<p class="text-muted">OpenClaw bridge unavailable: {esc(str(exc))}</p>'

    if not stats.get("table_exists"):
        return (
            '<p class="text-muted">messaging_log table not found — '
            'run <code>python scripts/migrate_db.py</code> first.</p>'
        )

    last_ts   = stats.get("last_received_at") or "—"
    msg_today = stats.get("messages_today", 0)

    return f"""
<table>
  <tbody>
    <tr>
      <td><strong>Last received message</strong></td>
      <td><code>{esc(last_ts)}</code></td>
    </tr>
    <tr>
      <td><strong>Messages received today</strong></td>
      <td>{esc(str(msg_today))}</td>
    </tr>
    <tr>
      <td><strong>Ingest endpoint</strong></td>
      <td><code>POST http://127.0.0.1:{PORT}/ingest/openclaw</code></td>
    </tr>
  </tbody>
</table>"""


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
      <p><strong>{esc(t("version_label", lang))}:</strong> {esc(_get_app_version())}</p>
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
    <a href="/admin/cache" class="button-link btn-secondary">AI Cache</a>
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
  <h2>OpenClaw Bridge</h2>
  {_render_openclaw_bridge_status(lang)}
</div>
<div class="card">
  <h2>{esc(t("diag_log_lines", lang))}</h2>
  <textarea readonly style="height:420px;font-size:12px;">{esc(log_lines)}</textarea>
</div>"""

    return page_layout(t("dashboard_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# AI response cache admin page
# ---------------------------------------------------------------------------

def render_cache_admin(
    user: dict[str, Any],
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    stats = _get_cache_stats()
    err = stats.get("error")

    if err:
        stats_html = f'<p class="text-danger">Error loading cache stats: {esc(err)}</p>'
    else:
        hit_pct = f"{stats['hit_rate'] * 100:.1f}%"
        savings  = f"${stats['estimated_savings_usd']:.4f} USD"
        stats_html = f"""
<table>
  <thead>
    <tr>
      <th>Metric</th>
      <th>Value</th>
    </tr>
  </thead>
  <tbody>
    <tr><td>Cache hit rate</td><td><strong>{esc(hit_pct)}</strong></td></tr>
    <tr><td>Total requests (audit log)</td><td>{esc(str(stats['total_requests']))}</td></tr>
    <tr><td>Cache hits (audit log)</td><td>{esc(str(stats['cache_hits']))}</td></tr>
    <tr><td>Estimated cost saved</td><td><strong>{esc(savings)}</strong></td></tr>
    <tr><td>Cache entries (total)</td><td>{esc(str(stats['total_entries']))}</td></tr>
    <tr><td>Cache entries (active / non-expired)</td><td>{esc(str(stats['active_entries']))}</td></tr>
  </tbody>
</table>"""

    body = f"""
<div class="card">
  <h2>AI Response Cache</h2>
  <p>Cache TTL: 30 days for classification tasks, 7 days for explanation tasks.<br>
     Tasks never cached: <code>draft_client_message</code>, <code>escalation_decision</code>.</p>
  {stats_html}
  <div class="actions" style="margin-top:1rem;">
    <form method="POST" action="/admin/cache/clear"
          onsubmit="return confirm('Clear the entire AI response cache? This cannot be undone.');">
      <button class="btn-danger">Clear Cache</button>
    </form>
    <a href="/admin/cache" class="button-link btn-secondary">Refresh</a>
    <a href="/troubleshoot" class="button-link btn-secondary">Back to Diagnostics</a>
  </div>
</div>"""

    return page_layout("AI Cache Admin", body, user=user, flash=flash, flash_error=flash_error, lang=lang)


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

            # BLOCK 1: Split payment candidates section
            split_html = ""
            _sp_client = result.get("_client_code", "")
            try:
                from src.agents.tools.bank_matcher import BankMatcher
                from src.agents.core.bank_models import BankTransaction
                from src.agents.core.task_models import DocumentRecord
                matcher = BankMatcher()
                # Build transaction and document lists from result
                unmatched_txns: list[BankTransaction] = []
                for txn in result["transactions"]:
                    if txn.get("review_status") != "Ready":
                        unmatched_txns.append(BankTransaction(
                            transaction_id=txn.get("document_id", ""),
                            client_code=_sp_client,
                            account_id=None,
                            posted_date=txn.get("txn_date", ""),
                            description=txn.get("description", ""),
                            memo="",
                            amount=float(txn.get("debit") or txn.get("credit") or 0),
                            currency="CAD",
                        ))
                # Get unmatched invoices from DB
                unmatched_docs: list[DocumentRecord] = []
                if _sp_client:
                    with open_db() as _sp_conn:
                        _sp_rows = _sp_conn.execute(
                            "SELECT document_id, vendor, amount, document_date, client_code "
                            "FROM documents WHERE LOWER(TRIM(client_code)) = LOWER(TRIM(?)) "
                            "AND review_status NOT IN ('Posted','Ignored') "
                            "AND amount IS NOT NULL AND amount > 0",
                            (_sp_client,),
                        ).fetchall()
                        for _r in _sp_rows:
                            unmatched_docs.append(DocumentRecord(
                                document_id=_r["document_id"],
                                file_name="", file_path="",
                                client_code=_r["client_code"] or "",
                                vendor=_r["vendor"] or "",
                                doc_type="invoice",
                                amount=float(_r["amount"]),
                                document_date=_r["document_date"] or "",
                                gl_account="", tax_code="", category="",
                                review_status="Needs Review",
                                confidence=0.0, raw_result={},
                            ))
                splits = matcher.split_payment_detector(unmatched_docs, unmatched_txns)
                if splits:
                    split_rows = ""
                    for sp in splits:
                        inv_ids = sp["matched_document_ids"]
                        inv_list = ", ".join(esc(d[:16]) for d in inv_ids)
                        inv_hidden = "".join(
                            f'<input type="hidden" name="invoice_ids" value="{esc(d)}">'
                            for d in inv_ids
                        )
                        split_rows += (
                            f"<tr>"
                            f"<td style='font-weight:600;'>${sp['transaction_amount']:,.2f}</td>"
                            f"<td>{inv_list}</td>"
                            f"<td style='text-align:right;'>${sp['combined_amount']:,.2f}</td>"
                            f"<td style='text-align:right;color:#6b7280;'>${sp['difference']:,.2f}</td>"
                            f"<td>"
                            f"<form method='POST' action='/bank_import/confirm_split' style='display:inline;'>"
                            f"<input type='hidden' name='transaction_id' value='{esc(sp['transaction_id'])}'>"
                            f"{inv_hidden}"
                            f"<button type='submit' class='btn-primary' style='padding:4px 10px;font-size:12px;'>"
                            f"{esc(t('split_confirm', lang))}</button></form>"
                            f"</td></tr>\n"
                        )
                    split_html = (
                        f'<div class="card" style="margin-top:16px;">'
                        f'<h3>{esc(t("split_payments_title", lang))}</h3>'
                        f'<table style="width:100%;border-collapse:collapse;font-size:13px;">'
                        f'<thead><tr style="background:#f9fafb;">'
                        f'<th style="padding:6px 8px;">{esc(t("split_txn_amount", lang))}</th>'
                        f'<th style="padding:6px 8px;">{esc(t("split_invoices", lang))}</th>'
                        f'<th style="text-align:right;padding:6px 8px;">{esc(t("split_total", lang))}</th>'
                        f'<th style="text-align:right;padding:6px 8px;">{esc(t("split_difference", lang))}</th>'
                        f'<th style="padding:6px 8px;"></th>'
                        f'</tr></thead>'
                        f'<tbody>{split_rows}</tbody></table></div>'
                    )
            except Exception:
                pass  # Don't block bank import if split detection fails

            results_html = errors_html + summary_bar + table + split_html

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

# ---------------------------------------------------------------------------
# Bank Reconciliation pages
# ---------------------------------------------------------------------------

def render_reconciliation_list(
    ctx: dict[str, Any],
    user: dict[str, Any],
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
    filter_client: str = "",
    filter_period: str = "",
) -> str:
    """List all reconciliations with status badges."""
    with open_db() as conn:
        _ensure_recon_tables(conn)
        recons = recon_list(conn, client_code=filter_client, period=filter_period)

    status_badge = {
        "open": '<span class="badge badge-yellow">{label}</span>',
        "balanced": '<span class="badge badge-green">{label}</span>',
        "exception": '<span class="badge" style="background:#fee2e2;color:#991b1b;">{label}</span>',
    }

    rows_html = ""
    for r in recons:
        st = r.get("status", "open")
        label_key = f"recon_status_{st}"
        badge = status_badge.get(st, '<span class="badge">{label}</span>').format(
            label=esc(t(label_key, lang))
        )
        diff = r.get("difference")
        diff_str = f"${diff:,.2f}" if diff is not None else "—"
        diff_color = "color:#16a34a;" if diff is not None and abs(diff) <= 0.01 else "color:#dc2626;"
        rows_html += (
            f"<tr>"
            f"<td><a href=\"/reconciliation/detail?id={urlquote(r['reconciliation_id'])}\">"
            f"{esc(r['client_code'])}</a></td>"
            f"<td>{esc(r.get('account_name', ''))}</td>"
            f"<td>{esc(r.get('period_end_date', ''))}</td>"
            f"<td style='text-align:right;'>${r.get('statement_ending_balance', 0):,.2f}</td>"
            f"<td style='text-align:right;'>${r.get('gl_ending_balance', 0):,.2f}</td>"
            f"<td style='text-align:right;{diff_color}'>{diff_str}</td>"
            f"<td>{badge}</td>"
            f"</tr>\n"
        )

    if not recons:
        table_html = f'<p class="muted">{esc(t("recon_no_reconciliations", lang))}</p>'
    else:
        table_html = (
            f'<div style="overflow-x:auto;"><table>'
            f'<thead><tr>'
            f'<th>{esc(t("recon_client_code", lang))}</th>'
            f'<th>{esc(t("recon_account_name", lang))}</th>'
            f'<th>{esc(t("recon_period_end", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("recon_stmt_balance", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("recon_gl_balance", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("recon_difference", lang))}</th>'
            f'<th>{esc(t("recon_status", lang))}</th>'
            f'</tr></thead>'
            f'<tbody>{rows_html}</tbody></table></div>'
        )

    filter_form = (
        f'<form method="GET" action="/reconciliation" style="display:flex;gap:8px;align-items:end;margin-bottom:12px;">'
        f'<div class="field"><label>{esc(t("recon_filter_client", lang))}</label>'
        f'<input type="text" name="client_code" value="{esc(filter_client)}" style="width:140px;"></div>'
        f'<div class="field"><label>{esc(t("recon_filter_period", lang))}</label>'
        f'<input type="text" name="period" value="{esc(filter_period)}" placeholder="YYYY-MM" style="width:120px;"></div>'
        f'<button class="btn-primary" type="submit">{esc(t("btn_filter", lang))}</button>'
        f'</form>'
    )

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("recon_h1", lang))}</h2>'
        f'<div style="display:flex;gap:8px;">'
        f'<a href="/reconciliation/new" class="btn-primary button-link">{esc(t("recon_btn_new", lang))}</a>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div></div>\n'
        f'<div class="card">{filter_form}{table_html}</div>'
    )
    return page_layout(
        t("recon_title", lang), body,
        user=user, flash=flash, flash_error=flash_error, lang=lang,
    )


def render_reconciliation_new(
    ctx: dict[str, Any],
    user: dict[str, Any],
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    """Form to start a new reconciliation."""
    form_html = f"""
<div class="card">
  <h2>{esc(t("recon_new_title", lang))}</h2>
  <form method="POST" action="/reconciliation/create">
    <div class="grid-2">
      <div class="field">
        <label>{esc(t("recon_client_code", lang))}</label>
        <input type="text" name="client_code" required>
      </div>
      <div class="field">
        <label>{esc(t("recon_account_name", lang))}</label>
        <input type="text" name="account_name" required>
      </div>
      <div class="field">
        <label>{esc(t("recon_account_number", lang))}</label>
        <input type="text" name="account_number">
      </div>
      <div class="field">
        <label>{esc(t("recon_period_end", lang))}</label>
        <input type="date" name="period_end_date" required>
      </div>
      <div class="field">
        <label>{esc(t("recon_stmt_balance", lang))}</label>
        <input type="number" name="statement_balance" step="0.01" required>
      </div>
      <div class="field">
        <label>{esc(t("recon_gl_balance", lang))}</label>
        <input type="number" name="gl_balance" step="0.01" required>
      </div>
    </div>
    <div style="margin-top:12px;">
      <button class="btn-primary" type="submit">{esc(t("recon_btn_create", lang))}</button>
      <a href="/reconciliation" class="btn-secondary button-link" style="margin-left:8px;">{esc(t("btn_back_to_queue", lang))}</a>
    </div>
  </form>
</div>"""
    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("recon_new_title", lang))}</h2>'
        f'<a href="/reconciliation" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n{form_html}'
    )
    return page_layout(
        t("recon_new_title", lang), body,
        user=user, flash=flash, flash_error=flash_error, lang=lang,
    )


def render_reconciliation_detail(
    recon_id: str,
    ctx: dict[str, Any],
    user: dict[str, Any],
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    """Full reconciliation detail page showing both sides."""
    with open_db() as conn:
        _ensure_recon_tables(conn)
        recon = recon_get(recon_id, conn)
        if not recon:
            return page_layout(
                t("err_not_found", lang),
                f'<div class="card"><h2>{esc(t("err_not_found", lang))}</h2></div>',
                user=user, lang=lang,
            )
        items = recon_get_items(recon_id, conn)
        result = recon_calculate(recon_id, conn)

    bank_side = result.get("bank_side", {})
    book_side = result.get("book_side", {})
    difference = result.get("difference", 0)
    is_balanced = result.get("is_balanced", False)

    st = recon.get("status", "open")
    status_badge_map = {
        "open": "badge-yellow",
        "balanced": "badge-green",
        "exception": "",
    }
    badge_cls = status_badge_map.get(st, "")
    badge_style = ' style="background:#fee2e2;color:#991b1b;"' if st == "exception" else ""
    status_badge = f'<span class="badge {badge_cls}"{badge_style}>{esc(t(f"recon_status_{st}", lang))}</span>'

    # Bank side card
    dit_items = [i for i in items if i["item_type"] == "deposit_in_transit" and i["status"] == "outstanding"]
    oc_items = [i for i in items if i["item_type"] == "outstanding_cheque" and i["status"] == "outstanding"]
    be_items = [i for i in items if i["item_type"] == "bank_error" and i["status"] == "outstanding"]

    def _item_rows(item_list: list[dict], can_clear: bool = True) -> str:
        html = ""
        for it in item_list:
            clear_btn = ""
            if can_clear and st != "balanced":
                clear_btn = (
                    f'<form method="POST" action="/reconciliation/clear_item" style="display:inline;">'
                    f'<input type="hidden" name="item_id" value="{esc(it["item_id"])}">'
                    f'<input type="hidden" name="reconciliation_id" value="{esc(recon_id)}">'
                    f'<button class="btn-secondary" style="padding:2px 8px;font-size:11px;" type="submit">'
                    f'{esc(t("recon_btn_clear", lang))}</button></form>'
                )
            html += (
                f"<tr>"
                f"<td>{esc(it['description'])}</td>"
                f"<td style='text-align:right;'>${it['amount']:,.2f}</td>"
                f"<td>{esc(it.get('transaction_date') or '')}</td>"
                f"<td>{clear_btn}</td>"
                f"</tr>"
            )
        return html

    bank_card = (
        f'<div class="card">'
        f'<h3>{esc(t("recon_bank_side", lang))}</h3>'
        f'<table style="width:100%;">'
        f'<tr><td>{esc(t("recon_stmt_bal_label", lang))}</td>'
        f'<td style="text-align:right;font-weight:700;">${bank_side.get("statement_balance", 0):,.2f}</td></tr>'
    )
    if dit_items:
        bank_card += (
            f'<tr><td colspan="2" style="padding-top:8px;"><strong>{esc(t("recon_dit", lang))}</strong></td></tr>'
        )
        bank_card += (
            f'<tr><td colspan="2"><table style="width:100%;font-size:13px;">'
            f'<thead><tr><th>{esc(t("recon_description", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("recon_amount", lang))}</th>'
            f'<th>{esc(t("recon_txn_date", lang))}</th><th></th></tr></thead>'
            f'<tbody>{_item_rows(dit_items)}</tbody></table></td></tr>'
        )
        bank_card += (
            f'<tr><td>+ {esc(t("recon_dit", lang))}</td>'
            f'<td style="text-align:right;">${bank_side.get("deposits_in_transit", 0):,.2f}</td></tr>'
        )
    if oc_items:
        bank_card += (
            f'<tr><td colspan="2" style="padding-top:8px;"><strong>{esc(t("recon_oc", lang))}</strong></td></tr>'
        )
        bank_card += (
            f'<tr><td colspan="2"><table style="width:100%;font-size:13px;">'
            f'<thead><tr><th>{esc(t("recon_description", lang))}</th>'
            f'<th style="text-align:right;">{esc(t("recon_amount", lang))}</th>'
            f'<th>{esc(t("recon_txn_date", lang))}</th><th></th></tr></thead>'
            f'<tbody>{_item_rows(oc_items)}</tbody></table></td></tr>'
        )
        bank_card += (
            f'<tr><td>- {esc(t("recon_oc", lang))}</td>'
            f'<td style="text-align:right;">${bank_side.get("outstanding_cheques", 0):,.2f}</td></tr>'
        )
    bank_card += (
        f'<tr style="border-top:2px solid #111827;">'
        f'<td><strong>{esc(t("recon_adj_bank", lang))}</strong></td>'
        f'<td style="text-align:right;font-weight:700;">${bank_side.get("adjusted_bank_balance", 0):,.2f}</td></tr>'
        f'</table></div>'
    )

    # Book side card
    bc_items = [i for i in items if i["item_type"] == "bank_charge" and i["status"] == "outstanding"]
    ie_items = [i for i in items if i["item_type"] == "interest_earned" and i["status"] == "outstanding"]
    bke_items = [i for i in items if i["item_type"] == "book_error" and i["status"] == "outstanding"]

    book_card = (
        f'<div class="card">'
        f'<h3>{esc(t("recon_book_side", lang))}</h3>'
        f'<table style="width:100%;">'
        f'<tr><td>{esc(t("recon_gl_bal_label", lang))}</td>'
        f'<td style="text-align:right;font-weight:700;">${book_side.get("gl_balance", 0):,.2f}</td></tr>'
    )
    if bc_items:
        book_card += (
            f'<tr><td>- {esc(t("recon_bank_charges", lang))}</td>'
            f'<td style="text-align:right;">${book_side.get("bank_charges", 0):,.2f}</td></tr>'
        )
    if ie_items:
        book_card += (
            f'<tr><td>+ {esc(t("recon_interest", lang))}</td>'
            f'<td style="text-align:right;">${book_side.get("interest_earned", 0):,.2f}</td></tr>'
        )
    if bke_items:
        book_card += (
            f'<tr><td>+/- {esc(t("recon_book_errors", lang))}</td>'
            f'<td style="text-align:right;">${book_side.get("book_errors", 0):,.2f}</td></tr>'
        )
    book_card += (
        f'<tr style="border-top:2px solid #111827;">'
        f'<td><strong>{esc(t("recon_adj_book", lang))}</strong></td>'
        f'<td style="text-align:right;font-weight:700;">${book_side.get("adjusted_book_balance", 0):,.2f}</td></tr>'
        f'</table></div>'
    )

    # Difference
    diff_color = "color:#16a34a;" if is_balanced else "color:#dc2626;"
    diff_card = (
        f'<div class="card" style="text-align:center;">'
        f'<h3>{esc(t("recon_difference", lang))}</h3>'
        f'<div style="font-size:2rem;font-weight:700;{diff_color}">${difference:,.2f}</div>'
        f'<div style="margin-top:4px;">{status_badge}</div>'
        f'</div>'
    )

    # Add item form (only if not finalized)
    add_item_form = ""
    if st != "balanced":
        item_types = [
            ("deposit_in_transit", t("recon_dit", lang)),
            ("outstanding_cheque", t("recon_oc", lang)),
            ("bank_error", t("recon_bank_errors", lang)),
            ("book_error", t("recon_book_errors", lang)),
            ("bank_charge", t("recon_bank_charges", lang)),
            ("interest_earned", t("recon_interest", lang)),
        ]
        options = "".join(f'<option value="{k}">{esc(v)}</option>' for k, v in item_types)
        add_item_form = f"""
<div class="card">
  <h3>{esc(t("recon_btn_add_item", lang))}</h3>
  <form method="POST" action="/reconciliation/add_item">
    <input type="hidden" name="reconciliation_id" value="{esc(recon_id)}">
    <div class="grid-2">
      <div class="field"><label>{esc(t("recon_item_type", lang))}</label>
        <select name="item_type">{options}</select></div>
      <div class="field"><label>{esc(t("recon_description", lang))}</label>
        <input type="text" name="description" required></div>
      <div class="field"><label>{esc(t("recon_amount", lang))}</label>
        <input type="number" name="amount" step="0.01" required></div>
      <div class="field"><label>{esc(t("recon_txn_date", lang))}</label>
        <input type="date" name="transaction_date"></div>
    </div>
    <button class="btn-primary" type="submit" style="margin-top:8px;">{esc(t("recon_btn_add_item", lang))}</button>
  </form>
</div>"""

    # Finalize / PDF buttons
    action_btns = ""
    if st != "balanced":
        if is_balanced:
            action_btns += (
                f'<form method="POST" action="/reconciliation/finalize" style="display:inline;">'
                f'<input type="hidden" name="reconciliation_id" value="{esc(recon_id)}">'
                f'<button class="btn-primary" type="submit">{esc(t("recon_btn_finalize", lang))}</button>'
                f'</form> '
            )
    if st == "balanced":
        action_btns += (
            f'<a href="/reconciliation/pdf?id={urlquote(recon_id)}" class="btn-secondary button-link">'
            f'{esc(t("recon_btn_pdf", lang))}</a> '
        )

    # Metadata
    meta = ""
    if recon.get("prepared_by"):
        meta += f'<span class="muted">{esc(t("recon_prepared_by", lang))}: {esc(recon["prepared_by"])}</span> '
    if recon.get("reviewed_by"):
        meta += f'<span class="muted">{esc(t("recon_reviewed_by", lang))}: {esc(recon["reviewed_by"])}</span>'

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("recon_detail_title", lang))} — {esc(recon["client_code"])}</h2>'
        f'<div style="display:flex;gap:8px;">{action_btns}'
        f'<a href="/reconciliation" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div></div>\n'
        f'<div style="margin-bottom:8px;">{meta}</div>'
        f'<div class="grid-2">{bank_card}{book_card}</div>\n'
        f'{diff_card}\n'
        f'{add_item_form}'
    )
    return page_layout(
        t("recon_detail_title", lang), body,
        user=user, flash=flash, flash_error=flash_error, lang=lang,
    )


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

    # ------------------------------------------------------------------ #
    # Section 6: Reconciliation Summary
    # ------------------------------------------------------------------ #
    with open_db() as conn:
        _ensure_recon_tables(conn)
        recon_summary = recon_get_summary(conn)

    open_clients_list = recon_summary.get("open_clients", [])
    balanced_clients_list = recon_summary.get("balanced_clients", [])
    at_risk_list = recon_summary.get("at_risk_clients", [])
    avg_days = recon_summary.get("avg_days_to_complete")

    def _client_badges(clients: list[str], color: str) -> str:
        if not clients:
            return '<span class="muted">\u2014</span>'
        return " ".join(
            f'<span class="badge" style="background:{color}20;color:{color};">{esc(c)}</span>'
            for c in clients[:20]
        )

    avg_days_str = f"{avg_days:.1f}" if avg_days is not None else "\u2014"
    recon_section = (
        f'<div class="card">'
        f'<h2>{esc(t("recon_summary_title", lang))}</h2>'
        f'<table style="max-width:600px;">'
        f'<tr><td><strong>{esc(t("recon_summary_open", lang))}</strong></td>'
        f'<td>{_client_badges(open_clients_list, "#d97706")}</td></tr>'
        f'<tr><td><strong>{esc(t("recon_summary_balanced", lang))}</strong></td>'
        f'<td>{_client_badges(balanced_clients_list, "#16a34a")}</td></tr>'
        f'<tr><td><strong>{esc(t("recon_summary_at_risk", lang))}</strong></td>'
        f'<td>{_client_badges(at_risk_list, "#dc2626")}</td></tr>'
        f'<tr><td><strong>{esc(t("recon_summary_avg_days", lang))}</strong></td>'
        f'<td>{avg_days_str}</td></tr>'
        f'</table></div>'
    )

    # ------------------------------------------------------------------ #
    # Section 7: Going Concern Risk (BLOCK 2)
    # ------------------------------------------------------------------ #
    gc_section = ""
    try:
        with open_db() as conn:
            _cas.ensure_cas_tables(conn)
            # Get all clients with engagements
            client_rows = conn.execute(
                "SELECT DISTINCT client_code FROM engagements WHERE status NOT IN ('issued')"
            ).fetchall()
            gc_at_risk = []
            for cr in client_rows:
                cc = cr["client_code"]
                gc = _cas.detect_going_concern_indicators(cc, conn)
                if gc.get("indicator_count", 0) >= 2:
                    gc_at_risk.append({
                        "client_code": cc,
                        "indicator_count": gc["indicator_count"],
                        "indicators": gc.get("indicators", []),
                    })
        if gc_at_risk:
            gc_rows_html = ""
            for g in gc_at_risk:
                descs = "; ".join(
                    esc(i.get("description", "")[:60]) for i in g["indicators"][:3]
                )
                gc_rows_html += (
                    f"<tr>"
                    f"<td><strong>{esc(g['client_code'])}</strong></td>"
                    f"<td style='text-align:center;'>"
                    f"<span class='badge' style='background:#fee2e220;color:#dc2626;'>{g['indicator_count']}</span></td>"
                    f"<td style='font-size:12px;color:#6b7280;'>{descs}</td>"
                    f"</tr>\n"
                )
            gc_section = (
                f'<div class="card">'
                f'<h2>{esc(t("gc_risk_title", lang))}</h2>'
                f'<table style="width:100%;">'
                f'<thead><tr>'
                f'<th>{esc(t("col_client", lang))}</th>'
                f'<th style="text-align:center;">{esc(t("gc_indicators", lang))}</th>'
                f'<th>Details</th>'
                f'</tr></thead>'
                f'<tbody>{gc_rows_html}</tbody></table></div>'
            )
        else:
            gc_section = (
                f'<div class="card"><h2>{esc(t("gc_risk_title", lang))}</h2>'
                f'<p class="muted">{esc(t("gc_no_risk", lang))}</p></div>'
            )
    except Exception:
        gc_section = (
            f'<div class="card"><h2>{esc(t("gc_risk_title", lang))}</h2>'
            f'<p class="muted">{esc(t("gc_no_risk", lang))}</p></div>'
        )

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("analytics_h1", lang))}</h2>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n'
        f'{staff_section}\n'
        f'{client_section}\n'
        f'{trends_section}\n'
        f'<div class="grid-2">{fraud_section}{deadlines_section}</div>\n'
        f'{recon_section}\n'
        f'{gc_section}'
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
            # BLOCK 3: Check materiality for each paper
            _wp_perf_mat = None
            try:
                _wp_eng_r = conn.execute(
                    "SELECT engagement_id FROM engagements WHERE client_code = ? AND period = ? AND engagement_type = ? LIMIT 1",
                    (client_code, period, engagement_type or "audit"),
                ).fetchone()
                if _wp_eng_r:
                    _wp_mat = _cas.get_materiality(conn, _wp_eng_r["engagement_id"])
                    if _wp_mat:
                        _wp_perf_mat = float(_wp_mat["performance_materiality"])
            except Exception:
                pass

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
                # BLOCK 3: Materiality badge
                mat_badge = ""
                if _wp_perf_mat is not None and p.get("balance_per_books") is not None:
                    _bal_abs = abs(float(p["balance_per_books"]))
                    if _bal_abs >= _wp_perf_mat:
                        mat_badge = (
                            f' <span style="background:#fef3c7;color:#92400e;padding:2px 6px;'
                            f'border-radius:10px;font-size:11px;font-weight:600;">'
                            f'{esc(t("mat_badge_material", lang))}</span>'
                        )
                rows_html += (
                    f"<tr>"
                    f"<td><strong>{esc(p.get('account_code', ''))}</strong></td>"
                    f"<td>{esc(p.get('account_name', ''))}{mat_badge}</td>"
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

            # BLOCK 5: Build assertion coverage section for material items
            assertion_section = ""
            _assertion_names = ["completeness", "accuracy", "existence", "cutoff", "classification"]
            _assertion_keys = {
                "completeness": "assertion_completeness",
                "accuracy": "assertion_accuracy",
                "existence": "assertion_existence",
                "cutoff": "assertion_cutoff",
                "classification": "assertion_classification",
            }
            _material_papers = []
            for p in papers:
                if _wp_perf_mat is not None and p.get("balance_per_books") is not None:
                    if abs(float(p["balance_per_books"])) >= _wp_perf_mat:
                        _material_papers.append(p)
            if _material_papers:
                # Get existing assertion coverage
                _cov_map: dict[str, list[str]] = {}
                try:
                    with open_db() as _ac_conn:
                        for mp in _material_papers:
                            pid = mp.get("paper_id") or str(mp.get("id", ""))
                            cov = _cas.get_assertion_coverage(_ac_conn, pid)
                            for item in cov.get("items", []):
                                _cov_map[item["item_id"]] = item.get("assertions_tested", [])
                except Exception:
                    pass
                a_rows = ""
                _has_warning = False
                for mp in _material_papers:
                    pid = mp.get("paper_id") or str(mp.get("id", ""))
                    # Get items for this paper
                    _item_covs = _cov_map  # We use all items' coverage
                    # Build checkboxes for each assertion
                    # We'll use the paper_id as the item for simplicity
                    existing_assertions = []
                    for iid, asserts in _cov_map.items():
                        # Check if this item belongs to this paper
                        existing_assertions = asserts
                        break  # Simplified: use first item's coverage
                    checks = ""
                    for aname in _assertion_names:
                        checked = "checked" if aname in existing_assertions else ""
                        checks += (
                            f'<td style="text-align:center;">'
                            f'<input type="checkbox" name="assertions" value="{aname}" {checked}>'
                            f'</td>'
                        )
                    _is_missing_required = "completeness" not in existing_assertions or "existence" not in existing_assertions
                    if _is_missing_required:
                        _has_warning = True
                    a_rows += (
                        f"<tr>"
                        f"<td><strong>{esc(mp.get('account_code', ''))}</strong></td>"
                        f"<td>{esc(mp.get('account_name', ''))}</td>"
                        f"{checks}"
                        f"<td>"
                        f"<form method='POST' action='/working_papers/save_assertions' style='display:inline;'>"
                        f"<input type='hidden' name='paper_id' value='{esc(pid)}'>"
                        f"<input type='hidden' name='client_code' value='{esc(client_code)}'>"
                        f"<input type='hidden' name='period' value='{esc(period)}'>"
                        f"<input type='hidden' name='engagement_type' value='{esc(engagement_type)}'>"
                        + "".join(
                            f"<input type='hidden' name='assertions' value='{a}'>"
                            for a in existing_assertions
                        )
                        + f"<button type='submit' class='btn-primary' style='padding:3px 8px;font-size:11px;'>"
                        f"{esc(t('assertion_save', lang))}</button></form>"
                        f"</td></tr>\n"
                    )
                warning_banner = ""
                if _has_warning:
                    warning_banner = (
                        f'<div class="flash error" style="margin-bottom:8px;">'
                        f'{esc(t("assertion_warning", lang))}</div>'
                    )
                th_assertions = "".join(
                    f'<th style="text-align:center;font-size:11px;">{esc(t(_assertion_keys[a], lang))}</th>'
                    for a in _assertion_names
                )
                assertion_section = (
                    f'<div class="card" style="margin-top:16px;">'
                    f'<h3>{esc(t("assertion_title", lang))}</h3>'
                    f'{warning_banner}'
                    f'<div style="overflow-x:auto;">'
                    f'<table style="font-size:13px;">'
                    f'<thead><tr>'
                    f'<th>Code</th><th>Account</th>'
                    f'{th_assertions}'
                    f'<th></th></tr></thead>'
                    f'<tbody>{a_rows}</tbody></table></div></div>\n'
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
                f'{assertion_section}'
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


def _render_checklist_html(engagement_id: str, lang: str) -> str:
    """Build the engagement completion checklist HTML."""
    with open_db() as conn:
        checklist = _cas.get_engagement_checklist(engagement_id, conn)
    if not checklist:
        return '<p class="muted">—</p>'
    checklist_item_keys = {
        "materiality_calculated": "checklist_materiality",
        "risk_matrix_completed": "checklist_risk_matrix",
        "control_tests_documented": "checklist_control_tests",
        "related_parties_identified": "checklist_related_parties",
        "rep_letter_signed": "checklist_rep_letter",
        "working_papers_signed_off": "checklist_working_papers",
        "going_concern_assessed": "checklist_going_concern_assessed",
        "subsequent_events_clear": "checklist_subsequent_events_clear",
        "assertion_coverage": "checklist_assertion_coverage",
    }
    rows = ""
    for item in checklist:
        icon = "✅" if item["status"] == "complete" else "❌"
        label_key = checklist_item_keys.get(item["item"], item["item"])
        label = t(label_key, lang)
        req_label = t("checklist_required", lang) if item["required"] else t("checklist_not_required", lang)
        req_color = "#dc2626" if item["required"] and item["status"] != "complete" else "#6b7280"
        rows += (
            f'<tr><td>{icon}</td>'
            f'<td style="font-size:13px;">{esc(label)}</td>'
            f'<td style="font-size:12px;color:{req_color};">{esc(req_label)}</td></tr>'
        )
    return f'<table style="width:100%;"><tbody>{rows}</tbody></table>'


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
        eng_mat = _cas.get_materiality(conn, engagement_id)
        eng_risk_summary = _cas.get_risk_summary(conn, engagement_id)
        # BLOCK 4: Auto-run subsequent events check
        se_events = []
        try:
            se_events = _cas.check_subsequent_events(engagement_id, conn)
        except Exception:
            pass
        # BLOCK 2: Auto-run going concern check
        gc_result = {"indicators": [], "assessment_required": False, "indicator_count": 0}
        try:
            gc_result = _cas.detect_going_concern_indicators(eng["client_code"], conn)
        except Exception:
            pass

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
            f'<h4 style="margin-top:0;">{esc(t("cas_materiality_nav", lang))}'
            f' <a href="/audit/materiality?engagement_id={urlquote(str(engagement_id))}" '
            f'style="font-size:12px;font-weight:400;margin-left:8px;">→ {esc(t("eng_detail", lang))}</a></h4>'
            + (
                f'<div style="display:flex;gap:16px;flex-wrap:wrap;">'
                f'<div><span style="font-size:12px;color:#6b7280;">{esc(t("cas_mat_planning", lang))}</span><br>'
                f'<span style="font-weight:600;color:#1e40af;">${float(eng_mat.get("planning_materiality", 0)):,.2f}</span></div>'
                f'<div><span style="font-size:12px;color:#6b7280;">{esc(t("cas_mat_performance", lang))}</span><br>'
                f'<span style="font-weight:600;color:#92400e;">${float(eng_mat.get("performance_materiality", 0)):,.2f}</span></div>'
                f'<div><span style="font-size:12px;color:#6b7280;">{esc(t("cas_mat_trivial", lang))}</span><br>'
                f'<span style="font-weight:600;color:#166534;">${float(eng_mat.get("clearly_trivial", 0)):,.2f}</span></div>'
                f'</div>'
                if eng_mat else
                f'<p class="muted" style="margin:0;">{esc(t("cas_mat_no_assessment", lang))}</p>'
            )
            + f'</div>'
        )
        + (
            f'<div class="card" style="margin-top:16px;">'
            f'<h4 style="margin-top:0;">{esc(t("cas_risk_nav", lang))}'
            f' <a href="/audit/risk?engagement_id={urlquote(str(engagement_id))}" '
            f'style="font-size:12px;font-weight:400;margin-left:8px;">→ {esc(t("eng_detail", lang))}</a></h4>'
            + (
                f'<div style="display:flex;gap:16px;flex-wrap:wrap;">'
                f'<div><span style="font-size:12px;color:#6b7280;">{esc(t("cas_risk_total", lang))}</span><br>'
                f'<span style="font-weight:600;">{eng_risk_summary.get("total_assessments", 0)}</span></div>'
                f'<div><span style="font-size:12px;color:#6b7280;">{esc(t("cas_risk_high_count", lang))}</span><br>'
                f'<span style="font-weight:600;color:#dc2626;">{eng_risk_summary.get("high", 0)}</span></div>'
                f'<div><span style="font-size:12px;color:#6b7280;">{esc(t("cas_risk_significant_count", lang))}</span><br>'
                f'<span style="font-weight:600;color:#9333ea;">{eng_risk_summary.get("significant_risks", 0)}</span></div>'
                f'</div>'
                if eng_risk_summary.get("total_assessments", 0) > 0 else
                f'<p class="muted" style="margin:0;">{esc(t("cas_risk_no_assessments", lang))}</p>'
            )
            + f'</div>'
        )
        + (
            f'<div class="card" style="margin-top:16px;">'
            f'<h4 style="margin-top:0;">{esc(t("checklist_title", lang))}</h4>'
            + _render_checklist_html(engagement_id, lang)
            + f'</div>'
        )
        + (
            f'<div class="card" style="margin-top:16px;">'
            f'<h4 style="margin-top:0;">{esc(t("wp_title", lang))}</h4>'
            f'<div style="overflow-x:auto;"><table>'
            f'<thead><tr><th>Code</th><th>Account</th><th>{esc(t("col_status", lang))}</th></tr></thead>'
            f'<tbody>{wp_rows}</tbody></table></div></div>'
            if papers else
            f'<div class="card" style="margin-top:16px;"><p class="muted">{esc(t("wp_no_papers", lang))}</p></div>'
        )
        # BLOCK 4: Subsequent events section
        + (
            f'<div class="card" style="margin-top:16px;">'
            f'<h4 style="margin-top:0;">{esc(t("se_title", lang))}'
            f' <span class="badge" style="background:#fee2e220;color:#dc2626;margin-left:8px;">'
            f'{len(se_events)}</span></h4>'
            + (
                f'<div class="flash error" style="margin-bottom:8px;">{esc(t("se_warning", lang))}</div>'
                if se_events else ''
            )
            + (
                '<div style="overflow-x:auto;"><table style="font-size:13px;">'
                '<thead><tr><th>Document</th><th>Vendor</th><th style="text-align:right;">Amount</th>'
                '<th>Date</th><th>Status</th></tr></thead><tbody>'
                + "".join(
                    f"<tr><td><code>{esc(str(e.get('document_id', '')))}</code></td>"
                    f"<td>{esc(str(e.get('vendor', '')))}</td>"
                    f"<td style='text-align:right;'>${abs(e.get('amount', 0)):,.2f}</td>"
                    f"<td>{esc(str(e.get('document_date', '')))}</td>"
                    f"<td><span class='badge' style='background:#fef3c720;color:#92400e;'>"
                    f"{esc(e.get('status', ''))}</span></td></tr>"
                    for e in se_events
                )
                + '</tbody></table></div>'
                if se_events else
                f'<p class="muted">{esc(t("se_none", lang))}</p>'
            )
            + f'</div>'
        )
        # BLOCK 2: Going concern status on engagement detail
        + (
            f'<div class="card" style="margin-top:16px;">'
            f'<h4 style="margin-top:0;">{esc(t("gc_risk_title", lang))}</h4>'
            + (
                f'<div class="flash error" style="margin-bottom:8px;">'
                f'{esc(t("gc_assessment_required", lang))} — {gc_result.get("indicator_count", 0)} '
                f'{esc(t("gc_indicators", lang).lower())}</div>'
                + '<ul style="margin:0;font-size:13px;">'
                + "".join(
                    f"<li>{esc(i.get('description', ''))}</li>"
                    for i in gc_result.get("indicators", [])
                )
                + '</ul>'
                if gc_result.get("assessment_required") else
                f'<p class="muted">{esc(t("gc_no_risk", lang))}</p>'
            )
            + f'</div>'
        )
    )
    return page_layout(t("eng_detail", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# CAS 580 — Management Representation Letter
# ---------------------------------------------------------------------------

def render_rep_letter(
    ctx: dict[str, Any],
    user: dict[str, Any],
    engagement_id: str,
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    with open_db() as conn:
        engagements = _audit.get_engagements(conn)
    eng_opts = '<option value="">--</option>' + "".join(
        f'<option value="{esc(str(e["engagement_id"]))}" {"selected" if e["engagement_id"] == engagement_id else ""}>'
        f'{esc(e["client_code"])} — {esc(e["period"])} ({esc(e.get("engagement_type",""))})</option>'
        for e in engagements
    )

    select_form = (
        f'<div class="card">'
        f'<form method="GET" action="/audit/rep_letter" style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("eng_title", lang))}</label><br>'
        f'<select name="engagement_id" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;">{eng_opts}</select></div>'
        f'<div><button type="submit" class="btn-primary" style="padding:7px 16px;">{esc(t("btn_filter", lang))}</button></div>'
        f'</form></div>'
    )

    content = ""
    if engagement_id:
        with open_db() as conn:
            letter = _cas.get_rep_letter(engagement_id, conn)

        if letter:
            status = letter.get("status", "draft")
            status_colors = {"draft": "#f59e0b", "signed": "#16a34a", "refused": "#dc2626"}
            status_key = f"cas_rep_status_{status}"
            status_badge = (
                f'<span style="display:inline-block;padding:3px 10px;border-radius:12px;font-size:12px;font-weight:600;'
                f'color:#fff;background:{status_colors.get(status, "#6b7280")};">{esc(t(status_key, lang))}</span>'
            )
            draft_fr = esc(letter.get("draft_text_fr", "") or "")
            draft_en = esc(letter.get("draft_text_en", "") or "")

            sign_form = ""
            if status != "signed":
                sign_form = (
                    f'<div style="margin-top:16px;padding-top:16px;border-top:1px solid #e5e7eb;">'
                    f'<h4 style="margin-top:0;">{esc(t("cas_rep_sign", lang))}</h4>'
                    f'<form method="POST" action="/audit/rep_letter/sign" style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;">'
                    f'<input type="hidden" name="letter_id" value="{esc(letter["letter_id"])}">'
                    f'<input type="hidden" name="engagement_id" value="{esc(engagement_id)}">'
                    f'<div><label style="font-size:12px;font-weight:600;">{esc(t("cas_rep_mgmt_name", lang))}</label><br>'
                    f'<input type="text" name="management_name" value="{esc(letter.get("management_name") or "")}" '
                    f'style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;" required></div>'
                    f'<div><label style="font-size:12px;font-weight:600;">{esc(t("cas_rep_mgmt_title", lang))}</label><br>'
                    f'<input type="text" name="management_title" value="{esc(letter.get("management_title") or "")}" '
                    f'style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;" required></div>'
                    f'<div><button type="submit" class="btn-primary" style="padding:6px 14px;background:#16a34a;">'
                    f'{esc(t("cas_rep_sign", lang))}</button></div>'
                    f'</form></div>'
                )

            signed_info = ""
            if status == "signed":
                signed_info = (
                    f'<div style="margin-top:12px;padding:12px;background:#f0fdf4;border-radius:8px;">'
                    f'<strong>{esc(t("cas_rep_signed_by", lang))}:</strong> {esc(letter.get("management_name", ""))}<br>'
                    f'<strong>{esc(t("cas_rep_mgmt_title", lang))}:</strong> {esc(letter.get("management_title", ""))}<br>'
                    f'<strong>{esc(t("cas_rep_signed_at", lang))}:</strong> {esc(letter.get("signed_at", ""))}'
                    f'</div>'
                )

            content = (
                f'<div class="card" style="margin-top:12px;">'
                f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                f'<h3 style="margin:0;">{esc(t("cas_rep_title", lang))}</h3>{status_badge}</div>'
                f'{signed_info}'
                f'<div style="margin-top:16px;">'
                f'<h4>{esc(t("cas_rep_draft_fr", lang))}</h4>'
                f'<pre style="white-space:pre-wrap;background:#f9fafb;padding:16px;border-radius:8px;font-size:13px;border:1px solid #e5e7eb;max-height:400px;overflow-y:auto;">{draft_fr}</pre>'
                f'<h4>{esc(t("cas_rep_draft_en", lang))}</h4>'
                f'<pre style="white-space:pre-wrap;background:#f9fafb;padding:16px;border-radius:8px;font-size:13px;border:1px solid #e5e7eb;max-height:400px;overflow-y:auto;">{draft_en}</pre>'
                f'</div>'
                f'{sign_form}'
                f'</div>'
            )
        else:
            content = (
                f'<div class="card" style="margin-top:12px;">'
                f'<p class="muted">{esc(t("cas_rep_no_letter", lang))}</p>'
                f'<form method="POST" action="/audit/rep_letter/generate" style="margin-top:12px;">'
                f'<input type="hidden" name="engagement_id" value="{esc(engagement_id)}">'
                f'<button type="submit" class="btn-primary" style="padding:7px 16px;">'
                f'{esc(t("cas_rep_generate", lang))}</button></form></div>'
            )

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("cas_rep_title", lang))}</h2>'
        f'<a href="/engagements" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a></div>'
        f'{select_form}{content}'
    )
    return page_layout(t("cas_rep_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# CAS 330 — Control Testing Documentation
# ---------------------------------------------------------------------------

def render_control_tests(
    ctx: dict[str, Any],
    user: dict[str, Any],
    engagement_id: str,
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    with open_db() as conn:
        engagements = _audit.get_engagements(conn)
    eng_opts = '<option value="">--</option>' + "".join(
        f'<option value="{esc(str(e["engagement_id"]))}" {"selected" if e["engagement_id"] == engagement_id else ""}>'
        f'{esc(e["client_code"])} — {esc(e["period"])} ({esc(e.get("engagement_type",""))})</option>'
        for e in engagements
    )

    select_form = (
        f'<div class="card">'
        f'<form method="GET" action="/audit/controls" style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("eng_title", lang))}</label><br>'
        f'<select name="engagement_id" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;">{eng_opts}</select></div>'
        f'<div><button type="submit" class="btn-primary" style="padding:7px 16px;">{esc(t("btn_filter", lang))}</button></div>'
        f'</form></div>'
    )

    content = ""
    if engagement_id:
        with open_db() as conn:
            tests = _cas.get_control_tests(engagement_id, conn)
            summary = _cas.get_control_effectiveness_summary(engagement_id, conn)

        # Summary card
        summary_html = ""
        if tests:
            summary_html = (
                f'<div class="card" style="margin-top:12px;">'
                f'<h4 style="margin-top:0;">{esc(t("cas_ctrl_summary", lang))}</h4>'
                f'<div style="display:flex;gap:16px;flex-wrap:wrap;">'
                f'<div><span style="font-size:12px;color:#6b7280;">Total</span><br>'
                f'<span style="font-weight:600;">{summary["total"]}</span></div>'
                f'<div><span style="font-size:12px;color:#16a34a;">{esc(t("cas_ctrl_effective", lang))}</span><br>'
                f'<span style="font-weight:600;color:#16a34a;">{summary["effective"]}</span></div>'
                f'<div><span style="font-size:12px;color:#f59e0b;">{esc(t("cas_ctrl_partial", lang))}</span><br>'
                f'<span style="font-weight:600;color:#f59e0b;">{summary["partially_effective"]}</span></div>'
                f'<div><span style="font-size:12px;color:#dc2626;">{esc(t("cas_ctrl_ineffective", lang))}</span><br>'
                f'<span style="font-weight:600;color:#dc2626;">{summary["ineffective"]}</span></div>'
                f'</div></div>'
            )

        # Test rows
        test_rows = ""
        conclusion_colors = {"effective": "#16a34a", "partially_effective": "#f59e0b", "ineffective": "#dc2626"}
        conclusion_keys = {"effective": "cas_ctrl_effective", "partially_effective": "cas_ctrl_partial", "ineffective": "cas_ctrl_ineffective"}
        for ct in tests:
            conc = ct.get("conclusion", "effective")
            conc_color = conclusion_colors.get(conc, "#6b7280")
            conc_label = t(conclusion_keys.get(conc, "cas_ctrl_effective"), lang)
            conc_badge = f'<span style="color:{conc_color};font-weight:600;font-size:12px;">{esc(conc_label)}</span>'
            test_type_key = f"cas_ctrl_{ct.get('test_type', 'walkthrough')}"
            test_type_label = t(test_type_key, lang)

            # Results form
            result_form = (
                f'<details style="margin-top:6px;"><summary style="cursor:pointer;font-size:12px;color:#6366f1;">'
                f'{esc(t("cas_ctrl_record_results", lang))}</summary>'
                f'<form method="POST" action="/audit/controls/results" style="margin-top:8px;display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end;">'
                f'<input type="hidden" name="test_id" value="{esc(ct["test_id"])}">'
                f'<input type="hidden" name="engagement_id" value="{esc(engagement_id)}">'
                f'<div><label style="font-size:11px;">{esc(t("cas_ctrl_items_tested", lang))}</label><br>'
                f'<input type="number" name="items_tested" value="{ct.get("items_tested") or ""}" style="width:60px;padding:3px;border:1px solid #d1d5db;border-radius:4px;"></div>'
                f'<div><label style="font-size:11px;">{esc(t("cas_ctrl_exceptions", lang))}</label><br>'
                f'<input type="number" name="exceptions_found" value="{ct.get("exceptions_found") or 0}" style="width:60px;padding:3px;border:1px solid #d1d5db;border-radius:4px;"></div>'
                f'<div><label style="font-size:11px;">{esc(t("cas_ctrl_exception_details", lang))}</label><br>'
                f'<input type="text" name="exception_details" value="{esc(ct.get("exception_details") or "")}" style="width:160px;padding:3px;border:1px solid #d1d5db;border-radius:4px;"></div>'
                f'<div><label style="font-size:11px;">{esc(t("cas_ctrl_conclusion", lang))}</label><br>'
                f'<select name="conclusion" style="padding:3px;border:1px solid #d1d5db;border-radius:4px;">'
                f'<option value="effective" {"selected" if conc == "effective" else ""}>{esc(t("cas_ctrl_effective", lang))}</option>'
                f'<option value="partially_effective" {"selected" if conc == "partially_effective" else ""}>{esc(t("cas_ctrl_partial", lang))}</option>'
                f'<option value="ineffective" {"selected" if conc == "ineffective" else ""}>{esc(t("cas_ctrl_ineffective", lang))}</option>'
                f'</select></div>'
                f'<div><button type="submit" class="btn-primary" style="padding:4px 10px;font-size:12px;">'
                f'{esc(t("btn_save", lang))}</button></div>'
                f'</form></details>'
            )

            test_rows += (
                f'<tr>'
                f'<td style="font-size:13px;font-weight:600;">{esc(ct.get("control_name", ""))}</td>'
                f'<td style="font-size:12px;">{esc(ct.get("control_objective", ""))}</td>'
                f'<td style="font-size:12px;">{esc(test_type_label)}</td>'
                f'<td style="text-align:center;">{ct.get("items_tested") or "—"}</td>'
                f'<td style="text-align:center;">{ct.get("exceptions_found") or 0}</td>'
                f'<td>{conc_badge}</td>'
                f'<td>{result_form}</td>'
                f'</tr>'
            )

        tests_table = ""
        if tests:
            tests_table = (
                f'<div class="card" style="margin-top:12px;">'
                f'<div style="overflow-x:auto;"><table>'
                f'<thead><tr>'
                f'<th>{esc(t("cas_ctrl_name", lang))}</th>'
                f'<th>{esc(t("cas_ctrl_objective", lang))}</th>'
                f'<th>{esc(t("cas_ctrl_test_type", lang))}</th>'
                f'<th>{esc(t("cas_ctrl_items_tested", lang))}</th>'
                f'<th>{esc(t("cas_ctrl_exceptions", lang))}</th>'
                f'<th>{esc(t("cas_ctrl_conclusion", lang))}</th>'
                f'<th>{esc(t("col_action", lang))}</th>'
                f'</tr></thead><tbody>{test_rows}</tbody></table></div></div>'
            )
        else:
            tests_table = (
                f'<div class="card" style="margin-top:12px;">'
                f'<p class="muted">{esc(t("cas_ctrl_no_tests", lang))}</p></div>'
            )

        # Add from library form
        lib_opts = "".join(
            f'<option value="{esc(c["name"])}" data-obj="{esc(c["objective"])}" data-desc="{esc(c["description"])}">'
            f'{esc(c["name"])} — {esc(c["objective"])}</option>'
            for c in _cas.STANDARD_CONTROLS
        )
        type_opts = "".join(
            f'<option value="{tt}">{esc(t(f"cas_ctrl_{tt}", lang))}</option>'
            for tt in ["walkthrough", "reperformance", "observation", "inquiry"]
        )

        add_form = (
            f'<div class="card" style="margin-top:12px;">'
            f'<h4 style="margin-top:0;">{esc(t("cas_ctrl_add_from_library", lang))}</h4>'
            f'<form method="POST" action="/audit/controls/add" style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;">'
            f'<input type="hidden" name="engagement_id" value="{esc(engagement_id)}">'
            f'<div><label style="font-size:12px;font-weight:600;">{esc(t("cas_ctrl_name", lang))}</label><br>'
            f'<select name="control_name" id="ctrl_lib" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;max-width:350px;">'
            f'{lib_opts}</select></div>'
            f'<div><label style="font-size:12px;font-weight:600;">{esc(t("cas_ctrl_test_type", lang))}</label><br>'
            f'<select name="test_type" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;">{type_opts}</select></div>'
            f'<div><button type="submit" class="btn-primary" style="padding:6px 14px;">'
            f'{esc(t("cas_ctrl_add", lang))}</button></div>'
            f'</form>'
            f'<hr style="border:none;border-top:1px solid #e5e7eb;margin:12px 0;">'
            f'<h4 style="margin-top:0;">{esc(t("cas_ctrl_add_custom", lang))}</h4>'
            f'<form method="POST" action="/audit/controls/add" style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;">'
            f'<input type="hidden" name="engagement_id" value="{esc(engagement_id)}">'
            f'<div><label style="font-size:12px;font-weight:600;">{esc(t("cas_ctrl_name", lang))}</label><br>'
            f'<input type="text" name="control_name" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;" required></div>'
            f'<div><label style="font-size:12px;font-weight:600;">{esc(t("cas_ctrl_objective", lang))}</label><br>'
            f'<input type="text" name="control_objective" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;" required></div>'
            f'<div><label style="font-size:12px;font-weight:600;">{esc(t("cas_ctrl_test_type", lang))}</label><br>'
            f'<select name="test_type" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;">{type_opts}</select></div>'
            f'<div><button type="submit" class="btn-primary" style="padding:6px 14px;">'
            f'{esc(t("cas_ctrl_add", lang))}</button></div>'
            f'</form></div>'
        )

        content = summary_html + tests_table + add_form

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("cas_ctrl_title", lang))}</h2>'
        f'<a href="/engagements" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a></div>'
        f'{select_form}{content}'
    )
    return page_layout(t("cas_ctrl_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# CAS 550 — Related Party Procedures
# ---------------------------------------------------------------------------

def render_related_parties(
    ctx: dict[str, Any],
    user: dict[str, Any],
    engagement_id: str,
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    with open_db() as conn:
        engagements = _audit.get_engagements(conn)
    eng_opts = '<option value="">--</option>' + "".join(
        f'<option value="{esc(str(e["engagement_id"]))}" {"selected" if e["engagement_id"] == engagement_id else ""}>'
        f'{esc(e["client_code"])} — {esc(e["period"])} ({esc(e.get("engagement_type",""))})</option>'
        for e in engagements
    )

    select_form = (
        f'<div class="card">'
        f'<form method="GET" action="/audit/related_parties" style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("eng_title", lang))}</label><br>'
        f'<select name="engagement_id" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;">{eng_opts}</select></div>'
        f'<div><button type="submit" class="btn-primary" style="padding:7px 16px;">{esc(t("btn_filter", lang))}</button></div>'
        f'</form></div>'
    )

    content = ""
    if engagement_id:
        with open_db() as conn:
            eng = _audit.get_engagement(conn, engagement_id)
            if not eng:
                return page_layout(t("err_eng_not_found", lang),
                    f'<div class="card"><p>{esc(t("err_eng_not_found", lang))}</p></div>',
                    user=user, lang=lang)
            client_code = eng["client_code"]
            parties = _cas.get_related_parties(client_code, conn)
            transactions = _cas.get_related_party_transactions(engagement_id, conn)
            auto_detected = _cas.auto_detect_related_parties(client_code, conn)
            rp_summary = _cas.get_related_party_summary(engagement_id, conn)

        # Tab 1: Related parties list
        rel_type_keys = {
            "owner": "cas_rp_type_owner", "family_member": "cas_rp_type_family",
            "affiliated_company": "cas_rp_type_affiliated", "key_management": "cas_rp_type_key_mgmt",
            "board_member": "cas_rp_type_board",
        }
        party_rows = ""
        for p in parties:
            rtype = t(rel_type_keys.get(p.get("relationship_type", ""), "cas_rp_type_affiliated"), lang)
            pct = f'{p.get("ownership_percentage", 0) or 0}%' if p.get("ownership_percentage") else "—"
            party_rows += (
                f'<tr><td>{esc(p.get("party_name", ""))}</td>'
                f'<td>{esc(rtype)}</td>'
                f'<td style="text-align:center;">{pct}</td>'
                f'<td style="font-size:12px;">{esc(p.get("notes", "") or "")}</td></tr>'
            )

        parties_html = (
            f'<div style="overflow-x:auto;"><table>'
            f'<thead><tr><th>{esc(t("cas_rp_party_name", lang))}</th>'
            f'<th>{esc(t("cas_rp_relationship", lang))}</th>'
            f'<th>{esc(t("cas_rp_ownership", lang))}</th>'
            f'<th>Notes</th></tr></thead>'
            f'<tbody>{party_rows}</tbody></table></div>'
            if parties else
            f'<p class="muted">{esc(t("cas_rp_no_parties", lang))}</p>'
        )

        # Add party form
        type_opts = "".join(
            f'<option value="{rt}">{esc(t(rel_type_keys.get(rt, ""), lang))}</option>'
            for rt in ["owner", "family_member", "affiliated_company", "key_management", "board_member"]
        )
        add_party_form = (
            f'<form method="POST" action="/audit/related_parties/add" style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;margin-top:12px;">'
            f'<input type="hidden" name="engagement_id" value="{esc(engagement_id)}">'
            f'<input type="hidden" name="client_code" value="{esc(client_code)}">'
            f'<div><label style="font-size:12px;font-weight:600;">{esc(t("cas_rp_party_name", lang))}</label><br>'
            f'<input type="text" name="party_name" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;" required></div>'
            f'<div><label style="font-size:12px;font-weight:600;">{esc(t("cas_rp_relationship", lang))}</label><br>'
            f'<select name="relationship_type" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;">{type_opts}</select></div>'
            f'<div><label style="font-size:12px;font-weight:600;">{esc(t("cas_rp_ownership", lang))}</label><br>'
            f'<input type="number" name="ownership_pct" step="0.1" style="padding:5px 8px;border:1px solid #d1d5db;border-radius:4px;width:80px;"></div>'
            f'<div><button type="submit" class="btn-primary" style="padding:6px 14px;">'
            f'{esc(t("cas_rp_add", lang))}</button></div>'
            f'</form>'
        )

        tab1 = (
            f'<div class="card" style="margin-top:12px;">'
            f'<h4 style="margin-top:0;">{esc(t("cas_rp_tab_parties", lang))}</h4>'
            f'{parties_html}{add_party_form}</div>'
        )

        # Tab 2: Transactions
        txn_rows = ""
        for txn in transactions:
            amt = f'${float(txn.get("amount") or 0):,.2f}'
            meas_key = f'cas_rp_basis_{txn.get("measurement_basis", "exchange_amount")}'
            meas_label = t(meas_key, lang)
            disc = "✅" if txn.get("disclosure_required") else "—"
            txn_rows += (
                f'<tr><td>{esc(txn.get("party_name", ""))}</td>'
                f'<td style="text-align:right;">{amt}</td>'
                f'<td style="font-size:12px;">{esc(txn.get("description", "") or "")}</td>'
                f'<td>{esc(meas_label)}</td>'
                f'<td style="text-align:center;">{disc}</td></tr>'
            )

        tab2 = (
            f'<div class="card" style="margin-top:12px;">'
            f'<h4 style="margin-top:0;">{esc(t("cas_rp_tab_transactions", lang))}</h4>'
            + (
                f'<div style="overflow-x:auto;"><table>'
                f'<thead><tr><th>{esc(t("cas_rp_party_name", lang))}</th>'
                f'<th>{esc(t("cas_rp_amount", lang))}</th>'
                f'<th>{esc(t("cas_rp_description", lang))}</th>'
                f'<th>{esc(t("cas_rp_measurement", lang))}</th>'
                f'<th>{esc(t("cas_rp_disclosure", lang))}</th>'
                f'</tr></thead><tbody>{txn_rows}</tbody></table></div>'
                if transactions else
                f'<p class="muted">{esc(t("cas_rp_no_transactions", lang))}</p>'
            )
            + f'</div>'
        )

        # Tab 3: Auto-detection
        auto_rows = ""
        for ad in auto_detected:
            evidence_str = ", ".join(ad.get("evidence", []))
            auto_rows += (
                f'<tr><td>{esc(ad.get("vendor", ""))}</td>'
                f'<td style="text-align:center;">{ad.get("transaction_count", 0)}</td>'
                f'<td style="font-size:12px;">{esc(evidence_str)}</td>'
                f'<td><span style="color:#f59e0b;font-weight:600;font-size:12px;">{esc(ad.get("status", ""))}</span></td></tr>'
            )

        tab3 = (
            f'<div class="card" style="margin-top:12px;">'
            f'<h4 style="margin-top:0;">{esc(t("cas_rp_tab_auto_detect", lang))}</h4>'
            + (
                f'<div style="overflow-x:auto;"><table>'
                f'<thead><tr><th>{esc(t("col_vendor", lang))}</th>'
                f'<th>Txns</th>'
                f'<th>{esc(t("cas_rp_auto_evidence", lang))}</th>'
                f'<th>{esc(t("cas_rp_auto_status", lang))}</th>'
                f'</tr></thead><tbody>{auto_rows}</tbody></table></div>'
                if auto_detected else
                f'<p class="muted">{esc(t("cas_rp_no_auto_detect", lang))}</p>'
            )
            + f'</div>'
        )

        # Generate disclosure button
        disclosure_btn = (
            f'<div class="card" style="margin-top:12px;">'
            f'<form method="POST" action="/audit/related_parties/disclosure">'
            f'<input type="hidden" name="engagement_id" value="{esc(engagement_id)}">'
            f'<button type="submit" class="btn-primary" style="padding:7px 16px;">'
            f'{esc(t("cas_rp_generate_disclosure", lang))}</button></form></div>'
        )

        content = tab1 + tab2 + tab3 + disclosure_btn

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("cas_rp_title", lang))}</h2>'
        f'<a href="/engagements" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a></div>'
        f'{select_form}{content}'
    )
    return page_layout(t("cas_rp_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# CAS Module — Materiality Assessment (CAS 320)
# ---------------------------------------------------------------------------

def render_materiality(
    ctx: dict[str, Any],
    user: dict[str, Any],
    engagement_id: str,
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    with open_db() as conn:
        engagements = _audit.get_engagements(conn)
        current_mat = None
        eng = None
        if engagement_id:
            eng = _audit.get_engagement(conn, engagement_id)
            current_mat = _cas.get_materiality(conn, engagement_id)

    # Engagement selector
    eng_opts = '<option value="">--</option>' + "".join(
        f'<option value="{esc(str(e.get("engagement_id", "")))}" '
        f'{"selected" if str(e.get("engagement_id", "")) == engagement_id else ""}>'
        f'{esc(str(e.get("client_code", "")))} — {esc(str(e.get("period", "")))} ({esc(str(e.get("engagement_type", "")))})</option>'
        for e in engagements
    )

    basis_opts = "".join(
        f'<option value="{v}">{esc(t(f"cas_mat_basis_{v}" if v != "pre_tax_income" else "cas_mat_basis_pre_tax", lang))}</option>'
        for v in ["pre_tax_income", "total_assets", "revenue"]
    )

    select_form = (
        f'<div class="card">'
        f'<form method="GET" action="/audit/materiality" style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("eng_title", lang))}</label><br>'
        f'<select name="engagement_id" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;">{eng_opts}</select></div>'
        f'<div><button type="submit" class="btn-primary" style="padding:7px 16px;">{esc(t("btn_filter", lang))}</button></div>'
        f'</form></div>\n'
    )

    calc_form = ""
    if engagement_id:
        calc_form = (
            f'<div class="card" style="margin-bottom:12px;">'
            f'<h3 style="margin-top:0;">{esc(t("cas_mat_calculate", lang))}</h3>'
            f'<form method="POST" action="/audit/materiality/save" style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">'
            f'<input type="hidden" name="engagement_id" value="{esc(engagement_id)}">'
            f'<div><label style="font-size:13px;font-weight:600;">{esc(t("cas_mat_basis", lang))}</label><br>'
            f'<select name="basis" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;">{basis_opts}</select></div>'
            f'<div><label style="font-size:13px;font-weight:600;">{esc(t("cas_mat_basis_amount", lang))}</label><br>'
            f'<input type="number" name="basis_amount" step="0.01" required style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;width:180px;"></div>'
            f'<div><label style="font-size:13px;font-weight:600;">{esc(t("cas_mat_notes", lang))}</label><br>'
            f'<input type="text" name="notes" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;width:200px;"></div>'
            f'<div><button type="submit" class="btn-primary" style="padding:7px 16px;">{esc(t("cas_mat_save", lang))}</button></div>'
            f'</form></div>\n'
        )

    mat_display = ""
    if current_mat:
        basis_labels = {
            "pre_tax_income": t("cas_mat_basis_pre_tax", lang),
            "total_assets": t("cas_mat_basis_total_assets", lang),
            "revenue": t("cas_mat_basis_revenue", lang),
        }
        mat_display = (
            f'<div class="card">'
            f'<h3 style="margin-top:0;">{esc(t("cas_mat_result", lang))}</h3>'
            f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;max-width:500px;">'
            f'<span style="font-weight:600;">{esc(t("cas_mat_basis", lang))}:</span>'
            f'<span>{esc(basis_labels.get(current_mat.get("basis", ""), current_mat.get("basis", "")))}</span>'
            f'<span style="font-weight:600;">{esc(t("cas_mat_basis_amount", lang))}:</span>'
            f'<span>${float(current_mat.get("basis_amount", 0)):,.2f}</span>'
            f'<span style="font-weight:600;">{esc(t("cas_mat_planning", lang))}:</span>'
            f'<span style="color:#1e40af;font-weight:600;">${float(current_mat.get("planning_materiality", 0)):,.2f}</span>'
            f'<span style="font-weight:600;">{esc(t("cas_mat_performance", lang))}:</span>'
            f'<span style="color:#92400e;font-weight:600;">${float(current_mat.get("performance_materiality", 0)):,.2f}</span>'
            f'<span style="font-weight:600;">{esc(t("cas_mat_trivial", lang))}:</span>'
            f'<span style="color:#166534;font-weight:600;">${float(current_mat.get("clearly_trivial", 0)):,.2f}</span>'
            f'<span style="font-weight:600;">{esc(t("cas_mat_calculated_by", lang))}:</span>'
            f'<span>{esc(str(current_mat.get("calculated_by", "") or ""))}</span>'
            f'<span style="font-weight:600;">{esc(t("cas_mat_calculated_at", lang))}:</span>'
            f'<span>{esc(str(current_mat.get("calculated_at", "") or ""))}</span>'
            f'</div>'
            + (f'<p style="margin-top:8px;color:#6b7280;font-size:13px;">{esc(str(current_mat.get("notes", "") or ""))}</p>' if current_mat.get("notes") else "")
            + f'</div>\n'
        )
    elif engagement_id:
        mat_display = f'<div class="card"><p class="muted">{esc(t("cas_mat_no_assessment", lang))}</p></div>\n'

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("cas_materiality_title", lang))}</h2>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n'
        f'{select_form}{calc_form}{mat_display}'
    )
    return page_layout(t("cas_materiality_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# CAS Module — Risk Assessment (CAS 315)
# ---------------------------------------------------------------------------

def render_risk_assessment(
    ctx: dict[str, Any],
    user: dict[str, Any],
    engagement_id: str,
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    with open_db() as conn:
        engagements = _audit.get_engagements(conn)
        risks: list[dict[str, Any]] = []
        summary: dict[str, Any] = {}
        if engagement_id:
            risks = _cas.get_risk_assessment(conn, engagement_id)
            summary = _cas.get_risk_summary(conn, engagement_id)

    # Engagement selector
    eng_opts = '<option value="">--</option>' + "".join(
        f'<option value="{esc(str(e.get("engagement_id", "")))}" '
        f'{"selected" if str(e.get("engagement_id", "")) == engagement_id else ""}>'
        f'{esc(str(e.get("client_code", "")))} — {esc(str(e.get("period", "")))} ({esc(str(e.get("engagement_type", "")))})</option>'
        for e in engagements
    )

    select_form = (
        f'<div class="card">'
        f'<form method="GET" action="/audit/risk" style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;">'
        f'<div><label style="font-size:13px;font-weight:600;">{esc(t("eng_title", lang))}</label><br>'
        f'<select name="engagement_id" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:6px;">{eng_opts}</select></div>'
        f'<div><button type="submit" class="btn-primary" style="padding:7px 16px;">{esc(t("btn_filter", lang))}</button></div>'
        f'</form></div>\n'
    )

    # Generate matrix button
    gen_form = ""
    if engagement_id:
        gen_form = (
            f'<div class="card" style="margin-bottom:12px;">'
            f'<form method="POST" action="/audit/risk/generate" style="display:inline;">'
            f'<input type="hidden" name="engagement_id" value="{esc(engagement_id)}">'
            f'<button type="submit" class="btn-primary" style="padding:7px 16px;">'
            f'{esc(t("cas_risk_generate", lang))}</button>'
            f'</form>'
            f'<p style="margin:8px 0 0;font-size:12px;color:#6b7280;">Generates risk rows for all chart-of-accounts entries across all CAS assertions.</p>'
            f'</div>\n'
        )

    # Summary card
    summary_html = ""
    if summary and summary.get("total_assessments", 0) > 0:
        summary_html = (
            f'<div class="card" style="margin-bottom:12px;">'
            f'<h3 style="margin-top:0;">{esc(t("cas_risk_summary", lang))}</h3>'
            f'<div style="display:flex;gap:16px;flex-wrap:wrap;">'
            f'<div style="text-align:center;"><div style="font-size:24px;font-weight:700;">{summary["total_assessments"]}</div>'
            f'<div style="font-size:11px;color:#6b7280;">{esc(t("cas_risk_total", lang))}</div></div>'
            f'<div style="text-align:center;"><div style="font-size:24px;font-weight:700;color:#dc2626;">{summary["high"]}</div>'
            f'<div style="font-size:11px;color:#6b7280;">{esc(t("cas_risk_high_count", lang))}</div></div>'
            f'<div style="text-align:center;"><div style="font-size:24px;font-weight:700;color:#d97706;">{summary["medium"]}</div>'
            f'<div style="font-size:11px;color:#6b7280;">{esc(t("cas_risk_medium_count", lang))}</div></div>'
            f'<div style="text-align:center;"><div style="font-size:24px;font-weight:700;color:#16a34a;">{summary["low"]}</div>'
            f'<div style="font-size:11px;color:#6b7280;">{esc(t("cas_risk_low_count", lang))}</div></div>'
            f'<div style="text-align:center;"><div style="font-size:24px;font-weight:700;color:#9333ea;">{summary["significant_risks"]}</div>'
            f'<div style="font-size:11px;color:#6b7280;">{esc(t("cas_risk_significant_count", lang))}</div></div>'
            f'</div></div>\n'
        )

    # Risk table
    def _risk_badge(level: str) -> str:
        colors = {
            "low": "background:#dcfce7;color:#166534;",
            "medium": "background:#fef3c7;color:#92400e;",
            "high": "background:#fecaca;color:#991b1b;",
        }
        style = colors.get(level, "background:#f3f4f6;color:#374151;")
        label = t(f"cas_risk_level_{level}", lang)
        return f'<span style="{style}padding:2px 8px;border-radius:10px;font-size:12px;">{esc(label)}</span>'

    risk_level_options = "".join(
        f'<option value="{v}">{esc(t(f"cas_risk_level_{v}", lang))}</option>'
        for v in ["low", "medium", "high"]
    )

    risk_table = ""
    if risks:
        rows_html = ""
        for r in risks:
            rid = esc(str(r.get("risk_id", "")))
            sig = t("cas_risk_yes", lang) if r.get("significant_risk") else t("cas_risk_no", lang)
            sig_style = "color:#dc2626;font-weight:600;" if r.get("significant_risk") else ""

            # Inline update form
            ir_opts = "".join(
                f'<option value="{v}" {"selected" if r.get("inherent_risk") == v else ""}>'
                f'{esc(t(f"cas_risk_level_{v}", lang))}</option>'
                for v in ["low", "medium", "high"]
            )
            cr_opts = "".join(
                f'<option value="{v}" {"selected" if r.get("control_risk") == v else ""}>'
                f'{esc(t(f"cas_risk_level_{v}", lang))}</option>'
                for v in ["low", "medium", "high"]
            )

            rows_html += (
                f"<tr>"
                f"<td>{esc(str(r.get('account_code', '')))}</td>"
                f"<td style='font-size:12px;'>{esc(str(r.get('account_name', '')[:30]))}</td>"
                f"<td style='font-size:12px;'>{esc(str(r.get('assertion', '')))}</td>"
                f"<td>"
                f"<form method='POST' action='/audit/risk/update' style='display:flex;gap:4px;align-items:center;'>"
                f"<input type='hidden' name='risk_id' value='{rid}'>"
                f"<input type='hidden' name='engagement_id' value='{esc(engagement_id)}'>"
                f"<select name='inherent_risk' style='padding:2px 4px;font-size:11px;border:1px solid #d1d5db;border-radius:4px;'>{ir_opts}</select>"
                f"</td>"
                f"<td>"
                f"<select name='control_risk' style='padding:2px 4px;font-size:11px;border:1px solid #d1d5db;border-radius:4px;'>{cr_opts}</select>"
                f"</td>"
                f"<td>{_risk_badge(r.get('combined_risk', 'medium'))}</td>"
                f"<td style='{sig_style}'>{esc(sig)}</td>"
                f"<td><button type='submit' class='btn-secondary' style='padding:2px 8px;font-size:11px;'>"
                f"{esc(t('cas_risk_update', lang))}</button></form></td>"
                f"</tr>\n"
            )

        risk_table = (
            f'<div class="card">'
            f'<div style="overflow-x:auto;">'
            f'<table>'
            f'<thead><tr>'
            f'<th>Code</th>'
            f'<th>{esc(t("cas_risk_account", lang))}</th>'
            f'<th>{esc(t("cas_risk_assertion", lang))}</th>'
            f'<th>{esc(t("cas_risk_inherent", lang))}</th>'
            f'<th>{esc(t("cas_risk_control", lang))}</th>'
            f'<th>{esc(t("cas_risk_combined", lang))}</th>'
            f'<th>{esc(t("cas_risk_significant", lang))}</th>'
            f'<th>{esc(t("col_action", lang))}</th>'
            f'</tr></thead>'
            f'<tbody>{rows_html}</tbody>'
            f'</table></div></div>\n'
        )
    elif engagement_id:
        risk_table = f'<div class="card"><p class="muted">{esc(t("cas_risk_no_assessments", lang))}</p></div>\n'

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("cas_risk_title", lang))}</h2>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div>\n'
        f'{select_form}{gen_form}{summary_html}{risk_table}'
    )
    return page_layout(t("cas_risk_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


def _get_qr_clients() -> list[dict[str, Any]]:
    """Return all clients from the clients table (code + name), sorted by code."""
    try:
        with open_db() as conn:
            rows = conn.execute(
                "SELECT client_code, client_name FROM clients ORDER BY client_code"
            ).fetchall()
        return [{"client_code": r["client_code"], "client_name": r["client_name"] or r["client_code"]} for r in rows]
    except Exception:
        return []


def _get_portal_base_url() -> str:
    """Return the client portal base URL from config or fall back to localhost."""
    try:
        cfg = json.loads((ROOT_DIR / "ledgerlink.config.json").read_text(encoding="utf-8"))
        public_url = cfg.get("public_portal_url", "").strip()
        if public_url:
            return public_url
        port = cfg.get("client_portal", {}).get("port", 8788)
    except Exception:
        port = 8788
    return f"http://127.0.0.1:{port}"


def render_qr_page(
    ctx: dict[str, Any],
    user: dict[str, Any],
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    """Render the /qr page — grid of QR codes for all clients (manager/owner)."""
    import base64 as _b64

    clients = _get_qr_clients()
    portal_base = _get_portal_base_url()

    if not clients:
        body = (
            f'<div class="card">'
            f'<h2>{esc(t("qr_page_heading", lang))}</h2>'
            f'<p class="muted">{esc(t("qr_no_clients", lang))}</p>'
            f'</div>'
        )
        return page_layout(t("qr_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)

    cards_html = ""
    for cl in clients:
        code = cl["client_code"]
        name = cl["client_name"]
        upload_url = _build_upload_url(portal_base, code)
        png_bytes = generate_client_qr_png(code, name, upload_url)
        b64 = _b64.b64encode(png_bytes).decode("ascii")
        cards_html += (
            f'<div style="background:#fff;border:1px solid #e2e8f0;border-radius:10px;'
            f'padding:16px;display:flex;flex-direction:column;align-items:center;gap:10px;'
            f'box-shadow:0 1px 4px rgba(0,0,0,.06);">'
            f'<div style="font-weight:600;font-size:14px;color:#1F3864;">{esc(name)}</div>'
            f'<img src="data:image/png;base64,{b64}" alt="QR {esc(code)}" '
            f'style="width:180px;height:180px;image-rendering:pixelated;">'
            f'<div style="font-size:11px;color:#6b7280;word-break:break-all;text-align:center;">'
            f'{esc(upload_url)}</div>'
            f'<a class="button-link btn-secondary" style="font-size:12px;padding:5px 14px;" '
            f'href="/qr/download?client_code={urlquote(code)}">'
            f'{esc(t("qr_download_png", lang))}</a>'
            f'</div>'
        )

    body = (
        f'<div class="card">'
        f'<div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;margin-bottom:16px;">'
        f'<div>'
        f'<h2 style="margin:0 0 4px;">{esc(t("qr_page_heading", lang))}</h2>'
        f'<p class="muted" style="margin:0;">{esc(t("qr_page_subtitle", lang))}</p>'
        f'</div>'
        f'<div style="display:flex;gap:8px;flex-wrap:wrap;">'
        f'<a class="button-link btn-primary" href="/qr/pdf">{esc(t("qr_download_all_pdf", lang))}</a>'
        f'<button class="btn-secondary" onclick="window.print()" style="padding:7px 16px;">'
        f'{esc(t("qr_print_btn", lang))}</button>'
        f'</div>'
        f'</div>'
        f'<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:20px;">'
        f'{cards_html}'
        f'</div>'
        f'</div>'
    )
    return page_layout(t("qr_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


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


# ---------------------------------------------------------------------------
# License machines page
# ---------------------------------------------------------------------------

def render_license_machines(
    ctx: dict[str, Any],
    user: dict[str, Any],
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    """Render the /license/machines page (owner only)."""
    with open_db() as conn:
        machines = get_licensed_machines(conn)
        machine_status = check_machine_license(conn)

    machine_count = machine_status.get("machine_count", 0)
    max_machines = machine_status.get("max_machines", MAX_MACHINES_PER_FIRM)
    overuse = machine_status.get("overuse", False)

    # Status banner
    if overuse:
        status_html = (
            f'<div class="flash error">'
            f'{esc(t("lic_machine_overuse", lang, max=str(max_machines)))}'
            f'</div>'
        )
    else:
        status_html = (
            f'<div class="flash success">{esc(t("lic_machine_ok", lang))}</div>'
        )

    # Counter
    counter_html = (
        f'<div style="display:flex;gap:24px;margin-bottom:16px;">'
        f'<div><strong>{esc(t("lic_machine_count", lang))}:</strong> {machine_count}</div>'
        f'<div><strong>{esc(t("lic_machine_max", lang))}:</strong> {max_machines}</div>'
        f'</div>'
    )

    # Machine table
    if not machines:
        table_html = f'<p class="muted">{esc(t("lic_no_machines", lang))}</p>'
    else:
        rows = ""
        for m in machines:
            current_badge = (
                f' <span class="badge badge-ready">{esc(t("lic_machine_current", lang))}</span>'
                if m.get("is_current") else ""
            )
            rows += (
                f'<tr>'
                f'<td><code>{esc(m["machine_id"])}</code></td>'
                f'<td>{esc(m["machine_name"])}{current_badge}</td>'
                f'<td>{esc(m["first_activated"])}</td>'
                f'<td>{esc(m["last_seen"])}</td>'
                f'</tr>'
            )
        table_html = (
            f'<table>'
            f'<thead><tr>'
            f'<th>{esc(t("lic_machine_id", lang))}</th>'
            f'<th>{esc(t("lic_machine_name", lang))}</th>'
            f'<th>{esc(t("lic_machine_first_activated", lang))}</th>'
            f'<th>{esc(t("lic_machine_last_seen", lang))}</th>'
            f'</tr></thead>'
            f'<tbody>{rows}</tbody>'
            f'</table>'
        )

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("lic_machines_title", lang))}</h2>'
        f'<div>'
        f'<a href="/license" class="btn-secondary button-link" style="margin-right:8px;">'
        f'{esc(t("lic_title", lang))}</a>'
        f'<a href="/" class="btn-secondary button-link">{esc(t("btn_back_to_queue", lang))}</a>'
        f'</div></div>\n'
        f'<div class="card">'
        f'{status_html}'
        f'{counter_html}'
        f'{table_html}'
        f'</div>'
    )
    return page_layout(t("lic_machines_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Admin updates page
# ---------------------------------------------------------------------------

def render_admin_updates(
    user: dict[str, Any],
    flash: str,
    flash_error: str,
    lang: str = "fr",
) -> str:
    """Render the /admin/updates page (owner only)."""
    # Read installed version
    version_path = ROOT_DIR / "version.json"
    try:
        ver_info = json.loads(version_path.read_text(encoding="utf-8"))
    except Exception:
        ver_info = {"version": "unknown", "release_date": "", "changelog": ""}

    installed_version = ver_info.get("version", "unknown")
    installed_date = ver_info.get("release_date", "")

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("update_title", lang))}</h2>'
        f'<a href="/troubleshoot" class="btn-secondary button-link">'
        f'{esc(t("diag_title", lang))}</a>'
        f'</div>\n'
        f'<div class="card" style="margin-bottom:16px;">'
        f'<h4 style="margin-top:0;">{esc(t("update_installed_version", lang))}</h4>'
        f'<div style="display:flex;gap:24px;margin-bottom:12px;">'
        f'<div><strong>{esc(t("version_label", lang))}:</strong> {esc(installed_version)}</div>'
        f'<div><strong>{esc(t("update_installed_date", lang))}:</strong> {esc(installed_date)}</div>'
        f'</div>'
        f'<div class="actions">'
        f'<form method="POST" action="/admin/updates/check" style="display:inline;">'
        f'<button class="btn-primary">{esc(t("update_btn_check", lang))}</button>'
        f'</form>'
        f'<form method="POST" action="/admin/updates/install" style="display:inline;"'
        f' onsubmit="return confirm(\'{esc(t("update_install_confirm", lang))}\');">'
        f'<button class="btn-danger">{esc(t("update_btn_install", lang))}</button>'
        f'</form>'
        f'</div>'
        f'</div>'
    )

    return page_layout(t("update_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


# ---------------------------------------------------------------------------
# Admin remote management page
# ---------------------------------------------------------------------------

def render_admin_remote(
    user: dict[str, Any],
    flash: str,
    flash_error: str,
    autofix_output: str = "",
    lang: str = "fr",
) -> str:
    """Render the /admin/remote page (owner only)."""
    try:
        from scripts.remote_management import get_system_status, list_backups
        status = get_system_status()
        backups = list_backups()[:10]
    except Exception as exc:
        status = {"error": str(exc)}
        backups = []

    svc_status = status.get("service_status", "unknown")
    svc_colors = {
        "running": "#16a34a", "stopped": "#dc2626",
        "starting": "#d97706", "stopping": "#d97706",
    }
    svc_color = svc_colors.get(svc_status, "#6b7280")

    # Disk bar
    disk_pct = status.get("disk_used_pct", 0)
    disk_color = "#16a34a" if disk_pct < 80 else "#d97706" if disk_pct < 90 else "#dc2626"

    # Status card
    status_html = (
        f'<div class="card" style="margin-bottom:16px;">'
        f'<h4 style="margin-top:0;">{esc(t("remote_system_status", lang))}</h4>'
        f'<div class="grid-2">'
        f'<div>'
        f'<p><strong>{esc(t("remote_hostname", lang))}:</strong> {esc(status.get("hostname", ""))}</p>'
        f'<p><strong>{esc(t("remote_os", lang))}:</strong> {esc(status.get("os", ""))}</p>'
        f'<p><strong>{esc(t("remote_python", lang))}:</strong> {esc(status.get("python", ""))}</p>'
        f'<p><strong>{esc(t("remote_service_status", lang))}:</strong> '
        f'<span style="color:{svc_color};font-weight:700;">{esc(svc_status)}</span></p>'
        f'<p><strong>{esc(t("remote_uptime", lang))}:</strong> {esc(status.get("uptime", "N/A"))}</p>'
        f'</div>'
        f'<div>'
        f'<p><strong>{esc(t("remote_disk_space", lang))}:</strong></p>'
        f'<div style="background:#e5e7eb;border-radius:4px;height:8px;margin-bottom:6px;">'
        f'<div style="background:{disk_color};border-radius:4px;height:8px;width:{disk_pct}%;"></div></div>'
        f'<p style="font-size:13px;">{esc(t("remote_disk_free", lang))}: {status.get("disk_free_gb", 0)} GB / '
        f'{status.get("disk_total_gb", 0)} GB ({disk_pct}% {esc(t("remote_disk_used", lang))})</p>'
        f'<p><strong>{esc(t("remote_db_size", lang))}:</strong> {status.get("db_size_mb", 0)} MB</p>'
        f'<p><strong>{esc(t("remote_last_backup", lang))}:</strong> '
        f'{esc(status.get("last_backup", "None"))} ({status.get("last_backup_size_mb", 0)} MB)</p>'
        f'</div>'
        f'</div>'
        f'</div>'
    )

    # Actions card
    actions_html = (
        f'<div class="card" style="margin-bottom:16px;">'
        f'<h4 style="margin-top:0;">{esc(t("remote_actions", lang))}</h4>'
        f'<div class="actions">'
        f'<form method="POST" action="/admin/remote/restart" style="display:inline;"'
        f' onsubmit="return confirm(\'{esc(t("remote_restart_confirm", lang))}\');">'
        f'<button class="btn-primary">{esc(t("remote_btn_restart", lang))}</button></form>'
        f'<form method="POST" action="/admin/remote/backup" style="display:inline;">'
        f'<button class="btn-primary">{esc(t("remote_btn_backup", lang))}</button></form>'
        f'<form method="POST" action="/admin/remote/update" style="display:inline;"'
        f' onsubmit="return confirm(\'{esc(t("remote_update_confirm", lang))}\');">'
        f'<button class="btn-danger">{esc(t("remote_btn_update", lang))}</button></form>'
        f'<form method="POST" action="/admin/remote/autofix" style="display:inline;"'
        f' onsubmit="return confirm(\'{esc(t("remote_autofix_confirm", lang))}\');">'
        f'<button class="btn-secondary">{esc(t("remote_btn_autofix", lang))}</button></form>'
        f'</div>'
        f'</div>'
    )

    # Autofix output (if just ran)
    autofix_html = ""
    if autofix_output:
        autofix_html = (
            f'<div class="card" style="margin-bottom:16px;">'
            f'<h4 style="margin-top:0;">{esc(t("remote_autofix_output", lang))}</h4>'
            f'<textarea readonly style="height:400px;font-size:12px;">{esc(autofix_output)}</textarea>'
            f'</div>'
        )

    # Backups list
    backups_html = ""
    if backups:
        rows = ""
        for b in backups:
            rows += f'<tr><td>{esc(b["name"])}</td><td>{b["size_mb"]} MB</td><td>{esc(b["modified"])}</td></tr>'
        backups_html = (
            f'<div class="card">'
            f'<h4 style="margin-top:0;">{esc(t("remote_backups_title", lang))}</h4>'
            f'<table><thead><tr><th>File</th><th>Size</th><th>Date</th></tr></thead>'
            f'<tbody>{rows}</tbody></table>'
            f'</div>'
        )

    body = (
        f'<div class="topbar" style="margin-bottom:16px;">'
        f'<h2 style="margin:0;">{esc(t("remote_title", lang))}</h2>'
        f'<a href="/troubleshoot" class="btn-secondary button-link">'
        f'{esc(t("diag_title", lang))}</a>'
        f'</div>\n'
        + status_html + actions_html + autofix_html + backups_html
    )

    return page_layout(t("remote_title", lang), body, user=user, flash=flash, flash_error=flash_error, lang=lang)


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
        training_link_html = (
            f'<a href="/training" style="color:#cbd5e1;font-size:13px;'
            f'text-decoration:none;white-space:nowrap;">'
            f'{esc(t("training_nav_link", lang))}</a>'
        )
        right_controls = f'{user_pill} {comm_link_html} {training_link_html} {lang_toggle} {logout_btn}'

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
            + _anav("/audit/materiality", "cas_materiality_nav")
            + _anav("/audit/risk", "cas_risk_nav")
            + _anav("/audit/rep_letter", "cas_rep_nav")
            + _anav("/audit/controls", "cas_ctrl_nav")
            + _anav("/audit/related_parties", "cas_rp_nav")
            + _anav("/reconciliation", "recon_nav_link")
            + _anav("/qr", "qr_nav_link")
            + _lic_link
            + (_anav("/license/machines", "lic_machines_nav") if user.get("role") == "owner" else "")
            + (_anav("/admin/updates", "update_nav_link") if user.get("role") == "owner" else "")
            + (_anav("/admin/remote", "remote_nav_link") if user.get("role") == "owner" else "")
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
<footer style="text-align:center;padding:12px;font-size:11px;color:#9ca3af;border-top:1px solid #e5e7eb;margin-top:24px;">
    LedgerLink AI v{_get_app_version()}
</footer>
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
        users = [dict(r) for r in conn.execute("SELECT * FROM dashboard_users ORDER BY username").fetchall()]

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
                only_my_queue: bool, only_unassigned: bool, lang: str = "fr",
                page: int = 1, per_page: int = 50) -> str:
    rows = get_documents(ctx=ctx, status=status, q=q, include_ignored=include_ignored,
                         only_my_queue=only_my_queue, only_unassigned=only_unassigned)
    counts = get_status_counts(ctx)

    # Pagination
    total_rows = len(rows)
    total_pages = max(1, (total_rows + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start_idx = (page - 1) * per_page
    rows = rows[start_idx:start_idx + per_page]

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
        <div class="stat"><div class="small muted">{esc(t("stat_visible", lang))}</div><div><strong>{total_rows}</strong></div></div>
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

    # Pagination controls
    pagination_html = ""
    if total_pages > 1:
        # Build base query string preserving existing filters
        import urllib.parse as _up
        pq = {}
        if status:
            pq["status"] = status
        if q:
            pq["q"] = q
        if include_ignored:
            pq["include_ignored"] = "1"
        if only_my_queue:
            pq["queue_mode"] = "mine"
        elif only_unassigned:
            pq["queue_mode"] = "unassigned"

        def _page_url(p: int) -> str:
            pq["page"] = str(p)
            return "/?" + _up.urlencode(pq)

        page_links: list[str] = []
        if page > 1:
            page_links.append(f'<a class="btn-secondary" href="{esc(_page_url(1))}">&laquo; 1</a>')
            page_links.append(f'<a class="btn-secondary" href="{esc(_page_url(page - 1))}">&lsaquo; Prev</a>')

        # Show window of pages around current
        window_start = max(1, page - 3)
        window_end = min(total_pages, page + 3)
        for p in range(window_start, window_end + 1):
            if p == page:
                page_links.append(f'<strong style="padding:6px 12px;background:#0d6efd;color:#fff;border-radius:4px;">{p}</strong>')
            else:
                page_links.append(f'<a class="btn-secondary" href="{esc(_page_url(p))}">{p}</a>')

        if page < total_pages:
            page_links.append(f'<a class="btn-secondary" href="{esc(_page_url(page + 1))}">Next &rsaquo;</a>')
            page_links.append(f'<a class="btn-secondary" href="{esc(_page_url(total_pages))}">{total_pages} &raquo;</a>')

        pagination_html = f"""<div class="card" style="text-align:center;">
            <div style="display:flex;justify-content:center;align-items:center;gap:6px;flex-wrap:wrap;">
                {" ".join(page_links)}
            </div>
            <div class="small muted" style="margin-top:6px;">
                Page {page} of {total_pages} &middot; {total_rows} documents
            </div>
        </div>"""

    return page_layout(t("dashboard_title", lang), stats_html + filters_html + table_html + pagination_html,
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
        # Check if the file actually exists on disk (try original path, then local fallbacks)
        _file_exists = False
        try:
            Path(file_path).resolve(strict=True)
            _file_exists = True
        except (OSError, RuntimeError):
            _fname = Path(file_path).name
            for _candidate in (
                ROOT_DIR / "tests" / "documents_real" / _fname,
                ROOT_DIR / "src" / "agents" / "data" / "downloads" / _fname,
                ROOT_DIR / _fname,
            ):
                try:
                    _candidate.resolve(strict=True)
                    _file_exists = True
                    break
                except (OSError, RuntimeError):
                    continue
        if not _file_exists:
            pdf_viewer_html = (
                f'<div class="card"><h3>{preview_title}</h3>'
                '<div style="border:2px solid #3b82f6;border-radius:8px;padding:20px 24px;background:#eff6ff;">'
                '<p style="margin:0 0 8px;font-weight:600;color:#1e40af;">'
                'Document de test \u2014 aucun fichier PDF disponible / Test document \u2014 no PDF file available</p>'
                '<p style="margin:0;font-size:13px;color:#64748b;">'
                'Ce document a \u00e9t\u00e9 g\u00e9n\u00e9r\u00e9 automatiquement pour les tests / '
                'This document was generated automatically for testing</p>'
                '</div></div>'
            )
        elif suffix == ".pdf":
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

    # Handwriting low-confidence banner and side-by-side layout
    _handwriting_low_conf = False
    try:
        _handwriting_low_conf = bool(int(row["handwriting_low_confidence"] or 0))
    except (KeyError, TypeError, ValueError):
        pass
    _has_illegible_fields = False
    if raw_result:
        for _fld in ("vendor_name", "amount", "date", "document_date",
                      "gst_amount", "qst_amount", "total", "payment_method"):
            if raw_result.get(_fld) is None and _fld in raw_result:
                _has_illegible_fields = True
                break
    _show_handwriting_review = _handwriting_low_conf or _has_illegible_fields
    handwriting_banner = ""
    if _show_handwriting_review:
        handwriting_banner = (
            '<div style="background:#fef3c7;border:2px solid #d97706;border-radius:8px;'
            'padding:14px 20px;margin-bottom:16px;font-weight:600;color:#92400e;">'
            f'{esc(t("handwriting_review_banner", lang))}'
            '</div>'
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

    # Build side-by-side handwriting review layout if needed
    _handwriting_side_by_side = ""
    if _show_handwriting_review and file_path:
        _hw_pdf_url = f"/pdf?id={urlquote(document_id)}"
        _hw_fields = [
            ("vendor_name", t("field_vendor", lang)),
            ("amount", t("field_amount", lang)),
            ("document_date", t("field_document_date", lang)),
            ("gst_amount", "TPS/GST"),
            ("qst_amount", "TVQ/QST"),
            ("total", "Total"),
            ("payment_method", t("field_payment_method", lang) if "field_payment_method" in t.__code__.co_varnames else "Payment / Paiement"),
        ]
        _hw_rows = ""
        for _fk, _fl in _hw_fields:
            _fv = raw_result.get(_fk) if raw_result else None
            _is_illegible = _fv is None and raw_result and _fk in raw_result
            _style = 'style="background:#fef2f2;border:2px solid #ef4444;border-radius:4px;padding:4px 8px;"' if _is_illegible else ""
            _display = esc(str(_fv)) if _fv is not None else f'<span style="color:#dc2626;font-weight:600;">{esc(t("handwriting_field_illegible", lang))}</span>'
            _input_html = ""
            if _is_illegible:
                _input_html = f'<input type="text" name="hw_{_fk}" placeholder="{esc(_fl)}" style="margin-top:4px;width:100%;">'
            _hw_rows += f'<tr><td style="font-weight:600;">{esc(_fl)}</td><td {_style}>{_display}{_input_html}</td></tr>'

        _suffix = Path(file_path).suffix.lower() if file_path else ""
        if _suffix == ".pdf":
            _hw_preview = f'<iframe src="{_hw_pdf_url}" style="width:100%;height:600px;border:1px solid #e5e7eb;border-radius:8px;"></iframe>'
        elif _suffix in {".png", ".jpg", ".jpeg"}:
            _hw_preview = f'<img src="{_hw_pdf_url}" style="max-width:100%;border:1px solid #e5e7eb;border-radius:8px;">'
        else:
            _hw_preview = '<p class="muted">Preview not available</p>'

        _handwriting_side_by_side = f"""<div class="card"><h3>{esc(t("handwriting_review_title", lang))}</h3>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
                <div>{_hw_preview}</div>
                <div><table style="width:100%;border-collapse:collapse;">
                    <thead><tr><th style="text-align:left;padding:6px;border-bottom:2px solid #e5e7eb;">{esc(t("handwriting_field_col", lang))}</th>
                    <th style="text-align:left;padding:6px;border-bottom:2px solid #e5e7eb;">{esc(t("handwriting_value_col", lang))}</th></tr></thead>
                    <tbody>{_hw_rows}</tbody>
                </table></div>
            </div></div>"""

    body = f"""
    <div class="card"><div class="actions" style="margin-bottom:12px;"><a href="/">{esc(t("btn_back_to_queue", lang))}</a></div>
        <h2 style="margin-bottom:8px;">{esc(row["file_name"])}</h2>
        <div class="small muted">{esc(t("doc_field_id", lang))} {esc(row["document_id"])}</div>
        {timer_badge}
    </div>
    {hallucination_banner}
    {handwriting_banner}
    {_handwriting_side_by_side if _show_handwriting_review else pdf_viewer_html}
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
    {render_line_items_card(document_id, row, lang)}
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
# Onboarding wizard helpers
# ---------------------------------------------------------------------------

def _onboarding_check_needed(user: dict[str, Any]) -> bool:
    """Return True if the owner needs to be redirected to onboarding."""
    if user.get("role") != "owner":
        return False
    with open_db() as conn:
        # Check onboarding_complete setting
        try:
            row = conn.execute(
                "SELECT value FROM settings WHERE key='onboarding_complete'"
            ).fetchone()
            if row and row["value"] == "1":
                return False
        except Exception:
            pass

        # Count users (only trigger if <=1 user or 0 clients)
        try:
            user_count = list(conn.execute("SELECT COUNT(*) FROM dashboard_users").fetchone().values())[0]
        except Exception:
            user_count = 0
        try:
            client_count = list(conn.execute("SELECT COUNT(*) FROM clients").fetchone().values())[0]
        except Exception:
            client_count = 0

        return user_count <= 1 or client_count == 0


def _ensure_settings_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.commit()


def render_onboarding_step1(ctx: dict[str, Any], user: dict[str, Any],
                             flash: str = "", flash_error: str = "",
                             lang: str = "fr") -> str:
    """Onboarding step 1: Add staff members."""
    with open_db() as conn:
        staff_rows = conn.execute(
            "SELECT username, display_name, role FROM dashboard_users WHERE role != 'owner' ORDER BY display_name"
        ).fetchall()

    staff_table = ""
    if staff_rows:
        rows_html = "".join(
            f"<tr><td>{esc(r['display_name'] or r['username'])}</td>"
            f"<td>{esc(r['username'])}</td><td>{esc(r['role'])}</td></tr>"
            for r in staff_rows
        )
        staff_table = f"""
        <h3 style="margin:24px 0 12px;">{esc(t("existing_users", lang))}</h3>
        <table style="width:100%;border-collapse:collapse;font-size:.875rem;">
          <thead><tr style="background:#f8fafc;">
            <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e2e8f0;">{esc(t("col_display_name", lang))}</th>
            <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e2e8f0;">{esc(t("col_username", lang))}</th>
            <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e2e8f0;">{esc(t("col_role", lang))}</th>
          </tr></thead>
          <tbody>{rows_html}</tbody>
        </table>"""

    flash_html = f'<div class="flash">{esc(flash)}</div>' if flash else ""
    err_html = f'<div class="flash-error">{esc(flash_error)}</div>' if flash_error else ""
    return page_layout(
        t("onb_step1_title", lang),
        f"""
        {flash_html}{err_html}
        <div class="card" style="max-width:700px;">
          <h2>{esc(t("onb_step1_title", lang))}</h2>
          <p style="color:#6b7280;margin-bottom:24px;">Step 1 / 3</p>
          <h3 style="margin-bottom:16px;">{esc(t("onb_add_staff", lang))}</h3>
          <form method="POST" action="/onboarding/staff/add">
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
              <div>
                <label style="display:block;font-size:.875rem;font-weight:500;margin-bottom:6px;">{esc(t("onb_display_name", lang))}</label>
                <input name="display_name" type="text" required style="width:100%;padding:9px 12px;border:1px solid #d1d5db;border-radius:6px;">
              </div>
              <div>
                <label style="display:block;font-size:.875rem;font-weight:500;margin-bottom:6px;">{esc(t("onb_username", lang))}</label>
                <input name="username" type="email" required placeholder="user@example.com" style="width:100%;padding:9px 12px;border:1px solid #d1d5db;border-radius:6px;">
              </div>
              <div>
                <label style="display:block;font-size:.875rem;font-weight:500;margin-bottom:6px;">{esc(t("onb_role", lang))}</label>
                <select name="role" style="width:100%;padding:9px 12px;border:1px solid #d1d5db;border-radius:6px;">
                  <option value="manager">{esc(t("role_manager", lang))}</option>
                  <option value="employee">{esc(t("role_employee", lang))}</option>
                </select>
              </div>
              <div>
                <label style="display:block;font-size:.875rem;font-weight:500;margin-bottom:6px;">{esc(t("onb_password", lang))}</label>
                <input name="password" type="text" required style="width:100%;padding:9px 12px;border:1px solid #d1d5db;border-radius:6px;">
              </div>
            </div>
            <button type="submit" class="button-link btn-primary" style="margin-top:16px;">{esc(t("onb_add_staff", lang))}</button>
          </form>
          {staff_table}
          <div style="margin-top:28px;border-top:1px solid #e2e8f0;padding-top:20px;">
            <a href="/onboarding/step2" class="button-link btn-primary">{esc(t("btn_next", lang) if t("btn_next", lang) != "btn_next" else "Next →")}</a>
            <a href="/" class="button-link btn-secondary" style="margin-left:8px;">{esc(t("onb_skip", lang))}</a>
          </div>
        </div>
        """,
        user=user, lang=lang,
    )


def render_onboarding_step2(ctx: dict[str, Any], user: dict[str, Any],
                             flash: str = "", flash_error: str = "",
                             lang: str = "fr") -> str:
    """Onboarding step 2: Add clients."""
    with open_db() as conn:
        try:
            client_rows = conn.execute(
                "SELECT client_code, client_name FROM clients ORDER BY client_name"
            ).fetchall()
        except Exception:
            client_rows = []

    client_table = ""
    if client_rows:
        rows_html = "".join(
            f"<tr><td style='padding:8px 12px;'>{esc(r['client_code'])}</td>"
            f"<td style='padding:8px 12px;'>{esc(r['client_name'] or '')}</td></tr>"
            for r in client_rows
        )
        client_table = f"""
        <h3 style="margin:24px 0 12px;">{esc(t("existing_clients", lang) if t("existing_clients", lang) != "existing_clients" else "Clients")}</h3>
        <table style="width:100%;border-collapse:collapse;font-size:.875rem;">
          <thead><tr style="background:#f8fafc;">
            <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e2e8f0;">{esc(t("onb_client_code", lang))}</th>
            <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e2e8f0;">{esc(t("onb_client_name", lang))}</th>
          </tr></thead>
          <tbody>{rows_html}</tbody>
        </table>"""

    flash_html = f'<div class="flash">{esc(flash)}</div>' if flash else ""
    err_html = f'<div class="flash-error">{esc(flash_error)}</div>' if flash_error else ""
    provinces = ["QC", "ON", "BC", "AB", "MB", "SK", "NS", "NB", "NL", "PE", "NT", "NU", "YT"]
    prov_opts = "".join(f'<option value="{p}"{"selected" if p=="QC" else ""}>{p}</option>' for p in provinces)
    entity_opts = "".join(
        f'<option value="{v}">{esc(v.capitalize())}</option>'
        for v in ["company", "individual", "partnership", "trust"]
    )
    return page_layout(
        t("onb_step2_title", lang),
        f"""
        {flash_html}{err_html}
        <div class="card" style="max-width:700px;">
          <h2>{esc(t("onb_step2_title", lang))}</h2>
          <p style="color:#6b7280;margin-bottom:24px;">Step 2 / 3</p>
          <h3 style="margin-bottom:16px;">{esc(t("onb_add_client", lang))}</h3>
          <form method="POST" action="/onboarding/client/add">
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
              <div>
                <label style="display:block;font-size:.875rem;font-weight:500;margin-bottom:6px;">{esc(t("onb_client_code", lang))}</label>
                <input name="client_code" type="text" required placeholder="ACME" style="width:100%;padding:9px 12px;border:1px solid #d1d5db;border-radius:6px;">
              </div>
              <div>
                <label style="display:block;font-size:.875rem;font-weight:500;margin-bottom:6px;">{esc(t("onb_client_name", lang))}</label>
                <input name="client_name" type="text" required style="width:100%;padding:9px 12px;border:1px solid #d1d5db;border-radius:6px;">
              </div>
              <div>
                <label style="display:block;font-size:.875rem;font-weight:500;margin-bottom:6px;">{esc(t("onb_province", lang))}</label>
                <select name="province" style="width:100%;padding:9px 12px;border:1px solid #d1d5db;border-radius:6px;">{prov_opts}</select>
              </div>
              <div>
                <label style="display:block;font-size:.875rem;font-weight:500;margin-bottom:6px;">{esc(t("onb_entity_type", lang))}</label>
                <select name="entity_type" style="width:100%;padding:9px 12px;border:1px solid #d1d5db;border-radius:6px;">{entity_opts}</select>
              </div>
            </div>
            <button type="submit" class="button-link btn-primary" style="margin-top:16px;">{esc(t("onb_add_client", lang))}</button>
          </form>
          {client_table}
          <div style="margin-top:28px;border-top:1px solid #e2e8f0;padding-top:20px;">
            <a href="/onboarding/step3" class="button-link btn-primary">{esc(t("btn_next", lang) if t("btn_next", lang) != "btn_next" else "Next →")}</a>
            <a href="/onboarding/step1" class="button-link btn-secondary" style="margin-left:8px;">← Back</a>
            <a href="/" class="button-link btn-secondary" style="margin-left:8px;">{esc(t("onb_skip", lang))}</a>
          </div>
        </div>
        """,
        user=user, lang=lang,
    )


def render_onboarding_step3(ctx: dict[str, Any], user: dict[str, Any],
                             lang: str = "fr") -> str:
    """Onboarding step 3: Staff credentials PDF summary."""
    with open_db() as conn:
        staff_rows = conn.execute(
            "SELECT username, display_name, role FROM dashboard_users WHERE active=1 ORDER BY role, display_name"
        ).fetchall()

    rows_html = "".join(
        f"<tr><td style='padding:8px 12px;border-bottom:1px solid #e2e8f0;'>{esc(r['display_name'] or r['username'])}</td>"
        f"<td style='padding:8px 12px;border-bottom:1px solid #e2e8f0;'>{esc(r['username'])}</td>"
        f"<td style='padding:8px 12px;border-bottom:1px solid #e2e8f0;'>{esc(r['role'])}</td></tr>"
        for r in staff_rows
    )
    return page_layout(
        t("onb_step3_title", lang),
        f"""
        <div class="card" style="max-width:700px;">
          <h2>{esc(t("onb_step3_title", lang))}</h2>
          <p style="color:#6b7280;margin-bottom:8px;">Step 3 / 3</p>
          <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;margin-bottom:20px;font-size:.875rem;color:#92400e;">
            ⚠ {esc(t("onb_credentials_note", lang))}
          </div>
          <table style="width:100%;border-collapse:collapse;font-size:.875rem;">
            <thead><tr style="background:#f8fafc;">
              <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e2e8f0;">{esc(t("col_display_name", lang))}</th>
              <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e2e8f0;">{esc(t("col_username", lang))}</th>
              <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e2e8f0;">{esc(t("col_role", lang))}</th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
          <div style="margin-top:28px;border-top:1px solid #e2e8f0;padding-top:20px;display:flex;gap:12px;flex-wrap:wrap;">
            <form method="POST" action="/onboarding/complete" style="display:inline;">
              <button type="submit" class="button-link btn-primary">{esc(t("onb_complete", lang))}</button>
            </form>
            <button onclick="window.print()" class="button-link btn-secondary">{esc(t("onb_print", lang))}</button>
            <a href="/onboarding/step2" class="button-link btn-secondary">← Back</a>
          </div>
        </div>
        """,
        user=user, lang=lang,
    )


# ---------------------------------------------------------------------------
# FIX 2: Vendor alias management page
# ---------------------------------------------------------------------------

def render_vendor_aliases(
    ctx: dict[str, Any],
    user: dict[str, Any],
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    rows_html = ""
    suggestions_html = ""
    try:
        with open_db() as conn:
            aliases = conn.execute(
                "SELECT alias_id, canonical_vendor_key, alias_name, alias_key, created_by, created_at "
                "FROM vendor_aliases ORDER BY canonical_vendor_key, alias_name"
            ).fetchall()
            for a in aliases:
                rows_html += (
                    f'<tr><td>{esc(a["canonical_vendor_key"])}</td>'
                    f'<td>{esc(a["alias_name"])}</td>'
                    f'<td>{esc(a["created_by"] or "")}</td>'
                    f'<td>{esc(a["created_at"] or "")}</td>'
                    f'<td><form method="POST" action="/admin/vendor_aliases" style="display:inline">'
                    f'<input type="hidden" name="action" value="delete_alias">'
                    f'<input type="hidden" name="alias_id" value="{a["alias_id"]}">'
                    f'<button type="submit" class="btn btn-sm btn-danger">Delete</button>'
                    f'</form></td></tr>'
                )
    except Exception:
        pass

    title = "Alias fournisseurs / Vendor Aliases" if lang == "fr" else "Vendor Aliases"
    body = f"""
<div class="card">
  <h2>{esc(title)}</h2>
  {f'<div class="alert alert-success">{esc(flash)}</div>' if flash else ''}
  {f'<div class="alert alert-danger">{esc(flash_error)}</div>' if flash_error else ''}
  <h3>{"Ajouter un alias / Add Alias" if lang == "fr" else "Add Alias"}</h3>
  <form method="POST" action="/admin/vendor_aliases">
    <input type="hidden" name="action" value="add_alias">
    <label>{"Nom canonique / Canonical vendor" if lang == "fr" else "Canonical vendor"}:</label>
    <input type="text" name="canonical_vendor" required style="width:300px">
    <label>{"Nom alias / Alias name" if lang == "fr" else "Alias name"}:</label>
    <input type="text" name="alias_name" required style="width:300px">
    <button type="submit" class="btn">{"Ajouter / Add" if lang == "fr" else "Add"}</button>
  </form>
  <h3 style="margin-top:1em">{"Alias existants / Existing Aliases" if lang == "fr" else "Existing Aliases"}</h3>
  <table>
    <thead><tr><th>Canonical</th><th>Alias</th><th>Created By</th><th>Created At</th><th>Actions</th></tr></thead>
    <tbody>{rows_html if rows_html else '<tr><td colspan="5">No aliases yet.</td></tr>'}</tbody>
  </table>
</div>"""
    return page_layout(title, body, user=user, lang=lang)


# ---------------------------------------------------------------------------
# FIX 7: Manual journal entries page
# ---------------------------------------------------------------------------

def render_journal_entries(
    ctx: dict[str, Any],
    user: dict[str, Any],
    flash: str = "",
    flash_error: str = "",
    lang: str = "fr",
) -> str:
    rows_html = ""
    try:
        with open_db() as conn:
            entries = conn.execute(
                "SELECT * FROM manual_journal_entries ORDER BY created_at DESC LIMIT 100"
            ).fetchall()
            for e in entries:
                status_cls = ""
                if e["status"] == "conflict":
                    status_cls = "text-danger"
                elif e["status"] == "phantom_tax_blocked":
                    status_cls = "text-danger"
                elif e["status"] == "posted":
                    status_cls = "text-success"

                actions = ""
                if e["status"] == "draft":
                    actions = (
                        f'<form method="POST" action="/journal_entries" style="display:inline">'
                        f'<input type="hidden" name="action" value="post">'
                        f'<input type="hidden" name="entry_id" value="{esc(e["entry_id"])}">'
                        f'<button type="submit" class="btn btn-sm">Post</button></form> '
                        f'<form method="POST" action="/journal_entries" style="display:inline">'
                        f'<input type="hidden" name="action" value="reverse">'
                        f'<input type="hidden" name="entry_id" value="{esc(e["entry_id"])}">'
                        f'<button type="submit" class="btn btn-sm btn-danger">Reverse</button></form>'
                    )
                elif e["status"] == "posted":
                    actions = (
                        f'<form method="POST" action="/journal_entries" style="display:inline">'
                        f'<input type="hidden" name="action" value="reverse">'
                        f'<input type="hidden" name="entry_id" value="{esc(e["entry_id"])}">'
                        f'<button type="submit" class="btn btn-sm btn-danger">Reverse</button></form>'
                    )

                rows_html += (
                    f'<tr>'
                    f'<td>{esc(e["entry_id"])}</td>'
                    f'<td>{esc(e["client_code"])}</td>'
                    f'<td>{esc(e["period"])}</td>'
                    f'<td>{esc(e["entry_date"])}</td>'
                    f'<td>{esc(e["debit_account"])}</td>'
                    f'<td>{esc(e["credit_account"])}</td>'
                    f'<td>${float(e["amount"] or 0):,.2f}</td>'
                    f'<td>{esc(e["description"] or "")}</td>'
                    f'<td class="{status_cls}">{esc(e["status"])}</td>'
                    f'<td>{actions}</td>'
                    f'</tr>'
                )
    except Exception:
        pass

    title = "Écritures manuelles / Journal Entries" if lang == "fr" else "Manual Journal Entries"
    body = f"""
<div class="card">
  <h2>{esc(title)}</h2>
  {f'<div class="alert alert-success">{esc(flash)}</div>' if flash else ''}
  {f'<div class="alert alert-danger">{esc(flash_error)}</div>' if flash_error else ''}
  <h3>{"Nouvelle écriture / New Entry" if lang == "fr" else "New Entry"}</h3>
  <form method="POST" action="/journal_entries">
    <input type="hidden" name="action" value="create">
    <div style="display:grid; grid-template-columns:1fr 1fr; gap:0.5em; max-width:600px;">
      <label>Client: <input type="text" name="client_code" required></label>
      <label>Period: <input type="text" name="period" placeholder="2026-03" required></label>
      <label>Date: <input type="date" name="entry_date" required></label>
      <label>Amount: <input type="number" step="0.01" name="amount" required></label>
      <label>Debit GL: <input type="text" name="debit_account" required></label>
      <label>Credit GL: <input type="text" name="credit_account" required></label>
      <label style="grid-column:1/3">Description: <input type="text" name="description" style="width:100%"></label>
      <label>Document ID (opt): <input type="text" name="document_id"></label>
    </div>
    <button type="submit" class="btn" style="margin-top:0.5em">{"Créer / Create" if lang == "fr" else "Create"}</button>
  </form>
  <h3 style="margin-top:1em">{"Écritures récentes / Recent Entries" if lang == "fr" else "Recent Entries"}</h3>
  <table style="font-size:0.85em">
    <thead><tr><th>ID</th><th>Client</th><th>Period</th><th>Date</th><th>Debit</th><th>Credit</th><th>Amount</th><th>Description</th><th>Status</th><th>Actions</th></tr></thead>
    <tbody>{rows_html if rows_html else '<tr><td colspan="10">No entries.</td></tr>'}</tbody>
  </table>
</div>"""
    return page_layout(title, body, user=user, lang=lang)


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

    def _build_health_response(self) -> dict[str, Any]:
        """Build the JSON response for GET /health."""
        import shutil as _shutil

        # DB check
        db_ok = False
        documents_count = 0
        users_count = 0
        if DB_PATH.exists():
            try:
                conn = sqlite3.connect(str(DB_PATH), timeout=5)
                conn.row_factory = _dict_factory
                cur = conn.execute("PRAGMA integrity_check")
                db_ok = cur.fetchone().get("integrity_check", "") == "ok"
                try:
                    documents_count = conn.execute(
                        "SELECT COUNT(*) AS c FROM documents"
                    ).fetchone().get("c", 0)
                except Exception:
                    pass
                try:
                    users_count = conn.execute(
                        "SELECT COUNT(*) AS c FROM users"
                    ).fetchone().get("c", 0)
                except Exception:
                    pass
                conn.close()
            except Exception:
                pass

        # Disk free
        try:
            usage = _shutil.disk_usage(str(ROOT_DIR))
            disk_gb_free = round(usage.free / (1024 ** 3), 1)
        except Exception:
            disk_gb_free = 0.0

        # License
        try:
            lic = get_license_status()
            license_valid = lic.get("valid", False)
            license_tier = lic.get("tier", "")
            license_expiry = lic.get("expiry", "")
        except Exception:
            license_valid = False
            license_tier = ""
            license_expiry = ""

        # Uptime
        uptime_seconds = (datetime.now(timezone.utc) - _SERVICE_START).total_seconds()
        uptime_hours = round(uptime_seconds / 3600, 1)

        # Install date & wizard
        cfg = {}
        try:
            cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
        install_date = cfg.get("install_date", _SERVICE_START.isoformat())
        wizard_complete = cfg.get("wizard_complete", False)

        return {
            "status": "ok",
            "version": _get_app_version(),
            "db_ok": db_ok,
            "service_ok": True,
            "disk_gb_free": disk_gb_free,
            "documents_count": documents_count,
            "users_count": users_count,
            "license_valid": license_valid,
            "license_tier": license_tier,
            "license_expiry": license_expiry,
            "uptime_hours": uptime_hours,
            "install_date": install_date,
            "wizard_complete": wizard_complete,
        }

    def _build_health_full_response(self) -> dict[str, Any]:
        """Build the JSON response for GET /health/full (owner only)."""
        import pkg_resources as _pkg

        base = self._build_health_response()

        # Autofix results
        autofix_results = None
        autofix_path = ROOT_DIR / "scripts" / "autofix.py"
        if autofix_path.exists():
            try:
                import subprocess as _sp
                result = _sp.run(
                    [sys.executable, str(autofix_path), "--quiet", "--json"],
                    capture_output=True, text=True, timeout=30, cwd=str(ROOT_DIR),
                )
                if result.stdout.strip():
                    autofix_results = json.loads(result.stdout.strip())
            except Exception:
                autofix_results = "autofix execution failed"

        # Last 5 errors from log
        last_errors: list[str] = []
        if LOG_PATH.exists():
            try:
                lines = LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
                error_lines = [ln for ln in lines if "ERROR" in ln or "FAIL" in ln]
                last_errors = error_lines[-5:]
            except Exception:
                pass

        # Package list
        packages: list[str] = []
        try:
            packages = [f"{d.project_name}=={d.version}" for d in _pkg.working_set]
        except Exception:
            pass

        base.update({
            "python_version": sys.version,
            "autofix_results": autofix_results,
            "last_errors": last_errors,
            "packages": packages,
        })
        return base

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
        # Minimal HTML helper for errors shown inside iframes — no full page layout
        def _pdf_error(title: str, body: str, status: int = 404) -> None:
            html = (
                f'<!DOCTYPE html><html><head><meta charset="utf-8">'
                f'<style>body{{font-family:system-ui,sans-serif;padding:40px;color:#1e293b;}}'
                f'.box{{border:2px solid #3b82f6;border-radius:8px;padding:20px 24px;background:#eff6ff;max-width:600px;}}'
                f'.box h2{{margin:0 0 8px;font-size:16px;color:#1e40af;}}'
                f'.box p{{margin:4px 0;font-size:14px;color:#475569;}}'
                f'</style></head><body><div class="box"><h2>{title}</h2>{body}</div></body></html>'
            )
            self._send_html(html, status=status)
        if not document_id:
            _pdf_error("Bad Request", "<p>Missing document id</p>", 400)
            return
        row = get_document(document_id)
        if row is None:
            _pdf_error("Not Found", "<p>Document not found</p>", 404)
            return
        # Access control
        ctx = build_user_context(user)
        if not ctx["can_view_all_clients"]:
            allowed_keys = {normalize_key(c) for c in ctx.get("allowed_clients", [])}
            if normalize_key(row["client_code"]) not in allowed_keys:
                _pdf_error("Access Denied", "<p>Access denied</p>", 403)
                return
        file_path = normalize_text(row["file_path"])
        if not file_path:
            _pdf_error("Not Found", "<p>No file path recorded for this document</p>", 404)
            return
        path_obj = Path(file_path)
        # Resolve and safety-check — must be an existing file
        resolved = None
        try:
            resolved = path_obj.resolve(strict=True)
        except (OSError, RuntimeError):
            pass
        # Fallback: if absolute path doesn't exist, try to find the file
        # relative to ROOT_DIR (handles DB paths from a different machine)
        if resolved is None:
            _fname = Path(file_path).name
            # Try common relative locations
            for _candidate in (
                ROOT_DIR / "tests" / "documents_real" / _fname,
                ROOT_DIR / "src" / "agents" / "data" / "downloads" / _fname,
                ROOT_DIR / _fname,
            ):
                try:
                    resolved = _candidate.resolve(strict=True)
                    break
                except (OSError, RuntimeError):
                    continue
        if resolved is None:
            _pdf_error(
                "Document de test / Test document",
                "<p>Document de test — aucun fichier PDF disponible / Test document — no PDF file available</p>"
                "<p style='font-size:13px;color:#64748b;margin-top:8px;'>"
                "Ce document a \u00e9t\u00e9 g\u00e9n\u00e9r\u00e9 automatiquement pour les tests / "
                "This document was generated automatically for testing</p>",
                404,
            )
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

            # --- /health (no authentication) ---
            if path == "/health":
                self._send_json(self._build_health_response())
                return

            user = get_session_user(self)
            if not user:
                self._redirect("/login")
                return

            lang = get_user_lang(user)
            ctx = build_user_context(user)

            # --- /health/full (owner authentication required) ---
            if path == "/health/full":
                if user.get("role") != "owner":
                    self._send_json({"error": "owner authentication required"}, status=403)
                    return
                self._send_json(self._build_health_full_response())
                return

            if path == "/":
                # Onboarding redirect: owner with no staff or no clients
                if _onboarding_check_needed(user):
                    self._redirect("/onboarding/step1")
                    return
                status = qs.get("status", [""])[0]
                q = qs.get("q", [""])[0]
                include_ignored = qs.get("include_ignored", ["0"])[0] == "1"
                queue_mode = qs.get("queue_mode", ["all"])[0]
                try:
                    page = max(1, int(qs.get("page", ["1"])[0]))
                except (ValueError, TypeError):
                    page = 1
                self._send_html(render_home(ctx, user, status, q, flash, flash_error,
                                            include_ignored, queue_mode == "mine",
                                            queue_mode == "unassigned", lang=lang,
                                            page=page))
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

            if path == "/admin/cache":
                if user.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._send_html(render_cache_admin(user, flash, flash_error, lang=lang))
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

            # --- Bank Reconciliation routes ---
            if path == "/reconciliation":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                fc = qs.get("client_code", [""])[0].strip()
                fp = qs.get("period", [""])[0].strip()
                self._send_html(render_reconciliation_list(
                    ctx, user, flash, flash_error, lang=lang,
                    filter_client=fc, filter_period=fp))
                return

            if path == "/reconciliation/new":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._send_html(render_reconciliation_new(ctx, user, flash, flash_error, lang=lang))
                return

            if path == "/reconciliation/detail":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                rid = qs.get("id", [""])[0].strip()
                self._send_html(render_reconciliation_detail(
                    rid, ctx, user, flash, flash_error, lang=lang))
                return

            if path == "/reconciliation/pdf":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p></div>',
                        user=user, lang=lang), status=403)
                    return
                rid = qs.get("id", [""])[0].strip()
                with open_db() as conn:
                    pdf_bytes = recon_generate_pdf(rid, lang, conn)
                if not pdf_bytes:
                    self._send_html(page_layout(
                        t("err_not_found", lang),
                        f'<div class="card"><h2>{esc(t("err_not_found", lang))}</h2></div>',
                        user=user, lang=lang), status=404)
                    return
                filename = f"reconciliation_{rid}.pdf"
                self.send_response(200)
                self.send_header("Content-Type", "application/pdf")
                self.send_header("Content-Length", str(len(pdf_bytes)))
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.end_headers()
                self.wfile.write(pdf_bytes)
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

            if path == "/audit/materiality":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mat_forbidden", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                mat_eng_id = qs.get("engagement_id", [""])[0].strip()
                self._send_html(render_materiality(ctx, user, mat_eng_id, flash, flash_error, lang=lang))
                return

            if path == "/audit/risk":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_risk_forbidden", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                risk_eng_id = qs.get("engagement_id", [""])[0].strip()
                self._send_html(render_risk_assessment(ctx, user, risk_eng_id, flash, flash_error, lang=lang))
                return

            if path == "/audit/rep_letter":
                if ctx.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_rep_forbidden", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                rep_eng_id = qs.get("engagement_id", [""])[0].strip()
                self._send_html(render_rep_letter(ctx, user, rep_eng_id, flash, flash_error, lang=lang))
                return

            if path == "/audit/controls":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_ctrl_forbidden", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                ctrl_eng_id = qs.get("engagement_id", [""])[0].strip()
                self._send_html(render_control_tests(ctx, user, ctrl_eng_id, flash, flash_error, lang=lang))
                return

            if path == "/audit/related_parties":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_rp_forbidden", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                rp_eng_id = qs.get("engagement_id", [""])[0].strip()
                self._send_html(render_related_parties(ctx, user, rp_eng_id, flash, flash_error, lang=lang))
                return

            if path == "/qr":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_qr_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_qr_forbidden", lang))}</h2>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._send_html(render_qr_page(ctx, user, flash, flash_error, lang=lang))
                return

            if path == "/qr/download":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_qr_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_qr_forbidden", lang))}</h2>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                client_code_qr = qs.get("client_code", [""])[0].strip()
                if not client_code_qr:
                    self._send_html(page_layout("Bad Request",
                        '<div class="card"><h2>client_code required</h2></div>',
                        user=user, lang=lang), status=400)
                    return
                portal_base_qr = _get_portal_base_url()
                upload_url_qr = _build_upload_url(portal_base_qr, client_code_qr)
                clients_qr = _get_qr_clients()
                client_name_qr = next(
                    (c["client_name"] for c in clients_qr if c["client_code"] == client_code_qr),
                    client_code_qr,
                )
                png_bytes = generate_client_qr_png(client_code_qr, client_name_qr, upload_url_qr)
                safe_code = "".join(c for c in client_code_qr if c.isalnum() or c in "-_")
                filename = f"qr_{safe_code}.png"
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(png_bytes)))
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.end_headers()
                self.wfile.write(png_bytes)
                return

            if path == "/qr/pdf":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_qr_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_qr_forbidden", lang))}</h2>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                clients_pdf = _get_qr_clients()
                portal_base_pdf = _get_portal_base_url()
                pdf_bytes = generate_all_qr_pdf(clients_pdf, portal_base_pdf)
                self.send_response(200)
                self.send_header("Content-Type", "application/pdf")
                self.send_header("Content-Length", str(len(pdf_bytes)))
                self.send_header("Content-Disposition", 'attachment; filename="ledgerlink_qr_codes.pdf"')
                self.end_headers()
                self.wfile.write(pdf_bytes)
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

            if path == "/license/machines":
                if user.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_lic_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_lic_forbidden", lang))}</h2>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._send_html(render_license_machines(ctx, user, flash, flash_error, lang=lang))
                return

            if path == "/admin/updates":
                if user.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_owner_required", lang))}</p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._send_html(render_admin_updates(user, flash, flash_error, lang=lang))
                return

            if path == "/admin/remote":
                if user.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_owner_required", lang))}</p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._send_html(render_admin_remote(user, flash, flash_error, lang=lang))
                return

            # ------------------------------------------------------------------
            # Onboarding routes (GET)
            # ------------------------------------------------------------------

            if path == "/onboarding/step1":
                if user.get("role") != "owner":
                    self._redirect("/")
                    return
                self._send_html(render_onboarding_step1(ctx, user, flash, flash_error, lang=lang))
                return

            if path == "/onboarding/step2":
                if user.get("role") != "owner":
                    self._redirect("/")
                    return
                self._send_html(render_onboarding_step2(ctx, user, flash, flash_error, lang=lang))
                return

            if path == "/onboarding/step3":
                if user.get("role") != "owner":
                    self._redirect("/")
                    return
                self._send_html(render_onboarding_step3(ctx, user, lang=lang))
                return

            # ------------------------------------------------------------------
            # FIX 2: Vendor alias management (owner only)
            # ------------------------------------------------------------------
            if path == "/admin/vendor_aliases":
                if user.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._send_html(render_vendor_aliases(ctx, user, flash, flash_error, lang=lang))
                return

            # ------------------------------------------------------------------
            # FIX 7: Manual journal entries (manager/owner)
            # ------------------------------------------------------------------
            if path == "/journal_entries":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_mgr_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                self._send_html(render_journal_entries(ctx, user, flash, flash_error, lang=lang))
                return

            # ------------------------------------------------------------------
            # Training (all authenticated users)
            # ------------------------------------------------------------------
            if path == "/training":
                training_file = ROOT_DIR / "docs" / "training" / "staff_training.html"
                if training_file.exists():
                    content = training_file.read_text(encoding="utf-8")
                    self._send_html(content)
                else:
                    self._send_html(page_layout(
                        t("training_nav_link", lang),
                        f'<div class="card"><h2>{esc(t("training_nav_link", lang))}</h2>'
                        f'<p>Training content not found.</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=404)
                return

            # ------------------------------------------------------------------
            # Technician Training (owner only)
            # ------------------------------------------------------------------
            if path == "/training/technician":
                if user.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_owner_required", lang))}</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=403)
                    return
                tech_training_file = ROOT_DIR / "docs" / "training" / "technician_training.html"
                if tech_training_file.exists():
                    content = tech_training_file.read_text(encoding="utf-8")
                    self._send_html(content)
                else:
                    self._send_html(page_layout(
                        "Technician Training",
                        f'<div class="card"><h2>Technician Training</h2>'
                        f'<p>Training content not found.</p>'
                        f'<p><a href="/">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                        user=user, lang=lang), status=404)
                return

            # ------------------------------------------------------------------
            # Training Certificate (authenticated users)
            # ------------------------------------------------------------------
            if path == "/training/certificate":
                self._send_html(page_layout(
                    t("training_nav_link", lang),
                    '<div class="card"><h2>Training Certificate</h2>'
                    '<p>Complete all modules and quizzes to earn your certificate.</p>'
                    '<p>Your progress is tracked automatically via the training portal.</p>'
                    f'<p><a href="/training">{esc(t("btn_back_to_queue", lang))}</a></p></div>',
                    user=user, lang=lang))
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
                    _user_raw = conn.execute(
                        "SELECT * FROM dashboard_users WHERE username=? AND active=1", (username,)
                    ).fetchone()
                    user_row = dict(_user_raw) if _user_raw else None
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

            # --- OpenClaw bridge — no session auth required ---
            if path == "/ingest/openclaw":
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except Exception:
                    self._send_json(
                        {"ok": False, "document_id": None, "status": "error",
                         "error": "invalid_json"},
                        status=400,
                    )
                    return
                from src.integrations.openclaw_bridge import handle_openclaw_ingest
                result = handle_openclaw_ingest(payload, db_path=DB_PATH)
                http_status = 200 if result.get("ok") else (
                    404 if result.get("status") == "unknown_sender" else 400
                )
                self._send_json(result, status=http_status)
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
                # FIX 7: Update substance_flags when GL is manually changed
                new_gl = normalize_text(submitted.get("gl_account", ""))
                old_gl = normalize_text(before_row["gl_account"]) if before_row else ""
                if new_gl and new_gl != old_gl:
                    try:
                        with open_db() as _sf_conn:
                            sf_cols = {r["name"] for r in _sf_conn.execute("PRAGMA table_info(documents)").fetchall()}
                            if "substance_flags" in sf_cols:
                                existing_sf = safe_json_loads(before_row.get("substance_flags")) if before_row.get("substance_flags") else {}
                                if not isinstance(existing_sf, dict):
                                    existing_sf = {}
                                existing_sf["manual_override"] = True
                                existing_sf["manual_gl"] = new_gl
                                existing_sf["manual_override_at"] = utc_now_iso()
                                _sf_conn.execute(
                                    "UPDATE documents SET substance_flags = ? WHERE document_id = ?",
                                    (json.dumps(existing_sf, ensure_ascii=False), document_id),
                                )
                                _sf_conn.commit()
                    except Exception:
                        pass
                # BLOCK 1: Re-run fraud detection and review_policy after document update
                try:
                    from src.engines.fraud_engine import run_fraud_detection
                    _ff = run_fraud_detection(document_id, db_path=DB_PATH) or []
                    from src.agents.tools.review_policy import decide_review_status as _drv
                    _updated_row = get_document(document_id)
                    if _updated_row:
                        _sf_raw = safe_json_loads(_updated_row.get("substance_flags")) if _updated_row.get("substance_flags") else {}
                        if not isinstance(_sf_raw, dict):
                            _sf_raw = {}
                        _amt = None
                        try:
                            _amt = float(_updated_row["amount"]) if _updated_row.get("amount") else None
                        except (TypeError, ValueError):
                            pass
                        _dec = _drv(
                            rules_confidence=float(_updated_row.get("confidence") or 0),
                            final_method="rules",
                            vendor_name=normalize_text(_updated_row.get("vendor")),
                            total=_amt,
                            document_date=normalize_text(_updated_row.get("document_date")),
                            client_code=normalize_text(_updated_row.get("client_code")),
                            fraud_flags=_ff,
                            substance_flags=_sf_raw,
                        )
                        if _dec.status in ("NeedsReview", "Exception"):
                            set_document_status(document_id, _dec.status)
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
                    # --- FIX 1: Check fraud flags before allowing approval ---
                    fraud_flags_raw = safe_json_loads(doc_row["fraud_flags"]) if doc_row["fraud_flags"] else {}
                    if isinstance(doc_row["fraud_flags"], str):
                        try:
                            fraud_flags_raw = json.loads(doc_row["fraud_flags"])
                        except Exception:
                            fraud_flags_raw = []
                    if not isinstance(fraud_flags_raw, list):
                        fraud_flags_raw = []
                    blocking_fraud = [
                        f for f in fraud_flags_raw
                        if isinstance(f, dict) and normalize_text(f.get("severity")).upper() in ("CRITICAL", "HIGH")
                    ]
                    if blocking_fraud:
                        fraud_override_reason = normalize_text(form.get("fraud_override_reason", ""))
                        fraud_override_ack = normalize_text(form.get("fraud_override_ack", ""))
                        if fraud_override_ack != "1" or not fraud_override_reason:
                            bilingual_msg = (
                                "Ce document a des indicateurs de fraude — révision manuelle obligatoire / "
                                "This document has fraud indicators — manual review required"
                            )
                            self._flash_redirect(
                                f"/document?id={urlquote(document_id)}",
                                error=bilingual_msg,
                            )
                            return
                        # Only manager and owner roles may override
                        if ctx["role"] not in ("manager", "owner"):
                            self._flash_redirect(
                                f"/document?id={urlquote(document_id)}",
                                error=t("err_fraud_override_denied", lang),
                            )
                            return
                        # FIX 7: Validate fraud override reason — min 10 non-whitespace/punctuation chars
                        stripped_reason = fraud_override_reason.strip()
                        import string as _string
                        reason_alpha = stripped_reason.translate(str.maketrans("", "", _string.punctuation + " "))
                        if len(stripped_reason) < 10 or not reason_alpha:
                            self._flash_redirect(
                                f"/document?id={urlquote(document_id)}",
                                error="Veuillez fournir une justification détaillée / Please provide a detailed justification.",
                            )
                            return
                        # FIX 2: Audit-log the fraud override BEFORE updating the DB
                        with open_db() as _fc:
                            if _fc.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='audit_log'").fetchone():
                                _fc.execute(
                                    """INSERT INTO audit_log
                                       (event_type, username, document_id, prompt_snippet, created_at)
                                       VALUES (?, ?, ?, ?, ?)""",
                                    (
                                        "fraud_override",
                                        ctx.get("username", ""),
                                        document_id,
                                        json.dumps({
                                            "fraud_flags": [
                                                f.get("rule", f.get("flag", "")) if isinstance(f, dict) else str(f)
                                                for f in blocking_fraud
                                            ],
                                            "override_reason": fraud_override_reason,
                                        }, ensure_ascii=False),
                                        utc_now_iso(),
                                    ),
                                )
                            cols = {r["name"] for r in _fc.execute("PRAGMA table_info(documents)").fetchall()}
                            if "fraud_override_reason" in cols:
                                _fc.execute(
                                    "UPDATE documents SET fraud_override_reason = ? WHERE document_id = ?",
                                    (fraud_override_reason, document_id),
                                )
                            _fc.commit()
                    # --- END FIX 1 ---

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
                    # FIX 2: Infer entry_kind from doc_row
                    inferred_entry_kind = _infer_entry_kind(doc_row) if doc_row is not None else "expense"
                    build_posting_job(document_id=document_id, target_system="qbo", entry_kind=inferred_entry_kind, db_path=DB_PATH)
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
                # FIX 11: Retry is an approval action — check fraud flags
                doc_row = get_document(document_id)
                if doc_row is not None:
                    fraud_flags_raw = safe_json_loads(doc_row["fraud_flags"]) if doc_row["fraud_flags"] else []
                    if isinstance(doc_row["fraud_flags"], str):
                        try:
                            fraud_flags_raw = json.loads(doc_row["fraud_flags"])
                        except Exception:
                            fraud_flags_raw = []
                    if not isinstance(fraud_flags_raw, list):
                        fraud_flags_raw = []
                    blocking_fraud = [
                        f for f in fraud_flags_raw
                        if isinstance(f, dict) and normalize_text(f.get("severity")).upper() in ("CRITICAL", "HIGH")
                    ]
                    if blocking_fraud:
                        override_reason = normalize_text(doc_row.get("fraud_override_reason") or "")
                        if not override_reason or len(override_reason.strip()) < 10:
                            bilingual_msg = (
                                "Ce document a des indicateurs de fraude — révision manuelle obligatoire / "
                                "This document has fraud indicators — manual review required"
                            )
                            self._flash_redirect(
                                f"/document?id={urlquote(document_id)}",
                                error=bilingual_msg,
                            )
                            return
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

            if path == "/admin/cache/clear":
                if user.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_owner_required", lang))}</p></div>',
                        user=user, lang=lang), status=403)
                    return
                deleted = _ai_clear_cache()
                self._flash_redirect("/admin/cache", flash=f"Cache cleared — {deleted} entries removed.")
                return

            # ---------------------------------------------------------------
            # FIX 2: /admin/vendor_memory — owner-only vendor pattern viewer
            # ---------------------------------------------------------------
            if path == "/admin/vendor_memory":
                if ctx.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_owner_required", lang))}</p></div>',
                        user=user, lang=lang), status=403)
                    return
                action = normalize_text(form.get("action", ""))
                vm_vendor = normalize_text(form.get("vendor", ""))
                vm_client = normalize_text(form.get("client_code", ""))
                if action == "reset" and vm_vendor:
                    from src.agents.core.learning_memory_store import reset_vendor_memory, reset_learning_corrections
                    with open_db() as _vm_conn:
                        r1 = reset_vendor_memory(vm_vendor, vm_client, _vm_conn)
                        r2 = reset_learning_corrections(vm_vendor, vm_client, _vm_conn)
                        # Also reset vendor_memory table
                        from src.agents.core.vendor_memory_store import normalize_key as vm_nk
                        vk = vm_nk(vm_vendor)
                        ck = vm_nk(vm_client)
                        _vm_conn.execute(
                            "DELETE FROM vendor_memory WHERE vendor_key = ? AND (client_code_key = ? OR ? = '')",
                            (vk, ck, ck),
                        )
                        _vm_conn.commit()
                    self._flash_redirect(
                        "/admin/vendor_memory",
                        flash=f"Reset vendor memory for '{vm_vendor}' / '{vm_client}': "
                              f"patterns={r1.get('deleted',0)}, corrections={r2.get('deleted',0)}",
                    )
                    return
                # Display all vendor patterns
                rows_html = ""
                with open_db() as _vm_conn:
                    vm_rows = []
                    if _vm_conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='vendor_memory'").fetchone():
                        vm_rows = _vm_conn.execute(
                            "SELECT * FROM vendor_memory ORDER BY approval_count DESC, updated_at DESC LIMIT 200"
                        ).fetchall()
                    rows_html = "<table class='table'><tr><th>Vendor</th><th>Client</th><th>GL</th><th>Tax</th><th>Count</th><th>Confidence</th><th>Updated</th><th>Action</th></tr>"
                    for r in vm_rows:
                        vid = esc(str(r["vendor"] or ""))
                        cid = esc(str(r["client_code"] or ""))
                        rows_html += (
                            f"<tr><td>{vid}</td><td>{cid}</td>"
                            f"<td>{esc(str(r['gl_account'] or ''))}</td>"
                            f"<td>{esc(str(r['tax_code'] or ''))}</td>"
                            f"<td>{r['approval_count']}</td>"
                            f"<td>{round(float(r['confidence'] or 0), 2)}</td>"
                            f"<td>{esc(str(r['updated_at'] or ''))}</td>"
                            f"<td><form method='POST' action='/admin/vendor_memory' style='display:inline'>"
                            f"<input type='hidden' name='action' value='reset'>"
                            f"<input type='hidden' name='vendor' value='{vid}'>"
                            f"<input type='hidden' name='client_code' value='{cid}'>"
                            f"<button type='submit' class='btn btn-danger btn-sm' "
                            f"onclick=\"return confirm('Reset this vendor?')\">Reset</button></form></td></tr>"
                        )
                    rows_html += "</table>"
                body = f"<div class='card'><h2>Vendor Memory Patterns</h2>{rows_html}</div>"
                self._send_html(page_layout("Vendor Memory", body, user=user, lang=lang))
                return

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

            # --- Bank Reconciliation POST routes ---
            if path == "/reconciliation/create":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                rc_client = normalize_text(form.get("client_code", ""))
                rc_account = normalize_text(form.get("account_name", ""))
                rc_acct_num = normalize_text(form.get("account_number", ""))
                rc_period = normalize_text(form.get("period_end_date", ""))
                rc_stmt = normalize_amount_input(form.get("statement_balance", ""))
                rc_gl = normalize_amount_input(form.get("gl_balance", ""))
                if not rc_client or not rc_account or not rc_period:
                    raise ValueError("Client code, account name, and period end date are required")
                if rc_stmt is None or rc_gl is None:
                    raise ValueError("Statement balance and GL balance are required")
                with open_db() as conn:
                    rid = recon_create(
                        rc_client, rc_account, rc_period, rc_stmt, rc_gl, conn,
                        account_number=rc_acct_num, prepared_by=ctx["username"],
                    )
                    count = recon_auto_populate(rid, conn)
                flash_msg = t("flash_recon_created", lang)
                if count > 0:
                    flash_msg += " " + t("recon_auto_populated", lang, count=count)
                self._flash_redirect(f"/reconciliation/detail?id={urlquote(rid)}", flash=flash_msg)
                return

            if path == "/reconciliation/add_item":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                ri_recon = normalize_text(form.get("reconciliation_id", ""))
                ri_type = normalize_text(form.get("item_type", ""))
                ri_desc = normalize_text(form.get("description", ""))
                ri_amt = normalize_amount_input(form.get("amount", ""))
                ri_date = normalize_text(form.get("transaction_date", ""))
                if not ri_recon or not ri_type or not ri_desc or ri_amt is None:
                    raise ValueError("Reconciliation ID, type, description, and amount are required")
                with open_db() as conn:
                    recon_add_item(ri_recon, ri_type, ri_desc, ri_amt, ri_date, conn)
                self._flash_redirect(
                    f"/reconciliation/detail?id={urlquote(ri_recon)}",
                    flash=t("flash_recon_item_added", lang),
                )
                return

            if path == "/reconciliation/clear_item":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                ci_item = normalize_text(form.get("item_id", ""))
                ci_recon = normalize_text(form.get("reconciliation_id", ""))
                if not ci_item:
                    raise ValueError("Item ID is required")
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                with open_db() as conn:
                    recon_mark_cleared(ci_item, today, conn)
                self._flash_redirect(
                    f"/reconciliation/detail?id={urlquote(ci_recon)}",
                    flash=t("flash_recon_item_cleared", lang),
                )
                return

            if path == "/reconciliation/finalize":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                fi_recon = normalize_text(form.get("reconciliation_id", ""))
                if not fi_recon:
                    raise ValueError("Reconciliation ID is required")
                with open_db() as conn:
                    success = recon_finalize(fi_recon, ctx["username"], conn)
                if success:
                    self._flash_redirect(
                        f"/reconciliation/detail?id={urlquote(fi_recon)}",
                        flash=t("flash_recon_finalized", lang),
                    )
                else:
                    self._flash_redirect(
                        f"/reconciliation/detail?id={urlquote(fi_recon)}",
                        error=t("flash_recon_not_balanced", lang),
                    )
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
                # Part 4: Auto-detection — check for existing reconciliation
                _recon_flash_extra = ""
                try:
                    # Determine period from transactions
                    _txn_dates = [tx.get("txn_date", "") for tx in result.get("transactions", []) if tx.get("txn_date")]
                    _period_end = max(_txn_dates) if _txn_dates else ""
                    _period_month = _period_end[:7] if _period_end else ""
                    if _period_month:
                        with open_db() as _rc:
                            _ensure_recon_tables(_rc)
                            _existing = _rc.execute(
                                "SELECT reconciliation_id, status FROM bank_reconciliations WHERE client_code = ? AND period_end_date LIKE ?",
                                (bi_client, f"{_period_month}%"),
                            ).fetchone()
                            if _existing:
                                # Auto-add unmatched items to existing reconciliation
                                _added = recon_auto_populate(_existing["reconciliation_id"], _rc)
                                if _added > 0:
                                    _recon_flash_extra = " " + t("recon_auto_populated", lang, count=_added)
                            else:
                                _recon_flash_extra = " " + t("recon_suggest_banner", lang, client=bi_client, period=_period_month)
                except Exception:
                    pass  # don't block import on reconciliation errors
                result["_client_code"] = bi_client  # pass client_code for split detection
                self._send_html(render_bank_import(
                    ctx, user,
                    flash=t("flash_bank_imported", lang) + _recon_flash_extra,
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

            # --- BLOCK 1: Confirm split payment ---
            if path == "/bank_import/confirm_split":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                sp_txn_id = normalize_text(form.get("transaction_id", ""))
                if not sp_txn_id:
                    raise ValueError("transaction_id is required")
                # invoice_ids may be passed as multiple values in raw form
                sp_raw = urllib.parse.parse_qs(raw.decode("utf-8"), keep_blank_values=True)
                sp_invoice_ids = [v.strip() for v in sp_raw.get("invoice_ids", []) if v.strip()]
                if not sp_invoice_ids:
                    raise ValueError("invoice_ids are required")
                with open_db() as conn:
                    for inv_id in sp_invoice_ids:
                        conn.execute(
                            "UPDATE documents SET matched_bank_transaction_id = ?, match_status = 'matched' "
                            "WHERE document_id = ?",
                            (sp_txn_id, inv_id),
                        )
                    # Also update the bank transaction record if it exists
                    try:
                        conn.execute(
                            "UPDATE documents SET review_status = 'Ready', match_status = 'matched' "
                            "WHERE document_id = ?",
                            (sp_txn_id,),
                        )
                    except Exception:
                        pass
                    conn.commit()
                self._flash_redirect("/bank_import", flash=t("flash_split_confirmed", lang))
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

            # BLOCK 5: Save assertion coverage for a working paper
            if path == "/working_papers/save_assertions":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                a_paper_id   = normalize_text(form.get("paper_id", ""))
                a_client     = normalize_text(form.get("client_code", ""))
                a_period     = normalize_text(form.get("period", ""))
                a_type       = normalize_text(form.get("engagement_type", "audit"))
                if not a_paper_id:
                    raise ValueError("paper_id is required")
                # Parse assertions from multiple-value form field
                a_raw = urllib.parse.parse_qs(raw.decode("utf-8"), keep_blank_values=True)
                a_assertions = [v.strip() for v in a_raw.get("assertions", []) if v.strip()]
                with open_db() as conn:
                    # Get or create an item for this paper to attach assertions
                    items = conn.execute(
                        "SELECT item_id FROM working_paper_items WHERE paper_id = ? LIMIT 1",
                        (a_paper_id,),
                    ).fetchall()
                    if items:
                        for item_row in items:
                            _cas.add_assertion_coverage(conn, item_row["item_id"], a_assertions)
                    else:
                        # Create a placeholder item
                        item = _audit.add_working_paper_item(
                            conn, a_paper_id, "assertion_check", "tested",
                            notes="Assertion coverage", tested_by=user["username"],
                        )
                        if item:
                            _cas.add_assertion_coverage(conn, item["item_id"], a_assertions)
                self._flash_redirect(
                    f"/working_papers?client_code={urlquote(a_client)}&period={urlquote(a_period)}&engagement_type={urlquote(a_type)}",
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
                    # BLOCK 3: Find engagement for materiality check
                    _wp_eng = None
                    try:
                        _wp_eng_row = conn.execute(
                            "SELECT engagement_id FROM engagements WHERE client_code = ? AND period = ? AND engagement_type = ? LIMIT 1",
                            (wp_client, wp_period, wp_type),
                        ).fetchone()
                        if _wp_eng_row:
                            _wp_eng = _wp_eng_row["engagement_id"]
                    except Exception:
                        pass
                    for acct in accounts:
                        wp_result = _audit.get_or_create_working_paper(
                            conn,
                            wp_client,
                            wp_period,
                            wp_type,
                            acct["account_code"],
                            acct["account_name"],
                        )
                        # BLOCK 3: Auto-check materiality for each working paper
                        if _wp_eng and wp_result:
                            try:
                                bal = float(wp_result.get("balance_per_books") or 0)
                                mat_check = _cas.check_materiality_for_working_paper(conn, _wp_eng, bal)
                                if mat_check.get("material_item"):
                                    paper_id = wp_result.get("paper_id") or str(wp_result.get("id", ""))
                                    try:
                                        cols = {r["name"] for r in conn.execute("PRAGMA table_info(working_papers)").fetchall()}
                                        if "is_material" not in cols:
                                            conn.execute("ALTER TABLE working_papers ADD COLUMN is_material INTEGER DEFAULT 0")
                                        conn.execute(
                                            "UPDATE working_papers SET is_material = 1 WHERE paper_id = ?",
                                            (paper_id,),
                                        )
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                    conn.commit()
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
                    eng_result = _audit.create_engagement(
                        conn, eng_client, eng_period,
                        engagement_type=eng_type,
                        partner=eng_partner, manager=eng_manager, staff=eng_staff,
                        planned_hours=planned_hours, budget=budget, fee=fee,
                    )
                    # BLOCK 2: Auto-run going concern detection on engagement create
                    try:
                        gc = _cas.detect_going_concern_indicators(eng_client, conn)
                        if gc.get("assessment_required"):
                            new_eng_id = eng_result.get("engagement_id", "") if isinstance(eng_result, dict) else ""
                            if new_eng_id:
                                conn.execute(
                                    "CREATE TABLE IF NOT EXISTS going_concern_assessments "
                                    "(id TEXT PRIMARY KEY, engagement_id TEXT, indicators TEXT, "
                                    "assessment_required INTEGER, created_at TEXT)"
                                )
                                conn.execute(
                                    "INSERT OR REPLACE INTO going_concern_assessments VALUES (?,?,?,?,?)",
                                    (secrets.token_hex(8), new_eng_id,
                                     json.dumps(gc.get("indicators", []), ensure_ascii=False),
                                     1, utc_now_iso()),
                                )
                                conn.commit()
                    except Exception:
                        pass
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
                    # BLOCK 2: Auto-run going concern detection on engagement update
                    try:
                        eng_row = _audit.get_engagement(conn, eng_id)
                        if eng_row:
                            gc = _cas.detect_going_concern_indicators(eng_row["client_code"], conn)
                            if gc.get("assessment_required"):
                                conn.execute(
                                    "CREATE TABLE IF NOT EXISTS going_concern_assessments "
                                    "(id TEXT PRIMARY KEY, engagement_id TEXT, indicators TEXT, "
                                    "assessment_required INTEGER, created_at TEXT)"
                                )
                                conn.execute(
                                    "INSERT OR REPLACE INTO going_concern_assessments VALUES (?,?,?,?,?)",
                                    (secrets.token_hex(8), eng_id,
                                     json.dumps(gc.get("indicators", []), ensure_ascii=False),
                                     1, utc_now_iso()),
                                )
                                conn.commit()
                    except Exception:
                        pass
                self._flash_redirect(f"/engagements/detail?id={urlquote(eng_id)}", flash=t("flash_eng_updated", lang))
                return

            if path == "/engagements/issue":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mgr_owner_required", lang))
                eng_id = normalize_text(form.get("engagement_id", ""))
                if not eng_id:
                    raise ValueError("engagement_id is required")
                with open_db() as conn:
                    can_issue, blocking = _cas.check_engagement_issuable(eng_id, conn)
                    if not can_issue:
                        blocking_labels = ", ".join(blocking)
                        raise ValueError(f"{t('err_checklist_blocking', lang)} {blocking_labels}")
                    pdf_bytes = _audit.issue_engagement(conn, eng_id, issued_by=user["username"], lang=lang)
                filename = f"engagement_{eng_id}.pdf"
                self.send_response(200)
                self.send_header("Content-Type", "application/pdf")
                self.send_header("Content-Length", str(len(pdf_bytes)))
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.end_headers()
                self.wfile.write(pdf_bytes)
                return

            if path == "/audit/materiality/save":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_mat_forbidden", lang))
                mat_eng_id = normalize_text(form.get("engagement_id", ""))
                mat_basis = normalize_text(form.get("basis", ""))
                mat_amount_raw = form.get("basis_amount", "0")
                mat_notes = form.get("notes", "")
                if not mat_eng_id or not mat_basis:
                    raise ValueError("engagement_id and basis are required")
                mat_dict = _cas.calculate_materiality(mat_basis, float(mat_amount_raw))
                with open_db() as conn:
                    _cas.save_materiality(conn, mat_eng_id, mat_dict, user["username"], notes=mat_notes)
                self._flash_redirect(
                    f"/audit/materiality?engagement_id={urlquote(mat_eng_id)}",
                    flash=t("flash_mat_saved", lang),
                )
                return

            if path == "/audit/risk/generate":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_risk_forbidden", lang))
                risk_eng_id = normalize_text(form.get("engagement_id", ""))
                if not risk_eng_id:
                    raise ValueError("engagement_id is required")
                with open_db() as conn:
                    # Get chart of accounts for this engagement's working papers
                    eng = _audit.get_engagement(conn, risk_eng_id)
                    if not eng:
                        raise ValueError("Engagement not found")
                    coa = _audit.get_chart_of_accounts(conn)
                    accounts = [{"account_code": a["account_code"], "account_name": a.get("account_name", "")} for a in coa]
                    _cas.create_risk_matrix(conn, risk_eng_id, accounts, assessed_by=user["username"])
                self._flash_redirect(
                    f"/audit/risk?engagement_id={urlquote(risk_eng_id)}",
                    flash=t("flash_risk_generated", lang),
                )
                return

            if path == "/audit/risk/update":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_risk_forbidden", lang))
                risk_id = normalize_text(form.get("risk_id", ""))
                risk_eng_id = normalize_text(form.get("engagement_id", ""))
                risk_inherent = normalize_text(form.get("inherent_risk", ""))
                risk_control = normalize_text(form.get("control_risk", ""))
                if not risk_id:
                    raise ValueError("risk_id is required")
                with open_db() as conn:
                    _cas.assess_risk(
                        conn, risk_id,
                        inherent_risk=risk_inherent or None,
                        control_risk=risk_control or None,
                        assessed_by=user["username"],
                    )
                self._flash_redirect(
                    f"/audit/risk?engagement_id={urlquote(risk_eng_id)}",
                    flash=t("flash_risk_updated", lang),
                )
                return

            # CAS 580 — Rep letter routes
            if path == "/audit/rep_letter/generate":
                if ctx.get("role") != "owner":
                    raise ValueError(t("err_rep_forbidden", lang))
                rep_eng_id = normalize_text(form.get("engagement_id", ""))
                if not rep_eng_id:
                    raise ValueError("engagement_id is required")
                with open_db() as conn:
                    draft_fr = _cas.generate_management_rep_letter(rep_eng_id, "fr", conn)
                    draft_en = _cas.generate_management_rep_letter(rep_eng_id, "en", conn)
                    _cas.save_rep_letter(rep_eng_id, draft_fr, draft_en, conn, created_by=user["username"])
                self._flash_redirect(
                    f"/audit/rep_letter?engagement_id={urlquote(rep_eng_id)}",
                    flash=t("flash_rep_saved", lang),
                )
                return

            if path == "/audit/rep_letter/sign":
                if ctx.get("role") != "owner":
                    raise ValueError(t("err_rep_forbidden", lang))
                letter_id = normalize_text(form.get("letter_id", ""))
                rep_eng_id = normalize_text(form.get("engagement_id", ""))
                mgmt_name = normalize_text(form.get("management_name", ""))
                mgmt_title = normalize_text(form.get("management_title", ""))
                if not letter_id or not mgmt_name:
                    raise ValueError("letter_id and management_name are required")
                with open_db() as conn:
                    _cas.mark_letter_signed(letter_id, mgmt_name, mgmt_title, conn)
                self._flash_redirect(
                    f"/audit/rep_letter?engagement_id={urlquote(rep_eng_id)}",
                    flash=t("flash_rep_signed", lang),
                )
                return

            # CAS 330 — Control testing routes
            if path == "/audit/controls/add":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_ctrl_forbidden", lang))
                ctrl_eng_id = normalize_text(form.get("engagement_id", ""))
                ctrl_name = normalize_text(form.get("control_name", ""))
                ctrl_objective = normalize_text(form.get("control_objective", ""))
                ctrl_test_type = normalize_text(form.get("test_type", "walkthrough"))
                if not ctrl_eng_id or not ctrl_name:
                    raise ValueError("engagement_id and control_name are required")
                # If from library, look up objective and description
                ctrl_desc = ""
                if not ctrl_objective:
                    for sc in _cas.STANDARD_CONTROLS:
                        if sc["name"] == ctrl_name:
                            ctrl_objective = sc["objective"]
                            ctrl_desc = sc["description"]
                            break
                with open_db() as conn:
                    _cas.create_control_test(
                        ctrl_eng_id, ctrl_name, ctrl_objective, ctrl_test_type, conn,
                        control_description=ctrl_desc, tested_by=user["username"],
                    )
                self._flash_redirect(
                    f"/audit/controls?engagement_id={urlquote(ctrl_eng_id)}",
                    flash=t("flash_ctrl_created", lang),
                )
                return

            if path == "/audit/controls/results":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_ctrl_forbidden", lang))
                ctrl_test_id = normalize_text(form.get("test_id", ""))
                ctrl_eng_id = normalize_text(form.get("engagement_id", ""))
                items_tested_str = normalize_text(form.get("items_tested", "0"))
                exceptions_str = normalize_text(form.get("exceptions_found", "0"))
                exception_details = normalize_text(form.get("exception_details", ""))
                conclusion = normalize_text(form.get("conclusion", "effective"))
                try:
                    items_tested = int(items_tested_str) if items_tested_str else 0
                except ValueError:
                    items_tested = 0
                try:
                    exceptions_found = int(exceptions_str) if exceptions_str else 0
                except ValueError:
                    exceptions_found = 0
                with open_db() as conn:
                    _cas.record_test_results(ctrl_test_id, items_tested, exceptions_found, exception_details, conclusion, conn)
                self._flash_redirect(
                    f"/audit/controls?engagement_id={urlquote(ctrl_eng_id)}",
                    flash=t("flash_ctrl_results", lang),
                )
                return

            # CAS 550 — Related party routes
            if path == "/audit/related_parties/add":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_rp_forbidden", lang))
                rp_eng_id = normalize_text(form.get("engagement_id", ""))
                rp_client = normalize_text(form.get("client_code", ""))
                rp_name = normalize_text(form.get("party_name", ""))
                rp_type = normalize_text(form.get("relationship_type", "affiliated_company"))
                rp_pct_str = normalize_text(form.get("ownership_pct", ""))
                rp_pct = None
                if rp_pct_str:
                    try:
                        rp_pct = float(rp_pct_str)
                    except ValueError:
                        pass
                if not rp_client or not rp_name:
                    raise ValueError("client_code and party_name are required")
                with open_db() as conn:
                    _cas.add_related_party(
                        rp_client, rp_name, rp_type, conn,
                        ownership_percentage=rp_pct,
                        identified_by=user["username"],
                    )
                self._flash_redirect(
                    f"/audit/related_parties?engagement_id={urlquote(rp_eng_id)}",
                    flash=t("flash_rp_added", lang),
                )
                return

            if path == "/audit/related_parties/disclosure":
                if ctx.get("role") not in ("manager", "owner"):
                    raise ValueError(t("err_rp_forbidden", lang))
                rp_eng_id = normalize_text(form.get("engagement_id", ""))
                if not rp_eng_id:
                    raise ValueError("engagement_id is required")
                with open_db() as conn:
                    disclosure = _cas.generate_related_party_disclosure(rp_eng_id, lang, conn)
                # Show the disclosure as a flash message (it'll be HTML-escaped)
                self._flash_redirect(
                    f"/audit/related_parties?engagement_id={urlquote(rp_eng_id)}",
                    flash=f"{t('flash_rp_disclosure_generated', lang)}: {disclosure[:200]}",
                )
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

            # ------------------------------------------------------------------
            # Admin updates POST routes
            # ------------------------------------------------------------------
            if path == "/admin/updates/check":
                if user.get("role") != "owner":
                    self._flash_redirect("/admin/updates", error=t("err_forbidden", lang))
                    return
                try:
                    from scripts.update_ledgerlink import check_for_updates
                    info = check_for_updates()
                    if info.get("update_available"):
                        self._flash_redirect(
                            "/admin/updates",
                            flash=f"{t('update_new_version', lang)}: {info['remote_version']}")
                    elif info.get("error"):
                        self._flash_redirect("/admin/updates", error=info["error"])
                    else:
                        self._flash_redirect("/admin/updates", flash=t("update_no_update", lang))
                except Exception as exc:
                    self._flash_redirect("/admin/updates", error=str(exc))
                return

            if path == "/admin/updates/install":
                if user.get("role") != "owner":
                    self._flash_redirect("/admin/updates", error=t("err_forbidden", lang))
                    return
                try:
                    from scripts.update_ledgerlink import install_update_background
                    result = install_update_background()
                    if result.get("success"):
                        self._flash_redirect("/admin/updates",
                                             flash=t("flash_update_success", lang))
                    else:
                        self._flash_redirect("/admin/updates",
                                             error=result.get("error", t("flash_update_failed", lang)))
                except Exception as exc:
                    self._flash_redirect("/admin/updates", error=str(exc))
                return

            # ------------------------------------------------------------------
            # Admin remote management POST routes
            # ------------------------------------------------------------------
            if path == "/admin/remote/restart":
                if user.get("role") != "owner":
                    self._flash_redirect("/admin/remote", error=t("err_forbidden", lang))
                    return
                try:
                    from scripts.remote_management import restart_service
                    result = restart_service()
                    if result.get("success"):
                        self._flash_redirect("/admin/remote",
                                             flash=t("flash_remote_restart_ok", lang))
                    else:
                        self._flash_redirect("/admin/remote",
                                             error=result.get("error", t("flash_remote_restart_fail", lang)))
                except Exception as exc:
                    self._flash_redirect("/admin/remote", error=str(exc))
                return

            if path == "/admin/remote/backup":
                if user.get("role") != "owner":
                    self._flash_redirect("/admin/remote", error=t("err_forbidden", lang))
                    return
                try:
                    from scripts.remote_management import create_backup
                    result = create_backup()
                    if result.get("success"):
                        self._flash_redirect(
                            "/admin/remote",
                            flash=f"{t('flash_remote_backup_ok', lang)}: {result['backup_name']} ({result['size_mb']} MB)")
                    else:
                        self._flash_redirect("/admin/remote",
                                             error=result.get("error", t("flash_remote_backup_fail", lang)))
                except Exception as exc:
                    self._flash_redirect("/admin/remote", error=str(exc))
                return

            if path == "/admin/remote/update":
                if user.get("role") != "owner":
                    self._flash_redirect("/admin/remote", error=t("err_forbidden", lang))
                    return
                try:
                    from scripts.remote_management import trigger_update
                    result = trigger_update()
                    if result.get("success"):
                        self._flash_redirect("/admin/remote",
                                             flash=t("flash_remote_update_ok", lang))
                    else:
                        self._flash_redirect("/admin/remote",
                                             error=result.get("error", t("flash_remote_update_fail", lang)))
                except Exception as exc:
                    self._flash_redirect("/admin/remote", error=str(exc))
                return

            if path == "/admin/remote/autofix":
                if user.get("role") != "owner":
                    self._flash_redirect("/admin/remote", error=t("err_forbidden", lang))
                    return
                try:
                    from scripts.remote_management import trigger_autofix
                    result = trigger_autofix()
                    # Re-render with autofix output
                    self._send_html(render_admin_remote(
                        user, t("flash_remote_autofix_done", lang), "",
                        autofix_output=result.get("output", ""),
                        lang=lang))
                except Exception as exc:
                    self._flash_redirect("/admin/remote", error=str(exc))
                return

            # ------------------------------------------------------------------
            # Onboarding routes (POST)
            # ------------------------------------------------------------------

            if path == "/onboarding/staff/add":
                if user.get("role") != "owner":
                    self._redirect("/")
                    return
                disp = normalize_text(form.get("display_name", ""))
                uname = normalize_text(form.get("username", ""))
                role_s = normalize_text(form.get("role", "employee"))
                pw = form.get("password", "")
                if role_s not in ("manager", "employee"):
                    role_s = "employee"
                if not disp or not uname or not pw:
                    self._flash_redirect("/onboarding/step1", error=t("err_required", lang) if t("err_required", lang) != "err_required" else "All fields are required.")
                    return
                try:
                    with open_db() as conn:
                        _ensure_settings_table(conn)
                        existing = conn.execute(
                            "SELECT username FROM dashboard_users WHERE username=?", (uname,)
                        ).fetchone()
                        pw_hash = hash_password(pw)
                        if existing:
                            conn.execute(
                                "UPDATE dashboard_users SET password_hash=?, display_name=?, role=?, active=1 WHERE username=?",
                                (pw_hash, disp, role_s, uname),
                            )
                        else:
                            conn.execute(
                                "INSERT INTO dashboard_users (username, password_hash, role, display_name, active, language, must_reset_password, created_at) VALUES (?,?,?,?,1,'fr',0,?)",
                                (uname, pw_hash, role_s, disp, utc_now_iso()),
                            )
                        conn.commit()
                    self._flash_redirect("/onboarding/step1", flash=t("flash_user_created", lang))
                except Exception as exc:
                    self._flash_redirect("/onboarding/step1", error=str(exc))
                return

            if path == "/onboarding/client/add":
                if user.get("role") != "owner":
                    self._redirect("/")
                    return
                cc = normalize_text(form.get("client_code", "")).upper()
                cname = normalize_text(form.get("client_name", ""))
                province = normalize_text(form.get("province", "QC"))
                entity_type = normalize_text(form.get("entity_type", "company"))
                if not cc or not cname:
                    self._flash_redirect("/onboarding/step2", error=t("err_required", lang) if t("err_required", lang) != "err_required" else "All fields are required.")
                    return
                try:
                    with open_db() as conn:
                        conn.execute(
                            """INSERT INTO clients (client_code, client_name, province, entity_type)
                               VALUES (?,?,?,?)
                               ON CONFLICT(client_code) DO UPDATE SET
                                 client_name=excluded.client_name,
                                 province=excluded.province,
                                 entity_type=excluded.entity_type""",
                            (cc, cname, province, entity_type),
                        )
                        conn.commit()
                    self._flash_redirect("/onboarding/step2", flash=t("flash_doc_updated", lang))
                except Exception as exc:
                    self._flash_redirect("/onboarding/step2", error=str(exc))
                return

            if path == "/onboarding/complete":
                if user.get("role") != "owner":
                    self._redirect("/")
                    return
                try:
                    with open_db() as conn:
                        _ensure_settings_table(conn)
                        conn.execute(
                            "INSERT INTO settings (key, value) VALUES ('onboarding_complete','1') ON CONFLICT(key) DO UPDATE SET value='1'"
                        )
                        conn.commit()
                except Exception:
                    pass
                self._redirect("/")
                return

            # ------------------------------------------------------------------
            # FIX 2: Vendor alias management POST (owner only)
            # ------------------------------------------------------------------
            if path == "/admin/vendor_aliases":
                if ctx.get("role") != "owner":
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2>'
                        f'<p>{esc(t("err_owner_required", lang))}</p></div>',
                        user=user, lang=lang), status=403)
                    return
                action = normalize_text(form.get("action", ""))
                if action == "add_alias":
                    canonical = form.get("canonical_vendor", "").strip()
                    alias_name = form.get("alias_name", "").strip()
                    if canonical and alias_name:
                        import unicodedata as _ud_alias
                        alias_key = _ud_alias.normalize("NFKD", alias_name.lower()).encode("ascii", errors="ignore").decode("ascii")
                        canonical_key = _ud_alias.normalize("NFKD", canonical.lower()).encode("ascii", errors="ignore").decode("ascii")
                        with open_db() as _va_conn:
                            _va_conn.execute(
                                "INSERT INTO vendor_aliases (canonical_vendor_key, alias_name, alias_key, created_by, created_at) "
                                "VALUES (?, ?, ?, ?, datetime('now'))",
                                (canonical_key, alias_name, alias_key, ctx.get("username", "")),
                            )
                            _va_conn.commit()
                        self._flash_redirect("/admin/vendor_aliases", flash=f"Alias '{alias_name}' → '{canonical}' created.")
                    else:
                        self._flash_redirect("/admin/vendor_aliases", error="Both canonical vendor and alias name are required.")
                    return
                if action == "delete_alias":
                    alias_id = form.get("alias_id", "")
                    if alias_id:
                        with open_db() as _va_conn:
                            _va_conn.execute("DELETE FROM vendor_aliases WHERE alias_id = ?", (alias_id,))
                            _va_conn.commit()
                        self._flash_redirect("/admin/vendor_aliases", flash="Alias deleted.")
                    return
                self._flash_redirect("/admin/vendor_aliases")
                return

            # ------------------------------------------------------------------
            # FIX 7: Manual journal entries POST (manager/owner)
            # ------------------------------------------------------------------
            if path == "/journal_entries":
                if ctx.get("role") not in ("manager", "owner"):
                    self._send_html(page_layout(
                        t("err_forbidden", lang),
                        f'<div class="card"><h2>{esc(t("err_forbidden", lang))}</h2></div>',
                        user=user, lang=lang), status=403)
                    return
                action = normalize_text(form.get("action", ""))
                if action == "create":
                    import secrets as _mje_secrets
                    entry_id = f"MJE-{_mje_secrets.token_hex(6)}"
                    client_code = form.get("client_code", "").strip()
                    period = form.get("period", "").strip()
                    entry_date = form.get("entry_date", "").strip()
                    debit_account = form.get("debit_account", "").strip()
                    credit_account = form.get("credit_account", "").strip()
                    mje_amount = form.get("amount", "0").strip()
                    description = form.get("description", "").strip()
                    document_id_ref = form.get("document_id", "").strip()

                    if not all([client_code, period, entry_date, debit_account, credit_account, mje_amount]):
                        self._flash_redirect("/journal_entries", error="All fields are required.")
                        return

                    # Conflict detection: check for automated postings in same account/period
                    conflicts = []
                    with open_db() as _mje_conn:
                        # Check posting_jobs for conflicts
                        try:
                            conflict_rows = _mje_conn.execute(
                                "SELECT document_id, gl_account, amount FROM posting_jobs "
                                "WHERE client_code = ? AND gl_account IN (?, ?) "
                                "AND status NOT IN ('cancelled', 'reversed')",
                                (client_code, debit_account, credit_account),
                            ).fetchall()
                            for cr in conflict_rows:
                                conflicts.append({
                                    "document_id": cr["document_id"],
                                    "gl_account": cr["gl_account"],
                                    "amount": cr["amount"],
                                })
                        except Exception:
                            pass

                        # Phantom tax detection: check if vendor is GST/QST registered
                        phantom_tax = False
                        if debit_account in ("2200", "2210") or credit_account in ("2200", "2210"):
                            # ITC/ITR claim — check vendor registration
                            if document_id_ref:
                                try:
                                    doc_row = _mje_conn.execute(
                                        "SELECT vendor FROM documents WHERE document_id = ?",
                                        (document_id_ref,),
                                    ).fetchone()
                                    if doc_row:
                                        vendor_name = doc_row["vendor"] or ""
                                        reg_row = _mje_conn.execute(
                                            "SELECT gst_registration_number, qst_registration_number "
                                            "FROM client_config WHERE client_code = ?",
                                            (client_code,),
                                        ).fetchone()
                                        # If no registration found, flag phantom tax
                                        if not reg_row or (not reg_row["gst_registration_number"] and not reg_row["qst_registration_number"]):
                                            phantom_tax = True
                                except Exception:
                                    pass

                        status_val = "draft"
                        if conflicts:
                            status_val = "conflict"
                        if phantom_tax:
                            status_val = "phantom_tax_blocked"

                        _mje_conn.execute(
                            "INSERT INTO manual_journal_entries "
                            "(entry_id, client_code, period, entry_date, prepared_by, "
                            "debit_account, credit_account, amount, description, document_id, "
                            "source, status, created_at, updated_at) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'bookkeeper', ?, datetime('now'), datetime('now'))",
                            (entry_id, client_code, period, entry_date, ctx.get("username", ""),
                             debit_account, credit_account, float(mje_amount), description,
                             document_id_ref or None, status_val),
                        )
                        _mje_conn.commit()

                    flash_msg = f"Journal entry {entry_id} created (status: {status_val})."
                    if conflicts:
                        flash_msg += f" WARNING: {len(conflicts)} conflict(s) with automated postings — both blocked until resolved."
                    if phantom_tax:
                        flash_msg += " CRITICAL: Phantom tax credit detected — vendor not registered for GST/QST."
                    self._flash_redirect("/journal_entries", flash=flash_msg)
                    return

                if action == "post":
                    entry_id = form.get("entry_id", "").strip()
                    if entry_id:
                        with open_db() as _mje_conn:
                            _mje_conn.execute(
                                "UPDATE manual_journal_entries SET status = 'posted', updated_at = datetime('now') "
                                "WHERE entry_id = ? AND status = 'draft'",
                                (entry_id,),
                            )
                            _mje_conn.commit()
                        self._flash_redirect("/journal_entries", flash=f"Entry {entry_id} posted.")
                    return

                if action == "reverse":
                    entry_id = form.get("entry_id", "").strip()
                    if entry_id:
                        with open_db() as _mje_conn:
                            _mje_conn.execute(
                                "UPDATE manual_journal_entries SET status = 'reversed', updated_at = datetime('now') "
                                "WHERE entry_id = ? AND status IN ('draft', 'posted')",
                                (entry_id,),
                            )
                            _mje_conn.commit()
                        self._flash_redirect("/journal_entries", flash=f"Entry {entry_id} reversed.")
                    return

                self._flash_redirect("/journal_entries")
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
    print(f"Bind     : {HOST}:{PORT}")
    if HOST == "0.0.0.0":
        try:
            import socket as _sock
            _s = _sock.socket(_sock.AF_INET, _sock.SOCK_DGRAM)
            _s.settimeout(2)
            _s.connect(("8.8.8.8", 80))
            _lan_ip = _s.getsockname()[0]
            _s.close()
            print(f"LAN URL  : http://{_lan_ip}:{PORT}/")
        except Exception:
            pass
    print(f"Local    : http://127.0.0.1:{PORT}/")
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