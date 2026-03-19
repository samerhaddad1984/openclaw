from __future__ import annotations

import hashlib
import html
import bcrypt
import json
import secrets
import sqlite3
import sys
import traceback
import urllib.parse
from datetime import datetime, timedelta, timezone
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


DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
HOST = "127.0.0.1"
PORT = 8787
DEFAULT_REVIEWER = "Sam"
SESSION_DURATION_HOURS = 12

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

        # Add must_reset_password column to dashboard_users if missing
        user_cols = {row["name"] for row in conn.execute("PRAGMA table_info(dashboard_users)").fetchall()}
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

def render_learning_history(document_id: str) -> str:
    history = get_learning_history(document_id)
    if not history:
        return '<div class="card"><h3>Learning History</h3><p class="muted">No corrections yet.</p></div>'
    rows_html = "".join(
        f"<tr><td>{esc(i.get('field_name'))}</td><td>{esc(i.get('old_value'))}</td>"
        f"<td>{esc(i.get('new_value'))}</td><td>{esc(i.get('reviewer'))}</td><td>{esc(i.get('created_at'))}</td></tr>"
        for i in history
    )
    return f'<div class="card"><h3>Learning History</h3><table><thead><tr><th>Field</th><th>Old</th><th>New</th><th>Reviewer</th><th>Date</th></tr></thead><tbody>{rows_html}</tbody></table></div>'


def render_learning_suggestions(document_id: str, row: sqlite3.Row, username: str) -> str:
    try:
        suggestions = suggestion_engine.suggestions_for_document(
            client_code=row["client_code"], vendor=row["vendor"], doc_type=row["doc_type"], limit_per_field=5)
    except Exception:
        suggestions = {}
    if not suggestions:
        return '<div class="card"><h3>Learning Suggestions</h3><p class="muted">No suggestions yet.</p></div>'

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
                    <button class="btn-primary" type="submit">Apply</button>
                </form></td></tr>""")

    if not rows_html:
        return '<div class="card"><h3>Learning Suggestions</h3><p class="muted">No remaining suggestions.</p></div>'
    return f'<div class="card"><h3>Learning Suggestions</h3><table><thead><tr><th>Field</th><th>Suggested</th><th>Support</th><th>Confidence</th><th>Source</th><th></th></tr></thead><tbody>{"".join(rows_html)}</tbody></table></div>'


def render_posting_readiness(row: sqlite3.Row) -> str:
    blocking = compute_blocking_issues(row)
    badge = '<span class="badge badge-exception">Blocked</span>' if blocking else '<span class="badge badge-ready">Ready for Posting</span>'
    issues = "".join(f"<li>{esc(x)}</li>" for x in blocking) or "<li>None</li>"
    return f'<div class="card"><h3>Posting Readiness</h3><p>{badge}</p><ul>{issues}</ul></div>'


def render_vendor_memory(raw_result: dict[str, Any]) -> str:
    enrichment = raw_result.get("vendor_memory_enrichment")
    if not isinstance(enrichment, dict) or not enrichment:
        return '<div class="card"><h3>Vendor Memory</h3><p class="muted">No vendor memory recorded.</p></div>'
    flagged = bool(enrichment.get("flagged_for_review"))
    badge = '<span class="badge badge-exception">Flagged</span>' if flagged else '<span class="badge badge-ready">OK</span>'
    reasons = enrichment.get("review_reasons") or []
    reasons_html = "".join(f"<li>{esc(r)}</li>" for r in reasons) or "<li>None</li>"
    return f'<div class="card"><h3>Vendor Memory</h3><p>{badge}</p><ul>{reasons_html}</ul><details><summary>Show JSON</summary><textarea readonly>{esc(json.dumps(enrichment, indent=2))}</textarea></details></div>'


def render_auto_approval(raw_result: dict[str, Any]) -> str:
    approval = raw_result.get("auto_approval_result")
    if not isinstance(approval, dict) or not approval:
        return '<div class="card"><h3>Auto Approval</h3><p class="muted">No auto approval result.</p></div>'
    auto_approved = bool(approval.get("auto_approved"))
    badge = '<span class="badge badge-ready">Auto Approved</span>' if auto_approved else '<span class="badge badge-needsreview">Manual Required</span>'
    return f'<div class="card"><h3>Auto Approval</h3><p>{badge} Score: {esc(approval.get("approval_score"))} — {esc(approval.get("reason"))}</p><details><summary>Show JSON</summary><textarea readonly>{esc(json.dumps(approval, indent=2))}</textarea></details></div>'


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
.queue-table td{white-space:nowrap}
.queue-table td.file-cell,.queue-table td.reason-cell{white-space:normal}
details{border:1px solid #e5e7eb;border-radius:8px;padding:10px 12px;background:#fff}
summary{cursor:pointer;font-weight:700}
ul{margin-top:6px}
@media(max-width:1100px){.filters{grid-template-columns:repeat(3,minmax(140px,1fr))}}
@media(max-width:900px){.grid-2,.grid-3,.grid-4,.filters{grid-template-columns:1fr}}
"""


def page_layout(title: str, body_html: str, user: dict[str, Any] | None = None, flash: str = "", flash_error: str = "") -> str:
    flash_html = ""
    if flash:
        flash_html = f'<div class="flash success">{esc(flash)}</div>'
    if flash_error:
        flash_html += f'<div class="flash error">{esc(flash_error)}</div>'

    user_pill = ""
    logout_btn = ""
    if user:
        display = esc(user.get("display_name") or user.get("username") or "")
        role = esc(user.get("role") or "")
        user_pill = f'<span class="user-pill">{display} &mdash; {role}</span>'
        logout_btn = '<form method="POST" action="/logout" style="display:inline;"><button class="btn-secondary" style="padding:6px 12px;font-size:13px;">Logout</button></form>'

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{esc(title)}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}</style>
</head>
<body>
<header>
    <h1>LedgerLink Accounting Queue</h1>
    <div style="display:flex;gap:12px;align-items:center;">{user_pill} {logout_btn}</div>
</header>
<main>
    {flash_html}
    {body_html}
</main>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Login page
# ---------------------------------------------------------------------------

def render_login(flash_error: str = "") -> str:
    err = f'<div class="flash error">{esc(flash_error)}</div>' if flash_error else ""
    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>Login — LedgerLink</title><style>{CSS}
.login-wrap{{min-height:100vh;display:flex;align-items:center;justify-content:center;background:#f5f7fb}}
.login-box{{background:white;border:1px solid #e5e7eb;border-radius:12px;padding:2rem 2.5rem;min-width:320px;box-shadow:0 2px 8px rgba(0,0,0,.06)}}
</style></head>
<body>
<div class="login-wrap">
    <div class="login-box">
        <h2 style="margin-bottom:1.5rem;">LedgerLink</h2>
        {err}
        <form method="POST" action="/login">
            <div class="field"><label>Username</label><input type="text" name="username" autofocus></div>
            <div class="field"><label>Password</label><input type="password" name="password"></div>
            <button class="btn-primary" type="submit" style="width:100%;padding:12px;">Sign in</button>
        </form>
    </div>
</div>
</body></html>"""


# ---------------------------------------------------------------------------
# Change password page (shown after bcrypt migration or must_reset_password=1)
# ---------------------------------------------------------------------------

def render_change_password(user: dict[str, Any] | None = None, flash_error: str = "") -> str:
    err = f'<div class="flash error">{esc(flash_error)}</div>' if flash_error else ""
    display = esc(user.get("display_name") or user.get("username") or "") if user else ""
    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>Set Password — LedgerLink</title><style>{CSS}
.login-wrap{{min-height:100vh;display:flex;align-items:center;justify-content:center;background:#f5f7fb}}
.login-box{{background:white;border:1px solid #e5e7eb;border-radius:12px;padding:2rem 2.5rem;min-width:340px;box-shadow:0 2px 8px rgba(0,0,0,.06)}}
</style></head>
<body>
<div class="login-wrap">
    <div class="login-box">
        <h2 style="margin-bottom:.5rem;">Set a new password</h2>
        <p class="muted" style="margin-bottom:1.5rem;">Hi {display} — please choose a new password to continue.</p>
        {err}
        <form method="POST" action="/change_password">
            <div class="field"><label>New Password</label><input type="password" name="new_password" autofocus minlength="8"></div>
            <div class="field"><label>Confirm Password</label><input type="password" name="confirm_password" minlength="8"></div>
            <button class="btn-primary" type="submit" style="width:100%;padding:12px;">Save Password</button>
        </form>
    </div>
</div>
</body></html>"""


# ---------------------------------------------------------------------------
# Portfolio page
# ---------------------------------------------------------------------------

def render_portfolios(ctx: dict[str, Any], user: dict[str, Any], flash: str, flash_error: str) -> str:
    if not ctx["can_manage_team"]:
        return page_layout("Access Denied",
            '<div class="card"><h2>Access denied</h2><p>Portfolio management requires manager or owner role.</p></div>',
            user=user, flash=flash, flash_error=flash_error)

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
            move_opts += '<option value="unassigned">Unassigned</option>'
            client_rows += f"""<tr>
                <td style="font-size:13px;">{esc(cc)}</td>
                <td>
                    <form method="POST" action="/portfolios/move" style="display:inline-flex;gap:6px;align-items:center;margin-right:6px;">
                        <input type="hidden" name="client_code" value="{esc(cc)}">
                        <input type="hidden" name="from_user" value="{esc(emp_name)}">
                        <select name="to_user" style="padding:4px 8px;font-size:12px;border:1px solid #d1d5db;border-radius:6px;">{move_opts}</select>
                        <button class="btn-secondary" type="submit" style="padding:4px 10px;font-size:12px;">Move</button>
                    </form>
                    <form method="POST" action="/portfolios/remove" style="display:inline;">
                        <input type="hidden" name="client_code" value="{esc(cc)}">
                        <input type="hidden" name="username_target" value="{esc(emp_name)}">
                        <button class="btn-danger" type="submit" style="padding:4px 10px;font-size:12px;">Remove</button>
                    </form>
                </td></tr>"""

        client_table = f'<table style="margin-bottom:10px;"><thead><tr><th>Client</th><th>Actions</th></tr></thead><tbody>{client_rows if client_rows else "<tr><td colspan=2 class=muted>No clients yet.</td></tr>"}</tbody></table>'

        assign_form = ""
        if unassigned:
            opts = "".join(f'<option value="{esc(c)}">{esc(c)}</option>' for c in unassigned)
            assign_form = f"""<form method="POST" action="/portfolios/assign" style="display:flex;gap:8px;align-items:center;margin-top:10px;flex-wrap:wrap;">
                <input type="hidden" name="username_target" value="{esc(emp_name)}">
                <select name="client_code" style="padding:6px 10px;font-size:13px;border:1px solid #d1d5db;border-radius:8px;">{opts}</select>
                <button class="btn-primary" type="submit" style="padding:7px 14px;font-size:13px;">Add client</button>
            </form>"""

        count_badge = f'<span class="badge badge-muted" style="font-size:11px;padding:3px 8px;">{len(emp_clients)} client{"s" if len(emp_clients)!=1 else ""}</span>'
        cards += f'<div class="card"><h3>{esc(emp_name)} {count_badge}</h3>{client_table}{assign_form}</div>'

    unassigned_html = ""
    if unassigned:
        pills = " ".join(f'<span class="badge badge-muted" style="margin:2px;">{esc(c)}</span>' for c in unassigned)
        unassigned_html = f'<div class="card"><h3>Unassigned clients</h3><p class="muted small">Use the Add client button on an accountant card below.</p><div>{pills}</div></div>'

    body = f"""
    <div class="card"><div class="actions" style="margin-bottom:12px;"><a href="/">Back to Queue</a></div>
        <h2 style="margin-bottom:6px;">Portfolio management</h2>
        <p class="muted">Assign clients to accountants. Employees only see their assigned clients.</p>
    </div>
    {unassigned_html}
    {cards if cards else '<div class="card"><p class="muted">No employee accounts found in the database.</p></div>'}
    """
    return page_layout("Portfolio Management", body, user=user, flash=flash, flash_error=flash_error)


# ---------------------------------------------------------------------------
# User management page (owner only)
# ---------------------------------------------------------------------------

def render_user_management(ctx: dict[str, Any], user: dict[str, Any], flash: str, flash_error: str) -> str:
    if ctx["role"] != "owner":
        return page_layout("Access Denied", '<div class="card"><h2>Access denied</h2></div>', user=user)

    with open_db() as conn:
        users = conn.execute("SELECT * FROM dashboard_users ORDER BY username").fetchall()

    rows_html = "".join(f"""
        <tr>
            <td>{esc(u["username"])}</td>
            <td>{esc(u["display_name"])}</td>
            <td>{esc(u["role"])}</td>
            <td>{"Active" if u["active"] else "Inactive"}</td>
            <td>
                <form method="POST" action="/users/set_password" style="display:inline-flex;gap:6px;align-items:center;margin-right:6px;">
                    <input type="hidden" name="username_target" value="{esc(u["username"])}">
                    <input type="text" name="new_password" placeholder="New password" style="width:160px;padding:4px 8px;font-size:12px;">
                    <button class="btn-secondary" type="submit" style="padding:4px 10px;font-size:12px;">Set</button>
                </form>
            </td>
        </tr>""" for u in users)

    body = f"""
    <div class="card"><div class="actions" style="margin-bottom:12px;"><a href="/">Back to Queue</a></div>
        <h2>User management</h2>
    </div>
    <div class="card">
        <h3>Existing users</h3>
        <table><thead><tr><th>Username</th><th>Display Name</th><th>Role</th><th>Status</th><th>Password</th></tr></thead>
        <tbody>{rows_html}</tbody></table>
    </div>
    <div class="card">
        <h3>Add user</h3>
        <form method="POST" action="/users/add">
            <div class="grid-3">
                <div class="field"><label>Username</label><input type="text" name="username"></div>
                <div class="field"><label>Display Name</label><input type="text" name="display_name"></div>
                <div class="field"><label>Password</label><input type="password" name="password"></div>
                <div class="field"><label>Role</label>
                    <select name="role">
                        <option value="employee">Employee</option>
                        <option value="manager">Manager</option>
                        <option value="owner">Owner</option>
                    </select>
                </div>
            </div>
            <button class="btn-primary" type="submit">Add User</button>
        </form>
    </div>"""
    return page_layout("User Management", body, user=user, flash=flash, flash_error=flash_error)


# ---------------------------------------------------------------------------
# Home page
# ---------------------------------------------------------------------------

def render_home(ctx: dict[str, Any], user: dict[str, Any], status: str, q: str,
                flash: str, flash_error: str, include_ignored: bool,
                only_my_queue: bool, only_unassigned: bool) -> str:
    rows = get_documents(ctx=ctx, status=status, q=q, include_ignored=include_ignored,
                         only_my_queue=only_my_queue, only_unassigned=only_unassigned)
    counts = get_status_counts(ctx)

    portfolio_btn = f'<a class="button-link btn-dark" href="/portfolios">Manage Portfolios</a>' if ctx["can_manage_team"] else ""
    users_btn = f'<a class="button-link btn-secondary" href="/users">Users</a>' if ctx["role"] == "owner" else ""

    stats_html = f"""
    <div class="card">
        <div class="topbar">
            <div><h2 style="margin-bottom:4px;">Queue</h2></div>
            <div class="actions">{portfolio_btn}{users_btn}<a class="button-link btn-secondary" href="/">Reset</a></div>
        </div>
    </div>
    <div class="stats">
        <div class="stat"><div class="small muted">Needs Review</div><div><strong>{counts.get("Needs Review",0)}</strong></div></div>
        <div class="stat"><div class="small muted">On Hold</div><div><strong>{counts.get("On Hold",0)}</strong></div></div>
        <div class="stat"><div class="small muted">Ready to Post</div><div><strong>{counts.get("Ready to Post",0)}</strong></div></div>
        <div class="stat"><div class="small muted">Posted</div><div><strong>{counts.get("Posted",0)}</strong></div></div>
        <div class="stat"><div class="small muted">Visible</div><div><strong>{len(rows)}</strong></div></div>
    </div>"""

    filters_html = f"""
    <div class="card">
        <form method="GET" action="/">
            <div class="filters">
                <div class="field"><label>Status</label><select name="status">
                    <option value="">All</option>
                    {"".join(f'<option value="{v}" {"selected" if status==v else ""}>{v}</option>' for v in ["Needs Review","On Hold","Ready to Post","Posted","Ignored"])}
                </select></div>
                <div class="field"><label>Search</label><input type="text" name="q" value="{esc(q)}" placeholder="file, vendor, client..."></div>
                <div class="field"><label>Queue View</label><select name="queue_mode">
                    <option value="all" {"selected" if not only_my_queue and not only_unassigned else ""}>All Visible</option>
                    <option value="mine" {"selected" if only_my_queue else ""}>My Queue</option>
                    <option value="unassigned" {"selected" if only_unassigned else ""}>Unassigned</option>
                </select></div>
                <div class="field"><label>Ignored</label><select name="include_ignored">
                    <option value="0" {"selected" if not include_ignored else ""}>Hide</option>
                    <option value="1" {"selected" if include_ignored else ""}>Show</option>
                </select></div>
                <div class="field" style="align-self:end;"><button class="btn-primary" type="submit">Filter</button></div>
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

        if ctx["can_assign"]:
            opts = "".join(f'<option value="{esc(u)}" {"selected" if normalize_key(assigned)==normalize_key(u) else ""}>{esc(u)}</option>' for u in all_usernames)
            assign_ctrl = f"""<form method="POST" action="/assign" class="inline-form">
                <input type="hidden" name="document_id" value="{esc(row["document_id"])}">
                <input type="hidden" name="redirect_to" value="home">
                <select name="assigned_to" style="min-width:110px;"><option value="">Unassigned</option>{opts}</select>
                <button class="btn-secondary" type="submit">Assign</button></form>"""
        elif not assigned:
            assign_ctrl = f"""<form method="POST" action="/claim" class="inline-form">
                <input type="hidden" name="document_id" value="{esc(row["document_id"])}">
                <input type="hidden" name="redirect_to" value="home">
                <button class="btn-secondary" type="submit">Claim</button></form>"""
        else:
            assign_ctrl = esc(assigned or "Unassigned")

        row_html.append(f"""<tr>
            <td class="file-cell"><a href="/document?id={urlquote(row["document_id"])}">{esc(row["file_name"])}</a>
                <div class="small muted">{esc(row["document_id"])}</div></td>
            <td>{esc(row["client_code"])}</td><td>{esc(row["vendor"])}</td>
            <td>{esc(row["amount"])}</td><td>{esc(row["document_date"])}</td>
            <td>{esc(row["category"])}</td><td>{esc(row["gl_account"])}</td>
            <td>{review_status_badge(status_display)}</td>
            <td>{assign_ctrl}</td>
            <td class="reason-cell">{esc(reason)}</td>
            <td>{esc(next_action)}</td></tr>""")

    table_html = f"""<div class="card"><table class="queue-table">
        <thead><tr><th>Document</th><th>Client</th><th>Vendor</th><th>Amount</th><th>Date</th>
        <th>Category</th><th>GL Account</th><th>Status</th><th>Assigned</th><th>Reason</th><th>Action</th></tr></thead>
        <tbody>{"".join(row_html) if row_html else "<tr><td colspan=11 class=muted>No documents found.</td></tr>"}</tbody>
    </table></div>"""

    return page_layout("LedgerLink Accounting Queue", stats_html + filters_html + table_html,
                       user=user, flash=flash, flash_error=flash_error)


# ---------------------------------------------------------------------------
# Document detail page
# ---------------------------------------------------------------------------

def render_document(document_id: str, ctx: dict[str, Any], user: dict[str, Any], flash: str, flash_error: str) -> str:
    row = get_document(document_id)
    if row is None:
        return page_layout("Not Found", '<div class="card"><h2>Document not found</h2><p><a href="/">Back</a></p></div>', user=user)

    # Access control
    if not ctx["can_view_all_clients"]:
        allowed_keys = {normalize_key(c) for c in ctx.get("allowed_clients", [])}
        if normalize_key(row["client_code"]) not in allowed_keys:
            return page_layout("Access Denied", '<div class="card"><h2>Access denied</h2><p><a href="/">Back</a></p></div>', user=user)

    raw_result = safe_json_loads(row["raw_result"])
    accounting_status = get_accounting_status(row)
    review_reason = get_plain_review_reason(row)
    blocking_issues = compute_blocking_issues(row)
    blocking_html = "".join(f"<li>{esc(x)}</li>" for x in blocking_issues) or "<li>None</li>"
    assigned = normalize_text(row["assigned_to"])

    try:
        human_summary = build_human_decision_summary(raw_result)
    except Exception:
        human_summary = "Could not generate summary."

    all_usernames = get_available_usernames()

    if ctx["can_assign"]:
        opts = "".join(f'<option value="{esc(u)}" {"selected" if normalize_key(assigned)==normalize_key(u) else ""}>{esc(u)}</option>' for u in all_usernames)
        assign_card = f"""<div class="card"><h3>Assignment</h3>
            <form method="POST" action="/assign" class="inline-form">
                <input type="hidden" name="document_id" value="{esc(document_id)}">
                <input type="hidden" name="redirect_to" value="document">
                <select name="assigned_to" style="min-width:200px;"><option value="">Unassigned</option>{opts}</select>
                <button class="btn-secondary" type="submit">Save</button>
            </form></div>"""
    elif not assigned:
        assign_card = f"""<div class="card"><h3>Assignment</h3>
            <form method="POST" action="/claim" class="inline-form">
                <input type="hidden" name="document_id" value="{esc(document_id)}">
                <input type="hidden" name="redirect_to" value="document">
                <button class="btn-secondary" type="submit">Claim This Item</button>
            </form></div>"""
    else:
        assign_card = f'<div class="card"><h3>Assignment</h3><p><strong>Assigned To:</strong> {esc(assigned)}</p></div>'

    qbo_actions = ""
    if ctx["can_post"]:
        qbo_actions = f"""<div class="card"><h3>Accounting Actions</h3><div class="actions">
            <form method="POST" action="/qbo/build"><input type="hidden" name="document_id" value="{esc(document_id)}"><button class="btn-primary" type="submit">Create Posting Job</button></form>
            <form method="POST" action="/qbo/approve"><input type="hidden" name="document_id" value="{esc(document_id)}"><button class="btn-success" type="submit">Approve</button></form>
            <form method="POST" action="/qbo/post"><input type="hidden" name="document_id" value="{esc(document_id)}"><button class="btn-dark" type="submit">Post to QBO</button></form>
            <form method="POST" action="/qbo/retry"><input type="hidden" name="document_id" value="{esc(document_id)}"><button class="btn-warning" type="submit">Retry</button></form>
        </div></div>"""

    status_options = "".join(f'<option value="{v}" {"selected" if normalize_text(row["review_status"])==v else ""}>{v}</option>'
                             for v in ["Ready","NeedsReview","Ignored","Exception"])

    file_path = normalize_text(row["file_path"])
    pdf_viewer_html = ""
    if file_path:
        suffix = Path(file_path).suffix.lower()
        pdf_url = f"/pdf?id={urlquote(document_id)}"
        if suffix == ".pdf":
            pdf_viewer_html = f"""<div class="card"><h3>Document Preview</h3>
                <iframe src="{pdf_url}" style="width:100%;height:800px;border:1px solid #e5e7eb;border-radius:8px;" title="PDF Preview"></iframe>
            </div>"""
        elif suffix in {".png", ".jpg", ".jpeg"}:
            pdf_viewer_html = f"""<div class="card"><h3>Document Preview</h3>
                <img src="{pdf_url}" style="max-width:100%;border:1px solid #e5e7eb;border-radius:8px;" alt="Document image">
            </div>"""

    body = f"""
    <div class="card"><div class="actions" style="margin-bottom:12px;"><a href="/">Back to Queue</a></div>
        <h2 style="margin-bottom:8px;">{esc(row["file_name"])}</h2>
        <div class="small muted">ID: {esc(row["document_id"])}</div>
    </div>
    {pdf_viewer_html}
    <div class="card"><h3>Summary</h3>
        <div class="grid-4">
            <div><strong>Status</strong><div>{review_status_badge(accounting_status)}</div></div>
            <div><strong>Client</strong><div>{esc(row["client_code"])}</div></div>
            <div><strong>Vendor</strong><div>{esc(row["vendor"])}</div></div>
            <div><strong>Assigned To</strong><div>{esc(assigned or "Unassigned")}</div></div>
            <div><strong>Amount</strong><div>{esc(row["amount"])}</div></div>
            <div><strong>Date</strong><div>{esc(row["document_date"])}</div></div>
            <div><strong>Category</strong><div>{esc(row["category"])}</div></div>
            <div><strong>GL Account</strong><div>{esc(row["gl_account"])}</div></div>
        </div>
    </div>
    <div class="card"><h3>What Needs Attention</h3>
        <div class="field"><label>Reason</label><div class="summary-box">{esc(review_reason)}</div></div>
        <div class="field"><label>Blocking Issues</label><ul>{blocking_html}</ul></div>
    </div>
    {assign_card}
    <div class="card"><h3>Edit Fields</h3>
        <form method="POST" action="/document/update">
            <input type="hidden" name="document_id" value="{esc(document_id)}">
            <div class="grid-3">
                <div class="field"><label>Vendor</label><input type="text" name="vendor" value="{esc(row["vendor"])}"></div>
                <div class="field"><label>Client Code</label><input type="text" name="client_code" value="{esc(row["client_code"])}"></div>
                <div class="field"><label>Doc Type</label><input type="text" name="doc_type" value="{esc(row["doc_type"])}"></div>
                <div class="field"><label>Amount</label><input type="text" name="amount" value="{esc(row["amount"])}"></div>
                <div class="field"><label>Document Date</label><input type="text" name="document_date" value="{esc(row["document_date"])}"></div>
                <div class="field"><label>GL Account</label><input type="text" name="gl_account" value="{esc(row["gl_account"])}"></div>
                <div class="field"><label>Tax Code</label><input type="text" name="tax_code" value="{esc(row["tax_code"])}"></div>
                <div class="field"><label>Category</label><input type="text" name="category" value="{esc(row["category"])}"></div>
                <div class="field"><label>Review Status</label><select name="review_status">{status_options}</select></div>
            </div>
            <button class="btn-primary" type="submit">Save Changes</button>
        </form>
    </div>
    <div class="card"><h3>Hold / Return</h3><div class="grid-2">
        <form method="POST" action="/document/hold">
            <input type="hidden" name="document_id" value="{esc(document_id)}">
            <div class="field"><label>Hold Reason</label><input type="text" name="hold_reason" value="{esc(normalize_text(row["manual_hold_reason"]))}" placeholder="Reason for hold"></div>
            <button class="btn-warning" type="submit">Put On Hold</button>
        </form>
        <form method="POST" action="/document/return_ready">
            <input type="hidden" name="document_id" value="{esc(document_id)}">
            <div class="field"><label>Return Action</label><div class="small muted" style="padding-top:10px;">Use after resolving the issue.</div></div>
            <button class="btn-success" type="submit">Return to Ready</button>
        </form>
    </div></div>
    <div class="card"><h3>Posting</h3><div class="grid-4">
        <div><strong>Approval</strong><div>{approval_state_badge(normalize_text(row["approval_state"]))}</div></div>
        <div><strong>Posting</strong><div>{posting_status_badge(normalize_text(row["posting_status"]))}</div></div>
        <div><strong>Reviewer</strong><div>{esc(row["posting_reviewer"])}</div></div>
        <div><strong>External ID</strong><div>{esc(row["external_id"])}</div></div>
    </div></div>
    {qbo_actions}
    <details><summary>Advanced Details</summary><div style="margin-top:16px;">
        {render_posting_readiness(row)}
        {render_vendor_memory(raw_result)}
        {render_auto_approval(raw_result)}
        {render_learning_suggestions(document_id, row, ctx["username"])}
        {render_learning_history(document_id)}
        <div class="card"><h3>Explain Decision</h3>
            <div class="field"><label>Summary</label><div class="summary-box">{esc(human_summary)}</div></div>
            <details><summary>Raw JSON</summary><div class="field" style="margin-top:12px;"><textarea readonly>{esc(json.dumps(raw_result, indent=2))}</textarea></div></details>
        </div>
        <div class="card"><h3>Technical</h3><div class="grid-3">
            <div><strong>Confidence</strong><div>{esc(row["confidence"])}</div></div>
            <div><strong>Hold By</strong><div>{esc(row["manual_hold_by"])}</div></div>
            <div><strong>Hold At</strong><div>{esc(row["manual_hold_at"])}</div></div>
            <div><strong>Assigned By</strong><div>{esc(row["assigned_by"])}</div></div>
            <div><strong>Assigned At</strong><div>{esc(row["assigned_at"])}</div></div>
            <div><strong>File Path</strong><div>{esc(row["file_path"])}</div></div>
        </div></div>
    </div></details>"""

    return page_layout(f"Document — {normalize_text(row['file_name'])}", body, user=user, flash=flash, flash_error=flash_error)


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

    def do_GET(self) -> None:
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            flash = qs.get("flash", [""])[0]
            flash_error = qs.get("error", [""])[0]

            if path == "/login":
                self._send_html(render_login(flash_error))
                return

            user = get_session_user(self)
            if not user:
                self._redirect("/login")
                return

            ctx = build_user_context(user)

            if path == "/":
                status = qs.get("status", [""])[0]
                q = qs.get("q", [""])[0]
                include_ignored = qs.get("include_ignored", ["0"])[0] == "1"
                queue_mode = qs.get("queue_mode", ["all"])[0]
                self._send_html(render_home(ctx, user, status, q, flash, flash_error,
                                            include_ignored, queue_mode == "mine", queue_mode == "unassigned"))
                return

            if path == "/change_password":
                self._send_html(render_change_password(user, flash_error=flash_error))
                return

            if path == "/pdf":
                document_id = qs.get("id", [""])[0]
                self._serve_pdf(document_id, user)
                return

            if path == "/document":
                document_id = qs.get("id", [""])[0]
                self._send_html(render_document(document_id, ctx, user, flash, flash_error))
                return

            if path == "/portfolios":
                self._send_html(render_portfolios(ctx, user, flash, flash_error))
                return

            if path == "/users":
                self._send_html(render_user_management(ctx, user, flash, flash_error))
                return

            self._send_html(page_layout("Not Found", '<div class="card"><h2>Not Found</h2><p><a href="/">Back</a></p></div>', user=user), status=404)

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
                username = form.get("username", "").strip()
                password = form.get("password", "")
                with open_db() as conn:
                    user_row = conn.execute(
                        "SELECT * FROM dashboard_users WHERE username=? AND active=1", (username,)
                    ).fetchone()
                if not user_row:
                    self._send_html(render_login("Invalid username or password"))
                    return
                if not verify_password(password, user_row["password_hash"]):
                    self._send_html(render_login("Invalid username or password"))
                    return
                # Upgrade legacy SHA-256 hash to bcrypt on successful login
                stored = user_row["password_hash"]
                is_legacy = not stored.startswith(("$2b$", "$2a$", "$2y$"))
                token = create_session(username)
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
                        ("Set-Cookie", f"session_token={token}; HttpOnly; SameSite=Lax; Path=/")
                    ])
                else:
                    self._redirect("/", extra_headers=[
                        ("Set-Cookie", f"session_token={token}; HttpOnly; SameSite=Lax; Path=/")
                    ])
                return

            # --- Logout (no auth required) ---
            if path == "/logout":
                token = get_token_from_cookie(self)
                if token:
                    delete_session(token)
                self._redirect("/login", extra_headers=[
                    ("Set-Cookie", "session_token=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0")
                ])
                return

            # --- Change password (requires active session, no must_reset check) ---
            if path == "/change_password":
                user = get_session_user(self)
                if not user:
                    self._redirect("/login")
                    return
                new_pw = form.get("new_password", "")
                confirm_pw = form.get("confirm_password", "")
                if not new_pw or len(new_pw) < 8:
                    self._send_html(render_change_password(user, flash_error="Password must be at least 8 characters"))
                    return
                if new_pw != confirm_pw:
                    self._send_html(render_change_password(user, flash_error="Passwords do not match"))
                    return
                with open_db() as conn:
                    conn.execute(
                        "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 WHERE username=?",
                        (hash_password(new_pw), user["username"]),
                    )
                    conn.commit()
                self._flash_redirect("/", flash="Password updated successfully")
                return

            # All other POSTs require auth
            user = get_session_user(self)
            if not user:
                self._redirect("/login")
                return
            ctx = build_user_context(user)
            redirect_to = form.get("redirect_to", "document")

            if path == "/document/update":
                before_row = get_document(document_id)
                if before_row is None:
                    raise ValueError("Document not found")
                submitted = {k: form.get(k, "") for k in ["vendor","client_code","doc_type","amount","document_date","gl_account","tax_code","category","review_status"]}
                update_document_fields(document_id, submitted)
                record_learning_corrections(document_id, before_row, submitted)
                self._flash_redirect(f"/document?id={urlquote(document_id)}", flash="Document updated")
                return

            if path == "/document/hold":
                hold_reason = form.get("hold_reason", "")
                if not normalize_text(hold_reason):
                    raise ValueError("Hold reason is required")
                set_manual_hold(document_id, hold_reason, ctx["username"])
                self._flash_redirect(f"/document?id={urlquote(document_id)}", flash="Document placed on hold")
                return

            if path == "/document/return_ready":
                clear_manual_hold(document_id)
                set_document_status(document_id, "Ready")
                self._flash_redirect(f"/document?id={urlquote(document_id)}", flash="Document returned to ready")
                return

            if path == "/assign":
                assign_document(document_id, form.get("assigned_to", ""), ctx["username"])
                dest = "/" if redirect_to == "home" else f"/document?id={urlquote(document_id)}"
                self._flash_redirect(dest, flash="Assignment updated")
                return

            if path == "/claim":
                assign_document(document_id, ctx["username"], ctx["username"], note="claimed from dashboard")
                dest = "/" if redirect_to == "home" else f"/document?id={urlquote(document_id)}"
                self._flash_redirect(dest, flash="Item claimed")
                return

            if path == "/apply_suggestion":
                before_row = get_document(document_id)
                if before_row is None:
                    raise ValueError("Document not found")
                update_document_fields(document_id, {form.get("field",""): form.get("value","")})
                record_learning_corrections(document_id, before_row, {form.get("field",""): form.get("value","")})
                self._flash_redirect(f"/document?id={urlquote(document_id)}", flash="Suggestion applied")
                return

            if path == "/qbo/build":
                payload = build_posting_job(document_id=document_id, target_system="qbo", entry_kind="expense", db_path=DB_PATH)
                self._flash_redirect(f"/document?id={urlquote(document_id)}", flash="Posting job created: " + normalize_text(payload.posting_id))
                return

            if path == "/qbo/approve":
                posting = get_qbo_posting_job(document_id)
                if posting is None:
                    build_posting_job(document_id=document_id, target_system="qbo", entry_kind="expense", db_path=DB_PATH)
                    posting = get_qbo_posting_job(document_id)
                if posting is None:
                    raise ValueError("Could not create posting job")
                payload = approve_posting_job(posting_id=normalize_text(posting["posting_id"]), reviewer=DEFAULT_REVIEWER, db_path=DB_PATH)
                clear_manual_hold(document_id)
                set_document_status(document_id, "Ready")
                self._flash_redirect(f"/document?id={urlquote(document_id)}", flash="Approved: " + normalize_text(payload.posting_id))
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
                    msg = "Posted to QBO"
                    ext = normalize_text(result.get("external_id"))
                    if ext:
                        msg += f". External ID: {ext}"
                    self._flash_redirect(f"/document?id={urlquote(document_id)}", flash=msg)
                else:
                    self._flash_redirect(f"/document?id={urlquote(document_id)}", error=normalize_text(result.get("error")) or "QBO post failed")
                return

            if path == "/qbo/retry":
                posting = get_qbo_posting_job(document_id)
                if posting is None:
                    raise ValueError("No posting job exists for this document")
                payload = retry_posting_job(posting_id=normalize_text(posting["posting_id"]), reviewer=DEFAULT_REVIEWER, note="retry from dashboard", db_path=DB_PATH)
                self._flash_redirect(f"/document?id={urlquote(document_id)}", flash="Retry prepared: " + normalize_text(payload.posting_id))
                return

            # Portfolio routes
            if path == "/portfolios/assign":
                username_target = form.get("username_target", "")
                client_code = form.get("client_code", "")
                if username_target and client_code:
                    assign_client_to_user(client_code, username_target, ctx["username"])
                self._flash_redirect("/portfolios", flash=f"{client_code} assigned to {username_target}")
                return

            if path == "/portfolios/remove":
                username_target = form.get("username_target", "")
                client_code = form.get("client_code", "")
                if username_target and client_code:
                    remove_client_from_user(client_code, username_target)
                self._flash_redirect("/portfolios", flash=f"{client_code} removed from {username_target}")
                return

            if path == "/portfolios/move":
                from_user = form.get("from_user", "")
                to_user = form.get("to_user", "")
                client_code = form.get("client_code", "")
                if from_user and to_user and client_code:
                    move_client_to_user(client_code, from_user, to_user, ctx["username"])
                self._flash_redirect("/portfolios", flash=f"{client_code} moved to {to_user}")
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
                self._flash_redirect("/users", flash=f"User {username} created")
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
                self._flash_redirect("/users", flash=f"Password updated for {username_target}")
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