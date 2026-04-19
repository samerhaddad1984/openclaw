"""
src/engines/bank_account_audit.py — Sprint G Feature 3.

Bank-account-change audit trail. Every add / modify / delete of a
bank_connections row should be mirrored into bank_account_audit so a
later forensic review can answer who/what/when. CAS 315 + 240 fraud-risk
factor: rapid bank-detail churn is a known mid-engagement-fraud signal.

Usage from request handlers:

    from src.engines.bank_account_audit import record_bank_change

    record_bank_change(
        firm_code=firm, client_code=client, action="modified",
        account_masked=mask(account), changed_by=user, reason=reason,
        ip_address=ip, db_path=DB_PATH,
    )

    # Periodically (or on /audit/anomalies):
    findings = detect_rapid_bank_changes(firm, client, days_back=7,
                                         max_changes=1)
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

HIGH = "high"
MEDIUM = "medium"
LOW = "low"

VALID_ACTIONS = {"added", "removed", "modified"}


BANK_ACCOUNT_AUDIT_DDL = """
CREATE TABLE IF NOT EXISTS bank_account_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_code TEXT NOT NULL DEFAULT '',
    client_code TEXT NOT NULL,
    action TEXT NOT NULL,
    account_masked TEXT,
    institution_name TEXT,
    changed_by TEXT,
    reason TEXT,
    ip_address TEXT,
    diff_summary TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_bank_account_audit_lookup
    ON bank_account_audit(firm_code, client_code, created_at);
"""


def ensure_bank_account_audit_table(conn: sqlite3.Connection) -> None:
    for stmt in BANK_ACCOUNT_AUDIT_DDL.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.commit()


def mask_account(account: str) -> str:
    """Return ****1234-style masked form. Empty string in => empty out."""
    if not account:
        return ""
    a = str(account).strip()
    if len(a) <= 4:
        return "*" * len(a)
    return ("*" * (len(a) - 4)) + a[-4:]


def record_bank_change(
    *,
    firm_code: str,
    client_code: str,
    action: str,
    account_masked: str = "",
    institution_name: str = "",
    changed_by: str = "",
    reason: str = "",
    ip_address: str = "",
    diff_summary: str = "",
    db_path: Path = DB_PATH,
    conn: sqlite3.Connection | None = None,
) -> int:
    """Insert one audit row. Returns the new row id."""
    if action not in VALID_ACTIONS:
        raise ValueError(f"action must be one of {VALID_ACTIONS}, got {action!r}")
    if not client_code:
        raise ValueError("client_code is required")
    own_conn = False
    if conn is None:
        conn = sqlite3.connect(str(db_path))
        own_conn = True
    try:
        ensure_bank_account_audit_table(conn)
        cur = conn.execute(
            """INSERT INTO bank_account_audit
               (firm_code, client_code, action, account_masked, institution_name,
                changed_by, reason, ip_address, diff_summary)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (firm_code, client_code, action, account_masked, institution_name,
             changed_by, reason, ip_address, diff_summary),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        if own_conn:
            conn.close()


def get_bank_audit_trail(
    firm_code: str = "",
    client_code: str = "",
    limit: int = 100,
    db_path: Path = DB_PATH,
    conn: sqlite3.Connection | None = None,
) -> list[dict[str, Any]]:
    """Return the audit trail rows, newest first."""
    own_conn = False
    if conn is None:
        if not db_path.exists():
            return []
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        own_conn = True
    try:
        ensure_bank_account_audit_table(conn)
        params: list[Any] = []
        where = []
        if firm_code:
            where.append("LOWER(COALESCE(firm_code,'')) = LOWER(?)")
            params.append(firm_code)
        if client_code:
            where.append("LOWER(COALESCE(client_code,'')) = LOWER(?)")
            params.append(client_code)
        sql = "SELECT * FROM bank_account_audit"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
        params.append(int(limit))
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        if own_conn:
            conn.close()


def detect_rapid_bank_changes(
    firm_code: str = "",
    client_code: str = "",
    days_back: int = 7,
    max_changes: int = 1,
    db_path: Path = DB_PATH,
    conn: sqlite3.Connection | None = None,
) -> list[dict[str, Any]]:
    """Flag clients where >max_changes bank rows changed in the last N days.

    A single legitimate change (open new account) is fine; two or more in a
    week is suspicious enough to raise a CAS 240 fraud-risk discussion.
    """
    own_conn = False
    if conn is None:
        if not db_path.exists():
            return []
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        own_conn = True
    try:
        ensure_bank_account_audit_table(conn)
        params: list[Any] = []
        where = ["created_at >= datetime('now', '-' || ? || ' days')"]
        params.append(int(days_back))
        if firm_code:
            where.append("LOWER(COALESCE(firm_code,'')) = LOWER(?)")
            params.append(firm_code)
        if client_code:
            where.append("LOWER(COALESCE(client_code,'')) = LOWER(?)")
            params.append(client_code)
        sql = (
            "SELECT firm_code, client_code, COUNT(*) AS change_count, "
            "       GROUP_CONCAT(action) AS actions, "
            "       GROUP_CONCAT(account_masked) AS accounts, "
            "       MIN(created_at) AS first_change, "
            "       MAX(created_at) AS last_change "
            "FROM bank_account_audit WHERE " + " AND ".join(where) +
            " GROUP BY firm_code, client_code "
            "HAVING change_count > ?"
        )
        params.append(int(max_changes))
        rows = conn.execute(sql, params).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            count = int(r["change_count"])
            severity = HIGH if count >= 3 else MEDIUM
            out.append({
                "type": "rapid_bank_account_change",
                "severity": severity,
                "firm_code": r["firm_code"],
                "client_code": r["client_code"],
                "change_count": count,
                "actions": r["actions"],
                "accounts": r["accounts"],
                "first_change": r["first_change"],
                "last_change": r["last_change"],
                "i18n_key": "fraud_rapid_bank_change",
                "detected_at": datetime.now(timezone.utc).replace(
                    microsecond=0).isoformat(),
            })
        return out
    finally:
        if own_conn:
            conn.close()


def diff_bank_connection(
    old: dict[str, Any] | None,
    new: dict[str, Any] | None,
) -> str:
    """Produce a one-line diff summary of meaningful field changes."""
    interesting = (
        "institution_name", "account_name", "account_type", "active",
    )
    if not old:
        return "added"
    if not new:
        return "removed"
    changes = []
    for k in interesting:
        a = str(old.get(k) or "")
        b = str(new.get(k) or "")
        if a != b:
            changes.append(f"{k}: {a!r} -> {b!r}")
    return "; ".join(changes) if changes else "no-op"
