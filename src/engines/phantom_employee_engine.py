"""
src/engines/phantom_employee_engine.py — Sprint G Feature 2.

Detects phantom-employee expense patterns:

  1. Submitter not in active dashboard_users roster.
  2. High-volume single submitter (statistical outlier).
  3. Recurring identical patterns: same submitter + vendor + amount + day-of-month
     occurring 3+ times.

Returns structured findings the same shape as fraud_engine rules so they
slot into the existing /audit/anomalies UI.
"""
from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

HIGH = "high"
MEDIUM = "medium"
LOW = "low"

# Thresholds (tunable per CPA preference; defaults are conservative).
HIGH_VOLUME_MIN_SUBMISSIONS = 5      # below this we don't bother flagging
HIGH_VOLUME_SIGMA = 2.5              # outlier = mean + 2.5σ
RECURRING_MIN_OCCURRENCES = 3        # 3+ identical patterns trip the flag
RECURRING_AMOUNT_TOLERANCE = Decimal("0.50")  # ±$0.50 = "same"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        return col in cols
    except sqlite3.OperationalError:
        return False


def _active_users(conn: sqlite3.Connection, firm_code: str) -> set[str]:
    """Return lowercase usernames + emails for active users in this firm."""
    if not _has_column(conn, "dashboard_users", "username"):
        return set()
    where = ["active=1"]
    params: list[Any] = []
    if firm_code and _has_column(conn, "dashboard_users", "firm_code"):
        where.append("LOWER(COALESCE(firm_code,'')) = LOWER(?)")
        params.append(firm_code)
    sql = (
        "SELECT username, COALESCE(email,'') AS email, "
        "COALESCE(display_name,'') AS dn FROM dashboard_users "
        "WHERE " + " AND ".join(where)
    )
    out: set[str] = set()
    try:
        for r in conn.execute(sql, params).fetchall():
            for v in r:
                if v:
                    out.add(str(v).strip().lower())
    except sqlite3.OperationalError:
        return set()
    return out


def detect_phantom_employee_expenses(
    firm_code: str = "",
    client_code: str = "",
    days_back: int = 365,
    db_path: Path = DB_PATH,
    conn: sqlite3.Connection | None = None,
) -> list[dict[str, Any]]:
    own_conn = False
    if conn is None:
        if not db_path.exists():
            return []
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        own_conn = True

    findings: list[dict[str, Any]] = []
    try:
        if not _has_column(conn, "documents", "submitted_by"):
            return []
        active = _active_users(conn, firm_code)

        params: list[Any] = []
        where = ["TRIM(COALESCE(d.submitted_by,'')) != ''",
                 "LOWER(COALESCE(d.review_status,'')) != 'ignored'"]
        if firm_code and _has_column(conn, "documents", "firm_code"):
            where.append("LOWER(COALESCE(d.firm_code,'')) = LOWER(?)")
            params.append(firm_code)
        if client_code:
            where.append("LOWER(COALESCE(d.client_code,'')) = LOWER(?)")
            params.append(client_code)
        where.append(
            "COALESCE(d.created_at, d.document_date, '1970-01-01') >= "
            "datetime('now', '-' || ? || ' days')"
        )
        params.append(int(days_back))
        sql = (
            "SELECT d.submitted_by, d.document_id, d.vendor, d.amount, d.document_date "
            "FROM documents d WHERE " + " AND ".join(where)
        )
        try:
            rows = conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError:
            return []

        # ----- 1. Phantom submitter (not in active roster) -----
        # Group by submitter.
        by_submitter: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for r in rows:
            sub = (r["submitted_by"] or "").strip().lower()
            if sub:
                by_submitter[sub].append(r)

        for sub, sub_rows in by_submitter.items():
            if len(sub_rows) < HIGH_VOLUME_MIN_SUBMISSIONS:
                continue
            if sub in active:
                continue
            # Not in active roster (whether the roster is empty or the user
            # is deactivated). Flag as phantom.
            total = sum(
                Decimal(str(r["amount"] or 0)) for r in sub_rows
            )
            vendors = sorted({(r["vendor"] or "").strip() for r in sub_rows})
            findings.append({
                "type": "phantom_employee",
                "subtype": "submitter_not_in_roster",
                "severity": HIGH,
                "submitter": sub,
                "submission_count": len(sub_rows),
                "total_amount": float(total),
                "vendors": vendors[:10],
                "evidence_docs": [r["document_id"] for r in sub_rows[:25]],
                "i18n_key": "fraud_phantom_employee",
                "detected_at": _utc_now(),
            })

        # ----- 2. High-volume outlier (statistical) -----
        # Flag any submitter whose count exceeds 4x the median of the others.
        # This catches the "one user submits 50 while peers submit 5" pattern
        # where a strict mean+sigma test gets blown out by the outlier itself.
        counts_by_sub = [(s, len(v)) for s, v in by_submitter.items()]
        if len(counts_by_sub) >= 5:
            sorted_counts = sorted(c for _, c in counts_by_sub)
            mid = sorted_counts[len(sorted_counts) // 2]
            for sub, sub_rows in by_submitter.items():
                if len(sub_rows) < HIGH_VOLUME_MIN_SUBMISSIONS:
                    continue
                if mid > 0 and len(sub_rows) >= mid * 4 and len(sub_rows) > mid + 5:
                    if any(f.get("submitter") == sub for f in findings):
                        continue
                    findings.append({
                        "type": "phantom_employee",
                        "subtype": "high_volume_outlier",
                        "severity": MEDIUM,
                        "submitter": sub,
                        "submission_count": len(sub_rows),
                        "median_submissions": mid,
                        "ratio_to_median": round(len(sub_rows) / mid, 1),
                        "evidence_docs": [r["document_id"] for r in sub_rows[:25]],
                        "i18n_key": "fraud_high_volume_submitter",
                        "detected_at": _utc_now(),
                    })

        # ----- 3. Recurring identical patterns -----
        # Bucket by (submitter, vendor, day_of_month, amount-bucket).
        buckets: dict[tuple, list[sqlite3.Row]] = defaultdict(list)
        for r in rows:
            sub = (r["submitted_by"] or "").strip().lower()
            v = (r["vendor"] or "").strip().lower()
            d = (r["document_date"] or "")
            if not sub or not v or not d or len(d) < 8:
                continue
            try:
                dom = int(d[-2:])
            except ValueError:
                continue
            amt = Decimal(str(r["amount"] or 0))
            # Bucket amounts by 10-cent increments to absorb tiny noise.
            amt_bucket = (amt / RECURRING_AMOUNT_TOLERANCE).quantize(Decimal("1"))
            buckets[(sub, v, dom, str(amt_bucket))].append(r)

        for (sub, vendor, dom, amt_bucket), bucket_rows in buckets.items():
            if len(bucket_rows) < RECURRING_MIN_OCCURRENCES:
                continue
            total = sum(Decimal(str(r["amount"] or 0)) for r in bucket_rows)
            findings.append({
                "type": "phantom_employee",
                "subtype": "recurring_identical_pattern",
                "severity": MEDIUM,
                "submitter": sub,
                "vendor": vendor,
                "day_of_month": dom,
                "occurrence_count": len(bucket_rows),
                "total_amount": float(total),
                "evidence_docs": [r["document_id"] for r in bucket_rows[:25]],
                "i18n_key": "fraud_recurring_identical_pattern",
                "detected_at": _utc_now(),
            })

        return findings
    finally:
        if own_conn:
            conn.close()
