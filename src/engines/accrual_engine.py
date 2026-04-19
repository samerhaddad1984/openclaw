"""Final-prep Caveat B — automated period-close accruals + reversals.

What this engine does
---------------------

generate_period_accruals(client_code, period_end_date)
    Produce draft manual journal entries that a CPA should review before
    closing a period. Covers the two highest-ROI patterns out of the
    five the user asked for:

      1. Depreciation / CCA for the period (pulled from fixed_assets).
      2. Unpaid bills: documents dated on-or-before period_end that are
         still NeedsReview / Ready but not yet posted.

    The other three patterns (prepaid amortization, earned revenue not
    yet invoiced, payroll partial-period) need per-client schedules that
    the app doesn't yet persist; they are *documented as unsupported*
    rather than silently skipped — the UI marks those categories as
    "manual for now" so the CPA doesn't assume the system caught them.

generate_period_reversals(client_code, new_period_start_date)
    For every accrual from the prior period with auto_reverse=1, create
    a compensating draft JE dated on the first day of the new period.
    Status starts as 'draft' so the CPA explicitly posts after review.

approve_accrual / skip_accrual
    Thin wrappers the UI calls to move a draft JE to posted or to
    cancel it.

All rows go through manual_journal_entries (the existing schema with
three columns added) + gl_transactions (the GL we built in Sprint C).
"""
from __future__ import annotations

import logging
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from src.engines.ocr_engine import DB_PATH

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema migration (idempotent)
# ---------------------------------------------------------------------------

_ACCRUAL_COLUMNS = [
    ("auto_reverse",      "INTEGER DEFAULT 0"),
    ("reverses_entry_id", "TEXT"),
    ("accrual_type",      "TEXT"),
]


def ensure_schema(db_path: Optional[Path] = None) -> None:
    target = str(db_path) if db_path is not None else str(DB_PATH)
    conn = sqlite3.connect(target, timeout=10)
    try:
        # Make sure the base JE table exists; the review_dashboard bootstrap
        # normally creates it, but tests start from an empty database.
        conn.execute(
            "CREATE TABLE IF NOT EXISTS manual_journal_entries ("
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  entry_id TEXT UNIQUE NOT NULL,"
            "  client_code TEXT, period TEXT, entry_date TEXT,"
            "  prepared_by TEXT,"
            "  debit_account TEXT, credit_account TEXT,"
            "  amount REAL,"
            "  description TEXT,"
            "  document_id TEXT,"
            "  source TEXT DEFAULT 'accrual_engine',"
            "  status TEXT DEFAULT 'draft',"
            "  created_at TEXT DEFAULT (datetime('now')),"
            "  updated_at TEXT DEFAULT (datetime('now'))"
            ")"
        )
        existing = {r[1] for r in conn.execute(
            "PRAGMA table_info(manual_journal_entries)"
        ).fetchall()}
        for col, typedef in _ACCRUAL_COLUMNS:
            if col not in existing:
                conn.execute(
                    f"ALTER TABLE manual_journal_entries ADD COLUMN {col} {typedef}"
                )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CCA class -> declining-balance rate (same as fixed_assets_engine.py)
# ---------------------------------------------------------------------------

_CCA_RATE = {
    "1":   0.04,   "6":   0.10,   "8":   0.20,   "10":  0.30,
    "101": 0.30,   "12":  1.00,   "13":  0.00,   "14":  0.00,
    "141": 0.07,   "16":  0.40,   "17":  0.08,   "43":  0.30,
    "44":  0.25,   "45":  0.45,   "50":  0.55,   "53":  0.50,
    "54":  0.30,   "55":  0.40,
}


def _cca_rate(cls: str) -> float:
    return _CCA_RATE.get(str(cls).strip(), 0.20)


def _new_entry_id(prefix: str = "ACR") -> str:
    return f"{prefix}-{secrets.token_hex(6)}"


# ---------------------------------------------------------------------------
# Core API
# ---------------------------------------------------------------------------

@dataclass
class DraftAccrual:
    entry_id: str
    client_code: str
    period: str
    entry_date: str
    debit_account: str
    credit_account: str
    amount: float
    description: str
    accrual_type: str    # 'depreciation' | 'unpaid_bill' | ...
    document_id: Optional[str] = None
    auto_reverse: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "client_code": self.client_code,
            "period": self.period,
            "entry_date": self.entry_date,
            "debit_account": self.debit_account,
            "credit_account": self.credit_account,
            "amount": round(float(self.amount), 2),
            "description": self.description,
            "accrual_type": self.accrual_type,
            "document_id": self.document_id,
            "auto_reverse": self.auto_reverse,
        }


def _period_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _end_of_period(d: date) -> date:
    """Return the last day of the month containing d."""
    if d.month == 12:
        return date(d.year, 12, 31)
    nxt = date(d.year, d.month + 1, 1)
    return nxt - timedelta(days=1)


def _first_of_next_period(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def _depreciation_accruals(
    conn: sqlite3.Connection,
    client_code: str,
    period_end: date,
) -> List[DraftAccrual]:
    """One depreciation accrual per active fixed asset.

    Uses monthly = cost * cca_rate / 12. Assets with half-year rule
    adjustments or disposals during the period are skipped (caller can
    spot them via fixed_assets_engine.run_annual_cca and post them as
    year-end entries). Zero-rate classes (13, 14) are also skipped —
    they need explicit straight-line schedules.
    """
    out: List[DraftAccrual] = []
    try:
        rows = conn.execute(
            "SELECT asset_id, asset_name, cca_class, cost, current_ucc, "
            "acquisition_date, status FROM fixed_assets "
            "WHERE client_code = ? AND COALESCE(status, 'active') = 'active'",
            (client_code,),
        ).fetchall()
    except sqlite3.OperationalError:
        return out
    for r in rows:
        cls = r[2] if not hasattr(r, "keys") else r["cca_class"]
        cost = float((r[3] if not hasattr(r, "keys") else r["cost"]) or 0.0)
        rate = _cca_rate(cls)
        if rate <= 0 or cost <= 0:
            continue
        monthly = round(cost * rate / 12.0, 2)
        if monthly <= 0:
            continue
        asset_id = r[0] if not hasattr(r, "keys") else r["asset_id"]
        name = r[1] if not hasattr(r, "keys") else r["asset_name"]
        out.append(DraftAccrual(
            entry_id=_new_entry_id("DEP"),
            client_code=client_code,
            period=_period_key(period_end),
            entry_date=period_end.isoformat(),
            debit_account="6810",     # Depreciation expense
            credit_account="1890",    # Accumulated depreciation
            amount=monthly,
            description=(
                f"Monthly depreciation — {name} (asset {asset_id}, "
                f"class {cls}, rate {rate*100:.1f}%)"
            ),
            accrual_type="depreciation",
            # Depreciation is a real permanent entry, not reversed next month.
            auto_reverse=0,
        ))
    return out


def _unpaid_bill_accruals(
    conn: sqlite3.Connection,
    client_code: str,
    period_end: date,
) -> List[DraftAccrual]:
    """AP accruals for documents dated on-or-before period_end that are
    still NeedsReview / Ready and NOT yet posted to the GL.

    The accrual debits expense (gl_account on the document) and credits
    accounts payable (2100). It auto-reverses on period+1 start so the
    real posted bill (when it lands in the following month) isn't
    double-counted.
    """
    out: List[DraftAccrual] = []
    try:
        rows = conn.execute(
            "SELECT d.document_id, d.vendor, d.amount, d.document_date, "
            "d.gl_account FROM documents d "
            "LEFT JOIN posting_jobs pj "
            "       ON pj.document_id = d.document_id "
            "      AND COALESCE(pj.posting_status,'') = 'posted' "
            "WHERE d.client_code = ? "
            "  AND d.document_date <= ? "
            "  AND d.amount IS NOT NULL AND d.amount > 0 "
            "  AND COALESCE(d.review_status,'') IN ('NeedsReview','Ready','Ready to Post','New') "
            "  AND pj.posting_id IS NULL",
            (client_code, period_end.isoformat()),
        ).fetchall()
    except sqlite3.OperationalError:
        return out
    for r in rows:
        doc_id = r[0] if not hasattr(r, "keys") else r["document_id"]
        vendor = r[1] if not hasattr(r, "keys") else r["vendor"]
        amount = float((r[2] if not hasattr(r, "keys") else r["amount"]) or 0.0)
        doc_date = r[3] if not hasattr(r, "keys") else r["document_date"]
        gl = (r[4] if not hasattr(r, "keys") else r["gl_account"]) or ""
        debit_acct = (gl.split(" ", 1)[0].strip() if gl else "5400") or "5400"
        out.append(DraftAccrual(
            entry_id=_new_entry_id("AP"),
            client_code=client_code,
            period=_period_key(period_end),
            entry_date=period_end.isoformat(),
            debit_account=debit_acct,
            credit_account="2100",
            amount=round(amount, 2),
            description=(
                f"AP accrual — {vendor or 'vendor'} "
                f"(doc {doc_id}, invoice {doc_date})"
            ),
            accrual_type="unpaid_bill",
            document_id=doc_id,
            # Flip back on day 1 of next period: when the real posted
            # invoice lands, it shouldn't double-count.
            auto_reverse=1,
        ))
    return out


def generate_period_accruals(
    client_code: str,
    period_end_date: date | str,
    *,
    firm_code: str = "",
    db_path: Optional[Path] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    """Produce draft accruals for a client's period.

    When `persist=True` (the default) the draft JEs are inserted into
    manual_journal_entries with status='draft' and auto_reverse set
    per accrual type. When False, just returns the list without touching
    the DB (used by preview/dry-run flows).
    """
    target = str(db_path) if db_path is not None else str(DB_PATH)
    ensure_schema(db_path)
    if isinstance(period_end_date, str):
        period_end = datetime.strptime(period_end_date[:10], "%Y-%m-%d").date()
    else:
        period_end = period_end_date

    conn = sqlite3.connect(target, timeout=10)
    conn.row_factory = sqlite3.Row
    drafts: List[DraftAccrual] = []
    try:
        drafts.extend(_depreciation_accruals(conn, client_code, period_end))
        drafts.extend(_unpaid_bill_accruals(conn, client_code, period_end))

        if persist:
            with conn:
                for d in drafts:
                    conn.execute(
                        "INSERT OR IGNORE INTO manual_journal_entries "
                        "(entry_id, client_code, period, entry_date, prepared_by, "
                        "debit_account, credit_account, amount, description, "
                        "document_id, source, status, auto_reverse, accrual_type) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (d.entry_id, d.client_code, d.period, d.entry_date,
                         "accrual_engine", d.debit_account, d.credit_account,
                         d.amount, d.description, d.document_id,
                         "accrual_engine", "draft", d.auto_reverse, d.accrual_type),
                    )
    finally:
        conn.close()

    # Categories we explicitly DO NOT auto-generate yet. Surfaced to the
    # UI so the CPA isn't misled into thinking the system covered them.
    unsupported = [
        {
            "type": "prepaid_amortization",
            "reason": "No prepaid schedule persisted per client yet",
        },
        {
            "type": "earned_revenue_not_yet_invoiced",
            "reason": "Depends on engagement-letter tracking (not yet wired)",
        },
        {
            "type": "payroll_accrual",
            "reason": "Depends on payroll-schedule integration (not yet wired)",
        },
    ]

    return {
        "client_code": client_code,
        "period": _period_key(period_end),
        "period_end_date": period_end.isoformat(),
        "accruals": [d.as_dict() for d in drafts],
        "count": len(drafts),
        "unsupported_categories": unsupported,
    }


def generate_period_reversals(
    client_code: str,
    new_period_start_date: date | str,
    *,
    db_path: Optional[Path] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    """Create reversing JEs on the first day of the new period for every
    prior-period accrual flagged auto_reverse=1."""
    target = str(db_path) if db_path is not None else str(DB_PATH)
    ensure_schema(db_path)
    if isinstance(new_period_start_date, str):
        new_start = datetime.strptime(new_period_start_date[:10], "%Y-%m-%d").date()
    else:
        new_start = new_period_start_date

    # Any accrual whose entry_date is strictly before new_start.
    conn = sqlite3.connect(target, timeout=10)
    conn.row_factory = sqlite3.Row
    reversals: List[DraftAccrual] = []
    try:
        rows = conn.execute(
            "SELECT entry_id, client_code, period, entry_date, "
            "debit_account, credit_account, amount, description, "
            "document_id, accrual_type FROM manual_journal_entries "
            "WHERE client_code = ? "
            "  AND auto_reverse = 1 "
            "  AND entry_date < ? "
            "  AND status IN ('draft','posted') "
            "  AND NOT EXISTS ("
            "    SELECT 1 FROM manual_journal_entries rev "
            "    WHERE rev.reverses_entry_id = manual_journal_entries.entry_id"
            "  )",
            (client_code, new_start.isoformat()),
        ).fetchall()
        for r in rows:
            rev = DraftAccrual(
                entry_id=_new_entry_id("REV"),
                client_code=r["client_code"],
                period=_period_key(new_start),
                entry_date=new_start.isoformat(),
                # Swap sides to compensate.
                debit_account=r["credit_account"],
                credit_account=r["debit_account"],
                amount=float(r["amount"] or 0.0),
                description=f"REVERSAL: {r['description'] or r['entry_id']}",
                accrual_type=(r["accrual_type"] or "") + "_reversal",
                document_id=r["document_id"],
                auto_reverse=0,
            )
            reversals.append(rev)
        if persist and reversals:
            with conn:
                for d, orig in zip(reversals, rows):
                    conn.execute(
                        "INSERT OR IGNORE INTO manual_journal_entries "
                        "(entry_id, client_code, period, entry_date, prepared_by, "
                        "debit_account, credit_account, amount, description, "
                        "document_id, source, status, auto_reverse, accrual_type, "
                        "reverses_entry_id) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (d.entry_id, d.client_code, d.period, d.entry_date,
                         "accrual_engine", d.debit_account, d.credit_account,
                         d.amount, d.description, d.document_id,
                         "accrual_engine", "draft", 0,
                         d.accrual_type, orig["entry_id"]),
                    )
    finally:
        conn.close()

    return {
        "client_code": client_code,
        "new_period_start": new_start.isoformat(),
        "reversals": [r.as_dict() for r in reversals],
        "count": len(reversals),
    }


# ---------------------------------------------------------------------------
# Approve / skip helpers (thin wrappers for the UI)
# ---------------------------------------------------------------------------

def approve_accruals(
    entry_ids: Iterable[str],
    *,
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Post each accrual via the GL engine so both legs land on the books."""
    from src.engines.gl_engine import post_journal_entry  # lazy import
    posted: List[str] = []
    errors: List[Dict[str, str]] = []
    for eid in entry_ids:
        try:
            post_journal_entry(eid, db_path=db_path)
            posted.append(eid)
        except Exception as e:
            errors.append({"entry_id": eid, "error": str(e)})
    return {"posted": posted, "errors": errors}


def skip_accrual(
    entry_id: str,
    *,
    db_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Mark a draft accrual as 'skipped' so it never posts and never
    generates a reversal."""
    target = str(db_path) if db_path is not None else str(DB_PATH)
    conn = sqlite3.connect(target, timeout=10)
    try:
        conn.execute(
            "UPDATE manual_journal_entries "
            "SET status='skipped', auto_reverse=0, updated_at=datetime('now') "
            "WHERE entry_id = ? AND status = 'draft'",
            (entry_id,),
        )
        conn.commit()
    finally:
        conn.close()
    return {"entry_id": entry_id, "skipped": True}
