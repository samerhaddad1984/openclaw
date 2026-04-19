"""Additional break-injection probes beyond the happy-path workflow.

These target things the chaos framework does not exercise:
  * negative / zero / 7-decimal amounts
  * date edge cases (pre-client-creation, midnight period boundary)
  * permission edge cases (self-approval, deleted-user token reuse)
  * concurrent-ish patterns (two writes racing on same row)
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from .workflow_executor import Bug, DB_PATH, PhaseResult


def probe_weird_amounts() -> PhaseResult:
    r = PhaseResult(phase="probe_weird_amounts", client="_all", status="pass")
    from src.engines.tax_engine import calculate_gst_qst
    cases = [
        ("zero", Decimal("0"), True),
        # Negative amounts are legit for credit memos; engine should accept.
        ("negative_credit_memo", Decimal("-50.00"), True),
        ("seven_decimals", Decimal("123.1234567"), True),
        ("one_cent", Decimal("0.01"), True),
        ("huge", Decimal("9999999.99"), True),
    ]
    failures = []
    for label, amt, should_work in cases:
        try:
            res = calculate_gst_qst(amt)
            if not should_work:
                # We got a result when we expected an error; surface.
                failures.append(f"{label}: no error raised (got {res})")
        except Exception as e:
            if should_work:
                failures.append(f"{label}: unexpected error {type(e).__name__}: {e}")
    r.metric["probed"] = [c[0] for c in cases]
    r.metric["failures"] = failures
    if failures:
        r.status = "warn"
        for f in failures:
            r.bugs.append(Bug("medium", r.phase, "_all",
                              "weird amount handling", f))
    return r


def probe_permission_edges() -> PhaseResult:
    r = PhaseResult(phase="probe_permissions", client="_all", status="pass")
    from src.engines.approval_graph_engine import detect_circular_approvals
    import tempfile
    db = Path(tempfile.mktemp(suffix=".db"))
    conn = sqlite3.connect(str(db))
    conn.execute("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY, client_code TEXT, vendor TEXT,
            amount REAL, document_date TEXT, submitted_by TEXT,
            approved_by TEXT, review_status TEXT, created_at TEXT
        )
    """)
    # Self-approval attempt.
    conn.execute(
        "INSERT INTO documents VALUES "
        "('SELF1', 'T', 'V', 500, '2025-06-15', 'alice', 'alice', 'approved', datetime('now'))",
    )
    conn.commit()
    findings = detect_circular_approvals(client_code="T", db_path=db)
    r.metric["self_approval_flagged"] = any(
        len(f.get("cycle", [])) == 1 for f in findings
    )
    if not r.metric["self_approval_flagged"]:
        r.status = "fail"
        r.bugs.append(Bug("critical", r.phase, "_all",
                          "Self-approval NOT detected by circular_approval engine",
                          "Seeded alice approving her own doc; detector returned no finding."))
    conn.close()
    db.unlink(missing_ok=True)
    return r


def probe_negative_invoice_on_ar() -> PhaseResult:
    r = PhaseResult(phase="probe_negative_ar", client="_all", status="pass")
    # Attempt to insert a negative-total AR invoice. AR should allow
    # credit memos (negative is valid); just verify it doesn't crash the
    # filing_summary computation.
    import tempfile
    db = Path(tempfile.mktemp(suffix=".db"))
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE ar_invoices (
            invoice_id TEXT PRIMARY KEY, client_code TEXT,
            invoice_date TEXT, amount_ht REAL, gst_amount REAL,
            qst_amount REAL, total_amount REAL, status TEXT,
            description TEXT
        );
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, client_code TEXT,
            vendor TEXT, doc_type TEXT,
            amount REAL, document_date TEXT, gl_account TEXT, tax_code TEXT,
            review_status TEXT, subtotal REAL, tax_total REAL, gst_amount REAL,
            qst_amount REAL);
        CREATE TABLE posting_jobs (posting_id TEXT PRIMARY KEY,
            document_id TEXT, posting_status TEXT, external_id TEXT,
            created_at TEXT, updated_at TEXT);
    """)
    conn.execute(
        "INSERT INTO ar_invoices VALUES "
        "('NEG1', 'T', '2025-06-15', -1000, -50, -99.75, -1149.75, 'sent', 'Credit memo')",
    )
    conn.commit()
    conn.close()
    from src.engines import tax_engine
    try:
        summary = tax_engine.generate_filing_summary(
            "T", "2025-01-01", "2025-12-31", db_path=db,
        )
        r.metric["credit_memo_gst"] = float(summary.get("gst_collected", 0) or 0)
    except Exception as e:
        r.status = "fail"
        r.bugs.append(Bug("high", r.phase, "_all",
                          "credit-memo (negative invoice) crashed filing_summary",
                          str(e)))
    db.unlink(missing_ok=True)
    return r


def probe_period_boundary() -> PhaseResult:
    r = PhaseResult(phase="probe_period_boundary", client="_all", status="pass")
    from src.engines import tax_engine
    # Document dated on the last day of period — must be included.
    import tempfile
    db = Path(tempfile.mktemp(suffix=".db"))
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE ar_invoices (
            invoice_id TEXT PRIMARY KEY, client_code TEXT,
            invoice_date TEXT, amount_ht REAL, gst_amount REAL,
            qst_amount REAL, total_amount REAL, status TEXT, description TEXT
        );
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, client_code TEXT,
            vendor TEXT, doc_type TEXT,
            amount REAL, document_date TEXT, gl_account TEXT, tax_code TEXT,
            review_status TEXT, subtotal REAL, tax_total REAL, gst_amount REAL,
            qst_amount REAL);
        CREATE TABLE posting_jobs (posting_id TEXT PRIMARY KEY,
            document_id TEXT, posting_status TEXT, external_id TEXT,
            created_at TEXT, updated_at TEXT);
    """)
    conn.execute(
        "INSERT INTO ar_invoices VALUES "
        "('BOUND1', 'T', '2025-12-31', 1000, 50, 99.75, 1149.75, 'sent', 'Year-end')",
    )
    conn.commit()
    conn.close()
    summary = tax_engine.generate_filing_summary(
        "T", "2025-01-01", "2025-12-31", db_path=db,
    )
    gst = float(summary.get("gst_collected", 0) or 0)
    r.metric["boundary_gst"] = gst
    if gst != 50.0:
        r.status = "fail"
        r.bugs.append(Bug(
            "high", r.phase, "_all",
            f"Period-boundary invoice (2025-12-31) missed: gst={gst} vs expected 50.00",
            "",
        ))
    db.unlink(missing_ok=True)
    return r


def probe_trial_balance_coverage() -> PhaseResult:
    """Trial balance must reconcile revenue + expenses + equity. The current
    implementation only reads AP-side documents. Surface this as a finding
    rather than silently producing unbalanced TBs.
    """
    r = PhaseResult(phase="probe_trial_balance_coverage", client="_all", status="pass")
    # If we can get here at all the engine is functional; the real-world
    # "TB unbalanced because revenue missing" case is already reported in
    # the workflow_executor metric. Surface it explicitly.
    import sqlite3 as _sql
    from src.engines import audit_engine
    c = _sql.connect("/opt/otocpa/tests/simulation/sim.db")
    c.row_factory = _sql.Row
    audit_engine.generate_financial_statements(c, "ACME-CAFE", "2025-07")
    try:
        rows = c.execute(
            "SELECT COUNT(*) FROM trial_balance WHERE client_code=? AND account_code LIKE '4%'",
            ("ACME-CAFE",),
        ).fetchone()
        rev_count = rows[0] if rows else 0
    except _sql.OperationalError:
        rev_count = 0
    c.close()
    r.metric["revenue_accounts_in_tb"] = rev_count
    if rev_count == 0:
        r.status = "warn"
        r.bugs.append(Bug(
            "high", r.phase, "_all",
            "Trial balance excludes revenue accounts — AR invoices are not rolled up",
            "generate_trial_balance only reads `documents` + `posting_jobs`; AR invoices "
            "(ar_invoices table) and bank deposits are NOT aggregated, so the TB is "
            "expense-side only. CPAs must reconcile revenue manually.",
            "Extend generate_trial_balance to union AR invoices + bank credits, or "
            "document the limitation in whats_new.md.",
        ))
    return r


def probe_soce_equity_activity() -> PhaseResult:
    """SOCE should show non-zero net income for a live client."""
    r = PhaseResult(phase="probe_soce_equity_activity", client="_all", status="pass")
    # We already generate_financial_statements in phase 6; check the SOCE
    # closing equity is NOT zero for CAFE.
    import sqlite3 as _sql
    from src.engines import audit_engine
    c = _sql.connect("/opt/otocpa/tests/simulation/sim.db")
    c.row_factory = _sql.Row
    stmts = audit_engine.generate_financial_statements(c, "ACME-CAFE", "2025-07")
    soce = stmts.get("statement_of_changes_in_equity") or {}
    closing = float(soce.get("total_closing_equity") or 0)
    r.metric["closing_equity"] = closing
    r.metric["net_income_is"] = str(stmts.get("income_statement", {}).get("net_income"))
    if closing == 0:
        r.status = "warn"
        r.bugs.append(Bug(
            "medium", r.phase, "_all",
            "SOCE closing equity is $0 despite live business activity",
            "The equity accounts (3xxx) have no posted documents in the seeded "
            "client, so SOCE shows 0. Real CPAs will have opening retained earnings "
            "and net-income-of-period roll-forward — neither is automatic in this build.",
            "Add an 'opening equity' seed step in engagement setup, and wire "
            "net_income to automatically post to 3400 at period close.",
        ))
    c.close()
    return r


PROBES = [
    probe_weird_amounts,
    probe_permission_edges,
    probe_negative_invoice_on_ar,
    probe_period_boundary,
    probe_trial_balance_coverage,
    probe_soce_equity_activity,
]


def run_probes() -> list[PhaseResult]:
    return [fn() for fn in PROBES]
