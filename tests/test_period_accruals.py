"""Final-prep Caveat B — period accrual + reversal engine."""
from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pytest

from src.engines import accrual_engine as ae
from src.engines import gl_engine


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "accrual.db"
    conn = sqlite3.connect(path)
    # Minimal schemas used by the engine.
    conn.executescript("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY, client_code TEXT, vendor TEXT,
            amount REAL, document_date TEXT, gl_account TEXT,
            review_status TEXT
        );
        CREATE TABLE posting_jobs (
            posting_id TEXT PRIMARY KEY, document_id TEXT,
            posting_status TEXT,
            created_at TEXT, updated_at TEXT
        );
        CREATE TABLE fixed_assets (
            asset_id TEXT PRIMARY KEY, client_code TEXT,
            asset_name TEXT, description TEXT,
            cca_class TEXT, acquisition_date TEXT,
            cost REAL, opening_ucc REAL, current_ucc REAL,
            accumulated_cca REAL, status TEXT,
            disposal_date TEXT, disposal_proceeds REAL,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    conn.close()
    monkeypatch.setattr(ae, "DB_PATH", path)
    monkeypatch.setattr(gl_engine, "DB_PATH", path)
    ae.ensure_schema(path)
    gl_engine.ensure_schema(path)
    return path


def _add_doc(db, doc_id, client="ACME", vendor="Acme",
              amount=500.0, date_str="2026-04-15",
              gl="5400 Supplies", status="Ready",
              posted=False):
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO documents (document_id, client_code, vendor, amount, "
        "document_date, gl_account, review_status) VALUES (?,?,?,?,?,?,?)",
        (doc_id, client, vendor, amount, date_str, gl, status),
    )
    if posted:
        conn.execute(
            "INSERT INTO posting_jobs (posting_id, document_id, posting_status, "
            "created_at, updated_at) VALUES (?,?,?,datetime('now'),datetime('now'))",
            (f"pj_{doc_id}", doc_id, "posted"),
        )
    conn.commit()
    conn.close()


def _add_asset(db, asset_id, client="ACME", name="Laptop",
               cls="50", cost=3600.0, acq_date="2026-01-01",
               status="active"):
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO fixed_assets (asset_id, client_code, asset_name, description, "
        "cca_class, acquisition_date, cost, opening_ucc, current_ucc, "
        "accumulated_cca, status) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (asset_id, client, name, "", cls, acq_date, cost, cost, cost, 0, status),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Depreciation
# ---------------------------------------------------------------------------

def test_depreciation_accrual_generated(db):
    # Class 50 = 55% rate -> monthly = 3600 * 0.55 / 12 = 165.00
    _add_asset(db, "A1", cost=3_600.0, cls="50")
    out = ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    deps = [a for a in out["accruals"] if a["accrual_type"] == "depreciation"]
    assert len(deps) == 1
    assert deps[0]["amount"] == pytest.approx(165.00)
    assert deps[0]["debit_account"] == "6810"
    assert deps[0]["credit_account"] == "1890"
    assert deps[0]["auto_reverse"] == 0   # permanent, don't reverse


def test_depreciation_skips_zero_rate_classes(db):
    # Class 14 has rate 0 (straight-line, needs manual schedule).
    _add_asset(db, "A2", cls="14", cost=10_000.0)
    out = ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    deps = [a for a in out["accruals"] if a["accrual_type"] == "depreciation"]
    assert deps == []


def test_depreciation_ignores_disposed_assets(db):
    _add_asset(db, "A3", cls="50", cost=1000.0, status="disposed")
    out = ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    assert [a for a in out["accruals"] if a["accrual_type"] == "depreciation"] == []


# ---------------------------------------------------------------------------
# Unpaid-bill accruals
# ---------------------------------------------------------------------------

def test_unpaid_bills_accrual_detected(db):
    _add_doc(db, "d1", amount=500.0, date_str="2026-04-10", status="Ready")
    _add_doc(db, "d2", amount=120.0, date_str="2026-04-20", status="NeedsReview")
    out = ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    ap = [a for a in out["accruals"] if a["accrual_type"] == "unpaid_bill"]
    assert len(ap) == 2
    for a in ap:
        assert a["credit_account"] == "2100"  # AP
        assert a["auto_reverse"] == 1         # reverse next period


def test_already_posted_docs_not_accrued(db):
    _add_doc(db, "d1", amount=500.0, date_str="2026-04-10",
              status="Ready", posted=True)
    out = ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    ap = [a for a in out["accruals"] if a["accrual_type"] == "unpaid_bill"]
    assert ap == []


def test_docs_after_period_end_not_accrued(db):
    # Invoice dated after the period being closed — excluded.
    _add_doc(db, "d_future", amount=500.0, date_str="2026-05-05",
              status="Ready")
    out = ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    ap = [a for a in out["accruals"] if a["accrual_type"] == "unpaid_bill"]
    assert ap == []


def test_unpaid_bill_debits_documents_gl_account(db):
    _add_doc(db, "d1", amount=750.0, date_str="2026-04-05",
              gl="5430 Publicite", status="Ready")
    out = ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    ap = [a for a in out["accruals"] if a["accrual_type"] == "unpaid_bill"][0]
    assert ap["debit_account"] == "5430"


# ---------------------------------------------------------------------------
# Reversal flow
# ---------------------------------------------------------------------------

def test_accrual_auto_reverses_next_period(db):
    _add_doc(db, "d1", amount=400.0, date_str="2026-04-10", status="Ready")
    ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    rev_out = ae.generate_period_reversals("ACME", date(2026, 5, 1), db_path=db)
    assert rev_out["count"] == 1
    rev = rev_out["reversals"][0]
    # Sides swapped relative to the original accrual (5400 <-> 2100).
    assert rev["debit_account"] == "2100"
    assert rev["credit_account"] == "5400"
    assert rev["amount"] == pytest.approx(400.0)


def test_reversal_dates_period_start(db):
    _add_doc(db, "d1", amount=100.0, date_str="2026-04-10", status="Ready")
    ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    rev = ae.generate_period_reversals("ACME", date(2026, 5, 1), db_path=db)
    assert rev["reversals"][0]["entry_date"] == "2026-05-01"
    assert rev["reversals"][0]["period"] == "2026-05"


def test_reversal_not_duplicated_when_run_twice(db):
    _add_doc(db, "d1", amount=100.0, date_str="2026-04-10", status="Ready")
    ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    r1 = ae.generate_period_reversals("ACME", date(2026, 5, 1), db_path=db)
    r2 = ae.generate_period_reversals("ACME", date(2026, 5, 1), db_path=db)
    assert r1["count"] == 1
    assert r2["count"] == 0   # second run sees reverses_entry_id populated


def test_depreciation_does_not_auto_reverse(db):
    _add_asset(db, "A1", cls="50", cost=3_600.0)
    ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    rev = ae.generate_period_reversals("ACME", date(2026, 5, 1), db_path=db)
    assert rev["count"] == 0


# ---------------------------------------------------------------------------
# CPA controls + downstream posting
# ---------------------------------------------------------------------------

def test_cpa_can_skip_suggested_accrual(db):
    _add_doc(db, "d1", amount=100.0, date_str="2026-04-10", status="Ready")
    out = ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    eid = out["accruals"][0]["entry_id"]
    ae.skip_accrual(eid, db_path=db)
    conn = sqlite3.connect(db)
    status = conn.execute(
        "SELECT status, auto_reverse FROM manual_journal_entries WHERE entry_id=?",
        (eid,),
    ).fetchone()
    conn.close()
    assert status == ("skipped", 0)


def test_skipped_accrual_does_not_reverse_next_period(db):
    _add_doc(db, "d1", amount=100.0, date_str="2026-04-10", status="Ready")
    out = ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    ae.skip_accrual(out["accruals"][0]["entry_id"], db_path=db)
    rev = ae.generate_period_reversals("ACME", date(2026, 5, 1), db_path=db)
    assert rev["count"] == 0


def test_approved_accruals_post_to_gl(db):
    _add_doc(db, "d1", amount=200.0, date_str="2026-04-10",
              gl="5400 Supplies", status="Ready")
    out = ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    eid = out["accruals"][0]["entry_id"]
    result = ae.approve_accruals([eid], db_path=db)
    assert result["posted"] == [eid]
    assert result["errors"] == []
    # Two GL rows (debit + credit) now exist for this entry.
    conn = sqlite3.connect(db)
    rows = conn.execute(
        "SELECT side, account_code, amount FROM gl_transactions WHERE entry_id=? "
        "ORDER BY side",
        (eid,),
    ).fetchall()
    conn.close()
    assert rows == [("credit", "2100", 200.0), ("debit", "5400", 200.0)]


# ---------------------------------------------------------------------------
# Unsupported categories surfaced to the UI
# ---------------------------------------------------------------------------

def test_unsupported_categories_reported(db):
    out = ae.generate_period_accruals("ACME", date(2026, 4, 30), db_path=db)
    types = {u["type"] for u in out["unsupported_categories"]}
    # The three the user asked for but we haven't wired schedules yet.
    assert "prepaid_amortization" in types
    assert "earned_revenue_not_yet_invoiced" in types
    assert "payroll_accrual" in types
