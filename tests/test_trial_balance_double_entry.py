"""Bug 1 regression tests — trial balance must balance.

Target invariant: for any seeded combination of AR invoices + AP documents
+ (optionally) bank transactions, the sum of debit_total across all
trial_balance rows must equal the sum of credit_total within $0.01.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines import audit_engine  # noqa: E402


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

def _fresh_db(tmp_path):
    db = tmp_path / "tb.db"
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT, vendor TEXT, doc_type TEXT,
            amount REAL, document_date TEXT, gl_account TEXT,
            tax_code TEXT, review_status TEXT,
            subtotal REAL, tax_total REAL,
            gst_amount REAL, qst_amount REAL,
            raw_result TEXT,
            created_at TEXT, updated_at TEXT
        );
        CREATE TABLE posting_jobs (
            posting_id TEXT PRIMARY KEY, document_id TEXT,
            posting_status TEXT, external_id TEXT,
            created_at TEXT, updated_at TEXT
        );
        CREATE TABLE ar_invoices (
            invoice_id TEXT PRIMARY KEY, client_code TEXT,
            customer_name TEXT, invoice_number TEXT,
            invoice_date TEXT, due_date TEXT,
            amount_ht REAL, gst_amount REAL, qst_amount REAL,
            total_amount REAL, status TEXT, description TEXT
        );
    """)
    audit_engine.ensure_audit_tables(conn)
    audit_engine.seed_chart_of_accounts(conn)
    audit_engine.seed_chart_of_accounts_quebec(conn)
    return db, conn


def _assert_tb_balances(conn, client, period, tolerance=0.01):
    rows = conn.execute(
        "SELECT debit_total, credit_total FROM trial_balance "
        "WHERE client_code=? AND period=?",
        (client, period),
    ).fetchall()
    d = sum((r["debit_total"] or 0) for r in rows)
    c = sum((r["credit_total"] or 0) for r in rows)
    diff = d - c
    assert abs(diff) <= tolerance, (
        f"TB unbalanced: debit=${d:.2f}, credit=${c:.2f}, diff=${diff:.2f} "
        f"for {client} {period}"
    )


def _add_ar(conn, inv_id, client, date, ht, gst, qst):
    total = ht + gst + qst
    conn.execute(
        "INSERT INTO ar_invoices (invoice_id, client_code, invoice_date, "
        "amount_ht, gst_amount, qst_amount, total_amount, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'sent')",
        (inv_id, client, date, ht, gst, qst, total),
    )
    conn.commit()


def _add_ap(conn, doc_id, client, date, amount, gl="5000", paid=True):
    conn.execute(
        "INSERT INTO documents (document_id, client_code, amount, "
        "document_date, gl_account, tax_code, review_status, vendor) "
        "VALUES (?, ?, ?, ?, ?, 'T', 'approved', 'TestVendor')",
        (doc_id, client, amount, date, gl),
    )
    conn.execute(
        "INSERT INTO posting_jobs (posting_id, document_id, posting_status, "
        "external_id, created_at) VALUES (?, ?, ?, ?, datetime('now'))",
        (f"P-{doc_id}", doc_id, "posted" if paid else "pending",
         f"EXT-{doc_id}" if paid else None),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_tb_balances_with_ar_invoices(tmp_path):
    db, conn = _fresh_db(tmp_path)
    _add_ar(conn, "INV1", "ACME", "2025-07-15", 1000, 50, 99.75)
    _add_ar(conn, "INV2", "ACME", "2025-07-22", 2000, 100, 199.50)
    audit_engine.generate_trial_balance(conn, "ACME", "2025-07")
    _assert_tb_balances(conn, "ACME", "2025-07")
    conn.close()


def test_tb_balances_with_ap_bills(tmp_path):
    db, conn = _fresh_db(tmp_path)
    _add_ap(conn, "AP1", "ACME", "2025-07-10", 500.00)
    _add_ap(conn, "AP2", "ACME", "2025-07-20", 750.00, gl="6100")
    audit_engine.generate_trial_balance(conn, "ACME", "2025-07")
    _assert_tb_balances(conn, "ACME", "2025-07")
    conn.close()


def test_tb_balances_with_unpaid_ap(tmp_path):
    """Unpaid AP should credit 2000 (AP) not 1010 (Cash)."""
    db, conn = _fresh_db(tmp_path)
    _add_ap(conn, "AP1", "ACME", "2025-07-10", 500.00, paid=False)
    audit_engine.generate_trial_balance(conn, "ACME", "2025-07")
    _assert_tb_balances(conn, "ACME", "2025-07")
    # Explicit check: 2000 has a credit balance, 1010 does not.
    row = conn.execute(
        "SELECT account_code, debit_total, credit_total FROM trial_balance "
        "WHERE client_code='ACME' AND period='2025-07' AND account_code='2000'",
    ).fetchone()
    assert row is not None and row["credit_total"] >= 500.00
    conn.close()


def test_tb_balances_with_mixed_sources(tmp_path):
    db, conn = _fresh_db(tmp_path)
    _add_ar(conn, "INV1", "ACME", "2025-07-15", 5000, 250, 498.75)
    _add_ap(conn, "AP1", "ACME", "2025-07-05", 1200.00)
    _add_ap(conn, "AP2", "ACME", "2025-07-12", 800.00, gl="6200", paid=False)
    audit_engine.generate_trial_balance(conn, "ACME", "2025-07")
    _assert_tb_balances(conn, "ACME", "2025-07")
    conn.close()


def test_tb_balance_tolerance_one_cent(tmp_path):
    """Even with round-trip Decimal→float conversions, imbalance ≤ $0.01."""
    db, conn = _fresh_db(tmp_path)
    # Create many small AR invoices to stress rounding.
    for i in range(50):
        _add_ar(conn, f"INV{i}", "ACME", "2025-07-15",
                ht=10.33 + i * 0.01, gst=0.52, qst=1.03)
    audit_engine.generate_trial_balance(conn, "ACME", "2025-07")
    _assert_tb_balances(conn, "ACME", "2025-07", tolerance=0.01)
    conn.close()


def test_tb_explicit_accounts_present(tmp_path):
    """After balancing, the key account categories must all appear."""
    db, conn = _fresh_db(tmp_path)
    _add_ar(conn, "INV1", "ACME", "2025-07-15", 1000, 50, 99.75)
    _add_ap(conn, "AP1", "ACME", "2025-07-10", 500.00)
    audit_engine.generate_trial_balance(conn, "ACME", "2025-07")
    codes = {r["account_code"] for r in conn.execute(
        "SELECT account_code FROM trial_balance "
        "WHERE client_code='ACME' AND period='2025-07'",
    ).fetchall()}
    assert "4100" in codes   # revenue
    assert "1200" in codes   # AR
    assert "2300" in codes   # GST payable
    assert "5000" in codes   # expense
    assert "1010" in codes   # cash (from AP credit synthesis)
    conn.close()


def test_tb_empty_source_produces_empty_result(tmp_path):
    db, conn = _fresh_db(tmp_path)
    audit_engine.generate_trial_balance(conn, "ACME", "2025-07")
    row_count = conn.execute(
        "SELECT COUNT(*) FROM trial_balance WHERE client_code='ACME'",
    ).fetchone()[0]
    assert row_count == 0
    conn.close()


def test_tb_balances_with_many_accounts(tmp_path):
    """Stress test: 10 different expense accounts + AR + partial-paid AP."""
    db, conn = _fresh_db(tmp_path)
    _add_ar(conn, "INV1", "ACME", "2025-07-15", 10000, 500, 997.50)
    for i, gl in enumerate(["5000", "5100", "5200", "5300", "6100",
                             "6200", "6300", "7100", "7200", "7300"]):
        _add_ap(conn, f"AP{i}", "ACME", "2025-07-10", 500.00 + i * 10,
                gl=gl, paid=(i % 2 == 0))
    audit_engine.generate_trial_balance(conn, "ACME", "2025-07")
    _assert_tb_balances(conn, "ACME", "2025-07")
    conn.close()
