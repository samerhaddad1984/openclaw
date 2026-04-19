"""Revenue-side GST/QST integration tests (Sprint F Fix 2).

Creates a throwaway SQLite DB, seeds AR + document revenue rows, and
checks that ``compute_revenue_side_taxes`` and ``generate_filing_summary``
produce the expected collected / taxable-sales figures.
"""
from __future__ import annotations

import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines import tax_engine  # noqa: E402


def _make_db(tmp_path: Path) -> Path:
    db = tmp_path / "t.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE ar_invoices (
            invoice_id TEXT PRIMARY KEY,
            client_code TEXT,
            customer_name TEXT,
            customer_email TEXT,
            invoice_number TEXT,
            invoice_date TEXT,
            due_date TEXT,
            amount_ht REAL,
            gst_amount REAL,
            qst_amount REAL,
            total_amount REAL,
            currency TEXT,
            status TEXT,
            amount_paid REAL,
            payment_date TEXT,
            description TEXT,
            created_at TEXT,
            created_by TEXT
        );
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT,
            file_path TEXT,
            client_code TEXT,
            vendor TEXT,
            doc_type TEXT,
            amount REAL,
            document_date TEXT,
            gl_account TEXT,
            tax_code TEXT,
            category TEXT,
            review_status TEXT,
            confidence REAL,
            raw_result TEXT,
            submitted_by TEXT,
            client_note TEXT,
            invoice_number TEXT,
            invoice_number_normalized TEXT,
            currency TEXT,
            subtotal REAL,
            tax_total REAL,
            extraction_method TEXT,
            ingest_source TEXT,
            fraud_flags TEXT,
            fraud_override_reason TEXT,
            fraud_override_locked INTEGER,
            substance_flags TEXT,
            entry_kind TEXT,
            review_history TEXT,
            raw_ocr_text TEXT,
            hallucination_suspected INTEGER,
            correction_count INTEGER,
            handwriting_low_confidence INTEGER,
            handwriting_sample INTEGER,
            physical_id TEXT,
            logical_fingerprint TEXT,
            assigned_to TEXT,
            manual_hold_reason TEXT,
            manual_hold_by TEXT,
            manual_hold_at TEXT,
            created_at TEXT,
            updated_at TEXT,
            ai_used INTEGER,
            ai_complexity TEXT,
            ai_model_used TEXT,
            ai_cost REAL,
            raw_ai_response TEXT,
            has_line_items TEXT,
            lines_reconciled TEXT,
            line_total_sum TEXT,
            invoice_total_gap TEXT,
            deposit_allocated TEXT,
            personal_use_percentage TEXT,
            version TEXT,
            activation_date TEXT,
            recognition_period TEXT,
            recognition_status TEXT,
            matched_bank_transaction TEXT,
            gst_amount REAL,
            qst_amount REAL,
            extraction_flags TEXT
        );
        CREATE TABLE posting_jobs (
            posting_id TEXT PRIMARY KEY,
            document_id TEXT,
            posting_status TEXT,
            external_id TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """
    )
    conn.commit()
    conn.close()
    return db


def _seed_ar_invoice(db: Path, **kwargs) -> None:
    conn = sqlite3.connect(db)
    defaults = {
        "invoice_id": kwargs.get("invoice_id", "AR1"),
        "client_code": kwargs.get("client_code", "ACME"),
        "invoice_date": kwargs.get("invoice_date", "2025-03-15"),
        "amount_ht": kwargs.get("amount_ht", 1000.00),
        "gst_amount": kwargs.get("gst_amount", 50.00),
        "qst_amount": kwargs.get("qst_amount", 99.75),
        "total_amount": kwargs.get("total_amount", 1149.75),
        "status": kwargs.get("status", "sent"),
        "description": kwargs.get("description", "Services"),
    }
    conn.execute(
        """INSERT INTO ar_invoices
           (invoice_id, client_code, invoice_date, amount_ht, gst_amount, qst_amount,
            total_amount, status, description)
           VALUES (:invoice_id, :client_code, :invoice_date, :amount_ht, :gst_amount,
                   :qst_amount, :total_amount, :status, :description)""",
        defaults,
    )
    conn.commit()
    conn.close()


def _seed_revenue_document(db: Path, **kwargs) -> None:
    conn = sqlite3.connect(db)
    defaults = {
        "document_id": kwargs.get("document_id", "D1"),
        "client_code": kwargs.get("client_code", "ACME"),
        "amount": kwargs.get("amount", 1000.00),
        "document_date": kwargs.get("document_date", "2025-03-15"),
        "gl_account": kwargs.get("gl_account", "4100"),
        "tax_code": kwargs.get("tax_code", "T"),
        "review_status": kwargs.get("review_status", "approved"),
        "subtotal": kwargs.get("subtotal", None),
        "tax_total": kwargs.get("tax_total", None),
        "gst_amount": kwargs.get("gst_amount", None),
        "qst_amount": kwargs.get("qst_amount", None),
    }
    conn.execute(
        """INSERT INTO documents
           (document_id, client_code, amount, document_date, gl_account,
            tax_code, review_status, subtotal, tax_total, gst_amount, qst_amount)
           VALUES (:document_id, :client_code, :amount, :document_date, :gl_account,
                   :tax_code, :review_status, :subtotal, :tax_total, :gst_amount,
                   :qst_amount)""",
        defaults,
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Direct compute_revenue_side_taxes tests
# ---------------------------------------------------------------------------

def test_gst_collected_from_taxable_revenue(tmp_path):
    db = _make_db(tmp_path)
    _seed_ar_invoice(db, amount_ht=1000, gst_amount=50, qst_amount=99.75, total_amount=1149.75)
    r = tax_engine.compute_revenue_side_taxes("ACME", "2025-01-01", "2025-12-31", db_path=db)
    assert r["gst_collected"] == Decimal("50.00")
    assert r["qst_collected"] == Decimal("99.75")
    assert r["taxable_sales"] == Decimal("1000.00")


def test_qst_compound_calculation(tmp_path):
    # GST and QST are now applied in parallel (not compounded) since 2013.
    # This test verifies our formula doesn't accidentally compound.
    db = _make_db(tmp_path)
    _seed_revenue_document(db, amount=1000, tax_code="T", gl_account="4100")
    r = tax_engine.compute_revenue_side_taxes("ACME", "2025-01-01", "2025-12-31", db_path=db)
    # 1000 * 5% = 50 gst; 1000 * 9.975% = 99.75 qst
    assert r["gst_collected"] == Decimal("50.00")
    assert r["qst_collected"] == Decimal("99.75")


def test_zero_rated_excluded_from_taxable(tmp_path):
    db = _make_db(tmp_path)
    _seed_revenue_document(db, document_id="D-Z", amount=500, tax_code="Z", gl_account="4100")
    r = tax_engine.compute_revenue_side_taxes("ACME", "2025-01-01", "2025-12-31", db_path=db)
    assert r["taxable_sales"] == Decimal("0.00")
    assert r["zero_rated_sales"] == Decimal("500.00")
    assert r["gst_collected"] == Decimal("0.00")


def test_exempt_excluded_from_taxable(tmp_path):
    db = _make_db(tmp_path)
    _seed_revenue_document(db, document_id="D-E", amount=400, tax_code="E", gl_account="4100")
    r = tax_engine.compute_revenue_side_taxes("ACME", "2025-01-01", "2025-12-31", db_path=db)
    assert r["exempt_sales"] == Decimal("400.00")
    assert r["gst_collected"] == Decimal("0.00")


def test_tax_included_revenue_handled(tmp_path):
    # When tax_total column is populated we treat amount as tax-inclusive.
    db = _make_db(tmp_path)
    _seed_revenue_document(
        db, document_id="DI", amount=1149.75, tax_code="T",
        tax_total=149.75, subtotal=1000, gl_account="4100",
    )
    r = tax_engine.compute_revenue_side_taxes("ACME", "2025-01-01", "2025-12-31", db_path=db)
    assert r["taxable_sales"] == Decimal("1000.00")
    assert r["gst_collected"] == Decimal("50.00")


def test_tax_excluded_revenue_handled(tmp_path):
    # No tax_total column => amount is the tax-exclusive base.
    db = _make_db(tmp_path)
    _seed_revenue_document(db, document_id="DX", amount=1000, tax_code="T", gl_account="4100")
    r = tax_engine.compute_revenue_side_taxes("ACME", "2025-01-01", "2025-12-31", db_path=db)
    assert r["taxable_sales"] == Decimal("1000.00")


def test_meals_revenue_full_tax(tmp_path):
    # Meals-served revenue (M code) should collect full GST+QST. The 50%
    # limit only applies on the input-tax-credit side.
    db = _make_db(tmp_path)
    _seed_revenue_document(db, document_id="DM", amount=1000, tax_code="M", gl_account="4100")
    r = tax_engine.compute_revenue_side_taxes("ACME", "2025-01-01", "2025-12-31", db_path=db)
    assert r["gst_collected"] == Decimal("50.00")
    assert r["qst_collected"] == Decimal("99.75")


def test_period_boundary_respected(tmp_path):
    db = _make_db(tmp_path)
    _seed_ar_invoice(db, invoice_id="IN", invoice_date="2025-03-15", amount_ht=1000, gst_amount=50, qst_amount=99.75)
    _seed_ar_invoice(db, invoice_id="OUT", invoice_date="2024-12-31", amount_ht=999, gst_amount=49.95, qst_amount=99.65)
    r = tax_engine.compute_revenue_side_taxes("ACME", "2025-01-01", "2025-12-31", db_path=db)
    # Only the 2025 invoice should count.
    assert r["gst_collected"] == Decimal("50.00")


def test_multiple_clients_isolated(tmp_path):
    db = _make_db(tmp_path)
    _seed_ar_invoice(db, invoice_id="A", client_code="ACME", amount_ht=1000, gst_amount=50, qst_amount=99.75)
    _seed_ar_invoice(db, invoice_id="B", client_code="OTHER", amount_ht=500, gst_amount=25, qst_amount=49.88)
    r = tax_engine.compute_revenue_side_taxes("ACME", "2025-01-01", "2025-12-31", db_path=db)
    assert r["gst_collected"] == Decimal("50.00")


def test_itc_vs_collected_reconciliation(tmp_path):
    # End-to-end: generate_filing_summary must combine revenue (collected)
    # with purchase-side ITC/ITR to produce a net_gst_payable that is NOT
    # equal to -ITC (which used to be the bug).
    db = _make_db(tmp_path)
    _seed_ar_invoice(db, amount_ht=1000, gst_amount=50, qst_amount=99.75)
    # Add a purchase document with ITC-eligible tax.
    _seed_revenue_document(
        db, document_id="PURCH", amount=1149.75, tax_code="T",
        gl_account="5000", gst_amount=50, qst_amount=99.75,
    )
    # Flag it posted so ITC counts.
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO posting_jobs (posting_id, document_id, posting_status, external_id) VALUES (?, ?, ?, ?)",
        ("P1", "PURCH", "posted", "EXT-1"),
    )
    conn.commit()
    conn.close()

    r = tax_engine.generate_filing_summary(
        "ACME", "2025-01-01", "2025-12-31", db_path=db,
    )
    assert r["gst_collected"] == Decimal("50.00")
    assert r["qst_collected"] == Decimal("99.75")
    # With 1149.75 total purchase the _itc_itr_from_total back-calculates
    # recoverable GST and QST; assert net is approximately zero.
    assert abs(r["net_gst_payable"]) < Decimal("1.00")


def test_filing_summary_source_attribution(tmp_path):
    db = _make_db(tmp_path)
    _seed_ar_invoice(db, amount_ht=1000, gst_amount=50, qst_amount=99.75)
    r = tax_engine.generate_filing_summary("ACME", "2025-01-01", "2025-12-31", db_path=db)
    assert r["revenue_source"] == "ar_invoices"


def test_draft_invoices_excluded(tmp_path):
    db = _make_db(tmp_path)
    _seed_ar_invoice(db, invoice_id="DR", amount_ht=5000, gst_amount=250, qst_amount=498.75, status="draft")
    r = tax_engine.compute_revenue_side_taxes("ACME", "2025-01-01", "2025-12-31", db_path=db)
    assert r["gst_collected"] == Decimal("0.00")
