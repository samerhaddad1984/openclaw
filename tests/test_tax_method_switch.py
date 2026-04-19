"""Sprint H F2 — mid-period method switch + rate change tests."""
from __future__ import annotations

import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines import tax_engine  # noqa: E402


def _seed_minimal(db: Path) -> None:
    conn = sqlite3.connect(str(db))
    conn.executescript(
        """
        CREATE TABLE ar_invoices (
            invoice_id TEXT PRIMARY KEY, client_code TEXT, customer_name TEXT,
            customer_email TEXT, invoice_number TEXT, invoice_date TEXT,
            due_date TEXT, amount_ht REAL, gst_amount REAL, qst_amount REAL,
            total_amount REAL, currency TEXT, status TEXT, amount_paid REAL,
            payment_date TEXT, description TEXT, created_at TEXT, created_by TEXT
        );
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY, file_name TEXT, file_path TEXT,
            client_code TEXT, vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT, category TEXT,
            review_status TEXT, confidence REAL, raw_result TEXT,
            submitted_by TEXT, client_note TEXT, invoice_number TEXT,
            invoice_number_normalized TEXT, currency TEXT, subtotal REAL,
            tax_total REAL, extraction_method TEXT, ingest_source TEXT,
            fraud_flags TEXT, fraud_override_reason TEXT,
            fraud_override_locked INTEGER, substance_flags TEXT,
            entry_kind TEXT, review_history TEXT, raw_ocr_text TEXT,
            hallucination_suspected INTEGER, correction_count INTEGER,
            handwriting_low_confidence INTEGER, handwriting_sample INTEGER,
            physical_id TEXT, logical_fingerprint TEXT, assigned_to TEXT,
            manual_hold_reason TEXT, manual_hold_by TEXT, manual_hold_at TEXT,
            created_at TEXT, updated_at TEXT, ai_used INTEGER,
            ai_complexity TEXT, ai_model_used TEXT, ai_cost REAL,
            raw_ai_response TEXT, has_line_items TEXT, lines_reconciled TEXT,
            line_total_sum TEXT, invoice_total_gap TEXT, deposit_allocated TEXT,
            personal_use_percentage TEXT, version TEXT, activation_date TEXT,
            recognition_period TEXT, recognition_status TEXT,
            matched_bank_transaction TEXT, gst_amount REAL, qst_amount REAL,
            extraction_flags TEXT
        );
        CREATE TABLE posting_jobs (
            posting_id TEXT PRIMARY KEY, document_id TEXT, posting_status TEXT,
            external_id TEXT, created_at TEXT, updated_at TEXT
        );
        """
    )
    conn.commit()
    conn.close()


def _add_invoice(db: Path, **kw):
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO ar_invoices (invoice_id, client_code, invoice_date, "
        "amount_ht, gst_amount, qst_amount, total_amount, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'sent')",
        (kw["id"], kw.get("client", "ACME"), kw["date"], kw["ht"],
         kw["gst"], kw["qst"], kw["ht"] + kw["gst"] + kw["qst"]),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Method switch
# ---------------------------------------------------------------------------

def test_method_switch_splits_period(tmp_path):
    db = tmp_path / "ms.db"
    _seed_minimal(db)
    _add_invoice(db, id="A", date="2025-03-15", ht=1000, gst=50, qst=99.75)
    _add_invoice(db, id="B", date="2025-08-15", ht=2000, gst=100, qst=199.50)
    r = tax_engine.compute_gst_with_mid_period_switch(
        "ACME", "2025-01-01", "2025-12-31", "2025-06-01",
        old_method="quick", new_method="regular", db_path=db,
    )
    assert r["pre_switch"]["method"] == "quick"
    assert r["post_switch"]["method"] == "regular"
    assert r["pre_switch"]["period_end"] == "2025-05-31"
    assert r["post_switch"]["period_start"] == "2025-06-01"


def test_method_switch_combined_totals(tmp_path):
    db = tmp_path / "ms2.db"
    _seed_minimal(db)
    _add_invoice(db, id="A", date="2025-03-15", ht=1000, gst=50, qst=99.75)
    _add_invoice(db, id="B", date="2025-08-15", ht=2000, gst=100, qst=199.50)
    r = tax_engine.compute_gst_with_mid_period_switch(
        "ACME", "2025-01-01", "2025-12-31", "2025-06-01",
        old_method="regular", new_method="quick", db_path=db,
    )
    assert r["combined"]["gst_collected"] == Decimal("150.00")
    assert r["combined"]["qst_collected"] == Decimal("299.25")


def test_method_switch_invalid_date_raises(tmp_path):
    db = tmp_path / "ms3.db"
    _seed_minimal(db)
    with pytest.raises(ValueError):
        tax_engine.compute_gst_with_mid_period_switch(
            "ACME", "2025-01-01", "2025-12-31", "2026-01-01",
            db_path=db,
        )


def test_method_switch_same_method_raises(tmp_path):
    db = tmp_path / "ms4.db"
    _seed_minimal(db)
    with pytest.raises(ValueError):
        tax_engine.compute_gst_with_mid_period_switch(
            "ACME", "2025-01-01", "2025-12-31", "2025-06-01",
            old_method="regular", new_method="regular", db_path=db,
        )


def test_method_change_notice_string(tmp_path):
    db = tmp_path / "ms5.db"
    _seed_minimal(db)
    r = tax_engine.compute_gst_with_mid_period_switch(
        "ACME", "2025-01-01", "2025-12-31", "2025-06-01",
        old_method="quick", new_method="regular", db_path=db,
    )
    assert "quick" in r["method_change_notice"]
    assert "regular" in r["method_change_notice"]
    assert "2025-06-01" in r["method_change_notice"]


def test_quick_method_uses_qm_rate(tmp_path):
    db = tmp_path / "ms6.db"
    _seed_minimal(db)
    _add_invoice(db, id="X", date="2025-03-15", ht=1000, gst=50, qst=99.75)
    p = tax_engine.compute_gst_for_subperiod(
        "ACME", "2025-01-01", "2025-06-01", method="quick", db_path=db,
    )
    assert p["method"] == "quick"
    assert p["qm_rate"] > 0
    # Net remittance is QM-rate × gross sales (1149.75 × 0.036 ≈ 41.39)
    assert p["net_remittance"] > Decimal("40")
    assert p["net_remittance"] < Decimal("45")


def test_regular_method_returns_itc(tmp_path):
    db = tmp_path / "ms7.db"
    _seed_minimal(db)
    _add_invoice(db, id="Y", date="2025-04-15", ht=2000, gst=100, qst=199.50)
    p = tax_engine.compute_gst_for_subperiod(
        "ACME", "2025-04-01", "2025-04-30", method="regular", db_path=db,
    )
    assert p["method"] == "regular"
    assert p["gst_collected"] == Decimal("100.00")


# ---------------------------------------------------------------------------
# Rate change
# ---------------------------------------------------------------------------

def test_rate_change_applies_two_rates(tmp_path):
    db = tmp_path / "rc.db"
    _seed_minimal(db)
    _add_invoice(db, id="A", date="2025-03-15", ht=1000, gst=50, qst=99.75)
    _add_invoice(db, id="B", date="2025-09-15", ht=1000, gst=60, qst=99.75)
    r = tax_engine.compute_gst_with_rate_change(
        "ACME", "2025-01-01", "2025-12-31", "2025-07-01",
        old_gst_rate=0.05, new_gst_rate=0.06, db_path=db,
    )
    assert r["pre_change"]["applied_rate"] == Decimal("0.05")
    assert r["post_change"]["applied_rate"] == Decimal("0.06")


def test_rate_change_invalid_date_raises(tmp_path):
    db = tmp_path / "rc2.db"
    _seed_minimal(db)
    with pytest.raises(ValueError):
        tax_engine.compute_gst_with_rate_change(
            "ACME", "2025-01-01", "2025-12-31", "2026-01-01",
            old_gst_rate=0.05, new_gst_rate=0.06, db_path=db,
        )


def test_rate_change_combined_totals(tmp_path):
    db = tmp_path / "rc3.db"
    _seed_minimal(db)
    _add_invoice(db, id="A", date="2025-03-15", ht=1000, gst=50, qst=99.75)
    _add_invoice(db, id="B", date="2025-09-15", ht=1000, gst=60, qst=99.75)
    r = tax_engine.compute_gst_with_rate_change(
        "ACME", "2025-01-01", "2025-12-31", "2025-07-01",
        old_gst_rate=0.05, new_gst_rate=0.06, db_path=db,
    )
    # 1000 × 5 % + 1000 × 6 % = 110 GST.
    assert r["combined"]["gst_collected"] == Decimal("110.00")
    # QST stays at QST_RATE on full 2000.
    assert r["combined"]["qst_collected"] == Decimal("199.50")
