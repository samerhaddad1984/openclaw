#!/usr/bin/env python3
"""
tests/test_advanced_training_data.py
====================================
15 pytest tests covering all 5 advanced training data generators.
"""
from __future__ import annotations

import csv
import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scripts.advanced_training_data import (
    generate_forensic_traps,
    generate_complex_invoices,
    generate_messy_emails,
    generate_edge_cases,
    generate_utility_bills,
    validate_forensic_traps,
    validate_edge_cases,
    _MESSY_EMAILS,
    _INVOICE_TEMPLATES,
    _build_edge_doc,
    _ensure_documents_table,
    _open_db,
    TRAINING_DIR,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def forensic_csv() -> Path:
    """Generate forensic traps CSV once for the module."""
    return generate_forensic_traps()


@pytest.fixture(scope="module")
def invoice_dir() -> Path:
    """Generate invoice text files once."""
    return generate_complex_invoices()


@pytest.fixture(scope="module")
def email_dir() -> Path:
    """Generate messy emails once."""
    return generate_messy_emails()


@pytest.fixture(scope="module")
def edge_docs() -> list[dict]:
    """Generate edge cases into a temp DB."""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db_path = Path(tmp.name)
    docs = generate_edge_cases(db_path=db_path)
    return docs


@pytest.fixture(scope="module")
def edge_db_path() -> Path:
    """Return a temp DB path with edge cases inserted."""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db_path = Path(tmp.name)
    generate_edge_cases(db_path=db_path)
    return db_path


@pytest.fixture(scope="module")
def utility_path() -> Path:
    """Generate utility bills JSON once."""
    return generate_utility_bills()


# ═══════════════════════════════════════════════════════════════════════════════
# Generator 1 — Forensic traps (3 tests)
# ═══════════════════════════════════════════════════════════════════════════════

class TestForensicTraps:
    def test_csv_exists_and_has_rows(self, forensic_csv: Path):
        """CSV file exists and has >= 500 rows."""
        assert forensic_csv.exists()
        with open(forensic_csv, encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) >= 500

    def test_all_anomaly_types_present(self, forensic_csv: Path):
        """All 10 anomaly types are represented."""
        with open(forensic_csv, encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        anomaly_types = {r["anomaly_type"] for r in rows if r["anomaly_type"]}
        expected = {
            "duplicate_payment", "personal_expense_disguised",
            "benford_violation", "round_trip", "vendor_name_variation",
            "end_of_period", "split_payment", "ghost_vendor",
            "unusual_timing", "amount_creep",
        }
        assert expected.issubset(anomaly_types), f"Missing: {expected - anomaly_types}"

    def test_csv_columns(self, forensic_csv: Path):
        """CSV has the required columns."""
        with open(forensic_csv, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            first = next(reader)
        required = {"Date", "Vendor", "Category", "Amount", "Memo",
                     "GST", "QST", "PaymentMethod", "anomaly_type"}
        assert required.issubset(set(first.keys()))


# ═══════════════════════════════════════════════════════════════════════════════
# Generator 2 — Complex invoices (3 tests)
# ═══════════════════════════════════════════════════════════════════════════════

class TestComplexInvoices:
    def test_twenty_invoice_files(self, invoice_dir: Path):
        """20 invoice text files are generated."""
        files = sorted(invoice_dir.glob("invoice_*.txt"))
        assert len(files) == 20

    def test_all_industries_covered(self, invoice_dir: Path):
        """All 5 industries appear across the 20 invoices."""
        industries = set()
        for f in invoice_dir.glob("invoice_*.txt"):
            text = f.read_text(encoding="utf-8")
            for ind in ("MEDICAL", "SAAS", "CONSTRUCTION", "RESTAURANT", "LEGAL"):
                if ind in text:
                    industries.add(ind)
        assert industries == {"MEDICAL", "SAAS", "CONSTRUCTION", "RESTAURANT", "LEGAL"}

    def test_invoices_have_mixed_date_formats(self, invoice_dir: Path):
        """At least 2 different date format styles appear."""
        formats_found = set()
        for f in invoice_dir.glob("invoice_*.txt"):
            text = f.read_text(encoding="utf-8")
            if "March" in text:
                formats_found.add("english_month")
            if "/03/2026" in text or "/2026" in text:
                formats_found.add("dd_mm_yyyy")
            if "2026-03" in text:
                formats_found.add("iso")
        assert len(formats_found) >= 2


# ═══════════════════════════════════════════════════════════════════════════════
# Generator 3 — Messy emails (3 tests)
# ═══════════════════════════════════════════════════════════════════════════════

class TestMessyEmails:
    def test_email_files_created(self, email_dir: Path):
        """10 email .txt files and 10 parsed .json files exist."""
        txt_files = list(email_dir.glob("email_*.txt"))
        json_files = list(email_dir.glob("email_*_parsed.json"))
        assert len(txt_files) == 10
        assert len(json_files) == 10

    def test_parsed_json_has_expenses(self, email_dir: Path):
        """Each parsed JSON has at least 5 expenses."""
        for jf in sorted(email_dir.glob("email_*_parsed.json")):
            data = json.loads(jf.read_text(encoding="utf-8"))
            assert "expenses" in data
            assert len(data["expenses"]) >= 5, f"{jf.name} has only {len(data['expenses'])} expenses"

    def test_emails_contain_french(self, email_dir: Path):
        """Emails contain French text (Quebec French indicators)."""
        french_indicators = {"fait que", "pis", "genre", "mettons", "faut",
                             "allo", "merci", "dépenses", "bon", "désolé",
                             "voici", "aussi", "bonjour", "correct", "mois",
                             "facture", "gars", "taxes", "anyway"}
        for tf in sorted(email_dir.glob("email_*.txt")):
            if "_parsed" in tf.name:
                continue
            text = tf.read_text(encoding="utf-8").lower()
            found = sum(1 for w in french_indicators if w in text)
            assert found >= 2, f"{tf.name} doesn't look like Quebec French"


# ═══════════════════════════════════════════════════════════════════════════════
# Generator 4 — Edge cases (3 tests)
# ═══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    def test_correct_count(self, edge_docs: list[dict]):
        """At least 300 edge case documents generated."""
        assert len(edge_docs) >= 300

    def test_all_edge_types_present(self, edge_docs: list[dict]):
        """All 15 edge case types are represented."""
        types = {d["edge_case_type"] for d in edge_docs}
        expected_prefixes = {
            "refund_before_charge", "vendor_name_change",
            "split_payment_methods", "foreign_currency",
            "recurring_stop_restart", "intercompany",
            "advance_payment", "chargeback",
            "hst_gst_qst_confusion", "cash_no_gst",
            "employee_reimbursement", "capital_vs_expense",
            "prepaid_multiperiod", "accrued_expense",
        }
        # Check that each prefix appears in at least one type
        for prefix in expected_prefixes:
            assert any(prefix in t for t in types), f"Missing edge type: {prefix}"

    def test_edge_docs_inserted_in_db(self, edge_db_path: Path):
        """Edge case documents are in the database."""
        conn = sqlite3.connect(str(edge_db_path))
        conn.row_factory = sqlite3.Row
        count = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE ingest_source LIKE 'edge:%'"
        ).fetchone()[0]
        conn.close()
        assert count >= 300


# ═══════════════════════════════════════════════════════════════════════════════
# Generator 5 — Utility bills (3 tests)
# ═══════════════════════════════════════════════════════════════════════════════

class TestUtilityBills:
    def test_five_thousand_bills(self, utility_path: Path):
        """JSON file contains 5,000 bills."""
        assert utility_path.exists()
        with open(utility_path, encoding="utf-8") as f:
            bills = json.load(f)
        assert len(bills) == 5000

    def test_seasonal_variation(self, utility_path: Path):
        """Winter bills are on average higher than summer bills for Hydro-Quebec."""
        with open(utility_path, encoding="utf-8") as f:
            bills = json.load(f)

        winter = [b["base_amount"] for b in bills
                  if b["provider"] == "Hydro-Québec"
                  and int(b["bill_date"].split("-")[1]) in (12, 1, 2, 3)]
        summer = [b["base_amount"] for b in bills
                  if b["provider"] == "Hydro-Québec"
                  and int(b["bill_date"].split("-")[1]) in (6, 7, 8)]

        assert len(winter) > 0 and len(summer) > 0
        avg_winter = sum(winter) / len(winter)
        avg_summer = sum(summer) / len(summer)
        assert avg_winter > avg_summer, (
            f"Winter avg ${avg_winter:.2f} should be > summer avg ${avg_summer:.2f}"
        )

    def test_late_fees_around_eight_percent(self, utility_path: Path):
        """About 8% of bills have late fees (allow 4%-14% tolerance)."""
        with open(utility_path, encoding="utf-8") as f:
            bills = json.load(f)
        with_late = sum(1 for b in bills if b.get("late_fee", 0) > 0)
        pct = with_late / len(bills)
        assert 0.04 <= pct <= 0.14, f"Late fee rate {pct:.2%} outside 4-14% range"
