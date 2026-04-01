"""Tests for the 10 decision card scenarios in the review dashboard."""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.review_dashboard import (
    _build_decision_cards,
    _render_decision_cards,
    _simple_similarity,
)


def _make_row(**overrides):
    """Build a dict that behaves like a sqlite3.Row for testing."""
    defaults = {
        "document_id": "test-doc-001",
        "file_name": "test_invoice.pdf",
        "file_path": "",
        "client_code": "CLIENT01",
        "vendor": "Test Vendor Inc.",
        "doc_type": "invoice",
        "amount": "1000.00",
        "document_date": "2025-03-15",
        "gl_account": "5100",
        "tax_code": "H",
        "category": "Office Supplies",
        "review_status": "NeedsReview",
        "posting_status": "",
        "approval_state": "",
        "external_id": "",
        "manual_hold_reason": "",
        "manual_hold_by": "",
        "manual_hold_at": "",
        "assigned_to": "",
        "assigned_by": "",
        "assigned_at": "",
        "posting_reviewer": "",
        "confidence": "0.95",
        "hallucination_suspected": 0,
        "handwriting_low_confidence": 0,
        "raw_ocr_text": "",
        "raw_result": "{}",
        "fraud_flags": "[]",
    }
    defaults.update(overrides)

    class FakeRow(dict):
        def __getitem__(self, key):
            return self.get(key)

    return FakeRow(defaults)


# ---------------------------------------------------------------------------
# Scenario 1: Credit memo with no GST/QST breakdown
# ---------------------------------------------------------------------------
class TestScenario1CreditMemoNoTax:
    def test_credit_memo_no_tax_generates_card(self):
        row = _make_row(doc_type="credit_note", amount="-2260.00")
        raw = {"gst_amount": None, "qst_amount": None}
        cards = _build_decision_cards(row, raw, "doc-001")
        credit_cards = [c for c in cards if "CRÉDIT" in c["title_fr"] or "CREDIT" in c["title_fr"]]
        assert len(credit_cards) >= 1
        card = credit_cards[0]
        assert card["severity"] == "amber"
        assert "CTI/RTI" in card["why_fr"]

    def test_credit_memo_with_tax_no_card(self):
        row = _make_row(doc_type="credit_note", amount="-2260.00")
        raw = {"gst_amount": 100.0, "qst_amount": 200.0}
        cards = _build_decision_cards(row, raw, "doc-001")
        credit_cards = [c for c in cards if "CRÉDIT" in c["title_fr"]]
        assert len(credit_cards) == 0


# ---------------------------------------------------------------------------
# Scenario 2: New vendor, large amount
# ---------------------------------------------------------------------------
class TestScenario2NewVendorLargeAmount:
    def test_new_vendor_large_amount(self):
        row = _make_row(vendor="Fournitures ABC", amount="4500.00")
        raw = {"vendor_memory_enrichment": {"is_new_vendor": True, "invoice_count": 1}}
        cards = _build_decision_cards(row, raw, "doc-002")
        vendor_cards = [c for c in cards if "NOUVEAU" in c["title_fr"] or "New Vendor" in c["title_en"]]
        assert len(vendor_cards) >= 1
        assert vendor_cards[0]["severity"] == "red"

    def test_known_vendor_no_card(self):
        row = _make_row(vendor="Fournitures ABC", amount="4500.00")
        raw = {"vendor_memory_enrichment": {"is_new_vendor": False, "invoice_count": 50}}
        cards = _build_decision_cards(row, raw, "doc-002")
        vendor_cards = [c for c in cards if "NOUVEAU" in c["title_fr"]]
        assert len(vendor_cards) == 0


# ---------------------------------------------------------------------------
# Scenario 3: Payment recipient mismatch
# ---------------------------------------------------------------------------
class TestScenario3RecipientMismatch:
    def test_recipient_mismatch(self):
        row = _make_row(vendor="Tech Solutions Inc.")
        raw = {"payment_recipient": "TSI Consulting Ltd"}
        cards = _build_decision_cards(row, raw, "doc-003")
        mismatch_cards = [c for c in cards if "BÉNÉFICIAIRE" in c["title_fr"] or "Recipient" in c["title_en"]]
        assert len(mismatch_cards) >= 1
        assert mismatch_cards[0]["severity"] == "red"

    def test_same_recipient_no_card(self):
        row = _make_row(vendor="Tech Solutions Inc.")
        raw = {"payment_recipient": "Tech Solutions Inc."}
        cards = _build_decision_cards(row, raw, "doc-003")
        mismatch_cards = [c for c in cards if "BÉNÉFICIAIRE" in c["title_fr"]]
        assert len(mismatch_cards) == 0


# ---------------------------------------------------------------------------
# Scenario 4: CapEx disguised as expense
# ---------------------------------------------------------------------------
class TestScenario4CapEx:
    def test_capex_detected(self):
        row = _make_row(amount="28750.00", gl_account="5200",
                        file_name="Système HVAC complet.pdf")
        raw = {"description": "Complete HVAC system replacement"}
        cards = _build_decision_cards(row, raw, "doc-004")
        capex_cards = [c for c in cards if "IMMOBILISATION" in c["title_fr"] or "Capital" in c["title_en"]]
        assert len(capex_cards) >= 1
        assert capex_cards[0]["severity"] == "amber"

    def test_small_amount_no_capex(self):
        row = _make_row(amount="200.00", gl_account="5200",
                        file_name="HVAC filter.pdf")
        raw = {"description": "HVAC filter replacement"}
        cards = _build_decision_cards(row, raw, "doc-004")
        capex_cards = [c for c in cards if "IMMOBILISATION" in c["title_fr"]]
        assert len(capex_cards) == 0


# ---------------------------------------------------------------------------
# Scenario 5: Ambiguous date
# ---------------------------------------------------------------------------
class TestScenario5AmbiguousDate:
    def test_ambiguous_date(self):
        row = _make_row(document_date="04/05/2025")
        raw = {}
        cards = _build_decision_cards(row, raw, "doc-005")
        date_cards = [c for c in cards if "DATE" in c["title_fr"] or "Ambiguous" in c["title_en"]]
        assert len(date_cards) >= 1
        assert date_cards[0]["severity"] == "amber"

    def test_unambiguous_date_no_card(self):
        row = _make_row(document_date="2025-03-15")
        raw = {}
        cards = _build_decision_cards(row, raw, "doc-005")
        date_cards = [c for c in cards if "DATE" in c["title_fr"] and "AMBIG" in c["title_fr"]]
        assert len(date_cards) == 0


# ---------------------------------------------------------------------------
# Scenario 6: Loan payment not split
# ---------------------------------------------------------------------------
class TestScenario6LoanPayment:
    def test_loan_payment_detected(self):
        row = _make_row(vendor="Desjardins", amount="2847.50", gl_account="5100",
                        file_name="Paiement prêt hypothécaire.pdf")
        raw = {"description": "Paiement hypothèque mensuel"}
        cards = _build_decision_cards(row, raw, "doc-006")
        loan_cards = [c for c in cards if "PRÊT" in c["title_fr"] or "Loan" in c["title_en"]]
        assert len(loan_cards) >= 1
        assert loan_cards[0]["severity"] == "blue"


# ---------------------------------------------------------------------------
# Scenario 7: Unregistered supplier
# ---------------------------------------------------------------------------
class TestScenario7UnregisteredSupplier:
    def test_unregistered_with_tax(self):
        row = _make_row(vendor="Services Pro Laval")
        raw = {"gst_amount": 47.50, "qst_amount": 94.63, "gst_number": ""}
        cards = _build_decision_cards(row, raw, "doc-007")
        unreg_cards = [c for c in cards if "NON INSCRIT" in c["title_fr"] or "Unregistered" in c["title_en"]]
        assert len(unreg_cards) >= 1
        assert unreg_cards[0]["severity"] == "red"

    def test_registered_supplier_no_card(self):
        row = _make_row(vendor="Services Pro Laval")
        raw = {"gst_amount": 47.50, "qst_amount": 94.63, "gst_number": "123456789RT0001"}
        cards = _build_decision_cards(row, raw, "doc-007")
        unreg_cards = [c for c in cards if "NON INSCRIT" in c["title_fr"]]
        assert len(unreg_cards) == 0


# ---------------------------------------------------------------------------
# Scenario 8: Personal expense
# ---------------------------------------------------------------------------
class TestScenario8PersonalExpense:
    def test_personal_expense_flagged(self):
        row = _make_row(file_name="Rénovation cottage Laurentides.pdf", amount="8500.00")
        raw = {"description": "Rénovation cottage"}
        cards = _build_decision_cards(row, raw, "doc-008")
        personal_cards = [c for c in cards if "PERSONNELLE" in c["title_fr"] or "Personal" in c["title_en"]]
        assert len(personal_cards) >= 1
        assert personal_cards[0]["severity"] == "amber"


# ---------------------------------------------------------------------------
# Scenario 9: Duplicate invoice
# ---------------------------------------------------------------------------
class TestScenario9DuplicateInvoice:
    def test_duplicate_detected(self):
        row = _make_row(vendor="Bell Canada", amount="234.50")
        raw = {"duplicate_result": {
            "risk_level": "high",
            "matched_document_id": "doc_4521",
            "similarity_score": 0.94,
        }}
        cards = _build_decision_cards(row, raw, "doc-009")
        dup_cards = [c for c in cards if "DOUBLE" in c["title_fr"] or "Duplicate" in c["title_en"]]
        assert len(dup_cards) >= 1
        assert dup_cards[0]["severity"] == "red"

    def test_no_duplicate_no_card(self):
        row = _make_row(vendor="Bell Canada", amount="234.50")
        raw = {"duplicate_result": {"risk_level": "low"}}
        cards = _build_decision_cards(row, raw, "doc-009")
        dup_cards = [c for c in cards if "DOUBLE" in c["title_fr"]]
        assert len(dup_cards) == 0


# ---------------------------------------------------------------------------
# Scenario 10: Period locked
# ---------------------------------------------------------------------------
class TestScenario10PeriodLocked:
    def test_locked_period_generates_card(self):
        row = _make_row(document_date="2025-01-15", client_code="CLI01")
        raw = {}
        with patch("scripts.review_dashboard.get_document_period", return_value="2025-01"), \
             patch("scripts.review_dashboard.open_db") as mock_db, \
             patch("scripts.review_dashboard.is_period_locked", return_value=True), \
             patch("scripts.review_dashboard.get_lock_info", return_value={"locked_at": "2025-02-15"}):
            mock_db.return_value.__enter__ = lambda s: s
            mock_db.return_value.__exit__ = lambda s, *a: None
            cards = _build_decision_cards(row, raw, "doc-010")
        lock_cards = [c for c in cards if "VERROUILLÉE" in c["title_fr"] or "Locked" in c["title_en"]]
        assert len(lock_cards) >= 1
        assert lock_cards[0]["severity"] == "blue"


# ---------------------------------------------------------------------------
# Rendering tests
# ---------------------------------------------------------------------------
class TestDecisionCardRendering:
    def test_render_empty_returns_empty(self):
        assert _render_decision_cards([], "doc-001") == ""

    def test_render_produces_html(self):
        cards = [{
            "icon": "📋", "severity": "amber",
            "title_fr": "TEST CARD", "title_en": "Test Card",
            "issue_fr": "Test issue", "issue_en": "Test issue en",
            "why_fr": "Test why", "why_en": "Test why en",
            "recommended": {
                "label_fr": "Do it", "label_en": "Do it",
                "action": "/test", "method": "POST", "fields": {"id": "1"},
            },
            "alternatives": [
                {"label_fr": "Alt", "label_en": "Alt",
                 "action": "/alt", "method": "GET", "fields": {}},
            ],
        }]
        html = _render_decision_cards(cards, "doc-001")
        assert "decision-card" in html
        assert "dc-severity-amber" in html
        assert "TEST CARD" in html
        assert "dc-btn-primary" in html
        assert "dc-btn-alt" in html


# ---------------------------------------------------------------------------
# Similarity helper
# ---------------------------------------------------------------------------
class TestSimpleSimilarity:
    def test_identical(self):
        assert _simple_similarity("abc", "abc") == 1.0

    def test_empty(self):
        assert _simple_similarity("", "abc") == 0.0

    def test_similar(self):
        sim = _simple_similarity("tech solutions", "tech solution")
        assert sim > 0.8

    def test_different(self):
        sim = _simple_similarity("abc corp", "xyz ltd")
        assert sim < 0.3
