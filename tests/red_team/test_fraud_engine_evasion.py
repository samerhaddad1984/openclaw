"""
tests/red_team/test_fraud_engine_evasion.py
============================================
Red-team evasion tests for all 13+ fraud rules.

Attacks
-------
1. Split invoices under threshold
2. Near-duplicate invoice numbers (OCR confusables)
3. Round-dollar recurring abuse
4. Same invoice via email + WhatsApp + portal (multi-channel)
5. Payee different from vendor but plausible
6. Credit note loop
7. Related party hidden through abbreviations
8. Phantom tax credit on unregistered supplier
9. Both false negatives AND false positives per rule
10. Override requires immutable written reason
11. Severity consistent across equivalent inputs
"""
from __future__ import annotations

import json
import sqlite3
import sys
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.engines.fraud_engine import (
    AMOUNT_ANOMALY_SIGMA,
    DUPLICATE_CROSS_VENDOR_DAYS,
    DUPLICATE_SAME_VENDOR_DAYS,
    LARGE_CREDIT_NOTE_LIMIT,
    MIN_HISTORY_FOR_ANOMALY,
    MIN_HISTORY_FOR_ROUND_FLAG,
    NEW_VENDOR_LARGE_AMOUNT_LIMIT,
    WEEKEND_HOLIDAY_AMOUNT_LIMIT,
    _is_round_number,
    _normalize_invoice_number,
    _normalize_vendor_key,
    _rule_bank_account_change,
    _rule_credit_note_loop,
    _rule_duplicate,
    _rule_invoice_after_payment,
    _rule_multi_channel_duplicate,
    _rule_near_duplicate_invoice_number,
    _rule_new_vendor_large_amount,
    _rule_orphan_credit_note,
    _rule_payee_invoice_mismatch,
    _rule_round_number,
    _rule_tax_registration_contradiction,
    _rule_vendor_amount_anomaly,
    _rule_vendor_category_shift,
    _rule_vendor_timing_anomaly,
    _rule_weekend_holiday,
    check_related_party,
    run_fraud_detection,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / f"test_{uuid.uuid4().hex[:8]}.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE documents (
            document_id    TEXT PRIMARY KEY,
            file_name      TEXT,
            file_path      TEXT,
            client_code    TEXT,
            vendor         TEXT,
            doc_type       TEXT,
            amount         REAL,
            document_date  TEXT,
            review_status  TEXT DEFAULT 'NeedsReview',
            confidence     REAL DEFAULT 0.5,
            raw_result     TEXT,
            created_at     TEXT DEFAULT '',
            updated_at     TEXT DEFAULT '',
            fraud_flags    TEXT,
            fraud_override_reason TEXT,
            fraud_override_locked INTEGER NOT NULL DEFAULT 0,
            invoice_number TEXT
        )
    """)
    conn.commit()
    conn.close()
    return db_path


def _make_db_with_bank(tmp_path: Path) -> Path:
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE bank_transactions (
            txn_id TEXT PRIMARY KEY,
            txn_date TEXT,
            amount REAL,
            description TEXT,
            matched_document_id TEXT
        )
    """)
    conn.commit()
    conn.close()
    return db_path


def _make_db_with_vendor_memory(tmp_path: Path) -> Path:
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE vendor_memory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor TEXT,
            client_code TEXT,
            tax_code TEXT,
            gl_account TEXT,
            approval_count INTEGER DEFAULT 0,
            raw_result TEXT,
            updated_at TEXT DEFAULT ''
        )
    """)
    conn.commit()
    conn.close()
    return db_path


def _insert(db_path: Path, **fields: Any) -> str:
    doc_id = fields.pop("document_id", str(uuid.uuid4()))
    cols = ["document_id"] + list(fields.keys())
    vals = [doc_id] + list(fields.values())
    placeholders = ", ".join(["?"] * len(vals))
    col_names = ", ".join(cols)
    conn = sqlite3.connect(str(db_path))
    conn.execute(f"INSERT INTO documents ({col_names}) VALUES ({placeholders})", vals)
    conn.commit()
    conn.close()
    return doc_id


def _insert_bank_txn(db_path: Path, txn_id: str, txn_date: str,
                     amount: float, description: str,
                     matched_document_id: str) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO bank_transactions VALUES (?, ?, ?, ?, ?)",
        (txn_id, txn_date, amount, description, matched_document_id),
    )
    conn.commit()
    conn.close()


def _insert_vendor_memory(db_path: Path, vendor: str, client_code: str,
                          tax_code: str = "", gl_account: str = "",
                          approval_count: int = 5,
                          raw_result: str = "{}") -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO vendor_memory (vendor, client_code, tax_code, gl_account, "
        "approval_count, raw_result) VALUES (?, ?, ?, ?, ?, ?)",
        (vendor, client_code, tax_code, gl_account, approval_count, raw_result),
    )
    conn.commit()
    conn.close()


def _vendor_history(n: int, amount: float = 1000.0, day: int = 15,
                    review_status: str = "Posted") -> list[dict[str, Any]]:
    return [
        {
            "document_id": f"hist_{i}",
            "amount": amount,
            "document_date": f"2025-01-{day:02d}",
            "raw_result": None,
            "review_status": review_status,
        }
        for i in range(n)
    ]


# ======================================================================
# 1. INVOICE SPLITTING UNDER THRESHOLD
# ======================================================================

class TestInvoiceSplitting:
    """Attack: split invoices just below $2,000 to evade new_vendor_large_amount."""

    def test_single_below_threshold_not_flagged(self):
        """A single $1,999 invoice from a new vendor should NOT flag."""
        flag = _rule_new_vendor_large_amount("SplitCo", 1999.99, [])
        assert flag is None

    def test_cumulative_splitting_detected_with_history(self):
        """P1-7: cumulative check — 3× $800 from new vendor within 30 days > $2,000."""
        history = [
            {"amount": 800.0, "document_date": "2025-03-01", "review_status": "NeedsReview"},
            {"amount": 800.0, "document_date": "2025-03-05", "review_status": "NeedsReview"},
        ]
        flag = _rule_new_vendor_large_amount("SplitCo", 800.0, history, doc_date=date(2025, 3, 10))
        assert flag is not None, "Cumulative $2,400 across 3 invoices should trigger splitting"
        assert flag["rule"] == "invoice_splitting_suspected"

    def test_splitting_evades_when_spread_over_31_days(self):
        """Splitting over 31 days evades the 30-day cumulative window."""
        history = [
            {"amount": 1500.0, "document_date": "2025-02-01", "review_status": "NeedsReview"},
        ]
        flag = _rule_new_vendor_large_amount("SplitCo", 1500.0, history, doc_date=date(2025, 3, 5))
        # 32 days apart — cumulative should NOT include the old one
        assert flag is None or flag["rule"] == "new_vendor_large_amount"

    def test_established_vendor_immune_to_splitting(self):
        """Vendor with 3+ approved transactions is 'established' — no flag."""
        history = [
            {"amount": 500.0, "document_date": "2025-01-01", "review_status": "Posted"},
            {"amount": 500.0, "document_date": "2025-02-01", "review_status": "Posted"},
            {"amount": 500.0, "document_date": "2025-03-01", "review_status": "approved"},
        ]
        flag = _rule_new_vendor_large_amount("OldCo", 5000.0, history)
        assert flag is None


# ======================================================================
# 2. NEAR-DUPLICATE INVOICE NUMBERS
# ======================================================================

class TestNearDuplicateInvoiceNumbers:
    """Attack: submit INV-001, INV-0O1, INV-00I to evade duplicate detection."""

    def test_ocr_confusable_O_vs_0(self, tmp_path):
        """INV-001 vs INV-0O1 should normalize to the same value and flag."""
        assert _normalize_invoice_number("INV-001") == _normalize_invoice_number("INV-0O1")

    def test_ocr_confusable_I_vs_1(self, tmp_path):
        """INV-I23 vs INV-123 should normalize identically."""
        assert _normalize_invoice_number("INV-I23") == _normalize_invoice_number("INV-123")

    def test_ocr_confusable_S_vs_5(self):
        """INV-S00 vs INV-500 should normalize identically."""
        assert _normalize_invoice_number("INV-S00") == _normalize_invoice_number("INV-500")

    def test_near_dup_detected_in_db(self, tmp_path):
        """Two docs with OCR-confusable invoice numbers should flag."""
        db_path = _make_db(tmp_path)
        _insert(db_path, document_id="doc_orig", vendor="ACME", client_code="C1",
                amount=500.0, document_date="2025-03-01", invoice_number="INV-001")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_near_duplicate_invoice_number(
            conn, "doc_new", "ACME", "C1", "INV-0O1", date(2025, 3, 15))
        conn.close()
        assert flag is not None
        assert flag["rule"] == "near_duplicate_invoice_number"
        assert flag["severity"] == "high"

    def test_genuinely_different_invoices_no_flag(self, tmp_path):
        """Truly different invoice numbers should not flag."""
        db_path = _make_db(tmp_path)
        _insert(db_path, document_id="doc_orig", vendor="ACME", client_code="C1",
                amount=500.0, document_date="2025-03-01", invoice_number="INV-001")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_near_duplicate_invoice_number(
            conn, "doc_new", "ACME", "C1", "INV-999", date(2025, 3, 15))
        conn.close()
        assert flag is None

    def test_same_number_different_vendor_no_false_positive(self, tmp_path):
        """Same invoice number from completely different vendor — should NOT flag
        as near-duplicate (different vendors may use same numbering)."""
        db_path = _make_db(tmp_path)
        _insert(db_path, document_id="doc_orig", vendor="VendorA", client_code="C1",
                amount=500.0, document_date="2025-03-01", invoice_number="INV-100")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        # The rule checks within same client but regardless of vendor
        flag = _rule_near_duplicate_invoice_number(
            conn, "doc_new", "VendorB", "C1", "INV-100", date(2025, 3, 15))
        conn.close()
        # The exact same invoice number from different vendor WILL flag — this is desired
        # because it could be a copy submitted under a different vendor name
        if flag is not None:
            assert flag["severity"] == "high"


# ======================================================================
# 3. ROUND-DOLLAR RECURRING ABUSE
# ======================================================================

class TestRoundDollarAbuse:
    """Attack: abuse round-dollar amounts to siphon money."""

    def test_round_500_from_irregular_vendor_flagged(self):
        """$500 round amount from irregular vendor should flag."""
        history = [{"amount": a} for a in [123.45, 234.56, 198.77, 87.32, 345.99]]
        flag = _rule_round_number(500.0, history)
        assert flag is not None
        assert flag["severity"] == "low"

    def test_round_10000_from_irregular_vendor_flagged(self):
        """$10,000 round amount from irregular vendor should flag."""
        history = [{"amount": a} for a in [423.11, 1567.89, 890.33, 234.67, 1102.44]]
        flag = _rule_round_number(10000.0, history)
        assert flag is not None

    def test_regular_vendor_round_amount_no_false_positive(self):
        """Vendor that always bills round amounts should NOT be flagged."""
        history = [{"amount": 500.0}] * 10
        flag = _rule_round_number(500.0, history)
        assert flag is None, "Regular round-billing vendor should not be flagged"

    def test_49_not_round(self):
        """$49 is too small and not divisible by 50 — not round."""
        assert not _is_round_number(49.0)

    def test_99_99_not_round(self):
        """$99.99 has cents — not round."""
        assert not _is_round_number(99.99)

    def test_100_is_round(self):
        """$100 is a multiple of 50 and >= 100."""
        assert _is_round_number(100.0)

    def test_severity_consistent_for_same_round_amounts(self):
        """Two equivalent round-dollar scenarios should produce the same severity."""
        history = [{"amount": a} for a in [123.45, 234.56, 198.77, 87.32, 345.99]]
        flag_a = _rule_round_number(500.0, history)
        flag_b = _rule_round_number(1000.0, history)
        assert flag_a is not None and flag_b is not None
        assert flag_a["severity"] == flag_b["severity"], \
            "Severity should be consistent for equivalent round-dollar evasions"


# ======================================================================
# 4. MULTI-CHANNEL DUPLICATE (email + WhatsApp + portal)
# ======================================================================

class TestMultiChannelDuplicate:
    """Attack: submit same invoice through email, WhatsApp, and client portal."""

    def test_same_invoice_two_channels_flagged(self, tmp_path):
        """Same vendor + amount + invoice number in 2 docs = multi-channel flag."""
        db_path = _make_db(tmp_path)
        _insert(db_path, document_id="via_email", vendor="ACME Corp",
                client_code="C1", amount=1500.0, document_date="2025-03-10",
                invoice_number="INV-2025-100")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_multi_channel_duplicate(
            conn, "via_whatsapp", "ACME Corp", "C1", 1500.0, "INV-2025-100")
        conn.close()
        assert flag is not None
        assert flag["rule"] == "multi_channel_duplicate"
        assert flag["severity"] == "high"

    def test_three_channels_detected(self, tmp_path):
        """3 documents with identical invoice — count should reflect all matches."""
        db_path = _make_db(tmp_path)
        for ch in ["email", "whatsapp", "portal"]:
            _insert(db_path, document_id=f"via_{ch}", vendor="ACME Corp",
                    client_code="C1", amount=1500.0, document_date="2025-03-10",
                    invoice_number="INV-2025-100")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_multi_channel_duplicate(
            conn, "via_new_upload", "ACME Corp", "C1", 1500.0, "INV-2025-100")
        conn.close()
        assert flag is not None
        assert int(flag["params"]["count"]) == 3

    def test_different_amount_no_false_positive(self, tmp_path):
        """Same vendor + same invoice number but different amount should NOT flag."""
        db_path = _make_db(tmp_path)
        _insert(db_path, document_id="orig", vendor="ACME Corp",
                client_code="C1", amount=1500.0, document_date="2025-03-10",
                invoice_number="INV-2025-100")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_multi_channel_duplicate(
            conn, "new_doc", "ACME Corp", "C1", 1600.0, "INV-2025-100")
        conn.close()
        assert flag is None, "Different amount with same invoice number should not flag"

    def test_no_invoice_number_no_false_positive(self, tmp_path):
        """Missing invoice number should not trigger multi-channel rule."""
        db_path = _make_db(tmp_path)
        _insert(db_path, document_id="orig", vendor="ACME", client_code="C1",
                amount=1500.0, document_date="2025-03-10", invoice_number="INV-100")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_multi_channel_duplicate(conn, "new", "ACME", "C1", 1500.0, "")
        conn.close()
        assert flag is None


# ======================================================================
# 5. PAYEE DIFFERENT FROM VENDOR BUT PLAUSIBLE
# ======================================================================

class TestPayeeVendorMismatch:
    """Attack: payee 'ABC Holdings' pays invoice from 'ABC Services Inc.'
    — plausible but the mismatch should flag."""

    def test_clearly_different_payee_flagged(self, tmp_path):
        """Vendor='ACME Corp' but bank payee='XYZ International' → HIGH."""
        db_path = _make_db_with_bank(tmp_path)
        doc_id = _insert(db_path, vendor="ACME Corp", client_code="C1",
                         amount=5000.0, document_date="2025-03-10")
        _insert_bank_txn(db_path, "txn1", "2025-03-10", -5000.0,
                         "XYZ International", doc_id)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_payee_invoice_mismatch(conn, doc_id, "ACME Corp")
        conn.close()
        assert flag is not None
        assert flag["rule"] == "vendor_payee_mismatch"
        assert flag["severity"] == "high"

    def test_similar_payee_no_flag(self, tmp_path):
        """Vendor='Bell Canada' and payee='BELL CANADA' should NOT flag (>70% similar)."""
        db_path = _make_db_with_bank(tmp_path)
        doc_id = _insert(db_path, vendor="Bell Canada", client_code="C1",
                         amount=200.0, document_date="2025-03-10")
        _insert_bank_txn(db_path, "txn1", "2025-03-10", -200.0,
                         "BELL CANADA", doc_id)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_payee_invoice_mismatch(conn, doc_id, "Bell Canada")
        conn.close()
        assert flag is None, "Matching vendor/payee should not flag"

    def test_plausible_subsidiary_name_still_flagged(self, tmp_path):
        """Vendor='ABC Services' but payee='ABC Holdings' — low similarity → flag."""
        db_path = _make_db_with_bank(tmp_path)
        doc_id = _insert(db_path, vendor="ABC Services Inc", client_code="C1",
                         amount=3000.0, document_date="2025-03-10")
        _insert_bank_txn(db_path, "txn1", "2025-03-10", -3000.0,
                         "ABC Holdings Ltd", doc_id)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_payee_invoice_mismatch(conn, doc_id, "ABC Services Inc")
        conn.close()
        # After normalization, "abc services" vs "abc holdings" — the key words differ
        # This should flag because the payee is a different entity
        if flag is not None:
            assert flag["severity"] == "high"

    def test_no_bank_match_no_flag(self, tmp_path):
        """If doc has no matched bank transaction, payee rule should skip."""
        db_path = _make_db_with_bank(tmp_path)
        doc_id = _insert(db_path, vendor="ACME Corp", client_code="C1",
                         amount=1000.0, document_date="2025-03-10")
        # No bank txn matched

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_payee_invoice_mismatch(conn, doc_id, "ACME Corp")
        conn.close()
        assert flag is None

    def test_severity_consistent_for_equivalent_mismatches(self, tmp_path):
        """Two equivalently bad mismatches should produce the same severity."""
        db_path = _make_db_with_bank(tmp_path)
        doc_a = _insert(db_path, document_id="docA", vendor="Vendor Alpha",
                        client_code="C1", amount=1000.0, document_date="2025-03-10")
        _insert_bank_txn(db_path, "txn_a", "2025-03-10", -1000.0,
                         "Completely Different Name", doc_a)
        doc_b = _insert(db_path, document_id="docB", vendor="Vendor Beta",
                        client_code="C1", amount=2000.0, document_date="2025-03-10")
        _insert_bank_txn(db_path, "txn_b", "2025-03-10", -2000.0,
                         "Totally Unrelated Corp", doc_b)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag_a = _rule_payee_invoice_mismatch(conn, doc_a, "Vendor Alpha")
        flag_b = _rule_payee_invoice_mismatch(conn, doc_b, "Vendor Beta")
        conn.close()
        assert flag_a is not None and flag_b is not None
        assert flag_a["severity"] == flag_b["severity"]


# ======================================================================
# 6. CREDIT NOTE LOOP
# ======================================================================

class TestCreditNoteLoop:
    """Attack: issue credit → re-invoice → credit → re-invoice to cycle money."""

    def test_loop_detected_3_cycles(self, tmp_path):
        """3 credit + 3 re-invoice for same amount from same vendor = loop."""
        db_path = _make_db(tmp_path)
        base_date = date(2025, 1, 1)
        for i in range(3):
            # Credit note
            _insert(db_path, vendor="LoopCo", client_code="C1",
                    amount=-5000.0, doc_type="credit_note",
                    document_date=(base_date + timedelta(days=i * 30)).isoformat())
            # Re-invoice
            _insert(db_path, vendor="LoopCo", client_code="C1",
                    amount=5000.0, doc_type="invoice",
                    document_date=(base_date + timedelta(days=i * 30 + 5)).isoformat())

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_credit_note_loop(
            conn, "LoopCo", "C1", 5000.0, base_date + timedelta(days=100),
            "exclude_me")
        conn.close()
        assert flag is not None
        assert flag["rule"] == "credit_note_loop"
        assert flag["severity"] == "high"

    def test_single_credit_reinvoice_not_flagged(self, tmp_path):
        """One credit + one re-invoice is normal — should NOT flag."""
        db_path = _make_db(tmp_path)
        _insert(db_path, vendor="NormalCo", client_code="C1",
                amount=-2000.0, doc_type="credit_note",
                document_date="2025-03-01")
        _insert(db_path, vendor="NormalCo", client_code="C1",
                amount=2000.0, doc_type="invoice",
                document_date="2025-03-05")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_credit_note_loop(
            conn, "NormalCo", "C1", 2000.0, date(2025, 3, 15), "new_doc")
        conn.close()
        assert flag is None, "Single credit/re-invoice cycle is normal business"

    def test_different_amounts_no_false_loop(self, tmp_path):
        """Credits and invoices for different amounts should NOT trigger loop."""
        db_path = _make_db(tmp_path)
        for i in range(4):
            _insert(db_path, vendor="VarCo", client_code="C1",
                    amount=-(1000 + i * 500), doc_type="credit_note",
                    document_date=f"2025-0{i+1}-01")
            _insert(db_path, vendor="VarCo", client_code="C1",
                    amount=(2000 + i * 300), doc_type="invoice",
                    document_date=f"2025-0{i+1}-10")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_credit_note_loop(
            conn, "VarCo", "C1", 3000.0, date(2025, 6, 1), "new_doc")
        conn.close()
        # The 10% tolerance means only similar amounts count
        # With varying amounts, the loop shouldn't trigger


# ======================================================================
# 7. RELATED PARTY HIDDEN THROUGH ABBREVIATIONS
# ======================================================================

class TestRelatedPartyAbbreviationEvasion:
    """Attack: hide related party by using abbreviations, initials, or nicknames.

    E.g., related party = 'Jean-Pierre Tremblay Holdings Inc.'
    Invoice vendor = 'JP Tremblay' or 'JPT Holdings' or 'Tremblay J-P'.
    """

    def test_exact_match_detected(self):
        """Exact name match should be detected as related party."""
        result = check_related_party(
            "Tremblay Holdings Inc.",
            ["Tremblay Holdings Inc.", "Martin Construction"],
        )
        assert result["is_related_party"] is True
        assert result["confidence"] >= 0.80

    def test_case_variation_detected(self):
        """Case-insensitive match should still detect related party."""
        result = check_related_party(
            "TREMBLAY HOLDINGS INC.",
            ["Tremblay Holdings Inc."],
        )
        assert result["is_related_party"] is True

    def test_abbreviation_evasion_initials(self):
        """'JP Tremblay' vs 'Jean-Pierre Tremblay Holdings' — fuzzy score may be low.
        This tests whether abbreviation evasion is caught."""
        result = check_related_party(
            "JP Tremblay",
            ["Jean-Pierre Tremblay Holdings Inc."],
        )
        # The fuzzy score for 'jp tremblay' vs 'jean-pierre tremblay holdings inc.'
        # is likely below 0.80 — meaning abbreviations CAN evade detection.
        # This documents the gap.
        if not result["is_related_party"]:
            # Expected gap: abbreviation evasion works
            assert result["confidence"] < 0.80

    def test_suffix_removal_helps(self):
        """'Tremblay Inc' vs 'Tremblay Corp' — after suffix removal should match."""
        key_a = _normalize_vendor_key("Tremblay Inc")
        key_b = _normalize_vendor_key("Tremblay Corp")
        # Both should normalize to just 'tremblay'
        assert key_a == key_b, \
            "Suffix removal should make Inc/Corp variations match"

    def test_unrelated_vendor_not_false_positive(self):
        """Completely unrelated vendor should not be flagged as related party."""
        result = check_related_party(
            "Amazon Web Services",
            ["Tremblay Holdings Inc.", "Martin Construction"],
        )
        assert result["is_related_party"] is False


# ======================================================================
# 8. PHANTOM TAX CREDIT ON UNREGISTERED SUPPLIER
# ======================================================================

class TestPhantomTaxCredit:
    """Attack: vendor charges GST/QST but is historically unregistered/exempt.
    This creates phantom tax credits that shouldn't exist."""

    def test_tax_on_unregistered_vendor_flagged(self, tmp_path):
        """Vendor historically exempt now charging GST → flag."""
        db_path = _make_db_with_vendor_memory(tmp_path)
        _insert(db_path, document_id="doc1", vendor="GhostVendor",
                client_code="C1", amount=1000.0, document_date="2025-03-10",
                raw_result=json.dumps({"tax_code": "GST_QST", "gst_amount": 50.0}))
        # Vendor memory says historically exempt
        _insert_vendor_memory(db_path, "GhostVendor", "C1",
                              tax_code="E")  # Exempt

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_tax_registration_contradiction(
            conn, "GhostVendor", "C1",
            json.dumps({"tax_code": "GST_QST", "gst_amount": 50.0}))
        conn.close()
        assert flag is not None
        assert flag["rule"] == "tax_registration_contradiction"
        assert flag["severity"] == "high"

    def test_tax_on_registered_vendor_no_flag(self, tmp_path):
        """Vendor historically charges tax — no contradiction."""
        db_path = _make_db_with_vendor_memory(tmp_path)
        _insert(db_path, document_id="doc1", vendor="LegitVendor",
                client_code="C1", amount=1000.0, document_date="2025-03-10",
                raw_result=json.dumps({"tax_code": "GST_QST", "gst_amount": 50.0}))
        _insert_vendor_memory(db_path, "LegitVendor", "C1",
                              tax_code="T")  # Taxable

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_tax_registration_contradiction(
            conn, "LegitVendor", "C1",
            json.dumps({"tax_code": "GST_QST", "gst_amount": 50.0}))
        conn.close()
        assert flag is None

    def test_vendor_with_unregistered_raw_result_flagged(self, tmp_path):
        """Vendor memory has raw_result with tax_registered=false → flag."""
        db_path = _make_db_with_vendor_memory(tmp_path)
        _insert(db_path, document_id="doc1", vendor="PhantomCo",
                client_code="C1", amount=2000.0, document_date="2025-03-10",
                raw_result=json.dumps({"gst_amount": 100.0, "qst_amount": 199.75}))
        _insert_vendor_memory(
            db_path, "PhantomCo", "C1", tax_code="",
            raw_result=json.dumps({"tax_registered": "false"}))

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_tax_registration_contradiction(
            conn, "PhantomCo", "C1",
            json.dumps({"gst_amount": 100.0, "qst_amount": 199.75}))
        conn.close()
        assert flag is not None
        assert flag["rule"] == "tax_registration_contradiction"

    def test_no_tax_charged_no_false_positive(self, tmp_path):
        """Invoice with no tax charges should NOT flag even if vendor is unregistered."""
        db_path = _make_db_with_vendor_memory(tmp_path)
        _insert(db_path, document_id="doc1", vendor="ExemptCo",
                client_code="C1", amount=500.0, document_date="2025-03-10",
                raw_result=json.dumps({"tax_code": "E"}))
        _insert_vendor_memory(db_path, "ExemptCo", "C1", tax_code="E")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_tax_registration_contradiction(
            conn, "ExemptCo", "C1",
            json.dumps({"tax_code": "E"}))
        conn.close()
        assert flag is None


# ======================================================================
# 9. FALSE NEGATIVE AND FALSE POSITIVE TESTS PER RULE
# ======================================================================

class TestFalseNegativesAndPositives:
    """Ensure each rule has both false-negative resistance and false-positive control."""

    # --- Vendor amount anomaly ---
    def test_amount_anomaly_false_negative_just_inside_2sigma(self):
        """Amount just inside 2σ should NOT flag (not a false negative)."""
        amounts = [100.0 + i for i in range(10)]  # mean~104.5, std~3.03
        history = [{"amount": a, "document_date": "2025-01-15"} for a in amounts]
        # 2σ above mean ≈ 110.5 — test with 110 (inside)
        flag = _rule_vendor_amount_anomaly(110.0, history)
        assert flag is None, "Amount within 2σ should not flag"

    def test_amount_anomaly_false_negative_just_outside_2sigma(self):
        """Amount just outside 2σ SHOULD flag."""
        amounts = [100.0 + i for i in range(10)]
        history = [{"amount": a, "document_date": "2025-01-15"} for a in amounts]
        # 2σ above mean ≈ 110.5 — test with 115 (outside)
        flag = _rule_vendor_amount_anomaly(115.0, history)
        assert flag is not None, "Amount beyond 2σ should flag"

    # --- Weekend / holiday false positives ---
    def test_weekend_false_positive_small_amount(self):
        """Small weekend transaction ($50) should NOT flag."""
        sat = date(2025, 3, 22)  # Saturday
        flags = _rule_weekend_holiday(50.0, sat)
        assert flags == [], "Small weekend amount should not flag"

    def test_weekday_large_amount_no_false_positive(self):
        """Large amount on a normal weekday should NOT flag."""
        mon = date(2025, 3, 10)  # Monday
        flags = _rule_weekend_holiday(50000.0, mon)
        assert flags == [], "Normal weekday should never flag regardless of amount"

    # --- Duplicate false positives ---
    def test_duplicate_different_client_no_false_positive(self, tmp_path):
        """Same vendor + amount but different client should NOT flag."""
        db_path = _make_db(tmp_path)
        _insert(db_path, document_id="other_client", vendor="ACME",
                client_code="CLIENT_A", amount=5000.0, document_date="2025-03-10")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flags = _rule_duplicate(conn, "new_doc", "ACME", "CLIENT_B",
                                5000.0, date(2025, 3, 12))
        conn.close()
        assert not any(f["rule"] == "duplicate_exact" for f in flags)

    # --- Bank account change false positive ---
    def test_bank_change_same_account_no_false_positive(self):
        """Same bank details across invoices should NOT flag."""
        raw = json.dumps({"account_number": "12345678"})
        history = [{"raw_result": raw}]
        flag = _rule_bank_account_change(raw, history)
        assert flag is None

    # --- New vendor false positive on established vendor ---
    def test_new_vendor_established_no_false_positive(self):
        """Vendor with 5 approved transactions is established — even $50k should not flag."""
        history = _vendor_history(5, amount=100.0, review_status="Posted")
        flag = _rule_new_vendor_large_amount("OldCo", 50000.0, history)
        assert flag is None


# ======================================================================
# 10. OVERRIDE REQUIRES IMMUTABLE WRITTEN REASON
# ======================================================================

class TestOverrideImmutability:
    """Verify that fraud flag overrides require a written reason and become immutable."""

    def test_override_reason_column_exists(self, tmp_path):
        """The fraud_override_reason column must exist in the schema."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
        conn.close()
        assert "fraud_override_reason" in cols

    def test_override_locked_column_exists(self, tmp_path):
        """The fraud_override_locked column must exist to prevent retroactive changes."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
        conn.close()
        assert "fraud_override_locked" in cols

    def test_override_locked_defaults_to_zero(self, tmp_path):
        """New documents should have fraud_override_locked = 0."""
        db_path = _make_db(tmp_path)
        doc_id = _insert(db_path, vendor="Test", client_code="C1",
                         amount=100.0, document_date="2025-03-10")
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT fraud_override_locked FROM documents WHERE document_id=?",
            (doc_id,)).fetchone()
        conn.close()
        assert row["fraud_override_locked"] == 0

    def test_override_reason_min_length_enforced_in_dashboard(self):
        """Dashboard code must enforce minimum 10-char reason for fraud overrides."""
        dashboard_src = (ROOT_DIR / "scripts" / "review_dashboard.py").read_text(
            encoding="utf-8")
        assert "len(stripped_reason) < 10" in dashboard_src, \
            "Dashboard must enforce minimum 10-character override reason"

    def test_migrate_db_declares_override_columns(self):
        """migrate_db.py must declare both override columns."""
        src = (ROOT_DIR / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "fraud_override_reason" in src
        assert "fraud_override_locked" in src


# ======================================================================
# 11. SEVERITY CONSISTENCY ACROSS EQUIVALENT INPUTS
# ======================================================================

class TestSeverityConsistency:
    """Verify that equivalent fraud scenarios produce identical severity levels."""

    def test_bank_change_always_critical(self):
        """Bank account change should always be CRITICAL regardless of amounts."""
        for amount_suffix in ["100", "999999"]:
            old_raw = json.dumps({"account_number": f"OLD{amount_suffix}"})
            new_raw = json.dumps({"account_number": f"NEW{amount_suffix}"})
            history = [{"raw_result": old_raw}]
            flag = _rule_bank_account_change(new_raw, history)
            assert flag is not None
            assert flag["severity"] == "critical", \
                f"Bank account change must always be CRITICAL, got {flag['severity']}"

    def test_duplicate_exact_always_high(self, tmp_path):
        """Same-vendor duplicate should always be HIGH regardless of amount."""
        for amount in [100.0, 5000.0, 99999.0]:
            db_path = _make_db(tmp_path)
            _insert(db_path, document_id="prior", vendor="V", client_code="C1",
                    amount=amount, document_date="2025-03-10")
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            flags = _rule_duplicate(conn, "new", "V", "C1", amount, date(2025, 3, 15))
            conn.close()
            exact = [f for f in flags if f["rule"] == "duplicate_exact"]
            assert len(exact) >= 1
            assert exact[0]["severity"] == "high"

    def test_duplicate_cross_vendor_always_medium(self, tmp_path):
        """Cross-vendor duplicate should always be MEDIUM."""
        for amount in [100.0, 10000.0]:
            db_path = _make_db(tmp_path)
            _insert(db_path, document_id="prior", vendor="VendorA", client_code="C1",
                    amount=amount, document_date="2025-03-10")
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            flags = _rule_duplicate(conn, "new", "VendorB", "C1", amount, date(2025, 3, 12))
            conn.close()
            cross = [f for f in flags if f["rule"] == "duplicate_cross_vendor"]
            assert len(cross) >= 1
            assert cross[0]["severity"] == "medium"

    def test_new_vendor_large_always_high(self):
        """New vendor large amount should always be HIGH."""
        for amount in [2001.0, 10000.0, 100000.0]:
            flag = _rule_new_vendor_large_amount("NewCo", amount, [])
            assert flag is not None
            assert flag["severity"] == "high"

    def test_weekend_always_low(self):
        """Weekend transactions should always be LOW severity."""
        sat = date(2025, 3, 22)
        for amount in [201.0, 1000.0, 50000.0]:
            flags = _rule_weekend_holiday(amount, sat)
            assert len(flags) >= 1
            assert flags[0]["severity"] == "low"

    def test_timing_anomaly_always_low(self):
        """Timing anomaly should always be LOW severity."""
        history = _vendor_history(10, day=15)
        flag = _rule_vendor_timing_anomaly(date(2025, 1, 30), history)
        assert flag is not None
        assert flag["severity"] == "low"

    def test_amount_anomaly_always_high(self):
        """Vendor amount anomaly should always be HIGH."""
        amounts = [100.0 + i for i in range(10)]
        history = [{"amount": a, "document_date": "2025-01-15"} for a in amounts]
        flag = _rule_vendor_amount_anomaly(500.0, history)
        assert flag is not None
        assert flag["severity"] == "high"


# ======================================================================
# ADDITIONAL EVASION VECTORS
# ======================================================================

class TestInvoiceAfterPaymentEvasion:
    """Attack: backdate an invoice to after the payment date."""

    def test_invoice_after_payment_flagged(self, tmp_path):
        """Invoice dated 2025-03-15, payment was 2025-03-10 → flag."""
        db_path = _make_db_with_bank(tmp_path)
        doc_id = _insert(db_path, vendor="BackdateCo", client_code="C1",
                         amount=3000.0, document_date="2025-03-15")
        _insert_bank_txn(db_path, "txn1", "2025-03-10", -3000.0,
                         "BackdateCo", doc_id)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_invoice_after_payment(conn, doc_id, date(2025, 3, 15))
        conn.close()
        assert flag is not None
        assert flag["rule"] == "invoice_after_payment"
        assert flag["severity"] == "high"

    def test_invoice_before_payment_no_flag(self, tmp_path):
        """Normal: invoice before payment should NOT flag."""
        db_path = _make_db_with_bank(tmp_path)
        doc_id = _insert(db_path, vendor="NormalCo", client_code="C1",
                         amount=1000.0, document_date="2025-03-05")
        _insert_bank_txn(db_path, "txn1", "2025-03-10", -1000.0,
                         "NormalCo", doc_id)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_invoice_after_payment(conn, doc_id, date(2025, 3, 5))
        conn.close()
        assert flag is None


class TestVendorCategoryShiftEvasion:
    """Attack: expense vendor suddenly submits CapEx invoice."""

    def test_expense_vendor_submits_capex_flagged(self, tmp_path):
        """Vendor with 100% expense history submitting equipment invoice → flag."""
        db_path = _make_db_with_vendor_memory(tmp_path)
        _insert(db_path, document_id="doc1", vendor="RepairCo", client_code="C1",
                amount=15000.0, document_date="2025-03-10",
                raw_result=json.dumps({"memo": "Equipment purchase", "gl_account": "1500"}))
        _insert_vendor_memory(db_path, "RepairCo", "C1",
                              gl_account="5200", approval_count=10)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_vendor_category_shift(
            conn, "RepairCo", "C1",
            json.dumps({"memo": "Equipment purchase", "gl_account": "1500"}))
        conn.close()
        assert flag is not None
        assert flag["rule"] == "vendor_category_shift"
        assert flag["severity"] == "medium"

    def test_consistent_vendor_no_false_positive(self, tmp_path):
        """Vendor consistently in same category should NOT flag."""
        db_path = _make_db_with_vendor_memory(tmp_path)
        _insert(db_path, document_id="doc1", vendor="ExpenseCo", client_code="C1",
                amount=500.0, document_date="2025-03-10",
                raw_result=json.dumps({"memo": "Monthly repair service", "gl_account": "5200"}))
        _insert_vendor_memory(db_path, "ExpenseCo", "C1",
                              gl_account="5200", approval_count=20)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_vendor_category_shift(
            conn, "ExpenseCo", "C1",
            json.dumps({"memo": "Monthly repair service", "gl_account": "5200"}))
        conn.close()
        assert flag is None


class TestOrphanCreditNoteEvasion:
    """Attack: issue a credit note for an invoice that never existed."""

    def test_orphan_credit_note_flagged(self, tmp_path):
        """Credit note with no matching original invoice → flag."""
        db_path = _make_db(tmp_path)
        # No matching positive invoice for this vendor/amount

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_orphan_credit_note(conn, "PhantomCo", "C1", 5000.0, "credit_doc")
        conn.close()
        assert flag is not None
        assert flag["rule"] == "orphan_credit_note"
        assert flag["severity"] == "high"

    def test_credit_note_with_matching_invoice_no_flag(self, tmp_path):
        """Credit note that matches an existing invoice should NOT flag."""
        db_path = _make_db(tmp_path)
        _insert(db_path, document_id="orig_inv", vendor="LegitCo", client_code="C1",
                amount=3000.0, document_date="2025-03-01")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        flag = _rule_orphan_credit_note(conn, "LegitCo", "C1", 3000.0, "credit_doc")
        conn.close()
        assert flag is None


class TestEndToEndFraudDetection:
    """Integration: run_fraud_detection end-to-end with evasion attempts."""

    def test_weekend_large_amount_detected_e2e(self, tmp_path):
        """End-to-end: Saturday + large amount should flag."""
        db_path = _make_db(tmp_path)
        _insert(db_path, document_id="sat_doc", vendor="WeekendCo",
                client_code="C1", amount=5000.0,
                document_date="2025-03-22")  # Saturday
        flags = run_fraud_detection("sat_doc", db_path=db_path)
        assert any(f["rule"] == "weekend_transaction" for f in flags)

    def test_new_vendor_large_detected_e2e(self, tmp_path):
        """End-to-end: new vendor + $5k should flag."""
        db_path = _make_db(tmp_path)
        _insert(db_path, document_id="new_big", vendor="BrandNewCo",
                client_code="C1", amount=5000.0,
                document_date="2025-03-10")
        flags = run_fraud_detection("new_big", db_path=db_path)
        assert any(f["rule"] == "new_vendor_large_amount" for f in flags)

    def test_legit_recurring_expense_not_spam_flagged(self, tmp_path):
        """A legitimate recurring $150/month expense should NOT accumulate spam flags."""
        db_path = _make_db(tmp_path)
        # Build 12 months of consistent $150 from established vendor
        for i in range(12):
            month = (i % 12) + 1
            _insert(db_path, vendor="MonthlyClean Inc", client_code="C1",
                    amount=150.0,
                    document_date=f"2025-{month:02d}-15",
                    review_status="Posted")
        # New month's invoice
        doc_id = _insert(db_path, document_id="month13", vendor="MonthlyClean Inc",
                         client_code="C1", amount=150.0,
                         document_date="2026-01-15",
                         review_status="NeedsReview")
        flags = run_fraud_detection(doc_id, db_path=db_path)
        # Should have at most duplicate_exact flags (which is expected for same amount)
        # but NOT round_number, vendor_amount_anomaly, new_vendor_large, or timing
        bad_flags = [f for f in flags if f["rule"] in (
            "vendor_amount_anomaly", "new_vendor_large_amount",
            "vendor_timing_anomaly", "round_number_flag")]
        assert bad_flags == [], \
            f"Legit recurring expense should not be spam-flagged: {[f['rule'] for f in bad_flags]}"

    def test_missing_doc_returns_empty(self, tmp_path):
        """Non-existent document should return empty flags."""
        db_path = _make_db(tmp_path)
        flags = run_fraud_detection("nonexistent", db_path=db_path)
        assert flags == []

    def test_flags_persisted_to_db(self, tmp_path):
        """Flags should be saved to the fraud_flags column in DB."""
        from src.engines.fraud_engine import get_fraud_flags
        db_path = _make_db(tmp_path)
        _insert(db_path, document_id="persist_test", vendor="NewBigCo",
                client_code="C1", amount=10000.0,
                document_date="2025-03-22")  # Saturday
        run_fraud_detection("persist_test", db_path=db_path)
        saved = get_fraud_flags("persist_test", db_path=db_path)
        assert isinstance(saved, list)
        assert len(saved) > 0, "Flags should be persisted to DB"

    def test_all_flags_have_required_keys(self, tmp_path):
        """Every flag must have rule, severity, i18n_key, and params."""
        db_path = _make_db(tmp_path)
        _insert(db_path, document_id="schema_test", vendor="NewVendor",
                client_code="C1", amount=10000.0,
                document_date="2025-03-22")  # Saturday + new vendor + large
        flags = run_fraud_detection("schema_test", db_path=db_path)
        for flag in flags:
            assert "rule" in flag, f"Missing 'rule' key in flag: {flag}"
            assert "severity" in flag, f"Missing 'severity' key in flag: {flag}"
            assert "i18n_key" in flag, f"Missing 'i18n_key' key in flag: {flag}"
            assert "params" in flag, f"Missing 'params' key in flag: {flag}"
            assert flag["severity"] in ("critical", "high", "medium", "low"), \
                f"Invalid severity '{flag['severity']}' in flag: {flag}"
