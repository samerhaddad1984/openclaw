"""
RED-TEAM: Bank Matcher Adversarial Tests
=========================================
Attack the bank reconciliation matching engine with hostile,
ambiguous, deceptive, and edge-case scenarios.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from src.agents.core.bank_models import BankTransaction, MatchCandidate, MatchResult
from src.agents.core.task_models import DocumentRecord
from src.agents.tools.bank_matcher import BankMatcher


def make_doc(
    doc_id="doc_001",
    vendor="Staples Canada",
    amount=100.00,
    date="2025-01-15",
    client_code="CLIENT1",
    doc_type="invoice",
    raw_result=None,
) -> DocumentRecord:
    return DocumentRecord(
        document_id=doc_id,
        file_name=f"{doc_id}.pdf",
        file_path=f"/docs/{doc_id}.pdf",
        client_code=client_code,
        vendor=vendor,
        doc_type=doc_type,
        amount=amount,
        document_date=date,
        gl_account="5200",
        tax_code="T",
        category="office",
        review_status="ReadyToPost",
        confidence=0.95,
        raw_result=raw_result or {},
    )


def make_txn(
    txn_id="txn_001",
    description="STAPLES CANADA",
    amount=-100.00,
    date="2025-01-15",
    client_code="CLIENT1",
    memo="",
    currency="CAD",
) -> BankTransaction:
    return BankTransaction(
        transaction_id=txn_id,
        client_code=client_code,
        account_id="acct_001",
        posted_date=date,
        description=description,
        memo=memo,
        amount=amount,
        currency=currency,
    )


# ===================================================================
# A. BASIC MATCHING ATTACKS
# ===================================================================

class TestBasicMatchingAttacks:
    """Attack the core matching logic."""

    def test_perfect_match(self):
        """Baseline: perfect match should score high."""
        matcher = BankMatcher()
        docs = [make_doc()]
        txns = [make_txn()]
        results = matcher.match_documents(docs, txns)
        assert len(results) == 1
        assert results[0].status in ("matched", "suggested")

    def test_sign_convention_invoice_vs_bank(self):
        """
        CRITICAL: Invoice amount is positive ($100), bank debit is
        negative (-$100). The matcher uses abs() — but does it work?
        """
        matcher = BankMatcher()
        doc = make_doc(amount=100.00)
        txn = make_txn(amount=-100.00)
        results = matcher.match_documents([doc], [txn])
        assert results[0].status != "unmatched", (
            "Positive invoice vs negative bank debit should match"
        )

    def test_both_positive_amounts(self):
        """Both doc and txn positive — should still match via abs()."""
        matcher = BankMatcher()
        doc = make_doc(amount=100.00)
        txn = make_txn(amount=100.00)
        results = matcher.match_documents([doc], [txn])
        assert results[0].amount_diff == 0.0

    def test_amount_exactly_at_tolerance(self):
        """
        DEFECT: Amount diff exactly at $5.00 (the max_amount_diff).
        The amount_score method uses `diff <= self.max_amount_diff` which
        should include $5.00. But the total score (amount+date+vendor)
        may still be below the suggest_threshold of 0.70.
        With $5 diff: amount_score = 0.08
        With same date: date_score = 0.25
        With same vendor: vendor_score = 0.25
        Total = 0.58 — BELOW 0.70 threshold → unmatched
        This means the $5 tolerance is effectively unreachable when
        combined with reasonable scores for other dimensions.
        """
        matcher = BankMatcher()
        doc = make_doc(amount=100.00)
        txn = make_txn(amount=-105.00)
        results = matcher.match_documents([doc], [txn])
        # The score is too low even though amount is within tolerance
        assert results[0].status == "unmatched", (
            "DEFECT CONFIRMED: $5 tolerance is unreachable — amount_score(5.00)=0.08 "
            "is too low to reach suggest_threshold when combined with other scores"
        )

    def test_amount_one_cent_over_tolerance(self):
        """Amount diff $5.01 — should NOT match."""
        matcher = BankMatcher()
        doc = make_doc(amount=100.00)
        txn = make_txn(amount=-105.01)
        results = matcher.match_documents([doc], [txn])
        assert results[0].status == "unmatched"

    def test_date_exactly_at_7_day_window(self):
        """Date delta exactly 7 days — should still match."""
        matcher = BankMatcher()
        doc = make_doc(date="2025-01-15")
        txn = make_txn(date="2025-01-22")
        results = matcher.match_documents([doc], [txn])
        assert results[0].status != "unmatched"

    def test_date_8_days_apart_no_match(self):
        """Date delta 8 days — should NOT match."""
        matcher = BankMatcher()
        doc = make_doc(date="2025-01-15")
        txn = make_txn(date="2025-01-23")
        results = matcher.match_documents([doc], [txn])
        assert results[0].status == "unmatched"


# ===================================================================
# B. VENDOR NAME ATTACKS
# ===================================================================

class TestVendorNameAttacks:
    """Attack vendor name normalization and matching."""

    def test_case_insensitive(self):
        matcher = BankMatcher()
        assert matcher.text_similarity("STAPLES CANADA", "staples canada") == 1.0

    def test_stop_words_stripped(self):
        """'Inc', 'Ltd', 'Corp' should be stripped."""
        matcher = BankMatcher()
        sim = matcher.text_similarity("Staples Canada Inc.", "STAPLES")
        assert sim >= 0.80

    def test_french_vendor_name_mismatch(self):
        """
        ATTACK: French vendor name on invoice vs English on bank statement.
        'Bureau en Gros' = 'Staples Canada'
        The matcher doesn't know this — it should NOT match.
        """
        matcher = BankMatcher()
        sim = matcher.text_similarity("Bureau en Gros", "Staples Canada")
        assert sim < 0.5, (
            "French/English vendor aliases should not auto-match "
            f"(similarity={sim})"
        )

    def test_abbreviated_vendor(self):
        """Bank truncates vendor to 'STAPLES CAN' — does it match?"""
        matcher = BankMatcher()
        sim = matcher.text_similarity("Staples Canada", "STAPLES CAN")
        assert sim >= 0.65  # Should still be reasonable

    def test_bank_description_noise(self):
        """
        Bank descriptions often include noise like card numbers, dates,
        reference numbers: 'VISA PURCHASE STAPLES CANADA #1234 01/15'
        """
        matcher = BankMatcher()
        # "visa" and "purchase" are stop words, but "#1234" and "01/15" are noise
        sim = matcher.text_similarity(
            "Staples Canada",
            "VISA PURCHASE STAPLES CANADA #1234 01/15",
        )
        assert sim >= 0.50

    def test_completely_different_vendors_no_match(self):
        """Completely different vendors should score very low."""
        matcher = BankMatcher()
        sim = matcher.text_similarity("Staples Canada", "Tim Hortons")
        assert sim < 0.3

    def test_unicode_accents_in_vendor(self):
        """
        ATTACK: French accented characters in vendor names.
        'Société de transport de Montréal' vs 'SOCIETE TRANSPORT MONTREAL'
        """
        matcher = BankMatcher()
        sim = matcher.text_similarity(
            "Société de transport de Montréal",
            "SOCIETE TRANSPORT MONTREAL",
        )
        # The normalize_text strips non-alphanumeric, which also strips accents
        # 'société' → 'soci t' (accented e becomes nothing) vs 'societe'
        # This is a DEFECT — accent stripping should normalize to base char
        if sim < 0.5:
            pytest.xfail(
                "DEFECT: Accent handling strips characters instead of normalizing. "
                f"Similarity={sim}"
            )

    def test_ampersand_normalization(self):
        """'M&M' should match 'M and M'."""
        matcher = BankMatcher()
        sim = matcher.text_similarity("M&M Food Market", "M AND M FOOD MARKET")
        assert sim >= 0.80

    def test_empty_vendor_vs_description(self):
        """Empty vendor name should return 0 similarity."""
        matcher = BankMatcher()
        assert matcher.text_similarity("", "STAPLES") == 0.0
        assert matcher.text_similarity(None, "STAPLES") == 0.0

    def test_very_long_vendor_name(self):
        """Extremely long vendor name — performance and correctness."""
        matcher = BankMatcher()
        long_name = "A" * 10000
        sim = matcher.text_similarity(long_name, "AAAA")
        assert isinstance(sim, float)

    def test_special_characters_only(self):
        """Vendor name with only special chars → empty after normalize."""
        matcher = BankMatcher()
        sim = matcher.text_similarity("###$$$%%%", "Normal Vendor")
        assert sim == 0.0


# ===================================================================
# C. MANY-TO-ONE / ONE-TO-MANY ATTACKS
# ===================================================================

class TestManyToOneMatching:
    """Attack the greedy matching algorithm."""

    def test_two_invoices_one_payment(self):
        """
        ATTACK: Two invoices ($50 each) for one bank payment ($100).
        The matcher is 1:1 — it cannot handle split payments.
        One invoice will match, the other will be orphaned.
        """
        matcher = BankMatcher()
        docs = [
            make_doc(doc_id="inv_1", amount=50.00, vendor="Vendor A"),
            make_doc(doc_id="inv_2", amount=50.00, vendor="Vendor A"),
        ]
        txns = [make_txn(txn_id="pay_1", amount=-100.00, description="VENDOR A")]
        results = matcher.match_documents(docs, txns)
        matched = [r for r in results if r.status != "unmatched"]
        unmatched = [r for r in results if r.status == "unmatched"]
        # Only one can match (1:1 constraint). The other is orphaned.
        # This is a known limitation but should be documented.
        assert len(matched) <= 1, "1:1 matcher should not double-match"
        assert len(unmatched) >= 1, "Second invoice should be orphaned"

    def test_one_invoice_multiple_payments(self):
        """
        ATTACK: One invoice ($200) paid in two installments ($100 each).
        Neither payment matches the invoice amount.
        """
        matcher = BankMatcher()
        docs = [make_doc(doc_id="inv_1", amount=200.00, vendor="Vendor B")]
        txns = [
            make_txn(txn_id="pay_1", amount=-100.00, description="VENDOR B PAYMENT 1"),
            make_txn(txn_id="pay_2", amount=-100.00, description="VENDOR B PAYMENT 2"),
        ]
        results = matcher.match_documents(docs, txns)
        # Amount diff = $100 > $5 tolerance → unmatched
        assert results[0].status == "unmatched", (
            "Split payment should not match single invoice"
        )

    def test_greedy_steals_better_match(self):
        """
        ATTACK: Greedy algorithm might give a good match to the wrong pair.

        Doc A ($100) and Doc B ($100.50) both match Txn X ($100).
        Doc A is the better match (exact amount) but if greedy picks
        Doc B first (alphabetically or by insertion order), Doc A gets orphaned.
        """
        matcher = BankMatcher()
        docs = [
            make_doc(doc_id="doc_B", amount=100.50, vendor="Same Vendor", date="2025-01-15"),
            make_doc(doc_id="doc_A", amount=100.00, vendor="Same Vendor", date="2025-01-15"),
        ]
        txns = [make_txn(txn_id="txn_X", amount=-100.00, description="SAME VENDOR")]
        results = matcher.match_documents(docs, txns)
        # The greedy sorts by score descending, so doc_A (exact amount) should win
        matched = [r for r in results if r.status != "unmatched"]
        if matched:
            assert matched[0].document_id == "doc_A", (
                "Greedy should prefer exact amount match"
            )

    def test_n_squared_performance(self):
        """
        ATTACK: The matcher is O(n*m) where n=docs, m=txns.
        With 500 docs and 500 txns, it evaluates 250,000 pairs.
        """
        import time
        matcher = BankMatcher()
        docs = [
            make_doc(doc_id=f"doc_{i}", amount=100.00 + i * 0.01,
                     vendor=f"Vendor {i}", date="2025-01-15")
            for i in range(500)
        ]
        txns = [
            make_txn(txn_id=f"txn_{i}", amount=-(100.00 + i * 0.01),
                     description=f"VENDOR {i}")
            for i in range(500)
        ]
        start = time.time()
        results = matcher.match_documents(docs, txns)
        elapsed = time.time() - start
        assert elapsed < 30.0, f"500x500 matching took {elapsed:.1f}s (>30s)"
        matched = [r for r in results if r.status != "unmatched"]
        assert len(matched) >= 400, f"Only {len(matched)}/500 matched"


# ===================================================================
# D. DATE FORMAT ATTACKS
# ===================================================================

class TestDateAttacks:
    """Attack date parsing with ambiguous and hostile formats."""

    def test_ambiguous_date_returns_none_without_language(self):
        """
        FIX 6: Ambiguous DD/MM vs MM/DD dates (both values <= 12)
        now return None when no language context is provided,
        preventing silent data corruption.
        """
        matcher = BankMatcher()
        d = matcher.parse_date("03/04/2025")
        assert d is None, "Ambiguous date without language should return None"

    def test_ambiguous_date_french_uses_dd_mm(self):
        """FIX 6: With language='fr', 03/04/2025 = April 3 (DD/MM)."""
        matcher = BankMatcher()
        d = matcher.parse_date("03/04/2025", language="fr")
        assert d is not None
        assert d.month == 4 and d.day == 3, "French dates use DD/MM"

    def test_ambiguous_date_english_uses_mm_dd(self):
        """FIX 6: With language='en', 03/04/2025 = March 4 (MM/DD)."""
        matcher = BankMatcher()
        d = matcher.parse_date("03/04/2025", language="en")
        assert d is not None
        assert d.month == 3 and d.day == 4, "English dates use MM/DD"

    def test_unambiguous_date_resolves_without_language(self):
        """FIX 6: 25/04/2025 is unambiguous (day=25 > 12) → April 25."""
        matcher = BankMatcher()
        d = matcher.parse_date("25/04/2025")
        assert d is not None
        assert d.month == 4 and d.day == 25

    def test_iso_date_correct(self):
        matcher = BankMatcher()
        d = matcher.parse_date("2025-01-15")
        assert d.year == 2025 and d.month == 1 and d.day == 15

    def test_invalid_date_returns_none(self):
        matcher = BankMatcher()
        assert matcher.parse_date("not-a-date") is None
        assert matcher.parse_date("") is None
        assert matcher.parse_date(None) is None

    def test_date_with_time(self):
        matcher = BankMatcher()
        d = matcher.parse_date("2025-01-15T14:30:00")
        assert d is not None
        assert d.day == 15

    def test_impossible_date(self):
        """Feb 30 — should return None."""
        matcher = BankMatcher()
        assert matcher.parse_date("2025-02-30") is None

    def test_leap_year_feb_29(self):
        matcher = BankMatcher()
        d = matcher.parse_date("2024-02-29")
        assert d is not None and d.day == 29

    def test_none_dates_produce_none_delta(self):
        matcher = BankMatcher()
        assert matcher.date_delta_days(None, "2025-01-15") is None
        assert matcher.date_delta_days("2025-01-15", None) is None


# ===================================================================
# E. CURRENCY MISMATCH ATTACKS
# ===================================================================

class TestCurrencyAttacks:
    """Attack currency handling."""

    def test_cad_vs_usd_penalty(self):
        """CAD doc matched to USD txn should be penalized."""
        matcher = BankMatcher()
        score, reasons = matcher.currency_score("CAD", "USD")
        assert score < 0, "Currency mismatch should produce negative score"

    def test_missing_currency_no_penalty(self):
        matcher = BankMatcher()
        score, _ = matcher.currency_score(None, "CAD")
        assert score == 0.0

    def test_same_currency_bonus(self):
        matcher = BankMatcher()
        score, _ = matcher.currency_score("CAD", "CAD")
        assert score > 0

    def test_currency_case_insensitive(self):
        matcher = BankMatcher()
        s1, _ = matcher.currency_score("cad", "CAD")
        s2, _ = matcher.currency_score("CAD", "CAD")
        assert s1 == s2


# ===================================================================
# F. CLIENT GATE ATTACKS
# ===================================================================

class TestClientGateAttacks:
    """Attack the client segregation gate."""

    def test_different_clients_blocked(self):
        """Documents from different clients must NOT match."""
        matcher = BankMatcher()
        doc = make_doc(client_code="CLIENT_A")
        txn = make_txn(client_code="CLIENT_B")
        candidate = matcher.evaluate_candidate(doc, txn)
        assert candidate is None, "Cross-client matching must be blocked"

    def test_missing_client_code_passes(self):
        """
        ATTACK: If either client code is missing, the gate passes.
        This could allow cross-client matching via null client codes.
        """
        matcher = BankMatcher()
        doc = make_doc(client_code="")
        txn = make_txn(client_code="CLIENT_B")
        allowed, _ = matcher.client_gate(doc, txn)
        assert allowed is True, (
            "Missing client code allows cross-client matching — is this intentional?"
        )

    def test_none_client_code_passes(self):
        """None client codes bypass the gate."""
        matcher = BankMatcher()
        doc = make_doc(client_code=None)
        txn = make_txn(client_code="CLIENT_B")
        allowed, _ = matcher.client_gate(doc, txn)
        assert allowed is True

    def test_case_insensitive_client(self):
        matcher = BankMatcher()
        doc = make_doc(client_code="client1")
        txn = make_txn(client_code="CLIENT1")
        allowed, _ = matcher.client_gate(doc, txn)
        assert allowed is True


# ===================================================================
# G. DUPLICATE / AMBIGUITY ATTACKS
# ===================================================================

class TestDuplicateAmbiguityAttacks:
    """Attack with duplicates and ambiguous matches."""

    def test_identical_invoices_same_vendor(self):
        """
        ATTACK: Two identical invoices from same vendor, same amount,
        same date. Only one bank transaction. Which invoice matches?
        """
        matcher = BankMatcher()
        docs = [
            make_doc(doc_id="inv_1", amount=100.00, vendor="Vendor", date="2025-01-15"),
            make_doc(doc_id="inv_2", amount=100.00, vendor="Vendor", date="2025-01-15"),
        ]
        txns = [make_txn(txn_id="txn_1", amount=-100.00, description="VENDOR")]
        results = matcher.match_documents(docs, txns)
        matched = [r for r in results if r.status != "unmatched"]
        assert len(matched) == 1, "Only one invoice should match one transaction"

    def test_same_amount_different_vendors(self):
        """Two vendors, same amount, same date — matcher must choose correctly."""
        matcher = BankMatcher()
        docs = [
            make_doc(doc_id="inv_a", amount=50.00, vendor="Alpha Co", date="2025-01-15"),
            make_doc(doc_id="inv_b", amount=50.00, vendor="Beta Inc", date="2025-01-15"),
        ]
        txns = [
            make_txn(txn_id="txn_a", amount=-50.00, description="ALPHA CO", date="2025-01-15"),
            make_txn(txn_id="txn_b", amount=-50.00, description="BETA INC", date="2025-01-15"),
        ]
        results = matcher.match_documents(docs, txns)
        for r in results:
            if r.document_id == "inv_a":
                assert r.transaction_id == "txn_a", "Alpha should match Alpha"
            elif r.document_id == "inv_b":
                assert r.transaction_id == "txn_b", "Beta should match Beta"

    def test_amount_with_tax_vs_without(self):
        """
        CRITICAL: Invoice shows $114.98 (with tax), bank shows $114.98.
        But another invoice shows $100.00 (pre-tax) for same vendor.
        The system might match the wrong one if amounts are stored
        inconsistently (some pre-tax, some with-tax).
        """
        matcher = BankMatcher()
        docs = [
            make_doc(doc_id="inv_pretax", amount=100.00, vendor="Shop"),
            make_doc(doc_id="inv_withtax", amount=114.98, vendor="Shop"),
        ]
        txns = [make_txn(txn_id="txn_1", amount=-114.98, description="SHOP")]
        results = matcher.match_documents(docs, txns)
        matched = [r for r in results if r.status != "unmatched"]
        if matched:
            assert matched[0].document_id == "inv_withtax", (
                "Should match the with-tax invoice (exact amount)"
            )


# ===================================================================
# H. EDGE CASE / HOSTILE INPUT ATTACKS
# ===================================================================

class TestHostileInputs:
    """Hostile and degenerate inputs."""

    def test_empty_documents_list(self):
        matcher = BankMatcher()
        results = matcher.match_documents([], [make_txn()])
        assert results == []

    def test_empty_transactions_list(self):
        matcher = BankMatcher()
        results = matcher.match_documents([make_doc()], [])
        assert len(results) == 1
        assert results[0].status == "unmatched"

    def test_both_empty(self):
        matcher = BankMatcher()
        results = matcher.match_documents([], [])
        assert results == []

    def test_none_amount_document(self):
        """Document with None amount should not crash."""
        matcher = BankMatcher()
        doc = make_doc(amount=None)
        txn = make_txn()
        results = matcher.match_documents([doc], [txn])
        assert len(results) == 1

    def test_zero_amount_match(self):
        """$0 invoice and $0 bank transaction — should they match?"""
        matcher = BankMatcher()
        doc = make_doc(amount=0.0)
        txn = make_txn(amount=0.0)
        results = matcher.match_documents([doc], [txn])
        # Zero-amount transactions are suspicious but shouldn't crash
        assert len(results) == 1

    def test_negative_document_amount(self):
        """FIX P1-1: Credit note (negative) SHOULD match a positive bank
        transaction (bank credit/refund). This is the credit_refund_match flow."""
        matcher = BankMatcher()
        doc = make_doc(amount=-50.00)  # Credit note
        txn = make_txn(amount=50.00)  # Bank credit (positive = refund)
        results = matcher.match_documents([doc], [txn])
        # FIX P1-1: credit note matching positive bank is now a valid credit_refund_match
        assert results[0].status in ("matched", "suggested", "ambiguous"), (
            "FIX P1-1: Credit note (-$50) should match bank refund (+$50) "
            "as a credit_refund_match."
        )

    def test_very_small_amounts(self):
        """Fractional cent amounts."""
        matcher = BankMatcher()
        doc = make_doc(amount=0.001)
        txn = make_txn(amount=-0.001)
        results = matcher.match_documents([doc], [txn])
        assert len(results) == 1

    def test_nan_amount_handling(self):
        """NaN in amount field."""
        matcher = BankMatcher()
        doc = make_doc(amount=float('nan'))
        txn = make_txn(amount=-100.00)
        # NaN comparisons are weird — abs(nan) = nan, nan - 100 = nan
        results = matcher.match_documents([doc], [txn])
        assert len(results) == 1  # Should not crash

    def test_inf_amount_handling(self):
        """Infinity in amount field."""
        matcher = BankMatcher()
        doc = make_doc(amount=float('inf'))
        txn = make_txn(amount=-100.00)
        results = matcher.match_documents([doc], [txn])
        assert len(results) == 1  # Should not crash
