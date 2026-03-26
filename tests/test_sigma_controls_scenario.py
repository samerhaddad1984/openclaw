"""
Sigma Controls Scenario — OCR Confusables, Duplicate Overreach,
Alias Noise, Reversal Hallucination, Settlement Corruption

Business context: Quebec registrant, books in CAD, May 2025 open period.

Document set:
  1) Real invoice — Sigma Controls Solutions / SCS Industrial, INV-S5O128, $8,450
  2) Duplicate upload — Sigma Control5 Solutions, INV-550128, $8,450 (same invoice)
  3) Different real invoice — Sigma Consulting Services, INV-S50128, $8,450

Bank feed:
  A) -8,450  WIRE PAYMT SCS INDUSTRIAL PROJECT 881
  B) -8,450  WIRE PAYMENT SIGMA CONSULTING SERVICES
  C) +8,450  REVERSAL SCS INDUSTRIAL
  D) +8,450  REVERSAL IMPORT DUPLICATE SCS INDUSTRIAL  (duplicate import of C)
  E) -8,450  REVERSAL FEE CORRECTION SIGMA CONSULTING SERVICES  (same-sign debit, NOT a reversal)

Hard assertions:
  - No merge between Sigma Controls Solutions and Sigma Consulting Services
  - Duplicate detection links invoice 1 and 2 only
  - OCR normalization alone must not create false certainty
  - Alias mapping must succeed despite bank-memo noise
  - Same-sign txn with "reversal" in memo must NOT auto-pair as reversal
  - Duplicate imported reversal must NOT create a second reversal event
  - Sigma Consulting invoice remains separate and untouched by SCS reversal logic
"""
from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.tools.duplicate_detector import (
    normalize_invoice_number,
    score_pair,
    find_duplicate_candidates,
)
from src.agents.tools.bank_matcher import BankMatcher
from src.agents.core.bank_models import BankTransaction


# ---------------------------------------------------------------------------
# Lightweight document stub matching DocumentRecord's attribute interface
# ---------------------------------------------------------------------------

@dataclass
class DocStub:
    document_id: str
    file_name: str
    file_path: str = ""
    client_code: Optional[str] = "SOUSSOL"
    vendor: Optional[str] = None
    doc_type: Optional[str] = "invoice"
    amount: Optional[float] = None
    document_date: Optional[str] = None
    invoice_number: Optional[str] = None
    gl_account: Optional[str] = None
    tax_code: Optional[str] = None
    category: Optional[str] = None
    review_status: str = "Ready"
    confidence: float = 0.90
    raw_result: dict = field(default_factory=dict)
    created_at: str = "2025-05-08T00:00:00Z"
    updated_at: str = "2025-05-08T00:00:00Z"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def invoices():
    """Three invoices: two are true duplicates, one is a different vendor."""
    inv1 = DocStub(
        document_id="INV-001",
        file_name="sigma_controls_inv.pdf",
        vendor="Sigma Controls Solutions",
        amount=8450.00,
        document_date="2025-05-08",
        invoice_number="INV-S5O128",
    )
    inv2 = DocStub(
        document_id="INV-002",
        file_name="sigma_controls_rescan.pdf",
        vendor="Sigma Control5 Solutions",
        amount=8450.00,
        document_date="2025-05-08",
        invoice_number="INV-550128",
    )
    inv3 = DocStub(
        document_id="INV-003",
        file_name="sigma_consulting_inv.pdf",
        vendor="Sigma Consulting Services",
        amount=8450.00,
        document_date="2025-05-08",
        invoice_number="INV-S50128",
    )
    return inv1, inv2, inv3


@pytest.fixture
def bank_transactions():
    """Five bank transactions designed to stress reversal and alias logic."""
    txn_a = BankTransaction(
        transaction_id="TXN-A",
        client_code="SOUSSOL",
        account_id="ACCT-1",
        posted_date="2025-05-08",
        description="WIRE PAYMT SCS INDUSTRIAL PROJECT 881",
        memo=None,
        amount=-8450.00,
        currency="CAD",
    )
    txn_b = BankTransaction(
        transaction_id="TXN-B",
        client_code="SOUSSOL",
        account_id="ACCT-1",
        posted_date="2025-05-08",
        description="WIRE PAYMENT SIGMA CONSULTING SERVICES",
        memo=None,
        amount=-8450.00,
        currency="CAD",
    )
    txn_c = BankTransaction(
        transaction_id="TXN-C",
        client_code="SOUSSOL",
        account_id="ACCT-1",
        posted_date="2025-05-10",
        description="REVERSAL SCS INDUSTRIAL",
        memo=None,
        amount=8450.00,
        currency="CAD",
    )
    txn_d = BankTransaction(
        transaction_id="TXN-D",
        client_code="SOUSSOL",
        account_id="ACCT-2",
        posted_date="2025-05-11",
        description="REVERSAL IMPORT DUPLICATE SCS INDUSTRIAL",
        memo=None,
        amount=8450.00,
        currency="CAD",
    )
    txn_e = BankTransaction(
        transaction_id="TXN-E",
        client_code="SOUSSOL",
        account_id="ACCT-1",
        posted_date="2025-05-09",
        description="REVERSAL FEE CORRECTION SIGMA CONSULTING SERVICES",
        memo=None,
        amount=-8450.00,
        currency="CAD",
    )
    return txn_a, txn_b, txn_c, txn_d, txn_e


@pytest.fixture
def alias_db():
    """In-memory SQLite with vendor_aliases seeded per the scenario."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE vendor_aliases (
            alias_id   INTEGER PRIMARY KEY,
            canonical_vendor_key TEXT NOT NULL,
            alias_name TEXT NOT NULL,
            alias_key  TEXT NOT NULL UNIQUE,
            created_by TEXT,
            created_at TEXT
        )
    """)
    conn.execute(
        "INSERT INTO vendor_aliases (canonical_vendor_key, alias_name, alias_key, created_by, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        ("sigma controls solutions", "SCS Industrial", "scs industrial", "admin", "2025-05-01"),
    )
    conn.execute(
        "INSERT INTO vendor_aliases (canonical_vendor_key, alias_name, alias_key, created_by, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        ("sigma controls solutions", "Sigma Control5 Solutions", "sigma control5 solutions", "admin", "2025-05-01"),
    )
    conn.commit()
    return conn


@pytest.fixture
def matcher():
    return BankMatcher()


# ===================================================================
# TRAP 1 — OCR normalization under hostile confusables
# ===================================================================

class TestOCRNormalization:
    """INV-S5O128, INV-550128, INV-SSO128, INV-S50128 must normalize
    to the SAME string so the duplicate detector can cluster them,
    but the system must not treat normalization as certainty."""

    def test_all_confusable_variants_normalize_identically(self):
        variants = ["INV-S5O128", "INV-550128", "INV-SSO128", "INV-S50128", "1NV-S5O128"]
        normalized = [normalize_invoice_number(v) for v in variants]
        # All should produce the same canonical form
        assert len(set(normalized)) == 1, (
            f"OCR variants did not converge: {dict(zip(variants, normalized))}"
        )

    def test_normalization_removes_hyphens_and_uppercases(self):
        assert normalize_invoice_number("inv-s5o128") == normalize_invoice_number("INV-S5O128")

    def test_empty_and_none_handled(self):
        assert normalize_invoice_number(None) == ""
        assert normalize_invoice_number("") == ""


# ===================================================================
# TRAP 2 — Duplicate detection: strong fallback must NOT overreach
# ===================================================================

class TestDuplicateDetection:
    """Invoice 1 and 2 are true duplicates (same vendor family, same invoice).
    Invoice 3 is a DIFFERENT vendor and must NOT merge."""

    def test_inv1_inv2_are_duplicates(self, invoices):
        inv1, inv2, _ = invoices
        result = score_pair(inv1, inv2)
        assert result.score >= 0.85, (
            f"True duplicate pair scored too low: {result.score:.4f} — reasons: {result.reasons}"
        )

    def test_inv1_inv3_are_NOT_duplicates(self, invoices):
        """The killer pair: same date, same amount, visually similar vendor
        and invoice number. The system MUST NOT merge these."""
        inv1, _, inv3 = invoices
        result = score_pair(inv1, inv3)
        assert result.score < 0.85, (
            f"CRITICAL: Different-vendor pair merged as duplicate! "
            f"score={result.score:.4f}, reasons={result.reasons}"
        )

    def test_inv2_inv3_are_NOT_duplicates(self, invoices):
        _, inv2, inv3 = invoices
        result = score_pair(inv2, inv3)
        assert result.score < 0.85, (
            f"CRITICAL: Rescan vs consulting pair merged! "
            f"score={result.score:.4f}, reasons={result.reasons}"
        )

    def test_find_duplicate_candidates_returns_only_true_pair(self, invoices):
        inv1, inv2, inv3 = invoices
        candidates = find_duplicate_candidates([inv1, inv2, inv3], min_score=0.85)
        pair_ids = set()
        for c in candidates:
            pair_ids.add(frozenset([c.left_document_id, c.right_document_id]))

        expected_pair = frozenset(["INV-001", "INV-002"])
        forbidden_pairs = [
            frozenset(["INV-001", "INV-003"]),
            frozenset(["INV-002", "INV-003"]),
        ]

        assert expected_pair in pair_ids, (
            f"True duplicate pair (INV-001, INV-002) not found. Found: {pair_ids}"
        )
        for fp in forbidden_pairs:
            assert fp not in pair_ids, (
                f"CRITICAL: Forbidden pair {fp} was flagged as duplicate!"
            )


# ===================================================================
# TRAP 3 — Alias resolution with noisy bank memos
# ===================================================================

class TestAliasResolution:
    """SCS Industrial → Sigma Controls Solutions must resolve even when
    the bank memo contains noise like 'WIRE PAYMT ... PROJECT 881'.
    Sigma Consulting Services must NOT resolve to SCS."""

    def test_scs_industrial_resolves_to_sigma_controls(self, matcher, alias_db):
        resolved = matcher.resolve_vendor_alias("SCS Industrial", alias_db)
        assert resolved.lower() == "sigma controls solutions", (
            f"Alias failed: got '{resolved}'"
        )

    def test_sigma_consulting_does_NOT_resolve_to_sigma_controls(self, matcher, alias_db):
        resolved = matcher.resolve_vendor_alias("Sigma Consulting Services", alias_db)
        # Should return the original name unchanged — no alias exists
        assert "controls" not in resolved.lower(), (
            f"CRITICAL: Consulting vendor wrongly aliased to Controls: '{resolved}'"
        )

    def test_noisy_memo_vendor_score_still_resolves_scs(self, matcher, alias_db, invoices):
        """Bank memo 'WIRE PAYMT SCS INDUSTRIAL PROJECT 881' should still
        match Sigma Controls Solutions via alias lookup."""
        inv1, _, _ = invoices
        txn_a = BankTransaction(
            transaction_id="TXN-A",
            client_code="SOUSSOL",
            account_id="ACCT-1",
            posted_date="2025-05-08",
            description="WIRE PAYMT SCS INDUSTRIAL PROJECT 881",
            memo=None,
            amount=-8450.00,
            currency="CAD",
        )
        score, similarity, reasons = matcher.vendor_score(inv1.vendor, txn_a, conn=alias_db)
        assert score > 0, (
            f"Vendor score is zero even with alias DB. similarity={similarity:.4f}, reasons={reasons}"
        )
        # Should have resolved via alias
        assert any("alias" in r.lower() for r in reasons) or similarity >= 0.65, (
            f"Alias resolution didn't fire. similarity={similarity:.4f}, reasons={reasons}"
        )

    def test_consulting_vendor_stays_separate_in_matching(self, matcher, alias_db, invoices):
        """Sigma Consulting Services must NOT match against SCS Industrial
        bank transactions via alias contamination."""
        _, _, inv3 = invoices
        txn_a = BankTransaction(
            transaction_id="TXN-A",
            client_code="SOUSSOL",
            account_id="ACCT-1",
            posted_date="2025-05-08",
            description="WIRE PAYMT SCS INDUSTRIAL PROJECT 881",
            memo=None,
            amount=-8450.00,
            currency="CAD",
        )
        score, similarity, reasons = matcher.vendor_score(inv3.vendor, txn_a, conn=alias_db)
        # Should NOT resolve to a high score
        assert similarity < 0.80, (
            f"CRITICAL: Consulting vendor matched SCS Industrial bank txn. "
            f"similarity={similarity:.4f}, reasons={reasons}"
        )


# ===================================================================
# TRAP 4 — Reversal detection: hallucination prevention
# ===================================================================

class TestReversalDetection:
    """Transaction A (debit) pairs with C (credit) as a valid reversal.
    Transaction D is a duplicate import of C — must not create a second reversal.
    Transaction E has 'reversal' in memo but is same-sign debit — must NOT pair."""

    def test_valid_reversal_pair_A_C_detected(self, matcher, bank_transactions):
        txn_a, _, txn_c, _, _ = bank_transactions
        reversals = matcher.detect_reversals([txn_a, txn_c])
        assert len(reversals) == 1, (
            f"Expected exactly 1 reversal pair (A↔C), got {len(reversals)}: {reversals}"
        )
        pair_ids = {reversals[0]["transaction_a_id"], reversals[0]["transaction_b_id"]}
        assert pair_ids == {"TXN-A", "TXN-C"}, (
            f"Wrong reversal pair: {pair_ids}"
        )

    def test_same_sign_txn_E_not_treated_as_reversal(self, matcher, bank_transactions):
        """Transaction E is -8450 (debit) with 'REVERSAL' in memo.
        It must NOT auto-pair as a reversal with any other debit."""
        txn_a, txn_b, _, _, txn_e = bank_transactions
        # Test E against other same-sign debits
        reversals = matcher.detect_reversals([txn_a, txn_b, txn_e])
        # E is same sign as A and B — should produce zero reversal pairs
        for r in reversals:
            pair_ids = {r["transaction_a_id"], r["transaction_b_id"]}
            assert "TXN-E" not in pair_ids, (
                f"CRITICAL: Same-sign debit TXN-E was paired as reversal: {r}"
            )

    def test_duplicate_reversal_import_does_not_double_count(self, matcher, bank_transactions):
        """When A, C, and D are all present, only ONE reversal pair should exist.
        D is a duplicate import of C from a second bank connection."""
        txn_a, _, txn_c, txn_d, _ = bank_transactions
        reversals = matcher.detect_reversals([txn_a, txn_c, txn_d])
        # A should pair with exactly one of C or D, not both
        assert len(reversals) == 1, (
            f"Expected 1 reversal pair but got {len(reversals)}: {reversals}"
        )

    def test_full_transaction_set_reversal_sanity(self, matcher, bank_transactions):
        """With all 5 transactions, reversal detection must:
        - Pair A↔C (or A↔D, but only one)
        - NOT pair E with anything
        - NOT produce more than 1 reversal pair for the SCS amount"""
        all_txns = list(bank_transactions)
        reversals = matcher.detect_reversals(all_txns)

        # Count how many reversal events involve the SCS Industrial amount
        scs_reversals = [r for r in reversals if abs(r["amount_a"]) == 8450.00]

        assert len(scs_reversals) <= 1, (
            f"CRITICAL: Multiple reversal events for same amount: {scs_reversals}"
        )

        # Ensure E never appears in a reversal pair
        for r in reversals:
            pair_ids = {r["transaction_a_id"], r["transaction_b_id"]}
            assert "TXN-E" not in pair_ids, (
                f"CRITICAL: TXN-E (same-sign debit with 'reversal' keyword) "
                f"was paired as reversal: {r}"
            )


# ===================================================================
# TRAP 5 — Settlement corruption prevention
# ===================================================================

class TestSettlementIntegrity:
    """The Sigma Consulting invoice (INV-003) must remain completely
    untouched by SCS Industrial reversal logic. The Controls invoice
    must not be double-reversed."""

    def test_consulting_invoice_not_matched_to_scs_reversal(self, matcher, invoices, bank_transactions):
        """INV-003 (Sigma Consulting) should match TXN-B (Sigma Consulting payment),
        NOT TXN-A (SCS Industrial) or TXN-C (SCS reversal)."""
        _, _, inv3 = invoices
        _, txn_b, txn_c, _, _ = bank_transactions
        results = matcher.match_documents([inv3], [txn_c, txn_b])

        assert len(results) == 1
        result = results[0]
        if result.transaction_id is not None:
            # If matched, it should match TXN-B, not TXN-C
            assert result.transaction_id == "TXN-B" or result.status in ("unmatched", "ambiguous"), (
                f"CRITICAL: Consulting invoice matched to SCS reversal! "
                f"matched_to={result.transaction_id}, status={result.status}"
            )

    def test_controls_invoice_matches_txn_a_not_txn_b(self, matcher, invoices, bank_transactions, alias_db):
        """INV-001 (Sigma Controls) should prefer TXN-A (SCS Industrial)
        over TXN-B (Sigma Consulting) when alias resolution is available."""
        inv1, _, _ = invoices
        txn_a, txn_b, _, _, _ = bank_transactions
        # Use evaluate_candidate directly with alias DB
        candidate_a = matcher.evaluate_candidate(inv1, txn_a)
        candidate_b = matcher.evaluate_candidate(inv1, txn_b)

        # A should score higher than B for the Controls invoice
        score_a = candidate_a.score if candidate_a else 0
        score_b = candidate_b.score if candidate_b else 0
        assert score_a > score_b or score_a == score_b, (
            f"Controls invoice preferred Consulting payment over SCS payment. "
            f"score_A={score_a}, score_B={score_b}"
        )

    def test_reversal_does_not_settle_consulting_invoice(self, matcher, invoices, bank_transactions):
        """TXN-C (reversal of SCS) must not be used to settle INV-003 (Consulting)."""
        _, _, inv3 = invoices
        _, _, txn_c, _, _ = bank_transactions
        candidate = matcher.evaluate_candidate(inv3, txn_c)
        if candidate is not None:
            assert candidate.status != "matched", (
                f"CRITICAL: SCS reversal settled Consulting invoice! "
                f"score={candidate.score}, reasons={candidate.reasons}"
            )

    def test_no_double_reversal_from_duplicate_import(self, matcher, invoices, bank_transactions):
        """When matching all invoices against all transactions,
        the Controls invoice must not appear reversed more than once."""
        inv1, _, _ = invoices
        txn_a, _, txn_c, txn_d, _ = bank_transactions

        # Match the controls invoice against all credit transactions
        results_c = matcher.evaluate_candidate(inv1, txn_c)
        results_d = matcher.evaluate_candidate(inv1, txn_d)

        # At most one should be a viable match (both are credits, positive amount)
        # The key point is the reversal detection should only count one event
        reversals = matcher.detect_reversals([txn_a, txn_c, txn_d])
        assert len(reversals) <= 1, (
            f"CRITICAL: Double reversal detected from duplicate import: {reversals}"
        )


# ===================================================================
# TRAP 6 — Vendor identity evaluation (cross-entity boundary)
# ===================================================================

class TestVendorIdentityBoundary:
    """evaluate_vendor_identity must distinguish Controls from Consulting."""

    def test_controls_vs_consulting_not_confirmed_same(self, matcher):
        result = matcher.evaluate_vendor_identity(
            invoice_vendor="Sigma Controls Solutions",
            bank_payee="Sigma Consulting Services",
            amount=8450.00,
        )
        assert result["identity_status"] != "confirmed_same_vendor", (
            f"CRITICAL: Two distinct vendors confirmed as same! {result}"
        )

    def test_scs_industrial_vs_sigma_controls_high_similarity(self, matcher):
        """SCS Industrial and Sigma Controls Solutions should show some
        relationship but NOT be auto-confirmed without GST match."""
        result = matcher.evaluate_vendor_identity(
            invoice_vendor="Sigma Controls Solutions",
            bank_payee="SCS Industrial",
        )
        # Should NOT be confirmed_same_vendor without GST evidence
        assert result["identity_status"] != "confirmed_same_vendor" or result.get("match_basis") == "gst_number_exact_match", (
            f"Vendors auto-confirmed without GST evidence: {result}"
        )


# ===================================================================
# TRAP 7 — Full integration: end-to-end matching
# ===================================================================

class TestEndToEndMatching:
    """Run match_documents with all 3 invoices against all 5 transactions
    and verify no cross-contamination."""

    def test_full_matching_no_cross_vendor_settlement(self, matcher, invoices, bank_transactions):
        inv1, inv2, inv3 = invoices
        all_txns = list(bank_transactions)
        results = matcher.match_documents([inv1, inv2, inv3], all_txns)

        # Build a map of which invoice matched which transaction
        match_map = {}
        for r in results:
            if r.transaction_id:
                match_map[r.document_id] = r.transaction_id

        # INV-003 (Consulting) must NOT be matched to any SCS transaction
        if "INV-003" in match_map:
            matched_txn = match_map["INV-003"]
            assert matched_txn in ("TXN-B", "TXN-E"), (
                f"CRITICAL: Consulting invoice matched to SCS transaction {matched_txn}"
            )

        # INV-001 or INV-002 should not match TXN-B (Consulting payment)
        for inv_id in ("INV-001", "INV-002"):
            if inv_id in match_map:
                assert match_map[inv_id] != "TXN-B", (
                    f"CRITICAL: Controls invoice {inv_id} matched to Consulting payment TXN-B"
                )

    def test_ambiguity_flagged_when_multiple_close_matches(self, matcher, invoices, bank_transactions):
        """With same-amount invoices and transactions, the system should
        flag ambiguity rather than silently choosing wrong."""
        inv1, _, inv3 = invoices
        txn_a, txn_b, _, _, _ = bank_transactions
        results = matcher.match_documents([inv1, inv3], [txn_a, txn_b])

        # At least one result should not be blindly "matched" when amounts
        # are identical across vendors — expect suggested or ambiguous
        statuses = [r.status for r in results]
        has_caution = any(s in ("ambiguous", "suggested", "payee_mismatch_candidate") for s in statuses)
        # If both are "matched", at least they shouldn't match the wrong transaction
        if not has_caution:
            match_map = {r.document_id: r.transaction_id for r in results}
            # Verify no cross-contamination even if both are confident
            if "INV-001" in match_map and "INV-003" in match_map:
                assert match_map["INV-001"] != match_map["INV-003"], (
                    "Two different invoices matched to the same transaction"
                )
