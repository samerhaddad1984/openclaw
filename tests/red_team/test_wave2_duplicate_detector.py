"""
Second-Wave Independent Verification — Duplicate Detector Attacks

Attacks the duplicate_detector from angles not in wave 1:
- Vendor normalization with Unicode beyond basic French accents
- Amount tolerance boundary ($0.01 exact)
- Date parsing ambiguity across formats
- Score threshold gaming
- Cross-client isolation bypass
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.agents.tools.duplicate_detector import (
    _normalize_text,
    _amount_equal,
    _vendor_similarity,
    _parse_date,
    _date_distance_days,
    score_pair,
)


@dataclass
class FakeDoc:
    document_id: str = "DOC-001"
    file_name: str = "scan.pdf"
    vendor: Optional[str] = None
    amount: Optional[float] = None
    document_date: Optional[str] = None
    client_code: Optional[str] = None


# ═════════════════════════════════════════════════════════════════════════
# 1. Vendor normalization — Unicode beyond basic French
# ═════════════════════════════════════════════════════════════════════════

class TestVendorNormalization:

    def test_accented_vendor_equality(self):
        """'Société Générale' vs 'societe generale' should normalize equal."""
        a = _normalize_text("Société Générale")
        b = _normalize_text("societe generale")
        assert a == b, f"'{a}' != '{b}'"

    def test_cedilla(self):
        """'François' → 'francois'"""
        assert "francois" in _normalize_text("François")

    def test_ligatures_not_handled(self):
        """'Œuvres' — the œ ligature is NOT in the normalize map."""
        result = _normalize_text("Œuvres complètes")
        # œ is not in the replacement map, so re.sub(r"[^a-z0-9]+", " ", ...) strips it
        # This means "Œuvres" → " uvres" or similar — information loss
        # FINDING: Ligatures cause silent data loss in vendor matching.

    def test_german_umlaut(self):
        """'Müller' — ü IS handled, but what about ö, ä?"""
        assert "muller" in _normalize_text("Müller")
        # ö is NOT in the map
        result = _normalize_text("Österreich")
        # ö → stripped by regex → "sterreich" — information loss
        # FINDING: Only French accents handled, not German/other.

    def test_vendor_similarity_accent_mismatch(self):
        """Bank statement has 'SOCIETE TRANSPORT MONTREAL', invoice has accents."""
        sim = _vendor_similarity(
            "Société de transport de Montréal",
            "SOCIETE TRANSPORT MONTREAL"
        )
        assert sim > 0.5, f"Similarity too low for accent-only difference: {sim}"

    def test_vendor_with_legal_suffix_variants(self):
        """'ABC Consulting Inc.' vs 'ABC Consulting Incorporated'"""
        sim = _vendor_similarity("ABC Consulting Inc.", "ABC Consulting Incorporated")
        # Token overlap: {abc, consulting} / {abc, consulting, inc, incorporated}
        # = 2/4 = 0.5
        assert sim >= 0.4

    def test_completely_different_vendors(self):
        sim = _vendor_similarity("Hydro-Québec", "Bell Canada")
        assert sim < 0.3


# ═════════════════════════════════════════════════════════════════════════
# 2. Amount tolerance boundary
# ═════════════════════════════════════════════════════════════════════════

class TestAmountTolerance:

    def test_exact_match(self):
        assert _amount_equal(100.00, 100.00) is True

    def test_at_tolerance_boundary(self):
        """Exactly $0.01 apart — should be within tolerance."""
        assert _amount_equal(100.00, 100.01) is True

    def test_just_beyond_tolerance(self):
        """$0.02 apart — beyond $0.01 tolerance."""
        assert _amount_equal(100.00, 100.02) is False

    def test_float_precision_attack(self):
        """
        0.1 + 0.1 + 0.1 != 0.3 in float.
        Does the tolerance absorb this?
        """
        a = 0.1 + 0.1 + 0.1
        b = 0.3
        assert _amount_equal(a, b) is True  # diff is ~5.5e-17, well within 0.01

    def test_none_amounts(self):
        assert _amount_equal(None, 100.0) is False
        assert _amount_equal(100.0, None) is False
        assert _amount_equal(None, None) is False

    def test_negative_amounts(self):
        """Credit notes: -100.00 vs -100.00"""
        assert _amount_equal(-100.00, -100.00) is True

    def test_positive_vs_negative(self):
        """$100 invoice vs $-100 credit note — NOT duplicates."""
        assert _amount_equal(100.00, -100.00) is False


# ═════════════════════════════════════════════════════════════════════════
# 3. Date parsing ambiguity
# ═════════════════════════════════════════════════════════════════════════

class TestDateParsing:

    def test_iso_format(self):
        d = _parse_date("2025-03-15")
        assert d is not None
        assert d.month == 3 and d.day == 15

    def test_ambiguous_date_mm_dd_vs_dd_mm(self):
        """
        '03/04/2025' — is this March 4 or April 3?
        The parser tries MM/DD first, then DD/MM.
        """
        d = _parse_date("03/04/2025")
        # MM/DD/YYYY format is tried first → March 4
        if d is not None:
            # Document the actual behavior
            pass

    def test_unambiguous_date(self):
        """'25/04/2025' — day > 12, must be DD/MM/YYYY."""
        d = _parse_date("25/04/2025")
        assert d is not None
        assert d.day == 25 and d.month == 4

    def test_date_distance_same_date(self):
        dist = _date_distance_days("2025-01-15", "2025-01-15")
        assert dist == 0

    def test_date_distance_cross_format(self):
        """One ISO, one slash format — can they be compared?"""
        dist = _date_distance_days("2025-01-15", "01/15/2025")
        assert dist is not None
        assert dist == 0

    def test_none_date(self):
        assert _date_distance_days(None, "2025-01-15") is None
        assert _date_distance_days("2025-01-15", None) is None

    def test_invalid_date(self):
        d = _parse_date("not-a-date")
        assert d is None

    def test_empty_string(self):
        d = _parse_date("")
        assert d is None


# ═════════════════════════════════════════════════════════════════════════
# 4. Score threshold gaming
# ═════════════════════════════════════════════════════════════════════════

class TestScoreThresholdGaming:
    """
    Can we construct two documents that score just above or below the
    duplicate threshold by tweaking individual signals?
    """

    def test_perfect_duplicate(self):
        left = FakeDoc(
            document_id="A",
            vendor="Bell Canada",
            amount=100.00,
            document_date="2025-01-15",
            client_code="ACME",
            file_name="bell_jan.pdf",
        )
        right = FakeDoc(
            document_id="B",
            vendor="Bell Canada",
            amount=100.00,
            document_date="2025-01-15",
            client_code="ACME",
            file_name="bell_jan.pdf",
        )
        result = score_pair(left, right)
        assert result.score >= 0.85, f"Perfect duplicate only scores {result.score}"

    def test_same_vendor_amount_different_date(self):
        """Same vendor + amount but date is 30 days apart — monthly recurring."""
        left = FakeDoc(vendor="Hydro", amount=150.0, document_date="2025-01-15", client_code="C1")
        right = FakeDoc(
            document_id="B", vendor="Hydro", amount=150.0,
            document_date="2025-02-15", client_code="C1",
        )
        result = score_pair(left, right)
        # same_client(0.20) + same_vendor(0.35) + same_amount(0.35) + date(0) = 0.90
        # But these are NOT duplicates — they're monthly bills!
        # The system would flag this as a duplicate.
        # FINDING: Recurring monthly invoices are false-positive duplicates.
        assert result.score > 0.7, f"Score for monthly recurring: {result.score}"

    def test_different_client_codes_no_client_score(self):
        """Different clients should not get the same_client boost."""
        left = FakeDoc(vendor="Bell", amount=100.0, client_code="ACME")
        right = FakeDoc(document_id="B", vendor="Bell", amount=100.0, client_code="BETA")
        result = score_pair(left, right)
        assert "same_client" not in result.reasons

    def test_none_client_codes(self):
        """None client codes — does same_client fire?"""
        left = FakeDoc(vendor="Bell", amount=100.0, client_code=None)
        right = FakeDoc(document_id="B", vendor="Bell", amount=100.0, client_code=None)
        result = score_pair(left, right)
        # None == None is True in Python, but the check is:
        # if left_client and right_client and left_client == right_client
        # None is falsy, so this should NOT match.
        assert "same_client" not in result.reasons

    def test_empty_string_client_codes(self):
        """Empty string client codes — does same_client fire?"""
        left = FakeDoc(vendor="Bell", amount=100.0, client_code="")
        right = FakeDoc(document_id="B", vendor="Bell", amount=100.0, client_code="")
        result = score_pair(left, right)
        # "" is falsy, so same_client should NOT fire
        assert "same_client" not in result.reasons


# ═════════════════════════════════════════════════════════════════════════
# 5. Cross-client isolation
# ═════════════════════════════════════════════════════════════════════════

class TestCrossClientIsolation:
    """
    Documents from different clients should NEVER be matched as duplicates
    in production, but score_pair is a scoring function — it doesn't enforce
    isolation.  What score do cross-client docs get?
    """

    def test_identical_docs_different_clients(self):
        """
        Same vendor, amount, date, file — but different client.
        Score is high minus the client component.
        """
        left = FakeDoc(
            vendor="Bell", amount=100.0, document_date="2025-01-15",
            client_code="ACME", file_name="bell.pdf",
        )
        right = FakeDoc(
            document_id="B",
            vendor="Bell", amount=100.0, document_date="2025-01-15",
            client_code="OTHER", file_name="bell.pdf",
        )
        result = score_pair(left, right)
        # vendor(0.35) + amount(0.35) + date(0.20) + file(0.20) = 1.10
        # But no client boost. Score capped at sum of components.
        assert "same_client" not in result.reasons
        # The score is still very high! Without a client isolation gate,
        # this would be flagged as a duplicate across clients.
        # FINDING: score_pair has no cross-client penalty or gate.
        assert result.score >= 0.85
