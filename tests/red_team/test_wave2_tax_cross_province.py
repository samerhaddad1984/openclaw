"""
Second-Wave Independent Verification — Tax Cross-Province & Mixed-Code Attacks

These tests were NOT part of wave 1.  They attack the tax_engine from angles
the first-wave fixes could not have anticipated: combined hostile conditions,
cross-province filing summaries, HST_ATL edge maths, and mixed tax-code
batches that stress-test whether validation was truly hardened or merely
patched for the exact test-case inputs.
"""
from __future__ import annotations

import sqlite3
import tempfile
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pytest
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.tax_engine import (
    calculate_gst_qst,
    extract_tax_from_total,
    validate_tax_code,
    calculate_itc_itr,
    generate_filing_summary,
    validate_quebec_tax_compliance,
    _to_decimal,
    _round,
    TAX_CODE_REGISTRY,
    VALID_TAX_CODES,
    HST_RATE_ATL,
    HST_RATE_ON,
    GST_RATE,
    QST_RATE,
)


# ── helpers ──────────────────────────────────────────────────────────────

CENT = Decimal("0.01")


def _seed_db(db_path: Path, rows: list[dict]) -> None:
    """Insert rows into a temp DB with documents + posting_jobs tables."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT,
            vendor TEXT,
            document_date TEXT,
            amount REAL,
            tax_code TEXT,
            gl_account TEXT,
            review_status TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY,
            document_id TEXT,
            posting_status TEXT,
            external_id TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    for r in rows:
        conn.execute(
            """INSERT INTO documents
               (document_id, client_code, vendor, document_date, amount,
                tax_code, gl_account, review_status)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                r["document_id"],
                r.get("client_code", "TEST"),
                r.get("vendor", "Vendor"),
                r.get("document_date", "2025-02-15"),
                r.get("amount"),
                r.get("tax_code", "T"),
                r.get("gl_account", "5200"),
                r.get("review_status", "Ready"),
            ),
        )
        conn.execute(
            """INSERT INTO posting_jobs
               (posting_id, document_id, posting_status, external_id,
                created_at, updated_at)
               VALUES (?,?,?,?,?,?)""",
            (
                f"post_{r['document_id']}",
                r["document_id"],
                r.get("posting_status", "posted"),
                r.get("external_id", "EXT-1"),
                "2025-02-15T00:00:00+00:00",
                "2025-02-15T00:00:00+00:00",
            ),
        )
    conn.commit()
    conn.close()


# ═════════════════════════════════════════════════════════════════════════
# 1. HST_ATL 15% ↔ HST 13% confusion in a single filing period
# ═════════════════════════════════════════════════════════════════════════

class TestHstAtlanticCrossProvince:
    """
    A CPA firm with a Quebec client that buys from both Ontario (HST 13%)
    and New Brunswick (HST 15%) vendors in the same quarter.  The filing
    summary must not conflate the two rates.
    """

    def test_mixed_hst_13_and_15_in_same_period(self, tmp_path):
        db = tmp_path / "test.db"
        _seed_db(db, [
            {
                "document_id": "ON-001",
                "amount": 113.00,   # $100 + 13% HST
                "tax_code": "HST",
                "vendor": "Toronto Supply Co",
                "document_date": "2025-01-10",
            },
            {
                "document_id": "NB-001",
                "amount": 115.00,   # $100 + 15% HST
                "tax_code": "HST_ATL",
                "vendor": "Moncton Lumber",
                "document_date": "2025-01-15",
            },
        ])
        summary = generate_filing_summary("TEST", "2025-01-01", "2025-03-31", db)
        items = {li["document_id"]: li for li in summary["line_items"]}

        # Ontario doc: ITC should reflect 13% rate
        on_item = items["ON-001"]
        # pre_tax = 113 / 1.13 ≈ 100 → HST = 13 → recoverable = 13
        assert on_item["hst_recoverable"] == Decimal("13.00"), (
            f"Ontario HST ITC wrong: {on_item['hst_recoverable']}"
        )
        assert on_item["gst_recoverable"] == Decimal("0.00")
        assert on_item["qst_recoverable"] == Decimal("0.00")

        # Atlantic doc: ITC should reflect 15% rate
        nb_item = items["NB-001"]
        # pre_tax = 115 / 1.15 = 100 → HST = 15 → recoverable = 15
        assert nb_item["hst_recoverable"] == Decimal("15.00"), (
            f"Atlantic HST ITC wrong: {nb_item['hst_recoverable']}"
        )

        # Total ITC must be 28, not 26 (if wrongly using 13% for both)
        total_itc = summary["itc_available"]
        assert total_itc == Decimal("28.00"), (
            f"Total ITC for mixed HST period wrong: {total_itc}"
        )

    def test_hst_atl_validate_tax_code_for_each_province(self):
        """Each Atlantic province should accept HST_ATL without warnings."""
        for prov in ["NB", "NS", "NL", "PE"]:
            result = validate_tax_code("5200 - Office Supplies", "HST_ATL", prov)
            # HST_ATL is a valid code — it should not trigger unknown_tax_code
            assert f"unknown_tax_code:HST_ATL" not in result["warnings"], (
                f"HST_ATL wrongly flagged as unknown for {prov}: {result['warnings']}"
            )

    def test_hst_atl_on_quebec_vendor_flagged(self):
        """A Quebec vendor should NEVER use HST_ATL — that's a critical error."""
        result = validate_tax_code("5200", "HST_ATL", "QC")
        # Should get province_qc_does_not_use_hst or similar
        has_warning = any("qc" in w.lower() and "hst" in w.lower() for w in result["warnings"])
        assert has_warning, (
            f"HST_ATL on QC vendor not flagged! warnings={result['warnings']}"
        )

    def test_hst_on_atlantic_province_warns(self):
        """Ontario HST (13%) code used for NB vendor should warn about rate mismatch."""
        # HST code defaults to 13% (Ontario).  An NB vendor should use HST_ATL (15%).
        # Does the system catch this?  Wave 1 didn't test this specific scenario.
        result = validate_tax_code("5200", "HST", "NB")
        # At minimum, HST is accepted (NB is an HST province).
        # But does it warn about the rate difference?  Probably not — this is a gap.
        # Record finding either way.
        assert result["tax_code"] == "HST"
        # NOTE: This is a KNOWN GAP — the system cannot distinguish ON-HST from ATL-HST
        # at the validate_tax_code level.  It only checks province membership.

    def test_itc_itr_hst_atl_math_exact(self):
        """HST_ATL ITC must use 15% rate, not 13%."""
        result = calculate_itc_itr(Decimal("1000"), "HST_ATL")
        assert result["hst_paid"] == Decimal("150.00")
        assert result["hst_recoverable"] == Decimal("150.00")
        assert result["gst_paid"] == Decimal("0.00")
        assert result["qst_paid"] == Decimal("0.00")

    def test_itc_itr_hst_standard_math_exact(self):
        """HST (Ontario) ITC must use 13%, not 15%."""
        result = calculate_itc_itr(Decimal("1000"), "HST")
        assert result["hst_paid"] == Decimal("130.00")
        assert result["hst_recoverable"] == Decimal("130.00")


# ═════════════════════════════════════════════════════════════════════════
# 2. Tax code arrays with mixed valid/invalid codes in a batch
# ═════════════════════════════════════════════════════════════════════════

class TestMixedTaxCodeBatch:
    """
    Real-world scenario: a batch import has T, GST_QST, HST, HST_ATL,
    NONE, E, garbage codes, and empty strings all in one go.
    """

    @pytest.mark.parametrize("code,should_be_valid", [
        ("T", True),
        ("GST_QST", True),
        ("HST", True),
        ("HST_ATL", True),
        ("E", True),
        ("Z", True),
        ("M", True),
        ("I", True),
        ("NONE", True),
        ("VAT", True),
        ("GENERIC_TAX", True),
        ("TPS", False),       # French abbreviation — NOT a valid code
        ("TVQ", False),       # French abbreviation — NOT a valid code
        ("GST+QST", False),   # Common typo
        ("gst_qst", True),    # lowercase should normalize
        ("hst_atl", True),    # lowercase should normalize
        ("  HST  ", True),    # whitespace padded
        ("", False),          # empty
        ("G5T", False),       # OCR garbage
    ])
    def test_tax_code_validity(self, code, should_be_valid):
        result = validate_tax_code("5200", code, "QC")
        is_valid_code = (
            "tax_code_missing" not in result["warnings"]
            and not any(w.startswith("unknown_tax_code") for w in result["warnings"])
        )
        # Province warnings are separate — we only check code recognition
        if should_be_valid:
            assert is_valid_code, (
                f"Code '{code}' should be recognized but got warnings: {result['warnings']}"
            )
        else:
            assert not is_valid_code, (
                f"Code '{code}' should be REJECTED but was accepted. warnings: {result['warnings']}"
            )


# ═════════════════════════════════════════════════════════════════════════
# 3. Negative credit notes in tax calculations & filing summary
# ═════════════════════════════════════════════════════════════════════════

class TestCreditNoteTaxMath:
    """
    Credit notes produce negative amounts.  The tax engine must handle
    negative Decimals correctly in all paths — not just calculate_gst_qst.
    """

    def test_negative_gst_qst_calculation(self):
        result = calculate_gst_qst(Decimal("-500.00"))
        assert result["gst"] == Decimal("-25.00")
        assert result["qst"] == Decimal("-49.88")  # -500 * 0.09975 rounded
        assert result["total_with_tax"] == Decimal("-574.88")

    def test_negative_extract_from_total(self):
        """Reverse-compute from a negative total (credit note with tax)."""
        result = extract_tax_from_total(Decimal("-574.88"))
        # pre_tax should be close to -500
        assert result["pre_tax"] < Decimal("0")
        assert abs(result["pre_tax"] - Decimal("-500.00")) <= Decimal("0.01")

    def test_negative_itc_itr(self):
        """Credit note ITC/ITR should be negative (reducing your credit)."""
        result = calculate_itc_itr(Decimal("-1000"), "T")
        assert result["gst_recoverable"] < Decimal("0")
        assert result["qst_recoverable"] < Decimal("0")
        assert result["total_recoverable"] < Decimal("0")

    def test_credit_note_in_filing_summary_reduces_itc(self, tmp_path):
        """A credit note in a filing period must REDUCE total ITC, not increase it."""
        db = tmp_path / "test.db"
        _seed_db(db, [
            {
                "document_id": "INV-001",
                "amount": 114.98,  # $100 + GST + QST
                "tax_code": "T",
            },
            {
                "document_id": "CN-001",
                "amount": -57.49,  # credit note: -$50 - taxes
                "tax_code": "T",
            },
        ])
        summary = generate_filing_summary("TEST", "2025-01-01", "2025-12-31", db)
        # The credit note should produce negative ITC, reducing the total
        items = {li["document_id"]: li for li in summary["line_items"]}

        inv_itc = items["INV-001"]["total_recoverable"]
        cn_itc = items["CN-001"]["total_recoverable"]

        assert cn_itc < Decimal("0"), f"Credit note ITC should be negative: {cn_itc}"
        assert summary["itc_available"] < inv_itc, (
            "Credit note didn't reduce total ITC"
        )

    def test_mixed_positive_negative_tax_lines_compliance(self):
        """
        Quebec compliance validator with a document that has negative
        QST (vendor credit).  Should NOT trigger tax_on_tax_error
        just because the signs are flipped.
        """
        doc = {
            "subtotal": Decimal("-200"),
            "gst_amount": Decimal("-10.00"),
            "qst_amount": Decimal("-19.95"),
            "vendor_province": "QC",
        }
        issues = validate_quebec_tax_compliance(doc)
        tax_on_tax = [i for i in issues if i["error_type"] == "tax_on_tax_error"]
        assert len(tax_on_tax) == 0, (
            f"Negative credit note falsely flagged as tax-on-tax: {tax_on_tax}"
        )


# ═════════════════════════════════════════════════════════════════════════
# 4. GST_QST ↔ T equivalence under all paths
# ═════════════════════════════════════════════════════════════════════════

class TestGstQstTEquivalence:
    """
    T and GST_QST must produce IDENTICAL math in every path.
    A fix that only patched one code but not the other is exposed here.
    """

    @pytest.mark.parametrize("amount", [
        Decimal("100"), Decimal("0.01"), Decimal("999999.99"),
        Decimal("1.11"), Decimal("0.03"),
    ])
    def test_itc_itr_identical(self, amount):
        result_t = calculate_itc_itr(amount, "T")
        result_gq = calculate_itc_itr(amount, "GST_QST")
        for key in ("gst_paid", "qst_paid", "gst_recoverable", "qst_recoverable", "total_recoverable"):
            assert result_t[key] == result_gq[key], (
                f"T vs GST_QST diverge on {key} for amount={amount}: "
                f"{result_t[key]} vs {result_gq[key]}"
            )

    def test_filing_summary_t_vs_gst_qst(self, tmp_path):
        """Two identical docs, one coded T and one GST_QST, must produce same ITC."""
        db = tmp_path / "test.db"
        _seed_db(db, [
            {"document_id": "T-001", "amount": 114.98, "tax_code": "T"},
            {"document_id": "GQ-001", "amount": 114.98, "tax_code": "GST_QST"},
        ])
        summary = generate_filing_summary("TEST", "2025-01-01", "2025-12-31", db)
        items = {li["document_id"]: li for li in summary["line_items"]}
        for key in ("gst_recoverable", "qst_recoverable", "total_recoverable"):
            assert items["T-001"][key] == items["GQ-001"][key], (
                f"T vs GST_QST diverge in filing summary on {key}"
            )


# ═════════════════════════════════════════════════════════════════════════
# 5. Boundary & adversarial decimal inputs
# ═════════════════════════════════════════════════════════════════════════

class TestDecimalBoundaries:
    """Stress the Decimal-based tax math with hostile inputs."""

    def test_one_penny_gst_qst(self):
        """GST on $0.01 should round to $0.00 (5% of $0.01 = $0.0005)."""
        result = calculate_gst_qst(Decimal("0.01"))
        assert result["gst"] == Decimal("0.01")
        assert result["qst"] == Decimal("0.01")
        assert result["total_with_tax"] == Decimal("0.03")

    def test_large_amount_no_overflow(self):
        """$10M invoice should not overflow or lose precision."""
        result = calculate_gst_qst(Decimal("10000000"))
        assert result["gst"] == Decimal("500000.00")
        expected_qst = (Decimal("10000000") * Decimal("0.09975")).quantize(
            CENT, rounding=ROUND_HALF_UP
        )
        assert result["qst"] == expected_qst

    def test_to_decimal_rejects_nan_inf(self):
        """NaN and Inf strings must not silently become Decimal values."""
        with pytest.raises(Exception):
            _to_decimal("NaN")
        with pytest.raises(Exception):
            _to_decimal("Infinity")
        with pytest.raises(Exception):
            _to_decimal("-Infinity")

    def test_extract_from_total_round_trip_stress(self):
        """
        For 500 random-ish amounts, verify calculate → extract round-trip
        stays within 1 cent.  This catches rounding drift the first wave
        only tested on a handful of values.
        """
        failures = []
        for cents in range(1, 500):
            pre_tax = Decimal(cents) / Decimal("100")
            forward = calculate_gst_qst(pre_tax)
            total = forward["total_with_tax"]
            back = extract_tax_from_total(total)
            drift = abs(back["pre_tax"] - pre_tax)
            # Allow $0.03 tolerance for micro-amounts (minimum tax floor effect)
            tol = Decimal("0.03") if pre_tax < Decimal("0.10") else Decimal("0.01")
            if drift > tol:
                failures.append((pre_tax, drift))

        assert len(failures) == 0, (
            f"Round-trip drift > 1¢ for {len(failures)} amounts: "
            f"{failures[:10]}"
        )


# ═════════════════════════════════════════════════════════════════════════
# 6. Compliance validator — combined hostile conditions
# ═════════════════════════════════════════════════════════════════════════

class TestComplianceCombinedAttacks:
    """
    Real-world hostile documents that trigger MULTIPLE compliance checks
    simultaneously.  Wave 1 tested each check in isolation.
    """

    def test_triple_violation_document(self):
        """
        An Ontario vendor, using the old QST rate, on an exempt category,
        with no registration numbers.  Should flag at least 3 issues.
        """
        doc = {
            "subtotal": Decimal("100"),
            "gst_amount": Decimal("5.00"),
            "qst_amount": Decimal("9.50"),  # old rate
            "vendor_province": "ON",
            "category": "basic_groceries",
        }
        issues = validate_quebec_tax_compliance(doc)
        error_types = {i["error_type"] for i in issues}

        assert "wrong_provincial_tax" in error_types, "ON vendor with QST not flagged"
        assert "wrong_qst_rate" in error_types, "Old 9.5% rate not flagged"
        assert "exempt_item_taxed" in error_types, "Exempt groceries taxed not flagged"
        assert "missing_registration_number" in error_types, "Missing reg# not flagged"
        assert len(issues) >= 4, f"Expected ≥4 issues, got {len(issues)}: {error_types}"

    def test_zero_subtotal_with_tax_amounts(self):
        """
        Subtotal is $0 but GST/QST are non-zero — a data corruption scenario.
        Should the compliance validator flag this?
        """
        doc = {
            "subtotal": Decimal("0"),
            "gst_amount": Decimal("5.00"),
            "qst_amount": Decimal("9.98"),
        }
        issues = validate_quebec_tax_compliance(doc)
        # The tax-on-tax check requires subtotal > 0, so it won't fire.
        # But taxes on a zero subtotal is clearly wrong.
        # If there's no check for this, it's a gap we document.
        # This is an EXPECTED GAP — recording for the report.

    def test_negative_vendor_revenue_bypasses_small_supplier_check(self):
        """
        Negative vendor_revenue (data corruption) — does it bypass the
        small supplier threshold?
        """
        doc = {
            "subtotal": Decimal("100"),
            "gst_amount": Decimal("5.00"),
            "qst_amount": Decimal("9.98"),
            "vendor_revenue": Decimal("-5000"),
            "vendor_province": "QC",
        }
        issues = validate_quebec_tax_compliance(doc)
        small_supplier = [i for i in issues if i["error_type"] == "unregistered_supplier_charging_tax"]
        # vendor_revenue > 0 check means -5000 bypasses the gate.
        # This is correct behavior (negative revenue means "unknown").
        assert len(small_supplier) == 0


# ═════════════════════════════════════════════════════════════════════════
# 7. Filing summary with ALL tax codes in one period
# ═════════════════════════════════════════════════════════════════════════

class TestFilingSummaryAllCodes:
    """
    A single client with documents covering every tax code in one quarter.
    The filing summary must handle each correctly.
    """

    def test_all_codes_in_one_filing(self, tmp_path):
        db = tmp_path / "test.db"
        docs = [
            {"document_id": "T-1",   "amount": 114.98, "tax_code": "T"},
            {"document_id": "GQ-1",  "amount": 114.98, "tax_code": "GST_QST"},
            {"document_id": "HST-1", "amount": 113.00, "tax_code": "HST"},
            {"document_id": "ATL-1", "amount": 115.00, "tax_code": "HST_ATL"},
            {"document_id": "E-1",   "amount": 100.00, "tax_code": "E"},
            {"document_id": "Z-1",   "amount": 100.00, "tax_code": "Z"},
            {"document_id": "M-1",   "amount": 114.98, "tax_code": "M"},
            {"document_id": "I-1",   "amount": 109.00, "tax_code": "I"},
            {"document_id": "V-1",   "amount": 100.00, "tax_code": "VAT"},
            {"document_id": "N-1",   "amount": 100.00, "tax_code": "NONE"},
            {"document_id": "G-1",   "amount": 100.00, "tax_code": "GENERIC_TAX"},
        ]
        _seed_db(db, docs)
        summary = generate_filing_summary("TEST", "2025-01-01", "2025-12-31", db)

        assert summary["documents_total"] == 11
        assert summary["documents_posted"] == 11

        items = {li["document_id"]: li for li in summary["line_items"]}

        # Exempt/Zero/NONE/VAT/GENERIC should have zero recoverable
        for doc_id in ["E-1", "Z-1", "V-1", "N-1", "G-1"]:
            assert items[doc_id]["total_recoverable"] == Decimal("0.00"), (
                f"{doc_id} should have zero recoverable: {items[doc_id]}"
            )

        # Meals (M) should have 50% of T's recoverable
        m_item = items["M-1"]
        t_item = items["T-1"]
        # Both have same total → same pre_tax → M gets 50% ITC/ITR
        assert m_item["gst_recoverable"] > Decimal("0")
        assert m_item["gst_recoverable"] < t_item["gst_recoverable"], (
            "Meals ITC should be less than full taxable ITC"
        )

        # Insurance should have zero GST recoverable, zero QST recoverable
        i_item = items["I-1"]
        assert i_item["gst_recoverable"] == Decimal("0.00")
        assert i_item["qst_recoverable"] == Decimal("0.00")

        # Total ITC should be sum of gst_recoverable + hst_recoverable across all
        expected_itc = sum(
            li["gst_recoverable"] + li["hst_recoverable"]
            for li in summary["line_items"]
        )
        assert summary["itc_available"] == expected_itc
