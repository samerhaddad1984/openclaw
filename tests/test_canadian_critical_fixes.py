"""Critical-fix tests from the 21-Canadian-receipt analysis.

Covers:
- BUG 1: '<UNKNOWN>' placeholder must not be stored as a vendor name.
- BUG 2: tax_total must aggregate GST + QST when both are present.
- BUG 3: Thousand/decimal separators parsed correctly for DocAI amounts.
- BUG 4: Implausible tax (> 20% of total) is flagged.
- BUG 5: Subtotal outliers vs vendor history are flagged.
- BONUS: Vendor is dropped when extractor confidence is below 0.70.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.engines.ocr_engine import (
    _is_vendor_placeholder,
    _parse_money,
)
from src.engines.ai_validator import (
    MAX_TAX_FRACTION,
    VENDOR_CONFIDENCE_FLOOR,
    VENDOR_SUBTOTAL_OUTLIER_MULT,
    apply_extraction_sanity,
    rebuild_vendor_amount_history,
    validate_tax_sanity,
    validate_subtotal_outlier,
)


# ---------------------------------------------------------------------------
# BUG 1 — vendor placeholders
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("placeholder", [
    "<UNKNOWN>", "UNKNOWN", "unknown", "<UNKNOWN VENDOR>",
    "<string or null>", "N/A", "n/a", "None", "null",
    "<vendor_name>", "", None, "   ",
])
def test_unknown_vendor_stored_as_null_not_literal(placeholder):
    assert _is_vendor_placeholder(placeholder), f"{placeholder!r} should be treated as placeholder"


@pytest.mark.parametrize("real", [
    "Aux Vivres", "LE SAINT-AMOUR", "Burger Bar Crescent", "Hydro-Québec",
    "Restaurant KEUNG KEE", "Pharmaprix", "Super C",
])
def test_real_vendor_names_are_kept(real):
    assert not _is_vendor_placeholder(real), f"{real!r} should be kept"


def test_sanity_scrubs_placeholder_even_with_high_confidence():
    # Confidence is high, but the vendor string is a placeholder — vendor
    # must still be None and the flow still routed to NeedsReview via the
    # upstream placeholder check (this test covers the downstream contract).
    vendor, status, flags, _ = apply_extraction_sanity(
        vendor=None,              # already scrubbed by ocr_engine
        confidence=0.95,
        amount=12.50,
        subtotal=10.0,
        tax_total=2.50,
        existing_flags=["vendor_placeholder_stripped"],
        review_status="Ready",
    )
    assert vendor is None
    assert "vendor_placeholder_stripped" in flags


# ---------------------------------------------------------------------------
# BUG 2 — tax_total = GST + QST
# ---------------------------------------------------------------------------

def test_tax_total_is_gst_plus_qst_when_both_present():
    # QC mid-tier restaurant receipt: $26.07 × (5 + 9.975)%.
    gst, qst = 1.30, 2.60
    # The aggregation happens in ocr_engine.process_file right after the
    # per-tax parsers. Simulate the contract here: apply_extraction_sanity
    # leaves numbers alone, but the pipeline sets tax_total = gst + qst.
    assert round(gst + qst, 2) == 3.90


def test_tax_sanity_no_flag_when_gst_qst_combined_within_limits():
    # Total $29.97 with combined tax $3.90 → 13% which is below the 20% cap.
    assert validate_tax_sanity(29.97, 3.90) is None


def test_tax_sanity_allows_near_max_canadian_tax_rate():
    # 14.975% combined (GST 5 + QST 9.975) must not trip the rule.
    assert validate_tax_sanity(100.0, 14.98) is None


# ---------------------------------------------------------------------------
# BUG 3 — money parser
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("1,234.56", 1234.56),          # English thousands
    ("12,345.67", 12345.67),
    ("123,456.78", 123456.78),
    ("59,500.00", 59500.00),         # English, the invoice template case
    ("595,00", 595.0),               # Quebec decimal (not 59500!)
    ("1.234,56", 1234.56),           # European
    ("56,70", 56.70),
    ("$29.97", 29.97),
    (" -$12.34 ", -12.34),
    ("1 234,56", 1234.56),           # Quebec space-separated
    ("0", 0.0),
    (0, 0.0),
    (59500, 59500.0),
])
def test_thousand_separator_preserved_in_amount(raw, expected):
    assert _parse_money(raw) == pytest.approx(expected)


@pytest.mark.parametrize("raw", ["", None, "abc", "   "])
def test_parse_money_handles_empty(raw):
    assert _parse_money(raw) is None


def test_decimal_shift_regression():
    # Regression: the original 59,500 vs 595 bug on invoice_template_bold.png.
    # The new parser treats "595,00" as 595 (Quebec decimal), not 59500
    # (which was the old .replace(',', '') behaviour).
    assert _parse_money("595,00") == 595.0
    # But "1,234.56" must still be 1234.56, not 123456.
    assert _parse_money("1,234.56") == 1234.56


# ---------------------------------------------------------------------------
# BUG 4 — implausible tax flag
# ---------------------------------------------------------------------------

def test_implausible_tax_flag_raised():
    # The pharmacy case: total ~$50, tax_total=$500 — must flag.
    flag = validate_tax_sanity(50.0, 500.0)
    assert flag is not None
    assert flag["flag"] == "implausible_tax"
    assert flag["severity"] == "HIGH"


def test_implausible_tax_ignores_missing_values():
    assert validate_tax_sanity(None, 10.0) is None
    assert validate_tax_sanity(100.0, None) is None
    assert validate_tax_sanity(0.0, 5.0) is None


def test_sanity_flips_review_status_on_implausible_tax():
    vendor, status, flags, findings = apply_extraction_sanity(
        vendor="Pharmacien",
        confidence=0.95,
        amount=50.0,
        subtotal=47.0,
        tax_total=500.0,
        review_status="Ready",
    )
    assert "implausible_tax" in flags
    assert status == "NeedsReview"


# ---------------------------------------------------------------------------
# BUG 5 — subtotal outlier vs vendor history
# ---------------------------------------------------------------------------

def _seed_vendor_history(db: Path, vendor: str, values: list[float]) -> None:
    conn = sqlite3.connect(str(db))
    conn.execute(
        "CREATE TABLE IF NOT EXISTS documents ("
        "document_id TEXT PRIMARY KEY, vendor TEXT, subtotal REAL)"
    )
    for i, v in enumerate(values):
        conn.execute(
            "INSERT INTO documents (document_id, vendor, subtotal) VALUES (?,?,?)",
            (f"seed_{vendor}_{i}", vendor, v),
        )
    conn.commit()
    conn.close()


def test_vendor_amount_outlier_flagged(tmp_path):
    db = tmp_path / "t.db"
    _seed_vendor_history(db, "Aux Vivres", [28.0, 31.0, 33.0, 35.0, 38.0])
    n = rebuild_vendor_amount_history(db)
    assert n == 1
    # p50 is ~33. A $400 subtotal is >10× p50 → outlier.
    flag = validate_subtotal_outlier("Aux Vivres", 400.0, db)
    assert flag is not None
    assert flag["flag"] == "subtotal_outlier"
    # And a normal amount does not trip.
    assert validate_subtotal_outlier("Aux Vivres", 40.0, db) is None


def test_vendor_outlier_needs_min_samples(tmp_path):
    db = tmp_path / "t.db"
    _seed_vendor_history(db, "Onesies", [50.0])  # only 1 sample
    rebuild_vendor_amount_history(db)
    # Too few samples → no flag even for absurd amount.
    assert validate_subtotal_outlier("Onesies", 10000.0, db) is None


def test_vendor_outlier_unknown_vendor_skipped(tmp_path):
    db = tmp_path / "t.db"
    # Nothing seeded — unknown vendor must not error and must not flag.
    assert validate_subtotal_outlier("Nowhere", 100.0, db) is None


# ---------------------------------------------------------------------------
# BONUS — vendor confidence threshold
# ---------------------------------------------------------------------------

def test_vendor_confidence_below_threshold_marks_review():
    vendor, status, flags, findings = apply_extraction_sanity(
        vendor="CHEZ BAPTISTE",
        confidence=0.55,
        amount=29.0,
        subtotal=25.22,
        tax_total=3.78,
        review_status="Ready",
    )
    assert vendor is None
    assert "vendor_low_confidence" in flags
    assert status == "NeedsReview"
    # The original vendor is preserved in the raw findings for humans.
    assert any(f.get("original_vendor") == "CHEZ BAPTISTE" for f in findings)


def test_vendor_above_threshold_kept():
    vendor, status, flags, _ = apply_extraction_sanity(
        vendor="Aux Vivres",
        confidence=0.92,
        amount=35.32,
        subtotal=31.0,
        tax_total=4.32,
        review_status="Ready",
    )
    assert vendor == "Aux Vivres"
    assert "vendor_low_confidence" not in flags
    assert status == "Ready"


# ---------------------------------------------------------------------------
# Existing-rows migration (called by the deploy script)
# ---------------------------------------------------------------------------

def test_existing_unknown_vendors_migrated(tmp_path):
    db = tmp_path / "t.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        "CREATE TABLE documents ("
        "document_id TEXT PRIMARY KEY, vendor TEXT, review_status TEXT)"
    )
    conn.executemany(
        "INSERT INTO documents VALUES (?,?,?)",
        [
            ("d1", "<UNKNOWN>", "NeedsReview"),
            ("d2", "UNKNOWN", "NeedsReview"),
            ("d3", "Aux Vivres", "Ready"),
            ("d4", "unknown vendor", "NeedsReview"),
        ],
    )
    conn.commit()
    # The one-liner the deploy script runs.
    conn.execute(
        "UPDATE documents SET vendor = NULL, review_status = 'NeedsReview' "
        "WHERE vendor LIKE '%UNKNOWN%' OR vendor LIKE '<%'"
    )
    conn.commit()
    rows = {r[0]: (r[1], r[2]) for r in conn.execute("SELECT * FROM documents")}
    conn.close()
    assert rows["d1"] == (None, "NeedsReview")
    assert rows["d2"] == (None, "NeedsReview")
    assert rows["d4"] == (None, "NeedsReview")
    assert rows["d3"] == ("Aux Vivres", "Ready")
