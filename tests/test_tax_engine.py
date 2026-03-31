"""
tests/test_tax_engine.py — pytest tests for src/engines/tax_engine.py

All tests are deterministic (no DB, no AI).  The only tests that touch the
filesystem are those for generate_filing_summary(), and they use a tmp_path
in-memory SQLite DB.
"""
from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from src.engines.tax_engine import (
    CENT,
    COMBINED_GST_QST,
    GST_RATE,
    HST_PROVINCES,
    HST_RATE_ATL,
    HST_RATE_ON,
    QST_RATE,
    TAX_CODE_REGISTRY,
    VALID_TAX_CODES,
    _itc_itr_from_total,
    _normalize_code,
    _registry_entry,
    _round,
    calculate_gst_qst,
    calculate_itc_itr,
    extract_tax_from_total,
    generate_filing_summary,
    validate_tax_code,
)

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

D = Decimal  # short alias


def _make_db(path: Path) -> None:
    """Create a minimal otocpa_agent.db for filing summary tests."""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE documents (
            document_id   TEXT PRIMARY KEY,
            client_code   TEXT,
            vendor        TEXT,
            document_date TEXT,
            amount        TEXT,
            tax_code      TEXT,
            gl_account    TEXT,
            review_status TEXT
        );
        CREATE TABLE posting_jobs (
            rowid         INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id   TEXT,
            target_system TEXT,
            posting_status TEXT,
            external_id   TEXT,
            created_at    TEXT,
            updated_at    TEXT
        );
    """)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Constants sanity checks
# ---------------------------------------------------------------------------

class TestConstants:
    def test_gst_rate_exact(self):
        assert GST_RATE == D("0.05")

    def test_qst_rate_exact(self):
        assert QST_RATE == D("0.09975")

    def test_combined_rate(self):
        assert COMBINED_GST_QST == D("0.14975")

    def test_hst_on(self):
        assert HST_RATE_ON == D("0.13")

    def test_hst_atl(self):
        assert HST_RATE_ATL == D("0.15")

    def test_cent_quantum(self):
        assert CENT == D("0.01")

    def test_hst_provinces_set(self):
        assert "ON" in HST_PROVINCES
        assert "NB" in HST_PROVINCES
        assert "QC" not in HST_PROVINCES

    def test_registry_has_all_required_codes(self):
        for code in ("T", "Z", "E", "M", "I", "GST_QST", "HST", "VAT", "GENERIC_TAX", "NONE"):
            assert code in TAX_CODE_REGISTRY

    def test_valid_tax_codes_frozenset(self):
        assert "T" in VALID_TAX_CODES
        assert "HST" in VALID_TAX_CODES


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

class TestInternalHelpers:
    def test_round_basic(self):
        assert _round(D("1.005")) == D("1.01")
        assert _round(D("1.004")) == D("1.00")

    def test_normalize_code_upper(self):
        assert _normalize_code("gst_qst") == "GST_QST"
        assert _normalize_code("  t  ")   == "T"
        assert _normalize_code(None)       == ""

    def test_registry_entry_fallback(self):
        entry = _registry_entry("UNKNOWN_CODE")
        assert entry == TAX_CODE_REGISTRY["NONE"]

    def test_registry_entry_known(self):
        entry = _registry_entry("T")
        assert entry["gst_rate"] == GST_RATE
        assert entry["qst_rate"] == QST_RATE


# ---------------------------------------------------------------------------
# calculate_gst_qst
# ---------------------------------------------------------------------------

class TestCalculateGstQst:
    def test_basic_100(self):
        r = calculate_gst_qst(D("100.00"))
        assert r["gst"] == D("5.00")
        assert r["qst"] == D("9.98")  # 100 × 0.09975 = 9.975 → rounds to 9.98
        assert r["total_with_tax"] == r["amount_before_tax"] + r["gst"] + r["qst"]

    def test_keys_present(self):
        r = calculate_gst_qst(D("200.00"))
        for k in ("amount_before_tax", "gst", "qst", "total_tax", "total_with_tax", "gst_rate", "qst_rate"):
            assert k in r

    def test_zero_amount(self):
        r = calculate_gst_qst(D("0.00"))
        assert r["gst"] == D("0.00")
        assert r["qst"] == D("0.00")
        assert r["total_with_tax"] == D("0.00")

    def test_accepts_string_input(self):
        r = calculate_gst_qst("50.00")
        assert r["gst"] == D("2.50")

    def test_parallel_not_cascaded(self):
        """QST must be applied to pre-tax amount, not to (pre_tax + GST)."""
        r = calculate_gst_qst(D("1000.00"))
        assert r["gst"] == D("50.00")
        assert r["qst"] == D("99.75")  # 1000 × 0.09975

    def test_rates_in_return(self):
        r = calculate_gst_qst(D("100"))
        assert r["gst_rate"] == GST_RATE
        assert r["qst_rate"] == QST_RATE

    def test_invalid_input_raises(self):
        with pytest.raises(Exception):
            calculate_gst_qst(None)

    def test_decimal_precision(self):
        """All monetary values must be Decimal, not float."""
        r = calculate_gst_qst(D("123.45"))
        assert isinstance(r["gst"], Decimal)
        assert isinstance(r["qst"], Decimal)


# ---------------------------------------------------------------------------
# extract_tax_from_total
# ---------------------------------------------------------------------------

class TestExtractTaxFromTotal:
    def test_round_trip(self):
        """calculate_gst_qst then extract_tax_from_total should return approx. original."""
        pre_tax = D("100.00")
        forward = calculate_gst_qst(pre_tax)
        back = extract_tax_from_total(forward["total_with_tax"])
        # Allow ±1 cent due to rounding at each step
        diff = abs(back["pre_tax"] - pre_tax)
        assert diff <= D("0.01"), f"pre_tax round-trip diff {diff} too large"

    def test_keys_present(self):
        r = extract_tax_from_total(D("114.98"))
        for k in ("total", "combined_rate", "combined_divisor", "pre_tax", "gst", "qst", "total_tax"):
            assert k in r

    def test_combined_divisor(self):
        r = extract_tax_from_total(D("114.98"))
        assert r["combined_divisor"] == D("1") + COMBINED_GST_QST

    def test_zero_total(self):
        r = extract_tax_from_total(D("0.00"))
        assert r["pre_tax"] == D("0.00")
        assert r["gst"] == D("0.00")

    def test_gst_qst_add_up(self):
        r = extract_tax_from_total(D("229.95"))
        assert r["total_tax"] == r["gst"] + r["qst"]

    def test_accepts_string(self):
        r = extract_tax_from_total("100.00")
        assert r["pre_tax"] > D("0")


# ---------------------------------------------------------------------------
# validate_tax_code
# ---------------------------------------------------------------------------

class TestValidateTaxCode:
    def test_valid_t_qc(self):
        r = validate_tax_code("5200 - Office Supplies", "T", "QC")
        assert r["valid"] is True
        assert r["warnings"] == []

    def test_missing_tax_code(self):
        r = validate_tax_code("5200", "", "QC")
        assert r["valid"] is False
        assert "tax_code_missing" in r["warnings"]

    def test_unknown_tax_code(self):
        r = validate_tax_code("5200", "BADCODE", "QC")
        assert r["valid"] is False
        assert any("unknown_tax_code" in w for w in r["warnings"])

    def test_hst_province_warns_on_gst_qst(self):
        r = validate_tax_code("5200", "T", "ON")
        assert not r["valid"]
        assert any("on" in w.lower() for w in r["warnings"])

    def test_hst_province_warns_on_gst_qst_all(self):
        for prov in ("ON", "NB", "NS", "NL", "PE"):
            r = validate_tax_code("5200", "T", prov)
            assert not r["valid"], f"Expected warning for {prov} + T"

    def test_qc_warns_on_hst(self):
        r = validate_tax_code("5200", "HST", "QC")
        assert not r["valid"]
        assert "province_qc_does_not_use_hst" in r["warnings"]

    def test_no_province_skips_province_check(self):
        r = validate_tax_code("5200", "T", "")
        assert r["valid"] is True

    def test_insurance_gl_warns_on_t(self):
        r = validate_tax_code("6100 - Insurance Expense", "T", "QC")
        assert not r["valid"]
        assert "insurance_gl_account_expects_code_i_or_exempt" in r["warnings"]

    def test_insurance_gl_ok_with_i(self):
        r = validate_tax_code("6100 - Insurance Expense", "I", "QC")
        assert r["valid"] is True

    def test_meals_gl_warns_on_t(self):
        r = validate_tax_code("7100 - Meals & Entertainment", "T", "QC")
        assert not r["valid"]
        assert "meals_gl_account_expects_code_m" in r["warnings"]

    def test_meals_gl_ok_with_m(self):
        r = validate_tax_code("7100 - Meals & Entertainment", "M", "QC")
        assert r["valid"] is True

    def test_none_code_valid_in_neutral_context(self):
        r = validate_tax_code("5200", "NONE", "QC")
        assert r["valid"] is True

    def test_z_code_valid(self):
        r = validate_tax_code("5200", "Z", "QC")
        assert r["valid"] is True

    def test_e_code_valid(self):
        r = validate_tax_code("5200", "E", "ON")
        assert r["valid"] is True

    def test_return_keys(self):
        r = validate_tax_code("5200", "T", "QC")
        for k in ("valid", "warnings", "tax_code", "gl_account", "vendor_province"):
            assert k in r

    def test_code_normalized_in_return(self):
        r = validate_tax_code("5200", "t", "qc")
        assert r["tax_code"] == "T"
        assert r["vendor_province"] == "QC"


# ---------------------------------------------------------------------------
# calculate_itc_itr
# ---------------------------------------------------------------------------

class TestCalculateItcItr:
    def test_t_full_recovery(self):
        r = calculate_itc_itr(D("100.00"), "T")
        assert r["gst_recoverable"] == D("5.00")
        assert r["qst_recoverable"] == D("9.98")  # rounds 9.975
        assert r["hst_recoverable"] == D("0.00")

    def test_z_no_recovery(self):
        r = calculate_itc_itr(D("100.00"), "Z")
        assert r["gst_recoverable"] == D("0.00")
        assert r["qst_recoverable"] == D("0.00")
        assert r["total_recoverable"] == D("0.00")

    def test_e_no_recovery(self):
        r = calculate_itc_itr(D("100.00"), "E")
        assert r["total_recoverable"] == D("0.00")

    def test_m_half_recovery(self):
        r = calculate_itc_itr(D("100.00"), "M")
        assert r["gst_recoverable"] == D("2.50")   # 5.00 × 0.5
        assert r["qst_recoverable"] == D("4.99")   # 9.975 × 0.5 = 4.9875 → 4.99

    def test_i_no_recovery(self):
        r = calculate_itc_itr(D("100.00"), "I")
        assert r["gst_recoverable"] == D("0.00")
        assert r["qst_recoverable"] == D("0.00")
        assert r["total_recoverable"] == D("0.00")

    def test_hst_full_recovery(self):
        r = calculate_itc_itr(D("100.00"), "HST")
        assert r["hst_recoverable"] == D("13.00")
        assert r["qst_recoverable"] == D("0.00")

    def test_return_keys(self):
        r = calculate_itc_itr(D("100.00"), "T")
        for k in ("expense_amount", "tax_code", "gst_paid", "qst_paid", "hst_paid",
                  "gst_recoverable", "qst_recoverable", "hst_recoverable",
                  "itc_rate", "itr_rate", "total_recoverable"):
            assert k in r

    def test_none_code_normalizes(self):
        r = calculate_itc_itr(D("100.00"), None)
        assert r["tax_code"] == "NONE"
        assert r["total_recoverable"] == D("0.00")

    def test_gst_qst_legacy_same_as_t(self):
        t_r    = calculate_itc_itr(D("100.00"), "T")
        gstqst = calculate_itc_itr(D("100.00"), "GST_QST")
        assert t_r["gst_recoverable"] == gstqst["gst_recoverable"]
        assert t_r["qst_recoverable"] == gstqst["qst_recoverable"]

    def test_total_recoverable_is_sum(self):
        r = calculate_itc_itr(D("100.00"), "T")
        assert r["total_recoverable"] == (
            r["gst_recoverable"] + r["qst_recoverable"] + r["hst_recoverable"]
        )


# ---------------------------------------------------------------------------
# _itc_itr_from_total (private helper)
# ---------------------------------------------------------------------------

class TestItcItrFromTotal:
    def test_t_extracts_pretax_first(self):
        """For T, total = pre_tax × 1.14975; recoverable should match direct calc."""
        pre_tax = D("100.00")
        total = calculate_gst_qst(pre_tax)["total_with_tax"]
        r_from_total  = _itc_itr_from_total(total, "T")
        r_from_pretax = calculate_itc_itr(pre_tax, "T")
        # Allow ±1 cent due to the rounding of total in the forward pass
        assert abs(r_from_total["gst_recoverable"] - r_from_pretax["gst_recoverable"]) <= D("0.01")

    def test_e_uses_total_as_pretax(self):
        r = _itc_itr_from_total(D("200.00"), "E")
        assert r["total_recoverable"] == D("0.00")

    def test_hst_extracts_pretax(self):
        r = _itc_itr_from_total(D("113.00"), "HST")
        # 113 / 1.13 = 100 pre-tax; itc = 100 × 0.13 = 13
        assert abs(r["hst_recoverable"] - D("13.00")) <= D("0.01")


# ---------------------------------------------------------------------------
# generate_filing_summary
# ---------------------------------------------------------------------------

class TestGenerateFilingSummary:
    def test_missing_db(self, tmp_path):
        result = generate_filing_summary("ACME", "2025-01-01", "2025-03-31",
                                         db_path=tmp_path / "nonexistent.db")
        assert "error" in result
        assert result["error"] == "database_not_found"

    def test_empty_period_returns_zeros(self, tmp_path):
        db = tmp_path / "test.db"
        _make_db(db)
        result = generate_filing_summary("ACME", "2025-01-01", "2025-03-31", db_path=db)
        assert result["documents_total"] == 0
        assert result["itc_available"] == D("0.00")
        assert result["itr_available"] == D("0.00")
        assert result["line_items"] == []

    def test_posted_document_counted(self, tmp_path):
        db = tmp_path / "test.db"
        _make_db(db)
        conn = sqlite3.connect(db)
        conn.execute(
            "INSERT INTO documents VALUES (?,?,?,?,?,?,?,?)",
            ("DOC001", "ACME", "Vendor Inc", "2025-02-15", "114.98", "T", "5200 Office", "Ready"),
        )
        conn.execute(
            "INSERT INTO posting_jobs (document_id, target_system, posting_status, external_id, created_at) VALUES (?,?,?,?,?)",
            ("DOC001", "qbo", "posted", "QB-001", "2025-02-20"),
        )
        conn.commit()
        conn.close()

        result = generate_filing_summary("ACME", "2025-01-01", "2025-03-31", db_path=db)
        assert result["documents_posted"] == 1
        assert result["documents_pending"] == 0
        assert result["documents_total"] == 1
        assert result["itc_available"] > D("0")
        assert len(result["line_items"]) == 1

    def test_pending_document_not_in_itc(self, tmp_path):
        db = tmp_path / "test.db"
        _make_db(db)
        conn = sqlite3.connect(db)
        conn.execute(
            "INSERT INTO documents VALUES (?,?,?,?,?,?,?,?)",
            ("DOC002", "ACME", "Vendor Inc", "2025-02-15", "100.00", "T", "5200 Office", "NeedsReview"),
        )
        conn.commit()
        conn.close()

        result = generate_filing_summary("ACME", "2025-01-01", "2025-03-31", db_path=db)
        assert result["documents_pending"] == 1
        assert result["documents_posted"] == 0
        assert result["itc_available"] == D("0.00")

    def test_ignored_document_excluded(self, tmp_path):
        db = tmp_path / "test.db"
        _make_db(db)
        conn = sqlite3.connect(db)
        conn.execute(
            "INSERT INTO documents VALUES (?,?,?,?,?,?,?,?)",
            ("DOC003", "ACME", "Vendor Inc", "2025-02-15", "100.00", "T", "5200 Office", "Ignored"),
        )
        conn.commit()
        conn.close()

        result = generate_filing_summary("ACME", "2025-01-01", "2025-03-31", db_path=db)
        assert result["documents_total"] == 0

    def test_client_code_case_insensitive(self, tmp_path):
        db = tmp_path / "test.db"
        _make_db(db)
        conn = sqlite3.connect(db)
        conn.execute(
            "INSERT INTO documents VALUES (?,?,?,?,?,?,?,?)",
            ("DOC004", "acme", "Vendor", "2025-02-15", "100.00", "T", "5200", "Ready"),
        )
        conn.commit()
        conn.close()

        result = generate_filing_summary("ACME", "2025-01-01", "2025-03-31", db_path=db)
        assert result["documents_total"] == 1

    def test_out_of_period_excluded(self, tmp_path):
        db = tmp_path / "test.db"
        _make_db(db)
        conn = sqlite3.connect(db)
        conn.execute(
            "INSERT INTO documents VALUES (?,?,?,?,?,?,?,?)",
            ("DOC005", "ACME", "Vendor", "2024-12-31", "100.00", "T", "5200", "Ready"),
        )
        conn.commit()
        conn.close()

        result = generate_filing_summary("ACME", "2025-01-01", "2025-03-31", db_path=db)
        assert result["documents_total"] == 0

    def test_return_keys(self, tmp_path):
        db = tmp_path / "test.db"
        _make_db(db)
        result = generate_filing_summary("X", "2025-01-01", "2025-03-31", db_path=db)
        for k in ("client_code", "period_start", "period_end",
                  "gst_collected", "qst_collected",
                  "itc_available", "itr_available",
                  "net_gst_payable", "net_qst_payable",
                  "documents_posted", "documents_pending", "documents_total",
                  "line_items"):
            assert k in result

    def test_net_gst_payable_formula(self, tmp_path):
        db = tmp_path / "test.db"
        _make_db(db)
        result = generate_filing_summary("NONE", "2025-01-01", "2025-03-31", db_path=db)
        # net_gst_payable = gst_collected - itc_available
        assert result["net_gst_payable"] == result["gst_collected"] - result["itc_available"]

    def test_external_id_marks_posted(self, tmp_path):
        """A document with a non-empty external_id should be counted as posted."""
        db = tmp_path / "test.db"
        _make_db(db)
        conn = sqlite3.connect(db)
        conn.execute(
            "INSERT INTO documents VALUES (?,?,?,?,?,?,?,?)",
            ("DOC006", "ACME", "Vendor", "2025-02-01", "200.00", "T", "5200", "Ready"),
        )
        conn.execute(
            "INSERT INTO posting_jobs (document_id, target_system, posting_status, external_id, created_at) VALUES (?,?,?,?,?)",
            ("DOC006", "qbo", "ready_to_post", "EXT-123", "2025-02-05"),
        )
        conn.commit()
        conn.close()

        result = generate_filing_summary("ACME", "2025-01-01", "2025-03-31", db_path=db)
        assert result["documents_posted"] == 1
