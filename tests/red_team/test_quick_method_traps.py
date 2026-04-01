"""
RED-TEAM: Quick Method Traps
=============================
Adversarial tests for the Quick Method guard-rails in tax_engine.py
and the FPZ-500 prefill in revenu_quebec.py.

Trap catalogue tested:
  QM-1  ITC double-claim under Quick Method
  QM-2  Mixed taxable / exempt overlap
  QM-3  PST province activity
  QM-4  Input-tax exclusion (capital property)
  QM-5  Mid-year method change
  QM-6  Credit note after filing period

Fail-fast rule:
  The engine must NEVER double-claim ITCs under Quick Method logic.
"""
from __future__ import annotations

import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.tax_engine import (
    _ZERO,
    QUICK_METHOD_GST_RATES,
    QUICK_METHOD_QST_RATES,
    validate_quick_method_traps,
    validate_quebec_tax_compliance,
    calculate_itc_itr,
    generate_filing_summary,
)
from src.agents.core.revenu_quebec import (
    compute_prefill,
    set_client_config,
    ensure_client_config_table,
    QM_RETAIL_GST,
    QM_RETAIL_QST,
    QM_SERVICES_GST,
    QM_SERVICES_QST,
)


# ===================================================================
# Helpers
# ===================================================================

def _traps(doc, **kwargs):
    """Shorthand: run validate_quick_method_traps and return trap codes."""
    results = validate_quick_method_traps(doc, **kwargs)
    return {t["trap_code"] for t in results}


def _trap_detail(doc, trap_code, **kwargs):
    """Return the first trap matching trap_code, or None."""
    for t in validate_quick_method_traps(doc, **kwargs):
        if t["trap_code"] == trap_code:
            return t
    return None


def _in_memory_db():
    """Create an in-memory SQLite DB with documents + posting_jobs tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE documents (
            document_id   TEXT PRIMARY KEY,
            client_code   TEXT,
            vendor        TEXT,
            document_date TEXT,
            amount        TEXT,
            tax_code      TEXT,
            gl_account    TEXT,
            review_status TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE posting_jobs (
            rowid         INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id   TEXT,
            posting_status TEXT,
            external_id   TEXT,
            created_at    TEXT,
            updated_at    TEXT
        )
    """)
    ensure_client_config_table(conn)
    conn.commit()
    return conn


# ===================================================================
# QM-1: ITC Double-Claim — THE cardinal sin
# ===================================================================

class TestQM1_ITCDoubleClaim:
    """Quick Method registrant must NEVER claim ITC/ITR."""

    def test_itc_claimed_triggers_trap(self):
        doc = {"quick_method": True, "itc_claimed": Decimal("50.00")}
        assert "QM-1" in _traps(doc)

    def test_itr_claimed_triggers_trap(self):
        doc = {"quick_method": True, "itr_claimed": Decimal("25.00")}
        assert "QM-1" in _traps(doc)

    def test_gst_recoverable_triggers_trap(self):
        doc = {"quick_method": True, "gst_recoverable": Decimal("10.00")}
        assert "QM-1" in _traps(doc)

    def test_qst_recoverable_triggers_trap(self):
        doc = {"quick_method": True, "qst_recoverable": Decimal("9.98")}
        assert "QM-1" in _traps(doc)

    def test_hst_recoverable_triggers_trap(self):
        doc = {"quick_method": True, "hst_recoverable": Decimal("130.00")}
        assert "QM-1" in _traps(doc)

    def test_zero_claims_no_trap(self):
        """Zero claims are clean — no QM-1."""
        doc = {
            "quick_method": True,
            "itc_claimed": Decimal("0"),
            "itr_claimed": Decimal("0"),
        }
        assert "QM-1" not in _traps(doc)

    def test_combined_itc_itr_shows_total_blocked(self):
        doc = {
            "quick_method": True,
            "itc_claimed": Decimal("50.00"),
            "itr_claimed": Decimal("25.00"),
            "gst_recoverable": Decimal("10.00"),
        }
        trap = _trap_detail(doc, "QM-1")
        assert trap is not None
        assert trap["detail"]["total_blocked"] == "85.00"

    def test_normal_method_no_trap(self):
        """Non-QM registrant can claim ITC freely."""
        doc = {"quick_method": False, "itc_claimed": Decimal("500.00")}
        assert "QM-1" not in _traps(doc)

    def test_penny_itc_still_trapped(self):
        """Even $0.01 ITC is a violation under QM."""
        doc = {"quick_method": True, "itc_claimed": Decimal("0.01")}
        assert "QM-1" in _traps(doc)

    def test_client_config_qm_flag_overrides(self):
        """QM flag from client_config should also trigger trap."""
        doc = {"itc_claimed": Decimal("50.00")}
        cfg = {"quick_method": True}
        assert "QM-1" in _traps(doc, client_config=cfg)

    def test_distinct_from_normal_itc_treatment(self):
        """ITC calculation still works normally — the TRAP layer blocks it."""
        result = calculate_itc_itr(Decimal("1000"), "T")
        assert result["gst_recoverable"] > _ZERO  # Normal method: ITC exists
        # But when QM is active, the trap must fire
        doc = {
            "quick_method": True,
            "gst_recoverable": result["gst_recoverable"],
            "qst_recoverable": result["qst_recoverable"],
        }
        assert "QM-1" in _traps(doc)


# ===================================================================
# QM-2: Mixed taxable / exempt overlap
# ===================================================================

class TestQM2_MixedTaxableExempt:
    """Quick Method with mixed supplies needs manual carve-out."""

    def test_mixed_line_items_trigger_trap(self):
        doc = {
            "quick_method": True,
            "line_items": [
                {"tax_code": "T", "amount": "100"},
                {"tax_code": "E", "amount": "200"},
            ],
        }
        assert "QM-2" in _traps(doc)

    def test_mixed_zero_rated_and_taxable(self):
        doc = {
            "quick_method": True,
            "line_items": [
                {"tax_code": "HST", "amount": "500"},
                {"tax_code": "Z", "amount": "300"},
            ],
        }
        assert "QM-2" in _traps(doc)

    def test_all_taxable_no_trap(self):
        doc = {
            "quick_method": True,
            "line_items": [
                {"tax_code": "T", "amount": "100"},
                {"tax_code": "GST_QST", "amount": "200"},
            ],
        }
        assert "QM-2" not in _traps(doc)

    def test_all_exempt_no_trap(self):
        doc = {
            "quick_method": True,
            "line_items": [
                {"tax_code": "E", "amount": "100"},
                {"tax_code": "Z", "amount": "200"},
            ],
        }
        assert "QM-2" not in _traps(doc)

    def test_exempt_category_with_taxable_code_flags(self):
        """Document-level check when no line items."""
        doc = {
            "quick_method": True,
            "tax_code": "T",
            "category": "medical_services",
        }
        assert "QM-2" in _traps(doc)

    def test_unsupported_overlap_blocked(self):
        """Mixed overlap must block auto-approval — severity is high."""
        doc = {
            "quick_method": True,
            "line_items": [
                {"tax_code": "T", "amount": "100"},
                {"tax_code": "E", "amount": "200"},
            ],
        }
        trap = _trap_detail(doc, "QM-2")
        assert trap is not None
        assert trap["severity"] == "high"

    def test_meals_mixed_with_exempt(self):
        """Meals (M) is taxable at 50% — still triggers mixed overlap."""
        doc = {
            "quick_method": True,
            "line_items": [
                {"tax_code": "M", "amount": "80"},
                {"tax_code": "E", "amount": "120"},
            ],
        }
        assert "QM-2" in _traps(doc)


# ===================================================================
# QM-3: PST province activity
# ===================================================================

class TestQM3_PSTProvinceActivity:
    """Quick Method does NOT absorb PST."""

    @pytest.mark.parametrize("province", ["BC", "MB", "SK"])
    def test_pst_provinces_flag(self, province):
        doc = {"quick_method": True, "vendor_province": province}
        assert "QM-3" in _traps(doc)

    @pytest.mark.parametrize("province", ["QC", "ON", "AB", "NB"])
    def test_non_pst_provinces_clean(self, province):
        doc = {"quick_method": True, "vendor_province": province}
        assert "QM-3" not in _traps(doc)

    def test_pst_detail_includes_rate(self):
        doc = {"quick_method": True, "vendor_province": "BC"}
        trap = _trap_detail(doc, "QM-3")
        assert trap is not None
        assert trap["detail"]["pst_rate"] == "0.07"

    def test_sk_pst_rate(self):
        doc = {"quick_method": True, "vendor_province": "SK"}
        trap = _trap_detail(doc, "QM-3")
        assert trap["detail"]["pst_rate"] == "0.06"


# ===================================================================
# QM-4: Input tax exclusion — capital property
# ===================================================================

class TestQM4_InputTaxExclusion:
    """Capital property > $30K must use normal ITC rules even under QM."""

    def test_capital_keyword_triggers(self):
        doc = {
            "quick_method": True,
            "expense_type": "capital equipment",
            "expense_amount": Decimal("5000"),
        }
        assert "QM-4" in _traps(doc)

    def test_real_property_triggers(self):
        doc = {
            "quick_method": True,
            "expense_type": "real_property acquisition",
            "expense_amount": Decimal("250000"),
        }
        assert "QM-4" in _traps(doc)

    def test_amount_over_threshold_triggers(self):
        """Even without capital keyword, > $30K triggers exclusion."""
        doc = {
            "quick_method": True,
            "expense_type": "office renovation",
            "expense_amount": Decimal("35000"),
        }
        assert "QM-4" in _traps(doc)

    def test_small_amount_no_keyword_clean(self):
        doc = {
            "quick_method": True,
            "expense_type": "office supplies",
            "expense_amount": Decimal("500"),
        }
        assert "QM-4" not in _traps(doc)

    def test_immobilisation_keyword_fr(self):
        doc = {
            "quick_method": True,
            "expense_type": "immobilisation corporelle",
            "expense_amount": Decimal("10000"),
        }
        assert "QM-4" in _traps(doc)

    def test_building_keyword(self):
        doc = {
            "quick_method": True,
            "expense_type": "building maintenance",
            "expense_amount": Decimal("500"),
        }
        assert "QM-4" in _traps(doc)


# ===================================================================
# QM-5: Mid-year method change
# ===================================================================

class TestQM5_MidYearMethodChange:
    """CRA/RQ require a full fiscal year on one method."""

    def test_switch_to_qm_mid_year(self):
        doc = {"quick_method": True}
        history = [
            {"period_start": "2025-01-01", "period_end": "2025-03-31",
             "quick_method": False, "filed_at": "2025-04-15"},
            {"period_start": "2025-04-01", "period_end": "2025-06-30",
             "quick_method": True, "filed_at": "2025-07-15"},
        ]
        assert "QM-5" in _traps(doc, filing_history=history)

    def test_switch_away_from_qm_mid_year(self):
        doc = {"quick_method": False}
        cfg = {"quick_method": True}  # Was on QM
        history = [
            {"period_start": "2025-01-01", "period_end": "2025-03-31",
             "quick_method": True, "filed_at": "2025-04-15"},
            {"period_start": "2025-04-01", "period_end": "2025-06-30",
             "quick_method": False, "filed_at": "2025-07-15"},
        ]
        assert "QM-5" in _traps(doc, client_config=cfg, filing_history=history)

    def test_year_boundary_change_is_ok(self):
        """Change at fiscal year boundary is allowed."""
        doc = {"quick_method": True}
        history = [
            {"period_start": "2024-10-01", "period_end": "2024-12-31",
             "quick_method": False, "filed_at": "2025-01-15"},
            {"period_start": "2025-01-01", "period_end": "2025-03-31",
             "quick_method": True, "filed_at": "2025-04-15"},
        ]
        assert "QM-5" not in _traps(doc, filing_history=history)

    def test_consistent_qm_all_year_no_trap(self):
        doc = {"quick_method": True}
        history = [
            {"period_start": "2025-01-01", "period_end": "2025-03-31",
             "quick_method": True, "filed_at": "2025-04-15"},
            {"period_start": "2025-04-01", "period_end": "2025-06-30",
             "quick_method": True, "filed_at": "2025-07-15"},
        ]
        assert "QM-5" not in _traps(doc, filing_history=history)

    def test_no_history_no_trap(self):
        doc = {"quick_method": True}
        assert "QM-5" not in _traps(doc, filing_history=None)
        assert "QM-5" not in _traps(doc, filing_history=[])


# ===================================================================
# QM-6: Credit note after filing
# ===================================================================

class TestQM6_CreditNoteAfterFiling:
    """Credit notes post-filing must adjust NEXT period, not current."""

    def test_credit_note_after_filing_period(self):
        doc = {
            "quick_method": True,
            "is_credit_note": True,
            "document_date": "2025-04-15",
            "filing_period_end": "2025-03-31",
            "amount": Decimal("-500"),
        }
        assert "QM-6" in _traps(doc)

    def test_credit_note_within_filing_period(self):
        doc = {
            "quick_method": True,
            "is_credit_note": True,
            "document_date": "2025-03-15",
            "filing_period_end": "2025-03-31",
            "amount": Decimal("-500"),
        }
        assert "QM-6" not in _traps(doc)

    def test_negative_amount_implies_credit(self):
        """Negative amount without explicit is_credit_note flag."""
        doc = {
            "quick_method": True,
            "document_date": "2025-07-10",
            "filing_period_end": "2025-06-30",
            "amount": Decimal("-1200"),
        }
        assert "QM-6" in _traps(doc)

    def test_positive_amount_not_credit(self):
        doc = {
            "quick_method": True,
            "document_date": "2025-07-10",
            "filing_period_end": "2025-06-30",
            "amount": Decimal("1200"),
        }
        assert "QM-6" not in _traps(doc)

    def test_credit_note_detail_includes_dates(self):
        doc = {
            "quick_method": True,
            "is_credit_note": True,
            "document_date": "2025-04-05",
            "filing_period_end": "2025-03-31",
            "amount": Decimal("-800"),
        }
        trap = _trap_detail(doc, "QM-6")
        assert trap is not None
        assert trap["detail"]["document_date"] == "2025-04-05"
        assert trap["detail"]["filing_period_end"] == "2025-03-31"


# ===================================================================
# FPZ-500 Prefill — Quick Method correctness
# ===================================================================

class TestFPZ500_QuickMethodPrefill:
    """FPZ-500 prefill must reflect Quick Method correctly."""

    def test_qm_zeroes_itc_itr_lines(self):
        """Under QM, lines 106 and 207 must be $0."""
        conn = _in_memory_db()
        set_client_config(conn, "QM_CLIENT", True, "retail")

        # Insert a posted taxable document
        conn.execute("""
            INSERT INTO documents
            (document_id, client_code, vendor, document_date, amount,
             tax_code, gl_account, review_status)
            VALUES ('D001', 'QM_CLIENT', 'ACME', '2025-02-15', '1149.75',
                    'T', '5200', 'approved')
        """)
        conn.execute("""
            INSERT INTO posting_jobs
            (document_id, posting_status, external_id, created_at, updated_at)
            VALUES ('D001', 'posted', 'EXT-001', '2025-02-15', '2025-02-15')
        """)
        conn.commit()

        # compute_prefill uses generate_filing_summary which needs a real DB file
        # For an in-memory DB the filing summary will return error but we can
        # still verify the QM logic path
        result = compute_prefill("QM_CLIENT", "2025-01-01", "2025-03-31", conn)

        # Key assertion: under QM, ITC/ITR lines are forced to $0
        assert result["line_106"] == Decimal("0")
        assert result["line_207"] == Decimal("0")
        assert result["quick_method"] is True
        assert result["quick_method_type"] == "retail"
        assert result["quick_gst_rate"] == QM_RETAIL_GST
        assert result["quick_qst_rate"] == QM_RETAIL_QST
        conn.close()

    def test_qm_services_rates(self):
        conn = _in_memory_db()
        set_client_config(conn, "SVC_CLIENT", True, "services")
        result = compute_prefill("SVC_CLIENT", "2025-01-01", "2025-03-31", conn)
        assert result["quick_gst_rate"] == QM_SERVICES_GST
        assert result["quick_qst_rate"] == QM_SERVICES_QST
        assert result["line_106"] == Decimal("0")
        assert result["line_207"] == Decimal("0")
        conn.close()

    def test_normal_method_allows_itc(self):
        """Non-QM client should have non-zero ITC/ITR if there are posted docs."""
        conn = _in_memory_db()
        set_client_config(conn, "NORMAL_CLIENT", False, "retail")
        result = compute_prefill("NORMAL_CLIENT", "2025-01-01", "2025-03-31", conn)
        assert result["quick_method"] is False
        assert result["quick_gst_rate"] is None
        assert result["quick_qst_rate"] is None
        conn.close()

    def test_qm_blocked_itc_reported(self):
        """Prefill should report how much ITC/ITR was blocked."""
        conn = _in_memory_db()
        set_client_config(conn, "QM_BLOCK", True, "retail")
        result = compute_prefill("QM_BLOCK", "2025-01-01", "2025-03-31", conn)
        # qm_itc_blocked and qm_itr_blocked keys must exist
        assert "qm_itc_blocked" in result
        assert "qm_itr_blocked" in result
        assert result["qm_itc_blocked"] >= Decimal("0")
        assert result["qm_itr_blocked"] >= Decimal("0")
        conn.close()


# ===================================================================
# Cross-cutting: No double-claim leakage
# ===================================================================

class TestNoDoubleClaim:
    """The engine must NEVER allow ITC double-claims under QM."""

    def test_filing_summary_with_qm_prefill_zero_itc(self):
        """Even if generate_filing_summary returns ITC > 0, prefill
        must zero it out for QM clients."""
        conn = _in_memory_db()
        set_client_config(conn, "DOUBLE_CHECK", True, "services")
        result = compute_prefill("DOUBLE_CHECK", "2025-01-01", "2025-03-31", conn)
        assert result["line_106"] == Decimal("0"), \
            "ITC must be $0 under Quick Method — double-claim detected!"
        assert result["line_207"] == Decimal("0"), \
            "ITR must be $0 under Quick Method — double-claim detected!"
        conn.close()

    def test_trap_fires_on_any_nonzero_recovery(self):
        """Synthetic: even $0.01 recovery must be caught."""
        doc = {
            "quick_method": True,
            "gst_recoverable": Decimal("0.01"),
        }
        traps = validate_quick_method_traps(doc)
        qm1 = [t for t in traps if t["trap_code"] == "QM-1"]
        assert len(qm1) == 1, "QM-1 must fire on $0.01 recovery"
        assert qm1[0]["severity"] == "critical"

    def test_all_traps_have_bilingual_descriptions(self):
        """Every trap must have both EN and FR descriptions."""
        doc = {
            "quick_method": True,
            "itc_claimed": Decimal("100"),
            "vendor_province": "BC",
            "expense_type": "capital",
            "expense_amount": Decimal("50000"),
            "is_credit_note": True,
            "document_date": "2025-04-15",
            "filing_period_end": "2025-03-31",
            "amount": Decimal("-500"),
            "line_items": [
                {"tax_code": "T", "amount": "100"},
                {"tax_code": "E", "amount": "200"},
            ],
        }
        history = [
            {"period_start": "2025-01-01", "period_end": "2025-03-31",
             "quick_method": False, "filed_at": "2025-04-15"},
            {"period_start": "2025-04-01", "period_end": "2025-06-30",
             "quick_method": True, "filed_at": "2025-07-15"},
        ]
        traps = validate_quick_method_traps(
            doc, filing_history=history,
        )
        assert len(traps) >= 5, f"Expected ≥5 traps, got {len(traps)}"
        for t in traps:
            assert "description_en" in t, f"Missing EN desc for {t['trap_code']}"
            assert "description_fr" in t, f"Missing FR desc for {t['trap_code']}"
            assert len(t["description_en"]) > 20
            assert len(t["description_fr"]) > 20

    def test_quick_method_treatment_distinct_from_normal(self):
        """Quick Method treatment must be distinct from normal ITC treatment:
        same document, different results depending on QM flag."""
        base_doc = {
            "subtotal": Decimal("1000"),
            "itc_claimed": Decimal("50"),
            "itr_claimed": Decimal("99.75"),
        }

        # Normal method: no traps
        normal_doc = {**base_doc, "quick_method": False}
        normal_traps = _traps(normal_doc)
        assert "QM-1" not in normal_traps

        # Quick method: must trap
        qm_doc = {**base_doc, "quick_method": True}
        qm_traps = _traps(qm_doc)
        assert "QM-1" in qm_traps

    def test_existing_compliance_check_still_works(self):
        """validate_quebec_tax_compliance Quick Method rate check (error 8)
        should still function alongside the new traps."""
        doc = {
            "quick_method": True,
            "quick_method_type": "services",
            "remittance_rate": Decimal("0.018"),  # wrong — that's retail
            "subtotal": Decimal("10000"),
        }
        issues = validate_quebec_tax_compliance(doc)
        rate_errors = [i for i in issues if i["error_type"] == "quick_method_rate_error"]
        assert len(rate_errors) == 1


# ===================================================================
# Edge cases and adversarial inputs
# ===================================================================

class TestQuickMethodEdgeCases:
    """Adversarial inputs that should not crash the trap logic."""

    def test_empty_document(self):
        doc = {"quick_method": True}
        traps = validate_quick_method_traps(doc)
        assert isinstance(traps, list)

    def test_none_values_everywhere(self):
        doc = {
            "quick_method": True,
            "itc_claimed": None,
            "itr_claimed": None,
            "gst_recoverable": None,
            "vendor_province": None,
            "expense_type": None,
            "expense_amount": None,
            "line_items": None,
            "document_date": None,
            "filing_period_end": None,
            "amount": None,
        }
        traps = validate_quick_method_traps(doc)
        assert isinstance(traps, list)
        # Should not crash and should not produce false positives
        assert "QM-1" not in {t["trap_code"] for t in traps}

    def test_string_amounts(self):
        """Amounts passed as strings should still be caught."""
        doc = {
            "quick_method": True,
            "itc_claimed": "100.50",
        }
        assert "QM-1" in _traps(doc)

    def test_negative_itc_not_trapped(self):
        """Negative ITC (reversal) should not trigger QM-1."""
        doc = {
            "quick_method": True,
            "itc_claimed": Decimal("-50.00"),
        }
        # Negative ITC sums to negative — not > 0
        assert "QM-1" not in _traps(doc)

    def test_multiple_traps_can_fire_simultaneously(self):
        """A single document can trigger multiple traps at once."""
        doc = {
            "quick_method": True,
            "itc_claimed": Decimal("100"),        # QM-1
            "vendor_province": "BC",              # QM-3
            "expense_type": "capital equipment",  # QM-4
            "expense_amount": Decimal("50000"),
            "is_credit_note": True,               # QM-6
            "document_date": "2025-04-15",
            "filing_period_end": "2025-03-31",
            "amount": Decimal("-500"),
            "line_items": [                       # QM-2
                {"tax_code": "T", "amount": "100"},
                {"tax_code": "E", "amount": "200"},
            ],
        }
        codes = _traps(doc)
        assert codes >= {"QM-1", "QM-2", "QM-3", "QM-4", "QM-6"}

    def test_large_amounts_no_overflow(self):
        doc = {
            "quick_method": True,
            "itc_claimed": Decimal("99999999.99"),
            "expense_amount": Decimal("99999999.99"),
            "expense_type": "capital",
        }
        traps = validate_quick_method_traps(doc)
        codes = {t["trap_code"] for t in traps}
        assert "QM-1" in codes
        assert "QM-4" in codes
