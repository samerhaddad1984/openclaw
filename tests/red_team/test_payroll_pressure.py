"""
RED-TEAM: Payroll Pressure Tests — Quebec Compliance
=====================================================
Adversarial edge-case tests for Quebec payroll:
  P1  QPP vs CPP edge cases
  P2  QPIP vs EI
  P3  HSF size thresholds
  P4  CNESST industry differences
  P5  RL-1 vs T4 reconciliation
  P6  Employee crosses province mid-year
  P7  Taxable benefit misclassification

Fail condition: payroll outputs differ from expected rules
or reconciliation breaks.
"""
from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.payroll_engine import (
    # Constants
    QPP_RATE_EMPLOYEE, QPP2_RATE_EMPLOYEE,
    CPP_RATE_EMPLOYEE, CPP2_RATE_EMPLOYEE,
    EI_RATE_REGULAR, EI_RATE_QUEBEC,
    QPIP_RATE_EMPLOYEE, QPIP_RATE_EMPLOYER,
    HSF_MAX_RATE, HSF_TIERS, CNESST_INDUSTRY_RATES,
    RL1_T4_BOX_MAP, TAXABLE_BENEFITS,
    # Functions
    validate_qpp_cpp, validate_qpip_ei, validate_hsf_rate,
    validate_cnesst_rate, reconcile_rl1_t4,
    prorate_province_deductions, validate_taxable_benefit,
    _round, _to_decimal,
)


# ===================================================================
# P1. QPP vs CPP EDGE CASES
# ===================================================================

class TestQppCppEdgeCases:
    """Attack QPP/CPP province validation with edge cases."""

    def test_qc_must_use_qpp(self):
        """Quebec employee MUST contribute to QPP, not CPP."""
        r = validate_qpp_cpp("QC", "QPP")
        assert r["valid"] is True
        assert r["expected_plan"] == "QPP"

    def test_qc_using_cpp_is_error(self):
        """Quebec employee on CPP must be flagged."""
        r = validate_qpp_cpp("QC", "CPP")
        assert r["valid"] is False
        assert r["error_type"] == "qpp_cpp_error"
        assert "QPP" in r["description_en"]

    def test_on_must_use_cpp(self):
        """Ontario employee MUST contribute to CPP."""
        r = validate_qpp_cpp("ON", "CPP")
        assert r["valid"] is True

    def test_on_using_qpp_is_error(self):
        """Ontario employee wrongly assigned QPP."""
        r = validate_qpp_cpp("ON", "QPP")
        assert r["valid"] is False
        assert r["error_type"] == "qpp_cpp_error"

    def test_all_provinces_use_cpp(self):
        """Every province except QC must use CPP."""
        provinces = ["ON", "BC", "AB", "SK", "MB", "NB", "NS", "PE", "NL", "NT", "YT", "NU"]
        for prov in provinces:
            r = validate_qpp_cpp(prov, "CPP")
            assert r["valid"] is True, f"{prov} should use CPP"

    def test_all_provinces_reject_qpp(self):
        """Non-QC provinces must reject QPP."""
        for prov in ["ON", "BC", "AB"]:
            r = validate_qpp_cpp(prov, "QPP")
            assert r["valid"] is False, f"{prov} should reject QPP"

    def test_case_insensitive_province(self):
        """Province codes with mixed case should still work."""
        r = validate_qpp_cpp("qc", "QPP")
        assert r["valid"] is True
        r2 = validate_qpp_cpp("Qc", "QPP")
        assert r2["valid"] is True

    def test_case_insensitive_plan(self):
        """Plan names with mixed case should still work."""
        r = validate_qpp_cpp("QC", "qpp")
        assert r["valid"] is True

    def test_whitespace_in_inputs(self):
        """Leading/trailing whitespace must be stripped."""
        r = validate_qpp_cpp("  QC  ", "  QPP  ")
        assert r["valid"] is True

    def test_qpp_rate_is_higher_than_cpp(self):
        """QPP rate (6.40%) > CPP rate (5.95%) — a common confusion point."""
        assert QPP_RATE_EMPLOYEE > CPP_RATE_EMPLOYEE
        assert QPP_RATE_EMPLOYEE == Decimal("0.064")
        assert CPP_RATE_EMPLOYEE == Decimal("0.0595")

    def test_qpp2_and_cpp2_same_rate(self):
        """QPP2 and CPP2 second-ceiling rates are both 4%."""
        assert QPP2_RATE_EMPLOYEE == CPP2_RATE_EMPLOYEE == Decimal("0.04")

    def test_bilingual_error_messages(self):
        """Both EN and FR error descriptions must be present on failure."""
        r = validate_qpp_cpp("QC", "CPP")
        assert "description_en" in r
        assert "description_fr" in r
        assert "RRQ" in r["description_fr"]  # QPP in French = RRQ


# ===================================================================
# P2. QPIP vs EI
# ===================================================================

class TestQpipEiEdgeCases:
    """Attack QPIP/EI rate validation."""

    def test_qc_reduced_ei_rate(self):
        """Quebec employees pay reduced EI rate (1.32%)."""
        r = validate_qpip_ei("QC", EI_RATE_QUEBEC)
        assert r["valid"] is True

    def test_qc_full_ei_rate_is_error(self):
        """Quebec employee paying full 1.66% EI — must flag overpayment."""
        r = validate_qpip_ei("QC", EI_RATE_REGULAR)
        assert r["valid"] is False
        assert r["error_type"] == "qpip_ei_error"
        assert "QPIP" in r["description_en"] or "reduced" in r["description_en"]

    def test_non_qc_standard_ei_rate(self):
        """Non-QC employees pay standard EI rate (1.66%)."""
        r = validate_qpip_ei("ON", EI_RATE_REGULAR)
        assert r["valid"] is True

    def test_non_qc_reduced_rate_is_error(self):
        """Non-QC employee paying QC-reduced rate — must flag underpayment."""
        r = validate_qpip_ei("ON", EI_RATE_QUEBEC)
        assert r["valid"] is False

    def test_qpip_rates_are_correct(self):
        """QPIP employee 0.494%, employer 0.692%."""
        assert QPIP_RATE_EMPLOYEE == Decimal("0.00494")
        assert QPIP_RATE_EMPLOYER == Decimal("0.00692")

    def test_ei_difference_is_qpip_offset(self):
        """The EI reduction for QC roughly offsets the QPIP employee premium."""
        ei_savings = EI_RATE_REGULAR - EI_RATE_QUEBEC  # 0.0034
        # QPIP is 0.00494, EI reduction is 0.0034 — QPIP costs more
        assert QPIP_RATE_EMPLOYEE > ei_savings, (
            "QPIP premium exceeds EI reduction — QC employees pay more total"
        )

    def test_rate_just_inside_tolerance(self):
        """Rate within ±0.0005 tolerance should pass."""
        rate_close = EI_RATE_QUEBEC + Decimal("0.0004")
        r = validate_qpip_ei("QC", rate_close)
        assert r["valid"] is True

    def test_rate_just_outside_tolerance(self):
        """Rate outside ±0.0005 tolerance should fail."""
        rate_far = EI_RATE_QUEBEC + Decimal("0.0006")
        r = validate_qpip_ei("QC", rate_far)
        assert r["valid"] is False

    def test_zero_ei_rate_is_error(self):
        """Zero EI rate for any province is wrong."""
        r = validate_qpip_ei("QC", 0)
        assert r["valid"] is False
        r2 = validate_qpip_ei("ON", 0)
        assert r2["valid"] is False

    def test_all_territories_standard_ei(self):
        """Territories (NT, YT, NU) use standard EI rate."""
        for prov in ["NT", "YT", "NU"]:
            r = validate_qpip_ei(prov, EI_RATE_REGULAR)
            assert r["valid"] is True, f"{prov} should use standard EI"


# ===================================================================
# P3. HSF SIZE THRESHOLDS
# ===================================================================

class TestHsfSizeThresholds:
    """Attack HSF rate tier boundaries."""

    def test_zero_payroll(self):
        """Zero payroll should use 0 rate or be valid at 0."""
        r = validate_hsf_rate(0, 0)
        assert r["valid"] is True  # 0 on 0 is fine

    def test_under_1m(self):
        """Payroll ≤ $1M → 1.25%."""
        r = validate_hsf_rate(500_000, Decimal("0.0125"))
        assert r["valid"] is True
        assert r["expected_rate"] == "0.0125"

    def test_exactly_1m_boundary(self):
        """Payroll exactly $1M → still 1.25%."""
        r = validate_hsf_rate(1_000_000, Decimal("0.0125"))
        assert r["valid"] is True

    def test_just_over_1m(self):
        """Payroll $1,000,001 → progressive tier (1.25% base)."""
        r = validate_hsf_rate(1_000_001, Decimal("0.0125"))
        assert r["valid"] is True

    def test_2m_tier(self):
        """Payroll $1M-$2M → rate 0.0125."""
        r = validate_hsf_rate(1_500_000, Decimal("0.0125"))
        assert r["valid"] is True

    def test_exactly_2m_boundary(self):
        """Payroll exactly $2M."""
        r = validate_hsf_rate(2_000_000, Decimal("0.0125"))
        assert r["valid"] is True

    def test_3m_tier(self):
        """Payroll $2M-$3M → 1.65%."""
        r = validate_hsf_rate(2_500_000, Decimal("0.0165"))
        assert r["valid"] is True

    def test_5m_tier(self):
        """Payroll $3M-$5M → 2.00%."""
        r = validate_hsf_rate(4_000_000, Decimal("0.0200"))
        assert r["valid"] is True

    def test_7m_tier(self):
        """Payroll $5M-$7M → 2.50%."""
        r = validate_hsf_rate(6_000_000, Decimal("0.0250"))
        assert r["valid"] is True

    def test_over_7m_max_rate(self):
        """Payroll > $7M → max rate 4.26%."""
        r = validate_hsf_rate(10_000_000, HSF_MAX_RATE)
        assert r["valid"] is True
        assert r["expected_rate"] == "0.0426"

    def test_massive_payroll(self):
        """$100M payroll still uses max rate."""
        r = validate_hsf_rate(100_000_000, HSF_MAX_RATE)
        assert r["valid"] is True

    def test_wrong_rate_under_1m(self):
        """Using max rate on small payroll must fail."""
        r = validate_hsf_rate(500_000, HSF_MAX_RATE)
        assert r["valid"] is False
        assert r["error_type"] == "hsf_rate_error"

    def test_wrong_rate_over_7m(self):
        """Using 1.25% on $10M payroll must fail."""
        r = validate_hsf_rate(10_000_000, Decimal("0.0125"))
        assert r["valid"] is False

    def test_penny_over_boundary(self):
        """$1,000,000.01 — one cent above first tier."""
        r = validate_hsf_rate(Decimal("1000000.01"), Decimal("0.0125"))
        assert r["valid"] is True

    def test_tier_boundaries_exhaustive(self):
        """Every tier boundary returns the correct expected rate."""
        cases = [
            (Decimal("999999.99"), "0.0125"),
            (Decimal("1000000.00"), "0.0125"),
            (Decimal("1000000.01"), "0.0125"),
            (Decimal("2000000.00"), "0.0125"),
            (Decimal("2000000.01"), "0.0165"),
            (Decimal("3000000.00"), "0.0165"),
            (Decimal("3000000.01"), "0.0200"),
            (Decimal("5000000.00"), "0.0200"),
            (Decimal("5000000.01"), "0.0250"),
            (Decimal("7000000.00"), "0.0250"),
            (Decimal("7000000.01"), "0.0426"),
        ]
        for payroll, expected_rate in cases:
            r = validate_hsf_rate(payroll, Decimal(expected_rate))
            assert r["valid"] is True, (
                f"Payroll ${payroll} should have rate {expected_rate}, "
                f"got expected={r['expected_rate']}"
            )

    def test_bilingual_hsf_error(self):
        """HSF error includes both EN and FR descriptions."""
        r = validate_hsf_rate(500_000, Decimal("0.09"))
        assert r["valid"] is False
        assert "HSF" in r["description_en"]
        assert "FSS" in r["description_fr"]


# ===================================================================
# P4. CNESST INDUSTRY DIFFERENCES
# ===================================================================

class TestCnesstIndustryDifferences:
    """Attack CNESST rate validation across industries."""

    def test_office_lowest_rate(self):
        """Office admin (54010) has one of the lowest rates: 0.54%."""
        r = validate_cnesst_rate("54010", Decimal("0.0054"))
        assert r["valid"] is True
        assert r["industry_en"] == "Office / administrative"

    def test_roofing_highest_rate(self):
        """Roofing (23050) has the highest rate: 8.92%."""
        r = validate_cnesst_rate("23050", Decimal("0.0892"))
        assert r["valid"] is True

    def test_construction_vs_office_spread(self):
        """Construction rate is 10x+ higher than office rate."""
        office_rate = CNESST_INDUSTRY_RATES["54010"]["rate"]
        construction_rate = CNESST_INDUSTRY_RATES["23010"]["rate"]
        assert construction_rate > office_rate * 10

    def test_all_known_industries_valid(self):
        """Every industry code in the table validates at its own rate."""
        for code, info in CNESST_INDUSTRY_RATES.items():
            r = validate_cnesst_rate(code, info["rate"])
            assert r["valid"] is True, f"Industry {code} should validate at rate {info['rate']}"

    def test_all_known_industries_reject_zero(self):
        """Every industry should reject a zero rate."""
        for code in CNESST_INDUSTRY_RATES:
            r = validate_cnesst_rate(code, Decimal("0"))
            assert r["valid"] is False, f"Industry {code} should reject rate 0"

    def test_unknown_industry_code(self):
        """Unknown code returns unknown_industry error."""
        r = validate_cnesst_rate("99999", Decimal("0.05"))
        assert r["valid"] is False
        assert r["error_type"] == "cnesst_unknown_industry"

    def test_swapped_industry_rates(self):
        """Using roofing rate for office work must fail."""
        r = validate_cnesst_rate("54010", Decimal("0.0892"))
        assert r["valid"] is False

    def test_using_office_rate_for_construction(self):
        """Using office rate for construction must fail."""
        r = validate_cnesst_rate("23010", Decimal("0.0054"))
        assert r["valid"] is False

    def test_similar_construction_codes_different_rates(self):
        """Different construction sub-codes have different rates."""
        general = CNESST_INDUSTRY_RATES["23010"]["rate"]
        residential = CNESST_INDUSTRY_RATES["23020"]["rate"]
        electrical = CNESST_INDUSTRY_RATES["23030"]["rate"]
        plumbing = CNESST_INDUSTRY_RATES["23040"]["rate"]
        roofing = CNESST_INDUSTRY_RATES["23050"]["rate"]

        # All different from each other
        rates = [general, residential, electrical, plumbing, roofing]
        assert len(set(rates)) == len(rates), "All construction sub-codes should have unique rates"
        # Roofing is highest risk
        assert roofing == max(rates)

    def test_rate_within_tolerance(self):
        """Rate within ±0.0005 tolerance passes."""
        r = validate_cnesst_rate("54010", Decimal("0.0054") + Decimal("0.0004"))
        assert r["valid"] is True

    def test_rate_outside_tolerance(self):
        """Rate outside ±0.0005 tolerance fails."""
        r = validate_cnesst_rate("54010", Decimal("0.0054") + Decimal("0.0006"))
        assert r["valid"] is False

    def test_empty_industry_code(self):
        """Empty string industry code returns unknown."""
        r = validate_cnesst_rate("", Decimal("0.05"))
        assert r["valid"] is False

    def test_bilingual_cnesst_descriptions(self):
        """All industries have EN and FR descriptions."""
        for code, info in CNESST_INDUSTRY_RATES.items():
            assert "description_en" in info, f"{code} missing EN description"
            assert "description_fr" in info, f"{code} missing FR description"


# ===================================================================
# P5. RL-1 vs T4 RECONCILIATION
# ===================================================================

class TestRl1T4Reconciliation:
    """Attack RL-1 / T4 reconciliation logic."""

    def test_perfect_match(self):
        """Identical amounts across all boxes = valid."""
        rl1 = {"A": 50000, "B": 50000, "C": 3200, "D": 12000,
               "E": 50000, "F": 660, "G": 500, "H": 247}
        t4 = {"14": 50000, "26": 50000, "16": 3200, "22": 12000,
              "24": 50000, "18": 660, "44": 500, "55": 247}
        r = reconcile_rl1_t4(rl1, t4)
        assert r["valid"] is True
        assert r["matched_count"] == 8
        assert len(r["mismatches"]) == 0

    def test_one_cent_difference_ok(self):
        """$0.01 difference is within tolerance."""
        rl1 = {"A": Decimal("50000.00")}
        t4 = {"14": Decimal("50000.01")}
        r = reconcile_rl1_t4(rl1, t4)
        assert r["valid"] is True

    def test_two_cent_difference_fails(self):
        """$0.02 difference exceeds tolerance."""
        rl1 = {"A": Decimal("50000.00")}
        t4 = {"14": Decimal("50000.02")}
        r = reconcile_rl1_t4(rl1, t4)
        assert r["valid"] is False
        assert len(r["mismatches"]) == 1
        assert r["mismatches"][0]["rl1_box"] == "A"
        assert r["mismatches"][0]["t4_box"] == "14"

    def test_multiple_mismatches(self):
        """Multiple boxes mismatched — all reported."""
        rl1 = {"A": 50000, "C": 3200, "F": 700}
        t4 = {"14": 49000, "16": 3100, "18": 600}
        r = reconcile_rl1_t4(rl1, t4)
        assert r["valid"] is False
        assert len(r["mismatches"]) == 3

    def test_missing_rl1_box(self):
        """RL-1 box missing but T4 box present → mismatch."""
        rl1 = {}  # no data
        t4 = {"14": 50000}
        r = reconcile_rl1_t4(rl1, t4)
        assert r["valid"] is False
        assert r["mismatches"][0]["rl1_amount"] == "0"
        assert r["mismatches"][0]["t4_amount"] == "50000"

    def test_missing_t4_box(self):
        """T4 box missing but RL-1 present → mismatch."""
        rl1 = {"A": 50000}
        t4 = {}
        r = reconcile_rl1_t4(rl1, t4)
        assert r["valid"] is False

    def test_both_zero_is_skip(self):
        """Both boxes zero → skipped (not counted as match or mismatch)."""
        rl1 = {"G": 0}
        t4 = {"44": 0}
        r = reconcile_rl1_t4(rl1, t4)
        assert r["valid"] is True
        assert r["matched_count"] == 0  # skipped, not matched

    def test_box_mapping_completeness(self):
        """All 8 RL-1↔T4 box mappings exist."""
        expected_rl1_boxes = {"A", "B", "C", "D", "E", "F", "G", "H"}
        assert set(RL1_T4_BOX_MAP.keys()) == expected_rl1_boxes

    def test_qpip_box_h_to_55(self):
        """RL-1 box H (QPIP) maps to T4 box 55."""
        assert RL1_T4_BOX_MAP["H"]["t4_box"] == "55"

    def test_employment_income_box_a_to_14(self):
        """RL-1 box A (employment income) maps to T4 box 14."""
        assert RL1_T4_BOX_MAP["A"]["t4_box"] == "14"

    def test_large_amounts_still_reconcile(self):
        """$10M+ amounts reconcile correctly."""
        rl1 = {"A": Decimal("10000000.00")}
        t4 = {"14": Decimal("10000000.00")}
        r = reconcile_rl1_t4(rl1, t4)
        assert r["valid"] is True

    def test_negative_mismatch_direction(self):
        """Mismatch reports signed difference (RL-1 minus T4)."""
        rl1 = {"A": Decimal("50000")}
        t4 = {"14": Decimal("48000")}
        r = reconcile_rl1_t4(rl1, t4)
        diff = Decimal(r["mismatches"][0]["difference"])
        assert diff == Decimal("2000.00")  # RL-1 > T4 = positive

    def test_reverse_mismatch_direction(self):
        """T4 > RL-1 produces negative difference."""
        rl1 = {"A": Decimal("48000")}
        t4 = {"14": Decimal("50000")}
        r = reconcile_rl1_t4(rl1, t4)
        diff = Decimal(r["mismatches"][0]["difference"])
        assert diff == Decimal("-2000.00")

    def test_bilingual_box_labels(self):
        """All box mappings have EN and FR labels."""
        for box, info in RL1_T4_BOX_MAP.items():
            assert "label_en" in info, f"Box {box} missing EN label"
            assert "label_fr" in info, f"Box {box} missing FR label"


# ===================================================================
# P6. EMPLOYEE CROSSES PROVINCE MID-YEAR
# ===================================================================

class TestMidYearProvinceCrossing:
    """Attack pro-rated deductions when employee moves QC ↔ ROC."""

    def test_full_year_qc(self):
        """12 months QC, 0 outside = 100% QPP, QC EI, QPIP."""
        r = prorate_province_deductions(12, 0, 60000)
        assert r["valid"] is True
        assert Decimal(r["cpp_deduction"]) == Decimal("0.00")
        assert Decimal(r["qpip_employee"]) > 0
        assert r["rl1_required"] is True

    def test_full_year_outside_qc(self):
        """0 months QC, 12 outside = 100% CPP, full EI, no QPIP."""
        r = prorate_province_deductions(0, 12, 60000)
        assert r["valid"] is True
        assert Decimal(r["qpp_deduction"]) == Decimal("0.00")
        assert Decimal(r["qpip_employee"]) == Decimal("0.00")
        assert r["rl1_required"] is False
        assert r["t4_required"] is True

    def test_half_year_split(self):
        """6 months QC, 6 months ON = 50/50 split."""
        r = prorate_province_deductions(6, 6, 60000)
        assert r["valid"] is True
        qc_gross = Decimal(r["qc_gross"])
        roc_gross = Decimal(r["roc_gross"])
        assert qc_gross == Decimal("30000.00")
        assert roc_gross == Decimal("30000.00")
        # Both QPP and CPP should be non-zero
        assert Decimal(r["qpp_deduction"]) > 0
        assert Decimal(r["cpp_deduction"]) > 0
        # Both RL-1 and T4 required
        assert r["rl1_required"] is True
        assert r["t4_required"] is True

    def test_one_month_qc_rest_outside(self):
        """1 month QC (e.g., January move) — still needs RL-1."""
        r = prorate_province_deductions(1, 11, 60000)
        assert r["valid"] is True
        assert r["rl1_required"] is True
        qc_gross = Decimal(r["qc_gross"])
        assert qc_gross == Decimal("5000.00")  # 60000 * 1/12

    def test_eleven_months_qc(self):
        """11 months QC, 1 month ON — mostly QPP."""
        r = prorate_province_deductions(11, 1, 60000)
        qpp = Decimal(r["qpp_deduction"])
        cpp = Decimal(r["cpp_deduction"])
        assert qpp > cpp * 10  # ~11x more QPP

    def test_gross_split_adds_up(self):
        """QC gross + ROC gross should approximately equal annual gross."""
        r = prorate_province_deductions(7, 5, 84000)
        total = Decimal(r["qc_gross"]) + Decimal(r["roc_gross"])
        assert total == Decimal("84000.00")

    def test_ei_rates_applied_correctly(self):
        """QC portion uses 1.32%, ROC uses 1.66%."""
        r = prorate_province_deductions(6, 6, 60000)
        ei_qc = Decimal(r["ei_qc_portion"])
        ei_roc = Decimal(r["ei_roc_portion"])
        # Same gross, but QC EI should be lower
        assert ei_qc < ei_roc

    def test_qpip_only_on_qc_months(self):
        """QPIP should only be calculated on QC gross."""
        r = prorate_province_deductions(3, 9, 60000)
        qpip = Decimal(r["qpip_employee"])
        qc_gross = Decimal(r["qc_gross"])
        expected_qpip = _round(qc_gross * QPIP_RATE_EMPLOYEE)
        assert qpip == expected_qpip

    def test_zero_months_invalid(self):
        """0 + 0 months = invalid."""
        r = prorate_province_deductions(0, 0, 60000)
        assert r["valid"] is False
        assert r["error_type"] == "invalid_months"

    def test_over_12_months_invalid(self):
        """7 + 7 = 14 months is impossible."""
        r = prorate_province_deductions(7, 7, 60000)
        assert r["valid"] is False
        assert r["error_type"] == "invalid_months"

    def test_exactly_12_months_valid(self):
        """12 total months in any split is valid."""
        for qc in range(0, 13):
            roc = 12 - qc
            r = prorate_province_deductions(qc, roc, 60000)
            assert r["valid"] is True, f"{qc} QC + {roc} ROC should be valid"

    def test_pension_total_equals_sum(self):
        """total_pension = qpp_deduction + cpp_deduction."""
        r = prorate_province_deductions(4, 8, 72000)
        total = Decimal(r["total_pension"])
        qpp = Decimal(r["qpp_deduction"])
        cpp = Decimal(r["cpp_deduction"])
        assert total == _round(qpp + cpp)

    def test_ei_total_equals_sum(self):
        """total_ei = ei_qc_portion + ei_roc_portion."""
        r = prorate_province_deductions(5, 7, 72000)
        total = Decimal(r["total_ei"])
        ei_qc = Decimal(r["ei_qc_portion"])
        ei_roc = Decimal(r["ei_roc_portion"])
        assert total == _round(ei_qc + ei_roc)


# ===================================================================
# P7. TAXABLE BENEFIT MISCLASSIFICATION
# ===================================================================

class TestTaxableBenefitMisclassification:
    """Attack taxable benefit classification for all known benefit types."""

    def test_vehicle_fully_classified(self):
        """Personal vehicle use — taxable, pensionable, insurable."""
        r = validate_taxable_benefit(
            "personal_vehicle_use", True, 5000,
            included_in_pensionable=True, included_in_insurable=True,
        )
        assert r["valid"] is True

    def test_vehicle_not_reported_taxable(self):
        """Not reporting vehicle benefit as taxable = error."""
        r = validate_taxable_benefit(
            "personal_vehicle_use", False, 5000,
        )
        assert r["valid"] is False
        assert any(e["error_type"] == "benefit_not_reported_taxable" for e in r["errors"])

    def test_vehicle_not_pensionable(self):
        """Vehicle benefit excluded from pensionable earnings = error."""
        r = validate_taxable_benefit(
            "personal_vehicle_use", True, 5000,
            included_in_pensionable=False, included_in_insurable=True,
        )
        assert r["valid"] is False
        assert any(e["error_type"] == "benefit_not_in_pensionable" for e in r["errors"])

    def test_vehicle_not_insurable(self):
        """Vehicle benefit excluded from insurable earnings = error."""
        r = validate_taxable_benefit(
            "personal_vehicle_use", True, 5000,
            included_in_pensionable=True, included_in_insurable=False,
        )
        assert r["valid"] is False
        assert any(e["error_type"] == "benefit_not_in_insurable" for e in r["errors"])

    def test_vehicle_triple_misclassification(self):
        """All three flags wrong = three errors."""
        r = validate_taxable_benefit(
            "personal_vehicle_use", False, 5000,
            included_in_pensionable=False, included_in_insurable=False,
        )
        assert r["valid"] is False
        assert len(r["errors"]) == 3

    def test_group_life_insurance_not_pensionable(self):
        """Group life insurance is NOT pensionable — only taxable + insurable."""
        r = validate_taxable_benefit(
            "group_life_insurance", True, 1200,
            included_in_pensionable=False, included_in_insurable=True,
        )
        assert r["valid"] is True  # pensionable=False is correct

    def test_group_life_insurance_wrongly_pensionable(self):
        """Marking group life as pensionable is not an error per se
        (we only flag when required flag is missing, not when extra)."""
        r = validate_taxable_benefit(
            "group_life_insurance", True, 1200,
            included_in_pensionable=True, included_in_insurable=True,
        )
        assert r["valid"] is True  # No error for extra inclusion

    def test_stock_option_not_pensionable_not_insurable(self):
        """Stock options: taxable but NOT pensionable, NOT insurable."""
        r = validate_taxable_benefit(
            "stock_option", True, 25000,
            included_in_pensionable=False, included_in_insurable=False,
        )
        assert r["valid"] is True

    def test_stock_option_on_rl1_box_a(self):
        """Stock option goes into RL-1 box A (employment income)."""
        r = validate_taxable_benefit(
            "stock_option", True, 25000,
            included_in_pensionable=False, included_in_insurable=False,
        )
        assert r["rl1_box"] == "A"

    def test_unknown_benefit_type(self):
        """Unknown benefit type = error."""
        r = validate_taxable_benefit("flying_carpet", True, 100)
        assert r["valid"] is False
        assert r["error_type"] == "unknown_benefit_type"

    def test_all_benefits_have_rl1_box(self):
        """Every known benefit maps to an RL-1 box."""
        for btype, info in TAXABLE_BENEFITS.items():
            assert "rl1_box" in info, f"{btype} missing rl1_box"
            assert info["rl1_box"] in ("A", "J", "L"), (
                f"{btype} has unexpected RL-1 box: {info['rl1_box']}"
            )

    def test_all_benefits_bilingual(self):
        """Every benefit has EN and FR labels."""
        for btype, info in TAXABLE_BENEFITS.items():
            assert "label_en" in info
            assert "label_fr" in info

    def test_zero_amount_benefit(self):
        """$0 benefit that is not reported taxable still flags error
        (it's about classification, not materiality)."""
        r = validate_taxable_benefit(
            "parking", False, 0,
        )
        assert r["valid"] is False

    def test_large_benefit_all_errors_include_amount(self):
        """Error messages should include the dollar amount."""
        r = validate_taxable_benefit(
            "housing_allowance", False, Decimal("2500.00"),
        )
        for err in r["errors"]:
            assert "2500" in err["description_en"]
            assert "2500" in err["description_fr"]

    def test_low_interest_loan_not_pensionable_ok(self):
        """Low-interest loan is NOT pensionable — correct classification."""
        r = validate_taxable_benefit(
            "low_interest_loan", True, 800,
            included_in_pensionable=False, included_in_insurable=True,
        )
        assert r["valid"] is True

    def test_gifts_over_500_fully_taxable(self):
        """Gifts/awards >$500 are fully taxable, pensionable, insurable."""
        r = validate_taxable_benefit(
            "gifts_awards_over_500", True, 750,
            included_in_pensionable=True, included_in_insurable=True,
        )
        assert r["valid"] is True

    def test_gifts_over_500_missing_all_flags(self):
        """Gifts not classified at all = 3 errors."""
        r = validate_taxable_benefit(
            "gifts_awards_over_500", False, 750,
        )
        assert len(r["errors"]) == 3


# ===================================================================
# P8. CROSS-CUTTING: COMBINED SCENARIO
# ===================================================================

class TestCombinedPayrollScenario:
    """End-to-end scenario: employee with multiple compliance checks."""

    def test_qc_employee_full_compliance(self):
        """QC employee: QPP, reduced EI, QPIP, correct HSF, CNESST, RL-1=T4."""
        # QPP
        assert validate_qpp_cpp("QC", "QPP")["valid"] is True
        # EI
        assert validate_qpip_ei("QC", EI_RATE_QUEBEC)["valid"] is True
        # HSF
        assert validate_hsf_rate(500_000, Decimal("0.0125"))["valid"] is True
        # CNESST
        assert validate_cnesst_rate("54010", Decimal("0.0054"))["valid"] is True
        # RL-1 / T4 reconciliation
        rl1 = {"A": 50000, "C": 3200, "F": 660, "H": 247}
        t4 = {"14": 50000, "16": 3200, "18": 660, "55": 247}
        assert reconcile_rl1_t4(rl1, t4)["valid"] is True

    def test_qc_employee_every_check_wrong(self):
        """QC employee with every single check failing."""
        # Wrong pension plan
        assert validate_qpp_cpp("QC", "CPP")["valid"] is False
        # Wrong EI rate
        assert validate_qpip_ei("QC", EI_RATE_REGULAR)["valid"] is False
        # Wrong HSF rate
        assert validate_hsf_rate(500_000, HSF_MAX_RATE)["valid"] is False
        # Wrong CNESST rate
        assert validate_cnesst_rate("54010", Decimal("0.0892"))["valid"] is False
        # RL-1 / T4 mismatch
        rl1 = {"A": 50000}
        t4 = {"14": 45000}
        assert reconcile_rl1_t4(rl1, t4)["valid"] is False

    def test_mid_year_move_then_reconcile(self):
        """Employee moves QC→ON in July, then reconcile RL-1/T4."""
        r = prorate_province_deductions(6, 6, 60000)
        assert r["valid"] is True
        assert r["rl1_required"] is True

        # RL-1 should only cover QC portion
        qc_gross = Decimal(r["qc_gross"])
        qpp = Decimal(r["qpp_deduction"])
        ei_qc = Decimal(r["ei_qc_portion"])
        qpip = Decimal(r["qpip_employee"])

        # T4 covers full year
        total_pension = Decimal(r["total_pension"])
        total_ei = Decimal(r["total_ei"])

        # RL-1 box A = QC gross only, T4 box 14 = full gross
        rl1 = {"A": qc_gross, "C": qpp, "F": ei_qc, "H": qpip}
        t4 = {"14": Decimal("60000"), "16": total_pension,
              "18": total_ei, "55": qpip}

        recon = reconcile_rl1_t4(rl1, t4)
        # This SHOULD have mismatches because RL-1 only covers QC portion
        assert recon["valid"] is False
        assert len(recon["mismatches"]) > 0, (
            "Mid-year move: RL-1 (QC only) vs T4 (full year) must mismatch"
        )

    def test_benefit_on_province_crossing_employee(self):
        """Vehicle benefit on employee who crossed provinces."""
        r = prorate_province_deductions(8, 4, 72000)
        assert r["valid"] is True

        # Benefit must still be reported
        b = validate_taxable_benefit(
            "personal_vehicle_use", True, 3600,
            included_in_pensionable=True, included_in_insurable=True,
        )
        assert b["valid"] is True
