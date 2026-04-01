"""
tests/red_team/test_boss_07_payroll_year_end.py
===============================================
BOSS FIGHT 7 — Payroll Year-End Grenade.

RL-1/T4 mismatch, QPP/CPP edge, HSF threshold, CNESST reclass,
mid-year province crossing, taxable benefit misclassification.
"""
from __future__ import annotations

import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.payroll_engine import (
    CENT,
    CPP_RATE_EMPLOYEE,
    EI_RATE_QUEBEC,
    EI_RATE_REGULAR,
    HSF_MAX_RATE,
    HSF_TIERS,
    QPP_RATE_EMPLOYEE,
    QPIP_RATE_EMPLOYEE,
    QPIP_RATE_EMPLOYER,
    RL1_T4_BOX_MAP,
    TAXABLE_BENEFITS,
    _expected_hsf_rate,
    prorate_province_deductions,
    reconcile_rl1_t4,
    validate_cnesst_rate,
    validate_hsf_rate,
    validate_qpip_ei,
    validate_qpp_cpp,
    validate_taxable_benefit,
)

_ROUND = lambda v: Decimal(str(v)).quantize(CENT, rounding=ROUND_HALF_UP)


class TestRL1T4Mismatch:
    """RL-1 (Quebec) vs T4 (federal) reconciliation edge cases."""

    def test_perfect_match(self):
        """All boxes match exactly — should be valid."""
        rl1 = {"A": 50000, "B": 50000, "C": 3200, "D": 8000,
               "E": 50000, "F": 660, "G": 500, "H": 247}
        t4 = {"14": 50000, "26": 50000, "16": 3200, "22": 8000,
              "24": 50000, "18": 660, "44": 500, "55": 247}
        result = reconcile_rl1_t4(rl1, t4)
        assert result["valid"]
        assert len(result["mismatches"]) == 0

    def test_employment_income_mismatch(self):
        """RL-1 Box A ≠ T4 Box 14 — must flag."""
        rl1 = {"A": 55000, "B": 50000}
        t4 = {"14": 50000, "26": 50000}
        result = reconcile_rl1_t4(rl1, t4)
        assert not result["valid"]
        assert any(m["rl1_box"] == "A" for m in result["mismatches"])

    def test_qpp_contribution_mismatch(self):
        """RL-1 Box C (QPP) ≠ T4 Box 16 — critical for year-end."""
        rl1 = {"C": 3200}
        t4 = {"16": 2975}  # CPP rate instead of QPP rate
        result = reconcile_rl1_t4(rl1, t4)
        assert not result["valid"]
        mm = [m for m in result["mismatches"] if m["rl1_box"] == "C"]
        assert len(mm) == 1

    def test_ei_premium_mismatch_quebec_vs_federal(self):
        """Quebec reduced EI vs federal full EI — different amounts."""
        qc_ei = _ROUND(Decimal("50000") * EI_RATE_QUEBEC)
        fed_ei = _ROUND(Decimal("50000") * EI_RATE_REGULAR)
        assert qc_ei < fed_ei  # Quebec rate is lower

        rl1 = {"F": float(qc_ei)}
        t4 = {"18": float(fed_ei)}  # Wrong: used federal rate for Quebec employee
        result = reconcile_rl1_t4(rl1, t4)
        assert not result["valid"]

    def test_all_boxes_zero_is_valid(self):
        """All-zero RL-1 and T4 should be valid (no mismatches)."""
        rl1 = {k: 0 for k in RL1_T4_BOX_MAP.keys()}
        t4 = {v["t4_box"]: 0 for v in RL1_T4_BOX_MAP.values()}
        result = reconcile_rl1_t4(rl1, t4)
        assert result["valid"]


class TestQPPCPPEdge:
    """QPP vs CPP province validation edge cases."""

    def test_quebec_must_use_qpp(self):
        result = validate_qpp_cpp("QC", "QPP")
        assert result["valid"]

    def test_quebec_using_cpp_is_error(self):
        result = validate_qpp_cpp("QC", "CPP")
        assert not result["valid"]
        assert result["error_type"] == "qpp_cpp_error"

    def test_ontario_must_use_cpp(self):
        result = validate_qpp_cpp("ON", "CPP")
        assert result["valid"]

    def test_ontario_using_qpp_is_error(self):
        result = validate_qpp_cpp("ON", "QPP")
        assert not result["valid"]

    def test_all_provinces_have_correct_plan(self):
        """Every province must map to the correct pension plan."""
        provinces = {
            "QC": "QPP", "ON": "CPP", "BC": "CPP", "AB": "CPP",
            "SK": "CPP", "MB": "CPP", "NB": "CPP", "NS": "CPP",
            "NL": "CPP", "PE": "CPP", "NT": "CPP", "NU": "CPP", "YT": "CPP",
        }
        for prov, expected_plan in provinces.items():
            result = validate_qpp_cpp(prov, expected_plan)
            assert result["valid"], f"{prov} should use {expected_plan}"


class TestHSFThreshold:
    """Health Services Fund rate tier validation."""

    def test_small_payroll_rate(self):
        """Payroll ≤ $1M → 1.25%."""
        result = validate_hsf_rate(500000, 0.0125)
        assert result["valid"]

    def test_small_payroll_wrong_rate(self):
        result = validate_hsf_rate(500000, 0.0200)
        assert not result["valid"]

    def test_large_payroll_max_rate(self):
        """Payroll > $7M → 4.26%."""
        result = validate_hsf_rate(10000000, float(HSF_MAX_RATE))
        assert result["valid"]

    def test_boundary_payrolls(self):
        """Test exact tier boundaries."""
        boundaries = [
            (1000000, Decimal("0.0125")),
            (2000000, Decimal("0.0125")),
            (3000000, Decimal("0.0165")),
            (5000000, Decimal("0.0200")),
            (7000000, Decimal("0.0250")),
            (7000001, HSF_MAX_RATE),
        ]
        for payroll, expected in boundaries:
            actual = _expected_hsf_rate(Decimal(str(payroll)))
            assert actual == expected, f"Payroll ${payroll}: expected {expected}, got {actual}"

    def test_zero_payroll(self):
        """Zero payroll → zero rate."""
        rate = _expected_hsf_rate(Decimal("0"))
        assert rate == Decimal("0")


class TestCNESSTReclass:
    """CNESST premium rate by industry."""

    def test_office_rate(self):
        result = validate_cnesst_rate("54010", 0.0054)
        assert result["valid"]

    def test_construction_rate(self):
        result = validate_cnesst_rate("23010", 0.0585)
        assert result["valid"]

    def test_wrong_rate_for_construction(self):
        """Using office rate for construction is a major error."""
        result = validate_cnesst_rate("23010", 0.0054)
        assert not result["valid"]
        assert result["error_type"] == "cnesst_rate_error"

    def test_unknown_industry_code(self):
        result = validate_cnesst_rate("99999", 0.01)
        assert not result["valid"]
        assert result["error_type"] == "cnesst_unknown_industry"

    def test_roofing_highest_rate(self):
        """Roofing (23050) has one of the highest rates."""
        result = validate_cnesst_rate("23050", 0.0892)
        assert result["valid"]


class TestQPIPEI:
    """QPIP and EI rate validation."""

    def test_quebec_reduced_ei(self):
        result = validate_qpip_ei("QC", float(EI_RATE_QUEBEC))
        assert result["valid"]

    def test_quebec_full_ei_is_error(self):
        """Quebec employee paying full EI rate (non-reduced) is wrong."""
        result = validate_qpip_ei("QC", float(EI_RATE_REGULAR))
        assert not result["valid"]

    def test_ontario_full_ei(self):
        result = validate_qpip_ei("ON", float(EI_RATE_REGULAR))
        assert result["valid"]

    def test_ontario_reduced_ei_is_error(self):
        """Non-Quebec employee paying Quebec reduced EI rate."""
        result = validate_qpip_ei("ON", float(EI_RATE_QUEBEC))
        assert not result["valid"]


class TestMidYearProvinceCrossing:
    """Employee moves QC ↔ ROC mid-year."""

    def test_full_year_quebec(self):
        result = prorate_province_deductions(12, 0, 60000)
        assert result["valid"]
        assert result["rl1_required"]
        assert Decimal(result["qpp_deduction"]) > Decimal("0")
        assert Decimal(result["cpp_deduction"]) == Decimal("0")

    def test_full_year_outside_quebec(self):
        result = prorate_province_deductions(0, 12, 60000)
        assert result["valid"]
        assert not result["rl1_required"]
        assert Decimal(result["qpp_deduction"]) == Decimal("0")
        assert Decimal(result["cpp_deduction"]) > Decimal("0")

    def test_half_year_split(self):
        """6 months QC, 6 months ON — both QPP and CPP should apply."""
        result = prorate_province_deductions(6, 6, 80000)
        assert result["valid"]
        assert Decimal(result["qpp_deduction"]) > Decimal("0")
        assert Decimal(result["cpp_deduction"]) > Decimal("0")
        assert result["rl1_required"]
        assert result["t4_required"]

    def test_invalid_months_rejected(self):
        """Total months > 12 or 0 must be rejected."""
        result = prorate_province_deductions(8, 6, 60000)
        assert not result["valid"]
        result2 = prorate_province_deductions(0, 0, 60000)
        assert not result2["valid"]

    def test_proration_amounts_sum_correctly(self):
        """QC gross + ROC gross should equal total annual gross."""
        result = prorate_province_deductions(4, 8, 120000)
        assert result["valid"]
        qc = Decimal(result["qc_gross"])
        roc = Decimal(result["roc_gross"])
        assert abs(qc + roc - Decimal("120000")) <= Decimal("0.01")


class TestTaxableBenefits:
    """Taxable benefit misclassification detection."""

    def test_vehicle_benefit_is_taxable(self):
        result = validate_taxable_benefit("personal_vehicle_use", reported_as_taxable=True, amount=1000,
                                           included_in_pensionable=True, included_in_insurable=True)
        assert result["valid"]

    def test_vehicle_benefit_not_reported_taxable(self):
        result = validate_taxable_benefit("personal_vehicle_use", reported_as_taxable=False, amount=1000,
                                           included_in_pensionable=True, included_in_insurable=True)
        assert not result["valid"]

    def test_unknown_benefit_type(self):
        result = validate_taxable_benefit("imaginary_benefit", reported_as_taxable=True, amount=1000,
                                           included_in_pensionable=False, included_in_insurable=False)
        assert not result["valid"]

    def test_all_known_benefits_validate(self):
        """Every known benefit type should validate when reported correctly."""
        for benefit_type, info in TAXABLE_BENEFITS.items():
            result = validate_taxable_benefit(
                benefit_type,
                reported_as_taxable=info["taxable_federal"],
                amount=1000,
                included_in_pensionable=info["pensionable"],
                included_in_insurable=info["insurable"],
            )
            assert result["valid"], f"Benefit {benefit_type} failed validation"
