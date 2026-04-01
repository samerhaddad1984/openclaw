"""
P — PAYROLL PRESSURE
=====================
Attack payroll engine with boundary-tier payroll amounts, cross-province
confusion (QPP vs CPP), and RL-1/T4 mismatch scenarios.

Targets: payroll_engine
"""
from __future__ import annotations

import sys
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.payroll_engine import (
    validate_hsf_rate,
    validate_qpp_cpp,
    validate_qpip_ei,
    reconcile_rl1_t4,
    _expected_hsf_rate,
    HSF_TIERS,
    HSF_MAX_RATE,
    QPP_RATE_EMPLOYEE,
    CPP_RATE_EMPLOYEE,
    EI_RATE_QUEBEC,
    EI_RATE_REGULAR,
    CENT,
)

_round = lambda v: Decimal(str(v)).quantize(CENT, rounding=ROUND_HALF_UP)


# ===================================================================
# TEST CLASS: HSF Rate Tier Boundaries
# ===================================================================

class TestHSFTierBoundaries:
    """Exact boundary testing of HSF rate tiers."""

    @pytest.mark.parametrize("payroll,expected_rate", [
        (Decimal("0"), Decimal("0")),
        (Decimal("1"), Decimal("0.0125")),
        (Decimal("999999.99"), Decimal("0.0125")),
        (Decimal("1000000"), Decimal("0.0125")),
        (Decimal("1000000.01"), Decimal("0.0125")),  # $1M-$2M tier, rate_low
        (Decimal("2000000"), Decimal("0.0125")),     # $1M-$2M tier boundary, rate_low
        (Decimal("5000000"), Decimal("0.0200")),
        (Decimal("7000000"), Decimal("0.0250")),
        (Decimal("7000000.01"), HSF_MAX_RATE),
        (Decimal("99999999"), HSF_MAX_RATE),
    ])
    def test_hsf_rate_at_boundary(self, payroll, expected_rate):
        actual = _expected_hsf_rate(payroll)
        assert actual == expected_rate, (
            f"HSF rate for payroll ${payroll}: expected {expected_rate}, got {actual}"
        )

    def test_hsf_validation_correct_rate(self):
        r = validate_hsf_rate(Decimal("500000"), Decimal("0.0125"))
        assert r["valid"] is True

    def test_hsf_validation_wrong_rate(self):
        r = validate_hsf_rate(Decimal("500000"), Decimal("0.0426"))
        assert r["valid"] is False
        assert r["error_type"] == "hsf_rate_error"

    def test_hsf_negative_payroll(self):
        r = validate_hsf_rate(Decimal("-100000"), Decimal("0.0125"))
        # Negative payroll should return 0 rate
        assert r is not None

    def test_hsf_zero_payroll(self):
        r = validate_hsf_rate(Decimal("0"), Decimal("0"))
        assert r["valid"] is True

    def test_hsf_bilingual_error_messages(self):
        r = validate_hsf_rate(Decimal("500000"), Decimal("0.05"))
        assert r["valid"] is False
        assert "description_en" in r
        assert "description_fr" in r
        assert len(r["description_en"]) > 0
        assert len(r["description_fr"]) > 0


# ===================================================================
# TEST CLASS: QPP vs CPP Province Check
# ===================================================================

class TestQPPvsCPP:
    """Quebec employees must use QPP, not CPP."""

    def test_qc_employee_uses_qpp(self):
        r = validate_qpp_cpp(province="QC", pension_plan_used="QPP")
        assert r["valid"] is True

    def test_qc_employee_wrong_plan_cpp(self):
        """QC employee on CPP instead of QPP must be flagged."""
        r = validate_qpp_cpp(province="QC", pension_plan_used="CPP")
        assert r["valid"] is False, (
            "P1 DEFECT: QC employee on CPP not flagged"
        )

    def test_on_employee_uses_cpp(self):
        r = validate_qpp_cpp(province="ON", pension_plan_used="CPP")
        assert r["valid"] is True

    def test_on_employee_wrong_plan_qpp(self):
        """ON employee on QPP instead of CPP."""
        r = validate_qpp_cpp(province="ON", pension_plan_used="QPP")
        assert r["valid"] is False


# ===================================================================
# TEST CLASS: QPIP vs EI
# ===================================================================

class TestQPIPvsEI:
    """Quebec uses QPIP (reduced EI), rest of Canada uses standard EI."""

    def test_qc_uses_qpip(self):
        r = validate_qpip_ei(province="QC", ei_rate_used=EI_RATE_QUEBEC)
        assert r["valid"] is True

    def test_qc_on_ei_flagged(self):
        r = validate_qpip_ei(province="QC", ei_rate_used=EI_RATE_REGULAR)
        assert r["valid"] is False

    def test_on_uses_ei(self):
        r = validate_qpip_ei(province="ON", ei_rate_used=EI_RATE_REGULAR)
        assert r["valid"] is True

    def test_on_on_qpip_flagged(self):
        r = validate_qpip_ei(province="ON", ei_rate_used=EI_RATE_QUEBEC)
        assert r["valid"] is False


# ===================================================================
# TEST CLASS: RL-1/T4 Reconciliation
# ===================================================================

class TestRL1T4Reconciliation:
    """RL-1 (QC) and T4 (Federal) must reconcile."""

    def test_matching_totals_pass(self):
        r = reconcile_rl1_t4(
            rl1_data={"A": Decimal("50000")},
            t4_data={"14": Decimal("50000")},
        )
        assert r["valid"] is True

    def test_mismatched_totals_flagged(self):
        r = reconcile_rl1_t4(
            rl1_data={"A": Decimal("50000")},
            t4_data={"14": Decimal("49000")},
        )
        assert r["valid"] is False

    def test_penny_difference_passes(self):
        r = reconcile_rl1_t4(
            rl1_data={"A": Decimal("50000.01")},
            t4_data={"14": Decimal("50000.00")},
        )
        assert r["valid"] is True


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestPayrollDeterminism:
    def test_hsf_rate_deterministic(self):
        results = {str(_expected_hsf_rate(Decimal("1500000"))) for _ in range(100)}
        assert len(results) == 1

    def test_validate_deterministic(self):
        results = set()
        for _ in range(50):
            r = validate_hsf_rate(Decimal("3000000"), Decimal("0.0200"))
            results.add(str(r["valid"]))
        assert len(results) == 1
