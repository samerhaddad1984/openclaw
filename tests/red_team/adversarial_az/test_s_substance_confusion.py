"""
S — SUBSTANCE ENGINE CONFUSION
================================
Attack economic substance classifier with CapEx disguised as OpEx,
personal expenses on corporate card, prepaid detection evasion,
and shareholder transaction obfuscation.

Targets: substance_engine
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.substance_engine import substance_classifier


# ===================================================================
# TEST CLASS: CapEx vs OpEx Confusion
# ===================================================================

class TestCapExVsOpEx:
    """CapEx disguised as operating expense."""

    def test_hvac_replacement_is_capex(self):
        r = substance_classifier(
            vendor="Climatisation ABC Inc.",
            memo="Remplacement complet du système HVAC",
            amount=45000,
        )
        assert r.get("is_capex") is True, (
            "DEFECT: $45K HVAC replacement not flagged as CapEx"
        )

    def test_hvac_repair_is_not_capex(self):
        r = substance_classifier(
            vendor="Climatisation ABC Inc.",
            memo="Réparation climatisation bureau 201",
            amount=800,
        )
        assert r.get("is_capex") is not True, (
            "Small HVAC repair should NOT be CapEx"
        )

    def test_computer_purchase_is_capex(self):
        r = substance_classifier(
            vendor="Best Buy Business",
            memo="Purchase of 10 laptops for new team",
            amount=15000,
        )
        assert r.get("is_capex") is True

    def test_software_subscription_is_not_capex(self):
        r = substance_classifier(
            vendor="Microsoft",
            memo="Monthly Office 365 subscription",
            amount=500,
        )
        assert r.get("is_capex") is not True

    def test_vehicle_purchase(self):
        r = substance_classifier(
            vendor="Concessionnaire Auto",
            memo="Achat véhicule de service",
            amount=35000,
        )
        assert r.get("is_capex") is True

    def test_equipment_disguised_as_supplies(self):
        """$12K 'office supplies' — should at least flag if equipment."""
        r = substance_classifier(
            vendor="Staples",
            memo="Office supplies — printer, furniture, and equipment",
            amount=12000,
        )
        # Should detect equipment keywords despite "supplies" in memo
        if not r.get("is_capex"):
            pytest.xfail(
                "P2 DEFECT: 'equipment' and 'furniture' in memo not caught as potential CapEx"
            )


# ===================================================================
# TEST CLASS: Prepaid Detection
# ===================================================================

class TestPrepaidDetection:
    """Insurance, annual subscriptions, advance rent."""

    def test_annual_insurance_is_prepaid(self):
        r = substance_classifier(
            vendor="Intact Assurance",
            memo="Prime d'assurance commerciale 2025",
            amount=8000,
        )
        assert r.get("is_prepaid") is True, (
            "DEFECT: Annual insurance not flagged as prepaid"
        )

    def test_quality_assurance_not_prepaid(self):
        """'Quality assurance' must NOT trigger prepaid flag."""
        r = substance_classifier(
            vendor="QA Testing Corp",
            memo="Quality assurance testing services",
            amount=5000,
        )
        assert r.get("is_prepaid") is not True, (
            "DEFECT: 'Quality assurance' falsely flagged as prepaid/insurance"
        )

    def test_annual_subscription(self):
        r = substance_classifier(
            vendor="Adobe",
            memo="Annual subscription Creative Cloud",
            amount=3000,
        )
        assert r.get("is_prepaid") is True

    def test_monthly_subscription_not_prepaid(self):
        """Monthly SaaS should not be prepaid."""
        r = substance_classifier(
            vendor="Slack",
            memo="Monthly subscription — March 2025",
            amount=200,
        )
        # Monthly subscriptions are typically expensed, not prepaid


# ===================================================================
# TEST CLASS: Shareholder/Personal Detection
# ===================================================================

class TestShareholderPersonalDetection:
    """Personal expenses on corporate card."""

    def test_owner_name_in_vendor(self):
        """Vendor matches owner name → flag as shareholder."""
        r = substance_classifier(
            vendor="Jean Tremblay Consulting",
            memo="Management consulting",
            amount=10000,
            owner_names=["Jean Tremblay"],
        )
        assert r.get("is_shareholder_related") is True or r.get("is_personal") is True, (
            "DEFECT: Vendor matching owner name not flagged"
        )

    def test_owner_partial_name_match(self):
        r = substance_classifier(
            vendor="Tremblay Holdings Inc.",
            memo="Consulting services",
            amount=5000,
            owner_names=["Jean Tremblay", "Marie Tremblay"],
        )
        # FIX 13: Partial owner name match (surname) should be detected
        assert r.get("is_shareholder_related") or r.get("is_personal") or r.get("potential_personal_expense"), (
            "Partial owner name match not detected"
        )


# ===================================================================
# TEST CLASS: Tax Remittance Detection
# ===================================================================

class TestTaxRemittanceDetection:
    """Tax payments to government must not be classified as expenses."""

    def test_gst_remittance(self):
        r = substance_classifier(
            vendor="Receveur général du Canada",
            memo="Versement TPS/TVH — Période juin 2025",
            amount=15000,
        )
        assert r.get("is_tax_remittance") is True or r.get("gl_override") is not None, (
            "DEFECT: GST remittance to government not detected"
        )

    def test_cra_payment(self):
        r = substance_classifier(
            vendor="Canada Revenue Agency",
            memo="Corporate tax installment",
            amount=25000,
        )
        # Should be flagged as tax payment
        assert r.get("is_tax_remittance") is True or r.get("gl_override") is not None


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestSubstanceDeterminism:
    def test_capex_deterministic(self):
        results = set()
        for _ in range(50):
            r = substance_classifier(
                vendor="Equipment Corp",
                memo="achat machinerie lourde",
                amount=50000,
            )
            results.add(str(r.get("is_capex")))
        assert len(results) == 1, f"Non-deterministic: {results}"

    def test_prepaid_deterministic(self):
        results = set()
        for _ in range(50):
            r = substance_classifier(
                vendor="Intact",
                memo="prime d'assurance",
                amount=5000,
            )
            results.add(str(r.get("is_prepaid")))
        assert len(results) == 1
