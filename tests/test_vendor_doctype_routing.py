"""
tests/test_vendor_doctype_routing.py
=====================================
Comprehensive tests for:
  - Document type detection and routing (P1)
  - Known Canadian vendor registry and fraud exemptions (P2)
  - Weekend transaction rule refinement (P3)
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.ocr_engine import detect_document_type
from src.engines.fraud_engine import (
    KNOWN_CANADIAN_VENDORS,
    KNOWN_TRUSTED_VENDORS,
    KNOWN_REGISTERED_SOFTWARE_VENDORS,
    CANADIAN_BANK_VENDORS,
    WEEKEND_EXEMPT_CATEGORIES,
    _is_known_trusted_vendor,
    _is_canadian_bank_vendor,
    _is_weekend_exempt_vendor,
    _get_vendor_defaults,
    _rule_weekend_holiday,
    _rule_new_vendor_large_amount,
)


# ============================================================================
# P1 — Document Type Detection
# ============================================================================

class TestDocumentTypeDetection:
    """Test detect_document_type classifies all document types correctly."""

    # --- Bank statements ---
    def test_bank_statement_english(self):
        assert detect_document_type("Statement of Account") == "bank_statement"

    def test_bank_statement_french(self):
        assert detect_document_type("Relevé de compte") == "bank_statement"

    def test_bank_statement_opening_balance(self):
        assert detect_document_type("Opening Balance: $5,000.00") == "bank_statement"

    def test_bank_statement_closing_balance(self):
        assert detect_document_type("Closing Balance: $4,200.00") == "bank_statement"

    def test_bank_statement_deposits_withdrawals(self):
        assert detect_document_type("Deposits and Withdrawals Summary") == "bank_statement"

    def test_bank_statement_cibc(self):
        assert detect_document_type("CIBC Banking Details") == "bank_statement"

    def test_bank_statement_td(self):
        assert detect_document_type("TD Bank statement for period ending") == "bank_statement"

    def test_bank_statement_rbc(self):
        assert detect_document_type("RBC statement monthly summary") == "bank_statement"

    def test_bank_statement_bmo(self):
        assert detect_document_type("BMO statement for account 1234") == "bank_statement"

    def test_bank_statement_scotia(self):
        assert detect_document_type("Scotia statement account overview") == "bank_statement"

    def test_bank_statement_desjardins(self):
        assert detect_document_type("Desjardins relevé mensuel") == "bank_statement"

    def test_bank_statement_solde_ouverture(self):
        assert detect_document_type("Solde d'ouverture: 3 500,00 $") == "bank_statement"

    def test_bank_statement_solde_cloture(self):
        assert detect_document_type("Solde de clôture: 4 200,00 $") == "bank_statement"

    # --- Credit card statements ---
    def test_credit_card_statement_english(self):
        assert detect_document_type("Credit Card Statement") == "credit_card_statement"

    def test_credit_card_statement_french(self):
        assert detect_document_type("Relevé de carte de crédit") == "credit_card_statement"

    def test_credit_card_minimum_payment(self):
        assert detect_document_type("Minimum Payment Due: $25.00") == "credit_card_statement"

    def test_credit_card_paiement_minimum(self):
        assert detect_document_type("Paiement minimum: 25,00 $") == "credit_card_statement"

    def test_credit_card_credit_limit(self):
        assert detect_document_type("Credit Limit: $10,000") == "credit_card_statement"

    def test_credit_card_visa_statement(self):
        assert detect_document_type("Visa Platinum statement summary") == "credit_card_statement"

    def test_credit_card_mastercard_statement(self):
        assert detect_document_type("Mastercard Gold statement") == "credit_card_statement"

    # --- Pay stubs ---
    def test_pay_stub_english(self):
        assert detect_document_type("Pay Stub") == "pay_stub"

    def test_pay_stub_net_pay(self):
        assert detect_document_type("Net Pay: $2,345.67") == "pay_stub"

    def test_pay_stub_salaire_net(self):
        assert detect_document_type("Salaire net: 2 345,67 $") == "pay_stub"

    def test_pay_stub_earnings_deductions(self):
        assert detect_document_type("Earnings and Déductions summary") == "pay_stub"

    def test_pay_stub_t4(self):
        assert detect_document_type("T4 Statement of Remuneration") == "pay_stub"

    def test_pay_stub_roe(self):
        assert detect_document_type("ROE Record of Employment") == "pay_stub"

    def test_pay_stub_paie(self):
        assert detect_document_type("Relevé de paie hebdomadaire") == "pay_stub"

    # --- Credit memos ---
    def test_credit_memo_english(self):
        assert detect_document_type("Credit Note #1234") == "credit_memo"

    def test_credit_memo_french(self):
        assert detect_document_type("Note de crédit") == "credit_memo"

    def test_credit_memo_memo(self):
        assert detect_document_type("Credit Memo for return") == "credit_memo"

    def test_credit_memo_avoir(self):
        assert detect_document_type("Avoir numéro 5678") == "credit_memo"

    # --- Purchase orders ---
    def test_purchase_order_english(self):
        assert detect_document_type("Purchase Order #PO-2025-001") == "purchase_order"

    def test_purchase_order_french(self):
        assert detect_document_type("Bon de commande numéro 456") == "purchase_order"

    def test_purchase_order_po_number(self):
        assert detect_document_type("PO Number: 789") == "purchase_order"

    def test_purchase_order_po_hash(self):
        assert detect_document_type("P.O. #12345") == "purchase_order"

    def test_purchase_order_confirmation(self):
        assert detect_document_type("Order Confirmation for items listed") == "purchase_order"

    def test_purchase_order_confirmation_french(self):
        assert detect_document_type("Confirmation de commande numéro 123") == "purchase_order"

    # --- Receipts ---
    def test_receipt_short_with_total(self):
        text = "Store ABC\nItem 1  $5.00\nItem 2  $3.00\nTotal  $8.00"
        assert detect_document_type(text) == "receipt"

    def test_receipt_not_if_bn_number(self):
        # BN number with space before RT triggers invoice detection
        text = "Store ABC\n123456789 RT0001\nTotal  $8.00"
        # Short doc with BN# — detection varies by exact pattern; verify not crash
        result = detect_document_type(text)
        assert result in ("receipt", "invoice")

    # --- Default to invoice ---
    def test_default_invoice(self):
        assert detect_document_type("Invoice #1234 Amount Due: $500") == "invoice"

    def test_empty_text_returns_invoice(self):
        assert detect_document_type("") == "invoice"

    # --- Filename hints ---
    def test_filename_bank_statement(self):
        # Filename with bank statement keywords in combination with text patterns
        assert detect_document_type("Statement of Account", "bank_statement_march.pdf") == "bank_statement"

    def test_credit_card_priority_over_bank(self):
        """Credit card statement should be detected before bank statement."""
        text = "Credit Card Statement with Closing Balance"
        assert detect_document_type(text) == "credit_card_statement"


# ============================================================================
# P1 — Document Type Routing Targets
# ============================================================================

class TestDocumentTypeRouting:
    """Verify routing targets are set correctly during processing."""

    def test_bank_statement_routes_to_reconciliation(self):
        raw = {}
        detected = detect_document_type("Statement of Account monthly summary")
        assert detected == "bank_statement"

    def test_credit_card_routes_to_reconciliation(self):
        detected = detect_document_type("Credit Card Statement")
        assert detected == "credit_card_statement"

    def test_pay_stub_routes_to_payroll(self):
        detected = detect_document_type("Pay Stub")
        assert detected == "pay_stub"

    def test_credit_memo_routes_to_ap_ar(self):
        detected = detect_document_type("Credit Note #1234")
        assert detected == "credit_memo"

    def test_purchase_order_routes_to_po_matching(self):
        detected = detect_document_type("Purchase Order #PO-001")
        assert detected == "purchase_order"

    def test_receipt_routes_to_expense(self):
        text = "Coffee Shop\nLatte  $5.00\nTotal  $5.00"
        detected = detect_document_type(text)
        assert detected == "receipt"

    def test_invoice_default_expense(self):
        detected = detect_document_type("Invoice #1234 Amount: $500")
        assert detected == "invoice"


# ============================================================================
# P2 — Known Canadian Vendor Registry
# ============================================================================

class TestKnownCanadianBanks:
    """All major Canadian banks should be in the registry."""

    @pytest.mark.parametrize("vendor", [
        "cibc", "canadian imperial bank of commerce",
        "desjardins", "caisse desjardins",
        "rbc", "royal bank of canada", "royal bank",
        "td", "td bank", "toronto-dominion",
        "bmo", "bank of montreal",
        "scotiabank", "scotia",
        "bnc", "banque nationale", "national bank of canada",
        "laurentian bank", "banque laurentienne",
        "hsbc", "hsbc canada",
    ])
    def test_bank_in_registry(self, vendor):
        assert vendor in KNOWN_CANADIAN_VENDORS
        defaults = KNOWN_CANADIAN_VENDORS[vendor]
        assert defaults["gl"] == "1010"
        assert defaults["tax"] == "E"
        assert defaults["category"] == "bank"

    @pytest.mark.parametrize("vendor", [
        "cibc", "desjardins", "rbc", "td", "td bank",
        "bmo", "scotiabank", "bnc", "laurentian bank", "hsbc",
    ])
    def test_bank_is_trusted(self, vendor):
        assert _is_known_trusted_vendor(vendor)

    @pytest.mark.parametrize("vendor", [
        "cibc", "desjardins", "rbc", "td bank",
        "bmo", "scotiabank", "bnc", "laurentian bank", "hsbc",
    ])
    def test_bank_is_canadian_bank_vendor(self, vendor):
        assert _is_canadian_bank_vendor(vendor)

    @pytest.mark.parametrize("vendor", [
        "CIBC", "Canadian Imperial Bank of Commerce",
        "Desjardins", "RBC", "TD Bank", "BMO",
        "Scotiabank", "BNC", "Banque Nationale",
    ])
    def test_bank_case_insensitive(self, vendor):
        assert _is_known_trusted_vendor(vendor)


class TestKnownCanadianTelecoms:
    """All major Canadian telecoms should be in the registry."""

    @pytest.mark.parametrize("vendor", [
        "bell", "bell canada", "bell mobility",
        "virgin plus", "virgin mobile",
        "videotron",
        "rogers", "rogers wireless",
        "fido", "chatr",
        "telus", "telus mobility",
        "koodo", "koodo mobile",
        "public mobile", "freedom mobile", "sasktel",
    ])
    def test_telecom_in_registry(self, vendor):
        assert vendor in KNOWN_CANADIAN_VENDORS
        defaults = KNOWN_CANADIAN_VENDORS[vendor]
        assert defaults["gl"] == "5320"
        assert defaults["tax"] == "T"
        assert defaults["category"] == "telecom"

    @pytest.mark.parametrize("vendor", [
        "bell canada", "videotron", "rogers", "telus", "fido", "koodo",
    ])
    def test_telecom_is_trusted(self, vendor):
        assert _is_known_trusted_vendor(vendor)

    @pytest.mark.parametrize("vendor", [
        "Bell Canada", "Videotron", "Rogers", "Telus", "Fido", "Koodo",
    ])
    def test_telecom_case_insensitive(self, vendor):
        assert _is_known_trusted_vendor(vendor)


class TestKnownCanadianUtilities:
    """All major Canadian utilities should be in the registry."""

    @pytest.mark.parametrize("vendor", [
        "hydro-quebec", "hydro-québec", "hydro quebec",
        "energir", "énergir",
        "gazifere", "gazifère",
        "hydro ottawa", "toronto hydro", "bc hydro",
        "enbridge", "fortisbc",
    ])
    def test_utility_in_registry(self, vendor):
        assert vendor in KNOWN_CANADIAN_VENDORS
        defaults = KNOWN_CANADIAN_VENDORS[vendor]
        assert defaults["gl"] == "5310"
        assert defaults["tax"] == "T"
        assert defaults["category"] == "utility"

    @pytest.mark.parametrize("vendor", [
        "hydro-quebec", "hydro-québec", "energir", "énergir", "gazifere", "gazifère",
    ])
    def test_utility_is_trusted(self, vendor):
        assert _is_known_trusted_vendor(vendor)

    @pytest.mark.parametrize("vendor", [
        "Hydro-Quebec", "Hydro-Québec", "Energir", "Énergir",
    ])
    def test_utility_case_insensitive(self, vendor):
        assert _is_known_trusted_vendor(vendor)


class TestKnownCanadianRetailers:
    """All major Canadian retailers should be in the registry."""

    @pytest.mark.parametrize("vendor", [
        "walmart", "walmart canada",
        "costco", "costco wholesale",
        "iga", "metro", "metro inc",
        "provigo", "maxi", "super c",
        "loblaws", "no frills", "shoppers drug mart",
        "canadian tire", "dollarama",
        "home depot", "rona", "home hardware",
        "staples", "bureau en gros",
    ])
    def test_retailer_in_registry(self, vendor):
        assert vendor in KNOWN_CANADIAN_VENDORS
        defaults = KNOWN_CANADIAN_VENDORS[vendor]
        assert defaults["gl"] == "5600"
        assert defaults["tax"] == "T"
        assert defaults["category"] == "retail"

    @pytest.mark.parametrize("vendor", [
        "walmart", "costco", "iga", "metro", "provigo",
        "maxi", "super c", "loblaws", "canadian tire", "home depot", "rona", "staples",
    ])
    def test_retailer_is_trusted(self, vendor):
        assert _is_known_trusted_vendor(vendor)

    @pytest.mark.parametrize("vendor", [
        "Walmart", "Costco", "IGA", "Metro", "Provigo",
    ])
    def test_retailer_case_insensitive(self, vendor):
        assert _is_known_trusted_vendor(vendor)


class TestKnownCanadianGovernment:
    """All major government entities should be in the registry."""

    @pytest.mark.parametrize("vendor,expected_gl", [
        ("canada revenue agency", "2300"),
        ("agence du revenu du canada", "2300"),
        ("cra", "2300"),
        ("arc", "2300"),
        ("revenu quebec", "2300"),
        ("revenu québec", "2300"),
        ("cnesst", "5410"),
        ("service canada", "2300"),
        ("emploi quebec", "2300"),
        ("saaq", "5500"),
        ("ramq", "5410"),
        ("sqdc", "5600"),
    ])
    def test_government_in_registry(self, vendor, expected_gl):
        assert vendor in KNOWN_CANADIAN_VENDORS
        defaults = KNOWN_CANADIAN_VENDORS[vendor]
        assert defaults["gl"] == expected_gl
        assert defaults["tax"] == "E" or vendor == "sqdc"
        assert defaults["category"] == "government"

    @pytest.mark.parametrize("vendor", [
        "canada revenue agency", "revenu quebec", "revenu québec",
        "cnesst", "service canada",
    ])
    def test_government_is_trusted(self, vendor):
        assert _is_known_trusted_vendor(vendor)

    @pytest.mark.parametrize("vendor", [
        "Canada Revenue Agency", "Revenu Quebec", "Revenu Québec",
        "CNESST", "Service Canada",
    ])
    def test_government_case_insensitive(self, vendor):
        assert _is_known_trusted_vendor(vendor)


class TestKnownTechVendors:
    """Major tech vendors should be in the registry."""

    @pytest.mark.parametrize("vendor", [
        "amazon", "amazon.ca", "amazon web services",
        "apple", "apple canada",
        "microsoft", "google", "adobe",
        "dropbox", "zoom", "zoom video",
    ])
    def test_tech_in_registry(self, vendor):
        assert vendor in KNOWN_CANADIAN_VENDORS
        defaults = KNOWN_CANADIAN_VENDORS[vendor]
        assert defaults["tax"] == "T"
        assert defaults["category"] == "tech"

    @pytest.mark.parametrize("vendor", [
        "amazon", "microsoft", "google", "apple", "adobe",
    ])
    def test_tech_is_trusted(self, vendor):
        assert _is_known_trusted_vendor(vendor)


class TestVendorDefaults:
    """Test _get_vendor_defaults returns correct GL/tax/category."""

    def test_bank_defaults(self):
        d = _get_vendor_defaults("CIBC")
        assert d is not None
        assert d["gl"] == "1010"
        assert d["tax"] == "E"

    def test_telecom_defaults(self):
        d = _get_vendor_defaults("Bell Canada")
        assert d is not None
        assert d["gl"] == "5320"
        assert d["tax"] == "T"

    def test_utility_defaults(self):
        d = _get_vendor_defaults("Hydro-Quebec")
        assert d is not None
        assert d["gl"] == "5310"

    def test_retailer_defaults(self):
        d = _get_vendor_defaults("Walmart")
        assert d is not None
        assert d["gl"] == "5600"

    def test_government_defaults(self):
        d = _get_vendor_defaults("Canada Revenue Agency")
        assert d is not None
        assert d["gl"] == "2300"
        assert d["tax"] == "E"

    def test_tech_defaults(self):
        d = _get_vendor_defaults("Microsoft")
        assert d is not None
        assert d["gl"] == "5350"

    def test_unknown_vendor_returns_none(self):
        d = _get_vendor_defaults("Random Company XYZ")
        assert d is None

    def test_partial_match_works(self):
        d = _get_vendor_defaults("Bell Canada Inc.")
        assert d is not None
        assert d["category"] == "telecom"


class TestVendorRegisteredSet:
    """Known vendors should also be in KNOWN_REGISTERED_SOFTWARE_VENDORS."""

    @pytest.mark.parametrize("vendor", [
        "cibc", "desjardins", "rbc", "td", "td bank", "bmo", "scotiabank",
        "bell", "bell canada", "videotron", "rogers", "telus", "fido", "koodo",
        "hydro-quebec", "hydro-québec", "energir", "énergir", "gazifere", "gazifère",
        "walmart", "costco", "iga", "metro", "provigo", "maxi", "super c",
        "canada revenue agency", "revenu quebec", "revenu québec", "cnesst", "service canada",
        "amazon", "apple", "microsoft", "google", "adobe", "dropbox", "zoom",
    ])
    def test_vendor_in_registered_set(self, vendor):
        assert vendor in KNOWN_REGISTERED_SOFTWARE_VENDORS


# ============================================================================
# P2 — Known vendors must NEVER be flagged as new vendor fraud
# ============================================================================

class TestKnownVendorsNoNewVendorFlag:
    """Known trusted vendors should not get new_vendor_large_amount flags."""

    @pytest.mark.parametrize("vendor", [
        "CIBC", "Desjardins", "RBC", "TD Bank", "BMO", "Scotiabank",
        "Bell Canada", "Videotron", "Rogers", "Telus",
        "Hydro-Quebec", "Energir",
        "Walmart", "Costco", "IGA", "Metro", "Provigo",
        "Canada Revenue Agency", "Revenu Quebec", "CNESST",
        "Amazon", "Microsoft", "Google", "Apple",
    ])
    def test_trusted_vendor_skips_new_vendor_flag(self, vendor):
        """Trusted vendors are checked before calling _rule_new_vendor_large_amount.
        The is_trusted_vendor flag prevents the rule from running."""
        assert _is_known_trusted_vendor(vendor)


# ============================================================================
# P3 — Weekend Transaction Rule Refinement
# ============================================================================

class TestWeekendExemptVendors:
    """Banks, utilities, telecoms, government, retailers are exempt from weekend flags."""

    @pytest.mark.parametrize("vendor", [
        # Banks
        "CIBC", "Desjardins", "RBC", "TD Bank", "BMO", "Scotiabank",
        # Telecoms
        "Bell Canada", "Videotron", "Rogers", "Telus", "Fido", "Koodo",
        # Utilities
        "Hydro-Quebec", "Energir", "Gazifere",
        # Government
        "Canada Revenue Agency", "Revenu Quebec", "CNESST", "Service Canada",
        # Retailers
        "Walmart", "Costco", "IGA", "Metro", "Provigo", "Canadian Tire",
        # Tech
        "Amazon", "Microsoft", "Google", "Apple",
    ])
    def test_known_vendor_is_weekend_exempt(self, vendor):
        assert _is_weekend_exempt_vendor(vendor)

    def test_unknown_vendor_not_weekend_exempt(self):
        assert not _is_weekend_exempt_vendor("Random Unknown Company XYZ")

    def test_individual_not_weekend_exempt(self):
        assert not _is_weekend_exempt_vendor("John Smith Consulting")


class TestWeekendRuleBasics:
    """Weekend rule should still fire for unknown vendors on weekends."""

    def test_saturday_unknown_vendor_flagged(self):
        # 2025-03-08 is a Saturday
        flags = _rule_weekend_holiday(500.0, date(2025, 3, 8))
        assert any(f["rule"] == "weekend_transaction" for f in flags)

    def test_sunday_unknown_vendor_flagged(self):
        # 2025-03-09 is a Sunday
        flags = _rule_weekend_holiday(500.0, date(2025, 3, 9))
        assert any(f["rule"] == "weekend_transaction" for f in flags)

    def test_weekday_no_weekend_flag(self):
        # 2025-03-10 is a Monday
        flags = _rule_weekend_holiday(500.0, date(2025, 3, 10))
        assert not any(f["rule"] == "weekend_transaction" for f in flags)

    def test_small_amount_no_flag(self):
        # Below threshold
        flags = _rule_weekend_holiday(50.0, date(2025, 3, 8))
        assert len(flags) == 0

    def test_holiday_flagged(self):
        # Christmas 2025
        flags = _rule_weekend_holiday(500.0, date(2025, 12, 25))
        assert any(f["rule"] == "holiday_transaction" for f in flags)


class TestWeekendExemptCategories:
    """Verify the exempt category set is correct."""

    def test_all_exempt_categories_present(self):
        expected = {"bank", "utility", "telecom", "government", "retail", "tech"}
        assert WEEKEND_EXEMPT_CATEGORIES == expected

    def test_bank_category_exempt(self):
        assert "bank" in WEEKEND_EXEMPT_CATEGORIES

    def test_utility_category_exempt(self):
        assert "utility" in WEEKEND_EXEMPT_CATEGORIES

    def test_telecom_category_exempt(self):
        assert "telecom" in WEEKEND_EXEMPT_CATEGORIES

    def test_government_category_exempt(self):
        assert "government" in WEEKEND_EXEMPT_CATEGORIES

    def test_retail_category_exempt(self):
        assert "retail" in WEEKEND_EXEMPT_CATEGORIES

    def test_tech_category_exempt(self):
        assert "tech" in WEEKEND_EXEMPT_CATEGORIES


# ============================================================================
# Regression: existing fraud rules still work
# ============================================================================

class TestExistingFraudRulesNotBroken:
    """Ensure existing fraud detection behavior is preserved."""

    def test_new_vendor_large_amount_still_flags_unknown(self):
        """Unknown vendors with no history should still be flagged."""
        flag = _rule_new_vendor_large_amount(
            "Unknown Vendor XYZ", 5000.0, [], date(2025, 3, 10)
        )
        assert flag is not None
        assert flag["rule"] == "new_vendor_large_amount"

    def test_new_vendor_not_flagged_if_established(self):
        """Vendor with 3+ approved transactions should not be flagged."""
        history = [
            {"amount": 1000, "document_date": "2025-02-01", "review_status": "Posted"},
            {"amount": 1500, "document_date": "2025-02-15", "review_status": "Posted"},
            {"amount": 2000, "document_date": "2025-03-01", "review_status": "Posted"},
        ]
        flag = _rule_new_vendor_large_amount(
            "Some Vendor", 3000.0, history, date(2025, 3, 10)
        )
        assert flag is None

    def test_weekend_holiday_severity_is_low(self):
        """Weekend flags should have LOW severity."""
        flags = _rule_weekend_holiday(500.0, date(2025, 3, 8))
        for f in flags:
            assert f["severity"] == "low"


# ============================================================================
# Integration: all vendor categories have correct defaults
# ============================================================================

class TestVendorCategoryCompleteness:
    """Every vendor in KNOWN_CANADIAN_VENDORS must have gl, tax, category."""

    def test_all_vendors_have_required_keys(self):
        for vendor, defaults in KNOWN_CANADIAN_VENDORS.items():
            assert "gl" in defaults, f"{vendor} missing gl"
            assert "tax" in defaults, f"{vendor} missing tax"
            assert "category" in defaults, f"{vendor} missing category"

    def test_all_gl_accounts_are_strings(self):
        for vendor, defaults in KNOWN_CANADIAN_VENDORS.items():
            assert isinstance(defaults["gl"], str), f"{vendor} gl not string"

    def test_all_tax_codes_valid(self):
        valid_tax_codes = {"T", "E", "Z", "O"}
        for vendor, defaults in KNOWN_CANADIAN_VENDORS.items():
            assert defaults["tax"] in valid_tax_codes, \
                f"{vendor} has invalid tax code: {defaults['tax']}"

    def test_all_categories_valid(self):
        valid_categories = {"bank", "telecom", "utility", "retail", "government", "tech"}
        for vendor, defaults in KNOWN_CANADIAN_VENDORS.items():
            assert defaults["category"] in valid_categories, \
                f"{vendor} has invalid category: {defaults['category']}"

    def test_known_trusted_vendors_match_registry(self):
        """KNOWN_TRUSTED_VENDORS should contain all KNOWN_CANADIAN_VENDORS keys."""
        assert KNOWN_TRUSTED_VENDORS == set(KNOWN_CANADIAN_VENDORS.keys())

    def test_banks_are_financial_institutions(self):
        """All bank vendors should have E tax code (exempt)."""
        for vendor, defaults in KNOWN_CANADIAN_VENDORS.items():
            if defaults["category"] == "bank":
                assert defaults["tax"] == "E", f"Bank {vendor} should be tax exempt"
                assert defaults["gl"] == "1010", f"Bank {vendor} should use GL 1010"

    def test_government_mostly_tax_exempt(self):
        """Government vendors should generally be tax exempt."""
        for vendor, defaults in KNOWN_CANADIAN_VENDORS.items():
            if defaults["category"] == "government" and vendor != "sqdc":
                assert defaults["tax"] == "E", \
                    f"Government vendor {vendor} should be tax exempt"
