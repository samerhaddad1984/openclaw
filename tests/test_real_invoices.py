"""
tests/test_real_invoices.py
===========================
Comprehensive tests for GST/QST tax number extraction from invoices.
Covers all known formats: keyword-prefixed, standalone, BN#, embedded in descriptions.
"""
import pytest
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.ocr_engine import parse_invoice_fields


class TestGSTNumberExtraction:
    """GST/HST number extraction across all known formats."""

    def test_gst_in_line_item_description(self):
        text = "CANADA GST/TPS # 805577574 RT0001 (5.00%) CAD 2.55"
        result = parse_invoice_fields(text)
        assert result["gst_number"] is not None

    def test_gst_standard_format(self):
        text = "GST: 805577574RT0001"
        result = parse_invoice_fields(text)
        assert result["gst_number"] == "805577574RT0001"

    def test_gst_with_spaces(self):
        text = "GST # 805577574 RT 0001"
        result = parse_invoice_fields(text)
        assert result["gst_number"] == "805577574RT0001"

    def test_gst_tps_prefix(self):
        text = "TPS # 805577574 RT0001"
        result = parse_invoice_fields(text)
        assert result["gst_number"] == "805577574RT0001"

    def test_gst_registration_prefix(self):
        text = "GST Registration: 123456789RT0001"
        result = parse_invoice_fields(text)
        assert result["gst_number"] is not None
        assert "123456789" in result["gst_number"]

    def test_gst_standalone_rt_pattern(self):
        """9 digits + RT + 4 digits anywhere should be detected."""
        text = "Some invoice text\n805577574RT0001\nMore text"
        result = parse_invoice_fields(text)
        assert result["gst_number"] == "805577574RT0001"


class TestQSTNumberExtraction:
    """QST/TVQ number extraction across all known formats."""

    def test_qst_in_line_item_description(self):
        text = "QUEBEC QST/TVQ # 1221825787 (9.975%) CAD 5.09"
        result = parse_invoice_fields(text)
        assert result["qst_number"] is not None

    def test_qst_standard_format(self):
        text = "QST: 1221825787"
        result = parse_invoice_fields(text)
        assert result["qst_number"] == "1221825787"

    def test_qst_tvq_prefix(self):
        text = "TVQ: 1221825787"
        result = parse_invoice_fields(text)
        assert result["qst_number"] == "1221825787"

    def test_qst_neq_prefix(self):
        text = "NEQ: 1234567890"
        result = parse_invoice_fields(text)
        assert result["qst_number"] == "1234567890"

    def test_qst_near_tax_keyword(self):
        """10-digit number on a line with tax keywords should be detected."""
        text = "Quebec tax registration 1221825787"
        result = parse_invoice_fields(text)
        assert result["qst_number"] == "1221825787"


class TestBNNumberExtraction:
    """Business Number (BN#) extraction."""

    def test_bn_bc_format(self):
        text = "BN# 764781803BC0001"
        result = parse_invoice_fields(text)
        assert result.get("bn_root") == "764781803"

    def test_bn_rt_format(self):
        text = "BN# 764781803RT0001"
        result = parse_invoice_fields(text)
        assert result.get("bn_root") == "764781803"
        assert result.get("gst_number") is not None

    def test_bn_derives_gst(self):
        text = "BN# 764781803BC0001"
        result = parse_invoice_fields(text)
        assert result.get("gst_number") == "764781803RT0001"


class TestHeaderRegistrationNumbers:
    """Tax numbers appearing in document headers."""

    def test_gst_qst_in_header(self):
        text = "GST Registration: 123456789RT0001\nQST Registration: 1234567890"
        result = parse_invoice_fields(text)
        assert result["gst_number"] is not None
        assert result["qst_number"] is not None


class TestGoToTechnologiesFormat:
    """Exact format from GoTo Technologies Canada invoices."""

    def test_goto_full_invoice(self):
        text = """GoTo Technologies Canada Ltd
CANADA GST/TPS # 805577574 RT0001 (5.00%) CAD 2.55
QUEBEC QST/TVQ # 1221825787 (9.975%) CAD 5.09"""
        result = parse_invoice_fields(text)
        assert result["gst_number"] == "805577574RT0001"
        assert result["qst_number"] == "1221825787"
        assert result.get("tax_code") == "T"


class TestTaxCodeFromRegistration:
    """FIX 5: Finding GST/QST numbers should set tax_code and is_registered."""

    def test_gst_sets_tax_code(self):
        text = "Invoice\nGST: 805577574RT0001\nTotal: $100.00"
        result = parse_invoice_fields(text)
        assert result.get("tax_code") == "T"
        assert result.get("is_registered") is True

    def test_qst_sets_tax_code(self):
        text = "Invoice\nQST: 1221825787\nTotal: $100.00"
        result = parse_invoice_fields(text)
        assert result.get("tax_code") == "T"
        assert result.get("is_registered") is True

    def test_no_tax_number_no_flag(self):
        text = "Invoice\nTotal: $100.00"
        result = parse_invoice_fields(text)
        assert result.get("is_registered") is None or result.get("is_registered") is False


class TestGoToTechnologiesFullExtraction:
    """Full extraction test for GoTo Technologies Canada invoice."""

    def test_goto_technologies_full_extraction(self):
        text = """GoTo Technologies Canada Ltd
    410 Charest Est Suite 250
    Quebec City QC G1K 8G3

    Invoice Date 24-Jan-2022
    Invoice Number INLASSYCCM1O880119600

    LastPass Premium Annual 1 CAD 51.00

    SUBTOTAL Excl Tax CAD 51.00
    QUEBEC QST/TVQ # 1221825787 (9.975%) CAD 5.09
    CANADA GST/TPS # 805577574 RT0001 (5.00%) CAD 2.55
    Total Including Tax CAD 58.64"""

        result = parse_invoice_fields(text)
        assert 'goto' in result.get('vendor_name', '').lower() or 'lastpass' in result.get('vendor_name', '').lower()
        assert result.get('amount') == 58.64 or result.get('amount') == '58.64'
        assert result.get('gst_number') is not None
        assert result.get('qst_number') is not None
        assert result.get('tax_code') == 'T'
        assert result.get('gst_amount') is not None
        assert result.get('qst_amount') is not None
