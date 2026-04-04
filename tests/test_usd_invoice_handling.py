"""
tests/test_usd_invoice_handling.py
===================================
Tests for USD invoice detection, FX conversion, and tax treatment.

5 tests:
  1. USD invoice converts to CAD correctly
  2. USD vendor with no GST number gets tax_code E
  3. USD vendor with GST number gets tax_code T
  4. Known digital service (Adobe) gets tax_code T
  5. Bank of Canada rate fetched or fallback used
"""
from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.ocr_engine import parse_invoice_fields, get_fx_rate, get_usd_cad_rate


class TestUsdInvoiceConversion:
    """USD invoice converts to CAD correctly."""

    def test_usd_invoice_converts_to_cad(self):
        text = """
ACME Software Inc.
New York NY USA
Invoice Date: March 15 2026
Total Due: USD 600.00
No Canadian GST number
"""
        result = parse_invoice_fields(text)
        assert result["currency"] == "USD"
        assert result["foreign_amount"] == 600.0
        assert result["fx_rate"] is not None
        assert result["fx_rate"] > 1.0  # USD/CAD always > 1
        assert result["cad_amount"] is not None
        assert result["cad_amount"] > 600.0  # CAD > USD amount
        assert result["currency_converted"] is True
        assert result["amount"] == result["cad_amount"]  # Amount stored in CAD
        assert "converted at" in result["currency_note"]


class TestUsdTaxTreatmentNoGst:
    """USD vendor with no GST number gets tax_code E (exempt)."""

    def test_usd_vendor_no_gst_gets_exempt(self):
        text = """
Foreign Corp LLC
123 Main St, Chicago IL USA
Invoice: 2026-03-20
Total: USD 1000.00
"""
        result = parse_invoice_fields(text)
        assert result["currency"] == "USD"
        assert result["tax_code"] == "E"
        assert result["gst_amount"] == 0
        assert result["qst_amount"] == 0
        assert "non inscrit" in result.get("tax_note", "") or "not registered" in result.get("tax_note", "")


class TestUsdTaxTreatmentWithGst:
    """USD vendor with Canadian GST number gets tax_code T (taxable)."""

    def test_usd_vendor_with_gst_gets_taxable(self):
        text = """
US-Canada Services Inc.
500 5th Ave, New York NY USA
GST# 123456789RT0001
Invoice: 2026-03-20
Total: USD 500.00
"""
        result = parse_invoice_fields(text)
        assert result["currency"] == "USD"
        assert result["tax_code"] == "T"
        assert result["gst_amount"] is not None
        assert result["gst_amount"] > 0
        assert result["qst_amount"] is not None
        assert result["qst_amount"] > 0


class TestDigitalServiceRegistered:
    """Known digital service (Adobe) gets tax_code T even without visible GST number."""

    def test_adobe_gets_taxable(self):
        text = """
Adobe Inc.
San Jose CA USA
Invoice: 2026-03-20
Total: USD 79.99
Creative Cloud Subscription
"""
        result = parse_invoice_fields(text)
        assert result["currency"] == "USD"
        assert result["tax_code"] == "T"
        assert result["gst_amount"] is not None
        assert result["gst_amount"] > 0
        assert "numérique" in result.get("tax_note", "") or "digital" in result.get("tax_note", "")


class TestFxRateFetchOrFallback:
    """Bank of Canada rate is fetched or fallback is used."""

    def test_fallback_rate_when_api_unavailable(self):
        """When BoC API is unavailable, fallback rate from config is used."""
        with patch("src.engines.ocr_engine.requests.get", side_effect=Exception("no network")):
            rate = get_fx_rate("USD", "2026-03-15")
            assert isinstance(rate, Decimal)
            assert rate > Decimal("1.0")
            assert rate < Decimal("2.0")  # Reasonable USD/CAD range

    def test_explicit_rate_on_invoice_preferred(self):
        """If invoice states exchange rate, use that."""
        text = """
Foreign Vendor Ltd
Total: USD 100.00
Exchange rate: 1.3542
"""
        result = parse_invoice_fields(text)
        assert result["fx_rate"] == 1.3542
        assert result["cad_amount"] == round(100.0 * 1.3542, 2)

    def test_get_usd_cad_rate_returns_decimal(self):
        """get_usd_cad_rate returns a Decimal."""
        rate = get_usd_cad_rate()
        assert isinstance(rate, Decimal)
        assert rate > Decimal("1.0")

    def test_cad_rate_is_one(self):
        """CAD -> CAD rate should be 1.0."""
        rate = get_fx_rate("CAD")
        assert rate == Decimal("1.0")
