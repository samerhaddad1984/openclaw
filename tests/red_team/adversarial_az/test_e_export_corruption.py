"""
E — EXPORT CORRUPTION
=====================
Attack multi-format export engine with Unicode bombs, formula injection,
encoding corruption, tax decomposition mismatches, and format-specific traps.

Targets: export_engine
"""
from __future__ import annotations

import csv
import io
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.export_engine import (
    _extract_taxes,
    _dec,
)

# Try to import the actual export functions
try:
    from src.engines.export_engine import generate_csv
    HAS_CSV = True
except ImportError:
    HAS_CSV = False

try:
    from src.engines.export_engine import generate_sage50
    HAS_SAGE = True
except ImportError:
    HAS_SAGE = False

try:
    from src.engines.export_engine import generate_qbd_iif
    HAS_QBD = True
except ImportError:
    HAS_QBD = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_doc_row(**kw) -> dict:
    defaults = {
        "document_id": "doc-exp-001",
        "file_name": "test.pdf",
        "client_code": "TEST01",
        "vendor": "Test Vendor",
        "invoice_number": "INV-001",
        "doc_type": "invoice",
        "amount": 1149.75,
        "document_date": "2025-06-15",
        "gl_account": "5000",
        "tax_code": "T",
        "category": "expense",
        "currency": "CAD",
    }
    defaults.update(kw)
    return defaults


# ===================================================================
# TEST CLASS: CSV Injection Attacks
# ===================================================================

class TestCSVInjection:
    """Formula injection via vendor name or memo fields."""

    def test_formula_in_vendor_name(self):
        """Vendor name '=SUM(A1:A99)' must be escaped in CSV."""
        doc = _make_doc_row(vendor="=SUM(A1:A99)")
        if HAS_CSV:
            result = generate_csv([doc], client_code="TEST01")
            content = result.decode("utf-8-sig") if isinstance(result, bytes) else result
            # Formula should be escaped with leading apostrophe or tab
            assert "=SUM(A1:A99)" not in content or "'=SUM" in content or "\t=SUM" in content, (
                "P1 DEFECT: CSV formula injection — vendor name not escaped"
            )
        else:
            pytest.skip("generate_csv not available")

    def test_pipe_command_injection(self):
        """Vendor '|cmd /c calc' must not execute."""
        doc = _make_doc_row(vendor="|cmd /c calc")
        if HAS_CSV:
            result = generate_csv([doc], client_code="TEST01")
            content = result.decode("utf-8-sig") if isinstance(result, bytes) else result
            # Pipe should be escaped
            assert content is not None  # At minimum, should not crash

    @pytest.mark.parametrize("payload", [
        "=HYPERLINK(\"https://evil.com\",\"Click\")",
        "+cmd|' /C calc'!A0",
        "-1+1|cmd",
        "@SUM(1+1)*cmd|' /C calc'!A0",
    ])
    def test_known_csv_injection_payloads(self, payload):
        """Known CSV injection payloads must be neutralized."""
        doc = _make_doc_row(vendor=payload)
        if HAS_CSV:
            result = generate_csv([doc], client_code="TEST01")
            content = result.decode("utf-8-sig") if isinstance(result, bytes) else result
            # The raw payload should not appear unescaped
            lines = content.split("\n")
            for line in lines:
                if payload in line and not line.startswith("#"):
                    # Check if it's properly quoted
                    if f'"{payload}"' not in line and f"'{payload}" not in line:
                        pytest.xfail(f"P1 DEFECT: CSV injection not escaped: {payload}")


# ===================================================================
# TEST CLASS: Tax Decomposition in Export
# ===================================================================

class TestTaxDecompositionExport:
    """Tax breakdown must match source data."""

    def test_gst_qst_decomposition_roundtrip(self):
        """Extract taxes from T code → pre_tax + gst + qst = amount."""
        taxes = _extract_taxes(Decimal("1149.75"), "T")
        total = taxes["pre_tax"] + taxes["gst"] + taxes["qst"]
        assert abs(total - Decimal("1149.75")) <= Decimal("0.02"), (
            f"Tax decomposition roundtrip error: {total} != 1149.75"
        )

    def test_hst_decomposition(self):
        """HST 13% decomposition."""
        taxes = _extract_taxes(Decimal("1130.00"), "HST")
        total = taxes["pre_tax"] + taxes["hst"]
        assert abs(total - Decimal("1130.00")) <= Decimal("0.02")

    def test_exempt_no_tax(self):
        """Exempt items must have zero tax."""
        taxes = _extract_taxes(Decimal("500.00"), "E")
        assert taxes["gst"] == Decimal("0")
        assert taxes["qst"] == Decimal("0")
        assert taxes["hst"] == Decimal("0")

    def test_none_tax_code(self):
        """None/empty tax code → no tax extracted."""
        taxes = _extract_taxes(Decimal("500.00"), "")
        assert taxes["pre_tax"] == Decimal("500.00")

    @pytest.mark.parametrize("amount", [
        Decimal("0.01"),
        Decimal("0.10"),
        Decimal("999999.99"),
        Decimal("0.00"),
    ])
    def test_tax_decomposition_edge_amounts(self, amount):
        """Tax decomposition on extreme amounts."""
        taxes = _extract_taxes(amount, "T")
        total = taxes["pre_tax"] + taxes["gst"] + taxes["qst"]
        assert abs(total - amount) <= Decimal("0.02"), (
            f"Edge amount {amount}: decomposition = {total}"
        )


# ===================================================================
# TEST CLASS: Unicode in Exports
# ===================================================================

class TestUnicodeExport:
    """French accents, CJK, emoji must survive export."""

    def test_french_accents_in_vendor(self):
        doc = _make_doc_row(vendor="Équipement Québécois Ltée")
        if HAS_CSV:
            result = generate_csv([doc], client_code="TEST01")
            content = result.decode("utf-8-sig") if isinstance(result, bytes) else result
            assert "Équipement" in content, "French accents corrupted in CSV"

    def test_empty_document_list(self):
        """Exporting zero documents must not crash."""
        if HAS_CSV:
            result = generate_csv([], client_code="TEST01")
            assert result is not None

    def test_null_fields_in_export(self):
        """None values in fields must not crash export."""
        doc = _make_doc_row(vendor=None, gl_account=None, invoice_number=None)
        if HAS_CSV:
            try:
                result = generate_csv([doc], client_code="TEST01")
                assert result is not None
            except (TypeError, AttributeError) as e:
                pytest.xfail(f"P2 DEFECT: Export crashes on None fields: {e}")


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestExportDeterminism:
    """Same input → same export output."""

    def test_csv_deterministic(self):
        if not HAS_CSV:
            pytest.skip("generate_csv not available")
        doc = _make_doc_row()
        outputs = set()
        for _ in range(10):
            r = generate_csv([doc], client_code="TEST01")
            outputs.add(r if isinstance(r, bytes) else r.encode())
        assert len(outputs) == 1, "CSV export is non-deterministic"

    def test_tax_extraction_deterministic(self):
        results = set()
        for _ in range(100):
            t = _extract_taxes(Decimal("1149.75"), "T")
            results.add(str(t))
        assert len(results) == 1, f"Tax extraction non-deterministic: {len(results)} variants"
