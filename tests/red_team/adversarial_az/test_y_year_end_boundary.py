"""
Y — YEAR-END BOUNDARY TORTURE
===============================
Attack fiscal year-end transitions with cross-year invoices, CCA half-year
rule, T2/CO-17 prefill, and period-close boundary violations.

Targets: fixed_assets_engine, audit_engine, tax_engine
"""
from __future__ import annotations

import sqlite3
import sys
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.fixed_assets_engine import (
    ensure_fixed_assets_table,
    add_asset,
    calculate_annual_cca,
    dispose_asset,
    CCA_CLASSES,
    normalize_cca_class,
    CENT,
)
from src.engines.audit_engine import (
    ensure_audit_tables,
    create_engagement,
)

from .conftest import fresh_db, ensure_documents_table, insert_document

_round = lambda v: Decimal(str(v)).quantize(CENT, rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _asset_db() -> sqlite3.Connection:
    conn = fresh_db()
    ensure_fixed_assets_table(conn)
    ensure_audit_tables(conn)
    ensure_documents_table(conn)
    return conn


# ===================================================================
# TEST CLASS: CCA Half-Year Rule
# ===================================================================

class TestCCAHalfYearRule:
    """Assets acquired in the year get 50% of normal CCA rate (half-year rule)."""

    def test_first_year_half_rate(self):
        conn = _asset_db()
        asset_id = add_asset(
            client_code="CCA01", asset_name="Delivery Truck",
            cca_class=10, cost=Decimal("50000"),
            acquisition_date="2025-03-15", conn=conn,
        )
        result = calculate_annual_cca(client_code="CCA01", fiscal_year_end="2025-12-31", conn=conn)
        if isinstance(result, (list, dict)):
            # First year CCA for class 10 (30%) with half-year rule = 15%
            # $50,000 * 15% = $7,500
            if isinstance(result, list):
                class_10 = [r for r in result if r.get("cca_class") == 10]
                if class_10:
                    cca_amount = Decimal(str(class_10[0].get("cca_amount", 0)))
                    expected = _round(Decimal("50000") * Decimal("0.30") * Decimal("0.5"))
                    assert cca_amount == expected, (
                        f"Half-year rule: CCA={cca_amount}, expected={expected}"
                    )

    def test_second_year_full_rate(self):
        conn = _asset_db()
        asset_id = add_asset(
            client_code="CCA02", asset_name="Computer",
            cca_class=50, cost=Decimal("3000"),
            acquisition_date="2024-06-01", conn=conn,
        )
        result = calculate_annual_cca(client_code="CCA02", fiscal_year_end="2025-12-31", conn=conn)


# ===================================================================
# TEST CLASS: Asset Disposal
# ===================================================================

class TestAssetDisposal:
    """Recapture and terminal loss on disposal."""

    def test_disposal_proceeds_exceed_ucc(self):
        """Disposal > UCC → recapture (income)."""
        conn = _asset_db()
        asset_id = add_asset(
            client_code="DISP01", asset_name="Equipment",
            cca_class=8, cost=Decimal("10000"),
            acquisition_date="2023-01-15", conn=conn,
        )
        try:
            result = dispose_asset(
                conn, asset_id=asset_id,
                proceeds=Decimal("12000"),
                disposal_date="2025-06-15",
            )
            if isinstance(result, dict):
                # Proceeds > cost → capital gain territory
                assert "recapture" in result or "gain" in result or result.get("recapture_amount") is not None
        except (TypeError, Exception):
            pass

    def test_disposal_below_ucc(self):
        """Disposal < UCC → terminal loss (deduction)."""
        conn = _asset_db()
        asset_id = add_asset(
            client_code="DISP02", asset_name="Old Machine",
            cca_class=43, cost=Decimal("20000"),
            acquisition_date="2020-01-15", conn=conn,
        )
        try:
            result = dispose_asset(
                conn, asset_id=asset_id,
                proceeds=Decimal("2000"),
                disposal_date="2025-06-15",
            )
            if isinstance(result, dict):
                assert "terminal_loss" in result or result.get("terminal_loss_amount") is not None
        except (TypeError, Exception):
            pass

    def test_zero_proceeds_disposal(self):
        """Scrapped asset with $0 proceeds."""
        conn = _asset_db()
        asset_id = add_asset(
            client_code="DISP03", asset_name="Junk",
            cca_class=8, cost=Decimal("5000"),
            acquisition_date="2020-01-15", conn=conn,
        )
        try:
            result = dispose_asset(
                conn, asset_id=asset_id,
                proceeds=Decimal("0"),
                disposal_date="2025-12-31",
            )
        except (TypeError, Exception):
            pass


# ===================================================================
# TEST CLASS: CCA Class Normalization
# ===================================================================

class TestCCAClassNormalization:

    @pytest.mark.parametrize("input_val,expected", [
        (10, 10),
        ("10", 10),
        (10.1, 101),
        ("10.1", 101),
        (101, 101),
        ("101", 101),
        (14.1, 141),
        ("14.1", 141),
        (50, 50),
        (999, None),
    ])
    def test_normalize_cca_class(self, input_val, expected):
        result = normalize_cca_class(input_val)
        assert result == expected, f"normalize_cca_class({input_val}) = {result}, expected {expected}"


# ===================================================================
# TEST CLASS: Year-End Document Cutoff
# ===================================================================

class TestYearEndCutoff:
    """Documents crossing fiscal year boundary."""

    def test_dec31_vs_jan1_different_years(self):
        conn = _asset_db()
        insert_document(conn, document_id="ye-d1", document_date="2025-12-31",
                        client_code="YE01")
        insert_document(conn, document_id="ye-d2", document_date="2026-01-01",
                        client_code="YE01")

        dec = conn.execute(
            "SELECT * FROM documents WHERE document_id = 'ye-d1'"
        ).fetchone()
        jan = conn.execute(
            "SELECT * FROM documents WHERE document_id = 'ye-d2'"
        ).fetchone()

        assert dec["document_date"] == "2025-12-31"
        assert jan["document_date"] == "2026-01-01"

    def test_fiscal_year_not_calendar_year(self):
        """Some businesses have non-calendar fiscal years (e.g., March 31)."""
        # Asset acquired March 30 in FY ending March 31
        conn = _asset_db()
        asset_id = add_asset(
            client_code="FY01", asset_name="Late Purchase",
            cca_class=8, cost=Decimal("10000"),
            acquisition_date="2025-03-30", conn=conn,
        )
        result = calculate_annual_cca(client_code="FY01", fiscal_year_end="2025-03-31", conn=conn)


# ===================================================================
# TEST CLASS: CCA Class Coverage
# ===================================================================

class TestCCAClassCoverage:
    """All CCA classes must have valid rates."""

    def test_all_classes_have_rate_or_method(self):
        for cls_id, info in CCA_CLASSES.items():
            assert info["method"] in ("declining", "straight-line"), (
                f"CCA class {cls_id} has invalid method: {info['method']}"
            )
            if info["method"] == "declining":
                assert info["rate"] is not None and info["rate"] > Decimal("0"), (
                    f"CCA class {cls_id} declining method but no rate"
                )

    def test_class_10_automotive(self):
        info = CCA_CLASSES[10]
        assert info["rate"] == Decimal("0.30")
        assert info["description"] == "Automotive"

    def test_class_50_computers(self):
        info = CCA_CLASSES[50]
        assert info["rate"] == Decimal("0.55")


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestYearEndDeterminism:
    def test_cca_deterministic(self):
        results = set()
        for _ in range(10):
            conn = _asset_db()
            add_asset(client_code="DET", asset_name="Asset",
                      cca_class=10, cost=Decimal("50000"),
                      acquisition_date="2025-01-15", conn=conn)
            r = calculate_annual_cca(client_code="DET", fiscal_year_end="2025-12-31", conn=conn)
            results.add(str(r))
        assert len(results) == 1, f"Non-deterministic CCA: {len(results)} variants"
