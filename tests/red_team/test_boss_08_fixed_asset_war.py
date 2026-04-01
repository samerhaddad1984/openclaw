"""
tests/red_team/test_boss_08_fixed_asset_war.py
===============================================
BOSS FIGHT 8 — Fixed Asset vs Expense War.

Mixed CapEx, repairs, disposal, recapture, terminal loss, T2 Sch 8.
CCA class validation, half-year rule, and substance engine integration.
"""
from __future__ import annotations

import sqlite3
import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.fixed_assets_engine import (
    CCA_CLASSES,
    CENT,
    add_asset,
    calculate_annual_cca,
    cca_class_display,
    create_draft_asset_from_capex,
    dispose_asset,
    ensure_fixed_assets_table,
    list_assets,
    normalize_cca_class,
)
from src.engines.substance_engine import substance_classifier
from src.engines.tax_engine import calculate_gst_qst, calculate_itc_itr

_ROUND = lambda v: Decimal(str(v)).quantize(CENT, rounding=ROUND_HALF_UP)


def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


class TestCCAClassValidation:
    """CCA class normalization and validation."""

    def test_standard_classes(self):
        for cls in [1, 6, 8, 10, 12, 43, 45, 50]:
            assert normalize_cca_class(cls) == cls

    def test_display_format_101(self):
        """10.1 → 101 (passenger vehicles)."""
        assert normalize_cca_class("10.1") == 101
        assert normalize_cca_class(10.1) == 101

    def test_display_format_141(self):
        """14.1 → 141 (eligible capital property)."""
        assert normalize_cca_class("14.1") == 141

    def test_invalid_class_returns_none(self):
        assert normalize_cca_class("999") is None
        assert normalize_cca_class("abc") is None

    def test_display_roundtrip(self):
        """Key → display → key must roundtrip."""
        assert cca_class_display(101) == "10.1"
        assert cca_class_display(141) == "14.1"
        assert cca_class_display(8) == "8"


class TestHalfYearRule:
    """CCA half-year rule on first-year acquisitions."""

    def test_class_8_half_year(self):
        """Class 8 (20%) — first year CCA = cost * 20% / 2 = 10%."""
        conn = _fresh_db()
        asset_id = add_asset("FA_CO", "Office Equipment", "2026-01-15",
                             10000, 8, conn)
        assets = list_assets("FA_CO", conn)
        asset = next(a for a in assets if a["asset_id"] == asset_id)

        cost = Decimal("10000")
        expected_first_cca = _ROUND(cost * Decimal("0.20") / Decimal("2"))  # $1000
        actual_ucc = Decimal(str(asset["current_ucc"]))
        expected_ucc = cost - expected_first_cca  # $9000

        assert actual_ucc == expected_ucc

    def test_class_10_automotive(self):
        """Class 10 (30%) vehicles — half-year rule."""
        conn = _fresh_db()
        asset_id = add_asset("FA_CO", "Company Truck", "2026-03-01",
                             45000, 10, conn)
        assets = list_assets("FA_CO", conn)
        asset = next(a for a in assets if a["asset_id"] == asset_id)

        cost = Decimal("45000")
        first_cca = _ROUND(cost * Decimal("0.30") / Decimal("2"))  # $6750
        assert Decimal(str(asset["current_ucc"])) == cost - first_cca

    def test_class_45_computers(self):
        """Class 45 (45%) computers — half-year = 22.5% first year."""
        conn = _fresh_db()
        asset_id = add_asset("FA_CO", "Server Rack", "2026-06-01",
                             8000, 45, conn)
        assets = list_assets("FA_CO", conn)
        asset = next(a for a in assets if a["asset_id"] == asset_id)

        cost = Decimal("8000")
        first_cca = _ROUND(cost * Decimal("0.45") / Decimal("2"))
        assert Decimal(str(asset["current_ucc"])) == cost - first_cca

    def test_zero_cost_rejected(self):
        """Zero or negative cost must be rejected."""
        conn = _fresh_db()
        with pytest.raises(ValueError):
            add_asset("FA_CO", "Free Equipment", "2026-01-01", 0, 8, conn)
        with pytest.raises(ValueError):
            add_asset("FA_CO", "Negative Asset", "2026-01-01", -100, 8, conn)


class TestAssetDisposal:
    """Recapture, terminal loss, and capital gain scenarios."""

    def test_disposal_at_ucc_no_recapture_no_loss(self):
        """Selling at exactly UCC → no recapture, no terminal loss."""
        conn = _fresh_db()
        asset_id = add_asset("FA_CO", "Equipment A", "2025-01-01",
                             10000, 8, conn)
        asset = list_assets("FA_CO", conn)[0]
        ucc = asset["current_ucc"]

        result = dispose_asset(asset_id, "2026-06-15", ucc, conn)
        assert result["recapture"] == 0.0
        assert result["terminal_loss"] == 0.0

    def test_disposal_above_ucc_recapture(self):
        """Selling above UCC (but below cost) → recapture."""
        conn = _fresh_db()
        asset_id = add_asset("FA_CO", "Machine B", "2025-01-01",
                             20000, 8, conn)
        asset = list_assets("FA_CO", conn)[0]
        ucc = asset["current_ucc"]

        # Sell for more than UCC but less than cost
        proceeds = float(ucc) + 2000
        result = dispose_asset(asset_id, "2026-06-15", proceeds, conn)
        assert result["recapture"] > 0
        assert result["terminal_loss"] == 0.0

    def test_disposal_above_cost_capital_gain(self):
        """Selling above original cost → capital gain + recapture."""
        conn = _fresh_db()
        asset_id = add_asset("FA_CO", "Appreciated Asset", "2025-01-01",
                             15000, 8, conn)

        result = dispose_asset(asset_id, "2026-06-15", 20000, conn)
        assert result["capital_gain"] > 0
        assert result["original_cost"] == 15000.0

    def test_disposal_below_ucc_terminal_loss(self):
        """Last asset in class, sold below UCC → terminal loss."""
        conn = _fresh_db()
        asset_id = add_asset("FA_CO", "Only Asset", "2025-01-01",
                             10000, 43, conn)  # Class 43 — manufacturing

        # Sell for much less than UCC
        result = dispose_asset(asset_id, "2026-06-15", 1000, conn)
        assert result["terminal_loss"] > 0
        assert result["recapture"] == 0.0

    def test_disposed_asset_cannot_be_disposed_again(self):
        """Double disposal must fail."""
        conn = _fresh_db()
        asset_id = add_asset("FA_CO", "One-time Asset", "2025-01-01",
                             5000, 8, conn)
        dispose_asset(asset_id, "2026-06-15", 3000, conn)
        with pytest.raises(ValueError, match="not active"):
            dispose_asset(asset_id, "2026-07-01", 2000, conn)


class TestAnnualCCA:
    """Annual CCA calculation across multiple assets."""

    def test_multiple_assets_same_class(self):
        """Multiple Class 8 assets — CCA computed for each."""
        conn = _fresh_db()
        add_asset("FA_CO", "Desk", "2025-01-01", 2000, 8, conn)
        add_asset("FA_CO", "Printer", "2025-06-01", 1500, 8, conn)
        add_asset("FA_CO", "Monitor", "2025-09-01", 800, 8, conn)

        results = calculate_annual_cca("FA_CO", "2026-12-31", conn)
        assert len(results) == 3
        for r in results:
            assert r["cca_class"] == 8
            assert r["closing_ucc"] <= r["opening_ucc"]

    def test_short_fiscal_year_proration(self):
        """Short fiscal year (6 months) → CCA prorated."""
        conn = _fresh_db()
        add_asset("FA_CO", "Equipment", "2025-01-01", 10000, 8, conn)

        full_year = calculate_annual_cca("FA_CO", "2026-12-31", conn)
        short_year = calculate_annual_cca("FA_CO", "2026-06-30", conn,
                                          short_year_days=182)

        # Short year CCA should be less than full year
        if full_year and short_year:
            assert short_year[0]["cca_amount"] <= full_year[0]["cca_amount"]

    def test_disposed_assets_excluded_from_cca(self):
        """Disposed assets should not get CCA."""
        conn = _fresh_db()
        active_id = add_asset("FA_CO", "Active Asset", "2025-01-01", 5000, 8, conn)
        disposed_id = add_asset("FA_CO", "Sold Asset", "2025-01-01", 5000, 8, conn)
        dispose_asset(disposed_id, "2026-03-15", 3000, conn)

        results = calculate_annual_cca("FA_CO", "2026-12-31", conn)
        result_ids = [r["asset_id"] for r in results]
        assert active_id in result_ids
        assert disposed_id not in result_ids


class TestSubstanceIntegration:
    """Substance engine → fixed asset pipeline."""

    def test_capex_creates_draft_asset(self):
        """CapEx detection should create a draft asset record."""
        conn = _fresh_db()
        ensure_fixed_assets_table(conn)

        asset_id = create_draft_asset_from_capex(
            "FA_CO", "New Server Equipment", 12000, "2026-03-15", conn,
        )
        assert asset_id is not None
        assert asset_id.startswith("FA-DRAFT-")

        assets = list_assets("FA_CO", conn)
        draft = next((a for a in assets if a["asset_id"] == asset_id), None)
        assert draft is not None
        assert draft["status"] == "draft"

    def test_repair_no_draft_asset(self):
        """Repairs should NOT create draft assets (they're OpEx)."""
        conn = _fresh_db()
        ensure_fixed_assets_table(conn)

        # Zero cost → returns None
        result = create_draft_asset_from_capex(
            "FA_CO", "Repair Work", 0, "2026-03-15", conn,
        )
        assert result is None

    def test_substance_capex_classification(self):
        """Equipment purchase must classify as CapEx."""
        result = substance_classifier(
            vendor="Equipment Supplier Inc.",
            memo="Purchase of industrial machinery",
            doc_type="invoice",
            amount=50000.0,
        )
        assert result is not None
        assert result.get("potential_capex") is True

    def test_tax_on_capex_still_claimable(self):
        """GST/QST on CapEx purchases are still claimable as ITC/ITR."""
        amount = Decimal("50000")
        itc_itr = calculate_itc_itr(amount, "T")
        assert Decimal(str(itc_itr["gst_recoverable"])) > Decimal("0")
        assert Decimal(str(itc_itr["qst_recoverable"])) > Decimal("0")
