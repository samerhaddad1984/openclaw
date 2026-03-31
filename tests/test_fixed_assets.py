"""
tests/test_fixed_assets.py — Fixed Assets Engine & CCA Schedule tests.
"""
from __future__ import annotations

import sqlite3
import pytest
from decimal import Decimal

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.fixed_assets_engine import (
    CCA_CLASSES,
    add_asset,
    calculate_annual_cca,
    dispose_asset,
    ensure_fixed_assets_table,
    generate_schedule_8,
    list_assets,
    normalize_cca_class,
    cca_class_display,
    create_draft_asset_from_capex,
)


@pytest.fixture
def conn():
    """In-memory SQLite with dict row factory."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    ensure_fixed_assets_table(c)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# CCA class validation
# ---------------------------------------------------------------------------

class TestCCAClasses:
    def test_all_classes_present(self):
        assert 1 in CCA_CLASSES
        assert 101 in CCA_CLASSES  # 10.1
        assert 141 in CCA_CLASSES  # 14.1
        assert 50 in CCA_CLASSES
        assert 55 in CCA_CLASSES

    def test_normalize_class_101(self):
        assert normalize_cca_class("10.1") == 101
        assert normalize_cca_class(10.1) == 101
        assert normalize_cca_class(101) == 101

    def test_normalize_class_141(self):
        assert normalize_cca_class("14.1") == 141
        assert normalize_cca_class(14.1) == 141

    def test_normalize_regular(self):
        assert normalize_cca_class(8) == 8
        assert normalize_cca_class("45") == 45

    def test_normalize_invalid(self):
        assert normalize_cca_class("999") is None
        assert normalize_cca_class("abc") is None

    def test_display(self):
        assert cca_class_display(101) == "10.1"
        assert cca_class_display(141) == "14.1"
        assert cca_class_display(8) == "8"


# ---------------------------------------------------------------------------
# add_asset + half-year rule
# ---------------------------------------------------------------------------

class TestAddAsset:
    def test_add_asset_basic(self, conn):
        aid = add_asset("ACME", "Truck", "2026-01-15", 60000, 10, conn)
        assert aid.startswith("FA-")

        row = conn.execute("SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)).fetchone()
        assert row is not None
        assert dict(row)["client_code"] == "ACME"
        assert dict(row)["cost"] == 60000.0

    def test_half_year_rule(self, conn):
        """Class 10 at 30%: first year CCA = (60000 * 0.30) / 2 = 9000."""
        aid = add_asset("ACME", "Van", "2026-03-01", 60000, 10, conn)
        row = dict(conn.execute("SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)).fetchone())

        expected_cca = 9000.0  # (60000 * 0.30) / 2
        assert row["accumulated_cca"] == expected_cca
        assert row["current_ucc"] == 60000.0 - expected_cca  # 51000

    def test_half_year_rule_class_8(self, conn):
        """Class 8 at 20%: first year CCA = (10000 * 0.20) / 2 = 1000."""
        aid = add_asset("ACME", "Equipment", "2026-06-01", 10000, 8, conn)
        row = dict(conn.execute("SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)).fetchone())

        assert row["accumulated_cca"] == 1000.0
        assert row["current_ucc"] == 9000.0

    def test_half_year_rule_100_pct(self, conn):
        """Class 12 (100%): first year = (500 * 1.0) / 2 = 250."""
        aid = add_asset("ACME", "Small Tool", "2026-01-01", 500, 12, conn)
        row = dict(conn.execute("SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)).fetchone())

        assert row["accumulated_cca"] == 250.0
        assert row["current_ucc"] == 250.0

    def test_invalid_class_raises(self, conn):
        with pytest.raises(ValueError, match="Invalid CCA class"):
            add_asset("ACME", "Mystery", "2026-01-01", 1000, 999, conn)

    def test_negative_cost_raises(self, conn):
        with pytest.raises(ValueError, match="Cost must be positive"):
            add_asset("ACME", "Bad", "2026-01-01", -100, 8, conn)


# ---------------------------------------------------------------------------
# calculate_annual_cca
# ---------------------------------------------------------------------------

class TestCalculateAnnualCCA:
    def test_basic_calculation(self, conn):
        add_asset("ACME", "Laptop", "2025-06-01", 2000, 50, conn)
        results = calculate_annual_cca("ACME", "2026-12-31", conn)

        assert len(results) == 1
        r = results[0]
        assert r["cca_class"] == 50
        assert r["cca_amount"] > 0
        assert r["closing_ucc"] < r["opening_ucc"]

    def test_short_year_proration(self, conn):
        aid = add_asset("ACME", "Desk", "2025-01-01", 5000, 8, conn)
        # Reset UCC for clean test
        conn.execute("UPDATE fixed_assets SET current_ucc = 5000, accumulated_cca = 0 WHERE asset_id = ?", (aid,))
        conn.commit()

        # Full year
        full = calculate_annual_cca("ACME", "2026-12-31", conn)
        full_cca = full[0]["cca_amount"]

        # Reset for short year
        conn.execute("UPDATE fixed_assets SET current_ucc = 5000, accumulated_cca = 0 WHERE asset_id = ?", (aid,))
        conn.commit()

        short = calculate_annual_cca("ACME", "2026-06-30", conn, short_year_days=182)
        short_cca = short[0]["cca_amount"]

        # Short year CCA should be less than full year
        assert short_cca < full_cca

    def test_zero_ucc_no_cca(self, conn):
        aid = add_asset("ACME", "Gone", "2025-01-01", 100, 12, conn)
        conn.execute("UPDATE fixed_assets SET current_ucc = 0 WHERE asset_id = ?", (aid,))
        conn.commit()

        results = calculate_annual_cca("ACME", "2026-12-31", conn)
        assert results[0]["cca_amount"] == 0.0


# ---------------------------------------------------------------------------
# generate_schedule_8
# ---------------------------------------------------------------------------

class TestSchedule8:
    def test_schedule_8_structure(self, conn):
        add_asset("ACME", "Truck", "2025-01-01", 50000, 10, conn)
        add_asset("ACME", "Laptop", "2025-06-01", 3000, 50, conn)

        data = generate_schedule_8("ACME", "2026", conn)

        assert data["client_code"] == "ACME"
        assert data["fiscal_year"] == "2026"
        assert "classes" in data
        assert "totals" in data
        assert len(data["classes"]) == 2  # class 10 and 50

        # Totals should be non-zero
        assert data["totals"]["closing_ucc"] > 0

    def test_schedule_8_groups_by_class(self, conn):
        add_asset("ACME", "Truck 1", "2025-01-01", 40000, 10, conn)
        add_asset("ACME", "Truck 2", "2025-06-01", 35000, 10, conn)
        add_asset("ACME", "PC", "2025-03-01", 2000, 50, conn)

        data = generate_schedule_8("ACME", "2026", conn)

        class_10 = [c for c in data["classes"] if c["cca_class"] == 10]
        assert len(class_10) == 1
        assert len(class_10[0]["assets"]) == 2


# ---------------------------------------------------------------------------
# dispose_asset — recapture, terminal loss, capital gain
# ---------------------------------------------------------------------------

class TestDisposeAsset:
    def test_terminal_loss(self, conn):
        """Sell below UCC when class is empty → terminal loss."""
        aid = add_asset("ACME", "Old Machine", "2024-01-01", 10000, 8, conn)
        # Set UCC to 5000
        conn.execute("UPDATE fixed_assets SET current_ucc = 5000 WHERE asset_id = ?", (aid,))
        conn.commit()

        result = dispose_asset(aid, "2026-06-01", 2000, conn)

        assert result["terminal_loss"] == 3000.0  # 5000 - 2000
        assert result["recapture"] == 0.0
        assert result["capital_gain"] == 0.0

    def test_recapture(self, conn):
        """Sell above UCC but below cost → recapture."""
        aid = add_asset("ACME", "Equipment", "2024-01-01", 10000, 8, conn)
        conn.execute("UPDATE fixed_assets SET current_ucc = 3000 WHERE asset_id = ?", (aid,))
        conn.commit()

        result = dispose_asset(aid, "2026-06-01", 7000, conn)

        assert result["recapture"] == 4000.0  # 7000 - 3000
        assert result["terminal_loss"] == 0.0
        assert result["capital_gain"] == 0.0

    def test_capital_gain(self, conn):
        """Sell above original cost → capital gain + recapture."""
        aid = add_asset("ACME", "Building", "2024-01-01", 100000, 1, conn)
        conn.execute("UPDATE fixed_assets SET current_ucc = 80000 WHERE asset_id = ?", (aid,))
        conn.commit()

        result = dispose_asset(aid, "2026-06-01", 120000, conn)

        assert result["capital_gain"] == 20000.0  # 120000 - 100000
        # Recapture is on proceeds capped at cost (100000) vs UCC (80000)
        assert result["recapture"] == 20000.0  # 100000 - 80000
        assert result["terminal_loss"] == 0.0

    def test_dispose_updates_status(self, conn):
        aid = add_asset("ACME", "Disposed Item", "2024-01-01", 5000, 8, conn)
        dispose_asset(aid, "2026-06-01", 1000, conn)

        row = dict(conn.execute("SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)).fetchone())
        assert row["status"] == "disposed"
        assert row["current_ucc"] == 0.0

    def test_dispose_already_disposed_raises(self, conn):
        aid = add_asset("ACME", "Item", "2024-01-01", 5000, 8, conn)
        dispose_asset(aid, "2026-06-01", 1000, conn)

        with pytest.raises(ValueError, match="not active"):
            dispose_asset(aid, "2026-07-01", 500, conn)

    def test_dispose_not_found_raises(self, conn):
        with pytest.raises(ValueError, match="not found"):
            dispose_asset("NONEXISTENT", "2026-01-01", 0, conn)


# ---------------------------------------------------------------------------
# Draft asset from CapEx detection
# ---------------------------------------------------------------------------

class TestDraftAsset:
    def test_create_draft(self, conn):
        aid = create_draft_asset_from_capex("ACME", "New Machine", 25000, "2026-03-15", conn)
        assert aid is not None
        assert aid.startswith("FA-DRAFT-")

        row = dict(conn.execute("SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)).fetchone())
        assert row["status"] == "draft"
        assert row["cca_class"] == 8  # default

    def test_zero_cost_returns_none(self, conn):
        assert create_draft_asset_from_capex("ACME", "Nothing", 0, "2026-01-01", conn) is None


# ---------------------------------------------------------------------------
# list_assets
# ---------------------------------------------------------------------------

class TestListAssets:
    def test_list_by_client(self, conn):
        add_asset("ACME", "A1", "2026-01-01", 1000, 8, conn)
        add_asset("ACME", "A2", "2026-01-01", 2000, 10, conn)
        add_asset("BETA", "B1", "2026-01-01", 3000, 50, conn)

        acme = list_assets("ACME", conn)
        assert len(acme) == 2

        beta = list_assets("BETA", conn)
        assert len(beta) == 1

    def test_list_by_status(self, conn):
        add_asset("ACME", "Active", "2026-01-01", 1000, 8, conn)
        aid = add_asset("ACME", "ToDispose", "2024-01-01", 2000, 8, conn)
        dispose_asset(aid, "2026-06-01", 500, conn)

        active = list_assets("ACME", conn, status="active")
        assert len(active) == 1

        disposed = list_assets("ACME", conn, status="disposed")
        assert len(disposed) == 1
