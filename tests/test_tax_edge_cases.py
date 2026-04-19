"""Sprint H F5 — NCL / residential rebate / gift cards / classification."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.tax_edge_cases import (  # noqa: E402
    NCL_CARRYFORWARD_YEARS,
    apply_ncl_carryforward,
    calculate_residential_rebate,
    ensure_ncl_table,
    get_ncl_balance,
    gift_card_tax_treatment,
    is_exempt_supply,
    is_zero_rated_grocery,
    record_ncl,
)


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "te.db"
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    ensure_ncl_table(c)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# NCL carryforward
# ---------------------------------------------------------------------------

def test_record_ncl_sets_expires_year(conn):
    record_ncl(conn, client_code="ACME", origin_year=2020, amount=10_000)
    row = conn.execute("SELECT expires_year FROM non_capital_losses").fetchone()
    assert row["expires_year"] == 2020 + NCL_CARRYFORWARD_YEARS


def test_negative_amount_rejected(conn):
    with pytest.raises(ValueError):
        record_ncl(conn, client_code="ACME", origin_year=2020, amount=-100)


def test_apply_ncl_to_positive_income(conn):
    record_ncl(conn, client_code="ACME", origin_year=2022, amount=20_000)
    r = apply_ncl_carryforward(conn, client_code="ACME", fiscal_year=2025,
                                current_income=35_000)
    assert r["applied"] == 20_000.0
    assert r["effective_taxable_income"] == 15_000.0
    assert r["remaining_ncl"] == 0.0


def test_apply_capped_at_income(conn):
    record_ncl(conn, client_code="ACME", origin_year=2022, amount=100_000)
    r = apply_ncl_carryforward(conn, client_code="ACME", fiscal_year=2025,
                                current_income=20_000)
    assert r["applied"] == 20_000.0
    assert r["remaining_ncl"] == 80_000.0
    assert r["effective_taxable_income"] == 0.0


def test_apply_zero_when_no_income(conn):
    record_ncl(conn, client_code="ACME", origin_year=2022, amount=10_000)
    r = apply_ncl_carryforward(conn, client_code="ACME", fiscal_year=2025,
                                current_income=0)
    assert r["applied"] == 0.0
    assert r["remaining_ncl"] == 10_000.0


def test_expired_ncl_not_applied(conn):
    # NCL from 2002 expires 2022; cannot be applied to 2025.
    record_ncl(conn, client_code="ACME", origin_year=2002, amount=10_000)
    r = apply_ncl_carryforward(conn, client_code="ACME", fiscal_year=2025,
                                current_income=15_000)
    assert r["applied"] == 0.0


def test_fifo_oldest_consumed_first(conn):
    record_ncl(conn, client_code="ACME", origin_year=2022, amount=5_000)
    record_ncl(conn, client_code="ACME", origin_year=2024, amount=5_000)
    apply_ncl_carryforward(conn, client_code="ACME", fiscal_year=2025,
                            current_income=5_000)
    rows = conn.execute(
        "SELECT origin_year, applied_amount FROM non_capital_losses ORDER BY origin_year"
    ).fetchall()
    # Oldest (2022) should be fully drawn, 2024 untouched.
    assert rows[0]["applied_amount"] == 5_000.0
    assert rows[1]["applied_amount"] == 0.0


# ---------------------------------------------------------------------------
# Residential rebate
# ---------------------------------------------------------------------------

def test_rebate_full_at_low_price():
    r = calculate_residential_rebate(300_000, province="QC")
    assert r["qualifies"] is True
    assert r["federal_rebate"] > 0
    # Should be capped or proportional, not zero.
    assert r["federal_rebate"] <= 6_300


def test_rebate_zero_at_high_price():
    r = calculate_residential_rebate(500_000, province="QC")
    assert r["federal_rebate"] == 0.0


def test_rebate_phase_out_in_middle():
    r1 = calculate_residential_rebate(400_000, province="QC")
    r2 = calculate_residential_rebate(425_000, province="QC")
    # Higher price = lower rebate in the phase-out range.
    assert r1["federal_rebate"] > r2["federal_rebate"]


def test_rebate_qc_provincial_component():
    r = calculate_residential_rebate(400_000, province="QC")
    assert r["provincial_rebate"] > 0


def test_rebate_non_qc_no_provincial():
    r = calculate_residential_rebate(400_000, province="ON")
    assert r["provincial_rebate"] == 0.0


def test_rebate_non_principal_residence_excluded():
    r = calculate_residential_rebate(400_000, is_principal_residence=False)
    assert r["qualifies"] is False
    assert r["total_rebate"] == 0.0


# ---------------------------------------------------------------------------
# Gift cards
# ---------------------------------------------------------------------------

def test_gift_card_sale_no_tax():
    r = gift_card_tax_treatment("sale")
    assert r["tax_collected_at_sale"] is False
    assert r["treat_as"] == "deferred_revenue"


def test_gift_card_redemption_taxable():
    r = gift_card_tax_treatment("redemption")
    assert r["tax_collected_at_sale"] is True


def test_gift_card_unknown_action_raises():
    with pytest.raises(ValueError):
        gift_card_tax_treatment("destroyed")


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def test_milk_zero_rated():
    assert is_zero_rated_grocery("Whole milk 2 L") is True


def test_pet_food_not_zero_rated():
    assert is_zero_rated_grocery("Cat food premium") is False


def test_residential_rent_exempt():
    assert is_exempt_supply("Loyer mensuel - apartment 4B") is True


def test_office_supplies_not_exempt():
    assert is_exempt_supply("Office paper supply") is False
