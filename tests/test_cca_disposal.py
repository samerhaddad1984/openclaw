"""Sprint H F1 — CCA recapture / terminal loss / capital gain tests."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.fixed_assets_engine import (  # noqa: E402
    CAPITAL_GAINS_INCLUSION_RATE,
    add_asset,
    dispose_asset,
    ensure_fixed_assets_table,
    process_asset_disposal,
)


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "fa.db"
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    ensure_fixed_assets_table(c)
    yield c
    c.close()


def _make_asset(conn, *, client="ACME", cost=10_000, cls=10, name="Truck",
                acquisition="2024-06-01", current_ucc=None):
    aid = add_asset(client, name, acquisition, cost, cls, conn)
    if current_ucc is not None:
        conn.execute(
            "UPDATE fixed_assets SET current_ucc=? WHERE asset_id=?",
            (current_ucc, aid),
        )
        conn.commit()
    return aid


# ---------------------------------------------------------------------------

def test_recapture_when_proceeds_exceed_ucc(conn):
    aid = _make_asset(conn, cost=10_000, current_ucc=4_000)
    res = process_asset_disposal(aid, "2025-06-01", 7_500, conn)
    # proceeds (7500) > UCC (4000) but < cost (10000): recapture = 7500 - 4000 = 3500
    assert res["recapture"] == 3500.0
    assert res["recapture_taxable"] is True
    assert res["terminal_loss"] == 0.0


def test_terminal_loss_when_last_asset_in_class(conn):
    aid = _make_asset(conn, cost=10_000, current_ucc=4_000)
    res = process_asset_disposal(aid, "2025-06-01", 1_000, conn)
    # proceeds < UCC, last in class => terminal loss = 4000 - 1000 = 3000
    assert res["terminal_loss"] == 3000.0
    assert res["terminal_loss_deductible"] is True
    assert res["is_last_in_class"] is True


def test_no_terminal_loss_when_other_assets_exist(conn):
    a1 = _make_asset(conn, name="Truck1", cost=10_000, current_ucc=4_000)
    a2 = _make_asset(conn, name="Truck2", cost=8_000, current_ucc=3_000)
    res = process_asset_disposal(a1, "2025-06-01", 1_000, conn)
    assert res["terminal_loss"] == 0.0
    assert res["is_last_in_class"] is False
    # UCC adjustment is -lesser_of(proceeds, cost) = -1000.
    assert res["ucc_adjustment"] == -1000.0


def test_capital_gain_when_proceeds_exceed_cost(conn):
    aid = _make_asset(conn, cost=10_000, current_ucc=4_000)
    res = process_asset_disposal(aid, "2025-06-01", 12_000, conn)
    # cap gain = 12000 - 10000 = 2000
    assert res["capital_gain"] == 2000.0


def test_50_percent_inclusion_rate_on_cap_gain(conn):
    aid = _make_asset(conn, cost=10_000, current_ucc=4_000)
    res = process_asset_disposal(aid, "2025-06-01", 14_000, conn)
    # cap gain = 4000; taxable = 2000.
    assert res["taxable_capital_gain"] == 2000.0
    # And recapture caps at cost: effective_proceeds = min(14000, 10000) = 10000
    # recapture = 10000 - 4000 = 6000
    assert res["recapture"] == 6000.0


def test_ucc_reduction_when_not_last_in_class(conn):
    a1 = _make_asset(conn, name="Asset A", cost=8_000, current_ucc=5_000)
    _ = _make_asset(conn, name="Asset B", cost=6_000, current_ucc=4_000)
    res = process_asset_disposal(a1, "2025-03-01", 3_500, conn)
    # proceeds < UCC, NOT last in class => no terminal loss, just reduce.
    assert res["terminal_loss"] == 0.0
    assert res["ucc_adjustment"] == -3500.0
    assert res["new_class_ucc"] == 1500.0  # 5000 - 3500


def test_disposal_proceeds_zero(conn):
    aid = _make_asset(conn, cost=5_000, current_ucc=2_000)
    res = process_asset_disposal(aid, "2025-06-01", 0, conn)
    # 0 proceeds, last in class => terminal loss = 2000.
    assert res["terminal_loss"] == 2000.0


def test_disposal_proceeds_negative_rejected(conn):
    aid = _make_asset(conn, cost=5_000, current_ucc=2_000)
    with pytest.raises(ValueError):
        process_asset_disposal(aid, "2025-06-01", -100, conn)


def test_class_10_vehicle_disposal(conn):
    aid = _make_asset(conn, name="Pickup", cls=10, cost=35_000, current_ucc=20_000)
    res = process_asset_disposal(aid, "2025-09-01", 25_000, conn)
    assert res["recapture"] == 5_000.0


def test_class_8_furniture_disposal(conn):
    aid = _make_asset(conn, name="Desk", cls=8, cost=2_000, current_ucc=1_200)
    res = process_asset_disposal(aid, "2025-04-15", 500, conn)
    # last in class => terminal loss
    assert res["terminal_loss"] == 700.0


def test_partial_disposal_multiple_assets(conn):
    a1 = _make_asset(conn, name="A", cost=5_000, current_ucc=3_000)
    a2 = _make_asset(conn, name="B", cost=5_000, current_ucc=3_000)
    a3 = _make_asset(conn, name="C", cost=5_000, current_ucc=3_000)
    res = process_asset_disposal(a1, "2025-05-01", 2_000, conn)
    assert res["is_last_in_class"] is False
    res2 = process_asset_disposal(a2, "2025-06-01", 2_000, conn)
    assert res2["is_last_in_class"] is False
    res3 = process_asset_disposal(a3, "2025-07-01", 1_000, conn)
    assert res3["is_last_in_class"] is True
    assert res3["terminal_loss"] == 2_000.0


def test_disposal_creates_adjusting_je(conn):
    aid = _make_asset(conn, cost=10_000, current_ucc=4_000)
    res = process_asset_disposal(aid, "2025-06-01", 7_500, conn,
                                  create_je=True)
    je = res["adjusting_je"]
    assert je is not None
    assert je["balanced"] is True
    assert any(l.get("account") == "4900" for l in je["lines"])  # recapture income


def test_disposal_persists_recapture_amount(conn):
    aid = _make_asset(conn, cost=10_000, current_ucc=4_000)
    process_asset_disposal(aid, "2025-06-01", 7_500, conn)
    row = conn.execute(
        "SELECT recapture_amount, terminal_loss_amount, capital_gain_amount, "
        "disposal_reason FROM fixed_assets WHERE asset_id=?",
        (aid,),
    ).fetchone()
    assert row["recapture_amount"] == 3500.0
    assert row["disposal_reason"] == "sale"


def test_disposal_inactive_asset_raises(conn):
    aid = _make_asset(conn, cost=5_000, current_ucc=2_000)
    process_asset_disposal(aid, "2025-06-01", 1_000, conn)
    with pytest.raises(ValueError):
        process_asset_disposal(aid, "2025-07-01", 500, conn)


def test_capital_gains_inclusion_rate_constant():
    # Lock the 50 % rate; if Canada changes it, this test forces a review.
    from decimal import Decimal as _D
    assert CAPITAL_GAINS_INCLUSION_RATE == _D("0.50")
