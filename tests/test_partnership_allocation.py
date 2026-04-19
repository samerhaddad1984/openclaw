"""Sprint H F3 — partnership allocation tests."""
from __future__ import annotations

import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.partnership_engine import (  # noqa: E402
    add_partner,
    calculate_active_days,
    compute_partnership_allocation,
    create_partnership,
    ensure_partnership_tables,
    get_active_partners,
    list_partnerships,
)


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "p.db"
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    ensure_partnership_tables(c)
    yield c
    c.close()


# ---------------------------------------------------------------------------

def test_create_partnership_inserts(conn):
    pid = create_partnership(conn, client_code="ACME",
                             partnership_name="ACME Partners")
    assert pid > 0
    rows = list_partnerships(conn, client_code="ACME")
    assert len(rows) == 1
    assert rows[0]["partnership_name"] == "ACME Partners"


def test_add_partner_validates_pct(conn):
    pid = create_partnership(conn, client_code="ACME", partnership_name="P")
    with pytest.raises(ValueError):
        add_partner(conn, partnership_id=pid, partner_name="X",
                    allocation_percentage=120, effective_date="2025-01-01")


def test_60_30_10_allocation_full_year(conn):
    pid = create_partnership(conn, client_code="ACME", partnership_name="P")
    add_partner(conn, partnership_id=pid, partner_name="A",
                allocation_percentage=60.0, effective_date="2025-01-01")
    add_partner(conn, partnership_id=pid, partner_name="B",
                allocation_percentage=30.0, effective_date="2025-01-01")
    add_partner(conn, partnership_id=pid, partner_name="C",
                allocation_percentage=10.0, effective_date="2025-01-01")
    r = compute_partnership_allocation(conn, pid, 2025, 100_000)
    by = {a["partner_name"]: a["allocated_income"] for a in r["allocations"]}
    assert by["A"] == 60_000.0
    assert by["B"] == 30_000.0
    assert by["C"] == 10_000.0
    assert "warning" not in r


def test_partner_joining_mid_year_prorated(conn):
    pid = create_partnership(conn, client_code="ACME", partnership_name="P")
    add_partner(conn, partnership_id=pid, partner_name="A",
                allocation_percentage=100.0, effective_date="2025-07-01")
    r = compute_partnership_allocation(conn, pid, 2025, 36_500)
    a = r["allocations"][0]
    # 184 days (Jul 1 - Dec 31). 184/365 = 0.5041. 36500 × 1.0 × 0.5041 = 18,400.
    assert 18_300 < a["allocated_income"] < 18_500


def test_partner_leaving_mid_year_prorated(conn):
    pid = create_partnership(conn, client_code="ACME", partnership_name="P")
    add_partner(conn, partnership_id=pid, partner_name="A",
                allocation_percentage=100.0, effective_date="2025-01-01",
                end_date="2025-06-30")
    r = compute_partnership_allocation(conn, pid, 2025, 36_500)
    a = r["allocations"][0]
    # Jan 1 - Jun 30 = 181 days. 36500 × 181/365 = 18,100.
    assert 18_000 < a["allocated_income"] < 18_300


def test_no_partners_warning(conn):
    pid = create_partnership(conn, client_code="ACME", partnership_name="P")
    r = compute_partnership_allocation(conn, pid, 2025, 50_000)
    assert "warning" in r
    assert r["allocations"] == []


def test_allocation_mismatch_warning(conn):
    # Two partners with 60/30 = 90% (intentional gap).
    pid = create_partnership(conn, client_code="ACME", partnership_name="P")
    add_partner(conn, partnership_id=pid, partner_name="A",
                allocation_percentage=60.0, effective_date="2025-01-01")
    add_partner(conn, partnership_id=pid, partner_name="B",
                allocation_percentage=30.0, effective_date="2025-01-01")
    r = compute_partnership_allocation(conn, pid, 2025, 100_000)
    assert r["total_allocated"] == 90_000.0
    assert "warning" in r


def test_t5013_slip_generated(conn):
    pid = create_partnership(conn, client_code="ACME", partnership_name="P")
    add_partner(conn, partnership_id=pid, partner_name="Alice",
                allocation_percentage=100.0, effective_date="2025-01-01",
                partner_sin_or_bn="123456789")
    r = compute_partnership_allocation(conn, pid, 2025, 50_000)
    slip = r["allocations"][0]["t5013_slip"]
    assert slip["form"] == "T5013"
    assert slip["partner_name"] == "Alice"
    assert slip["box_104_share_of_net_income"] == 50_000.0
    assert slip["partner_sin_or_bn"] == "123456789"


def test_corp_partner_uses_code_5(conn):
    pid = create_partnership(conn, client_code="ACME", partnership_name="P")
    add_partner(conn, partnership_id=pid, partner_name="ACME Inc",
                partner_type="corporation",
                allocation_percentage=100.0, effective_date="2025-01-01")
    r = compute_partnership_allocation(conn, pid, 2025, 50_000)
    assert r["allocations"][0]["t5013_slip"]["box_002_partner_code"] == "5"


def test_get_active_partners_excludes_pre_year(conn):
    pid = create_partnership(conn, client_code="ACME", partnership_name="P")
    add_partner(conn, partnership_id=pid, partner_name="GoneBefore",
                allocation_percentage=100.0,
                effective_date="2023-01-01", end_date="2023-12-31")
    add_partner(conn, partnership_id=pid, partner_name="Present",
                allocation_percentage=100.0,
                effective_date="2025-01-01")
    actives = get_active_partners(conn, pid, 2025)
    names = {p["partner_name"] for p in actives}
    assert "Present" in names
    assert "GoneBefore" not in names


def test_active_days_full_year(conn):
    p = {"effective_date": "2024-01-01", "end_date": None}
    assert calculate_active_days(p, 2025) == 365


def test_active_days_leap_year(conn):
    p = {"effective_date": "2020-01-01", "end_date": None}
    assert calculate_active_days(p, 2024) == 366


def test_loss_allocation_negative_income(conn):
    pid = create_partnership(conn, client_code="ACME", partnership_name="P")
    add_partner(conn, partnership_id=pid, partner_name="A",
                allocation_percentage=50.0, effective_date="2025-01-01")
    add_partner(conn, partnership_id=pid, partner_name="B",
                allocation_percentage=50.0, effective_date="2025-01-01")
    r = compute_partnership_allocation(conn, pid, 2025, -20_000)
    by = {a["partner_name"]: a["allocated_income"] for a in r["allocations"]}
    assert by["A"] == -10_000.0
    assert by["B"] == -10_000.0
