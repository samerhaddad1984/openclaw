"""Sprint H F4 — SR&ED ITC + T661 tests."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.sred_engine import (  # noqa: E402
    CCPC_SMALL_TAXABLE_INCOME_LIMIT,
    ENHANCED_RATE,
    REGULAR_RATE,
    add_expenditure,
    calculate_quebec_rd_credit,
    calculate_sred_itc,
    create_claim,
    ensure_sred_tables,
    generate_t661_summary,
    get_claim,
    get_expenditures,
)


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "s.db"
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    ensure_sred_tables(c)
    yield c
    c.close()


def _seed_basic_claim(conn, *, claim_type="traditional"):
    cid = create_claim(conn, client_code="ACME", tax_year=2025,
                       project_name="ML model R&D", claim_type=claim_type,
                       technological_advancement="Better accuracy",
                       technological_obstacles="Sparse training data")
    return cid


# ---------------------------------------------------------------------------

def test_create_claim_inserts(conn):
    cid = _seed_basic_claim(conn)
    claim = get_claim(conn, cid)
    assert claim["project_name"] == "ML model R&D"
    assert claim["claim_type"] == "traditional"


def test_invalid_claim_type_raises(conn):
    with pytest.raises(ValueError):
        create_claim(conn, client_code="ACME", tax_year=2025,
                     project_name="P", claim_type="bogus")


def test_invalid_category_raises(conn):
    cid = _seed_basic_claim(conn)
    with pytest.raises(ValueError):
        add_expenditure(conn, claim_id=cid, category="rent",
                        amount=1000)


def test_negative_amount_raises(conn):
    cid = _seed_basic_claim(conn)
    with pytest.raises(ValueError):
        add_expenditure(conn, claim_id=cid, category="salaries", amount=-100)


def test_ccpc_small_enhanced_35_percent(conn):
    cid = _seed_basic_claim(conn)
    add_expenditure(conn, claim_id=cid, category="salaries", amount=100_000)
    r = calculate_sred_itc(conn, cid, corp_type="ccpc_small",
                            taxable_income=100_000, taxable_capital=1_000_000)
    # 35 % × 100k = 35,000.
    assert r["itc_total"] == 35_000.0
    # 100 % refundable for CCPC small under enhanced.
    assert r["itc_refundable"] == 35_000.0


def test_ccpc_small_above_3m_uses_regular_rate(conn):
    cid = _seed_basic_claim(conn)
    add_expenditure(conn, claim_id=cid, category="salaries", amount=4_000_000)
    r = calculate_sred_itc(conn, cid, corp_type="ccpc_small",
                            taxable_income=400_000, taxable_capital=5_000_000)
    # First 3M × 35% = 1,050,000. Excess 1M × 15% = 150,000. Total = 1,200,000.
    assert r["itc_total"] == 1_200_000.0
    # Refundable: 1,050,000 + 150,000 × 40 % = 1,110,000.
    assert r["itc_refundable"] == 1_110_000.0


def test_other_corp_15_percent_non_refundable(conn):
    cid = _seed_basic_claim(conn)
    add_expenditure(conn, claim_id=cid, category="salaries", amount=100_000)
    r = calculate_sred_itc(conn, cid, corp_type="other")
    assert r["itc_total"] == 15_000.0
    assert r["itc_refundable"] == 0.0
    assert r["itc_non_refundable"] == 15_000.0


def test_proxy_method_uplifts_salaries_by_55(conn):
    cid = _seed_basic_claim(conn, claim_type="proxy")
    add_expenditure(conn, claim_id=cid, category="salaries", amount=100_000)
    r = calculate_sred_itc(conn, cid, corp_type="ccpc_small")
    # qualifying = 100k + (100k × 55%) = 155k. ITC = 155k × 35 % = 54,250.
    assert r["proxy_uplift_applied"] == 55_000.0
    assert r["qualifying_expenditures"] == 155_000.0
    assert r["itc_total"] == 54_250.0


def test_traditional_method_no_uplift(conn):
    cid = _seed_basic_claim(conn, claim_type="traditional")
    add_expenditure(conn, claim_id=cid, category="salaries", amount=100_000)
    r = calculate_sred_itc(conn, cid, corp_type="ccpc_small")
    assert r["proxy_uplift_applied"] == 0.0


def test_ccpc_threshold_breach_falls_to_regular(conn):
    cid = _seed_basic_claim(conn)
    add_expenditure(conn, claim_id=cid, category="salaries", amount=100_000)
    # Taxable income exceeds the limit → drop to regular CCPC.
    r = calculate_sred_itc(conn, cid, corp_type="ccpc",
                            taxable_income=600_000)
    # 15 % × 100,000 = 15,000.
    assert r["itc_total"] == 15_000.0
    # 40 % refundable.
    assert r["itc_refundable"] == 6_000.0


def test_no_expenditures_returns_zero(conn):
    cid = _seed_basic_claim(conn)
    r = calculate_sred_itc(conn, cid, corp_type="ccpc_small")
    assert r["itc_total"] == 0.0
    assert r["status"] == "no_expenditures"


def test_qualifying_amount_overrides_amount(conn):
    cid = _seed_basic_claim(conn)
    add_expenditure(conn, claim_id=cid, category="salaries",
                    amount=100_000, qualifying_amount=80_000)
    r = calculate_sred_itc(conn, cid, corp_type="ccpc_small")
    # 80k × 35 % = 28,000.
    assert r["itc_total"] == 28_000.0
    assert r["qualifying_expenditures"] == 80_000.0


def test_quebec_sme_credit_30_percent(conn):
    cid = _seed_basic_claim(conn)
    add_expenditure(conn, claim_id=cid, category="salaries", amount=200_000)
    r = calculate_quebec_rd_credit(conn, cid, is_sme=True)
    # 200k × 30 % = 60,000.
    assert r["credit"] == 60_000.0
    assert r["refundable"] is True


def test_quebec_large_corp_lower_rate(conn):
    cid = _seed_basic_claim(conn)
    add_expenditure(conn, claim_id=cid, category="salaries", amount=200_000)
    r = calculate_quebec_rd_credit(conn, cid, is_sme=False)
    assert r["credit"] == 28_000.0  # 200k × 14 %


def test_t661_summary_has_required_lines(conn):
    cid = _seed_basic_claim(conn)
    add_expenditure(conn, claim_id=cid, category="salaries", amount=200_000)
    s = generate_t661_summary(conn, cid, corp_type="ccpc_small")
    assert s["form"] == "T661"
    assert "line_500_itc_earned" in s
    assert s["line_500_itc_earned"] == 70_000.0  # 35 % × 200k
    assert s["client_code"] == "ACME"


def test_get_claim_missing_raises(conn):
    with pytest.raises(ValueError):
        get_claim(conn, 99999)
