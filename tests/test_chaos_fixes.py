"""Regression tests for Stage 2 chaos-framework fixes.

Each test reproduces a minimal version of a scenario that was failing in
the stage-1 chaos run, then asserts the fix holds.
"""
from __future__ import annotations

import sqlite3
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def chaos_db(tmp_path: Path) -> Path:
    return tmp_path / "chaos_fixes.db"


# ---------------------------------------------------------------------------
# Fix 1: audit_runner seeds for amount_just_under_individual_limit use
# distinct amounts so duplicate_exact does not fire alongside
# invoice_splitting_suspected.
# ---------------------------------------------------------------------------

def test_amount_just_under_individual_limit_seeds_distinct_amounts(chaos_db):
    from chaos.runners.audit_runner import _fresh_db, _seed_population

    scenario = {
        "subtype": "amount_just_under_individual_limit",
        "input_spec": {"population": 20},
        "ground_truth": {"expected_rules_fired": ["invoice_splitting_suspected"]},
    }
    import random
    rnd = random.Random(42)
    conn = _fresh_db(chaos_db)
    try:
        _seed_population(conn, scenario, rnd)
        vendor_amounts = [
            r["amount"] for r in conn.execute(
                "SELECT amount FROM documents WHERE vendor = 'Split Vendor Chaos' ORDER BY document_date"
            )
        ]
    finally:
        conn.close()

    # Three distinct amounts — two priors + probe, with priors < probe so
    # duplicate_exact does not fire but cumulative exceeds $2,000.
    assert len(vendor_amounts) == 3
    assert len(set(vendor_amounts)) == 3, "seeds must be distinct to avoid duplicate_exact"
    assert sum(vendor_amounts) > 2000.0
    assert max(vendor_amounts) <= 2000.0  # each tx individually under threshold


def test_split_to_avoid_approval_limit_keeps_duplicate_amounts(chaos_db):
    """split_to_avoid_approval_limit still expects duplicate_exact + splitting."""
    from chaos.runners.audit_runner import _fresh_db, _seed_population

    scenario = {
        "subtype": "split_to_avoid_approval_limit",
        "input_spec": {"population": 20},
        "ground_truth": {"expected_rules_fired": ["invoice_splitting_suspected"]},
    }
    import random
    rnd = random.Random(42)
    conn = _fresh_db(chaos_db)
    try:
        _seed_population(conn, scenario, rnd)
        amounts = [r["amount"] for r in conn.execute(
            "SELECT amount FROM documents WHERE vendor = 'Split Vendor Chaos'"
        )]
    finally:
        conn.close()

    # Intentional duplicates so duplicate_exact + invoice_splitting both fire.
    assert len(amounts) == 3
    assert amounts.count(1999.00) == 3


# ---------------------------------------------------------------------------
# Fix 2: receipt_runner mock noise is absolute (cents), not percentage.
# ---------------------------------------------------------------------------

def test_receipt_mock_noise_is_absolute_cents_not_percentage():
    """A $100 total with 0.04 noise must stay within $0.05 oracle tolerance."""
    import random
    from chaos.runners.receipt_runner import _degrade

    gt = {
        "vendor": "IGA Des Sources",
        "document_date": "2026-04-15",
        "total": "100.00",
        "gst": "5.00",
        "qst": "9.98",
        "currency": "CAD",
        "tax_code": "T",
        "line_count": 6,
    }
    # thermal_fade_50pct uses noise=0.04 per the degradation table.
    # Run many seeds — none should push total beyond $0.05.
    max_delta = Decimal("0")
    for seed in range(200):
        rnd = random.Random(seed)
        ex = _degrade(gt, ["thermal_fade_50pct", "low_contrast"], rnd)
        delta = abs(Decimal(ex["total"]) - Decimal(gt["total"]))
        if delta > max_delta:
            max_delta = delta
    assert max_delta <= Decimal("0.05"), f"noise must stay within $0.05 tolerance, saw {max_delta}"


def test_receipt_mock_vendor_not_dropped_on_faded_thermal():
    """Faded thermal should degrade vendor (prefix), not drop entirely."""
    import random
    from chaos.runners.receipt_runner import _degrade

    gt = {
        "vendor": "Couche-Tard",
        "document_date": "2026-04-15",
        "total": "12.34",
        "gst": "0.62",
        "qst": "1.23",
        "currency": "CAD",
        "tax_code": "T",
        "line_count": 3,
    }
    rnd = random.Random(0)
    ex = _degrade(gt, ["thermal_fade_50pct", "low_contrast"], rnd)
    assert ex["vendor"], "vendor should not be dropped on faded thermal"
    # Prefix of the real name must match what the oracle checks.
    assert gt["vendor"].lower().startswith(ex["vendor"].lower())


# ---------------------------------------------------------------------------
# Fix 3: foreign-language Arabic/Chinese marked expected_fail.
# ---------------------------------------------------------------------------

def test_foreign_language_arabic_and_chinese_are_expected_fail():
    from chaos.generators.scenario_catalog import build_all_scenarios

    scs = {s["subtype"]: s for s in build_all_scenarios() if s.get("category") == "receipts"}
    assert scs["foreign_language_arabic"]["expected_fail"] is True
    assert scs["foreign_language_chinese"]["expected_fail"] is True


# ---------------------------------------------------------------------------
# Fix 4: vendor_amount_anomaly / vendor_timing_anomaly seed ≥5 priors to
# satisfy MIN_HISTORY_FOR_ANOMALY.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("subtype,vendor", [
    ("vendor_amount_anomaly", "History Vendor Amount"),
    ("vendor_timing_anomaly", "History Vendor Timing"),
])
def test_vendor_anomaly_seeds_enough_history(subtype, vendor, chaos_db):
    from chaos.runners.audit_runner import _fresh_db, _seed_population
    from src.engines.fraud_engine import MIN_HISTORY_FOR_ANOMALY

    scenario = {
        "subtype": subtype,
        "input_spec": {"population": 60},
        "ground_truth": {},
    }
    import random
    rnd = random.Random(42)
    conn = _fresh_db(chaos_db)
    try:
        _seed_population(conn, scenario, rnd)
        cnt = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE vendor = ?", (vendor,)
        ).fetchone()[0]
    finally:
        conn.close()

    # Eight priors + one probe = nine total. Need >= MIN_HISTORY_FOR_ANOMALY + 1.
    assert cnt >= MIN_HISTORY_FOR_ANOMALY + 1


# ---------------------------------------------------------------------------
# Fix 5: cashflow engine is wired into the financial runner.
# ---------------------------------------------------------------------------

def test_cashflow_engine_is_invoked_by_financial_runner():
    from chaos.runners.financial_runner import FinancialRunner

    scenario = {
        "category": "financial",
        "subtype": "cashflow_indirect_method",
        "input_spec": {"kind": "financial", "net_income": "50000.00",
                       "ar_change": "-5000.00", "ap_change": "3000.00"},
        "ground_truth": {"statement_built": True},
    }
    runner = FinancialRunner(chaos_db_path=Path("/tmp/chaos_test_cf.db"))
    result = runner.run(scenario)
    assert result.passed, f"cashflow scenario should pass, got {result.oracle_result}"
    assert "cashflow_engine.generate_cash_flow_statement" in result.output["functions_called"]
