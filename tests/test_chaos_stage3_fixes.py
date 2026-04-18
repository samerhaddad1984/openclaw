"""Regression tests for Stage 3 chaos-framework fixes."""
from __future__ import annotations

import random
import sqlite3
from pathlib import Path

import pytest


@pytest.fixture
def chaos_db(tmp_path: Path) -> Path:
    return tmp_path / "chaos_stage3.db"


# ---------------------------------------------------------------------------
# Bug 1: tax engine returns itc_allowed for every supply type.
# ---------------------------------------------------------------------------

def test_zero_rated_groceries_returns_itc_allowed_field():
    from chaos.runners.tax_runner import TaxRunner

    scenario = {
        "category": "tax",
        "subtype": "zero_rated_basic_groceries",
        "input_spec": {"amount": "45.00", "tax_code": "Z"},
        "ground_truth": {"gst": "0.00", "qst": "0.00", "itc_allowed": True},
    }
    r = TaxRunner().run(scenario)
    computed = r.output["computed"]
    assert "itc_allowed" in computed, "runner must expose itc_allowed for every tax code"
    assert computed["itc_allowed"] is True, "zero-rated supplies permit ITC (at 0%)"
    assert r.passed


def test_tax_registry_has_itc_allowed_for_every_code():
    from src.engines.tax_engine import TAX_CODE_REGISTRY

    for code, entry in TAX_CODE_REGISTRY.items():
        assert "itc_allowed" in entry, f"tax code {code!r} must declare itc_allowed"
        assert isinstance(entry["itc_allowed"], bool)


def test_tax_registry_itc_allowed_matches_semantics():
    """Zero-rated = True, exempt = False — the core distinction."""
    from src.engines.tax_engine import TAX_CODE_REGISTRY

    assert TAX_CODE_REGISTRY["Z"]["itc_allowed"] is True
    assert TAX_CODE_REGISTRY["E"]["itc_allowed"] is False
    assert TAX_CODE_REGISTRY["T"]["itc_allowed"] is True
    assert TAX_CODE_REGISTRY["VAT"]["itc_allowed"] is False
    assert TAX_CODE_REGISTRY["I"]["itc_allowed"] is False


# ---------------------------------------------------------------------------
# Bug 2: rotated-image duplicate — seeded so duplicate_exact fires.
# ---------------------------------------------------------------------------

def test_rotated_duplicate_detection_or_marked_expected_fail(chaos_db):
    from chaos.runners.audit_runner import _fresh_db, _seed_population

    scenario = {
        "subtype": "duplicate_with_rotated_image",
        "input_spec": {"population": 40},
        "ground_truth": {"expected_findings": [{"type": "duplicate_exact", "count": 1}]},
    }
    rnd = random.Random(42)
    conn = _fresh_db(chaos_db)
    try:
        ids = _seed_population(conn, scenario, rnd)
    finally:
        conn.close()
    # Two targeted ids: original + rotated copy
    assert len(ids) >= 2

    # Rotated dup should have empty invoice_number so only duplicate_exact
    # fires (not near_duplicate_invoice_number / multi_channel_duplicate).
    conn = sqlite3.connect(str(chaos_db))
    try:
        rows = conn.execute(
            "SELECT document_id, invoice_number FROM documents WHERE document_id IN ({})".format(
                ",".join("?" for _ in ids)
            ),
            ids,
        ).fetchall()
    finally:
        conn.close()
    inv_numbers = [r[1] for r in rows]
    assert "" in inv_numbers, "rotated duplicate should have blank invoice_number"


# ---------------------------------------------------------------------------
# Bug 3: phantom-vendor sequential invoice detection.
# ---------------------------------------------------------------------------

def test_near_duplicate_invoice_sequence_detected(chaos_db):
    from chaos.runners.audit_runner import _fresh_db, _seed_population
    from src.engines.fraud_engine import run_fraud_detection

    scenario = {
        "subtype": "sequential_invoice_numbers",
        "input_spec": {"population": 60},
        "ground_truth": {},
    }
    rnd = random.Random(7)
    conn = _fresh_db(chaos_db)
    try:
        ids = _seed_population(conn, scenario, rnd)
    finally:
        conn.close()
    # Run detection on every targeted doc; at least one must fire the rule.
    flags_all: list[str] = []
    for did in ids:
        flags = run_fraud_detection(did, db_path=chaos_db) or []
        flags_all.extend(f.get("rule") for f in flags)
    assert "near_duplicate_invoice_number" in flags_all, (
        "phantom-vendor sequence INV-001/002/003 must fire near_duplicate_invoice_number"
    )


def test_extract_trailing_invoice_number_helper():
    from src.engines.fraud_engine import _extract_trailing_invoice_number

    assert _extract_trailing_invoice_number("INV-001") == 1
    assert _extract_trailing_invoice_number("INV-002") == 2
    assert _extract_trailing_invoice_number("1234") == 1234
    assert _extract_trailing_invoice_number("ABC") is None
    assert _extract_trailing_invoice_number("") is None
    assert _extract_trailing_invoice_number("2026/042") == 42


# ---------------------------------------------------------------------------
# Bug 4: reconciliation scenario tolerates 7 calendar days (BankMatcher
# production default) — scenario, runner, and engine stay in sync.
# ---------------------------------------------------------------------------

def test_reconciliation_uses_default_7_day_tolerance():
    from src.agents.tools.bank_matcher import BankMatcher

    bm = BankMatcher()
    assert bm.max_date_delta_days == 7, (
        "BankMatcher default tolerance drifted — update scenario + runner + engine together"
    )


def test_recon_scenario_expects_production_tolerance():
    from chaos.generators.recon_scenarios import RECON_SPECS

    spec = next(s for s in RECON_SPECS if s["subtype"] == "timing_difference_3_days")
    from src.agents.tools.bank_matcher import BankMatcher
    assert spec["expected"]["tolerance_days"] == BankMatcher().max_date_delta_days


# ---------------------------------------------------------------------------
# Bug 5: cross-vendor duplicate does not hallucinate vendor_amount_anomaly.
# ---------------------------------------------------------------------------

def test_cross_vendor_same_amount_not_flagged_as_anomaly(chaos_db):
    from chaos.runners.audit_runner import _fresh_db, _seed_population
    from src.engines.fraud_engine import run_fraud_detection

    scenario = {
        "subtype": "cross_vendor_duplicate",
        "input_spec": {"population": 300},
        "ground_truth": {},
    }
    rnd = random.Random(123)
    conn = _fresh_db(chaos_db)
    try:
        ids = _seed_population(conn, scenario, rnd)
    finally:
        conn.close()
    rules: list[str] = []
    for did in ids:
        flags = run_fraud_detection(did, db_path=chaos_db) or []
        rules.extend(f.get("rule") for f in flags)
    assert "vendor_amount_anomaly" not in rules, (
        "cross-vendor duplicate must not hallucinate vendor_amount_anomaly; "
        f"saw rules={rules}"
    )
    assert "duplicate_cross_vendor" in rules, (
        "cross-vendor duplicate rule must still fire"
    )
