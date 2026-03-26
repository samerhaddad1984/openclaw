#!/usr/bin/env python3
"""
tests/test_stress_test.py

8 pytest tests for scripts/run_stress_test.py.
These are integration tests that run against the live test database.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from scripts.run_stress_test import (
    check_1_tax_engine_math,
    check_2_fraud_engine_coverage,
    check_3_hallucination_guard,
    check_4_learning_memory,
    check_5_ai_router_cache,
    check_6_performance,
    check_7_filing_summary,
    DB_PATH,
)


@pytest.fixture(scope="module", autouse=True)
def _require_db():
    """Skip all tests if the database does not exist."""
    if not DB_PATH.exists():
        pytest.skip(f"Database not found: {DB_PATH}")


# ── Test 1: Tax engine math produces correct structure and passes ─────────

def test_check_1_tax_engine_math():
    result = check_1_tax_engine_math()
    assert "name" in result
    assert "passed" in result
    assert "checked" in result
    assert "mismatches" in result
    assert "mismatch_rate" in result
    assert result["checked"] > 0, "Expected at least some documents to check"
    assert result["passed"], (
        f"Tax engine math failed: {result['mismatches']}/{result['checked']} "
        f"mismatches ({result['mismatch_rate']})"
    )


# ── Test 2: Fraud engine coverage for all four scenarios ──────────────────

def test_check_2_fraud_engine_coverage():
    result = check_2_fraud_engine_coverage()
    assert "sub_checks" in result
    for scenario in ("duplicate", "weekend", "new_vendor", "round_number"):
        sc = result["sub_checks"][scenario]
        assert sc["total"] > 0, f"No {scenario} docs found"
        assert sc["passed"], (
            f"Fraud coverage failed for {scenario}: "
            f"{sc['flagged']}/{sc['total']} ({sc['rate']})"
        )
    assert result["passed"]


# ── Test 3: Hallucination guard detects low-confidence and math mismatch ──

def test_check_3_hallucination_guard():
    result = check_3_hallucination_guard()
    assert "low_confidence" in result
    assert "math_mismatch" in result

    lc = result["low_confidence"]
    assert lc["total"] > 0, "No low-confidence documents found"
    assert lc["passed"], (
        f"Low-confidence sub-check failed: "
        f"{lc['needs_review']}/{lc['total']} ({lc['rate']})"
    )

    mm = result["math_mismatch"]
    assert mm["total"] > 0, "No math_mismatch documents found"
    assert mm["passed"], (
        f"Math mismatch sub-check failed: "
        f"{mm['detected']}/{mm['total']} ({mm['rate']})"
    )

    assert result["passed"]


# ── Test 4: Learning memory returns suggestions for most documents ────────

def test_check_4_learning_memory():
    result = check_4_learning_memory()
    assert "total_sampled" in result
    assert "with_suggestion" in result
    assert result["total_sampled"] == 500, (
        f"Expected 500 sampled (20 per client × 25 clients), got {result['total_sampled']}"
    )
    assert result["passed"], (
        f"Learning suggestions failed: "
        f"{result['with_suggestion']}/{result['total_sampled']} ({result['rate']})"
    )


# ── Test 5: AI router cache has at least 200 seeded vendor entries ────────

def test_check_5_ai_router_cache():
    result = check_5_ai_router_cache()
    assert "vendor_entries" in result
    assert result["vendor_entries"] >= 500, (
        f"Expected >= 500 vendor cache entries, got {result['vendor_entries']}"
    )
    assert result["passed"]


# ── Test 6: Performance under 500ms per document ─────────────────────────

def test_check_6_performance():
    result = check_6_performance()
    assert "documents_processed" in result
    assert "avg_ms" in result
    assert result["documents_processed"] == 500
    avg = float(result["avg_ms"])
    assert avg < 500, f"Average {avg}ms exceeds 500ms threshold"
    assert result["passed"]


# ── Test 7: Filing summary math is internally consistent ─────────────────

def test_check_7_filing_summary():
    result = check_7_filing_summary()
    assert "clients" in result
    assert len(result["clients"]) == 25
    for client, info in result["clients"].items():
        assert "error" not in info, f"{client} filing summary error: {info.get('error')}"
        assert info["correct"], (
            f"{client} filing summary incorrect: "
            f"totals_match={info['totals_match']}, "
            f"line_item_errors={info['line_item_errors']}"
        )
    assert result["passed"]


# ── Test 8: Overall — at least 6 of 7 checks pass ────────────────────────

def test_overall_pass_count():
    checks = [
        check_1_tax_engine_math,
        check_2_fraud_engine_coverage,
        check_3_hallucination_guard,
        check_4_learning_memory,
        check_5_ai_router_cache,
        check_6_performance,
        check_7_filing_summary,
    ]
    passed = 0
    failed_names = []
    for fn in checks:
        try:
            result = fn()
            if result["passed"]:
                passed += 1
            else:
                failed_names.append(result["name"])
        except Exception as exc:
            failed_names.append(f"{fn.__name__}: {exc}")

    assert passed >= 6, (
        f"Only {passed}/7 checks passed. Failed: {', '.join(failed_names)}"
    )
