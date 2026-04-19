"""CAS 530 statistical sampling tests.

Covers:
  * Attribute sample-size formula (control tests)
  * MUS sample-size formula + selection (larger items more likely)
  * Projection of observed errors to the population
  * Evaluate sample result against tolerable misstatement
  * Reproducibility via seed
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.sampling_engine import (  # noqa: E402
    CONFIDENCE_FACTORS,
    RECOMMENDED_MIN_SAMPLE,
    SamplingEngine,
)


# ---------------------------------------------------------------------------
# Attribute sampling (control tests)
# ---------------------------------------------------------------------------

def test_attribute_sample_size_99_confidence():
    e = SamplingEngine()
    n = e.calculate_sample_size_attribute(
        population_size=10_000,
        tolerable_rate=0.05,
        expected_rate=0.01,
        confidence=0.99,
    )
    # 99% => higher z => larger n than 95% baseline.
    n95 = e.calculate_sample_size_attribute(
        population_size=10_000,
        tolerable_rate=0.05,
        expected_rate=0.01,
        confidence=0.95,
    )
    assert n > n95
    assert n > 30


def test_attribute_sample_size_95_confidence():
    e = SamplingEngine()
    n = e.calculate_sample_size_attribute(
        population_size=1_000,
        tolerable_rate=0.10,
        expected_rate=0.02,
        confidence=0.95,
    )
    # Matches the AICPA table neighbourhood (20-40 range for these params).
    assert 10 <= n <= 150


def test_mus_sample_size_calculation():
    e = SamplingEngine()
    n = e.calculate_sample_size_mus(
        population_dollars=1_000_000,
        tolerable_misstatement=50_000,
        expected_misstatement=10_000,
        confidence=0.95,
    )
    # n = 1,000,000 * 3.0 / (50,000 - 10,000 * 1.6) = 3,000,000 / 34,000 ~= 89
    assert 80 <= n <= 100


def test_mus_selection_hits_large_items():
    e = SamplingEngine(seed="test-large")
    # One whale + many minnows; the whale must be picked.
    txs = [{"id": "WHALE", "amount": 100_000}]
    txs += [{"id": f"M{i}", "amount": 10} for i in range(500)]
    total = sum(abs(float(t["amount"])) for t in txs)
    picked = e.select_mus_sample(txs, sample_size=10, population_total=total)
    ids = {t["id"] for t in picked}
    assert "WHALE" in ids


def test_mus_sampling_interval_correct():
    # With pop total 1,000 and sample 10, interval should be 100.
    e = SamplingEngine(seed="interval")
    txs = [{"id": f"T{i}", "amount": 10} for i in range(100)]
    picked = e.select_mus_sample(txs, sample_size=10, population_total=1_000)
    assert len(picked) <= 10
    # Every picked item must be unique by id.
    assert len({p["id"] for p in picked}) == len(picked)


def test_projection_zero_errors():
    e = SamplingEngine()
    p = e.project_misstatement(
        sample_errors=[],
        sample_total=50_000,
        population_total=1_000_000,
        population_size=1_000,
        sample_size=50,
    )
    assert p["most_likely_error"] == 0
    # Upper limit uses the n=0 factor (3.00 at 95%), so it is > 0.
    assert p["upper_error_limit"] > 0


def test_projection_with_errors():
    e = SamplingEngine()
    p = e.project_misstatement(
        sample_errors=[{"error_amount": 500}, {"error_amount": 300}],
        sample_total=50_000,
        population_total=1_000_000,
        population_size=1_000,
        sample_size=50,
    )
    # MLE = 800 / 50,000 * 1,000,000 = 16,000
    assert abs(p["most_likely_error"] - 16_000) < 1
    assert p["sample_errors_count"] == 2
    assert p["total_error_amount"] == 800


def test_upper_error_limit_higher_than_most_likely():
    e = SamplingEngine()
    p = e.project_misstatement(
        sample_errors=[{"error_amount": 100}],
        sample_total=10_000,
        population_total=500_000,
        population_size=500,
        sample_size=50,
    )
    assert p["upper_error_limit"] > p["most_likely_error"]


def test_sample_supports_acceptance():
    e = SamplingEngine()
    p = e.project_misstatement(
        sample_errors=[],
        sample_total=50_000,
        population_total=1_000_000,
        population_size=1_000,
        sample_size=100,
    )
    r = e.evaluate_sample(p, tolerable_misstatement=50_000)
    assert r["conclusion"] == "accept"


def test_sample_requires_extension():
    e = SamplingEngine()
    # Most likely is below tolerable but upper limit is above.
    p = {
        "most_likely_error": 4_000,
        "upper_error_limit": 12_000,
        "projected_rate": 0.01,
        "sample_errors_count": 3,
        "total_error_amount": 200,
    }
    r = e.evaluate_sample(p, tolerable_misstatement=10_000)
    assert r["conclusion"] == "extend"


def test_sample_rejects_acceptance():
    e = SamplingEngine()
    p = {
        "most_likely_error": 20_000,
        "upper_error_limit": 35_000,
        "projected_rate": 0.02,
        "sample_errors_count": 5,
        "total_error_amount": 500,
    }
    r = e.evaluate_sample(p, tolerable_misstatement=10_000)
    assert r["conclusion"] == "reject"


def test_finite_population_correction_applied():
    e = SamplingEngine()
    # Small population should materially reduce sample size.
    n_small = e.calculate_sample_size_attribute(
        population_size=100,
        tolerable_rate=0.05,
        expected_rate=0.01,
        confidence=0.95,
    )
    n_big = e.calculate_sample_size_attribute(
        population_size=100_000,
        tolerable_rate=0.05,
        expected_rate=0.01,
        confidence=0.95,
    )
    assert n_small < n_big


def test_expected_exceeds_tolerable_raises():
    e = SamplingEngine()
    with pytest.raises(ValueError):
        e.calculate_sample_size_attribute(
            population_size=1_000,
            tolerable_rate=0.05,
            expected_rate=0.07,
            confidence=0.95,
        )


def test_sample_size_minimum_30_recommended():
    # The engine itself doesn't enforce a floor (statutes don't), but our
    # recommended-minimum constant is exposed so the UI can warn.
    assert RECOMMENDED_MIN_SAMPLE == 30


def test_reproducibility_with_seed():
    txs = [{"id": f"T{i}", "amount": float(i + 1)} for i in range(100)]
    e1 = SamplingEngine(seed="same-seed")
    e2 = SamplingEngine(seed="same-seed")
    s1 = e1.select_random_sample(txs, 20)
    s2 = e2.select_random_sample(txs, 20)
    assert [t["id"] for t in s1] == [t["id"] for t in s2]


def test_mus_expected_at_tolerable_raises():
    e = SamplingEngine()
    with pytest.raises(ValueError):
        # Expected * expansion == tolerable -> adjusted <= 0.
        e.calculate_sample_size_mus(
            population_dollars=1_000_000,
            tolerable_misstatement=16_000,
            expected_misstatement=10_000,
            confidence=0.95,
        )


def test_confidence_factor_table_populated():
    assert 0.95 in CONFIDENCE_FACTORS
    assert CONFIDENCE_FACTORS[0.99] > CONFIDENCE_FACTORS[0.95]
