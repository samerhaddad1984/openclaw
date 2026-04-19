"""
src/engines/sampling_engine.py - CAS 530 compliant statistical sampling.

Implements:
  - Attribute sampling size formula (control tests)
  - Monetary Unit Sampling (MUS) size + cumulative dollar selection
  - Simple random sampling
  - Projection of sample errors to the population (most-likely + upper limit)
  - Evaluate sample outcome against tolerable misstatement

References:
  - CAS 530 Audit Sampling (paragraphs 4, 7, A4-A24)
  - AICPA Audit Sampling Guide, Appendix B (Poisson upper-limit factors)

Design notes:
  * Pure math / data-structure module. No I/O, no DB writes. Caller owns
    persistence. This keeps the engine unit-testable without fixtures.
  * Uses `random.Random(seed)` when a seed is provided, so samples can be
    reproduced and defended in working papers.
"""
from __future__ import annotations

import math
import random
from typing import Any, Iterable


# Two-sided z-factors commonly published in sampling tables. CAS 530 does not
# dictate specific values, but these align with AICPA and IAASB guidance.
CONFIDENCE_FACTORS: dict[float, float] = {
    0.99: 4.61,
    0.95: 3.00,
    0.90: 2.31,
    0.85: 1.90,
    0.80: 1.61,
    0.75: 1.39,
    0.70: 1.20,
    0.50: 0.70,
}

# Poisson-based upper-error-limit factors at 95% confidence, indexed by number
# of sample errors observed. AICPA Sampling Guide Appendix B (abridged).
UL_FACTORS_95 = [3.00, 4.75, 6.30, 7.76, 9.16, 10.52, 11.85, 13.15, 14.44, 15.71]
UL_FACTORS_99 = [4.61, 6.64, 8.41, 10.05, 11.61, 13.11, 14.57, 16.00, 17.41, 18.79]
UL_FACTORS_90 = [2.31, 3.89, 5.33, 6.69, 8.00, 9.28, 10.54, 11.78, 13.00, 14.21]

# Expansion factor applied to expected misstatement in MUS size formula.
# AICPA Sampling Guide; 1.6 is the commonly published value at 95%.
MUS_EXPANSION_FACTOR = 1.6

# Minimum practical sample size. CAS 530 does not mandate a floor, but
# practitioner guidance warns that very small samples produce unreliable
# projections; we surface a conservative floor so the UI can warn.
RECOMMENDED_MIN_SAMPLE = 30


class SamplingEngine:
    """CAS 530 compliant statistical sampling."""

    def __init__(self, seed: str | int | None = None) -> None:
        self._rng = random.Random(seed) if seed is not None else random.Random()

    # ------------------------------------------------------------------
    # Attribute sampling (control tests)
    # ------------------------------------------------------------------
    def calculate_sample_size_attribute(
        self,
        population_size: int,
        tolerable_rate: float,
        expected_rate: float,
        confidence: float = 0.95,
    ) -> int:
        """Sample size for attribute sampling (control deviation tests).

        n = z^2 * p(1-p) / (tolerable - expected)^2 with finite population
        correction. expected_rate must be strictly < tolerable_rate.
        """
        if confidence not in CONFIDENCE_FACTORS:
            raise ValueError(f"Unsupported confidence: {confidence}")
        if not 0 <= expected_rate < 1:
            raise ValueError("expected_rate must be in [0, 1)")
        if not 0 < tolerable_rate <= 1:
            raise ValueError("tolerable_rate must be in (0, 1]")
        if expected_rate >= tolerable_rate:
            raise ValueError(
                "Expected deviation rate is not less than tolerable rate; "
                "sampling is not appropriate.",
            )
        z = CONFIDENCE_FACTORS[confidence]
        # If expected is 0 the formula collapses; use z^2 / tolerable_rate as
        # the conservative zero-expected form found in audit guidance tables.
        if expected_rate == 0:
            n = (z * z) / tolerable_rate
        else:
            n = (z * z * expected_rate * (1 - expected_rate)) / (
                (tolerable_rate - expected_rate) ** 2
            )
        if population_size > 0:
            n = n / (1 + (n - 1) / population_size)
        return int(math.ceil(n))

    # ------------------------------------------------------------------
    # Monetary Unit Sampling (substantive tests of balances)
    # ------------------------------------------------------------------
    def calculate_sample_size_mus(
        self,
        population_dollars: float,
        tolerable_misstatement: float,
        expected_misstatement: float = 0.0,
        confidence: float = 0.95,
    ) -> int:
        """MUS sample size:
            n = ceil(population $ * confidence factor
                    / (tolerable - expected * expansion_factor))
        """
        if population_dollars <= 0:
            raise ValueError("population_dollars must be positive")
        if tolerable_misstatement <= 0:
            raise ValueError("tolerable_misstatement must be positive")
        if expected_misstatement < 0:
            raise ValueError("expected_misstatement must be >= 0")
        if confidence not in CONFIDENCE_FACTORS:
            raise ValueError(f"Unsupported confidence: {confidence}")
        cf = CONFIDENCE_FACTORS[confidence]
        adjusted = tolerable_misstatement - expected_misstatement * MUS_EXPANSION_FACTOR
        if adjusted <= 0:
            raise ValueError(
                "Tolerable misstatement is too low relative to expected; "
                "MUS is not appropriate.",
            )
        n = (population_dollars * cf) / adjusted
        return int(math.ceil(n))

    def select_mus_sample(
        self,
        transactions: Iterable[dict[str, Any]],
        sample_size: int,
        population_total: float | None = None,
    ) -> list[dict[str, Any]]:
        """Monetary Unit Sampling: systematic probability-proportional-to-size
        selection. Each dollar in the population has equal chance of selection,
        which means large transactions are likely to be selected (or selected
        multiple times, which we dedupe).

        Transactions must expose an 'amount' and an 'id' (or 'document_id')
        key. Absolute value is used so credits don't interfere with dollar
        accumulation. Items larger than the sampling interval are guaranteed
        to be picked.
        """
        txs = [t for t in transactions if abs(float(t.get("amount") or 0)) > 0]
        if not txs:
            return []
        if sample_size <= 0:
            return []
        total = (
            population_total
            if population_total is not None
            else sum(abs(float(t.get("amount") or 0)) for t in txs)
        )
        if total <= 0:
            return []
        if sample_size >= len(txs):
            return list(txs)

        interval = total / sample_size
        random_start = self._rng.uniform(0, interval)

        def _id(tx: dict[str, Any]) -> str:
            return str(tx.get("id") or tx.get("document_id") or tx.get("posting_id") or "")

        ordered = sorted(txs, key=_id)
        selected: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        cumulative = 0.0
        next_hit = random_start

        for tx in ordered:
            amount = abs(float(tx.get("amount") or 0))
            cumulative += amount
            while cumulative >= next_hit and len(selected) < sample_size:
                tid = _id(tx)
                if tid not in seen_ids:
                    selected.append(tx)
                    seen_ids.add(tid)
                next_hit += interval
            if len(selected) >= sample_size:
                break
        return selected

    # ------------------------------------------------------------------
    # Simple random sampling
    # ------------------------------------------------------------------
    def select_random_sample(
        self,
        items: list[dict[str, Any]],
        n: int,
    ) -> list[dict[str, Any]]:
        if n >= len(items):
            return list(items)
        if n <= 0:
            return []
        return self._rng.sample(items, n)

    # ------------------------------------------------------------------
    # Projection & evaluation
    # ------------------------------------------------------------------
    def project_misstatement(
        self,
        sample_errors: list[dict[str, Any]],
        sample_total: float,
        population_total: float,
        population_size: int,
        sample_size: int,
        confidence: float = 0.95,
    ) -> dict[str, float]:
        """Project observed sample errors to the population.

        Most-likely-error = (error $ / sample $) * population $.
        Upper-error-limit uses a Poisson-based incremental factor so the
        limit widens as error count grows (CAS 530.A23).
        """
        total_error_amount = sum(float(e.get("error_amount") or 0) for e in sample_errors)
        if sample_total <= 0 or population_total <= 0 or sample_size <= 0:
            return {
                "most_likely_error": 0.0,
                "upper_error_limit": 0.0,
                "projected_rate": 0.0,
                "sample_errors_count": len(sample_errors),
                "total_error_amount": round(total_error_amount, 2),
            }

        error_rate = total_error_amount / sample_total
        most_likely = error_rate * population_total

        n_errors = len(sample_errors)
        ul_factor = self._ul_factor(n_errors, confidence)
        # Upper limit = (factor / sample_size) * population_total — this is
        # the Poisson upper-bound approximation used in MUS evaluation.
        upper_limit = (ul_factor / max(sample_size, 1)) * population_total

        return {
            "most_likely_error": round(most_likely, 2),
            "upper_error_limit": round(upper_limit, 2),
            "projected_rate": round(error_rate, 4),
            "sample_errors_count": n_errors,
            "total_error_amount": round(total_error_amount, 2),
        }

    def _ul_factor(self, n_errors: int, confidence: float) -> float:
        if confidence >= 0.99:
            table = UL_FACTORS_99
        elif confidence >= 0.95:
            table = UL_FACTORS_95
        else:
            table = UL_FACTORS_90
        if n_errors < len(table):
            return table[n_errors]
        # Extend linearly with ~1.3 incremental per additional error; matches
        # the asymptotic behaviour of the Poisson table.
        return table[-1] + (n_errors - len(table) + 1) * 1.3

    # ------------------------------------------------------------------
    # Outcome evaluation
    # ------------------------------------------------------------------
    def evaluate_sample(
        self,
        projection: dict[str, float],
        tolerable_misstatement: float,
    ) -> dict[str, str]:
        """Return a conclusion keyed by CAS 530.A22-A23 decision logic."""
        upper = float(projection.get("upper_error_limit") or 0)
        most_likely = float(projection.get("most_likely_error") or 0)
        if upper < tolerable_misstatement:
            return {
                "conclusion": "accept",
                "message": (
                    "Upper error limit is below tolerable misstatement; the "
                    "sample supports the recorded balance."
                ),
            }
        if most_likely < tolerable_misstatement:
            return {
                "conclusion": "extend",
                "message": (
                    "Most-likely error is below tolerable but the upper limit "
                    "is not; extend the sample or perform additional procedures."
                ),
            }
        return {
            "conclusion": "reject",
            "message": (
                "Projected misstatement equals or exceeds tolerable; material "
                "misstatement is likely. Propose adjustment or qualify opinion."
            ),
        }
