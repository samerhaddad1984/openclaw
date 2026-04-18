"""Score audit / fraud findings against the expected-findings list."""
from __future__ import annotations

from collections import Counter
from typing import Any

from ._base import ValidationResult


class AuditOracle:
    name = "audit"

    def validate(
        self,
        findings: list[dict[str, Any]],
        ground_truth: dict[str, Any],
    ) -> ValidationResult:
        r = ValidationResult()
        expected = ground_truth.get("expected_findings") or ground_truth.get("expected_rules_fired") or []
        if isinstance(expected, list) and expected and isinstance(expected[0], str):
            # fraud-style: list of rule names → convert to {type: count}
            expected_counts = Counter(expected)
        else:
            expected_counts = Counter()
            for e in expected:
                expected_counts[e["type"]] += int(e.get("count", 1))

        actual_counts: Counter = Counter()
        for f in findings or []:
            t = f.get("type") or f.get("rule") or f.get("category") or ""
            actual_counts[t] += 1

        true_positives = 0
        false_negatives = 0
        for rule, expected_count in expected_counts.items():
            got = actual_counts.get(rule, 0)
            matched = min(got, expected_count)
            true_positives += matched
            if got < expected_count:
                false_negatives += (expected_count - got)
                r.omissions.append(f"{rule}: expected {expected_count}, got {got}")

        false_positives = 0
        for rule, got in actual_counts.items():
            expected_count = expected_counts.get(rule, 0)
            if expected_count > 0 and got > expected_count:
                # Rule WAS expected — over-firing is OK (real engines emit
                # per-pair or per-match). Not a false positive.
                continue
            if got > expected_count:
                false_positives += (got - expected_count)
                r.hallucinations.append(f"{rule}: extra {got - expected_count}")

        total_expected = sum(expected_counts.values()) or 1
        recall = true_positives / total_expected
        precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) else 1.0
        # F-beta with beta=2 → recall weighted more (false negatives are critical)
        beta2 = 4.0
        if precision == 0 and recall == 0:
            f_score = 0.0
        else:
            f_score = (1 + beta2) * precision * recall / (beta2 * precision + recall + 1e-9)

        r.field_scores = {"precision": precision, "recall": recall, "f_beta2": f_score}
        r.extra = {
            "true_positives":  true_positives,
            "false_negatives": false_negatives,
            "false_positives": false_positives,
            "expected":        dict(expected_counts),
            "actual":          dict(actual_counts),
        }
        r.total_score = f_score * 100.0
        # Allow stochastic FPs from real engine rules (weekend/timing/amount
        # anomalies) on baseline samples: 2 FPs ok when expected is empty.
        fp_budget = 2 if sum(expected_counts.values()) == 0 else max(1, total_expected // 4)
        r.passed = false_negatives == 0 and false_positives <= fp_budget
        return r
