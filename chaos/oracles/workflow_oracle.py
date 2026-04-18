"""Validate end-to-end workflow outputs."""
from __future__ import annotations

from typing import Any

from ._base import ValidationResult


class WorkflowOracle:
    name = "workflow"

    def validate(self, result: dict[str, Any], expected: dict[str, Any]) -> ValidationResult:
        r = ValidationResult()
        result = result or {}
        total_checks = 0
        hits = 0

        for key, exp_val in expected.items():
            total_checks += 1
            act_val = result.get(key)
            if isinstance(exp_val, bool):
                ok = bool(act_val) == exp_val
            elif isinstance(exp_val, (int, float)):
                ok = float(act_val or 0) >= float(exp_val)  # perf "under X" flips sign elsewhere
            else:
                ok = str(act_val) == str(exp_val)
            r.field_scores[key] = 1.0 if ok else 0.0
            if ok:
                hits += 1
            else:
                r.wrong_values.append({"field": key, "expected": exp_val, "actual": act_val})

        # Stage-by-stage diagnostics
        if "stages" in result:
            r.extra["stages"] = result["stages"]

        r.total_score = (hits / total_checks * 100.0) if total_checks else 0.0
        r.passed = hits == total_checks and total_checks > 0
        return r
