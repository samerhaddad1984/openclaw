"""Validate tax computations from the tax engine."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from ._base import ValidationResult, amount_close


class TaxOracle:
    name = "tax"

    def validate(self, computed: dict[str, Any], expected: dict[str, Any]) -> ValidationResult:
        r = ValidationResult()
        computed = computed or {}
        total_checks = 0
        hits = 0

        for key, exp_val in expected.items():
            total_checks += 1
            act_val = computed.get(key)
            if isinstance(exp_val, bool):
                ok = bool(act_val) == exp_val
            elif isinstance(exp_val, (int, float, Decimal)) or (
                isinstance(exp_val, str) and exp_val.replace(".", "", 1).replace("-", "", 1).isdigit()
            ):
                ok = amount_close(act_val, exp_val, tolerance=0.001)
            else:
                ok = str(act_val or "") == str(exp_val or "")
            r.field_scores[key] = 1.0 if ok else 0.0
            if ok:
                hits += 1
            else:
                r.wrong_values.append({"field": key, "expected": exp_val, "actual": act_val})

        r.total_score = (hits / total_checks * 100.0) if total_checks else 0.0
        r.passed = hits == total_checks and total_checks > 0
        return r
