"""Score financial-engine outputs: balancing, precision, period assignment."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from ._base import ValidationResult, amount_close


class FinancialOracle:
    name = "financial"

    def validate(self, computed: dict[str, Any], expected: dict[str, Any]) -> ValidationResult:
        r = ValidationResult()
        computed = computed or {}
        score = 0.0
        total_checks = 0

        # Balance check
        if "balanced" in expected:
            total_checks += 1
            ok = bool(computed.get("balanced")) == bool(expected["balanced"])
            r.field_scores["balanced"] = 1.0 if ok else 0.0
            if ok:
                score += 1
            else:
                r.wrong_values.append({
                    "field": "balanced",
                    "expected": expected["balanced"],
                    "actual":   computed.get("balanced"),
                })

        # Delta precision
        if "delta" in expected:
            total_checks += 1
            ok = amount_close(computed.get("delta", 0), expected["delta"], tolerance=0.001)
            r.field_scores["delta"] = 1.0 if ok else 0.0
            score += 1 if ok else 0
            if not ok:
                r.wrong_values.append({
                    "field":    "delta",
                    "expected": expected["delta"],
                    "actual":   computed.get("delta"),
                })

        # Flag assertions (must-be-true booleans)
        for flag in (
            "rejected", "flagged", "translation_gain_loss_required",
            "proration_required", "reversal_posted", "opening_balance_changed",
            "consolidated_net", "deferred_tax_line_required", "lead_sheet_required",
            "mismatch_flagged", "new_ytd_starts_at_zero", "sum_matches_entity",
            "mutation_blocked", "auto_adjusted", "statement_built",
        ):
            if flag in expected:
                total_checks += 1
                exp_val = expected[flag]
                act_val = computed.get(flag)
                ok = (exp_val == act_val) if isinstance(exp_val, bool) else amount_close(act_val, exp_val)
                r.field_scores[flag] = 1.0 if ok else 0.0
                score += 1 if ok else 0
                if not ok:
                    r.wrong_values.append({"field": flag, "expected": exp_val, "actual": act_val})

        # Performance check — soft, records but does not fail
        if "performance_seconds_max" in expected:
            perf = computed.get("elapsed_seconds", 0)
            r.extra["performance_seconds"] = perf
            r.extra["performance_budget"]  = expected["performance_seconds_max"]

        r.total_score = (score / total_checks * 100.0) if total_checks else (100.0 if computed else 0.0)
        r.passed = bool(computed) and not r.wrong_values
        return r
