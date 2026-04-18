"""Common result structures for all oracles."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ValidationResult:
    total_score: float = 0.0
    passed: bool = False
    field_scores: dict[str, float] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)
    hallucinations: list[str] = field(default_factory=list)
    omissions: list[str] = field(default_factory=list)
    wrong_values: list[dict[str, Any]] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_score":    round(self.total_score, 4),
            "passed":         self.passed,
            "field_scores":   {k: round(v, 4) for k, v in self.field_scores.items()},
            "errors":         self.errors,
            "hallucinations": self.hallucinations,
            "omissions":      self.omissions,
            "wrong_values":   self.wrong_values,
            "extra":          self.extra,
        }


def amount_close(a: Any, b: Any, *, tolerance: float = 0.01) -> bool:
    try:
        return abs(float(a) - float(b)) <= tolerance
    except (TypeError, ValueError):
        return False


def string_eq_norm(a: Any, b: Any) -> bool:
    return str(a or "").strip().lower() == str(b or "").strip().lower()
