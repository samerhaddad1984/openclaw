"""Score a receipt extraction against ground truth."""
from __future__ import annotations

from typing import Any

from ._base import ValidationResult, amount_close, string_eq_norm


# Field weights sum to 100
FIELD_WEIGHTS = {
    "vendor":        20.0,
    "document_date": 15.0,
    "total":         25.0,
    "gst":           10.0,
    "qst":           10.0,
    "currency":      5.0,
    "tax_code":      10.0,
    "line_count":    5.0,
}


class ReceiptOracle:
    name = "receipt"

    def validate(self, extracted: dict[str, Any], ground_truth: dict[str, Any]) -> ValidationResult:
        r = ValidationResult()
        extracted = extracted or {}

        score = 0.0
        for field, weight in FIELD_WEIGHTS.items():
            expected = ground_truth.get(field)
            actual = extracted.get(field)

            if actual is None:
                r.omissions.append(field)
                r.field_scores[field] = 0.0
                continue

            ok = False
            if field in ("total", "gst", "qst"):
                ok = amount_close(actual, expected, tolerance=0.05)
            elif field == "line_count":
                try:
                    ok = abs(int(actual) - int(expected)) <= max(1, int(expected) // 10)
                except Exception:
                    ok = False
            elif field == "vendor":
                # Logos survive partial damage → accept if actual is a
                # non-trivial prefix of expected (or vice versa).
                a = str(actual or "").strip().lower()
                e = str(expected or "").strip().lower()
                ok = bool(a) and bool(e) and (a in e or e in a or len(a) >= max(4, len(e) // 2) and e.startswith(a))
            else:
                ok = string_eq_norm(actual, expected)

            if ok:
                r.field_scores[field] = 1.0
                score += weight
            else:
                r.field_scores[field] = 0.0
                r.wrong_values.append({
                    "field":    field,
                    "expected": str(expected),
                    "actual":   str(actual),
                })

        # Hallucinations: extra keys that look like fields we didn't ask for
        expected_keys = set(ground_truth.keys())
        for k in extracted.keys():
            if k in ("ok", "document_id", "file_name", "file_path", "format",
                     "extraction_method", "doc_type", "confidence", "review_status",
                     "low_confidence_flagged", "error", "raw_result"):
                continue
            if k not in expected_keys and k not in FIELD_WEIGHTS:
                r.hallucinations.append(k)

        r.total_score = score
        r.passed = score >= 75.0 and not r.omissions
        return r
