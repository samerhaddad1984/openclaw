"""Score a receipt extraction against ground truth."""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

from ._base import ValidationResult, amount_close, string_eq_norm


_DATE_TOKEN_RE = re.compile(r"(\d{1,4})[/\-.](\d{1,2})[/\-.](\d{1,4})")


def _to_ymd(raw: Any) -> tuple[int, int, int] | None:
    """Best-effort parse of common receipt date shapes into (year, month, day).

    Handles the formats we see from DocAI + SROIE ground truth:
      - "2018-12-25"           (ISO, DocAI default)
      - "25/12/2018 8:13:39 PM" (SROIE train set)
      - "12-01-19 21:13 SH01"   (SROIE noisy tail)
      - "2018/12/25"
    Two-digit years are assumed 2000+.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    m = _DATE_TOKEN_RE.search(s)
    if not m:
        return None
    a, b, c = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    # Decide YMD vs DMY vs MDY by looking at which component is four digits.
    if a > 31:  # YYYY-MM-DD
        year, month, day = a, b, c
    elif c > 31:  # DD/MM/YYYY or MM/DD/YYYY
        year = c
        if a > 12:  # first token > 12 → must be day
            month, day = b, a
        elif b > 12:  # second token > 12 → must be day
            month, day = a, b
        else:
            # Ambiguous (both ≤ 12). SROIE uses DD/MM/YYYY, so prefer that.
            month, day = b, a
    else:  # two-digit year at end
        year = 2000 + c if c < 100 else c
        if a > 12:
            month, day = b, a
        elif b > 12:
            month, day = a, b
        else:
            month, day = b, a
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    return year, month, day


def _dates_equivalent(actual: Any, expected: Any) -> bool:
    a = _to_ymd(actual)
    e = _to_ymd(expected)
    if a is None or e is None:
        return string_eq_norm(actual, expected)
    return a == e


# Field weights sum to 100
FIELD_WEIGHTS = {
    "vendor":        20.0,
    "document_date": 15.0,
    "total":         25.0,
    "subtotal":      5.0,
    "gst":           5.0,
    "qst":           5.0,
    "currency":      5.0,
    "tax_code":      5.0,
    "line_count":    5.0,
}


def _amount_close_scaled(actual: Any, expected: Any, *, tolerance: float = 0.05) -> bool:
    """Tolerance check that also accepts values expressed in different
    locale conventions. CORD stores Indonesian rupiah as ``60.000``
    (meaning 60,000 IDR); an OCR AI that treats ``.`` as a decimal
    separator returns ``60.0`` instead. When the two values differ only
    by a power-of-1000 scaling, treat them as equal — we are testing
    reading fidelity, not locale interpretation.
    """
    if amount_close(actual, expected, tolerance=tolerance):
        return True
    try:
        da = Decimal(str(actual))
        de = Decimal(str(expected))
    except (InvalidOperation, ValueError, TypeError):
        return False
    if de == 0:
        return da == 0
    # Percentage tolerance for large amounts
    rel = abs(da - de) / abs(de)
    if rel < Decimal("0.02"):
        return True
    # Power-of-1000 scaling mismatch (locale: thousands '.' vs decimal '.')
    for scale in (Decimal("1000"), Decimal("0.001"), Decimal("100"), Decimal("0.01")):
        if de == 0:
            continue
        scaled = da * scale
        if abs(scaled - de) / abs(de) < Decimal("0.02"):
            return True
    return False


class ReceiptOracle:
    name = "receipt"

    def validate(
        self,
        extracted: dict[str, Any],
        ground_truth: dict[str, Any],
        *,
        skip_fields: list[str] | None = None,
    ) -> ValidationResult:
        r = ValidationResult()
        extracted = extracted or {}
        skip = set(skip_fields or [])

        # When a field is skipped, redistribute its weight over the rest
        # so the remaining score still sums to 100. Otherwise a dataset
        # that legitimately lacks GST/QST would cap at ~90.
        active_weights = {f: w for f, w in FIELD_WEIGHTS.items() if f not in skip}
        total_w = sum(active_weights.values()) or 1.0
        norm = 100.0 / total_w

        score = 0.0
        for field, weight in FIELD_WEIGHTS.items():
            if field in skip:
                r.field_scores[field] = None  # not scored
                continue
            expected = ground_truth.get(field)
            actual = extracted.get(field)

            # If expected is absent (dataset-specific), skip
            if expected is None:
                r.field_scores[field] = None
                continue

            if actual is None:
                r.omissions.append(field)
                r.field_scores[field] = 0.0
                continue

            ok = False
            if field in ("total", "subtotal", "gst", "qst"):
                ok = _amount_close_scaled(actual, expected, tolerance=0.05)
            elif field == "document_date":
                ok = _dates_equivalent(actual, expected)
            elif field == "line_count":
                try:
                    ok = abs(int(actual) - int(expected)) <= max(1, int(expected) // 10)
                except Exception:
                    ok = False
            elif field == "vendor":
                # Logos survive partial damage → accept if actual is a
                # non-trivial prefix of expected (or vice versa).
                # Normalise by stripping punctuation/whitespace so the OCR
                # isn't penalised for "BOOK TA K" vs "BOOK TA .K" — the
                # characters read are the same, just tokenised differently.
                def _norm(s: str) -> str:
                    s = s.strip().lower()
                    return re.sub(r"[^a-z0-9]+", "", s)
                a = _norm(str(actual or ""))
                e = _norm(str(expected or ""))
                ok = bool(a) and bool(e) and (
                    a in e or e in a or len(a) >= max(4, len(e) // 2) and e.startswith(a)
                )
            else:
                ok = string_eq_norm(actual, expected)

            if ok:
                r.field_scores[field] = 1.0
                score += weight * norm
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
                     "low_confidence_flagged", "error", "raw_result", "reason"):
                continue
            if k not in expected_keys and k not in FIELD_WEIGHTS:
                r.hallucinations.append(k)

        r.total_score = score
        # Pass gate: 75 of available score. Omissions already cost their
        # weight, so we do not double-punish them with a hard gate.
        r.passed = score >= 75.0
        return r
