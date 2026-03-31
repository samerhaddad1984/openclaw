"""
Benchmark simulated OCR accuracy against ground-truth messy images.

Since we cannot make real API calls without credentials, this script
simulates OCR extraction with realistic accuracy based on distortion type,
then runs the results through hallucination_guard.verify_ai_output() and
_post_process_handwriting() from the OCR engine.
"""

from __future__ import annotations

import json
import os
import random
import sys
from pathlib import Path
from typing import Any

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.agents.core.hallucination_guard import verify_ai_output
from src.engines.ocr_engine import _post_process_handwriting

IMAGE_DIR = ROOT / "data" / "training" / "messy_images"
REPORT_PATH = ROOT / "data" / "training" / "ocr_benchmark_report.txt"

random.seed(99)

# ---------------------------------------------------------------------------
# Accuracy profiles by distortion type / severity
# ---------------------------------------------------------------------------

ACCURACY_MAP: dict[tuple[str, str], float] = {
    ("thermal_fading", "mild"):  0.85,
    ("thermal_fading", "severe"): 0.65,
    ("bad_lighting", "mild"):    0.80,
    ("bad_lighting", "severe"):  0.60,
    ("crumpled", "mild"):        0.75,
    ("crumpled", "severe"):      0.55,
    ("handwritten", "mild"):     0.70,
    ("handwritten", "severe"):   0.70,
}


def _severity_bucket(severity: float) -> str:
    return "mild" if severity < 0.5 else "severe"


# ---------------------------------------------------------------------------
# Simulate noisy extraction
# ---------------------------------------------------------------------------

def _maybe_corrupt_vendor(vendor: str, accuracy: float) -> str:
    """Truncate or garble vendor name based on accuracy."""
    if random.random() > accuracy:
        mode = random.choice(["truncate", "garble", "empty"])
        if mode == "truncate":
            cut = max(2, len(vendor) // 2)
            return vendor[:cut]
        elif mode == "garble":
            chars = list(vendor)
            for i in range(len(chars)):
                if random.random() < 0.3:
                    chars[i] = random.choice("abcdefghijklmnopqrstuvwxyz")
            return "".join(chars)
        else:
            return ""
    return vendor


def _maybe_corrupt_amount(val: float, accuracy: float) -> float | None:
    """Swap digits or return None."""
    if random.random() > accuracy:
        mode = random.choice(["swap_digit", "none", "off_by"])
        if mode == "swap_digit":
            s = f"{val:.2f}"
            digits = [i for i, c in enumerate(s) if c.isdigit()]
            if digits:
                idx = random.choice(digits)
                new_digit = str(random.randint(0, 9))
                s = s[:idx] + new_digit + s[idx + 1:]
                try:
                    return float(s)
                except ValueError:
                    return val
            return val
        elif mode == "none":
            return None
        else:
            return round(val + random.uniform(-5, 5), 2)
    return val


def _maybe_corrupt_date(date_str: str, accuracy: float) -> str | None:
    """Occasionally return wrong date or None."""
    if random.random() > accuracy:
        mode = random.choice(["wrong_day", "none", "bad_format"])
        if mode == "wrong_day":
            parts = date_str.split("-")
            if len(parts) == 3:
                day = max(1, min(28, int(parts[2]) + random.randint(-5, 5)))
                return f"{parts[0]}-{parts[1]}-{day:02d}"
            return date_str
        elif mode == "none":
            return None
        else:
            return date_str.replace("-", "/")
    return date_str


def simulate_extraction(gt: dict[str, Any]) -> dict[str, Any]:
    """Simulate an OCR extraction with realistic noise for the given ground truth."""
    dtype = gt["distortion_type"]
    sev_bucket = _severity_bucket(gt["severity"])
    accuracy = ACCURACY_MAP.get((dtype, sev_bucket), 0.70)

    vendor = _maybe_corrupt_vendor(gt["vendor"], accuracy)
    amount = _maybe_corrupt_amount(gt["amount"], accuracy)
    date = _maybe_corrupt_date(gt["date"], accuracy)
    gst = _maybe_corrupt_amount(gt.get("gst", 0.0), accuracy)
    qst = _maybe_corrupt_amount(gt.get("qst", 0.0), accuracy)

    # Build a result dict matching the schema expected by the guards
    confidence = round(accuracy * random.uniform(0.7, 1.1), 4)
    confidence = min(1.0, max(0.0, confidence))

    extracted = {
        "vendor_name": vendor,
        "vendor": vendor,
        "amount": amount,
        "total": amount,
        "document_date": date,
        "date": date,
        "gst_amount": gst,
        "qst_amount": qst,
        "subtotal": round((amount or 0) - (gst or 0) - (qst or 0), 2) if amount else None,
        "confidence": confidence,
        "doc_type": "receipt",
    }
    return extracted


# ---------------------------------------------------------------------------
# Field comparison
# ---------------------------------------------------------------------------

def _field_match(extracted_val: Any, truth_val: Any, field: str) -> bool:
    """Return True if extracted value matches ground truth (with tolerance)."""
    if extracted_val is None and truth_val is None:
        return True
    if extracted_val is None or truth_val is None:
        return False

    if field in ("amount", "gst", "qst"):
        try:
            return abs(float(extracted_val) - float(truth_val)) < 0.015
        except (TypeError, ValueError):
            return False
    elif field == "date":
        return str(extracted_val).strip() == str(truth_val).strip()
    elif field == "vendor":
        return str(extracted_val).strip().lower() == str(truth_val).strip().lower()
    return str(extracted_val) == str(truth_val)


# ---------------------------------------------------------------------------
# Main benchmark
# ---------------------------------------------------------------------------

def load_ground_truths() -> list[dict[str, Any]]:
    """Load all JSON sidecar files from the image directory."""
    gts = []
    for f in sorted(IMAGE_DIR.glob("*.json")):
        gts.append(json.loads(f.read_text(encoding="utf-8")))
    return gts


def run_benchmark() -> str:
    """Run the benchmark and return the report text."""
    ground_truths = load_ground_truths()
    if not ground_truths:
        return "ERROR: No ground truth files found. Run generate_messy_images.py first."

    fields = ["vendor", "amount", "date", "gst", "qst"]

    # Accumulators
    total_correct: dict[str, int] = {f: 0 for f in fields}
    total_count: dict[str, int] = {f: 0 for f in fields}
    by_distortion: dict[str, dict[str, list[bool]]] = {}
    handwriting_triggered = 0
    needs_review_count = 0
    hallucination_count = 0

    results_detail: list[dict] = []

    for gt in ground_truths:
        extracted = simulate_extraction(gt)
        dtype = gt["distortion_type"]

        # --- Run through hallucination guard ---
        guard_result = verify_ai_output(extracted)

        # --- Run through handwriting post-processing ---
        hw_result = _post_process_handwriting(dict(extracted))

        if hw_result.get("handwriting_low_confidence"):
            handwriting_triggered += 1

        if guard_result.get("review_status") == "NeedsReview" or hw_result.get("review_status") == "NeedsReview":
            needs_review_count += 1

        if guard_result.get("hallucination_suspected"):
            hallucination_count += 1

        # --- Compare fields ---
        field_map = {
            "vendor": ("vendor_name", "vendor"),
            "amount": ("total", "amount"),
            "date": ("document_date", "date"),
            "gst": ("gst_amount", "gst"),
            "qst": ("qst_amount", "qst"),
        }

        if dtype not in by_distortion:
            by_distortion[dtype] = {f: [] for f in fields}

        for field in fields:
            ext_key, gt_key = field_map[field]
            ext_val = hw_result.get(ext_key) if ext_key in hw_result else extracted.get(ext_key)
            gt_val = gt.get(gt_key)
            match = _field_match(ext_val, gt_val, field)
            total_count[field] += 1
            if match:
                total_correct[field] += 1
            by_distortion[dtype][field].append(match)

    # --- Build report ---
    lines: list[str] = []
    lines.append("=" * 70)
    lines.append("  OtoCPA OCR Benchmark Report")
    lines.append("=" * 70)
    lines.append(f"\nTotal images evaluated: {len(ground_truths)}")
    lines.append("")

    # Overall accuracy
    lines.append("OVERALL ACCURACY PER FIELD")
    lines.append("-" * 40)
    for f in fields:
        pct = total_correct[f] / total_count[f] * 100 if total_count[f] else 0
        lines.append(f"  {f:>8s}: {pct:5.1f}%  ({total_correct[f]}/{total_count[f]})")

    overall = sum(total_correct.values()) / max(1, sum(total_count.values())) * 100
    lines.append(f"\n  {'OVERALL':>8s}: {overall:5.1f}%")

    # Accuracy by distortion type
    lines.append("\n\nACCURACY BY DISTORTION TYPE")
    lines.append("-" * 40)
    for dtype in sorted(by_distortion.keys()):
        lines.append(f"\n  [{dtype}]")
        for f in fields:
            matches = by_distortion[dtype][f]
            pct = sum(matches) / len(matches) * 100 if matches else 0
            lines.append(f"    {f:>8s}: {pct:5.1f}%  ({sum(matches)}/{len(matches)})")

    # Summary stats
    lines.append("\n\nSUMMARY STATISTICS")
    lines.append("-" * 40)
    lines.append(f"  Handwriting low-confidence triggered: {handwriting_triggered}/{len(ground_truths)}")
    lines.append(f"  NeedsReview flagged:                  {needs_review_count}/{len(ground_truths)}")
    lines.append(f"  Hallucination suspected:              {hallucination_count}/{len(ground_truths)}")
    lines.append("\n" + "=" * 70)

    return "\n".join(lines)


if __name__ == "__main__":
    report = run_benchmark()
    print(report)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report, encoding="utf-8")
    print(f"\nReport saved to {REPORT_PATH}")
