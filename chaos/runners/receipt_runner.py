"""Runner for receipt scenarios.

Two modes:

1. **Mock OCR (default)** — deterministic extractor that ingests the
   scenario's ground truth and then DEGRADES it according to the
   nightmare conditions (coffee stain → drop vendor, crumpled → fuzz
   amount ±5%, low-light → blank random fields, Arabic → zero output).
   This simulates the *shape* of typical OCR failures without calling
   the API. Fast, free, and lets us exercise the oracle + report.

2. **Real OCR (`--real-ocr`)** — invokes `src.engines.ocr_engine.process_file`
   against the (possibly AI-generated) image bytes. Costs real money on
   every run that isn't a cache hit.

Either way, the extracted dict is validated by `ReceiptOracle`.
"""
from __future__ import annotations

import logging
import random
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..oracles import get_oracle
from ._base import RunnerResult, safe_exec

log = logging.getLogger(__name__)


# Condition → degradation spec. Keep these deterministic so a given
# (scenario_id, seed) pair always produces the same mock output.
_DEGRADATIONS = {
    "coffee_stain_bottom_third": {"drop_fields": ["line_count"], "noise": 0.02},
    "crumpled":                  {"noise": 0.05},
    "thermal_fade_right":        {"drop_fields": ["qst"], "noise": 0.01},
    "thermal_fade_50pct":        {"drop_fields": ["vendor"], "noise": 0.10},
    "thermal_fade_middle_band":  {"noise": 0.03},
    "4_decimal_fuel_price":      {"noise": 0.02},
    "torn_corner":               {"drop_fields": ["line_count"]},
    "torn_bottom":               {"drop_fields": ["total", "gst", "qst"]},
    "rotated_45deg":             {"noise": 0.07},
    "perspective_distortion":    {"noise": 0.04},
    "brightness_minus_80pct":    {"drop_fields": ["qst", "line_count"], "noise": 0.08},
    "noise":                     {"noise": 0.03},
    "flash_glare_center":        {"drop_fields": ["total"], "noise": 0.10},
    "reflection":                {"noise": 0.02},
    "handwritten_overlay_tip":   {},
    "handwritten_gl_codes_overlay": {},
    "plastic_sleeve_reflection": {"noise": 0.04},
    "bilingual_labels":          {},
    "two_tax_lines":             {"drop_fields": ["qst"]},
    "split_across_photos":       {"drop_fields": ["line_count"]},
    "overlapping_cc_slip":       {"drop_fields": ["line_count"]},
    "motion_blur_horizontal":    {"noise": 0.06},
    "surface_glare":             {"noise": 0.03},
    "logo_watermark":            {"noise": 0.01},
    "dual_currency_display":     {"swap_currency": True},
    "promo_barcodes_noise":      {"noise": 0.01},
    "mixed_rx_otc":              {"drop_fields": ["gst", "qst"]},
    "service_charge_line":       {},
    "tip_line":                  {},
    "french_only_labels":        {},
    "dual_payment_display":      {"noise": 0.02},
    "numeric_item_codes_only":   {"drop_fields": ["line_count"]},
    "truncated_item_names":      {},
    "missing_vendor_header":     {"drop_fields": ["vendor"]},
    "platform_fees_breakdown":   {"noise": 0.02},
    "subscription_line":         {},
    "screenshot_pixel_perfect":  {},
    "negative_total":            {"flip_total_sign": True},
    "zero_total":                {"force_total_zero": True},
    "4_digit_cent_price":        {"noise": 0.01},
    "discount_makes_negative":   {"noise": 0.05},
    "date_in_future":            {},
    "date_10_years_old":         {},
    "identical_line_amounts":    {},
    "single_line_huge_amount":   {},
    "very_many_items":           {"drop_fields": ["line_count"]},
    "foreign_labels_spanish":    {"drop_fields": ["vendor"], "noise": 0.10},
    "foreign_labels_arabic":     {"drop_fields": ["vendor", "total", "gst", "qst"]},  # impossible
    "foreign_labels_chinese":    {"drop_fields": ["vendor", "gst", "qst"]},
    "rtl_text":                  {},
    "totals_only_no_lines":      {"drop_fields": ["line_count"]},
    "low_contrast":              {"noise": 0.04},
    "thermal_paper":             {},
}


def _degrade(gt: dict[str, Any], conditions: list[str], rnd: random.Random) -> dict[str, Any]:
    """Apply condition-driven degradation to produce a realistic mock extraction."""
    extracted = {
        "vendor":        gt.get("vendor"),
        "document_date": gt.get("document_date"),
        "total":         gt.get("total"),
        "gst":           gt.get("gst"),
        "qst":           gt.get("qst"),
        "currency":      gt.get("currency", "CAD"),
        "tax_code":      gt.get("tax_code"),
        "line_count":    gt.get("line_count"),
    }

    combined_noise = 0.0
    for c in conditions or []:
        deg = _DEGRADATIONS.get(c, {})
        combined_noise = max(combined_noise, float(deg.get("noise", 0.0)))
        for field in deg.get("drop_fields", []):
            extracted[field] = None
        if deg.get("swap_currency"):
            extracted["currency"] = "USD"  # misread
        if deg.get("flip_total_sign") and extracted.get("total"):
            try:
                extracted["total"] = str(-Decimal(str(extracted["total"])))
            except Exception:
                pass
        if deg.get("force_total_zero"):
            extracted["total"] = "0.00"
            extracted["gst"] = "0.00"
            extracted["qst"] = "0.00"

    # Apply per-field noise within oracle tolerance (~$0.05)
    if combined_noise > 0:
        for field in ("total", "gst", "qst"):
            val = extracted.get(field)
            if val is None:
                continue
            try:
                d = Decimal(str(val))
                jitter = Decimal(str(rnd.uniform(-combined_noise, combined_noise)))
                extracted[field] = str((d * (Decimal("1") + jitter)).quantize(Decimal("0.01")))
            except Exception:
                pass
    return extracted


class ReceiptRunner:
    track = "receipts"

    def __init__(self, *, image_generator, chaos_db_path: Path, real_ocr: bool = False,
                 seed: int = 1337):
        self.image_generator = image_generator
        self.chaos_db_path = chaos_db_path
        self.real_ocr = real_ocr
        self.seed = seed

    def run(self, scenario: dict[str, Any]) -> RunnerResult:
        return safe_exec(scenario, self._run)

    def _run(self, scenario: dict[str, Any], result: RunnerResult) -> None:
        spec = scenario.get("input_spec") or {}
        gt = scenario.get("ground_truth") or {}
        conditions = spec.get("conditions") or []
        rnd = random.Random(self.seed + (abs(hash(scenario.get("id", ""))) % 10_000_000))

        if self.real_ocr:
            # Generate image + hit real pipeline (costs money)
            image_path, _ = self.image_generator.generate(scenario)
            try:
                from src.engines.ocr_engine import process_file  # type: ignore
                file_bytes = image_path.read_bytes() if image_path.exists() else b""
                if not file_bytes:
                    extracted = {"ok": False, "reason": "empty_image"}
                    calls = []
                else:
                    processed = process_file(
                        file_bytes=file_bytes, filename=image_path.name,
                        client_code="CHAOS", ingest_source="chaos",
                        db_path=self.chaos_db_path,
                        upload_dir=self.chaos_db_path.parent / "uploads",
                    ) or {}
                    extracted = {
                        "vendor":        processed.get("vendor"),
                        "document_date": processed.get("document_date"),
                        "total":         processed.get("amount"),
                        "gst":           processed.get("gst"),
                        "qst":           processed.get("qst"),
                        "currency":      processed.get("currency"),
                        "tax_code":      processed.get("tax_code"),
                        "line_count":    processed.get("line_count") or len(processed.get("line_items") or []),
                    }
                    calls = ["ocr_engine.process_file"]
            except Exception as e:
                extracted = {"ok": False, "error": f"{type(e).__name__}: {e}"}
                calls = []
        else:
            # Realistic deterministic mock — degrade GT by conditions
            extracted = _degrade(gt, conditions, rnd)
            calls = ["mock_degradation"]

        oracle = get_oracle("receipt")
        oracle_result = oracle.validate(extracted, gt)

        result.output = {"extracted": extracted, "conditions": conditions,
                         "functions_called": calls,
                         "mode": "real_ocr" if self.real_ocr else "mock"}
        result.oracle_result = oracle_result.to_dict()
        result.score = oracle_result.total_score
        result.passed = oracle_result.passed
