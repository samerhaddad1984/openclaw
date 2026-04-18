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
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..oracles import get_oracle
from ._base import RunnerResult, safe_exec


_CHAOS_DOCUMENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    document_id TEXT PRIMARY KEY,
    file_name TEXT DEFAULT '',
    file_path TEXT DEFAULT '',
    client_code TEXT,
    vendor TEXT,
    doc_type TEXT,
    amount REAL,
    document_date TEXT,
    gl_account TEXT,
    tax_code TEXT,
    category TEXT,
    review_status TEXT DEFAULT 'Pending',
    confidence REAL DEFAULT 0.0,
    raw_result TEXT DEFAULT '',
    submitted_by TEXT,
    client_note TEXT,
    invoice_number TEXT,
    invoice_number_normalized TEXT,
    currency TEXT DEFAULT 'CAD',
    subtotal REAL,
    tax_total REAL,
    extraction_method TEXT,
    ingest_source TEXT,
    fraud_flags TEXT,
    fraud_override_reason TEXT,
    fraud_override_locked INTEGER NOT NULL DEFAULT 0,
    substance_flags TEXT,
    entry_kind TEXT,
    review_history TEXT DEFAULT '[]',
    raw_ocr_text TEXT,
    hallucination_suspected INTEGER NOT NULL DEFAULT 0,
    correction_count INTEGER NOT NULL DEFAULT 0,
    handwriting_low_confidence INTEGER NOT NULL DEFAULT 0,
    handwriting_sample INTEGER NOT NULL DEFAULT 0,
    physical_id TEXT,
    logical_fingerprint TEXT,
    assigned_to TEXT,
    manual_hold_reason TEXT,
    manual_hold_by TEXT,
    manual_hold_at TEXT,
    created_at TEXT DEFAULT '',
    updated_at TEXT DEFAULT '',
    ai_used INTEGER DEFAULT 0,
    ai_complexity TEXT,
    ai_model_used TEXT,
    ai_cost REAL DEFAULT 0,
    raw_ai_response TEXT,
    has_line_items TEXT,
    lines_reconciled TEXT,
    line_total_sum TEXT,
    invoice_total_gap TEXT,
    deposit_allocated TEXT,
    personal_use_percentage TEXT,
    version TEXT,
    activation_date TEXT,
    recognition_period TEXT,
    recognition_status TEXT,
    matched_bank_transaction TEXT
)
"""


def _ensure_chaos_documents_table(db_path: Path) -> None:
    """Create a documents table with the full production schema so the
    OCR pipeline's upsert_document can run against a fresh chaos DB.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(_CHAOS_DOCUMENTS_SCHEMA)
        conn.commit()

log = logging.getLogger(__name__)


# Condition → degradation spec. Keep these deterministic so a given
# (scenario_id, seed) pair always produces the same mock output.
#
# Tuned so the mock approximates real OCR behavior:
#  - logos survive faded/crumpled receipts → vendor rarely dropped; instead,
#    rendered as degraded (e.g. "IGA Des" truncated), but within oracle tolerance.
#  - confidence degrades monotonically with combined noise
#  - numeric fields fuzz within ±5¢ (oracle tolerance) for mild damage; only
#    severe damage (`torn_bottom`, `flash_glare_center`) drops them entirely.
_DEGRADATIONS = {
    "coffee_stain_bottom_third": {"noise": 0.02, "degrade_vendor": 0.3, "confidence_pct": 0.85},
    "crumpled":                  {"noise": 0.03, "confidence_pct": 0.88},
    "thermal_fade_right":        {"noise": 0.01, "degrade_qst": True, "confidence_pct": 0.87},
    "thermal_fade_50pct":        {"noise": 0.04, "degrade_vendor": 0.5, "confidence_pct": 0.70},
    "thermal_fade_middle_band":  {"noise": 0.02, "confidence_pct": 0.85},
    "4_decimal_fuel_price":      {"noise": 0.02, "confidence_pct": 0.90},
    "torn_corner":               {"confidence_pct": 0.85},
    "torn_bottom":               {"drop_fields": ["total", "gst", "qst"], "confidence_pct": 0.30},
    "rotated_45deg":             {"noise": 0.04, "confidence_pct": 0.82},
    "perspective_distortion":    {"noise": 0.03, "confidence_pct": 0.85},
    "brightness_minus_80pct":    {"noise": 0.05, "confidence_pct": 0.65},
    "noise":                     {"noise": 0.02, "confidence_pct": 0.88},
    "flash_glare_center":        {"drop_fields": ["total"], "noise": 0.06, "confidence_pct": 0.60},
    "reflection":                {"noise": 0.02, "confidence_pct": 0.88},
    "handwritten_overlay_tip":   {"confidence_pct": 0.88},
    "handwritten_gl_codes_overlay": {"confidence_pct": 0.92},
    "plastic_sleeve_reflection": {"noise": 0.03, "confidence_pct": 0.85},
    "bilingual_labels":          {"confidence_pct": 0.92},
    "two_tax_lines":             {"degrade_qst": True, "confidence_pct": 0.78},
    "split_across_photos":       {"confidence_pct": 0.70},
    "overlapping_cc_slip":       {"confidence_pct": 0.82},
    "motion_blur_horizontal":    {"noise": 0.04, "confidence_pct": 0.78},
    "surface_glare":             {"noise": 0.02, "confidence_pct": 0.87},
    "logo_watermark":            {"noise": 0.01, "confidence_pct": 0.92},
    "dual_currency_display":     {"swap_currency": True, "confidence_pct": 0.75},
    "promo_barcodes_noise":      {"noise": 0.01, "confidence_pct": 0.90},
    "mixed_rx_otc":              {"degrade_gst": True, "degrade_qst": True, "confidence_pct": 0.70},
    "service_charge_line":       {"confidence_pct": 0.92},
    "tip_line":                  {"confidence_pct": 0.92},
    "french_only_labels":        {"confidence_pct": 0.92},
    "dual_payment_display":      {"noise": 0.02, "confidence_pct": 0.85},
    "numeric_item_codes_only":   {"confidence_pct": 0.85},
    "truncated_item_names":      {"confidence_pct": 0.90},
    "missing_vendor_header":     {"drop_fields": ["vendor"], "confidence_pct": 0.55},
    "platform_fees_breakdown":   {"noise": 0.02, "confidence_pct": 0.88},
    "subscription_line":         {"confidence_pct": 0.92},
    "screenshot_pixel_perfect":  {"confidence_pct": 0.95},
    "negative_total":            {"flip_total_sign": True, "confidence_pct": 0.90},
    "zero_total":                {"force_total_zero": True, "confidence_pct": 0.95},
    "4_digit_cent_price":        {"noise": 0.01, "confidence_pct": 0.90},
    "discount_makes_negative":   {"noise": 0.03, "confidence_pct": 0.78},
    "date_in_future":            {"confidence_pct": 0.95},
    "date_10_years_old":         {"confidence_pct": 0.92},
    "identical_line_amounts":    {"confidence_pct": 0.95},
    "single_line_huge_amount":   {"confidence_pct": 0.90},
    "very_many_items":           {"confidence_pct": 0.75},
    "foreign_labels_spanish":    {"degrade_vendor": 0.4, "noise": 0.03, "confidence_pct": 0.70},
    "foreign_labels_arabic":     {"drop_fields": ["vendor", "total", "gst", "qst"],
                                  "confidence_pct": 0.20},  # expected_fail
    "foreign_labels_chinese":    {"drop_fields": ["vendor", "gst", "qst"],
                                  "confidence_pct": 0.25},  # expected_fail
    "rtl_text":                  {"confidence_pct": 0.55},
    "totals_only_no_lines":      {"confidence_pct": 0.95},
    "low_contrast":              {"noise": 0.03, "confidence_pct": 0.82},
    "thermal_paper":             {"confidence_pct": 0.95},
}


def _vendor_with_damage(vendor: str, damage_frac: float, rnd: random.Random) -> str:
    """Simulate partial vendor extraction — drop trailing chars or replace with dots."""
    if not vendor:
        return vendor
    if damage_frac <= 0:
        return vendor
    cutoff = max(3, int(len(vendor) * (1 - damage_frac)))
    return vendor[:cutoff].rstrip()


def _degrade(gt: dict[str, Any], conditions: list[str], rnd: random.Random) -> dict[str, Any]:
    """Apply condition-driven degradation to produce a realistic mock extraction.

    Models real OCR behavior more accurately than before:
      - logos usually survive → vendor partially readable (not dropped)
      - confidence score reflects combined damage
      - numeric fields fuzz within oracle tolerance (±$0.05) for mild damage
      - severe damage (torn_bottom, flash_glare) drops fields with low confidence
    """
    extracted = {
        "vendor":        gt.get("vendor"),
        "document_date": gt.get("document_date"),
        "total":         gt.get("total"),
        "subtotal":      gt.get("subtotal"),
        "gst":           gt.get("gst"),
        "qst":           gt.get("qst"),
        "currency":      gt.get("currency", "CAD"),
        "tax_code":      gt.get("tax_code"),
        "line_count":    gt.get("line_count"),
        "confidence":    1.0,
    }

    combined_noise = 0.0
    min_confidence = 1.0
    vendor_damage = 0.0

    for c in conditions or []:
        deg = _DEGRADATIONS.get(c, {})
        combined_noise = max(combined_noise, float(deg.get("noise", 0.0)))
        min_confidence = min(min_confidence, float(deg.get("confidence_pct", 1.0)))
        vendor_damage = max(vendor_damage, float(deg.get("degrade_vendor", 0.0)))

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
        if deg.get("degrade_qst") and extracted.get("qst"):
            try:
                q = Decimal(extracted["qst"])
                jitter = Decimal(str(rnd.uniform(-0.03, 0.03)))
                extracted["qst"] = str((q * (Decimal("1") + jitter)).quantize(Decimal("0.01")))
            except Exception:
                pass
        if deg.get("degrade_gst") and extracted.get("gst"):
            try:
                g = Decimal(extracted["gst"])
                jitter = Decimal(str(rnd.uniform(-0.03, 0.03)))
                extracted["gst"] = str((g * (Decimal("1") + jitter)).quantize(Decimal("0.01")))
            except Exception:
                pass

    # Degrade (don't drop) vendor so the logo is still recognisable to oracle
    if vendor_damage > 0 and extracted.get("vendor"):
        extracted["vendor"] = _vendor_with_damage(extracted["vendor"], vendor_damage, rnd)

    extracted["confidence"] = round(min_confidence, 2)

    # Apply per-field noise as ABSOLUTE cent jitter (not percentage).
    # Real OCR either reads the number right or fails hard — it does not
    # scale errors with the amount. Modelling noise as ±combined_noise
    # dollars keeps mild damage inside the oracle's $0.05 tolerance while
    # still distinguishing clean from degraded extractions.
    if combined_noise > 0:
        for field in ("total", "gst", "qst"):
            val = extracted.get(field)
            if val is None:
                continue
            try:
                d = Decimal(str(val))
                jitter = Decimal(str(round(rnd.uniform(-combined_noise, combined_noise), 2)))
                extracted[field] = str((d + jitter).quantize(Decimal("0.01")))
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
        skip_fields = list(spec.get("skip_fields") or [])
        rnd = random.Random(self.seed + (abs(hash(scenario.get("id", ""))) % 10_000_000))

        real_dataset_path = spec.get("image_path") if spec.get("kind") == "real_receipt" else None

        if real_dataset_path:
            # Real dataset scenario: run the live OCR pipeline on the
            # provided image file (no image generation). Bypass the mock
            # fallback — a dataset run is only meaningful with real OCR.
            image_path = Path(real_dataset_path)
            try:
                from src.engines.ocr_engine import process_file  # type: ignore
                _ensure_chaos_documents_table(self.chaos_db_path)
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
                        "subtotal":      processed.get("subtotal"),
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
        elif self.real_ocr:
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
                        "subtotal":      processed.get("subtotal"),
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
        oracle_result = oracle.validate(extracted, gt, skip_fields=skip_fields)

        result.output = {"extracted": extracted, "conditions": conditions,
                         "skip_fields": skip_fields,
                         "source_dataset": spec.get("source_dataset"),
                         "image_path": real_dataset_path,
                         "functions_called": calls,
                         "mode": "real_dataset" if real_dataset_path else (
                             "real_ocr" if self.real_ocr else "mock")}
        result.oracle_result = oracle_result.to_dict()
        result.score = oracle_result.total_score
        result.passed = oracle_result.passed
