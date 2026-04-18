"""Receipt nightmare scenarios.

Each scenario describes how a synthetic receipt should be generated (prompt
for the image model, plus optional text-only fallback) and what the system
should extract. Ground truth values let the ReceiptOracle score accuracy.
"""
from __future__ import annotations

import json
import random
from decimal import Decimal
from pathlib import Path
from typing import Any

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


def _load_vendors() -> list[dict[str, Any]]:
    path = FIXTURES / "vendors_quebec.json"
    if path.exists():
        return json.loads(path.read_text())
    return []


# ---------------------------------------------------------------------------
# Nightmare conditions — rotating list so we cover all of them
# ---------------------------------------------------------------------------

NIGHTMARE_CONDITIONS: list[dict[str, Any]] = [
    {
        "subtype": "coffee_stained_grocery_30_items",
        "difficulty": "nightmare",
        "conditions": ["coffee_stain_bottom_third", "crumpled", "thermal_fade_right"],
        "vendor_kind": "grocery",
        "item_count": 30,
        "severity_on_failure": "high",
        "description": "Coffee-stained IGA receipt with 30 items, thermal fade on right",
    },
    {
        "subtype": "gas_station_fuel_snacks",
        "difficulty": "nightmare",
        "conditions": ["crumpled", "4_decimal_fuel_price", "torn_corner"],
        "vendor_kind": "gas",
        "item_count": 4,
        "severity_on_failure": "high",
        "description": "Crumpled gas receipt with 4-decimal fuel price + snacks + torn corner",
    },
    {
        "subtype": "faded_thermal_half_unreadable",
        "difficulty": "nightmare",
        "conditions": ["thermal_fade_50pct", "low_contrast"],
        "vendor_kind": "convenience",
        "item_count": 6,
        "severity_on_failure": "high",
        "description": "Thermal paper half-faded — only top/bottom lines readable",
    },
    {
        "subtype": "torn_receipt_partial_totals",
        "difficulty": "nightmare",
        "conditions": ["torn_bottom", "missing_total_line"],
        "vendor_kind": "restaurant",
        "item_count": 5,
        "severity_on_failure": "medium",
        "description": "Torn receipt showing only partial totals",
    },
    {
        "subtype": "photo_45_deg_angle",
        "difficulty": "hard",
        "conditions": ["rotated_45deg", "perspective_distortion"],
        "vendor_kind": "grocery",
        "item_count": 12,
        "severity_on_failure": "medium",
        "description": "Receipt photographed at 45 degree angle",
    },
    {
        "subtype": "photo_low_light",
        "difficulty": "hard",
        "conditions": ["brightness_minus_80pct", "noise"],
        "vendor_kind": "restaurant",
        "item_count": 7,
        "severity_on_failure": "medium",
        "description": "Low-light photo, 80% darker than normal",
    },
    {
        "subtype": "flash_glare_obscures_amounts",
        "difficulty": "nightmare",
        "conditions": ["flash_glare_center", "reflection"],
        "vendor_kind": "pharmacy",
        "item_count": 8,
        "severity_on_failure": "high",
        "description": "Flash glare obscures total and line items",
    },
    {
        "subtype": "handwritten_tip_addendum",
        "difficulty": "hard",
        "conditions": ["handwritten_overlay_tip"],
        "vendor_kind": "restaurant",
        "item_count": 6,
        "extras": {"handwritten_tip_cad": "10.00"},
        "severity_on_failure": "medium",
        "description": "Printed restaurant receipt with handwritten '+$10 tip' overlay",
    },
    {
        "subtype": "through_plastic_sleeve",
        "difficulty": "hard",
        "conditions": ["plastic_sleeve_reflection"],
        "vendor_kind": "office_supplies",
        "item_count": 5,
        "severity_on_failure": "medium",
        "description": "Photographed through plastic sleeve with reflection",
    },
    {
        "subtype": "bilingual_fr_en_mixed",
        "difficulty": "hard",
        "conditions": ["bilingual_labels"],
        "vendor_kind": "grocery",
        "item_count": 10,
        "severity_on_failure": "medium",
        "description": "Bilingual receipt — FR and EN labels mixed on same line",
    },
    {
        "subtype": "multi_tax_zone_qc_on",
        "difficulty": "nightmare",
        "conditions": ["two_tax_lines", "multi_province"],
        "vendor_kind": "generic",
        "item_count": 8,
        "extras": {"provinces": ["QC", "ON"]},
        "severity_on_failure": "high",
        "description": "Receipt crossing QC and ON tax zones on same trip",
    },
    {
        "subtype": "very_long_50_items_3_photos",
        "difficulty": "nightmare",
        "conditions": ["split_across_photos"],
        "vendor_kind": "grocery",
        "item_count": 50,
        "severity_on_failure": "high",
        "description": "50-item receipt cut across 3 photos — must stitch",
    },
    {
        "subtype": "thermal_middle_faded",
        "difficulty": "hard",
        "conditions": ["thermal_fade_middle_band"],
        "vendor_kind": "grocery",
        "item_count": 15,
        "severity_on_failure": "medium",
        "description": "Top and bottom readable, middle band faded",
    },
    {
        "subtype": "customer_gl_code_overlay",
        "difficulty": "hard",
        "conditions": ["handwritten_gl_codes_overlay"],
        "vendor_kind": "office_supplies",
        "item_count": 4,
        "severity_on_failure": "low",
        "description": "Customer wrote GL codes directly on the receipt",
    },
    {
        "subtype": "stapled_cc_slip_overlap",
        "difficulty": "hard",
        "conditions": ["overlapping_cc_slip"],
        "vendor_kind": "restaurant",
        "item_count": 4,
        "severity_on_failure": "medium",
        "description": "Credit card slip stapled on top, partially covering receipt",
    },
    {
        "subtype": "motion_blur",
        "difficulty": "hard",
        "conditions": ["motion_blur_horizontal"],
        "vendor_kind": "grocery",
        "item_count": 8,
        "severity_on_failure": "medium",
        "description": "Hand-shake motion blur",
    },
    {
        "subtype": "reflective_surface_glare",
        "difficulty": "hard",
        "conditions": ["surface_glare"],
        "vendor_kind": "gas",
        "item_count": 3,
        "severity_on_failure": "medium",
        "description": "Receipt on reflective surface with glare",
    },
    {
        "subtype": "logo_watermark_overlay",
        "difficulty": "hard",
        "conditions": ["logo_watermark"],
        "vendor_kind": "generic",
        "item_count": 5,
        "severity_on_failure": "low",
        "description": "Logo watermark across amounts",
    },
    {
        "subtype": "dual_currency_cad_usd",
        "difficulty": "nightmare",
        "conditions": ["dual_currency_display"],
        "vendor_kind": "online",
        "item_count": 3,
        "extras": {"currencies": ["CAD", "USD"], "fx_rate": 1.37},
        "severity_on_failure": "high",
        "description": "Receipt shows both CAD and USD totals — which is canonical?",
    },
    {
        "subtype": "self_checkout_promo_barcodes",
        "difficulty": "hard",
        "conditions": ["promo_barcodes_noise"],
        "vendor_kind": "grocery",
        "item_count": 12,
        "severity_on_failure": "low",
        "description": "Self-checkout receipt with promo barcodes cluttering line items",
    },
    {
        "subtype": "pharmacy_rx_plus_otc",
        "difficulty": "nightmare",
        "conditions": ["mixed_rx_otc"],
        "vendor_kind": "pharmacy",
        "item_count": 8,
        "extras": {"rx_count": 2, "otc_count": 6},
        "severity_on_failure": "high",
        "description": "Pharmacy receipt mixing Rx (zero-rated) with OTC (taxable)",
    },
    {
        "subtype": "restaurant_service_charge_plus_tip",
        "difficulty": "hard",
        "conditions": ["service_charge_line", "tip_line"],
        "vendor_kind": "restaurant",
        "item_count": 6,
        "extras": {"service_charge_cad": "3.50", "tip_cad": "8.00"},
        "severity_on_failure": "medium",
        "description": "Restaurant bill with separate service charge + tip",
    },
    {
        "subtype": "french_only",
        "difficulty": "hard",
        "conditions": ["french_only_labels"],
        "vendor_kind": "grocery",
        "item_count": 10,
        "severity_on_failure": "medium",
        "description": "All labels in French only — no English",
    },
    {
        "subtype": "cash_vs_credit_both_shown",
        "difficulty": "hard",
        "conditions": ["dual_payment_display"],
        "vendor_kind": "restaurant",
        "item_count": 4,
        "severity_on_failure": "medium",
        "description": "Shows both cash-price and credit-price totals",
    },
    {
        "subtype": "costco_item_codes",
        "difficulty": "nightmare",
        "conditions": ["numeric_item_codes_only"],
        "vendor_kind": "warehouse",
        "item_count": 20,
        "severity_on_failure": "high",
        "description": "Costco-style long receipt with only numeric SKUs, no names",
    },
    {
        "subtype": "walmart_abbreviated_names",
        "difficulty": "hard",
        "conditions": ["truncated_item_names"],
        "vendor_kind": "big_box",
        "item_count": 15,
        "severity_on_failure": "medium",
        "description": "Walmart receipt with heavily abbreviated item names",
    },
    {
        "subtype": "dollarama_missing_header",
        "difficulty": "hard",
        "conditions": ["missing_vendor_header"],
        "vendor_kind": "dollar_store",
        "item_count": 8,
        "severity_on_failure": "medium",
        "description": "Dollarama receipt with vendor header cut off",
    },
    {
        "subtype": "uber_eats_weird_fees",
        "difficulty": "hard",
        "conditions": ["platform_fees_breakdown"],
        "vendor_kind": "delivery",
        "item_count": 3,
        "extras": {"service_fee": "2.99", "delivery_fee": "3.49"},
        "severity_on_failure": "medium",
        "description": "Uber Eats receipt with unusual fee structure",
    },
    {
        "subtype": "amazon_ca_subscription",
        "difficulty": "hard",
        "conditions": ["subscription_line"],
        "vendor_kind": "online",
        "item_count": 4,
        "severity_on_failure": "medium",
        "description": "Amazon.ca with a Subscribe-and-Save line item",
    },
    {
        "subtype": "online_screenshot",
        "difficulty": "normal",
        "conditions": ["screenshot_pixel_perfect"],
        "vendor_kind": "online",
        "item_count": 3,
        "severity_on_failure": "low",
        "description": "Screenshot of an online receipt — not paper",
    },
    # ---- edge cases ----
    {
        "subtype": "negative_total_full_return",
        "difficulty": "nightmare",
        "conditions": ["negative_total"],
        "vendor_kind": "grocery",
        "item_count": 3,
        "extras": {"is_return": True},
        "severity_on_failure": "critical",
        "description": "Full return — total is negative",
    },
    {
        "subtype": "zero_total_loyalty_points",
        "difficulty": "hard",
        "conditions": ["zero_total"],
        "vendor_kind": "pharmacy",
        "item_count": 2,
        "severity_on_failure": "medium",
        "description": "$0.00 total — fully paid with loyalty points",
    },
    {
        "subtype": "fuel_4_digit_cents",
        "difficulty": "hard",
        "conditions": ["4_digit_cent_price"],
        "vendor_kind": "gas",
        "item_count": 1,
        "severity_on_failure": "medium",
        "description": "Fuel price with 4-digit cents (e.g. $1.4799/L)",
    },
    {
        "subtype": "discount_flip_negative",
        "difficulty": "nightmare",
        "conditions": ["discount_makes_negative"],
        "vendor_kind": "grocery",
        "item_count": 5,
        "severity_on_failure": "high",
        "description": "Discount line causes subtotal to flip negative then positive",
    },
    {
        "subtype": "future_date",
        "difficulty": "hard",
        "conditions": ["date_in_future"],
        "vendor_kind": "grocery",
        "item_count": 3,
        "severity_on_failure": "high",
        "description": "Receipt date is 2 years in the future",
    },
    {
        "subtype": "ancient_date_10y",
        "difficulty": "hard",
        "conditions": ["date_10_years_old"],
        "vendor_kind": "grocery",
        "item_count": 3,
        "severity_on_failure": "medium",
        "description": "Receipt dated 10 years ago — stale period",
    },
    {
        "subtype": "same_amount_all_lines",
        "difficulty": "normal",
        "conditions": ["identical_line_amounts"],
        "vendor_kind": "office_supplies",
        "item_count": 12,
        "severity_on_failure": "low",
        "description": "Every line item has the same dollar amount",
    },
    {
        "subtype": "single_huge_line",
        "difficulty": "nightmare",
        "conditions": ["single_line_huge_amount"],
        "vendor_kind": "generic",
        "item_count": 1,
        "extras": {"amount_cad": "999999.99"},
        "severity_on_failure": "critical",
        "description": "One line item at $999,999.99 — catastrophic if misread",
    },
    {
        "subtype": "hundred_plus_items",
        "difficulty": "nightmare",
        "conditions": ["very_many_items"],
        "vendor_kind": "warehouse",
        "item_count": 105,
        "severity_on_failure": "high",
        "description": "100+ line items",
    },
    {
        "subtype": "foreign_language_spanish",
        "difficulty": "nightmare",
        "conditions": ["foreign_labels_spanish"],
        "vendor_kind": "generic",
        "item_count": 6,
        "severity_on_failure": "medium",
        "description": "Entirely Spanish-language receipt",
    },
    {
        "subtype": "foreign_language_arabic",
        "difficulty": "impossible",
        "conditions": ["foreign_labels_arabic", "rtl_text"],
        "vendor_kind": "generic",
        "item_count": 6,
        "severity_on_failure": "medium",
        "expected_fail": True,
        "description": "Arabic-language receipt with RTL text (known OCR limitation)",
    },
    {
        "subtype": "foreign_language_chinese",
        "difficulty": "impossible",
        "conditions": ["foreign_labels_chinese"],
        "vendor_kind": "generic",
        "item_count": 6,
        "severity_on_failure": "medium",
        "expected_fail": True,
        "description": "Chinese-language receipt (known OCR limitation)",
    },
    {
        "subtype": "total_only_no_items",
        "difficulty": "hard",
        "conditions": ["totals_only_no_lines"],
        "vendor_kind": "parking",
        "item_count": 0,
        "severity_on_failure": "low",
        "description": "Only total and tax shown — no line items",
    },
    # ---- normal/easy baseline for calibration ----
    {
        "subtype": "clean_iga_grocery",
        "difficulty": "easy",
        "conditions": [],
        "vendor_kind": "grocery",
        "item_count": 8,
        "severity_on_failure": "low",
        "description": "Baseline clean IGA receipt — should always pass",
    },
    {
        "subtype": "clean_restaurant",
        "difficulty": "easy",
        "conditions": [],
        "vendor_kind": "restaurant",
        "item_count": 5,
        "severity_on_failure": "low",
        "description": "Baseline clean restaurant receipt",
    },
    {
        "subtype": "clean_gas",
        "difficulty": "easy",
        "conditions": [],
        "vendor_kind": "gas",
        "item_count": 1,
        "severity_on_failure": "low",
        "description": "Baseline clean Shell/Petro-Canada fuel receipt",
    },
    {
        "subtype": "normal_pharmacy",
        "difficulty": "normal",
        "conditions": ["thermal_paper"],
        "vendor_kind": "pharmacy",
        "item_count": 5,
        "severity_on_failure": "low",
        "description": "Thermal pharmacy receipt, mostly clean",
    },
    {
        "subtype": "normal_office_supplies",
        "difficulty": "normal",
        "conditions": [],
        "vendor_kind": "office_supplies",
        "item_count": 6,
        "severity_on_failure": "low",
        "description": "Staples receipt — normal conditions",
    },
    {
        "subtype": "normal_construction_hardware",
        "difficulty": "normal",
        "conditions": [],
        "vendor_kind": "hardware",
        "item_count": 10,
        "severity_on_failure": "low",
        "description": "Rona / Home Depot normal hardware receipt",
    },
    {
        "subtype": "normal_telecom",
        "difficulty": "normal",
        "conditions": [],
        "vendor_kind": "telecom",
        "item_count": 4,
        "severity_on_failure": "low",
        "description": "Bell/Videotron monthly bill",
    },
    {
        "subtype": "normal_courier",
        "difficulty": "normal",
        "conditions": [],
        "vendor_kind": "courier",
        "item_count": 3,
        "severity_on_failure": "low",
        "description": "Purolator/UPS shipping receipt",
    },
]


VENDOR_PROFILES = {
    "grocery":        {"name": "IGA Des Sources",     "province": "QC", "tax_code": "T"},
    "gas":            {"name": "Petro-Canada",        "province": "QC", "tax_code": "T"},
    "convenience":    {"name": "Couche-Tard",         "province": "QC", "tax_code": "T"},
    "restaurant":     {"name": "Restaurant L'Express", "province": "QC", "tax_code": "M"},
    "pharmacy":       {"name": "Jean Coutu",          "province": "QC", "tax_code": "T"},
    "office_supplies":{"name": "Staples",             "province": "QC", "tax_code": "T"},
    "hardware":       {"name": "Rona",                "province": "QC", "tax_code": "T"},
    "warehouse":      {"name": "Costco Wholesale",    "province": "QC", "tax_code": "T"},
    "big_box":        {"name": "Walmart Canada",      "province": "QC", "tax_code": "T"},
    "dollar_store":   {"name": "Dollarama",           "province": "QC", "tax_code": "T"},
    "delivery":       {"name": "Uber Eats",           "province": "QC", "tax_code": "M"},
    "online":         {"name": "Amazon.ca",           "province": "QC", "tax_code": "T"},
    "parking":        {"name": "Indigo Stationnement","province": "QC", "tax_code": "T"},
    "telecom":        {"name": "Bell Canada",         "province": "QC", "tax_code": "T"},
    "courier":        {"name": "Purolator",           "province": "QC", "tax_code": "T"},
    "generic":        {"name": "Fournisseur Générique", "province": "QC", "tax_code": "T"},
}


def _build_ground_truth(spec: dict[str, Any], rnd: random.Random) -> dict[str, Any]:
    """Synthesize the expected extraction for a scenario spec."""
    vendor_kind = spec.get("vendor_kind", "generic")
    profile = VENDOR_PROFILES.get(vendor_kind, VENDOR_PROFILES["generic"]).copy()
    item_count = int(spec.get("item_count") or 3)
    extras = spec.get("extras") or {}

    # Generate line items with reproducible amounts
    unit_range = {
        "grocery":         (1.99, 25.0),
        "gas":             (30.0, 95.0),
        "restaurant":      (8.0, 45.0),
        "pharmacy":        (3.0, 60.0),
        "warehouse":       (4.0, 50.0),
        "office_supplies": (2.0, 120.0),
        "hardware":        (5.0, 250.0),
        "dollar_store":    (1.25, 4.5),
        "delivery":        (6.0, 22.0),
        "online":          (9.0, 140.0),
        "parking":         (3.0, 35.0),
        "telecom":         (45.0, 145.0),
        "courier":         (8.0, 60.0),
        "generic":         (5.0, 80.0),
        "convenience":     (1.5, 15.0),
        "big_box":         (1.99, 55.0),
    }.get(vendor_kind, (5.0, 80.0))

    lines = []
    subtotal = Decimal("0")
    for i in range(max(1, item_count)):
        price = Decimal(str(round(rnd.uniform(*unit_range), 2)))
        lines.append({"line": i + 1, "description": f"ITEM {i+1}", "amount": str(price)})
        subtotal += price

    # Edge cases override the normal math
    if "amount_cad" in extras:
        amount_total = Decimal(extras["amount_cad"])
        subtotal = amount_total / Decimal("1.14975")
    elif spec.get("subtype") == "zero_total_loyalty_points":
        subtotal = Decimal("0")
    elif spec.get("subtype") == "negative_total_full_return":
        subtotal = -subtotal

    gst = (subtotal * Decimal("0.05")).quantize(Decimal("0.01"))
    qst = (subtotal * Decimal("0.09975")).quantize(Decimal("0.01"))
    total = subtotal + gst + qst

    # Handwritten tip / service charge ride on top
    if "handwritten_tip_cad" in extras:
        total += Decimal(extras["handwritten_tip_cad"])
    if "tip_cad" in extras:
        total += Decimal(extras["tip_cad"])
    if "service_charge_cad" in extras:
        total += Decimal(extras["service_charge_cad"])

    # Deterministic date based on subtype for reproducibility
    date_str = "2026-03-15"
    if spec.get("subtype") == "future_date":
        date_str = "2028-06-01"
    elif spec.get("subtype") == "ancient_date_10y":
        date_str = "2016-01-15"

    return {
        "vendor":   profile["name"],
        "province": profile["province"],
        "tax_code": profile["tax_code"],
        "currency": "CAD" if "currencies" not in extras else extras["currencies"][0],
        "document_date": date_str,
        "subtotal": str(subtotal.quantize(Decimal("0.01"))),
        "gst":      str(gst),
        "qst":      str(qst),
        "total":    str(total.quantize(Decimal("0.01"))),
        "line_count": len(lines),
        "is_return": bool(extras.get("is_return", False)),
    }


def _build_prompt(spec: dict[str, Any], gt: dict[str, Any]) -> str:
    """Build an Imagen prompt for a scenario."""
    conds = spec.get("conditions") or []
    cond_phrases = {
        "coffee_stain_bottom_third":   "a large brown coffee stain covering the bottom third",
        "crumpled":                    "heavily crumpled and wrinkled",
        "thermal_fade_right":          "thermal print faded on the right side",
        "thermal_fade_50pct":          "thermal paper half-faded, middle barely legible",
        "thermal_fade_middle_band":    "a horizontal faded band across the middle",
        "4_decimal_fuel_price":        "fuel price shown with four decimal digits (e.g. $1.4799/L)",
        "torn_corner":                 "torn bottom-right corner",
        "torn_bottom":                 "the bottom section torn away",
        "rotated_45deg":               "photographed at a 45-degree angle",
        "perspective_distortion":      "with strong perspective distortion",
        "brightness_minus_80pct":      "in very low lighting (80% darker)",
        "noise":                       "with digital noise",
        "flash_glare_center":          "with harsh camera flash glare obscuring the center",
        "reflection":                  "with glossy reflection",
        "handwritten_overlay_tip":     "with a handwritten '+$10 tip' in blue ink",
        "handwritten_gl_codes_overlay":"with handwritten GL account codes in the margin",
        "plastic_sleeve_reflection":   "photographed through a clear plastic sleeve",
        "bilingual_labels":            "with bilingual French/English labels on the same line",
        "two_tax_lines":               "with two separate tax jurisdictions (QC and ON)",
        "split_across_photos":         "extremely long — 50+ items — cut across three photos",
        "overlapping_cc_slip":         "with a credit-card slip stapled partly over the receipt",
        "motion_blur_horizontal":      "with horizontal motion blur",
        "surface_glare":               "on a shiny reflective surface with glare",
        "logo_watermark":              "with a semi-transparent store logo watermark",
        "dual_currency_display":       "showing both CAD and USD totals",
        "promo_barcodes_noise":        "cluttered with promotional barcodes between lines",
        "mixed_rx_otc":                "mixing prescription (zero-rated) with OTC items",
        "service_charge_line":         "with an explicit service charge line",
        "tip_line":                    "with a tip line",
        "french_only_labels":          "entirely in Quebec French",
        "dual_payment_display":        "showing separate cash-price and credit-price totals",
        "numeric_item_codes_only":     "with only numeric SKU codes instead of item names",
        "truncated_item_names":        "with heavily abbreviated item names",
        "missing_vendor_header":       "with the vendor header cut off",
        "platform_fees_breakdown":     "with a platform service fee and delivery fee line",
        "subscription_line":           "with a Subscribe-and-Save subscription line",
        "screenshot_pixel_perfect":    "rendered as a pixel-perfect web screenshot, not paper",
        "negative_total":              "showing a negative grand total (full return)",
        "zero_total":                  "with a $0.00 grand total (loyalty points applied)",
        "4_digit_cent_price":          "with fuel price at four decimal digits",
        "discount_makes_negative":     "with a large discount flipping subtotal temporarily negative",
        "date_in_future":              "dated 2 years in the future",
        "date_10_years_old":           "dated a decade ago with aged paper",
        "identical_line_amounts":      "where every line item has the same dollar amount",
        "single_line_huge_amount":     "with one line item at nearly one million dollars",
        "very_many_items":             "over one hundred line items long",
        "foreign_labels_spanish":      "entirely in Spanish",
        "foreign_labels_arabic":       "entirely in Arabic (right-to-left)",
        "foreign_labels_chinese":      "entirely in simplified Chinese",
        "rtl_text":                    "right-to-left layout",
        "totals_only_no_lines":        "showing only total and tax with no line items",
        "low_contrast":                "very low contrast",
        "thermal_paper":               "on thermal paper",
    }

    desc_fragments = [cond_phrases[c] for c in conds if c in cond_phrases]
    vendor_name = gt.get("vendor", "Retail Store")
    total = gt.get("total", "0.00")
    item_count = spec.get("item_count", 3)

    prompt = (
        f"A realistic photo of a Canadian retail receipt from {vendor_name}, "
        f"dated {gt.get('document_date','2026-03-15')}, total CAD ${total}, "
        f"showing approximately {item_count} line items"
    )
    if desc_fragments:
        prompt += ". Conditions: " + ", ".join(desc_fragments)
    prompt += ". The receipt text must be legible enough that key amounts are visible."
    return prompt


def generate(rnd: random.Random) -> list[dict[str, Any]]:
    """Yield all receipt scenarios with ground truth + image prompt."""
    out: list[dict[str, Any]] = []
    for spec in NIGHTMARE_CONDITIONS:
        gt = _build_ground_truth(spec, rnd)
        out.append({
            "category":       "receipts",
            "subtype":        spec["subtype"],
            "difficulty":     spec["difficulty"],
            "description":    spec["description"],
            "severity_on_failure": spec["severity_on_failure"],
            "expected_fail":  bool(spec.get("expected_fail", False)),
            "future_feature": bool(spec.get("future_feature", False)),
            "affects_engines": [
                "src.engines.ocr_engine",
                "src.engines.tax_engine",
                "src.engines.line_item_engine",
                "src.engines.merchant_overlay",
                "src.engines.ai_validator",
            ],
            "oracle":         "receipt",
            "input_spec": {
                "kind":       "image",
                "conditions": spec.get("conditions", []),
                "extras":     spec.get("extras", {}),
                "item_count": spec.get("item_count", 3),
                "vendor_kind":spec.get("vendor_kind", "generic"),
                "prompt":     _build_prompt(spec, gt),
            },
            "ground_truth":   gt,
        })
    return out
