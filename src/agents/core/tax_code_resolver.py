from __future__ import annotations

import re
from typing import Any, Dict

GST_KEYWORDS = [
    "gst",
    "tps"
]

QST_KEYWORDS = [
    "qst",
    "tvq"
]

HST_KEYWORDS = [
    "hst"
]

VAT_KEYWORDS = [
    "vat"
]

TAX_GENERIC_KEYWORDS = [
    "tax",
    "sales tax"
]

# FIX 9: Tax-inclusive pricing labels (FR/EN)
TAX_INCLUSIVE_KEYWORDS = [
    "taxes incluses",
    "tax included",
    "tps/tvq incluses",
    "all taxes included",
    "toutes taxes comprises",
    "ttc",
]


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).lower().strip()


def contains_any(text: str, keywords: list[str]) -> bool:
    for k in keywords:
        if k in text:
            return True
    return False


def _parse_tax_amount(s: str) -> float:
    """Parse a tax amount string, handling French comma-decimal format."""
    # French format: "5,00" or "1 234,50" → normalize to dot decimal
    s = s.strip()
    s = re.sub(r"(\d)\s+(\d)", r"\1\2", s)  # strip thousands space
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    return float(s)


def extract_tax_lines(text: str) -> Dict[str, float]:
    """
    Very simple tax amount detection.
    Looks for lines containing tax keywords and numbers.
    Handles both English (5.00) and French-Canadian (5,00) decimal formats.
    """
    results = {}
    lines = text.splitlines()

    for line in lines:
        l = line.lower()
        # Match English "5.00" and French "5,00" decimal formats
        number_match = re.findall(r"\d[\d ]*[.,]\d{2}", l)
        if not number_match:
            continue

        value = _parse_tax_amount(number_match[-1])

        if contains_any(l, GST_KEYWORDS):
            results["gst"] = value
        elif contains_any(l, QST_KEYWORDS):
            results["qst"] = value
        elif contains_any(l, HST_KEYWORDS):
            results["hst"] = value
        elif contains_any(l, VAT_KEYWORDS):
            results["vat"] = value
        elif contains_any(l, TAX_GENERIC_KEYWORDS):
            results["tax"] = value

    return results


def resolve_tax_code(document: Dict[str, Any]) -> Dict[str, Any]:
    raw = document.get("raw_result") or {}
    text = raw.get("text_preview", "")
    currency = normalize_text(raw.get("raw_rules_output", {}).get("currency"))
    vendor = normalize_text(document.get("vendor"))
    text_lower = normalize_text(text)
    tax_lines = extract_tax_lines(text_lower)

    decision = "NONE"
    reason = "default_no_tax"
    tax_inclusive = False
    tax_inclusive_note = ""

    # FIX 9: Detect tax-inclusive pricing before standard detection
    if contains_any(text_lower, TAX_INCLUSIVE_KEYWORDS):
        tax_inclusive = True
        decision = "T"
        reason = "tax_inclusive_detected"
        tax_inclusive_note = (
            "Prix taxes incluses — CTI/RTI peut ne pas s'appliquer / "
            "Tax-inclusive price — ITC/ITR may not apply"
        )

    if "gst" in tax_lines or "qst" in tax_lines:
        decision = "GST_QST"
        reason = "gst_qst_detected"
    elif "hst" in tax_lines:
        decision = "HST"
        reason = "hst_detected"
    elif "vat" in tax_lines:
        decision = "VAT"
        reason = "vat_detected"
    elif currency == "usd":
        decision = "NONE"
        reason = "foreign_vendor_usd_invoice"
        tax_inclusive = False
    elif not tax_inclusive and contains_any(text_lower, TAX_GENERIC_KEYWORDS):
        decision = "GENERIC_TAX"
        reason = "generic_tax_line_found"

    result: Dict[str, Any] = {
        "tax_code": decision,
        "reason": reason,
        "detected_tax_lines": tax_lines,
        "tax_inclusive": tax_inclusive,
    }
    if tax_inclusive_note:
        result["note"] = tax_inclusive_note
        result["requires_review"] = True

    return result