"""
src/engines/tax_code_resolver.py
================================
Mixed taxable/exempt invoice detection for LedgerLink.

Detects invoices that contain both taxable and tax-exempt line items,
which require manual tax allocation.  Uses keyword detection first;
when inconclusive, falls back to AI-assisted mixed tax detection via
ai_router.
"""
from __future__ import annotations

import logging
import re
from typing import Any

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Mixed tax keywords (bilingual FR/EN)
# ---------------------------------------------------------------------------

_MIXED_TAX_KEYWORDS = re.compile(
    r"("
    r"\bfournitures médicales et alimentaires\b|"
    r"\bfournitures medicales et alimentaires\b|"
    r"\bpartial exempt\b|"
    r"\bpartiellement exempté\b|"
    r"\bpartiellement exonéré\b|"
    r"\bmixed suppl(?:y|ies)\b|"
    r"\bfournitures mixtes\b|"
    r"\bligne taxable.*exonérée\b|"
    r"\bligne exonérée.*taxable\b|"
    r"\btaxable.*exempt\b|"
    r"\bexempt.*taxable\b|"
    r"\bexonéré.*taxable\b|"
    r"\btaxable.*exonéré\b|"
    r"\bzero.rated.*taxable\b|"
    r"\btaxable.*zero.rated\b|"
    r"\bdétaxé.*taxable\b|"
    r"\btaxable.*détaxé\b"
    r")",
    re.IGNORECASE,
)

# Secondary indicators — weaker signals that together suggest mixed tax
_MIXED_TAX_SECONDARY = re.compile(
    r"\b("
    r"exempt|exempté|exonéré|"
    r"détaxé|zero.rated|"
    r"non.taxable|non taxable|"
    r"basic groceries|épicerie de base|"
    r"medical supplies|fournitures médicales|"
    r"prescription|ordonnance"
    r")\b",
    re.IGNORECASE,
)

_TAXABLE_INDICATORS = re.compile(
    r"\b("
    r"taxable|tps|gst|tvq|qst|"
    r"fournitures de bureau|office supplies|"
    r"professional services|services professionnels|"
    r"prepared food|nourriture préparée"
    r")\b",
    re.IGNORECASE,
)


def resolve_mixed_tax(
    *,
    memo: str = "",
    line_items: list[str] | None = None,
    invoice_text: str = "",
    vendor: str = "",
) -> dict[str, Any]:
    """Detect mixed taxable/exempt invoices.

    Returns a dict with:
      mixed_tax_invoice  — True if mixed tax detected
      tax_code           — None if mixed (requires manual allocation)
      block_auto_approval — True if mixed detected
      review_notes       — list of bilingual notes
      confidence         — detection confidence 0.0-1.0
    """
    combined = f"{memo} {invoice_text} {' '.join(line_items or [])} {vendor}".lower()

    result: dict[str, Any] = {
        "mixed_tax_invoice": False,
        "tax_code": None,
        "block_auto_approval": False,
        "review_notes": [],
        "confidence": 0.0,
    }

    # Strong keyword match
    if _MIXED_TAX_KEYWORDS.search(combined):
        result["mixed_tax_invoice"] = True
        result["tax_code"] = None
        result["block_auto_approval"] = True
        result["confidence"] = 0.85
        result["review_notes"].append(
            "Facture mixte détectée (articles taxables et exonérés) — "
            "allocation manuelle des taxes requise. Code de taxe non attribué. / "
            "Mixed invoice detected (taxable and exempt items) — "
            "manual tax allocation required. Tax code not assigned."
        )
        return result

    # Secondary detection: both exempt AND taxable indicators present
    has_exempt = bool(_MIXED_TAX_SECONDARY.search(combined))
    has_taxable = bool(_TAXABLE_INDICATORS.search(combined))

    if has_exempt and has_taxable:
        result["mixed_tax_invoice"] = True
        result["tax_code"] = None
        result["block_auto_approval"] = True
        result["confidence"] = 0.65
        result["review_notes"].append(
            "Facture potentiellement mixte (indicateurs taxables et exonérés trouvés) — "
            "vérification manuelle requise. / "
            "Potentially mixed invoice (taxable and exempt indicators found) — "
            "manual verification required."
        )
        # Confidence < 0.70 — call AI for confirmation
        result = _ai_mixed_tax_fallback(result, invoice_text or combined)
        return result

    # No keyword match — check if AI should be called
    # Only call AI if there's meaningful text to analyze
    if len(combined.strip()) > 50 and (has_exempt or has_taxable):
        result = _ai_mixed_tax_fallback(result, invoice_text or combined)

    return result


def _ai_mixed_tax_fallback(
    result: dict[str, Any],
    invoice_text: str,
) -> dict[str, Any]:
    """Call AI mixed tax detection as fallback when keyword detection is inconclusive."""
    try:
        from src.agents.core import ai_router

        ai_result = ai_router.call_mixed_tax_detection(
            invoice_text=invoice_text,
        )
        if ai_result.get("is_mixed") is True and not ai_result.get("error"):
            result["mixed_tax_invoice"] = True
            result["tax_code"] = None
            result["block_auto_approval"] = True
            result["confidence"] = max(result["confidence"], 0.75)

            taxable = ai_result.get("taxable_items") or []
            exempt = ai_result.get("exempt_items") or []
            allocation = ai_result.get("suggested_allocation")

            note_parts = [
                "AI: Facture mixte confirmée / Mixed invoice confirmed."
            ]
            if taxable:
                note_parts.append(f"Taxable: {', '.join(taxable[:5])}")
            if exempt:
                note_parts.append(f"Exempt: {', '.join(exempt[:5])}")
            if allocation:
                note_parts.append(
                    f"Allocation suggérée / Suggested: "
                    f"taxable={allocation.get('taxable_total', '?')}, "
                    f"exempt={allocation.get('exempt_total', '?')}"
                )
            result["review_notes"].append(" | ".join(note_parts))
    except Exception as exc:
        log.debug("AI mixed tax detection fallback failed: %s", exc)

    return result


# =========================================================================
# PART 3 — Document footer boilerplate vs transaction truth
# =========================================================================

BOILERPLATE_PATTERNS: list[str] = [
    "all prices include applicable taxes",
    "tous les prix incluent les taxes applicables",
    "prices shown include all applicable taxes",
    "les prix affichés incluent toutes les taxes",
    "tax included",
    "taxes incluses",
    "toutes taxes comprises",
    "ttc",
    "prix taxes incluses",
    "prices include gst and qst",
    "prix incluant tps et tvq",
]

_FOOTER_MARKERS = re.compile(
    r"("
    r"terms?\s*(and|&|et)\s*conditions?|"
    r"conditions?\s*(de\s+)?(vente|paiement)|"
    r"thank\s+you|merci|"
    r"remit\s+to|adresse\s+de\s+paiement|"
    r"notes?:|remarques?:|"
    r"page\s+\d+\s+(of|de|sur)\s+\d+|"
    r"©|copyright|all\s+rights\s+reserved|"
    r"--+|_{3,}|={3,}"
    r")",
    re.IGNORECASE,
)


def detect_tax_inclusive_position(
    document_text: str,
) -> dict[str, Any]:
    """Check if tax-inclusive language appears in footer vs line item area.

    1. If in footer only: weight = 0.30 (likely boilerplate)
    2. If in line item area or next to specific amounts: weight = 0.80
    3. If footer boilerplate: do NOT auto-extract implicit tax
    """
    if not document_text:
        return {
            "tax_inclusive_found": False,
            "weight": 0.0,
            "position": None,
        }

    text_lower = document_text.lower()
    lines = text_lower.split("\n")
    total_lines = len(lines)

    if total_lines == 0:
        return {
            "tax_inclusive_found": False,
            "weight": 0.0,
            "position": None,
        }

    # Find tax-inclusive phrases
    found_positions: list[dict[str, Any]] = []
    for i, line in enumerate(lines):
        for pattern in BOILERPLATE_PATTERNS:
            if pattern in line:
                relative_position = i / max(total_lines, 1)
                is_footer = (
                    relative_position > 0.75
                    or bool(_FOOTER_MARKERS.search(line))
                    or (i > 0 and bool(_FOOTER_MARKERS.search(lines[i - 1])))
                )
                has_amount = bool(re.search(r"\$\s*\d+[.,]\d{2}", line))

                found_positions.append({
                    "line_number": i + 1,
                    "text": line.strip()[:100],
                    "is_footer": is_footer,
                    "near_amount": has_amount,
                    "pattern_matched": pattern,
                })

    if not found_positions:
        return {
            "tax_inclusive_found": False,
            "weight": 0.0,
            "position": None,
        }

    any_in_body = any(not fp["is_footer"] for fp in found_positions)
    any_near_amount = any(fp["near_amount"] for fp in found_positions)
    all_in_footer = all(fp["is_footer"] for fp in found_positions)

    if any_near_amount or (any_in_body and not all_in_footer):
        weight = 0.80
        position = "line_item_area"
        is_boilerplate = False
    else:
        weight = 0.30
        position = "footer"
        is_boilerplate = True

    return {
        "tax_inclusive_found": True,
        "weight": weight,
        "position": position,
        "is_boilerplate": is_boilerplate,
        "boilerplate_tax_disclaimer": is_boilerplate,
        "auto_extract_implicit_tax": not is_boilerplate,
        "found_positions": found_positions,
        "reasoning": (
            f"Tax-inclusive language found in {position} "
            f"(weight: {weight}). "
            + (
                "Likely boilerplate — do NOT auto-extract implicit tax."
                if is_boilerplate
                else "Found near line items — may reflect actual tax treatment."
            )
        ),
    }
