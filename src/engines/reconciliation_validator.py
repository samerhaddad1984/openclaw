"""
src/engines/reconciliation_validator.py — Invoice reconciliation engine.

Provides deterministic invoice total reconciliation and FX conversion
validation.  Never produces unreconciled totals — gaps are always
explained or flagged as UNRESOLVABLE_GAP.

No AI calls.  All arithmetic uses Python Decimal.
"""
from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ZERO = Decimal("0")
_ONE = Decimal("1")
CENT = Decimal("0.01")

# Gap classification thresholds
_FX_ROUNDING_THRESHOLD = Decimal("1.00")
_TAX_AMBIGUITY_THRESHOLD = Decimal("1.00")
_MISSING_LINE_THRESHOLD = Decimal("5.00")
_VENDOR_MARKUP_THRESHOLD = Decimal("50.00")

# FX tolerance: 0.5%
_FX_TOLERANCE_PCT = Decimal("0.005")


def _round(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _to_dec(value: Any) -> Decimal:
    if value is None or str(value).strip() == "":
        return _ZERO
    return Decimal(str(value))


# =========================================================================
# Invoice total reconciliation
# =========================================================================

def reconcile_invoice_total(
    lines: list[dict[str, Any]],
    invoice_total_shown: Any,
    currency: str,
    fx_rate: Any,
    vendor_markup: Any = None,
) -> dict[str, Any]:
    """Reconcile computed total against the invoice total shown.

    Steps:
    1. Sum all line pretax amounts in original currency.
    2. Apply FX rate to get CAD.
    3. Add all taxes.
    4. Compare to invoice_total_shown.
    5. If gap > $0.02: MUST explain gap.
    6. Never return unreconciled — either reconciled or UNRESOLVABLE_GAP.

    Parameters
    ----------
    lines : list of dicts with keys:
        pretax_amount (original currency), gst, qst, hst (all in CAD)
    invoice_total_shown : the total printed on the invoice (in CAD)
    currency : ISO currency code (e.g. "CAD", "USD")
    fx_rate : exchange rate to CAD (1.0 for CAD)
    vendor_markup : any vendor markup/surcharge not on lines
    """
    total_shown = _to_dec(invoice_total_shown)
    rate = _to_dec(fx_rate) if fx_rate else _ONE
    markup = _to_dec(vendor_markup) if vendor_markup else _ZERO
    curr = (currency or "CAD").strip().upper()

    # Sum line amounts
    line_sum_orig = _ZERO
    tax_sum_cad = _ZERO

    for line in lines:
        line_sum_orig += _to_dec(line.get("pretax_amount", 0))
        tax_sum_cad += _to_dec(line.get("gst", 0))
        tax_sum_cad += _to_dec(line.get("qst", 0))
        tax_sum_cad += _to_dec(line.get("hst", 0))

    # Convert to CAD
    line_sum_cad = _round(line_sum_orig * rate)

    # Computed total
    computed_total = _round(line_sum_cad + tax_sum_cad + markup)

    gap = _round(abs(computed_total - total_shown))

    result: dict[str, Any] = {
        "reconciled": False,
        "line_sum_original_currency": _round(line_sum_orig),
        "currency": curr,
        "fx_rate": rate,
        "line_sum_cad": line_sum_cad,
        "tax_sum_cad": _round(tax_sum_cad),
        "vendor_markup": _round(markup),
        "computed_total_cad": computed_total,
        "invoice_total_shown": _round(total_shown),
        "gap": gap,
        "gap_explanations": [],
        "block_posting": False,
    }

    # Reconciled within $0.02
    if gap <= Decimal("0.02"):
        result["reconciled"] = True
        result["gap_explanations"].append("Totals match within $0.02 tolerance.")
        return result

    # Attempt to explain the gap
    explanations = []
    remaining_gap = gap

    # (a) FX rounding difference
    if curr != "CAD" and remaining_gap <= _FX_ROUNDING_THRESHOLD:
        explanations.append({
            "type": "fx_rounding",
            "severity": "acceptable",
            "amount": remaining_gap,
            "description": (
                f"FX rounding difference of ${remaining_gap} "
                f"(< ${_FX_ROUNDING_THRESHOLD}) — acceptable for "
                f"{curr}→CAD conversion at rate {rate}."
            ),
        })
        remaining_gap = _ZERO

    # (b) Tax inclusion ambiguity
    if remaining_gap > _ZERO and remaining_gap <= _TAX_AMBIGUITY_THRESHOLD:
        explanations.append({
            "type": "tax_inclusion_ambiguity",
            "severity": "flag",
            "amount": remaining_gap,
            "description": (
                f"Gap of ${remaining_gap} may indicate tax inclusion "
                f"ambiguity — verify whether line amounts are pre-tax or "
                f"tax-inclusive."
            ),
        })
        remaining_gap = _ZERO

    # (c) Missing line items
    if remaining_gap > _ZERO and remaining_gap <= _MISSING_LINE_THRESHOLD:
        explanations.append({
            "type": "possible_missing_lines",
            "severity": "flag",
            "amount": remaining_gap,
            "description": (
                f"Gap of ${remaining_gap} suggests possible missing line "
                f"items (shipping, handling, fees). Review invoice for "
                f"additional charges."
            ),
        })
        remaining_gap = _ZERO

    # (d) Vendor markup / undisclosed charges
    if remaining_gap > _ZERO and remaining_gap <= _VENDOR_MARKUP_THRESHOLD:
        explanations.append({
            "type": "vendor_markup",
            "severity": "warning",
            "amount": remaining_gap,
            "description": (
                f"Gap of ${remaining_gap} suggests vendor markup or "
                f"undisclosed charges. Verify with vendor."
            ),
        })
        remaining_gap = _ZERO

    # If still unexplained
    if remaining_gap > _ZERO:
        explanations.append({
            "type": "UNRESOLVABLE_GAP",
            "severity": "critical",
            "amount": remaining_gap,
            "description": (
                f"Unresolvable gap of ${remaining_gap} between computed "
                f"total (${computed_total}) and invoice total shown "
                f"(${total_shown}). Posting blocked — human review required."
            ),
        })
        result["block_posting"] = True

    result["gap_explanations"] = explanations
    if not result["block_posting"] and remaining_gap == _ZERO:
        result["reconciled"] = True

    return result


# =========================================================================
# FX conversion reconciliation
# =========================================================================

def reconcile_fx_conversion(
    original_amount: Any,
    original_currency: str,
    cad_amount: Any,
    fx_rate: Any,
    fx_date: str,
) -> dict[str, Any]:
    """Verify FX conversion: original_amount * fx_rate ≈ cad_amount.

    Gap must be within 0.5%.  If larger, flag fx_reconciliation_gap.
    Source of FX rate must be documented (Bank of Canada rate date).
    """
    orig = _to_dec(original_amount)
    cad = _to_dec(cad_amount)
    rate = _to_dec(fx_rate)
    curr = (original_currency or "").strip().upper()

    if rate <= _ZERO:
        return {
            "reconciled": False,
            "flag": "invalid_fx_rate",
            "original_amount": _round(orig),
            "original_currency": curr,
            "cad_amount": _round(cad),
            "fx_rate": rate,
            "fx_date": fx_date,
            "reasoning": "FX rate must be positive.",
        }

    expected_cad = _round(orig * rate)
    difference = _round(abs(expected_cad - cad))

    if cad == _ZERO and orig == _ZERO:
        pct_gap = _ZERO
    elif cad == _ZERO:
        pct_gap = _ONE  # 100% gap
    else:
        pct_gap = abs(expected_cad - cad) / abs(cad)

    within_tolerance = pct_gap <= _FX_TOLERANCE_PCT

    result: dict[str, Any] = {
        "reconciled": within_tolerance,
        "original_amount": _round(orig),
        "original_currency": curr,
        "cad_amount": _round(cad),
        "fx_rate": rate,
        "fx_date": fx_date,
        "expected_cad": expected_cad,
        "difference": difference,
        "percentage_gap": _round(pct_gap * Decimal("100")),
        "fx_rate_source": f"Bank of Canada rate for {fx_date}",
    }

    if not within_tolerance:
        result["flag"] = "fx_reconciliation_gap"
        result["reasoning"] = (
            f"FX conversion gap exceeds 0.5% tolerance. "
            f"Expected {curr} {orig} × {rate} = CAD {expected_cad}, "
            f"but CAD amount is {cad} (difference: ${difference}, "
            f"gap: {result['percentage_gap']}%)."
        )
    else:
        result["reasoning"] = (
            f"FX conversion verified: {curr} {orig} × {rate} = "
            f"CAD {expected_cad} (within 0.5% of CAD {cad})."
        )

    return result


# =========================================================================
# PART 4 — Credit memo vs refund vs settlement deduplication
# =========================================================================

def detect_duplicate_economic_event(
    credit_memo_amount: Any,
    credit_memo_date: str,
    credit_memo_vendor: str,
    bank_deposit_amount: Any,
    bank_deposit_date: str,
    bank_deposit_payee: str,
    window_days: int = 30,
) -> dict[str, Any]:
    """Detect when a credit memo AND a bank deposit exist for the same
    amount from the same vendor within window_days.

    Presents three scenarios for accountant selection.
    Blocks posting until resolved.
    """
    from difflib import SequenceMatcher

    cm = _to_dec(credit_memo_amount)
    bd = _to_dec(bank_deposit_amount)

    diff = _round(abs(abs(cm) - abs(bd)))
    tolerance = max(_round(abs(cm) * Decimal("0.01")), Decimal("0.50"))

    if diff > tolerance:
        return {
            "potential_duplicate_economic_event": False,
            "reasoning": f"Amounts differ by ${diff} (tolerance: ${tolerance}).",
        }

    vendor_norm = (credit_memo_vendor or "").strip().lower()
    payee_norm = (bank_deposit_payee or "").strip().lower()
    similarity = SequenceMatcher(None, vendor_norm, payee_norm).ratio() if vendor_norm and payee_norm else 0.0

    if similarity < 0.50:
        return {
            "potential_duplicate_economic_event": False,
            "reasoning": f"Vendor similarity too low ({similarity:.2f}).",
        }

    try:
        from datetime import datetime
        cm_dt = datetime.strptime(credit_memo_date.strip(), "%Y-%m-%d")
        bd_dt = datetime.strptime(bank_deposit_date.strip(), "%Y-%m-%d")
        delta_days = abs((cm_dt - bd_dt).days)
    except Exception:
        delta_days = None

    if delta_days is not None and delta_days > window_days:
        return {
            "potential_duplicate_economic_event": False,
            "reasoning": f"Dates are {delta_days} days apart (window: {window_days}).",
        }

    return {
        "potential_duplicate_economic_event": True,
        "credit_memo_amount": _round(abs(cm)),
        "bank_deposit_amount": _round(abs(bd)),
        "vendor_similarity": round(similarity, 4),
        "date_delta_days": delta_days,
        "settlement_state": "UNRESOLVED",
        "block_posting": True,
        "scenarios": [
            {
                "scenario": "SCENARIO_A",
                "description_en": "Credit memo + bank refund are two separate real events",
                "description_fr": "Note de crédit + remboursement bancaire sont deux événements distincts",
            },
            {
                "scenario": "SCENARIO_B",
                "description_en": "Credit memo used as settlement — one event, two documents",
                "description_fr": "Note de crédit utilisée comme règlement — un seul événement, deux documents",
            },
            {
                "scenario": "SCENARIO_C",
                "description_en": "Duplicate ingestion — same event, should deduplicate",
                "description_fr": "Ingestion en double — même événement, devrait être dédupliqué",
            },
        ],
        "requires_accountant_selection": True,
        "reasoning": (
            f"Credit memo (${abs(cm)}) and bank deposit (${abs(bd)}) from same vendor "
            f"within {delta_days} days. Posting blocked until accountant selects scenario."
        ),
    }
