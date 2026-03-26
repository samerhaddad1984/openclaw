from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Optional, Union


@dataclass
class AmountPolicyResult:
    bookkeeping_amount: Optional[float]
    amount_source: str
    reason: str


# Comprehensive regex for all Unicode whitespace, zero-width, and BOM characters
_UNICODE_WS_RE = re.compile(
    r"[\s"                  # all standard \s whitespace (includes \u00a0, \u2009, etc.)
    r"\u200b"               # zero width space
    r"\u200c"               # zero width non-joiner
    r"\u200d"               # zero width joiner
    r"\u2007"               # figure space (not matched by \s)
    r"\ufeff"               # BOM / zero width no-break space
    r"\u2060"               # word joiner
    r"\u180e"               # Mongolian vowel separator
    r"]"
)


def _to_float(value: Union[None, int, float, str]) -> Optional[float]:
    try:
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        s = str(value).strip()

        # Strip ALL unicode whitespace, zero-width, and BOM characters
        s = _UNICODE_WS_RE.sub("", s)

        # Remove currency symbols
        s = s.replace("$", "")

        # Strip again after removals
        s = s.strip()

        if not s or s == ".":
            return None

        # P2-1: Handle accounting parenthetical notation: (1,234.56) → -1234.56
        is_parenthesized = s.startswith("(") and s.endswith(")")
        if is_parenthesized:
            s = s[1:-1].strip()
            if not s:
                return None

        # Handle formats with both dot and comma present
        if "," in s and "." in s:
            # Determine which is the decimal separator by position:
            # whichever comes LAST is the decimal separator
            last_comma = s.rfind(",")
            last_dot = s.rfind(".")
            if last_comma > last_dot:
                # European: 1.234,56 → remove dots, replace comma with period
                s = s.replace(".", "").replace(",", ".")
            else:
                # North American: 1,234.56 → remove commas
                s = s.replace(",", "")
        elif "," in s and "." not in s:
            # Comma only: inspect digits after last comma
            last_comma = s.rfind(",")
            digits_after = s[last_comma + 1:]
            if len(digits_after) == 3 and digits_after.isdigit():
                # Exactly 3 digits → thousands separator (North American): "1,234" → 1234
                s = s.replace(",", "")
            elif len(digits_after) <= 2 and digits_after.isdigit():
                # 1-2 digits → decimal separator (French): "5,00" → 5.00
                s = s.replace(",", ".")
            elif len(digits_after) >= 4 and digits_after.isdigit():
                # 4+ digits → ambiguous, cannot determine format safely
                return None
            else:
                # Non-digit chars after comma, treat as decimal separator
                s = s.replace(",", ".")

        if not s or s == ".":
            return None

        result = float(s)
        # Negate if parenthesized
        if is_parenthesized:
            result = -result
        return result
    except Exception:
        return None


def choose_bookkeeping_amount(
    *,
    vendor_name: Optional[str],
    doc_type: Optional[str],
    total: Optional[Union[float, str]],
    notes: Optional[str],
) -> AmountPolicyResult:
    """
    Accounting-friendly amount selection:

    - credit_note:
        use negative/credit total if present
    - utility_bill / invoice / receipt:
        use extracted total
    - credit_card_statement:
        keep total, but later may become review-only depending on policy
    - if notes indicate paid / no payment necessary / credits:
        still keep extracted total for bookkeeping if total is non-zero
    """

    vendor_name = (vendor_name or "").strip()
    doc_type = (doc_type or "").strip()
    notes_lower = (notes or "").lower()
    total_num = _to_float(total)

    if total_num is None:
        return AmountPolicyResult(
            bookkeeping_amount=None,
            amount_source="missing",
            reason="no_total_extracted",
        )

    if doc_type == "credit_note":
        return AmountPolicyResult(
            bookkeeping_amount=total_num,
            amount_source="credit_note_total",
            reason="credit_note_uses_signed_total",
        )

    if doc_type in ("invoice", "receipt", "utility_bill"):
        if "no payment necessary" in notes_lower or "aucun règlement n'est dû" in notes_lower:
            return AmountPolicyResult(
                bookkeeping_amount=total_num,
                amount_source="invoice_total_paid_doc",
                reason="paid_invoice_still_books_total",
            )

        if "payment terms: credit card" in notes_lower or "carte de débit" in notes_lower:
            return AmountPolicyResult(
                bookkeeping_amount=total_num,
                amount_source="invoice_total_card_terms",
                reason="card_paid_invoice_books_total",
            )

        return AmountPolicyResult(
            bookkeeping_amount=total_num,
            amount_source="document_total",
            reason="standard_bill_invoice_total",
        )

    if doc_type == "credit_card_statement":
        return AmountPolicyResult(
            bookkeeping_amount=total_num,
            amount_source="statement_total",
            reason="statement_uses_extracted_total",
        )

    return AmountPolicyResult(
        bookkeeping_amount=total_num,
        amount_source="fallback_total",
        reason="fallback_to_extracted_total",
    )


# ---------------------------------------------------------------------------
# Mixed settlement detection and tax-aware split bookkeeping
# ---------------------------------------------------------------------------

@dataclass
class SplitAmountResult:
    """One leg of a split settlement with tax breakout."""
    method: str
    payment_amount: Optional[float]
    pre_tax_portion: Optional[float]
    tax_portion: Optional[float]
    amount_source: str
    reason: str


def choose_split_bookkeeping_amounts(
    *,
    vendor_name: Optional[str],
    doc_type: Optional[str],
    total: Optional[Union[float, str]],
    notes: Optional[str],
    payments: list[dict],
    tax_code: str = "NONE",
    vendor_province: str = "",
    client_province: str = "",
) -> list[SplitAmountResult]:
    """
    Compute bookkeeping amounts for each leg of a split settlement.

    ``payments`` is a list of dicts each with ``"amount"`` and ``"method"``.

    Uses the tax engine's pro-rata allocator when a known tax code is
    present; otherwise falls back to proportional splitting without
    tax breakout.

    Returns one :class:`SplitAmountResult` per payment.
    """
    total_num = _to_float(total)
    if total_num is None or not payments:
        return [
            SplitAmountResult(
                method="unknown",
                payment_amount=total_num,
                pre_tax_portion=None,
                tax_portion=None,
                amount_source="missing",
                reason="no_payments_or_total",
            )
        ]

    # Try the tax engine allocator for tax-aware splitting
    try:
        from decimal import Decimal
        from src.engines.tax_engine import allocate_tax_to_payments

        decimal_payments = [
            {"amount": Decimal(str(p["amount"])), "method": str(p.get("method", "unknown"))}
            for p in payments
        ]
        alloc = allocate_tax_to_payments(
            Decimal(str(total_num)),
            tax_code,
            decimal_payments,
            vendor_province=vendor_province,
            client_province=client_province,
        )

        results: list[SplitAmountResult] = []
        for pa in alloc["payment_allocations"]:
            results.append(SplitAmountResult(
                method=pa["method"],
                payment_amount=float(pa["payment_amount"]),
                pre_tax_portion=float(pa["pre_tax_portion"]),
                tax_portion=float(pa["tax_portion"]),
                amount_source="tax_engine_prorate",
                reason=f"pro_rata_split_{pa['method']}",
            ))
        return results

    except Exception:
        # Fallback: proportional split without tax breakout
        results = []
        for p in payments:
            p_amount = _to_float(p.get("amount"))
            results.append(SplitAmountResult(
                method=str(p.get("method", "unknown")),
                payment_amount=p_amount,
                pre_tax_portion=None,
                tax_portion=None,
                amount_source="proportional_fallback",
                reason="tax_engine_unavailable",
            ))
        return results


def detect_credit_note_settlement(
    *,
    doc_type: Optional[str],
    total: Optional[Union[float, str]],
    notes: Optional[str],
    bank_payment: Optional[Union[float, str]] = None,
) -> dict:
    """
    Detect when an invoice appears to be partially settled by a credit note.

    Checks whether ``notes`` or ``doc_type`` context implies a credit note
    offset, and whether a ``bank_payment`` amount is less than the invoice
    total — suggesting the difference was covered by a credit note.

    Returns a dict with:
        mixed_settlement: bool
        credit_note_amount: float | None
        bank_amount: float | None
        invoice_total: float | None
        reason: str
    """
    total_num = _to_float(total)
    bank_num = _to_float(bank_payment)
    notes_lower = (notes or "").lower()
    dt = (doc_type or "").lower()

    result: dict = {
        "mixed_settlement": False,
        "credit_note_amount": None,
        "bank_amount": bank_num,
        "invoice_total": total_num,
        "reason": "",
    }

    if total_num is None:
        result["reason"] = "no_total"
        return result

    # Pattern 1: bank payment is less than total — implies credit offset
    if bank_num is not None and total_num > 0 and bank_num < total_num:
        diff = round(total_num - bank_num, 2)
        # Only flag if the difference is material (> $1)
        if diff > 1.0:
            result["mixed_settlement"] = True
            result["credit_note_amount"] = diff
            result["reason"] = (
                f"Paiement bancaire ({bank_num:.2f}$) inférieur au total "
                f"({total_num:.2f}$). Différence de {diff:.2f}$ possiblement "
                f"couverte par une note de crédit. / "
                f"Bank payment (${bank_num:.2f}) less than total "
                f"(${total_num:.2f}). Difference of ${diff:.2f} possibly "
                f"covered by credit note."
            )
            return result

    # Pattern 2: notes mention credit note / note de crédit
    credit_keywords = [
        "credit note", "note de crédit", "note de credit",
        "credit memo", "avoir", "applied credit", "crédit appliqué",
    ]
    for kw in credit_keywords:
        if kw in notes_lower:
            result["mixed_settlement"] = True
            result["reason"] = (
                f"Notes mention '{kw}' — règlement mixte probable. / "
                f"Notes mention '{kw}' — likely mixed settlement."
            )
            # Try to extract amount from notes
            amount_match = re.search(
                r"(?:credit|crédit|avoir|note)[^0-9$]*\$?\s*([\d,]+\.?\d*)",
                notes_lower,
            )
            if amount_match:
                parsed = _to_float(amount_match.group(1))
                if parsed is not None and parsed > 0:
                    result["credit_note_amount"] = parsed
                    if bank_num is None and total_num is not None:
                        result["bank_amount"] = round(total_num - parsed, 2)
            return result

    result["reason"] = "no_mixed_settlement_detected"
    return result


# ---------------------------------------------------------------------------
# Credit memo classification — settlement vs adjustment
# ---------------------------------------------------------------------------

@dataclass
class CreditClassification:
    """Classification of a credit document relative to an invoice."""
    credit_type: str           # "settlement" | "adjustment"
    credit_total: Optional[float]
    pre_tax_reduction: Optional[float]
    tax_reduction: Optional[float]
    reason: str


_ADJUSTMENT_KEYWORDS = [
    "damage", "dommage", "price adjustment", "ajustement de prix",
    "price reduction", "réduction de prix", "return", "retour",
    "defective", "défectueux", "shortage", "manquant",
    "rebate", "rabais", "discount", "escompte", "remise",
    "warranty", "garantie", "correction", "error", "erreur",
    "allowance", "allocation", "overcharge", "surcharge",
]

_SETTLEMENT_KEYWORDS = [
    "applied to invoice", "appliqué à la facture",
    "prior balance", "solde antérieur",
    "prior credit", "crédit antérieur",
    "offset", "compensation",
    "balance owing", "solde dû",
    "applied from", "appliqué de",
    "carried forward", "reporté",
    "on account", "au compte",
    "deposit applied", "dépôt appliqué",
]


def classify_credit_document(
    *,
    credit_doc_type: Optional[str] = None,
    credit_total: Optional[Union[float, str]] = None,
    credit_notes: Optional[str] = None,
    references_invoice: bool = False,
    tax_code: str = "NONE",
) -> CreditClassification:
    """
    Classify a credit document as either a **settlement** (payment
    method applied against an invoice balance) or an **adjustment**
    (reduction of consideration that changes the taxable base).

    The distinction matters for tax:
    - **Settlement**: The credit is a payment method.  It does NOT
      change the ITC/ITR claim on the original invoice.
    - **Adjustment**: The credit reduces the original consideration.
      The ITC/ITR claim must be reduced by the tax portion of the
      credit memo.

    Parameters
    ----------
    credit_doc_type  : Document type (e.g. "credit_note", "credit_memo").
    credit_total     : Total amount of the credit (tax-included).
    credit_notes     : Free-text notes or description on the credit.
    references_invoice : Whether the credit explicitly references the
                         same invoice it is being applied against.
    tax_code         : Tax code of the original invoice (used to
                       extract the tax portion from the credit).

    Returns
    -------
    CreditClassification with:
        credit_type       — "settlement" or "adjustment"
        credit_total      — the credit amount
        pre_tax_reduction — pre-tax portion (only for adjustments)
        tax_reduction     — tax portion to reverse (only for adjustments)
        reason            — bilingual explanation
    """
    total_num = _to_float(credit_total)
    notes_lower = (credit_notes or "").lower()
    dt = (credit_doc_type or "").lower()

    # Score each category by keyword hits
    adj_score = 0
    settle_score = 0

    for kw in _ADJUSTMENT_KEYWORDS:
        if kw in notes_lower:
            adj_score += 1

    for kw in _SETTLEMENT_KEYWORDS:
        if kw in notes_lower:
            settle_score += 1

    # "credit_memo" doc type leans toward adjustment (new document
    # reducing consideration); "credit_note" used as payment leans
    # toward settlement when notes say so.
    if dt in ("credit_memo",):
        adj_score += 2
    if dt in ("debit_note", "debit_memo"):
        settle_score += 2

    # If it references the same invoice AND has adjustment language,
    # it is almost certainly an adjustment.
    if references_invoice and adj_score > 0:
        adj_score += 1

    # Default: if no keywords match, a credit memo that references an
    # invoice is an adjustment; a standalone credit note is settlement.
    if adj_score == 0 and settle_score == 0:
        if references_invoice:
            adj_score = 1
        else:
            settle_score = 1

    if adj_score > settle_score:
        # Adjustment — compute tax reduction
        pre_tax, tax = _extract_credit_tax(total_num, tax_code)
        return CreditClassification(
            credit_type="adjustment",
            credit_total=total_num,
            pre_tax_reduction=pre_tax,
            tax_reduction=tax,
            reason=(
                f"Note de crédit classée comme ajustement (réduction du "
                f"montant imposable). Réduction CTI/RTI de {tax}$ requise. / "
                f"Credit memo classified as adjustment (reduces taxable "
                f"consideration). ITC/ITR reduction of ${tax} required."
            ),
        )
    else:
        return CreditClassification(
            credit_type="settlement",
            credit_total=total_num,
            pre_tax_reduction=None,
            tax_reduction=None,
            reason=(
                f"Note de crédit classée comme règlement (mode de paiement "
                f"appliqué contre le solde). Aucun ajustement CTI/RTI requis. / "
                f"Credit note classified as settlement (payment method "
                f"applied against balance). No ITC/ITR adjustment required."
            ),
        )


def _extract_credit_tax(
    total: Optional[float],
    tax_code: str,
) -> tuple[Optional[float], Optional[float]]:
    """
    Extract pre-tax and tax portions from a tax-included credit amount.

    Uses the tax engine's reverse formulas.  Returns (pre_tax, tax).
    """
    if total is None or total == 0:
        return (None, None)

    try:
        from decimal import Decimal as D
        from src.engines.tax_engine import (
            extract_tax_from_total,
            _registry_entry,
            _normalize_code,
            _round,
            _ONE,
        )

        tc = _normalize_code(tax_code)
        amt = D(str(abs(total)))

        if tc in ("T", "GST_QST", "M"):
            extracted = extract_tax_from_total(amt)
            return (
                round(float(extracted["pre_tax"]), 2),
                round(float(extracted["total_tax"]), 2),
            )
        elif tc in ("HST", "HST_ATL"):
            entry = _registry_entry(tc)
            hst_rate = entry["hst_rate"]
            pre_tax = _round(amt / (_ONE + hst_rate))
            tax = amt - pre_tax
            return (round(float(pre_tax), 2), round(float(tax), 2))
        else:
            return (round(float(amt), 2), 0.0)
    except Exception:
        return (round(abs(total), 2) if total else None, None)