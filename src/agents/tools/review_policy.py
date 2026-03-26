from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass
class ReviewDecision:
    status: str
    reason: str
    effective_confidence: float
    review_notes: Optional[list[str]] = None


# ---------------------------------------------------------------------------
# Fraud flag integration
# ---------------------------------------------------------------------------

_BLOCKING_SEVERITIES = frozenset({"critical", "high"})
_FRAUD_CONFIDENCE_CAP = 0.60


def check_fraud_flags(fraud_flags: list[dict[str, Any]] | None) -> bool:
    """Return True (block) if any CRITICAL or HIGH fraud flag exists."""
    if not fraud_flags:
        return False
    for flag in fraud_flags:
        severity = str(flag.get("severity", "")).strip().lower()
        if severity in _BLOCKING_SEVERITIES:
            return True
    return False


def _parse_substance_flags(substance_flags: dict[str, Any] | None) -> dict[str, Any]:
    """Parse substance_flags from JSON string or dict."""
    if substance_flags is None:
        return {}
    if isinstance(substance_flags, str):
        import json
        try:
            substance_flags = json.loads(substance_flags)
        except (json.JSONDecodeError, TypeError):
            return {}
    if not isinstance(substance_flags, dict):
        return {}
    return substance_flags


# ---------------------------------------------------------------------------
# Substance flag confidence caps (BLOCK 5)
# ---------------------------------------------------------------------------

_SUBSTANCE_CONFIDENCE_CAPS: dict[str, float] = {
    "potential_capex": 0.70,
    "potential_customer_deposit": 0.60,
    "potential_intercompany": 0.60,
    "mixed_tax_invoice": 0.50,
}


def effective_confidence(
    rules_confidence: float,
    final_method: str,
    has_required: bool,
    ai_confidence: float = 0.0,
    fraud_flags: list[dict[str, Any]] | None = None,
    substance_flags: dict[str, Any] | None = None,
) -> float:
    """Compute effective confidence.

    Algorithm:
    - base = max(rules_confidence, ai_confidence) if both present, else whichever exists
    - If all required fields present: boost = min(0.10, 1.0 - base)
    - effective = base + boost
    - Cap at 1.0, floor at 0.0
    - If fraud flags present: cap at 0.60 maximum
    - Substance flag caps (BLOCK 5):
      - potential_capex=True → cap at 0.70
      - potential_customer_deposit=True → cap at 0.60
      - potential_intercompany=True → cap at 0.60
      - mixed_tax_invoice=True → cap at 0.50
    - Never return a fixed 0.85 regardless of base confidence
    """
    rc = float(rules_confidence or 0.0)
    ac = float(ai_confidence or 0.0)

    # base = best available confidence
    if rc > 0 and ac > 0:
        base = max(rc, ac)
    elif rc > 0:
        base = rc
    elif ac > 0:
        base = ac
    else:
        base = 0.0

    # Required-field boost: max +0.10 (or +0.05 if base < 0.80), never pushes above 1.0
    # FIX 24: Reduce max boost for low-confidence documents
    boost = 0.0
    if has_required:
        max_boost = 0.05 if base < 0.80 else 0.10
        boost = min(max_boost, 1.0 - base)

    eff = base + boost
    # Floor / cap
    eff = max(0.0, min(1.0, eff))

    # Fraud flag penalty: cap confidence at 0.60
    if fraud_flags and any(
        str(f.get("severity", "")).strip().lower() in _BLOCKING_SEVERITIES
        for f in fraud_flags
    ):
        eff = min(eff, _FRAUD_CONFIDENCE_CAP)

    # BLOCK 5: Substance flag confidence caps
    sf = _parse_substance_flags(substance_flags)
    for flag_key, cap_value in _SUBSTANCE_CONFIDENCE_CAPS.items():
        if sf.get(flag_key):
            eff = min(eff, cap_value)

    return eff


def check_substance_block(substance_flags: dict[str, Any] | None) -> bool:
    """Return True (block) if substance_flags has block_auto_approval=True."""
    if not substance_flags:
        return False
    return bool(substance_flags.get("block_auto_approval"))


def should_auto_approve(
    confidence: float,
    fraud_flags: list[dict[str, Any]] | None = None,
    substance_flags: dict[str, Any] | None = None,
) -> bool:
    """Return True only if confidence >= 0.85, no blocking fraud flags,
    AND no substance-based block.

    BLOCK 5: Also returns False if any substance flag is set that requires
    human review (capex, customer_deposit, intercompany, mixed_tax).
    """
    if check_fraud_flags(fraud_flags):
        return False
    # FIX 6: Enforce block_auto_approval from substance_flags
    sf = _parse_substance_flags(substance_flags)
    if sf.get("block_auto_approval"):
        return False
    # BLOCK 5: Any capped substance flag blocks auto-approval
    for flag_key in _SUBSTANCE_CONFIDENCE_CAPS:
        if sf.get(flag_key):
            return False
    return confidence >= 0.85


def decide_review_status(
    *,
    rules_confidence: float,
    final_method: str,
    vendor_name: Optional[str],
    total: Optional[float],
    document_date: Optional[str],
    client_code: Optional[str],
    fraud_flags: list[dict[str, Any]] | None = None,
    substance_flags: dict[str, Any] | None = None,
) -> ReviewDecision:
    review_notes: list[str] = []

    # Strip whitespace-only vendor → treat as missing
    has_vendor = bool(vendor_name and str(vendor_name).strip())
    has_total = total is not None
    has_date = bool(document_date)
    has_client = bool(client_code)

    # Validate date format (YYYY-MM-DD) — flag invalid but don't block
    if document_date is not None and has_date:
        try:
            datetime.strptime(str(document_date).strip(), "%Y-%m-%d")
        except ValueError:
            review_notes.append(f"invalid_date:{document_date}")

    has_required = has_vendor and has_total and has_date
    eff = effective_confidence(
        rules_confidence=rules_confidence,
        final_method=final_method,
        has_required=has_required,
        fraud_flags=fraud_flags,
        substance_flags=substance_flags,
    )

    # P2-4: Large amount escalation — >= $25,000 requires human review
    if total is not None and total >= 25000:
        eff = min(eff, 0.75)
        review_notes.append("large_amount_escalation")

    # FIX 25: Negative amount escalation — large credit notes always require review
    if total is not None and total < -5000:
        eff = min(eff, 0.65)
        review_notes.append("negative_amount_escalation")

    # Fraud flag blocking — always NeedsReview
    fraud_blocked = check_fraud_flags(fraud_flags)
    if fraud_blocked:
        review_notes.append("fraud_flags_block_auto_approval")

    # FIX 6 + BLOCK 5: Substance flags blocking — enforce block_auto_approval and caps
    sf = _parse_substance_flags(substance_flags)
    substance_blocked = bool(sf.get("block_auto_approval"))
    # BLOCK 5: Any capped substance flag also blocks
    for flag_key in _SUBSTANCE_CONFIDENCE_CAPS:
        if sf.get(flag_key):
            substance_blocked = True
            break
    if substance_blocked:
        review_notes.append("substance_flags_block_auto_approval")

    # BLOCK 5: mixed_tax_invoice always NeedsReview (handled below via substance_blocked)

    notes = review_notes if review_notes else None

    if not has_client:
        return ReviewDecision(
            status="NeedsReview",
            reason="missing_client_route",
            effective_confidence=eff,
            review_notes=notes,
        )

    if not has_vendor:
        return ReviewDecision(
            status="Exception",
            reason="missing_vendor",
            effective_confidence=eff,
            review_notes=notes,
        )

    if not has_total:
        return ReviewDecision(
            status="NeedsReview",
            reason="missing_total",
            effective_confidence=eff,
            review_notes=notes,
        )

    if not has_date:
        return ReviewDecision(
            status="NeedsReview",
            reason="missing_document_date",
            effective_confidence=eff,
            review_notes=notes,
        )

    # suspicious zero totals should be reviewed
    if total == 0:
        return ReviewDecision(
            status="NeedsReview",
            reason="zero_total",
            effective_confidence=eff,
            review_notes=notes,
        )

    # Fraud-flagged documents never auto-approved
    if fraud_blocked:
        return ReviewDecision(
            status="NeedsReview",
            reason="fraud_flags_present",
            effective_confidence=eff,
            review_notes=notes,
        )

    # FIX 6: Substance-blocked documents never auto-approved
    if substance_blocked:
        return ReviewDecision(
            status="NeedsReview",
            reason="substance_flags_block",
            effective_confidence=eff,
            review_notes=notes,
        )

    if eff >= 0.85:
        return ReviewDecision(
            status="Ready",
            reason="all_required_fields_present",
            effective_confidence=eff,
            review_notes=notes,
        )

    return ReviewDecision(
        status="NeedsReview",
        reason="low_effective_confidence",
        effective_confidence=eff,
        review_notes=notes,
    )


def validate_tax_extraction(
    *,
    subtotal: Optional[float],
    gst_amount: Optional[float],
    qst_amount: Optional[float],
    tax_code: Optional[str],
) -> list[str]:
    """FIX 5b: Cross-document tax validation.

    After extracting GST and QST from an invoice, compute expected values
    and flag mismatches (catches swapped GST/QST values).
    """
    warnings: list[str] = []
    if subtotal is None or subtotal == 0:
        return warnings

    tc = (tax_code or "").strip().upper()
    if tc not in ("T", "GST_QST"):
        return warnings

    expected_gst = subtotal * 0.05
    expected_qst = subtotal * 0.09975
    tolerance = 0.02

    if gst_amount is not None and abs(gst_amount - expected_gst) > tolerance:
        warnings.append("tax_extraction_mismatch")
    if qst_amount is not None and abs(qst_amount - expected_qst) > tolerance:
        warnings.append("tax_extraction_mismatch")

    # Deduplicate
    return list(dict.fromkeys(warnings))
