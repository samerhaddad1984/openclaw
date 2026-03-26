"""
src/engines/customs_engine.py — Deterministic tax resolution engine.

Covers:
- CBSA customs value determination (Customs Act Section 45)
- Import GST / QST calculation
- Remote services place of supply (ETA Section 142.1)
- Registration overlap & double-tax prevention
- Credit memo complete decomposition
- Multi-period tax event tracking
- Apportionment enforcement
- FIX 4: Bank of Canada FX rate validation

All monetary arithmetic uses Python Decimal.  No AI calls.
"""
from __future__ import annotations

import json
import logging
import secrets
import sqlite3
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ZERO = Decimal("0")
_ONE = Decimal("1")
CENT = Decimal("0.01")

GST_RATE = Decimal("0.05")
QST_RATE = Decimal("0.09975")
HST_RATE_ON = Decimal("0.13")
HST_RATE_ATL = Decimal("0.15")

PST_RATES: dict[str, Decimal] = {
    "BC": Decimal("0.07"),
    "MB": Decimal("0.07"),
    "SK": Decimal("0.06"),
}

HST_PROVINCES = frozenset({"ON", "NB", "NS", "NL", "PE"})
ATL_PROVINCES = frozenset({"NB", "NS", "NL", "PE"})
GST_ONLY_PROVINCES = frozenset({"AB", "NT", "NU", "YT"})


def _round(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _to_dec(value: Any) -> Decimal:
    if value is None or str(value).strip() == "":
        return _ZERO
    return Decimal(str(value))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# =========================================================================
# PART 1 — CBSA customs value determination
# =========================================================================

def calculate_customs_value(
    invoice_amount: Any,
    discount: Any,
    discount_type: str,
    discount_shown_on_invoice: bool,
    discount_is_conditional: bool,
    post_import_discount: bool,
) -> dict[str, Any]:
    """Determine customs value per Customs Act Section 45.

    Rules:
    - Discount shown on commercial invoice at time of import AND unconditional
      → customs value = discounted price.
    - Discount is conditional (volume, loyalty, future purchase)
      → customs value = undiscounted price.
    - Discount applied post-import → customs value = undiscounted price.

    Never returns "depends" — always deterministic.
    """
    inv = _to_dec(invoice_amount)
    disc = _to_dec(discount)

    # Determine if discount applies to customs value
    use_discounted = (
        discount_shown_on_invoice
        and not discount_is_conditional
        and not post_import_discount
    )

    if disc <= _ZERO:
        # No meaningful discount
        return {
            "customs_value": _round(inv),
            "discount_applied": False,
            "discount_amount": _ZERO,
            "undiscounted_price": _round(inv),
            "reasoning": "No discount to apply — customs value equals invoice amount.",
        }

    # Calculate discounted price based on discount type
    if discount_type == "percentage":
        discount_amount = _round(inv * disc / Decimal("100"))
    elif discount_type == "flat":
        discount_amount = _round(disc)
    else:
        # Unknown discount type — treat as flat
        discount_amount = _round(disc)

    discounted_price = _round(inv - discount_amount)

    if use_discounted:
        return {
            "customs_value": discounted_price,
            "discount_applied": True,
            "discount_amount": discount_amount,
            "undiscounted_price": _round(inv),
            "reasoning": (
                "Discount shown on commercial invoice, unconditional, and "
                "not post-import — customs value uses discounted price "
                f"(Section 45). Discount: ${discount_amount}."
            ),
        }

    # Discount does NOT reduce customs value
    reasons = []
    if discount_is_conditional:
        reasons.append(
            f"Discount is conditional ({discount_type}) — "
            "conditional discounts (volume, loyalty, future purchase) "
            "do not reduce customs value."
        )
    if post_import_discount:
        reasons.append(
            "Discount applied post-import — post-import discounts "
            "do not reduce customs value."
        )
    if not discount_shown_on_invoice:
        reasons.append(
            "Discount not shown on commercial invoice at time of import — "
            "customs value uses undiscounted price."
        )

    return {
        "customs_value": _round(inv),
        "discount_applied": False,
        "discount_amount": discount_amount,
        "undiscounted_price": _round(inv),
        "reasoning": " ".join(reasons) if reasons else "Discount excluded from customs value.",
    }


def calculate_import_gst(
    customs_value: Any,
    duties: Any,
    excise_taxes: Any,
) -> dict[str, Any]:
    """Calculate GST on imported goods.

    Import GST base = customs_value + duties + excise_taxes.
    GST = base * 5%.
    GST paid on imports is recoverable as ITC.
    """
    cv = _to_dec(customs_value)
    d = _to_dec(duties)
    ex = _to_dec(excise_taxes)

    gst_base = cv + d + ex
    gst_amount = _round(gst_base * GST_RATE)

    return {
        "gst_base": _round(gst_base),
        "gst_amount": gst_amount,
        "gst_rate": GST_RATE,
        "gst_recoverable_as_itc": True,
        "components": {
            "customs_value": _round(cv),
            "duties": _round(d),
            "excise_taxes": _round(ex),
        },
    }


def calculate_qst_on_import(
    customs_value: Any,
    duties: Any,
    gst_amount: Any,
) -> dict[str, Any]:
    """Calculate QST on imported goods for Quebec registrants.

    QST base = customs_value + duties + gst_amount (NOT just invoice value).
    QST = base * 9.975%.
    QST paid on imports is recoverable as ITR.
    """
    cv = _to_dec(customs_value)
    d = _to_dec(duties)
    gst = _to_dec(gst_amount)

    qst_base = cv + d + gst
    qst_amount = _round(qst_base * QST_RATE)

    return {
        "qst_base": _round(qst_base),
        "qst_amount": qst_amount,
        "qst_rate": QST_RATE,
        "qst_recoverable_as_itr": True,
        "components": {
            "customs_value": _round(cv),
            "duties": _round(d),
            "gst_amount": _round(gst),
        },
    }


# =========================================================================
# PART 3 — Remote services place of supply (deterministic)
# =========================================================================

# Province → tax regime mapping
_PROVINCE_TAX_REGIME: dict[str, dict[str, Any]] = {
    "QC": {"gst_rate": GST_RATE, "qst_rate": QST_RATE, "hst_rate": _ZERO, "pst_rate": _ZERO,
           "tax_regime": "GST_QST", "description": "GST 5% + QST 9.975%"},
    "ON": {"gst_rate": _ZERO, "qst_rate": _ZERO, "hst_rate": HST_RATE_ON, "pst_rate": _ZERO,
           "tax_regime": "HST", "description": "HST 13%"},
    "NB": {"gst_rate": _ZERO, "qst_rate": _ZERO, "hst_rate": HST_RATE_ATL, "pst_rate": _ZERO,
           "tax_regime": "HST", "description": "HST 15%"},
    "NS": {"gst_rate": _ZERO, "qst_rate": _ZERO, "hst_rate": HST_RATE_ATL, "pst_rate": _ZERO,
           "tax_regime": "HST", "description": "HST 15%"},
    "NL": {"gst_rate": _ZERO, "qst_rate": _ZERO, "hst_rate": HST_RATE_ATL, "pst_rate": _ZERO,
           "tax_regime": "HST", "description": "HST 15%"},
    "PE": {"gst_rate": _ZERO, "qst_rate": _ZERO, "hst_rate": HST_RATE_ATL, "pst_rate": _ZERO,
           "tax_regime": "HST", "description": "HST 15%"},
    "AB": {"gst_rate": GST_RATE, "qst_rate": _ZERO, "hst_rate": _ZERO, "pst_rate": _ZERO,
           "tax_regime": "GST_ONLY", "description": "GST 5% only"},
    "NT": {"gst_rate": GST_RATE, "qst_rate": _ZERO, "hst_rate": _ZERO, "pst_rate": _ZERO,
           "tax_regime": "GST_ONLY", "description": "GST 5% only"},
    "NU": {"gst_rate": GST_RATE, "qst_rate": _ZERO, "hst_rate": _ZERO, "pst_rate": _ZERO,
           "tax_regime": "GST_ONLY", "description": "GST 5% only"},
    "YT": {"gst_rate": GST_RATE, "qst_rate": _ZERO, "hst_rate": _ZERO, "pst_rate": _ZERO,
           "tax_regime": "GST_ONLY", "description": "GST 5% only"},
    "BC": {"gst_rate": GST_RATE, "qst_rate": _ZERO, "hst_rate": _ZERO, "pst_rate": Decimal("0.07"),
           "tax_regime": "GST_PST", "description": "GST 5% + PST 7% (PST non-recoverable)"},
    "MB": {"gst_rate": GST_RATE, "qst_rate": _ZERO, "hst_rate": _ZERO, "pst_rate": Decimal("0.07"),
           "tax_regime": "GST_PST", "description": "GST 5% + PST 7% (PST non-recoverable)"},
    "SK": {"gst_rate": GST_RATE, "qst_rate": _ZERO, "hst_rate": _ZERO, "pst_rate": Decimal("0.06"),
           "tax_regime": "GST_PST", "description": "GST 5% + PST 6% (PST non-recoverable)"},
}


def determine_remote_service_supply(
    service_type: str,
    vendor_location: str,
    recipient_location: str,
    benefit_location: str,
    recipient_is_registered: bool,
) -> dict[str, Any]:
    """Determine place of supply for remote services per ETA Section 142.1.

    Never returns "ambiguous" — either resolves deterministically or
    flags SUPPLY_LOCATION_REQUIRED with exact information needed.
    """
    vendor_loc = (vendor_location or "").strip().upper()
    recipient_loc = (recipient_location or "").strip().upper()
    benefit_loc = (benefit_location or "").strip().upper()

    # Determine effective location: benefit location overrides recipient
    # if they differ (ETA 142.1 — supply made where benefit received)
    if benefit_loc and benefit_loc != recipient_loc and benefit_loc in _PROVINCE_TAX_REGIME:
        effective_location = benefit_loc
        location_reasoning = (
            f"Benefit location ({benefit_loc}) differs from recipient location "
            f"({recipient_loc}) — using benefit location per ETA 142.1."
        )
    elif recipient_loc and recipient_loc in _PROVINCE_TAX_REGIME:
        effective_location = recipient_loc
        location_reasoning = f"Using recipient location ({recipient_loc})."
    elif benefit_loc and benefit_loc in _PROVINCE_TAX_REGIME:
        effective_location = benefit_loc
        location_reasoning = f"Recipient location unknown — using benefit location ({benefit_loc})."
    else:
        # Cannot determine — flag as unresolvable
        missing = []
        if not recipient_loc:
            missing.append("recipient_location (province code)")
        elif recipient_loc not in _PROVINCE_TAX_REGIME:
            missing.append(f"valid recipient_location ('{recipient_loc}' not recognized)")
        if not benefit_loc:
            missing.append("benefit_location (province code)")
        elif benefit_loc not in _PROVINCE_TAX_REGIME:
            missing.append(f"valid benefit_location ('{benefit_loc}' not recognized)")

        return {
            "resolved": False,
            "flag": "SUPPLY_LOCATION_REQUIRED",
            "information_needed": missing,
            "block_posting": True,
            "reasoning": (
                "Cannot determine place of supply — neither recipient location "
                "nor benefit location maps to a known Canadian province/territory."
            ),
        }

    regime = _PROVINCE_TAX_REGIME[effective_location]

    return {
        "resolved": True,
        "effective_location": effective_location,
        "tax_regime": regime["tax_regime"],
        "gst_rate": regime["gst_rate"],
        "qst_rate": regime["qst_rate"],
        "hst_rate": regime["hst_rate"],
        "pst_rate": regime["pst_rate"],
        "pst_recoverable": False if regime["pst_rate"] > _ZERO else None,
        "description": regime["description"],
        "reasoning": location_reasoning,
        "service_type": service_type,
        "vendor_location": vendor_loc,
        "recipient_location": recipient_loc,
        "benefit_location": benefit_loc,
        "recipient_is_registered": recipient_is_registered,
    }


# =========================================================================
# PART 4 — Registration overlap and double-tax prevention
# =========================================================================

def detect_registration_overlap(
    vendor_id: str,
    document_date: str,
    vendor_registration_date: str,
    prior_self_assessments: list[dict[str, Any]],
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Detect double-tax risk from vendor registration overlap.

    Scenario: vendor was unregistered, buyer self-assessed GST/QST.
    Vendor later registers retroactively and charges tax on invoices
    that were already self-assessed.
    """
    doc_date = document_date.strip() if document_date else ""
    reg_date = vendor_registration_date.strip() if vendor_registration_date else ""

    result: dict[str, Any] = {
        "vendor_id": vendor_id,
        "document_date": doc_date,
        "vendor_registration_date": reg_date,
        "double_tax_risk": False,
        "reversal_required": False,
        "amount_to_reverse": _ZERO,
        "amount_to_claim_from_vendor": _ZERO,
        "net_adjustment": _ZERO,
        "prior_period_correction_required": False,
        "reasoning": [],
    }

    if not doc_date or not reg_date:
        result["reasoning"].append(
            "Missing document_date or vendor_registration_date — cannot assess overlap."
        )
        return result

    # Rule 1: If document_date < vendor_registration_date, vendor should NOT
    # be charging tax (they were not registered at that time)
    if doc_date < reg_date:
        result["double_tax_risk"] = True
        result["reasoning"].append(
            f"Document date ({doc_date}) is before vendor registration date "
            f"({reg_date}) — vendor was not registered and should not charge "
            f"GST/QST on this invoice."
        )

    # Rule 3-4: Check prior self-assessments for same vendor and period
    matching_assessments = [
        sa for sa in prior_self_assessments
        if sa.get("vendor_id") == vendor_id
    ]

    if matching_assessments:
        result["reversal_required"] = True
        total_self_assessed = _ZERO
        for sa in matching_assessments:
            total_self_assessed += _to_dec(sa.get("gst_amount", 0))
            total_self_assessed += _to_dec(sa.get("qst_amount", 0))

        result["amount_to_reverse"] = _round(total_self_assessed)
        result["reasoning"].append(
            f"Found {len(matching_assessments)} prior self-assessment(s) "
            f"for vendor {vendor_id} totalling ${total_self_assessed}. "
            f"These must be reversed."
        )

    # Rule 5-6: Calculate vendor-charged tax from the document
    # Look up the document's tax amounts if available
    try:
        row = conn.execute(
            """SELECT COALESCE(gst_amount, 0) AS gst,
                      COALESCE(qst_amount, 0) AS qst
               FROM documents
               WHERE vendor = ? AND document_date = ?
               LIMIT 1""",
            (vendor_id, doc_date),
        ).fetchone()
        if row:
            vendor_charged = _to_dec(row[0]) + _to_dec(row[1])
            result["amount_to_claim_from_vendor"] = _round(vendor_charged)
        else:
            vendor_charged = _ZERO
    except Exception:
        vendor_charged = _ZERO

    # Rule 6-7: Net adjustment
    net_adj = vendor_charged - result["amount_to_reverse"]
    result["net_adjustment"] = _round(net_adj)

    if net_adj != _ZERO:
        result["prior_period_correction_required"] = True
        result["reasoning"].append(
            f"Net adjustment required: ${net_adj} "
            f"(vendor charged ${vendor_charged} - reversed self-assessment "
            f"${result['amount_to_reverse']})."
        )

    return result


# =========================================================================
# PART 5 — Credit memo complete decomposition
# =========================================================================

def decompose_credit_memo(
    credit_memo_amount: Any,
    original_invoice_id: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Decompose a credit memo into its tax components.

    Links credit memo to original invoice, proportionally allocates
    across line items, and calculates per-tax-regime adjustments.

    Never returns partial decomposition — full or flagged as orphan.
    """
    cm_amount = _to_dec(credit_memo_amount)

    # Try to load original invoice lines
    try:
        lines = conn.execute(
            """SELECT line_number, description, line_total_pretax,
                      tax_code, tax_regime, gst_amount, qst_amount, hst_amount
               FROM invoice_lines
               WHERE document_id = ?
               ORDER BY line_number""",
            (original_invoice_id,),
        ).fetchall()
    except Exception:
        lines = []

    if not lines:
        # Check if original invoice exists at all
        try:
            doc = conn.execute(
                "SELECT document_id FROM documents WHERE document_id = ?",
                (original_invoice_id,),
            ).fetchone()
        except Exception:
            doc = None

        if not doc:
            return {
                "decomposed": False,
                "flag": "orphan_credit_memo_undecomposable",
                "credit_memo_amount": _round(cm_amount),
                "original_invoice_id": original_invoice_id,
                "reasoning": (
                    f"Original invoice '{original_invoice_id}' not found. "
                    f"Cannot decompose credit memo — manual decomposition required."
                ),
                "block_posting": True,
                "manual_decomposition_required": True,
            }

        # Invoice exists but has no line items — use document-level tax info
        try:
            doc_row = conn.execute(
                """SELECT amount, tax_code, COALESCE(gst_amount, 0) AS gst,
                          COALESCE(qst_amount, 0) AS qst,
                          COALESCE(hst_amount, 0) AS hst
                   FROM documents WHERE document_id = ?""",
                (original_invoice_id,),
            ).fetchone()
        except Exception:
            doc_row = None

        if not doc_row or not doc_row[0]:
            return {
                "decomposed": False,
                "flag": "orphan_credit_memo_undecomposable",
                "credit_memo_amount": _round(cm_amount),
                "original_invoice_id": original_invoice_id,
                "reasoning": (
                    f"Original invoice '{original_invoice_id}' has no line items "
                    f"and no amount — cannot decompose credit memo."
                ),
                "block_posting": True,
                "manual_decomposition_required": True,
            }

        # Single-line decomposition from document-level data
        orig_amount = _to_dec(doc_row[0])
        if orig_amount == _ZERO:
            ratio = _ZERO
        else:
            ratio = abs(cm_amount) / abs(orig_amount)

        orig_gst = _to_dec(doc_row[2])
        orig_qst = _to_dec(doc_row[3])
        orig_hst = _to_dec(doc_row[4])

        return {
            "decomposed": True,
            "credit_memo_amount": _round(cm_amount),
            "original_invoice_id": original_invoice_id,
            "ratio": _round(ratio * Decimal("10000")) / Decimal("10000"),
            "gst_portion_of_credit": _round(orig_gst * ratio),
            "qst_portion_of_credit": _round(orig_qst * ratio),
            "hst_portion_of_credit": _round(orig_hst * ratio),
            "self_assessed_reversal": _ZERO,
            "prior_period_adjustment": _ZERO,
            "line_decomposition": [],
            "reasoning": "Decomposed from document-level tax data (no line items).",
        }

    # Full line-level decomposition
    total_pretax = _ZERO
    for line in lines:
        total_pretax += _to_dec(line[2])

    line_decomposition = []
    total_gst_credit = _ZERO
    total_qst_credit = _ZERO
    total_hst_credit = _ZERO

    for line in lines:
        line_pretax = _to_dec(line[2])
        if total_pretax == _ZERO:
            share = _ZERO
        else:
            share = line_pretax / total_pretax

        line_credit = _round(abs(cm_amount) * share)
        line_gst = _to_dec(line[5])
        line_qst = _to_dec(line[6])
        line_hst = _to_dec(line[7])

        # Tax credit proportional to credit memo share
        gst_credit = _round(line_gst * share * abs(cm_amount) / abs(total_pretax)) if total_pretax != _ZERO else _ZERO
        qst_credit = _round(line_qst * share * abs(cm_amount) / abs(total_pretax)) if total_pretax != _ZERO else _ZERO
        hst_credit = _round(line_hst * share * abs(cm_amount) / abs(total_pretax)) if total_pretax != _ZERO else _ZERO

        total_gst_credit += gst_credit
        total_qst_credit += qst_credit
        total_hst_credit += hst_credit

        line_decomposition.append({
            "line_number": line[0],
            "description": line[1],
            "original_pretax": _round(line_pretax),
            "credit_share": _round(share * Decimal("10000")) / Decimal("10000"),
            "credit_amount": line_credit,
            "tax_regime": line[4] or "",
            "gst_credit": gst_credit,
            "qst_credit": qst_credit,
            "hst_credit": hst_credit,
        })

    return {
        "decomposed": True,
        "credit_memo_amount": _round(cm_amount),
        "original_invoice_id": original_invoice_id,
        "gst_portion_of_credit": total_gst_credit,
        "qst_portion_of_credit": total_qst_credit,
        "hst_portion_of_credit": total_hst_credit,
        "self_assessed_reversal": _ZERO,
        "prior_period_adjustment": _ZERO,
        "line_decomposition": line_decomposition,
        "reasoning": (
            f"Credit memo fully decomposed across {len(lines)} line items "
            f"proportional to original pretax amounts."
        ),
    }


# =========================================================================
# PART 6 — Multi-period tax timing (tax_events)
# =========================================================================

def _ensure_tax_events_table(conn: sqlite3.Connection) -> None:
    """Create tax_events table if missing."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tax_events (
            event_id         TEXT PRIMARY KEY,
            document_id      TEXT NOT NULL,
            event_type       TEXT NOT NULL,
            amount           REAL NOT NULL,
            tax_code         TEXT NOT NULL,
            reporting_period TEXT NOT NULL,
            incurrence_date  TEXT NOT NULL,
            claim_date       TEXT NOT NULL,
            status           TEXT NOT NULL DEFAULT 'pending',
            created_at       TEXT NOT NULL DEFAULT ''
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_tax_events_doc "
        "ON tax_events(document_id)"
    )


def create_tax_event(
    document_id: str,
    event_type: str,
    amount: Any,
    tax_code: str,
    reporting_period: str,
    incurrence_date: str,
    claim_date: str,
    conn: sqlite3.Connection,
) -> str:
    """Create a tax event tracking incurrence vs claim timing.

    Tax events track two dates:
    - incurrence_date: when tax obligation arose
    - claim_date: which reporting period the ITC/ITR is claimed in

    Returns the event_id.
    """
    _ensure_tax_events_table(conn)

    event_id = f"TE-{secrets.token_hex(8)}"
    amt = float(_to_dec(amount))

    conn.execute(
        """INSERT INTO tax_events
           (event_id, document_id, event_type, amount, tax_code,
            reporting_period, incurrence_date, claim_date, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
        (
            event_id, document_id, event_type, amt, tax_code,
            reporting_period, incurrence_date, claim_date, _utc_now_iso(),
        ),
    )
    conn.commit()
    return event_id


def get_tax_events(
    document_id: str,
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Retrieve all tax events for a document."""
    _ensure_tax_events_table(conn)
    rows = conn.execute(
        """SELECT event_id, document_id, event_type, amount, tax_code,
                  reporting_period, incurrence_date, claim_date, status
           FROM tax_events WHERE document_id = ?
           ORDER BY incurrence_date""",
        (document_id,),
    ).fetchall()

    events = []
    for r in rows:
        event = {
            "event_id": r[0],
            "document_id": r[1],
            "event_type": r[2],
            "amount": r[3],
            "tax_code": r[4],
            "reporting_period": r[5],
            "incurrence_date": r[6],
            "claim_date": r[7],
            "status": r[8],
        }
        # Flag timing mismatches
        if r[6] != r[7]:
            event["timing_mismatch"] = True
            event["timing_note"] = (
                f"Tax incurred {r[6]} but claimed in period {r[7]}."
            )
        else:
            event["timing_mismatch"] = False
        events.append(event)

    return events


def update_tax_event_status(
    event_id: str,
    new_status: str,
    conn: sqlite3.Connection,
) -> bool:
    """Update a tax event status (claimed/reversed/pending)."""
    _ensure_tax_events_table(conn)
    valid_statuses = {"pending", "claimed", "reversed"}
    if new_status not in valid_statuses:
        return False

    cursor = conn.execute(
        "UPDATE tax_events SET status = ? WHERE event_id = ?",
        (new_status, event_id),
    )
    conn.commit()
    return cursor.rowcount > 0


# =========================================================================
# PART 7 — Apportionment enforcement
# =========================================================================

def _ensure_apportionment_table(conn: sqlite3.Connection) -> None:
    """Create apportionment_records table if missing."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS apportionment_records (
            record_id           TEXT PRIMARY KEY,
            document_id         TEXT NOT NULL,
            gross_itc           REAL NOT NULL,
            gross_itr           REAL NOT NULL,
            apportionment_rate  REAL NOT NULL,
            apportionment_basis TEXT NOT NULL,
            net_itc             REAL NOT NULL,
            net_itr             REAL NOT NULL,
            created_at          TEXT NOT NULL DEFAULT ''
        )
    """)


def _ensure_audit_log_table(conn: sqlite3.Connection) -> None:
    """Create audit_log table if missing."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type      TEXT NOT NULL,
            document_id     TEXT,
            task_type       TEXT,
            prompt_snippet  TEXT,
            created_at      TEXT NOT NULL DEFAULT ''
        )
    """)


def enforce_apportionment(
    document_id: str,
    apportionment_rate: Any,
    apportionment_basis: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Enforce ITC/ITR apportionment for mixed business/personal use.

    Rules:
    1. Apportionment must be explicitly set — default is NOT 100%.
    2. If rate not set for vendor with personal use history: block claim.
    3. Apply rate to ALL tax components.
    4. Log to audit_log.
    """
    _ensure_apportionment_table(conn)
    _ensure_audit_log_table(conn)

    rate = _to_dec(apportionment_rate)

    # Rule 1: Apportionment must be explicitly set
    if rate <= _ZERO or rate > _ONE:
        return {
            "applied": False,
            "apportionment_required": True,
            "block_itc_itr_claim": True,
            "document_id": document_id,
            "reasoning": (
                "Apportionment rate must be explicitly set between 0 and 1 "
                "(exclusive of 0). Cannot default to 100% — personal use "
                "portion must be determined."
            ),
        }

    # Load document tax amounts
    try:
        doc = conn.execute(
            """SELECT COALESCE(gst_amount, 0) AS gst,
                      COALESCE(qst_amount, 0) AS qst,
                      COALESCE(hst_amount, 0) AS hst
               FROM documents WHERE document_id = ?""",
            (document_id,),
        ).fetchone()
    except Exception:
        doc = None

    if not doc:
        return {
            "applied": False,
            "apportionment_required": True,
            "block_itc_itr_claim": True,
            "document_id": document_id,
            "reasoning": f"Document '{document_id}' not found.",
        }

    gross_gst = _to_dec(doc[0])
    gross_qst = _to_dec(doc[1])
    gross_hst = _to_dec(doc[2])
    gross_itc = gross_gst + gross_hst
    gross_itr = gross_qst

    net_itc = _round(gross_itc * rate)
    net_itr = _round(gross_itr * rate)

    # Store apportionment record
    record_id = f"AP-{secrets.token_hex(8)}"
    now = _utc_now_iso()

    conn.execute(
        """INSERT INTO apportionment_records
           (record_id, document_id, gross_itc, gross_itr,
            apportionment_rate, apportionment_basis, net_itc, net_itr, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            record_id, document_id,
            float(gross_itc), float(gross_itr),
            float(rate), apportionment_basis,
            float(net_itc), float(net_itr), now,
        ),
    )

    # Audit log
    conn.execute(
        """INSERT INTO audit_log
           (event_type, document_id, task_type, prompt_snippet, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (
            "apportionment_applied",
            document_id,
            "tax_apportionment",
            (
                f"Apportionment rate {float(rate)*100:.1f}% applied. "
                f"Basis: {apportionment_basis}. "
                f"Gross ITC: ${gross_itc}, Net ITC: ${net_itc}. "
                f"Gross ITR: ${gross_itr}, Net ITR: ${net_itr}."
            )[:500],
            now,
        ),
    )
    conn.commit()

    return {
        "applied": True,
        "document_id": document_id,
        "apportionment_rate": float(rate),
        "apportionment_basis": apportionment_basis,
        "gross_itc": gross_itc,
        "gross_itr": gross_itr,
        "net_itc": net_itc,
        "net_itr": net_itr,
        "disallowed_itc": _round(gross_itc - net_itc),
        "disallowed_itr": _round(gross_itr - net_itr),
        "record_id": record_id,
        "reasoning": (
            f"Apportionment applied at {float(rate)*100:.1f}% "
            f"({apportionment_basis}). "
            f"GST/HST recovery reduced from ${gross_itc} to ${net_itc}. "
            f"QST recovery reduced from ${gross_itr} to ${net_itr}."
        ),
    }


# =========================================================================
# PART 2 — False precision prevention on goods/service allocation
# =========================================================================

_SERVICE_KEYWORDS = frozenset({
    "service", "services", "consulting", "conseil", "installation",
    "maintenance", "entretien", "repair", "réparation", "reparation",
    "labour", "labor", "main-d'oeuvre", "main d'oeuvre",
    "professional", "professionnel",
})

_SHIPPING_KEYWORDS = frozenset({
    "shipping", "freight", "livraison", "transport", "expédition",
    "expedition", "delivery", "fret", "courier", "messagerie",
})

_INSURANCE_KEYWORDS = frozenset({
    "insurance", "assurance", "coverage", "couverture", "prime",
    "premium",
})

_CUSTOMS_KEYWORDS = frozenset({
    "customs", "douane", "duty", "duties", "droit", "droits",
    "tariff", "tarif", "brokerage", "courtage",
})

_DISCOUNT_KEYWORDS = frozenset({
    "discount", "escompte", "rabais", "remise", "reduction",
    "réduction",
})


def analyze_allocation_gap(
    invoice_total: Any,
    cbsa_goods_value: Any,
    invoice_text: str = "",
) -> dict[str, Any]:
    """Analyze gap between invoice total and CBSA goods value.

    When invoice total exceeds documented CBSA goods value:
    1. Calculate gap
    2. Do NOT hard-allocate gap to services
    3. Return allocation_gap_unproven=True with possible_components
    4. Set allocation_confidence = 0.50 (uncertain)
    5. Require human confirmation before posting any allocation
    """
    total = _to_dec(invoice_total)
    goods = _to_dec(cbsa_goods_value)

    if total <= goods:
        return {
            "allocation_gap_unproven": False,
            "gap": _ZERO,
            "allocation_confidence": 1.0,
            "reasoning": "Invoice total does not exceed CBSA goods value — no gap to explain.",
        }

    gap = _round(total - goods)
    text_lower = invoice_text.lower() if invoice_text else ""
    words = set(text_lower.split())

    possible_components = []

    if words & _SERVICE_KEYWORDS:
        possible_components.append({
            "component": "service_component",
            "possible": True,
            "keywords_found": sorted(words & _SERVICE_KEYWORDS),
        })
    if words & _SHIPPING_KEYWORDS:
        possible_components.append({
            "component": "shipping_component",
            "possible": True,
            "keywords_found": sorted(words & _SHIPPING_KEYWORDS),
        })
    if words & _INSURANCE_KEYWORDS:
        possible_components.append({
            "component": "insurance_component",
            "possible": True,
            "keywords_found": sorted(words & _INSURANCE_KEYWORDS),
        })
    if words & _CUSTOMS_KEYWORDS:
        possible_components.append({
            "component": "customs_charges",
            "possible": True,
            "keywords_found": sorted(words & _CUSTOMS_KEYWORDS),
        })
    if words & _DISCOUNT_KEYWORDS:
        possible_components.append({
            "component": "bundled_discount",
            "possible": True,
            "keywords_found": sorted(words & _DISCOUNT_KEYWORDS),
        })

    return {
        "allocation_gap_unproven": True,
        "gap": gap,
        "invoice_total": _round(total),
        "cbsa_goods_value": _round(goods),
        "possible_components": possible_components,
        "allocation_confidence": 0.50,
        "requires_human_confirmation": True,
        "reasoning": (
            f"Invoice total (${total}) exceeds CBSA goods value (${goods}) "
            f"by ${gap}. Gap NOT hard-allocated — {len(possible_components)} "
            f"possible component(s) identified. Human confirmation required."
        ),
    }


# =========================================================================
# PART 6 — Customs note scope limitation
# =========================================================================

_CUSTOMS_NOTE_PATTERNS = [
    "tax paid at customs",
    "taxe payée aux douanes",
    "taxes payées aux douanes",
    "douane",
    "customs cleared",
    "dédouané",
    "dedouane",
]


def check_customs_note_scope(
    document_text: str,
    cbsa_goods_value: Any,
    invoice_total: Any,
) -> dict[str, Any]:
    """When customs note appears, limit tax-paid-at-customs to goods only.

    1. Apply tax-paid-at-customs treatment ONLY to goods component
    2. Do NOT generalize to entire invoice
    3. Service component still requires separate GST/QST analysis
    4. Flag customs_note_scope_limited=True
    """
    text_lower = (document_text or "").lower()
    goods = _to_dec(cbsa_goods_value)
    total = _to_dec(invoice_total)

    has_customs_note = any(p in text_lower for p in _CUSTOMS_NOTE_PATTERNS)

    if not has_customs_note:
        return {
            "customs_note_scope_limited": False,
            "customs_note_found": False,
            "reasoning": "No customs note found in document.",
        }

    service_component = _round(total - goods) if total > goods else _ZERO

    return {
        "customs_note_scope_limited": True,
        "customs_note_found": True,
        "goods_value_customs_treated": _round(goods),
        "service_component_untreated": service_component,
        "invoice_total": _round(total),
        "requires_separate_gst_qst_analysis": service_component > _ZERO,
        "reasoning": (
            f"Customs note found — tax-paid-at-customs applies ONLY to goods "
            f"portion (${goods}). "
            + (
                f"Service component (${service_component}) requires separate "
                f"GST/QST analysis."
                if service_component > _ZERO
                else "No service component detected."
            )
        ),
        "note_fr": (
            f"Note douanière — le traitement de taxes payées aux douanes "
            f"s'applique UNIQUEMENT à la portion des biens (${goods})."
        ),
        "note_en": (
            f"Customs note — tax-paid-at-customs treatment applies ONLY to "
            f"goods portion (${goods})."
        ),
    }


# =========================================================================
# FIX 4 — Bank of Canada FX rate validation
# =========================================================================

def _ensure_boc_fx_rates_table(conn: sqlite3.Connection) -> None:
    """Create boc_fx_rates cache table if missing."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS boc_fx_rates (
            rate_date   TEXT PRIMARY KEY,
            usd_cad     REAL NOT NULL,
            fetched_at  TEXT NOT NULL DEFAULT ''
        )
    """)


def fetch_boc_rate(transaction_date: str, conn: sqlite3.Connection) -> Decimal | None:
    """Fetch Bank of Canada USD/CAD rate for a given date.

    Checks cache first; fetches from BoC Valet API if not cached.
    Returns None if rate unavailable (no internet, API error, etc.).
    """
    _ensure_boc_fx_rates_table(conn)
    date_str = transaction_date.strip()[:10]  # YYYY-MM-DD

    # Check cache first
    cached = conn.execute(
        "SELECT usd_cad FROM boc_fx_rates WHERE rate_date = ?",
        (date_str,),
    ).fetchone()
    if cached:
        return Decimal(str(cached[0] if isinstance(cached, (tuple, list)) else cached["usd_cad"]))

    # Fetch from Bank of Canada Valet API
    try:
        import urllib.request
        url = (
            f"https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json"
            f"?start_date={date_str}&end_date={date_str}"
        )
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        observations = data.get("observations", [])
        if observations:
            rate_val = observations[0].get("FXUSDCAD", {}).get("v")
            if rate_val:
                rate = Decimal(str(rate_val))
                # Cache the rate
                conn.execute(
                    "INSERT OR REPLACE INTO boc_fx_rates (rate_date, usd_cad, fetched_at) "
                    "VALUES (?, ?, ?)",
                    (date_str, float(rate), _utc_now_iso()),
                )
                conn.commit()
                return rate

        # No observation for this date (weekend/holiday) — try nearby dates
        # Look back up to 5 days for the most recent rate
        from datetime import timedelta
        base_date = datetime.strptime(date_str, "%Y-%m-%d")
        for offset in range(1, 6):
            fallback_date = (base_date - timedelta(days=offset)).strftime("%Y-%m-%d")
            fb_cached = conn.execute(
                "SELECT usd_cad FROM boc_fx_rates WHERE rate_date = ?",
                (fallback_date,),
            ).fetchone()
            if fb_cached:
                return Decimal(str(fb_cached[0] if isinstance(fb_cached, (tuple, list)) else fb_cached["usd_cad"]))

        # Try fetching a range
        start = (base_date - timedelta(days=5)).strftime("%Y-%m-%d")
        url2 = (
            f"https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json"
            f"?start_date={start}&end_date={date_str}"
        )
        req2 = urllib.request.Request(url2, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req2, timeout=10) as resp2:
            data2 = json.loads(resp2.read().decode("utf-8"))
        obs2 = data2.get("observations", [])
        if obs2:
            # Take the most recent rate
            last_obs = obs2[-1]
            rate_val = last_obs.get("FXUSDCAD", {}).get("v")
            obs_date = last_obs.get("d", date_str)
            if rate_val:
                rate = Decimal(str(rate_val))
                conn.execute(
                    "INSERT OR REPLACE INTO boc_fx_rates (rate_date, usd_cad, fetched_at) "
                    "VALUES (?, ?, ?)",
                    (obs_date, float(rate), _utc_now_iso()),
                )
                conn.commit()
                return rate

    except Exception as exc:
        _log.debug("BoC FX rate fetch failed for %s: %s", date_str, exc)

    return None


def validate_fx_rate(
    amount_usd: Any,
    amount_cad: Any,
    transaction_date: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Validate that a USD→CAD conversion matches the Bank of Canada rate.

    If the document's implicit rate differs from the BoC rate by more than 2%,
    flags fx_rate_deviation with the BoC rate as reference.
    """
    usd = _to_dec(amount_usd)
    cad = _to_dec(amount_cad)

    if usd <= _ZERO or cad <= _ZERO:
        return {
            "validated": False,
            "reason": "Missing or zero USD/CAD amounts.",
        }

    doc_rate = cad / usd
    boc_rate = fetch_boc_rate(transaction_date, conn)

    if boc_rate is None:
        return {
            "validated": False,
            "doc_rate": _round(doc_rate * Decimal("10000")) / Decimal("10000"),
            "boc_rate": None,
            "reason": "Bank of Canada rate unavailable — cannot validate.",
            "fallback": True,
        }

    deviation = abs(doc_rate - boc_rate) / boc_rate
    deviation_pct = _round(deviation * Decimal("100"))

    if deviation > Decimal("0.02"):
        return {
            "validated": True,
            "flag": "fx_rate_deviation",
            "severity": "high" if deviation > Decimal("0.05") else "medium",
            "doc_rate": _round(doc_rate * Decimal("10000")) / Decimal("10000"),
            "boc_rate": _round(boc_rate * Decimal("10000")) / Decimal("10000"),
            "deviation_pct": float(deviation_pct),
            "amount_usd": _round(usd),
            "amount_cad": _round(cad),
            "expected_cad": _round(usd * boc_rate),
            "transaction_date": transaction_date,
            "reasoning": (
                f"Document FX rate ({doc_rate:.4f}) differs from Bank of Canada "
                f"rate ({boc_rate:.4f}) by {deviation_pct}%. "
                f"Expected CAD amount: ${_round(usd * boc_rate)}. "
                f"Actual CAD amount: ${cad}."
            ),
            "note_fr": (
                f"Écart de taux de change : taux du document ({doc_rate:.4f}) "
                f"diffère du taux de la Banque du Canada ({boc_rate:.4f}) "
                f"de {deviation_pct} %."
            ),
        }

    return {
        "validated": True,
        "flag": None,
        "doc_rate": _round(doc_rate * Decimal("10000")) / Decimal("10000"),
        "boc_rate": _round(boc_rate * Decimal("10000")) / Decimal("10000"),
        "deviation_pct": float(deviation_pct),
        "reasoning": (
            f"FX rate validated — deviation {deviation_pct}% within 2% tolerance."
        ),
    }
