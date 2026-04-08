"""
src/agents/core/hallucination_guard.py
======================================
Hallucination prevention and AI output verification for OtoCPA.

Public interface
----------------
verify_numeric_totals(result)
    Check that subtotal + GST + QST = total within $0.02 tolerance.
    Returns {"ok": bool, "delta": float, "skipped": bool, ...}

verify_ai_output(result, client_code="")
    Validate fields in an AI extraction result.
    Returns {"hallucination_suspected": bool, "review_status": str|None, "failures": [str]}

record_math_mismatch(document_id, delta, computed, claimed_total, *, db_path)
    Append a math_mismatch fraud flag to the document and set review_status=NeedsReview.

set_hallucination_suspected(document_id, failures, *, db_path)
    Mark document hallucination_suspected=1 and review_status=NeedsReview.

track_correction_count(document_id, before_row, submitted_fields, *, db_path)
    If more than CORRECTION_THRESHOLD fields were corrected, increment correction_count
    and write an audit_log entry.  Returns the number of changed fields.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_TAX_CODES: frozenset[str] = frozenset({
    "T", "Z", "E", "M", "I",
    "GST_QST", "HST", "HST_ATL", "VAT", "GENERIC_TAX", "NONE",
})
GL_ACCOUNT_RE = re.compile(r"^[A-Za-z0-9-]+$")

AMOUNT_MIN: float = 0.01
AMOUNT_MAX: float = 500_000.0
VENDOR_MIN_LEN: int = 2
VENDOR_MAX_LEN: int = 100
DATE_PAST_YEARS: int = 5
DATE_FUTURE_DAYS: int = 7
MATH_TOLERANCE: float = 0.02
CONFIDENCE_THRESHOLD: float = 0.7
CORRECTION_THRESHOLD: int = 2   # more than this many changed fields triggers the audit


# ---------------------------------------------------------------------------
# Numeric verification
# ---------------------------------------------------------------------------

def verify_numeric_totals(result: dict[str, Any]) -> dict[str, Any]:
    """
    Verify that subtotal + GST + QST = total within MATH_TOLERANCE ($0.02).

    Extracts GST and QST amounts from the ``taxes`` array.  If the taxes
    array is missing or empty, falls back to ``tax_total``.

    Returns
    -------
    dict with keys:
        ok            – True if totals match OR there is not enough data to check
        delta         – abs(computed - claimed_total)  (0.0 when skipped)
        skipped       – True when data is insufficient to run the check
        computed      – subtotal + gst + qst  (present when not skipped)
        claimed_total – the total field from the extraction (present when not skipped)
    """
    subtotal = result.get("subtotal")
    total = result.get("total")

    if subtotal is None or total is None:
        return {"ok": True, "delta": 0.0, "skipped": True}

    try:
        subtotal = float(subtotal)
        total = float(total)
    except (TypeError, ValueError):
        return {"ok": True, "delta": 0.0, "skipped": True}

    # Sum tax amounts from the taxes array
    # Recognise GST, QST, HST and their French equivalents TPS, TVQ, TVH
    _RECOGNISED_TAX_TYPES = frozenset({
        "GST", "QST", "HST", "TPS", "TVQ", "TVH",
    })
    taxes = result.get("taxes") or []
    gst_qst = 0.0
    has_gst_qst = False
    for entry in taxes:
        if not isinstance(entry, dict):
            continue
        t_type = str(entry.get("type", "")).upper()
        if t_type in _RECOGNISED_TAX_TYPES:
            try:
                gst_qst += float(entry.get("amount", 0))
                has_gst_qst = True
            except (TypeError, ValueError):
                pass

    if not has_gst_qst:
        # Fall back to tax_total when taxes array has no GST/QST entries
        tax_total_raw = result.get("tax_total")
        if tax_total_raw is None:
            return {"ok": True, "delta": 0.0, "skipped": True}
        try:
            gst_qst = float(tax_total_raw)
        except (TypeError, ValueError):
            return {"ok": True, "delta": 0.0, "skipped": True}

    computed = subtotal + gst_qst
    delta = abs(computed - total)
    return {
        "ok": delta <= MATH_TOLERANCE,
        "delta": round(delta, 4),
        "skipped": False,
        "computed": round(computed, 4),
        "claimed_total": round(total, 4),
    }


def verify_line_item_totals(
    document_id: str,
    invoice_total: float,
    *,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Check invoice line-item sum against the invoice total.

    Flags ``invoice_total_gap`` as a hallucination indicator if
    the gap exceeds $0.50.

    Returns
    -------
    dict with keys:
        ok              – True if line sum matches invoice total within $0.50
        line_sum        – sum of line pretax amounts
        tax_sum         – sum of line tax amounts
        computed_total  – line_sum + tax_sum
        invoice_total   – the claimed invoice total
        gap             – abs(computed_total - invoice_total)
        hallucination_flag – True if gap > $0.50
    """
    LINE_SUM_TOLERANCE = 0.50

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT line_total_pretax, gst_amount, qst_amount, hst_amount
               FROM invoice_lines WHERE document_id = ?""",
            (document_id,),
        ).fetchall()
        conn.close()
    except Exception:
        return {"ok": True, "line_sum": 0, "tax_sum": 0, "computed_total": 0,
                "invoice_total": invoice_total, "gap": 0, "hallucination_flag": False}

    if not rows:
        return {"ok": True, "line_sum": 0, "tax_sum": 0, "computed_total": 0,
                "invoice_total": invoice_total, "gap": 0, "hallucination_flag": False}

    line_sum = 0.0
    tax_sum = 0.0
    for r in rows:
        line_sum += float(r["line_total_pretax"] or 0)
        tax_sum += float(r["gst_amount"] or 0) + float(r["qst_amount"] or 0) + float(r["hst_amount"] or 0)

    computed = line_sum + tax_sum
    gap = abs(computed - invoice_total)
    is_hallucination = gap > LINE_SUM_TOLERANCE

    return {
        "ok": not is_hallucination,
        "line_sum": round(line_sum, 4),
        "tax_sum": round(tax_sum, 4),
        "computed_total": round(computed, 4),
        "invoice_total": round(invoice_total, 4),
        "gap": round(gap, 4),
        "hallucination_flag": is_hallucination,
    }


# ---------------------------------------------------------------------------
# Field validation
# ---------------------------------------------------------------------------

def _is_random_chars(s: str) -> bool:
    """Heuristic: string looks like random characters (no vowel, no space)."""
    if len(s) < 6:
        return False
    vowels = set("aeiouAEIOU ")
    return all(c not in vowels for c in s)


def verify_ai_output(result: dict[str, Any], client_code: str = "") -> dict[str, Any]:
    """
    Validate fields extracted by AI.

    Parameters
    ----------
    result:
        The raw dict returned by the AI extraction step.
    client_code:
        Optional client identifier for future per-client rules.

    Returns
    -------
    dict with keys:
        hallucination_suspected – bool
        review_status           – "NeedsReview" if any check failed, else None
        failures                – list of human-readable failure descriptions
    """
    failures: list[str] = []

    # --- 1. Vendor name -------------------------------------------------------
    vendor = result.get("vendor_name") or result.get("vendor") or ""
    vendor = str(vendor).strip()
    if not vendor:
        failures.append("vendor_name is empty")
    elif len(vendor) < VENDOR_MIN_LEN:
        failures.append(
            f"vendor_name too short ({len(vendor)} chars, min {VENDOR_MIN_LEN})"
        )
    elif len(vendor) > VENDOR_MAX_LEN:
        failures.append(
            f"vendor_name too long ({len(vendor)} chars, max {VENDOR_MAX_LEN})"
        )
    elif _is_random_chars(vendor):
        failures.append(f"vendor_name appears to be random characters: {vendor!r}")

    # --- 2. Amount ------------------------------------------------------------
    amount_raw = result.get("total") if result.get("total") is not None else result.get("amount")
    doc_type = str(result.get("document_type") or result.get("doc_type") or "").strip().lower()
    _NEGATIVE_ALLOWED_TYPES = {"credit_note", "refund", "chargeback", "reversal"}
    _ZERO_ALLOWED_TYPES = _NEGATIVE_ALLOWED_TYPES | {"informational"}
    if amount_raw is not None:
        try:
            amount = float(amount_raw)
            if amount < 0 and doc_type in _NEGATIVE_ALLOWED_TYPES:
                # Negative amounts are valid for credit notes, refunds, etc.
                if abs(amount) >= AMOUNT_MAX:
                    failures.append(
                        f"amount {amount} exceeds maximum ${AMOUNT_MAX:,.0f}"
                    )
            elif amount == 0 and doc_type in _ZERO_ALLOWED_TYPES:
                pass  # Zero is acceptable for informational/credit documents
            elif amount <= AMOUNT_MIN:
                failures.append(
                    f"amount {amount} is not positive or below minimum ${AMOUNT_MIN}"
                )
            elif amount >= AMOUNT_MAX:
                failures.append(
                    f"amount {amount} exceeds maximum ${AMOUNT_MAX:,.0f}"
                )
        except (TypeError, ValueError):
            failures.append(f"amount is not a valid number: {amount_raw!r}")

    # --- 3. Date --------------------------------------------------------------
    date_raw = result.get("document_date")
    if date_raw:
        try:
            doc_date = datetime.strptime(str(date_raw).strip(), "%Y-%m-%d").date()
            today = datetime.now(timezone.utc).date()
            past_limit = today.replace(year=today.year - DATE_PAST_YEARS)
            future_limit = today + timedelta(days=DATE_FUTURE_DAYS)
            if doc_date < past_limit:
                failures.append(
                    f"document_date {date_raw} is more than {DATE_PAST_YEARS} years in the past"
                )
            elif doc_date > future_limit:
                failures.append(
                    f"document_date {date_raw} is more than {DATE_FUTURE_DAYS} days in the future"
                )
        except ValueError:
            failures.append(
                f"document_date {date_raw!r} is not a valid YYYY-MM-DD date"
            )

    # --- 4. GL account --------------------------------------------------------
    gl_account = str(result.get("gl_account") or "").strip()
    if gl_account and not GL_ACCOUNT_RE.match(gl_account):
        failures.append(
            f"gl_account {gl_account!r} contains invalid characters "
            "(only letters, digits, dashes allowed)"
        )

    # --- 5. Tax code ----------------------------------------------------------
    tax_code = str(result.get("tax_code") or "").strip().upper()
    if tax_code and tax_code not in VALID_TAX_CODES:
        failures.append(
            f"tax_code {tax_code!r} is not valid "
            f"(expected one of {sorted(VALID_TAX_CODES)})"
        )

    # --- 6. Confidence --------------------------------------------------------
    confidence_raw = result.get("confidence")
    if confidence_raw is not None:
        try:
            confidence = float(confidence_raw)
            if confidence < CONFIDENCE_THRESHOLD:
                failures.append(
                    f"confidence {confidence:.2f} is below threshold {CONFIDENCE_THRESHOLD}"
                )
        except (TypeError, ValueError):
            pass

    hallucination_suspected = len(failures) > 0
    return {
        "hallucination_suspected": hallucination_suspected,
        "review_status": "NeedsReview" if hallucination_suspected else None,
        "failures": failures,
    }


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Math mismatch — fraud flag
# ---------------------------------------------------------------------------

def record_math_mismatch(
    document_id: str,
    delta: float,
    computed: float,
    claimed_total: float,
    *,
    db_path: Path = DB_PATH,
) -> None:
    """
    Append a ``math_mismatch`` fraud flag to the document and set
    review_status=NeedsReview.  Idempotent — duplicate math_mismatch
    flags are replaced, not accumulated.
    """
    flag: dict[str, Any] = {
        "rule":       "math_mismatch",
        "severity":   "high",
        "note": (
            f"Subtotal + taxes = {computed:.2f} but total = {claimed_total:.2f} "
            f"(delta {delta:.4f}, tolerance {MATH_TOLERANCE})"
        ),
        "flagged_at": _utc_now(),
    }
    try:
        conn = _open_db(db_path)
        try:
            row = conn.execute(
                "SELECT fraud_flags FROM documents WHERE document_id = ?",
                (document_id,),
            ).fetchone()
            if row is None:
                return
            existing: list[dict[str, Any]] = []
            if row["fraud_flags"]:
                try:
                    existing = json.loads(row["fraud_flags"])
                except (json.JSONDecodeError, TypeError):
                    existing = []
            # Replace any previous math_mismatch flag
            existing = [f for f in existing if f.get("rule") != "math_mismatch"]
            existing.append(flag)
            conn.execute(
                """UPDATE documents
                   SET fraud_flags    = ?,
                       review_status  = 'NeedsReview',
                       updated_at     = ?
                   WHERE document_id  = ?""",
                (json.dumps(existing, ensure_ascii=False), _utc_now(), document_id),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass  # never break the calling code


# ---------------------------------------------------------------------------
# Hallucination flag
# ---------------------------------------------------------------------------

def set_hallucination_suspected(
    document_id: str,
    failures: list[str],
    *,
    db_path: Path = DB_PATH,
) -> None:
    """
    Set hallucination_suspected=1 and review_status=NeedsReview on a document.
    Fire-and-forget; never raises.
    """
    try:
        conn = _open_db(db_path)
        try:
            conn.execute(
                """UPDATE documents
                   SET hallucination_suspected = 1,
                       review_status           = 'NeedsReview',
                       updated_at              = ?
                   WHERE document_id           = ?""",
                (_utc_now(), document_id),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Correction count tracking
# ---------------------------------------------------------------------------

def track_correction_count(
    document_id: str,
    before_row: Any,
    submitted_fields: dict[str, Any],
    *,
    db_path: Path = DB_PATH,
) -> int:
    """
    Count how many tracked fields were actually changed by this save.

    If the count exceeds CORRECTION_THRESHOLD (currently 2):
      - increment ``correction_count`` on the document
      - write an ``audit_log`` entry

    Parameters
    ----------
    before_row:
        sqlite3.Row (or dict-like) from get_document() before the update.
    submitted_fields:
        The dict of fields submitted by the accountant.

    Returns
    -------
    Number of changed fields.
    """
    tracked = {
        "vendor", "client_code", "doc_type", "amount", "document_date",
        "gl_account", "tax_code", "category", "review_status",
    }
    changed_fields: list[str] = []

    for field, new_val in submitted_fields.items():
        if field not in tracked:
            continue
        try:
            old = before_row[field]
        except (KeyError, IndexError, TypeError):
            continue
        old_str = str(old).strip() if old is not None else ""
        new_str = str(new_val).strip() if new_val is not None else ""
        if old_str != new_str:
            changed_fields.append(field)

    if len(changed_fields) > CORRECTION_THRESHOLD:
        try:
            conn = _open_db(db_path)
            try:
                now = _utc_now()
                conn.execute(
                    """UPDATE documents
                       SET correction_count = COALESCE(correction_count, 0) + 1,
                           updated_at       = ?
                       WHERE document_id    = ?""",
                    (now, document_id),
                )
                note = (
                    f"Accountant corrected {len(changed_fields)} fields: "
                    + ", ".join(changed_fields)
                )
                conn.execute(
                    """INSERT INTO audit_log
                       (event_type, document_id, task_type, prompt_snippet, created_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        "multi_field_correction",
                        document_id,
                        "document_update",
                        note[:500],
                        now,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass

    return len(changed_fields)


# =========================================================================
# PART 8 — Provenance-preserving uncertainty reasons
# =========================================================================

# Structured reason codes for manual review flags
UNCERTAINTY_REASON_CODES = {
    "VENDOR_NAME_CONFLICT": {
        "description_en": "Vendor name OCR ambiguity or alias detected",
        "description_fr": "Ambiguïté OCR du nom du fournisseur ou alias détecté",
    },
    "INVOICE_NUMBER_OCR_CONFLICT": {
        "description_en": "Invoice number has OCR ambiguity (O vs 0, I vs 1)",
        "description_fr": "Numéro de facture ambigu en OCR (O vs 0, I vs 1)",
    },
    "DATE_AMBIGUOUS": {
        "description_en": "Date format ambiguous — affects period classification",
        "description_fr": "Format de date ambigu — affecte la classification de période",
    },
    "ALLOCATION_GAP_UNEXPLAINED": {
        "description_en": "Invoice total exceeds documented component values",
        "description_fr": "Le total de la facture dépasse les valeurs documentées",
    },
    "TAX_REGISTRATION_INCOMPLETE": {
        "description_en": "Vendor GST/QST number missing or unverified",
        "description_fr": "Numéro TPS/TVQ du fournisseur manquant ou non vérifié",
    },
    "SETTLEMENT_STATE_UNRESOLVED": {
        "description_en": "Credit memo + bank deposit same amount same period",
        "description_fr": "Note de crédit + dépôt bancaire même montant même période",
    },
    "PAYEE_IDENTITY_UNPROVEN": {
        "description_en": "Bank payee differs from invoice vendor",
        "description_fr": "Le bénéficiaire bancaire diffère du fournisseur de la facture",
    },
    "CUSTOMS_NOTE_SCOPE_LIMITED": {
        "description_en": "Tax-paid-at-customs applies to goods portion only",
        "description_fr": "Taxe payée aux douanes s'applique à la portion des biens uniquement",
    },
}


def build_structured_review_reasons(
    reason_codes: list[str],
    context: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    """Build structured review reasons from reason codes.

    Never just says "manual review required" — always says WHY with
    structured codes and bilingual descriptions.
    """
    ctx = context or {}
    reasons = []
    for code in reason_codes:
        info = UNCERTAINTY_REASON_CODES.get(code, {})
        reasons.append({
            "reason_code": code,
            "description_en": info.get("description_en", f"Review required: {code}"),
            "description_fr": info.get("description_fr", f"Révision requise: {code}"),
            "evidence": ctx.get(code, ""),
        })
    return reasons


def flag_for_review_with_reasons(
    document_id: str,
    reason_codes: list[str],
    context: dict[str, str] | None = None,
    *,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Flag a document for review with structured reasons.

    Always includes WHY review is needed, never just "manual review required".
    """
    reasons = build_structured_review_reasons(reason_codes, context)

    review_notes = []
    for r in reasons:
        review_notes.append(
            f"[{r['reason_code']}] {r['description_en']} / {r['description_fr']}"
            + (f" | {r['evidence']}" if r.get("evidence") else "")
        )

    try:
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            import json
            now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            conn.execute(
                """UPDATE documents
                   SET review_status = 'NeedsReview',
                       review_notes = COALESCE(review_notes, '') || ?
                   WHERE document_id = ?""",
                ("\n".join(review_notes) + "\n", document_id),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass

    return {
        "document_id": document_id,
        "review_status": "NeedsReview",
        "structured_reasons": reasons,
        "review_notes": review_notes,
    }
