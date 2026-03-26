"""
src/engines/line_item_engine.py — Line-level invoice parsing engine.

Extracts individual line items from multi-line invoices, determines per-line
tax regime (GST/QST, HST, GST-only, exempt), calculates tax amounts, and
reconciles line totals against the invoice total.

All monetary arithmetic uses Python Decimal.  AI calls are limited to
the ``extract_invoice_lines`` function which uses the OpenRouter client.

Public interface
----------------
extract_invoice_lines(document_id, raw_ocr_text, conn)
detect_tax_included_per_line(line)
determine_place_of_supply(line, vendor_province, buyer_province)
assign_line_tax_regime(line, place_of_supply)
calculate_line_tax(line, tax_regime, is_tax_included)
reconcile_invoice_lines(document_id, conn)
allocate_deposit_proportionally(document_id, deposit_amount, conn)
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"

# ---------------------------------------------------------------------------
# Tax rate constants (mirrors tax_engine.py)
# ---------------------------------------------------------------------------

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HALF = Decimal("0.5")
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

# ---------------------------------------------------------------------------
# Prompt template
# ---------------------------------------------------------------------------

_PROMPT_PATH = Path(__file__).resolve().parent.parent / "agents" / "prompts" / "extract_invoice_lines.txt"


def _load_prompt() -> str:
    try:
        return _PROMPT_PATH.read_text(encoding="utf-8")
    except Exception:
        return (
            "Extract every line item from this invoice as JSON. "
            "Include shipping, fees, adjustments. Return JSON only."
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _round(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _to_dec(value: Any) -> Decimal:
    if value is None or str(value).strip() == "":
        return _ZERO
    return Decimal(str(value))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# TAX-INCLUDED DETECTION
# ---------------------------------------------------------------------------

_TAX_INCLUDED_KW = re.compile(
    r"tax\s*incl|taxes?\s*incluses?|ttc|incl\.?\s*hst|incl\.?\s*tax|"
    r"toutes\s*taxes\s*comprises",
    re.IGNORECASE,
)
_TAX_EXCLUDED_KW = re.compile(
    r"before\s*tax|avant\s*taxes?|excl\.?\s*tax|net\b|ht\b|hors\s*taxes?",
    re.IGNORECASE,
)


def detect_tax_included_per_line(line: dict[str, Any]) -> dict[str, Any]:
    """Detect if a line amount is tax-included, pre-tax, or ambiguous.

    Returns dict with:
        is_tax_included: True / False / None (ambiguous)
        detection_method: "keyword" | "indicator" | "ambiguous"
        keyword_found: the matched keyword or None
    """
    text = " ".join(str(v) for v in [
        line.get("description", ""),
        line.get("notes", ""),
        line.get("tax_indicator", ""),
    ]).strip()

    # Check AI-extracted indicator first
    indicator = str(line.get("tax_indicator", "")).lower().strip()
    if indicator == "tax_included":
        return {"is_tax_included": True, "detection_method": "indicator", "keyword_found": None}
    if indicator in ("taxable", "exempt"):
        return {"is_tax_included": False, "detection_method": "indicator", "keyword_found": None}

    # Keyword scan
    m_incl = _TAX_INCLUDED_KW.search(text)
    if m_incl:
        return {"is_tax_included": True, "detection_method": "keyword", "keyword_found": m_incl.group()}

    m_excl = _TAX_EXCLUDED_KW.search(text)
    if m_excl:
        return {"is_tax_included": False, "detection_method": "keyword", "keyword_found": m_excl.group()}

    # Ambiguous — flag for review
    return {"is_tax_included": None, "detection_method": "ambiguous", "keyword_found": None}


# ---------------------------------------------------------------------------
# PLACE OF SUPPLY
# ---------------------------------------------------------------------------

# Supply type constants
SUPPLY_TANGIBLE = "tangible_personal_property"
SUPPLY_SERVICE = "service"
SUPPLY_REAL_PROPERTY = "real_property"
SUPPLY_INTANGIBLE = "intangible"
SUPPLY_TRANSPORTATION = "transportation"
SUPPLY_SHIPPING = "shipping"


def determine_place_of_supply(
    line: dict[str, Any],
    vendor_province: str,
    buyer_province: str,
) -> str:
    """Determine the place of supply for a line item.

    Uses ETA Schedule IX rules:
    - Tangible goods: destination (buyer province)
    - Services: where predominantly performed
    - Shipping: follows principal supply if same contract, else destination
    - Ambiguous: returns "AMBIGUOUS"

    Returns a two-letter province code or "AMBIGUOUS".
    """
    supply_type = str(line.get("supply_type", "")).strip().lower()
    desc = str(line.get("description", "")).lower()

    # Heuristic: detect supply type from description if not set
    if not supply_type:
        _SHIPPING_KW = {"shipping", "freight", "delivery", "livraison", "transport", "expédition"}
        _SERVICE_KW = {"service", "labour", "labor", "installation", "consulting",
                       "main d'oeuvre", "consultation", "professional fee"}
        if any(kw in desc for kw in _SHIPPING_KW):
            supply_type = SUPPLY_SHIPPING
        elif any(kw in desc for kw in _SERVICE_KW):
            supply_type = SUPPLY_SERVICE
        else:
            supply_type = SUPPLY_TANGIBLE

    vendor_prov = vendor_province.strip().upper() if vendor_province else ""
    buyer_prov = buyer_province.strip().upper() if buyer_province else ""

    if supply_type == SUPPLY_TANGIBLE:
        # Rule 1: delivery destination
        return buyer_prov if buyer_prov else (vendor_prov or "AMBIGUOUS")

    if supply_type == SUPPLY_SERVICE:
        # Rule 2: where predominantly performed
        # If we know both, prefer buyer location (conservative — most services
        # are performed where the buyer is). Flag ambiguous if unsure.
        service_location = str(line.get("service_location", "")).strip().upper()
        if service_location:
            return service_location
        if buyer_prov and vendor_prov and buyer_prov != vendor_prov:
            return "AMBIGUOUS"
        return buyer_prov or vendor_prov or "AMBIGUOUS"

    if supply_type == SUPPLY_REAL_PROPERTY:
        # Rule 3: where situated
        property_location = str(line.get("property_location", "")).strip().upper()
        return property_location or "AMBIGUOUS"

    if supply_type == SUPPLY_INTANGIBLE:
        # Rule 4: where recipient belongs
        return buyer_prov if buyer_prov else (vendor_prov or "AMBIGUOUS")

    if supply_type == SUPPLY_TRANSPORTATION:
        # Rule 5: origin to destination — use destination
        return buyer_prov if buyer_prov else "AMBIGUOUS"

    if supply_type == SUPPLY_SHIPPING:
        # Shipping: if same contract as principal supply → follows principal
        # We default to buyer (destination) for standalone shipping
        return buyer_prov if buyer_prov else (vendor_prov or "AMBIGUOUS")

    return "AMBIGUOUS"


# ---------------------------------------------------------------------------
# TAX REGIME ASSIGNMENT
# ---------------------------------------------------------------------------

def assign_line_tax_regime(
    line: dict[str, Any],
    place_of_supply: str,
) -> dict[str, Any]:
    """Assign tax regime to a line based on place of supply.

    Returns dict with:
        tax_regime: HST / GST_QST / GST_ONLY / EXEMPT / GST_PST / AMBIGUOUS
        tax_code: the tax code string
        gst_rate, qst_rate, hst_rate, pst_rate: Decimal rates
        notes: str
    """
    indicator = str(line.get("tax_indicator", "")).lower().strip()
    if indicator == "exempt":
        return {
            "tax_regime": "EXEMPT",
            "tax_code": "E",
            "gst_rate": _ZERO, "qst_rate": _ZERO,
            "hst_rate": _ZERO, "pst_rate": _ZERO,
            "notes": "Line marked exempt",
        }

    prov = place_of_supply.strip().upper()

    if prov == "AMBIGUOUS" or not prov:
        return {
            "tax_regime": "AMBIGUOUS",
            "tax_code": "",
            "gst_rate": _ZERO, "qst_rate": _ZERO,
            "hst_rate": _ZERO, "pst_rate": _ZERO,
            "notes": "Place of supply ambiguous — requires human review",
        }

    if prov == "QC":
        return {
            "tax_regime": "GST_QST",
            "tax_code": "T",
            "gst_rate": GST_RATE, "qst_rate": QST_RATE,
            "hst_rate": _ZERO, "pst_rate": _ZERO,
            "notes": f"Quebec: GST 5% + QST 9.975%",
        }

    if prov == "ON":
        return {
            "tax_regime": "HST",
            "tax_code": "HST",
            "gst_rate": _ZERO, "qst_rate": _ZERO,
            "hst_rate": HST_RATE_ON, "pst_rate": _ZERO,
            "notes": f"Ontario: HST 13%",
        }

    if prov in ATL_PROVINCES:
        return {
            "tax_regime": "HST",
            "tax_code": "HST_ATL",
            "gst_rate": _ZERO, "qst_rate": _ZERO,
            "hst_rate": HST_RATE_ATL, "pst_rate": _ZERO,
            "notes": f"{prov}: HST 15%",
        }

    if prov in GST_ONLY_PROVINCES:
        return {
            "tax_regime": "GST_ONLY",
            "tax_code": "GST_ONLY",
            "gst_rate": GST_RATE, "qst_rate": _ZERO,
            "hst_rate": _ZERO, "pst_rate": _ZERO,
            "notes": f"{prov}: GST 5% only",
        }

    if prov in PST_RATES:
        pst = PST_RATES[prov]
        return {
            "tax_regime": "GST_PST",
            "tax_code": "GST_ONLY",
            "gst_rate": GST_RATE, "qst_rate": _ZERO,
            "hst_rate": _ZERO, "pst_rate": pst,
            "notes": f"{prov}: GST 5% + PST {pst * 100}% (PST non-recoverable)",
        }

    # Unknown province — fall back to GST only
    return {
        "tax_regime": "GST_ONLY",
        "tax_code": "GST_ONLY",
        "gst_rate": GST_RATE, "qst_rate": _ZERO,
        "hst_rate": _ZERO, "pst_rate": _ZERO,
        "notes": f"Unknown province {prov} — defaulting to GST only",
    }


# ---------------------------------------------------------------------------
# LINE TAX CALCULATION
# ---------------------------------------------------------------------------

def calculate_line_tax(
    line: dict[str, Any],
    tax_regime: dict[str, Any],
    is_tax_included: bool | None,
) -> dict[str, Any]:
    """Calculate tax amounts for a single line.

    If tax_included: extract pre-tax = line_total / (1 + rate)
    If not tax_included: calculate tax = line_total * rate

    Returns dict with:
        pretax_amount, gst, qst, hst,
        recoverable_gst, recoverable_qst, recoverable_hst
    """
    line_total = _to_dec(line.get("line_total", 0))
    gst_rate = tax_regime.get("gst_rate", _ZERO)
    qst_rate = tax_regime.get("qst_rate", _ZERO)
    hst_rate = tax_regime.get("hst_rate", _ZERO)
    combined_rate = gst_rate + qst_rate + hst_rate

    regime_name = tax_regime.get("tax_regime", "")

    # ITC/ITR recovery percentages
    itc_pct = _ONE  # full recovery by default
    itr_pct = _ONE

    # Meals: 50% recovery
    desc = str(line.get("description", "")).lower()
    _MEAL_KW = {"meal", "repas", "restaurant", "dining", "entertainment",
                "divertissement", "reception"}
    if any(kw in desc for kw in _MEAL_KW):
        itc_pct = _HALF
        itr_pct = _HALF

    if regime_name == "EXEMPT":
        return {
            "pretax_amount": line_total,
            "gst": _ZERO, "qst": _ZERO, "hst": _ZERO,
            "recoverable_gst": _ZERO, "recoverable_qst": _ZERO,
            "recoverable_hst": _ZERO,
        }

    if is_tax_included and combined_rate > _ZERO:
        # Reverse-calculate pre-tax amount
        divisor = _ONE + combined_rate
        pretax = _round(line_total / divisor)
    else:
        pretax = line_total

    gst = _round(pretax * gst_rate)
    qst = _round(pretax * qst_rate)
    hst = _round(pretax * hst_rate)

    return {
        "pretax_amount": pretax,
        "gst": gst,
        "qst": qst,
        "hst": hst,
        "recoverable_gst": _round(gst * itc_pct),
        "recoverable_qst": _round(qst * itr_pct),
        "recoverable_hst": _round(hst * itc_pct),
    }


# ---------------------------------------------------------------------------
# AI EXTRACTION
# ---------------------------------------------------------------------------

def extract_invoice_lines(
    document_id: str,
    raw_ocr_text: str,
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Extract line items from OCR text using AI, store in invoice_lines table.

    Uses OpenRouterClient with the extract_invoice_lines prompt template.
    Returns the list of extracted line dicts.
    """
    import sys
    root_str = str(ROOT_DIR)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

    from src.agents.tools.openrouter_client import OpenRouterClient

    prompt_template = _load_prompt()
    prompt = prompt_template.replace("{INVOICE_TEXT}", raw_ocr_text[:20000])

    system_msg = (
        "You are an accounting document line-item extractor for a Canadian "
        "bookkeeping workflow. Return STRICT JSON only. Never invent numbers "
        "not present in the text."
    )

    client = OpenRouterClient()
    result = client.chat_json(system=system_msg, user=prompt, temperature=0.0)

    lines = result.get("lines", [])
    invoice_total_shown = result.get("invoice_total_shown")
    tax_total_shown = result.get("tax_total_shown")
    deposit_found = result.get("deposit_found", False)
    deposit_amount = result.get("deposit_amount", 0)
    now = _utc_now_iso()

    # Ensure invoice_lines table columns exist
    _ensure_invoice_lines_table(conn)

    # Clear previous lines for this document
    conn.execute("DELETE FROM invoice_lines WHERE document_id = ?", (document_id,))

    stored_lines: list[dict[str, Any]] = []

    for raw_line in lines:
        line_num = int(raw_line.get("line_number", 0))
        description = str(raw_line.get("description", ""))
        quantity = raw_line.get("quantity", 1)
        unit_price = raw_line.get("unit_price")
        line_total = raw_line.get("line_total", 0)
        tax_indicator = str(raw_line.get("tax_indicator", "taxable"))
        tax_amount_shown = raw_line.get("tax_amount_shown")
        notes = raw_line.get("notes", "")

        # Detect tax-included
        tax_det = detect_tax_included_per_line(raw_line)
        is_tax_included = tax_det["is_tax_included"]

        line_record = {
            "document_id": document_id,
            "line_number": line_num,
            "description": description,
            "quantity": quantity,
            "unit_price": unit_price,
            "line_total_pretax": float(line_total) if line_total else None,
            "tax_indicator": tax_indicator,
            "tax_amount_shown": tax_amount_shown,
            "is_tax_included": 1 if is_tax_included else (0 if is_tax_included is False else None),
            "line_notes": notes,
            "created_at": now,
        }

        conn.execute(
            """INSERT INTO invoice_lines
               (document_id, line_number, description, quantity, unit_price,
                line_total_pretax, is_tax_included, line_notes, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                document_id, line_num, description, quantity, unit_price,
                line_record["line_total_pretax"],
                line_record["is_tax_included"],
                notes, now,
            ),
        )
        stored_lines.append(line_record)

    # Update document flags
    has_lines = 1 if stored_lines else 0
    conn.execute(
        """UPDATE documents
           SET has_line_items = ?,
               deposit_allocated = ?
           WHERE document_id = ?""",
        (has_lines, 1 if deposit_found else 0, document_id),
    )
    conn.commit()

    return stored_lines


def _ensure_invoice_lines_table(conn: sqlite3.Connection) -> None:
    """Create invoice_lines table if missing (runtime safety net)."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS invoice_lines (
            line_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id      TEXT NOT NULL,
            line_number      INTEGER NOT NULL,
            description      TEXT,
            quantity         REAL,
            unit_price       REAL,
            line_total_pretax REAL,
            tax_code         TEXT,
            tax_regime       TEXT,
            gst_amount       REAL,
            qst_amount       REAL,
            hst_amount       REAL,
            province_of_supply TEXT,
            is_tax_included  INTEGER,
            line_notes       TEXT,
            created_at       TEXT NOT NULL DEFAULT ''
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_invoice_lines_doc "
        "ON invoice_lines(document_id)"
    )


# ---------------------------------------------------------------------------
# RECONCILIATION
# ---------------------------------------------------------------------------

def reconcile_invoice_lines(
    document_id: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Reconcile line totals against the invoice total.

    1. Sum all line pretax amounts
    2. Sum all line tax amounts
    3. Compare to invoice total — flag gap if difference > $0.02
    4. If gap: lines_reconciled=False, invoice_total_gap=gap_amount
    5. If no gap: lines_reconciled=True

    Returns dict with: line_sum, tax_sum, invoice_total, gap, reconciled.
    """
    rows = conn.execute(
        """SELECT line_total_pretax, gst_amount, qst_amount, hst_amount
           FROM invoice_lines WHERE document_id = ?""",
        (document_id,),
    ).fetchall()

    line_sum = _ZERO
    tax_sum = _ZERO
    for r in rows:
        line_sum += _to_dec(r[0])
        tax_sum += _to_dec(r[1]) + _to_dec(r[2]) + _to_dec(r[3])

    # Get invoice total from documents table
    doc_row = conn.execute(
        "SELECT amount FROM documents WHERE document_id = ?",
        (document_id,),
    ).fetchone()
    invoice_total = _to_dec(doc_row[0]) if doc_row and doc_row[0] else _ZERO

    total_computed = line_sum + tax_sum
    gap = _round(abs(total_computed - invoice_total))
    reconciled = gap <= Decimal("0.02")

    # Update document
    conn.execute(
        """UPDATE documents
           SET lines_reconciled = ?,
               line_total_sum = ?,
               invoice_total_gap = ?
           WHERE document_id = ?""",
        (
            1 if reconciled else 0,
            float(line_sum),
            float(gap) if not reconciled else 0.0,
            document_id,
        ),
    )
    conn.commit()

    return {
        "line_sum": float(line_sum),
        "tax_sum": float(tax_sum),
        "total_computed": float(total_computed),
        "invoice_total": float(invoice_total),
        "gap": float(gap),
        "reconciled": reconciled,
    }


# ---------------------------------------------------------------------------
# DEPOSIT ALLOCATION
# ---------------------------------------------------------------------------

def allocate_deposit_proportionally(
    document_id: str,
    deposit_amount: float | Decimal,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Allocate a deposit proportionally across invoice lines.

    1. Calculate each line's share of total pretax value
    2. Allocate deposit proportionally
    3. Recalculate tax recovery net of deposit allocation per line
    4. Return per-line deposit allocation and adjusted ITC/ITR

    Returns dict with:
        total_pretax, deposit, allocations: list[dict]
    """
    deposit = _to_dec(deposit_amount)

    rows = conn.execute(
        """SELECT line_id, line_number, description, line_total_pretax,
                  gst_amount, qst_amount, hst_amount, tax_code
           FROM invoice_lines WHERE document_id = ?
           ORDER BY line_number""",
        (document_id,),
    ).fetchall()

    total_pretax = sum(_to_dec(r[3]) for r in rows)
    if total_pretax <= _ZERO:
        return {"total_pretax": 0, "deposit": float(deposit), "allocations": []}

    allocations: list[dict[str, Any]] = []
    for r in rows:
        line_pretax = _to_dec(r[3])
        share = line_pretax / total_pretax if total_pretax > _ZERO else _ZERO
        line_deposit = _round(deposit * share)
        net_pretax = line_pretax - line_deposit

        # Recalculate recoverable tax on net amount
        gst_orig = _to_dec(r[4])
        qst_orig = _to_dec(r[5])
        hst_orig = _to_dec(r[6])

        # Proportional reduction of tax recovery
        if line_pretax > _ZERO:
            reduction_factor = net_pretax / line_pretax
        else:
            reduction_factor = _ZERO

        adj_gst = _round(gst_orig * reduction_factor)
        adj_qst = _round(qst_orig * reduction_factor)
        adj_hst = _round(hst_orig * reduction_factor)

        allocations.append({
            "line_id": r[0],
            "line_number": r[1],
            "description": r[2],
            "original_pretax": float(line_pretax),
            "deposit_allocated": float(line_deposit),
            "net_pretax": float(net_pretax),
            "adjusted_gst_recovery": float(adj_gst),
            "adjusted_qst_recovery": float(adj_qst),
            "adjusted_hst_recovery": float(adj_hst),
        })

    # Mark document as deposit-allocated
    conn.execute(
        "UPDATE documents SET deposit_allocated = 1 WHERE document_id = ?",
        (document_id,),
    )
    conn.commit()

    return {
        "total_pretax": float(total_pretax),
        "deposit": float(deposit),
        "allocations": allocations,
    }


# ---------------------------------------------------------------------------
# FULL LINE PROCESSING PIPELINE
# ---------------------------------------------------------------------------

def process_line_items(
    document_id: str,
    raw_ocr_text: str,
    vendor_province: str = "QC",
    buyer_province: str = "QC",
    *,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Run the full line-item pipeline for a document.

    1. Extract lines via AI
    2. For each line: determine place of supply, assign tax regime, calculate tax
    3. Store results in invoice_lines
    4. Reconcile against invoice total

    Returns summary dict.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        # Step 1: AI extraction
        lines = extract_invoice_lines(document_id, raw_ocr_text, conn)

        # Step 2: Per-line tax processing
        for line in lines:
            # Place of supply
            pos = determine_place_of_supply(line, vendor_province, buyer_province)

            # Tax regime
            regime = assign_line_tax_regime(line, pos)

            # Tax-included detection
            tax_det = detect_tax_included_per_line(line)
            is_tax_included = tax_det["is_tax_included"]

            # Calculate tax
            tax = calculate_line_tax(line, regime, is_tax_included)

            # Update invoice_lines row
            conn.execute(
                """UPDATE invoice_lines
                   SET tax_code = ?,
                       tax_regime = ?,
                       gst_amount = ?,
                       qst_amount = ?,
                       hst_amount = ?,
                       province_of_supply = ?,
                       line_total_pretax = ?,
                       line_notes = COALESCE(line_notes, '') || ?
                   WHERE document_id = ? AND line_number = ?""",
                (
                    regime.get("tax_code", ""),
                    regime.get("tax_regime", ""),
                    float(tax["gst"]),
                    float(tax["qst"]),
                    float(tax["hst"]),
                    pos,
                    float(tax["pretax_amount"]),
                    (f" | {regime.get('notes', '')}" if regime.get("notes") else ""),
                    document_id,
                    line.get("line_number", 0),
                ),
            )

        conn.commit()

        # Step 3: Reconcile
        recon = reconcile_invoice_lines(document_id, conn)

        return {
            "ok": True,
            "document_id": document_id,
            "lines_extracted": len(lines),
            "reconciliation": recon,
        }

    except Exception as exc:
        return {
            "ok": False,
            "document_id": document_id,
            "error": str(exc),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MULTI-LINE INVOICE DETECTION (used by ocr_engine integration)
# ---------------------------------------------------------------------------

_LINE_ITEM_KEYWORDS = re.compile(
    r"qty|quantity|quantité|unit\s*price|prix\s*unitaire|"
    r"item\s*#|line\s*#|description.*amount|"
    r"subtotal.*total|sous-total|shipping|livraison|"
    r"item\s+description|no\.\s*article",
    re.IGNORECASE,
)

_MULTI_AMOUNT_RE = re.compile(r"\$?\d{1,3}(?:[,\s]\d{3})*(?:\.\d{2})")


def looks_like_multiline_invoice(raw_text: str) -> bool:
    """Heuristic: does this text look like a multi-line invoice?

    True if it has line-item keywords AND 3+ distinct dollar amounts.
    """
    if not raw_text:
        return False
    has_keywords = bool(_LINE_ITEM_KEYWORDS.search(raw_text))
    amounts = _MULTI_AMOUNT_RE.findall(raw_text)
    return has_keywords and len(amounts) >= 3


# =========================================================================
# PART 2 (line_item_engine) — False precision prevention
# =========================================================================

def analyze_line_allocation_gap(
    invoice_total: Any,
    cbsa_goods_value: Any,
    invoice_text: str = "",
) -> dict[str, Any]:
    """Analyze gap between invoice total and CBSA documented goods value.

    Delegates to customs_engine.analyze_allocation_gap for consistency.
    """
    from src.engines.customs_engine import analyze_allocation_gap
    return analyze_allocation_gap(invoice_total, cbsa_goods_value, invoice_text)
