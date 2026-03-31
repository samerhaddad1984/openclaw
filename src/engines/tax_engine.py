"""
src/engines/tax_engine.py — Layer 1 deterministic tax calculation engine.

This is purely deterministic code.  No AI calls anywhere in this module.
All monetary arithmetic uses Python Decimal to avoid floating-point errors.

Tax codes
---------
T           Taxable — GST (5%) + QST (9.975%) applied in parallel to pre-tax amount
Z           Zero-rated — taxable at 0%, ITC can still be claimed on inputs
E           Exempt — no tax collected, no ITC/ITR on inputs
M           Mixed/Meals — 50% of GST+QST claimable (Canadian meal entertainment rule)
I           Insurance — Quebec special: no GST, 9% non-recoverable provincial charge
GST_QST     Legacy code (from tax_code_resolver.py) — equivalent to T
HST         Harmonized Sales Tax (ON=13%, Atlantic=15%)
VAT         Foreign VAT — not recoverable in Canada
GENERIC_TAX Unclassified tax line — no recovery assumed
NONE        No tax

ITC = Input Tax Credit  (GST recovery on business purchases)
ITR = Input Tax Refund  (QST recovery on Quebec purchases)
"""
from __future__ import annotations

import sqlite3
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

# ---------------------------------------------------------------------------
# Tax rates — exact, no float
# ---------------------------------------------------------------------------

GST_RATE: Decimal = Decimal("0.05")       # Federal GST:  5.000%
QST_RATE: Decimal = Decimal("0.09975")    # Quebec QST:   9.975%
HST_RATE_ON: Decimal = Decimal("0.13")   # Ontario HST: 13.000%
HST_RATE_ATL: Decimal = Decimal("0.15")  # Atlantic HST: 15.000% (NB/NS/NL/PEI)

COMBINED_GST_QST: Decimal = GST_RATE + QST_RATE  # 0.14975

CENT: Decimal = Decimal("0.01")           # Rounding quantum

# ---------------------------------------------------------------------------
# Province sets
# ---------------------------------------------------------------------------

# Provinces/territories that use HST (ISO 3166-2 sub-code without "CA-")
HST_PROVINCES: frozenset[str] = frozenset({"ON", "NB", "NS", "NL", "PE"})
ATL_PROVINCES: frozenset[str] = frozenset({"NB", "NS", "NL", "PE"})
QC_PROVINCE: str = "QC"

# Provinces/territories that charge only GST (5%) — no PST/QST/HST
GST_ONLY_PROVINCES: frozenset[str] = frozenset({"AB", "NT", "NU", "YT"})

# Provinces with non-recoverable PST alongside GST
PST_PROVINCES: dict[str, Decimal] = {
    "BC": Decimal("0.07"),   # 7% PST
    "MB": Decimal("0.07"),   # 7% PST (RST)
    "SK": Decimal("0.06"),   # 6% PST
}

# ---------------------------------------------------------------------------
# Tax code registry
# ---------------------------------------------------------------------------
# gst_rate / qst_rate / hst_rate: statutory rate charged on purchases
# itc_pct: fraction of GST (or HST) that is recoverable as ITC
# itr_pct: fraction of QST that is recoverable as ITR

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HALF = Decimal("0.5")

TAX_CODE_REGISTRY: dict[str, dict[str, Any]] = {
    "T": {
        "label": "Taxable (GST + QST)",
        "gst_rate": GST_RATE,
        "qst_rate": QST_RATE,
        "hst_rate": _ZERO,
        "itc_pct": _ONE,
        "itr_pct": _ONE,
    },
    "Z": {
        "label": "Zero-rated",
        "gst_rate": _ZERO,
        "qst_rate": _ZERO,
        "hst_rate": _ZERO,
        "itc_pct": _ZERO,
        "itr_pct": _ZERO,
    },
    "E": {
        "label": "Exempt",
        "gst_rate": _ZERO,
        "qst_rate": _ZERO,
        "hst_rate": _ZERO,
        "itc_pct": _ZERO,
        "itr_pct": _ZERO,
    },
    "M": {
        "label": "Mixed / Meals (50% deductible)",
        "gst_rate": GST_RATE,
        "qst_rate": QST_RATE,
        "hst_rate": _ZERO,
        "itc_pct": _HALF,
        "itr_pct": _HALF,
    },
    "I": {
        "label": "Insurance — Quebec special rate",
        # In Canada, insurance premiums are exempt from GST.
        # Quebec levies a ~9% non-recoverable charge on insurance premiums;
        # it is NOT QST and cannot be claimed as ITR.
        "gst_rate": _ZERO,
        "qst_rate": Decimal("0.09"),
        "hst_rate": _ZERO,
        "itc_pct": _ZERO,
        "itr_pct": _ZERO,
    },
    # ---- Legacy codes from tax_code_resolver.py ----
    "GST_QST": {
        "label": "GST + QST (legacy code — same as T)",
        "gst_rate": GST_RATE,
        "qst_rate": QST_RATE,
        "hst_rate": _ZERO,
        "itc_pct": _ONE,
        "itr_pct": _ONE,
    },
    "HST": {
        "label": "Harmonized Sales Tax (ON default: 13%)",
        "gst_rate": _ZERO,
        "qst_rate": _ZERO,
        "hst_rate": HST_RATE_ON,
        "itc_pct": _ONE,
        "itr_pct": _ZERO,  # No QST in HST provinces
    },
    "VAT": {
        "label": "Foreign VAT — not recoverable in Canada",
        "gst_rate": _ZERO,
        "qst_rate": _ZERO,
        "hst_rate": _ZERO,
        "itc_pct": _ZERO,
        "itr_pct": _ZERO,
    },
    "HST_ATL": {
        "label": "Harmonized Sales Tax — Atlantic (NB/NS/NL/PE: 15%)",
        "gst_rate": _ZERO,
        "qst_rate": _ZERO,
        "hst_rate": HST_RATE_ATL,
        "itc_pct": _ONE,
        "itr_pct": _ZERO,
    },
    "GENERIC_TAX": {
        "label": "Generic / unclassified tax",
        "gst_rate": _ZERO,
        "qst_rate": _ZERO,
        "hst_rate": _ZERO,
        "itc_pct": _ZERO,
        "itr_pct": _ZERO,
    },
    "GST_ONLY": {
        "label": "GST only (5%) — no provincial tax",
        "gst_rate": GST_RATE,
        "qst_rate": _ZERO,
        "hst_rate": _ZERO,
        "itc_pct": _ONE,
        "itr_pct": _ZERO,
    },
    "NONE": {
        "label": "No tax",
        "gst_rate": _ZERO,
        "qst_rate": _ZERO,
        "hst_rate": _ZERO,
        "itc_pct": _ZERO,
        "itr_pct": _ZERO,
    },
}

VALID_TAX_CODES: frozenset[str] = frozenset(TAX_CODE_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _round(value: Decimal) -> Decimal:
    """Round to the nearest cent using ROUND_HALF_UP."""
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _to_decimal(value: Any) -> Decimal:
    """Convert any numeric-ish value to Decimal, raising ValueError on failure."""
    if isinstance(value, Decimal):
        if value.is_nan() or value.is_infinite():
            raise ValueError("Invalid decimal value: NaN or Infinity not allowed")
        return value
    if value is None or str(value).strip() == "":
        raise ValueError("amount must not be None or empty")
    d = Decimal(str(value))
    if d.is_nan() or d.is_infinite():
        raise ValueError("Invalid decimal value: NaN or Infinity not allowed")
    return d


def _normalize_code(tax_code: Any) -> str:
    """Normalize a tax code to uppercase stripped string."""
    if tax_code is None:
        return ""
    return str(tax_code).strip().upper()


def _registry_entry(tax_code: str) -> dict[str, Any]:
    """Return the registry entry for a code, falling back to NONE."""
    return TAX_CODE_REGISTRY.get(_normalize_code(tax_code), TAX_CODE_REGISTRY["NONE"])


# ---------------------------------------------------------------------------
# calculate_gst_qst
# ---------------------------------------------------------------------------

def calculate_gst_qst(amount_before_tax: Decimal) -> dict[str, Any]:
    """
    Calculate GST and QST on a pre-tax (net) amount.

    Both taxes are applied in *parallel* to the pre-tax amount — they are
    not cascaded (QST is NOT applied to GST-inclusive price).

      GST = amount_before_tax × 5%
      QST = amount_before_tax × 9.975%

    Parameters
    ----------
    amount_before_tax : Decimal
        Net invoice amount (excluding all taxes).

    Returns
    -------
    dict with keys:
        amount_before_tax, gst, qst, total_tax, total_with_tax,
        gst_rate, qst_rate
    """
    amount_before_tax = _to_decimal(amount_before_tax)

    gst = _round(amount_before_tax * GST_RATE)
    qst = _round(amount_before_tax * QST_RATE)

    # P3-1: Prevent micro-transaction tax leakage — minimum $0.01 per tax
    _MIN_TAX = Decimal("0.01")
    if amount_before_tax > _ZERO and gst == _ZERO:
        gst = _MIN_TAX
    if amount_before_tax > _ZERO and qst == _ZERO:
        qst = _MIN_TAX

    total_tax = gst + qst
    total_with_tax = amount_before_tax + total_tax

    return {
        "amount_before_tax": amount_before_tax,
        "gst": gst,
        "qst": qst,
        "total_tax": total_tax,
        "total_with_tax": total_with_tax,
        "gst_rate": GST_RATE,
        "qst_rate": QST_RATE,
    }


# ---------------------------------------------------------------------------
# extract_tax_from_total
# ---------------------------------------------------------------------------

def extract_tax_from_total(total: Decimal) -> dict[str, Any]:
    """
    Reverse-compute the pre-tax amount, GST, and QST from a *total* that
    already includes both GST (5%) and QST (9.975%) applied in parallel.

    Derivation:
      total = pre_tax × (1 + GST_RATE + QST_RATE)
            = pre_tax × 1.14975
      → pre_tax = total ÷ 1.14975

    Parameters
    ----------
    total : Decimal
        Invoice total including GST + QST.

    Returns
    -------
    dict with keys:
        total, combined_rate, combined_divisor,
        pre_tax, gst, qst, total_tax
    """
    total = _to_decimal(total)
    combined_divisor = _ONE + COMBINED_GST_QST   # 1.14975

    pre_tax = _round(total / combined_divisor)
    gst = _round(pre_tax * GST_RATE)
    qst = _round(pre_tax * QST_RATE)
    total_tax = gst + qst

    return {
        "total": total,
        "combined_rate": COMBINED_GST_QST,
        "combined_divisor": combined_divisor,
        "pre_tax": pre_tax,
        "gst": gst,
        "qst": qst,
        "total_tax": total_tax,
    }


# ---------------------------------------------------------------------------
# validate_tax_code
# ---------------------------------------------------------------------------

def validate_tax_code(
    gl_account: Any,
    tax_code: Any,
    vendor_province: Any,
) -> dict[str, Any]:
    """
    Validate that a tax code is consistent with the GL account and province.

    All checks are deterministic — no AI calls.

    Parameters
    ----------
    gl_account      : GL account name or number (e.g. "5200 - Office Supplies")
    tax_code        : Tax code string (e.g. "T", "GST_QST", "HST")
    vendor_province : Two-letter Canadian province code (e.g. "QC", "ON")

    Returns
    -------
    dict with keys:
        valid: bool                     — True iff warnings list is empty
        warnings: list[str]             — machine-readable warning identifiers
        tax_code: str                   — normalized code
        gl_account: str
        vendor_province: str
    """
    warnings: list[str] = []
    tc = _normalize_code(tax_code)
    gl = str(gl_account or "").strip()
    province = str(vendor_province or "").strip().upper()

    # ---- Unknown or missing tax code ----
    if not tc:
        warnings.append("tax_code_missing")
    elif tc not in VALID_TAX_CODES:
        warnings.append(f"unknown_tax_code:{tc}")

    # ---- Province / code consistency ----
    if tc and tc not in ("", "NONE", "E", "Z") and province:
        if province in HST_PROVINCES and tc in ("T", "GST_QST"):
            warnings.append(f"province_{province}_uses_hst_not_gst_qst")
        elif province == QC_PROVINCE and tc in ("HST", "HST_ATL"):
            warnings.append("province_qc_does_not_use_hst")
        elif province not in HST_PROVINCES and province != QC_PROVINCE and tc in ("HST", "HST_ATL"):
            # AB, SK, MB, BC, YT, NT, NU do not have HST
            warnings.append(f"province_{province}_does_not_use_hst")
        # GST-only / PST provinces should not use T (GST+QST)
        if province in GST_ONLY_PROVINCES and tc in ("T", "GST_QST"):
            warnings.append(
                f"province_{province}_gst_only_not_gst_qst — use GST_ONLY code"
            )
        elif province in PST_PROVINCES and tc in ("T", "GST_QST"):
            warnings.append(
                f"province_{province}_gst_only_not_gst_qst — use GST_ONLY code. "
                f"TVP provinciale non récupérable / Provincial PST not recoverable — "
                f"ITC applies to GST only."
            )

    # ---- GL account heuristics ----
    if gl and tc and tc in VALID_TAX_CODES:
        gl_lower = gl.lower()

        _INSURANCE_KW = {"insurance", "assurance", "insur", "prime"}
        _MEAL_KW = {"meal", "repas", "entertainment", "dining", "restaurant",
                    "divertissement", "reception", "réception"}

        if any(kw in gl_lower for kw in _INSURANCE_KW) and tc not in ("I", "E", "Z", "NONE"):
            warnings.append("insurance_gl_account_expects_code_i_or_exempt")

        if any(kw in gl_lower for kw in _MEAL_KW) and tc not in ("M", "E", "Z"):
            warnings.append("meals_gl_account_expects_code_m")

    return {
        "valid": len(warnings) == 0,
        "warnings": warnings,
        "tax_code": tc,
        "gl_account": gl,
        "vendor_province": province,
    }


def validate_tax_code_per_line(
    document_id: str,
    gl_account: Any,
    vendor_province: Any,
    *,
    db_path: Path = DB_PATH,
) -> list[dict[str, Any]]:
    """Validate tax code for each invoice line of a document separately.

    When a document has invoice_lines, each line may have a different tax
    regime (e.g., one line taxable in QC, another exempt).  This validates
    each line's tax_code against its province_of_supply.

    Falls back to :func:`validate_tax_code` if no invoice lines exist.

    Returns a list of per-line validation results.
    """
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(str(db_path))
    conn.row_factory = _sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT line_number, tax_code, province_of_supply, description "
            "FROM invoice_lines WHERE document_id = ? ORDER BY line_number",
            (document_id,),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        # No line items — fall back to document-level validation
        return [validate_tax_code(gl_account, None, vendor_province)]

    results: list[dict[str, Any]] = []
    for r in rows:
        line_province = r["province_of_supply"] or str(vendor_province or "").strip().upper()
        line_tc = r["tax_code"] or ""
        result = validate_tax_code(gl_account, line_tc, line_province)
        result["line_number"] = r["line_number"]
        result["description"] = r["description"] or ""
        results.append(result)

    return results


# ---------------------------------------------------------------------------
# calculate_itc_itr
# ---------------------------------------------------------------------------

def calculate_itc_itr(
    expense_amount: Decimal,
    tax_code: Any,
) -> dict[str, Any]:
    """
    Calculate the recoverable ITC (GST Input Tax Credit) and ITR
    (QST Input Tax Refund) for a business expense.

    ``expense_amount`` is the **pre-tax (net) amount** — i.e. the invoice
    amount *before* any taxes are added.

    Parameters
    ----------
    expense_amount : Decimal
        Net (pre-tax) expense amount.
    tax_code       : str
        Tax code for the expense (T, Z, E, M, I, GST_QST, HST, …).

    Returns
    -------
    dict with keys:
        expense_amount, tax_code,
        gst_paid, qst_paid, hst_paid,
        gst_recoverable, qst_recoverable, hst_recoverable,
        itc_rate, itr_rate, total_recoverable
    """
    expense_amount = _to_decimal(expense_amount)
    tc = _normalize_code(tax_code)
    entry = _registry_entry(tc)

    gst_rate: Decimal = entry["gst_rate"]
    qst_rate: Decimal = entry["qst_rate"]
    hst_rate: Decimal = entry["hst_rate"]
    itc_pct: Decimal = entry["itc_pct"]
    itr_pct: Decimal = entry["itr_pct"]

    gst_paid = _round(expense_amount * gst_rate)
    qst_paid = _round(expense_amount * qst_rate)
    hst_paid = _round(expense_amount * hst_rate)

    gst_recoverable = _round(gst_paid * itc_pct)
    qst_recoverable = _round(qst_paid * itr_pct)
    hst_recoverable = _round(hst_paid * itc_pct)  # HST ITC uses the same itc_pct

    total_recoverable = gst_recoverable + qst_recoverable + hst_recoverable

    return {
        "expense_amount": expense_amount,
        "tax_code": tc or "NONE",
        "gst_paid": gst_paid,
        "qst_paid": qst_paid,
        "hst_paid": hst_paid,
        "gst_recoverable": gst_recoverable,
        "qst_recoverable": qst_recoverable,
        "hst_recoverable": hst_recoverable,
        "itc_rate": _round(gst_rate * itc_pct),
        "itr_rate": _round(qst_rate * itr_pct),
        "total_recoverable": total_recoverable,
    }


# ---------------------------------------------------------------------------
# Internal helper for filing summary: ITC/ITR from a *total* amount
# ---------------------------------------------------------------------------

def _itc_itr_from_total(total: Decimal, tax_code: str) -> dict[str, Any]:
    """
    Compute ITC/ITR when ``total`` is the invoice total (tax-inclusive).

    For T/GST_QST/M codes the pre-tax amount is extracted first using
    the exact reverse formula.  For all other codes the total is used
    directly as the pre-tax base (no tax is embedded).
    """
    tc = _normalize_code(tax_code)
    if tc in ("T", "GST_QST"):
        extracted = extract_tax_from_total(total)
        return calculate_itc_itr(extracted["pre_tax"], tc)
    elif tc == "M":
        # Meals: total includes GST+QST; extract pre-tax, then apply 50%
        extracted = extract_tax_from_total(total)
        return calculate_itc_itr(extracted["pre_tax"], "M")
    elif tc in ("HST", "HST_ATL"):
        # total = pre_tax × (1 + hst_rate); use registry rate for the code
        hst_rate = _registry_entry(tc)["hst_rate"]
        hst_divisor = _ONE + hst_rate
        pre_tax = _round(total / hst_divisor)
        return calculate_itc_itr(pre_tax, tc)
    else:
        # Z, E, I, VAT, GENERIC_TAX, NONE — treat total as pre-tax base
        return calculate_itc_itr(total, tc)


# ---------------------------------------------------------------------------
# generate_filing_summary
# ---------------------------------------------------------------------------

def _open_db_readonly(db_path: Path) -> sqlite3.Connection:
    uri = db_path.as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def generate_filing_summary(
    client_code: str,
    period_start: str,
    period_end: str,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """
    Generate a GST/QST filing summary for a client and accounting period.

    This is an **expense-side** summary only.  The system does not track
    revenue, so ``gst_collected`` and ``qst_collected`` represent output
    taxes that must be entered separately; they default to zero here.

    ITC/ITR totals are computed only for documents with
    ``posting_status = 'posted'`` or a non-empty ``external_id``.
    Documents not yet posted are counted under ``documents_pending``.

    For T/GST_QST/M codes the stored ``amount`` is treated as the
    **total invoice amount** (tax-inclusive); the pre-tax base is
    back-calculated.  For all other codes the stored amount is used
    directly.

    Parameters
    ----------
    client_code  : Client identifier (case-insensitive match).
    period_start : ISO date string, inclusive (e.g. "2025-01-01").
    period_end   : ISO date string, inclusive (e.g. "2025-03-31").
    db_path      : Path to the SQLite database.

    Returns
    -------
    dict with keys:
        client_code, period_start, period_end,
        gst_collected, qst_collected,        ← output taxes (revenue-side)
        itc_available, itr_available,        ← recoverable input taxes
        net_gst_payable, net_qst_payable,    ← collected - recoverable (may be negative)
        documents_posted, documents_pending, documents_total,
        line_items: list[dict]
    """
    _ZERO_D = Decimal("0")
    gst_collected = _ZERO_D
    qst_collected = _ZERO_D
    itc_available = _ZERO_D
    itr_available = _ZERO_D
    documents_posted = 0
    documents_pending = 0
    line_items: list[dict[str, Any]] = []

    _empty = {
        "client_code": client_code,
        "period_start": period_start,
        "period_end": period_end,
        "gst_collected": _ZERO_D,
        "qst_collected": _ZERO_D,
        "itc_available": _ZERO_D,
        "itr_available": _ZERO_D,
        "net_gst_payable": _ZERO_D,
        "net_qst_payable": _ZERO_D,
        "documents_posted": 0,
        "documents_pending": 0,
        "documents_total": 0,
        "line_items": [],
    }

    if not db_path.exists():
        return {**_empty, "error": "database_not_found"}

    try:
        conn = _open_db_readonly(db_path)
        try:
            rows = conn.execute(
                """
                SELECT
                    d.document_id,
                    d.vendor,
                    d.document_date,
                    d.amount,
                    d.tax_code,
                    d.gl_account,
                    d.review_status,
                    COALESCE(pj.posting_status, '') AS posting_status,
                    COALESCE(pj.external_id,    '') AS external_id
                FROM documents d
                LEFT JOIN posting_jobs pj
                    ON pj.document_id = d.document_id
                    AND pj.rowid = (
                        SELECT pj2.rowid FROM posting_jobs pj2
                        WHERE pj2.document_id = d.document_id
                        ORDER BY COALESCE(pj2.updated_at, pj2.created_at) DESC,
                                 pj2.rowid DESC
                        LIMIT 1
                    )
                WHERE LOWER(COALESCE(d.client_code, '')) = LOWER(?)
                  AND COALESCE(d.document_date, '') >= ?
                  AND COALESCE(d.document_date, '') <= ?
                  AND LOWER(COALESCE(d.review_status, '')) != 'ignored'
                ORDER BY d.document_date, d.document_id
                """,
                (str(client_code).strip(), str(period_start), str(period_end)),
            ).fetchall()
        finally:
            conn.close()
    except Exception as exc:
        return {**_empty, "error": str(exc)}

    for row in rows:
        raw_amount = row["amount"]
        tax_code = str(row["tax_code"] or "").strip()
        posting_status = str(row["posting_status"] or "").strip()
        external_id = str(row["external_id"] or "").strip()
        is_posted = bool(external_id) or posting_status == "posted"

        try:
            amount = Decimal(str(raw_amount)) if raw_amount not in (None, "") else _ZERO_D
        except Exception:
            amount = _ZERO_D

        # Compute ITC/ITR using total-based helper
        itc_itr = _itc_itr_from_total(amount, tax_code)

        if is_posted:
            documents_posted += 1
            itc_available += itc_itr["gst_recoverable"] + itc_itr["hst_recoverable"]
            itr_available += itc_itr["qst_recoverable"]
        else:
            documents_pending += 1

        line_items.append({
            "document_id": str(row["document_id"] or ""),
            "vendor": str(row["vendor"] or ""),
            "document_date": str(row["document_date"] or ""),
            "amount": amount,
            "tax_code": tax_code,
            "gl_account": str(row["gl_account"] or ""),
            "is_posted": is_posted,
            "gst_recoverable": itc_itr["gst_recoverable"],
            "qst_recoverable": itc_itr["qst_recoverable"],
            "hst_recoverable": itc_itr["hst_recoverable"],
            "total_recoverable": itc_itr["total_recoverable"],
        })

    net_gst_payable = _round(gst_collected - itc_available)
    net_qst_payable = _round(qst_collected - itr_available)

    return {
        "client_code": client_code,
        "period_start": period_start,
        "period_end": period_end,
        "gst_collected": _round(gst_collected),
        "qst_collected": _round(qst_collected),
        "itc_available": _round(itc_available),
        "itr_available": _round(itr_available),
        "net_gst_payable": net_gst_payable,
        "net_qst_payable": net_qst_payable,
        "documents_posted": documents_posted,
        "documents_pending": documents_pending,
        "documents_total": len(rows),
        "line_items": line_items,
    }


# ---------------------------------------------------------------------------
# Quebec GST/QST compliance validator — detects 8 error types
# ---------------------------------------------------------------------------

# Quick Method remittance rates (services / goods)
QUICK_METHOD_RATES: dict[str, Decimal] = {
    "services": Decimal("0.036"),   # 3.6%
    "goods":    Decimal("0.066"),   # 6.6%
}

# Exempt categories (basic groceries, medical, etc.)
_EXEMPT_CATEGORIES: frozenset[str] = frozenset({
    "basic_groceries", "groceries", "épicerie",
    "medical_services", "medical", "médicaux", "santé",
    "child_care", "garde_enfants",
    "educational_services", "éducation",
    "residential_rent", "loyer_résidentiel",
})

_OLD_QST_RATE = Decimal("0.095")  # Pre-2013 rate (9.5%)


def validate_quebec_tax_compliance(document: dict) -> list[dict]:
    """
    Validate a document/transaction for Quebec GST/QST compliance.

    Detects 8 error types:
      1. tax_on_tax_error           — QST calculated on GST-inclusive amount
      2. large_business_itr_restricted — large business claiming full ITR
      3. unregistered_supplier_charging_tax — small supplier charging tax
      4. wrong_qst_rate             — using old 9.5% rate
      5. missing_registration_number — taxable invoice >$30 without reg#
      6. wrong_provincial_tax       — cross-provincial tax mismatch
      7. exempt_item_taxed          — exempt item incorrectly taxed
      8. quick_method_rate_error    — wrong Quick Method remittance rate

    Parameters
    ----------
    document : dict
        Must contain at least ``subtotal`` (pre-tax amount).  Optional keys:
        ``gst_amount``, ``qst_amount``, ``vendor_province``, ``vendor_revenue``,
        ``gst_registration``, ``qst_registration``, ``category``,
        ``company_revenue``, ``quick_method``, ``quick_method_type``,
        ``remittance_rate``, ``total_with_tax``.

    Returns
    -------
    list[dict]  — each dict has keys: error_type, severity, description_en,
                  description_fr, correct_calculation (dict or None).
    """
    issues: list[dict] = []
    subtotal = _to_decimal(document.get("subtotal", 0))
    gst_amount = _to_decimal(document.get("gst_amount", 0))
    qst_amount = _to_decimal(document.get("qst_amount", 0))
    vendor_province = str(document.get("vendor_province", "")).strip().upper()
    vendor_revenue = _to_decimal(document.get("vendor_revenue", 0))
    gst_reg = str(document.get("gst_registration", "")).strip()
    qst_reg = str(document.get("qst_registration", "")).strip()
    category = str(document.get("category", "")).strip().lower()
    company_revenue = _to_decimal(document.get("company_revenue", 0))
    quick_method = bool(document.get("quick_method", False))
    quick_method_type = str(document.get("quick_method_type", "services")).strip().lower()
    remittance_rate = _to_decimal(document.get("remittance_rate", 0))
    total_with_tax = _to_decimal(document.get("total_with_tax", 0))

    # ---- 0. Zero subtotal with non-zero tax ---------------------------------
    if subtotal == _ZERO and (gst_amount > _ZERO or qst_amount > _ZERO):
        issues.append({
            "error_type": "zero_subtotal_nonzero_tax",
            "severity": "critical",
            "description_en": (
                f"Subtotal is $0 but GST (${gst_amount}) and/or QST "
                f"(${qst_amount}) are non-zero. This indicates data "
                f"corruption or an incorrectly constructed document."
            ),
            "description_fr": (
                f"Le sous-total est 0$ mais la TPS ({gst_amount}$) et/ou "
                f"la TVQ ({qst_amount}$) sont non nulles. Cela indique une "
                f"corruption de données ou un document mal construit."
            ),
            "correct_calculation": {
                "subtotal": str(subtotal),
                "gst_amount": str(gst_amount),
                "qst_amount": str(qst_amount),
            },
        })

    # ---- 1. Tax-on-tax error ------------------------------------------------
    if subtotal > _ZERO and qst_amount > _ZERO:
        correct_qst = _round(subtotal * QST_RATE)
        gst_inclusive = subtotal + _round(subtotal * GST_RATE)
        wrong_qst = _round(gst_inclusive * QST_RATE)
        # If the charged QST is closer to the wrong (tax-on-tax) value
        diff_correct = abs(qst_amount - correct_qst)
        diff_wrong = abs(qst_amount - wrong_qst)
        if diff_wrong < diff_correct and diff_wrong < Decimal("0.02"):
            issues.append({
                "error_type": "tax_on_tax_error",
                "severity": "critical",
                "description_en": (
                    f"QST appears calculated on GST-inclusive amount "
                    f"(${qst_amount} charged vs ${correct_qst} correct). "
                    f"QST must be applied to the pre-tax subtotal, not the "
                    f"GST-inclusive amount."
                ),
                "description_fr": (
                    f"La TVQ semble calculée sur le montant incluant la TPS "
                    f"({qst_amount}$ facturé vs {correct_qst}$ correct). "
                    f"La TVQ doit être appliquée sur le sous-total avant taxes, "
                    f"et non sur le montant incluant la TPS."
                ),
                "correct_calculation": {
                    "subtotal": str(subtotal),
                    "correct_qst": str(correct_qst),
                    "charged_qst": str(qst_amount),
                    "overcharge": str(_round(qst_amount - correct_qst)),
                },
            })

    # ---- 2. Large business ITR restriction ----------------------------------
    if company_revenue > Decimal("10000000") and subtotal > _ZERO:
        itr_claimed = _to_decimal(document.get("itr_claimed", 0))
        if itr_claimed > _ZERO:
            expense_type = str(document.get("expense_type", "")).strip().lower()
            restricted_types = {"fuel", "carburant", "vehicle", "véhicule",
                                "road_vehicle", "véhicule_routier", "energy",
                                "énergie", "telecom", "télécommunications"}
            if expense_type in restricted_types or any(
                kw in expense_type for kw in ("fuel", "vehicle", "véhicule", "carburant")
            ):
                issues.append({
                    "error_type": "large_business_itr_restricted",
                    "severity": "critical",
                    "description_en": (
                        f"Company revenue exceeds $10M — ITR on fuel/road vehicles "
                        f"is restricted. Full ITR of ${itr_claimed} cannot be claimed."
                    ),
                    "description_fr": (
                        f"Le chiffre d'affaires dépasse 10M$ — le RTI sur le "
                        f"carburant/véhicules routiers est restreint. Le RTI complet "
                        f"de {itr_claimed}$ ne peut être réclamé."
                    ),
                    "correct_calculation": {
                        "company_revenue": str(company_revenue),
                        "itr_claimed": str(itr_claimed),
                        "restriction": "Large business ITR restriction applies",
                    },
                })

    # ---- 3. Unregistered supplier charging tax ------------------------------
    if vendor_revenue > _ZERO and vendor_revenue < Decimal("30000"):
        if gst_amount > _ZERO or qst_amount > _ZERO:
            issues.append({
                "error_type": "unregistered_supplier_charging_tax",
                "severity": "warning",
                "description_en": (
                    f"Vendor with revenue under $30,000 (${vendor_revenue}) "
                    f"is charging GST/QST. Small suppliers under the $30K "
                    f"threshold are not required to register or charge tax."
                ),
                "description_fr": (
                    f"Fournisseur avec un chiffre d'affaires inférieur à 30 000$ "
                    f"({vendor_revenue}$) facture la TPS/TVQ. Les petits "
                    f"fournisseurs sous le seuil de 30 000$ n'ont pas à "
                    f"s'inscrire ni à facturer les taxes."
                ),
                "correct_calculation": {
                    "vendor_revenue": str(vendor_revenue),
                    "threshold": "30000",
                    "gst_charged": str(gst_amount),
                    "qst_charged": str(qst_amount),
                },
            })

    # ---- 4. Wrong QST rate --------------------------------------------------
    if subtotal > _ZERO and qst_amount > _ZERO:
        old_qst = _round(subtotal * _OLD_QST_RATE)
        if abs(qst_amount - old_qst) < Decimal("0.02") and abs(qst_amount - _round(subtotal * QST_RATE)) > Decimal("0.02"):
            issues.append({
                "error_type": "wrong_qst_rate",
                "severity": "critical",
                "description_en": (
                    f"QST calculated at old 9.5% rate (${old_qst}) instead of "
                    f"current 9.975% (${_round(subtotal * QST_RATE)}). "
                    f"The 9.975% rate has been in effect since January 1, 2013."
                ),
                "description_fr": (
                    f"TVQ calculée à l'ancien taux de 9,5% ({old_qst}$) au lieu "
                    f"du taux actuel de 9,975% ({_round(subtotal * QST_RATE)}$). "
                    f"Le taux de 9,975% est en vigueur depuis le 1er janvier 2013."
                ),
                "correct_calculation": {
                    "subtotal": str(subtotal),
                    "wrong_rate": "0.095",
                    "correct_rate": "0.09975",
                    "wrong_qst": str(old_qst),
                    "correct_qst": str(_round(subtotal * QST_RATE)),
                },
            })

    # ---- 5. Missing registration number -------------------------------------
    if subtotal > Decimal("30") and (gst_amount > _ZERO or qst_amount > _ZERO):
        if not gst_reg and not qst_reg:
            issues.append({
                "error_type": "missing_registration_number",
                "severity": "warning",
                "description_en": (
                    f"Taxable invoice over $30 (subtotal ${subtotal}) has no "
                    f"GST/QST registration number. ITC/ITR claims require "
                    f"the supplier's registration numbers on the invoice."
                ),
                "description_fr": (
                    f"Facture taxable de plus de 30$ (sous-total {subtotal}$) "
                    f"sans numéro d'inscription TPS/TVQ. Les demandes de "
                    f"CTI/RTI exigent les numéros d'inscription du fournisseur."
                ),
                "correct_calculation": None,
            })

    # ---- 6. Wrong provincial tax --------------------------------------------
    if vendor_province:
        if vendor_province in HST_PROVINCES and qst_amount > _ZERO:
            issues.append({
                "error_type": "wrong_provincial_tax",
                "severity": "critical",
                "description_en": (
                    f"Vendor in {vendor_province} is charging QST (${qst_amount}). "
                    f"{vendor_province} uses HST, not GST+QST."
                ),
                "description_fr": (
                    f"Fournisseur en {vendor_province} facture la TVQ ({qst_amount}$). "
                    f"Le {vendor_province} utilise la TVH, pas la TPS+TVQ."
                ),
                "correct_calculation": {
                    "vendor_province": vendor_province,
                    "expected_tax": "HST",
                    "charged_tax": "QST",
                },
            })
        elif vendor_province == QC_PROVINCE:
            hst_amount = _to_decimal(document.get("hst_amount", 0))
            if hst_amount > _ZERO:
                issues.append({
                    "error_type": "wrong_provincial_tax",
                    "severity": "critical",
                    "description_en": (
                        f"Quebec vendor charging HST (${hst_amount}). "
                        f"Quebec uses GST+QST, not HST."
                    ),
                    "description_fr": (
                        f"Fournisseur du Québec facture la TVH ({hst_amount}$). "
                        f"Le Québec utilise la TPS+TVQ, pas la TVH."
                    ),
                    "correct_calculation": {
                        "vendor_province": "QC",
                        "expected_tax": "GST+QST",
                        "charged_tax": "HST",
                    },
                })
            # FIX 22: Detect HST-coded Quebec vendor via total context
            # If GST and QST are both zero but total implies HST rate
            if (
                gst_amount == _ZERO
                and qst_amount == _ZERO
                and hst_amount == _ZERO
                and subtotal > _ZERO
                and total_with_tax > subtotal
            ):
                implied_rate = (total_with_tax - subtotal) / subtotal
                # Check if implied rate matches HST (13% ON or 15% ATL)
                is_hst_rate = (
                    abs(implied_rate - HST_RATE_ON) < Decimal("0.015")
                    or abs(implied_rate - HST_RATE_ATL) < Decimal("0.015")
                )
                if is_hst_rate:
                    issues.append({
                        "error_type": "wrong_provincial_tax",
                        "severity": "high",
                        "description_en": (
                            f"Quebec vendor total (${total_with_tax}) implies HST rate "
                            f"({implied_rate:.4f}) but GST and QST are both $0. "
                            f"Quebec vendors must charge GST+QST, not HST."
                        ),
                        "description_fr": (
                            f"Le total du fournisseur québécois ({total_with_tax}$) "
                            f"implique un taux de TVH ({implied_rate:.4f}) mais la TPS "
                            f"et la TVQ sont à 0$. Les fournisseurs du Québec doivent "
                            f"facturer la TPS+TVQ, pas la TVH."
                        ),
                        "correct_calculation": {
                            "vendor_province": "QC",
                            "subtotal": str(subtotal),
                            "total_with_tax": str(total_with_tax),
                            "implied_rate": str(_round(implied_rate * Decimal("100"))) + "%",
                            "expected_tax": "GST+QST",
                        },
                    })
        # Non-Quebec vendor should not have GST_QST or QST charges
        if vendor_province != QC_PROVINCE and vendor_province not in HST_PROVINCES:
            tax_code_doc = str(document.get("tax_code", "")).strip().upper()
            if tax_code_doc in ("GST_QST", "QST") or (
                vendor_province and qst_amount > _ZERO
                and vendor_province in (GST_ONLY_PROVINCES | frozenset(PST_PROVINCES.keys()))
            ):
                issues.append({
                    "error_type": "cross_provincial_tax_error",
                    "severity": "critical",
                    "description_en": (
                        f"Vendor in {vendor_province} coded as GST+QST or QST. "
                        f"QST applies only to Quebec vendors. "
                        f"Use GST_ONLY for this province."
                    ),
                    "description_fr": (
                        f"Fournisseur en {vendor_province} codé TPS+TVQ ou TVQ. "
                        f"La TVQ s'applique uniquement aux fournisseurs du Québec. "
                        f"Utilisez GST_ONLY pour cette province."
                    ),
                    "correct_calculation": {
                        "vendor_province": vendor_province,
                        "expected_tax": "GST_ONLY",
                        "charged_tax": tax_code_doc or "QST",
                    },
                })

    # ---- 7. Exempt item taxed -----------------------------------------------
    if category in _EXEMPT_CATEGORIES and (gst_amount > _ZERO or qst_amount > _ZERO):
        issues.append({
            "error_type": "exempt_item_taxed",
            "severity": "critical",
            "description_en": (
                f"Exempt category '{category}' has been charged "
                f"GST (${gst_amount}) and/or QST (${qst_amount}). "
                f"This category is tax-exempt under the Excise Tax Act."
            ),
            "description_fr": (
                f"Catégorie exonérée '{category}' a été taxée — "
                f"TPS ({gst_amount}$) et/ou TVQ ({qst_amount}$). "
                f"Cette catégorie est exonérée en vertu de la Loi sur "
                f"la taxe d'accise."
            ),
            "correct_calculation": {
                "category": category,
                "correct_gst": "0.00",
                "correct_qst": "0.00",
                "charged_gst": str(gst_amount),
                "charged_qst": str(qst_amount),
            },
        })

    # ---- 8. Quick Method rate error -----------------------------------------
    if quick_method and subtotal > _ZERO and remittance_rate > _ZERO:
        expected_rate = QUICK_METHOD_RATES.get(quick_method_type, Decimal("0.036"))
        if abs(remittance_rate - expected_rate) > Decimal("0.001"):
            issues.append({
                "error_type": "quick_method_rate_error",
                "severity": "critical",
                "description_en": (
                    f"Quick Method remittance rate {remittance_rate} does not "
                    f"match expected rate {expected_rate} for '{quick_method_type}'. "
                    f"Using the full rate instead of the Quick Method rate "
                    f"results in over-remittance."
                ),
                "description_fr": (
                    f"Le taux de versement de la méthode rapide {remittance_rate} "
                    f"ne correspond pas au taux attendu {expected_rate} pour "
                    f"'{quick_method_type}'. L'utilisation du taux complet au "
                    f"lieu du taux de la méthode rapide entraîne un "
                    f"versement excédentaire."
                ),
                "correct_calculation": {
                    "quick_method_type": quick_method_type,
                    "used_rate": str(remittance_rate),
                    "correct_rate": str(expected_rate),
                    "subtotal": str(subtotal),
                    "correct_remittance": str(_round(subtotal * expected_rate)),
                    "actual_remittance": str(_round(subtotal * remittance_rate)),
                },
            })

    return issues


# ---------------------------------------------------------------------------
# Cross-provincial ITC/ITR — QST self-assessment for Quebec registrants
# ---------------------------------------------------------------------------

def calculate_cross_provincial_itc_itr(
    expense_amount: Decimal,
    tax_code: Any,
    *,
    vendor_province: str = "",
    client_province: str = "",
) -> dict[str, Any]:
    """
    Calculate ITC/ITR with cross-provincial QST self-assessment.

    When a **Quebec registrant** (``client_province="QC"``) purchases from
    an HST province (ON, NB, NS, NL, PE), the HST paid is fully claimable
    as ITC on the federal return.  Additionally, the buyer must
    **self-assess QST** on the pre-tax amount and can then claim the
    corresponding ITR.

    If the provinces do not trigger cross-provincial logic, this function
    delegates to :func:`calculate_itc_itr` unchanged.

    ``expense_amount`` is the **pre-tax (net) amount**.

    Returns the same dict as ``calculate_itc_itr`` plus:
        qst_self_assessed       — QST the QC buyer must remit
        qst_self_assessed_itr   — ITR claimable on the self-assessed QST
        cross_provincial        — True when self-assessment applies
        advisory_notes          — bilingual explanation list
    """
    expense_amount = _to_decimal(expense_amount)
    vp = str(vendor_province).strip().upper()
    cp = str(client_province).strip().upper()
    tc = _normalize_code(tax_code)

    base = calculate_itc_itr(expense_amount, tc)

    # Enrich with cross-provincial defaults
    base["qst_self_assessed"] = _ZERO
    base["qst_self_assessed_itr"] = _ZERO
    base["cross_provincial"] = False
    base["advisory_notes"] = []

    if cp != QC_PROVINCE:
        return base

    # QC buyer purchasing from an HST province
    if vp in HST_PROVINCES and tc in ("HST", "HST_ATL"):
        qst_self = _round(expense_amount * QST_RATE)
        # ITR recovery percentage follows the same itr_pct as code T (100%)
        t_entry = TAX_CODE_REGISTRY["T"]
        qst_itr = _round(qst_self * t_entry["itr_pct"])

        base["qst_self_assessed"] = qst_self
        base["qst_self_assessed_itr"] = qst_itr
        base["cross_provincial"] = True
        base["total_recoverable"] = base["total_recoverable"] + qst_itr
        base["advisory_notes"].append(
            f"Achat interprovincial: fournisseur en {vp} (TVH), "
            f"acheteur inscrit au QC. Auto-cotisation TVQ de "
            f"{qst_self}$ requise; RTI récupérable de {qst_itr}$. / "
            f"Cross-provincial purchase: vendor in {vp} (HST), "
            f"QC-registered buyer. QST self-assessment of "
            f"${qst_self} required; ITR of ${qst_itr} claimable."
        )
        return base

    # QC buyer purchasing from a GST-only province (AB, NT, NU, YT)
    if vp in GST_ONLY_PROVINCES and tc in ("GST_ONLY", "T", "GST_QST"):
        qst_self = _round(expense_amount * QST_RATE)
        t_entry = TAX_CODE_REGISTRY["T"]
        qst_itr = _round(qst_self * t_entry["itr_pct"])

        base["qst_self_assessed"] = qst_self
        base["qst_self_assessed_itr"] = qst_itr
        base["cross_provincial"] = True
        base["total_recoverable"] = base["total_recoverable"] + qst_itr
        base["advisory_notes"].append(
            f"Achat interprovincial: fournisseur en {vp} (TPS seulement), "
            f"acheteur inscrit au QC. Auto-cotisation TVQ de "
            f"{qst_self}$ requise; RTI récupérable de {qst_itr}$. / "
            f"Cross-provincial purchase: vendor in {vp} (GST only), "
            f"QC-registered buyer. QST self-assessment of "
            f"${qst_self} required; ITR of ${qst_itr} claimable."
        )
        return base

    # QC buyer purchasing from a PST province (BC, MB, SK)
    if vp in PST_PROVINCES and tc in ("GST_ONLY", "T", "GST_QST"):
        qst_self = _round(expense_amount * QST_RATE)
        t_entry = TAX_CODE_REGISTRY["T"]
        qst_itr = _round(qst_self * t_entry["itr_pct"])

        base["qst_self_assessed"] = qst_self
        base["qst_self_assessed_itr"] = qst_itr
        base["cross_provincial"] = True
        base["total_recoverable"] = base["total_recoverable"] + qst_itr
        base["advisory_notes"].append(
            f"Achat interprovincial: fournisseur en {vp} (TPS + TVP), "
            f"acheteur inscrit au QC. Auto-cotisation TVQ de "
            f"{qst_self}$ requise; RTI récupérable de {qst_itr}$. "
            f"La TVP de {vp} ({PST_PROVINCES[vp]}) n'est pas récupérable. / "
            f"Cross-provincial purchase: vendor in {vp} (GST + PST), "
            f"QC-registered buyer. QST self-assessment of "
            f"${qst_self} required; ITR of ${qst_itr} claimable. "
            f"{vp} PST ({PST_PROVINCES[vp]}) is not recoverable."
        )
        return base

    return base


def cross_provincial_itc_itr_from_total(
    total: Decimal,
    tax_code: str,
    *,
    vendor_province: str = "",
    client_province: str = "",
) -> dict[str, Any]:
    """
    Compute cross-provincial ITC/ITR when ``total`` is tax-inclusive.

    Extracts the pre-tax amount using the same logic as
    :func:`_itc_itr_from_total`, then delegates to
    :func:`calculate_cross_provincial_itc_itr`.
    """
    total = _to_decimal(total)
    tc = _normalize_code(tax_code)

    if tc in ("T", "GST_QST"):
        extracted = extract_tax_from_total(total)
        pre_tax = extracted["pre_tax"]
    elif tc == "M":
        extracted = extract_tax_from_total(total)
        pre_tax = extracted["pre_tax"]
    elif tc in ("HST", "HST_ATL"):
        hst_rate = _registry_entry(tc)["hst_rate"]
        pre_tax = _round(total / (_ONE + hst_rate))
    else:
        pre_tax = total

    return calculate_cross_provincial_itc_itr(
        pre_tax, tc,
        vendor_province=vendor_province,
        client_province=client_province,
    )


# ---------------------------------------------------------------------------
# Pro-rata tax allocation across mixed settlement methods
# ---------------------------------------------------------------------------

def allocate_tax_to_payments(
    invoice_total: Decimal,
    tax_code: Any,
    payments: list[dict[str, Any]],
    *,
    vendor_province: str = "",
    client_province: str = "",
) -> dict[str, Any]:
    """
    Split tax proportionally across multiple payment methods.

    Given a tax-inclusive ``invoice_total`` and a list of ``payments``
    (each with ``"amount"`` and ``"method"``), compute the pre-tax,
    tax, ITC, and ITR portion attributable to each payment.

    Example::

        allocate_tax_to_payments(
            Decimal("10000"), "HST",
            [
                {"amount": Decimal("8000"), "method": "bank_transfer"},
                {"amount": Decimal("2000"), "method": "credit_note"},
            ],
            vendor_province="ON",
            client_province="QC",
        )

    Returns
    -------
    dict with keys:
        invoice_total, tax_code, pre_tax, total_tax,
        payment_allocations: list[dict]   — one per payment
        cross_provincial: dict | None     — QST self-assessment if applicable
        warnings: list[str]
    """
    invoice_total = _to_decimal(invoice_total)
    tc = _normalize_code(tax_code)
    warnings: list[str] = []

    # Extract pre-tax from the invoice total
    if tc in ("T", "GST_QST", "M"):
        extracted = extract_tax_from_total(invoice_total)
        pre_tax = extracted["pre_tax"]
        total_tax = extracted["total_tax"]
    elif tc in ("HST", "HST_ATL"):
        hst_rate = _registry_entry(tc)["hst_rate"]
        pre_tax = _round(invoice_total / (_ONE + hst_rate))
        total_tax = invoice_total - pre_tax
    else:
        pre_tax = invoice_total
        total_tax = _ZERO

    # Validate payment sum
    payment_sum = _ZERO
    for p in payments:
        payment_sum += _to_decimal(p.get("amount", 0))

    diff = abs(payment_sum - invoice_total)
    if diff > Decimal("0.02"):
        warnings.append(
            f"Somme des paiements ({payment_sum}$) ≠ total facture "
            f"({invoice_total}$), écart de {diff}$. / "
            f"Payment sum (${payment_sum}) ≠ invoice total "
            f"(${invoice_total}), difference ${diff}."
        )

    # Pro-rata allocation
    allocations: list[dict[str, Any]] = []
    allocated_pre_tax = _ZERO
    allocated_tax = _ZERO

    for i, p in enumerate(payments):
        p_amount = _to_decimal(p.get("amount", 0))
        p_method = str(p.get("method", "unknown")).strip()

        if invoice_total == _ZERO:
            ratio = _ZERO
        else:
            ratio = p_amount / invoice_total

        if i == len(payments) - 1:
            # Last payment gets the remainder to avoid rounding drift
            p_pre_tax = pre_tax - allocated_pre_tax
            p_tax = total_tax - allocated_tax
        else:
            p_pre_tax = _round(pre_tax * ratio)
            p_tax = _round(total_tax * ratio)
            allocated_pre_tax += p_pre_tax
            allocated_tax += p_tax

        allocations.append({
            "method": p_method,
            "payment_amount": p_amount,
            "pre_tax_portion": p_pre_tax,
            "tax_portion": p_tax,
            "ratio": round(float(ratio), 6),
        })

    # Cross-provincial enrichment
    cross_prov = None
    cp = str(client_province).strip().upper()
    vp = str(vendor_province).strip().upper()
    if cp == QC_PROVINCE and vp and vp != QC_PROVINCE:
        cross_prov = calculate_cross_provincial_itc_itr(
            pre_tax, tc,
            vendor_province=vp,
            client_province=cp,
        )

    return {
        "invoice_total": invoice_total,
        "tax_code": tc or "NONE",
        "pre_tax": pre_tax,
        "total_tax": total_tax,
        "payment_allocations": allocations,
        "cross_provincial": cross_prov,
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# Business-use apportionment — scales ITC/ITR by commercial-use percentage
# ---------------------------------------------------------------------------

def apply_business_use_apportionment(
    itc_itr_result: dict[str, Any],
    business_use_pct: Decimal,
) -> dict[str, Any]:
    """
    Scale recoverable ITC/ITR by a business-use percentage.

    When a purchase is used partially for commercial activities and
    partially for exempt or personal purposes, only the commercial
    fraction of the input tax is recoverable.

    This function takes the output of :func:`calculate_itc_itr` (or
    :func:`calculate_cross_provincial_itc_itr`) and applies the
    ``business_use_pct`` multiplier (0.00–1.00) to all recoverable
    amounts **without modifying the tax-paid figures**.

    Parameters
    ----------
    itc_itr_result : dict
        Output from ``calculate_itc_itr`` or
        ``calculate_cross_provincial_itc_itr``.
    business_use_pct : Decimal
        Fraction of the purchase used for commercial activities
        (e.g. ``Decimal("0.70")`` for 70%).

    Returns
    -------
    dict — a copy of ``itc_itr_result`` with adjusted recoverable
    amounts and added keys:
        business_use_pct            — the multiplier applied
        full_gst_recoverable        — recoverable before apportionment
        full_qst_recoverable        — recoverable before apportionment
        full_hst_recoverable        — recoverable before apportionment
        full_total_recoverable      — recoverable before apportionment
        apportionment_applied       — True
    """
    pct = _to_decimal(business_use_pct)
    if pct < _ZERO or pct > _ONE:
        raise ValueError(
            f"business_use_pct must be between 0 and 1, got {pct}"
        )

    result = dict(itc_itr_result)

    # Preserve the full (unapportioned) recoverable amounts
    result["full_gst_recoverable"] = result.get("gst_recoverable", _ZERO)
    result["full_qst_recoverable"] = result.get("qst_recoverable", _ZERO)
    result["full_hst_recoverable"] = result.get("hst_recoverable", _ZERO)
    result["full_total_recoverable"] = result.get("total_recoverable", _ZERO)

    # Scale recoverable amounts
    result["gst_recoverable"] = _round(result["full_gst_recoverable"] * pct)
    result["qst_recoverable"] = _round(result["full_qst_recoverable"] * pct)
    result["hst_recoverable"] = _round(result["full_hst_recoverable"] * pct)

    # Cross-provincial QST self-assessment ITR (if present)
    full_qst_sa_itr = result.get("qst_self_assessed_itr", _ZERO)
    if full_qst_sa_itr > _ZERO:
        result["full_qst_self_assessed_itr"] = full_qst_sa_itr
        result["qst_self_assessed_itr"] = _round(full_qst_sa_itr * pct)

    result["total_recoverable"] = (
        result["gst_recoverable"]
        + result["qst_recoverable"]
        + result["hst_recoverable"]
        + result.get("qst_self_assessed_itr", _ZERO)
    )

    result["business_use_pct"] = pct
    result["apportionment_applied"] = True

    return result


def calculate_itc_itr_with_apportionment(
    expense_amount: Decimal,
    tax_code: Any,
    *,
    business_use_pct: Decimal = _ONE,
    vendor_province: str = "",
    client_province: str = "",
) -> dict[str, Any]:
    """
    Calculate ITC/ITR with optional business-use apportionment and
    cross-provincial self-assessment.

    Chains :func:`calculate_cross_provincial_itc_itr` →
    :func:`apply_business_use_apportionment`.

    When ``business_use_pct`` is ``1`` (default), the result is
    identical to :func:`calculate_cross_provincial_itc_itr`.

    Parameters
    ----------
    expense_amount   : Pre-tax (net) expense amount.
    tax_code         : Tax code string.
    business_use_pct : Commercial-use fraction (0.00–1.00).
    vendor_province  : Vendor's province code.
    client_province  : Client's province code.

    Returns
    -------
    dict — same structure as ``calculate_cross_provincial_itc_itr``
    with apportionment fields added when ``business_use_pct < 1``.
    """
    base = calculate_cross_provincial_itc_itr(
        expense_amount,
        tax_code,
        vendor_province=vendor_province,
        client_province=client_province,
    )

    pct = _to_decimal(business_use_pct)
    if pct < _ONE:
        return apply_business_use_apportionment(base, pct)

    return base


def itc_itr_from_total_with_apportionment(
    total: Decimal,
    tax_code: str,
    *,
    business_use_pct: Decimal = _ONE,
    vendor_province: str = "",
    client_province: str = "",
) -> dict[str, Any]:
    """
    Compute ITC/ITR from a tax-inclusive total with optional
    business-use apportionment and cross-provincial self-assessment.

    Extracts the pre-tax amount, then delegates to
    :func:`calculate_itc_itr_with_apportionment`.
    """
    total = _to_decimal(total)
    tc = _normalize_code(tax_code)

    if tc in ("T", "GST_QST", "M"):
        extracted = extract_tax_from_total(total)
        pre_tax = extracted["pre_tax"]
    elif tc in ("HST", "HST_ATL"):
        hst_rate = _registry_entry(tc)["hst_rate"]
        pre_tax = _round(total / (_ONE + hst_rate))
    else:
        pre_tax = total

    return calculate_itc_itr_with_apportionment(
        pre_tax,
        tc,
        business_use_pct=business_use_pct,
        vendor_province=vendor_province,
        client_province=client_province,
    )


# ---------------------------------------------------------------------------
# Place of Supply Rules — ETA Schedule IX
# ---------------------------------------------------------------------------

def place_of_supply_rules(
    supply_type: str,
    vendor_province: str = "",
    buyer_province: str = "",
    *,
    delivery_destination: str = "",
    service_location: str = "",
    property_location: str = "",
    origin: str = "",
    destination: str = "",
    is_separate_shipping: bool = False,
    principal_supply_province: str = "",
) -> dict[str, Any]:
    """
    Determine the place of supply under ETA Schedule IX.

    Covers five main rules plus shipping:

    Rule 1: Tangible personal property — delivery destination.
    Rule 2: Services — where predominantly performed.
    Rule 3: Real property — where situated.
    Rule 4: Intangibles — where recipient belongs (buyer province).
    Rule 5: Transportation — origin to destination (use destination).
    Shipping: if same contract as principal supply → follows principal.
              If separate contract → delivery destination.

    Parameters
    ----------
    supply_type : str
        One of: "tangible", "service", "real_property", "intangible",
                "transportation", "shipping".
    vendor_province  : Two-letter province code of vendor.
    buyer_province   : Two-letter province code of buyer.
    delivery_destination : Province where tangible goods are delivered.
    service_location     : Province where service is predominantly performed.
    property_location    : Province where real property is situated.
    origin               : Province of origin (transportation).
    destination          : Province of destination (transportation).
    is_separate_shipping : Whether shipping is a separate supply from goods.
    principal_supply_province : Province of the principal supply (for bundled shipping).

    Returns
    -------
    dict with keys:
        province_of_supply : str — two-letter province code or "AMBIGUOUS"
        rule_applied       : str — description of the rule
        tax_regime         : str — HST / GST_QST / GST_ONLY / GST_PST / AMBIGUOUS
        gst_rate           : Decimal
        hst_rate           : Decimal
        qst_rate           : Decimal
        notes              : str
    """
    st = supply_type.strip().lower()
    vp = vendor_province.strip().upper()
    bp = buyer_province.strip().upper()

    prov = "AMBIGUOUS"
    rule = ""

    if st == "tangible":
        # Rule 1 — delivery destination
        dd = delivery_destination.strip().upper() or bp
        prov = dd if dd else (bp or vp or "AMBIGUOUS")
        rule = "Rule 1: Tangible personal property — delivery destination"

    elif st == "service":
        # Rule 2 — where predominantly performed
        sl = service_location.strip().upper()
        if sl:
            prov = sl
            rule = "Rule 2: Services — location where predominantly performed"
        elif bp and vp and bp == vp:
            prov = bp
            rule = "Rule 2: Services — vendor and buyer in same province"
        elif bp and vp and bp != vp:
            prov = "AMBIGUOUS"
            rule = "Rule 2: Services — cross-provincial, location unclear"
        else:
            prov = bp or vp or "AMBIGUOUS"
            rule = "Rule 2: Services — defaulting to known province"

    elif st == "real_property":
        # Rule 3 — where situated
        pl = property_location.strip().upper()
        prov = pl if pl else "AMBIGUOUS"
        rule = "Rule 3: Real property — where property is situated"

    elif st == "intangible":
        # Rule 4 — where recipient belongs
        prov = bp if bp else (vp or "AMBIGUOUS")
        rule = "Rule 4: Intangibles — where recipient (buyer) belongs"

    elif st == "transportation":
        # Rule 5 — origin to destination, use destination
        dest = destination.strip().upper() or bp
        prov = dest if dest else "AMBIGUOUS"
        rule = "Rule 5: Transportation — destination province"

    elif st == "shipping":
        if not is_separate_shipping and principal_supply_province:
            # Follows principal supply
            prov = principal_supply_province.strip().upper()
            rule = "Shipping: follows principal supply (same contract)"
        else:
            # Separate contract — delivery destination
            dd = delivery_destination.strip().upper() or bp
            prov = dd if dd else "AMBIGUOUS"
            rule = "Shipping: separate contract — delivery destination"

    else:
        prov = "AMBIGUOUS"
        rule = f"Unknown supply type: {supply_type}"

    # Determine tax regime from province
    regime = _province_to_tax_regime(prov)

    return {
        "province_of_supply": prov,
        "rule_applied": rule,
        "tax_regime": regime["tax_regime"],
        "gst_rate": regime["gst_rate"],
        "hst_rate": regime["hst_rate"],
        "qst_rate": regime["qst_rate"],
        "notes": regime.get("notes", ""),
    }


def _province_to_tax_regime(province: str) -> dict[str, Any]:
    """Map a province code to its tax regime and rates."""
    if province == "AMBIGUOUS" or not province:
        return {
            "tax_regime": "AMBIGUOUS",
            "gst_rate": _ZERO, "hst_rate": _ZERO, "qst_rate": _ZERO,
            "notes": "Place of supply ambiguous — requires human review",
        }
    if province == "QC":
        return {
            "tax_regime": "GST_QST",
            "gst_rate": GST_RATE, "hst_rate": _ZERO, "qst_rate": QST_RATE,
            "notes": "Quebec: GST 5% + QST 9.975%",
        }
    if province == "ON":
        return {
            "tax_regime": "HST",
            "gst_rate": _ZERO, "hst_rate": HST_RATE_ON, "qst_rate": _ZERO,
            "notes": "Ontario: HST 13%",
        }
    if province in ATL_PROVINCES:
        return {
            "tax_regime": "HST",
            "gst_rate": _ZERO, "hst_rate": HST_RATE_ATL, "qst_rate": _ZERO,
            "notes": f"{province}: HST 15%",
        }
    if province in GST_ONLY_PROVINCES:
        return {
            "tax_regime": "GST_ONLY",
            "gst_rate": GST_RATE, "hst_rate": _ZERO, "qst_rate": _ZERO,
            "notes": f"{province}: GST 5% only",
        }
    if province in PST_PROVINCES:
        return {
            "tax_regime": "GST_PST",
            "gst_rate": GST_RATE, "hst_rate": _ZERO, "qst_rate": _ZERO,
            "notes": f"{province}: GST 5% + PST {PST_PROVINCES[province] * 100}% (PST non-recoverable)",
        }
    return {
        "tax_regime": "GST_ONLY",
        "gst_rate": GST_RATE, "hst_rate": _ZERO, "qst_rate": _ZERO,
        "notes": f"Unknown province {province} — defaulting to GST only",
    }


# ---------------------------------------------------------------------------
# PART 8 — Explicit unresolvability
# ---------------------------------------------------------------------------

def cannot_determine_response(
    reason: str,
    information_needed: list[str],
    document_id: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Handle cases where tax treatment genuinely cannot be determined.

    Rules:
    1. Do NOT fill gaps with assumptions.
    2. Do NOT move forward with incomplete data.
    3. Set document review_status=NeedsReview.
    4. Block all ITC/ITR claims until resolved.
    5. Return structured response with required information.
    """
    now = _utc_now()

    # Update document status
    try:
        conn.execute(
            """UPDATE documents
               SET review_status = 'NeedsReview',
                   review_reason = 'cannot_determine_tax_treatment',
                   updated_at = ?
               WHERE document_id = ?""",
            (now, document_id),
        )
        conn.commit()
    except Exception:
        pass  # Column may not exist — best effort

    return {
        "can_determine": False,
        "reason": reason,
        "information_needed": information_needed,
        "document_id": document_id,
        "review_status": "NeedsReview",
        "review_reason": "cannot_determine_tax_treatment",
        "block_itc_itr": True,
        "display_message_en": (
            "Tax treatment cannot be determined — information required: "
            + "; ".join(information_needed)
        ),
        "display_message_fr": (
            "Traitement fiscal indéterminé — informations requises : "
            + "; ".join(information_needed)
        ),
    }


def _utc_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
