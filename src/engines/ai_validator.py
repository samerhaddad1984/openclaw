"""Validation and correction layer for AI-extracted document/line data.

Catches math errors, invalid GL/tax codes, suspicious amounts, and
reconciliation gaps before they reach the database. Auto-corrects
known-bad codes to safe defaults.
"""
import re
import logging
import sqlite3
from pathlib import Path
from typing import Any, List, Dict, Optional, Tuple

VALID_GL_ACCOUNTS = {
    '5400', '5410', '5420', '5430', '5440', '5450',
    '5500', '5640', '5650', '5750', '1820', '1830',
}
VALID_TAX_CODES = {'T', 'E', 'M', 'Z'}


def validate_line_item(item: Dict, line_num: int) -> Tuple[bool, List[str]]:
    errors: List[str] = []

    if not item.get('description'):
        errors.append(f'Line {line_num}: missing description')

    total = item.get('total_price')
    if total is None:
        errors.append(f'Line {line_num}: missing total_price')
        return False, errors

    qty = float(item.get('quantity') or 1.0)
    unit = item.get('unit_price')
    if unit is not None:
        calculated = round(qty * float(unit), 2)
        actual = round(float(total), 2)
        if abs(calculated - actual) > 0.03:
            errors.append(
                f'Line {line_num}: math error {qty} × {unit} = {calculated} ≠ {actual}'
            )

    gl = item.get('gl_account', '')
    if gl and gl not in VALID_GL_ACCOUNTS:
        errors.append(f'Line {line_num}: invalid GL {gl}')
        item['gl_account'] = '5440'

    tax = item.get('tax_code', '')
    if tax and tax not in VALID_TAX_CODES:
        errors.append(f'Line {line_num}: invalid tax code {tax}')
        item['tax_code'] = 'T'

    if abs(float(total)) > 50000:
        errors.append(f'Line {line_num}: suspiciously large amount {total}')

    return len(errors) == 0, errors


def validate_line_items(
    items: List[Dict],
    invoice_total: Optional[float] = None,
) -> Tuple[List[Dict], List[str], bool]:
    all_errors: List[str] = []
    valid_items: List[Dict] = []

    for i, item in enumerate(items):
        _valid, errors = validate_line_item(item, i + 1)
        all_errors.extend(errors)
        valid_items.append(item)

    if invoice_total and valid_items:
        line_sum = sum(float(item.get('total_price') or 0) for item in valid_items)
        gap = abs(line_sum - float(invoice_total))
        if gap > 0.03:
            all_errors.append(
                f'Reconciliation gap: lines sum {line_sum:.2f} vs invoice '
                f'{invoice_total:.2f} (gap {gap:.2f})'
            )

    has_errors = len(all_errors) > 0
    if all_errors:
        logging.warning('AI validation errors: %s', all_errors)

    return valid_items, all_errors, has_errors


def validate_document_extraction(result: Dict) -> Tuple[Dict, List[str]]:
    errors: List[str] = []

    amount = result.get('amount') or result.get('total_amount')
    if amount is not None and amount != '':
        try:
            amt = float(amount)
            if amt > 1_000_000:
                errors.append(f'Suspiciously large amount: {amt}')
                result['amount'] = None
            if amt < 0:
                errors.append(f'Negative total amount: {amt}')
        except (TypeError, ValueError):
            errors.append(f'Invalid amount format: {amount}')

    date = result.get('document_date')
    if date:
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', str(date)):
            errors.append(f'Invalid date format: {date}')
            result['document_date'] = None

    gl = result.get('gl_account', '')
    if gl and gl not in VALID_GL_ACCOUNTS:
        errors.append(f'Invalid GL account: {gl}')
        result['gl_account'] = '5440'

    tax = result.get('tax_code', '')
    if tax and tax not in VALID_TAX_CODES:
        errors.append(f'Invalid tax code: {tax}')
        result['tax_code'] = 'T'

    return result, errors


# ---------------------------------------------------------------------------
# Extraction-level sanity rules (Canadian-receipts pass)
# ---------------------------------------------------------------------------

# Combined federal + provincial tax in Canada tops out at 14.975% (GST 5% +
# QST 9.975%). 20% gives a margin for rounding / small-base receipts but
# still catches the pathological cases (e.g. a $500 "tax" on a $50 receipt).
MAX_TAX_FRACTION = 0.20

# Vendor confidence floor; below this we refuse to publish the vendor name
# and send the document to manual review.
VENDOR_CONFIDENCE_FLOOR = 0.70

# Outlier threshold on subtotal vs. vendor history (×p50). 10× is generous
# but catches the decimal-shift class of bugs (595 → 59,500).
VENDOR_SUBTOTAL_OUTLIER_MULT = 10.0


def validate_tax_sanity(total: Any, tax_total: Any) -> Optional[Dict[str, str]]:
    """Flag implausibly large tax amounts.

    Returns a sanity flag dict or None when the tax line is plausible.
    """
    try:
        t = float(total) if total is not None else None
        tx = float(tax_total) if tax_total is not None else None
    except (TypeError, ValueError):
        return None
    if t is None or tx is None or t <= 0:
        return None
    if tx > t * MAX_TAX_FRACTION:
        return {
            "flag": "implausible_tax",
            "severity": "HIGH",
            "detail": f"tax_total={tx} exceeds {MAX_TAX_FRACTION*100:.0f}% of total={t}",
        }
    return None


def lookup_vendor_amount_stats(
    vendor: Optional[str],
    db_path: Path,
) -> Optional[Tuple[float, float, int]]:
    """Return (p50, p95, sample_count) for a vendor or None if unknown.

    Reads from the vendor_amount_history table, which is maintained by
    rebuild_vendor_amount_history. Table is created lazily so callers
    don't need to worry about migrations.
    """
    if not vendor:
        return None
    try:
        conn = sqlite3.connect(str(db_path), timeout=5)
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS vendor_amount_history ("
                "  vendor_name TEXT PRIMARY KEY,"
                "  amount_p50 REAL,"
                "  amount_p95 REAL,"
                "  sample_count INTEGER"
                ")"
            )
            row = conn.execute(
                "SELECT amount_p50, amount_p95, sample_count "
                "FROM vendor_amount_history WHERE vendor_name = ?",
                (vendor,),
            ).fetchone()
            if row is None:
                return None
            return (row[0] or 0.0, row[1] or 0.0, row[2] or 0)
        finally:
            conn.close()
    except Exception:
        return None


def validate_subtotal_outlier(
    vendor: Optional[str],
    subtotal: Any,
    db_path: Path,
) -> Optional[Dict[str, Any]]:
    """Flag subtotals that are wildly larger than what we've seen for this vendor."""
    try:
        s = float(subtotal) if subtotal is not None else None
    except (TypeError, ValueError):
        return None
    if s is None or s <= 0:
        return None
    stats = lookup_vendor_amount_stats(vendor, db_path)
    if not stats:
        return None
    p50, _p95, n = stats
    # Require at least 3 prior samples before trusting the history — one or
    # two outliers in history shouldn't drive the bar.
    if n < 3 or p50 <= 0:
        return None
    if s > p50 * VENDOR_SUBTOTAL_OUTLIER_MULT:
        return {
            "flag": "subtotal_outlier",
            "severity": "HIGH",
            "detail": f"subtotal={s} > {VENDOR_SUBTOTAL_OUTLIER_MULT}× vendor p50 ({p50})",
        }
    return None


def apply_extraction_sanity(
    *,
    vendor: Optional[str],
    confidence: float,
    amount: Optional[float],
    subtotal: Optional[float],
    tax_total: Optional[float],
    gst_amount: Optional[float] = None,
    qst_amount: Optional[float] = None,
    review_status: str = "Ready",
    existing_flags: Optional[List[str]] = None,
    raw: Optional[Dict[str, Any]] = None,
    db_path: Optional[Path] = None,
) -> Tuple[Optional[str], str, List[str], List[Dict[str, Any]]]:
    """Run sanity rules and return (vendor, review_status, flags, raw_sanity).

    - Drops vendor to None when confidence is too low (BONUS).
    - Flags implausible tax (BUG 4).
    - Flags subtotal outliers vs vendor history (BUG 5).
    - Returns all findings as both a list of flag keys (for extraction_flags)
      and the raw dicts (persisted inside raw_result under sanity_flags).
    """
    flags: List[str] = list(existing_flags or [])
    raw_findings: List[Dict[str, Any]] = []

    # BUG 1 (belt-and-braces): downstream pipelines (AI-primary, regex) can
    # reassign vendor from raw['vendor_name'] after the initial scrub in
    # process_file. Re-check here so '<UNKNOWN>' / 'null' / 'N/A' never
    # make it to the DB no matter which extractor produced them.
    if vendor is not None:
        # Local import avoids a circular dependency at module-load time.
        from src.engines.ocr_engine import _is_vendor_placeholder  # noqa: PLC0415
        if _is_vendor_placeholder(vendor):
            flags.append("vendor_placeholder_stripped")
            raw_findings.append({
                "flag": "vendor_placeholder_stripped",
                "severity": "MED",
                "detail": f"placeholder={vendor!r}",
                "original_vendor": vendor,
            })
            vendor = None
            review_status = "NeedsReview"

    # BONUS: vendor confidence threshold.
    if vendor and confidence < VENDOR_CONFIDENCE_FLOOR:
        flags.append("vendor_low_confidence")
        raw_findings.append({
            "flag": "vendor_low_confidence",
            "severity": "MED",
            "detail": f"confidence={confidence:.2f} < {VENDOR_CONFIDENCE_FLOOR}",
            "original_vendor": vendor,
        })
        vendor = None
        review_status = "NeedsReview"

    # BUG 4: implausible tax.
    tx = validate_tax_sanity(amount if amount is not None else subtotal, tax_total)
    if tx:
        flags.append("implausible_tax")
        raw_findings.append(tx)
        review_status = "NeedsReview"

    # BUG 5: subtotal outlier vs vendor history.
    if db_path is not None:
        out = validate_subtotal_outlier(vendor, subtotal, db_path)
        if out:
            flags.append("subtotal_outlier")
            raw_findings.append(out)
            review_status = "NeedsReview"

    return vendor, review_status, flags, raw_findings


def rebuild_vendor_amount_history(db_path: Path) -> int:
    """Recompute p50/p95/sample_count per vendor from the documents table.

    Safe to call periodically; idempotent. Returns the number of vendor rows
    written.
    """
    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS vendor_amount_history ("
            "  vendor_name TEXT PRIMARY KEY,"
            "  amount_p50 REAL,"
            "  amount_p95 REAL,"
            "  sample_count INTEGER"
            ")"
        )
        rows = conn.execute(
            "SELECT vendor, subtotal FROM documents "
            "WHERE vendor IS NOT NULL AND vendor != '' "
            "AND subtotal IS NOT NULL AND subtotal > 0"
        ).fetchall()
        from collections import defaultdict
        buckets: Dict[str, List[float]] = defaultdict(list)
        for v, s in rows:
            try:
                buckets[v].append(float(s))
            except (TypeError, ValueError):
                pass
        count = 0
        conn.execute("DELETE FROM vendor_amount_history")
        for v, vs in buckets.items():
            vs.sort()
            n = len(vs)
            p50 = vs[n // 2]
            p95 = vs[min(int(n * 0.95), n - 1)]
            conn.execute(
                "INSERT INTO vendor_amount_history (vendor_name, amount_p50, amount_p95, sample_count) "
                "VALUES (?,?,?,?)",
                (v, p50, p95, n),
            )
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()
