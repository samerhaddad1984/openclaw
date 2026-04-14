"""Validation and correction layer for AI-extracted document/line data.

Catches math errors, invalid GL/tax codes, suspicious amounts, and
reconciliation gaps before they reach the database. Auto-corrects
known-bad codes to safe defaults.
"""
import re
import logging
from typing import List, Dict, Optional, Tuple

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
