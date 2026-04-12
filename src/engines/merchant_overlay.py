"""Merchant-specific post-processing for line-item extraction.

The spatial engine and DocAI parsers produce generic candidates; certain
merchants (Costco, Walmart) use barcode-first layouts with discount
conventions that require merchant-aware rewriting to avoid mis-classifying
totals/tax lines as items.
"""
from __future__ import annotations

import re
from typing import Any

SKIP_LINE_PATTERNS = [
    r'^sous-total',
    r'^total\s+rabais',
    r'^\*+\s*total',
    r'^taxe\s',
    r'^p\.?\s*t\.?v\.?q',
    r'^f\.?\s*t\.?p\.?s',
    r'^mastercard',
    r'^visa',
    r'^monnaie',
    r'^acct:',
    r'^auth\s*#',
    r'^reference\s*#',
    r'^invoice\s+number',
    r'^purchase\s*-',
    r'^approved',
    r'^amount:',
    r'^nombre\s+d',
    r'^merci',
    r'^customer\s+copy',
    r'^important',
    r'^caissier',
    r'^\d{4}/\d{2}/\d{2}',  # dates
    r'^uo\s+membre',
    r'^membre\s+\d',
    r'^entr:',
    r'^#oper',
    r'^#tps',
    r'^#tvq',
    r'^\d{13,}$',  # barcodes alone
    r'^libre-service',
    r'^wholesale$',
    r'^costco$',
]

_SKIP_RE = [re.compile(p, re.IGNORECASE) for p in SKIP_LINE_PATTERNS]
_TPD_RE = re.compile(r'\bTPD/(\d+)\b', re.IGNORECASE)
_BARCODE_DESC_RE = re.compile(r'^(\d{5,7})\s+(.+)$')
_AMOUNT_RE = re.compile(r'(\d+[.,]\d{2})(-)?')
_COSTCO_TAX_CODES = ('FP', 'P', 'F')


def _should_skip(text: str) -> bool:
    t = text.strip()
    if not t:
        return True
    return any(p.search(t) for p in _SKIP_RE)


def _parse_amount(text: str) -> tuple[float | None, bool]:
    """Return (amount, is_discount). Trailing '-' marks a discount."""
    m = _AMOUNT_RE.search(text)
    if not m:
        return None, False
    raw = m.group(1).replace(',', '.')
    is_discount = bool(m.group(2))
    try:
        return float(raw), is_discount
    except ValueError:
        return None, False


def _detect_tax_suffix(text: str) -> str | None:
    for code in _COSTCO_TAX_CODES:
        if re.search(rf'(?<![A-Z]){code}(?![A-Z])', text):
            return code
    return None


def parse_costco_receipt(raw_text: str) -> list[dict[str, Any]]:
    """Parse a Costco receipt directly from OCR text.

    Layout:
        BARCODE DESCRIPTION
        PRICE [TAX_CODE]

    Discount lines:
        BARCODE TPD/PARENT_BARCODE
        AMOUNT-
    """
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    items: list[dict[str, Any]] = []

    i = 0
    while i < len(lines):
        line = lines[i]

        if _should_skip(line):
            i += 1
            continue

        m = _BARCODE_DESC_RE.match(line)
        if not m:
            i += 1
            continue

        barcode = m.group(1)
        desc = m.group(2).strip()

        if i + 1 >= len(lines):
            break

        price_line = lines[i + 1]
        amount, is_discount = _parse_amount(price_line)
        if amount is None:
            i += 1
            continue

        # TPD discount line — attach to parent item by barcode
        tpd = _TPD_RE.search(desc)
        if tpd:
            parent_barcode = tpd.group(1)
            for item in items:
                if item.get('_barcode') == parent_barcode:
                    prev_discount = float(item.get('discount') or 0)
                    item['discount'] = prev_discount + amount
                    item['total_price'] = float(item['unit_price']) - item['discount']
                    break
            i += 2
            continue

        tax_suffix = _detect_tax_suffix(price_line)
        tax_code = 'T' if tax_suffix else 'Z'

        items.append({
            '_barcode': barcode,
            'description': desc,
            'quantity': 1.0,
            'unit_price': amount,
            'total_price': amount,
            'tax_code': tax_code,
            'confidence': 0.95,
        })
        i += 2

    for item in items:
        item.pop('_barcode', None)

    return items


def apply_merchant_overlay(
    items: list[dict[str, Any]],
    merchant_name: str,
    raw_text: str,
) -> list[dict[str, Any]]:
    """Return merchant-aware items, falling back to the input list."""
    merchant_lower = (merchant_name or '').lower()

    if 'costco' in merchant_lower:
        costco_items = parse_costco_receipt(raw_text or '')
        if costco_items:
            return costco_items

    return items
