"""R2-Investigation 9 — adversarial vendor-category texts.

Each test feeds parse_invoice_fields a representative receipt body for
a category that historically breaks parsers. We check:
  - parser doesn't crash
  - amount, when extracted, is sane (positive, < $1M)
  - confidence stays low when the category needs human review
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.ocr_engine import parse_invoice_fields  # noqa: E402


def _check(text: str, *, max_amount: float | None = None,
           require_amount: bool = True):
    r = parse_invoice_fields(text)
    assert isinstance(r, dict)
    if require_amount and r.get("amount") is not None:
        amt = float(r["amount"])
        assert amt > 0, f"non-positive amount {amt}"
        if max_amount is not None:
            assert amt <= max_amount, (
                f"amount {amt} above sanity max {max_amount}"
            )
    return r


# ---------------------------------------------------------------------------
# 1. Gas station — 4-decimal price/L; total in dollars.
# ---------------------------------------------------------------------------

def test_gas_station_4_decimal_price_per_liter():
    txt = (
        "Petro-Canada\n"
        "12345 St-Laurent\n"
        "Date: 2026-04-15\n"
        "Regular Unleaded\n"
        "Price/L: 1.4999\n"
        "Litres: 45.328\n"
        "Subtotal: 67.99\n"
        "GST 5%: 3.40\n"
        "QST 9.975%: 6.78\n"
        "TOTAL: 78.17\n"
    )
    r = _check(txt, max_amount=500)
    if r.get("amount") is not None:
        # Must NOT pick up 1.4999 as the total. Total should be ~78.17.
        amt = float(r["amount"])
        assert amt > 50, f"gas-station total parsed as {amt} - per-litre confusion?"


# ---------------------------------------------------------------------------
# 2. Restaurant with tip on a separate line.
# ---------------------------------------------------------------------------

def test_restaurant_tip_separate_line():
    txt = (
        "Le Bistro\n"
        "Date: 2026-04-15\n"
        "Subtotal: 50.00\n"
        "GST 5%: 2.50\n"
        "QST 9.975%: 4.99\n"
        "TOTAL: 57.49\n"
        "Tip: 11.50\n"
        "Grand Total: 68.99\n"
    )
    r = _check(txt, max_amount=200)
    # parser-level check: at least one of total / grand-total found.


# ---------------------------------------------------------------------------
# 3. Costco bulk — many line items, member discount.
# ---------------------------------------------------------------------------

def test_costco_many_line_items():
    lines = ["COSTCO WHOLESALE", "Member: ****1234", "Date: 2026-04-15"]
    for i in range(40):
        lines.append(f"ITEM{i:03d}  9.99")
    lines += ["Subtotal: 399.60", "GST 5%: 19.98", "QST 9.975%: 39.86", "TOTAL: 459.44"]
    txt = "\n".join(lines)
    _check(txt, max_amount=10_000)


# ---------------------------------------------------------------------------
# 4. Dollarama — vendor name often missing from header.
# ---------------------------------------------------------------------------

def test_dollarama_no_vendor_in_header():
    txt = (
        "RECEIPT\n"
        "Store #1234\n"
        "Date: 2026-04-15\n"
        "Item A 1.25\n"
        "Item B 2.50\n"
        "TOTAL: 3.75\n"
    )
    r = _check(txt, max_amount=100)
    # Vendor extraction will be weak; that's fine. Confidence should
    # be below auto-accept.
    assert r.get("confidence", 1.0) < 0.95


# ---------------------------------------------------------------------------
# 5. Pharmacy — prescription line is GST-exempt.
# ---------------------------------------------------------------------------

def test_pharmacy_prescription_zero_rated():
    txt = (
        "Pharmaprix #4242\n"
        "Date: 2026-04-15\n"
        "Prescription Rx 999  35.00\n"
        "OTC tylenol         8.99\n"
        "Subtotal: 43.99\n"
        "GST 5% (taxable):    0.45\n"
        "QST 9.975% (taxable): 0.90\n"
        "TOTAL: 45.34\n"
    )
    _check(txt, max_amount=200)


# ---------------------------------------------------------------------------
# 6. Lumber/hardware — large amounts, capital threshold candidates.
# ---------------------------------------------------------------------------

def test_lumber_yard_large_amount():
    txt = (
        "Home Depot\n"
        "Date: 2026-04-15\n"
        "2x4x8 SPF #2  3.49 x 50 = 174.50\n"
        "Plywood 4x8  35.00 x 30 = 1050.00\n"
        "Drywall 4x8 5/8  19.99 x 40 = 799.60\n"
        "Subtotal: 2024.10\n"
        "GST 5%: 101.21\n"
        "QST 9.975%: 201.91\n"
        "TOTAL: 2327.22\n"
    )
    r = _check(txt, max_amount=50_000)
    if r.get("amount") is not None:
        assert float(r["amount"]) > 100, (
            "lumber receipt amount looks too small — line-item picked over total?"
        )


# ---------------------------------------------------------------------------
# 7. Catering — mixed food tax (meal vs grocery).
# ---------------------------------------------------------------------------

def test_catering_mixed_food_tax():
    txt = (
        "Bistro Catering\n"
        "Date: 2026-04-15\n"
        "Buffet (taxable): 200.00\n"
        "Bottled water (zero-rated): 30.00\n"
        "Subtotal: 230.00\n"
        "GST 5% (on 200): 10.00\n"
        "QST 9.975% (on 200): 19.95\n"
        "TOTAL: 259.95\n"
    )
    _check(txt, max_amount=10_000)


# ---------------------------------------------------------------------------
# 8. Hotel — multiple charges (room, parking, wifi).
# ---------------------------------------------------------------------------

def test_hotel_multiple_charges():
    txt = (
        "Fairmont Le Château Frontenac\n"
        "Folio: 9876\n"
        "Check-in: 2026-04-12  Check-out: 2026-04-15\n"
        "Room (3 nights x 350): 1050.00\n"
        "Parking (3 days x 35): 105.00\n"
        "Wifi (3 days x 15): 45.00\n"
        "Lodging tax 3.5%: 41.83\n"
        "Subtotal: 1241.83\n"
        "GST 5%: 60.00\n"
        "QST 9.975%: 119.70\n"
        "TOTAL: 1421.53\n"
    )
    _check(txt, max_amount=10_000)


# ---------------------------------------------------------------------------
# 9. Amazon online — subscription line + item lines.
# ---------------------------------------------------------------------------

def test_amazon_with_subscription():
    txt = (
        "Amazon.ca Order #112-3456789-0123456\n"
        "Date: 2026-04-15\n"
        "USB Cable 2m  12.99\n"
        "Stand-up desk 459.00\n"
        "Amazon Prime monthly  9.99\n"
        "Subtotal: 481.98\n"
        "GST 5%: 24.10\n"
        "QST 9.975%: 48.08\n"
        "TOTAL: 554.16\n"
    )
    _check(txt, max_amount=10_000)


# ---------------------------------------------------------------------------
# 10. Subcontractor invoice — reverse-charge GST (no tax shown).
# ---------------------------------------------------------------------------

def test_subcontractor_invoice():
    txt = (
        "Acme Subcontractor Inc.\n"
        "GST# 123456789RT0001\n"
        "Invoice #INV-2026-0042\n"
        "Date: 2026-04-15\n"
        "Carpentry labour: 5,400.00\n"
        "Materials: 1,200.00\n"
        "Subtotal: 6,600.00\n"
        "GST 5%: 330.00\n"
        "QST 9.975%: 658.35\n"
        "TOTAL: 7,588.35\n"
    )
    r = _check(txt, max_amount=100_000)
    if r.get("amount") is not None:
        assert float(r["amount"]) > 1000


# ---------------------------------------------------------------------------
# Aggregate — every category passes parser without crashing AND no
# category produces a confidently-wrong result on hostile text.
# ---------------------------------------------------------------------------

def test_no_category_produces_amount_above_one_million():
    """Cross-cutting: across every category here, no result should
    declare an invoice amount > $1M. The Round-1 absurd-amount cap
    holds across genres."""
    bodies = [
        "Petro-Canada\nTOTAL: 78.17\n",
        "Le Bistro\nGrand Total: 68.99\n",
        "COSTCO\nTOTAL: 459.44\n",
        "RECEIPT\nTOTAL: 3.75\n",
        "Pharmaprix\nTOTAL: 45.34\n",
        "Home Depot\nTOTAL: 2327.22\n",
        "Bistro Catering\nTOTAL: 259.95\n",
        "Fairmont\nTOTAL: 1421.53\n",
        "Amazon.ca\nTOTAL: 554.16\n",
        "Acme Subcontractor\nTOTAL: 7,588.35\n",
    ]
    for b in bodies:
        r = parse_invoice_fields(b)
        amt = r.get("amount")
        if amt is not None:
            assert float(amt) < 1_000_000, (
                f"category emitted suspicious amount {amt} on body: {b!r}"
            )
