"""
tests/test_line_item_detection.py
=================================
Regression tests for the line-item detection pipeline (Stage 5 fixes).

Covers the spatial engine on synthetic OCR-word input so the tests stay
fast and hermetic — no DocAI calls, no image rendering. Each test
constructs a list of ``OCRWord`` segments that mirror what DocAI returns
for a real receipt and asserts the count / classification that the
``ReceiptSpatialEngine`` produces.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.receipt_spatial_engine import (
    BoundingBox,
    OCRWord,
    ReceiptSpatialEngine,
)


def _word(text: str, x0: float, y: float, x1: float | None = None,
          h: float = 0.02) -> OCRWord:
    """Build an OCRWord at a given y-row. x1 defaults to x0 + 0.1."""
    return OCRWord(
        text=text,
        bbox=BoundingBox(
            x0=x0, y0=y - h / 2,
            x1=(x0 + 0.1) if x1 is None else x1,
            y1=y + h / 2,
        ),
        confidence=0.99,
    )


def _count_items(words: list[OCRWord]) -> int:
    engine = ReceiptSpatialEngine()
    parsed = engine.parse(words)
    return sum(1 for s in parsed.structures if s.structure_type == 'ITEM_CANDIDATE')


def _item_texts(words: list[OCRWord]) -> list[str]:
    engine = ReceiptSpatialEngine()
    parsed = engine.parse(words)
    return [s.parent_line.raw_text for s in parsed.structures
            if s.structure_type == 'ITEM_CANDIDATE']


# ---------------------------------------------------------------------------
# OVER_COUNT protection — keep non-item rows out of the item list
# ---------------------------------------------------------------------------

def test_subtotal_line_not_counted_as_item():
    # "SUBTOTAL" label on the left, "60.31" on the right — DocAI splits
    # these into two segments but they belong to the same visual row.
    words = [
        _word("APPLE", 0.1, 0.3), _word("2.49", 0.8, 0.3),
        _word("BREAD", 0.1, 0.4), _word("3.19", 0.8, 0.4),
        _word("SUBTOTAL", 0.1, 0.6), _word("5.68", 0.8, 0.6),
    ]
    assert _count_items(words) == 2


def test_tax_line_not_counted_as_item():
    words = [
        _word("APPLE", 0.1, 0.3), _word("2.49", 0.8, 0.3),
        _word("GST", 0.1, 0.7),   _word("0.12", 0.8, 0.7),
        _word("QST", 0.1, 0.75),  _word("0.24", 0.8, 0.75),
    ]
    assert _count_items(words) == 1


def test_total_line_not_counted_as_item():
    # Merged total row — "TOTAL 60.30".
    words = [
        _word("MILK", 0.1, 0.3), _word("4.99", 0.8, 0.3),
        _word("TOTAL", 0.1, 0.8), _word("4.99", 0.8, 0.8),
    ]
    assert _count_items(words) == 1


def test_cash_change_lines_not_counted_as_items():
    # DocAI commonly splits the label ("CASH"/"CHANGE") onto the same row
    # as the amount. The spatial grouping has to keep them together so the
    # SKIP keyword matches.
    words = [
        _word("ITEM1", 0.1, 0.3), _word("10.00", 0.8, 0.3),
        _word("CASH", 0.1, 0.8),   _word("20.00", 0.8, 0.8),
        _word("CHANGE", 0.1, 0.85), _word("10.00", 0.8, 0.85),
    ]
    assert _count_items(words) == 1


def test_rounding_adjustment_line_not_counted_as_item():
    words = [
        _word("SHAMPOO", 0.1, 0.3), _word("12.50", 0.8, 0.3),
        _word("ROUNDING", 0.1, 0.7), _word("ADJUSTMENT", 0.25, 0.7),
        _word("-0.05", 0.8, 0.7),
    ]
    assert _count_items(words) == 1


def test_rounding_with_ocr_split_still_filtered():
    # OCR sometimes fractures "Rounding" as "Rour" "ding"; the SKIP regex
    # must tolerate the gap.
    words = [
        _word("SHAMPOO", 0.1, 0.3), _word("12.50", 0.8, 0.3),
        _word("Rour", 0.1, 0.7), _word("ding", 0.2, 0.7),
        _word("Adjustment:", 0.35, 0.7), _word("0.00", 0.8, 0.7),
    ]
    assert _count_items(words) == 1


def test_malaysian_address_header_not_counted_as_item():
    # "NO.53, JALAN SAGU 18" on SROIE receipts contains "55,57" by
    # coincidence — that used to trip the ITEM regex. HEADER filter must
    # catch Malaysian street markers.
    words = [
        _word("789417-W", 0.1, 0.15),
        _word("NO.53,", 0.25, 0.15),
        _word("JALAN", 0.4, 0.15), _word("SAGU", 0.55, 0.15),
        _word("55,57", 0.7, 0.15),
        _word("APPLE", 0.1, 0.4), _word("9.00", 0.8, 0.4),
    ]
    items = _item_texts(words)
    assert len(items) == 1
    assert "JALAN" not in items[0]
    assert "APPLE" in items[0]


def test_discount_line_merged_as_child_not_separate_item():
    # "@DISC 10% -5.59" is a discount on the prior item, not its own item.
    words = [
        _word("LAMP", 0.1, 0.3), _word("55.90", 0.8, 0.3),
        _word("@DISC", 0.1, 0.35), _word("10%", 0.25, 0.35),
        _word("-5.59", 0.8, 0.35),
    ]
    assert _count_items(words) == 1


def test_non_dollar_currency_bare_price_not_promoted_to_item():
    # On SROIE / CORD receipts the price column has no `$`. Bare numbers
    # that stand alone on their visual row should be PRICE_ONLY, not
    # ITEM. An orphan price (no paired desc) is discarded.
    words = [
        _word("WIDGET", 0.1, 0.3), _word("3.50", 0.8, 0.3),
        _word("60.30", 0.8, 0.7),  # orphan price, no label
    ]
    assert _count_items(words) == 1


def test_malaysian_tax_code_summary_rows_not_counted_as_items():
    # SROIE receipts frequently end with "SR 6% 15.00 0.90",
    # "ZR/OS/EZ 0.00", "TAX AMT (S) 6% RM 13.30" — tax-code summary rows
    # that the earlier version treated as four more ITEMs.
    words = [
        _word("WIDGET", 0.1, 0.3), _word("5.20", 0.8, 0.3),
        _word("GADGET", 0.1, 0.4), _word("8.90", 0.8, 0.4),
        _word("TAX", 0.1, 0.7), _word("AMT", 0.18, 0.7),
        _word("(S)", 0.28, 0.7), _word("6%", 0.4, 0.7),
        _word("RM", 0.55, 0.7), _word("13.30", 0.7, 0.7),
        _word("ZR/OS/EZ", 0.1, 0.75), _word("0.00", 0.8, 0.75),
        _word("SR", 0.1, 0.8), _word("6%", 0.22, 0.8),
        _word("60.50", 0.55, 0.8), _word("3.63", 0.8, 0.8),
    ]
    assert _count_items(words) == 2


def test_promotional_header_lines_excluded():
    # Table headers like "Description Qty Price Amount" and the trailing
    # "No. Items: 2" summary row are not items.
    words = [
        _word("Description", 0.1, 0.2), _word("Qty", 0.4, 0.2),
        _word("Price", 0.55, 0.2), _word("Amount", 0.75, 0.2),
        _word("WIDGET", 0.1, 0.35), _word("2", 0.4, 0.35),
        _word("5.20", 0.55, 0.35), _word("10.40", 0.8, 0.35),
        _word("GADGET", 0.1, 0.45), _word("1", 0.4, 0.45),
        _word("8.90", 0.55, 0.45), _word("8.90", 0.8, 0.45),
        _word("No.", 0.1, 0.8), _word("Items:", 0.22, 0.8),
        _word("2", 0.35, 0.8), _word("19.30", 0.8, 0.8),
    ]
    assert _count_items(words) == 2


def test_bilingual_french_receipt_item_detection():
    # French Quebec-style: "Sous-total" should behave like "SUBTOTAL".
    words = [
        _word("PAIN", 0.1, 0.3), _word("3,99", 0.8, 0.3),
        _word("LAIT", 0.1, 0.4), _word("4,49", 0.8, 0.4),
        _word("Sous-total", 0.1, 0.65), _word("8,48", 0.8, 0.65),
        _word("TPS", 0.1, 0.7), _word("0,42", 0.8, 0.7),
        _word("TVQ", 0.1, 0.75), _word("0,85", 0.8, 0.75),
    ]
    assert _count_items(words) == 2


def test_very_long_receipt_50_plus_items():
    # Synthetic 60-row receipt; ensure the engine scales and doesn't
    # double-count or drop entries. Step > word height so rows don't
    # overlap into each other.
    words: list[OCRWord] = []
    for i in range(60):
        y = 0.2 + i * 0.008
        words.append(_word(f"ITEM{i:02d}", 0.1, y, h=0.004))
        words.append(_word(f"{(i + 1) * 1.5:.2f}", 0.8, y, h=0.004))
    words.append(_word("TOTAL", 0.1, 0.95, h=0.004))
    words.append(_word("2745.00", 0.8, 0.95, h=0.004))
    assert _count_items(words) == 60


def test_single_item_receipt_not_confused():
    # Degenerate receipt — header + 1 item + total.
    words = [
        _word("MY", 0.3, 0.1), _word("STORE", 0.45, 0.1),
        _word("APPLE", 0.1, 0.4), _word("1.99", 0.8, 0.4),
        _word("TOTAL", 0.1, 0.8), _word("1.99", 0.8, 0.8),
    ]
    assert _count_items(words) == 1


# ---------------------------------------------------------------------------
# ZERO_LINES protection — the Stage 5 root-cause regression
# ---------------------------------------------------------------------------

def test_invoice_lines_table_auto_created_by_docai_path(tmp_path):
    """Regression for the Stage 4.5 → 5 root cause: `_process_docai_line_items`
    used to DELETE from `invoice_lines` before `_ensure_invoice_lines_table`
    ran, so on a fresh DB the whole call failed silently and DocAI-derived
    lines were lost. This test constructs a fresh DB with just the
    `documents` table and verifies the DocAI path still lands items.
    """
    import sqlite3
    from src.engines.line_item_engine import _process_docai_line_items

    db = tmp_path / "fresh.db"
    with sqlite3.connect(str(db)) as conn:
        # Just the minimum documents schema the UPDATE at the end of the
        # DocAI path touches.
        conn.execute("""
            CREATE TABLE documents (
                document_id TEXT PRIMARY KEY,
                has_line_items INTEGER DEFAULT 0,
                lines_reconciled INTEGER,
                line_total_sum REAL,
                invoice_total_gap REAL,
                deposit_allocated INTEGER DEFAULT 0
            )
        """)
        conn.execute("INSERT INTO documents (document_id) VALUES (?)", ("doc_test",))
        conn.commit()

    items = [
        {"description": "APPLE", "quantity": 1.0, "unit_price": 1.99, "total_price": 1.99},
        {"description": "BREAD", "quantity": 1.0, "unit_price": 3.19, "total_price": 3.19},
    ]
    # The function may try to call DeepSeek for classification — a network
    # failure there is swallowed and shouldn't block insertion.
    _process_docai_line_items(
        "doc_test", items,
        vendor_name="TEST SHOP",
        raw_ocr_text="APPLE 1.99\nBREAD 3.19",
        db_path=db,
    )
    with sqlite3.connect(str(db)) as conn:
        cur = conn.execute(
            "SELECT COUNT(*) FROM invoice_lines WHERE document_id = 'doc_test'"
        )
        assert cur.fetchone()[0] == 2
