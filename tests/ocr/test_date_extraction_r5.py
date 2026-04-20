"""R5 SROIE investigation found three date-extraction gaps:

1. DocAI's ``receipt_date`` field (expense processor) was silently
   dropped because only ``invoice_date`` was mapped to our
   ``document_date``.
2. ``_fix_quebec_date`` accepted clearly-invalid dates — feeding it
   a SKU code like ``23-33-53`` produced ``2053-33-23`` instead of
   ``None``.
3. ``parse_invoice_fields`` scanned lines in order and took the
   first regex hit. A SKU line (``KE23-33-53``) would win over the
   real date in the receipt footer.
4. DocAI dates like ``05-JAN-2017`` / ``22 MAR 2018`` / ``24-03-18``
   weren't normalised before downstream comparison.

These tests pin each gap.
"""
from __future__ import annotations

import pytest
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.ocr_engine import _fix_quebec_date, parse_invoice_fields
from src.engines.google_docai import process_with_docai  # noqa: F401 (import smoke)


# --- _fix_quebec_date: validation ---

@pytest.mark.parametrize("raw, expected", [
    # Canonical formats
    ("2018-03-24", "2018-03-24"),
    ("24-03-18",   "2018-03-24"),
    ("24/03/2018", "2018-03-24"),
    # Two-digit years
    ("16-03-18",   "2018-03-16"),
    ("24-01-16",   "2016-01-24"),
    # US-format when day > 12
    ("12/13/2016", "2016-12-13"),
    ("12/28/2017", "2017-12-28"),
    # Named-month formats
    ("05-JAN-2017", "2017-01-05"),
    ("22 MAR 2018", "2018-03-22"),
    ("14/JUN/2017", "2017-06-14"),
    ("05 MAY 18",   "2018-05-05"),
])
def test_fix_quebec_date_accepts(raw: str, expected: str) -> None:
    assert _fix_quebec_date(raw) == expected


@pytest.mark.parametrize("raw", [
    "23-33-53",       # SKU code shaped like DD-MM-YY but month/day > 12
    "45-88-99",       # all components out of range
    "00-00-00",       # zeroes (year 2000 is valid but day 0 is not)
    "32/13/2018",     # day > 31 and month > 12
    "2019-02-31",     # invalid calendar day (Feb 31)
    "2018-13-15",     # invalid ISO month 13 (no swap possible — day > 12)
])
def test_fix_quebec_date_rejects_invalid(raw: str) -> None:
    assert _fix_quebec_date(raw) is None


# --- parse_invoice_fields: prefer labeled date ---

def test_parse_invoice_fields_prefers_labeled_date_over_sku():
    text = (
        "MR. D.I.Y.\n"
        "KE23-33-53 - 12/120\n"   # SKU that looks like DD-MM-YY
        "9074333\n"
        "TOTAL 12.30\n"
        "24-03-18 18:10 SH01 ZJ86\n"  # unlabeled footer date
    )
    result = parse_invoice_fields(text)
    assert result["document_date"] == "2018-03-24"


def test_parse_invoice_fields_prefers_date_label():
    text = (
        "ORD #18 -REG #19- 21/03/2018\n"
        "DATE: 25/03/2018\n"
        "TOTAL 10.00\n"
    )
    # Even though 21/03/2018 appears first, DATE: wins.
    result = parse_invoice_fields(text)
    assert result["document_date"] == "2018-03-25"


def test_parse_invoice_fields_rejects_sku_only():
    text = (
        "PRODUCT KE99-88-77\n"   # all components > 12
        "TOTAL 5.00\n"
    )
    result = parse_invoice_fields(text)
    # No valid date — should stay None, not invent 2077-88-99.
    assert result["document_date"] is None


def test_parse_invoice_fields_handles_text_month_with_invalid_day():
    # A "Feb 31, 2019" garbage should NOT become 2019-02-31.
    text = "Feb 31, 2019\nTOTAL 5.00\n"
    result = parse_invoice_fields(text)
    assert result["document_date"] is None


# --- DocAI receipt_date mapping ---

def test_docai_receipt_date_mapped_to_document_date(monkeypatch):
    """DocAI's expense processor returns receipt_date; the OCR pipeline
    depends on document_date. Before the R5 fix, receipt_date was
    silently dropped and the noisy regex fallback ran."""
    from src.engines import google_docai

    class FakeEntity:
        def __init__(self, t, text, conf=0.9):
            self.type_ = t
            self.mention_text = text
            self.confidence = conf

    class FakeDoc:
        def __init__(self):
            self.entities = [
                FakeEntity('supplier_name', 'TEST VENDOR'),
                FakeEntity('receipt_date', '24-03-18'),  # DD-MM-YY
                FakeEntity('total_amount', '12.30'),
            ]
            self.text = 'raw text'

    class FakeResult:
        document = FakeDoc()

    class FakeClient:
        def process_document(self, request):
            return FakeResult()

    monkeypatch.setattr(google_docai, 'get_docai_client', lambda: FakeClient())
    monkeypatch.setattr(google_docai, 'PROJECT_ID', 'proj', raising=False)
    monkeypatch.setattr(google_docai, 'LOCATION', 'loc', raising=False)
    monkeypatch.setattr(google_docai, 'INVOICE_PROCESSOR_ID', 'inv', raising=False)

    import tempfile
    with tempfile.NamedTemporaryFile(suffix='.jpg') as f:
        f.write(b'\xff\xd8\xff')
        f.flush()
        result = google_docai.process_with_docai(Path(f.name), doc_type='receipt')

    assert result.get('document_date') == '24-03-18', \
        f"receipt_date should map to document_date; got {result!r}"
