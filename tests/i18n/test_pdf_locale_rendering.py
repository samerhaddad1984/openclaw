"""End-to-end PDF rendering guards for locale correctness.

Generates real PyMuPDF PDFs via the engines, extracts the text layer,
and asserts French accented characters + locale-correct currency
appear in both languages. Skipped automatically if ``fitz`` (PyMuPDF)
is not installed.
"""
from __future__ import annotations

import re
from decimal import Decimal

import pytest

fitz = pytest.importorskip("fitz")

from src.engines.audit_engine import _lead_sheet_pymupdf
from src.i18n import t


_PAPERS = [
    {
        "account_code": "1000",
        "account_name": "Caisse",
        "balance_per_books": Decimal("12345.67"),
        "balance_confirmed": Decimal("12345.67"),
        "difference": Decimal("0"),
        "status": "complete",
        "sign_off_at": "2026-04-21",
    },
    {
        "account_code": "1200",
        "account_name": "Comptes clients",
        "balance_per_books": Decimal("98765.43"),
        "balance_confirmed": Decimal("98000.00"),
        "difference": Decimal("-765.43"),
        "status": "exception",
        "sign_off_at": None,
    },
]


def _render_lead_sheet(lang: str) -> str:
    pdf = _lead_sheet_pymupdf(
        papers=_PAPERS,
        client_code="TREMB-LEV",
        period="2025-12-31",
        engagement_type="audit",
        prepared_by="Marie Tremblay",
        reviewed_by_firm="Sam CPA",
        firm_name="Lévesque & Associés CPA",
        lang=lang,
        t=t,
    )
    doc = fitz.open(stream=pdf, filetype="pdf")
    text = doc[0].get_text()
    doc.close()
    return text


def test_lead_sheet_fr_renders_french_accents() -> None:
    text = _render_lead_sheet("fr")
    assert "Lévesque & Associés" in text
    # Legacy French vendor name with accents on a real row
    assert "Comptes clients" in text


def test_lead_sheet_fr_uses_locale_currency_and_not_en_form() -> None:
    text = _render_lead_sheet("fr")
    # FR canonical form: "12 345,67 $" with space-thousands, comma decimal
    assert re.search(r"12\s345,67\s*\$", text), (
        f"expected '12 345,67 $' in FR lead sheet, got: {text[:300]!r}"
    )
    # Must NOT leak English form
    assert "$12,345.67" not in text


def test_lead_sheet_en_keeps_anglo_currency() -> None:
    text = _render_lead_sheet("en")
    assert "$12,345.67" in text
    # Must NOT leak the French form
    assert "12 345,67 $" not in text


def test_lead_sheet_pdf_has_no_html_entity_leak() -> None:
    """Regression from the prior entity-cleanup sprint: no ``&eacute;``
    style entities should appear in the extracted PDF text — they
    would indicate a literal entity sat in a Python string literal."""
    text_fr = _render_lead_sheet("fr")
    text_en = _render_lead_sheet("en")
    for t_out in (text_fr, text_en):
        assert "&eacute;" not in t_out
        assert "&amp;" not in t_out
        assert "&ccedil;" not in t_out


def test_lead_sheet_fr_variance_column_has_minus_sign() -> None:
    """The exception row has a -765.43 difference; FR form must render
    as '-765,43 $' (minus before the number, not before the $)."""
    text = _render_lead_sheet("fr")
    assert re.search(r"-765,43\s*\$", text), (
        f"expected FR negative currency, got: {text[:400]!r}"
    )
