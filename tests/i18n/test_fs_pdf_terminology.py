"""Regression guards for Québec-French terminology in financial
statement PDFs.

The FS PDF renderer plumbs ``lang`` through to every label via the
JSON i18n dict. These tests render a real PyMuPDF PDF and assert the
expected vocabulary appears in the text layer — both the French
Ordre des CPA du Québec conventions (Bilan, État des résultats,
Capitaux propres) and the English equivalents.
"""
from __future__ import annotations

import pytest

fitz = pytest.importorskip("fitz")

from src.engines.audit_engine import _fs_pdf_pymupdf
from src.i18n import t


@pytest.fixture
def sample_stmts() -> dict:
    """A minimal but complete statements dict the renderer accepts."""
    return {
        "client_code": "TREMB-LEV",
        "period": "2025-12-31",
        "balance_sheet": {
            "assets": {
                "current": [{"account_code": "1000", "account_name": "Caisse",
                              "amount": 50000.00}],
                "non_current": [{"account_code": "1500",
                                  "account_name": "Immobilisations",
                                  "amount": 120000.00}],
                "total": 170000.00,
            },
            "liabilities": {
                "current": [{"account_code": "2000",
                              "account_name": "Comptes fournisseurs",
                              "amount": 30000.00}],
                "long_term": [{"account_code": "2500",
                                "account_name": "Emprunt hypothécaire",
                                "amount": 80000.00}],
                "total": 110000.00,
            },
            "equity": {"items": [{"account_code": "3000",
                                    "account_name": "Capital actions",
                                    "amount": 60000.00}]},
            "equity_detail": {"total": 60000.00},
        },
        "income_statement": {
            "revenue": {"total": 200000.00},
            "expenses": {"total": 150000.00},
            "revenue_detail": [{"account_code": "4000",
                                 "account_name": "Ventes",
                                 "amount": 200000.00}],
            "expenses_detail": [{"account_code": "5000",
                                  "account_name": "Coût des marchandises",
                                  "amount": 150000.00}],
            "total_revenue": 200000.00,
            "total_expenses": 150000.00,
            "net_income": 50000.00,
        },
    }


def _render(stmts: dict, lang: str) -> str:
    pdf = _fs_pdf_pymupdf(stmts, "Lévesque & Associés CPA", lang, t)
    doc = fitz.open(stream=pdf, filetype="pdf")
    text = doc[0].get_text()
    doc.close()
    return text


@pytest.mark.parametrize("term", [
    "Bilan",
    "État des résultats",
    "Actif à court terme",
    "Actif à long terme",
    "Passif à court terme",
    "Passif à long terme",
    "Capitaux propres",
    "Total de l'actif",
    "Total du passif",
    "Total des charges",
    "Total des produits",
    "Résultat net",
    "Charges",
    "Produits",
])
def test_fs_pdf_fr_contains_quebec_cpa_term(sample_stmts, term: str) -> None:
    text = _render(sample_stmts, "fr")
    assert term in text, (
        f"expected Québec-French term {term!r} in FS PDF, "
        f"got (first 400 chars): {text[:400]!r}"
    )


@pytest.mark.parametrize("english_leak", [
    "Balance Sheet",
    "Income Statement",
    "Current Assets",
    "Long-term Liabilities",
    "Net Income",
    "Total Assets",
])
def test_fs_pdf_fr_has_no_english_leak(sample_stmts, english_leak: str) -> None:
    """The FR PDF must not contain the English label strings."""
    text = _render(sample_stmts, "fr")
    assert english_leak not in text, (
        f"English label {english_leak!r} leaked into the FR FS PDF: "
        f"{text[:400]!r}"
    )


@pytest.mark.parametrize("term", [
    "Balance Sheet",
    "Income Statement",
    "Total Assets",
    "Total Expenses",
    "Net Income",
])
def test_fs_pdf_en_contains_expected_label(sample_stmts, term: str) -> None:
    text = _render(sample_stmts, "en")
    assert term in text


def test_fs_pdf_fr_currency_is_locale_correct(sample_stmts) -> None:
    text = _render(sample_stmts, "fr")
    # 50000.00 → 50 000,00 $ (space thousands, comma decimal, trailing $)
    assert "50 000,00 $" in text
    # English form must not appear
    assert "$50,000.00" not in text


def test_fs_pdf_en_currency_is_locale_correct(sample_stmts) -> None:
    text = _render(sample_stmts, "en")
    assert "$50,000.00" in text
    # French form must not appear
    assert "50 000,00 $" not in text
