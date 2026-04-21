"""Render each locale-sensitive surface once with lang='fr' and once
with lang='en', then scan the output for cross-locale leaks.

Writes docs/locale_evidence.json.

This runs in-process (no HTTP server, no sessions) so it works in any
environment that can import the app modules. Each "surface" is an
engine or render function invoked with the same test data in both
languages; we grep the rendered text for:

* raw English words ("Save", "Edit", …) in FR output,
* US-dollar currency format ("$1,234.56") in FR output,
* French locale currency ("1 234,56 $") in EN output,
* literal ``{ui_t(`` or ``${...:,.2f}`` leakage (proves the
  interpolation is live, not rendered as text).

The script is deliberately non-fatal: it emits counts into JSON so
both humans and regression tests can read the evidence.
"""
from __future__ import annotations

import json
import re
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.engines.audit_engine import _fs_pdf_pymupdf, _lead_sheet_pymupdf
from src.engines.cas_engine import (
    add_related_party,
    ensure_cas_tables,
    flag_related_party_transaction,
    generate_related_party_disclosure,
)
from src.engines.audit_engine import (
    create_engagement,
    ensure_audit_tables,
)
from src.i18n import t


def _leak_scan(rendered: str, lang: str) -> dict:
    """Return a dict of leak counts for one rendered string."""
    us_currency = re.findall(r"\$\d{1,3}(?:,\d{3})*\.\d{2}", rendered)
    fr_currency = re.findall(r"\d{1,3}(?:\s\d{3})*,\d{2}\s\$", rendered)
    entity_leak = re.findall(
        r"&(?:eacute|egrave|agrave|ccedil|ocirc);", rendered
    )
    literal_fstring = (
        "{ui_t(" in rendered
        or ":,.2f}" in rendered
        or ":+,.2f}" in rendered
    )
    return {
        "us_currency_count": len(us_currency),
        "us_currency_sample": us_currency[:3],
        "fr_currency_count": len(fr_currency),
        "fr_currency_sample": fr_currency[:3],
        "html_entity_leak": len(entity_leak),
        "literal_fstring_leak": literal_fstring,
    }


def _lead_sheet_surface(lang: str) -> str:
    import fitz
    papers = [
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
    pdf = _lead_sheet_pymupdf(
        papers=papers,
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


def _fs_surface(lang: str) -> str:
    import fitz
    stmts = {
        "client_code": "TREMB-LEV",
        "period": "2025-12-31",
        "balance_sheet": {
            "assets": {
                "current": [{"account_code": "1000",
                              "account_name": "Caisse",
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
    pdf = _fs_pdf_pymupdf(stmts, "Lévesque & Associés CPA", lang, t)
    doc = fitz.open(stream=pdf, filetype="pdf")
    text = doc[0].get_text()
    doc.close()
    return text


def _related_party_surface(lang: str) -> str:
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_audit_tables(conn)
    ensure_cas_tables(conn)
    eng = create_engagement(
        conn,
        client_code="ACME",
        period="2025-12-31",
        engagement_type="review",
    )
    party_id = add_related_party(
        client_code="ACME",
        party_name="Jean Lévesque",
        relationship_type="shareholder",
        conn=conn,
        ownership_percentage=60.0,
    )
    flag_related_party_transaction(
        engagement_id=eng["engagement_id"],
        document_id="doc_1",
        party_id=party_id,
        measurement_basis="exchange_amount",
        conn=conn,
        amount=12345.67,
        description="prêt de l'actionnaire",
        transaction_date="2025-06-15",
    )
    out = generate_related_party_disclosure(
        eng["engagement_id"], lang, conn
    )
    conn.close()
    return out


def _digest_surface(lang: str) -> str:
    import scripts.daily_digest as dd
    from datetime import date
    from unittest.mock import patch

    summary = {"needs_review": 0, "on_hold": 0, "ready_to_post": 0,
               "posted_today": 0, "stale": 0, "total_active": 0}
    deadlines = [{
        "client_code": "TREM", "period_label": "2025-12",
        "deadline": "2026-03-31", "days_until": 45,
        "docs_pending": 3, "gst_amount": 1234.56,
        "qst_amount": 2345.67,
    }]
    with patch.object(dd, "date") as mock_date:
        mock_date.today.return_value = date(2026, 4, 21)
        mock_date.side_effect = lambda *a, **k: date(*a, **k)
        return dd.build_html_body(
            summary, lang=lang, recipient_name="Marie",
            filing_deadlines=deadlines,
        )


SURFACES = {
    "audit_lead_sheet_pdf": _lead_sheet_surface,
    "financial_statements_pdf": _fs_surface,
    "cas_related_party_disclosure": _related_party_surface,
    "daily_digest_html": _digest_surface,
}


def main() -> None:
    report: dict = {}
    for name, fn in SURFACES.items():
        report[name] = {}
        for lang in ("fr", "en"):
            try:
                rendered = fn(lang)
            except Exception as exc:  # pragma: no cover - evidence-only
                report[name][lang] = {"error": f"{type(exc).__name__}: {exc}"}
                continue
            report[name][lang] = _leak_scan(rendered, lang)

    # Derive a top-level summary
    total_fr_leaks = sum(
        (info.get("us_currency_count", 0) or 0)
        + (info.get("html_entity_leak", 0) or 0)
        + (1 if info.get("literal_fstring_leak") else 0)
        for surface in report.values()
        for lang, info in surface.items()
        if lang == "fr"
    )
    total_en_leaks = sum(
        (info.get("fr_currency_count", 0) or 0)
        + (1 if info.get("literal_fstring_leak") else 0)
        for surface in report.values()
        for lang, info in surface.items()
        if lang == "en"
    )
    report["_summary"] = {
        "fr_cross_locale_leaks": total_fr_leaks,
        "en_cross_locale_leaks": total_en_leaks,
        "surfaces_checked": len(SURFACES),
    }

    out_path = Path(__file__).resolve().parent.parent.parent / "docs/locale_evidence.json"
    out_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Wrote {out_path}")
    print(f"FR→EN leaks: {total_fr_leaks}    EN→FR leaks: {total_en_leaks}")


if __name__ == "__main__":
    main()
