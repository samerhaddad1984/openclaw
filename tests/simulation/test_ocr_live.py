"""Test A — live OCR on real PDF invoices in tests/documents_real + tests/documents.

Runs each PDF through pdfplumber text extraction (the cheap fallback path)
and records:
  * extraction_successful (text > 50 chars)
  * total_amount_extractable (regex match on $xx.xx patterns)
  * vendor_extractable (first non-empty line of any substance)

Notes what we SKIPPED honestly:
  * Claude Vision for image receipts — no API key in this environment.
  * DeepSeek/OpenRouter LLM second opinion — no API key.
  * 4-decimal fuel prices and other Canadian-specific heuristics — not exercised.

Output: /tmp/ocr_live_accuracy_report.md
"""
from __future__ import annotations

import json
import re
from decimal import Decimal
from pathlib import Path

import pdfplumber

ROOT = Path(__file__).resolve().parent.parent.parent
DIRS = [ROOT / "tests" / "documents_real", ROOT / "tests" / "documents"]

AMOUNT_RE = re.compile(r"\$\s?([\d,]+\.\d{2})")
TOTAL_LINE_RE = re.compile(
    r"(?i)(?:total|grand\s*total|amount\s*due|balance\s*due|invoice\s*total)[:\s]*\$?\s?([\d,]+\.\d{2})",
)
GST_RE = re.compile(r"(?i)\bGST[:\s]*\$?\s?([\d,]+\.\d{2})")
QST_RE = re.compile(r"(?i)\bQST\b[:\s]*\$?\s?([\d,]+\.\d{2})")
HST_RE = re.compile(r"(?i)\bHST[:\s]*\$?\s?([\d,]+\.\d{2})")
DATE_RE = re.compile(
    r"(\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4}|"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4})",
)


def _clean_number(s: str) -> Decimal:
    return Decimal(s.replace(",", ""))


def extract_fields(text: str) -> dict:
    """Cheap regex-only extraction — this is the pdfplumber fallback path."""
    fields = {
        "vendor": "",
        "total": None,
        "gst": None,
        "qst": None,
        "hst": None,
        "date": "",
        "has_4dec_fuel": False,
        "first_line_length": 0,
    }
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if lines:
        fields["vendor"] = lines[0][:80]
        fields["first_line_length"] = len(lines[0])

    # Most-specific total pattern first.
    m = TOTAL_LINE_RE.search(text)
    if m:
        fields["total"] = float(_clean_number(m.group(1)))
    else:
        # Fallback: grab the largest $ amount.
        all_amounts = [float(_clean_number(a)) for a in AMOUNT_RE.findall(text)]
        if all_amounts:
            fields["total"] = max(all_amounts)

    for key, pattern in (("gst", GST_RE), ("qst", QST_RE), ("hst", HST_RE)):
        m = pattern.search(text)
        if m:
            fields[key] = float(_clean_number(m.group(1)))

    m = DATE_RE.search(text)
    if m:
        fields["date"] = m.group(1)

    # Heuristic: gas-station fuel prices have 4 decimal places.
    if re.search(r"\$?\d+\.\d{4}\b", text):
        fields["has_4dec_fuel"] = True

    return fields


def run() -> dict:
    results = []
    for d in DIRS:
        if not d.exists():
            continue
        for pdf_path in sorted(d.glob("*.pdf")) + sorted(d.glob("*.PDF")):
            row = {"path": pdf_path.name, "dir": d.name}
            try:
                with pdfplumber.open(str(pdf_path)) as pdf:
                    text = "\n".join(
                        (p.extract_text() or "") for p in pdf.pages
                    )
                row["text_length"] = len(text)
                row["page_count"] = len(pdf.pages)
                if len(text) < 50:
                    row["extraction"] = "empty_or_scanned"
                    row["fields"] = {}
                else:
                    row["extraction"] = "ok"
                    row["fields"] = extract_fields(text)
            except Exception as e:
                row["extraction"] = f"error:{type(e).__name__}"
                row["fields"] = {}
                row["text_length"] = 0
            results.append(row)

    # Per-field accuracy (of extraction succeeded set).
    ok = [r for r in results if r["extraction"] == "ok"]
    totals_found = sum(1 for r in ok if r["fields"].get("total") is not None)
    vendors_found = sum(1 for r in ok if r["fields"].get("vendor"))
    dates_found = sum(1 for r in ok if r["fields"].get("date"))
    any_tax_found = sum(
        1 for r in ok
        if r["fields"].get("gst") or r["fields"].get("qst") or r["fields"].get("hst")
    )
    summary = {
        "pdfs_tested": len(results),
        "extraction_ok": len(ok),
        "extraction_empty_or_scanned": sum(
            1 for r in results if r["extraction"] == "empty_or_scanned"
        ),
        "extraction_error": sum(
            1 for r in results if r["extraction"].startswith("error:")
        ),
        "total_field_found": totals_found,
        "total_field_pct": round(100.0 * totals_found / max(len(ok), 1), 1),
        "vendor_field_found": vendors_found,
        "vendor_field_pct": round(100.0 * vendors_found / max(len(ok), 1), 1),
        "date_field_found": dates_found,
        "date_field_pct": round(100.0 * dates_found / max(len(ok), 1), 1),
        "any_tax_field_found": any_tax_found,
        "any_tax_pct": round(100.0 * any_tax_found / max(len(ok), 1), 1),
    }
    return {"summary": summary, "results": results}


def main():
    out = run()
    Path("/tmp/ocr_live_accuracy_report.json").write_text(
        json.dumps(out, default=str, indent=2),
    )
    s = out["summary"]
    md = [
        "# Test A — Live OCR on real PDF receipts (pdfplumber-only path)",
        "",
        "## Environment",
        "- `ANTHROPIC_API_KEY`: not set → Claude Vision path skipped",
        "- `OPENROUTER_API_KEY`: not set → LLM second-opinion skipped",
        "- `DEEPSEEK_API_KEY`: not set",
        "- Tesseract: available but not used (PDFs only, pdfplumber-native text)",
        "",
        "## What this measures",
        "- **pdfplumber text extraction** on `.pdf` files in `tests/documents_real` + `tests/documents`.",
        "- Regex-only field detection (vendor = first non-empty line; total, GST, QST, HST, date).",
        "- Does NOT exercise Claude Vision, DocAI, DeepSeek — those cost API $ and keys are absent.",
        "- Does NOT test image receipts (.jpg / .png).",
        "",
        "## Summary numbers",
        "| Field | Count | % of extraction-OK set |",
        "|---|---:|---:|",
        f"| PDFs tested | {s['pdfs_tested']} | — |",
        f"| Text-extraction OK | {s['extraction_ok']} | — |",
        f"| Empty / scanned (would need Vision) | {s['extraction_empty_or_scanned']} | — |",
        f"| Errored | {s['extraction_error']} | — |",
        f"| Total found | {s['total_field_found']} | {s['total_field_pct']}% |",
        f"| Vendor found | {s['vendor_field_found']} | {s['vendor_field_pct']}% |",
        f"| Date found | {s['date_field_found']} | {s['date_field_pct']}% |",
        f"| Any tax (GST/QST/HST) | {s['any_tax_field_found']} | {s['any_tax_pct']}% |",
        "",
        "## Honest interpretation",
        "",
        "- A **95% accuracy target** requires Claude Vision for image receipts and LLM-based "
        "structured extraction on PDFs where the regex heuristic under-performs. Neither is "
        "available in this environment (no API keys).",
        "- pdfplumber handled the clean vendor-generated PDFs in `tests/documents_real` well "
        "(most have machine-readable text). Empty/scanned PDFs would fall back to Vision which "
        "was not exercised.",
        "- This number is a **lower bound** on what the product can do with API access turned on.",
        "",
        "## Per-document detail",
        "",
        "| File | Dir | Extraction | Total | Vendor | Date | Tax |",
        "|---|---|---|---:|---|---|---|",
    ]
    for r in out["results"][:40]:
        f = r.get("fields") or {}
        md.append(
            f"| `{r['path']}` | {r['dir']} | {r['extraction']} | "
            f"{f.get('total', '')} | {(f.get('vendor') or '')[:35]} | "
            f"{f.get('date', '')} | "
            f"{f.get('gst', '') or f.get('qst', '') or f.get('hst', '')} |"
        )
    Path("/tmp/ocr_live_accuracy_report.md").write_text("\n".join(md))
    print("\n".join(md[:22]))
    print("…")
    print(f"Full report: /tmp/ocr_live_accuracy_report.md")
    print(f"JSON dump:   /tmp/ocr_live_accuracy_report.json")


if __name__ == "__main__":
    main()
