"""
src/agents/core/invoice_generator.py — GST/QST-compliant invoice PDF generation.

Produces a Revenu Québec-compliant tax invoice for professional services.

Required elements per Revenu Québec:
  - Supplier name, GST registration number, QST registration number
  - Client name, unique invoice number, invoice date
  - Description of services rendered
  - Pre-tax subtotal
  - GST amount (5%) with registration number
  - QST amount (9.975%) with registration number
  - Total amount payable

Uses tax_engine.calculate_gst_qst() for all tax calculations.
All monetary arithmetic uses Decimal to avoid floating-point errors.
"""
from __future__ import annotations

import secrets
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from src.engines.tax_engine import calculate_gst_qst
from src.formatting import format_number, money


def generate_invoice_number() -> str:
    """Return a unique invoice number: INV-YYYYMMDD-XXXXXX."""
    date_part = datetime.now(timezone.utc).strftime("%Y%m%d")
    rand_part = secrets.token_hex(3).upper()
    return f"INV-{date_part}-{rand_part}"


def generate_invoice_pdf(
    *,
    invoice_number: str,
    invoice_date: str,
    firm_name: str,
    gst_number: str,
    qst_number: str,
    client_name: str,
    client_code: str,
    period_start: str,
    period_end: str,
    hourly_rate: Decimal,
    billable_hours: Decimal,
    entries: list[dict[str, Any]],
    lang: str = "fr",
) -> bytes:
    """Generate and return a GST/QST-compliant invoice PDF as bytes."""
    from src.i18n import t

    subtotal = (hourly_rate * billable_hours).quantize(Decimal("0.01"))
    tax = calculate_gst_qst(subtotal)
    gst_amount = tax["gst"]
    qst_amount = tax["qst"]
    total = tax["total_with_tax"]

    try:
        import fitz  # noqa: F401
        return _pdf_pymupdf(
            invoice_number=invoice_number, invoice_date=invoice_date,
            firm_name=firm_name, gst_number=gst_number, qst_number=qst_number,
            client_name=client_name, client_code=client_code,
            period_start=period_start, period_end=period_end,
            hourly_rate=hourly_rate, billable_hours=billable_hours,
            entries=entries, subtotal=subtotal, gst_amount=gst_amount,
            qst_amount=qst_amount, total=total, lang=lang, t=t,
        )
    except ImportError:
        return _pdf_minimal(
            invoice_number=invoice_number, invoice_date=invoice_date,
            firm_name=firm_name, gst_number=gst_number, qst_number=qst_number,
            client_name=client_name, client_code=client_code,
            period_start=period_start, period_end=period_end,
            hourly_rate=hourly_rate, billable_hours=billable_hours,
            entries=entries, subtotal=subtotal, gst_amount=gst_amount,
            qst_amount=qst_amount, total=total, lang=lang, t=t,
        )


# ---------------------------------------------------------------------------
# PyMuPDF implementation
# ---------------------------------------------------------------------------

def _pdf_pymupdf(
    *, invoice_number, invoice_date, firm_name, gst_number, qst_number,
    client_name, client_code, period_start, period_end, hourly_rate,
    billable_hours, entries, subtotal, gst_amount, qst_amount, total,
    lang, t,
) -> bytes:
    import fitz  # type: ignore[import]

    doc = fitz.open()
    page = doc.new_page(width=612, height=792)  # US Letter

    y = 60

    # Firm name (top-left)
    page.insert_text((50, y), firm_name, fontsize=16, fontname="hebo",
                     color=(0.08, 0.16, 0.44))
    # Invoice # and date (top-right)
    page.insert_text((400, y), f"{t('inv_invoice_number', lang)}: {invoice_number}",
                     fontsize=9)
    y += 16
    page.insert_text((400, y), f"{t('inv_invoice_date', lang)}: {invoice_date}",
                     fontsize=9)
    y += 14

    # GST/QST registration numbers
    page.insert_text((50, y), f"{t('inv_reg_gst', lang)}: {gst_number}",
                     fontsize=9, color=(0.4, 0.4, 0.4))
    y += 12
    page.insert_text((50, y), f"{t('inv_reg_qst', lang)}: {qst_number}",
                     fontsize=9, color=(0.4, 0.4, 0.4))
    y += 20

    # Separator
    page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
    y += 14

    # Bill to
    page.insert_text((50, y), t("inv_bill_to", lang).upper(),
                     fontsize=9, color=(0.5, 0.5, 0.5))
    y += 14
    page.insert_text((50, y), client_name, fontsize=11, fontname="hebo")
    y += 14
    page.insert_text((50, y), f"{t('doc_field_client', lang)}: {client_code}",
                     fontsize=9, color=(0.4, 0.4, 0.4))
    y += 22

    # Period
    page.insert_text((50, y),
                     f"{t('inv_period', lang)}: {period_start}  \u2013  {period_end}",
                     fontsize=10)
    y += 22

    # Separator
    page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
    y += 14

    # Services table header
    page.insert_text((50, y), t("inv_description", lang), fontsize=9, fontname="hebo")
    page.insert_text((380, y), t("inv_hours", lang), fontsize=9, fontname="hebo")
    page.insert_text((440, y), t("inv_rate", lang), fontsize=9, fontname="hebo")
    page.insert_text((510, y), t("inv_amount", lang), fontsize=9, fontname="hebo")
    y += 10
    page.draw_line((50, y), (562, y), color=(0.85, 0.85, 0.85), width=0.3)
    y += 14

    # Aggregate line item
    svc_label = f"{t('inv_services', lang)} \u2014 {period_start} / {period_end}"
    page.insert_text((50, y), svc_label, fontsize=9)
    page.insert_text((380, y), format_number(float(billable_hours), lang, decimals=2), fontsize=9)
    page.insert_text((440, y), money(float(hourly_rate), lang), fontsize=9)
    page.insert_text((510, y), money(float(subtotal), lang), fontsize=9)
    y += 14

    # Individual entry details
    for e in entries[:30]:
        if y > 640:
            break
        mins = e.get("duration_minutes") or 0.0
        e_hours = mins / 60
        e_user = e.get("username", "")
        e_desc = (e.get("description") or "")[:50]
        line = f"  {e_user}" + (f": {e_desc}" if e_desc else "")
        page.insert_text((50, y), line, fontsize=8, color=(0.5, 0.5, 0.5))
        page.insert_text((380, y), format_number(e_hours, lang, decimals=2), fontsize=8, color=(0.5, 0.5, 0.5))
        y += 12

    y += 6
    page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
    y += 14

    # Totals (right-aligned)
    lx, rx = 380, 510
    page.insert_text((lx, y), t("inv_subtotal", lang), fontsize=10)
    page.insert_text((rx, y), money(float(subtotal), lang), fontsize=10)
    y += 16

    gst_label = f"{t('inv_gst', lang)}  [{t('inv_reg_gst', lang)}: {gst_number}]"
    page.insert_text((lx, y), gst_label, fontsize=9)
    page.insert_text((rx, y), money(float(gst_amount), lang), fontsize=9)
    y += 14

    qst_label = f"{t('inv_qst', lang)}  [{t('inv_reg_qst', lang)}: {qst_number}]"
    page.insert_text((lx, y), qst_label, fontsize=9)
    page.insert_text((rx, y), money(float(qst_amount), lang), fontsize=9)
    y += 6

    page.draw_line((50, y), (562, y), color=(0.08, 0.16, 0.44), width=1.0)
    y += 16

    page.insert_text((lx, y), t("inv_total", lang), fontsize=12, fontname="hebo",
                     color=(0.08, 0.16, 0.44))
    page.insert_text((rx, y), money(float(total), lang), fontsize=12, fontname="hebo",
                     color=(0.08, 0.16, 0.44))

    pdf_bytes: bytes = doc.tobytes()
    doc.close()
    return pdf_bytes


# ---------------------------------------------------------------------------
# Minimal raw-PDF fallback (no external library required)
# ---------------------------------------------------------------------------

def _pdf_minimal(
    *, invoice_number, invoice_date, firm_name, gst_number, qst_number,
    client_name, client_code, period_start, period_end, hourly_rate,
    billable_hours, entries, subtotal, gst_amount, qst_amount, total,
    lang, t,
) -> bytes:
    """Build a valid PDF invoice without external libraries using Tm positioning."""

    def _enc(s: str) -> bytes:
        b = str(s).encode("latin-1", errors="replace")
        return b.replace(b"\\", b"\\\\").replace(b"(", b"\\(").replace(b")", b"\\)")

    # List of (x, y, text, fontsize) — y measured from bottom of page
    elements: list[tuple[int, int, str, int]] = []

    def add(x: int, y: int, text: str, size: int = 10) -> None:
        elements.append((x, y, text, size))

    y = 740

    # Header
    add(50, y, firm_name, 14)
    add(400, y, f"{t('inv_invoice_number', lang)}: {invoice_number}", 9)
    y -= 16
    add(50, y, f"{t('inv_reg_gst', lang)}: {gst_number}", 9)
    add(400, y, f"{t('inv_invoice_date', lang)}: {invoice_date}", 9)
    y -= 13
    add(50, y, f"{t('inv_reg_qst', lang)}: {qst_number}", 9)
    y -= 20

    # Bill to
    add(50, y, f"{t('inv_bill_to', lang).upper()}:", 9)
    y -= 14
    add(50, y, client_name, 11)
    y -= 14
    add(50, y, f"{t('doc_field_client', lang)}: {client_code}", 9)
    y -= 16

    # Period
    add(50, y, f"{t('inv_period', lang)}: {period_start}  to  {period_end}", 10)
    y -= 20

    # Services table header
    add(50, y, t("inv_description", lang), 9)
    add(370, y, t("inv_hours", lang), 9)
    add(430, y, t("inv_rate", lang), 9)
    add(495, y, t("inv_amount", lang), 9)
    y -= 14

    # Aggregate service line
    svc_label = t("inv_services", lang)
    add(50, y, svc_label, 9)
    add(370, y, format_number(float(billable_hours), lang, decimals=2), 9)
    add(430, y, f"{money(float(hourly_rate), lang)}/h", 9)
    add(495, y, money(float(subtotal), lang), 9)
    y -= 12

    # Individual entries
    for e in entries[:20]:
        if y < 220:
            break
        mins = e.get("duration_minutes") or 0.0
        e_hours = mins / 60
        e_user = e.get("username", "")
        e_desc = (e.get("description") or "")[:35]
        line = f"  {e_user}" + (f": {e_desc}" if e_desc else "")
        add(50, y, line, 8)
        add(370, y, format_number(e_hours, lang, decimals=2), 8)
        y -= 11

    y -= 10

    # Totals
    add(370, y, f"{t('inv_subtotal', lang)}:", 10)
    add(495, y, money(float(subtotal), lang), 10)
    y -= 14
    add(370, y, f"{t('inv_gst', lang)}:", 9)
    add(495, y, money(float(gst_amount), lang), 9)
    y -= 13
    add(370, y, f"  {t('inv_reg_gst', lang)}: {gst_number}", 8)
    y -= 12
    add(370, y, f"{t('inv_qst', lang)}:", 9)
    add(495, y, money(float(qst_amount), lang), 9)
    y -= 13
    add(370, y, f"  {t('inv_reg_qst', lang)}: {qst_number}", 8)
    y -= 14
    add(370, y, f"{t('inv_total', lang)}:", 12)
    add(495, y, money(float(total), lang), 12)

    # Registration footer
    y -= 28
    add(50, y, f"TPS/GST # {gst_number}    TVQ/QST # {qst_number}", 8)

    # Build PDF content stream — use Tm for absolute positioning
    cmds: list[bytes] = [b"BT"]
    for (x, y_pos, text, size) in elements:
        cmds.append(f"/F1 {size} Tf".encode())
        cmds.append(f"1 0 0 1 {x} {y_pos} Tm".encode())
        cmds.append(b"(" + _enc(text) + b") Tj")
    cmds.append(b"ET")
    content = b"\n".join(cmds)

    # PDF structure
    catalog = b"1 0 obj\n<</Type /Catalog /Pages 2 0 R>>\nendobj\n"
    pages   = b"2 0 obj\n<</Type /Pages /Kids [3 0 R] /Count 1>>\nendobj\n"
    page_   = (
        b"3 0 obj\n<</Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]"
        b" /Contents 5 0 R /Resources <</Font <</F1 4 0 R>>>>>>\nendobj\n"
    )
    font    = b"4 0 obj\n<</Type /Font /Subtype /Type1 /BaseFont /Helvetica>>\nendobj\n"
    stream  = (
        b"5 0 obj\n<</Length " + str(len(content)).encode() + b">>\nstream\n"
        + content + b"\nendstream\nendobj\n"
    )

    header = b"%PDF-1.4\n"
    objs   = [catalog, pages, page_, font, stream]
    body   = b"".join(objs)

    offsets: list[int] = []
    pos = len(header)
    for obj in objs:
        offsets.append(pos)
        pos += len(obj)

    xref_pos = len(header) + len(body)
    xref = f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \r\n"
    for off in offsets:
        xref += f"{off:010d} 00000 n \r\n"
    trailer = (
        f"trailer\n<</Size {len(objs) + 1} /Root 1 0 R>>\n"
        f"startxref\n{xref_pos}\n%%EOF\n"
    )

    return header + body + xref.encode() + trailer.encode()
