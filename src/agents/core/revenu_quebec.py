"""
src/agents/core/revenu_quebec.py — Revenu Québec form pre-fill (deterministic).

Computes GST/QST filing line values from approved documents using the existing
tax engine.  No AI calls anywhere in this module.

Line numbers match the Revenu Québec VD-471 / FPZ-500 return:
  101  Total sales and other revenues      (revenue-side — not tracked by this system)
  103  GST/HST collected or collectible
  106  Input Tax Credits (ITCs) claimed
  108  Net GST  (line 103 − line 106)

  205  QST collected or collectible
  207  Input Tax Refunds (ITRs) claimed
  209  Net QST  (line 205 − line 207)

Quick Method (Méthode simplifiée):
  - Retail:   remit GST at 1.8 %  of (sales + GST),  QST at 3.4 %  of (sales + QST)
  - Services: remit GST at 3.6 %  of (sales + GST),  QST at 6.6 %  of (sales + QST)
  Under Quick Method, ITC/ITR lines are $0 (with narrow capital-property exceptions
  that are out of scope for this pre-fill tool).
"""
from __future__ import annotations

import sqlite3
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

# Quick Method remittance rates (per CRA / Revenu Québec rules)
QM_RETAIL_GST   = Decimal("0.018")   # 1.8 %
QM_RETAIL_QST   = Decimal("0.034")   # 3.4 %
QM_SERVICES_GST = Decimal("0.036")   # 3.6 %
QM_SERVICES_QST = Decimal("0.066")   # 6.6 %

_ZERO = Decimal("0")
_CENT = Decimal("0.01")


def _round(value: Decimal) -> Decimal:
    return value.quantize(_CENT, rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------------------
# client_config table helpers
# ---------------------------------------------------------------------------

def ensure_client_config_table(conn: sqlite3.Connection) -> None:
    """Create the client_config table if it does not exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS client_config (
            client_code        TEXT PRIMARY KEY,
            quick_method       INTEGER NOT NULL DEFAULT 0,
            quick_method_type  TEXT    NOT NULL DEFAULT 'retail',
            updated_at         TEXT
        )
    """)
    conn.commit()


def get_client_config(conn: sqlite3.Connection, client_code: str) -> dict[str, Any]:
    """Return the client_config row as a dict; returns defaults if not found."""
    ensure_client_config_table(conn)
    row = conn.execute(
        "SELECT * FROM client_config WHERE client_code = ?",
        (str(client_code).strip(),),
    ).fetchone()
    if row:
        return dict(row)
    return {
        "client_code": client_code,
        "quick_method": 0,
        "quick_method_type": "retail",
        "updated_at": None,
    }


def set_client_config(
    conn: sqlite3.Connection,
    client_code: str,
    quick_method: bool,
    quick_method_type: str,
    updated_at: str = "",
) -> None:
    """Insert or replace a client_config row."""
    ensure_client_config_table(conn)
    conn.execute(
        """
        INSERT INTO client_config (client_code, quick_method, quick_method_type, updated_at)
             VALUES (?, ?, ?, ?)
        ON CONFLICT(client_code) DO UPDATE SET
            quick_method      = excluded.quick_method,
            quick_method_type = excluded.quick_method_type,
            updated_at        = excluded.updated_at
        """,
        (
            str(client_code).strip(),
            1 if quick_method else 0,
            str(quick_method_type or "retail").strip(),
            updated_at,
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Pre-fill computation
# ---------------------------------------------------------------------------

def compute_prefill(
    client_code: str,
    period_start: str,
    period_end: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """
    Compute the Revenu Québec form pre-fill for a client and period.

    Parameters
    ----------
    client_code  : Client identifier.
    period_start : ISO date string, inclusive (e.g. "2025-01-01").
    period_end   : ISO date string, inclusive (e.g. "2025-03-31").
    conn         : Open SQLite connection to the OtoCPA database.

    Returns
    -------
    dict with keys:
        client_code, period_start, period_end,
        line_101, line_103, line_106, line_108,
        line_205, line_207, line_209,
        quick_method (bool), quick_method_type (str | None),
        quick_gst_rate (Decimal | None), quick_qst_rate (Decimal | None),
        documents_posted, documents_pending, documents_total,
        error (str | None)
    """
    from src.engines.tax_engine import generate_filing_summary

    db_path = None
    try:
        db_path_str = conn.execute("PRAGMA database_list").fetchone()
        if db_path_str:
            from pathlib import Path
            db_path = Path(db_path_str[2]) if db_path_str[2] else None
    except Exception:
        pass

    if db_path is None:
        # For in-memory connections use the conn-based path (for testing,
        # generate_filing_summary receives the path but the test DB is temp).
        # Fall back to a dummy path that won't exist so filing summary returns error.
        from pathlib import Path
        db_path = Path(":memory:")

    summary = generate_filing_summary(client_code, period_start, period_end, db_path)
    error = summary.get("error")

    # Load client config from the same connection
    cfg = get_client_config(conn, client_code)
    quick_method = bool(cfg.get("quick_method", 0))
    qm_type = str(cfg.get("quick_method_type") or "retail").strip()

    # Determine Quick Method remittance rates
    if quick_method:
        if qm_type == "services":
            qm_gst_rate: Decimal | None = QM_SERVICES_GST
            qm_qst_rate: Decimal | None = QM_SERVICES_QST
        else:
            qm_gst_rate = QM_RETAIL_GST
            qm_qst_rate = QM_RETAIL_QST
    else:
        qm_gst_rate = None
        qm_qst_rate = None

    # Revenue-side lines — expense system does not track sales
    line_101 = _ZERO   # total sales:    must be provided by the accountant
    line_103 = _ZERO   # GST on sales:   not tracked
    line_205 = _ZERO   # QST on sales:   not tracked

    # ITC / ITR from the filing summary (normal-method values)
    raw_itc = _round(Decimal(str(summary.get("itc_available", _ZERO))))
    raw_itr = _round(Decimal(str(summary.get("itr_available", _ZERO))))

    # Quick Method trap: force ITC/ITR to $0 and flag if the engine
    # computed non-zero recoverable amounts (would be a double-claim).
    qm_itc_blocked = _ZERO
    qm_itr_blocked = _ZERO

    if quick_method:
        # Under Quick Method: no ITC/ITR; remit flat rate on gross sales.
        # Sales are unknown here, so computed amounts default to zero.
        if raw_itc > _ZERO or raw_itr > _ZERO:
            qm_itc_blocked = raw_itc
            qm_itr_blocked = raw_itr
        line_106 = _ZERO
        line_207 = _ZERO
        line_108 = _ZERO
        line_209 = _ZERO
    else:
        line_106 = raw_itc
        line_207 = raw_itr
        line_108 = _round(line_103 - line_106)   # negative = refund owed
        line_209 = _round(line_205 - line_207)   # negative = refund owed

    return {
        "client_code": client_code,
        "period_start": period_start,
        "period_end": period_end,
        "line_101": line_101,
        "line_103": line_103,
        "line_106": line_106,
        "line_108": line_108,
        "line_205": line_205,
        "line_207": line_207,
        "line_209": line_209,
        "quick_method": quick_method,
        "quick_method_type": qm_type if quick_method else None,
        "quick_gst_rate": qm_gst_rate,
        "quick_qst_rate": qm_qst_rate,
        "qm_itc_blocked": qm_itc_blocked,
        "qm_itr_blocked": qm_itr_blocked,
        "documents_posted": summary.get("documents_posted", 0),
        "documents_pending": summary.get("documents_pending", 0),
        "documents_total": summary.get("documents_total", 0),
        "error": error,
    }


# ---------------------------------------------------------------------------
# PDF generation
# ---------------------------------------------------------------------------

_WARN_FR = (
    "AVERTISSEMENT : Ce pré-remplissage est fourni à titre indicatif seulement. "
    "Le comptable est responsable de la production des déclarations. "
    "OtoCPA ne produit pas de déclarations directement auprès de Revenu Québec."
)
_WARN_EN = (
    "WARNING: This pre-fill is for reference only. "
    "The accountant is responsible for filing. "
    "OtoCPA does not file directly with Revenu Québec."
)


def _fmt(value: Any) -> str:
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def generate_revenu_quebec_pdf(
    *,
    client_code: str,
    period_start: str,
    period_end: str,
    prefill: dict[str, Any],
    generated_at: str,
) -> bytes:
    """
    Generate a bilingual (French / English) Revenu Québec pre-fill summary PDF.

    Uses PyMuPDF when available; falls back to a raw-PDF builder that requires
    no external libraries (same pattern as invoice_generator.py).
    """
    try:
        import fitz  # noqa: F401
        return _pdf_pymupdf(
            client_code=client_code,
            period_start=period_start,
            period_end=period_end,
            prefill=prefill,
            generated_at=generated_at,
        )
    except ImportError:
        return _pdf_minimal(
            client_code=client_code,
            period_start=period_start,
            period_end=period_end,
            prefill=prefill,
            generated_at=generated_at,
        )


# ---------------------------------------------------------------------------
# PyMuPDF implementation
# ---------------------------------------------------------------------------

def _pdf_pymupdf(
    *, client_code, period_start, period_end, prefill, generated_at,
) -> bytes:
    import fitz  # type: ignore[import]

    doc = fitz.open()
    page = doc.new_page(width=612, height=792)

    BLUE  = (0.08, 0.16, 0.44)
    GRAY  = (0.5,  0.5,  0.5)
    AMBER = (0.5,  0.2,  0.0)
    GREEN = (0.1,  0.4,  0.1)

    y = 54

    # ---- Title ----
    page.insert_text(
        (50, y),
        "Revenu Québec — Pré-remplissage  /  Pre-fill",
        fontsize=14, fontname="hebo", color=BLUE,
    )
    y += 20
    page.insert_text(
        (50, y),
        f"Client: {client_code}    Période / Period: {period_start}  –  {period_end}",
        fontsize=10,
    )
    y += 14
    page.insert_text(
        (50, y), f"Généré le / Generated: {generated_at}",
        fontsize=9, color=GRAY,
    )
    y += 18

    # ---- Warning banner ----
    page.draw_rect(
        fitz.Rect(46, y - 4, 566, y + 42),
        color=(0.8, 0.4, 0.0), fill=(1.0, 0.97, 0.88), width=1.2,
    )
    page.insert_text((52, y + 4),  _WARN_FR, fontsize=7.5, color=AMBER)
    page.insert_text((52, y + 18), _WARN_EN, fontsize=7.5, color=AMBER)
    y += 52

    page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
    y += 14

    # ---- Quick Method banner (if active) ----
    if prefill.get("quick_method"):
        qm_type = prefill.get("quick_method_type", "retail")
        if qm_type == "services":
            rates = "TPS 3,6 % / GST 3.6 %    TVQ 6,6 % / QST 6.6 %"
        else:
            rates = "TPS 1,8 % / GST 1.8 %    TVQ 3,4 % / QST 3.4 %"
        qm_label = (
            f"Méthode simplifiée / Quick Method  ({qm_type})   {rates}"
            "   |   CTI/ITR = $0.00"
        )
        page.insert_text((50, y), qm_label, fontsize=9, color=GREEN)
        y += 16
        page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
        y += 14

    # ---- GST / TPS section ----
    page.insert_text((50, y), "TPS / GST", fontsize=12, fontname="hebo", color=BLUE)
    y += 16

    gst_rows: list[tuple[str, str, Any, str]] = [
        ("101",
         "Ventes totales / Total sales",
         prefill["line_101"],
         "(non suivi — fournir les ventes réelles / not tracked — enter actual sales)"),
        ("103", "TPS perçue / GST collected",     prefill["line_103"], ""),
        ("106", "CTI réclamés / ITC claimed",      prefill["line_106"], ""),
        ("108", "TPS nette / Net GST  (103 − 106)", prefill["line_108"], ""),
    ]
    for num, label, value, note in gst_rows:
        page.insert_text((50,  y), f"Ligne / Line {num}", fontsize=9, fontname="hebo")
        page.insert_text((145, y), label, fontsize=9)
        page.insert_text((445, y), _fmt(value), fontsize=10, fontname="hebo")
        if note:
            page.insert_text((54, y + 11), note, fontsize=7.5, color=GRAY)
            y += 12
        y += 16

    y += 6
    page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
    y += 14

    # ---- QST / TVQ section ----
    page.insert_text((50, y), "TVQ / QST", fontsize=12, fontname="hebo", color=BLUE)
    y += 16

    qst_rows: list[tuple[str, str, Any, str]] = [
        ("205", "TVQ perçue / QST collected",       prefill["line_205"], ""),
        ("207", "RTI réclamés / ITR claimed",        prefill["line_207"], ""),
        ("209", "TVQ nette / Net QST  (205 − 207)", prefill["line_209"], ""),
    ]
    for num, label, value, note in qst_rows:
        page.insert_text((50,  y), f"Ligne / Line {num}", fontsize=9, fontname="hebo")
        page.insert_text((145, y), label, fontsize=9)
        page.insert_text((445, y), _fmt(value), fontsize=10, fontname="hebo")
        if note:
            page.insert_text((54, y + 11), note, fontsize=7.5, color=GRAY)
            y += 12
        y += 16

    y += 10
    page.draw_line((50, y), (562, y), color=(0.75, 0.75, 0.75), width=0.5)
    y += 12

    # ---- Document statistics ----
    page.insert_text(
        (50, y),
        (
            f"Documents comptabilisés / Posted: {prefill['documents_posted']}   "
            f"En attente / Pending: {prefill['documents_pending']}   "
            f"Total: {prefill['documents_total']}"
        ),
        fontsize=9, color=GRAY,
    )

    pdf_bytes: bytes = doc.tobytes()
    doc.close()
    return pdf_bytes


# ---------------------------------------------------------------------------
# Minimal raw-PDF fallback (no external library required)
# ---------------------------------------------------------------------------

def _pdf_minimal(
    *, client_code, period_start, period_end, prefill, generated_at,
) -> bytes:
    """Build a valid PDF without external libraries using Tm absolute positioning."""

    def _enc(s: str) -> bytes:
        b = str(s).encode("latin-1", errors="replace")
        return b.replace(b"\\", b"\\\\").replace(b"(", b"\\(").replace(b")", b"\\)")

    elements: list[tuple[int, int, str, int]] = []

    def add(x: int, y: int, text: str, size: int = 10) -> None:
        elements.append((x, y, text, size))

    y = 750

    # Title
    add(50, y, "Revenu Quebec -- Pre-remplissage / Pre-fill", 13)
    y -= 16
    add(50, y, f"Client: {client_code}   Period: {period_start} to {period_end}", 10)
    y -= 13
    add(50, y, f"Generated: {generated_at}", 8)
    y -= 18

    # Warning
    add(50, y, "AVERTISSEMENT / WARNING:", 9)
    y -= 12
    add(52, y, "Pre-remplissage a titre indicatif / For reference only.", 8)
    y -= 11
    add(52, y, "Comptable responsable de la production / Accountant responsible for filing.", 8)
    y -= 11
    add(52, y, "OtoCPA ne produit pas de declarations / does not file directly.", 8)
    y -= 16

    # Quick Method
    if prefill.get("quick_method"):
        qm_type = prefill.get("quick_method_type", "retail")
        rates = "1.8% GST / 3.4% QST" if qm_type != "services" else "3.6% GST / 6.6% QST"
        add(50, y, f"Methode simplifiee / Quick Method ({qm_type}): {rates}  | CTI/ITR = $0.00", 9)
        y -= 14

    y -= 4

    # GST section
    add(50, y, "--- TPS / GST ---", 11)
    y -= 14
    gst_rows = [
        ("101", "Ventes totales / Total sales",        prefill["line_101"],
         "(non suivi / not tracked)"),
        ("103", "TPS percue / GST collected",           prefill["line_103"], ""),
        ("106", "CTI / ITC claimed",                    prefill["line_106"], ""),
        ("108", "TPS nette / Net GST (103 - 106)",      prefill["line_108"], ""),
    ]
    for num, label, value, note in gst_rows:
        add(50,  y, f"Line {num}", 9)
        add(110, y, label, 9)
        add(440, y, _fmt(value), 9)
        y -= 12
        if note:
            add(60, y, note, 8)
            y -= 11

    y -= 6

    # QST section
    add(50, y, "--- TVQ / QST ---", 11)
    y -= 14
    qst_rows = [
        ("205", "TVQ percue / QST collected",           prefill["line_205"], ""),
        ("207", "RTI / ITR claimed",                    prefill["line_207"], ""),
        ("209", "TVQ nette / Net QST (205 - 207)",      prefill["line_209"], ""),
    ]
    for num, label, value, note in qst_rows:
        add(50,  y, f"Line {num}", 9)
        add(110, y, label, 9)
        add(440, y, _fmt(value), 9)
        y -= 12
        if note:
            add(60, y, note, 8)
            y -= 11

    y -= 10
    add(
        50, y,
        (
            f"Posted: {prefill['documents_posted']}   "
            f"Pending: {prefill['documents_pending']}   "
            f"Total: {prefill['documents_total']}"
        ),
        8,
    )

    # Build content stream
    cmds: list[bytes] = [b"BT"]
    for (x, y_pos, text, size) in elements:
        cmds.append(f"/F1 {size} Tf".encode())
        cmds.append(f"1 0 0 1 {x} {y_pos} Tm".encode())
        cmds.append(b"(" + _enc(text) + b") Tj")
    cmds.append(b"ET")
    content = b"\n".join(cmds)

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
