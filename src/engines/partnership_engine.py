"""
src/engines/partnership_engine.py — Sprint H F3.

Partnership income allocation per CRA T5013 rules.

Key concepts:
  * Partnership net income flows through to partners (no entity tax).
  * Each partner has an allocation_percentage (e.g., 60/30/10).
  * Partner changes mid-year are prorated by days active.
  * T5013 Statement of Partnership Income is generated per partner.

Tables: ``partnerships``, ``partners``.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import date as _date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

CENT = Decimal("0.01")
_ZERO = Decimal("0")


def _round(v: Decimal) -> Decimal:
    return v.quantize(CENT, rounding=ROUND_HALF_UP)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _is_leap_year(y: int) -> bool:
    return (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0)


def _year_days(year: int) -> int:
    return 366 if _is_leap_year(year) else 365


PARTNERSHIPS_DDL = """
CREATE TABLE IF NOT EXISTS partnerships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_code TEXT NOT NULL DEFAULT '',
    client_code TEXT NOT NULL,
    partnership_name TEXT NOT NULL,
    tax_year_end TEXT,
    partnership_type TEXT DEFAULT 'general',
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS partners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    partnership_id INTEGER NOT NULL,
    partner_name TEXT NOT NULL,
    partner_type TEXT DEFAULT 'individual',
    partner_sin_or_bn TEXT,
    allocation_percentage REAL NOT NULL,
    effective_date TEXT NOT NULL,
    end_date TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (partnership_id) REFERENCES partnerships(id)
);

CREATE INDEX IF NOT EXISTS idx_partners_partnership
    ON partners(partnership_id);
"""


def ensure_partnership_tables(conn: sqlite3.Connection) -> None:
    for stmt in PARTNERSHIPS_DDL.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.commit()


def create_partnership(
    conn: sqlite3.Connection,
    *,
    firm_code: str = "",
    client_code: str,
    partnership_name: str,
    tax_year_end: str = "",
    partnership_type: str = "general",
) -> int:
    ensure_partnership_tables(conn)
    cur = conn.execute(
        """INSERT INTO partnerships
           (firm_code, client_code, partnership_name, tax_year_end, partnership_type)
           VALUES (?, ?, ?, ?, ?)""",
        (firm_code, client_code, partnership_name, tax_year_end, partnership_type),
    )
    conn.commit()
    return int(cur.lastrowid)


def add_partner(
    conn: sqlite3.Connection,
    *,
    partnership_id: int,
    partner_name: str,
    allocation_percentage: float,
    effective_date: str,
    partner_type: str = "individual",
    partner_sin_or_bn: str = "",
    end_date: str = "",
) -> int:
    if not (0 <= allocation_percentage <= 100):
        raise ValueError("allocation_percentage must be in [0, 100]")
    ensure_partnership_tables(conn)
    cur = conn.execute(
        """INSERT INTO partners
           (partnership_id, partner_name, partner_type, partner_sin_or_bn,
            allocation_percentage, effective_date, end_date)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (partnership_id, partner_name, partner_type, partner_sin_or_bn,
         allocation_percentage, effective_date, end_date or None),
    )
    conn.commit()
    return int(cur.lastrowid)


def get_active_partners(
    conn: sqlite3.Connection,
    partnership_id: int,
    fiscal_year: int,
) -> list[dict[str, Any]]:
    """Return all partners with any overlap with the fiscal year."""
    ensure_partnership_tables(conn)
    fy_start = _date(fiscal_year, 1, 1).isoformat()
    fy_end = _date(fiscal_year, 12, 31).isoformat()
    rows = conn.execute(
        """SELECT * FROM partners
           WHERE partnership_id = ?
             AND effective_date <= ?
             AND (end_date IS NULL OR end_date >= ?)""",
        (partnership_id, fy_end, fy_start),
    ).fetchall()
    return [dict(r) for r in rows]


def calculate_active_days(
    partner: dict[str, Any],
    fiscal_year: int,
) -> int:
    """Days the partner was active during the fiscal year."""
    fy_start = _date(fiscal_year, 1, 1)
    fy_end = _date(fiscal_year, 12, 31)
    eff = _date.fromisoformat(str(partner["effective_date"])[:10])
    end_str = partner.get("end_date")
    end = _date.fromisoformat(str(end_str)[:10]) if end_str else fy_end
    actual_start = max(eff, fy_start)
    actual_end = min(end, fy_end)
    if actual_end < actual_start:
        return 0
    return (actual_end - actual_start).days + 1


def compute_partnership_allocation(
    conn: sqlite3.Connection,
    partnership_id: int,
    fiscal_year: int,
    partnership_income: float | Decimal | str,
) -> dict[str, Any]:
    """Allocate partnership_income to partners with proration.

    Validates that the sum of (allocation% × proration) is reasonable; if
    partner allocations don't sum to 100 % during the year, the result
    includes a ``warning`` field rather than silently losing income.
    """
    ensure_partnership_tables(conn)
    income_d = Decimal(str(partnership_income))
    partners = get_active_partners(conn, partnership_id, fiscal_year)
    if not partners:
        return {
            "partnership_id": partnership_id,
            "fiscal_year": fiscal_year,
            "total_partnership_income": float(income_d),
            "allocations": [],
            "warning": "no active partners during fiscal year",
        }

    year_days = _year_days(fiscal_year)
    allocations: list[dict[str, Any]] = []
    total_allocated = _ZERO
    for p in partners:
        days_active = calculate_active_days(p, fiscal_year)
        proration = Decimal(days_active) / Decimal(year_days)
        pct = Decimal(str(p["allocation_percentage"])) / Decimal("100")
        allocated = _round(income_d * pct * proration)
        total_allocated += allocated
        allocations.append({
            "partner_id": p["id"],
            "partner_name": p["partner_name"],
            "partner_type": p["partner_type"],
            "partner_sin_or_bn": p.get("partner_sin_or_bn"),
            "allocation_percentage": float(p["allocation_percentage"]),
            "days_active": days_active,
            "proration": float(round(proration, 4)),
            "allocated_income": float(allocated),
            "taxable_portion": float(allocated),
            "t5013_slip": _t5013_slip(partnership_id, p, allocated, fiscal_year),
        })

    result: dict[str, Any] = {
        "partnership_id": partnership_id,
        "fiscal_year": fiscal_year,
        "total_partnership_income": float(income_d),
        "total_allocated": float(_round(total_allocated)),
        "allocations": allocations,
    }
    delta = abs(total_allocated - income_d)
    if delta > Decimal("0.05"):
        result["warning"] = (
            f"Allocation mismatch: allocated {total_allocated} "
            f"vs income {income_d} (delta {delta})"
        )
    return result


def _t5013_slip(
    partnership_id: int,
    partner: dict[str, Any],
    allocated: Decimal,
    fiscal_year: int,
) -> dict[str, Any]:
    """Minimum CRA T5013 slip data structure."""
    return {
        "form": "T5013",
        "partnership_id": partnership_id,
        "tax_year": fiscal_year,
        "partner_name": partner["partner_name"],
        "partner_type": partner["partner_type"],
        "partner_sin_or_bn": partner.get("partner_sin_or_bn"),
        "box_001_country": "CA",
        "box_002_partner_code": (
            "0" if partner.get("partner_type") == "individual" else "5"
        ),
        "box_104_share_of_net_income": float(allocated),
        "box_113_partnership_total_income": None,
        "issued_at": _utc_now(),
    }


def list_partnerships(
    conn: sqlite3.Connection,
    *,
    firm_code: str = "",
    client_code: str = "",
) -> list[dict[str, Any]]:
    ensure_partnership_tables(conn)
    where = []
    params: list[Any] = []
    if firm_code:
        where.append("LOWER(firm_code)=LOWER(?)")
        params.append(firm_code)
    if client_code:
        where.append("LOWER(client_code)=LOWER(?)")
        params.append(client_code)
    sql = "SELECT * FROM partnerships"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC"
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_partnership(conn: sqlite3.Connection, partnership_id: int) -> dict[str, Any] | None:
    ensure_partnership_tables(conn)
    row = conn.execute(
        "SELECT * FROM partnerships WHERE id=?", (partnership_id,),
    ).fetchone()
    return dict(row) if row else None


def list_partners(conn: sqlite3.Connection, partnership_id: int) -> list[dict[str, Any]]:
    ensure_partnership_tables(conn)
    rows = conn.execute(
        "SELECT * FROM partners WHERE partnership_id=? ORDER BY id",
        (partnership_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def remove_partner(conn: sqlite3.Connection, partner_id: int) -> bool:
    ensure_partnership_tables(conn)
    cur = conn.execute("DELETE FROM partners WHERE id=?", (partner_id,))
    conn.commit()
    return cur.rowcount > 0


# ---------------------------------------------------------------------------
# T5013 PDF rendering
# ---------------------------------------------------------------------------

def generate_t5013_pdf(
    conn: sqlite3.Connection,
    partnership_id: int,
    fiscal_year: int,
    partnership_income: float | Decimal | str,
) -> bytes:
    """Render one T5013 statement covering all partners in the partnership."""
    allocation = compute_partnership_allocation(
        conn, partnership_id, fiscal_year, partnership_income,
    )
    p = get_partnership(conn, partnership_id) or {}

    try:
        return _render_t5013_reportlab(p, allocation)
    except ImportError:
        return _render_t5013_minimal(p, allocation)


def _render_t5013_reportlab(partnership: dict, allocation: dict) -> bytes:
    from io import BytesIO
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
    )
    from reportlab.lib import colors

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=letter,
        leftMargin=0.6 * inch, rightMargin=0.6 * inch,
        topMargin=0.6 * inch, bottomMargin=0.6 * inch,
    )
    styles = getSampleStyleSheet()
    h_style = ParagraphStyle("h", parent=styles["Heading1"], fontSize=14,
                              spaceAfter=10)
    sub = ParagraphStyle("sub", parent=styles["Heading2"], fontSize=11,
                         spaceAfter=6)
    story = []
    story.append(Paragraph("T5013 — Statement of Partnership Income", h_style))
    story.append(Paragraph(
        f"Partnership: <b>{partnership.get('partnership_name', '')}</b> "
        f"({partnership.get('partnership_type', 'general')})",
        styles["BodyText"],
    ))
    story.append(Paragraph(
        f"Tax year: <b>{allocation['fiscal_year']}</b> &mdash; "
        f"Total partnership income: <b>${allocation['total_partnership_income']:,.2f}</b>",
        styles["BodyText"],
    ))
    story.append(Spacer(1, 0.2 * inch))

    for alloc in allocation.get("allocations", []):
        story.append(Paragraph(
            f"Partner: {alloc['partner_name']} ({alloc['partner_type']})",
            sub,
        ))
        rows = [
            ["Box", "Description", "Value"],
            ["001", "Country of residence", "CA"],
            ["002", "Partner code",
             alloc["t5013_slip"]["box_002_partner_code"]],
            ["003", "Partner SIN / BN",
             str(alloc.get("partner_sin_or_bn") or "")],
            ["104", "Share of net income", f"${alloc['allocated_income']:,.2f}"],
            ["105", "Allocation %", f"{alloc['allocation_percentage']:.2f}%"],
            ["106", "Days active", str(alloc["days_active"])],
        ]
        tbl = Table(rows, colWidths=[0.6 * inch, 3.8 * inch, 2.4 * inch])
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a2d5c")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 0.25 * inch))

    if allocation.get("warning"):
        story.append(Paragraph(
            f"<b>Warning:</b> {allocation['warning']}", styles["BodyText"],
        ))

    doc.build(story)
    return buf.getvalue()


def _render_t5013_minimal(partnership: dict, allocation: dict) -> bytes:
    """Minimal PDF fallback when reportlab is unavailable."""
    lines = [
        "T5013 Statement of Partnership Income",
        f"Partnership: {partnership.get('partnership_name', '')}",
        f"Tax year: {allocation.get('fiscal_year', '')}",
        f"Total income: ${allocation.get('total_partnership_income', 0):,.2f}",
        "",
    ]
    for a in allocation.get("allocations", []):
        lines.append(f"{a['partner_name']}: ${a['allocated_income']:,.2f}")
    body = "\n".join(lines).encode("latin-1", errors="replace")
    return b"%PDF-1.4\n" + body + b"\n%%EOF\n"
