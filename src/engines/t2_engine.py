"""
src/engines/t2_engine.py — T2 Corporate Tax Pre-fill Engine.

Pre-fills T2 schedules (1, 8, 50, 100, 125) and CO-17 Quebec mappings
from bookkeeping data.  All monetary arithmetic uses Python Decimal.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

CENT = Decimal("0.01")
_ZERO = Decimal("0")


def _round(v: Decimal) -> Decimal:
    return v.quantize(CENT, rounding=ROUND_HALF_UP)


def _to_decimal(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if v is None or str(v).strip() == "":
        return _ZERO
    try:
        return Decimal(str(v))
    except Exception:
        return _ZERO


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _sum_gl_range(client_code: str, gl_start: int, gl_end: int,
                  period_start: str, period_end: str, conn: sqlite3.Connection) -> Decimal:
    """Sum amounts for GL accounts in a numeric range within a period."""
    row = conn.execute(
        """SELECT COALESCE(SUM(amount), 0) AS total
           FROM documents
           WHERE LOWER(COALESCE(client_code, '')) = LOWER(?)
             AND COALESCE(document_date, '') >= ?
             AND COALESCE(document_date, '') <= ?
             AND CAST(SUBSTR(COALESCE(gl_account, '0'), 1, 4) AS INTEGER) BETWEEN ? AND ?""",
        (client_code, period_start, period_end, gl_start, gl_end),
    ).fetchone()
    return _to_decimal(row["total"] if row else 0)


def _sum_gl_range_balance(client_code: str, gl_start: int, gl_end: int,
                          as_of: str, conn: sqlite3.Connection) -> Decimal:
    """Sum amounts for GL accounts up to a given date (balance sheet)."""
    row = conn.execute(
        """SELECT COALESCE(SUM(amount), 0) AS total
           FROM documents
           WHERE LOWER(COALESCE(client_code, '')) = LOWER(?)
             AND COALESCE(document_date, '') <= ?
             AND CAST(SUBSTR(COALESCE(gl_account, '0'), 1, 4) AS INTEGER) BETWEEN ? AND ?""",
        (client_code, as_of, gl_start, gl_end),
    ).fetchone()
    return _to_decimal(row["total"] if row else 0)


def _sum_tax_code_expenses(client_code: str, tax_code: str,
                           period_start: str, period_end: str,
                           conn: sqlite3.Connection) -> Decimal:
    """Sum expense amounts with a specific tax code in the period."""
    row = conn.execute(
        """SELECT COALESCE(SUM(amount), 0) AS total
           FROM documents
           WHERE LOWER(COALESCE(client_code, '')) = LOWER(?)
             AND COALESCE(document_date, '') >= ?
             AND COALESCE(document_date, '') <= ?
             AND UPPER(COALESCE(tax_code, '')) = ?""",
        (client_code, period_start, period_end, tax_code.upper()),
    ).fetchone()
    return _to_decimal(row["total"] if row else 0)


def _gl_accounts_for_range(client_code: str, gl_start: int, gl_end: int,
                           period_start: str, period_end: str,
                           conn: sqlite3.Connection) -> list[str]:
    """Get distinct GL accounts contributing to a range."""
    rows = conn.execute(
        """SELECT DISTINCT gl_account
           FROM documents
           WHERE LOWER(COALESCE(client_code, '')) = LOWER(?)
             AND COALESCE(document_date, '') >= ?
             AND COALESCE(document_date, '') <= ?
             AND CAST(SUBSTR(COALESCE(gl_account, '0'), 1, 4) AS INTEGER) BETWEEN ? AND ?
           ORDER BY gl_account""",
        (client_code, period_start, period_end, gl_start, gl_end),
    ).fetchall()
    return [r["gl_account"] for r in rows if r["gl_account"]]


def _make_line(line_number: int | str, description: str, amount: Decimal,
               gl_accounts: list[str] | None = None,
               confidence: str = "high") -> dict[str, Any]:
    """Build a T2 line item dict."""
    return {
        "line": str(line_number),
        "description": description,
        "amount": float(_round(amount)),
        "gl_accounts": gl_accounts or [],
        "confidence": confidence,
    }


# ---------------------------------------------------------------------------
# Schedule 1 — Net Income for Tax Purposes
# ---------------------------------------------------------------------------

def generate_schedule_1(client_code: str, fiscal_year_end: str,
                        conn: sqlite3.Connection) -> dict[str, Any]:
    """Generate T2 Schedule 1: Net Income for Tax Purposes."""
    fy = fiscal_year_end[:4]
    period_start = f"{fy}-01-01"
    period_end = fiscal_year_end[:10]

    # Line 001: Net income per financial statements
    revenue = _sum_gl_range(client_code, 4000, 4999, period_start, period_end, conn)
    expenses = _sum_gl_range(client_code, 5000, 9999, period_start, period_end, conn)
    net_income = _round(revenue - expenses)

    # Line 101: Meals & entertainment add-back (50%)
    meals_total = _sum_tax_code_expenses(client_code, "M", period_start, period_end, conn)
    meals_addback = _round(meals_total * Decimal("0.5"))

    # Line 104: Amortization per books
    depreciation = _ZERO
    try:
        row = conn.execute(
            """SELECT COALESCE(SUM(accumulated_cca), 0) AS total
               FROM fixed_assets WHERE client_code = ?""",
            (client_code,),
        ).fetchone()
        depreciation = _to_decimal(row["total"] if row else 0)
    except Exception:
        pass

    # Line 107: Charitable donations
    donations = _sum_gl_range(client_code, 5800, 5899, period_start, period_end, conn)

    # Line 200: CCA per tax (from Schedule 8)
    cca_per_tax = _ZERO
    try:
        from src.engines.fixed_assets_engine import generate_schedule_8
        sched8 = generate_schedule_8(client_code, fy, conn)
        cca_per_tax = _to_decimal(sched8["totals"]["cca_claimed"])
    except Exception:
        pass

    # Line 205: Terminal losses
    terminal_losses = _ZERO
    try:
        rows = conn.execute(
            """SELECT cost, disposal_proceeds, current_ucc
               FROM fixed_assets
               WHERE client_code = ? AND status = 'disposed'
                 AND disposal_date >= ? AND disposal_date <= ?""",
            (client_code, period_start, period_end),
        ).fetchall()
        for r in rows:
            proceeds = _to_decimal(r.get("disposal_proceeds", 0) if isinstance(r, dict) else 0)
            ucc = _to_decimal(r.get("current_ucc", 0) if isinstance(r, dict) else 0)
            if proceeds < ucc:
                terminal_losses += _round(ucc - proceeds)
    except Exception:
        pass

    # Line 300: Net income for tax purposes
    taxable_income = _round(
        net_income + meals_addback + depreciation + donations - cca_per_tax + terminal_losses
    )

    lines = [
        _make_line("001", "Net income per financial statements", net_income,
                   _gl_accounts_for_range(client_code, 4000, 9999, period_start, period_end, conn)),
        _make_line("101", "Meals and entertainment (50% add-back)", meals_addback,
                   confidence="high" if meals_total > _ZERO else "estimated"),
        _make_line("104", "Amortization per books (add-back)", depreciation),
        _make_line("107", "Charitable donations (add-back)", donations,
                   _gl_accounts_for_range(client_code, 5800, 5899, period_start, period_end, conn)),
        _make_line("200", "CCA per tax (deduction)", cca_per_tax),
        _make_line("205", "Terminal losses", terminal_losses),
        _make_line("300", "Net income for tax purposes", taxable_income),
    ]

    return {
        "schedule": "1",
        "title": "Net Income for Tax Purposes",
        "title_fr": "Revenu net aux fins de l'impôt",
        "lines": lines,
    }


# ---------------------------------------------------------------------------
# Schedule 8 — CCA (delegate to fixed_assets_engine)
# ---------------------------------------------------------------------------

def generate_schedule_8(client_code: str, fiscal_year: str,
                        conn: sqlite3.Connection) -> dict[str, Any]:
    """Generate T2 Schedule 8 from fixed_assets_engine."""
    try:
        from src.engines.fixed_assets_engine import generate_schedule_8 as _fa_sched8
        return _fa_sched8(client_code, fiscal_year, conn)
    except Exception:
        return {
            "schedule": "8",
            "title": "Capital Cost Allowance",
            "classes": [],
            "totals": {"opening_ucc": 0, "cca_claimed": 0, "closing_ucc": 0},
        }


# ---------------------------------------------------------------------------
# Schedule 50 — Shareholder Information
# ---------------------------------------------------------------------------

def generate_schedule_50(client_code: str, fiscal_year_end: str,
                         conn: sqlite3.Connection) -> dict[str, Any]:
    """Generate T2 Schedule 50: Shareholder Information."""
    shareholders: list[dict[str, Any]] = []

    try:
        rows = conn.execute(
            """SELECT * FROM related_parties
               WHERE client_code = ? AND relationship_type = 'shareholder'""",
            (client_code,),
        ).fetchall()
        for r in rows:
            rd = r if isinstance(r, dict) else {}
            shareholders.append({
                "name": rd.get("party_name", "Unknown"),
                "ownership_pct": float(_to_decimal(rd.get("ownership_pct", 0))),
                "dividends_paid": float(_to_decimal(rd.get("dividends_paid", 0))),
                "salary_paid": float(_to_decimal(rd.get("salary_paid", 0))),
                "loans_to_shareholder": float(_to_decimal(rd.get("loans_amount", 0))),
            })
    except Exception:
        pass

    return {
        "schedule": "50",
        "title": "Shareholder Information",
        "title_fr": "Renseignements sur les actionnaires",
        "shareholders": shareholders,
    }


# ---------------------------------------------------------------------------
# Schedule 100 — Balance Sheet
# ---------------------------------------------------------------------------

def generate_schedule_100(client_code: str, fiscal_year_end: str,
                          conn: sqlite3.Connection) -> dict[str, Any]:
    """Generate T2 Schedule 100: Balance Sheet."""
    fy = fiscal_year_end[:4]
    period_start = f"{fy}-01-01"
    period_end = fiscal_year_end[:10]

    # Assets
    cash = _sum_gl_range_balance(client_code, 1000, 1099, period_end, conn)
    ar = _sum_gl_range_balance(client_code, 1100, 1199, period_end, conn)
    inventory = _sum_gl_range_balance(client_code, 1200, 1299, period_end, conn)
    prepaid = _sum_gl_range_balance(client_code, 1400, 1499, period_end, conn)

    # Capital assets net of CCA
    capital_assets_net = _ZERO
    try:
        row = conn.execute(
            """SELECT COALESCE(SUM(current_ucc), 0) AS total
               FROM fixed_assets
               WHERE client_code = ? AND status = 'active'""",
            (client_code,),
        ).fetchone()
        capital_assets_net = _to_decimal(row["total"] if row else 0)
    except Exception:
        pass

    total_assets = _round(cash + ar + inventory + prepaid + capital_assets_net)

    # Liabilities
    ap = _sum_gl_range_balance(client_code, 2000, 2099, period_end, conn)
    taxes_payable = _sum_gl_range_balance(client_code, 2100, 2199, period_end, conn)
    gst_qst_payable = _sum_gl_range_balance(client_code, 2200, 2299, period_end, conn)
    lt_debt = _sum_gl_range_balance(client_code, 2500, 2599, period_end, conn)
    total_liabilities = _round(ap + taxes_payable + gst_qst_payable + lt_debt)

    # Equity
    share_capital = _sum_gl_range_balance(client_code, 3000, 3099, period_end, conn)
    re_opening = _sum_gl_range_balance(client_code, 3100, 3199, period_end, conn)
    revenue = _sum_gl_range(client_code, 4000, 4999, period_start, period_end, conn)
    expenses_total = _sum_gl_range(client_code, 5000, 9999, period_start, period_end, conn)
    net_income_year = _round(revenue - expenses_total)
    total_equity = _round(share_capital + re_opening + net_income_year)

    lines = [
        # Assets
        _make_line("101", "Cash and deposits", cash,
                   _gl_accounts_for_range(client_code, 1000, 1099, period_start, period_end, conn)),
        _make_line("105", "Accounts receivable", ar,
                   _gl_accounts_for_range(client_code, 1100, 1199, period_start, period_end, conn)),
        _make_line("110", "Inventory", inventory,
                   _gl_accounts_for_range(client_code, 1200, 1299, period_start, period_end, conn)),
        _make_line("125", "Prepaid expenses", prepaid,
                   _gl_accounts_for_range(client_code, 1400, 1499, period_start, period_end, conn)),
        _make_line("171", "Capital assets net of CCA", capital_assets_net),
        _make_line("199", "Total assets", total_assets),
        # Liabilities
        _make_line("301", "Accounts payable", ap,
                   _gl_accounts_for_range(client_code, 2000, 2099, period_start, period_end, conn)),
        _make_line("305", "Income taxes payable", taxes_payable,
                   _gl_accounts_for_range(client_code, 2100, 2199, period_start, period_end, conn)),
        _make_line("310", "GST/QST payable", gst_qst_payable,
                   _gl_accounts_for_range(client_code, 2200, 2299, period_start, period_end, conn)),
        _make_line("320", "Long-term debt", lt_debt,
                   _gl_accounts_for_range(client_code, 2500, 2599, period_start, period_end, conn)),
        _make_line("399", "Total liabilities", total_liabilities),
        # Equity
        _make_line("500", "Share capital", share_capital,
                   _gl_accounts_for_range(client_code, 3000, 3099, period_start, period_end, conn)),
        _make_line("525", "Retained earnings opening", re_opening,
                   _gl_accounts_for_range(client_code, 3100, 3199, period_start, period_end, conn)),
        _make_line("530", "Net income for year", net_income_year),
        _make_line("599", "Total equity", total_equity),
    ]

    return {
        "schedule": "100",
        "title": "Balance Sheet",
        "title_fr": "Bilan",
        "lines": lines,
    }


# ---------------------------------------------------------------------------
# Schedule 125 — Income Statement
# ---------------------------------------------------------------------------

def generate_schedule_125(client_code: str, fiscal_year_end: str,
                          conn: sqlite3.Connection) -> dict[str, Any]:
    """Generate T2 Schedule 125: Income Statement."""
    fy = fiscal_year_end[:4]
    period_start = f"{fy}-01-01"
    period_end = fiscal_year_end[:10]

    revenue = _sum_gl_range(client_code, 8000, 8299, period_start, period_end, conn)
    # Also include 4000-4999 if 8xxx is empty (common GL range for revenue)
    if revenue == _ZERO:
        revenue = _sum_gl_range(client_code, 4000, 4999, period_start, period_end, conn)

    cogs = _sum_gl_range(client_code, 8300, 8499, period_start, period_end, conn)
    if cogs == _ZERO:
        cogs = _sum_gl_range(client_code, 5000, 5499, period_start, period_end, conn)

    gross_profit = _round(revenue - cogs)

    operating_expenses = _sum_gl_range(client_code, 8500, 8799, period_start, period_end, conn)
    if operating_expenses == _ZERO:
        operating_expenses = _sum_gl_range(client_code, 5500, 7999, period_start, period_end, conn)

    net_income_before_tax = _round(gross_profit - operating_expenses)

    lines = [
        _make_line("8000", "Revenue", revenue,
                   _gl_accounts_for_range(client_code, 4000, 4999, period_start, period_end, conn)
                   or _gl_accounts_for_range(client_code, 8000, 8299, period_start, period_end, conn)),
        _make_line("8300", "Cost of goods sold", cogs,
                   _gl_accounts_for_range(client_code, 5000, 5499, period_start, period_end, conn)
                   or _gl_accounts_for_range(client_code, 8300, 8499, period_start, period_end, conn)),
        _make_line("GP", "Gross profit", gross_profit),
        _make_line("8500", "Operating expenses", operating_expenses,
                   _gl_accounts_for_range(client_code, 5500, 7999, period_start, period_end, conn)
                   or _gl_accounts_for_range(client_code, 8500, 8799, period_start, period_end, conn)),
        _make_line("NI", "Net income before tax", net_income_before_tax),
    ]

    return {
        "schedule": "125",
        "title": "Income Statement",
        "title_fr": "État des résultats",
        "lines": lines,
    }


# ---------------------------------------------------------------------------
# CO-17 Quebec Mapping
# ---------------------------------------------------------------------------

# T2 line -> CO-17 line mapping
T2_TO_CO17: dict[str, str] = {
    # Schedule 100 Balance Sheet
    "101": "10",    # Cash
    "105": "14",    # Accounts receivable
    "110": "18",    # Inventory
    "125": "22",    # Prepaid
    "171": "30",    # Capital assets
    "199": "60",    # Total assets
    "301": "100",   # AP
    "305": "104",   # Taxes payable
    "310": "108",   # GST/QST payable
    "320": "120",   # Long-term debt
    "399": "140",   # Total liabilities
    "500": "200",   # Share capital
    "525": "210",   # Retained earnings
    "530": "220",   # Net income
    "599": "250",   # Total equity
    # Schedule 125 Income Statement
    "8000": "30a",  # Revenue
    "8300": "40a",  # COGS
    "8500": "60a",  # Operating expenses
    # Schedule 1
    "001": "1a",    # Net income per FS
    "101_s1": "10a",  # Meals add-back
    "104_s1": "20a",  # Amortization add-back
    "200": "30b",   # CCA deduction
    "300": "99a",   # Taxable income
}


def generate_co17_mapping(client_code: str, fiscal_year_end: str,
                          conn: sqlite3.Connection) -> dict[str, Any]:
    """Generate CO-17 Quebec corporate tax return mapping."""
    sched100 = generate_schedule_100(client_code, fiscal_year_end, conn)
    sched125 = generate_schedule_125(client_code, fiscal_year_end, conn)
    sched1 = generate_schedule_1(client_code, fiscal_year_end, conn)

    co17_lines: list[dict[str, Any]] = []

    # Map balance sheet lines
    for line in sched100["lines"]:
        co17_line = T2_TO_CO17.get(line["line"])
        if co17_line:
            co17_lines.append({
                "t2_line": line["line"],
                "co17_line": co17_line,
                "description": line["description"],
                "amount": line["amount"],
                "gl_accounts": line["gl_accounts"],
                "confidence": line["confidence"],
            })

    # Map income statement lines
    for line in sched125["lines"]:
        co17_line = T2_TO_CO17.get(line["line"])
        if co17_line:
            co17_lines.append({
                "t2_line": line["line"],
                "co17_line": co17_line,
                "description": line["description"],
                "amount": line["amount"],
                "gl_accounts": line["gl_accounts"],
                "confidence": line["confidence"],
            })

    # Map schedule 1 lines
    for line in sched1["lines"]:
        co17_key = line["line"] + "_s1" if line["line"] in ("101", "104") else line["line"]
        co17_line = T2_TO_CO17.get(co17_key)
        if co17_line:
            co17_lines.append({
                "t2_line": line["line"],
                "co17_line": co17_line,
                "description": line["description"],
                "amount": line["amount"],
                "gl_accounts": line["gl_accounts"],
                "confidence": line["confidence"],
            })

    return {
        "title": "CO-17 Quebec Corporate Tax Return",
        "title_fr": "CO-17 Déclaration de revenus des sociétés du Québec",
        "lines": co17_lines,
    }


# ---------------------------------------------------------------------------
# Main pre-fill function
# ---------------------------------------------------------------------------

def generate_t2_prefill(client_code: str, fiscal_year_end: str,
                        conn: sqlite3.Connection) -> dict:
    """Pre-fill all T2 schedules from bookkeeping data."""
    sched1 = generate_schedule_1(client_code, fiscal_year_end, conn)
    sched8 = generate_schedule_8(client_code, fiscal_year_end[:4], conn)
    sched50 = generate_schedule_50(client_code, fiscal_year_end, conn)
    sched100 = generate_schedule_100(client_code, fiscal_year_end, conn)
    sched125 = generate_schedule_125(client_code, fiscal_year_end, conn)
    co17 = generate_co17_mapping(client_code, fiscal_year_end, conn)

    return {
        "client_code": client_code,
        "fiscal_year_end": fiscal_year_end,
        "schedule_1": sched1,
        "schedule_8": sched8,
        "schedule_50": sched50,
        "schedule_100": sched100,
        "schedule_125": sched125,
        "co17": co17,
        "disclaimer": {
            "fr": (
                "Ces montants sont pré-remplis à partir de la comptabilité. "
                "Veuillez vérifier chaque ligne avant de préparer la déclaration T2 officielle."
            ),
            "en": (
                "These amounts are pre-filled from the bookkeeping data. "
                "Please verify each line before preparing the official T2 return."
            ),
        },
        "generated_at": _utc_now(),
    }


# ---------------------------------------------------------------------------
# Filing history persistence + PDF generation (Sprint F Fix 3)
# ---------------------------------------------------------------------------

FILING_HISTORY_DDL = """
CREATE TABLE IF NOT EXISTS filing_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_code TEXT NOT NULL DEFAULT '',
    client_code TEXT NOT NULL,
    filing_type TEXT NOT NULL,
    tax_year INTEGER,
    period_start TEXT,
    period_end TEXT,
    file_path TEXT,
    generated_by TEXT,
    generated_at TEXT DEFAULT (datetime('now')),
    filed_at TEXT,
    status TEXT DEFAULT 'draft',
    cra_confirmation TEXT
);
CREATE INDEX IF NOT EXISTS idx_filing_history_client
    ON filing_history(client_code, filing_type, tax_year);
"""


def ensure_filing_history_table(conn: sqlite3.Connection) -> None:
    """Create filing_history table (idempotent)."""
    for stmt in FILING_HISTORY_DDL.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()


def record_filing(
    conn: sqlite3.Connection,
    *,
    firm_code: str,
    client_code: str,
    filing_type: str,
    tax_year: int | None = None,
    period_start: str = "",
    period_end: str = "",
    file_path: str = "",
    generated_by: str = "",
    status: str = "generated",
) -> int:
    """Insert a row into filing_history. Returns the new row id."""
    ensure_filing_history_table(conn)
    cur = conn.execute(
        """INSERT INTO filing_history
           (firm_code, client_code, filing_type, tax_year,
            period_start, period_end, file_path, generated_by, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (firm_code, client_code, filing_type, tax_year, period_start,
         period_end, file_path, generated_by, status),
    )
    conn.commit()
    return int(cur.lastrowid)


def mark_filing_submitted(
    conn: sqlite3.Connection,
    filing_id: int,
    cra_confirmation: str = "",
) -> None:
    """Transition a filing from 'generated' -> 'filed'."""
    conn.execute(
        """UPDATE filing_history
           SET status='filed', filed_at=datetime('now'), cra_confirmation=?
           WHERE id=?""",
        (cra_confirmation, filing_id),
    )
    conn.commit()


def get_filings(
    conn: sqlite3.Connection,
    client_code: str,
    filing_type: str = "",
) -> list[dict]:
    """Return filings for a client, newest first."""
    ensure_filing_history_table(conn)
    if filing_type:
        rows = conn.execute(
            """SELECT * FROM filing_history
               WHERE LOWER(client_code)=LOWER(?) AND filing_type=?
               ORDER BY generated_at DESC""",
            (client_code, filing_type),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT * FROM filing_history
               WHERE LOWER(client_code)=LOWER(?)
               ORDER BY generated_at DESC""",
            (client_code,),
        ).fetchall()
    conn.row_factory = sqlite3.Row
    return [dict(r) for r in rows] if rows else []


def generate_t2_pdf(
    client_code: str,
    fiscal_year_end: str,
    conn: sqlite3.Connection,
    *,
    generated_by: str = "",
    output_dir: str | Path | None = None,
    persist: bool = True,
) -> tuple[bytes, str, int | None]:
    """Generate a T2 PDF and (optionally) persist it to filing_history.

    Returns (pdf_bytes, file_path, filing_history_id).
    """
    data = generate_t2_prefill(client_code, fiscal_year_end, conn)
    # Guard: refuse to produce a nil return as a "real" filing. Every schedule
    # always emits at least one zero line, so we require at least one non-zero
    # amount somewhere before persisting. CPAs who want a setup preview can
    # call generate_t2_prefill() directly.
    has_data = False
    for sched_key in ("schedule_1", "schedule_8", "schedule_100", "schedule_125"):
        sched = data.get(sched_key) or {}
        for ln in sched.get("lines", []) or []:
            amt = ln.get("amount", 0)
            try:
                if abs(float(amt)) > 0.01:
                    has_data = True
                    break
            except (TypeError, ValueError):
                continue
        if has_data:
            break
    if not has_data:
        raise ValueError(
            "T2 PDF cannot be generated: no non-zero GL postings for "
            f"{client_code} / {fiscal_year_end}. Verify bookkeeping data exists.",
        )

    try:
        pdf_bytes = _render_t2_reportlab(data)
    except ImportError:
        pdf_bytes = _render_t2_minimal(data)

    tax_year = int(fiscal_year_end[:4]) if fiscal_year_end[:4].isdigit() else None
    file_path = ""
    filing_id: int | None = None
    if persist:
        base_dir = Path(output_dir) if output_dir else ROOT_DIR / "data" / "filings"
        target = base_dir / client_code
        target.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = target / f"T2_{tax_year}_{stamp}.pdf"
        path.write_bytes(pdf_bytes)
        file_path = str(path)
        filing_id = record_filing(
            conn,
            firm_code="",
            client_code=client_code,
            filing_type="T2",
            tax_year=tax_year,
            period_end=fiscal_year_end,
            file_path=file_path,
            generated_by=generated_by,
            status="generated",
        )
    return pdf_bytes, file_path, filing_id


def _render_t2_reportlab(data: dict) -> bytes:
    """Render the T2 PDF using reportlab (preferred path)."""
    from io import BytesIO
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        SimpleDocTemplate,
        Paragraph,
        Spacer,
        Table,
        TableStyle,
        PageBreak,
    )
    from reportlab.lib import colors

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=letter,
        leftMargin=0.6 * inch, rightMargin=0.6 * inch,
        topMargin=0.6 * inch, bottomMargin=0.6 * inch,
    )
    styles = getSampleStyleSheet()
    h_style = ParagraphStyle("h", parent=styles["Heading1"], fontSize=16, spaceAfter=12)
    sub_style = ParagraphStyle("sub", parent=styles["Heading2"], fontSize=12, spaceAfter=6)
    normal = styles["BodyText"]

    client = data.get("client_code", "")
    fye = data.get("fiscal_year_end", "")
    tax_year = fye[:4] if fye else ""

    story: list = []
    story.append(Paragraph("T2 Corporation Income Tax Return", h_style))
    story.append(Paragraph(f"Client: <b>{client}</b>", normal))
    story.append(Paragraph(f"Fiscal year end: <b>{fye}</b>", normal))
    story.append(Paragraph(f"Tax year: <b>{tax_year}</b>", normal))
    story.append(Paragraph(f"Generated: {data.get('generated_at', '')}", normal))
    story.append(Spacer(1, 0.2 * inch))
    story.append(Paragraph(
        "<b>Pre-fill disclaimer:</b> " + data.get("disclaimer", {}).get("en", ""),
        normal,
    ))
    story.append(PageBreak())

    def _schedule_table(sched_data: dict, title: str) -> list:
        out = [Paragraph(title, sub_style)]
        lines = sched_data.get("lines") or []
        if not lines:
            out.append(Paragraph("<i>No data.</i>", normal))
            return out
        rows = [["Line", "Description", "Amount"]]
        for ln in lines:
            amt = ln.get("amount", 0)
            if isinstance(amt, Decimal):
                amt_str = f"${amt:,.2f}"
            else:
                try:
                    amt_str = f"${float(amt):,.2f}"
                except (TypeError, ValueError):
                    amt_str = str(amt)
            rows.append([
                str(ln.get("line", "")),
                str(ln.get("description", ""))[:60],
                amt_str,
            ])
        tbl = Table(rows, colWidths=[0.7 * inch, 4.5 * inch, 1.3 * inch])
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a2d5c")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ALIGN", (2, 0), (2, -1), "RIGHT"),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f7f7f9")]),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
        ]))
        out.append(tbl)
        out.append(Spacer(1, 0.25 * inch))
        return out

    for sched_key, title in (
        ("schedule_1", "Schedule 1 — Net Income for Tax Purposes"),
        ("schedule_8", "Schedule 8 — Capital Cost Allowance (CCA)"),
        ("schedule_50", "Schedule 50 — Shareholder Information"),
        ("schedule_100", "Schedule 100 — Balance Sheet"),
        ("schedule_125", "Schedule 125 — Income Statement"),
    ):
        if data.get(sched_key):
            story.extend(_schedule_table(data[sched_key], title))

    co17 = data.get("co17") or {}
    if co17.get("lines"):
        story.append(PageBreak())
        story.extend(_schedule_table(co17, "CO-17 — Quebec Corporation Return"))

    doc.build(story)
    return buf.getvalue()


def _render_t2_minimal(data: dict) -> bytes:
    """Fallback renderer: valid single-page PDF even if reportlab is absent.
    Uses a hand-rolled PDF writer; same structural shape as the real renderer.
    """
    content = _t2_plain_text(data)
    # Minimal PDF 1.4 with one text page using Courier.
    # Each line becomes a showText call; pages auto-break every ~55 lines.
    lines = content.split("\n")
    page_lines = [lines[i:i + 55] for i in range(0, len(lines), 55)]
    objects: list[bytes] = []

    def _add(obj: bytes) -> int:
        objects.append(obj)
        return len(objects)

    # Header objects are added in proper order to keep refs right.
    page_ids = list(range(3, 3 + len(page_lines)))
    kids = " ".join(f"{pid} 0 R" for pid in page_ids)
    _add(b"<< /Type /Catalog /Pages 2 0 R >>")
    _add(f"<< /Type /Pages /Kids [{kids}] /Count {len(page_lines)} >>".encode())
    content_offsets: list[int] = []
    for idx, chunk in enumerate(page_lines, start=3):
        page_obj = (
            f"<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 "
            f"{3 + 2 * len(page_lines)} 0 R >> >> "
            f"/MediaBox [0 0 612 792] /Contents {idx + len(page_lines)} 0 R >>"
        ).encode()
        _add(page_obj)
    for chunk in page_lines:
        stream_lines = ["BT", "/F1 9 Tf", "50 752 Td"]
        for ln in chunk:
            safe = ln.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
            stream_lines.append(f"({safe}) Tj")
            stream_lines.append("0 -12 Td")
        stream_lines.append("ET")
        stream = "\n".join(stream_lines).encode("latin-1", errors="replace")
        _add(f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream")
    _add(b"<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>")

    out = bytearray(b"%PDF-1.4\n")
    offsets: list[int] = [0]
    for i, obj in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode() + obj + b"\nendobj\n"
    xref_pos = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode()
    out += b"0000000000 65535 f \n"
    for off in offsets[1:]:
        out += f"{off:010d} 00000 n \n".encode()
    out += (
        f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\n"
        f"startxref\n{xref_pos}\n%%EOF\n"
    ).encode()
    return bytes(out)


def _t2_plain_text(data: dict) -> str:
    lines = []
    client = data.get("client_code", "")
    fye = data.get("fiscal_year_end", "")
    lines.append(f"T2 Corporation Income Tax Return -- {client} -- FYE {fye}")
    lines.append(data.get("disclaimer", {}).get("en", ""))
    lines.append("")
    for sched_key, title in (
        ("schedule_1", "Schedule 1 - Net Income for Tax Purposes"),
        ("schedule_8", "Schedule 8 - Capital Cost Allowance"),
        ("schedule_50", "Schedule 50 - Shareholder Information"),
        ("schedule_100", "Schedule 100 - Balance Sheet"),
        ("schedule_125", "Schedule 125 - Income Statement"),
    ):
        sched = data.get(sched_key) or {}
        if not sched.get("lines"):
            continue
        lines.append("")
        lines.append(title)
        lines.append("-" * len(title))
        for ln in sched.get("lines", []):
            amt = ln.get("amount", 0)
            try:
                amt_f = float(amt)
                amt_str = f"${amt_f:>12,.2f}"
            except (TypeError, ValueError):
                amt_str = str(amt)
            lines.append(
                f"  Line {str(ln.get('line',''))[:6]:6s} {str(ln.get('description',''))[:45]:45s} {amt_str}"
            )
    return "\n".join(lines)
