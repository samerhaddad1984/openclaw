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
