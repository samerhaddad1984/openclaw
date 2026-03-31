"""
src/engines/cashflow_engine.py — Cash Flow Statement (Indirect Method).

Generates a complete statement of cash flows per ASPE Section 1540.
All monetary arithmetic uses Python Decimal.
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
    return _to_decimal(row.get("total", 0) if row else 0)


def _sum_gl_range_at_date(client_code: str, gl_start: int, gl_end: int,
                          as_of: str, conn: sqlite3.Connection) -> Decimal:
    """Sum amounts for GL accounts up to a given date (balance sheet snapshot)."""
    row = conn.execute(
        """SELECT COALESCE(SUM(amount), 0) AS total
           FROM documents
           WHERE LOWER(COALESCE(client_code, '')) = LOWER(?)
             AND COALESCE(document_date, '') <= ?
             AND CAST(SUBSTR(COALESCE(gl_account, '0'), 1, 4) AS INTEGER) BETWEEN ? AND ?""",
        (client_code, as_of, gl_start, gl_end),
    ).fetchone()
    return _to_decimal(row.get("total", 0) if row else 0)


# ---------------------------------------------------------------------------
# Component functions
# ---------------------------------------------------------------------------

def get_net_income(client_code: str, period_start: str, period_end: str,
                   conn: sqlite3.Connection) -> Decimal:
    """Calculate net income from revenue minus expenses in the period."""
    revenue = _sum_gl_range(client_code, 4000, 4999, period_start, period_end, conn)
    expenses = _sum_gl_range(client_code, 5000, 9999, period_start, period_end, conn)
    return _round(revenue - expenses)


def get_depreciation(client_code: str, period_start: str, period_end: str,
                     conn: sqlite3.Connection) -> Decimal:
    """Get total depreciation/CCA from fixed_assets for the period."""
    try:
        row = conn.execute(
            """SELECT COALESCE(SUM(accumulated_cca), 0) AS total
               FROM fixed_assets
               WHERE client_code = ? AND status IN ('active', 'disposed')""",
            (client_code,),
        ).fetchone()
        return _to_decimal(row.get("total", 0) if row else 0)
    except Exception:
        return _ZERO


def get_working_capital_changes(client_code: str, period_start: str, period_end: str,
                                conn: sqlite3.Connection) -> dict:
    """Calculate changes in working capital accounts between period start and end.

    Returns dict with individual changes and net total.
    Increase in current asset = negative cash impact.
    Increase in current liability = positive cash impact.
    """
    # Get balances at start and end of period
    # AR: GL 1100-1199
    ar_start = _sum_gl_range_at_date(client_code, 1100, 1199, period_start, conn)
    ar_end = _sum_gl_range_at_date(client_code, 1100, 1199, period_end, conn)
    ar_change = _round(ar_start - ar_end)  # decrease is positive cash

    # AP: GL 2000-2099
    ap_start = _sum_gl_range_at_date(client_code, 2000, 2099, period_start, conn)
    ap_end = _sum_gl_range_at_date(client_code, 2000, 2099, period_end, conn)
    ap_change = _round(ap_end - ap_start)  # increase is positive cash

    # Prepaid: GL 1400-1499
    prepaid_start = _sum_gl_range_at_date(client_code, 1400, 1499, period_start, conn)
    prepaid_end = _sum_gl_range_at_date(client_code, 1400, 1499, period_end, conn)
    prepaid_change = _round(prepaid_start - prepaid_end)  # decrease is positive cash

    # Inventory: GL 1200-1299
    inv_start = _sum_gl_range_at_date(client_code, 1200, 1299, period_start, conn)
    inv_end = _sum_gl_range_at_date(client_code, 1200, 1299, period_end, conn)
    inv_change = _round(inv_start - inv_end)  # decrease is positive cash

    net = _round(ar_change + ap_change + prepaid_change + inv_change)

    return {
        "accounts_receivable_change": float(ar_change),
        "accounts_payable_change": float(ap_change),
        "prepaid_expenses_change": float(prepaid_change),
        "inventory_change": float(inv_change),
        "net_working_capital_change": float(net),
    }


def get_investing_activities(client_code: str, period_start: str, period_end: str,
                             conn: sqlite3.Connection) -> dict:
    """Calculate investing cash flows from fixed asset transactions."""
    purchase_of_assets = _ZERO
    disposal_proceeds = _ZERO
    purchase_of_investments = _ZERO

    try:
        # Additions in the period
        rows = conn.execute(
            """SELECT COALESCE(SUM(cost), 0) AS total
               FROM fixed_assets
               WHERE client_code = ?
                 AND acquisition_date >= ? AND acquisition_date <= ?""",
            (client_code, period_start, period_end),
        ).fetchone()
        purchase_of_assets = _to_decimal(rows.get("total", 0) if rows else 0)

        # Disposals in the period
        rows = conn.execute(
            """SELECT COALESCE(SUM(disposal_proceeds), 0) AS total
               FROM fixed_assets
               WHERE client_code = ?
                 AND status = 'disposed'
                 AND disposal_date >= ? AND disposal_date <= ?""",
            (client_code, period_start, period_end),
        ).fetchone()
        disposal_proceeds = _to_decimal(rows.get("total", 0) if rows else 0)
    except Exception:
        pass

    # Investment purchases GL 1500-1599
    purchase_of_investments = _sum_gl_range(client_code, 1500, 1599, period_start, period_end, conn)

    net = _round(disposal_proceeds - purchase_of_assets - purchase_of_investments)

    return {
        "purchase_of_capital_assets": float(_round(purchase_of_assets)),
        "proceeds_from_disposal": float(_round(disposal_proceeds)),
        "purchase_of_investments": float(_round(purchase_of_investments)),
        "net_investing_activities": float(net),
    }


def get_financing_activities(client_code: str, period_start: str, period_end: str,
                             conn: sqlite3.Connection) -> dict:
    """Calculate financing cash flows from debt and equity transactions."""
    # Long-term debt: GL 2500-2599
    lt_debt_start = _sum_gl_range_at_date(client_code, 2500, 2599, period_start, conn)
    lt_debt_end = _sum_gl_range_at_date(client_code, 2500, 2599, period_end, conn)
    lt_debt_change = _round(lt_debt_end - lt_debt_start)

    # Positive = proceeds from new debt, Negative = repayments
    proceeds_from_debt = max(lt_debt_change, _ZERO)
    repayment_of_debt = abs(min(lt_debt_change, _ZERO))

    # Share capital: GL 3000-3099
    share_start = _sum_gl_range_at_date(client_code, 3000, 3099, period_start, conn)
    share_end = _sum_gl_range_at_date(client_code, 3000, 3099, period_end, conn)
    share_issuance = _round(max(share_end - share_start, _ZERO))

    # Dividends: GL 3200-3299
    dividends = _sum_gl_range(client_code, 3200, 3299, period_start, period_end, conn)

    net = _round(proceeds_from_debt - repayment_of_debt + share_issuance - dividends)

    return {
        "proceeds_from_long_term_debt": float(_round(proceeds_from_debt)),
        "repayment_of_long_term_debt": float(_round(repayment_of_debt)),
        "proceeds_from_share_issuance": float(_round(share_issuance)),
        "payment_of_dividends": float(_round(dividends)),
        "net_financing_activities": float(net),
    }


def validate_closing_cash(client_code: str, period_end: str,
                          calculated_cash: Decimal, conn: sqlite3.Connection) -> dict:
    """Validate that closing cash equals the bank balance."""
    # Cash accounts: GL 1000-1099
    bank_balance = _sum_gl_range_at_date(client_code, 1000, 1099, period_end, conn)

    difference = _round(calculated_cash - bank_balance)
    reconciled = abs(difference) < Decimal("0.02")  # Allow 1 cent rounding

    result: dict[str, Any] = {
        "calculated_closing_cash": float(_round(calculated_cash)),
        "bank_balance": float(_round(bank_balance)),
        "difference": float(difference),
        "reconciled": reconciled,
    }
    if not reconciled:
        result["flag"] = "cash_flow_reconciliation_gap"
        result["flag_amount"] = float(difference)

    return result


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------

def generate_cash_flow_statement(client_code: str, period_start: str, period_end: str,
                                 conn: sqlite3.Connection) -> dict:
    """Build a complete statement of cash flows using the indirect method per ASPE Section 1540."""
    net_income = get_net_income(client_code, period_start, period_end, conn)
    depreciation = get_depreciation(client_code, period_start, period_end, conn)

    # Loss/gain on disposal
    loss_on_disposal = _ZERO
    gain_on_disposal = _ZERO
    try:
        rows = conn.execute(
            """SELECT disposal_proceeds, cost, current_ucc
               FROM fixed_assets
               WHERE client_code = ? AND status = 'disposed'
                 AND disposal_date >= ? AND disposal_date <= ?""",
            (client_code, period_start, period_end),
        ).fetchall()
        for r in rows:
            proceeds = _to_decimal(r.get("disposal_proceeds", 0) if isinstance(r, dict) else 0)
            cost = _to_decimal(r.get("cost", 0) if isinstance(r, dict) else 0)
            # book value approximation
            if proceeds < cost:
                loss_on_disposal += _round(cost - proceeds)
            elif proceeds > cost:
                gain_on_disposal += _round(proceeds - cost)
    except Exception:
        pass

    working_capital = get_working_capital_changes(client_code, period_start, period_end, conn)
    wc_net = _to_decimal(working_capital["net_working_capital_change"])

    # Operating activities subtotal
    net_operating = _round(
        net_income + depreciation + loss_on_disposal - gain_on_disposal + wc_net
    )

    investing = get_investing_activities(client_code, period_start, period_end, conn)
    financing = get_financing_activities(client_code, period_start, period_end, conn)

    net_investing = _to_decimal(investing["net_investing_activities"])
    net_financing = _to_decimal(financing["net_financing_activities"])

    net_change = _round(net_operating + net_investing + net_financing)

    # Opening cash balance
    opening_cash = _sum_gl_range_at_date(client_code, 1000, 1099, period_start, conn)
    closing_cash = _round(opening_cash + net_change)

    # Validate against bank
    validation = validate_closing_cash(client_code, period_end, closing_cash, conn)

    flags: list[dict[str, Any]] = []
    if not validation["reconciled"]:
        flags.append({
            "flag": "cash_flow_reconciliation_gap",
            "amount": validation["difference"],
        })

    return {
        "client_code": client_code,
        "period_start": period_start,
        "period_end": period_end,
        "operating_activities": {
            "net_income": float(net_income),
            "depreciation_amortization": float(_round(depreciation)),
            "loss_on_disposal": float(_round(loss_on_disposal)),
            "gain_on_disposal": float(_round(gain_on_disposal)),
            "working_capital_changes": working_capital,
            "net_cash_from_operating": float(net_operating),
        },
        "investing_activities": investing,
        "financing_activities": financing,
        "net_change_in_cash": float(net_change),
        "opening_cash_balance": float(_round(opening_cash)),
        "closing_cash_balance": float(_round(closing_cash)),
        "bank_reconciliation": validation,
        "flags": flags,
        "generated_at": _utc_now(),
        "labels": {
            "fr": {
                "operating": "Activités d'exploitation",
                "investing": "Activités d'investissement",
                "financing": "Activités de financement",
                "net_change": "Augmentation (diminution) nette de la trésorerie",
                "opening": "Trésorerie à l'ouverture de la période",
                "closing": "Trésorerie à la clôture de la période",
            },
            "en": {
                "operating": "Operating Activities",
                "investing": "Investing Activities",
                "financing": "Financing Activities",
                "net_change": "Net increase (decrease) in cash",
                "opening": "Cash at beginning of period",
                "closing": "Cash at end of period",
            },
        },
    }
