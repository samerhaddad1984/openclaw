"""
src/engines/sred_engine.py — Sprint H F4.

Scientific Research & Experimental Development (SR&ED) Investment Tax
Credit (ITC) calculator + minimal T661 form generator.

Federal rates (CRA T4088):
  * CCPC, taxable income ≤ $500K and taxable capital ≤ $10M:
      35% on first $3M of qualifying SR&ED expenditures (refundable)
      15% on excess over $3M (40% refundable for CCPC)
  * Other corporations: 15% (non-refundable)

Quebec R&D tax credit (RS&DE):
  * 30% on eligible salaries (refundable for SMEs ≤ $50M assets)

Proxy method: 55% prescribed proxy amount on salaries replaces tracked
overhead; standard "traditional" method requires individual overhead
tracking.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
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


# Federal rates / thresholds.
CCPC_SMALL_TAXABLE_INCOME_LIMIT = Decimal("500000")
CCPC_SMALL_TAXABLE_CAPITAL_LIMIT = Decimal("10000000")
ENHANCED_RATE = Decimal("0.35")
REGULAR_RATE = Decimal("0.15")
ENHANCED_EXPENDITURE_LIMIT = Decimal("3000000")
CCPC_REGULAR_REFUNDABLE_PCT = Decimal("0.40")
PROXY_METHOD_UPLIFT = Decimal("0.55")

# Quebec.
QC_RD_RATE_SMALL = Decimal("0.30")  # SME refundable rate

# Eligible categories.
CATEGORIES = {"salaries", "contractors", "materials", "overhead"}


SRED_DDL = """
CREATE TABLE IF NOT EXISTS sred_claims (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_code TEXT NOT NULL DEFAULT '',
    client_code TEXT NOT NULL,
    tax_year INTEGER NOT NULL,
    claim_type TEXT NOT NULL DEFAULT 'traditional',
    project_name TEXT NOT NULL,
    technological_advancement TEXT,
    technological_obstacles TEXT,
    work_performed TEXT,
    status TEXT DEFAULT 'draft',
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sred_expenditures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_id INTEGER NOT NULL,
    category TEXT NOT NULL,
    amount REAL NOT NULL,
    qualifying_amount REAL,
    document_id TEXT,
    description TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (claim_id) REFERENCES sred_claims(id)
);

CREATE INDEX IF NOT EXISTS idx_sred_exp_claim ON sred_expenditures(claim_id);
"""


def ensure_sred_tables(conn: sqlite3.Connection) -> None:
    for stmt in SRED_DDL.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.commit()


def create_claim(
    conn: sqlite3.Connection,
    *,
    firm_code: str = "",
    client_code: str,
    tax_year: int,
    project_name: str,
    claim_type: str = "traditional",
    technological_advancement: str = "",
    technological_obstacles: str = "",
    work_performed: str = "",
) -> int:
    if claim_type not in ("traditional", "proxy"):
        raise ValueError("claim_type must be 'traditional' or 'proxy'")
    ensure_sred_tables(conn)
    cur = conn.execute(
        """INSERT INTO sred_claims
           (firm_code, client_code, tax_year, claim_type, project_name,
            technological_advancement, technological_obstacles, work_performed)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (firm_code, client_code, tax_year, claim_type, project_name,
         technological_advancement, technological_obstacles, work_performed),
    )
    conn.commit()
    return int(cur.lastrowid)


def add_expenditure(
    conn: sqlite3.Connection,
    *,
    claim_id: int,
    category: str,
    amount: float | Decimal | str,
    qualifying_amount: float | Decimal | str | None = None,
    document_id: str = "",
    description: str = "",
) -> int:
    if category not in CATEGORIES:
        raise ValueError(f"category must be one of {CATEGORIES}, got {category!r}")
    amount_d = Decimal(str(amount))
    if amount_d < 0:
        raise ValueError("amount must be >= 0")
    qa = (
        Decimal(str(qualifying_amount))
        if qualifying_amount is not None
        else amount_d
    )
    ensure_sred_tables(conn)
    cur = conn.execute(
        """INSERT INTO sred_expenditures
           (claim_id, category, amount, qualifying_amount, document_id, description)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (claim_id, category, float(amount_d), float(qa), document_id, description),
    )
    conn.commit()
    return int(cur.lastrowid)


def get_claim(conn: sqlite3.Connection, claim_id: int) -> dict[str, Any]:
    ensure_sred_tables(conn)
    row = conn.execute(
        "SELECT * FROM sred_claims WHERE id=?", (claim_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"sred claim not found: {claim_id}")
    return dict(row)


def get_expenditures(
    conn: sqlite3.Connection, claim_id: int,
) -> list[dict[str, Any]]:
    ensure_sred_tables(conn)
    rows = conn.execute(
        "SELECT * FROM sred_expenditures WHERE claim_id=?", (claim_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def calculate_sred_itc(
    conn: sqlite3.Connection,
    claim_id: int,
    *,
    corp_type: str = "ccpc_small",
    taxable_income: float | Decimal | str = 0,
    taxable_capital: float | Decimal | str = 0,
) -> dict[str, Any]:
    """Return the federal ITC for a claim.

    corp_type: 'ccpc_small' / 'ccpc' / 'other'
    """
    if corp_type not in ("ccpc_small", "ccpc", "other"):
        raise ValueError("corp_type must be ccpc_small, ccpc, or other")

    claim = get_claim(conn, claim_id)
    expenditures = get_expenditures(conn, claim_id)
    if not expenditures:
        return {
            "claim_id": claim_id,
            "status": "no_expenditures",
            "total_expenditures": 0.0,
            "qualifying_expenditures": 0.0,
            "itc_total": 0.0,
            "itc_refundable": 0.0,
            "itc_non_refundable": 0.0,
            "corp_type_applied": corp_type,
        }

    qualifying = sum(
        Decimal(str(e["qualifying_amount"] or e["amount"] or 0))
        for e in expenditures
    )
    total = sum(Decimal(str(e["amount"] or 0)) for e in expenditures)

    if claim["claim_type"] == "proxy":
        salaries = sum(
            Decimal(str(e["qualifying_amount"] or e["amount"] or 0))
            for e in expenditures if e["category"] == "salaries"
        )
        proxy_uplift = _round(salaries * PROXY_METHOD_UPLIFT)
        qualifying += proxy_uplift
    else:
        proxy_uplift = _ZERO

    income_d = Decimal(str(taxable_income))
    capital_d = Decimal(str(taxable_capital))

    if corp_type == "ccpc_small" and (
        income_d <= CCPC_SMALL_TAXABLE_INCOME_LIMIT
        and capital_d <= CCPC_SMALL_TAXABLE_CAPITAL_LIMIT
    ):
        enhanced_portion = min(qualifying, ENHANCED_EXPENDITURE_LIMIT)
        regular_portion = max(qualifying - ENHANCED_EXPENDITURE_LIMIT, _ZERO)
        enhanced_itc = _round(enhanced_portion * ENHANCED_RATE)
        regular_itc = _round(regular_portion * REGULAR_RATE)
        itc = enhanced_itc + regular_itc
        refundable = enhanced_itc + _round(regular_itc * CCPC_REGULAR_REFUNDABLE_PCT)
    elif corp_type == "ccpc":
        itc = _round(qualifying * REGULAR_RATE)
        refundable = _round(itc * CCPC_REGULAR_REFUNDABLE_PCT)
    else:  # other
        itc = _round(qualifying * REGULAR_RATE)
        refundable = _ZERO

    non_ref = _round(itc - refundable)
    return {
        "claim_id": claim_id,
        "claim_type": claim["claim_type"],
        "status": "ok",
        "total_expenditures": float(_round(total)),
        "proxy_uplift_applied": float(proxy_uplift),
        "qualifying_expenditures": float(_round(qualifying)),
        "itc_total": float(itc),
        "itc_refundable": float(refundable),
        "itc_non_refundable": float(non_ref),
        "corp_type_applied": corp_type,
    }


def calculate_quebec_rd_credit(
    conn: sqlite3.Connection,
    claim_id: int,
    *,
    is_sme: bool = True,
) -> dict[str, Any]:
    """Compute QC R&D refundable credit on eligible salaries."""
    expenditures = get_expenditures(conn, claim_id)
    salaries = sum(
        Decimal(str(e["qualifying_amount"] or e["amount"] or 0))
        for e in expenditures if e["category"] == "salaries"
    )
    rate = QC_RD_RATE_SMALL if is_sme else Decimal("0.14")
    credit = _round(salaries * rate)
    return {
        "claim_id": claim_id,
        "salaries": float(_round(salaries)),
        "rate": float(rate),
        "credit": float(credit),
        "refundable": is_sme,
    }


def generate_t661_summary(
    conn: sqlite3.Connection,
    claim_id: int,
    corp_type: str = "ccpc_small",
    taxable_income: float = 0,
    taxable_capital: float = 0,
) -> dict[str, Any]:
    """Generate a T661 summary as a dict; PDF rendering is a layer up.

    The CRA T661 form has dozens of boxes; we expose the most important
    line items so a CPA can transcribe them or a future PDF renderer can
    fill them in.
    """
    claim = get_claim(conn, claim_id)
    itc = calculate_sred_itc(conn, claim_id, corp_type=corp_type,
                              taxable_income=taxable_income,
                              taxable_capital=taxable_capital)
    return {
        "form": "T661",
        "claim_id": claim_id,
        "client_code": claim["client_code"],
        "tax_year": claim["tax_year"],
        "project_name": claim["project_name"],
        "claim_type": claim["claim_type"],
        "advancement": claim.get("technological_advancement"),
        "obstacles": claim.get("technological_obstacles"),
        "work_performed": claim.get("work_performed"),
        "line_350_total_qualifying": itc["qualifying_expenditures"],
        "line_400_proxy_uplift": itc.get("proxy_uplift_applied", 0.0),
        "line_500_itc_earned": itc["itc_total"],
        "line_530_refundable_itc": itc["itc_refundable"],
        "line_540_non_refundable_itc": itc["itc_non_refundable"],
        "generated_at": _utc_now(),
    }
