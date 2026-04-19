"""
src/engines/tax_edge_cases.py — Sprint H F5.

Tax edge-case helpers that don't justify their own engine module:

  * apply_ncl_carryforward — apply Non-Capital Loss carryforward up to 20 years.
  * calculate_residential_rebate — GST/HST New Housing Rebate.
  * gift_card_tax_treatment — clarifies that gift-card sales are not taxable
    at sale; only when redeemed.
  * is_zero_rated_grocery / is_exempt_supply — better classification rules.
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


# ---------------------------------------------------------------------------
# NCL carryforward
# ---------------------------------------------------------------------------

NCL_DDL = """
CREATE TABLE IF NOT EXISTS non_capital_losses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_code TEXT DEFAULT '',
    client_code TEXT NOT NULL,
    origin_year INTEGER NOT NULL,
    amount REAL NOT NULL,
    applied_amount REAL DEFAULT 0,
    expires_year INTEGER,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_ncl_client
    ON non_capital_losses(client_code, origin_year);
"""

# CRA: NCL carryforward = 20 years (since March 22, 2004).
NCL_CARRYFORWARD_YEARS = 20


def ensure_ncl_table(conn: sqlite3.Connection) -> None:
    for stmt in NCL_DDL.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.commit()


def record_ncl(
    conn: sqlite3.Connection,
    *,
    client_code: str,
    origin_year: int,
    amount: float | Decimal | str,
    firm_code: str = "",
) -> int:
    """Record a fresh NCL for a client. Sets expires_year automatically."""
    amount_d = Decimal(str(amount))
    if amount_d < 0:
        raise ValueError("NCL amount must be >= 0")
    ensure_ncl_table(conn)
    cur = conn.execute(
        """INSERT INTO non_capital_losses
           (firm_code, client_code, origin_year, amount, expires_year)
           VALUES (?, ?, ?, ?, ?)""",
        (firm_code, client_code, origin_year, float(amount_d),
         origin_year + NCL_CARRYFORWARD_YEARS),
    )
    conn.commit()
    return int(cur.lastrowid)


def get_ncl_balance(
    conn: sqlite3.Connection,
    client_code: str,
    as_of_year: int | None = None,
) -> Decimal:
    """Return total unapplied NCL still within the 20-year window."""
    ensure_ncl_table(conn)
    if as_of_year is None:
        as_of_year = _date.today().year
    rows = conn.execute(
        """SELECT (amount - COALESCE(applied_amount, 0)) AS remaining
           FROM non_capital_losses
           WHERE LOWER(client_code) = LOWER(?)
             AND expires_year >= ?
             AND (amount - COALESCE(applied_amount, 0)) > 0""",
        (client_code, as_of_year),
    ).fetchall()
    return sum((Decimal(str(r["remaining"])) for r in rows), _ZERO)


def apply_ncl_carryforward(
    conn: sqlite3.Connection,
    *,
    client_code: str,
    fiscal_year: int,
    current_income: float | Decimal | str,
) -> dict[str, Any]:
    """Apply NCL up to current_income. Oldest losses applied first (FIFO).

    Returns a dict with applied amount, remaining NCL, and effective taxable
    income.
    """
    ensure_ncl_table(conn)
    income_d = Decimal(str(current_income))
    if income_d <= 0:
        return {
            "applied": 0.0,
            "remaining_ncl": float(get_ncl_balance(conn, client_code, fiscal_year)),
            "effective_taxable_income": float(income_d),
        }

    # FIFO: oldest origin_year first, drawn down before newer ones.
    rows = conn.execute(
        """SELECT id, origin_year, amount, COALESCE(applied_amount, 0) AS applied
           FROM non_capital_losses
           WHERE LOWER(client_code) = LOWER(?)
             AND expires_year >= ?
             AND (amount - COALESCE(applied_amount, 0)) > 0
           ORDER BY origin_year ASC, id ASC""",
        (client_code, fiscal_year),
    ).fetchall()

    remaining_to_offset = income_d
    total_applied = _ZERO
    for r in rows:
        if remaining_to_offset <= 0:
            break
        available = Decimal(str(r["amount"])) - Decimal(str(r["applied"]))
        take = min(available, remaining_to_offset)
        if take <= 0:
            continue
        new_applied = Decimal(str(r["applied"])) + take
        conn.execute(
            "UPDATE non_capital_losses SET applied_amount=? WHERE id=?",
            (float(new_applied), r["id"]),
        )
        total_applied += take
        remaining_to_offset -= take
    conn.commit()

    return {
        "applied": float(_round(total_applied)),
        "remaining_ncl": float(get_ncl_balance(conn, client_code, fiscal_year)),
        "effective_taxable_income": float(_round(income_d - total_applied)),
    }


# ---------------------------------------------------------------------------
# Residential GST/HST New Housing Rebate
# ---------------------------------------------------------------------------

# Federal GST New Housing Rebate (2026 rules):
#   * Full 36% federal GST rebate for homes priced ≤ $350,000 (max $6,300).
#   * Phase-out between $350K and $450K (linear).
#   * No federal rebate above $450K.

FED_REBATE_FULL_THRESHOLD = Decimal("350000")
FED_REBATE_ZERO_THRESHOLD = Decimal("450000")
FED_REBATE_MAX = Decimal("6300")
FED_REBATE_PCT_OF_GST = Decimal("0.36")

# Quebec QST New Housing Rebate (50% of QST up to a $9,975 cap is the
# common 2025 number; we expose it as the QC component).
QC_REBATE_MAX = Decimal("9975")
QC_REBATE_PCT_OF_QST = Decimal("0.50")


def calculate_residential_rebate(
    purchase_price: float | Decimal | str,
    *,
    province: str = "QC",
    is_principal_residence: bool = True,
    gst_rate: float | Decimal | str = "0.05",
    qst_rate: float | Decimal | str = "0.09975",
) -> dict[str, Any]:
    """Compute the federal GST + provincial QST New Housing Rebate.

    Only the principal-residence case qualifies.
    """
    if not is_principal_residence:
        return {
            "qualifies": False,
            "reason": "not_principal_residence",
            "federal_rebate": 0.0,
            "provincial_rebate": 0.0,
            "total_rebate": 0.0,
        }

    price = Decimal(str(purchase_price))
    if price <= 0:
        return {
            "qualifies": False,
            "reason": "non_positive_price",
            "federal_rebate": 0.0,
            "provincial_rebate": 0.0,
            "total_rebate": 0.0,
        }

    gst_paid = _round(price * Decimal(str(gst_rate)))
    qst_paid = _round(price * Decimal(str(qst_rate)))

    if price <= FED_REBATE_FULL_THRESHOLD:
        fed_rebate = min(FED_REBATE_MAX, _round(gst_paid * FED_REBATE_PCT_OF_GST))
    elif price < FED_REBATE_ZERO_THRESHOLD:
        phase_out = (FED_REBATE_ZERO_THRESHOLD - price) / Decimal("100000")
        fed_rebate = _round(FED_REBATE_MAX * phase_out)
    else:
        fed_rebate = _ZERO

    if province.upper() == "QC":
        prov_rebate_uncapped = _round(qst_paid * QC_REBATE_PCT_OF_QST)
        prov_rebate = min(QC_REBATE_MAX, prov_rebate_uncapped)
    else:
        prov_rebate = _ZERO

    return {
        "qualifies": True,
        "purchase_price": float(price),
        "gst_paid": float(gst_paid),
        "qst_paid": float(qst_paid),
        "federal_rebate": float(fed_rebate),
        "provincial_rebate": float(prov_rebate),
        "total_rebate": float(_round(fed_rebate + prov_rebate)),
        "computed_at": _utc_now(),
    }


# ---------------------------------------------------------------------------
# Gift cards
# ---------------------------------------------------------------------------

def gift_card_tax_treatment(action: str) -> dict[str, Any]:
    """Gift-card semantics under CRA / Revenu Québec.

    action = 'sale' → no tax collected; record as deferred revenue.
    action = 'redemption' → tax collected on the underlying purchase.
    action = 'expiration' → recognise as revenue (not tax-related).
    """
    if action == "sale":
        return {
            "tax_collected_at_sale": False,
            "treat_as": "deferred_revenue",
            "rationale": (
                "Per CRA and Revenu Québec, gift cards are payment "
                "instruments. Tax applies on redemption, not on sale."
            ),
        }
    if action == "redemption":
        return {
            "tax_collected_at_sale": True,
            "treat_as": "regular_taxable_supply",
            "rationale": "Tax applies to the actual goods/services purchased.",
        }
    if action == "expiration":
        return {
            "tax_collected_at_sale": False,
            "treat_as": "revenue_recognition",
            "rationale": (
                "Quebec prohibits expiry on most gift cards; only specific "
                "categories (promotional, multi-merchant) can expire."
            ),
        }
    raise ValueError(f"unknown gift-card action: {action!r}")


# ---------------------------------------------------------------------------
# Zero-rated vs exempt classification
# ---------------------------------------------------------------------------

_ZERO_RATED_GROCERIES_KEYWORDS = (
    "milk", "bread", "egg", "rice", "pasta", "flour", "sugar", "salt",
    "vegetable", "fruit", "lait", "pain", "oeuf", "œuf", "riz",
    "farine", "sucre", "sel", "légume", "fruit",
    "meat", "poultry", "fish", "viande", "volaille", "poisson",
)

_EXEMPT_KEYWORDS = (
    "rent", "loyer", "long-term residential",
    "tuition", "frais scolarité", "school course",
    "medical service", "service médical", "physiotherapy", "physiothérapie",
    "dental", "dentaire", "optometry", "optométrie",
    "child care", "garderie", "garde enfants",
    "bank fee", "interest", "intérêt",
)


def is_zero_rated_grocery(description: str) -> bool:
    s = (description or "").lower()
    return any(k in s for k in _ZERO_RATED_GROCERIES_KEYWORDS)


def is_exempt_supply(description: str) -> bool:
    s = (description or "").lower()
    return any(k in s for k in _EXEMPT_KEYWORDS)
