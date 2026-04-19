"""
src/engines/fixed_assets_engine.py — Fixed Assets Register with CCA Schedules.

Canadian Capital Cost Allowance (CCA) engine for OtoCPA.
Handles asset tracking, CCA calculation with half-year rule,
Schedule 8 generation, and asset disposals (recapture / terminal loss).

All monetary arithmetic uses Python Decimal.
"""
from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

CENT = Decimal("0.01")
_ZERO = Decimal("0")
_ONE = Decimal("1")
_TWO = Decimal("2")


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


# ---------------------------------------------------------------------------
# CCA Classes — all major Canadian classes
# ---------------------------------------------------------------------------

CCA_CLASSES: dict[int, dict[str, Any]] = {
    1:  {"description": "Buildings", "rate": Decimal("0.04"), "method": "declining"},
    6:  {"description": "Buildings (wood frame)", "rate": Decimal("0.10"), "method": "declining"},
    8:  {"description": "Miscellaneous equipment", "rate": Decimal("0.20"), "method": "declining"},
    10: {"description": "Automotive", "rate": Decimal("0.30"), "method": "declining"},
    101: {"description": "Passenger vehicles over $36,000", "rate": Decimal("0.30"), "method": "declining"},
    12: {"description": "Small tools under $500", "rate": Decimal("1.00"), "method": "declining"},
    13: {"description": "Leasehold improvements", "rate": None, "method": "straight-line"},
    14: {"description": "Patents/licenses", "rate": None, "method": "straight-line"},
    141: {"description": "Eligible capital property", "rate": Decimal("0.05"), "method": "declining"},
    16: {"description": "Taxis/rental cars", "rate": Decimal("0.40"), "method": "declining"},
    17: {"description": "Roads/parking", "rate": Decimal("0.08"), "method": "declining"},
    43: {"description": "Manufacturing equipment", "rate": Decimal("0.30"), "method": "declining"},
    44: {"description": "Patents", "rate": Decimal("0.25"), "method": "declining"},
    45: {"description": "Computers", "rate": Decimal("0.45"), "method": "declining"},
    50: {"description": "Computer equipment", "rate": Decimal("0.55"), "method": "declining"},
    53: {"description": "Manufacturing equipment (accelerated)", "rate": Decimal("0.50"), "method": "declining"},
    54: {"description": "Zero-emission vehicles (phase out)", "rate": Decimal("1.00"), "method": "declining"},
    55: {"description": "Zero-emission vehicles", "rate": Decimal("1.00"), "method": "declining"},
}

# Map display labels like "10.1" -> internal key 101, "14.1" -> 141
_DISPLAY_TO_KEY: dict[str, int] = {
    "10.1": 101,
    "14.1": 141,
}
_KEY_TO_DISPLAY: dict[int, str] = {
    101: "10.1",
    141: "14.1",
}


def normalize_cca_class(value: Any) -> int | None:
    """Accept '10.1', 10.1, 101, '101', 10, '10', etc. and return the internal int key."""
    s = str(value).strip()
    if s in _DISPLAY_TO_KEY:
        return _DISPLAY_TO_KEY[s]
    try:
        f = float(s)
        # 10.1 -> 101, 14.1 -> 141
        if f == 10.1:
            return 101
        if f == 14.1:
            return 141
        i = int(f)
        if i in CCA_CLASSES:
            return i
    except (ValueError, TypeError):
        pass
    return None


def cca_class_display(key: int) -> str:
    """Return the display string for a CCA class key (e.g. 101 -> '10.1')."""
    return _KEY_TO_DISPLAY.get(key, str(key))


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------

def ensure_fixed_assets_table(conn: sqlite3.Connection) -> None:
    """Create fixed_assets table (idempotent)."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fixed_assets (
            asset_id         TEXT PRIMARY KEY,
            client_code      TEXT NOT NULL,
            asset_name       TEXT NOT NULL,
            description      TEXT,
            cca_class        INTEGER NOT NULL,
            acquisition_date TEXT NOT NULL,
            cost             REAL NOT NULL,
            opening_ucc      REAL NOT NULL DEFAULT 0,
            current_ucc      REAL NOT NULL DEFAULT 0,
            accumulated_cca  REAL NOT NULL DEFAULT 0,
            status           TEXT NOT NULL DEFAULT 'active',
            disposal_date    TEXT,
            disposal_proceeds REAL,
            recapture_amount  REAL DEFAULT 0,
            terminal_loss_amount REAL DEFAULT 0,
            capital_gain_amount  REAL DEFAULT 0,
            disposal_reason   TEXT,
            created_at       TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_fixed_assets_client
            ON fixed_assets(client_code)
    """)
    # Idempotent ALTER for already-created tables (Sprint H Fix 1).
    for col, ddl in (
        ("recapture_amount", "REAL DEFAULT 0"),
        ("terminal_loss_amount", "REAL DEFAULT 0"),
        ("capital_gain_amount", "REAL DEFAULT 0"),
        ("disposal_reason", "TEXT"),
    ):
        try:
            conn.execute(f"ALTER TABLE fixed_assets ADD COLUMN {col} {ddl}")
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def add_asset(
    client_code: str,
    asset_name: str,
    acquisition_date: str,
    cost: float | Decimal | str,
    cca_class: int | str,
    conn: sqlite3.Connection,
) -> str:
    """Add a new fixed asset. Returns asset_id.

    Applies half-year rule: first-year CCA = (cost * rate) / 2.
    """
    ensure_fixed_assets_table(conn)

    cls_key = normalize_cca_class(cca_class)
    if cls_key is None or cls_key not in CCA_CLASSES:
        raise ValueError(f"Invalid CCA class: {cca_class}")

    cost_d = _to_decimal(cost)
    if cost_d <= _ZERO:
        raise ValueError("Cost must be positive")

    cls_info = CCA_CLASSES[cls_key]
    rate = cls_info["rate"]

    # Calculate first-year CCA with half-year rule
    if rate is not None:
        first_year_cca = _round((cost_d * rate) / _TWO)
    else:
        # Straight-line: no automatic CCA on add
        first_year_cca = _ZERO

    opening_ucc = cost_d
    current_ucc = _round(cost_d - first_year_cca)

    # Stable asset_id derived from inputs — deterministic across runs
    _hash_input = f"{client_code}|{asset_name}|{acquisition_date}|{float(cost_d)}|{cls_key}"
    asset_id = "FA-" + hashlib.sha256(_hash_input.encode()).hexdigest()[:12].upper()

    conn.execute(
        """INSERT INTO fixed_assets
           (asset_id, client_code, asset_name, cca_class, acquisition_date,
            cost, opening_ucc, current_ucc, accumulated_cca, status, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            asset_id,
            client_code,
            asset_name,
            cls_key,
            acquisition_date,
            float(_round(cost_d)),
            float(_round(opening_ucc)),
            float(_round(current_ucc)),
            float(first_year_cca),
            "active",
            _utc_now(),
        ),
    )
    conn.commit()
    return asset_id


def calculate_annual_cca(
    client_code: str,
    fiscal_year_end: str,
    conn: sqlite3.Connection,
    short_year_days: int | None = None,
) -> list[dict[str, Any]]:
    """Calculate CCA for all active assets for the given fiscal year.

    Parameters
    ----------
    fiscal_year_end : str
        ISO date string (YYYY-MM-DD) for the fiscal year end.
    short_year_days : int | None
        If the fiscal year is shorter than 365 days, pass the actual
        number of days for proration.

    Returns list of dicts with asset-level CCA breakdown.
    """
    ensure_fixed_assets_table(conn)

    rows = conn.execute(
        """SELECT * FROM fixed_assets
           WHERE client_code = ? AND status = 'active'
           ORDER BY cca_class, asset_name""",
        (client_code,),
    ).fetchall()

    proration = _ONE
    if short_year_days is not None and 0 < short_year_days < 365:
        proration = Decimal(str(short_year_days)) / Decimal("365")

    fy_end = fiscal_year_end[:10]  # YYYY-MM-DD
    results: list[dict[str, Any]] = []

    for row in rows:
        r = dict(row) if not isinstance(row, dict) else row
        cls_key = int(r["cca_class"])
        cls_info = CCA_CLASSES.get(cls_key)
        if cls_info is None:
            continue

        rate = cls_info["rate"]
        if rate is None:
            # Straight-line assets: skip auto-CCA
            results.append({
                "asset_id": r["asset_id"],
                "asset_name": r["asset_name"],
                "cca_class": cls_key,
                "cca_class_display": cca_class_display(cls_key),
                "opening_ucc": float(r["current_ucc"]),
                "additions": 0.0,
                "disposals": 0.0,
                "cca_amount": 0.0,
                "closing_ucc": float(r["current_ucc"]),
            })
            continue

        opening_ucc = _to_decimal(r["current_ucc"])
        acq_date = str(r["acquisition_date"])[:10]

        # Determine if this is a new acquisition in the current fiscal year
        is_new = acq_date >= fy_end[:4] + "-01-01"

        if opening_ucc <= _ZERO:
            results.append({
                "asset_id": r["asset_id"],
                "asset_name": r["asset_name"],
                "cca_class": cls_key,
                "cca_class_display": cca_class_display(cls_key),
                "opening_ucc": 0.0,
                "additions": 0.0,
                "disposals": 0.0,
                "cca_amount": 0.0,
                "closing_ucc": 0.0,
            })
            continue

        # Half-year rule: for new acquisitions where add_asset already
        # pre-applied half-year CCA, report that amount instead of
        # recalculating on the reduced UCC.
        accumulated = _to_decimal(r["accumulated_cca"])
        if is_new and accumulated > _ZERO:
            cca_amount = accumulated
        else:
            cca_amount = _round(opening_ucc * rate * proration)
        # Don't allow CCA to exceed UCC
        if cca_amount > opening_ucc:
            cca_amount = opening_ucc

        closing_ucc = _round(opening_ucc - cca_amount)

        # Update the asset
        accumulated = _to_decimal(r["accumulated_cca"]) + cca_amount
        conn.execute(
            """UPDATE fixed_assets
               SET current_ucc = ?, accumulated_cca = ?
               WHERE asset_id = ?""",
            (float(closing_ucc), float(_round(accumulated)), r["asset_id"]),
        )

        results.append({
            "asset_id": r["asset_id"],
            "asset_name": r["asset_name"],
            "cca_class": cls_key,
            "cca_class_display": cca_class_display(cls_key),
            "opening_ucc": float(_round(opening_ucc)),
            "additions": float(r["cost"]) if is_new else 0.0,
            "disposals": 0.0,
            "cca_amount": float(cca_amount),
            "closing_ucc": float(closing_ucc),
        })

    conn.commit()
    # FIX 6 (AZ): Deterministic output — sort by asset_id for stable ordering
    results.sort(key=lambda r: r["asset_id"])
    return results


def generate_schedule_8(
    client_code: str,
    fiscal_year: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Generate T2 Schedule 8 data structure grouped by CCA class.

    Parameters
    ----------
    fiscal_year : str
        Four-digit year or YYYY-MM-DD fiscal year end.

    Returns dict with class-level totals ready for Schedule 8 formatting.
    """
    ensure_fixed_assets_table(conn)

    rows = conn.execute(
        """SELECT * FROM fixed_assets
           WHERE client_code = ? AND status = 'active'
           ORDER BY cca_class, asset_name""",
        (client_code,),
    ).fetchall()

    classes: dict[int, dict[str, Any]] = {}

    for row in rows:
        r = dict(row) if not isinstance(row, dict) else row
        cls_key = int(r["cca_class"])
        cls_info = CCA_CLASSES.get(cls_key)
        if cls_info is None:
            continue

        if cls_key not in classes:
            classes[cls_key] = {
                "cca_class": cls_key,
                "cca_class_display": cca_class_display(cls_key),
                "description": cls_info["description"],
                "rate": float(cls_info["rate"]) if cls_info["rate"] is not None else None,
                "opening_ucc": 0.0,
                "additions": 0.0,
                "disposals": 0.0,
                "cca_claimed": 0.0,
                "closing_ucc": 0.0,
                "assets": [],
            }

        entry = classes[cls_key]
        cost = float(_round(_to_decimal(r["cost"])))
        current_ucc = float(_round(_to_decimal(r["current_ucc"])))
        accumulated = float(_round(_to_decimal(r["accumulated_cca"])))

        entry["opening_ucc"] += cost
        entry["cca_claimed"] += accumulated
        entry["closing_ucc"] += current_ucc
        entry["assets"].append({
            "asset_id": r["asset_id"],
            "asset_name": r["asset_name"],
            "acquisition_date": r["acquisition_date"],
            "cost": cost,
            "current_ucc": current_ucc,
            "accumulated_cca": accumulated,
        })

    # Round class totals
    for c in classes.values():
        c["opening_ucc"] = round(c["opening_ucc"], 2)
        c["cca_claimed"] = round(c["cca_claimed"], 2)
        c["closing_ucc"] = round(c["closing_ucc"], 2)

    total_opening = round(sum(c["opening_ucc"] for c in classes.values()), 2)
    total_cca = round(sum(c["cca_claimed"] for c in classes.values()), 2)
    total_closing = round(sum(c["closing_ucc"] for c in classes.values()), 2)

    return {
        "client_code": client_code,
        "fiscal_year": fiscal_year,
        "classes": list(classes.values()),
        "totals": {
            "opening_ucc": total_opening,
            "additions": 0.0,
            "disposals": 0.0,
            "cca_claimed": total_cca,
            "closing_ucc": total_closing,
        },
        "generated_at": _utc_now(),
    }


CAPITAL_GAINS_INCLUSION_RATE = Decimal("0.50")


def process_asset_disposal(
    asset_id: str,
    disposal_date: str,
    proceeds_of_disposition: float | Decimal | str,
    conn: sqlite3.Connection,
    *,
    disposal_reason: str = "sale",
    create_je: bool = False,
) -> dict[str, Any]:
    """Sprint H F1 — full-fidelity asset disposal per CRA T2 Schedule 8 rules.

    Returns the same dict as ``dispose_asset`` plus:
        is_last_in_class      bool — drives terminal-loss eligibility
        taxable_capital_gain  half of any capital gain (50 % inclusion rate)
        ucc_adjustment        when the class still has assets, the lesser-of
                              proceeds-or-cost amount that reduces class UCC
        new_class_ucc         resulting UCC after the adjustment
        disposal_reason       echoed for audit trail
        recapture_taxable     bool — flag for the income-statement adder
        terminal_loss_deductible  bool — flag for the IS deduction
        adjusting_je          dict skeleton if create_je=True

    Negative proceeds are rejected; zero proceeds are valid (write-off).
    """
    proceeds_d = _to_decimal(proceeds_of_disposition)
    if proceeds_d < 0:
        raise ValueError("proceeds_of_disposition cannot be negative")

    base = dispose_asset(asset_id, disposal_date, proceeds_d, conn)

    cap_gain = Decimal(str(base.get("capital_gain") or 0))
    taxable_cap_gain = _round(cap_gain * CAPITAL_GAINS_INCLUSION_RATE)

    # Determine last-in-class. dispose_asset already wrote status='disposed'
    # so any remaining sibling counts the class as non-empty.
    row = conn.execute(
        "SELECT client_code, cca_class FROM fixed_assets WHERE asset_id=?",
        (asset_id,),
    ).fetchone()
    if row is None:
        is_last = True
    else:
        # Re-check, but excluding self because we already flipped status.
        client_code = row["client_code"] if not isinstance(row, dict) else row["client_code"]
        cls_key = int(row["cca_class"] if not isinstance(row, dict) else row["cca_class"])
        sibling = conn.execute(
            "SELECT COUNT(*) AS n FROM fixed_assets "
            "WHERE client_code=? AND cca_class=? AND status='active'",
            (client_code, cls_key),
        ).fetchone()
        n = (dict(sibling) if not isinstance(sibling, dict) else sibling).get("n", 0)
        is_last = n == 0

    cost_d = Decimal(str(base.get("original_cost") or 0))
    ucc_d = Decimal(str(base.get("ucc_at_disposal") or 0))
    effective_proceeds = min(proceeds_d, cost_d)

    ucc_adjustment = _ZERO
    new_class_ucc = ucc_d
    if base.get("recapture", 0) == 0 and base.get("terminal_loss", 0) == 0:
        # Class still has other active assets and proceeds <= UCC.
        # Reduce class UCC by lesser_of(proceeds, cost).
        ucc_adjustment = -effective_proceeds
        new_class_ucc = _round(ucc_d - effective_proceeds)

    # Persist the new computed columns.
    conn.execute(
        """UPDATE fixed_assets
           SET recapture_amount=?, terminal_loss_amount=?,
               capital_gain_amount=?, disposal_reason=?
           WHERE asset_id=?""",
        (
            float(base.get("recapture", 0)),
            float(base.get("terminal_loss", 0)),
            float(cap_gain),
            disposal_reason,
            asset_id,
        ),
    )
    conn.commit()

    adjusting_je = None
    if create_je:
        adjusting_je = _build_disposal_je(asset_id, base, cap_gain, taxable_cap_gain,
                                            disposal_date, disposal_reason)

    return {
        **base,
        "is_last_in_class": is_last,
        "taxable_capital_gain": float(taxable_cap_gain),
        "ucc_adjustment": float(ucc_adjustment),
        "new_class_ucc": float(new_class_ucc),
        "disposal_reason": disposal_reason,
        "recapture_taxable": bool(base.get("recapture", 0) > 0),
        "terminal_loss_deductible": bool(base.get("terminal_loss", 0) > 0),
        "adjusting_je": adjusting_je,
    }


def _build_disposal_je(asset_id, base, cap_gain, taxable_cap_gain,
                        disposal_date, reason):
    """Skeleton journal entry for the disposal — caller is responsible for
    posting it through gl_engine.
    """
    proceeds = base.get("proceeds", 0)
    ucc = base.get("ucc_at_disposal", 0)
    lines = []
    # Cash received
    lines.append({"account": "1010", "debit": proceeds, "credit": 0,
                  "memo": f"Disposal proceeds {asset_id}"})
    # Remove asset cost (credit class register)
    lines.append({"account": "1500", "debit": 0,
                  "credit": base.get("original_cost", 0),
                  "memo": f"Asset disposed {asset_id}"})
    # Accumulated CCA contra (debit)
    lines.append({"account": "1599", "debit": base.get("original_cost", 0) - ucc,
                  "credit": 0, "memo": "Accumulated CCA reversed"})
    # Recapture or terminal loss
    if base.get("recapture", 0) > 0:
        lines.append({"account": "4900", "debit": 0,
                      "credit": base["recapture"],
                      "memo": "CCA recapture (taxable)"})
    if base.get("terminal_loss", 0) > 0:
        lines.append({"account": "5900", "debit": base["terminal_loss"],
                      "credit": 0, "memo": "Terminal loss (deductible)"})
    if cap_gain > 0:
        lines.append({"account": "4910", "debit": 0,
                      "credit": float(taxable_cap_gain),
                      "memo": "Taxable capital gain (50%)"})
    return {
        "date": disposal_date,
        "reason": reason,
        "lines": lines,
        "balanced": _decimal_je_balanced(lines),
    }


def _decimal_je_balanced(lines):
    d = sum(Decimal(str(l.get("debit") or 0)) for l in lines)
    c = sum(Decimal(str(l.get("credit") or 0)) for l in lines)
    return abs(d - c) < Decimal("0.02")


def dispose_asset(
    asset_id: str,
    disposal_date: str,
    proceeds: float | Decimal | str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Dispose of an asset and calculate recapture or terminal loss.

    - If proceeds > UCC: recapture = proceeds - UCC (taxable income)
    - If proceeds < UCC and class is empty: terminal loss = UCC - proceeds (deductible)
    - Capital gain if proceeds > original cost

    Returns dict with recapture, terminal_loss, capital_gain.
    """
    ensure_fixed_assets_table(conn)

    row = conn.execute(
        "SELECT * FROM fixed_assets WHERE asset_id = ?", (asset_id,)
    ).fetchone()

    if row is None:
        raise ValueError(f"Asset not found: {asset_id}")

    r = dict(row) if not isinstance(row, dict) else row

    if r["status"] != "active":
        raise ValueError(f"Asset {asset_id} is not active (status={r['status']})")

    proceeds_d = _to_decimal(proceeds)
    cost_d = _to_decimal(r["cost"])
    ucc_d = _to_decimal(r["current_ucc"])
    client_code = r["client_code"]
    cls_key = int(r["cca_class"])

    recapture = _ZERO
    terminal_loss = _ZERO
    capital_gain = _ZERO

    # Capital gain: if proceeds exceed original cost
    if proceeds_d > cost_d:
        capital_gain = _round(proceeds_d - cost_d)
        # For CCA purposes, limit proceeds to cost
        effective_proceeds = cost_d
    else:
        effective_proceeds = proceeds_d

    # Check if this is the last active asset in the class for this client
    other_active = conn.execute(
        """SELECT COUNT(*) as cnt FROM fixed_assets
           WHERE client_code = ? AND cca_class = ? AND status = 'active'
           AND asset_id != ?""",
        (client_code, cls_key, asset_id),
    ).fetchone()
    other_count = (dict(other_active) if not isinstance(other_active, dict) else other_active).get("cnt", 0)
    class_empty_after = other_count == 0

    if effective_proceeds > ucc_d:
        # Recapture: taxable income
        recapture = _round(effective_proceeds - ucc_d)
    elif effective_proceeds < ucc_d and class_empty_after:
        # Terminal loss: deductible
        terminal_loss = _round(ucc_d - effective_proceeds)

    # Update asset status
    conn.execute(
        """UPDATE fixed_assets
           SET status = 'disposed', disposal_date = ?, disposal_proceeds = ?,
               current_ucc = 0
           WHERE asset_id = ?""",
        (disposal_date, float(_round(proceeds_d)), asset_id),
    )
    conn.commit()

    return {
        "asset_id": asset_id,
        "disposal_date": disposal_date,
        "proceeds": float(_round(proceeds_d)),
        "original_cost": float(_round(cost_d)),
        "ucc_at_disposal": float(_round(ucc_d)),
        "recapture": float(_round(recapture)),
        "terminal_loss": float(_round(terminal_loss)),
        "capital_gain": float(_round(capital_gain)),
    }


def list_assets(
    client_code: str,
    conn: sqlite3.Connection,
    status: str | None = None,
) -> list[dict[str, Any]]:
    """List all assets for a client, optionally filtered by status."""
    ensure_fixed_assets_table(conn)

    if status:
        rows = conn.execute(
            """SELECT * FROM fixed_assets
               WHERE client_code = ? AND status = ?
               ORDER BY cca_class, asset_name""",
            (client_code, status),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT * FROM fixed_assets
               WHERE client_code = ?
               ORDER BY cca_class, asset_name""",
            (client_code,),
        ).fetchall()

    return [dict(r) if not isinstance(r, dict) else r for r in rows]


def create_draft_asset_from_capex(
    client_code: str,
    asset_name: str,
    cost: float | Decimal | str,
    document_date: str,
    conn: sqlite3.Connection,
) -> str | None:
    """Create a draft asset record when substance engine detects CapEx.

    Returns asset_id or None if cost is not positive.
    The asset is created with status='draft' requiring accountant confirmation.
    """
    ensure_fixed_assets_table(conn)

    cost_d = _to_decimal(cost)
    if cost_d <= _ZERO:
        return None

    _hash_input = f"DRAFT|{client_code}|{asset_name}|{float(cost_d)}|{document_date}"
    asset_id = "FA-DRAFT-" + hashlib.sha256(_hash_input.encode()).hexdigest()[:12].upper()

    # Default to class 8 (miscellaneous) — accountant will reclassify
    conn.execute(
        """INSERT INTO fixed_assets
           (asset_id, client_code, asset_name, cca_class, acquisition_date,
            cost, opening_ucc, current_ucc, accumulated_cca, status, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            asset_id,
            client_code,
            asset_name,
            8,
            document_date,
            float(_round(cost_d)),
            float(_round(cost_d)),
            float(_round(cost_d)),
            0.0,
            "draft",
            _utc_now(),
        ),
    )
    conn.commit()
    return asset_id
