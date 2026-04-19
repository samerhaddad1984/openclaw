"""
src/engines/benford_engine.py — Sprint G Feature 4.

Benford's-Law first-digit test plus a round-dollar-spike detector.

Both are classic CAS 240 (fraud-risk) analytical procedures. We expose
them as standalone functions so they can be invoked from
/audit/analytical, the chaos runner, and a daily cron.

References:
  - AICPA Audit Sampling Guide, Appendix on digital analysis.
  - Nigrini, "Forensic Analytics", chapter on Benford's Law.
"""
from __future__ import annotations

import logging
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from math import isfinite
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

HIGH = "high"
MEDIUM = "medium"
LOW = "low"

# Benford's expected first-digit frequencies (1..9), sums to 1.0.
BENFORD_EXPECTED: dict[int, float] = {
    1: 0.30103, 2: 0.17609, 3: 0.12494,
    4: 0.09691, 5: 0.07918, 6: 0.06695,
    7: 0.05799, 8: 0.05115, 9: 0.04576,
}

# Chi-squared critical values at 8 degrees of freedom (digits 1..9 minus 1).
CHI2_8DF_05 = 15.507   # significant @ p < 0.05
CHI2_8DF_01 = 20.090   # very significant @ p < 0.01

# Minimum sample size for a meaningful Benford test. AICPA guidance is 100;
# we use 50 as a soft floor and refuse below 30.
BENFORD_MIN_SAMPLE = 50
BENFORD_HARD_FLOOR = 30


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Pure helpers — no DB access
# ---------------------------------------------------------------------------

def first_digit(amount: float | Decimal | str) -> int | None:
    """Return the first significant digit (1..9) of a positive amount.

    Negative or zero amounts return None and should be excluded by the caller.
    """
    try:
        n = abs(float(amount))
    except (TypeError, ValueError):
        return None
    if n <= 0 or not isfinite(n):
        return None
    # Strip leading zeros from the decimal representation.
    s = f"{n:.10g}".lstrip("0.").lstrip(".")
    for ch in s:
        if ch.isdigit() and ch != "0":
            return int(ch)
    return None


def count_first_digits(amounts: Iterable[float]) -> dict[int, int]:
    """Return a {digit: count} dict for digits 1..9; missing digits = 0."""
    out: dict[int, int] = {d: 0 for d in range(1, 10)}
    for a in amounts:
        d = first_digit(a)
        if d is not None:
            out[d] += 1
    return out


def chi_squared_benford(observed: dict[int, int]) -> float:
    """Pearson chi-squared statistic comparing observed to Benford."""
    total = sum(observed.values())
    if total <= 0:
        return 0.0
    chi2 = 0.0
    for d in range(1, 10):
        obs = observed.get(d, 0)
        exp = BENFORD_EXPECTED[d] * total
        if exp > 0:
            chi2 += (obs - exp) ** 2 / exp
    return chi2


def analyze_benford_compliance(
    amounts: Iterable[float] | None = None,
    *,
    firm_code: str = "",
    client_code: str = "",
    days_back: int = 365,
    min_amount: float = 1.0,
    db_path: Path = DB_PATH,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    """Apply Benford to a sequence of amounts (preferred) or pull from DB.

    Returns a dict with the chi-squared statistic, p-value bucket,
    severity, and observed-vs-expected counts.
    """
    if amounts is None:
        amounts = _fetch_amounts(
            firm_code=firm_code, client_code=client_code,
            days_back=days_back, min_amount=min_amount,
            db_path=db_path, conn=conn,
        )
    amounts = [a for a in amounts if a is not None]
    if len(amounts) < BENFORD_HARD_FLOOR:
        return {
            "status": "insufficient_data",
            "min_required": BENFORD_HARD_FLOOR,
            "sample_size": len(amounts),
        }

    observed = count_first_digits(amounts)
    total = sum(observed.values())
    chi2 = chi_squared_benford(observed)
    if chi2 > CHI2_8DF_01:
        severity = HIGH
    elif chi2 > CHI2_8DF_05:
        severity = MEDIUM
    else:
        severity = LOW

    return {
        "status": "ok" if total >= BENFORD_MIN_SAMPLE else "low_sample",
        "sample_size": total,
        "observed_distribution": observed,
        "expected_counts": {d: round(BENFORD_EXPECTED[d] * total, 2)
                             for d in range(1, 10)},
        "chi_squared": round(chi2, 3),
        "chi2_critical_05": CHI2_8DF_05,
        "chi2_critical_01": CHI2_8DF_01,
        "significant_deviation": chi2 > CHI2_8DF_05,
        "severity": severity,
        "i18n_key": "fraud_benford_deviation",
        "detected_at": _utc_now(),
    }


def detect_round_dollar_spike(
    amounts: Iterable[float] | None = None,
    *,
    firm_code: str = "",
    client_code: str = "",
    days_back: int = 365,
    threshold_pct: float = 0.30,
    min_sample: int = 30,
    min_amount: float = 1.0,
    db_path: Path = DB_PATH,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    """Flag if more than threshold_pct of amounts are exact round dollars.

    'Round' = no cents (e.g., 1000.00, 250.00, 25.00). Honest invoicing
    rarely produces > 30 % round amounts.
    """
    if amounts is None:
        amounts = _fetch_amounts(
            firm_code=firm_code, client_code=client_code,
            days_back=days_back, min_amount=min_amount,
            db_path=db_path, conn=conn,
        )
    amounts = [a for a in amounts if a is not None]
    if len(amounts) < min_sample:
        return {
            "status": "insufficient_data",
            "min_required": min_sample,
            "sample_size": len(amounts),
        }
    round_count = 0
    for a in amounts:
        try:
            f = float(a)
        except (TypeError, ValueError):
            continue
        if f <= 0:
            continue
        if abs(f - round(f)) < 0.005:
            round_count += 1
    pct = round_count / len(amounts)
    severity = LOW
    if pct >= threshold_pct + 0.20:
        severity = HIGH
    elif pct >= threshold_pct:
        severity = MEDIUM
    return {
        "status": "ok",
        "sample_size": len(amounts),
        "round_dollar_count": round_count,
        "round_dollar_pct": round(pct, 4),
        "threshold_pct": threshold_pct,
        "significant": pct >= threshold_pct,
        "severity": severity,
        "i18n_key": "fraud_round_dollar_spike",
        "detected_at": _utc_now(),
    }


# ---------------------------------------------------------------------------
# DB fetch helper
# ---------------------------------------------------------------------------

def _fetch_amounts(
    *,
    firm_code: str,
    client_code: str,
    days_back: int,
    min_amount: float,
    db_path: Path,
    conn: sqlite3.Connection | None = None,
) -> list[float]:
    own_conn = False
    if conn is None:
        if not db_path.exists():
            return []
        conn = sqlite3.connect(str(db_path))
        own_conn = True
    try:
        params: list[Any] = []
        cols = {r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
        if "amount" not in cols:
            return []
        where = ["d.amount IS NOT NULL", "d.amount >= ?"]
        params.append(min_amount)
        if firm_code and "firm_code" in cols:
            where.append("LOWER(COALESCE(d.firm_code,'')) = LOWER(?)")
            params.append(firm_code)
        if client_code:
            where.append("LOWER(COALESCE(d.client_code,'')) = LOWER(?)")
            params.append(client_code)
        where.append(
            "COALESCE(d.created_at, d.document_date, '1970-01-01') >= "
            "datetime('now', '-' || ? || ' days')"
        )
        params.append(int(days_back))
        if "review_status" in cols:
            where.append("LOWER(COALESCE(d.review_status,'')) != 'ignored'")
        sql = "SELECT d.amount FROM documents d WHERE " + " AND ".join(where)
        try:
            rows = conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError:
            return []
        return [float(r[0]) for r in rows if r[0] is not None]
    finally:
        if own_conn:
            conn.close()
