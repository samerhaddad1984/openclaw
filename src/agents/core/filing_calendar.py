"""
src/agents/core/filing_calendar.py — GST/QST filing deadline calendar.

Computes upcoming filing deadlines for each client based on their filing
frequency (monthly, quarterly, annual) and fiscal year end.  No AI calls.

Deadline rules (CRA / Revenu Québec):
  Monthly   : last day of the calendar month following the period end
  Quarterly : last day of the calendar month following the quarter end
              (Q1 Jan-Mar → Apr 30, Q2 Apr-Jun → Jul 31, Q3 → Oct 31, Q4 → Jan 31)
  Annual    : last day of the calendar month 3 months after fiscal year end

fiscal_year_end is stored as "MM-DD" (e.g., "12-31" for December 31).
"""
from __future__ import annotations

import calendar as _calendar
import sqlite3
from datetime import date, timedelta
from typing import Any


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def ensure_filing_tables(conn: sqlite3.Connection) -> None:
    """Create the gst_filings table if it does not exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gst_filings (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code  TEXT NOT NULL,
            period_label TEXT NOT NULL,
            deadline     TEXT NOT NULL,
            filed_at     TEXT,
            filed_by     TEXT,
            UNIQUE(client_code, period_label)
        )
    """)
    conn.commit()


def mark_as_filed(
    conn: sqlite3.Connection,
    client_code: str,
    period_label: str,
    deadline: str,
    filed_by: str,
    filed_at: str,
) -> None:
    """Record (or update) a filing for client_code / period_label."""
    ensure_filing_tables(conn)
    conn.execute(
        """
        INSERT INTO gst_filings (client_code, period_label, deadline, filed_at, filed_by)
             VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(client_code, period_label) DO UPDATE SET
            filed_at = excluded.filed_at,
            filed_by = excluded.filed_by,
            deadline = excluded.deadline
        """,
        (client_code, period_label, deadline, filed_at, filed_by),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Date arithmetic helpers
# ---------------------------------------------------------------------------

def _last_day_of_month(year: int, month: int) -> date:
    return date(year, month, _calendar.monthrange(year, month)[1])


# ---------------------------------------------------------------------------
# Deadline calculation per frequency
# ---------------------------------------------------------------------------

def _monthly_deadline(period_year: int, period_month: int) -> date:
    """Last day of the month following the monthly period."""
    if period_month == 12:
        return _last_day_of_month(period_year + 1, 1)
    return _last_day_of_month(period_year, period_month + 1)


def _quarterly_deadline(year: int, quarter: int) -> date:
    """Last day of the month following the quarter end."""
    # Quarter ends: Q1=Mar, Q2=Jun, Q3=Sep, Q4=Dec
    qe_months = {1: 3, 2: 6, 3: 9, 4: 12}
    qe_month = qe_months[quarter]
    if qe_month == 12:
        return _last_day_of_month(year + 1, 1)
    return _last_day_of_month(year, qe_month + 1)


def _annual_deadline(fy_month: int, fy_end_year: int) -> date:
    """Last day of the month 3 calendar months after the fiscal year end month."""
    dl_month = fy_month + 3
    dl_year  = fy_end_year
    if dl_month > 12:
        dl_month -= 12
        dl_year  += 1
    return _last_day_of_month(dl_year, dl_month)


# ---------------------------------------------------------------------------
# Convert period_label → (period_start, period_end) ISO strings
# ---------------------------------------------------------------------------

def period_label_to_dates(
    period_label: str,
    fiscal_year_end: str = "12-31",
) -> tuple[str, str]:
    """
    Return (period_start, period_end) as ISO date strings for the given period_label.

    Examples
    --------
    "2026-02"       → ("2026-02-01", "2026-02-28")
    "2026-Q1"       → ("2026-01-01", "2026-03-31")
    "FY2025"        → ("2025-01-01", "2025-12-31")  (fiscal_year_end="12-31")
    """
    label = str(period_label).strip()

    # Monthly: "YYYY-MM"
    if len(label) == 7 and label[4] == "-" and not label.startswith("FY"):
        try:
            y = int(label[:4])
            m = int(label[5:])
            start = date(y, m, 1).isoformat()
            end   = _last_day_of_month(y, m).isoformat()
            return start, end
        except ValueError:
            pass

    # Quarterly: "YYYY-Qn"
    if len(label) == 7 and "-Q" in label:
        try:
            parts = label.split("-Q")
            y = int(parts[0])
            q = int(parts[1])
            q_start_months = {1: 1, 2: 4, 3: 7, 4: 10}
            q_end_months   = {1: 3, 2: 6, 3: 9, 4: 12}
            sm = q_start_months[q]
            em = q_end_months[q]
            start = date(y, sm, 1).isoformat()
            end   = _last_day_of_month(y, em).isoformat()
            return start, end
        except (ValueError, KeyError):
            pass

    # Annual: "FYnnnn"
    if label.startswith("FY"):
        try:
            y = int(label[2:])
            parts = fiscal_year_end.split("-")
            fy_month = int(parts[0]) if parts else 12
            fy_day   = int(parts[1]) if len(parts) > 1 else 31
            try:
                fy_end = date(y, fy_month, fy_day)
            except ValueError:
                fy_end = _last_day_of_month(y, fy_month)
            # Fiscal year start = one year before FY end + 1 day
            fy_start = date(y - 1, fy_month, fy_day) + timedelta(days=1)
            return fy_start.isoformat(), fy_end.isoformat()
        except (ValueError, IndexError):
            pass

    # Fallback: use today's month
    today = date.today()
    return date(today.year, today.month, 1).isoformat(), today.isoformat()


# ---------------------------------------------------------------------------
# Candidate period generation
# ---------------------------------------------------------------------------

def _generate_monthly_periods(as_of: date, window_end: date) -> list[tuple[str, date]]:
    results: list[tuple[str, date]] = []
    # Start from 2 months before as_of so recently-passed deadlines appear if unfiled
    py, pm = as_of.year, as_of.month
    start_month = pm - 2
    start_year  = py
    while start_month < 1:
        start_month += 12
        start_year  -= 1
    cy, cm = start_year, start_month
    for _ in range(18):   # at most 18 months
        deadline = _monthly_deadline(cy, cm)
        label    = f"{cy}-{cm:02d}"
        if deadline > window_end:
            break
        if deadline >= as_of:
            results.append((label, deadline))
        # advance one month
        if cm == 12:
            cy, cm = cy + 1, 1
        else:
            cm += 1
    return results


def _generate_quarterly_periods(as_of: date, window_end: date) -> list[tuple[str, date]]:
    results: list[tuple[str, date]] = []
    for year in range(as_of.year - 1, as_of.year + 3):
        for q in range(1, 5):
            deadline = _quarterly_deadline(year, q)
            label    = f"{year}-Q{q}"
            if deadline < as_of:
                continue
            if deadline > window_end:
                return results
            results.append((label, deadline))
    return results


def _generate_annual_periods(
    as_of: date,
    window_end: date,
    fy_month: int,
) -> list[tuple[str, date]]:
    results: list[tuple[str, date]] = []
    for year in range(as_of.year - 3, as_of.year + 4):
        deadline = _annual_deadline(fy_month, year)
        label    = f"FY{year}"
        if deadline < as_of:
            continue
        if deadline > window_end:
            break
        results.append((label, deadline))
    return results


# ---------------------------------------------------------------------------
# Main public function
# ---------------------------------------------------------------------------

def get_upcoming_deadlines(
    conn: sqlite3.Connection,
    as_of: date | None = None,
    days_ahead: int = 90,
) -> list[dict[str, Any]]:
    """
    Return all client filing deadlines whose deadline falls within
    [as_of, as_of + days_ahead].

    Each dict contains:
        client_code, period_label, deadline (ISO str), is_filed,
        filed_at, filed_by, days_until (int), frequency,
        gst_reg, qst_reg, fiscal_year_end
    """
    if as_of is None:
        as_of = date.today()
    window_end = as_of + timedelta(days=days_ahead)

    ensure_filing_tables(conn)

    # Load client configs — columns added by migrate_db.py
    try:
        rows = conn.execute("""
            SELECT
                client_code,
                COALESCE(filing_frequency,        'monthly') AS filing_frequency,
                COALESCE(fiscal_year_end,         '12-31')   AS fiscal_year_end,
                COALESCE(gst_registration_number, '')         AS gst_reg,
                COALESCE(qst_registration_number, '')         AS qst_reg
            FROM client_config
        """).fetchall()
    except sqlite3.OperationalError:
        return []

    # Load existing filings for fast lookup
    filings: dict[tuple[str, str], dict[str, Any]] = {}
    try:
        for frow in conn.execute(
            "SELECT client_code, period_label, filed_at, filed_by FROM gst_filings"
        ):
            filings[(frow[0], frow[1])] = {
                "filed_at": frow[2],
                "filed_by": frow[3],
            }
    except sqlite3.OperationalError:
        pass

    results: list[dict[str, Any]] = []

    for row in rows:
        cc        = row[0]
        freq      = (row[1] or "monthly").lower()
        fy_end    = row[2] or "12-31"
        gst_reg   = row[3] or ""
        qst_reg   = row[4] or ""

        # Parse fiscal year end
        try:
            fy_parts = fy_end.split("-")
            fy_month = int(fy_parts[0])
        except (ValueError, IndexError):
            fy_month = 12

        # Generate candidate periods
        if freq == "quarterly":
            periods = _generate_quarterly_periods(as_of, window_end)
        elif freq == "annual":
            periods = _generate_annual_periods(as_of, window_end, fy_month)
        else:
            periods = _generate_monthly_periods(as_of, window_end)

        for period_label, deadline in periods:
            filing = filings.get((cc, period_label))
            results.append({
                "client_code":    cc,
                "period_label":   period_label,
                "deadline":       deadline.isoformat(),
                "is_filed":       bool(filing and filing.get("filed_at")),
                "filed_at":       filing["filed_at"] if filing else None,
                "filed_by":       filing["filed_by"] if filing else None,
                "days_until":     (deadline - as_of).days,
                "frequency":      freq,
                "gst_reg":        gst_reg,
                "qst_reg":        qst_reg,
                "fiscal_year_end": fy_end,
            })

    results.sort(key=lambda x: (x["deadline"], x["client_code"]))
    return results


def get_filing_deadlines(
    client_code: str,
    frequency: str = "monthly",
    year: int | None = None,
) -> list[dict[str, Any]]:
    """Return filing deadlines for a given client, frequency, and year.

    This is a standalone helper that doesn't require a DB connection —
    it computes deadlines purely from the frequency and year.
    """
    if year is None:
        year = date.today().year

    freq = frequency.strip().lower()
    results: list[dict[str, Any]] = []

    if freq == "monthly":
        for month in range(1, 13):
            period_label = f"{year}-{month:02d}"
            deadline = _monthly_deadline(year, month)
            results.append({
                "client_code": client_code,
                "period_label": period_label,
                "deadline": deadline.isoformat(),
                "frequency": freq,
            })
    elif freq == "quarterly":
        for q in range(1, 5):
            period_label = f"{year}-Q{q}"
            deadline = _quarterly_deadline(year, q)
            results.append({
                "client_code": client_code,
                "period_label": period_label,
                "deadline": deadline.isoformat(),
                "frequency": freq,
            })
    elif freq == "annual":
        deadline = _annual_deadline(12, year)
        results.append({
            "client_code": client_code,
            "period_label": str(year),
            "deadline": deadline.isoformat(),
            "frequency": freq,
        })

    return results


def get_next_deadline(
    conn: sqlite3.Connection | None = None,
    client_code: str | None = None,
    as_of: date | None = None,
    **kwargs: Any,
) -> dict[str, Any] | None:
    """Return the single next filing deadline (optionally filtered by client)."""
    if conn is None:
        return None
    deadlines = get_upcoming_deadlines(conn, as_of=as_of, days_ahead=365)
    if client_code:
        deadlines = [d for d in deadlines if d["client_code"] == client_code]
    unfiled = [d for d in deadlines if not d["is_filed"]]
    return unfiled[0] if unfiled else (deadlines[0] if deadlines else None)
