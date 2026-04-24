"""Client inactivity detection.

Dormant clients (no activity in 90 days) need a CPA's attention —
either a nudge to prompt the client, or an archive decision. This
module scans the firm's clients weekly and surfaces the at-risk
ones.

Sources of "activity" (any one resets the clock):
  - documents inserted for the client (``documents.created_at``)
  - portal uploads (covered by documents)
  - messages to/from the client (``client_messages.created_at``)
  - completed CPA requests (``client_requests.completed_at``)

Output:
  - ``inactive_clients(days=90)`` — list of clients over the
    threshold, with their last_activity_at and days_inactive.
  - ``at_risk_summary(firm_code)`` — compact widget payload for the
    admin dashboard.

Note: archived clients are excluded — they have their own retention
mechanism (Scope 3.1). Clients with zero activity ever are included
with ``days_inactive`` measured from ``created_at``.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


DEFAULT_INACTIVITY_DAYS = 90


def _open(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _now(now: datetime | None = None) -> datetime:
    return now or datetime.now(timezone.utc)


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone() is not None


def last_activity_for(
    db_path: Path | str, client_code: str,
    now: datetime | None = None,
) -> datetime | None:
    """Return the most recent activity timestamp for the client, or
    None if the client has no recorded activity.
    """
    with _open(db_path) as conn:
        stamps: list[str] = []
        if _table_exists(conn, 'documents'):
            r = conn.execute(
                "SELECT MAX(created_at) AS t FROM documents "
                "WHERE client_code=?", (client_code,),
            ).fetchone()
            if r and r['t']:
                stamps.append(str(r['t']))
        if _table_exists(conn, 'client_messages'):
            r = conn.execute(
                "SELECT MAX(created_at) AS t FROM client_messages "
                "WHERE client_code=?", (client_code,),
            ).fetchone()
            if r and r['t']:
                stamps.append(str(r['t']))
        if _table_exists(conn, 'client_requests'):
            r = conn.execute(
                "SELECT MAX(COALESCE(completed_at, created_at)) AS t "
                "FROM client_requests WHERE client_code=?",
                (client_code,),
            ).fetchone()
            if r and r['t']:
                stamps.append(str(r['t']))
    if not stamps:
        return None
    best = max(stamps)
    # Tolerate both "2026-04-24T..." and "2026-04-24 ..." stamps.
    best = best.replace(' ', 'T')
    try:
        if best.endswith('Z'):
            best = best.replace('Z', '+00:00')
        dt = datetime.fromisoformat(best)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def inactive_clients(
    db_path: Path | str, *, firm_code: str | None = None,
    days: int = DEFAULT_INACTIVITY_DAYS,
    now: datetime | None = None,
) -> list[dict]:
    """Clients whose last activity was more than ``days`` ago (or
    never). Archived clients are excluded.
    """
    anchor = _now(now)
    cutoff = anchor - timedelta(days=days)
    with _open(db_path) as conn:
        sql = (
            "SELECT client_code, client_name, firm_code, created_at "
            "FROM clients "
            "WHERE COALESCE(status,'active')='active' "
        )
        params: list[Any] = []
        if firm_code is not None:
            sql += "AND firm_code=? "
            params.append(firm_code)
        sql += "ORDER BY client_code"
        rows = conn.execute(sql, params).fetchall()
    out: list[dict] = []
    for r in rows:
        last = last_activity_for(db_path, r['client_code'], now=anchor)
        effective_dt = last
        if effective_dt is None:
            # Fall back to created_at on the client row.
            ca = r['created_at']
            if ca:
                try:
                    effective_dt = datetime.fromisoformat(
                        str(ca).replace(' ', 'T').replace('Z', '+00:00')
                    )
                    if effective_dt.tzinfo is None:
                        effective_dt = effective_dt.replace(
                            tzinfo=timezone.utc,
                        )
                except ValueError:
                    effective_dt = None
        if effective_dt is None:
            days_inactive = None
        else:
            days_inactive = (anchor - effective_dt).days
        if (days_inactive is None
                or (effective_dt is not None
                    and effective_dt <= cutoff)):
            out.append({
                'client_code': r['client_code'],
                'client_name': r['client_name'],
                'firm_code': r['firm_code'],
                'last_activity_at': (effective_dt.isoformat()
                                     if effective_dt else None),
                'days_inactive': days_inactive,
            })
    return out


def at_risk_summary(
    db_path: Path | str, firm_code: str,
    days: int = DEFAULT_INACTIVITY_DAYS,
    now: datetime | None = None,
) -> dict:
    inactive = inactive_clients(
        db_path, firm_code=firm_code, days=days, now=now,
    )
    never = [c for c in inactive if c['days_inactive'] is None]
    over = [c for c in inactive if c['days_inactive'] is not None]
    return {
        'days': days,
        'total': len(inactive),
        'never_active_count': len(never),
        'inactive_over_threshold': over,
        'at_risk_client_codes': [c['client_code'] for c in inactive],
    }


# ---------------------------------------------------------------------------
# Cron entry point
# ---------------------------------------------------------------------------


def weekly_scan(
    db_path: Path | str, *, firm_code: str | None = None,
    days: int = DEFAULT_INACTIVITY_DAYS,
    notifier: Any = None,
    now: datetime | None = None,
) -> dict:
    """Scan every firm (or a specific firm) and alert the owner /
    firm_admin when at-risk clients show up.

    ``notifier`` is a callable ``fn(firm_code, payload)``; the actual
    email dispatch lives in ``notification_sender`` so this module
    stays test-friendly.
    """
    anchor = _now(now)
    if firm_code is None:
        with _open(db_path) as conn:
            firm_rows = conn.execute(
                "SELECT DISTINCT firm_code FROM clients "
                "WHERE COALESCE(status,'active')='active'"
            ).fetchall()
        firms = [r['firm_code'] for r in firm_rows if r['firm_code']]
    else:
        firms = [firm_code]
    reports = {}
    for fc in firms:
        summary = at_risk_summary(db_path, fc, days=days, now=anchor)
        reports[fc] = summary
        if summary['total'] > 0 and notifier is not None:
            try:
                notifier(firm_code=fc, summary=summary)
            except Exception:
                log.exception("notifier failed for %s", fc)
    return reports
