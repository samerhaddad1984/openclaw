"""
src/agents/core/time_tracker.py — Billable time tracking for OtoCPA.

Provides:
  - Table creation (time_entries, invoices)
  - start_time_entry / stop_time_entry
  - Time summary queries (per client, per period)
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_time_tables(conn: sqlite3.Connection) -> None:
    """Create time_entries and invoices tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS time_entries (
            entry_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            username         TEXT NOT NULL,
            client_code      TEXT NOT NULL,
            document_id      TEXT,
            started_at       TEXT NOT NULL,
            ended_at         TEXT,
            duration_minutes REAL,
            description      TEXT,
            billable         INTEGER NOT NULL DEFAULT 1,
            hourly_rate      REAL
        );
        CREATE TABLE IF NOT EXISTS invoices (
            invoice_id    TEXT PRIMARY KEY,
            client_code   TEXT NOT NULL,
            period_start  TEXT NOT NULL,
            period_end    TEXT NOT NULL,
            generated_by  TEXT NOT NULL,
            generated_at  TEXT NOT NULL,
            hourly_rate   REAL NOT NULL,
            subtotal      REAL NOT NULL,
            gst_amount    REAL NOT NULL,
            qst_amount    REAL NOT NULL,
            total_amount  REAL NOT NULL,
            entry_count   INTEGER NOT NULL DEFAULT 0
        );
    """)
    conn.commit()


def start_time_entry(
    conn: sqlite3.Connection,
    username: str,
    client_code: str,
    document_id: str | None = None,
) -> int:
    """Insert a new open time entry and return its entry_id."""
    ensure_time_tables(conn)
    cur = conn.execute(
        """
        INSERT INTO time_entries (username, client_code, document_id, started_at, billable)
        VALUES (?, ?, ?, ?, 1)
        """,
        (username, client_code, document_id or None, _utc_now_iso()),
    )
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def stop_time_entry(
    conn: sqlite3.Connection,
    entry_id: int,
    duration_minutes: float,
    description: str = "",
) -> None:
    """Record the end time and duration for an open time entry."""
    conn.execute(
        """
        UPDATE time_entries
           SET ended_at         = ?,
               duration_minutes = ?,
               description      = COALESCE(NULLIF(?, ''), description)
         WHERE entry_id = ?
        """,
        (_utc_now_iso(), max(0.0, float(duration_minutes)), description, entry_id),
    )
    conn.commit()


def get_entries_for_period(
    conn: sqlite3.Connection,
    client_code: str,
    period_start: str,
    period_end: str,
) -> list[dict[str, Any]]:
    """Return all completed time entries for a client in [period_start, period_end]."""
    ensure_time_tables(conn)
    rows = conn.execute(
        """
        SELECT * FROM time_entries
         WHERE LOWER(client_code) = LOWER(?)
           AND started_at >= ?
           AND started_at <= ?
           AND duration_minutes IS NOT NULL
         ORDER BY started_at
        """,
        (client_code.strip(), period_start, period_end + "T23:59:59"),
    ).fetchall()
    return [dict(r) for r in rows]


def get_time_summary(
    conn: sqlite3.Connection,
    client_code: str,
    period_start: str,
    period_end: str,
) -> dict[str, Any]:
    """Return aggregated time stats for a client over a period."""
    entries = get_entries_for_period(conn, client_code, period_start, period_end)
    total_minutes = sum(e["duration_minutes"] or 0.0 for e in entries)
    billable_minutes = sum(
        e["duration_minutes"] or 0.0 for e in entries if e["billable"]
    )

    by_user: dict[str, dict[str, float]] = {}
    for e in entries:
        u = e["username"]
        if u not in by_user:
            by_user[u] = {"total_minutes": 0.0, "billable_minutes": 0.0}
        by_user[u]["total_minutes"] += e["duration_minutes"] or 0.0
        if e["billable"]:
            by_user[u]["billable_minutes"] += e["duration_minutes"] or 0.0

    return {
        "client_code": client_code,
        "period_start": period_start,
        "period_end": period_end,
        "entry_count": len(entries),
        "total_minutes": total_minutes,
        "total_hours": round(total_minutes / 60, 2),
        "billable_minutes": billable_minutes,
        "billable_hours": round(billable_minutes / 60, 2),
        "by_user": by_user,
        "entries": entries,
    }
