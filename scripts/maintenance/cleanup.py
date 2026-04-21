#!/usr/bin/env python3
"""Daily housekeeping cron.

Prunes old rows from tables that accumulate forever otherwise:

    wizard_posting_attempts      90 days
    rate_limit_events            1 hour     (Item 4 limiter)
    client_notifications (sent)  180 days
    client_notifications (failed) 30 days
    impersonation_audit          365 days   (compliance retention)
    client_portal_user_audit     365 days
    accrual_line_overrides       730 days

Each table prune is independent and idempotent — a missing table
(old DB) logs a warning and moves on. Summary line written to
/var/log/otocpa/maintenance.log.

Runs as cron every day at 03:00:

    0 3 * * * deploy cd /opt/otocpa && .venv/bin/python3 scripts/maintenance/cleanup.py >> /var/log/otocpa/maintenance.log 2>&1
"""
from __future__ import annotations

import logging
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "data" / "otocpa_agent.db"

log = logging.getLogger("otocpa.maintenance")


# Retention schedule. Tuple: (table, column_with_timestamp, days,
#                              optional_extra_where_clause).
RETENTION_SCHEDULE: list[tuple[str, str, int, str]] = [
    ('wizard_posting_attempts', 'started_at', 90, ''),
    ('rate_limit_events', 'created_at', 0, ''),   # 1 hour via hours_retention below
    ('client_notifications', 'sent_at', 180,
     "AND status='sent'"),
    ('client_notifications', 'created_at', 30,
     "AND status='failed'"),
    ('impersonation_audit', 'at', 365, ''),
    ('client_portal_user_audit', 'created_at', 365, ''),
    ('accrual_line_overrides', 'created_at', 730, ''),
]

# Hour-level overrides for high-churn tables where days is too coarse.
HOURLY_RETENTION: list[tuple[str, str, int, str]] = [
    ('rate_limit_events', 'created_at', 1, ''),
]


def _iso_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc)
            - timedelta(days=days)).replace(microsecond=0).isoformat()


def _iso_hours_ago(hours: int) -> str:
    return (datetime.now(timezone.utc)
            - timedelta(hours=hours)).replace(microsecond=0).isoformat()


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def prune_table(
    conn: sqlite3.Connection, *,
    table: str, ts_column: str, cutoff_iso: str,
    extra_where: str = '',
) -> int:
    """DELETE rows older than cutoff. Returns rows-deleted count.

    `extra_where` is concatenated verbatim after the timestamp check
    (must start with 'AND '). Missing tables silently return 0 so a
    pre-migration DB doesn't fail the whole cron run."""
    if not _table_exists(conn, table):
        log.warning("skip %s: table missing", table)
        return 0
    sql = (f"DELETE FROM {table} "
            f"WHERE datetime({ts_column}) < datetime(?) {extra_where}")
    try:
        cur = conn.execute(sql, (cutoff_iso,))
        return cur.rowcount or 0
    except sqlite3.OperationalError as exc:
        log.warning("prune %s failed: %s", table, exc)
        return 0


def run_cleanup(db_path: Path | str = DB_PATH) -> dict[str, int]:
    """Execute every retention rule. Returns {table: rows_deleted}."""
    results: dict[str, int] = {}
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        # Daily rules — skip rate_limit_events here; it has its own
        # hourly entry.
        for table, col, days, extra in RETENTION_SCHEDULE:
            if table == 'rate_limit_events':
                continue
            cutoff = _iso_days_ago(days)
            deleted = prune_table(
                conn, table=table, ts_column=col,
                cutoff_iso=cutoff, extra_where=extra,
            )
            key = (f"{table}:{extra.strip()}"
                   if extra else f"{table}:{days}d")
            results[key] = deleted
        # Hourly rules
        for table, col, hours, extra in HOURLY_RETENTION:
            cutoff = _iso_hours_ago(hours)
            deleted = prune_table(
                conn, table=table, ts_column=col,
                cutoff_iso=cutoff, extra_where=extra,
            )
            results[f"{table}:{hours}h"] = deleted
        conn.commit()
    return results


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    if not Path(DB_PATH).exists():
        print(f"[maintenance] DB not found at {DB_PATH}; skipping.")
        return 0
    results = run_cleanup(DB_PATH)
    total = sum(results.values())
    summary = ", ".join(f"{k}={v}" for k, v in sorted(results.items()))
    print(f"[maintenance] deleted={total} ({summary})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
