"""Optimistic-concurrency helpers for document-like tables.

Usage pattern:

    # Reader:
    row = conn.execute("SELECT document_id, version, ... FROM documents ...").fetchone()

    # Writer (must supply the version it read):
    try:
        version_check_update(
            conn, table="documents",
            pk_column="document_id", pk_value=doc_id,
            expected_version=row["version"],
            fields={"amount": 500, "vendor": "new"},
        )
    except OptimisticConcurrencyError:
        # Return 409 to caller; client must reload + re-submit.
        ...
"""
from __future__ import annotations

import sqlite3
from typing import Any


class OptimisticConcurrencyError(Exception):
    """Raised when an UPDATE's stale-version guard matches 0 rows."""
    def __init__(self, table: str, pk: Any, expected_version: int):
        self.table = table
        self.pk = pk
        self.expected_version = expected_version
        super().__init__(
            f"Stale version for {table} {pk!r}: expected v{expected_version}, "
            "another write has landed since read. Reload and retry.",
        )


# Tables that carry a ``version`` column (populated lazily via migrations).
# Keep this registry in sync with src/db/version_handlers.py dispatch map.
VERSIONED_TABLES: dict[str, str] = {
    "documents":      "document_id",
    "journal_entries": "id",
    "clients":        "client_code",
    "engagements":    "engagement_id",
    "fixed_assets":   "asset_id",
    "working_papers": "paper_id",
    "partnerships":   "id",
    "partners":       "id",
    "manual_journal_entries": "entry_id",
}


def _quoted(s: str) -> str:
    """Identifier quoting for SQLite."""
    return '"' + s.replace('"', '""') + '"'


def _pragma_column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    """PRAGMA table_info returns different shapes depending on the
    connection's row_factory (sqlite3.Row → positional, dict factory →
    named). Extract column names without assuming either shape."""
    names: set[str] = set()
    for r in conn.execute(f"PRAGMA table_info({table})").fetchall():
        if isinstance(r, dict):
            name = r.get("name")
        else:
            try:
                name = r[1]
            except (IndexError, KeyError, TypeError):
                name = None
        if name:
            names.add(name)
    return names


def add_version_column_if_missing(
    conn: sqlite3.Connection, table: str,
) -> bool:
    """Idempotently add a ``version INTEGER DEFAULT 1`` column. Returns
    True if a migration ran, False if the column already existed.
    """
    cols = _pragma_column_names(conn, table)
    if "version" in cols:
        return False
    try:
        conn.execute(
            f"ALTER TABLE {_quoted(table)} ADD COLUMN version INTEGER DEFAULT 1",
        )
        conn.execute(
            f"UPDATE {_quoted(table)} SET version = 1 WHERE version IS NULL",
        )
        conn.commit()
        return True
    except sqlite3.OperationalError:
        return False


def ensure_all_version_columns(conn: sqlite3.Connection) -> list[str]:
    """Migrate every ``VERSIONED_TABLES`` entry. Returns the list of
    table names that were just migrated (empty if all already had it).
    """
    migrated = []
    for table in VERSIONED_TABLES:
        try:
            if add_version_column_if_missing(conn, table):
                migrated.append(table)
        except sqlite3.OperationalError:
            # Table might not exist yet in this DB (e.g., fresh chaos DB).
            pass
    return migrated


def version_check_update(
    conn: sqlite3.Connection,
    *,
    table: str,
    pk_column: str,
    pk_value: Any,
    expected_version: int,
    fields: dict[str, Any],
) -> int:
    """UPDATE with a WHERE version = expected_version guard.

    Bumps version by 1 on success. Raises
    ``OptimisticConcurrencyError`` if 0 rows were updated (stale read).

    Returns the NEW version number.
    """
    if not fields:
        raise ValueError("fields dict must not be empty")
    sets = ", ".join(f"{_quoted(k)} = ?" for k in fields)
    sql = (
        f"UPDATE {_quoted(table)} "
        f"SET {sets}, version = version + 1 "
        f"WHERE {_quoted(pk_column)} = ? AND version = ?"
    )
    params = list(fields.values()) + [pk_value, int(expected_version)]
    cur = conn.execute(sql, params)
    if cur.rowcount == 0:
        raise OptimisticConcurrencyError(table, pk_value, expected_version)
    conn.commit()
    return int(expected_version) + 1


def read_with_version(
    conn: sqlite3.Connection,
    *,
    table: str,
    pk_column: str,
    pk_value: Any,
) -> dict[str, Any] | None:
    """Read a row and return a dict including its current ``version``."""
    row = conn.execute(
        f"SELECT * FROM {_quoted(table)} WHERE {_quoted(pk_column)} = ?",
        (pk_value,),
    ).fetchone()
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return dict(row)
    if isinstance(row, dict):
        return dict(row)
    # Old row_factory = tuple → pair up with PRAGMA columns.
    cols = sorted(_pragma_column_names(conn, table))
    return dict(zip(cols, row))
