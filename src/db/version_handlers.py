"""HTTP glue for optimistic-concurrency updates.

``version_check_update`` is the engine-level primitive. This module wraps
it so HTTP POST handlers can (a) pull the expected version out of a
request body / form, (b) call the update, and (c) return the conventional
{409, current_version, reload_required: True} JSON when the caller's
version is stale.

The goal is that wiring an existing POST handler becomes a one-liner
swap: instead of a raw ``UPDATE ... WHERE pk = ?``, the handler calls
``versioned_update_from_request(...)`` and lets this module decide
whether to return 200 + new_version or 409.

Legacy compatibility: if the request body does not carry a ``version``
field, we perform the update without the guard and return ``None`` for
the new version. This keeps existing UI forms that never knew about
versions working, while giving newer clients a safe upgrade path. Call
sites can set ``require_version=True`` to reject legacy requests
outright.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Any

from .optimistic import (
    OptimisticConcurrencyError,
    VERSIONED_TABLES,
    add_version_column_if_missing,
    version_check_update,
)

log = logging.getLogger(__name__)


@dataclass
class VersionedUpdateResult:
    """Outcome of a versioned-update attempt.

    - ``status`` is 200 when the update landed, 409 when the caller's
      version was stale, 400 when ``require_version=True`` and no version
      was supplied.
    - ``new_version`` is populated on status 200.
    - ``current_version`` is populated on status 409 so the client can
      display "someone edited this — reload to see the latest" and carry
      the fresh version in the retry.
    """
    status: int
    new_version: int | None = None
    current_version: int | None = None
    error: str | None = None

    def to_json(self) -> dict[str, Any]:
        if self.status == 200:
            return {"ok": True, "version": self.new_version}
        if self.status == 409:
            return {
                "ok": False,
                "error": "version_conflict",
                "current_version": self.current_version,
                "reload_required": True,
                "message": (
                    "Another user edited this record since you opened it. "
                    "Reload to see their changes and re-submit."
                ),
            }
        return {"ok": False, "error": self.error or "bad_request"}


def extract_version(body: dict[str, Any] | None) -> int | None:
    """Pull a version integer out of a request body. Accepts ``version``,
    ``expected_version``, and ``__version`` (the last for embedded hidden
    form fields). Returns None if missing or malformed."""
    if not body:
        return None
    for key in ("expected_version", "version", "__version"):
        v = body.get(key)
        if v is None or v == "":
            continue
        try:
            return int(v)
        except (TypeError, ValueError):
            continue
    return None


def _read_current_version(
    conn: sqlite3.Connection, table: str, pk_column: str, pk_value: Any,
) -> int | None:
    try:
        row = conn.execute(
            f"SELECT version FROM \"{table}\" WHERE \"{pk_column}\" = ?",
            (pk_value,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    # sqlite3.Row or tuple — both subscriptable.
    try:
        return int(row["version"])
    except (KeyError, IndexError, TypeError):
        try:
            return int(row[0])
        except (TypeError, ValueError):
            return None


def versioned_child_mutation(
    conn: sqlite3.Connection,
    *,
    parent_table: str,
    parent_pk_value: Any,
    body: dict[str, Any] | None,
    child_operation: "callable",  # type: ignore[valid-type]
    require_version: bool = False,
) -> VersionedUpdateResult:
    """Guard a child-table mutation (INSERT/DELETE) with a parent-row
    version check.

    Semantics:
      1. Read parent_table.version for parent_pk_value.
      2. If the caller supplied ``expected_version`` / ``version`` in
         ``body`` and it doesn't match the current parent version, return
         ``VersionedUpdateResult(status=409, current_version=N)``.
      3. If ``require_version=True`` and no version was supplied, return
         status=400 without touching anything.
      4. Invoke ``child_operation(conn)`` — the actual INSERT/DELETE.
      5. Bump the parent row's version by 1 and commit.

    This prevents cross-table races such as "two reviewers hit
    /partnerships/42/partners/add at the same time, each ended up
    allocating 100% of the profit to their own partner because the
    parent partnership row was never locked."
    """
    from .optimistic import VERSIONED_TABLES, add_version_column_if_missing
    if parent_table not in VERSIONED_TABLES:
        raise ValueError(f"parent_table {parent_table!r} not in VERSIONED_TABLES")
    parent_pk_column = VERSIONED_TABLES[parent_table]

    try:
        add_version_column_if_missing(conn, parent_table)
    except sqlite3.OperationalError:
        pass

    expected_version = extract_version(body)
    if expected_version is None:
        if require_version:
            return VersionedUpdateResult(status=400, error="version_required")
        # Legacy path: take a write lock up-front so the parent bump +
        # child op land atomically relative to other writers. BEGIN
        # IMMEDIATE serializes concurrent legacy writers as well.
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            pass
        child_operation(conn)
        try:
            conn.execute(
                f'UPDATE "{parent_table}" '
                f'SET version = COALESCE(version, 1) + 1 '
                f'WHERE "{parent_pk_column}" = ?',
                (parent_pk_value,),
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass
        new_version = _read_current_version(
            conn, parent_table, parent_pk_column, parent_pk_value,
        )
        return VersionedUpdateResult(status=200, new_version=new_version)

    # Versioned path: take an IMMEDIATE write-lock BEFORE the version
    # check, bump the parent first (if version matches), then run the
    # child op. Doing the bump first makes the lost-update check an
    # atomic compare-and-swap — two concurrent writers with the same
    # stale read can't both pass; the second sees rowcount=0 and 409s
    # *before* touching any child rows.
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        # Already in a transaction — continue; the caller's enclosing
        # ``with open_db() as conn`` scope is still authoritative.
        pass
    try:
        cur = conn.execute(
            f'UPDATE "{parent_table}" '
            f'SET version = version + 1 '
            f'WHERE "{parent_pk_column}" = ? AND version = ?',
            (parent_pk_value, int(expected_version)),
        )
        if cur.rowcount == 0:
            current = _read_current_version(
                conn, parent_table, parent_pk_column, parent_pk_value,
            )
            try:
                conn.rollback()
            except sqlite3.OperationalError:
                pass
            return VersionedUpdateResult(status=409, current_version=current)
        child_operation(conn)
        conn.commit()
    except sqlite3.OperationalError:
        try:
            conn.rollback()
        except sqlite3.OperationalError:
            pass
        raise
    return VersionedUpdateResult(
        status=200, new_version=int(expected_version) + 1,
    )


def versioned_update_from_request(
    conn: sqlite3.Connection,
    *,
    table: str,
    pk_value: Any,
    fields: dict[str, Any],
    body: dict[str, Any] | None,
    require_version: bool = False,
) -> VersionedUpdateResult:
    """Perform an UPDATE guarded by an optimistic-concurrency check.

    ``fields`` is the mapping of column → new value to UPDATE. It must
    not include ``version`` (that's managed by ``version_check_update``).

    ``body`` is the parsed request body (dict); we pull ``version`` out
    of it via ``extract_version``.

    When ``require_version`` is True and the body has no version, we
    return status=400 without touching the row — use this for newer
    endpoints that should reject legacy unversioned clients.
    """
    if table not in VERSIONED_TABLES:
        raise ValueError(f"table {table!r} is not registered in VERSIONED_TABLES")
    pk_column = VERSIONED_TABLES[table]

    # Ensure the version column exists. Older DBs without the column get
    # a lazy migration; safe to call repeatedly.
    try:
        add_version_column_if_missing(conn, table)
    except sqlite3.OperationalError:
        pass

    expected_version = extract_version(body)
    if expected_version is None:
        if require_version:
            return VersionedUpdateResult(
                status=400, error="version_required",
            )
        # Legacy path: run the update without the guard. We still bump
        # the version column so subsequent versioned callers see the
        # write that just landed.
        if not fields:
            return VersionedUpdateResult(status=200, new_version=None)
        sets = ", ".join(f'"{k}" = ?' for k in fields)
        sql = (
            f'UPDATE "{table}" SET {sets}, version = COALESCE(version, 1) + 1 '
            f'WHERE "{pk_column}" = ?'
        )
        params = list(fields.values()) + [pk_value]
        try:
            conn.execute(sql, params)
            conn.commit()
        except sqlite3.OperationalError as exc:
            log.warning("legacy versioned update on %s failed: %s", table, exc)
            return VersionedUpdateResult(status=400, error=str(exc))
        new_version = _read_current_version(conn, table, pk_column, pk_value)
        return VersionedUpdateResult(status=200, new_version=new_version)

    try:
        new_version = version_check_update(
            conn,
            table=table,
            pk_column=pk_column,
            pk_value=pk_value,
            expected_version=expected_version,
            fields=fields,
        )
    except OptimisticConcurrencyError:
        current = _read_current_version(conn, table, pk_column, pk_value)
        return VersionedUpdateResult(status=409, current_version=current)
    return VersionedUpdateResult(status=200, new_version=new_version)
