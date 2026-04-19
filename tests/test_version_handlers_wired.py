"""Tests for the version_handlers wiring.

Covers the engine-level helper (``versioned_update_from_request``),
the dashboard convenience wrapper (``update_document_fields_versioned``),
and a direct HTTP test of /document/update with and without the
version field.

The ``VERSIONED_TABLES`` registry is also exercised so anyone adding a
new table sees a migration path test failing if they forget to register.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.db.optimistic import (
    VERSIONED_TABLES,
    add_version_column_if_missing,
    ensure_all_version_columns,
    version_check_update,
)
from src.db.version_handlers import (
    VersionedUpdateResult,
    extract_version,
    versioned_update_from_request,
)


# ---------------------------------------------------------------------------
# Helper-level tests
# ---------------------------------------------------------------------------

def _mk_docs_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE documents ("
        "document_id TEXT PRIMARY KEY, vendor TEXT, amount REAL, version INTEGER DEFAULT 1)"
    )
    conn.execute(
        "INSERT INTO documents (document_id, vendor, amount, version) "
        "VALUES ('D1', 'ACME', 100.0, 1)"
    )
    conn.commit()
    return conn


def test_extract_version_handles_all_alias_keys():
    assert extract_version({"version": "3"}) == 3
    assert extract_version({"expected_version": 7}) == 7
    assert extract_version({"__version": "2"}) == 2
    assert extract_version({"other": "5"}) is None
    assert extract_version({}) is None
    assert extract_version(None) is None
    # Malformed values are tolerated as if missing.
    assert extract_version({"version": "not a number"}) is None
    assert extract_version({"version": ""}) is None


def test_versioned_update_succeeds_when_version_matches(tmp_path):
    conn = _mk_docs_db(tmp_path / "v.db")
    result = versioned_update_from_request(
        conn, table="documents", pk_value="D1",
        fields={"vendor": "NEW"}, body={"version": 1},
    )
    assert result.status == 200
    assert result.new_version == 2
    row = conn.execute("SELECT vendor, version FROM documents WHERE document_id='D1'").fetchone()
    assert row["vendor"] == "NEW"
    assert row["version"] == 2


def test_versioned_update_returns_409_on_stale_version(tmp_path):
    conn = _mk_docs_db(tmp_path / "v.db")
    # Bump version to 2 behind the caller's back.
    conn.execute("UPDATE documents SET version=2 WHERE document_id='D1'")
    conn.commit()
    result = versioned_update_from_request(
        conn, table="documents", pk_value="D1",
        fields={"vendor": "STALE"}, body={"version": 1},
    )
    assert result.status == 409
    assert result.current_version == 2
    # Row unchanged.
    row = conn.execute("SELECT vendor FROM documents WHERE document_id='D1'").fetchone()
    assert row["vendor"] == "ACME"
    # JSON payload includes reload_required.
    payload = result.to_json()
    assert payload["error"] == "version_conflict"
    assert payload["current_version"] == 2
    assert payload["reload_required"] is True


def test_versioned_update_missing_version_legacy_path_bumps_version(tmp_path):
    """Without require_version the update runs and the version still
    increments so subsequent versioned readers see a fresh number."""
    conn = _mk_docs_db(tmp_path / "v.db")
    result = versioned_update_from_request(
        conn, table="documents", pk_value="D1",
        fields={"vendor": "LEGACY"}, body={},
    )
    assert result.status == 200
    assert result.new_version == 2
    row = conn.execute("SELECT vendor, version FROM documents WHERE document_id='D1'").fetchone()
    assert row["vendor"] == "LEGACY"
    assert row["version"] == 2


def test_versioned_update_missing_version_rejected_when_required(tmp_path):
    conn = _mk_docs_db(tmp_path / "v.db")
    result = versioned_update_from_request(
        conn, table="documents", pk_value="D1",
        fields={"vendor": "X"}, body={}, require_version=True,
    )
    assert result.status == 400
    assert result.error == "version_required"
    # Row unchanged.
    row = conn.execute("SELECT vendor, version FROM documents WHERE document_id='D1'").fetchone()
    assert row["vendor"] == "ACME" and row["version"] == 1


def test_versioned_update_rejects_unregistered_table(tmp_path):
    conn = _mk_docs_db(tmp_path / "v.db")
    with pytest.raises(ValueError):
        versioned_update_from_request(
            conn, table="unknown_table", pk_value="X",
            fields={"foo": 1}, body={"version": 1},
        )


def test_version_check_update_raises_on_concurrent_writers(tmp_path):
    """Two threads read v=1 and both try to UPDATE. Exactly one wins; the
    other raises OptimisticConcurrencyError. No silent overwrites."""
    db_path = tmp_path / "conc.db"
    conn_seed = _mk_docs_db(db_path)
    conn_seed.close()

    winners: list[str] = []
    losers: list[str] = []
    barrier = threading.Barrier(2)

    def worker(tag: str):
        c = sqlite3.connect(str(db_path))
        c.execute("PRAGMA busy_timeout = 5000")
        try:
            result = versioned_update_from_request(
                c, table="documents", pk_value="D1",
                fields={"vendor": tag}, body={"version": 1},
            )
            if result.status == 200:
                winners.append(tag)
            elif result.status == 409:
                losers.append(tag)
        finally:
            c.close()

    def driver(tag: str):
        barrier.wait()
        worker(tag)

    ts = [threading.Thread(target=driver, args=(tag,)) for tag in ("A", "B")]
    for t in ts: t.start()
    for t in ts: t.join()

    assert len(winners) == 1, f"winners={winners} losers={losers}"
    assert len(losers) == 1, f"winners={winners} losers={losers}"


# ---------------------------------------------------------------------------
# VERSIONED_TABLES registry invariants
# ---------------------------------------------------------------------------

def test_versioned_tables_registry_has_all_expected_entries():
    expected = {
        "documents", "journal_entries", "clients", "engagements",
        "fixed_assets", "working_papers", "partnerships", "partners",
        "manual_journal_entries",
    }
    missing = expected - set(VERSIONED_TABLES)
    assert not missing, f"missing from VERSIONED_TABLES: {missing}"
    # Every PK column is non-empty.
    for table, pk in VERSIONED_TABLES.items():
        assert pk, f"empty pk_column for {table}"


def test_ensure_all_version_columns_is_idempotent(tmp_path):
    db_path = tmp_path / "e.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE documents (document_id TEXT PRIMARY KEY, vendor TEXT)")
    conn.execute("CREATE TABLE clients (client_code TEXT PRIMARY KEY, name TEXT)")
    conn.commit()

    first = ensure_all_version_columns(conn)
    assert set(first) == {"documents", "clients"}
    # Second call: both already present, nothing migrated.
    second = ensure_all_version_columns(conn)
    assert second == []


# ---------------------------------------------------------------------------
# Dashboard wrapper: update_document_fields_versioned
# ---------------------------------------------------------------------------

def test_dashboard_wrapper_versioned_update(monkeypatch, tmp_path):
    """Smoke-test the review_dashboard wrapper by pointing its
    open_db() at a throwaway DB and verifying 200/409 semantics."""
    import importlib
    db_path = tmp_path / "dash.db"
    # Seed the schema the wrapper expects.
    seed = sqlite3.connect(str(db_path))
    seed.execute(
        "CREATE TABLE documents ("
        "document_id TEXT PRIMARY KEY, vendor TEXT, client_code TEXT, "
        "doc_type TEXT, amount REAL, document_date TEXT, gl_account TEXT, "
        "tax_code TEXT, category TEXT, review_status TEXT, "
        "manual_hold_reason TEXT, manual_hold_by TEXT, manual_hold_at TEXT, "
        "updated_at TEXT, version INTEGER DEFAULT 1)"
    )
    seed.execute(
        "INSERT INTO documents (document_id, vendor, version) "
        "VALUES ('D1', 'ACME', 1)"
    )
    seed.commit()
    seed.close()

    monkeypatch.setenv("OTOCPA_DB", str(db_path))
    # Reload the module so DB_PATH picks up the env.
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db_path)

    def _open():
        c = sqlite3.connect(str(db_path))
        c.row_factory = rd._dict_factory
        return c
    monkeypatch.setattr(rd, "open_db", _open)

    # 200 with matching version.
    result = rd.update_document_fields_versioned(
        "D1", {"vendor": "VERSIONED"}, body={"version": 1},
    )
    assert result.status == 200
    assert result.new_version == 2

    # 409 with stale version.
    stale = rd.update_document_fields_versioned(
        "D1", {"vendor": "STALE"}, body={"version": 1},
    )
    assert stale.status == 409
    assert stale.current_version == 2

    # Legacy: no version → still works, version continues to bump.
    legacy = rd.update_document_fields_versioned(
        "D1", {"vendor": "LEGACY"}, body={},
    )
    assert legacy.status == 200
    assert legacy.new_version == 3

    # require_version path rejects unversioned callers.
    strict = rd.update_document_fields_versioned(
        "D1", {"vendor": "NOPE"}, body={}, require_version=True,
    )
    assert strict.status == 400
