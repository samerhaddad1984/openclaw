"""Part 1 Bug B — optimistic-concurrency regression tests."""
from __future__ import annotations

import sqlite3
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.db.optimistic import (  # noqa: E402
    OptimisticConcurrencyError,
    VERSIONED_TABLES,
    add_version_column_if_missing,
    ensure_all_version_columns,
    read_with_version,
    version_check_update,
)


def _mk_documents(tmp_path):
    db = tmp_path / "oc.db"
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT,
            vendor TEXT,
            amount REAL,
            document_date TEXT,
            review_status TEXT
        );
    """)
    c.commit()
    return db, c


def test_migration_adds_version_column(tmp_path):
    db, c = _mk_documents(tmp_path)
    ran = add_version_column_if_missing(c, "documents")
    cols = {r[1] for r in c.execute("PRAGMA table_info(documents)").fetchall()}
    assert ran is True
    assert "version" in cols


def test_migration_idempotent(tmp_path):
    db, c = _mk_documents(tmp_path)
    add_version_column_if_missing(c, "documents")
    # Second call should be a no-op.
    ran = add_version_column_if_missing(c, "documents")
    assert ran is False


def test_first_save_succeeds_with_version_1(tmp_path):
    db, c = _mk_documents(tmp_path)
    add_version_column_if_missing(c, "documents")
    c.execute(
        "INSERT INTO documents (document_id, client_code, amount) "
        "VALUES ('D1', 'ACME', 100)",
    )
    c.commit()
    row = read_with_version(c, table="documents", pk_column="document_id",
                              pk_value="D1")
    assert row["version"] == 1
    new_v = version_check_update(
        c, table="documents", pk_column="document_id", pk_value="D1",
        expected_version=1, fields={"amount": 200},
    )
    assert new_v == 2


def test_stale_version_raises(tmp_path):
    db, c = _mk_documents(tmp_path)
    add_version_column_if_missing(c, "documents")
    c.execute(
        "INSERT INTO documents (document_id, client_code, amount) "
        "VALUES ('D2', 'ACME', 100)",
    )
    c.commit()
    # Alice reads v1.
    row = read_with_version(c, table="documents", pk_column="document_id",
                              pk_value="D2")
    assert row["version"] == 1
    # Bob updates first, now v=2.
    version_check_update(
        c, table="documents", pk_column="document_id", pk_value="D2",
        expected_version=1, fields={"amount": 500},
    )
    # Alice tries to update with stale v=1.
    with pytest.raises(OptimisticConcurrencyError):
        version_check_update(
            c, table="documents", pk_column="document_id", pk_value="D2",
            expected_version=1, fields={"amount": 999},
        )
    # Final amount is Bob's 500, not Alice's 999.
    final = c.execute(
        "SELECT amount, version FROM documents WHERE document_id='D2'",
    ).fetchone()
    assert final["amount"] == 500
    assert final["version"] == 2


def test_version_increments(tmp_path):
    db, c = _mk_documents(tmp_path)
    add_version_column_if_missing(c, "documents")
    c.execute(
        "INSERT INTO documents (document_id, client_code, amount) "
        "VALUES ('D3', 'ACME', 10)",
    )
    c.commit()
    v = 1
    for i in range(5):
        v = version_check_update(
            c, table="documents", pk_column="document_id", pk_value="D3",
            expected_version=v, fields={"amount": 10 + i * 10},
        )
    assert v == 6


def test_read_with_version_missing_pk(tmp_path):
    db, c = _mk_documents(tmp_path)
    add_version_column_if_missing(c, "documents")
    row = read_with_version(c, table="documents", pk_column="document_id",
                              pk_value="NOPE")
    assert row is None


def test_empty_fields_raises(tmp_path):
    db, c = _mk_documents(tmp_path)
    add_version_column_if_missing(c, "documents")
    c.execute(
        "INSERT INTO documents (document_id, client_code) VALUES ('D4', 'ACME')",
    )
    c.commit()
    with pytest.raises(ValueError):
        version_check_update(
            c, table="documents", pk_column="document_id", pk_value="D4",
            expected_version=1, fields={},
        )


def test_concurrent_no_lost_updates(tmp_path):
    """Two threads each try to increment amount from the value they read.
    After the fix, one wins, the other raises OptimisticConcurrencyError.
    """
    db, c = _mk_documents(tmp_path)
    add_version_column_if_missing(c, "documents")
    c.execute(
        "INSERT INTO documents (document_id, client_code, amount) "
        "VALUES ('D5', 'ACME', 100)",
    )
    c.commit()
    c.close()

    results = {"alice": None, "bob": None}
    errors = []

    def worker(name, delta, delay):
        conn = sqlite3.connect(str(db), timeout=5)
        conn.row_factory = sqlite3.Row
        row = read_with_version(conn, table="documents",
                                  pk_column="document_id", pk_value="D5")
        import time as _t
        _t.sleep(delay)
        try:
            new_v = version_check_update(
                conn, table="documents", pk_column="document_id",
                pk_value="D5", expected_version=row["version"],
                fields={"amount": row["amount"] + delta},
            )
            results[name] = new_v
        except OptimisticConcurrencyError as e:
            errors.append((name, str(e)))
        conn.close()

    t1 = threading.Thread(target=worker, args=("alice", 100, 0.02))
    t2 = threading.Thread(target=worker, args=("bob", 50, 0.05))
    t1.start(); t2.start(); t1.join(); t2.join()

    assert len(errors) == 1, "exactly one writer should fail on stale version"
    assert sum(1 for v in results.values() if v is not None) == 1

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    final = conn.execute(
        "SELECT amount, version FROM documents WHERE document_id='D5'",
    ).fetchone()
    conn.close()
    # Winner wrote either +100 or +50 (not +150, which would be a lost update).
    assert final["amount"] in (200.0, 150.0)
    assert final["version"] == 2


def test_bulk_migration(tmp_path):
    """ensure_all_version_columns adds 'version' to every registered table
    it finds, and silently skips tables that don't exist yet.
    """
    db = tmp_path / "bulk.db"
    c = sqlite3.connect(str(db))
    # Create only documents + clients.
    c.executescript("""
        CREATE TABLE documents (document_id TEXT PRIMARY KEY);
        CREATE TABLE clients (client_code TEXT PRIMARY KEY, name TEXT);
    """)
    c.commit()
    migrated = ensure_all_version_columns(c)
    # documents + clients should both migrate; the others are absent.
    assert "documents" in migrated
    assert "clients" in migrated
    for absent in ("journal_entries", "engagements", "fixed_assets",
                    "working_papers"):
        assert absent not in migrated
    c.close()
