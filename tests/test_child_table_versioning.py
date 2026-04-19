"""Edge case 3 — parent-version check on partnership/SR&ED child routes.

The affected routes INSERT/DELETE into child tables that share a parent
row in a versioned table:

  /partnerships/<id>/partners/add    → partners table, parent=partnerships
  /partnerships/<id>/partners/delete → partners table, parent=partnerships
  /partnerships/<id>/allocate        → computed allocations, parent=partnerships
  /sred/<id>/expenditures/add        → sred_expenditures, parent=sred_claims
  /sred/<id>/expenditures/delete     → sred_expenditures, parent=sred_claims
  /sred/<id>/narrative               → updates sred_claims directly

The child helper ``versioned_child_mutation`` reads the parent version,
refuses the mutation when the caller supplied a stale ``version``, runs
the child op, and bumps the parent's version. These tests confirm that
contract at the helper level plus a threading race.
"""
from __future__ import annotations

import sqlite3
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db.version_handlers import (  # noqa: E402
    VersionedUpdateResult,
    versioned_child_mutation,
)


def _mk_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "child.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE partnerships (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT, partnership_name TEXT,
            partnership_type TEXT, tax_year_end TEXT,
            version INTEGER DEFAULT 1
        );
        CREATE TABLE partners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            partnership_id INTEGER, partner_name TEXT,
            partner_type TEXT, partner_sin_or_bn TEXT,
            allocation_percentage REAL, effective_date TEXT, end_date TEXT,
            version INTEGER DEFAULT 1
        );
        CREATE TABLE sred_claims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT, tax_year INTEGER, claim_type TEXT,
            project_name TEXT, status TEXT,
            technological_advancement TEXT, technological_obstacles TEXT,
            work_performed TEXT,
            version INTEGER DEFAULT 1
        );
        CREATE TABLE sred_expenditures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            claim_id INTEGER, category TEXT, amount REAL,
            qualifying_amount REAL, description TEXT,
            version INTEGER DEFAULT 1
        );
        INSERT INTO partnerships (client_code, partnership_name, partnership_type,
                                   tax_year_end, version)
        VALUES ('C1', 'Acme LP', 'general', '2026-12-31', 1);
        INSERT INTO sred_claims (client_code, tax_year, claim_type, project_name,
                                  status, version)
        VALUES ('C1', 2026, 'traditional', 'AI', 'draft', 1);
    """)
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Partnership child routes
# ---------------------------------------------------------------------------

def test_add_partner_requires_parent_version(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row

    def _op(c):  # pragma: no cover — only runs on the "matches" path
        c.execute(
            "INSERT INTO partners (partnership_id, partner_name, allocation_percentage, effective_date) "
            "VALUES (1, 'P', 50.0, '2026-01-01')",
        )

    res = versioned_child_mutation(
        conn, parent_table="partnerships", parent_pk_value=1,
        body={}, child_operation=_op, require_version=True,
    )
    assert res.status == 400 and res.error == "version_required"
    conn.close()


def test_add_partner_409_on_stale_parent(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    # Parent has moved to v=3 behind the caller's back.
    conn.execute("UPDATE partnerships SET version = 3 WHERE id = 1")
    conn.commit()
    called = {"n": 0}

    def _op(c):  # must NOT be called on 409
        called["n"] += 1
        c.execute(
            "INSERT INTO partners (partnership_id, partner_name, allocation_percentage, effective_date) "
            "VALUES (1, 'Stale', 50.0, '2026-01-01')",
        )

    res = versioned_child_mutation(
        conn, parent_table="partnerships", parent_pk_value=1,
        body={"version": 1}, child_operation=_op,
    )
    assert res.status == 409
    assert res.current_version == 3
    assert called["n"] == 0
    # No partner row was inserted.
    n = conn.execute("SELECT COUNT(*) FROM partners WHERE partnership_id=1").fetchone()[0]
    assert n == 0
    conn.close()


def test_delete_partner_requires_parent_version(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO partners (partnership_id, partner_name, allocation_percentage, effective_date) "
        "VALUES (1, 'Alice', 50.0, '2026-01-01')",
    )
    conn.commit()
    pid = conn.execute("SELECT id FROM partners WHERE partner_name='Alice'").fetchone()["id"]

    res = versioned_child_mutation(
        conn, parent_table="partnerships", parent_pk_value=1,
        body={}, child_operation=lambda c: c.execute("DELETE FROM partners WHERE id=?", (pid,)),
        require_version=True,
    )
    assert res.status == 400
    # Row still there.
    n = conn.execute("SELECT COUNT(*) FROM partners").fetchone()[0]
    assert n == 1
    conn.close()


def test_allocate_partnership_requires_parent_version(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    res = versioned_child_mutation(
        conn, parent_table="partnerships", parent_pk_value=1,
        body={}, child_operation=lambda c: None,
        require_version=True,
    )
    assert res.status == 400 and res.error == "version_required"
    conn.close()


# ---------------------------------------------------------------------------
# SR&ED child routes
# ---------------------------------------------------------------------------

def test_add_sred_expenditure_requires_parent_version(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    res = versioned_child_mutation(
        conn, parent_table="sred_claims", parent_pk_value=1,
        body={}, child_operation=lambda c: c.execute(
            "INSERT INTO sred_expenditures (claim_id, category, amount) VALUES (1, 'salaries', 10000)",
        ),
        require_version=True,
    )
    assert res.status == 400
    conn.close()


def test_add_sred_expenditure_409_on_stale_parent(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.execute("UPDATE sred_claims SET version = 5 WHERE id = 1")
    conn.commit()
    res = versioned_child_mutation(
        conn, parent_table="sred_claims", parent_pk_value=1,
        body={"version": 1},
        child_operation=lambda c: c.execute(
            "INSERT INTO sred_expenditures (claim_id, category, amount) VALUES (1, 'salaries', 10000)",
        ),
    )
    assert res.status == 409
    assert res.current_version == 5
    n = conn.execute("SELECT COUNT(*) FROM sred_expenditures WHERE claim_id=1").fetchone()[0]
    assert n == 0
    conn.close()


def test_delete_sred_expenditure_requires_parent_version(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO sred_expenditures (claim_id, category, amount) VALUES (1, 'salaries', 10000)",
    )
    conn.commit()
    eid = conn.execute("SELECT id FROM sred_expenditures").fetchone()["id"]

    res = versioned_child_mutation(
        conn, parent_table="sred_claims", parent_pk_value=1,
        body={},
        child_operation=lambda c: c.execute("DELETE FROM sred_expenditures WHERE id=?", (eid,)),
        require_version=True,
    )
    assert res.status == 400
    n = conn.execute("SELECT COUNT(*) FROM sred_expenditures").fetchone()[0]
    assert n == 1
    conn.close()


# ---------------------------------------------------------------------------
# Parent version bumps after a successful child mutation
# ---------------------------------------------------------------------------

def test_parent_version_bumps_after_child_mutation(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    before = int(conn.execute("SELECT version FROM partnerships WHERE id=1").fetchone()["version"])
    res = versioned_child_mutation(
        conn, parent_table="partnerships", parent_pk_value=1,
        body={"version": 1},
        child_operation=lambda c: c.execute(
            "INSERT INTO partners (partnership_id, partner_name, allocation_percentage, effective_date) "
            "VALUES (1, 'Carol', 25.0, '2026-01-01')",
        ),
    )
    assert res.status == 200
    after = int(conn.execute("SELECT version FROM partnerships WHERE id=1").fetchone()["version"])
    assert after == before + 1
    assert res.new_version == after
    # Child row landed.
    n = conn.execute("SELECT COUNT(*) FROM partners WHERE partnership_id=1").fetchone()[0]
    assert n == 1
    conn.close()


# ---------------------------------------------------------------------------
# Concurrent writers: one lands, one 409s. No silent double-writes.
# ---------------------------------------------------------------------------

def test_concurrent_add_partner_and_edit_allocation(tmp_path):
    db = _mk_db(tmp_path)
    winners: list[str] = []
    losers: list[str] = []
    barrier = threading.Barrier(2)
    lock = threading.Lock()

    def worker(tag: str):
        c = sqlite3.connect(str(db))
        c.execute("PRAGMA busy_timeout = 5000")
        c.row_factory = sqlite3.Row
        barrier.wait()
        try:
            res = versioned_child_mutation(
                c, parent_table="partnerships", parent_pk_value=1,
                body={"version": 1},
                child_operation=lambda conn: conn.execute(
                    "INSERT INTO partners (partnership_id, partner_name, "
                    "allocation_percentage, effective_date) VALUES (1, ?, 50.0, '2026-01-01')",
                    (f"P-{tag}",),
                ),
            )
            with lock:
                if res.status == 200:
                    winners.append(tag)
                elif res.status == 409:
                    losers.append(tag)
        finally:
            c.close()

    ts = [threading.Thread(target=worker, args=(t,)) for t in ("A", "B")]
    for t in ts: t.start()
    for t in ts: t.join()

    assert len(winners) == 1, f"winners={winners} losers={losers}"
    assert len(losers) == 1, f"winners={winners} losers={losers}"

    # Exactly one partner row landed.
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    n = conn.execute("SELECT COUNT(*) FROM partners WHERE partnership_id=1").fetchone()[0]
    assert n == 1
    # Parent bumped to v=2.
    v = int(conn.execute("SELECT version FROM partnerships WHERE id=1").fetchone()["version"])
    assert v == 2
    conn.close()


# ---------------------------------------------------------------------------
# Legacy path — missing version, require_version=False — still runs but
# bumps parent version so subsequent versioned readers see freshness.
# ---------------------------------------------------------------------------

def test_legacy_child_mutation_still_bumps_parent(tmp_path):
    db = _mk_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    res = versioned_child_mutation(
        conn, parent_table="partnerships", parent_pk_value=1,
        body={},  # no version, legacy caller
        child_operation=lambda c: c.execute(
            "INSERT INTO partners (partnership_id, partner_name, "
            "allocation_percentage, effective_date) VALUES (1, 'Legacy', 10.0, '2026-01-01')",
        ),
    )
    assert res.status == 200
    v = int(conn.execute("SELECT version FROM partnerships WHERE id=1").fetchone()["version"])
    assert v == 2
    conn.close()
