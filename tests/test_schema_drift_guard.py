"""Pytest wrapper for scripts/guards/check_schema_drift.py.

Asserts the current codebase has zero schema drift (every SQL-selected
column has a corresponding bootstrap migration).

Also exercises the guard's parsing on synthetic inputs so a future
breakage of the guard itself is caught.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.guards.check_schema_drift import (  # noqa: E402
    find_drift,
    _all_py_files,
    _all_schema_sources,
    _extract_columns_from_create_body,
    _split_sql_blocks,
    _extract_alias_map,
    _extract_alias_col_refs,
    collect_migrated_columns,
)


# ---------------------------------------------------------------------------
# Cornerstone: current codebase has no drift.
# ---------------------------------------------------------------------------

def test_current_codebase_has_no_schema_drift():
    drifts = find_drift(_all_py_files())
    assert drifts == [], (
        "Schema drift detected. Add bootstrap migrations for the "
        "following columns, then re-run.\n\n"
        + "\n".join(
            f"  {f}:{ln}  {table!r}.{col!r}" for f, ln, table, col in drifts
        )
    )


# ---------------------------------------------------------------------------
# Parser unit tests.
# ---------------------------------------------------------------------------

def test_extract_columns_picks_up_primary_key_columns():
    body = """
        client_code TEXT PRIMARY KEY,
        client_name TEXT NOT NULL,
        email       TEXT
    """
    cols = _extract_columns_from_create_body(body)
    assert cols >= {"client_code", "client_name", "email"}


def test_extract_columns_skips_named_constraints():
    body = """
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        PRIMARY KEY (id),
        UNIQUE (name),
        FOREIGN KEY (id) REFERENCES parent(id)
    """
    cols = _extract_columns_from_create_body(body)
    assert cols == {"id", "name"}


def test_split_sql_blocks_finds_triple_quoted_sql():
    src = '''
def f():
    q = """
        SELECT * FROM foo
    """
    return q
'''
    blocks = _split_sql_blocks(src)
    assert any("SELECT" in b for b in blocks)


def test_extract_alias_map_handles_from_and_joins():
    sql = """
        SELECT d.id, pj.status
        FROM documents d
        LEFT JOIN posting_jobs pj ON pj.document_id = d.document_id
    """
    aliases = _extract_alias_map(sql)
    assert aliases["d"] == "documents"
    assert aliases["pj"] == "posting_jobs"


def test_extract_alias_col_refs_ignores_keywords():
    sql = "SELECT d.id, d.vendor FROM documents d WHERE d.amount > 0"
    refs = _extract_alias_col_refs(sql)
    cols = {c for _, c in refs}
    assert cols == {"id", "vendor", "amount"}


def test_collect_migrated_columns_spans_alter_and_create(tmp_path):
    f = tmp_path / "example.py"
    f.write_text('''
def make():
    conn.execute("""
        CREATE TABLE foo (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    """)
    conn.execute("ALTER TABLE foo ADD COLUMN new_col TEXT")
''')
    tables = collect_migrated_columns([f])
    assert tables["foo"] == {"id", "name", "new_col"}


# ---------------------------------------------------------------------------
# End-to-end: inject drift, verify caught; restore; verify clean.
# ---------------------------------------------------------------------------

def test_guard_catches_injected_drift(tmp_path):
    """Simulate the whole pipeline in a throwaway dir: a fake file
    defines a table, another fake file queries an unknown column on
    it. find_drift must flag."""
    p = tmp_path / "fake_with_drift.py"
    p.write_text('''
def schema():
    conn.execute("""
        CREATE TABLE widgets (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    """)

def query():
    conn.execute("""
        SELECT w.unknown_column FROM widgets w
    """)
''')
    drifts = find_drift([p])
    assert len(drifts) == 1, drifts
    _, _, table, col = drifts[0]
    assert table == "widgets"
    assert col == "unknown_column"


def test_guard_silent_when_all_columns_migrated(tmp_path):
    p = tmp_path / "fake_clean.py"
    p.write_text('''
def schema():
    conn.execute("""
        CREATE TABLE widgets (
            id INTEGER PRIMARY KEY,
            name TEXT,
            color TEXT
        )
    """)

def query():
    conn.execute("""
        SELECT w.name, w.color FROM widgets w
    """)
''')
    drifts = find_drift([p])
    assert drifts == []


def test_guard_ignores_unknown_tables(tmp_path):
    """A query against a table we've never seen CREATE for is ignored
    — we can only check what we know."""
    p = tmp_path / "fake_unknown.py"
    p.write_text('''
def query():
    conn.execute("SELECT u.field FROM some_external_table u")
''')
    drifts = find_drift([p])
    assert drifts == []


def test_guard_ignores_sqlite_pseudo_columns(tmp_path):
    """rowid / oid / _rowid_ are always present in SQLite, no
    migration required."""
    p = tmp_path / "fake_rowid.py"
    p.write_text('''
def schema():
    conn.execute("""
        CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)
    """)

def query():
    conn.execute("SELECT t.rowid, t.v FROM t")
''')
    drifts = find_drift([p])
    assert drifts == []
