"""R3-Investigation 10 — upgrade/migration safety.

A deploy that hits bootstrap_schema twice, or runs against a restored
backup that's a week old, or picks up a DB created by an older build
must not crash and must not lose data.
"""
from __future__ import annotations

import shutil
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# bootstrap_schema is idempotent.
# ---------------------------------------------------------------------------

def test_bootstrap_schema_runs_twice_without_error(tmp_path, monkeypatch):
    """No ALTER TABLE should error on second run. No CREATE should
    fail. Running the same bootstrap twice must be a no-op beyond
    printing info."""
    db = tmp_path / "x.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    # Seed minimal tables the bootstrap expects to find.
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT);
        CREATE TABLE documents (document_id TEXT PRIMARY KEY);
    """)
    conn.commit(); conn.close()

    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    # First run.
    rd.bootstrap_schema()
    # Second run.
    rd.bootstrap_schema()
    # Third run, for good measure.
    rd.bootstrap_schema()


# ---------------------------------------------------------------------------
# Missing columns backfilled idempotently.
# ---------------------------------------------------------------------------

def test_bootstrap_backfills_columns_only_once(tmp_path, monkeypatch):
    """Run bootstrap on a minimal schema, then inspect the documents
    table — the expected new columns should appear exactly once each,
    not duplicated."""
    db = tmp_path / "backfill.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE documents (document_id TEXT PRIMARY KEY);
        CREATE TABLE clients (client_code TEXT PRIMARY KEY);
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
    """)
    conn.commit(); conn.close()
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))

    rd.bootstrap_schema()
    with sqlite3.connect(str(db)) as c:
        cols_1 = [r[1] for r in c.execute("PRAGMA table_info(documents)").fetchall()]
    # Second bootstrap must not re-add columns.
    rd.bootstrap_schema()
    with sqlite3.connect(str(db)) as c:
        cols_2 = [r[1] for r in c.execute("PRAGMA table_info(documents)").fetchall()]
    assert cols_1 == cols_2, (
        f"bootstrap added columns on second run: "
        f"{set(cols_2) - set(cols_1)}"
    )
    # Sanity: key columns exist.
    for must in ("vendor", "amount", "document_date", "review_status",
                 "version", "ai_used", "hallucination_suspected"):
        assert must in cols_1, f"missing expected column {must}"


# ---------------------------------------------------------------------------
# Data preservation — existing rows survive a bootstrap pass.
# ---------------------------------------------------------------------------

def test_existing_document_data_survives_bootstrap(tmp_path, monkeypatch):
    """Seed a documents row, run bootstrap, verify the row still exists
    with original values."""
    db = tmp_path / "preserve.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (client_code TEXT PRIMARY KEY);
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            vendor TEXT,
            client_code TEXT
        );
        INSERT INTO documents VALUES ('DOC', 'Acme', 'C1');
    """)
    conn.commit(); conn.close()
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    with sqlite3.connect(str(db)) as c:
        c.row_factory = sqlite3.Row
        row = c.execute(
            "SELECT document_id, vendor, client_code FROM documents WHERE document_id='DOC'",
        ).fetchone()
    assert row is not None
    assert row["vendor"] == "Acme"
    assert row["client_code"] == "C1"


# ---------------------------------------------------------------------------
# Restore-from-backup scenario.
# ---------------------------------------------------------------------------

def test_old_backup_can_be_loaded_and_bootstrapped(tmp_path, monkeypatch):
    """Restore the latest production SQLite backup into a tmp location,
    then run bootstrap against it. No crashes; data still accessible."""
    backup_dir = Path("/opt/backups/otocpa")
    if not backup_dir.exists():
        pytest.skip("no backup directory in this environment")
    backups = sorted(backup_dir.glob("sqlite_*.db"),
                     key=lambda p: p.stat().st_mtime)
    if not backups:
        pytest.skip("no sqlite backups present")
    restored = tmp_path / "restored.db"
    shutil.copy2(backups[-1], restored)
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", restored)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    # Verify we can still read from documents.
    with sqlite3.connect(str(restored)) as c:
        n = c.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    assert n >= 0
    # Integrity check still passes after migration.
    with sqlite3.connect(str(restored)) as c:
        result = c.execute("PRAGMA integrity_check").fetchall()
    assert result == [("ok",)], f"integrity broken after bootstrap: {result}"


# ---------------------------------------------------------------------------
# Every ALTER TABLE in bootstrap_schema is IF-COLUMN-NOT-EXISTS gated.
# ---------------------------------------------------------------------------

def test_bootstrap_uses_guarded_alters_only():
    """Static check: every ALTER TABLE ADD COLUMN in review_dashboard.py
    should be inside an `if col not in <cols>` block OR wrapped in a
    try/except OperationalError. Running ALTER TABLE on an existing
    column raises; unguarded ALTERs in bootstrap mean a second-run
    crash."""
    src = (ROOT / "scripts" / "review_dashboard.py").read_text().splitlines()
    offenders: list[int] = []
    for i, line in enumerate(src):
        if "ALTER TABLE" in line and "ADD COLUMN" in line:
            # Scan up to 20 lines back for a guard.
            window = "\n".join(src[max(0, i - 20):i + 1])
            guarded = (
                "not in " in window
                or "OperationalError" in window
                or "try:" in window
                or "IF NOT EXISTS" in line.upper()
            )
            if not guarded:
                offenders.append(i + 1)
    assert not offenders, (
        f"bootstrap_schema has unguarded ALTER TABLE ADD COLUMN lines: "
        f"{offenders} — rerun will crash"
    )


def test_bootstrap_create_table_all_use_if_not_exists():
    """Static check: every CREATE TABLE in bootstrap_schema should be
    IF NOT EXISTS. A plain CREATE TABLE would crash on a rerun."""
    src = (ROOT / "scripts" / "review_dashboard.py").read_text()
    # Only scan inside def bootstrap_schema.
    start = src.index("def bootstrap_schema")
    try:
        end = src.index("\ndef ", start + 1)
    except ValueError:
        end = len(src)
    body = src[start:end]
    # Count plain CREATE TABLE vs CREATE TABLE IF NOT EXISTS.
    import re
    plain = re.findall(r"CREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS)\w+", body,
                       re.IGNORECASE)
    assert not plain, (
        f"bootstrap_schema contains unguarded CREATE TABLE: {plain}"
    )
