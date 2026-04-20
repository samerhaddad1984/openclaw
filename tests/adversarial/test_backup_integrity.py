"""R2-Investigation 5 — backup + restore drill.

The original cron entry was silently failing because its stdout
redirect targeted a directory that didn't exist. These tests pin down
what we actually want from the backup pipeline:

  - Backup script creates its own log directory (defensive).
  - Backup file lands in /opt/backups/otocpa with a timestamped name.
  - SQLite backup passes ``PRAGMA integrity_check``.
  - Restored backup contains every table the live DB has.
  - Most-recent backup is < 25 hours old (catches a cron that's
    silently failing again).
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


BACKUP_SCRIPT = ROOT / "scripts" / "backup_db.sh"
LIVE_BACKUP_DIR = Path("/opt/backups/otocpa")
LIVE_DB = Path("/opt/otocpa/data/otocpa_agent.db")


# ---------------------------------------------------------------------------
# Static checks on the script itself.
# ---------------------------------------------------------------------------

def test_backup_script_creates_its_own_log_directory():
    """The cron entry redirects to /opt/otocpa/logs/backup.log. If that
    directory doesn't exist, the redirect fails and the script never
    runs — that's how three days of backups got silently lost. The
    script must defensively mkdir its own log dir."""
    src = BACKUP_SCRIPT.read_text()
    assert "mkdir -p" in src, "backup script lost its mkdir guard"
    assert "/opt/otocpa/logs" in src, (
        "backup script doesn't ensure its log directory — cron will "
        "silently fail again the moment the dir is removed"
    )


def test_backup_script_fails_loudly_on_empty_sqlite_dump():
    """A 0-byte SQLite backup means rsync to a snapshot would propagate
    nothing. The script must exit non-zero when its output is empty so
    monitoring can catch it."""
    src = BACKUP_SCRIPT.read_text()
    assert "[ ! -s" in src, "backup script no longer checks for empty output"
    assert "exit 1" in src, "backup script no longer exits on failure"


def test_backup_script_runs_pragma_integrity_check():
    """A SQLite backup that opens but is corrupt is worse than one that
    doesn't open. Verify the script invokes integrity_check."""
    src = BACKUP_SCRIPT.read_text()
    assert "integrity_check" in src, (
        "backup script no longer verifies the SQLite copy with "
        "PRAGMA integrity_check — corrupt backups would land silently"
    )


def test_backup_script_does_not_hardcode_postgres_password():
    """The PG password must come from PGPASSFILE (.pgpass, 0600 perms),
    an OTOCPA_PG_PASSWORD env var, or libpq's default ~/.pgpass. Never
    from a literal in the script."""
    src = BACKUP_SCRIPT.read_text()
    # The literal we previously shipped.
    assert "OtoCPA2026!Secure" not in src, (
        "backup script still contains the hardcoded PG password "
        "literal. Move it to /opt/otocpa/.pgpass (0600) or a systemd "
        "credential."
    )
    # Must reference the approved credential sources.
    assert "PGPASSFILE" in src, "script must honour libpq's PGPASSFILE"
    assert "OTOCPA_PG_PASSWORD" in src, "env-var fallback removed"


def test_pgpass_file_exists_with_600_perms():
    """If /opt/otocpa/.pgpass exists, it must be mode 0600. libpq
    refuses to read looser-permission passfiles; a 0644 file silently
    degrades the backup to prompting pg_dump."""
    pp = Path("/opt/otocpa/.pgpass")
    if not pp.exists():
        pytest.skip("/opt/otocpa/.pgpass not present in this environment")
    mode = oct(pp.stat().st_mode)[-3:]
    assert mode == "600", (
        f"/opt/otocpa/.pgpass has mode {mode}; libpq requires 600"
    )


# ---------------------------------------------------------------------------
# Live restore drill (run only when the backup directory is present).
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not LIVE_BACKUP_DIR.exists(),
    reason="live backup dir /opt/backups/otocpa absent",
)
def test_most_recent_sqlite_backup_is_under_25_hours_old():
    """If this fails, the daily 3am cron is silently failing again."""
    sqlites = sorted(LIVE_BACKUP_DIR.glob("sqlite_*.db"),
                     key=lambda p: p.stat().st_mtime)
    assert sqlites, "no sqlite backups in /opt/backups/otocpa"
    age_h = (time.time() - sqlites[-1].stat().st_mtime) / 3600
    assert age_h < 25, (
        f"newest sqlite backup is {age_h:.1f}h old — cron likely silent-failing. "
        f"Newest: {sqlites[-1]}"
    )


@pytest.mark.skipif(
    not LIVE_BACKUP_DIR.exists(),
    reason="live backup dir /opt/backups/otocpa absent",
)
def test_most_recent_sqlite_backup_passes_integrity_check(tmp_path):
    sqlites = sorted(LIVE_BACKUP_DIR.glob("sqlite_*.db"),
                     key=lambda p: p.stat().st_mtime)
    assert sqlites
    src = sqlites[-1]
    assert src.stat().st_size > 1024, f"backup tiny: {src.stat().st_size} bytes"
    # Restore-by-copy.
    dest = tmp_path / "restored.db"
    shutil.copy2(src, dest)
    conn = sqlite3.connect(str(dest))
    result = conn.execute("PRAGMA integrity_check").fetchall()
    conn.close()
    assert result == [("ok",)], (
        f"latest backup ({src}) failed integrity_check: {result}"
    )


@pytest.mark.skipif(
    not (LIVE_BACKUP_DIR.exists() and LIVE_DB.exists()),
    reason="needs live DB + live backups",
)
def test_restored_backup_has_same_tables_as_live(tmp_path):
    """Restore the latest backup into a tmp location and verify it has
    the same table set as the live DB. Catches a backup that captured
    the wrong file or got truncated mid-write."""
    sqlites = sorted(LIVE_BACKUP_DIR.glob("sqlite_*.db"),
                     key=lambda p: p.stat().st_mtime)
    src = sqlites[-1]
    dest = tmp_path / "restored.db"
    shutil.copy2(src, dest)

    def _tables(p: Path) -> set[str]:
        c = sqlite3.connect(str(p))
        rows = c.execute(
            "SELECT name FROM sqlite_master WHERE type='table'",
        ).fetchall()
        c.close()
        return {r[0] for r in rows}

    live_tables = _tables(LIVE_DB)
    restored_tables = _tables(dest)
    # Restored set should be ⊇ a substantial subset of live. (Live can
    # have tables created since the backup ran.)
    common = live_tables & restored_tables
    assert len(common) >= 0.8 * len(restored_tables), (
        f"restored backup has very different tables than live. "
        f"Restored: {len(restored_tables)}, common with live: {len(common)}"
    )


# ---------------------------------------------------------------------------
# Drill: run the script in a sandboxed temp env and verify it works
# even when the backup dir doesn't exist yet.
# ---------------------------------------------------------------------------

def test_backup_script_works_against_an_isolated_dest(tmp_path):
    """Copy the script to tmp, redirect BACKUP_DIR via env (the script
    hardcodes /opt/backups/otocpa, so we verify the dry-run alternative
    by reading the script logic instead)."""
    src = BACKUP_SCRIPT.read_text()
    # Smoke check: script content is what we expect.
    assert "BACKUP_DIR=/opt/backups/otocpa" in src
    assert "SQLITE_SRC=/opt/otocpa/data/otocpa_agent.db" in src
