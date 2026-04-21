"""Cleanup Item 6: daily maintenance cron prunes stale rows."""
from __future__ import annotations

import sqlite3
import stat
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.maintenance import cleanup as mc  # noqa: E402


CRON_FILE = Path('/etc/cron.d/otocpa-maintenance')


def _mkdb(tmp_path):
    db = tmp_path / 'maint.db'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE wizard_posting_attempts (
                request_id TEXT PRIMARY KEY,
                firm_code TEXT, client_code TEXT, period_end TEXT,
                started_at TEXT, completed_at TEXT, result_json TEXT
            );
            CREATE TABLE rate_limit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT NOT NULL, created_at TEXT NOT NULL
            );
            CREATE TABLE client_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, kind TEXT, title TEXT, body TEXT,
                status TEXT, sent_at TEXT, created_at TEXT
            );
            CREATE TABLE impersonation_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT, action TEXT, at TEXT
            );
            CREATE TABLE client_portal_user_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portal_user_id INTEGER, firm_code TEXT, client_code TEXT,
                actor_email TEXT, action TEXT, detail TEXT,
                ip TEXT, user_agent TEXT, created_at TEXT
            );
            CREATE TABLE accrual_line_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, client_code TEXT, period TEXT,
                kind TEXT, line_key TEXT,
                suggested_amount REAL, final_amount REAL,
                included INTEGER, actor_email TEXT,
                notes TEXT, entry_id TEXT, created_at TEXT
            );
        """)
    return db


def _days_ago(days):
    return (datetime.now(timezone.utc) - timedelta(days=days)
            ).replace(microsecond=0).isoformat()


def _hours_ago(h):
    return (datetime.now(timezone.utc) - timedelta(hours=h)
            ).replace(microsecond=0).isoformat()


def test_cleanup_removes_old_wizard_attempts(tmp_path):
    db = _mkdb(tmp_path)
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO wizard_posting_attempts "
                   "(request_id, started_at) VALUES ('old', ?)",
                   (_days_ago(100),))
        c.execute("INSERT INTO wizard_posting_attempts "
                   "(request_id, started_at) VALUES ('keep', ?)",
                   (_days_ago(30),))
        c.commit()
    mc.run_cleanup(db)
    with sqlite3.connect(db) as c:
        remaining = {r[0] for r in c.execute(
            "SELECT request_id FROM wizard_posting_attempts"
        ).fetchall()}
    assert 'keep' in remaining
    assert 'old' not in remaining


def test_cleanup_preserves_recent_rate_limits(tmp_path):
    db = _mkdb(tmp_path)
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO rate_limit_events (key, created_at) "
                   "VALUES ('old', ?)", (_hours_ago(2),))
        c.execute("INSERT INTO rate_limit_events (key, created_at) "
                   "VALUES ('fresh', ?)", (_hours_ago(0),))
        c.commit()
    mc.run_cleanup(db)
    with sqlite3.connect(db) as c:
        keys = {r[0] for r in c.execute(
            "SELECT key FROM rate_limit_events"
        ).fetchall()}
    assert 'fresh' in keys
    assert 'old' not in keys


def test_cleanup_respects_retention_per_status(tmp_path):
    db = _mkdb(tmp_path)
    # Sent: retained 180 days. Failed: retained 30 days.
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO client_notifications "
                   "(client_code, kind, title, body, status, sent_at, created_at) "
                   "VALUES ('C','k','t','b','sent', ?, ?)",
                   (_days_ago(200), _days_ago(200)))  # should prune
        c.execute("INSERT INTO client_notifications "
                   "(client_code, kind, title, body, status, sent_at, created_at) "
                   "VALUES ('C','k','t','b','sent', ?, ?)",
                   (_days_ago(100), _days_ago(100)))  # keep (<180d)
        c.execute("INSERT INTO client_notifications "
                   "(client_code, kind, title, body, status, created_at) "
                   "VALUES ('C','k','t','b','failed', ?)",
                   (_days_ago(45),))  # failed, >30d → prune
        c.execute("INSERT INTO client_notifications "
                   "(client_code, kind, title, body, status, created_at) "
                   "VALUES ('C','k','t','b','failed', ?)",
                   (_days_ago(5),))  # failed, <30d → keep
        c.commit()
    mc.run_cleanup(db)
    with sqlite3.connect(db) as c:
        rows = c.execute(
            "SELECT status, sent_at, created_at FROM client_notifications"
        ).fetchall()
    statuses = [r[0] for r in rows]
    # Expect: one old sent pruned, one recent sent kept,
    # one old failed pruned, one recent failed kept.
    assert statuses.count('sent') == 1
    assert statuses.count('failed') == 1


def test_cleanup_missing_table_logs_warning_not_raise(tmp_path, caplog):
    import logging
    db = tmp_path / 'empty.db'
    with sqlite3.connect(db) as conn:
        pass  # no tables created
    with caplog.at_level(logging.WARNING):
        results = mc.run_cleanup(db)
    assert isinstance(results, dict)
    # Every entry should be 0 (tables missing → skip)
    assert all(v == 0 for v in results.values())
    # Some "skip X: table missing" warnings logged
    assert any('table missing' in r.message for r in caplog.records)


def test_cleanup_preserves_recent_wizard_attempts(tmp_path):
    db = _mkdb(tmp_path)
    with sqlite3.connect(db) as c:
        for days, rid in ((10, 'r10'), (50, 'r50'), (89, 'r89')):
            c.execute("INSERT INTO wizard_posting_attempts "
                       "(request_id, started_at) VALUES (?, ?)",
                       (rid, _days_ago(days)))
        c.commit()
    mc.run_cleanup(db)
    with sqlite3.connect(db) as c:
        remaining = {r[0] for r in c.execute(
            "SELECT request_id FROM wizard_posting_attempts"
        ).fetchall()}
    assert remaining == {'r10', 'r50', 'r89'}


def test_cleanup_script_runs_end_to_end(tmp_path, monkeypatch):
    db = _mkdb(tmp_path)
    monkeypatch.setattr(mc, 'DB_PATH', db)
    # main() shouldn't raise and returns 0.
    assert mc.main() == 0


def test_cleanup_logs_summary(tmp_path, capsys, monkeypatch):
    db = _mkdb(tmp_path)
    monkeypatch.setattr(mc, 'DB_PATH', db)
    mc.main()
    out = capsys.readouterr().out
    assert '[maintenance] deleted=' in out
    assert 'wizard_posting_attempts:90d' in out
    assert 'rate_limit_events:1h' in out


@pytest.mark.skipif(not CRON_FILE.exists(),
                     reason='maintenance cron not installed on this host')
def test_cron_file_installed_correctly():
    assert CRON_FILE.stat().st_uid == 0
    mode = stat.S_IMODE(CRON_FILE.stat().st_mode)
    assert mode == 0o644
    content = CRON_FILE.read_text()
    assert 'deploy' in content
    assert 'cleanup.py' in content
    assert '0 3 * * *' in content  # daily at 03:00
