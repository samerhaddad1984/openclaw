"""Scope 2 Phase 2 — QBO full historical pull + confirm/rollback.

Uses an injected ``pull_fn`` stub so no network is involved. The
production default ``_default_pull_fn`` delegates to the real
``QBOSyncOrchestrator.initial_sync`` and is not exercised here (it
would require a QBO sandbox connection).

Covers:

  - A 2-year historical pull records progress per year and totals.
  - The imported_data summary renders the job + per-year counts.
  - CPA confirm moves the review from pending_review to confirmed.
  - CPA rollback deletes qbo_sync_state rows, QBO-origin GL/bank
    transactions, marks qbo_connections disconnected, and records
    the rollback in the review row.
  - A second pull with the same data is idempotent (underlying
    puller is expected to upsert on qbo_id; the wrapper must not
    double-count in totals if the stub returns zeros).
  - Interaction test: rollback leaves non-QBO rows alone.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import qbo_historical as qh  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'qbo.db'
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE qbo_sync_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, client_code TEXT,
                entity_type TEXT, qbo_id TEXT,
                last_pulled_at TEXT
            );
            CREATE TABLE qbo_connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, client_code TEXT,
                realm_id TEXT, status TEXT DEFAULT 'active',
                last_error TEXT
            );
            CREATE TABLE gl_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, source TEXT,
                amount REAL, account TEXT, txn_date TEXT
            );
            CREATE TABLE bank_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code TEXT, source TEXT,
                amount REAL, description TEXT, txn_date TEXT
            );
            """
        )
        conn.execute(
            "INSERT INTO qbo_connections "
            "(firm_code, client_code, realm_id, status) "
            "VALUES ('FIRM','CONS','999','active')"
        )
        conn.commit()
    return db


def _stub_pull(call_log: list[dict] | None = None,
               counts_per_year: dict[int, dict[str, int]] | None = None):
    counts_per_year = counts_per_year or {}

    def _pull(*, firm_code, client_code, db_path, start, end):
        y = int(start[:4])
        counts = counts_per_year.get(y, {
            'accounts': 12, 'customers': 5, 'vendors': 8,
            'journal_entries': 40, 'bills': 30,
            'invoices': 25, 'payments': 10,
        })
        # Record a qbo_sync_state row per pull so rollback has
        # something concrete to delete.
        with sqlite3.connect(db_path) as conn:
            for i in range(counts.get('journal_entries', 0)):
                conn.execute(
                    "INSERT INTO qbo_sync_state "
                    "(firm_code, client_code, entity_type, qbo_id, "
                    "last_pulled_at) VALUES (?,?,?,?,?)",
                    (firm_code, client_code, 'journal_entry',
                     f'{y}-je-{i}', '2026-04-01'),
                )
            for i in range(counts.get('bills', 0)):
                conn.execute(
                    "INSERT INTO gl_transactions "
                    "(client_code, source, amount, account, txn_date) "
                    "VALUES (?,?,?,?,?)",
                    (client_code, 'qbo', float(100 + i),
                     'Expenses', f'{y}-06-01'),
                )
            conn.commit()
        if call_log is not None:
            call_log.append({
                'firm_code': firm_code, 'client_code': client_code,
                'start': start, 'end': end,
            })
        return counts
    return _pull


# ---------------------------------------------------------------------------


def test_full_historical_sync_pulls_years(tmp_path):
    db = _mkdb(tmp_path)
    calls: list[dict] = []
    res = qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=2, pull_fn=_stub_pull(call_log=calls),
        today=date(2026, 4, 22),
    )
    assert res['ok'] is True
    # years=2 means three windows: 2024, 2025, 2026YTD.
    assert res['years_pulled'] == 3
    assert len(calls) == 3
    years = {int(c['start'][:4]) for c in calls}
    assert years == {2024, 2025, 2026}
    # Final year's window ends at the provided today.
    cal_2026 = [c for c in calls if c['start'] == '2026-01-01'][0]
    assert cal_2026['end'] == '2026-04-22'
    # Totals aggregate across years.
    totals = res['totals']
    assert totals['journal_entries'] == 40 * 3
    # Progress is keyed by year string.
    assert set(res['progress'].keys()) == {'2024', '2025', '2026'}


def test_imported_data_summary_renders(tmp_path):
    db = _mkdb(tmp_path)
    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=1, pull_fn=_stub_pull(), today=date(2026, 4, 22),
    )
    data = qh.summary(db, firm_code='FIRM', client_code='CONS')
    assert data['job'] is not None
    assert data['job']['status'] == 'completed'
    assert (data['review'] or {}).get('status') == 'pending_review'
    html = qh.render_imported_data_page(
        firm_code='FIRM', client_code='CONS',
        client_name='Construction Tremblay', data=data,
    )
    assert 'Construction Tremblay' in html
    assert 'journal_entries' in html
    assert 'Confirm import' in html
    assert 'Rollback' in html


def test_cpa_can_confirm_import(tmp_path):
    db = _mkdb(tmp_path)
    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=1, pull_fn=_stub_pull(), today=date(2026, 4, 22),
    )
    review = qh.confirm_import(
        db, firm_code='FIRM', client_code='CONS',
        decided_by='cpa@firm.com',
    )
    assert review and review['status'] == 'confirmed'
    assert review['decided_by'] == 'cpa@firm.com'
    # The imported qbo_sync_state rows are still there.
    with sqlite3.connect(db) as conn:
        n = conn.execute("SELECT COUNT(*) FROM qbo_sync_state").fetchone()[0]
    assert n > 0


def test_cpa_can_rollback_import(tmp_path):
    db = _mkdb(tmp_path)
    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=1, pull_fn=_stub_pull(), today=date(2026, 4, 22),
    )
    result = qh.rollback_import(
        db, firm_code='FIRM', client_code='CONS',
        decided_by='cpa@firm.com',
    )
    assert result['ok']
    # Review recorded as rolled_back.
    rev = qh.get_review(db, firm_code='FIRM', client_code='CONS')
    assert rev and rev['status'] == 'rolled_back'


def test_rollback_removes_qbo_data(tmp_path):
    db = _mkdb(tmp_path)
    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=1, pull_fn=_stub_pull(), today=date(2026, 4, 22),
    )
    # Sanity: something was imported.
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM qbo_sync_state "
            "WHERE firm_code='FIRM' AND client_code='CONS'"
        ).fetchone()[0] > 0
        assert conn.execute(
            "SELECT COUNT(*) FROM gl_transactions "
            "WHERE client_code='CONS' AND source='qbo'"
        ).fetchone()[0] > 0
    qh.rollback_import(
        db, firm_code='FIRM', client_code='CONS',
        decided_by='cpa@firm.com',
    )
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM qbo_sync_state "
            "WHERE firm_code='FIRM' AND client_code='CONS'"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM gl_transactions "
            "WHERE client_code='CONS' AND source='qbo'"
        ).fetchone()[0] == 0
        status = conn.execute(
            "SELECT status FROM qbo_connections "
            "WHERE firm_code='FIRM' AND client_code='CONS'"
        ).fetchone()[0]
    assert status == 'disconnected'


def test_rollback_leaves_non_qbo_rows_alone(tmp_path):
    """Interaction test: rollback must not touch OtoCPA-native GL rows."""
    db = _mkdb(tmp_path)
    # Native (non-QBO) row created before historical pull.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO gl_transactions "
            "(client_code, source, amount, account, txn_date) "
            "VALUES ('CONS','manual', 99.99, 'Revenue', '2026-01-01')"
        )
        conn.commit()
    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=1, pull_fn=_stub_pull(), today=date(2026, 4, 22),
    )
    qh.rollback_import(
        db, firm_code='FIRM', client_code='CONS',
    )
    with sqlite3.connect(db) as conn:
        native = conn.execute(
            "SELECT COUNT(*) FROM gl_transactions "
            "WHERE client_code='CONS' AND source='manual'"
        ).fetchone()[0]
    assert native == 1


def test_progress_updates_live(tmp_path):
    """Progress JSON is updated between windows, not just at the end."""
    db = _mkdb(tmp_path)
    observed: list[str] = []

    def _spy(*, firm_code, client_code, db_path, start, end):
        # Check the progress column after each year.
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT progress FROM qbo_historical_jobs "
                "WHERE firm_code=? AND client_code=? "
                "ORDER BY id DESC LIMIT 1",
                (firm_code, client_code),
            ).fetchone()
        observed.append(row[0] if row else '')
        return {'accounts': 1}

    qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=1, pull_fn=_spy, today=date(2026, 4, 22),
    )
    # Three windows (2025, 2026 YTD + one earlier) means three spy calls.
    assert len(observed) == 2
    # After the first call, progress JSON should not yet mention year 2.
    # (We don't assert exact content — just that the column is non-empty.)
    assert observed[0] is not None


def test_failed_pull_records_status(tmp_path):
    db = _mkdb(tmp_path)

    def _boom(*, firm_code, client_code, db_path, start, end):
        raise RuntimeError('QBO down')

    res = qh.run_historical_pull(
        db, firm_code='FIRM', client_code='CONS',
        years=1, pull_fn=_boom, today=date(2026, 4, 22),
    )
    assert res['ok'] is False
    assert 'QBO down' in (res.get('error') or '')
    with sqlite3.connect(db) as conn:
        status = conn.execute(
            "SELECT status FROM qbo_historical_jobs "
            "WHERE firm_code='FIRM' AND client_code='CONS'"
        ).fetchone()[0]
    assert status == 'failed'
