"""Hybrid assignment phase 7: team workload report.

Covers get_team_workload — the per-employee roll-up that backs
/reports/team_workload and the owner-dashboard widget.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.team_workload import get_team_workload  # noqa: E402
from src.integrations.review_workflow import (  # noqa: E402
    approve,
    assign,
    ensure_review_schema,
    submit_for_review,
)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _days_ago_iso(days: int) -> str:
    return (datetime.now(timezone.utc)
             - timedelta(days=days)).replace(microsecond=0).isoformat()


def _seed(db_path: Path) -> None:
    ensure_review_schema(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("""
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY,
                firm_code   TEXT,
                primary_employee_email   TEXT,
                secondary_employee_email TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE dashboard_users (
                username TEXT PRIMARY KEY, email TEXT,
                display_name TEXT, role TEXT, firm_code TEXT,
                active INTEGER DEFAULT 1
            )
        """)
        conn.executemany(
            "INSERT INTO dashboard_users "
            "(username,email,display_name,role,firm_code,active) "
            "VALUES (?,?,?,?,?,1)",
            [('sophie@f1.com', 'sophie@f1.com', 'Sophie',
              'employee', 'F1'),
             ('jean@f1.com',   'jean@f1.com',   'Jean',
              'employee', 'F1'),
             ('marie@f1.com',  'marie@f1.com',  'Marie',
              'firm_admin', 'F1'),
             ('ghost@f2.com',  'ghost@f2.com',  'Ghost',
              'employee', 'F2')],
        )
        conn.executemany(
            "INSERT INTO clients (client_code, firm_code, "
            " primary_employee_email, secondary_employee_email) "
            "VALUES (?,?,?,?)",
            [('TREMBLAY', 'F1', 'sophie@f1.com', 'jean@f1.com'),
             ('CAFE',     'F1', 'jean@f1.com',   'sophie@f1.com'),
             ('MARCHAND', 'F1', 'marie@f1.com',  None),
             ('OTHER',    'F2', 'ghost@f2.com',  None)],
        )
        conn.commit()
    finally:
        conn.close()
    # Assign some docs: Sophie has 2 open, Jean has 1 open + 1 approved
    # today, Marie has no docs.
    assign(db_path, firm_code='F1', entity_type='document', entity_id='D1',
            assignee_email='sophie@f1.com',
            actor_email='marie@f1.com', actor_role='firm_admin')
    assign(db_path, firm_code='F1', entity_type='document', entity_id='D2',
            assignee_email='sophie@f1.com',
            actor_email='marie@f1.com', actor_role='firm_admin')
    assign(db_path, firm_code='F1', entity_type='document', entity_id='D3',
            assignee_email='jean@f1.com',
            actor_email='marie@f1.com', actor_role='firm_admin')
    # Jean: D3 open; simulate completion of D4 by approving it.
    assign(db_path, firm_code='F1', entity_type='document', entity_id='D4',
            assignee_email='jean@f1.com',
            actor_email='marie@f1.com', actor_role='firm_admin')
    submit_for_review(db_path, firm_code='F1', entity_type='document',
                        entity_id='D4', actor_email='jean@f1.com',
                        actor_role='employee')
    approve(db_path, firm_code='F1', entity_type='document', entity_id='D4',
             actor_email='marie@f1.com', actor_role='firm_admin')


@pytest.fixture
def db(tmp_path):
    p = tmp_path / 'wl.db'
    _seed(p)
    return p


def test_workload_aggregates_per_employee(db):
    rows = get_team_workload(db, firm_code='F1')
    by_email = {r['email']: r for r in rows}
    assert 'sophie@f1.com' in by_email
    sophie = by_email['sophie@f1.com']
    assert sophie['primary_clients'] == 1  # TREMBLAY
    assert sophie['secondary_clients'] == 1  # CAFE
    assert sophie['open_docs'] == 2  # D1, D2
    jean = by_email['jean@f1.com']
    assert jean['primary_clients'] == 1  # CAFE
    assert jean['secondary_clients'] == 1  # TREMBLAY
    assert jean['open_docs'] == 1  # D3 (D4 is approved)
    assert jean['completed_this_week'] == 1  # D4 just approved


def test_workload_respects_firm_isolation(db):
    rows_f1 = get_team_workload(db, firm_code='F1')
    emails = {r['email'] for r in rows_f1}
    assert 'ghost@f2.com' not in emails  # other firm
    rows_f2 = get_team_workload(db, firm_code='F2')
    f2_emails = {r['email'] for r in rows_f2}
    assert f2_emails == {'ghost@f2.com'}


def test_workload_owner_view_crosses_firms(db):
    rows = get_team_workload(db, firm_code=None)
    emails = {r['email'] for r in rows}
    # Both firms' members appear.
    assert {'sophie@f1.com', 'jean@f1.com',
             'marie@f1.com', 'ghost@f2.com'} <= emails


def test_workload_sorted_by_open_docs(db):
    rows = get_team_workload(db, firm_code='F1')
    # Sophie has 2 open -> she should appear before Jean (1 open)
    sophie_idx = next(i for i, r in enumerate(rows)
                        if r['email'] == 'sophie@f1.com')
    jean_idx = next(i for i, r in enumerate(rows)
                      if r['email'] == 'jean@f1.com')
    assert sophie_idx < jean_idx


def test_inactive_users_excluded(db):
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("INSERT INTO dashboard_users "
                      "(username,email,display_name,role,firm_code,active) "
                      "VALUES ('x@f1.com','x@f1.com','X','employee','F1',0)")
        conn.commit()
    finally:
        conn.close()
    rows = get_team_workload(db, firm_code='F1')
    emails = {r['email'] for r in rows}
    assert 'x@f1.com' not in emails


def test_admin_with_no_queue_shows_zero_open(db):
    """Marie is firm_admin — she approves (doesn't hold) but still
    carries MARCHAND as primary."""
    rows = get_team_workload(db, firm_code='F1')
    marie = next(r for r in rows if r['email'] == 'marie@f1.com')
    assert marie['open_docs'] == 0
    # Marie performed the approval on D4, so completed_this_week
    # counts it toward her throughput.
    assert marie['completed_this_week'] == 1
    assert marie['primary_clients'] == 1  # still primary on MARCHAND


def test_shared_docs_not_double_counted(db):
    """A doc assigned to one person shouldn't inflate the other
    person's open count, even when they're secondary on the client."""
    rows = get_team_workload(db, firm_code='F1')
    # Sophie is secondary on CAFE; D3 is on CAFE and assigned to Jean.
    # D3 must NOT count toward Sophie's open_docs (only assigned_to_email
    # drives open_docs).
    sophie = next(r for r in rows if r['email'] == 'sophie@f1.com')
    assert sophie['open_docs'] == 2  # D1, D2 only — not D3
