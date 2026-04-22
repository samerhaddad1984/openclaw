"""Hybrid assignment phase 4: client assignment UI helpers + audit trail.

Covers the backend layer that the /clients/edit page + /clients/assignment
endpoint sit on top of:

- Owner / firm_admin can update client assignment.
- Employees CANNOT (defensive role check at the helper layer).
- assignment_updated_at + assignment_updated_by are stamped.
- client_assignment_history is appended on every change with the
  delta (previous vs new) and reason.
- list_clients_with_assignment supports the filter dropdown
  (mine / pool / by employee).
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.client_assignment import (  # noqa: E402
    ClientAssignmentError,
    get_assignment_history,
    get_client_assignment,
    get_firm_employees,
    list_clients_with_assignment,
    update_client_assignment,
)


def _bootstrap(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                client_code               TEXT PRIMARY KEY,
                client_name               TEXT,
                firm_code                 TEXT,
                primary_employee_email    TEXT,
                secondary_employee_email  TEXT,
                assignment_updated_at     TEXT,
                assignment_updated_by     TEXT,
                active                    INTEGER DEFAULT 1
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_users (
                username      TEXT PRIMARY KEY,
                email         TEXT,
                display_name  TEXT,
                firm_code     TEXT,
                role          TEXT DEFAULT 'employee',
                active        INTEGER NOT NULL DEFAULT 1
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS client_assignment_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code           TEXT NOT NULL,
                client_code         TEXT NOT NULL,
                action              TEXT NOT NULL,
                previous_primary    TEXT,
                new_primary         TEXT,
                previous_secondary  TEXT,
                new_secondary       TEXT,
                reason              TEXT,
                changed_by          TEXT,
                changed_at          TEXT
            )
        """)
        conn.executemany(
            "INSERT INTO clients (client_code, client_name, firm_code) "
            "VALUES (?,?,?)",
            [
                ('TREMBLAY', 'Tremblay Inc', 'F1'),
                ('CAFE',     'Cafe Centro',  'F1'),
                ('MARCHAND', 'Marchand SA',  'F1'),
                ('OUTSIDER', 'OutOfFirm',    'F2'),
            ],
        )
        conn.executemany(
            "INSERT INTO dashboard_users "
            "(username, email, display_name, firm_code, role, active) "
            "VALUES (?,?,?,?,?,1)",
            [
                ('sam@f1.com',    'sam@f1.com',    'Sam',    'F1', 'owner'),
                ('marie@f1.com',  'marie@f1.com',  'Marie',  'F1', 'firm_admin'),
                ('sophie@f1.com', 'sophie@f1.com', 'Sophie', 'F1', 'employee'),
                ('jean@f1.com',   'jean@f1.com',   'Jean',   'F1', 'employee'),
                ('inactive@f1.com', 'inactive@f1.com', 'Inactive',
                 'F1', 'employee'),
            ],
        )
        conn.execute("UPDATE dashboard_users SET active=0 "
                      "WHERE email='inactive@f1.com'")
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def db(tmp_path):
    p = tmp_path / 'a.db'
    _bootstrap(p)
    return p


# ---------------------------------------------------------------------------
# Role gating
# ---------------------------------------------------------------------------


def test_owner_can_assign_client_to_employee(db):
    res = update_client_assignment(
        db, firm_code='F1', client_code='TREMBLAY',
        primary_email='sophie@f1.com', secondary_email='jean@f1.com',
        changed_by='sam@f1.com', actor_role='owner',
    )
    assert res['changed'] is True
    cur = get_client_assignment(db, firm_code='F1', client_code='TREMBLAY')
    assert cur['primary_employee_email'] == 'sophie@f1.com'
    assert cur['secondary_employee_email'] == 'jean@f1.com'
    assert cur['assignment_updated_by'] == 'sam@f1.com'
    assert cur['assignment_updated_at']


def test_firm_admin_can_assign_client(db):
    res = update_client_assignment(
        db, firm_code='F1', client_code='CAFE',
        primary_email='jean@f1.com', secondary_email=None,
        changed_by='marie@f1.com', actor_role='firm_admin',
    )
    assert res['changed'] is True


def test_employee_cannot_reassign_clients(db):
    with pytest.raises(ClientAssignmentError):
        update_client_assignment(
            db, firm_code='F1', client_code='TREMBLAY',
            primary_email='sophie@f1.com', secondary_email=None,
            changed_by='sophie@f1.com', actor_role='employee',
        )


def test_primary_and_secondary_must_differ(db):
    with pytest.raises(ClientAssignmentError):
        update_client_assignment(
            db, firm_code='F1', client_code='TREMBLAY',
            primary_email='sophie@f1.com',
            secondary_email='SOPHIE@f1.COM',  # case-insensitive collision
            changed_by='sam@f1.com', actor_role='owner',
        )


def test_unknown_client_raises(db):
    with pytest.raises(ClientAssignmentError):
        update_client_assignment(
            db, firm_code='F1', client_code='GHOST',
            primary_email='sophie@f1.com', secondary_email=None,
            changed_by='sam@f1.com', actor_role='owner',
        )


# ---------------------------------------------------------------------------
# Audit trail
# ---------------------------------------------------------------------------


def test_assignment_history_logged(db):
    update_client_assignment(
        db, firm_code='F1', client_code='TREMBLAY',
        primary_email='sophie@f1.com', secondary_email=None,
        changed_by='sam@f1.com', actor_role='owner',
        reason='initial onboarding',
    )
    history = get_assignment_history(db, firm_code='F1',
                                       client_code='TREMBLAY')
    assert len(history) == 1
    h = history[0]
    assert h['action'] == 'assigned'
    assert h['previous_primary'] is None
    assert h['new_primary'] == 'sophie@f1.com'
    assert h['reason'] == 'initial onboarding'
    assert h['changed_by'] == 'sam@f1.com'


def test_assignment_history_classifies_reassign(db):
    update_client_assignment(
        db, firm_code='F1', client_code='TREMBLAY',
        primary_email='sophie@f1.com', secondary_email=None,
        changed_by='sam@f1.com', actor_role='owner',
    )
    update_client_assignment(
        db, firm_code='F1', client_code='TREMBLAY',
        primary_email='jean@f1.com', secondary_email='sophie@f1.com',
        changed_by='marie@f1.com', actor_role='firm_admin',
        reason='Sophie on leave',
    )
    hist = get_assignment_history(db, firm_code='F1',
                                    client_code='TREMBLAY')
    assert hist[0]['action'] == 'reassigned'
    assert hist[0]['previous_primary'] == 'sophie@f1.com'
    assert hist[0]['new_primary'] == 'jean@f1.com'
    assert hist[0]['reason'] == 'Sophie on leave'


def test_assignment_history_classifies_unassign(db):
    update_client_assignment(
        db, firm_code='F1', client_code='TREMBLAY',
        primary_email='sophie@f1.com', secondary_email=None,
        changed_by='sam@f1.com', actor_role='owner',
    )
    update_client_assignment(
        db, firm_code='F1', client_code='TREMBLAY',
        primary_email=None, secondary_email=None,
        changed_by='sam@f1.com', actor_role='owner',
    )
    hist = get_assignment_history(db, firm_code='F1',
                                    client_code='TREMBLAY')
    assert hist[0]['action'] == 'unassigned'


def test_no_change_skips_history(db):
    update_client_assignment(
        db, firm_code='F1', client_code='TREMBLAY',
        primary_email='sophie@f1.com', secondary_email=None,
        changed_by='sam@f1.com', actor_role='owner',
    )
    res = update_client_assignment(
        db, firm_code='F1', client_code='TREMBLAY',
        primary_email='sophie@f1.com', secondary_email=None,
        changed_by='sam@f1.com', actor_role='owner',
    )
    assert res['changed'] is False
    hist = get_assignment_history(db, firm_code='F1',
                                    client_code='TREMBLAY')
    assert len(hist) == 1  # only the original change recorded


# ---------------------------------------------------------------------------
# Listing + filter helpers (used by /clients dropdown)
# ---------------------------------------------------------------------------


def test_get_firm_employees_returns_only_active_in_firm(db):
    emps = get_firm_employees(db, 'F1')
    emails = {e['email'] for e in emps}
    assert 'sophie@f1.com' in emails
    assert 'inactive@f1.com' not in emails  # inactive filtered
    # Outsider firm
    assert get_firm_employees(db, 'F2') == []


def test_list_filter_mine(db):
    update_client_assignment(
        db, firm_code='F1', client_code='TREMBLAY',
        primary_email='sophie@f1.com', secondary_email='jean@f1.com',
        changed_by='sam@f1.com', actor_role='owner',
    )
    update_client_assignment(
        db, firm_code='F1', client_code='CAFE',
        primary_email='jean@f1.com', secondary_email=None,
        changed_by='sam@f1.com', actor_role='owner',
    )
    sophie = list_clients_with_assignment(db, firm_code='F1',
                                           employee_email='sophie@f1.com')
    codes = {c['client_code'] for c in sophie}
    assert codes == {'TREMBLAY'}  # primary on TREMBLAY only
    jean = list_clients_with_assignment(db, firm_code='F1',
                                         employee_email='jean@f1.com')
    jcodes = {c['client_code'] for c in jean}
    assert jcodes == {'TREMBLAY', 'CAFE'}


def test_list_filter_pool(db):
    update_client_assignment(
        db, firm_code='F1', client_code='TREMBLAY',
        primary_email='sophie@f1.com', secondary_email=None,
        changed_by='sam@f1.com', actor_role='owner',
    )
    pool = list_clients_with_assignment(db, firm_code='F1',
                                         only_unassigned=True)
    pool_codes = {c['client_code'] for c in pool}
    # CAFE + MARCHAND have no primary/secondary set
    assert pool_codes == {'CAFE', 'MARCHAND'}


def test_firm_isolation_other_firm_invisible(db):
    """Owner of F1 cannot poke F2's clients via update_client_assignment."""
    with pytest.raises(ClientAssignmentError):
        update_client_assignment(
            db, firm_code='F1', client_code='OUTSIDER',
            primary_email='sophie@f1.com', secondary_email=None,
            changed_by='sam@f1.com', actor_role='owner',
        )
