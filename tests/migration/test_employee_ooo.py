"""Scope 3.2 — employee OOO + coverage + firm-departure rebalancing."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import employee_ooo as ooo  # noqa: E402


@pytest.fixture()
def db(tmp_path):
    db_path = tmp_path / 'ooo.db'
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE dashboard_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT, firm_code TEXT, role TEXT,
                active INTEGER DEFAULT 1
            );
            CREATE TABLE clients (
                client_code TEXT PRIMARY KEY,
                client_name TEXT, firm_code TEXT,
                primary_employee_email TEXT,
                secondary_employee_email TEXT,
                status TEXT DEFAULT 'active'
            );
            CREATE TABLE review_workflow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_code TEXT, entity_type TEXT, entity_id TEXT,
                status TEXT, assigned_to_email TEXT,
                priority TEXT DEFAULT 'normal',
                assigned_at TEXT
            );
            INSERT INTO dashboard_users (email, firm_code, role, active)
            VALUES
              ('sam@f.com','FIRM','employee',1),
              ('jean@f.com','FIRM','employee',1),
              ('pat@f.com','FIRM','employee',1),
              ('admin@f.com','FIRM','firm_admin',1);
            INSERT INTO clients (client_code, client_name, firm_code,
                                 primary_employee_email,
                                 secondary_employee_email)
            VALUES
              ('ACME','Acme','FIRM','sam@f.com','jean@f.com'),
              ('BETA','Beta','FIRM','sam@f.com','jean@f.com'),
              ('GAMMA','Gamma','FIRM','jean@f.com','sam@f.com'),
              ('DELTA','Delta','FIRM','sam@f.com',NULL);
            """
        )
        conn.commit()
    ooo.ensure_schema(db_path)
    return db_path


# ---------------------------------------------------------------------------
# Set / end
# ---------------------------------------------------------------------------


def test_set_ooo_records_window(db):
    # Use admin as coverage so the permission check passes.
    r = ooo.set_ooo(
        db, firm_code='FIRM',
        employee_email='sam@f.com',
        coverage_email='admin@f.com',
        start_date='2026-04-25', end_date='2026-05-03',
        created_by='admin@f.com',
    )
    assert r['ok'] is True
    assert isinstance(r['id'], int)


def test_set_ooo_rejects_bad_date_range(db):
    r = ooo.set_ooo(
        db, firm_code='FIRM',
        employee_email='sam@f.com', coverage_email='jean@f.com',
        start_date='2026-05-03', end_date='2026-04-25',
        created_by='admin@f.com',
    )
    assert r == {'ok': False, 'reason': 'bad_date_range'}


def test_set_ooo_rejects_double_active(db):
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='admin@f.com',
                start_date='2026-04-25', end_date='2026-05-03',
                created_by='a@f.com')
    r = ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                    coverage_email='admin@f.com',
                    start_date='2026-06-01', end_date='2026-06-10',
                    created_by='a@f.com')
    assert r == {'ok': False, 'reason': 'already_active'}


def test_end_ooo_closes_active_window(db):
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='admin@f.com',
                start_date='2026-04-25', end_date='2026-05-03',
                created_by='a@f.com')
    r = ooo.end_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                    ended_by='a@f.com')
    assert r['ok'] is True
    assert r['status'] == 'ended'


def test_end_ooo_when_none_active_returns_reason(db):
    r = ooo.end_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                    ended_by='a@f.com')
    assert r == {'ok': False, 'reason': 'no_active_ooo'}


# ---------------------------------------------------------------------------
# Coverage permission check
# ---------------------------------------------------------------------------


def test_coverage_permission_check_accepts_shared_clients(db):
    # Jean covers ACME, BETA (secondary), GAMMA (primary). Sam's
    # clients are ACME, BETA, GAMMA, DELTA. DELTA is uncovered.
    assert ooo.coverage_has_client_access(
        db, firm_code='FIRM', employee_email='sam@f.com',
        coverage_email='jean@f.com',
    ) is False  # DELTA uncovered


def test_coverage_permission_check_returns_true_when_full_overlap(db):
    # Delete the DELTA pointer so jean covers all of sam's clients.
    with sqlite3.connect(db) as conn:
        conn.execute("DELETE FROM clients WHERE client_code='DELTA'")
        conn.commit()
    assert ooo.coverage_has_client_access(
        db, firm_code='FIRM', employee_email='sam@f.com',
        coverage_email='jean@f.com',
    ) is True


def test_coverage_permission_check_firm_admin_ok(db):
    assert ooo.coverage_has_client_access(
        db, firm_code='FIRM', employee_email='sam@f.com',
        coverage_email='admin@f.com',
    ) is True


def test_set_ooo_rejects_coverage_without_access(db):
    r = ooo.set_ooo(
        db, firm_code='FIRM', employee_email='sam@f.com',
        coverage_email='pat@f.com',  # pat has no overlapping clients
        start_date='2026-04-25', end_date='2026-05-03',
        created_by='admin@f.com',
    )
    assert r == {'ok': False, 'reason': 'coverage_missing_client_access'}


def test_set_ooo_admin_override_allows_any_coverage(db):
    r = ooo.set_ooo(
        db, firm_code='FIRM', employee_email='sam@f.com',
        coverage_email='pat@f.com',
        start_date='2026-04-25', end_date='2026-05-03',
        created_by='admin@f.com',
        require_coverage_permission=False,
    )
    assert r['ok'] is True


# ---------------------------------------------------------------------------
# Coverage lookup / is_ooo
# ---------------------------------------------------------------------------


def test_get_coverage_for_inside_window(db):
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='jean@f.com',
                start_date='2026-04-20', end_date='2026-05-10',
                created_by='a@f.com',
                require_coverage_permission=False)
    assert ooo.get_coverage_for(
        db, firm_code='FIRM', employee_email='sam@f.com',
        as_of='2026-04-25',
    ) == 'jean@f.com'


def test_get_coverage_for_outside_window_returns_none(db):
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='jean@f.com',
                start_date='2026-04-20', end_date='2026-05-10',
                created_by='a@f.com',
                require_coverage_permission=False)
    assert ooo.get_coverage_for(
        db, firm_code='FIRM', employee_email='sam@f.com',
        as_of='2026-06-01',
    ) is None


def test_is_ooo_helper(db):
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='jean@f.com',
                start_date='2026-04-20', end_date='2026-05-10',
                created_by='a@f.com',
                require_coverage_permission=False)
    assert ooo.is_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                      as_of='2026-04-25') is True
    assert ooo.is_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                      as_of='2026-06-01') is False


# ---------------------------------------------------------------------------
# Auto-reply payload
# ---------------------------------------------------------------------------


def test_auto_reply_payload_defaults_fr(db):
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='jean@f.com',
                start_date='2026-04-20', end_date='2026-05-10',
                created_by='a@f.com',
                require_coverage_permission=False)
    msg = ooo.auto_reply_payload(db, firm_code='FIRM',
                                 employee_email='sam@f.com', lang='fr')
    assert msg is not None
    assert '2026-05-10' in msg['subject']
    assert 'jean@f.com' in msg['body']


def test_auto_reply_payload_english(db):
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='jean@f.com',
                start_date='2026-04-20', end_date='2026-05-10',
                created_by='a@f.com',
                require_coverage_permission=False)
    msg = ooo.auto_reply_payload(db, firm_code='FIRM',
                                 employee_email='sam@f.com', lang='en')
    assert 'Out of office' in msg['subject']


def test_auto_reply_payload_custom_message(db):
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='jean@f.com',
                start_date='2026-04-20', end_date='2026-05-10',
                auto_reply_subject='I am away',
                auto_reply_body='Email jean directly.',
                created_by='a@f.com',
                require_coverage_permission=False)
    msg = ooo.auto_reply_payload(db, firm_code='FIRM',
                                 employee_email='sam@f.com', lang='en')
    assert msg == {'subject': 'I am away',
                   'body': 'Email jean directly.'}


# ---------------------------------------------------------------------------
# Depart / bulk reassign
# ---------------------------------------------------------------------------


def test_depart_reassigns_workflows_and_clients(db):
    # Create workflows for sam
    with sqlite3.connect(db) as conn:
        conn.executemany(
            "INSERT INTO review_workflow "
            "(firm_code, entity_type, entity_id, status, "
            " assigned_to_email, assigned_at) VALUES (?,?,?,?,?,?)",
            [
                ('FIRM', 'document', 'D1', 'assigned', 'sam@f.com',
                 '2026-04-24'),
                ('FIRM', 'document', 'D2', 'in_review', 'sam@f.com',
                 '2026-04-24'),
                ('FIRM', 'document', 'D3', 'resolved', 'sam@f.com',
                 '2026-04-24'),  # resolved → untouched
            ],
        )
        conn.commit()
    r = ooo.depart_employee(
        db, firm_code='FIRM', employee_email='sam@f.com',
        replacement_email='jean@f.com', actor='admin@f.com',
    )
    assert r['ok'] is True
    assert r['workflows_reassigned'] == 2
    assert r['primaries_updated'] == 3  # ACME, BETA, DELTA
    assert r['secondaries_updated'] == 1  # GAMMA
    # dashboard access revoked
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        u = conn.execute(
            "SELECT active FROM dashboard_users WHERE email='sam@f.com'"
        ).fetchone()
    assert u['active'] == 0


def test_depart_ends_active_ooo(db):
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='jean@f.com',
                start_date='2026-04-20', end_date='2026-05-10',
                created_by='a@f.com',
                require_coverage_permission=False)
    ooo.depart_employee(
        db, firm_code='FIRM', employee_email='sam@f.com',
        replacement_email='jean@f.com', actor='admin@f.com',
    )
    assert ooo.get_coverage_for(
        db, firm_code='FIRM', employee_email='sam@f.com',
        as_of='2026-04-25',
    ) is None


def test_bulk_reassign_open_only_by_default(db):
    with sqlite3.connect(db) as conn:
        conn.executemany(
            "INSERT INTO review_workflow "
            "(firm_code, entity_type, entity_id, status, "
            " assigned_to_email, assigned_at) VALUES (?,?,?,?,?,?)",
            [
                ('FIRM', 'document', 'D1', 'assigned', 'sam@f.com', 'x'),
                ('FIRM', 'document', 'D2', 'resolved', 'sam@f.com', 'x'),
            ],
        )
        conn.commit()
    r = ooo.bulk_reassign(db, firm_code='FIRM',
                          from_email='sam@f.com',
                          to_email='jean@f.com',
                          actor='admin@f.com')
    assert r == {'ok': True, 'reassigned': 1}


def test_bulk_reassign_include_resolved(db):
    with sqlite3.connect(db) as conn:
        conn.executemany(
            "INSERT INTO review_workflow "
            "(firm_code, entity_type, entity_id, status, "
            " assigned_to_email, assigned_at) VALUES (?,?,?,?,?,?)",
            [
                ('FIRM', 'document', 'D1', 'assigned', 'sam@f.com', 'x'),
                ('FIRM', 'document', 'D2', 'resolved', 'sam@f.com', 'x'),
            ],
        )
        conn.commit()
    r = ooo.bulk_reassign(db, firm_code='FIRM',
                          from_email='sam@f.com',
                          to_email='jean@f.com',
                          actor='admin@f.com',
                          include_resolved=True)
    assert r == {'ok': True, 'reassigned': 2}


def test_list_active_ooo_filters_by_firm_and_date(db):
    ooo.set_ooo(db, firm_code='FIRM', employee_email='sam@f.com',
                coverage_email='admin@f.com',
                start_date='2026-04-20', end_date='2026-05-10',
                created_by='a@f.com',
                require_coverage_permission=False)
    active = ooo.list_active_ooo(db, 'FIRM')
    assert len(active) == 1
    assert active[0]['employee_email'] == 'sam@f.com'
