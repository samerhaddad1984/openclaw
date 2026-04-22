"""Hybrid assignment phase 8: end-to-end role-correct visibility scenario.

4 users, 3 clients, 10 documents — exercises every layer of the
hybrid model in one go:

Firm F1
  owner:       sam@f1.com
  firm_admin:  marie@f1.com
  employees:   sophie@f1.com, jean@f1.com

Clients:
  TREMBLAY  primary=sophie, secondary=jean
  CAFE      primary=jean,   secondary=sophie
  MARCHAND  primary=marie,  secondary=<none>

Flow:
  1. TREMBLAY ingests 5 docs  → auto-assigned to Sophie
  2. CAFE    ingests 3 docs   → auto-assigned to Jean
  3. MARCHAND ingests 2 docs  → auto-assigned to Marie
  4. Sophie reassigns one TREMBLAY doc to Jean (ask for help)
  5. Expected visibility:
      Sophie  → 4 docs (4 TREMBLAY, 0 CAFE, 0 MARCHAND)
      Jean    → 4 docs (3 CAFE + 1 reassigned TREMBLAY)
      Marie   → all 10 (firm_admin)
      Sam     → all 10 (owner)
  6. Sam's team workload report shows non-zero rows for Sophie + Jean.

This is the single scenario the design brief asks for; we assert
the exact counts above.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.review_dashboard as rd  # noqa: E402
from src.integrations.auto_assign import (  # noqa: E402
    auto_assign_new_document,
)
from src.integrations.client_assignment import (  # noqa: E402
    update_client_assignment,
)
from src.integrations.review_workflow import (  # noqa: E402
    my_tasks, reassign_document,
)
from src.integrations.team_workload import get_team_workload  # noqa: E402


def _build_world(db_path: Path) -> None:
    # Use the production bootstrap so every downstream query has its
    # columns. We seed the base tables first so ALTER TABLE finds them.
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE clients ("
                      "client_code TEXT PRIMARY KEY, client_name TEXT)")
        conn.execute("CREATE TABLE documents ("
                      "document_id TEXT PRIMARY KEY, client_code TEXT)")
        conn.commit()
    finally:
        conn.close()
    saved = rd.DB_PATH
    rd.DB_PATH = db_path
    try:
        rd.bootstrap_schema()
    finally:
        rd.DB_PATH = saved
    conn = sqlite3.connect(str(db_path))
    try:
        # Users
        for email, role in [
            ('sam@f1.com', 'owner'),
            ('marie@f1.com', 'firm_admin'),
            ('sophie@f1.com', 'employee'),
            ('jean@f1.com', 'employee'),
        ]:
            conn.execute(
                "INSERT INTO dashboard_users "
                "(username,password_hash,email,firm_code,role,active) "
                "VALUES (?,'x',?,?,?,1)",
                (email, email, 'F1', role),
            )
        # Clients (initially unassigned — we assign via the
        # update_client_assignment helper below so the audit rows land).
        conn.execute(
            "UPDATE clients SET firm_code='OWNER' "
            "WHERE firm_code IS NULL OR firm_code=''"
        )
        conn.executemany(
            "INSERT INTO clients (client_code, client_name, firm_code) "
            "VALUES (?,?,'F1')",
            [('TREMBLAY', 'Tremblay Inc'),
             ('CAFE',     'Cafe Centro'),
             ('MARCHAND', 'Marchand SA')],
        )
        conn.commit()
    finally:
        conn.close()
    # Step 2: owner assigns the team to each client
    update_client_assignment(
        db_path, firm_code='F1', client_code='TREMBLAY',
        primary_email='sophie@f1.com', secondary_email='jean@f1.com',
        changed_by='sam@f1.com', actor_role='owner',
        reason='onboarding',
    )
    update_client_assignment(
        db_path, firm_code='F1', client_code='CAFE',
        primary_email='jean@f1.com', secondary_email='sophie@f1.com',
        changed_by='sam@f1.com', actor_role='owner',
        reason='onboarding',
    )
    update_client_assignment(
        db_path, firm_code='F1', client_code='MARCHAND',
        primary_email='marie@f1.com', secondary_email=None,
        changed_by='sam@f1.com', actor_role='owner',
        reason='firm_admin takes direct',
    )


def _ingest(db_path: Path, doc_id: str, client_code: str) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO documents "
            "(document_id, client_code, file_name, review_status, "
            " vendor, amount, created_at, updated_at) "
            "VALUES (?,?,?,?, 'V', 1.0, '2026-04-01','2026-04-01')",
            (doc_id, client_code, f'{doc_id}.jpg', 'New'),
        )
        conn.commit()
    finally:
        conn.close()
    auto_assign_new_document(document_id=doc_id, db_path=db_path)


@pytest.fixture
def scenario(tmp_path, monkeypatch):
    db = tmp_path / 'e2e.db'
    _build_world(db)
    # Step 3: ingest documents and let auto_assign route them.
    for i in range(1, 6):
        _ingest(db, f'T{i}', 'TREMBLAY')
    for i in range(1, 4):
        _ingest(db, f'C{i}', 'CAFE')
    for i in range(1, 3):
        _ingest(db, f'M{i}', 'MARCHAND')
    monkeypatch.setattr(rd, 'DB_PATH', db)
    return db


def _ctx(role: str, email: str) -> dict:
    return rd.build_user_context({
        'username': email, 'email': email,
        'role': role, 'firm_code': 'F1',
        'display_name': email,
    })


# ---------------------------------------------------------------------------


def test_auto_assignment_distributes_to_primaries(scenario):
    """All 10 docs should be routed to their clients' primary employee."""
    conn = sqlite3.connect(str(scenario))
    try:
        rows = conn.execute(
            "SELECT assigned_to_email, COUNT(*) AS n "
            "FROM review_workflow WHERE entity_type='document' "
            "GROUP BY assigned_to_email"
        ).fetchall()
    finally:
        conn.close()
    bucket = {r[0]: r[1] for r in rows}
    assert bucket == {
        'sophie@f1.com': 5,  # TREMBLAY x5
        'jean@f1.com':   3,  # CAFE x3
        'marie@f1.com':  2,  # MARCHAND x2
    }


def test_employee_queue_visibility_before_override(scenario):
    sophie_rows = rd.get_documents(ctx=_ctx('employee', 'sophie@f1.com'))
    sophie_ids = sorted(r['document_id'] for r in sophie_rows)
    assert sophie_ids == ['T1', 'T2', 'T3', 'T4', 'T5']

    jean_rows = rd.get_documents(ctx=_ctx('employee', 'jean@f1.com'))
    jean_ids = sorted(r['document_id'] for r in jean_rows)
    assert jean_ids == ['C1', 'C2', 'C3']


def test_firm_admin_and_owner_see_everything(scenario):
    admin_rows = rd.get_documents(ctx=_ctx('firm_admin', 'marie@f1.com'))
    admin_ids = sorted(r['document_id'] for r in admin_rows)
    assert admin_ids == ['C1', 'C2', 'C3',
                          'M1', 'M2',
                          'T1', 'T2', 'T3', 'T4', 'T5']
    owner_rows = rd.get_documents(ctx=_ctx('owner', 'sam@f1.com'))
    owner_ids = sorted(r['document_id'] for r in owner_rows)
    assert owner_ids == admin_ids


def test_document_reassign_moves_between_queues(scenario):
    # Step 4: Sophie hands T1 off to Jean for a tax question.
    reassign_document(
        scenario, firm_code='F1', document_id='T1',
        new_assignee_email='jean@f1.com',
        actor_email='sophie@f1.com', actor_role='employee',
        reason='help: tax credit eligibility',
    )
    sophie_ids = sorted(r['document_id']
                         for r in rd.get_documents(
                             ctx=_ctx('employee', 'sophie@f1.com')))
    jean_ids = sorted(r['document_id']
                       for r in rd.get_documents(
                           ctx=_ctx('employee', 'jean@f1.com')))
    assert sophie_ids == ['T2', 'T3', 'T4', 'T5']
    assert jean_ids == ['C1', 'C2', 'C3', 'T1']


def test_my_tasks_tracks_assignee_changes(scenario):
    # Sophie's /my_tasks initially has 5 rows.
    t0 = my_tasks(scenario, assignee_email='sophie@f1.com')
    assert len(t0) == 5
    reassign_document(
        scenario, firm_code='F1', document_id='T2',
        new_assignee_email='jean@f1.com',
        actor_email='sophie@f1.com', actor_role='employee',
        reason='help',
    )
    t1_s = my_tasks(scenario, assignee_email='sophie@f1.com')
    t1_j = my_tasks(scenario, assignee_email='jean@f1.com')
    assert len(t1_s) == 4
    assert any(t['entity_id'] == 'T2' for t in t1_j)


def test_owner_workload_report_reflects_distribution(scenario):
    rows = get_team_workload(scenario, firm_code='F1')
    by_email = {r['email']: r for r in rows}
    assert by_email['sophie@f1.com']['open_docs'] == 5
    assert by_email['jean@f1.com']['open_docs']   == 3
    assert by_email['marie@f1.com']['open_docs']  == 2
    # Primary / secondary client counts match the setup.
    assert by_email['sophie@f1.com']['primary_clients']   == 1  # TREMBLAY
    assert by_email['sophie@f1.com']['secondary_clients'] == 1  # CAFE
    assert by_email['jean@f1.com']['primary_clients']     == 1  # CAFE
    assert by_email['jean@f1.com']['secondary_clients']   == 1  # TREMBLAY
    assert by_email['marie@f1.com']['primary_clients']    == 1  # MARCHAND


def test_client_assignment_history_records_onboarding(scenario):
    """Every update_client_assignment call in setup wrote an audit row."""
    conn = sqlite3.connect(str(scenario))
    try:
        rows = conn.execute(
            "SELECT client_code, action, changed_by, reason "
            "FROM client_assignment_history ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    actions = {(r[0], r[1]) for r in rows}
    assert actions == {
        ('TREMBLAY', 'assigned'),
        ('CAFE',     'assigned'),
        ('MARCHAND', 'assigned'),
    }
    assert all(r[2] == 'sam@f1.com' for r in rows)
