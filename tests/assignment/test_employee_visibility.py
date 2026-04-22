"""Hybrid assignment phase 3: role-based queue filtering.

Asserts the per-role visibility surface for /queue + /clients +
document detail:

EMPLOYEE
- Sees documents whose review_workflow row is assigned to them by email.
- Sees documents on clients where they are primary or secondary, when
  no document-level override exists.
- Does NOT see documents on other clients.
- Cannot open a document detail page outside their portfolio unless
  they have an explicit override.

FIRM_ADMIN / OWNER
- Sees everything in the firm.

The tests exercise ``_build_documents_where`` + ``render_document``'s
access check through ``build_user_context`` so they cover the whole
read surface, not just the SQL fragment.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Test fixture: build a mini-DB with the columns _build_documents_where reads.
# ---------------------------------------------------------------------------


def _bootstrap_min(db_path: Path) -> None:
    """Use the production bootstrap so every dashboard SELECT works."""
    # Need clients/documents tables to exist BEFORE bootstrap_schema runs
    # (it uses ALTER TABLE to back-fill missing columns and assumes the
    # base table exists).
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS clients ("
            " client_code TEXT PRIMARY KEY, "
            " client_name TEXT)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS documents ("
            " document_id TEXT PRIMARY KEY, "
            " client_code TEXT)"
        )
        conn.commit()
    finally:
        conn.close()
    # Now run the dashboard's bootstrap to add every missing column
    # the queue/render code SELECTs.
    import scripts.review_dashboard as rd
    real = rd.DB_PATH
    rd.DB_PATH = db_path
    try:
        rd.bootstrap_schema()
    finally:
        rd.DB_PATH = real


def _seed_world(db_path: Path) -> None:
    """4 users, 3 clients, 7 documents."""
    _bootstrap_min(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        # Firm F1: owner Sam, firm_admin Marie, employees Sophie + Jean
        conn.executemany(
            "INSERT INTO dashboard_users "
            "(username, password_hash, email, firm_code, role, active) "
            "VALUES (?,'x',?,?,?,1)",
            [
                ('sam@f1.com',     'sam@f1.com',     'F1', 'owner'),
                ('marie@f1.com',   'marie@f1.com',   'F1', 'firm_admin'),
                ('sophie@f1.com',  'sophie@f1.com',  'F1', 'employee'),
                ('jean@f1.com',    'jean@f1.com',    'F1', 'employee'),
            ],
        )
        conn.executemany(
            "INSERT INTO clients "
            "(client_code, client_name, firm_code, "
            " primary_employee_email, secondary_employee_email) "
            "VALUES (?,?,?,?,?)",
            [
                ('TREMBLAY', 'Tremblay Inc', 'F1',
                 'sophie@f1.com', 'jean@f1.com'),
                ('CAFE',     'Cafe Centro',  'F1',
                 'jean@f1.com',   'sophie@f1.com'),
                ('MARCHAND', 'Marchand SA',  'F1',
                 'marie@f1.com',  None),
            ],
        )
        # Documents
        docs = [
            ('D-T1', 'TREMBLAY'),
            ('D-T2', 'TREMBLAY'),
            ('D-C1', 'CAFE'),
            ('D-C2', 'CAFE'),
            ('D-M1', 'MARCHAND'),
            ('D-POOL', 'TREMBLAY'),  # auto-assigned to Sophie below
        ]
        for did, cc in docs:
            conn.execute(
                "INSERT INTO documents "
                "(document_id, client_code, file_name, review_status, "
                " vendor, amount, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (did, cc, f'{did}.jpg', 'New',
                 'Vendor', 100.0, '2026-04-01', '2026-04-01'),
            )
        # Auto-assign: Sophie -> D-T1, D-T2, D-POOL ; Jean -> D-C1, D-C2 ;
        # Marie -> D-M1
        rw = [
            ('F1', 'D-T1',   'sophie@f1.com'),
            ('F1', 'D-T2',   'sophie@f1.com'),
            ('F1', 'D-POOL', 'sophie@f1.com'),
            ('F1', 'D-C1',   'jean@f1.com'),
            ('F1', 'D-C2',   'jean@f1.com'),
            ('F1', 'D-M1',   'marie@f1.com'),
        ]
        for fc, eid, email in rw:
            conn.execute(
                "INSERT INTO review_workflow "
                "(firm_code, entity_type, entity_id, status, "
                " assigned_to_email, priority, assigned_at) "
                "VALUES (?, 'document', ?, 'assigned', ?, 'normal', ?)",
                (fc, eid, email, '2026-04-01'),
            )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def world(tmp_path, monkeypatch):
    """Spin up the dashboard module with DB_PATH pointed at a fresh DB."""
    db = tmp_path / 'world.db'
    _seed_world(db)
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, 'DB_PATH', db)
    yield rd, db


def _ctx_for(rd, *, role: str, email: str, firm_code: str = 'F1'):
    return rd.build_user_context({
        'username': email,
        'email': email,
        'role': role,
        'firm_code': firm_code,
        'display_name': email,
    })


# ---------------------------------------------------------------------------
# Employee visibility
# ---------------------------------------------------------------------------


def test_employee_sees_only_assigned_documents(world):
    rd, _db = world
    ctx = _ctx_for(rd, role='employee', email='sophie@f1.com')
    rows = rd.get_documents(ctx=ctx)
    seen = sorted(r['document_id'] for r in rows)
    # Sophie is primary on TREMBLAY (D-T1, D-T2, D-POOL) and secondary
    # on CAFE — but CAFE docs are explicitly assigned to Jean, so she
    # should NOT see CAFE docs. She must not see MARCHAND at all.
    assert seen == ['D-POOL', 'D-T1', 'D-T2'], seen


def test_employee_sees_clients_primary_on_them(world):
    rd, _db = world
    ctx = _ctx_for(rd, role='employee', email='sophie@f1.com')
    # Sophie's portfolio (allowed_clients) must include TREMBLAY (primary)
    # and CAFE (secondary).
    assert set(ctx['allowed_clients']) >= {'TREMBLAY', 'CAFE'}
    assert 'MARCHAND' not in ctx['allowed_clients']


def test_employee_sees_clients_secondary_on_them(world):
    rd, _db = world
    ctx = _ctx_for(rd, role='employee', email='jean@f1.com')
    # Jean is primary on CAFE, secondary on TREMBLAY
    assert set(ctx['allowed_clients']) >= {'CAFE', 'TREMBLAY'}
    assert 'MARCHAND' not in ctx['allowed_clients']


def test_employee_cannot_access_other_clients_documents_directly(world):
    rd, _db = world
    ctx = _ctx_for(rd, role='employee', email='sophie@f1.com')
    user = {'username': 'sophie@f1.com', 'email': 'sophie@f1.com',
            'role': 'employee', 'firm_code': 'F1'}
    # MARCHAND is Marie's; Sophie isn't primary/secondary. The detail
    # page must refuse, not 200.
    html = rd.render_document('D-M1', ctx, user, '', '', lang='en')
    # access_denied banner is rendered for refused access
    assert 'err_access_denied' in html or 'Access' in html or 'Acces' in html or 'Refus' in html
    # Sanity: she CAN access her own
    html_ok = rd.render_document('D-T1', ctx, user, '', '', lang='en')
    assert 'err_doc_not_found' not in html_ok


def test_firm_admin_sees_everything(world):
    rd, _db = world
    ctx = _ctx_for(rd, role='firm_admin', email='marie@f1.com')
    rows = rd.get_documents(ctx=ctx)
    seen = sorted(r['document_id'] for r in rows)
    assert seen == ['D-C1', 'D-C2', 'D-M1', 'D-POOL', 'D-T1', 'D-T2'], seen


def test_owner_sees_everything(world):
    rd, _db = world
    ctx = _ctx_for(rd, role='owner', email='sam@f1.com')
    rows = rd.get_documents(ctx=ctx)
    seen = sorted(r['document_id'] for r in rows)
    assert seen == ['D-C1', 'D-C2', 'D-M1', 'D-POOL', 'D-T1', 'D-T2'], seen


def test_document_override_assignment_respected(world):
    """Sophie reassigns one of her TREMBLAY docs to Jean (override).
    Jean now sees 3 (his 2 CAFE + the 1 reassigned TREMBLAY).
    Sophie now sees only 2 of her original 3."""
    rd, db = world
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "UPDATE review_workflow SET assigned_to_email='jean@f1.com' "
            "WHERE entity_id='D-T1'"
        )
        conn.commit()
    finally:
        conn.close()

    sophie = _ctx_for(rd, role='employee', email='sophie@f1.com')
    sophie_seen = sorted(r['document_id']
                          for r in rd.get_documents(ctx=sophie))
    assert sophie_seen == ['D-POOL', 'D-T2'], sophie_seen

    jean = _ctx_for(rd, role='employee', email='jean@f1.com')
    jean_seen = sorted(r['document_id']
                        for r in rd.get_documents(ctx=jean))
    assert jean_seen == ['D-C1', 'D-C2', 'D-T1'], jean_seen


def test_employee_can_open_overridden_document_outside_portfolio(world):
    """Even on a client outside the employee's portfolio, an explicit
    review_workflow assignment must let them open the detail page."""
    rd, db = world
    # Reassign D-M1 (MARCHAND, outside Sophie's portfolio) to Sophie.
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "UPDATE review_workflow SET assigned_to_email='sophie@f1.com' "
            "WHERE entity_id='D-M1'"
        )
        conn.commit()
    finally:
        conn.close()
    sophie_ctx = _ctx_for(rd, role='employee', email='sophie@f1.com')
    user = {'username': 'sophie@f1.com', 'email': 'sophie@f1.com',
            'role': 'employee', 'firm_code': 'F1'}
    html = rd.render_document('D-M1', sophie_ctx, user, '', '', lang='en')
    # Should NOT show the access-denied banner — override grants access.
    # The banner key is err_access_denied — when access is granted, the
    # render returns the document body which does not contain that key.
    assert ('err_access_denied' not in html
            and 'Access denied' not in html
            and 'denied' not in html.lower())


def test_pool_document_visible_only_to_owner_admin(world):
    """A document with NO review_workflow row (true firm pool) is
    visible to admins but not to employees who aren't on the client."""
    rd, db = world
    # Add a fresh document on TREMBLAY with no review_workflow row.
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "INSERT INTO documents "
            "(document_id, client_code, file_name, review_status, "
            " vendor, amount, created_at, updated_at) "
            "VALUES ('D-FREE', 'MARCHAND', 'free.jpg', 'New', "
            " 'V', 1.0, '2026-04-02','2026-04-02')",
        )
        conn.commit()
    finally:
        conn.close()
    sophie = _ctx_for(rd, role='employee', email='sophie@f1.com')
    rows = rd.get_documents(ctx=sophie)
    assert 'D-FREE' not in {r['document_id'] for r in rows}
    # Owner sees it.
    sam = _ctx_for(rd, role='owner', email='sam@f1.com')
    rows_o = rd.get_documents(ctx=sam)
    assert 'D-FREE' in {r['document_id'] for r in rows_o}
