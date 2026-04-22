"""Hybrid assignment phase 2: auto-assign new documents to client primary.

Covers:
- Primary employee gets newly ingested documents.
- Falls back to secondary when primary is inactive.
- Stays in pool when neither is set / active.
- Wired into save_and_queue_document (web/portal/multi-user portal),
  whatsapp ingest, and openclaw bridge.
- Existing workflow rows are not clobbered (idempotent on re-ingest).
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.auto_assign import (  # noqa: E402
    REASON_AUTO_PRIMARY,
    REASON_AUTO_SECONDARY,
    REASON_PRIMARY_INACTIVE,
    auto_assign_new_document,
)
from src.integrations.review_workflow import (  # noqa: E402
    ensure_review_schema,
    get_workflow,
)


# ---------------------------------------------------------------------------
# Minimal-bootstrap helper: build the docs/clients/dashboard_users tables
# the auto-assigner reads, exactly as production schema does.
# ---------------------------------------------------------------------------

def _bootstrap(db_path: Path,
               *,
               firm_code: str = 'F1',
               clients: list[dict] | None = None,
               employees: list[dict] | None = None) -> None:
    ensure_review_schema(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                client_code TEXT PRIMARY KEY,
                client_name TEXT,
                firm_code   TEXT,
                primary_employee_email   TEXT,
                secondary_employee_email TEXT,
                active INTEGER DEFAULT 1
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_users (
                username      TEXT PRIMARY KEY,
                email         TEXT,
                firm_code     TEXT,
                role          TEXT DEFAULT 'employee',
                active        INTEGER NOT NULL DEFAULT 1
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                document_id  TEXT PRIMARY KEY,
                client_code  TEXT,
                file_name    TEXT,
                created_at   TEXT
            )
        """)
        for c in (clients or []):
            conn.execute(
                "INSERT INTO clients "
                "(client_code, client_name, firm_code, "
                " primary_employee_email, secondary_employee_email, active) "
                "VALUES (?,?,?,?,?,?)",
                (c['client_code'], c.get('client_name', c['client_code']),
                 c.get('firm_code', firm_code),
                 c.get('primary'), c.get('secondary'),
                 c.get('active', 1)),
            )
        for e in (employees or []):
            conn.execute(
                "INSERT INTO dashboard_users "
                "(username, email, firm_code, role, active) "
                "VALUES (?,?,?,?,?)",
                (e['email'], e['email'],
                 e.get('firm_code', firm_code),
                 e.get('role', 'employee'),
                 e.get('active', 1)),
            )
        conn.commit()
    finally:
        conn.close()


def _insert_doc(db_path: Path, document_id: str, *,
                 client_code: str = 'TREMBLAY') -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO documents (document_id, client_code, file_name, created_at) "
            "VALUES (?, ?, 'r.jpg', '2026-04-22T00:00:00Z')",
            (document_id, client_code),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Core auto-assign behaviour
# ---------------------------------------------------------------------------


def test_new_document_auto_assigned_to_primary(tmp_path):
    db = tmp_path / 'a.db'
    _bootstrap(
        db,
        clients=[{'client_code': 'TREMBLAY',
                   'primary': 'sophie@f1.com'}],
        employees=[{'email': 'sophie@f1.com'}],
    )
    _insert_doc(db, 'D-NEW-1')
    res = auto_assign_new_document(document_id='D-NEW-1', db_path=db)
    assert res is not None
    assert res['assigned_to_email'] == 'sophie@f1.com'
    assert res['reason'] == REASON_AUTO_PRIMARY
    wf = get_workflow(db, firm_code='F1', entity_type='document',
                       entity_id='D-NEW-1')
    assert wf and wf['assigned_to_email'] == 'sophie@f1.com'
    assert wf['status'] == 'assigned'


def test_fallback_to_secondary_when_primary_inactive(tmp_path):
    db = tmp_path / 'a.db'
    _bootstrap(
        db,
        clients=[{'client_code': 'TREMBLAY',
                   'primary': 'sophie@f1.com',
                   'secondary': 'jean@f1.com'}],
        employees=[
            {'email': 'sophie@f1.com', 'active': 0},
            {'email': 'jean@f1.com', 'active': 1},
        ],
    )
    _insert_doc(db, 'D-FALLBACK')
    res = auto_assign_new_document(document_id='D-FALLBACK', db_path=db)
    assert res is not None
    assert res['assigned_to_email'] == 'jean@f1.com'
    assert res['reason'] == REASON_PRIMARY_INACTIVE


def test_secondary_only_uses_secondary_reason(tmp_path):
    # Primary unset (NULL) but secondary is set — that's not a
    # "primary inactive" fallback; it's just the only choice.
    db = tmp_path / 'a.db'
    _bootstrap(
        db,
        clients=[{'client_code': 'TREMBLAY',
                   'primary': None,
                   'secondary': 'jean@f1.com'}],
        employees=[{'email': 'jean@f1.com'}],
    )
    _insert_doc(db, 'D-SEC-ONLY')
    res = auto_assign_new_document(document_id='D-SEC-ONLY', db_path=db)
    assert res is not None
    assert res['reason'] == REASON_AUTO_SECONDARY


def test_no_assignment_when_neither_available(tmp_path):
    db = tmp_path / 'a.db'
    _bootstrap(
        db,
        clients=[{'client_code': 'TREMBLAY',
                   'primary': None, 'secondary': None}],
    )
    _insert_doc(db, 'D-POOL')
    assert auto_assign_new_document(document_id='D-POOL', db_path=db) is None
    assert get_workflow(db, firm_code='F1', entity_type='document',
                          entity_id='D-POOL') is None


def test_no_assignment_when_both_inactive(tmp_path):
    db = tmp_path / 'a.db'
    _bootstrap(
        db,
        clients=[{'client_code': 'TREMBLAY',
                   'primary': 'sophie@f1.com',
                   'secondary': 'jean@f1.com'}],
        employees=[
            {'email': 'sophie@f1.com', 'active': 0},
            {'email': 'jean@f1.com', 'active': 0},
        ],
    )
    _insert_doc(db, 'D-BOTH-INACTIVE')
    assert auto_assign_new_document(document_id='D-BOTH-INACTIVE',
                                       db_path=db) is None


def test_assignment_recorded_with_reason(tmp_path):
    """Audit row carries the reason for forensic visibility."""
    db = tmp_path / 'a.db'
    _bootstrap(
        db,
        clients=[{'client_code': 'TREMBLAY',
                   'primary': 'sophie@f1.com'}],
        employees=[{'email': 'sophie@f1.com'}],
    )
    _insert_doc(db, 'D-AUDIT')
    auto_assign_new_document(document_id='D-AUDIT', db_path=db)
    conn = sqlite3.connect(str(db))
    try:
        row = conn.execute(
            "SELECT actor_email, actor_role, action, notes "
            "FROM review_workflow_audit ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    actor_email, actor_role, action, notes = row
    assert actor_email == 'system'
    assert actor_role == 'system'
    assert action == 'auto_assign'
    assert 'reason=auto_primary' in notes
    assert 'sophie@f1.com' in notes


def test_existing_documents_untouched(tmp_path):
    """Re-ingesting same doc_id must not clobber prior assignment."""
    db = tmp_path / 'a.db'
    _bootstrap(
        db,
        clients=[{'client_code': 'TREMBLAY',
                   'primary': 'sophie@f1.com'}],
        employees=[
            {'email': 'sophie@f1.com'},
            {'email': 'jean@f1.com'},
        ],
    )
    _insert_doc(db, 'D-IDEMPO')
    auto_assign_new_document(document_id='D-IDEMPO', db_path=db)
    # Pretend an admin already overrode the assignment to Jean.
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "UPDATE review_workflow SET assigned_to_email='jean@f1.com' "
            "WHERE entity_id='D-IDEMPO'"
        )
        conn.commit()
    finally:
        conn.close()
    # Second auto-assign call should be a no-op.
    res = auto_assign_new_document(document_id='D-IDEMPO', db_path=db)
    assert res is None
    wf = get_workflow(db, firm_code='F1', entity_type='document',
                       entity_id='D-IDEMPO')
    assert wf['assigned_to_email'] == 'jean@f1.com'


def test_unknown_client_falls_to_pool(tmp_path):
    """Document with no clients row must not crash the ingest path."""
    db = tmp_path / 'a.db'
    _bootstrap(db)
    _insert_doc(db, 'D-ORPHAN', client_code='UNASSIGNED')
    assert auto_assign_new_document(document_id='D-ORPHAN', db_path=db) is None


def test_missing_document_id_no_crash(tmp_path):
    db = tmp_path / 'a.db'
    _bootstrap(db)
    assert auto_assign_new_document(document_id='', db_path=db) is None
    assert auto_assign_new_document(document_id='ghost', db_path=db) is None


# ---------------------------------------------------------------------------
# Wiring tests: make sure the ingest entry points actually call
# auto_assign_new_document. Each test patches the integration boundary
# with a recorder so the real OCR pipeline doesn't run.
# ---------------------------------------------------------------------------


def test_works_for_save_and_queue_document(tmp_path, monkeypatch):
    """save_and_queue_document hooks auto-assign for /upload + portal."""
    from src.engines import upload_queue as uq

    db = tmp_path / 'a.db'
    _bootstrap(
        db,
        clients=[{'client_code': 'TREMBLAY',
                   'primary': 'sophie@f1.com'}],
        employees=[{'email': 'sophie@f1.com'}],
    )

    captured: list[dict] = []

    real = __import__('src.integrations.auto_assign',
                       fromlist=['auto_assign_new_document'])

    def spy(*, document_id, db_path=None):
        captured.append({'document_id': document_id,
                          'db_path': str(db_path) if db_path else None})
        return None

    monkeypatch.setattr(real, 'auto_assign_new_document', spy)
    # Stop the queue from running pipeline work in this unit test.
    monkeypatch.setattr(uq, 'get_upload_queue',
                        lambda: type('Q', (), {
                            'enqueue': lambda self, *a, **k: None,
                        })())
    # Use a fake upsert so we don't drag in the full ocr_engine schema.
    monkeypatch.setattr(uq, 'upsert_document',
                        lambda rec, *, db_path: _insert_doc(
                            db_path, rec['document_id'],
                            client_code=rec['client_code'],
                        ))

    uq.save_and_queue_document(
        b'fake', 'r.jpg',
        client_code='TREMBLAY',
        ingest_source='public_upload',
        db_path=db,
        upload_dir=tmp_path / 'uploads',
    )
    assert captured, "auto_assign_new_document was not invoked from save_and_queue_document"
    assert captured[0]['document_id']


def test_whatsapp_calls_auto_assign_in_source():
    """Belt-and-suspenders: the whatsapp ingest source has the hook."""
    src = (ROOT / 'src/integrations/whatsapp.py').read_text()
    assert 'auto_assign_new_document' in src, \
        "whatsapp.py must call auto_assign_new_document after process_file"


def test_openclaw_bridge_calls_auto_assign_in_source():
    src = (ROOT / 'src/integrations/openclaw_bridge.py').read_text()
    assert 'auto_assign_new_document' in src, \
        "openclaw_bridge.py must call auto_assign_new_document after process_file"


def test_save_and_queue_document_calls_auto_assign_in_source():
    src = (ROOT / 'src/engines/upload_queue.py').read_text()
    assert 'auto_assign_new_document' in src, \
        "upload_queue.py must call auto_assign_new_document after upsert"


def test_works_for_multi_user_portal_upload(tmp_path, monkeypatch):
    """Multi-user portal goes through save_and_queue_document too —
    same hook covers it. Verified by exercising the same code path
    with a different ingest_source label."""
    from src.engines import upload_queue as uq

    db = tmp_path / 'a.db'
    _bootstrap(
        db,
        clients=[{'client_code': 'CAFE',
                   'primary': 'jean@f1.com'}],
        employees=[{'email': 'jean@f1.com'}],
    )
    captured: list[str] = []
    monkeypatch.setattr(
        'src.integrations.auto_assign.auto_assign_new_document',
        lambda *, document_id, db_path=None: (
            captured.append(document_id) or {'assigned_to_email': 'jean@f1.com'}
        ),
    )
    monkeypatch.setattr(uq, 'get_upload_queue',
                        lambda: type('Q', (), {
                            'enqueue': lambda self, *a, **k: None,
                        })())
    monkeypatch.setattr(uq, 'upsert_document',
                        lambda rec, *, db_path: _insert_doc(
                            db_path, rec['document_id'],
                            client_code=rec['client_code'],
                        ))
    uq.save_and_queue_document(
        b'x', 'r.jpg',
        client_code='CAFE',
        ingest_source='multi_user_portal',
        db_path=db,
        upload_dir=tmp_path / 'uploads',
    )
    assert captured, "auto_assign hook didn't fire for multi-user portal upload"


def test_pool_documents_can_be_picked_up_later(tmp_path):
    """When a client has no primary and a doc lands in the pool, the
    document later gets assigned when an admin sets a primary and a
    *new* document arrives — but the existing pool doc stays in pool
    (no retroactive assignment from auto_assign — that's an opt-in
    bulk-assign action, see Phase 4)."""
    db = tmp_path / 'a.db'
    _bootstrap(
        db,
        clients=[{'client_code': 'TREMBLAY',
                   'primary': None}],
        employees=[{'email': 'sophie@f1.com'}],
    )
    _insert_doc(db, 'D-POOL-1')
    auto_assign_new_document(document_id='D-POOL-1', db_path=db)
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "UPDATE clients SET primary_employee_email='sophie@f1.com' "
            "WHERE client_code='TREMBLAY'"
        )
        conn.commit()
    finally:
        conn.close()
    # Existing pool doc still has no workflow row.
    assert get_workflow(db, firm_code='F1', entity_type='document',
                          entity_id='D-POOL-1') is None
    # New doc gets assigned to sophie.
    _insert_doc(db, 'D-NEW-2')
    auto_assign_new_document(document_id='D-NEW-2', db_path=db)
    wf = get_workflow(db, firm_code='F1', entity_type='document',
                       entity_id='D-NEW-2')
    assert wf and wf['assigned_to_email'] == 'sophie@f1.com'
