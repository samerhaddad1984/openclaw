"""Layer 3: backend systems must not consume silent default GL.

When a document is flagged ``needs_categorization=1`` (Layer 2), it
has no real categorisation. Pushing it to QBO or including it in an
export would propagate the lie into actual ledgers / CSVs.

These tests pin the contract:

  - export_engine.fetch_posted_documents skips uncategorised docs.
  - qbo_push raises before pushing an uncategorised doc.
  - Documents with valid line items are unaffected — line-level data
    is the source of truth.
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
# DB fixtures
# ---------------------------------------------------------------------------


def _bootstrap(db_path: Path) -> None:
    """Schema sufficient for export_engine + qbo_push to query."""
    c = sqlite3.connect(str(db_path))
    c.executescript(
        """
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, file_path TEXT, client_code TEXT,
            vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT,
            category TEXT, review_status TEXT, confidence REAL,
            raw_result TEXT, created_at TEXT, updated_at TEXT,
            submitted_by TEXT, client_note TEXT,
            currency TEXT, subtotal REAL, tax_total REAL,
            has_line_items INTEGER DEFAULT 0,
            needs_categorization INTEGER DEFAULT 0,
            due_date TEXT
        );
        CREATE TABLE posting_jobs (
            posting_id TEXT PRIMARY KEY,
            document_id TEXT,
            posting_status TEXT,
            external_id TEXT,
            approval_state TEXT,
            reviewer TEXT,
            payload_json TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE invoice_lines (
            line_id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id TEXT NOT NULL,
            line_number INTEGER,
            description TEXT,
            quantity REAL, unit_price REAL,
            line_total_pretax REAL,
            tax_code TEXT, tax_regime TEXT,
            gst_amount REAL, qst_amount REAL, hst_amount REAL,
            province_of_supply TEXT,
            is_tax_included INTEGER,
            line_notes TEXT,
            created_at TEXT NOT NULL DEFAULT '',
            gl_account TEXT, category TEXT,
            deleted_at TEXT
        );
        """
    )
    c.commit(); c.close()


def _seed_doc(db, doc_id, *, vendor='V', amount=100.0, gl='5400',
              category='telecom', has_lines=0, needs_cat=0,
              date='2026-04-01', posted=True, line_items=None):
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "INSERT INTO documents (document_id, file_name, client_code, "
            "vendor, amount, document_date, gl_account, tax_code, category, "
            "review_status, has_line_items, needs_categorization, "
            "created_at, updated_at, currency) VALUES "
            "(?, ?, 'CLI1', ?, ?, ?, ?, 'T', ?, 'Posted', ?, ?, "
            "datetime('now'), datetime('now'), 'CAD')",
            (doc_id, f'{doc_id}.pdf', vendor, amount, date, gl, category,
             has_lines, needs_cat),
        )
        if posted:
            conn.execute(
                "INSERT INTO posting_jobs (posting_id, document_id, "
                "posting_status, external_id, approval_state, "
                "created_at, updated_at) VALUES "
                "(?, ?, 'posted', 'EXT-1', 'approved', "
                "datetime('now'), datetime('now'))",
                (f'pj_{doc_id}', doc_id),
            )
        if line_items:
            for i, li in enumerate(line_items, start=1):
                conn.execute(
                    "INSERT INTO invoice_lines (document_id, line_number, "
                    "description, line_total_pretax, gl_account, category, "
                    "tax_code, gst_amount, qst_amount, hst_amount, "
                    "created_at) VALUES "
                    "(?, ?, ?, ?, ?, ?, 'T', 0, 0, 0, datetime('now'))",
                    (doc_id, i, li.get('desc', f'line {i}'),
                     li.get('amount', 50.0),
                     li.get('gl', ''), li.get('cat', '')),
                )
        conn.commit()


# ---------------------------------------------------------------------------
# export_engine.fetch_posted_documents excludes needs_categorization
# ---------------------------------------------------------------------------


def test_export_skips_uncategorized_doc(tmp_path):
    db = tmp_path / 'exp.db'
    _bootstrap(db)
    _seed_doc(db, 'OK',  vendor='Bell', gl='5400', needs_cat=0)
    _seed_doc(db, 'BAD', vendor=None, amount=None,
              gl='5440', category='operating_expense', needs_cat=1)

    from src.engines.export_engine import fetch_posted_documents
    docs = fetch_posted_documents('CLI1', '2026-01-01', '2026-12-31',
                                   db_path=db)
    ids = {d['document_id'] for d in docs}
    assert 'OK' in ids
    assert 'BAD' not in ids, (
        "export must NOT return docs flagged needs_categorization=1"
    )


def test_export_includes_legitimate_5440_doc_when_not_flagged(tmp_path):
    """A doc with gl=5440 that's NOT flagged (CPA confirmed it as
    real general expenses) must still be exported."""
    db = tmp_path / 'exp2.db'
    _bootstrap(db)
    _seed_doc(db, 'REAL5440', vendor='Acme', amount=50.0,
              gl='5440', category='operating_expense', needs_cat=0)
    from src.engines.export_engine import fetch_posted_documents
    docs = fetch_posted_documents('CLI1', '2026-01-01', '2026-12-31',
                                   db_path=db)
    assert any(d['document_id'] == 'REAL5440' for d in docs), (
        "CPA-confirmed 5440 docs must still export"
    )


def test_export_legacy_db_without_column_works(tmp_path):
    """Test fixtures and very old DBs may not have
    needs_categorization yet; the export must not crash and must
    return the docs (legacy behaviour)."""
    db = tmp_path / 'legacy.db'
    c = sqlite3.connect(str(db))
    c.executescript(
        """
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT, vendor TEXT, document_date TEXT,
            amount REAL, gl_account TEXT, tax_code TEXT,
            category TEXT, doc_type TEXT, file_name TEXT
        );
        CREATE TABLE posting_jobs (
            posting_id TEXT PRIMARY KEY,
            document_id TEXT, posting_status TEXT,
            external_id TEXT, created_at TEXT, updated_at TEXT
        );
        INSERT INTO documents (document_id, client_code, vendor,
                               document_date, amount, gl_account,
                               tax_code, category, doc_type, file_name)
            VALUES ('OLD', 'CLI1', 'V', '2026-04-01', 50.0,
                    '5400', 'T', 'telecom', 'invoice', 'old.pdf');
        INSERT INTO posting_jobs (posting_id, document_id,
                                  posting_status, external_id,
                                  created_at, updated_at)
            VALUES ('p1', 'OLD', 'posted', 'EXT', datetime('now'),
                    datetime('now'));
        """
    )
    c.commit(); c.close()
    from src.engines.export_engine import fetch_posted_documents
    docs = fetch_posted_documents('CLI1', '2026-01-01', '2026-12-31',
                                   db_path=db)
    assert any(d['document_id'] == 'OLD' for d in docs)


# ---------------------------------------------------------------------------
# qbo_push refuses uncategorised documents
# ---------------------------------------------------------------------------


def _make_qbo_pusher(db):
    """Build a QBOPush instance bypassing __init__ (we don't have a
    real QBO connection; we only need _build_bill_payload)."""
    from src.integrations.qbo_push import QBOPush
    pusher = QBOPush.__new__(QBOPush)
    pusher.firm_code = 'F1'
    pusher.client_code = 'CLI1'
    pusher.db_path = db
    pusher.sandbox = False
    pusher._tokens = {'realm_id': 'r1', 'access_token': 'tok',
                      'status': 'active'}
    pusher.base_url = 'http://test'
    pusher._resolve_vendor_qbo_id = lambda name: 'qbo_vendor_1'
    pusher._resolve_qbo_account_id = (
        lambda code: f'qbo_acc_{code}' if code else None)
    return pusher


def test_qbo_push_refuses_uncategorized_doc(tmp_path):
    db = tmp_path / 'qbo.db'
    _bootstrap(db)
    _seed_doc(db, 'BAD', vendor=None, amount=None,
              gl='5440', category='operating_expense', needs_cat=1)
    pusher = _make_qbo_pusher(db)
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        doc = conn.execute(
            "SELECT * FROM documents WHERE document_id='BAD'"
        ).fetchone()
    with pytest.raises(RuntimeError) as exc:
        pusher._build_bill_payload(doc)
    msg = str(exc.value)
    assert 'needs_categorization' in msg or 'categori' in msg.lower(), (
        f"refusal should mention categorisation; got: {msg}"
    )


def test_qbo_push_refuses_doc_with_no_gl_and_no_lines(tmp_path):
    """Edge case: doc without needs_categorization flag but also
    without gl_account and without line items. Still must refuse."""
    db = tmp_path / 'qbo2.db'
    _bootstrap(db)
    _seed_doc(db, 'NOGL', vendor='V', amount=50.0,
              gl=None, category=None, needs_cat=0)
    pusher = _make_qbo_pusher(db)
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        doc = conn.execute(
            "SELECT * FROM documents WHERE document_id='NOGL'"
        ).fetchone()
    with pytest.raises(RuntimeError) as exc:
        pusher._build_bill_payload(doc)
    assert 'gl_account' in str(exc.value) or 'categori' in str(exc.value).lower()


def test_qbo_push_accepts_categorized_single_line_doc(tmp_path):
    db = tmp_path / 'qbo3.db'
    _bootstrap(db)
    _seed_doc(db, 'OK', vendor='Bell', amount=42.0,
              gl='5400', category='telecom', needs_cat=0)
    pusher = _make_qbo_pusher(db)
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        doc = conn.execute(
            "SELECT * FROM documents WHERE document_id='OK'"
        ).fetchone()
    payload = pusher._build_bill_payload(doc)
    assert payload['VendorRef']['value'] == 'qbo_vendor_1'
    assert len(payload['Line']) == 1
    assert payload['Line'][0][
        'AccountBasedExpenseLineDetail']['AccountRef']['value'] == 'qbo_acc_5400'


def test_qbo_push_accepts_doc_with_line_items_even_if_doc_level_empty(tmp_path):
    """When invoice_lines exist, doc-level gl_account doesn't matter —
    lines drive the QBO push."""
    db = tmp_path / 'qbo4.db'
    _bootstrap(db)
    _seed_doc(db, 'LINES', vendor='V', amount=200.0,
              gl=None, category=None, needs_cat=0, has_lines=1,
              line_items=[
                  {'gl': '5420', 'cat': 'office',  'amount': 100.0},
                  {'gl': '5430', 'cat': 'travel',  'amount': 100.0},
              ])
    pusher = _make_qbo_pusher(db)
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        doc = conn.execute(
            "SELECT * FROM documents WHERE document_id='LINES'"
        ).fetchone()
    payload = pusher._build_bill_payload(doc)
    assert len(payload['Line']) == 2
    refs = sorted(
        l['AccountBasedExpenseLineDetail']['AccountRef']['value']
        for l in payload['Line']
    )
    assert refs == ['qbo_acc_5420', 'qbo_acc_5430']
