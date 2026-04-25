"""Document-level Category/GL is no longer the source of truth.

Goal: line-level data wins. The queue list and document detail must
reflect the line items, not the doc-level columns. Specifically:

* Queue list with multi-line doc → Category column shows "Multiple"
  (or "Plusieurs" in FR) and GL column likewise — NOT the stale
  doc-level value.
* Queue list with single-line doc → shows that one line's category.
* Document detail with line items → no doc-level Category input in
  the edit form, no doc-level GL input either; instead a banner
  explains that lines define the entries.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def db(tmp_path, monkeypatch):
    import sqlite3
    p = tmp_path / 'q.db'
    secret = tmp_path / 's'
    secret.write_text('x' * 48)
    # Pre-create canonical tables so bootstrap_schema's ALTER calls
    # have something to attach to.
    c = sqlite3.connect(str(p))
    c.executescript(
        """
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (
            client_code TEXT PRIMARY KEY, client_name TEXT,
            contact_email TEXT, firm_code TEXT,
            active INTEGER DEFAULT 1, version INTEGER DEFAULT 1,
            language TEXT DEFAULT 'fr',
            portal_token TEXT,
            portal_token_created_at TEXT,
            portal_mode TEXT DEFAULT 'single'
        );
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, file_path TEXT, client_code TEXT,
            firm_code TEXT, vendor TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT,
            category TEXT, review_status TEXT,
            raw_result TEXT, has_line_items INTEGER DEFAULT 0,
            confidence TEXT, raw_ocr_text TEXT,
            uploaded_at TEXT, created_at TEXT, updated_at TEXT,
            version INTEGER DEFAULT 1
        );
        """
    )
    c.commit(); c.close()
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, 'DB_PATH', p)
    monkeypatch.setattr(rd, 'PASSWORD_LINK_SECRET_FILE', str(secret))
    rd.bootstrap_schema()
    # Seed firm + client so document INSERTs reference real targets.
    with sqlite3.connect(str(p)) as conn:
        conn.execute("INSERT INTO firms (firm_code) VALUES ('F1')")
        conn.execute(
            "INSERT INTO clients (client_code, client_name, firm_code, "
            "active, language) VALUES ('CLI1','Acme','F1',1,'fr')"
        )
        conn.commit()
    yield p, rd


def _seed_doc(rd, doc_id: str, *, has_lines: int = 0,
              category: str = '', gl: str = '',
              line_items: list[dict] | None = None) -> None:
    """Insert a document and optional invoice_lines."""
    import sqlite3
    with sqlite3.connect(str(rd.DB_PATH)) as conn:
        conn.execute(
            "INSERT INTO documents (document_id, file_name, client_code, "
            "vendor, amount, document_date, gl_account, tax_code, category, "
            "review_status, has_line_items, raw_result, created_at) "
            "VALUES (?, ?, 'CLI1', 'TestVendor', 100.00, '2026-04-01', ?, ?, "
            "?, 'New', ?, '{}', datetime('now'))",
            (doc_id, f'{doc_id}.pdf', gl, '', category, has_lines),
        )
        if line_items:
            from src.engines.line_item_engine import _ensure_invoice_lines_table
            _ensure_invoice_lines_table(conn)
            for i, li in enumerate(line_items, start=1):
                conn.execute(
                    "INSERT INTO invoice_lines (document_id, line_number, "
                    "description, line_total_pretax, gl_account, category) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (doc_id, i, li.get('desc', f'line {i}'),
                     li.get('amount', 10.0),
                     li.get('gl', ''), li.get('cat', '')),
                )
        conn.commit()


def _build_ctx(rd) -> dict:
    """Minimal ctx + user for the queue render. Uses
    build_user_context so we get every flag the renderer touches
    (can_view_all_clients, role-derived perms, etc.)."""
    user = {'username': 'sam', 'role': 'owner', 'firm_code': 'F1',
            'language': 'fr', 'email': 'sam@example.com'}
    return rd.build_user_context(user)


def test_queue_shows_multiple_for_multi_line_docs(db):
    p, rd = db
    _seed_doc(rd, 'D_MULTI',
              has_lines=1, category='Old Doc Cat', gl='6000',
              line_items=[
                  {'gl': '6000', 'cat': 'Office', 'amount': 50.0},
                  {'gl': '6500', 'cat': 'Travel', 'amount': 50.0},
              ])
    ctx = _build_ctx(rd)
    rows = rd.get_documents(ctx=ctx, limit=10)
    assert len(rows) == 1, f'expected 1 doc, got {rows}'
    assert int(rows[0]['has_line_items']) == 1

    html = rd.render_home(
        ctx=ctx,
        user={'username': 'sam', 'role': 'owner', 'language': 'fr'},
        status='', q='', flash='', flash_error='',
        include_ignored=False,
        only_my_queue=False, only_unassigned=False,
        lang='fr', page=1, per_page=20,
    )
    # The stale doc-level "Old Doc Cat" must NOT leak into the row;
    # the line-derived "Plusieurs" should be there instead.
    assert 'Old Doc Cat' not in html, (
        "Queue still shows stale doc-level Category for multi-line doc"
    )
    assert 'Plusieurs' in html, (
        "Queue should show 'Plusieurs' for multi-category multi-line doc"
    )


def test_queue_shows_single_category_for_single_line(db):
    p, rd = db
    _seed_doc(rd, 'D_SINGLE',
              has_lines=1, category='Old Doc Cat', gl='6000',
              line_items=[
                  {'gl': '6000', 'cat': 'Office Supplies', 'amount': 100.0},
              ])
    ctx = _build_ctx(rd)
    rows = rd.get_documents(ctx=ctx, limit=10)
    html = rd.render_home(
        ctx=ctx,
        user={'username': 'sam', 'role': 'owner', 'language': 'fr'},
        status='', q='', flash='', flash_error='',
        include_ignored=False,
        only_my_queue=False, only_unassigned=False,
        lang='fr', page=1, per_page=20,
    )
    assert 'Office Supplies' in html
    assert 'Plusieurs' not in html, (
        "Single-category line should NOT show 'Plusieurs'"
    )


def test_queue_falls_back_to_doc_level_when_no_lines(db):
    p, rd = db
    _seed_doc(rd, 'D_NOLINES',
              has_lines=0, category='Office', gl='6000')
    ctx = _build_ctx(rd)
    rows = rd.get_documents(ctx=ctx, limit=10)
    html = rd.render_home(
        ctx=ctx,
        user={'username': 'sam', 'role': 'owner', 'language': 'fr'},
        status='', q='', flash='', flash_error='',
        include_ignored=False,
        only_my_queue=False, only_unassigned=False,
        lang='fr', page=1, per_page=20,
    )
    assert 'Office' in html
    assert '6000' in html


def test_document_detail_no_doc_level_category_input_when_lines(db, monkeypatch):
    """The edit form must not include a doc-level Category input when
    has_line_items=1: line items are the source of truth."""
    p, rd = db
    _seed_doc(rd, 'D_MULTI',
              has_lines=1, category='Stale', gl='6000',
              line_items=[
                  {'gl': '6000', 'cat': 'Office', 'amount': 50.0},
                  {'gl': '6500', 'cat': 'Travel', 'amount': 50.0},
              ])
    # Render document detail
    user = {'username': 'sam', 'role': 'owner', 'language': 'fr'}
    ctx = _build_ctx(rd)
    html = rd.render_document('D_MULTI', ctx, user, '', '', lang='fr')
    # Edit form must not have a doc-level category input
    assert 'name="category"' not in html, (
        "Edit form still has doc-level <input name=category> "
        "when has_line_items=1 — this duplicates line-level data."
    )
    # Edit form must not have a doc-level gl_account input either
    assert 'name="gl_account"' not in html, (
        "Edit form still has doc-level <input name=gl_account> "
        "when has_line_items=1."
    )


def test_document_detail_shows_lines_count_summary(db):
    p, rd = db
    _seed_doc(rd, 'D_MULTI',
              has_lines=1, category='Stale', gl='6000',
              line_items=[
                  {'gl': '6000', 'cat': 'Office', 'amount': 50.0},
                  {'gl': '6500', 'cat': 'Travel', 'amount': 50.0},
              ])
    user = {'username': 'sam', 'role': 'owner', 'language': 'fr'}
    ctx = _build_ctx(rd)
    html = rd.render_document('D_MULTI', ctx, user, '', '', lang='fr')
    # The banner should mention number of lines and GL accounts
    assert ('2' in html and 'poste' in html and 'compte' in html.lower()), (
        "Document detail missing 'N postes / M comptes GL' summary"
    )


def test_document_detail_single_line_keeps_doc_level_inputs(db):
    """When has_line_items=0 (single-line/no-lines), the edit form
    still has Category and GL inputs (legacy behavior is preserved)."""
    p, rd = db
    _seed_doc(rd, 'D_NOLINES',
              has_lines=0, category='Office', gl='6000')
    user = {'username': 'sam', 'role': 'owner', 'language': 'fr'}
    ctx = _build_ctx(rd)
    html = rd.render_document('D_NOLINES', ctx, user, '', '', lang='fr')
    assert 'name="category"' in html
    assert 'name="gl_account"' in html
