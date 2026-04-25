"""Queue must NOT show silent defaults for failed-OCR documents.

Background: rcpt_16.png is a real document where OCR fully failed —
vendor=NULL, amount=NULL, document_date=NULL, confidence=0. Yet the
documents row shows gl_account='5440' and category='operating_expense'
because the OCR engine writes those as a silent default. The queue
was happily printing them as if they were real, hiding the fact that
the document is actually uncategorized.

This file pins the contract: the queue column shows the doc-level
value ONLY when OCR pulled real signal (vendor + amount). Otherwise
it shows "Non catégorisé" / "Uncategorized". Multi-line docs are
covered by test_no_redundant_doc_level_gl.py — this file focuses on
the no-line-items / failed-OCR cases.
"""
from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / 'q.db'
    secret = tmp_path / 's'
    secret.write_text('x' * 48)
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
    with sqlite3.connect(str(p)) as conn:
        conn.execute("INSERT INTO firms (firm_code) VALUES ('F1')")
        conn.execute(
            "INSERT INTO clients (client_code, client_name, firm_code, "
            "active, language) VALUES ('CLI1','Acme','F1',1,'fr')"
        )
        conn.commit()
    yield p, rd


def _seed(rd, doc_id, *, vendor=None, amount=None, gl=None, category=None,
          has_lines=0, confidence=None, line_items=None):
    with sqlite3.connect(str(rd.DB_PATH)) as conn:
        conn.execute(
            "INSERT INTO documents (document_id, file_name, client_code, "
            "vendor, amount, document_date, gl_account, tax_code, category, "
            "review_status, has_line_items, confidence, raw_result, created_at) "
            "VALUES (?, ?, 'CLI1', ?, ?, NULL, ?, '', ?, "
            "'NeedsReview', ?, ?, '{}', datetime('now'))",
            (doc_id, f'{doc_id}.png', vendor, amount, gl, category,
             has_lines, confidence),
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
                     li.get('amount', 10.0), li.get('gl', ''), li.get('cat', '')),
                )
        conn.commit()


def _ctx(rd):
    user = {'username': 'sam', 'role': 'owner', 'firm_code': 'F1',
            'language': 'fr', 'email': 'sam@example.com'}
    return rd.build_user_context(user), user


def _render(rd, lang='fr'):
    ctx, user = _ctx(rd)
    user['language'] = lang
    return rd.render_home(
        ctx=ctx, user=user, status='', q='', flash='', flash_error='',
        include_ignored=False, only_my_queue=False, only_unassigned=False,
        lang=lang, page=1, per_page=20,
    )


# ---------------------------------------------------------------------------
# Failed-OCR docs (the rcpt_16 bug)
# ---------------------------------------------------------------------------


def test_queue_shows_uncategorized_for_failed_ocr_doc_fr(db):
    """Repro of the rcpt_16 bug: vendor/amount NULL but gl_account is
    the OCR engine's silent default. Queue must hide the lie."""
    p, rd = db
    _seed(rd, 'doc_FAIL', vendor=None, amount=None,
          gl='5440', category='operating_expense',
          has_lines=0, confidence=0.0)
    html = _render(rd, lang='fr')
    # The fake values must NOT show up in the row's table cells.
    # (We can't just grep the whole HTML because nav/footer may
    # legitimately mention "5440" elsewhere in the future, so we
    # scope the check to a row that contains the doc_id.)
    row_match = re.search(
        r'<tr class="data-row"[^>]*>.*?</tr>\s*<tr id="row\d+"',
        html, re.DOTALL)
    assert row_match, "no data row rendered"
    # Find the row containing our doc_FAIL.
    doc_row = None
    for chunk in re.findall(
            r'<tr class="data-row"[^>]*>.*?</tr>',
            html, re.DOTALL):
        if 'doc_FAIL' in chunk:
            doc_row = chunk
            break
    assert doc_row, f"doc_FAIL row not found in HTML; html=\n{html[:2000]}"
    assert '5440' not in doc_row, (
        f"queue still shows fake GL '5440' for failed-OCR doc:\n{doc_row}"
    )
    assert 'operating_expense' not in doc_row, (
        f"queue still shows fake category for failed-OCR doc:\n{doc_row}"
    )
    assert 'Non catégorisé' in doc_row, (
        f"queue should show FR 'Non catégorisé' for failed-OCR doc:\n{doc_row}"
    )


def test_queue_shows_uncategorized_for_failed_ocr_doc_en(db):
    p, rd = db
    _seed(rd, 'doc_FAIL_EN', vendor=None, amount=None,
          gl='5440', category='operating_expense',
          has_lines=0, confidence=0.0)
    html = _render(rd, lang='en')
    doc_row = next(
        (chunk for chunk in re.findall(
            r'<tr class="data-row"[^>]*>.*?</tr>', html, re.DOTALL)
         if 'doc_FAIL_EN' in chunk),
        None,
    )
    assert doc_row, "doc_FAIL_EN row not found"
    assert '5440' not in doc_row
    assert 'operating_expense' not in doc_row
    assert 'Uncategorized' in doc_row


# ---------------------------------------------------------------------------
# Successful single-extraction (legitimate doc-level value)
# ---------------------------------------------------------------------------


def test_queue_shows_doc_level_when_ocr_succeeded(db):
    """Single-line doc with vendor + amount: the doc-level gl_account
    is real, show it as-is."""
    p, rd = db
    _seed(rd, 'doc_OK', vendor='Bell Canada', amount=42.50,
          gl='5400', category='telecom',
          has_lines=0, confidence=0.92)
    html = _render(rd, lang='fr')
    doc_row = next(
        (chunk for chunk in re.findall(
            r'<tr class="data-row"[^>]*>.*?</tr>', html, re.DOTALL)
         if 'doc_OK' in chunk),
        None,
    )
    assert doc_row
    assert '5400' in doc_row
    assert 'telecom' in doc_row
    assert 'Non catégorisé' not in doc_row


# ---------------------------------------------------------------------------
# Single-line invoice_lines case
# ---------------------------------------------------------------------------


def test_queue_shows_single_line_gl_for_single_line_doc(db):
    p, rd = db
    _seed(rd, 'doc_1L', vendor='V', amount=100.0,
          gl='IGNORED-DOC-LEVEL', category='IGNORED',
          has_lines=1, confidence=0.9,
          line_items=[{'gl': '5420', 'cat': 'office_supplies', 'amount': 100.0}])
    html = _render(rd, lang='fr')
    doc_row = next(
        (c for c in re.findall(r'<tr class="data-row"[^>]*>.*?</tr>',
                                 html, re.DOTALL)
         if 'doc_1L' in c),
        None,
    )
    assert doc_row
    assert '5420' in doc_row, f"single-line GL not shown:\n{doc_row}"
    assert 'IGNORED-DOC-LEVEL' not in doc_row
    assert 'office_supplies' in doc_row


# ---------------------------------------------------------------------------
# Multi-GL invoice_lines case (count + tooltip)
# ---------------------------------------------------------------------------


def test_queue_shows_count_for_multi_gl_doc_fr(db):
    p, rd = db
    _seed(rd, 'doc_3GL', vendor='V', amount=300.0,
          gl='X', category='Y', has_lines=1, confidence=0.9,
          line_items=[
              {'gl': '5420', 'cat': 'office', 'amount': 100.0},
              {'gl': '5430', 'cat': 'travel', 'amount': 100.0},
              {'gl': '5440', 'cat': 'meals',  'amount': 100.0},
          ])
    html = _render(rd, lang='fr')
    doc_row = next(
        (c for c in re.findall(r'<tr class="data-row"[^>]*>.*?</tr>',
                                 html, re.DOTALL)
         if 'doc_3GL' in c),
        None,
    )
    assert doc_row
    # Count present
    assert '3 comptes GL' in doc_row, (
        f"FR multi-GL queue cell missing '3 comptes GL':\n{doc_row}"
    )
    # Tooltip lists the actual GL accounts. Scope to the GL cell
    # specifically (the Category cell also has a tooltip listing
    # categories — that's a separate cell).
    gl_cell = re.search(
        r'<span data-cell="gl"[^>]*title="([^"]+)"', doc_row)
    assert gl_cell, f"no GL-cell tooltip in row:\n{doc_row}"
    for gl in ('5420', '5430', '5440'):
        assert gl in gl_cell.group(1), (
            f"GL {gl} missing from tooltip {gl_cell.group(1)!r}"
        )


def test_queue_shows_count_for_multi_gl_doc_en(db):
    p, rd = db
    _seed(rd, 'doc_3GL_EN', vendor='V', amount=300.0,
          gl='X', category='Y', has_lines=1, confidence=0.9,
          line_items=[
              {'gl': '5420', 'cat': 'office', 'amount': 100.0},
              {'gl': '5430', 'cat': 'travel', 'amount': 100.0},
              {'gl': '5440', 'cat': 'meals',  'amount': 100.0},
          ])
    html = _render(rd, lang='en')
    doc_row = next(
        (c for c in re.findall(r'<tr class="data-row"[^>]*>.*?</tr>',
                                 html, re.DOTALL)
         if 'doc_3GL_EN' in c),
        None,
    )
    assert doc_row
    assert '3 GLs' in doc_row, (
        f"EN multi-GL queue cell missing '3 GLs':\n{doc_row}"
    )


# ---------------------------------------------------------------------------
# has_line_items flag set but no actual line rows yet
# ---------------------------------------------------------------------------


def test_queue_shows_uncategorized_when_lines_flag_but_no_lines(db):
    """Edge case: has_line_items=1 but invoice_lines table is empty
    for that doc. Treat as uncategorized rather than crashing or
    falling back to doc-level."""
    p, rd = db
    _seed(rd, 'doc_FLAG', vendor='V', amount=100.0,
          gl='5440', category='operating_expense',
          has_lines=1, confidence=0.9, line_items=None)
    html = _render(rd, lang='fr')
    doc_row = next(
        (c for c in re.findall(r'<tr class="data-row"[^>]*>.*?</tr>',
                                 html, re.DOTALL)
         if 'doc_FLAG' in c),
        None,
    )
    assert doc_row
    assert 'Non catégorisé' in doc_row
    assert '5440' not in doc_row


# ---------------------------------------------------------------------------
# Document detail page mirrors the same honesty
# ---------------------------------------------------------------------------


def test_doc_detail_shows_uncategorized_for_failed_ocr(db):
    p, rd = db
    _seed(rd, 'doc_FAIL_DETAIL', vendor=None, amount=None,
          gl='5440', category='operating_expense',
          has_lines=0, confidence=0.0)
    ctx, user = _ctx(rd)
    user['language'] = 'fr'
    html = rd.render_document('doc_FAIL_DETAIL', ctx, user, '', '', lang='fr')
    # The summary card is everything between the summary <h3> and the
    # next <h3>. That spans the GL + Category cells we care about.
    summary = re.search(
        r'<h3>Résumé</h3>(.*?)<h3>',
        html, re.DOTALL,
    )
    if not summary:
        summary = re.search(
            r'<h3>Summary</h3>(.*?)<h3>',
            html, re.DOTALL,
        )
    assert summary, (
        "couldn't locate the summary card via the FR or EN heading"
    )
    summary_html = summary.group(1)
    assert '5440' not in summary_html, (
        f"summary still shows fake GL '5440':\n{summary_html}"
    )
    assert 'operating_expense' not in summary_html, (
        f"summary still shows fake category:\n{summary_html}"
    )
    assert summary_html.count('Non catégorisé') >= 2, (
        f"summary should show 'Non catégorisé' for both GL and "
        f"Category cells, got:\n{summary_html}"
    )
