"""Edge case 2 — /apply_suggestion now rejects stale reads.

The suggestion panel renders AI-recommended field changes. Previously
the handler called the legacy ``update_document_fields`` with no
version guard, so if a reviewer left the panel open while another user
edited the doc, clicking "Apply" silently overwrote the newer value.

These tests verify that the handler now routes through
``update_document_fields_versioned`` with ``require_version=True``.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def dash(tmp_path, monkeypatch):
    """Boot review_dashboard pointing at a throwaway DB with one doc."""
    db_path = tmp_path / "sug.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            vendor TEXT, client_code TEXT, doc_type TEXT,
            amount REAL, document_date TEXT,
            gl_account TEXT, tax_code TEXT, category TEXT,
            review_status TEXT, manual_hold_reason TEXT,
            manual_hold_by TEXT, manual_hold_at TEXT,
            updated_at TEXT,
            version INTEGER DEFAULT 1
        );
        INSERT INTO documents (document_id, vendor, gl_account, version)
        VALUES ('D1', 'ACME', '5000', 1);
    """)
    conn.commit()
    conn.close()

    monkeypatch.setenv("OTOCPA_DB", str(db_path))
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db_path)

    def _open():
        c = sqlite3.connect(str(db_path))
        c.row_factory = rd._dict_factory
        return c
    monkeypatch.setattr(rd, "open_db", _open)
    return rd, db_path


def test_apply_suggestion_requires_version(dash):
    """Caller omits version — must be rejected (400 version_required)."""
    rd, _ = dash
    r = rd.update_document_fields_versioned(
        "D1", {"gl_account": "5010"}, body={}, require_version=True,
    )
    assert r.status == 400
    assert r.error == "version_required"


def test_apply_suggestion_409_on_stale_version(dash):
    """Caller carries v=1 but the row has moved to v=2 — 409."""
    rd, db = dash
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.execute("UPDATE documents SET version=2 WHERE document_id='D1'")
    conn.commit()
    r = rd.update_document_fields_versioned(
        "D1", {"gl_account": "5010"}, body={"version": 1},
        require_version=True,
    )
    assert r.status == 409
    assert r.current_version == 2
    payload = r.to_json()
    assert payload["error"] == "version_conflict"
    assert payload["reload_required"] is True
    # Row unchanged.
    row = conn.execute("SELECT gl_account FROM documents WHERE document_id='D1'").fetchone()
    assert row["gl_account"] == "5000"
    conn.close()


def test_apply_suggestion_succeeds_with_current_version(dash):
    rd, db = dash
    r = rd.update_document_fields_versioned(
        "D1", {"gl_account": "5020"}, body={"version": 1},
        require_version=True,
    )
    assert r.status == 200
    assert r.new_version == 2
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT gl_account, version FROM documents WHERE document_id='D1'").fetchone()
    assert row["gl_account"] == "5020"
    assert row["version"] == 2
    conn.close()


def test_apply_suggestion_increments_version(dash):
    """Each successful apply bumps the version by exactly 1."""
    rd, db = dash
    r1 = rd.update_document_fields_versioned(
        "D1", {"gl_account": "5020"}, body={"version": 1},
        require_version=True,
    )
    assert r1.status == 200 and r1.new_version == 2
    r2 = rd.update_document_fields_versioned(
        "D1", {"gl_account": "5030"}, body={"version": 2},
        require_version=True,
    )
    assert r2.status == 200 and r2.new_version == 3
    r3 = rd.update_document_fields_versioned(
        "D1", {"tax_code": "E"}, body={"version": 3},
        require_version=True,
    )
    assert r3.status == 200 and r3.new_version == 4


def test_apply_suggestion_rendered_form_embeds_version(dash):
    """render_learning_suggestions must emit a hidden ``expected_version``
    input so the click carries the row version back. Without it the
    handler's require_version=True check would always 400 for this
    flow."""
    rd, _ = dash
    row = {
        "document_id": "D1",
        "vendor": "ACME", "client_code": "C1", "doc_type": "invoice",
        "gl_account": "5000", "tax_code": "T", "category": "Office",
        "review_status": "NeedsReview",
        "version": 7,
    }
    # Stub suggestion_engine to return a known suggestion.
    class _Stub:
        def suggestions_for_document(self, **kw):
            return {"gl_account": [
                {"value": "5010", "support": "3", "confidence": "0.9", "source": "learned"},
            ]}

    import scripts.review_dashboard as _rd
    import unittest.mock as _mock
    with _mock.patch.object(_rd, "suggestion_engine", _Stub()):
        html = rd.render_learning_suggestions("D1", row, username="u", lang="en")
    assert 'name="expected_version"' in html
    # The row's actual version should be embedded, not a hardcoded 1.
    assert 'value="7"' in html
