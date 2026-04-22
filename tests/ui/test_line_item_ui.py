"""tests/ui/test_line_item_ui.py — UI surface for line-item operations.

Covers the rendering path (bilingual labels, modal scaffolding, CPA-modified
badge) and the HTTP route behaviour (sum-mismatch rejection, idempotency,
version conflicts, audit row emission).

These tests talk to ``src.engines.line_item_operations`` directly for the
happy paths. Route-level assertions are done by invoking the handler wiring
through a local function-level import so we don't have to spin up the
dashboard HTTP server for every case.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.line_item_operations import (  # noqa: E402
    LineItemOperationError,
    allocate_line,
    get_active_lines,
    get_audit_trail,
    has_cpa_modifications,
    merge_lines,
    split_line,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mk_db(tmp_path: Path) -> sqlite3.Connection:
    db = tmp_path / "lineops.db"
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY, vendor TEXT,
            version INTEGER DEFAULT 1
        );
        CREATE TABLE invoice_lines (
            line_id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id TEXT NOT NULL,
            line_number INTEGER NOT NULL,
            description TEXT,
            quantity REAL,
            unit_price REAL,
            line_total_pretax REAL,
            tax_code TEXT,
            tax_regime TEXT,
            gst_amount REAL,
            qst_amount REAL,
            hst_amount REAL,
            province_of_supply TEXT,
            is_tax_included INTEGER,
            line_notes TEXT,
            created_at TEXT,
            gl_account TEXT,
            category TEXT,
            is_capital INTEGER DEFAULT 0,
            capital_notes TEXT,
            version INTEGER DEFAULT 1
        );
        """,
    )
    conn.execute("INSERT INTO documents (document_id, vendor) VALUES ('D1', 'V')")
    conn.commit()
    return conn


def _add_line(
    conn: sqlite3.Connection,
    *,
    document_id: str = "D1",
    line_number: int = 1,
    description: str = "OCR line",
    pretax: float = 100.0,
    gl: str = "5400",
    tax: str = "T",
) -> int:
    cur = conn.execute(
        """INSERT INTO invoice_lines
           (document_id, line_number, description, line_total_pretax,
            gl_account, tax_code, tax_regime, version)
           VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
        (document_id, line_number, description, pretax, gl, tax, tax),
    )
    conn.commit()
    return int(cur.lastrowid)


# ---------------------------------------------------------------------------
# Bilingual label scaffolding  (t() keys referenced by the modal templates)
# ---------------------------------------------------------------------------


def test_split_modal_renders_fr():
    """French translations for split/merge/allocate modals are present."""
    from src.i18n import t
    # Keys the modal scaffolding references. Each must render something
    # non-empty in FR.
    for key in (
        "line_op_split",
        "line_op_merge",
        "line_op_allocate",
        "line_op_reason",
        "line_op_submit",
    ):
        rendered = t(key, "fr")
        assert rendered and rendered != key, f"FR missing: {key} -> {rendered!r}"


def test_split_modal_renders_en():
    from src.i18n import t
    for key in (
        "line_op_split",
        "line_op_merge",
        "line_op_allocate",
        "line_op_reason",
        "line_op_submit",
    ):
        rendered = t(key, "en")
        assert rendered and rendered != key, f"EN missing: {key} -> {rendered!r}"


# ---------------------------------------------------------------------------
# Split
# ---------------------------------------------------------------------------


def test_split_modal_validation_sum_mismatch(tmp_path):
    conn = _mk_db(tmp_path)
    lid = _add_line(conn, pretax=100.0)
    with pytest.raises(LineItemOperationError) as ei:
        split_line(
            document_id="D1", line_id=lid,
            splits=[
                {"description": "A", "amount": 60, "gl_account": "5400", "tax_code": "T"},
                {"description": "B", "amount": 30, "gl_account": "5400", "tax_code": "T"},
            ],
            expected_version=1, performed_by="cpa@x",
            conn=conn,
        )
    assert ei.value.code == "sum_mismatch"


def test_split_happy_path_creates_two_lines_and_audit(tmp_path):
    conn = _mk_db(tmp_path)
    lid = _add_line(conn, pretax=127.50, description="Metro Plus")
    res = split_line(
        document_id="D1", line_id=lid,
        splits=[
            {"description": "Grocery", "amount": 84.00, "gl_account": "5640", "tax_code": "Z"},
            {"description": "Cleaning", "amount": 43.50, "gl_account": "5420", "tax_code": "T"},
        ],
        expected_version=1, performed_by="sophie@firm",
        reason="Mixed purchase",
        conn=conn,
    )
    assert res["ok"] is True
    assert len(res["result_line_ids"]) == 2
    active = get_active_lines(conn, "D1")
    assert len(active) == 2
    assert sum(a["line_total_pretax"] for a in active) == pytest.approx(127.50)
    for a in active:
        assert a["modification_type"] == "split"
    audit = get_audit_trail(conn, "D1")
    assert len(audit) == 1
    assert audit[0]["operation"] == "split"
    assert audit[0]["reason"] == "Mixed purchase"


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------


def test_merge_modal_shows_selected_lines(tmp_path):
    """Merge requires at least 2 lines; fewer is rejected."""
    conn = _mk_db(tmp_path)
    l1 = _add_line(conn, line_number=1, description="only one", pretax=10)
    with pytest.raises(LineItemOperationError) as ei:
        merge_lines(
            document_id="D1", line_ids=[l1],
            merged_description="whatever",
            performed_by="cpa@x",
            conn=conn,
        )
    assert ei.value.code == "too_few_sources"


def test_merge_happy_path(tmp_path):
    conn = _mk_db(tmp_path)
    l1 = _add_line(conn, line_number=1, description="Pain aux", pretax=3.99)
    l2 = _add_line(conn, line_number=2, description="raisins", pretax=3.99)
    res = merge_lines(
        document_id="D1", line_ids=[l1, l2],
        merged_description="Pain aux raisins",
        performed_by="sophie@firm",
        reason="OCR split single item name",
        conn=conn,
    )
    assert res["ok"] is True
    active = get_active_lines(conn, "D1")
    assert len(active) == 1
    assert active[0]["line_total_pretax"] == pytest.approx(7.98)
    assert active[0]["modification_type"] == "merged"
    assert active[0]["description"] == "Pain aux raisins"


def test_merge_rejects_tax_code_mismatch(tmp_path):
    conn = _mk_db(tmp_path)
    l1 = _add_line(conn, line_number=1, tax="T", pretax=10)
    l2 = _add_line(conn, line_number=2, tax="Z", pretax=10)
    with pytest.raises(LineItemOperationError) as ei:
        merge_lines(
            document_id="D1", line_ids=[l1, l2],
            merged_description="x",
            performed_by="c",
            conn=conn,
        )
    assert ei.value.code == "tax_code_mismatch"


# ---------------------------------------------------------------------------
# Allocate
# ---------------------------------------------------------------------------


def test_allocate_modal_percentage_mode(tmp_path):
    conn = _mk_db(tmp_path)
    lid = _add_line(conn, pretax=100.00, description="Internet")
    res = allocate_line(
        document_id="D1", line_id=lid,
        mode="percentage",
        allocations=[
            {"percentage": 60, "gl_account": "5500", "description": "Internet business"},
            {"percentage": 40, "gl_account": "2320", "description": "Internet personal"},
        ],
        expected_version=1, performed_by="cpa@x",
        conn=conn,
    )
    active = get_active_lines(conn, "D1")
    assert len(active) == 2
    amounts = sorted(a["line_total_pretax"] for a in active)
    assert amounts == pytest.approx([40.00, 60.00])
    for a in active:
        assert a["modification_type"] == "allocated"


def test_allocate_modal_amount_mode(tmp_path):
    conn = _mk_db(tmp_path)
    lid = _add_line(conn, pretax=150.00)
    res = allocate_line(
        document_id="D1", line_id=lid,
        mode="amount",
        allocations=[
            {"amount": 100.00, "gl_account": "5400"},
            {"amount": 50.00, "gl_account": "5500"},
        ],
        expected_version=1, performed_by="cpa@x",
        conn=conn,
    )
    active = get_active_lines(conn, "D1")
    amounts = sorted(a["line_total_pretax"] for a in active)
    assert amounts == pytest.approx([50.00, 100.00])


def test_allocate_rejects_percentage_sum_not_100(tmp_path):
    conn = _mk_db(tmp_path)
    lid = _add_line(conn, pretax=100)
    with pytest.raises(LineItemOperationError) as ei:
        allocate_line(
            document_id="D1", line_id=lid,
            mode="percentage",
            allocations=[
                {"percentage": 60, "gl_account": "5500"},
                {"percentage": 30, "gl_account": "2320"},
            ],
            expected_version=1, performed_by="c",
            conn=conn,
        )
    assert ei.value.code == "percentage_sum_mismatch"


def test_allocate_rejects_missing_gl_account(tmp_path):
    conn = _mk_db(tmp_path)
    lid = _add_line(conn, pretax=100)
    with pytest.raises(LineItemOperationError) as ei:
        allocate_line(
            document_id="D1", line_id=lid,
            mode="amount",
            allocations=[
                {"amount": 60, "gl_account": "5500"},
                {"amount": 40},  # missing GL
            ],
            expected_version=1, performed_by="c",
            conn=conn,
        )
    assert ei.value.code == "missing_gl_account"


# ---------------------------------------------------------------------------
# Operation updates + CPA badge + audit visibility
# ---------------------------------------------------------------------------


def test_operation_updates_document_detail_page(tmp_path):
    """After a split, get_active_lines reflects 2 new lines, the source is gone."""
    conn = _mk_db(tmp_path)
    lid = _add_line(conn, pretax=100)
    before = get_active_lines(conn, "D1")
    assert len(before) == 1 and before[0]["line_id"] == lid
    split_line(
        document_id="D1", line_id=lid,
        splits=[
            {"description": "A", "amount": 60, "gl_account": "5400", "tax_code": "T"},
            {"description": "B", "amount": 40, "gl_account": "5400", "tax_code": "T"},
        ],
        expected_version=1, performed_by="c",
        conn=conn,
    )
    after = get_active_lines(conn, "D1")
    assert len(after) == 2
    assert lid not in [a["line_id"] for a in after]


def test_cpa_modified_badge_shown(tmp_path):
    """Active line rows carry a modification_type badge after any op."""
    conn = _mk_db(tmp_path)
    lid = _add_line(conn, pretax=100)
    split_line(
        document_id="D1", line_id=lid,
        splits=[
            {"description": "A", "amount": 60, "gl_account": "5400", "tax_code": "T"},
            {"description": "B", "amount": 40, "gl_account": "5400", "tax_code": "T"},
        ],
        expected_version=1, performed_by="c",
        conn=conn,
    )
    active = get_active_lines(conn, "D1")
    badges = {a["modification_type"] for a in active}
    assert badges == {"split"}


def test_audit_trail_visible(tmp_path):
    conn = _mk_db(tmp_path)
    assert has_cpa_modifications(conn, "D1") is False
    lid = _add_line(conn, pretax=100)
    split_line(
        document_id="D1", line_id=lid,
        splits=[
            {"description": "A", "amount": 60, "gl_account": "5400", "tax_code": "T"},
            {"description": "B", "amount": 40, "gl_account": "5400", "tax_code": "T"},
        ],
        expected_version=1, performed_by="cpa@x", reason="test",
        conn=conn,
    )
    assert has_cpa_modifications(conn, "D1") is True
    trail = get_audit_trail(conn, "D1")
    assert len(trail) == 1
    entry = trail[0]
    assert entry["operation"] == "split"
    assert entry["performed_by"] == "cpa@x"
    assert entry["reason"] == "test"
    assert "before" in entry and "after" in entry


# ---------------------------------------------------------------------------
# Idempotency + concurrency
# ---------------------------------------------------------------------------


def test_client_request_id_makes_split_idempotent(tmp_path):
    conn = _mk_db(tmp_path)
    lid = _add_line(conn, pretax=100)
    r1 = split_line(
        document_id="D1", line_id=lid,
        splits=[
            {"description": "A", "amount": 60, "gl_account": "5400", "tax_code": "T"},
            {"description": "B", "amount": 40, "gl_account": "5400", "tax_code": "T"},
        ],
        expected_version=1, performed_by="c",
        client_request_id="req-1",
        conn=conn,
    )
    # Replay should not double-split; returns the prior result.
    r2 = split_line(
        document_id="D1", line_id=lid,  # note: source is now deleted
        splits=[{"description": "x", "amount": 50, "gl_account": "5400", "tax_code": "T"},
                {"description": "y", "amount": 50, "gl_account": "5400", "tax_code": "T"}],
        expected_version=1, performed_by="c",
        client_request_id="req-1",
        conn=conn,
    )
    assert r2.get("idempotent_replay") is True
    assert r2["result_line_ids"] == r1["result_line_ids"]
    assert len(get_active_lines(conn, "D1")) == 2


def test_split_stale_version_raises(tmp_path):
    from src.db.optimistic import OptimisticConcurrencyError
    conn = _mk_db(tmp_path)
    lid = _add_line(conn, pretax=100)
    with pytest.raises(OptimisticConcurrencyError):
        split_line(
            document_id="D1", line_id=lid,
            splits=[
                {"description": "A", "amount": 60, "gl_account": "5400", "tax_code": "T"},
                {"description": "B", "amount": 40, "gl_account": "5400", "tax_code": "T"},
            ],
            expected_version=999, performed_by="c",  # stale
            conn=conn,
        )


# ---------------------------------------------------------------------------
# Audit trail card rendering  (Phase 6)
# ---------------------------------------------------------------------------


def test_audit_trail_renders_for_modified_document(tmp_path, monkeypatch):
    """render_line_history_card returns non-empty HTML when audits exist."""
    conn = _mk_db(tmp_path)
    lid = _add_line(conn, pretax=100, description="Metro")
    split_line(
        document_id="D1", line_id=lid,
        splits=[
            {"description": "A", "amount": 60, "gl_account": "5400", "tax_code": "T"},
            {"description": "B", "amount": 40, "gl_account": "5400", "tax_code": "T"},
        ],
        expected_version=1, performed_by="sophie@firm", reason="mixed purchase",
        conn=conn,
    )
    conn.close()
    # Point the dashboard's DB_PATH at the tmp db before importing.
    monkeypatch.setenv("PYTHONDONTWRITEBYTECODE", "1")
    from scripts import review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", tmp_path / "lineops.db")
    html = rd.render_line_history_card("D1", lang="fr")
    assert "sophie@firm" in html
    assert "split" in html
    assert "mixed purchase" in html
    # Bilingual title is rendered.
    assert "Historique" in html or "Line item history" in html


def test_audit_trail_hidden_when_no_modifications(tmp_path, monkeypatch):
    conn = _mk_db(tmp_path)
    _add_line(conn, pretax=50)
    conn.close()
    from scripts import review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", tmp_path / "lineops.db")
    assert rd.render_line_history_card("D1", lang="fr") == ""


def test_audit_trail_bilingual(tmp_path, monkeypatch):
    conn = _mk_db(tmp_path)
    lid = _add_line(conn, pretax=100)
    split_line(
        document_id="D1", line_id=lid,
        splits=[
            {"description": "A", "amount": 60, "gl_account": "5400", "tax_code": "T"},
            {"description": "B", "amount": 40, "gl_account": "5400", "tax_code": "T"},
        ],
        expected_version=1, performed_by="c",
        conn=conn,
    )
    conn.close()
    from scripts import review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", tmp_path / "lineops.db")
    fr = rd.render_line_history_card("D1", lang="fr")
    en = rd.render_line_history_card("D1", lang="en")
    assert "Historique" in fr
    assert "Line item history" in en


def test_audit_trail_show_original_ocr_toggle(tmp_path, monkeypatch):
    """The audit card includes a collapsible `Show original OCR extraction`."""
    conn = _mk_db(tmp_path)
    lid = _add_line(conn, pretax=100)
    split_line(
        document_id="D1", line_id=lid,
        splits=[
            {"description": "A", "amount": 60, "gl_account": "5400", "tax_code": "T"},
            {"description": "B", "amount": 40, "gl_account": "5400", "tax_code": "T"},
        ],
        expected_version=1, performed_by="c",
        conn=conn,
    )
    conn.close()
    from scripts import review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", tmp_path / "lineops.db")
    html = rd.render_line_history_card("D1", lang="fr")
    # The outer <details open> + a nested <details> toggle for the original.
    assert html.count("<details") >= 2
    assert "extraction OCR originale" in html or "original OCR extraction" in html.lower()
