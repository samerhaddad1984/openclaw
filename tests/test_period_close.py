"""
tests/test_period_close.py — pytest tests for the period-end close checklist.

Covers:
  - DB table creation (ensure_period_close_tables)
  - Default checklist creation (get_or_create_period_checklist)
  - Item status updates (update_checklist_item)
  - Period completeness check (is_period_complete)
  - Period locking (lock_period, is_period_locked, get_lock_info)
  - Document period extraction (get_document_period)
  - PDF generation (generate_period_close_pdf)
  - Migration script declares the tables
  - review_dashboard exposes render_period_close, routes, and nav link
  - i18n keys present for all period-close strings
"""
from __future__ import annotations

import inspect
import json
import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _in_memory_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Core module: table creation
# ---------------------------------------------------------------------------

class TestEnsurePeriodCloseTables:
    def test_creates_period_close_table(self):
        from src.agents.core.period_close import ensure_period_close_tables
        conn = _in_memory_db()
        ensure_period_close_tables(conn)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "period_close" in tables

    def test_creates_period_close_locks_table(self):
        from src.agents.core.period_close import ensure_period_close_tables
        conn = _in_memory_db()
        ensure_period_close_tables(conn)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "period_close_locks" in tables

    def test_idempotent(self):
        from src.agents.core.period_close import ensure_period_close_tables
        conn = _in_memory_db()
        ensure_period_close_tables(conn)
        ensure_period_close_tables(conn)  # must not raise


# ---------------------------------------------------------------------------
# Core module: default checklist creation
# ---------------------------------------------------------------------------

class TestGetOrCreatePeriodChecklist:
    def test_creates_seven_default_items(self):
        from src.agents.core.period_close import (
            DEFAULT_CHECKLIST_KEYS,
            get_or_create_period_checklist,
        )
        conn = _in_memory_db()
        items = get_or_create_period_checklist(conn, "ACME", "2025-01")
        assert len(items) == 7
        assert len(items) == len(DEFAULT_CHECKLIST_KEYS)

    def test_default_status_is_open(self):
        from src.agents.core.period_close import get_or_create_period_checklist
        conn = _in_memory_db()
        items = get_or_create_period_checklist(conn, "ACME", "2025-01")
        for item in items:
            assert item["status"] == "open"

    def test_keys_match_defaults(self):
        from src.agents.core.period_close import (
            DEFAULT_CHECKLIST_KEYS,
            get_or_create_period_checklist,
        )
        conn = _in_memory_db()
        items = get_or_create_period_checklist(conn, "ACME", "2025-01")
        keys_in_db = [item["checklist_item"] for item in items]
        assert keys_in_db == DEFAULT_CHECKLIST_KEYS

    def test_second_call_does_not_duplicate(self):
        from src.agents.core.period_close import get_or_create_period_checklist
        conn = _in_memory_db()
        get_or_create_period_checklist(conn, "ACME", "2025-01")
        items2 = get_or_create_period_checklist(conn, "ACME", "2025-01")
        assert len(items2) == 7

    def test_different_clients_are_independent(self):
        from src.agents.core.period_close import get_or_create_period_checklist
        conn = _in_memory_db()
        get_or_create_period_checklist(conn, "ACME", "2025-01")
        items_b = get_or_create_period_checklist(conn, "BETA", "2025-01")
        assert len(items_b) == 7

    def test_different_periods_are_independent(self):
        from src.agents.core.period_close import get_or_create_period_checklist
        conn = _in_memory_db()
        get_or_create_period_checklist(conn, "ACME", "2025-01")
        items_feb = get_or_create_period_checklist(conn, "ACME", "2025-02")
        assert len(items_feb) == 7


# ---------------------------------------------------------------------------
# Core module: update checklist item
# ---------------------------------------------------------------------------

class TestUpdateChecklistItem:
    def _setup(self) -> tuple[sqlite3.Connection, int]:
        from src.agents.core.period_close import get_or_create_period_checklist
        conn = _in_memory_db()
        items = get_or_create_period_checklist(conn, "ACME", "2025-03")
        return conn, items[0]["id"]

    def test_mark_complete(self):
        from src.agents.core.period_close import update_checklist_item
        conn, iid = self._setup()
        update_checklist_item(conn, iid, "complete", completed_by="alice")
        row = conn.execute("SELECT * FROM period_close WHERE id=?", (iid,)).fetchone()
        assert row["status"] == "complete"
        assert row["completed_by"] == "alice"
        assert row["completed_at"] is not None

    def test_mark_waived(self):
        from src.agents.core.period_close import update_checklist_item
        conn, iid = self._setup()
        update_checklist_item(conn, iid, "waived")
        row = conn.execute("SELECT * FROM period_close WHERE id=?", (iid,)).fetchone()
        assert row["status"] == "waived"

    def test_reopen(self):
        from src.agents.core.period_close import update_checklist_item
        conn, iid = self._setup()
        update_checklist_item(conn, iid, "complete", completed_by="alice")
        update_checklist_item(conn, iid, "open")
        row = conn.execute("SELECT * FROM period_close WHERE id=?", (iid,)).fetchone()
        assert row["status"] == "open"
        assert row["completed_by"] is None
        assert row["completed_at"] is None

    def test_save_notes(self):
        from src.agents.core.period_close import update_checklist_item
        conn, iid = self._setup()
        update_checklist_item(conn, iid, "open", notes="Checked manually")
        row = conn.execute("SELECT * FROM period_close WHERE id=?", (iid,)).fetchone()
        assert row["notes"] == "Checked manually"

    def test_save_responsible_user(self):
        from src.agents.core.period_close import update_checklist_item
        conn, iid = self._setup()
        update_checklist_item(conn, iid, "open", responsible_user="bob")
        row = conn.execute("SELECT * FROM period_close WHERE id=?", (iid,)).fetchone()
        assert row["responsible_user"] == "bob"

    def test_invalid_status_raises(self):
        from src.agents.core.period_close import update_checklist_item
        conn, iid = self._setup()
        with pytest.raises(ValueError, match="Invalid status"):
            update_checklist_item(conn, iid, "invalid_status")


# ---------------------------------------------------------------------------
# Core module: period completeness
# ---------------------------------------------------------------------------

class TestIsPeriodComplete:
    def test_all_open_is_not_complete(self):
        from src.agents.core.period_close import (
            get_or_create_period_checklist,
            is_period_complete,
        )
        conn = _in_memory_db()
        get_or_create_period_checklist(conn, "ACME", "2025-04")
        assert is_period_complete(conn, "ACME", "2025-04") is False

    def test_all_complete_is_complete(self):
        from src.agents.core.period_close import (
            get_or_create_period_checklist,
            is_period_complete,
            update_checklist_item,
        )
        conn = _in_memory_db()
        items = get_or_create_period_checklist(conn, "ACME", "2025-04")
        for item in items:
            update_checklist_item(conn, item["id"], "complete", completed_by="mgr")
        assert is_period_complete(conn, "ACME", "2025-04") is True

    def test_mix_complete_waived_is_complete(self):
        from src.agents.core.period_close import (
            get_or_create_period_checklist,
            is_period_complete,
            update_checklist_item,
        )
        conn = _in_memory_db()
        items = get_or_create_period_checklist(conn, "ACME", "2025-05")
        for i, item in enumerate(items):
            status = "complete" if i % 2 == 0 else "waived"
            update_checklist_item(conn, item["id"], status, completed_by="mgr")
        assert is_period_complete(conn, "ACME", "2025-05") is True

    def test_one_open_item_is_not_complete(self):
        from src.agents.core.period_close import (
            get_or_create_period_checklist,
            is_period_complete,
            update_checklist_item,
        )
        conn = _in_memory_db()
        items = get_or_create_period_checklist(conn, "ACME", "2025-06")
        for item in items[:-1]:
            update_checklist_item(conn, item["id"], "complete", completed_by="mgr")
        # last item remains open
        assert is_period_complete(conn, "ACME", "2025-06") is False

    def test_no_items_is_not_complete(self):
        from src.agents.core.period_close import (
            ensure_period_close_tables,
            is_period_complete,
        )
        conn = _in_memory_db()
        ensure_period_close_tables(conn)
        # No items created — should return False
        assert is_period_complete(conn, "NOBODY", "2025-01") is False


# ---------------------------------------------------------------------------
# Core module: period locking
# ---------------------------------------------------------------------------

class TestPeriodLocking:
    def _setup_complete_period(self) -> tuple[sqlite3.Connection, str, str]:
        from src.agents.core.period_close import (
            get_or_create_period_checklist,
            update_checklist_item,
        )
        conn = _in_memory_db()
        items = get_or_create_period_checklist(conn, "ACME", "2025-07")
        for item in items:
            update_checklist_item(conn, item["id"], "complete", completed_by="mgr")
        return conn, "ACME", "2025-07"

    def test_not_locked_initially(self):
        from src.agents.core.period_close import (
            ensure_period_close_tables,
            is_period_locked,
        )
        conn = _in_memory_db()
        ensure_period_close_tables(conn)
        assert is_period_locked(conn, "ACME", "2025-07") is False

    def test_lock_and_check(self):
        from src.agents.core.period_close import is_period_locked, lock_period
        conn, cc, per = self._setup_complete_period()
        assert is_period_locked(conn, cc, per) is False
        lock_period(conn, cc, per, "alice")
        assert is_period_locked(conn, cc, per) is True

    def test_lock_info_returns_dict(self):
        from src.agents.core.period_close import get_lock_info, lock_period
        conn, cc, per = self._setup_complete_period()
        lock_period(conn, cc, per, "alice")
        info = get_lock_info(conn, cc, per)
        assert info is not None
        assert info["locked_by"] == "alice"
        assert info["locked_at"] is not None
        assert info["client_code"] == cc
        assert info["period"] == per

    def test_get_lock_info_returns_none_when_unlocked(self):
        from src.agents.core.period_close import ensure_period_close_tables, get_lock_info
        conn = _in_memory_db()
        ensure_period_close_tables(conn)
        assert get_lock_info(conn, "ACME", "2025-07") is None

    def test_idempotent_lock(self):
        from src.agents.core.period_close import is_period_locked, lock_period
        conn, cc, per = self._setup_complete_period()
        lock_period(conn, cc, per, "alice")
        lock_period(conn, cc, per, "bob")  # replace — must not raise
        assert is_period_locked(conn, cc, per) is True
        info = __import__("src.agents.core.period_close", fromlist=["get_lock_info"]).get_lock_info(conn, cc, per)
        assert info["locked_by"] == "bob"


# ---------------------------------------------------------------------------
# Core module: document period helper
# ---------------------------------------------------------------------------

class TestGetDocumentPeriod:
    def test_iso_date(self):
        from src.agents.core.period_close import get_document_period
        assert get_document_period("2025-03-15") == "2025-03"

    def test_yyyy_mm_only(self):
        from src.agents.core.period_close import get_document_period
        assert get_document_period("2025-03") == "2025-03"

    def test_none_returns_empty(self):
        from src.agents.core.period_close import get_document_period
        assert get_document_period(None) == ""

    def test_empty_string_returns_empty(self):
        from src.agents.core.period_close import get_document_period
        assert get_document_period("") == ""

    def test_short_string_returns_empty(self):
        from src.agents.core.period_close import get_document_period
        assert get_document_period("20250") == ""

    def test_wrong_separator_returns_empty(self):
        from src.agents.core.period_close import get_document_period
        # No dash in position 4
        assert get_document_period("20250315") == ""


# ---------------------------------------------------------------------------
# Core module: PDF generation
# ---------------------------------------------------------------------------

class TestGeneratePeriodClosePdf:
    def _complete_period(self) -> tuple[sqlite3.Connection, str, str]:
        from src.agents.core.period_close import (
            get_or_create_period_checklist,
            lock_period,
            update_checklist_item,
        )
        conn = _in_memory_db()
        items = get_or_create_period_checklist(conn, "ACME", "2025-08")
        for item in items:
            update_checklist_item(conn, item["id"], "complete", completed_by="mgr",
                                  notes="OK", responsible_user="alice")
        lock_period(conn, "ACME", "2025-08", "alice")
        return conn, "ACME", "2025-08"

    def test_returns_bytes(self):
        from src.agents.core.period_close import generate_period_close_pdf
        conn, cc, per = self._complete_period()
        result = generate_period_close_pdf(conn, cc, per, lang="en")
        assert isinstance(result, bytes)
        assert len(result) > 100

    def test_starts_with_pdf_header(self):
        from src.agents.core.period_close import generate_period_close_pdf
        conn, cc, per = self._complete_period()
        result = generate_period_close_pdf(conn, cc, per, lang="fr")
        assert result.startswith(b"%PDF")

    def test_works_with_both_langs(self):
        from src.agents.core.period_close import generate_period_close_pdf
        conn, cc, per = self._complete_period()
        pdf_en = generate_period_close_pdf(conn, cc, per, lang="en")
        pdf_fr = generate_period_close_pdf(conn, cc, per, lang="fr")
        assert len(pdf_en) > 100
        assert len(pdf_fr) > 100


# ---------------------------------------------------------------------------
# Migration script
# ---------------------------------------------------------------------------

class TestMigrateDbScript:
    def test_declares_period_close_table(self):
        src = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "period_close" in src

    def test_declares_period_close_locks_table(self):
        src = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "period_close_locks" in src


# ---------------------------------------------------------------------------
# review_dashboard.py integration
# ---------------------------------------------------------------------------

class TestReviewDashboardPeriodClose:
    def test_render_period_close_exported(self):
        from scripts.review_dashboard import render_period_close
        assert callable(render_period_close)

    def test_render_period_close_accepts_lang(self):
        from scripts.review_dashboard import render_period_close
        sig = inspect.signature(render_period_close)
        assert "lang" in sig.parameters

    def test_period_close_route_in_do_get(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert '"/period_close"' in src or "path == \"/period_close\"" in src

    def test_period_close_pdf_route_in_do_get(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "/period_close/pdf" in src

    def test_period_close_check_item_post_route(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "/period_close/check_item" in src

    def test_period_close_lock_post_route(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "/period_close/lock" in src

    def test_period_close_nav_link_in_render_home(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "period_close_btn" in src

    def test_period_lock_guard_imported_and_used(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "_check_period_not_locked_for_doc" in src

    def test_manager_owner_role_check_in_period_close_route(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        # The route should check for manager/owner
        assert 'not in ("manager", "owner")' in src


# ---------------------------------------------------------------------------
# i18n keys for period close
# ---------------------------------------------------------------------------

class TestPeriodCloseI18nKeys:
    REQUIRED_KEYS = [
        "pc_title", "pc_nav_link", "pc_period", "pc_btn_load",
        "pc_btn_complete", "pc_btn_waive", "pc_btn_reopen",
        "pc_btn_close_period", "pc_btn_download_pdf",
        "pc_close_period_confirm", "pc_period_locked",
        "pc_locked_by", "pc_locked_at", "pc_completed_by", "pc_completed_at",
        "pc_responsible", "pc_notes_ph", "pc_all_complete_msg",
        "pc_items_remaining", "pc_col_item", "pc_col_status",
        "pc_status_open", "pc_status_complete", "pc_status_waived",
        "pc_item_all_docs_reviewed", "pc_item_no_unresolved_holds",
        "pc_item_gst_qst_reconciled", "pc_item_itc_itr_verified",
        "pc_item_je_posted_qbo", "pc_item_trial_balance_reviewed",
        "pc_item_manager_signoff",
        "flash_pc_item_updated", "flash_pc_period_locked",
        "err_pc_period_locked", "err_pc_items_open",
        "err_pc_client_period_required",
    ]

    def _load_json(self, lang: str) -> dict:
        path = ROOT / "src" / "i18n" / f"{lang}.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def test_en_has_all_period_close_keys(self):
        data = self._load_json("en")
        for key in self.REQUIRED_KEYS:
            assert key in data, f"en.json missing period-close key: {key!r}"

    def test_fr_has_all_period_close_keys(self):
        data = self._load_json("fr")
        for key in self.REQUIRED_KEYS:
            assert key in data, f"fr.json missing period-close key: {key!r}"

    def test_checklist_item_keys_translate_correctly(self):
        from src.i18n import reload_cache, t
        reload_cache()
        from src.agents.core.period_close import DEFAULT_CHECKLIST_KEYS
        for key in DEFAULT_CHECKLIST_KEYS:
            en_val = t(key, "en")
            fr_val = t(key, "fr")
            # Should return real translations, not the key itself
            assert en_val != key, f"en.json has no translation for {key!r}"
            assert fr_val != key, f"fr.json has no translation for {key!r}"

    def test_status_keys_translate_correctly(self):
        from src.i18n import reload_cache, t
        reload_cache()
        for status in ("open", "complete", "waived"):
            key = f"pc_status_{status}"
            assert t(key, "en") != key
            assert t(key, "fr") != key
