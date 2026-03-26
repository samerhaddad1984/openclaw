"""
tests/test_client_comms.py — pytest tests for the client communications module.

Covers:
  - ensure_comms_table creates the table and indexes
  - save_draft inserts an unsent record and returns a comm_id
  - update_draft modifies an unsent draft
  - get_document_comms / get_client_comms / get_all_comms queries
  - get_unread_count counts messages where read_at IS NULL
  - mark_read / mark_all_read set read_at
  - draft_message returns a non-empty string (AI mocked to avoid real HTTP)
  - send_comm calls SMTP (mocked) and marks sent_at/read_at
  - send_comm raises when SMTP is not configured
  - send_comm raises when message already sent
  - migrate_db.py declares client_communications
  - review_dashboard.py exposes routes and render function
  - i18n keys present for all communications strings
"""
from __future__ import annotations

import inspect
import json
import sqlite3
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _in_memory_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _seeded_db() -> sqlite3.Connection:
    """In-memory DB with the comms table and a minimal documents table bootstrapped."""
    from src.agents.core.client_comms import ensure_comms_table
    conn = _in_memory_db()
    ensure_comms_table(conn)
    # Minimal documents table so LEFT JOIN queries in get_client_comms / get_all_comms work
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name   TEXT,
            vendor      TEXT,
            amount      REAL
        )
        """
    )
    conn.commit()
    return conn


def _add_draft(conn: sqlite3.Connection, **kwargs) -> str:
    from src.agents.core.client_comms import save_draft
    defaults = dict(
        document_id="doc-001",
        client_code="ACME",
        message="Test message body.",
        sent_by="alice",
    )
    defaults.update(kwargs)
    return save_draft(conn, **defaults)


# ---------------------------------------------------------------------------
# ensure_comms_table
# ---------------------------------------------------------------------------

class TestEnsureCommsTable:
    def test_creates_table(self):
        from src.agents.core.client_comms import ensure_comms_table
        conn = _in_memory_db()
        ensure_comms_table(conn)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        assert "client_communications" in tables

    def test_idempotent(self):
        from src.agents.core.client_comms import ensure_comms_table
        conn = _in_memory_db()
        ensure_comms_table(conn)
        ensure_comms_table(conn)  # must not raise

    def test_expected_columns(self):
        from src.agents.core.client_comms import ensure_comms_table
        conn = _in_memory_db()
        ensure_comms_table(conn)
        cols = {r["name"] for r in conn.execute(
            "PRAGMA table_info(client_communications)"
        )}
        for col in ("comm_id", "document_id", "client_code", "direction",
                    "message", "sent_at", "sent_by", "read_at"):
            assert col in cols, f"Missing column: {col}"

    def test_creates_document_index(self):
        from src.agents.core.client_comms import ensure_comms_table
        conn = _in_memory_db()
        ensure_comms_table(conn)
        indexes = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )}
        assert "idx_comms_document" in indexes

    def test_creates_client_index(self):
        from src.agents.core.client_comms import ensure_comms_table
        conn = _in_memory_db()
        ensure_comms_table(conn)
        indexes = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )}
        assert "idx_comms_client" in indexes


# ---------------------------------------------------------------------------
# save_draft
# ---------------------------------------------------------------------------

class TestSaveDraft:
    def test_returns_comm_id_string(self):
        conn = _seeded_db()
        comm_id = _add_draft(conn)
        assert isinstance(comm_id, str) and len(comm_id) > 0

    def test_record_is_inserted(self):
        conn = _seeded_db()
        comm_id = _add_draft(conn, message="Hello client.")
        row = conn.execute(
            "SELECT * FROM client_communications WHERE comm_id=?", (comm_id,)
        ).fetchone()
        assert row is not None
        assert row["message"] == "Hello client."

    def test_sent_at_is_null(self):
        conn = _seeded_db()
        comm_id = _add_draft(conn)
        row = conn.execute(
            "SELECT sent_at FROM client_communications WHERE comm_id=?", (comm_id,)
        ).fetchone()
        assert row["sent_at"] is None

    def test_read_at_is_null(self):
        conn = _seeded_db()
        comm_id = _add_draft(conn)
        row = conn.execute(
            "SELECT read_at FROM client_communications WHERE comm_id=?", (comm_id,)
        ).fetchone()
        assert row["read_at"] is None

    def test_direction_defaults_to_outbound(self):
        conn = _seeded_db()
        comm_id = _add_draft(conn)
        row = conn.execute(
            "SELECT direction FROM client_communications WHERE comm_id=?", (comm_id,)
        ).fetchone()
        assert row["direction"] == "outbound"

    def test_custom_direction(self):
        conn = _seeded_db()
        comm_id = _add_draft(conn, direction="inbound")
        row = conn.execute(
            "SELECT direction FROM client_communications WHERE comm_id=?", (comm_id,)
        ).fetchone()
        assert row["direction"] == "inbound"

    def test_multiple_drafts_have_unique_ids(self):
        conn = _seeded_db()
        ids = [_add_draft(conn) for _ in range(5)]
        assert len(set(ids)) == 5


# ---------------------------------------------------------------------------
# update_draft
# ---------------------------------------------------------------------------

class TestUpdateDraft:
    def test_updates_message(self):
        from src.agents.core.client_comms import update_draft
        conn = _seeded_db()
        comm_id = _add_draft(conn, message="Original message.")
        update_draft(conn, comm_id, "Revised message.")
        row = conn.execute(
            "SELECT message FROM client_communications WHERE comm_id=?", (comm_id,)
        ).fetchone()
        assert row["message"] == "Revised message."

    def test_no_op_on_nonexistent_id(self):
        from src.agents.core.client_comms import update_draft
        conn = _seeded_db()
        update_draft(conn, "nonexistent-id", "Should not crash.")  # must not raise

    def test_no_op_after_sent(self):
        from src.agents.core.client_comms import update_draft
        conn = _seeded_db()
        comm_id = _add_draft(conn, message="Original.")
        # Mark as sent manually
        conn.execute(
            "UPDATE client_communications SET sent_at='2026-01-01T00:00:00+00:00' WHERE comm_id=?",
            (comm_id,),
        )
        conn.commit()
        update_draft(conn, comm_id, "Attempted update after send.")
        row = conn.execute(
            "SELECT message FROM client_communications WHERE comm_id=?", (comm_id,)
        ).fetchone()
        assert row["message"] == "Original."  # unchanged


# ---------------------------------------------------------------------------
# get_document_comms
# ---------------------------------------------------------------------------

class TestGetDocumentComms:
    def test_returns_empty_for_unknown_doc(self):
        from src.agents.core.client_comms import get_document_comms
        conn = _seeded_db()
        assert get_document_comms(conn, "no-such-doc") == []

    def test_returns_all_for_document(self):
        from src.agents.core.client_comms import get_document_comms
        conn = _seeded_db()
        _add_draft(conn, document_id="doc-A")
        _add_draft(conn, document_id="doc-A")
        _add_draft(conn, document_id="doc-B")
        result = get_document_comms(conn, "doc-A")
        assert len(result) == 2

    def test_excludes_other_documents(self):
        from src.agents.core.client_comms import get_document_comms
        conn = _seeded_db()
        _add_draft(conn, document_id="doc-X")
        result = get_document_comms(conn, "doc-Y")
        assert result == []

    def test_returns_list_of_dicts(self):
        from src.agents.core.client_comms import get_document_comms
        conn = _seeded_db()
        _add_draft(conn, document_id="doc-Z")
        result = get_document_comms(conn, "doc-Z")
        assert isinstance(result, list)
        assert isinstance(result[0], dict)


# ---------------------------------------------------------------------------
# get_client_comms
# ---------------------------------------------------------------------------

class TestGetClientComms:
    def test_returns_empty_for_unknown_client(self):
        from src.agents.core.client_comms import get_client_comms
        conn = _seeded_db()
        assert get_client_comms(conn, "NOBODY") == []

    def test_returns_all_for_client(self):
        from src.agents.core.client_comms import get_client_comms
        conn = _seeded_db()
        _add_draft(conn, client_code="ACME")
        _add_draft(conn, client_code="ACME")
        _add_draft(conn, client_code="BETA")
        result = get_client_comms(conn, "ACME")
        assert len(result) == 2

    def test_excludes_other_clients(self):
        from src.agents.core.client_comms import get_client_comms
        conn = _seeded_db()
        _add_draft(conn, client_code="ACME")
        result = get_client_comms(conn, "BETA")
        assert result == []


# ---------------------------------------------------------------------------
# get_all_comms
# ---------------------------------------------------------------------------

class TestGetAllComms:
    def test_returns_empty_when_no_records(self):
        from src.agents.core.client_comms import get_all_comms
        conn = _seeded_db()
        assert get_all_comms(conn) == []

    def test_returns_all_records(self):
        from src.agents.core.client_comms import get_all_comms
        conn = _seeded_db()
        _add_draft(conn, client_code="ACME")
        _add_draft(conn, client_code="BETA")
        result = get_all_comms(conn)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# get_unread_count
# ---------------------------------------------------------------------------

class TestGetUnreadCount:
    def test_zero_when_empty(self):
        from src.agents.core.client_comms import get_unread_count
        conn = _seeded_db()
        assert get_unread_count(conn) == 0

    def test_counts_null_read_at(self):
        from src.agents.core.client_comms import get_unread_count
        conn = _seeded_db()
        _add_draft(conn)
        _add_draft(conn)
        assert get_unread_count(conn) == 2

    def test_excludes_read_messages(self):
        from src.agents.core.client_comms import get_unread_count
        conn = _seeded_db()
        cid = _add_draft(conn)
        conn.execute(
            "UPDATE client_communications SET read_at='2026-01-01T00:00:00+00:00' WHERE comm_id=?",
            (cid,),
        )
        conn.commit()
        _add_draft(conn)  # one unread
        assert get_unread_count(conn) == 1


# ---------------------------------------------------------------------------
# mark_read
# ---------------------------------------------------------------------------

class TestMarkRead:
    def test_sets_read_at(self):
        from src.agents.core.client_comms import mark_read
        conn = _seeded_db()
        cid = _add_draft(conn)
        mark_read(conn, cid)
        row = conn.execute(
            "SELECT read_at FROM client_communications WHERE comm_id=?", (cid,)
        ).fetchone()
        assert row["read_at"] is not None

    def test_idempotent(self):
        from src.agents.core.client_comms import mark_read
        conn = _seeded_db()
        cid = _add_draft(conn)
        mark_read(conn, cid)
        first_ts = conn.execute(
            "SELECT read_at FROM client_communications WHERE comm_id=?", (cid,)
        ).fetchone()["read_at"]
        mark_read(conn, cid)
        second_ts = conn.execute(
            "SELECT read_at FROM client_communications WHERE comm_id=?", (cid,)
        ).fetchone()["read_at"]
        assert first_ts == second_ts  # timestamp not overwritten

    def test_no_op_on_nonexistent_id(self):
        from src.agents.core.client_comms import mark_read
        conn = _seeded_db()
        mark_read(conn, "no-such-id")  # must not raise


# ---------------------------------------------------------------------------
# mark_all_read
# ---------------------------------------------------------------------------

class TestMarkAllRead:
    def test_marks_all_messages(self):
        from src.agents.core.client_comms import get_unread_count, mark_all_read
        conn = _seeded_db()
        _add_draft(conn)
        _add_draft(conn)
        mark_all_read(conn)
        assert get_unread_count(conn) == 0

    def test_no_op_when_all_already_read(self):
        from src.agents.core.client_comms import mark_all_read
        conn = _seeded_db()
        mark_all_read(conn)  # must not raise


# ---------------------------------------------------------------------------
# draft_message
# ---------------------------------------------------------------------------

class TestDraftMessage:
    def test_returns_string_on_ai_success(self):
        from src.agents.core.client_comms import draft_message

        with patch("src.agents.core.ai_router.call") as mock_call:
            mock_call.return_value = {
                "provider": "routine",
                "result": "Bonjour, veuillez confirmer la dépense.",
                "latency_ms": 200,
                "fallback_used": False,
                "error": None,
            }
            msg = draft_message(
                document_id="doc-001",
                vendor="Staples",
                amount="45.00",
                client_code="ACME",
                lang="fr",
            )
        assert isinstance(msg, str)
        assert len(msg) > 0

    def test_returns_fallback_fr_on_ai_error(self):
        from src.agents.core.client_comms import draft_message

        with patch("src.agents.core.ai_router.call") as mock_call:
            mock_call.return_value = {
                "provider": "routine",
                "result": None,
                "latency_ms": 0,
                "fallback_used": False,
                "error": "provider_not_configured",
            }
            msg = draft_message(
                document_id="doc-002",
                vendor="Bell Canada",
                amount="120.00",
                client_code="BETA",
                lang="fr",
            )
        assert "Bell Canada" in msg
        assert len(msg) > 10

    def test_returns_fallback_en_on_ai_error(self):
        from src.agents.core.client_comms import draft_message

        with patch("src.agents.core.ai_router.call") as mock_call:
            mock_call.return_value = {
                "provider": "routine",
                "result": None,
                "latency_ms": 0,
                "fallback_used": False,
                "error": "provider_not_configured",
            }
            msg = draft_message(
                document_id="doc-003",
                vendor="Rogers",
                amount=None,
                client_code="ACME",
                lang="en",
            )
        assert "Rogers" in msg
        assert len(msg) > 10

    def test_uses_draft_client_message_task_type(self):
        from src.agents.core.client_comms import draft_message

        with patch("src.agents.core.ai_router.call") as mock_call:
            mock_call.return_value = {
                "provider": "routine",
                "result": "Test message.",
                "latency_ms": 100,
                "fallback_used": False,
                "error": None,
            }
            draft_message(
                document_id="doc-004",
                vendor="Vendor X",
                amount="50.00",
                client_code="ACME",
                lang="fr",
            )
        call_args = mock_call.call_args
        assert call_args[0][0] == "draft_client_message"

    def test_strips_whitespace_from_result(self):
        from src.agents.core.client_comms import draft_message

        with patch("src.agents.core.ai_router.call") as mock_call:
            mock_call.return_value = {
                "provider": "routine",
                "result": "  Bonjour.  \n\n",
                "latency_ms": 50,
                "fallback_used": False,
                "error": None,
            }
            msg = draft_message(
                document_id="doc-005",
                vendor="X",
                amount=None,
                client_code="ACME",
                lang="fr",
            )
        assert msg == "Bonjour."


# ---------------------------------------------------------------------------
# send_comm
# ---------------------------------------------------------------------------

class TestSendComm:
    def _make_smtp_config(self) -> dict:
        return {
            "smtp_host": "smtp.example.com",
            "smtp_port": 587,
            "smtp_user": "user@example.com",
            "smtp_password": "secret",
            "from_address": "ledgerlink@example.com",
            "from_name": "LedgerLink AI",
        }

    def test_marks_sent_at_after_send(self):
        from src.agents.core.client_comms import send_comm

        conn = _seeded_db()
        cid = _add_draft(conn, message="Hello client.")

        with patch("src.agents.core.client_comms._load_smtp_config",
                   return_value=self._make_smtp_config()), \
             patch("smtplib.SMTP") as mock_smtp:
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__ = MagicMock(return_value=mock_server)
            mock_smtp.return_value.__exit__ = MagicMock(return_value=False)
            send_comm(conn, comm_id=cid, to_email="client@example.com")

        row = conn.execute(
            "SELECT sent_at, read_at FROM client_communications WHERE comm_id=?", (cid,)
        ).fetchone()
        assert row["sent_at"] is not None
        assert row["read_at"] is not None

    def test_raises_when_already_sent(self):
        from src.agents.core.client_comms import send_comm

        conn = _seeded_db()
        cid = _add_draft(conn)
        conn.execute(
            "UPDATE client_communications SET sent_at='2026-01-01T00:00:00+00:00' WHERE comm_id=?",
            (cid,),
        )
        conn.commit()

        with pytest.raises(ValueError, match="already been sent"):
            send_comm(conn, comm_id=cid, to_email="x@example.com")

    def test_raises_when_not_found(self):
        from src.agents.core.client_comms import send_comm

        conn = _seeded_db()
        with pytest.raises(ValueError, match="not found"):
            send_comm(conn, comm_id="nonexistent", to_email="x@example.com")

    def test_raises_when_smtp_not_configured(self):
        from src.agents.core.client_comms import send_comm

        conn = _seeded_db()
        cid = _add_draft(conn)

        with patch("src.agents.core.client_comms._load_smtp_config", return_value={}):
            with pytest.raises(RuntimeError, match="SMTP is not configured"):
                send_comm(conn, comm_id=cid, to_email="client@example.com")

    def test_raises_on_smtp_failure(self):
        from src.agents.core.client_comms import send_comm

        conn = _seeded_db()
        cid = _add_draft(conn)

        with patch("src.agents.core.client_comms._load_smtp_config",
                   return_value=self._make_smtp_config()), \
             patch("smtplib.SMTP", side_effect=ConnectionRefusedError("refused")):
            with pytest.raises(RuntimeError, match="SMTP send failed"):
                send_comm(conn, comm_id=cid, to_email="client@example.com")

    def test_sent_at_not_set_on_failure(self):
        from src.agents.core.client_comms import send_comm

        conn = _seeded_db()
        cid = _add_draft(conn)

        with patch("src.agents.core.client_comms._load_smtp_config",
                   return_value=self._make_smtp_config()), \
             patch("smtplib.SMTP", side_effect=ConnectionRefusedError("refused")):
            with pytest.raises(RuntimeError):
                send_comm(conn, comm_id=cid, to_email="client@example.com")

        row = conn.execute(
            "SELECT sent_at FROM client_communications WHERE comm_id=?", (cid,)
        ).fetchone()
        assert row["sent_at"] is None  # must not be marked sent on failure


# ---------------------------------------------------------------------------
# migrate_db.py
# ---------------------------------------------------------------------------

class TestMigrateDbScript:
    def test_declares_client_communications_table(self):
        src = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "client_communications" in src

    def test_declares_expected_columns(self):
        src = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        for col in ("comm_id", "document_id", "client_code", "direction",
                    "message", "sent_at", "sent_by", "read_at"):
            assert col in src, f"migrate_db.py missing column declaration: {col!r}"


# ---------------------------------------------------------------------------
# review_dashboard.py integration
# ---------------------------------------------------------------------------

class TestReviewDashboardIntegration:
    def test_render_doc_communications_exported(self):
        from scripts.review_dashboard import render_doc_communications
        assert callable(render_doc_communications)

    def test_render_communications_exported(self):
        from scripts.review_dashboard import render_communications
        assert callable(render_communications)

    def test_render_communications_accepts_lang(self):
        from scripts.review_dashboard import render_communications
        sig = inspect.signature(render_communications)
        assert "lang" in sig.parameters

    def test_communications_get_route_in_do_get(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert '"/communications"' in src

    def test_communications_draft_post_route(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert '"/communications/draft"' in src

    def test_communications_send_post_route(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert '"/communications/send"' in src

    def test_communications_nav_btn_in_render_home(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "comms_btn" in src

    def test_render_doc_communications_called_in_render_document(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "render_doc_communications" in src

    def test_manager_owner_role_check_in_communications_route(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        # The /communications GET route must check for manager/owner
        assert "manager" in src and "owner" in src

    def test_client_comms_imported_in_dashboard(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "_client_comms" in src

    def test_unread_badge_in_page_layout(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "badge-unread" in src

    def test_never_auto_send_requires_send_route(self):
        """Drafts are only sent via explicit POST /communications/send."""
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        # draft route must NOT call send_comm
        draft_section_start = src.find('"/communications/draft"')
        send_section_start = src.find('"/communications/send"')
        assert draft_section_start != -1 and send_section_start != -1
        draft_block = src[draft_section_start:send_section_start]
        assert "send_comm" not in draft_block


# ---------------------------------------------------------------------------
# i18n keys
# ---------------------------------------------------------------------------

class TestCommsI18nKeys:
    REQUIRED_KEYS = [
        "comm_nav_link", "comm_title", "comm_draft_btn", "comm_send_btn",
        "comm_draft_heading", "comm_message_label", "comm_to_email_label",
        "comm_to_email_ph", "comm_subject_label", "comm_subject_ph",
        "comm_edit_hint", "comm_no_comms", "comm_col_date", "comm_col_direction",
        "comm_col_message", "comm_col_by", "comm_col_document",
        "comm_direction_outbound", "comm_direction_inbound", "comm_draft_unsent",
        "comm_filter_client_ph", "comm_unread_badge",
        "flash_comm_draft_created", "flash_comm_sent",
        "err_comm_message_required", "err_comm_smtp_not_configured",
        "err_comm_not_found", "err_comm_already_sent",
    ]

    def _load_json(self, lang: str) -> dict:
        path = ROOT / "src" / "i18n" / f"{lang}.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def test_en_has_all_comm_keys(self):
        data = self._load_json("en")
        for key in self.REQUIRED_KEYS:
            assert key in data, f"en.json missing communications key: {key!r}"

    def test_fr_has_all_comm_keys(self):
        data = self._load_json("fr")
        for key in self.REQUIRED_KEYS:
            assert key in data, f"fr.json missing communications key: {key!r}"

    def test_direction_keys_translate_correctly(self):
        from src.i18n import reload_cache, t
        reload_cache()
        for direction in ("outbound", "inbound"):
            key = f"comm_direction_{direction}"
            assert t(key, "en") != key, f"en.json has no translation for {key!r}"
            assert t(key, "fr") != key, f"fr.json has no translation for {key!r}"

    def test_flash_keys_are_non_empty(self):
        from src.i18n import reload_cache, t
        reload_cache()
        for key in ("flash_comm_draft_created", "flash_comm_sent"):
            assert t(key, "en") != key
            assert t(key, "fr") != key
