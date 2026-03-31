"""
tests/test_openclaw_bridge.py
==============================
Pytest tests for the OpenClaw bridge:

  * src/integrations/openclaw_bridge.py  — unit tests for the bridge logic
  * POST /ingest/openclaw               — integration test via HTTP handler

All tests use an in-memory (or temp-file) SQLite database; no real OCR
calls are made (process_file is monkey-patched).
"""
from __future__ import annotations

import base64
import io
import json
import sqlite3
import sys
import tempfile
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock
from urllib.request import urlopen, Request

import pytest

# ---------------------------------------------------------------------------
# Make sure the project root is on sys.path
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.integrations.openclaw_bridge import (
    get_bridge_stats,
    get_client_by_sender_id,
    handle_openclaw_ingest,
    log_messaging_event,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    """Create a minimal in-memory SQLite database with required tables."""
    db = tmp_path / "test_otocpa.db"
    with sqlite3.connect(str(db)) as conn:
        conn.executescript("""
            CREATE TABLE dashboard_users (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                username        TEXT NOT NULL UNIQUE,
                password_hash   TEXT NOT NULL DEFAULT '',
                role            TEXT NOT NULL DEFAULT 'employee',
                display_name    TEXT,
                active          INTEGER NOT NULL DEFAULT 1,
                language        TEXT DEFAULT 'fr',
                client_code     TEXT,
                whatsapp_number TEXT,
                telegram_id     TEXT,
                must_reset_password INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT DEFAULT ''
            );

            CREATE TABLE documents (
                document_id    TEXT PRIMARY KEY,
                file_name      TEXT,
                file_path      TEXT,
                client_code    TEXT,
                vendor         TEXT,
                doc_type       TEXT,
                amount         REAL,
                document_date  TEXT,
                review_status  TEXT,
                confidence     REAL,
                raw_result     TEXT,
                created_at     TEXT,
                updated_at     TEXT,
                submitted_by   TEXT,
                client_note    TEXT,
                currency       TEXT,
                subtotal       REAL,
                tax_total      REAL,
                extraction_method TEXT,
                ingest_source  TEXT,
                raw_ocr_text   TEXT,
                hallucination_suspected INTEGER DEFAULT 0
            );

            CREATE TABLE messaging_log (
                log_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                client_code  TEXT,
                platform     TEXT NOT NULL DEFAULT '',
                direction    TEXT NOT NULL DEFAULT 'inbound',
                message_type TEXT NOT NULL DEFAULT 'media',
                document_id  TEXT,
                sent_at      TEXT NOT NULL DEFAULT '',
                status       TEXT NOT NULL DEFAULT 'delivered'
            );
        """)
        # Seed one WhatsApp user and one Telegram user
        conn.execute(
            "INSERT INTO dashboard_users "
            "(username, password_hash, role, active, client_code, whatsapp_number) "
            "VALUES ('wa_client', 'x', 'employee', 1, 'ACME', '+15141234567')"
        )
        conn.execute(
            "INSERT INTO dashboard_users "
            "(username, password_hash, role, active, client_code, telegram_id) "
            "VALUES ('tg_client', 'x', 'employee', 1, 'BETA', '987654321')"
        )
        conn.commit()
    return db


@pytest.fixture()
def upload_dir(tmp_path: Path) -> Path:
    d = tmp_path / "uploads"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# Minimal valid JPEG bytes (1×1 pixel)
# ---------------------------------------------------------------------------

_JPEG_BYTES = (
    b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
    b"\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t"
    b"\x08\n\x0c\x14\r\x0c\x0b\x0b\x0c\x19\x12\x13\x0f\x14\x1d\x1a"
    b"\x1f\x1e\x1d\x1a\x1c\x1c $.' \",#\x1c\x1c(7),01444\x1f'9=82<.342\x1eC"
    b"\xff\xc0\x00\x0b\x08\x00\x01\x00\x01\x01\x01\x11\x00"
    b"\xff\xc4\x00\x1f\x00\x00\x01\x05\x01\x01\x01\x01\x01\x01\x00\x00"
    b"\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b"
    b"\xff\xc4\x00\xb5\x10\x00\x02\x01\x03\x03\x02\x04\x03\x05\x05\x04"
    b"\x04\x00\x00\x01}\x01\x02\x03\x00\x04\x11\x05\x12!1A\x06\x13Qa"
    b"\x07\"q\x142\x81\x91\xa1\x08#B\xb1\xc1\x15R\xd1\xf0$3br"
    b"\x82\t\n\x16\x17\x18\x19\x1a%&'()*456789:CDEFGHIJSTUVWXYZ"
    b"cdefghijstuvwxyz\x83\x84\x85\x86\x87\x88\x89\x8a\x92\x93\x94"
    b"\x95\x96\x97\x98\x99\x9a\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa"
    b"\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xc2\xc3\xc4\xc5\xc6\xc7"
    b"\xc8\xc9\xca\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xe1\xe2\xe3"
    b"\xe4\xe5\xe6\xe7\xe8\xe9\xea\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8"
    b"\xf9\xfa\xff\xda\x00\x08\x01\x01\x00\x00?\x00\xfb\xd5P\x00\x00"
    b"\x00\x1f\xff\xd9"
)
_JPEG_B64 = base64.b64encode(_JPEG_BYTES).decode()


# ---------------------------------------------------------------------------
# Helper: fake process_file result
# ---------------------------------------------------------------------------

def _fake_process_file(file_bytes, filename, **kwargs):
    """Return a successful process_file result without touching any real AI."""
    return {
        "ok":           True,
        "document_id":  "doc_test1234",
        "file_name":    filename,
        "file_path":    "/tmp/test",
        "format":       "jpeg",
        "extraction_method": "vision_jpeg",
        "vendor":       "Test Vendor",
        "doc_type":     "receipt",
        "amount":       42.00,
        "document_date": "2026-03-21",
        "confidence":   0.95,
        "review_status": "New",
        "currency":     "CAD",
        "low_confidence_flagged": False,
        "hallucination_suspected": False,
        "error":        None,
    }


# ===========================================================================
# Tests: get_client_by_sender_id
# ===========================================================================

class TestGetClientBySenderId:

    def test_whatsapp_exact_match(self, tmp_db):
        client = get_client_by_sender_id("whatsapp", "+15141234567", db_path=tmp_db)
        assert client is not None
        assert client["client_code"] == "ACME"

    def test_whatsapp_without_prefix(self, tmp_db):
        # "5141234567" should still match "+15141234567"
        client = get_client_by_sender_id("whatsapp", "5141234567", db_path=tmp_db)
        assert client is not None
        assert client["client_code"] == "ACME"

    def test_whatsapp_openclaw_prefix(self, tmp_db):
        # OpenClaw may strip the country code prefix
        client = get_client_by_sender_id("whatsapp", "15141234567", db_path=tmp_db)
        assert client is not None

    def test_whatsapp_unknown_number(self, tmp_db):
        client = get_client_by_sender_id("whatsapp", "+19995550000", db_path=tmp_db)
        assert client is None

    def test_telegram_exact_match(self, tmp_db):
        client = get_client_by_sender_id("telegram", "987654321", db_path=tmp_db)
        assert client is not None
        assert client["client_code"] == "BETA"

    def test_telegram_unknown(self, tmp_db):
        client = get_client_by_sender_id("telegram", "111111111", db_path=tmp_db)
        assert client is None

    def test_empty_sender_id(self, tmp_db):
        assert get_client_by_sender_id("whatsapp", "", db_path=tmp_db) is None

    def test_invalid_platform(self, tmp_db):
        # Unsupported platform returns None gracefully
        assert get_client_by_sender_id("signal", "5141234567", db_path=tmp_db) is None


# ===========================================================================
# Tests: log_messaging_event
# ===========================================================================

class TestLogMessagingEvent:

    def test_inserts_row(self, tmp_db):
        log_messaging_event(
            client_code="ACME",
            platform="whatsapp",
            direction="inbound",
            message_type="media",
            document_id="doc_abc",
            status="delivered",
            db_path=tmp_db,
        )
        with sqlite3.connect(str(tmp_db)) as conn:
            row = conn.execute("SELECT * FROM messaging_log").fetchone()
        assert row is not None
        assert row[1] == "ACME"    # client_code
        assert row[2] == "whatsapp"
        assert row[3] == "inbound"

    def test_silent_on_missing_table(self, tmp_path):
        # Should not raise even if the table is absent
        bad_db = tmp_path / "bad.db"
        with sqlite3.connect(str(bad_db)) as conn:
            conn.execute("CREATE TABLE dummy (id INTEGER PRIMARY KEY)")
        log_messaging_event(
            client_code="X", platform="whatsapp", direction="inbound",
            message_type="text", db_path=bad_db,
        )  # no exception expected


# ===========================================================================
# Tests: handle_openclaw_ingest — unit (process_file mocked)
# ===========================================================================

class TestHandleOpenclawIngest:

    def test_unsupported_platform(self, tmp_db, upload_dir):
        result = handle_openclaw_ingest(
            {"platform": "signal", "sender_id": "123"},
            db_path=tmp_db, upload_dir=upload_dir,
        )
        assert result["ok"] is False
        assert result["status"] == "error"
        assert "unsupported_platform" in result["error"]

    def test_unknown_sender_whatsapp(self, tmp_db, upload_dir):
        result = handle_openclaw_ingest(
            {"platform": "whatsapp", "sender_id": "+10000000000",
             "file_bytes": _JPEG_B64, "media_type": "image/jpeg"},
            db_path=tmp_db, upload_dir=upload_dir,
        )
        assert result["ok"] is False
        assert result["status"] == "unknown_sender"
        # Event should be logged
        with sqlite3.connect(str(tmp_db)) as conn:
            row = conn.execute("SELECT status FROM messaging_log").fetchone()
        assert row[0] == "unknown_sender"

    def test_unknown_sender_telegram(self, tmp_db, upload_dir):
        result = handle_openclaw_ingest(
            {"platform": "telegram", "sender_id": "000000000",
             "file_bytes": _JPEG_B64},
            db_path=tmp_db, upload_dir=upload_dir,
        )
        assert result["status"] == "unknown_sender"

    def test_text_only_no_file(self, tmp_db, upload_dir):
        result = handle_openclaw_ingest(
            {"platform": "whatsapp", "sender_id": "+15141234567",
             "client_message": "Hello!"},
            db_path=tmp_db, upload_dir=upload_dir,
        )
        assert result["ok"] is True
        assert result["status"] == "no_file"
        assert result["document_id"] is None

    def test_bad_base64(self, tmp_db, upload_dir):
        result = handle_openclaw_ingest(
            {"platform": "whatsapp", "sender_id": "+15141234567",
             "file_bytes": "!!!not-valid-b64!!!",
             "media_type": "image/jpeg"},
            db_path=tmp_db, upload_dir=upload_dir,
        )
        assert result["ok"] is False
        assert result["status"] == "error"
        assert "base64_decode_failed" in result["error"]

    @mock.patch("src.engines.ocr_engine.process_file", side_effect=_fake_process_file)
    def test_successful_whatsapp_ingest(self, mock_pf, tmp_db, upload_dir):
        result = handle_openclaw_ingest(
            {
                "platform":       "whatsapp",
                "sender_id":      "+15141234567",
                "media_type":     "image/jpeg",
                "client_message": "My receipt",
                "file_bytes":     _JPEG_B64,
            },
            db_path=tmp_db, upload_dir=upload_dir,
        )
        assert result["ok"] is True
        assert result["status"] == "processed"
        assert result["document_id"] == "doc_test1234"
        mock_pf.assert_called_once()
        call_kwargs = mock_pf.call_args
        assert call_kwargs.kwargs.get("ingest_source") == "whatsapp"
        assert call_kwargs.kwargs.get("client_code") == "ACME"

        # Log row written
        with sqlite3.connect(str(tmp_db)) as conn:
            row = conn.execute(
                "SELECT status, document_id FROM messaging_log"
                " WHERE direction='inbound'"
            ).fetchone()
        assert row[0] == "delivered"
        assert row[1] == "doc_test1234"

    @mock.patch("src.engines.ocr_engine.process_file", side_effect=_fake_process_file)
    def test_successful_telegram_ingest(self, mock_pf, tmp_db, upload_dir):
        result = handle_openclaw_ingest(
            {
                "platform":   "telegram",
                "sender_id":  "987654321",
                "media_type": "application/pdf",
                "file_bytes": _JPEG_B64,  # content doesn't matter; process_file is mocked
            },
            db_path=tmp_db, upload_dir=upload_dir,
        )
        assert result["ok"] is True
        call_kwargs = mock_pf.call_args
        assert call_kwargs.kwargs.get("ingest_source") == "telegram"
        assert call_kwargs.kwargs.get("client_code") == "BETA"

    @mock.patch(
        "src.engines.ocr_engine.process_file",
        side_effect=RuntimeError("vision_provider_not_configured"),
    )
    def test_process_file_exception(self, mock_pf, tmp_db, upload_dir):
        result = handle_openclaw_ingest(
            {"platform": "whatsapp", "sender_id": "+15141234567",
             "file_bytes": _JPEG_B64, "media_type": "image/jpeg"},
            db_path=tmp_db, upload_dir=upload_dir,
        )
        assert result["ok"] is False
        assert result["status"] == "error"
        assert "vision_provider_not_configured" in result["error"]

        # Failed log row
        with sqlite3.connect(str(tmp_db)) as conn:
            row = conn.execute(
                "SELECT status FROM messaging_log WHERE direction='inbound'"
            ).fetchone()
        assert row[0] == "failed"

    def test_missing_platform_field(self, tmp_db, upload_dir):
        result = handle_openclaw_ingest(
            {"sender_id": "+15141234567", "file_bytes": _JPEG_B64},
            db_path=tmp_db, upload_dir=upload_dir,
        )
        assert result["ok"] is False

    def test_empty_payload(self, tmp_db, upload_dir):
        result = handle_openclaw_ingest({}, db_path=tmp_db, upload_dir=upload_dir)
        assert result["ok"] is False


# ===========================================================================
# Tests: get_bridge_stats
# ===========================================================================

class TestGetBridgeStats:

    def test_no_table(self, tmp_path):
        db = tmp_path / "empty.db"
        with sqlite3.connect(str(db)) as conn:
            conn.execute("CREATE TABLE dummy (id INTEGER PRIMARY KEY)")
        stats = get_bridge_stats(db_path=db)
        assert stats["table_exists"] is False

    def test_empty_table(self, tmp_db):
        stats = get_bridge_stats(db_path=tmp_db)
        assert stats["table_exists"] is True
        assert stats["last_received_at"] is None
        assert stats["messages_today"] == 0

    def test_counts_today(self, tmp_db):
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).date().isoformat()
        with sqlite3.connect(str(tmp_db)) as conn:
            for i in range(3):
                conn.execute(
                    "INSERT INTO messaging_log (platform, direction, message_type, sent_at, status) "
                    "VALUES ('whatsapp', 'inbound', 'media', ?, 'delivered')",
                    (f"{today}T10:00:0{i}+00:00",),
                )
            conn.commit()
        stats = get_bridge_stats(db_path=tmp_db)
        assert stats["messages_today"] == 3
        assert stats["last_received_at"] is not None


# ===========================================================================
# Tests: HTTP endpoint — POST /ingest/openclaw
# ===========================================================================

def _make_server(tmp_db: Path) -> tuple[ThreadingHTTPServer, int]:
    """Start a ReviewDashboardHandler on an ephemeral port and return (server, port)."""
    # Patch DB_PATH inside review_dashboard before importing the handler
    import scripts.review_dashboard as _rd
    _rd.DB_PATH = tmp_db

    from scripts.review_dashboard import ReviewDashboardHandler
    server = ThreadingHTTPServer(("127.0.0.1", 0), ReviewDashboardHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port


def _post_json(port: int, path: str, body: dict) -> tuple[int, dict]:
    data = json.dumps(body).encode()
    req  = Request(
        f"http://127.0.0.1:{port}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=10) as resp:
        return resp.status, json.loads(resp.read())


def _post_json_expect_error(port: int, path: str, body: dict) -> tuple[int, dict]:
    """Like _post_json but handles non-2xx responses."""
    import urllib.error
    data = json.dumps(body).encode()
    req  = Request(
        f"http://127.0.0.1:{port}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


class TestOpenclawEndpoint:

    @pytest.fixture(autouse=True)
    def server(self, tmp_db):
        import scripts.review_dashboard as _rd
        orig_db_path = _rd.DB_PATH
        srv, port = _make_server(tmp_db)
        self._port = port
        self._tmp_db = tmp_db
        yield
        srv.shutdown()
        _rd.DB_PATH = orig_db_path

    def test_unknown_sender_returns_404(self):
        status, body = _post_json_expect_error(
            self._port, "/ingest/openclaw",
            {"platform": "whatsapp", "sender_id": "+10000000000",
             "file_bytes": _JPEG_B64, "media_type": "image/jpeg"},
        )
        assert status == 404
        assert body["status"] == "unknown_sender"

    def test_invalid_json_returns_400(self):
        import urllib.error, urllib.request
        req = Request(
            f"http://127.0.0.1:{self._port}/ingest/openclaw",
            data=b"not json at all",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(req, timeout=10) as resp:
                status, body = resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            status, body = exc.code, json.loads(exc.read())
        assert status == 400
        assert body["error"] == "invalid_json"

    @mock.patch("src.engines.ocr_engine.process_file", side_effect=_fake_process_file)
    def test_successful_ingest_returns_200(self, mock_pf):
        status, body = _post_json(
            self._port, "/ingest/openclaw",
            {
                "platform":   "whatsapp",
                "sender_id":  "+15141234567",
                "media_type": "image/jpeg",
                "file_bytes": _JPEG_B64,
            },
        )
        assert status == 200
        assert body["ok"] is True
        assert body["document_id"] == "doc_test1234"
        assert body["status"] == "processed"

    @mock.patch("src.engines.ocr_engine.process_file", side_effect=_fake_process_file)
    def test_telegram_successful_ingest(self, mock_pf):
        status, body = _post_json(
            self._port, "/ingest/openclaw",
            {
                "platform":   "telegram",
                "sender_id":  "987654321",
                "media_type": "image/jpeg",
                "file_bytes": _JPEG_B64,
            },
        )
        assert status == 200
        assert body["ok"] is True

    def test_text_only_returns_200_no_doc(self):
        status, body = _post_json(
            self._port, "/ingest/openclaw",
            {"platform": "whatsapp", "sender_id": "+15141234567",
             "client_message": "Hello"},
        )
        assert status == 200
        assert body["status"] == "no_file"
        assert body["document_id"] is None

    def test_unsupported_platform_returns_400(self):
        status, body = _post_json_expect_error(
            self._port, "/ingest/openclaw",
            {"platform": "signal", "sender_id": "123"},
        )
        assert status == 400
        assert body["ok"] is False
