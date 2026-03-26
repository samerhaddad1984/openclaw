"""
tests/test_folder_watcher.py
============================
Pytest tests for scripts/folder_watcher.py.

All external I/O (OCR pipeline, SQLite) is mocked — tests run in a
temporary directory and leave no persistent state.
"""
from __future__ import annotations

import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# sys.path bootstrap
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.folder_watcher as fw


# ---------------------------------------------------------------------------
# Magic-byte stubs (valid PDF signature so process_file won't reject them)
# ---------------------------------------------------------------------------
_PDF_BYTES = b"%PDF-1.4 minimal content here\n"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_inbox(tmp_path: Path) -> Path:
    inbox = tmp_path / "Inbox"
    inbox.mkdir()
    return inbox


@pytest.fixture(autouse=True)
def reset_state():
    """Reset module-level watcher state between tests."""
    with fw._state["_lock"]:
        fw._state["enabled"]         = False
        fw._state["inbox_folder"]    = ""
        fw._state["processed_today"] = 0
        fw._state["last_file"]       = ""
        fw._state["last_file_at"]    = ""
        fw._state["errors"]          = []
        fw._state["_today_date"]     = ""
    yield


# ---------------------------------------------------------------------------
# match_client_code
# ---------------------------------------------------------------------------

class TestMatchClientCode:
    def test_exact_prefix_match(self):
        assert fw.match_client_code("SOUSSOL_invoice.pdf", ["SOUSSOL", "ABC"], "") == "SOUSSOL"

    def test_underscore_delimited(self):
        assert fw.match_client_code("ABC_2026_01_receipt.pdf", ["ABC", "XYZ"], "") == "ABC"

    def test_hyphen_delimited(self):
        assert fw.match_client_code("XYZ-invoice.pdf", ["XYZ"], "") == "XYZ"

    def test_no_match_returns_default(self):
        assert fw.match_client_code("UNKNOWN_file.pdf", ["SOUSSOL"], "DEFAULT") == "DEFAULT"

    def test_no_match_empty_default(self):
        assert fw.match_client_code("UNKNOWN_file.pdf", [], "") == ""

    def test_case_insensitive(self):
        # known_codes are already uppercased; filename stem is uppercased internally
        assert fw.match_client_code("soussol_invoice.pdf", ["SOUSSOL"], "") == "SOUSSOL"

    def test_no_delimiter_full_stem_match(self):
        assert fw.match_client_code("ACME.pdf", ["ACME"], "") == "ACME"

    def test_multi_part_client_code(self):
        # e.g. client code is "SMITH_CO" and file is "SMITH_CO_jan.pdf"
        assert fw.match_client_code("SMITH_CO_jan.pdf", ["SMITH_CO", "SMITH"], "") == "SMITH_CO"

    def test_longest_prefix_wins(self):
        # "SMITH_CO" should win over "SMITH" when both exist
        codes = ["SMITH", "SMITH_CO"]
        assert fw.match_client_code("SMITH_CO_jan.pdf", codes, "") == "SMITH_CO"


# ---------------------------------------------------------------------------
# known_client_codes (DB query)
# ---------------------------------------------------------------------------

class TestKnownClientCodes:
    def _make_db(self, tmp_path: Path) -> Path:
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE documents (client_code TEXT)")
        conn.execute("CREATE TABLE user_portfolios (client_code TEXT)")
        conn.execute("INSERT INTO documents VALUES ('ALPHA')")
        conn.execute("INSERT INTO documents VALUES ('ALPHA')")   # duplicate
        conn.execute("INSERT INTO documents VALUES ('BETA')")
        conn.execute("INSERT INTO documents VALUES (NULL)")      # null — must be ignored
        conn.execute("INSERT INTO user_portfolios VALUES ('GAMMA')")
        conn.commit()
        conn.close()
        return db

    def test_returns_distinct_uppercased_codes(self, tmp_path: Path):
        db = self._make_db(tmp_path)
        codes = fw.known_client_codes(db)
        assert "ALPHA" in codes
        assert "BETA" in codes
        assert "GAMMA" in codes
        assert codes.count("ALPHA") == 1   # no duplicates

    def test_ignores_null_codes(self, tmp_path: Path):
        db = self._make_db(tmp_path)
        codes = fw.known_client_codes(db)
        assert None not in codes
        assert "" not in codes

    def test_returns_empty_when_db_missing(self, tmp_path: Path):
        codes = fw.known_client_codes(tmp_path / "nonexistent.db")
        assert codes == []

    def test_returns_empty_when_tables_missing(self, tmp_path: Path):
        db = tmp_path / "empty.db"
        sqlite3.connect(str(db)).close()
        codes = fw.known_client_codes(db)
        assert codes == []


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

class TestStateHelpers:
    def test_record_success_increments_counter(self):
        fw._record_success("invoice.pdf")
        status = fw.get_watcher_status()
        assert status["processed_today"] == 1
        assert status["last_file"] == "invoice.pdf"
        assert status["last_file_at"] != ""

    def test_record_success_resets_on_new_day(self):
        with fw._state["_lock"]:
            fw._state["_today_date"] = "2020-01-01"
            fw._state["processed_today"] = 99

        fw._record_success("file.pdf")
        assert fw._state["processed_today"] == 1

    def test_record_error_stores_message(self):
        fw._record_error("something went wrong")
        status = fw.get_watcher_status()
        assert "something went wrong" in status["errors"]

    def test_record_error_caps_at_max(self):
        for i in range(fw._MAX_ERROR_LOG + 5):
            fw._record_error(f"error {i}")
        assert len(fw.get_watcher_status()["errors"]) == fw._MAX_ERROR_LOG

    def test_get_watcher_status_defaults(self):
        status = fw.get_watcher_status()
        assert status["enabled"] is False
        assert status["processed_today"] == 0
        assert status["errors"] == []


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

class TestAuditLog:
    def _make_db_with_audit(self, tmp_path: Path) -> Path:
        db = tmp_path / "audit.db"
        conn = sqlite3.connect(str(db))
        conn.execute("""
            CREATE TABLE audit_log (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type    TEXT NOT NULL DEFAULT 'ai_call',
                username      TEXT,
                document_id   TEXT,
                provider      TEXT,
                task_type     TEXT,
                prompt_snippet TEXT,
                latency_ms    INTEGER,
                created_at    TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.commit()
        conn.close()
        return db

    def test_inserts_folder_intake_row(self, tmp_path: Path):
        db = self._make_db_with_audit(tmp_path)
        fw._log_audit(db, "doc_abc123", "receipt.pdf", "SOUSSOL")

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM audit_log").fetchall()
        conn.close()

        assert len(rows) == 1
        row = rows[0]
        # columns: id, event_type, username, document_id, provider, task_type, prompt_snippet, latency_ms, created_at
        assert row[1] == "folder_intake"        # event_type
        assert row[2] == "folder_watcher"       # username
        assert row[3] == "doc_abc123"           # document_id
        assert row[5] == "SOUSSOL"              # task_type (client_code)
        assert row[6] == "receipt.pdf"          # prompt_snippet (file_name)

    def test_silent_on_missing_table(self, tmp_path: Path):
        db = tmp_path / "no_audit.db"
        sqlite3.connect(str(db)).close()
        # Must not raise
        fw._log_audit(db, "doc_x", "file.pdf", "CLIENT")


# ---------------------------------------------------------------------------
# File move helpers
# ---------------------------------------------------------------------------

class TestFileMoveHelpers:
    def test_move_to_processed_creates_dated_subfolder(self, tmp_inbox: Path):
        src = tmp_inbox / "receipt.pdf"
        src.write_bytes(_PDF_BYTES)

        fw._move_to_processed(src, tmp_inbox)

        assert not src.exists()
        processed_dirs = list((tmp_inbox / "Processed").iterdir())
        assert len(processed_dirs) == 1
        assert (processed_dirs[0] / "receipt.pdf").exists()

    def test_move_to_failed_creates_failed_subfolder(self, tmp_inbox: Path):
        src = tmp_inbox / "bad.pdf"
        src.write_bytes(_PDF_BYTES)

        fw._move_to_failed(src, tmp_inbox)

        assert not src.exists()
        assert (tmp_inbox / "Failed" / "bad.pdf").exists()

    def test_move_to_processed_no_collision(self, tmp_inbox: Path):
        """Two files with the same name must not overwrite each other."""
        src1 = tmp_inbox / "dup.pdf"
        src1.write_bytes(b"%PDF dup1")

        # Pre-create a file in the Processed folder to force the rename path
        from datetime import datetime, timezone
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dest_dir = tmp_inbox / "Processed" / date_str
        dest_dir.mkdir(parents=True, exist_ok=True)
        (dest_dir / "dup.pdf").write_bytes(b"%PDF existing")

        fw._move_to_processed(src1, tmp_inbox)

        files = list(dest_dir.iterdir())
        assert len(files) == 2   # original + renamed copy


# ---------------------------------------------------------------------------
# process_one  (mocked pipeline)
# ---------------------------------------------------------------------------

class TestProcessOne:
    def _db_with_audit(self, tmp_path: Path) -> Path:
        db = tmp_path / "proc.db"
        conn = sqlite3.connect(str(db))
        conn.execute(
            "CREATE TABLE audit_log ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT, "
            "username TEXT, document_id TEXT, provider TEXT, task_type TEXT, "
            "prompt_snippet TEXT, latency_ms INTEGER, created_at TEXT DEFAULT '')"
        )
        conn.execute("CREATE TABLE documents (client_code TEXT)")
        conn.execute("CREATE TABLE user_portfolios (client_code TEXT)")
        conn.commit()
        conn.close()
        return db

    @patch("scripts.folder_watcher._SETTLE_SECONDS", 0)
    @patch("scripts.folder_watcher.process_one.__module__")
    def test_success_moves_to_processed(self, _, tmp_inbox: Path, tmp_path: Path):
        db = self._db_with_audit(tmp_path)
        src = tmp_inbox / "SOUSSOL_invoice.pdf"
        src.write_bytes(_PDF_BYTES)

        ok_result = {"ok": True, "document_id": "doc_test1"}

        with patch("scripts.folder_watcher._SETTLE_SECONDS", 0), \
             patch("src.engines.ocr_engine.process_file", return_value=ok_result):
            fw.process_one(src, tmp_inbox, "", db)

        assert not src.exists()
        processed = list((tmp_inbox / "Processed").rglob("SOUSSOL_invoice.pdf"))
        assert len(processed) == 1

    @patch("scripts.folder_watcher._SETTLE_SECONDS", 0)
    def test_success_writes_audit_log(self, tmp_inbox: Path, tmp_path: Path):
        db = self._db_with_audit(tmp_path)
        src = tmp_inbox / "SOUSSOL_receipt.pdf"
        src.write_bytes(_PDF_BYTES)

        ok_result = {"ok": True, "document_id": "doc_audit_test"}

        with patch("src.engines.ocr_engine.process_file", return_value=ok_result):
            fw.process_one(src, tmp_inbox, "SOUSSOL", db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT event_type, document_id FROM audit_log").fetchall()
        conn.close()
        assert len(rows) == 1
        assert rows[0][0] == "folder_intake"
        assert rows[0][1] == "doc_audit_test"

    @patch("scripts.folder_watcher._SETTLE_SECONDS", 0)
    def test_success_records_state(self, tmp_inbox: Path, tmp_path: Path):
        db = self._db_with_audit(tmp_path)
        src = tmp_inbox / "file.pdf"
        src.write_bytes(_PDF_BYTES)

        with patch("src.engines.ocr_engine.process_file",
                   return_value={"ok": True, "document_id": "doc_x"}):
            fw.process_one(src, tmp_inbox, "", db)

        status = fw.get_watcher_status()
        assert status["processed_today"] == 1
        assert status["last_file"] == "file.pdf"

    @patch("scripts.folder_watcher._SETTLE_SECONDS", 0)
    def test_pipeline_failure_moves_to_failed(self, tmp_inbox: Path, tmp_path: Path):
        db = self._db_with_audit(tmp_path)
        src = tmp_inbox / "bad.pdf"
        src.write_bytes(_PDF_BYTES)

        fail_result = {"ok": False, "error": "unsupported_format:unknown"}

        with patch("src.engines.ocr_engine.process_file", return_value=fail_result):
            fw.process_one(src, tmp_inbox, "", db)

        assert not src.exists()
        assert (tmp_inbox / "Failed" / "bad.pdf").exists()

    @patch("scripts.folder_watcher._SETTLE_SECONDS", 0)
    def test_pipeline_exception_moves_to_failed(self, tmp_inbox: Path, tmp_path: Path):
        db = self._db_with_audit(tmp_path)
        src = tmp_inbox / "explode.pdf"
        src.write_bytes(_PDF_BYTES)

        with patch("src.engines.ocr_engine.process_file",
                   side_effect=RuntimeError("vision API down")):
            fw.process_one(src, tmp_inbox, "", db)

        assert not src.exists()
        assert (tmp_inbox / "Failed" / "explode.pdf").exists()

    @patch("scripts.folder_watcher._SETTLE_SECONDS", 0)
    def test_pipeline_exception_records_error(self, tmp_inbox: Path, tmp_path: Path):
        db = self._db_with_audit(tmp_path)
        src = tmp_inbox / "oops.pdf"
        src.write_bytes(_PDF_BYTES)

        with patch("src.engines.ocr_engine.process_file",
                   side_effect=RuntimeError("boom")):
            fw.process_one(src, tmp_inbox, "", db)

        status = fw.get_watcher_status()
        assert any("oops.pdf" in e for e in status["errors"])

    @patch("scripts.folder_watcher._SETTLE_SECONDS", 0)
    def test_missing_file_is_silently_skipped(self, tmp_inbox: Path, tmp_path: Path):
        db = self._db_with_audit(tmp_path)
        ghost = tmp_inbox / "ghost.pdf"
        # Do NOT create the file — simulates a race where file was deleted
        with patch("src.engines.ocr_engine.process_file") as mock_pf:
            fw.process_one(ghost, tmp_inbox, "", db)
            mock_pf.assert_not_called()


# ---------------------------------------------------------------------------
# _FileDispatcher (no duplicate dispatching)
# ---------------------------------------------------------------------------

class TestFileDispatcher:
    def test_does_not_dispatch_same_file_twice(self, tmp_inbox: Path, tmp_path: Path):
        called: list[Path] = []

        def fake_process_one(file_path, inbox, default, db):
            called.append(file_path)

        db = tmp_path / "any.db"
        dispatcher = fw._FileDispatcher(tmp_inbox, "", db)

        src = tmp_inbox / "once.pdf"
        src.write_bytes(_PDF_BYTES)

        with patch("scripts.folder_watcher.process_one", side_effect=fake_process_one):
            dispatcher.dispatch(src)
            dispatcher.dispatch(src)  # second dispatch of the same file

        time.sleep(0.1)  # let daemon thread finish
        assert len(called) == 1

    def test_ignores_unsupported_extension(self, tmp_inbox: Path, tmp_path: Path):
        db = tmp_path / "any.db"
        dispatcher = fw._FileDispatcher(tmp_inbox, "", db)
        txt_file = tmp_inbox / "notes.txt"
        txt_file.write_text("hello")

        with patch("scripts.folder_watcher.process_one") as mock_po:
            dispatcher.dispatch(txt_file)
            time.sleep(0.05)
            mock_po.assert_not_called()


# ---------------------------------------------------------------------------
# _scan_existing (startup scan)
# ---------------------------------------------------------------------------

class TestStartupScan:
    def test_scan_processes_existing_pdf(self, tmp_inbox: Path, tmp_path: Path):
        existing = tmp_inbox / "SOUSSOL_2026.pdf"
        existing.write_bytes(_PDF_BYTES)

        dispatched: list[Path] = []

        class _FakeDispatcher:
            def dispatch(self, p: Path) -> None:
                dispatched.append(p)

        fw._scan_existing(tmp_inbox, _FakeDispatcher())  # type: ignore[arg-type]
        assert existing in dispatched

    def test_scan_ignores_txt_files(self, tmp_inbox: Path):
        txt = tmp_inbox / "readme.txt"
        txt.write_text("ignore me")

        dispatched: list[Path] = []

        class _FakeDispatcher:
            def dispatch(self, p: Path) -> None:
                dispatched.append(p)

        fw._scan_existing(tmp_inbox, _FakeDispatcher())  # type: ignore[arg-type]
        assert txt not in dispatched

    def test_scan_ignores_subdirectories(self, tmp_inbox: Path):
        subdir = tmp_inbox / "Processed"
        subdir.mkdir()

        dispatched: list[Path] = []

        class _FakeDispatcher:
            def dispatch(self, p: Path) -> None:
                dispatched.append(p)

        fw._scan_existing(tmp_inbox, _FakeDispatcher())  # type: ignore[arg-type]
        # Directories must not appear in dispatched
        for p in dispatched:
            assert p.is_file()


# ---------------------------------------------------------------------------
# start_folder_watcher
# ---------------------------------------------------------------------------

class TestStartFolderWatcher:
    def test_returns_none_when_no_inbox_configured(self, tmp_path: Path):
        with patch("scripts.folder_watcher._load_config", return_value={}):
            result = fw.start_folder_watcher()
        assert result is None

    def test_creates_inbox_folder_if_missing(self, tmp_path: Path):
        inbox = tmp_path / "NewInbox"
        assert not inbox.exists()

        with patch("scripts.folder_watcher._run_watcher"):
            thread = fw.start_folder_watcher(
                inbox_folder=inbox,
                default_client_code="",
                db_path=tmp_path / "dummy.db",
            )

        assert inbox.exists()
        assert thread is not None
        thread.join(timeout=0.5)   # daemon thread; just check it started

    def test_sets_enabled_state(self, tmp_path: Path):
        inbox = tmp_path / "WatchInbox"

        with patch("scripts.folder_watcher._run_watcher"):
            fw.start_folder_watcher(
                inbox_folder=inbox,
                default_client_code="",
                db_path=tmp_path / "dummy.db",
            )

        status = fw.get_watcher_status()
        assert status["enabled"] is True
        assert str(inbox) in status["inbox_folder"]

    def test_reads_config_for_defaults(self, tmp_path: Path):
        inbox = tmp_path / "ConfigInbox"
        cfg = {
            "inbox_folder": str(inbox),
            "default_client_code": "TESTCLIENT",
        }
        with patch("scripts.folder_watcher._load_config", return_value=cfg), \
             patch("scripts.folder_watcher._run_watcher"):
            thread = fw.start_folder_watcher()

        assert thread is not None
        thread.join(timeout=0.5)


# ---------------------------------------------------------------------------
# _is_supported
# ---------------------------------------------------------------------------

class TestIsSupported:
    @pytest.mark.parametrize("filename,expected", [
        ("doc.pdf",  True),
        ("img.jpg",  True),
        ("img.jpeg", True),
        ("img.png",  True),
        ("img.heic", True),
        ("img.tiff", True),
        ("img.tif",  True),
        ("img.webp", True),
        ("doc.txt",  False),
        ("doc.docx", False),
        ("doc.csv",  False),
        ("doc.PDF",  True),   # case-insensitive
        ("doc.PNG",  True),
    ])
    def test_supported(self, filename: str, expected: bool):
        assert fw._is_supported(Path(filename)) is expected
