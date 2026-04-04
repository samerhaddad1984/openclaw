"""
scripts/folder_watcher.py
=========================
Background folder-watcher service for OtoCPA.

Monitors a local inbox folder and automatically processes any new files
dropped into it via the existing OCR pipeline (src/engines/ocr_engine.py).

Configuration (otocpa.config.json):
    {
        "inbox_folder":          "C:\\OtoCPA\\Inbox",
        "folder_watcher_enabled": true,
        "default_client_code":   ""
    }

Processed files → Inbox/Processed/YYYY-MM-DD/
Failed files    → Inbox/Failed/

Supported formats (detected by magic bytes, same as ocr_engine):
    PDF, JPG, PNG, HEIC, TIFF, WebP
"""
from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONFIG_PATH = ROOT_DIR / "otocpa.config.json"
DB_PATH     = ROOT_DIR / "data" / "otocpa_agent.db"

SUPPORTED_SUFFIXES: frozenset[str] = frozenset(
    {".pdf", ".jpg", ".jpeg", ".png", ".heic", ".tiff", ".tif", ".webp"}
)

_SETTLE_SECONDS = 2          # wait after creation before reading
_MAX_ERROR_LOG  = 20         # keep last N errors in memory


# ---------------------------------------------------------------------------
# Module-level status state  (safe to read from /troubleshoot)
# ---------------------------------------------------------------------------

_state: dict[str, Any] = {
    "enabled":         False,
    "inbox_folder":    "",
    "processed_today": 0,
    "last_file":       "",
    "last_file_at":    "",
    "errors":          [],
    "_lock":           threading.Lock(),
    "_today_date":     "",
}


def get_watcher_status() -> dict[str, Any]:
    """Return a safe snapshot of the watcher state (callable from any thread)."""
    with _state["_lock"]:
        return {
            "enabled":         _state["enabled"],
            "inbox_folder":    _state["inbox_folder"],
            "processed_today": _state["processed_today"],
            "last_file":       _state["last_file"],
            "last_file_at":    _state["last_file_at"],
            "errors":          list(_state["errors"]),
        }


def _record_success(file_name: str) -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with _state["_lock"]:
        if _state["_today_date"] != today:
            _state["_today_date"] = today
            _state["processed_today"] = 0
        _state["processed_today"] += 1
        _state["last_file"]    = file_name
        _state["last_file_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _record_error(msg: str) -> None:
    with _state["_lock"]:
        _state["errors"].append(msg)
        if len(_state["errors"]) > _MAX_ERROR_LOG:
            _state["errors"] = _state["errors"][-_MAX_ERROR_LOG:]


# ---------------------------------------------------------------------------
# Config helper
# ---------------------------------------------------------------------------

def _load_config() -> dict[str, Any]:
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# UTC timestamp helper
# ---------------------------------------------------------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Client-code matching
# ---------------------------------------------------------------------------

def known_client_codes(db_path: Path = DB_PATH) -> list[str]:
    """Return all distinct non-empty client codes from the database (uppercased)."""
    codes: set[str] = set()
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            for row in conn.execute(
                "SELECT DISTINCT client_code FROM documents "
                "WHERE client_code IS NOT NULL AND client_code != ''"
            ).fetchall():
                codes.add(str(row[0]).upper())
            for row in conn.execute(
                "SELECT DISTINCT client_code FROM user_portfolios "
                "WHERE client_code IS NOT NULL AND client_code != ''"
            ).fetchall():
                codes.add(str(row[0]).upper())
        except Exception:
            pass
        finally:
            conn.close()
    except Exception:
        pass
    return sorted(codes)


def get_client_from_subfolder(
    file_path: Path, inbox_folder: Path, conn: sqlite3.Connection,
) -> str | None:
    """
    If the file is inside a subfolder of the inbox, check whether that
    subfolder name matches a known client code in the database.

    Example: inbox/BOLDUC/invoice.pdf → returns 'BOLDUC'
    """
    subfolder = file_path.parent.name
    if subfolder != inbox_folder.name:
        row = conn.execute(
            "SELECT client_code FROM clients WHERE client_code = ? OR UPPER(client_code) = UPPER(?)",
            (subfolder, subfolder),
        ).fetchone()
        if row:
            return row[0]
        # Also check documents/user_portfolios for known codes
        row = conn.execute(
            "SELECT DISTINCT client_code FROM documents "
            "WHERE UPPER(client_code) = UPPER(?) AND client_code IS NOT NULL AND client_code != '' "
            "LIMIT 1",
            (subfolder,),
        ).fetchone()
        if row:
            return row[0]
        row = conn.execute(
            "SELECT DISTINCT client_code FROM user_portfolios "
            "WHERE UPPER(client_code) = UPPER(?) AND client_code IS NOT NULL AND client_code != '' "
            "LIMIT 1",
            (subfolder,),
        ).fetchone()
        if row:
            return row[0]
    return None


def match_client_code(filename: str, known_codes: list[str], default: str = "") -> str:
    """
    Try to match the filename prefix (before the first ``_`` or ``-``) against
    a known client code.  Tries progressively shorter prefixes so that both
    ``SOUSSOL_invoice.pdf`` and ``SOUSSOL-ACME_invoice.pdf`` will match SOUSSOL.

    Falls back to *default* when nothing matches.
    """
    stem = Path(filename).stem.upper()
    parts = stem.replace("-", "_").split("_")
    for length in range(len(parts), 0, -1):
        candidate = "_".join(parts[:length])
        if candidate in known_codes:
            return candidate
    return default


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def _log_audit(
    db_path: Path,
    document_id: str,
    file_name: str,
    client_code: str,
) -> None:
    """Insert a folder_intake row into audit_log."""
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute(
                """
                INSERT INTO audit_log
                    (event_type, document_id, task_type, prompt_snippet, username, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("folder_intake", document_id, client_code, file_name,
                 "folder_watcher", _utc_now_iso()),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logging.warning("folder_watcher: audit_log insert failed: %s", exc)


# ---------------------------------------------------------------------------
# File move helpers
# ---------------------------------------------------------------------------

def _move_to_processed(file_path: Path, inbox_folder: Path) -> None:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dest_dir = inbox_folder / "Processed" / date_str
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / file_path.name
    if dest.exists():
        dest = dest_dir / f"{file_path.stem}_{int(time.monotonic_ns())}{file_path.suffix}"
    try:
        shutil.move(str(file_path), str(dest))
    except Exception as exc:
        logging.warning("folder_watcher: could not move %s to Processed: %s",
                        file_path.name, exc)


def _move_to_failed(file_path: Path, inbox_folder: Path) -> None:
    dest_dir = inbox_folder / "Failed"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / file_path.name
    if dest.exists():
        dest = dest_dir / f"{file_path.stem}_{int(time.monotonic_ns())}{file_path.suffix}"
    try:
        shutil.move(str(file_path), str(dest))
    except Exception as exc:
        logging.warning("folder_watcher: could not move %s to Failed: %s",
                        file_path.name, exc)


# ---------------------------------------------------------------------------
# Core processing step
# ---------------------------------------------------------------------------

def _is_supported(path: Path) -> bool:
    return path.suffix.lower() in SUPPORTED_SUFFIXES


def process_one(
    file_path: Path,
    inbox_folder: Path,
    default_client_code: str,
    db_path: Path,
) -> None:
    """
    Run a single file through the OCR pipeline.

    Waits for the file to settle, resolves the client code from the filename
    prefix, calls process_file(), then moves the file to Processed/ or Failed/.
    Also writes an audit_log row on success.
    """
    # Allow the OS to finish writing the file
    time.sleep(_SETTLE_SECONDS)

    if not file_path.exists():
        return  # deleted or moved before we could read it

    try:
        file_bytes = file_path.read_bytes()
    except Exception as exc:
        err_msg = f"{file_path.name}: could not read file — {exc}"
        logging.error("folder_watcher: %s", err_msg)
        _record_error(err_msg)
        _move_to_failed(file_path, inbox_folder)
        return

    filename = file_path.name

    # ------------------------------------------------------------------
    # Resolve client code — priority order:
    # 1. Subfolder name (most reliable)
    # 2. Filename prefix (existing logic)
    # 3. Unassigned (fallback)
    # ------------------------------------------------------------------
    client_code = ""
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            client_code = get_client_from_subfolder(file_path, inbox_folder, conn) or ""
        finally:
            conn.close()
    except Exception:
        pass

    if not client_code:
        codes = known_client_codes(db_path)
        client_code = match_client_code(filename, codes, default_client_code)

    # FIX 2 — If still no client code, mark as UNASSIGNED
    is_unassigned = False
    if not client_code:
        client_code = "UNASSIGNED"
        is_unassigned = True

    # Import here so unit tests can patch it without importing the whole engine
    from src.engines.ocr_engine import process_file  # noqa: PLC0415

    try:
        result = process_file(
            file_bytes,
            filename,
            client_code=client_code,
            ingest_source="folder_intake",
            db_path=db_path,
        )
    except Exception as exc:
        err_msg = f"{filename}: {exc}"
        logging.error("folder_watcher: process_file raised: %s", exc)
        _record_error(err_msg)
        _move_to_failed(file_path, inbox_folder)
        return

    if not result.get("ok"):
        err_msg = f"{filename}: {result.get('error', 'unknown error')}"
        logging.error("folder_watcher: pipeline failed — %s", err_msg)
        _record_error(err_msg)
        _move_to_failed(file_path, inbox_folder)
        return

    # Success path
    doc_id = result.get("document_id", "")

    # Mark UNASSIGNED documents as NeedsReview
    if is_unassigned and doc_id:
        try:
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute(
                    "UPDATE documents SET review_status = ?, review_note = ?, updated_at = ? "
                    "WHERE document_id = ?",
                    (
                        "NeedsReview",
                        "Client non identifié — assigner manuellement / Client not identified — assign manually",
                        _utc_now_iso(),
                        doc_id,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("folder_watcher: failed to set UNASSIGNED review status: %s", exc)

    # FIX 3 — Cross-client mismatch detection
    if doc_id and client_code != "UNASSIGNED":
        try:
            from src.engines.client_mismatch_engine import detect_client_mismatch  # noqa: PLC0415
            extracted_data = {
                k: result.get(k, "")
                for k in ("raw_ocr_text", "bill_to", "billing_email", "gst_number", "qst_number", "vendor")
            }
            conn = sqlite3.connect(str(db_path))
            try:
                mismatch = detect_client_mismatch(extracted_data, client_code, conn)
                if mismatch.get("mismatch_detected"):
                    conn.execute(
                        "UPDATE documents SET review_status = ?, review_note = ?, updated_at = ? "
                        "WHERE document_id = ?",
                        (
                            "NeedsReview",
                            f"Mismatch client détecté — suggéré: {mismatch.get('suggested_client_code', '?')}",
                            _utc_now_iso(),
                            doc_id,
                        ),
                    )
                    conn.commit()
                    logging.info(
                        "folder_watcher: mismatch detected for %s — suggested client: %s",
                        filename, mismatch.get("suggested_client_code"),
                    )
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("folder_watcher: mismatch detection failed: %s", exc)

    _log_audit(db_path, doc_id, filename, client_code)
    _move_to_processed(file_path, inbox_folder)
    _record_success(filename)
    logging.info(
        "folder_watcher: processed %s → doc_id=%s client=%s",
        filename, doc_id, client_code,
    )


# ---------------------------------------------------------------------------
# File dispatcher (shared between watchdog and polling loop)
# ---------------------------------------------------------------------------

class _FileDispatcher:
    """
    Tracks files we have already dispatched to avoid duplicate processing,
    and spawns a daemon thread per file.
    """

    def __init__(
        self,
        inbox_folder: Path,
        default_client_code: str,
        db_path: Path,
    ) -> None:
        self._inbox = inbox_folder
        self._default = default_client_code
        self._db = db_path
        self._seen: set[str] = set()
        self._lock = threading.Lock()

    def dispatch(self, file_path: Path) -> None:
        if not _is_supported(file_path):
            return
        key = str(file_path.resolve())
        with self._lock:
            if key in self._seen:
                return
            self._seen.add(key)
        thread = threading.Thread(
            target=process_one,
            args=(file_path, self._inbox, self._default, self._db),
            daemon=True,
            name=f"watcher-{file_path.name}",
        )
        thread.start()


# ---------------------------------------------------------------------------
# Startup scan (catches files dropped while service was offline)
# ---------------------------------------------------------------------------

def _scan_existing(inbox_folder: Path, dispatcher: _FileDispatcher) -> None:
    try:
        for entry in inbox_folder.rglob("*"):
            if entry.is_file() and _is_supported(entry):
                # Skip files in Processed/ and Failed/ subdirectories
                rel = entry.relative_to(inbox_folder)
                if rel.parts and rel.parts[0] in ("Processed", "Failed"):
                    continue
                dispatcher.dispatch(entry)
    except Exception as exc:
        logging.warning("folder_watcher: startup scan error: %s", exc)


# ---------------------------------------------------------------------------
# Watcher loop (watchdog preferred, polling fallback)
# ---------------------------------------------------------------------------

def _run_watcher(
    inbox_folder: Path,
    default_client_code: str,
    db_path: Path,
) -> None:
    """
    Main loop — runs in a dedicated daemon thread.
    Uses watchdog if installed; falls back to 5-second polling.
    """
    dispatcher = _FileDispatcher(inbox_folder, default_client_code, db_path)

    # Startup scan — process any files already in the inbox
    _scan_existing(inbox_folder, dispatcher)

    try:
        from watchdog.observers import Observer  # type: ignore[import]
        from watchdog.events import FileSystemEventHandler as _WDBase  # type: ignore[import]

        class _Bridge(_WDBase):
            def on_created(self, event: Any) -> None:  # type: ignore[override]
                if not event.is_directory:
                    dispatcher.dispatch(Path(event.src_path))

            def on_moved(self, event: Any) -> None:  # type: ignore[override]
                if not event.is_directory:
                    dispatcher.dispatch(Path(event.dest_path))

        observer = Observer()
        observer.schedule(_Bridge(), str(inbox_folder), recursive=True)
        observer.start()
        logging.info("folder_watcher: watchdog observer started on %s", inbox_folder)
        try:
            while True:
                time.sleep(1)
        finally:
            observer.stop()
            observer.join()

    except ImportError:
        # watchdog not installed — fall back to polling
        logging.warning(
            "folder_watcher: watchdog not available — using 5-second polling fallback"
        )
        def _poll_files(folder: Path) -> set[str]:
            result: set[str] = set()
            for f in folder.rglob("*"):
                if f.is_file():
                    rel = f.relative_to(folder)
                    if rel.parts and rel.parts[0] in ("Processed", "Failed"):
                        continue
                    result.add(str(f))
            return result

        seen: set[str] = _poll_files(inbox_folder)
        while True:
            time.sleep(5)
            try:
                current = _poll_files(inbox_folder)
                for new_str in current - seen:
                    dispatcher.dispatch(Path(new_str))
                seen = current
            except Exception as exc:
                logging.error("folder_watcher: polling error: %s", exc)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def start_folder_watcher(
    *,
    inbox_folder: Path | None = None,
    default_client_code: str | None = None,
    db_path: Path = DB_PATH,
) -> "threading.Thread | None":
    """
    Start the folder watcher in a background daemon thread.

    Reads otocpa.config.json when *inbox_folder* is not supplied.
    Returns the Thread if started, or None if no inbox_folder is configured.

    Guarded by the caller: only call this when ``folder_watcher_enabled`` is
    true and ``inbox_folder`` is set in the config.
    """
    cfg = _load_config()

    if inbox_folder is None:
        folder_str = cfg.get("inbox_folder", "")
        if not folder_str:
            logging.info("folder_watcher: inbox_folder not configured — watcher skipped")
            return None
        inbox_folder = Path(folder_str)

    if default_client_code is None:
        default_client_code = cfg.get("default_client_code", "")

    # Ensure the inbox folder exists
    inbox_folder.mkdir(parents=True, exist_ok=True)

    # Update shared status state
    with _state["_lock"]:
        _state["enabled"]      = True
        _state["inbox_folder"] = str(inbox_folder)

    thread = threading.Thread(
        target=_run_watcher,
        args=(inbox_folder, default_client_code, db_path),
        daemon=True,
        name="folder-watcher",
    )
    thread.start()
    logging.info("folder_watcher: started — watching %s", inbox_folder)
    return thread


# ---------------------------------------------------------------------------
# CLI entry point (manual testing / standalone run)
# ---------------------------------------------------------------------------

def main() -> int:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="OtoCPA Folder Watcher")
    parser.add_argument("--folder", help="Inbox folder path (overrides config)")
    parser.add_argument("--default-client", default="", help="Default client code")
    args = parser.parse_args()

    inbox = Path(args.folder) if args.folder else None
    thread = start_folder_watcher(
        inbox_folder=inbox,
        default_client_code=args.default_client,
    )
    if thread is None:
        print("Watcher not started — set inbox_folder in otocpa.config.json")
        return 1
    try:
        thread.join()
    except KeyboardInterrupt:
        print("\nShutting down folder watcher...")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
