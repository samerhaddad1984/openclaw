"""Background queue for async document upload processing.

Fast HTTP upload handlers save files to disk, insert a placeholder row with
review_status='Queued', then enqueue here. Worker threads run the full
OCR + extraction pipeline off-request, so bulk uploads no longer block the
HTTP handler past Cloudflare's 100s limit.
"""
from __future__ import annotations

import json
import logging
import os
import queue
import sqlite3
import threading
from pathlib import Path
from typing import Any, Callable, Optional

from src.engines.ocr_engine import (
    DB_PATH,
    UPLOAD_DIR,
    _new_doc_id,
    _utc_now_iso,
    process_file,
    upsert_document,
)

log = logging.getLogger(__name__)

_DEFAULT_WORKERS = 3


def _safe_filename(name: str) -> str:
    cleaned = "".join(c for c in (name or "") if c.isalnum() or c in "._- ").strip()
    return cleaned or "document"


def save_and_queue_document(
    file_bytes: bytes,
    filename: str,
    *,
    client_code: str = "UNASSIGNED",
    ingest_source: str = "web_upload",
    client_note: str = "",
    submitted_by: str = "",
    db_path: Path | None = None,
    upload_dir: Path | None = None,
) -> str:
    """Save the upload, insert a placeholder row, enqueue for processing.

    Returns the new document_id. The row starts as review_status='Queued'.
    A worker updates it to Processing, then to the pipeline's final status.
    """
    # Resolve defaults at call time so monkeypatching module-level paths
    # in tests actually takes effect.
    db_path = db_path if db_path is not None else DB_PATH
    upload_dir = upload_dir if upload_dir is not None else UPLOAD_DIR

    doc_id = _new_doc_id()
    safe = _safe_filename(filename)
    client_dir = upload_dir / (client_code or "unknown")
    client_dir.mkdir(parents=True, exist_ok=True)
    file_path = client_dir / f"{doc_id}_{safe}"
    file_path.write_bytes(file_bytes)

    now = _utc_now_iso()
    upsert_document(
        {
            "document_id": doc_id,
            "file_name": filename,
            "file_path": str(file_path),
            "client_code": client_code or "UNASSIGNED",
            "vendor": None,
            "doc_type": None,
            "amount": None,
            "document_date": None,
            "review_status": "Queued",
            "confidence": 0.0,
            "raw_result": None,
            "created_at": now,
            "updated_at": now,
            "submitted_by": submitted_by,
            "client_note": client_note,
            "currency": None,
            "subtotal": None,
            "tax_total": None,
            "extraction_method": "queued",
            "ingest_source": ingest_source,
        },
        db_path=db_path,
    )

    get_upload_queue().enqueue(
        doc_id,
        file_path,
        filename,
        client_code,
        ingest_source,
        client_note,
        submitted_by,
        db_path=db_path,
    )
    return doc_id


class UploadQueue:
    """Bounded in-memory queue + worker pool.

    The queue is deliberately in-process so deployment stays simple; if the
    service restarts with items still queued, the placeholder rows stay in the
    DB and can be re-queued via scripts/reprocess_queued.py.
    """

    def __init__(self, num_workers: int = _DEFAULT_WORKERS) -> None:
        self.queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self.num_workers = num_workers
        self.running = True
        self.workers: list[threading.Thread] = []
        for i in range(num_workers):
            t = threading.Thread(
                target=self._worker,
                name=f"upload-worker-{i}",
                daemon=True,
            )
            t.start()
            self.workers.append(t)

    def enqueue(
        self,
        doc_id: str,
        file_path: Path,
        filename: str,
        client_code: str,
        ingest_source: str,
        client_note: str,
        submitted_by: str,
        processor: Optional[Callable] = None,
        db_path: Path | None = None,
    ) -> None:
        self.queue.put(
            {
                "document_id": doc_id,
                "file_path": str(file_path),
                "filename": filename,
                "client_code": client_code or "UNASSIGNED",
                "ingest_source": ingest_source,
                "client_note": client_note or "",
                "submitted_by": submitted_by or "",
                "processor": processor,
                "db_path": str(db_path) if db_path else None,
            }
        )

    def queue_size(self) -> int:
        return self.queue.qsize()

    def wait_idle(self, timeout: float | None = None) -> None:
        """Block until the queue is drained; used in tests."""
        self.queue.join()

    def shutdown(self) -> None:
        self.running = False

    def _worker(self) -> None:
        while self.running:
            try:
                job = self.queue.get(timeout=1.0)
            except queue.Empty:
                continue
            doc_id = job.get("document_id", "")
            job_db = job.get("db_path") or str(DB_PATH)
            try:
                self._process(job)
            except Exception as e:
                log.exception("upload worker failed for %s", doc_id)
                self._mark_error(doc_id, str(e), db_path=job_db)
            finally:
                self.queue.task_done()

    def _process(self, job: dict[str, Any]) -> None:
        doc_id = job["document_id"]
        file_path = Path(job["file_path"])
        filename = job["filename"]
        db_path = job.get("db_path") or str(DB_PATH)

        # Resolve the processor at call time so tests that monkeypatch
        # src.engines.ocr_engine.process_file see their patch applied.
        processor = job.get("processor")
        if processor is None:
            import src.engines.ocr_engine as _ocr  # noqa: PLC0415
            processor = _ocr.process_file

        self._update_status(doc_id, "Processing", db_path=db_path)

        if not file_path.exists():
            raise FileNotFoundError(f"queued file missing: {file_path}")
        file_bytes = file_path.read_bytes()

        result = processor(
            file_bytes,
            filename,
            document_id=doc_id,
            client_code=job["client_code"],
            ingest_source=job["ingest_source"],
            client_note=job["client_note"],
            submitted_by=job["submitted_by"],
        )
        if isinstance(result, dict) and not result.get("ok", True):
            self._mark_error(
                doc_id, result.get("error") or "processing_failed", db_path=db_path,
            )

    @staticmethod
    def _update_status(doc_id: str, status: str, *, db_path: str | None = None) -> None:
        if not doc_id:
            return
        target = db_path or str(DB_PATH)
        try:
            conn = sqlite3.connect(target, timeout=10)
            conn.execute(
                "UPDATE documents SET review_status=?, updated_at=? WHERE document_id=?",
                (status, _utc_now_iso(), doc_id),
            )
            conn.commit()
            conn.close()
        except Exception:
            log.exception("failed to update status for %s", doc_id)

    @staticmethod
    def _mark_error(doc_id: str, msg: str, *, db_path: str | None = None) -> None:
        if not doc_id:
            return
        target = db_path or str(DB_PATH)
        try:
            payload = json.dumps({"error": (msg or "")[:500]})
            conn = sqlite3.connect(target, timeout=10)
            conn.execute(
                "UPDATE documents SET review_status='Error', "
                "raw_result=?, updated_at=? WHERE document_id=?",
                (payload, _utc_now_iso(), doc_id),
            )
            conn.commit()
            conn.close()
        except Exception:
            log.exception("failed to mark error for %s", doc_id)


_upload_queue_lock = threading.Lock()
_upload_queue: Optional[UploadQueue] = None


def get_upload_queue() -> UploadQueue:
    global _upload_queue
    if _upload_queue is None:
        with _upload_queue_lock:
            if _upload_queue is None:
                n = int(os.environ.get("OTOCPA_UPLOAD_WORKERS", _DEFAULT_WORKERS))
                _upload_queue = UploadQueue(num_workers=max(1, n))
    return _upload_queue


def reset_upload_queue_for_tests() -> None:
    """Tests reset the singleton so each test gets a fresh queue."""
    global _upload_queue
    with _upload_queue_lock:
        if _upload_queue is not None:
            _upload_queue.shutdown()
        _upload_queue = None


def get_document_statuses(document_ids: list[str], db_path: Path | None = None) -> dict[str, str]:
    """Map document_id -> review_status for the given IDs."""
    if not document_ids:
        return {}
    ids = [d for d in document_ids if d]
    if not ids:
        return {}
    db_path = db_path if db_path is not None else DB_PATH
    placeholders = ",".join("?" for _ in ids)
    conn = sqlite3.connect(str(db_path), timeout=10)
    try:
        rows = conn.execute(
            f"SELECT document_id, review_status FROM documents WHERE document_id IN ({placeholders})",
            ids,
        ).fetchall()
    finally:
        conn.close()
    return {r[0]: (r[1] or "") for r in rows}
