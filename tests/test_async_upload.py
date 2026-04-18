"""Tests for async upload queue.

Cloudflare times out uploads at 100s; the async queue keeps HTTP handlers
fast by enqueuing files for background processing. These tests stub out the
OCR pipeline so they stay deterministic and fast.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

import pytest

from src.engines.ocr_engine import upsert_document
from src.engines import upload_queue as uq


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    conn = sqlite3.connect(str(db))
    conn.execute("""CREATE TABLE documents (
        document_id TEXT PRIMARY KEY, file_name TEXT, file_path TEXT,
        client_code TEXT, vendor TEXT, doc_type TEXT, amount REAL,
        document_date TEXT, gl_account TEXT, tax_code TEXT, category TEXT,
        review_status TEXT, confidence REAL, raw_result TEXT,
        submitted_by TEXT, client_note TEXT, currency TEXT,
        subtotal REAL, tax_total REAL, extraction_method TEXT,
        ingest_source TEXT, created_at TEXT, updated_at TEXT,
        raw_ocr_text TEXT, hallucination_suspected INTEGER,
        handwriting_low_confidence INTEGER, ai_used INTEGER,
        ai_complexity TEXT, ai_model_used TEXT, ai_cost REAL,
        raw_ai_response TEXT, logical_fingerprint TEXT
    )""")
    conn.commit()
    conn.close()

    monkeypatch.setattr(uq, "DB_PATH", db)
    monkeypatch.setattr(uq, "UPLOAD_DIR", uploads)
    import src.engines.ocr_engine as oe
    monkeypatch.setattr(oe, "DB_PATH", db)
    monkeypatch.setattr(oe, "UPLOAD_DIR", uploads)

    uq.reset_upload_queue_for_tests()
    yield db
    uq.reset_upload_queue_for_tests()


def _fake_process(seen, delay=0.0, ok=True, error=None):
    def _fn(file_bytes, filename, *, document_id, client_code,
            ingest_source, client_note, submitted_by, **kw):
        if delay:
            time.sleep(delay)
        seen.append(document_id)
        conn = sqlite3.connect(str(uq.DB_PATH))
        conn.execute(
            "UPDATE documents SET review_status=?, vendor=?, amount=?, "
            "extraction_method=? WHERE document_id=?",
            ("Ready" if ok else "Error", "TEST VENDOR", 42.0, "test_fake", document_id),
        )
        conn.commit()
        conn.close()
        return {"ok": ok, "document_id": document_id, "error": error}
    return _fn


def test_upload_returns_immediately_with_document_ids(tmp_db):
    # Background worker is slow so we can prove save_and_queue returns
    # before processing finishes.
    seen: list[str] = []
    fn = _fake_process(seen, delay=0.5)
    q = uq.get_upload_queue()
    q.enqueue = lambda *a, **kw: _override_enqueue(q, fn, *a, **kw)

    t0 = time.time()
    doc_id = uq.save_and_queue_document(
        b"hello", "a.pdf", client_code="C1", ingest_source="web_upload",
    )
    elapsed = time.time() - t0

    assert doc_id.startswith("doc_")
    assert elapsed < 0.3, f"save_and_queue should be fast, took {elapsed:.2f}s"

    conn = sqlite3.connect(str(tmp_db))
    row = conn.execute(
        "SELECT review_status FROM documents WHERE document_id=?", (doc_id,),
    ).fetchone()
    conn.close()
    # It's either still Queued or Processing/Ready — just not errored.
    assert row[0] in ("Queued", "Processing", "Ready")


def _override_enqueue(q, fn, doc_id, file_path, filename, client_code,
                      ingest_source, client_note, submitted_by,
                      processor=None, db_path=None):
    # Shim to inject the fake processor into the queue payload.
    q.queue.put({
        "document_id": doc_id, "file_path": str(file_path),
        "filename": filename, "client_code": client_code,
        "ingest_source": ingest_source, "client_note": client_note,
        "submitted_by": submitted_by, "processor": fn,
        "db_path": str(db_path) if db_path else None,
    })


def test_upload_queue_processes_in_background(tmp_db):
    seen: list[str] = []
    fn = _fake_process(seen, delay=0.0)
    q = uq.get_upload_queue()
    q.enqueue = lambda *a, **kw: _override_enqueue(q, fn, *a, **kw)

    doc_id = uq.save_and_queue_document(b"x", "f.pdf", client_code="C1")
    q.wait_idle()

    assert doc_id in seen
    statuses = uq.get_document_statuses([doc_id])
    assert statuses.get(doc_id) == "Ready"


def test_status_endpoint_returns_current_state(tmp_db):
    # Insert 3 documents directly, in different states.
    for did, st in [("doc_a", "Queued"), ("doc_b", "Processing"), ("doc_c", "Ready")]:
        upsert_document({
            "document_id": did, "file_name": f"{did}.pdf", "file_path": "",
            "client_code": "C1",
            "vendor": None, "doc_type": None, "amount": None,
            "document_date": None,
            "review_status": st, "confidence": 0.0,
            "raw_result": None, "created_at": "2026-04-18T00:00:00+00:00",
            "updated_at": "2026-04-18T00:00:00+00:00",
            "submitted_by": "", "client_note": "", "currency": None,
            "subtotal": None, "tax_total": None, "extraction_method": "",
            "ingest_source": "test",
        }, db_path=tmp_db)

    statuses = uq.get_document_statuses(["doc_a", "doc_b", "doc_c", "doc_missing"])
    assert statuses["doc_a"] == "Queued"
    assert statuses["doc_b"] == "Processing"
    assert statuses["doc_c"] == "Ready"
    assert "doc_missing" not in statuses


def test_failed_processing_marks_error_status(tmp_db):
    def boom(*args, **kwargs):
        raise RuntimeError("docai exploded")

    q = uq.get_upload_queue()
    q.enqueue = lambda *a, **kw: _override_enqueue(q, boom, *a, **kw)

    doc_id = uq.save_and_queue_document(b"x", "f.pdf", client_code="C1")
    q.wait_idle()

    statuses = uq.get_document_statuses([doc_id])
    assert statuses.get(doc_id) == "Error"
    conn = sqlite3.connect(str(tmp_db))
    raw = conn.execute(
        "SELECT raw_result FROM documents WHERE document_id=?", (doc_id,),
    ).fetchone()[0]
    conn.close()
    assert "docai exploded" in (raw or "")


def test_queue_handles_20_parallel_uploads(tmp_db):
    seen: list[str] = []
    lock = threading.Lock()

    def fn(file_bytes, filename, *, document_id, client_code, ingest_source,
           client_note, submitted_by, **kw):
        with lock:
            seen.append(document_id)
        conn = sqlite3.connect(str(uq.DB_PATH))
        conn.execute(
            "UPDATE documents SET review_status='Ready' WHERE document_id=?",
            (document_id,),
        )
        conn.commit()
        conn.close()
        return {"ok": True, "document_id": document_id}

    q = uq.get_upload_queue()
    q.enqueue = lambda *a, **kw: _override_enqueue(q, fn, *a, **kw)

    doc_ids = [
        uq.save_and_queue_document(
            f"file{i}".encode(), f"f{i}.pdf", client_code="C1",
        )
        for i in range(20)
    ]
    q.wait_idle()

    assert len(seen) == 20
    statuses = uq.get_document_statuses(doc_ids)
    assert all(v == "Ready" for v in statuses.values())


def test_batch_size_limit_enforced():
    # _batch_limit_error lives in review_dashboard.py. Import lazily so the
    # test doesn't pull in the HTTP server on collection.
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "rd", str(Path(__file__).resolve().parent.parent / "scripts" / "review_dashboard.py"),
    )
    # Only load the function — we avoid executing main() by patching __name__.
    import sys
    if "rd" in sys.modules:
        mod = sys.modules["rd"]
    else:
        mod = importlib.util.module_from_spec(spec)
        sys.modules["rd"] = mod
        try:
            spec.loader.exec_module(mod)
        except SystemExit:
            pass

    limit = mod._batch_limit_error

    assert limit([("a.pdf", b"x")]) is None  # small batch passes
    files = [(f"f{i}.pdf", b"x") for i in range(51)]
    err = limit(files)
    assert err and "Too many files" in err

    big = [("big.pdf", b"y" * (101 * 1024 * 1024))]
    err = limit(big)
    assert err and "Upload too large" in err
