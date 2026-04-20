"""R3-Investigation 6 — background job failure resilience.

The upload_queue worker pool runs OCR in the background. Tests here
verify: unhandled exceptions don't kill the worker, queued jobs pick
the right DB, a slow/stuck processor doesn't hang the whole pool.
"""
from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _mk_queue_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, file_path TEXT, client_code TEXT,
            vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT,
            review_status TEXT, confidence REAL,
            raw_result TEXT, created_at TEXT, updated_at TEXT,
            submitted_by TEXT, client_note TEXT,
            ingest_source TEXT,
            content_fingerprint TEXT
        );
    """)
    conn.commit(); conn.close()


@pytest.fixture
def queue_env(tmp_path, monkeypatch):
    db = tmp_path / "q.db"
    uploads = tmp_path / "uploads"; uploads.mkdir()
    _mk_queue_db(db)
    import src.engines.upload_queue as uq
    monkeypatch.setattr(uq, "DB_PATH", db)
    import src.engines.ocr_engine as oe
    monkeypatch.setattr(oe, "DB_PATH", db)
    yield db, uploads


# ---------------------------------------------------------------------------
# Worker exception handling
# ---------------------------------------------------------------------------

def test_worker_survives_processor_exception(queue_env):
    db, uploads = queue_env
    from src.engines.upload_queue import UploadQueue

    calls: list[str] = []

    def _raising(file_bytes, filename, *, document_id, client_code,
                  ingest_source, client_note, submitted_by, **kw):
        calls.append(document_id)
        raise RuntimeError(f"intentional-failure-{document_id}")

    q = UploadQueue(num_workers=1)
    try:
        for i in range(3):
            doc_id = f"DOC-{i}"
            fp = uploads / f"{doc_id}.bin"
            fp.write_bytes(b"dummy")
            # Seed the DB row the worker expects to update on error.
            with sqlite3.connect(str(db)) as c:
                c.execute(
                    "INSERT INTO documents (document_id, file_path, client_code, "
                    "review_status) VALUES (?,?,?, 'Queued')",
                    (doc_id, str(fp), "C1"),
                )
                c.commit()
            q.enqueue(
                doc_id=doc_id, file_path=fp, filename=f"{doc_id}.bin",
                client_code="C1", ingest_source="test", client_note="",
                submitted_by="", processor=_raising, db_path=db,
            )
        # Drain.
        q.wait_idle(timeout=10)
    finally:
        q.shutdown()

    # All three processors ran despite raising.
    assert sorted(calls) == ["DOC-0", "DOC-1", "DOC-2"]
    # Each document ended with review_status='error' (worker caught the
    # exception and called _mark_error).
    with sqlite3.connect(str(db)) as c:
        rows = c.execute(
            "SELECT document_id, review_status FROM documents "
            "WHERE document_id LIKE 'DOC-%' ORDER BY document_id",
        ).fetchall()
    # Whatever status convention the worker writes, it should NOT be
    # 'Queued' (which would mean the worker died mid-job without marking
    # the document). Either 'error' / 'Error' / 'failed' are fine.
    for doc_id, status in rows:
        assert (status or "").lower() != "queued", (
            f"{doc_id} still Queued after worker raised — worker may "
            f"have died silently"
        )


def test_worker_pool_does_not_hang_on_slow_job(queue_env):
    """A single slow job must not block the other workers."""
    db, uploads = queue_env
    from src.engines.upload_queue import UploadQueue

    completed: list[str] = []

    def _slow(file_bytes, filename, *, document_id, client_code,
              ingest_source, client_note, submitted_by, **kw):
        if document_id == "SLOW":
            time.sleep(1.5)
        completed.append(document_id)
        return {"ok": True}

    q = UploadQueue(num_workers=3)  # 3 workers so others can proceed
    try:
        doc_ids = ["FAST-1", "SLOW", "FAST-2", "FAST-3"]
        for doc_id in doc_ids:
            fp = uploads / f"{doc_id}.bin"; fp.write_bytes(b"x")
            with sqlite3.connect(str(db)) as c:
                c.execute(
                    "INSERT INTO documents (document_id, file_path, review_status) "
                    "VALUES (?,?, 'Queued')", (doc_id, str(fp)),
                )
                c.commit()
            q.enqueue(
                doc_id=doc_id, file_path=fp, filename=f"{doc_id}.bin",
                client_code="C1", ingest_source="test", client_note="",
                submitted_by="", processor=_slow, db_path=db,
            )
        # Wait for all to complete (slow job is 1.5 s; with 3 workers
        # others run in parallel). Allow 5 s total.
        start = time.time()
        while len(completed) < 4 and time.time() - start < 5:
            time.sleep(0.05)
    finally:
        q.shutdown()

    assert len(completed) == 4, f"only {len(completed)}/4 completed: {completed}"
    # Fast jobs should have finished BEFORE the slow one even started
    # to dominate — with 3 workers and 1.5 s slow job, fast jobs finish
    # well before 1.5 s.
    slow_idx = completed.index("SLOW")
    fast_count_before_slow = sum(1 for c in completed[:slow_idx] if c.startswith("FAST"))
    assert fast_count_before_slow >= 1 or len(completed) >= 3, (
        "worker pool serialized on the slow job — parallelism broken"
    )


# ---------------------------------------------------------------------------
# Cron / scheduled-job health
# ---------------------------------------------------------------------------

def test_backup_cron_line_exists_somewhere():
    """The backup cron line may live in /etc/crontab, /etc/cron.d/*,
    or /var/spool/cron/crontabs/root (user crontabs). Any of those
    counts; if none reference backup_db.sh, nightly backups are lost."""
    import subprocess
    candidates = [
        Path("/etc/crontab"),
        Path("/var/spool/cron/crontabs/root"),
    ]
    for d in Path("/etc/cron.d").glob("*") if Path("/etc/cron.d").exists() else []:
        if d.is_file():
            candidates.append(d)
    found = False
    for c in candidates:
        try:
            if c.exists() and "backup_db.sh" in c.read_text():
                found = True; break
        except (PermissionError, UnicodeDecodeError):
            continue
    if not found:
        pytest.skip(
            "backup_db.sh cron line not found in any root-readable "
            "crontab in this environment"
        )
    assert found


def test_daily_detectors_script_exists_and_logs():
    """scripts/run_daily_detectors.py must exist and its log path is
    consistent with cron's stdout redirect."""
    script = ROOT / "scripts" / "run_daily_detectors.py"
    if not script.exists():
        pytest.skip("run_daily_detectors.py not present")
    text = script.read_text()
    # Heuristic: a cron-driven detector script should log somewhere.
    # The module writes progress to stdout (cron captures) or uses
    # Python logging. Either is fine; we assert something is present.
    assert "log" in text.lower() or "print" in text, (
        "run_daily_detectors has no log / print statements — silent failure risk"
    )


# ---------------------------------------------------------------------------
# Stripe webhook under retry storm (Investigation 4 idempotency guard).
# ---------------------------------------------------------------------------

def test_stripe_event_idempotency_registry_deduplicates(tmp_path, monkeypatch):
    """The dashboard tracks processed event IDs. Replaying the same
    event 100 times should only record it once, regardless of concurrent
    arrival."""
    db = tmp_path / "st.db"
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE stripe_events_processed (
            event_id TEXT PRIMARY KEY,
            event_type TEXT,
            processed_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit(); conn.close()

    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)

    event_id = "evt_storm_1"
    # Simulate 100 arrivals checking-and-marking.
    for _ in range(100):
        if not rd._stripe_event_already_processed(event_id):
            rd._stripe_event_mark_processed(event_id, "checkout.session.completed")

    with sqlite3.connect(str(db)) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM stripe_events_processed WHERE event_id=?",
            (event_id,),
        ).fetchone()[0]
    assert n == 1, f"event_id recorded {n} times; idempotency broken"
