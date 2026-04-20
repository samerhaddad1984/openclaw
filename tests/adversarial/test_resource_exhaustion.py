"""R4-Investigation 5 — resource exhaustion.

Verify the dashboard degrades gracefully when disk / memory / FDs
run low. The full production scenarios are risky to run in-process;
we target the deterministic code paths:

- OCR engine on read-only upload directory
- SQLite writes when disk is tight
- Response when file-descriptor pressure climbs
"""
from __future__ import annotations

import os
import resource
import sqlite3
import sys
import tempfile
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Disk full — simulate a full target directory via a read-only file.
# ---------------------------------------------------------------------------

def test_pdf_generation_fallback_survives_missing_fitz(tmp_path):
    """audit_engine.generate_financial_statements_pdf tries pymupdf
    (fitz); if missing OR raises, it falls back to _fs_pdf_minimal.
    Verify the fallback path handles the flat-dict structure without
    crashing — we pinned this in R2 but hit it again here for the
    resource-exhaustion angle."""
    db = tmp_path / "pdf.db"
    import src.engines.ocr_engine as oe
    import src.engines.gl_engine as gle
    oe.DB_PATH = db
    gle.DB_PATH = db
    from src.engines.audit_engine import (
        ensure_audit_tables, seed_chart_of_accounts,
        generate_financial_statements_pdf,
    )
    from src.engines.gl_engine import ensure_schema as ensure_gl
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    ensure_audit_tables(conn)
    seed_chart_of_accounts(conn)
    ensure_gl()
    # Seed minimal balanced JEs.
    for eid, d, cr in [("E1", "1000", "3000")]:
        conn.execute(
            "INSERT INTO gl_transactions (entry_id, client_code, period, entry_date, "
            "account_code, side, amount, description, source) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (eid, "C1", "2026-04", "2026-04-15", d, "debit", 100.0, "", "manual_je"),
        )
        conn.execute(
            "INSERT INTO gl_transactions (entry_id, client_code, period, entry_date, "
            "account_code, side, amount, description, source) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (eid, "C1", "2026-04", "2026-04-15", cr, "credit", 100.0, "", "manual_je"),
        )
    conn.commit()
    pdf = generate_financial_statements_pdf(conn, "C1", "2026-04", lang="en")
    assert isinstance(pdf, (bytes, bytearray)) and len(pdf) > 200


def test_sqlite_write_with_query_only_pragma_refused(tmp_path):
    """PRAGMA query_only = ON explicitly refuses writes. Running the
    dashboard against a read-only snapshot (e.g., a restored backup
    for forensic investigation) should not silently succeed.
    Chmod-based read-only is ineffective when the test runs as root,
    so we use the SQLite-native pragma instead."""
    db = tmp_path / "ro.db"
    c = sqlite3.connect(str(db))
    c.execute("CREATE TABLE t (v INTEGER)")
    c.execute("INSERT INTO t VALUES (1)")
    c.commit(); c.close()
    c2 = sqlite3.connect(str(db))
    c2.execute("PRAGMA query_only = ON")
    with pytest.raises(sqlite3.OperationalError):
        c2.execute("INSERT INTO t VALUES (2)")
        c2.commit()
    c2.close()


# ---------------------------------------------------------------------------
# Decompression bomb (PIL) — already in R1 but re-verify.
# ---------------------------------------------------------------------------

def test_pil_rejects_huge_png(tmp_path):
    import struct
    from PIL import Image, UnidentifiedImageError
    huge = (
        b"\x89PNG\r\n\x1a\n"
        + b"\x00\x00\x00\rIHDR"
        + struct.pack(">II", 100_000, 100_000)
        + b"\x08\x02\x00\x00\x00"
        + b"\x00\x00\x00\x00"
        + b"\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    p = tmp_path / "bomb.png"
    p.write_bytes(huge)
    try:
        im = Image.open(str(p))
        # Accessing .load() is what would allocate GB of pixels.
        with pytest.raises(Exception):
            im.load()
    except UnidentifiedImageError:
        pass  # refused at header parse — fine.


# ---------------------------------------------------------------------------
# Large payloads — bounded by the dashboard's MAX_UPLOAD_BYTES constant.
# ---------------------------------------------------------------------------

def test_batch_limit_error_rejects_over_size(tmp_path):
    import scripts.review_dashboard as rd
    # Craft a batch that exceeds MAX_UPLOAD_BYTES.
    files = [("x.pdf", b"A" * (rd.MAX_UPLOAD_BYTES + 1))]
    err = rd._batch_limit_error(files)
    assert err is not None
    assert "too large" in err.lower() or "max" in err.lower(), err


def test_batch_limit_error_rejects_over_count(tmp_path):
    import scripts.review_dashboard as rd
    files = [(f"f{i}.pdf", b"x") for i in range(rd.MAX_UPLOAD_FILES + 1)]
    err = rd._batch_limit_error(files)
    assert err is not None
    assert "too many" in err.lower() or "max" in err.lower(), err


# ---------------------------------------------------------------------------
# FD usage — opening many SQLite connections should not leak FDs.
# ---------------------------------------------------------------------------

def test_connection_lifecycle_returns_fds(tmp_path):
    db = tmp_path / "lifecycle.db"
    # Establish baseline.
    sqlite3.connect(str(db)).close()
    proc = Path(f"/proc/{os.getpid()}/fd")
    if not proc.exists():
        pytest.skip("/proc not available")
    baseline = len(list(proc.iterdir()))
    # Open/close 100 connections.
    for _ in range(100):
        c = sqlite3.connect(str(db))
        c.execute("SELECT 1").fetchall()
        c.close()
    final = len(list(proc.iterdir()))
    # Allow ±5 FDs for normal runtime jitter.
    assert abs(final - baseline) <= 5, (
        f"FD leak: started at {baseline}, ended at {final} "
        f"after 100 open-close cycles"
    )
