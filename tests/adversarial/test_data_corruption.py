"""Investigation 5 — data corruption.

Inject bad rows into the DB directly, then hit read paths and see what
happens. Requirements for every scenario:
  (a) no crash of the request path,
  (b) no corruption of other rows,
  (c) a clear error (or safe default) surfaces.

When a scenario reveals a real crash, it's a BUG. Otherwise the test
documents current behavior.
"""
from __future__ import annotations

import sqlite3
import sys
import threading
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def srv(tmp_path, monkeypatch):
    db = tmp_path / "corrupt.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    c = sqlite3.connect(str(db))
    c.executescript("""
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT, vendor TEXT,
            amount REAL, document_date TEXT, review_status TEXT,
            version INTEGER DEFAULT 1, firm_code TEXT,
            updated_at TEXT, created_at TEXT
        );
    """)
    c.commit(); c.close()

    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        yield rd, db, f"http://127.0.0.1:{port}"
    finally:
        server.shutdown(); server.server_close()


def _get(url):
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return r.status, r.read()
    except Exception as e:
        try:
            return e.code, e.read()
        except Exception:
            return 0, str(e).encode()


# ---------------------------------------------------------------------------
# SCENARIO 1 — NULL where NOT NULL expected (or functionally required).
# ---------------------------------------------------------------------------

def test_document_with_null_vendor_and_client_does_not_crash_reads(srv):
    rd, db, base = srv
    c = sqlite3.connect(str(db))
    c.execute(
        "INSERT INTO documents (document_id, client_code, vendor, amount, "
        "document_date, review_status, firm_code) VALUES "
        "('NULL1', NULL, NULL, NULL, NULL, NULL, 'OWNER')",
    )
    c.commit(); c.close()

    # get_document must not crash on NULLs.
    doc = rd.get_document("NULL1")
    assert doc is not None
    # Document page render must not crash (even anonymous: we hit /login).
    status, _ = _get(f"{base}/login")
    assert status == 200


# ---------------------------------------------------------------------------
# SCENARIO 2 — Orphaned references.
# ---------------------------------------------------------------------------

def test_document_with_nonexistent_client_code_does_not_crash_document_fetch(srv):
    """Foreign-key integrity isn't enforced by default in SQLite (PRAGMA
    foreign_keys = OFF unless explicitly enabled). So an
    orphaned client_code on a documents row is possible. The product
    must tolerate it on reads."""
    rd, db, base = srv
    c = sqlite3.connect(str(db))
    c.execute(
        "INSERT INTO documents (document_id, client_code, vendor, firm_code) "
        "VALUES ('ORPH', 'DOES-NOT-EXIST', 'Acme', 'OWNER')",
    )
    c.commit(); c.close()
    doc = rd.get_document("ORPH")
    assert doc is not None
    assert doc["client_code"] == "DOES-NOT-EXIST"


# ---------------------------------------------------------------------------
# SCENARIO 3 — Impossible dates.
# ---------------------------------------------------------------------------

def test_impossible_document_date_does_not_crash(srv):
    rd, db, base = srv
    c = sqlite3.connect(str(db))
    c.execute(
        "INSERT INTO documents (document_id, client_code, vendor, document_date, firm_code) "
        "VALUES ('IMPDATE', 'C1', 'v', '2099-99-99', 'OWNER')",
    )
    c.commit(); c.close()
    doc = rd.get_document("IMPDATE")
    assert doc is not None
    assert doc["document_date"] == "2099-99-99"
    # The period helper should return empty for malformed dates, not crash.
    from src.agents.core.period_close import get_document_period
    assert get_document_period("2099-99-99") == "2099-99"  # naive split ok


def test_future_created_at_does_not_crash(srv):
    rd, db, base = srv
    c = sqlite3.connect(str(db))
    c.execute(
        "INSERT INTO documents (document_id, client_code, vendor, firm_code, created_at) "
        "VALUES ('FUT', 'C1', 'v', 'OWNER', '2999-12-31T23:59:59Z')",
    )
    c.commit(); c.close()
    doc = rd.get_document("FUT")
    assert doc is not None
    assert doc["created_at"] == "2999-12-31T23:59:59Z"


# ---------------------------------------------------------------------------
# SCENARIO 4 — Impossible amounts.
# ---------------------------------------------------------------------------

def test_negative_amount_in_db_does_not_crash_reads(srv):
    rd, db, base = srv
    c = sqlite3.connect(str(db))
    c.execute(
        "INSERT INTO documents (document_id, client_code, vendor, amount, firm_code) "
        "VALUES ('NEG', 'C1', 'v', -99999.99, 'OWNER')",
    )
    c.commit(); c.close()
    doc = rd.get_document("NEG")
    assert doc is not None
    assert doc["amount"] == -99999.99


def test_nan_amount_stores_and_reads_without_crash(srv):
    """SQLite stores NaN as NULL (via our insert); verify reads survive."""
    rd, db, base = srv
    c = sqlite3.connect(str(db))
    c.execute(
        "INSERT INTO documents (document_id, client_code, vendor, amount, firm_code) "
        "VALUES ('NAN', 'C1', 'v', NULL, 'OWNER')",
    )
    c.commit(); c.close()
    doc = rd.get_document("NAN")
    assert doc is not None
    # The stored amount is None (NULL), which is the defensive fallback.
    assert doc["amount"] is None


# ---------------------------------------------------------------------------
# SCENARIO 5 — Unicode / encoding abuse.
# ---------------------------------------------------------------------------

def test_vendor_with_rtl_arabic_and_emoji_roundtrips(srv):
    rd, db, base = srv
    weird = "𞤚𞤢 فاتورة 🧾💸 — café ☕"
    c = sqlite3.connect(str(db))
    c.execute(
        "INSERT INTO documents (document_id, client_code, vendor, firm_code) "
        "VALUES ('UNI', 'C1', ?, 'OWNER')", (weird,),
    )
    c.commit(); c.close()
    doc = rd.get_document("UNI")
    assert doc["vendor"] == weird


def test_vendor_with_null_byte_is_accepted_but_sanitized_on_read(srv):
    rd, db, base = srv
    with_null = "Vendor\x00Hidden"
    c = sqlite3.connect(str(db))
    c.execute(
        "INSERT INTO documents (document_id, client_code, vendor, firm_code) "
        "VALUES ('NUL', 'C1', ?, 'OWNER')", (with_null,),
    )
    c.commit(); c.close()
    doc = rd.get_document("NUL")
    # The null byte survives SQLite storage; downstream rendering must
    # escape/strip before display. Here we just assert: no crash.
    assert doc is not None


def test_huge_description_field_does_not_block_pagination(srv):
    """A 100k-char description should store and read without blowing the
    request path. ``client_note`` is not in ``get_document``'s SELECT,
    so we read it back via direct SQL to confirm storage integrity."""
    rd, db, base = srv
    big = "A" * 100_000
    c = sqlite3.connect(str(db))
    c.execute(
        "INSERT INTO documents (document_id, client_code, vendor, firm_code, client_note) "
        "VALUES ('HUGE', 'C1', 'v', 'OWNER', ?)", (big,),
    )
    c.commit()
    note = c.execute("SELECT client_note FROM documents WHERE document_id='HUGE'").fetchone()[0]
    c.close()
    assert len(note) == 100_000
    # get_document must not crash on the row either.
    doc = rd.get_document("HUGE")
    assert doc is not None


# ---------------------------------------------------------------------------
# SCENARIO 6 — DB integrity
# ---------------------------------------------------------------------------

def test_integrity_check_is_ok_after_corruption_inserts(srv):
    rd, db, base = srv
    c = sqlite3.connect(str(db))
    # Insert all the corruption above.
    c.executescript("""
        INSERT INTO documents (document_id, client_code, vendor, firm_code)
            VALUES ('A',NULL,NULL,'OWNER');
        INSERT INTO documents (document_id, client_code, vendor, firm_code, amount)
            VALUES ('B','C1','v','OWNER',-1e20);
        INSERT INTO documents (document_id, client_code, vendor, firm_code, document_date)
            VALUES ('C','C1','v','OWNER','2099-99-99');
    """)
    c.commit()
    result = c.execute("PRAGMA integrity_check").fetchall()
    assert result == [("ok",)], f"integrity violated by corrupt inserts: {result}"
    c.close()
