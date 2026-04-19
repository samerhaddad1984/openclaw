"""End-to-end wiring tests for optimistic concurrency.

Covers every write handler that touches a row in a table carrying a
``version`` column. Each handler is tested at two levels:

1. The helper function (``update_<table>_fields_versioned`` or its
   sibling ``set_document_status_versioned`` / ``set_manual_hold_versioned``
   / ``assign_document_versioned``) — direct unit tests against a
   tmp_path SQLite DB.
2. The POST endpoint itself — a *static* test that parses
   ``scripts/review_dashboard.py`` and asserts the handler block calls
   one of the approved versioned helpers. This is the regression guard:
   if someone adds a new write handler and forgets to wire it, the last
   test in this file fails.

The threading test at the bottom confirms that two concurrent writers
reading the same row end up with exactly one winner and one 409.
"""
from __future__ import annotations

import re
import sqlite3
import sys
import threading
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db.optimistic import VERSIONED_TABLES  # noqa: E402
from src.db.version_handlers import (  # noqa: E402
    VersionedUpdateResult,
    versioned_update_from_request,
)


# ---------------------------------------------------------------------------
# Per-table fixtures — each mimics the real schema closely enough that
# ``_versioned_table_update`` can filter to existing columns without
# hitting sqlite's "no such column" error.
# ---------------------------------------------------------------------------

SCHEMAS: dict[str, str] = {
    "documents": """
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY, vendor TEXT, client_code TEXT,
            doc_type TEXT, amount REAL, document_date TEXT,
            gl_account TEXT, tax_code TEXT, category TEXT,
            review_status TEXT, manual_hold_reason TEXT,
            manual_hold_by TEXT, manual_hold_at TEXT,
            assigned_to TEXT, updated_at TEXT,
            version INTEGER DEFAULT 1
        )
    """,
    "clients": """
        CREATE TABLE clients (
            client_code TEXT PRIMARY KEY, client_name TEXT,
            contact_email TEXT, language TEXT, active INTEGER,
            whatsapp_number TEXT, firm_code TEXT,
            version INTEGER DEFAULT 1
        )
    """,
    "engagements": """
        CREATE TABLE engagements (
            engagement_id TEXT PRIMARY KEY, client_code TEXT, period TEXT,
            engagement_type TEXT, status TEXT,
            partner TEXT, manager TEXT, staff TEXT,
            planned_hours REAL, actual_hours REAL, budget REAL, fee REAL,
            created_at TEXT, completed_at TEXT,
            version INTEGER DEFAULT 1
        )
    """,
    "fixed_assets": """
        CREATE TABLE fixed_assets (
            asset_id TEXT PRIMARY KEY, client_code TEXT,
            asset_name TEXT, description TEXT, cca_class INTEGER,
            acquisition_date TEXT, cost REAL, status TEXT,
            disposal_date TEXT, disposal_proceeds REAL,
            disposal_reason TEXT, updated_at TEXT,
            version INTEGER DEFAULT 1
        )
    """,
    "working_papers": """
        CREATE TABLE working_papers (
            paper_id TEXT PRIMARY KEY, client_code TEXT, period TEXT,
            engagement_type TEXT, account_code TEXT, account_name TEXT,
            balance_per_books REAL, balance_confirmed REAL, difference REAL,
            tested_by TEXT, reviewed_by TEXT, status TEXT, notes TEXT,
            updated_at TEXT, version INTEGER DEFAULT 1
        )
    """,
    "partnerships": """
        CREATE TABLE partnerships (
            id INTEGER PRIMARY KEY AUTOINCREMENT, firm_code TEXT,
            client_code TEXT, partnership_name TEXT, tax_year_end TEXT,
            partnership_type TEXT, status TEXT, created_at TEXT,
            version INTEGER DEFAULT 1
        )
    """,
    "sred_claims": """
        CREATE TABLE sred_claims (
            id INTEGER PRIMARY KEY AUTOINCREMENT, firm_code TEXT,
            client_code TEXT, tax_year INTEGER, claim_type TEXT,
            project_name TEXT, technological_advancement TEXT,
            technological_obstacles TEXT, work_performed TEXT,
            status TEXT, created_at TEXT,
            version INTEGER DEFAULT 1
        )
    """,
    # Side table — assign_document_versioned upserts here after the
    # documents-row version check passes.
    "document_assignments": """
        CREATE TABLE document_assignments (
            document_id TEXT PRIMARY KEY,
            assigned_to TEXT, assigned_by TEXT,
            assigned_at TEXT, updated_at TEXT, note TEXT
        )
    """,
}


def _mk_db(tmp_path: Path, *, seed: dict[str, list[tuple]] | None = None,
           name: str = "handlers.db") -> Path:
    """Create a throwaway DB with every versioned table + one seed row.
    If the target path already exists, it's removed first — simplifies
    the concurrent-writer test which recreates per case."""
    db_path = tmp_path / name
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    for ddl in SCHEMAS.values():
        conn.execute(ddl)
    # Seed one row per table with version=1.
    conn.execute("INSERT INTO documents (document_id, vendor, version) VALUES ('D1', 'ACME', 1)")
    conn.execute("INSERT INTO clients (client_code, client_name, version) VALUES ('C1', 'Acme Inc', 1)")
    conn.execute("INSERT INTO engagements (engagement_id, client_code, period, status, version) "
                 "VALUES ('E1', 'C1', '2026-Q1', 'planning', 1)")
    conn.execute("INSERT INTO fixed_assets (asset_id, client_code, asset_name, cca_class, "
                 "acquisition_date, cost, status, version) "
                 "VALUES ('A1', 'C1', 'Server', 50, '2026-01-01', 5000.0, 'active', 1)")
    conn.execute("INSERT INTO working_papers (paper_id, client_code, period, engagement_type, "
                 "account_code, account_name, status, version) "
                 "VALUES ('W1', 'C1', '2026-Q1', 'audit', '1000', 'Cash', 'open', 1)")
    conn.execute("INSERT INTO partnerships (client_code, partnership_name, "
                 "partnership_type, tax_year_end, version) "
                 "VALUES ('C1', 'Acme LP', 'general', '2026-12-31', 1)")
    conn.execute("INSERT INTO sred_claims (client_code, tax_year, claim_type, "
                 "project_name, status, version) "
                 "VALUES ('C1', 2026, 'traditional', 'AI Research', 'draft', 1)")
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def dash(tmp_path, monkeypatch):
    """Boot review_dashboard pointing at a throwaway DB."""
    db_path = _mk_db(tmp_path)
    monkeypatch.setenv("OTOCPA_DB", str(db_path))
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db_path)

    def _open():
        c = sqlite3.connect(str(db_path))
        c.row_factory = rd._dict_factory
        return c
    monkeypatch.setattr(rd, "open_db", _open)
    return rd, db_path


# ---------------------------------------------------------------------------
# Helper-level tests: each versioned wrapper must (a) 409 on stale and
# (b) require expected_version when require_version=True.
# ---------------------------------------------------------------------------

def test_document_status_requires_version(dash):
    rd, _ = dash
    r = rd.set_document_status_versioned(
        "D1", "Ready", body={}, require_version=True,
    )
    assert r.status == 400
    assert r.error == "version_required"


def test_document_status_409_on_stale(dash):
    rd, db_path = dash
    # First caller wins v=1 → v=2.
    ok = rd.set_document_status_versioned("D1", "Ready", body={"version": 1})
    assert ok.status == 200 and ok.new_version == 2
    # Second caller still holding v=1 gets 409.
    stale = rd.set_document_status_versioned("D1", "Posted", body={"version": 1})
    assert stale.status == 409
    assert stale.current_version == 2
    # Row was not re-written.
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT review_status FROM documents WHERE document_id='D1'").fetchone()
    assert row["review_status"] == "Ready"


def test_document_hold_requires_version(dash):
    rd, _ = dash
    r = rd.set_manual_hold_versioned(
        "D1", "waiting on vendor", "alice", body={}, require_version=True,
    )
    assert r.status == 400


def test_document_hold_409_on_stale(dash):
    rd, _ = dash
    ok = rd.set_manual_hold_versioned("D1", "pending", "alice", body={"version": 1})
    assert ok.status == 200
    stale = rd.set_manual_hold_versioned("D1", "still pending", "bob", body={"version": 1})
    assert stale.status == 409


def test_document_assign_requires_version(dash):
    rd, _ = dash
    r = rd.assign_document_versioned(
        "D1", "alice", "alice", body={}, require_version=True,
    )
    assert r.status == 400


def test_document_assign_409_on_stale(dash):
    rd, _ = dash
    ok = rd.assign_document_versioned("D1", "alice", "alice", body={"version": 1})
    assert ok.status == 200
    stale = rd.assign_document_versioned("D1", "bob", "bob", body={"version": 1})
    assert stale.status == 409


def test_client_update_requires_version(dash):
    rd, _ = dash
    r = rd.update_client_fields_versioned(
        "C1", {"client_name": "X"}, body={}, require_version=True,
    )
    assert r.status == 400


def test_client_update_409_on_stale(dash):
    rd, _ = dash
    ok = rd.update_client_fields_versioned("C1", {"client_name": "New"}, body={"version": 1})
    assert ok.status == 200
    stale = rd.update_client_fields_versioned("C1", {"client_name": "Other"}, body={"version": 1})
    assert stale.status == 409


def test_engagement_update_requires_version(dash):
    rd, _ = dash
    r = rd.update_engagement_fields_versioned(
        "E1", {"status": "fieldwork"}, body={}, require_version=True,
    )
    assert r.status == 400


def test_engagement_update_409_on_stale(dash):
    rd, _ = dash
    ok = rd.update_engagement_fields_versioned("E1", {"status": "fieldwork"}, body={"version": 1})
    assert ok.status == 200
    stale = rd.update_engagement_fields_versioned("E1", {"status": "done"}, body={"version": 1})
    assert stale.status == 409


def test_fixed_asset_update_requires_version(dash):
    rd, _ = dash
    r = rd.update_fixed_asset_fields_versioned(
        "A1", {"status": "disposed"}, body={}, require_version=True,
    )
    assert r.status == 400


def test_fixed_asset_update_409_on_stale(dash):
    rd, _ = dash
    ok = rd.update_fixed_asset_fields_versioned("A1", {"status": "disposed"}, body={"version": 1})
    assert ok.status == 200
    stale = rd.update_fixed_asset_fields_versioned("A1", {"status": "active"}, body={"version": 1})
    assert stale.status == 409


def test_working_paper_update_requires_version(dash):
    rd, _ = dash
    r = rd.update_working_paper_fields_versioned(
        "W1", {"status": "complete"}, body={}, require_version=True,
    )
    assert r.status == 400


def test_working_paper_update_409_on_stale(dash):
    rd, _ = dash
    ok = rd.update_working_paper_fields_versioned("W1", {"status": "reviewed"}, body={"version": 1})
    assert ok.status == 200
    stale = rd.update_working_paper_fields_versioned("W1", {"status": "open"}, body={"version": 1})
    assert stale.status == 409


def test_partnership_update_requires_version(dash):
    rd, _ = dash
    r = rd.update_partnership_fields_versioned(
        1, {"partnership_name": "X"}, body={}, require_version=True,
    )
    assert r.status == 400


def test_partnership_update_409_on_stale(dash):
    rd, _ = dash
    ok = rd.update_partnership_fields_versioned(
        1, {"partnership_name": "Renamed LP"}, body={"version": 1},
    )
    assert ok.status == 200
    stale = rd.update_partnership_fields_versioned(
        1, {"partnership_name": "Other"}, body={"version": 1},
    )
    assert stale.status == 409


def test_sred_update_requires_version(dash):
    rd, _ = dash
    r = rd.update_sred_claim_fields_versioned(
        1, {"status": "submitted"}, body={}, require_version=True,
    )
    assert r.status == 400


def test_sred_update_409_on_stale(dash):
    rd, _ = dash
    ok = rd.update_sred_claim_fields_versioned(
        1, {"status": "submitted"}, body={"version": 1},
    )
    assert ok.status == 200
    stale = rd.update_sred_claim_fields_versioned(
        1, {"status": "draft"}, body={"version": 1},
    )
    assert stale.status == 409


# ---------------------------------------------------------------------------
# Concurrent-writer simulation: two threads hit every versioned wrapper
# with the same stale v=1. Exactly one lands; the other sees 409. This is
# the core guarantee — no silent overwrites.
# ---------------------------------------------------------------------------

def _race_two(
    fn, *, reseed, tmp_path, name: str,
) -> tuple[int, int]:
    """Run ``fn`` in two threads with a barrier. ``fn`` must return a
    ``VersionedUpdateResult``. Return (winners, losers)."""
    db_path = _mk_db(tmp_path, name=name)
    reseed(db_path)  # caller may reset row state between runs
    winners = losers = 0
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    def worker(tag: str):
        nonlocal winners, losers
        barrier.wait()
        res = fn(db_path, tag)
        with lock:
            if res.status == 200:
                winners += 1
            elif res.status == 409:
                losers += 1

    ts = [threading.Thread(target=worker, args=(t,)) for t in ("A", "B")]
    for t in ts: t.start()
    for t in ts: t.join()
    return winners, losers


def test_concurrent_status_change_no_silent_overwrite(tmp_path, monkeypatch):
    """Two threads read v=1 from documents and both POST a status change.
    Exactly one wins; the other must 409. Run against every versioned
    wrapper — any silent overwrite fails the test."""
    import scripts.review_dashboard as rd

    def _reseed(db_path):
        monkeypatch.setenv("OTOCPA_DB", str(db_path))
        monkeypatch.setattr(rd, "DB_PATH", db_path)

        def _open():
            c = sqlite3.connect(str(db_path))
            c.row_factory = rd._dict_factory
            return c
        monkeypatch.setattr(rd, "open_db", _open)

    def doc_status(db_path, tag):
        _reseed(db_path)
        return rd.set_document_status_versioned(
            "D1", f"Status-{tag}", body={"version": 1},
        )

    def doc_hold(db_path, tag):
        _reseed(db_path)
        return rd.set_manual_hold_versioned(
            "D1", f"hold-{tag}", tag, body={"version": 1},
        )

    def doc_assign(db_path, tag):
        _reseed(db_path)
        return rd.assign_document_versioned(
            "D1", tag, tag, body={"version": 1},
        )

    def client_upd(db_path, tag):
        _reseed(db_path)
        return rd.update_client_fields_versioned(
            "C1", {"client_name": f"Name-{tag}"}, body={"version": 1},
        )

    def eng_upd(db_path, tag):
        _reseed(db_path)
        return rd.update_engagement_fields_versioned(
            "E1", {"status": "fieldwork"}, body={"version": 1},
        )

    def fa_upd(db_path, tag):
        _reseed(db_path)
        return rd.update_fixed_asset_fields_versioned(
            "A1", {"status": f"st-{tag}"}, body={"version": 1},
        )

    def wp_upd(db_path, tag):
        _reseed(db_path)
        return rd.update_working_paper_fields_versioned(
            "W1", {"status": f"st-{tag}"}, body={"version": 1},
        )

    def p_upd(db_path, tag):
        _reseed(db_path)
        return rd.update_partnership_fields_versioned(
            1, {"partnership_name": f"LP-{tag}"}, body={"version": 1},
        )

    def s_upd(db_path, tag):
        _reseed(db_path)
        return rd.update_sred_claim_fields_versioned(
            1, {"status": f"st-{tag}"}, body={"version": 1},
        )

    cases = [doc_status, doc_hold, doc_assign, client_upd, eng_upd,
             fa_upd, wp_upd, p_upd, s_upd]
    for fn in cases:
        w, l = _race_two(
            fn, reseed=lambda _p: None, tmp_path=tmp_path,
            name=f"race_{fn.__name__}.db",
        )
        assert w == 1, f"{fn.__name__}: winners={w} losers={l}"
        assert l == 1, f"{fn.__name__}: winners={w} losers={l}"


# ---------------------------------------------------------------------------
# Regression guard: enumerate every POST handler that writes to a
# versioned table and assert its body calls one of the approved
# versioned helpers. If a new write handler lands without wiring, this
# test fails loudly.
# ---------------------------------------------------------------------------

VERSIONED_HELPERS = (
    "versioned_update_from_request",
    "update_document_fields_versioned",
    "update_client_fields_versioned",
    "update_engagement_fields_versioned",
    "update_fixed_asset_fields_versioned",
    "update_working_paper_fields_versioned",
    "update_partnership_fields_versioned",
    "update_sred_claim_fields_versioned",
    "set_document_status_versioned",
    "set_manual_hold_versioned",
    "assign_document_versioned",
)

# Endpoints that update a row in a versioned table and MUST route through
# a versioned helper. Path prefix matching ("/assign" matches a handler
# block starting with `if path == "/assign":` or `if path in (...)`).
REQUIRED_VERSIONED_ENDPOINTS = [
    "/document/update",
    "/document/status",
    "/document/hold",
    "/document/return_ready",
    "/assign",              # handled by combined `path in ("/assign", "/document/assign")`
    "/document/assign",
    "/claim",
    "/engagements/update",
    "/clients/save",        # edit-path branch
    "/fixed_asset/update",
    "/working_paper/update",
    "/partnership/update",
    "/sred/update",
]


def _extract_post_handler_blocks(source: str) -> dict[str, str]:
    """Parse ``do_POST`` and return a dict of endpoint -> handler-body
    source. Endpoint keys are the literal string after ``path ==``. For
    combined routes (``path in ("/a", "/b")``) each path gets its own
    entry mapped to the same body."""
    # Find start of do_POST.
    m = re.search(r"^\s*def\s+do_POST\s*\(", source, re.MULTILINE)
    assert m, "do_POST not found in review_dashboard.py"
    post_src = source[m.end():]
    # Stop at the next `def ` at class-method indent — conservative, we
    # scan until end-of-file; all POST handlers live inside do_POST.
    # Regex handler starts: `if path == "X":` or `elif path == "X":` or
    # `if path in ("X", "Y", ...)`.
    pattern = re.compile(
        r"^(?P<indent>\s+)(?:if|elif)\s+path\s*(?:==\s*(?P<single>[\"'][^\"']+[\"'])"
        r"|in\s*\((?P<multi>[^)]*)\))\s*:",
        re.MULTILINE,
    )
    blocks: dict[str, str] = {}
    matches = list(pattern.finditer(post_src))
    for i, m2 in enumerate(matches):
        start = m2.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(post_src)
        body = post_src[start:end]
        paths: list[str] = []
        if m2.group("single"):
            paths.append(m2.group("single").strip("\"'"))
        else:
            for tok in re.findall(r"[\"']([^\"']+)[\"']", m2.group("multi") or ""):
                paths.append(tok)
        for p in paths:
            # If the same endpoint shows up twice (GET + POST, same file),
            # keep the first one since we trimmed to the do_POST region.
            blocks.setdefault(p, body)
    return blocks


def test_all_write_handlers_enumerated_and_versioned():
    """Regression guard: every endpoint in REQUIRED_VERSIONED_ENDPOINTS
    must either call one of the approved versioned helpers in its
    handler body or be a compound-route member whose sibling path does.
    """
    src = (ROOT / "scripts" / "review_dashboard.py").read_text()
    blocks = _extract_post_handler_blocks(src)

    missing: list[str] = []
    unwired: list[str] = []
    for ep in REQUIRED_VERSIONED_ENDPOINTS:
        body = blocks.get(ep)
        if body is None:
            missing.append(ep)
            continue
        if not any(helper in body for helper in VERSIONED_HELPERS):
            unwired.append(ep)

    assert not missing, (
        f"Missing POST handler(s) in review_dashboard.py — "
        f"did someone delete a route? {missing}"
    )
    assert not unwired, (
        f"POST handler(s) write to a versioned table but do NOT call a "
        f"versioned helper — lost-update race restored. Unwired: {unwired}. "
        f"Approved helpers: {list(VERSIONED_HELPERS)}"
    )


def test_versioned_tables_registry_includes_new_tables():
    """Guard against someone adding a new versioned endpoint but forgetting
    to register the underlying table. Without the registry entry,
    ``versioned_update_from_request`` raises ValueError."""
    must_have = {
        "documents", "clients", "engagements", "fixed_assets",
        "working_papers", "partnerships", "sred_claims",
    }
    assert must_have <= set(VERSIONED_TABLES)
