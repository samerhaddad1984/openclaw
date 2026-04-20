"""R4-Investigation 10 — audit trail completeness + tamper surface.

The audit trail is the CPA firm's evidence of what happened. Missing
entries, missing metadata (who/when), or post-write mutability are
SOC-relevant findings.

What the dashboard has today:
- login_attempts (failed + successful auth)
- client_portal_access (client opening /c/<token> pages)
- ai_usage (processing costs per doc)
- document_corrections (field-level change log)
- manual_journal_entries (status tracked via status column)
- stripe_events_processed (webhook idempotency registry)

This file audits each of those for completeness + retention.
"""
from __future__ import annotations

import sqlite3
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _bootstrap(db: Path) -> None:
    c = sqlite3.connect(str(db))
    c.executescript("""
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT,
            active INTEGER DEFAULT 1, version INTEGER DEFAULT 1,
            portal_token TEXT, portal_token_created_at TEXT);
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, firm_code TEXT);
    """)
    c.commit(); c.close()


@pytest.fixture
def audit_app(tmp_path, monkeypatch):
    db = tmp_path / "audit.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    rd._portal_token_log.clear(); rd._portal_ip_log.clear()
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "au@det.com", "customer": "cus_AU",
             "subscription": "sub_AU", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?", (rd.hash_password("Au1Pw!"), "au@det.com"),
        )
        conn.commit()
    server = ThreadingHTTPServer(("127.0.0.1", 0), rd.ReviewDashboardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield {"base": f"http://127.0.0.1:{port}", "rd": rd, "db": db}
    finally:
        server.shutdown(); server.server_close()


# ---------------------------------------------------------------------------
# Login audit
# ---------------------------------------------------------------------------

def test_every_login_attempt_captures_ip_username_and_success(audit_app):
    base = audit_app["base"]
    p = urllib.parse.urlparse(base)
    hdrs = {"Content-Type": "application/x-www-form-urlencoded",
            "Origin": f"{p.scheme}://{p.netloc}"}
    # Failed login.
    body_fail = urllib.parse.urlencode({
        "username": "wrong@det.com", "password": "nope",
    }).encode()
    try:
        urllib.request.urlopen(
            urllib.request.Request(f"{base}/login", data=body_fail,
                                     method="POST", headers=hdrs),
            timeout=10)
    except urllib.error.HTTPError:
        pass
    with sqlite3.connect(str(audit_app["db"])) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT username, success, ip_address, attempted_at "
            "FROM login_attempts WHERE username='wrong@det.com'",
        ).fetchall()
    assert rows, "failed login not audited"
    r = dict(rows[0])
    assert int(r["success"]) == 0
    assert r["ip_address"], "login audit missing IP"
    assert r["attempted_at"], "login audit missing timestamp"


# ---------------------------------------------------------------------------
# Portal access audit
# ---------------------------------------------------------------------------

def test_every_portal_access_audited_with_action(audit_app):
    rd = audit_app["rd"]
    tok = rd.generate_portal_token()
    with sqlite3.connect(str(audit_app["db"])) as conn:
        conn.execute(
            "INSERT INTO clients (client_code, firm_code, active, "
            "portal_token, portal_token_created_at) "
            "VALUES ('P1', 'OWNER', 1, ?, datetime('now'))", (tok,),
        )
        conn.commit()
    # Hit three different portal pages.
    for path in ("", "/documents", "/messages"):
        urllib.request.urlopen(
            urllib.request.Request(f"{audit_app['base']}/c/{tok}{path}"),
            timeout=10,
        ).read()
    with sqlite3.connect(str(audit_app["db"])) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT action, ip, user_agent FROM client_portal_access "
            "WHERE client_code='P1' ORDER BY id",
        ).fetchall()
    actions = [r["action"] for r in rows]
    # Should have one entry per page view, with view_* actions.
    assert len(actions) >= 3, (
        f"expected >=3 portal audit rows, got {len(actions)}: {actions}"
    )
    for a in actions:
        assert a.startswith("view_"), f"unexpected action: {a}"


# ---------------------------------------------------------------------------
# JE posting audit — the entry itself captures prepared_by + timestamp.
# ---------------------------------------------------------------------------

def test_je_post_records_prepared_by_and_timestamp(tmp_path, monkeypatch):
    db = tmp_path / "je.db"
    import src.engines.ocr_engine as oe
    import src.engines.gl_engine as gle
    monkeypatch.setattr(oe, "DB_PATH", db)
    monkeypatch.setattr(gle, "DB_PATH", db)
    from src.engines.audit_engine import ensure_audit_tables, seed_chart_of_accounts
    from src.engines.gl_engine import ensure_schema as ensure_gl
    import sqlite3 as _s
    conn = _s.connect(str(db))
    conn.row_factory = _s.Row
    ensure_audit_tables(conn); seed_chart_of_accounts(conn); ensure_gl()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS manual_journal_entries (
            entry_id TEXT PRIMARY KEY,
            client_code TEXT, period TEXT, entry_date TEXT,
            prepared_by TEXT,
            debit_account TEXT, credit_account TEXT,
            amount REAL, description TEXT, document_id TEXT,
            source TEXT DEFAULT 'manual_je',
            status TEXT DEFAULT 'draft',
            created_at TEXT, updated_at TEXT
        );
        INSERT INTO manual_journal_entries
        (entry_id, client_code, period, entry_date, prepared_by,
         debit_account, credit_account, amount, status, created_at, updated_at)
        VALUES ('JA', 'C1', '2026-04', '2026-04-15', 'cpa@det.com',
                '6000', '1000', 50.0, 'draft',
                datetime('now'), datetime('now'));
    """)
    conn.commit()
    from src.engines.gl_engine import post_journal_entry
    post_journal_entry("JA")
    row = conn.execute(
        "SELECT prepared_by, created_at, updated_at, status "
        "FROM manual_journal_entries WHERE entry_id='JA'",
    ).fetchone()
    assert row["prepared_by"] == "cpa@det.com", (
        "prepared_by missing from JE audit — who posted?"
    )
    assert row["created_at"]
    assert row["updated_at"]
    assert row["status"] == "posted"
    conn.close()


# ---------------------------------------------------------------------------
# Reversal preserves original audit trail.
# ---------------------------------------------------------------------------

def test_reverse_does_not_delete_original_gl_rows(tmp_path, monkeypatch):
    db = tmp_path / "rev.db"
    import src.engines.ocr_engine as oe
    import src.engines.gl_engine as gle
    monkeypatch.setattr(oe, "DB_PATH", db)
    monkeypatch.setattr(gle, "DB_PATH", db)
    from src.engines.audit_engine import ensure_audit_tables, seed_chart_of_accounts
    from src.engines.gl_engine import ensure_schema as ensure_gl
    import sqlite3 as _s
    conn = _s.connect(str(db))
    conn.row_factory = _s.Row
    ensure_audit_tables(conn); seed_chart_of_accounts(conn); ensure_gl()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS manual_journal_entries (
            entry_id TEXT PRIMARY KEY,
            client_code TEXT, period TEXT, entry_date TEXT,
            prepared_by TEXT,
            debit_account TEXT, credit_account TEXT,
            amount REAL, description TEXT, document_id TEXT,
            source TEXT DEFAULT 'manual_je',
            status TEXT DEFAULT 'draft',
            created_at TEXT, updated_at TEXT
        );
        INSERT INTO manual_journal_entries
        (entry_id, client_code, period, entry_date, prepared_by,
         debit_account, credit_account, amount, status, created_at, updated_at)
        VALUES ('JR', 'C1', '2026-04', '2026-04-15', 'cpa',
                '6000', '1000', 100.0, 'draft',
                datetime('now'), datetime('now'));
    """)
    conn.commit()
    from src.engines.gl_engine import post_journal_entry, reverse_journal_entry
    post_journal_entry("JR")
    reverse_journal_entry("JR")
    # Both original (manual_je) and reversal (manual_je_reversal) rows
    # must coexist: audit trail is append-only.
    sources = [
        r["source"] for r in conn.execute(
            "SELECT source FROM gl_transactions WHERE entry_id='JR'",
        ).fetchall()
    ]
    assert "manual_je" in sources, (
        "reverse deleted original GL rows — audit trail lost"
    )
    assert "manual_je_reversal" in sources
    conn.close()


# ---------------------------------------------------------------------------
# Stripe event registry is write-once per event_id.
# ---------------------------------------------------------------------------

def test_stripe_event_registry_write_once(tmp_path, monkeypatch):
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
    rd._stripe_event_mark_processed("evt_X", "checkout.session.completed")
    # Re-marking should be idempotent (INSERT OR IGNORE).
    rd._stripe_event_mark_processed("evt_X", "checkout.session.completed")
    with sqlite3.connect(str(db)) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM stripe_events_processed WHERE event_id='evt_X'",
        ).fetchone()[0]
    assert n == 1


# ---------------------------------------------------------------------------
# password_reset_used is append-only (INSERT OR IGNORE).
# ---------------------------------------------------------------------------

def test_password_reset_registry_is_append_only(audit_app):
    rd = audit_app["rd"]
    # Mark same token-hash twice; only one row should exist.
    rd._password_reset_token_mark_used("x" * 32, "au@det.com")
    rd._password_reset_token_mark_used("x" * 32, "au@det.com")
    with sqlite3.connect(str(audit_app["db"])) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM password_reset_used",
        ).fetchone()[0]
    assert n == 1
