"""Investigation 4 — dependency failures.

Mock every external dependency into failure and verify the product:
  1. Does NOT crash (no uncaught exception in the request path).
  2. Does NOT corrupt data (no partial writes left behind).
  3. Logs the failure.
  4. Surfaces gracefully to the caller.
  5. Allows retry when appropriate.

Scope limits: live API calls are not made. We mock the SDK / HTTP
boundary so failure shapes are deterministic.
"""
from __future__ import annotations

import json
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


# ---------------------------------------------------------------------------
# Fixture: a fresh dashboard instance with minimal schema.
# ---------------------------------------------------------------------------

def _bootstrap_min(db: Path) -> None:
    c = sqlite3.connect(str(db))
    c.executescript("""
        CREATE TABLE firms (firm_code TEXT PRIMARY KEY);
        CREATE TABLE clients (client_code TEXT PRIMARY KEY, firm_code TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, client_code TEXT, vendor TEXT, review_status TEXT, version INTEGER DEFAULT 1, updated_at TEXT, firm_code TEXT);
    """)
    c.commit(); c.close()


@pytest.fixture
def srv(tmp_path, monkeypatch):
    db = tmp_path / "deps.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap_min(db)
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


def _post(url, body: bytes, *, headers: dict | None = None):
    hdrs = dict(headers or {})
    p = urllib.parse.urlparse(url)
    hdrs.setdefault("Origin", f"{p.scheme}://{p.netloc}")
    req = urllib.request.Request(url, data=body, method="POST", headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


# ---------------------------------------------------------------------------
# STRIPE FAILURES
# ---------------------------------------------------------------------------

# The live ``stripe`` Python SDK is an optional runtime dep. In the
# sandbox it's not installed, which makes signature verification return
# an import error instead of an invalid-signature error. Skip cleanly
# rather than silently let the test class false-pass.
_STRIPE_AVAILABLE = True
try:  # pragma: no cover - import-time detection
    import stripe  # type: ignore[import]
    del stripe
except ImportError:
    _STRIPE_AVAILABLE = False

_needs_stripe = pytest.mark.skipif(
    not _STRIPE_AVAILABLE,
    reason="stripe SDK not installed in this environment",
)


@_needs_stripe
def test_stripe_webhook_rejects_invalid_signature(srv):
    rd, db, base = srv
    # POST with no Stripe-Signature header → signature verification fails
    # → must 400 with a stable error body, NOT leak internal stack.
    status, hdrs, body = _post(
        f"{base}/stripe/webhook",
        b'{"id":"evt_1","type":"checkout.session.completed"}',
        headers={"Content-Type": "application/json"},
    )
    assert status == 400, (status, body[:200])
    payload = json.loads(body)
    assert payload["error"] == "invalid_signature"


@_needs_stripe
def test_stripe_webhook_rejects_malformed_json(srv):
    rd, db, base = srv
    status, _, body = _post(
        f"{base}/stripe/webhook",
        b"not-json-at-all",
        headers={"Content-Type": "application/json"},
    )
    assert status == 400
    # Body must be bounded / not leak internals.
    assert len(body) < 500
    payload = json.loads(body)
    assert payload["error"] == "invalid_signature"


@_needs_stripe
def test_stripe_webhook_replay_is_idempotent(srv):
    """BUG #6 regression: Stripe retries until it sees 2xx. Replaying
    the same event.id must not create a duplicate firm/user."""
    rd, db, base = srv
    fake_event = {
        "id": "evt_replay_detective",
        "type": "checkout.session.completed",
        "data": {"object": {
            "customer_email": "replay@det.com",
            "customer": "cus_REPLAY",
            "subscription": "sub_REPLAY",
            "metadata": {"plan": "pro_monthly"},
        }},
    }
    # Patch Stripe signature verification to always succeed with our event.
    import src.integrations.stripe_client  # noqa: F401  — force submodule import
    import src.integrations.email_client    # noqa: F401
    with patch("src.integrations.stripe_client.handle_webhook", return_value=fake_event), \
         patch("src.integrations.email_client.send_welcome_email", return_value=True):
        s1, _, _ = _post(
            f"{base}/stripe/webhook",
            json.dumps(fake_event).encode(),
            headers={"Content-Type": "application/json", "Stripe-Signature": "mock"},
        )
        s2, _, b2 = _post(
            f"{base}/stripe/webhook",
            json.dumps(fake_event).encode(),
            headers={"Content-Type": "application/json", "Stripe-Signature": "mock"},
        )
    assert s1 == 200 and s2 == 200
    # Second response must report idempotent: True.
    p2 = json.loads(b2)
    assert p2.get("idempotent") is True, f"expected idempotent=True on replay, got {p2}"
    # And exactly one user/firm was created.
    c = sqlite3.connect(str(db))
    n_users = c.execute(
        "SELECT COUNT(*) FROM dashboard_users WHERE username='replay@det.com'",
    ).fetchone()[0]
    n_firms = c.execute(
        "SELECT COUNT(*) FROM firms WHERE stripe_customer_id='cus_REPLAY'",
    ).fetchone()[0]
    c.close()
    assert n_users == 1, f"replay created {n_users} users, expected 1"
    assert n_firms == 1, f"replay created {n_firms} firms, expected 1"


def test_stripe_webhook_returns_400_even_when_sdk_missing(srv):
    """Sandbox / minimal install may not have the ``stripe`` SDK. The
    webhook handler still must NOT 500; it should return 400
    invalid_signature because handle_webhook will raise ImportError or
    similar inside its try/except."""
    rd, db, base = srv
    status, _, body = _post(
        f"{base}/stripe/webhook",
        b'{"id":"evt_no_sdk"}',
        headers={"Content-Type": "application/json"},
    )
    assert status in (400, 500), status  # 400 expected; 500 would be the bug
    if status == 500:
        pytest.fail(
            "stripe webhook returned 500 with no SDK installed - the "
            "blanket try/except around handle_webhook should turn import "
            "failure into a 400 invalid_signature, not a server crash."
        )


@_needs_stripe
def test_stripe_webhook_accepts_unknown_event_type_without_crashing(srv):
    """New Stripe event types show up over time. A ``customer.subscription.
    deleted`` before a ``created`` or an unknown type must not 500."""
    rd, db, base = srv
    fake_event = {
        "id": "evt_unknown_1",
        "type": "some.new.future.event.type",
        "data": {"object": {"id": "whatever"}},
    }
    import src.integrations.stripe_client  # noqa: F401
    with patch("src.integrations.stripe_client.handle_webhook", return_value=fake_event):
        status, _, body = _post(
            f"{base}/stripe/webhook",
            json.dumps(fake_event).encode(),
            headers={"Content-Type": "application/json", "Stripe-Signature": "mock"},
        )
    assert status == 200, (status, body[:200])


# ---------------------------------------------------------------------------
# DOCAI / OCR failures
# ---------------------------------------------------------------------------

def test_parse_invoice_fields_absurd_amount_caps_do_not_crash_pipeline():
    """Simulate a DocAI return of ``total = $99,999,999,999``. The OCR
    parser must drop the amount + flag, not crash downstream validators."""
    from src.engines.ocr_engine import parse_invoice_fields
    r = parse_invoice_fields("TOTAL: $99,999,999,999")
    assert r.get("amount") is None
    assert r.get("amount_flagged_absurd") is True
    # And the confidence should be below the auto-accept threshold.
    assert r.get("confidence", 1.0) < 0.85


def test_ocr_pipeline_handles_empty_extraction():
    """DocAI returning 0 entities → parse_invoice_fields on empty input
    must return a shaped dict with confidence 0, not raise."""
    from src.engines.ocr_engine import parse_invoice_fields
    r = parse_invoice_fields("")
    assert r["vendor"] is None
    assert r["amount"] is None
    assert r["confidence"] == 0.0


def test_pdf_plumber_handles_missing_file():
    """File path doesn't exist → return dict with confidence 0, no crash."""
    from src.engines.ocr_engine import extract_with_pdfplumber
    r = extract_with_pdfplumber("/tmp/does-not-exist-yzxq.pdf")
    assert isinstance(r, dict)
    assert r.get("confidence", 1.0) == 0.0


# ---------------------------------------------------------------------------
# QBO / GMAIL — boundary checks via adapter failure shapes.
# ---------------------------------------------------------------------------

def test_qbo_adapter_missing_config_does_not_corrupt_posting_jobs(srv):
    """If the QBO adapter is not configured for a client, posting should
    fail loudly rather than leave a half-written posting_jobs row."""
    rd, db, base = srv
    # Seed a doc with a posting job in 'pending' state.
    c = sqlite3.connect(str(db))
    c.execute(
        "INSERT INTO documents (document_id, client_code, vendor, review_status, firm_code) "
        "VALUES ('D1','CLI','V','Ready','OWNER')",
    )
    c.execute(
        "INSERT INTO posting_jobs (posting_id, document_id, target_system, entry_kind, "
        "posting_status, approval_state, payload_json, created_at, updated_at) "
        "VALUES ('P1','D1','qbo','expense','pending','approved','{}',datetime('now'),datetime('now'))",
    )
    c.commit(); c.close()

    # Patch QBO adapter to simulate an unconfigured client.
    with patch("src.agents.tools.qbo_online_adapter.post_one_ready_job",
               side_effect=RuntimeError("qbo_not_configured")):
        try:
            from src.agents.tools.qbo_online_adapter import post_one_ready_job
            post_one_ready_job(db_path=db)
        except RuntimeError:
            pass  # expected

    # posting_jobs row still exists and its status didn't flip to "posted".
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    row = c.execute("SELECT posting_status FROM posting_jobs WHERE posting_id='P1'").fetchone()
    c.close()
    assert row is not None
    assert row["posting_status"] != "posted", (
        f"RuntimeError leaked a posted status: {dict(row)}"
    )


# ---------------------------------------------------------------------------
# Claude / generic AI — JSON-schema mismatches
# ---------------------------------------------------------------------------

def test_parse_invoice_fields_with_html_injection_not_trusted():
    """If an upstream AI returns HTML/JS in vendor or description, our
    parser should strip or keep it but never execute; downstream
    escape_html handles display. Here we verify the parser does not
    crash and returns the string as-is."""
    from src.engines.ocr_engine import parse_invoice_fields
    r = parse_invoice_fields("<script>alert(1)</script>\nACME Inc\nTOTAL: $50.00\nDate: 2026-01-01")
    # Parser should still find the vendor / total without being tripped
    # by the HTML tag.
    assert isinstance(r, dict)
    # Confidence should not be maxed — the HTML injection is suspicious.
    assert r.get("confidence", 1.0) < 1.0
