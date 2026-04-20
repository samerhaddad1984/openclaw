"""R5-Investigation 1 — time-based attacks."""
from __future__ import annotations

import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
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
            active INTEGER DEFAULT 1, version INTEGER DEFAULT 1);
        CREATE TABLE documents (document_id TEXT PRIMARY KEY, firm_code TEXT);
    """)
    c.commit(); c.close()


@pytest.fixture
def app(tmp_path, monkeypatch):
    db = tmp_path / "t.db"
    secret = tmp_path / "s"; secret.write_text("x" * 48)
    _bootstrap(db)
    import scripts.review_dashboard as rd
    monkeypatch.setattr(rd, "DB_PATH", db)
    monkeypatch.setattr(rd, "PASSWORD_LINK_SECRET_FILE", str(secret))
    rd.bootstrap_schema()
    with patch("src.integrations.email_client.send_welcome_email", return_value=True):
        rd._provision_firm_from_stripe(
            {"customer_email": "t@det.com", "customer": "cus_T",
             "subscription": "sub_T", "metadata": {"plan": "pro_monthly"}},
            base_url="http://test",
        )
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "UPDATE dashboard_users SET password_hash=?, must_reset_password=0 "
            "WHERE username=?", (rd.hash_password("T1Time!"), "t@det.com"),
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
# A — Session expires mid-request
# ---------------------------------------------------------------------------

def test_expired_session_token_redirects_to_login(app):
    """A session token whose DB expires_at is in the past must not
    authenticate."""
    rd = app["rd"]
    tok = rd.create_session("t@det.com")
    # Force-expire the row.
    with sqlite3.connect(str(app["db"])) as c:
        c.execute(
            "UPDATE dashboard_sessions SET expires_at='2020-01-01T00:00:00+00:00' "
            "WHERE session_token=?", (tok,),
        )
        c.commit()
    req = urllib.request.Request(
        f"{app['base']}/",
        headers={"Cookie": f"session_token={tok}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            status = r.status
            body = r.read()
    except urllib.error.HTTPError as e:
        status, body = e.code, e.read()
    text = body.decode("utf-8", errors="replace")
    # Must be a login page response (200 + login form or 302/303 to /login).
    if status == 200:
        assert "type=\"password\"" in text or "Connexion" in text, (
            f"expired session returned dashboard instead of login"
        )


def test_session_not_returned_after_delete(app):
    rd = app["rd"]
    tok = rd.create_session("t@det.com")
    rd.delete_session(tok)
    with sqlite3.connect(str(app["db"])) as c:
        n = c.execute(
            "SELECT COUNT(*) FROM dashboard_sessions WHERE session_token=?",
            (tok,),
        ).fetchone()[0]
    assert n == 0


# ---------------------------------------------------------------------------
# B — Password-reset token expiry
# ---------------------------------------------------------------------------

def test_password_link_72h_default_expiry(app):
    rd = app["rd"]
    tok = rd._generate_password_link("t@det.com")
    # Decode to inspect.
    import base64
    padded = tok + "=" * (4 - len(tok) % 4)
    decoded = base64.urlsafe_b64decode(padded).decode()
    parts = decoded.rsplit(".", 2)
    expiry = int(parts[1])
    # Default is 72 hours → expiry is 72h ahead of now (± a couple seconds).
    now = int(time.time())
    delta = expiry - now
    assert 72 * 3600 - 10 <= delta <= 72 * 3600 + 10, (
        f"default expiry not 72h: delta={delta}s"
    )


def test_password_link_with_custom_expiry(app, monkeypatch):
    rd = app["rd"]
    tok = rd._generate_password_link("t@det.com", expires_hours=1)
    import base64
    padded = tok + "=" * (4 - len(tok) % 4)
    decoded = base64.urlsafe_b64decode(padded).decode()
    expiry = int(decoded.rsplit(".", 2)[1])
    delta = expiry - int(time.time())
    assert 3600 - 10 <= delta <= 3600 + 10


def test_password_link_rejects_future_timestamp(app, monkeypatch):
    """A token 100 years in the future (clock skew / forged) should be
    treated as expired (or at least not accepted). We verify by
    building a token that's "now", then stubbing time to 200 years
    ahead — the token should be rejected as expired."""
    rd = app["rd"]
    tok = rd._generate_password_link("t@det.com")
    # Jump forward 200 years.
    future_time = time.time() + 200 * 365 * 86400
    with patch("scripts.review_dashboard.time.time",
                side_effect=lambda: future_time):
        result = rd._verify_password_link(tok)
    assert result is None


# ---------------------------------------------------------------------------
# C — DST boundary queries
# ---------------------------------------------------------------------------

def test_spring_forward_period_extraction():
    """2026-03-08 is a DST spring-forward day in US/Eastern. A document
    stored with document_date='2026-03-08' must extract period '2026-03',
    no crash, no empty period."""
    from src.agents.core.period_close import get_document_period
    assert get_document_period("2026-03-08") == "2026-03"


def test_fall_back_ambiguous_stored_as_utc():
    """A UTC timestamp that happens to represent both pre- and
    post-DST-end local times still maps to the same month."""
    from src.agents.core.period_close import get_document_period
    for utc in ("2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z"):
        assert get_document_period(utc) == "2026-11"


# ---------------------------------------------------------------------------
# D — Year boundary
# ---------------------------------------------------------------------------

def test_year_boundary_iso_dates_land_in_right_period():
    """Documents dated 2026-12-31 and 2027-01-01 must each go into
    their own period."""
    from src.agents.core.period_close import get_document_period
    assert get_document_period("2026-12-31") == "2026-12"
    assert get_document_period("2027-01-01") == "2027-01"


def test_leap_year_feb_29_handled():
    """Feb 29 2024 is a valid leap-year date."""
    from src.agents.core.period_close import get_document_period
    assert get_document_period("2024-02-29") == "2024-02"


# ---------------------------------------------------------------------------
# F — Server clock drift (regression from R3 test)
# ---------------------------------------------------------------------------

def test_utc_now_iso_matches_wall_clock_within_one_minute():
    import scripts.review_dashboard as rd
    system = datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(rd.utc_now_iso().replace("Z", "+00:00"))
    delta = abs((system - parsed).total_seconds())
    assert delta < 60.0


# ---------------------------------------------------------------------------
# E — Password-reset cleanup retention policy
# ---------------------------------------------------------------------------

def test_password_reset_used_7day_cleanup(app):
    """The consumed-token registry purges entries older than 7 days on
    every mark_used call."""
    rd = app["rd"]
    # Seed an ancient row and a fresh row.
    with sqlite3.connect(str(app["db"])) as c:
        c.execute(
            "INSERT INTO password_reset_used (token_hash, username, used_at) "
            "VALUES ('ancient_xyz', 't@det.com', datetime('now','-8 days'))",
        )
        c.execute(
            "INSERT INTO password_reset_used (token_hash, username, used_at) "
            "VALUES ('recent_xyz', 't@det.com', datetime('now','-2 days'))",
        )
        c.commit()
    # Trigger cleanup via a new mark_used. The real API hashes the
    # token; we don't need to predict the hash — just that cleanup ran.
    rd._password_reset_token_mark_used("trigger_xyz", "t@det.com")
    with sqlite3.connect(str(app["db"])) as c:
        rows = {
            r[0] for r in c.execute(
                "SELECT token_hash FROM password_reset_used",
            ).fetchall()
        }
    assert "ancient_xyz" not in rows, "8-day-old entry not purged"
    assert "recent_xyz" in rows, "2-day-old entry over-purged"
    # A fresh row exists (hashed, so we check count instead of value).
    assert len(rows) == 2  # recent + newly-marked hashed
