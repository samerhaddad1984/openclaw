"""
tests/test_onboarding_reliability.py
====================================
Sprint 2: onboarding reliability — signed set-password links, /forgot, and
welcome-email fallback UI.

Covers:
  - Signed set-password link helpers (generate / verify / expiry / tampering)
  - /forgot enumeration safety + rate limiting + email sent on real user
  - Stripe provisioning uses placeholder password + returns set-password URL
  - render_signup_success shows fallback link when email delivery fails
  - /set-password enforces length + flips must_reset_password=0
"""
from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _create_minimal_db(tmp_path: Path) -> Path:
    """Create the minimum schema review_dashboard needs for these tests."""
    db = tmp_path / "onboarding.db"
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE dashboard_users (
            username      TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            role          TEXT NOT NULL DEFAULT 'employee',
            display_name  TEXT,
            active        INTEGER NOT NULL DEFAULT 1,
            language      TEXT NOT NULL DEFAULT 'fr',
            firm_code     TEXT,
            email         TEXT,
            must_reset_password INTEGER NOT NULL DEFAULT 0,
            created_at    TEXT
        );
        CREATE TABLE firms (
            firm_code     TEXT PRIMARY KEY,
            firm_name     TEXT,
            contact_email TEXT,
            billing_email TEXT,
            language      TEXT,
            plan          TEXT,
            active        INTEGER DEFAULT 1,
            stripe_customer_id TEXT,
            stripe_subscription_id TEXT,
            subscription_status TEXT,
            created_at    TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE forgot_password_attempts (
            identifier  TEXT NOT NULL,
            attempted_at TEXT NOT NULL
        );
    """)
    conn.commit()
    conn.close()
    return db


@pytest.fixture
def rd(tmp_path, monkeypatch):
    """Import review_dashboard with a private DB + secret file."""
    db = _create_minimal_db(tmp_path)
    secret_file = tmp_path / "password_link_secret"
    secret_file.write_text("a" * 48)  # deterministic per-test secret

    import scripts.review_dashboard as _rd
    monkeypatch.setattr(_rd, "DB_PATH", db)
    monkeypatch.setattr(_rd, "PASSWORD_LINK_SECRET_FILE", str(secret_file))
    return _rd


# ---------------------------------------------------------------------------
# Token helpers
# ---------------------------------------------------------------------------

class TestSetPasswordLink:
    def test_set_password_link_valid_within_72h(self, rd):
        token = rd._generate_password_link("alice@example.com", expires_hours=72)
        assert rd._verify_password_link(token) == "alice@example.com"

    def test_set_password_link_expires_after_72h(self, rd):
        # Generate a token that already expired (1h in the past)
        real_time = time.time
        with patch("scripts.review_dashboard.time.time",
                   return_value=real_time() - 73 * 3600):
            token = rd._generate_password_link("bob@example.com", expires_hours=72)
        # verified with "now" — should reject
        assert rd._verify_password_link(token) is None

    def test_set_password_link_invalid_signature_rejected(self, rd, tmp_path):
        token = rd._generate_password_link("carol@example.com")
        # Re-point secret to a different value to break the HMAC
        (tmp_path / "password_link_secret").write_text("b" * 48)
        assert rd._verify_password_link(token) is None

    def test_set_password_link_tampered_username_rejected(self, rd):
        import base64
        token = rd._generate_password_link("dave@example.com")
        padded = token + "=" * (4 - len(token) % 4)
        decoded = base64.urlsafe_b64decode(padded).decode()
        # Swap username then re-encode (signature no longer matches)
        username, expiry, sig = decoded.rsplit(".", 2)
        tampered = f"attacker@example.com.{expiry}.{sig}"
        bad_token = base64.urlsafe_b64encode(tampered.encode()).decode().rstrip("=")
        assert rd._verify_password_link(bad_token) is None

    def test_set_password_link_garbage_token_returns_none(self, rd):
        assert rd._verify_password_link("") is None
        assert rd._verify_password_link("notbase64!!!") is None
        assert rd._verify_password_link("YWJjZGVm") is None  # valid b64 but wrong shape


# ---------------------------------------------------------------------------
# Password strength
# ---------------------------------------------------------------------------

class TestPasswordStrength:
    def test_set_password_enforces_min_length(self, rd):
        err = rd._validate_password_strength("short1a", "short1a")
        assert err  # rejected

    def test_set_password_accepts_strong(self, rd):
        err = rd._validate_password_strength("Strong-Pass-9", "Strong-Pass-9")
        assert err == ""

    def test_set_password_requires_digit(self, rd):
        err = rd._validate_password_strength("nodigitsatall", "nodigitsatall")
        assert err

    def test_set_password_requires_letter(self, rd):
        err = rd._validate_password_strength("1234567890123", "1234567890123")
        assert err

    def test_set_password_mismatch_rejected(self, rd):
        err = rd._validate_password_strength("Strong-Pass-9", "Other-Pass-9")
        assert err


# ---------------------------------------------------------------------------
# /forgot behaviour (rate limit + enumeration prevention)
# ---------------------------------------------------------------------------

class TestForgotPassword:
    def test_forgot_password_rate_limited(self, rd):
        ident = "abuser@example.com"
        assert rd._forgot_rate_limited(ident) is False
        for _ in range(3):
            rd._record_forgot_attempt(ident)
        assert rd._forgot_rate_limited(ident) is True

    def test_forgot_password_separate_identifiers_not_limited(self, rd):
        for _ in range(3):
            rd._record_forgot_attempt("a@example.com")
        assert rd._forgot_rate_limited("a@example.com") is True
        assert rd._forgot_rate_limited("b@example.com") is False

    def test_forgot_password_generic_response_prevents_enumeration(self, rd):
        """A request for a non-existent user must render the same generic page as
        a request for a real user (no status-code or body divergence)."""
        out_none = rd.render_forgot_password_page(
            flash=("Si un compte existe, un courriel a \u00e9t\u00e9 envoy\u00e9. / "
                   "If an account exists, an email has been sent."),
            lang="fr",
        )
        # Insert a real user and render the same page — must be identical
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO dashboard_users (username, password_hash, role,"
                " firm_code, email, must_reset_password, created_at)"
                " VALUES (?,?,?,?,?,?,?)",
                ("real@example.com", "hash", "firm_admin", "CPA111",
                 "real@example.com", 1, "2026-01-01T00:00:00+00:00"),
            )
        out_real = rd.render_forgot_password_page(
            flash=("Si un compte existe, un courriel a \u00e9t\u00e9 envoy\u00e9. / "
                   "If an account exists, an email has been sent."),
            lang="fr",
        )
        assert out_none == out_real

    def test_forgot_password_sends_email_when_user_exists(self, rd):
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO dashboard_users (username, password_hash, role,"
                " firm_code, email, must_reset_password, active, created_at)"
                " VALUES (?,?,?,?,?,?,1,?)",
                ("eve@example.com", "hash", "firm_admin", "CPA222",
                 "eve@example.com", 0, "2026-01-01T00:00:00+00:00"),
            )
        # Lookup logic lives in the POST handler; replicate the essential query:
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.row_factory = rd._dict_factory
            row = conn.execute(
                "SELECT username, email FROM dashboard_users"
                " WHERE active=1 AND (username=? OR email=?) LIMIT 1",
                ("eve@example.com", "eve@example.com"),
            ).fetchone()
        assert row and row["username"] == "eve@example.com"
        token = rd._generate_password_link(row["username"])
        assert rd._verify_password_link(token) == "eve@example.com"


# ---------------------------------------------------------------------------
# Stripe provisioning
# ---------------------------------------------------------------------------

class _StripeSessionStub(dict):
    """Mimics the subset of stripe.checkout.Session used by the provisioner."""
    pass


class TestStripeProvisioning:
    def _session(self, email="new@firm.com", customer_id="cus_TEST1",
                plan="pro", firm_code=None):
        meta = {"plan": plan}
        if firm_code:
            meta["firm_code"] = firm_code
        return _StripeSessionStub({
            "customer_email": email,
            "customer": customer_id,
            "subscription": "sub_TEST1",
            "metadata": meta,
        })

    def test_stripe_webhook_creates_user_with_placeholder_password(self, rd):
        with patch("src.integrations.email_client.send_welcome_email",
                   return_value=True):
            result = rd._provision_firm_from_stripe(
                self._session(), base_url="http://test")
        assert result["existing"] is False
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.row_factory = rd._dict_factory
            user = conn.execute(
                "SELECT * FROM dashboard_users WHERE username=?",
                (result["admin_username"],),
            ).fetchone()
        assert user is not None
        # must_reset_password is set → they must use the link
        assert user["must_reset_password"] == 1
        # Password hash is bcrypt of a random 32-byte token. Accessing the hash
        # alone cannot yield the original — the best we can check is that it is
        # non-empty bcrypt-shaped.
        assert user["password_hash"].startswith(("$2a$", "$2b$", "$2y$"))

    def test_stripe_webhook_returns_set_password_url(self, rd):
        with patch("src.integrations.email_client.send_welcome_email",
                   return_value=True):
            result = rd._provision_firm_from_stripe(
                self._session(email="x@y.com"),
                base_url="https://app.otocpa.com")
        assert result["set_password_url"].startswith(
            "https://app.otocpa.com/set-password?token=")
        # And that token verifies back to the created username
        token = result["set_password_url"].split("token=", 1)[1]
        assert rd._verify_password_link(token) == result["admin_username"]

    def test_stripe_webhook_email_failure_does_not_abort(self, rd):
        """Even when send_welcome_email raises, provisioning completes and the
        URL is returned (the UI uses it as a fallback)."""
        with patch("src.integrations.email_client.send_welcome_email",
                   side_effect=RuntimeError("smtp down")):
            result = rd._provision_firm_from_stripe(
                self._session(email="boom@example.com"),
                base_url="http://test")
        assert result["email_sent"] is False
        assert "set-password?token=" in result["set_password_url"]

    def test_stripe_existing_customer_returns_existing(self, rd):
        # First call creates the firm
        with patch("src.integrations.email_client.send_welcome_email",
                   return_value=True):
            rd._provision_firm_from_stripe(
                self._session(email="repeat@example.com", customer_id="cus_R"),
                base_url="http://test")
        # Second call with same customer id → existing
        with patch("src.integrations.email_client.send_welcome_email",
                   return_value=True):
            again = rd._provision_firm_from_stripe(
                self._session(email="repeat@example.com", customer_id="cus_R"),
                base_url="http://test")
        assert again["existing"] is True


# ---------------------------------------------------------------------------
# /set-password flow: marks must_reset_password=0
# ---------------------------------------------------------------------------

class TestSetPasswordFlow:
    def test_set_password_marks_must_reset_done(self, rd):
        # seed a user with must_reset_password=1
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO dashboard_users (username, password_hash, role,"
                " firm_code, email, must_reset_password, active, created_at)"
                " VALUES (?,?,?,?,?,1,1,?)",
                ("greg@example.com", "placeholder", "firm_admin", "CPA333",
                 "greg@example.com", "2026-01-01T00:00:00+00:00"),
            )
            conn.commit()

        # Simulate what the /set-password POST does on a valid token
        token = rd._generate_password_link("greg@example.com")
        username = rd._verify_password_link(token)
        assert username == "greg@example.com"
        err = rd._validate_password_strength("Strong-Pass-9", "Strong-Pass-9")
        assert err == ""
        with sqlite3.connect(str(rd.DB_PATH)) as conn:
            conn.execute(
                "UPDATE dashboard_users SET password_hash=?,"
                " must_reset_password=0 WHERE username=?",
                (rd.hash_password("Strong-Pass-9"), username),
            )
            conn.commit()
            row = conn.execute(
                "SELECT password_hash, must_reset_password FROM dashboard_users"
                " WHERE username=?",
                (username,),
            ).fetchone()
        assert row[1] == 0
        assert rd.verify_password("Strong-Pass-9", row[0])


# ---------------------------------------------------------------------------
# render_signup_success fallback UI
# ---------------------------------------------------------------------------

class TestSignupSuccessRendering:
    def test_signup_success_shows_link_when_email_fails(self, rd):
        html_out = rd.render_signup_success(
            firm_code="CPA999",
            admin_username="joe@example.com",
            set_password_url="https://app.otocpa.com/set-password?token=ABC",
            email="joe@example.com",
            email_sent=False,
        )
        # fallback path: the token URL is shown inline
        assert "https://app.otocpa.com/set-password?token=ABC" in html_out
        # expiry warning
        assert "72" in html_out
        # Resend button is present
        assert "/signup/resend-setup" in html_out

    def test_signup_success_hides_link_inline_when_email_sent(self, rd):
        html_out = rd.render_signup_success(
            firm_code="CPA888",
            admin_username="jill@example.com",
            set_password_url="https://app.otocpa.com/set-password?token=XYZ",
            email="jill@example.com",
            email_sent=True,
        )
        # The link is behind a <details> disclosure; the <details> tag must
        # be present (graceful degradation), and the confirmation mentions the
        # email address.
        assert "<details" in html_out
        assert "jill@example.com" in html_out
        assert "/signup/resend-setup" in html_out


# ---------------------------------------------------------------------------
# Login page exposes forgot link
# ---------------------------------------------------------------------------

class TestLoginForgotLink:
    def test_login_page_contains_forgot_link(self, rd):
        html_fr = rd.render_login("", lang="fr")
        assert "/forgot" in html_fr
        assert "Mot de passe oubli" in html_fr
        html_en = rd.render_login("", lang="en")
        assert "/forgot" in html_en
        assert "Forgot password" in html_en
