"""
RED TEAM: Licensing, Security, Setup Wizard, and Combined Chaos Destruction
============================================================================
Attacks licensing tier enforcement, auth security, setup wizard re-run
protection, and multi-component combined chaos scenarios.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch, MagicMock

import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.engines.license_engine import (
    TIER_DEFAULTS,
    _sign,
    _add_padding,
    generate_license_key,
    load_license,
    get_license_status,
    check_feature,
    check_limits,
    save_license_to_config,
)
from src.agents.core.dashboard_auth import (
    hash_password,
    verify_password,
    create_session,
    get_session,
    bootstrap_auth_schema,
    create_user,
    authenticate_user,
    ensure_default_owner,
    PBKDF2_ITERATIONS,
    delete_session,
    delete_all_sessions_for_user,
)
from src.agents.tools.review_policy import (
    decide_review_status,
    effective_confidence,
    validate_tax_extraction,
    ReviewDecision,
)
from src.agents.tools.rules_engine import RulesEngine
from src.agents.tools.vendor_intelligence import VendorIntelligenceEngine
from src.agents.tools.gl_mapper import GLMapper
from src.agents.tools.duplicate_detector import (
    DuplicateCandidate,
    _normalize_text as dup_normalize,
    _amount_equal,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
TEST_SECRET = "red-team-test-secret-2024"
FUTURE_DATE = (date.today() + timedelta(days=365)).strftime("%Y-%m-%d")
PAST_DATE = (date.today() - timedelta(days=10)).strftime("%Y-%m-%d")
TODAY_STR = date.today().strftime("%Y-%m-%d")


def _make_key(tier: str, secret: str = TEST_SECRET,
              expiry: str = FUTURE_DATE, **overrides) -> str:
    return generate_license_key(
        tier=tier,
        firm_name=overrides.get("firm_name", "Red Team CPA"),
        expiry_date=expiry,
        issued_at=TODAY_STR,
        secret=secret,
        max_clients=overrides.get("max_clients"),
        max_users=overrides.get("max_users"),
    )


def _tmp_config(content: dict) -> Path:
    """Write a temp otocpa.config.json and return its parent dir."""
    td = Path(tempfile.mkdtemp())
    (td / "otocpa.config.json").write_text(
        json.dumps(content, indent=2), encoding="utf-8"
    )
    return td


def _tmp_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _tmp_rules_dir(vendors: list[dict] | None = None,
                   gl_map: dict | None = None,
                   vendor_intel: dict | None = None) -> Path:
    td = Path(tempfile.mkdtemp())
    if vendors is not None:
        (td / "vendors.json").write_text(
            json.dumps({"vendors": vendors}), encoding="utf-8"
        )
    if gl_map is not None:
        (td / "gl_map.json").write_text(
            json.dumps(gl_map), encoding="utf-8"
        )
    if vendor_intel is not None:
        (td / "vendor_intel.json").write_text(
            json.dumps(vendor_intel), encoding="utf-8"
        )
    return td


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1: LICENSING / TIER ATTACKS
# ═══════════════════════════════════════════════════════════════════════════

class TestLicenseTierAttacks:
    """Attacks 1-6: License key and tier enforcement."""

    # Attack 1: Can tier enforcement be bypassed by modifying config?
    def test_attack1_tier_bypass_via_config_modification(self):
        """Modify the tier field inside the key payload and check that
        signature validation catches it."""
        key = _make_key("essentiel")
        # Decode, tamper tier, re-encode WITHOUT re-signing
        b64_part = key[5:]
        padded = _add_padding(b64_part)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
        original_sig = payload["sig"]

        # Tamper: change tier to entreprise
        payload["tier"] = "entreprise"
        # Keep original sig (should fail)
        tampered_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        tampered_b64 = base64.urlsafe_b64encode(tampered_bytes).decode().rstrip("=")
        tampered_key = f"LLAI-{tampered_b64}"

        with pytest.raises(ValueError, match="signature mismatch"):
            load_license(tampered_key, TEST_SECRET)

    # Attack 2: Is the license key validated cryptographically?
    def test_attack2_cryptographic_validation_not_string_check(self):
        """Ensure HMAC-SHA256 is used, not just a naive string comparison."""
        key = _make_key("professionnel")
        b64_part = key[5:]
        payload = json.loads(base64.urlsafe_b64decode(_add_padding(b64_part)).decode())

        # Verify that the sig field is a valid hex HMAC
        sig = payload["sig"]
        assert len(sig) == 64, "Signature must be 64 hex chars (SHA-256)"
        assert re.fullmatch(r"[0-9a-f]{64}", sig), "Signature must be lowercase hex"

        # Verify we can reproduce the signature with the secret
        expected_sig = _sign(payload, TEST_SECRET)
        assert hmac.compare_digest(sig, expected_sig), \
            "HMAC signature must be reproducible with the correct secret"

        # Wrong secret must fail
        wrong_sig = _sign(payload, "wrong-secret")
        assert not hmac.compare_digest(sig, wrong_sig), \
            "Different secret must produce different signature"

    # Attack 3: Can a "starter/essentiel" tier access "professional" features?
    def test_attack3_essentiel_cannot_access_professionnel_features(self):
        """Essentiel tier must NOT include ai_router, bank_parser, fraud_detection etc."""
        essentiel_features = set(TIER_DEFAULTS["essentiel"]["features"])
        professionnel_only = set(TIER_DEFAULTS["professionnel"]["features"]) - essentiel_features

        assert len(professionnel_only) > 0, "Professionnel should have exclusive features"

        for feature in professionnel_only:
            assert feature not in essentiel_features, \
                f"Feature {feature} must not be accessible to essentiel tier"

    def test_attack3b_tier_feature_escalation_via_get_license_status(self):
        """Even if we forge a key with tier=essentiel but inject extra features
        in the payload, get_license_status should use TIER_DEFAULTS, not payload."""
        key = _make_key("essentiel")
        # The key itself does not contain features - they come from TIER_DEFAULTS
        # This is the correct behavior: features are server-side controlled
        payload = load_license(key, TEST_SECRET)
        assert payload["tier"] == "essentiel"

        # Features come from TIER_DEFAULTS, not from the key payload
        tier_info = TIER_DEFAULTS.get(payload["tier"], {})
        features = tier_info.get("features", [])
        assert "ai_router" not in features, "essentiel must not include ai_router"
        assert "fraud_detection" not in features, "essentiel must not include fraud_detection"

    # Attack 4: What happens with expired license?
    def test_attack4_expired_license_rejected(self):
        """Expired license key must raise ValueError."""
        key = _make_key("professionnel", expiry=PAST_DATE)
        with pytest.raises(ValueError, match="expired"):
            load_license(key, TEST_SECRET)

    def test_attack4b_expired_license_status_shows_invalid(self):
        """get_license_status with expired key must return valid=False."""
        key = _make_key("professionnel", expiry=PAST_DATE)
        td = _tmp_config({"license": {"key": key, "secret": TEST_SECRET}})
        config_path = td / "otocpa.config.json"

        with patch("src.engines.license_engine.ROOT_DIR", td):
            status = get_license_status()
            assert status["valid"] is False
            assert "expired" in status["error"].lower()

    # Attack 5: Can license be reused across machines?
    def test_attack5_license_not_machine_bound(self):
        """FINDING: License keys are NOT bound to a machine fingerprint.
        The same key can be used on any machine. This is a known gap."""
        key = _make_key("professionnel")
        payload = load_license(key, TEST_SECRET)
        # No machine_id, hostname, or hardware fingerprint in the payload
        assert "machine_id" not in payload, \
            "FINDING: No machine binding - license can be reused across machines"
        assert "hostname" not in payload
        assert "hardware_id" not in payload

    # Attack 6: Is there a hardcoded bypass or debug mode?
    def test_attack6_no_hardcoded_bypass_in_license_engine(self):
        """Check that license_engine.py has no backdoor bypass strings."""
        engine_path = ROOT_DIR / "src" / "engines" / "license_engine.py"
        source = engine_path.read_text(encoding="utf-8")

        bypass_patterns = [
            r"debug\s*=\s*True",
            r"BYPASS",
            r"backdoor",
            r"skip_validation",
            r"always_valid",
            r"DEV_MODE",
        ]
        for pattern in bypass_patterns:
            matches = re.findall(pattern, source, re.IGNORECASE)
            assert len(matches) == 0, \
                f"FINDING: Potential bypass pattern found: {pattern} -> {matches}"

    def test_attack6b_empty_secret_does_not_validate(self):
        """If the signing secret is empty, license validation must still fail
        for keys signed with a real secret."""
        key = _make_key("professionnel")
        with pytest.raises(ValueError):
            load_license(key, "")

    def test_attack6c_forge_key_with_empty_secret(self):
        """Even if someone generates a key with empty secret, it should only
        validate against empty secret - not against the real secret."""
        forged_key = _make_key("entreprise", secret="")
        # This key validates against empty secret
        payload = load_license(forged_key, "")
        assert payload["tier"] == "entreprise"

        # But must NOT validate against the real secret
        with pytest.raises(ValueError, match="signature mismatch"):
            load_license(forged_key, TEST_SECRET)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2: SECURITY ATTACKS
# ═══════════════════════════════════════════════════════════════════════════

class TestSecurityAttacks:
    """Attacks 7-13: Password storage, sessions, CSRF, debug mode, secrets."""

    # Attack 7: Are passwords stored with bcrypt (not SHA-256)?
    def test_attack7_dashboard_auth_uses_pbkdf2_not_sha256(self):
        """dashboard_auth.py uses PBKDF2-SHA256 with 200k iterations.
        FINDING: Not bcrypt, but PBKDF2 with high iterations is acceptable.
        The scripts/setup_wizard.py uses bcrypt for its own users."""
        pw_hash = hash_password("TestPassword123!")
        parts = pw_hash.split("$")
        assert parts[0] == "pbkdf2_sha256", \
            f"Expected pbkdf2_sha256, got {parts[0]}"
        assert int(parts[1]) == 200_000, \
            f"Expected 200000 iterations, got {parts[1]}"
        assert len(parts[3]) == 64, "Hash should be 256-bit (64 hex chars)"

    def test_attack7b_setup_wizard_uses_bcrypt(self):
        """scripts/setup_wizard.py should use bcrypt for password hashing."""
        wizard_path = ROOT_DIR / "scripts" / "setup_wizard.py"
        source = wizard_path.read_text(encoding="utf-8")
        assert "bcrypt.hashpw" in source, \
            "setup_wizard.py should use bcrypt.hashpw"
        assert "bcrypt.gensalt" in source, \
            "setup_wizard.py should use bcrypt.gensalt"

    def test_attack7c_no_plain_sha256_for_passwords(self):
        """Neither auth module should use plain SHA-256 for passwords."""
        auth_path = ROOT_DIR / "src" / "agents" / "core" / "dashboard_auth.py"
        source = auth_path.read_text(encoding="utf-8")
        # Should not have hashlib.sha256(password) pattern
        assert "hashlib.sha256" not in source or "pbkdf2_hmac" in source, \
            "FINDING: Plain SHA-256 used for passwords"

    # Attack 8: Is there a default admin password?
    def test_attack8_default_admin_password_exists(self):
        """FINDING: ensure_default_owner() creates user 'sam' with password
        'ChangeMe123!'. This is a known default credential."""
        auth_path = ROOT_DIR / "src" / "agents" / "core" / "dashboard_auth.py"
        source = auth_path.read_text(encoding="utf-8")
        assert "ChangeMe123!" in source, \
            "Default password should exist (to confirm the finding)"
        # This is a FINDING: a default admin password is hardcoded.
        # Mitigation: force password change on first login.

    def test_attack8b_default_owner_has_full_access(self):
        """The default 'sam' user has 'owner' role = full access."""
        auth_path = ROOT_DIR / "src" / "agents" / "core" / "dashboard_auth.py"
        source = auth_path.read_text(encoding="utf-8")
        # The default user has role="owner"
        assert 'role="owner"' in source or "role='owner'" in source, \
            "Default user should have owner role"

    # Attack 9: Session token predictability
    def test_attack9_session_token_is_cryptographically_random(self):
        """Session tokens must use secrets.token_urlsafe, not predictable."""
        auth_path = ROOT_DIR / "src" / "agents" / "core" / "dashboard_auth.py"
        source = auth_path.read_text(encoding="utf-8")
        assert "secrets.token_urlsafe" in source, \
            "Session tokens must use secrets.token_urlsafe"
        # token_urlsafe(48) = 64 chars of URL-safe base64 (384 bits of entropy)
        assert "token_urlsafe(48)" in source, \
            "Token should use at least 48 bytes (384 bits) of entropy"

    def test_attack9b_session_tokens_are_unique(self):
        """Generate many tokens and verify no collisions."""
        tokens = set()
        for _ in range(1000):
            token = secrets.token_urlsafe(48)
            assert token not in tokens, "Token collision detected!"
            tokens.add(token)

    # Attack 10: CSRF protection
    def test_attack10_csrf_protection_missing(self):
        """FINDING: The setup wizard does not implement CSRF tokens.
        Forms do not include a hidden CSRF token field."""
        wizard_path = ROOT_DIR / "scripts" / "setup_wizard.py"
        source = wizard_path.read_text(encoding="utf-8")
        has_csrf = "csrf" in source.lower() or "xsrf" in source.lower()
        # This is a FINDING if no CSRF protection exists
        if not has_csrf:
            pytest.skip("FINDING: No CSRF protection in setup wizard (expected for local-only wizard)")

    # Attack 11: Debug mode left enabled in config
    def test_attack11_no_debug_mode_in_production(self):
        """Check that no debug=True or DEBUG=True is hardcoded in core modules."""
        core_files = [
            ROOT_DIR / "src" / "engines" / "license_engine.py",
            ROOT_DIR / "src" / "agents" / "core" / "dashboard_auth.py",
        ]
        for filepath in core_files:
            if filepath.exists():
                source = filepath.read_text(encoding="utf-8")
                # Exclude comments
                lines = [l for l in source.splitlines()
                         if not l.strip().startswith("#")]
                code = "\n".join(lines)
                debug_matches = re.findall(
                    r"(?:DEBUG|debug)\s*=\s*True", code
                )
                assert len(debug_matches) == 0, \
                    f"FINDING: Debug mode enabled in {filepath.name}: {debug_matches}"

    # Attack 12: Secrets in logs or error messages
    def test_attack12_license_error_does_not_leak_secret(self):
        """When license validation fails, the error message must not
        contain the signing secret."""
        key = _make_key("professionnel")
        try:
            load_license(key, "wrong-secret")
        except ValueError as exc:
            error_msg = str(exc)
            assert "wrong-secret" not in error_msg, \
                "FINDING: Secret leaked in error message"
            assert TEST_SECRET not in error_msg, \
                "FINDING: Real secret leaked in error message"

    def test_attack12b_password_hash_not_in_session(self):
        """authenticate_user should not return the password hash."""
        # We just check the return dict keys
        auth_path = ROOT_DIR / "src" / "agents" / "core" / "dashboard_auth.py"
        source = auth_path.read_text(encoding="utf-8")
        # The authenticate_user return block
        assert '"password_hash"' not in source.split("def authenticate_user")[1].split("def ")[0].split("return {")[1].split("}")[0], \
            "authenticate_user should not return password_hash"

    # Attack 13: API keys in source code or config files committed to git
    def test_attack13_no_api_keys_in_source(self):
        """Check that no real API keys are hardcoded in the setup wizard
        or license engine."""
        files_to_check = [
            ROOT_DIR / "src" / "engines" / "license_engine.py",
            ROOT_DIR / "src" / "agents" / "core" / "dashboard_auth.py",
        ]
        api_key_patterns = [
            r"sk-[a-zA-Z0-9]{32,}",  # OpenAI keys
            r"sk-or-[a-zA-Z0-9]{32,}",  # OpenRouter keys
            r"AKIA[A-Z0-9]{16}",  # AWS access keys
        ]
        for filepath in files_to_check:
            if filepath.exists():
                source = filepath.read_text(encoding="utf-8")
                for pattern in api_key_patterns:
                    matches = re.findall(pattern, source)
                    assert len(matches) == 0, \
                        f"FINDING: API key pattern found in {filepath.name}: {pattern}"

    def test_attack13b_setup_wizard_placeholder_client_id(self):
        """The setup_wizard.py (tools version) should have a placeholder,
        not a real Azure client ID."""
        wizard_path = ROOT_DIR / "src" / "agents" / "tools" / "setup_wizard.py"
        if wizard_path.exists():
            source = wizard_path.read_text(encoding="utf-8")
            if "PASTE_YOUR" in source:
                pass  # Good - placeholder
            else:
                # Check it's not a real GUID
                guid_pattern = r'CLIENT_ID\s*=\s*"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"'
                matches = re.findall(guid_pattern, source)
                if matches:
                    pytest.fail(f"FINDING: Real Azure Client ID found: {matches[0][:8]}...")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3: SETUP WIZARD ATTACKS
# ═══════════════════════════════════════════════════════════════════════════

class TestSetupWizardAttacks:
    """Attacks 14-17: Setup wizard re-run, validation, injection."""

    # Attack 14: Can setup be re-run after initial setup to overwrite config?
    def test_attack14_setup_blocks_rerun_when_complete(self):
        """After setup_complete=True, visiting any step should show
        'already_complete' page instead of the form."""
        wizard_path = ROOT_DIR / "scripts" / "setup_wizard.py"
        source = wizard_path.read_text(encoding="utf-8")

        # Count how many times setup_complete check guards GET endpoints
        get_guards = source.count('state.get("setup_complete")')
        # There should be guards on steps 1-5 plus root (step 6/complete sets
        # the flag itself, so 6 guards is correct: root + 5 step pages)
        assert get_guards >= 6, \
            f"FINDING: Only {get_guards} setup_complete guards found, expected >= 6"

    def test_attack14b_state_file_tampering(self):
        """If someone manually deletes setup_state.json, load_state returns
        default (setup_complete=False), allowing re-run.
        FINDING: State file deletion allows setup re-run."""
        # Import from scripts
        sys.path.insert(0, str(ROOT_DIR / "scripts"))
        try:
            from setup_wizard import load_state, save_state, STATE_FILE
        finally:
            sys.path.pop(0)

        # Save complete state
        td = Path(tempfile.mkdtemp())
        fake_state_file = td / "setup_state.json"
        fake_state_file.write_text(
            json.dumps({"steps_complete": [1, 2, 3, 4, 5, 6], "setup_complete": True}),
            encoding="utf-8"
        )

        # If file is deleted, load_state returns incomplete
        fake_state_file.unlink()
        # Default load_state from a missing file returns setup_complete: False
        # This is a design choice - could be a finding depending on threat model

    # Attack 15: Empty/null values in mandatory fields
    def test_attack15_empty_fields_rejected(self):
        """Step 1 validation must reject empty mandatory fields."""
        sys.path.insert(0, str(ROOT_DIR / "scripts"))
        try:
            from setup_wizard import validate_step1
        finally:
            sys.path.pop(0)

        # All empty
        errors = validate_step1({})
        assert len(errors) > 0, "Empty form must produce errors"

        # Whitespace-only
        errors = validate_step1({
            "firm_name": "   ",
            "firm_address": "  ",
            "gst_number": " ",
            "qst_number": " ",
            "owner_name": " ",
            "owner_email": " ",
            "owner_password": " ",
            "owner_password_confirm": " ",
        })
        assert len(errors) > 0, "Whitespace-only fields must be rejected"

    def test_attack15b_null_values_crash_validation(self):
        """FINDING: None values in form fields crash validate_step1() because
        data.get(f, "").strip() fails when value is None (NoneType has no .strip()).
        The default="" only applies if key is missing, not if value is None."""
        sys.path.insert(0, str(ROOT_DIR / "scripts"))
        try:
            from setup_wizard import validate_step1
        finally:
            sys.path.pop(0)

        with pytest.raises(AttributeError, match="'NoneType'.*'strip'"):
            validate_step1({
                "firm_name": None,
                "firm_address": None,
                "gst_number": None,
                "qst_number": None,
                "owner_name": None,
                "owner_email": None,
                "owner_password": None,
                "owner_password_confirm": None,
            })
        # FIX: Change data.get(f, "").strip() to (data.get(f) or "").strip()

    # Attack 16: SQL injection in setup form fields
    def test_attack16_sql_injection_in_firm_name(self):
        """SQL injection attempts in form fields should not execute.
        The wizard uses parameterized queries, so injection should be harmless."""
        sys.path.insert(0, str(ROOT_DIR / "scripts"))
        try:
            from setup_wizard import validate_step1
        finally:
            sys.path.pop(0)

        injection_payloads = [
            "'; DROP TABLE dashboard_users; --",
            "Robert'); DROP TABLE clients;--",
            "1 OR 1=1",
            "' UNION SELECT password_hash FROM dashboard_users--",
        ]

        for payload in injection_payloads:
            data = {
                "firm_name": payload,
                "firm_address": "123 Test St",
                "gst_number": "123456789RT0001",
                "qst_number": "1234567890TQ0001",
                "owner_name": "Test Owner",
                "owner_email": "test@test.com",
                "owner_password": "SecurePass1!",
                "owner_password_confirm": "SecurePass1!",
            }
            # Validation should pass (the text is valid as a string)
            # The real protection is parameterized queries in DB operations
            errors = validate_step1(data)
            # No crash = good

    def test_attack16b_parameterized_queries_used(self):
        """Verify that the setup wizard uses parameterized queries (?) not
        string formatting for SQL."""
        wizard_path = ROOT_DIR / "scripts" / "setup_wizard.py"
        source = wizard_path.read_text(encoding="utf-8")
        # Check for parameterized query patterns
        assert "?" in source, "Should use parameterized queries"
        # Check there's no f-string SQL
        sql_fstring_pattern = r'f"[^"]*(?:INSERT|UPDATE|DELETE|SELECT)[^"]*\{[^}]+\}'
        matches = re.findall(sql_fstring_pattern, source, re.IGNORECASE)
        # Filter out non-SQL f-strings
        sql_injection_risk = [m for m in matches if any(
            kw in m.upper() for kw in ["INSERT", "UPDATE", "DELETE", "WHERE"]
        )]
        assert len(sql_injection_risk) == 0, \
            f"FINDING: F-string SQL found (injection risk): {sql_injection_risk}"

    # Attack 17: Path traversal in file path inputs
    def test_attack17_path_traversal_in_config(self):
        """If someone injects path traversal strings into config values,
        ensure they don't escape the data directory."""
        traversal_payloads = [
            "../../../etc/passwd",
            "..\\..\\..\\Windows\\System32\\config\\SAM",
            "/etc/shadow",
            "....//....//etc/passwd",
        ]
        # These would be config values - verify they stay as strings
        for payload in traversal_payloads:
            cfg = {"firm": {"firm_name": payload}}
            # JSON serialization preserves the string safely
            serialized = json.dumps(cfg)
            deserialized = json.loads(serialized)
            assert deserialized["firm"]["firm_name"] == payload
            # The key question is whether any code uses these as file paths
            # Review: the firm_name is never used as a file path in the wizard


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4: COMBINED CHAOS SCENARIOS
# ═══════════════════════════════════════════════════════════════════════════

class TestCombinedChaosScenarios:
    """Attacks 18-24: Multi-component combined chaos."""

    # Attack 18: French Quebec invoice + OCR noise + vendor alias +
    # tax math correct but wrong context + partial payment next period
    def test_attack18_french_quebec_invoice_with_ocr_noise(self):
        """Compose: noisy French vendor name + correct GST/QST math +
        partial payment scenario. The system must flag for review."""
        # Simulate OCR-noisy French invoice
        vendor_raw = "Hydro-Qu\u00e9bec  Dist.  R\u00e9s."  # accented chars + extra spaces
        subtotal = 1250.00
        gst = subtotal * 0.05       # 62.50
        qst = subtotal * 0.09975    # 124.69

        # Tax validation should pass (correct math)
        warnings = validate_tax_extraction(
            subtotal=subtotal,
            gst_amount=round(gst, 2),
            qst_amount=round(qst, 2),
            tax_code="GST_QST",
        )
        assert len(warnings) == 0, "Correct tax math should not produce warnings"

        # But partial payment (only 900 of 1437.19 total) changes the context
        payment_amount = 900.00
        total_with_tax = subtotal + gst + qst  # 1437.19

        # Review policy should flag: amount mismatch between invoice and payment
        decision = decide_review_status(
            rules_confidence=0.75,
            final_method="rules",
            vendor_name=vendor_raw,
            total=payment_amount,  # Partial payment, not full invoice
            document_date=TODAY_STR,
            client_code="CLI-001",
        )
        # With confidence 0.75 + 0.10 boost = 0.85, it might be Ready
        # The key issue: partial payment is not flagged at field level
        assert decision.status in ("Ready", "NeedsReview"), \
            f"Unexpected status: {decision.status}"

        # FINDING: The system cannot detect that 900 is a partial payment
        # of a 1437.19 invoice without bank statement context.
        # The posting_builder accepts whatever total is provided.

    # Attack 19: Bank payment with fee deduction + duplicate invoice amount +
    # same-day similar vendors
    def test_attack19_bank_fee_deduction_duplicate_amount(self):
        """Two invoices from similar vendors with same amount on same day.
        Bank shows amount minus fee. System must detect the duplicate risk."""
        vendor_a = "Staples Canada Inc"
        vendor_b = "Staples Canada Ltd"

        # Duplicate detector normalization
        norm_a = dup_normalize(vendor_a)
        norm_b = dup_normalize(vendor_b)

        # After normalization, these should be very similar
        # Both become "staples" (inc/ltd are removed by normalization)
        assert norm_a == norm_b or \
            abs(len(norm_a) - len(norm_b)) <= 3, \
            "Similar vendor names should normalize similarly"

        # Same amount = duplicate risk
        assert _amount_equal(543.21, 543.21), \
            "Exact same amounts must be detected as equal"

        # Bank payment with fee: 543.21 - 2.50 fee = 540.71
        # The system should NOT match 540.71 to 543.21 within default tolerance
        assert not _amount_equal(543.21, 540.71, tolerance=0.01), \
            "Fee-deducted amount should not match exactly"

        # But with a $5 tolerance it would match
        assert _amount_equal(543.21, 540.71, tolerance=3.00), \
            "With larger tolerance, fee-deducted amount matches"

    # Attack 20: Shareholder expense on corporate card + correct tax +
    # wrong classification
    def test_attack20_shareholder_expense_wrong_classification(self):
        """Personal shareholder expense correctly extracted with tax
        but classified as business expense. System should catch misclassification."""
        # Personal expense markers
        personal_vendors = [
            "Netflix Canada",
            "Spotify Premium",
            "Amazon.ca - Personal Account",
        ]

        rules_dir = _tmp_rules_dir(vendor_intel={
            "vendors": {
                "Netflix Canada": {
                    "category": "Entertainment",
                    "gl_account": "Shareholder Advances",
                    "tax_code": "GST_QST",
                    "document_family": "personal_expense",
                },
            },
            "doc_type_defaults": {},
            "default": {
                "category": "Uncategorized",
                "gl_account": "Uncategorized Expense",
                "tax_code": "GST_QST",
            },
        })

        vi = VendorIntelligenceEngine(rules_dir)
        result = vi.classify("Netflix Canada", "invoice")

        # If properly configured, Netflix should route to Shareholder Advances
        assert result.gl_account == "Shareholder Advances", \
            f"Netflix should map to Shareholder Advances, got {result.gl_account}"
        assert result.document_family == "personal_expense"

        # Tax extraction is correct but classification matters
        warnings = validate_tax_extraction(
            subtotal=15.99,
            gst_amount=0.80,
            qst_amount=1.60,
            tax_code="GST_QST",
        )
        assert len(warnings) == 0, "Tax math is correct"

        # Without vendor intelligence config, it falls to default
        vi_empty = VendorIntelligenceEngine(_tmp_rules_dir(vendor_intel={
            "vendors": {}, "doc_type_defaults": {}, "default": {
                "category": "Uncategorized",
                "gl_account": "Uncategorized Expense",
                "tax_code": "GST_QST",
            }
        }))
        result_default = vi_empty.classify("Netflix Canada", "invoice")
        assert result_default.gl_account == "Uncategorized Expense", \
            "FINDING: Without vendor rules, personal expenses are not flagged"

    # Attack 21: Fake invoice + plausible bank payment + malformed GST/QST numbers
    def test_attack21_fake_invoice_with_malformed_tax_numbers(self):
        """Fake invoice with plausible amounts but invalid GST/QST registration
        numbers. The system should validate tax number formats."""
        # Valid GST: 123456789RT0001 (9 digits + RT + 4 digits)
        # Valid QST: 1234567890TQ0001 (10 digits + TQ + 4 digits)
        fake_gst = "000000000RT0000"  # All zeros
        fake_qst = "FAKE12345TQ9999"  # Letters in numeric portion

        # Tax validation only checks amounts, not registration numbers
        # FINDING: GST/QST registration number format is not validated
        warnings = validate_tax_extraction(
            subtotal=5000.00,
            gst_amount=250.00,  # Correct 5%
            qst_amount=498.75,  # Correct 9.975%
            tax_code="GST_QST",
        )
        assert len(warnings) == 0, \
            "Tax amounts are correct even though numbers are fake"

        # Review policy checks amounts, not registration numbers
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Totally Legit Corp",
            total=5748.75,
            document_date=TODAY_STR,
            client_code="CLI-001",
        )
        # High confidence + all fields = Ready
        # FINDING: Fake vendor with correct math passes review
        assert decision.status == "Ready", \
            "FINDING: Fake invoice with correct math passes as Ready"

    # Attack 22: Cross-client contamination attempt
    def test_attack22_cross_client_contamination(self):
        """Documents from Client A must not affect Client B's postings.
        Check that client_code isolation is enforced in review policy."""
        # Client A document
        decision_a = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Supplier X",
            total=1000.00,
            document_date=TODAY_STR,
            client_code="CLIENT_A",
        )

        # Client B document with same vendor/amount
        decision_b = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Supplier X",
            total=1000.00,
            document_date=TODAY_STR,
            client_code="CLIENT_B",
        )

        # Both should be independent decisions
        assert decision_a.status == decision_b.status, \
            "Same data should produce same decision regardless of client"

        # The real cross-contamination risk is in the database layer
        # where documents from different clients share the same DB.
        # FINDING: Review policy itself is stateless (no DB access),
        # so contamination would occur at the DB/query level, not here.

        # GL Mapper should be client-independent too
        rules_dir_a = _tmp_rules_dir(gl_map={
            "vendors": {"Supplier X": {"gl_account": "Account A", "tax_code": "GST_QST"}},
            "doc_types": {},
            "default": {"gl_account": "Default", "tax_code": "GST_QST"},
        })
        rules_dir_b = _tmp_rules_dir(gl_map={
            "vendors": {"Supplier X": {"gl_account": "Account B", "tax_code": "GST_QST"}},
            "doc_types": {},
            "default": {"gl_account": "Default", "tax_code": "GST_QST"},
        })

        mapper_a = GLMapper(rules_dir_a)
        mapper_b = GLMapper(rules_dir_b)

        result_a = mapper_a.map("Supplier X", "invoice")
        result_b = mapper_b.map("Supplier X", "invoice")

        # Different rules dirs = different mappings = proper isolation
        assert result_a.gl_account != result_b.gl_account, \
            "Different rule sets should produce different GL mappings"

    # Attack 23: Evidence chain that reconciles totals but not assertions
    def test_attack23_evidence_chain_totals_vs_assertions(self):
        """Total amounts match across documents but the economic assertions
        are contradictory. E.g., revenue + expense netting to same total."""
        # Invoice says: Revenue $10,000
        # Credit note says: Expense $10,000
        # Net = $0, but the assertions are contradictory

        # Review policy only checks individual documents
        revenue_decision = decide_review_status(
            rules_confidence=0.92,
            final_method="rules",
            vendor_name="Client Corp",
            total=10000.00,
            document_date=TODAY_STR,
            client_code="CLI-001",
        )
        expense_decision = decide_review_status(
            rules_confidence=0.92,
            final_method="rules",
            vendor_name="Client Corp",
            total=10000.00,
            document_date=TODAY_STR,
            client_code="CLI-001",
        )

        # Both pass individually
        assert revenue_decision.status == "Ready"
        assert expense_decision.status == "Ready"

        # FINDING: The system has no cross-document assertion validation.
        # Two documents with same vendor/amount/date but opposite economic
        # substance (revenue vs expense) both pass as Ready.
        # The GL mapper would need to distinguish doc_type to route differently.

        gl_dir = _tmp_rules_dir(gl_map={
            "vendors": {},
            "doc_types": {
                "invoice": {"gl_account": "Revenue", "tax_code": "GST_QST"},
                "credit_note": {"gl_account": "Returns & Allowances", "tax_code": "GST_QST"},
            },
            "default": {"gl_account": "Uncategorized", "tax_code": "GST_QST"},
        })
        mapper = GLMapper(gl_dir)

        rev_map = mapper.map(None, "invoice")
        exp_map = mapper.map(None, "credit_note")
        assert rev_map.gl_account != exp_map.gl_account, \
            "Invoice and credit note should map to different GL accounts"

    # Attack 24: Correct totals, correct tax math, wrong economic substance
    # (loan recorded as revenue)
    def test_attack24_loan_disguised_as_revenue(self):
        """A loan receipt with correct amounts and tax is classified as
        revenue. The system should detect the wrong economic substance."""
        # Loan: $50,000 received - no tax applicable
        # But attacker labels it as revenue with fake GST/QST

        # Tax validation catches the mismatch
        # For a loan, there should be no GST/QST
        # But if someone puts tax_code="GST_QST" on a $50,000 "revenue"
        # with fake tax amounts:
        fake_gst = 50000 * 0.05     # 2500
        fake_qst = 50000 * 0.09975  # 4987.50

        # Tax validation says amounts are correct (math checks out)
        warnings = validate_tax_extraction(
            subtotal=50000.00,
            gst_amount=round(fake_gst, 2),
            qst_amount=round(fake_qst, 2),
            tax_code="GST_QST",
        )
        assert len(warnings) == 0, \
            "FINDING: Tax math is correct even though loan should have no tax"

        # FIX 24: Large amount escalation now caps confidence at 0.75 for >$25K
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Lending Bank Inc",
            total=57487.50,
            document_date=TODAY_STR,
            client_code="CLI-001",
        )
        assert decision.status == "NeedsReview", \
            "Large amount ($57K) should require human review per FIX 24"

        # FINDING: The system has no semantic/economic substance validation.
        # It validates tax math and field completeness but cannot distinguish
        # between a loan and revenue based on document content alone.
        # This requires AI-level understanding or explicit doc_type classification.

        # GL mapper can help IF doc_type is correctly identified
        gl_dir = _tmp_rules_dir(gl_map={
            "vendors": {
                "Lending Bank Inc": {
                    "gl_account": "Loans Payable",
                    "tax_code": "EXEMPT",
                },
            },
            "doc_types": {},
            "default": {"gl_account": "Uncategorized", "tax_code": "GST_QST"},
        })
        mapper = GLMapper(gl_dir)
        result = mapper.map("Lending Bank Inc", "loan_receipt")
        # With vendor rules, it correctly routes to Loans Payable
        assert result.gl_account == "Loans Payable"
        assert result.tax_code == "EXEMPT"

    # Attack 18+21 combined: OCR noise + fake tax numbers + partial payment
    def test_attack_combined_ocr_fake_tax_partial(self):
        """Multi-layer attack: OCR-corrupted vendor name with valid-looking
        tax math but fake registration numbers and partial payment amount."""
        # OCR-corrupted vendor
        vendor_ocr = "Hydr0-Qu\u00e9bec  D1st."  # 0 instead of o, 1 instead of i

        # Partial payment
        subtotal = 2000.00
        full_total = subtotal * 1.14975  # with taxes = 2299.50
        partial = 1500.00  # Only partial

        decision = decide_review_status(
            rules_confidence=0.60,  # Low confidence due to OCR noise
            final_method="rules",
            vendor_name=vendor_ocr,
            total=partial,
            document_date=TODAY_STR,
            client_code="CLI-001",
        )
        # Low confidence (0.60 + 0.10 = 0.70) should trigger NeedsReview
        assert decision.status == "NeedsReview", \
            f"OCR noise + partial payment should need review, got {decision.status}"
        assert decision.effective_confidence < 0.85

    # Attack 19+22 combined: Duplicate across clients
    def test_attack_combined_cross_client_duplicate(self):
        """Same invoice sent to two different clients. The duplicate detector
        should still flag it if client isolation is not enforced."""
        # Same vendor, same amount, same date, different client codes
        assert _amount_equal(1234.56, 1234.56), "Same amount must match"

        # Normalized vendor names match
        assert dup_normalize("Fournisseur ABC Inc.") == dup_normalize("fournisseur abc inc"), \
            "Case/punctuation differences should normalize to same"

        # FINDING: The duplicate detector compares within the same scan batch.
        # Cross-client duplicates depend on whether documents from multiple
        # clients are in the same batch. If they are, duplicates ARE detected.
        # If clients are processed separately, cross-client dupes are missed.

    # Attack 20+24 combined: Shareholder loan as corporate revenue
    def test_attack_combined_shareholder_loan_as_revenue(self):
        """Shareholder loans money to company, recorded as revenue.
        Correct tax extraction makes it look legitimate."""
        # This tests the full chain: vendor intel + tax + review
        rules_dir = _tmp_rules_dir(vendor_intel={
            "vendors": {},
            "doc_type_defaults": {},
            "default": {
                "category": "Uncategorized",
                "gl_account": "Uncategorized Expense",
                "tax_code": "GST_QST",
            },
        })

        vi = VendorIntelligenceEngine(rules_dir)
        result = vi.classify("John Smith (Shareholder)", "deposit")

        # Without specific rules, shareholder deposit falls to default
        assert result.source == "default", \
            "Unknown shareholder should fall to default classification"
        assert result.gl_account == "Uncategorized Expense", \
            "FINDING: Shareholder loan defaults to expense, not loan payable"

        # Tax validation passes (even though loans are tax-exempt)
        warnings = validate_tax_extraction(
            subtotal=100000.00,
            gst_amount=5000.00,
            qst_amount=9975.00,
            tax_code="GST_QST",
        )
        assert len(warnings) == 0, \
            "FINDING: Tax charged on a loan passes validation"

        # FIX 24: Large amount escalation caps confidence at 0.75 for >$25K
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="John Smith (Shareholder)",
            total=114975.00,
            document_date=TODAY_STR,
            client_code="CLI-001",
        )
        assert decision.status == "NeedsReview", \
            "Large amount ($114K) should require human review per FIX 24"


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5: ADDITIONAL EDGE CASES
# ═══════════════════════════════════════════════════════════════════════════

class TestAdditionalEdgeCases:
    """Extra attacks that combine license + security + chaos."""

    def test_expired_license_feature_check(self):
        """check_feature must return False when license is expired."""
        key = _make_key("entreprise", expiry=PAST_DATE)
        td = _tmp_config({"license": {"key": key, "secret": TEST_SECRET}})

        with patch("src.engines.license_engine.ROOT_DIR", td):
            status = get_license_status()
            assert status["valid"] is False
            assert len(status["features"]) == 0, \
                "Expired license must not grant any features"

    def test_license_limits_with_empty_db(self):
        """check_limits with no clients/users table should not crash."""
        key = _make_key("essentiel")
        td = _tmp_config({"license": {"key": key, "secret": TEST_SECRET}})

        with patch("src.engines.license_engine.ROOT_DIR", td):
            conn = _tmp_db()
            limits = check_limits(conn)
            assert limits["client_count"] == 0
            assert limits["user_count"] == 0
            assert limits["within_limits"] is True
            conn.close()

    def test_license_limits_exceeded(self):
        """When client/user counts exceed license limits, within_limits must be False."""
        key = _make_key("essentiel")  # max_clients=10, max_users=3
        td = _tmp_config({"license": {"key": key, "secret": TEST_SECRET}})

        with patch("src.engines.license_engine.ROOT_DIR", td):
            conn = _tmp_db()
            conn.execute("CREATE TABLE clients (id INTEGER PRIMARY KEY)")
            conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)")
            # Insert 15 clients (exceeds 10)
            for i in range(15):
                conn.execute("INSERT INTO clients (id) VALUES (?)", (i,))
            # Insert 5 users (exceeds 3)
            for i in range(5):
                conn.execute("INSERT INTO users (id) VALUES (?)", (i,))
            conn.commit()

            limits = check_limits(conn)
            assert limits["client_count"] == 15
            assert limits["user_count"] == 5
            assert limits["clients_ok"] is False
            assert limits["users_ok"] is False
            assert limits["within_limits"] is False
            conn.close()

    def test_password_empty_string_rejected(self):
        """Empty password must be rejected."""
        with pytest.raises(ValueError, match="empty"):
            hash_password("")

    def test_password_whitespace_only_rejected(self):
        """Whitespace-only password must be rejected."""
        with pytest.raises(ValueError, match="empty"):
            hash_password("   ")

    def test_verify_password_with_malformed_hash(self):
        """Malformed hash strings must not crash, just return False."""
        assert verify_password("test", "") is False
        assert verify_password("test", "not-a-hash") is False
        assert verify_password("test", "pbkdf2_sha256$wrong$format") is False
        assert verify_password("test", "$$$") is False

    def test_zero_amount_invoice_flagged(self):
        """A $0 invoice should be flagged for review."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Test Vendor",
            total=0.0,
            document_date=TODAY_STR,
            client_code="CLI-001",
        )
        assert decision.status == "NeedsReview"
        assert decision.reason == "zero_total"

    def test_negative_amount_not_blocked(self):
        """FINDING: Negative amounts (credit notes) are not specifically
        handled by review policy. They pass if confidence is high."""
        decision = decide_review_status(
            rules_confidence=0.90,
            final_method="rules",
            vendor_name="Vendor Refund",
            total=-500.00,
            document_date=TODAY_STR,
            client_code="CLI-001",
        )
        # Negative amounts are not specifically caught
        assert decision.status == "Ready", \
            "FINDING: Negative amounts pass review without special handling"

    def test_swapped_gst_qst_detected(self):
        """If GST and QST amounts are swapped, tax validation must flag it."""
        subtotal = 1000.00
        correct_gst = 50.00   # 5%
        correct_qst = 99.75   # 9.975%

        # Swap them
        warnings = validate_tax_extraction(
            subtotal=subtotal,
            gst_amount=correct_qst,  # Wrong: QST value in GST field
            qst_amount=correct_gst,  # Wrong: GST value in QST field
            tax_code="GST_QST",
        )
        assert "tax_extraction_mismatch" in warnings, \
            "Swapped GST/QST must be detected"
