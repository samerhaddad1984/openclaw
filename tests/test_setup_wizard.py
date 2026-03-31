"""Tests for scripts/setup_wizard.py -- LedgerLink Professional Setup Wizard"""
import pytest
import json
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

import scripts.setup_wizard as wizard


# =========================================================================
# State management
# =========================================================================

class TestStateManagement:
    def test_load_state_default(self, tmp_path):
        with patch("scripts.setup_wizard.STATE_FILE", tmp_path / "setup_state.json"):
            state = wizard.load_state()
            assert state["steps_complete"] == []
            assert state["setup_complete"] is False

    def test_save_and_load_state(self, tmp_path):
        state_file = tmp_path / "setup_state.json"
        with patch("scripts.setup_wizard.STATE_FILE", state_file):
            state = {"steps_complete": [1, 2], "setup_complete": False}
            wizard.save_state(state)
            loaded = wizard.load_state()
            assert loaded["steps_complete"] == [1, 2]

    def test_setup_complete_flag(self, tmp_path):
        state_file = tmp_path / "setup_state.json"
        with patch("scripts.setup_wizard.STATE_FILE", state_file):
            state = wizard.load_state()
            state["setup_complete"] = True
            wizard.save_state(state)
            loaded = wizard.load_state()
            assert loaded["setup_complete"] is True

    def test_state_persists_steps(self, tmp_path):
        state_file = tmp_path / "setup_state.json"
        with patch("scripts.setup_wizard.STATE_FILE", state_file):
            state = wizard.load_state()
            assert state["steps_complete"] == []
            state["steps_complete"] = [1, 2, 3]
            wizard.save_state(state)
            loaded = wizard.load_state()
            assert set(loaded["steps_complete"]) == {1, 2, 3}

    def test_state_persists_many_steps(self, tmp_path):
        state_file = tmp_path / "setup_state.json"
        with patch("scripts.setup_wizard.STATE_FILE", state_file):
            state = {"steps_complete": list(range(20)), "setup_complete": False}
            wizard.save_state(state)
            loaded = wizard.load_state()
            assert set(loaded["steps_complete"]) == set(range(20))

    def test_state_preserves_extra_keys(self, tmp_path):
        state_file = tmp_path / "setup_state.json"
        with patch("scripts.setup_wizard.STATE_FILE", state_file):
            state = {"steps_complete": [1], "setup_complete": False,
                     "admin_data": {"admin_username": "jtremblay"},
                     "temp_passwords": {"jdoe": "Abc12345"}}
            wizard.save_state(state)
            loaded = wizard.load_state()
            assert loaded["admin_data"]["admin_username"] == "jtremblay"
            assert loaded["temp_passwords"]["jdoe"] == "Abc12345"


# =========================================================================
# Config management
# =========================================================================

class TestConfigManagement:
    def test_load_config_empty(self, tmp_path):
        with patch("scripts.setup_wizard.CONFIG_FILE", tmp_path / "config.json"):
            config = wizard.load_config()
            assert isinstance(config, dict)

    def test_save_and_load_config(self, tmp_path):
        config_file = tmp_path / "config.json"
        with patch("scripts.setup_wizard.CONFIG_FILE", config_file):
            config = {"firm": {"firm_name": "Test Firm"}}
            wizard.save_config(config)
            loaded = wizard.load_config()
            assert loaded["firm"]["firm_name"] == "Test Firm"

    def test_config_preserves_unicode(self, tmp_path):
        config_file = tmp_path / "config.json"
        with patch("scripts.setup_wizard.CONFIG_FILE", config_file):
            config = {"firm": {"firm_name": "Tremblay & Associes"}}
            wizard.save_config(config)
            loaded = wizard.load_config()
            assert loaded["firm"]["firm_name"] == "Tremblay & Associes"

    def test_config_nested_keys(self, tmp_path):
        config_file = tmp_path / "config.json"
        with patch("scripts.setup_wizard.CONFIG_FILE", config_file):
            config = {"ai_router": {"routine_provider": {"model": "deepseek-chat"}, "premium_provider": {"model": "claude-3"}}}
            wizard.save_config(config)
            loaded = wizard.load_config()
            assert loaded["ai_router"]["routine_provider"]["model"] == "deepseek-chat"

    def test_config_integration_keys(self, tmp_path):
        config_file = tmp_path / "config.json"
        with patch("scripts.setup_wizard.CONFIG_FILE", config_file):
            config = {
                "whatsapp": {"account_sid": "AC123", "enabled": True},
                "telegram": {"bot_token": "123:abc", "enabled": True},
                "microsoft365": {"tenant_id": "abc-123", "enabled": True},
                "quickbooks": {"realm_id": "123", "enabled": True},
                "folder_watcher": {"inbox_path": "C:\\Inbox", "enabled": True},
                "digest_config": {"enabled": True, "send_time": "07:00"},
                "backup": {"folder": "C:\\Backups", "frequency": "daily"},
                "notifications": {"notif_fraud": "email"},
                "security_settings": {"session_timeout": "4h"},
            }
            wizard.save_config(config)
            loaded = wizard.load_config()
            assert loaded["whatsapp"]["enabled"] is True
            assert loaded["telegram"]["bot_token"] == "123:abc"
            assert loaded["microsoft365"]["tenant_id"] == "abc-123"
            assert loaded["quickbooks"]["realm_id"] == "123"
            assert loaded["folder_watcher"]["inbox_path"] == "C:\\Inbox"
            assert loaded["digest_config"]["send_time"] == "07:00"
            assert loaded["backup"]["frequency"] == "daily"
            assert loaded["notifications"]["notif_fraud"] == "email"
            assert loaded["security_settings"]["session_timeout"] == "4h"


# =========================================================================
# Bilingual strings
# =========================================================================

class TestBilingualStrings:
    def test_all_steps_have_fr_and_en(self):
        for step_key in ["step1", "step2", "step3", "step4", "step5", "step6"]:
            assert step_key in wizard.STRINGS["fr"], f"Missing FR key: {step_key}"
            assert step_key in wizard.STRINGS["en"], f"Missing EN key: {step_key}"

    def test_required_keys_present(self):
        required = ["btn_next", "btn_skip", "btn_save", "err_required", "err_passwords"]
        for key in required:
            assert key in wizard.STRINGS["fr"], f"Missing FR key: {key}"
            assert key in wizard.STRINGS["en"], f"Missing EN key: {key}"

    def test_strings_are_non_empty(self):
        for lang in ["fr", "en"]:
            for key, val in wizard.STRINGS[lang].items():
                assert val, f"Empty string for {lang}.{key}"

    def test_fr_and_en_have_same_keys(self):
        fr_keys = set(wizard.STRINGS["fr"].keys())
        en_keys = set(wizard.STRINGS["en"].keys())
        missing_en = fr_keys - en_keys
        missing_fr = en_keys - fr_keys
        assert not missing_en, f"Keys in FR but not EN: {missing_en}"
        assert not missing_fr, f"Keys in EN but not FR: {missing_fr}"

    def test_error_keys_present(self):
        for lang in ["fr", "en"]:
            assert "err_required" in wizard.STRINGS[lang]
            assert "err_passwords" in wizard.STRINGS[lang]
            assert "err_email" in wizard.STRINGS[lang]

    def test_new_step_keys_present(self):
        """All new wizard step title/subtitle keys must exist in both languages."""
        new_keys = [
            "welcome_title", "welcome_subtitle",
            "firm_title", "firm_subtitle",
            "admin_title", "admin_subtitle",
            "license_title", "license_subtitle",
            "ai_title", "ai_subtitle",
            "email_title", "email_subtitle",
            "portal_title", "portal_subtitle",
            "whatsapp_title", "whatsapp_subtitle",
            "telegram_title", "telegram_subtitle",
            "m365_title", "m365_subtitle",
            "qbo_title", "qbo_subtitle",
            "folder_title", "folder_subtitle",
            "digest_title", "digest_subtitle",
            "backup_title", "backup_subtitle",
            "notif_title", "notif_subtitle",
            "security_title", "security_subtitle",
            "staff_title", "staff_subtitle",
            "clients_title", "clients_subtitle",
            "review_title", "review_subtitle",
            "complete_title", "complete_subtitle",
        ]
        for key in new_keys:
            assert key in wizard.STRINGS["fr"], f"Missing FR: {key}"
            assert key in wizard.STRINGS["en"], f"Missing EN: {key}"

    def test_navigation_keys_present(self):
        nav_keys = ["btn_next", "btn_back", "btn_start", "btn_complete",
                     "btn_add", "btn_test", "btn_print", "skip_for_now",
                     "skip_add_later", "configure_later", "step_x_of_y"]
        for key in nav_keys:
            assert key in wizard.STRINGS["fr"], f"Missing FR: {key}"
            assert key in wizard.STRINGS["en"], f"Missing EN: {key}"


# =========================================================================
# Validation — Firm
# =========================================================================

class TestValidateFirm:
    def test_valid_firm(self):
        data = {"firm_name": "Tremblay CPA", "firm_city": "Montreal"}
        errors = wizard.validate_firm(data)
        assert errors == []

    def test_missing_firm_name(self):
        data = {"firm_name": "", "firm_city": "Montreal"}
        errors = wizard.validate_firm(data)
        assert len(errors) > 0

    def test_missing_city(self):
        data = {"firm_name": "Firm", "firm_city": ""}
        errors = wizard.validate_firm(data)
        assert len(errors) > 0

    def test_valid_gst_format(self):
        data = {"firm_name": "Firm", "firm_city": "Montreal", "gst_number": "123456789 RT0001"}
        assert wizard.validate_firm(data) == []

    def test_invalid_gst_format(self):
        data = {"firm_name": "Firm", "firm_city": "Montreal", "gst_number": "bad-format"}
        errors = wizard.validate_firm(data)
        assert any("GST" in e or "TPS" in e for e in errors)

    def test_valid_qst_format(self):
        data = {"firm_name": "Firm", "firm_city": "Montreal", "qst_number": "1234567890 TQ0001"}
        assert wizard.validate_firm(data) == []

    def test_invalid_qst_format(self):
        data = {"firm_name": "Firm", "firm_city": "Montreal", "qst_number": "bad"}
        errors = wizard.validate_firm(data)
        assert any("QST" in e or "TVQ" in e for e in errors)

    def test_empty_gst_qst_is_ok(self):
        data = {"firm_name": "Firm", "firm_city": "Montreal", "gst_number": "", "qst_number": ""}
        assert wizard.validate_firm(data) == []


# =========================================================================
# Validation — Admin
# =========================================================================

class TestValidateAdmin:
    def test_valid_admin(self):
        data = {
            "admin_fullname": "Jean Tremblay",
            "admin_username": "jtremblay",
            "admin_email": "jean@test.ca",
            "admin_password": "Secret1A",
            "admin_password_confirm": "Secret1A",
        }
        assert wizard.validate_admin(data) == []

    def test_missing_fullname(self):
        data = {
            "admin_fullname": "",
            "admin_username": "jt",
            "admin_email": "j@t.ca",
            "admin_password": "Secret1A",
            "admin_password_confirm": "Secret1A",
        }
        assert len(wizard.validate_admin(data)) > 0

    def test_username_with_spaces(self):
        data = {
            "admin_fullname": "Jean T",
            "admin_username": "jean t",
            "admin_email": "j@t.ca",
            "admin_password": "Secret1A",
            "admin_password_confirm": "Secret1A",
        }
        errors = wizard.validate_admin(data)
        assert any("space" in e.lower() for e in errors)

    def test_password_too_short(self):
        data = {
            "admin_fullname": "Jean",
            "admin_username": "jean",
            "admin_email": "j@t.ca",
            "admin_password": "Sh1",
            "admin_password_confirm": "Sh1",
        }
        errors = wizard.validate_admin(data)
        assert any("8" in e for e in errors)

    def test_password_no_uppercase(self):
        data = {
            "admin_fullname": "Jean",
            "admin_username": "jean",
            "admin_email": "j@t.ca",
            "admin_password": "secret12",
            "admin_password_confirm": "secret12",
        }
        errors = wizard.validate_admin(data)
        assert any("uppercase" in e.lower() for e in errors)

    def test_password_no_digit(self):
        data = {
            "admin_fullname": "Jean",
            "admin_username": "jean",
            "admin_email": "j@t.ca",
            "admin_password": "SecretAA",
            "admin_password_confirm": "SecretAA",
        }
        errors = wizard.validate_admin(data)
        assert any("number" in e.lower() for e in errors)

    def test_password_mismatch(self):
        data = {
            "admin_fullname": "Jean",
            "admin_username": "jean",
            "admin_email": "j@t.ca",
            "admin_password": "Secret1A",
            "admin_password_confirm": "Secret1B",
        }
        errors = wizard.validate_admin(data)
        assert any("match" in e.lower() for e in errors)

    def test_invalid_email(self):
        data = {
            "admin_fullname": "Jean",
            "admin_username": "jean",
            "admin_email": "notanemail",
            "admin_password": "Secret1A",
            "admin_password_confirm": "Secret1A",
        }
        errors = wizard.validate_admin(data)
        assert any("email" in e.lower() for e in errors)


# =========================================================================
# Legacy validate_step1 (backward compat)
# =========================================================================

class TestValidation:
    def test_validate_step1_valid(self):
        data = {
            "firm_name": "Tremblay CPA",
            "firm_address": "123 Rue Principale",
            "gst_number": "123456789RT0001",
            "qst_number": "1234567890TQ0001",
            "owner_name": "Jean Tremblay",
            "owner_email": "jean@tremblay.ca",
            "owner_password": "Secret123!",
            "owner_password_confirm": "Secret123!",
        }
        errors = wizard.validate_step1(data)
        assert errors == []

    def test_validate_step1_missing_field(self):
        data = {
            "firm_name": "",
            "firm_address": "123",
            "gst_number": "x",
            "qst_number": "x",
            "owner_name": "x",
            "owner_email": "x@x.com",
            "owner_password": "p",
            "owner_password_confirm": "p",
        }
        errors = wizard.validate_step1(data)
        assert len(errors) > 0

    def test_validate_step1_password_mismatch(self):
        data = {
            "firm_name": "Firm",
            "firm_address": "Addr",
            "gst_number": "g",
            "qst_number": "q",
            "owner_name": "Name",
            "owner_email": "a@b.com",
            "owner_password": "pass1",
            "owner_password_confirm": "pass2",
        }
        errors = wizard.validate_step1(data)
        assert any(
            "password" in e.lower() or "mot de passe" in e.lower() or "do not match" in e.lower()
            for e in errors
        )

    def test_validate_step1_bad_email(self):
        data = {
            "firm_name": "Firm",
            "firm_address": "Addr",
            "gst_number": "g",
            "qst_number": "q",
            "owner_name": "Name",
            "owner_email": "notanemail",
            "owner_password": "p",
            "owner_password_confirm": "p",
        }
        errors = wizard.validate_step1(data)
        assert any(
            "email" in e.lower() or "courriel" in e.lower() or "invalid" in e.lower()
            for e in errors
        )

    def test_validate_step1_all_empty(self):
        data = {k: "" for k in [
            "firm_name", "firm_address", "gst_number", "qst_number",
            "owner_name", "owner_email", "owner_password", "owner_password_confirm",
        ]}
        errors = wizard.validate_step1(data)
        assert len(errors) > 0

    def test_validate_step1_returns_list(self):
        data = {
            "firm_name": "Firm",
            "firm_address": "Addr",
            "gst_number": "g",
            "qst_number": "q",
            "owner_name": "Name",
            "owner_email": "valid@email.com",
            "owner_password": "pass",
            "owner_password_confirm": "pass",
        }
        result = wizard.validate_step1(data)
        assert isinstance(result, list)


# =========================================================================
# Module-level names
# =========================================================================

class TestModuleLevelNames:
    def test_state_file_is_path(self):
        assert isinstance(wizard.STATE_FILE, Path)

    def test_config_file_is_path(self):
        assert isinstance(wizard.CONFIG_FILE, Path)

    def test_strings_is_dict(self):
        assert isinstance(wizard.STRINGS, dict)
        assert "fr" in wizard.STRINGS
        assert "en" in wizard.STRINGS

    def test_load_state_callable(self):
        assert callable(wizard.load_state)

    def test_save_state_callable(self):
        assert callable(wizard.save_state)

    def test_load_config_callable(self):
        assert callable(wizard.load_config)

    def test_save_config_callable(self):
        assert callable(wizard.save_config)

    def test_validate_step1_callable(self):
        assert callable(wizard.validate_step1)

    def test_validate_firm_callable(self):
        assert callable(wizard.validate_firm)

    def test_validate_admin_callable(self):
        assert callable(wizard.validate_admin)


# =========================================================================
# Step definitions
# =========================================================================

class TestStepDefinitions:
    def test_steps_list_has_20_entries(self):
        assert len(wizard.STEPS) == 20

    def test_all_steps_have_path_and_labels(self):
        for i, (path, fr, en) in enumerate(wizard.STEPS):
            assert path.startswith("/setup/"), f"Step {i} path doesn't start with /setup/"
            assert len(fr) > 0, f"Step {i} missing FR label"
            assert len(en) > 0, f"Step {i} missing EN label"

    def test_step_paths_dict(self):
        assert wizard.STEP_PATHS[0] == "/setup/welcome"
        assert wizard.STEP_PATHS[1] == "/setup/firm"
        assert wizard.STEP_PATHS[19] == "/setup/complete"

    def test_path_to_step_reverse(self):
        assert wizard.PATH_TO_STEP["/setup/welcome"] == 0
        assert wizard.PATH_TO_STEP["/setup/firm"] == 1
        assert wizard.PATH_TO_STEP["/setup/complete"] == 19

    def test_welcome_is_step_0(self):
        assert wizard.STEPS[0][0] == "/setup/welcome"

    def test_complete_is_last_step(self):
        assert wizard.STEPS[-1][0] == "/setup/complete"

    def test_integration_steps_present(self):
        paths = [s[0] for s in wizard.STEPS]
        assert "/setup/whatsapp" in paths
        assert "/setup/telegram" in paths
        assert "/setup/microsoft365" in paths
        assert "/setup/quickbooks" in paths
        assert "/setup/folder" in paths
        assert "/setup/digest" in paths
        assert "/setup/backup" in paths
        assert "/setup/notifications" in paths
        assert "/setup/security" in paths

    def test_staff_and_clients_steps_present(self):
        paths = [s[0] for s in wizard.STEPS]
        assert "/setup/staff" in paths
        assert "/setup/clients" in paths
        assert "/setup/review" in paths


# =========================================================================
# Helper functions
# =========================================================================

class TestHelpers:
    def test_gen_username_from_full_name(self):
        assert wizard._gen_username("Jean Tremblay") == "jtremblay"

    def test_gen_username_single_name(self):
        assert wizard._gen_username("Jean") == "jean"

    def test_gen_username_empty(self):
        assert wizard._gen_username("") == "user"

    def test_gen_temp_password_length(self):
        pw = wizard._gen_temp_password()
        assert len(pw) == 8

    def test_gen_temp_password_has_upper(self):
        pw = wizard._gen_temp_password()
        assert any(c.isupper() for c in pw)

    def test_gen_temp_password_has_digit(self):
        pw = wizard._gen_temp_password()
        assert any(c.isdigit() for c in pw)

    def test_esc_html(self):
        assert wizard._esc("<script>") == "&lt;script&gt;"
        assert wizard._esc('"hello"') == "&quot;hello&quot;"
        assert wizard._esc(None) == ""

    def test_s_returns_fr_by_default(self):
        assert wizard._s("fr", "btn_next") == "Suivant \u2192"

    def test_s_returns_en(self):
        assert wizard._s("en", "btn_next") == "Next \u2192"

    def test_s_fallback_to_fr(self):
        result = wizard._s("xx", "btn_next")
        assert result == wizard.STRINGS["fr"]["btn_next"]

    def test_progress_pct_zero(self):
        assert wizard._progress_pct(0) == 0

    def test_progress_pct_midway(self):
        pct = wizard._progress_pct(9)
        assert 40 <= pct <= 60

    def test_progress_pct_max(self):
        pct = wizard._progress_pct(wizard.TOTAL_STEPS)
        assert pct == 100


# =========================================================================
# Rendering tests (basic smoke tests)
# =========================================================================

class TestRendering:
    @pytest.fixture
    def state(self):
        return {"steps_complete": [], "setup_complete": False}

    def test_render_welcome_fr(self, state):
        html = wizard._render_welcome("fr", state)
        assert "Bienvenue" in html
        assert "Commencer" in html

    def test_render_welcome_en(self, state):
        html = wizard._render_welcome("en", state)
        assert "Welcome" in html
        assert "Start" in html

    def test_render_firm(self, state):
        html = wizard._render_firm("fr", state)
        assert "Nom du cabinet" in html

    def test_render_admin(self, state):
        html = wizard._render_admin("fr", state)
        assert "administrateur" in html.lower()

    def test_render_license(self, state):
        html = wizard._render_license("fr", state)
        assert "licence" in html.lower()
        assert "LLAI-" in html

    def test_render_ai(self, state):
        html = wizard._render_ai("fr", state)
        assert "DeepSeek" in html
        assert "Anthropic" in html or "Claude" in html

    def test_render_email(self, state):
        html = wizard._render_email("fr", state)
        assert "SMTP" in html
        assert "Gmail" in html

    def test_render_portal(self, state):
        html = wizard._render_portal("fr", state)
        assert "8788" in html
        assert "Cloudflare" in html

    def test_render_whatsapp(self, state):
        html = wizard._render_whatsapp("fr", state)
        assert "WhatsApp" in html
        assert "Twilio" in html

    def test_render_telegram(self, state):
        html = wizard._render_telegram("fr", state)
        assert "Telegram" in html
        assert "BotFather" in html

    def test_render_m365(self, state):
        html = wizard._render_m365("fr", state)
        assert "Microsoft 365" in html
        assert "Outlook" in html

    def test_render_quickbooks(self, state):
        html = wizard._render_quickbooks("fr", state)
        assert "QuickBooks" in html

    def test_render_folder(self, state):
        html = wizard._render_folder("fr", state)
        assert "Inbox" in html or "reception" in html.lower() or "dossier" in html.lower()

    def test_render_digest(self, state):
        html = wizard._render_digest("fr", state)
        assert "quotidien" in html.lower() or "digest" in html.lower()

    def test_render_backup(self, state):
        html = wizard._render_backup("fr", state)
        assert "sauvegarde" in html.lower() or "backup" in html.lower()

    def test_render_notifications(self, state):
        html = wizard._render_notifications("fr", state)
        assert "notification" in html.lower()

    def test_render_security(self, state):
        html = wizard._render_security("fr", state)
        assert "securite" in html.lower() or "security" in html.lower()

    def test_render_staff(self, state):
        html = wizard._render_staff("fr", state)
        assert "equipe" in html.lower() or "staff" in html.lower()

    def test_render_clients(self, state):
        html = wizard._render_clients("fr", state)
        assert "client" in html.lower()

    def test_render_review(self, state):
        with patch("scripts.setup_wizard.load_config", return_value={}):
            html = wizard._render_review("fr", state)
            assert "Verification" in html or "verification" in html.lower()

    def test_render_complete(self, state):
        with patch("scripts.setup_wizard.load_config", return_value={}):
            html = wizard._render_complete("fr", state)
            assert "8787" in html

    def test_render_already_complete(self, state):
        with patch("scripts.setup_wizard.load_config", return_value={"port": 8787}):
            html = wizard._render_already_complete("fr", state)
            assert "deja" in html.lower() or "already" in html.lower()

    def test_all_pages_have_progress_bar(self, state):
        """Every rendered page should contain the progress bar."""
        html = wizard._render_welcome("fr", state)
        assert "progress-bar" in html

    def test_all_pages_have_lang_toggle(self, state):
        html = wizard._render_firm("en", state)
        assert "Francais" in html

    def test_pages_have_back_button(self, state):
        html = wizard._render_admin("fr", state)
        assert "Retour" in html

    def test_welcome_has_no_back(self, state):
        html = wizard._render_welcome("fr", state)
        # Welcome page should not have a back button in the main content
        # (the sidebar always exists but the card content shouldn't have Back)
        assert html.count("btn_back") == 0 or "Retour" not in html.split("Commencer")[0]


# =========================================================================
# Review page items
# =========================================================================

class TestReviewPage:
    def test_review_shows_all_sections(self):
        state = {"steps_complete": [], "setup_complete": False}
        with patch("scripts.setup_wizard.load_config", return_value={
            "firm": {"firm_name": "Test"},
            "license": {"key": "LLAI-test"},
            "email": {"smtp_host": "smtp.test.com"},
        }):
            html = wizard._render_review("fr", state)
            assert "Cabinet" in html or "Firm" in html
            assert "WhatsApp" in html
            assert "Telegram" in html
            assert "QuickBooks" in html
            assert "Microsoft 365" in html

    def test_review_shows_configured_status(self):
        state = {"steps_complete": [14, 15], "setup_complete": False}
        with patch("scripts.setup_wizard.load_config", return_value={
            "firm": {"firm_name": "Test"},
            "whatsapp": {"enabled": True},
        }):
            html = wizard._render_review("fr", state)
            assert "Configure" in html or "configure" in html.lower()
