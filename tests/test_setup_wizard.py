"""Tests for scripts/setup_wizard.py"""
import pytest
import json
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

# Import wizard module functions
import scripts.setup_wizard as wizard


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
            config = {"firm": {"firm_name": "Tremblay & Associés"}}
            wizard.save_config(config)
            loaded = wizard.load_config()
            assert loaded["firm"]["firm_name"] == "Tremblay & Associés"

    def test_config_nested_keys(self, tmp_path):
        config_file = tmp_path / "config.json"
        with patch("scripts.setup_wizard.CONFIG_FILE", config_file):
            config = {"ai_router": {"routine": {"model": "deepseek-chat"}, "premium": {"model": "claude-3"}}}
            wizard.save_config(config)
            loaded = wizard.load_config()
            assert loaded["ai_router"]["routine"]["model"] == "deepseek-chat"


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


class TestModuleLevelNames:
    """Verify that all module-level names required by the spec are exported."""

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
