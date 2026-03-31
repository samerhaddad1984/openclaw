"""
tests/test_install_second_machine.py — Tests for the second-machine installer
"""
from __future__ import annotations

import json
import platform
import subprocess
import sys
import textwrap
from pathlib import Path
from unittest import mock

import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPT = ROOT_DIR / "scripts" / "install_second_machine.py"
CONFIG_FILE = ROOT_DIR / "otocpa.config.json"
DOCS_FILE = ROOT_DIR / "docs" / "SECOND_MACHINE_INSTALL.md"


# ── file existence ──────────────────────────────────────────────────────────

class TestFilesExist:
    def test_install_script_exists(self):
        assert SCRIPT.exists(), "scripts/install_second_machine.py must exist"

    def test_docs_exist(self):
        assert DOCS_FILE.exists(), "docs/SECOND_MACHINE_INSTALL.md must exist"

    def test_config_has_database_path(self):
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        assert "database_path" in cfg, "otocpa.config.json must contain database_path"
        assert cfg["database_path"] == "data/otocpa_agent.db"


# ── script imports cleanly ──────────────────────────────────────────────────

class TestScriptImports:
    def test_import_module(self):
        """The installer script should be importable without side-effects."""
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "install_second_machine", str(SCRIPT),
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        assert hasattr(mod, "main")
        assert hasattr(mod, "step_check_python")
        assert hasattr(mod, "step_install_deps")
        assert hasattr(mod, "step_migrate_db")
        assert hasattr(mod, "step_copy_config")
        assert hasattr(mod, "step_open_browser")


# ── step_check_python ───────────────────────────────────────────────────────

class TestCheckPython:
    def _load(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "install_second_machine", str(SCRIPT),
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod

    def test_passes_on_current_python(self):
        """Should not raise when Python >= 3.11."""
        mod = self._load()
        # Current test runner is >= 3.11, so this should pass
        mod.step_check_python()  # no exception = pass

    def test_fails_on_old_python(self):
        """Should sys.exit when Python is below 3.11."""
        mod = self._load()
        fake_ver = (3, 9, 0, "final", 0)
        with mock.patch.object(sys, "version_info", fake_ver):
            with pytest.raises(SystemExit):
                mod.step_check_python()


# ── step_copy_config ────────────────────────────────────────────────────────

class TestCopyConfig:
    def _load(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "install_second_machine", str(SCRIPT),
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod

    def test_copy_from_path(self, tmp_path):
        """Should copy config from a specified path."""
        mod = self._load()
        src = tmp_path / "source_config.json"
        src.write_text('{"test": true}', encoding="utf-8")

        dest = tmp_path / "otocpa.config.json"
        with mock.patch.object(mod, "CONFIG_FILE", dest):
            mod.step_copy_config(str(src))
        assert dest.exists()
        assert json.loads(dest.read_text(encoding="utf-8")) == {"test": True}

    def test_missing_source_exits(self, tmp_path):
        """Should sys.exit if the source config doesn't exist."""
        mod = self._load()
        with pytest.raises(SystemExit):
            mod.step_copy_config(str(tmp_path / "nonexistent.json"))

    def test_no_source_keeps_existing(self, tmp_path):
        """When no source is given and config exists, keep it."""
        mod = self._load()
        dest = tmp_path / "otocpa.config.json"
        dest.write_text('{"existing": true}', encoding="utf-8")
        with mock.patch.object(mod, "CONFIG_FILE", dest):
            mod.step_copy_config(None)
        assert json.loads(dest.read_text(encoding="utf-8")) == {"existing": True}


# ── step_install_deps ───────────────────────────────────────────────────────

class TestInstallDeps:
    def _load(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "install_second_machine", str(SCRIPT),
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod

    def test_skip_flag(self):
        """--skip-deps should not run any subprocess."""
        mod = self._load()
        with mock.patch.object(mod, "_run") as mock_run:
            mod.step_install_deps(skip=True)
        mock_run.assert_not_called()


# ── docs content ────────────────────────────────────────────────────────────

class TestDocsContent:
    @pytest.fixture(autouse=True)
    def _read_docs(self):
        self.content = DOCS_FILE.read_text(encoding="utf-8")

    def test_has_windows_section(self):
        assert "Windows" in self.content

    def test_has_mac_section(self):
        assert "macOS" in self.content or "Mac" in self.content

    def test_has_shared_database_section(self):
        assert "Sharing the Database" in self.content or "database" in self.content.lower()

    def test_has_license_transfer_section(self):
        assert "License" in self.content or "license" in self.content

    def test_has_config_copy_section(self):
        assert "config" in self.content.lower()

    def test_mentions_option_a_network_drive(self):
        assert "network" in self.content.lower()

    def test_mentions_option_b_browser_only(self):
        assert "browser" in self.content.lower()

    def test_mentions_option_c_backup_restore(self):
        assert "backup" in self.content.lower()

    def test_mentions_port_8787(self):
        assert "8787" in self.content

    def test_mentions_launchd(self):
        assert "launchd" in self.content or "launchctl" in self.content


# ── config file ─────────────────────────────────────────────────────────────

class TestConfigFile:
    def test_valid_json(self):
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        assert isinstance(cfg, dict)

    def test_database_path_value(self):
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        assert cfg["database_path"] == "data/otocpa_agent.db"

    def test_existing_keys_preserved(self):
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        assert cfg["host"] == "0.0.0.0"
        assert cfg["port"] == 8787
        assert "ai_router" in cfg
        assert "security" in cfg


# ── main() with --help ──────────────────────────────────────────────────────

class TestMainHelp:
    def test_help_flag(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--help"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0
        assert "second machine" in result.stdout.lower() or "config" in result.stdout.lower()


# ── launchd plist generation (mac) ──────────────────────────────────────────

class TestLaunchdPlist:
    def _load(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "install_second_machine", str(SCRIPT),
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod

    def test_plist_generation(self, tmp_path):
        """The mac auto-start step should write a valid plist file."""
        mod = self._load()
        plist_dir = tmp_path / "LaunchAgents"
        plist_dir.mkdir()

        with mock.patch.object(mod, "LAUNCHD_PLIST_DIR", plist_dir):
            mod.step_register_autostart_mac()

        plist_file = plist_dir / "com.otocpa.plist"
        assert plist_file.exists()
        content = plist_file.read_text(encoding="utf-8")
        assert "com.otocpa" in content
        assert "review_dashboard.py" in content
        assert "RunAtLoad" in content
