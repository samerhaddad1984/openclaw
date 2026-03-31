#!/usr/bin/env python3
"""
scripts/install_second_machine.py — Install OtoCPA on a second machine
==========================================================================
Cross-platform installer for deploying OtoCPA on a fresh Windows or Mac
machine.  Assumes you already have a working OtoCPA instance and want to
set up a second workstation.

Usage:
    python  scripts/install_second_machine.py            # Windows
    python3 scripts/install_second_machine.py            # Mac

Optional flags:
    --config PATH   Path to otocpa.config.json copied from first machine
    --skip-deps     Skip pip install of requirements
    --server-mode   Configure this machine as server (others connect via browser)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import textwrap
import webbrowser
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MIN_PYTHON = (3, 11)
DASHBOARD_PORT = 8787
ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT_DIR / "otocpa.config.json"
REQUIREMENTS_FILE = ROOT_DIR / "requirements.txt"
MIGRATE_SCRIPT = ROOT_DIR / "scripts" / "migrate_db.py"
SERVICE_WRAPPER = ROOT_DIR / "installer" / "service_wrapper.py"
LAUNCHD_PLIST_DIR = Path.home() / "Library" / "LaunchAgents"
LAUNCHD_PLIST_NAME = "com.otocpa.plist"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="  [%(levelname)s] %(message)s",
)
log = logging.getLogger("install_second_machine")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _run(cmd: list[str], check: bool = True, timeout: int = 300) -> subprocess.CompletedProcess:
    """Run a subprocess with logging."""
    log.debug("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode != 0:
        stderr_snippet = (result.stderr or "")[:500]
        raise RuntimeError(
            f"Command failed (exit {result.returncode}): {' '.join(cmd)}\n{stderr_snippet}"
        )
    return result


def _python_cmd() -> str:
    """Return the correct python command for this platform."""
    if platform.system() == "Darwin":
        return "python3"
    return "python"


def _pip_cmd() -> list[str]:
    """Return the correct pip invocation for this platform."""
    if platform.system() == "Darwin":
        return ["python3", "-m", "pip"]
    return ["python", "-m", "pip"]


# ---------------------------------------------------------------------------
# Step 1: Check Python version
# ---------------------------------------------------------------------------
def step_check_python() -> None:
    """Verify Python >= 3.11 is installed."""
    log.info("Step 1: Checking Python version ...")
    ver = sys.version_info
    if ver >= MIN_PYTHON:
        log.info("Python %d.%d.%d — OK", ver[0], ver[1], ver[2])
        return

    system = platform.system()
    if system == "Darwin":
        log.error(
            "Python %d.%d is below minimum %d.%d. "
            "Install via Homebrew:  brew install python@3.11",
            ver[0], ver[1], MIN_PYTHON[0], MIN_PYTHON[1],
        )
    else:
        log.error(
            "Python %d.%d is below minimum %d.%d. "
            "Download from https://www.python.org/downloads/",
            ver[0], ver[1], MIN_PYTHON[0], MIN_PYTHON[1],
        )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Step 2: Install dependencies
# ---------------------------------------------------------------------------
def step_install_deps(skip: bool = False) -> None:
    """Install Python packages from requirements.txt."""
    log.info("Step 2: Installing dependencies ...")
    if skip:
        log.info("Skipping dependency install (--skip-deps)")
        return

    if not REQUIREMENTS_FILE.exists():
        log.warning("requirements.txt not found at %s — skipping", REQUIREMENTS_FILE)
        return

    pip = _pip_cmd()
    _run([*pip, "install", "--upgrade", "pip"], check=False)
    _run([*pip, "install", "-r", str(REQUIREMENTS_FILE)], timeout=600)
    log.info("Dependencies installed")


# ---------------------------------------------------------------------------
# Step 3: Run database migration
# ---------------------------------------------------------------------------
def step_migrate_db() -> None:
    """Run migrate_db.py to initialise / upgrade the database schema."""
    log.info("Step 3: Running database migration ...")

    # Ensure data directory exists
    data_dir = ROOT_DIR / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    if not MIGRATE_SCRIPT.exists():
        log.warning("migrate_db.py not found — skipping (DB will be created on first run)")
        return

    python = _python_cmd()
    _run([python, str(MIGRATE_SCRIPT)], timeout=120)
    log.info("Database migration complete")


# ---------------------------------------------------------------------------
# Step 4: Copy configuration from first machine
# ---------------------------------------------------------------------------
def step_copy_config(config_src: str | None) -> None:
    """Copy otocpa.config.json from USB / network share / specified path."""
    log.info("Step 4: Configuration ...")

    if config_src:
        src = Path(config_src)
        if not src.exists():
            log.error("Config file not found: %s", src)
            sys.exit(1)
        shutil.copy2(str(src), str(CONFIG_FILE))
        log.info("Copied config from %s", src)
    elif CONFIG_FILE.exists():
        log.info("Config already exists at %s — keeping it", CONFIG_FILE)
    else:
        log.warning(
            "No config file found. Copy otocpa.config.json from your first machine "
            "to: %s", CONFIG_FILE,
        )


# ---------------------------------------------------------------------------
# Step 5 (platform-specific): Register auto-start
# ---------------------------------------------------------------------------
def step_register_autostart_windows() -> None:
    """Register OtoCPA as a Windows Service via service_wrapper.py."""
    log.info("Step 5: Registering Windows Service ...")

    if SERVICE_WRAPPER.exists():
        python = _python_cmd()
        _run([python, str(SERVICE_WRAPPER), "install"], check=False)
        log.info("Windows service installed")
    else:
        log.info(
            "service_wrapper.py not found — you can start OtoCPA manually:\n"
            "    python scripts/review_dashboard.py"
        )


def step_register_autostart_mac() -> None:
    """Create a launchd plist so OtoCPA starts automatically on macOS."""
    log.info("Step 5: Creating launchd plist for auto-start ...")

    LAUNCHD_PLIST_DIR.mkdir(parents=True, exist_ok=True)
    plist_path = LAUNCHD_PLIST_DIR / LAUNCHD_PLIST_NAME

    dashboard_script = ROOT_DIR / "scripts" / "review_dashboard.py"
    plist_content = textwrap.dedent(f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
          "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
        <plist version="1.0">
        <dict>
            <key>Label</key>
            <string>com.otocpa</string>
            <key>ProgramArguments</key>
            <array>
                <string>/usr/local/bin/python3</string>
                <string>{dashboard_script}</string>
            </array>
            <key>WorkingDirectory</key>
            <string>{ROOT_DIR}</string>
            <key>RunAtLoad</key>
            <true/>
            <key>KeepAlive</key>
            <true/>
            <key>StandardOutPath</key>
            <string>{ROOT_DIR / "data" / "otocpa.stdout.log"}</string>
            <key>StandardErrorPath</key>
            <string>{ROOT_DIR / "data" / "otocpa.stderr.log"}</string>
        </dict>
        </plist>
    """)

    plist_path.write_text(plist_content, encoding="utf-8")
    log.info("Plist written to %s", plist_path)


# ---------------------------------------------------------------------------
# Step 6: Start the service
# ---------------------------------------------------------------------------
def step_start_service_windows() -> None:
    """Start the Windows service."""
    log.info("Step 6: Starting service ...")
    if SERVICE_WRAPPER.exists():
        python = _python_cmd()
        _run([python, str(SERVICE_WRAPPER), "start"], check=False)
        log.info("Service start requested")
    else:
        log.info("Start manually:  python scripts/review_dashboard.py")


def step_start_service_mac() -> None:
    """Load the launchd plist to start OtoCPA."""
    log.info("Step 6: Loading launchd plist ...")
    plist_path = LAUNCHD_PLIST_DIR / LAUNCHD_PLIST_NAME
    if plist_path.exists():
        _run(["launchctl", "load", str(plist_path)], check=False)
        log.info("launchctl load complete")
    else:
        log.info("Start manually:  python3 scripts/review_dashboard.py")


# ---------------------------------------------------------------------------
# Step 7: Open browser
# ---------------------------------------------------------------------------
def step_open_browser() -> None:
    """Open the OtoCPA dashboard in the default browser."""
    url = f"http://127.0.0.1:{DASHBOARD_PORT}/"
    log.info("Step 7: Opening %s ...", url)
    try:
        webbrowser.open(url)
    except Exception:
        log.info("Could not open browser automatically — visit %s", url)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Install OtoCPA on a second machine (Windows or Mac)",
    )
    parser.add_argument(
        "--config", default=None,
        help="Path to otocpa.config.json copied from your first machine",
    )
    parser.add_argument(
        "--skip-deps", action="store_true",
        help="Skip pip install of requirements",
    )
    parser.add_argument(
        "--server-mode", action="store_true",
        help="Configure this machine as the server (binds to 0.0.0.0)",
    )
    args = parser.parse_args()

    system = platform.system()
    log.info("=" * 60)
    log.info("OtoCPA — Second Machine Installer")
    log.info("Platform : %s", platform.platform())
    log.info("Root dir : %s", ROOT_DIR)
    log.info("=" * 60)

    if system not in ("Windows", "Darwin"):
        log.error("Unsupported platform: %s. Only Windows and macOS are supported.", system)
        return 1

    # Steps common to both platforms
    step_check_python()
    step_install_deps(skip=args.skip_deps)
    step_migrate_db()
    step_copy_config(args.config)

    # Platform-specific auto-start
    if system == "Windows":
        step_register_autostart_windows()
        step_start_service_windows()
    else:
        step_register_autostart_mac()
        step_start_service_mac()

    step_open_browser()

    log.info("=" * 60)
    log.info("Installation complete!")
    log.info("Dashboard: http://127.0.0.1:%d/", DASHBOARD_PORT)
    if args.server_mode:
        log.info(
            "Server mode: other machines on your network can access "
            "OtoCPA at http://<this-machine-ip>:%d/", DASHBOARD_PORT,
        )
    log.info("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
