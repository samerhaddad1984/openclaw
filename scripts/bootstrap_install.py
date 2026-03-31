#!/usr/bin/env python3
"""
scripts/bootstrap_install.py — OtoCPA Bootstrap Installer
==============================================================
A single Python script that can be run on any clean Windows 10/11 machine
to install OtoCPA from scratch.

Usage:
    python bootstrap_install.py --license-key LLAI-XXXX --firm-name "Tremblay CPA"

Optional flags:
    --release-url   URL to download the latest release archive (default: from config)
    --install-dir   Installation directory (default: C:\\Program Files\\OtoCPA)
    --skip-python   Skip Python installation check
"""
from __future__ import annotations

import argparse
import ctypes
import io
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_INSTALL_DIR = Path(r"C:\Program Files\OtoCPA")
LOG_DIR = Path(r"C:\OtoCPA")
LOG_FILE = LOG_DIR / "install.log"
MIN_PYTHON = (3, 11)
SERVICE_NAME = "OtoCPA"
DASHBOARD_PORT = 8787
WIZARD_PORT = 8790
DEFAULT_RELEASE_URL = "https://releases.otocpa.ai/latest/otocpa-latest.zip"
PYTHON_DOWNLOAD_URL = "https://www.python.org/ftp/python/3.12.3/python-3.12.3-amd64.exe"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _setup_logging() -> logging.Logger:
    """Configure file + console logging."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("bootstrap")
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(str(LOG_FILE), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("  [%(levelname)s] %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


log: logging.Logger | None = None


def _log_info(msg: str) -> None:
    if log:
        log.info(msg)
    else:
        print(f"  [INFO] {msg}")


def _log_error(msg: str) -> None:
    if log:
        log.error(msg)
    else:
        print(f"  [ERROR] {msg}", file=sys.stderr)


def _log_debug(msg: str) -> None:
    if log:
        log.debug(msg)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _is_admin() -> bool:
    """Check if running with admin privileges."""
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0  # type: ignore[attr-defined]
    except Exception:
        return False


def _run(cmd: list[str], check: bool = True, timeout: int = 300, **kwargs) -> subprocess.CompletedProcess:
    """Run a subprocess, logging the command."""
    _log_debug(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, **kwargs)
    if result.stdout:
        _log_debug(f"stdout: {result.stdout.strip()[:500]}")
    if result.stderr:
        _log_debug(f"stderr: {result.stderr.strip()[:500]}")
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed (exit {result.returncode}): {' '.join(cmd)}\n"
            f"{result.stderr[:500]}"
        )
    return result


def _download(url: str, dest: Path, label: str = "") -> None:
    """Download a file with progress indication."""
    _log_info(f"Downloading {label or url} ...")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "OtoCPA-Installer/1.0"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        size_mb = len(data) / (1024 * 1024)
        _log_info(f"Downloaded {size_mb:.1f} MB to {dest}")
    except Exception as exc:
        raise RuntimeError(f"Failed to download {url}: {exc}") from exc


# ---------------------------------------------------------------------------
# Step 1: Check / Install Python
# ---------------------------------------------------------------------------
def step_check_python(skip: bool = False) -> str:
    """Verify Python 3.11+ is available. Returns path to python executable."""
    _log_info("Step 1: Checking Python installation ...")

    if skip:
        _log_info("Skipping Python check (--skip-python)")
        return sys.executable

    ver = sys.version_info
    if ver >= MIN_PYTHON:
        _log_info(f"Python {ver.major}.{ver.minor}.{ver.micro} found — OK")
        return sys.executable

    _log_info(f"Python {ver.major}.{ver.minor} is below minimum {MIN_PYTHON[0]}.{MIN_PYTHON[1]}")
    _log_info("Downloading Python installer ...")

    installer = Path(tempfile.gettempdir()) / "python-installer.exe"
    _download(PYTHON_DOWNLOAD_URL, installer, "Python installer")

    _log_info("Installing Python silently (this may take a minute) ...")
    _run([
        str(installer),
        "/quiet",
        "InstallAllUsers=1",
        "PrependPath=1",
        "Include_pip=1",
        "Include_test=0",
    ], timeout=600)

    _log_info("Python installed successfully")

    # Find the newly installed Python
    for candidate in [
        Path(r"C:\Program Files\Python312\python.exe"),
        Path(r"C:\Program Files\Python311\python.exe"),
        Path(r"C:\Python312\python.exe"),
        Path(r"C:\Python311\python.exe"),
    ]:
        if candidate.exists():
            _log_info(f"Found Python at {candidate}")
            return str(candidate)

    # Fallback: try from PATH
    result = subprocess.run(["where", "python"], capture_output=True, text=True)
    if result.returncode == 0:
        py_path = result.stdout.strip().splitlines()[0]
        _log_info(f"Found Python in PATH: {py_path}")
        return py_path

    raise RuntimeError("Python was installed but could not be found. Please restart and try again.")


# ---------------------------------------------------------------------------
# Step 2: Check pip
# ---------------------------------------------------------------------------
def step_check_pip(python: str) -> None:
    """Ensure pip is available."""
    _log_info("Step 2: Checking pip ...")
    try:
        _run([python, "-m", "pip", "--version"])
        _log_info("pip is available")
    except Exception:
        _log_info("pip not found — installing via ensurepip ...")
        _run([python, "-m", "ensurepip", "--upgrade"])
        _log_info("pip installed")


# ---------------------------------------------------------------------------
# Step 3: Install requirements
# ---------------------------------------------------------------------------
def step_install_requirements(python: str, install_dir: Path) -> None:
    """Install all required packages from requirements.txt."""
    _log_info("Step 3: Installing Python dependencies ...")
    req_file = install_dir / "requirements.txt"
    if not req_file.exists():
        # Generate a minimal requirements.txt if none exists
        _log_info("No requirements.txt found — creating default ...")
        req_file.write_text(
            "bcrypt>=4.0\n"
            "pdfplumber>=0.9\n"
            "Pillow>=9.0\n"
            "requests>=2.28\n"
            "reportlab>=4.0\n"
            "psutil>=5.9\n"
            "watchdog>=3.0\n",
            encoding="utf-8",
        )

    _run([python, "-m", "pip", "install", "--upgrade", "pip"], check=False)
    _run([python, "-m", "pip", "install", "-r", str(req_file)], timeout=600)
    _log_info("All dependencies installed")


# ---------------------------------------------------------------------------
# Step 4: Download latest release
# ---------------------------------------------------------------------------
def step_download_release(release_url: str, install_dir: Path) -> None:
    """Download the latest OtoCPA release archive."""
    _log_info("Step 4: Downloading latest OtoCPA release ...")
    archive = Path(tempfile.gettempdir()) / "otocpa-release.zip"
    _download(release_url, archive, "OtoCPA release")

    _log_info(f"Extracting to {install_dir} ...")
    install_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(str(archive), "r") as zf:
        zf.extractall(str(install_dir))
    _log_info("Extraction complete")

    # Clean up
    try:
        archive.unlink()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Step 5: Extract to install dir (already done in step 4)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Step 6: Run migrate_db.py
# ---------------------------------------------------------------------------
def step_migrate_db(python: str, install_dir: Path) -> None:
    """Initialize / migrate the database."""
    _log_info("Step 6: Initializing database ...")
    migrate_script = install_dir / "scripts" / "migrate_db.py"
    if not migrate_script.exists():
        _log_info("migrate_db.py not found — skipping (database will be created on first run)")
        return
    _run([python, str(migrate_script)], timeout=120)
    _log_info("Database initialized successfully")


# ---------------------------------------------------------------------------
# Step 7: Register Windows Service
# ---------------------------------------------------------------------------
def step_register_service(python: str, install_dir: Path) -> None:
    """Register OtoCPA as a Windows Service."""
    _log_info("Step 7: Registering Windows Service ...")

    service_wrapper = install_dir / "installer" / "service_wrapper.py"
    dashboard_script = install_dir / "scripts" / "review_dashboard.py"

    # Use sc.exe to create the service
    # The service will run the dashboard via Python
    bin_path = f'"{python}" "{dashboard_script}"'

    # Check if service already exists
    result = subprocess.run(
        ["sc", "query", SERVICE_NAME],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        _log_info(f"Service '{SERVICE_NAME}' already exists — stopping and removing ...")
        subprocess.run(["sc", "stop", SERVICE_NAME], capture_output=True, timeout=30)
        time.sleep(2)
        subprocess.run(["sc", "delete", SERVICE_NAME], capture_output=True, timeout=30)
        time.sleep(1)

    # Create the service using NSSM if available, otherwise use sc
    nssm = install_dir / "tools" / "nssm.exe"
    if nssm.exists():
        _run([
            str(nssm), "install", SERVICE_NAME,
            python, str(dashboard_script),
        ])
        _run([str(nssm), "set", SERVICE_NAME, "DisplayName", "OtoCPA Accounting"])
        _run([str(nssm), "set", SERVICE_NAME, "Description",
              "OtoCPA — Intelligent Accounting Document Queue"])
        _run([str(nssm), "set", SERVICE_NAME, "Start", "SERVICE_AUTO_START"])
        _run([str(nssm), "set", SERVICE_NAME, "AppDirectory", str(install_dir)])
    else:
        # Fallback: create a simple batch wrapper
        wrapper_bat = install_dir / "run_service.bat"
        wrapper_bat.write_text(
            f'@echo off\r\ncd /d "{install_dir}"\r\n"{python}" "{dashboard_script}"\r\n',
            encoding="utf-8",
        )
        _run([
            "sc", "create", SERVICE_NAME,
            f"binPath={bin_path}",
            "start=auto",
            f"DisplayName=OtoCPA Accounting",
        ])

    # Start the service
    try:
        _run(["sc", "start", SERVICE_NAME], check=False)
        _log_info(f"Service '{SERVICE_NAME}' registered and started")
    except Exception as exc:
        _log_info(f"Service registered but could not be started: {exc}")
        _log_info("The dashboard can still be run manually.")


# ---------------------------------------------------------------------------
# Step 8: Create desktop shortcuts
# ---------------------------------------------------------------------------
def step_create_shortcuts(install_dir: Path) -> None:
    """Create desktop shortcuts for dashboard and setup wizard."""
    _log_info("Step 8: Creating desktop shortcuts ...")

    desktop = Path(os.path.expanduser("~")) / "Desktop"
    if not desktop.exists():
        desktop = Path(os.environ.get("USERPROFILE", "~")) / "Desktop"

    if not desktop.exists():
        _log_info("Desktop folder not found — skipping shortcuts")
        return

    # Dashboard shortcut (.url file — works without COM dependencies)
    dash_shortcut = desktop / "OtoCPA Dashboard.url"
    dash_shortcut.write_text(
        f"[InternetShortcut]\r\n"
        f"URL=http://127.0.0.1:{DASHBOARD_PORT}/\r\n"
        f"IconIndex=0\r\n",
        encoding="utf-8",
    )

    # Setup wizard shortcut
    wizard_shortcut = desktop / "OtoCPA Setup.url"
    wizard_shortcut.write_text(
        f"[InternetShortcut]\r\n"
        f"URL=http://127.0.0.1:{WIZARD_PORT}/\r\n"
        f"IconIndex=0\r\n",
        encoding="utf-8",
    )

    _log_info("Desktop shortcuts created")


# ---------------------------------------------------------------------------
# Step 9: Open setup wizard
# ---------------------------------------------------------------------------
def step_open_wizard() -> None:
    """Open the setup wizard in the default browser."""
    _log_info("Step 9: Opening setup wizard ...")
    import webbrowser
    url = f"http://127.0.0.1:{WIZARD_PORT}/"
    try:
        webbrowser.open(url)
        _log_info(f"Opened {url} in default browser")
    except Exception as exc:
        _log_info(f"Could not open browser automatically: {exc}")
        _log_info(f"Please open {url} manually to complete setup.")


# ---------------------------------------------------------------------------
# Step 10: Save license and firm info
# ---------------------------------------------------------------------------
def step_save_license(install_dir: Path, license_key: str, firm_name: str) -> None:
    """Save the license key and firm name to config."""
    _log_info("Step 10: Saving license and firm configuration ...")
    config_path = install_dir / "otocpa.config.json"

    try:
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        cfg = {}

    # Save firm name
    cfg["firm_name"] = firm_name

    # Save license key (validation will happen via setup wizard)
    if license_key:
        cfg.setdefault("license", {})
        cfg["license"]["key"] = license_key

    config_path.write_text(
        json.dumps(cfg, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    _log_info(f"Configuration saved for firm: {firm_name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    global log

    parser = argparse.ArgumentParser(
        description="OtoCPA — Bootstrap Installer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Example:\n  python bootstrap_install.py --license-key LLAI-XXXX --firm-name \"Tremblay CPA\"",
    )
    parser.add_argument("--license-key", required=True, help="License key (LLAI-...)")
    parser.add_argument("--firm-name", required=True, help="Firm / cabinet name")
    parser.add_argument("--release-url", default=DEFAULT_RELEASE_URL,
                        help="URL to download the release archive")
    parser.add_argument("--install-dir", default=str(DEFAULT_INSTALL_DIR),
                        help="Installation directory")
    parser.add_argument("--skip-python", action="store_true",
                        help="Skip Python installation check")
    args = parser.parse_args()

    install_dir = Path(args.install_dir)
    log = _setup_logging()

    _log_info("=" * 60)
    _log_info("OtoCPA — Bootstrap Installer")
    _log_info(f"Date      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    _log_info(f"Platform  : {platform.platform()}")
    _log_info(f"Install to: {install_dir}")
    _log_info(f"Firm      : {args.firm_name}")
    _log_info("=" * 60)

    if platform.system() != "Windows":
        _log_error("This installer is designed for Windows 10/11 only.")
        return 1

    if not _is_admin():
        _log_error("Administrator privileges required. Please run as Administrator.")
        _log_info("Right-click the script and select 'Run as administrator'.")
        return 1

    try:
        # Step 1: Python
        python = step_check_python(args.skip_python)

        # Step 2: pip
        step_check_pip(python)

        # Step 4: Download release (creates install_dir)
        step_download_release(args.release_url, install_dir)

        # Step 3: Requirements (after download so requirements.txt is available)
        step_install_requirements(python, install_dir)

        # Step 6: Database
        step_migrate_db(python, install_dir)

        # Step 10: License & config (before service start)
        step_save_license(install_dir, args.license_key, args.firm_name)

        # Step 7: Service
        step_register_service(python, install_dir)

        # Step 8: Shortcuts
        step_create_shortcuts(install_dir)

        # Step 9: Open wizard
        step_open_wizard()

        _log_info("=" * 60)
        _log_info("Installation complete!")
        _log_info(f"Dashboard : http://127.0.0.1:{DASHBOARD_PORT}/")
        _log_info(f"Setup     : http://127.0.0.1:{WIZARD_PORT}/")
        _log_info(f"Log file  : {LOG_FILE}")
        _log_info("=" * 60)
        return 0

    except Exception as exc:
        _log_error(f"Installation failed: {exc}")
        _log_info(f"Check {LOG_FILE} for details.")
        if log:
            log.debug("Traceback:", exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
