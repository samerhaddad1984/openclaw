#!/usr/bin/env python3
"""
scripts/update_otocpa.py — OtoCPA Remote Update Mechanism
==================================================================
Allows remote updates without visiting the client machine.

Usage:
    python update_otocpa.py --check       # Just check for updates
    python update_otocpa.py --install     # Download and install update
    python update_otocpa.py --rollback    # Roll back to previous backup
"""
from __future__ import annotations

import argparse
import json
import os
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
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
VERSION_FILE = ROOT / "version.json"
DATA_DIR = ROOT / "data"
BACKUP_DIR = DATA_DIR / "backups"
DB_PATH = DATA_DIR / "otocpa_agent.db"
MIGRATE_SCRIPT = ROOT / "scripts" / "migrate_db.py"
CONFIG_PATH = ROOT / "otocpa.config.json"

SERVICE_NAME = "OtoCPA"
DASHBOARD_PORT = 8787
DEFAULT_UPDATE_URL = "https://releases.otocpa.ai/latest/version.json"

# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

def _get_installed_version() -> dict:
    """Read the installed version.json."""
    try:
        return json.loads(VERSION_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"version": "0.0.0", "release_date": "", "changelog": ""}


def _get_update_url() -> str:
    """Read update URL from config, fallback to default."""
    try:
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return cfg.get("update_url", DEFAULT_UPDATE_URL)
    except Exception:
        return DEFAULT_UPDATE_URL


def _fetch_remote_version(update_url: str) -> dict | None:
    """Fetch the remote version.json from the update server."""
    try:
        req = urllib.request.Request(
            update_url,
            headers={"User-Agent": "OtoCPA-Updater/1.0"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        print(f"  [ERROR] Could not reach update server: {exc}")
        return None


def _version_tuple(version_str: str) -> tuple[int, ...]:
    """Parse '1.2.3' into (1, 2, 3)."""
    try:
        return tuple(int(x) for x in version_str.split("."))
    except Exception:
        return (0, 0, 0)


# ---------------------------------------------------------------------------
# Service management
# ---------------------------------------------------------------------------

def _stop_service() -> bool:
    """Stop the OtoCPA Windows Service."""
    print("  Stopping OtoCPA service ...")
    try:
        subprocess.run(["sc", "stop", SERVICE_NAME], capture_output=True, timeout=30)
        time.sleep(3)
        # Verify stopped
        result = subprocess.run(
            ["sc", "query", SERVICE_NAME],
            capture_output=True, text=True, timeout=10,
        )
        if "STOPPED" in result.stdout:
            print("  Service stopped")
            return True
        # Wait a bit more
        time.sleep(5)
        return True
    except Exception as exc:
        print(f"  [WARN] Could not stop service: {exc}")
        return False


def _start_service() -> bool:
    """Start the OtoCPA Windows Service."""
    print("  Starting OtoCPA service ...")
    try:
        subprocess.run(["sc", "start", SERVICE_NAME], capture_output=True, timeout=30)
        time.sleep(3)
        result = subprocess.run(
            ["sc", "query", SERVICE_NAME],
            capture_output=True, text=True, timeout=10,
        )
        if "RUNNING" in result.stdout:
            print("  Service started")
            return True
        print("  [WARN] Service may not have started fully")
        return True
    except Exception as exc:
        print(f"  [ERROR] Could not start service: {exc}")
        return False


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------

def _backup_database() -> Path | None:
    """Create a timestamped backup of the database."""
    if not DB_PATH.exists():
        print("  No database to back up")
        return None

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"otocpa_agent_{ts}.db"
    shutil.copy2(str(DB_PATH), str(backup_path))
    size_mb = backup_path.stat().st_size / (1024 * 1024)
    print(f"  Database backed up: {backup_path.name} ({size_mb:.2f} MB)")
    return backup_path


def _backup_application(install_dir: Path) -> Path | None:
    """Create a backup of key application files."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = BACKUP_DIR / f"app_backup_{ts}"
    backup_dir.mkdir(parents=True, exist_ok=True)

    # Back up scripts and src directories
    for subdir in ["scripts", "src", "version.json", "otocpa.config.json"]:
        src = install_dir / subdir
        if src.is_file():
            shutil.copy2(str(src), str(backup_dir / subdir))
        elif src.is_dir():
            shutil.copytree(str(src), str(backup_dir / subdir), dirs_exist_ok=True)

    print(f"  Application backed up: {backup_dir.name}")
    return backup_dir


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

def _download_update(download_url: str) -> Path:
    """Download the update package."""
    print(f"  Downloading update ...")
    dest = Path(tempfile.gettempdir()) / "otocpa-update.zip"
    req = urllib.request.Request(
        download_url,
        headers={"User-Agent": "OtoCPA-Updater/1.0"},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    dest.write_bytes(data)
    size_mb = len(data) / (1024 * 1024)
    print(f"  Downloaded {size_mb:.1f} MB")
    return dest


def _apply_update(archive: Path, install_dir: Path) -> None:
    """Extract update files over the installation directory."""
    print("  Applying update files ...")
    with zipfile.ZipFile(str(archive), "r") as zf:
        zf.extractall(str(install_dir))
    print("  Files updated")

    # Clean up archive
    try:
        archive.unlink()
    except Exception:
        pass


def _run_migrations(install_dir: Path) -> bool:
    """Run database migrations."""
    migrate = install_dir / "scripts" / "migrate_db.py"
    if not migrate.exists():
        migrate = MIGRATE_SCRIPT
    if not migrate.exists():
        print("  [WARN] migrate_db.py not found — skipping migrations")
        return True

    print("  Running database migrations ...")
    try:
        result = subprocess.run(
            [sys.executable, str(migrate)],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode == 0:
            print("  Migrations complete")
            return True
        print(f"  [ERROR] Migration failed: {result.stderr[:300]}")
        return False
    except Exception as exc:
        print(f"  [ERROR] Migration error: {exc}")
        return False


def _verify_dashboard() -> bool:
    """Verify the dashboard is responding."""
    print("  Verifying dashboard ...")
    time.sleep(3)
    try:
        url = f"http://127.0.0.1:{DASHBOARD_PORT}/login"
        req = urllib.request.Request(url, headers={"User-Agent": "OtoCPA-Updater/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                print("  Dashboard is responding (HTTP 200)")
                return True
            print(f"  [WARN] Dashboard returned HTTP {resp.status}")
            return False
    except Exception as exc:
        print(f"  [WARN] Dashboard not responding: {exc}")
        return False


def _rollback(db_backup: Path | None, app_backup: Path | None, install_dir: Path) -> None:
    """Roll back from backup if update failed."""
    print("\n  ROLLING BACK ...")

    if app_backup and app_backup.exists():
        print("  Restoring application files ...")
        for item in app_backup.iterdir():
            dest = install_dir / item.name
            if item.is_file():
                shutil.copy2(str(item), str(dest))
            elif item.is_dir():
                if dest.exists():
                    shutil.rmtree(str(dest))
                shutil.copytree(str(item), str(dest))
        print("  Application files restored")

    if db_backup and db_backup.exists():
        print("  Restoring database ...")
        shutil.copy2(str(db_backup), str(DB_PATH))
        print("  Database restored")

    print("  Rollback complete")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_check() -> int:
    """Check for available updates."""
    installed = _get_installed_version()
    update_url = _get_update_url()

    print()
    print("OtoCPA Update Check")
    print("=" * 50)
    print(f"  Installed version : {installed['version']}")
    print(f"  Release date      : {installed.get('release_date', 'unknown')}")
    print(f"  Update server     : {update_url}")
    print()

    remote = _fetch_remote_version(update_url)
    if not remote:
        print("  Could not check for updates.")
        return 1

    remote_ver = remote.get("version", "0.0.0")
    installed_ver = installed.get("version", "0.0.0")

    if _version_tuple(remote_ver) > _version_tuple(installed_ver):
        print(f"  UPDATE AVAILABLE: {installed_ver} → {remote_ver}")
        print(f"  Release date : {remote.get('release_date', 'unknown')}")
        print(f"  Changelog    : {remote.get('changelog', 'N/A')}")
        print()
        print(f"  Run: python update_otocpa.py --install")
        return 0
    else:
        print(f"  You are running the latest version ({installed_ver})")
        return 0


def cmd_install() -> int:
    """Download and install an update."""
    installed = _get_installed_version()
    update_url = _get_update_url()

    print()
    print("OtoCPA Update Installer")
    print("=" * 50)
    print(f"  Current version: {installed['version']}")

    # Fetch remote version info
    remote = _fetch_remote_version(update_url)
    if not remote:
        print("  Could not reach update server. Aborting.")
        return 1

    remote_ver = remote.get("version", "0.0.0")
    installed_ver = installed.get("version", "0.0.0")

    if _version_tuple(remote_ver) <= _version_tuple(installed_ver):
        print(f"  Already up to date ({installed_ver})")
        return 0

    print(f"  Updating: {installed_ver} → {remote_ver}")
    download_url = remote.get("download_url", "")
    if not download_url:
        print("  [ERROR] No download_url in remote version.json")
        return 1

    install_dir = ROOT
    db_backup = None
    app_backup = None

    try:
        # Step 1: Stop service
        _stop_service()

        # Step 2: Backup
        print("\n  Creating backups ...")
        db_backup = _backup_database()
        app_backup = _backup_application(install_dir)

        # Step 3: Download
        archive = _download_update(download_url)

        # Step 4: Apply
        _apply_update(archive, install_dir)

        # Step 5: Migrate
        if not _run_migrations(install_dir):
            print("  [ERROR] Migrations failed — rolling back")
            _rollback(db_backup, app_backup, install_dir)
            _start_service()
            return 1

        # Step 6: Start service
        _start_service()

        # Step 7: Verify
        if not _verify_dashboard():
            print("  [WARN] Dashboard did not respond — rolling back")
            _stop_service()
            _rollback(db_backup, app_backup, install_dir)
            _start_service()
            return 1

        print()
        print("=" * 50)
        print(f"  Update successful: {installed_ver} → {remote_ver}")
        print("=" * 50)
        return 0

    except Exception as exc:
        print(f"\n  [ERROR] Update failed: {exc}")
        print("  Attempting rollback ...")
        try:
            _stop_service()
        except Exception:
            pass
        _rollback(db_backup, app_backup, install_dir)
        _start_service()
        return 1


def cmd_rollback() -> int:
    """Roll back to the most recent backup."""
    print()
    print("OtoCPA Rollback")
    print("=" * 50)

    if not BACKUP_DIR.exists():
        print("  No backups found")
        return 1

    # Find latest app backup
    app_backups = sorted(
        [d for d in BACKUP_DIR.iterdir() if d.is_dir() and d.name.startswith("app_backup_")],
        reverse=True,
    )
    db_backups = sorted(
        [f for f in BACKUP_DIR.iterdir() if f.is_file() and f.name.startswith("otocpa_agent_")],
        reverse=True,
    )

    if not app_backups and not db_backups:
        print("  No backups found")
        return 1

    _stop_service()
    _rollback(
        db_backups[0] if db_backups else None,
        app_backups[0] if app_backups else None,
        ROOT,
    )
    _start_service()

    print("  Rollback complete")
    return 0


# ---------------------------------------------------------------------------
# Public API (for dashboard integration)
# ---------------------------------------------------------------------------

def check_for_updates() -> dict:
    """Check for updates and return status dict (used by dashboard)."""
    installed = _get_installed_version()
    update_url = _get_update_url()
    remote = _fetch_remote_version(update_url)

    result = {
        "installed_version": installed.get("version", "0.0.0"),
        "installed_date": installed.get("release_date", ""),
        "update_available": False,
        "remote_version": "",
        "remote_date": "",
        "changelog": "",
        "download_url": "",
        "error": "",
    }

    if not remote:
        result["error"] = "Could not reach update server"
        return result

    remote_ver = remote.get("version", "0.0.0")
    if _version_tuple(remote_ver) > _version_tuple(installed.get("version", "0.0.0")):
        result["update_available"] = True
        result["remote_version"] = remote_ver
        result["remote_date"] = remote.get("release_date", "")
        result["changelog"] = remote.get("changelog", "")
        result["download_url"] = remote.get("download_url", "")

    return result


def install_update_background() -> dict:
    """Run the update process and return result dict (used by dashboard)."""
    try:
        exit_code = cmd_install()
        if exit_code == 0:
            new_ver = _get_installed_version()
            return {"success": True, "version": new_ver.get("version", ""), "error": ""}
        return {"success": False, "version": "", "error": "Update failed — check logs"}
    except Exception as exc:
        return {"success": False, "version": "", "error": str(exc)}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="OtoCPA — Update Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true", help="Check for available updates")
    group.add_argument("--install", action="store_true", help="Download and install update")
    group.add_argument("--rollback", action="store_true", help="Roll back to previous backup")
    args = parser.parse_args()

    if args.check:
        return cmd_check()
    elif args.install:
        return cmd_install()
    elif args.rollback:
        return cmd_rollback()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
