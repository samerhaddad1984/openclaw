#!/usr/bin/env python3
"""
scripts/remote_management.py — OtoCPA Remote Management Utilities
======================================================================
Provides remote management capabilities over Cloudflare Tunnel.
Used by the dashboard's /admin/remote route.

Functions return dicts suitable for JSON or HTML rendering.
"""
from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "otocpa_agent.db"
BACKUP_DIR = ROOT / "data" / "backups"
LOG_PATH = ROOT / "data" / "otocpa.log"
AUTOFIX_SCRIPT = ROOT / "scripts" / "autofix.py"
UPDATE_SCRIPT = ROOT / "scripts" / "update_otocpa.py"

SERVICE_NAME = "OtoCPA"


# ---------------------------------------------------------------------------
# System status
# ---------------------------------------------------------------------------

def get_system_status() -> dict:
    """Gather system status: OS, disk, memory, service status, last backup."""
    status: dict = {
        "hostname": platform.node(),
        "os": platform.platform(),
        "python": sys.version.split()[0],
        "service_status": "unknown",
        "disk_total_gb": 0,
        "disk_free_gb": 0,
        "disk_used_pct": 0,
        "db_size_mb": 0,
        "last_backup": "",
        "last_backup_size_mb": 0,
        "uptime": "",
        "error": "",
    }

    # Service status
    try:
        result = subprocess.run(
            ["sc", "query", SERVICE_NAME],
            capture_output=True, text=True, timeout=10,
        )
        if "RUNNING" in result.stdout:
            status["service_status"] = "running"
        elif "STOPPED" in result.stdout:
            status["service_status"] = "stopped"
        elif "STOP_PENDING" in result.stdout:
            status["service_status"] = "stopping"
        elif "START_PENDING" in result.stdout:
            status["service_status"] = "starting"
        else:
            status["service_status"] = "not_installed"
    except Exception:
        status["service_status"] = "unknown"

    # Disk space
    try:
        total, used, free = shutil.disk_usage(str(ROOT))
        status["disk_total_gb"] = round(total / (1024 ** 3), 1)
        status["disk_free_gb"] = round(free / (1024 ** 3), 1)
        status["disk_used_pct"] = round(used * 100 / total, 1) if total > 0 else 0
    except Exception:
        pass

    # Database size
    try:
        if DB_PATH.exists():
            status["db_size_mb"] = round(DB_PATH.stat().st_size / (1024 * 1024), 2)
    except Exception:
        pass

    # Last backup
    try:
        if BACKUP_DIR.exists():
            backups = sorted(
                [f for f in BACKUP_DIR.iterdir()
                 if f.is_file() and f.name.startswith("otocpa_agent_")],
                reverse=True,
            )
            if backups:
                latest = backups[0]
                status["last_backup"] = latest.name
                status["last_backup_size_mb"] = round(
                    latest.stat().st_size / (1024 * 1024), 2
                )
    except Exception:
        pass

    # System uptime (Windows)
    try:
        result = subprocess.run(
            ["wmic", "os", "get", "LastBootUpTime", "/value"],
            capture_output=True, text=True, timeout=10,
        )
        for line in result.stdout.strip().splitlines():
            if line.startswith("LastBootUpTime="):
                boot_str = line.split("=", 1)[1].strip()[:14]
                boot_dt = datetime.strptime(boot_str, "%Y%m%d%H%M%S")
                delta = datetime.now() - boot_dt
                days = delta.days
                hours = delta.seconds // 3600
                status["uptime"] = f"{days}d {hours}h"
    except Exception:
        pass

    return status


# ---------------------------------------------------------------------------
# Service management
# ---------------------------------------------------------------------------

def restart_service() -> dict:
    """Restart the OtoCPA Windows Service."""
    result = {"success": False, "message": "", "error": ""}

    try:
        # Stop
        subprocess.run(["sc", "stop", SERVICE_NAME], capture_output=True, timeout=30)
        time.sleep(3)

        # Start
        proc = subprocess.run(
            ["sc", "start", SERVICE_NAME],
            capture_output=True, text=True, timeout=30,
        )
        time.sleep(2)

        # Verify
        query = subprocess.run(
            ["sc", "query", SERVICE_NAME],
            capture_output=True, text=True, timeout=10,
        )
        if "RUNNING" in query.stdout:
            result["success"] = True
            result["message"] = "Service restarted successfully"
        else:
            result["success"] = True
            result["message"] = "Service restart initiated (may take a moment)"
    except Exception as exc:
        result["error"] = str(exc)

    return result


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------

def create_backup() -> dict:
    """Create an immediate database backup."""
    result = {"success": False, "backup_name": "", "size_mb": 0, "error": ""}

    if not DB_PATH.exists():
        result["error"] = "Database file not found"
        return result

    try:
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = BACKUP_DIR / f"otocpa_agent_{ts}.db"
        shutil.copy2(str(DB_PATH), str(backup_path))
        result["success"] = True
        result["backup_name"] = backup_path.name
        result["size_mb"] = round(backup_path.stat().st_size / (1024 * 1024), 2)
    except Exception as exc:
        result["error"] = str(exc)

    return result


def list_backups() -> list[dict]:
    """List available backups."""
    backups = []
    if not BACKUP_DIR.exists():
        return backups

    for f in sorted(BACKUP_DIR.iterdir(), reverse=True):
        if f.is_file() and f.name.startswith("otocpa_agent_"):
            backups.append({
                "name": f.name,
                "size_mb": round(f.stat().st_size / (1024 * 1024), 2),
                "modified": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
            })
    return backups


# ---------------------------------------------------------------------------
# Remote update
# ---------------------------------------------------------------------------

def trigger_update() -> dict:
    """Trigger an update check and installation."""
    result = {"success": False, "message": "", "error": ""}

    if not UPDATE_SCRIPT.exists():
        result["error"] = "update_otocpa.py not found"
        return result

    try:
        proc = subprocess.run(
            [sys.executable, str(UPDATE_SCRIPT), "--install"],
            capture_output=True, text=True, timeout=300,
        )
        if proc.returncode == 0:
            result["success"] = True
            result["message"] = "Update completed successfully"
        else:
            result["error"] = proc.stderr[:500] if proc.stderr else "Update failed"
            # Include stdout for context
            if proc.stdout:
                result["message"] = proc.stdout[-500:]
    except subprocess.TimeoutExpired:
        result["error"] = "Update timed out (5 min)"
    except Exception as exc:
        result["error"] = str(exc)

    return result


# ---------------------------------------------------------------------------
# Remote autofix
# ---------------------------------------------------------------------------

def trigger_autofix() -> dict:
    """Run autofix.py and return results."""
    result = {"success": False, "output": "", "error": ""}

    if not AUTOFIX_SCRIPT.exists():
        result["error"] = "autofix.py not found"
        return result

    try:
        proc = subprocess.run(
            [sys.executable, str(AUTOFIX_SCRIPT), "--lang", "en", "--no-color"],
            capture_output=True, text=True, timeout=120,
        )
        result["success"] = proc.returncode == 0
        result["output"] = proc.stdout[-3000:] if proc.stdout else ""
        if proc.stderr:
            result["output"] += "\n" + proc.stderr[-500:]
    except subprocess.TimeoutExpired:
        result["error"] = "Autofix timed out (2 min)"
    except Exception as exc:
        result["error"] = str(exc)

    return result


# ---------------------------------------------------------------------------
# Recent logs
# ---------------------------------------------------------------------------

def get_recent_logs(lines: int = 100) -> str:
    """Return the last N lines of the log file."""
    if not LOG_PATH.exists():
        return "(no log file found)"
    try:
        all_lines = LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(all_lines[-lines:])
    except Exception as exc:
        return f"(error reading logs: {exc})"
