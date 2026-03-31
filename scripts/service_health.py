#!/usr/bin/env python3
"""
scripts/service_health.py — LedgerLink Self-Healing Health Monitor
===================================================================
Runs every 5 minutes via Windows Scheduled Task.
Checks 5 health indicators and auto-heals failures.
After 3 consecutive failures of the same check, sends alert email.
"""
from __future__ import annotations

import importlib
import json
import logging
import os
import platform
import shutil
import smtplib
import sqlite3
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path

INSTALL_DIR = Path(r"C:\LedgerLink")
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
CONFIG_PATH = ROOT_DIR / "ledgerlink.config.json"
LOG_PATH = INSTALL_DIR / "service.log" if INSTALL_DIR.exists() else ROOT_DIR / "data" / "service_health.log"
STATE_PATH = ROOT_DIR / "data" / "health_state.json"
DASH_PORT = 8787
SUPPORT_EMAIL = "support@ledgerlink.ca"
MAX_CONSECUTIVE_FAILURES = 3
MIN_DISK_MB = 500
FREE_TARGET_MB = 1024

REQUIRED_PACKAGES = [
    "bcrypt", "sqlite3", "json", "hashlib", "html",
]

logger = logging.getLogger("ledgerlink.health")


def _setup_logging() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=str(LOG_PATH),
        level=logging.INFO,
        format="%(asctime)s [HEALTH] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Also log to stdout when run interactively
    if sys.stdout.isatty():
        logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def _load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"consecutive_failures": {}}


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _load_config() -> dict:
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _send_alert_email(subject: str, body: str) -> bool:
    """Send alert email via SMTP configured in ledgerlink.config.json."""
    cfg = _load_config().get("digest", {})
    smtp_host = cfg.get("smtp_host", "")
    smtp_port = cfg.get("smtp_port", 587)
    smtp_user = cfg.get("smtp_user", "")
    smtp_password = cfg.get("smtp_password", "")
    from_addr = cfg.get("from_address", smtp_user)

    if not smtp_host or not smtp_user or not smtp_password:
        logger.warning("SMTP not configured — cannot send alert email")
        return False

    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = SUPPORT_EMAIL

        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(from_addr, [SUPPORT_EMAIL], msg.as_string())
        logger.info(f"Alert email sent to {SUPPORT_EMAIL}: {subject}")
        return True
    except Exception as exc:
        logger.error(f"Failed to send alert email: {exc}")
        return False


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------

def _check_http_health() -> tuple[bool, str]:
    """Check 1: Port 8787 responds to HTTP GET /health."""
    try:
        url = f"http://127.0.0.1:{DASH_PORT}/health"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode("utf-8"))
                if data.get("status") == "ok":
                    return True, "HTTP /health responding"
                return False, f"HTTP /health returned status={data.get('status')}"
            return False, f"HTTP /health returned {resp.status}"
    except Exception as exc:
        return False, f"HTTP /health unreachable: {exc}"


def _check_database() -> tuple[bool, str]:
    """Check 2: Database accessible and not corrupted."""
    if not DB_PATH.exists():
        return False, "Database file not found"
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=5)
        cursor = conn.execute("PRAGMA integrity_check")
        result = cursor.fetchone()
        conn.close()
        if result and result[0] == "ok":
            return True, "Database integrity OK"
        return False, f"Database integrity: {result}"
    except Exception as exc:
        return False, f"Database error: {exc}"


def _check_disk_space() -> tuple[bool, str]:
    """Check 3: Disk space above 500MB."""
    try:
        usage = shutil.disk_usage(str(ROOT_DIR))
        free_mb = usage.free / (1024 * 1024)
        if free_mb >= MIN_DISK_MB:
            return True, f"Disk free: {free_mb:.0f} MB"
        return False, f"Disk free: {free_mb:.0f} MB (below {MIN_DISK_MB} MB)"
    except Exception as exc:
        return False, f"Disk check error: {exc}"


def _check_packages() -> tuple[bool, str]:
    """Check 4: All required Python packages importable."""
    missing = []
    for pkg in REQUIRED_PACKAGES:
        try:
            importlib.import_module(pkg)
        except ImportError:
            missing.append(pkg)
    if not missing:
        return True, "All required packages available"
    return False, f"Missing packages: {', '.join(missing)}"


def _check_cloudflare() -> tuple[bool, str]:
    """Check 5: Cloudflare tunnel responding."""
    if platform.system() != "Windows":
        return True, "Cloudflare check skipped (non-Windows)"
    try:
        result = subprocess.run(
            ["sc", "query", "cloudflared"],
            capture_output=True, text=True, timeout=10,
        )
        if "RUNNING" in result.stdout:
            return True, "Cloudflare tunnel running"
        return False, "Cloudflare tunnel not running"
    except FileNotFoundError:
        return True, "sc.exe not found — skipping Cloudflare check"
    except Exception as exc:
        return False, f"Cloudflare check error: {exc}"


def check_health() -> dict:
    """Run all 5 health checks and return results."""
    checks = {
        "http_health": _check_http_health,
        "database": _check_database,
        "disk_space": _check_disk_space,
        "packages": _check_packages,
        "cloudflare": _check_cloudflare,
    }
    results = {}
    for name, fn in checks.items():
        ok, msg = fn()
        results[name] = {"ok": ok, "message": msg}
    return results


# ---------------------------------------------------------------------------
# Auto-healing
# ---------------------------------------------------------------------------

def _heal_http() -> str:
    """Restart dashboard process."""
    try:
        python = sys.executable or "python"
        dashboard = ROOT_DIR / "scripts" / "review_dashboard.py"
        if not dashboard.exists():
            return "Dashboard script not found"

        # Try service restart first (Windows)
        if platform.system() == "Windows":
            result = subprocess.run(
                ["sc", "stop", "LedgerLinkAI"],
                capture_output=True, text=True, timeout=15,
            )
            import time
            time.sleep(2)
            result = subprocess.run(
                ["sc", "start", "LedgerLinkAI"],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0:
                return "Service LedgerLinkAI restarted"

        # Fallback: start directly
        subprocess.Popen(
            [python, str(dashboard)],
            cwd=str(ROOT_DIR),
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return "Dashboard started directly"
    except Exception as exc:
        return f"Failed to restart dashboard: {exc}"


def _heal_database() -> str:
    """Run PRAGMA integrity_check to attempt repair."""
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute("PRAGMA integrity_check")
        conn.close()
        return "Database integrity check executed"
    except Exception as exc:
        return f"Database heal failed: {exc}"


def _heal_disk_space() -> str:
    """Delete oldest backups until 1GB free."""
    backup_dir = ROOT_DIR / "data" / "backups"
    if not backup_dir.exists():
        return "No backup directory to clean"

    deleted = []
    backups = sorted(backup_dir.glob("*.db"), key=lambda p: p.stat().st_mtime)

    for backup in backups:
        usage = shutil.disk_usage(str(ROOT_DIR))
        free_mb = usage.free / (1024 * 1024)
        if free_mb >= FREE_TARGET_MB:
            break
        try:
            size_mb = backup.stat().st_size / (1024 * 1024)
            backup.unlink()
            deleted.append(f"{backup.name} ({size_mb:.1f} MB)")
        except Exception:
            pass

    if deleted:
        return f"Deleted {len(deleted)} backups: {', '.join(deleted)}"
    return "No backups to delete"


def _heal_packages() -> str:
    """pip install missing packages."""
    missing = []
    for pkg in REQUIRED_PACKAGES:
        try:
            importlib.import_module(pkg)
        except ImportError:
            missing.append(pkg)

    if not missing:
        return "No packages to install"

    installed = []
    for pkg in missing:
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", pkg],
                capture_output=True, text=True, timeout=120,
            )
            installed.append(pkg)
        except Exception:
            pass
    return f"Installed: {', '.join(installed)}" if installed else "No packages installed"


def _heal_cloudflare() -> str:
    """Start cloudflared service."""
    if platform.system() != "Windows":
        return "Skipped (non-Windows)"
    try:
        result = subprocess.run(
            ["sc", "start", "cloudflared"],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0 or "RUNNING" in result.stdout:
            return "Cloudflare tunnel started"
        return f"sc start cloudflared returned {result.returncode}: {result.stderr.strip()[:100]}"
    except Exception as exc:
        return f"Failed to start cloudflared: {exc}"


HEALERS = {
    "http_health": _heal_http,
    "database": _heal_database,
    "disk_space": _heal_disk_space,
    "packages": _heal_packages,
    "cloudflare": _heal_cloudflare,
}


def auto_heal(failed_checks: list) -> dict:
    """For each failed check, run the fix. Returns healing report."""
    state = _load_state()
    consecutive = state.get("consecutive_failures", {})

    actions_taken = []
    still_failing = []

    for check_name in failed_checks:
        # Track consecutive failures
        consecutive[check_name] = consecutive.get(check_name, 0) + 1
        count = consecutive[check_name]

        healer = HEALERS.get(check_name)
        if healer:
            action = healer()
            actions_taken.append(f"{check_name}: {action}")
            logger.info(f"HEAL {check_name} (attempt {count}): {action}")
        else:
            actions_taken.append(f"{check_name}: no healer available")

        # If same check fails 3 times in a row, send email
        if count >= MAX_CONSECUTIVE_FAILURES:
            subject = f"[LedgerLink ALERT] {check_name} failed {count} times"
            body = (
                f"LedgerLink Health Monitor Alert\n"
                f"{'=' * 40}\n"
                f"Check: {check_name}\n"
                f"Consecutive failures: {count}\n"
                f"Machine: {platform.node()}\n"
                f"Time: {datetime.now(timezone.utc).isoformat()}\n"
                f"Actions taken: {actions_taken[-1]}\n"
            )
            _send_alert_email(subject, body)
            still_failing.append(check_name)

    # Reset counters for checks that passed
    health = check_health()
    for name, result in health.items():
        if result["ok"] and name in consecutive:
            consecutive[name] = 0

    state["consecutive_failures"] = consecutive
    _save_state(state)

    healed = len(still_failing) == 0
    return {
        "healed": healed,
        "actions_taken": actions_taken,
        "still_failing": still_failing,
    }


def run_health_cycle() -> dict:
    """Run one full health check + heal cycle."""
    logger.info("--- Health check cycle starting ---")
    results = check_health()

    failed = [name for name, r in results.items() if not r["ok"]]
    passed = [name for name, r in results.items() if r["ok"]]

    for name in passed:
        logger.info(f"  OK: {name} — {results[name]['message']}")

    heal_report = {"healed": True, "actions_taken": [], "still_failing": []}
    if failed:
        for name in failed:
            logger.warning(f"  FAIL: {name} — {results[name]['message']}")
        heal_report = auto_heal(failed)
    else:
        # Reset all consecutive failure counters
        state = _load_state()
        state["consecutive_failures"] = {}
        _save_state(state)

    logger.info(f"  Result: healed={heal_report['healed']}, actions={len(heal_report['actions_taken'])}")
    return {"checks": results, "healing": heal_report}


def run_as_scheduled_task() -> int:
    """Register this script as a Windows Scheduled Task running every 5 minutes."""
    if platform.system() != "Windows":
        print("ERROR: Scheduled tasks are only supported on Windows.")
        return 1

    task_name = "LedgerLink Health Monitor"
    python = sys.executable or "python"
    script = Path(__file__).resolve()

    cmd = [
        "schtasks", "/create",
        "/tn", task_name,
        "/tr", f'"{python}" "{script}"',
        "/sc", "minute",
        "/mo", "5",
        "/ru", "SYSTEM",
        "/f",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print(f"Scheduled task '{task_name}' created successfully.")
            print(f"  Runs every 5 minutes as SYSTEM.")
            return 0
        else:
            print(f"Failed to create scheduled task: {result.stderr.strip()}")
            return 1
    except Exception as exc:
        print(f"Error creating scheduled task: {exc}")
        return 1


def main() -> int:
    _setup_logging()

    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd == "--register":
            return run_as_scheduled_task()
        elif cmd == "--check":
            results = check_health()
            for name, r in results.items():
                status = "OK" if r["ok"] else "FAIL"
                print(f"  [{status}] {name}: {r['message']}")
            failed = [n for n, r in results.items() if not r["ok"]]
            return 0 if not failed else 1
        elif cmd in ("--help", "-h"):
            print("LedgerLink Health Monitor")
            print()
            print("Usage:")
            print("  python service_health.py           Run health check + auto-heal cycle")
            print("  python service_health.py --check   Run checks only (no healing)")
            print("  python service_health.py --register Register as Windows Scheduled Task")
            print("  python service_health.py --help    Show this help")
            return 0

    # Default: run health cycle
    report = run_health_cycle()
    failed = [n for n, r in report["checks"].items() if not r["ok"]]
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
