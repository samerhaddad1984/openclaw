#!/usr/bin/env python3
"""
installer/service_wrapper.py — OtoCPA Windows Service Manager
=================================================================
Install, start, stop, restart, and remove OtoCPA as a Windows service.

Usage:
    python installer/service_wrapper.py install
    python installer/service_wrapper.py start
    python installer/service_wrapper.py stop
    python installer/service_wrapper.py restart
    python installer/service_wrapper.py remove
    python installer/service_wrapper.py status
"""
from __future__ import annotations

import os
import platform
import subprocess
import sys
import time
from pathlib import Path

SERVICE_NAME = "OtoCPA"
SERVICE_DISPLAY = "OtoCPA Accounting"
SERVICE_DESC = "OtoCPA — Intelligent Accounting Document Queue & Client Portal"
ROOT_DIR = Path(__file__).resolve().parent.parent
DASHBOARD_SCRIPT = ROOT_DIR / "scripts" / "review_dashboard.py"
RUN_BAT = ROOT_DIR / "run_otocpa.bat"
DASH_PORT = 8787
PORTAL_PORT = 8788


def _find_python() -> str:
    """Find the best Python executable."""
    # Prefer the Python running this script
    py = sys.executable
    if py and Path(py).exists():
        return py

    # Search common locations
    for candidate in [
        Path(r"C:\Program Files\Python311\python.exe"),
        Path(r"C:\Program Files\Python312\python.exe"),
        Path(r"C:\Python311\python.exe"),
        Path(r"C:\Python312\python.exe"),
    ]:
        if candidate.exists():
            return str(candidate)

    # Fallback to PATH
    result = subprocess.run(["where", "python"], capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout.strip().splitlines()[0]

    return "python"


def _run(cmd: list[str], check: bool = False) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if check and result.returncode != 0:
        print(f"  ERROR: {' '.join(cmd)}")
        if result.stderr:
            print(f"  {result.stderr.strip()[:200]}")
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return result


def _service_exists() -> bool:
    """Check if the service is registered."""
    result = _run(["sc", "query", SERVICE_NAME])
    return result.returncode == 0


def _service_running() -> bool:
    """Check if the service is currently running."""
    result = _run(["sc", "query", SERVICE_NAME])
    return "RUNNING" in result.stdout


def _create_run_bat(python: str) -> None:
    """Create the batch file that the service will execute."""
    content = (
        f'@echo off\r\n'
        f'cd /d "{ROOT_DIR}"\r\n'
        f'"{python}" "{DASHBOARD_SCRIPT}"\r\n'
    )
    RUN_BAT.write_text(content, encoding="utf-8")


def _find_nssm() -> str | None:
    """Find NSSM (Non-Sucking Service Manager) if available."""
    nssm = ROOT_DIR / "tools" / "nssm.exe"
    if nssm.exists():
        return str(nssm)

    # Check PATH
    result = subprocess.run(["where", "nssm"], capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout.strip().splitlines()[0]

    return None


def install() -> int:
    """Install OtoCPA as a Windows service."""
    print(f"Installing service '{SERVICE_NAME}'...")

    python = _find_python()
    print(f"  Python: {python}")
    print(f"  Dashboard: {DASHBOARD_SCRIPT}")

    if not DASHBOARD_SCRIPT.exists():
        print(f"  ERROR: {DASHBOARD_SCRIPT} not found")
        return 1

    # Remove existing service if present
    if _service_exists():
        print(f"  Removing existing service...")
        stop()
        time.sleep(2)
        _run(["sc", "delete", SERVICE_NAME])
        time.sleep(1)

    nssm = _find_nssm()
    if nssm:
        # Use NSSM for proper service management
        print(f"  Using NSSM: {nssm}")
        _run([nssm, "install", SERVICE_NAME, python, str(DASHBOARD_SCRIPT)], check=True)
        _run([nssm, "set", SERVICE_NAME, "DisplayName", SERVICE_DISPLAY])
        _run([nssm, "set", SERVICE_NAME, "Description", SERVICE_DESC])
        _run([nssm, "set", SERVICE_NAME, "Start", "SERVICE_AUTO_START"])
        _run([nssm, "set", SERVICE_NAME, "AppDirectory", str(ROOT_DIR)])
        _run([nssm, "set", SERVICE_NAME, "AppStdout", str(ROOT_DIR / "logs" / "service_stdout.log")])
        _run([nssm, "set", SERVICE_NAME, "AppStderr", str(ROOT_DIR / "logs" / "service_stderr.log")])

        # Create logs directory
        (ROOT_DIR / "logs").mkdir(exist_ok=True)

    else:
        # Fallback: use sc.exe with a batch wrapper
        print(f"  Using sc.exe (NSSM not found)")
        _create_run_bat(python)

        bin_path = f'"{python}" "{DASHBOARD_SCRIPT}"'
        result = _run([
            "sc", "create", SERVICE_NAME,
            f"binPath={bin_path}",
            "start=auto",
            f"DisplayName={SERVICE_DISPLAY}",
        ])
        if result.returncode != 0:
            print(f"  WARNING: sc create returned {result.returncode}")
            if result.stderr:
                print(f"  {result.stderr.strip()[:200]}")

        _run(["sc", "description", SERVICE_NAME, SERVICE_DESC])

    # Also create the startup batch for fallback auto-start
    _create_run_bat(python)

    print(f"  Service '{SERVICE_NAME}' installed successfully")
    return 0


def start() -> int:
    """Start the OtoCPA service."""
    print(f"Starting service '{SERVICE_NAME}'...")

    if not _service_exists():
        print(f"  Service not installed — starting dashboard directly...")
        python = _find_python()
        subprocess.Popen(
            [python, str(DASHBOARD_SCRIPT)],
            cwd=str(ROOT_DIR),
            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
        )
        print(f"  Dashboard started on port {DASH_PORT}")
        return 0

    nssm = _find_nssm()
    if nssm:
        result = _run([nssm, "start", SERVICE_NAME])
    else:
        result = _run(["sc", "start", SERVICE_NAME])

    if result.returncode == 0 or "RUNNING" in result.stdout:
        print(f"  Service started")
        return 0

    # Fallback: start directly
    print(f"  Service start failed — starting dashboard directly...")
    python = _find_python()
    subprocess.Popen(
        [python, str(DASHBOARD_SCRIPT)],
        cwd=str(ROOT_DIR),
        creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
    )
    print(f"  Dashboard started directly on port {DASH_PORT}")
    return 0


def stop() -> int:
    """Stop the OtoCPA service."""
    print(f"Stopping service '{SERVICE_NAME}'...")

    if not _service_exists():
        print(f"  Service not installed")
        return 0

    nssm = _find_nssm()
    if nssm:
        _run([nssm, "stop", SERVICE_NAME])
    else:
        _run(["sc", "stop", SERVICE_NAME])

    # Wait for stop
    for _ in range(10):
        if not _service_running():
            print(f"  Service stopped")
            return 0
        time.sleep(1)

    print(f"  WARNING: Service may still be running")
    return 1


def restart() -> int:
    """Restart the OtoCPA service."""
    stop()
    time.sleep(2)
    return start()


def remove() -> int:
    """Remove the OtoCPA service."""
    print(f"Removing service '{SERVICE_NAME}'...")

    if not _service_exists():
        print(f"  Service not installed")
        return 0

    stop()
    time.sleep(2)

    nssm = _find_nssm()
    if nssm:
        _run([nssm, "remove", SERVICE_NAME, "confirm"])
    else:
        _run(["sc", "delete", SERVICE_NAME])

    # Clean up
    if RUN_BAT.exists():
        RUN_BAT.unlink()

    print(f"  Service removed")
    return 0


def status() -> int:
    """Show service status."""
    if not _service_exists():
        print(f"Service '{SERVICE_NAME}': NOT INSTALLED")
        return 1

    result = _run(["sc", "query", SERVICE_NAME])
    if "RUNNING" in result.stdout:
        print(f"Service '{SERVICE_NAME}': RUNNING")
    elif "STOPPED" in result.stdout:
        print(f"Service '{SERVICE_NAME}': STOPPED")
    elif "PAUSED" in result.stdout:
        print(f"Service '{SERVICE_NAME}': PAUSED")
    else:
        print(f"Service '{SERVICE_NAME}': UNKNOWN")
        print(result.stdout)

    return 0


COMMANDS = {
    "install": install,
    "start": start,
    "stop": stop,
    "restart": restart,
    "remove": remove,
    "uninstall": remove,
    "status": status,
}


def main() -> int:
    if platform.system() != "Windows":
        print("ERROR: This script is designed for Windows only.")
        return 1

    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print("OtoCPA — Service Manager")
        print()
        print("Usage:")
        for cmd in COMMANDS:
            print(f"  python service_wrapper.py {cmd}")
        return 1

    command = sys.argv[1]
    return COMMANDS[command]()


if __name__ == "__main__":
    raise SystemExit(main())
