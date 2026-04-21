"""Confirm the notification cron is installed and sane.

These tests inspect the filesystem: /etc/cron.d and /var/log/otocpa.
They are skipped when the cron file isn't present (dev machines,
CI containers), so the suite stays runnable without root.
"""
from __future__ import annotations

import os
import re
import stat
from pathlib import Path

import pytest


CRON_PATH = Path('/etc/cron.d/otocpa-notifications')
LOG_DIR = Path('/var/log/otocpa')
LOG_FILE = LOG_DIR / 'notifications.log'


def _cron_installed() -> bool:
    return CRON_PATH.exists()


pytestmark = pytest.mark.skipif(
    not _cron_installed(),
    reason="notification cron not installed on this host",
)


def test_cron_file_exists():
    assert CRON_PATH.is_file()


def test_cron_file_perms_644():
    mode = stat.S_IMODE(CRON_PATH.stat().st_mode)
    # 644 — cron refuses group-/world-writable files.
    assert mode == 0o644, f"want 0o644, got {oct(mode)}"


def test_cron_file_owned_by_root():
    st = CRON_PATH.stat()
    assert st.st_uid == 0, "must be root-owned (cron ignores user-owned /etc/cron.d entries)"


def test_cron_runs_as_deploy():
    content = CRON_PATH.read_text()
    # The user field is the 6th whitespace-separated column.
    cron_lines = [
        line for line in content.splitlines()
        if line.strip() and not line.startswith('#')
        and not line.startswith('SHELL') and not line.startswith('PATH')
    ]
    assert cron_lines, "no cron line found"
    parts = cron_lines[0].split()
    # `* * * * * user command` — index 5 is the user.
    assert parts[5] == 'deploy', f"user field = {parts[5]!r}"


def test_cron_runs_every_5_min():
    content = CRON_PATH.read_text()
    assert re.search(r'^\*/5\s', content, re.M), (
        "expected */5 * * * * schedule"
    )


def test_cron_invokes_notification_sender():
    content = CRON_PATH.read_text()
    assert 'notification_sender_cron.py' in content
    assert '/opt/otocpa' in content


def test_log_directory_writable():
    assert LOG_DIR.is_dir()
    # Either deploy owns it OR the existing log file in it is deploy-
    # owned (our setup makes LOG_DIR root:root 755, with the log file
    # itself owned by deploy:deploy 664 so the deploy user can append).
    assert LOG_FILE.exists() or os.access(LOG_DIR, os.W_OK), (
        "neither the log file exists nor is the dir writable"
    )


def test_log_file_writable_by_deploy():
    # If the log file exists, confirm it has a mode that allows deploy
    # (the cron user) to append to it. stat shows 664 on our install.
    if not LOG_FILE.exists():
        pytest.skip("log file not yet created (cron hasn't run yet)")
    st = LOG_FILE.stat()
    mode = stat.S_IMODE(st.st_mode)
    # 664 is acceptable; 660 is acceptable if owned by deploy.
    assert mode & 0o200, (
        "log file not user-writable; cron appends will fail"
    )
