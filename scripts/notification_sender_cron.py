#!/usr/bin/env python3
"""Cron entry point: drain queued client_notifications.

Run every 5 minutes via /etc/cron.d/otocpa-notifications. Writes a
summary line per run to /var/log/otocpa/notifications.log (if the log
directory exists); prints to stdout otherwise.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations.notification_sender import (  # noqa: E402
    send_pending_notifications,
)

DB_PATH = ROOT / "data" / "otocpa_agent.db"


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    if not DB_PATH.exists():
        print(f"[notification_sender] DB not found at {DB_PATH}; skipping.")
        return 0
    result = send_pending_notifications(DB_PATH)
    print(f"[notification_sender] {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
