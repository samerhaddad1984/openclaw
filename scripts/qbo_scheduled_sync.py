"""Entry point invoked by the QBO bidirectional-sync cron job.

Runs :func:`src.integrations.qbo_sync.scheduled_sync_all` against the
production DB. Logs a one-line summary and exits. Intended to be
called every 15 minutes by ``/etc/cron.d/otocpa-qbo-sync``.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

env_path = ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


LOG_DIR = Path(os.environ.get("OTOCPA_LOG_DIR", "/var/log/otocpa"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "qbo_sync.log"

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def main() -> int:
    db_path = Path(os.environ.get(
        "OTOCPA_DB_PATH", str(ROOT / "data" / "otocpa_agent.db"),
    ))
    sandbox = os.environ.get("QBO_ENVIRONMENT", "").lower() == "sandbox"
    started = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    from src.integrations.qbo_sync import scheduled_sync_all

    try:
        rollup = scheduled_sync_all(db_path, sandbox=sandbox)
    except Exception as exc:  # noqa: BLE001
        logging.exception("qbo_sync cron fatal: %s", exc)
        print(f"qbo_sync: FAILED {exc}", file=sys.stderr)
        return 1

    ok = sum(1 for r in rollup.get("results", {}).values() if r.get("ok"))
    bad = rollup.get("connections", 0) - ok
    logging.info(
        "qbo_sync started=%s connections=%d ok=%d failed=%d details=%s",
        started, rollup.get("connections", 0), ok, bad,
        json.dumps(rollup.get("results", {}))[:600],
    )
    print(f"qbo_sync: started={started} ok={ok}/{rollup.get('connections', 0)}")
    return 0 if bad == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
