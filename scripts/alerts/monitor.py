"""Owner-alert cron entry point.

Run every 5 minutes by ``/etc/cron.d/otocpa-alerts``. Computes
owner_dashboard.detect_anomalies and dispatches email / SMS via the
integrations already wired into the repo.

Exit codes:
- 0: no anomalies detected
- 1: anomalies found + dispatched (still a clean run from cron's POV)
- 2: module import or DB access failure (cron will alert)
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

env_path = ROOT / '.env'
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, _, v = line.partition('=')
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


LOG_DIR = Path(os.environ.get('OTOCPA_LOG_DIR', '/var/log/otocpa'))
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=str(LOG_DIR / 'alerts.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)


def main() -> int:
    try:
        from src.integrations.owner_dashboard import (
            detect_anomalies, dispatch_alerts,
        )
    except ImportError as exc:
        print(f"monitor: import failed: {exc}", file=sys.stderr)
        return 2

    db_path = Path(os.environ.get(
        'OTOCPA_DB_PATH', str(ROOT / 'data' / 'otocpa_agent.db'),
    ))
    try:
        alerts = detect_anomalies(db_path)
    except Exception as exc:  # noqa: BLE001
        logging.exception("detect_anomalies failed: %s", exc)
        print(f"monitor: detect failed: {exc}", file=sys.stderr)
        return 2

    if not alerts:
        logging.info("no anomalies detected")
        return 0

    logging.warning("anomalies detected: %d", len(alerts))

    email_fn = None
    sms_fn = None
    try:
        from src.integrations.email_client import send_notification_email
        email_fn = send_notification_email
    except Exception:
        pass
    try:
        from src.integrations.whatsapp import send_sms
        sms_fn = send_sms
    except Exception:
        pass

    to_email = os.environ.get('OWNER_ALERT_EMAIL') or os.environ.get('NOTIFICATION_EMAIL')
    to_sms = os.environ.get('OWNER_ALERT_SMS')
    out = dispatch_alerts(
        alerts, email_fn=email_fn, sms_fn=sms_fn,
        to_email=to_email, to_sms=to_sms,
    )
    logging.info("dispatch tally: %s", out)
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
