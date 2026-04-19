#!/usr/bin/env python3
"""Sprint I Part 3 — daily anomaly-detector cron job.

Runs the Sprint G + H detectors against every active firm/client and
persists findings to ``anomaly_findings``. Intended to be invoked from
crontab:

    0 2 * * *  /usr/bin/python3 /opt/otocpa/scripts/run_daily_detectors.py
    0 3 * * 1  /usr/bin/python3 /opt/otocpa/scripts/run_daily_detectors.py --weekly

Usage:
    --weekly   include Benford + vendor-typo (heavy detectors)
    --client   only run for this client_code
    --dry-run  print what would be persisted, don't write
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.approval_graph_engine import (  # noqa: E402
    detect_circular_approvals,
    ensure_anomaly_findings_table,
    persist_findings,
)
from src.engines.benford_engine import (  # noqa: E402
    analyze_benford_compliance,
    detect_round_dollar_spike,
)
from src.engines.phantom_employee_engine import (  # noqa: E402
    detect_phantom_employee_expenses,
)
from src.engines.vendor_typo_engine import (  # noqa: E402
    detect_vendor_typos_refined,
)


DB_PATH = ROOT / "data" / "otocpa_agent.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(ROOT / "data" / "daily_detectors.log")),
    ],
)
log = logging.getLogger("daily_detectors")


def _list_clients(conn: sqlite3.Connection, only: str = "") -> list[str]:
    """Return distinct client_codes seen in documents."""
    if only:
        return [only]
    try:
        rows = conn.execute(
            "SELECT DISTINCT client_code FROM documents "
            "WHERE TRIM(COALESCE(client_code, '')) != ''",
        ).fetchall()
        return [r[0] for r in rows]
    except sqlite3.OperationalError:
        return []


def _persist(
    conn: sqlite3.Connection,
    client_code: str,
    detector: str,
    findings: list,
    dry_run: bool,
) -> int:
    if not findings:
        return 0
    if dry_run:
        log.info("DRY RUN: %s [%s] would persist %d findings",
                 client_code, detector, len(findings))
        return 0
    return persist_findings(conn, findings, client_code=client_code,
                             detector=detector)


def run(client_only: str = "", weekly: bool = False, dry_run: bool = False) -> dict:
    if not DB_PATH.exists():
        log.error("DB not found at %s", DB_PATH)
        return {"error": "no_db"}

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_anomaly_findings_table(conn)
    started = datetime.now(timezone.utc).isoformat()
    summary: dict = {"started_at": started, "weekly": weekly,
                      "dry_run": dry_run, "by_client": {}}

    try:
        clients = _list_clients(conn, only=client_only)
        log.info("Running detectors for %d clients (weekly=%s)", len(clients), weekly)

        totals = {"circular_approval": 0, "phantom_employee": 0,
                   "round_dollar": 0, "benford": 0, "vendor_typo": 0,
                   "errors": 0}

        for client in clients:
            client_summary = {}
            log.info("--- client %s ---", client)
            # 1. Circular approvals (daily)
            try:
                f = detect_circular_approvals(client_code=client, db_path=DB_PATH)
                n = _persist(conn, client, "circular_approval", f, dry_run)
                client_summary["circular_approval"] = len(f)
                totals["circular_approval"] += n
            except Exception as e:
                log.warning("circular_approval failed: %s", e)
                totals["errors"] += 1

            # 2. Phantom employee (daily)
            try:
                f = detect_phantom_employee_expenses(client_code=client, db_path=DB_PATH)
                n = _persist(conn, client, "phantom_employee", f, dry_run)
                client_summary["phantom_employee"] = len(f)
                totals["phantom_employee"] += n
            except Exception as e:
                log.warning("phantom_employee failed: %s", e)
                totals["errors"] += 1

            # 3. Round-dollar spike (daily, lightweight)
            try:
                r = detect_round_dollar_spike(client_code=client, db_path=DB_PATH)
                if r.get("significant"):
                    n = _persist(conn, client, "round_dollar_spike", [r], dry_run)
                    totals["round_dollar"] += n
                client_summary["round_dollar_significant"] = bool(r.get("significant"))
            except Exception as e:
                log.warning("round_dollar failed: %s", e)
                totals["errors"] += 1

            # 4. Benford (weekly only — needs >= 50 samples)
            if weekly:
                try:
                    r = analyze_benford_compliance(client_code=client, db_path=DB_PATH)
                    if r.get("status") == "ok" and r.get("significant_deviation"):
                        n = _persist(conn, client, "benford", [r], dry_run)
                        totals["benford"] += n
                    client_summary["benford_status"] = r.get("status")
                except Exception as e:
                    log.warning("benford failed: %s", e)
                    totals["errors"] += 1

                # 5. Vendor typo (weekly — O(n²) cost)
                try:
                    f = detect_vendor_typos_refined(client_code=client, db_path=DB_PATH)
                    n = _persist(conn, client, "vendor_typo", f, dry_run)
                    client_summary["vendor_typo_pairs"] = len(f)
                    totals["vendor_typo"] += n
                except Exception as e:
                    log.warning("vendor_typo failed: %s", e)
                    totals["errors"] += 1

            summary["by_client"][client] = client_summary
        summary["totals"] = totals
        summary["completed_at"] = datetime.now(timezone.utc).isoformat()
        log.info("Daily detector run complete: %s", totals)
        return summary
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--weekly", action="store_true",
                    help="include Benford + vendor-typo (heavy)")
    ap.add_argument("--client", default="",
                    help="restrict to a single client_code")
    ap.add_argument("--dry-run", action="store_true",
                    help="don't persist; log only")
    args = ap.parse_args(argv)
    summary = run(client_only=args.client, weekly=args.weekly,
                   dry_run=args.dry_run)
    import json as _json
    print(_json.dumps(summary, default=str, indent=2))
    return 0 if "error" not in summary else 1


if __name__ == "__main__":
    sys.exit(main())
