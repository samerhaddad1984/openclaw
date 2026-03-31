from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.tools.qbo_online_adapter import post_one_ready_job as qbo_post_one_ready_job


DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"
EXPORTS_DIR = ROOT_DIR / "exports"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def normalize_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def ensure_posting_jobs_table(db_path: Path = DB_PATH) -> None:
    with open_db(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS posting_jobs (
                posting_id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                target_system TEXT NOT NULL,
                entry_kind TEXT NOT NULL,
                posting_status TEXT NOT NULL,
                approval_state TEXT NOT NULL,
                reviewer TEXT,
                external_id TEXT,
                payload_json TEXT NOT NULL,
                error_text TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_posting_jobs_document_id
            ON posting_jobs(document_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_posting_jobs_target_system
            ON posting_jobs(target_system)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_posting_jobs_posting_status
            ON posting_jobs(posting_status)
            """
        )
        conn.commit()


def list_ready_jobs(
    *,
    db_path: Path = DB_PATH,
    target_system: Optional[str] = None,
) -> list[sqlite3.Row]:
    ensure_posting_jobs_table(db_path)

    where_clauses = [
        "approval_state = 'approved_for_posting'",
        "posting_status = 'ready_to_post'",
    ]
    params: list[Any] = []

    if target_system:
        where_clauses.append("target_system = ?")
        params.append(target_system)

    where_sql = " AND ".join(where_clauses)

    with open_db(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM posting_jobs
            WHERE {where_sql}
            ORDER BY updated_at ASC, created_at ASC, posting_id ASC
            """,
            tuple(params),
        ).fetchall()

    return list(rows)


def dispatch_posting_job(
    posting_id: str,
    target_system: str,
) -> dict[str, Any]:
    target_system = (target_system or "").strip().lower()

    if target_system == "qbo":
        return qbo_post_one_ready_job(posting_id)

    if target_system == "xero":
        raise RuntimeError(
            "Xero posting is not built yet. Create src/agents/tools/xero_adapter.py before using target_system='xero'."
        )

    raise ValueError(f"Unsupported target_system: {target_system}")


def run_posting_queue(
    *,
    db_path: Path = DB_PATH,
    target_system: Optional[str] = None,
    stop_on_error: bool = False,
) -> dict[str, Any]:
    ready_rows = list_ready_jobs(db_path=db_path, target_system=target_system)

    results: list[dict[str, Any]] = []
    posted_count = 0
    failed_count = 0

    for row in ready_rows:
        posting_id = str(row["posting_id"])
        system_name = normalize_text(row["target_system"]) or ""

        try:
            result = dispatch_posting_job(posting_id, system_name)
        except Exception as exc:
            result = {
                "posting_id": posting_id,
                "status": "post_failed",
                "error": str(exc),
                "target_system": system_name,
            }

        results.append(result)

        if result.get("status") == "posted":
            posted_count += 1
        else:
            failed_count += 1
            if stop_on_error:
                break

    return {
        "run_at": utc_now_iso(),
        "target_system": target_system,
        "ready_job_count": len(ready_rows),
        "posted_count": posted_count,
        "failed_count": failed_count,
        "results": results,
    }


def export_run_results(
    run_results: dict[str, Any],
    *,
    out_path: Optional[Path] = None,
) -> Path:
    if out_path is None:
        suffix = ""
        target_system = normalize_text(run_results.get("target_system"))
        if target_system:
            suffix = f"_{target_system}"
        out_path = EXPORTS_DIR / f"posting_queue_run{suffix}.json"

    out_path.write_text(
        json.dumps(run_results, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return out_path


def print_summary(run_results: dict[str, Any]) -> None:
    print()
    print("POSTING QUEUE SUMMARY")
    print("-" * 100)
    print(f"Run at          : {run_results.get('run_at')}")
    print(f"Target system   : {run_results.get('target_system') or 'all'}")
    print(f"Ready jobs      : {run_results.get('ready_job_count', 0)}")
    print(f"Posted          : {run_results.get('posted_count', 0)}")
    print(f"Failed          : {run_results.get('failed_count', 0)}")

    results = run_results.get("results", []) or []
    if results:
        print()
        print("RESULTS")
        print("-" * 100)
        for item in results:
            posting_id = item.get("posting_id", "")
            status = item.get("status", "")
            external_id = item.get("external_id", "")
            error = item.get("error", "")

            if status == "posted":
                print(f"{posting_id} -> posted (external_id={external_id})")
            else:
                print(f"{posting_id} -> {status} ({error})")


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run approved posting jobs queue")
    parser.add_argument(
        "--target-system",
        choices=["qbo", "xero"],
        default="",
        help="Only post jobs for one target system",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop queue execution on first failed job",
    )
    parser.add_argument(
        "--out",
        default="",
        help="Optional output file path for run results JSON",
    )

    args = parser.parse_args()

    target_system = args.target_system.strip() or None
    out_path = Path(args.out) if args.out else None

    run_results = run_posting_queue(
        db_path=DB_PATH,
        target_system=target_system,
        stop_on_error=args.stop_on_error,
    )

    saved_to = export_run_results(run_results, out_path=out_path)
    print_summary(run_results)
    print()
    print(f"Saved results to: {saved_to}")

    return 0 if run_results.get("failed_count", 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())