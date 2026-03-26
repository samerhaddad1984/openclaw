
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.openclaw_case_orchestrator import OpenClawCaseOrchestrator

DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def fetch_documents_for_queue(
    *,
    statuses: list[str] | None = None,
    limit: int = 50,
) -> list[sqlite3.Row]:
    where_clauses: list[str] = [
        "COALESCE(document_id, '') != ''",
        "(review_status IS NULL OR review_status != 'Ignored')",
    ]
    params: list[Any] = []

    if statuses:
        placeholders = ",".join("?" for _ in statuses)
        where_clauses.append(f"review_status IN ({placeholders})")
        params.extend(statuses)

    where_sql = " AND ".join(where_clauses)

    query = f"""
        SELECT
            document_id,
            file_name,
            vendor,
            client_code,
            amount,
            document_date,
            review_status,
            updated_at,
            created_at
        FROM documents
        WHERE {where_sql}
        ORDER BY
            CASE review_status
                WHEN 'NeedsReview' THEN 1
                WHEN 'Ready' THEN 2
                WHEN 'Exception' THEN 3
                ELSE 4
            END,
            updated_at DESC,
            created_at DESC
        LIMIT ?
    """

    params.append(limit)

    with open_db() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()

    return list(rows)


def build_summary(results: list[dict[str, Any]], *, plan_only: bool) -> dict[str, Any]:
    summary = {
        "mode": "plan_only" if plan_only else "execute",
        "documents_seen": len(results),
        "planned": 0,
        "success": 0,
        "failed": 0,
        "create_posting_job": 0,
        "approve_for_posting": 0,
        "post_now": 0,
        "hold_for_review": 0,
        "block_as_duplicate": 0,
        "mark_exception": 0,
        "do_nothing": 0,
    }

    for item in results:
        status = normalize_text(item.get("status"))
        next_step = normalize_text(item.get("next_step"))

        if status == "planned":
            summary["planned"] += 1
        elif status == "failed":
            summary["failed"] += 1
        else:
            summary["success"] += 1

        if next_step in summary:
            summary[next_step] += 1

    return summary


def parse_status_args(raw_status_values: list[str] | None) -> list[str]:
    if not raw_status_values:
        return ["Ready", "NeedsReview"]

    cleaned: list[str] = []
    for item in raw_status_values:
        for piece in str(item).split(","):
            text = normalize_text(piece)
            if text:
                cleaned.append(text)

    return cleaned or ["Ready", "NeedsReview"]


def force_execute_if_needed(
    *,
    orchestrator: OpenClawCaseOrchestrator,
    document_id: str,
    result: dict[str, Any],
    plan_only: bool,
) -> dict[str, Any]:
    """
    Defensive fallback.

    In execute mode, if run_case still returns status='planned',
    we manually execute the chosen step through the orchestrator's action path.
    """
    if plan_only:
        return result

    status = normalize_text(result.get("status"))
    if status != "planned":
        return result

    next_step = normalize_text(result.get("next_step"))
    reason = normalize_text(result.get("reason"))

    if not next_step:
        result["status"] = "failed"
        result.setdefault("action_results", [])
        result["action_results"].append(
            {
                "action": "unknown",
                "status": "failed",
                "error": "run_case returned planned without next_step",
            }
        )
        return result

    try:
        action_result = orchestrator._execute(document_id, next_step, reason)

        result["status"] = normalize_text(action_result.get("status")) or "success"
        result["actions_attempted"] = [next_step]
        result["action_results"] = [action_result]

        signals = result.get("signals", {}) or {}
        signals["forced_execution_fallback"] = True
        result["signals"] = signals

        return result

    except Exception as exc:
        result["status"] = "failed"
        result["actions_attempted"] = [next_step]
        result["action_results"] = [
            {
                "action": next_step,
                "status": "failed",
                "error": str(exc),
            }
        ]

        signals = result.get("signals", {}) or {}
        signals["forced_execution_fallback"] = True
        result["signals"] = signals

        return result


def run_queue(
    *,
    statuses: list[str] | None,
    limit: int,
    plan_only: bool,
) -> dict[str, Any]:
    orchestrator = OpenClawCaseOrchestrator()
    queue_rows = fetch_documents_for_queue(statuses=statuses, limit=limit)

    results: list[dict[str, Any]] = []

    for row in queue_rows:
        document_id = normalize_text(row["document_id"])
        if not document_id:
            continue

        try:
            result = orchestrator.run_case(
                document_id=document_id,
                execute_actions=not plan_only,
            )

            result = force_execute_if_needed(
                orchestrator=orchestrator,
                document_id=document_id,
                result=result,
                plan_only=plan_only,
            )

            results.append(result)

            raw_status = normalize_text(result.get("status"))
            if raw_status:
                status = raw_status.upper()
            else:
                status = "PLANNED" if plan_only else "UNKNOWN"

            next_step = normalize_text(result.get("next_step"))
            reason = normalize_text(result.get("reason"))

            print(f"[{status}] {document_id} -> {next_step} ({reason})")

            action_results = result.get("action_results", []) or []
            for action_result in action_results:
                action_name = normalize_text(action_result.get("action"))
                action_status = normalize_text(action_result.get("status"))
                external_id = normalize_text(action_result.get("external_id"))
                error = normalize_text(action_result.get("error"))

                extra_parts: list[str] = []
                if external_id:
                    extra_parts.append(f"external_id={external_id}")
                if error:
                    extra_parts.append(f"error={error}")

                signals = result.get("signals", {}) or {}
                if signals.get("forced_execution_fallback"):
                    extra_parts.append("forced_execution_fallback=true")

                extra_text = ""
                if extra_parts:
                    extra_text = " | " + " | ".join(extra_parts)

                print(f"    action_result: {action_name} -> {action_status}{extra_text}")

        except Exception as exc:
            failed = {
                "document_id": document_id,
                "next_step": "unhandled_error",
                "status": "failed",
                "reason": str(exc),
                "signals": {},
                "actions_attempted": [],
                "action_results": [],
            }
            results.append(failed)
            print(f"[FAILED] {document_id} -> {exc}")

    summary = build_summary(results, plan_only=plan_only)

    return {
        "filters": {
            "statuses": statuses or [],
            "limit": limit,
            "plan_only": plan_only,
        },
        "summary": summary,
        "results": results,
    }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run LedgerLink OpenClaw queue")
    parser.add_argument(
        "--status",
        action="append",
        help="Document status filter. Can be repeated or comma-separated. Default: Ready,NeedsReview",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of documents to process",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Plan actions without executing them",
    )

    args = parser.parse_args()

    statuses = parse_status_args(args.status)

    output = run_queue(
        statuses=statuses,
        limit=max(1, int(args.limit)),
        plan_only=bool(args.plan_only),
    )

    print()
    print("OPENCLAW QUEUE SUMMARY")
    print("=" * 100)
    print(json.dumps(output["summary"], indent=2, ensure_ascii=False))

    return 0 if output["summary"]["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
