from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"
EXPORTS_DIR = ROOT_DIR / "exports"

from src.agents.core.auto_approval_engine import AutoApprovalEngine
from src.agents.core.duplicate_guard import DuplicateGuard
from src.agents.core.openclaw_case_orchestrator import OpenClawCaseOrchestrator


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_key(value: Any) -> str:
    return " ".join(normalize_text(value).lower().split())


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def fetch_documents_for_queue(limit: int = 20) -> list[sqlite3.Row]:
    with open_db() as conn:
        has_posting_jobs = table_exists(conn, "posting_jobs")

        if has_posting_jobs:
            rows = conn.execute(
                """
                SELECT
                    d.*,
                    COALESCE(p.posting_id, '') AS posting_id,
                    COALESCE(p.posting_status, '') AS posting_status,
                    COALESCE(p.approval_state, '') AS approval_state
                FROM documents d
                LEFT JOIN posting_jobs p
                    ON p.document_id = d.document_id
                WHERE COALESCE(d.document_id, '') != ''
                ORDER BY
                    CASE
                        WHEN COALESCE(d.review_status, '') IN ('NeedsReview', 'Ready') THEN 0
                        ELSE 1
                    END,
                    COALESCE(d.updated_at, d.created_at, '') DESC,
                    COALESCE(d.document_id, '') ASC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                    d.*,
                    '' AS posting_id,
                    '' AS posting_status,
                    '' AS approval_state
                FROM documents d
                WHERE COALESCE(d.document_id, '') != ''
                ORDER BY
                    CASE
                        WHEN COALESCE(d.review_status, '') IN ('NeedsReview', 'Ready') THEN 0
                        ELSE 1
                    END,
                    COALESCE(d.updated_at, d.created_at, '') DESC,
                    COALESCE(d.document_id, '') ASC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()

    return list(rows)


def route_from_auto_result(auto_result: dict[str, Any]) -> str:
    decision = normalize_key(auto_result.get("decision"))

    if decision == "auto_post":
        return "auto_post"
    if decision == "approve_but_hold":
        return "approve_but_hold"
    if decision == "needs_review":
        return "review"
    if decision == "block_posting":
        return "block_posting"

    return "unknown"


def duplicate_bucket(duplicate_result: dict[str, Any]) -> str:
    risk = normalize_key(duplicate_result.get("risk_level"))
    if risk in {"high", "medium", "low", "none"}:
        return risk
    return "none"


def normalize_orchestrator_step(orchestrator_result: dict[str, Any]) -> str:
    if not isinstance(orchestrator_result, dict):
        return "unknown"

    next_step = normalize_key(orchestrator_result.get("next_step"))
    if next_step:
        return next_step

    action = normalize_key(orchestrator_result.get("action"))
    if action:
        return action

    actions_attempted = orchestrator_result.get("actions_attempted")
    if isinstance(actions_attempted, list) and actions_attempted:
        first = normalize_key(actions_attempted[0])
        if first:
            return first

    return "unknown"


def veto_reason(auto_result: dict[str, Any]) -> str:
    if bool(auto_result.get("auto_approved")):
        return ""

    if not bool(auto_result.get("vendor_memory_ok")) and not normalize_text(auto_result.get("document_id")):
        return "missing_document"

    if not bool(auto_result.get("vendor_memory_ok")):
        return "missing_vendor"

    if not bool(auto_result.get("document_confidence_ok")):
        if bool(auto_result.get("amount_suspicious")):
            return "missing_amount"
        return "low_confidence"

    if bool(auto_result.get("amount_suspicious")):
        return "amount_suspicious"

    return normalize_text(auto_result.get("reason")) or "needs_review"


def build_summary() -> dict[str, int]:
    return {
        "documents_seen": 0,
        "needs_review": 0,
        "ready": 0,
        "exception": 0,
        "duplicate_high": 0,
        "duplicate_medium": 0,
        "duplicate_low": 0,
        "duplicate_none": 0,
        "auto_approved_true": 0,
        "auto_approved_false": 0,
        "vendor_memory_ok_true": 0,
        "vendor_memory_ok_false": 0,
        "document_confidence_ok_true": 0,
        "document_confidence_ok_false": 0,
        "amount_suspicious_true": 0,
        "amount_suspicious_false": 0,
        "route_auto_post": 0,
        "route_approve_but_hold": 0,
        "route_review": 0,
        "route_block_posting": 0,
        "route_unknown": 0,
        "orc_create_posting_job": 0,
        "orc_approve_for_posting": 0,
        "orc_post_now": 0,
        "orc_hold_for_review": 0,
        "orc_block_as_duplicate": 0,
        "orc_mark_exception": 0,
        "orc_do_nothing": 0,
        "orc_unknown": 0,
    }


def run_diagnostics(limit: int = 20) -> dict[str, Any]:
    rows = fetch_documents_for_queue(limit=limit)

    auto_engine = AutoApprovalEngine()
    duplicate_guard = DuplicateGuard()
    orchestrator = OpenClawCaseOrchestrator()

    summary = build_summary()
    results: list[dict[str, Any]] = []

    for row in rows:
        document_id = normalize_text(row["document_id"])

        duplicate_result = duplicate_guard.evaluate_document(
            document_id=document_id,
            limit=10,
        )

        auto_result = auto_engine.evaluate_document(
            dict(row),
            duplicate_result=duplicate_result,
        )

        orchestrator_result = orchestrator.evaluate_document(
            document_id=document_id,
            execute=False,
        )

        route = route_from_auto_result(auto_result)
        orc = normalize_orchestrator_step(orchestrator_result)
        dup = duplicate_bucket(duplicate_result)
        auto = bool(auto_result.get("auto_approved"))
        veto = veto_reason(auto_result)

        review_status = normalize_key(row["review_status"])
        if review_status in {"needsreview", "ready"}:
            if review_status == "needsreview":
                summary["needs_review"] += 1
            else:
                summary["ready"] += 1

        summary["documents_seen"] += 1
        summary[f"duplicate_{dup}"] += 1
        summary[f"route_{route}"] += 1 if f"route_{route}" in summary else 0
        summary[f"orc_{orc}"] += 1 if f"orc_{orc}" in summary else 0

        if auto:
            summary["auto_approved_true"] += 1
        else:
            summary["auto_approved_false"] += 1

        if bool(auto_result.get("vendor_memory_ok")):
            summary["vendor_memory_ok_true"] += 1
        else:
            summary["vendor_memory_ok_false"] += 1

        if bool(auto_result.get("document_confidence_ok")):
            summary["document_confidence_ok_true"] += 1
        else:
            summary["document_confidence_ok_false"] += 1

        if bool(auto_result.get("amount_suspicious")):
            summary["amount_suspicious_true"] += 1
        else:
            summary["amount_suspicious_false"] += 1

        line = f"[OK] {document_id} -> route={route} | orc={orc} | dup={dup} | auto={str(auto)}"
        if veto:
            line += f" | veto={veto}"
        print(line)

        results.append(
            {
                "document_id": document_id,
                "route": route,
                "orc": orc,
                "dup": dup,
                "auto": auto,
                "veto": veto,
                "duplicate_result": duplicate_result,
                "auto_result": auto_result,
                "orchestrator_result": orchestrator_result,
            }
        )

    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    export_path = EXPORTS_DIR / "openclaw_case_diagnostics.json"
    payload = {
        "summary": summary,
        "results": results,
    }
    export_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    print()
    print("OPENCLAW CASE DIAGNOSTICS SUMMARY")
    print("=" * 100)
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print()
    print(f"Saved diagnostics to: {export_path}")

    return payload


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="OpenClaw case diagnostics")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    run_diagnostics(limit=max(1, int(args.limit)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
