from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
SCRIPTS_DIR = ROOT_DIR / "scripts"
EXPORTS_DIR = ROOT_DIR / "exports"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def safe_json_loads(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        loaded = json.loads(str(value))
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


@dataclass
class StageResult:
    stage_name: str
    success: bool
    started_at: str
    finished_at: str
    return_code: int
    command: list[str]
    stdout: str
    stderr: str
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PipelineRunResult:
    run_at: str
    success: bool
    stages: list[dict[str, Any]]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def script_path(name: str) -> Path:
    path = SCRIPTS_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"Script not found: {path}")
    return path


def run_python_script(
    script_name: str,
    *,
    args: list[str] | None = None,
    timeout_seconds: int = 900,
) -> StageResult:
    args = args or []
    started_at = utc_now_iso()
    target_script = script_path(script_name)
    command = [sys.executable, str(target_script), *args]

    completed = subprocess.run(
        command,
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )

    finished_at = utc_now_iso()
    success = completed.returncode == 0

    return StageResult(
        stage_name=script_name,
        success=success,
        started_at=started_at,
        finished_at=finished_at,
        return_code=completed.returncode,
        command=command,
        stdout=completed.stdout,
        stderr=completed.stderr,
        summary=parse_stage_summary(script_name, completed.stdout, completed.stderr, completed.returncode),
    )


def extract_first_int_after(text: str, label: str) -> int | None:
    for line in text.splitlines():
        if label.lower() in line.lower():
            parts = line.split(":")
            if len(parts) < 2:
                continue
            candidate = parts[-1].strip()
            try:
                return int(candidate)
            except Exception:
                continue
    return None


def parse_stage_summary(script_name: str, stdout: str, stderr: str, return_code: int) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "return_code": return_code,
        "stderr_present": bool(normalize_text(stderr)),
    }

    if script_name == "cleanup_bad_documents.py":
        summary["documents_scanned"] = extract_first_int_after(stdout, "Documents scanned")
        summary["documents_cleaned"] = extract_first_int_after(stdout, "Documents cleaned")
        return summary

    if script_name == "rebuild_document_store.py":
        summary["documents_scanned"] = extract_first_int_after(stdout, "Documents scanned")
        summary["documents_updated"] = extract_first_int_after(stdout, "Documents updated")
        return summary

    if script_name == "run_auto_review_classifier.py":
        summary["documents_scanned"] = extract_first_int_after(stdout, "Documents scanned")
        summary["statuses_updated"] = extract_first_int_after(stdout, "Statuses updated")
        return summary

    if script_name == "run_posting_queue.py":
        summary["ready_jobs"] = extract_first_int_after(stdout, "Ready jobs")
        summary["posted"] = extract_first_int_after(stdout, "Posted")
        summary["failed"] = extract_first_int_after(stdout, "Failed")
        return summary

    if script_name == "ingest_folder_to_store.py":
        summary["stored_documents"] = stdout.lower().count("document stored")
        summary["duplicate_skipped"] = stdout.lower().count("duplicate logical document skipped")
        return summary

    return summary


def get_review_queue_summary(db_path: Path = DB_PATH) -> dict[str, Any]:
    with open_db(db_path) as conn:
        rows = conn.execute(
            """
            SELECT review_status, COUNT(*) AS c
            FROM documents
            GROUP BY review_status
            """
        ).fetchall()

    counts = {
        "Ready": 0,
        "NeedsReview": 0,
        "Ignored": 0,
        "Exception": 0,
    }

    for row in rows:
        key = normalize_text(row["review_status"])
        counts[key] = int(row["c"])

    counts["total"] = sum(int(v) for v in counts.values())
    return counts


def get_posting_queue_summary(db_path: Path = DB_PATH) -> dict[str, Any]:
    summary = {
        "draft": 0,
        "ready_to_post": 0,
        "posted": 0,
        "post_failed": 0,
        "approved_for_posting": 0,
        "pending_human_approval": 0,
        "rejected": 0,
        "needs_review": 0,
        "total": 0,
    }

    with open_db(db_path) as conn:
        posting_status_rows = conn.execute(
            """
            SELECT posting_status, COUNT(*) AS c
            FROM posting_jobs
            GROUP BY posting_status
            """
        ).fetchall()

        approval_state_rows = conn.execute(
            """
            SELECT approval_state, COUNT(*) AS c
            FROM posting_jobs
            GROUP BY approval_state
            """
        ).fetchall()

    for row in posting_status_rows:
        key = normalize_text(row["posting_status"])
        summary[key] = int(row["c"])

    for row in approval_state_rows:
        key = normalize_text(row["approval_state"])
        summary[key] = int(row["c"])

    posting_total = 0
    for key in ["draft", "ready_to_post", "posted", "post_failed"]:
        posting_total += int(summary.get(key, 0))
    summary["total"] = posting_total

    return summary


def get_document_details(document_id: str, db_path: Path = DB_PATH) -> dict[str, Any]:
    with open_db(db_path) as conn:
        doc = conn.execute(
            """
            SELECT *
            FROM documents
            WHERE document_id = ?
            """,
            (document_id,),
        ).fetchone()

        posting = conn.execute(
            """
            SELECT *
            FROM posting_jobs
            WHERE document_id = ?
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """,
            (document_id,),
        ).fetchone()

    if doc is None:
        raise ValueError(f"Document not found: {document_id}")

    doc_dict = dict(doc)
    doc_dict["raw_result"] = safe_json_loads(doc_dict.get("raw_result"))

    posting_dict = dict(posting) if posting else None
    if posting_dict:
        posting_dict["payload_json"] = safe_json_loads(posting_dict.get("payload_json"))

    return {
        "document": doc_dict,
        "latest_posting_job": posting_dict,
    }


def list_documents_for_review(
    *,
    review_status: str | None = None,
    limit: int = 50,
    db_path: Path = DB_PATH,
) -> list[dict[str, Any]]:
    params: list[Any] = []
    where_sql = ""

    if review_status:
        where_sql = "WHERE review_status = ?"
        params.append(review_status)

    with open_db(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
                document_id,
                file_name,
                vendor,
                client_code,
                doc_type,
                amount,
                document_date,
                gl_account,
                tax_code,
                category,
                review_status,
                confidence,
                updated_at
            FROM documents
            {where_sql}
            ORDER BY
                CASE review_status
                    WHEN 'NeedsReview' THEN 1
                    WHEN 'Exception' THEN 2
                    WHEN 'Ready' THEN 3
                    WHEN 'Ignored' THEN 4
                    ELSE 5
                END,
                updated_at DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()

    return [dict(r) for r in rows]


def apply_document_fix(
    *,
    document_id: str,
    fields: dict[str, Any],
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    allowed = {
        "vendor",
        "client_code",
        "doc_type",
        "amount",
        "document_date",
        "gl_account",
        "tax_code",
        "category",
        "review_status",
    }

    updates: list[str] = []
    params: list[Any] = []

    for key, value in fields.items():
        if key not in allowed:
            continue

        if key == "amount":
            text = normalize_text(value)
            if text:
                try:
                    params.append(round(float(text.replace(",", "")), 2))
                except Exception:
                    params.append(None)
            else:
                params.append(None)
        else:
            text = normalize_text(value)
            params.append(text if text else None)

        updates.append(f"{key} = ?")

    if not updates:
        raise ValueError("No valid fields provided for update")

    updates.append("updated_at = ?")
    params.append(utc_now_iso())
    params.append(document_id)

    with open_db(db_path) as conn:
        conn.execute(
            f"""
            UPDATE documents
            SET {", ".join(updates)}
            WHERE document_id = ?
            """,
            tuple(params),
        )
        conn.commit()

    return get_document_details(document_id, db_path=db_path)


def build_posting_jobs_for_ready_documents(
    *,
    target_system: str = "qbo",
    entry_kind: str = "expense",
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    from src.agents.tools.posting_builder import build_posting_job

    built = 0
    failed = 0
    results: list[dict[str, Any]] = []

    with open_db(db_path) as conn:
        rows = conn.execute(
            """
            SELECT document_id
            FROM documents
            WHERE review_status = 'Ready'
            ORDER BY updated_at DESC
            """
        ).fetchall()

    for row in rows:
        document_id = normalize_text(row["document_id"])
        try:
            payload = build_posting_job(
                document_id=document_id,
                target_system=target_system,
                entry_kind=entry_kind,
                db_path=db_path,
            )
            built += 1
            results.append(
                {
                    "document_id": document_id,
                    "posting_id": payload.posting_id,
                    "status": payload.posting_status,
                    "approval_state": payload.approval_state,
                    "blocking_issues": payload.blocking_issues,
                }
            )
        except Exception as exc:
            failed += 1
            results.append(
                {
                    "document_id": document_id,
                    "status": "build_failed",
                    "error": str(exc),
                }
            )

    return {
        "built_count": built,
        "failed_count": failed,
        "results": results,
    }


def retry_failed_posting(
    *,
    posting_id: str,
    reviewer: str = "OpenClaw",
    note: str = "retry requested by orchestrator",
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    from src.agents.tools.posting_builder import retry_posting_job

    payload = retry_posting_job(
        posting_id=posting_id,
        reviewer=reviewer,
        note=note,
        db_path=db_path,
    )
    return payload.to_dict()


def ingest_documents() -> dict[str, Any]:
    return run_python_script("ingest_folder_to_store.py").to_dict()


def rebuild_with_learning() -> dict[str, Any]:
    return run_python_script("rebuild_document_store.py").to_dict()


def cleanup_bad_documents() -> dict[str, Any]:
    return run_python_script("cleanup_bad_documents.py").to_dict()


def classify_review_status() -> dict[str, Any]:
    return run_python_script("run_auto_review_classifier.py").to_dict()


def run_posting_queue(*, target_system: str | None = "qbo", stop_on_error: bool = False) -> dict[str, Any]:
    args: list[str] = []
    if target_system:
        args.extend(["--target-system", target_system])
    if stop_on_error:
        args.append("--stop-on-error")
    return run_python_script("run_posting_queue.py", args=args).to_dict()


def save_pipeline_run(run_result: PipelineRunResult) -> Path:
    filename = f"ledgerlink_runner_{run_result.run_at.replace(':', '-').replace('+', '_')}.json"
    path = EXPORTS_DIR / filename
    path.write_text(json.dumps(run_result.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def run_full_cycle(
    *,
    include_posting_queue: bool = False,
    stop_on_error: bool = True,
    posting_target_system: str = "qbo",
) -> dict[str, Any]:
    stages: list[StageResult] = []

    pipeline = [
        ("ingest_documents", lambda: run_python_script("ingest_folder_to_store.py")),
        ("rebuild_with_learning", lambda: run_python_script("rebuild_document_store.py")),
        ("cleanup_bad_documents", lambda: run_python_script("cleanup_bad_documents.py")),
        ("classify_review_status", lambda: run_python_script("run_auto_review_classifier.py")),
    ]

    if include_posting_queue:
        pipeline.append(
            (
                "run_posting_queue",
                lambda: run_python_script(
                    "run_posting_queue.py",
                    args=["--target-system", posting_target_system],
                ),
            )
        )

    overall_success = True

    for stage_name, runner in pipeline:
        stage_result = runner()
        stages.append(stage_result)

        if not stage_result.success:
            overall_success = False
            if stop_on_error:
                break

    final_summary = {
        "review_queue": get_review_queue_summary(),
        "posting_queue": get_posting_queue_summary(),
        "stage_count": len(stages),
        "include_posting_queue": include_posting_queue,
    }

    run_result = PipelineRunResult(
        run_at=utc_now_iso(),
        success=overall_success,
        stages=[stage.to_dict() for stage in stages],
        summary=final_summary,
    )

    export_path = save_pipeline_run(run_result)
    payload = run_result.to_dict()
    payload["saved_to"] = str(export_path)
    return payload


def print_full_cycle_summary(run_result: dict[str, Any]) -> None:
    print()
    print("LEDGERLINK RUNNER SUMMARY")
    print("-" * 100)
    print(f"Run at      : {run_result.get('run_at')}")
    print(f"Success     : {run_result.get('success')}")
    print(f"Saved to    : {run_result.get('saved_to', '')}")
    print()

    stages = run_result.get("stages", []) or []
    for stage in stages:
        print(
            f"{stage.get('stage_name')} -> "
            f"success={stage.get('success')} "
            f"return_code={stage.get('return_code')}"
        )
        summary = stage.get("summary", {}) or {}
        if summary:
            print(f"  summary={json.dumps(summary, ensure_ascii=False)}")

    review_queue = (run_result.get("summary", {}) or {}).get("review_queue", {})
    posting_queue = (run_result.get("summary", {}) or {}).get("posting_queue", {})

    print()
    print("FINAL REVIEW QUEUE")
    print(json.dumps(review_queue, indent=2, ensure_ascii=False))

    print()
    print("FINAL POSTING QUEUE")
    print(json.dumps(posting_queue, indent=2, ensure_ascii=False))


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="LedgerLink workflow runner for OpenClaw orchestration")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("ingest")
    subparsers.add_parser("rebuild")
    subparsers.add_parser("cleanup")
    subparsers.add_parser("classify")

    run_queue_parser = subparsers.add_parser("post-queue")
    run_queue_parser.add_argument("--target-system", default="qbo")
    run_queue_parser.add_argument("--stop-on-error", action="store_true")

    full_cycle_parser = subparsers.add_parser("run-full-cycle")
    full_cycle_parser.add_argument("--include-posting-queue", action="store_true")
    full_cycle_parser.add_argument("--no-stop-on-error", action="store_true")
    full_cycle_parser.add_argument("--posting-target-system", default="qbo")

    review_summary_parser = subparsers.add_parser("review-summary")
    review_summary_parser.add_argument("--status", default="")
    review_summary_parser.add_argument("--limit", type=int, default=50)

    doc_parser = subparsers.add_parser("document")
    doc_parser.add_argument("--document-id", required=True)

    fix_parser = subparsers.add_parser("apply-fix")
    fix_parser.add_argument("--document-id", required=True)
    fix_parser.add_argument("--vendor", default="")
    fix_parser.add_argument("--client-code", default="")
    fix_parser.add_argument("--doc-type", default="")
    fix_parser.add_argument("--amount", default="")
    fix_parser.add_argument("--document-date", default="")
    fix_parser.add_argument("--gl-account", default="")
    fix_parser.add_argument("--tax-code", default="")
    fix_parser.add_argument("--category", default="")
    fix_parser.add_argument("--review-status", default="")

    build_jobs_parser = subparsers.add_parser("build-posting-jobs")
    build_jobs_parser.add_argument("--target-system", default="qbo")
    build_jobs_parser.add_argument("--entry-kind", default="expense")

    retry_parser = subparsers.add_parser("retry-posting")
    retry_parser.add_argument("--posting-id", required=True)
    retry_parser.add_argument("--reviewer", default="OpenClaw")
    retry_parser.add_argument("--note", default="retry requested by orchestrator")

    args = parser.parse_args()

    if args.command == "ingest":
        result = ingest_documents()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("success") else 1

    if args.command == "rebuild":
        result = rebuild_with_learning()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("success") else 1

    if args.command == "cleanup":
        result = cleanup_bad_documents()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("success") else 1

    if args.command == "classify":
        result = classify_review_status()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("success") else 1

    if args.command == "post-queue":
        result = run_posting_queue(
            target_system=args.target_system or None,
            stop_on_error=args.stop_on_error,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("success") else 1

    if args.command == "run-full-cycle":
        result = run_full_cycle(
            include_posting_queue=args.include_posting_queue,
            stop_on_error=not args.no_stop_on_error,
            posting_target_system=args.posting_target_system,
        )
        print_full_cycle_summary(result)
        return 0 if result.get("success") else 1

    if args.command == "review-summary":
        result = {
            "review_queue_summary": get_review_queue_summary(),
            "documents": list_documents_for_review(
                review_status=args.status or None,
                limit=args.limit,
            ),
        }
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "document":
        result = get_document_details(args.document_id)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "apply-fix":
        fields = {
            "vendor": args.vendor,
            "client_code": args.client_code,
            "doc_type": args.doc_type,
            "amount": args.amount,
            "document_date": args.document_date,
            "gl_account": args.gl_account,
            "tax_code": args.tax_code,
            "category": args.category,
            "review_status": args.review_status,
        }
        filtered_fields = {k: v for k, v in fields.items() if normalize_text(v)}
        result = apply_document_fix(
            document_id=args.document_id,
            fields=filtered_fields,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.command == "build-posting-jobs":
        result = build_posting_jobs_for_ready_documents(
            target_system=args.target_system,
            entry_kind=args.entry_kind,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("failed_count", 0) == 0 else 1

    if args.command == "retry-posting":
        result = retry_failed_posting(
            posting_id=args.posting_id,
            reviewer=args.reviewer,
            note=args.note,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())