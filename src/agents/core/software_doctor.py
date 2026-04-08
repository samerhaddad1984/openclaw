from __future__ import annotations

import json
import os
import sqlite3
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"
QBO_CONFIG_PATH = ROOT_DIR / "data" / "qbo_config.json"
QBO_MAPPINGS_PATH = ROOT_DIR / "src" / "agents" / "data" / "rules" / "qbo_mappings.json"


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


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()
    return row is not None


@dataclass
class DoctorCheck:
    name: str
    ok: bool
    severity: str
    summary: str
    details: dict[str, Any]
    suggested_fix: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SoftwareDoctor:
    def __init__(
        self,
        *,
        db_path: Path = DB_PATH,
        qbo_config_path: Path = QBO_CONFIG_PATH,
        qbo_mappings_path: Path = QBO_MAPPINGS_PATH,
    ) -> None:
        self.db_path = db_path
        self.qbo_config_path = qbo_config_path
        self.qbo_mappings_path = qbo_mappings_path

    def _check_database(self) -> DoctorCheck:
        if not self.db_path.exists():
            return DoctorCheck(
                name="database_file",
                ok=False,
                severity="critical",
                summary="OtoCPA database file is missing.",
                details={"db_path": str(self.db_path)},
                suggested_fix="Recreate the environment or restore data/otocpa_agent.db.",
            )

        try:
            with open_db(self.db_path) as conn:
                required_tables = [
                    "documents",
                    "posting_jobs",
                    "orchestrator_runs",
                ]
                existing = {}
                missing = []

                for table_name in required_tables:
                    exists = table_exists(conn, table_name)
                    existing[table_name] = exists
                    if not exists:
                        missing.append(table_name)

            return DoctorCheck(
                name="database_file",
                ok=len(missing) == 0,
                severity="critical" if missing else "info",
                summary="Database file and core tables checked.",
                details={
                    "db_path": str(self.db_path),
                    "required_tables": existing,
                    "missing_tables": missing,
                },
                suggested_fix=(
                    "Run rebuild/setup scripts to recreate missing tables."
                    if missing
                    else "No action needed."
                ),
            )
        except Exception as exc:
            return DoctorCheck(
                name="database_file",
                ok=False,
                severity="critical",
                summary="Database exists but could not be opened.",
                details={"db_path": str(self.db_path), "error": str(exc)},
                suggested_fix="Check file permissions and whether another process locked the SQLite database.",
            )

    def _check_qbo_config(self) -> DoctorCheck:
        config_exists = self.qbo_config_path.exists()
        config_json = {}
        if config_exists:
            try:
                config_json = safe_json_loads(self.qbo_config_path.read_text(encoding="utf-8"))
            except Exception:
                config_json = {}

        access_token = normalize_text(os.environ.get("QBO_ACCESS_TOKEN")) or normalize_text(config_json.get("access_token"))
        realm_id = normalize_text(os.environ.get("QBO_REALM_ID")) or normalize_text(config_json.get("realm_id"))
        environment = normalize_text(os.environ.get("QBO_ENVIRONMENT")) or normalize_text(config_json.get("environment")) or "production"
        auto_create_vendors = bool(config_json.get("auto_create_vendors", False))

        ok = bool(access_token and realm_id)

        return DoctorCheck(
            name="qbo_config",
            ok=ok,
            severity="critical" if not ok else "info",
            summary="QuickBooks Online configuration checked.",
            details={
                "config_path": str(self.qbo_config_path),
                "config_exists": config_exists,
                "access_token_present": bool(access_token),
                "realm_id_present": bool(realm_id),
                "environment": environment,
                "auto_create_vendors": auto_create_vendors,
            },
            suggested_fix=(
                "Set QBO_ACCESS_TOKEN and QBO_REALM_ID in the environment, or add them to data/qbo_config.json."
                if not ok
                else "No action needed."
            ),
        )

    def _check_qbo_mappings(self) -> DoctorCheck:
        if not self.qbo_mappings_path.exists():
            return DoctorCheck(
                name="qbo_mappings",
                ok=False,
                severity="high",
                summary="QBO mappings file is missing.",
                details={"mappings_path": str(self.qbo_mappings_path)},
                suggested_fix="Create src/agents/data/rules/qbo_mappings.json.",
            )

        mappings = safe_json_loads(self.qbo_mappings_path.read_text(encoding="utf-8"))
        accounts = mappings.get("accounts", {}) if isinstance(mappings, dict) else {}
        vendors = mappings.get("vendors", {}) if isinstance(mappings, dict) else {}
        payment = mappings.get("payment", {}) if isinstance(mappings, dict) else {}

        important_account_keys = [
            "Software Expense",
            "Utilities - Electricity",
            "Credit Card Payable",
        ]
        missing_account_keys = [key for key in important_account_keys if key not in accounts]

        default_payment_account = normalize_text(payment.get("default_account_name"))

        ok = len(missing_account_keys) == 0 and bool(default_payment_account)

        return DoctorCheck(
            name="qbo_mappings",
            ok=ok,
            severity="high" if not ok else "info",
            summary="QBO mappings checked.",
            details={
                "mappings_path": str(self.qbo_mappings_path),
                "vendor_count": len(vendors) if isinstance(vendors, dict) else 0,
                "account_count": len(accounts) if isinstance(accounts, dict) else 0,
                "missing_account_keys": missing_account_keys,
                "default_payment_account_name": default_payment_account,
            },
            suggested_fix=(
                "Add missing account mappings and set payment.default_account_name."
                if not ok
                else "No action needed."
            ),
        )

    def _check_documents_and_queue(self) -> DoctorCheck:
        try:
            with open_db(self.db_path) as conn:
                documents_count = conn.execute("SELECT COUNT(*) AS c FROM documents").fetchone()["c"] if table_exists(conn, "documents") else 0
                ready_count = conn.execute("SELECT COUNT(*) AS c FROM documents WHERE review_status = 'Ready'").fetchone()["c"] if table_exists(conn, "documents") else 0
                needs_review_count = conn.execute("SELECT COUNT(*) AS c FROM documents WHERE review_status = 'NeedsReview'").fetchone()["c"] if table_exists(conn, "documents") else 0

                if table_exists(conn, "posting_jobs"):
                    posting_counts = {
                        "draft": conn.execute("SELECT COUNT(*) AS c FROM posting_jobs WHERE posting_status = 'draft'").fetchone()["c"],
                        "ready_to_post": conn.execute("SELECT COUNT(*) AS c FROM posting_jobs WHERE posting_status = 'ready_to_post'").fetchone()["c"],
                        "posted": conn.execute("SELECT COUNT(*) AS c FROM posting_jobs WHERE posting_status = 'posted'").fetchone()["c"],
                        "post_failed": conn.execute("SELECT COUNT(*) AS c FROM posting_jobs WHERE posting_status = 'post_failed'").fetchone()["c"],
                    }
                else:
                    posting_counts = {}

            ok = documents_count > 0

            return DoctorCheck(
                name="documents_and_queue",
                ok=ok,
                severity="medium" if not ok else "info",
                summary="Documents and queue state checked.",
                details={
                    "documents_count": documents_count,
                    "ready_count": ready_count,
                    "needs_review_count": needs_review_count,
                    "posting_counts": posting_counts,
                },
                suggested_fix=(
                    "Ingest or rebuild documents; the system has no documents to process."
                    if not ok
                    else "No action needed."
                ),
            )
        except Exception as exc:
            return DoctorCheck(
                name="documents_and_queue",
                ok=False,
                severity="high",
                summary="Could not inspect documents or queue state.",
                details={"error": str(exc)},
                suggested_fix="Check database integrity and rebuild document store.",
            )

    def _check_failed_posts(self) -> DoctorCheck:
        try:
            with open_db(self.db_path) as conn:
                if not table_exists(conn, "posting_jobs"):
                    return DoctorCheck(
                        name="failed_posts",
                        ok=True,
                        severity="info",
                        summary="posting_jobs table does not exist yet.",
                        details={},
                        suggested_fix="No action needed.",
                    )

                rows = conn.execute(
                    """
                    SELECT posting_id, document_id, error_text, payload_json
                    FROM posting_jobs
                    WHERE posting_status = 'post_failed'
                    ORDER BY updated_at DESC, created_at DESC
                    LIMIT 10
                    """
                ).fetchall()

            failed_items = []
            for row in rows:
                payload = safe_json_loads(row["payload_json"])
                failed_items.append(
                    {
                        "posting_id": normalize_text(row["posting_id"]),
                        "document_id": normalize_text(row["document_id"]),
                        "error_text": normalize_text(row["error_text"]),
                        "vendor": normalize_text(payload.get("vendor")),
                        "gl_account": normalize_text(payload.get("gl_account")),
                    }
                )

            ok = len(failed_items) == 0

            return DoctorCheck(
                name="failed_posts",
                ok=ok,
                severity="high" if not ok else "info",
                summary="Checked failed posting jobs.",
                details={
                    "failed_count_sample": len(failed_items),
                    "items": failed_items,
                },
                suggested_fix=(
                    "Inspect error_text values, fix mappings/auth/vendors, then retry failed jobs."
                    if not ok
                    else "No action needed."
                ),
            )
        except Exception as exc:
            return DoctorCheck(
                name="failed_posts",
                ok=False,
                severity="medium",
                summary="Could not inspect failed posting jobs.",
                details={"error": str(exc)},
                suggested_fix="Check posting_jobs table and database health.",
            )

    def run(self) -> dict[str, Any]:
        checks = [
            self._check_database(),
            self._check_qbo_config(),
            self._check_qbo_mappings(),
            self._check_documents_and_queue(),
            self._check_failed_posts(),
        ]

        critical = sum(1 for c in checks if not c.ok and c.severity == "critical")
        high = sum(1 for c in checks if not c.ok and c.severity == "high")
        medium = sum(1 for c in checks if not c.ok and c.severity == "medium")

        overall_ok = critical == 0 and high == 0

        return {
            "overall_ok": overall_ok,
            "summary": {
                "critical_failures": critical,
                "high_failures": high,
                "medium_failures": medium,
                "checks_run": len(checks),
            },
            "checks": [c.to_dict() for c in checks],
        }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="OtoCPA software doctor")
    parser.add_argument("--json-only", action="store_true")
    args = parser.parse_args()

    result = SoftwareDoctor().run()

    if args.json_only:
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result["overall_ok"] else 1

    print("OTOCPA SOFTWARE DOCTOR")
    print("=" * 100)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["overall_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
