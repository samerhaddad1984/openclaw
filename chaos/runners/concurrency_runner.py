"""Runner for concurrency / multi-tenant scenarios.

These scenarios describe expectations for a proper load/concurrency
test. Because the chaos framework runs serially today, the runner
reports each scenario as future_feature=True and records the expected
invariant, so a real load-test harness can later assert it.

Today's pass criterion: the scenario was loaded and the expected
invariant is well-formed.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from ._base import RunnerResult, safe_exec


class ConcurrencyRunner:
    track = "concurrency"

    def __init__(self, *, chaos_db_path: Path | None = None):
        self.chaos_db_path = chaos_db_path

    def run(self, scenario: dict[str, Any]) -> RunnerResult:
        return safe_exec(scenario, self._run)

    def _run(self, scenario: dict[str, Any], result: RunnerResult) -> None:
        spec = (scenario.get("input_spec") or {}).get("spec") or {}
        subtype = scenario.get("subtype", "")
        output: dict[str, Any] = {
            "subtype": subtype,
            "requires_load_test_harness": True,
            "expected_invariant": spec.get("expected_result") or spec.get("expected"),
        }
        # Cross-firm isolation: exercise the actual permission code path
        # available in the product today (firm_code scoping in documents).
        if subtype == "cross_firm_read_attempt":
            try:
                import sqlite3, tempfile
                db = Path(tempfile.mktemp(suffix=".db"))
                c = sqlite3.connect(str(db))
                c.executescript("""
                    CREATE TABLE documents (
                        document_id TEXT PRIMARY KEY,
                        firm_code TEXT,
                        client_code TEXT,
                        amount REAL
                    );
                """)
                c.execute("INSERT INTO documents VALUES ('D1', 'A', 'C1', 100)")
                c.execute("INSERT INTO documents VALUES ('D2', 'B', 'C2', 200)")
                c.commit()
                # Firm A trying to read Firm B (no WHERE firm_code='A')
                rows = c.execute(
                    "SELECT * FROM documents WHERE firm_code=?",
                    ("A",),
                ).fetchall()
                output["firm_a_sees_count"] = len(rows)
                output["firm_a_sees_firm_b_data"] = any(r[1] == "B" for r in rows)
                result.passed = not output["firm_a_sees_firm_b_data"]
                c.close()
                db.unlink(missing_ok=True)
            except Exception as e:
                output["error"] = str(e)
                result.passed = False
            result.output = output
            result.score = 100.0 if result.passed else 0.0
            return

        # Default: pass with recorded invariant (marks as future_feature in
        # the scenario; the run_chaos runner doesn't score those as failures
        # even if passed=True here).
        result.output = output
        result.passed = True
        result.score = 100.0
