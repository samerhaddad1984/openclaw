"""Top-level simulation orchestrator. Run with:

    python3 -m tests.simulation.run_simulation
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from tests.simulation.scenario_generator import generate_all, expected_totals
from tests.simulation.workflow_executor import run_all
from tests.simulation.bug_hunter import run_probes


def main() -> int:
    profiles = generate_all()
    print(f"Seeded {len(profiles)} clients.")
    for p in profiles:
        print("  ", expected_totals(p))
    workflow_summary = run_all(profiles)
    probe_results = run_probes()

    # Merge probe results into the workflow bug list.
    probe_bugs = [b.__dict__ for r in probe_results for b in r.bugs]
    workflow_summary["probes"] = [
        {"phase": r.phase, "status": r.status,
         "metric": r.metric, "bugs": [b.__dict__ for b in r.bugs]}
        for r in probe_results
    ]
    workflow_summary["probe_bugs"] = probe_bugs
    workflow_summary["bug_count"] = (
        workflow_summary.get("bug_count", 0) + len(probe_bugs)
    )

    Path("/tmp/cpa_simulation_summary.json").write_text(
        json.dumps(workflow_summary, default=str, indent=2),
    )
    print(json.dumps({
        "pass": workflow_summary["pass"],
        "warn": workflow_summary["warn"],
        "fail": workflow_summary["fail"],
        "bug_count": workflow_summary["bug_count"],
    }, indent=2))
    return 0 if workflow_summary["fail"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
