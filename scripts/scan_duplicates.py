from __future__ import annotations

from pathlib import Path
import json
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.task_store import TaskStore  # noqa: E402
from src.agents.tools.duplicate_detector import find_duplicate_candidates_from_store  # noqa: E402


DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"
EXPORT_DIR = ROOT_DIR / "exports"


def main():
    EXPORT_DIR.mkdir(exist_ok=True)

    store = TaskStore(DB_PATH)
    candidates = find_duplicate_candidates_from_store(store, min_score=0.85)

    export_file = EXPORT_DIR / "duplicate_candidates.json"

    with open(export_file, "w", encoding="utf-8") as f:
        json.dump([c.__dict__ for c in candidates], f, indent=2, ensure_ascii=False)

    print("")
    print("DUPLICATE SCAN COMPLETED")
    print("")

    if not candidates:
        print("No duplicate candidates found.")
    else:
        print(f"Duplicate candidates found: {len(candidates)}")
        print("")
        print("TOP RESULTS")
        print("-" * 100)
        for c in candidates[:20]:
            print(
                f"{c.score:>5} | "
                f"{c.left_file_name}  <->  {c.right_file_name} | "
                f"{c.left_vendor} | {c.left_amount} | {c.left_date} | "
                f"reasons={','.join(c.reasons)}"
            )

    print("")
    print(f"Saved to: {export_file}")


if __name__ == "__main__":
    main()