from __future__ import annotations

import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.bookkeeper_agent import BookkeeperAgent  # noqa: E402


def main() -> int:
    db_path = ROOT_DIR / "data" / "otocpa_agent.db"
    agent = BookkeeperAgent(project_root=ROOT_DIR, db_path=db_path)

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python scripts/run_bookkeeper_agent.py add <file_path>")
        print("  python scripts/run_bookkeeper_agent.py run")
        print("  python scripts/run_bookkeeper_agent.py run-all")
        return 1

    command = sys.argv[1].strip().lower()

    if command == "add":
        if len(sys.argv) < 3:
            print("Missing file path.")
            return 1

        file_path = sys.argv[2]
        task = agent.enqueue_document(file_path)
        print(f"Queued task: {task.task_id}")
        print(f"File: {file_path}")
        return 0

    if command == "run":
        task = agent.run_once()
        if task is None:
            print("No pending tasks.")
            return 0

        print(f"Processed task: {task.task_id}")
        print(f"Type: {task.task_type}")
        print(f"Status: {task.status}")
        print(f"Error: {task.error}")
        print(f"Result: {task.result}")
        return 0

    if command == "run-all":
        tasks = agent.run_until_empty()
        print(f"Processed tasks: {len(tasks)}")
        for task in tasks:
            print(f"- {task.task_id} | {task.task_type} | {task.status}")
        return 0

    print(f"Unknown command: {command}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())