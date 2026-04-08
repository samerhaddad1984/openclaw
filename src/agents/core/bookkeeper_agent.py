from __future__ import annotations

from pathlib import Path
from typing import Optional

from src.agents.core.task_models import Task, DocumentRecord
from src.agents.core.task_store import TaskStore
from src.agents.core.tool_registry import ToolRegistry


class BookkeeperAgent:
    def __init__(self, project_root: Path | str, db_path: Path | str):
        self.project_root = Path(project_root).resolve()
        self.store = TaskStore(db_path)
        self.tools = ToolRegistry(self.project_root)

    def enqueue_document(self, file_path: str, priority: int = 100) -> Task:
        task = Task.create(
            task_type="process_document",
            priority=priority,
            file_path=file_path,
            payload={},
        )
        self.store.add_task(task)
        return task

    def run_once(self) -> Optional[Task]:
        task = self.store.get_next_pending_task()
        if not task:
            return None

        task.status = "running"
        self.store.update_task(task)

        try:
            if task.task_type == "process_document":
                self._handle_process_document(task)
            elif task.task_type == "review_document":
                self._handle_review_document(task)
            else:
                raise ValueError(f"Unsupported task type: {task.task_type}")

            task.status = "done"
            task.error = None
            self.store.update_task(task)
            return task

        except Exception as exc:
            task.status = "failed"
            task.error = str(exc)
            self.store.update_task(task)
            return task

    def run_until_empty(self, max_tasks: int = 100) -> list[Task]:
        processed: list[Task] = []

        for _ in range(max_tasks):
            task = self.run_once()
            if task is None:
                break
            processed.append(task)

        return processed

    def _handle_process_document(self, task: Task) -> None:
        if not task.file_path:
            raise ValueError("process_document task is missing file_path")

        result = self.tools.process_document(task.file_path)
        task.result = result

        document = DocumentRecord.from_pipeline_result(result)
        self.store.add_document(document)

        review_status = (result.get("review_status") or "").strip()
        if review_status in {"Ready", "NeedsReview", "Exception", "Ignored"}:
            review_task = Task.create(
                task_type="review_document",
                priority=200,
                client_code=result.get("client_code"),
                file_path=result.get("file_path"),
                payload={
                    "document_id": document.document_id,
                    "review_status": review_status,
                    "vendor": result.get("vendor"),
                    "amount": result.get("amount"),
                    "gl_account": result.get("gl_account"),
                },
            )
            self.store.add_task(review_task)

    def _handle_review_document(self, task: Task) -> None:
        # For now this is a placeholder workflow step.
        # Later this becomes:
        # - accountant approval
        # - auto-approve policy
        # - export decision
        task.result = {
            "message": "Review task created by agent",
            "payload": task.payload,
        }