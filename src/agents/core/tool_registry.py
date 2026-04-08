from __future__ import annotations

from pathlib import Path

from src.agents.tools.document_processing_tool import DocumentProcessingTool


class ToolRegistry:
    def __init__(self, project_root: Path | str):
        self.project_root = Path(project_root).resolve()
        self.document_tool = DocumentProcessingTool(self.project_root)

    def process_document(self, file_path: str) -> dict:
        return self.document_tool.process_document(file_path)