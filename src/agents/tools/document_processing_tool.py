from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


class DocumentProcessingTool:
    def __init__(self, project_root: Path | str):
        self.project_root = Path(project_root).resolve()
        self.script_path = self.project_root / "src" / "agents" / "tools" / "local_document_processor.py"

    def process_document(self, file_path: str) -> dict:
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            raise FileNotFoundError(f"Document file not found: {file_path_obj}")

        if not self.script_path.exists():
            raise FileNotFoundError(f"Processor script not found: {self.script_path}")

        cmd = [
            sys.executable,
            str(self.script_path),
            str(file_path_obj),
        ]

        proc = subprocess.run(
            cmd,
            cwd=str(self.project_root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        stdout = proc.stdout.strip()
        stderr = proc.stderr.strip()

        if proc.returncode != 0:
            raise RuntimeError(
                "Document processor failed.\n"
                f"Return code: {proc.returncode}\n"
                f"STDOUT:\n{stdout}\n\nSTDERR:\n{stderr}"
            )

        start = stdout.find("{")
        end = stdout.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise RuntimeError(
                "Could not find JSON in processor output.\n"
                f"STDOUT:\n{stdout}\n\nSTDERR:\n{stderr}"
            )

        json_text = stdout[start : end + 1]

        try:
            return json.loads(json_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "Processor output was not valid JSON.\n"
                f"JSON snippet:\n{json_text}\n\nSTDERR:\n{stderr}"
            ) from exc