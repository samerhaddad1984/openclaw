"""Runner for receipt scenarios — invokes the real OCR pipeline."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..oracles import get_oracle
from ._base import RunnerResult, safe_exec

log = logging.getLogger(__name__)


class ReceiptRunner:
    track = "receipts"

    def __init__(self, *, image_generator, chaos_db_path: Path):
        self.image_generator = image_generator
        self.chaos_db_path = chaos_db_path

    def run(self, scenario: dict[str, Any]) -> RunnerResult:
        return safe_exec(scenario, self._run)

    def _run(self, scenario: dict[str, Any], result: RunnerResult) -> None:
        # 1. Generate or fetch the image
        image_path, ground_truth = self.image_generator.generate(scenario)

        # 2. Read bytes and run through the OCR pipeline
        try:
            from src.engines.ocr_engine import process_file  # type: ignore
        except Exception as e:
            result.output = {"skipped": True, "reason": f"ocr_engine import failed: {e}"}
            result.passed = False
            return

        file_bytes = image_path.read_bytes() if image_path.exists() else b""
        if not file_bytes:
            result.output = {"skipped": True, "reason": "empty_image"}
            result.passed = False
            return

        try:
            processed = process_file(
                file_bytes=file_bytes,
                filename=image_path.name,
                client_code="CHAOS",
                ingest_source="chaos",
                db_path=self.chaos_db_path,
                upload_dir=self.chaos_db_path.parent / "uploads",
            ) or {}
        except Exception as e:
            processed = {"ok": False, "error": f"{type(e).__name__}: {e}"}

        # Normalize extracted fields for the oracle
        extracted = {
            "vendor":        processed.get("vendor"),
            "document_date": processed.get("document_date"),
            "total":         processed.get("amount"),
            "gst":           processed.get("gst"),
            "qst":           processed.get("qst"),
            "currency":      processed.get("currency"),
            "tax_code":      processed.get("tax_code"),
            "line_count":    processed.get("line_count") or len(processed.get("line_items") or []),
        }

        oracle = get_oracle("receipt")
        oracle_result = oracle.validate(extracted, ground_truth)

        result.output = {"raw": processed, "extracted": extracted, "image": str(image_path)}
        result.oracle_result = oracle_result.to_dict()
        result.score = oracle_result.total_score
        result.passed = oracle_result.passed
