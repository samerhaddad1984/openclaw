"""R2-Investigation 2 — pytest wrapper around the OCR batch stress run.

Asserts the deterministic OCR surface survives the full real-receipt
corpus (~800 files: SROIE jpegs + CORD pngs + bundled Canadian PDFs)
with zero crashes and no confident-wrong on empty text.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.stress.ocr_batch_stress import run  # noqa: E402


@pytest.mark.slow
def test_ocr_batch_stress_no_crashes_at_full_corpus():
    """800+ real receipts. Deterministic stages must not crash and must
    not confidently emit garbage."""
    summary = run()
    assert summary["total_files"] >= 100, summary
    assert summary["fmt_errors"] == 0, summary["fmt_error_samples"]
    assert summary["pdf_errors"] == 0, summary["pdf_error_samples"]
    assert summary["parse_errors"] == 0, summary["parse_error_samples"]
    assert summary["confident_wrong_on_empty_text"] == 0, (
        summary["confident_wrong_samples"]
    )
    assert summary["huge_amount_high_confidence"] == 0, (
        summary["huge_amount_samples"]
    )
    # PDF parsing should average under a second per file. A regression
    # that tanks pdfplumber speed is loud here.
    assert summary["pdf_avg_ms"] < 1500, summary
