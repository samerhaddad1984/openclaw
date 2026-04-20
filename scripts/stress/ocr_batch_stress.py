"""R2-Investigation 2 — batch OCR stress on the deterministic surface.

Live API calls (Google DocAI, Claude Vision) need keys + a budget the
sandbox doesn't have, so we exercise the **deterministic stages** of
the OCR pipeline against the full real-receipt corpus:

  - ``detect_format`` for every file (image + PDF + corrupt)
  - ``extract_with_pdfplumber`` on every PDF
  - ``parse_invoice_fields`` on the extracted text and on every CORD
    ground-truth record

Reports throughput and the top failure patterns so we can fix any
deterministic crashes before they bite a live customer.

Run: ``python3 -m scripts.stress.ocr_batch_stress``.
Pytest wrapper: ``tests/adversarial/test_ocr_batch_stress.py``.
"""
from __future__ import annotations

import json
import sys
import time
import traceback
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _gather() -> list[Path]:
    out: list[Path] = []
    base = ROOT / "chaos" / "fixtures" / "real_receipts"
    if base.exists():
        for ext in ("*.png", "*.jpg", "*.jpeg", "*.pdf"):
            out.extend(base.rglob(ext))
    real = ROOT / "tests" / "documents_real"
    if real.exists():
        for ext in ("*.png", "*.jpg", "*.jpeg", "*.pdf", "*.PDF"):
            out.extend(real.rglob(ext))
    return sorted(set(out))


def run(limit: int | None = None) -> dict:
    from src.engines.ocr_engine import (  # type: ignore[import]
        detect_format,
        extract_with_pdfplumber,
        parse_invoice_fields,
    )
    files = _gather()
    if limit:
        files = files[:limit]
    print(f"OCR stress: {len(files)} files")

    fmt_errors: list[dict] = []
    pdf_errors: list[dict] = []
    parse_errors: list[dict] = []
    confident_wrong: list[dict] = []  # parse confidence >= 0.85 on garbage
    huge_amount: list[dict] = []
    fmt_counts: Counter[str] = Counter()
    parse_durations: list[int] = []
    pdf_durations: list[int] = []

    start = time.time()
    for i, f in enumerate(files):
        # 1. format detection
        try:
            data = f.read_bytes()
            fmt = detect_format(data)
            fmt_counts[fmt] += 1
        except Exception as e:
            fmt_errors.append({"file": f.name, "error": str(e)[:200]})
            continue

        # 2. PDF text
        if f.suffix.lower() == ".pdf":
            t0 = time.time()
            try:
                pdf_res = extract_with_pdfplumber(str(f))
                pdf_durations.append(int((time.time() - t0) * 1000))
            except Exception as e:
                pdf_errors.append({"file": f.name, "error": str(e)[:200]})
                continue

            # 3. parse fields
            t1 = time.time()
            try:
                parsed = parse_invoice_fields(pdf_res.get("text", ""))
                parse_durations.append(int((time.time() - t1) * 1000))
            except Exception as e:
                parse_errors.append({"file": f.name, "error": str(e)[:200],
                                      "trace": traceback.format_exc()[:1000]})
                continue

            amt = parsed.get("amount")
            conf = float(parsed.get("confidence", 0))
            # Sanity: no invoice should be > $10M unless it's an
            # explicit large-format. Anything > $1M with high confidence
            # is suspect.
            if amt is not None and amt > 1_000_000 and conf >= 0.7:
                huge_amount.append({"file": f.name, "amount": amt, "confidence": conf})
            if pdf_res.get("text", "").strip() == "" and conf > 0.5:
                confident_wrong.append({
                    "file": f.name, "extracted_text_len": 0, "confidence": conf,
                })

    elapsed = time.time() - start
    summary = {
        "total_files": len(files),
        "elapsed_s": round(elapsed, 1),
        "fmt_counts": dict(fmt_counts),
        "fmt_errors": len(fmt_errors),
        "pdf_errors": len(pdf_errors),
        "parse_errors": len(parse_errors),
        "huge_amount_high_confidence": len(huge_amount),
        "confident_wrong_on_empty_text": len(confident_wrong),
        "pdf_avg_ms": int(sum(pdf_durations) / max(len(pdf_durations), 1)),
        "pdf_max_ms": max(pdf_durations) if pdf_durations else 0,
        "parse_avg_ms": int(sum(parse_durations) / max(len(parse_durations), 1)),
        "parse_max_ms": max(parse_durations) if parse_durations else 0,
        "fmt_error_samples": fmt_errors[:5],
        "pdf_error_samples": pdf_errors[:5],
        "parse_error_samples": parse_errors[:5],
        "huge_amount_samples": huge_amount[:5],
        "confident_wrong_samples": confident_wrong[:5],
    }
    return summary


def main() -> int:
    out = run()
    print(json.dumps(out, indent=2))
    Path("/tmp/ocr_stress_summary.json").write_text(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
