"""Generate a per-field + per-difficulty accuracy report for a real-dataset run."""
from __future__ import annotations

import json
import shutil
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def _load_results(run_dir: Path) -> list[dict[str, Any]]:
    results_file = run_dir / "results.jsonl"
    out: list[dict[str, Any]] = []
    if results_file.exists():
        for line in results_file.read_text().splitlines():
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _load_summary(run_dir: Path) -> dict[str, Any]:
    summary_file = run_dir / "summary.json"
    if summary_file.exists():
        return json.loads(summary_file.read_text())
    return {}


def _classify_failure(wrong: list[dict[str, Any]], omissions: list[str]) -> str:
    fields = {w.get("field") for w in wrong}
    fields.update(omissions)
    if "vendor" in fields:
        return "vendor_misidentified"
    if "document_date" in fields:
        return "date_format_error"
    if "total" in fields:
        return "amount_error_total"
    if "subtotal" in fields:
        return "amount_error_subtotal"
    if "line_count" in fields:
        return "line_item_missed"
    if "gst" in fields or "qst" in fields:
        return "tax_calculation_wrong"
    if "currency" in fields:
        return "currency_misread"
    if "tax_code" in fields:
        return "gl_assignment_wrong"
    return "other"


def report(run_dir: Path, out_md: Path, out_failures_dir: Path) -> None:
    results = _load_results(run_dir)
    summary = _load_summary(run_dir)
    if not results:
        raise RuntimeError(f"no results in {run_dir}")

    total = len(results)
    passed = sum(1 for r in results if r.get("passed"))
    errored = sum(1 for r in results if r.get("error"))

    # Per-field tally
    field_stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {"expected": 0, "correct": 0, "wrong": 0, "missing": 0, "skipped": 0}
    )
    per_difficulty: dict[str, dict[str, int]] = defaultdict(
        lambda: {"total": 0, "passed": 0, "score_sum": 0.0}
    )
    failure_patterns: Counter[str] = Counter()
    worst: list[tuple[float, dict[str, Any]]] = []

    for r in results:
        diff = r.get("difficulty", "unknown")
        per_difficulty[diff]["total"] += 1
        if r.get("passed"):
            per_difficulty[diff]["passed"] += 1
        per_difficulty[diff]["score_sum"] += float(r.get("score") or 0.0)

        oracle = r.get("oracle_result") or {}
        fs = oracle.get("field_scores") or {}
        wrong = oracle.get("wrong_values") or []
        omissions = oracle.get("omissions") or []

        wrong_fields = {w.get("field") for w in wrong}
        for field, score in fs.items():
            st = field_stats[field]
            if score is None:
                st["skipped"] += 1
                continue
            st["expected"] += 1
            if score >= 1.0:
                st["correct"] += 1
            elif field in wrong_fields:
                st["wrong"] += 1
            elif field in omissions:
                st["missing"] += 1
            else:
                st["wrong"] += 1

        if not r.get("passed"):
            failure_patterns[_classify_failure(wrong, omissions)] += 1
            worst.append((float(r.get("score") or 0.0), r))

    worst.sort(key=lambda x: x[0])
    worst = worst[:10]

    # ----- compose markdown ------------------------------------------------
    out_md.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Real Receipt OCR Performance — Baseline")
    lines.append("")
    lines.append(f"_(Run {summary.get('run_id', run_dir.name)}, CORD dataset, live OCR + AI pipeline.)_")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Receipts scored: **{total}**")
    lines.append(f"- Passed (≥75 score): **{passed}** ({passed / total * 100:.1f}%)")
    lines.append(f"- Errored: **{errored}**")
    avg_score = sum(float(r.get("score") or 0) for r in results) / total
    lines.append(f"- Average score: **{avg_score:.1f} / 100**")
    b = summary.get("budget", {}) or {}
    lines.append(f"- Image generation cost: ${b.get('used_usd', 0):.2f} "
                 f"(real dataset — images were not generated)")
    lines.append(f"- Duration: {summary.get('duration_s', 0):.1f}s")
    lines.append("")

    lines.append("## Per-field accuracy")
    lines.append("")
    lines.append("| Field | Scored | Correct | Wrong | Missing | Accuracy |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    order = ["vendor", "document_date", "total", "subtotal", "gst", "qst",
             "currency", "tax_code", "line_count"]
    for field in order:
        st = field_stats.get(field, {})
        n = st.get("expected", 0)
        c = st.get("correct", 0)
        w = st.get("wrong", 0)
        m = st.get("missing", 0)
        acc = (c / n * 100) if n else float("nan")
        acc_s = f"{acc:.1f}%" if n else "n/a"
        lines.append(f"| {field} | {n} | {c} | {w} | {m} | {acc_s} |")
    lines.append("")

    lines.append("## Per-difficulty breakdown")
    lines.append("")
    lines.append("| Difficulty | Count | Passed | Pass % | Avg score |")
    lines.append("|---|---:|---:|---:|---:|")
    for diff in ("easy", "normal", "hard", "nightmare"):
        d = per_difficulty.get(diff)
        if not d or d["total"] == 0:
            continue
        pct = d["passed"] / d["total"] * 100
        avg = d["score_sum"] / d["total"]
        lines.append(f"| {diff} | {d['total']} | {d['passed']} | {pct:.1f}% | {avg:.1f} |")
    lines.append("")

    lines.append("## Top failure patterns")
    lines.append("")
    for pattern, count in failure_patterns.most_common(10):
        lines.append(f"- **{pattern}** — {count}")
    lines.append("")

    lines.append("## Worst 10 scores")
    lines.append("")
    out_failures_dir.mkdir(parents=True, exist_ok=True)
    lines.append("| # | Receipt | Score | Wrong fields | Image |")
    lines.append("|---:|---|---:|---|---|")
    for idx, (score, r) in enumerate(worst, 1):
        sid = r.get("scenario_id")
        oracle = r.get("oracle_result") or {}
        wrong_fields = [w.get("field") for w in oracle.get("wrong_values", [])]
        omitted = oracle.get("omissions", [])
        tag = ", ".join(wrong_fields + [f"miss:{f}" for f in omitted]) or "—"
        # Copy the image for inspection (best-effort)
        img_src = (r.get("output") or {}).get("image_path")
        img_note = ""
        if img_src:
            img_src_path = Path(img_src)
            if img_src_path.exists():
                img_copy = out_failures_dir / f"{sid}_{img_src_path.name}"
                try:
                    shutil.copyfile(img_src_path, img_copy)
                    img_note = f"[{img_copy.name}]({img_copy.name})"
                except Exception:
                    img_note = "(copy failed)"
            # write per-receipt diff json
            (out_failures_dir / f"{sid}.json").write_text(json.dumps({
                "scenario_id": sid,
                "score": score,
                "expected": (r.get("output") or {}).get("ground_truth")
                            or r.get("ground_truth") or {},
                "extracted": (r.get("output") or {}).get("extracted"),
                "oracle":    oracle,
            }, indent=2))
        lines.append(f"| {idx} | `{sid}` | {score:.1f} | {tag} | {img_note} |")
    lines.append("")

    # Heuristic cost + claim comparison
    pass_rate = passed / total * 100 if total else 0
    claim = 98.0
    gap = claim - pass_rate
    lines.append("## Comparison to 98% accuracy claim")
    lines.append("")
    lines.append(f"- Claim: **{claim:.0f}%**")
    lines.append(f"- Observed: **{pass_rate:.1f}%**")
    lines.append(f"- Gap: **{gap:+.1f} pp**")
    lines.append("")
    lines.append("Note: observed pass rate is against CORD (Indonesian rupiah, no vendor/date GT) using Tesseract → AI fallback because DocAI is unavailable in this environment. Production with DocAI on Canadian receipts is expected to outperform this baseline.")
    lines.append("")

    out_md.write_text("\n".join(lines))
    print(f"wrote {out_md}")
    print(f"wrote {len(list(out_failures_dir.iterdir()))} worst-failure artifacts to {out_failures_dir}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("usage: real_dataset_report.py <run_dir> [out_md]")
        sys.exit(2)
    run_dir = Path(sys.argv[1])
    out_md = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("/tmp/chaos_stage4_report.md")
    out_fail = Path("/opt/otocpa/chaos/results/worst_failures") / run_dir.name
    report(run_dir, out_md, out_fail)
