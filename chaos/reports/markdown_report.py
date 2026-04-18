"""Render Markdown report from a run's results — for GitHub / Slack paste."""
from __future__ import annotations

from pathlib import Path
from typing import Any


def write_markdown_report(
    *,
    results: list[dict[str, Any]],
    summary: dict[str, Any],
    out_path: Path,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append(f"# Chaos Run {summary.get('run_id','')}")
    lines.append("")
    b = summary.get("budget", {}) or {}
    lines.append(f"- Total: **{summary.get('total',0)}** "
                 f"· Pass: **{summary.get('passed',0)}** "
                 f"· Fail: **{summary.get('failed',0)}** "
                 f"· Error: **{summary.get('errored',0)}** "
                 f"· Pass rate: **{summary.get('pass_rate',0)*100:.1f}%**")
    lines.append(f"- Duration: {summary.get('duration_s',0):.1f}s  "
                 f"Budget: ${b.get('used_usd',0):.2f}/${b.get('budget_usd',0):.2f}  "
                 f"Images: {b.get('generated_count',0)} (cache hits {b.get('cache_hits',0)}, "
                 f"placeholders {b.get('fallback_count',0)})")
    lines.append("")

    lines.append("## By track")
    lines.append("")
    lines.append("| Track | Total | Pass | Fail | Error | Pass % |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for track, d in sorted((summary.get("by_track") or {}).items()):
        total = d["total"] or 1
        lines.append(f"| {track} | {d['total']} | {d['passed']} | {d['failed']} | "
                     f"{d['errored']} | {d['passed']/total*100:.1f}% |")
    lines.append("")

    lines.append("## By difficulty")
    lines.append("")
    lines.append("| Difficulty | Total | Pass | Fail | Error | Pass % |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    order = ["easy", "normal", "hard", "nightmare", "impossible"]
    by_diff = summary.get("by_difficulty") or {}
    for diff in order:
        d = by_diff.get(diff)
        if not d:
            continue
        total = d["total"] or 1
        lines.append(f"| {diff} | {d['total']} | {d['passed']} | {d['failed']} | "
                     f"{d['errored']} | {d['passed']/total*100:.1f}% |")
    lines.append("")

    failures = [r for r in results if not r.get("passed")]
    if failures:
        lines.append(f"## Top failures ({len(failures)} total, showing up to 20)")
        lines.append("")
        for r in failures[:20]:
            sid = r.get("scenario_id", "?")
            lines.append(f"- **{r.get('category')}/{r.get('subtype')}** "
                         f"(score {r.get('score',0):.1f}, diff {r.get('difficulty')}) — "
                         f"`python3 chaos/run_chaos.py --reproduce {sid}`")
            if r.get("error"):
                lines.append(f"  - error: `{r['error']}`")
    else:
        lines.append("## No failures 🎉")

    lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")
