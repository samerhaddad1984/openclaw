"""Render HTML dashboard from a run's results."""
from __future__ import annotations

import csv
import html
import json
from pathlib import Path
from typing import Any


SEVERITY_CLASS = {
    "critical": "sev-crit",
    "high":     "sev-high",
    "medium":   "sev-med",
    "low":      "sev-low",
}


CSS = """
body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif;
       margin: 0; padding: 24px; background: #f5f7fa; color: #1a202c; }
h1 { margin-top: 0; }
.card { background: #fff; border-radius: 8px; padding: 16px 20px; margin-bottom: 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; }
.metric { padding: 16px; background: #edf2f7; border-radius: 6px; }
.metric .v { font-size: 28px; font-weight: 600; }
.metric .k { font-size: 12px; color: #4a5568; text-transform: uppercase; letter-spacing: 0.05em; }
table { width: 100%; border-collapse: collapse; }
th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid #e2e8f0; }
th { background: #edf2f7; font-weight: 600; font-size: 13px; }
.pass { color: #22543d; }
.fail { color: #742a2a; font-weight: 600; }
.err  { color: #9b2c2c; font-weight: 600; }
.sev-crit { background: #fed7d7; color: #742a2a; padding: 2px 6px; border-radius: 3px; }
.sev-high { background: #feebc8; color: #7b341e; padding: 2px 6px; border-radius: 3px; }
.sev-med  { background: #fefcbf; color: #744210; padding: 2px 6px; border-radius: 3px; }
.sev-low  { background: #c6f6d5; color: #22543d; padding: 2px 6px; border-radius: 3px; }
pre { background: #1a202c; color: #e2e8f0; padding: 12px; border-radius: 6px; overflow: auto;
      font-size: 12px; }
.small { font-size: 12px; color: #4a5568; }
details summary { cursor: pointer; font-weight: 500; }
.repro { background: #2d3748; color: #e2e8f0; padding: 4px 8px; border-radius: 4px;
         font-family: monospace; font-size: 12px; }
"""


def _kv(k: str, v: Any) -> str:
    return f"<div class='metric'><div class='k'>{html.escape(k)}</div>" \
           f"<div class='v'>{html.escape(str(v))}</div></div>"


def _track_table(by_track: dict[str, dict[str, int]]) -> str:
    rows = []
    for track, d in sorted(by_track.items()):
        total = d["total"] or 1
        rate = d["passed"] / total * 100.0
        rows.append(
            f"<tr><td><b>{track}</b></td><td>{d['total']}</td>"
            f"<td class='pass'>{d['passed']}</td>"
            f"<td class='fail'>{d['failed']}</td>"
            f"<td class='err'>{d['errored']}</td>"
            f"<td>{rate:.1f}%</td></tr>"
        )
    return (
        "<table><thead><tr><th>Track</th><th>Total</th><th>Pass</th>"
        "<th>Fail</th><th>Error</th><th>Pass %</th></tr></thead><tbody>"
        + "".join(rows) + "</tbody></table>"
    )


def _difficulty_table(by_diff: dict[str, dict[str, int]]) -> str:
    order = ["easy", "normal", "hard", "nightmare", "impossible"]
    rows = []
    for diff in order:
        d = by_diff.get(diff)
        if not d:
            continue
        total = d["total"] or 1
        rate = d["passed"] / total * 100.0
        rows.append(
            f"<tr><td><b>{diff}</b></td><td>{d['total']}</td>"
            f"<td class='pass'>{d['passed']}</td>"
            f"<td class='fail'>{d['failed']}</td>"
            f"<td class='err'>{d['errored']}</td>"
            f"<td>{rate:.1f}%</td></tr>"
        )
    return (
        "<table><thead><tr><th>Difficulty</th><th>Total</th><th>Pass</th>"
        "<th>Fail</th><th>Error</th><th>Pass %</th></tr></thead><tbody>"
        + "".join(rows) + "</tbody></table>"
    )


def _failure_card(r: dict[str, Any]) -> str:
    sev = (r.get("output", {}) or {}).get("severity_on_failure") or "medium"
    cls = SEVERITY_CLASS.get(sev, "sev-med")
    sid = r.get("scenario_id", "?")
    cat = r.get("category", "?")
    st  = r.get("subtype", "?")
    desc = r.get("output", {}).get("subtype") if isinstance(r.get("output"), dict) else ""
    oracle = r.get("oracle_result", {})
    errors_html = ""
    if oracle.get("omissions"):
        errors_html += f"<div><b>Omissions:</b> {html.escape(str(oracle['omissions']))}</div>"
    if oracle.get("hallucinations"):
        errors_html += f"<div><b>Hallucinations:</b> {html.escape(str(oracle['hallucinations']))}</div>"
    if oracle.get("wrong_values"):
        errors_html += "<div><b>Wrong values:</b><pre>" + \
                       html.escape(json.dumps(oracle["wrong_values"], indent=2)) + "</pre></div>"
    if r.get("traceback"):
        errors_html += "<details><summary>Traceback</summary><pre>" + \
                       html.escape(r["traceback"]) + "</pre></details>"
    repro = f"python3 chaos/run_chaos.py --reproduce {html.escape(sid)}"
    return f"""
    <div class="card">
      <div><span class="{cls}">{sev.upper()}</span>
           <b>{html.escape(cat)}</b> / {html.escape(st)}
           <span class="small">— score {r.get('score', 0):.1f}</span></div>
      <div class="small">id: {html.escape(sid)}</div>
      {errors_html}
      <div class="small">Reproduce: <span class="repro">{repro}</span></div>
    </div>
    """


def write_html_report(
    *,
    results: list[dict[str, Any]],
    summary: dict[str, Any],
    out_path: Path,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # CSV of failures alongside
    failures = [r for r in results if not r.get("passed")]
    if failures:
        csv_path = out_path.with_suffix(".failures.csv")
        with csv_path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["scenario_id", "category", "subtype", "difficulty",
                        "score", "error", "elapsed_ms"])
            for r in failures:
                w.writerow([
                    r.get("scenario_id"),
                    r.get("category"),
                    r.get("subtype"),
                    r.get("difficulty"),
                    r.get("score"),
                    r.get("error") or "",
                    r.get("elapsed_ms"),
                ])

    budget = summary.get("budget", {}) or {}
    pass_rate = summary.get("pass_rate", 0.0) * 100.0

    body = f"""
    <h1>OtoCPA Chaos Report — {html.escape(summary.get('run_id',''))}</h1>
    <div class="card">
      <div class="grid">
        {_kv('Total', summary.get('total', 0))}
        {_kv('Passed', summary.get('passed', 0))}
        {_kv('Failed', summary.get('failed', 0))}
        {_kv('Errored', summary.get('errored', 0))}
        {_kv('Pass %', f"{pass_rate:.1f}%")}
        {_kv('Duration', f"{summary.get('duration_s', 0):.1f}s")}
        {_kv('Budget used', f"${budget.get('used_usd', 0):.2f}")}
        {_kv('Budget cap', f"${budget.get('budget_usd', 0):.2f}")}
        {_kv('Images gen', budget.get('generated_count', 0))}
        {_kv('Cache hits', budget.get('cache_hits', 0))}
        {_kv('Placeholders', budget.get('fallback_count', 0))}
      </div>
    </div>

    <div class="card"><h2>By track</h2>{_track_table(summary.get('by_track', {}))}</div>
    <div class="card"><h2>By difficulty</h2>{_difficulty_table(summary.get('by_difficulty', {}))}</div>

    <h2>Failures ({len(failures)})</h2>
    {''.join(_failure_card(r) for r in failures[:200])}
    {('<div class=\"small\">… truncated, see failures CSV for full list</div>' if len(failures) > 200 else '')}
    """

    html_doc = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>OtoCPA Chaos Report</title>
<style>{CSS}</style></head><body>{body}</body></html>"""
    out_path.write_text(html_doc, encoding="utf-8")
