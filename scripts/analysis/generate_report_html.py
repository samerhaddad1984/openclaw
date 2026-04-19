#!/usr/bin/env python3
"""Build a human-readable HTML review report from the JSON analysis."""
from __future__ import annotations

import base64
import html
import json
from pathlib import Path

DATA = Path("/tmp/canadian_receipts_analysis.json")
OUT = Path("/tmp/canadian_receipts_report.html")

data = json.loads(DATA.read_text())

# Sort most-broken first.
data.sort(key=lambda e: (-len(e["mismatches"]), e["file_name"]))

total = len(data)
perfect = sum(1 for e in data if not e["mismatches"])


def _thumb(fp: str) -> str:
    try:
        b = Path(fp).read_bytes()
        if len(b) > 3_000_000:
            return ""
        ext = Path(fp).suffix.lower().lstrip(".")
        mt = {"jpg": "jpeg", "jpeg": "jpeg", "png": "png", "webp": "webp"}.get(ext, ext)
        return f"data:image/{mt};base64,{base64.standard_b64encode(b).decode()}"
    except Exception:
        return ""


def _fmt(v) -> str:
    if v is None:
        return "<span class='null'>null</span>"
    return html.escape(str(v))


def _diff_cell(ours, theirs) -> str:
    same = (ours is None and theirs is None) or (
        ours is not None and theirs is not None and str(ours) == str(theirs)
    )
    cls = "ok" if same else "bad"
    return f"<td class='{cls}'>{_fmt(ours)}</td><td class='{cls}'>{_fmt(theirs)}</td>"


cards = []
for i, e in enumerate(data, 1):
    ours = e["ours"]
    truth = e["truth"]
    thumb = _thumb(e["file_path"])
    img_tag = (
        f"<img src='{thumb}' class='thumb' alt='{html.escape(e['file_name'])}'>"
        if thumb else "<div class='thumb missing'>no image</div>"
    )
    mm = "".join(f"<li>{html.escape(m)}</li>" for m in e["mismatches"]) or "<li class='none'>—</li>"
    tax_row = (
        f"<tr><th>tax_total</th><td>{_fmt(ours['tax_total'])}</td>"
        f"<td>{_fmt(e['truth_tax_total'])} "
        f"<span class='small'>(gst={_fmt(truth.get('gst'))}, qst={_fmt(truth.get('qst'))})</span></td></tr>"
    )
    api_err = truth.get("_api_error")
    api_html = f"<div class='apierr'>API error: {html.escape(api_err)}</div>" if api_err else ""
    conf = truth.get("confidence") or "?"
    notes = truth.get("notes") or ""
    notes_html = f"<div class='notes'>Claude notes: {html.escape(str(notes))}</div>" if notes else ""
    broken_cls = "broken" if e["mismatches"] else "clean"
    cards.append(f"""
    <section class='card {broken_cls}'>
      <header>
        <span class='idx'>#{i}</span>
        <h2>{html.escape(e['file_name'])}</h2>
        <span class='status'>{html.escape(e['review_status'] or '')}</span>
        <span class='conf conf-{html.escape(conf)}'>Claude: {html.escape(conf)}</span>
        <span class='count'>{len(e['mismatches'])} issue(s)</span>
      </header>
      <div class='body'>
        {img_tag}
        <div class='tables'>
          <table class='cmp'>
            <thead><tr><th></th><th>OtoCPA</th><th>Claude 4.6 Vision</th></tr></thead>
            <tbody>
              <tr><th>vendor</th><td>{_fmt(ours['vendor'])}</td><td>{_fmt(truth.get('vendor'))}</td></tr>
              <tr><th>date</th><td>{_fmt(ours['date'])}</td><td>{_fmt(truth.get('date'))}</td></tr>
              <tr><th>subtotal</th><td>{_fmt(ours['subtotal'])}</td><td>{_fmt(truth.get('subtotal'))}</td></tr>
              <tr><th>total</th><td>{_fmt(ours['total'])}</td><td>{_fmt(truth.get('total'))}</td></tr>
              {tax_row}
              <tr><th>extraction_method</th><td colspan='2' class='small'>{_fmt(e['extraction_method'])}</td></tr>
              <tr><th>Claude cost</th><td colspan='2' class='small'>${e['cost_usd']}</td></tr>
            </tbody>
          </table>
          {api_html}
          <div class='mm'><strong>Mismatches:</strong><ul>{mm}</ul></div>
          {notes_html}
          <div class='actions'>
            <button data-id='{html.escape(e['document_id'])}' class='btn ok'>Mark correct</button>
            <button data-id='{html.escape(e['document_id'])}' class='btn bad'>Mark wrong</button>
            <button data-id='{html.escape(e['document_id'])}' class='btn note'>Note correction needed</button>
          </div>
        </div>
      </div>
    </section>
    """)

per_field = {}
for f in ["vendor", "date", "subtotal", "total", "tax_total"]:
    bad = sum(1 for e in data for m in e["mismatches"] if m.startswith(f + ":"))
    per_field[f] = (total - bad, total, (total - bad) / total * 100)

pf_html = "".join(
    f"<div class='stat'><span class='label'>{k}</span>"
    f"<span class='val'>{v[2]:.1f}%</span>"
    f"<span class='sub'>{v[0]}/{v[1]}</span></div>"
    for k, v in per_field.items()
)

HTML = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>OtoCPA vs Claude Vision — 21 Canadian Receipts</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
          background: #0f172a; color: #e5e7eb; margin: 0; padding: 24px; }}
  h1 {{ color: #fbbf24; margin-top: 0; }}
  .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px,1fr));
              gap: 12px; margin-bottom: 32px; }}
  .stat {{ background: #1e293b; padding: 14px; border-radius: 10px; text-align: center; }}
  .stat .label {{ display:block; font-size:12px; text-transform: uppercase;
                   letter-spacing: 0.5px; opacity: 0.7; }}
  .stat .val {{ display:block; font-size: 26px; font-weight: 700; color: #22d3ee; margin: 4px 0;}}
  .stat .sub {{ display:block; font-size: 11px; opacity: 0.6; }}
  .card {{ background: #1e293b; border-radius: 12px; padding: 18px;
           margin-bottom: 18px; border-left: 4px solid #374151; }}
  .card.broken {{ border-left-color: #ef4444; }}
  .card.clean {{ border-left-color: #22c55e; }}
  .card header {{ display:flex; align-items:center; gap: 12px; margin-bottom: 14px; }}
  .card h2 {{ font-size: 16px; margin: 0; color: #f3f4f6; }}
  .idx {{ background: #334155; padding: 2px 8px; border-radius: 6px; font-size: 12px; }}
  .status, .conf, .count {{ font-size: 11px; padding: 3px 8px; border-radius: 6px;
                            background: #334155; text-transform: uppercase; letter-spacing: 0.4px; }}
  .count {{ background: #7f1d1d; }}
  .conf-high {{ background: #065f46; }}
  .conf-medium {{ background: #78350f; }}
  .conf-low {{ background: #7f1d1d; }}
  .body {{ display: grid; grid-template-columns: 280px 1fr; gap: 20px; align-items: start; }}
  .thumb {{ width: 100%; max-width: 280px; max-height: 380px; object-fit: contain;
            background: #0f172a; border-radius: 8px; }}
  .thumb.missing {{ height: 120px; display:flex; align-items:center; justify-content:center;
                    color: #6b7280; font-style: italic; }}
  table.cmp {{ width: 100%; border-collapse: collapse; }}
  table.cmp th {{ text-align: left; padding: 6px 10px; color: #94a3b8; font-size: 12px;
                  font-weight: 600; border-bottom: 1px solid #334155; }}
  table.cmp td {{ padding: 6px 10px; font-size: 13px; border-bottom: 1px solid #334155;
                  font-family: monospace; }}
  td.ok {{ color: #86efac; }}
  td.bad {{ color: #fca5a5; background: rgba(239,68,68,0.08); }}
  .null {{ color: #64748b; font-style: italic; }}
  .small {{ font-size: 11px; opacity: 0.7; }}
  .mm {{ margin-top: 10px; font-size: 13px; }}
  .mm ul {{ margin: 6px 0 0 0; padding-left: 20px; color: #fca5a5; }}
  .mm li.none {{ color: #86efac; list-style:none; margin-left: -20px; }}
  .notes {{ margin-top: 8px; font-size: 12px; color: #fbbf24; font-style: italic; }}
  .apierr {{ margin-top: 8px; color: #f87171; font-weight: 600; }}
  .actions {{ margin-top: 12px; display:flex; gap: 8px; }}
  .btn {{ padding: 6px 12px; border-radius: 6px; border: none; font-size: 12px;
          cursor: pointer; color: white; }}
  .btn.ok {{ background: #16a34a; }}
  .btn.bad {{ background: #dc2626; }}
  .btn.note {{ background: #2563eb; }}
  footer {{ margin-top: 32px; opacity: 0.6; font-size: 12px; text-align: center; }}
</style>
</head>
<body>
  <h1>OtoCPA vs Claude 4.6 Vision — 21 Canadian Receipts</h1>
  <p>Second-opinion comparison. Cards are sorted with the most-broken first.</p>
  <div class='summary'>
    <div class='stat'><span class='label'>Receipts</span><span class='val'>{total}</span><span class='sub'>analyzed</span></div>
    <div class='stat'><span class='label'>Perfect match</span><span class='val'>{perfect}</span><span class='sub'>{perfect/total*100:.1f}%</span></div>
    <div class='stat'><span class='label'>With issues</span><span class='val'>{total - perfect}</span><span class='sub'>{(total-perfect)/total*100:.1f}%</span></div>
    {pf_html}
  </div>
  {''.join(cards)}
  <footer>
    Generated by OtoCPA analysis script · model: claude-sonnet-4-6 ·
    data: /tmp/canadian_receipts_analysis.json
  </footer>
  <script>
    // Mark buttons just record the click for now — wire up to an API endpoint later.
    document.querySelectorAll('.btn').forEach(function(b) {{
      b.addEventListener('click', function() {{
        b.textContent = 'saved: ' + b.textContent;
        b.disabled = true;
      }});
    }});
  </script>
</body>
</html>
"""

OUT.write_text(HTML)
print(f"Wrote {OUT} ({OUT.stat().st_size // 1024} KB)")
