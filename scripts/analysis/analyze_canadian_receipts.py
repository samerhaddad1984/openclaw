#!/usr/bin/env python3
"""Second-opinion analysis of 21 recent Canadian receipts via Claude Sonnet 4.6 Vision."""
from __future__ import annotations

import base64
import json
import os
import re
import sqlite3
import sys
import time
from pathlib import Path

# Load .env
env_path = Path("/opt/otocpa/.env")
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

import anthropic

DB = "/opt/otocpa/data/otocpa_agent.db"
MODEL = "claude-sonnet-4-5"  # alias → Sonnet 4.6 is claude-sonnet-4-6
# Per the system prompt the latest Sonnet is claude-sonnet-4-6.
MODEL = "claude-sonnet-4-6"

PROMPT = """Look at this Canadian receipt. Extract each field if visible.

Respond ONLY with JSON (no markdown, no prose):
{"vendor": "exact printed vendor name or null",
 "date": "YYYY-MM-DD or null",
 "subtotal": number or null,
 "gst": number or null,
 "qst": number or null,
 "total": number or null,
 "line_count": integer,
 "confidence": "high|medium|low",
 "notes": "short note on anything unusual or null"}

Rules:
- gst = federal tax (GST/TPS, ~5%). Null if not on receipt.
- qst = provincial tax (QST/TVQ, ~9.975%) or other provincial sales tax. Null if not on receipt.
- If only a combined tax line, put the full amount in gst and leave qst null.
- Use null (not 0) for fields that are missing."""

MEDIA_TYPE = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".png": "image/png", ".webp": "image/webp",
    ".gif": "image/gif", ".heic": "image/heic",
}


def _parse_json(text: str) -> dict:
    text = text.strip()
    # Strip code fences if present.
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except Exception:
                pass
        return {"_parse_error": True, "raw": text[:500]}


def _num(x):
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _close(a, b, tol=0.02) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tol


def _norm_vendor(s):
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    # Always target the same 21 receipts from the initial audit so
    # before/after numbers stay comparable across runs and ingest sources
    # (reingest changes ingest_source to 'reingest_manual', which would
    # otherwise drop these rows out of a time-based query).
    audit_path = Path(
        "/opt/otocpa/scripts/analysis/canadian_receipts_analysis_before_fixes.json"
    )
    if audit_path.exists():
        target_ids = [e["document_id"] for e in json.loads(audit_path.read_text())]
        rows = conn.execute(
            f"SELECT document_id, file_name, file_path, client_code, review_status, "
            f"vendor, amount, subtotal, tax_total, document_date, category, "
            f"extraction_method, raw_result, created_at FROM documents "
            f"WHERE document_id IN ({','.join('?'*len(target_ids))})",
            target_ids,
        ).fetchall()
    else:
        rows = conn.execute("""
            SELECT document_id, file_name, file_path, client_code, review_status,
                   vendor, amount, subtotal, tax_total, document_date, category,
                   extraction_method, raw_result, created_at
            FROM documents
            WHERE created_at > datetime('now', '-48 hours')
              AND ingest_source IN ('web_upload','public_upload','portal','reingest_manual')
              AND (subtotal IS NOT NULL OR amount IS NOT NULL OR vendor IS NOT NULL)
            ORDER BY created_at DESC
        """).fetchall()
    print(f"{len(rows)} receipts to analyze\n")

    client = anthropic.Anthropic()
    report = []
    total_cost = 0.0

    for i, r in enumerate(rows, 1):
        fp = r["file_path"] or ""
        if not fp or not Path(fp).exists():
            print(f"{i}. SKIP (file missing): {r['file_name']}")
            continue

        ext = Path(fp).suffix.lower()
        media = MEDIA_TYPE.get(ext)
        if not media:
            print(f"{i}. SKIP (unsupported type {ext}): {r['file_name']}")
            continue

        img_b64 = base64.standard_b64encode(Path(fp).read_bytes()).decode()

        try:
            resp = client.messages.create(
                model=MODEL,
                max_tokens=500,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image", "source": {
                            "type": "base64", "media_type": media, "data": img_b64,
                        }},
                        {"type": "text", "text": PROMPT},
                    ],
                }],
            )
            truth = _parse_json(resp.content[0].text)
            usage = resp.usage
            # Sonnet 4.6: $3/MTok input, $15/MTok output (approximate).
            cost = (usage.input_tokens / 1_000_000) * 3.0 + (usage.output_tokens / 1_000_000) * 15.0
            total_cost += cost
        except Exception as e:
            truth = {"_api_error": str(e)}
            cost = 0.0

        # Our tax_total is combined; Claude returns gst + qst separately.
        our = {
            "vendor": r["vendor"],
            "date": r["document_date"],
            "subtotal": _num(r["subtotal"]),
            "total": _num(r["amount"]),
            "tax_total": _num(r["tax_total"]),
        }
        truth_gst = _num(truth.get("gst"))
        truth_qst = _num(truth.get("qst"))
        truth_tax_total = None
        if truth_gst is not None or truth_qst is not None:
            truth_tax_total = (truth_gst or 0) + (truth_qst or 0)

        mismatches = []
        # Vendor: compare normalized prefix (first 10 alphanum).
        our_v = _norm_vendor(our["vendor"])[:10]
        truth_v = _norm_vendor(truth.get("vendor"))[:10]
        if our_v and truth_v and our_v != truth_v:
            mismatches.append(f"vendor: ours={our['vendor']!r} truth={truth.get('vendor')!r}")
        elif truth_v and not our_v:
            mismatches.append(f"vendor: we had nothing, truth={truth.get('vendor')!r}")
        elif our_v and not truth_v:
            mismatches.append(f"vendor: we had {our['vendor']!r}, truth=null")
        # Date: exact string match (both YYYY-MM-DD).
        if truth.get("date") and our["date"] and truth["date"] != our["date"]:
            mismatches.append(f"date: ours={our['date']} truth={truth['date']}")
        elif truth.get("date") and not our["date"]:
            mismatches.append(f"date: we had null, truth={truth['date']}")
        # Numeric fields.
        if not _close(our["total"], _num(truth.get("total"))):
            mismatches.append(f"total: ours={our['total']} truth={truth.get('total')}")
        if not _close(our["subtotal"], _num(truth.get("subtotal"))):
            mismatches.append(f"subtotal: ours={our['subtotal']} truth={truth.get('subtotal')}")
        if not _close(our["tax_total"], truth_tax_total):
            mismatches.append(
                f"tax_total: ours={our['tax_total']} truth={truth_tax_total} "
                f"(gst={truth_gst} qst={truth_qst})"
            )

        entry = {
            "document_id": r["document_id"],
            "file_name": r["file_name"],
            "file_path": fp,
            "review_status": r["review_status"],
            "extraction_method": r["extraction_method"],
            "ours": our,
            "truth": truth,
            "truth_tax_total": truth_tax_total,
            "mismatches": mismatches,
            "cost_usd": round(cost, 4),
        }
        report.append(entry)
        mark = "✓" if not mismatches else f"✗ ({len(mismatches)})"
        print(f"{i:2}. {mark} {r['file_name']}")
        for m in mismatches:
            print(f"      {m}")
        # Gentle rate limit.
        time.sleep(0.2)

    out = Path("/tmp/canadian_receipts_analysis.json")
    out.write_text(json.dumps(report, indent=2, default=str))

    total = len(report)
    perfect = sum(1 for e in report if not e["mismatches"] and "_api_error" not in e["truth"])
    api_errors = sum(1 for e in report if "_api_error" in e["truth"])
    print(f"\n=== SUMMARY ===")
    print(f"Analyzed:       {total}")
    print(f"API errors:     {api_errors}")
    print(f"Perfect match:  {perfect} ({perfect/total*100:.1f}%)")
    print(f"With issues:    {total - perfect - api_errors}")
    # Per-field accuracy.
    for fld in ["vendor", "date", "subtotal", "total", "tax_total"]:
        bad = sum(1 for e in report for m in e["mismatches"] if m.startswith(fld + ":"))
        print(f"  {fld:10} {(total - bad)/total*100:5.1f}%  ({total - bad}/{total})")
    print(f"\nReport: {out}")
    print(f"Estimated cost: ${total_cost:.4f}")
    conn.close()
    return report, total_cost


if __name__ == "__main__":
    main()
