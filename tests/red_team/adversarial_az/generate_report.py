#!/usr/bin/env python3
"""
A-Z Adversarial Campaign — Final Report Generator
===================================================
Run with: python -m pytest tests/red_team/adversarial_az/ -v --tb=short --no-header 2>&1 | python tests/red_team/adversarial_az/generate_report.py

Or standalone after running tests with JUnit XML:
  python -m pytest tests/red_team/adversarial_az/ --junitxml=az_results.xml -v
  python tests/red_team/adversarial_az/generate_report.py az_results.xml
"""
from __future__ import annotations

import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Map letter to subsystem
LETTER_MAP = {
    "a": ("Alias Poisoning", "vendor_memory_store, bank_matcher"),
    "b": ("Bank Reconciliation Ambush", "reconciliation_engine, bank_parser"),
    "c": ("CBSA Customs Chaos", "customs_engine"),
    "d": ("Date Ambiguity Destruction", "uncertainty_engine"),
    "e": ("Export Corruption", "export_engine"),
    "f": ("Fraud Engine Evasion", "fraud_engine"),
    "g": ("GL Suggestion Sabotage", "gl_account_learning_engine"),
    "h": ("Hallucination Guard Attack", "hallucination_guard"),
    "i": ("Immutable Audit Trail Breach", "audit_engine, audit_log"),
    "j": ("Journal Abuse", "concurrency_engine, correction_chain"),
    "k": ("Key/Session Abuse", "license_engine, dashboard_auth"),
    "l": ("Localization War", "i18n, substance_engine"),
    "m": ("Multi-Currency Meltdown", "multicurrency_engine"),
    "n": ("Null Data Nastiness", "tax_engine, substance_engine"),
    "o": ("OCR Sabotage", "ocr_engine, correction_chain"),
    "p": ("Payroll Pressure", "payroll_engine"),
    "q": ("Quick Method Traps", "tax_engine"),
    "r": ("RBAC Destruction", "review_permissions"),
    "s": ("Substance Confusion", "substance_engine"),
    "t": ("Tax Engine Hell", "tax_engine"),
    "u": ("Uncertainty Abuse", "uncertainty_engine"),
    "v": ("Vendor Memory Poisoning", "vendor_memory_store"),
    "w": ("Working Paper Assault", "audit_engine, cas_engine"),
    "x": ("Cross-Client Contamination", "ALL engines"),
    "y": ("Year-End Boundary", "fixed_assets_engine"),
    "z": ("Zero-Trust Disaster Replay", "correction_chain, concurrency_engine"),
}


def classify_severity(test_name: str, failure_msg: str) -> str:
    """Classify severity based on failure message patterns."""
    msg = (failure_msg or "").lower()
    if "p0" in msg or "forged session" in msg or "cross-client" in msg:
        return "P0"
    elif "p1" in msg or any(w in msg for w in ["audit trail", "immutable", "escalation", "block"]):
        return "P1"
    elif "p2" in msg or any(w in msg for w in ["defect", "splitting", "flooding", "rewriting"]):
        return "P2"
    elif "p3" in msg or "xfail" in msg:
        return "P3"
    elif "error" in msg or "assert" in msg:
        return "P2"
    return "P3"


def extract_letter(test_file: str) -> str:
    """Extract letter from test filename."""
    m = re.search(r"test_([a-z])_", test_file)
    return m.group(1) if m else "?"


def parse_junit_xml(xml_path: str) -> list[dict]:
    """Parse JUnit XML output from pytest."""
    tree = ET.parse(xml_path)
    root = tree.getroot()
    results = []

    for suite in root.iter("testsuite"):
        for case in suite.iter("testcase"):
            name = case.get("name", "")
            classname = case.get("classname", "")
            time_val = float(case.get("time", 0))

            failure = case.find("failure")
            error = case.find("error")
            skipped = case.find("skipped")

            status = "PASSED"
            message = ""
            if failure is not None:
                status = "FAILED"
                message = failure.get("message", failure.text or "")
            elif error is not None:
                status = "ERROR"
                message = error.get("message", error.text or "")
            elif skipped is not None:
                status = "SKIPPED"
                message = skipped.get("message", "")

            results.append({
                "name": name,
                "classname": classname,
                "status": status,
                "message": message,
                "time": time_val,
            })

    return results


def parse_pytest_output(lines: list[str]) -> list[dict]:
    """Parse pytest verbose text output."""
    results = []
    for line in lines:
        line = line.strip()
        if " PASSED" in line or " FAILED" in line or " ERROR" in line or " XFAIL" in line or " SKIPPED" in line:
            parts = line.split(" ")
            name = parts[0] if parts else line
            status = "PASSED"
            if "FAILED" in line:
                status = "FAILED"
            elif "ERROR" in line:
                status = "ERROR"
            elif "XFAIL" in line:
                status = "XFAIL"
            elif "SKIPPED" in line:
                status = "SKIPPED"
            results.append({"name": name, "status": status, "message": "", "classname": name})
    return results


def generate_report(results: list[dict]) -> str:
    """Generate the final adversarial campaign report."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Aggregate by letter
    by_letter: dict[str, dict] = defaultdict(lambda: {"passed": 0, "failed": 0, "xfail": 0, "error": 0, "skipped": 0, "failures": []})

    for r in results:
        letter = extract_letter(r.get("classname", r.get("name", "")))
        status = r["status"].upper()
        if status == "PASSED":
            by_letter[letter]["passed"] += 1
        elif status in ("FAILED", "XFAIL"):
            by_letter[letter]["failed"] += 1
            by_letter[letter]["failures"].append(r)
        elif status == "ERROR":
            by_letter[letter]["error"] += 1
            by_letter[letter]["failures"].append(r)
        elif status == "SKIPPED":
            by_letter[letter]["skipped"] += 1

    # Count totals
    total_passed = sum(d["passed"] for d in by_letter.values())
    total_failed = sum(d["failed"] for d in by_letter.values())
    total_error = sum(d["error"] for d in by_letter.values())
    total_skipped = sum(d["skipped"] for d in by_letter.values())
    total = total_passed + total_failed + total_error + total_skipped

    # Collect all defects with severity
    defects = []
    for letter, data in sorted(by_letter.items()):
        for f in data["failures"]:
            severity = classify_severity(f.get("name", ""), f.get("message", ""))
            info = LETTER_MAP.get(letter, ("Unknown", "unknown"))
            defects.append({
                "severity": severity,
                "letter": letter.upper(),
                "subsystem": info[0],
                "implicated": info[1],
                "test": f.get("name", ""),
                "message": (f.get("message", "")[:200]).replace("\n", " "),
            })

    # Sort defects by severity
    severity_order = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
    defects.sort(key=lambda d: severity_order.get(d["severity"], 9))

    # Build report
    lines = []
    lines.append("=" * 80)
    lines.append("  OtoCPA A-Z ADVERSARIAL CAMPAIGN — FINAL REPORT")
    lines.append(f"  Generated: {now}")
    lines.append("=" * 80)
    lines.append("")

    # Summary
    lines.append("## EXECUTIVE SUMMARY")
    lines.append(f"  Total tests:   {total}")
    lines.append(f"  Passed:        {total_passed}")
    lines.append(f"  Failed/XFail:  {total_failed}")
    lines.append(f"  Errors:        {total_error}")
    lines.append(f"  Skipped:       {total_skipped}")
    lines.append(f"  Pass rate:     {total_passed / max(total, 1) * 100:.1f}%")
    lines.append("")

    # Scoreboard by letter
    lines.append("## SCOREBOARD BY LETTER")
    lines.append(f"  {'Letter':<8} {'Attack':<35} {'Pass':>5} {'Fail':>5} {'Err':>5} {'Skip':>5}")
    lines.append("  " + "-" * 66)
    for letter in "abcdefghijklmnopqrstuvwxyz":
        if letter in by_letter:
            data = by_letter[letter]
            info = LETTER_MAP.get(letter, ("Unknown",))[0]
            lines.append(
                f"  {letter.upper():<8} {info:<35} {data['passed']:>5} "
                f"{data['failed']:>5} {data['error']:>5} {data['skipped']:>5}"
            )
    lines.append("")

    # Top 20 Worst Defects
    lines.append("## TOP 20 WORST DEFECTS")
    lines.append("")
    for i, d in enumerate(defects[:20], 1):
        lines.append(f"  #{i:02d}  [{d['severity']}] {d['letter']} — {d['subsystem']}")
        lines.append(f"       Test: {d['test']}")
        lines.append(f"       Implicated: {d['implicated']}")
        lines.append(f"       Message: {d['message']}")
        lines.append(f"       Business consequence: See test docstring for details")
        lines.append("")

    # Severity summary
    sev_counts = defaultdict(int)
    for d in defects:
        sev_counts[d["severity"]] += 1
    lines.append("## SEVERITY DISTRIBUTION")
    for s in ["P0", "P1", "P2", "P3"]:
        lines.append(f"  {s}: {sev_counts.get(s, 0)} defects")
    lines.append("")

    # Subsystem heatmap
    lines.append("## SUBSYSTEM FAILURE HEATMAP")
    sub_counts: dict[str, int] = defaultdict(int)
    for d in defects:
        sub_counts[d["implicated"]] += 1
    for sub, count in sorted(sub_counts.items(), key=lambda x: -x[1]):
        bar = "█" * count
        lines.append(f"  {sub:<40} {count:>3} {bar}")
    lines.append("")

    lines.append("=" * 80)
    lines.append("  END OF REPORT")
    lines.append("=" * 80)

    return "\n".join(lines)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].endswith(".xml"):
        results = parse_junit_xml(sys.argv[1])
    else:
        # Read from stdin (piped pytest output)
        lines = sys.stdin.readlines()
        results = parse_pytest_output(lines)

    report = generate_report(results)
    print(report)

    # Also save to file
    report_path = Path(__file__).parent / "AZ_CAMPAIGN_REPORT.txt"
    report_path.write_text(report, encoding="utf-8")
    print(f"\nReport saved to: {report_path}")
