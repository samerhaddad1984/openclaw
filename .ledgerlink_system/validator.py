import json
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

NUMBER_RE = re.compile(
    r"""(?x)
    (?<![A-Za-z])                 # avoid matching inside words
    (?:\$|USD|CAD|EUR|GBP)?\s*     # optional currency
    -?
    (?:
        \d{1,3}(?:,\d{3})+|\d+     # 1,234 or 1234
    )
    (?:\.\d+)?                    # optional decimals
    (?:\s*(?:%|percent))?         # optional percent
    """
)

PROVENANCE_RE = re.compile(r"(?im)^\s*PROVENANCE\s*:\s*$")
RUNLOG_RE = re.compile(r"(?i)\brun[_-]logs[\\/].+?\.json\b")
SOURCE_PAGE_RE = re.compile(r"(?i)\bSOURCE\s*:\s*.+?\(p\.\s*\d+\)")

@dataclass
class Verdict:
    status: str          # "OK" or "BLOCK"
    reasons: List[str]
    required_action: str

def extract_numbers(text: str) -> List[str]:
    # Filter out common false-positives like years can be treated as numbers too—keep strict for now.
    matches = [m.group(0).strip() for m in NUMBER_RE.finditer(text)]
    # De-dup while preserving order
    seen = set()
    out = []
    for x in matches:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def validate(text: str) -> Verdict:
    nums = extract_numbers(text)

    if not nums:
        return Verdict("OK", [], "")

    has_prov = bool(PROVENANCE_RE.search(text))
    has_runlog = bool(RUNLOG_RE.search(text))
    has_source = bool(SOURCE_PAGE_RE.search(text))

    reasons = []
    if not has_prov:
        reasons.append("Missing PROVENANCE block (required when any numbers appear).")
    if not (has_runlog or has_source):
        reasons.append("Missing run log reference (run_logs/...json) or SOURCE with page number (file (p. X)).")

    if reasons:
        return Verdict(
            "BLOCK",
            reasons,
            "Recalculate all numeric outputs via executed Python and append a PROVENANCE block referencing run_logs/<date>/run_XXXX.json OR cite SOURCE: <file> (p. X).",
        )

    return Verdict("OK", [], "")

def main() -> None:
    """
    Input JSON via stdin:
      { "text": "...", "channel": "telegram|web|...", "meta": {...} }

    Output JSON to stdout:
      { "status": "OK|BLOCK", "reasons": [...], "required_action": "..." }
    """
    raw = sys.stdin.read()
    if not raw.strip():
        print(json.dumps({"status": "BLOCK", "reasons": ["Empty input"], "required_action": "Provide text to validate."}))
        sys.exit(2)

    try:
        payload = json.loads(raw)
    except Exception as e:
        print(json.dumps({"status": "BLOCK", "reasons": [f"Invalid JSON input: {e!r}"], "required_action": "Send valid JSON."}))
        sys.exit(2)

    text = str(payload.get("text", ""))
    verdict = validate(text)

    print(json.dumps({
        "status": verdict.status,
        "reasons": verdict.reasons,
        "required_action": verdict.required_action
    }, ensure_ascii=False))

    sys.exit(0 if verdict.status == "OK" else 2)

if __name__ == "__main__":
    main()