"""Load real-world receipt datasets for chaos testing.

Supports:

- ``cord``  — CORD-v2 (Clova-IX), 1000 receipts, Indonesian (rupiah).
              Ground truth: menu, sub_total, total; no vendor/date fields.
- ``sroie`` — ICDAR-2019 SROIE mirror (companies, dates, totals).

The loader normalises each row to a common schema so the receipt oracle
can score any dataset uniformly. Non-Canadian datasets surface
``skip_fields`` so the oracle skips ``gst`` / ``qst`` instead of
penalising every row.
"""
from __future__ import annotations

import json
import logging
import re
import statistics
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator

log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DATASETS_ROOT = REPO_ROOT / "chaos" / "fixtures" / "real_receipts"


# ---------------------------------------------------------------------------
# Amount parsing — each dataset has its own locale convention
# ---------------------------------------------------------------------------

_AMOUNT_RE = re.compile(r"[-+]?\d[\d.,]*")


def _parse_cord_amount(raw: Any) -> Decimal | None:
    """CORD uses Indonesian rupiah with '.' as thousands separator.

    '60.000' means 60,000 rupiah, not 60.0. '5.455' means 5,455.
    Decimals are rare; when present they're usually '.00' tails.
    Heuristic: if the value has exactly one '.' and three digits after
    it, treat as thousands; otherwise treat as decimal.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    m = _AMOUNT_RE.search(s)
    if not m:
        return None
    token = m.group(0).replace(",", "")
    if "." in token:
        # Indonesian convention: dot = thousands if followed by exactly 3 digits
        parts = token.split(".")
        if len(parts) >= 2 and all(len(p) == 3 for p in parts[1:]):
            token = token.replace(".", "")
    try:
        return Decimal(token)
    except (InvalidOperation, ValueError):
        return None


def _parse_sroie_amount(raw: Any) -> Decimal | None:
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "")
    m = _AMOUNT_RE.search(s)
    if not m:
        return None
    try:
        return Decimal(m.group(0))
    except (InvalidOperation, ValueError):
        return None


# ---------------------------------------------------------------------------
# Image-quality heuristic for difficulty auto-classification
# ---------------------------------------------------------------------------

def _estimate_difficulty(image_path: Path) -> str:
    """Cheap quality estimate. Reads only pixel statistics — no OCR."""
    try:
        from PIL import Image, ImageStat  # type: ignore
    except Exception:
        return "normal"
    try:
        with Image.open(image_path) as im:
            w, h = im.size
            gray = im.convert("L")
            stat = ImageStat.Stat(gray)
            mean = stat.mean[0]
            stddev = stat.stddev[0]
    except Exception:
        return "normal"

    # Resolution bucket
    longest = max(w, h)
    if longest < 400:
        res = "nightmare"
    elif longest < 700:
        res = "hard"
    elif longest < 1200:
        res = "normal"
    else:
        res = "easy"

    # Contrast bucket (stddev of a crisp receipt ≥ 60; faded < 30)
    if stddev < 25:
        contrast = "nightmare"
    elif stddev < 40:
        contrast = "hard"
    elif stddev < 60:
        contrast = "normal"
    else:
        contrast = "easy"

    order = {"easy": 0, "normal": 1, "hard": 2, "nightmare": 3}
    worst = max(res, contrast, key=lambda x: order[x])
    return worst


# ---------------------------------------------------------------------------
# Normalised receipt record
# ---------------------------------------------------------------------------

@dataclass
class RealReceipt:
    image_path: Path
    vendor: str | None
    document_date: str | None
    total: str | None
    subtotal: str | None
    gst: str | None
    qst: str | None
    currency: str
    line_count: int
    line_items: list[dict[str, Any]]
    source_dataset: str
    difficulty_estimate: str
    skip_fields: list[str]
    raw_gt: dict[str, Any]

    def to_scenario(self) -> dict[str, Any]:
        """Convert to a chaos scenario dict matching ReceiptRunner's shape."""
        gt: dict[str, Any] = {}
        if self.vendor is not None:
            gt["vendor"] = self.vendor
        if self.document_date is not None:
            gt["document_date"] = self.document_date
        if self.total is not None:
            gt["total"] = self.total
        if self.subtotal is not None:
            gt["subtotal"] = self.subtotal
        if self.gst is not None:
            gt["gst"] = self.gst
        if self.qst is not None:
            gt["qst"] = self.qst
        gt["currency"] = self.currency
        gt["line_count"] = self.line_count
        return {
            "id": f"{self.source_dataset}_{self.image_path.stem}",
            "category": "receipts",
            "subtype": f"real_{self.source_dataset}_{self.difficulty_estimate}",
            "difficulty": self.difficulty_estimate,
            "description": f"Real receipt from {self.source_dataset} ({self.difficulty_estimate})",
            "severity_on_failure": "medium",
            "expected_fail": False,
            "future_feature": False,
            "affects_engines": ["src.engines.ocr_engine"],
            "oracle": "receipt",
            "input_spec": {
                "kind": "real_receipt",
                "image_path": str(self.image_path),
                "source_dataset": self.source_dataset,
                "conditions": [f"real_{self.source_dataset}"],
                "skip_fields": list(self.skip_fields),
            },
            "ground_truth": gt,
        }


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

class RealReceiptLoader:
    def __init__(self, dataset_name: str = "cord", base_path: Path | None = None):
        self.dataset_name = dataset_name
        self.base_path = Path(base_path) if base_path else DATASETS_ROOT / dataset_name
        if not self.base_path.exists():
            raise FileNotFoundError(
                f"Dataset {dataset_name!r} not found at {self.base_path}. "
                f"Run the download script first."
            )
        self.receipts: list[RealReceipt] = self._index_dataset()
        log.info("loaded %d receipts from %s", len(self.receipts), self.base_path)

    # ----- indexing --------------------------------------------------------
    def _index_dataset(self) -> list[RealReceipt]:
        if self.dataset_name == "cord":
            return self._index_cord()
        if self.dataset_name == "sroie":
            return self._index_sroie()
        raise ValueError(f"unknown dataset: {self.dataset_name!r}")

    def _index_cord(self) -> list[RealReceipt]:
        images_dir = self.base_path / "images"
        gt_dir = self.base_path / "ground_truth"
        out: list[RealReceipt] = []
        for img in sorted(images_dir.glob("*.png")):
            gt_path = gt_dir / f"{img.stem}.json"
            if not gt_path.exists():
                continue
            try:
                raw = json.loads(gt_path.read_text())
            except Exception as e:
                log.warning("bad gt json %s: %s", gt_path, e)
                continue
            parse = raw.get("gt_parse") or {}
            menu = parse.get("menu") or []
            if isinstance(menu, dict):
                menu = [menu]
            line_items: list[dict[str, Any]] = []
            for entry in menu:
                if not isinstance(entry, dict):
                    continue
                amt = _parse_cord_amount(entry.get("price"))
                line_items.append({
                    "description": str(entry.get("nm") or "").strip(),
                    "amount": str(amt) if amt is not None else None,
                    "count": str(entry.get("cnt") or "").strip(),
                })
            subtotal = None
            if isinstance(parse.get("sub_total"), dict):
                subtotal_val = _parse_cord_amount(
                    parse["sub_total"].get("subtotal_price")
                )
                if subtotal_val is not None:
                    subtotal = str(subtotal_val)
            total = None
            if isinstance(parse.get("total"), dict):
                total_val = _parse_cord_amount(parse["total"].get("total_price"))
                if total_val is not None:
                    total = str(total_val)
            difficulty = _estimate_difficulty(img)
            out.append(RealReceipt(
                image_path=img,
                vendor=None,            # CORD has no vendor ground truth
                document_date=None,     # CORD has no date ground truth
                total=total,
                subtotal=subtotal,
                gst=None,
                qst=None,
                currency="IDR",
                line_count=len(line_items),
                line_items=line_items,
                source_dataset="cord",
                difficulty_estimate=difficulty,
                # CORD is Indonesian → no vendor/date/GST/QST ground truth
                # to compare against. Currency is always IDR so our CAD
                # default is irrelevant for scoring.
                skip_fields=["gst", "qst", "vendor", "document_date",
                             "tax_code", "currency"],
                raw_gt=raw,
            ))
        return out

    def _index_sroie(self) -> list[RealReceipt]:
        out: list[RealReceipt] = []
        # SROIE layout (our mirror of arvindrajan92/sroie_document_understanding):
        #   images/<name>.jpg + ground_truth/<name>.json
        #   json has: company, date, total, line_count, line_descriptions, line_totals.
        # Older "flat" SROIE mirrors: <name>.jpg + <name>.txt or <name>.json in
        # the same directory — we still support that layout for safety.
        images_dir = self.base_path / "images"
        gt_dir = self.base_path / "ground_truth"

        if images_dir.is_dir() and gt_dir.is_dir():
            candidates = [(img, gt_dir / f"{img.stem}.json")
                          for img in sorted(images_dir.glob("*.jpg"))]
        else:
            candidates = []
            for img in sorted(self.base_path.rglob("*.jpg")):
                gt = img.with_suffix(".json")
                if not gt.exists():
                    gt = img.with_suffix(".txt")
                if gt.exists():
                    candidates.append((img, gt))

        for img, gt in candidates:
            if not gt.exists():
                continue
            vendor = date = total = None
            line_count = 0
            line_items: list[dict[str, Any]] = []
            try:
                if gt.suffix == ".json":
                    data = json.loads(gt.read_text())
                    vendor = data.get("company")
                    date = data.get("date")
                    total = data.get("total")
                    line_count = int(data.get("line_count") or 0)
                    descs = data.get("line_descriptions") or []
                    totals = data.get("line_totals") or []
                    for i, d in enumerate(descs):
                        amt = totals[i] if i < len(totals) else None
                        amt_d = _parse_sroie_amount(amt) if amt else None
                        line_items.append({
                            "description": str(d),
                            "amount": str(amt_d) if amt_d is not None else None,
                        })
                else:
                    for line in gt.read_text(errors="ignore").splitlines():
                        if ":" not in line:
                            continue
                        k, v = line.split(":", 1)
                        k, v = k.strip().lower(), v.strip()
                        if k == "company":
                            vendor = v
                        elif k == "date":
                            date = v
                        elif k == "total":
                            total = v
            except Exception as e:
                log.warning("bad sroie gt %s: %s", gt, e)
                continue
            total_d = _parse_sroie_amount(total)
            difficulty = _estimate_difficulty(img)
            out.append(RealReceipt(
                image_path=img,
                vendor=vendor,
                document_date=date,
                total=str(total_d) if total_d is not None else None,
                subtotal=None,
                gst=None,
                qst=None,
                currency="MYR",  # SROIE is mostly Malaysian retail, English labels
                line_count=line_count or len(line_items),
                line_items=line_items,
                source_dataset="sroie",
                difficulty_estimate=difficulty,
                # SROIE has English text but no subtotal/GST/QST — we score
                # vendor/date/total/line_count. Currency is MYR but our engine
                # defaults to CAD; skip currency to avoid scoring noise.
                skip_fields=["gst", "qst", "subtotal", "tax_code", "currency"],
                raw_gt={"company": vendor, "date": date, "total": total},
            ))
        return out

    # ----- sampling --------------------------------------------------------
    def sample(self, n: int = 50, difficulty: str = "all") -> list[RealReceipt]:
        pool = self.receipts if difficulty == "all" else [
            r for r in self.receipts if r.difficulty_estimate == difficulty
        ]
        return pool[:n]

    def __iter__(self) -> Iterator[RealReceipt]:
        return iter(self.receipts)

    def __len__(self) -> int:
        return len(self.receipts)

    # ----- quick summary ---------------------------------------------------
    def stats(self) -> dict[str, Any]:
        by_diff: dict[str, int] = {}
        line_counts: list[int] = []
        for r in self.receipts:
            by_diff[r.difficulty_estimate] = by_diff.get(r.difficulty_estimate, 0) + 1
            line_counts.append(r.line_count)
        return {
            "total":         len(self.receipts),
            "by_difficulty": by_diff,
            "median_lines":  statistics.median(line_counts) if line_counts else 0,
            "max_lines":     max(line_counts) if line_counts else 0,
        }
