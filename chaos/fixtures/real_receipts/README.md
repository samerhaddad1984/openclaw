# Real receipt datasets

Public datasets for chaos-testing the OCR pipeline against known ground truth.
Images are **not** checked in (~250 MB for 150 CORD receipts). Ground-truth
JSON is committed so the loader and oracle can be exercised without
re-downloading.

## CORD-v2 (included)

- Source: `naver-clova-ix/cord-v2` on Hugging Face (CC-BY 4.0).
- Content: 1,000 receipts, Indonesian (rupiah), labelled `menu` / `sub_total` / `total`.
- No vendor / date ground truth. Oracle applies `skip_fields` accordingly.

### Refetch the images

```bash
pip install --break-system-packages datasets
mkdir -p chaos/fixtures/real_receipts/cord/images chaos/fixtures/real_receipts/cord/ground_truth

python3 - <<'PY'
import json
from pathlib import Path
from datasets import load_dataset
OUT = Path('chaos/fixtures/real_receipts/cord')
count = 0
for split in ('test', 'validation', 'train'):
    for i, row in enumerate(load_dataset('naver-clova-ix/cord-v2', split=split)):
        rid = f"{split}_{i:04d}"
        row['image'].convert('RGB').save(OUT / 'images' / f"{rid}.png", format='PNG')
        (OUT / 'ground_truth' / f"{rid}.json").write_text(row['ground_truth'])
        count += 1
        if count >= 150:
            break
    if count >= 150:
        break
print(f"saved {count} receipts")
PY
```

## SROIE (included — English Malaysian retail, 652 receipts)

- Source: `arvindrajan92/sroie_document_understanding` on Hugging Face
  (SROIE-2019 mirror with per-word labels: `company`, `date`, `total`,
  `line_description`, `line_total`).
- Useful as a locale-compatible proxy for Quebec receipts — English labels,
  Decimal-point amounts, variable date formats (`25/12/2018 8:13:39 PM`).

### Refetch the images

```bash
pip install --break-system-packages pyarrow huggingface_hub
mkdir -p chaos/fixtures/real_receipts/sroie/images chaos/fixtures/real_receipts/sroie/ground_truth

python3 - <<'PY'
from huggingface_hub import hf_hub_download
from pathlib import Path
from collections import defaultdict
import pyarrow.parquet as pq
import json

p = hf_hub_download(
    repo_id="arvindrajan92/sroie_document_understanding",
    repo_type="dataset",
    filename="data/train-00000-of-00001-66201d4afd6b73ca.parquet",
)
OUT = Path("chaos/fixtures/real_receipts/sroie")
for r in pq.read_table(p).to_pylist():
    img_bytes = (r.get("image") or {}).get("bytes")
    if not img_bytes:
        continue
    groups = defaultdict(list)
    for w in (r.get("ocr") or []):
        t = (w.get("text") or "").strip()
        if t:
            groups[w.get("label") or ""].append(t)
    i = r.get("id") or 0
    name = f"sroie_{i:04d}"
    (OUT / "images" / f"{name}.jpg").write_bytes(img_bytes)
    (OUT / "ground_truth" / f"{name}.json").write_text(json.dumps({
        "company":           " ".join(groups.get("company", [])).strip() or None,
        "date":              (groups.get("date") or [None])[0],
        "total":             (groups.get("total") or [None])[0],
        "line_count":        len(groups.get("line_description", [])),
        "line_descriptions": groups.get("line_description", []),
        "line_totals":       groups.get("line_total", []),
    }, indent=2))
PY
```

## Running

```bash
python3 chaos/run_chaos.py --dataset cord --count 50 --real-ocr
python3 chaos/run_chaos.py --dataset sroie --count 50 --real-ocr
python3 chaos/reports/real_dataset_report.py \
  chaos/results/runs/<run_id> /tmp/chaos_report.md
```

Use `--skip-fields total,subtotal,tax` with CORD if you want to compare
vendor/date/line_count only (the oracle's locale-scaled tolerance already
handles IDR `.` thousands, so the flag is optional).
