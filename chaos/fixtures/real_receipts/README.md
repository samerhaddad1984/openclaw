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

## SROIE (not included)

ICDAR-2019 SROIE requires registration at https://rrc.cvc.uab.es/?ch=13.
Alternative mirrors exist on Kaggle (`urbikn/sroie-datasetv2`) and Hugging Face
(`mychen76/invoices-and-receipts_ocr_v1`). Drop `.jpg` + `.txt` pairs into
`chaos/fixtures/real_receipts/sroie/` and the loader will pick them up.

## Running

```bash
python3 chaos/run_chaos.py --dataset cord --count 50 --real-ocr
python3 chaos/reports/real_dataset_report.py \
  chaos/results/runs/<run_id> /tmp/chaos_report.md
```
