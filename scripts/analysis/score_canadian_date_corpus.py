"""Run the 50-image Canadian date corpus through process_file, score
per-receipt date extraction accuracy, write out a per-failure breakdown.

Produces ``/tmp/canadian_date_results.json``.

Budget: ~$1.50 at DocAI + haiku rates.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Load .env so Anthropic + GCP creds are present.
import os
env_path = ROOT / '.env'
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, _, v = line.partition('=')
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

MANIFEST = Path('/tmp/canadian_date_corpus/manifest.json')
OUT_PATH = Path('/tmp/canadian_date_results.json')


def bootstrap_tmp_db() -> tuple[Path, Path]:
    tmp_root = Path(tempfile.mkdtemp(prefix='can_date_'))
    db_path = tmp_root / 'can.db'
    upload_dir = tmp_root / 'uploads'
    upload_dir.mkdir(parents=True)

    prod_db = Path('/opt/otocpa/data/otocpa_agent.db')
    if prod_db.exists():
        with sqlite3.connect(str(prod_db)) as src, \
             sqlite3.connect(str(db_path)) as dst:
            rows = src.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
            for _type, _name, sql in rows:
                try:
                    dst.execute(sql)
                except sqlite3.OperationalError:
                    pass
            dst.commit()

    import scripts.review_dashboard as rd
    rd.DB_PATH = db_path
    try:
        rd.bootstrap_schema()
    except Exception:
        pass
    return db_path, upload_dir


def run() -> dict[str, Any]:
    manifest = json.loads(MANIFEST.read_text())
    db_path, upload_dir = bootstrap_tmp_db()

    from src.engines.ocr_engine import process_file

    records: list[dict[str, Any]] = []
    for i, item in enumerate(manifest, start=1):
        img = Path(item['path'])
        if not img.exists():
            continue
        try:
            result = process_file(img.read_bytes(), filename=img.name,
                                   client_code='can_date_test',
                                   db_path=db_path, upload_dir=upload_dir)
        except Exception as exc:
            records.append({**item, 'extracted': None, 'error': str(exc)})
            continue

        extracted = result.get('document_date')
        truth = item.get('truth')
        ok = (extracted == truth) if truth else None
        records.append({
            **item,
            'extracted': extracted,
            'method': result.get('extraction_method'),
            'vendor_extracted': result.get('vendor'),
            'score': ok,
        })
        if i % 10 == 0:
            print(f"[{i}/{len(manifest)}] last: {item['format_label']} "
                  f"truth={truth} extr={extracted}")

    # Score
    total = sum(1 for r in records if r.get('truth'))
    matched = sum(1 for r in records if r.get('score') is True)
    per_label: dict[str, dict[str, int]] = {}
    for r in records:
        lbl = r['format_label']
        d = per_label.setdefault(lbl, {'matched': 0, 'total': 0})
        if r.get('truth'):
            d['total'] += 1
            if r.get('score') is True:
                d['matched'] += 1

    summary = {
        'n': len(records),
        'scored': total,
        'matched': matched,
        'accuracy': round(matched / total, 4) if total else 0.0,
        'per_format': per_label,
    }

    full = {'summary': summary, 'records': records}
    OUT_PATH.write_text(json.dumps(full, indent=2, default=str))
    return summary


if __name__ == '__main__':
    print(json.dumps(run(), indent=2))
