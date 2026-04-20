"""Generate 50 synthetic Canadian receipt images with varied date formats.

Each image is a small PNG with a vendor name, an items block, a total,
and a date in one of many formats. Output includes the ground-truth
ISO date so a harness can score the OCR pipeline without relying on
external fixtures.

Output: ``/tmp/canadian_date_corpus/`` with ``img_NNN.png`` +
``manifest.json``.
"""
from __future__ import annotations

import json
import random
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont


OUT_DIR = Path('/tmp/canadian_date_corpus')
OUT_DIR.mkdir(exist_ok=True)

ENGLISH_MONTHS = ['January','February','March','April','May','June',
                  'July','August','September','October','November','December']
FRENCH_MONTHS = ['janvier','février','mars','avril','mai','juin',
                 'juillet','août','septembre','octobre','novembre','décembre']
SHORT_MONTHS  = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
UPPER_SHORT   = [m.upper() for m in SHORT_MONTHS]

VENDORS = ['Metro', 'Provigo', 'Tim Hortons', 'Canadian Tire',
           'Jean Coutu', 'Petro-Canada', 'Walmart', 'SAQ', 'Starbucks']


def _day_suffix(d: int) -> str:
    if 10 <= d % 100 <= 20:
        return 'th'
    return {1:'st', 2:'nd', 3:'rd'}.get(d % 10, 'th')


def build_formats(d: date) -> list[tuple[str, str]]:
    """Return a list of (label, formatted-date) pairs for every format
    variant we want to exercise. Label is used only in the manifest."""
    y = d.year
    m = d.month
    day = d.day
    mm = f"{m:02d}"
    dd = f"{day:02d}"
    em = ENGLISH_MONTHS[m-1]
    fm = FRENCH_MONTHS[m-1]
    sm = SHORT_MONTHS[m-1]
    um = UPPER_SHORT[m-1]
    return [
        ('iso',                    f"{y}-{mm}-{dd}"),
        ('iso_slash',              f"{y}/{mm}/{dd}"),
        ('dd_mm_yyyy_slash',       f"{dd}/{mm}/{y}"),
        ('dd_mm_yyyy_dash',        f"{dd}-{mm}-{y}"),
        ('mm_dd_yyyy_us',          f"{mm}/{dd}/{y}"),
        ('dd_mm_yy',               f"{dd}/{mm}/{y % 100:02d}"),
        ('english_long',           f"{em} {day}, {y}"),
        ('english_long_day_first', f"{day} {em} {y}"),
        ('english_short',          f"{sm} {day}, {y}"),
        ('english_upper_short',    f"{um} {day}, {y}"),
        ('french_long',            f"{day} {fm} {y}"),
        ('dd_mmm_yyyy',            f"{dd}-{um}-{y}"),
        ('yyyy_mmm_dd',            f"{y}-{um}-{dd}"),
        ('with_time',              f"{y}-{mm}-{dd} 14:30"),
        ('with_day_name',          f"Mon {dd}/{mm}/{y}"),
        ('label_en',               f"Date: {dd}/{mm}/{y}"),
        ('label_fr',               f"Date: {day} {fm} {y}"),
        ('label_receipt',          f"Receipt Date: {y}-{mm}-{dd}"),
        ('label_invoice',          f"INVOICE DATE: {mm}/{dd}/{y}"),
        ('label_bizdate',          f"BIZDATE: {dd}/{mm}/{y}"),
        ('day_suffix',             f"{em} {day}{_day_suffix(day)}, {y}"),
        ('iso_in_text',            f"Generated on {y}-{mm}-{dd} at 09:45"),
        # Note: genuinely-ambiguous forms like '03/04/YYYY' are excluded
        # because there is no ground-truth answer the OCR can be scored
        # against — the pipeline defaults to Canadian DD/MM, which is
        # correct for the operator's market but inconsistent with the
        # date we'd print here. That format is exercised only in the
        # ``ambiguous_03_04`` edge case below with truth=None.
    ]


def make_image(out: Path, vendor: str, date_str: str, *,
               total: float, noise_dates: list[str] | None = None,
               label_first: bool = False) -> None:
    """Render a vertical receipt image with the given fields."""
    W, H = 420, 520
    img = Image.new('RGB', (W, H), 'white')
    d = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf', 16)
        small = ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf', 13)
    except OSError:
        font = ImageFont.load_default()
        small = ImageFont.load_default()

    y = 20
    d.text((W // 2 - len(vendor) * 4, y), vendor, fill='black', font=font)
    y += 40
    d.text((20, y), '123 Rue Exemple', fill='black', font=small); y += 20
    d.text((20, y), 'Montreal, QC H1A 1A1', fill='black', font=small); y += 20
    d.text((20, y), 'GST: 123456789 RT0001', fill='black', font=small); y += 30

    if label_first:
        d.text((20, y), date_str, fill='black', font=font); y += 30

    for desc, amt in [('Item A', total * 0.4), ('Item B', total * 0.4),
                      ('Item C', total * 0.2)]:
        d.text((20, y), f"{desc}          ${amt:.2f}", fill='black', font=small)
        y += 20

    y += 10
    if noise_dates:
        for nd in noise_dates:
            d.text((20, y), f"REF# {nd}", fill='black', font=small); y += 18
    y += 10
    d.text((20, y), f"Subtotal          ${total * 0.87:.2f}", fill='black', font=small); y += 18
    d.text((20, y), f"GST (5%)          ${total * 0.05:.2f}", fill='black', font=small); y += 18
    d.text((20, y), f"QST (9.975%)      ${total * 0.08:.2f}", fill='black', font=small); y += 18
    d.text((20, y), f"TOTAL             ${total:.2f}", fill='black', font=font); y += 30

    if not label_first:
        d.text((20, y), date_str, fill='black', font=font)

    img.save(out, 'PNG')


def generate(seed: int = 2026_04_20) -> list[dict[str, Any]]:
    rng = random.Random(seed)

    # 50 receipts covering format variety + edge cases.
    base = date(2026, 4, 20)
    manifest: list[dict[str, Any]] = []

    # Generate across a year of dates
    format_rotation: list[tuple[str, str]] = []
    for offset in range(40):
        d_i = base - timedelta(days=offset * 9)
        for lbl, fmt in build_formats(d_i):
            format_rotation.append((lbl, fmt, d_i))

    # 40 from varied formats (picked round-robin to ensure coverage)
    rng.shuffle(format_rotation)
    for i in range(40):
        lbl, fmt, d_i = format_rotation[i]
        vendor = rng.choice(VENDORS)
        path = OUT_DIR / f"img_{i:03d}.png"
        make_image(path, vendor, fmt, total=rng.uniform(8, 220),
                   label_first=bool(i % 2))
        manifest.append({
            'file': path.name,
            'path': str(path),
            'format_label': lbl,
            'date_printed': fmt,
            'truth': d_i.isoformat(),
            'vendor': vendor,
        })

    # 10 edge cases
    edge_cases = [
        ('multi_date', 'Metro', '2026-04-20',
         ['2025-11-03', '2026-03-12'], 120.55),
        ('spanning', 'Provigo', 'Date:\n2026-04-18', None, 88.20),
        ('near_vendor', 'Tim Hortons 2026-04-19', '2026-04-19', None, 4.75),
        ('ambiguous_03_04', 'Walmart', '03/04/2026', None, 44.50),
        ('ambiguous_us_format', 'Walmart', '12/13/2026', None, 99.99),
        ('sku_then_date', 'Canadian Tire', '2026-04-15',
         ['KE23-33-53', 'REF 99-77-88'], 55.50),
        ('french_long', 'Jean Coutu', '20 avril 2026', None, 23.10),
        ('upper_month', 'SAQ', '14/JUN/2026', None, 75.00),
        ('two_digit_year', 'Starbucks', '16/03/26', None, 7.55),
        ('time_included', 'Petro-Canada', '2026-04-20 14:30:45', None, 60.00),
    ]
    for i, (lbl, vendor, date_str, noise, total) in enumerate(edge_cases, start=40):
        path = OUT_DIR / f"img_{i:03d}.png"
        # Compute truth from the date_str where possible.
        truth_map = {
            'multi_date':          '2026-04-20',
            'spanning':            '2026-04-18',
            'near_vendor':         '2026-04-19',
            'ambiguous_03_04':     None,       # genuinely ambiguous
            'ambiguous_us_format': '2026-12-13',  # forced MM/DD by day>12
            'sku_then_date':       '2026-04-15',
            'french_long':         '2026-04-20',
            'upper_month':         '2026-06-14',
            'two_digit_year':      '2026-03-16',
            'time_included':       '2026-04-20',
        }
        make_image(path, vendor, date_str, total=total, noise_dates=noise,
                   label_first=(i % 2 == 0))
        manifest.append({
            'file': path.name,
            'path': str(path),
            'format_label': lbl,
            'date_printed': date_str,
            'truth': truth_map[lbl],
            'vendor': vendor,
            'edge_case': True,
        })

    (OUT_DIR / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    return manifest


if __name__ == '__main__':
    m = generate()
    print(f"Generated {len(m)} images in {OUT_DIR}")
