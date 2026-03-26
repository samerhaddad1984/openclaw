"""Generate 200 realistic bad-quality document images to stress-test the OCR pipeline."""

import json
import math
import os
import random
import sys
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw, ImageFilter, ImageFont

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "data" / "training" / "messy_images"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

random.seed(42)
np.random.seed(42)

# ---------------------------------------------------------------------------
# Font helpers – use built-in Pillow fonts as fallback
# ---------------------------------------------------------------------------

def _get_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Return a TrueType font if available, else Pillow default."""
    candidates = [
        "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "",
    ]
    for path in candidates:
        if path and os.path.isfile(path):
            try:
                return ImageFont.truetype(path, size)
            except OSError:
                continue
    return ImageFont.load_default()


def _get_mono_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "C:/Windows/Fonts/consola.ttf",
        "C:/Windows/Fonts/cour.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    ]
    for path in candidates:
        if os.path.isfile(path):
            try:
                return ImageFont.truetype(path, size)
            except OSError:
                continue
    return ImageFont.load_default()


# ===================================================================
# DISTORTION FUNCTIONS
# ===================================================================

def add_bad_lighting(image: Image.Image, severity: float) -> Image.Image:
    """Dark vignette corners, random bright spot/glare, reduced contrast, warm yellow tint."""
    img = image.copy().convert("RGB")
    w, h = img.size
    arr = np.array(img, dtype=np.float32)

    # --- vignette ---
    Y, X = np.ogrid[:h, :w]
    cx, cy = w / 2, h / 2
    dist = np.sqrt((X - cx) ** 2 + (Y - cy) ** 2)
    max_dist = np.sqrt(cx ** 2 + cy ** 2)
    vignette = 1.0 - severity * 0.6 * (dist / max_dist) ** 2
    arr *= vignette[:, :, np.newaxis]

    # --- random bright spot / glare ---
    gx = random.randint(int(w * 0.2), int(w * 0.8))
    gy = random.randint(int(h * 0.2), int(h * 0.8))
    glare_r = int(min(w, h) * 0.25 * severity)
    if glare_r > 0:
        dist_g = np.sqrt((X - gx) ** 2 + (Y - gy) ** 2)
        glare = np.clip(1.0 - dist_g / glare_r, 0, 1) * severity * 120
        arr += glare[:, :, np.newaxis]

    # --- reduced contrast ---
    mean = arr.mean()
    arr = mean + (arr - mean) * (1.0 - severity * 0.4)

    # --- warm yellow tint ---
    arr[:, :, 0] += severity * 25  # red
    arr[:, :, 1] += severity * 15  # green

    arr = np.clip(arr, 0, 255).astype(np.uint8)
    return Image.fromarray(arr)


def add_angle_distortion(image: Image.Image, max_degrees: int = 15) -> Image.Image:
    """Rotate randomly up to max_degrees, perspective transform, slight blur."""
    angle = random.uniform(-max_degrees, max_degrees)
    img = image.rotate(angle, resample=Image.BICUBIC, expand=False, fillcolor=(245, 245, 245))

    # slight perspective via quad transform
    w, h = img.size
    d = int(min(w, h) * 0.04)
    coeffs = [
        random.randint(0, d), random.randint(0, d),
        random.randint(0, d), h - random.randint(0, d),
        w - random.randint(0, d), h - random.randint(0, d),
        w - random.randint(0, d), random.randint(0, d),
    ]
    img = img.transform((w, h), Image.QUAD, coeffs, resample=Image.BICUBIC)

    # slight blur
    img = img.filter(ImageFilter.GaussianBlur(radius=random.uniform(0.3, 1.2)))
    return img


def add_crumple_effect(image: Image.Image, severity: float) -> Image.Image:
    """Wave distortion, dark crease lines, reduced sharpness."""
    img = image.copy().convert("RGB")
    w, h = img.size
    arr = np.array(img, dtype=np.float32)

    # --- wave distortion ---
    amplitude = severity * 6
    freq = random.uniform(0.01, 0.03)
    Y, X = np.mgrid[:h, :w]
    dx = (amplitude * np.sin(2 * np.pi * freq * Y)).astype(int)
    dy = (amplitude * np.sin(2 * np.pi * freq * X)).astype(int)
    new_x = np.clip(X + dx, 0, w - 1)
    new_y = np.clip(Y + dy, 0, h - 1)
    warped = arr[new_y, new_x]

    # --- dark crease lines ---
    n_creases = int(2 + severity * 5)
    crease_img = Image.fromarray(warped.astype(np.uint8))
    draw = ImageDraw.Draw(crease_img)
    for _ in range(n_creases):
        x0 = random.randint(0, w)
        y0 = random.randint(0, h)
        x1 = random.randint(0, w)
        y1 = random.randint(0, h)
        lw = random.randint(1, max(1, int(severity * 3)))
        gray = random.randint(60, 130)
        draw.line([(x0, y0), (x1, y1)], fill=(gray, gray, gray), width=lw)

    # --- reduced sharpness ---
    blur_r = 0.5 + severity * 1.5
    crease_img = crease_img.filter(ImageFilter.GaussianBlur(radius=blur_r))
    return crease_img


def add_thermal_fading(image: Image.Image, severity: float) -> Image.Image:
    """Near-grayscale brown tint, reduced contrast, horizontal fade bands."""
    img = image.copy().convert("RGB")
    arr = np.array(img, dtype=np.float32)

    # desaturate
    gray = arr.mean(axis=2, keepdims=True)
    arr = arr * (1 - severity * 0.85) + gray * severity * 0.85

    # brown tint
    arr[:, :, 0] += severity * 20
    arr[:, :, 1] += severity * 10

    # reduce contrast
    mean = arr.mean()
    arr = mean + (arr - mean) * (1.0 - severity * 0.5)

    # horizontal fade bands
    h = arr.shape[0]
    n_bands = random.randint(2, 5)
    for _ in range(n_bands):
        y_start = random.randint(0, h - 1)
        band_h = random.randint(20, max(21, int(h * 0.15)))
        y_end = min(h, y_start + band_h)
        fade = 1.0 - severity * random.uniform(0.15, 0.4)
        arr[y_start:y_end] *= fade

    arr = np.clip(arr, 0, 255).astype(np.uint8)
    return Image.fromarray(arr)


def add_shadow(image: Image.Image, severity: float) -> Image.Image:
    """Gradient shadow from one edge, darkened random region."""
    img = image.copy().convert("RGB")
    w, h = img.size
    arr = np.array(img, dtype=np.float32)

    # gradient shadow from random edge
    edge = random.choice(["left", "right", "top", "bottom"])
    gradient = np.ones((h, w), dtype=np.float32)
    if edge == "left":
        gradient *= np.linspace(1.0 - severity * 0.5, 1.0, w)[np.newaxis, :]
    elif edge == "right":
        gradient *= np.linspace(1.0, 1.0 - severity * 0.5, w)[np.newaxis, :]
    elif edge == "top":
        gradient *= np.linspace(1.0 - severity * 0.5, 1.0, h)[:, np.newaxis]
    else:
        gradient *= np.linspace(1.0, 1.0 - severity * 0.5, h)[:, np.newaxis]
    arr *= gradient[:, :, np.newaxis]

    # darkened random region
    rx = random.randint(0, max(1, w - 150))
    ry = random.randint(0, max(1, h - 150))
    rw = random.randint(80, 200)
    rh = random.randint(80, 200)
    arr[ry:ry + rh, rx:rx + rw] *= (1.0 - severity * 0.35)

    arr = np.clip(arr, 0, 255).astype(np.uint8)
    return Image.fromarray(arr)


# ===================================================================
# BASE DOCUMENT GENERATORS
# ===================================================================

def generate_base_receipt(
    vendor: str, amount: float, date: str,
    gst: float, qst: float, language: str,
) -> Image.Image:
    """White background 600x900px receipt with vendor, date, amounts, GST/QST."""
    img = Image.new("RGB", (600, 900), "white")
    draw = ImageDraw.Draw(img)
    font_bold = _get_font(28, bold=True)
    font_reg = _get_font(20)
    font_sm = _get_font(16)

    y = 30
    # vendor name bold at top
    draw.text((300, y), vendor.upper(), fill="black", font=font_bold, anchor="mt")
    y += 50

    # address / phone filler
    addr = random.choice(["123 rue Principale", "456 boul. des Laurentides", "789 ch. Ste-Foy"])
    draw.text((300, y), addr, fill="gray", font=font_sm, anchor="mt")
    y += 30

    # date
    lbl_date = "Date:" if language == "en" else "Date:"
    draw.text((50, y), f"{lbl_date} {date}", fill="black", font=font_reg)
    y += 40

    # separator
    draw.line([(40, y), (560, y)], fill="black", width=1)
    y += 20

    # random line items
    subtotal = amount - gst - qst
    n_items = random.randint(1, 5)
    item_labels_en = ["Office supplies", "Hardware", "Fuel", "Parking", "Materials", "Service"]
    item_labels_fr = ["Fournitures", "Quincaillerie", "Essence", "Stationnement", "Matériaux", "Service"]
    items = item_labels_fr if language == "fr" else item_labels_en
    item_amounts = []
    for i in range(n_items):
        if i == n_items - 1:
            ia = round(subtotal - sum(item_amounts), 2)
        else:
            ia = round(random.uniform(1.0, subtotal / max(1, n_items) * 1.5), 2)
            ia = min(ia, subtotal - sum(item_amounts) - 0.01 * (n_items - i - 1))
        item_amounts.append(ia)
        label = random.choice(items)
        draw.text((60, y), label, fill="black", font=font_reg)
        draw.text((500, y), f"${ia:.2f}", fill="black", font=font_reg, anchor="rt")
        y += 30

    y += 10
    draw.line([(40, y), (560, y)], fill="black", width=1)
    y += 15

    # subtotal
    lbl_sub = "Sous-total" if language == "fr" else "Subtotal"
    draw.text((60, y), lbl_sub, fill="black", font=font_reg)
    draw.text((500, y), f"${subtotal:.2f}", fill="black", font=font_reg, anchor="rt")
    y += 30

    # GST
    draw.text((60, y), "TPS/GST (5%)", fill="black", font=font_reg)
    draw.text((500, y), f"${gst:.2f}", fill="black", font=font_reg, anchor="rt")
    y += 30

    # QST
    draw.text((60, y), "TVQ/QST (9.975%)", fill="black", font=font_reg)
    draw.text((500, y), f"${qst:.2f}", fill="black", font=font_reg, anchor="rt")
    y += 30

    draw.line([(40, y), (560, y)], fill="black", width=2)
    y += 15

    # total
    lbl_total = "TOTAL"
    draw.text((60, y), lbl_total, fill="black", font=font_bold)
    draw.text((500, y), f"${amount:.2f}", fill="black", font=font_bold, anchor="rt")
    y += 50

    # payment method
    method = random.choice(["VISA ****1234", "MC ****5678", "INTERAC", "COMPTANT / CASH"])
    draw.text((300, y), method, fill="gray", font=font_sm, anchor="mt")
    y += 30
    draw.text((300, y), "MERCI / THANK YOU", fill="gray", font=font_sm, anchor="mt")

    return img


def generate_handwritten_receipt(
    vendor: str, amount: float, date: str, language: str,
) -> Image.Image:
    """Slightly yellow background, monospace font with random slight rotations per line."""
    bg_color = (255, 252, 235)  # slight yellow
    img = Image.new("RGB", (600, 900), bg_color)
    font_mono = _get_mono_font(22)

    subtotal = round(amount / 1.14975, 2)
    gst = round(subtotal * 0.05, 2)
    qst = round(subtotal * 0.09975, 2)
    # adjust so total matches
    total = subtotal + gst + qst
    diff = round(amount - total, 2)
    subtotal = round(subtotal + diff, 2)

    lines_fr = [
        f"  {vendor}",
        f"  Date: {date}",
        "",
        f"  Service / travaux",
        f"  Montant: {subtotal:.2f}$",
        f"  TPS: {gst:.2f}$",
        f"  TVQ: {qst:.2f}$",
        "",
        f"  TOTAL: {amount:.2f}$",
        "",
        f"  Payé comptant",
        f"  Merci!",
    ]
    lines_en = [
        f"  {vendor}",
        f"  Date: {date}",
        "",
        f"  Service / labour",
        f"  Amount: ${subtotal:.2f}",
        f"  GST: ${gst:.2f}",
        f"  QST: ${qst:.2f}",
        "",
        f"  TOTAL: ${amount:.2f}",
        "",
        f"  Paid cash",
        f"  Thank you!",
    ]
    lines = lines_fr if language == "fr" else lines_en

    y = 40
    for line in lines:
        # create small image for each line, rotate slightly
        if not line.strip():
            y += 25
            continue
        line_img = Image.new("RGB", (560, 36), bg_color)
        line_draw = ImageDraw.Draw(line_img)
        # slight color variation to simulate pen pressure
        ink = (
            random.randint(10, 50),
            random.randint(10, 50),
            random.randint(80, 160),
        )
        line_draw.text((5, 5), line, fill=ink, font=font_mono)

        # random slight rotation
        rot = random.uniform(-2.5, 2.5)
        line_img = line_img.rotate(rot, resample=Image.BICUBIC, expand=False, fillcolor=bg_color)

        img.paste(line_img, (20 + random.randint(-3, 3), y))
        y += 38 + random.randint(-2, 4)

    return img


# ===================================================================
# DATA POOLS
# ===================================================================

THERMAL_VENDORS = ["Ultramar", "Esso", "Petro-Canada", "Couche-Tard", "Stationnement Centre-Ville"]
PHONE_VENDORS = ["Home Depot", "Rona", "Bureau en Gros", "Restaurant Le Sultan",
                 "Tim Hortons", "McDonald's", "Subway"]
CRUMPLE_VENDORS = ["Plomberie J. Tremblay", "Marché Jean-Talon", "Cash receipt - misc",
                   "Entrepreneur Général Lavoie", "Rénovations Côté"]
HANDWRITTEN_VENDORS = ["Plombier Mario", "Électricien Gagné", "Peintre Leblanc",
                       "Marché du Coin", "Menuiserie Bouchard"]


def _rand_amount() -> tuple[float, float, float]:
    """Return (total, gst, qst) with proper Quebec tax math."""
    subtotal = round(random.uniform(8.0, 500.0), 2)
    gst = round(subtotal * 0.05, 2)
    qst = round(subtotal * 0.09975, 2)
    total = round(subtotal + gst + qst, 2)
    return total, gst, qst


def _rand_date() -> str:
    """Return a random date within the last 6 months as YYYY-MM-DD."""
    import datetime
    base = datetime.date(2026, 3, 23)
    offset = random.randint(0, 180)
    d = base - datetime.timedelta(days=offset)
    return d.isoformat()


# ===================================================================
# MAIN GENERATION
# ===================================================================

def generate_all() -> list[dict]:
    """Generate 200 images and return list of ground-truth metadata dicts."""
    manifests: list[dict] = []
    idx = 0

    def _save(img: Image.Image, meta: dict) -> None:
        nonlocal idx
        idx += 1
        fname = f"receipt_{idx:03d}.png"
        meta["image_file"] = fname
        img.save(OUTPUT_DIR / fname)
        json_path = OUTPUT_DIR / f"receipt_{idx:03d}.json"
        json_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        manifests.append(meta)

    # --- 50 thermal receipts ---
    for i in range(50):
        vendor = random.choice(THERMAL_VENDORS)
        amount, gst, qst = _rand_amount()
        date = _rand_date()
        lang = random.choice(["fr", "fr", "en"])  # mostly French
        severity = round(random.uniform(0.3, 0.9), 2)

        img = generate_base_receipt(vendor, amount, date, gst, qst, lang)
        img = add_thermal_fading(img, severity)

        _save(img, {
            "vendor": vendor, "amount": amount, "date": date,
            "gst": gst, "qst": qst,
            "distortion_type": "thermal_fading", "severity": severity,
            "language": lang,
        })

    # --- 50 phone photos (bad lighting + angle) ---
    for i in range(50):
        vendor = random.choice(PHONE_VENDORS)
        amount, gst, qst = _rand_amount()
        date = _rand_date()
        lang = random.choice(["en", "fr"])
        severity = round(random.uniform(0.3, 0.9), 2)

        img = generate_base_receipt(vendor, amount, date, gst, qst, lang)
        img = add_bad_lighting(img, severity)
        img = add_angle_distortion(img, max_degrees=15)

        _save(img, {
            "vendor": vendor, "amount": amount, "date": date,
            "gst": gst, "qst": qst,
            "distortion_type": "bad_lighting", "severity": severity,
            "language": lang,
        })

    # --- 50 crumpled damaged receipts ---
    for i in range(50):
        vendor = random.choice(CRUMPLE_VENDORS)
        amount, gst, qst = _rand_amount()
        date = _rand_date()
        lang = random.choice(["fr", "en"])
        severity = round(random.uniform(0.3, 0.9), 2)

        img = generate_base_receipt(vendor, amount, date, gst, qst, lang)
        img = add_crumple_effect(img, severity)
        img = add_bad_lighting(img, severity * 0.5)

        _save(img, {
            "vendor": vendor, "amount": amount, "date": date,
            "gst": gst, "qst": qst,
            "distortion_type": "crumpled", "severity": severity,
            "language": lang,
        })

    # --- 50 handwritten receipts ---
    for i in range(50):
        vendor = random.choice(HANDWRITTEN_VENDORS)
        amount, gst, qst = _rand_amount()
        date = _rand_date()
        lang = random.choice(["fr", "en"])
        severity = round(random.uniform(0.3, 0.7), 2)

        img = generate_handwritten_receipt(vendor, amount, date, lang)
        img = add_angle_distortion(img, max_degrees=10)

        _save(img, {
            "vendor": vendor, "amount": amount, "date": date,
            "gst": round(round(amount / 1.14975, 2) * 0.05, 2),
            "qst": round(round(amount / 1.14975, 2) * 0.09975, 2),
            "distortion_type": "handwritten", "severity": severity,
            "language": lang,
        })

    return manifests


if __name__ == "__main__":
    print(f"Generating 200 messy receipt images to {OUTPUT_DIR} ...")
    results = generate_all()
    print(f"Done – {len(results)} images + JSON sidecars written.")
