"""
src/integrations/qr_generator.py
==================================
QR code generation for LedgerLink client portal upload links.

Functions
---------
generate_client_qr_png(client_code, client_name, portal_url) -> bytes
    Generates a QR code PNG. QR encodes the direct upload URL for that
    client. Client name printed below the code in LedgerLink brand blue.

generate_all_qr_pdf(clients_list, portal_base_url) -> bytes
    Generates a single PDF with one QR code per page — header, client name,
    QR code centred, portal URL, bilingual scan instructions.
"""
from __future__ import annotations

import io
import urllib.parse
from typing import Any

import qrcode
import qrcode.constants
from PIL import Image, ImageDraw, ImageFont
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas as rl_canvas

# LedgerLink brand blue
_BLUE_HEX = "#1F3864"
_BLUE_RGB = (31, 56, 100)

# Instruction text (bilingual)
_INSTRUCTIONS_FR = "Scannez pour soumettre vos documents"
_INSTRUCTIONS_EN = "Scan to submit your documents"


def _build_upload_url(portal_base_url: str, client_code: str) -> str:
    """Return the direct upload URL for a client."""
    base = portal_base_url.rstrip("/")
    encoded = urllib.parse.quote(client_code, safe="")
    return f"{base}/?client_code={encoded}"


def _load_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Try to load a TrueType font; fall back to default."""
    for name in ("arial.ttf", "Arial.ttf", "DejaVuSans.ttf", "FreeSans.ttf"):
        try:
            return ImageFont.truetype(name, size)
        except (OSError, IOError):
            pass
    return ImageFont.load_default()


def generate_client_qr_png(
    client_code: str,
    client_name: str,
    portal_url: str,
) -> bytes:
    """
    Generate a QR code PNG for a single client.

    The QR encodes *portal_url* (the direct upload URL for that client).
    The client name is printed below the QR in LedgerLink brand blue
    (#1F3864). Returns raw PNG bytes.
    """
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10,
        border=4,
    )
    qr.add_data(portal_url)
    qr.make(fit=True)

    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
    qr_w, qr_h = qr_img.size

    # Reserve space below for the client name label
    label_height = 56
    canvas_h = qr_h + label_height
    final_img = Image.new("RGB", (qr_w, canvas_h), "white")
    final_img.paste(qr_img, (0, 0))

    draw = ImageDraw.Draw(final_img)
    font = _load_font(20)

    label = client_name or client_code
    bbox = draw.textbbox((0, 0), label, font=font)
    text_w = bbox[2] - bbox[0]
    x = max(0, (qr_w - text_w) // 2)
    y = qr_h + 12
    draw.text((x, y), label, fill=_BLUE_RGB, font=font)

    buf = io.BytesIO()
    final_img.save(buf, format="PNG")
    return buf.getvalue()


def generate_all_qr_pdf(
    clients_list: list[dict[str, Any]],
    portal_base_url: str,
) -> bytes:
    """
    Generate a single PDF with one QR code per page.

    Each page contains:
      - LedgerLink AI header (brand blue)
      - Client name (large, centred)
      - QR code (centred)
      - Portal URL printed below the QR
      - Bilingual instructions: FR and EN

    *clients_list* is a list of dicts with keys ``client_code`` and
    ``client_name``.  *portal_base_url* is used to build each client's
    upload URL.

    Returns raw PDF bytes.
    """
    buf = io.BytesIO()
    page_w, page_h = letter  # 612 x 792 pts

    c = rl_canvas.Canvas(buf, pagesize=letter, pageCompression=0)
    blue_r, blue_g, blue_b = _BLUE_RGB[0] / 255, _BLUE_RGB[1] / 255, _BLUE_RGB[2] / 255

    for client in clients_list:
        code = client.get("client_code") or ""
        name = client.get("client_name") or code
        upload_url = _build_upload_url(portal_base_url, code)

        # ---- Header -------------------------------------------------------
        c.setFillColorRGB(blue_r, blue_g, blue_b)
        c.setFont("Helvetica-Bold", 22)
        c.drawCentredString(page_w / 2, page_h - 0.75 * inch, "LedgerLink AI")

        c.setFont("Helvetica", 11)
        c.setFillColorRGB(0.4, 0.4, 0.4)
        c.drawCentredString(page_w / 2, page_h - 1.05 * inch, "Client Document Portal")

        # Horizontal rule
        c.setStrokeColorRGB(blue_r, blue_g, blue_b)
        c.setLineWidth(1.5)
        c.line(0.75 * inch, page_h - 1.2 * inch, page_w - 0.75 * inch, page_h - 1.2 * inch)

        # ---- Client name --------------------------------------------------
        c.setFillColorRGB(blue_r, blue_g, blue_b)
        c.setFont("Helvetica-Bold", 18)
        c.drawCentredString(page_w / 2, page_h - 1.65 * inch, name)

        # ---- QR code image ------------------------------------------------
        qr = qrcode.QRCode(
            version=None,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=10,
            border=4,
        )
        qr.add_data(upload_url)
        qr.make(fit=True)
        qr_pil = qr.make_image(fill_color="black", back_color="white").convert("RGB")

        qr_size_pts = 3.2 * inch  # print size on page
        qr_x = (page_w - qr_size_pts) / 2
        qr_y = page_h - 1.65 * inch - 0.3 * inch - qr_size_pts

        # Save PIL image to a temp buffer and draw via reportlab
        tmp = io.BytesIO()
        qr_pil.save(tmp, format="PNG")
        tmp.seek(0)
        from reportlab.lib.utils import ImageReader
        c.drawImage(ImageReader(tmp), qr_x, qr_y, width=qr_size_pts, height=qr_size_pts)

        # ---- Portal URL ---------------------------------------------------
        url_y = qr_y - 0.35 * inch
        c.setFont("Helvetica", 9)
        c.setFillColorRGB(0.35, 0.35, 0.35)
        c.drawCentredString(page_w / 2, url_y, upload_url)

        # ---- Bilingual instructions ----------------------------------------
        instr_y = url_y - 0.38 * inch
        c.setFont("Helvetica-Bold", 11)
        c.setFillColorRGB(blue_r, blue_g, blue_b)
        c.drawCentredString(page_w / 2, instr_y,
                            f"{_INSTRUCTIONS_FR}  /  {_INSTRUCTIONS_EN}")

        c.showPage()

    c.save()
    buf.seek(0)
    return buf.read()
