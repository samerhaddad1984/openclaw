from google.cloud import documentai
import logging
import os
import re
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

PROJECT_ID = os.environ.get('GOOGLE_PROJECT_ID', 'otocpa')
LOCATION = 'us'
INVOICE_PROCESSOR_ID = os.environ.get('GOOGLE_INVOICE_PROCESSOR_ID', 'f8264dbed8c1558c')
EXPENSE_PROCESSOR_ID = os.environ.get('GOOGLE_EXPENSE_PROCESSOR_ID', 'c1829b346ab094a8')

def get_docai_client():
    return documentai.DocumentProcessorServiceClient()

def process_with_docai(file_path: Path, doc_type: str = 'invoice') -> dict:
    """
    Extract fields from document using Google Document AI.
    Returns dict with vendor, amount, date, tax fields etc.
    doc_type: 'invoice' or 'expense'
    """
    try:
        client = get_docai_client()

        processor_id = INVOICE_PROCESSOR_ID if doc_type == 'invoice' else EXPENSE_PROCESSOR_ID
        processor_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{processor_id}"

        # Read file
        with open(file_path, 'rb') as f:
            content = f.read()

        # Detect mime type
        suffix = file_path.suffix.lower()
        mime_types = {
            '.pdf': 'application/pdf',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.tiff': 'image/tiff',
            '.tif': 'image/tiff',
            '.webp': 'image/webp',
            '.heic': 'image/heic',
        }
        mime_type = mime_types.get(suffix, 'application/pdf')

        raw_document = documentai.RawDocument(
            content=content,
            mime_type=mime_type
        )

        request = documentai.ProcessRequest(
            name=processor_name,
            raw_document=raw_document
        )

        result = client.process_document(request=request)
        document = result.document

        # Extract fields from Document AI response
        extracted = {}

        for entity in document.entities:
            entity_type = entity.type_.lower().replace('-', '_').replace(' ', '_')
            value = entity.mention_text.strip() if entity.mention_text else ''
            confidence = entity.confidence

            # Map Document AI fields to OtoCPA fields.
            # receipt_date is what the DocAI *expense* processor returns;
            # invoice_date is what the invoice processor returns. Both map
            # to our single `document_date` column. Before this fix the
            # expense processor's date was silently dropped (no mapping),
            # which pushed date extraction onto the much noisier regex
            # path in parse_invoice_fields — SKU codes like `23-33-53`
            # would pattern-match and win over the real date.
            field_map = {
                'supplier_name': 'vendor_name',
                'vendor_name': 'vendor_name',
                'receiver_name': 'vendor_name',
                'total_amount': 'amount',
                'net_amount': 'subtotal',
                'total_tax_amount': 'tax_total',
                'invoice_date': 'document_date',
                'receipt_date': 'document_date',
                'due_date': 'due_date',
                'invoice_id': 'invoice_number',
                'purchase_order': 'po_number',
                'supplier_tax_id': 'gst_number',
                'tax_id': 'gst_number',
                'currency': 'currency',
                'description': 'description',
            }

            mapped_field = field_map.get(entity_type, entity_type)

            if value and mapped_field not in extracted:
                extracted[mapped_field] = value

        # Get full text for fallback
        extracted['raw_text'] = document.text
        extracted['docai_used'] = True
        extracted['docai_confidence'] = sum(e.confidence for e in document.entities) / max(len(document.entities), 1)

        return extracted

    except Exception as e:
        import logging
        logging.warning(f'Google Document AI failed for {file_path}: {e}')
        return {}

def extract_line_items_from_docai(file_path):
    """Extract line items from a receipt/invoice using Google DocAI Expense Parser.

    Returns a list of dicts with keys: description, quantity, unit_price, total_price.
    Returns [] on any failure so the caller can fall back to text-based extraction.
    """
    try:
        import logging
        import pathlib

        client = get_docai_client()

        suffix = pathlib.Path(str(file_path)).suffix.lower()
        is_image = suffix in ['.jpg', '.jpeg', '.png', '.heic', '.webp', '.tiff']
        processor_id = EXPENSE_PROCESSOR_ID if is_image else INVOICE_PROCESSOR_ID
        processor_name = f'projects/{PROJECT_ID}/locations/{LOCATION}/processors/{processor_id}'

        mime_map = {
            '.pdf': 'application/pdf',
            '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
            '.png': 'image/png', '.tiff': 'image/tiff',
            '.heic': 'image/heic', '.webp': 'image/webp',
        }
        mime_type = mime_map.get(suffix, 'image/jpeg')

        with open(str(file_path), 'rb') as f:
            content = f.read()

        raw_document = documentai.RawDocument(content=content, mime_type=mime_type)
        request = documentai.ProcessRequest(name=processor_name, raw_document=raw_document)
        result = client.process_document(request=request)

        # Parse each line_item entity into a structured record
        raw_entities = []
        for entity in result.document.entities:
            if entity.type_ != 'line_item':
                continue

            descriptions = []
            amounts = []
            quantities = []
            unit_prices = []

            for prop in entity.properties:
                ptype = prop.type_
                value = prop.mention_text.strip()

                if ptype == 'line_item/description':
                    descriptions.append(value)
                elif ptype == 'line_item/amount':
                    try:
                        amounts.append(float(value.replace('$', '').replace(',', '.').strip()))
                    except Exception:
                        pass
                elif ptype == 'line_item/quantity':
                    try:
                        quantities.append(float(value.replace(',', '.')))
                    except Exception:
                        pass
                elif ptype == 'line_item/unit_price':
                    try:
                        unit_prices.append(float(value.replace('$', '').replace(',', '.').strip()))
                    except Exception:
                        pass

            raw_entities.append({
                'descriptions': descriptions,
                'amounts': amounts,
                'quantities': quantities,
                'unit_prices': unit_prices,
            })

        line_items = []

        # Two-pass approach:
        # Pass 1 — extract complete entities and grouped (item+discount)
        # Pass 2 — pair consecutive desc-only + amount-only fragments
        pending_desc = None  # description-only entity waiting for an amount
        pending_qty = None   # quantity-only entity waiting for pairing

        for ent in raw_entities:
            descriptions = ent['descriptions']
            amounts = ent['amounts']
            quantities = ent['quantities']
            unit_prices = ent['unit_prices']

            has_desc = bool(descriptions)
            has_amt = bool(amounts)
            has_qty_only = bool(quantities) and not has_desc and not has_amt

            # Grouped item+discount in same entity (2 descriptions)
            if len(descriptions) == 2 and has_amt:
                pending_desc = None
                pending_qty = None
                item_amount = amounts[0]
                line_items.append({
                    'description': descriptions[0],
                    'quantity': quantities[0] if quantities else 1.0,
                    'unit_price': unit_prices[0] if unit_prices else item_amount,
                    'total_price': item_amount,
                })
                if len(amounts) > 1:
                    line_items.append({
                        'description': descriptions[1],
                        'quantity': 1.0,
                        'unit_price': -amounts[1],
                        'total_price': -amounts[1],
                    })

            # Complete entity (description + amount)
            elif has_desc and has_amt:
                pending_desc = None
                pending_qty = None
                line_items.append({
                    'description': descriptions[0],
                    'quantity': quantities[0] if quantities else 1.0,
                    'unit_price': unit_prices[0] if unit_prices else amounts[0],
                    'total_price': amounts[0],
                })

            # Description only — hold for pairing with next amount-only entity
            elif has_desc and not has_amt:
                pending_desc = {
                    'description': descriptions[0],
                    'quantity': pending_qty or (quantities[0] if quantities else 1.0),
                }
                pending_qty = None

            # Amount only — pair with pending description if available
            elif has_amt and not has_desc:
                if pending_desc is not None:
                    line_items.append({
                        'description': pending_desc['description'],
                        'quantity': pending_desc.get('quantity', 1.0),
                        'unit_price': unit_prices[0] if unit_prices else amounts[0],
                        'total_price': amounts[0],
                    })
                    pending_desc = None
                    pending_qty = None
                # else: orphan amount — skip

            # Quantity only — remember for next entity
            elif has_qty_only:
                pending_qty = quantities[0]

        # Deduplicate exact matches
        seen: set[tuple[str, float]] = set()
        unique_items = []
        for item in line_items:
            key = (item['description'], item['total_price'])
            if key not in seen:
                seen.add(key)
                unique_items.append(item)

        logging.info(f'DocAI extracted {len(unique_items)} unique line items')
        return unique_items

    except Exception as e:
        import logging
        logging.warning(f'DocAI line item extraction failed for {file_path}: {e}')
        return []


def extract_words_with_bbox(file_path) -> list:
    """Extract OCR lines with bounding boxes from Google DocAI.

    Uses DocAI's line-level segmentation (not raw tokens) for clean text.
    Returns list of OCRWord objects (one per DocAI line) for use with
    ReceiptSpatialEngine.  Returns [] on any failure.
    """
    try:
        import pathlib

        client = get_docai_client()
        suffix = pathlib.Path(str(file_path)).suffix.lower()
        is_image = suffix in ['.jpg', '.jpeg', '.png', '.heic', '.webp', '.tiff']
        processor_id = EXPENSE_PROCESSOR_ID if is_image else INVOICE_PROCESSOR_ID
        processor_name = f'projects/{PROJECT_ID}/locations/{LOCATION}/processors/{processor_id}'

        mime_map = {
            '.pdf': 'application/pdf',
            '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
            '.png': 'image/png', '.tiff': 'image/tiff',
            '.heic': 'image/heic', '.webp': 'image/webp',
        }
        mime_type = mime_map.get(suffix, 'image/jpeg')

        with open(str(file_path), 'rb') as f:
            content = f.read()

        raw_document = documentai.RawDocument(content=content, mime_type=mime_type)
        request = documentai.ProcessRequest(
            name=processor_name,
            raw_document=raw_document,
        )
        result = client.process_document(request=request)

        from src.engines.receipt_spatial_engine import OCRWord, BoundingBox

        words = []
        document = result.document

        for page in document.pages:
            for line in page.lines:
                layout = line.layout
                if not layout.bounding_poly.normalized_vertices:
                    continue

                verts = layout.bounding_poly.normalized_vertices
                x_coords = [v.x for v in verts]
                y_coords = [v.y for v in verts]

                segments = layout.text_anchor.text_segments
                start = segments[0].start_index if segments else 0
                end = segments[0].end_index if segments else 0
                text = document.text[start:end].strip()

                if not text:
                    continue

                words.append(OCRWord(
                    text=text,
                    bbox=BoundingBox(
                        x0=min(x_coords),
                        y0=min(y_coords),
                        x1=max(x_coords),
                        y1=max(y_coords),
                    ),
                    confidence=layout.confidence,
                ))

        return words

    except Exception as e:
        logging.warning(f'extract_words_with_bbox failed: {e}')
        return []


def extract_text_with_docai(file_path: Path) -> str:
    """Extract raw text from document using Google Document AI."""
    result = process_with_docai(file_path)
    return result.get('raw_text', '')


# ---------------------------------------------------------------------------
# OCR-text-based receipt line parser
# ---------------------------------------------------------------------------
# Quebec grocery receipts (IGA, Metro, Maxi, etc.) follow a consistent format
# where item name is on one line and price on the next.  DocAI entity
# extraction misses items and collapses duplicates; Claude invents
# descriptions.  Parsing the clean OCR text with regex is far more reliable.

# Lines to skip (totals, payment, header, tax lines)
_SKIP_WORDS: list[str] = [
    'sous-total', 'subtotal', 'total', 'tps', 'tvq', 'gst', 'qst',
    'visa', 'debit', 'credit', 'monnaie', 'merci', 'thank', 'tel',
    'scene', 'argent', 'mastercard', 'nombre', 'economies', 'economie',
    'valeur', 'servi par', 'numero carte', 'bienvenue', 'vive la bouffe',
    'taxe', 'media', 'média', 'rabais:', 'prix membre',
]

# "RABAIS SUR LE PRIX COURANT" lines show the per-item saving —
# they are NOT standalone discount line items.
_COURANT_RABAIS_RE = re.compile(
    r'RABAIS\s+SUR\s+LE\s+PRIX\s+COURANT', re.IGNORECASE,
)

# Price on its own line: "$2.49" or "-$1.00"
_PRICE_LINE_RE = re.compile(r'^-?\$(\d+\.\d{2})$')

# Weight/unit-price line: "0.275 kg @ $8.80 / kg"
_WEIGHT_LINE_RE = re.compile(r'^\d+\.\d+\s*kg\s*@', re.IGNORECASE)

# Section headers: "EPICERIE", "FRUITS/LEGUMES", "BOULANGERIE", etc.
_SECTION_HEADERS = frozenset({
    'epicerie', 'fruits/legumes', 'boulangerie', 'boucherie',
    'charcuterie', 'poissonnerie', 'produits laitiers', 'surgeles',
    'boissons', 'entretien', 'sante', 'beaute',
})


def parse_receipt_ocr_text(clean_text: str) -> list[dict]:
    """Parse receipt line items from clean OCR text.

    Returns list of dicts with keys:
        description, quantity, unit_price, total_price, tax_indicator
    """
    text_lines = clean_text.split('\n')
    items: list[dict] = []
    i = 0

    while i < len(text_lines):
        line = text_lines[i].strip()

        # Skip blank / star divider lines
        if not line or line.startswith('***') or line.startswith('==='):
            i += 1
            continue

        line_lower = line.lower()

        # Skip known non-item lines
        if any(s in line_lower for s in _SKIP_WORDS):
            i += 1
            continue

        # Skip section headers
        if line_lower.strip('/') in _SECTION_HEADERS:
            i += 1
            continue

        # Skip "RABAIS SUR LE PRIX COURANT" (per-item saving, not a line item)
        if _COURANT_RABAIS_RE.search(line):
            i += 1
            continue

        # Skip weight/unit-price detail lines
        if _WEIGHT_LINE_RE.match(line):
            i += 1
            continue

        # Skip lines that are just a price (orphan price without a preceding desc)
        if _PRICE_LINE_RE.match(line):
            i += 1
            continue

        # Check if next line is a price → this line is the description
        if i + 1 < len(text_lines):
            next_line = text_lines[i + 1].strip()
            price_m = _PRICE_LINE_RE.match(next_line)
            if price_m:
                desc = line
                amount = float(price_m.group(1))
                is_negative = next_line.startswith('-')

                # "RABAIS INSTANTANÉ" / "RABAIS ..." with a negative price
                # are real discount line items
                is_discount = is_negative or 'RABAIS' in desc.upper()

                if len(desc) >= 3:
                    items.append({
                        'description': desc,
                        'quantity': 1.0,
                        'unit_price': -amount if is_discount else amount,
                        'total_price': -amount if is_discount else amount,
                        'tax_indicator': None,
                    })
                i += 2
                continue

        # Also handle single-line format: "ITEM_NAME  $PRICE [TAX_CODE]"
        single_m = re.match(
            r'^(.+?)\s+\$?(\d+\.\d{2})\s*([DJNEFHZ])?\s*$', line,
        )
        if single_m:
            desc = single_m.group(1).strip()
            amount = float(single_m.group(2))
            tax_ind = single_m.group(3)
            if len(desc) >= 3 and not any(s in desc.lower() for s in _SKIP_WORDS):
                items.append({
                    'description': desc,
                    'quantity': 1.0,
                    'unit_price': amount,
                    'total_price': amount,
                    'tax_indicator': tax_ind,
                })
                i += 1
                continue

        i += 1

    logging.info(f'OCR text parser found {len(items)} line items')
    return items


def extract_line_items_from_ocr_text(file_path) -> list[dict]:
    """Get clean OCR text from DocAI then parse line items with regex.

    Returns list of dicts compatible with _process_docai_line_items.
    Falls back to [] on any error.
    """
    try:
        import pathlib
        fp = pathlib.Path(str(file_path))
        result = process_with_docai(fp)
        clean_text = result.get('raw_text', '')
        if not clean_text:
            return []
        return parse_receipt_ocr_text(clean_text)
    except Exception as e:
        logging.warning(f'OCR text line item extraction failed for {file_path}: {e}')
        return []
