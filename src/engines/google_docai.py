from google.cloud import documentai
import os
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

            # Map Document AI fields to OtoCPA fields
            field_map = {
                'supplier_name': 'vendor_name',
                'vendor_name': 'vendor_name',
                'receiver_name': 'vendor_name',
                'total_amount': 'amount',
                'net_amount': 'subtotal',
                'total_tax_amount': 'tax_total',
                'invoice_date': 'document_date',
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


def extract_text_with_docai(file_path: Path) -> str:
    """Extract raw text from document using Google Document AI."""
    result = process_with_docai(file_path)
    return result.get('raw_text', '')
