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

def extract_text_with_docai(file_path: Path) -> str:
    """Extract raw text from document using Google Document AI."""
    result = process_with_docai(file_path)
    return result.get('raw_text', '')
