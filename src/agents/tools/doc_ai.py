from __future__ import annotations
try:
    from openrouter_client import OpenRouterClient
except ImportError:
    from src.agents.tools.openrouter_client import OpenRouterClient

SYSTEM = """You are an accounting document classifier + extractor for a Canadian bookkeeping workflow.
Return STRICT JSON only.
If you are unsure, set confidence low and leave fields null.
Never invent numbers not present in the text."""

USER_TEMPLATE = """Classify and extract from this document text.

Province default: {province}
Language default: {language}

Return JSON with this schema:
{{
  "doc_type": "invoice|receipt|bank_statement|payroll|other|unknown",
  "confidence": 0.0-1.0,
  "vendor_name": string|null,
  "document_date": "YYYY-MM-DD"|null,
  "invoice_number": string|null,
  "currency": "CAD|USD|OTHER"|null,
  "subtotal": number|null,
  "tax_total": number|null,
  "total": number|null,
  "taxes": [
    {{"type":"GST|HST|QST|PST|OTHER","amount": number}}
  ],
  "notes": string
}}

DOCUMENT TEXT:
---
{text}
---
"""

def classify_and_extract(text: str, province: str = "QC", language: str = "EN") -> dict:
    client = OpenRouterClient()
    user = USER_TEMPLATE.format(text=text[:20000], province=province, language=language)
    return client.chat_json(system=SYSTEM, user=user, temperature=0.0)