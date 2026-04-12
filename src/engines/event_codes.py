"""
src/engines/event_codes.py — Complete event code catalog for receipt processing.

Each event code defines a structured event type with severity, description,
and expected JSON shape for details. Used by the shadow log to produce
calibration-ready processing traces.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import logging


@dataclass
class EventCode:
    code: str
    event_type: str   # BLOCKER, WARNING, INFO
    severity: int     # 1-10
    description: str
    json_shape: Dict[str, Any]


EVENT_CATALOG: dict[str, EventCode] = {

    # HARD BLOCKERS (severity 9-10)
    'RECONCILIATION_FAILED': EventCode(
        code='RECONCILIATION_FAILED',
        event_type='BLOCKER',
        severity=10,
        description='Line item sum does not match receipt total beyond tolerance',
        json_shape={'line_sum': 0.0, 'receipt_total': 0.0, 'delta': 0.0, 'tolerance': 0.03, 'line_count': 0},
    ),
    'WEIGHTED_ITEM_VOID': EventCode(
        code='WEIGHTED_ITEM_VOID',
        event_type='BLOCKER',
        severity=9,
        description='Weight found but no unit price to calculate total',
        json_shape={'description': '', 'weight_found': 0.0, 'unit_price_found': None, 'line_index': 0},
    ),
    'ORPHAN_DISCOUNT': EventCode(
        code='ORPHAN_DISCOUNT',
        event_type='BLOCKER',
        severity=9,
        description='Discount line found with no parent item',
        json_shape={'description': '', 'amount': 0.0, 'line_index': 0, 'nearest_item': ''},
    ),
    'TAX_UNKNOWN': EventCode(
        code='TAX_UNKNOWN',
        event_type='BLOCKER',
        severity=9,
        description='Cannot determine tax code for item',
        json_shape={'description': '', 'line_index': 0, 'vendor': ''},
    ),
    'MULTI_HYPOTHESIS_TIE': EventCode(
        code='MULTI_HYPOTHESIS_TIE',
        event_type='BLOCKER',
        severity=9,
        description='Two interpretations equally likely - cannot determine correct one',
        json_shape={'description': '', 'hypothesis_a': '', 'hypothesis_b': '', 'score_diff': 0.0},
    ),
    'OCR_EMPTY_OUTPUT': EventCode(
        code='OCR_EMPTY_OUTPUT',
        event_type='BLOCKER',
        severity=10,
        description='Google DocAI returned no words or text',
        json_shape={'file_path': '', 'file_size_bytes': 0},
    ),
    'AMOUNT_MISMATCH': EventCode(
        code='AMOUNT_MISMATCH',
        event_type='BLOCKER',
        severity=9,
        description='Extracted amount differs significantly from receipt total',
        json_shape={'extracted': 0.0, 'receipt_total': 0.0, 'delta': 0.0},
    ),

    # WARNINGS (severity 6-8)
    'MISSING_TAX_ANCHOR': EventCode(
        code='MISSING_TAX_ANCHOR',
        event_type='WARNING',
        severity=7,
        description='No TPS/TVQ block found but receipt appears taxable',
        json_shape={'vendor': '', 'total': 0.0, 'expected_tax': 0.0},
    ),
    'DUPLICATE_SUSPECT': EventCode(
        code='DUPLICATE_SUSPECT',
        event_type='WARNING',
        severity=7,
        description='Same vendor + date + amount seen recently',
        json_shape={'vendor': '', 'amount': 0.0, 'date': '', 'existing_doc_id': ''},
    ),
    'LOW_CONFIDENCE': EventCode(
        code='LOW_CONFIDENCE',
        event_type='WARNING',
        severity=6,
        description='Average OCR or spatial confidence below threshold',
        json_shape={'avg_confidence': 0.0, 'threshold': 0.6, 'item_count': 0},
    ),
    'MERCHANT_UNKNOWN': EventCode(
        code='MERCHANT_UNKNOWN',
        event_type='WARNING',
        severity=6,
        description='Vendor not in known merchant list',
        json_shape={'vendor_guess': '', 'ocr_text_sample': ''},
    ),
    'DATE_MISSING': EventCode(
        code='DATE_MISSING',
        event_type='WARNING',
        severity=8,
        description='No document date could be extracted',
        json_shape={'vendor': '', 'amount': 0.0},
    ),
    'PERSONAL_ITEM_DETECTED': EventCode(
        code='PERSONAL_ITEM_DETECTED',
        event_type='WARNING',
        severity=7,
        description='Clothing or personal items found - verify business purpose',
        json_shape={'items_found': [], 'vendor': ''},
    ),
    'ALCOHOL_DETECTED': EventCode(
        code='ALCOHOL_DETECTED',
        event_type='WARNING',
        severity=7,
        description='Alcohol items require business purpose verification',
        json_shape={'items_found': [], 'total_alcohol_amount': 0.0},
    ),
    'CAPITAL_ASSET_DETECTED': EventCode(
        code='CAPITAL_ASSET_DETECTED',
        event_type='WARNING',
        severity=8,
        description='Item over $500 requires CCA class assignment',
        json_shape={'description': '', 'amount': 0.0, 'suggested_gl': '1820'},
    ),

    # INFO (severity 1-5)
    'DOCAI_FALLBACK': EventCode(
        code='DOCAI_FALLBACK',
        event_type='INFO',
        severity=3,
        description='Spatial engine failed, used DocAI entity extraction',
        json_shape={'reason': ''},
    ),
    'TEXT_FALLBACK': EventCode(
        code='TEXT_FALLBACK',
        event_type='INFO',
        severity=3,
        description='DocAI failed, used Claude text extraction as fallback',
        json_shape={'reason': ''},
    ),
    'MERCHANT_IDENTIFIED': EventCode(
        code='MERCHANT_IDENTIFIED',
        event_type='INFO',
        severity=2,
        description='Merchant fingerprint matched known profile',
        json_shape={'merchant': '', 'confidence': 0.0},
    ),
    'ZERO_RATED_APPLIED': EventCode(
        code='ZERO_RATED_APPLIED',
        event_type='INFO',
        severity=1,
        description='Grocery items correctly marked zero-rated',
        json_shape={'item_count': 0, 'items': []},
    ),
    'DISCOUNT_ATTACHED': EventCode(
        code='DISCOUNT_ATTACHED',
        event_type='INFO',
        severity=1,
        description='Discount correctly linked to parent item',
        json_shape={'parent': '', 'discount': 0.0},
    ),
    'SPATIAL_ENGINE_USED': EventCode(
        code='SPATIAL_ENGINE_USED',
        event_type='INFO',
        severity=2,
        description='Adaptive spatial grouping engine used successfully',
        json_shape={'word_count': 0, 'line_count': 0, 'item_count': 0, 'avg_confidence': 0.0},
    ),
}


def get_event(code: str) -> Optional[EventCode]:
    """Look up an event code definition."""
    return EVENT_CATALOG.get(code)


def log_event(shadow_log, code: str, details: Dict[str, Any],
              line_id: str = None, message: str = None):
    """Log a cataloged event to a ShadowLog instance.

    Resolves event_type and severity from the catalog so callers only
    need to supply the code and details dict.
    """
    event = EVENT_CATALOG.get(code)
    if not event:
        logging.warning(f'Unknown event code: {code}')
        return

    shadow_log.log_event(
        event_code=code,
        message=message or event.description,
        event_type=event.event_type,
        severity=event.severity,
        line_id=line_id,
        details=details,
    )
