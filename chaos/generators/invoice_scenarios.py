"""Invoice-specific chaos scenarios (Sprint F+ Round 2).

Covers 22 invoice edge cases that prior chaos tracks under-tested:
multi-line invoices, credits, discounts, payment terms, POs, wire
transfers, multi-ship-to, backorder, FX, retainers, late fees, weekend
dates, backdating, tax-included pricing, per-line tax rates, shipping
insurance, handling fees, and rounding adjustments.

Scoring: all scenarios use the ``financial`` oracle category so they plug
into the existing reconciliation / trial-balance integrity checks. The
runner is expected to feed these through the document ingest + AR
pipelines.
"""
from __future__ import annotations

import random
from typing import Any


INVOICE_SCENARIOS: list[dict[str, Any]] = [
    {
        "subtype": "invoice_200_line_items",
        "difficulty": "hard",
        "description": "Invoice with 200 line items — stresses line_item_engine pagination",
        "severity": "medium",
        "line_count": 200,
        "subtotal": 15432.10,
        "total": 17744.73,
    },
    {
        "subtype": "invoice_multi_page_pdf",
        "difficulty": "hard",
        "description": "Multi-page invoice PDF — OCR must concatenate pages and dedupe headers",
        "severity": "medium",
        "pages": 4,
        "line_count": 48,
        "subtotal": 6200.00,
        "total": 7127.95,
    },
    {
        "subtype": "invoice_with_credit_lines",
        "difficulty": "normal",
        "description": "Invoice with negative (credit) line items mixed with positives",
        "severity": "high",
        "line_count": 6,
        "credits": 2,
        "subtotal": 450.00,
        "total": 517.39,
    },
    {
        "subtype": "invoice_subtotal_discount",
        "difficulty": "normal",
        "description": "Discount applied at subtotal level (percentage)",
        "severity": "medium",
        "line_count": 5,
        "discount_pct": 0.10,
        "subtotal": 1000.00,
        "discount": 100.00,
        "total": 1034.78,
    },
    {
        "subtype": "invoice_with_payment_terms",
        "difficulty": "easy",
        "description": "Invoice embeds net-30 payment terms; due_date must be computed",
        "severity": "low",
        "payment_terms": "net_30",
        "subtotal": 800.00,
        "total": 919.80,
    },
    {
        "subtype": "invoice_with_po_reference",
        "difficulty": "normal",
        "description": "Invoice references a purchase order by number — three-way match",
        "severity": "high",
        "po_number": "PO-2025-00123",
        "subtotal": 2500.00,
        "total": 2874.38,
    },
    {
        "subtype": "invoice_wire_transfer_details",
        "difficulty": "normal",
        "description": "Invoice contains bank wire details (IBAN/SWIFT) in the footer",
        "severity": "medium",
        "payment_method": "wire",
        "subtotal": 5000.00,
        "total": 5748.75,
    },
    {
        "subtype": "invoice_multi_ship_to",
        "difficulty": "hard",
        "description": "Invoice with 3 different ship-to addresses; must not double-count",
        "severity": "medium",
        "ship_to_count": 3,
        "subtotal": 900.00,
        "total": 1034.78,
    },
    {
        "subtype": "invoice_backorder_status",
        "difficulty": "normal",
        "description": "Some line items are flagged backordered; quantity shipped < quantity ordered",
        "severity": "medium",
        "line_count": 6,
        "backorder_count": 2,
        "subtotal": 670.00,
        "total": 770.35,
    },
    {
        "subtype": "invoice_usd_with_cad_conversion",
        "difficulty": "hard",
        "description": "USD invoice with CAD conversion note; posting must use CAD amount",
        "severity": "high",
        "currency": "USD",
        "fx_rate": 1.35,
        "subtotal_usd": 1000.00,
        "subtotal_cad": 1350.00,
        "total_cad": 1552.16,
    },
    {
        "subtype": "invoice_retainer_applied",
        "difficulty": "hard",
        "description": "Retainer deposit applied — prior credit reduces amount due",
        "severity": "high",
        "retainer_applied": 500.00,
        "subtotal": 2000.00,
        "total_before_retainer": 2299.50,
        "total": 1799.50,
    },
    {
        "subtype": "invoice_with_late_fees",
        "difficulty": "normal",
        "description": "Invoice includes a late-fee line for overdue prior invoice",
        "severity": "medium",
        "late_fee": 75.00,
        "subtotal": 475.00,
        "total": 546.25,
    },
    {
        "subtype": "invoice_dated_weekend",
        "difficulty": "easy",
        "description": "Invoice date falls on a Saturday — must not reject as invalid",
        "severity": "low",
        "document_date": "2025-03-15",  # Saturday
        "subtotal": 300.00,
        "total": 344.93,
    },
    {
        "subtype": "invoice_backdated_6_months",
        "difficulty": "hard",
        "description": "Invoice backdated 6 months — period-close logic must handle",
        "severity": "high",
        "document_date": "2024-10-15",
        "received_date": "2025-04-15",
        "subtotal": 1200.00,
        "total": 1379.70,
    },
    {
        "subtype": "invoice_tax_included_pricing",
        "difficulty": "hard",
        "description": "Line prices are tax-included; subtotal must be back-calculated",
        "severity": "high",
        "tax_included": True,
        "total": 1149.75,
        "subtotal": 1000.00,
        "gst": 50.00,
        "qst": 99.75,
    },
    {
        "subtype": "invoice_mixed_per_line_tax_rates",
        "difficulty": "nightmare",
        "description": "Each line has a different tax code (T / Z / E / M)",
        "severity": "high",
        "line_count": 8,
        # 3 T + 1 M taxable out of 8 lines @ $100 each.
        # Tax = 400 * (5% GST + 9.975% QST) = 59.90. Subtotal 800 + 59.90 = 859.90.
        "mixed_tax_codes": ["T", "Z", "E", "M", "T", "T", "Z", "E"],
        "subtotal": 800.00,
        "total": 859.90,
    },
    {
        "subtype": "invoice_shipping_insurance",
        "difficulty": "normal",
        "description": "Separate shipping insurance line item — optional, taxable",
        "severity": "low",
        "insurance_amount": 25.00,
        "subtotal": 525.00,
        "total": 603.61,
    },
    {
        "subtype": "invoice_handling_fee",
        "difficulty": "normal",
        "description": "Handling fee added as separate line — taxable",
        "severity": "low",
        "handling_fee": 15.00,
        "subtotal": 315.00,
        "total": 362.10,
    },
    {
        "subtype": "invoice_rounding_adjustment",
        "difficulty": "hard",
        "description": "Rounding adjustment line (sub-cent) to reconcile totals",
        "severity": "medium",
        "rounding_adjustment": 0.01,
        "subtotal": 333.33,
        "total": 383.25,
    },
    {
        "subtype": "invoice_installment_payment",
        "difficulty": "hard",
        "description": "Invoice payable in 3 installments — partial payments apply",
        "severity": "medium",
        "installments": 3,
        "subtotal": 3000.00,
        "total": 3449.25,
    },
    {
        "subtype": "invoice_client_billed_twice_same_period",
        "difficulty": "hard",
        "description": "Same client receives 2 separate invoices in one day — should not duplicate",
        "severity": "high",
        "invoice_count": 2,
        "subtotal_each": 500.00,
    },
    {
        "subtype": "invoice_very_long_description",
        "difficulty": "normal",
        "description": "Line item with 1000+ char description — UI/DB truncation stress",
        "severity": "low",
        "line_count": 3,
        "description_length": 1200,
        "subtotal": 450.00,
        "total": 517.39,
    },
]


def generate(rnd: random.Random) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for spec in INVOICE_SCENARIOS:
        out.append({
            "category": "invoice",
            "subtype": spec["subtype"],
            "difficulty": spec["difficulty"],
            "description": spec["description"],
            "severity_on_failure": spec.get("severity", "medium"),
            "expected_fail": False,
            "future_feature": False,
            "affects_engines": [
                "src.engines.invoice_schema",
                "src.engines.line_item_engine",
                "src.engines.tax_engine",
                "src.engines.multicurrency_engine",
                "src.engines.ar_engine",
            ],
            "oracle": "financial",
            "input_spec": {
                "kind": "invoice_synthetic",
                "spec": {k: v for k, v in spec.items()
                         if k not in ("subtype", "difficulty", "description", "severity")},
            },
            "ground_truth": {
                "subtype": spec["subtype"],
                **{k: v for k, v in spec.items()
                   if k in ("subtotal", "total", "gst", "qst",
                            "line_count", "currency", "fx_rate",
                            "tax_included", "discount", "discount_pct",
                            "retainer_applied", "late_fee", "rounding_adjustment",
                            "payment_terms", "po_number", "document_date",
                            "received_date", "mixed_tax_codes",
                            "subtotal_usd", "subtotal_cad", "total_cad",
                            "total_before_retainer", "insurance_amount",
                            "handling_fee", "invoice_count", "subtotal_each")},
            },
        })
    return out
