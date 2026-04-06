"""
Pydantic model for structured invoice extraction via Instructor.

Validates AI output so that storage sizes (100 GB), tariff codes (DEF, GD),
and other non-monetary values are rejected before they reach the database.
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, field_validator


class InvoiceExtraction(BaseModel):
    vendor_name: str = Field(description="Company name sending the invoice")
    amount: float = Field(description="Final total amount due in dollars")
    currency: str = Field(default="CAD", description="CAD or USD")
    document_date: Optional[str] = Field(None, description="YYYY-MM-DD format")
    invoice_number: Optional[str] = None
    gst_number: Optional[str] = None
    qst_number: Optional[str] = None
    gst_amount: Optional[float] = None
    qst_amount: Optional[float] = None
    gl_account: str = Field(description="4-digit GL account code")
    tax_code: str = Field(description="T, E, M, or Z")
    category: str = Field(default="operating_expense")
    document_type: str = Field(default="invoice")
    is_proration: bool = Field(default=False)

    @field_validator('amount')
    @classmethod
    def amount_must_be_reasonable(cls, v: float) -> float:
        if v <= 0:
            raise ValueError('Amount must be positive')
        if v > 1000000:
            raise ValueError('Amount too large - likely extraction error')
        return round(v, 2)

    @field_validator('vendor_name')
    @classmethod
    def vendor_must_be_company(cls, v: str) -> str:
        skip = ['invoice', 'receipt', 'statement', 'summary', 'billing', 'def', 'gd']
        if v.lower().strip() in skip:
            raise ValueError(f'"{v}" is not a valid vendor name')
        if len(v) <= 2:
            raise ValueError('Vendor name too short')
        return v.strip()

    @field_validator('tax_code')
    @classmethod
    def valid_tax_code(cls, v: str) -> str:
        if v not in ['T', 'E', 'M', 'Z']:
            return 'T'
        return v

    @field_validator('gl_account')
    @classmethod
    def valid_gl_account(cls, v: str) -> str:
        valid = ['5400', '5410', '5420', '5430', '5440', '5450',
                 '5500', '5640', '5650']
        if str(v) not in valid:
            return '5440'
        return str(v)
