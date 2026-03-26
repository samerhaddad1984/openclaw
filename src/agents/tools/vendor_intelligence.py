from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class VendorIntelResult:
    category: str
    gl_account: str
    tax_code: str
    source: str
    document_family: str


class VendorIntelligenceEngine:
    def __init__(self, rules_dir: Path):
        self.rules_dir = Path(rules_dir)
        self.config = self._load_config()

    def _load_config(self) -> dict:
        f = self.rules_dir / "vendor_intel.json"
        if not f.exists():
            return {"vendors": {}, "doc_type_defaults": {}, "default": {}}
        return json.loads(f.read_text(encoding="utf-8"))

    def classify(self, vendor_name: Optional[str], doc_type: Optional[str]) -> VendorIntelResult:
        vendor_name = (vendor_name or "").strip()
        doc_type = (doc_type or "").strip()

        vendors = self.config.get("vendors", {}) or {}
        doc_type_defaults = self.config.get("doc_type_defaults", {}) or {}
        default = self.config.get("default", {}) or {}

        if vendor_name and vendor_name in vendors:
            v = vendors[vendor_name]
            return VendorIntelResult(
                category=v.get("category", "Uncategorized"),
                gl_account=v.get("gl_account", "Uncategorized Expense"),
                tax_code=v.get("tax_code", "GST_QST"),
                source=f"vendor:{vendor_name}",
                document_family=v.get("document_family", "unknown"),
            )

        if doc_type and doc_type in doc_type_defaults:
            d = doc_type_defaults[doc_type]
            return VendorIntelResult(
                category=d.get("category", "Uncategorized"),
                gl_account=d.get("gl_account", "Uncategorized Expense"),
                tax_code=d.get("tax_code", "GST_QST"),
                source=f"doc_type:{doc_type}",
                document_family=doc_type,
            )

        return VendorIntelResult(
            category=default.get("category", "Uncategorized"),
            gl_account=default.get("gl_account", "Uncategorized Expense"),
            tax_code=default.get("tax_code", "GST_QST"),
            source="default",
            document_family="unknown",
        )