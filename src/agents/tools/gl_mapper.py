from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class GLMapResult:
    gl_account: str
    tax_code: str
    source: str


class GLMapper:
    def __init__(self, rules_dir: Path):
        self.rules_dir = Path(rules_dir)
        self.config = self._load_config()

    def _load_config(self) -> dict:
        f = self.rules_dir / "gl_map.json"
        if not f.exists():
            return {"vendors": {}, "doc_types": {}, "default": {}}
        return json.loads(f.read_text(encoding="utf-8"))

    def map(self, vendor_name: Optional[str], doc_type: Optional[str]) -> GLMapResult:
        vendor_name = (vendor_name or "").strip()
        doc_type = (doc_type or "").strip()

        vendors = self.config.get("vendors", {}) or {}
        doc_types = self.config.get("doc_types", {}) or {}
        default = self.config.get("default", {}) or {}

        if vendor_name and vendor_name in vendors:
            v = vendors[vendor_name]
            return GLMapResult(
                gl_account=v.get("gl_account", "Uncategorized Expense"),
                tax_code=v.get("tax_code", "GST_QST"),
                source=f"vendor:{vendor_name}",
            )

        if doc_type and doc_type in doc_types:
            d = doc_types[doc_type]
            return GLMapResult(
                gl_account=d.get("gl_account", "Uncategorized Expense"),
                tax_code=d.get("tax_code", "GST_QST"),
                source=f"doc_type:{doc_type}",
            )

        return GLMapResult(
            gl_account=default.get("gl_account", "Uncategorized Expense"),
            tax_code=default.get("tax_code", "GST_QST"),
            source="default",
        )