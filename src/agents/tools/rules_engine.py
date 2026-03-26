from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class RulesResult:
    doc_type: Optional[str]
    confidence: float
    vendor_name: Optional[str]
    total: Optional[float]
    document_date: Optional[str]
    currency: Optional[str]
    notes: str


class RulesEngine:
    def __init__(self, rules_dir: Path):
        self.rules_dir = rules_dir
        self.vendors = self._load_vendors()

    def _load_vendors(self) -> list[dict]:
        f = self.rules_dir / "vendors.json"
        if not f.exists():
            return []

        data = json.loads(f.read_text(encoding="utf-8"))
        vendors = data.get("vendors", [])
        if not isinstance(vendors, list):
            return []
        return vendors

    def _normalize_text(self, value: str) -> str:
        if value is None:
            return ""

        s = str(value)
        s = s.replace("\u00A0", " ")
        s = s.replace("\u2009", " ")
        s = s.replace("\u202F", " ")
        s = s.replace("\ufeff", "")
        s = s.replace("\x00", "")
        s = s.replace("–", "-")
        s = s.replace("—", "-")
        s = s.replace("−", "-")
        s = s.replace("’", "'")
        s = s.replace("“", '"')
        s = s.replace("”", '"')
        return s

    def _safe_group_value(self, match: re.Match) -> Optional[str]:
        if match is None:
            return None

        if match.lastindex:
            for idx in range(match.lastindex, 0, -1):
                value = match.group(idx)
                if value is not None and str(value).strip():
                    return str(value).strip()

        value = match.group(0)
        if value is None:
            return None
        return str(value).strip()

    def _parse_amount(self, value: str) -> Optional[float]:
        if value is None:
            return None

        s = self._normalize_text(str(value)).strip()

        if not s:
            return None

        s = s.replace("CAD", "")
        s = s.replace("USD", "")
        s = s.replace("EUR", "")
        s = s.replace("$", "")
        s = s.replace("€", "")
        s = s.replace("£", "")
        s = s.replace("(", "-").replace(")", "")
        s = s.replace(" ", "")

        # Keep only digits, separators, and minus
        s = re.sub(r"[^0-9,.\-]", "", s)

        if not s or s in {"-", ".", ","}:
            return None

        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "")
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            comma_parts = s.split(",")
            if len(comma_parts) == 2 and len(comma_parts[1]) == 2:
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "." in s:
            dot_parts = s.split(".")
            if len(dot_parts) > 2:
                last = dot_parts[-1]
                if len(last) == 2:
                    s = "".join(dot_parts[:-1]) + "." + last
                else:
                    s = "".join(dot_parts)

        try:
            return float(s)
        except Exception:
            return None

    def _month_to_number(self, month_name: str) -> Optional[str]:
        if month_name is None:
            return None

        month_map = {
            "january": "01",
            "jan": "01",
            "february": "02",
            "feb": "02",
            "march": "03",
            "mar": "03",
            "april": "04",
            "apr": "04",
            "may": "05",
            "june": "06",
            "jun": "06",
            "july": "07",
            "jul": "07",
            "august": "08",
            "aug": "08",
            "september": "09",
            "sep": "09",
            "sept": "09",
            "october": "10",
            "oct": "10",
            "november": "11",
            "nov": "11",
            "december": "12",
            "dec": "12",
            "janvier": "01",
            "janv": "01",
            "février": "02",
            "fevrier": "02",
            "févr": "02",
            "fevr": "02",
            "mars": "03",
            "avril": "04",
            "avr": "04",
            "mai": "05",
            "juin": "06",
            "juillet": "07",
            "juil": "07",
            "août": "08",
            "aout": "08",
            "septembre": "09",
            "octobre": "10",
            "novembre": "11",
            "décembre": "12",
            "decembre": "12",
            "déc": "12",
        }

        key = self._normalize_text(month_name).strip().lower().rstrip(".")
        return month_map.get(key)

    def _iso_date(self, year: int, month: int, day: int) -> Optional[str]:
        if year < 2000 or year > 2100:
            return None
        if month < 1 or month > 12:
            return None
        if day < 1 or day > 31:
            return None
        return f"{year:04d}-{month:02d}-{day:02d}"

    def _parse_named_month_date(self, value: str) -> Optional[str]:
        if value is None:
            return None

        s = self._normalize_text(value).strip()

        # 04 February 2026 / 4 février 2026
        m = re.search(
            r"\b(\d{1,2})\s+([A-Za-zÀ-ÿ]{3,20})\.?,?\s+(20\d{2})\b",
            s,
            re.IGNORECASE,
        )
        if m:
            day = int(m.group(1))
            month = self._month_to_number(m.group(2))
            year = int(m.group(3))
            if month:
                return self._iso_date(year, int(month), day)

        # February 4, 2026 / février 4 2026
        m = re.search(
            r"\b([A-Za-zÀ-ÿ]{3,20})\.?\s+(\d{1,2}),?\s+(20\d{2})\b",
            s,
            re.IGNORECASE,
        )
        if m:
            month = self._month_to_number(m.group(1))
            day = int(m.group(2))
            year = int(m.group(3))
            if month:
                return self._iso_date(year, int(month), day)

        return None

    def _parse_numeric_date(self, value: str) -> Optional[str]:
        if value is None:
            return None

        s = self._normalize_text(value).strip()

        # yyyy-mm-dd or yyyy/mm/dd
        m = re.search(r"\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b", s)
        if m:
            year = int(m.group(1))
            month = int(m.group(2))
            day = int(m.group(3))
            return self._iso_date(year, month, day)

        # dd/mm/yyyy or dd-mm-yyyy
        m = re.search(r"\b(\d{1,2})[-/](\d{1,2})[-/](20\d{2})\b", s)
        if m:
            first = int(m.group(1))
            second = int(m.group(2))
            year = int(m.group(3))

            # Default to day/month/year. This was the bug before.
            day = first
            month = second

            # If obviously US-style mm/dd/yyyy, flip it.
            if first <= 12 and second > 12:
                month = first
                day = second

            return self._iso_date(year, month, day)

        return None

    def _parse_date_value(self, value: str) -> Optional[str]:
        if value is None:
            return None

        s = self._normalize_text(value).strip()
        if not s:
            return None

        named = self._parse_named_month_date(s)
        if named:
            return named

        numeric = self._parse_numeric_date(s)
        if numeric:
            return numeric

        return None

    def _find_date(self, text: str) -> Optional[str]:
        if not text:
            return None

        s = self._normalize_text(text)

        patterns = [
            r"\b(20\d{2}[-/]\d{1,2}[-/]\d{1,2})\b",
            r"\b(\d{1,2}[-/]\d{1,2}[-/](20\d{2}))\b",
            r"\b(\d{1,2}\s+[A-Za-zÀ-ÿ]{3,20}\.?,?\s+(20\d{2}))\b",
            r"\b([A-Za-zÀ-ÿ]{3,20}\.?\s+\d{1,2},?\s+(20\d{2}))\b",
        ]

        for pattern in patterns:
            m = re.search(pattern, s, re.IGNORECASE)
            if not m:
                continue

            value = self._safe_group_value(m)
            parsed = self._parse_date_value(value)
            if parsed:
                return parsed

        return None

    def _pick_likely_total(self, text: str) -> Optional[float]:
        if not text:
            return None

        s = self._normalize_text(text)

        labeled_patterns = [
            r"total\s*(?:amount)?\s*(?:due|payable|paid)?\s*[:\-]?\s*(?:cad|usd)?\s*\$?\s*(-?\d{1,3}(?:[ ,]\d{3})*(?:[.,]\d{2})?)",
            r"amount\s*due\s*[:\-]?\s*(?:cad|usd)?\s*\$?\s*(-?\d{1,3}(?:[ ,]\d{3})*(?:[.,]\d{2})?)",
            r"total\s*\(including\s*tax\)\s*[:\-]?\s*(?:cad|usd)?\s*\$?\s*(-?\d{1,3}(?:[ ,]\d{3})*(?:[.,]\d{2})?)",
            r"total\s*facture\s*[:\-]?\s*(?:cad|usd)?\s*\$?\s*(-?\d{1,3}(?:[ ,]\d{3})*(?:[.,]\d{2})?)",
            r"total\s*[:\-]?\s*(?:cad|usd)?\s*\$?\s*(-?\d{1,3}(?:[ ,]\d{3})*(?:[.,]\d{2})?)",
        ]

        for pattern in labeled_patterns:
            matches = re.findall(pattern, s, re.IGNORECASE)
            if not matches:
                continue

            for raw in reversed(matches):
                amt = self._parse_amount(raw)
                if amt is not None:
                    return amt

        raw_matches = re.findall(r"-?\d{1,3}(?:[ ,]\d{3})*(?:[.,]\d{2})", s)
        if not raw_matches:
            return None

        parsed = []
        for raw in raw_matches:
            amt = self._parse_amount(raw)
            if amt is not None:
                parsed.append(amt)

        if not parsed:
            return None

        return parsed[-1]

    def _extract_total_from_vendor_rule(self, text: str, total_regex: Optional[str]) -> Optional[float]:
        if not total_regex:
            return None

        try:
            m = re.search(total_regex, text, re.IGNORECASE | re.DOTALL)
        except re.error:
            return None

        if not m:
            return None

        raw_amount = self._safe_group_value(m)
        if raw_amount is None:
            return None

        return self._parse_amount(raw_amount)

    def _extract_date_from_vendor_rule(self, text: str, date_regex: Optional[str]) -> Optional[str]:
        if not date_regex:
            return None

        try:
            m = re.search(date_regex, text, re.IGNORECASE | re.DOTALL)
        except re.error:
            return None

        if not m:
            return None

        raw_date = self._safe_group_value(m)
        if raw_date is None:
            return None

        return self._parse_date_value(raw_date)

    def run(self, text: str) -> RulesResult:
        normalized_text = self._normalize_text(text)
        text_lower = normalized_text.lower()

        vendor_name = None
        doc_type = None
        currency = None
        confidence = 0.0
        total = None
        document_date = None
        notes: list[str] = []

        for vendor in self.vendors:
            patterns = vendor.get("patterns", [])
            if not isinstance(patterns, list):
                continue

            matched_patterns = []
            for pattern in patterns:
                if not pattern:
                    continue
                if str(pattern).lower() in text_lower:
                    matched_patterns.append(str(pattern))

            if not matched_patterns:
                continue

            vendor_name = vendor.get("vendor_name")
            doc_type = vendor.get("doc_type")
            currency = vendor.get("currency")
            confidence = float(vendor.get("min_confidence", 0.9))

            vendor_id = vendor.get("id") or "unknown_vendor"
            notes.append(f"vendor_match:{vendor_id}")
            notes.append(f"matched_patterns:{len(matched_patterns)}")

            total = self._extract_total_from_vendor_rule(
                normalized_text,
                vendor.get("total_regex"),
            )
            if total is None:
                total = self._pick_likely_total(normalized_text)

            document_date = self._extract_date_from_vendor_rule(
                normalized_text,
                vendor.get("date_regex"),
            )
            if document_date is None:
                document_date = self._find_date(normalized_text)

            return RulesResult(
                doc_type=doc_type,
                confidence=confidence,
                vendor_name=vendor_name,
                total=total,
                document_date=document_date,
                currency=currency,
                notes=";".join(notes),
            )

        total = self._pick_likely_total(normalized_text)
        document_date = self._find_date(normalized_text)

        return RulesResult(
            doc_type=None,
            confidence=0.4,
            vendor_name=None,
            total=total,
            document_date=document_date,
            currency=None,
            notes="no_vendor_match",
        )