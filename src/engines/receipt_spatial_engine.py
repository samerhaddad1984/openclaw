"""
src/engines/receipt_spatial_engine.py — Adaptive Spatial Grouping Engine.

Production-quality receipt line item extraction using OCR word bounding boxes.
Groups words into lines by vertical proximity, then classifies each line
(item, discount, subtotal, tax, etc.) and builds parent-child structures
(e.g. item + discount).

Approach used by Dext/AutoEntry/Hubdoc: spatial grouping of OCR tokens
rather than relying on entity extraction or regex on plain text.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BoundingBox:
    """Normalized bounding box (0..1 coordinates)."""
    x0: float
    y0: float
    x1: float
    y1: float

    @property
    def height(self) -> float:
        return self.y1 - self.y0

    @property
    def width(self) -> float:
        return self.x1 - self.x0

    @property
    def mid_y(self) -> float:
        return (self.y0 + self.y1) / 2

    @property
    def mid_x(self) -> float:
        return (self.x0 + self.x1) / 2


@dataclass
class OCRWord:
    """Single word/token from OCR with bounding box."""
    text: str
    bbox: BoundingBox
    confidence: float = 1.0


@dataclass
class ReceiptLine:
    """A horizontal line of words grouped by spatial proximity."""
    words: list[OCRWord] = field(default_factory=list)
    raw_text: str = ""
    line_type: str = "UNKNOWN"  # ITEM, DISCOUNT, SUBTOTAL, TAX, TOTAL, HEADER, SKIP
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def bbox(self) -> BoundingBox | None:
        if not self.words:
            return None
        return BoundingBox(
            x0=min(w.bbox.x0 for w in self.words),
            y0=min(w.bbox.y0 for w in self.words),
            x1=max(w.bbox.x1 for w in self.words),
            y1=max(w.bbox.y1 for w in self.words),
        )

    @property
    def mid_y(self) -> float:
        bb = self.bbox
        return bb.mid_y if bb else 0.0


@dataclass
class LineStructure:
    """Parent-child relationship (e.g. item + discount child)."""
    structure_type: str  # ITEM_CANDIDATE, SUBTOTAL_GROUP, etc.
    parent_line: ReceiptLine
    child_lines: list[ReceiptLine] = field(default_factory=list)
    confidence: float = 1.0


@dataclass
class ParsedReceipt:
    """Full parsed receipt result."""
    lines: list[ReceiptLine] = field(default_factory=list)
    structures: list[LineStructure] = field(default_factory=list)
    merchant: str = ""
    subtotal: float | None = None
    tax_total: float | None = None
    grand_total: float | None = None


# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------

_PRICE_RE = re.compile(r'-?\$?\d+[.,]\d{2}')
# Accept bare-number price-only lines ("10.00") and common non-US currency
# prefixes ("RM 60.30", "MYR 60.30", "Rs 120,00", "€2,49"). Production Quebec
# receipts almost always prefix with `$`, but international receipts (SROIE,
# CORD) drop the `$`, which used to cause every bare-number line to be
# classified as an ITEM.
_PRICE_ONLY_RE = re.compile(
    r'^(?:\$|RM|MYR|Rp|IDR|Rs|€|£|¥|CAD|USD)?\s*-?\d+[.,]\d{2}$',
    re.IGNORECASE,
)
_WEIGHT_RE = re.compile(r'(\d+[.,]\d+)\s*(?:kg|lb|g)\b', re.IGNORECASE)
_AT_PRICE_RE = re.compile(r'@\s*\$?\s*(\d+[.,]\d{2})', re.IGNORECASE)
_QTY_RE = re.compile(r'^(\d+)\s*[xX@]\s', re.IGNORECASE)

_DISCOUNT_KW = re.compile(
    r'rabais|discount|escompte|remise|economie|savings?|off\b|ristourne|'
    r'prix\s+membre|member\s+price|coupon|'
    # "@DISC 10.00%", "@DISC 5%" — a percentage discount modifier tacked
    # onto the line above it. The retail receipts in SROIE use this a lot.
    r'@?disc\b|\d+\s*%\s*(?:off|disc)',
    re.IGNORECASE,
)
_SUBTOTAL_KW = re.compile(
    r'sous[- ]?total|subtotal|sub\s*total|^sous\s+\$|'
    # OCR sometimes mangles "Subtotal" as "Sub 1 al" (the 't' kerning gets
    # read as a digit). Match "sub" followed by up to 6 non-letter chars
    # and "al" / "tal" — covers "subtotal", "sub-total", "sub 1 al".
    r'\bsub[^a-z]{0,6}t?al\b',
    re.IGNORECASE,
)
_TAX_KW = re.compile(
    r'\bTPS\b|\bTVQ\b|\bGST\b|\bQST\b|\bHST\b|\bPST\b|\btaxe?\b|'
    # Malaysian GST-code summary rows: "SR 6% 15.00 0.90", "ZR 0% 0.00",
    # "ZR/OS/EZ 0.00 0.00", "TAX AMT (S) 6% RM 13.30", "S 6% 60.50 3.63".
    r'^\s*(?:SR|ZR|GR|EZ|OS|DS|ES|RS|S|Z|T)\s*[@/\s]?\s*\d{1,2}\s*%|'
    # Allow the leading tax-code token to be followed by a slash-joined
    # chain of other codes before the amount ("ZR/OS/EZ 0.00").
    r'^\s*(?:SR|ZR|GR|EZ|OS|DS|ES|RS)(?:\s*/\s*[A-Z]{1,3})*\s*[\d.,-]|'
    r'\btax\s*amt\b|\btax\s*code\b',
    re.IGNORECASE,
)
_TOTAL_KW = re.compile(
    r'\btotal\b|montant\s+total|amount\s+due|balance\s+due',
    re.IGNORECASE,
)
_SKIP_KW = re.compile(
    r'visa|debit|credit|mastercard|merci|thank|bienvenue|welcome|'
    r'tel[:\s]|numero\s+carte|servi\s+par|nombre\s+d|scene|'
    r'argent|monnaie|change\b|cash\b|terminal|transaction|'
    r'approbation|approved|declined|'
    r'^\*+\s*[eéÉ]conomies|^rabais\s*:|prix\s+membre\s+scene|'
    r'^m[eéÉ]dia$|'
    # Rounding adjustment lines (Malaysian/Indonesian receipts love these
    # because local currencies round to 0.05). Never a line item. Pattern
    # tolerates common OCR corruption ("Rour ding", "Roun ding") where the
    # ascender on 'd' gets split by upstream OCR.
    r'\brou[a-z]*\s*ding?\b|\badjustment\b|'
    # Payment / service charge rows. "Payment: 50.00", "Service Charge",
    # "S/Charge", "Govt Tax", "Rounded" on retail receipts.
    r'\bpayment\s*:|service\s+charge|s/?charge|govt\s+tax|'
    # Fuel pump metadata on gas receipts: "35.10 litre Pump # 02",
    # "2.450 RM / litre". The line above (V-Power 97 RM 86.00) is the
    # real item; these siblings count as one item too.
    r'\bpump\s*#?|\blitre\b|\bgallon\b|'
    # Grand-total alias on Malaysian receipts ("CAL RM 73.00") and the
    # "No. Qtys / No. Items" summary row. Never a line item.
    r'^\s*cal\s+(?:rm|myr|\$)|no\.\s*qtys?|no\.\s*items?|'
    # Unit-price headers that DocAI sometimes emits as their own row
    # ("Qty S/Price Item 50.88 SR", "Description Qty Price Amount").
    r'^(?:qty\b|s/?price|description|item\b)',
    re.IGNORECASE,
)
_HEADER_KW = re.compile(
    r'caiss|store|magasin|succursale|branch|address|adresse|'
    r'date\s*:|heure|time\s*:|receipt|recu|facture|invoice\s*#|'
    # Street-address markers (SROIE receipts have Malaysian addresses as
    # their header rows; DocAI merges the shop name above them, leaving
    # "NO.53, JALAN SAGU 18" + a coincidental price-like token like
    # "55,57" to trip the ITEM classifier).
    r'\bjalan\b|\bjln\b|\btaman\b|\bno\.\s*\d',
    re.IGNORECASE,
)
_SECTION_HEADERS = frozenset({
    'epicerie', 'fruits/legumes', 'boulangerie', 'boucherie',
    'charcuterie', 'poissonnerie', 'produits laitiers', 'surgeles',
    'boissons', 'entretien', 'sante', 'beaute',
})

# "RABAIS SUR LE PRIX COURANT" is a per-item savings annotation, not a
# standalone discount — skip it.
_COURANT_RABAIS_RE = re.compile(
    r'RABAIS\s+SUR\s+LE\s+PRIX\s+COURANT', re.IGNORECASE,
)


def _parse_price(text: str) -> float | None:
    """Extract the last price from text."""
    matches = _PRICE_RE.findall(text)
    if not matches:
        return None
    raw = matches[-1].replace('$', '').replace(',', '.')
    try:
        return float(raw)
    except ValueError:
        return None


def _extract_prices(text: str) -> list[float]:
    """Extract all prices from text."""
    results = []
    for m in _PRICE_RE.finditer(text):
        raw = m.group().replace('$', '').replace(',', '.')
        try:
            results.append(float(raw))
        except ValueError:
            pass
    return results


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class ReceiptSpatialEngine:
    """Adaptive Spatial Grouping Engine for receipt parsing."""

    def __init__(self, line_merge_overlap: float = 0.5):
        # When two OCR lines overlap vertically by more than this fraction
        # of the shorter line's height, they are on the same receipt row
        # (e.g. description on left + price on right).
        self.line_merge_overlap = line_merge_overlap

    # -- public API ---------------------------------------------------------

    def parse(self, words: list[OCRWord]) -> ParsedReceipt:
        if not words:
            return ParsedReceipt()

        lines = self._group_into_lines(words)
        self._classify_lines(lines)
        merchant = self._detect_merchant(lines)
        structures = self._build_structures(lines)

        result = ParsedReceipt(
            lines=lines,
            structures=structures,
            merchant=merchant,
        )
        self._extract_totals(result)
        return result

    # -- step 1: spatial grouping -------------------------------------------

    def _group_into_lines(self, words: list[OCRWord]) -> list[ReceiptLine]:
        """Group DocAI segments into receipt rows by Y-coordinate proximity.

        DocAI often splits a single visual receipt row ("SUBTOTAL 60.31") into
        two separate text segments — the label on the left, the amount on the
        right. Treating each segment as its own line means the bare "60.31"
        gets classified as an ITEM instead of a SUBTOTAL. So we first merge
        segments whose bounding boxes overlap vertically by more than
        `line_merge_overlap` of the shorter segment's height, then sort each
        merged group left-to-right so the joined `raw_text` reads naturally.
        """
        if not words:
            return []

        sorted_words = sorted(words, key=lambda w: w.bbox.mid_y)

        rows: list[list[OCRWord]] = []
        for w in sorted_words:
            placed = False
            wh = max(w.bbox.height, 1e-6)
            for row in rows:
                # Compare vertical overlap against this row's anchor word.
                anchor = row[0]
                ah = max(anchor.bbox.height, 1e-6)
                y_overlap = min(w.bbox.y1, anchor.bbox.y1) - max(w.bbox.y0, anchor.bbox.y0)
                if y_overlap / min(wh, ah) >= self.line_merge_overlap:
                    row.append(w)
                    placed = True
                    break
            if not placed:
                rows.append([w])

        lines: list[ReceiptLine] = []
        for row in rows:
            ordered = sorted(row, key=lambda w: w.bbox.x0)
            merged_text = " ".join(w.text for w in ordered).strip()
            lines.append(ReceiptLine(words=ordered, raw_text=merged_text))

        lines.sort(key=lambda l: l.mid_y)
        return lines

    # -- step 2: classify lines ---------------------------------------------

    def _classify_lines(self, lines: list[ReceiptLine]) -> None:
        for line in lines:
            text = line.raw_text
            text_lower = text.lower().strip('/ ')

            # Section headers (also partial matches since OCR may merge words)
            stripped = text_lower.strip('/ ')
            if stripped in _SECTION_HEADERS or any(
                h in stripped for h in _SECTION_HEADERS
            ):
                line.line_type = 'SECTION'
                continue

            # "RABAIS SUR LE PRIX COURANT" — informational, not a discount
            if _COURANT_RABAIS_RE.search(text):
                line.line_type = 'SKIP'
                continue

            # Skip lines (payment, header noise)
            if _SKIP_KW.search(text):
                line.line_type = 'SKIP'
                continue
            if _HEADER_KW.search(text):
                line.line_type = 'HEADER'
                continue

            # Total (must check before subtotal)
            if _TOTAL_KW.search(text) and not _SUBTOTAL_KW.search(text):
                line.line_type = 'TOTAL'
                price = _parse_price(text)
                if price is not None:
                    line.metadata['total'] = price
                continue

            # Subtotal
            if _SUBTOTAL_KW.search(text):
                line.line_type = 'SUBTOTAL'
                price = _parse_price(text)
                if price is not None:
                    line.metadata['subtotal'] = price
                continue

            # Tax
            if _TAX_KW.search(text):
                line.line_type = 'TAX'
                price = _parse_price(text)
                if price is not None:
                    line.metadata['tax'] = price
                continue

            # Discount (but not RABAIS SUR LE PRIX COURANT, already handled above)
            if _DISCOUNT_KW.search(text):
                prices = _extract_prices(text)
                if prices:
                    line.line_type = 'DISCOUNT'
                    line.metadata['prices'] = prices
                    continue

            # Weight/unit-price detail line: "0.275 kg @ $8.80 / kg"
            wm = _WEIGHT_RE.search(text)
            am = _AT_PRICE_RE.search(text)
            if wm and am:
                line.line_type = 'WEIGHT_DETAIL'
                try:
                    line.metadata['weight'] = float(wm.group(1).replace(',', '.'))
                except ValueError:
                    pass
                try:
                    line.metadata['at_price'] = float(am.group(1).replace(',', '.'))
                except ValueError:
                    pass
                prices = _extract_prices(text)
                if prices:
                    line.metadata['prices'] = prices
                continue

            # Price-only line (e.g. "$2.49" on the right side of a receipt row)
            if _PRICE_ONLY_RE.match(text.strip()):
                prices = _extract_prices(text)
                line.line_type = 'PRICE_ONLY'
                line.metadata['prices'] = prices
                continue

            # Item detection: has a price
            prices = _extract_prices(text)
            if prices:
                line.line_type = 'ITEM'
                line.metadata['prices'] = prices

                # Weight detection
                wm = _WEIGHT_RE.search(text)
                if wm:
                    try:
                        line.metadata['weight'] = float(wm.group(1).replace(',', '.'))
                    except ValueError:
                        pass

                # @ price (unit price for weighted)
                am = _AT_PRICE_RE.search(text)
                if am:
                    try:
                        line.metadata['at_price'] = float(am.group(1).replace(',', '.'))
                    except ValueError:
                        pass

                # Quantity prefix: "2 x ITEM"
                qm = _QTY_RE.match(text)
                if qm:
                    try:
                        line.metadata['qty'] = float(qm.group(1))
                    except ValueError:
                        pass

                continue

            # No price — could be a description-only line or noise
            if len(text.strip()) >= 3:
                line.line_type = 'DESC_ONLY'
            else:
                line.line_type = 'SKIP'

    # -- step 3: merchant detection -----------------------------------------

    def _detect_merchant(self, lines: list[ReceiptLine]) -> str:
        """First non-skip line in the top 20% is likely the merchant."""
        if not lines:
            return ""
        # Top 20% by y-position
        all_y = [l.mid_y for l in lines if l.mid_y > 0]
        if not all_y:
            return ""
        y_threshold = min(all_y) + 0.2 * (max(all_y) - min(all_y))
        for line in lines:
            if line.mid_y <= y_threshold and line.line_type in ('HEADER', 'UNKNOWN', 'DESC_ONLY'):
                candidate = line.raw_text.strip()
                if len(candidate) >= 2 and not candidate.replace(' ', '').isdigit():
                    return candidate
        return ""

    # -- step 4: build parent-child structures ------------------------------

    def _build_structures(self, lines: list[ReceiptLine]) -> list[LineStructure]:
        """Build ITEM_CANDIDATE structures.

        Quebec grocery receipt pattern (DocAI lines):
          DESC_ONLY  "COMP OEUFS BL GROS"
          PRICE_ONLY "$2.49"
          SKIP       "RABAIS SUR LE PRIX COURANT: $1.60"

        Also handles:
          - DESC_ONLY + WEIGHT_DETAIL (weighed produce)
          - ITEM lines (desc + price on same OCR line)
          - DISCOUNT children attached to preceding item
        """
        structures: list[LineStructure] = []
        pending_desc: ReceiptLine | None = None
        i = 0
        while i < len(lines):
            line = lines[i]

            if line.line_type == 'ITEM':
                if pending_desc:
                    line.raw_text = pending_desc.raw_text + ' ' + line.raw_text
                    line.words = pending_desc.words + line.words
                    pending_desc = None

                struct = LineStructure(
                    structure_type='ITEM_CANDIDATE',
                    parent_line=line,
                    confidence=self._item_confidence(line),
                )
                i = self._consume_children(lines, i + 1, line, struct)
                structures.append(struct)
                continue

            if line.line_type == 'PRICE_ONLY':
                if pending_desc:
                    # DESC then PRICE
                    prices = line.metadata.get('prices', [])
                    pending_desc.metadata['prices'] = prices
                    pending_desc.line_type = 'ITEM'
                    struct = LineStructure(
                        structure_type='ITEM_CANDIDATE',
                        parent_line=pending_desc,
                        confidence=self._item_confidence(pending_desc),
                    )
                    pending_desc = None
                    i = self._consume_children(lines, i + 1, struct.parent_line, struct)
                    structures.append(struct)
                    continue
                # PRICE then DESC pattern (common on receipts where price is
                # slightly higher than description in y-coordinates)
                if i + 1 < len(lines) and lines[i + 1].line_type == 'DESC_ONLY':
                    desc_line = lines[i + 1]
                    prices = line.metadata.get('prices', [])
                    desc_line.metadata['prices'] = prices
                    desc_line.line_type = 'ITEM'
                    struct = LineStructure(
                        structure_type='ITEM_CANDIDATE',
                        parent_line=desc_line,
                        confidence=self._item_confidence(desc_line),
                    )
                    i = self._consume_children(lines, i + 2, struct.parent_line, struct)
                    structures.append(struct)
                    continue
                # Orphan price
                i += 1
                continue

            if line.line_type == 'WEIGHT_DETAIL':
                # Weight detail without a preceding price is consumed by
                # _consume_children of the previous item. If we reach here
                # with pending_desc but no price, the item's price is likely
                # on the PRICE_ONLY that was already consumed. Skip this.
                i += 1
                continue

            if line.line_type == 'DESC_ONLY':
                pending_desc = line
                i += 1
                continue

            # Non-item line, reset pending
            pending_desc = None
            i += 1

        return structures

    def _consume_children(
        self, lines: list[ReceiptLine], j: int,
        parent: ReceiptLine, struct: LineStructure,
    ) -> int:
        """Look ahead from index j, attach discount/weight children.

        WEIGHT_DETAIL lines add weight/at_price metadata but do NOT
        override the parent's price (the total was already set by the
        PRICE_ONLY that paired with the description).
        SKIP lines (RABAIS SUR LE PRIX COURANT) are consumed silently.
        PRICE_ONLY lines are never consumed — they belong to the next item.
        """
        while j < len(lines):
            nxt = lines[j]
            if nxt.line_type == 'DISCOUNT':
                struct.child_lines.append(nxt)
                j += 1
            elif nxt.line_type == 'WEIGHT_DETAIL':
                if 'weight' in nxt.metadata:
                    parent.metadata['weight'] = nxt.metadata['weight']
                if 'at_price' in nxt.metadata:
                    parent.metadata['at_price'] = nxt.metadata['at_price']
                struct.child_lines.append(nxt)
                j += 1
            elif nxt.line_type == 'SKIP':
                j += 1
            else:
                break
        return j

    def _item_confidence(self, line: ReceiptLine) -> float:
        """Heuristic confidence score for an item line."""
        score = 0.5
        prices = line.metadata.get('prices', [])
        if prices:
            score += 0.2
        # Text length — very short or very long is less likely a real item
        text_len = len(line.raw_text.strip())
        if 3 <= text_len <= 60:
            score += 0.2
        # Word confidence
        if line.words:
            avg_conf = sum(w.confidence for w in line.words) / len(line.words)
            score += 0.1 * avg_conf
        return min(score, 1.0)

    # -- step 5: extract totals ---------------------------------------------

    def _extract_totals(self, receipt: ParsedReceipt) -> None:
        for line in receipt.lines:
            if line.line_type == 'SUBTOTAL' and 'subtotal' in line.metadata:
                receipt.subtotal = line.metadata['subtotal']
            elif line.line_type == 'TAX' and 'tax' in line.metadata:
                if receipt.tax_total is None:
                    receipt.tax_total = line.metadata['tax']
                else:
                    receipt.tax_total += line.metadata['tax']
            elif line.line_type == 'TOTAL' and 'total' in line.metadata:
                receipt.grand_total = line.metadata['total']
