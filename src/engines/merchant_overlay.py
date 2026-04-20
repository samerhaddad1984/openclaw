"""Merchant-specific overlays for Canadian (especially Quebec) receipts.

Each overlay declares:
- ``VENDOR_PATTERNS``  — regexes that identify the merchant in raw OCR text.
- ``VENDOR_CANONICAL`` — the canonical brand name to emit.
- ``DEFAULT_GL_ACCOUNT`` / ``TAX_CODE_DEFAULT`` — sensible defaults for this
  merchant's typical transactions (CPA side can still override).
- ``MIXED_TAX_EXPECTED`` — True when the merchant mixes zero-rated and
  taxable items on the same receipt (grocery, pharmacy).
- ``TAXABLE_KEYWORDS`` / ``ZERO_RATED_KEYWORDS`` — French + English terms
  that help classify line tax when the receipt isn't explicit.

The ``apply_merchant_overlay`` and ``parse_costco_receipt`` module-level
functions are preserved for backwards compatibility with
``src/engines/line_item_engine.py``.
"""
from __future__ import annotations

import re
from typing import Any, Iterable

# ---------------------------------------------------------------------------
# Shared heuristics (Costco-specific; used by the legacy parser below)
# ---------------------------------------------------------------------------

SKIP_LINE_PATTERNS = [
    r'^sous-total',
    r'^total\s+rabais',
    r'^\*+\s*total',
    r'^taxe\s',
    r'^p\.?\s*t\.?v\.?q',
    r'^f\.?\s*t\.?p\.?s',
    r'^mastercard',
    r'^visa',
    r'^monnaie',
    r'^acct:',
    r'^auth\s*#',
    r'^reference\s*#',
    r'^invoice\s+number',
    r'^purchase\s*-',
    r'^approved',
    r'^amount:',
    r'^nombre\s+d',
    r'^merci',
    r'^customer\s+copy',
    r'^important',
    r'^caissier',
    r'^\d{4}/\d{2}/\d{2}',
    r'^uo\s+membre',
    r'^membre\s+\d',
    r'^entr:',
    r'^#oper',
    r'^#tps',
    r'^#tvq',
    r'^\d{13,}$',
    r'^libre-service',
    r'^wholesale$',
    r'^costco$',
]

_SKIP_RE = [re.compile(p, re.IGNORECASE) for p in SKIP_LINE_PATTERNS]
_TPD_RE = re.compile(r'\bTPD/(\d+)\b', re.IGNORECASE)
_BARCODE_DESC_RE = re.compile(r'^(\d{5,7})\s+(.+)$')
_AMOUNT_RE = re.compile(r'(\d+[.,]\d{2})(-)?')
_COSTCO_TAX_CODES = ('FP', 'P', 'F')


def _should_skip(text: str) -> bool:
    t = text.strip()
    if not t:
        return True
    return any(p.search(t) for p in _SKIP_RE)


def _parse_amount(text: str) -> tuple[float | None, bool]:
    m = _AMOUNT_RE.search(text)
    if not m:
        return None, False
    raw = m.group(1).replace(',', '.')
    is_discount = bool(m.group(2))
    try:
        return float(raw), is_discount
    except ValueError:
        return None, False


def _detect_tax_suffix(text: str) -> str | None:
    for code in _COSTCO_TAX_CODES:
        if re.search(rf'(?<![A-Z]){code}(?![A-Z])', text):
            return code
    return None


def parse_costco_receipt(raw_text: str) -> list[dict[str, Any]]:
    """Parse a Costco receipt directly from OCR text.

    Layout:
        BARCODE DESCRIPTION
        PRICE [TAX_CODE]

    Discount lines:
        BARCODE TPD/PARENT_BARCODE
        AMOUNT-
    """
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    items: list[dict[str, Any]] = []

    i = 0
    while i < len(lines):
        line = lines[i]

        if _should_skip(line):
            i += 1
            continue

        m = _BARCODE_DESC_RE.match(line)
        if not m:
            i += 1
            continue

        barcode = m.group(1)
        desc = m.group(2).strip()

        if i + 1 >= len(lines):
            break

        price_line = lines[i + 1]
        amount, is_discount = _parse_amount(price_line)
        if amount is None:
            i += 1
            continue

        tpd = _TPD_RE.search(desc)
        if tpd:
            parent_barcode = tpd.group(1)
            for item in items:
                if item.get('_barcode') == parent_barcode:
                    prev_discount = float(item.get('discount') or 0)
                    item['discount'] = prev_discount + amount
                    item['total_price'] = float(item['unit_price']) - item['discount']
                    break
            i += 2
            continue

        tax_suffix = _detect_tax_suffix(price_line)
        tax_code = 'T' if tax_suffix else 'Z'

        items.append({
            '_barcode': barcode,
            'description': desc,
            'quantity': 1.0,
            'unit_price': amount,
            'total_price': amount,
            'tax_code': tax_code,
            'confidence': 0.95,
        })
        i += 2

    for item in items:
        item.pop('_barcode', None)

    return items


# ---------------------------------------------------------------------------
# Overlay framework
# ---------------------------------------------------------------------------

class MerchantOverlay:
    """Base class. Subclasses set class attributes + optional hooks."""

    VENDOR_PATTERNS: list[str] = []
    VENDOR_CANONICAL: str = ""
    DEFAULT_GL_ACCOUNT: str = "5440"
    TAX_CODE_DEFAULT: str = "T"
    MIXED_TAX_EXPECTED: bool = False
    DATE_PATTERNS: list[str] = [
        r'(\d{4})-(\d{2})-(\d{2})',
        r'(\d{2})/(\d{2})/(\d{4})',
    ]
    TAXABLE_KEYWORDS: tuple[str, ...] = ()
    ZERO_RATED_KEYWORDS: tuple[str, ...] = ()
    CATEGORY: str = ""
    CONFIDENCE: float = 0.95

    @classmethod
    def matches(cls, text: str) -> bool:
        if not text:
            return False
        for pattern in cls.VENDOR_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False

    @classmethod
    def extract_vendor(cls, text: str) -> dict[str, Any] | None:
        if cls.matches(text):
            return {"name": cls.VENDOR_CANONICAL, "confidence": cls.CONFIDENCE}
        return None

    @classmethod
    def classify_line_tax(cls, description: str, amount: float = 0.0) -> str:
        desc_lower = (description or "").lower()
        for kw in cls.TAXABLE_KEYWORDS:
            if kw in desc_lower:
                return "T"
        for kw in cls.ZERO_RATED_KEYWORDS:
            if kw in desc_lower:
                return "Z"
        return cls.TAX_CODE_DEFAULT

    @classmethod
    def parse_line_items(cls, raw_text: str) -> list[dict[str, Any]]:
        return []


# ---------------------------------------------------------------------------
# Shared keyword sets
# ---------------------------------------------------------------------------

# Prepared / hot food — always taxable in Canada.
_PREPARED_FOOD = (
    "chef", "prêt", "pret", "ready", "chaud", "hot", "pizza",
    "sandwich", "burger", "sushi", "rotisserie", "rotisserie",
    "sub ", "wrap", "panini", "combo", "repas",
)
_BAKERY_TAXABLE = ("gateau", "gâteau", "tarte", "viennoiserie")
_SOFT_DRINKS = (
    "coke", "pepsi", "sprite", "soda", "eau ", "water ", "jus ",
    "juice", "energy drink", "red bull", "monster",
)
_SNACKS_TAXABLE = (
    "chips", "chocolate", "chocolat", "candy", "bonbon", "gum", "gomme",
    "cookies", "biscuits", "crackers", "croustille",
)
_ALCOHOL = (
    "beer", "biere", "bière", "wine", "vin", "spirits", "liqueur",
    "vodka", "rhum", "whisky", "tequila", "cider", "cidre",
)
_GROCERY_ZERO = (
    "lait ", "milk ", "pain ", "bread ", "riz ", "rice ",
    "farine", "flour", "oeufs", "eggs", "legume", "légume",
    "fruit", "banane", "banana", "pomme", "apple", "orange ",
    "viande", "meat", "poulet", "chicken ", "boeuf", "beef",
    "poisson", "fish ", "yogourt", "yogurt", "fromage",
    "cheese", "beurre", "butter", "carotte", "tomate", "tomato",
)
_OTC_DRUGS_TAXABLE = (
    "advil", "tylenol", "motrin", "aspirin", "claritin", "benadryl",
    "vitamin", "vitamine", "supplement", "multivit", "cough", "toux",
    "shampoo", "shampooing", "toothpaste", "dentifrice", "deodorant",
    "deodorant", "soap", "savon", "lotion", "creme", "crème",
    "makeup", "maquillage", "nail", "ongle",
)
_PRESCRIPTION_KEYWORDS = (
    "rx", "prescription", "rgam", "ordonnance", "patient",
    "franchise", "coassurance", "contribution",
)


# ---------------------------------------------------------------------------
# Grocery (Quebec)
# ---------------------------------------------------------------------------

class MetroOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bMETRO\b(?!\s*VANCOUVER)',
        r'\bMETRO\s+PLUS\b',
        r'\bMETRO\s+RICHELIEU\b',
        r'\bMETRO\s*#?\s*\d+',
    ]
    VENDOR_CANONICAL = "Metro"
    CATEGORY = "grocery"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "Z"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _PREPARED_FOOD + _SOFT_DRINKS + _SNACKS_TAXABLE + _ALCOHOL
    ZERO_RATED_KEYWORDS = _GROCERY_ZERO


class ProvigoOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bPROVIGO\b', r'\bPROVIGO\s*LE\s*MARCHE\b']
    VENDOR_CANONICAL = "Provigo"
    CATEGORY = "grocery"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "Z"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _PREPARED_FOOD + _SOFT_DRINKS + _SNACKS_TAXABLE + _ALCOHOL
    ZERO_RATED_KEYWORDS = _GROCERY_ZERO


class SuperCOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bSUPER\s*C\b',
        r'\bSUPERC\b',
    ]
    VENDOR_CANONICAL = "Super C"
    CATEGORY = "grocery"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "Z"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _PREPARED_FOOD + _SOFT_DRINKS + _SNACKS_TAXABLE + _ALCOHOL
    ZERO_RATED_KEYWORDS = _GROCERY_ZERO


class MaxiOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bMAXI\b', r'\bMAXI\s*&\s*CIE\b', r'\bMAXI\s+ET\s+CIE\b']
    VENDOR_CANONICAL = "Maxi"
    CATEGORY = "grocery"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "Z"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _PREPARED_FOOD + _SOFT_DRINKS + _SNACKS_TAXABLE + _ALCOHOL
    ZERO_RATED_KEYWORDS = _GROCERY_ZERO


class AdonisOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bADONIS\b', r'\bMARCHE\s+ADONIS\b', r'\bMARCHÉ\s+ADONIS\b']
    VENDOR_CANONICAL = "Adonis"
    CATEGORY = "grocery"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "Z"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _PREPARED_FOOD + _SOFT_DRINKS + _SNACKS_TAXABLE + _ALCOHOL
    ZERO_RATED_KEYWORDS = _GROCERY_ZERO + (
        "pita", "hummus", "tabouleh", "baklava", "feta",
    )


class RichelieuOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bMARCHE\s+RICHELIEU\b',
        r'\bMARCHÉ\s+RICHELIEU\b',
        r'\bRICHELIEU\s+MARCHE\b',
    ]
    VENDOR_CANONICAL = "Marché Richelieu"
    CATEGORY = "grocery"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "Z"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _PREPARED_FOOD + _SOFT_DRINKS + _SNACKS_TAXABLE + _ALCOHOL
    ZERO_RATED_KEYWORDS = _GROCERY_ZERO


class IGAOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bIGA\b', r'\bIGA\s+EXTRA\b', r'\bIGA\s*#?\s*\d+']
    VENDOR_CANONICAL = "IGA"
    CATEGORY = "grocery"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "Z"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _PREPARED_FOOD + _SOFT_DRINKS + _SNACKS_TAXABLE + _ALCOHOL
    ZERO_RATED_KEYWORDS = _GROCERY_ZERO


class WalmartOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bWAL\s*MART\b',
        r'\bWALMART\b',
        r'\bWAL-MART\b',
    ]
    VENDOR_CANONICAL = "Walmart"
    CATEGORY = "grocery_big_box"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "T"  # Walmart is mostly general merch = taxable
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _PREPARED_FOOD + _SOFT_DRINKS + _SNACKS_TAXABLE + _ALCOHOL
    ZERO_RATED_KEYWORDS = _GROCERY_ZERO


# ---------------------------------------------------------------------------
# Pharmacy (Quebec)
# ---------------------------------------------------------------------------

class JeanCoutuOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bJEAN\s*COUTU\b',
        r'\bGROUPE\s+JEAN\s+COUTU\b',
        r'\bPJC\b',
        r'\bPJC\s+JEAN\s+COUTU\b',
    ]
    VENDOR_CANONICAL = "Jean Coutu"
    CATEGORY = "pharmacy"
    DEFAULT_GL_ACCOUNT = "5640"
    TAX_CODE_DEFAULT = "T"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _OTC_DRUGS_TAXABLE + _SNACKS_TAXABLE + _SOFT_DRINKS
    ZERO_RATED_KEYWORDS = _PRESCRIPTION_KEYWORDS


class PharmaprixOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bPHARMAPRIX\b',
        r'\bSHOPPERS\s+DRUG\s+MART\b',
    ]
    VENDOR_CANONICAL = "Pharmaprix"
    CATEGORY = "pharmacy"
    DEFAULT_GL_ACCOUNT = "5640"
    TAX_CODE_DEFAULT = "T"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _OTC_DRUGS_TAXABLE + _SNACKS_TAXABLE + _SOFT_DRINKS
    ZERO_RATED_KEYWORDS = _PRESCRIPTION_KEYWORDS


class FamiliprixOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bFAMILIPRIX\b', r'\bPHARMACIE\s+FAMILIPRIX\b']
    VENDOR_CANONICAL = "Familiprix"
    CATEGORY = "pharmacy"
    DEFAULT_GL_ACCOUNT = "5640"
    TAX_CODE_DEFAULT = "T"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _OTC_DRUGS_TAXABLE + _SNACKS_TAXABLE + _SOFT_DRINKS
    ZERO_RATED_KEYWORDS = _PRESCRIPTION_KEYWORDS


class UniprixOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bUNIPRIX\b',
        r'\bUNI-PRIX\b',
        r'\bPHARMACIE\s+UNIPRIX\b',
    ]
    VENDOR_CANONICAL = "Uniprix"
    CATEGORY = "pharmacy"
    DEFAULT_GL_ACCOUNT = "5640"
    TAX_CODE_DEFAULT = "T"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _OTC_DRUGS_TAXABLE + _SNACKS_TAXABLE + _SOFT_DRINKS
    ZERO_RATED_KEYWORDS = _PRESCRIPTION_KEYWORDS


class BrunetOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bBRUNET\b', r'\bPHARMACIE\s+BRUNET\b']
    VENDOR_CANONICAL = "Brunet"
    CATEGORY = "pharmacy"
    DEFAULT_GL_ACCOUNT = "5640"
    TAX_CODE_DEFAULT = "T"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _OTC_DRUGS_TAXABLE + _SNACKS_TAXABLE + _SOFT_DRINKS
    ZERO_RATED_KEYWORDS = _PRESCRIPTION_KEYWORDS


# ---------------------------------------------------------------------------
# Coffee / QSR
# ---------------------------------------------------------------------------

class TimHortonsOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bTIM\s*HORTONS?\b',
        r'\bTDL\s+GROUP\b',
        r'\bTHE\s+TDL\s+GROUP\b',
    ]
    VENDOR_CANONICAL = "Tim Hortons"
    CATEGORY = "qsr"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = ("coffee", "cafe", "donut", "beigne", "timbit", "latte", "espresso")


class StarbucksOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bSTARBUCKS\b']
    VENDOR_CANONICAL = "Starbucks"
    CATEGORY = "qsr"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = ("coffee", "cafe", "latte", "espresso", "frappuccino", "tea", "the")


class SecondCupOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bSECOND\s+CUP\b']
    VENDOR_CANONICAL = "Second Cup"
    CATEGORY = "qsr"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = ("coffee", "cafe", "latte", "espresso", "tea", "the")


class McDonaldsOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r"\bMCDONALD'?S\b",
        r'\bMC\s*DONALD\b',
        r'\bMCDO\b',
    ]
    VENDOR_CANONICAL = "McDonald's"
    CATEGORY = "qsr"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = ("big mac", "mcmuffin", "mcwrap", "combo", "fries", "frites")


class SubwayOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bSUBWAY\b']
    VENDOR_CANONICAL = "Subway"
    CATEGORY = "qsr"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = ("sub ", "footlong", "6-inch", "6 inch", "wrap", "salad", "salade")


class SaintHubertOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bST-HUBERT\b',
        r'\bSAINT-HUBERT\b',
        r'\bST\.\s*HUBERT\b',
        r'\bROTISSERIE\s+ST[- ]HUBERT\b',
    ]
    VENDOR_CANONICAL = "Saint-Hubert"
    CATEGORY = "qsr"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = ("poulet", "rotisserie", "combo", "repas", "frites")


class LaCageOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bLA\s+CAGE\b',
        r'\bCAGE\s+AUX\s+SPORTS\b',
        r'\bLA\s+CAGE\s+BRASSERIE\b',
    ]
    VENDOR_CANONICAL = "La Cage"
    CATEGORY = "qsr"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = ("burger", "ailes", "wings", "nachos", "frites", "bière", "biere")


class NormandinOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bNORMANDIN\b', r'\bRESTAURANT\s+NORMANDIN\b']
    VENDOR_CANONICAL = "Normandin"
    CATEGORY = "qsr"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = ("combo", "dejeuner", "déjeuner", "burger", "frites")


# ---------------------------------------------------------------------------
# Gas stations
# ---------------------------------------------------------------------------

class PetroCanadaOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bPETRO[- ]?CAN(?:ADA)?\b',
        r'\bPETROCANADA\b',
    ]
    VENDOR_CANONICAL = "Petro-Canada"
    CATEGORY = "gas"
    DEFAULT_GL_ACCOUNT = "5430"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = ("essence", "gasoline", "diesel", "regular", "super")


class UltramarOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bULTRAMAR\b']
    VENDOR_CANONICAL = "Ultramar"
    CATEGORY = "gas"
    DEFAULT_GL_ACCOUNT = "5430"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = ("essence", "gasoline", "diesel", "regular", "super")


class ShellOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bSHELL\b(?!\s+FISH)', r'\bSHELL\s+CANADA\b']
    VENDOR_CANONICAL = "Shell"
    CATEGORY = "gas"
    DEFAULT_GL_ACCOUNT = "5430"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = ("essence", "gasoline", "diesel", "v-power", "regular")


class EssoOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bESSO\b', r'\bIMPERIAL\s+OIL\b']
    VENDOR_CANONICAL = "Esso"
    CATEGORY = "gas"
    DEFAULT_GL_ACCOUNT = "5430"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = ("essence", "gasoline", "diesel", "supreme", "regular")


class SonicOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bSONIC\b', r'\bPETROLES\s+SONIC\b']
    VENDOR_CANONICAL = "Sonic"
    CATEGORY = "gas"
    DEFAULT_GL_ACCOUNT = "5430"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = ("essence", "gasoline", "diesel", "regular", "super")


class CoucheTardOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bCOUCHE[- ]TARD\b',
        r'\bCOUCHE TARD\b',
        r'\bCIRCLE\s*K\b',
    ]
    VENDOR_CANONICAL = "Couche-Tard"
    CATEGORY = "gas_convenience"
    DEFAULT_GL_ACCOUNT = "5430"
    TAX_CODE_DEFAULT = "T"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = ("essence", "gasoline", "diesel") + _SNACKS_TAXABLE + _SOFT_DRINKS
    ZERO_RATED_KEYWORDS = _GROCERY_ZERO


# ---------------------------------------------------------------------------
# Hardware / big box
# ---------------------------------------------------------------------------

class HomeDepotOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bHOME\s+DEPOT\b', r'\bTHE\s+HOME\s+DEPOT\b']
    VENDOR_CANONICAL = "Home Depot"
    CATEGORY = "hardware"
    DEFAULT_GL_ACCOUNT = "5500"
    TAX_CODE_DEFAULT = "T"


class RonaOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bRONA\b', r'\bRONA\s+INC\b', r'\bRONA\s+L\'ENTREPÔT\b']
    VENDOR_CANONICAL = "Rona"
    CATEGORY = "hardware"
    DEFAULT_GL_ACCOUNT = "5500"
    TAX_CODE_DEFAULT = "T"


class CanadianTireOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bCANADIAN\s+TIRE\b',
        r'\bCAN(?:ADIAN)?\s*TIRE\b',
    ]
    VENDOR_CANONICAL = "Canadian Tire"
    CATEGORY = "hardware"
    DEFAULT_GL_ACCOUNT = "5500"
    TAX_CODE_DEFAULT = "T"


class RenoDepotOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bRENO[- ]?DEPOT\b', r'\bRENO-DÉPÔT\b', r'\bRÉNO-DÉPÔT\b']
    VENDOR_CANONICAL = "Reno Depot"
    CATEGORY = "hardware"
    DEFAULT_GL_ACCOUNT = "5500"
    TAX_CODE_DEFAULT = "T"


class PatrickMorinOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bPATRICK\s+MORIN\b']
    VENDOR_CANONICAL = "Patrick Morin"
    CATEGORY = "hardware"
    DEFAULT_GL_ACCOUNT = "5500"
    TAX_CODE_DEFAULT = "T"


# ---------------------------------------------------------------------------
# Other (discount, alcohol, office supplies, online)
# ---------------------------------------------------------------------------

class DollaramaOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [r'\bDOLLARAMA\b']
    VENDOR_CANONICAL = "Dollarama"
    CATEGORY = "discount"
    DEFAULT_GL_ACCOUNT = "5410"
    TAX_CODE_DEFAULT = "T"
    CONFIDENCE = 0.9  # No header logo makes this harder


class SAQOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bSAQ\b',
        r'\bSOCIETE\s+DES\s+ALCOOLS\b',
        r'\bSOCIÉTÉ\s+DES\s+ALCOOLS\b',
    ]
    VENDOR_CANONICAL = "SAQ"
    CATEGORY = "alcohol"
    DEFAULT_GL_ACCOUNT = "5420"
    TAX_CODE_DEFAULT = "T"
    TAXABLE_KEYWORDS = _ALCOHOL


class StaplesOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bSTAPLES\b',
        r'\bBUREAU\s+EN\s+GROS\b',
        r'\bBUREAU\s+ENGROS\b',
    ]
    VENDOR_CANONICAL = "Staples / Bureau en Gros"
    CATEGORY = "office_supplies"
    DEFAULT_GL_ACCOUNT = "5410"
    TAX_CODE_DEFAULT = "T"


class AmazonOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bAMAZON\.?CA\b',
        r'\bAMAZON\.?COM\b',
        r'\bAMAZON\s+MARKETPLACE\b',
        r'\bAMZN\b',
    ]
    VENDOR_CANONICAL = "Amazon.ca"
    CATEGORY = "online_retail"
    DEFAULT_GL_ACCOUNT = "5440"
    TAX_CODE_DEFAULT = "T"
    MIXED_TAX_EXPECTED = True


class CostcoOverlay(MerchantOverlay):
    VENDOR_PATTERNS = [
        r'\bCOSTCO\b',
        r'\bCOSTCO\s+WHOLESALE\b',
    ]
    VENDOR_CANONICAL = "Costco"
    CATEGORY = "wholesale"
    DEFAULT_GL_ACCOUNT = "5410"
    TAX_CODE_DEFAULT = "T"
    MIXED_TAX_EXPECTED = True
    TAXABLE_KEYWORDS = _PREPARED_FOOD + _SOFT_DRINKS + _SNACKS_TAXABLE + _ALCOHOL
    ZERO_RATED_KEYWORDS = _GROCERY_ZERO

    @classmethod
    def parse_line_items(cls, raw_text: str) -> list[dict[str, Any]]:
        return parse_costco_receipt(raw_text)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

OVERLAYS: tuple[type[MerchantOverlay], ...] = (
    # Grocery
    MetroOverlay, ProvigoOverlay, SuperCOverlay, MaxiOverlay,
    AdonisOverlay, RichelieuOverlay, IGAOverlay, WalmartOverlay,
    # Pharmacy
    JeanCoutuOverlay, PharmaprixOverlay, FamiliprixOverlay,
    UniprixOverlay, BrunetOverlay,
    # Coffee / QSR
    TimHortonsOverlay, StarbucksOverlay, SecondCupOverlay,
    McDonaldsOverlay, SubwayOverlay, SaintHubertOverlay,
    LaCageOverlay, NormandinOverlay,
    # Gas
    PetroCanadaOverlay, UltramarOverlay, ShellOverlay, EssoOverlay,
    SonicOverlay, CoucheTardOverlay,
    # Hardware
    HomeDepotOverlay, RonaOverlay, CanadianTireOverlay,
    RenoDepotOverlay, PatrickMorinOverlay,
    # Other
    DollaramaOverlay, SAQOverlay, StaplesOverlay, AmazonOverlay,
    CostcoOverlay,
)


def find_overlay(raw_text: str) -> type[MerchantOverlay] | None:
    """Return the first overlay whose vendor patterns match ``raw_text``."""
    if not raw_text:
        return None
    for overlay in OVERLAYS:
        if overlay.matches(raw_text):
            return overlay
    return None


def find_overlay_by_name(name: str) -> type[MerchantOverlay] | None:
    """Return the overlay whose canonical name matches ``name``
    case-insensitively."""
    if not name:
        return None
    target = name.strip().lower()
    for overlay in OVERLAYS:
        if overlay.VENDOR_CANONICAL.lower() == target:
            return overlay
    # Fallback: test the name against patterns.
    for overlay in OVERLAYS:
        if overlay.matches(name):
            return overlay
    return None


def overlay_hints(name_or_text: str) -> dict[str, Any]:
    """Return ``{canonical, gl_account, tax_code, category, mixed_tax_expected}``
    for the first overlay matching either the vendor name or raw OCR text.
    Empty dict if no overlay matches."""
    overlay = find_overlay_by_name(name_or_text) or find_overlay(name_or_text)
    if not overlay:
        return {}
    return {
        "canonical": overlay.VENDOR_CANONICAL,
        "gl_account": overlay.DEFAULT_GL_ACCOUNT,
        "tax_code": overlay.TAX_CODE_DEFAULT,
        "category": overlay.CATEGORY,
        "mixed_tax_expected": overlay.MIXED_TAX_EXPECTED,
    }


def apply_merchant_overlay(
    items: list[dict[str, Any]],
    merchant_name: str,
    raw_text: str,
) -> list[dict[str, Any]]:
    """Return merchant-aware items, falling back to the input list.

    Kept signature-compatible with the prior free-function version so that
    ``src/engines/line_item_engine.py`` does not need changes.
    """
    # 1. Try to resolve the overlay by vendor name first; fall back to OCR
    #    text match (helps when the extractor didn't pick up the vendor).
    overlay = find_overlay_by_name(merchant_name) or find_overlay(raw_text or "")
    if overlay is None:
        return items

    # 2. Merchants with custom parse_line_items (e.g. Costco barcode layout):
    #    trust the custom parser when it returns rows.
    custom_items = overlay.parse_line_items(raw_text or "")
    if custom_items:
        return custom_items

    # 3. For every other overlay, re-tag tax codes on the generic items so
    #    grocery zero-rated / prepared-food taxable distinctions land
    #    correctly for Canadian CPA review.
    if not items:
        return items
    retagged: list[dict[str, Any]] = []
    for item in items:
        new_item = dict(item)
        desc = new_item.get("description") or ""
        amt = float(new_item.get("total_price") or 0)
        # Respect explicit zero prices (comp items) and fixed tax codes that
        # appear to come directly from the receipt (T/Z suffixes).
        current = new_item.get("tax_code")
        if current not in ("T", "Z", "E", "M"):
            new_item["tax_code"] = overlay.classify_line_tax(desc, amt)
        # Let overlays inject the default GL only when missing.
        if not new_item.get("gl_account"):
            new_item["gl_account"] = overlay.DEFAULT_GL_ACCOUNT
        retagged.append(new_item)
    return retagged


def resolve_vendor(raw_text: str) -> dict[str, Any] | None:
    """Attempt to extract canonical vendor from raw OCR text via overlays.
    Returns ``{name, confidence, category, gl_account, tax_code}`` or None.
    """
    overlay = find_overlay(raw_text or "")
    if overlay is None:
        return None
    vendor = overlay.extract_vendor(raw_text or "")
    if vendor is None:
        return None
    return {
        **vendor,
        "category": overlay.CATEGORY,
        "gl_account": overlay.DEFAULT_GL_ACCOUNT,
        "tax_code": overlay.TAX_CODE_DEFAULT,
    }


def list_overlays() -> list[dict[str, Any]]:
    """Introspection helper for reports/docs."""
    return [
        {
            "canonical": o.VENDOR_CANONICAL,
            "category": o.CATEGORY,
            "gl_account": o.DEFAULT_GL_ACCOUNT,
            "tax_code_default": o.TAX_CODE_DEFAULT,
            "patterns": list(o.VENDOR_PATTERNS),
        }
        for o in OVERLAYS
    ]
