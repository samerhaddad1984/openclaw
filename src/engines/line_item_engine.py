"""
src/engines/line_item_engine.py — Line-level invoice parsing engine.

Extracts individual line items from multi-line invoices, determines per-line
tax regime (GST/QST, HST, GST-only, exempt), calculates tax amounts, and
reconciles line totals against the invoice total.

All monetary arithmetic uses Python Decimal.  AI calls are limited to
the ``extract_invoice_lines`` function which uses the OpenRouter client.

Public interface
----------------
extract_invoice_lines(document_id, raw_ocr_text, conn)
detect_tax_included_per_line(line)
determine_place_of_supply(line, vendor_province, buyer_province)
assign_line_tax_regime(line, place_of_supply)
calculate_line_tax(line, tax_regime, is_tax_included)
reconcile_invoice_lines(document_id, conn)
allocate_deposit_proportionally(document_id, deposit_amount, conn)
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

# ---------------------------------------------------------------------------
# Tax rate constants (mirrors tax_engine.py)
# ---------------------------------------------------------------------------

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HALF = Decimal("0.5")
CENT = Decimal("0.01")

MAX_LINE_ITEMS = 50

GST_RATE = Decimal("0.05")
QST_RATE = Decimal("0.09975")
HST_RATE_ON = Decimal("0.13")
HST_RATE_ATL = Decimal("0.15")
PST_RATES: dict[str, Decimal] = {
    "BC": Decimal("0.07"),
    "MB": Decimal("0.07"),
    "SK": Decimal("0.06"),
}

HST_PROVINCES = frozenset({"ON", "NB", "NS", "NL", "PE"})
ATL_PROVINCES = frozenset({"NB", "NS", "NL", "PE"})
GST_ONLY_PROVINCES = frozenset({"AB", "NT", "NU", "YT"})

# ---------------------------------------------------------------------------
# Prompt template
# ---------------------------------------------------------------------------

_PROMPT_PATH = Path(__file__).resolve().parent.parent / "agents" / "prompts" / "extract_invoice_lines.txt"


def _load_prompt() -> str:
    try:
        return _PROMPT_PATH.read_text(encoding="utf-8")
    except Exception:
        return (
            "Extract every line item from this invoice as JSON. "
            "Include shipping, fees, adjustments. Return JSON only."
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _round(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _to_dec(value: Any) -> Decimal:
    if value is None or str(value).strip() == "":
        return _ZERO
    return Decimal(str(value))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# TAX-INCLUDED DETECTION
# ---------------------------------------------------------------------------

_PRORATION_KW = re.compile(
    r"remaining\s+time|unused\s+time|proration|prorated|"
    r"credit\s+for|adjustment|ajustement",
    re.IGNORECASE,
)


_TAX_INCLUDED_KW = re.compile(
    r"tax\s*incl|taxes?\s*incluses?|ttc|incl\.?\s*hst|incl\.?\s*tax|"
    r"toutes\s*taxes\s*comprises",
    re.IGNORECASE,
)
_TAX_EXCLUDED_KW = re.compile(
    r"before\s*tax|avant\s*taxes?|excl\.?\s*tax|net\b|ht\b|hors\s*taxes?",
    re.IGNORECASE,
)


def detect_tax_included_per_line(line: dict[str, Any]) -> dict[str, Any]:
    """Detect if a line amount is tax-included, pre-tax, or ambiguous.

    Returns dict with:
        is_tax_included: True / False / None (ambiguous)
        detection_method: "keyword" | "indicator" | "ambiguous"
        keyword_found: the matched keyword or None
    """
    text = " ".join(str(v) for v in [
        line.get("description", ""),
        line.get("notes", ""),
        line.get("tax_indicator", ""),
    ]).strip()

    # Check AI-extracted indicator first
    indicator = str(line.get("tax_indicator", "")).lower().strip()
    if indicator == "tax_included":
        return {"is_tax_included": True, "detection_method": "indicator", "keyword_found": None}
    if indicator in ("taxable", "exempt"):
        return {"is_tax_included": False, "detection_method": "indicator", "keyword_found": None}

    # Keyword scan
    m_incl = _TAX_INCLUDED_KW.search(text)
    if m_incl:
        return {"is_tax_included": True, "detection_method": "keyword", "keyword_found": m_incl.group()}

    m_excl = _TAX_EXCLUDED_KW.search(text)
    if m_excl:
        return {"is_tax_included": False, "detection_method": "keyword", "keyword_found": m_excl.group()}

    # Ambiguous — flag for review
    return {"is_tax_included": None, "detection_method": "ambiguous", "keyword_found": None}


# ---------------------------------------------------------------------------
# PLACE OF SUPPLY
# ---------------------------------------------------------------------------

# Supply type constants
SUPPLY_TANGIBLE = "tangible_personal_property"
SUPPLY_SERVICE = "service"
SUPPLY_REAL_PROPERTY = "real_property"
SUPPLY_INTANGIBLE = "intangible"
SUPPLY_TRANSPORTATION = "transportation"
SUPPLY_SHIPPING = "shipping"


def determine_place_of_supply(
    line: dict[str, Any],
    vendor_province: str,
    buyer_province: str,
) -> str:
    """Determine the place of supply for a line item.

    Uses ETA Schedule IX rules:
    - Tangible goods: destination (buyer province)
    - Services: where predominantly performed
    - Shipping: follows principal supply if same contract, else destination
    - Ambiguous: returns "AMBIGUOUS"

    Returns a two-letter province code or "AMBIGUOUS".
    """
    supply_type = str(line.get("supply_type", "")).strip().lower()
    desc = str(line.get("description", "")).lower()

    # Heuristic: detect supply type from description if not set
    if not supply_type:
        _SHIPPING_KW = {"shipping", "freight", "delivery", "livraison", "transport", "expédition"}
        _SERVICE_KW = {"service", "labour", "labor", "installation", "consulting",
                       "main d'oeuvre", "consultation", "professional fee",
                       "honoraire", "honoraires"}
        if any(kw in desc for kw in _SHIPPING_KW):
            supply_type = SUPPLY_SHIPPING
        elif any(kw in desc for kw in _SERVICE_KW):
            supply_type = SUPPLY_SERVICE
        else:
            supply_type = SUPPLY_TANGIBLE

    vendor_prov = vendor_province.strip().upper() if vendor_province else ""
    buyer_prov = buyer_province.strip().upper() if buyer_province else ""

    if supply_type == SUPPLY_TANGIBLE:
        # Rule 1: delivery destination
        return buyer_prov if buyer_prov else (vendor_prov or "AMBIGUOUS")

    if supply_type == SUPPLY_SERVICE:
        # Rule 2: where predominantly performed
        # If we know both, prefer buyer location (conservative — most services
        # are performed where the buyer is). Flag ambiguous if unsure.
        service_location = str(line.get("service_location", "")).strip().upper()
        if service_location:
            return service_location
        if buyer_prov and vendor_prov and buyer_prov != vendor_prov:
            return "AMBIGUOUS"
        return buyer_prov or vendor_prov or "AMBIGUOUS"

    if supply_type == SUPPLY_REAL_PROPERTY:
        # Rule 3: where situated
        property_location = str(line.get("property_location", "")).strip().upper()
        return property_location or "AMBIGUOUS"

    if supply_type == SUPPLY_INTANGIBLE:
        # Rule 4: where recipient belongs
        return buyer_prov if buyer_prov else (vendor_prov or "AMBIGUOUS")

    if supply_type == SUPPLY_TRANSPORTATION:
        # Rule 5: origin to destination — use destination
        return buyer_prov if buyer_prov else "AMBIGUOUS"

    if supply_type == SUPPLY_SHIPPING:
        # Shipping: if same contract as principal supply → follows principal
        # We default to buyer (destination) for standalone shipping
        return buyer_prov if buyer_prov else (vendor_prov or "AMBIGUOUS")

    return "AMBIGUOUS"


# ---------------------------------------------------------------------------
# TAX REGIME ASSIGNMENT
# ---------------------------------------------------------------------------

def assign_line_tax_regime(
    line: dict[str, Any],
    place_of_supply: str,
) -> dict[str, Any]:
    """Assign tax regime to a line based on place of supply.

    Returns dict with:
        tax_regime: HST / GST_QST / GST_ONLY / EXEMPT / GST_PST / AMBIGUOUS
        tax_code: the tax code string
        gst_rate, qst_rate, hst_rate, pst_rate: Decimal rates
        notes: str
    """
    indicator = str(line.get("tax_indicator", "")).lower().strip()
    if indicator == "exempt":
        return {
            "tax_regime": "EXEMPT",
            "tax_code": "E",
            "gst_rate": _ZERO, "qst_rate": _ZERO,
            "hst_rate": _ZERO, "pst_rate": _ZERO,
            "notes": "Line marked exempt",
        }

    prov = place_of_supply.strip().upper()

    if prov == "AMBIGUOUS" or not prov:
        return {
            "tax_regime": "AMBIGUOUS",
            "tax_code": "",
            "gst_rate": _ZERO, "qst_rate": _ZERO,
            "hst_rate": _ZERO, "pst_rate": _ZERO,
            "notes": "Place of supply ambiguous — requires human review",
        }

    if prov == "QC":
        return {
            "tax_regime": "GST_QST",
            "tax_code": "T",
            "gst_rate": GST_RATE, "qst_rate": QST_RATE,
            "hst_rate": _ZERO, "pst_rate": _ZERO,
            "notes": f"Quebec: GST 5% + QST 9.975%",
        }

    if prov == "ON":
        return {
            "tax_regime": "HST",
            "tax_code": "HST",
            "gst_rate": _ZERO, "qst_rate": _ZERO,
            "hst_rate": HST_RATE_ON, "pst_rate": _ZERO,
            "notes": f"Ontario: HST 13%",
        }

    if prov in ATL_PROVINCES:
        return {
            "tax_regime": "HST",
            "tax_code": "HST_ATL",
            "gst_rate": _ZERO, "qst_rate": _ZERO,
            "hst_rate": HST_RATE_ATL, "pst_rate": _ZERO,
            "notes": f"{prov}: HST 15%",
        }

    if prov in GST_ONLY_PROVINCES:
        return {
            "tax_regime": "GST_ONLY",
            "tax_code": "GST_ONLY",
            "gst_rate": GST_RATE, "qst_rate": _ZERO,
            "hst_rate": _ZERO, "pst_rate": _ZERO,
            "notes": f"{prov}: GST 5% only",
        }

    if prov in PST_RATES:
        pst = PST_RATES[prov]
        return {
            "tax_regime": "GST_PST",
            "tax_code": "GST_ONLY",
            "gst_rate": GST_RATE, "qst_rate": _ZERO,
            "hst_rate": _ZERO, "pst_rate": pst,
            "notes": f"{prov}: GST 5% + PST {pst * 100}% (PST non-recoverable)",
        }

    # Unknown province — fall back to GST only
    return {
        "tax_regime": "GST_ONLY",
        "tax_code": "GST_ONLY",
        "gst_rate": GST_RATE, "qst_rate": _ZERO,
        "hst_rate": _ZERO, "pst_rate": _ZERO,
        "notes": f"Unknown province {prov} — defaulting to GST only",
    }


# ---------------------------------------------------------------------------
# LINE TAX CALCULATION
# ---------------------------------------------------------------------------

def calculate_line_tax(
    line: dict[str, Any],
    tax_regime: dict[str, Any],
    is_tax_included: bool | None,
) -> dict[str, Any]:
    """Calculate tax amounts for a single line.

    If tax_included: extract pre-tax = line_total / (1 + rate)
    If not tax_included: calculate tax = line_total * rate

    Returns dict with:
        pretax_amount, gst, qst, hst,
        recoverable_gst, recoverable_qst, recoverable_hst
    """
    line_total = _to_dec(line.get("line_total", 0))
    gst_rate = tax_regime.get("gst_rate", _ZERO)
    qst_rate = tax_regime.get("qst_rate", _ZERO)
    hst_rate = tax_regime.get("hst_rate", _ZERO)
    combined_rate = gst_rate + qst_rate + hst_rate

    regime_name = tax_regime.get("tax_regime", "")

    # ITC/ITR recovery percentages
    itc_pct = _ONE  # full recovery by default
    itr_pct = _ONE

    # Meals: 50% recovery (covers alcohol via _ALCOHOL_LINE_KW too —
    # alcohol lines are always 5640 / meals per the line GL classifier).
    desc = str(line.get("description", "")).lower()
    _MEAL_KW = {"meal", "repas", "restaurant", "dining", "entertainment",
                "divertissement", "reception", "réception"}
    line_gl = str(line.get("gl_account", "") or "").strip()
    line_cat = str(line.get("category", "") or "").strip().lower()
    if (
        any(kw in desc for kw in _MEAL_KW)
        or _line_is_alcohol(desc)
        or line_gl == "5640"
        or line_cat == "meals"
    ):
        itc_pct = _HALF
        itr_pct = _HALF

    if regime_name == "EXEMPT":
        return {
            "pretax_amount": line_total,
            "gst": _ZERO, "qst": _ZERO, "hst": _ZERO,
            "recoverable_gst": _ZERO, "recoverable_qst": _ZERO,
            "recoverable_hst": _ZERO,
        }

    if is_tax_included and combined_rate > _ZERO:
        # Reverse-calculate pre-tax amount
        divisor = _ONE + combined_rate
        pretax = _round(line_total / divisor)
    else:
        pretax = line_total

    gst = _round(pretax * gst_rate)
    qst = _round(pretax * qst_rate)
    hst = _round(pretax * hst_rate)

    return {
        "pretax_amount": pretax,
        "gst": gst,
        "qst": qst,
        "hst": hst,
        "recoverable_gst": _round(gst * itc_pct),
        "recoverable_qst": _round(qst * itr_pct),
        "recoverable_hst": _round(hst * itc_pct),
    }


def validate_line_tax(
    tax: dict[str, Any],
    tax_code: str,
    raw_text: str,
) -> tuple[dict[str, Any], str]:
    """Zero out any per-line tax that was *calculated* rather than *read*.

    Fundamental fix: the AI/engine must not invent per-line tax via
    price × rate.  If the calculated GST amount does not appear verbatim
    anywhere in the receipt text, it was fabricated — zero out GST/QST/HST
    and force tax_code="Z" so downstream totals don't pick up phantom tax.

    Accepts both "0.12" and "0,12" (Quebec receipts often use comma
    decimals).  Amounts of $0.00 are untouched (nothing to validate).
    """
    gst = tax.get("gst", _ZERO)
    if not isinstance(gst, Decimal):
        gst = _to_dec(gst)

    if gst <= _ZERO:
        return tax, tax_code

    amount_dot = f"{gst:.2f}"
    amount_comma = amount_dot.replace(".", ",")
    if amount_dot in raw_text or amount_comma in raw_text:
        return tax, tax_code

    # Amount was calculated, not read — strip phantom tax.
    tax = dict(tax)
    tax["gst"] = _ZERO
    tax["qst"] = _ZERO
    tax["hst"] = _ZERO
    tax["recoverable_gst"] = _ZERO
    tax["recoverable_qst"] = _ZERO
    tax["recoverable_hst"] = _ZERO
    return tax, "Z"


# ---------------------------------------------------------------------------
# DETERMINISTIC LINE-LEVEL GL CLASSIFIER
# ---------------------------------------------------------------------------
#
# Runs as a safety net for every extracted line so the database always has a
# usable gl_account/category even when the AI omits the field or the AI call
# is unavailable. The AI's classification (when present) takes precedence —
# this only fills in blanks. The Quebec GL codes mirror the rules in
# src/agents/prompts/extract_invoice_lines.txt.

# Capital threshold ($CAD) for treating equipment/furniture as a fixed asset.
CAPITAL_ASSET_THRESHOLD = Decimal("500")

_ALCOHOL_LINE_KW = (
    "boiss. alc", "boiss alc", "boissons alc", "alcool", "alcohol",
    "mojito", "cocktail", "martini", "whisky", "whiskey", "vodka",
    "biere", "bière", "beer", "vin ", " vin", "wine", "champagne",
    "liqueur", "spiritueux", "spirits", "rum", "tequila", "gin ",
    "sangria",
)

# Word-boundary regex used by the classifier so short tokens like "gin",
# "rum", "wine", "vin", "beer" don't false-match inside "virgin", "rumour",
# "wineberry", etc. Multi-word tokens (e.g. "boiss. alc") are matched as
# substrings since their full form is unambiguous.
_ALCOHOL_BOUNDARY_RE = re.compile(
    r"\b(?:mojito|cocktail|martini|whisky|whiskey|vodka|biere|bière|beer|"
    r"vin|wine|champagne|liqueur|spiritueux|spirits|rum|tequila|gin|"
    r"sangria|alcool|alcohol)\b",
    re.IGNORECASE,
)
_ALCOHOL_SUBSTRING_KW = (
    "boiss. alc", "boiss alc", "boissons alc",
)


def _line_is_alcohol(desc_lc: str) -> bool:
    """Return True if the line description signals an alcoholic item.

    Uses word-boundary matching so substrings like "gin" inside "virgin"
    or "rum" inside "rumour" do not trigger a false positive.
    """
    if not desc_lc:
        return False
    if any(kw in desc_lc for kw in _ALCOHOL_SUBSTRING_KW):
        return True
    return bool(_ALCOHOL_BOUNDARY_RE.search(desc_lc))
_RESTAURANT_VENDOR_KW = (
    "restaurant", "bistro", "café", "cafe", "deli", "diner",
    "grill", "saloon", "tavern", "taverne", "pub ", " pub",
    "bar ", "lounge", "nightclub", "brasserie", "pizzeria",
    "sushi", "buffet",
)
_HARDWARE_VENDOR_KW = (
    "reno-depot", "renodepot", "rénovation", "home depot", "home-depot",
    "homedepot", "rona", "canadian tire", "lowes", "lowe's",
    "ace hardware", "patrick morin", "matériaux",
)
_GROCERY_VENDOR_KW = (
    "iga", "metro", "provigo", "maxi", "super c", "superc",
    "loblaws", "sobeys", "food basics", "freshco", "no frills",
    "walmart supercent", "costco", "sam's club", "marché",
    "épicerie", "epicerie", "grocery",
)
_TELECOM_KW = (
    "phone", "téléphone", "telephone", "mobile", "cellular", "cell ",
    "internet", "wi-fi", "wifi", "data plan", "forfait", "hosting",
    "domain", "dns", "vpn",
)
_UTILITIES_KW = (
    "hydro", "electricity", "électricité", "electricite", "gas bill",
    "natural gas", "gaz naturel", "water bill", "aqueduc", "energir",
)
_SOFTWARE_KW = (
    "software", "logiciel", "subscription", "abonnement", "saas",
    "license", "licence", "app store", "google play", "monthly plan",
    "annual plan", "cloud", "hosting plan",
)
_SUPPLIES_KW = (
    "paper", "papier", "pen", "stylo", "stapler", "agrafeuse",
    "ink", "encre", "toner", "cartridge", "folder", "envelope",
    "office supply", "fourniture",
)
_REPAIRS_KW = (
    "repair", "réparation", "reparation", "maintenance", "entretien",
    "tune-up", "tune up", "service call", "fix",
)
_BANK_FEE_KW = (
    "bank fee", "frais bancaire", "service charge", "interac fee",
    "payment processing", "merchant fee", "card fee", "nsf",
    "wire fee", "transfer fee",
)
_FURNITURE_KW = (
    "chair", "chaise", "desk", "bureau ", "table", "sofa", "couch",
    "fauteuil", "shelving", "étagère", "etagere", "filing cabinet",
    "classeur", "armoire", "furniture", "mobilier",
)
_EQUIPMENT_KW = (
    "drill", "perceuse", "saw", "scie", "compressor", "compresseur",
    "ladder", "échelle", "machine", "outil", "tool ", "equipment",
    "équipement", "equipement", "generator", "génératrice",
    "computer", "ordinateur", "laptop", "imprimante", "printer",
    "monitor", "écran", "écran",
)

# Zero-rated basic groceries (Canadian tax rules): fresh produce, eggs,
# dairy, meat, fish, bread/cereals/grains. These items carry no GST/QST
# regardless of the province of supply, so we override tax_code to "Z"
# and zero out gst/qst when a line description matches one of these
# tokens. Word-boundary matching avoids false positives (e.g. "pate"
# inside "update").
ZERO_RATED_GROCERY = (
    "egg", "oeuf", "oeufs", "lait", "milk", "fromage", "cheese",
    "yogourt", "yogurt", "beurre", "butter",
    "pomme", "apple", "banane", "banana", "orange", "citron",
    "fraise", "strawberry", "framboise", "blueberry", "bluet",
    "tomate", "tomato", "laitue", "lettuce", "epinard", "spinach",
    "carotte", "carrot", "oignon", "onion", "patate", "potato",
    "poivron", "pepper", "asperge", "asparagus", "avocat", "avocado",
    "brocoli", "broccoli", "chou", "cabbage", "celeri", "celery",
    "concombre", "cucumber", "champignon", "mushroom",
    "poulet", "chicken", "boeuf", "beef", "porc", "pork",
    "saumon", "salmon", "thon", "tuna", "crevette", "shrimp",
    "pain", "bread", "riz", "rice", "pate", "pasta", "farine", "flour",
)

_ZERO_RATED_GROCERY_RE = re.compile(
    r"\b(?:" + "|".join(ZERO_RATED_GROCERY) + r")\b",
    re.IGNORECASE,
)


def _line_is_zero_rated_grocery(desc_lc: str) -> bool:
    """Return True if the description matches a zero-rated basic grocery.

    Uses word-boundary matching so short tokens like "egg" don't match
    inside "eggplant" and "pate" doesn't match inside "update".
    """
    if not desc_lc:
        return False
    return bool(_ZERO_RATED_GROCERY_RE.search(desc_lc))


def _line_amount_dec(raw_line: dict[str, Any]) -> Decimal:
    """Best-effort Decimal of a line's monetary value."""
    for key in ("line_total", "line_total_pretax", "unit_price"):
        v = raw_line.get(key)
        if v is None:
            continue
        try:
            return _to_dec(v).copy_abs()
        except Exception:
            continue
    return _ZERO


def classify_line_gl(
    raw_line: dict[str, Any],
    vendor_name: str = "",
) -> dict[str, Any]:
    """Deterministic GL classifier for a single line item.

    Returns dict with gl_account, category, is_capital, capital_notes,
    optionally tax_indicator and notes additions. Caller decides how to
    merge these against any AI-supplied values (AI wins on conflict).
    """
    desc = str(raw_line.get("description", "") or "").lower()
    vendor = (vendor_name or "").lower()
    amount = _line_amount_dec(raw_line)

    extra_notes: list[str] = []
    is_alcohol = _line_is_alcohol(desc)
    is_restaurant_vendor = any(kw in vendor for kw in _RESTAURANT_VENDOR_KW)
    is_hardware_vendor = any(kw in vendor for kw in _HARDWARE_VENDOR_KW)
    is_grocery_vendor = any(kw in vendor for kw in _GROCERY_VENDOR_KW)
    is_zero_rated_grocery = (
        _line_is_zero_rated_grocery(desc) and not is_alcohol
    )
    if is_zero_rated_grocery:
        pass  # tax_code=Z is self-explanatory; no redundant note needed

    # 1) Alcohol always wins — flagged for human review even at restaurants.
    if is_alcohol:
        extra_notes.append("alcohol — verify business purpose")
        return {
            "gl_account": "5640",
            "category": "meals",
            "is_capital": False,
            "capital_notes": "",
            "tax_indicator": "taxable",
            "extra_notes": extra_notes,
        }

    # 2) Restaurant/bar vendor → meals (only food/drink lines reach here).
    if is_restaurant_vendor:
        return {
            "gl_account": "5640",
            "category": "meals",
            "is_capital": False,
            "capital_notes": "",
            "tax_indicator": None,
            "extra_notes": extra_notes,
        }

    # 3) Hardware-store vendor → classify each line on its own merits.
    if is_hardware_vendor:
        if amount >= CAPITAL_ASSET_THRESHOLD and any(kw in desc for kw in _FURNITURE_KW):
            return {
                "gl_account": "1830",
                "category": "capital",
                "is_capital": True,
                "capital_notes": "Class 8 — furniture/fixtures",
                "tax_indicator": None,
                "extra_notes": extra_notes,
            }
        if amount >= CAPITAL_ASSET_THRESHOLD and any(kw in desc for kw in _EQUIPMENT_KW):
            return {
                "gl_account": "1820",
                "category": "capital",
                "is_capital": True,
                "capital_notes": "Class 8 — equipment/tools",
                "tax_indicator": None,
                "extra_notes": extra_notes,
            }
        # Hardware store small purchases default to supplies.
        return {
            "gl_account": "5430",
            "category": "supplies",
            "is_capital": False,
            "capital_notes": "",
            "tax_indicator": None,
            "extra_notes": extra_notes,
        }

    # 3b) Grocery vendor → supplies (5430) for food/produce items.
    if is_grocery_vendor:
        return {
            "gl_account": "5430",
            "category": "supplies",
            "is_capital": False,
            "capital_notes": "",
            "tax_indicator": None,
            "extra_notes": extra_notes,
        }

    # 4) Description-based routing.
    if any(kw in desc for kw in _BANK_FEE_KW):
        return {"gl_account": "5500", "category": "bank_fees",
                "is_capital": False, "capital_notes": "",
                "tax_indicator": None, "extra_notes": extra_notes}
    if any(kw in desc for kw in _SOFTWARE_KW):
        return {"gl_account": "5420", "category": "software",
                "is_capital": False, "capital_notes": "",
                "tax_indicator": None, "extra_notes": extra_notes}
    if any(kw in desc for kw in _TELECOM_KW):
        return {"gl_account": "5400", "category": "telecom",
                "is_capital": False, "capital_notes": "",
                "tax_indicator": None, "extra_notes": extra_notes}
    if any(kw in desc for kw in _UTILITIES_KW):
        return {"gl_account": "5410", "category": "utilities",
                "is_capital": False, "capital_notes": "",
                "tax_indicator": None, "extra_notes": extra_notes}
    if any(kw in desc for kw in _REPAIRS_KW):
        return {"gl_account": "5750", "category": "repairs",
                "is_capital": False, "capital_notes": "",
                "tax_indicator": None, "extra_notes": extra_notes}

    # 5) Capital asset by description + threshold (no hardware vendor).
    if amount >= CAPITAL_ASSET_THRESHOLD and any(kw in desc for kw in _FURNITURE_KW):
        return {"gl_account": "1830", "category": "capital",
                "is_capital": True, "capital_notes": "Class 8 — furniture/fixtures",
                "tax_indicator": None, "extra_notes": extra_notes}
    if amount >= CAPITAL_ASSET_THRESHOLD and any(kw in desc for kw in _EQUIPMENT_KW):
        return {"gl_account": "1820", "category": "capital",
                "is_capital": True, "capital_notes": "Class 8 — equipment/tools",
                "tax_indicator": None, "extra_notes": extra_notes}

    if any(kw in desc for kw in _SUPPLIES_KW):
        return {"gl_account": "5430", "category": "supplies",
                "is_capital": False, "capital_notes": "",
                "tax_indicator": None, "extra_notes": extra_notes}

    # 6) Default: general operating expense.
    return {
        "gl_account": "5440",
        "category": "operating_expense",
        "is_capital": False,
        "capital_notes": "",
        "tax_indicator": None,
        "extra_notes": extra_notes,
    }


# ---------------------------------------------------------------------------
# AI EXTRACTION
# ---------------------------------------------------------------------------

def extract_invoice_lines(
    document_id: str,
    raw_ocr_text: str,
    conn: sqlite3.Connection,
    vendor_name: str = "",
) -> list[dict[str, Any]]:
    """Extract line items from OCR text using AI, store in invoice_lines table.

    Uses OpenRouterClient with the extract_invoice_lines prompt template.
    Returns the list of extracted line dicts.

    *vendor_name* is used by the deterministic GL classifier to apply
    restaurant/hardware-store rules even when the AI omits the gl_account
    field on a line.
    """
    import sys
    root_str = str(ROOT_DIR)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

    from src.agents.tools.openrouter_client import OpenRouterClient

    prompt_template = _load_prompt()
    prompt = prompt_template.replace("{INVOICE_TEXT}", raw_ocr_text[:20000])

    system_msg = (
        "You are an accounting document line-item extractor for a Canadian "
        "bookkeeping workflow. Return STRICT JSON only. Never invent numbers "
        "not present in the text."
    )

    client = OpenRouterClient()
    result = client.chat_json(system=system_msg, user=prompt, temperature=0.0)

    lines = result.get("lines", [])
    invoice_total_shown = result.get("invoice_total_shown")
    tax_total_shown = result.get("tax_total_shown")
    deposit_found = result.get("deposit_found", False)
    deposit_amount = result.get("deposit_amount", 0)
    now = _utc_now_iso()

    # Ensure invoice_lines table columns exist
    _ensure_invoice_lines_table(conn)

    # Clear previous lines for this document
    conn.execute("DELETE FROM invoice_lines WHERE document_id = ?", (document_id,))

    # ── Proration / credit-memo detection ──
    all_negative = bool(lines) and all(
        float(l.get("quantity", 1)) < 0 or float(l.get("line_total", 0)) < 0
        for l in lines
    )
    has_proration = any(
        _PRORATION_KW.search(str(l.get("description", "")))
        for l in lines
    )
    is_proration_invoice = has_proration or all_negative

    if is_proration_invoice:
        # Collapse proration lines into a single net-amount summary line
        net_total = sum(float(l.get("line_total", 0)) for l in lines)
        invoice_type = "credit_memo" if all_negative else "proration_adjustment"
        summary_desc_parts = [str(l.get("description", "")) for l in lines[:5]]
        summary_desc = f"{invoice_type.replace('_', ' ').title()}: {'; '.join(summary_desc_parts)}"
        if len(lines) > 5:
            summary_desc += f" (+{len(lines) - 5} more)"
        lines = [{
            "line_number": 1,
            "description": summary_desc,
            "quantity": 1,
            "unit_price": net_total,
            "line_total": net_total,
            "tax_indicator": "taxable",
            "notes": f"Net of {len(result.get('lines', []))} proration/adjustment lines. "
                     f"invoice_type={invoice_type}",
        }]

    # ── Cap at MAX_LINE_ITEMS ──
    truncated_count = 0
    if len(lines) > MAX_LINE_ITEMS:
        truncated_count = len(lines) - MAX_LINE_ITEMS
        lines = lines[:MAX_LINE_ITEMS]
        lines.append({
            "line_number": MAX_LINE_ITEMS + 1,
            "description": f"{truncated_count} additional lines — see original invoice",
            "quantity": None,
            "unit_price": None,
            "line_total": None,
            "tax_indicator": "exempt",
            "notes": "Truncated summary row",
        })

    stored_lines: list[dict[str, Any]] = []

    for raw_line in lines:
        line_num = int(raw_line.get("line_number", 0))
        description = str(raw_line.get("description", ""))
        quantity = raw_line.get("quantity", 1)
        unit_price = raw_line.get("unit_price")
        line_total = raw_line.get("line_total", 0)
        tax_indicator = str(raw_line.get("tax_indicator", "taxable"))
        tax_amount_shown = raw_line.get("tax_amount_shown")
        notes = raw_line.get("notes", "")

        # Detect tax-included
        tax_det = detect_tax_included_per_line(raw_line)
        is_tax_included = tax_det["is_tax_included"]

        # ── GL classification: AI value wins, classifier fills blanks. ──
        ai_gl = str(raw_line.get("gl_account") or "").strip()
        ai_category = str(raw_line.get("category") or "").strip()
        ai_is_capital_raw = raw_line.get("is_capital")
        ai_capital_notes = str(raw_line.get("capital_notes") or "").strip()

        det = classify_line_gl(raw_line, vendor_name=vendor_name)
        gl_account = ai_gl or det["gl_account"]
        category = ai_category or det["category"]
        if ai_is_capital_raw is None:
            is_capital_bool = bool(det["is_capital"])
        else:
            is_capital_bool = bool(ai_is_capital_raw)
        capital_notes = ai_capital_notes or det["capital_notes"]
        # If classifier flagged extra notes (e.g., alcohol warning) and the AI
        # didn't already include them, append.
        for _en in det.get("extra_notes", []):
            if _en and _en not in notes:
                notes = (notes + " | " + _en).strip(" |")

        line_record = {
            "document_id": document_id,
            "line_number": line_num,
            "description": description,
            "quantity": quantity,
            "unit_price": unit_price,
            # line_total stays on the dict so the downstream
            # process_line_items step (which calls calculate_line_tax with
            # this dict) can read it. line_total_pretax mirrors what's
            # actually persisted in the row.
            "line_total": float(line_total) if line_total else None,
            "line_total_pretax": float(line_total) if line_total else None,
            "tax_indicator": tax_indicator,
            "tax_amount_shown": tax_amount_shown,
            "is_tax_included": 1 if is_tax_included else (0 if is_tax_included is False else None),
            "line_notes": notes,
            "created_at": now,
            "gl_account": gl_account,
            "category": category,
            "is_capital": 1 if is_capital_bool else 0,
            "capital_notes": capital_notes,
        }

        conn.execute(
            """INSERT INTO invoice_lines
               (document_id, line_number, description, quantity, unit_price,
                line_total_pretax, is_tax_included, line_notes, created_at,
                gl_account, category, is_capital, capital_notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                document_id, line_num, description, quantity, unit_price,
                line_record["line_total_pretax"],
                line_record["is_tax_included"],
                notes, now,
                gl_account, category,
                line_record["is_capital"],
                capital_notes,
            ),
        )
        stored_lines.append(line_record)

    # Update document flags
    has_lines = 1 if stored_lines else 0
    conn.execute(
        """UPDATE documents
           SET has_line_items = ?,
               deposit_allocated = ?
           WHERE document_id = ?""",
        (has_lines, 1 if deposit_found else 0, document_id),
    )
    conn.commit()

    return stored_lines


def _ensure_invoice_lines_table(conn: sqlite3.Connection) -> None:
    """Create invoice_lines table if missing (runtime safety net).

    Also adds any GL classification columns that may be missing on older DBs.
    Adding columns is idempotent: ALTER TABLE failures (column exists) are
    swallowed, mirroring the production migration in scripts/review_dashboard.py.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS invoice_lines (
            line_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id      TEXT NOT NULL,
            line_number      INTEGER NOT NULL,
            description      TEXT,
            quantity         REAL,
            unit_price       REAL,
            line_total_pretax REAL,
            tax_code         TEXT,
            tax_regime       TEXT,
            gst_amount       REAL,
            qst_amount       REAL,
            hst_amount       REAL,
            province_of_supply TEXT,
            is_tax_included  INTEGER,
            line_notes       TEXT,
            created_at       TEXT NOT NULL DEFAULT '',
            gl_account       TEXT,
            category         TEXT,
            is_capital       INTEGER DEFAULT 0,
            capital_notes    TEXT
        )
    """)
    # Backfill columns on pre-existing tables.
    for _col_def in (
        "gl_account TEXT",
        "category TEXT",
        "is_capital INTEGER DEFAULT 0",
        "capital_notes TEXT",
    ):
        try:
            conn.execute(f"ALTER TABLE invoice_lines ADD COLUMN {_col_def}")
        except sqlite3.OperationalError:
            pass
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_invoice_lines_doc "
        "ON invoice_lines(document_id)"
    )


# ---------------------------------------------------------------------------
# RECONCILIATION
# ---------------------------------------------------------------------------

def reconcile_invoice_lines(
    document_id: str,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Reconcile line totals against the invoice total.

    1. Sum all line pretax amounts
    2. Sum all line tax amounts
    3. Compare to invoice total — flag gap if difference > $0.02
    4. If gap: lines_reconciled=False, invoice_total_gap=gap_amount
    5. If no gap: lines_reconciled=True

    Returns dict with: line_sum, tax_sum, invoice_total, gap, reconciled.
    """
    rows = conn.execute(
        """SELECT line_total_pretax, gst_amount, qst_amount, hst_amount
           FROM invoice_lines WHERE document_id = ?""",
        (document_id,),
    ).fetchall()

    line_sum = _ZERO
    tax_sum = _ZERO
    for r in rows:
        line_sum += _to_dec(r[0])
        tax_sum += _to_dec(r[1]) + _to_dec(r[2]) + _to_dec(r[3])

    # Get invoice total from documents table
    doc_row = conn.execute(
        "SELECT amount FROM documents WHERE document_id = ?",
        (document_id,),
    ).fetchone()
    invoice_total = _to_dec(doc_row[0]) if doc_row and doc_row[0] else _ZERO

    total_computed = line_sum + tax_sum
    gap = _round(abs(total_computed - invoice_total))
    reconciled = gap <= Decimal("0.02")

    # Update document
    conn.execute(
        """UPDATE documents
           SET lines_reconciled = ?,
               line_total_sum = ?,
               invoice_total_gap = ?
           WHERE document_id = ?""",
        (
            1 if reconciled else 0,
            float(line_sum),
            float(gap) if not reconciled else 0.0,
            document_id,
        ),
    )
    conn.commit()

    return {
        "line_sum": float(line_sum),
        "tax_sum": float(tax_sum),
        "total_computed": float(total_computed),
        "invoice_total": float(invoice_total),
        "gap": float(gap),
        "reconciled": reconciled,
    }


# ---------------------------------------------------------------------------
# DEPOSIT ALLOCATION
# ---------------------------------------------------------------------------

def allocate_deposit_proportionally(
    document_id: str,
    deposit_amount: float | Decimal,
    conn: sqlite3.Connection,
) -> dict[str, Any]:
    """Allocate a deposit proportionally across invoice lines.

    1. Calculate each line's share of total pretax value
    2. Allocate deposit proportionally
    3. Recalculate tax recovery net of deposit allocation per line
    4. Return per-line deposit allocation and adjusted ITC/ITR

    Returns dict with:
        total_pretax, deposit, allocations: list[dict]
    """
    deposit = _to_dec(deposit_amount)

    rows = conn.execute(
        """SELECT line_id, line_number, description, line_total_pretax,
                  gst_amount, qst_amount, hst_amount, tax_code
           FROM invoice_lines WHERE document_id = ?
           ORDER BY line_number""",
        (document_id,),
    ).fetchall()

    total_pretax = sum(_to_dec(r[3]) for r in rows)
    if total_pretax <= _ZERO:
        return {"total_pretax": 0, "deposit": float(deposit), "allocations": []}

    allocations: list[dict[str, Any]] = []
    for r in rows:
        line_pretax = _to_dec(r[3])
        share = line_pretax / total_pretax if total_pretax > _ZERO else _ZERO
        line_deposit = _round(deposit * share)
        net_pretax = line_pretax - line_deposit

        # Recalculate recoverable tax on net amount
        gst_orig = _to_dec(r[4])
        qst_orig = _to_dec(r[5])
        hst_orig = _to_dec(r[6])

        # Proportional reduction of tax recovery
        if line_pretax > _ZERO:
            reduction_factor = net_pretax / line_pretax
        else:
            reduction_factor = _ZERO

        adj_gst = _round(gst_orig * reduction_factor)
        adj_qst = _round(qst_orig * reduction_factor)
        adj_hst = _round(hst_orig * reduction_factor)

        allocations.append({
            "line_id": r[0],
            "line_number": r[1],
            "description": r[2],
            "original_pretax": float(line_pretax),
            "deposit_allocated": float(line_deposit),
            "net_pretax": float(net_pretax),
            "adjusted_gst_recovery": float(adj_gst),
            "adjusted_qst_recovery": float(adj_qst),
            "adjusted_hst_recovery": float(adj_hst),
        })

    # Mark document as deposit-allocated
    conn.execute(
        "UPDATE documents SET deposit_allocated = 1 WHERE document_id = ?",
        (document_id,),
    )
    conn.commit()

    return {
        "total_pretax": float(total_pretax),
        "deposit": float(deposit),
        "allocations": allocations,
    }


# ---------------------------------------------------------------------------
# DOCAI LINE-ITEM PROCESSING
# ---------------------------------------------------------------------------


def _process_docai_line_items(
    document_id: str,
    items: list[dict],
    vendor_name: str,
    raw_ocr_text: str,
    db_path: Path,
    vendor_province: str = "QC",
    buyer_province: str = "QC",
) -> dict[str, Any]:
    """Process line items extracted by Google DocAI.

    DocAI provides exact descriptions and amounts from the receipt image.
    Claude Haiku classifies GL account and tax code only — no extraction needed.
    Then the standard per-line tax pipeline (place of supply, regime, calculation)
    runs on each line.
    """
    import anthropic
    import os
    from dotenv import load_dotenv
    load_dotenv()

    # Ask Claude to classify GL and tax code only
    items_text = '\n'.join([
        f'{i+1}. "{item["description"]}" amount={item["total_price"]}'
        for i, item in enumerate(items)
    ])

    prompt = f"""Quebec CPA. Classify each item with gl_account and tax_code only.
Vendor: {vendor_name}

Items:
{items_text}

GL: 5400=telecom 5410=utilities 5420=software 5430=supplies/groceries
5440=general 5500=bank 5640=meals 5750=repairs 1820=equipment>500

TAX: Z=zero-rated grocery (fresh produce eggs dairy meat bread)
T=taxable (snacks prepared food household items)
M=meals/restaurant E=exempt

Return JSON only:
[{{"line": 1, "gl_account": "5430", "tax_code": "Z"}}]"""

    classifications: dict[int, dict] = {}
    try:
        key = os.environ.get('ANTHROPIC_API_KEY', '')
        client = anthropic.Anthropic(api_key=key)
        msg = client.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=1000,
            messages=[{'role': 'user', 'content': prompt}],
        )
        text = msg.content[0].text
        text = re.sub(r'```json\s*', '', text)
        text = re.sub(r'```\s*', '', text)
        for cls in json.loads(text.strip()):
            classifications[cls['line']] = cls
    except Exception as e:
        logging.warning(f'Claude classification failed: {e}')

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        # Clear old lines
        conn.execute('DELETE FROM invoice_lines WHERE document_id = ?', (document_id,))

        for i, item in enumerate(items):
            cls = classifications.get(i + 1, {})
            gl = cls.get('gl_account', '5440')
            tax_code = cls.get('tax_code', 'T')
            desc_lc = str(item.get('description', '')).lower()

            # Zero-rated grocery override
            if _line_is_zero_rated_grocery(desc_lc):
                tax_code = 'Z'

            # E→Z correction for groceries
            if tax_code == 'E' and _line_is_zero_rated_grocery(desc_lc):
                tax_code = 'Z'

            # Build a line dict compatible with the standard tax pipeline
            # (calculate_line_tax reads "line_total")
            line = {
                'line_number': i + 1,
                'description': item['description'],
                'quantity': item.get('quantity', 1.0),
                'unit_price': item.get('unit_price'),
                'line_total': item['total_price'],
                'gl_account': gl,
                'tax_code': tax_code,
                'category': 'meals' if gl == '5640' else '',
            }

            pos = determine_place_of_supply(line, vendor_province, buyer_province)
            regime = assign_line_tax_regime(line, pos)

            # Override tax_code with regime if not already set to a special code
            if tax_code not in ('Z', 'M'):
                tax_code = regime.get('tax_code', tax_code)
            if gl == '5640':
                tax_code = 'M'

            tax_det = detect_tax_included_per_line(line)
            is_tax_included = tax_det['is_tax_included']
            tax = calculate_line_tax(line, regime, is_tax_included)

            if tax_code == 'Z':
                tax['gst'] = _ZERO
                tax['qst'] = _ZERO
                tax['hst'] = _ZERO

            # Phantom-tax guard
            tax, tax_code = validate_line_tax(tax, tax_code, raw_ocr_text)

            conn.execute('''
                INSERT INTO invoice_lines
                (document_id, line_number, description, quantity, unit_price,
                 line_total_pretax, gl_account, tax_code, gst_amount, qst_amount,
                 hst_amount, province_of_supply, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ''', (
                document_id, i + 1,
                item['description'],
                item.get('quantity', 1.0),
                item.get('unit_price'),
                float(tax['pretax_amount']),
                gl, tax_code,
                float(tax['gst']),
                float(tax['qst']),
                float(tax.get('hst', _ZERO)),
                pos,
            ))

        # Mark document as having line items
        conn.execute('UPDATE documents SET has_line_items = 1 WHERE document_id = ?', (document_id,))
        conn.commit()

        # Reconcile
        recon = reconcile_invoice_lines(document_id, conn)
        logging.info(f'Saved {len(items)} DocAI line items for {document_id}')

        return {
            'ok': True,
            'document_id': document_id,
            'lines_extracted': len(items),
            'reconciliation': recon,
            'source': 'docai',
        }

    except Exception as exc:
        return {
            'ok': False,
            'document_id': document_id,
            'error': str(exc),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# FULL LINE PROCESSING PIPELINE
# ---------------------------------------------------------------------------

def process_line_items(
    document_id: str,
    raw_ocr_text: str,
    vendor_province: str = "QC",
    buyer_province: str = "QC",
    *,
    vendor_name: str = "",
    db_path: Path = DB_PATH,
    file_path: str | Path | None = None,
) -> dict[str, Any]:
    """Run the full line-item pipeline for a document.

    1. Try Google DocAI line item extraction first (if file_path provided)
    2. Fall back to AI extraction from OCR text
    3. For each line: determine place of supply, assign tax regime, calculate tax
    4. Store results in invoice_lines
    5. Reconcile against invoice total

    *vendor_name* is forwarded to extract_invoice_lines so the deterministic
    GL classifier can apply restaurant/hardware-store rules per line.

    Returns summary dict.
    """
    # Priority 0: Adaptive Spatial Grouping Engine — uses word-level
    # bounding boxes from DocAI for production-quality receipt parsing.
    spatial_items: list[dict] = []
    if file_path and Path(str(file_path)).exists():
        try:
            from src.engines.google_docai import extract_words_with_bbox
            from src.engines.receipt_spatial_engine import ReceiptSpatialEngine

            words = extract_words_with_bbox(file_path)
            if words:
                engine = ReceiptSpatialEngine()
                parsed = engine.parse(words)

                for structure in parsed.structures:
                    if structure.structure_type == 'ITEM_CANDIDATE':
                        parent = structure.parent_line
                        prices = parent.metadata.get('prices', [])
                        if not prices:
                            continue

                        amount = prices[-1]  # Last price is usually the total
                        item: dict[str, Any] = {
                            'description': parent.raw_text,
                            'quantity': parent.metadata.get('qty') or 1.0,
                            'unit_price': amount,
                            'total_price': amount,
                            'confidence': structure.confidence,
                        }

                        # Handle weighted items
                        weight = parent.metadata.get('weight')
                        at_price = parent.metadata.get('at_price')
                        if weight and at_price:
                            item['quantity'] = weight
                            item['unit_price'] = at_price

                        # Attach child discount
                        for child in structure.child_lines:
                            if child.line_type == 'DISCOUNT':
                                child_prices = child.metadata.get('prices', [])
                                if child_prices:
                                    discount_amount = child_prices[-1]
                                    item['total_price'] = amount - discount_amount

                        spatial_items.append(item)

                logging.info(f'Spatial engine found {len(spatial_items)} items for {document_id}')
        except Exception as e:
            logging.warning(f'Spatial engine failed: {e}')

    if len(spatial_items) >= 2:
        return _process_docai_line_items(
            document_id, spatial_items, vendor_name, raw_ocr_text, db_path,
            vendor_province, buyer_province,
        )

    # Priority 1: Parse clean OCR text from DocAI with regex (most reliable
    # for grocery/retail receipts — no hallucinated descriptions, no
    # collapsed duplicates).
    ocr_parsed_items: list[dict] = []
    if file_path:
        try:
            from src.engines.google_docai import extract_line_items_from_ocr_text
            ocr_parsed_items = extract_line_items_from_ocr_text(file_path)
            logging.info(f'OCR text parser found {len(ocr_parsed_items)} line items for {document_id}')
        except Exception as e:
            logging.warning(f'OCR text parsing failed: {e}')

    if len(ocr_parsed_items) >= 3:
        return _process_docai_line_items(
            document_id, ocr_parsed_items, vendor_name, raw_ocr_text, db_path,
            vendor_province, buyer_province,
        )

    # Priority 2: DocAI entity extraction (structured but misses items)
    docai_items: list[dict] = []
    if file_path:
        try:
            from src.engines.google_docai import extract_line_items_from_docai
            docai_items = extract_line_items_from_docai(file_path)
            logging.info(f'DocAI entities found {len(docai_items)} line items for {document_id}')
        except Exception as e:
            logging.warning(f'DocAI line items failed: {e}')

    if len(docai_items) >= 2:
        return _process_docai_line_items(
            document_id, docai_items, vendor_name, raw_ocr_text, db_path,
            vendor_province, buyer_province,
        )

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        # Step 1: AI extraction (with vendor for GL classifier)
        lines = extract_invoice_lines(document_id, raw_ocr_text, conn, vendor_name=vendor_name)

        # Step 2: Per-line tax processing
        for line in lines:
            # Place of supply
            pos = determine_place_of_supply(line, vendor_province, buyer_province)

            # Tax regime
            regime = assign_line_tax_regime(line, pos)

            # Tax-included detection
            tax_det = detect_tax_included_per_line(line)
            is_tax_included = tax_det["is_tax_included"]

            # Calculate tax (calculate_line_tax inspects gl_account/category to
            # apply 50% recovery on meals).
            tax = calculate_line_tax(line, regime, is_tax_included)

            # Tax code override: meals (alcohol/restaurant) → "M" so the
            # downstream tax_code consumer applies the 50% restriction even if
            # the regime resolved to plain "T".
            tax_code = regime.get("tax_code", "")
            if (
                str(line.get("category", "")).lower() == "meals"
                or str(line.get("gl_account", "")) == "5640"
            ):
                tax_code = "M"

            # Zero-rated basic groceries (fresh produce, eggs, dairy, meat,
            # fish, bread/cereals): no GST/QST regardless of province. This
            # runs after the meals override so alcohol/prepared meals aren't
            # affected (alcohol items never match the grocery keyword list).
            desc_lc = str(line.get("description", "")).lower()
            if _line_is_zero_rated_grocery(desc_lc):
                tax_code = "Z"
                tax["gst"] = _ZERO
                tax["qst"] = _ZERO
                tax["hst"] = _ZERO

            # Phantom-tax guard: if the per-line GST amount doesn't appear
            # verbatim in the OCR text, it was calculated rather than read
            # off the receipt — zero it out and downgrade to Z.
            tax, tax_code = validate_line_tax(tax, tax_code, raw_ocr_text)

            # Build deduplicated notes: keep existing + add regime note once
            regime_note = regime.get("notes", "")
            existing_notes = str(line.get("line_notes", "") or "")
            parts = [p.strip() for p in existing_notes.split("|") if p.strip()]
            if regime_note:
                parts.append(regime_note)
            # Deduplicate while preserving order
            parts = list(dict.fromkeys(parts))
            updated_notes = " | ".join(parts)

            # Strip redundant notes that just restate what tax_code already
            # conveys — these clutter the UI without adding information.
            if updated_notes:
                updated_notes = re.sub(r'zero-rated basic grocery\s*\|?\s*', '', updated_notes)
                updated_notes = re.sub(r'Line marked exempt\s*\|?\s*', '', updated_notes)
                updated_notes = re.sub(r'Quebec: GST 5% \+ QST 9\.975%\s*\|?\s*', '', updated_notes)
                # Strip AI-generated tax regime descriptions that duplicate
                # the tax_code field — e.g. "marked D (taxable)" or
                # "marked E (exempt from tax)".
                updated_notes = re.sub(r',?\s*marked [A-Z] \([^)]*\)\s*\|?\s*', '', updated_notes)
                # Strip per-line tax calculation descriptions
                updated_notes = re.sub(r'(?:Federal )?GST/TPS calculated at [\d.]+%[^|]*\|?\s*', '', updated_notes)
                updated_notes = re.sub(r'Quebec provincial sales tax calculated at [\d.]+%[^|]*\|?\s*', '', updated_notes)
                updated_notes = re.sub(r'HST \d+% applied at invoice level\s*\|?\s*', '', updated_notes)
                updated_notes = re.sub(r'\|\s*\|', '|', updated_notes)
                updated_notes = updated_notes.strip('| \t\n')
                if not updated_notes:
                    updated_notes = ""

            # E→Z correction: AI sometimes returns E (exempt) for groceries
            # which should be Z (zero-rated). E and Z have different ITC/ITR
            # implications — groceries are zero-rated, not exempt.
            if tax_code == "E" and _line_is_zero_rated_grocery(desc_lc):
                tax_code = "Z"
                tax["gst"] = _ZERO
                tax["qst"] = _ZERO
                tax["hst"] = _ZERO

            # Update invoice_lines row (gl_account/category/is_capital/
            # capital_notes were already set during the INSERT in
            # extract_invoice_lines and intentionally NOT overwritten here).
            conn.execute(
                """UPDATE invoice_lines
                   SET tax_code = ?,
                       tax_regime = ?,
                       gst_amount = ?,
                       qst_amount = ?,
                       hst_amount = ?,
                       province_of_supply = ?,
                       line_total_pretax = ?,
                       line_notes = ?
                   WHERE document_id = ? AND line_number = ?""",
                (
                    tax_code,
                    regime.get("tax_regime", ""),
                    float(tax["gst"]),
                    float(tax["qst"]),
                    float(tax["hst"]),
                    pos,
                    float(tax["pretax_amount"]),
                    updated_notes,
                    document_id,
                    line.get("line_number", 0),
                ),
            )

        conn.commit()

        # Step 3: Reconcile
        recon = reconcile_invoice_lines(document_id, conn)

        return {
            "ok": True,
            "document_id": document_id,
            "lines_extracted": len(lines),
            "reconciliation": recon,
        }

    except Exception as exc:
        return {
            "ok": False,
            "document_id": document_id,
            "error": str(exc),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MULTI-LINE INVOICE DETECTION (used by ocr_engine integration)
# ---------------------------------------------------------------------------

_LINE_ITEM_KEYWORDS = re.compile(
    r"qty|quantity|quantité|unit\s*price|prix\s*unitaire|"
    r"item\s*#|line\s*#|description.*amount|"
    r"subtotal.*total|sous-total|shipping|livraison|"
    r"item\s+description|no\.\s*article",
    re.IGNORECASE,
)

_MULTI_AMOUNT_RE = re.compile(r"\$?\d{1,3}(?:[,\s]\d{3})*(?:\.\d{2})")

# Barcode/UPC-like pattern on an item line: a 10+ digit run followed by a
# dollar amount on the same line (e.g. "SHORTSETS 073077268457 10.00 J").
# Walmart and other grocery/retail receipts use this format and rarely carry
# the explicit "qty/quantity/unit price" keywords the original heuristic
# looked for, so they need their own trigger.
_BARCODE_LINE_RE = re.compile(
    r"\d{10,}\s+\$?\d{1,3}(?:[,\s]\d{3})*\.\d{2}"
)

# Vendors that almost always have itemised receipts worth classifying line by
# line — restaurants/bars (mixed food + alcohol GL), hardware stores
# (mixed supply + capital GL) and big-box / grocery retailers (mixed supply +
# personal expense risk). Match is substring + case-insensitive.
_LINE_ITEM_VENDOR_HINTS = (
    # Restaurants / bars
    "saloon", "tavern", "taverne", "pub", "lounge", "nightclub",
    "brasserie", "bistro", "restaurant", "bar ", "café", "cafe",
    "deli", "diner", "grill", "pizzeria", "sushi", "buffet",
    # Hardware / building supply
    "reno-depot", "renodepot", "rénovation", "home depot",
    "homedepot", "rona", "canadian tire", "lowes", "lowe's",
    "ace hardware", "patrick morin", "matériaux",
    # Big-box / grocery / retail (mixed supply + personal expense risk)
    "walmart", "costco", "target", "superstore", "real canadian",
    "loblaws", "loblaw", "maxi", "iga", "metro ", "sobeys",
    "provigo", "super c", "superc", "adonis", "avril",
    "pa ", "pa nature", "bulk barn", "dollarama", "dollar tree",
)


def looks_like_multiline_invoice(raw_text: str, vendor_name: str = "") -> bool:
    """Heuristic: should we run line-item extraction on this document?

    Triggers when ANY of the following hold:
    - The text has line-item keywords AND ≥3 distinct dollar amounts
      (the original "structured invoice" path).
    - The vendor is a known restaurant/bar/hardware/grocery/big-box store
      (mixed-GL receipts that benefit most from per-line classification)
      AND the text has ≥2 dollar amounts.
    - The text has ≥2 dollar amounts AND looks like an itemised receipt
      (subtotal/sub-total/sous-total marker).
    - The text has ≥5 distinct dollar amounts (retail receipt pattern —
      most grocery/big-box receipts have no explicit "qty/unit price"
      keywords but list one amount per item).
    - The text has ≥2 barcode-like item lines (long digit run followed
      by a dollar amount on the same line). Catches Walmart-style
      "SHORTSETS 073077268457 10.00 J" rows even when the OCR drops the
      SUBTOTAL marker.
    """
    if not raw_text:
        return False
    amounts = _MULTI_AMOUNT_RE.findall(raw_text)
    n_amounts = len(amounts)
    # Distinct amounts matter more than the raw count: "10.00 J" appearing
    # five times in a Walmart "SHORTSETS" run should still count as five
    # separate line items, but duplicate totals ("TOTAL 98.95 / PAID 98.95")
    # shouldn't inflate the signal for two-line receipts.
    n_distinct_amounts = len(set(amounts))

    has_keywords = bool(_LINE_ITEM_KEYWORDS.search(raw_text))
    if has_keywords and n_amounts >= 3:
        return True

    vendor_lc = (vendor_name or "").lower()
    text_lc = raw_text.lower()
    vendor_hint = any(kw in vendor_lc for kw in _LINE_ITEM_VENDOR_HINTS) or any(
        kw in text_lc for kw in _LINE_ITEM_VENDOR_HINTS
    )
    if vendor_hint and n_amounts >= 2:
        return True

    has_subtotal_marker = bool(re.search(
        r"sub[-\s]?total|sous[-\s]?total|s/?total|total\s+ttc|tps|tvq",
        text_lc,
    ))
    if has_subtotal_marker and n_amounts >= 2:
        return True

    # Retail receipt pattern: many items but no structured keywords.
    if n_distinct_amounts >= 5:
        return True

    # Walmart-style "DESCRIPTION BARCODE AMOUNT" rows.
    if len(_BARCODE_LINE_RE.findall(raw_text)) >= 2:
        return True

    return False


# ---------------------------------------------------------------------------
# PERSONAL-ITEM DETECTION (apparel / personal-use keywords on a business
# receipt). Runs on the raw OCR text so we don't depend on the AI
# line-extraction succeeding. A hit sets substance_flags.potential_personal_
# expense so the existing review_policy re-run downgrades the document to
# NeedsReview.
# ---------------------------------------------------------------------------

PERSONAL_ITEM_KEYWORDS = (
    "shortset", "swimwear", "swimsuit", "clothing", "shirt", "shorts",
    "dress", "pants", "shoes", "socks", "underwear", "tee", "jean",
    "hat", "cap", "jacket", "coat", "sweater", "hoodie",
)

# Precompile as word-boundary regex so "teeth", "capacitor", "jeans" etc.
# still hit sensibly ("jeans" → "jean", "tees" → "tee") without matching
# unrelated substrings like "capital" for "cap".
_PERSONAL_ITEM_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in PERSONAL_ITEM_KEYWORDS) + r")s?\b",
    re.IGNORECASE,
)


def detect_personal_items(raw_text: str) -> list[str]:
    """Return the list of distinct personal-item keywords found in *raw_text*.

    Empty list = no personal items detected. Callers typically treat any
    non-empty result as a NeedsReview trigger on business receipts.
    """
    if not raw_text:
        return []
    hits: list[str] = []
    seen: set[str] = set()
    for match in _PERSONAL_ITEM_RE.finditer(raw_text):
        kw = match.group(1).lower()
        if kw not in seen:
            seen.add(kw)
            hits.append(kw)
    return hits


# =========================================================================
# PART 2 (line_item_engine) — False precision prevention
# =========================================================================

def analyze_line_allocation_gap(
    invoice_total: Any,
    cbsa_goods_value: Any,
    invoice_text: str = "",
) -> dict[str, Any]:
    """Analyze gap between invoice total and CBSA documented goods value.

    Delegates to customs_engine.analyze_allocation_gap for consistency.
    """
    from src.engines.customs_engine import analyze_allocation_gap
    return analyze_allocation_gap(invoice_total, cbsa_goods_value, invoice_text)
