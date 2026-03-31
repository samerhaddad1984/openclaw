"""
src/engines/substance_engine.py
===============================
Economic substance classifier for OtoCPA.

Detects CapEx, prepaids, loans, tax remittances, and shareholder/personal
expenses from vendor name, memo, doc_type, and amount.  Runs deterministically
first; when keyword detection is inconclusive, falls back to AI-assisted
classification via ai_router.
"""
from __future__ import annotations

import json
import logging
import re
import unicodedata
from typing import Any

log = logging.getLogger(__name__)


def _strip_accents(text: str) -> str:
    """NFKD + ASCII normalization: 'équipement' → 'equipement'."""
    return unicodedata.normalize("NFKD", text).encode("ascii", errors="ignore").decode("ascii")


def _normalize(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Keyword sets (bilingual FR/EN)
# ---------------------------------------------------------------------------

_CAPEX_KEYWORDS = re.compile(
    r"\b("
    r"equipments?|équipements?|machinery|machines?|machinerie|"
    r"véhicules?|vehicules?|vehicles?|trucks?|trailers?|forklifts?|cranes?|"
    r"ordinateurs?|computers?|laptops?|serveurs?|servers?|racks?|"
    r"logiciels?|software|generators?|génératrices?|"
    r"immeubles?|buildings?|terrains?|land|rénovations?|renovations?|"
    r"mobiliers?|furniture|outillage|tooling|imprimantes?|printers?|"
    r"construction|amélioration locative|leasehold improvement|"
    # FIX 2 (nightmare): HVAC / climate control CapEx keywords
    r"hvac|système hvac|systeme hvac|remplacement hvac|"
    r"air climatisé|air climatise|chauffage|ventilation|climatisation"
    r")\b",
    re.IGNORECASE,
)

# FIX 3 (nightmare): "remplacement complet" — strong CapEx signal that overrides
# repair/maintenance negative keywords when combined with equipment/system references.
_REPLACEMENT_KEYWORDS = re.compile(
    r"\b("
    r"remplacement complet|full replacement|complete replacement|"
    r"remplacement total|remplacé en entier|remplace en entier|"
    r"replaced entirely"
    r")\b",
    re.IGNORECASE,
)

# FIX 8: Negative keywords that override CapEx detection
_CAPEX_NEGATIVE = re.compile(
    r"("
    r"\b(?:réparation|reparation|repair|dépannage|depannage|troubleshoot)\b|"
    r"\b(?:entretien|maintenance|nettoyage|cleaning)\b|"
    r"\b(?:saas|as-a-service)\b|"
    r"\b(?:monthly|mensuel)\s+(?:subscription|abonnement)\b"
    r")",
    re.IGNORECASE,
)

_PREPAID_KEYWORDS = re.compile(
    r"\b("
    r"assurance|insurance|"
    r"loyer.*avance|advance.*rent|prepaid.*rent|"
    r"abonnement annuel|annual subscription|"
    r"abonnement|subscription|"
    r"prime d'assurance|insurance premium|"
    r"prépayé|prepaid"
    r")\b",
    re.IGNORECASE,
)

# FIX 8: Negative keywords that override prepaid/insurance detection
_PREPAID_NEGATIVE = re.compile(
    r"\b("
    r"assurance qualité|quality assurance|"
    r"qa\b|contrôle qualité|quality control"
    r")\b",
    re.IGNORECASE,
)

_LOAN_KEYWORDS = re.compile(
    r"\b("
    r"prêt|pret|loan|financement|financing|"
    r"hypothèque|hypotheque|mortgage|"
    r"emprunt|borrowing|marge de crédit|line of credit|"
    r"crédit-bail|credit bail|capital lease"
    r")\b",
    re.IGNORECASE,
)

# FIX 8: Negative keywords/phrases that override loan detection
_LOAN_NEGATIVE = re.compile(
    r"("
    r"prêt-à-porter|pret-a-porter|ready.to.wear|"
    r"bibliothèque|bibliotheque|library|"
    r"emprunt de livres|book borrowing|"
    r"emprunt de matériel|equipment borrowing"
    r")",
    re.IGNORECASE,
)

_TAX_REMITTANCE_KEYWORDS = re.compile(
    r"\b("
    r"tps|gst|tvq|qst|hst|tvh|"
    r"das|source deductions|retenues à la source|"
    r"cnesst|csst|hsf|fss|"
    r"remise|remittance|acompte provisionnel|instalment"
    r")\b",
    re.IGNORECASE,
)

_PERSONAL_KEYWORDS = re.compile(
    r"\b("
    r"épicerie personnelle|personal groceries|"
    r"vêtements|clothing|personal|personnel|"
    r"netflix|spotify|gym|fitness|"
    r"vacances|vacation|voyage personnel|personal travel|"
    r"amazon personal|restaurant personnel"
    r")\b",
    re.IGNORECASE,
)

# FIX 8: B2B streaming/software vendors that are NOT personal expenses
# Only match explicit B2B service context, not just corporate suffixes
# FIX 21/26: French HR department phrases that are NOT personal expenses
_PERSONAL_NEGATIVE = re.compile(
    r"("
    r"production services|services de production|"
    r"enterprise|entreprise|"
    r"b2b|commercial license|licence commerciale|"
    r"software subscription|abonnement logiciel|"
    r"equipment rental|location d'équipement|"
    # FIX P1-3: Expanded HR/staffing phrases — singular AND plural variants
    r"services? du personnel|services? de personnel|"
    r"personnel temporaires?|temporaires? personnel|"
    r"employés? temporaires?|"
    r"main[- ]d[''']?(?:oe|œ)uvre(?:\s+temporaire)?|"
    r"agence de placement|"
    r"ressources humaines|département des? ressources|département rh|"
    r"service rh|gestion rh|"
    r"\brh\b|gestion du personnel|"
    r"agence de personnel|human resources|"
    r"staffing|temp staff"
    r")",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# GL account suggestions
# ---------------------------------------------------------------------------

_CAPEX_GL_RANGE = "1500"          # Fixed assets (1500-1599)
_PREPAID_GL = "1300"              # Prepaid expenses
_LOAN_GL = "2500"                 # Long-term liabilities
_TAX_LIABILITY_GL_TPS = "2200"    # GST payable
_TAX_LIABILITY_GL_TVQ = "2210"    # QST payable (matches chart of accounts)
_TAX_LIABILITY_GL_DAS = "2215"    # Source deductions payable
_DEPOSIT_GL = "1400"              # Security deposits / other assets
_GIFT_CARD_GL = "5400"            # Employee Benefits Expense

# FIX 19: Bank name heuristic keywords for large wire transfer detection
_BANK_NAME_KEYWORDS = re.compile(
    r"\b("
    r"bank|banque|financial|financière|financiere|"
    r"trust|credit union|caisse populaire|caisse desjardins|"
    r"bdc|national bank|banque nationale|"
    r"royal bank|rbc|td bank|bmo|cibc|scotiabank|"
    r"banque de développement|development bank"
    r")\b",
    re.IGNORECASE,
)

# FIX 20: Known CapEx vendors (tech hardware manufacturers)
_CAPEX_VENDORS = re.compile(
    r"\b("
    r"dell|hewlett.?packard|hp\b|lenovo|apple|cisco|ibm|xerox"
    r")\b",
    re.IGNORECASE,
)
_CAPEX_VENDOR_THRESHOLD = 1500.0

_DEPOSIT_KEYWORDS = re.compile(
    r"\b("
    r"security deposit|dépôt de garantie|dépôt|deposit.*refundable|"
    r"refundable.*deposit|caution|"
    # FIX 1: Customer deposit / advance payment keywords
    r"dépôt client|customer deposit|advance deposit|"
    r"acompte|acompte client|down payment|"
    r"client advance|advance payment|deposit received|"
    r"dépôt reçu|paiement anticipé|"
    # FIX BLOCK2: Additional customer deposit keywords
    r"retainer|arrhes|avance"
    r")\b",
    re.IGNORECASE,
)

# FIX BLOCK2: Customer deposit GL — unearned revenue / deferred revenue liability
_CUSTOMER_DEPOSIT_GL = "2400"
_CUSTOMER_DEPOSIT_AMOUNT_THRESHOLD = 500.0

# FIX 1: Patterns that indicate a customer/advance deposit (liability) vs security deposit (asset)
_CUSTOMER_DEPOSIT_PATTERNS = re.compile(
    r"\b("
    r"dépôt client|customer deposit|advance deposit|"
    r"acompte|acompte client|client advance|advance payment|"
    r"deposit received|dépôt reçu|paiement anticipé|"
    r"projet|project|down payment|"
    # FIX BLOCK2: Additional customer deposit patterns
    r"retainer|arrhes|avance"
    r")\b",
    re.IGNORECASE,
)

# FIX 2: Intercompany transaction keywords
_INTERCOMPANY_KEYWORDS = re.compile(
    r"\b("
    r"intercompany|inter-company|interco|intersociété|inter-société|"
    r"intercompagnie|"
    r"management fees|frais de gestion|"
    r"group transfer|transfert groupe|"
    r"holding|société mère|parent company|"
    r"filiale|subsidiary|affiliated|affiliée|affiliated company|"
    r"société liée|"
    r"intra-group|intragroupe|"
    r"related party|partie liée|"
    # FIX BLOCK2: Additional intercompany keywords
    r"division|related entity"
    r")\b",
    re.IGNORECASE,
)
_INTERCOMPANY_GL = "1700"  # Due from/to related companies

# ---------------------------------------------------------------------------
# Priority override types — substance engine always wins for these
# ---------------------------------------------------------------------------

PRIORITY_OVERRIDE_TYPES = {"loan", "tax_remittance", "security_deposit", "personal_expense"}

_GIFT_CARD_KEYWORDS = re.compile(
    r"\b("
    r"gift cards?|carte.?cadeau|carte cadeau|"
    r"employee.*gifts?|cadeau.*employés?|"
    r"stored value|prepaid card"
    r")\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# FIX 1 (nightmare): Owner name abbreviation & reversed-order matching
# ---------------------------------------------------------------------------

def _extract_initials(token: str) -> str | None:
    """Extract initials from abbreviated tokens like 'J.P.', 'J-P', 'JP'.

    Returns uppercase initials string (e.g. 'JP') or None if not an abbreviation.
    """
    cleaned = token.strip().rstrip(".")
    # "j.p." or "j.p" → initials with dots
    if re.match(r'^[a-z]\.[a-z]\.?$', cleaned, re.IGNORECASE):
        return re.sub(r'\.', '', cleaned).upper()
    # "j-p" → initials with hyphen
    if re.match(r'^[a-z]-[a-z]$', cleaned, re.IGNORECASE):
        return cleaned.replace('-', '').upper()
    # "jp" → two bare initials (only if exactly 2 uppercase-able chars)
    if re.match(r'^[a-z]{2}$', cleaned, re.IGNORECASE) and cleaned == cleaned.lower():
        # Only match bare 2-letter tokens that are not common words
        return cleaned.upper()
    return None


def _name_part_matches(owner_part: str, vendor_token: str) -> bool:
    """Check if a single vendor token matches an owner name part.

    Handles exact match, hyphenated-name match, and abbreviation match.
    owner_part is already lowercased. vendor_token is already lowercased.
    """
    # Exact match
    if owner_part == vendor_token:
        return True

    # Abbreviation in vendor vs full owner part
    # e.g. vendor "j.p." vs owner part "jean-pierre"
    initials = _extract_initials(vendor_token)
    if initials:
        # Owner part may be hyphenated like "jean-pierre"
        # Extract first letters of each hyphen-separated segment
        segments = owner_part.split('-')
        owner_initials = ''.join(s[0].upper() for s in segments if s)
        if initials == owner_initials:
            return True

    # Full owner part is hyphenated, vendor has it without hyphen or vice versa
    if owner_part.replace('-', '') == vendor_token.replace('-', ''):
        return True

    return False


def _owner_name_matches_vendor(name_parts: list[str], vendor_lower: str) -> bool:
    """Check if multi-word owner name matches in vendor string.

    Supports:
    - Direct word-boundary matching (existing logic)
    - Abbreviation matching (J.P. → Jean-Pierre)
    - Reversed name order (Tremblay Jean-Pierre → Jean-Pierre Tremblay)
    """
    # Tokenize vendor into words for abbreviation matching
    vendor_tokens = re.findall(r"[a-z\u00e0-\u00ff][a-z\u00e0-\u00ff.\-']*", vendor_lower)

    # Try both original order and reversed order of name parts
    orderings = [name_parts]
    if len(name_parts) >= 2:
        orderings.append(list(reversed(name_parts)))

    for ordering in orderings:
        all_match = True
        for part in ordering:
            # First try exact word-boundary match (original logic)
            if re.search(r'\b' + re.escape(part) + r'\b', vendor_lower):
                continue
            # Then try abbreviation matching against vendor tokens
            if any(_name_part_matches(part, vt) for vt in vendor_tokens):
                continue
            all_match = False
            break
        if all_match:
            return True

    return False


# ---------------------------------------------------------------------------
# Main classifier
# ---------------------------------------------------------------------------

def substance_classifier(
    *,
    vendor: str = "",
    memo: str = "",
    doc_type: str = "",
    amount: Any = None,
    owner_names: list[str] | None = None,
    province: str = "",
) -> dict[str, Any]:
    """Classify economic substance of a transaction.

    Returns a dict with boolean flags and suggested GL overrides.
    """
    vendor_lower = _normalize(vendor)
    memo_lower = _normalize(memo)
    combined = f"{vendor_lower} {memo_lower} {_normalize(doc_type)}"
    amount_val = _safe_float(amount)

    # FIX 2 (nightmare): accent-normalized variant for keyword matching
    combined_ascii = _strip_accents(combined)

    flags: dict[str, Any] = {
        "potential_capex": False,
        "potential_prepaid": False,
        "potential_loan": False,
        "potential_tax_remittance": False,
        "potential_personal_expense": False,
        "potential_customer_deposit": False,
        "potential_intercompany": False,
        "mixed_tax_invoice": False,
        "suggested_gl": None,
        "review_notes": [],
        "block_auto_approval": False,
    }

    # CapEx detection (threshold $1,500 for software/equipment)
    # FIX 8: Negative keywords override CapEx detection (repair, maintenance, etc.)
    # FIX 2 (nightmare): search BOTH accented and ASCII-stripped text
    _has_capex_kw = bool(
        _CAPEX_KEYWORDS.search(combined) or _CAPEX_KEYWORDS.search(combined_ascii)
    )
    _has_capex_neg = bool(
        _CAPEX_NEGATIVE.search(combined) or _CAPEX_NEGATIVE.search(combined_ascii)
    )
    # FIX 3 (nightmare): "remplacement complet" + equipment/system → CapEx override
    _has_replacement = bool(
        _REPLACEMENT_KEYWORDS.search(combined) or _REPLACEMENT_KEYWORDS.search(combined_ascii)
    )

    if _has_capex_kw and _has_capex_neg and not _has_replacement:
        # SUBSTANCE CONFLICT: both CapEx and Expense signals present — flag for review
        flags["block_auto_approval"] = True
        flags["review_notes"].append(
            "Conflit de substance : signaux CapEx ET dépense détectés "
            "(ex. « équipement » vs « entretien ») — vérifier la nature économique / "
            "Substance conflict: both CapEx AND Expense signals detected "
            "(e.g. 'equipment' vs 'maintenance') — verify economic substance"
        )

    # FIX 3 (nightmare): replacement keywords override negative keywords —
    # "remplacement complet" with any equipment/system reference is CapEx
    if _has_capex_kw and _has_replacement:
        capex_threshold = 1500.0
        if amount_val is None or abs(amount_val) >= capex_threshold:
            flags["potential_capex"] = True
            flags["block_auto_approval"] = True
            if not flags["suggested_gl"]:
                flags["suggested_gl"] = _CAPEX_GL_RANGE
            flags["review_notes"].append(
                "Remplacement complet détecté — potentiel CapEx malgré fournisseur de réparation / "
                "Full replacement detected — potential CapEx despite repair vendor"
            )

    if _has_capex_kw and not _has_capex_neg and not flags["potential_capex"]:
        capex_threshold = 1500.0
        if amount_val is None or abs(amount_val) >= capex_threshold:
            flags["potential_capex"] = True
            flags["suggested_gl"] = _CAPEX_GL_RANGE
            flags["review_notes"].append(
                "Vérifier si immobilisation / Verify if capital asset"
            )

    # FIX 20: Vendor-based CapEx detection for known tech hardware vendors
    if not flags["potential_capex"] and _CAPEX_VENDORS.search(vendor_lower):
        if amount_val is not None and abs(amount_val) >= _CAPEX_VENDOR_THRESHOLD:
            flags["potential_capex"] = True
            if not flags["suggested_gl"]:
                flags["suggested_gl"] = _CAPEX_GL_RANGE
            flags["review_notes"].append(
                "Known CapEx vendor detected / Fournisseur d'immobilisations connu"
            )

    # Prepaid detection
    # FIX 8: Negative keywords override prepaid/insurance (QA, quality assurance)
    if _PREPAID_KEYWORDS.search(combined) and not _PREPAID_NEGATIVE.search(combined):
        flags["potential_prepaid"] = True
        if not flags["suggested_gl"]:
            flags["suggested_gl"] = _PREPAID_GL
        flags["review_notes"].append(
            "Vérifier si charge payée d'avance / Verify if prepaid expense"
        )

    # Loan / financing detection
    # FIX 8: Negative keywords override loan detection (prêt-à-porter, bibliothèque)
    if _LOAN_KEYWORDS.search(combined) and not _LOAN_NEGATIVE.search(combined):
        flags["potential_loan"] = True
        if not flags["suggested_gl"]:
            flags["suggested_gl"] = _LOAN_GL
        flags["review_notes"].append(
            "Vérifier si passif / Verify if liability — never expense"
        )
        # FIX 3 + BLOCK2: Add principal/interest split guidance for loan payments
        flags["review_notes"].append(
            "Paiement de prêt — vérifier la répartition capital/intérêts. "
            "Intérêts: GL 5480, Capital: GL 2500 / "
            "Loan payment — verify principal/interest split. "
            "Interest: GL 5480, Principal: GL 2500."
        )

    # FIX 19: Large wire transfer detection — bank name heuristic
    doc_type_lower = _normalize(doc_type)
    if (
        not flags["potential_loan"]
        and _BANK_NAME_KEYWORDS.search(vendor_lower)
        and amount_val is not None
        and abs(amount_val) > 10000
        and doc_type_lower != "invoice"
    ):
        flags["potential_loan"] = True
        flags["block_auto_approval"] = True
        if not flags["suggested_gl"]:
            flags["suggested_gl"] = _LOAN_GL
        flags["review_notes"].append(
            "Large wire from bank/financial institution — potential loan proceeds / "
            "Virement important d'une institution financière — produit de prêt potentiel"
        )

    # Tax remittance detection
    if _TAX_REMITTANCE_KEYWORDS.search(combined):
        flags["potential_tax_remittance"] = True
        if not flags["suggested_gl"]:
            # Try to pick the right sub-account
            if re.search(r"\btps\b|\bgst\b", combined):
                flags["suggested_gl"] = _TAX_LIABILITY_GL_TPS
            elif re.search(r"\btvq\b|\bqst\b", combined):
                flags["suggested_gl"] = _TAX_LIABILITY_GL_TVQ
            elif re.search(r"\bdas\b|\bsource deduction", combined):
                flags["suggested_gl"] = _TAX_LIABILITY_GL_DAS
            else:
                flags["suggested_gl"] = _TAX_LIABILITY_GL_TPS
        flags["review_notes"].append(
            "Vérifier si compensation de passif fiscal / Verify if tax liability clearing — never expense"
        )

    # Security deposit / customer deposit detection (FIX 1 + BLOCK2)
    # Customer deposits override other classifications (e.g. CapEx from vendor name)
    if _DEPOSIT_KEYWORDS.search(combined):
        flags["potential_prepaid"] = True
        if _CUSTOMER_DEPOSIT_PATTERNS.search(combined):
            # FIX BLOCK2: Customer deposit with $500 threshold
            if amount_val is None or abs(amount_val) > _CUSTOMER_DEPOSIT_AMOUNT_THRESHOLD:
                # Customer/advance deposit → liability (GL 2400)
                # Override any prior classification — deposit semantics take priority
                flags["potential_capex"] = False
                flags["potential_customer_deposit"] = True
                flags["suggested_gl"] = _CUSTOMER_DEPOSIT_GL
                flags["block_auto_approval"] = True
                flags["review_notes"].append(
                    "Dépôt client détecté (> 500 $) — passif (produit reporté GL 2400), "
                    "non revenu. Approbation automatique bloquée. / "
                    "Customer deposit detected (> $500) — liability (deferred revenue GL 2400), "
                    "not revenue. Auto-approval blocked."
                )
            else:
                flags["potential_capex"] = False
                flags["suggested_gl"] = _CUSTOMER_DEPOSIT_GL
                flags["review_notes"].append(
                    "Dépôt client / Customer deposit — passif (produit reporté), non revenu / "
                    "liability (deferred revenue), not revenue"
                )
        else:
            if not flags["suggested_gl"]:
                flags["suggested_gl"] = _DEPOSIT_GL
            flags["review_notes"].append(
                "Vérifier si dépôt remboursable / Verify if refundable security deposit — asset, not expense"
            )

    # FIX 2 + BLOCK2: Intercompany transfer detection
    if _INTERCOMPANY_KEYWORDS.search(combined):
        flags["potential_loan"] = True
        flags["potential_intercompany"] = True
        flags["block_auto_approval"] = True
        if not flags["suggested_gl"]:
            flags["suggested_gl"] = _INTERCOMPANY_GL
        flags["review_notes"].append(
            "Transaction intersociété potentielle — ne pas comptabiliser en revenus/dépenses. "
            "Vérifier la table des parties liées. / "
            "Potential intercompany transaction — do not book as revenue/expense. "
            "Check related_parties table."
        )

    # FIX 4: Mixed taxable/exempt detection
    _mixed_tax_keywords = re.compile(
        r"\b(exempt|exempté|taxable.*exempt|exempt.*taxable|"
        r"fournitures détaxées|zero.rated|mixed.supply|"
        r"fournitures mixtes)\b",
        re.IGNORECASE,
    )
    if _mixed_tax_keywords.search(combined):
        flags["review_notes"].append(
            "Facture potentiellement mixte (articles taxables et exonérés) — "
            "vérifier les codes de taxe par ligne / "
            "Potentially mixed invoice (taxable and exempt items) — "
            "verify tax codes per line"
        )

    # Gift card / employee benefits detection
    if _GIFT_CARD_KEYWORDS.search(combined):
        if not flags["suggested_gl"]:
            flags["suggested_gl"] = _GIFT_CARD_GL
        flags["review_notes"].append(
            "Carte-cadeau / Gift card — Employee Benefits Expense or Prepaid"
        )

    # Shareholder / personal expense detection
    # FIX 8: B2B vendors (Inc, Corp, etc.) override personal expense detection
    if _PERSONAL_KEYWORDS.search(combined) and not _PERSONAL_NEGATIVE.search(combined):
        flags["potential_personal_expense"] = True
        flags["block_auto_approval"] = True
        flags["review_notes"].append(
            "Dépense personnelle potentielle — approbation automatique bloquée / "
            "Potential personal expense — auto-approval blocked"
        )
        # FIX 5: Proportional ITC/ITR disallowance guidance
        flags["personal_use_detected"] = True
        flags["block_itc_itr_until_percentage_set"] = True
        flags["review_notes"].append(
            "Usage personnel détecté — veuillez confirmer le pourcentage d'utilisation "
            "professionnelle (ex: 80% affaires, 20% personnel) pour calculer les "
            "CTI/RTI admissibles / Personal use detected — please confirm business use "
            "percentage (e.g. 80% business, 20% personal) to calculate eligible ITC/ITR"
        )

    # Owner name match
    # FIX 11: Use full-word matching — require ALL name parts to match,
    # not substring. "Jean Tremblay" should not match "Jean Coutu".
    # Single-word names (e.g. "Jean") require exact vendor match to avoid
    # false positives on common first names.
    # FIX 1 (nightmare): Also match abbreviations (J.P., J-P, JP) and
    # reversed name order (Tremblay Jean-Pierre).
    if owner_names:
        for name in owner_names:
            if not name:
                continue
            name_parts = _normalize(name).split()
            if not name_parts:
                continue
            # Single-word owner names are too common for word-boundary matching
            # (e.g. "Jean" would match "Jean Coutu"). Require the vendor name
            # to start with the full owner name or be an exact match.
            if len(name_parts) == 1:
                # Single name: only match if vendor IS the owner name
                # (possibly followed by business suffixes like Inc, Ltd)
                owner_word = name_parts[0]
                vendor_words = vendor_lower.split()
                # Filter out common business suffixes
                _biz_suffixes = {"inc", "ltd", "ltée", "ltee", "corp", "llc", "enr", "senc"}
                meaningful_vendor_words = [w for w in vendor_words if w not in _biz_suffixes]
                # Single name only matches if it's the ONLY meaningful word
                all_parts_match = (
                    len(meaningful_vendor_words) == 1
                    and meaningful_vendor_words[0] == owner_word
                )
            else:
                # Multi-word name: all parts must appear as whole words in vendor
                # Also try reversed order: "Tremblay Jean-Pierre" matches "Jean-Pierre Tremblay"
                all_parts_match = _owner_name_matches_vendor(name_parts, vendor_lower)
            if all_parts_match:
                flags["potential_personal_expense"] = True
                flags["block_auto_approval"] = True
                if "owner_name_match" not in str(flags["review_notes"]):
                    flags["review_notes"].append(
                        f"Vendor name matches owner '{name}' — verify if personal expense"
                    )

    # -----------------------------------------------------------------------
    # Keyword confidence heuristic
    # -----------------------------------------------------------------------
    keyword_matched = (
        flags["potential_capex"]
        or flags["potential_prepaid"]
        or flags["potential_loan"]
        or flags["potential_tax_remittance"]
        or flags["potential_personal_expense"]
    )
    # Compute a keyword-based confidence:
    # - Strong keyword match: 0.80
    # - Vendor-only heuristic (CAPEX_VENDORS, BANK_NAME_KEYWORDS): 0.60
    # - No match: 0.0
    keyword_confidence = 0.0
    if keyword_matched:
        keyword_confidence = 0.80
        # Lower confidence for vendor-only or bank-name heuristic matches
        if flags["potential_capex"] and not _CAPEX_KEYWORDS.search(combined):
            # Matched only via _CAPEX_VENDORS — weaker signal
            keyword_confidence = min(keyword_confidence, 0.60)
        if flags["potential_loan"] and not _LOAN_KEYWORDS.search(combined) and _BANK_NAME_KEYWORDS.search(vendor_lower):
            # Matched only via bank name heuristic — weaker signal
            keyword_confidence = min(keyword_confidence, 0.60)

    # -----------------------------------------------------------------------
    # AI fallback: when no keywords matched OR confidence < 0.70
    # -----------------------------------------------------------------------
    _CLASS_TO_FLAG = {
        "capex":                "potential_capex",
        "capital_asset":        "potential_capex",
        "prepaid":              "potential_prepaid",
        "prepaid_expense":      "potential_prepaid",
        "loan":                 "potential_loan",
        "loan_payment":         "potential_loan",
        "deposit":              "potential_prepaid",
        "customer_deposit":     "potential_customer_deposit",
        "intercompany":         "potential_intercompany",
        "intercompany_transfer":"potential_intercompany",
        "personal":             "potential_personal_expense",
        "personal_expense":     "potential_personal_expense",
        "tax_remittance":       "potential_tax_remittance",
        "mixed_invoice":        "mixed_tax_invoice",
    }

    if not keyword_matched or keyword_confidence < 0.70:
        try:
            from src.agents.core import ai_router

            ai_result = ai_router.call_substance_classification(
                vendor=vendor,
                amount=amount,
                memo=memo,
                doc_type=doc_type,
                province=province,
            )
            ai_conf = float(ai_result.get("confidence") or 0)
            ai_class = (ai_result.get("classification") or "").lower()

            # Accept AI result if confidence >= 0.70 and not a plain operating expense
            if ai_conf >= 0.70 and ai_class and ai_class not in ("operating", "operating_expense"):
                flag_key = _CLASS_TO_FLAG.get(ai_class)
                if flag_key:
                    # Merge: set the flag (don't unset keyword flags)
                    if flag_key not in flags:
                        # New flag type (customer_deposit, intercompany, mixed_tax)
                        flags[flag_key] = True
                    else:
                        flags[flag_key] = True

                if ai_result.get("gl_suggestion") and not flags["suggested_gl"]:
                    flags["suggested_gl"] = ai_result["gl_suggestion"]

                reasoning = ai_result.get("reasoning") or ai_class
                flags["review_notes"].append(
                    f"AI classification: {ai_class} (confidence {ai_conf:.0%}) — {reasoning}"
                )

                if ai_class in ("personal", "personal_expense"):
                    flags["block_auto_approval"] = True
                if ai_class in ("customer_deposit", "intercompany", "intercompany_transfer"):
                    flags["block_auto_approval"] = True
                if ai_class == "mixed_invoice":
                    flags["block_auto_approval"] = True
        except Exception as exc:
            log.debug("AI substance classification fallback failed: %s", exc)

    return flags


def propagate_gl_change_suggestions(
    vendor: str,
    new_gl: str,
    client_code: str,
    db_path: str | None = None,
) -> int:
    """FIX 10: When vendor memory GL changes, add review_note to unprocessed docs.

    Adds a suggestion note to all unprocessed documents from this vendor.
    Does NOT change approved/posted documents. Returns count of updated docs.
    """
    import sqlite3
    from pathlib import Path

    if not db_path:
        db_path_obj = Path(__file__).resolve().parent.parent.parent / "data" / "otocpa_agent.db"
    else:
        db_path_obj = Path(db_path)

    if not db_path_obj.exists():
        return 0

    try:
        conn = sqlite3.connect(str(db_path_obj))
        conn.row_factory = sqlite3.Row
        try:
            # Only update unprocessed documents (New, NeedsReview)
            rows = conn.execute(
                """SELECT document_id, raw_result FROM documents
                   WHERE LOWER(TRIM(COALESCE(vendor, ''))) = LOWER(TRIM(?))
                     AND LOWER(TRIM(COALESCE(client_code, ''))) = LOWER(TRIM(?))
                     AND LOWER(COALESCE(review_status, '')) IN ('new', 'needsreview', 'needs_review')
                """,
                (vendor, client_code),
            ).fetchall()

            count = 0
            for row in rows:
                doc_id = row["document_id"]
                raw = row["raw_result"] or "{}"
                try:
                    data = json.loads(raw)
                except Exception:
                    data = {}
                notes = data.get("review_notes", [])
                if not isinstance(notes, list):
                    notes = []
                note = (
                    f"GL suggestion updated to {new_gl} based on vendor memory change / "
                    f"Suggestion GL mise à jour à {new_gl} selon le changement de mémoire fournisseur"
                )
                if note not in notes:
                    notes.append(note)
                    data["review_notes"] = notes
                    conn.execute(
                        "UPDATE documents SET raw_result = ? WHERE document_id = ?",
                        (json.dumps(data, ensure_ascii=False), doc_id),
                    )
                    count += 1
            conn.commit()
            return count
        finally:
            conn.close()
    except Exception as exc:
        log.debug("GL change propagation failed: %s", exc)
        return 0


def run_substance_classifier(
    document: dict[str, Any],
    *,
    owner_names: list[str] | None = None,
    province: str = "",
) -> dict[str, Any]:
    """Convenience wrapper that takes a document dict."""
    raw_result = {}
    if document.get("raw_result"):
        try:
            raw_result = json.loads(str(document["raw_result"]))
        except Exception:
            pass

    memo = (
        _normalize(document.get("memo"))
        or _normalize(raw_result.get("memo"))
        or _normalize(raw_result.get("notes"))
        or ""
    )

    return substance_classifier(
        vendor=document.get("vendor") or "",
        memo=memo,
        doc_type=document.get("doc_type") or "",
        amount=document.get("amount"),
        owner_names=owner_names,
        province=province or document.get("province") or "",
    )


# =========================================================================
# PART 10 — Missing subcontractor document detection
# =========================================================================

_LOCAL_SERVICE_KEYWORDS = re.compile(
    r"\b("
    r"installation|mise en place|montage|"
    r"réparation|reparation|repair|"
    r"maintenance|entretien|"
    r"construction|rénovation|renovation|"
    r"plumbing|plomberie|"
    r"electrical|électricité|electricite|"
    r"cleaning|nettoyage|"
    r"landscaping|aménagement paysager|amenagement paysager|"
    r"delivery.*install|livraison.*install|"
    r"on.?site|sur.?place"
    r")\b",
    re.IGNORECASE,
)

_FOREIGN_VENDOR_INDICATORS = re.compile(
    r"\b("
    r"usa|united states|états-unis|etats-unis|"
    r"china|chine|"
    r"mexico|mexique|"
    r"europe|eu\b|"
    r"import|importé|importe|"
    r"international|"
    r"foreign|étranger|etranger"
    r")\b",
    re.IGNORECASE,
)


def detect_missing_subcontractor_document(
    document: dict[str, Any],
    existing_documents: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Detect when a foreign vendor invoice includes local service
    but no corresponding local subcontractor invoice exists.

    Parameters
    ----------
    document : dict with vendor, memo, doc_type, amount, client_code,
               document_date, and optionally vendor_country
    existing_documents : list of other documents in same period/client
    """
    vendor = _normalize(document.get("vendor"))
    memo = _normalize(document.get("memo"))
    combined = f"{vendor} {memo}"

    # Check for local service keywords
    has_local_service = bool(_LOCAL_SERVICE_KEYWORDS.search(combined))
    if not has_local_service:
        return {
            "missing_supporting_vendor_document": False,
            "reasoning": "No local service keywords detected.",
        }

    # Check if vendor is foreign
    vendor_country = _normalize(document.get("vendor_country"))
    is_foreign = (
        bool(_FOREIGN_VENDOR_INDICATORS.search(combined))
        or (vendor_country and vendor_country not in ("ca", "canada", ""))
    )

    if not is_foreign:
        return {
            "missing_supporting_vendor_document": False,
            "reasoning": "Vendor does not appear to be foreign.",
        }

    # Check existing documents for a local subcontractor invoice
    client_code = _normalize(document.get("client_code"))
    doc_date = document.get("document_date") or ""

    if existing_documents:
        for other in existing_documents:
            other_vendor = _normalize(other.get("vendor"))
            other_country = _normalize(other.get("vendor_country"))
            other_client = _normalize(other.get("client_code"))

            # Must be same client
            if client_code and other_client and client_code != other_client:
                continue

            # Must not be from a foreign vendor
            is_other_foreign = (
                bool(_FOREIGN_VENDOR_INDICATORS.search(other_vendor))
                or (other_country and other_country not in ("ca", "canada", ""))
            )
            if is_other_foreign:
                continue

            # Must reference service
            other_memo = _normalize(other.get("memo"))
            other_combined = f"{other_vendor} {other_memo}"
            if _LOCAL_SERVICE_KEYWORDS.search(other_combined):
                return {
                    "missing_supporting_vendor_document": False,
                    "local_subcontractor_found": other.get("document_id"),
                    "reasoning": (
                        f"Local subcontractor invoice found: {other.get('document_id')}"
                    ),
                }

    return {
        "missing_supporting_vendor_document": True,
        "vendor": document.get("vendor"),
        "local_service_detected": True,
        "vendor_appears_foreign": True,
        "client_code": client_code,
        "note_fr": (
            "Service local potentiellement sous-traité — vérifier si facture "
            "de sous-traitant requise"
        ),
        "note_en": (
            "Potentially subcontracted local service — verify if subcontractor "
            "invoice required"
        ),
        "reasoning": (
            f"Foreign vendor '{document.get('vendor')}' claims local service "
            f"but no local subcontractor invoice found for same client/period."
        ),
    }


# =========================================================================
# FIX 5 — Proportional ITC/ITR calculation from personal use percentage
# =========================================================================

def calculate_net_itc_from_personal_use(
    document_id: str,
    personal_use_percentage: float,
    conn: Any,
) -> dict[str, Any]:
    """Calculate net ITC/ITR after personal use disallowance.

    When personal_use_percentage is set:
    - business_percentage = 100 - personal_use_percentage
    - net_itc = gross_itc * (business_percentage / 100)
    - net_itr = gross_itr * (business_percentage / 100)

    Also updates the document's personal_use_percentage field.
    """
    if personal_use_percentage < 0 or personal_use_percentage > 100:
        return {
            "applied": False,
            "error": "personal_use_percentage must be between 0 and 100.",
        }

    business_pct = 100.0 - personal_use_percentage
    rate = business_pct / 100.0

    try:
        row = conn.execute(
            """SELECT COALESCE(gst_amount, 0) AS gst,
                      COALESCE(qst_amount, 0) AS qst,
                      COALESCE(hst_amount, 0) AS hst
               FROM documents WHERE document_id = ?""",
            (document_id,),
        ).fetchone()
    except Exception:
        row = None

    if not row:
        return {
            "applied": False,
            "error": f"Document '{document_id}' not found.",
        }

    gross_gst = float(row[0] if isinstance(row, (tuple, list)) else row["gst"])
    gross_qst = float(row[1] if isinstance(row, (tuple, list)) else row["qst"])
    gross_hst = float(row[2] if isinstance(row, (tuple, list)) else row["hst"])
    gross_itc = gross_gst + gross_hst
    gross_itr = gross_qst

    net_itc = round(gross_itc * rate, 2)
    net_itr = round(gross_itr * rate, 2)

    # Update the document
    try:
        conn.execute(
            "UPDATE documents SET personal_use_percentage = ? WHERE document_id = ?",
            (personal_use_percentage, document_id),
        )
        conn.commit()
    except Exception:
        pass

    return {
        "applied": True,
        "document_id": document_id,
        "personal_use_percentage": personal_use_percentage,
        "business_use_percentage": business_pct,
        "gross_itc": gross_itc,
        "gross_itr": gross_itr,
        "net_itc": net_itc,
        "net_itr": net_itr,
        "disallowed_itc": round(gross_itc - net_itc, 2),
        "disallowed_itr": round(gross_itr - net_itr, 2),
        "reasoning": (
            f"Personal use {personal_use_percentage}% / Business use {business_pct}%. "
            f"ITC: ${gross_itc:.2f} × {rate:.2%} = ${net_itc:.2f}. "
            f"ITR: ${gross_itr:.2f} × {rate:.2%} = ${net_itr:.2f}."
        ),
    }
