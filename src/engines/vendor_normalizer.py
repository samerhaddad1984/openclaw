"""Vendor name normalization layer.

Maps noisy extracted vendor names → canonical brand names:

1. Strip legal-entity suffixes (Inc, Ltée, Corp, etc.)
2. Apply known OCR-typo corrections (unprix → Uniprix)
3. Look up against a static brand map (seeded from merchant overlays +
   common legal-entity parents, e.g. ``TDL Group`` → ``Tim Hortons``).
4. Consult the firm-scoped ``vendor_learning`` self-learning table when
   there are ≥2 corrections agreeing on the same canonical.
5. Fuzzy-match against known brands (Levenshtein similarity ≥0.85).
6. Otherwise pass the input through title-cased.

``normalize()`` returns ``{canonical, confidence, source, original}``. The
``source`` field lets callers tell the user how we arrived at the answer
(``brand_map`` / ``self_learning`` / ``fuzzy`` / ``typo`` / ``no_change``).
"""
from __future__ import annotations

import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

from src.engines.merchant_overlay import OVERLAYS

log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DEFAULT_DB = ROOT_DIR / "data" / "otocpa_agent.db"


# ---------------------------------------------------------------------------
# Legal-entity suffixes (longest first so 'incorporated' wins over 'inc')
# ---------------------------------------------------------------------------

LEGAL_SUFFIXES: tuple[str, ...] = (
    " incorporated",
    " corporation",
    " companies",
    " enterprises",
    " holdings",
    " group",
    " l.l.c.",
    " l.l.p.",
    " s.a.r.l.",
    " sdn bhd",
    " limited",
    " company",
    " corp.",
    " ltée",
    " ltee",
    " ltd.",
    " llc",
    " llp",
    " plc",
    " sarl",
    " stores",
    " gmbh",
    " inc.",
    " bv",
    " nv",
    " ag",
    " sa",
    " s.a.",
    " co.",
    " enr.",
    " enr",
    " corp",
    " ltd",
    " inc",
    " co",
)


# ---------------------------------------------------------------------------
# Brand map
# ---------------------------------------------------------------------------

# Parent / legal-entity variants that should map to the consumer brand. Keys
# are lowercase; compare uses ``vendor.strip().lower()`` after suffix strip.
_MANUAL_BRAND_MAP: dict[str, str] = {
    # Tim Hortons
    "tim hortons": "Tim Hortons",
    "tim horton": "Tim Hortons",
    "tdl group": "Tim Hortons",
    "the tdl group corp": "Tim Hortons",
    "tdl": "Tim Hortons",
    # Uniprix
    "uniprix": "Uniprix",
    "uniprix pharmacie": "Uniprix",
    "pharmacie uniprix": "Uniprix",
    "uni-prix": "Uniprix",
    # Jean Coutu
    "jean coutu": "Jean Coutu",
    "le groupe jean coutu": "Jean Coutu",
    "pjc jean coutu": "Jean Coutu",
    "pjc": "Jean Coutu",
    # Pharmaprix / Shoppers
    "pharmaprix": "Pharmaprix",
    "shoppers drug mart": "Pharmaprix",
    # Familiprix
    "familiprix": "Familiprix",
    "pharmacie familiprix": "Familiprix",
    # Brunet
    "brunet": "Brunet",
    "pharmacie brunet": "Brunet",
    # Metro
    "metro plus": "Metro",
    "metro richelieu": "Metro",
    "marche metro": "Metro",
    # Provigo / Loblaws
    "provigo": "Provigo",
    "provigo le marche": "Provigo",
    # Super C
    "super c": "Super C",
    "superc": "Super C",
    # Maxi
    "maxi & cie": "Maxi",
    "maxi et cie": "Maxi",
    # IGA
    "iga extra": "IGA",
    "iga": "IGA",
    # Adonis
    "adonis": "Adonis",
    "marche adonis": "Adonis",
    "marché adonis": "Adonis",
    # Walmart
    "walmart": "Walmart",
    "wal-mart": "Walmart",
    "wal mart": "Walmart",
    "walmart supercentre": "Walmart",
    # Costco
    "costco wholesale": "Costco",
    "costco canada": "Costco",
    # McDonald's
    "mcdonald's": "McDonald's",
    "mcdonalds": "McDonald's",
    "mcdonald": "McDonald's",
    "mcdo": "McDonald's",
    # Starbucks
    "starbucks coffee": "Starbucks",
    "starbucks": "Starbucks",
    # Subway
    "subway": "Subway",
    "subway restaurant": "Subway",
    # Saint-Hubert
    "rotisserie st-hubert": "Saint-Hubert",
    "rotisserie saint-hubert": "Saint-Hubert",
    "saint-hubert": "Saint-Hubert",
    "st-hubert": "Saint-Hubert",
    # La Cage
    "la cage brasserie sportive": "La Cage",
    "cage aux sports": "La Cage",
    "la cage": "La Cage",
    # Normandin
    "restaurant normandin": "Normandin",
    "normandin": "Normandin",
    # Second Cup
    "second cup": "Second Cup",
    "second cup cafe": "Second Cup",
    # Gas
    "petro-canada": "Petro-Canada",
    "petro canada": "Petro-Canada",
    "petrocanada": "Petro-Canada",
    "ultramar": "Ultramar",
    "shell canada": "Shell",
    "shell": "Shell",
    "esso": "Esso",
    "imperial oil": "Esso",
    "sonic": "Sonic",
    "petroles sonic": "Sonic",
    "couche-tard": "Couche-Tard",
    "couche tard": "Couche-Tard",
    "circle k": "Couche-Tard",
    # Hardware
    "the home depot": "Home Depot",
    "home depot": "Home Depot",
    "rona": "Rona",
    "canadian tire": "Canadian Tire",
    "reno-depot": "Reno Depot",
    "reno depot": "Reno Depot",
    "réno-dépôt": "Reno Depot",
    "patrick morin": "Patrick Morin",
    # Other
    "dollarama": "Dollarama",
    "saq": "SAQ",
    "societe des alcools du quebec": "SAQ",
    "société des alcools du québec": "SAQ",
    "staples": "Staples / Bureau en Gros",
    "bureau en gros": "Staples / Bureau en Gros",
    "amazon.ca": "Amazon.ca",
    "amazon ca": "Amazon.ca",
    "amzn": "Amazon.ca",
    "amazon marketplace": "Amazon.ca",
}


# Common OCR typos on Canadian merchant names. Extend as we observe more.
TYPO_CORRECTIONS: dict[str, str] = {
    "unprix": "uniprix",
    "unniprix": "uniprix",
    "uniprx": "uniprix",
    "pharmacien": "pharmaprix",  # analysis of real Quebec pharmacy receipts
    "tin hortons": "tim hortons",
    "tm hortons": "tim hortons",
    "mcdonals": "mcdonald",
    "mcdnald": "mcdonald",
    "costc0": "costco",
    "costeo": "costco",
    "maeri": "maxi",  # OCR noise
    "cnadian tire": "canadian tire",
    "canadien tire": "canadian tire",
    "petre canada": "petro-canada",
    "petro canda": "petro-canada",
}


def _seed_brand_map_from_overlays() -> dict[str, str]:
    """Merge manual brand map with the canonical names from every overlay."""
    seeded = dict(_MANUAL_BRAND_MAP)
    for overlay in OVERLAYS:
        seeded[overlay.VENDOR_CANONICAL.lower()] = overlay.VENDOR_CANONICAL
    return seeded


# Built at import time; covers all known overlays + manual parent→brand map.
BRAND_MAP: dict[str, str] = _seed_brand_map_from_overlays()


# ---------------------------------------------------------------------------
# Levenshtein (pure-Python; no external dep)
# ---------------------------------------------------------------------------

def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        cur = [i] + [0] * len(b)
        for j, cb in enumerate(b, start=1):
            cost = 0 if ca == cb else 1
            cur[j] = min(
                cur[j - 1] + 1,
                prev[j] + 1,
                prev[j - 1] + cost,
            )
        prev = cur
    return prev[-1]


def _similarity(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    max_len = max(len(a), len(b))
    if max_len == 0:
        return 1.0
    return 1.0 - (_levenshtein(a, b) / max_len)


# ---------------------------------------------------------------------------
# Core class
# ---------------------------------------------------------------------------

FUZZY_THRESHOLD = 0.85
LEARNING_CONF_CAP = 0.95
MIN_LEARNED_CORRECTIONS = 2


class VendorNormalizer:
    """Normalize extracted vendor names to canonical brand names."""

    def __init__(
        self,
        db_path: str | Path | None = None,
        firm_code: str = "",
        brand_map: dict[str, str] | None = None,
        typo_corrections: dict[str, str] | None = None,
    ) -> None:
        self.db_path = Path(db_path) if db_path else DEFAULT_DB
        self.firm_code = firm_code or ""
        self.brand_map = brand_map if brand_map is not None else BRAND_MAP
        self.typo_corrections = (
            typo_corrections if typo_corrections is not None else TYPO_CORRECTIONS
        )

    # -- public -----------------------------------------------------------

    def normalize(self, extracted_vendor: str | None) -> dict[str, Any]:
        """Return ``{canonical, confidence, source, original}`` for vendor.

        ``source`` ∈ ``{brand_map, typo, self_learning, fuzzy, no_change,
        empty}``.
        """
        if not extracted_vendor or not str(extracted_vendor).strip():
            return {
                "canonical": None,
                "confidence": 0.0,
                "source": "empty",
                "original": extracted_vendor,
            }

        original = str(extracted_vendor).strip()
        stripped = self._strip_suffixes(original)
        key = stripped.lower()

        # 1. Exact brand map.
        if key in self.brand_map:
            return {
                "canonical": self.brand_map[key],
                "confidence": 1.0,
                "source": "brand_map",
                "original": original,
            }

        # 2. Typo correction, then brand-map lookup against the fix.
        if key in self.typo_corrections:
            fix = self.typo_corrections[key]
            canonical = self.brand_map.get(fix, fix.title())
            return {
                "canonical": canonical,
                "confidence": 0.9,
                "source": "typo",
                "original": original,
            }

        # 3. Self-learning (firm-scoped vendor_learning).
        learned = self._lookup_learned(key)
        if learned:
            return {
                "canonical": learned["canonical_vendor"],
                "confidence": min(
                    float(learned.get("confidence") or 1.0),
                    LEARNING_CONF_CAP,
                ),
                "source": "self_learning",
                "original": original,
            }

        # 4. Fuzzy match against known brands.
        fuzzy = self._fuzzy_brand_match(key)
        if fuzzy:
            return {
                "canonical": fuzzy["match"],
                "confidence": fuzzy["score"],
                "source": "fuzzy",
                "original": original,
            }

        # 5. Pass through, title-cased if it came in all-caps.
        canonical = stripped if stripped.isupper() or stripped.islower() \
            else stripped
        if canonical.isupper():
            canonical = canonical.title()
        return {
            "canonical": canonical,
            "confidence": 0.5,
            "source": "no_change",
            "original": original,
        }

    # -- helpers ----------------------------------------------------------

    def _strip_suffixes(self, vendor: str) -> str:
        """Repeatedly strip trailing legal suffixes + punctuation."""
        out = vendor.strip().rstrip(",.;: ")
        lower = out.lower()
        changed = True
        while changed:
            changed = False
            for suf in LEGAL_SUFFIXES:
                if lower.endswith(suf):
                    out = out[: -len(suf)].rstrip(",.;: ")
                    lower = out.lower()
                    changed = True
                    break
        return out

    def _lookup_learned(self, vendor_lower: str) -> dict[str, Any] | None:
        if not self.db_path.exists():
            return None
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=5)
            try:
                row = conn.execute(
                    "SELECT canonical_vendor, confidence, correction_count "
                    "FROM vendor_learning "
                    "WHERE LOWER(extracted_vendor) = ? "
                    "  AND COALESCE(firm_code,'') = ? "
                    "  AND correction_count >= ? "
                    "ORDER BY correction_count DESC LIMIT 1",
                    (vendor_lower, self.firm_code or "", MIN_LEARNED_CORRECTIONS),
                ).fetchone()
            finally:
                conn.close()
        except sqlite3.Error:
            log.exception("vendor_learning lookup failed")
            return None
        if not row:
            return None
        return {
            "canonical_vendor": row[0],
            "confidence": row[1],
            "correction_count": row[2],
        }

    def _fuzzy_brand_match(self, vendor_lower: str) -> dict[str, Any] | None:
        best_score = 0.0
        best_canonical: str | None = None
        for variant, canonical in self.brand_map.items():
            score = _similarity(vendor_lower, variant)
            if score > FUZZY_THRESHOLD and score > best_score:
                best_score = score
                best_canonical = canonical
        if best_canonical is None:
            return None
        return {"match": best_canonical, "score": round(best_score, 4)}


# ---------------------------------------------------------------------------
# Convenience wrapper
# ---------------------------------------------------------------------------

_DEFAULT_NORMALIZER: VendorNormalizer | None = None


def normalize_vendor(
    vendor: str | None,
    firm_code: str = "",
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Module-level helper that reuses a single VendorNormalizer when the
    caller doesn't need anything custom."""
    global _DEFAULT_NORMALIZER  # noqa: PLW0603
    if (
        _DEFAULT_NORMALIZER is None
        or _DEFAULT_NORMALIZER.firm_code != firm_code
        or (db_path and Path(db_path) != _DEFAULT_NORMALIZER.db_path)
    ):
        _DEFAULT_NORMALIZER = VendorNormalizer(
            db_path=db_path or DEFAULT_DB,
            firm_code=firm_code,
        )
    return _DEFAULT_NORMALIZER.normalize(vendor)
