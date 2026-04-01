"""
L — LOCALIZATION WAR (FR/EN)
=============================
Attack bilingual behavior with mixed-language inputs, accent stripping,
encoding corruption, and translation completeness.

Targets: i18n, substance_engine, uncertainty_engine, export_engine
"""
from __future__ import annotations

import sys
import unicodedata
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.substance_engine import substance_classifier, _strip_accents
from src.engines.uncertainty_engine import (
    UncertaintyReason,
    evaluate_uncertainty,
)

try:
    from src.i18n import get_translation, SUPPORTED_LOCALES
    HAS_I18N = True
except ImportError:
    HAS_I18N = False


# ===================================================================
# TEST CLASS: Accent Handling
# ===================================================================

class TestAccentHandling:
    """French accents must not break classification or matching."""

    def test_strip_accents_function(self):
        """_strip_accents must correctly normalize French characters."""
        assert _strip_accents("équipement") == "equipement"
        assert _strip_accents("Québécois") == "Quebecois"
        assert _strip_accents("résumé") == "resume"
        assert _strip_accents("naïve") == "naive"
        assert _strip_accents("café") == "cafe"

    def test_cedilla(self):
        assert _strip_accents("façade") == "facade"
        assert _strip_accents("garçon") == "garcon"

    def test_accented_vendor_in_classifier(self):
        """Accented French vendor names must still trigger CapEx detection."""
        r = substance_classifier(
            vendor="Équipements Industriels Ltée",
            memo="Achat équipement lourd",
            amount=15000,
        )
        assert r.get("is_capex", False) is True, (
            "DEFECT: Accented 'Équipements' not detected as CapEx"
        )

    def test_unaccented_equivalent_same_result(self):
        """Accented and unaccented inputs must produce same classification."""
        r1 = substance_classifier(vendor="Équipements Ltée", memo="équipement", amount=10000)
        r2 = substance_classifier(vendor="Equipements Ltee", memo="equipement", amount=10000)
        assert r1.get("is_capex") == r2.get("is_capex"), (
            "DEFECT: Accented vs unaccented gives different CapEx classification"
        )


# ===================================================================
# TEST CLASS: Bilingual Uncertainty Reasons
# ===================================================================

class TestBilingualUncertainty:
    """Every uncertainty reason must have both FR and EN descriptions."""

    def test_uncertainty_reason_bilingual(self):
        reason = UncertaintyReason(
            reason_code="DATE_AMBIGUOUS",
            description_fr="Date ambiguë",
            description_en="Ambiguous date",
            evidence_available="03/04/2025",
            evidence_needed="Locale info",
        )
        d = reason.to_dict()
        assert d["description_fr"] != "", "Missing French description"
        assert d["description_en"] != "", "Missing English description"
        assert d["description_fr"] != d["description_en"], "FR and EN are identical"

    def test_all_reason_codes_have_both_languages(self):
        """Factory functions must produce bilingual reasons."""
        from src.engines.uncertainty_engine import (
            reason_manual_journal_collision,
            reason_reimport_blocked,
        )
        r1 = reason_manual_journal_collision("test", "collision")
        assert r1.description_fr and r1.description_en
        try:
            r2 = reason_reimport_blocked("doc-test")
            assert r2.description_fr and r2.description_en
        except TypeError:
            pass


# ===================================================================
# TEST CLASS: Mixed Language Input
# ===================================================================

class TestMixedLanguageInput:
    """Franglais — mixed FR/EN input in same field."""

    def test_franglais_vendor_name(self):
        """'Services de Computer Repair' — mixed FR/EN."""
        r = substance_classifier(
            vendor="Services de Computer Repair Inc.",
            memo="réparation ordinateur",
            amount=500,
        )
        # Should not crash, classification should be reasonable
        assert isinstance(r, dict)

    def test_french_memo_english_vendor(self):
        """French memo with English vendor."""
        r = substance_classifier(
            vendor="ABC Equipment Ltd",
            memo="achat de machinerie lourde pour l'usine",
            amount=50000,
        )
        assert r.get("is_capex", False) is True, (
            "DEFECT: French memo 'machinerie lourde' not detected as CapEx"
        )

    def test_emoji_in_vendor_name(self):
        """Emoji must not crash classification."""
        r = substance_classifier(
            vendor="🏗️ Construction Pro",
            memo="construction materials",
            amount=25000,
        )
        assert isinstance(r, dict)


# ===================================================================
# TEST CLASS: Encoding Edge Cases
# ===================================================================

class TestEncodingEdgeCases:

    def test_null_bytes_in_vendor(self):
        """Null bytes must be handled."""
        try:
            r = substance_classifier(vendor="Vendor\x00Name", memo="test", amount=100)
            assert isinstance(r, dict)
        except (ValueError, UnicodeError):
            pass  # Acceptable to reject

    def test_very_long_vendor_name(self):
        """10,000 character vendor name."""
        r = substance_classifier(vendor="A" * 10000, memo="test", amount=100)
        assert isinstance(r, dict)

    def test_rtl_text(self):
        """Right-to-left Arabic text in vendor."""
        r = substance_classifier(vendor="شركة الأعمال", memo="business", amount=100)
        assert isinstance(r, dict)

    def test_cjk_characters(self):
        """Chinese/Japanese/Korean characters."""
        r = substance_classifier(vendor="株式会社テスト", memo="equipment", amount=5000)
        assert isinstance(r, dict)


# ===================================================================
# TEST CLASS: Determinism
# ===================================================================

class TestLocalizationDeterminism:
    def test_strip_accents_deterministic(self):
        results = {_strip_accents("Équipement Québécois") for _ in range(100)}
        assert len(results) == 1

    def test_classifier_deterministic_bilingual(self):
        results = set()
        for _ in range(30):
            r = substance_classifier(vendor="Équipements", memo="machinerie", amount=15000)
            results.add(str(r.get("is_capex")))
        assert len(results) == 1, f"Non-deterministic: {results}"
