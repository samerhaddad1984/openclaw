"""Regression tests for the 25+ Quebec / Canadian merchant overlays.

Each merchant gets at least three cases:
  1. Vendor pattern matches on a realistic header snippet.
  2. A line-tax classification matches the expected rule (taxable /
     zero-rated) where the overlay supplies keywords.
  3. ``apply_merchant_overlay`` stamps the overlay's DEFAULT_GL_ACCOUNT
     on items that arrive without one.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.merchant_overlay import (  # noqa: E402
    OVERLAYS,
    apply_merchant_overlay,
    find_overlay,
    find_overlay_by_name,
    list_overlays,
    overlay_hints,
    resolve_vendor,
)
from src.engines.merchant_overlay import (  # noqa: E402
    AdonisOverlay, AmazonOverlay, BrunetOverlay, CanadianTireOverlay,
    CostcoOverlay, CoucheTardOverlay, DollaramaOverlay, EssoOverlay,
    FamiliprixOverlay, HomeDepotOverlay, IGAOverlay, JeanCoutuOverlay,
    LaCageOverlay, MaxiOverlay, McDonaldsOverlay, MetroOverlay,
    NormandinOverlay, PatrickMorinOverlay, PetroCanadaOverlay,
    PharmaprixOverlay, ProvigoOverlay, RenoDepotOverlay, RichelieuOverlay,
    RonaOverlay, SAQOverlay, SaintHubertOverlay, SecondCupOverlay,
    ShellOverlay, SonicOverlay, StaplesOverlay, StarbucksOverlay,
    SubwayOverlay, SuperCOverlay, TimHortonsOverlay, UltramarOverlay,
    UniprixOverlay, WalmartOverlay,
)


# ---------------------------------------------------------------------------
# Common case builder
# ---------------------------------------------------------------------------

def _apply_default_gl(overlay_cls, sample_desc: str = "Misc item"):
    items = [{"description": sample_desc, "total_price": 10.0}]
    out = apply_merchant_overlay(
        items, overlay_cls.VENDOR_CANONICAL, "irrelevant",
    )
    return out[0]


# ---------------------------------------------------------------------------
# Grocery (8 merchants × 3 tests = 24)
# ---------------------------------------------------------------------------

class TestMetro:
    def test_vendor_matched(self):
        text = "METRO PLUS\n123 Rue Main\nSOUS-TOTAL 45.00"
        assert MetroOverlay.matches(text)
        assert resolve_vendor(text)["name"] == "Metro"

    def test_tax_classifier_grocery_zero(self):
        assert MetroOverlay.classify_line_tax("Lait 2%") == "Z"
        assert MetroOverlay.classify_line_tax("Pain baguette") == "Z"

    def test_tax_classifier_alcohol_taxable(self):
        assert MetroOverlay.classify_line_tax("Biere Molson") == "T"

    def test_default_gl_applied(self):
        item = _apply_default_gl(MetroOverlay)
        assert item["gl_account"] == "5420"


class TestProvigo:
    def test_vendor_matched(self):
        assert ProvigoOverlay.matches("PROVIGO LE MARCHE\nQC")
        assert resolve_vendor("PROVIGO\n#123")["name"] == "Provigo"

    def test_zero_rated_grocery(self):
        assert ProvigoOverlay.classify_line_tax("Yogourt nature") == "Z"

    def test_default_gl(self):
        item = _apply_default_gl(ProvigoOverlay)
        assert item["gl_account"] == "5420"


class TestSuperC:
    def test_vendor_matched(self):
        assert SuperCOverlay.matches("SUPER C #5927")
        assert SuperCOverlay.matches("Super C Boulevard Arthur-Sauve")

    def test_zero_rated_grocery(self):
        assert SuperCOverlay.classify_line_tax("Carotte bio") == "Z"

    def test_alcohol_taxable(self):
        assert SuperCOverlay.classify_line_tax("Biere Budweiser 12") == "T"


class TestMaxi:
    def test_vendor_matched(self):
        assert MaxiOverlay.matches("MAXI & CIE")
        assert MaxiOverlay.matches("MAXI\n#1234")

    def test_zero_rated(self):
        assert MaxiOverlay.classify_line_tax("Pomme Gala") == "Z"

    def test_default_gl(self):
        item = _apply_default_gl(MaxiOverlay)
        assert item["gl_account"] == "5420"


class TestAdonis:
    def test_vendor_matched(self):
        assert AdonisOverlay.matches("MARCHE ADONIS\nMontreal")

    def test_mediterranean_zero_rated(self):
        assert AdonisOverlay.classify_line_tax("Pita plain") == "Z"
        assert AdonisOverlay.classify_line_tax("Hummus classic") == "Z"

    def test_default_gl(self):
        item = _apply_default_gl(AdonisOverlay)
        assert item["gl_account"] == "5420"


class TestRichelieu:
    def test_vendor_matched(self):
        assert RichelieuOverlay.matches("MARCHE RICHELIEU")

    def test_zero_rated_grocery(self):
        assert RichelieuOverlay.classify_line_tax("Fromage gruyere") == "Z"

    def test_default_gl(self):
        item = _apply_default_gl(RichelieuOverlay)
        assert item["gl_account"] == "5420"


class TestIGA:
    def test_vendor_matched(self):
        assert IGAOverlay.matches("IGA EXTRA\n#42")

    def test_zero_rated(self):
        assert IGAOverlay.classify_line_tax("Oeufs brown") == "Z"

    def test_default_gl(self):
        item = _apply_default_gl(IGAOverlay)
        assert item["gl_account"] == "5420"


class TestWalmart:
    def test_vendor_matched(self):
        assert WalmartOverlay.matches("WALMART SUPERCENTRE")
        assert WalmartOverlay.matches("WAL-MART")

    def test_default_taxable(self):
        assert WalmartOverlay.TAX_CODE_DEFAULT == "T"

    def test_grocery_keyword_still_zero(self):
        assert WalmartOverlay.classify_line_tax("Banane") == "Z"


# ---------------------------------------------------------------------------
# Pharmacy (5 × 3 = 15)
# ---------------------------------------------------------------------------

class TestJeanCoutu:
    def test_vendor_matched(self):
        assert JeanCoutuOverlay.matches("JEAN COUTU\n#PJC 0099")
        assert JeanCoutuOverlay.matches("LE GROUPE JEAN COUTU (PJC) INC")

    def test_prescription_zero(self):
        assert JeanCoutuOverlay.classify_line_tax("Rx patient") == "Z"

    def test_otc_taxable(self):
        assert JeanCoutuOverlay.classify_line_tax("Advil extra fort") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(JeanCoutuOverlay)
        assert item["gl_account"] == "5640"


class TestPharmaprix:
    def test_vendor_matched(self):
        assert PharmaprixOverlay.matches("PHARMAPRIX\n1234")
        assert PharmaprixOverlay.matches("SHOPPERS DRUG MART")

    def test_prescription_zero(self):
        assert PharmaprixOverlay.classify_line_tax("Rx contribution") == "Z"

    def test_default_gl(self):
        item = _apply_default_gl(PharmaprixOverlay)
        assert item["gl_account"] == "5640"


class TestFamiliprix:
    def test_vendor_matched(self):
        assert FamiliprixOverlay.matches("FAMILIPRIX")

    def test_otc_taxable(self):
        assert FamiliprixOverlay.classify_line_tax("Tylenol 500mg") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(FamiliprixOverlay)
        assert item["gl_account"] == "5640"


class TestUniprix:
    def test_vendor_matched(self):
        assert UniprixOverlay.matches("PHARMACIE UNIPRIX")
        assert UniprixOverlay.matches("Uniprix 3456")

    def test_ocr_typo_needs_normalizer(self):
        # Raw OCR often emits 'unprix'; the overlay does not fix that here —
        # normalizer covers it. Overlay should NOT match the typo.
        assert not UniprixOverlay.matches("unprix")

    def test_default_gl(self):
        item = _apply_default_gl(UniprixOverlay)
        assert item["gl_account"] == "5640"


class TestBrunet:
    def test_vendor_matched(self):
        assert BrunetOverlay.matches("PHARMACIE BRUNET\n#3-221")

    def test_prescription_zero(self):
        assert BrunetOverlay.classify_line_tax("Rx ordonnance") == "Z"

    def test_default_gl(self):
        item = _apply_default_gl(BrunetOverlay)
        assert item["gl_account"] == "5640"


# ---------------------------------------------------------------------------
# Coffee / QSR (8 × 3 = 24)
# ---------------------------------------------------------------------------

class TestTimHortons:
    def test_vendor_matched(self):
        assert TimHortonsOverlay.matches("TIM HORTONS #0428")
        assert TimHortonsOverlay.matches("THE TDL GROUP CORP")

    def test_coffee_taxable(self):
        assert TimHortonsOverlay.classify_line_tax("Coffee medium") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(TimHortonsOverlay)
        assert item["gl_account"] == "5420"


class TestStarbucks:
    def test_vendor_matched(self):
        assert StarbucksOverlay.matches("STARBUCKS COFFEE\nMontreal")

    def test_latte_taxable(self):
        assert StarbucksOverlay.classify_line_tax("Caramel Latte grande") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(StarbucksOverlay)
        assert item["gl_account"] == "5420"


class TestSecondCup:
    def test_vendor_matched(self):
        assert SecondCupOverlay.matches("SECOND CUP CAFE")

    def test_coffee_taxable(self):
        assert SecondCupOverlay.classify_line_tax("Espresso double") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(SecondCupOverlay)
        assert item["gl_account"] == "5420"


class TestMcDonalds:
    def test_vendor_matched(self):
        assert McDonaldsOverlay.matches("MCDONALD'S #1234")
        assert McDonaldsOverlay.matches("MCDONALDS REST")

    def test_combo_taxable(self):
        assert McDonaldsOverlay.classify_line_tax("Big Mac Combo") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(McDonaldsOverlay)
        assert item["gl_account"] == "5420"


class TestSubway:
    def test_vendor_matched(self):
        assert SubwayOverlay.matches("SUBWAY 12345")

    def test_sub_taxable(self):
        assert SubwayOverlay.classify_line_tax("Sub BLT footlong") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(SubwayOverlay)
        assert item["gl_account"] == "5420"


class TestSaintHubert:
    def test_vendor_matched(self):
        assert SaintHubertOverlay.matches("ROTISSERIE ST-HUBERT")
        assert SaintHubertOverlay.matches("SAINT-HUBERT #42")

    def test_poulet_taxable(self):
        assert SaintHubertOverlay.classify_line_tax("Poulet rotisserie") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(SaintHubertOverlay)
        assert item["gl_account"] == "5420"


class TestLaCage:
    def test_vendor_matched(self):
        assert LaCageOverlay.matches("LA CAGE BRASSERIE SPORTIVE")
        assert LaCageOverlay.matches("CAGE AUX SPORTS Québec")

    def test_wings_taxable(self):
        assert LaCageOverlay.classify_line_tax("Ailes de poulet 20") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(LaCageOverlay)
        assert item["gl_account"] == "5420"


class TestNormandin:
    def test_vendor_matched(self):
        assert NormandinOverlay.matches("RESTAURANT NORMANDIN")

    def test_dejeuner_taxable(self):
        assert NormandinOverlay.classify_line_tax("Dejeuner no 3") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(NormandinOverlay)
        assert item["gl_account"] == "5420"


# ---------------------------------------------------------------------------
# Gas (6 × 3 = 18)
# ---------------------------------------------------------------------------

class TestPetroCanada:
    def test_vendor_matched(self):
        assert PetroCanadaOverlay.matches("PETRO-CANADA Ste-Foy")
        assert PetroCanadaOverlay.matches("PETROCANADA")

    def test_gas_taxable(self):
        assert PetroCanadaOverlay.classify_line_tax("Essence regular 87") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(PetroCanadaOverlay)
        assert item["gl_account"] == "5430"


class TestUltramar:
    def test_vendor_matched(self):
        assert UltramarOverlay.matches("ULTRAMAR\n42 rue Main")

    def test_gas_taxable(self):
        assert UltramarOverlay.classify_line_tax("Diesel") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(UltramarOverlay)
        assert item["gl_account"] == "5430"


class TestShell:
    def test_vendor_matched(self):
        assert ShellOverlay.matches("SHELL CANADA\nVille de Quebec")

    def test_shellfish_does_not_match(self):
        assert not ShellOverlay.matches("SHELL FISH BAR")

    def test_default_gl(self):
        item = _apply_default_gl(ShellOverlay)
        assert item["gl_account"] == "5430"


class TestEsso:
    def test_vendor_matched(self):
        assert EssoOverlay.matches("ESSO 1234")
        assert EssoOverlay.matches("IMPERIAL OIL #123")

    def test_gas_taxable(self):
        assert EssoOverlay.classify_line_tax("Essence supreme") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(EssoOverlay)
        assert item["gl_account"] == "5430"


class TestSonic:
    def test_vendor_matched(self):
        assert SonicOverlay.matches("PETROLES SONIC")

    def test_gas_taxable(self):
        assert SonicOverlay.classify_line_tax("Essence super") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(SonicOverlay)
        assert item["gl_account"] == "5430"


class TestCoucheTard:
    def test_vendor_matched(self):
        assert CoucheTardOverlay.matches("COUCHE-TARD 5555")
        assert CoucheTardOverlay.matches("CIRCLE K")

    def test_gas_taxable(self):
        assert CoucheTardOverlay.classify_line_tax("Essence regular") == "T"

    def test_chips_taxable(self):
        assert CoucheTardOverlay.classify_line_tax("Chips BBQ 80g") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(CoucheTardOverlay)
        assert item["gl_account"] == "5430"


# ---------------------------------------------------------------------------
# Hardware (5 × 3 = 15)
# ---------------------------------------------------------------------------

class TestHomeDepot:
    def test_vendor_matched(self):
        assert HomeDepotOverlay.matches("THE HOME DEPOT #7042")

    def test_default_taxable(self):
        assert HomeDepotOverlay.TAX_CODE_DEFAULT == "T"

    def test_default_gl(self):
        item = _apply_default_gl(HomeDepotOverlay)
        assert item["gl_account"] == "5500"


class TestRona:
    def test_vendor_matched(self):
        assert RonaOverlay.matches("RONA INC\n#13")

    def test_default_taxable(self):
        assert RonaOverlay.TAX_CODE_DEFAULT == "T"

    def test_default_gl(self):
        item = _apply_default_gl(RonaOverlay)
        assert item["gl_account"] == "5500"


class TestCanadianTire:
    def test_vendor_matched(self):
        assert CanadianTireOverlay.matches("CANADIAN TIRE #567")

    def test_default_taxable(self):
        assert CanadianTireOverlay.TAX_CODE_DEFAULT == "T"

    def test_default_gl(self):
        item = _apply_default_gl(CanadianTireOverlay)
        assert item["gl_account"] == "5500"


class TestRenoDepot:
    def test_vendor_matched(self):
        assert RenoDepotOverlay.matches("RENO-DEPOT Montreal")

    def test_default_taxable(self):
        assert RenoDepotOverlay.TAX_CODE_DEFAULT == "T"

    def test_default_gl(self):
        item = _apply_default_gl(RenoDepotOverlay)
        assert item["gl_account"] == "5500"


class TestPatrickMorin:
    def test_vendor_matched(self):
        assert PatrickMorinOverlay.matches("PATRICK MORIN QUINCAILLERIE")

    def test_default_taxable(self):
        assert PatrickMorinOverlay.TAX_CODE_DEFAULT == "T"

    def test_default_gl(self):
        item = _apply_default_gl(PatrickMorinOverlay)
        assert item["gl_account"] == "5500"


# ---------------------------------------------------------------------------
# Other (5 × 3 = 15)
# ---------------------------------------------------------------------------

class TestDollarama:
    def test_vendor_matched(self):
        assert DollaramaOverlay.matches("DOLLARAMA #999")

    def test_default_taxable(self):
        assert DollaramaOverlay.TAX_CODE_DEFAULT == "T"

    def test_default_gl_office_supplies(self):
        item = _apply_default_gl(DollaramaOverlay)
        assert item["gl_account"] == "5410"


class TestSAQ:
    def test_vendor_matched(self):
        assert SAQOverlay.matches("SAQ SIGNATURE\n#402")
        assert SAQOverlay.matches("SOCIETE DES ALCOOLS DU QUEBEC")

    def test_alcohol_taxable(self):
        assert SAQOverlay.classify_line_tax("Vin rouge Bordeaux") == "T"

    def test_default_gl(self):
        item = _apply_default_gl(SAQOverlay)
        assert item["gl_account"] == "5420"


class TestStaples:
    def test_vendor_matched(self):
        assert StaplesOverlay.matches("BUREAU EN GROS")
        assert StaplesOverlay.matches("STAPLES #123")

    def test_default_taxable(self):
        assert StaplesOverlay.TAX_CODE_DEFAULT == "T"

    def test_default_gl_office(self):
        item = _apply_default_gl(StaplesOverlay)
        assert item["gl_account"] == "5410"


class TestAmazon:
    def test_vendor_matched(self):
        assert AmazonOverlay.matches("AMAZON.CA ORDER")
        assert AmazonOverlay.matches("AMZN Mktp CA")

    def test_mixed_tax_expected(self):
        assert AmazonOverlay.MIXED_TAX_EXPECTED

    def test_default_gl(self):
        item = _apply_default_gl(AmazonOverlay)
        assert item["gl_account"] == "5440"


class TestCostco:
    def test_vendor_matched(self):
        assert CostcoOverlay.matches("COSTCO WHOLESALE")

    def test_has_custom_parser(self):
        receipt = (
            "COSTCO\n"
            "1234567 BATTERIES AA\n"
            "12.99 FP\n"
            "7654321 LAIT 2L\n"
            "5.49 P\n"
        )
        items = CostcoOverlay.parse_line_items(receipt)
        assert len(items) == 2

    def test_default_gl(self):
        item = _apply_default_gl(CostcoOverlay)
        assert item["gl_account"] == "5410"


# ---------------------------------------------------------------------------
# Registry-level sanity
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_all_overlays_have_canonical_name(self):
        for ov in OVERLAYS:
            assert ov.VENDOR_CANONICAL, f"{ov.__name__} missing canonical name"

    def test_all_overlays_have_at_least_one_pattern(self):
        for ov in OVERLAYS:
            assert ov.VENDOR_PATTERNS, f"{ov.__name__} has no patterns"

    def test_count_is_at_least_25(self):
        # Spec said "~25"; we shipped 36. Guard against accidental removal.
        assert len(OVERLAYS) >= 25

    def test_all_patterns_compile(self):
        import re
        for ov in OVERLAYS:
            for pat in ov.VENDOR_PATTERNS:
                re.compile(pat)  # raises on bad regex

    def test_all_gl_codes_are_valid(self):
        from src.engines.ai_validator import VALID_GL_ACCOUNTS
        for ov in OVERLAYS:
            assert ov.DEFAULT_GL_ACCOUNT in VALID_GL_ACCOUNTS, (
                f"{ov.__name__} GL {ov.DEFAULT_GL_ACCOUNT} not in VALID_GL_ACCOUNTS"
            )

    def test_all_tax_codes_are_valid(self):
        from src.engines.ai_validator import VALID_TAX_CODES
        for ov in OVERLAYS:
            assert ov.TAX_CODE_DEFAULT in VALID_TAX_CODES

    def test_list_overlays_returns_structured_rows(self):
        rows = list_overlays()
        assert len(rows) == len(OVERLAYS)
        assert all("canonical" in r and "patterns" in r for r in rows)

    def test_overlay_hints_by_name(self):
        hints = overlay_hints("Tim Hortons")
        assert hints["canonical"] == "Tim Hortons"
        assert hints["gl_account"] == "5420"

    def test_overlay_hints_no_match(self):
        assert overlay_hints("") == {}
        assert overlay_hints("RandomFakeVendor999") == {}

    def test_find_overlay_by_name_handles_none(self):
        assert find_overlay_by_name(None) is None

    def test_find_overlay_handles_none(self):
        assert find_overlay(None) is None

    def test_two_overlays_dont_double_match_common_brand(self):
        # "Metro" should resolve specifically to Metro, not a substring match
        # elsewhere.
        first_match = find_overlay("METRO PLUS 42")
        assert first_match is MetroOverlay

    def test_apply_merchant_overlay_handles_empty_items(self):
        # Non-empty OCR text without any matching overlay returns the input
        # list unchanged.
        out = apply_merchant_overlay([], "UnknownVendor", "no match here")
        assert out == []

    def test_apply_merchant_overlay_preserves_existing_tax_codes(self):
        items = [
            {"description": "Advil", "total_price": 12.0, "tax_code": "T"},
            {"description": "Rx", "total_price": 19.0, "tax_code": "Z"},
        ]
        out = apply_merchant_overlay(items, "Jean Coutu", "JEAN COUTU")
        assert out[0]["tax_code"] == "T"
        assert out[1]["tax_code"] == "Z"
