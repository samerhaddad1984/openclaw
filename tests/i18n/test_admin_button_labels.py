"""Regression guards for CPA admin button labels.

Migrated admin buttons in ``scripts/review_dashboard.py`` now render
via :func:`src.i18n.ui_labels.ui_t`. These tests cover:

- every label in the dict has both fr and en values,
- Quebec-specific tax terminology is correct (TPS / TVQ, not GST / QST in FR),
- no critical admin button hardcodes English text anymore,
- helpers fall back sensibly on unknown keys.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.i18n.ui_labels import LABELS, bilingual, ui_t


DASHBOARD_PATH = Path(__file__).resolve().parent.parent.parent / "scripts/review_dashboard.py"


def test_every_label_has_fr_and_en_value() -> None:
    missing_fr = [k for k, v in LABELS.items() if not v.get("fr")]
    missing_en = [k for k, v in LABELS.items() if not v.get("en")]
    assert not missing_fr, f"labels missing FR: {missing_fr}"
    assert not missing_en, f"labels missing EN: {missing_en}"


def test_quebec_tax_acronyms_use_fr_conventions() -> None:
    """GST → TPS, QST → TVQ, HST → TVH. The English side keeps the
    federal English acronyms."""
    assert ui_t("gst", "fr") == "TPS"
    assert ui_t("qst", "fr") == "TVQ"
    assert ui_t("hst", "fr") == "TVH"
    assert ui_t("gst", "en") == "GST"
    assert ui_t("qst", "en") == "QST"


def test_canadian_french_accounting_terminology_is_precise() -> None:
    """Checks against Ordre des CPA du Québec conventional terms."""
    assert ui_t("balance_sheet", "fr") == "Bilan"
    assert ui_t("trial_balance", "fr") == "Balance de vérification"
    assert ui_t("income_statement", "fr") == "État des résultats"
    assert ui_t("cash_flow", "fr") == "Flux de trésorerie"
    assert ui_t("general_ledger", "fr") == "Grand livre"
    assert ui_t("journal_entry", "fr") == "Écriture de journal"
    assert ui_t("chart_of_accounts", "fr") == "Plan comptable"
    assert ui_t("reconciliation", "fr") == "Rapprochement"
    assert ui_t("pl_net_income", "fr") == "Bénéfice net"
    assert ui_t("bs_assets", "fr") == "Actif"
    assert ui_t("bs_liabilities", "fr") == "Passif"
    assert ui_t("bs_equity", "fr") == "Capitaux propres"


@pytest.mark.parametrize("key", [
    "add_client", "edit", "save", "cancel", "submit", "approve", "reject",
    "download_t661", "soce_pdf", "connect_bank", "run_all_detectors",
    "save_narrative", "clear_cache", "calculate_sample",
])
def test_critical_button_has_non_english_fr_value(key: str) -> None:
    """For labels that were English-only before migration, the FR value
    must genuinely differ from EN (proves it's translated, not a
    copy-paste fallback)."""
    fr = ui_t(key, "fr")
    en = ui_t(key, "en")
    assert fr != en, f"{key!r} FR equals EN: {fr!r}"


def test_ui_t_falls_back_gracefully_on_unknown_key() -> None:
    assert ui_t("totally_bogus_key_xxx", "fr") == "totally_bogus_key_xxx"


def test_ui_t_falls_back_to_en_when_unknown_lang() -> None:
    assert ui_t("add_client", "zz") == "Add client"


def test_bilingual_helper_joins_fr_slash_en() -> None:
    assert bilingual("approve") == "Approuver / Approve"
    # Unknown key: returns the key itself (same fallback as ui_t)
    assert bilingual("unknown_key") == "unknown_key"


def test_no_english_leak_in_migrated_admin_buttons() -> None:
    """Scan the dashboard source for the specific admin button strings
    that were migrated. Raw English must no longer appear as button
    inner text.
    """
    content = DASHBOARD_PATH.read_text(encoding="utf-8")
    # Patterns: each entry is (button text, expected ui_t key that
    # should now be used). The test asserts the raw text is not still
    # appearing inside a <button>...</button> or as a .button-link.
    FORBIDDEN_RAW_BUTTONS = [
        ">Run all detectors<",
        ">Add partner<",
        ">Save narrative<",
        ">Calculate &amp; sample<",
        ">Clear Cache<",
        ">SOCE PDF<",
        ">PDF (CAS 580)<",
        ">Management letter (CAS 265)<",
        ">Save changes<",
        ">T5013 PDF<",
        ">Download T661 PDF<",
    ]
    offenders = [p for p in FORBIDDEN_RAW_BUTTONS if p in content]
    assert not offenders, (
        "raw English button labels still present in dashboard — "
        f"migration slipped: {offenders}"
    )


def test_ui_t_import_is_wired_in_dashboard() -> None:
    """Migration requires the import; fail loudly if it's missing."""
    content = DASHBOARD_PATH.read_text(encoding="utf-8")
    assert "from src.i18n.ui_labels import ui_t" in content
