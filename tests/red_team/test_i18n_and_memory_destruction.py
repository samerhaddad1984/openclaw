"""
RED TEAM: Bilingual i18n integrity, Vendor Memory Poisoning, and Learning Bias attacks.

Tests:
  1-9:   i18n key completeness, placeholder parity, translation quality, locale handling
  10-18: Vendor memory poisoning, cross-client leaks, normalization, staleness
  19-23: Learning bias feedback loops, decay, minimum samples, reset

Run:
    python -m pytest tests/red_team/test_i18n_and_memory_destruction.py -v
"""
from __future__ import annotations

import json
import re
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent.parent
I18N_DIR = ROOT / "src" / "i18n"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_json(name: str) -> dict[str, str]:
    path = I18N_DIR / f"{name}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _extract_placeholders(text: str) -> set[str]:
    """Return all {placeholder} names from a format string."""
    return set(re.findall(r"\{(\w+)\}", text))


def _tmp_db() -> Path:
    """Return a fresh temp DB path for isolated memory tests."""
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    f.close()
    return Path(f.name)


# ===================================================================
# SECTION 1:  I18N ATTACKS  (tests 1-9)
# ===================================================================

class TestI18nKeyCompleteness:
    """Attack 1: Are en.json and fr.json key-complete?"""

    def test_en_keys_present_in_fr(self):
        en = _load_json("en")
        fr = _load_json("fr")
        missing = set(en.keys()) - set(fr.keys())
        assert not missing, f"Keys in en.json missing from fr.json: {sorted(missing)}"

    def test_fr_keys_present_in_en(self):
        en = _load_json("en")
        fr = _load_json("fr")
        missing = set(fr.keys()) - set(en.keys())
        assert not missing, f"Keys in fr.json missing from en.json: {sorted(missing)}"


class TestI18nPlaceholderParity:
    """Attack 2: Placeholder mismatches between EN and FR."""

    def test_all_placeholders_match(self):
        en = _load_json("en")
        fr = _load_json("fr")
        mismatches: list[str] = []
        common_keys = set(en.keys()) & set(fr.keys())
        for key in sorted(common_keys):
            en_ph = _extract_placeholders(en[key])
            fr_ph = _extract_placeholders(fr[key])
            if en_ph != fr_ph:
                mismatches.append(
                    f"  {key}: EN={sorted(en_ph)} vs FR={sorted(fr_ph)}"
                )
        assert not mismatches, (
            "Placeholder mismatches found:\n" + "\n".join(mismatches)
        )


class TestI18nUntranslatedStrings:
    """Attack 3: English text left in fr.json."""

    # Words that appear frequently in English but should be translated in French.
    ENGLISH_GIVEAWAYS = [
        r"\bAccess denied\b",
        r"\bSign in\b",
        r"\bSign out\b",
        r"\bPassword\b",
        r"\bUpload\b",
        r"\bDownload\b",
        r"\bInvalid\b",
        r"\bSubmit\b",
    ]

    # Keys known to be intentionally bilingual or brand names
    EXEMPT_KEYS = {
        "_comment", "switch_lang", "too_many_attempts",
        "handwriting_review_banner", "qr_scan_instructions",
        # Bilingual Revenu Quebec lines are intentionally bilingual
    }

    def test_no_untranslated_strings_in_fr(self):
        fr = _load_json("fr")
        issues: list[str] = []
        for key, value in sorted(fr.items()):
            if key in self.EXEMPT_KEYS:
                continue
            if key.startswith("rq_line_"):
                continue  # intentionally bilingual
            for pattern in self.ENGLISH_GIVEAWAYS:
                if re.search(pattern, value, re.IGNORECASE):
                    issues.append(f"  {key}: contains English '{pattern}' -> '{value[:80]}'")
        assert not issues, (
            "Untranslated English strings in fr.json:\n" + "\n".join(issues)
        )


class TestI18nAccountingTerms:
    """Attack 4: Accounting terms correctly translated."""

    ACCOUNTING_TERMS = {
        # key: (expected_en_substring, expected_fr_substring)
        "bank_import_col_debit":   ("Debit",   "bit"),
        "bank_import_col_credit":  ("Credit",  "dit"),
        "fs_current_liabilities":  ("Liabilities", "Passif"),
        "fs_equity":               ("Equity",  "Capitaux"),
        "fs_revenue":              ("Revenue", "Produits"),
        "fs_expenses":             ("Expenses", "Charges"),
        "fs_net_income":           ("Net Income", "sultat net"),
        "fs_balance_sheet":        ("Balance Sheet", "Bilan"),
        "col_gl_account":          ("GL Account", "Compte GL"),
    }

    def test_accounting_terms_translated(self):
        en = _load_json("en")
        fr = _load_json("fr")
        failures: list[str] = []
        for key, (en_sub, fr_sub) in self.ACCOUNTING_TERMS.items():
            en_val = en.get(key, "")
            fr_val = fr.get(key, "")
            if en_sub.lower() not in en_val.lower():
                failures.append(f"  EN {key}: expected '{en_sub}' in '{en_val}'")
            if fr_sub.lower() not in fr_val.lower():
                failures.append(f"  FR {key}: expected '{fr_sub}' in '{fr_val}'")
        assert not failures, (
            "Accounting term translation issues:\n" + "\n".join(failures)
        )


class TestI18nTaxLabels:
    """Attack 5: GST=TPS, QST=TVQ, HST=TVH in French."""

    def test_gst_is_tps_in_french(self):
        fr = _load_json("fr")
        # Check filing and invoice keys that contain GST references
        gst_keys = [k for k in fr if "gst" in k.lower() and not k.startswith("_")]
        for key in gst_keys:
            val = fr[key]
            # French should use TPS, not GST (except bilingual labels)
            if "GST" in val and "TPS" not in val:
                pytest.fail(f"FR key '{key}' uses GST without TPS: '{val}'")

    def test_qst_is_tvq_in_french(self):
        fr = _load_json("fr")
        qst_keys = [k for k in fr if "qst" in k.lower() and not k.startswith("_")]
        for key in qst_keys:
            val = fr[key]
            if "QST" in val and "TVQ" not in val:
                pytest.fail(f"FR key '{key}' uses QST without TVQ: '{val}'")

    def test_hst_is_tvh_in_french(self):
        fr = _load_json("fr")
        en = _load_json("en")
        hst_keys = [k for k in en if "hst" in k.lower() and not k.startswith("_")]
        for key in hst_keys:
            fr_val = fr.get(key, "")
            en_val = en.get(key, "")
            if "HST" in en_val:
                assert "TVH" in fr_val, (
                    f"FR key '{key}' should translate HST to TVH: EN='{en_val}' FR='{fr_val}'"
                )


class TestI18nErrorMessagesTranslated:
    """Attack 6: Error messages reaching users are translated."""

    def test_error_keys_have_french_translations(self):
        en = _load_json("en")
        fr = _load_json("fr")
        err_keys = [k for k in en if k.startswith("err_")]
        untranslated: list[str] = []
        for key in err_keys:
            fr_val = fr.get(key)
            en_val = en[key]
            if fr_val is None:
                untranslated.append(f"  {key}: missing in fr.json")
            elif fr_val == en_val:
                untranslated.append(f"  {key}: FR identical to EN -> '{en_val[:60]}'")
        assert not untranslated, (
            "Error messages not translated:\n" + "\n".join(untranslated)
        )

    def test_flash_keys_have_french_translations(self):
        en = _load_json("en")
        fr = _load_json("fr")
        flash_keys = [k for k in en if k.startswith("flash_")]
        untranslated: list[str] = []
        for key in flash_keys:
            fr_val = fr.get(key)
            en_val = en[key]
            if fr_val is None:
                untranslated.append(f"  {key}: missing in fr.json")
            elif fr_val == en_val:
                untranslated.append(f"  {key}: FR identical to EN -> '{en_val[:60]}'")
        assert not untranslated, (
            "Flash messages not translated:\n" + "\n".join(untranslated)
        )


class TestI18nMissingKeyFallback:
    """Attack 7: What happens when requesting a key that doesn't exist?"""

    def test_missing_key_returns_key_itself(self):
        from src.i18n import t, reload_cache
        reload_cache()
        result = t("this_key_does_not_exist_xyz", "en")
        assert result == "this_key_does_not_exist_xyz", (
            f"Missing key should return the key itself, got: '{result}'"
        )

    def test_missing_key_in_en_falls_back_to_fr(self):
        """If a key exists in FR but not EN, the EN lookup should fall back to FR."""
        from src.i18n import t, reload_cache
        reload_cache()
        fr = _load_json("fr")
        fr_only_keys = set(fr.keys()) - set(_load_json("en").keys())
        # If there are FR-only keys, verify fallback
        if fr_only_keys:
            key = next(iter(fr_only_keys))
            result = t(key, "en")
            assert result == fr[key], (
                f"EN lookup for FR-only key '{key}' should fallback to FR value"
            )
        else:
            # All keys present in both -- just confirm fallback mechanism works
            result = t("nonexistent_key_abc", "en")
            assert result == "nonexistent_key_abc"

    def test_missing_key_never_returns_empty(self):
        from src.i18n import t, reload_cache
        reload_cache()
        result = t("", "en")
        # Empty key should return empty string (the key itself)
        assert result is not None


class TestI18nUnsupportedLanguage:
    """Attack 8: What happens with language='de' (unsupported)?"""

    def test_unsupported_language_falls_back_to_french(self):
        from src.i18n import t, reload_cache
        reload_cache()
        result_de = t("login_title", "de")
        result_fr = t("login_title", "fr")
        assert result_de == result_fr, (
            f"Unsupported 'de' should fallback to FR. Got DE='{result_de}' FR='{result_fr}'"
        )

    def test_unsupported_language_with_placeholder(self):
        from src.i18n import t, reload_cache
        reload_cache()
        result = t("change_pw_intro", "de", name="Pierre")
        assert "Pierre" in result, (
            f"Placeholder substitution should work even with fallback locale: '{result}'"
        )

    def test_various_unsupported_codes(self):
        from src.i18n import t, reload_cache
        reload_cache()
        for lang in ["de", "es", "zh", "ja", "", None, "EN", "FR"]:
            result = t("login_title", lang)
            assert result and result != "", (
                f"Language '{lang}' should never produce empty string"
            )


class TestI18nNumberFormatting:
    """Attack 9: Number formatting for locale (1,234.56 EN vs 1 234,56 FR)."""

    def test_inv_qst_rate_formatting(self):
        """QST rate: EN uses 9.975% and FR should use 9,975 %."""
        en = _load_json("en")
        fr = _load_json("fr")
        en_qst = en.get("inv_qst", "")
        fr_qst = fr.get("inv_qst", "")
        # EN should use period as decimal
        assert "9.975" in en_qst or "9.975" in en_qst.replace("\u00a0", " "), (
            f"EN inv_qst should use period decimal: '{en_qst}'"
        )
        # FR should use comma as decimal separator
        assert "9,975" in fr_qst, (
            f"FR inv_qst should use comma decimal: '{fr_qst}'"
        )

    def test_quick_method_rates_locale_format(self):
        """Quick method rates should respect locale decimal conventions."""
        en = _load_json("en")
        fr = _load_json("fr")
        en_retail = en.get("rq_quick_method_retail", "")
        fr_retail = fr.get("rq_quick_method_retail", "")
        # EN: 1.8%, 3.4%
        assert "1.8" in en_retail, f"EN should use period: '{en_retail}'"
        # FR: 1,8 %, 3,4 %
        assert "1,8" in fr_retail, f"FR should use comma: '{fr_retail}'"

    def test_max_file_size_locale(self):
        """File size limit: EN '20 MB' vs FR '20 Mo'."""
        en = _load_json("en")
        fr = _load_json("fr")
        en_val = en.get("file_too_large", "")
        fr_val = fr.get("file_too_large", "")
        assert "MB" in en_val, f"EN should say MB: '{en_val}'"
        assert "Mo" in fr_val, f"FR should say Mo: '{fr_val}'"


# ===================================================================
# SECTION 2:  VENDOR MEMORY POISONING  (tests 10-18)
# ===================================================================

class TestVendorMemoryPoisoning:
    """Attacks 10-11: Can a bad actor poison vendor memory with wrong GL accounts?"""

    def _make_store(self):
        from src.agents.core.vendor_memory_store import VendorMemoryStore
        db = _tmp_db()
        return VendorMemoryStore(db_path=db), db

    def test_poisoning_with_wrong_gl_account(self):
        """Attack 10: Submit corrections with wrong GL -- does the system accept them?"""
        store, _ = self._make_store()
        # Correct mapping: Hydro-Quebec -> Utilities Expense
        store.record_approval(
            vendor="Hydro-Quebec", gl_account="Utilities Expense",
            tax_code="TX1", client_code="ACME", source="approval",
        )
        # Bad actor submits 5 "corrections" mapping to wrong account
        for i in range(5):
            store.record_approval(
                vendor="Hydro-Quebec", gl_account="Entertainment Expense",
                tax_code="TX1", client_code="ACME",
                source="manual_correction",
            )
        # Now look up the best match
        best = store.get_best_match(vendor="Hydro-Quebec", client_code="ACME")
        assert best is not None
        # The poisoned account should have higher approval_count
        # FINDING: the system DOES learn the poisoned mapping
        if best["gl_account"] == "Entertainment Expense":
            pytest.xfail(
                "VULNERABILITY: 5 poisoned corrections override 1 correct approval. "
                f"approval_count={best['approval_count']}, confidence={best['confidence']}"
            )

    def test_five_wrong_corrections_override_correct(self):
        """Attack 11: After 5 corrections to wrong GL, does system learn wrong mapping?"""
        store, _ = self._make_store()
        # First record 3 correct approvals
        for _ in range(3):
            store.record_approval(
                vendor="Bell Canada", gl_account="Telecom Expense",
                tax_code="TX1", client_code="ACME",
            )
        # Then record 5 bad corrections
        for _ in range(5):
            store.record_approval(
                vendor="Bell Canada", gl_account="Office Supplies",
                tax_code="TX1", client_code="ACME",
            )
        best = store.get_best_match(vendor="Bell Canada", client_code="ACME")
        assert best is not None
        # The system stores them separately (different gl_account rows),
        # but get_best_match returns the one with highest approval_count.
        if best["gl_account"] == "Office Supplies":
            pytest.xfail(
                "VULNERABILITY: 5 poisoned overrides 3 correct. "
                "No rate-limiting on corrections."
            )


class TestVendorMemoryVsDocumentEvidence:
    """Attack 12: Can vendor memory override document evidence?"""

    def test_memory_suggests_wrong_category(self):
        """Memory says 'office supplies' but document evidence says 'insurance'."""
        from src.agents.core.vendor_memory_engine import VendorMemoryEngine
        db = _tmp_db()
        engine = VendorMemoryEngine(db_path=db)
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE learning_memory (id INTEGER PRIMARY KEY, vendor TEXT, client_code TEXT, doc_type TEXT, field_name TEXT, new_value TEXT)")
        # Poison the learning memory with wrong category
        for i in range(10):
            conn.execute(
                "INSERT INTO learning_memory (vendor, client_code, doc_type, field_name, new_value) VALUES (?, ?, ?, ?, ?)",
                ("Desjardins Assurance", "ACME", "invoice", "category", "Office Supplies"),
            )
        conn.commit()
        conn.close()

        suggestions = engine.suggest_fields_for_document(
            vendor="Desjardins Assurance",
            client_code="ACME",
            doc_type="invoice",
            min_support=2,
        )
        if "category" in suggestions:
            sugg = suggestions["category"]
            if sugg["suggested_value"] == "Office Supplies":
                pytest.xfail(
                    "VULNERABILITY: Vendor memory suggests 'Office Supplies' for an "
                    "insurance vendor -- no cross-check against document content."
                )


class TestVendorNameNormalization:
    """Attacks 13-15: Near-match vendor names, case sensitivity, accents."""

    def _make_engine(self):
        from src.agents.core.vendor_memory_engine import VendorMemoryEngine, normalize_key
        db = _tmp_db()
        engine = VendorMemoryEngine(db_path=db)
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE learning_memory (id INTEGER PRIMARY KEY, vendor TEXT, client_code TEXT, doc_type TEXT, field_name TEXT, new_value TEXT)")
        conn.commit()
        return engine, conn, db

    def test_near_match_vendor_hyphen_vs_space(self):
        """Attack 13: 'Hydro-Quebec' vs 'Hydro Quebec' vs 'hydro-quebec'."""
        from src.agents.core.vendor_memory_engine import normalize_key
        k1 = normalize_key("Hydro-Quebec")
        k2 = normalize_key("Hydro Quebec")
        k3 = normalize_key("hydro-quebec")
        # All should normalize to the same key for safe matching
        if k1 != k2 or k2 != k3:
            pytest.xfail(
                f"VULNERABILITY: Vendor name variants produce different keys: "
                f"'{k1}' vs '{k2}' vs '{k3}'. "
                "An attacker could create shadow vendor entries."
            )

    def test_case_sensitivity(self):
        """Attack 14: 'BELL CANADA' vs 'Bell Canada' vs 'bell canada'."""
        from src.agents.core.vendor_memory_engine import normalize_key
        k1 = normalize_key("BELL CANADA")
        k2 = normalize_key("Bell Canada")
        k3 = normalize_key("bell canada")
        assert k1 == k2 == k3, (
            f"Case sensitivity vulnerability: '{k1}' vs '{k2}' vs '{k3}'"
        )

    def test_accent_normalization(self):
        """Attack 15: Accented characters (e, e, e, e)."""
        from src.agents.core.vendor_memory_engine import normalize_key
        k1 = normalize_key("Hydro-Quebec")
        k2 = normalize_key("Hydro-Qu\u00e9bec")  # e with acute
        # Ideally these should match; accents are common in Quebec French
        if k1 != k2:
            pytest.xfail(
                f"VULNERABILITY: Accented vendor names produce different keys: "
                f"'{k1}' vs '{k2}'. Attackers can exploit accent variants."
            )

    def test_vendor_memory_store_case_insensitive_lookup(self):
        """Vendor memory store: lookup is case-insensitive via vendor_key."""
        from src.agents.core.vendor_memory_store import VendorMemoryStore
        db = _tmp_db()
        store = VendorMemoryStore(db_path=db)
        store.record_approval(
            vendor="Bell Canada", gl_account="Telecom", tax_code="TX1",
        )
        # Use min_support=1 — this test checks case-insensitivity, not suggestion quality
        result = store.get_best_match(vendor="BELL CANADA", min_support=1)
        assert result is not None, "Lookup should be case-insensitive"
        assert result["gl_account"] == "Telecom"

    def test_vendor_memory_store_accent_lookup(self):
        """Vendor memory store: lookup with/without accents."""
        from src.agents.core.vendor_memory_store import VendorMemoryStore, normalize_key
        db = _tmp_db()
        store = VendorMemoryStore(db_path=db)
        store.record_approval(
            vendor="Hydro-Qu\u00e9bec", gl_account="Utilities", tax_code="TX1",
        )
        k_accent = normalize_key("Hydro-Qu\u00e9bec")
        k_plain = normalize_key("Hydro-Quebec")
        if k_accent != k_plain:
            result = store.get_best_match(vendor="Hydro-Quebec")
            if result is None:
                pytest.xfail(
                    "VULNERABILITY: Cannot find 'Hydro-Quebec' when stored as "
                    "'Hydro-Qu\u00e9bec' -- accent normalization missing."
                )


class TestCrossClientLeakage:
    """Attack 16: Can one client's corrections leak to another?"""

    def test_client_isolation_in_vendor_memory_store(self):
        from src.agents.core.vendor_memory_store import VendorMemoryStore
        db = _tmp_db()
        store = VendorMemoryStore(db_path=db)
        # Client A records vendor mapping
        store.record_approval(
            vendor="Staples", gl_account="Office Supplies",
            tax_code="TX1", client_code="CLIENT_A",
        )
        # Client B should not see Client A's mapping as preferred
        result = store.get_best_match(
            vendor="Staples", client_code="CLIENT_B",
        )
        # The store does ORDER BY client_code match first, but falls back
        if result is not None and result.get("client_code") == "CLIENT_A":
            pytest.xfail(
                "VULNERABILITY: Client B's lookup returns Client A's vendor memory. "
                "Cross-client leakage detected."
            )

    def test_client_isolation_in_vendor_memory_engine(self):
        from src.agents.core.vendor_memory_engine import VendorMemoryEngine
        db = _tmp_db()
        engine = VendorMemoryEngine(db_path=db)
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE learning_memory (id INTEGER PRIMARY KEY, vendor TEXT, client_code TEXT, doc_type TEXT, field_name TEXT, new_value TEXT)")
        # Client A corrections
        for _ in range(5):
            conn.execute(
                "INSERT INTO learning_memory (vendor, client_code, doc_type, field_name, new_value) VALUES (?, ?, ?, ?, ?)",
                ("Staples", "CLIENT_A", "invoice", "gl_account", "Office Supplies A"),
            )
        conn.commit()
        conn.close()

        # Client B lookup -- should not inherit Client A's mappings at the
        # client_vendor_doc_type context level
        suggestions = engine.suggest_fields_for_document(
            vendor="Staples",
            client_code="CLIENT_B",
            doc_type="invoice",
            min_support=2,
        )
        if "gl_account" in suggestions:
            sugg = suggestions["gl_account"]
            # If it matched at vendor_only level, that's a weaker leak but still a concern
            if sugg["source"] == "client_vendor_doc_type":
                pytest.xfail(
                    "VULNERABILITY: Client B got suggestions from Client A's "
                    "client-specific memory context."
                )
            elif sugg["suggested_value"] == "Office Supplies A":
                # Leaked via vendor_only context
                pytest.xfail(
                    "FINDING: Client A's corrections influence Client B via "
                    f"vendor_only context (source={sugg['source']}). "
                    "This is cross-client leakage at a broad level."
                )


class TestVendorMemoryConfidenceConflict:
    """Attack 17: High vendor memory confidence vs low document confidence."""

    def test_high_memory_low_doc_confidence(self):
        from src.agents.core.vendor_memory_store import VendorMemoryStore
        db = _tmp_db()
        store = VendorMemoryStore(db_path=db)
        # Build high-confidence memory (many approvals)
        for i in range(20):
            store.record_approval(
                vendor="Rogers", gl_account="Telecom",
                tax_code="TX1", client_code="ACME",
                confidence=0.95,
            )
        best = store.get_best_match(vendor="Rogers", client_code="ACME")
        assert best is not None
        # High approval_count and confidence
        assert best["approval_count"] >= 20
        # FINDING: There is no mechanism to compare memory confidence against
        # current document extraction confidence.
        # The system blindly trusts high-frequency memory regardless of
        # the current document's actual content.
        assert best["confidence"] > 0.5, (
            f"Expected high confidence after 20 approvals, got {best['confidence']}"
        )


class TestStaleVendorMemory:
    """Attack 18: Old patterns from 2 years ago still influencing decisions."""

    def test_no_time_decay_in_vendor_memory(self):
        """FIX P0-2: Vendor memory now filters out patterns older than 24 months.
        A 4-year-old pattern should no longer influence suggestions."""
        from src.agents.core.vendor_memory_store import VendorMemoryStore
        db = _tmp_db()
        store = VendorMemoryStore(db_path=db)
        # Record approval with old timestamp
        store.record_approval(
            vendor="Acme Corp", gl_account="Old Account",
            tax_code="TX1", client_code="CLIENT",
        )
        # Manually backdate the created_at and updated_at to 4 years ago
        conn = sqlite3.connect(str(db))
        conn.execute(
            "UPDATE vendor_memory SET created_at='2022-01-01T00:00:00+00:00', updated_at='2022-01-01T00:00:00+00:00'"
        )
        conn.commit()
        conn.close()

        # FIX P0-2: 4-year-old pattern should be filtered out by 24-month cutoff
        best = store.get_best_match(vendor="Acme Corp", client_code="CLIENT", min_support=1)
        assert best is None, (
            "FIX P0-2: 4-year-old vendor memory should be excluded by 24-month time decay"
        )

    def test_engine_no_time_filter(self):
        """VendorMemoryEngine filters out documents older than 24 months."""
        from src.agents.core.vendor_memory_engine import VendorMemoryEngine
        db = _tmp_db()
        engine = VendorMemoryEngine(db_path=db)
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE documents (document_id TEXT, vendor TEXT, client_code TEXT, doc_type TEXT, gl_account TEXT, tax_code TEXT, category TEXT, amount TEXT, review_status TEXT, created_at TEXT)")
        # Insert document with a 4-year-old created_at timestamp
        conn.execute(
            "INSERT INTO documents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("DOC1", "OldVendor", "CLIENT", "invoice", "Ancient Account", "TX1", "Misc", "100.00", "Ready", "2022-01-01T00:00:00+00:00"),
        )
        conn.commit()
        conn.close()

        suggestions = engine.suggest_fields_for_document(
            vendor="OldVendor", client_code="CLIENT", doc_type="invoice",
            min_support=1, min_confidence=0.5,
        )
        # After FIX: 4-year-old document should be excluded by 24-month cutoff
        assert "gl_account" not in suggestions, (
            "Engine should filter out documents older than 24 months"
        )


# ===================================================================
# SECTION 3:  LEARNING BIAS ATTACKS  (tests 19-23)
# ===================================================================

class TestLearningOverTrust:
    """Attack 19: Does the system over-trust learned patterns vs fresh document evidence?"""

    def test_learning_memory_has_no_confidence_threshold(self):
        """Learning patterns are stored regardless of confidence level."""
        from src.agents.core.learning_memory_store import LearningMemoryStore
        db = _tmp_db()
        store = LearningMemoryStore(db_path=db)
        # Record a pattern with very low confidence
        result = store.record_feedback({
            "vendor": "TestVendor",
            "event_type": "posted_successfully",
            "gl_account": "Fake Account",
            "tax_code": "TX1",
            "confidence": 0.05,
        })
        assert result["ok"] is True
        # Now check: the pattern is stored regardless of confidence
        match = store.get_best_match(
            event_type="posted_successfully",
            vendor="TestVendor",
            gl_account="Fake Account",
            tax_code="TX1",
        )
        assert match is not None, "Low-confidence pattern stored and retrievable"
        # FINDING: no minimum confidence threshold to store patterns
        assert match["avg_confidence"] == pytest.approx(0.05, abs=0.01)


class TestLearningFeedbackLoop:
    """Attack 20: Can learning memory create a feedback loop?"""

    def test_feedback_loop_amplification(self):
        """Wrong classification -> learned -> used to classify -> more wrong."""
        from src.agents.core.learning_memory_store import LearningMemoryStore
        db = _tmp_db()
        store = LearningMemoryStore(db_path=db)
        # Simulate a feedback loop: same wrong pattern recorded repeatedly
        for i in range(10):
            store.record_feedback({
                "vendor": "LoopVendor",
                "event_type": "posted_successfully",
                "gl_account": "Wrong Account",
                "tax_code": "TX1",
                "category": "Wrong Category",
                "confidence": 0.3 + (i * 0.07),  # confidence grows each time
            })
        match = store.get_best_match(
            event_type="posted_successfully",
            vendor="LoopVendor",
            gl_account="Wrong Account",
            tax_code="TX1",
            category="Wrong Category",
        )
        assert match is not None
        assert match["outcome_count"] == 10
        # Average confidence increases over time -- feedback loop in action
        assert match["avg_confidence"] > 0.5, (
            f"Feedback loop: avg_confidence={match['avg_confidence']} after 10 iterations. "
            "The system has no mechanism to break this loop."
        )


class TestLearningDecayMechanism:
    """Attack 21: Is there a decay mechanism for old patterns?"""

    def test_no_decay_on_old_patterns(self):
        from src.agents.core.learning_memory_store import LearningMemoryStore
        db = _tmp_db()
        store = LearningMemoryStore(db_path=db)
        store.record_feedback({
            "vendor": "DecayTest",
            "event_type": "posted_successfully",
            "gl_account": "Old Pattern",
            "tax_code": "TX1",
            "confidence": 0.9,
        })
        # Backdate
        conn = sqlite3.connect(str(db))
        conn.execute(
            "UPDATE learning_memory_patterns SET created_at='2022-01-01T00:00:00+00:00', updated_at='2022-01-01T00:00:00+00:00'"
        )
        conn.commit()
        conn.close()

        match = store.get_best_match(
            event_type="posted_successfully",
            vendor="DecayTest",
            gl_account="Old Pattern",
            tax_code="TX1",
        )
        assert match is not None
        # FINDING: No decay mechanism exists
        assert match["avg_confidence"] == pytest.approx(0.9, abs=0.01), (
            "Pattern confidence unchanged after 2+ years with no decay."
        )

    def test_correction_store_no_decay(self):
        from src.agents.core.learning_correction_store import LearningCorrectionStore
        db = _tmp_db()
        store = LearningCorrectionStore(db_path=db)
        store.record_correction(
            vendor="DecayTest", field_name="gl_account",
            old_value="Old", new_value="New", client_code="ACME",
        )
        # Backdate
        conn = sqlite3.connect(str(db))
        conn.execute(
            "UPDATE learning_corrections SET created_at='2022-01-01T00:00:00+00:00', updated_at='2022-01-01T00:00:00+00:00'"
        )
        conn.commit()
        conn.close()

        result = store.suggest(
            vendor="DecayTest", field_name="gl_account",
            old_value="Old", client_code="ACME",
        )
        if result.get("found"):
            pytest.xfail(
                "FINDING: 2-year-old correction still influences suggestions "
                "with no decay mechanism."
            )


class TestMinimumSamplesBeforeTrust:
    """Attack 22: Minimum number of samples before a pattern is trusted."""

    def test_vendor_memory_engine_min_support_default(self):
        """Default min_support is 2 -- is that enough?"""
        from src.agents.core.vendor_memory_engine import VendorMemoryEngine
        db = _tmp_db()
        engine = VendorMemoryEngine(db_path=db)
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE learning_memory (id INTEGER PRIMARY KEY, vendor TEXT, client_code TEXT, doc_type TEXT, field_name TEXT, new_value TEXT)")
        # Insert exactly 2 rows -- minimum support
        for _ in range(2):
            conn.execute(
                "INSERT INTO learning_memory (vendor, client_code, doc_type, field_name, new_value) VALUES (?, ?, ?, ?, ?)",
                ("MinTest", "ACME", "invoice", "gl_account", "Suspicious Account"),
            )
        conn.commit()
        conn.close()

        suggestions = engine.suggest_fields_for_document(
            vendor="MinTest", client_code="ACME", doc_type="invoice",
        )
        if "gl_account" in suggestions:
            sugg = suggestions["gl_account"]
            assert sugg["support"] == 2
            # FINDING: Only 2 samples needed to influence suggestions
            # This is a very low bar for trusting a pattern

    def test_single_sample_blocked(self):
        """A single sample should NOT produce a suggestion at default min_support=2."""
        from src.agents.core.vendor_memory_engine import VendorMemoryEngine
        db = _tmp_db()
        engine = VendorMemoryEngine(db_path=db)
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE learning_memory (id INTEGER PRIMARY KEY, vendor TEXT, client_code TEXT, doc_type TEXT, field_name TEXT, new_value TEXT)")
        conn.execute(
            "INSERT INTO learning_memory (vendor, client_code, doc_type, field_name, new_value) VALUES (?, ?, ?, ?, ?)",
            ("SingleTest", "ACME", "invoice", "gl_account", "One-Shot Account"),
        )
        conn.commit()
        conn.close()

        suggestions = engine.suggest_fields_for_document(
            vendor="SingleTest", client_code="ACME", doc_type="invoice",
        )
        assert "gl_account" not in suggestions, (
            "Single sample should NOT generate a suggestion with default min_support=2"
        )

    def test_vendor_memory_store_trusts_first_approval(self):
        """VendorMemoryStore records confidence=0.2 on first approval -- very low bar."""
        from src.agents.core.vendor_memory_store import VendorMemoryStore
        db = _tmp_db()
        store = VendorMemoryStore(db_path=db)
        result = store.record_approval(
            vendor="FirstTime", gl_account="Acc1", tax_code="TX1",
        )
        assert result["ok"]
        assert result["approval_count"] == 1
        assert result["confidence"] == pytest.approx(0.2, abs=0.01)
        # FINDING: Single approval creates a memory entry with confidence 0.2


class TestLearningResetCapability:
    """Attack 23: Can learning be reset per-vendor or per-client?"""

    def test_vendor_memory_store_has_no_reset_method(self):
        """Check if VendorMemoryStore has a reset or delete method."""
        from src.agents.core.vendor_memory_store import VendorMemoryStore
        store_methods = [m for m in dir(VendorMemoryStore) if not m.startswith("_")]
        reset_methods = [m for m in store_methods if "reset" in m.lower() or "delete" in m.lower() or "clear" in m.lower() or "purge" in m.lower()]
        if not reset_methods:
            pytest.xfail(
                f"VULNERABILITY: VendorMemoryStore has no reset/delete/clear method. "
                f"Available methods: {store_methods}. "
                "Cannot reset poisoned vendor memory."
            )

    def test_learning_memory_store_has_no_reset_method(self):
        """Check if LearningMemoryStore has a reset or delete method."""
        from src.agents.core.learning_memory_store import LearningMemoryStore
        store_methods = [m for m in dir(LearningMemoryStore) if not m.startswith("_")]
        reset_methods = [m for m in store_methods if "reset" in m.lower() or "delete" in m.lower() or "clear" in m.lower() or "purge" in m.lower()]
        if not reset_methods:
            pytest.xfail(
                f"VULNERABILITY: LearningMemoryStore has no reset/delete/clear method. "
                f"Available methods: {store_methods}. "
                "Cannot reset biased learning patterns."
            )

    def test_learning_correction_store_has_no_reset_method(self):
        """Check if LearningCorrectionStore has a reset or delete method."""
        from src.agents.core.learning_correction_store import LearningCorrectionStore
        store_methods = [m for m in dir(LearningCorrectionStore) if not m.startswith("_")]
        reset_methods = [m for m in store_methods if "reset" in m.lower() or "delete" in m.lower() or "clear" in m.lower() or "purge" in m.lower()]
        if not reset_methods:
            pytest.xfail(
                f"VULNERABILITY: LearningCorrectionStore has no reset/delete/clear method. "
                f"Available methods: {store_methods}. "
                "Cannot reset learned corrections."
            )

    def test_rejection_only_lowers_confidence_slightly(self):
        """VendorMemoryStore.record_rejection only lowers confidence by 0.2."""
        from src.agents.core.vendor_memory_store import VendorMemoryStore
        db = _tmp_db()
        store = VendorMemoryStore(db_path=db)
        # Build up high confidence
        for _ in range(10):
            store.record_approval(
                vendor="PoisonVendor", gl_account="Bad Account",
                tax_code="TX1", client_code="ACME",
            )
        before = store.get_best_match(vendor="PoisonVendor", client_code="ACME")
        assert before is not None
        initial_confidence = before["confidence"]

        # Now reject -- only drops by 0.2
        store.record_rejection(
            vendor="PoisonVendor", gl_account="Bad Account",
            tax_code="TX1", client_code="ACME",
        )
        after = store.get_best_match(vendor="PoisonVendor", client_code="ACME")
        assert after is not None
        confidence_drop = initial_confidence - after["confidence"]
        # FINDING: rejection only drops confidence by 0.2, regardless of
        # how wrong the mapping is. Need 5 rejections to undo 10 approvals.
        assert confidence_drop == pytest.approx(0.2, abs=0.05), (
            f"Rejection dropped confidence by {confidence_drop} (expected ~0.2)"
        )
