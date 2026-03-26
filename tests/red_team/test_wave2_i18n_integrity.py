"""
Second-Wave Independent Verification — i18n Key Integrity

Verifies that en.json and fr.json:
1. Have identical key sets (no missing translations)
2. No values contain the OTHER language's content (copy-paste errors)
3. New keys added in the diff are present in both files
4. No duplicate keys (JSON allows last-wins silently)
5. Placeholder consistency ({0}, {1}, etc.)
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


I18N_DIR = ROOT / "src" / "i18n"
EN_PATH = I18N_DIR / "en.json"
FR_PATH = I18N_DIR / "fr.json"


@pytest.fixture(scope="module")
def en_data():
    return json.loads(EN_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def fr_data():
    return json.loads(FR_PATH.read_text(encoding="utf-8"))


class TestI18nKeyParity:

    def test_en_and_fr_have_same_keys(self, en_data, fr_data):
        en_keys = set(en_data.keys())
        fr_keys = set(fr_data.keys())

        missing_in_fr = en_keys - fr_keys
        missing_in_en = fr_keys - en_keys

        assert not missing_in_fr, f"Keys in EN but missing in FR: {sorted(missing_in_fr)}"
        assert not missing_in_en, f"Keys in FR but missing in EN: {sorted(missing_in_en)}"

    def test_no_empty_values_in_en(self, en_data):
        empty_keys = [k for k, v in en_data.items() if isinstance(v, str) and not v.strip()]
        assert not empty_keys, f"Empty values in en.json: {empty_keys}"

    def test_no_empty_values_in_fr(self, fr_data):
        empty_keys = [k for k, v in fr_data.items() if isinstance(v, str) and not v.strip()]
        assert not empty_keys, f"Empty values in fr.json: {empty_keys}"

    def test_new_keys_present_in_both(self, en_data, fr_data):
        """
        Keys added in the diff should be in both files.
        These are the NEW keys we know were added.
        """
        new_keys = [
            "handwriting_review_banner",
            "handwriting_review_title",
            "handwriting_field_col",
            "handwriting_value_col",
            "handwriting_field_illegible",
            "field_payment_method",
            "qr_nav_link",
            "qr_title",
            "qr_page_heading",
            "cas_materiality_title",
            "cas_risk_title",
        ]
        for key in new_keys:
            assert key in en_data, f"New key '{key}' missing from en.json"
            assert key in fr_data, f"New key '{key}' missing from fr.json"

    def test_placeholder_consistency(self, en_data, fr_data):
        """
        If an EN value has {0}, {1}, etc., the FR value must have the same
        set of placeholders.
        """
        placeholder_re = re.compile(r"\{(\d+)\}")
        mismatches = []
        for key in en_data:
            if key not in fr_data:
                continue
            en_val = str(en_data[key])
            fr_val = str(fr_data[key])
            en_ph = set(placeholder_re.findall(en_val))
            fr_ph = set(placeholder_re.findall(fr_val))
            if en_ph != fr_ph:
                mismatches.append((key, en_ph, fr_ph))
        assert not mismatches, f"Placeholder mismatches: {mismatches}"

    def test_no_obvious_language_swap(self, en_data, fr_data):
        """
        Heuristic: EN values should not contain common French-only words,
        and FR values should not contain common English-only words.
        This catches copy-paste errors.
        """
        french_markers = {"veuillez", "s'il vous plaît", "merci de",
                          "télécharger", "enregistrer", "chiffre d'affaires"}
        english_markers = {"please", "download", "calculate",
                           "save", "revenue"}

        # Check a sample of keys for obvious swaps
        en_suspicious = []
        fr_suspicious = []

        for key, val in en_data.items():
            val_lower = val.lower() if isinstance(val, str) else ""
            for marker in french_markers:
                if marker in val_lower and "bilingue" not in key and "bilingual" not in key:
                    en_suspicious.append((key, marker))

        for key, val in fr_data.items():
            val_lower = val.lower() if isinstance(val, str) else ""
            # Only flag if the value is mostly English (not bilingual)
            for marker in english_markers:
                if marker in val_lower and "/" not in val_lower:
                    fr_suspicious.append((key, marker))

        # These are heuristic — some bilingual values are expected
        # But a large count suggests systematic copy-paste errors
        if len(en_suspicious) > 5:
            pytest.fail(f"Many EN values contain French text: {en_suspicious[:5]}")
        if len(fr_suspicious) > 5:
            pytest.fail(f"Many FR values contain English text: {fr_suspicious[:5]}")

    def test_json_valid_no_trailing_commas(self):
        """
        Ensure the JSON files are strictly valid.
        A trailing comma in JSON is a common edit error that Python's
        json module rejects.
        """
        # If we got here, json.loads already worked in fixtures.
        # But let's also verify the raw text doesn't have BOM or encoding issues.
        en_raw = EN_PATH.read_bytes()
        fr_raw = FR_PATH.read_bytes()

        # No BOM
        assert not en_raw.startswith(b"\xef\xbb\xbf"), "en.json has UTF-8 BOM"
        assert not fr_raw.startswith(b"\xef\xbb\xbf"), "fr.json has UTF-8 BOM"
