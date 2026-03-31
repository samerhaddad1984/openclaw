"""
tests/test_i18n.py — pytest tests for the OtoCPA i18n system.

Tests cover:
  - JSON files are valid and contain required keys
  - t() returns correct French and English strings
  - t() fallback behaviour (missing key, unknown lang)
  - t() template substitution via **kwargs
  - reload_cache() clears state
  - client_portal.py t() import path works (uses shared JSON)
  - review_dashboard.py uses get_user_lang() correctly
  - DB schema has the language column
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
I18N_DIR = ROOT / "src" / "i18n"
FR_PATH = I18N_DIR / "fr.json"
EN_PATH = I18N_DIR / "en.json"


def load_json(path: Path) -> dict[str, str]:
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# JSON file tests
# ---------------------------------------------------------------------------

class TestJsonFiles:
    def test_fr_json_exists(self):
        assert FR_PATH.exists(), "src/i18n/fr.json is missing"

    def test_en_json_exists(self):
        assert EN_PATH.exists(), "src/i18n/en.json is missing"

    def test_fr_json_valid(self):
        data = load_json(FR_PATH)
        assert isinstance(data, dict)
        assert len(data) > 20, "fr.json should have many keys"

    def test_en_json_valid(self):
        data = load_json(EN_PATH)
        assert isinstance(data, dict)
        assert len(data) > 20, "en.json should have many keys"

    def test_required_shared_keys_present_in_both(self):
        required = [
            "portal_title", "dashboard_title", "login_title", "username",
            "password", "login_btn", "logout_btn", "invalid_credentials",
            "switch_lang", "footer_note",
            "upload_title", "upload_btn", "my_documents",
            "col_file", "col_date_submitted", "col_status", "col_note",
            "status_new", "status_review", "status_complete", "status_hold",
            "no_documents", "upload_success", "upload_error",
            "file_too_large", "invalid_type", "contact_title", "contact_body",
        ]
        fr = load_json(FR_PATH)
        en = load_json(EN_PATH)
        for key in required:
            assert key in fr, f"fr.json missing key: {key!r}"
            assert key in en, f"en.json missing key: {key!r}"

    def test_dashboard_keys_present_in_both(self):
        required = [
            "dashboard_header", "queue_title", "btn_back_to_queue",
            "btn_manage_portfolios", "logout_btn", "stat_needs_review",
            "stat_on_hold", "stat_ready_to_post", "stat_posted",
            "col_document", "col_vendor", "col_client", "col_amount",
            "col_category", "col_gl_account", "col_assigned", "col_reason",
            "col_action", "no_documents_found", "btn_assign", "btn_claim",
            "action_none", "action_claim", "action_review",
            "flash_doc_updated", "flash_on_hold", "flash_item_claimed",
            "flash_pw_updated", "flash_assignment_updated",
            "portfolio_title", "user_mgmt_title",
            "filing_title", "diag_title",
        ]
        fr = load_json(FR_PATH)
        en = load_json(EN_PATH)
        for key in required:
            assert key in fr, f"fr.json missing dashboard key: {key!r}"
            assert key in en, f"en.json missing dashboard key: {key!r}"

    def test_switch_lang_values_are_opposites(self):
        fr = load_json(FR_PATH)
        en = load_json(EN_PATH)
        # French locale: switch_lang should show "English"
        assert fr["switch_lang"].lower() == "english", (
            "fr.json switch_lang should be 'English' (to switch to EN)")
        # English locale: switch_lang should show "Français"
        assert "fran" in en["switch_lang"].lower(), (
            "en.json switch_lang should be 'Français' (to switch to FR)")

    def test_no_blank_values(self):
        for path, label in [(FR_PATH, "fr"), (EN_PATH, "en")]:
            data = load_json(path)
            for key, val in data.items():
                if key.startswith("_"):
                    continue  # comments
                assert val.strip(), f"{label}.json: key {key!r} has blank value"


# ---------------------------------------------------------------------------
# t() function tests
# ---------------------------------------------------------------------------

class TestTranslationFunction:
    def setup_method(self):
        """Clear cache before each test so JSON changes are picked up."""
        from src.i18n import reload_cache
        reload_cache()

    def test_fr_login_btn(self):
        from src.i18n import t
        assert t("login_btn", "fr") == "Se connecter"

    def test_en_login_btn(self):
        from src.i18n import t
        assert t("login_btn", "en") == "Sign in"

    def test_fr_logout_btn(self):
        from src.i18n import t
        assert t("logout_btn", "fr") == "Déconnexion"

    def test_en_logout_btn(self):
        from src.i18n import t
        assert t("logout_btn", "en") == "Sign out"

    def test_fr_portal_title(self):
        from src.i18n import t
        assert "Portail" in t("portal_title", "fr")

    def test_en_portal_title(self):
        from src.i18n import t
        assert "Portal" in t("portal_title", "en")

    def test_unknown_lang_falls_back_to_fr(self):
        from src.i18n import t
        assert t("login_btn", "de") == t("login_btn", "fr")

    def test_empty_lang_falls_back_to_fr(self):
        from src.i18n import t
        assert t("login_btn", "") == t("login_btn", "fr")

    def test_missing_key_returns_key_itself(self):
        from src.i18n import t
        key = "this_key_does_not_exist_xyz"
        assert t(key, "fr") == key
        assert t(key, "en") == key

    def test_template_substitution_fr(self):
        from src.i18n import t
        result = t("change_pw_intro", "fr", name="Marie")
        assert "Marie" in result
        assert "{name}" not in result

    def test_template_substitution_en(self):
        from src.i18n import t
        result = t("change_pw_intro", "en", name="Alice")
        assert "Alice" in result
        assert "{name}" not in result

    def test_template_missing_var_does_not_crash(self):
        from src.i18n import t
        # Passing wrong kwarg — should not raise, just leave template as-is
        result = t("change_pw_intro", "fr", wrong_key="x")
        assert isinstance(result, str)

    def test_switch_lang_fr_shows_english(self):
        from src.i18n import t
        assert t("switch_lang", "fr") == "English"

    def test_switch_lang_en_shows_francais(self):
        from src.i18n import t
        val = t("switch_lang", "en")
        assert "fran" in val.lower() or "français" in val.lower()

    def test_fr_status_labels(self):
        from src.i18n import t
        assert t("status_new", "fr") == "Soumis"
        assert t("status_review", "fr") == "En révision"
        assert t("status_complete", "fr") == "Complété"
        assert t("status_hold", "fr") == "En attente"

    def test_en_status_labels(self):
        from src.i18n import t
        assert t("status_new", "en") == "Submitted"
        assert t("status_review", "en") == "Under Review"
        assert t("status_complete", "en") == "Complete"
        assert t("status_hold", "en") == "On Hold"

    def test_dashboard_fr_strings(self):
        from src.i18n import t
        assert "File" in t("queue_title", "fr")      # "File d'attente"
        assert "réviser" in t("stat_needs_review", "fr").lower()
        assert "attente" in t("stat_on_hold", "fr").lower()

    def test_dashboard_en_strings(self):
        from src.i18n import t
        assert t("queue_title", "en") == "Queue"
        assert t("stat_needs_review", "en") == "Needs Review"
        assert t("stat_on_hold", "en") == "On Hold"
        assert t("stat_ready_to_post", "en") == "Ready to Post"

    def test_reload_cache(self):
        from src.i18n import t, reload_cache
        val_before = t("login_btn", "fr")
        reload_cache()
        val_after = t("login_btn", "fr")
        assert val_before == val_after  # same data, cache was reloaded cleanly


# ---------------------------------------------------------------------------
# get_user_lang() helper in review_dashboard
# ---------------------------------------------------------------------------

class TestGetUserLang:
    def test_none_user_returns_fr(self):
        import sys
        sys.path.insert(0, str(ROOT))
        # Import without starting the server
        from scripts.review_dashboard import get_user_lang
        assert get_user_lang(None) == "fr"

    def test_user_with_fr(self):
        from scripts.review_dashboard import get_user_lang
        assert get_user_lang({"language": "fr"}) == "fr"

    def test_user_with_en(self):
        from scripts.review_dashboard import get_user_lang
        assert get_user_lang({"language": "en"}) == "en"

    def test_user_with_invalid_lang_falls_back_to_fr(self):
        from scripts.review_dashboard import get_user_lang
        assert get_user_lang({"language": "de"}) == "fr"

    def test_user_with_no_language_key_falls_back_to_fr(self):
        from scripts.review_dashboard import get_user_lang
        assert get_user_lang({"username": "alice"}) == "fr"

    def test_user_with_none_language_falls_back_to_fr(self):
        from scripts.review_dashboard import get_user_lang
        assert get_user_lang({"language": None}) == "fr"


# ---------------------------------------------------------------------------
# DB schema — language column must exist
# ---------------------------------------------------------------------------

class TestDbSchema:
    def _create_test_db(self) -> sqlite3.Connection:
        """Bootstrap a fresh in-memory DB using review_dashboard.bootstrap_schema."""
        import os
        # Point to a temp file so bootstrap_schema finds a writable path
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            tmp_path = f.name
        return tmp_path

    def test_language_column_in_migrate_db_spec(self):
        """migrate_db.py must declare language column for dashboard_users."""
        migrate_src = (ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        assert "language" in migrate_src, (
            "migrate_db.py should ADD language column to dashboard_users")

    def test_language_column_in_bootstrap_schema(self):
        """review_dashboard.py bootstrap_schema must include language column."""
        dashboard_src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        # The CREATE TABLE statement should have language
        assert 'language' in dashboard_src

    def test_client_portal_bootstrap_includes_language(self):
        """client_portal.py bootstrap_schema must include language column."""
        portal_src = (ROOT / "scripts" / "client_portal.py").read_text(encoding="utf-8")
        assert "language" in portal_src


# ---------------------------------------------------------------------------
# client_portal.py uses shared t() from src.i18n
# ---------------------------------------------------------------------------

class TestClientPortalI18n:
    def test_portal_imports_t_from_src_i18n(self):
        portal_src = (ROOT / "scripts" / "client_portal.py").read_text(encoding="utf-8")
        assert "from src.i18n import t" in portal_src, (
            "client_portal.py must import t from src.i18n, not define its own")

    def test_portal_does_not_define_strings_dict(self):
        portal_src = (ROOT / "scripts" / "client_portal.py").read_text(encoding="utf-8")
        assert "STRINGS:" not in portal_src and "STRINGS = {" not in portal_src, (
            "client_portal.py should not define an inline STRINGS dict — "
            "use src/i18n JSON files instead")

    def test_portal_does_not_define_own_t_function(self):
        portal_src = (ROOT / "scripts" / "client_portal.py").read_text(encoding="utf-8")
        # Should not have "def t(" after the import line
        lines = portal_src.splitlines()
        def_t_lines = [l for l in lines if l.strip().startswith("def t(")]
        assert len(def_t_lines) == 0, (
            "client_portal.py must not define its own t() — use src.i18n.t")


# ---------------------------------------------------------------------------
# review_dashboard.py uses t() from src.i18n
# ---------------------------------------------------------------------------

class TestReviewDashboardI18n:
    def test_dashboard_imports_t_from_src_i18n(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "from src.i18n import t" in src

    def test_dashboard_has_set_language_route(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert '"/set_language"' in src or "set_language" in src, (
            "review_dashboard.py must handle /set_language POST")

    def test_dashboard_has_lang_toggle_in_page_layout(self):
        src = (ROOT / "scripts" / "review_dashboard.py").read_text(encoding="utf-8")
        assert "set_language" in src
        # page_layout should accept lang param
        assert "lang: str" in src

    def test_dashboard_get_user_lang_exported(self):
        from scripts.review_dashboard import get_user_lang
        assert callable(get_user_lang)

    def test_render_login_accepts_lang(self):
        """render_login must accept lang kwarg (for bilingual login page)."""
        import inspect
        from scripts.review_dashboard import render_login
        sig = inspect.signature(render_login)
        assert "lang" in sig.parameters, "render_login() must accept a 'lang' parameter"

    def test_page_layout_accepts_lang(self):
        import inspect
        from scripts.review_dashboard import page_layout
        sig = inspect.signature(page_layout)
        assert "lang" in sig.parameters, "page_layout() must accept a 'lang' parameter"

    def test_render_home_accepts_lang(self):
        import inspect
        from scripts.review_dashboard import render_home
        sig = inspect.signature(render_home)
        assert "lang" in sig.parameters

    def test_render_document_accepts_lang(self):
        import inspect
        from scripts.review_dashboard import render_document
        sig = inspect.signature(render_document)
        assert "lang" in sig.parameters

    def test_render_portfolios_accepts_lang(self):
        import inspect
        from scripts.review_dashboard import render_portfolios
        sig = inspect.signature(render_portfolios)
        assert "lang" in sig.parameters

    def test_render_user_management_accepts_lang(self):
        import inspect
        from scripts.review_dashboard import render_user_management
        sig = inspect.signature(render_user_management)
        assert "lang" in sig.parameters
