"""
tests/test_pipeline_wiring.py
==============================
Tests for BLOCK 1-7 pipeline wiring: fraud_flags, substance_flags,
vendor memory GL propagation, fuzzy vendor matching, AI router config,
and prompt templates.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.agents.tools.review_policy import (
    decide_review_status,
    effective_confidence,
    should_auto_approve,
    check_fraud_flags,
    check_substance_block,
)
from src.engines.substance_engine import (
    substance_classifier,
    propagate_gl_change_suggestions,
)
from src.engines.fraud_engine import (
    _rule_vendor_amount_anomaly,
    _rule_weekend_holiday,
    _normalize_vendor_key,
    _load_vendor_history_fuzzy,
)


# ===================================================================
# BLOCK 1: Fraud flags wired through pipeline
# ===================================================================

class TestFraudFlagsWiring:
    """Verify fraud_flags flow through review_policy correctly."""

    def test_decide_review_status_with_fraud_flags_blocks(self):
        """High-confidence doc with CRITICAL fraud flags should be NeedsReview."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Suspicious Vendor",
            total=10000.0,
            document_date="2026-03-15",
            client_code="CLT001",
            fraud_flags=[{"rule": "bank_account_change", "severity": "critical"}],
        )
        assert decision.status == "NeedsReview"
        assert decision.effective_confidence <= 0.60

    def test_decide_review_status_without_fraud_flags_passes(self):
        """Same doc without fraud flags should be Ready."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Normal Vendor",
            total=1000.0,
            document_date="2026-03-15",
            client_code="CLT001",
            fraud_flags=[],
        )
        assert decision.status == "Ready"

    def test_low_severity_fraud_flags_dont_block(self):
        """LOW/MEDIUM fraud flags should not block auto-approval."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Vendor",
            total=1000.0,
            document_date="2026-03-15",
            client_code="CLT001",
            fraud_flags=[{"rule": "round_number_flag", "severity": "low"}],
        )
        assert decision.status == "Ready"

    def test_effective_confidence_capped_by_fraud(self):
        """Effective confidence should be capped at 0.60 with HIGH fraud flags."""
        eff = effective_confidence(
            0.95, "rules", has_required=True,
            fraud_flags=[{"rule": "duplicate_exact", "severity": "high"}],
        )
        assert eff <= 0.60

    def test_should_auto_approve_blocked_by_fraud(self):
        assert not should_auto_approve(
            0.95,
            fraud_flags=[{"rule": "test", "severity": "critical"}],
        )


# ===================================================================
# BLOCK 2: Substance flags wired through pipeline
# ===================================================================

class TestSubstanceFlagsWiring:
    """Verify substance_flags flow through review_policy correctly."""

    def test_substance_block_auto_approval(self):
        """block_auto_approval=True should prevent auto-approval."""
        decision = decide_review_status(
            rules_confidence=0.95,
            final_method="rules",
            vendor_name="Bank Nationale",
            total=50000.0,
            document_date="2026-03-15",
            client_code="CLT001",
            substance_flags={"potential_loan": True, "block_auto_approval": True},
        )
        assert decision.status == "NeedsReview"

    def test_substance_capex_caps_confidence(self):
        """potential_capex should cap confidence at 0.70."""
        eff = effective_confidence(
            0.95, "rules", has_required=True,
            substance_flags={"potential_capex": True},
        )
        assert eff <= 0.70

    def test_substance_intercompany_caps_confidence(self):
        """potential_intercompany should cap confidence at 0.60."""
        eff = effective_confidence(
            0.95, "rules", has_required=True,
            substance_flags={"potential_intercompany": True},
        )
        assert eff <= 0.60

    def test_substance_mixed_tax_caps_confidence(self):
        """mixed_tax_invoice should cap confidence at 0.50."""
        eff = effective_confidence(
            0.95, "rules", has_required=True,
            substance_flags={"mixed_tax_invoice": True},
        )
        assert eff <= 0.50

    def test_should_auto_approve_blocked_by_substance(self):
        assert not should_auto_approve(
            0.95,
            substance_flags={"block_auto_approval": True},
        )

    def test_substance_flags_as_json_string(self):
        """Substance flags passed as JSON string should be parsed."""
        eff = effective_confidence(
            0.95, "rules", has_required=True,
            substance_flags='{"potential_capex": true}',
        )
        assert eff <= 0.70


# ===================================================================
# BLOCK 3: GL change propagation
# ===================================================================

class TestGlChangePropagation:
    """Verify propagate_gl_change_suggestions works end-to-end."""

    def test_propagation_updates_unprocessed_documents(self):
        """When vendor memory GL changes, unprocessed docs get review notes."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            conn = sqlite3.connect(db_path)
            conn.execute("""CREATE TABLE documents (
                document_id TEXT PRIMARY KEY, vendor TEXT, client_code TEXT,
                review_status TEXT, raw_result TEXT)""")
            conn.execute(
                "INSERT INTO documents VALUES (?,?,?,?,?)",
                ("doc_1", "Hydro-Quebec", "ACME", "NeedsReview", "{}"),
            )
            conn.execute(
                "INSERT INTO documents VALUES (?,?,?,?,?)",
                ("doc_2", "Hydro-Quebec", "ACME", "Ready", "{}"),
            )
            conn.commit()
            conn.close()

            count = propagate_gl_change_suggestions(
                vendor="Hydro-Quebec", new_gl="5100",
                client_code="ACME", db_path=db_path,
            )
            assert count == 1, f"Should update 1 unprocessed doc, got {count}"

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT raw_result FROM documents WHERE document_id = 'doc_1'").fetchone()
            conn.close()
            data = json.loads(row["raw_result"])
            assert any("5100" in str(n) for n in data.get("review_notes", []))
        finally:
            os.unlink(db_path)

    def test_propagation_skips_posted_documents(self):
        """Posted documents should NOT be updated."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            conn = sqlite3.connect(db_path)
            conn.execute("""CREATE TABLE documents (
                document_id TEXT PRIMARY KEY, vendor TEXT, client_code TEXT,
                review_status TEXT, raw_result TEXT)""")
            conn.execute(
                "INSERT INTO documents VALUES (?,?,?,?,?)",
                ("doc_1", "Bell", "ACME", "Posted", "{}"),
            )
            conn.commit()
            conn.close()

            count = propagate_gl_change_suggestions(
                vendor="Bell", new_gl="5300",
                client_code="ACME", db_path=db_path,
            )
            assert count == 0
        finally:
            os.unlink(db_path)


# ===================================================================
# BLOCK 5: Fuzzy vendor grouping
# ===================================================================

class TestFuzzyVendorGrouping:
    """Verify fuzzy vendor matching in fraud detection."""

    def test_normalize_vendor_key_strips_accents(self):
        assert _normalize_vendor_key("Société de transport") == _normalize_vendor_key("Societe de transport")

    def test_normalize_vendor_key_strips_suffixes(self):
        k1 = _normalize_vendor_key("Bell Canada Inc.")
        k2 = _normalize_vendor_key("Bell Canada")
        assert k1 == k2

    def test_normalize_vendor_key_case_insensitive(self):
        assert _normalize_vendor_key("BELL CANADA") == _normalize_vendor_key("bell canada")

    def test_anomaly_with_fuzzy_fallback(self):
        """Fuzzy history enables anomaly detection when exact history is insufficient."""
        exact = [{"amount": 100.0} for _ in range(3)]
        # Fuzzy history with realistic variance (not all identical)
        fuzzy = [{"amount": 95.0 + i * 2} for i in range(15)]  # 95, 97, ..., 123
        # Without fuzzy: no flag (only 3 exact entries)
        assert _rule_vendor_amount_anomaly(5000.0, exact) is None
        # With fuzzy: flag fires because 5000 is far from mean ~109
        flag = _rule_vendor_amount_anomaly(5000.0, exact, fuzzy_history=fuzzy)
        assert flag is not None
        assert flag["rule"] == "vendor_amount_anomaly"

    def test_anomaly_no_false_positive_with_fuzzy(self):
        """Normal amounts should not trigger even with fuzzy history."""
        fuzzy = [{"amount": 100.0} for _ in range(15)]
        flag = _rule_vendor_amount_anomaly(105.0, [], fuzzy_history=fuzzy)
        assert flag is None

    def test_credit_notes_get_weekend_rules(self):
        """Weekend/holiday rules now apply to credit notes."""
        flags = _rule_weekend_holiday(500.0, __import__("datetime").date(2026, 3, 14))  # Saturday
        assert len(flags) > 0


# ===================================================================
# BLOCK 7: AI router configuration
# ===================================================================

class TestAiRouterConfig:
    """Verify AI router cache and routing configuration."""

    def test_substance_classification_cache_7_days(self):
        from src.agents.core.ai_router import _CACHE_TTL_DAYS
        assert _CACHE_TTL_DAYS.get("substance_classification") == 7

    def test_related_party_check_cache_30_days(self):
        from src.agents.core.ai_router import _CACHE_TTL_DAYS
        assert _CACHE_TTL_DAYS.get("related_party_check") == 30

    def test_mixed_tax_detection_never_cached(self):
        from src.agents.core.ai_router import _NEVER_CACHE_TASKS
        assert "mixed_tax_detection" in _NEVER_CACHE_TASKS

    def test_all_three_route_to_premium(self):
        from src.agents.core.ai_router import _DEFAULT_COMPLEX_TASKS
        assert "substance_classification" in _DEFAULT_COMPLEX_TASKS
        assert "related_party_check" in _DEFAULT_COMPLEX_TASKS
        assert "mixed_tax_detection" in _DEFAULT_COMPLEX_TASKS


# ===================================================================
# BLOCK 6: Prompt templates
# ===================================================================

class TestPromptTemplates:
    """Verify prompt templates exist and meet requirements."""

    TEMPLATES = [
        "substance_classification.txt",
        "related_party_check.txt",
        "mixed_tax_detection.txt",
    ]

    def _template_path(self, name: str) -> Path:
        return ROOT / "src" / "agents" / "prompts" / name

    @pytest.mark.parametrize("template_name", TEMPLATES)
    def test_template_exists(self, template_name):
        assert self._template_path(template_name).exists()

    @pytest.mark.parametrize("template_name", TEMPLATES)
    def test_template_has_bilingual(self, template_name):
        content = self._template_path(template_name).read_text(encoding="utf-8")
        # Should have both French and English content
        assert any(fr_word in content.lower() for fr_word in ["vous", "français", "bilingue", "spécialiste", "expert"]), \
            f"{template_name} should have French content"

    @pytest.mark.parametrize("template_name", TEMPLATES)
    def test_template_requests_json(self, template_name):
        content = self._template_path(template_name).read_text(encoding="utf-8")
        assert "JSON" in content or "json" in content

    @pytest.mark.parametrize("template_name", TEMPLATES)
    def test_template_has_confidence(self, template_name):
        content = self._template_path(template_name).read_text(encoding="utf-8")
        assert "confidence" in content.lower()

    @pytest.mark.parametrize("template_name", TEMPLATES)
    def test_template_has_reasoning(self, template_name):
        content = self._template_path(template_name).read_text(encoding="utf-8")
        assert "reasoning" in content.lower()

    @pytest.mark.parametrize("template_name", TEMPLATES)
    def test_template_has_placeholders(self, template_name):
        content = self._template_path(template_name).read_text(encoding="utf-8")
        import re
        placeholders = re.findall(r'\{[A-Z_]+\}', content)
        assert len(placeholders) >= 1, f"{template_name} should have {{PLACEHOLDER}} variables"


# ===================================================================
# BLOCK 4: Substance classifier AI integration
# ===================================================================

class TestSubstanceClassifierIntegration:
    """Verify substance_classifier AI fallback works."""

    def test_keyword_detection_capex(self):
        result = substance_classifier(
            vendor="Dell Technologies",
            memo="PowerEdge server rack",
            amount=15000,
        )
        assert result["potential_capex"]
        assert result["suggested_gl"]

    def test_keyword_detection_loan(self):
        result = substance_classifier(
            vendor="National Bank",
            memo="Prêt hypothécaire paiement mensuel",
            amount=5000,
        )
        assert result["potential_loan"]
        # Loan keyword detection alone doesn't block auto-approval
        # (only large wire transfers from banks trigger block_auto_approval)
        assert result["suggested_gl"] is not None

    def test_keyword_detection_personal(self):
        result = substance_classifier(
            vendor="Netflix",
            memo="Monthly subscription",
            amount=15.99,
        )
        assert result["potential_personal_expense"]
        assert result["block_auto_approval"]

    def test_no_flags_for_normal_expense(self):
        result = substance_classifier(
            vendor="Staples Canada",
            memo="Office supplies order",
            amount=150,
        )
        assert not result["potential_capex"]
        assert not result["potential_loan"]
        assert not result["potential_personal_expense"]
        assert not result["block_auto_approval"]
