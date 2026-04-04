"""
tests/test_ai_router.py

Unit tests for src/agents/core/ai_router.py.

All external HTTP calls and the SQLite DB path are mocked so the suite
runs with no network access and no persistent database side-effects.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Ensure project root is on sys.path so "src.agents.core" imports work
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import src.agents.core.ai_router as ai_router


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ROUTINE_CFG = {"base_url": "https://routine.example.com/v1", "api_key": "rkey", "model": "routine-model"}
_PREMIUM_CFG = {"base_url": "https://premium.example.com/v1", "api_key": "pkey", "model": "premium-model"}

_TEST_CONFIG = {
    "ai_router": {
        "routine_provider": _ROUTINE_CFG,
        "premium_provider": _PREMIUM_CFG,
        "routine_tasks": ["classify_document", "extract_vendor", "suggest_gl"],
        "complex_tasks": ["explain_anomaly", "escalation_decision", "compliance_narrative", "working_paper"],
    }
}


def _make_http_response(content: str, status: int = 200) -> MagicMock:
    """Return a mock that looks like a requests.Response."""
    resp = MagicMock()
    resp.status_code = status
    resp.text = content
    resp.json.return_value = {
        "choices": [{"message": {"content": content}}]
    }
    return resp


def _fresh_router() -> ai_router.AIRouter:
    """Build an AIRouter with test config (no file I/O)."""
    with patch.object(ai_router, "_load_config", return_value=_TEST_CONFIG):
        return ai_router.AIRouter()


# ---------------------------------------------------------------------------
# sanitize_prompt
# ---------------------------------------------------------------------------

class TestSanitizePrompt:
    def test_sin_with_dashes_redacted(self):
        text = "Client SIN is 123-456-789 on file."
        result = ai_router.sanitize_prompt(text)
        assert "123-456-789" not in result
        assert "[SIN-REDACTED]" in result

    def test_sin_with_spaces_redacted(self):
        text = "SIN 987 654 321 must not be logged."
        result = ai_router.sanitize_prompt(text)
        assert "987 654 321" not in result
        assert "[SIN-REDACTED]" in result

    def test_account_number_8_digits_redacted(self):
        text = "Account 12345678 overdraft notice."
        result = ai_router.sanitize_prompt(text)
        assert "12345678" not in result
        assert "[ACCT-REDACTED]" in result

    def test_account_number_17_digits_redacted(self):
        text = "Card number: 12345678901234567"
        result = ai_router.sanitize_prompt(text)
        assert "12345678901234567" not in result
        assert "[ACCT-REDACTED]" in result

    def test_short_numbers_not_redacted(self):
        # 7 digits should NOT be treated as an account number
        text = "Invoice #1234567 is due."
        result = ai_router.sanitize_prompt(text)
        assert "1234567" in result
        assert "[ACCT-REDACTED]" not in result

    def test_dollar_amount_bucketed_small(self):
        text = "Total: $49.99"
        result = ai_router.sanitize_prompt(text)
        assert "$49.99" not in result
        assert "<$100" in result

    def test_dollar_amount_bucketed_mid(self):
        text = "Invoice total $1,250.00 payable."
        result = ai_router.sanitize_prompt(text)
        assert "$1,250.00" not in result
        assert "$1,000–$4,999" in result

    def test_dollar_amount_bucketed_large(self):
        text = "Retainer fee $150,000.00"
        result = ai_router.sanitize_prompt(text)
        assert "$150,000.00" not in result
        assert "$100,000+" in result

    def test_clean_text_unchanged(self):
        text = "Vendor is ACME Corp. Category: Office Supplies."
        result = ai_router.sanitize_prompt(text)
        assert result == text

    def test_multiple_pii_in_one_string(self):
        text = "SIN 111-222-333, account 99887766, paid $2,500.00"
        result = ai_router.sanitize_prompt(text)
        assert "111-222-333" not in result
        assert "99887766" not in result
        assert "$2,500.00" not in result
        assert "[SIN-REDACTED]" in result
        assert "[ACCT-REDACTED]" in result
        assert "$1,000–$4,999" in result


# ---------------------------------------------------------------------------
# Amount range buckets
# ---------------------------------------------------------------------------

class TestAmountBuckets:
    @pytest.mark.parametrize("amount,expected", [
        ("$0.99",      "<$100"),
        ("$99.99",     "<$100"),
        ("$100.00",    "$100–$499"),
        ("$499.99",    "$100–$499"),
        ("$500.00",    "$500–$999"),
        ("$999.99",    "$500–$999"),
        ("$1,000.00",  "$1,000–$4,999"),
        ("$4,999.99",  "$1,000–$4,999"),
        ("$5,000.00",  "$5,000–$9,999"),
        ("$9,999.99",  "$5,000–$9,999"),
        ("$10,000.00", "$10,000–$49,999"),
        ("$49,999.99", "$10,000–$49,999"),
        ("$50,000.00", "$50,000–$99,999"),
        ("$99,999.99", "$50,000–$99,999"),
        ("$100,000.00","$100,000+"),
        ("$999,999.99","$100,000+"),
    ])
    def test_bucket(self, amount: str, expected: str):
        result = ai_router.sanitize_prompt(f"Amount: {amount}")
        assert expected in result, f"{amount!r} → expected {expected!r}, got: {result!r}"


# ---------------------------------------------------------------------------
# AIRouter – provider selection
# ---------------------------------------------------------------------------

class TestRouterProviderSelection:
    def test_routine_task_picks_routine_provider(self):
        router = _fresh_router()
        name, cfg = router._pick_provider("classify_document")
        assert name == "google/gemini-2.0-flash-001"
        assert cfg["model"] == "google/gemini-2.0-flash-001"

    def test_complex_task_picks_premium_provider(self):
        router = _fresh_router()
        name, cfg = router._pick_provider("escalation_decision")
        assert name == "anthropic/claude-haiku-4-5"
        assert cfg["model"] == "anthropic/claude-haiku-4-5"

    def test_unknown_task_picks_premium_provider(self):
        router = _fresh_router()
        name, cfg = router._pick_provider("some_unknown_task")
        # Unknown tasks fall back to deepseek/deepseek-chat
        assert name == "deepseek/deepseek-chat"
        assert cfg["model"] == "deepseek/deepseek-chat"

    def test_all_default_routine_tasks_go_to_routine(self):
        router = _fresh_router()
        expected = {
            "classify_document": "google/gemini-2.0-flash-001",
            "extract_vendor": "google/gemini-2.0-flash-001",
            "suggest_gl": "deepseek/deepseek-chat",
        }
        for task, model in expected.items():
            name, _ = router._pick_provider(task)
            assert name == model, f"{task} should map to {model}"

    def test_all_default_complex_tasks_go_to_premium(self):
        router = _fresh_router()
        for task in ("explain_anomaly", "escalation_decision", "compliance_narrative", "working_paper"):
            name, _ = router._pick_provider(task)
            assert name == router.get_model_for_task(task), f"{task} model mismatch"


# ---------------------------------------------------------------------------
# AIRouter – _try_call
# ---------------------------------------------------------------------------

class TestTryCall:
    def test_successful_call_returns_content(self):
        router = _fresh_router()
        with patch("src.agents.core.ai_router.requests.post") as mock_post:
            mock_post.return_value = _make_http_response("hello world")
            result, error, latency = router._try_call(
                model="routine-model",
                task_type="classify_document",
                prompt="Classify this.",
            )
        assert result == "hello world"
        assert error is None
        assert latency >= 0

    def test_http_error_returns_error_string(self):
        router = _fresh_router()
        with patch("src.agents.core.ai_router.requests.post") as mock_post:
            mock_post.return_value = _make_http_response("Service Unavailable", status=503)
            result, error, latency = router._try_call(
                model="routine-model",
                task_type="classify_document",
                prompt="Classify this.",
            )
        assert result is None
        assert error is not None
        assert "503" in error

    def test_missing_api_key_returns_not_configured(self):
        router = _fresh_router()
        router._api_key = ""
        result, error, latency = router._try_call(
            model="some-model",
            task_type="classify_document",
            prompt="Classify this.",
        )
        assert result is None
        assert "not_configured" in (error or "")

    def test_missing_model_returns_not_configured(self):
        router = _fresh_router()
        router._api_key = ""
        result, error, latency = router._try_call(
            model="",
            task_type="classify_document",
            prompt="Classify this.",
        )
        assert result is None
        assert "not_configured" in (error or "")

    def test_connection_error_returns_error_string(self):
        router = _fresh_router()
        with patch("src.agents.core.ai_router.requests.post", side_effect=ConnectionError("timeout")):
            result, error, latency = router._try_call(
                model="routine-model",
                task_type="classify_document",
                prompt="Classify this.",
            )
        assert result is None
        assert error is not None
        assert latency >= 0


# ---------------------------------------------------------------------------
# AIRouter.call – full integration (mocked HTTP + mocked DB)
# ---------------------------------------------------------------------------

class TestRouterCall:
    def _patched_call(self, task_type: str, prompt: str, **kwargs) -> dict[str, Any]:
        """Run router.call with HTTP, DB, cache, and memory short-circuit all mocked."""
        router = _fresh_router()
        with patch("src.agents.core.ai_router.requests.post") as mock_post, \
             patch.object(ai_router, "_write_audit_log"), \
             patch.object(ai_router, "_check_cache", return_value=None), \
             patch.object(ai_router, "_store_cache"), \
             patch.object(ai_router, "_check_memory_shortcircuit", return_value=None):
            mock_post.return_value = _make_http_response("AI response text")
            return router.call(task_type, prompt, **kwargs)

    def test_returns_expected_keys(self):
        result = self._patched_call("classify_document", "Some invoice text")
        assert set(result.keys()) == {"provider", "result", "latency_ms", "fallback_used", "error"}

    def test_successful_routine_call(self):
        result = self._patched_call("classify_document", "Some invoice text")
        assert result["result"] == "AI response text"
        assert result["error"] is None
        assert result["fallback_used"] is False
        assert result["provider"] == "google/gemini-2.0-flash-001"

    def test_successful_premium_call(self):
        result = self._patched_call("escalation_decision", "Should we post this?")
        assert result["result"] == "AI response text"
        assert result["provider"] == "anthropic/claude-haiku-4-5"

    def test_fallback_used_when_primary_fails(self):
        router = _fresh_router()

        # Simulate a DB provider that fails, then legacy _try_call succeeds
        fake_db_provider = {
            "name": "db-provider", "api_url": "https://fail.example.com/v1",
            "api_key": "key", "model": "fail-model", "api_format": "openai",
            "provider_id": 1, "enabled": 1, "notes": "", "priority": 1,
        }

        with patch.object(ai_router, "get_providers_for_task", return_value=[fake_db_provider]), \
             patch.object(ai_router, "call_provider", side_effect=ConnectionError("db provider down")), \
             patch("src.agents.core.ai_router.requests.post") as mock_post, \
             patch.object(ai_router, "_write_audit_log"), \
             patch.object(ai_router, "_check_cache", return_value=None), \
             patch.object(ai_router, "_store_cache"), \
             patch.object(ai_router, "_check_memory_shortcircuit", return_value=None):
            mock_post.return_value = _make_http_response("fallback response")
            result = router.call("classify_document", "classify this", fallback_on_error=True)

        assert result["fallback_used"] is True
        assert result["result"] == "fallback response"
        assert result["error"] is None

    def test_no_fallback_when_disabled(self):
        router = _fresh_router()
        with patch("src.agents.core.ai_router.requests.post", side_effect=ConnectionError("down")), \
             patch.object(ai_router, "_write_audit_log"), \
             patch.object(ai_router, "_check_cache", return_value=None), \
             patch.object(ai_router, "_store_cache"), \
             patch.object(ai_router, "_check_memory_shortcircuit", return_value=None):
            result = router.call("classify_document", "classify this", fallback_on_error=False)

        assert result["fallback_used"] is False
        assert result["result"] is None
        assert result["error"] is not None

    def test_prompt_sanitized_before_http(self):
        """Verify sanitized text (not raw PII) reaches the HTTP call."""
        router = _fresh_router()
        captured_payload: list[dict] = []

        def capture(*args, **kwargs):
            captured_payload.append(kwargs.get("json") or {})
            return _make_http_response("ok")

        with patch("src.agents.core.ai_router.requests.post", side_effect=capture), \
             patch.object(ai_router, "_write_audit_log"), \
             patch.object(ai_router, "_check_cache", return_value=None), \
             patch.object(ai_router, "_store_cache"), \
             patch.object(ai_router, "_check_memory_shortcircuit", return_value=None):
            router.call("classify_document", "Amount $1,500.00 for SIN 111-222-333")

        assert captured_payload, "HTTP was never called"
        messages = captured_payload[0].get("messages", [])
        user_content = next(m["content"] for m in messages if m["role"] == "user")
        assert "111-222-333" not in user_content
        assert "$1,500.00" not in user_content

    def test_context_dict_included_in_prompt(self):
        """Context dict should appear JSON-serialized in the outbound prompt."""
        router = _fresh_router()
        captured: list[dict] = []

        def capture(*args, **kwargs):
            captured.append(kwargs.get("json") or {})
            return _make_http_response("ok")

        with patch("src.agents.core.ai_router.requests.post", side_effect=capture), \
             patch.object(ai_router, "_write_audit_log"), \
             patch.object(ai_router, "_check_cache", return_value=None), \
             patch.object(ai_router, "_store_cache"), \
             patch.object(ai_router, "_check_memory_shortcircuit", return_value=None):
            router.call("classify_document", "classify", context={"vendor": "ACME", "doc_type": "invoice"})

        messages = captured[0].get("messages", [])
        user_content = next(m["content"] for m in messages if m["role"] == "user")
        assert "ACME" in user_content
        assert "invoice" in user_content

    def test_latency_ms_is_non_negative_int(self):
        result = self._patched_call("classify_document", "invoice")
        assert isinstance(result["latency_ms"], int)
        assert result["latency_ms"] >= 0

    def test_audit_log_written_once_per_call(self):
        router = _fresh_router()
        with patch("src.agents.core.ai_router.requests.post") as mock_post, \
             patch.object(ai_router, "_write_audit_log") as mock_audit, \
             patch.object(ai_router, "_check_cache", return_value=None), \
             patch.object(ai_router, "_store_cache"), \
             patch.object(ai_router, "_check_memory_shortcircuit", return_value=None):
            mock_post.return_value = _make_http_response("ok")
            router.call("escalation_decision", "decision prompt", username="alice", document_id="doc-1")

        mock_audit.assert_called_once()
        _, kwargs = mock_audit.call_args
        assert kwargs["username"] == "alice"
        assert kwargs["document_id"] == "doc-1"
        assert kwargs["task_type"] == "escalation_decision"
        assert kwargs["provider"] == "anthropic/claude-haiku-4-5"


# ---------------------------------------------------------------------------
# Audit log – DB writes
# ---------------------------------------------------------------------------

class TestAuditLog:
    def test_audit_log_table_created_and_row_inserted(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        with patch.object(ai_router, "DB_PATH", Path(db_path)):
            ai_router._write_audit_log(
                event_type="ai_call",
                username="tester",
                document_id="doc-xyz",
                provider="routine",
                task_type="classify_document",
                prompt_snippet="classify this invoice",
                latency_ms=42,
            )

        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT * FROM audit_log WHERE document_id='doc-xyz'").fetchone()
        conn.close()
        assert row is not None

    def test_prompt_snippet_truncated_to_500(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        long_prompt = "x" * 2000
        with patch.object(ai_router, "DB_PATH", Path(db_path)):
            ai_router._write_audit_log(
                event_type="ai_call",
                username=None,
                document_id=None,
                provider="premium",
                task_type="escalation_decision",
                prompt_snippet=long_prompt,
                latency_ms=100,
            )

        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT prompt_snippet FROM audit_log").fetchone()
        conn.close()
        assert row is not None
        assert len(row[0]) == 500

    def test_audit_log_never_raises_on_bad_db_path(self):
        # Should silently swallow the error
        with patch.object(ai_router, "DB_PATH", Path("/nonexistent/path/db.sqlite")):
            ai_router._write_audit_log(
                event_type="ai_call",
                username=None,
                document_id=None,
                provider="routine",
                task_type="extract_vendor",
                prompt_snippet="test",
                latency_ms=5,
            )  # must not raise

    def test_audit_log_created_at_is_iso8601(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        with patch.object(ai_router, "DB_PATH", Path(db_path)):
            ai_router._write_audit_log(
                event_type="ai_call",
                username="bob",
                document_id="doc-1",
                provider="routine",
                task_type="suggest_gl",
                prompt_snippet="suggest gl for this",
                latency_ms=15,
            )

        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT created_at FROM audit_log").fetchone()
        conn.close()
        # ISO 8601 format: 2026-03-19T12:00:00+00:00
        created_at = row[0]
        assert "T" in created_at
        assert len(created_at) >= 19


# ---------------------------------------------------------------------------
# _call_provider URL normalization
# ---------------------------------------------------------------------------

class TestCallProviderUrlNormalization:
    def _post_url(self, base_url: str) -> str:
        captured = []
        with patch("src.agents.core.ai_router.requests.post") as mock_post:
            mock_post.return_value = _make_http_response("ok")
            try:
                ai_router._call_provider(
                    base_url=base_url,
                    api_key="key",
                    model="model",
                    system_prompt="sys",
                    user_prompt="user",
                )
            except Exception:
                pass
            if mock_post.call_args:
                captured.append(mock_post.call_args[0][0])
        return captured[0] if captured else ""

    def test_bare_base_url_gets_completions_appended(self):
        url = self._post_url("https://api.example.com/v1")
        assert url == "https://api.example.com/v1/chat/completions"

    def test_trailing_slash_removed(self):
        url = self._post_url("https://api.example.com/v1/")
        assert url == "https://api.example.com/v1/chat/completions"

    def test_url_already_ending_with_completions_not_doubled(self):
        url = self._post_url("https://api.example.com/v1/chat/completions")
        assert url == "https://api.example.com/v1/chat/completions"


# ---------------------------------------------------------------------------
# Module-level call() convenience function
# ---------------------------------------------------------------------------

class TestModuleLevelCall:
    def test_module_call_returns_dict(self):
        ai_router._reset()
        with patch.object(ai_router, "_load_config", return_value=_TEST_CONFIG), \
             patch("src.agents.core.ai_router.requests.post") as mock_post, \
             patch.object(ai_router, "_write_audit_log"):
            mock_post.return_value = _make_http_response("result text")
            out = ai_router.call("classify_document", "invoice text")

        assert isinstance(out, dict)
        assert "provider" in out
        assert "result" in out

    def test_module_call_reuses_singleton(self):
        ai_router._reset()
        with patch.object(ai_router, "_load_config", return_value=_TEST_CONFIG), \
             patch("src.agents.core.ai_router.requests.post") as mock_post, \
             patch.object(ai_router, "_write_audit_log"):
            mock_post.return_value = _make_http_response("ok")
            ai_router.call("classify_document", "a")
            r1 = ai_router._router
            ai_router.call("classify_document", "b")
            r2 = ai_router._router
        assert r1 is r2  # same object

    def test_reset_clears_singleton(self):
        ai_router._reset()
        assert ai_router._router is None


# ---------------------------------------------------------------------------
# OpenClaw escalation engine – router integration
# ---------------------------------------------------------------------------

class TestOpenClawRouterIntegration:
    """
    Verify that OpenClawEscalationEngine uses ai_router when no callable is
    configured, and falls back to deterministic when the router also fails.
    """

    def _make_case(self, **overrides) -> dict[str, Any]:
        base = {
            "document_id": "doc-test-001",
            "vendor": "ACME Corp",
            "client_code": "CLIENT1",
            "amount": 499.00,
            "currency": "CAD",
            "doc_type": "invoice",
            "category": "office_supplies",
            "gl_account": "Office Expense",
            "tax_code": "GST",
            "confidence": 0.80,  # below 0.95 → triggers escalation
            "review_status": "pending",
            "document_date": "2026-03-01",
            "raw_result": {},
        }
        base.update(overrides)
        return base

    def test_router_used_when_no_callable_and_returns_hold(self):
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine

        ai_response = json.dumps({"decision": "hold", "reason": "AI says hold", "confidence": 0.88})
        mock_router = MagicMock()
        mock_router.call.return_value = {
            "provider": "premium",
            "result": ai_response,
            "latency_ms": 200,
            "fallback_used": False,
            "error": None,
        }

        engine = OpenClawEscalationEngine(openclaw_callable=None)
        with patch("src.agents.core.openclaw_escalation_engine._load_ai_router", return_value=mock_router):
            decision = engine.decide(document=self._make_case())

        assert decision.decision == "hold"
        assert decision.should_escalate is True
        assert "ai_router" in decision.provider
        mock_router.call.assert_called_once()

    def test_router_used_with_post_decision(self):
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine

        ai_response = json.dumps({"decision": "post", "reason": "Looks clean", "confidence": 0.95})
        mock_router = MagicMock()
        mock_router.call.return_value = {
            "provider": "premium",
            "result": ai_response,
            "latency_ms": 180,
            "fallback_used": False,
            "error": None,
        }

        engine = OpenClawEscalationEngine(openclaw_callable=None)
        with patch("src.agents.core.openclaw_escalation_engine._load_ai_router", return_value=mock_router):
            decision = engine.decide(document=self._make_case())

        assert decision.decision == "post"

    def test_deterministic_fallback_when_router_unavailable(self):
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine

        engine = OpenClawEscalationEngine(openclaw_callable=None)
        with patch("src.agents.core.openclaw_escalation_engine._load_ai_router", return_value=None):
            decision = engine.decide(document=self._make_case())

        assert decision.provider == "deterministic_fallback"
        assert decision.decision in {"post", "hold", "reject"}

    def test_deterministic_fallback_when_router_errors(self):
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine

        mock_router = MagicMock()
        mock_router.call.side_effect = RuntimeError("router exploded")

        engine = OpenClawEscalationEngine(openclaw_callable=None)
        with patch("src.agents.core.openclaw_escalation_engine._load_ai_router", return_value=mock_router):
            decision = engine.decide(document=self._make_case())

        assert decision.provider == "deterministic_fallback"

    def test_deterministic_fallback_when_router_returns_error(self):
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine

        mock_router = MagicMock()
        mock_router.call.return_value = {
            "provider": "routine",
            "result": None,
            "latency_ms": 0,
            "fallback_used": False,
            "error": "provider_not_configured",
        }

        engine = OpenClawEscalationEngine(openclaw_callable=None)
        with patch("src.agents.core.openclaw_escalation_engine._load_ai_router", return_value=mock_router):
            decision = engine.decide(document=self._make_case())

        assert decision.provider == "deterministic_fallback"

    def test_explicit_callable_takes_priority_over_router(self):
        """If openclaw_callable is set, it must be used and router never called."""
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine

        explicit_callable = MagicMock(return_value={
            "decision": "reject",
            "reason": "explicit callable says reject",
            "confidence": 0.99,
        })
        mock_router = MagicMock()

        engine = OpenClawEscalationEngine(openclaw_callable=explicit_callable)
        with patch("src.agents.core.openclaw_escalation_engine._load_ai_router", return_value=mock_router):
            decision = engine.decide(document=self._make_case())

        assert decision.decision == "reject"
        assert decision.provider == "openclaw"
        explicit_callable.assert_called_once()
        mock_router.call.assert_not_called()

    def test_no_escalation_skips_router(self):
        """High-confidence, low-risk documents should not reach the router."""
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine

        mock_router = MagicMock()
        engine = OpenClawEscalationEngine(openclaw_callable=None)
        clean_doc = self._make_case(confidence=0.99)  # above threshold

        with patch("src.agents.core.openclaw_escalation_engine._load_ai_router", return_value=mock_router):
            decision = engine.decide(document=clean_doc)

        assert decision.should_escalate is False
        assert decision.provider == "deterministic"
        mock_router.call.assert_not_called()

    def test_router_receives_document_id(self):
        """document_id must be forwarded to the router for audit tracing."""
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine

        ai_response = json.dumps({"decision": "hold", "reason": "test", "confidence": 0.8})
        mock_router = MagicMock()
        mock_router.call.return_value = {
            "provider": "premium",
            "result": ai_response,
            "latency_ms": 100,
            "fallback_used": False,
            "error": None,
        }

        engine = OpenClawEscalationEngine(openclaw_callable=None)
        with patch("src.agents.core.openclaw_escalation_engine._load_ai_router", return_value=mock_router):
            engine.decide(document=self._make_case(document_id="DOC-TRACE-999"))

        _, call_kwargs = mock_router.call.call_args
        assert call_kwargs.get("document_id") == "DOC-TRACE-999"
