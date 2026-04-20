"""Tests for the multi-model OCR consensus engine."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.consensus_engine import (  # noqa: E402
    COSTS,
    ConsensusOCR,
    consensus_enabled,
)


# ---------------------------------------------------------------------------
# Helpers — deterministic mock engines
# ---------------------------------------------------------------------------

def _engine_returns(data, *, delay=0.0):
    def _fn(_image_path):
        if delay:
            time.sleep(delay)
        return dict(data)
    return _fn


def _engine_raises(exc):
    def _fn(_image_path):
        raise exc
    return _fn


def _engine_error(msg="runtime_error"):
    def _fn(_image_path):
        return {"error": msg}
    return _fn


SAMPLE_A = {
    "vendor": "Metro",
    "date": "2026-04-20",
    "subtotal": 40.00,
    "total": 45.99,
    "gst": 2.00,
    "qst": 3.99,
}


# ---------------------------------------------------------------------------
# Consensus behavior
# ---------------------------------------------------------------------------

class TestConsensus:
    def test_three_engines_all_agree(self):
        engine_a = _engine_returns(SAMPLE_A)
        engine_b = _engine_returns(SAMPLE_A)
        engine_c = _engine_returns(SAMPLE_A)
        c = ConsensusOCR(engines={"a": engine_a, "b": engine_b, "c": engine_c})
        out = c.process("dummy.jpg", budget_cap=1.0)
        assert out["extracted"]["vendor"] == "Metro"
        assert out["extracted"]["total"] == 45.99
        assert all(v == 1.0 for v in out["confidence_per_field"].values())
        assert out["needs_review"] is False
        assert out["disagreements"] == []

    def test_two_of_three_agree(self):
        alt = dict(SAMPLE_A, total=99.99)
        c = ConsensusOCR(engines={
            "a": _engine_returns(SAMPLE_A),
            "b": _engine_returns(SAMPLE_A),
            "c": _engine_returns(alt),
        })
        out = c.process("dummy.jpg", budget_cap=1.0)
        assert out["extracted"]["total"] == 45.99
        assert pytest.approx(out["confidence_per_field"]["total"], rel=1e-3) == 2/3
        # 99.99 should appear as an alternate for 'total'.
        totals_disagreements = [d for d in out["disagreements"] if d["field"] == "total"]
        assert totals_disagreements
        assert 99.99 in totals_disagreements[0]["alternates"]

    def test_all_three_disagree_flags_review(self):
        c = ConsensusOCR(engines={
            "a": _engine_returns({**SAMPLE_A, "total": 45.99}),
            "b": _engine_returns({**SAMPLE_A, "total": 50.00}),
            "c": _engine_returns({**SAMPLE_A, "total": 55.00}),
        })
        out = c.process("dummy.jpg", budget_cap=1.0)
        # Tie — first pick wins, but confidence is 1/3, so needs_review fires.
        assert out["confidence_per_field"]["total"] == pytest.approx(1/3)
        assert out["needs_review"] is True

    def test_one_engine_fails_others_succeed(self):
        c = ConsensusOCR(engines={
            "a": _engine_returns(SAMPLE_A),
            "b": _engine_returns(SAMPLE_A),
            "c": _engine_error("api_key_missing"),
        })
        out = c.process("dummy.jpg", budget_cap=1.0)
        assert "c" in out["engines_run"]
        assert "error" in out["raw_results"]["c"]
        # Two valid agreements → confidence 1.0 (both agree).
        assert out["confidence_per_field"]["total"] == 1.0

    def test_all_engines_fail_graceful(self):
        c = ConsensusOCR(engines={
            "a": _engine_error(),
            "b": _engine_error(),
            "c": _engine_error(),
        })
        out = c.process("dummy.jpg", budget_cap=1.0)
        assert out["extracted"] == {}
        assert out["needs_review"] is True
        assert out["engines_agreed"] == 0

    def test_all_engines_raise_graceful(self):
        c = ConsensusOCR(engines={
            "a": _engine_raises(RuntimeError("kaboom")),
            "b": _engine_raises(ValueError("nope")),
        })
        out = c.process("dummy.jpg", budget_cap=1.0)
        assert out["extracted"] == {}
        assert out["needs_review"] is True
        for v in out["raw_results"].values():
            assert "error" in v

    def test_budget_cap_respected(self):
        """With budget 0.02, only docai (0.015) fits; second engine excluded."""
        engines = {
            "docai": _engine_returns(SAMPLE_A),
            "claude": _engine_returns(SAMPLE_A),
            "deepseek": _engine_returns(SAMPLE_A),
        }
        costs = {"docai": 0.015, "claude": 0.015, "deepseek": 0.010}
        c = ConsensusOCR(engines=engines, costs=costs)
        out = c.process("dummy.jpg", budget_cap=0.02)
        assert out["engines_run"] == ["docai"]

    def test_budget_exhausted_before_any_engine(self):
        c = ConsensusOCR(
            engines={"docai": _engine_returns(SAMPLE_A)},
            costs={"docai": 0.015},
        )
        out = c.process("dummy.jpg", budget_cap=0.001)
        assert out.get("error") == "budget_exhausted_before_any_engine"
        assert out["engines_run"] == []

    def test_parallel_execution_faster_than_serial(self):
        """3 slow engines running in parallel should finish closer to one
        engine's duration than to 3× that duration."""
        delay = 0.15
        engines = {
            "a": _engine_returns(SAMPLE_A, delay=delay),
            "b": _engine_returns(SAMPLE_A, delay=delay),
            "c": _engine_returns(SAMPLE_A, delay=delay),
        }
        c = ConsensusOCR(engines=engines, max_workers=3)
        started = time.monotonic()
        c.process("dummy.jpg", budget_cap=1.0)
        elapsed = time.monotonic() - started
        # Serial would be ~0.45s; allow generous margin for CI jitter.
        assert elapsed < 0.4, f"parallel took {elapsed:.2f}s"

    def test_consensus_field_missing_from_one_engine(self):
        """Field present in only some engines still drives consensus among
        those engines. Confidence normalised by total engine count."""
        partial = {k: v for k, v in SAMPLE_A.items() if k != "qst"}
        c = ConsensusOCR(engines={
            "a": _engine_returns(SAMPLE_A),
            "b": _engine_returns(SAMPLE_A),
            "c": _engine_returns(partial),
        })
        out = c.process("dummy.jpg", budget_cap=1.0)
        # qst present in 2 of 3 → confidence 2/3.
        assert out["confidence_per_field"]["qst"] == pytest.approx(2/3)

    def test_case_insensitive_vendor_match(self):
        c = ConsensusOCR(engines={
            "a": _engine_returns({**SAMPLE_A, "vendor": "METRO"}),
            "b": _engine_returns({**SAMPLE_A, "vendor": "metro"}),
            "c": _engine_returns({**SAMPLE_A, "vendor": "Metro"}),
        })
        out = c.process("dummy.jpg", budget_cap=1.0)
        # Three engines with same case-folded vendor → confidence 1.0.
        assert out["confidence_per_field"]["vendor"] == 1.0

    def test_single_engine_returns_low_confidence(self):
        c = ConsensusOCR(engines={"only": _engine_returns(SAMPLE_A)})
        out = c.process("dummy.jpg", budget_cap=1.0)
        # single engine → 0.5 confidence per spec.
        for f, conf in out["confidence_per_field"].items():
            assert conf == 0.5
        assert out["needs_review"] is True

    def test_engine_timeout_recorded_as_error(self):
        """Engine that sleeps longer than timeout must not blow up the
        consensus — it's recorded as a timeout error."""
        engines = {
            "slow": _engine_returns(SAMPLE_A, delay=0.3),
            "fast_a": _engine_returns(SAMPLE_A),
            "fast_b": _engine_returns(SAMPLE_A),
        }
        c = ConsensusOCR(engines=engines, timeout=0.05)
        out = c.process("dummy.jpg", budget_cap=1.0)
        assert "error" in out["raw_results"]["slow"]
        # The two fast engines agreeing should still yield confidence 1.0.
        assert out["confidence_per_field"]["total"] == 1.0

    def test_numeric_rounding_consistent(self):
        """Different engines with float noise should still agree."""
        c = ConsensusOCR(engines={
            "a": _engine_returns({**SAMPLE_A, "total": 45.99}),
            "b": _engine_returns({**SAMPLE_A, "total": 45.990001}),
            "c": _engine_returns({**SAMPLE_A, "total": 45.99}),
        })
        out = c.process("dummy.jpg", budget_cap=1.0)
        assert out["confidence_per_field"]["total"] == 1.0


# ---------------------------------------------------------------------------
# Feature flag
# ---------------------------------------------------------------------------

class TestFeatureFlag:
    def test_disabled_by_default(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("USE_CONSENSUS", None)
            assert consensus_enabled() is False

    def test_env_override_enables(self):
        with patch.dict(os.environ, {"USE_CONSENSUS": "1"}):
            assert consensus_enabled() is True

    def test_env_override_values(self):
        for val in ("true", "yes", "ON", "1"):
            with patch.dict(os.environ, {"USE_CONSENSUS": val}):
                assert consensus_enabled() is True
        for val in ("0", "false", "no", ""):
            with patch.dict(os.environ, {"USE_CONSENSUS": val}, clear=False):
                os.environ["USE_CONSENSUS"] = val
                assert consensus_enabled() is False

    def test_default_costs_declared(self):
        for eng in ("docai", "claude_vision", "deepseek_vision"):
            assert eng in COSTS

    def test_default_engines_without_keys_return_errors(self):
        """Without ANTHROPIC_API_KEY / DEEPSEEK_API_KEY, those adapters
        must error out gracefully so the pipeline can fall back to docai."""
        from src.engines.consensus_engine import (
            _engine_claude_vision,
            _engine_deepseek_vision,
        )
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            os.environ.pop("DEEPSEEK_API_KEY", None)
            assert _engine_claude_vision("x")["error"] == "api_key_missing"
            assert _engine_deepseek_vision("x")["error"] == "api_key_missing"
