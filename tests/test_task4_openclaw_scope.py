"""
tests/test_task4_openclaw_scope.py

Tests for Task 4a:
  - open_db_readonly() present in all 5 OpenClaw files
  - maybe_post_ready_job() raises PermissionError
  - build_prompt_payload() loads locked template
  - _load_prompt_template() fills placeholders correctly
  - Prompt template files exist and contain required placeholders
  - _serve_db_backup() / render_troubleshoot() in dashboard (Task 4b)
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
from dataclasses import field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_test_db(path: Path) -> None:
    """Create a minimal SQLite DB with the tables OpenClaw expects."""
    conn = sqlite3.connect(str(path))
    conn.execute(
        """CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, file_path TEXT, client_code TEXT, vendor TEXT,
            doc_type TEXT, category TEXT, gl_account TEXT, tax_code TEXT,
            amount REAL, document_date TEXT, review_status TEXT, confidence REAL,
            currency TEXT, raw_result TEXT, created_at TEXT, updated_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY,
            document_id TEXT, posting_status TEXT, approval_state TEXT,
            reviewer TEXT, target_system TEXT, entry_kind TEXT,
            external_id TEXT, payload_json TEXT, error_text TEXT,
            assigned_to TEXT, updated_at TEXT, created_at TEXT
        )"""
    )
    conn.execute(
        """INSERT OR IGNORE INTO documents
           (document_id, vendor, client_code, doc_type, amount, currency,
            confidence, review_status, gl_account, tax_code, category,
            document_date, created_at, updated_at)
           VALUES
           ('DOC-001','TestVendor','CLIENT1','invoice',100.0,'CAD',
            0.95,'Ready','5000','TX1','office','2025-01-01','2025-01-01','2025-01-01')"""
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# 1. open_db_readonly() present in all 5 OpenClaw files
# ---------------------------------------------------------------------------

class TestOpenDbReadonlyPresence:
    """All 5 OpenClaw files must expose open_db_readonly()."""

    def _module_has_fn(self, module_path: str) -> bool:
        import importlib
        mod = importlib.import_module(module_path)
        return callable(getattr(mod, "open_db_readonly", None))

    def test_bridge_has_open_db_readonly(self):
        assert self._module_has_fn("src.agents.core.openclaw_otocpa_bridge")

    def test_orchestrator_has_open_db_readonly(self):
        assert self._module_has_fn("src.agents.core.openclaw_case_orchestrator")

    def test_escalation_has_open_db_readonly(self):
        assert self._module_has_fn("src.agents.core.openclaw_escalation_engine")

    def test_learning_loop_has_open_db_readonly(self):
        assert self._module_has_fn("src.agents.core.openclaw_learning_loop")

    def test_review_queue_has_open_db_readonly(self):
        assert self._module_has_fn("src.agents.core.openclaw_review_queue")


# ---------------------------------------------------------------------------
# 2. open_db_readonly() actually opens read-only
# ---------------------------------------------------------------------------

class TestOpenDbReadonlyBehavior:
    def test_readonly_allows_select(self, tmp_path: Path):
        db = tmp_path / "test.db"
        make_test_db(db)

        from src.agents.core.openclaw_otocpa_bridge import open_db_readonly
        with open_db_readonly(db) as conn:
            row = conn.execute("SELECT document_id FROM documents").fetchone()
        assert row is not None

    def test_readonly_rejects_insert(self, tmp_path: Path):
        db = tmp_path / "test.db"
        make_test_db(db)

        from src.agents.core.openclaw_otocpa_bridge import open_db_readonly
        with pytest.raises(Exception):
            with open_db_readonly(db) as conn:
                conn.execute("INSERT INTO documents (document_id) VALUES ('X')")
                conn.commit()

    def test_readonly_rejects_update(self, tmp_path: Path):
        db = tmp_path / "test.db"
        make_test_db(db)

        from src.agents.core.openclaw_otocpa_bridge import open_db_readonly
        with pytest.raises(Exception):
            with open_db_readonly(db) as conn:
                conn.execute("UPDATE documents SET vendor='X' WHERE document_id='DOC-001'")
                conn.commit()


# ---------------------------------------------------------------------------
# 3. maybe_post_ready_job() raises PermissionError
# ---------------------------------------------------------------------------

class TestMaybePostReadyJobGuard:
    def test_raises_permission_error_when_execute_false(self):
        from src.agents.core.openclaw_otocpa_bridge import maybe_post_ready_job
        with pytest.raises(PermissionError, match="OpenClaw"):
            maybe_post_ready_job(
                orchestrator_result={"next_step": "do_nothing"},
                document_id="DOC-001",
                execute=False,
            )

    def test_raises_permission_error_when_execute_true(self):
        from src.agents.core.openclaw_otocpa_bridge import maybe_post_ready_job
        with pytest.raises(PermissionError, match="OpenClaw"):
            maybe_post_ready_job(
                orchestrator_result={"next_step": "post_now"},
                document_id="DOC-001",
                execute=True,
            )

    def test_error_message_mentions_posting_builder(self):
        from src.agents.core.openclaw_otocpa_bridge import maybe_post_ready_job
        with pytest.raises(PermissionError) as exc_info:
            maybe_post_ready_job(
                orchestrator_result={},
                document_id="DOC-001",
                execute=True,
            )
        assert "posting_builder" in str(exc_info.value).lower() or "QBO" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 4. Prompt template files exist with required placeholders
# ---------------------------------------------------------------------------

PROMPTS_DIR = ROOT / "src" / "agents" / "prompts"

class TestPromptTemplateFiles:
    def test_escalation_decision_exists(self):
        assert (PROMPTS_DIR / "escalation_decision.txt").exists()

    def test_classify_document_exists(self):
        assert (PROMPTS_DIR / "classify_document.txt").exists()

    def test_explain_anomaly_exists(self):
        assert (PROMPTS_DIR / "explain_anomaly.txt").exists()

    def test_working_paper_exists(self):
        assert (PROMPTS_DIR / "working_paper.txt").exists()

    def _read(self, name: str) -> str:
        return (PROMPTS_DIR / name).read_text(encoding="utf-8")

    def test_escalation_has_document_id_placeholder(self):
        assert "{DOCUMENT_ID}" in self._read("escalation_decision.txt")

    def test_escalation_has_vendor_placeholder(self):
        assert "{VENDOR}" in self._read("escalation_decision.txt")

    def test_escalation_has_client_placeholder(self):
        assert "{CLIENT}" in self._read("escalation_decision.txt")

    def test_escalation_has_amount_placeholder(self):
        assert "{AMOUNT}" in self._read("escalation_decision.txt")

    def test_escalation_has_duplicate_risk_placeholder(self):
        assert "{DUPLICATE_RISK}" in self._read("escalation_decision.txt")

    def test_escalation_has_confidence_placeholder(self):
        assert "{CONFIDENCE}" in self._read("escalation_decision.txt")

    def test_escalation_instructs_json_output(self):
        text = self._read("escalation_decision.txt")
        assert "JSON" in text
        assert "decision" in text

    def test_classify_document_has_text_snippet(self):
        assert "{TEXT_SNIPPET}" in self._read("classify_document.txt")

    def test_explain_anomaly_has_anomaly_context(self):
        assert "{ANOMALY_CONTEXT}" in self._read("explain_anomaly.txt")

    def test_working_paper_has_reviewer(self):
        assert "{REVIEWER}" in self._read("working_paper.txt")


# ---------------------------------------------------------------------------
# 5. build_prompt_payload() loads template and fills placeholders
# ---------------------------------------------------------------------------

class TestBuildPromptPayload:
    def _make_case(self):
        from src.agents.core.openclaw_escalation_engine import EscalationCase
        return EscalationCase(
            document_id="DOC-TEST",
            vendor="Acme Corp",
            client_code="CLIENT1",
            amount=250.00,
            currency="CAD",
            doc_type="invoice",
            category="office",
            gl_account="5000",
            tax_code="TX1",
            confidence=0.87,
            review_status="NeedsReview",
            document_date="2025-03-01",
            duplicate_risk="low",
            duplicate_confirmed=False,
            duplicate_score=0.1,
            learning_reason="insufficient_support",
            exception_action="review",
            vendor_memory_flagged_for_review=True,
        )

    def test_instruction_comes_from_template(self):
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine
        engine = OpenClawEscalationEngine()
        case = self._make_case()
        payload = engine.build_prompt_payload(case)
        assert "DOC-TEST" in payload["instruction"]

    def test_vendor_placeholder_filled(self):
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine
        engine = OpenClawEscalationEngine()
        case = self._make_case()
        payload = engine.build_prompt_payload(case)
        assert "Acme Corp" in payload["instruction"]
        assert "{VENDOR}" not in payload["instruction"]

    def test_amount_placeholder_filled(self):
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine
        engine = OpenClawEscalationEngine()
        case = self._make_case()
        payload = engine.build_prompt_payload(case)
        assert "250.0" in payload["instruction"]
        assert "{AMOUNT}" not in payload["instruction"]

    def test_confidence_placeholder_filled(self):
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine
        engine = OpenClawEscalationEngine()
        case = self._make_case()
        payload = engine.build_prompt_payload(case)
        assert "0.87" in payload["instruction"]
        assert "{CONFIDENCE}" not in payload["instruction"]

    def test_no_remaining_unfilled_placeholders(self):
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine
        engine = OpenClawEscalationEngine()
        case = self._make_case()
        payload = engine.build_prompt_payload(case)
        # Verify none of the known placeholders remain
        for ph in [
            "{DOCUMENT_ID}", "{VENDOR}", "{CLIENT}", "{AMOUNT}", "{CURRENCY}",
            "{DOC_TYPE}", "{DOCUMENT_DATE}", "{GL_ACCOUNT}", "{TAX_CODE}",
            "{CONFIDENCE}", "{DUPLICATE_RISK}", "{DUPLICATE_CONFIRMED}",
            "{DUPLICATE_SCORE}", "{LEARNING_REASON}", "{EXCEPTION_ACTION}",
            "{VENDOR_FLAGGED}",
        ]:
            assert ph not in payload["instruction"], f"Placeholder {ph} was not filled"

    def test_case_dict_still_present(self):
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine
        engine = OpenClawEscalationEngine()
        case = self._make_case()
        payload = engine.build_prompt_payload(case)
        assert "case" in payload
        assert payload["case"]["vendor"] == "Acme Corp"

    def test_fallback_when_template_missing(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """If the template file is missing, a sensible fallback instruction is returned."""
        from src.agents.core.openclaw_escalation_engine import OpenClawEscalationEngine
        engine = OpenClawEscalationEngine()
        case = self._make_case()

        # Point the engine's template loader at an empty directory
        monkeypatch.setattr(
            "src.agents.core.openclaw_escalation_engine.Path",
            lambda *args: tmp_path / "nonexistent" if "prompts" in str(args) else Path(*args),
        )
        # Use _load_prompt_template directly with a bad path
        result = engine._load_prompt_template.__func__(engine, "escalation_decision", case)  # type: ignore[attr-defined]
        # Even with missing file it returns a non-empty string
        assert isinstance(result, str) and len(result) > 10


# ---------------------------------------------------------------------------
# 6. Orchestrator still uses readonly for fetches, rw for writes
# ---------------------------------------------------------------------------

class TestOrchestratorReadonlySplit:
    def test_open_returns_readonly(self, tmp_path: Path):
        db = tmp_path / "test.db"
        make_test_db(db)
        from src.agents.core.openclaw_case_orchestrator import OpenClawCaseOrchestrator
        orc = OpenClawCaseOrchestrator(db_path=db)
        conn = orc._open()
        # Should be readonly — inserting must fail
        with pytest.raises(Exception):
            conn.execute("INSERT INTO documents (document_id) VALUES ('ZZZ')")
            conn.commit()
        conn.close()

    def test_open_rw_allows_writes(self, tmp_path: Path):
        db = tmp_path / "test.db"
        make_test_db(db)
        from src.agents.core.openclaw_case_orchestrator import OpenClawCaseOrchestrator
        orc = OpenClawCaseOrchestrator(db_path=db)
        conn = orc._open_rw()
        # Should succeed
        conn.execute(
            "UPDATE documents SET vendor='Updated' WHERE document_id='DOC-001'"
        )
        conn.commit()
        conn.close()


# ---------------------------------------------------------------------------
# 7. Dashboard — render_troubleshoot produces expected HTML
# ---------------------------------------------------------------------------

class TestRenderTroubleshoot:
    def _get_fn(self):
        import importlib
        mod = importlib.import_module("scripts.review_dashboard")
        return mod

    def test_render_troubleshoot_is_callable(self):
        mod = self._get_fn()
        assert callable(getattr(mod, "render_troubleshoot", None))

    def test_render_troubleshoot_contains_uptime(self):
        mod = self._get_fn()
        html_out = mod.render_troubleshoot({}, {"username": "sam", "role": "owner"}, lang="en")
        assert "uptime" in html_out.lower() or "Service uptime" in html_out

    def test_render_troubleshoot_contains_db_path(self):
        mod = self._get_fn()
        html_out = mod.render_troubleshoot({}, {"username": "sam", "role": "owner"})
        assert "otocpa_agent.db" in html_out

    def test_render_troubleshoot_has_backup_link(self):
        mod = self._get_fn()
        html_out = mod.render_troubleshoot({}, {"username": "sam", "role": "owner"})
        assert "/troubleshoot/backup" in html_out

    def test_render_troubleshoot_has_restart_form(self):
        mod = self._get_fn()
        html_out = mod.render_troubleshoot({}, {"username": "sam", "role": "owner"})
        assert "/troubleshoot/restart" in html_out

    def test_render_troubleshoot_has_log_section(self):
        mod = self._get_fn()
        html_out = mod.render_troubleshoot({}, {"username": "sam", "role": "owner"})
        assert "Log" in html_out or "log" in html_out.lower()

    def test_render_troubleshoot_shows_ai_provider_labels(self):
        mod = self._get_fn()
        html_out = mod.render_troubleshoot({}, {"username": "sam", "role": "owner"})
        assert "Standard" in html_out or "Routine" in html_out or "routine" in html_out.lower()
        assert "Premium" in html_out or "premium" in html_out.lower()


# ---------------------------------------------------------------------------
# 8. _SERVICE_START is set at module load time
# ---------------------------------------------------------------------------

class TestServiceStart:
    def test_service_start_exists(self):
        import importlib
        mod = importlib.import_module("scripts.review_dashboard")
        from datetime import datetime
        assert hasattr(mod, "_SERVICE_START")
        assert isinstance(mod._SERVICE_START, datetime)

    def test_service_start_is_in_the_past_or_now(self):
        import importlib
        from datetime import datetime, timezone
        mod = importlib.import_module("scripts.review_dashboard")
        assert mod._SERVICE_START <= datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# 9. _format_uptime helper
# ---------------------------------------------------------------------------

class TestFormatUptime:
    def _fn(self):
        import importlib
        mod = importlib.import_module("scripts.review_dashboard")
        return mod._format_uptime

    def test_zero_uptime(self):
        from datetime import datetime, timezone
        fn = self._fn()
        now = datetime.now(timezone.utc)
        result = fn(now)
        assert "0m" in result or "0s" in result

    def test_one_hour(self):
        from datetime import datetime, timezone, timedelta
        fn = self._fn()
        start = datetime.now(timezone.utc) - timedelta(hours=1, minutes=5)
        result = fn(start)
        assert "1h" in result
        assert "5m" in result

    def test_one_day(self):
        from datetime import datetime, timezone, timedelta
        fn = self._fn()
        start = datetime.now(timezone.utc) - timedelta(days=2, hours=3)
        result = fn(start)
        assert "2d" in result
        assert "3h" in result


# ---------------------------------------------------------------------------
# 10. LOG_PATH constant exists on dashboard module
# ---------------------------------------------------------------------------

class TestLogPath:
    def test_log_path_constant_exists(self):
        import importlib
        mod = importlib.import_module("scripts.review_dashboard")
        assert hasattr(mod, "LOG_PATH")
        from pathlib import Path
        assert isinstance(mod.LOG_PATH, Path)

    def test_log_path_ends_with_log(self):
        import importlib
        mod = importlib.import_module("scripts.review_dashboard")
        assert mod.LOG_PATH.suffix == ".log"
