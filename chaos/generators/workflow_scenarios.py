"""End-to-end workflow stress scenarios.

These exercise the full pipeline: upload → OCR → review → QBO post → recon.
Runner uses mocks for external services (QBO, Plaid, Stripe) so the whole
pipeline can be exercised deterministically.
"""
from __future__ import annotations

import random
from typing import Any

WORKFLOW_SPECS: list[dict[str, Any]] = [
    {
        "subtype": "portal_50_concurrent_uploads",
        "difficulty": "nightmare",
        "description": "50 docs uploaded via client portal simultaneously — no lost docs",
        "input": {"channel": "portal", "count": 50, "concurrent": True},
        "expected": {"all_persisted": True, "no_duplicates": True},
        "severity_on_failure": "critical",
    },
    {
        "subtype": "multichannel_same_client_simultaneous",
        "difficulty": "nightmare",
        "description": "Same client uploads via WhatsApp + portal + email simultaneously",
        "input": {"channels": ["whatsapp", "portal", "email"], "count_each": 10},
        "expected": {"all_persisted": True, "client_correctly_assigned": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "employee_assigned_200_pending",
        "difficulty": "hard",
        "description": "One employee queue has 200 pending docs — dashboard paginates/performs",
        "input": {"employee_pending_count": 200},
        "expected": {"queue_loads_under_seconds": 3.0},
        "severity_on_failure": "medium",
    },
    {
        "subtype": "firm_plan_switch_midperiod",
        "difficulty": "hard",
        "description": "Firm upgrades plan mid-period — entitlements change immediately",
        "input": {"old_plan": "starter", "new_plan": "pro", "switch_mid_month": True},
        "expected": {"entitlement_updated": True, "historical_data_preserved": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "client_switches_firms",
        "difficulty": "hard",
        "description": "Client moves from firm A to firm B during period close",
        "input": {"old_firm": "A", "new_firm": "B"},
        "expected": {"client_reassigned": True, "old_data_retained_for_firm_a": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "qbo_token_expires_mid_batch",
        "difficulty": "nightmare",
        "description": "QBO OAuth token expires during batch post — must pause, refresh, resume",
        "input": {"batch_size": 30, "expire_at": 15},
        "expected": {"resumed": True, "no_duplicate_posts": True},
        "severity_on_failure": "critical",
    },
    {
        "subtype": "plaid_webhook_storm",
        "difficulty": "hard",
        "description": "100 Plaid webhooks fire in 10s — idempotency + rate-limit",
        "input": {"webhook_count": 100, "window_seconds": 10},
        "expected": {"all_processed_once": True, "no_duplicate_work": True},
        "severity_on_failure": "high",
    },
    {
        "subtype": "stripe_webhook_replay",
        "difficulty": "hard",
        "description": "Stripe replays the same webhook — must be idempotent",
        "input": {"replay_count": 5},
        "expected": {"processed_once": True, "subscription_not_duplicated": True},
        "severity_on_failure": "critical",
    },
    {
        "subtype": "full_journey_signup_to_post",
        "difficulty": "normal",
        "description": "Happy-path E2E: signup → onboard → upload → review → post",
        "input": {"steps": ["signup", "onboard", "upload", "review", "post"]},
        "expected": {"all_steps_green": True},
        "severity_on_failure": "critical",
    },
    {
        "subtype": "post_retry_after_network_error",
        "difficulty": "hard",
        "description": "QBO post fails once with network error — retries and succeeds",
        "input": {"fail_count_before_success": 1},
        "expected": {"retried": True, "finally_posted": True, "no_duplicates": True},
        "severity_on_failure": "high",
    },
]


def generate(rnd: random.Random) -> list[dict[str, Any]]:
    out = []
    for spec in WORKFLOW_SPECS:
        out.append({
            "category": "workflow",
            "subtype": spec["subtype"],
            "difficulty": spec["difficulty"],
            "description": spec["description"],
            "severity_on_failure": spec["severity_on_failure"],
            "affects_engines": [
                "src.engines.ocr_engine",
                "src.agents.tools.qbo_online_adapter",
                "src.integrations.plaid_client",
                "src.integrations.stripe_client",
                "src.integrations.whatsapp",
            ],
            "oracle": "workflow",
            "input_spec": {"kind": "workflow", **spec["input"]},
            "ground_truth": spec["expected"],
        })
    return out
