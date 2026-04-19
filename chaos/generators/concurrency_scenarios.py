"""Concurrency / multi-tenant stress scenarios (Sprint F+ Round 2).

Tests that cross-tenant isolation, race conditions, webhook retries,
session timeouts, and concurrent edits all behave correctly under load.
"""
from __future__ import annotations

import random
from typing import Any


CONCURRENCY_SCENARIOS: list[dict[str, Any]] = [
    {
        "subtype": "concurrent_firm_uploads",
        "difficulty": "hard",
        "description": "Firm A and Firm B each upload 50 docs simultaneously",
        "firms": 2,
        "docs_per_firm": 50,
        "severity": "high",
    },
    {
        "subtype": "cross_firm_read_attempt",
        "difficulty": "nightmare",
        "description": "Firm A admin attempts to read Firm B data via URL manipulation",
        "attacker_firm": "A",
        "target_firm": "B",
        "expected_result": "403_forbidden",
        "severity": "high",
    },
    {
        "subtype": "same_doc_concurrent_edits",
        "difficulty": "hard",
        "description": "10 users of same firm edit same document simultaneously",
        "user_count": 10,
        "expected": "last_write_wins_with_audit",
        "severity": "medium",
    },
    {
        "subtype": "client_portal_during_cpa_review",
        "difficulty": "normal",
        "description": "Client submits doc via portal while CPA is reviewing the queue",
        "client_uploads": 3,
        "cpa_active": True,
        "severity": "low",
    },
    {
        "subtype": "qbo_token_expires_midbatch",
        "difficulty": "hard",
        "description": "QBO OAuth token expires while batch posting 20 docs",
        "batch_size": 20,
        "expiry_at_doc": 8,
        "severity": "high",
    },
    {
        "subtype": "stripe_webhook_retry_during_signup",
        "difficulty": "normal",
        "description": "Stripe webhook retry fires during user signup — idempotency key required",
        "webhook_count": 3,
        "severity": "medium",
    },
    {
        "subtype": "plaid_webhook_during_recon",
        "difficulty": "normal",
        "description": "Plaid bank-tx update webhook while reconciliation is running",
        "running_recon": True,
        "webhook_tx_count": 15,
        "severity": "medium",
    },
    {
        "subtype": "two_fa_timeout_long_op",
        "difficulty": "normal",
        "description": "2FA session times out during a 6-minute T2 generation",
        "session_timeout_s": 300,
        "op_duration_s": 360,
        "severity": "low",
    },
    {
        "subtype": "session_expires_midupload",
        "difficulty": "normal",
        "description": "Dashboard session expires during multi-file upload",
        "files_to_upload": 5,
        "expiry_at_file": 2,
        "severity": "low",
    },
    {
        "subtype": "concurrent_delete_and_edit",
        "difficulty": "hard",
        "description": "User A deletes doc while User B is editing it",
        "expected": "edit_blocked_or_rolled_back",
        "severity": "high",
    },
    {
        "subtype": "rapid_login_rate_limit",
        "difficulty": "hard",
        "description": "Same IP attempts 50 logins in 10 seconds — rate limiter must kick in",
        "attempts": 50,
        "window_s": 10,
        "expected_block_after": 10,
        "severity": "high",
    },
    {
        "subtype": "webhook_idempotency_duplicate",
        "difficulty": "hard",
        "description": "Same Stripe event fires twice — second must be idempotent",
        "event_id": "evt_test_duplicate_abc",
        "severity": "high",
    },
]


def generate(rnd: random.Random) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for spec in CONCURRENCY_SCENARIOS:
        out.append({
            "category": "concurrency",
            "subtype": spec["subtype"],
            "difficulty": spec["difficulty"],
            "description": spec["description"],
            "severity_on_failure": spec.get("severity", "medium"),
            "expected_fail": False,
            "future_feature": True,  # marked future because the chaos
                                      # runners don't yet simulate true
                                      # concurrency; these are design-level
                                      # scenarios for a real load test.
            "affects_engines": [
                "src.engines.concurrency_engine",
                "src.engines.correction_chain",
            ],
            "oracle": "workflow",
            "input_spec": {"kind": "concurrency_synthetic", "spec": spec},
            "ground_truth": {"subtype": spec["subtype"], "expected": spec},
        })
    return out
