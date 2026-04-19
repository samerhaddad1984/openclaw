"""End-to-end workflow breaking scenarios (Sprint F+ Round 2)."""
from __future__ import annotations

import random
from typing import Any


WORKFLOW_BREAKING_SCENARIOS: list[dict[str, Any]] = [
    {
        "subtype": "month_close_500_tx",
        "difficulty": "hard",
        "description": "Full month close with 500 transactions — integrity + audit trail",
        "tx_count": 500,
        "severity": "high",
    },
    {
        "subtype": "year_end_close_5000_tx",
        "difficulty": "nightmare",
        "description": "Year-end close with 5000 transactions — period lock, reversals, roll-forward",
        "tx_count": 5000,
        "severity": "high",
    },
    {
        "subtype": "client_switches_firms_midengagement",
        "difficulty": "nightmare",
        "description": "Client moves from Firm A to Firm B mid-engagement — data ownership transfer",
        "from_firm": "A",
        "to_firm": "B",
        "severity": "high",
    },
    {
        "subtype": "firm_scale_1_to_50_clients",
        "difficulty": "hard",
        "description": "Firm onboards 50 clients in one day — no throttling regressions",
        "new_clients": 50,
        "severity": "medium",
    },
    {
        "subtype": "cpa_user_deactivation_midengagement",
        "difficulty": "hard",
        "description": "CPA leaves firm; pending reviews must be reassigned, not orphaned",
        "pending_reviews": 30,
        "severity": "high",
    },
    {
        "subtype": "client_portal_token_rotated_mid_upload",
        "difficulty": "hard",
        "description": "Portal token refreshed while client is actively uploading",
        "pending_uploads": 5,
        "severity": "medium",
    },
    {
        "subtype": "qbo_disconnected_mid_batch_post",
        "difficulty": "hard",
        "description": "QBO account disconnected halfway through batch post — queue + retry",
        "batch_size": 20,
        "disconnect_at_doc": 10,
        "severity": "high",
    },
    {
        "subtype": "plaid_reauth_required_mid_session",
        "difficulty": "normal",
        "description": "Plaid link requires re-authentication mid-session",
        "severity": "medium",
    },
    {
        "subtype": "stripe_subscription_downgrade_midperiod",
        "difficulty": "normal",
        "description": "Customer downgrades from Pro → Starter mid-month; feature gating updates",
        "severity": "medium",
    },
    {
        "subtype": "backup_during_active_edit",
        "difficulty": "hard",
        "description": "Backup snapshot taken while user is editing 3 docs — data consistency",
        "active_edits": 3,
        "severity": "high",
    },
    {
        "subtype": "fiscal_year_change_midyear",
        "difficulty": "nightmare",
        "description": "Client changes fiscal year end from Dec to Mar mid-year — re-period all txs",
        "old_fy_end": "12-31",
        "new_fy_end": "03-31",
        "severity": "high",
    },
]


def generate(rnd: random.Random) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for spec in WORKFLOW_BREAKING_SCENARIOS:
        out.append({
            "category": "workflow",
            "subtype": spec["subtype"],
            "difficulty": spec["difficulty"],
            "description": spec["description"],
            "severity_on_failure": spec.get("severity", "medium"),
            "expected_fail": False,
            "future_feature": True,
            "affects_engines": [
                "src.engines.period_close",
                "src.engines.concurrency_engine",
                "src.engines.correction_chain",
                "src.engines.license_engine",
            ],
            "oracle": "workflow",
            "input_spec": {"kind": "workflow_synthetic", "spec": spec},
            "ground_truth": {"subtype": spec["subtype"], "expected": spec},
        })
    return out
