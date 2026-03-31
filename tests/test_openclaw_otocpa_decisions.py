from __future__ import annotations

from src.agents.core.openclaw_otocpa_skill import summarize_bridge_result


def test_skill_summary_for_posted_document() -> None:
    bridge_result = {
        "document_id": "doc_f3a8b9c96528",
        "working_document": {
            "vendor": "CompanyCam",
            "client_code": "SOUSSOL Quebec",
            "doc_type": "invoice",
            "amount": 11.72,
            "currency": "USD",
            "document_date": "2025-10-22",
            "confidence": 0.97,
            "gl_account": "Software Expense",
            "tax_code": "NONE",
        },
        "learning_gl_account_result": {
            "applied": False,
            "reason": "insufficient_support",
        },
        "duplicate_result": {
            "risk_level": "low",
            "duplicate_confirmed": False,
            "score": 0.59,
            "candidates": [{}, {}, {}],
            "reasons": [
                "weak_duplicate_signal",
                "same_client_exact",
                "same_vendor_exact",
                "same_doc_type",
                "near_amount",
            ],
        },
        "auto_result": {
            "decision": "auto_post",
            "auto_approved": True,
            "approval_score": 1.0,
        },
        "exception_result": {
            "action": "auto_post",
        },
        "orchestrator_result": {
            "next_step": "do_nothing",
            "status": "planned",
            "reason": "document already posted",
        },
        "posting_snapshot_after": {
            "exists": True,
            "posting_id": "post_qbo_expense_doc_f3a8b9c96528",
            "posting_status": "posted",
            "approval_state": "approved_for_posting",
            "external_id": "145",
        },
        "final_action": "do_nothing",
        "final_reason": "document already posted",
        "execute": False,
    }

    result = summarize_bridge_result(bridge_result, debug=False)

    assert result["status"] == "ok"
    assert result["decision"]["auto_decision"] == "auto_post"
    assert result["decision"]["final_action"] == "do_nothing"
    assert result["posting"]["posting_status_after"] == "posted"
    assert result["posting"]["external_id"] == "145"
    assert result["duplicate"]["risk_level"] == "low"


def test_skill_summary_for_duplicate_hold_document() -> None:
    bridge_result = {
        "document_id": "doc_d915301a259c",
        "working_document": {
            "vendor": "CompanyCam",
            "client_code": "SOUSSOL Quebec",
            "doc_type": "invoice",
            "amount": 1503.22,
            "currency": "USD",
            "document_date": "2025-10-06",
            "confidence": 0.97,
            "gl_account": "Software Expense",
            "tax_code": "NONE",
        },
        "learning_gl_account_result": {
            "applied": False,
            "reason": "insufficient_support",
        },
        "duplicate_result": {
            "risk_level": "medium",
            "duplicate_confirmed": False,
            "score": 0.75,
            "candidates": [{}, {}],
            "reasons": [
                "possible_duplicate",
                "same_client_exact",
                "same_vendor_exact",
                "same_doc_type",
                "same_amount",
            ],
        },
        "auto_result": {
            "decision": "approve_but_hold",
            "auto_approved": True,
            "approval_score": 1.0,
        },
        "exception_result": {
            "action": "approve_but_hold",
        },
        "orchestrator_result": {
            "next_step": "do_nothing",
            "status": "planned",
            "reason": "approve_but_hold document already ready",
        },
        "posting_snapshot_after": {
            "exists": True,
            "posting_id": "post_qbo_expense_doc_d915301a259c",
            "posting_status": "ready_to_post",
            "approval_state": "approved_for_posting",
            "external_id": "",
        },
        "final_action": "do_nothing",
        "final_reason": "approve_but_hold document already ready",
        "execute": False,
    }

    result = summarize_bridge_result(bridge_result, debug=False)

    assert result["status"] == "ok"
    assert result["decision"]["auto_decision"] == "approve_but_hold"
    assert result["decision"]["final_action"] == "do_nothing"
    assert result["posting"]["posting_status_after"] == "ready_to_post"
    assert result["posting"]["external_id"] == ""
    assert result["duplicate"]["risk_level"] == "medium"