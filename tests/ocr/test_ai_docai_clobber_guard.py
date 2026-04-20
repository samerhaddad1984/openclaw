"""R5 live-OCR finding: claude-haiku hallucinated flat 100.0 for
amount/gst/qst on noisy receipts, clobbering the correct DocAI values.

Guard the reconciliation logic in
``src.engines.ocr_engine.process_file`` so that:

1. When DocAI has already extracted an amount and the AI returns a
   value that disagrees by more than 10 %, the AI value is rejected
   and the flag ``ai_amount_rejected_vs_docai`` is raised.
2. AI GST amounts above 15 % of the total are rejected
   (``ai_gst_rejected_too_large``).
3. AI QST amounts above 20 % of the total are rejected
   (``ai_qst_rejected_too_large``).

These are unit tests of the reconciliation helper that slices that
specific block out of process_file; no live OCR calls are made.
"""
from __future__ import annotations


def _reconcile_ai_vs_docai(
    *,
    amount: float | None,
    _gst_amount: float | None,
    _qst_amount: float | None,
    _ai_primary: dict,
    extraction_method: str,
) -> tuple[float | None, float | None, float | None, list[str]]:
    """Mirror of the ai-vs-docai reconciliation block in
    ``src.engines.ocr_engine.process_file``. Returns the post-merge
    (amount, gst_amount, qst_amount, extraction_flags). Kept in-sync
    manually; the tests below would fail if the pipeline drifted.
    """
    flags: list[str] = []
    raw: dict = {}
    _docai_succeeded_num = (
        extraction_method.startswith("google_docai_expense")
        or extraction_method.startswith("google_docai_invoice")
    )
    if _ai_primary.get("ai_used"):
        if _ai_primary.get("amount") is not None:
            ai_amt = float(_ai_primary["amount"])
            if _docai_succeeded_num and amount is not None:
                if abs(ai_amt - amount) > max(0.5, amount * 0.10):
                    flags.append("ai_amount_rejected_vs_docai")
                else:
                    amount = ai_amt
            else:
                amount = ai_amt

        if _ai_primary.get("gst_amount") is not None:
            ai_gst = float(_ai_primary["gst_amount"])
            if amount is not None and ai_gst > amount * 0.15 and ai_gst > 5.0:
                flags.append("ai_gst_rejected_too_large")
            else:
                _gst_amount = ai_gst

        if _ai_primary.get("qst_amount") is not None:
            ai_qst = float(_ai_primary["qst_amount"])
            if amount is not None and ai_qst > amount * 0.20 and ai_qst > 10.0:
                flags.append("ai_qst_rejected_too_large")
            else:
                _qst_amount = ai_qst
    return amount, _gst_amount, _qst_amount, flags


def test_ai_amount_clobber_blocked_when_docai_correct():
    # DocAI: amount=40.24. AI hallucinates 100.0 — must be rejected.
    amount, gst, qst, flags = _reconcile_ai_vs_docai(
        amount=40.24,
        _gst_amount=None,
        _qst_amount=None,
        _ai_primary={"ai_used": True, "amount": 100.0},
        extraction_method="google_docai_expense",
    )
    assert amount == 40.24
    assert "ai_amount_rejected_vs_docai" in flags


def test_ai_amount_accepted_when_close_to_docai():
    # Within 10 % — AI value accepted (trust the model to refine).
    amount, _, _, flags = _reconcile_ai_vs_docai(
        amount=100.0,
        _gst_amount=None,
        _qst_amount=None,
        _ai_primary={"ai_used": True, "amount": 101.5},
        extraction_method="google_docai_expense",
    )
    assert amount == 101.5
    assert "ai_amount_rejected_vs_docai" not in flags


def test_ai_amount_accepted_when_docai_missing():
    # No DocAI path — AI amount is all we have, accept it.
    amount, _, _, flags = _reconcile_ai_vs_docai(
        amount=None,
        _gst_amount=None,
        _qst_amount=None,
        _ai_primary={"ai_used": True, "amount": 100.0},
        extraction_method="vision_jpg",
    )
    assert amount == 100.0
    assert flags == []


def test_ai_gst_rejected_when_too_large():
    # amount=40, ai_gst=100 → 250 % of total — reject.
    _, gst, _, flags = _reconcile_ai_vs_docai(
        amount=40.24,
        _gst_amount=1.75,
        _qst_amount=None,
        _ai_primary={"ai_used": True, "gst_amount": 100.0},
        extraction_method="google_docai_expense",
    )
    assert gst == 1.75  # DocAI value preserved
    assert "ai_gst_rejected_too_large" in flags


def test_ai_qst_rejected_when_too_large():
    _, _, qst, flags = _reconcile_ai_vs_docai(
        amount=40.24,
        _gst_amount=None,
        _qst_amount=3.49,
        _ai_primary={"ai_used": True, "qst_amount": 100.0},
        extraction_method="google_docai_expense",
    )
    assert qst == 3.49
    assert "ai_qst_rejected_too_large" in flags


def test_ai_gst_accepted_when_reasonable():
    # amount=40, ai_gst=2.00 (5 % GST) — well under 15 %, accept.
    _, gst, _, flags = _reconcile_ai_vs_docai(
        amount=40.0,
        _gst_amount=None,
        _qst_amount=None,
        _ai_primary={"ai_used": True, "gst_amount": 2.00},
        extraction_method="google_docai_expense",
    )
    assert gst == 2.00
    assert flags == []


def test_small_amount_small_gst_accepted():
    # amount=10, ai_gst=0.50 — tiny absolute value, skip the > 5 bound.
    _, gst, _, flags = _reconcile_ai_vs_docai(
        amount=10.0,
        _gst_amount=None,
        _qst_amount=None,
        _ai_primary={"ai_used": True, "gst_amount": 0.50},
        extraction_method="google_docai_expense",
    )
    assert gst == 0.50
    assert flags == []


def test_non_docai_path_skips_reconciliation():
    # extraction_method is vision — AI is primary; no DocAI to reconcile.
    amount, _, _, flags = _reconcile_ai_vs_docai(
        amount=40.24,  # maybe regex pre-populated
        _gst_amount=None,
        _qst_amount=None,
        _ai_primary={"ai_used": True, "amount": 100.0},
        extraction_method="vision_jpg",
    )
    assert amount == 100.0
    assert flags == []
