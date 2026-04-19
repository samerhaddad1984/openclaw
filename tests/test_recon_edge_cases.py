"""Sprint I Part 1 — recon edge-case tests."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.engines.recon_edge_cases import (  # noqa: E402
    DEFAULT_NSF_FEE,
    DEFAULT_STOP_PAYMENT_FEE,
    detect_internal_transfers,
    ensure_edge_tables,
    handle_bank_error_correction,
    handle_nsf_cheque,
    handle_stop_payment,
    list_recon_adjustments,
    reconcile_fx_transaction,
)


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "r.db"
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    # Seed bank_transactions schema (matches production layout).
    c.executescript("""
        CREATE TABLE bank_transactions (
            id TEXT PRIMARY KEY,
            client_code TEXT,
            plaid_transaction_id TEXT,
            account_id TEXT,
            date TEXT,
            amount REAL,
            description TEXT,
            merchant_name TEXT,
            category TEXT,
            pending INTEGER,
            matched_document_id TEXT,
            reconciled INTEGER DEFAULT 0,
            created_at TEXT,
            match_confidence_tier TEXT,
            match_score_json TEXT
        );
    """)
    ensure_edge_tables(c)
    yield c
    c.close()


def _seed_bank_tx(conn, **kw):
    conn.execute(
        "INSERT INTO bank_transactions (id, client_code, account_id, date, "
        "amount, description, merchant_name) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (kw["id"], kw.get("client", "ACME"), kw.get("account", "ACCT1"),
         kw["date"], kw["amount"], kw.get("desc", ""), kw.get("merchant", "")),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# F1 — NSF
# ---------------------------------------------------------------------------

def test_nsf_emits_two_balanced_jes(conn):
    _seed_bank_tx(conn, id="DEP1", date="2025-06-01", amount=2500.0,
                  desc="Cheque deposit Acme Customer")
    r = handle_nsf_cheque(
        conn, firm_code="F1", client_code="ACME",
        original_deposit_tx_id="DEP1", nsf_date="2025-06-05",
        customer_name="Acme Customer", cheque_number="1234",
        amount=2500.0,
    )
    assert r["reversal_je"]["balanced"] is True
    assert r["fee_je"]["balanced"] is True
    assert r["nsf_fee"] == float(DEFAULT_NSF_FEE)


def test_nsf_marks_bank_tx_returned(conn):
    _seed_bank_tx(conn, id="DEP2", date="2025-06-01", amount=1000.0)
    handle_nsf_cheque(
        conn, firm_code="F1", client_code="ACME",
        original_deposit_tx_id="DEP2", nsf_date="2025-06-05",
        amount=1000.0,
    )
    row = conn.execute(
        "SELECT nsf_returned, nsf_date FROM bank_transactions WHERE id='DEP2'",
    ).fetchone()
    assert row["nsf_returned"] == 1
    assert row["nsf_date"] == "2025-06-05"


def test_nsf_persists_audit_row(conn):
    _seed_bank_tx(conn, id="DEP3", date="2025-06-01", amount=500.0)
    r = handle_nsf_cheque(
        conn, firm_code="F1", client_code="ACME",
        original_deposit_tx_id="DEP3", nsf_date="2025-06-05",
        amount=500.0,
    )
    rows = list_recon_adjustments(conn, client_code="ACME",
                                   adjustment_type="nsf_return")
    assert len(rows) == 1
    assert rows[0]["amount"] == 500.0
    assert rows[0]["fee_amount"] == float(DEFAULT_NSF_FEE)


def test_nsf_rejects_zero_amount(conn):
    _seed_bank_tx(conn, id="DEP4", date="2025-06-01", amount=0.0)
    with pytest.raises(ValueError):
        handle_nsf_cheque(
            conn, firm_code="F1", client_code="ACME",
            original_deposit_tx_id="DEP4", nsf_date="2025-06-05",
            amount=0,
        )


def test_nsf_rejects_missing_tx_id(conn):
    with pytest.raises(ValueError):
        handle_nsf_cheque(
            conn, firm_code="F1", client_code="ACME",
            original_deposit_tx_id="", nsf_date="2025-06-05", amount=100,
        )


def test_nsf_custom_fee(conn):
    _seed_bank_tx(conn, id="DEP5", date="2025-06-01", amount=1500.0)
    r = handle_nsf_cheque(
        conn, firm_code="F1", client_code="ACME",
        original_deposit_tx_id="DEP5", nsf_date="2025-06-05",
        amount=1500.0, nsf_fee=60.0,
    )
    assert r["nsf_fee"] == 60.0


# ---------------------------------------------------------------------------
# F2 — Stop payment
# ---------------------------------------------------------------------------

def test_stop_payment_fee_only_when_not_cleared(conn):
    _seed_bank_tx(conn, id="CHQ1", date="2025-07-01", amount=-1200.0,
                  desc="Cheque #500")
    r = handle_stop_payment(
        conn, firm_code="F1", client_code="ACME",
        cheque_tx_id="CHQ1", stop_date="2025-07-02",
        amount=1200.0, payee="Vendor X", cheque_number="500",
    )
    assert r["fee_je"] is not None
    assert r["fee_je"]["balanced"] is True
    assert r["reversal_je"] is None
    assert r["stop_fee"] == float(DEFAULT_STOP_PAYMENT_FEE)


def test_stop_payment_with_reversal_when_cleared(conn):
    _seed_bank_tx(conn, id="CHQ2", date="2025-07-01", amount=-1200.0)
    r = handle_stop_payment(
        conn, firm_code="F1", client_code="ACME",
        cheque_tx_id="CHQ2", stop_date="2025-07-03",
        amount=1200.0, cheque_already_cleared=True,
        payee="Vendor Y", cheque_number="501",
    )
    assert r["reversal_je"] is not None
    assert r["reversal_je"]["balanced"] is True
    assert r["fee_je"] is not None


def test_stop_payment_marks_tx_stopped(conn):
    _seed_bank_tx(conn, id="CHQ3", date="2025-07-01", amount=-800.0)
    handle_stop_payment(
        conn, firm_code="F1", client_code="ACME",
        cheque_tx_id="CHQ3", stop_date="2025-07-04",
        amount=800.0, cheque_number="502",
    )
    row = conn.execute(
        "SELECT stop_payment, stop_payment_date FROM bank_transactions WHERE id='CHQ3'",
    ).fetchone()
    assert row["stop_payment"] == 1
    assert row["stop_payment_date"] == "2025-07-04"


def test_stop_payment_audit_recorded(conn):
    _seed_bank_tx(conn, id="CHQ4", date="2025-07-01", amount=-500.0)
    handle_stop_payment(
        conn, firm_code="F1", client_code="ACME",
        cheque_tx_id="CHQ4", stop_date="2025-07-04",
        amount=500.0, cheque_number="503",
    )
    rows = list_recon_adjustments(conn, client_code="ACME",
                                   adjustment_type="stop_payment")
    assert len(rows) == 1


def test_stop_payment_rejects_negative_amount(conn):
    _seed_bank_tx(conn, id="CHQ5", date="2025-07-01", amount=-100.0)
    with pytest.raises(ValueError):
        handle_stop_payment(
            conn, firm_code="F1", client_code="ACME",
            cheque_tx_id="CHQ5", stop_date="2025-07-04", amount=-50.0,
        )


# ---------------------------------------------------------------------------
# F3 — Internal transfers
# ---------------------------------------------------------------------------

def test_detect_internal_transfer_pair(conn):
    _seed_bank_tx(conn, id="W1", account="OPS", date="2025-06-01",
                  amount=-5000.0, desc="Transfer to savings")
    _seed_bank_tx(conn, id="D1", account="SAV", date="2025-06-01",
                  amount=5000.0, desc="Transfer from operating")
    matches = detect_internal_transfers(conn, client_code="ACME")
    assert len(matches) == 1
    m = matches[0]
    assert sorted(m["transfer_pair"]) == ["D1", "W1"]
    assert m["amount"] == 5000.0


def test_detect_skips_single_account(conn):
    _seed_bank_tx(conn, id="A1", account="OPS", date="2025-06-01", amount=-100.0)
    _seed_bank_tx(conn, id="A2", account="OPS", date="2025-06-01", amount=100.0)
    matches = detect_internal_transfers(conn, client_code="ACME")
    assert matches == []


def test_detect_amount_mismatch_skipped(conn):
    _seed_bank_tx(conn, id="W2", account="OPS", date="2025-06-01", amount=-5000.0)
    _seed_bank_tx(conn, id="D2", account="SAV", date="2025-06-01", amount=4900.0)
    matches = detect_internal_transfers(conn, client_code="ACME")
    assert matches == []


def test_detect_outside_tolerance_window_skipped(conn):
    _seed_bank_tx(conn, id="W3", account="OPS", date="2025-06-01", amount=-1000.0)
    _seed_bank_tx(conn, id="D3", account="SAV", date="2025-06-15", amount=1000.0)
    matches = detect_internal_transfers(conn, client_code="ACME",
                                         tolerance_days=3)
    assert matches == []


def test_detect_marks_both_reconciled(conn):
    _seed_bank_tx(conn, id="W4", account="OPS", date="2025-06-01", amount=-2500.0)
    _seed_bank_tx(conn, id="D4", account="SAV", date="2025-06-02", amount=2500.0)
    detect_internal_transfers(conn, client_code="ACME")
    rec = {r[0]: r[1] for r in conn.execute(
        "SELECT id, reconciled FROM bank_transactions"
    )}
    assert rec["W4"] == 1
    assert rec["D4"] == 1


def test_detect_persists_internal_transfer_row(conn):
    _seed_bank_tx(conn, id="W5", account="OPS", date="2025-06-01", amount=-7500.0)
    _seed_bank_tx(conn, id="D5", account="SAV", date="2025-06-01", amount=7500.0)
    detect_internal_transfers(conn, client_code="ACME")
    rows = conn.execute("SELECT * FROM internal_transfers").fetchall()
    assert len(rows) == 1
    assert rows[0]["amount"] == 7500.0


# ---------------------------------------------------------------------------
# F4 — FX
# ---------------------------------------------------------------------------

def test_fx_within_tolerance_no_je():
    r = reconcile_fx_transaction(
        bank_amount_cad=1349.50, document_amount_foreign=1000.0,
        exchange_rate=1.35, foreign_currency="USD",
    )
    # CAD eq = 1350.00; bank 1349.50 → diff -0.50, |0.50/1350| ≈ 0.04%, well
    # within 2%. fx_je is still produced because diff > 0; matched=True.
    assert r["matched"] is True
    assert r["within_tolerance"] is True


def test_fx_outside_tolerance_emits_je():
    r = reconcile_fx_transaction(
        bank_amount_cad=1500.0, document_amount_foreign=1000.0,
        exchange_rate=1.35, foreign_currency="USD",
    )
    # CAD eq = 1350; bank 1500 → diff +150, ~11% > 2% tolerance.
    assert r["within_tolerance"] is False
    assert r["fx_je"] is not None
    assert r["fx_je"]["balanced"] is True
    # Loss because bank > expected.
    assert r["difference"] == 150.0


def test_fx_negative_difference_is_gain():
    r = reconcile_fx_transaction(
        bank_amount_cad=1000.0, document_amount_foreign=1000.0,
        exchange_rate=1.30, foreign_currency="USD",
    )
    # CAD eq = 1300; bank 1000 → diff -300, FX gain.
    assert r["difference"] == -300.0
    assert r["fx_je"]["balanced"] is True


def test_fx_invalid_inputs_raise():
    with pytest.raises(ValueError):
        reconcile_fx_transaction(
            bank_amount_cad=0, document_amount_foreign=1000,
            exchange_rate=1.35,
        )


def test_fx_perfect_match_no_diff():
    r = reconcile_fx_transaction(
        bank_amount_cad=1350.0, document_amount_foreign=1000.0,
        exchange_rate=1.35,
    )
    assert r["difference"] == 0.0
    assert r["fx_je"] is None
    assert r["matched"] is True


def test_fx_returns_pct_difference():
    r = reconcile_fx_transaction(
        bank_amount_cad=1400.0, document_amount_foreign=1000.0,
        exchange_rate=1.35,
    )
    # diff 50 / 1350 ≈ 0.037 ≈ 3.7 %.
    assert 0.03 < r["pct_difference"] < 0.04


# ---------------------------------------------------------------------------
# F5 — Bank-error correction
# ---------------------------------------------------------------------------

def test_bank_error_correction_emits_balanced_je(conn):
    r = handle_bank_error_correction(
        conn, firm_code="F1", client_code="ACME",
        wrong_tx_id="WRG1", wrong_amount=1234.56, correct_amount=1234.00,
        correction_date="2025-08-15", description="Cheque mis-keyed",
    )
    assert r["audit_id"] is not None
    assert r["je"]["balanced"] is True
    # Net effect = correct - wrong = -0.56.
    assert r["net_effect"] == -0.56


def test_bank_error_no_diff_returns_no_op(conn):
    r = handle_bank_error_correction(
        conn, firm_code="F1", client_code="ACME",
        wrong_tx_id="WRG2", wrong_amount=100.0, correct_amount=100.0,
        correction_date="2025-08-15",
    )
    assert r["audit_id"] is None
    assert r["net_effect"] == 0.0


def test_bank_error_records_audit(conn):
    handle_bank_error_correction(
        conn, firm_code="F1", client_code="ACME",
        wrong_tx_id="WRG3", wrong_amount=500.00, correct_amount=550.00,
        correction_date="2025-08-15", description="Underposted",
    )
    rows = list_recon_adjustments(conn, client_code="ACME",
                                   adjustment_type="bank_error_correction")
    assert len(rows) == 1


def test_bank_error_positive_net_uses_debit_cash(conn):
    # correct > wrong → cash should INCREASE (debit cash).
    r = handle_bank_error_correction(
        conn, firm_code="F1", client_code="ACME",
        wrong_tx_id="WRG4", wrong_amount=100.00, correct_amount=200.00,
        correction_date="2025-08-15",
    )
    cash_line = next(l for l in r["je"]["lines"] if l["account"] == "1010")
    assert cash_line["debit"] == 100.0
    assert cash_line["credit"] == 0


def test_list_adjustments_filtered_by_type(conn):
    _seed_bank_tx(conn, id="X1", date="2025-06-01", amount=100.0)
    handle_nsf_cheque(conn, firm_code="F1", client_code="ACME",
                      original_deposit_tx_id="X1", nsf_date="2025-06-05",
                      amount=100.0)
    handle_bank_error_correction(
        conn, firm_code="F1", client_code="ACME",
        wrong_tx_id="X1", wrong_amount=100, correct_amount=99,
        correction_date="2025-06-10",
    )
    nsf = list_recon_adjustments(conn, client_code="ACME",
                                  adjustment_type="nsf_return")
    err = list_recon_adjustments(conn, client_code="ACME",
                                  adjustment_type="bank_error_correction")
    assert len(nsf) == 1
    assert len(err) == 1
