"""
tests/red_team/test_audit_evidence_destruction.py
==================================================
Red-team attacks against audit evidence sufficiency, three-way matching,
bank reconciliation, and CAS compliance.

Targets:
  - src/engines/audit_engine.py   (working papers, evidence chains, sampling)
  - src/engines/reconciliation_engine.py (bank reconciliation)
  - src/engines/cas_engine.py     (materiality, risk, controls, related parties)

35 attack vectors across 5 categories.
"""
from __future__ import annotations

import json
import sqlite3
from decimal import Decimal

import pytest

# ---------------------------------------------------------------------------
# Engine imports
# ---------------------------------------------------------------------------
from src.engines.audit_engine import (
    ensure_audit_tables,
    get_or_create_evidence,
    link_evidence_documents,
    get_evidence_chains,
    check_and_update_evidence_for_period,
    get_or_create_working_paper,
    update_working_paper,
    add_working_paper_item,
    get_working_paper_items,
    get_sample,
    get_sample_status,
    create_engagement,
    get_engagement,
    get_engagement_progress,
    VALID_EVIDENCE_TYPES,
)
from src.engines.reconciliation_engine import (
    ensure_reconciliation_tables,
    create_reconciliation,
    add_reconciliation_item,
    calculate_reconciliation,
    finalize_reconciliation,
    get_reconciliation,
    get_reconciliation_items,
    BALANCE_TOLERANCE,
)
from src.engines.cas_engine import (
    ensure_cas_tables,
    calculate_materiality,
    save_materiality,
    get_materiality,
    create_risk_matrix,
    assess_risk,
    get_risk_assessment,
    get_risk_summary,
    create_control_test,
    record_test_results,
    get_control_tests,
    get_control_effectiveness_summary,
    add_related_party,
    flag_related_party_transaction,
    get_related_party_transactions,
    VALID_ASSERTIONS,
    VALID_RISK_LEVELS,
    VALID_MATERIALITY_BASES,
    _MATERIALITY_RATES,
    PERFORMANCE_RATE,
    CLEARLY_TRIVIAL_RATE,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn():
    """In-memory SQLite with row_factory."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    ensure_audit_tables(c)
    ensure_reconciliation_tables(c)
    ensure_cas_tables(c)
    yield c
    c.close()


@pytest.fixture
def engagement(conn):
    """Create a standard test engagement."""
    return create_engagement(
        conn, "RED_TEAM_INC", "2025",
        engagement_type="audit",
        partner="Partner A",
        manager="Manager B",
    )


def _insert_doc(conn, doc_id, client="RED_TEAM_INC", doc_type="invoice",
                date="2025-03-15", amount=1000.0, vendor="Vendor A",
                gl_account="5200", review_status="approved"):
    """Insert a fake document row for testing."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            client_code TEXT,
            doc_type TEXT,
            document_date TEXT,
            amount REAL,
            vendor TEXT,
            gl_account TEXT,
            review_status TEXT DEFAULT 'approved'
        )"""
    )
    conn.execute(
        "INSERT OR REPLACE INTO documents VALUES (?,?,?,?,?,?,?,?)",
        (doc_id, client, doc_type, date, amount, vendor, gl_account, review_status),
    )
    conn.commit()


def _insert_posting(conn, doc_id, status="posted"):
    """Insert a fake posting_jobs row."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS posting_jobs (
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id TEXT,
            posting_status TEXT,
            external_id TEXT DEFAULT '',
            created_at TEXT DEFAULT '2025-03-15',
            updated_at TEXT DEFAULT '2025-03-15'
        )"""
    )
    conn.execute(
        "INSERT INTO posting_jobs (document_id, posting_status) VALUES (?,?)",
        (doc_id, status),
    )
    conn.commit()


# ============================================================================
# CATEGORY 1: EVIDENCE SUFFICIENCY ATTACKS (1-15)
# ============================================================================

class TestEvidenceSufficiency:
    """Attack the evidence chain model to see if it catches incomplete audit
    evidence scenarios that a CPA auditor would flag."""

    # ------------------------------------------------------------------
    # 1. GL entry exists, no invoice support
    # ------------------------------------------------------------------
    def test_01_gl_entry_no_invoice(self, conn):
        """A GL posting with no supporting invoice should be flagged as 'missing'."""
        _insert_doc(conn, "DOC_GL_ONLY", doc_type="journal_entry")
        _insert_posting(conn, "DOC_GL_ONLY")
        ev = get_or_create_evidence(conn, "DOC_GL_ONLY", "invoice")
        assert ev["match_status"] == "missing", (
            "VULNERABILITY: GL entry without invoice support is NOT flagged as missing"
        )

    # ------------------------------------------------------------------
    # 2. Invoice exists, no payment evidence
    # ------------------------------------------------------------------
    def test_02_invoice_no_payment(self, conn):
        """Invoice evidence with no payment should remain 'missing' or 'partial'."""
        _insert_doc(conn, "DOC_INV_ONLY", doc_type="invoice")
        ev_inv = get_or_create_evidence(conn, "DOC_INV_ONLY", "invoice")
        # No payment evidence created -- status should not be 'complete'
        assert ev_inv["match_status"] != "complete", (
            "VULNERABILITY: Invoice without payment evidence is marked complete"
        )

    # ------------------------------------------------------------------
    # 3. Payment exists, no corresponding invoice
    # ------------------------------------------------------------------
    def test_03_payment_no_invoice(self, conn):
        """Payment with no invoice should be flagged."""
        _insert_doc(conn, "DOC_PAY_ONLY", doc_type="payment")
        ev = get_or_create_evidence(conn, "DOC_PAY_ONLY", "payment")
        assert ev["match_status"] != "complete", (
            "VULNERABILITY: Payment without invoice is marked complete"
        )

    # ------------------------------------------------------------------
    # 4. Bank evidence exists but business purpose unclear
    # ------------------------------------------------------------------
    def test_04_bank_evidence_no_purpose(self, conn):
        """Bank transaction with no description should not pass evidence check."""
        _insert_doc(conn, "DOC_BANK_NOPURPOSE", doc_type="bank_statement",
                    vendor="", amount=500.0)
        ev = get_or_create_evidence(conn, "DOC_BANK_NOPURPOSE", "payment")
        # Evidence model only tracks type linkage, not business purpose
        # This is a gap -- we document it
        has_purpose_check = ev.get("business_purpose_validated", False)
        assert not has_purpose_check, (
            "FINDING: Evidence model has no business purpose validation field. "
            "Bank transactions without clear business purpose get same treatment "
            "as fully documented ones."
        )

    # ------------------------------------------------------------------
    # 5. Existence supported, valuation NOT supported
    # ------------------------------------------------------------------
    def test_05_existence_without_valuation(self, conn, engagement):
        """Evidence model tracks type chains (PO/invoice/payment) but does
        NOT track audit assertions (existence, valuation, etc.)."""
        ev = get_or_create_evidence(conn, "DOC_EXISTS", "invoice")
        # The evidence table has no assertion-level tracking
        schema = conn.execute(
            "PRAGMA table_info(audit_evidence)"
        ).fetchall()
        col_names = {r["name"] for r in schema}
        has_assertion_columns = "assertion" in col_names or "assertions_covered" in col_names
        assert not has_assertion_columns, (
            "FINDING: audit_evidence table has NO assertion-level columns. "
            "An item can satisfy existence but not valuation and the system "
            "cannot distinguish this."
        )

    # ------------------------------------------------------------------
    # 6. Occurrence supported, completeness NOT supported
    # ------------------------------------------------------------------
    def test_06_occurrence_without_completeness(self, conn):
        """The evidence model does not distinguish occurrence vs completeness."""
        ev = get_or_create_evidence(conn, "DOC_OCCUR", "invoice")
        link_evidence_documents(conn, ev["evidence_id"], ["DOC_SUPPORT_1"])
        # Check: is there any assertion-aware logic?
        updated = conn.execute(
            "SELECT * FROM audit_evidence WHERE evidence_id = ?",
            (ev["evidence_id"],)
        ).fetchone()
        # The match_status is about document linkage, not assertion coverage
        assert updated["match_status"] in ("missing", "partial", "complete"), (
            "match_status only tracks document chain, not assertion coverage"
        )

    # ------------------------------------------------------------------
    # 7. Support document belongs to a different entity/client
    # ------------------------------------------------------------------
    def test_07_cross_entity_support(self, conn):
        """Link evidence from CLIENT_A to a document from CLIENT_B.
        System should reject or flag this, but likely does not."""
        _insert_doc(conn, "DOC_CLIENT_A", client="CLIENT_A")
        _insert_doc(conn, "DOC_CLIENT_B", client="CLIENT_B")
        ev = get_or_create_evidence(conn, "DOC_CLIENT_A", "invoice")
        # Link a document from a different client
        result = link_evidence_documents(
            conn, ev["evidence_id"], ["DOC_CLIENT_B"]
        )
        linked = json.loads(result["linked_document_ids"])
        cross_entity_accepted = "DOC_CLIENT_B" in linked
        assert cross_entity_accepted, (
            "VULNERABILITY: System accepts cross-entity evidence linkage without "
            "checking that linked documents belong to the same client."
        )

    # ------------------------------------------------------------------
    # 8. Altered support documents (different amounts)
    # ------------------------------------------------------------------
    def test_08_altered_support_docs(self, conn):
        """Invoice says $1000, but linked PO says $500.
        Evidence chain says 'partial' but does NOT flag the amount mismatch."""
        _insert_doc(conn, "DOC_INV_1K", doc_type="invoice", amount=1000.0)
        _insert_doc(conn, "DOC_PO_500", doc_type="purchase_order", amount=500.0)
        ev_inv = get_or_create_evidence(conn, "DOC_INV_1K", "invoice")
        ev_po = get_or_create_evidence(conn, "DOC_INV_1K", "purchase_order")
        link_evidence_documents(conn, ev_inv["evidence_id"], ["DOC_PO_500"])
        link_evidence_documents(conn, ev_po["evidence_id"], ["DOC_INV_1K"])
        # The system does not compare amounts between linked documents
        schema = conn.execute("PRAGMA table_info(audit_evidence)").fetchall()
        col_names = {r["name"] for r in schema}
        has_amount_validation = "amount_validated" in col_names or "amount_match" in col_names
        assert not has_amount_validation, (
            "VULNERABILITY: Evidence chain does NOT validate that amounts match "
            "between linked documents. Invoice=$1000, PO=$500 accepted silently."
        )

    # ------------------------------------------------------------------
    # 9. Duplicate support docs (same doc used for multiple entries)
    # ------------------------------------------------------------------
    def test_09_duplicate_support_docs(self, conn):
        """Same support document linked to two different evidence chains.
        System should flag reuse."""
        _insert_doc(conn, "DOC_ENTRY_1")
        _insert_doc(conn, "DOC_ENTRY_2")
        _insert_doc(conn, "DOC_SUPPORT_SHARED")
        ev1 = get_or_create_evidence(conn, "DOC_ENTRY_1", "invoice")
        ev2 = get_or_create_evidence(conn, "DOC_ENTRY_2", "invoice")
        link_evidence_documents(conn, ev1["evidence_id"], ["DOC_SUPPORT_SHARED"])
        link_evidence_documents(conn, ev2["evidence_id"], ["DOC_SUPPORT_SHARED"])
        # Both accepted -- no duplicate detection
        r1 = json.loads(conn.execute(
            "SELECT linked_document_ids FROM audit_evidence WHERE evidence_id=?",
            (ev1["evidence_id"],)
        ).fetchone()["linked_document_ids"])
        r2 = json.loads(conn.execute(
            "SELECT linked_document_ids FROM audit_evidence WHERE evidence_id=?",
            (ev2["evidence_id"],)
        ).fetchone()["linked_document_ids"])
        both_have_shared = "DOC_SUPPORT_SHARED" in r1 and "DOC_SUPPORT_SHARED" in r2
        assert both_have_shared, (
            "VULNERABILITY: Same support document linked to multiple evidence "
            "chains without any duplicate-use detection."
        )

    # ------------------------------------------------------------------
    # 10. Incomplete support chain (missing middle link)
    # ------------------------------------------------------------------
    def test_10_incomplete_chain(self, conn):
        """PO and payment exist but invoice is missing.
        Three-way match should be incomplete."""
        _insert_doc(conn, "DOC_PO_CHAIN", doc_type="purchase_order")
        _insert_doc(conn, "DOC_PAY_CHAIN", doc_type="payment")
        ev_po = get_or_create_evidence(conn, "DOC_PO_CHAIN", "purchase_order")
        ev_pay = get_or_create_evidence(conn, "DOC_PO_CHAIN", "payment")
        link_evidence_documents(conn, ev_po["evidence_id"], ["DOC_PAY_CHAIN"])
        link_evidence_documents(conn, ev_pay["evidence_id"], ["DOC_PO_CHAIN"])
        # No invoice evidence created -- should remain partial, not complete
        types_for_doc = {r["evidence_type"] for r in conn.execute(
            "SELECT evidence_type FROM audit_evidence WHERE document_id=?",
            ("DOC_PO_CHAIN",)
        ).fetchall()}
        required = {"purchase_order", "invoice", "payment"}
        assert not required.issubset(types_for_doc), (
            "Chain should be incomplete: PO + payment but no invoice"
        )

    # ------------------------------------------------------------------
    # 11. Related-party transaction with insufficient disclosure
    # ------------------------------------------------------------------
    def test_11_related_party_no_disclosure(self, conn, engagement):
        """Flag a related party transaction but set no disclosure details.
        System should warn about missing disclosure."""
        party_id = add_related_party(
            "RED_TEAM_INC", "Owner's Wife Corp",
            "family_member", conn, identified_by="auditor"
        )
        rpt_id = flag_related_party_transaction(
            engagement["engagement_id"], "DOC_RPT_1", party_id,
            "exchange_amount", conn,
            amount=50000.0, description="",  # empty description
        )
        rpts = get_related_party_transactions(engagement["engagement_id"], conn)
        # System accepts RPT with no description -- no disclosure check
        rpt = [r for r in rpts if r["rpt_id"] == rpt_id][0]
        assert rpt["description"] == "", (
            "VULNERABILITY: Related party transaction accepted with empty "
            "description. No disclosure sufficiency check."
        )
        assert rpt["audit_procedures_performed"] is None, (
            "VULNERABILITY: RPT has no audit procedures documented and "
            "system does not require them."
        )

    # ------------------------------------------------------------------
    # 12. Going concern indicators present but not escalated
    # ------------------------------------------------------------------
    def test_12_going_concern_not_escalated(self, conn, engagement):
        """There is no going concern module at all in the engine."""
        schema = conn.execute("PRAGMA table_info(engagements)").fetchall()
        col_names = {r["name"] for r in schema}
        has_gc = "going_concern_flag" in col_names or "going_concern" in col_names
        # Also check for a separate going_concern table
        tables = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        has_gc_table = "going_concern" in tables or "going_concern_indicators" in tables
        assert not has_gc and not has_gc_table, (
            "FINDING: No going concern tracking in the audit engine. "
            "CAS 570 requires assessment of going concern, but the system "
            "has no mechanism to flag or escalate going concern indicators."
        )

    # ------------------------------------------------------------------
    # 13. Conflict between source documents
    # ------------------------------------------------------------------
    def test_13_conflicting_amounts(self, conn):
        """Invoice=$1000, payment=$1050. System does not detect the $50 gap."""
        _insert_doc(conn, "DOC_INV_CONFLICT", doc_type="invoice", amount=1000.0)
        _insert_doc(conn, "DOC_PAY_CONFLICT", doc_type="payment", amount=1050.0)
        ev_inv = get_or_create_evidence(conn, "DOC_INV_CONFLICT", "invoice")
        ev_pay = get_or_create_evidence(conn, "DOC_INV_CONFLICT", "payment")
        link_evidence_documents(conn, ev_inv["evidence_id"], ["DOC_PAY_CONFLICT"])
        link_evidence_documents(conn, ev_pay["evidence_id"], ["DOC_INV_CONFLICT"])
        # No amount comparison is performed by the evidence linkage system
        # match_status only cares about type coverage
        types_present = {r["evidence_type"] for r in conn.execute(
            "SELECT evidence_type FROM audit_evidence WHERE document_id=?",
            ("DOC_INV_CONFLICT",)
        ).fetchall()}
        # If all three types existed, it would be "complete" regardless of amount conflict
        assert "invoice" in types_present and "payment" in types_present

    # ------------------------------------------------------------------
    # 14. Support for amount but not for tax treatment
    # ------------------------------------------------------------------
    def test_14_no_tax_treatment_validation(self, conn):
        """Evidence model has no concept of tax-specific assertions."""
        schema = conn.execute("PRAGMA table_info(audit_evidence)").fetchall()
        col_names = {r["name"] for r in schema}
        tax_cols = {"tax_treatment_verified", "gst_qst_validated", "tax_assertion"}
        has_tax = tax_cols & col_names
        assert not has_tax, (
            "FINDING: Evidence model has no tax treatment validation. "
            "A document can have correct amount but wrong GST/QST treatment "
            "and the system cannot track this at the evidence level."
        )

    # ------------------------------------------------------------------
    # 15. Support for payment but not for authorization
    # ------------------------------------------------------------------
    def test_15_no_authorization_tracking(self, conn):
        """Evidence chain does not validate payment authorization."""
        _insert_doc(conn, "DOC_UNAUTH_PAY", doc_type="payment", amount=50000.0)
        ev = get_or_create_evidence(conn, "DOC_UNAUTH_PAY", "payment")
        schema = conn.execute("PRAGMA table_info(audit_evidence)").fetchall()
        col_names = {r["name"] for r in schema}
        auth_cols = {"authorization_verified", "approved_by", "authorization_status"}
        has_auth = auth_cols & col_names
        assert not has_auth, (
            "VULNERABILITY: $50,000 payment evidence has no authorization "
            "tracking. System cannot distinguish authorized from unauthorized "
            "payments in the evidence chain."
        )


# ============================================================================
# CATEGORY 2: THREE-WAY MATCH ATTACKS (16-22)
# ============================================================================

class TestThreeWayMatch:
    """Attack the three-way match (PO -> Invoice -> Payment) logic."""

    # ------------------------------------------------------------------
    # 16. PO != Invoice within tolerance
    # ------------------------------------------------------------------
    def test_16_po_invoice_within_tolerance(self, conn):
        """PO=$1000, Invoice=$1002. Small variance -- should still link."""
        _insert_doc(conn, "PO_16", doc_type="purchase_order", amount=1000.0)
        _insert_doc(conn, "INV_16", doc_type="invoice", amount=1002.0)
        ev_po = get_or_create_evidence(conn, "PO_16", "purchase_order")
        ev_inv = get_or_create_evidence(conn, "PO_16", "invoice")
        link_evidence_documents(conn, ev_po["evidence_id"], ["INV_16"])
        # System links without checking amounts at all
        linked = json.loads(conn.execute(
            "SELECT linked_document_ids FROM audit_evidence WHERE evidence_id=?",
            (ev_po["evidence_id"],)
        ).fetchone()["linked_document_ids"])
        assert "INV_16" in linked, (
            "Link accepted (expected: no amount tolerance logic exists)"
        )

    # ------------------------------------------------------------------
    # 17. PO != Invoice outside tolerance
    # ------------------------------------------------------------------
    def test_17_po_invoice_outside_tolerance(self, conn):
        """PO=$1000, Invoice=$2000. 100% variance -- still links silently."""
        _insert_doc(conn, "PO_17", doc_type="purchase_order", amount=1000.0)
        _insert_doc(conn, "INV_17", doc_type="invoice", amount=2000.0)
        ev = get_or_create_evidence(conn, "PO_17", "purchase_order")
        link_evidence_documents(conn, ev["evidence_id"], ["INV_17"])
        linked = json.loads(conn.execute(
            "SELECT linked_document_ids FROM audit_evidence WHERE evidence_id=?",
            (ev["evidence_id"],)
        ).fetchone()["linked_document_ids"])
        assert "INV_17" in linked, (
            "VULNERABILITY: PO=$1000 linked to Invoice=$2000 with no "
            "amount variance check. 100% variance accepted silently."
        )

    # ------------------------------------------------------------------
    # 18. Invoice quantity != Delivery receipt quantity
    # ------------------------------------------------------------------
    def test_18_quantity_mismatch(self, conn):
        """Evidence model has no quantity field -- only amounts."""
        schema = conn.execute("PRAGMA table_info(audit_evidence)").fetchall()
        col_names = {r["name"] for r in schema}
        quantity_cols = {"quantity", "quantity_received", "quantity_invoiced"}
        has_qty = quantity_cols & col_names
        assert not has_qty, (
            "FINDING: Evidence model has no quantity tracking. "
            "Quantity mismatches between invoice and delivery receipt "
            "cannot be detected."
        )

    # ------------------------------------------------------------------
    # 19. PO vendor != Invoice vendor (name variation)
    # ------------------------------------------------------------------
    def test_19_vendor_name_mismatch(self, conn):
        """PO for 'ABC Inc.' linked to invoice from 'ABC Incorporated'.
        No vendor name validation in evidence chain."""
        _insert_doc(conn, "PO_19", doc_type="purchase_order", vendor="ABC Inc.")
        _insert_doc(conn, "INV_19", doc_type="invoice", vendor="XYZ Corp.")
        ev = get_or_create_evidence(conn, "PO_19", "purchase_order")
        result = link_evidence_documents(conn, ev["evidence_id"], ["INV_19"])
        linked = json.loads(result["linked_document_ids"])
        assert "INV_19" in linked, (
            "VULNERABILITY: PO from 'ABC Inc.' linked to invoice from "
            "'XYZ Corp.' -- completely different vendor accepted."
        )

    # ------------------------------------------------------------------
    # 20. Missing PO entirely
    # ------------------------------------------------------------------
    def test_20_missing_po(self, conn):
        """Invoice and payment exist but no PO. Should be partial."""
        _insert_doc(conn, "INV_20", doc_type="invoice")
        _insert_doc(conn, "PAY_20", doc_type="payment")
        ev_inv = get_or_create_evidence(conn, "INV_20", "invoice")
        ev_pay = get_or_create_evidence(conn, "INV_20", "payment")
        link_evidence_documents(conn, ev_inv["evidence_id"], ["PAY_20"])
        link_evidence_documents(conn, ev_pay["evidence_id"], ["INV_20"])
        # Without PO, the three-way match should NOT be complete
        types = {r["evidence_type"] for r in conn.execute(
            "SELECT evidence_type FROM audit_evidence WHERE document_id=?",
            ("INV_20",)
        ).fetchall()}
        assert "purchase_order" not in types, "No PO created as expected"
        # Check that match_status is NOT 'complete'
        ev_updated = conn.execute(
            "SELECT match_status FROM audit_evidence WHERE evidence_id=?",
            (ev_inv["evidence_id"],)
        ).fetchone()
        assert ev_updated["match_status"] != "complete", (
            "Without PO, evidence should not be complete"
        )

    # ------------------------------------------------------------------
    # 21. Missing delivery receipt
    # ------------------------------------------------------------------
    def test_21_missing_delivery_receipt(self, conn):
        """Evidence model does not support delivery receipts at all."""
        assert "delivery_receipt" not in VALID_EVIDENCE_TYPES, (
            "FINDING: VALID_EVIDENCE_TYPES = {purchase_order, invoice, payment}. "
            "Delivery receipts are NOT a valid evidence type. "
            "The system cannot track goods receipt separately from payment."
        )

    # ------------------------------------------------------------------
    # 22. PO date > Invoice date (chronologically impossible)
    # ------------------------------------------------------------------
    def test_22_chronological_impossibility(self, conn):
        """PO dated 2025-06-01, Invoice dated 2025-01-01.
        Invoice before PO -- system does not check chronology."""
        _insert_doc(conn, "PO_22", doc_type="purchase_order", date="2025-06-01")
        _insert_doc(conn, "INV_22", doc_type="invoice", date="2025-01-01")
        ev = get_or_create_evidence(conn, "PO_22", "purchase_order")
        link_evidence_documents(conn, ev["evidence_id"], ["INV_22"])
        linked = json.loads(conn.execute(
            "SELECT linked_document_ids FROM audit_evidence WHERE evidence_id=?",
            (ev["evidence_id"],)
        ).fetchone()["linked_document_ids"])
        assert "INV_22" in linked, (
            "VULNERABILITY: Invoice dated before PO (chronologically "
            "impossible) accepted without any date validation."
        )


# ============================================================================
# CATEGORY 3: RECONCILIATION ATTACKS (23-30)
# ============================================================================

class TestReconciliationAttacks:
    """Attack the bank reconciliation engine."""

    # ------------------------------------------------------------------
    # 23. Off by $0.01 (at tolerance boundary)
    # ------------------------------------------------------------------
    def test_23_off_by_one_cent(self, conn):
        """$0.01 difference should be within tolerance and marked balanced."""
        recon_id = create_reconciliation(
            "CLIENT_23", "Chequing", "2025-03-31",
            statement_balance=10000.00,
            gl_balance=10000.01,
            conn=conn,
        )
        result = calculate_reconciliation(recon_id, conn)
        assert result["is_balanced"], (
            "Reconciliation off by $0.01 should be within tolerance"
        )
        assert abs(result["difference"]) <= BALANCE_TOLERANCE

    # ------------------------------------------------------------------
    # 24. Off by $0.02 (outside tolerance?)
    # ------------------------------------------------------------------
    def test_24_off_by_two_cents(self, conn):
        """$0.02 difference should be outside the $0.01 tolerance."""
        recon_id = create_reconciliation(
            "CLIENT_24", "Chequing", "2025-03-31",
            statement_balance=10000.00,
            gl_balance=10000.02,
            conn=conn,
        )
        result = calculate_reconciliation(recon_id, conn)
        assert not result["is_balanced"], (
            "VULNERABILITY: $0.02 difference should NOT be balanced. "
            f"Tolerance is ${BALANCE_TOLERANCE}."
        )

    # ------------------------------------------------------------------
    # 25. Outstanding cheques older than 6 months (stale)
    # ------------------------------------------------------------------
    def test_25_stale_outstanding_cheques(self, conn):
        """Outstanding cheque from 8 months ago. System does not flag stale items."""
        recon_id = create_reconciliation(
            "CLIENT_25", "Chequing", "2025-03-31",
            statement_balance=10000.00, gl_balance=9500.00,
            conn=conn,
        )
        add_reconciliation_item(
            recon_id, "outstanding_cheque", "Stale cheque #1234",
            500.00, "2024-07-15", conn,  # 8+ months old
        )
        result = calculate_reconciliation(recon_id, conn)
        items = get_reconciliation_items(recon_id, conn)
        stale_items = [i for i in items if i["item_type"] == "outstanding_cheque"]
        # System has no age-based flagging
        for item in stale_items:
            has_stale_flag = item.get("stale", False) or item.get("is_stale", False)
            assert not has_stale_flag, (
                "FINDING: Outstanding cheque from 8 months ago is NOT flagged "
                "as stale. System has no age-based item classification."
            )

    # ------------------------------------------------------------------
    # 26. Deposit in transit not clearing after 30 days
    # ------------------------------------------------------------------
    def test_26_old_deposit_in_transit(self, conn):
        """Deposit in transit from 45 days ago. Should be investigated."""
        recon_id = create_reconciliation(
            "CLIENT_26", "Chequing", "2025-03-31",
            statement_balance=10000.00, gl_balance=10500.00,
            conn=conn,
        )
        add_reconciliation_item(
            recon_id, "deposit_in_transit", "Old deposit",
            500.00, "2025-02-14", conn,  # 45 days ago
        )
        items = get_reconciliation_items(recon_id, conn)
        dit = [i for i in items if i["item_type"] == "deposit_in_transit"]
        # No aging analysis
        schema = conn.execute("PRAGMA table_info(reconciliation_items)").fetchall()
        col_names = {r["name"] for r in schema}
        has_aging = "days_outstanding" in col_names or "aging_flag" in col_names
        assert not has_aging, (
            "FINDING: No aging analysis for deposits in transit. "
            "45-day-old deposit accepted without investigation flag."
        )

    # ------------------------------------------------------------------
    # 27. Duplicate reconciling items
    # ------------------------------------------------------------------
    def test_27_duplicate_reconciling_items(self, conn):
        """FIX P1-3: Same cheque added twice is now rejected."""
        from src.engines.reconciliation_engine import DuplicateItemError
        recon_id = create_reconciliation(
            "CLIENT_27", "Chequing", "2025-03-31",
            statement_balance=10000.00, gl_balance=9000.00,
            conn=conn,
        )
        add_reconciliation_item(
            recon_id, "outstanding_cheque", "Cheque #5555",
            500.00, "2025-03-20", conn,
        )
        with pytest.raises(DuplicateItemError):
            add_reconciliation_item(
                recon_id, "outstanding_cheque", "Cheque #5555",
                500.00, "2025-03-20", conn,
            )
        result = calculate_reconciliation(recon_id, conn)
        assert result["bank_side"]["outstanding_cheques"] == 500.0, (
            "Only one cheque should have been accepted"
        )

    # ------------------------------------------------------------------
    # 28. Negative outstanding cheque
    # ------------------------------------------------------------------
    def test_28_negative_outstanding_cheque(self, conn):
        """FIX P1-1: Negative outstanding cheque is now rejected."""
        from src.engines.reconciliation_engine import NegativeAmountError
        recon_id = create_reconciliation(
            "CLIENT_28", "Chequing", "2025-03-31",
            statement_balance=10000.00, gl_balance=10000.00,
            conn=conn,
        )
        with pytest.raises(NegativeAmountError):
            add_reconciliation_item(
                recon_id, "outstanding_cheque", "Negative cheque??",
                -500.00, "2025-03-20", conn,
            )

    # ------------------------------------------------------------------
    # 29. Reconciliation with zero items
    # ------------------------------------------------------------------
    def test_29_zero_items_reconciliation(self, conn):
        """Empty reconciliation where bank = GL exactly."""
        recon_id = create_reconciliation(
            "CLIENT_29", "Chequing", "2025-03-31",
            statement_balance=10000.00, gl_balance=10000.00,
            conn=conn,
        )
        result = calculate_reconciliation(recon_id, conn)
        assert result["is_balanced"], "Perfect reconciliation with no items"
        assert result["difference"] == 0.0

    # ------------------------------------------------------------------
    # 30. Bank statement date != reconciliation date
    # ------------------------------------------------------------------
    def test_30_date_mismatch(self, conn):
        """Reconciliation for March 31 but no validation of statement date."""
        recon_id = create_reconciliation(
            "CLIENT_30", "Chequing", "2025-03-31",
            statement_balance=10000.00, gl_balance=10000.00,
            conn=conn,
        )
        recon = get_reconciliation(recon_id, conn)
        # There is no field for bank_statement_date separate from period_end_date
        schema = conn.execute("PRAGMA table_info(bank_reconciliations)").fetchall()
        col_names = {r["name"] for r in schema}
        has_stmt_date = "bank_statement_date" in col_names
        assert not has_stmt_date, (
            "FINDING: No separate bank_statement_date field. "
            "Cannot detect when the bank statement date differs "
            "from the reconciliation period end date."
        )


# ============================================================================
# CATEGORY 4: CAS COVERAGE ATTACKS (31-35)
# ============================================================================

class TestCASCoverage:
    """Attack CAS compliance to verify whether it is genuine or cosmetic."""

    # ------------------------------------------------------------------
    # 31. Does CAS engine actually check specific standards?
    # ------------------------------------------------------------------
    def test_31_cas_standard_references(self, conn, engagement):
        """Check if CAS standards are referenced in output, not just names."""
        mat = calculate_materiality("revenue", 1000000)
        # The function calculates but does not reference CAS 320 in output
        assert "planning_materiality" in mat
        assert "performance_materiality" in mat
        assert "clearly_trivial" in mat
        # No CAS reference in the data
        cas_ref = mat.get("cas_reference") or mat.get("standard")
        assert cas_ref is None, (
            "FINDING: Materiality calculation has no CAS standard reference "
            "in its output. The function implements CAS 320 logic but does "
            "not tag the output with the standard number."
        )

    # ------------------------------------------------------------------
    # 32. Are assertions properly mapped to evidence types?
    # ------------------------------------------------------------------
    def test_32_assertion_evidence_mapping(self, conn, engagement):
        """Risk assessments track assertions. Evidence tracks types.
        There is no mapping between them."""
        # CAS assertions
        cas_assertions = VALID_ASSERTIONS
        # Evidence types
        evidence_types = VALID_EVIDENCE_TYPES
        # No mapping table or function connecting them
        tables = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        mapping_tables = {t for t in tables if "assertion" in t.lower() and "evidence" in t.lower()}
        assert not mapping_tables, (
            "FINDING: No assertion-to-evidence-type mapping. "
            f"Assertions={sorted(cas_assertions)}, "
            f"Evidence types={sorted(evidence_types)}. "
            "An auditor needs to know which evidence types satisfy which "
            "assertions, but this mapping does not exist."
        )

    # ------------------------------------------------------------------
    # 33. Are materiality calculations correct?
    # ------------------------------------------------------------------
    def test_33_materiality_math(self, conn):
        """Verify materiality rates against CAS 320 guidance."""
        for basis, rate in _MATERIALITY_RATES.items():
            mat = calculate_materiality(basis, 1_000_000)
            expected_planning = Decimal("1000000") * rate
            expected_performance = expected_planning * PERFORMANCE_RATE
            expected_trivial = expected_planning * CLEARLY_TRIVIAL_RATE
            assert mat["planning_materiality"] == expected_planning.quantize(Decimal("0.01")), (
                f"Planning materiality wrong for {basis}"
            )
            assert mat["performance_materiality"] == expected_performance.quantize(Decimal("0.01")), (
                f"Performance materiality wrong for {basis}"
            )
            assert mat["clearly_trivial"] == expected_trivial.quantize(Decimal("0.01")), (
                f"Clearly trivial wrong for {basis}"
            )

    def test_33b_materiality_zero_amount(self):
        """Materiality with zero basis should raise error."""
        with pytest.raises(ValueError, match="positive"):
            calculate_materiality("revenue", 0)

    def test_33c_materiality_negative(self):
        """Materiality with negative basis should raise error."""
        with pytest.raises(ValueError, match="positive"):
            calculate_materiality("revenue", -100000)

    def test_33d_materiality_invalid_basis(self):
        """Invalid basis type should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid basis_type"):
            calculate_materiality("made_up_basis", 100000)

    # ------------------------------------------------------------------
    # 34. Is sampling statistically valid?
    # ------------------------------------------------------------------
    def test_34_sampling_reproducibility(self, conn):
        """Verify sampling is reproducible (uses paper_id as seed)."""
        _insert_doc(conn, "SAMPLE_1", client="SAMPLE_CO", date="2025-01-10", amount=100)
        _insert_doc(conn, "SAMPLE_2", client="SAMPLE_CO", date="2025-01-20", amount=200)
        _insert_doc(conn, "SAMPLE_3", client="SAMPLE_CO", date="2025-01-30", amount=300)
        _insert_posting(conn, "SAMPLE_1")
        _insert_posting(conn, "SAMPLE_2")
        _insert_posting(conn, "SAMPLE_3")
        wp = get_or_create_working_paper(
            conn, "SAMPLE_CO", "2025", "audit", "5200", "Salaries"
        )
        s1 = get_sample(conn, "SAMPLE_CO", "2025", "", 2, wp["paper_id"])
        s2 = get_sample(conn, "SAMPLE_CO", "2025", "", 2, wp["paper_id"])
        ids1 = sorted([d["document_id"] for d in s1])
        ids2 = sorted([d["document_id"] for d in s2])
        assert ids1 == ids2, (
            "Sampling should be reproducible with same paper_id seed"
        )

    def test_34b_sampling_no_statistical_basis(self, conn):
        """Sampling uses random selection but has no statistical basis
        (no confidence level, no expected error rate, no population size calc)."""
        # The get_sample function just takes a sample_size parameter
        # with no statistical calculation
        import inspect
        source = inspect.getsource(get_sample)
        has_confidence = "confidence" in source.lower()
        has_error_rate = "error_rate" in source.lower() or "tolerable" in source.lower()
        has_population = "population_size" in source.lower()
        assert not has_confidence and not has_error_rate and not has_population, (
            "FINDING: Sampling has no statistical basis. No confidence level, "
            "no expected error rate, no population-based sample size calculation. "
            "The auditor just picks an arbitrary sample_size number."
        )

    # ------------------------------------------------------------------
    # 35. Do working papers reference specific CAS standards?
    # ------------------------------------------------------------------
    def test_35_working_papers_cas_references(self, conn):
        """Working papers should reference applicable CAS standards."""
        wp = get_or_create_working_paper(
            conn, "RED_TEAM_INC", "2025", "audit", "1010", "Cash"
        )
        schema = conn.execute("PRAGMA table_info(working_papers)").fetchall()
        col_names = {r["name"] for r in schema}
        cas_cols = {"cas_reference", "applicable_standards", "cas_standard"}
        has_cas = cas_cols & col_names
        assert not has_cas, (
            "FINDING: Working papers have no CAS standard reference field. "
            "CPA Quebec documentation standards require that each working paper "
            "references the applicable Canadian Auditing Standard."
        )

    def test_35b_engagement_completion_no_checklist(self, conn, engagement):
        """Engagement can be completed without verifying all required
        CAS procedures were performed."""
        # Get progress -- no papers created, should be 0%
        progress = get_engagement_progress(conn, engagement["engagement_id"])
        assert progress["pct"] == 0
        # Despite 0% completion, no guard prevents marking as complete
        # (update_engagement allows status changes freely)
        from src.engines.audit_engine import update_engagement
        updated = update_engagement(
            conn, engagement["engagement_id"], status="complete"
        )
        assert updated["status"] == "complete", (
            "VULNERABILITY: Engagement marked 'complete' with 0% working "
            "papers signed off. No CAS checklist prevents premature completion."
        )


# ============================================================================
# BONUS: Cross-module integration attacks
# ============================================================================

class TestCrossModuleGaps:
    """Test for gaps between modules that create false comfort."""

    def test_evidence_complete_but_recon_unbalanced(self, conn):
        """Evidence chain says 'complete' but bank reconciliation is off.
        No cross-check between the two."""
        _insert_doc(conn, "DOC_CROSS_1", doc_type="invoice", amount=5000)
        ev_inv = get_or_create_evidence(conn, "DOC_CROSS_1", "invoice")
        ev_po = get_or_create_evidence(conn, "DOC_CROSS_1", "purchase_order")
        ev_pay = get_or_create_evidence(conn, "DOC_CROSS_1", "payment")
        # Link to make complete
        link_evidence_documents(conn, ev_inv["evidence_id"], ["DOC_CROSS_1"])
        link_evidence_documents(conn, ev_po["evidence_id"], ["DOC_CROSS_1"])
        link_evidence_documents(conn, ev_pay["evidence_id"], ["DOC_CROSS_1"])
        # Meanwhile, reconciliation is off by $5000
        recon_id = create_reconciliation(
            "RED_TEAM_INC", "Chequing", "2025-03-31",
            statement_balance=100000.00, gl_balance=95000.00,
            conn=conn,
        )
        result = calculate_reconciliation(recon_id, conn)
        assert not result["is_balanced"], "Recon is off by $5000"
        # But evidence says complete -- no integration
        final_ev = conn.execute(
            "SELECT match_status FROM audit_evidence WHERE evidence_id=?",
            (ev_inv["evidence_id"],)
        ).fetchone()
        # Evidence can be "complete" while bank recon is unbalanced

    def test_risk_high_but_no_extended_procedures(self, conn, engagement):
        """High risk assessment but no requirement for extended audit procedures."""
        accounts = [{"account_code": "1010", "account_name": "Cash"}]
        risks = create_risk_matrix(
            conn, engagement["engagement_id"], accounts, assessed_by="auditor"
        )
        # Set all to high risk
        for risk in risks:
            assess_risk(conn, risk["risk_id"], inherent_risk="high", control_risk="high")
        summary = get_risk_summary(conn, engagement["engagement_id"])
        assert summary["high"] > 0 and summary["significant_risks"] > 0
        # But the system has no linkage from high risk to required procedures
        # No table or field tracks "risk response procedures"
        tables = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        risk_response_tables = {t for t in tables if "risk_response" in t.lower() or "audit_plan" in t.lower()}
        assert not risk_response_tables, (
            "FINDING: High/significant risk identified but no risk response "
            "mechanism. CAS 330 requires the auditor to design audit procedures "
            "responsive to assessed risks, but there is no linkage."
        )

    def test_control_ineffective_no_impact_on_risk(self, conn, engagement):
        """Control test shows 'ineffective' but risk assessment is not updated."""
        test_id = create_control_test(
            engagement["engagement_id"],
            "AP authorization",
            "Invoices approved before payment",
            "walkthrough",
            conn,
            tested_by="auditor",
        )
        record_test_results(
            test_id, items_tested=25, exceptions_found=10,
            exception_details="10 of 25 invoices had no approval signature",
            conclusion="ineffective", conn=conn,
        )
        # Control is ineffective, but risk assessments remain at default medium
        risks = get_risk_assessment(conn, engagement["engagement_id"])
        # If risks exist, check none were auto-updated
        # (They won't be because no auto-linkage exists)
        summary = get_control_effectiveness_summary(
            engagement["engagement_id"], conn
        )
        assert summary["ineffective"] >= 1
        # No automatic escalation to risk assessment
