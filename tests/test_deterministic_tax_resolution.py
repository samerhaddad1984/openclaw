"""
tests/test_deterministic_tax_resolution.py
==========================================
Comprehensive tests for the deterministic tax resolution engine:
- Part 1: Customs value calculation, import GST, QST on imports
- Part 2: Invoice reconciliation, FX reconciliation
- Part 3: Remote service supply determination
- Part 4: Registration overlap detection
- Part 5: Credit memo decomposition
- Part 6: Tax event timing
- Part 7: Apportionment enforcement
- Part 8: Unresolvability handling
"""
from __future__ import annotations

import sqlite3
from decimal import Decimal

import pytest

# ---------------------------------------------------------------------------
# Import modules under test
# ---------------------------------------------------------------------------

from src.engines.customs_engine import (
    calculate_customs_value,
    calculate_import_gst,
    calculate_qst_on_import,
    determine_remote_service_supply,
    detect_registration_overlap,
    decompose_credit_memo,
    create_tax_event,
    get_tax_events,
    update_tax_event_status,
    enforce_apportionment,
)
from src.engines.reconciliation_validator import (
    reconcile_invoice_total,
    reconcile_fx_conversion,
)
from src.engines.tax_engine import cannot_determine_response


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    """In-memory SQLite database with required tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id     TEXT PRIMARY KEY,
            vendor          TEXT,
            client_code     TEXT,
            amount          REAL,
            document_date   TEXT,
            tax_code        TEXT,
            gl_account      TEXT,
            review_status   TEXT DEFAULT 'New',
            review_reason   TEXT,
            gst_amount      REAL DEFAULT 0,
            qst_amount      REAL DEFAULT 0,
            hst_amount      REAL DEFAULT 0,
            updated_at      TEXT,
            has_line_items  INTEGER DEFAULT 0,
            deposit_allocated INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS invoice_lines (
            line_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id      TEXT NOT NULL,
            line_number      INTEGER NOT NULL,
            description      TEXT,
            quantity         REAL,
            unit_price       REAL,
            line_total_pretax REAL,
            tax_code         TEXT,
            tax_regime       TEXT,
            gst_amount       REAL,
            qst_amount       REAL,
            hst_amount       REAL,
            province_of_supply TEXT,
            is_tax_included  INTEGER,
            line_notes       TEXT,
            created_at       TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS audit_log (
            log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type      TEXT NOT NULL,
            document_id     TEXT,
            task_type       TEXT,
            prompt_snippet  TEXT,
            created_at      TEXT NOT NULL DEFAULT ''
        );
    """)
    yield conn
    conn.close()


# =========================================================================
# Part 1 — Customs value calculation
# =========================================================================

class TestCustomsValue:
    def test_no_discount(self):
        r = calculate_customs_value(
            invoice_amount=10000, discount=0, discount_type="flat",
            discount_shown_on_invoice=False,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert r["customs_value"] == Decimal("10000.00")
        assert r["discount_applied"] is False

    def test_unconditional_discount_on_invoice(self):
        r = calculate_customs_value(
            invoice_amount=10000, discount=500, discount_type="flat",
            discount_shown_on_invoice=True,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert r["customs_value"] == Decimal("9500.00")
        assert r["discount_applied"] is True
        assert r["discount_amount"] == Decimal("500.00")

    def test_conditional_discount_excluded(self):
        r = calculate_customs_value(
            invoice_amount=10000, discount=1000, discount_type="flat",
            discount_shown_on_invoice=True,
            discount_is_conditional=True,
            post_import_discount=False,
        )
        assert r["customs_value"] == Decimal("10000.00")
        assert r["discount_applied"] is False
        assert "conditional" in r["reasoning"].lower()

    def test_post_import_discount_excluded(self):
        r = calculate_customs_value(
            invoice_amount=10000, discount=500, discount_type="flat",
            discount_shown_on_invoice=True,
            discount_is_conditional=False,
            post_import_discount=True,
        )
        assert r["customs_value"] == Decimal("10000.00")
        assert r["discount_applied"] is False
        assert "post-import" in r["reasoning"].lower()

    def test_discount_not_on_invoice_excluded(self):
        r = calculate_customs_value(
            invoice_amount=10000, discount=500, discount_type="flat",
            discount_shown_on_invoice=False,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert r["customs_value"] == Decimal("10000.00")
        assert r["discount_applied"] is False

    def test_percentage_discount(self):
        r = calculate_customs_value(
            invoice_amount=10000, discount=10, discount_type="percentage",
            discount_shown_on_invoice=True,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert r["customs_value"] == Decimal("9000.00")
        assert r["discount_amount"] == Decimal("1000.00")

    def test_never_returns_depends(self):
        """Customs value must always be deterministic."""
        r = calculate_customs_value(
            invoice_amount=5000, discount=250, discount_type="flat",
            discount_shown_on_invoice=True,
            discount_is_conditional=True,
            post_import_discount=True,
        )
        assert "depends" not in str(r).lower()
        assert isinstance(r["customs_value"], Decimal)


# =========================================================================
# Part 1 — Import GST and QST on import base
# =========================================================================

class TestImportGstQst:
    def test_import_gst_calculation(self):
        r = calculate_import_gst(customs_value=10000, duties=500, excise_taxes=100)
        expected_base = Decimal("10600.00")
        assert r["gst_base"] == expected_base
        assert r["gst_amount"] == Decimal("530.00")  # 10600 * 0.05
        assert r["gst_recoverable_as_itc"] is True

    def test_qst_on_import_includes_gst(self):
        """QST base = customs_value + duties + gst_amount (NOT just invoice value)."""
        gst_amount = Decimal("530.00")
        r = calculate_qst_on_import(customs_value=10000, duties=500, gst_amount=gst_amount)
        expected_base = Decimal("10000") + Decimal("500") + gst_amount
        assert r["qst_base"] == expected_base
        expected_qst = (expected_base * Decimal("0.09975")).quantize(Decimal("0.01"))
        assert r["qst_amount"] == expected_qst
        assert r["qst_recoverable_as_itr"] is True

    def test_zero_duties(self):
        r = calculate_import_gst(customs_value=5000, duties=0, excise_taxes=0)
        assert r["gst_base"] == Decimal("5000.00")
        assert r["gst_amount"] == Decimal("250.00")

    def test_qst_base_differs_from_gst_base(self):
        """GST base = CV + duties + excise. QST base = CV + duties + GST amount.
        These are different because QST base includes GST, not excise taxes."""
        gst_result = calculate_import_gst(customs_value=10000, duties=500, excise_taxes=200)
        qst_result = calculate_qst_on_import(
            customs_value=10000, duties=500, gst_amount=gst_result["gst_amount"]
        )
        # GST base = 10700 (CV + duties + excise)
        assert gst_result["gst_base"] == Decimal("10700.00")
        # QST base = 10000 + 500 + 535 = 11035 (CV + duties + GST)
        assert qst_result["qst_base"] == Decimal("10000") + Decimal("500") + gst_result["gst_amount"]
        assert qst_result["qst_base"] != gst_result["gst_base"]


# =========================================================================
# Part 2 — Invoice reconciliation gaps
# =========================================================================

class TestInvoiceReconciliation:
    def test_exact_match(self):
        lines = [
            {"pretax_amount": 100, "gst": 5, "qst": 9.98, "hst": 0},
        ]
        r = reconcile_invoice_total(lines, 114.98, "CAD", 1.0)
        assert r["reconciled"] is True
        assert r["gap"] <= Decimal("0.02")

    def test_small_fx_rounding_acceptable(self):
        lines = [
            {"pretax_amount": 1000, "gst": 50, "qst": 0, "hst": 0},
        ]
        # Shown total differs by $0.50 due to FX rounding
        r = reconcile_invoice_total(lines, 1050.50, "USD", 1.0)
        assert r["reconciled"] is True
        expl_types = [e["type"] for e in r["gap_explanations"]]
        assert "fx_rounding" in expl_types

    def test_tax_ambiguity_flag(self):
        """Gap between $0.02 and $1.00 with CAD should flag tax ambiguity."""
        lines = [{"pretax_amount": 100, "gst": 5, "qst": 9.98, "hst": 0}]
        # Total is 114.98 computed, show 115.50 (gap = 0.52)
        r = reconcile_invoice_total(lines, 115.50, "CAD", 1.0)
        assert r["reconciled"] is True
        expl_types = [e["type"] for e in r["gap_explanations"]]
        assert "tax_inclusion_ambiguity" in expl_types

    def test_missing_line_items_flag(self):
        lines = [{"pretax_amount": 100, "gst": 5, "qst": 0, "hst": 0}]
        # Gap of $3.00
        r = reconcile_invoice_total(lines, 108, "CAD", 1.0)
        assert r["reconciled"] is True
        expl_types = [e["type"] for e in r["gap_explanations"]]
        assert "possible_missing_lines" in expl_types

    def test_large_unresolvable_gap_blocks_posting(self):
        lines = [{"pretax_amount": 100, "gst": 5, "qst": 0, "hst": 0}]
        # Gap of $100
        r = reconcile_invoice_total(lines, 205, "CAD", 1.0)
        assert r["reconciled"] is False
        assert r["block_posting"] is True
        expl_types = [e["type"] for e in r["gap_explanations"]]
        assert "UNRESOLVABLE_GAP" in expl_types

    def test_fx_conversion(self):
        lines = [{"pretax_amount": 1000, "gst": 65, "qst": 0, "hst": 0}]
        # 1000 USD * 1.30 = 1300 CAD + 65 tax = 1365
        r = reconcile_invoice_total(lines, 1365, "USD", 1.30)
        assert r["reconciled"] is True
        assert r["line_sum_cad"] == Decimal("1300.00")

    def test_vendor_markup(self):
        lines = [{"pretax_amount": 100, "gst": 5, "qst": 0, "hst": 0}]
        r = reconcile_invoice_total(lines, 130, "CAD", 1.0, vendor_markup=25)
        assert r["reconciled"] is True


# =========================================================================
# Part 2 — FX reconciliation
# =========================================================================

class TestFxReconciliation:
    def test_valid_conversion(self):
        r = reconcile_fx_conversion(
            original_amount=1000, original_currency="USD",
            cad_amount=1350, fx_rate=1.35, fx_date="2025-01-15",
        )
        assert r["reconciled"] is True
        assert "Bank of Canada" in r["fx_rate_source"]

    def test_gap_exceeds_tolerance(self):
        r = reconcile_fx_conversion(
            original_amount=1000, original_currency="USD",
            cad_amount=1400, fx_rate=1.35, fx_date="2025-01-15",
        )
        assert r["reconciled"] is False
        assert r["flag"] == "fx_reconciliation_gap"
        assert r["difference"] == Decimal("50.00")

    def test_within_tolerance(self):
        # 1000 * 1.35 = 1350, cad = 1354 → gap = 4/1354 ≈ 0.3% < 0.5%
        r = reconcile_fx_conversion(
            original_amount=1000, original_currency="USD",
            cad_amount=1354, fx_rate=1.35, fx_date="2025-01-15",
        )
        assert r["reconciled"] is True

    def test_invalid_fx_rate(self):
        r = reconcile_fx_conversion(
            original_amount=1000, original_currency="USD",
            cad_amount=1350, fx_rate=0, fx_date="2025-01-15",
        )
        assert r["reconciled"] is False
        assert r["flag"] == "invalid_fx_rate"


# =========================================================================
# Part 3 — Remote service supply determination
# =========================================================================

class TestRemoteServiceSupply:
    def test_recipient_in_qc(self):
        r = determine_remote_service_supply(
            service_type="consulting", vendor_location="ON",
            recipient_location="QC", benefit_location="QC",
            recipient_is_registered=True,
        )
        assert r["resolved"] is True
        assert r["gst_rate"] == Decimal("0.05")
        assert r["qst_rate"] == Decimal("0.09975")

    def test_recipient_in_on(self):
        r = determine_remote_service_supply(
            service_type="consulting", vendor_location="QC",
            recipient_location="ON", benefit_location="ON",
            recipient_is_registered=True,
        )
        assert r["resolved"] is True
        assert r["hst_rate"] == Decimal("0.13")

    def test_recipient_in_atlantic(self):
        for prov in ("NB", "NS", "NL", "PE"):
            r = determine_remote_service_supply(
                service_type="consulting", vendor_location="QC",
                recipient_location=prov, benefit_location=prov,
                recipient_is_registered=True,
            )
            assert r["resolved"] is True
            assert r["hst_rate"] == Decimal("0.15"), f"Failed for {prov}"

    def test_recipient_in_gst_only(self):
        for prov in ("AB", "NT", "NU", "YT"):
            r = determine_remote_service_supply(
                service_type="consulting", vendor_location="QC",
                recipient_location=prov, benefit_location=prov,
                recipient_is_registered=True,
            )
            assert r["resolved"] is True
            assert r["gst_rate"] == Decimal("0.05")
            assert r["qst_rate"] == Decimal("0")
            assert r["hst_rate"] == Decimal("0")

    def test_recipient_in_pst_province(self):
        r = determine_remote_service_supply(
            service_type="consulting", vendor_location="QC",
            recipient_location="BC", benefit_location="BC",
            recipient_is_registered=True,
        )
        assert r["resolved"] is True
        assert r["gst_rate"] == Decimal("0.05")
        assert r["pst_rate"] == Decimal("0.07")
        assert r["pst_recoverable"] is False

    def test_benefit_location_overrides_recipient(self):
        r = determine_remote_service_supply(
            service_type="consulting", vendor_location="QC",
            recipient_location="ON", benefit_location="QC",
            recipient_is_registered=True,
        )
        assert r["resolved"] is True
        assert r["effective_location"] == "QC"
        assert r["qst_rate"] == Decimal("0.09975")

    def test_unknown_location_flags_required(self):
        r = determine_remote_service_supply(
            service_type="consulting", vendor_location="QC",
            recipient_location="", benefit_location="",
            recipient_is_registered=True,
        )
        assert r["resolved"] is False
        assert r["flag"] == "SUPPLY_LOCATION_REQUIRED"
        assert r["block_posting"] is True
        assert len(r["information_needed"]) > 0

    def test_never_returns_ambiguous(self):
        """Must never return 'ambiguous' — either resolved or flagged."""
        r = determine_remote_service_supply(
            service_type="consulting", vendor_location="QC",
            recipient_location="XX", benefit_location="",
            recipient_is_registered=True,
        )
        assert "ambiguous" not in str(r).lower()
        assert r["resolved"] is False


# =========================================================================
# Part 4 — Registration overlap detection
# =========================================================================

class TestRegistrationOverlap:
    def test_document_before_registration(self, db):
        r = detect_registration_overlap(
            vendor_id="VENDOR-001",
            document_date="2024-06-15",
            vendor_registration_date="2024-09-01",
            prior_self_assessments=[],
            conn=db,
        )
        assert r["double_tax_risk"] is True
        assert "before vendor registration" in r["reasoning"][0].lower()

    def test_document_after_registration_no_risk(self, db):
        r = detect_registration_overlap(
            vendor_id="VENDOR-001",
            document_date="2025-01-15",
            vendor_registration_date="2024-09-01",
            prior_self_assessments=[],
            conn=db,
        )
        assert r["double_tax_risk"] is False

    def test_prior_self_assessment_reversal(self, db):
        prior = [
            {"vendor_id": "VENDOR-001", "gst_amount": 500, "qst_amount": 997.50},
        ]
        r = detect_registration_overlap(
            vendor_id="VENDOR-001",
            document_date="2024-06-15",
            vendor_registration_date="2024-09-01",
            prior_self_assessments=prior,
            conn=db,
        )
        assert r["reversal_required"] is True
        assert r["amount_to_reverse"] == Decimal("1497.50")

    def test_net_adjustment_calculation(self, db):
        # Insert a document with tax amounts
        db.execute(
            """INSERT INTO documents (document_id, vendor, document_date,
               gst_amount, qst_amount) VALUES (?, ?, ?, ?, ?)""",
            ("DOC-001", "VENDOR-001", "2024-06-15", 250, 498.75),
        )
        db.commit()

        prior = [
            {"vendor_id": "VENDOR-001", "gst_amount": 500, "qst_amount": 997.50},
        ]
        r = detect_registration_overlap(
            vendor_id="VENDOR-001",
            document_date="2024-06-15",
            vendor_registration_date="2024-09-01",
            prior_self_assessments=prior,
            conn=db,
        )
        assert r["prior_period_correction_required"] is True
        # vendor charged 748.75, reverse 1497.50 → net = -748.75
        assert r["net_adjustment"] == Decimal("-748.75")

    def test_missing_dates(self, db):
        r = detect_registration_overlap(
            vendor_id="VENDOR-001",
            document_date="",
            vendor_registration_date="",
            prior_self_assessments=[],
            conn=db,
        )
        assert r["double_tax_risk"] is False
        assert "missing" in r["reasoning"][0].lower()


# =========================================================================
# Part 5 — Credit memo decomposition
# =========================================================================

class TestCreditMemoDecomposition:
    def test_orphan_credit_memo(self, db):
        r = decompose_credit_memo(
            credit_memo_amount=-500,
            original_invoice_id="NONEXISTENT",
            conn=db,
        )
        assert r["decomposed"] is False
        assert r["flag"] == "orphan_credit_memo_undecomposable"
        assert r["manual_decomposition_required"] is True
        assert r["block_posting"] is True

    def test_decomposition_from_document_level(self, db):
        db.execute(
            """INSERT INTO documents (document_id, amount, tax_code,
               gst_amount, qst_amount, hst_amount)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("INV-001", 1000, "T", 50, 99.75, 0),
        )
        db.commit()

        r = decompose_credit_memo(
            credit_memo_amount=-200,
            original_invoice_id="INV-001",
            conn=db,
        )
        assert r["decomposed"] is True
        # 200/1000 = 0.2 ratio
        assert r["gst_portion_of_credit"] == Decimal("10.00")  # 50 * 0.2
        assert r["qst_portion_of_credit"] == Decimal("19.95")  # 99.75 * 0.2

    def test_full_line_decomposition(self, db):
        db.execute(
            "INSERT INTO documents (document_id, amount) VALUES (?, ?)",
            ("INV-002", 1000),
        )
        db.execute(
            """INSERT INTO invoice_lines
               (document_id, line_number, description, line_total_pretax,
                tax_code, tax_regime, gst_amount, qst_amount, hst_amount, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '')""",
            ("INV-002", 1, "Widget A", 600, "T", "GST_QST", 30, 59.85, 0),
        )
        db.execute(
            """INSERT INTO invoice_lines
               (document_id, line_number, description, line_total_pretax,
                tax_code, tax_regime, gst_amount, qst_amount, hst_amount, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '')""",
            ("INV-002", 2, "Widget B", 400, "T", "GST_QST", 20, 39.90, 0),
        )
        db.commit()

        r = decompose_credit_memo(
            credit_memo_amount=-100,
            original_invoice_id="INV-002",
            conn=db,
        )
        assert r["decomposed"] is True
        assert len(r["line_decomposition"]) == 2
        # Line 1 is 60% of total, Line 2 is 40%
        assert r["line_decomposition"][0]["description"] == "Widget A"

    def test_never_partial_decomposition(self, db):
        """Must return full decomposition or flag as orphan."""
        db.execute(
            "INSERT INTO documents (document_id, amount) VALUES (?, ?)",
            ("INV-003", 500),
        )
        db.commit()
        # No lines, but document exists with amount
        r = decompose_credit_memo(-100, "INV-003", db)
        assert r["decomposed"] is True or r.get("flag") == "orphan_credit_memo_undecomposable"


# =========================================================================
# Part 6 — Tax event timing
# =========================================================================

class TestTaxEventTiming:
    def test_create_and_retrieve_event(self, db):
        event_id = create_tax_event(
            document_id="DOC-100",
            event_type="import_gst",
            amount=530,
            tax_code="T",
            reporting_period="2025-Q1",
            incurrence_date="2025-01-15",
            claim_date="2025-01-15",
            conn=db,
        )
        assert event_id.startswith("TE-")

        events = get_tax_events("DOC-100", db)
        assert len(events) == 1
        assert events[0]["event_type"] == "import_gst"
        assert events[0]["amount"] == 530.0
        assert events[0]["timing_mismatch"] is False

    def test_timing_mismatch_detection(self, db):
        create_tax_event(
            document_id="DOC-101",
            event_type="vendor_invoice_gst",
            amount=250,
            tax_code="T",
            reporting_period="2025-Q1",
            incurrence_date="2025-01-15",
            claim_date="2025-03-31",
            conn=db,
        )
        events = get_tax_events("DOC-101", db)
        assert events[0]["timing_mismatch"] is True
        assert "incurred" in events[0]["timing_note"]

    def test_update_event_status(self, db):
        event_id = create_tax_event(
            document_id="DOC-102",
            event_type="self_assessed_gst",
            amount=100,
            tax_code="T",
            reporting_period="2025-Q1",
            incurrence_date="2025-02-01",
            claim_date="2025-02-01",
            conn=db,
        )
        assert update_tax_event_status(event_id, "claimed", db) is True
        events = get_tax_events("DOC-102", db)
        assert events[0]["status"] == "claimed"

    def test_invalid_status_rejected(self, db):
        event_id = create_tax_event(
            document_id="DOC-103",
            event_type="import_gst",
            amount=100,
            tax_code="T",
            reporting_period="2025-Q1",
            incurrence_date="2025-01-01",
            claim_date="2025-01-01",
            conn=db,
        )
        assert update_tax_event_status(event_id, "invalid_status", db) is False

    def test_multiple_events_per_document(self, db):
        for i, etype in enumerate(["import_gst", "import_qst", "duty"]):
            create_tax_event(
                document_id="DOC-104",
                event_type=etype,
                amount=100 * (i + 1),
                tax_code="T",
                reporting_period="2025-Q1",
                incurrence_date="2025-01-15",
                claim_date="2025-01-15",
                conn=db,
            )
        events = get_tax_events("DOC-104", db)
        assert len(events) == 3


# =========================================================================
# Part 7 — Apportionment enforcement
# =========================================================================

class TestApportionmentEnforcement:
    def test_apportionment_applied(self, db):
        db.execute(
            """INSERT INTO documents (document_id, gst_amount, qst_amount, hst_amount)
               VALUES (?, ?, ?, ?)""",
            ("DOC-200", 100, 199.50, 0),
        )
        db.commit()

        r = enforce_apportionment(
            document_id="DOC-200",
            apportionment_rate=Decimal("0.75"),
            apportionment_basis="75% business use per vehicle logbook",
            conn=db,
        )
        assert r["applied"] is True
        assert r["gross_itc"] == Decimal("100.00")
        assert r["net_itc"] == Decimal("75.00")
        assert r["gross_itr"] == Decimal("199.50")
        assert r["net_itr"] == Decimal("149.63")  # 199.50 * 0.75 rounded
        assert r["disallowed_itc"] == Decimal("25.00")

    def test_zero_rate_blocked(self, db):
        db.execute(
            "INSERT INTO documents (document_id, gst_amount) VALUES (?, ?)",
            ("DOC-201", 100),
        )
        db.commit()

        r = enforce_apportionment(
            document_id="DOC-201",
            apportionment_rate=0,
            apportionment_basis="",
            conn=db,
        )
        assert r["applied"] is False
        assert r["apportionment_required"] is True
        assert r["block_itc_itr_claim"] is True

    def test_rate_over_one_blocked(self, db):
        db.execute(
            "INSERT INTO documents (document_id, gst_amount) VALUES (?, ?)",
            ("DOC-202", 100),
        )
        db.commit()

        r = enforce_apportionment(
            document_id="DOC-202",
            apportionment_rate=1.5,
            apportionment_basis="invalid",
            conn=db,
        )
        assert r["applied"] is False
        assert r["block_itc_itr_claim"] is True

    def test_missing_document(self, db):
        r = enforce_apportionment(
            document_id="NONEXISTENT",
            apportionment_rate=Decimal("0.80"),
            apportionment_basis="test",
            conn=db,
        )
        assert r["applied"] is False

    def test_audit_log_created(self, db):
        db.execute(
            """INSERT INTO documents (document_id, gst_amount, qst_amount, hst_amount)
               VALUES (?, ?, ?, ?)""",
            ("DOC-203", 50, 99.75, 0),
        )
        db.commit()

        enforce_apportionment(
            document_id="DOC-203",
            apportionment_rate=Decimal("0.60"),
            apportionment_basis="60% business use",
            conn=db,
        )
        row = db.execute(
            "SELECT event_type FROM audit_log WHERE document_id = ?",
            ("DOC-203",),
        ).fetchone()
        assert row is not None
        assert row[0] == "apportionment_applied"

    def test_full_business_use(self, db):
        """Rate of exactly 1.0 should be accepted (100% business)."""
        db.execute(
            """INSERT INTO documents (document_id, gst_amount, qst_amount, hst_amount)
               VALUES (?, ?, ?, ?)""",
            ("DOC-204", 50, 99.75, 0),
        )
        db.commit()

        r = enforce_apportionment(
            document_id="DOC-204",
            apportionment_rate=Decimal("1.0"),
            apportionment_basis="100% business use",
            conn=db,
        )
        assert r["applied"] is True
        assert r["net_itc"] == r["gross_itc"]


# =========================================================================
# Part 8 — Unresolvability handling
# =========================================================================

class TestUnresolvability:
    def test_cannot_determine_response(self, db):
        db.execute(
            "INSERT INTO documents (document_id, review_status) VALUES (?, ?)",
            ("DOC-300", "New"),
        )
        db.commit()

        r = cannot_determine_response(
            reason="Cannot determine if service is performed in QC or ON",
            information_needed=[
                "Location where service is predominantly performed",
                "Recipient's business address",
            ],
            document_id="DOC-300",
            conn=db,
        )
        assert r["can_determine"] is False
        assert r["block_itc_itr"] is True
        assert r["review_status"] == "NeedsReview"
        assert len(r["information_needed"]) == 2
        assert "indéterminé" in r["display_message_fr"].lower()
        assert "cannot be determined" in r["display_message_en"].lower()

    def test_document_status_updated(self, db):
        db.execute(
            "INSERT INTO documents (document_id, review_status) VALUES (?, ?)",
            ("DOC-301", "New"),
        )
        db.commit()

        cannot_determine_response(
            reason="Unknown tax jurisdiction",
            information_needed=["Recipient province"],
            document_id="DOC-301",
            conn=db,
        )
        row = db.execute(
            "SELECT review_status FROM documents WHERE document_id = ?",
            ("DOC-301",),
        ).fetchone()
        assert row[0] == "NeedsReview"

    def test_no_assumptions_made(self, db):
        """The response must not contain any assumed values."""
        db.execute(
            "INSERT INTO documents (document_id) VALUES (?)", ("DOC-302",),
        )
        db.commit()

        r = cannot_determine_response(
            reason="Ambiguous vendor province",
            information_needed=["Vendor registration province"],
            document_id="DOC-302",
            conn=db,
        )
        # Must not contain any tax amount or rate
        assert "gst_amount" not in r
        assert "qst_amount" not in r
        assert "tax_rate" not in r


# =========================================================================
# Edge cases and integration
# =========================================================================

class TestEdgeCases:
    def test_customs_then_import_gst_then_qst_pipeline(self):
        """Full pipeline: customs value → import GST → QST on import."""
        cv = calculate_customs_value(
            invoice_amount=25000, discount=2500, discount_type="flat",
            discount_shown_on_invoice=True,
            discount_is_conditional=False,
            post_import_discount=False,
        )
        assert cv["customs_value"] == Decimal("22500.00")

        gst = calculate_import_gst(
            customs_value=cv["customs_value"],
            duties=1125, excise_taxes=0,
        )
        assert gst["gst_base"] == Decimal("23625.00")

        qst = calculate_qst_on_import(
            customs_value=cv["customs_value"],
            duties=1125,
            gst_amount=gst["gst_amount"],
        )
        # QST base = 22500 + 1125 + GST amount
        expected_qst_base = Decimal("22500") + Decimal("1125") + gst["gst_amount"]
        assert qst["qst_base"] == expected_qst_base

    def test_remote_service_all_provinces(self):
        """Every Canadian province/territory must resolve deterministically."""
        all_provs = [
            "QC", "ON", "NB", "NS", "NL", "PE",
            "AB", "NT", "NU", "YT", "BC", "MB", "SK",
        ]
        for prov in all_provs:
            r = determine_remote_service_supply(
                service_type="consulting",
                vendor_location="QC",
                recipient_location=prov,
                benefit_location=prov,
                recipient_is_registered=True,
            )
            assert r["resolved"] is True, f"Failed to resolve for {prov}"
            assert "ambiguous" not in str(r).lower(), f"Ambiguous for {prov}"

    def test_reconciliation_never_unreconciled_silently(self):
        """reconcile_invoice_total must always explain gaps."""
        lines = [{"pretax_amount": 100, "gst": 5, "qst": 0, "hst": 0}]
        for total in [105, 105.01, 110, 200, 500]:
            r = reconcile_invoice_total(lines, total, "CAD", 1.0)
            if not r["reconciled"]:
                assert r["block_posting"] is True
                assert len(r["gap_explanations"]) > 0
            else:
                assert len(r["gap_explanations"]) > 0
