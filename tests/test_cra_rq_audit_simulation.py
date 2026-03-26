"""
tests/test_cra_rq_audit_simulation.py — Regulator-level CRA & Revenu Quebec
audit simulation.

Scenario
--------
Client BSQ (Basement Systems Quebec) has three filed periods: March, April,
May 2025.  An Aqua Motion Systems equipment purchase spans all three periods
with deposit, main invoice, credit memo, Quebec subcontractor, CBSA import
document, activation log, and an owner note introducing personal-use
uncertainty.

The test exercises every engine seam:
  - amendment_engine  (Traps 1, 4, 9)
  - correction_chain  (Traps 2, 3, 5, 8)
  - uncertainty_engine (structured reasons, blocking, exposure)
  - tax_engine         (HST, GST, QST calculations)
  - reconciliation_validator (gap classification)

Regulator positions
-------------------
CRA:
  - Imported goods GST comes from CBSA, not invoice HST.
  - Invoice HST is unreliable; no ITC from it.
  - Subscription not claimable in April (activation June 8).
  - March deposit ITC premature without goods received.

RQ:
  - Quebec commissioning implies local QST treatment.
  - Subcontractor overlap with original vendor.
  - Owner personal-use note blocks full ITR recovery.
  - Allocation uncertainty makes full claim indefensible.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Engine imports
# ---------------------------------------------------------------------------
from src.engines.tax_engine import (
    GST_RATE,
    QST_RATE,
    calculate_gst_qst,
    calculate_itc_itr,
)
from src.engines.amendment_engine import (
    flag_amendment_needed,
    get_amendment_timeline,
    get_open_amendment_flags,
    is_period_filed,
    resolve_amendment_flag,
    snapshot_document,
    take_filing_snapshot,
    update_recognition_period,
    validate_recognition_timing,
    build_period_correction_entry,
)
from src.engines.correction_chain import (
    apply_single_correction,
    build_correction_chain_link,
    cluster_documents,
    decompose_credit_memo_safe,
    detect_overlap_anomaly,
    get_cluster_for_document,
    get_full_correction_chain,
    is_duplicate_of_cluster_head,
)
from src.engines.uncertainty_engine import (
    BLOCK_PENDING_REVIEW,
    PARTIAL_POST_WITH_FLAGS,
    SAFE_TO_POST,
    DateResolutionState,
    UncertaintyReason,
    UncertaintyState,
    build_date_resolution,
    evaluate_posting_readiness,
    evaluate_uncertainty,
    reason_credit_memo_tax_split_unproven,
    reason_customs_note_scope_limited,
    reason_date_ambiguous,
    reason_filed_period_amendment,
    reason_recognition_timing_deferred,
    reason_subcontractor_overlap,
    reason_prior_treatment_contradiction,
)

# ---------------------------------------------------------------------------
# Constants for the scenario
# ---------------------------------------------------------------------------
CLIENT = "BSQ"
VENDOR_AMS = "Aqua Motion Systems Ltd."
VENDOR_AMS_REMIT = "AMS Industrial"
VENDOR_HTL = "Hydro Techniques Laval inc."

FX_RATE_USD_CAD = Decimal("1.36")  # representative March-April 2025

# March deposit
DEPOSIT_INV = "AMS-7781-D"
DEPOSIT_USD = Decimal("10000")
DEPOSIT_CAD = (DEPOSIT_USD * FX_RATE_USD_CAD).quantize(Decimal("0.01"))
DEPOSIT_HST_CAD = Decimal("1300")  # HST shown as CAD equivalent on deposit

# April main invoice
MAIN_INV = "AMS-7781"
MAIN_USD = Decimal("31500")
MAIN_CAD = (MAIN_USD * FX_RATE_USD_CAD).quantize(Decimal("0.01"))
MAIN_HST = Decimal("3950")

# April invoice line breakdown (USD)
LINES = [
    {"desc": "Imported pump hardware",        "usd": Decimal("18000"), "tax_treatment": "CBSA_GST"},
    {"desc": "Quebec commissioning",          "usd": Decimal("5500"),  "tax_treatment": "QC_SERVICE"},
    {"desc": "Remote monitoring subscription", "usd": Decimal("3600"),  "tax_treatment": "DEFERRED"},
    {"desc": "Freight FOB Shanghai",          "usd": Decimal("2800"),  "tax_treatment": "CBSA_GST"},
    {"desc": "Environmental compliance fee",  "usd": Decimal("1800"),  "tax_treatment": "EXEMPT"},
    {"desc": "Volume discount",               "usd": Decimal("-200"),  "tax_treatment": "DISCOUNT"},
]

# May/June evidence
CREDIT_MEMO_CAD = Decimal("8050")  # tax-included, no breakdown
SUBCONTRACTOR_PRETAX = Decimal("5000")
SUBCONTRACTOR_GST = (SUBCONTRACTOR_PRETAX * GST_RATE).quantize(Decimal("0.01"))
SUBCONTRACTOR_QST = (SUBCONTRACTOR_PRETAX * QST_RATE).quantize(Decimal("0.01"))

CBSA_GOODS_VALUE_CAD = Decimal("28288")  # (18000+2800)*1.36
CBSA_GST = (CBSA_GOODS_VALUE_CAD * GST_RATE).quantize(Decimal("0.01"))

SUBSCRIPTION_ACTIVATION = "2025-06-08"


# ---------------------------------------------------------------------------
# Database fixture
# ---------------------------------------------------------------------------

def _create_schema(conn: sqlite3.Connection) -> None:
    """Build all tables required by the engines under test."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id        TEXT PRIMARY KEY,
            client_code        TEXT,
            vendor             TEXT,
            invoice_number     TEXT,
            document_date      TEXT,
            amount             REAL,
            subtotal           REAL,
            tax_total          REAL,
            tax_code           TEXT,
            gl_account         TEXT,
            review_status      TEXT DEFAULT 'pending',
            memo               TEXT DEFAULT '',
            raw_ocr_text       TEXT DEFAULT '',
            currency           TEXT DEFAULT 'CAD',
            fx_rate            REAL DEFAULT 1.0,
            activation_date    TEXT DEFAULT '',
            recognition_period TEXT DEFAULT '',
            recognition_status TEXT DEFAULT 'immediate',
            personal_use_pct   REAL DEFAULT NULL,
            regulator_source   TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS invoice_lines (
            line_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id        TEXT,
            description        TEXT,
            line_total_pretax  REAL,
            gst_amount         REAL DEFAULT 0,
            qst_amount         REAL DEFAULT 0,
            tax_treatment      TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS posting_jobs (
            rowid              INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id        TEXT,
            posting_id         TEXT DEFAULT '',
            posting_status     TEXT DEFAULT 'pending',
            external_id        TEXT DEFAULT '',
            created_at         TEXT,
            updated_at         TEXT
        );
        CREATE TABLE IF NOT EXISTS gst_filings (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code        TEXT,
            period_label       TEXT,
            deadline           TEXT,
            filed_at           TEXT,
            filed_by           TEXT,
            UNIQUE(client_code, period_label)
        );
        CREATE TABLE IF NOT EXISTS amendment_flags (
            flag_id                INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code            TEXT NOT NULL,
            filed_period           TEXT NOT NULL,
            trigger_document_id    TEXT NOT NULL,
            trigger_type           TEXT DEFAULT 'credit_memo',
            reason_en              TEXT,
            reason_fr              TEXT,
            original_filing_id     TEXT,
            status                 TEXT DEFAULT 'open',
            resolved_by            TEXT,
            resolved_at            TEXT,
            amendment_filing_id    TEXT,
            created_at             TEXT,
            updated_at             TEXT,
            UNIQUE(client_code, filed_period, trigger_document_id)
        );
        CREATE TABLE IF NOT EXISTS document_snapshots (
            snapshot_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id      TEXT NOT NULL,
            snapshot_type    TEXT DEFAULT 'filing',
            snapshot_reason  TEXT,
            state_json       TEXT,
            taken_by         TEXT DEFAULT 'system',
            taken_at         TEXT
        );
        CREATE TABLE IF NOT EXISTS posting_snapshots (
            snapshot_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            posting_id       TEXT DEFAULT '',
            document_id      TEXT NOT NULL,
            snapshot_type    TEXT DEFAULT 'filing',
            snapshot_reason  TEXT,
            state_json       TEXT,
            taken_by         TEXT DEFAULT 'system',
            taken_at         TEXT
        );
        CREATE TABLE IF NOT EXISTS correction_chains (
            chain_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            chain_root_id       TEXT NOT NULL,
            client_code         TEXT NOT NULL,
            source_document_id  TEXT NOT NULL,
            target_document_id  TEXT NOT NULL,
            link_type           TEXT DEFAULT 'credit_memo',
            economic_effect     TEXT DEFAULT 'reduction',
            amount              REAL,
            tax_impact_gst      REAL,
            tax_impact_qst      REAL,
            uncertainty_flags   TEXT DEFAULT '[]',
            status              TEXT DEFAULT 'active',
            created_by          TEXT,
            created_at          TEXT,
            superseded_by       INTEGER,
            rollback_of         INTEGER
        );
        CREATE TABLE IF NOT EXISTS rollback_log (
            rollback_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code        TEXT NOT NULL,
            target_type        TEXT DEFAULT 'correction_chain',
            target_id          TEXT NOT NULL,
            rollback_reason    TEXT,
            rolled_back_by     TEXT,
            state_before_json  TEXT,
            state_after_json   TEXT,
            is_reimport_blocked INTEGER DEFAULT 0,
            created_at         TEXT
        );
        CREATE TABLE IF NOT EXISTS document_clusters (
            cluster_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_key      TEXT UNIQUE,
            client_code      TEXT,
            cluster_head_id  TEXT,
            member_count     INTEGER,
            status           TEXT DEFAULT 'active',
            created_at       TEXT,
            updated_at       TEXT
        );
        CREATE TABLE IF NOT EXISTS document_cluster_members (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_id       INTEGER,
            document_id      TEXT NOT NULL,
            is_cluster_head  INTEGER DEFAULT 0,
            similarity_score REAL,
            variant_notes    TEXT,
            added_at         TEXT,
            UNIQUE(cluster_id, document_id),
            FOREIGN KEY (cluster_id) REFERENCES document_clusters(cluster_id)
        );
        CREATE TABLE IF NOT EXISTS overlap_anomalies (
            anomaly_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code      TEXT,
            document_a_id    TEXT,
            document_b_id    TEXT,
            vendor_a         TEXT,
            vendor_b         TEXT,
            overlap_type     TEXT,
            overlap_description TEXT,
            status           TEXT DEFAULT 'open',
            created_at       TEXT,
            UNIQUE(document_a_id, document_b_id)
        );
        CREATE TABLE IF NOT EXISTS audit_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type      TEXT,
            document_id     TEXT,
            prompt_snippet  TEXT,
            created_at      TEXT
        );
    """)


def _seed_scenario(conn: sqlite3.Connection) -> None:
    """Insert the full AMS scenario: deposit, main invoice, evidence docs."""
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    # -- March deposit invoice --
    conn.execute("""
        INSERT INTO documents (document_id, client_code, vendor, invoice_number,
            document_date, amount, subtotal, tax_total, tax_code, gl_account,
            review_status, memo, currency, fx_rate)
        VALUES ('DOC-MAR-DEP', ?, ?, ?, '2025-03-26', ?, ?, ?, 'HST',
                '1500 Equipment', 'approved',
                'Deposit for pump system AMS-7781-D',
                'USD', ?)
    """, (CLIENT, VENDOR_AMS, DEPOSIT_INV,
          float(DEPOSIT_CAD + DEPOSIT_HST_CAD),
          float(DEPOSIT_CAD), float(DEPOSIT_HST_CAD), float(FX_RATE_USD_CAD)))

    conn.execute("""
        INSERT INTO posting_jobs (document_id, posting_status, created_at)
        VALUES ('DOC-MAR-DEP', 'posted', '2025-03-27')
    """)

    # -- April main invoice --
    conn.execute("""
        INSERT INTO documents (document_id, client_code, vendor, invoice_number,
            document_date, amount, subtotal, tax_total, tax_code, gl_account,
            review_status, memo, raw_ocr_text, currency, fx_rate)
        VALUES ('DOC-APR-MAIN', ?, ?, ?, '2025-04-05', ?, ?, ?, 'HST',
                '1500 Equipment', 'approved',
                'Main invoice pump system with commissioning and monitoring',
                'AMS-7781 commissioning mise en service monitoring subscription hardware pump imported',
                'USD', ?)
    """, (CLIENT, VENDOR_AMS_REMIT, MAIN_INV,
          float(MAIN_CAD + MAIN_HST), float(MAIN_CAD), float(MAIN_HST),
          float(FX_RATE_USD_CAD)))

    conn.execute("""
        INSERT INTO posting_jobs (document_id, posting_status, created_at)
        VALUES ('DOC-APR-MAIN', 'posted', '2025-04-08')
    """)

    # Insert line items for the main invoice
    for line in LINES:
        cad = float((line["usd"] * FX_RATE_USD_CAD).quantize(Decimal("0.01")))
        conn.execute("""
            INSERT INTO invoice_lines (document_id, description,
                line_total_pretax, tax_treatment)
            VALUES ('DOC-APR-MAIN', ?, ?, ?)
        """, (line["desc"], cad, line["tax_treatment"]))

    # -- May credit memo (tax-included, no breakdown) --
    conn.execute("""
        INSERT INTO documents (document_id, client_code, vendor, invoice_number,
            document_date, amount, subtotal, tax_total, tax_code, gl_account,
            review_status, memo, currency, fx_rate)
        VALUES ('DOC-MAY-CM', ?, ?, 'CM-AMS-7781', '2025-05-15', ?, NULL, NULL,
                'GENERIC_TAX', '1500 Equipment', 'pending',
                'Subscription removed, commissioning rebilled separately, freight adjusted',
                'CAD', 1.0)
    """, (CLIENT, VENDOR_AMS_REMIT, float(-CREDIT_MEMO_CAD)))

    # -- May Quebec subcontractor invoice --
    sub_total = float(SUBCONTRACTOR_PRETAX + SUBCONTRACTOR_GST + SUBCONTRACTOR_QST)
    conn.execute("""
        INSERT INTO documents (document_id, client_code, vendor, invoice_number,
            document_date, amount, subtotal, tax_total, tax_code, gl_account,
            review_status, memo, raw_ocr_text, currency, fx_rate)
        VALUES ('DOC-MAY-SUB', ?, ?, 'HTL-2025-042', '2025-05-20', ?, ?, ?,
                'T', '5300 Subcontractors', 'approved',
                'Mise en service finale - commissioning',
                'commissioning mise en service installation startup démarrage',
                'CAD', 1.0)
    """, (CLIENT, VENDOR_HTL, sub_total,
          float(SUBCONTRACTOR_PRETAX),
          float(SUBCONTRACTOR_GST + SUBCONTRACTOR_QST)))

    conn.execute("""
        INSERT INTO posting_jobs (document_id, posting_status, created_at)
        VALUES ('DOC-MAY-SUB', 'posted', '2025-05-22')
    """)

    # -- May CBSA customs document --
    conn.execute("""
        INSERT INTO documents (document_id, client_code, vendor, invoice_number,
            document_date, amount, subtotal, tax_total, tax_code, gl_account,
            review_status, memo, regulator_source, currency, fx_rate)
        VALUES ('DOC-MAY-CBSA', ?, 'CBSA / ASFC', 'CBSA-2025-IMP-4491',
                '2025-05-10', ?, ?, ?, 'GST_ONLY',
                '2200 Import Duties', 'approved',
                'GST on imported goods only — hardware and freight',
                'CBSA', 'CAD', 1.0)
    """, (CLIENT, float(CBSA_GOODS_VALUE_CAD + CBSA_GST),
          float(CBSA_GOODS_VALUE_CAD), float(CBSA_GST)))

    # -- Subscription monitoring activation log (June 8) --
    conn.execute("""
        INSERT INTO documents (document_id, client_code, vendor, invoice_number,
            document_date, amount, subtotal, tax_total, tax_code, gl_account,
            review_status, memo, activation_date, recognition_status, currency)
        VALUES ('DOC-APR-SUB-MON', ?, ?, 'AMS-MON-2025', '2025-04-05',
                ?, ?, 0, 'HST', '5400 Monitoring', 'approved',
                'Remote monitoring subscription - activation June 8',
                '2025-06-08', 'deferred', 'USD')
    """, (CLIENT, VENDOR_AMS,
          float(Decimal("3600") * FX_RATE_USD_CAD),
          float(Decimal("3600") * FX_RATE_USD_CAD)))

    # -- Owner note (personal use, no percentage) --
    conn.execute("""
        INSERT INTO documents (document_id, client_code, vendor, invoice_number,
            document_date, amount, tax_code, gl_account, review_status, memo)
        VALUES ('DOC-OWNER-NOTE', ?, 'Owner', 'NOTE-2025-06',
                '2025-06-01', 0, 'NONE', '0000 Notes', 'info',
                'Part of system tested at cottage before final install. No percentage given.')
    """, (CLIENT,))

    # -- OCR variant documents for clustering (Trap 5) --
    for variant_id, inv_num in [("DOC-APR-VAR1", "AMS7781"),
                                 ("DOC-APR-VAR2", "AMS-77B1")]:
        conn.execute("""
            INSERT INTO documents (document_id, client_code, vendor,
                invoice_number, document_date, amount, subtotal, tax_total,
                tax_code, gl_account, review_status, memo, currency)
            VALUES (?, ?, ?, ?, '2025-04-05', ?, ?, ?, 'HST',
                    '1500 Equipment', 'duplicate_candidate',
                    'OCR variant of AMS-7781', 'USD')
        """, (variant_id, CLIENT, VENDOR_AMS_REMIT, inv_num,
              float(MAIN_CAD + MAIN_HST), float(MAIN_CAD), float(MAIN_HST)))

    # -- File March, April, May periods --
    for period, filed_at in [("2025-03", "2025-04-30"),
                              ("2025-04", "2025-05-31"),
                              ("2025-05", "2025-06-30")]:
        conn.execute("""
            INSERT INTO gst_filings (client_code, period_label, deadline,
                filed_at, filed_by)
            VALUES (?, ?, ?, ?, 'bookkeeper')
        """, (CLIENT, period, filed_at, filed_at))

    conn.commit()


@pytest.fixture
def db() -> sqlite3.Connection:
    """In-memory database fully seeded with the AMS audit scenario."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _create_schema(conn)
    _seed_scenario(conn)
    return conn


# =========================================================================
# 1. Filed period preservation
# =========================================================================

class TestFiledPeriodPreservation:
    """Original filed states for March, April, May must remain intact."""

    def test_march_is_filed(self, db):
        assert is_period_filed(db, CLIENT, "2025-03")

    def test_april_is_filed(self, db):
        assert is_period_filed(db, CLIENT, "2025-04")

    def test_may_is_filed(self, db):
        assert is_period_filed(db, CLIENT, "2025-05")

    def test_filing_snapshot_captures_march_deposit(self, db):
        result = take_filing_snapshot(db, CLIENT, "2025-03", filed_by="auditor")
        assert result["snapshot_count"] >= 1

    def test_filing_snapshot_captures_april_main(self, db):
        result = take_filing_snapshot(db, CLIENT, "2025-04", filed_by="auditor")
        assert result["snapshot_count"] >= 1

    def test_march_deposit_not_mutated_after_snapshot(self, db):
        take_filing_snapshot(db, CLIENT, "2025-03", filed_by="auditor")
        doc = db.execute(
            "SELECT * FROM documents WHERE document_id = 'DOC-MAR-DEP'"
        ).fetchone()
        assert doc["review_status"] == "approved"
        assert float(doc["tax_total"]) == float(DEPOSIT_HST_CAD)

    def test_april_main_not_mutated_after_snapshot(self, db):
        take_filing_snapshot(db, CLIENT, "2025-04", filed_by="auditor")
        doc = db.execute(
            "SELECT * FROM documents WHERE document_id = 'DOC-APR-MAIN'"
        ).fetchone()
        assert doc["review_status"] == "approved"
        assert doc["tax_code"] == "HST"


# =========================================================================
# 2. Period-specific amendment/reassessment records
# =========================================================================

class TestAmendmentRecords:
    """CRA/RQ corrections produce amendment flags, not silent rewrites."""

    def test_credit_memo_flags_april_amendment(self, db):
        flag_amendment_needed(
            db,
            client_code=CLIENT,
            filed_period="2025-04",
            trigger_document_id="DOC-MAY-CM",
            trigger_type="credit_memo",
            reason_en="Credit memo received May contradicts April filing",
            reason_fr="Note de credit recu en mai contredit la declaration d'avril",
        )
        flags = get_open_amendment_flags(db, CLIENT, "2025-04")
        assert len(flags) >= 1
        assert flags[0]["trigger_document_id"] == "DOC-MAY-CM"

    def test_cbsa_evidence_flags_april_amendment(self, db):
        flag_amendment_needed(
            db,
            client_code=CLIENT,
            filed_period="2025-04",
            trigger_document_id="DOC-MAY-CBSA",
            trigger_type="new_evidence",
            reason_en="CBSA document shows GST on goods only — April HST-based ITC invalid",
            reason_fr="Document ASFC montre TPS sur biens seulement — CTI base HST d'avril invalide",
        )
        flags = get_open_amendment_flags(db, CLIENT, "2025-04")
        assert any(f["trigger_document_id"] == "DOC-MAY-CBSA" for f in flags)

    def test_march_deposit_flag_raised_separately(self, db):
        flag_amendment_needed(
            db,
            client_code=CLIENT,
            filed_period="2025-03",
            trigger_document_id="DOC-MAY-CBSA",
            trigger_type="new_evidence",
            reason_en="Deposit ITC premature — goods not received in March",
            reason_fr="CTI sur depot premature — biens non recus en mars",
        )
        flags = get_open_amendment_flags(db, CLIENT, "2025-03")
        assert len(flags) >= 1

    def test_amendment_flag_resolution_tracked(self, db):
        flag_amendment_needed(
            db, client_code=CLIENT, filed_period="2025-04",
            trigger_document_id="DOC-MAY-CM", trigger_type="credit_memo",
            reason_en="Credit memo", reason_fr="Note de credit",
        )
        resolve_amendment_flag(
            db, client_code=CLIENT, filed_period="2025-04",
            trigger_document_id="DOC-MAY-CM",
            resolved_by="tax_partner",
            amendment_filing_id="AMEND-2025-04-001",
            resolution="amended",
        )
        flags = get_open_amendment_flags(db, CLIENT, "2025-04")
        assert not any(f["trigger_document_id"] == "DOC-MAY-CM" for f in flags)


# =========================================================================
# 3. CRA and RQ proposed adjustments kept separate
# =========================================================================

class TestSeparateRegulatorAdjustments:
    """CRA (federal) and RQ (provincial) positions must be independently
    tracked without merging or netting."""

    def test_cra_adjustment_cbsa_gst_on_goods(self, db):
        """CRA position: GST comes from CBSA, not invoice HST."""
        cra_adj = {
            "regulator": "CRA",
            "period": "2025-04",
            "adjustment_type": "itc_disallowance",
            "original_itc_claimed": float(MAIN_HST),
            "revised_itc_allowed": float(CBSA_GST),
            "disallowed": float(MAIN_HST - CBSA_GST),
            "reason": "Invoice HST unreliable; use CBSA GST on imported goods only",
            "evidence": "DOC-MAY-CBSA",
        }
        assert cra_adj["regulator"] == "CRA"
        assert cra_adj["disallowed"] == float(MAIN_HST - CBSA_GST)
        assert cra_adj["disallowed"] > 0

    def test_rq_adjustment_qst_on_quebec_service(self, db):
        """RQ position: Quebec commissioning subject to QST, not HST."""
        commissioning_cad = float(
            (Decimal("5500") * FX_RATE_USD_CAD).quantize(Decimal("0.01"))
        )
        rq_adj = {
            "regulator": "RQ",
            "period": "2025-04",
            "adjustment_type": "itr_reclassification",
            "component": "Quebec commissioning",
            "original_tax_treatment": "HST",
            "revised_tax_treatment": "GST_QST",
            "qst_on_commissioning": float(
                (Decimal(str(commissioning_cad)) * QST_RATE).quantize(Decimal("0.01"))
            ),
            "reason": "Service performed in Quebec subject to QST, not HST",
        }
        assert rq_adj["regulator"] == "RQ"
        assert rq_adj["revised_tax_treatment"] != rq_adj["original_tax_treatment"]
        assert rq_adj["qst_on_commissioning"] > 0

    def test_cra_and_rq_not_merged(self, db):
        """Ensure we can hold both regulator adjustments without netting."""
        cra_disallowed = float(MAIN_HST - CBSA_GST)
        rq_personal_block = True  # full ITR blocked

        # They must be independently addressable
        adjustments = [
            {"regulator": "CRA", "amount": cra_disallowed},
            {"regulator": "RQ", "blocked": rq_personal_block},
        ]
        cra_items = [a for a in adjustments if a["regulator"] == "CRA"]
        rq_items = [a for a in adjustments if a["regulator"] == "RQ"]
        assert len(cra_items) == 1
        assert len(rq_items) == 1
        assert cra_items[0]["amount"] != 0


# =========================================================================
# 4. Contradiction timeline from later evidence
# =========================================================================

class TestContradictionTimeline:
    """Later evidence must build a traceable contradiction chain."""

    def test_cbsa_contradicts_april_hst_treatment(self, db):
        """CBSA shows GST on goods only — contradicts April all-HST treatment."""
        flag_amendment_needed(
            db, client_code=CLIENT, filed_period="2025-04",
            trigger_document_id="DOC-MAY-CBSA",
            trigger_type="new_evidence",
            reason_en="CBSA GST on goods contradicts April HST ITC",
            reason_fr="TPS ASFC sur biens contredit CTI TVH avril",
        )
        timeline = get_amendment_timeline(db, CLIENT, "2025-04")
        assert "amendment_flags" in timeline
        assert len(timeline["amendment_flags"]) >= 1

    def test_credit_memo_contradicts_line_allocation(self, db):
        """Credit memo removes subscription + adjusts freight — contradicts
        April line-level allocation."""
        decomp = decompose_credit_memo_safe(
            db,
            credit_memo_id="DOC-MAY-CM",
            credit_memo_amount_tax_included=float(CREDIT_MEMO_CAD),
            original_invoice_id="DOC-APR-MAIN",
            has_tax_breakdown=False,
            memo_text="subscription removed, commissioning rebilled, freight adjusted",
        )
        assert decomp["confidence"] < 0.90
        assert len(decomp["uncertainty_flags"]) > 0

    def test_subcontractor_contradicts_original_commissioning(self, db):
        """Hydro Techniques Laval commissioning overlaps with AMS commissioning."""
        anomalies = detect_overlap_anomaly(
            db,
            new_document_id="DOC-MAY-SUB",
            client_code=CLIENT,
        )
        assert len(anomalies) >= 1
        keywords = []
        for a in anomalies:
            keywords.extend(a.get("shared_keywords", []))
        assert any("commissioning" in kw or "mise en service" in kw
                    for kw in keywords)

    def test_activation_log_contradicts_april_recognition(self, db):
        """Monitoring activation June 8 contradicts April recognition."""
        result = validate_recognition_timing(db, "DOC-APR-SUB-MON")
        assert len(result["issues"]) >= 1
        assert any("deferred" in i.get("issue", "") for i in result["issues"])

    def test_full_timeline_has_multiple_contradiction_events(self, db):
        """After all evidence, April timeline shows multiple contradictions."""
        # Raise all flags
        for doc_id, ttype, reason in [
            ("DOC-MAY-CM", "credit_memo", "Credit memo adjustments"),
            ("DOC-MAY-CBSA", "new_evidence", "CBSA import GST"),
            ("DOC-MAY-SUB", "new_evidence", "Subcontractor overlap"),
        ]:
            flag_amendment_needed(
                db, client_code=CLIENT, filed_period="2025-04",
                trigger_document_id=doc_id, trigger_type=ttype,
                reason_en=reason, reason_fr=reason,
            )
        flags = get_open_amendment_flags(db, CLIENT, "2025-04")
        assert len(flags) >= 3


# =========================================================================
# 5. No double-unwinding of deposit tax
# =========================================================================

class TestNoDoubleUnwinding:
    """March deposit ITC and April main invoice ITC must not both be
    reversed for the same goods portion."""

    def test_deposit_correction_scoped_to_march(self, db):
        """Deposit correction only targets March, not April."""
        result = build_period_correction_entry(
            db,
            original_document_id="DOC-MAR-DEP",
            correction_document_id="DOC-MAY-CBSA",
            client_code=CLIENT,
            correction_period="2025-05",
            correction_amount=float(DEPOSIT_HST_CAD),
            correction_gst=float(DEPOSIT_HST_CAD),
            correction_qst=0,
            reason_en="Deposit ITC premature — reverse March HST claim",
        )
        assert result["original_period"] == "2025-03"
        assert result["correction_period"] == "2025-05"
        assert result["amendment_flag_raised"] is True

    def test_deposit_and_main_invoice_separate_chains(self, db):
        """Correction chains for deposit and main are separate roots."""
        build_correction_chain_link(
            db,
            chain_root_id="DOC-MAR-DEP",
            client_code=CLIENT,
            source_document_id="DOC-MAR-DEP",
            target_document_id="DOC-MAY-CBSA",
            link_type="new_evidence",
            economic_effect="reduction",
            amount=float(DEPOSIT_HST_CAD),
        )
        build_correction_chain_link(
            db,
            chain_root_id="DOC-APR-MAIN",
            client_code=CLIENT,
            source_document_id="DOC-APR-MAIN",
            target_document_id="DOC-MAY-CM",
            link_type="credit_memo",
            economic_effect="reduction",
            amount=float(CREDIT_MEMO_CAD),
        )
        dep_chain = get_full_correction_chain(db, "DOC-MAR-DEP")
        main_chain = get_full_correction_chain(db, "DOC-APR-MAIN")
        assert dep_chain["chain_root_id"] != main_chain["chain_root_id"]

    def test_total_gst_reversal_does_not_exceed_total_claimed(self, db):
        """Sum of all GST reversals must not exceed total GST originally claimed."""
        total_original_gst = float(DEPOSIT_HST_CAD + MAIN_HST)
        # Deposit reversal + main invoice adjustment
        deposit_reversal = float(DEPOSIT_HST_CAD)
        main_reversal = float(MAIN_HST - CBSA_GST)
        total_reversal = deposit_reversal + main_reversal
        assert total_reversal <= total_original_gst


# =========================================================================
# 6. Subscription deferred to June without mutating April
# =========================================================================

class TestSubscriptionDeferral:
    """Monitoring subscription must be deferred to June per activation log."""

    def test_activation_date_is_june(self, db):
        doc = db.execute(
            "SELECT activation_date FROM documents WHERE document_id = 'DOC-APR-SUB-MON'"
        ).fetchone()
        assert doc["activation_date"] == "2025-06-08"

    def test_recognition_status_is_deferred(self, db):
        doc = db.execute(
            "SELECT recognition_status FROM documents WHERE document_id = 'DOC-APR-SUB-MON'"
        ).fetchone()
        assert doc["recognition_status"] == "deferred"

    def test_update_recognition_creates_snapshot_not_rewrite(self, db):
        """update_recognition_period snapshots, does not erase April date."""
        result = update_recognition_period(
            db, "DOC-APR-SUB-MON", SUBSCRIPTION_ACTIVATION,
            updated_by="cra_auditor",
        )
        assert result["recognition_period"] == "2025-06"
        assert result["recognition_status"] == "deferred"
        # The document_date still says 2025-04-05 (not mutated)
        doc = db.execute(
            "SELECT document_date FROM documents WHERE document_id = 'DOC-APR-SUB-MON'"
        ).fetchone()
        assert doc["document_date"] == "2025-04-05"

    def test_april_amendment_flag_raised_for_deferred_subscription(self, db):
        """Deferring subscription recognition raises April amendment flag."""
        result = update_recognition_period(
            db, "DOC-APR-SUB-MON", SUBSCRIPTION_ACTIVATION,
            updated_by="cra_auditor",
        )
        assert result.get("prior_period_impact") is True
        assert result.get("amendment_needed") is True

    def test_uncertainty_engine_marks_deferred(self):
        """Uncertainty engine produces RECOGNITION_TIMING_DEFERRED reason."""
        reason = reason_recognition_timing_deferred(
            document_date="2025-04-05",
            activation_date=SUBSCRIPTION_ACTIVATION,
        )
        assert reason.reason_code == "RECOGNITION_TIMING_DEFERRED"
        assert "2025-06-08" in reason.description_en


# =========================================================================
# 7. Personal use blocks exact recoverability
# =========================================================================

class TestPersonalUseBlocking:
    """Owner note about cottage testing blocks full ITC/ITR recovery
    when no percentage is specified."""

    def test_personal_use_pct_is_null(self, db):
        """Owner note does not specify a percentage — field is NULL."""
        doc = db.execute(
            "SELECT personal_use_pct FROM documents WHERE document_id = 'DOC-OWNER-NOTE'"
        ).fetchone()
        assert doc["personal_use_pct"] is None

    def test_unspecified_personal_use_blocks_full_recovery(self):
        """Without a percentage, full ITC/ITR cannot be justified."""
        personal_use_pct = None  # owner didn't specify
        if personal_use_pct is None:
            max_recoverable_fraction = None  # unknown — block exact claim
        else:
            max_recoverable_fraction = 1.0 - personal_use_pct

        assert max_recoverable_fraction is None

    def test_uncertainty_engine_blocks_posting(self):
        """Uncertainty engine must block when personal use is indeterminate."""
        reasons = [
            UncertaintyReason(
                reason_code="ALLOCATION_GAP_UNEXPLAINED",
                description_en="Personal use portion unknown — owner note has no percentage",
                description_fr="Portion usage personnel inconnue — note du proprio sans pourcentage",
                evidence_available="Owner note: cottage testing before install",
                evidence_needed="Appraisal or declaration of personal use percentage",
            ),
        ]
        state = evaluate_uncertainty(
            {"personal_use_allocation": 0.30},  # below 0.60 threshold
            reasons=reasons,
        )
        assert state.must_block is True
        assert state.posting_recommendation == BLOCK_PENDING_REVIEW

    def test_rq_position_partial_itr_blocked(self):
        """RQ cannot accept full ITR when personal use is acknowledged
        but unquantified."""
        rq_position = {
            "regulator": "RQ",
            "issue": "personal_use_itr",
            "full_itr_allowed": False,
            "reason": "Owner acknowledges personal use but provides no allocation",
            "exposure_category": "MEDIUM",
            "exposure_range_cad": (500, 5000),
        }
        assert rq_position["full_itr_allowed"] is False
        assert isinstance(rq_position["exposure_range_cad"], tuple)
        assert rq_position["exposure_range_cad"][0] < rq_position["exposure_range_cad"][1]


# =========================================================================
# 8. Subcontractor overlap flagging
# =========================================================================

class TestSubcontractorOverlap:
    """Hydro Techniques Laval commissioning overlaps with AMS commissioning."""

    def test_overlap_detected_between_ams_and_htl(self, db):
        anomalies = detect_overlap_anomaly(
            db, new_document_id="DOC-MAY-SUB", client_code=CLIENT,
        )
        assert len(anomalies) >= 1

    def test_overlap_vendors_are_different(self, db):
        anomalies = detect_overlap_anomaly(
            db, new_document_id="DOC-MAY-SUB", client_code=CLIENT,
        )
        for a in anomalies:
            assert a["vendor_a"].lower() != a["vendor_b"].lower()

    def test_overlap_has_commissioning_keyword(self, db):
        anomalies = detect_overlap_anomaly(
            db, new_document_id="DOC-MAY-SUB", client_code=CLIENT,
        )
        all_kw = []
        for a in anomalies:
            all_kw.extend(a.get("shared_keywords", []))
        commissioning_found = any(
            kw in ("commissioning", "mise en service", "commissionnement",
                   "startup", "start-up", "démarrage")
            for kw in all_kw
        )
        assert commissioning_found

    def test_uncertainty_reason_for_overlap(self):
        reason = reason_subcontractor_overlap(
            vendor_a=VENDOR_AMS,
            vendor_b=VENDOR_HTL,
            keywords="commissioning, mise en service",
        )
        assert reason.reason_code == "SUBCONTRACTOR_WORK_SCOPE_OVERLAP"
        assert VENDOR_AMS in reason.description_en
        assert VENDOR_HTL in reason.description_en


# =========================================================================
# 9. Exposure ranges / categories instead of fake exact penalties
# =========================================================================

class TestExposureRanges:
    """System must give ranges and categories, never fabricated exact
    penalty amounts."""

    def test_cra_exposure_is_range_not_exact(self):
        cra_exposure = {
            "category": "HIGH",
            "tax_at_risk_low": float(CBSA_GST * Decimal("0.8")),
            "tax_at_risk_high": float(MAIN_HST),
            "interest_estimate_range": (200, 800),
            "penalty_category": "gross_negligence_possible",
            "notes": "Exact penalty depends on CRA assessment; "
                     "ranges reflect minimum (CBSA GST accepted) "
                     "to maximum (all ITC denied) scenarios.",
        }
        assert cra_exposure["tax_at_risk_low"] < cra_exposure["tax_at_risk_high"]
        assert isinstance(cra_exposure["interest_estimate_range"], tuple)
        # Never a single fake exact number
        assert cra_exposure["tax_at_risk_low"] != cra_exposure["tax_at_risk_high"]

    def test_rq_exposure_is_range_not_exact(self):
        rq_exposure = {
            "category": "MEDIUM",
            "itr_at_risk_low": 0,  # if personal use is 100%
            "itr_at_risk_high": float(SUBCONTRACTOR_QST),
            "penalty_category": "allocation_dispute",
            "notes": "ITR recovery disputed due to unquantified personal use.",
        }
        assert rq_exposure["itr_at_risk_low"] <= rq_exposure["itr_at_risk_high"]

    def test_exposure_categories_valid(self):
        valid_categories = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
        for cat in ["HIGH", "MEDIUM"]:
            assert cat in valid_categories

    def test_no_fabricated_exact_penalty(self):
        """The system must never output a single 'penalty = $X' number."""
        # Build an audit summary — it should contain ranges
        summary = _build_audit_exposure_summary()
        for item in summary:
            assert "range" in item or "category" in item
            assert "exact_penalty" not in item


def _build_audit_exposure_summary() -> list[dict]:
    """Helper: build exposure summary with ranges, not exact amounts."""
    return [
        {
            "regulator": "CRA",
            "issue": "HST ITC overclaim",
            "category": "HIGH",
            "range": (float(MAIN_HST - CBSA_GST), float(MAIN_HST)),
        },
        {
            "regulator": "CRA",
            "issue": "March deposit ITC premature",
            "category": "MEDIUM",
            "range": (0, float(DEPOSIT_HST_CAD)),
        },
        {
            "regulator": "CRA",
            "issue": "Subscription recognition timing",
            "category": "LOW",
            "range": (0, float(Decimal("3600") * FX_RATE_USD_CAD * Decimal("0.13"))),
        },
        {
            "regulator": "RQ",
            "issue": "Personal use ITR block",
            "category": "MEDIUM",
            "range": (0, float(SUBCONTRACTOR_QST + Decimal("500"))),
        },
        {
            "regulator": "RQ",
            "issue": "Commissioning QST reclassification",
            "category": "LOW",
            "range": (0, float(
                (Decimal("5500") * FX_RATE_USD_CAD * QST_RATE).quantize(Decimal("0.01"))
            )),
        },
    ]


# =========================================================================
# 10. Duplicate evidence clustering (Trap 5)
# =========================================================================

class TestDuplicateClustering:
    """OCR variants AMS7781, AMS-77B1, AMS-7781 must cluster once."""

    def test_cluster_three_variants(self, db):
        result = cluster_documents(
            db,
            ["DOC-APR-MAIN", "DOC-APR-VAR1", "DOC-APR-VAR2"],
            client_code=CLIENT,
            reason="ocr_variant_detection",
        )
        assert result["member_count"] == 3
        assert result["cluster_head_id"] == "DOC-APR-MAIN"

    def test_variant_is_duplicate_of_head(self, db):
        cluster_documents(
            db,
            ["DOC-APR-MAIN", "DOC-APR-VAR1", "DOC-APR-VAR2"],
            client_code=CLIENT,
        )
        assert is_duplicate_of_cluster_head(db, "DOC-APR-VAR1") is True
        assert is_duplicate_of_cluster_head(db, "DOC-APR-VAR2") is True
        assert is_duplicate_of_cluster_head(db, "DOC-APR-MAIN") is False

    def test_duplicate_does_not_create_correction(self, db):
        """Non-head cluster member must not create its own correction chain."""
        cluster_documents(
            db,
            ["DOC-APR-MAIN", "DOC-APR-VAR1", "DOC-APR-VAR2"],
            client_code=CLIENT,
        )
        result = apply_single_correction(
            db,
            credit_memo_id="DOC-APR-VAR1",
            original_invoice_id="DOC-MAR-DEP",
            client_code=CLIENT,
            decomposition={"pretax": 100, "gst": 5, "qst": 9.98},
        )
        assert result["status"] == "skipped_duplicate"

    def test_cluster_head_can_create_correction(self, db):
        cluster_documents(
            db,
            ["DOC-APR-MAIN", "DOC-APR-VAR1", "DOC-APR-VAR2"],
            client_code=CLIENT,
        )
        result = apply_single_correction(
            db,
            credit_memo_id="DOC-MAY-CM",
            original_invoice_id="DOC-APR-MAIN",
            client_code=CLIENT,
            decomposition={"pretax": 7000, "gst": 350, "qst": 700},
        )
        assert result["status"] == "created"

    def test_reclustering_is_idempotent(self, db):
        """Clustering the same docs twice does not create duplicate clusters."""
        r1 = cluster_documents(
            db, ["DOC-APR-MAIN", "DOC-APR-VAR1", "DOC-APR-VAR2"],
            client_code=CLIENT,
        )
        r2 = cluster_documents(
            db, ["DOC-APR-MAIN", "DOC-APR-VAR1", "DOC-APR-VAR2"],
            client_code=CLIENT,
        )
        assert r1["cluster_id"] == r2["cluster_id"]


# =========================================================================
# 11. Regulator-ready chronology and audit-defense packet
# =========================================================================

class TestAuditDefensePacket:
    """System must produce a structured chronology and defense packet."""

    def _build_full_scenario(self, db) -> dict:
        """Run all evidence processing and return structured output."""
        # 1. Take filing snapshots
        for period in ("2025-03", "2025-04", "2025-05"):
            take_filing_snapshot(db, CLIENT, period, filed_by="bookkeeper")

        # 2. Raise amendment flags
        flag_amendment_needed(
            db, client_code=CLIENT, filed_period="2025-04",
            trigger_document_id="DOC-MAY-CM", trigger_type="credit_memo",
            reason_en="Credit memo received", reason_fr="Note de credit",
        )
        flag_amendment_needed(
            db, client_code=CLIENT, filed_period="2025-04",
            trigger_document_id="DOC-MAY-CBSA", trigger_type="new_evidence",
            reason_en="CBSA import GST", reason_fr="TPS ASFC",
        )
        flag_amendment_needed(
            db, client_code=CLIENT, filed_period="2025-03",
            trigger_document_id="DOC-MAY-CBSA", trigger_type="new_evidence",
            reason_en="Deposit ITC premature", reason_fr="CTI depot premature",
        )

        # 3. Cluster duplicates
        cluster_documents(
            db, ["DOC-APR-MAIN", "DOC-APR-VAR1", "DOC-APR-VAR2"],
            client_code=CLIENT,
        )

        # 4. Decompose credit memo
        decomp = decompose_credit_memo_safe(
            db,
            credit_memo_id="DOC-MAY-CM",
            credit_memo_amount_tax_included=float(CREDIT_MEMO_CAD),
            original_invoice_id="DOC-APR-MAIN",
            has_tax_breakdown=False,
            memo_text="subscription removed, commissioning rebilled, freight adjusted",
        )

        # 5. Detect subcontractor overlap
        overlaps = detect_overlap_anomaly(
            db, new_document_id="DOC-MAY-SUB", client_code=CLIENT,
        )

        # 6. Defer subscription
        recog = update_recognition_period(
            db, "DOC-APR-SUB-MON", SUBSCRIPTION_ACTIVATION,
            updated_by="auditor",
        )

        # 7. Build timelines
        march_tl = get_amendment_timeline(db, CLIENT, "2025-03")
        april_tl = get_amendment_timeline(db, CLIENT, "2025-04")

        # 8. Build exposure summary
        exposure = _build_audit_exposure_summary()

        return {
            "chronology": {
                "march_timeline": march_tl,
                "april_timeline": april_tl,
            },
            "credit_memo_decomposition": decomp,
            "subcontractor_overlaps": overlaps,
            "subscription_deferral": recog,
            "exposure_summary": exposure,
            "amendment_flags": {
                "march": get_open_amendment_flags(db, CLIENT, "2025-03"),
                "april": get_open_amendment_flags(db, CLIENT, "2025-04"),
            },
        }

    def test_packet_has_chronology(self, db):
        packet = self._build_full_scenario(db)
        assert "chronology" in packet
        assert "march_timeline" in packet["chronology"]
        assert "april_timeline" in packet["chronology"]

    def test_chronology_has_filing_snapshots(self, db):
        packet = self._build_full_scenario(db)
        april_tl = packet["chronology"]["april_timeline"]
        assert "filing_snapshots" in april_tl
        assert len(april_tl["filing_snapshots"]) >= 1

    def test_chronology_has_amendment_flags(self, db):
        packet = self._build_full_scenario(db)
        april_tl = packet["chronology"]["april_timeline"]
        assert "amendment_flags" in april_tl
        assert len(april_tl["amendment_flags"]) >= 2

    def test_packet_has_credit_memo_decomposition(self, db):
        packet = self._build_full_scenario(db)
        decomp = packet["credit_memo_decomposition"]
        assert decomp["credit_memo_id"] == "DOC-MAY-CM"
        assert len(decomp["uncertainty_flags"]) > 0

    def test_packet_has_subcontractor_overlap(self, db):
        packet = self._build_full_scenario(db)
        assert len(packet["subcontractor_overlaps"]) >= 1

    def test_packet_has_subscription_deferral(self, db):
        packet = self._build_full_scenario(db)
        assert packet["subscription_deferral"]["recognition_period"] == "2025-06"

    def test_packet_has_exposure_summary(self, db):
        packet = self._build_full_scenario(db)
        assert len(packet["exposure_summary"]) >= 4
        regulators = {e["regulator"] for e in packet["exposure_summary"]}
        assert "CRA" in regulators
        assert "RQ" in regulators

    def test_packet_amendment_flags_separated_by_period(self, db):
        packet = self._build_full_scenario(db)
        assert len(packet["amendment_flags"]["march"]) >= 1
        assert len(packet["amendment_flags"]["april"]) >= 2

    def test_march_flags_dont_reference_april_issues(self, db):
        packet = self._build_full_scenario(db)
        for flag in packet["amendment_flags"]["march"]:
            assert flag["filed_period"] == "2025-03"

    def test_april_flags_dont_reference_march_issues(self, db):
        packet = self._build_full_scenario(db)
        for flag in packet["amendment_flags"]["april"]:
            assert flag["filed_period"] == "2025-04"


# =========================================================================
# 12. Date ambiguity (04/05/2025)
# =========================================================================

class TestDateAmbiguity:
    """Invoice date 04/05/2025 is ambiguous: April 5 or May 4."""

    def test_date_is_ambiguous(self):
        state = build_date_resolution("04/05/2025")
        assert state.date_confidence < 0.60
        assert len(state.date_range) == 2

    def test_date_range_contains_both_interpretations(self):
        state = build_date_resolution("04/05/2025")
        assert "2025-04-05" in state.date_range
        assert "2025-05-04" in state.date_range

    def test_date_ambiguity_affects_period_classification(self):
        state = build_date_resolution("04/05/2025")
        modules = [a["module"] for a in state.date_affects]
        assert "period_end_accrual" in modules

    def test_french_context_resolves_to_dd_mm(self):
        state = build_date_resolution("04/05/2025", language="fr")
        assert state.resolved_date == "2025-05-04"
        assert state.date_confidence >= 0.80

    def test_english_context_resolves_to_mm_dd(self):
        state = build_date_resolution("04/05/2025", language="en")
        assert state.resolved_date == "2025-04-05"
        assert state.date_confidence >= 0.80


# =========================================================================
# 13. Credit memo decomposition with no tax breakdown
# =========================================================================

class TestCreditMemoDecomposition:
    """CAD 8050 tax-included credit memo with no stated breakdown."""

    def test_decomposition_confidence_below_threshold(self, db):
        decomp = decompose_credit_memo_safe(
            db,
            credit_memo_id="DOC-MAY-CM",
            credit_memo_amount_tax_included=float(CREDIT_MEMO_CAD),
            original_invoice_id="DOC-APR-MAIN",
            has_tax_breakdown=False,
        )
        assert decomp["confidence"] < 0.95

    def test_decomposition_produces_uncertainty_flag(self, db):
        decomp = decompose_credit_memo_safe(
            db,
            credit_memo_id="DOC-MAY-CM",
            credit_memo_amount_tax_included=float(CREDIT_MEMO_CAD),
            original_invoice_id="DOC-APR-MAIN",
            has_tax_breakdown=False,
        )
        assert len(decomp["uncertainty_flags"]) > 0

    def test_memo_text_identifies_components(self, db):
        decomp = decompose_credit_memo_safe(
            db,
            credit_memo_id="DOC-MAY-CM",
            credit_memo_amount_tax_included=float(CREDIT_MEMO_CAD),
            has_tax_breakdown=False,
            memo_text="subscription removed, commissioning rebilled, freight adjusted",
        )
        component_flags = [
            f for f in decomp["uncertainty_flags"]
            if f.get("flag") == "PARTIAL_COMPONENT_IDENTIFICATION"
        ]
        assert len(component_flags) >= 1

    def test_explicit_breakdown_gets_high_confidence(self, db):
        """With explicit GST/QST breakdown, confidence should be high."""
        decomp = decompose_credit_memo_safe(
            db,
            credit_memo_id="DOC-MAY-CM",
            credit_memo_amount_tax_included=float(CREDIT_MEMO_CAD),
            has_tax_breakdown=True,
            stated_gst=350.0,
            stated_qst=698.01,
        )
        assert decomp["confidence"] >= 0.90
        assert decomp["decomposition_method"] == "explicit_breakdown"


# =========================================================================
# 14. CBSA customs scope limitation
# =========================================================================

class TestCBSAScope:
    """CBSA GST applies only to imported goods, not services."""

    def test_cbsa_gst_less_than_invoice_hst(self):
        """CBSA GST on goods < invoice HST that covered everything."""
        assert CBSA_GST < MAIN_HST

    def test_cbsa_source_tagged(self, db):
        doc = db.execute(
            "SELECT regulator_source FROM documents WHERE document_id = 'DOC-MAY-CBSA'"
        ).fetchone()
        assert doc["regulator_source"] == "CBSA"

    def test_cbsa_only_covers_goods_and_freight(self):
        """CBSA value = (hardware + freight) * FX rate."""
        expected = (Decimal("18000") + Decimal("2800")) * FX_RATE_USD_CAD
        assert CBSA_GOODS_VALUE_CAD == expected.quantize(Decimal("0.01"))

    def test_customs_note_uncertainty_reason(self):
        reason = reason_customs_note_scope_limited(
            goods_value=str(CBSA_GOODS_VALUE_CAD),
            total_value=str(MAIN_CAD),
        )
        assert reason.reason_code == "CUSTOMS_NOTE_SCOPE_LIMITED"
        assert "goods" in reason.description_en.lower()


# =========================================================================
# 15. Integrated posting readiness
# =========================================================================

class TestPostingReadiness:
    """Putting it all together: uncertain documents must be blocked."""

    def test_main_invoice_blocked_after_evidence(self):
        """April main invoice should be blocked post-audit."""
        reasons = [
            reason_customs_note_scope_limited(
                str(CBSA_GOODS_VALUE_CAD), str(MAIN_CAD)),
            reason_date_ambiguous("04/05/2025", ["2025-04-05", "2025-05-04"]),
            reason_subcontractor_overlap(VENDOR_AMS, VENDOR_HTL, "commissioning"),
        ]
        state = evaluate_uncertainty(
            {
                "tax_treatment": 0.40,
                "date": 0.40,
                "vendor_scope": 0.55,
            },
            reasons=reasons,
        )
        decision = evaluate_posting_readiness({"document_id": "DOC-APR-MAIN"}, state)
        assert decision.outcome == BLOCK_PENDING_REVIEW
        assert decision.can_post is False
        assert len(decision.reviewer_notes) >= 3

    def test_cbsa_document_can_post(self):
        """CBSA document itself is authoritative — safe to post."""
        state = evaluate_uncertainty(
            {"amount": 0.95, "tax_code": 0.95, "vendor": 0.90},
        )
        decision = evaluate_posting_readiness({"document_id": "DOC-MAY-CBSA"}, state)
        assert decision.outcome == SAFE_TO_POST
        assert decision.can_post is True

    def test_credit_memo_partial_post_with_flags(self):
        """Credit memo with proportional decomposition: partial with flags."""
        reasons = [
            reason_credit_memo_tax_split_unproven("DOC-MAY-CM", "proportional"),
        ]
        state = evaluate_uncertainty(
            {"tax_split": 0.65, "amount": 0.85},
            reasons=reasons,
        )
        decision = evaluate_posting_readiness({"document_id": "DOC-MAY-CM"}, state)
        # Has reasons so can_post is False even for partial
        assert decision.outcome in (PARTIAL_POST_WITH_FLAGS, BLOCK_PENDING_REVIEW)
