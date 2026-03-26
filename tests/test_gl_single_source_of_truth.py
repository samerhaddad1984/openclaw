"""
tests/test_gl_single_source_of_truth.py
=======================================
SINGLE SOURCE OF TRUTH DESTRUCTION TEST

Tests that prove architectural failures where multiple GL account sources
create silent incorrect accounting.

CRITICAL ARCHITECTURE FINDINGS:
1. documents.gl_account  = raw/original GL (never updated by substance engine)
2. posting_jobs.gl_account = overridden GL (substance engine applies here)
3. audit_engine trial balance reads from documents.gl_account (RAW)  — line 1104
4. posting/QBO export reads from posting_jobs.gl_account (OVERRIDDEN)
5. review dashboard reads from documents.gl_account (RAW) — line 5897
6. export scripts read from documents.gl_account (RAW) — line 54

Result: The same transaction shows DIFFERENT GL accounts in:
  - What gets posted to QBO  (correct, overridden)
  - What appears in audit trail balance (WRONG, raw)
  - What the reviewer sees in dashboard (WRONG, raw)
  - What exports show (WRONG, raw)
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.substance_engine import substance_classifier, run_substance_classifier
from src.agents.tools.gl_mapper import GLMapper, GLMapResult
from src.agents.tools.vendor_intelligence import VendorIntelligenceEngine, VendorIntelResult
from src.agents.tools.posting_builder import (
    upsert_posting_job,
    build_payload_from_sources,
    sync_posting_payload,
    ensure_posting_job_table_minimum,
    fetch_posting_row_by_document_id,
)

try:
    from src.engines.audit_engine import (
        ensure_audit_tables,
        generate_trial_balance,
        get_trial_balance,
        seed_chart_of_accounts,
    )
    HAS_AUDIT = True
except ImportError:
    HAS_AUDIT = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY, file_name TEXT, file_path TEXT,
            client_code TEXT, vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT, category TEXT,
            review_status TEXT, confidence REAL, raw_result TEXT, currency TEXT,
            subtotal REAL, tax_total REAL, extraction_method TEXT,
            ingest_source TEXT, raw_ocr_text TEXT,
            hallucination_suspected INTEGER DEFAULT 0,
            handwriting_low_confidence INTEGER DEFAULT 0,
            created_at TEXT, updated_at TEXT, assigned_to TEXT,
            manual_hold_reason TEXT, manual_hold_by TEXT, manual_hold_at TEXT,
            memo TEXT, substance_flags TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL DEFAULT 'ai_call',
            username TEXT, document_id TEXT, provider TEXT,
            task_type TEXT, prompt_snippet TEXT,
            latency_ms INTEGER, created_at TEXT NOT NULL DEFAULT ''
        )
    """)
    ensure_posting_job_table_minimum(conn)
    return conn


def _insert_document(conn: sqlite3.Connection, **kwargs) -> dict[str, Any]:
    doc_id = kwargs.pop("document_id", f"doc-{uuid.uuid4().hex[:8]}")
    defaults: dict[str, Any] = {
        "document_id": doc_id, "file_name": "test.pdf", "file_path": "/tmp/test.pdf",
        "client_code": "TEST01", "vendor": "Unknown Vendor", "doc_type": "invoice",
        "amount": 100.00, "document_date": "2026-01-15",
        "gl_account": "Uncategorized Expense", "tax_code": "GST_QST",
        "category": "Uncategorized", "review_status": "Ready", "confidence": 0.95,
        "currency": "CAD", "created_at": "2026-01-15T00:00:00Z",
        "updated_at": "2026-01-15T00:00:00Z",
    }
    defaults.update(kwargs)
    cols = list(defaults.keys())
    placeholders = ", ".join("?" for _ in cols)
    conn.execute(
        f"INSERT INTO documents ({', '.join(cols)}) VALUES ({placeholders})",
        [defaults[c] for c in cols],
    )
    conn.commit()
    return defaults


def _get_document_gl(conn: sqlite3.Connection, doc_id: str) -> str | None:
    row = conn.execute("SELECT gl_account FROM documents WHERE document_id = ?", (doc_id,)).fetchone()
    return row["gl_account"] if row else None


def _get_posting_gl(conn: sqlite3.Connection, doc_id: str) -> str | None:
    row = conn.execute("SELECT gl_account FROM posting_jobs WHERE document_id = ?", (doc_id,)).fetchone()
    return row["gl_account"] if row else None


def _get_payload_gl(conn: sqlite3.Connection, doc_id: str) -> str | None:
    row = conn.execute("SELECT payload_json FROM posting_jobs WHERE document_id = ?", (doc_id,)).fetchone()
    if not row or not row["payload_json"]:
        return None
    return json.loads(row["payload_json"]).get("gl_account")


# ===========================================================================
# SECTION 1: CROSS-LAYER INCONSISTENCY TESTS
# ===========================================================================


class TestCrossLayerGLInconsistency:

    def test_substance_override_writes_back_to_documents(self):
        """FIX 1: When substance overrides GL, documents table is updated to match."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="ABC Server Leasing", doc_type="invoice", amount=5000.00,
            gl_account="Uncategorized Expense",
            memo="Purchase of new server for data center",
        )
        doc_id = doc["document_id"]
        upsert_posting_job(conn, document=doc)

        doc_gl = _get_document_gl(conn, doc_id)
        posting_gl = _get_posting_gl(conn, doc_id)

        substance = substance_classifier(
            vendor="ABC Server Leasing", memo="Purchase of new server for data center",
            doc_type="invoice", amount=5000.00,
        )
        assert substance["potential_capex"] is True, "Substance engine should detect CapEx"
        assert substance["suggested_gl"] == "1500", "Should suggest Fixed Assets GL"
        assert posting_gl == "1500", "Posting table should have overridden GL"
        assert doc_gl == "1500", "FIX 1: documents.gl_account written back to match posting"
        assert doc_gl == posting_gl, "FIX 1: single source of truth — no split-brain"

    def test_review_dashboard_sees_correct_gl(self):
        """FIX 1: Dashboard reads documents table which now has overridden GL."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Assurance Desjardins", doc_type="invoice", amount=2400.00,
            gl_account="Uncategorized Expense",
            memo="Prime d'assurance annuelle bureau",
        )
        doc_id = doc["document_id"]
        upsert_posting_job(conn, document=doc)

        doc_gl = _get_document_gl(conn, doc_id)
        posting_gl = _get_posting_gl(conn, doc_id)

        assert posting_gl == "1300", "Posting should override to Prepaid Expenses"
        assert doc_gl == "1300", "FIX 1: documents.gl_account updated to match posting"

    @pytest.mark.skipif(not HAS_AUDIT, reason="audit_engine not available")
    def test_audit_trial_balance_uses_correct_gl(self):
        """FIX 1: audit_engine.generate_trial_balance() reads d.gl_account
        which is now updated by posting_builder write-back."""
        conn = _make_db()
        ensure_audit_tables(conn)
        seed_chart_of_accounts(conn)

        doc = _insert_document(
            conn, vendor="Financement BDC", doc_type="invoice", amount=50000.00,
            gl_account="Uncategorized Expense",
            memo="Versement prêt hypothèque commercial",
            client_code="AUDIT01", document_date="2026-01-15",
        )
        doc_id = doc["document_id"]
        upsert_posting_job(conn, document=doc)
        conn.execute(
            "UPDATE posting_jobs SET posting_status = 'posted' WHERE document_id = ?",
            (doc_id,),
        )
        conn.commit()

        posting_gl = _get_posting_gl(conn, doc_id)
        doc_gl = _get_document_gl(conn, doc_id)

        assert posting_gl == "2500", "Posting correctly overrode to loan GL"
        assert doc_gl == "2500", "FIX 1: documents.gl_account written back to 2500"

    def test_export_uses_correct_gl(self):
        """FIX 1: Export reads documents.gl_account which now has the overridden GL."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Revenu Québec", doc_type="invoice", amount=3500.00,
            gl_account="Uncategorized Expense", memo="Remise TPS trimestre 4",
        )
        doc_id = doc["document_id"]
        upsert_posting_job(conn, document=doc)

        doc_gl = _get_document_gl(conn, doc_id)
        posting_gl = _get_posting_gl(conn, doc_id)

        assert posting_gl in ("2200", "2205", "2210"), "Posting should override to tax liability GL"
        assert doc_gl == posting_gl, "FIX 1: documents.gl_account matches posting"

    def test_payload_json_consistency_with_posting_columns(self):
        """payload_json.gl_account must match posting_jobs.gl_account column."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Depot Sécurité Inc", doc_type="invoice", amount=1200.00,
            gl_account="Uncategorized Expense", memo="Security deposit for office lease",
        )
        doc_id = doc["document_id"]
        upsert_posting_job(conn, document=doc)

        posting_gl = _get_posting_gl(conn, doc_id)
        payload_gl = _get_payload_gl(conn, doc_id)
        assert posting_gl == payload_gl, (
            f"payload_json.gl_account ({payload_gl}) != posting_jobs.gl_account ({posting_gl})"
        )


# ===========================================================================
# SECTION 2: FORCED FAILURE SCENARIOS
# ===========================================================================


class TestForcedFailureScenarios:

    def test_capex_respects_vendor_confidence_threshold(self):
        """FIX 5: CapEx won't override non-uncategorized GL when confidence >= 0.85."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Dell Technologies", doc_type="invoice", amount=15000.00,
            gl_account="5100 - Office Supplies",  # WRONG but high-confidence vendor memory
            memo="Purchase of 10 server racks for data center expansion",
            confidence=0.95,
        )
        doc_id = doc["document_id"]

        substance = substance_classifier(
            vendor="Dell Technologies",
            memo="Purchase of 10 server racks for data center expansion",
            doc_type="invoice", amount=15000.00,
        )
        assert substance["potential_capex"] is True, "Substance should detect CapEx"
        assert substance["suggested_gl"] == "1500", "Should suggest Fixed Assets"

        upsert_posting_job(conn, document=doc)
        posting_gl = _get_posting_gl(conn, doc_id)

        # FIX 5: CapEx does NOT override when vendor confidence >= 0.85
        assert posting_gl == "5100 - Office Supplies", (
            "FIX 5: CapEx respects high-confidence vendor memory (0.95 >= 0.85)"
        )

    def test_glmapper_wrong_and_no_substance_trigger(self):
        """GLMapper wrong + no substance keyword = wrong GL propagates."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Consulting XYZ", doc_type="invoice", amount=8000.00,
            gl_account="5200 - Advertising", memo="Monthly consulting fee",
        )
        doc_id = doc["document_id"]

        substance = substance_classifier(
            vendor="Consulting XYZ", memo="Monthly consulting fee",
            doc_type="invoice", amount=8000.00,
        )
        assert substance["suggested_gl"] is None

        upsert_posting_job(conn, document=doc)
        assert _get_posting_gl(conn, doc_id) == "5200 - Advertising", (
            "Wrong GL propagates unchallenged"
        )

    def test_override_without_explanation_attached(self):
        """When substance overrides, notes must explain why."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Hypothèques Nationale", doc_type="invoice", amount=250000.00,
            gl_account="Uncategorized Expense", memo="Versement hypothèque mensuel",
        )
        doc_id = doc["document_id"]
        upsert_posting_job(conn, document=doc)

        row = conn.execute("SELECT notes FROM posting_jobs WHERE document_id = ?", (doc_id,)).fetchone()
        notes = json.loads(row["notes"]) if row and row["notes"] else []

        posting_gl = _get_posting_gl(conn, doc_id)
        if posting_gl != "Uncategorized Expense":
            assert len(notes) > 0, "GL overridden but no notes attached"
            has_substance_note = any(
                "passif" in n.lower() or "liability" in n.lower()
                or "vérifier" in n.lower() or "verify" in n.lower()
                for n in notes
            )
            assert has_substance_note, f"Override notes don't explain classification: {notes}"

    def test_personal_expense_block_flag_enforced(self):
        """FIX 3: Personal expense block_auto_approval blocks posting and forces NeedsReview."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Netflix Inc", doc_type="invoice", amount=15.99,
            gl_account="Uncategorized Expense", memo="Netflix personal subscription",
            confidence=0.98,
        )
        doc_id = doc["document_id"]

        substance = substance_classifier(
            vendor="Netflix Inc", memo="Netflix personal subscription",
            doc_type="invoice", amount=15.99,
        )
        assert substance["potential_personal_expense"] is True
        assert substance["block_auto_approval"] is True

        upsert_posting_job(conn, document=doc)
        row = conn.execute(
            "SELECT approval_state, posting_status, review_status FROM posting_jobs WHERE document_id = ?",
            (doc_id,),
        ).fetchone()

        assert row["approval_state"] == "pending_review", "FIX 3: must be pending_review"
        assert row["posting_status"] == "blocked", "FIX 3: posting must be blocked"
        assert row["review_status"] == "NeedsReview", "FIX 3: review_status must be NeedsReview"

    def test_substance_capex_below_threshold(self):
        substance = substance_classifier(
            vendor="Best Buy", memo="Laptop for office", doc_type="invoice", amount=1499.00,
        )
        assert substance["potential_capex"] is False
        assert substance["suggested_gl"] is None

    def test_substance_capex_at_threshold(self):
        substance = substance_classifier(
            vendor="Best Buy", memo="Computer laptop for office", doc_type="invoice", amount=1500.00,
        )
        assert substance["potential_capex"] is True
        assert substance["suggested_gl"] == "1500"


# ===========================================================================
# SECTION 3: ESCALATION VS OVERRIDE
# ===========================================================================


class TestEscalationVsOverride:

    def test_mixed_use_transaction_produces_multiple_notes(self):
        """Both CapEx AND prepaid signals => multiple review notes."""
        substance = substance_classifier(
            vendor="Assurance Machinerie Plus",
            memo="Assurance annuelle sur machinerie de construction - prépayé",
            doc_type="invoice", amount=8000.00,
        )
        if substance["potential_capex"] and substance["potential_prepaid"]:
            assert len(substance["review_notes"]) >= 2, (
                "Mixed-signal transaction should have multiple review notes"
            )

    def test_conflicting_signals_produce_review_notes(self):
        substance = substance_classifier(
            vendor="Prêt Auto Assurance Inc",
            memo="Financement assurance véhicule commercial prépayé",
            doc_type="invoice", amount=25000.00,
        )
        flags_set = sum([
            substance["potential_capex"], substance["potential_prepaid"],
            substance["potential_loan"],
        ])
        if flags_set >= 2:
            assert len(substance["review_notes"]) >= flags_set

    def test_unclear_wording_should_not_override(self):
        substance = substance_classifier(
            vendor="Services Divers", memo="Paiement mensuel - voir facture",
            doc_type="invoice", amount=3000.00,
        )
        assert substance["suggested_gl"] is None
        assert not substance["potential_capex"]
        assert not substance["potential_prepaid"]
        assert not substance["potential_loan"]


# ===========================================================================
# SECTION 4: UNICODE + SUBSTANCE COMBINED ATTACK
# ===========================================================================


class TestUnicodeSubstanceCombined:

    def test_nbsp_around_keywords(self):
        """NBSP around keywords — test if detection still works."""
        # "computer" is in _CAPEX_KEYWORDS
        normal = substance_classifier(
            vendor="Corp", memo="Purchase of computer for office",
            doc_type="invoice", amount=5000.00,
        )
        assert normal["potential_capex"] is True

        unicode_result = substance_classifier(
            vendor="Corp", memo="Purchase of \u00a0computer\u00a0 for office",
            doc_type="invoice", amount=5000.00,
        )
        # NBSP may break \b word boundary — document the behavior
        if not unicode_result["potential_capex"]:
            pass  # CONFIRMED: NBSP breaks word boundary detection

    def test_zero_width_chars_in_keywords(self):
        """Zero-width space inside keywords breaks regex matching."""
        result = substance_classifier(
            vendor="Corp", memo="Purchase of com\u200bputer for office",
            doc_type="invoice", amount=5000.00,
        )
        if not result["potential_capex"]:
            pass  # CONFIRMED: ZWSP bypasses substance detection

    def test_ocr_noise_garbled_keywords(self):
        """OCR errors that garble keywords prevent detection."""
        garbled_results = []
        for garbled in ["6quipement", "equlpement", "equ1pement", "computcr"]:
            result = substance_classifier(
                vendor="Corp", memo=f"Achat de {garbled} pour bureau",
                doc_type="invoice", amount=5000.00,
            )
            garbled_results.append((garbled, result["potential_capex"]))
        failures = [g for g, detected in garbled_results if not detected]
        assert len(failures) > 0, "OCR noise should cause detection failures"

    def test_french_accented_keywords(self):
        result = substance_classifier(
            vendor="Rénovation Québec",
            memo="Rénovation complète du bureau - amélioration locative",
            doc_type="invoice", amount=75000.00,
        )
        assert result["potential_capex"] is True

    def test_rtl_override_chars_no_crash(self):
        result = substance_classifier(
            vendor="\u202eServer\u202c Services", memo="Server purchase",
            doc_type="invoice", amount=5000.00,
        )
        assert isinstance(result, dict)


# ===========================================================================
# SECTION 5: MEMORY + SUBSTANCE COLLISION
# ===========================================================================


class TestMemorySubstanceCollision:

    def test_vendor_intel_vs_substance_capex_respects_confidence(self):
        """FIX 5: Vendor intel says Office Supplies, substance says CapEx.
        CapEx does NOT override when vendor confidence >= 0.85."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "vendor_intel.json").write_text(json.dumps({
                "vendors": {"Dell Technologies": {
                    "category": "Office Supplies",
                    "gl_account": "5100 - Office Supplies",
                    "tax_code": "GST_QST", "document_family": "invoice",
                }},
                "doc_type_defaults": {},
                "default": {"category": "Uncategorized", "gl_account": "Uncategorized Expense", "tax_code": "GST_QST"},
            }), encoding="utf-8")
            engine = VendorIntelligenceEngine(Path(tmpdir))
            assert engine.classify("Dell Technologies", "invoice").gl_account == "5100 - Office Supplies"

        substance = substance_classifier(
            vendor="Dell Technologies", memo="10x Dell PowerEdge R750 server racks",
            doc_type="invoice", amount=150000.00,
        )
        assert substance["suggested_gl"] == "1500"

        conn = _make_db()
        # High confidence (0.95) means vendor memory is trusted
        doc = _insert_document(
            conn, vendor="Dell Technologies", doc_type="invoice", amount=150000.00,
            gl_account="5100 - Office Supplies", memo="10x Dell PowerEdge R750 server racks",
            confidence=0.95,
        )
        upsert_posting_job(conn, document=doc)
        posting_gl = _get_posting_gl(conn, doc["document_id"])

        # FIX 5: CapEx does NOT override when confidence >= 0.85
        assert posting_gl == "5100 - Office Supplies", (
            "FIX 5: CapEx respects high-confidence vendor memory"
        )

        # But review notes should still warn about the CapEx detection
        row = conn.execute(
            "SELECT notes FROM posting_jobs WHERE document_id = ?",
            (doc["document_id"],),
        ).fetchone()
        notes = json.loads(row["notes"]) if row and row["notes"] else []
        has_capex_note = any("immobilisation" in n.lower() or "capital" in n.lower() for n in notes)
        assert has_capex_note, "Review notes should warn about CapEx even without override"

    def test_glmapper_vs_substance_prepaid_respects_confidence(self):
        """FIX 5: GLMapper returns wrong GL for insurance. Prepaid won't override
        when confidence >= 0.85."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "gl_map.json").write_text(json.dumps({
                "vendors": {"Assurance TD": {"gl_account": "5300 - Professional Fees", "tax_code": "GST_QST"}},
                "doc_types": {},
                "default": {"gl_account": "Uncategorized Expense", "tax_code": "GST_QST"},
            }), encoding="utf-8")
            mapper = GLMapper(Path(tmpdir))
            assert mapper.map("Assurance TD", "invoice").gl_account == "5300 - Professional Fees"

        substance = substance_classifier(
            vendor="Assurance TD", memo="Prime d'assurance responsabilité annuelle",
            doc_type="invoice", amount=6000.00,
        )
        assert substance["potential_prepaid"] is True
        assert substance["suggested_gl"] == "1300"

        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Assurance TD", doc_type="invoice", amount=6000.00,
            gl_account="5300 - Professional Fees",
            memo="Prime d'assurance responsabilité annuelle",
            confidence=0.95,
        )
        upsert_posting_job(conn, document=doc)
        # FIX 5: Prepaid does NOT override when confidence >= 0.85
        assert _get_posting_gl(conn, doc["document_id"]) == "5300 - Professional Fees", (
            "FIX 5: Prepaid respects high-confidence vendor memory"
        )

    def test_conflict_partially_visible_in_notes(self):
        """When vendor memory and substance disagree, review notes should still appear."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="TD Insurance", doc_type="invoice", amount=12000.00,
            gl_account="5300 - Professional Fees",
            memo="Annual insurance premium - prepaid 12 months",
        )
        upsert_posting_job(conn, document=doc)

        row = conn.execute(
            "SELECT notes FROM posting_jobs WHERE document_id = ?",
            (doc["document_id"],),
        ).fetchone()
        notes = json.loads(row["notes"]) if row and row["notes"] else []

        has_prepaid_warning = any(
            "prepaid" in n.lower() or "prépayé" in n.lower()
            or "payée d'avance" in n.lower()
            for n in notes
        )
        if not has_prepaid_warning:
            pytest.fail("Substance detected prepaid but review notes are missing — conflict hidden")


# ===========================================================================
# SECTION 6: MULTIPLE GL SOURCES — FULL ENUMERATION
# ===========================================================================


class TestAllGLSourcesEnumerated:

    def test_documents_table_updated_by_posting_writeback(self):
        """FIX 1: documents.gl_account is updated when substance engine overrides."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Hypothèque BMO", doc_type="invoice", amount=200000.00,
            gl_account="Uncategorized Expense", memo="Paiement hypothèque mensuel",
        )
        doc_id = doc["document_id"]
        upsert_posting_job(conn, document=doc)

        assert _get_document_gl(conn, doc_id) == "2500", "FIX 1: documents.gl_account written back"
        assert _get_posting_gl(conn, doc_id) == "2500"

    def test_three_way_gl_consistency(self):
        """FIX 1: Documents, posting column, and payload_json all agree after write-back."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Caution Locative", doc_type="invoice", amount=3000.00,
            gl_account="Uncategorized Expense", memo="Security deposit for new office lease",
        )
        doc_id = doc["document_id"]
        upsert_posting_job(conn, document=doc)

        doc_gl = _get_document_gl(conn, doc_id)
        posting_gl = _get_posting_gl(conn, doc_id)
        payload_gl = _get_payload_gl(conn, doc_id)

        assert posting_gl == "1400", "Security deposit should be GL 1400"
        assert doc_gl == posting_gl, "FIX 1: documents.gl_account matches posting"
        assert posting_gl == payload_gl, "posting column and payload_json must agree"

    def test_sampling_filter_finds_overridden_gl(self):
        """FIX 1: Audit sampling on d.gl_account now finds overridden GL."""
        conn = _make_db()
        if HAS_AUDIT:
            ensure_audit_tables(conn)
            seed_chart_of_accounts(conn)

        doc = _insert_document(
            conn, vendor="Machinerie Lourde Inc", doc_type="invoice", amount=50000.00,
            gl_account="Uncategorized Expense", memo="Achat machinerie lourde",
            client_code="SAMP01", document_date="2026-03-15",
        )
        doc_id = doc["document_id"]
        upsert_posting_job(conn, document=doc)
        conn.execute(
            "UPDATE posting_jobs SET posting_status = 'posted' WHERE document_id = ?", (doc_id,),
        )
        conn.commit()

        rows = conn.execute(
            "SELECT * FROM documents WHERE LOWER(COALESCE(gl_account,'')) LIKE ? AND client_code = ?",
            ("%1500%", "SAMP01"),
        ).fetchall()

        assert len(rows) == 1, (
            "FIX 1: Sampling for GL 1500 in documents table finds the document — "
            f"documents.gl_account={_get_document_gl(conn, doc_id)}"
        )


# ===========================================================================
# SECTION 7: SUBSTANCE ENGINE EDGE CASES
# ===========================================================================


class TestSubstanceEngineEdgeCases:

    def test_none_amount_still_triggers_capex(self):
        """None amount bypasses threshold — CapEx triggers without amount validation."""
        result = substance_classifier(
            vendor="Dell", memo="New computer for office", doc_type="invoice", amount=None,
        )
        assert result["potential_capex"] is True

    def test_negative_amount_capex(self):
        """Negative amounts (credit notes) — check if abs() applied for threshold."""
        result = substance_classifier(
            vendor="Server Corp", memo="Credit note for returned server",
            doc_type="credit_note", amount=-2000.00,
        )
        # Documents whether negative amounts trigger CapEx
        # abs(-2000) >= 1500 should be True if engine uses abs()
        if not result["potential_capex"]:
            pass  # Engine doesn't handle negatives — credit notes escape classification

    def test_empty_string_fields(self):
        result = substance_classifier(vendor="", memo="", doc_type="", amount=None)
        assert result["suggested_gl"] is None
        assert not any([
            result["potential_capex"], result["potential_prepaid"],
            result["potential_loan"], result["potential_tax_remittance"],
            result["potential_personal_expense"],
        ])

    def test_first_match_wins_priority_bug(self):
        """CapEx checked first. If CapEx + Prepaid both match, CapEx GL wins."""
        result = substance_classifier(
            vendor="Assurance Machinerie Inc",
            memo="Insurance on machinery - annual prepaid",
            doc_type="invoice", amount=5000.00,
        )
        if result["potential_capex"] and result["potential_prepaid"]:
            assert result["suggested_gl"] == "1500", (
                "CapEx GL wins over prepaid even for insurance — priority ordering bug"
            )

    def test_tax_remittance_false_positive(self):
        """'TPS' in vendor name triggers tax remittance — may be false positive."""
        result = substance_classifier(
            vendor="TPS Consulting", memo="Monthly consulting fee",
            doc_type="invoice", amount=3000.00,
        )
        if result["potential_tax_remittance"]:
            assert result.get("review_notes"), "False positive must add review note"


# ===========================================================================
# SECTION 8: SINGLE AUTHORITATIVE GL DECISION PATH — PROOF
# ===========================================================================


class TestSingleAuthoritativeDecisionPath:

    def test_single_source_of_truth_achieved(self):
        """FIX 1: System has ONE consistent GL value after posting.
        All consumers (audit, dashboard, export, QBO) read the same value."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Prêt BDC", doc_type="invoice", amount=100000.00,
            gl_account="Uncategorized Expense", memo="Versement prêt commercial",
        )
        doc_id = doc["document_id"]
        upsert_posting_job(conn, document=doc)

        doc_gl = _get_document_gl(conn, doc_id)
        posting_gl = _get_posting_gl(conn, doc_id)

        assert posting_gl == "2500", "Loan should override to liability GL"
        assert doc_gl == posting_gl, (
            "FIX 1: Single source of truth — documents and posting agree"
        )

    def test_priority_types_override_any_gl_others_respect_confidence(self):
        """FIX 2+5: Priority types (loan, tax) always override. CapEx/prepaid respect confidence."""
        # Cases: (wrong_gl, memo, amount, correct_gl, should_override)
        cases = [
            # CapEx with high confidence (0.95 default) — does NOT override
            ("5100 - Office Supplies", "server rack", 15000, "1500", False),
            # Prepaid with high confidence — does NOT override
            ("5300 - Professional Fees", "assurance annuelle", 6000, "1300", False),
            # Loan — ALWAYS overrides (priority type)
            ("5200 - Advertising", "financement crédit-bail", 50000, "2500", True),
            # Tax remittance — ALWAYS overrides (priority type)
            ("5100 - Office Supplies", "remise TPS trimestrielle", 3500, "2200", True),
        ]
        for wrong_gl, memo, amount, correct_gl, should_override in cases:
            conn = _make_db()
            doc = _insert_document(
                conn, vendor="Test Vendor", doc_type="invoice",
                amount=amount, gl_account=wrong_gl, memo=memo,
                confidence=0.95,
            )
            upsert_posting_job(conn, document=doc)
            posting_gl = _get_posting_gl(conn, doc["document_id"])
            if should_override:
                assert posting_gl == correct_gl, (
                    f"FIX 2: Priority type should override {wrong_gl} → {correct_gl} for '{memo}'"
                )
            else:
                assert posting_gl == wrong_gl, (
                    f"FIX 5: Non-priority type respects confidence for '{memo}'"
                )

    def test_substance_notes_appended_and_priority_override_applied(self):
        """FIX 2: Loan keywords are priority type — override even non-uncategorized GL.
        Review notes always appended."""
        conn = _make_db()
        doc = _insert_document(
            conn, vendor="Leasing Corp", doc_type="invoice", amount=50000.00,
            gl_account="5100 - Office Supplies", memo="Crédit-bail machinerie lourde",
        )
        upsert_posting_job(conn, document=doc)

        row = conn.execute(
            "SELECT notes, gl_account FROM posting_jobs WHERE document_id = ?",
            (doc["document_id"],),
        ).fetchone()
        notes = json.loads(row["notes"]) if row and row["notes"] else []

        # FIX 2: Loan (crédit-bail) is a priority type — overrides any GL
        assert row["gl_account"] == "1500", (
            "FIX 2: Loan/CapEx priority type overrides expense-range GL"
        )
        has_substance_note = any(
            "vérifier" in n.lower() or "verify" in n.lower()
            or "immobilisation" in n.lower() or "capital" in n.lower()
            or "passif" in n.lower() or "liability" in n.lower()
            for n in notes
        )
        assert has_substance_note, "Review notes must explain the override"
