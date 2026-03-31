"""
tests/red_team/test_journal_abuse.py
=====================================
Red-team test suite — Journal Abuse

Attack surface: manual journal entries that conflict with source documents,
attempting to silently distort tax positions, reverse automated postings,
reclassify assets, or manipulate payables.

Four abuse vectors:

    J-1. Phantom GST/QST             — fabricate tax credits with no source doc
    J-2. Reverse automated posting    — undo a system posting without doc link
    J-3. Reclassify fixed asset       — move capital cost to expense via journal
    J-4. Reduce payable after refund  — double-dip on a refund already linked

Pass criteria:
    - Conflict detection triggers on every abuse vector
    - Unsupported journals are quarantined or blocked
    - No silent coexistence with official posting chain
    - Every blocked action logged in audit_log

Fail if:
    - Manual journal can quietly distort tax or settlement
"""
from __future__ import annotations

import json
import secrets
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.concurrency_engine import (
    detect_manual_journal_collision,
    quarantine_manual_journal,
    validate_manual_journal,
)
from src.engines.tax_engine import GST_RATE, QST_RATE, calculate_gst_qst

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _uid() -> str:
    return secrets.token_hex(6)


def _make_db() -> sqlite3.Connection:
    """In-memory database with every table needed for journal abuse testing."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id             TEXT PRIMARY KEY,
            client_code             TEXT NOT NULL DEFAULT '',
            vendor                  TEXT NOT NULL DEFAULT '',
            amount                  REAL,
            subtotal                REAL,
            tax_total               REAL,
            document_date           TEXT,
            gl_account              TEXT DEFAULT '',
            doc_type                TEXT DEFAULT 'invoice',
            fraud_flags             TEXT DEFAULT '[]',
            fraud_override_reason   TEXT DEFAULT '',
            fraud_override_locked   INTEGER NOT NULL DEFAULT 0,
            posting_status          TEXT DEFAULT 'draft',
            approval_state          TEXT DEFAULT 'pending_review',
            review_status           TEXT DEFAULT 'Reviewed',
            version                 INTEGER NOT NULL DEFAULT 1,
            created_at              TEXT NOT NULL DEFAULT '',
            updated_at              TEXT NOT NULL DEFAULT ''
        );

        CREATE TRIGGER IF NOT EXISTS trg_document_version_increment
        AFTER UPDATE ON documents
        WHEN NEW.version = OLD.version
        BEGIN
            UPDATE documents SET version = OLD.version + 1
            WHERE document_id = NEW.document_id;
        END;

        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id      TEXT PRIMARY KEY,
            document_id     TEXT NOT NULL,
            client_code     TEXT NOT NULL DEFAULT '',
            document_date   TEXT DEFAULT '',
            gl_account      TEXT DEFAULT '',
            entry_kind      TEXT DEFAULT '',
            amount          REAL,
            approval_state  TEXT DEFAULT 'pending_review',
            posting_status  TEXT DEFAULT 'draft',
            reviewer        TEXT,
            version         INTEGER NOT NULL DEFAULT 1,
            payload_json    TEXT DEFAULT '{}',
            created_at      TEXT NOT NULL DEFAULT '',
            updated_at      TEXT NOT NULL DEFAULT ''
        );

        -- DB trigger: block posting if not approved
        CREATE TRIGGER IF NOT EXISTS trg_posting_status_guard
        AFTER UPDATE OF posting_status ON posting_jobs
        WHEN NEW.posting_status = 'posted'
             AND NEW.approval_state NOT LIKE '%approved%'
        BEGIN
            UPDATE posting_jobs SET posting_status = OLD.posting_status
            WHERE posting_id = NEW.posting_id;
            INSERT INTO audit_log (event_type, document_id, prompt_snippet, created_at)
            VALUES ('invalid_state_blocked', NEW.document_id,
                    '{"reason":"posting_without_approval"}',
                    strftime('%Y-%m-%dT%H:%M:%SZ','now'));
        END;

        CREATE TABLE IF NOT EXISTS audit_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type      TEXT NOT NULL DEFAULT 'ai_call',
            document_id     TEXT,
            username        TEXT,
            prompt_snippet  TEXT,
            latency_ms      INTEGER,
            created_at      TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS correction_chains (
            chain_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            chain_root_id       TEXT NOT NULL,
            client_code         TEXT NOT NULL,
            source_document_id  TEXT NOT NULL,
            target_document_id  TEXT NOT NULL,
            link_type           TEXT NOT NULL DEFAULT 'credit_memo',
            economic_effect     TEXT NOT NULL DEFAULT 'reduction',
            amount              REAL,
            tax_impact_gst      REAL,
            tax_impact_qst      REAL,
            uncertainty_flags   TEXT DEFAULT '[]',
            status              TEXT NOT NULL DEFAULT 'active',
            created_by          TEXT NOT NULL DEFAULT 'system',
            created_at          TEXT NOT NULL DEFAULT '',
            superseded_by       INTEGER,
            rollback_of         INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_chain_root
            ON correction_chains(chain_root_id);

        CREATE TABLE IF NOT EXISTS manual_journal_entries (
            entry_id            TEXT PRIMARY KEY,
            client_code         TEXT NOT NULL,
            period              TEXT NOT NULL DEFAULT '',
            entry_date          TEXT,
            prepared_by         TEXT DEFAULT '',
            debit_account       TEXT NOT NULL DEFAULT '',
            credit_account      TEXT NOT NULL DEFAULT '',
            amount              REAL NOT NULL DEFAULT 0,
            description         TEXT DEFAULT '',
            document_id         TEXT,
            source              TEXT DEFAULT 'bookkeeper',
            status              TEXT DEFAULT 'draft',
            collision_status    TEXT DEFAULT 'clear',
            collision_document_id TEXT,
            collision_chain_id  INTEGER,
            reviewed_by         TEXT,
            reviewed_at         TEXT,
            created_at          TEXT NOT NULL DEFAULT '',
            updated_at          TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS period_close_locks (
            client_code     TEXT NOT NULL,
            period          TEXT NOT NULL,
            locked_by       TEXT NOT NULL DEFAULT '',
            locked_at       TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (client_code, period)
        );

        CREATE TABLE IF NOT EXISTS fixed_assets (
            asset_id         TEXT PRIMARY KEY,
            client_code      TEXT NOT NULL,
            asset_name       TEXT NOT NULL,
            description      TEXT,
            cca_class        INTEGER NOT NULL,
            acquisition_date TEXT NOT NULL,
            cost             REAL NOT NULL,
            opening_ucc      REAL NOT NULL DEFAULT 0,
            current_ucc      REAL NOT NULL DEFAULT 0,
            accumulated_cca  REAL NOT NULL DEFAULT 0,
            status           TEXT NOT NULL DEFAULT 'active',
            disposal_date    TEXT,
            disposal_proceeds REAL,
            created_at       TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS invoice_lines (
            line_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id     TEXT NOT NULL,
            line_number     INTEGER,
            description     TEXT,
            quantity        REAL,
            unit_price      REAL,
            line_total_pretax REAL,
            tax_code        TEXT,
            gst_amount      REAL,
            qst_amount      REAL,
            created_at      TEXT NOT NULL DEFAULT ''
        );
    """)
    conn.commit()
    return conn


def _insert_doc(conn: sqlite3.Connection, doc_id: str, **kw) -> dict[str, Any]:
    """Insert a document with sensible defaults."""
    now = _utc_now()
    defaults = {
        "client_code": "JOURNAL_ABUSE_CO",
        "vendor": "Honest Vendor Inc.",
        "amount": 1000.00,
        "subtotal": 869.25,
        "tax_total": 130.75,
        "document_date": "2026-01-15",
        "gl_account": "5200",
        "doc_type": "invoice",
        "fraud_flags": "[]",
        "fraud_override_reason": "",
        "fraud_override_locked": 0,
        "posting_status": "draft",
        "approval_state": "pending_review",
        "review_status": "Reviewed",
        "version": 1,
        "created_at": now,
        "updated_at": now,
    }
    defaults.update(kw)
    cols = ", ".join(["document_id"] + list(defaults.keys()))
    placeholders = ", ".join(["?"] * (1 + len(defaults)))
    conn.execute(
        f"INSERT INTO documents ({cols}) VALUES ({placeholders})",
        [doc_id] + list(defaults.values()),
    )
    conn.commit()
    return {"document_id": doc_id, **defaults}


def _insert_posting(
    conn: sqlite3.Connection,
    doc_id: str,
    posting_id: str | None = None,
    **kw,
) -> str:
    """Insert a posting job linked to a document."""
    pid = posting_id or f"PJ-{_uid()}"
    now = _utc_now()
    defaults = {
        "document_id": doc_id,
        "client_code": "JOURNAL_ABUSE_CO",
        "document_date": "2026-01-15",
        "gl_account": "5200",
        "entry_kind": "debit",
        "amount": 1000.00,
        "approval_state": "approved_for_posting",
        "posting_status": "posted",
        "reviewer": "auto",
        "version": 1,
        "payload_json": "{}",
        "created_at": now,
        "updated_at": now,
    }
    defaults.update(kw)
    cols = ", ".join(["posting_id"] + list(defaults.keys()))
    placeholders = ", ".join(["?"] * (1 + len(defaults)))
    conn.execute(
        f"INSERT INTO posting_jobs ({cols}) VALUES ({placeholders})",
        [pid] + list(defaults.values()),
    )
    conn.commit()
    return pid


def _insert_correction_chain(conn: sqlite3.Connection, **kw) -> int:
    """Insert a correction chain link and return chain_id."""
    now = _utc_now()
    defaults = {
        "chain_root_id": f"ROOT-{_uid()}",
        "client_code": "JOURNAL_ABUSE_CO",
        "source_document_id": f"SRC-{_uid()}",
        "target_document_id": f"TGT-{_uid()}",
        "link_type": "credit_memo",
        "economic_effect": "reduction",
        "amount": 500.00,
        "tax_impact_gst": 25.00,
        "tax_impact_qst": 49.88,
        "uncertainty_flags": "[]",
        "status": "active",
        "created_by": "system",
        "created_at": now,
    }
    defaults.update(kw)
    cols = ", ".join(defaults.keys())
    placeholders = ", ".join(["?"] * len(defaults))
    cur = conn.execute(
        f"INSERT INTO correction_chains ({cols}) VALUES ({placeholders})",
        list(defaults.values()),
    )
    conn.commit()
    return cur.lastrowid


def _insert_mje(conn: sqlite3.Connection, entry_id: str, **kw) -> dict[str, Any]:
    """Insert a manual journal entry."""
    now = _utc_now()
    defaults = {
        "client_code": "JOURNAL_ABUSE_CO",
        "period": "2026-01",
        "entry_date": "2026-01-20",
        "prepared_by": "bookkeeper_eve",
        "debit_account": "2300",
        "credit_account": "1100",
        "amount": 500.00,
        "description": "Manual adjustment",
        "document_id": None,
        "source": "bookkeeper",
        "status": "draft",
        "collision_status": "clear",
        "collision_document_id": None,
        "collision_chain_id": None,
        "reviewed_by": None,
        "reviewed_at": None,
        "created_at": now,
        "updated_at": now,
    }
    defaults.update(kw)
    cols = ", ".join(["entry_id"] + list(defaults.keys()))
    placeholders = ", ".join(["?"] * (1 + len(defaults)))
    conn.execute(
        f"INSERT INTO manual_journal_entries ({cols}) VALUES ({placeholders})",
        [entry_id] + list(defaults.values()),
    )
    conn.commit()
    return {"entry_id": entry_id, **defaults}


def _insert_fixed_asset(conn: sqlite3.Connection, asset_id: str, **kw) -> dict[str, Any]:
    """Insert a fixed asset record."""
    now = _utc_now()
    defaults = {
        "client_code": "JOURNAL_ABUSE_CO",
        "asset_name": "Office Computer",
        "description": "Dell Precision workstation",
        "cca_class": 50,
        "acquisition_date": "2025-06-01",
        "cost": 5000.00,
        "opening_ucc": 5000.00,
        "current_ucc": 3625.00,
        "accumulated_cca": 1375.00,
        "status": "active",
        "disposal_date": None,
        "disposal_proceeds": None,
        "created_at": now,
    }
    defaults.update(kw)
    cols = ", ".join(["asset_id"] + list(defaults.keys()))
    placeholders = ", ".join(["?"] * (1 + len(defaults)))
    conn.execute(
        f"INSERT INTO fixed_assets ({cols}) VALUES ({placeholders})",
        [asset_id] + list(defaults.values()),
    )
    conn.commit()
    return {"asset_id": asset_id, **defaults}


def _audit_events(conn: sqlite3.Connection, event_type: str | None = None) -> list[dict]:
    """Retrieve audit log entries, optionally filtered by type."""
    if event_type:
        rows = conn.execute(
            "SELECT * FROM audit_log WHERE event_type = ? ORDER BY id",
            (event_type,),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
    return [dict(r) for r in rows]


# =========================================================================
# J-1: Phantom GST/QST — fabricate tax credits with no source document
# =========================================================================

class TestJ1PhantomGstQst:
    """Attack: create manual journal entries that debit GST/QST receivable
    accounts without any supporting source document.  This would fabricate
    ITC/ITR claims and distort the tax return."""

    def test_phantom_gst_debit_no_doc_triggers_unsupported_flag(self):
        """MJE debiting GST receivable (2420) with no document_id must be
        flagged as unsupported journal."""
        conn = _make_db()

        collision = detect_manual_journal_collision(
            conn,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="2420",       # GST receivable (ITC)
            credit_account="5200",       # expense — pretending to claim input tax
            amount=50.00,
            document_id=None,            # NO supporting document
        )

        # Must flag the unsupported journal
        assert any(
            c["type"] == "unsupported_journal" for c in collision["collisions"]
        ), "Phantom GST journal without doc must be flagged unsupported"

    def test_phantom_qst_debit_no_doc_triggers_unsupported_flag(self):
        """MJE debiting QST receivable (2430) with no document_id."""
        conn = _make_db()

        collision = detect_manual_journal_collision(
            conn,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="2430",       # QST receivable (ITR)
            credit_account="5200",
            amount=99.75,
            document_id=None,
        )

        assert any(
            c["type"] == "unsupported_journal" for c in collision["collisions"]
        ), "Phantom QST journal without doc must be flagged unsupported"

    def test_phantom_gst_qst_quarantined_via_validate(self):
        """Full validation path: phantom tax journal is quarantined."""
        conn = _make_db()
        eid = f"MJE-PHANTOM-{_uid()}"
        _insert_mje(
            conn, eid,
            debit_account="2420",
            credit_account="5200",
            amount=50.00,
            document_id=None,
            description="Phantom GST claim",
        )

        result = validate_manual_journal(
            conn,
            entry_id=eid,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="2420",
            credit_account="5200",
            amount=50.00,
            document_id=None,
        )

        # Without a collision with an existing chain, validate still flags
        # the unsupported journal.  The entry should NOT be silently accepted
        # with collision_status = 'clear' when touching tax accounts with no doc.
        assert "unsupported_journal" in json.dumps(result), \
            "Phantom tax journal must surface unsupported_journal flag"

    def test_phantom_gst_collides_with_real_posting(self):
        """Phantom GST journal that matches an existing system posting
        must be detected as a collision and quarantined."""
        conn = _make_db()

        # Real system posting: invoice with GST
        doc_id = f"INV-{_uid()}"
        _insert_doc(conn, doc_id, gl_account="2420", amount=50.00)
        _insert_posting(
            conn, doc_id,
            gl_account="2420",
            amount=50.00,
            entry_kind="credit",
        )

        # Attacker enters phantom MJE hitting same GL and amount
        eid = f"MJE-{_uid()}"
        _insert_mje(
            conn, eid,
            debit_account="2420",
            credit_account="5200",
            amount=50.00,
            document_id=None,
        )

        collision = detect_manual_journal_collision(
            conn,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="2420",
            credit_account="5200",
            amount=50.00,
            document_id=None,
        )

        assert collision["has_collision"], \
            "Phantom GST journal overlapping real posting must collide"

        # Quarantine it
        quarantine_manual_journal(
            conn, eid,
            collision_document_id=doc_id,
            reason="phantom_gst_overlap",
        )

        row = conn.execute(
            "SELECT status, collision_status FROM manual_journal_entries WHERE entry_id = ?",
            (eid,),
        ).fetchone()
        assert row["status"] == "quarantined"
        assert row["collision_status"] == "collision_detected"

        # Audit trail must record the quarantine
        events = _audit_events(conn, "manual_journal_quarantined")
        assert len(events) >= 1
        assert doc_id in events[-1]["prompt_snippet"]

    def test_phantom_tax_amounts_do_not_pass_tax_engine_validation(self):
        """If someone fabricates GST/QST amounts, they must not match
        the deterministic tax engine output for the stated pre-tax."""
        # Attacker claims $100 pre-tax with $10 GST and $20 QST
        real = calculate_gst_qst(100)
        assert real["gst"] != 10, "Fabricated GST must not match real calculation"
        assert real["qst"] != 20, "Fabricated QST must not match real calculation"
        # Real values: GST = 5.00, QST = 9.98 (Decimal)
        from decimal import Decimal
        assert real["gst"] == Decimal("5.00")
        assert real["qst"] == Decimal("9.98")


# =========================================================================
# J-2: Reverse automated posting without document link
# =========================================================================

class TestJ2ReverseAutomatedPosting:
    """Attack: bookkeeper enters a reversing journal that undoes a system
    posting without providing a linked document.  This would break the
    correction chain and allow untracked write-offs."""

    def test_reversal_without_doc_link_detected(self):
        """MJE reversing a posted amount on the same GL must be flagged
        when there is no document_id."""
        conn = _make_db()

        # System posting: $1000 debit on GL 5200
        doc_id = f"INV-{_uid()}"
        _insert_doc(conn, doc_id, gl_account="5200", amount=1000.00)
        _insert_posting(
            conn, doc_id,
            gl_account="5200",
            amount=1000.00,
            entry_kind="credit",
        )

        # Attacker reverses: credit 5200, debit cash — no doc
        collision = detect_manual_journal_collision(
            conn,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="1000",       # cash
            credit_account="5200",       # reverse the expense
            amount=1000.00,
            document_id=None,
        )

        assert collision["has_collision"], \
            "Reversal of posted amount without doc link must be detected"
        assert any(
            c["type"] == "posting_job_overlap" for c in collision["collisions"]
        ), "Must identify overlap with existing posting job"

    def test_reversal_quarantined_via_full_validate(self):
        """Full validation quarantines the unsupported reversal."""
        conn = _make_db()

        doc_id = f"INV-{_uid()}"
        _insert_doc(conn, doc_id, gl_account="5200", amount=1000.00)
        _insert_posting(
            conn, doc_id,
            gl_account="5200",
            amount=1000.00,
            entry_kind="credit",
        )

        eid = f"MJE-REV-{_uid()}"
        _insert_mje(
            conn, eid,
            debit_account="1000",
            credit_account="5200",
            amount=1000.00,
            document_id=None,
            description="Reversing entry — bookkeeper adjustment",
        )

        result = validate_manual_journal(
            conn,
            entry_id=eid,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="1000",
            credit_account="5200",
            amount=1000.00,
            document_id=None,
        )

        assert result["accepted"] is False, \
            "Unsupported reversal must NOT be accepted"
        assert result["status"] == "quarantined"

        # Verify MJE row is quarantined in DB
        row = conn.execute(
            "SELECT status FROM manual_journal_entries WHERE entry_id = ?",
            (eid,),
        ).fetchone()
        assert row["status"] == "quarantined"

    def test_reversal_collides_with_active_correction_chain(self):
        """MJE that reverses amounts already handled by a correction chain
        must be caught as correction_chain_overlap."""
        conn = _make_db()

        # Document backed by correction chain
        src_doc = f"INV-{_uid()}"
        tgt_doc = f"CM-{_uid()}"
        _insert_doc(conn, src_doc, gl_account="5200", amount=1000.00)
        _insert_doc(conn, tgt_doc, gl_account="5200", amount=-500.00,
                    doc_type="credit_memo")

        chain_id = _insert_correction_chain(
            conn,
            source_document_id=src_doc,
            target_document_id=tgt_doc,
            amount=500.00,
            economic_effect="reduction",
        )

        # Attacker tries to also reverse 500 on same GL
        collision = detect_manual_journal_collision(
            conn,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="1000",
            credit_account="5200",
            amount=500.00,
            document_id=None,
        )

        assert collision["has_collision"], \
            "Double-reversal against active correction chain must be detected"
        assert any(
            c["type"] == "correction_chain_overlap" for c in collision["collisions"]
        ), "Must identify correction_chain_overlap"

    def test_reversal_with_doc_link_no_collision_passes(self):
        """A properly linked reversal (with document_id) that does NOT
        overlap any existing posting or chain should be accepted."""
        conn = _make_db()

        # New credit memo with its own document
        cm_doc = f"CM-{_uid()}"
        _insert_doc(conn, cm_doc, gl_account="6100", amount=-200.00,
                    doc_type="credit_memo")

        eid = f"MJE-OK-{_uid()}"
        _insert_mje(
            conn, eid,
            debit_account="2100",
            credit_account="6100",
            amount=200.00,
            document_id=cm_doc,
            description="Legitimate reversal with doc link",
        )

        result = validate_manual_journal(
            conn,
            entry_id=eid,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="2100",
            credit_account="6100",
            amount=200.00,
            document_id=cm_doc,
        )

        assert result["accepted"] is True, \
            "Properly linked reversal on non-overlapping GL should be accepted"


# =========================================================================
# J-3: Reclassify fixed asset as expense
# =========================================================================

class TestJ3ReclassifyFixedAssetAsExpense:
    """Attack: enter a manual journal that credits a fixed-asset GL account
    and debits an expense account, effectively expensing a capital item
    outside the CCA schedule.  This distorts both the balance sheet and
    the tax depreciation pool."""

    def test_asset_reclassification_collides_with_asset_posting(self):
        """MJE that moves amount from asset GL (1500) to expense GL (5200)
        must collide if a posting already capitalized that amount."""
        conn = _make_db()

        # Original capitalization posting
        doc_id = f"INV-ASSET-{_uid()}"
        _insert_doc(conn, doc_id, gl_account="1500", amount=5000.00)
        _insert_posting(
            conn, doc_id,
            gl_account="1500",
            amount=5000.00,
            entry_kind="credit",
        )

        # Attacker reclassifies: debit expense, credit asset
        collision = detect_manual_journal_collision(
            conn,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="5200",       # expense
            credit_account="1500",       # fixed asset
            amount=5000.00,
            document_id=None,
        )

        assert collision["has_collision"], \
            "Reclassification from asset to expense must be detected"

    def test_asset_reclassification_quarantined(self):
        """Full validation path quarantines the reclassification attempt."""
        conn = _make_db()

        doc_id = f"INV-ASSET-{_uid()}"
        _insert_doc(conn, doc_id, gl_account="1500", amount=5000.00)
        _insert_posting(
            conn, doc_id,
            gl_account="1500",
            amount=5000.00,
            entry_kind="credit",
        )

        eid = f"MJE-RECLASS-{_uid()}"
        _insert_mje(
            conn, eid,
            debit_account="5200",
            credit_account="1500",
            amount=5000.00,
            document_id=None,
            description="Reclassify computer from asset to expense",
        )

        result = validate_manual_journal(
            conn,
            entry_id=eid,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="5200",
            credit_account="1500",
            amount=5000.00,
            document_id=None,
        )

        assert result["accepted"] is False, \
            "Asset reclassification without doc must be quarantined"
        assert result["status"] == "quarantined"

        events = _audit_events(conn, "manual_journal_quarantined")
        assert len(events) >= 1, \
            "Quarantine must be logged in audit trail"

    def test_partial_asset_reclassification_within_tolerance(self):
        """Even a partial reclassification (within 5% of posted amount)
        must be caught by the tolerance-based collision detector."""
        conn = _make_db()

        doc_id = f"INV-ASSET-{_uid()}"
        _insert_doc(conn, doc_id, gl_account="1500", amount=5000.00)
        _insert_posting(
            conn, doc_id,
            gl_account="1500",
            amount=5000.00,
            entry_kind="credit",
        )

        # Attacker moves $4900 (2% under posted amount — within 5% tolerance)
        collision = detect_manual_journal_collision(
            conn,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="5200",
            credit_account="1500",
            amount=4900.00,
            document_id=None,
        )

        assert collision["has_collision"], \
            "Partial reclassification within tolerance must still collide"

    def test_asset_still_in_register_after_blocked_reclassification(self):
        """After quarantining the MJE, the fixed asset register must remain
        unchanged — cost and UCC must not be altered."""
        conn = _make_db()

        asset_id = f"ASSET-{_uid()}"
        _insert_fixed_asset(conn, asset_id, cost=5000.00, current_ucc=3625.00)

        # Simulate the reclassification attempt (quarantined)
        eid = f"MJE-{_uid()}"
        _insert_mje(
            conn, eid,
            debit_account="5200",
            credit_account="1500",
            amount=5000.00,
            document_id=None,
        )

        # Quarantine the MJE
        quarantine_manual_journal(conn, eid, reason="asset_reclassification")

        # Asset register must be untouched
        asset = conn.execute(
            "SELECT cost, current_ucc, status FROM fixed_assets WHERE asset_id = ?",
            (asset_id,),
        ).fetchone()
        assert asset["cost"] == 5000.00, "Asset cost must not change"
        assert asset["current_ucc"] == 3625.00, "UCC must not change"
        assert asset["status"] == "active", "Asset must remain active"


# =========================================================================
# J-4: Reduce payable after refund already linked
# =========================================================================

class TestJ4ReducePayableAfterRefund:
    """Attack: after a refund/credit memo is already linked in the
    correction chain (reducing the payable), the bookkeeper enters a
    manual journal to reduce the payable AGAIN.  This double-dips:
    the vendor owes less, but the company also reduces its liability."""

    def test_double_reduction_detected(self):
        """MJE reducing payable (2100) that is already reduced by a
        correction chain must be caught as correction_chain_overlap."""
        conn = _make_db()

        # Original invoice + credit memo in correction chain
        inv_id = f"INV-{_uid()}"
        cm_id = f"CM-{_uid()}"
        _insert_doc(conn, inv_id, gl_account="2100", amount=2000.00)
        _insert_doc(conn, cm_id, gl_account="2100", amount=-500.00,
                    doc_type="credit_memo")

        _insert_correction_chain(
            conn,
            source_document_id=inv_id,
            target_document_id=cm_id,
            amount=500.00,
            tax_impact_gst=25.00,
            tax_impact_qst=49.88,
            economic_effect="reduction",
        )

        # Attacker reduces payable again via MJE
        collision = detect_manual_journal_collision(
            conn,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="2100",       # reduce payable
            credit_account="1000",       # credit cash
            amount=500.00,
            document_id=None,
        )

        assert collision["has_collision"], \
            "Double payable reduction must be detected"
        assert any(
            c["type"] == "correction_chain_overlap" for c in collision["collisions"]
        ), "Must identify overlap with existing correction chain"

    def test_double_reduction_quarantined_with_audit_trail(self):
        """Full validation quarantines and logs the double-dip attempt."""
        conn = _make_db()

        inv_id = f"INV-{_uid()}"
        cm_id = f"CM-{_uid()}"
        _insert_doc(conn, inv_id, gl_account="2100", amount=2000.00)
        _insert_doc(conn, cm_id, gl_account="2100", amount=-500.00,
                    doc_type="credit_memo")

        _insert_correction_chain(
            conn,
            source_document_id=inv_id,
            target_document_id=cm_id,
            amount=500.00,
            economic_effect="reduction",
        )

        eid = f"MJE-DBL-{_uid()}"
        _insert_mje(
            conn, eid,
            debit_account="2100",
            credit_account="1000",
            amount=500.00,
            document_id=None,
            description="Reduce payable — bookkeeper says vendor already paid",
        )

        result = validate_manual_journal(
            conn,
            entry_id=eid,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="2100",
            credit_account="1000",
            amount=500.00,
            document_id=None,
        )

        assert result["accepted"] is False
        assert result["status"] == "quarantined"

        # Audit trail must capture the quarantine
        events = _audit_events(conn, "manual_journal_quarantined")
        assert len(events) >= 1

    def test_slightly_different_amount_still_caught(self):
        """Attacker tries $495 instead of $500 — still within 5% tolerance."""
        conn = _make_db()

        inv_id = f"INV-{_uid()}"
        cm_id = f"CM-{_uid()}"
        _insert_doc(conn, inv_id, gl_account="2100", amount=2000.00)
        _insert_doc(conn, cm_id, gl_account="2100", amount=-500.00,
                    doc_type="credit_memo")

        _insert_correction_chain(
            conn,
            source_document_id=inv_id,
            target_document_id=cm_id,
            amount=500.00,
            economic_effect="reduction",
        )

        collision = detect_manual_journal_collision(
            conn,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="2100",
            credit_account="1000",
            amount=495.00,             # 1% under — within tolerance
            document_id=None,
        )

        assert collision["has_collision"], \
            "Near-match amount must still be caught by tolerance"

    def test_different_period_no_false_positive(self):
        """A payable reduction in a different period should NOT collide
        with a correction chain from another period."""
        conn = _make_db()

        inv_id = f"INV-{_uid()}"
        cm_id = f"CM-{_uid()}"
        _insert_doc(conn, inv_id, gl_account="2100", amount=2000.00,
                    document_date="2025-12-15")
        _insert_doc(conn, cm_id, gl_account="2100", amount=-500.00,
                    document_date="2025-12-20", doc_type="credit_memo")

        _insert_correction_chain(
            conn,
            source_document_id=inv_id,
            target_document_id=cm_id,
            amount=500.00,
            economic_effect="reduction",
        )

        # MJE in a completely different period
        collision = detect_manual_journal_collision(
            conn,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-03",           # different period
            debit_account="2100",
            credit_account="1000",
            amount=500.00,
            document_id=None,
        )

        # Should NOT have a correction_chain_overlap (chain is in 2025-12)
        chain_overlaps = [
            c for c in collision["collisions"]
            if c["type"] == "correction_chain_overlap"
        ]
        assert len(chain_overlaps) == 0, \
            "Different period should not trigger false positive chain overlap"

    def test_no_silent_coexistence(self):
        """The core invariant: a manual journal must NEVER silently
        coexist alongside a document-backed correction that handles
        the same economic event."""
        conn = _make_db()

        # Set up: invoice, credit memo, correction chain, and a posting
        inv_id = f"INV-{_uid()}"
        cm_id = f"CM-{_uid()}"
        _insert_doc(conn, inv_id, gl_account="2100", amount=3000.00)
        _insert_doc(conn, cm_id, gl_account="2100", amount=-750.00,
                    doc_type="credit_memo")

        _insert_correction_chain(
            conn,
            source_document_id=inv_id,
            target_document_id=cm_id,
            amount=750.00,
            economic_effect="reduction",
        )

        _insert_posting(
            conn, inv_id,
            gl_account="2100",
            amount=750.00,
            entry_kind="credit",
        )

        # Bookkeeper enters MJE touching same GL and amount
        eid = f"MJE-SILENT-{_uid()}"
        _insert_mje(
            conn, eid,
            debit_account="2100",
            credit_account="1000",
            amount=750.00,
            document_id=None,
        )

        result = validate_manual_journal(
            conn,
            entry_id=eid,
            client_code="JOURNAL_ABUSE_CO",
            period="2026-01",
            debit_account="2100",
            credit_account="1000",
            amount=750.00,
            document_id=None,
        )

        # MUST be blocked — no silent coexistence
        assert result["accepted"] is False, \
            "Manual journal must NEVER silently coexist with doc-backed correction"

        # MJE must not remain in 'draft' or 'clear' status
        row = conn.execute(
            "SELECT status, collision_status FROM manual_journal_entries WHERE entry_id = ?",
            (eid,),
        ).fetchone()
        assert row["status"] == "quarantined", \
            "MJE must be quarantined, not left in draft"
        assert row["collision_status"] != "clear", \
            "Collision status must not remain 'clear'"
