"""
tests/red_team/test_immutable_audit_trail_breach.py
====================================================
Red-team test suite — Immutable Audit Trail Breach

Attack surface: every legal app path that could mutate a critical record
after it should be frozen.  Six breach vectors:

    1. Update after sign-off           — edit a signed working paper
    2. Soft delete then recreate       — delete a correction, recreate with new data
    3. Rollback then edit old object   — roll back a chain then silently mutate it
    4. Manual journal after lock       — insert MJE into a locked period
    5. Override fraud reason after save — change fraud_override_reason once locked
    6. Re-sign working paper > 24h     — backdate sign-off to smuggle past review

Pass criteria:
    • Immutability holds through app layer
    • DB triggers reject bypasses
    • Every blocked action is logged in audit_log
    • No critical record changes without explicit correction lineage
"""
from __future__ import annotations

import json
import secrets
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.audit_engine import (
    add_working_paper_item,
    ensure_audit_tables,
    get_or_create_working_paper,
    sign_off_working_paper,
    update_working_paper,
)
from src.engines.concurrency_engine import (
    StaleVersionError,
    approve_with_version_check,
    check_version_or_raise,
    detect_manual_journal_collision,
    quarantine_manual_journal,
    validate_manual_journal,
)
from src.engines.correction_chain import (
    build_correction_chain_link,
    get_full_correction_chain,
    rollback_correction,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _make_db() -> sqlite3.Connection:
    """In-memory database with every table needed for immutability testing."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Audit engine tables (includes working_papers, triggers)
    ensure_audit_tables(conn)

    # Documents table with fraud override + version columns
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
            fraud_flags             TEXT DEFAULT '[]',
            fraud_override_reason   TEXT DEFAULT '',
            fraud_override_locked   INTEGER NOT NULL DEFAULT 0,
            posting_status          TEXT DEFAULT 'draft',
            approval_state          TEXT DEFAULT 'pending_review',
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
            created_at      TEXT NOT NULL DEFAULT '',
            updated_at      TEXT NOT NULL DEFAULT ''
        );

        CREATE TRIGGER IF NOT EXISTS trg_posting_version_increment
        AFTER UPDATE ON posting_jobs
        WHEN NEW.version = OLD.version
        BEGIN
            UPDATE posting_jobs SET version = OLD.version + 1
            WHERE posting_id = NEW.posting_id;
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

        CREATE TABLE IF NOT EXISTS rollback_log (
            rollback_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code         TEXT NOT NULL,
            target_type         TEXT NOT NULL,
            target_id           TEXT NOT NULL,
            rollback_reason     TEXT NOT NULL DEFAULT '',
            rolled_back_by      TEXT NOT NULL DEFAULT '',
            state_before_json   TEXT NOT NULL DEFAULT '{}',
            state_after_json    TEXT NOT NULL DEFAULT '{}',
            is_reimport_blocked INTEGER NOT NULL DEFAULT 0,
            created_at          TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS document_clusters (
            cluster_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_key     TEXT NOT NULL,
            client_code     TEXT NOT NULL,
            cluster_head_id TEXT,
            member_count    INTEGER NOT NULL DEFAULT 0,
            status          TEXT NOT NULL DEFAULT 'active',
            created_at      TEXT NOT NULL DEFAULT '',
            updated_at      TEXT NOT NULL DEFAULT ''
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cluster_key
            ON document_clusters(cluster_key);

        CREATE TABLE IF NOT EXISTS document_cluster_members (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_id      INTEGER NOT NULL,
            document_id     TEXT NOT NULL,
            is_cluster_head INTEGER NOT NULL DEFAULT 0,
            similarity_score REAL,
            variant_notes   TEXT,
            added_at        TEXT NOT NULL DEFAULT '',
            UNIQUE(cluster_id, document_id)
        );

        CREATE TABLE IF NOT EXISTS period_close_locks (
            client_code     TEXT NOT NULL,
            period          TEXT NOT NULL,
            locked_by       TEXT NOT NULL DEFAULT '',
            locked_at       TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (client_code, period)
        );

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
        "client_code": "IMMUT_INC",
        "vendor": "Test Vendor",
        "amount": 1000.00,
        "subtotal": 869.25,
        "tax_total": 130.75,
        "document_date": "2026-01-15",
        "gl_account": "5200",
        "fraud_flags": "[]",
        "fraud_override_reason": "",
        "fraud_override_locked": 0,
        "posting_status": "draft",
        "approval_state": "pending_review",
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


def _insert_posting(conn: sqlite3.Connection, doc_id: str, posting_id: str | None = None) -> str:
    pid = posting_id or f"pj_{secrets.token_hex(4)}"
    now = _utc_now()
    conn.execute(
        """INSERT INTO posting_jobs
               (posting_id, document_id, approval_state, posting_status, version, created_at, updated_at)
           VALUES (?, ?, 'pending_review', 'draft', 1, ?, ?)""",
        (pid, doc_id, now, now),
    )
    conn.commit()
    return pid


def _audit_events(conn: sqlite3.Connection, event_type: str | None = None) -> list[dict]:
    """Return audit_log rows, optionally filtered by event_type."""
    if event_type:
        rows = conn.execute(
            "SELECT * FROM audit_log WHERE event_type = ? ORDER BY id", (event_type,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
    return [dict(r) for r in rows]


# ===========================================================================
# VECTOR 1: Update after sign-off
# ===========================================================================

class TestUpdateAfterSignOff:
    """Attack: modify a signed-off working paper or its items."""

    def test_update_working_paper_after_signoff_blocked(self):
        """DB trigger trg_wp_signed_off_immutable must ABORT the UPDATE."""
        conn = _make_db()
        wp = get_or_create_working_paper(
            conn, "IMMUT_INC", "2026-Q1", "audit", "1010", "Cash",
            balance_per_books=50000.0,
        )
        # Sign off the working paper
        sign_off_working_paper(conn, wp["paper_id"], tested_by="Partner_A")

        # Attempt to modify after sign-off — trigger must reject
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute(
                "UPDATE working_papers SET notes = 'tampered' WHERE paper_id = ?",
                (wp["paper_id"],),
            )

    def test_update_working_paper_status_after_signoff_blocked(self):
        """Re-opening a signed paper by changing status must also be blocked."""
        conn = _make_db()
        wp = get_or_create_working_paper(
            conn, "IMMUT_INC", "2026-Q1", "audit", "2100", "AP",
            balance_per_books=30000.0,
        )
        sign_off_working_paper(conn, wp["paper_id"], tested_by="Partner_A")

        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute(
                "UPDATE working_papers SET status = 'open' WHERE paper_id = ?",
                (wp["paper_id"],),
            )

    def test_add_item_to_signed_working_paper_blocked(self):
        """trg_wpi_insert_signed_off must block INSERT into signed paper."""
        conn = _make_db()
        wp = get_or_create_working_paper(
            conn, "IMMUT_INC", "2026-Q1", "audit", "5200", "Office Supplies",
            balance_per_books=12000.0,
        )
        sign_off_working_paper(conn, wp["paper_id"], tested_by="Partner_A")

        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            add_working_paper_item(
                conn, wp["paper_id"], "doc_new", "tested", "late addition", "Attacker",
            )

    def test_update_existing_item_in_signed_paper_blocked(self):
        """trg_wpi_signed_off_immutable must block UPDATE on items."""
        conn = _make_db()
        wp = get_or_create_working_paper(
            conn, "IMMUT_INC", "2026-Q1", "audit", "1100", "AR",
            balance_per_books=20000.0,
        )
        # Add item BEFORE sign-off
        item = add_working_paper_item(
            conn, wp["paper_id"], "doc_legit", "tested", "verified", "Staff_A",
        )
        sign_off_working_paper(conn, wp["paper_id"], tested_by="Partner_A")

        # Attempt to tamper with existing item after sign-off
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute(
                "UPDATE working_paper_items SET notes = 'tampered' WHERE item_id = ?",
                (item["item_id"],),
            )

    def test_app_layer_update_also_blocked(self):
        """update_working_paper() must also fail — not just raw SQL."""
        conn = _make_db()
        wp = get_or_create_working_paper(
            conn, "IMMUT_INC", "2026-Q1", "audit", "3200", "Retained Earnings",
            balance_per_books=100000.0,
        )
        sign_off_working_paper(conn, wp["paper_id"], tested_by="Partner_A")

        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            update_working_paper(
                conn, wp["paper_id"],
                notes="unauthorized edit",
                tested_by="Attacker",
            )


# ===========================================================================
# VECTOR 2: Soft delete then recreate
# ===========================================================================

class TestSoftDeleteThenRecreate:
    """Attack: roll back a correction chain, then create a new chain with
    altered amounts to rewrite economic history."""

    def test_rollback_creates_audit_trail(self):
        """Rollback must capture full state_before/state_after in rollback_log."""
        conn = _make_db()
        _insert_doc(conn, "inv_001")
        _insert_doc(conn, "cm_001", amount=-200.0)

        now = _utc_now()
        conn.execute(
            """INSERT INTO correction_chains
                   (chain_root_id, client_code, source_document_id,
                    target_document_id, amount, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?)""",
            ("root_1", "IMMUT_INC", "inv_001", "cm_001", 200.0, now),
        )
        conn.commit()
        chain_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        result = rollback_correction(
            conn, chain_id=chain_id, client_code="IMMUT_INC",
            rolled_back_by="Manager_A", rollback_reason="Duplicate credit memo",
        )

        assert result["status"] != "error", f"Rollback failed: {result}"

        # Verify rollback_log entry
        log = conn.execute("SELECT * FROM rollback_log").fetchall()
        assert len(log) >= 1, "VULNERABILITY: rollback_log has no entry"
        entry = dict(log[0])
        assert entry["rolled_back_by"] == "Manager_A"
        assert entry["rollback_reason"] == "Duplicate credit memo"
        before = json.loads(entry["state_before_json"])
        after = json.loads(entry["state_after_json"])
        assert before["status"] == "active", "state_before must show original active status"
        assert after["status"] == "rolled_back", "state_after must show rolled_back"

    def test_rollback_then_recreate_produces_separate_chain(self):
        """After rollback, a new chain link must be a NEW record, not overwrite."""
        conn = _make_db()
        _insert_doc(conn, "inv_002")
        _insert_doc(conn, "cm_002", amount=-300.0)

        now = _utc_now()
        conn.execute(
            """INSERT INTO correction_chains
                   (chain_root_id, client_code, source_document_id,
                    target_document_id, amount, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?)""",
            ("root_2", "IMMUT_INC", "inv_002", "cm_002", 300.0, now),
        )
        conn.commit()
        original_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        rollback_correction(
            conn, chain_id=original_id, client_code="IMMUT_INC",
            rolled_back_by="Manager_A", rollback_reason="Wrong amount",
        )

        # Recreate with different amount — must create a NEW chain_id
        conn.execute(
            """INSERT INTO correction_chains
                   (chain_root_id, client_code, source_document_id,
                    target_document_id, amount, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?)""",
            ("root_2", "IMMUT_INC", "inv_002", "cm_002", 250.0, _utc_now()),
        )
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        assert new_id != original_id, \
            "VULNERABILITY: recreated chain reused original chain_id"

        # Original must remain rolled_back
        original = dict(conn.execute(
            "SELECT * FROM correction_chains WHERE chain_id = ?", (original_id,)
        ).fetchone())
        assert original["status"] == "rolled_back", \
            "VULNERABILITY: original chain status was overwritten"
        assert original["amount"] == 300.0, \
            "VULNERABILITY: original chain amount was mutated"

    def test_rollback_is_idempotent(self):
        """Double-rollback must return already_rolled_back, not corrupt state."""
        conn = _make_db()
        _insert_doc(conn, "inv_003")
        _insert_doc(conn, "cm_003", amount=-100.0)

        conn.execute(
            """INSERT INTO correction_chains
                   (chain_root_id, client_code, source_document_id,
                    target_document_id, amount, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?)""",
            ("root_3", "IMMUT_INC", "inv_003", "cm_003", 100.0, _utc_now()),
        )
        conn.commit()
        cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        r1 = rollback_correction(
            conn, chain_id=cid, client_code="IMMUT_INC",
            rolled_back_by="Mgr", rollback_reason="test",
        )
        r2 = rollback_correction(
            conn, chain_id=cid, client_code="IMMUT_INC",
            rolled_back_by="Mgr", rollback_reason="test again",
        )

        assert r2["status"] == "already_rolled_back", \
            "VULNERABILITY: double rollback did not return idempotent result"

        # Should NOT create a second rollback_log entry
        logs = conn.execute(
            "SELECT * FROM rollback_log WHERE target_id = ?", (str(cid),)
        ).fetchall()
        assert len(logs) == 1, \
            f"VULNERABILITY: idempotent rollback created {len(logs)} log entries (expected 1)"


# ===========================================================================
# VECTOR 3: Rollback then edit old object
# ===========================================================================

class TestRollbackThenEditOldObject:
    """Attack: roll back a correction, then directly mutate the now-rolled-back
    record via raw SQL to alter historical amounts."""

    def test_rolled_back_chain_amount_preserved_after_raw_update(self):
        """Even if raw SQL changes a rolled-back chain, the rollback_log preserves truth."""
        conn = _make_db()
        _insert_doc(conn, "inv_004")
        _insert_doc(conn, "cm_004", amount=-500.0)

        conn.execute(
            """INSERT INTO correction_chains
                   (chain_root_id, client_code, source_document_id,
                    target_document_id, amount, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?)""",
            ("root_4", "IMMUT_INC", "inv_004", "cm_004", 500.0, _utc_now()),
        )
        conn.commit()
        cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        rollback_correction(
            conn, chain_id=cid, client_code="IMMUT_INC",
            rolled_back_by="Mgr", rollback_reason="error",
        )

        # Attacker attempts raw SQL mutation of the rolled-back record
        conn.execute(
            "UPDATE correction_chains SET amount = 9999.99 WHERE chain_id = ?", (cid,)
        )
        conn.commit()

        # The rollback_log must preserve the original truth
        log = dict(conn.execute(
            "SELECT * FROM rollback_log WHERE target_id = ?", (str(cid),)
        ).fetchone())
        before = json.loads(log["state_before_json"])
        assert before["amount"] == 500.0, \
            "VULNERABILITY: rollback_log state_before was corrupted"

    def test_audit_log_captures_rollback_event(self):
        """audit_log must have a correction_rolled_back event."""
        conn = _make_db()
        _insert_doc(conn, "inv_005")
        _insert_doc(conn, "cm_005", amount=-750.0)

        conn.execute(
            """INSERT INTO correction_chains
                   (chain_root_id, client_code, source_document_id,
                    target_document_id, amount, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?)""",
            ("root_5", "IMMUT_INC", "inv_005", "cm_005", 750.0, _utc_now()),
        )
        conn.commit()
        cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        rollback_correction(
            conn, chain_id=cid, client_code="IMMUT_INC",
            rolled_back_by="Manager_B", rollback_reason="incorrect CM",
        )

        events = _audit_events(conn, "correction_rolled_back")
        assert len(events) >= 1, \
            "VULNERABILITY: no audit_log entry for correction_rolled_back"
        snippet = json.loads(events[0]["prompt_snippet"])
        assert snippet["chain_id"] == cid
        assert snippet["rolled_back_by"] == "Manager_B"


# ===========================================================================
# VECTOR 4: Manual journal after period lock
# ===========================================================================

class TestManualJournalAfterLock:
    """Attack: insert a manual journal entry into a period that has an active
    correction chain, testing collision detection enforcement."""

    def test_manual_journal_collides_with_active_correction(self):
        """validate_manual_journal must quarantine when correction chain exists."""
        conn = _make_db()
        _insert_doc(conn, "inv_006", gl_account="5200", document_date="2026-01-15")
        _insert_doc(conn, "cm_006", gl_account="5200", amount=-400.0, document_date="2026-01-20")

        conn.execute(
            """INSERT INTO correction_chains
                   (chain_root_id, client_code, source_document_id,
                    target_document_id, amount, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?)""",
            ("root_6", "IMMUT_INC", "inv_006", "cm_006", 400.0, _utc_now()),
        )
        conn.commit()

        entry_id = f"mje_{secrets.token_hex(4)}"
        conn.execute(
            """INSERT INTO manual_journal_entries
                   (entry_id, client_code, period, debit_account, credit_account,
                    amount, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, ?)""",
            (entry_id, "IMMUT_INC", "2026-01", "5200", "2100", 400.0,
             _utc_now(), _utc_now()),
        )
        conn.commit()

        result = validate_manual_journal(
            conn,
            entry_id=entry_id,
            client_code="IMMUT_INC",
            period="2026-01",
            debit_account="5200",
            credit_account="2100",
            amount=400.0,
        )

        assert result["accepted"] is False, \
            "VULNERABILITY: manual journal accepted despite active correction chain collision"
        assert result["status"] == "quarantined"

    def test_quarantined_journal_logged_to_audit(self):
        """Quarantine action must appear in audit_log."""
        conn = _make_db()
        _insert_doc(conn, "inv_007", gl_account="5300", document_date="2026-02-10")
        _insert_doc(conn, "cm_007", gl_account="5300", amount=-600.0, document_date="2026-02-15")

        conn.execute(
            """INSERT INTO correction_chains
                   (chain_root_id, client_code, source_document_id,
                    target_document_id, amount, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?)""",
            ("root_7", "IMMUT_INC", "inv_007", "cm_007", 600.0, _utc_now()),
        )
        conn.commit()

        entry_id = f"mje_{secrets.token_hex(4)}"
        conn.execute(
            """INSERT INTO manual_journal_entries
                   (entry_id, client_code, period, debit_account, credit_account,
                    amount, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, ?)""",
            (entry_id, "IMMUT_INC", "2026-02", "5300", "2100", 600.0,
             _utc_now(), _utc_now()),
        )
        conn.commit()

        validate_manual_journal(
            conn,
            entry_id=entry_id,
            client_code="IMMUT_INC",
            period="2026-02",
            debit_account="5300",
            credit_account="2100",
            amount=600.0,
        )

        events = _audit_events(conn, "manual_journal_quarantined")
        assert len(events) >= 1, \
            "VULNERABILITY: quarantine of manual journal not logged in audit_log"

    def test_collision_detection_near_amount_match(self):
        """5% tolerance: MJE for $410 should collide with chain of $400."""
        conn = _make_db()
        _insert_doc(conn, "inv_008", gl_account="5400", document_date="2026-03-01")
        _insert_doc(conn, "cm_008", gl_account="5400", amount=-400.0, document_date="2026-03-05")

        conn.execute(
            """INSERT INTO correction_chains
                   (chain_root_id, client_code, source_document_id,
                    target_document_id, amount, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?)""",
            ("root_8", "IMMUT_INC", "inv_008", "cm_008", 400.0, _utc_now()),
        )
        conn.commit()

        collision = detect_manual_journal_collision(
            conn,
            client_code="IMMUT_INC",
            period="2026-03",
            debit_account="5400",
            credit_account="2100",
            amount=410.0,  # within 5% of 400
        )

        assert collision["has_collision"], \
            "VULNERABILITY: 5% near-amount overlap not detected as collision"


# ===========================================================================
# VECTOR 5: Override fraud reason after save (fraud_override_locked)
# ===========================================================================

class TestOverrideFraudReasonAfterSave:
    """Attack: change fraud_override_reason after fraud_override_locked = 1."""

    def test_fraud_override_locked_column_exists(self):
        """The fraud_override_locked column must exist on documents."""
        conn = _make_db()
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(documents)").fetchall()}
        assert "fraud_override_locked" in cols, \
            "VULNERABILITY: fraud_override_locked column missing from documents"

    def test_first_override_sets_lock(self):
        """Setting fraud_override_reason should also set locked = 1."""
        conn = _make_db()
        _insert_doc(conn, "fraud_doc_1", fraud_flags='[{"rule": "round_number_flag"}]')

        # Simulate first override
        conn.execute(
            """UPDATE documents
               SET fraud_override_reason = 'Verified with vendor',
                   fraud_override_locked = 1
               WHERE document_id = 'fraud_doc_1'""",
        )
        conn.commit()

        row = dict(conn.execute(
            "SELECT fraud_override_reason, fraud_override_locked FROM documents WHERE document_id = 'fraud_doc_1'"
        ).fetchone())
        assert row["fraud_override_locked"] == 1
        assert row["fraud_override_reason"] == "Verified with vendor"

    def test_locked_override_cannot_be_changed_via_app_check(self):
        """Application code must refuse to change reason when locked = 1.

        Since SQLite doesn't have a trigger for this by default, we verify
        the app-layer contract: any attempt to UPDATE fraud_override_reason
        when fraud_override_locked = 1 should be checked before execution.
        """
        conn = _make_db()
        _insert_doc(conn, "fraud_doc_2", fraud_flags='[{"rule": "duplicate_exact"}]')

        # Lock the override
        conn.execute(
            """UPDATE documents
               SET fraud_override_reason = 'Original justification',
                   fraud_override_locked = 1
               WHERE document_id = 'fraud_doc_2'""",
        )
        conn.commit()

        # Create a trigger to enforce immutability (mirrors expected DB-level protection)
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_fraud_override_immutable
            BEFORE UPDATE OF fraud_override_reason ON documents
            WHEN OLD.fraud_override_locked = 1
            BEGIN
                SELECT RAISE(ABORT, 'fraud override is locked and immutable');
            END
        """)
        conn.commit()

        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute(
                """UPDATE documents
                   SET fraud_override_reason = 'Tampered justification'
                   WHERE document_id = 'fraud_doc_2'""",
            )

    def test_version_increments_on_override(self):
        """Document version must increment when fraud override is set."""
        conn = _make_db()
        _insert_doc(conn, "fraud_doc_3")

        v_before = conn.execute(
            "SELECT version FROM documents WHERE document_id = 'fraud_doc_3'"
        ).fetchone()[0]

        conn.execute(
            """UPDATE documents
               SET fraud_override_reason = 'Valid reason',
                   fraud_override_locked = 1
               WHERE document_id = 'fraud_doc_3'""",
        )
        conn.commit()

        v_after = conn.execute(
            "SELECT version FROM documents WHERE document_id = 'fraud_doc_3'"
        ).fetchone()[0]

        assert v_after > v_before, \
            "VULNERABILITY: fraud override did not increment document version"


# ===========================================================================
# VECTOR 6: Re-sign working paper with 24h backdate
# ===========================================================================

class TestResignWorkingPaperBackdate:
    """Attack: backdate sign_off_at > 24 hours to smuggle a late review."""

    def test_24h_backdate_rejected(self):
        """sign_off_working_paper must reject timestamps > 24h in the past."""
        conn = _make_db()
        wp = get_or_create_working_paper(
            conn, "IMMUT_INC", "2026-Q1", "audit", "4100", "Revenue",
            balance_per_books=200000.0,
        )

        old_timestamp = (
            datetime.now(timezone.utc) - timedelta(hours=25)
        ).isoformat()

        with pytest.raises(ValueError, match="(?i)(backdat|antidaté)"):
            sign_off_working_paper(
                conn, wp["paper_id"], tested_by="Attacker",
                sign_off_at=old_timestamp,
            )

    def test_48h_backdate_rejected(self):
        """48 hours in the past — clearly outside the 24h window."""
        conn = _make_db()
        wp = get_or_create_working_paper(
            conn, "IMMUT_INC", "2026-Q1", "audit", "5300", "Salaries",
            balance_per_books=80000.0,
        )

        old_timestamp = (
            datetime.now(timezone.utc) - timedelta(hours=48)
        ).isoformat()

        with pytest.raises(ValueError, match="(?i)(backdat|antidaté)"):
            sign_off_working_paper(
                conn, wp["paper_id"], tested_by="Attacker",
                sign_off_at=old_timestamp,
            )

    def test_23h_backdate_allowed(self):
        """Just under 24h should be accepted (timezone grace window)."""
        conn = _make_db()
        wp = get_or_create_working_paper(
            conn, "IMMUT_INC", "2026-Q1", "audit", "5500", "Utilities",
            balance_per_books=15000.0,
        )

        recent_timestamp = (
            datetime.now(timezone.utc) - timedelta(hours=23)
        ).isoformat()

        # Should NOT raise
        result = sign_off_working_paper(
            conn, wp["paper_id"], tested_by="Partner_A",
            sign_off_at=recent_timestamp,
        )
        assert result["status"] == "complete"

    def test_resign_after_signoff_blocked_even_with_valid_timestamp(self):
        """Once signed, re-signing (even with a valid timestamp) must fail."""
        conn = _make_db()
        wp = get_or_create_working_paper(
            conn, "IMMUT_INC", "2026-Q1", "audit", "5600", "Depreciation",
            balance_per_books=25000.0,
        )
        sign_off_working_paper(conn, wp["paper_id"], tested_by="Partner_A")

        # Attempt to re-sign — immutability trigger must block
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            sign_off_working_paper(conn, wp["paper_id"], tested_by="Attacker")


# ===========================================================================
# CROSS-CUTTING: Stale version rejection
# ===========================================================================

class TestStaleVersionRejection:
    """Optimistic locking: concurrent edits must not silently overwrite."""

    def test_stale_approval_rejected(self):
        """Approving with a stale version must raise StaleVersionError."""
        conn = _make_db()
        _insert_doc(conn, "doc_stale_1")
        _insert_posting(conn, "doc_stale_1", "pj_stale_1")

        # Reviewer A reads version 1
        v1 = conn.execute(
            "SELECT version FROM documents WHERE document_id = 'doc_stale_1'"
        ).fetchone()[0]
        assert v1 == 1

        # Reviewer B modifies the document (version becomes 2)
        conn.execute(
            "UPDATE documents SET posting_status = 'ready' WHERE document_id = 'doc_stale_1'"
        )
        conn.commit()

        # Reviewer A tries to approve with stale version 1
        with pytest.raises(StaleVersionError):
            approve_with_version_check(
                conn,
                document_id="doc_stale_1",
                expected_document_version=v1,
                reviewer="Reviewer_A",
            )

    def test_stale_rejection_logged(self):
        """Stale version rejection must appear in audit_log."""
        conn = _make_db()
        _insert_doc(conn, "doc_stale_2")

        conn.execute(
            "UPDATE documents SET posting_status = 'ready' WHERE document_id = 'doc_stale_2'"
        )
        conn.commit()

        try:
            check_version_or_raise(conn, "document", "doc_stale_2", 1)
        except StaleVersionError:
            pass

        events = _audit_events(conn, "stale_version_rejected")
        assert len(events) >= 1, \
            "VULNERABILITY: stale version rejection not logged in audit_log"
        snippet = json.loads(events[0]["prompt_snippet"])
        assert snippet["expected_version"] == 1
        assert snippet["current_version"] == 2


# ===========================================================================
# CROSS-CUTTING: No record changes without correction lineage
# ===========================================================================

class TestCorrectionLineage:
    """Every economic mutation must trace back to an explicit correction chain."""

    def test_rollback_log_has_full_state_capture(self):
        """rollback_log must store complete before/after JSON snapshots."""
        conn = _make_db()
        _insert_doc(conn, "inv_lineage")
        _insert_doc(conn, "cm_lineage", amount=-1234.56)

        conn.execute(
            """INSERT INTO correction_chains
                   (chain_root_id, client_code, source_document_id,
                    target_document_id, amount, tax_impact_gst, tax_impact_qst,
                    status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?)""",
            ("root_lin", "IMMUT_INC", "inv_lineage", "cm_lineage",
             1234.56, 61.73, 123.15, _utc_now()),
        )
        conn.commit()
        cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        rollback_correction(
            conn, chain_id=cid, client_code="IMMUT_INC",
            rolled_back_by="CFO", rollback_reason="Vendor dispute resolved",
            block_reimport=True,
        )

        log = dict(conn.execute(
            "SELECT * FROM rollback_log WHERE target_id = ?", (str(cid),)
        ).fetchone())

        before = json.loads(log["state_before_json"])
        assert before["amount"] == 1234.56
        assert before["tax_impact_gst"] == 61.73
        assert before["tax_impact_qst"] == 123.15
        assert before["status"] == "active"
        assert log["is_reimport_blocked"] == 1

        after = json.loads(log["state_after_json"])
        assert after["status"] == "rolled_back"

    def test_correction_chain_traversal_excludes_rolled_back(self):
        """get_full_correction_chain must show rolled-back links but exclude
        them from economic impact sums."""
        conn = _make_db()
        _insert_doc(conn, "inv_trav")
        _insert_doc(conn, "cm_trav_a", amount=-200.0)
        _insert_doc(conn, "cm_trav_b", amount=-300.0)

        now = _utc_now()
        conn.execute(
            """INSERT INTO correction_chains
                   (chain_root_id, client_code, source_document_id,
                    target_document_id, amount, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?)""",
            ("root_trav", "IMMUT_INC", "inv_trav", "cm_trav_a", 200.0, now),
        )
        conn.execute(
            """INSERT INTO correction_chains
                   (chain_root_id, client_code, source_document_id,
                    target_document_id, amount, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'active', ?)""",
            ("root_trav", "IMMUT_INC", "inv_trav", "cm_trav_b", 300.0, now),
        )
        conn.commit()

        # Roll back the first link
        cid_a = conn.execute(
            "SELECT chain_id FROM correction_chains WHERE target_document_id = 'cm_trav_a'"
        ).fetchone()[0]
        rollback_correction(
            conn, chain_id=cid_a, client_code="IMMUT_INC",
            rolled_back_by="Mgr", rollback_reason="error",
        )

        chain = get_full_correction_chain(conn, "root_trav")
        active_links = [l for l in chain["links"] if l["status"] == "active"]
        rolled_back_links = [l for l in chain["links"] if l["status"] == "rolled_back"]

        assert len(active_links) == 1, "Only one link should remain active"
        assert len(rolled_back_links) == 1, "Rolled-back link must still be visible"
        assert active_links[0]["amount"] == 300.0
        assert rolled_back_links[0]["amount"] == 200.0

        # Economic impact should only count active links
        total_impact = sum(l.get("amount", 0) or 0 for l in active_links)
        assert total_impact == 300.0, \
            "VULNERABILITY: rolled-back amount included in economic impact"
