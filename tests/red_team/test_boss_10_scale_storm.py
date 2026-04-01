"""
tests/red_team/test_boss_10_scale_storm.py
==========================================
BOSS FIGHT 10 — 10,000-Transaction Scale Storm.

Mass ingest, duplicate clusters, near-duplicate vendors,
async job simulation, exports, dashboard render data,
and reconciliation at scale.
"""
from __future__ import annotations

import io
import sqlite3
import sys
import time
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.tax_engine import (
    CENT,
    GST_RATE,
    QST_RATE,
    calculate_gst_qst,
    calculate_itc_itr,
    extract_tax_from_total,
    validate_tax_code,
)
from src.engines.reconciliation_engine import (
    add_reconciliation_item,
    calculate_reconciliation,
    create_reconciliation,
    ensure_reconciliation_tables,
)
from src.engines.export_engine import (
    generate_csv,
    generate_sage50,
)
from src.engines.fraud_engine import _normalize_vendor_key
from src.engines.uncertainty_engine import evaluate_uncertainty

_ROUND = lambda v: Decimal(str(v)).quantize(CENT, rounding=ROUND_HALF_UP)

SCALE = 10_000  # Number of transactions


def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _seed_mass_documents(conn: sqlite3.Connection, count: int = SCALE):
    """Create a large batch of documents for scale testing."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY, file_name TEXT, file_path TEXT,
            client_code TEXT, vendor TEXT, doc_type TEXT, amount REAL,
            document_date TEXT, gl_account TEXT, tax_code TEXT,
            category TEXT, review_status TEXT DEFAULT 'approved',
            confidence REAL DEFAULT 0.95, raw_result TEXT,
            submitted_by TEXT, client_note TEXT, fraud_flags TEXT
        );
        CREATE TABLE IF NOT EXISTS posting_jobs (
            posting_id TEXT PRIMARY KEY, document_id TEXT,
            file_name TEXT, file_path TEXT, client_code TEXT,
            vendor TEXT, document_date TEXT, amount REAL,
            currency TEXT DEFAULT 'CAD', doc_type TEXT,
            category TEXT, gl_account TEXT, tax_code TEXT,
            memo TEXT, review_status TEXT DEFAULT 'approved',
            confidence REAL DEFAULT 0.95, blocking_issues TEXT, notes TEXT
        );
    """)

    vendors = [f"Vendor_{i:04d}" for i in range(200)]
    tax_codes = ["T", "M", "E", "Z", "HST", "I"]
    gl_accounts = ["5000", "5100", "5200", "5300", "5400", "6000", "6100"]

    # Batch insert for speed
    doc_rows = []
    pj_rows = []
    for i in range(count):
        doc_id = f"MASS-{i:06d}"
        vendor = vendors[i % len(vendors)]
        tc = tax_codes[i % len(tax_codes)]
        gl = gl_accounts[i % len(gl_accounts)]
        amount = round(100 + (i % 10000) * 0.53, 2)
        month = (i % 12) + 1
        day = (i % 28) + 1
        doc_date = f"2026-{month:02d}-{day:02d}"

        doc_rows.append((doc_id, "SCALE_CO", vendor, amount, gl, tc,
                         doc_date, "invoice", "approved"))
        pj_rows.append((f"pj_{doc_id}", doc_id, "SCALE_CO", vendor, amount,
                        gl, tc, doc_date, "invoice", "approved"))

    conn.executemany(
        """INSERT INTO documents
           (document_id, client_code, vendor, amount, gl_account, tax_code,
            document_date, doc_type, review_status)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        doc_rows,
    )
    conn.executemany(
        """INSERT INTO posting_jobs
           (posting_id, document_id, client_code, vendor, amount, gl_account,
            tax_code, document_date, doc_type, review_status)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        pj_rows,
    )
    conn.commit()


class TestMassIngest:
    """10,000-transaction ingest into the database."""

    def test_mass_insert_completes(self):
        """10k documents must insert in under 10 seconds."""
        conn = _fresh_db()
        start = time.time()
        _seed_mass_documents(conn, SCALE)
        elapsed = time.time() - start

        count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        assert count == SCALE
        assert elapsed < 10.0, f"Mass insert took {elapsed:.1f}s (limit: 10s)"

    def test_mass_query_by_client(self):
        """Client-scoped query over 10k docs must be fast."""
        conn = _fresh_db()
        _seed_mass_documents(conn, SCALE)

        start = time.time()
        rows = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE client_code = ?",
            ("SCALE_CO",),
        ).fetchone()[0]
        elapsed = time.time() - start

        assert rows == SCALE
        assert elapsed < 1.0


class TestDuplicateClusters:
    """Near-duplicate detection at scale."""

    def test_vendor_normalization_at_scale(self):
        """Normalize 10k vendor names — no crashes, consistent output."""
        vendors = [f"Vendor_{i:04d} Inc." for i in range(SCALE)]
        keys = set()
        for v in vendors:
            k = _normalize_vendor_key(v)
            assert k, f"Empty key for vendor: {v}"
            keys.add(k)
        # All should be unique (different vendor numbers)
        assert len(keys) == SCALE

    def test_near_duplicate_vendors_collapse(self):
        """Vendors with minor variations should normalize to the same key."""
        pairs = [
            ("Bell Canada Inc.", "Bell Canada Ltée"),
            ("Hydro-Québec", "Hydro-Quebec"),
            ("Home Depot", "HOME DEPOT INC."),
        ]
        for v1, v2 in pairs:
            assert _normalize_vendor_key(v1) == _normalize_vendor_key(v2), \
                f"{v1} and {v2} should normalize to same key"


class TestTaxAtScale:
    """Tax calculations over 10,000 items — correctness at volume."""

    def test_gst_qst_10k_items(self):
        """calculate_gst_qst on 10k different amounts — no crash, all valid."""
        for i in range(SCALE):
            amount = Decimal(str(100 + i * 0.53))
            result = calculate_gst_qst(amount)
            assert result["total_with_tax"] > amount
            assert result["gst"] >= Decimal("0.01")

    def test_extract_tax_10k_roundtrip(self):
        """Extract → recompute for 10k totals — max drift $0.02."""
        max_diff = Decimal("0")
        for i in range(SCALE):
            total = Decimal(str(115 + i * 0.47))
            extracted = extract_tax_from_total(total)
            recomputed = calculate_gst_qst(extracted["pre_tax"])
            diff = abs(recomputed["total_with_tax"] - total)
            if diff > max_diff:
                max_diff = diff
        assert max_diff <= Decimal("0.02"), f"Max roundtrip diff: {max_diff}"

    def test_itc_itr_10k_mixed_codes(self):
        """ITC/ITR for 10k items with mixed tax codes — all non-negative."""
        codes = ["T", "M", "E", "Z", "HST", "I"]
        for i in range(SCALE):
            code = codes[i % len(codes)]
            amount = Decimal(str(500 + i))
            result = calculate_itc_itr(amount, code)
            assert Decimal(str(result["gst_recoverable"])) >= Decimal("0")
            assert Decimal(str(result["qst_recoverable"])) >= Decimal("0")


class TestReconAtScale:
    """Reconciliation with many items."""

    def test_reconciliation_100_items(self):
        """100 reconciliation items — calculation must complete and be correct."""
        conn = _fresh_db()
        ensure_reconciliation_tables(conn)
        rid = create_reconciliation("SCALE_CO", "Chequing", "2026-03-31",
                                    500000.0, 500000.0, conn)

        total_deposits = Decimal("0")
        total_cheques = Decimal("0")

        for i in range(50):
            amt = round(100 + i * 50.0, 2)
            add_reconciliation_item(
                rid, "deposit_in_transit", f"Deposit #{i}",
                amt, f"2026-03-{(i%28)+1:02d}", conn,
            )
            total_deposits += Decimal(str(amt))

        for i in range(50):
            amt = round(100 + i * 50.0, 2)
            add_reconciliation_item(
                rid, "outstanding_cheque", f"Cheque #{i}",
                amt, f"2026-03-{(i%28)+1:02d}", conn,
            )
            total_cheques += Decimal(str(amt))

        result = calculate_reconciliation(rid, conn)
        assert result is not None
        # Deposits and cheques are equal amounts, so bank adjustments cancel
        assert result["is_balanced"]

    def test_reconciliation_with_errors(self):
        """Reconciliation with bank errors at scale."""
        conn = _fresh_db()
        ensure_reconciliation_tables(conn)
        rid = create_reconciliation("SCALE_CO", "Savings", "2026-03-31",
                                    100000.0, 99000.0, conn)

        # Add bank errors totaling $1000 to balance
        for i in range(10):
            add_reconciliation_item(
                rid, "bank_error", f"Error correction #{i}",
                -100.0, f"2026-03-{i+1:02d}", conn,
            )

        result = calculate_reconciliation(rid, conn)
        # 100000 + (-1000 errors) = 99000 = GL
        assert result["is_balanced"]


class TestExportAtScale:
    """Export large document sets."""

    def test_csv_export_1000_docs(self):
        """CSV export of 1000 docs must produce valid output."""
        docs = []
        for i in range(1000):
            docs.append({
                "document_id": f"EXP-{i:04d}",
                "vendor": f"Vendor {i % 100}",
                "amount": round(100 + i * 1.5, 2),
                "gl_account": "5200",
                "tax_code": "T",
                "document_date": f"2026-03-{(i%28)+1:02d}",
                "doc_type": "invoice",
                "client_code": "SCALE_CO",
                "review_status": "approved",
                "currency": "CAD",
                "category": "supplies",
            })

        output = generate_csv(docs)
        assert isinstance(output, bytes)
        lines = output.decode("utf-8-sig").strip().split("\n")
        # Header + 1000 data rows
        assert len(lines) >= 1001, f"Expected 1001 lines, got {len(lines)}"

    def test_sage50_export_500_docs(self):
        """Sage50 export of 500 docs must produce bytes."""
        docs = [{
            "document_id": f"S50-{i:04d}",
            "vendor": f"Vendor {i}",
            "amount": round(200 + i * 2.0, 2),
            "gl_account": "5200",
            "tax_code": "T",
            "document_date": f"2026-03-{(i%28)+1:02d}",
            "doc_type": "invoice",
            "client_code": "SCALE_CO",
            "review_status": "approved",
            "currency": "CAD",
            "category": "supplies",
        } for i in range(500)]

        output = generate_sage50(docs)
        assert isinstance(output, bytes)
        assert len(output) > 1000


class TestUncertaintyAtScale:
    """Uncertainty engine over 10k evaluations."""

    def test_evaluate_uncertainty_10k(self):
        """10k uncertainty evaluations — consistent results."""
        safe_count = 0
        block_count = 0
        partial_count = 0

        for i in range(SCALE):
            # Vary confidence levels cyclically
            vendor_conf = 0.50 + (i % 50) / 100.0  # 0.50 to 0.99
            amount_conf = 0.60 + (i % 40) / 100.0
            gl_conf = 0.70 + (i % 30) / 100.0

            state = evaluate_uncertainty({
                "vendor": vendor_conf,
                "amount": amount_conf,
                "gl_account": gl_conf,
            })

            if state.can_post:
                safe_count += 1
            elif state.must_block:
                block_count += 1
            else:
                partial_count += 1

        # All 10k should be categorized
        assert safe_count + block_count + partial_count == SCALE
        # With the range 0.50-0.99, we should have some of each
        assert block_count > 0, "Expected some blocked items"
        assert safe_count > 0 or partial_count > 0, "Expected some passable items"


class TestDashboardRenderData:
    """Dashboard-style aggregation queries over 10k docs."""

    def test_monthly_summary_aggregation(self):
        """Aggregate 10k docs by month — must complete and be accurate."""
        conn = _fresh_db()
        _seed_mass_documents(conn, SCALE)

        rows = conn.execute("""
            SELECT substr(document_date, 1, 7) as month,
                   COUNT(*) as doc_count,
                   SUM(amount) as total_amount
            FROM documents
            WHERE client_code = 'SCALE_CO'
            GROUP BY month
            ORDER BY month
        """).fetchall()

        total_docs = sum(r["doc_count"] for r in rows)
        assert total_docs == SCALE
        assert len(rows) == 12  # All 12 months should have data

    def test_vendor_summary(self):
        """Top vendors by volume over 10k docs."""
        conn = _fresh_db()
        _seed_mass_documents(conn, SCALE)

        rows = conn.execute("""
            SELECT vendor, COUNT(*) as cnt, SUM(amount) as total
            FROM documents
            WHERE client_code = 'SCALE_CO'
            GROUP BY vendor
            ORDER BY cnt DESC
            LIMIT 10
        """).fetchall()

        assert len(rows) == 10
        assert all(r["cnt"] > 0 for r in rows)

    def test_tax_code_distribution(self):
        """Tax code distribution over 10k docs."""
        conn = _fresh_db()
        _seed_mass_documents(conn, SCALE)

        rows = conn.execute("""
            SELECT tax_code, COUNT(*) as cnt
            FROM documents
            WHERE client_code = 'SCALE_CO'
            GROUP BY tax_code
            ORDER BY cnt DESC
        """).fetchall()

        total = sum(r["cnt"] for r in rows)
        assert total == SCALE
        # We used 6 tax codes cyclically
        assert len(rows) == 6
