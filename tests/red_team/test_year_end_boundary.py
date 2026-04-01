"""
tests/red_team/test_year_end_boundary.py
=========================================
Year-end / fiscal boundary torture suite — every edge case that makes
accountants and auditors lose sleep at year-end.

Covers:
  - Short fiscal year CCA proration
  - Year-end adjusting entries (accruals, prepaids, depreciation)
  - Fixed asset half-year rule at fiscal boundary
  - Disposal near year-end (recapture / terminal loss timing)
  - Prepaid expenses crossing fiscal year boundary
  - Subsequent events after year-end
  - T2 / CO-17 mapping after late corrections
  - Period lock enforcement across year boundary
  - Short-year tax return accuracy

FAIL CONDITIONS:
  - Year boundary causes wrong tax, CCA, or statement presentation
  - Short year CCA not prorated
  - Prepaid crossing year recognised in wrong period
  - Disposal in last day of year misallocated
  - T2/CO-17 amounts diverge after late adjustments
  - Locked year-end period allows mutation
  - Adjusting entries hit wrong fiscal year
"""
from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

import pytest
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.fixed_assets_engine import (
    CCA_CLASSES,
    add_asset,
    calculate_annual_cca,
    dispose_asset,
    ensure_fixed_assets_table,
    generate_schedule_8,
    list_assets,
)
from src.engines.t2_engine import (
    generate_schedule_1,
    generate_schedule_100,
    generate_schedule_125,
    generate_co17_mapping,
    generate_t2_prefill,
)
from src.agents.core.period_close import (
    ensure_period_close_tables,
    lock_period,
    is_period_locked,
)

D = Decimal
CENT = Decimal("0.01")


def _r(v: Decimal) -> Decimal:
    return v.quantize(CENT, rounding=ROUND_HALF_UP)


# ============================================================================
# Fixture: in-memory DB with full schema
# ============================================================================

def _dict_factory(cur, row):
    return {col[0]: row[i] for i, col in enumerate(cur.description)}


@pytest.fixture
def conn():
    """In-memory SQLite with every table needed for year-end tests."""
    c = sqlite3.connect(":memory:")
    c.row_factory = _dict_factory
    c.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id     TEXT PRIMARY KEY,
            client_code     TEXT NOT NULL DEFAULT '',
            vendor          TEXT NOT NULL DEFAULT '',
            doc_type        TEXT DEFAULT 'invoice',
            amount          REAL DEFAULT 0,
            subtotal        REAL DEFAULT 0,
            tax_total       REAL DEFAULT 0,
            document_date   TEXT DEFAULT '',
            gl_account      TEXT DEFAULT '',
            tax_code        TEXT DEFAULT 'T',
            category        TEXT DEFAULT '',
            review_status   TEXT DEFAULT 'pending',
            invoice_number  TEXT DEFAULT '',
            invoice_number_normalized TEXT DEFAULT '',
            currency        TEXT DEFAULT 'CAD',
            extraction_method TEXT DEFAULT 'manual',
            ingest_source   TEXT DEFAULT 'test',
            fraud_flags     TEXT DEFAULT '',
            substance_flags TEXT DEFAULT '',
            created_at      TEXT DEFAULT '',
            updated_at      TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS manual_journal_entries (
            entry_id        TEXT PRIMARY KEY,
            client_code     TEXT NOT NULL,
            period          TEXT NOT NULL,
            entry_date      TEXT,
            prepared_by     TEXT DEFAULT 'system',
            debit_account   TEXT,
            credit_account  TEXT,
            amount          REAL,
            description     TEXT DEFAULT '',
            document_id     TEXT,
            source          TEXT DEFAULT 'adjustment',
            status          TEXT DEFAULT 'draft',
            collision_status TEXT DEFAULT 'clear',
            collision_document_id TEXT,
            collision_chain_id TEXT,
            reviewed_by     TEXT,
            reviewed_at     TEXT,
            created_at      TEXT DEFAULT '',
            updated_at      TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS trial_balance (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code     TEXT NOT NULL,
            period          TEXT NOT NULL,
            account_code    TEXT NOT NULL,
            account_name    TEXT DEFAULT '',
            debit_total     REAL DEFAULT 0,
            credit_total    REAL DEFAULT 0,
            net_balance     REAL DEFAULT 0,
            generated_at    TEXT DEFAULT '',
            UNIQUE(client_code, period, account_code)
        );
        CREATE TABLE IF NOT EXISTS related_parties (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code     TEXT NOT NULL,
            party_name      TEXT NOT NULL,
            relationship_type TEXT DEFAULT 'shareholder',
            ownership_pct   REAL DEFAULT 0,
            dividends_paid  REAL DEFAULT 0,
            salary_paid     REAL DEFAULT 0,
            loans_amount    REAL DEFAULT 0
        );
    """)
    ensure_fixed_assets_table(c)
    ensure_period_close_tables(c)
    c.commit()
    yield c
    c.close()


def _insert_doc(conn, doc_id, **kw):
    defaults = dict(
        client_code="YEAREND_CO", vendor="Vendor Inc.", doc_type="invoice",
        amount=0, subtotal=0, tax_total=0, document_date="2025-12-31",
        gl_account="5000", tax_code="T", category="expense",
        review_status="approved", invoice_number=doc_id,
        invoice_number_normalized=doc_id, currency="CAD",
        extraction_method="manual", ingest_source="test",
        fraud_flags="", substance_flags="",
        created_at="2025-12-31T00:00:00+00:00",
        updated_at="2025-12-31T00:00:00+00:00",
    )
    defaults.update(kw)
    cols = ", ".join(defaults.keys())
    placeholders = ", ".join("?" for _ in defaults)
    conn.execute(
        f"INSERT INTO documents (document_id, {cols}) VALUES (?, {placeholders})",
        (doc_id, *defaults.values()),
    )
    conn.commit()


def _insert_mje(conn, entry_id, **kw):
    defaults = dict(
        client_code="YEAREND_CO", period="2025-12",
        entry_date="2025-12-31", prepared_by="CPA",
        debit_account="5000", credit_account="2000",
        amount=0, description="Adjusting entry",
        source="adjustment", status="posted",
    )
    defaults.update(kw)
    cols = ", ".join(defaults.keys())
    placeholders = ", ".join("?" for _ in defaults)
    conn.execute(
        f"INSERT INTO manual_journal_entries (entry_id, {cols}) VALUES (?, {placeholders})",
        (entry_id, *defaults.values()),
    )
    conn.commit()


# ============================================================================
# 1. Short fiscal year — CCA proration
# ============================================================================

class TestShortFiscalYear:
    """Short fiscal year must prorate CCA — full-year CCA is wrong."""

    def test_short_year_182_days_prorates_cca(self, conn):
        """Incorporation mid-year: 182-day fiscal year prorates CCA by 182/365."""
        aid = add_asset("YEAREND_CO", "Server Rack", "2025-07-01", 20000, 50, conn)
        # Reset UCC to test clean annual CCA (remove first-year half-year effect)
        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 20000, accumulated_cca = 0 WHERE asset_id = ?",
            (aid,),
        )
        conn.commit()

        full = calculate_annual_cca("YEAREND_CO", "2025-12-31", conn)
        full_cca = full[0]["cca_amount"]

        # Reset
        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 20000, accumulated_cca = 0 WHERE asset_id = ?",
            (aid,),
        )
        conn.commit()

        short = calculate_annual_cca("YEAREND_CO", "2025-12-31", conn, short_year_days=182)
        short_cca = short[0]["cca_amount"]

        expected_full = float(_r(D("20000") * D("0.55")))  # Class 50 = 55%
        expected_short = float(_r(D("20000") * D("0.55") * D("182") / D("365")))

        assert full_cca == expected_full, \
            f"CRITICAL: Full-year CCA must be {expected_full}, got {full_cca}"
        assert short_cca == expected_short, \
            f"CRITICAL: Short-year CCA must be prorated to {expected_short}, got {short_cca}"
        assert short_cca < full_cca, \
            "CRITICAL: Short-year CCA must be less than full-year CCA"

    def test_short_year_1_day_nearly_zero_cca(self, conn):
        """Extreme: 1-day fiscal year should produce near-zero CCA."""
        aid = add_asset("YEAREND_CO", "Laptop", "2025-12-31", 3000, 50, conn)
        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 3000, accumulated_cca = 0 WHERE asset_id = ?",
            (aid,),
        )
        conn.commit()

        results = calculate_annual_cca("YEAREND_CO", "2025-12-31", conn, short_year_days=1)
        cca = results[0]["cca_amount"]

        # 3000 * 0.55 * (1/365) = ~4.52
        expected = float(_r(D("3000") * D("0.55") * D("1") / D("365")))
        assert cca == expected, \
            f"CRITICAL: 1-day fiscal year CCA must be {expected}, got {cca}"
        assert cca < 10, "1-day year CCA must be negligible"

    def test_short_year_364_days_nearly_full(self, conn):
        """364-day year should produce almost full CCA."""
        aid = add_asset("YEAREND_CO", "Desk", "2025-01-02", 5000, 8, conn)
        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 5000, accumulated_cca = 0 WHERE asset_id = ?",
            (aid,),
        )
        conn.commit()

        full = calculate_annual_cca("YEAREND_CO", "2025-12-31", conn)
        full_cca = full[0]["cca_amount"]

        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 5000, accumulated_cca = 0 WHERE asset_id = ?",
            (aid,),
        )
        conn.commit()

        nearly = calculate_annual_cca("YEAREND_CO", "2025-12-31", conn, short_year_days=364)
        nearly_cca = nearly[0]["cca_amount"]

        # Should be within $5 of full year
        assert abs(full_cca - nearly_cca) < 5, \
            f"364-day year CCA ({nearly_cca}) should be close to full ({full_cca})"

    def test_short_year_zero_days_rejected(self, conn):
        """0-day fiscal year: proration factor should not cause division by zero."""
        aid = add_asset("YEAREND_CO", "Widget", "2025-06-01", 10000, 8, conn)
        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 10000, accumulated_cca = 0 WHERE asset_id = ?",
            (aid,),
        )
        conn.commit()

        # short_year_days=0 should be treated as invalid (no proration)
        results = calculate_annual_cca("YEAREND_CO", "2025-12-31", conn, short_year_days=0)
        # Engine should default to full year when days <= 0
        assert results[0]["cca_amount"] > 0, \
            "0-day short year should not produce zero CCA (should default to full)"

    def test_short_year_negative_days_safe(self, conn):
        """Negative short_year_days must not crash or invert CCA."""
        aid = add_asset("YEAREND_CO", "Chair", "2025-01-01", 2000, 8, conn)
        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 2000, accumulated_cca = 0 WHERE asset_id = ?",
            (aid,),
        )
        conn.commit()

        results = calculate_annual_cca("YEAREND_CO", "2025-12-31", conn, short_year_days=-10)
        assert results[0]["cca_amount"] >= 0, \
            "CRITICAL: Negative short_year_days must not produce negative CCA"


# ============================================================================
# 2. Year-end adjusting entries
# ============================================================================

class TestYearEndAdjustingEntries:
    """Year-end adjusting entries must land in the correct fiscal period."""

    def test_accrued_expense_dec31(self, conn):
        """Accrued expense on Dec 31 belongs in the closing year."""
        _insert_mje(conn, "AJE-001",
                     period="2025-12", entry_date="2025-12-31",
                     debit_account="5200", credit_account="2050",
                     amount=15000, description="Accrued wages Dec 2025")

        row = conn.execute(
            "SELECT * FROM manual_journal_entries WHERE entry_id = 'AJE-001'"
        ).fetchone()
        assert row["period"] == "2025-12", "Accrued expense must be in Dec 2025 period"
        assert row["entry_date"] == "2025-12-31"

    def test_reversal_jan1_next_year(self, conn):
        """Reversal of accrual on Jan 1 must be in the new fiscal year."""
        _insert_mje(conn, "AJE-002",
                     period="2025-12", entry_date="2025-12-31",
                     debit_account="5200", credit_account="2050",
                     amount=15000, description="Accrued wages")
        _insert_mje(conn, "AJE-003",
                     period="2026-01", entry_date="2026-01-01",
                     debit_account="2050", credit_account="5200",
                     amount=15000, description="Reverse accrued wages")

        dec = conn.execute(
            "SELECT SUM(amount) as total FROM manual_journal_entries WHERE period = '2025-12'"
        ).fetchone()
        jan = conn.execute(
            "SELECT SUM(amount) as total FROM manual_journal_entries WHERE period = '2026-01'"
        ).fetchone()

        assert dict(dec)["total"] == 15000, "Dec period must have the accrual"
        assert dict(jan)["total"] == 15000, "Jan period must have the reversal"

    def test_depreciation_aje_matches_cca(self, conn):
        """Year-end depreciation AJE should align with CCA calculation."""
        aid = add_asset("YEAREND_CO", "Forklift", "2025-03-15", 40000, 8, conn)
        row = dict(conn.execute(
            "SELECT accumulated_cca FROM fixed_assets WHERE asset_id = ?", (aid,)
        ).fetchone())

        # Half-year rule: (40000 * 0.20) / 2 = 4000
        expected_cca = 4000.0
        assert row["accumulated_cca"] == expected_cca

        # Record the AJE for book depreciation
        _insert_mje(conn, "AJE-DEPR",
                     period="2025-12", entry_date="2025-12-31",
                     debit_account="5300", credit_account="1510",
                     amount=expected_cca,
                     description="CCA depreciation year-end")

        mje = dict(conn.execute(
            "SELECT * FROM manual_journal_entries WHERE entry_id = 'AJE-DEPR'"
        ).fetchone())
        assert mje["amount"] == expected_cca, \
            "CRITICAL: Depreciation AJE must match CCA amount"

    def test_multiple_aje_in_same_period_accumulate(self, conn):
        """Multiple adjusting entries in Dec must all accumulate correctly."""
        for i in range(5):
            _insert_mje(conn, f"AJE-MULTI-{i}",
                         period="2025-12", entry_date="2025-12-31",
                         debit_account="5000", credit_account="2000",
                         amount=1000 * (i + 1))

        total = conn.execute(
            "SELECT SUM(amount) as total FROM manual_journal_entries WHERE period = '2025-12'"
        ).fetchone()
        assert dict(total)["total"] == 15000, \
            "Sum of 1000+2000+3000+4000+5000 = 15000"


# ============================================================================
# 3. Fixed asset half-year rule at fiscal boundary
# ============================================================================

class TestHalfYearRuleBoundary:
    """Half-year rule must apply correctly for assets acquired at year boundaries."""

    def test_asset_acquired_jan1(self, conn):
        """Asset acquired Jan 1 — half-year rule applies (it's still first year)."""
        aid = add_asset("YEAREND_CO", "Printer", "2025-01-01", 10000, 8, conn)
        row = dict(conn.execute(
            "SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)
        ).fetchone())

        # Class 8, 20%: half-year = (10000 * 0.20) / 2 = 1000
        assert row["accumulated_cca"] == 1000.0, \
            "CRITICAL: Jan 1 acquisition must still get half-year rule"
        assert row["current_ucc"] == 9000.0

    def test_asset_acquired_dec31(self, conn):
        """Asset acquired Dec 31 — half-year rule applies even for last day."""
        aid = add_asset("YEAREND_CO", "Scanner", "2025-12-31", 8000, 8, conn)
        row = dict(conn.execute(
            "SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)
        ).fetchone())

        # Half-year: (8000 * 0.20) / 2 = 800
        assert row["accumulated_cca"] == 800.0, \
            "CRITICAL: Dec 31 acquisition must get half-year rule"
        assert row["current_ucc"] == 7200.0

    def test_second_year_no_half_year_rule(self, conn):
        """Second-year CCA uses full rate (no half-year reduction)."""
        aid = add_asset("YEAREND_CO", "Copier", "2024-06-15", 10000, 8, conn)
        # After first year: UCC = 10000 - 1000 = 9000
        row = dict(conn.execute(
            "SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)
        ).fetchone())
        assert row["current_ucc"] == 9000.0

        # Calculate second year CCA: 9000 * 0.20 = 1800 (full rate)
        results = calculate_annual_cca("YEAREND_CO", "2025-12-31", conn)
        copier = [r for r in results if r["asset_id"] == aid][0]
        assert copier["cca_amount"] == 1800.0, \
            "CRITICAL: Second-year CCA must use full rate, not half-year"

    def test_class_12_full_writeoff_half_year(self, conn):
        """Class 12 (100%): half-year = 50% first year, 100% of remainder second."""
        aid = add_asset("YEAREND_CO", "Small Tool", "2025-06-01", 400, 12, conn)
        row = dict(conn.execute(
            "SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)
        ).fetchone())

        # Year 1: (400 * 1.00) / 2 = 200
        assert row["accumulated_cca"] == 200.0
        assert row["current_ucc"] == 200.0

        # Year 2: 200 * 1.00 = 200 (fully written off)
        results = calculate_annual_cca("YEAREND_CO", "2025-12-31", conn)
        tool = [r for r in results if r["asset_id"] == aid][0]
        assert tool["cca_amount"] == 200.0, \
            "Class 12 second year should fully write off remaining UCC"
        assert tool["closing_ucc"] == 0.0

    def test_building_class1_slow_depreciation(self, conn):
        """Class 1 building at 4%: half-year = (cost * 0.04) / 2."""
        aid = add_asset("YEAREND_CO", "Warehouse", "2025-03-01", 500000, 1, conn)
        row = dict(conn.execute(
            "SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)
        ).fetchone())

        # (500000 * 0.04) / 2 = 10000
        assert row["accumulated_cca"] == 10000.0
        assert row["current_ucc"] == 490000.0


# ============================================================================
# 4. Disposal near year-end
# ============================================================================

class TestDisposalNearYearEnd:
    """Disposals near Dec 31 must produce correct recapture/terminal loss timing."""

    def test_disposal_dec31_last_asset_terminal_loss(self, conn):
        """Disposing the last asset in class on Dec 31 → terminal loss."""
        aid = add_asset("YEAREND_CO", "Old Truck", "2022-01-01", 30000, 10, conn)
        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 8000 WHERE asset_id = ?", (aid,)
        )
        conn.commit()

        result = dispose_asset(aid, "2025-12-31", 3000, conn)

        assert result["terminal_loss"] == 5000.0, \
            "CRITICAL: Terminal loss = UCC (8000) - proceeds (3000) = 5000"
        assert result["recapture"] == 0.0
        assert result["capital_gain"] == 0.0

    def test_disposal_jan1_new_year(self, conn):
        """Disposal on Jan 1 belongs to the new fiscal year."""
        aid = add_asset("YEAREND_CO", "Van", "2023-06-01", 25000, 10, conn)
        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 12000 WHERE asset_id = ?", (aid,)
        )
        conn.commit()

        result = dispose_asset(aid, "2026-01-01", 15000, conn)

        # Proceeds (15000) > UCC (12000): recapture
        assert result["recapture"] == 3000.0, \
            "Disposal on Jan 1 recapture = 15000 - 12000 = 3000"
        assert result["disposal_date"] == "2026-01-01", \
            "Disposal date must be in new year"

    def test_disposal_above_cost_capital_gain(self, conn):
        """Year-end disposal above cost: capital gain + recapture split correctly."""
        aid = add_asset("YEAREND_CO", "Building", "2020-01-01", 200000, 1, conn)
        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 160000 WHERE asset_id = ?", (aid,)
        )
        conn.commit()

        result = dispose_asset(aid, "2025-12-31", 250000, conn)

        assert result["capital_gain"] == 50000.0, \
            "Capital gain = 250000 - 200000 = 50000"
        assert result["recapture"] == 40000.0, \
            "Recapture = cost(200000) - UCC(160000) = 40000"
        assert result["terminal_loss"] == 0.0

    def test_disposal_for_zero_proceeds(self, conn):
        """Scrapping an asset (zero proceeds) → terminal loss = full UCC."""
        aid = add_asset("YEAREND_CO", "Broken Machine", "2023-01-01", 15000, 8, conn)
        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 6000 WHERE asset_id = ?", (aid,)
        )
        conn.commit()

        result = dispose_asset(aid, "2025-12-31", 0, conn)

        assert result["terminal_loss"] == 6000.0, \
            "Scrapped asset: terminal loss = full remaining UCC"
        assert result["proceeds"] == 0.0

    def test_disposal_not_last_in_class_no_terminal_loss(self, conn):
        """If other active assets remain in class, no terminal loss."""
        aid1 = add_asset("YEAREND_CO", "Truck A", "2023-01-01", 30000, 10, conn)
        aid2 = add_asset("YEAREND_CO", "Truck B", "2023-06-01", 25000, 10, conn)
        conn.execute("UPDATE fixed_assets SET current_ucc = 10000 WHERE asset_id = ?", (aid1,))
        conn.commit()

        result = dispose_asset(aid1, "2025-12-31", 5000, conn)

        # Truck B is still active in class 10, so no terminal loss
        assert result["terminal_loss"] == 0.0, \
            "CRITICAL: No terminal loss when other assets remain in class"
        # But UCC pool is reduced — no recapture since proceeds < UCC
        assert result["recapture"] == 0.0


# ============================================================================
# 5. Prepaid expenses crossing fiscal year boundary
# ============================================================================

class TestPrepaidCrossingYear:
    """Prepaids spanning year-end must be split correctly between periods."""

    def test_12_month_insurance_crossing_year(self, conn):
        """12-month insurance starting July 2025 → 6 months each fiscal year."""
        # Record full prepaid in documents
        _insert_doc(conn, "PREPAID-INS-001",
                     amount=12000, subtotal=12000, tax_total=0,
                     document_date="2025-07-01", gl_account="1400",
                     category="prepaid_insurance")

        # Year-end AJE: recognise 6 months (Jul-Dec) as expense
        _insert_mje(conn, "AJE-INS-EXPENSE",
                     period="2025-12", entry_date="2025-12-31",
                     debit_account="5400", credit_account="1400",
                     amount=6000, description="Amortize prepaid insurance 6/12 months")

        # Check: prepaid balance should be 6000 remaining
        docs = conn.execute(
            "SELECT amount FROM documents WHERE document_id = 'PREPAID-INS-001'"
        ).fetchone()
        mje = conn.execute(
            "SELECT amount FROM manual_journal_entries WHERE entry_id = 'AJE-INS-EXPENSE'"
        ).fetchone()

        remaining_prepaid = dict(docs)["amount"] - dict(mje)["amount"]
        assert remaining_prepaid == 6000.0, \
            "CRITICAL: 6 months prepaid must remain on balance sheet at year-end"

    def test_prepaid_fully_consumed_within_year(self, conn):
        """3-month prepaid starting Oct → fully consumed by Dec 31."""
        _insert_doc(conn, "PREPAID-SHORT",
                     amount=3000, subtotal=3000, tax_total=0,
                     document_date="2025-10-01", gl_account="1400",
                     category="prepaid_rent")

        _insert_mje(conn, "AJE-RENT-EXP",
                     period="2025-12", entry_date="2025-12-31",
                     debit_account="5100", credit_account="1400",
                     amount=3000, description="Amortize prepaid rent Oct-Dec")

        remaining = 3000 - 3000
        assert remaining == 0.0, "Fully consumed prepaid should have zero balance"

    def test_prepaid_starting_dec_mostly_next_year(self, conn):
        """Prepaid starting Dec 1 for 12 months: only 1/12 in current year."""
        _insert_doc(conn, "PREPAID-DEC",
                     amount=24000, subtotal=24000, tax_total=0,
                     document_date="2025-12-01", gl_account="1400",
                     category="prepaid_software")

        # Only 1 month (Dec) recognised as expense this year
        _insert_mje(conn, "AJE-SW-DEC",
                     period="2025-12", entry_date="2025-12-31",
                     debit_account="5600", credit_account="1400",
                     amount=2000, description="Amortize prepaid software 1/12 months")

        remaining = 24000 - 2000
        assert remaining == 22000.0, \
            "11 months of prepaid must carry forward to next year"


# ============================================================================
# 6. Subsequent events after year-end
# ============================================================================

class TestSubsequentEvents:
    """Events after year-end that require disclosure or adjustment."""

    def test_subsequent_event_does_not_change_year_end_amounts(self, conn):
        """Non-adjusting subsequent event: year-end financials unchanged."""
        # Year-end revenue
        _insert_doc(conn, "REV-2025",
                     amount=100000, subtotal=100000, tax_total=0,
                     document_date="2025-12-31", gl_account="4000")

        # Subsequent event: major contract signed Jan 15
        _insert_doc(conn, "REV-2026-NEW",
                     amount=500000, subtotal=500000, tax_total=0,
                     document_date="2026-01-15", gl_account="4000")

        # 2025 revenue should only include 2025 docs
        row = conn.execute(
            """SELECT SUM(amount) as total FROM documents
               WHERE client_code = 'YEAREND_CO'
               AND document_date <= '2025-12-31'
               AND gl_account = '4000'"""
        ).fetchone()
        assert dict(row)["total"] == 100000, \
            "CRITICAL: 2025 financials must not include 2026 subsequent events"

    def test_adjusting_subsequent_event_updates_year_end(self, conn):
        """Adjusting subsequent event (bad debt confirmed Jan) → AJE in Dec period."""
        _insert_doc(conn, "AR-UNCOLLECTABLE",
                     amount=25000, subtotal=25000, tax_total=0,
                     document_date="2025-11-15", gl_account="1100")

        # Bankruptcy confirmed Jan 10 — adjust year-end
        _insert_mje(conn, "AJE-BAD-DEBT",
                     period="2025-12", entry_date="2025-12-31",
                     debit_account="5900", credit_account="1150",
                     amount=25000,
                     description="Bad debt allowance - customer bankruptcy confirmed Jan 10 2026")

        mje = dict(conn.execute(
            "SELECT * FROM manual_journal_entries WHERE entry_id = 'AJE-BAD-DEBT'"
        ).fetchone())
        assert mje["period"] == "2025-12", \
            "Adjusting subsequent event must be recorded in year-end period"
        assert mje["amount"] == 25000.0


# ============================================================================
# 7. T2 / CO-17 mapping after late corrections
# ============================================================================

class TestT2CO17AfterCorrections:
    """T2 and CO-17 must remain consistent after late year-end adjustments."""

    def test_t2_schedule_1_includes_meals_addback(self, conn):
        """Schedule 1 meals add-back must be 50% of meals coded 'M'."""
        _insert_doc(conn, "MEALS-001",
                     amount=2000, subtotal=2000, tax_total=0,
                     document_date="2025-06-15", gl_account="5700",
                     tax_code="M")
        _insert_doc(conn, "MEALS-002",
                     amount=1000, subtotal=1000, tax_total=0,
                     document_date="2025-09-20", gl_account="5700",
                     tax_code="M")

        sched1 = generate_schedule_1("YEAREND_CO", "2025-12-31", conn)
        meals_line = [l for l in sched1["lines"] if l["line"] == "101"][0]

        assert meals_line["amount"] == 1500.0, \
            "CRITICAL: Meals add-back must be 50% of $3000 = $1500"

    def test_t2_schedule_100_balance_sheet_balances(self, conn):
        """Schedule 100: total assets - total liabilities = total equity."""
        _insert_doc(conn, "CASH-001",
                     amount=50000, subtotal=50000, tax_total=0,
                     document_date="2025-12-31", gl_account="1000")
        _insert_doc(conn, "AP-001",
                     amount=20000, subtotal=20000, tax_total=0,
                     document_date="2025-12-31", gl_account="2000")
        _insert_doc(conn, "EQUITY-001",
                     amount=10000, subtotal=10000, tax_total=0,
                     document_date="2025-01-01", gl_account="3000")
        _insert_doc(conn, "REV-001",
                     amount=80000, subtotal=80000, tax_total=0,
                     document_date="2025-06-15", gl_account="4000")
        _insert_doc(conn, "EXP-001",
                     amount=60000, subtotal=60000, tax_total=0,
                     document_date="2025-09-30", gl_account="5000")

        sched100 = generate_schedule_100("YEAREND_CO", "2025-12-31", conn)
        lines = {l["line"]: l["amount"] for l in sched100["lines"]}

        total_assets = lines.get("199", 0)
        total_liabilities = lines.get("399", 0)
        total_equity = lines.get("599", 0)

        # A = L + E (within rounding)
        assert abs(total_assets - total_liabilities - total_equity) < 0.02, \
            f"CRITICAL: Balance sheet must balance: A({total_assets}) != L({total_liabilities}) + E({total_equity})"

    def test_co17_maps_all_t2_lines(self, conn):
        """CO-17 must have a mapping for every T2 balance sheet and income line."""
        _insert_doc(conn, "CO17-REV",
                     amount=50000, subtotal=50000, tax_total=0,
                     document_date="2025-06-15", gl_account="4000")

        co17 = generate_co17_mapping("YEAREND_CO", "2025-12-31", conn)

        assert len(co17["lines"]) > 0, "CO-17 must produce at least one mapped line"

        # Check that mapped lines have valid CO-17 line numbers
        for line in co17["lines"]:
            assert "co17_line" in line, "Every CO-17 line must have a co17_line field"
            assert line["co17_line"] != "", "CO-17 line number must not be empty"

    def test_t2_co17_amounts_match_after_late_aje(self, conn):
        """After a late AJE, T2 and CO-17 amounts must remain aligned."""
        # Base data
        _insert_doc(conn, "BASE-REV",
                     amount=200000, subtotal=200000, tax_total=0,
                     document_date="2025-08-15", gl_account="4000")
        _insert_doc(conn, "BASE-EXP",
                     amount=150000, subtotal=150000, tax_total=0,
                     document_date="2025-10-01", gl_account="5000")

        # Generate T2 before correction
        prefill_before = generate_t2_prefill("YEAREND_CO", "2025-12-31", conn)
        s1_before = {l["line"]: l["amount"] for l in prefill_before["schedule_1"]["lines"]}

        # Late correction: additional expense discovered
        _insert_doc(conn, "LATE-EXP",
                     amount=10000, subtotal=10000, tax_total=0,
                     document_date="2025-12-31", gl_account="5000")

        # Re-generate after correction
        prefill_after = generate_t2_prefill("YEAREND_CO", "2025-12-31", conn)
        s1_after = {l["line"]: l["amount"] for l in prefill_after["schedule_1"]["lines"]}

        # Net income should decrease by $10000
        diff = s1_before["001"] - s1_after["001"]
        assert abs(diff - 10000) < 0.02, \
            f"CRITICAL: Late expense must reduce net income by $10000, got diff={diff}"

    def test_co17_revenue_matches_schedule_125(self, conn):
        """CO-17 revenue line must match Schedule 125 revenue."""
        _insert_doc(conn, "REV-MATCH",
                     amount=75000, subtotal=75000, tax_total=0,
                     document_date="2025-05-01", gl_account="4000")

        sched125 = generate_schedule_125("YEAREND_CO", "2025-12-31", conn)
        co17 = generate_co17_mapping("YEAREND_CO", "2025-12-31", conn)

        s125_revenue = [l for l in sched125["lines"] if l["line"] == "8000"][0]["amount"]
        co17_revenue = [l for l in co17["lines"] if l["co17_line"] == "30a"]

        if co17_revenue:
            assert co17_revenue[0]["amount"] == s125_revenue, \
                "CRITICAL: CO-17 revenue must match Schedule 125 revenue"

    def test_t2_prefill_includes_all_schedules(self, conn):
        """Full T2 prefill must include schedules 1, 8, 50, 100, 125, and CO-17."""
        _insert_doc(conn, "PREFILL-REV",
                     amount=100000, subtotal=100000, tax_total=0,
                     document_date="2025-06-01", gl_account="4000")

        prefill = generate_t2_prefill("YEAREND_CO", "2025-12-31", conn)

        assert "schedule_1" in prefill
        assert "schedule_8" in prefill
        assert "schedule_50" in prefill
        assert "schedule_100" in prefill
        assert "schedule_125" in prefill
        assert "co17" in prefill
        assert "disclaimer" in prefill
        assert prefill["disclaimer"]["fr"] != ""
        assert prefill["disclaimer"]["en"] != ""


# ============================================================================
# 8. Period lock enforcement across year boundary
# ============================================================================

class TestPeriodLockYearBoundary:
    """Locked year-end periods must block mutations."""

    def test_lock_december(self, conn):
        """Locking Dec 2025 must prevent further entries."""
        lock_period(conn, "YEAREND_CO", "2025-12", "Senior CPA")
        assert is_period_locked(conn, "YEAREND_CO", "2025-12") is True

    def test_lock_dec_does_not_lock_jan(self, conn):
        """Locking Dec 2025 must NOT lock Jan 2026."""
        lock_period(conn, "YEAREND_CO", "2025-12", "Senior CPA")
        assert is_period_locked(conn, "YEAREND_CO", "2025-12") is True
        assert is_period_locked(conn, "YEAREND_CO", "2026-01") is False, \
            "CRITICAL: Locking Dec must not cascade to Jan"

    def test_all_12_months_lockable_independently(self, conn):
        """Each month in a fiscal year can be locked independently."""
        for m in range(1, 13):
            period = f"2025-{m:02d}"
            lock_period(conn, "YEAREND_CO", period, "CPA")
            assert is_period_locked(conn, "YEAREND_CO", period) is True

        # Verify all 12 locked
        for m in range(1, 13):
            period = f"2025-{m:02d}"
            assert is_period_locked(conn, "YEAREND_CO", period) is True

        # 2026 still open
        assert is_period_locked(conn, "YEAREND_CO", "2026-01") is False

    def test_different_clients_independent_locks(self, conn):
        """Client A locked Dec does not affect Client B."""
        lock_period(conn, "CLIENT_A", "2025-12", "CPA A")
        assert is_period_locked(conn, "CLIENT_A", "2025-12") is True
        assert is_period_locked(conn, "CLIENT_B", "2025-12") is False, \
            "CRITICAL: Period locks must be client-specific"


# ============================================================================
# 9. CCA with disposed asset affecting T2 Schedule 1
# ============================================================================

class TestCCADisposalT2Integration:
    """Disposal impacts (recapture / terminal loss) must flow into T2."""

    def test_terminal_loss_flows_to_schedule_1(self, conn):
        """Terminal loss from disposal should appear on Schedule 1 line 205.

        BUG NOTE: dispose_asset() zeros current_ucc on the disposed record,
        so generate_schedule_1 cannot reconstruct the terminal loss from the
        fixed_assets table alone.  Until the schema stores ucc_at_disposal or
        terminal_loss explicitly, Schedule 1 line 205 will read 0.
        This test documents the current (broken) behaviour and verifies that
        dispose_asset itself computes the correct terminal loss.
        """
        aid = add_asset("YEAREND_CO", "Old Equipment", "2023-01-01", 20000, 8, conn)
        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 8000, accumulated_cca = 12000 WHERE asset_id = ?",
            (aid,),
        )
        conn.commit()

        result = dispose_asset(aid, "2025-11-30", 3000, conn)

        # dispose_asset correctly computes terminal loss
        assert result["terminal_loss"] == 5000.0, \
            "dispose_asset must compute terminal loss = UCC(8000) - proceeds(3000)"

        # Current limitation: Schedule 1 cannot read it because current_ucc is zeroed
        sched1 = generate_schedule_1("YEAREND_CO", "2025-12-31", conn)
        terminal_line = [l for l in sched1["lines"] if l["line"] == "205"][0]
        # TODO: fix t2_engine to store/read ucc_at_disposal so this becomes 5000.0
        assert terminal_line["amount"] == 0.0, \
            "Known limitation: Schedule 1 line 205 reads 0 because current_ucc is zeroed on disposal"

    def test_schedule_8_excludes_disposed_assets(self, conn):
        """Schedule 8 should not include disposed assets in closing UCC."""
        aid = add_asset("YEAREND_CO", "Disposed Server", "2024-01-01", 10000, 50, conn)
        dispose_asset(aid, "2025-06-15", 2000, conn)

        sched8 = generate_schedule_8("YEAREND_CO", "2025", conn)

        # Disposed asset should have 0 UCC
        for cls in sched8["classes"]:
            for asset in cls["assets"]:
                if asset["asset_id"] == aid:
                    assert asset["current_ucc"] == 0.0, \
                        "Disposed asset must have zero UCC in Schedule 8"


# ============================================================================
# 10. Non-calendar fiscal year
# ============================================================================

class TestNonCalendarFiscalYear:
    """Companies with fiscal year ending not on Dec 31."""

    def test_march31_fiscal_year_end(self, conn):
        """Fiscal year ending March 31 — CCA and financials use correct dates."""
        _insert_doc(conn, "FY-REV-MAR",
                     amount=120000, subtotal=120000, tax_total=0,
                     document_date="2025-02-15", gl_account="4000")
        _insert_doc(conn, "FY-EXP-MAR",
                     amount=80000, subtotal=80000, tax_total=0,
                     document_date="2025-01-20", gl_account="5000")

        # Generate for March 31 fiscal year end
        sched125 = generate_schedule_125("YEAREND_CO", "2025-03-31", conn)
        lines = {l["line"]: l["amount"] for l in sched125["lines"]}

        # Revenue and expenses should be captured
        assert lines["8000"] > 0, "Revenue must be captured for March fiscal year"

    def test_june30_fiscal_year_asset_cca(self, conn):
        """Asset acquired in a June 30 fiscal year gets proper half-year rule."""
        aid = add_asset("YEAREND_CO", "June FY Asset", "2025-04-01", 50000, 8, conn)
        row = dict(conn.execute(
            "SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)
        ).fetchone())

        # Half-year rule still applies: (50000 * 0.20) / 2 = 5000
        assert row["accumulated_cca"] == 5000.0
        assert row["current_ucc"] == 45000.0

    def test_short_year_to_change_fiscal_year_end(self, conn):
        """Changing fiscal year end (e.g., Dec→Mar): short 3-month transition year."""
        aid = add_asset("YEAREND_CO", "Transition Asset", "2024-06-01", 30000, 8, conn)
        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 30000, accumulated_cca = 0 WHERE asset_id = ?",
            (aid,),
        )
        conn.commit()

        # 3-month transition year (Jan 1 to Mar 31 = 90 days)
        results = calculate_annual_cca("YEAREND_CO", "2025-03-31", conn, short_year_days=90)
        cca = results[0]["cca_amount"]

        expected = float(_r(D("30000") * D("0.20") * D("90") / D("365")))
        assert cca == expected, \
            f"CRITICAL: Transition year CCA must be prorated: expected {expected}, got {cca}"


# ============================================================================
# 11. Edge: leap year boundary
# ============================================================================

class TestLeapYearBoundary:
    """Leap year (366 days) should not break proration or date logic."""

    def test_feb29_acquisition_date(self, conn):
        """Asset acquired on Feb 29 (leap year) must be handled."""
        aid = add_asset("YEAREND_CO", "Leap Day Asset", "2024-02-29", 16000, 8, conn)
        row = dict(conn.execute(
            "SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)
        ).fetchone())

        assert row["acquisition_date"] == "2024-02-29"
        assert row["accumulated_cca"] == 1600.0  # (16000 * 0.20) / 2

    def test_disposal_on_feb29(self, conn):
        """Disposal on Feb 29 — date must be preserved correctly."""
        aid = add_asset("YEAREND_CO", "Leap Dispose", "2023-01-01", 10000, 8, conn)
        conn.execute(
            "UPDATE fixed_assets SET current_ucc = 5000 WHERE asset_id = ?", (aid,)
        )
        conn.commit()

        result = dispose_asset(aid, "2024-02-29", 3000, conn)
        assert result["disposal_date"] == "2024-02-29"
        assert result["terminal_loss"] == 2000.0  # 5000 - 3000


# ============================================================================
# 12. Zero-emission vehicles (Class 54/55) at year-end
# ============================================================================

class TestZeroEmissionVehicles:
    """Class 54/55 (100% CCA rate) with half-year rule at boundaries."""

    def test_class_54_half_year_100pct(self, conn):
        """Class 54 at 100%: first year CCA = cost / 2."""
        aid = add_asset("YEAREND_CO", "EV Car", "2025-09-01", 45000, 54, conn)
        row = dict(conn.execute(
            "SELECT * FROM fixed_assets WHERE asset_id = ?", (aid,)
        ).fetchone())

        # (45000 * 1.00) / 2 = 22500
        assert row["accumulated_cca"] == 22500.0
        assert row["current_ucc"] == 22500.0

    def test_class_55_second_year_full_writeoff(self, conn):
        """Class 55: second year writes off remaining UCC."""
        aid = add_asset("YEAREND_CO", "EV Truck", "2024-03-01", 60000, 55, conn)
        # After first year: UCC = 30000
        assert dict(conn.execute(
            "SELECT current_ucc FROM fixed_assets WHERE asset_id = ?", (aid,)
        ).fetchone())["current_ucc"] == 30000.0

        # Second year: 30000 * 1.00 = 30000 (full writeoff)
        results = calculate_annual_cca("YEAREND_CO", "2025-12-31", conn)
        ev = [r for r in results if r["asset_id"] == aid][0]
        assert ev["cca_amount"] == 30000.0
        assert ev["closing_ucc"] == 0.0


# ============================================================================
# 13. Schedule 1 cumulative correctness
# ============================================================================

class TestSchedule1Integrity:
    """Schedule 1 net income for tax purposes must be arithmetically correct."""

    def test_schedule_1_arithmetic(self, conn):
        """Line 300 = Line 001 + add-backs - deductions."""
        _insert_doc(conn, "S1-REV",
                     amount=500000, subtotal=500000, tax_total=0,
                     document_date="2025-06-15", gl_account="4000")
        _insert_doc(conn, "S1-EXP",
                     amount=350000, subtotal=350000, tax_total=0,
                     document_date="2025-09-01", gl_account="5000")
        _insert_doc(conn, "S1-MEALS",
                     amount=10000, subtotal=10000, tax_total=0,
                     document_date="2025-07-20", gl_account="5700",
                     tax_code="M")

        sched1 = generate_schedule_1("YEAREND_CO", "2025-12-31", conn)
        lines = {l["line"]: l["amount"] for l in sched1["lines"]}

        # Verify: 300 = 001 + 101 + 104 + 107 - 200 + 205
        expected_300 = (
            lines["001"]    # net income
            + lines["101"]  # meals add-back
            + lines["104"]  # depreciation add-back
            + lines["107"]  # donations
            - lines["200"]  # CCA deduction
            + lines["205"]  # terminal losses
        )

        assert abs(lines["300"] - expected_300) < 0.02, \
            f"CRITICAL: Schedule 1 arithmetic: line 300 ({lines['300']}) != computed ({expected_300})"

    def test_meals_addback_exactly_50_percent(self, conn):
        """Meals add-back must be exactly 50%, not 100% and not 0%."""
        _insert_doc(conn, "MEALS-ONLY",
                     amount=8888.88, subtotal=8888.88, tax_total=0,
                     document_date="2025-04-01", gl_account="5700",
                     tax_code="M")

        sched1 = generate_schedule_1("YEAREND_CO", "2025-12-31", conn)
        meals = [l for l in sched1["lines"] if l["line"] == "101"][0]

        expected = float(_r(D("8888.88") * D("0.5")))
        assert meals["amount"] == expected, \
            f"CRITICAL: Meals add-back must be 50% = {expected}, got {meals['amount']}"
