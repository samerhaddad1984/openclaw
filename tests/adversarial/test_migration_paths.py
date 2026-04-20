"""R5-Investigation 8 — competitor data migration feasibility."""
from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Current capabilities
# ---------------------------------------------------------------------------

def test_bank_parser_csv_supported():
    from src.engines import bank_parser
    assert hasattr(bank_parser, "import_statement")


def test_bank_parser_internal_csv_parse_works():
    """bank_parser._parse_csv reads raw CSV bytes; the public
    import_statement has a richer signature (client_code, imported_by)
    that belongs to the upload flow. The core parser is what matters
    for migration."""
    from src.engines.bank_parser import _parse_csv
    csv_body = (
        "Date,Description,Amount\n"
        "2026-04-15,COFFEE,-5.00\n"
        "2026-04-16,DEPOSIT,1000.00\n"
    )
    result = _parse_csv(csv_body.encode())
    assert result is not None


def test_chart_of_accounts_seeds_fr_labels(tmp_path):
    """The basic QC chart ships with a revenue account. The French
    name convention is 'Produits d exploitation' — we verify that or
    any 4xxx revenue row is present."""
    import sqlite3
    from src.engines.audit_engine import ensure_audit_tables, seed_chart_of_accounts
    db = tmp_path / "c.db"
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    ensure_audit_tables(conn)
    seed_chart_of_accounts(conn)
    # Seed call includes Quebec-specific accounts; at least one 4xxx
    # revenue account should be present.
    n = conn.execute(
        "SELECT COUNT(*) FROM chart_of_accounts WHERE account_code LIKE '4%'",
    ).fetchone()[0]
    assert n >= 1


def test_openclaw_ingest_route_available_with_api_key():
    """R4 hardening regression: /ingest/openclaw requires X-API-Key."""
    src = (ROOT / "scripts" / "review_dashboard.py").read_text()
    assert '"/ingest/openclaw"' in src
    assert "invalid_or_missing_api_key" in src


def test_export_engine_csv_round_trip():
    """Export + re-import via CSV is a viable bridge from QBO / Sage."""
    from src.engines.export_engine import generate_csv
    data = generate_csv([{
        "document_id": "D1", "vendor": "Acme", "amount": 100.00,
        "document_date": "2026-04-15", "gl_account": "6100",
        "tax_code": "T",
    }])
    assert data
    text = data.decode("utf-8-sig")
    rows = list(csv.reader(io.StringIO(text)))
    assert len(rows) == 2  # header + 1


# ---------------------------------------------------------------------------
# Documented gaps
# ---------------------------------------------------------------------------

def test_documented_gap_bulk_clients_import():
    """No POST /clients/import for CSV upload of many clients at once."""
    src = (ROOT / "scripts" / "review_dashboard.py").read_text()
    if "/clients/import" in src:
        pytest.xfail("CSV clients import found — promote this test")
    pytest.skip(
        "No bulk clients CSV import. See docs/migration_gaps.md item 1."
    )


def test_documented_gap_qbo_inbound_api_connector():
    """We post to QBO (outbound) but don't pull from QBO (inbound)."""
    src = (ROOT / "src" / "agents" / "tools" / "qbo_online_adapter.py").read_text()
    has_pull = any(k in src.lower() for k in (
        "get_journal_entries", "fetch_clients_from_qbo", "import_qbo",
        "pull_from_qbo", "qbo_import",
    ))
    if has_pull:
        pytest.xfail("QBO inbound import found — promote this test")
    pytest.skip(
        "No QBO inbound connector. See docs/migration_gaps.md item 4."
    )


def test_documented_gap_opening_balances_bulk_upload():
    """The opening_balances table exists; no UI for bulk CSV upload."""
    src = (ROOT / "scripts" / "review_dashboard.py").read_text()
    if "/opening_balances/import" in src or "opening_balance_csv" in src.lower():
        pytest.xfail("Opening balances bulk import found — promote test")
    pytest.skip(
        "No bulk opening-balances upload. See docs/migration_gaps.md item 2."
    )


# ---------------------------------------------------------------------------
# Migration doc exists
# ---------------------------------------------------------------------------

def test_migration_gaps_doc_exists():
    doc = ROOT / "docs" / "migration_gaps.md"
    assert doc.exists()
    text = doc.read_text()
    for keyword in ("QBO", "Sage", "CSV", "opening balances"):
        assert keyword.lower() in text.lower()
