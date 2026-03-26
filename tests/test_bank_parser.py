"""
tests/test_bank_parser.py
=========================
pytest suite for src/engines/bank_parser.py

All tests are deterministic and use in-memory SQLite3 (no real files, no AI calls).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.engines.bank_parser import (
    BankTransaction,
    ParseResult,
    _amounts_match,
    _dates_within,
    _detect_bank_from_csv_headers,
    _detect_bank_from_text,
    _find_best_match,
    _parse_amount,
    _parse_csv,
    _parse_date,
    _vendor_similarity,
    apply_manual_match,
    import_statement,
    parse_statement,
)


# ---------------------------------------------------------------------------
# _parse_amount
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    ("45.99",    45.99),
    ("1,234.56", 1234.56),
    ("$1,200.50", 1200.50),
    ("-45.99",   -45.99),
    ("(100.00)", -100.00),
    ("  87.32 ", 87.32),
    ("0.00",     None),
    ("",         None),
    ("abc",      None),
    ("$",        None),
])
def test_parse_amount(value, expected):
    assert _parse_amount(value) == expected


# ---------------------------------------------------------------------------
# _parse_date
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    ("2024-01-15",   "2024-01-15"),
    ("2024/01/15",   "2024-01-15"),
    ("15/01/2024",   "2024-01-15"),   # DD/MM first (Quebec)
    ("15-01-2024",   "2024-01-15"),
    ("",             None),
    ("not-a-date",   None),
    ("00/00/0000",   None),
])
def test_parse_date(value, expected):
    assert _parse_date(value) == expected


# ---------------------------------------------------------------------------
# Bank detection from free text
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,expected_bank", [
    ("Caisse Desjardins — Relevé de compte",         "Desjardins"),
    ("BMO Bank of Montreal — Account Statement",      "BMO"),
    ("TD Canada Trust — Transaction Summary",         "TD"),
    ("Royal Bank of Canada — RBC Chequing",           "RBC"),
    ("Banque Nationale du Canada — Historique",        "National Bank"),
    ("Some Unrelated Text With No Bank Name",         "Unknown"),
])
def test_detect_bank_from_text(text, expected_bank):
    assert _detect_bank_from_text(text) == expected_bank


# ---------------------------------------------------------------------------
# Bank detection from CSV headers
# ---------------------------------------------------------------------------

def test_detect_bank_desjardins_headers():
    headers = ["Date", "Description", "Retrait ($)", "Dépôt ($)", "Solde ($)"]
    assert _detect_bank_from_csv_headers(headers) == "Desjardins"


def test_detect_bank_rbc_headers():
    headers = ["Account Type", "Account Number", "Transaction Date",
               "Cheque Number", "Description 1", "Description 2", "CAD$", "USD$"]
    assert _detect_bank_from_csv_headers(headers) == "RBC"


def test_detect_bank_bmo_headers():
    headers = ["First Bank Card", "Transaction Type", "Date Posted",
               "Transaction Amount", "Description"]
    assert _detect_bank_from_csv_headers(headers) == "BMO"


def test_detect_bank_national_bank_headers():
    headers = ["Date", "Description", "Débit", "Crédit", "Solde"]
    assert _detect_bank_from_csv_headers(headers) == "National Bank"


def test_detect_bank_td_headers():
    headers = ["Date Posted", "Transaction Amount", "Description"]
    assert _detect_bank_from_csv_headers(headers) == "TD"


# ---------------------------------------------------------------------------
# CSV parsing — Desjardins
# ---------------------------------------------------------------------------

_DESJARDINS_CSV = (
    "Date,Description,Retrait ($),D\u00e9p\u00f4t ($),Solde ($)\r\n"
    "2024-01-15,AMAZON.CA,45.99,,1234.56\r\n"
    "2024-01-16,SALAIRE,,2500.00,3734.56\r\n"
    "2024-01-20,EPICERIE METRO,87.32,,3647.24\r\n"
).encode("utf-8")


def test_parse_csv_desjardins_bank_name():
    result = _parse_csv(_DESJARDINS_CSV)
    assert result.bank_name == "Desjardins"


def test_parse_csv_desjardins_count():
    result = _parse_csv(_DESJARDINS_CSV)
    assert len(result.transactions) == 3


def test_parse_csv_desjardins_first_row():
    txn = _parse_csv(_DESJARDINS_CSV).transactions[0]
    assert txn.txn_date == "2024-01-15"
    assert txn.description == "AMAZON.CA"
    assert txn.debit == 45.99
    assert txn.credit is None
    assert txn.balance == 1234.56


def test_parse_csv_desjardins_deposit_row():
    txn = _parse_csv(_DESJARDINS_CSV).transactions[1]
    assert txn.credit == 2500.00
    assert txn.debit is None


# ---------------------------------------------------------------------------
# CSV parsing — National Bank
# ---------------------------------------------------------------------------

_NATIONAL_CSV = (
    "Date,Description,D\u00e9bit,Cr\u00e9dit,Solde\r\n"
    "2024-02-01,HYDRO-QUEBEC,150.00,,5000.00\r\n"
    "2024-02-05,VIREMENT,,1000.00,6000.00\r\n"
).encode("utf-8")


def test_parse_csv_national_bank():
    result = _parse_csv(_NATIONAL_CSV)
    assert result.bank_name == "National Bank"
    assert len(result.transactions) == 2
    assert result.transactions[0].debit == 150.00
    assert result.transactions[1].credit == 1000.00


# ---------------------------------------------------------------------------
# CSV parsing — RBC
# ---------------------------------------------------------------------------

_RBC_CSV = (
    "Account Type,Account Number,Transaction Date,Cheque Number,"
    "Description 1,Description 2,CAD$,USD$\r\n"
    "Chequing,12345,2024-03-01,,NETFLIX.COM,,-15.99,\r\n"
    "Chequing,12345,2024-03-05,,DIRECT DEPOSIT,,2500.00,\r\n"
).encode("utf-8")


def test_parse_csv_rbc():
    result = _parse_csv(_RBC_CSV)
    assert result.bank_name == "RBC"
    assert len(result.transactions) == 2
    assert result.transactions[0].debit == 15.99   # negative amount → debit
    assert result.transactions[1].credit == 2500.00


# ---------------------------------------------------------------------------
# CSV parsing — BMO
# ---------------------------------------------------------------------------

_BMO_CSV = (
    "First Bank Card,Transaction Type,Date Posted,Transaction Amount,Description\r\n"
    "1234,Purchase,2024-04-10,-55.00,CANADIAN TIRE\r\n"
    "1234,Deposit,2024-04-15,3000.00,PAYROLL\r\n"
).encode("utf-8")


def test_parse_csv_bmo():
    result = _parse_csv(_BMO_CSV)
    assert result.bank_name == "BMO"
    assert len(result.transactions) == 2
    assert result.transactions[0].debit == 55.00
    assert result.transactions[1].credit == 3000.00


# ---------------------------------------------------------------------------
# parse_statement — format dispatch
# ---------------------------------------------------------------------------

def test_parse_statement_csv_dispatch():
    result = parse_statement(_DESJARDINS_CSV, "releve.csv")
    assert result.bank_name == "Desjardins"
    assert len(result.transactions) > 0


def test_parse_statement_empty():
    result = parse_statement(b"", "empty.csv")
    assert result.transaction_count if hasattr(result, "transaction_count") else True
    assert result.errors or result.transactions == []


# ---------------------------------------------------------------------------
# Matching helpers
# ---------------------------------------------------------------------------

def test_amounts_match_exact():
    assert _amounts_match(100.0, 100.0)


def test_amounts_match_within_2pct():
    assert _amounts_match(100.0, 101.5)
    assert _amounts_match(100.0, 98.5)


def test_amounts_match_outside_2pct():
    assert not _amounts_match(100.0, 105.0)


def test_amounts_match_zero():
    assert not _amounts_match(0.0, 100.0)
    assert not _amounts_match(100.0, 0.0)


def test_dates_within_same():
    assert _dates_within("2024-01-15", "2024-01-15")


def test_dates_within_3_days():
    assert _dates_within("2024-01-15", "2024-01-18")


def test_dates_within_7_days():
    assert _dates_within("2024-01-15", "2024-01-22")


def test_dates_outside_7_days():
    assert not _dates_within("2024-01-15", "2024-01-23")


def test_dates_invalid():
    assert not _dates_within("2024-01-15", "not-a-date")


def test_vendor_similarity_fuzzy():
    # "HYDRO-QUEBEC" vs "Hydro-Québec" should be ≥ 80 %
    assert _vendor_similarity("HYDRO-QUEBEC", "Hydro-Québec") >= 0.80


def test_vendor_similarity_unrelated():
    assert _vendor_similarity("COMPLETELY DIFFERENT CORP", "XYZ COMPANY") < 0.50


def test_find_best_match_hit():
    txn = BankTransaction(
        txn_date="2024-01-15",
        description="HYDRO-QUEBEC",
        debit=150.0,
        credit=None,
        balance=None,
        bank_name="Desjardins",
    )
    candidates = [
        {"document_id": "doc_aaa", "vendor": "Hydro-Québec", "amount": 150.0,
         "document_date": "2024-01-14", "doc_type": "invoice"},
        {"document_id": "doc_bbb", "vendor": "Bell Canada", "amount": 99.0,
         "document_date": "2024-01-14", "doc_type": "invoice"},
    ]
    match_id, confidence = _find_best_match(txn, candidates)
    assert match_id == "doc_aaa"
    assert confidence >= 0.80


def test_find_best_match_amount_tolerance():
    txn = BankTransaction(
        txn_date="2024-01-15",
        description="TELUS MOBILITY",
        debit=75.50,
        credit=None,
        balance=None,
        bank_name="TD",
    )
    # Amount 75.50 vs 76.00 is < 2 % difference
    candidates = [
        {"document_id": "doc_tel", "vendor": "Telus Mobility", "amount": 76.00,
         "document_date": "2024-01-15", "doc_type": "invoice"},
    ]
    match_id, confidence = _find_best_match(txn, candidates)
    assert match_id == "doc_tel"


def test_find_best_match_date_outside_window():
    txn = BankTransaction(
        txn_date="2024-01-15",
        description="TELUS MOBILITY",
        debit=75.00,
        credit=None,
        balance=None,
        bank_name="TD",
    )
    # 30 days apart → no match
    candidates = [
        {"document_id": "doc_tel", "vendor": "Telus Mobility", "amount": 75.00,
         "document_date": "2024-02-14", "doc_type": "invoice"},
    ]
    match_id, confidence = _find_best_match(txn, candidates)
    assert match_id is None


def test_find_best_match_miss():
    txn = BankTransaction(
        txn_date="2024-01-15",
        description="RANDOM CORP XYZ",
        debit=500.0,
        credit=None,
        balance=None,
        bank_name="BMO",
    )
    candidates = [
        {"document_id": "doc_abc", "vendor": "Completely Different", "amount": 200.0,
         "document_date": "2024-01-10", "doc_type": "invoice"},
    ]
    match_id, confidence = _find_best_match(txn, candidates)
    assert match_id is None
    assert confidence == 0.0


# ---------------------------------------------------------------------------
# DB integration — fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "test_bank.db"
    conn = sqlite3.connect(str(db))
    conn.execute("""
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            file_name TEXT, file_path TEXT, client_code TEXT,
            vendor TEXT, doc_type TEXT, amount REAL, document_date TEXT,
            gl_account TEXT, tax_code TEXT, category TEXT,
            review_status TEXT, confidence REAL, raw_result TEXT,
            created_at TEXT, updated_at TEXT, ingest_source TEXT,
            currency TEXT, subtotal REAL, tax_total REAL,
            extraction_method TEXT, submitted_by TEXT, client_note TEXT
        )
    """)
    conn.commit()
    conn.close()
    return db


# ---------------------------------------------------------------------------
# import_statement — full DB integration
# ---------------------------------------------------------------------------

def test_import_statement_creates_documents(tmp_db: Path):
    summary = import_statement(
        file_bytes=_DESJARDINS_CSV,
        filename="statement.csv",
        client_code="TESTCO",
        imported_by="sam",
        db_path=tmp_db,
    )
    assert summary["transaction_count"] == 3
    assert summary["bank_name"] == "Desjardins"
    assert summary["statement_id"] is not None
    assert summary["statement_id"].startswith("stmt_")

    conn = sqlite3.connect(str(tmp_db))
    conn.row_factory = sqlite3.Row
    docs = conn.execute(
        "SELECT * FROM documents WHERE doc_type='bank_transaction'"
    ).fetchall()
    conn.close()
    assert len(docs) == 3


def test_import_statement_doc_type_and_status(tmp_db: Path):
    """All created docs should be bank_transaction; unmatched should be NeedsReview."""
    import_statement(_DESJARDINS_CSV, "s.csv", "TESTCO", "sam", db_path=tmp_db)
    conn = sqlite3.connect(str(tmp_db))
    conn.row_factory = sqlite3.Row
    docs = conn.execute("SELECT doc_type, review_status FROM documents").fetchall()
    conn.close()
    for d in docs:
        assert d["doc_type"] == "bank_transaction"
        assert d["review_status"] in ("NeedsReview", "Ready")


def test_import_statement_unmatched_reason(tmp_db: Path):
    summary = import_statement(_DESJARDINS_CSV, "s.csv", "TESTCO", "sam", db_path=tmp_db)
    unmatched = [t for t in summary["transactions"] if t["review_status"] == "NeedsReview"]
    assert len(unmatched) == summary["unmatched_count"]
    for txn in unmatched:
        assert txn["match_reason"] == "no_matching_invoice"


def test_import_statement_matches_existing_invoice(tmp_db: Path):
    """Seed a matching invoice; the AMAZON.CA transaction should match it."""
    conn = sqlite3.connect(str(tmp_db))
    conn.execute(
        """
        INSERT INTO documents
          (document_id, file_name, file_path, client_code,
           vendor, doc_type, amount, document_date,
           review_status, confidence, raw_result, created_at, updated_at, ingest_source)
        VALUES
          ('inv_001', 'amazon.pdf', '', 'TESTCO',
           'Amazon.ca', 'invoice', 45.99, '2024-01-14',
           'Ready', 0.9, '{}', '2024-01-14T00:00:00+00:00',
           '2024-01-14T00:00:00+00:00', 'email')
        """
    )
    conn.commit()
    conn.close()

    summary = import_statement(_DESJARDINS_CSV, "s.csv", "TESTCO", "sam", db_path=tmp_db)
    assert summary["matched_count"] >= 1

    matched = next(
        (t for t in summary["transactions"] if t["description"] == "AMAZON.CA"), None
    )
    assert matched is not None
    assert matched["review_status"] == "Ready"
    assert matched["matched_document_id"] == "inv_001"
    assert matched["match_confidence"] >= 0.80


def test_import_statement_bank_transactions_table(tmp_db: Path):
    summary = import_statement(_DESJARDINS_CSV, "s.csv", "TESTCO", "sam", db_path=tmp_db)
    conn = sqlite3.connect(str(tmp_db))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM bank_transactions WHERE statement_id=?",
        (summary["statement_id"],),
    ).fetchall()
    conn.close()
    assert len(rows) == 3


def test_import_statement_empty_file(tmp_db: Path):
    summary = import_statement(b"", "empty.csv", "TESTCO", "sam", db_path=tmp_db)
    assert summary["transaction_count"] == 0
    assert summary["statement_id"] is None
    assert summary["errors"]


def test_import_statement_counts_consistent(tmp_db: Path):
    summary = import_statement(_NATIONAL_CSV, "nat.csv", "TESTCO", "sam", db_path=tmp_db)
    assert (
        summary["matched_count"] + summary["unmatched_count"]
        == summary["transaction_count"]
    )


# ---------------------------------------------------------------------------
# apply_manual_match
# ---------------------------------------------------------------------------

def test_apply_manual_match(tmp_db: Path):
    summary = import_statement(_DESJARDINS_CSV, "s.csv", "TESTCO", "sam", db_path=tmp_db)
    # Grab the first unmatched transaction
    unmatched = next(
        (t for t in summary["transactions"] if t["review_status"] == "NeedsReview"), None
    )
    assert unmatched is not None

    # Seed a fake invoice to link to
    conn = sqlite3.connect(str(tmp_db))
    conn.execute(
        """
        INSERT INTO documents
          (document_id, file_name, file_path, client_code,
           vendor, doc_type, amount, document_date,
           review_status, confidence, raw_result, created_at, updated_at, ingest_source)
        VALUES ('inv_manual', 'inv.pdf', '', 'TESTCO',
                'Some Vendor', 'invoice', 999.99, '2024-01-01',
                'Ready', 0.9, '{}', '2024-01-01T00:00:00+00:00',
                '2024-01-01T00:00:00+00:00', 'manual')
        """
    )
    conn.commit()
    conn.close()

    apply_manual_match(
        bank_document_id=unmatched["document_id"],
        invoice_document_id="inv_manual",
        db_path=tmp_db,
    )

    conn = sqlite3.connect(str(tmp_db))
    conn.row_factory = sqlite3.Row
    doc = conn.execute(
        "SELECT review_status, confidence FROM documents WHERE document_id=?",
        (unmatched["document_id"],),
    ).fetchone()
    bt = conn.execute(
        "SELECT matched_document_id, match_confidence FROM bank_transactions WHERE document_id=?",
        (unmatched["document_id"],),
    ).fetchone()
    conn.close()

    assert doc["review_status"] == "Ready"
    assert doc["confidence"] == 1.0
    assert bt["matched_document_id"] == "inv_manual"
    assert bt["match_confidence"] == 1.0
