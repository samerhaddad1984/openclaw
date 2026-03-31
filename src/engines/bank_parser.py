"""
src/engines/bank_parser.py
==========================
Bank statement parser for major Quebec banks.

Supported banks (auto-detected from header content or column structure):
    Desjardins, National Bank (Banque Nationale), BMO, TD, RBC

Supported formats:
    CSV  — delimited transaction export
    PDF  — text-based PDF (uses pdfplumber, same as ocr_engine)

Main entry point
----------------
    from src.engines.bank_parser import import_statement

    summary = import_statement(
        file_bytes=...,
        filename="statement.csv",
        client_code="ACME",
        imported_by="sam",
    )

The function:
  1. Detects the bank and parses every transaction line.
  2. Creates one ``documents`` row (doc_type='bank_transaction',
     review_status='New') per transaction.
  3. Runs smart matching against existing non-bank documents:
       • vendor fuzzy similarity ≥ 80 % (difflib)
       • amount within 2 % tolerance
       • date within 7 days
     Matched  → review_status='Ready',  confidence=<score>
     Unmatched→ review_status='NeedsReview', match_reason='no_matching_invoice'
  4. Records each transaction in ``bank_transactions`` and the statement
     header in ``bank_statements``.
  5. Returns a summary dict.
"""
from __future__ import annotations

import csv
import io
import json
import re
import secrets
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

try:
    import pdfplumber  # type: ignore
    _PDF_OK = True
except ImportError:
    _PDF_OK = False

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BankTransaction:
    txn_date: str          # ISO YYYY-MM-DD, or "" if unparseable
    description: str
    debit: float | None    # positive withdrawal amount
    credit: float | None   # positive deposit amount
    balance: float | None
    bank_name: str
    raw_line: str = ""


@dataclass
class ParseResult:
    bank_name: str
    transactions: list[BankTransaction] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Bank detection
# ---------------------------------------------------------------------------

_BANK_KEYWORDS: dict[str, list[str]] = {
    "Desjardins": [
        "desjardins", "caisse populaire", "fédération des caisses",
        "caisses desjardins",
    ],
    "National Bank": [
        "banque nationale", "national bank", "bnc", "bnc.ca",
    ],
    "BMO": [
        "bmo", "bank of montreal", "banque de montréal",
        "banque de montreal",
    ],
    "TD": [
        "td canada trust", "td bank", "td banque", "toronto-dominion",
        "toronto dominion", "tdcanadatrust",
    ],
    "RBC": [
        "royal bank", "rbc", "banque royale", "rbc royal bank",
    ],
}


def _detect_bank_from_text(text: str) -> str:
    lower = text.lower()
    for bank, keywords in _BANK_KEYWORDS.items():
        for kw in keywords:
            if kw in lower:
                return bank
    return "Unknown"


def _detect_bank_from_csv_headers(headers: list[str]) -> str:
    joined = " ".join(h.lower() for h in headers)
    if "retrait" in joined or "dépôt" in joined or "depot" in joined:
        return "Desjardins"
    if "cheque number" in joined or "numéro de chèque" in joined:
        return "RBC"
    if "first bank card" in joined or (
        "transaction type" in joined and "date posted" in joined
    ):
        return "BMO"
    if "date posted" in joined:
        return "TD"
    if "débit" in joined or ("debit" in joined and "credit" not in joined.replace("crédit", "")):
        # distinguish from generic debit/credit
        if "débit" in joined or "crédit" in joined:
            return "National Bank"
    if "debit" in joined and "credit" in joined:
        return "National Bank"
    return "Unknown"


# ---------------------------------------------------------------------------
# Amount / date helpers
# ---------------------------------------------------------------------------

def _parse_amount(value: str) -> float | None:
    """Parse an amount string; returns None for blank or zero.

    Handles:
      English format  1,234.56  → thousands comma, decimal period
      French format   1 234,56  → space thousands, decimal comma
      Negative        (100.00)  → accounting parentheses
    """
    if not value:
        return None
    s = value.strip()
    # Parentheses → negative
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    negative = s.startswith("-")
    if negative:
        s = s[1:]
    # Strip non-numeric except comma, period, space
    s = re.sub(r"[^\d,. ]", "", s).strip()
    if not s:
        return None
    # Determine format by last comma vs last period position
    has_comma = "," in s
    has_period = "." in s
    if has_comma and has_period:
        if s.rfind(".") > s.rfind(","):
            # English: 1,234.56 → remove commas
            s = s.replace(",", "").replace(" ", "")
        else:
            # French: 1.234,56 → remove periods/spaces, comma→period
            s = s.replace(".", "").replace(" ", "").replace(",", ".")
    elif has_comma:
        # French decimal (45,99) or thousands-only (1,234)
        parts = s.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2:
            s = s.replace(",", ".")   # decimal comma
        else:
            s = s.replace(",", "").replace(" ", "")  # thousands
    else:
        s = s.replace(" ", "")

    if not s:
        return None
    try:
        v = float(s)
        if negative:
            v = -v
        return round(v, 2) if v != 0.0 else None
    except ValueError:
        return None


_MONTH_MAP: dict[str, int] = {
    "jan": 1, "janv": 1,
    "fév": 2, "fevr": 2, "fev": 2, "feb": 2,
    "mars": 3, "mar": 3,
    "avr": 4, "apr": 4,
    "mai": 5, "may": 5,
    "juin": 6, "jun": 6,
    "juil": 7, "jul": 7,
    "août": 8, "aout": 8, "aug": 8,
    "sept": 9, "sep": 9,
    "oct": 10,
    "nov": 11,
    "déc": 12, "dec": 12,
}


def _parse_date(value: str) -> str | None:
    """Parse various date formats to ISO YYYY-MM-DD; returns None on failure."""
    value = value.strip()
    if not value:
        return None

    # ISO: 2024-01-15
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", value)
    if m:
        return value

    # YYYY/MM/DD
    m = re.fullmatch(r"(\d{4})/(\d{2})/(\d{2})", value)
    if m:
        return f"{m[1]}-{m[2]}-{m[3]}"

    # DD/MM/YYYY or MM/DD/YYYY
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", value)
    if m:
        a, b, y = int(m[1]), int(m[2]), int(m[3])
        # Prefer DD/MM (Quebec default)
        if 1 <= b <= 12 and 1 <= a <= 31:
            try:
                datetime(y, b, a)
                return f"{y:04d}-{b:02d}-{a:02d}"
            except ValueError:
                pass
        if 1 <= a <= 12 and 1 <= b <= 31:
            try:
                datetime(y, a, b)
                return f"{y:04d}-{a:02d}-{b:02d}"
            except ValueError:
                pass
        return None

    # DD-MM-YYYY
    m = re.fullmatch(r"(\d{1,2})-(\d{1,2})-(\d{4})", value)
    if m:
        a, b, y = int(m[1]), int(m[2]), int(m[3])
        if 1 <= b <= 12 and 1 <= a <= 31:
            try:
                datetime(y, b, a)
                return f"{y:04d}-{b:02d}-{a:02d}"
            except ValueError:
                pass
        return None

    # Written date: "Jan 15, 2024" or "15 janvier 2024"
    m = re.fullmatch(
        r"(\d{1,2})\s+([A-Za-zÀ-ÿ]+)\.?,?\s+(\d{4})",
        value, re.IGNORECASE
    )
    if m:
        day, month_str, year = int(m[1]), m[2].lower()[:4], int(m[3])
        month = _MONTH_MAP.get(month_str)
        if month:
            try:
                datetime(year, month, day)
                return f"{year:04d}-{month:02d}-{day:02d}"
            except ValueError:
                pass

    m = re.fullmatch(
        r"([A-Za-zÀ-ÿ]+)\.?\s+(\d{1,2}),?\s+(\d{4})",
        value, re.IGNORECASE
    )
    if m:
        month_str, day, year = m[1].lower()[:4], int(m[2]), int(m[3])
        month = _MONTH_MAP.get(month_str)
        if month:
            try:
                datetime(year, month, day)
                return f"{year:04d}-{month:02d}-{day:02d}"
            except ValueError:
                pass

    return None


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------

def _norm_header(h: str) -> str:
    return h.strip().lower().replace("\ufeff", "")


def _parse_csv_rows(
    rows: list[list[str]],
    bank_name: str,
    date_col: int,
    desc_col: int,
    debit_col: int | None,
    credit_col: int | None,
    amount_col: int | None,
    balance_col: int | None,
) -> list[BankTransaction]:
    """Generic CSV row iterator; row 0 is always the header (skipped)."""
    txns: list[BankTransaction] = []
    for row in rows[1:]:
        if not row:
            continue
        raw = ",".join(row)
        try:
            if date_col >= len(row) or desc_col >= len(row):
                continue
            raw_date = row[date_col].strip()
            txn_date = _parse_date(raw_date) or ""
            if not txn_date:
                continue
            desc = row[desc_col].strip()
            if not desc:
                continue

            debit: float | None = None
            credit: float | None = None
            balance: float | None = None

            if debit_col is not None and debit_col < len(row):
                debit = _parse_amount(row[debit_col])
            if credit_col is not None and credit_col < len(row):
                credit = _parse_amount(row[credit_col])
            if amount_col is not None and amount_col < len(row):
                amt = _parse_amount(row[amount_col])
                if amt is not None:
                    if amt < 0:
                        debit = abs(amt)
                    else:
                        credit = amt
            if balance_col is not None and balance_col < len(row):
                balance = _parse_amount(row[balance_col])

            if debit is None and credit is None:
                continue

            txns.append(BankTransaction(
                txn_date=txn_date,
                description=desc,
                debit=debit,
                credit=credit,
                balance=balance,
                bank_name=bank_name,
                raw_line=raw,
            ))
        except Exception:
            continue
    return txns


def _parse_csv(content: bytes) -> ParseResult:
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return ParseResult(bank_name="Unknown", errors=["Empty CSV file"])

    headers = [_norm_header(h) for h in rows[0]]
    bank_name = _detect_bank_from_csv_headers(headers)

    def _col(*keys: str) -> int | None:
        for k in keys:
            idx = next((i for i, h in enumerate(headers) if k in h), None)
            if idx is not None:
                return idx
        return None

    def _col_exact(*keys: str) -> int | None:
        for k in keys:
            idx = next((i for i, h in enumerate(headers) if h == k), None)
            if idx is not None:
                return idx
        return None

    if bank_name == "Desjardins":
        txns = _parse_csv_rows(
            rows, bank_name,
            date_col=_col("date") or 0,
            desc_col=_col("description", "libellé", "libelle") or 1,
            debit_col=_col("retrait"),
            credit_col=_col("dépôt", "depot"),
            amount_col=None,
            balance_col=_col("solde"),
        )

    elif bank_name == "National Bank":
        txns = _parse_csv_rows(
            rows, bank_name,
            date_col=_col("date") or 0,
            desc_col=_col("description") or 1,
            debit_col=_col_exact("débit", "debit"),
            credit_col=_col_exact("crédit", "credit"),
            amount_col=None,
            balance_col=_col("solde", "balance"),
        )

    elif bank_name == "RBC":
        # "Account Type","Account Number","Transaction Date","Cheque Number",
        # "Description 1","Description 2","CAD$","USD$"
        txns = _parse_csv_rows(
            rows, bank_name,
            date_col=_col("transaction date", "date") or 2,
            desc_col=_col("description 1", "description") or 4,
            debit_col=None,
            credit_col=None,
            amount_col=_col("cad$", "cad", "amount"),
            balance_col=None,
        )

    elif bank_name == "BMO":
        # "First Bank Card","Transaction Type","Date Posted",
        # "Transaction Amount","Description"
        txns = _parse_csv_rows(
            rows, bank_name,
            date_col=_col("date") or 2,
            desc_col=_col("description") or 4,
            debit_col=None,
            credit_col=None,
            amount_col=_col("amount") or 3,
            balance_col=None,
        )

    elif bank_name == "TD":
        txns = _parse_csv_rows(
            rows, bank_name,
            date_col=_col("date") or 0,
            desc_col=_col("description", "type", "transaction") or 1,
            debit_col=None,
            credit_col=None,
            amount_col=_col("amount", "montant") or 2,
            balance_col=_col("balance", "solde"),
        )

    else:
        # Generic fallback
        txns = _parse_csv_rows(
            rows, bank_name,
            date_col=_col("date") or 0,
            desc_col=_col("description", "libellé", "memo", "payee", "narration") or 1,
            debit_col=_col("debit", "débit", "retrait", "withdrawal"),
            credit_col=_col("credit", "crédit", "dépôt", "depot", "deposit"),
            amount_col=_col("amount", "montant"),
            balance_col=_col("balance", "solde"),
        )

    return ParseResult(bank_name=bank_name, transactions=txns)


# ---------------------------------------------------------------------------
# PDF parsing
# ---------------------------------------------------------------------------

def _parse_pdf(content: bytes) -> ParseResult:
    if not _PDF_OK:
        return ParseResult(bank_name="Unknown", errors=["pdfplumber not installed"])

    try:
        text_pages: list[str] = []
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                text_pages.append(page.extract_text() or "")
    except Exception as exc:
        return ParseResult(bank_name="Unknown", errors=[f"PDF read error: {exc}"])

    full_text = "\n".join(text_pages)
    bank_name = _detect_bank_from_text(full_text)
    txns: list[BankTransaction] = []

    for line in full_text.splitlines():
        line = line.strip()
        if not line:
            continue

        # Require at least one amount at the end
        amounts_in_line = re.findall(r"-?\d[\d,.]*\d|-?\d", line)
        if not amounts_in_line:
            continue

        # Extract date at line start
        raw_date: str | None = None
        rest: str = line

        iso_m = re.match(r"^(\d{4}-\d{2}-\d{2})\s+(.*)", line)
        slashdate_m = re.match(r"^(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})\s+(.*)", line)
        written_m = re.match(
            r"^(\d{1,2}\s+[A-Za-zÀ-ÿ]+\.?\s+\d{4}|[A-Za-zÀ-ÿ]+\.?\s+\d{1,2},?\s+\d{4})\s+(.*)",
            line, re.IGNORECASE
        )

        if iso_m:
            raw_date, rest = iso_m.group(1), iso_m.group(2)
        elif slashdate_m:
            raw_date, rest = slashdate_m.group(1), slashdate_m.group(2)
        elif written_m:
            raw_date, rest = written_m.group(1), written_m.group(2)

        if raw_date is None:
            continue

        txn_date = _parse_date(raw_date)
        if not txn_date:
            continue

        # Amounts from end of rest
        line_amounts = re.findall(r"-?\d[\d,.]*\d|-?\d", rest)
        if not line_amounts:
            continue

        parsed_amounts = [
            _parse_amount(a) for a in line_amounts if _parse_amount(a) is not None
        ]
        if not parsed_amounts:
            continue

        # Description = rest minus trailing amount tokens
        desc = rest
        for amt_str in reversed(line_amounts):
            idx = desc.rfind(amt_str)
            if idx != -1:
                desc = desc[:idx]
        desc = re.sub(r"\s+", " ", desc).strip(" ,.-|")
        if not desc:
            desc = "Bank Transaction"

        debit: float | None = None
        credit: float | None = None
        balance: float | None = None

        if len(parsed_amounts) >= 2:
            balance = parsed_amounts[-1]
            amt = parsed_amounts[-2]
            if amt is not None and amt < 0:
                debit = abs(amt)
            elif amt is not None:
                credit = amt
        elif parsed_amounts:
            amt = parsed_amounts[0]
            if amt is not None and amt < 0:
                debit = abs(amt)
            elif amt is not None:
                credit = amt

        if debit is None and credit is None:
            continue

        txns.append(BankTransaction(
            txn_date=txn_date,
            description=desc,
            debit=debit,
            credit=credit,
            balance=balance,
            bank_name=bank_name,
            raw_line=line,
        ))

    return ParseResult(bank_name=bank_name, transactions=txns)


# ---------------------------------------------------------------------------
# Format dispatch
# ---------------------------------------------------------------------------

def parse_statement(file_bytes: bytes, filename: str = "") -> ParseResult:
    """Detect file format (PDF or CSV) and return a ParseResult."""
    if not file_bytes:
        return ParseResult(bank_name="Unknown", errors=["Empty file"])

    # PDF magic bytes
    if file_bytes[:4] == b"%PDF" or filename.lower().endswith(".pdf"):
        return _parse_pdf(file_bytes)

    return _parse_csv(file_bytes)


# ---------------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------------

def _ensure_bank_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bank_statements (
            statement_id      TEXT PRIMARY KEY,
            bank_name         TEXT,
            file_name         TEXT,
            client_code       TEXT,
            imported_by       TEXT,
            imported_at       TEXT,
            period_start      TEXT,
            period_end        TEXT,
            transaction_count INTEGER DEFAULT 0,
            matched_count     INTEGER DEFAULT 0,
            unmatched_count   INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bank_transactions (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            statement_id        TEXT NOT NULL,
            document_id         TEXT NOT NULL,
            txn_date            TEXT,
            description         TEXT,
            debit               REAL,
            credit              REAL,
            balance             REAL,
            matched_document_id TEXT,
            match_confidence    REAL,
            match_reason        TEXT
        )
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Matching helpers
# ---------------------------------------------------------------------------

FUZZY_THRESHOLD = 0.80
AMOUNT_TOLERANCE = 0.02   # 2 %
DATE_WINDOW_DAYS = 7


def _vendor_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.strip().lower(), b.strip().lower()).ratio()


def _amounts_match(bank_amt: float, doc_amt: float) -> bool:
    if bank_amt == 0 or doc_amt == 0:
        return False
    diff = abs(bank_amt - doc_amt) / max(abs(bank_amt), abs(doc_amt))
    return diff <= AMOUNT_TOLERANCE


def _dates_within(d1: str, d2: str) -> bool:
    try:
        dt1 = datetime.strptime(d1, "%Y-%m-%d").date()
        dt2 = datetime.strptime(d2, "%Y-%m-%d").date()
        return abs((dt1 - dt2).days) <= DATE_WINDOW_DAYS
    except Exception:
        return False


def _find_best_match(
    txn: BankTransaction,
    candidates: list[dict[str, Any]],
) -> tuple[str | None, float]:
    """Return (matched_document_id, confidence) or (None, 0.0)."""
    bank_amt = (txn.debit or 0.0) + (txn.credit or 0.0)
    best_id: str | None = None
    best_score = 0.0

    for doc in candidates:
        doc_amt = doc.get("amount") or 0.0
        doc_date = doc.get("document_date") or ""
        doc_vendor = doc.get("vendor") or ""

        if not _amounts_match(bank_amt, doc_amt):
            continue
        if txn.txn_date and doc_date and not _dates_within(txn.txn_date, doc_date):
            continue

        sim = _vendor_similarity(txn.description, doc_vendor)
        if sim >= FUZZY_THRESHOLD and sim > best_score:
            best_score = sim
            best_id = doc["document_id"]

    return best_id, round(best_score, 4)


def _load_candidate_documents(
    conn: sqlite3.Connection,
    client_code: str,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT document_id, vendor, amount, document_date, doc_type
          FROM documents
         WHERE client_code = ?
           AND doc_type NOT IN ('bank_transaction')
           AND review_status NOT IN ('Ignored', 'Posted')
           AND amount IS NOT NULL
           AND vendor  IS NOT NULL
        """,
        (client_code,),
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Document row creation
# ---------------------------------------------------------------------------

def _new_doc_id() -> str:
    return "doc_" + secrets.token_hex(6)


def _new_statement_id() -> str:
    return "stmt_" + secrets.token_hex(6)


def _create_document_row(
    conn: sqlite3.Connection,
    txn: BankTransaction,
    client_code: str,
    statement_id: str,
    now_iso: str,
) -> str:
    doc_id = _new_doc_id()
    amount = txn.debit if txn.debit is not None else txn.credit
    conn.execute(
        """
        INSERT INTO documents (
            document_id, file_name, file_path, client_code,
            vendor, doc_type, amount, document_date,
            review_status, confidence, raw_result,
            created_at, updated_at, ingest_source
        ) VALUES (
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            'New', NULL, ?,
            ?, ?, ?
        )
        """,
        (
            doc_id,
            f"bank_{statement_id}",
            "",
            client_code,
            txn.description,
            "bank_transaction",
            amount,
            txn.txn_date,
            json.dumps({
                "bank_name": txn.bank_name,
                "debit": txn.debit,
                "credit": txn.credit,
                "balance": txn.balance,
                "raw_line": txn.raw_line,
            }),
            now_iso,
            now_iso,
            f"bank_import:{txn.bank_name}",
        ),
    )
    return doc_id


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def import_statement(
    file_bytes: bytes,
    filename: str,
    client_code: str,
    imported_by: str,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """
    Parse a bank statement, persist to DB, run smart matching.

    Returns a summary dict:
        statement_id, bank_name, transaction_count, matched_count,
        unmatched_count, errors, transactions (list of row dicts)
    """
    result = parse_statement(file_bytes, filename)

    if not result.transactions:
        return {
            "statement_id": None,
            "bank_name": result.bank_name,
            "transaction_count": 0,
            "matched_count": 0,
            "unmatched_count": 0,
            "errors": result.errors or ["No transactions found in statement"],
            "transactions": [],
        }

    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    statement_id = _new_statement_id()

    dates = [t.txn_date for t in result.transactions if t.txn_date]
    period_start = min(dates) if dates else ""
    period_end = max(dates) if dates else ""

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_bank_tables(conn)

        conn.execute(
            """
            INSERT INTO bank_statements
              (statement_id, bank_name, file_name, client_code,
               imported_by, imported_at, period_start, period_end,
               transaction_count, matched_count, unmatched_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0)
            """,
            (
                statement_id, result.bank_name, filename, client_code,
                imported_by, now_iso, period_start, period_end,
            ),
        )

        # Load candidates BEFORE inserting new docs so we don't self-match
        candidates = _load_candidate_documents(conn, client_code)

        matched = 0
        unmatched = 0
        tx_rows: list[dict[str, Any]] = []

        for txn in result.transactions:
            doc_id = _create_document_row(conn, txn, client_code, statement_id, now_iso)
            match_id, confidence = _find_best_match(txn, candidates)

            if match_id:
                review_status = "Ready"
                match_reason: str | None = None
                matched += 1
            else:
                review_status = "NeedsReview"
                match_reason = "no_matching_invoice"
                confidence = 0.0
                unmatched += 1

            conn.execute(
                "UPDATE documents SET review_status=?, confidence=? WHERE document_id=?",
                (review_status, confidence if match_id else None, doc_id),
            )
            conn.execute(
                """
                INSERT INTO bank_transactions
                  (statement_id, document_id, txn_date, description,
                   debit, credit, balance,
                   matched_document_id, match_confidence, match_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    statement_id, doc_id, txn.txn_date, txn.description,
                    txn.debit, txn.credit, txn.balance,
                    match_id, confidence if match_id else None, match_reason,
                ),
            )
            tx_rows.append({
                "document_id": doc_id,
                "txn_date": txn.txn_date,
                "description": txn.description,
                "debit": txn.debit,
                "credit": txn.credit,
                "balance": txn.balance,
                "review_status": review_status,
                "matched_document_id": match_id,
                "match_confidence": confidence if match_id else None,
                "match_reason": match_reason,
            })

        conn.execute(
            """
            UPDATE bank_statements
               SET transaction_count=?, matched_count=?, unmatched_count=?
             WHERE statement_id=?
            """,
            (len(result.transactions), matched, unmatched, statement_id),
        )
        conn.commit()
    finally:
        conn.close()

    return {
        "statement_id": statement_id,
        "bank_name": result.bank_name,
        "transaction_count": len(result.transactions),
        "matched_count": matched,
        "unmatched_count": unmatched,
        "errors": result.errors,
        "transactions": tx_rows,
    }


def get_statement_transactions(
    statement_id: str,
    db_path: Path = DB_PATH,
) -> list[dict[str, Any]]:
    """Load bank_transactions + matching documents row for a statement."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT bt.*,
                   d.review_status,
                   d.confidence
              FROM bank_transactions bt
              JOIN documents d ON d.document_id = bt.document_id
             WHERE bt.statement_id = ?
             ORDER BY bt.txn_date, bt.id
            """,
            (statement_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def apply_manual_match(
    bank_document_id: str,
    invoice_document_id: str,
    db_path: Path = DB_PATH,
) -> None:
    """Manually match a bank transaction document to an invoice document."""
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            UPDATE documents
               SET review_status='Ready', confidence=1.0, updated_at=?
             WHERE document_id=?
            """,
            (now_iso, bank_document_id),
        )
        conn.execute(
            """
            UPDATE bank_transactions
               SET matched_document_id=?, match_confidence=1.0, match_reason=NULL
             WHERE document_id=?
            """,
            (invoice_document_id, bank_document_id),
        )
        conn.commit()
    finally:
        conn.close()
