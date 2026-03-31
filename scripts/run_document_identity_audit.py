from __future__ import annotations

import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"


def open_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def detect_columns(conn) -> list[str]:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(documents)")
    return [row[1] for row in cur.fetchall()]


def resolve_column(cols: list[str], *candidates: str) -> str | None:
    for candidate in candidates:
        if candidate in cols:
            return candidate
    return None


def select_expr(real_col: str | None, alias: str) -> str:
    if real_col:
        return f"{real_col} AS {alias}"
    return f"NULL AS {alias}"


def load_documents(conn):
    cols = detect_columns(conn)

    if not cols:
        raise RuntimeError("documents table was not found or has no readable columns")

    id_col = resolve_column(cols, "id", "doc_id", "document_id")
    file_name_col = resolve_column(cols, "file_name", "filename", "name")
    vendor_col = resolve_column(cols, "vendor", "vendor_name", "supplier", "merchant")
    amount_col = resolve_column(cols, "amount", "total_amount", "gross_amount")
    date_col = resolve_column(cols, "document_date", "date", "invoice_date", "txn_date")
    fp_col = resolve_column(cols, "logical_fingerprint")
    review_col = resolve_column(cols, "review_status", "status")

    query = f"""
    SELECT
        {select_expr(id_col, "doc_id")},
        {select_expr(file_name_col, "file_name")},
        {select_expr(vendor_col, "vendor")},
        {select_expr(amount_col, "amount")},
        {select_expr(date_col, "document_date")},
        {select_expr(fp_col, "logical_fingerprint")},
        {select_expr(review_col, "review_status")}
    FROM documents
    """

    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()


def detect_duplicate_fingerprints(rows):
    index = defaultdict(list)

    for row in rows:
        fp = row["logical_fingerprint"]
        if fp is None or str(fp).strip() == "":
            continue
        index[fp].append(row)

    return {k: v for k, v in index.items() if len(v) > 1}


def detect_missing_vendor(rows):
    problems = []

    for row in rows:
        vendor = row["vendor"]
        if vendor is None or str(vendor).strip() == "":
            problems.append(row)

    return problems


def detect_missing_amount(rows):
    problems = []

    for row in rows:
        if row["amount"] is None:
            problems.append(row)

    return problems


def detect_missing_date(rows):
    problems = []

    for row in rows:
        value = row["document_date"]
        if value is None or str(value).strip() == "":
            problems.append(row)

    return problems


def detect_missing_fingerprint(rows):
    problems = []

    for row in rows:
        value = row["logical_fingerprint"]
        if value is None or str(value).strip() == "":
            problems.append(row)

    return problems


def print_section(title: str):
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)


def safe(row, key: str):
    try:
        return row[key]
    except Exception:
        return None


def report_duplicates(duplicates):
    print_section("DUPLICATE LOGICAL DOCUMENTS")

    if not duplicates:
        print("None")
        return

    for fp, docs in duplicates.items():
        print()
        print("Fingerprint:", fp)

        for row in docs:
            print(
                f"id={safe(row, 'doc_id')} "
                f"file={safe(row, 'file_name')} "
                f"vendor={safe(row, 'vendor')} "
                f"amount={safe(row, 'amount')} "
                f"date={safe(row, 'document_date')}"
            )


def report_simple_rows(title: str, rows):
    print_section(title)

    if not rows:
        print("None")
        return

    for row in rows:
        print(
            f"id={safe(row, 'doc_id')} "
            f"file={safe(row, 'file_name')} "
            f"vendor={safe(row, 'vendor')} "
            f"amount={safe(row, 'amount')} "
            f"date={safe(row, 'document_date')}"
        )


def run():
    conn = open_db()

    try:
        rows = load_documents(conn)

        print()
        print("Documents scanned:", len(rows))

        duplicates = detect_duplicate_fingerprints(rows)
        vendor_missing = detect_missing_vendor(rows)
        amount_missing = detect_missing_amount(rows)
        date_missing = detect_missing_date(rows)
        fingerprint_missing = detect_missing_fingerprint(rows)

        report_duplicates(duplicates)
        report_simple_rows("MISSING VENDOR", vendor_missing)
        report_simple_rows("MISSING AMOUNT", amount_missing)
        report_simple_rows("MISSING DATE", date_missing)
        report_simple_rows("MISSING LOGICAL FINGERPRINT", fingerprint_missing)

        print()
        print("=" * 60)
        print("AUDIT COMPLETE")
        print("=" * 60)

    finally:
        conn.close()


if __name__ == "__main__":
    run()