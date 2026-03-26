from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DB_PATH = ROOT_DIR / "data" / "ledgerlink_agent.db"


def open_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def detect_columns(conn):

    cur = conn.cursor()
    cur.execute("PRAGMA table_info(documents)")

    cols = [row[1] for row in cur.fetchall()]

    return cols


def resolve_column(cols, *candidates):

    for c in candidates:
        if c in cols:
            return c

    return None


def select_expr(col, alias):

    if col:
        return f"{col} AS {alias}"

    return f"NULL AS {alias}"


def ensure_review_table(conn):

    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS review_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id TEXT,
            file_name TEXT,
            vendor TEXT,
            amount REAL,
            document_date TEXT,
            status TEXT,
            created_at TEXT
        )
        """
    )

    conn.commit()


def load_documents(conn):

    cols = detect_columns(conn)

    id_col = resolve_column(cols, "id", "doc_id", "document_id")
    file_col = resolve_column(cols, "file_name", "filename", "name")
    vendor_col = resolve_column(cols, "vendor")
    amount_col = resolve_column(cols, "amount")
    date_col = resolve_column(cols, "document_date", "date")
    status_col = resolve_column(cols, "review_status", "status")

    query = f"""
    SELECT
        {select_expr(id_col,'doc_id')},
        {select_expr(file_col,'file_name')},
        {select_expr(vendor_col,'vendor')},
        {select_expr(amount_col,'amount')},
        {select_expr(date_col,'document_date')},
        {select_expr(status_col,'review_status')}
    FROM documents
    """

    cur = conn.cursor()
    cur.execute(query)

    return cur.fetchall()


def clear_queue(conn):

    cur = conn.cursor()

    cur.execute("DELETE FROM review_queue")

    conn.commit()


def determine_status(row):

    vendor = row["vendor"]
    amount = row["amount"]
    date = row["document_date"]

    if vendor and amount and date:
        return "Ready"

    return "NeedsReview"


def push_queue(conn, rows):

    cur = conn.cursor()

    inserted = 0

    for r in rows:

        status = determine_status(r)

        cur.execute(
            """
            INSERT INTO review_queue
            (
                document_id,
                file_name,
                vendor,
                amount,
                document_date,
                status,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                r["doc_id"],
                r["file_name"],
                r["vendor"],
                r["amount"],
                r["document_date"],
                status,
            ),
        )

        inserted += 1

    conn.commit()

    return inserted


def run():

    conn = open_db()

    ensure_review_table(conn)

    docs = load_documents(conn)

    clear_queue(conn)

    inserted = push_queue(conn, docs)

    print()
    print("Documents scanned:", len(docs))
    print("Queue items created:", inserted)

    ready = sum(1 for d in docs if determine_status(d) == "Ready")
    review = sum(1 for d in docs if determine_status(d) == "NeedsReview")

    print("Ready:", ready)
    print("NeedsReview:", review)

    conn.close()


if __name__ == "__main__":
    run()