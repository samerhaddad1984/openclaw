from __future__ import annotations

import sqlite3
from pathlib import Path
from collections import defaultdict

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"


def open_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_patterns_table(conn):

    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS learning_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_code TEXT,
            vendor TEXT,
            doc_type TEXT,
            field_name TEXT,
            learned_value TEXT,
            support_count INTEGER,
            confidence REAL,
            created_at TEXT
        )
        """
    )

    conn.commit()


def load_learning_memory(conn):

    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            client_code,
            vendor,
            doc_type,
            field_name,
            new_value
        FROM learning_memory
        """
    )

    return cur.fetchall()


def rebuild_patterns(conn):

    cur = conn.cursor()

    cur.execute("DELETE FROM learning_patterns")

    rows = load_learning_memory(conn)

    counter = defaultdict(int)

    for r in rows:

        key = (
            r["client_code"],
            r["vendor"],
            r["doc_type"],
            r["field_name"],
            r["new_value"],
        )

        counter[key] += 1

    for (client_code, vendor, doc_type, field, value), support in counter.items():

        confidence = min(0.99, 0.6 + support * 0.05)

        cur.execute(
            """
            INSERT INTO learning_patterns (
                client_code,
                vendor,
                doc_type,
                field_name,
                learned_value,
                support_count,
                confidence,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                client_code,
                vendor,
                doc_type,
                field,
                value,
                support,
                confidence,
            ),
        )

    conn.commit()

    print("Patterns built:", len(counter))


def show_patterns(conn):

    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            vendor,
            field_name,
            learned_value,
            support_count,
            confidence
        FROM learning_patterns
        ORDER BY support_count DESC
        LIMIT 20
        """
    )

    rows = cur.fetchall()

    print("\nTop Learned Patterns\n")

    for r in rows:

        print(
            f"{r['vendor']} | {r['field_name']} → {r['learned_value']} "
            f"(support={r['support_count']} confidence={r['confidence']})"
        )


def run():

    with open_db() as conn:

        ensure_patterns_table(conn)

        rebuild_patterns(conn)

        show_patterns(conn)


if __name__ == "__main__":
    run()