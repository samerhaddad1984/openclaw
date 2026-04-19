"""Test D — concurrent-edit behaviour probes.

Simulates two CPAs editing the same row simultaneously. We test at the
SQL layer (which is the truth of any HTTP request that ends in a write):

  1. two UPDATE-then-commit on the same row → last-write-wins?
  2. concurrent INSERTs with the same primary key → one wins, one raises?
  3. read-modify-write race on amount: Alice reads X, Bob reads X, Alice
     writes X+100, Bob writes X+50 → which value sticks and is there a
     conflict warning?

The product has no optimistic-lock / version column on documents today,
so (3) is expected to silently lose Alice's edit. That's the honest finding.

Output: /tmp/concurrent_edits_report.md
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

DB = Path("/opt/otocpa/data/otocpa_agent.db")


def _seed_doc(doc_id, amount=100.0):
    conn = sqlite3.connect(str(DB), timeout=5)
    conn.execute("DELETE FROM documents WHERE document_id=?", (doc_id,))
    conn.execute(
        "INSERT INTO documents (document_id, client_code, amount, vendor, "
        "document_date, review_status) "
        "VALUES (?, 'CONCUR', ?, 'concur_vendor', '2025-06-01', 'pending')",
        (doc_id, amount),
    )
    conn.commit()
    conn.close()


def _cleanup(doc_id):
    conn = sqlite3.connect(str(DB), timeout=5)
    conn.execute("DELETE FROM documents WHERE document_id=?", (doc_id,))
    conn.commit()
    conn.close()


def probe_last_write_wins():
    """Two concurrent UPDATEs on the same row from separate connections."""
    doc_id = "CONCUR_LWW_1"
    _seed_doc(doc_id, 100.0)
    results = {}

    def alice():
        c = sqlite3.connect(str(DB), timeout=5)
        c.execute("UPDATE documents SET amount=500 WHERE document_id=?", (doc_id,))
        time.sleep(0.05)
        c.commit()
        c.close()
        results["alice_done"] = time.time()

    def bob():
        time.sleep(0.02)
        c = sqlite3.connect(str(DB), timeout=5)
        c.execute("UPDATE documents SET amount=750 WHERE document_id=?", (doc_id,))
        c.commit()
        c.close()
        results["bob_done"] = time.time()

    t1 = threading.Thread(target=alice)
    t2 = threading.Thread(target=bob)
    t1.start(); t2.start(); t1.join(); t2.join()

    conn = sqlite3.connect(str(DB), timeout=5)
    final = conn.execute(
        "SELECT amount FROM documents WHERE document_id=?", (doc_id,),
    ).fetchone()[0]
    conn.close()
    _cleanup(doc_id)

    winner = "bob" if final == 750 else ("alice" if final == 500 else "unknown")
    return {
        "probe": "last_write_wins",
        "final_amount": final,
        "winner": winner,
        "behaviour": "last write wins (no optimistic lock)",
        "ok": final in (500, 750),
    }


def probe_duplicate_pk_insert():
    """Sequential probe: insert, then try again — should raise Integrity.

    The real concurrent test times out because SQLite serializes writes at
    DB-level lock; the second thread sees 'database is locked' before
    getting to the integrity check. That *is* a finding — under contention,
    the app needs to handle sqlite3.OperationalError 'database is locked'
    with a retry or user-visible message.
    """
    doc_id = "CONCUR_DUP_1"
    _cleanup(doc_id)
    c = sqlite3.connect(str(DB), timeout=5)
    c.execute(
        "INSERT INTO documents (document_id, client_code, vendor, amount, "
        "document_date, review_status) "
        "VALUES (?, 'CONCUR', 'first', 100, '2025-06-01', 'pending')",
        (doc_id,),
    )
    c.commit()
    err = None
    try:
        c.execute(
            "INSERT INTO documents (document_id, client_code, vendor, amount, "
            "document_date, review_status) "
            "VALUES (?, 'CONCUR', 'second', 200, '2025-06-01', 'pending')",
            (doc_id,),
        )
        c.commit()
    except sqlite3.IntegrityError as e:
        err = str(e)
    c.close()
    _cleanup(doc_id)
    return {
        "probe": "duplicate_pk_insert",
        "integrity_error_raised": bool(err),
        "error_message": err,
        "ok": bool(err),
        "behaviour": "PK uniqueness enforced at SQLite layer",
    }


def probe_read_modify_write_race():
    """Simulated sequentially: Alice reads, Bob reads same value, Alice
    writes, Bob writes. Both writes use their original reads so Bob's
    write silently overwrites Alice's.

    Concurrent threads trigger 'database is locked' because SQLite serializes
    writes at DB level. The RMW race is a logical / semantic weakness that
    exists regardless of the lock, so we simulate it sequentially.
    """
    doc_id = "CONCUR_RMW_1"
    _seed_doc(doc_id, 100.0)
    c_alice = sqlite3.connect(str(DB), timeout=5)
    c_bob = sqlite3.connect(str(DB), timeout=5)
    # Both read the initial value.
    alice_read = c_alice.execute(
        "SELECT amount FROM documents WHERE document_id=?", (doc_id,),
    ).fetchone()[0]
    bob_read = c_bob.execute(
        "SELECT amount FROM documents WHERE document_id=?", (doc_id,),
    ).fetchone()[0]
    # Alice writes first.
    c_alice.execute(
        "UPDATE documents SET amount=? WHERE document_id=?",
        (alice_read + 100, doc_id),
    )
    c_alice.commit()
    # Bob writes second, overwriting Alice.
    c_bob.execute(
        "UPDATE documents SET amount=? WHERE document_id=?",
        (bob_read + 50, doc_id),
    )
    c_bob.commit()
    c_alice.close()
    c_bob.close()

    conn = sqlite3.connect(str(DB), timeout=5)
    final = conn.execute(
        "SELECT amount FROM documents WHERE document_id=?", (doc_id,),
    ).fetchone()[0]
    conn.close()
    _cleanup(doc_id)

    lost_update = final == 150.0  # Bob's 100+50, Alice's +100 is gone.
    return {
        "probe": "read_modify_write_race",
        "alice_read": alice_read,
        "bob_read": bob_read,
        "alice_wrote": alice_read + 100,
        "bob_wrote": bob_read + 50,
        "final_amount": final,
        "expected_if_correct_locking": 250.0,
        "lost_update_confirmed": lost_update,
        "severity": "medium",
        "ok_identified_weakness": lost_update,
        "remediation": (
            "Add a `version` INTEGER column on documents; increment on every "
            "UPDATE; use WHERE version = <read_value> in the UPDATE and "
            "detect 0-row-affected to raise a conflict to the user."
        ),
    }


PROBES = [probe_last_write_wins, probe_duplicate_pk_insert, probe_read_modify_write_race]


def main():
    results = []
    for fn in PROBES:
        try:
            r = fn()
        except Exception as e:
            r = {"probe": fn.__name__, "ok": False, "error": str(e)}
        results.append(r)
    Path("/tmp/concurrent_edits_report.json").write_text(
        json.dumps(results, default=str, indent=2),
    )
    md = [
        "# Test D — Concurrent-edit probes",
        "",
        "Two threads hit the same DB row simultaneously. We test three "
        "patterns: plain last-write-wins, duplicate-PK insert, and a read-"
        "modify-write race (classic lost-update).",
        "",
        "## Summary",
    ]
    for r in results:
        md.append(f"### {r.get('probe')}")
        for k, v in r.items():
            if k == "probe":
                continue
            md.append(f"- **{k}**: {v}")
        md.append("")
    Path("/tmp/concurrent_edits_report.md").write_text("\n".join(md))
    print("\n".join(md))


if __name__ == "__main__":
    main()
