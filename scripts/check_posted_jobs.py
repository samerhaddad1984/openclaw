import sqlite3
import json

conn = sqlite3.connect("data/ledgerlink_agent.db")
conn.row_factory = sqlite3.Row

rows = conn.execute("""
SELECT
    posting_id,
    document_id,
    external_id,
    posting_status,
    approval_state,
    payload_json
FROM posting_jobs
WHERE posting_status = 'posted'
ORDER BY updated_at DESC
""").fetchall()

results = [dict(r) for r in rows]

print(json.dumps(results, indent=2, ensure_ascii=False))