import hashlib
import sqlite3
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "data" / "otocpa_agent.db"

print(f"DB: {DB_PATH}")
print(f"DB exists: {DB_PATH.exists()}")
print()

conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row

# Step 1: check user lookup
print("=== Step 1: user lookup ===")
username = "sam"
row = conn.execute(
    "SELECT * FROM dashboard_users WHERE username=? AND active=1", (username,)
).fetchone()
if row:
    print(f"  Found user: {dict(row)}")
else:
    print("  USER NOT FOUND or not active!")

print()

# Step 2: check password
print("=== Step 2: password check ===")
password = "SamOwner123!"
stored_hash = row["password_hash"] if row else ""
computed_hash = hashlib.sha256(password.encode()).hexdigest()
print(f"  Stored  : {stored_hash}")
print(f"  Computed: {computed_hash}")
print(f"  Match   : {stored_hash == computed_hash}")

print()

# Step 3: try creating a session
print("=== Step 3: session insert ===")
try:
    token = secrets.token_hex(32)
    now = datetime.now(timezone.utc)
    expires = (now + timedelta(hours=12)).replace(microsecond=0).isoformat()
    created = now.replace(microsecond=0).isoformat()
    print(f"  Token   : {token[:20]}...")
    print(f"  Expires : {expires}")
    conn.execute(
        "INSERT INTO dashboard_sessions (session_token, username, expires_at, created_at) VALUES (?,?,?,?)",
        (token, username, expires, created),
    )
    conn.commit()
    print("  Session insert: OK")
except Exception as e:
    print(f"  Session insert FAILED: {e}")

print()

# Step 4: check sessions table schema
print("=== Step 4: sessions table schema ===")
cols = conn.execute("PRAGMA table_info(dashboard_sessions)").fetchall()
for c in cols:
    print(f"  {c['name']:20s} {c['type']}")

print()

# Step 5: check users table schema
print("=== Step 5: users table schema ===")
cols = conn.execute("PRAGMA table_info(dashboard_users)").fetchall()
for c in cols:
    print(f"  {c['name']:20s} {c['type']}")

conn.close()
print()
print("All checks done.")
