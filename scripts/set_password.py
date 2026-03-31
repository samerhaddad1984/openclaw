import hashlib
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "otocpa_agent.db"

users = [
    ("sam",       "SamOwner123!"),
    ("manager1",  "Manager1Pass!"),
    ("employee1", "Employee1Pass!"),
    ("employee2", "Employee2Pass!"),
]

conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row

for username, password in users:
    h = hashlib.sha256(password.encode()).hexdigest()
    conn.execute(
        "UPDATE dashboard_users SET password_hash = ? WHERE username = ?",
        (h, username),
    )
    print(f"  {username:20s} -> password set")

conn.commit()
conn.close()
print()
print("Done. Passwords updated.")
