import sqlite3

conn = sqlite3.connect("data/otocpa_agent.db")
cursor = conn.cursor()

rows = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")

print("\nTABLES IN DATABASE:\n")

for r in rows:
    print(r[0])

conn.close()