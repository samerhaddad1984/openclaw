from __future__ import annotations

"""
OtoCPA — Client Account Manager
========================================
Run from project root:

    python scripts/manage_clients.py list
    python scripts/manage_clients.py add --username clientabc --client-code SOUSSOL --name "Sous-Sol Quebec" --lang fr
    python scripts/manage_clients.py set-password --username clientabc --password NewPass123!
    python scripts/manage_clients.py deactivate --username clientabc

The generated password is printed once. Store it securely and give it to the client.
"""

import argparse
import hashlib
import secrets
import sqlite3
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = ROOT_DIR / "data" / "otocpa_agent.db"


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def generate_password(length: int = 16) -> str:
    alphabet = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ23456789!@#"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def cmd_list(args: argparse.Namespace) -> None:
    with open_db() as conn:
        rows = conn.execute(
            "SELECT username, display_name, client_code, language, active FROM dashboard_users WHERE role='client' ORDER BY username"
        ).fetchall()
    if not rows:
        print("No client accounts found.")
        return
    print(f"\n{'USERNAME':20s} {'DISPLAY NAME':24s} {'CLIENT CODE':16s} {'LANG':6s} {'ACTIVE':6s}")
    print("-" * 80)
    for r in rows:
        print(f"{r['username']:20s} {str(r['display_name'] or ''):24s} {str(r['client_code'] or ''):16s} {str(r['language'] or 'fr'):6s} {'Yes' if r['active'] else 'No':6s}")
    print()


def cmd_add(args: argparse.Namespace) -> None:
    username     = args.username.strip().lower()
    client_code  = args.client_code.strip()
    display_name = args.name or username
    lang         = args.lang or "fr"
    password     = args.password or generate_password()

    with open_db() as conn:
        existing = conn.execute(
            "SELECT username FROM dashboard_users WHERE username=?", (username,)
        ).fetchone()
        if existing:
            print(f"Error: user '{username}' already exists.")
            return

        # Ensure columns exist
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(dashboard_users)").fetchall()}
        for col in ["client_code", "language"]:
            if col not in cols:
                conn.execute(f"ALTER TABLE dashboard_users ADD COLUMN {col} TEXT")

        conn.execute(
            """
            INSERT INTO dashboard_users
                (username, password_hash, role, display_name, client_code, language, active, created_at)
            VALUES (?,?,?,?,?,?,1,datetime('now'))
            """,
            (username, hash_password(password), "client", display_name, client_code, lang),
        )
        conn.commit()

    print()
    print("✓ Client account created:")
    print(f"  Username    : {username}")
    print(f"  Password    : {password}   ← give this to the client, it won't be shown again")
    print(f"  Client code : {client_code}")
    print(f"  Language    : {lang}")
    print(f"  Portal URL  : http://127.0.0.1:8788/")
    print()


def cmd_set_password(args: argparse.Namespace) -> None:
    username = args.username.strip().lower()
    password = args.password or generate_password()
    with open_db() as conn:
        n = conn.execute(
            "UPDATE dashboard_users SET password_hash=? WHERE username=? AND role='client'",
            (hash_password(password), username),
        ).rowcount
        conn.commit()
    if n == 0:
        print(f"Error: client '{username}' not found.")
    else:
        print(f"✓ Password updated for '{username}'.")
        if not args.password:
            print(f"  New password: {password}   ← send this to the client")


def cmd_deactivate(args: argparse.Namespace) -> None:
    username = args.username.strip().lower()
    with open_db() as conn:
        n = conn.execute(
            "UPDATE dashboard_users SET active=0 WHERE username=? AND role='client'",
            (username,),
        ).rowcount
        conn.commit()
    print(f"✓ Client '{username}' deactivated." if n else f"Error: '{username}' not found.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Manage OtoCPA client portal accounts"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="List all client accounts")

    p_add = sub.add_parser("add", help="Create a new client account")
    p_add.add_argument("--username",    required=True, help="Login username (e.g. clientabc)")
    p_add.add_argument("--client-code", required=True, dest="client_code",
                       help="Client code matching documents table (e.g. SOUSSOL)")
    p_add.add_argument("--name",       default=None, help="Display name (e.g. 'Sous-Sol Quebec')")
    p_add.add_argument("--lang",       default="fr", choices=["fr","en"], help="Language preference")
    p_add.add_argument("--password",   default=None, help="Password (auto-generated if omitted)")

    p_pw = sub.add_parser("set-password", help="Reset a client's password")
    p_pw.add_argument("--username", required=True)
    p_pw.add_argument("--password", default=None)

    p_de = sub.add_parser("deactivate", help="Deactivate a client account")
    p_de.add_argument("--username", required=True)

    args = parser.parse_args()
    {"list": cmd_list, "add": cmd_add, "set-password": cmd_set_password,
     "deactivate": cmd_deactivate}[args.command](args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
