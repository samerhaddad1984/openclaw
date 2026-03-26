from __future__ import annotations

import argparse
import getpass
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.agents.core.dashboard_auth import (
    bootstrap_auth_with_default_owner,
    create_user,
    list_users,
    set_user_active,
    update_user_password,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage LedgerLink dashboard users"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", help="Create auth tables and seed default owner")

    add_user = subparsers.add_parser("add-user", help="Add a dashboard user")
    add_user.add_argument("--username", required=True)
    add_user.add_argument("--role", required=True, choices=["owner", "manager", "employee"])
    add_user.add_argument("--display-name", default="")
    add_user.add_argument("--password", default="")

    reset_password = subparsers.add_parser("reset-password", help="Reset a user password")
    reset_password.add_argument("--username", required=True)
    reset_password.add_argument("--password", default="")

    deactivate = subparsers.add_parser("deactivate-user", help="Deactivate a user")
    deactivate.add_argument("--username", required=True)

    activate = subparsers.add_parser("activate-user", help="Activate a user")
    activate.add_argument("--username", required=True)

    subparsers.add_parser("list-users", help="List all users")

    return parser


def prompt_password(label: str) -> str:
    password = getpass.getpass(f"{label}: ").strip()
    if not password:
        raise ValueError("Password cannot be empty")
    return password


def command_init() -> int:
    bootstrap_auth_with_default_owner()
    print()
    print("AUTH INITIALIZED")
    print("=" * 80)
    print("Default owner user created if missing:")
    print("  username: sam")
    print("  password: ChangeMe123!")
    print()
    print("Change that password immediately.")
    print()
    return 0


def command_add_user(args: argparse.Namespace) -> int:
    bootstrap_auth_with_default_owner()

    password = args.password.strip() if args.password else ""
    if not password:
        password = prompt_password("Enter password")

    user = create_user(
        username=args.username,
        password=password,
        role=args.role,
        display_name=args.display_name,
        is_active=True,
    )

    print()
    print("USER CREATED")
    print("=" * 80)
    print(f"username     : {user.get('username')}")
    print(f"display_name : {user.get('display_name')}")
    print(f"role         : {user.get('role')}")
    print(f"is_active    : {user.get('is_active')}")
    print()
    return 0


def command_reset_password(args: argparse.Namespace) -> int:
    bootstrap_auth_with_default_owner()

    password = args.password.strip() if args.password else ""
    if not password:
        password = prompt_password("Enter new password")

    update_user_password(args.username, password)

    print()
    print("PASSWORD UPDATED")
    print("=" * 80)
    print(f"username: {args.username}")
    print()
    return 0


def command_deactivate_user(args: argparse.Namespace) -> int:
    bootstrap_auth_with_default_owner()
    set_user_active(args.username, False)

    print()
    print("USER DEACTIVATED")
    print("=" * 80)
    print(f"username: {args.username}")
    print()
    return 0


def command_activate_user(args: argparse.Namespace) -> int:
    bootstrap_auth_with_default_owner()
    set_user_active(args.username, True)

    print()
    print("USER ACTIVATED")
    print("=" * 80)
    print(f"username: {args.username}")
    print()
    return 0


def command_list_users() -> int:
    bootstrap_auth_with_default_owner()
    users = list_users()

    print()
    print("DASHBOARD USERS")
    print("=" * 120)
    print(f"{'USERNAME':<20} {'DISPLAY NAME':<25} {'ROLE':<12} {'ACTIVE':<8} {'LAST LOGIN':<25}")
    print("-" * 120)

    for user in users:
        print(
            f"{str(user.get('username', '')):<20} "
            f"{str(user.get('display_name', '')):<25} "
            f"{str(user.get('role', '')):<12} "
            f"{str(user.get('is_active', '')):<8} "
            f"{str(user.get('last_login_at', '')):<25}"
        )

    print()
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "init":
        return command_init()

    if args.command == "add-user":
        return command_add_user(args)

    if args.command == "reset-password":
        return command_reset_password(args)

    if args.command == "deactivate-user":
        return command_deactivate_user(args)

    if args.command == "activate-user":
        return command_activate_user(args)

    if args.command == "list-users":
        return command_list_users()

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())