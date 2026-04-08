from __future__ import annotations

import hashlib
import hmac
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

PBKDF2_ITERATIONS = 200_000
SESSION_HOURS_DEFAULT = 12


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().replace(microsecond=0).isoformat()


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_key(value: Any) -> str:
    return " ".join(normalize_text(value).casefold().split())


def iso_after_hours(hours: int) -> str:
    return (utc_now() + timedelta(hours=hours)).replace(microsecond=0).isoformat()


def _pbkdf2_hash(password: str, salt_hex: str) -> str:
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt_hex),
        PBKDF2_ITERATIONS,
    )
    return digest.hex()


def hash_password(password: str, *, salt_hex: str | None = None) -> str:
    password = normalize_text(password)
    if not password:
        raise ValueError("Password cannot be empty")

    salt_hex = salt_hex or secrets.token_hex(16)
    password_hash = _pbkdf2_hash(password, salt_hex)
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt_hex}${password_hash}"


def verify_password(password: str, stored_hash: str) -> bool:
    password = normalize_text(password)
    stored_hash = normalize_text(stored_hash)

    if not password or not stored_hash:
        return False

    try:
        algorithm, iterations_text, salt_hex, expected_hash = stored_hash.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        iterations = int(iterations_text)
        if iterations != PBKDF2_ITERATIONS:
            return False
    except Exception:
        return False

    actual_hash = _pbkdf2_hash(password, salt_hex)
    return hmac.compare_digest(actual_hash, expected_hash)


def bootstrap_auth_schema() -> None:
    with open_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dashboard_users (
                username TEXT PRIMARY KEY,
                display_name TEXT,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_login_at TEXT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dashboard_sessions (
                session_token TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                role TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                FOREIGN KEY(username) REFERENCES dashboard_users(username)
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dashboard_sessions_username
            ON dashboard_sessions(username)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dashboard_sessions_expires_at
            ON dashboard_sessions(expires_at)
            """
        )

        conn.commit()


def cleanup_expired_sessions() -> int:
    now_iso = utc_now_iso()
    with open_db() as conn:
        cursor = conn.execute(
            """
            DELETE FROM dashboard_sessions
            WHERE expires_at <= ?
            """,
            (now_iso,),
        )
        conn.commit()
        return int(cursor.rowcount)


def create_user(
    *,
    username: str,
    password: str,
    role: str,
    display_name: str = "",
    is_active: bool = True,
) -> dict[str, Any]:
    bootstrap_auth_schema()

    username_clean = normalize_key(username)
    role_clean = normalize_key(role)
    display_name_clean = normalize_text(display_name) or username_clean

    if not username_clean:
        raise ValueError("Username cannot be empty")

    if role_clean not in {"owner", "manager", "employee"}:
        raise ValueError("Role must be owner, manager, or employee")

    password_hash = hash_password(password)
    now_iso = utc_now_iso()

    with open_db() as conn:
        existing = conn.execute(
            "SELECT username FROM dashboard_users WHERE username = ?",
            (username_clean,),
        ).fetchone()
        if existing is not None:
            raise ValueError(f"User already exists: {username_clean}")

        conn.execute(
            """
            INSERT INTO dashboard_users (
                username,
                display_name,
                password_hash,
                role,
                is_active,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                username_clean,
                display_name_clean,
                password_hash,
                role_clean,
                1 if is_active else 0,
                now_iso,
                now_iso,
            ),
        )
        conn.commit()

    return get_user(username_clean) or {}


def update_user_password(username: str, new_password: str) -> None:
    bootstrap_auth_schema()

    username_clean = normalize_key(username)
    if not username_clean:
        raise ValueError("Username cannot be empty")

    password_hash = hash_password(new_password)

    with open_db() as conn:
        cursor = conn.execute(
            """
            UPDATE dashboard_users
            SET password_hash = ?, updated_at = ?
            WHERE username = ?
            """,
            (password_hash, utc_now_iso(), username_clean),
        )
        conn.commit()

    if cursor.rowcount <= 0:
        raise ValueError(f"User not found: {username_clean}")


def set_user_active(username: str, is_active: bool) -> None:
    bootstrap_auth_schema()

    username_clean = normalize_key(username)
    if not username_clean:
        raise ValueError("Username cannot be empty")

    with open_db() as conn:
        cursor = conn.execute(
            """
            UPDATE dashboard_users
            SET is_active = ?, updated_at = ?
            WHERE username = ?
            """,
            (1 if is_active else 0, utc_now_iso(), username_clean),
        )
        conn.commit()

    if cursor.rowcount <= 0:
        raise ValueError(f"User not found: {username_clean}")


def get_user(username: str) -> dict[str, Any] | None:
    bootstrap_auth_schema()

    username_clean = normalize_key(username)
    if not username_clean:
        return None

    with open_db() as conn:
        row = conn.execute(
            """
            SELECT
                username,
                display_name,
                role,
                is_active,
                created_at,
                updated_at,
                last_login_at
            FROM dashboard_users
            WHERE username = ?
            """,
            (username_clean,),
        ).fetchone()

    return dict(row) if row is not None else None


def list_users() -> list[dict[str, Any]]:
    bootstrap_auth_schema()

    with open_db() as conn:
        rows = conn.execute(
            """
            SELECT
                username,
                display_name,
                role,
                is_active,
                created_at,
                updated_at,
                last_login_at
            FROM dashboard_users
            ORDER BY role, username
            """
        ).fetchall()

    return [dict(r) for r in rows]


def authenticate_user(username: str, password: str) -> dict[str, Any] | None:
    bootstrap_auth_schema()
    cleanup_expired_sessions()

    username_clean = normalize_key(username)
    password = normalize_text(password)

    if not username_clean or not password:
        return None

    with open_db() as conn:
        row = conn.execute(
            """
            SELECT
                username,
                display_name,
                password_hash,
                role,
                is_active,
                created_at,
                updated_at,
                last_login_at
            FROM dashboard_users
            WHERE username = ?
            """,
            (username_clean,),
        ).fetchone()

        if row is None:
            return None

        if int(row["is_active"]) != 1:
            return None

        if not verify_password(password, normalize_text(row["password_hash"])):
            return None

        login_time = utc_now_iso()
        conn.execute(
            """
            UPDATE dashboard_users
            SET last_login_at = ?, updated_at = ?
            WHERE username = ?
            """,
            (login_time, login_time, username_clean),
        )
        conn.commit()

    return {
        "username": row["username"],
        "display_name": row["display_name"],
        "role": row["role"],
        "is_active": bool(row["is_active"]),
    }


def create_session(
    *,
    username: str,
    role: str,
    hours: int = SESSION_HOURS_DEFAULT,
) -> str:
    bootstrap_auth_schema()
    cleanup_expired_sessions()

    username_clean = normalize_key(username)
    role_clean = normalize_key(role)

    if not username_clean:
        raise ValueError("Username cannot be empty")
    if role_clean not in {"owner", "manager", "employee"}:
        raise ValueError("Invalid role")
    if hours <= 0:
        raise ValueError("Session hours must be greater than zero")

    token = secrets.token_urlsafe(48)
    now_iso = utc_now_iso()
    expires_at = iso_after_hours(hours)

    with open_db() as conn:
        conn.execute(
            """
            INSERT INTO dashboard_sessions (
                session_token,
                username,
                role,
                created_at,
                expires_at,
                last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (token, username_clean, role_clean, now_iso, expires_at, now_iso),
        )
        conn.commit()

    return token


def get_session(session_token: str) -> dict[str, Any] | None:
    bootstrap_auth_schema()
    cleanup_expired_sessions()

    token = normalize_text(session_token)
    if not token:
        return None

    with open_db() as conn:
        row = conn.execute(
            """
            SELECT
                s.session_token,
                s.username,
                s.role,
                s.created_at,
                s.expires_at,
                s.last_seen_at,
                u.display_name,
                u.is_active
            FROM dashboard_sessions s
            JOIN dashboard_users u
              ON u.username = s.username
            WHERE s.session_token = ?
            """,
            (token,),
        ).fetchone()

        if row is None:
            return None

        if int(row["is_active"]) != 1:
            conn.execute(
                "DELETE FROM dashboard_sessions WHERE session_token = ?",
                (token,),
            )
            conn.commit()
            return None

        now_iso = utc_now_iso()
        if normalize_text(row["expires_at"]) <= now_iso:
            conn.execute(
                "DELETE FROM dashboard_sessions WHERE session_token = ?",
                (token,),
            )
            conn.commit()
            return None

        conn.execute(
            """
            UPDATE dashboard_sessions
            SET last_seen_at = ?
            WHERE session_token = ?
            """,
            (now_iso, token),
        )
        conn.commit()

    return {
        "session_token": row["session_token"],
        "username": row["username"],
        "display_name": row["display_name"],
        "role": row["role"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "last_seen_at": now_iso,
    }


def delete_session(session_token: str) -> None:
    bootstrap_auth_schema()

    token = normalize_text(session_token)
    if not token:
        return

    with open_db() as conn:
        conn.execute(
            "DELETE FROM dashboard_sessions WHERE session_token = ?",
            (token,),
        )
        conn.commit()


def delete_all_sessions_for_user(username: str) -> None:
    bootstrap_auth_schema()

    username_clean = normalize_key(username)
    if not username_clean:
        return

    with open_db() as conn:
        conn.execute(
            "DELETE FROM dashboard_sessions WHERE username = ?",
            (username_clean,),
        )
        conn.commit()


def ensure_default_owner() -> None:
    bootstrap_auth_schema()

    with open_db() as conn:
        row = conn.execute(
            "SELECT username FROM dashboard_users WHERE username = 'sam'"
        ).fetchone()

    if row is not None:
        return

    create_user(
        username="sam",
        password="ChangeMe123!",
        role="owner",
        display_name="Sam",
        is_active=True,
    )


def bootstrap_auth_with_default_owner() -> None:
    bootstrap_auth_schema()
    ensure_default_owner()