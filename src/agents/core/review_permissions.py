from __future__ import annotations

from typing import Optional, Dict, Any


# ===============================
# ROLE DEFINITIONS
# ===============================

ROLE_EMPLOYEE = "employee"
ROLE_MANAGER = "manager"
ROLE_OWNER = "owner"

ALL_ROLES = {ROLE_EMPLOYEE, ROLE_MANAGER, ROLE_OWNER}

# Role hierarchy — higher index = more privilege
ROLE_HIERARCHY = {
    "viewer": 0,
    "bookkeeper": 1,
    "employee": 1,
    "manager": 2,
    "admin": 3,
    "owner": 4,
}


# ===============================
# CORE CHECKS
# ===============================

def normalize_role(role: Optional[str]) -> str:
    if not role:
        return ROLE_EMPLOYEE
    role = role.strip().lower()
    if role not in ALL_ROLES:
        return ROLE_EMPLOYEE
    return role


def is_employee(role: str) -> bool:
    return normalize_role(role) == ROLE_EMPLOYEE


def is_manager(role: str) -> bool:
    return normalize_role(role) == ROLE_MANAGER


def is_owner(role: str) -> bool:
    return normalize_role(role) == ROLE_OWNER


def is_manager_or_owner(role: str) -> bool:
    role = normalize_role(role)
    return role in {ROLE_MANAGER, ROLE_OWNER}


# ===============================
# ACTION PERMISSIONS
# ===============================

def can_assign_documents(role: str) -> bool:
    return is_manager_or_owner(role)


def can_manage_portfolios(role: str) -> bool:
    return is_manager_or_owner(role)


def can_post_to_qbo(role: str) -> bool:
    # Only managers and owners can push to accounting system
    return is_manager_or_owner(role)


def can_approve_posting(role: str) -> bool:
    return is_manager_or_owner(role)


def can_retry_posting(role: str) -> bool:
    return is_manager_or_owner(role)


def can_build_posting(role: str) -> bool:
    return is_manager_or_owner(role)


def can_edit_accounting(role: str) -> bool:
    """Role-based accounting edit control.

    - owner: full edit access
    - manager: can edit GL account, tax code, category, vendor
    - employee: limited (vendor name and description only)
    - client / unknown: read-only
    """
    role = normalize_role(role)
    return role in {ROLE_MANAGER, ROLE_OWNER}


def can_edit_amount(role: str, approved: bool = False) -> bool:
    """Check if role can edit monetary amounts.

    - owner: always
    - manager: only before approval
    - employee / client: never
    """
    role = normalize_role(role)
    if role == ROLE_OWNER:
        return True
    if role == ROLE_MANAGER:
        return not approved
    return False


def can_edit_gl(role: str) -> bool:
    """Check if role can edit GL account."""
    return normalize_role(role) in {ROLE_MANAGER, ROLE_OWNER}


def can_edit_tax_code(role: str) -> bool:
    """Check if role can edit tax code."""
    return normalize_role(role) in {ROLE_MANAGER, ROLE_OWNER}


def can_edit_vendor(role: str) -> bool:
    """Employees and above can edit vendor name."""
    return normalize_role(role) in {ROLE_EMPLOYEE, ROLE_MANAGER, ROLE_OWNER}


def can_edit_description(role: str) -> bool:
    """Employees and above can edit description."""
    return normalize_role(role) in {ROLE_EMPLOYEE, ROLE_MANAGER, ROLE_OWNER}


def can_hold_document(role: str) -> bool:
    return True


def can_release_hold(role: str) -> bool:
    return True


# ===============================
# PORTFOLIO ACCESS
# ===============================

def has_portfolio_access(
    *,
    role: str,
    username: str,
    document_client: Optional[str],
    user_portfolios: Dict[str, set[str]],
) -> bool:
    """
    Determines if user can access this document based on client.
    """

    role = normalize_role(role)

    # Owners see everything
    if is_owner(role):
        return True

    if not document_client:
        return True

    document_client = document_client.strip().upper()

    # Manager / employee → must be in portfolio
    allowed_clients = user_portfolios.get(username, set())

    return document_client in allowed_clients


# ===============================
# HARD ENFORCEMENT HELPERS
# ===============================

class PermissionDenied(Exception):
    pass


def require(condition: bool, message: str = "Permission denied"):
    if not condition:
        raise PermissionDenied(message)


def enforce_qbo_access(role: str):
    require(
        can_post_to_qbo(role),
        "You are not allowed to post to QuickBooks"
    )


def enforce_assignment_access(role: str):
    require(
        can_assign_documents(role),
        "You are not allowed to assign documents"
    )


def enforce_portfolio_access(role: str):
    require(
        can_manage_portfolios(role),
        "You are not allowed to manage portfolios"
    )


def enforce_document_access(
    *,
    role: str,
    username: str,
    document_client: Optional[str],
    user_portfolios: Dict[str, set[str]],
):
    require(
        has_portfolio_access(
            role=role,
            username=username,
            document_client=document_client,
            user_portfolios=user_portfolios,
        ),
        "You do not have access to this document"
    )


# ===============================
# DB-BACKED PERMISSION CHECKS
# ===============================

# Permission → minimum role level required
_PERMISSION_MATRIX = {
    "view_document":    0,   # viewer+
    "create_document":  1,   # bookkeeper+
    "approve_document": 2,   # manager+
    "delete_document":  3,   # admin+
    "sign_off_audit":   3,   # admin+
    "manage_users":     4,   # owner only
}


def get_user_role(conn: Any, user_id: str) -> str:
    """Look up user role from the users or dashboard_users table."""
    import sqlite3 as _sql
    if not isinstance(conn, _sql.Connection):
        return "viewer"
    for table in ("users", "dashboard_users"):
        try:
            row = conn.execute(
                f"SELECT role, active FROM {table} WHERE user_id = ? OR username = ?",
                (user_id, user_id),
            ).fetchone()
            if row:
                if not row["active"]:
                    return "__deactivated__"
                return row["role"] or "viewer"
        except Exception:
            continue
    return "viewer"


def check_permission(
    conn: Any,
    *,
    user_id: str,
    permission: str,
    resource_client: Optional[str] = None,
) -> bool:
    """Check if a user has a given permission based on their role.

    Returns False for deactivated users and users whose role level
    is below the permission's minimum.
    """
    role = get_user_role(conn, user_id)
    if role == "__deactivated__":
        return False
    role_level = ROLE_HIERARCHY.get(role.lower(), 0)
    required_level = _PERMISSION_MATRIX.get(permission, 99)

    if role_level < required_level:
        return False

    # Cross-client check
    if resource_client is not None:
        try:
            import sqlite3 as _sql
            if isinstance(conn, _sql.Connection):
                for table in ("users", "dashboard_users"):
                    try:
                        row = conn.execute(
                            f"SELECT client_code FROM {table} WHERE user_id = ? OR username = ?",
                            (user_id, user_id),
                        ).fetchone()
                        if row:
                            user_client = row["client_code"]
                            if user_client and user_client != resource_client:
                                return False
                            break
                    except Exception:
                        continue
        except Exception:
            pass

    return True