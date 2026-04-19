"""Sprint C Batch 3 — role-assignment guard.

BUG #4: firm_admin could previously create peer firm_admin accounts via
the /users/add path. The guard now lives in can_assign_role(actor_role,
target_role), which both the handler and these tests exercise.
"""
from __future__ import annotations

import importlib
import sys

import pytest


def _load_rd():
    """Import scripts/review_dashboard without triggering main()."""
    if "rd" in sys.modules:
        return sys.modules["rd"]
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "rd", "/opt/otocpa/scripts/review_dashboard.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["rd"] = mod
    try:
        spec.loader.exec_module(mod)
    except SystemExit:
        pass
    return mod


@pytest.fixture(scope="module")
def rd():
    return _load_rd()


def test_firm_admin_cannot_create_firm_admin(rd):
    allowed, reason = rd.can_assign_role("firm_admin", "firm_admin")
    assert allowed is False
    assert "firm_admin" in reason


def test_firm_admin_cannot_create_owner(rd):
    allowed, reason = rd.can_assign_role("firm_admin", "owner")
    assert allowed is False
    assert "owner" in reason


def test_firm_admin_can_create_manager(rd):
    allowed, reason = rd.can_assign_role("firm_admin", "manager")
    assert allowed is True
    assert reason == ""


def test_firm_admin_can_create_employee(rd):
    allowed, _ = rd.can_assign_role("firm_admin", "employee")
    assert allowed is True


def test_owner_can_create_firm_admin(rd):
    allowed, _ = rd.can_assign_role("owner", "firm_admin")
    assert allowed is True


def test_owner_can_create_owner(rd):
    allowed, _ = rd.can_assign_role("owner", "owner")
    assert allowed is True


def test_owner_can_create_manager(rd):
    allowed, _ = rd.can_assign_role("owner", "manager")
    assert allowed is True


def test_case_insensitive_actor_and_target(rd):
    # Form posts may arrive with uppercase in legacy payloads; the guard
    # should still catch escalation attempts.
    allowed, _ = rd.can_assign_role("FIRM_ADMIN", "FIRM_ADMIN")
    assert allowed is False


def test_unknown_actor_defaults_to_restricted(rd):
    # Unknown / missing actor role falls through to the "non-owner" branch.
    allowed, reason = rd.can_assign_role(None, "firm_admin")
    assert allowed is False
    allowed, _ = rd.can_assign_role("", "employee")
    # Target is employee — permissive for anyone with manage_users at the
    # handler layer; the helper only restricts owner / firm_admin escalation.
    assert allowed is True


def test_manager_cannot_create_firm_admin_or_owner(rd):
    # Managers don't have manage_users at the handler level, but the
    # helper must still refuse to assign firm_admin/owner if called with a
    # manager actor (defence in depth).
    allowed, _ = rd.can_assign_role("manager", "firm_admin")
    assert allowed is False
    allowed, _ = rd.can_assign_role("manager", "owner")
    assert allowed is False
