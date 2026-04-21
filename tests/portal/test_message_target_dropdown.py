"""Item 2: CPA messaging target user dropdown."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import multi_user_portal as mup  # noqa: E402


def _u(id_, name, email, role='contributor', status='active',
        last_active='2026-04-10'):
    return {'id': id_, 'full_name': name, 'email': email,
            'role': role, 'status': status,
            'last_active_at': last_active}


def test_dropdown_lists_all_active_portal_users():
    users = [_u(1, 'Jean', 'j@c', role='admin'),
             _u(2, 'Marie', 'm@c', role='contributor')]
    html = mup.render_target_user_dropdown(users)
    assert 'Jean (Admin)' in html
    assert 'Marie (Contributor)' in html
    assert 'broadcast' in html
    # Each user id is an <option value=N>
    assert 'value="1"' in html
    assert 'value="2"' in html


def test_suspended_users_shown_greyed_out():
    users = [_u(1, 'Jean', 'j@c', role='admin'),
             _u(2, 'Marie', 'm@c', status='suspended')]
    html = mup.render_target_user_dropdown(users)
    # Suspended user present but disabled + prefix
    assert 'value="2" disabled' in html
    assert '[suspended]' in html
    # Active user not disabled
    assert 'value="1" disabled' not in html


def test_removed_users_not_in_dropdown():
    users = [_u(1, 'Jean', 'j@c', role='admin'),
             _u(2, 'Ghost', 'g@c', status='removed')]
    html = mup.render_target_user_dropdown(users)
    assert 'Jean' in html
    assert 'Ghost' not in html
    assert 'value="2"' not in html


def test_broadcast_default_when_no_selection():
    users = [_u(1, 'Jean', 'j@c', role='admin')]
    html = mup.render_target_user_dropdown(users)
    # Broadcast option selected by default
    assert 'value="" selected' in html


def test_specific_user_selected_marks_option():
    users = [_u(1, 'Jean', 'j@c'), _u(2, 'Marie', 'm@c')]
    html = mup.render_target_user_dropdown(users, selected_id=2)
    assert 'value="2"  selected' in html or 'value="2" selected' in html


def test_empty_user_list_returns_blank():
    assert mup.render_target_user_dropdown([]) == ''


def test_last_active_in_tooltip():
    users = [_u(1, 'Jean', 'j@c', role='admin',
                 last_active='2026-04-11T12:00:00Z')]
    html = mup.render_target_user_dropdown(users)
    assert 'title="last active 2026-04-11T12:00:00Z"' in html


def test_sending_to_specific_user_saves_target_id():
    """Confirms the dropdown field name matches what the POST handler reads."""
    users = [_u(1, 'Jean', 'j@c', role='admin')]
    html = mup.render_target_user_dropdown(users)
    assert 'name="target_portal_user_id"' in html
