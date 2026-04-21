"""Cleanup Item 2: user_ui_preferences table + resolve helper."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.integrations import ui_preferences as uip  # noqa: E402


def _mkdb(tmp_path):
    db = tmp_path / 'prefs.db'
    uip.ensure_preferences_schema(db)
    return db


def test_get_unset_returns_default(tmp_path):
    db = _mkdb(tmp_path)
    assert uip.get_preference(
        db, user_email='a@b', firm_code='F', preference_key='x',
        default='fallback',
    ) == 'fallback'


def test_set_then_get(tmp_path):
    db = _mkdb(tmp_path)
    uip.set_preference(
        db, user_email='a@b', firm_code='F',
        preference_key=uip.PREF_QUEUE_UPLOADER,
        preference_value='x@y.com,z@w.com',
    )
    assert uip.get_preference(
        db, user_email='a@b', firm_code='F',
        preference_key=uip.PREF_QUEUE_UPLOADER,
    ) == 'x@y.com,z@w.com'


def test_set_overwrites(tmp_path):
    db = _mkdb(tmp_path)
    uip.set_preference(db, user_email='a@b', firm_code='F',
                        preference_key='k', preference_value='v1')
    uip.set_preference(db, user_email='a@b', firm_code='F',
                        preference_key='k', preference_value='v2')
    assert uip.get_preference(db, user_email='a@b', firm_code='F',
                                preference_key='k') == 'v2'


def test_clear_deletes_preference(tmp_path):
    db = _mkdb(tmp_path)
    uip.set_preference(db, user_email='a@b', firm_code='F',
                        preference_key='k', preference_value='v')
    uip.clear_preference(db, user_email='a@b', firm_code='F',
                          preference_key='k')
    assert uip.get_preference(db, user_email='a@b', firm_code='F',
                                preference_key='k') is None


def test_preferences_scoped_per_user(tmp_path):
    db = _mkdb(tmp_path)
    uip.set_preference(db, user_email='a@b', firm_code='F',
                        preference_key='k', preference_value='A_VAL')
    uip.set_preference(db, user_email='c@d', firm_code='F',
                        preference_key='k', preference_value='C_VAL')
    assert uip.get_preference(db, user_email='a@b', firm_code='F',
                                preference_key='k') == 'A_VAL'
    assert uip.get_preference(db, user_email='c@d', firm_code='F',
                                preference_key='k') == 'C_VAL'


def test_preferences_scoped_per_firm(tmp_path):
    db = _mkdb(tmp_path)
    uip.set_preference(db, user_email='a@b', firm_code='F1',
                        preference_key='k', preference_value='one')
    uip.set_preference(db, user_email='a@b', firm_code='F2',
                        preference_key='k', preference_value='two')
    assert uip.get_preference(db, user_email='a@b', firm_code='F1',
                                preference_key='k') == 'one'
    assert uip.get_preference(db, user_email='a@b', firm_code='F2',
                                preference_key='k') == 'two'


def test_multiple_filter_types_coexist(tmp_path):
    db = _mkdb(tmp_path)
    uip.set_preference(db, user_email='a@b', firm_code='F',
                        preference_key=uip.PREF_QUEUE_UPLOADER,
                        preference_value='u1@x.com')
    uip.set_preference(db, user_email='a@b', firm_code='F',
                        preference_key=uip.PREF_QUEUE_STATUS,
                        preference_value='Ready')
    uip.set_preference(db, user_email='a@b', firm_code='F',
                        preference_key=uip.PREF_REVIEW_PRIORITY,
                        preference_value='urgent')
    all_prefs = uip.get_all_preferences(db, user_email='a@b', firm_code='F')
    assert all_prefs[uip.PREF_QUEUE_UPLOADER] == 'u1@x.com'
    assert all_prefs[uip.PREF_QUEUE_STATUS] == 'Ready'
    assert all_prefs[uip.PREF_REVIEW_PRIORITY] == 'urgent'


def test_resolve_url_overrides_stored(tmp_path):
    db = _mkdb(tmp_path)
    uip.set_preference(db, user_email='a@b', firm_code='F',
                        preference_key='k', preference_value='old')
    # URL passes a new value — winner AND updates stored state.
    got = uip.resolve_with_override(
        db, user_email='a@b', firm_code='F', preference_key='k',
        url_value='new',
    )
    assert got == 'new'
    assert uip.get_preference(db, user_email='a@b', firm_code='F',
                                preference_key='k') == 'new'


def test_resolve_no_url_uses_stored(tmp_path):
    db = _mkdb(tmp_path)
    uip.set_preference(db, user_email='a@b', firm_code='F',
                        preference_key='k', preference_value='saved')
    got = uip.resolve_with_override(
        db, user_email='a@b', firm_code='F', preference_key='k',
    )
    assert got == 'saved'


def test_resolve_no_url_no_stored_returns_default(tmp_path):
    db = _mkdb(tmp_path)
    got = uip.resolve_with_override(
        db, user_email='a@b', firm_code='F', preference_key='k',
        default='fallback',
    )
    assert got == 'fallback'


def test_resolve_empty_url_with_persist_empty_clears(tmp_path):
    db = _mkdb(tmp_path)
    uip.set_preference(db, user_email='a@b', firm_code='F',
                        preference_key='k', preference_value='old')
    got = uip.resolve_with_override(
        db, user_email='a@b', firm_code='F', preference_key='k',
        url_value='', persist_empty=True,
    )
    assert got is None
    # Stored preference gone
    assert uip.get_preference(db, user_email='a@b', firm_code='F',
                                preference_key='k') is None


def test_missing_table_returns_defaults(tmp_path):
    """Pre-migration DB with no user_ui_preferences table: reads
    return default + writes log but don't raise."""
    db = tmp_path / 'nomigration.db'
    # Don't run ensure_preferences_schema — table absent.
    assert uip.get_preference(
        db, user_email='a@b', firm_code='F', preference_key='k',
        default='fallback',
    ) == 'fallback'
    # Write also doesn't raise.
    uip.set_preference(db, user_email='a@b', firm_code='F',
                        preference_key='k', preference_value='v')


def test_blank_user_email_noop(tmp_path):
    db = _mkdb(tmp_path)
    # Blank user → writes no-op, reads return default.
    uip.set_preference(db, user_email='', firm_code='F',
                        preference_key='k', preference_value='v')
    assert uip.get_preference(db, user_email='', firm_code='F',
                                preference_key='k',
                                default='d') == 'd'
