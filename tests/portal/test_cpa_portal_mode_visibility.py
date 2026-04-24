"""Phase 3 — CPA dashboard visibility of portal mode + upgrade history."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_rd():
    if 'review_dashboard' in sys.modules:
        return sys.modules['review_dashboard']
    spec = importlib.util.spec_from_file_location(
        'review_dashboard',
        str(ROOT / 'scripts' / 'review_dashboard.py'),
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules['review_dashboard'] = module
    spec.loader.exec_module(module)
    return module


RD = _load_rd()


# ---------------------------------------------------------------------------
# Portal mode section rendering
# ---------------------------------------------------------------------------


def test_cpa_sees_portal_mode_single(monkeypatch):
    # Stub open_db so the renderer can query (returns empty).
    class _Stub:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, *a, **kw):
            class _R:
                def fetchall(self): return []
                def fetchone(self): return (0,)
            return _R()
    monkeypatch.setattr(RD, 'open_db', _Stub)
    html = RD._render_portal_mode_section(
        {'client_code': 'ACME', 'portal_mode': 'single'},
        lang='en',
    )
    assert 'Single-user portal' in html
    assert 'data-testid="portal-mode-section"' in html
    assert 'data-testid="portal-mode-value">single' in html
    # Upgrade cue is mentioned.
    assert 'self-upgrade' in html


def test_cpa_sees_portal_mode_multi_with_user_count(monkeypatch):
    # Stub open_db to report 3 users.
    class _Stub:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, sql, *a, **kw):
            class _R:
                def fetchall(self): return []
                def fetchone(self): return (3,)
            return _R()
    monkeypatch.setattr(RD, 'open_db', _Stub)
    html = RD._render_portal_mode_section(
        {'client_code': 'ACME', 'portal_mode': 'multi'},
        lang='en',
    )
    assert '3 user(s) registered' in html
    assert 'data-testid="portal-mode-value">multi' in html


def test_cpa_sees_bilingual_portal_mode_fr(monkeypatch):
    class _Stub:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, *a, **kw):
            class _R:
                def fetchall(self): return []
                def fetchone(self): return (0,)
            return _R()
    monkeypatch.setattr(RD, 'open_db', _Stub)
    html = RD._render_portal_mode_section(
        {'client_code': 'ACME', 'portal_mode': 'single'},
        lang='fr',
    )
    assert 'Mode du portail' in html
    assert 'mono-utilisateur' in html


def test_cpa_sees_upgrade_history(monkeypatch):
    """Recent portal_mode_changed audit rows surface in the history
    table."""
    class _Stub:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, sql, params=()):
            class _R:
                def __init__(self, sql_):
                    self.sql = sql_
                def fetchall(self):
                    if 'client_portal_user_audit' in self.sql:
                        class _Row(dict):
                            def __getitem__(self, k):
                                return super().__getitem__(k)
                        return [
                            _Row(
                                actor_email='marie@acme.com',
                                detail='mode=multi;reason=self_upgrade',
                                created_at='2026-04-24T12:00:00+00:00',
                            ),
                            _Row(
                                actor_email='cpa@firm.com',
                                detail='mode=multi',
                                created_at='2026-04-10T08:00:00+00:00',
                            ),
                        ]
                    return []
                def fetchone(self): return (2,)
            return _R(sql)
    monkeypatch.setattr(RD, 'open_db', _Stub)
    html = RD._render_portal_mode_section(
        {'client_code': 'ACME', 'portal_mode': 'multi'},
        lang='en',
    )
    # Both audit entries rendered.
    assert 'marie@acme.com' in html
    assert 'self_upgrade' in html
    assert 'cpa@firm.com' in html
    assert 'data-testid="portal-mode-history"' in html


def test_cpa_history_empty_state(monkeypatch):
    class _Stub:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, *a, **kw):
            class _R:
                def fetchall(self): return []
                def fetchone(self): return (0,)
            return _R()
    monkeypatch.setattr(RD, 'open_db', _Stub)
    html = RD._render_portal_mode_section(
        {'client_code': 'ACME', 'portal_mode': 'single'},
        lang='en',
    )
    assert 'No mode changes on record' in html
