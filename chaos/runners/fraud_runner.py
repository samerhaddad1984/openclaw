"""Fraud scenarios reuse the AuditRunner's seeding/detection pipeline."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from .audit_runner import AuditRunner
from ._base import RunnerResult


class FraudRunner(AuditRunner):
    track = "fraud"

    def __init__(self, *, chaos_db_path: Path, seed: int = 1337):
        super().__init__(chaos_db_path=chaos_db_path, seed=seed)
