from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def append_jsonl(log_path: Path, obj: Dict[str, Any]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    record = dict(obj)
    record.setdefault("logged_at", _now_iso())
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")