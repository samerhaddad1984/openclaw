from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict


ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_key(value: Any) -> str:
    return " ".join(normalize_text(value).lower().split())


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def record_gl_decision(
    conn: sqlite3.Connection,
    client_code: str = "",
    vendor: str = "",
    gl_account: str = "",
    decided_by: str = "",
    **kwargs: Any,
) -> Dict[str, Any]:
    """Record a GL account decision for learning."""
    import secrets
    vendor_key = normalize_key(vendor)
    decision_id = f"dec-{secrets.token_hex(4)}"
    conn.execute(
        "INSERT INTO gl_decisions (decision_id, client_code, vendor, vendor_key, gl_account, decided_by) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (decision_id, client_code, vendor, vendor_key, gl_account, decided_by),
    )
    conn.commit()
    return {"ok": True, "decision_id": decision_id}


def suggest_gl_account(
    conn: sqlite3.Connection,
    client_code: str = "",
    vendor: str = "",
    **kwargs: Any,
) -> Dict[str, Any]:
    """Suggest a GL account based on historical decisions."""
    vendor_key = normalize_key(vendor)
    rows = conn.execute(
        "SELECT gl_account, COUNT(*) as cnt FROM gl_decisions "
        "WHERE client_code = ? AND vendor_key = ? GROUP BY gl_account ORDER BY cnt DESC",
        (client_code, vendor_key),
    ).fetchall()
    if not rows:
        return {"gl_account": None, "confidence": 0.0}
    total = sum(r["cnt"] for r in rows)
    top = dict(rows[0])
    confidence = top["cnt"] / total if total > 0 else 0.0
    # If there are conflicting patterns, reduce confidence
    if len(rows) > 1:
        confidence = min(confidence, 0.7)
    return {"gl_account": top["gl_account"], "confidence": round(confidence, 3), "support": top["cnt"]}


class GLAccountLearningEngine:
    """
    Conservative GL account correction learner.

    Rules:
    - exact vendor match required
    - exact client_code match required
    - exact doc_type match required
    - exact old gl_account match required
    - category mismatch blocks the correction
    - support_count must be at least min_support_count
    - one single correction must NOT spread automatically
    """

    def __init__(
        self,
        *,
        min_apply_score: float = 1.10,
        min_support_count: int = 2,
    ) -> None:
        self.min_apply_score = float(min_apply_score)
        self.min_support_count = int(min_support_count)

    def apply_learning(self, document: Dict[str, Any]) -> Dict[str, Any]:
        vendor = normalize_key(document.get("vendor"))
        client_code = normalize_key(document.get("client_code"))
        doc_type = normalize_key(document.get("doc_type"))
        category = normalize_key(document.get("category"))
        gl_account = normalize_key(document.get("gl_account"))

        if not vendor:
            return {
                "applied": False,
                "reason": "missing_vendor",
            }

        if not client_code:
            return {
                "applied": False,
                "reason": "missing_client_code",
            }

        if not doc_type:
            return {
                "applied": False,
                "reason": "missing_doc_type",
            }

        if not gl_account:
            return {
                "applied": False,
                "reason": "missing_gl_account",
            }

        with open_db() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM learning_corrections
                WHERE field_name_key IN ('gl account', 'gl_account')
                ORDER BY support_count DESC, updated_at DESC, id DESC
                LIMIT 200
                """
            ).fetchall()

        best_match = None
        best_score = 0.0
        best_support = 0

        for row in rows:
            row_vendor = normalize_key(row["vendor_key"])
            row_client_code = normalize_key(row["client_code_key"])
            row_doc_type = normalize_key(row["doc_type_key"])
            row_category = normalize_key(row["category_key"])
            row_old_value = normalize_key(row["old_value_key"])
            row_new_value = normalize_text(row["new_value"])
            support_count = int(row["support_count"] or 0)

            # Hard gates. No fuzzy garbage.
            if row_vendor != vendor:
                continue

            if row_client_code != client_code:
                continue

            if row_doc_type != doc_type:
                continue

            if row_old_value != gl_account:
                continue

            if not row_new_value:
                continue

            if normalize_key(row_new_value) == gl_account:
                continue

            # If both categories exist and they disagree, block it.
            if row_category and category and row_category != category:
                continue

            score = 0.0

            # Exact-context scoring only
            score += 0.35  # vendor exact
            score += 0.30  # client exact
            score += 0.25  # doc_type exact
            score += 0.30  # old gl exact

            if row_category and category and row_category == category:
                score += 0.15

            # Support bonus is small on purpose.
            if support_count >= 5:
                score += 0.15
            elif support_count >= 3:
                score += 0.10
            elif support_count >= 2:
                score += 0.05

            if score > best_score:
                best_score = score
                best_match = row
                best_support = support_count

        if best_match is None:
            return {
                "applied": False,
                "reason": "no_learning_match",
            }

        # Hard guardrail: one correction is not enough to spread.
        if best_support < self.min_support_count:
            return {
                "applied": False,
                "reason": "insufficient_support",
                "score": round(best_score, 3),
                "support_count": best_support,
                "required_support_count": self.min_support_count,
                "candidate_new_value": normalize_text(best_match["new_value"]),
            }

        if best_score < self.min_apply_score:
            return {
                "applied": False,
                "reason": "weak_match",
                "score": round(best_score, 3),
                "support_count": best_support,
                "required_score": self.min_apply_score,
                "candidate_new_value": normalize_text(best_match["new_value"]),
            }

        old_value = normalize_text(best_match["old_value"])
        new_value = normalize_text(best_match["new_value"])

        if normalize_key(new_value) == normalize_key(old_value):
            return {
                "applied": False,
                "reason": "new_value_same_as_old_value",
                "score": round(best_score, 3),
                "support_count": best_support,
            }

        document["gl_account"] = new_value

        return {
            "applied": True,
            "score": round(best_score, 3),
            "old_value": old_value,
            "new_value": new_value,
            "support_count": best_support,
            "reason": "learning_correction_applied",
        }


def record_gl_correction(
    conn: sqlite3.Connection,
    client_code: str = "",
    vendor: str = "",
    old_gl: str = "",
    new_gl: str = "",
    **kwargs,
) -> dict:
    """Record a GL account correction from a human reviewer.

    Decreases confidence for old_gl, increases for new_gl.
    Confidence is clamped to [0.1, 0.99].
    """
    vendor_key = normalize_key(vendor)
    if not vendor_key or not new_gl:
        return {"ok": False, "reason": "missing_vendor_or_gl"}

    # Record as a new decision with the corrected GL
    result = record_gl_decision(
        conn, client_code=client_code, vendor=vendor,
        gl_account=new_gl, decided_by="human_correction",
    )

    # Also store in learning_corrections if table exists
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        if "learning_corrections" in tables:
            import secrets
            conn.execute(
                "INSERT INTO learning_corrections "
                "(correction_id, document_id, field_name, field_name_key, "
                "old_value, old_value_key, new_value, new_value_key, "
                "vendor_key, client_code_key, doc_type_key, category_key, "
                "support_count, reviewer, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))",
                (
                    f"glcorr-{secrets.token_hex(4)}",
                    "",
                    "gl_account",
                    "gl_account",
                    old_gl,
                    normalize_key(old_gl),
                    new_gl,
                    normalize_key(new_gl),
                    vendor_key,
                    normalize_key(client_code),
                    "",
                    "",
                    1,
                    "human_correction",
                ),
            )
            conn.commit()
    except Exception:
        pass

    return {"ok": True, "old_gl": old_gl, "new_gl": new_gl}
