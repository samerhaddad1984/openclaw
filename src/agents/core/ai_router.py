"""
ai_router.py — Unified AI call router for OtoCPA.

Routes every AI task through the Anthropic Claude API directly (Law 25
compliant — data stays with Anthropic, no third-party relay).  OpenRouter is
kept as a fallback only, used when the Anthropic API call fails.

Tier routing (per-task model mapping):
    Simple / high-confidence docs  → claude-haiku-4-5-20251001  (~$0.001/doc)
    Medium complexity docs         → claude-haiku-4-5-20251001  (~$0.002/doc)
    Complex / sensitive docs       → claude-sonnet-4-6          (~$0.015/doc)

Configuration (otocpa.config.json):
    openrouter       – { api_key, enabled, base_url }   (fallback only)
    ai_task_models   – { task_type: "model", … }

Public interface:
    ai_router.call(task_type, prompt, context, fallback_on_error=True,
                   username=None, document_id=None, image_bytes=None)
    → {"provider", "result", "latency_ms", "fallback_used", "error"}

    ai_router.get_cache_stats()
    → {"hit_rate", "total_requests", "cache_hits", "estimated_savings_usd",
       "total_entries", "active_entries"}

All external calls are:
  1. Prompt-sanitized  (SINs, account numbers stripped; $ amounts bucketed)
  2. Audit-logged      (audit_log table, created automatically)
  3. Response-cached   (ai_response_cache table; skips HTTP on hits)
  4. Memory-short-circuited for routine tasks with confident learning patterns
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import requests

try:
    import anthropic as _anthropic_sdk
except ImportError:
    _anthropic_sdk = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
CONFIG_PATH = ROOT_DIR / "otocpa.config.json"
DB_PATH = ROOT_DIR / "data" / "otocpa_agent.db"

# ---------------------------------------------------------------------------
# Default per-task model mapping (all via OpenRouter)
# ---------------------------------------------------------------------------
TASK_MODEL_MAP: dict[str, str] = {
    # --- Simple / cheap tasks → Haiku ---
    "pdf_extraction":          "claude-haiku-4-5-20251001",
    "image_ocr":               "claude-haiku-4-5-20251001",
    "gl_suggestion":           "claude-haiku-4-5-20251001",
    "vendor_identification":   "claude-haiku-4-5-20251001",
    "fraud_explanation":       "claude-haiku-4-5-20251001",
    "related_party_check":     "claude-haiku-4-5-20251001",
    "client_communication":    "claude-haiku-4-5-20251001",
    "tax_code_suggestion":     "claude-haiku-4-5-20251001",
    "substance_classification":"claude-haiku-4-5-20251001",
    "mixed_tax_detection":     "claude-haiku-4-5-20251001",
    "extract_invoice_lines":   "claude-haiku-4-5-20251001",
    # --- Complex / sensitive tasks → Sonnet ---
    "handwriting_ocr":         "claude-sonnet-4-6",
    "going_concern_analysis":  "claude-sonnet-4-6",
    # 4-tier invoice extraction (complexity-routed from ocr_engine)
    "invoice_extraction_simple":       "claude-haiku-4-5-20251001",
    "invoice_extraction_medium":       "claude-haiku-4-5-20251001",
    "invoice_extraction_complex":      "claude-sonnet-4-6",
    "invoice_extraction_very_complex": "claude-sonnet-4-6",
    # Legacy task names mapped to cheap Haiku
    "classify_document":       "claude-haiku-4-5-20251001",
    "extract_vendor":          "claude-haiku-4-5-20251001",
    "suggest_gl":              "claude-haiku-4-5-20251001",
    "explain_anomaly":         "claude-haiku-4-5-20251001",
    "escalation_decision":     "claude-haiku-4-5-20251001",
    "compliance_narrative":    "claude-haiku-4-5-20251001",
    "working_paper":           "claude-haiku-4-5-20251001",
}

# Image-capable tasks — these include base64 image in the message content
_IMAGE_TASKS: frozenset[str] = frozenset({"image_ocr", "handwriting_ocr"})

# Default OpenRouter base URL
_DEFAULT_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"

# Backwards compatibility: keep these for tests that import them
_DEFAULT_ROUTINE_TASKS: set[str] = {"classify_document", "extract_vendor", "suggest_gl"}
_DEFAULT_COMPLEX_TASKS: set[str] = {
    "explain_anomaly",
    "escalation_decision",
    "compliance_narrative",
    "working_paper",
    "substance_classification",
    "related_party_check",
    "mixed_tax_detection",
}

# ---------------------------------------------------------------------------
# Cache constants
# ---------------------------------------------------------------------------

# These tasks produce user-specific or time-sensitive output — never cache them
# BLOCK 7: mixed_tax_detection never cached (invoice content always unique)
_NEVER_CACHE_TASKS: frozenset[str] = frozenset({"draft_client_message", "escalation_decision", "mixed_tax_detection"})

# Cache TTL per task type (days); anything not listed uses _DEFAULT_CACHE_TTL_DAYS
# BLOCK 7: substance_classification=7 days, related_party_check=30 days
_CACHE_TTL_DAYS: dict[str, int] = {
    "classify_document":        30,
    "extract_vendor":           30,
    "suggest_gl":               30,
    "explain_anomaly":           7,
    "compliance_narrative":      7,
    "working_paper":             7,
    "substance_classification":  7,
    "related_party_check":      30,
}
_DEFAULT_CACHE_TTL_DAYS: int = 7

# Learning-memory short-circuit: only apply to these routine task types
_MEMORY_SHORTCIRCUIT_TASKS: frozenset[str] = frozenset(
    {"classify_document", "extract_vendor", "suggest_gl"}
)
# Minimum accumulated outcomes before trusting the memory pattern
_MEMORY_MIN_OUTCOMES: int = 5
# Minimum success rate (success_count / outcome_count) required
_MEMORY_MIN_SUCCESS_RATE: float = 0.85

# ---------------------------------------------------------------------------
# Prompt sanitizer
# ---------------------------------------------------------------------------

# Canadian SIN  –  formatted: 123-456-789  or  123 456 789
_SIN_RE = re.compile(r"\b\d{3}[-\s]\d{3}[-\s]\d{3}\b")

# Bank / account numbers  –  8-17 consecutive digits not part of a longer run
_ACCT_RE = re.compile(r"(?<!\d)\d{8,17}(?!\d)")

# Dollar amounts that start with $  (e.g.  $1,234.56  $500  $ 99.00)
_AMOUNT_RE = re.compile(
    r"\$\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?(?!\d)"
    r"|\$\s*\d+(?:\.\d{2})?(?!\d)"
)


def _amount_to_range(m: re.Match) -> str:
    raw = m.group(0).replace("$", "").replace(",", "").strip()
    try:
        val = float(raw)
    except ValueError:
        return m.group(0)

    if val < 100:
        return "<$100"
    if val < 500:
        return "$100–$499"
    if val < 1_000:
        return "$500–$999"
    if val < 5_000:
        return "$1,000–$4,999"
    if val < 10_000:
        return "$5,000–$9,999"
    if val < 50_000:
        return "$10,000–$49,999"
    if val < 100_000:
        return "$50,000–$99,999"
    return "$100,000+"


def sanitize_prompt(text: str) -> str:
    """Remove PII and replace exact dollar amounts with ranges."""
    text = _SIN_RE.sub("[SIN-REDACTED]", text)
    text = _ACCT_RE.sub("[ACCT-REDACTED]", text)
    text = _AMOUNT_RE.sub(_amount_to_range, text)
    return text


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

_AUDIT_CREATE = """
CREATE TABLE IF NOT EXISTS audit_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type     TEXT    NOT NULL DEFAULT 'ai_call',
    username       TEXT,
    document_id    TEXT,
    provider       TEXT,
    task_type      TEXT,
    prompt_snippet TEXT,
    latency_ms     INTEGER,
    created_at     TEXT    NOT NULL
)
"""


def _write_audit_log(
    *,
    event_type: str = "ai_call",
    username: str | None,
    document_id: str | None,
    provider: str,
    task_type: str,
    prompt_snippet: str,
    latency_ms: int,
) -> None:
    """Fire-and-forget audit write; never raises."""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            conn.execute(_AUDIT_CREATE)
            conn.execute(
                """INSERT INTO audit_log
                   (event_type, username, document_id, provider,
                    task_type, prompt_snippet, latency_ms, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event_type,
                    username or "",
                    document_id or "",
                    provider,
                    task_type,
                    prompt_snippet[:500],
                    latency_ms,
                    datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass  # audit must never break the calling code


# ---------------------------------------------------------------------------
# Response cache (SQLite-backed)
# ---------------------------------------------------------------------------

_CACHE_CREATE = """
CREATE TABLE IF NOT EXISTS ai_response_cache (
    cache_key     TEXT PRIMARY KEY,
    task_type     TEXT NOT NULL,
    provider      TEXT NOT NULL,
    response_json TEXT NOT NULL,
    hit_count     INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL,
    last_used_at  TEXT NOT NULL,
    expires_at    TEXT NOT NULL
)
"""


def _cache_key(task_type: str, sanitized_prompt: str) -> str:
    """SHA-256 of (task_type + NUL + sanitized_prompt)."""
    raw = f"{task_type}\x00{sanitized_prompt}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _ensure_cache_table(conn: sqlite3.Connection) -> None:
    conn.execute(_CACHE_CREATE)


def _check_cache(key: str) -> str | None:
    """Return cached response text if valid and not expired, else None."""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            _ensure_cache_table(conn)
            now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            row = conn.execute(
                """SELECT response_json FROM ai_response_cache
                   WHERE cache_key = ? AND expires_at > ?""",
                (key, now_iso),
            ).fetchone()
            if row:
                conn.execute(
                    """UPDATE ai_response_cache
                       SET hit_count = hit_count + 1, last_used_at = ?
                       WHERE cache_key = ?""",
                    (now_iso, key),
                )
                conn.commit()
                return row[0]
        finally:
            conn.close()
    except Exception:
        pass
    return None


def _store_cache(key: str, task_type: str, provider: str, response_text: str, ttl_days: int) -> None:
    """Store an AI response in the cache. Fire-and-forget."""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            _ensure_cache_table(conn)
            now = datetime.now(timezone.utc).replace(microsecond=0)
            expires = now + timedelta(days=ttl_days)
            now_iso = now.isoformat()
            expires_iso = expires.isoformat()
            conn.execute(
                """INSERT OR REPLACE INTO ai_response_cache
                   (cache_key, task_type, provider, response_json,
                    hit_count, created_at, last_used_at, expires_at)
                   VALUES (?, ?, ?, ?, 0, ?, ?, ?)""",
                (key, task_type, provider, response_text, now_iso, now_iso, expires_iso),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


def _clear_cache() -> int:
    """Delete all cache rows. Returns number of rows deleted."""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            _ensure_cache_table(conn)
            cur = conn.execute("DELETE FROM ai_response_cache")
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()
    except Exception:
        return 0


def get_cache_stats() -> dict[str, Any]:
    """
    Return cache performance statistics.

    Keys returned
    -------------
    hit_rate              – fraction of audit_log calls that were cache hits
    total_requests        – total ai_call + cache_hit rows in audit_log
    cache_hits            – cache_hit rows in audit_log
    estimated_savings_usd – rough estimate (hits × $0.002 avg call cost)
    total_entries         – rows in ai_response_cache (including expired)
    active_entries        – non-expired rows
    """
    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            _ensure_cache_table(conn)

            total_entries = int(
                conn.execute("SELECT COUNT(*) FROM ai_response_cache").fetchone()[0]
            )
            total_hits_db = int(
                conn.execute(
                    "SELECT COALESCE(SUM(hit_count), 0) FROM ai_response_cache"
                ).fetchone()[0]
            )
            now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            active_entries = int(
                conn.execute(
                    "SELECT COUNT(*) FROM ai_response_cache WHERE expires_at > ?",
                    (now_iso,),
                ).fetchone()[0]
            )

            audit_total = 0
            cache_hits = 0
            try:
                conn.execute(_AUDIT_CREATE)
                audit_total = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM audit_log WHERE event_type IN ('ai_call','cache_hit','memory_shortcircuit')"
                    ).fetchone()[0]
                )
                cache_hits = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM audit_log WHERE event_type = 'cache_hit'"
                    ).fetchone()[0]
                )
            except Exception:
                pass

            hit_rate = round(cache_hits / audit_total, 4) if audit_total > 0 else 0.0
            estimated_savings_usd = round(total_hits_db * 0.002, 4)

            return {
                "hit_rate": hit_rate,
                "total_requests": audit_total,
                "cache_hits": cache_hits,
                "estimated_savings_usd": estimated_savings_usd,
                "total_entries": total_entries,
                "active_entries": active_entries,
            }
        finally:
            conn.close()
    except Exception as exc:
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Database-driven provider helpers
# ---------------------------------------------------------------------------

def get_providers_for_task(task_type: str, conn: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
    """Return ordered list of enabled providers assigned to *task_type*.

    Each dict has keys: provider_id, name, api_url, api_key, model,
    api_format, enabled, notes, priority.
    """
    should_close = False
    if conn is None:
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.row_factory = sqlite3.Row
            should_close = True
        except Exception:
            return []
    try:
        rows = conn.execute(
            """SELECT p.provider_id, p.name, p.api_url, p.api_key, p.model,
                      p.api_format, p.enabled, p.notes, a.priority
               FROM ai_providers p
               JOIN ai_task_assignments a ON p.provider_id = a.provider_id
               WHERE a.task_type = ? AND p.enabled = 1
               ORDER BY a.priority ASC""",
            (task_type,),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        if should_close:
            conn.close()


def call_provider(
    provider: dict[str, Any],
    prompt: str,
    image_bytes: bytes | None = None,
    *,
    system_prompt: str = "",
    timeout: int = 90,
) -> str:
    """Call a provider using its api_format.

    Supported formats: openai (default), anthropic, google.
    Returns the response text content.
    """
    api_format = (provider.get("api_format") or "openai").lower()
    api_url = provider["api_url"]
    api_key = provider["api_key"]
    model = provider["model"]

    if api_format == "anthropic":
        return _call_anthropic(
            api_url=api_url, api_key=api_key, model=model,
            system_prompt=system_prompt, user_prompt=prompt,
            image_bytes=image_bytes, timeout=timeout,
        )
    elif api_format == "google":
        return _call_google(
            api_url=api_url, api_key=api_key,
            system_prompt=system_prompt, user_prompt=prompt,
            image_bytes=image_bytes, timeout=timeout,
        )
    else:
        # openai format (works with OpenRouter, DeepSeek, most providers)
        return _call_openai(
            base_url=api_url, api_key=api_key, model=model,
            system_prompt=system_prompt, user_prompt=prompt,
            image_bytes=image_bytes, timeout=timeout,
        )


def test_provider(provider: dict[str, Any]) -> dict[str, Any]:
    """Send a simple test message to a provider.

    Returns: {ok: bool, response_time_ms: int, error: str | None}
    """
    t0 = time.monotonic()
    try:
        result = call_provider(
            provider, "Reply with OK",
            system_prompt="You are a test assistant. Reply with exactly: OK",
            timeout=30,
        )
        ms = int((time.monotonic() - t0) * 1_000)
        return {"ok": bool(result), "response_time_ms": ms, "error": None}
    except Exception as exc:
        ms = int((time.monotonic() - t0) * 1_000)
        return {"ok": False, "response_time_ms": ms, "error": str(exc)}


# ---------------------------------------------------------------------------
# Provider HTTP calls — one per API format
# ---------------------------------------------------------------------------

def _call_openai(
    *,
    base_url: str,
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    image_bytes: bytes | None = None,
    timeout: int = 90,
) -> str:
    """POST to any OpenAI-compatible /chat/completions endpoint."""
    url = base_url.rstrip("/")
    if not url.endswith("/chat/completions"):
        url = f"{url}/chat/completions"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    # OpenRouter requires HTTP-Referer and X-Title for some plans
    if "openrouter.ai" in url:
        headers["HTTP-Referer"] = "https://otocpa.com"
        headers["X-Title"] = "OtoCPA"

    if image_bytes:
        b64 = base64.b64encode(image_bytes).decode("ascii")
        user_content: Any = [
            {"type": "text", "text": user_prompt},
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{b64}"},
            },
        ]
    else:
        user_content = user_prompt

    payload = {
        "model": model,
        "temperature": 0.0,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_content},
        ],
    }

    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if r.status_code == 401:
        raise RuntimeError(
            f"Provider HTTP 401 Unauthorized — API key invalid or account not found. "
            f"Update OPENROUTER_API_KEY env var or openrouter.api_key in config. "
            f"Response: {r.text[:200]}"
        )
    if r.status_code != 200:
        raise RuntimeError(f"Provider HTTP {r.status_code}: {r.text[:400]}")

    data = r.json()
    return data["choices"][0]["message"]["content"]


def _call_anthropic(
    *,
    api_url: str,
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    image_bytes: bytes | None = None,
    timeout: int = 90,
) -> str:
    """POST to the Anthropic native messages API."""
    url = api_url.rstrip("/")
    if not url.endswith("/messages"):
        url = f"{url}/v1/messages"

    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }

    content: Any
    if image_bytes:
        b64 = base64.b64encode(image_bytes).decode("ascii")
        content = [
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": b64}},
            {"type": "text", "text": user_prompt},
        ]
    else:
        content = user_prompt

    payload: dict[str, Any] = {
        "model": model,
        "max_tokens": 4096,
        "messages": [{"role": "user", "content": content}],
    }
    if system_prompt:
        payload["system"] = system_prompt

    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"Anthropic HTTP {r.status_code}: {r.text[:400]}")

    data = r.json()
    # Anthropic returns content as a list of blocks
    blocks = data.get("content", [])
    return "".join(b.get("text", "") for b in blocks if b.get("type") == "text")


def _call_google(
    *,
    api_url: str,
    api_key: str,
    system_prompt: str,
    user_prompt: str,
    image_bytes: bytes | None = None,
    timeout: int = 90,
) -> str:
    """POST to the Google Gemini generateContent API."""
    url = api_url.rstrip("/")
    if "?" not in url:
        url = f"{url}?key={api_key}"

    headers = {"Content-Type": "application/json"}

    parts: list[dict[str, Any]] = []
    if system_prompt:
        parts.append({"text": f"{system_prompt}\n\n{user_prompt}"})
    else:
        parts.append({"text": user_prompt})

    if image_bytes:
        b64 = base64.b64encode(image_bytes).decode("ascii")
        parts.append({"inline_data": {"mime_type": "image/png", "data": b64}})

    payload = {"contents": [{"parts": parts}]}

    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"Google HTTP {r.status_code}: {r.text[:400]}")

    data = r.json()
    candidates = data.get("candidates", [])
    if candidates:
        parts_out = candidates[0].get("content", {}).get("parts", [])
        return "".join(p.get("text", "") for p in parts_out)
    return ""


# Backwards-compatibility alias used by existing code
def _call_provider(
    *,
    base_url: str,
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    image_bytes: bytes | None = None,
    timeout: int = 90,
) -> str:
    """Legacy wrapper — delegates to _call_openai."""
    return _call_openai(
        base_url=base_url, api_key=api_key, model=model,
        system_prompt=system_prompt, user_prompt=user_prompt,
        image_bytes=image_bytes, timeout=timeout,
    )


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_config() -> dict[str, Any]:
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Learning memory short-circuit helper
# ---------------------------------------------------------------------------

def _check_memory_shortcircuit(
    task_type: str,
    context: dict[str, Any] | None,
) -> str | None:
    """
    For routine tasks, query learning_memory_patterns for a high-confidence
    prior outcome.  If one exists, synthesize a JSON response and return it
    directly so we skip the AI HTTP call entirely.

    Returns a JSON string (matching typical AI response format) or None.
    """
    if task_type not in _MEMORY_SHORTCIRCUIT_TASKS:
        return None
    if not context:
        return None

    vendor = str(context.get("vendor") or context.get("vendor_name") or "").strip().casefold()
    client_code = str(context.get("client_code") or "").strip().casefold()
    if not vendor:
        return None

    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            row = conn.execute(
                """SELECT gl_account, tax_code, category, doc_type,
                          outcome_count, success_count, avg_confidence
                   FROM learning_memory_patterns
                   WHERE vendor_key = ?
                     AND (client_code_key = '' OR client_code_key = ?)
                     AND outcome_count >= ?
                   ORDER BY
                     CASE WHEN client_code_key = ? THEN 0 ELSE 1 END,
                     success_count DESC,
                     outcome_count DESC
                   LIMIT 1""",
                (vendor, client_code, _MEMORY_MIN_OUTCOMES, client_code),
            ).fetchone()
        finally:
            conn.close()
    except Exception:
        return None

    if row is None:
        return None

    outcome_count = int(row[4] or 0)
    success_count = int(row[5] or 0)
    if outcome_count < _MEMORY_MIN_OUTCOMES:
        return None
    success_rate = success_count / outcome_count
    if success_rate < _MEMORY_MIN_SUCCESS_RATE:
        return None

    # Synthesize a response matching the typical AI extraction JSON format
    synthesized: dict[str, Any] = {
        "gl_account":  row[0] or "",
        "tax_code":    row[1] or "",
        "category":    row[2] or "",
        "doc_type":    row[3] or "",
        "confidence":  round(float(row[6] or 0.0), 4),
        "source":      "learning_memory",
        "outcome_count": outcome_count,
        "success_rate":  round(success_rate, 4),
    }
    return json.dumps(synthesized, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

class AIRouter:
    """
    Stateless-ish router; config is loaded once at construction time.
    Re-instantiate (or call _reset()) to pick up config file changes.
    """

    def __init__(self) -> None:
        cfg = _load_config()

        # --- Anthropic (primary) ---
        self._anthropic_api_key: str = os.environ.get("ANTHROPIC_API_KEY", "").strip()

        # --- OpenRouter (fallback) ---
        or_cfg = cfg.get("openrouter", {})
        self._base_url: str = or_cfg.get("base_url", or_cfg.get("url", _DEFAULT_BASE_URL)).strip()
        self._enabled: bool = bool(or_cfg.get("enabled", False))

        # API key resolution — env var always wins, config is fallback
        env_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
        cfg_key = or_cfg.get("api_key", "").strip()

        # Backwards compat: also check old ai_router section for api_key
        old_cfg = cfg.get("ai_router", {})
        old_key = (
            old_cfg.get("premium_provider", {}).get("api_key", "").strip()
            or old_cfg.get("routine_provider", {}).get("api_key", "").strip()
        )

        self._api_key: str = env_key or cfg_key or old_key

        # Per-task model overrides from config
        self._task_models: dict[str, str] = dict(TASK_MODEL_MAP)
        config_models = cfg.get("ai_task_models", {})
        if isinstance(config_models, dict):
            for task, model in config_models.items():
                if isinstance(model, str) and model.strip():
                    self._task_models[task] = model.strip()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_model_for_task(self, task_type: str) -> str:
        """Return the model string for a given task type."""
        return self._task_models.get(task_type, "claude-haiku-4-5-20251001")

    def call(
        self,
        task_type: str,
        prompt: str,
        context: dict[str, Any] | None = None,
        fallback_on_error: bool = True,
        *,
        username: str | None = None,
        document_id: str | None = None,
        image_bytes: bytes | None = None,
    ) -> dict[str, Any]:
        """
        Route an AI task to OpenRouter with the appropriate model.

        Parameters
        ----------
        task_type:
            Determines which model to use from TASK_MODEL_MAP / config.
        prompt:
            Human-readable task description or question.
        context:
            Optional structured data dict appended to the prompt as JSON.
        fallback_on_error:
            Kept for API compatibility (unused — single provider).
        username / document_id:
            Written to audit_log for traceability.
        image_bytes:
            Raw image bytes for vision tasks (image_ocr, handwriting_ocr).

        Returns
        -------
        dict with keys:
            provider    – model name used
            result      – raw text from the model (or None on error)
            latency_ms  – wall-clock ms for the successful call
            fallback_used – always False (single provider)
            error       – error message string (or None on success)
        """
        model = self.get_model_for_task(task_type)

        # Build full prompt and sanitize
        full_prompt = prompt
        if context:
            full_prompt = (
                f"{prompt}\n\nContext:\n"
                f"{json.dumps(context, ensure_ascii=False, indent=2)}"
            )
        clean_prompt = sanitize_prompt(full_prompt)

        # ------------------------------------------------------------------
        # OPTIMIZATION 2 — Learning memory short-circuit
        # Skip the HTTP call entirely when a high-confidence prior exists
        # ------------------------------------------------------------------
        memory_result = _check_memory_shortcircuit(task_type, context)
        if memory_result is not None:
            _write_audit_log(
                event_type="memory_shortcircuit",
                username=username,
                document_id=document_id,
                provider="learning_memory",
                task_type=task_type,
                prompt_snippet=clean_prompt,
                latency_ms=0,
            )
            return {
                "provider": "learning_memory",
                "result": memory_result,
                "latency_ms": 0,
                "fallback_used": False,
                "error": None,
            }

        # ------------------------------------------------------------------
        # OPTIMIZATION 1 — Response cache
        # Check cache before making any HTTP call
        # ------------------------------------------------------------------
        use_cache = task_type not in _NEVER_CACHE_TASKS and image_bytes is None
        ck = _cache_key(task_type, clean_prompt) if use_cache else ""

        if use_cache:
            cached = _check_cache(ck)
            if cached is not None:
                _write_audit_log(
                    event_type="cache_hit",
                    username=username,
                    document_id=document_id,
                    provider=f"cache:{model}",
                    task_type=task_type,
                    prompt_snippet=clean_prompt,
                    latency_ms=0,
                )
                return {
                    "provider": f"cache:{model}",
                    "result": cached,
                    "latency_ms": 0,
                    "fallback_used": False,
                    "error": None,
                }

        # ------------------------------------------------------------------
        # DB-driven provider chain — try each assigned provider in priority
        # ------------------------------------------------------------------
        system_prompt = (
            "You are a precise accounting AI assistant. "
            f"Task: {task_type}. "
            "Respond concisely and accurately. "
            "If the response must be structured data, return valid JSON only."
        )

        db_providers = get_providers_for_task(task_type)
        fallback_used = False
        last_error = None

        for idx, prov in enumerate(db_providers):
            t0 = time.monotonic()
            try:
                content = call_provider(
                    prov, clean_prompt,
                    image_bytes=image_bytes if task_type in _IMAGE_TASKS else None,
                    system_prompt=system_prompt,
                )
                latency_ms = int((time.monotonic() - t0) * 1_000)
                prov_label = f"{prov['name']}:{prov['model']}"

                if use_cache:
                    ttl = _CACHE_TTL_DAYS.get(task_type, _DEFAULT_CACHE_TTL_DAYS)
                    _store_cache(ck, task_type, prov_label, content, ttl)

                _write_audit_log(
                    username=username,
                    document_id=document_id,
                    provider=prov_label,
                    task_type=task_type,
                    prompt_snippet=clean_prompt,
                    latency_ms=latency_ms,
                )
                return {
                    "provider": prov_label,
                    "result": content,
                    "latency_ms": latency_ms,
                    "fallback_used": fallback_used,
                    "error": None,
                }
            except Exception as exc:
                last_error = str(exc)
                fallback_used = True
                continue

        # ------------------------------------------------------------------
        # Legacy fallback — config-based OpenRouter call
        # ------------------------------------------------------------------
        result, error, latency_ms = self._try_call(
            model=model,
            task_type=task_type,
            prompt=clean_prompt,
            image_bytes=image_bytes,
        )

        if use_cache and result is not None and error is None:
            ttl = _CACHE_TTL_DAYS.get(task_type, _DEFAULT_CACHE_TTL_DAYS)
            _store_cache(ck, task_type, model, result, ttl)

        _write_audit_log(
            username=username,
            document_id=document_id,
            provider=model,
            task_type=task_type,
            prompt_snippet=clean_prompt,
            latency_ms=latency_ms,
        )

        return {
            "provider": model,
            "result": result,
            "latency_ms": latency_ms,
            "fallback_used": fallback_used,
            "error": error,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _try_anthropic_sdk(
        self,
        *,
        model: str,
        system_prompt: str,
        prompt: str,
        image_bytes: bytes | None = None,
    ) -> str:
        """Call Anthropic API directly via the anthropic Python SDK.

        Raises on any failure so the caller can fall back to OpenRouter.
        """
        if _anthropic_sdk is None:
            raise RuntimeError("anthropic package not installed")
        if not self._anthropic_api_key:
            raise RuntimeError("ANTHROPIC_API_KEY not set")

        client = _anthropic_sdk.Anthropic(api_key=self._anthropic_api_key)

        content: Any
        if image_bytes:
            b64 = base64.b64encode(image_bytes).decode("ascii")
            content = [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": b64}},
                {"type": "text", "text": prompt},
            ]
        else:
            content = prompt

        msg = client.messages.create(
            model=model,
            max_tokens=4096,
            system=system_prompt,
            messages=[{"role": "user", "content": content}],
        )
        return "".join(b.text for b in msg.content if b.type == "text")

    def _try_call(
        self,
        *,
        model: str,
        task_type: str,
        prompt: str,
        image_bytes: bytes | None = None,
    ) -> tuple[str | None, str | None, int]:
        """Returns (result, error, latency_ms).

        Strategy: DeepSeek V3.2 (AWS Bedrock) first → Anthropic SDK → OpenRouter fallback.
        """
        system_prompt = (
            "You are a precise accounting AI assistant. "
            f"Task: {task_type}. "
            "Respond concisely and accurately. "
            "If the response must be structured data, return valid JSON only."
        )

        # --- Primary: DeepSeek V3.2 on AWS Bedrock ---
        # Text-only tasks; vision tasks skip DeepSeek and fall through.
        if image_bytes is None and os.environ.get("AWS_BEARER_TOKEN_BEDROCK"):
            t0 = time.monotonic()
            try:
                from src.engines.deepseek_client import call_deepseek
                ds_result = call_deepseek(prompt, system=system_prompt)
                latency_ms = int((time.monotonic() - t0) * 1_000)
                if isinstance(ds_result, dict) and "text" in ds_result and len(ds_result) == 1:
                    content = ds_result["text"]
                else:
                    content = json.dumps(ds_result, ensure_ascii=False)
                return content, None, latency_ms
            except Exception:
                pass  # fall through to Anthropic

        # --- Secondary: Anthropic SDK ---
        t0 = time.monotonic()
        try:
            content = self._try_anthropic_sdk(
                model=model,
                system_prompt=system_prompt,
                prompt=prompt,
                image_bytes=image_bytes if task_type in _IMAGE_TASKS else None,
            )
            latency_ms = int((time.monotonic() - t0) * 1_000)
            return content, None, latency_ms
        except Exception:
            pass  # fall through to OpenRouter

        # --- Fallback: OpenRouter ---
        if not self._api_key:
            return None, "anthropic_and_openrouter_not_configured", 0

        # Map native Claude model IDs to OpenRouter-prefixed names for fallback
        or_model = model
        if not any(model.startswith(p) for p in ("anthropic/", "google/", "openai/", "deepseek/")):
            or_model = f"anthropic/{model}"

        t0 = time.monotonic()
        try:
            content = _call_provider(
                base_url=self._base_url,
                api_key=self._api_key,
                model=or_model,
                system_prompt=system_prompt,
                user_prompt=prompt,
                image_bytes=image_bytes if task_type in _IMAGE_TASKS else None,
            )
            latency_ms = int((time.monotonic() - t0) * 1_000)
            return content, None, latency_ms
        except Exception as exc:
            latency_ms = int((time.monotonic() - t0) * 1_000)
            return None, str(exc), latency_ms

    # Backwards compatibility shims
    def _pick_provider(self, task_type: str) -> tuple[str, dict[str, str]]:
        """Legacy method kept for test compatibility."""
        model = self.get_model_for_task(task_type)
        return model, {"base_url": self._base_url, "api_key": self._api_key, "model": model}


# ---------------------------------------------------------------------------
# Prompt template loader
# ---------------------------------------------------------------------------

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"


def _load_prompt_template(name: str) -> str:
    """Load a prompt template from src/agents/prompts/{name}.txt."""
    path = _PROMPTS_DIR / f"{name}.txt"
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Specialized AI-assisted task helpers
# ---------------------------------------------------------------------------

def call_substance_classification(
    *,
    vendor: str = "",
    amount: Any = None,
    memo: str = "",
    doc_type: str = "",
    province: str = "",
    username: str | None = None,
    document_id: str | None = None,
) -> dict[str, Any]:
    """
    AI-assisted economic substance classification.

    Returns dict with keys: classification, confidence, gl_suggestion, reasoning.
    On error or unavailable AI, returns None-valued result with error.
    """
    template = _load_prompt_template("substance_classification")
    prompt = template.format(
        VENDOR=vendor or "Unknown",
        AMOUNT=str(amount) if amount is not None else "Unknown",
        MEMO=memo or "N/A",
        DOC_TYPE=doc_type or "Unknown",
        PROVINCE=province or "QC",
    )
    result = call(
        "substance_classification",
        prompt,
        context={"vendor": vendor, "amount": amount, "memo": memo, "doc_type": doc_type},
        username=username,
        document_id=document_id,
    )
    # Try to parse JSON from AI response
    parsed = _try_parse_json(result.get("result"))
    return {
        "classification": parsed.get("classification") if parsed else None,
        "confidence": parsed.get("confidence") if parsed else None,
        "gl_suggestion": parsed.get("gl_suggestion") if parsed else None,
        "reasoning": parsed.get("reasoning") if parsed else None,
        "provider": result.get("provider"),
        "error": result.get("error"),
        "latency_ms": result.get("latency_ms", 0),
    }


def call_related_party_check(
    *,
    vendor: str,
    related_parties: list[str],
    owner_names: list[str] | None = None,
    username: str | None = None,
    document_id: str | None = None,
) -> dict[str, Any]:
    """
    AI-assisted related party check.

    Returns dict with keys: is_related_party, confidence, reasoning.
    """
    template = _load_prompt_template("related_party_check")
    parties_text = "\n".join(f"  - {p}" for p in related_parties) if related_parties else "  (none registered)"
    owners_text = ", ".join(owner_names) if owner_names else "(not provided)"
    prompt = template.format(
        VENDOR=vendor,
        OWNER_NAMES=owners_text,
        RELATED_PARTIES=parties_text,
    )
    result = call(
        "related_party_check",
        prompt,
        context={"vendor": vendor, "related_parties": related_parties},
        username=username,
        document_id=document_id,
    )
    parsed = _try_parse_json(result.get("result"))
    return {
        "is_related_party": parsed.get("is_related_party") if parsed else None,
        "confidence": parsed.get("confidence") if parsed else None,
        "reasoning": parsed.get("reasoning") if parsed else None,
        "provider": result.get("provider"),
        "error": result.get("error"),
        "latency_ms": result.get("latency_ms", 0),
    }


def call_mixed_tax_detection(
    *,
    invoice_text: str,
    username: str | None = None,
    document_id: str | None = None,
) -> dict[str, Any]:
    """
    AI-assisted mixed tax detection for invoices.

    Returns dict with keys: is_mixed, taxable_items, exempt_items, suggested_allocation.
    """
    template = _load_prompt_template("mixed_tax_detection")
    prompt = template.format(
        INVOICE_TEXT=invoice_text[:4000],  # limit to avoid token overflow
    )
    result = call(
        "mixed_tax_detection",
        prompt,
        context={"invoice_text_length": len(invoice_text)},
        username=username,
        document_id=document_id,
    )
    parsed = _try_parse_json(result.get("result"))
    return {
        "is_mixed": parsed.get("is_mixed") if parsed else None,
        "taxable_items": parsed.get("taxable_items") if parsed else None,
        "exempt_items": parsed.get("exempt_items") if parsed else None,
        "suggested_allocation": parsed.get("suggested_allocation") if parsed else None,
        "provider": result.get("provider"),
        "error": result.get("error"),
        "latency_ms": result.get("latency_ms", 0),
    }


def _try_parse_json(text: str | None) -> dict[str, Any] | None:
    """Try to extract a JSON object from AI response text."""
    if not text:
        return None
    # Strip markdown code fences if present
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        # Remove first and last lines (``` markers)
        lines = [l for l in lines if not l.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to find JSON object within the text
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(cleaned[start:end + 1])
            except json.JSONDecodeError:
                pass
    return None


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Public helper — secure API key retrieval
# ---------------------------------------------------------------------------

def get_api_key() -> str:
    """Return the active AI API key without ever logging its value.

    Priority:
        1. ANTHROPIC_API_KEY environment variable (primary — Law 25 compliant)
        2. OPENROUTER_API_KEY environment variable (fallback)
        3. otocpa.config.json → openrouter.api_key (backward compat)
    """
    # Check Anthropic first (primary provider)
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if key:
        return key
    # Then OpenRouter (fallback)
    key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if key:
        return key
    # Fallback to config (for backward compatibility)
    try:
        cfg = _load_config()
        key = cfg.get("openrouter", {}).get("api_key", "").strip()
        if key:
            return key
        # Also check legacy locations
        key = cfg.get("ai_router", {}).get("premium_provider", {}).get("api_key", "").strip()
        if key:
            return key
        key = cfg.get("ai_router", {}).get("routine_provider", {}).get("api_key", "").strip()
        if key:
            return key
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# Module-level singleton + convenience function
# ---------------------------------------------------------------------------

_router: AIRouter | None = None


def _get_router() -> AIRouter:
    global _router
    if _router is None:
        _router = AIRouter()
    return _router


def _reset() -> None:
    """Force reload of config (useful after otocpa.config.json changes)."""
    global _router
    _router = None


def call(
    task_type: str,
    prompt: str,
    context: dict[str, Any] | None = None,
    fallback_on_error: bool = True,
    *,
    username: str | None = None,
    document_id: str | None = None,
    image_bytes: bytes | None = None,
) -> dict[str, Any]:
    """
    Module-level convenience wrapper around AIRouter.call().

    Usage::

        from src.agents.core import ai_router

        out = ai_router.call(
            "escalation_decision",
            "Should this invoice be posted?",
            context={"vendor": "ACME", "amount": 4500},
            document_id="doc-123",
        )
        print(out["result"])   # raw model text
    """
    return _get_router().call(
        task_type,
        prompt,
        context,
        fallback_on_error,
        username=username,
        document_id=document_id,
        image_bytes=image_bytes,
    )
