"""DeepSeek V3.2 (AWS Bedrock) client + validation-aware AI call wrappers.

``call_deepseek`` invokes DeepSeek V3.2 on AWS Bedrock using a Bedrock
API bearer token (``AWS_BEARER_TOKEN_BEDROCK``). Returns parsed JSON
when the model emits JSON, else wraps the raw text under ``{"text": ...}``.

``call_with_validation`` retries any JSON-returning callable when the
validator rejects its output, appending the error context so the model
can self-correct on the next attempt.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests


_DEFAULT_BEDROCK_REGION = "us-west-2"
_DEFAULT_DEEPSEEK_MODEL = "deepseek.v3-v1:0"


def call_deepseek(prompt: str, *, max_tokens: int = 2048,
                  temperature: float = 0.0, system: Optional[str] = None,
                  timeout: float = 60.0) -> Dict[str, Any]:
    """Call DeepSeek V3.2 on AWS Bedrock and return parsed JSON result.

    Uses the Bedrock ``converse`` API with a bearer token
    (``AWS_BEARER_TOKEN_BEDROCK``). Region / model id are overridable
    via ``AWS_REGION`` and ``DEEPSEEK_MODEL_ID``.

    If the model's reply contains a JSON object/array it is parsed and
    returned. Otherwise the raw text is returned as ``{"text": ...}``.
    """
    token = os.environ.get("AWS_BEARER_TOKEN_BEDROCK", "").strip()
    if not token:
        raise RuntimeError("AWS_BEARER_TOKEN_BEDROCK not set")

    region = os.environ.get("AWS_REGION", _DEFAULT_BEDROCK_REGION).strip() or _DEFAULT_BEDROCK_REGION
    model_id = os.environ.get("DEEPSEEK_MODEL_ID", _DEFAULT_DEEPSEEK_MODEL).strip() or _DEFAULT_DEEPSEEK_MODEL

    url = f"https://bedrock-runtime.{region}.amazonaws.com/model/{model_id}/converse"
    payload: Dict[str, Any] = {
        "messages": [
            {"role": "user", "content": [{"text": prompt}]},
        ],
        "inferenceConfig": {
            "maxTokens": max_tokens,
            "temperature": temperature,
        },
    }
    if system:
        payload["system"] = [{"text": system}]

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()

    try:
        blocks = data["output"]["message"]["content"]
    except (KeyError, TypeError) as exc:
        raise RuntimeError(f"unexpected Bedrock response shape: {exc}") from exc
    text = "".join(b.get("text", "") for b in blocks)

    cleaned = re.sub(r"```(?:json)?\s*", "", text).replace("```", "").strip()
    match = re.search(r"(\{.*\}|\[.*\])", cleaned, re.DOTALL)
    if match:
        try:
            parsed = json.loads(match.group(1))
            if isinstance(parsed, dict):
                return parsed
            return {"items": parsed, "text": text}
        except json.JSONDecodeError:
            pass
    return {"text": text}


def call_with_validation(
    call_fn: Callable[[str], Dict[str, Any]],
    prompt: str,
    validator_fn: Callable[[Dict[str, Any]], Tuple[bool, List[str]]],
    max_retries: int = 2,
) -> Tuple[Dict[str, Any], List[str]]:
    """Invoke ``call_fn(prompt)`` and validate; retry with error context on failure.

    ``validator_fn`` receives the parsed JSON dict and returns
    ``(is_valid, errors)``. On the final attempt the last result is
    returned even if validation still fails, so callers can flag for
    review rather than crash.
    """
    last_result: Dict[str, Any] = {}
    last_errors: List[str] = []
    current_prompt = prompt

    for attempt in range(max_retries + 1):
        try:
            result = call_fn(current_prompt)
            is_valid, errors = validator_fn(result)
            last_result, last_errors = result, errors

            if is_valid:
                return result, []

            if attempt < max_retries:
                error_context = '\n'.join(errors)
                current_prompt = (
                    f"{prompt}\n\nPREVIOUS ATTEMPT HAD ERRORS - FIX THESE:\n"
                    f"{error_context}\n\nReturn corrected JSON only."
                )
                logging.info('AI retry %d due to: %s', attempt + 1, errors)
            else:
                logging.warning(
                    'AI validation failed after %d retries: %s', max_retries, errors
                )
                return result, errors
        except Exception as e:
            logging.warning('AI attempt %d failed: %s', attempt + 1, e)
            if attempt == max_retries:
                raise

    return last_result, last_errors


# Backwards-compatible alias matching original spec naming.
call_deepseek_with_validation = call_with_validation


def get_vendor_gl_history(
    conn: sqlite3.Connection, vendor_name: str
) -> Optional[str]:
    """Return the most-frequent historical GL account for *vendor_name*.

    Used to detect AI inconsistencies: a vendor that has always been
    coded to 5410 but suddenly comes back as 5750 is suspicious.
    """
    if not vendor_name:
        return None
    try:
        row = conn.execute(
            """
            SELECT gl_account, COUNT(*) as cnt
            FROM documents
            WHERE LOWER(vendor) LIKE LOWER(?)
              AND gl_account IS NOT NULL
            GROUP BY gl_account
            ORDER BY cnt DESC
            LIMIT 1
            """,
            (f'%{vendor_name[:10]}%',),
        ).fetchone()
        return row[0] if row else None
    except sqlite3.Error:
        return None
