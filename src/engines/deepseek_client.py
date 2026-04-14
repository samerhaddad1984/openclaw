"""Validation-aware AI call wrappers and vendor-consistency memory.

Despite the historical name, this module does not bind to any single
provider. ``call_with_validation`` retries any JSON-returning callable
when the validator rejects its output, appending the error context so
the model can self-correct on the next attempt.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Callable, Dict, List, Optional, Tuple


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
