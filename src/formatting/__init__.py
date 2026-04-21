"""Locale-aware formatting helpers for OtoCPA.

Why this module exists
----------------------
Most of the codebase historically formatted dates with ``strftime("%Y-%m-%d")``
and currency with f-strings (``f"${x:,.2f}"``) — both are locale-insensitive
and produce English-looking output for French users. These helpers give a
single seam to produce correct Québec-French output (space-separated
thousands, comma decimal, trailing ``$``) alongside the existing English
output.

Keep the API tiny: pass the raw value plus the locale (``"fr"`` or ``"en"``)
and get back the formatted string. No implicit locale fallback to the system
— callers always know which locale they want.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Union


_FR_MONTHS = (
    "janvier", "février", "mars", "avril", "mai", "juin",
    "juillet", "août", "septembre", "octobre", "novembre", "décembre",
)
_EN_MONTHS = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)


def _coerce_date(value: Union[date, datetime, str]) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    # ISO string
    s = str(value)[:10]
    return datetime.strptime(s, "%Y-%m-%d").date()


def format_date(value: Union[date, datetime, str], lang: str = "fr") -> str:
    """Render a date in the conventional style for *lang*.

    - fr: ``21 avril 2026``
    - en: ``April 21, 2026``

    Accepts a ``date``, ``datetime``, or ISO-8601 ``YYYY-MM-DD`` string.
    """
    d = _coerce_date(value)
    if lang == "fr":
        return f"{d.day} {_FR_MONTHS[d.month - 1]} {d.year}"
    return f"{_EN_MONTHS[d.month - 1]} {d.day}, {d.year}"


def format_date_short(value: Union[date, datetime, str], lang: str = "fr") -> str:
    """Short numeric date.

    - fr: ``21/04/2026`` (Québec common form, DD/MM/YYYY)
    - en: ``2026-04-21`` (ISO; most Canadian English contexts use ISO in
      accounting UIs)
    """
    d = _coerce_date(value)
    if lang == "fr":
        return f"{d.day:02d}/{d.month:02d}/{d.year}"
    return d.strftime("%Y-%m-%d")


def format_currency(
    amount: Union[int, float, str],
    lang: str = "fr",
    *,
    decimals: int = 2,
    symbol: str = "$",
) -> str:
    """Render a currency amount.

    - fr: ``1 234,56 $`` (space thousands, comma decimal, trailing symbol,
      NBSP-like space before symbol). Returns a regular space to keep the
      output grep-friendly; callers that need NBSP can substitute.
    - en: ``$1,234.56`` (comma thousands, dot decimal, leading symbol)

    Negatives render with a leading minus in both languages.
    """
    try:
        f = float(amount)
    except (TypeError, ValueError):
        return str(amount)

    sign = "-" if f < 0 else ""
    f = abs(f)
    if lang == "fr":
        # 1234567.89 -> "1 234 567,89"
        integer, _, frac = f"{f:,.{decimals}f}".partition(".")
        integer = integer.replace(",", " ")
        body = integer if not frac else f"{integer},{frac}"
        return f"{sign}{body} {symbol}"
    return f"{sign}{symbol}{f:,.{decimals}f}"


def format_number(
    amount: Union[int, float, str],
    lang: str = "fr",
    *,
    decimals: int = 0,
) -> str:
    """Render a plain number with locale-conventional separators.

    - fr: ``1 234,56`` (space thousands, comma decimal)
    - en: ``1,234.56`` (comma thousands, dot decimal)
    """
    try:
        f = float(amount)
    except (TypeError, ValueError):
        return str(amount)
    if lang == "fr":
        integer, _, frac = f"{f:,.{decimals}f}".partition(".")
        integer = integer.replace(",", " ")
        return integer if not frac else f"{integer},{frac}"
    return f"{f:,.{decimals}f}"


# ---------------------------------------------------------------------------
# Short aliases — intended for inline wrapping at call sites that currently
# use ``f"${x:,.2f}"`` / ``f"{n:,}"`` patterns. They call the canonical
# helpers above; the short name is the only reason they exist.
# ---------------------------------------------------------------------------


def money(amount: Union[int, float, str], lang: str = "fr") -> str:
    """Short alias for :func:`format_currency` at default 2-decimal places."""
    return format_currency(amount, lang)


def money_signed(amount: Union[int, float, str], lang: str = "fr") -> str:
    """Currency with an explicit + sign for non-negative values.

    Matches the ``f"${x:+,.2f}"`` pattern used in reconciliation diffs.
    """
    try:
        f = float(amount)
    except (TypeError, ValueError):
        return str(amount)
    if f >= 0:
        return "+" + format_currency(f, lang)
    return format_currency(f, lang)


def num(value: Union[int, float, str], lang: str = "fr", *, decimals: int = 0) -> str:
    """Short alias for :func:`format_number`."""
    return format_number(value, lang, decimals=decimals)


def format_time(value: Union[datetime, str], lang: str = "fr") -> str:
    """Render a time-of-day.

    - fr: ``14h30`` (24-hour, ``h`` separator — Québec convention)
    - en: ``2:30 PM`` (12-hour)

    Accepts a ``datetime`` or a ``HH:MM`` string. If a date string is passed,
    returns it unchanged.
    """
    if isinstance(value, datetime):
        hh, mm = value.hour, value.minute
    else:
        s = str(value)
        if ":" not in s:
            return s
        parts = s.split(":")
        hh, mm = int(parts[0]), int(parts[1][:2])
    if lang == "fr":
        return f"{hh:02d}h{mm:02d}"
    ampm = "AM" if hh < 12 else "PM"
    disp = hh % 12 or 12
    return f"{disp}:{mm:02d} {ampm}"
