"""Phone-number normalization for WhatsApp identity.

Every surface that touches a WhatsApp identity — the admin invite
form, the Twilio webhook, the CPA override page — funnels numbers
through :func:`normalize_phone` so we store exactly one canonical
format (E.164, ``+15141234567``) in ``client_portal_users``. A
``whatsapp:`` prefix or spaces / dashes / parens are accepted on
input; anything we can't turn into a valid NANP (+1, 10 digits) is
rejected.

Why keep this separate from ``src/integrations/whatsapp.py``
:func:`normalize_phone`:

* The old helper strips the leading ``1`` so it can compare against
  ``dashboard_users`` rows that stored bare 10-digit numbers. We
  need the opposite: always keep ``+1`` so the column is unambiguous
  across the multi-user portal.
* The old helper never raises — it just returns whatever digits are
  left. We need a validator that fails closed on foreign numbers,
  short codes, and typos.
"""
from __future__ import annotations

import re


_DIGIT_RE = re.compile(r"[^\d]")


def normalize_phone(raw: str | None) -> str | None:
    """Return the number in E.164 NANP form, or ``None`` if invalid.

    Accepted shapes:

    * ``+1 (514) 123-4567``
    * ``514-123-4567`` (10 digits, no country code → +1 assumed)
    * ``1-514-123-4567``
    * ``whatsapp:+15141234567``
    * ``+15141234567``

    Rejected:

    * Non-NANP country codes (e.g. ``+44``, ``+33``). Scope is Canada +
      US for now; extend once we have a reason.
    * Short codes and N11 numbers (anything shorter than 10 digits).
    * NANP numbers whose area code or central-office code starts with
      ``0`` or ``1`` (not assignable — these are structurally invalid).
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None

    # Strip the Twilio ``whatsapp:`` prefix so either raw sender
    # strings or form fields normalize the same way.
    if text.lower().startswith("whatsapp:"):
        text = text[len("whatsapp:"):]

    # Reject explicit non-NANP country codes up front. We only want
    # ``+1`` here; anything else is out of scope.
    stripped = text.lstrip()
    if stripped.startswith("+") and not stripped.startswith("+1"):
        return None

    digits = _DIGIT_RE.sub("", text)

    # 10-digit NANP number with no country code → +1 assumed.
    if len(digits) == 10:
        national = digits
    elif len(digits) == 11 and digits.startswith("1"):
        national = digits[1:]
    else:
        return None

    # NANP structural check: NPA (area code) and NXX (central office)
    # must start with 2-9. Anything else is unassignable per NANP plan.
    if national[0] in "01" or national[3] in "01":
        return None

    return "+1" + national


def format_display(e164: str | None) -> str:
    """Return a human-friendly format for admin UIs.

    ``+15141234567`` → ``+1 (514) 123-4567``. Pass-through when the
    input doesn't look like NANP E.164 so callers can display raw
    text (e.g. legacy ten-digit numbers) without a crash.
    """
    if not e164:
        return ""
    if not (e164.startswith("+1") and len(e164) == 12 and e164[2:].isdigit()):
        return e164
    d = e164[2:]
    return f"+1 ({d[0:3]}) {d[3:6]}-{d[6:10]}"
