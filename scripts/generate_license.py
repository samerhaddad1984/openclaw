#!/usr/bin/env python3
"""
generate_license.py — OtoCPA license key generator.

Usage:
    python scripts/generate_license.py --tier professionnel --firm "Tremblay CPA" --months 12
    python scripts/generate_license.py --tier cabinet --firm "Audit Inc." --months 24 --secret MY_SECRET
    python scripts/generate_license.py --tier entreprise --firm "Big Firm" --months 36 \\
        --max-clients 200 --max-users 50
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.license_engine import (
    TIER_DEFAULTS,
    generate_license_key,
    get_signing_secret,
)


def _load_env_secret() -> str:
    """Read OTOCPA_SIGNING_SECRET from .env or environment."""
    env_file = ROOT / ".env"
    if env_file.exists():
        try:
            for line in env_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("OTOCPA_SIGNING_SECRET"):
                    _, _, value = line.partition("=")
                    return value.strip().strip('"').strip("'")
        except Exception:
            pass
    return os.environ.get("OTOCPA_SIGNING_SECRET", "")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a OtoCPA license key",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--tier",
        required=True,
        choices=list(TIER_DEFAULTS.keys()),
        help="License tier",
    )
    parser.add_argument(
        "--firm",
        required=True,
        help="Licensed firm name",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=12,
        help="License validity in months (default: 12)",
    )
    parser.add_argument(
        "--secret",
        default="",
        help="HMAC signing secret (overrides .env / environment variable)",
    )
    parser.add_argument(
        "--max-clients",
        type=int,
        default=None,
        help="Override max clients (defaults to tier default)",
    )
    parser.add_argument(
        "--max-users",
        type=int,
        default=None,
        help="Override max users (defaults to tier default)",
    )

    args = parser.parse_args()

    # Resolve signing secret
    secret = args.secret or _load_env_secret()
    if not secret:
        print(
            "ERROR: No signing secret provided.\n"
            "  Set OTOCPA_SIGNING_SECRET in .env or pass --secret SECRET",
            file=sys.stderr,
        )
        sys.exit(1)

    issued_at = date.today()
    # Add months by computing target month/year
    month = issued_at.month - 1 + args.months
    year = issued_at.year + month // 12
    month = month % 12 + 1
    # Clamp day to valid range for target month
    import calendar
    max_day = calendar.monthrange(year, month)[1]
    expiry = issued_at.replace(year=year, month=month, day=min(issued_at.day, max_day))

    issued_str = issued_at.strftime("%Y-%m-%d")
    expiry_str = expiry.strftime("%Y-%m-%d")

    tier_info = TIER_DEFAULTS[args.tier]
    max_clients = args.max_clients if args.max_clients is not None else tier_info["max_clients"]
    max_users = args.max_users if args.max_users is not None else tier_info["max_users"]

    key = generate_license_key(
        tier=args.tier,
        firm_name=args.firm,
        expiry_date=expiry_str,
        issued_at=issued_str,
        secret=secret,
        max_clients=max_clients,
        max_users=max_users,
    )

    divider = "─" * 72
    print()
    print(divider)
    print("  OtoCPA License Key")
    print(divider)
    print(f"  Firm        : {args.firm}")
    print(f"  Tier        : {args.tier}")
    print(f"  Issued      : {issued_str}")
    print(f"  Expires     : {expiry_str}  ({args.months} months)")
    print(f"  Max Clients : {max_clients}")
    print(f"  Max Users   : {max_users}")
    print(f"  Features    : {', '.join(tier_info['features'])}")
    print(divider)
    print()
    print(f"  License Key:")
    print(f"  {key}")
    print()
    print(divider)
    print()
    print("  To activate, paste the key into the License Management page")
    print("  in the OtoCPA review dashboard (/license), or run:")
    print()
    print("  python scripts/manage_clients.py  (if CLI activation is added)")
    print()


if __name__ == "__main__":
    main()
