#!/usr/bin/env python3
"""
scripts/provision_client.py -- Provision a new OtoCPA client.

Generates a license key, builds the installer ZIP, optionally sends email,
and logs the client to clients.csv.

Usage:
    python scripts/provision_client.py \
        --firm "Tremblay CPA Inc" \
        --tier professionnel \
        --months 12 \
        --email sam@tremblaycpa.com \
        --contact "Sam Tremblay"
"""
from __future__ import annotations

import argparse
import csv
import os
import smtplib
import sys
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.engines.license_engine import (
    TIER_DEFAULTS,
    generate_license_key,
    get_signing_secret,
)
from scripts.build_installer import build as build_zip


CLIENTS_CSV = ROOT / "clients.csv"
CSV_HEADERS = [
    "firm_name", "tier", "email", "contact", "license_key",
    "expiry_date", "date_provisioned",
]


def _compute_expiry(months: int) -> str:
    import calendar
    today = date.today()
    month = today.month - 1 + months
    year = today.year + month // 12
    month = month % 12 + 1
    max_day = calendar.monthrange(year, month)[1]
    expiry = today.replace(year=year, month=month, day=min(today.day, max_day))
    return expiry.strftime("%Y-%m-%d")


def _log_client(firm: str, tier: str, email: str, contact: str,
                license_key: str, expiry: str) -> None:
    write_header = not CLIENTS_CSV.exists()
    with open(CLIENTS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(CSV_HEADERS)
        writer.writerow([
            firm, tier, email, contact, license_key,
            expiry, date.today().strftime("%Y-%m-%d"),
        ])


def _send_email(
    email_to: str,
    contact: str,
    firm: str,
    tier: str,
    license_key: str,
    expiry: str,
    zip_path: Path,
) -> bool:
    """Send installation email to client. Returns True on success."""
    # Read SMTP config from otocpa.config.json
    import json
    config_path = ROOT / "otocpa.config.json"
    try:
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        cfg = {}

    digest = cfg.get("digest", {})
    smtp_host = digest.get("smtp_host", "smtp.gmail.com")
    smtp_port = digest.get("smtp_port", 587)
    smtp_user = digest.get("smtp_user", "")
    smtp_pass = digest.get("smtp_password", "")
    from_addr = digest.get("from_address", smtp_user)
    from_name = digest.get("from_name", "OtoCPA")

    if not smtp_user or not smtp_pass or smtp_pass == "your-app-password":
        print("  WARNING: SMTP not configured. Email not sent.")
        print("  Configure digest.smtp_* in otocpa.config.json to enable email.")
        return False

    msg = MIMEMultipart()
    msg["From"] = f"{from_name} <{from_addr}>"
    msg["To"] = email_to
    msg["Subject"] = f"OtoCPA - Votre installation / Your installation"

    body = f"""Bonjour {contact},

Votre licence OtoCPA est prete!
Your OtoCPA license is ready!

=== INFORMATIONS / DETAILS ===
Cabinet / Firm: {firm}
Forfait / Tier: {tier}
Expiration: {expiry}

=== CLE DE LICENCE / LICENSE KEY ===
{license_key}

=== INSTRUCTIONS D'INSTALLATION / INSTALLATION STEPS ===

WINDOWS:
1. Extrayez le fichier ZIP sur votre bureau / Extract the ZIP to your desktop
2. Ouvrez le dossier OtoCPA / Open the OtoCPA folder
3. Double-cliquez INSTALL.bat / Double-click INSTALL.bat
4. Cliquez Oui si Windows demande la permission / Click Yes if Windows asks
5. Attendez 5 minutes / Wait 5 minutes

MAC:
1. Extrayez le fichier ZIP sur votre bureau / Extract the ZIP to your desktop
2. Ouvrez Terminal / Open Terminal
3. Tapez / Type: cd ~/Desktop/OtoCPA && bash INSTALL_MAC.sh
4. Attendez 5 minutes / Wait 5 minutes

Support: support@otocpa.com

Merci de votre confiance!
Thank you for your trust!

-- OtoCPA
"""
    msg.attach(MIMEText(body, "plain", "utf-8"))

    # Attach ZIP if under 25 MB
    zip_size_mb = zip_path.stat().st_size / (1024 * 1024)
    if zip_size_mb < 25:
        with open(zip_path, "rb") as f:
            part = MIMEBase("application", "zip")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={zip_path.name}",
            )
            msg.attach(part)
    else:
        msg.attach(MIMEText(
            f"\n[ZIP trop volumineux pour courriel ({zip_size_mb:.1f} MB). "
            f"Veuillez le transmettre via un lien de telechargement.]\n"
            f"[ZIP too large for email ({zip_size_mb:.1f} MB). "
            f"Please share via download link.]\n",
            "plain", "utf-8",
        ))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        return True
    except Exception as exc:
        print(f"  ERROR sending email: {exc}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Provision a new OtoCPA client",
    )
    parser.add_argument("--firm", required=True, help="Client firm name")
    parser.add_argument(
        "--tier", required=True, choices=list(TIER_DEFAULTS.keys()),
        help="License tier",
    )
    parser.add_argument(
        "--months", type=int, default=12,
        help="License duration in months (default: 12)",
    )
    parser.add_argument("--email", required=True, help="Client email address")
    parser.add_argument("--contact", required=True, help="Contact person name")
    parser.add_argument(
        "--no-email", action="store_true",
        help="Skip sending email",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  OtoCPA - Client Provisioning")
    print("=" * 60)

    # Step 1: Read signing secret
    print("\n[1/5] Reading signing secret...")
    secret = get_signing_secret()
    if not secret:
        print("ERROR: OTOCPA_SIGNING_SECRET not found in .env")
        print("Create .env with: OTOCPA_SIGNING_SECRET=your-secret-here")
        sys.exit(1)
    print("  OK")

    # Step 2: Generate license key
    print("\n[2/5] Generating license key...")
    issued_str = date.today().strftime("%Y-%m-%d")
    expiry_str = _compute_expiry(args.months)

    license_key = generate_license_key(
        tier=args.tier,
        firm_name=args.firm,
        expiry_date=expiry_str,
        issued_at=issued_str,
        secret=secret,
    )
    print(f"  Firm:    {args.firm}")
    print(f"  Tier:    {args.tier}")
    print(f"  Expiry:  {expiry_str}")
    print(f"  Key:     {license_key[:30]}...")

    # Step 3: Build installer ZIP
    print("\n[3/5] Building installer ZIP...")
    zip_path = build_zip()
    zip_size_mb = zip_path.stat().st_size / (1024 * 1024)
    print(f"  ZIP: {zip_path.name} ({zip_size_mb:.1f} MB)")

    # Step 4: Send email
    if not args.no_email:
        print(f"\n[4/5] Sending email to {args.email}...")
        sent = _send_email(
            args.email, args.contact, args.firm, args.tier,
            license_key, expiry_str, zip_path,
        )
        if sent:
            print("  Email sent!")
        else:
            print("  Email skipped (see warning above)")
    else:
        print("\n[4/5] Email skipped (--no-email)")

    # Step 5: Log to clients.csv
    print("\n[5/5] Logging to clients.csv...")
    _log_client(args.firm, args.tier, args.email, args.contact,
                license_key, expiry_str)
    print("  OK")

    # Summary
    print("\n" + "=" * 60)
    print("  PROVISIONING COMPLETE")
    print("=" * 60)
    print(f"  Firm:        {args.firm}")
    print(f"  Tier:        {args.tier}")
    print(f"  Contact:     {args.contact}")
    print(f"  Email:       {args.email}")
    print(f"  Expiry:      {expiry_str}")
    print(f"  License Key: {license_key}")
    print(f"  ZIP:         {zip_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
