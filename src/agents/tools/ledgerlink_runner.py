from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from graph_auth import GraphAuth
from graph_mail import GraphMail
from graph_sharepoint import GraphSharePoint

CLIENT_ID = "11da5dd7-6b6f-4367-9815-562805ae9b40"

SCOPES = [
    "User.Read",
    "Mail.ReadWrite",
    "Mail.Send",
    "Sites.ReadWrite.All",
]

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

CONFIG_FILE = DATA_DIR / "config.json"
TOKENS_FILE = DATA_DIR / "tokens.json"


def run_agent_once() -> dict:
    if not CONFIG_FILE.exists():
        raise RuntimeError(f"config.json not found at: {CONFIG_FILE}")

    cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))

    mailbox = cfg["mailbox"]
    site_url = cfg["sharepoint_site_url"]
    inbox_folder = cfg.get("folders_inbox", "/AI/Inbox")

    # NEW:
    mail_source_folder = cfg.get("mail_source_folder", "ledger")
    include_read = bool(cfg.get("include_read", False))  # default False = safer

    auth = GraphAuth(client_id=CLIENT_ID, scopes=SCOPES)
    token = auth.acquire_token(TOKENS_FILE)

    mail = GraphMail(token)
    sp = GraphSharePoint(token)

    site_id = sp.resolve_site_id(site_url)
    drive_id = sp.get_default_drive_id(site_id)

    sp.ensure_folder_path(drive_id, inbox_folder)

    processed_folder_id = mail.get_or_create_folder(mailbox, "Processed")

    messages = mail.list_messages(
        mailbox=mailbox,
        folder_name=mail_source_folder,
        top=50,
        unread_only=(not include_read),
    )

    processed = 0
    uploaded = 0
    skipped_non_pdf_attachments = 0
    errors = []

    for m in messages:
        mid = m["id"]
        subject = m.get("subject", "")
        has_attachments = bool(m.get("hasAttachments"))

        if not has_attachments:
            # move out of the way
            try:
                mail.mark_read(mailbox, mid)
                mail.move_message(mailbox, mid, processed_folder_id)
                processed += 1
            except Exception as e:
                errors.append(f"{mid} move/read failed: {e}")
            continue

        try:
            atts = mail.download_file_attachments(mailbox, mid)

            if not atts:
                types = mail.debug_attachment_types(mailbox, mid)
                errors.append(f"{mid} had no file attachments. attachment_types={types}")
                continue

            for a in atts:
                if not (a.name or "").lower().endswith(".pdf"):
                    skipped_non_pdf_attachments += 1
                    continue
                filename = f"{mid[:8]}_{a.name}"
                sp.upload_bytes(drive_id, inbox_folder, filename, a.content_bytes)
                uploaded += 1

            mail.mark_read(mailbox, mid)
            mail.move_message(mailbox, mid, processed_folder_id)
            processed += 1

        except Exception as e:
            errors.append(f"{mid} ({subject}) failed: {e}")

    return {
        "run_at": datetime.utcnow().isoformat() + "Z",
        "mailbox": mailbox,
        "site_url": site_url,
        "mail_source_folder": mail_source_folder,
        "include_read": include_read,
        "sharepoint_inbox": inbox_folder,
        "emails_checked": len(messages),
        "emails_processed": processed,
        "attachments_uploaded": uploaded,
        "skipped_non_pdf_attachments": skipped_non_pdf_attachments,
        "errors": errors,
    }


if __name__ == "__main__":
    print(json.dumps(run_agent_once(), indent=2))