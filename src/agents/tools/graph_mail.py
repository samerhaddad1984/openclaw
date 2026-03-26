from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests

GRAPH = "https://graph.microsoft.com/v1.0"


@dataclass
class AttachmentFile:
    name: str
    content_bytes: bytes
    content_type: str = "application/octet-stream"


class GraphMail:
    def __init__(self, access_token: str):
        self.access_token = access_token

    def _h(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.access_token}"}

    def get_folder_id_by_name(self, mailbox: str, folder_name: str) -> str:
        # search under root folders (top 200)
        url = f"{GRAPH}/users/{mailbox}/mailFolders/msgfolderroot/childFolders?$top=200&$select=id,displayName"
        r = requests.get(url, headers=self._h(), timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"list folders failed {r.status_code}: {r.text}")

        for f in r.json().get("value", []):
            if (f.get("displayName") or "").lower() == folder_name.lower():
                return f["id"]

        raise RuntimeError(f"Folder not found: {folder_name}")

    def list_messages(
        self,
        mailbox: str,
        folder_name: str = "Inbox",
        top: int = 10,
        unread_only: bool = True,
    ) -> List[Dict]:
        folder_id = self.get_folder_id_by_name(mailbox, folder_name)

        base = f"{GRAPH}/users/{mailbox}/mailFolders/{folder_id}/messages"
        qs = [f"$top={top}", "$select=id,subject,from,receivedDateTime,hasAttachments,isRead", "$orderby=receivedDateTime desc"]
        if unread_only:
            qs.append("$filter=isRead eq false")

        url = base + "?" + "&".join(qs)

        r = requests.get(url, headers=self._h(), timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"list_messages failed {r.status_code}: {r.text}")
        return r.json().get("value", [])

    def list_attachments(self, mailbox: str, message_id: str) -> List[Dict]:
        url = f"{GRAPH}/users/{mailbox}/messages/{message_id}/attachments?$top=50"
        r = requests.get(url, headers=self._h(), timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"list_attachments failed {r.status_code}: {r.text}")
        return r.json().get("value", [])

    def debug_attachment_types(self, mailbox: str, message_id: str) -> List[Dict]:
        atts = self.list_attachments(mailbox, message_id)
        out: List[Dict] = []
        for a in atts:
            out.append(
                {
                    "odata_type": a.get("@odata.type"),
                    "name": a.get("name"),
                    "contentType": a.get("contentType"),
                }
            )
        return out

    def download_file_attachments(self, mailbox: str, message_id: str) -> List[AttachmentFile]:
        atts = self.list_attachments(mailbox, message_id)
        out: List[AttachmentFile] = []

        for a in atts:
            if a.get("@odata.type") != "#microsoft.graph.fileAttachment":
                continue

            name = a.get("name") or "attachment"
            ctype = a.get("contentType") or "application/octet-stream"
            content_b64 = a.get("contentBytes")
            if not content_b64:
                continue

            out.append(
                AttachmentFile(
                    name=name,
                    content_bytes=base64.b64decode(content_b64),
                    content_type=ctype,
                )
            )

        return out

    def mark_read(self, mailbox: str, message_id: str) -> None:
        url = f"{GRAPH}/users/{mailbox}/messages/{message_id}"
        r = requests.patch(
            url,
            headers={**self._h(), "Content-Type": "application/json"},
            json={"isRead": True},
            timeout=30,
        )
        if r.status_code not in (200, 204):
            raise RuntimeError(f"mark_read failed {r.status_code}: {r.text}")

    def move_message(self, mailbox: str, message_id: str, dest_folder_id: str) -> None:
        url = f"{GRAPH}/users/{mailbox}/messages/{message_id}/move"
        body = {"destinationId": dest_folder_id}
        r = requests.post(
            url,
            headers={**self._h(), "Content-Type": "application/json"},
            json=body,
            timeout=30,
        )
        if r.status_code not in (200, 201):
            raise RuntimeError(f"move_message failed {r.status_code}: {r.text}")

    def get_or_create_folder(self, mailbox: str, folder_name: str) -> str:
        url = f"{GRAPH}/users/{mailbox}/mailFolders/msgfolderroot/childFolders?$top=200&$select=id,displayName"
        r = requests.get(url, headers=self._h(), timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"list folders failed {r.status_code}: {r.text}")

        folders = r.json().get("value", [])
        for f in folders:
            if (f.get("displayName") or "").lower() == folder_name.lower():
                return f["id"]

        url2 = f"{GRAPH}/users/{mailbox}/mailFolders/msgfolderroot/childFolders"
        r2 = requests.post(
            url2,
            headers={**self._h(), "Content-Type": "application/json"},
            json={"displayName": folder_name},
            timeout=30,
        )
        if r2.status_code not in (200, 201):
            raise RuntimeError(f"create folder failed {r2.status_code}: {r2.text}")

        return r2.json()["id"]