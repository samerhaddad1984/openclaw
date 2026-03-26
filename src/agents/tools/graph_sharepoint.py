from __future__ import annotations

import time
from typing import Dict, Optional, List

import requests

GRAPH = "https://graph.microsoft.com/v1.0"


class GraphSharePoint:
    def __init__(self, access_token: str):
        self.access_token = access_token

    def _h(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.access_token}"}

    def resolve_site_id(self, site_url: str) -> str:
        # site_url like: https://tenant.sharepoint.com/sites/IT
        parts = site_url.replace("https://", "").split("/")
        hostname = parts[0]
        site_path = "/" + "/".join(parts[1:])
        url = f"{GRAPH}/sites/{hostname}:{site_path}"
        r = requests.get(url, headers=self._h(), timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"resolve_site_id failed {r.status_code}: {r.text}")
        return r.json()["id"]

    def get_default_drive_id(self, site_id: str) -> str:
        url = f"{GRAPH}/sites/{site_id}/drive"
        r = requests.get(url, headers=self._h(), timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"get_default_drive_id failed {r.status_code}: {r.text}")
        return r.json()["id"]

    def ensure_folder_path(self, drive_id: str, folder_path: str) -> None:
        folder_path = folder_path.strip("/")
        if not folder_path:
            return

        parts = folder_path.split("/")
        current = ""
        for p in parts:
            current = f"{current}/{p}" if current else p
            try:
                self.get_item_by_path(drive_id, f"/{current}")
            except Exception:
                # create folder at this level
                parent = "/".join(current.split("/")[:-1])
                name = current.split("/")[-1]
                if parent:
                    parent_item = self.get_item_by_path(drive_id, f"/{parent}")
                    parent_id = parent_item["id"]
                    url = f"{GRAPH}/drives/{drive_id}/items/{parent_id}/children"
                else:
                    url = f"{GRAPH}/drives/{drive_id}/root/children"

                body = {
                    "name": name,
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "rename",
                }
                r = requests.post(url, headers={**self._h(), "Content-Type": "application/json"}, json=body, timeout=30)
                if r.status_code not in (200, 201):
                    raise RuntimeError(f"ensure_folder_path create failed {r.status_code}: {r.text}")

    def get_item_by_path(self, drive_id: str, path: str) -> dict:
        # path must start with "/"
        path = path.strip()
        if not path.startswith("/"):
            path = "/" + path
        url = f"{GRAPH}/drives/{drive_id}/root:{path}"
        r = requests.get(url, headers=self._h(), timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"get_item_by_path failed {r.status_code}: {r.text}")
        return r.json()

    def upload_bytes(self, drive_id: str, folder_path: str, filename: str, content: bytes, content_type: str = "application/octet-stream") -> dict:
        folder_path = folder_path.strip("/")
        if folder_path:
            url = f"{GRAPH}/drives/{drive_id}/root:/{folder_path}/{filename}:/content"
        else:
            url = f"{GRAPH}/drives/{drive_id}/root:/{filename}:/content"

        r = requests.put(url, headers={**self._h(), "Content-Type": content_type}, data=content, timeout=60)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"upload_bytes failed {r.status_code}: {r.text}")
        return r.json()

    def list_folder_children(self, drive_id: str, folder_path: str, top: int = 200) -> List[dict]:
        folder_path = folder_path.strip("/")
        url = f"{GRAPH}/drives/{drive_id}/root:/{folder_path}:/children?$top={top}"
        r = requests.get(url, headers=self._h(), timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"list_folder_children failed {r.status_code}: {r.text}")
        return r.json().get("value", [])

    def download_item_bytes(self, drive_id: str, item_id: str) -> bytes:
        url = f"{GRAPH}/drives/{drive_id}/items/{item_id}/content"
        r = requests.get(url, headers=self._h(), timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"download_item_bytes failed {r.status_code}: {r.text}")
        return r.content

    def move_item(self, drive_id: str, item_id: str, dest_folder_path: str, new_name: Optional[str] = None) -> dict:
        """
        Move an item into dest folder path (e.g. /AI/Completed).
        If name exists, auto-rename by appending timestamp.
        """
        dest_folder_path = dest_folder_path.strip().strip("/")
        if not dest_folder_path:
            raise ValueError("dest_folder_path cannot be empty")

        # Resolve destination folder item id
        url_get = f"{GRAPH}/drives/{drive_id}/root:/{dest_folder_path}"
        rget = requests.get(url_get, headers=self._h(), timeout=30)
        if rget.status_code != 200:
            raise RuntimeError(f"resolve dest folder failed {rget.status_code}: {rget.text}")
        dest_id = rget.json()["id"]

        url_patch = f"{GRAPH}/drives/{drive_id}/items/{item_id}"

        def _try_move(name_to_use: Optional[str]) -> requests.Response:
            body = {"parentReference": {"id": dest_id}}
            if name_to_use:
                body["name"] = name_to_use
            return requests.patch(
                url_patch,
                headers={**self._h(), "Content-Type": "application/json"},
                json=body,
                timeout=30,
            )

        r = _try_move(new_name)
        if r.status_code in (200, 201):
            return r.json()

        if r.status_code == 409 and "nameAlreadyExists" in (r.text or ""):
            # fetch current name if needed
            if not new_name:
                url_item = f"{GRAPH}/drives/{drive_id}/items/{item_id}?$select=name"
                ritem = requests.get(url_item, headers=self._h(), timeout=30)
                if ritem.status_code != 200:
                    raise RuntimeError(f"move_item collision; read name failed {ritem.status_code}: {ritem.text}")
                original = ritem.json().get("name") or "file"
            else:
                original = new_name

            if "." in original:
                base, ext = original.rsplit(".", 1)
                ext = "." + ext
            else:
                base, ext = original, ""

            stamp = time.strftime("%Y%m%d-%H%M%S")
            new_name2 = f"{base}__{stamp}{ext}"

            r2 = _try_move(new_name2)
            if r2.status_code in (200, 201):
                return r2.json()
            raise RuntimeError(f"move_item failed after rename {r2.status_code}: {r2.text}")

        raise RuntimeError(f"move_item failed {r.status_code}: {r.text}")