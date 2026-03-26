from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import requests

GRAPH = "https://graph.microsoft.com/v1.0"


@dataclass
class ListRef:
    list_id: str
    display_name: str


class GraphList:
    def __init__(self, access_token: str):
        self.access_token = access_token

    def _h(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.access_token}"}

    def get_list_by_name(self, site_id: str, display_name: str) -> ListRef:
        url = f"{GRAPH}/sites/{site_id}/lists?$select=id,displayName"
        r = requests.get(url, headers=self._h(), timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"lists lookup failed {r.status_code}: {r.text}")

        items = r.json().get("value", [])
        for it in items:
            if (it.get("displayName") or "").strip().lower() == display_name.strip().lower():
                return ListRef(
                    list_id=it["id"],
                    display_name=it.get("displayName") or display_name,
                )

        names = [x.get("displayName") for x in items]
        raise RuntimeError(f"List '{display_name}' not found on site. Found lists: {names}")

    def create_item(self, site_id: str, list_id: str, fields: Dict) -> Dict:
        url = f"{GRAPH}/sites/{site_id}/lists/{list_id}/items"
        body = {"fields": fields}
        r = requests.post(
            url,
            headers={**self._h(), "Content-Type": "application/json"},
            json=body,
            timeout=30,
        )
        if r.status_code not in (200, 201):
            raise RuntimeError(f"create_item failed {r.status_code}: {r.text}")
        return r.json()

    def update_item_fields(self, site_id: str, list_id: str, item_id: str, fields: Dict) -> None:
        url = f"{GRAPH}/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
        r = requests.patch(
            url,
            headers={**self._h(), "Content-Type": "application/json"},
            json=fields,
            timeout=30,
        )
        if r.status_code not in (200, 204):
            raise RuntimeError(f"update_item_fields failed {r.status_code}: {r.text}")

    def list_items(self, site_id: str, list_id: str, top: int = 200) -> List[Dict]:
        url = f"{GRAPH}/sites/{site_id}/lists/{list_id}/items?$top={top}&$expand=fields"
        r = requests.get(url, headers=self._h(), timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"list_items failed {r.status_code}: {r.text}")
        return r.json().get("value", [])

    def find_item_by_field_value(
        self,
        site_id: str,
        list_id: str,
        field_name: str,
        field_value: str,
        top: int = 1000,
    ) -> Optional[Dict]:
        items = self.list_items(site_id, list_id, top=top)
        target = (field_value or "").strip()

        for it in items:
            fields = it.get("fields") or {}
            current = str(fields.get(field_name) or "").strip()
            if current == target:
                return it

        return None