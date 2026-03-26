from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from graph_list import GraphList


@dataclass
class ClientRegistryEntry:
    client_code: str
    client_name: str
    assigned_to: str
    team: str
    status: str
    storage_path: str
    notes: str


class ClientRegistry:
    def __init__(self, graph_list: GraphList, site_id: str, list_name: str = "LedgerLink Clients"):
        self.graph_list = graph_list
        self.site_id = site_id
        self.list_name = list_name
        self.list_id: Optional[str] = None
        self.clients: Dict[str, ClientRegistryEntry] = {}

    def load(self) -> None:
        ref = self.graph_list.get_list_by_name(self.site_id, self.list_name)
        self.list_id = ref.list_id

        items = self.graph_list.list_items(
            site_id=self.site_id,
            list_id=self.list_id,
            top=1000,
        )

        out: Dict[str, ClientRegistryEntry] = {}

        for item in items:
            fields = item.get("fields") or {}

            client_code = str(fields.get("ClientCode") or "").strip()
            if not client_code:
                continue

            out[client_code] = ClientRegistryEntry(
                client_code=client_code,
                client_name=str(fields.get("ClientName") or "").strip(),
                assigned_to=str(fields.get("AssignedTo") or "").strip(),
                team=str(fields.get("Team") or "").strip(),
                status=str(fields.get("Status") or "").strip(),
                storage_path=str(fields.get("StoragePath") or "").strip(),
                notes=str(fields.get("Notes") or "").strip(),
            )

        self.clients = out

    def get(self, client_code: Optional[str]) -> Optional[ClientRegistryEntry]:
        if not client_code:
            return None
        return self.clients.get(client_code)

    def has_client(self, client_code: Optional[str]) -> bool:
        if not client_code:
            return False
        return client_code in self.clients