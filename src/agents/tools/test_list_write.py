# test_list_write.py
from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime

from graph_auth import GraphAuth
from graph_sharepoint import GraphSharePoint
from graph_list import GraphList

CLIENT_ID = "11da5dd7-6b6f-4367-9815-562805ae9b40"
SCOPES = ["User.Read", "Sites.ReadWrite.All"]

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
CONFIG_FILE = DATA_DIR / "config.json"
TOKENS_FILE = DATA_DIR / "tokens.json"

LIST_NAME = "OtoCPA Queue"


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def main() -> None:
    cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    site_url = cfg["sharepoint_site_url"]

    auth = GraphAuth(client_id=CLIENT_ID, scopes=SCOPES)
    token = auth.acquire_token(TOKENS_FILE)

    sp = GraphSharePoint(token)
    gl = GraphList(token)

    site_id = sp.resolve_site_id(site_url)
    lst = gl.get_list_by_name(site_id, LIST_NAME)

    fields = {
        "Title": f"TEST {now_iso()}",
        "Status": "For Review",
        "FileName": "test.pdf",
        "SharePointItemId": "TEST_ITEM_ID",
        "Vendor": "Test Vendor",
        "Currency": "CAD",
        "Method": "rules",
        "Notes": "Created by test_list_write.py",
    }

    created = gl.create_item(site_id, lst.list_id, fields)
    print("✅ Created list item:")
    print(json.dumps(created, indent=2))


if __name__ == "__main__":
    main()