from __future__ import annotations

import json
from pathlib import Path

import msal


class GraphAuth:

    def __init__(self, client_id: str, scopes: list[str]):
        self.client_id = client_id
        self.scopes = scopes
        self.authority = "https://login.microsoftonline.com/common"

    def acquire_token(self, token_cache_file: Path) -> str:

        cache = msal.SerializableTokenCache()

        if token_cache_file.exists():
            cache.deserialize(token_cache_file.read_text())

        app = msal.PublicClientApplication(
            self.client_id,
            authority=self.authority,
            token_cache=cache,
        )

        accounts = app.get_accounts()

        result = None

        if accounts:
            result = app.acquire_token_silent(self.scopes, account=accounts[0])

        if not result:
            flow = app.initiate_device_flow(scopes=self.scopes)

            if "user_code" not in flow:
                raise RuntimeError("Failed to create device flow")

            print()
            print("=================================================")
            print("LOGIN REQUIRED")
            print()
            print("Open this URL in your browser:")
            print(flow["verification_uri"])
            print()
            print("Enter this code:")
            print(flow["user_code"])
            print("=================================================")
            print()

            result = app.acquire_token_by_device_flow(flow)

        if "access_token" not in result:
            raise RuntimeError(f"Token acquisition failed: {result}")

        token_cache_file.write_text(cache.serialize())

        return result["access_token"]