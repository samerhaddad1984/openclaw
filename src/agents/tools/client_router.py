from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class ClientRouteResult:
    client_code: Optional[str]
    client_name: Optional[str]
    assigned_to: Optional[str]
    score: int
    matched_signals: list[str]
    min_score_required: int


class ClientRouter:
    """
    Scored routing:
    - sender email exact match = +10
    - account number exact match = +10
    - address fragment match = +6
    - client name match = +5
    - generic keyword match = +2

    Returns no client if best score < min_score.
    """

    def __init__(self, rules_dir: Path):
        self.rules_dir = Path(rules_dir)
        self.default_min_score, self.client_map = self._load_client_map()

    def _load_client_map(self) -> tuple[int, list[dict]]:
        f = self.rules_dir / "client_map.json"
        if not f.exists():
            return 6, []

        data = json.loads(f.read_text(encoding="utf-8"))
        default_min = int(data.get("default_min_score", 6))
        clients = data.get("clients", [])
        if not isinstance(clients, list):
            clients = []
        return default_min, clients

    def _normalize_text(self, s: str) -> str:
        s = (s or "").lower()
        s = unicodedata.normalize("NFKD", s)
        s = "".join(c for c in s if not unicodedata.combining(c))
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _digits_only(self, s: str) -> str:
        return re.sub(r"\D+", "", s or "")

    def route(self, text: str, sender_email: Optional[str] = None) -> ClientRouteResult:
        norm_text = self._normalize_text(text)
        digits_text = self._digits_only(text)
        sender_email = (sender_email or "").strip().lower()

        best_client = None
        best_score = 0
        best_matches: list[str] = []
        best_min_score = self.default_min_score

        for client in self.client_map:
            score = 0
            matches: list[str] = []

            min_score = int(client.get("min_score", self.default_min_score))

            # 1) sender email exact match
            for email in client.get("sender_emails", []) or []:
                e = (email or "").strip().lower()
                if e and sender_email and e == sender_email:
                    score += 10
                    matches.append(f"sender_email:{email}")

            # 2) account numbers exact digit match
            for acct in client.get("account_numbers", []) or []:
                acct_digits = self._digits_only(acct)
                if acct_digits and acct_digits in digits_text:
                    score += 10
                    matches.append(f"account_number:{acct}")

            # 3) address fragments
            for addr in client.get("addresses", []) or []:
                norm_addr = self._normalize_text(addr)
                if norm_addr and norm_addr in norm_text:
                    score += 6
                    matches.append(f"address:{addr}")

            # 4) client names
            for name in client.get("client_names", []) or []:
                norm_name = self._normalize_text(name)
                if norm_name and norm_name in norm_text:
                    score += 5
                    matches.append(f"client_name:{name}")

            # 5) generic keywords
            for kw in client.get("pdf_keywords", []) or []:
                norm_kw = self._normalize_text(kw)
                if norm_kw and norm_kw in norm_text:
                    score += 2
                    matches.append(f"keyword:{kw}")

            if score > best_score:
                best_score = score
                best_client = client
                best_matches = matches
                best_min_score = min_score

        if not best_client or best_score < best_min_score:
            return ClientRouteResult(
                client_code=None,
                client_name=None,
                assigned_to=None,
                score=best_score,
                matched_signals=best_matches,
                min_score_required=best_min_score,
            )

        return ClientRouteResult(
            client_code=best_client.get("client_code"),
            client_name=best_client.get("client_name"),
            assigned_to=best_client.get("assigned_to"),
            score=best_score,
            matched_signals=best_matches,
            min_score_required=best_min_score,
        )