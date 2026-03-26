"""
PROJECT CONTEXT: AI Client Router for CPA Workflow
STATUS: Refactoring stage - Improving AI response parsing and confidence scoring.
OBJECTIVE: Map document text to a client registry using OpenRouter (LLM).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Any, Dict, List

# Assuming this is your local/custom library
from openrouter_client import OpenRouterClient

@dataclass
class AIClientRouteResult:
    client_code: Optional[str]
    assigned_to: Optional[str]
    confidence: float
    reason: str

def _safe_confidence(value: Any) -> float:
    """
    Safely converts AI confidence outputs (strings, percentages, or labels) 
    into a float between 0.0 and 1.0.
    """
    if value is None:
        return 0.0

    if isinstance(value, (int, float)):
        return max(0.0, min(float(value), 1.0))

    s = str(value).strip().lower()
    if not s:
        return 0.0

    if s.endswith("%"):
        try:
            return max(0.0, min(float(s[:-1]) / 100.0, 1.0))
        except (ValueError, TypeError):
            return 0.0

    # Map text-based confidence to numerical values
    mappings = {
        "high": 0.85, "strong": 0.85, "very high": 0.95,
        "medium": 0.60, "moderate": 0.60,
        "low": 0.30, "weak": 0.30
    }
    
    if s in mappings:
        return mappings[s]

    try:
        return max(0.0, min(float(s), 1.0))
    except (ValueError, TypeError):
        return 0.0

class AIClientRouter:
    def __init__(self, rules_dir: Path | str):
        self.rules_dir = Path(rules_dir)
        self.client_map = self._load_client_map()
        self.ai = OpenRouterClient()

    def _load_client_map(self) -> Dict:
        """Loads the registry of clients from the local JSON file."""
        f = self.rules_dir / "client_map.json"
        if not f.exists():
            return {"clients": []}
        try:
            return json.loads(f.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"clients": []}

    def route(self, text: str) -> AIClientRouteResult:
        """
        Analyzes document text and routes it to the most likely client 
        based on the registry.
        """
        clients = self.client_map.get("clients", [])

        # Simplify registry to save tokens and prevent LLM confusion
        slim_clients = [
            {
                "client_code": c.get("client_code"),
                "client_name": c.get("client_name"),
                "assigned_to": c.get("assigned_to"),
                "known_names": c.get("client_names", []),
                "known_addresses": c.get("addresses", []),
                "known_accounts": c.get("account_numbers", []),
            }
            for c in clients
        ]

        prompt = (
            "Select the best client_code from the registry for this document.\n"
            "If evidence is weak, return null for client_code.\n"
            "Return STRICT JSON only.\n\n"
            "Keys: client_code, confidence (0-1), reason\n\n"
            f"CLIENT REGISTRY:\n{json.dumps(slim_clients, ensure_ascii=False)}\n\n"
            f"DOCUMENT TEXT:\n{text[:15000]}"
        )

        # Execute AI call
        result = self.ai.chat_json(
            system=(
                "You are a precise CPA routing assistant. "
                "Output JSON only. Be conservative with confidence scores."
            ),
            user=prompt,
            temperature=0.0,
        ) or {}

        client_code = result.get("client_code")
        confidence = _safe_confidence(result.get("confidence"))
        reason = str(result.get("reason", "") or "").strip()

        # Cross-reference the assigned staff member from the original map
        assigned_to = None
        if client_code:
            for c in clients:
                if c.get("client_code") == client_code:
                    assigned_to = c.get("assigned_to")
                    break

        return AIClientRouteResult(
            client_code=client_code,
            assigned_to=assigned_to,
            confidence=confidence,
            reason=reason,
        )
