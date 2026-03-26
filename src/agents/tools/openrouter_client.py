from __future__ import annotations
import os
import json
import requests

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

class OpenRouterClient:
    def __init__(self, api_key: str | None = None, model: str | None = None):
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY")
        if not self.api_key:
            raise RuntimeError("Missing OPENROUTER_API_KEY env var")

        # Good default for cost/perf (change later if you want)
        self.model = model or os.environ.get("OPENROUTER_MODEL", "deepseek/deepseek-chat")

    def chat_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "temperature": temperature,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "response_format": {"type": "json_object"},
        }

        r = requests.post(OPENROUTER_URL, headers=headers, data=json.dumps(payload), timeout=90)
        if r.status_code != 200:
            raise RuntimeError(f"OpenRouter error {r.status_code}: {r.text}")

        data = r.json()
        content = data["choices"][0]["message"]["content"]
        try:
            return json.loads(content)
        except Exception:
            # If model returns invalid JSON, fail hard (don’t silently continue with garbage)
            raise RuntimeError(f"Model returned non-JSON: {content[:500]}")