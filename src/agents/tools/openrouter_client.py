from __future__ import annotations
import os
import json
import re
import requests

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Extract the first JSON object from a model response — handles ```json fences
# and leading/trailing prose, both of which Anthropic models sometimes emit
# even when explicitly asked for JSON only.
_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)


def _extract_json(content: str) -> dict:
    text = content.strip()
    # Strip markdown code fences (```json ... ``` or ``` ... ```).
    if text.startswith("```"):
        # remove leading fence (possibly with language) and trailing fence
        text = re.sub(r"^```[a-zA-Z0-9]*\s*", "", text)
        text = re.sub(r"\s*```\s*$", "", text)
    try:
        return json.loads(text)
    except Exception:
        m = _JSON_OBJECT_RE.search(text)
        if m:
            return json.loads(m.group(0))
        raise


class OpenRouterClient:
    """Thin AI client with Anthropic fallback.

    History: the original client only spoke OpenRouter's chat-completions API.
    In this deployment the OPENROUTER_API_KEY env var actually holds an
    Anthropic key (sk-ant-...), so every OpenRouter call was 401'ing and
    downstream features like line-item extraction silently returned 0 rows.
    When we detect an Anthropic-shaped key we call the Anthropic Messages API
    directly so the same client keeps working for every call site.
    """

    def __init__(self, api_key: str | None = None, model: str | None = None):
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY")
        # Fall back to ANTHROPIC_API_KEY so deployments that only set one
        # key still work.
        if not self.api_key:
            self.api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise RuntimeError(
                "Missing OPENROUTER_API_KEY / ANTHROPIC_API_KEY env var"
            )

        self._use_anthropic = self.api_key.startswith("sk-ant-")

        if self._use_anthropic:
            # Default to Haiku for cost/latency parity with the previous
            # deepseek default; override via OPENROUTER_MODEL.
            self.model = model or os.environ.get(
                "OPENROUTER_MODEL", "claude-haiku-4-5-20251001"
            )
        else:
            self.model = model or os.environ.get(
                "OPENROUTER_MODEL", "deepseek/deepseek-chat"
            )

    def chat_json(self, system: str, user: str, temperature: float = 0.0) -> dict:
        if self._use_anthropic:
            return self._chat_json_anthropic(system, user, temperature)
        return self._chat_json_openrouter(system, user, temperature)

    def _chat_json_openrouter(
        self, system: str, user: str, temperature: float
    ) -> dict:
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
            return _extract_json(content)
        except Exception:
            raise RuntimeError(f"Model returned non-JSON: {content[:500]}")

    def _chat_json_anthropic(
        self, system: str, user: str, temperature: float
    ) -> dict:
        # Lazy import so environments without the SDK still import this module.
        try:
            import anthropic  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "anthropic package not installed; cannot use Anthropic key"
            ) from exc

        client = anthropic.Anthropic(api_key=self.api_key)
        # Nudge Anthropic to emit raw JSON. The JSON extractor below still
        # strips ```json fences defensively.
        system_msg = (
            (system or "")
            + "\n\nReturn STRICT JSON only — no prose, no markdown fences."
        )
        msg = client.messages.create(
            model=self.model,
            max_tokens=4096,
            temperature=temperature,
            system=system_msg,
            messages=[{"role": "user", "content": user}],
        )
        content = "".join(b.text for b in msg.content if getattr(b, "type", None) == "text")
        try:
            return _extract_json(content)
        except Exception:
            raise RuntimeError(f"Model returned non-JSON: {content[:500]}")