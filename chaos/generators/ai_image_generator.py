"""Google Imagen client with budget tracking and a deterministic cache.

Uses google.generativeai. If the SDK is not installed, the class degrades
gracefully to a placeholder (PIL-rendered text page) so the framework can run
without spending money, with --no-ai or when the key is absent.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class BudgetState:
    used_usd: float = 0.0
    generated_count: int = 0
    cache_hits: int = 0
    fallback_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "used_usd":        round(self.used_usd, 4),
            "generated_count": self.generated_count,
            "cache_hits":      self.cache_hits,
            "fallback_count":  self.fallback_count,
        }


class BudgetExceededError(Exception):
    pass


class AIImageGenerator:
    """Generate receipt images via Google Imagen, with on-disk caching."""

    def __init__(
        self,
        *,
        api_key: str | None,
        cache_dir: Path,
        budget_usd: float,
        cost_per_image: float,
        model: str = "imagen-3.0-generate-002",
        no_ai: bool = False,
    ):
        self.api_key = api_key or ""
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.budget_usd = float(budget_usd)
        self.cost_per_image = float(cost_per_image)
        self.model = model
        self.no_ai = bool(no_ai) or not self.api_key
        self.state = BudgetState()

        # Lazy import; record whether SDK is available
        self._sdk_ready = False
        self._client = None
        if not self.no_ai:
            try:
                import google.generativeai as genai  # type: ignore
                genai.configure(api_key=self.api_key)
                self._client = genai
                self._sdk_ready = True
            except Exception as e:  # pragma: no cover
                log.warning("google.generativeai not available: %s — falling back to placeholders", e)
                self._sdk_ready = False

    # ------------------------------------------------------------------
    # Budget
    # ------------------------------------------------------------------
    def remaining_budget(self) -> float:
        return max(0.0, self.budget_usd - self.state.used_usd)

    def can_afford_one(self) -> bool:
        return self.remaining_budget() >= self.cost_per_image

    def budget_snapshot(self) -> dict[str, Any]:
        return {
            "budget_usd":      self.budget_usd,
            "used_usd":        round(self.state.used_usd, 4),
            "remaining_usd":   round(self.remaining_budget(), 4),
            "generated_count": self.state.generated_count,
            "cache_hits":      self.state.cache_hits,
            "fallback_count":  self.state.fallback_count,
            "cost_per_image":  self.cost_per_image,
        }

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------
    def _cache_key(self, prompt: str) -> str:
        return hashlib.sha1(f"{self.model}|{prompt}".encode("utf-8")).hexdigest()

    def _cache_path(self, prompt: str) -> Path:
        return self.cache_dir / f"{self._cache_key(prompt)}.png"

    # ------------------------------------------------------------------
    # Generation
    # ------------------------------------------------------------------
    def generate(self, scenario: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
        """Return (image_path, ground_truth) for a receipt scenario."""
        spec = scenario.get("input_spec") or {}
        prompt = spec.get("prompt") or scenario.get("description") or ""
        gt = scenario.get("ground_truth") or {}

        # 1. Cache hit
        cached = self._cache_path(prompt)
        if cached.exists() and cached.stat().st_size > 0:
            self.state.cache_hits += 1
            return cached, gt

        # 2. No-AI fallback or missing SDK → render a placeholder
        if self.no_ai or not self._sdk_ready:
            self._render_placeholder(cached, scenario)
            self.state.fallback_count += 1
            return cached, gt

        # 3. Budget enforcement
        if not self.can_afford_one():
            raise BudgetExceededError(
                f"Budget exhausted (remaining ${self.remaining_budget():.2f} < "
                f"cost ${self.cost_per_image:.2f})"
            )

        # 4. Real generation
        try:
            img_bytes = self._call_imagen(prompt)
            cached.write_bytes(img_bytes)
            self.state.used_usd += self.cost_per_image
            self.state.generated_count += 1
            return cached, gt
        except Exception as e:
            log.warning("Imagen call failed (%s) — falling back to placeholder", e)
            self._render_placeholder(cached, scenario)
            self.state.fallback_count += 1
            return cached, gt

    # ------------------------------------------------------------------
    # Imagen HTTP via SDK
    # ------------------------------------------------------------------
    def _call_imagen(self, prompt: str) -> bytes:
        """Call Google's image generation endpoint.

        The Gemini Python SDK exposes this under slightly different paths
        across versions. We try the image-generation client paths in order.
        """
        assert self._client is not None
        genai = self._client

        # Newer SDK: genai.Client().models.generate_images(...)
        try:
            client = genai.Client()  # type: ignore[attr-defined]
            resp = client.models.generate_images(  # type: ignore[attr-defined]
                model=self.model,
                prompt=prompt,
                config={"number_of_images": 1, "aspect_ratio": "3:4"},
            )
            imgs = getattr(resp, "generated_images", None) or []
            if imgs:
                b = getattr(imgs[0].image, "image_bytes", None)
                if b:
                    return b
        except Exception:
            pass

        # Older SDK variant
        try:
            model = genai.ImageGenerationModel.from_pretrained(self.model)  # type: ignore[attr-defined]
            resp = model.generate_images(prompt=prompt, number_of_images=1)
            imgs = getattr(resp, "images", None) or []
            if imgs:
                b = getattr(imgs[0], "_image_bytes", None) or getattr(imgs[0], "image_bytes", None)
                if b:
                    return b
        except Exception:
            pass

        raise RuntimeError("no compatible Imagen SDK surface found")

    # ------------------------------------------------------------------
    # Placeholder renderer (no $)
    # ------------------------------------------------------------------
    def _render_placeholder(self, path: Path, scenario: dict[str, Any]) -> None:
        """Render a text-only receipt PNG so the pipeline has *something* to OCR."""
        try:
            from PIL import Image, ImageDraw, ImageFont  # type: ignore
        except Exception:
            path.write_bytes(b"")
            return

        gt = scenario.get("ground_truth") or {}
        img = Image.new("RGB", (720, 1200), color="white")
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.load_default()
        except Exception:
            font = None

        lines = [
            f"CHAOS PLACEHOLDER — {scenario.get('subtype','?')}",
            "",
            f"Vendor:   {gt.get('vendor','?')}",
            f"Date:     {gt.get('document_date','?')}",
            f"Currency: {gt.get('currency','CAD')}",
            f"Subtotal: {gt.get('subtotal','?')}",
            f"GST:      {gt.get('gst','?')}",
            f"QST:      {gt.get('qst','?')}",
            f"TOTAL:    {gt.get('total','?')}",
            "",
            f"Items:    {gt.get('line_count','?')}",
            "",
            f"(difficulty={scenario.get('difficulty','?')})",
        ]
        y = 20
        for line in lines:
            draw.text((20, y), line, fill="black", font=font)
            y += 22

        path.parent.mkdir(parents=True, exist_ok=True)
        img.save(path, format="PNG")
