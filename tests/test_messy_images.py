"""Tests for generate_messy_images.py and benchmark_ocr.py."""

import json
import sys
from pathlib import Path

import numpy as np
import pytest
from PIL import Image

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.generate_messy_images import (
    add_angle_distortion,
    add_bad_lighting,
    add_crumple_effect,
    add_shadow,
    add_thermal_fading,
    generate_base_receipt,
    generate_handwritten_receipt,
)
from scripts.benchmark_ocr import (
    _field_match,
    _severity_bucket,
    load_ground_truths,
    simulate_extraction,
)

IMAGE_DIR = ROOT / "data" / "training" / "messy_images"


# ---------------------------------------------------------------------------
# 1. Verify 200 images were generated
# ---------------------------------------------------------------------------
class TestImageGeneration:

    def test_200_images_exist(self):
        """Exactly 200 PNG images should exist in the output directory."""
        pngs = list(IMAGE_DIR.glob("*.png"))
        assert len(pngs) == 200, f"Expected 200 images, found {len(pngs)}"

    def test_200_json_sidecars_exist(self):
        """Each image should have a matching JSON sidecar."""
        jsons = list(IMAGE_DIR.glob("*.json"))
        assert len(jsons) == 200, f"Expected 200 JSON files, found {len(jsons)}"

    def test_json_sidecar_schema(self):
        """Every JSON sidecar must contain the required ground-truth fields."""
        required = {"image_file", "vendor", "amount", "date", "gst", "qst",
                    "distortion_type", "severity", "language"}
        for jf in sorted(IMAGE_DIR.glob("*.json"))[:10]:  # spot-check first 10
            data = json.loads(jf.read_text(encoding="utf-8"))
            missing = required - data.keys()
            assert not missing, f"{jf.name} missing keys: {missing}"


# ---------------------------------------------------------------------------
# 2. Distortion functions produce valid images
# ---------------------------------------------------------------------------
class TestDistortions:

    @pytest.fixture()
    def base_image(self):
        return generate_base_receipt("Test Vendor", 115.0, "2026-01-15", 5.0, 9.97, "en")

    def test_bad_lighting_preserves_size(self, base_image):
        result = add_bad_lighting(base_image, severity=0.7)
        assert result.size == (600, 900)
        assert result.mode == "RGB"

    def test_angle_distortion_preserves_size(self, base_image):
        result = add_angle_distortion(base_image, max_degrees=10)
        assert result.size == (600, 900)

    def test_crumple_effect_preserves_size(self, base_image):
        result = add_crumple_effect(base_image, severity=0.5)
        assert result.size == (600, 900)

    def test_thermal_fading_adds_brown_tint(self, base_image):
        """Thermal fading at high severity should add a brown/warm tint."""
        result = add_thermal_fading(base_image, severity=0.9)
        arr = np.array(result, dtype=np.float32)
        orig_arr = np.array(base_image, dtype=np.float32)
        # Red channel should increase more than blue due to brown tint
        red_diff = arr[:, :, 0].mean() - orig_arr[:, :, 0].mean()
        blue_diff = arr[:, :, 2].mean() - orig_arr[:, :, 2].mean()
        assert red_diff > blue_diff, "Thermal fading should add warm (red/brown) tint"

    def test_shadow_darkens_image(self, base_image):
        result = add_shadow(base_image, severity=0.8)
        orig_mean = np.array(base_image, dtype=np.float32).mean()
        new_mean = np.array(result, dtype=np.float32).mean()
        assert new_mean < orig_mean, "Shadow should darken the image overall"


# ---------------------------------------------------------------------------
# 3. Benchmark utility functions
# ---------------------------------------------------------------------------
class TestBenchmarkUtils:

    def test_severity_bucket(self):
        assert _severity_bucket(0.3) == "mild"
        assert _severity_bucket(0.49) == "mild"
        assert _severity_bucket(0.5) == "severe"
        assert _severity_bucket(0.9) == "severe"

    def test_field_match_vendor(self):
        assert _field_match("Home Depot", "Home Depot", "vendor")
        assert _field_match("home depot", "Home Depot", "vendor")
        assert not _field_match("Home", "Home Depot", "vendor")

    def test_field_match_amount_tolerance(self):
        assert _field_match(67.43, 67.43, "amount")
        assert _field_match(67.44, 67.43, "amount")  # within 0.015
        assert not _field_match(67.50, 67.43, "amount")

    def test_simulate_extraction_returns_required_keys(self):
        gt = {
            "vendor": "Ultramar", "amount": 50.0, "date": "2026-01-01",
            "gst": 2.17, "qst": 4.32, "distortion_type": "thermal_fading",
            "severity": 0.4, "language": "fr",
        }
        ext = simulate_extraction(gt)
        assert "vendor_name" in ext
        assert "total" in ext
        assert "document_date" in ext
        assert "confidence" in ext

    def test_ground_truths_load(self):
        """load_ground_truths should return 200 items after generation."""
        gts = load_ground_truths()
        assert len(gts) == 200
