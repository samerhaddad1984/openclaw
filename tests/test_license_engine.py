"""Tests for src/engines/license_engine.py"""
import pytest
import json
import hmac
import hashlib
import base64
from datetime import date, datetime, timedelta
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.engines.license_engine import (
    load_license,
    get_license_status,
    check_feature,
    generate_license_key,
    TIER_DEFAULTS as TIER_DEFINITIONS,
)

SECRET = "test-secret-key-12345"


def make_license(tier: str = "professionnel", months: int = 12, secret: str = SECRET) -> str:
    """Helper to generate a valid license key."""
    today = date.today()
    expiry = today + timedelta(days=max(1, months * 30))
    return generate_license_key(
        tier=tier,
        firm_name="Test Firm",
        expiry_date=expiry.strftime("%Y-%m-%d"),
        issued_at=today.strftime("%Y-%m-%d"),
        secret=secret,
    )


class TestLoadLicense:
    def test_valid_license_loads(self):
        key = make_license()
        result = load_license(key, SECRET)
        assert result["tier"] == "professionnel"
        assert result["firm_name"] == "Test Firm"

    def test_wrong_secret_fails(self):
        key = make_license()
        with pytest.raises(ValueError) as exc_info:
            load_license(key, "wrong-secret")
        assert "signature" in str(exc_info.value).lower() or "mismatch" in str(exc_info.value).lower()

    def test_expired_license(self):
        today = date.today()
        expired_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        key = generate_license_key(
            tier="professionnel",
            firm_name="Test Firm",
            expiry_date=expired_date,
            issued_at=(today - timedelta(days=32)).strftime("%Y-%m-%d"),
            secret=SECRET,
        )
        with pytest.raises(ValueError) as exc_info:
            load_license(key, SECRET)
        assert "expire" in str(exc_info.value).lower()

    def test_invalid_format(self):
        with pytest.raises(ValueError):
            load_license("NOT-A-VALID-KEY", SECRET)

    def test_all_tiers(self):
        for tier in ["essentiel", "professionnel", "cabinet", "entreprise"]:
            key = make_license(tier=tier)
            result = load_license(key, SECRET)
            assert result["tier"] == tier


class TestCheckFeature:
    def test_essentiel_has_basic_review(self):
        key = make_license(tier="essentiel")
        with patch("src.engines.license_engine.get_license_status") as mock_status:
            mock_status.return_value = {
                "valid": True,
                "tier": "essentiel",
                "features": TIER_DEFINITIONS["essentiel"]["features"],
            }
            assert check_feature("basic_review") is True

    def test_essentiel_lacks_audit_module(self):
        key = make_license(tier="essentiel")
        with patch("src.engines.license_engine.get_license_status") as mock_status:
            mock_status.return_value = {
                "valid": True,
                "tier": "essentiel",
                "features": TIER_DEFINITIONS["essentiel"]["features"],
            }
            assert check_feature("audit_module") is False

    def test_entreprise_has_all_features(self):
        with patch("src.engines.license_engine.get_license_status") as mock_status:
            mock_status.return_value = {
                "valid": True,
                "tier": "entreprise",
                "features": TIER_DEFINITIONS["entreprise"]["features"],
            }
            for feature in ["basic_review", "audit_module", "api_access", "sampling"]:
                assert check_feature(feature) is True

    def test_no_license_returns_false(self):
        with patch("src.engines.license_engine.get_license_status") as mock_status:
            mock_status.return_value = {
                "valid": False,
                "tier": "none",
                "features": [],
            }
            assert check_feature("basic_review") is False


class TestTierDefinitions:
    def test_all_tiers_present(self):
        for tier in ["essentiel", "professionnel", "cabinet", "entreprise"]:
            assert tier in TIER_DEFINITIONS

    def test_tier_limits_increase(self):
        assert TIER_DEFINITIONS["essentiel"]["max_clients"] < TIER_DEFINITIONS["professionnel"]["max_clients"]
        assert TIER_DEFINITIONS["professionnel"]["max_clients"] < TIER_DEFINITIONS["cabinet"]["max_clients"]

    def test_entreprise_unlimited(self):
        # entreprise has 999999 (effectively unlimited) max_clients and max_users
        assert TIER_DEFINITIONS["entreprise"]["max_clients"] >= 999999
        assert TIER_DEFINITIONS["entreprise"]["max_users"] >= 999999

    def test_tier_features_are_lists(self):
        for tier, info in TIER_DEFINITIONS.items():
            assert isinstance(info.get("features", []), list), f"features for {tier} should be a list"

    def test_higher_tiers_include_lower_features(self):
        """Entreprise should include all essentiel features."""
        essentiel_features = set(TIER_DEFINITIONS["essentiel"]["features"])
        entreprise_features = set(TIER_DEFINITIONS["entreprise"]["features"])
        assert essentiel_features.issubset(entreprise_features)
