"""Extended LicenseGate tests for Taskpilot — tier restrictions and upgrade paths."""

import time
from unittest.mock import MagicMock, patch

import pytest

from taskpilot.licensing import LicenseGate, PRICING_URL, _FEATURE_TIER_MAP


# ---------------------------------------------------------------------------
# Helpers (same pattern as existing test_licensing.py)
# ---------------------------------------------------------------------------

def _make_gate(key="", **kw):
    return LicenseGate(license_key=key, **kw)


def _mock_client(features=None, valid=True, raises=None):
    client = MagicMock()
    if raises:
        client.validate.side_effect = raises
    else:
        result = MagicMock()
        result.valid = valid
        result.features = features or []
        client.validate.return_value = result
    return client


# ---------------------------------------------------------------------------
# Free (community) tier restrictions
# ---------------------------------------------------------------------------

class TestFreeTierRestrictions:
    """Community mode blocks all gated features."""

    def test_advanced_blocked(self):
        gate = _make_gate(key="")
        assert gate.check_feature("std.taskpilot.advanced") is False

    def test_enterprise_blocked(self):
        gate = _make_gate(key="")
        assert gate.check_feature("std.taskpilot.enterprise") is False

    def test_gate_advanced_raises_with_tier_name(self):
        gate = _make_gate(key="")
        with pytest.raises(PermissionError, match="Pro"):
            gate.gate("std.taskpilot.advanced")

    def test_gate_enterprise_raises_with_tier_name(self):
        gate = _make_gate(key="")
        with pytest.raises(PermissionError, match="Growth"):
            gate.gate("std.taskpilot.enterprise")

    def test_unknown_feature_blocked_in_community(self):
        gate = _make_gate(key="")
        assert gate.check_feature("std.taskpilot.unknown_future") is False

    def test_decorator_blocks_community(self):
        gate = _make_gate(key="")

        @gate.require_feature("std.taskpilot.advanced")
        def premium_action():
            return "should not reach"

        with pytest.raises(PermissionError):
            premium_action()


# ---------------------------------------------------------------------------
# Pro tier unlocked features
# ---------------------------------------------------------------------------

class TestProTierFeatures:
    """Pro key grants advanced but not enterprise."""

    def test_advanced_allowed(self):
        gate = _make_gate(key="PRO-KEY")
        gate._client = _mock_client(features=["std.taskpilot.advanced"])
        assert gate.check_feature("std.taskpilot.advanced") is True

    def test_enterprise_still_blocked(self):
        gate = _make_gate(key="PRO-KEY")
        gate._client = _mock_client(features=["std.taskpilot.advanced"])
        assert gate.check_feature("std.taskpilot.enterprise") is False

    def test_gate_passes_for_advanced(self):
        gate = _make_gate(key="PRO-KEY")
        gate._client = _mock_client(features=["std.taskpilot.advanced"])
        gate.gate("std.taskpilot.advanced")  # should not raise

    def test_gate_raises_for_enterprise(self):
        gate = _make_gate(key="PRO-KEY")
        gate._client = _mock_client(features=["std.taskpilot.advanced"])
        with pytest.raises(PermissionError, match="Growth"):
            gate.gate("std.taskpilot.enterprise")


# ---------------------------------------------------------------------------
# Enterprise (Growth) tier full access
# ---------------------------------------------------------------------------

class TestEnterpriseTierFullAccess:
    """Enterprise key grants both advanced and enterprise."""

    def test_advanced_allowed(self):
        gate = _make_gate(key="ENT-KEY")
        gate._client = _mock_client(
            features=["std.taskpilot.advanced", "std.taskpilot.enterprise"]
        )
        assert gate.check_feature("std.taskpilot.advanced") is True

    def test_enterprise_allowed(self):
        gate = _make_gate(key="ENT-KEY")
        gate._client = _mock_client(
            features=["std.taskpilot.advanced", "std.taskpilot.enterprise"]
        )
        assert gate.check_feature("std.taskpilot.enterprise") is True

    def test_gate_both_pass(self):
        gate = _make_gate(key="ENT-KEY")
        gate._client = _mock_client(
            features=["std.taskpilot.advanced", "std.taskpilot.enterprise"]
        )
        gate.gate("std.taskpilot.advanced")
        gate.gate("std.taskpilot.enterprise")


# ---------------------------------------------------------------------------
# Expired / invalid license
# ---------------------------------------------------------------------------

class TestExpiredOrInvalidLicense:
    """Expired or invalid license falls back to community."""

    def test_invalid_key_blocks_advanced(self):
        gate = _make_gate(key="EXPIRED-KEY")
        gate._client = _mock_client(valid=False)
        assert gate.check_feature("std.taskpilot.advanced") is False

    def test_invalid_key_gate_raises(self):
        gate = _make_gate(key="EXPIRED-KEY")
        gate._client = _mock_client(valid=False)
        with pytest.raises(PermissionError):
            gate.gate("std.taskpilot.advanced")

    def test_server_error_blocks_feature(self):
        gate = _make_gate(key="VALID-KEY")
        gate._client = _mock_client(raises=TimeoutError("timeout"))
        assert gate.check_feature("std.taskpilot.advanced") is False

    def test_error_message_includes_pricing_url(self):
        gate = _make_gate(key="")
        with pytest.raises(PermissionError, match=PRICING_URL):
            gate.gate("std.taskpilot.enterprise")


# ---------------------------------------------------------------------------
# Upgrade path validation
# ---------------------------------------------------------------------------

class TestUpgradePath:
    """Feature-tier map correctness and upgrade semantics."""

    def test_advanced_tier_is_pro(self):
        _, tier = _FEATURE_TIER_MAP["std.taskpilot.advanced"]
        assert tier == "Pro"

    def test_enterprise_tier_is_growth(self):
        _, tier = _FEATURE_TIER_MAP["std.taskpilot.enterprise"]
        assert tier == "Growth"

    def test_custom_label_overrides_default(self):
        gate = _make_gate(key="")
        with pytest.raises(PermissionError, match="Custom Label"):
            gate.gate("std.taskpilot.advanced", label="Custom Label")

    def test_cache_refreshes_after_upgrade(self):
        """Simulate upgrade: Pro features first, then Enterprise after TTL."""
        gate = _make_gate(key="UPGRADE-KEY", cache_ttl=1)
        pro_client = _mock_client(features=["std.taskpilot.advanced"])
        gate._client = pro_client

        assert gate.check_feature("std.taskpilot.advanced") is True
        assert gate.check_feature("std.taskpilot.enterprise") is False

        # Expire cache and swap to enterprise
        gate._cache_time = time.time() - 2
        ent_client = _mock_client(
            features=["std.taskpilot.advanced", "std.taskpilot.enterprise"]
        )
        gate._client = ent_client

        assert gate.check_feature("std.taskpilot.enterprise") is True
