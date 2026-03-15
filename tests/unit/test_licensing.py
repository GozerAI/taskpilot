"""Tests for taskpilot.licensing — LicenseGate."""

import time
from unittest.mock import MagicMock, patch

import pytest

from taskpilot.licensing import LicenseGate, PRICING_URL, _FEATURE_TIER_MAP


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


class TestCommunityMode:
    def test_is_community_mode_no_key(self):
        gate = _make_gate(key="")
        assert gate.is_community_mode is True

    def test_check_feature_returns_false(self):
        gate = _make_gate(key="")
        assert gate.check_feature("std.taskpilot.advanced") is False

    def test_gate_raises_permission_error(self):
        gate = _make_gate(key="")
        with pytest.raises(PermissionError, match="Pro license"):
            gate.gate("std.taskpilot.advanced")

    def test_gate_enterprise_raises(self):
        gate = _make_gate(key="")
        with pytest.raises(PermissionError, match="Growth license"):
            gate.gate("std.taskpilot.enterprise")

    def test_error_message_contains_pricing_url(self):
        gate = _make_gate(key="")
        with pytest.raises(PermissionError, match=PRICING_URL):
            gate.gate("std.taskpilot.advanced")


class TestWithValidKey:
    def test_is_not_community_mode(self):
        gate = _make_gate(key="VALID-KEY")
        assert gate.is_community_mode is False

    def test_check_feature_entitled(self):
        gate = _make_gate(key="VALID-KEY")
        gate._client = _mock_client(features=["std.taskpilot.advanced"])
        assert gate.check_feature("std.taskpilot.advanced") is True

    def test_check_feature_not_entitled(self):
        gate = _make_gate(key="VALID-KEY")
        gate._client = _mock_client(features=["std.taskpilot.advanced"])
        assert gate.check_feature("std.taskpilot.enterprise") is False

    def test_gate_passes_when_entitled(self):
        gate = _make_gate(key="VALID-KEY")
        gate._client = _mock_client(features=["std.taskpilot.advanced", "std.taskpilot.enterprise"])
        gate.gate("std.taskpilot.advanced")

    def test_gate_raises_when_not_entitled(self):
        gate = _make_gate(key="VALID-KEY")
        gate._client = _mock_client(features=["std.taskpilot.advanced"])
        with pytest.raises(PermissionError, match="Enterprise"):
            gate.gate("std.taskpilot.enterprise")

    def test_require_feature_decorator_passes(self):
        gate = _make_gate(key="VALID-KEY")
        gate._client = _mock_client(features=["std.taskpilot.advanced"])

        @gate.require_feature("std.taskpilot.advanced")
        def my_func():
            return "ok"

        assert my_func() == "ok"

    def test_require_feature_decorator_blocks(self):
        gate = _make_gate(key="VALID-KEY")
        gate._client = _mock_client(features=[])

        @gate.require_feature("std.taskpilot.advanced")
        def my_func():
            return "ok"

        with pytest.raises(PermissionError):
            my_func()


class TestFailClosed:
    def test_unreachable_server(self):
        gate = _make_gate(key="VALID-KEY")
        gate._client = _mock_client(raises=ConnectionError("offline"))
        assert gate.check_feature("std.taskpilot.advanced") is False

    def test_invalid_key(self):
        gate = _make_gate(key="BAD-KEY")
        gate._client = _mock_client(valid=False)
        assert gate.check_feature("std.taskpilot.advanced") is False

    def test_no_vinzy_sdk(self):
        gate = _make_gate(key="VALID-KEY")
        with patch.object(gate, "_get_client", return_value=None):
            assert gate.check_feature("std.taskpilot.advanced") is False


class TestCaching:
    def test_cache_hit(self):
        gate = _make_gate(key="VALID-KEY", cache_ttl=60)
        client = _mock_client(features=["std.taskpilot.advanced"])
        gate._client = client

        assert gate.check_feature("std.taskpilot.advanced") is True
        assert gate.check_feature("std.taskpilot.advanced") is True
        assert client.validate.call_count == 1

    def test_cache_expires(self):
        gate = _make_gate(key="VALID-KEY", cache_ttl=1)
        client = _mock_client(features=["std.taskpilot.advanced"])
        gate._client = client

        gate.check_feature("std.taskpilot.advanced")
        assert client.validate.call_count == 1

        gate._cache_time = time.time() - 2
        gate.check_feature("std.taskpilot.advanced")
        assert client.validate.call_count == 2

    def test_feature_tier_map_has_both_flags(self):
        assert "std.taskpilot.advanced" in _FEATURE_TIER_MAP
        assert "std.taskpilot.enterprise" in _FEATURE_TIER_MAP

    def test_close_clears_client(self):
        gate = _make_gate(key="VALID-KEY")
        gate._client = _mock_client(features=[])
        gate.close()
        assert gate._client is None
