"""LicenseGate — feature entitlement checker for Taskpilot.

Enforcement philosophy:
- No VINZY_LICENSE_KEY set → community only (gated features blocked)
- Key present + feature entitled → allow
- Key present + feature NOT entitled → block with PermissionError
- Vinzy-Engine unreachable → community only (fail-closed for gated features)

Usage:
    from taskpilot.licensing import license_gate

    # Imperative check
    license_gate.gate("std.taskpilot.advanced")

    # Decorator
    @license_gate.require_feature("std.taskpilot.enterprise")
    def get_executive_report(self):
        ...
"""

import functools
import logging
import os
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

PRICING_URL = "https://gozerai.com/pricing"

# Feature flag → human-readable name + required tier
_FEATURE_TIER_MAP = {
    # Pro (Tier 2)
    "std.taskpilot.advanced": ("Taskpilot Advanced Workflows", "Pro"),
    # Enterprise (Tier 1)
    "std.taskpilot.enterprise": ("Taskpilot Enterprise Reports", "Enterprise"),
}


class LicenseGate:
    """Cached feature entitlement checker backed by Vinzy-Engine SDK."""

    def __init__(
        self,
        license_key: Optional[str] = None,
        server_url: Optional[str] = None,
        cache_ttl: int = 300,
    ):
        self._license_key = license_key or os.environ.get("VINZY_LICENSE_KEY", "")
        self._server_url = server_url or os.environ.get(
            "VINZY_SERVER", "http://localhost:8080"
        )
        self._cache_ttl = cache_ttl
        self._client = None
        self._features_cache: Optional[list[str]] = None
        self._cache_time: float = 0.0

    @property
    def is_community_mode(self) -> bool:
        """True when no license key is configured (community mode = gated features blocked)."""
        return not self._license_key

    def _get_client(self):
        if self._client is None:
            try:
                from vinzy_engine import LicenseClient

                self._client = LicenseClient(
                    server_url=self._server_url,
                    license_key=self._license_key,
                    cache_ttl=self._cache_ttl,
                )
            except ImportError:
                logger.debug("vinzy_engine not installed; LicenseGate in community mode")
                return None
        return self._client

    def _refresh_features(self) -> list[str]:
        """Validate license and cache the entitled features list."""
        now = time.time()
        if self._features_cache is not None and (now - self._cache_time) < self._cache_ttl:
            return self._features_cache

        client = self._get_client()
        if client is None:
            return []

        try:
            result = client.validate()
            if result.valid:
                self._features_cache = result.features
                self._cache_time = now
                return self._features_cache
            self._features_cache = []
            self._cache_time = now
            return []
        except Exception:
            logger.warning("Vinzy-Engine unreachable; fail-closed — all gated features disabled")
            return []

    def check_feature(self, flag: str) -> bool:
        """Check if a feature flag is entitled."""
        if self.is_community_mode:
            return False

        features = self._refresh_features()
        if not features:
            return False

        return flag in features

    def require_feature(self, flag: str, label: str | None = None):
        """Decorator that gates a function behind a feature flag."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if not self.check_feature(flag):
                    name, tier = _FEATURE_TIER_MAP.get(flag, (flag, "a commercial"))
                    raise PermissionError(
                        f"{label or name} requires {tier} license. "
                        f"Set VINZY_LICENSE_KEY or visit {PRICING_URL}"
                    )
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def gate(self, flag: str, label: str | None = None) -> None:
        """Imperative gate — call at the top of a method."""
        if not self.check_feature(flag):
            name, tier = _FEATURE_TIER_MAP.get(flag, (flag, "a commercial"))
            raise PermissionError(
                f"{label or name} requires {tier} license. "
                f"Set VINZY_LICENSE_KEY or visit {PRICING_URL}"
            )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None


# Module-level singleton
license_gate = LicenseGate()
