"""Automation feature upsell engine.

Tracks automation usage (workflows, scheduled tasks, triggers) and
surfaces upgrade offers when users approach limits or attempt
features beyond their current tier.

Backlog item: #314
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from taskpilot.licensing import license_gate

logger = logging.getLogger(__name__)

ADVANCED_FEATURE = "std.taskpilot.advanced"
ENTERPRISE_FEATURE = "std.taskpilot.enterprise"

PRICING_URL = "https://gozerai.com/pricing"


class AutomationTier(Enum):
    FREE = "free"
    STARTER = "starter"
    PRO = "pro"
    ENTERPRISE = "enterprise"


_TIER_LIMITS = {
    AutomationTier.FREE: {
        "workflows": 3,
        "scheduled_tasks": 5,
        "triggers": 3,
        "executions_per_day": 10,
        "parallel_tasks": 1,
    },
    AutomationTier.STARTER: {
        "workflows": 15,
        "scheduled_tasks": 25,
        "triggers": 15,
        "executions_per_day": 100,
        "parallel_tasks": 3,
    },
    AutomationTier.PRO: {
        "workflows": 100,
        "scheduled_tasks": 200,
        "triggers": 100,
        "executions_per_day": 1000,
        "parallel_tasks": 10,
    },
    AutomationTier.ENTERPRISE: {
        "workflows": 99999,
        "scheduled_tasks": 99999,
        "triggers": 99999,
        "executions_per_day": 99999,
        "parallel_tasks": 50,
    },
}

_TIER_PRICING_MONTHLY_CENTS = {
    AutomationTier.FREE: 0,
    AutomationTier.STARTER: 2900,
    AutomationTier.PRO: 7900,
    AutomationTier.ENTERPRISE: 19900,
}

_TIER_FEATURES = {
    AutomationTier.FREE: ["basic_scheduling", "manual_triggers"],
    AutomationTier.STARTER: [
        "basic_scheduling",
        "manual_triggers",
        "cron_scheduling",
        "webhook_triggers",
        "email_notifications",
    ],
    AutomationTier.PRO: [
        "basic_scheduling",
        "manual_triggers",
        "cron_scheduling",
        "webhook_triggers",
        "email_notifications",
        "parallel_execution",
        "conditional_logic",
        "retry_policies",
        "custom_handlers",
        "api_access",
    ],
    AutomationTier.ENTERPRISE: [
        "all_pro_features",
        "sso_integration",
        "audit_logging",
        "sla_guarantees",
        "dedicated_workers",
        "custom_integrations",
        "priority_support",
    ],
}


@dataclass
class AutomationPackage:
    """An automation tier package."""

    id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    tier: AutomationTier = AutomationTier.FREE
    price_monthly_cents: int = 0
    features: List[str] = field(default_factory=list)
    limits: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        if self.price_monthly_cents == 0 and self.tier != AutomationTier.FREE:
            self.price_monthly_cents = _TIER_PRICING_MONTHLY_CENTS.get(self.tier, 0)
        if not self.features:
            self.features = list(_TIER_FEATURES.get(self.tier, []))
        if not self.limits:
            self.limits = dict(_TIER_LIMITS.get(self.tier, {}))

    @property
    def price_monthly_dollars(self) -> float:
        return self.price_monthly_cents / 100.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "tier": self.tier.value,
            "price_monthly_cents": self.price_monthly_cents,
            "price_monthly_dollars": self.price_monthly_dollars,
            "features": self.features,
            "limits": self.limits,
        }


@dataclass
class AutomationUsage:
    """Current automation usage for a tenant."""

    tenant_id: str = ""
    tier: AutomationTier = AutomationTier.FREE
    workflows_created: int = 0
    scheduled_tasks_created: int = 0
    triggers_created: int = 0
    executions_today: int = 0
    parallel_tasks_running: int = 0
    feature_blocks_hit: int = 0

    @property
    def limits(self) -> Dict[str, int]:
        return _TIER_LIMITS.get(self.tier, _TIER_LIMITS[AutomationTier.FREE])

    def utilization(self, metric: str) -> float:
        metric_map = {
            "workflows": self.workflows_created,
            "scheduled_tasks": self.scheduled_tasks_created,
            "triggers": self.triggers_created,
            "executions_per_day": self.executions_today,
            "parallel_tasks": self.parallel_tasks_running,
        }
        used = metric_map.get(metric, 0)
        limit = self.limits.get(metric, 0)
        if limit <= 0:
            return 0.0
        return min(100.0, (used / limit) * 100.0)


@dataclass
class AutomationOffer:
    """An upsell offer for automation features."""

    id: str = field(default_factory=lambda: str(uuid4()))
    tenant_id: str = ""
    current_tier: AutomationTier = AutomationTier.FREE
    recommended_tier: AutomationTier = AutomationTier.STARTER
    headline: str = ""
    reason: str = ""
    benefits: List[str] = field(default_factory=list)
    monthly_increase_cents: int = 0
    priority: float = 0.0
    cta_url: str = PRICING_URL
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    accepted: Optional[bool] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "tenant_id": self.tenant_id,
            "current_tier": self.current_tier.value,
            "recommended_tier": self.recommended_tier.value,
            "headline": self.headline,
            "reason": self.reason,
            "benefits": self.benefits,
            "monthly_increase_cents": self.monthly_increase_cents,
            "monthly_increase_dollars": self.monthly_increase_cents / 100.0,
            "priority": self.priority,
            "cta_url": self.cta_url,
            "accepted": self.accepted,
        }


class AutomationUpsellEngine:
    """Evaluates automation usage and generates upgrade offers.

    Tracks usage against tier limits for workflows, scheduled tasks,
    triggers, daily executions, and parallel tasks.  Generates
    context-aware offers when limits are approached or exceeded.
    """

    SOFT_THRESHOLD = 70.0
    HARD_THRESHOLD = 90.0
    LIMIT_THRESHOLD = 100.0

    _METRIC_LABELS = {
        "workflows": "Workflows",
        "scheduled_tasks": "Scheduled Tasks",
        "triggers": "Triggers",
        "executions_per_day": "Daily Executions",
        "parallel_tasks": "Parallel Tasks",
    }

    def __init__(self):
        self._offers: Dict[str, AutomationOffer] = {}
        self._conversions: int = 0
        self._total_offers: int = 0

    def evaluate(self, usage: AutomationUsage) -> List[AutomationOffer]:
        """Evaluate usage and return prioritized upsell offers."""
        if usage.tier == AutomationTier.ENTERPRISE:
            return []

        offers: List[AutomationOffer] = []
        next_tier = self._next_tier(usage.tier)
        price_increase = (
            _TIER_PRICING_MONTHLY_CENTS.get(next_tier, 0)
            - _TIER_PRICING_MONTHLY_CENTS.get(usage.tier, 0)
        )

        for metric, label in self._METRIC_LABELS.items():
            util = usage.utilization(metric)
            offer = self._evaluate_metric(usage, metric, label, util, next_tier, price_increase)
            if offer:
                offers.append(offer)

        # Feature-gate trigger
        if usage.feature_blocks_hit > 0:
            new_features = set(_TIER_FEATURES.get(next_tier, [])) - set(
                _TIER_FEATURES.get(usage.tier, [])
            )
            offers.append(
                AutomationOffer(
                    tenant_id=usage.tenant_id,
                    current_tier=usage.tier,
                    recommended_tier=next_tier,
                    headline="Unlock Advanced Automation",
                    reason=(
                        f"You've hit {usage.feature_blocks_hit} feature gate(s). "
                        f"Upgrade to {next_tier.value} for more capabilities."
                    ),
                    benefits=list(new_features)[:5],
                    monthly_increase_cents=price_increase,
                    priority=85.0,
                )
            )

        offers.sort(key=lambda o: o.priority, reverse=True)
        for o in offers:
            self._offers[o.id] = o
            self._total_offers += 1

        return offers

    def accept_offer(self, offer_id: str) -> Dict[str, Any]:
        """Accept an automation upsell offer."""
        offer = self._offers.get(offer_id)
        if not offer:
            raise ValueError(f"Offer not found: {offer_id}")
        if offer.accepted is not None:
            raise ValueError("Offer already processed")
        offer.accepted = True
        self._conversions += 1
        return {
            "success": True,
            "offer_id": offer_id,
            "new_tier": offer.recommended_tier.value,
            "monthly_cost_cents": _TIER_PRICING_MONTHLY_CENTS[offer.recommended_tier],
        }

    def decline_offer(self, offer_id: str) -> Dict[str, Any]:
        """Decline an automation upsell offer."""
        offer = self._offers.get(offer_id)
        if not offer:
            raise ValueError(f"Offer not found: {offer_id}")
        offer.accepted = False
        return {"success": True, "offer_id": offer_id}

    def get_tier_comparison(self) -> List[Dict[str, Any]]:
        """Get tier comparison for upsell display."""
        comparison = []
        for tier in AutomationTier:
            comparison.append({
                "tier": tier.value,
                "price_monthly_cents": _TIER_PRICING_MONTHLY_CENTS[tier],
                "price_monthly_dollars": _TIER_PRICING_MONTHLY_CENTS[tier] / 100.0,
                "limits": _TIER_LIMITS[tier],
                "features": _TIER_FEATURES[tier],
            })
        return comparison

    def get_conversion_rate(self) -> float:
        if self._total_offers == 0:
            return 0.0
        return (self._conversions / self._total_offers) * 100.0

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_offers": self._total_offers,
            "accepted_offers": self._conversions,
            "declined_offers": len([o for o in self._offers.values() if o.accepted is False]),
            "pending_offers": len([o for o in self._offers.values() if o.accepted is None]),
            "conversion_rate": self.get_conversion_rate(),
        }

    # -- internals --

    def _next_tier(self, current: AutomationTier) -> AutomationTier:
        order = list(AutomationTier)
        idx = order.index(current)
        return order[min(idx + 1, len(order) - 1)]

    def _evaluate_metric(
        self,
        usage: AutomationUsage,
        metric: str,
        label: str,
        utilization: float,
        next_tier: AutomationTier,
        price_increase: int,
    ) -> Optional[AutomationOffer]:
        limit = usage.limits.get(metric, 0)
        next_limit = _TIER_LIMITS[next_tier].get(metric, 0)

        if utilization >= self.LIMIT_THRESHOLD:
            return AutomationOffer(
                tenant_id=usage.tenant_id,
                current_tier=usage.tier,
                recommended_tier=next_tier,
                headline=f"{label} Limit Reached",
                reason=f"You've used all {limit} {label.lower()}.",
                benefits=[f"Up to {next_limit} {label.lower()} on {next_tier.value}"],
                monthly_increase_cents=price_increase,
                priority=100.0,
            )
        elif utilization >= self.HARD_THRESHOLD:
            return AutomationOffer(
                tenant_id=usage.tenant_id,
                current_tier=usage.tier,
                recommended_tier=next_tier,
                headline=f"Almost at {label} Limit",
                reason=f"At {utilization:.0f}% of your {label.lower()} limit.",
                benefits=[f"Upgrade for {next_limit} {label.lower()}"],
                monthly_increase_cents=price_increase,
                priority=75.0,
            )
        elif utilization >= self.SOFT_THRESHOLD:
            return AutomationOffer(
                tenant_id=usage.tenant_id,
                current_tier=usage.tier,
                recommended_tier=next_tier,
                headline=f"Approaching {label} Limit",
                reason=f"Using {utilization:.0f}% of your {label.lower()} quota.",
                benefits=[f"More {label.lower()} on {next_tier.value}"],
                monthly_increase_cents=price_increase,
                priority=50.0,
            )
        return None
