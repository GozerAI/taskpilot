"""Priority queue upsell for faster task execution.

Premium users can purchase priority boosts that move their tasks
to the front of the execution queue, or subscribe to a priority
tier for always-elevated execution.

Backlog item: #325
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from taskpilot.licensing import license_gate

logger = logging.getLogger(__name__)

ADVANCED_FEATURE = "std.taskpilot.advanced"
ENTERPRISE_FEATURE = "std.taskpilot.enterprise"

PRICING_URL = "https://gozerai.com/pricing"


class QueueTier(Enum):
    STANDARD = "standard"
    PRIORITY = "priority"
    EXPRESS = "express"
    DEDICATED = "dedicated"


_TIER_CONFIG = {
    QueueTier.STANDARD: {
        "multiplier": 1.0,
        "max_concurrent": 1,
        "retry_priority": False,
        "sla_seconds": 300,
        "price_monthly_cents": 0,
    },
    QueueTier.PRIORITY: {
        "multiplier": 2.0,
        "max_concurrent": 3,
        "retry_priority": True,
        "sla_seconds": 120,
        "price_monthly_cents": 2900,
    },
    QueueTier.EXPRESS: {
        "multiplier": 5.0,
        "max_concurrent": 5,
        "sla_seconds": 30,
        "retry_priority": True,
        "price_monthly_cents": 7900,
    },
    QueueTier.DEDICATED: {
        "multiplier": 10.0,
        "max_concurrent": 10,
        "retry_priority": True,
        "sla_seconds": 10,
        "price_monthly_cents": 19900,
    },
}

# One-time boost pricing (cents)
_BOOST_PRICING = {
    "single": 299,      # Single task boost
    "batch_5": 999,      # 5-task boost pack
    "batch_20": 2999,    # 20-task boost pack
    "unlimited_24h": 4999,  # Unlimited boosts for 24 hours
}


@dataclass
class PriorityBoost:
    """A one-time priority boost for a task or batch."""

    id: str = field(default_factory=lambda: str(uuid4()))
    tenant_id: str = ""
    boost_type: str = "single"
    task_ids: List[str] = field(default_factory=list)
    amount_cents: int = 0
    boosts_remaining: int = 1
    boosts_used: int = 0
    status: str = "active"
    purchased_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None

    def __post_init__(self):
        if self.amount_cents == 0:
            self.amount_cents = _BOOST_PRICING.get(self.boost_type, 299)
        boost_counts = {
            "single": 1,
            "batch_5": 5,
            "batch_20": 20,
            "unlimited_24h": 99999,
        }
        if self.boosts_remaining <= 1 and self.boost_type != "single":
            self.boosts_remaining = boost_counts.get(self.boost_type, 1)
        if self.boost_type == "unlimited_24h" and not self.expires_at:
            self.expires_at = self.purchased_at + timedelta(hours=24)

    @property
    def is_active(self) -> bool:
        if self.status != "active":
            return False
        if self.expires_at and datetime.now(timezone.utc) > self.expires_at:
            return False
        return self.boosts_remaining > 0

    def use_boost(self, task_id: str) -> bool:
        """Use one boost charge on a task."""
        if not self.is_active:
            return False
        self.task_ids.append(task_id)
        self.boosts_used += 1
        if self.boost_type != "unlimited_24h":
            self.boosts_remaining -= 1
        if self.boosts_remaining <= 0 and self.boost_type != "unlimited_24h":
            self.status = "exhausted"
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "tenant_id": self.tenant_id,
            "boost_type": self.boost_type,
            "amount_cents": self.amount_cents,
            "amount_dollars": self.amount_cents / 100.0,
            "boosts_remaining": self.boosts_remaining,
            "boosts_used": self.boosts_used,
            "status": self.status,
            "is_active": self.is_active,
            "purchased_at": self.purchased_at.isoformat() if self.purchased_at else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }


@dataclass
class QueueSubscription:
    """A subscription to a priority queue tier."""

    id: str = field(default_factory=lambda: str(uuid4()))
    tenant_id: str = ""
    tier: QueueTier = QueueTier.STANDARD
    price_monthly_cents: int = 0
    status: str = "active"
    tasks_executed: int = 0
    avg_wait_seconds: float = 0.0
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    cancelled_at: Optional[datetime] = None

    def __post_init__(self):
        if self.price_monthly_cents == 0 and self.tier != QueueTier.STANDARD:
            self.price_monthly_cents = _TIER_CONFIG[self.tier]["price_monthly_cents"]

    @property
    def config(self) -> Dict[str, Any]:
        return _TIER_CONFIG.get(self.tier, _TIER_CONFIG[QueueTier.STANDARD])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "tenant_id": self.tenant_id,
            "tier": self.tier.value,
            "price_monthly_cents": self.price_monthly_cents,
            "price_monthly_dollars": self.price_monthly_cents / 100.0,
            "status": self.status,
            "tasks_executed": self.tasks_executed,
            "avg_wait_seconds": round(self.avg_wait_seconds, 2),
            "config": self.config,
            "started_at": self.started_at.isoformat() if self.started_at else None,
        }


@dataclass
class QueuePosition:
    """A task's position and priority in the execution queue."""

    task_id: str = ""
    tenant_id: str = ""
    base_priority: float = 1.0
    boost_multiplier: float = 1.0
    tier_multiplier: float = 1.0
    effective_priority: float = 1.0
    estimated_wait_seconds: float = 300.0
    position: int = 0
    boosted: bool = False

    def __post_init__(self):
        self.effective_priority = self.base_priority * self.boost_multiplier * self.tier_multiplier

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "base_priority": self.base_priority,
            "boost_multiplier": self.boost_multiplier,
            "tier_multiplier": self.tier_multiplier,
            "effective_priority": self.effective_priority,
            "estimated_wait_seconds": round(self.estimated_wait_seconds, 1),
            "position": self.position,
            "boosted": self.boosted,
        }


class PriorityQueueManager:
    """Manages priority queue subscriptions, boosts, and task ordering.

    Standard users execute tasks in FIFO order.  Subscribers and boost
    purchasers get elevated priority, reduced wait times, and higher
    concurrency.
    """

    def __init__(self):
        self._subscriptions: Dict[str, QueueSubscription] = {}
        self._boosts: Dict[str, PriorityBoost] = {}
        self._tenant_subs: Dict[str, str] = {}  # tenant_id -> subscription_id
        self._mrr_cents: int = 0
        self._boost_revenue_cents: int = 0

    @property
    def mrr_dollars(self) -> float:
        return self._mrr_cents / 100.0

    @property
    def total_revenue_cents(self) -> int:
        return self._mrr_cents + self._boost_revenue_cents

    def subscribe(
        self,
        tenant_id: str,
        tier: str = "priority",
    ) -> QueueSubscription:
        """Subscribe a tenant to a priority queue tier."""
        license_gate.gate(ADVANCED_FEATURE)

        tier_enum = QueueTier(tier)

        if tier_enum == QueueTier.DEDICATED:
            license_gate.gate(ENTERPRISE_FEATURE)

        # Check for existing active subscription
        if tenant_id in self._tenant_subs:
            existing = self._subscriptions.get(self._tenant_subs[tenant_id])
            if existing and existing.status == "active":
                raise ValueError("Already subscribed. Upgrade instead.")

        sub = QueueSubscription(
            tenant_id=tenant_id,
            tier=tier_enum,
        )
        self._subscriptions[sub.id] = sub
        self._tenant_subs[tenant_id] = sub.id
        self._mrr_cents += sub.price_monthly_cents

        return sub

    def upgrade_subscription(self, tenant_id: str, new_tier: str) -> QueueSubscription:
        """Upgrade a tenant's queue subscription."""
        license_gate.gate(ADVANCED_FEATURE)

        sub_id = self._tenant_subs.get(tenant_id)
        if not sub_id:
            raise ValueError("No active subscription to upgrade")

        sub = self._subscriptions.get(sub_id)
        if not sub or sub.status != "active":
            raise ValueError("No active subscription to upgrade")

        new_tier_enum = QueueTier(new_tier)
        if new_tier_enum == QueueTier.DEDICATED:
            license_gate.gate(ENTERPRISE_FEATURE)

        tier_order = list(QueueTier)
        if tier_order.index(new_tier_enum) <= tier_order.index(sub.tier):
            raise ValueError("Can only upgrade to a higher tier")

        old_price = sub.price_monthly_cents
        sub.tier = new_tier_enum
        sub.price_monthly_cents = _TIER_CONFIG[new_tier_enum]["price_monthly_cents"]
        self._mrr_cents += (sub.price_monthly_cents - old_price)

        return sub

    def cancel_subscription(self, tenant_id: str) -> Dict[str, Any]:
        """Cancel a tenant's queue subscription."""
        sub_id = self._tenant_subs.get(tenant_id)
        if not sub_id:
            raise ValueError("No active subscription")

        sub = self._subscriptions.get(sub_id)
        if not sub or sub.status != "active":
            raise ValueError("No active subscription")

        sub.status = "cancelled"
        sub.cancelled_at = datetime.now(timezone.utc)
        self._mrr_cents -= sub.price_monthly_cents
        del self._tenant_subs[tenant_id]

        return {"success": True, "tenant_id": tenant_id}

    def purchase_boost(
        self,
        tenant_id: str,
        boost_type: str = "single",
    ) -> PriorityBoost:
        """Purchase a one-time priority boost."""
        license_gate.gate(ADVANCED_FEATURE)

        if boost_type not in _BOOST_PRICING:
            raise ValueError(f"Invalid boost type: {boost_type}")

        boost = PriorityBoost(
            tenant_id=tenant_id,
            boost_type=boost_type,
        )

        self._boosts[boost.id] = boost
        self._boost_revenue_cents += boost.amount_cents

        return boost

    def apply_boost(self, boost_id: str, task_id: str) -> Dict[str, Any]:
        """Apply a boost to a specific task."""
        boost = self._boosts.get(boost_id)
        if not boost:
            raise ValueError(f"Boost not found: {boost_id}")

        if not boost.use_boost(task_id):
            raise ValueError("Boost is exhausted or expired")

        return {
            "success": True,
            "task_id": task_id,
            "boost_id": boost_id,
            "boosts_remaining": boost.boosts_remaining,
        }

    def get_queue_position(
        self,
        task_id: str,
        tenant_id: str,
        base_priority: float = 1.0,
        queue_depth: int = 10,
    ) -> QueuePosition:
        """Calculate a task's effective priority and estimated wait time."""
        # Get tier multiplier from subscription
        tier_mult = 1.0
        sub_id = self._tenant_subs.get(tenant_id)
        if sub_id:
            sub = self._subscriptions.get(sub_id)
            if sub and sub.status == "active":
                tier_mult = _TIER_CONFIG[sub.tier]["multiplier"]

        # Check for active boosts
        boost_mult = 1.0
        boosted = False
        for boost in self._boosts.values():
            if boost.tenant_id == tenant_id and boost.is_active:
                boost_mult = max(boost_mult, 3.0)
                boosted = True
                break

        effective = base_priority * tier_mult * boost_mult

        # Estimate wait based on queue depth and priority
        base_wait = queue_depth * 30  # 30 seconds per task ahead
        estimated_wait = base_wait / max(effective, 0.1)

        # Estimate position
        position = max(1, int(queue_depth / max(effective, 0.1)))

        return QueuePosition(
            task_id=task_id,
            tenant_id=tenant_id,
            base_priority=base_priority,
            boost_multiplier=boost_mult,
            tier_multiplier=tier_mult,
            effective_priority=effective,
            estimated_wait_seconds=estimated_wait,
            position=position,
            boosted=boosted,
        )

    def get_tenant_boosts(self, tenant_id: str) -> List[Dict[str, Any]]:
        """Get all boosts for a tenant."""
        boosts = [b for b in self._boosts.values() if b.tenant_id == tenant_id]
        return [b.to_dict() for b in boosts]

    def get_tenant_subscription(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        """Get the current subscription for a tenant."""
        sub_id = self._tenant_subs.get(tenant_id)
        if not sub_id:
            return None
        sub = self._subscriptions.get(sub_id)
        return sub.to_dict() if sub and sub.status == "active" else None

    def get_upsell_prompt(self, tenant_id: str, current_wait: float = 0.0) -> Dict[str, Any]:
        """Generate a priority queue upsell prompt based on wait time."""
        sub_id = self._tenant_subs.get(tenant_id)
        current_tier = QueueTier.STANDARD
        if sub_id:
            sub = self._subscriptions.get(sub_id)
            if sub and sub.status == "active":
                current_tier = sub.tier

        if current_tier == QueueTier.DEDICATED:
            return {"show_upsell": False}

        next_tier = self._next_tier(current_tier)
        next_config = _TIER_CONFIG[next_tier]
        current_config = _TIER_CONFIG[current_tier]

        speedup = next_config["multiplier"] / current_config["multiplier"]
        estimated_with_upgrade = current_wait / speedup

        return {
            "show_upsell": True,
            "current_tier": current_tier.value,
            "recommended_tier": next_tier.value,
            "current_wait_seconds": current_wait,
            "estimated_wait_with_upgrade": round(estimated_with_upgrade, 1),
            "speedup_factor": f"{speedup:.0f}x",
            "price_monthly_cents": next_config["price_monthly_cents"],
            "price_monthly_dollars": next_config["price_monthly_cents"] / 100.0,
            "max_concurrent": next_config["max_concurrent"],
            "sla_seconds": next_config["sla_seconds"],
            "boost_option": {
                "type": "single",
                "price_cents": _BOOST_PRICING["single"],
                "price_dollars": _BOOST_PRICING["single"] / 100.0,
                "description": "One-time priority boost for this task",
            },
            "cta_url": PRICING_URL,
        }

    def get_boost_options(self) -> List[Dict[str, Any]]:
        """Get available boost purchase options."""
        return [
            {
                "type": boost_type,
                "price_cents": price,
                "price_dollars": price / 100.0,
                "description": {
                    "single": "Priority boost for 1 task",
                    "batch_5": "Priority boost for 5 tasks",
                    "batch_20": "Priority boost for 20 tasks",
                    "unlimited_24h": "Unlimited priority boosts for 24 hours",
                }[boost_type],
            }
            for boost_type, price in _BOOST_PRICING.items()
        ]

    def get_revenue_report(self) -> Dict[str, Any]:
        active_subs = [s for s in self._subscriptions.values() if s.status == "active"]
        cancelled_subs = [s for s in self._subscriptions.values() if s.status == "cancelled"]
        active_boosts = [b for b in self._boosts.values() if b.is_active]
        return {
            "mrr_cents": self._mrr_cents,
            "mrr_dollars": self.mrr_dollars,
            "arr_dollars": self.mrr_dollars * 12,
            "boost_revenue_cents": self._boost_revenue_cents,
            "boost_revenue_dollars": self._boost_revenue_cents / 100.0,
            "total_revenue_cents": self.total_revenue_cents,
            "active_subscriptions": len(active_subs),
            "cancelled_subscriptions": len(cancelled_subs),
            "active_boosts": len(active_boosts),
            "total_boosts_sold": len(self._boosts),
            "by_tier": {
                tier.value: len([s for s in self._subscriptions.values() if s.tier == tier and s.status == "active"])
                for tier in QueueTier
            },
        }

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_subscriptions": len(self._subscriptions),
            "active_subscriptions": len([s for s in self._subscriptions.values() if s.status == "active"]),
            "total_boosts": len(self._boosts),
            "mrr_dollars": self.mrr_dollars,
            "boost_revenue_dollars": self._boost_revenue_cents / 100.0,
        }

    def _next_tier(self, current: QueueTier) -> QueueTier:
        order = list(QueueTier)
        idx = order.index(current)
        return order[min(idx + 1, len(order) - 1)]
