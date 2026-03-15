"""Tests for priority queue upsell."""

import pytest
from datetime import datetime, timezone, timedelta

from taskpilot.revenue.priority_queue import (
    PriorityQueueManager,
    QueueTier,
    PriorityBoost,
    QueueSubscription,
    QueuePosition,
    _TIER_CONFIG,
    _BOOST_PRICING,
)


@pytest.fixture
def manager():
    return PriorityQueueManager()


class TestQueueSubscription:
    def test_subscribe(self, manager):
        sub = manager.subscribe("tenant-1", "priority")
        assert sub.tier == QueueTier.PRIORITY
        assert sub.status == "active"

    def test_subscribe_auto_pricing(self, manager):
        sub = manager.subscribe("tenant-1", "priority")
        assert sub.price_monthly_cents == _TIER_CONFIG[QueueTier.PRIORITY]["price_monthly_cents"]

    def test_subscribe_tracks_mrr(self, manager):
        manager.subscribe("tenant-1", "priority")
        assert manager.mrr_dollars > 0

    def test_duplicate_subscription_raises(self, manager):
        manager.subscribe("tenant-1", "priority")
        with pytest.raises(ValueError, match="Already subscribed"):
            manager.subscribe("tenant-1", "express")

    def test_upgrade_subscription(self, manager):
        manager.subscribe("tenant-1", "priority")
        sub = manager.upgrade_subscription("tenant-1", "express")
        assert sub.tier == QueueTier.EXPRESS

    def test_upgrade_increases_mrr(self, manager):
        manager.subscribe("tenant-1", "priority")
        mrr_before = manager.mrr_dollars
        manager.upgrade_subscription("tenant-1", "express")
        assert manager.mrr_dollars > mrr_before

    def test_downgrade_raises(self, manager):
        manager.subscribe("tenant-1", "express")
        with pytest.raises(ValueError, match="higher tier"):
            manager.upgrade_subscription("tenant-1", "priority")

    def test_cancel_subscription(self, manager):
        manager.subscribe("tenant-1", "priority")
        result = manager.cancel_subscription("tenant-1")
        assert result["success"] is True
        assert manager.mrr_dollars == 0

    def test_cancel_no_subscription_raises(self, manager):
        with pytest.raises(ValueError, match="No active"):
            manager.cancel_subscription("tenant-1")

    def test_get_tenant_subscription(self, manager):
        manager.subscribe("tenant-1", "priority")
        sub = manager.get_tenant_subscription("tenant-1")
        assert sub is not None
        assert sub["tier"] == "priority"

    def test_no_subscription_returns_none(self, manager):
        assert manager.get_tenant_subscription("tenant-1") is None

    def test_upgrade_no_sub_raises(self, manager):
        with pytest.raises(ValueError, match="No active"):
            manager.upgrade_subscription("tenant-1", "express")


class TestPriorityBoost:
    def test_purchase_single_boost(self, manager):
        boost = manager.purchase_boost("tenant-1", "single")
        assert boost.boost_type == "single"
        assert boost.boosts_remaining == 1
        assert boost.amount_cents == _BOOST_PRICING["single"]

    def test_purchase_batch_boost(self, manager):
        boost = manager.purchase_boost("tenant-1", "batch_5")
        assert boost.boosts_remaining == 5

    def test_purchase_unlimited_boost(self, manager):
        boost = manager.purchase_boost("tenant-1", "unlimited_24h")
        assert boost.boosts_remaining == 99999
        assert boost.expires_at is not None

    def test_invalid_boost_type_raises(self, manager):
        with pytest.raises(ValueError, match="Invalid boost type"):
            manager.purchase_boost("tenant-1", "invalid")

    def test_apply_boost(self, manager):
        boost = manager.purchase_boost("tenant-1", "single")
        result = manager.apply_boost(boost.id, "task-1")
        assert result["success"] is True
        assert result["boosts_remaining"] == 0

    def test_exhausted_boost_raises(self, manager):
        boost = manager.purchase_boost("tenant-1", "single")
        manager.apply_boost(boost.id, "task-1")
        with pytest.raises(ValueError, match="exhausted"):
            manager.apply_boost(boost.id, "task-2")

    def test_batch_boost_multiple_uses(self, manager):
        boost = manager.purchase_boost("tenant-1", "batch_5")
        for i in range(5):
            manager.apply_boost(boost.id, f"task-{i}")
        with pytest.raises(ValueError, match="exhausted"):
            manager.apply_boost(boost.id, "task-extra")

    def test_boost_revenue_tracked(self, manager):
        manager.purchase_boost("tenant-1", "single")
        manager.purchase_boost("tenant-1", "batch_5")
        report = manager.get_revenue_report()
        expected = _BOOST_PRICING["single"] + _BOOST_PRICING["batch_5"]
        assert report["boost_revenue_cents"] == expected

    def test_get_tenant_boosts(self, manager):
        manager.purchase_boost("tenant-1", "single")
        manager.purchase_boost("tenant-1", "batch_5")
        boosts = manager.get_tenant_boosts("tenant-1")
        assert len(boosts) == 2

    def test_nonexistent_boost_raises(self, manager):
        with pytest.raises(ValueError, match="not found"):
            manager.apply_boost("bad-id", "task-1")


class TestQueuePosition:
    def test_standard_priority(self, manager):
        pos = manager.get_queue_position("task-1", "tenant-1", base_priority=1.0)
        assert pos.tier_multiplier == 1.0
        assert pos.boost_multiplier == 1.0
        assert pos.effective_priority == 1.0

    def test_subscriber_gets_multiplier(self, manager):
        manager.subscribe("tenant-1", "priority")
        pos = manager.get_queue_position("task-1", "tenant-1")
        assert pos.tier_multiplier == _TIER_CONFIG[QueueTier.PRIORITY]["multiplier"]
        assert pos.effective_priority > 1.0

    def test_boosted_task(self, manager):
        boost = manager.purchase_boost("tenant-1", "single")
        # Boost is active but not yet applied — just having it increases priority
        pos = manager.get_queue_position("task-1", "tenant-1")
        assert pos.boosted is True
        assert pos.boost_multiplier > 1.0

    def test_combined_boost_and_subscription(self, manager):
        manager.subscribe("tenant-1", "express")
        manager.purchase_boost("tenant-1", "single")
        pos = manager.get_queue_position("task-1", "tenant-1")
        assert pos.tier_multiplier == _TIER_CONFIG[QueueTier.EXPRESS]["multiplier"]
        assert pos.boost_multiplier > 1.0
        assert pos.effective_priority > 5.0

    def test_estimated_wait_decreases(self, manager):
        pos_standard = manager.get_queue_position("t1", "no-sub", queue_depth=20)
        manager.subscribe("tenant-1", "express")
        pos_express = manager.get_queue_position("t1", "tenant-1", queue_depth=20)
        assert pos_express.estimated_wait_seconds < pos_standard.estimated_wait_seconds

    def test_position_to_dict(self):
        pos = QueuePosition(task_id="t1", base_priority=1.0)
        d = pos.to_dict()
        assert "effective_priority" in d
        assert "estimated_wait_seconds" in d


class TestUpsellPrompt:
    def test_standard_user_sees_upsell(self, manager):
        prompt = manager.get_upsell_prompt("tenant-1", current_wait=120.0)
        assert prompt["show_upsell"] is True
        assert prompt["recommended_tier"] == "priority"
        assert prompt["estimated_wait_with_upgrade"] < 120.0

    def test_dedicated_no_upsell(self, manager):
        manager.subscribe("tenant-1", "dedicated")
        prompt = manager.get_upsell_prompt("tenant-1")
        assert prompt["show_upsell"] is False

    def test_prompt_includes_boost_option(self, manager):
        prompt = manager.get_upsell_prompt("tenant-1", current_wait=60.0)
        assert "boost_option" in prompt
        assert prompt["boost_option"]["price_cents"] == _BOOST_PRICING["single"]

    def test_speedup_factor(self, manager):
        prompt = manager.get_upsell_prompt("tenant-1", current_wait=100.0)
        assert "speedup_factor" in prompt


class TestBoostOptions:
    def test_get_boost_options(self, manager):
        options = manager.get_boost_options()
        assert len(options) == 4
        types = [o["type"] for o in options]
        assert "single" in types
        assert "unlimited_24h" in types


class TestRevenueAndStats:
    def test_revenue_report(self, manager):
        manager.subscribe("t1", "priority")
        manager.purchase_boost("t1", "single")
        report = manager.get_revenue_report()
        assert report["mrr_cents"] > 0
        assert report["boost_revenue_cents"] > 0
        assert report["total_revenue_cents"] > 0
        assert "by_tier" in report

    def test_stats(self, manager):
        manager.subscribe("t1", "priority")
        stats = manager.get_stats()
        assert stats["active_subscriptions"] == 1
        assert "mrr_dollars" in stats

    def test_total_revenue(self, manager):
        manager.subscribe("t1", "priority")
        manager.purchase_boost("t2", "batch_20")
        assert manager.total_revenue_cents > 0
