"""Tests for the automation feature upsell engine."""

import pytest

from taskpilot.revenue.automation_upsell import (
    AutomationUpsellEngine,
    AutomationTier,
    AutomationPackage,
    AutomationUsage,
    AutomationOffer,
    _TIER_LIMITS,
    _TIER_PRICING_MONTHLY_CENTS,
)


@pytest.fixture
def engine():
    return AutomationUpsellEngine()


@pytest.fixture
def free_usage():
    return AutomationUsage(tenant_id="t1", tier=AutomationTier.FREE)


class TestAutomationUsage:
    def test_limits_by_tier(self):
        usage = AutomationUsage(tier=AutomationTier.FREE)
        assert usage.limits["workflows"] == 3

    def test_utilization(self):
        usage = AutomationUsage(tier=AutomationTier.FREE, workflows_created=2)
        util = usage.utilization("workflows")
        assert util == pytest.approx(66.67, abs=0.1)

    def test_utilization_at_limit(self):
        usage = AutomationUsage(tier=AutomationTier.FREE, workflows_created=3)
        assert usage.utilization("workflows") == 100.0

    def test_utilization_over_limit_capped(self):
        usage = AutomationUsage(tier=AutomationTier.FREE, workflows_created=5)
        assert usage.utilization("workflows") == 100.0


class TestAutomationPackage:
    def test_auto_pricing(self):
        pkg = AutomationPackage(tier=AutomationTier.PRO)
        assert pkg.price_monthly_cents == 7900

    def test_free_tier_zero(self):
        pkg = AutomationPackage(tier=AutomationTier.FREE)
        assert pkg.price_monthly_cents == 0

    def test_auto_features(self):
        pkg = AutomationPackage(tier=AutomationTier.PRO)
        assert "parallel_execution" in pkg.features

    def test_auto_limits(self):
        pkg = AutomationPackage(tier=AutomationTier.STARTER)
        assert pkg.limits["workflows"] == 15

    def test_to_dict(self):
        pkg = AutomationPackage(name="Test", tier=AutomationTier.STARTER)
        d = pkg.to_dict()
        assert d["name"] == "Test"
        assert "price_monthly_dollars" in d


class TestAutomationUpsellEngine:
    def test_enterprise_returns_empty(self, engine):
        usage = AutomationUsage(tier=AutomationTier.ENTERPRISE)
        assert engine.evaluate(usage) == []

    def test_soft_threshold(self, engine):
        usage = AutomationUsage(
            tenant_id="t1",
            tier=AutomationTier.FREE,
            workflows_created=3,  # 100% of 3
        )
        offers = engine.evaluate(usage)
        assert any(o.priority == 100.0 for o in offers)

    def test_hard_threshold(self, engine):
        usage = AutomationUsage(
            tenant_id="t1",
            tier=AutomationTier.STARTER,
            scheduled_tasks_created=23,  # 92% of 25
        )
        offers = engine.evaluate(usage)
        assert any(o.priority == 75.0 for o in offers)

    def test_approaching_threshold(self, engine):
        usage = AutomationUsage(
            tenant_id="t1",
            tier=AutomationTier.STARTER,
            triggers_created=11,  # 73% of 15
        )
        offers = engine.evaluate(usage)
        assert any(o.priority == 50.0 for o in offers)

    def test_feature_blocked_trigger(self, engine):
        usage = AutomationUsage(
            tenant_id="t1",
            tier=AutomationTier.FREE,
            feature_blocks_hit=2,
        )
        offers = engine.evaluate(usage)
        assert any(o.headline == "Unlock Advanced Automation" for o in offers)

    def test_offers_sorted_by_priority(self, engine):
        usage = AutomationUsage(
            tenant_id="t1",
            tier=AutomationTier.FREE,
            workflows_created=3,
            scheduled_tasks_created=5,
            feature_blocks_hit=1,
        )
        offers = engine.evaluate(usage)
        priorities = [o.priority for o in offers]
        assert priorities == sorted(priorities, reverse=True)

    def test_recommended_tier_is_next(self, engine, free_usage):
        free_usage.workflows_created = 3
        offers = engine.evaluate(free_usage)
        assert all(o.recommended_tier == AutomationTier.STARTER for o in offers)


class TestOfferManagement:
    def test_accept_offer(self, engine, free_usage):
        free_usage.workflows_created = 3
        offers = engine.evaluate(free_usage)
        result = engine.accept_offer(offers[0].id)
        assert result["success"] is True
        assert result["new_tier"] == "starter"

    def test_decline_offer(self, engine, free_usage):
        free_usage.workflows_created = 3
        offers = engine.evaluate(free_usage)
        result = engine.decline_offer(offers[0].id)
        assert result["success"] is True

    def test_accept_processed_raises(self, engine, free_usage):
        free_usage.workflows_created = 3
        offers = engine.evaluate(free_usage)
        engine.accept_offer(offers[0].id)
        with pytest.raises(ValueError, match="already processed"):
            engine.accept_offer(offers[0].id)

    def test_nonexistent_offer_raises(self, engine):
        with pytest.raises(ValueError, match="not found"):
            engine.accept_offer("bad-id")

    def test_conversion_rate(self, engine, free_usage):
        free_usage.workflows_created = 3
        offers = engine.evaluate(free_usage)
        engine.accept_offer(offers[0].id)
        assert engine.get_conversion_rate() > 0


class TestTierComparison:
    def test_tier_comparison(self, engine):
        comparison = engine.get_tier_comparison()
        assert len(comparison) == 4
        assert comparison[0]["tier"] == "free"
        assert comparison[0]["price_monthly_cents"] == 0

    def test_comparison_has_limits(self, engine):
        comparison = engine.get_tier_comparison()
        for tier_info in comparison:
            assert "limits" in tier_info
            assert "features" in tier_info


class TestStats:
    def test_stats_structure(self, engine, free_usage):
        free_usage.workflows_created = 3
        engine.evaluate(free_usage)
        stats = engine.get_stats()
        assert "total_offers" in stats
        assert "conversion_rate" in stats
        assert stats["total_offers"] > 0

    def test_offer_to_dict(self):
        offer = AutomationOffer(
            tenant_id="t1",
            headline="Test",
            monthly_increase_cents=2900,
        )
        d = offer.to_dict()
        assert d["monthly_increase_dollars"] == 29.0
