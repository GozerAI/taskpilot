"""Tests for team collaboration upgrade path."""

import pytest

from taskpilot.revenue.team_collaboration import (
    TeamCollaborationManager,
    TeamPlan,
    TeamMember,
    TeamRole,
    CollaborationFeature,
    Team,
    _PLAN_CONFIG,
)


@pytest.fixture
def manager():
    return TeamCollaborationManager()


@pytest.fixture
def solo_team(manager):
    return manager.create_team("Solo Team", "owner-1", "o@test.com", "Owner")


@pytest.fixture
def team_plan_team(manager):
    return manager.create_team("Team Plan", "owner-1", "o@test.com", "Owner", plan="team")


class TestTeamCreation:
    def test_create_solo_team(self, manager):
        team = manager.create_team("My Team", "owner-1")
        assert team.plan == TeamPlan.SOLO
        assert team.seat_count == 1

    def test_create_team_plan(self, manager):
        team = manager.create_team("My Team", "owner-1", plan="team")
        assert team.plan == TeamPlan.TEAM

    def test_owner_auto_added(self, manager):
        team = manager.create_team("T", "o1", "o@t.com", "Owner")
        assert team.members[0].role == TeamRole.OWNER
        assert team.members[0].user_id == "o1"

    def test_get_team(self, manager, solo_team):
        found = manager.get_team(solo_team.id)
        assert found is solo_team


class TestMemberManagement:
    def test_add_member_to_team_plan(self, manager, team_plan_team):
        member = manager.add_member(
            team_plan_team.id, "user-2", "u2@test.com", "User 2"
        )
        assert member.role == TeamRole.MEMBER
        assert team_plan_team.seat_count == 2

    def test_solo_plan_add_member_raises(self, manager, solo_team):
        with pytest.raises(PermissionError, match="Team plan"):
            manager.add_member(solo_team.id, "user-2")

    def test_solo_plan_records_upgrade_prompt(self, manager, solo_team):
        try:
            manager.add_member(solo_team.id, "user-2")
        except PermissionError:
            pass
        prompts = manager.get_upgrade_prompts()
        assert len(prompts) == 1
        assert prompts[0]["recommended_plan"] == "team"

    def test_capacity_limit(self, manager):
        team = manager.create_team("T", "o1", plan="team")  # max 10
        for i in range(9):
            manager.add_member(team.id, f"user-{i}")
        with pytest.raises(ValueError, match="capacity"):
            manager.add_member(team.id, "user-extra")

    def test_duplicate_member_raises(self, manager, team_plan_team):
        manager.add_member(team_plan_team.id, "user-2")
        with pytest.raises(ValueError, match="already a member"):
            manager.add_member(team_plan_team.id, "user-2")

    def test_remove_member(self, manager, team_plan_team):
        manager.add_member(team_plan_team.id, "user-2")
        result = manager.remove_member(team_plan_team.id, "user-2")
        assert result["success"] is True
        assert team_plan_team.seat_count == 1

    def test_remove_owner_raises(self, manager, team_plan_team):
        with pytest.raises(ValueError, match="Cannot remove the team owner"):
            manager.remove_member(team_plan_team.id, "owner-1")

    def test_nonexistent_team_raises(self, manager):
        with pytest.raises(ValueError, match="not found"):
            manager.add_member("bad-id", "u1")

    def test_add_member_with_role(self, manager, team_plan_team):
        member = manager.add_member(team_plan_team.id, "u2", role="admin")
        assert member.role == TeamRole.ADMIN


class TestFeatureGating:
    def test_feature_available_on_plan(self, manager, team_plan_team):
        assert manager.check_feature(
            team_plan_team.id, CollaborationFeature.SHARED_WORKFLOWS
        )

    def test_feature_not_available_on_solo(self, manager, solo_team):
        assert not manager.check_feature(
            solo_team.id, CollaborationFeature.SHARED_WORKFLOWS
        )

    def test_require_feature_raises(self, manager, solo_team):
        with pytest.raises(PermissionError, match="higher plan"):
            manager.require_feature(
                solo_team.id, CollaborationFeature.SHARED_WORKFLOWS
            )

    def test_approval_chains_requires_business(self, manager, team_plan_team):
        assert not manager.check_feature(
            team_plan_team.id, CollaborationFeature.APPROVAL_CHAINS
        )


class TestPlanUpgrade:
    def test_upgrade_plan(self, manager, solo_team):
        upgraded = manager.upgrade_plan(solo_team.id, "team")
        assert upgraded.plan == TeamPlan.TEAM

    def test_upgrade_to_business(self, manager, team_plan_team):
        upgraded = manager.upgrade_plan(team_plan_team.id, "business")
        assert upgraded.plan == TeamPlan.BUSINESS

    def test_downgrade_raises(self, manager, team_plan_team):
        with pytest.raises(ValueError, match="higher plan"):
            manager.upgrade_plan(team_plan_team.id, "solo")

    def test_nonexistent_team_upgrade_raises(self, manager):
        with pytest.raises(ValueError, match="not found"):
            manager.upgrade_plan("bad-id", "team")


class TestSharedWorkflows:
    def test_share_workflow(self, manager, team_plan_team):
        result = manager.share_workflow(team_plan_team.id, "wf-1")
        assert result["success"] is True
        assert result["total_shared"] == 1

    def test_share_workflow_solo_raises(self, manager, solo_team):
        with pytest.raises(PermissionError):
            manager.share_workflow(solo_team.id, "wf-1")

    def test_workflow_limit_enforced(self, manager):
        team = manager.create_team("T", "o1", plan="team")  # limit 25
        for i in range(25):
            manager.share_workflow(team.id, f"wf-{i}")
        with pytest.raises(ValueError, match="limit reached"):
            manager.share_workflow(team.id, "wf-overflow")

    def test_duplicate_workflow_no_error(self, manager, team_plan_team):
        manager.share_workflow(team_plan_team.id, "wf-1")
        result = manager.share_workflow(team_plan_team.id, "wf-1")
        assert result["total_shared"] == 1


class TestRevenueAndStats:
    def test_mrr_tracking(self, manager, team_plan_team):
        assert manager.total_mrr_cents > 0

    def test_mrr_increases_with_members(self, manager, team_plan_team):
        mrr_before = manager.total_mrr_cents
        manager.add_member(team_plan_team.id, "u2")
        assert manager.total_mrr_cents > mrr_before

    def test_plan_comparison(self, manager):
        comparison = manager.get_plan_comparison()
        assert len(comparison) == 4
        assert comparison[0]["plan"] == "solo"
        assert comparison[1]["plan"] == "team"

    def test_revenue_report(self, manager, team_plan_team):
        report = manager.get_revenue_report()
        assert "mrr_cents" in report
        assert "paying_teams" in report
        assert "by_plan" in report
        assert report["paying_teams"] == 1

    def test_stats(self, manager, team_plan_team):
        stats = manager.get_stats()
        assert stats["total_teams"] == 1
        assert "mrr_dollars" in stats
