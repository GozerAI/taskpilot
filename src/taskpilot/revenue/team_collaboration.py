"""Team collaboration upgrade path.

Manages team plans, member seats, role-based access, and collaborative
workflow features.  Solo users are prompted to upgrade when they attempt
team-only actions.

Backlog item: #303
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


class TeamRole(Enum):
    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"
    VIEWER = "viewer"


class CollaborationFeature(Enum):
    SHARED_WORKFLOWS = "shared_workflows"
    TASK_ASSIGNMENT = "task_assignment"
    TEAM_DASHBOARD = "team_dashboard"
    ACTIVITY_FEED = "activity_feed"
    COMMENTS = "comments"
    APPROVAL_CHAINS = "approval_chains"
    AUDIT_LOG = "audit_log"
    SSO = "sso"
    CUSTOM_ROLES = "custom_roles"
    API_ACCESS = "api_access"


class TeamPlan(Enum):
    SOLO = "solo"
    TEAM = "team"
    BUSINESS = "business"
    ENTERPRISE = "enterprise"


_PLAN_CONFIG = {
    TeamPlan.SOLO: {
        "max_members": 1,
        "price_per_seat_cents": 0,
        "features": set(),
        "workflows_limit": 3,
    },
    TeamPlan.TEAM: {
        "max_members": 10,
        "price_per_seat_cents": 1500,
        "features": {
            CollaborationFeature.SHARED_WORKFLOWS,
            CollaborationFeature.TASK_ASSIGNMENT,
            CollaborationFeature.TEAM_DASHBOARD,
            CollaborationFeature.ACTIVITY_FEED,
            CollaborationFeature.COMMENTS,
        },
        "workflows_limit": 25,
    },
    TeamPlan.BUSINESS: {
        "max_members": 50,
        "price_per_seat_cents": 2900,
        "features": {
            CollaborationFeature.SHARED_WORKFLOWS,
            CollaborationFeature.TASK_ASSIGNMENT,
            CollaborationFeature.TEAM_DASHBOARD,
            CollaborationFeature.ACTIVITY_FEED,
            CollaborationFeature.COMMENTS,
            CollaborationFeature.APPROVAL_CHAINS,
            CollaborationFeature.AUDIT_LOG,
            CollaborationFeature.API_ACCESS,
        },
        "workflows_limit": 100,
    },
    TeamPlan.ENTERPRISE: {
        "max_members": 99999,
        "price_per_seat_cents": 4900,
        "features": set(CollaborationFeature),
        "workflows_limit": 99999,
    },
}


@dataclass
class TeamMember:
    """A member of a team."""

    id: str = field(default_factory=lambda: str(uuid4()))
    user_id: str = ""
    email: str = ""
    name: str = ""
    role: TeamRole = TeamRole.MEMBER
    active: bool = True
    invited_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    joined_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "user_id": self.user_id,
            "email": self.email,
            "name": self.name,
            "role": self.role.value,
            "active": self.active,
            "invited_at": self.invited_at.isoformat() if self.invited_at else None,
            "joined_at": self.joined_at.isoformat() if self.joined_at else None,
        }


@dataclass
class Team:
    """A team entity."""

    id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    owner_id: str = ""
    plan: TeamPlan = TeamPlan.SOLO
    members: List[TeamMember] = field(default_factory=list)
    shared_workflow_ids: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def active_members(self) -> List[TeamMember]:
        return [m for m in self.members if m.active]

    @property
    def seat_count(self) -> int:
        return len(self.active_members)

    @property
    def mrr_cents(self) -> int:
        config = _PLAN_CONFIG[self.plan]
        return self.seat_count * config["price_per_seat_cents"]

    def to_dict(self) -> Dict[str, Any]:
        config = _PLAN_CONFIG[self.plan]
        return {
            "id": self.id,
            "name": self.name,
            "owner_id": self.owner_id,
            "plan": self.plan.value,
            "seat_count": self.seat_count,
            "max_members": config["max_members"],
            "mrr_cents": self.mrr_cents,
            "mrr_dollars": self.mrr_cents / 100.0,
            "shared_workflows": len(self.shared_workflow_ids),
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class TeamCollaborationManager:
    """Manages team creation, membership, and collaboration feature gating.

    Solo plan users are prompted to upgrade when they attempt team actions.
    Team/Business/Enterprise plans unlock progressively more collaboration
    features.
    """

    def __init__(self):
        self._teams: Dict[str, Team] = {}
        self._upgrade_prompts: List[Dict[str, Any]] = []

    @property
    def total_teams(self) -> int:
        return len(self._teams)

    @property
    def total_mrr_cents(self) -> int:
        return sum(t.mrr_cents for t in self._teams.values())

    def create_team(
        self,
        name: str,
        owner_id: str,
        owner_email: str = "",
        owner_name: str = "",
        plan: str = "solo",
    ) -> Team:
        """Create a new team."""
        plan_enum = TeamPlan(plan)

        if plan_enum != TeamPlan.SOLO:
            license_gate.gate(ADVANCED_FEATURE)

        if plan_enum == TeamPlan.ENTERPRISE:
            license_gate.gate(ENTERPRISE_FEATURE)

        owner_member = TeamMember(
            user_id=owner_id,
            email=owner_email,
            name=owner_name,
            role=TeamRole.OWNER,
            joined_at=datetime.now(timezone.utc),
        )

        team = Team(
            name=name,
            owner_id=owner_id,
            plan=plan_enum,
            members=[owner_member],
        )

        self._teams[team.id] = team
        return team

    def get_team(self, team_id: str) -> Optional[Team]:
        return self._teams.get(team_id)

    def add_member(
        self,
        team_id: str,
        user_id: str,
        email: str = "",
        name: str = "",
        role: str = "member",
    ) -> TeamMember:
        """Add a member to a team. Triggers upgrade prompt on solo plans."""
        team = self._teams.get(team_id)
        if not team:
            raise ValueError(f"Team not found: {team_id}")

        config = _PLAN_CONFIG[team.plan]

        # Solo plan cannot add members — trigger upsell
        if team.plan == TeamPlan.SOLO:
            prompt = {
                "team_id": team_id,
                "action": "add_member",
                "current_plan": team.plan.value,
                "recommended_plan": "team",
                "headline": "Upgrade to Team Plan",
                "reason": "Adding team members requires a Team plan or higher.",
                "price_per_seat_cents": _PLAN_CONFIG[TeamPlan.TEAM]["price_per_seat_cents"],
                "cta_url": PRICING_URL,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self._upgrade_prompts.append(prompt)
            raise PermissionError(
                "Team collaboration requires a Team plan or higher. "
                f"Upgrade at {PRICING_URL}"
            )

        license_gate.gate(ADVANCED_FEATURE)

        if team.seat_count >= config["max_members"]:
            raise ValueError(
                f"Team is at capacity ({config['max_members']} members). "
                "Upgrade plan for more seats."
            )

        # Check for duplicate
        for m in team.members:
            if m.user_id == user_id and m.active:
                raise ValueError("User already a member of this team")

        role_enum = TeamRole(role)
        member = TeamMember(
            user_id=user_id,
            email=email,
            name=name,
            role=role_enum,
            joined_at=datetime.now(timezone.utc),
        )
        team.members.append(member)
        return member

    def remove_member(self, team_id: str, user_id: str) -> Dict[str, Any]:
        """Remove a member from a team."""
        team = self._teams.get(team_id)
        if not team:
            raise ValueError(f"Team not found: {team_id}")

        for m in team.members:
            if m.user_id == user_id and m.active:
                if m.role == TeamRole.OWNER:
                    raise ValueError("Cannot remove the team owner")
                m.active = False
                return {"success": True, "user_id": user_id, "team_id": team_id}

        raise ValueError("Member not found")

    def check_feature(self, team_id: str, feature: CollaborationFeature) -> bool:
        """Check if a collaboration feature is available on the team's plan."""
        team = self._teams.get(team_id)
        if not team:
            return False
        config = _PLAN_CONFIG[team.plan]
        return feature in config["features"]

    def require_feature(
        self, team_id: str, feature: CollaborationFeature
    ) -> None:
        """Gate a collaboration feature. Raises PermissionError with upsell."""
        if not self.check_feature(team_id, feature):
            team = self._teams.get(team_id)
            current = team.plan.value if team else "solo"
            raise PermissionError(
                f"{feature.value} requires a higher plan (current: {current}). "
                f"Upgrade at {PRICING_URL}"
            )

    def upgrade_plan(self, team_id: str, new_plan: str) -> Team:
        """Upgrade a team to a higher plan."""
        license_gate.gate(ADVANCED_FEATURE)

        team = self._teams.get(team_id)
        if not team:
            raise ValueError(f"Team not found: {team_id}")

        new_plan_enum = TeamPlan(new_plan)
        if new_plan_enum == TeamPlan.ENTERPRISE:
            license_gate.gate(ENTERPRISE_FEATURE)

        plan_order = list(TeamPlan)
        if plan_order.index(new_plan_enum) <= plan_order.index(team.plan):
            raise ValueError("Can only upgrade to a higher plan")

        team.plan = new_plan_enum
        return team

    def share_workflow(self, team_id: str, workflow_id: str) -> Dict[str, Any]:
        """Share a workflow with the team."""
        team = self._teams.get(team_id)
        if not team:
            raise ValueError(f"Team not found: {team_id}")

        self.require_feature(team_id, CollaborationFeature.SHARED_WORKFLOWS)

        config = _PLAN_CONFIG[team.plan]
        if len(team.shared_workflow_ids) >= config["workflows_limit"]:
            raise ValueError(
                f"Shared workflow limit reached ({config['workflows_limit']}). "
                "Upgrade plan for more."
            )

        if workflow_id not in team.shared_workflow_ids:
            team.shared_workflow_ids.append(workflow_id)

        return {
            "success": True,
            "team_id": team_id,
            "workflow_id": workflow_id,
            "total_shared": len(team.shared_workflow_ids),
        }

    def get_upgrade_prompts(self) -> List[Dict[str, Any]]:
        """Get all recorded upgrade prompts (for analytics)."""
        return list(self._upgrade_prompts)

    def get_plan_comparison(self) -> List[Dict[str, Any]]:
        """Get a comparison of all plans for upsell display."""
        comparison = []
        for plan in TeamPlan:
            config = _PLAN_CONFIG[plan]
            comparison.append({
                "plan": plan.value,
                "max_members": config["max_members"],
                "price_per_seat_cents": config["price_per_seat_cents"],
                "price_per_seat_dollars": config["price_per_seat_cents"] / 100.0,
                "features": [f.value for f in config["features"]],
                "workflows_limit": config["workflows_limit"],
            })
        return comparison

    def get_revenue_report(self) -> Dict[str, Any]:
        total_seats = sum(t.seat_count for t in self._teams.values())
        paying_teams = [t for t in self._teams.values() if t.plan != TeamPlan.SOLO]
        return {
            "total_teams": self.total_teams,
            "paying_teams": len(paying_teams),
            "total_seats": total_seats,
            "mrr_cents": self.total_mrr_cents,
            "mrr_dollars": self.total_mrr_cents / 100.0,
            "arr_dollars": self.total_mrr_cents / 100.0 * 12,
            "by_plan": {
                plan.value: len([t for t in self._teams.values() if t.plan == plan])
                for plan in TeamPlan
            },
            "upgrade_prompts_total": len(self._upgrade_prompts),
        }

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_teams": self.total_teams,
            "total_members": sum(t.seat_count for t in self._teams.values()),
            "mrr_dollars": self.total_mrr_cents / 100.0,
            "by_plan": {
                plan.value: len([t for t in self._teams.values() if t.plan == plan])
                for plan in TeamPlan
            },
        }
