"""
Scheduler Core - Data models and task structures.

Provides foundational data structures for task scheduling
and autonomous execution.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)


class ScheduleFrequency(Enum):
    """Task scheduling frequencies."""
    ONCE = "once"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CUSTOM = "custom"  # Uses cron expression
    ON_DEMAND = "on_demand"  # Manual execution only


class TaskStatus(Enum):
    """Status of a scheduled task."""
    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class TaskPriority(Enum):
    """Task execution priority."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class TaskCategory(Enum):
    """Categories of scheduled tasks."""
    TREND_ANALYSIS = "trend_analysis"
    COMMERCE = "commerce"
    PRICING = "pricing"
    INVENTORY = "inventory"
    BRAND = "brand"
    REPORTING = "reporting"
    MAINTENANCE = "maintenance"
    OPERATIONS = "operations"
    ANALYTICS = "analytics"
    INTEGRATION = "integration"
    CUSTOM = "custom"


@dataclass
class ScheduleConfig:
    """Configuration for task scheduling."""
    frequency: ScheduleFrequency = ScheduleFrequency.DAILY
    time_of_day: Optional[str] = None  # HH:MM format
    day_of_week: Optional[int] = None  # 0-6, Monday=0
    day_of_month: Optional[int] = None  # 1-31
    cron_expression: Optional[str] = None  # For custom schedules
    timezone: str = "UTC"
    hour: int = 0  # Hour to run (0-23)
    minute: int = 0  # Minute to run (0-59)

    # Execution window
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    max_executions: Optional[int] = None

    def __post_init__(self):
        """Set time_of_day from hour/minute if not specified."""
        if self.time_of_day is None and (self.hour != 0 or self.minute != 0):
            self.time_of_day = f"{self.hour:02d}:{self.minute:02d}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "frequency": self.frequency.value,
            "time_of_day": self.time_of_day,
            "day_of_week": self.day_of_week,
            "day_of_month": self.day_of_month,
            "cron_expression": self.cron_expression,
            "timezone": self.timezone,
            "hour": self.hour,
            "minute": self.minute,
        }

    def get_next_run(self, after: Optional[datetime] = None) -> datetime:
        """Calculate next run time."""
        now = after or datetime.now(timezone.utc)

        if self.frequency == ScheduleFrequency.ONCE:
            return self.start_date or now

        elif self.frequency == ScheduleFrequency.HOURLY:
            next_run = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            return next_run

        elif self.frequency == ScheduleFrequency.DAILY:
            if self.time_of_day:
                hour, minute = map(int, self.time_of_day.split(":"))
                next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if next_run <= now:
                    next_run += timedelta(days=1)
                return next_run
            else:
                return now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

        elif self.frequency == ScheduleFrequency.WEEKLY:
            days_ahead = (self.day_of_week or 0) - now.weekday()
            if days_ahead <= 0:
                days_ahead += 7
            next_run = now + timedelta(days=days_ahead)
            if self.time_of_day:
                hour, minute = map(int, self.time_of_day.split(":"))
                next_run = next_run.replace(hour=hour, minute=minute, second=0, microsecond=0)
            return next_run

        elif self.frequency == ScheduleFrequency.MONTHLY:
            next_run = now.replace(day=self.day_of_month or 1)
            if next_run <= now:
                if now.month == 12:
                    next_run = next_run.replace(year=now.year + 1, month=1)
                else:
                    next_run = next_run.replace(month=now.month + 1)
            return next_run

        elif self.frequency == ScheduleFrequency.CUSTOM and self.cron_expression:
            return self._parse_cron_next(now)

        return now + timedelta(hours=1)

    def _parse_cron_next(self, after: datetime) -> datetime:
        """Parse a cron expression and find the next matching time.

        Supports standard 5-field cron: minute hour day_of_month month day_of_week.
        Fields can be: *, a number, or */N for step values.
        """
        parts = self.cron_expression.split()
        if len(parts) != 5:
            return after + timedelta(hours=1)  # Fallback for invalid expressions

        def matches(field: str, value: int, max_val: int) -> bool:
            if field == "*":
                return True
            if field.startswith("*/"):
                step = int(field[2:])
                return value % step == 0
            if "," in field:
                return value in [int(v) for v in field.split(",")]
            if "-" in field:
                low, high = field.split("-")
                return int(low) <= value <= int(high)
            return value == int(field)

        cron_min, cron_hour, cron_dom, cron_month, cron_dow = parts
        candidate = after.replace(second=0, microsecond=0) + timedelta(minutes=1)

        # Search up to 7 days ahead
        max_attempts = 7 * 24 * 60
        for _ in range(max_attempts):
            if (matches(cron_min, candidate.minute, 59) and
                matches(cron_hour, candidate.hour, 23) and
                matches(cron_dom, candidate.day, 31) and
                matches(cron_month, candidate.month, 12) and
                matches(cron_dow, candidate.weekday(), 6)):
                return candidate
            candidate += timedelta(minutes=1)

        return after + timedelta(hours=1)  # Fallback if no match found


@dataclass
class ExecutionResult:
    """Result of a task execution."""
    id: str = field(default_factory=lambda: str(uuid4()))
    success: bool = True
    output: Any = None
    error: Optional[str] = None
    duration_seconds: float = 0.0
    metrics: Dict[str, Any] = field(default_factory=dict)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "success": self.success,
            "output": self.output if isinstance(self.output, (dict, list, str, int, float, bool, type(None))) else str(self.output),
            "error": self.error,
            "duration_seconds": self.duration_seconds,
            "metrics": self.metrics,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


@dataclass
class TaskExecution:
    """Record of a task execution."""
    id: str = field(default_factory=lambda: str(uuid4()))
    task_id: str = ""
    execution_number: int = 1
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[ExecutionResult] = None
    scheduled_for: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "task_id": self.task_id,
            "execution_number": self.execution_number,
            "status": self.status.value,
            "result": self.result.to_dict() if self.result else None,
            "scheduled_for": self.scheduled_for.isoformat() if self.scheduled_for else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "retry_count": self.retry_count,
        }


@dataclass
class ScheduledTask:
    """Represents a scheduled task."""
    id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    description: str = ""
    category: TaskCategory = TaskCategory.CUSTOM

    # Ownership
    owner_executive: str = "COO"  # Executive code
    created_by: str = ""

    # Scheduling
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.SCHEDULED

    # Execution
    handler_name: str = ""  # Name of function/method to call
    handler_params: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: int = 300  # 5 minutes default
    max_retries: int = 3
    retry_delay_seconds: int = 60

    # Tracking
    execution_count: int = 0
    last_execution: Optional[TaskExecution] = None
    next_run: Optional[datetime] = None
    executions: List[TaskExecution] = field(default_factory=list)

    # Notifications
    notify_on_failure: bool = True
    notify_on_success: bool = False
    notification_targets: List[str] = field(default_factory=list)

    # State
    enabled: bool = True

    # Metadata
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "category": self.category.value,
            "owner_executive": self.owner_executive,
            "schedule": self.schedule.to_dict(),
            "priority": self.priority.value,
            "status": self.status.value,
            "handler_name": self.handler_name,
            "execution_count": self.execution_count,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "last_execution": self.last_execution.to_dict() if self.last_execution else None,
            "enabled": self.enabled,
            "tags": self.tags,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    def calculate_next_run(self) -> None:
        """Calculate and set the next run time."""
        after = self.last_execution.completed_at if self.last_execution else None
        self.next_run = self.schedule.get_next_run(after)

    def should_run(self, now: Optional[datetime] = None) -> bool:
        """Check if task should run now."""
        if not self.enabled:
            return False

        if self.status not in [TaskStatus.SCHEDULED, TaskStatus.PENDING]:
            return False

        now = now or datetime.now(timezone.utc)

        if self.next_run and now >= self.next_run:
            return True

        return False

    def record_execution(self, execution: TaskExecution) -> None:
        """Record an execution."""
        self.execution_count += 1
        self.last_execution = execution
        self.executions.append(execution)

        # Calculate next run
        if self.schedule.frequency != ScheduleFrequency.ONCE:
            self.calculate_next_run()
        else:
            self.status = TaskStatus.COMPLETED

        self.updated_at = datetime.now(timezone.utc)

    def get_success_rate(self) -> float:
        """Calculate success rate of executions."""
        if not self.executions:
            return 0.0

        successful = len([e for e in self.executions if e.result and e.result.success])
        return (successful / len(self.executions)) * 100


# Pre-defined task templates
TASK_TEMPLATES = {
    "daily_trend_scan": {
        "name": "Daily Trend Scan",
        "description": "Collect and analyze trends from all sources",
        "category": TaskCategory.TREND_ANALYSIS,
        "owner_executive": "CMO",
        "handler_name": "trend_service.run_autonomous_analysis",
        "schedule": {
            "frequency": ScheduleFrequency.DAILY,
            "time_of_day": "08:00",
        },
    },
    "hourly_pricing_check": {
        "name": "Hourly Pricing Check",
        "description": "Monitor pricing and generate recommendations",
        "category": TaskCategory.PRICING,
        "owner_executive": "CRO",
        "handler_name": "commerce_service.run_autonomous_analysis",
        "schedule": {
            "frequency": ScheduleFrequency.HOURLY,
        },
    },
    "daily_inventory_alert": {
        "name": "Daily Inventory Alert",
        "description": "Check inventory levels and generate alerts",
        "category": TaskCategory.INVENTORY,
        "owner_executive": "COO",
        "handler_name": "commerce_service.get_inventory_alerts",
        "schedule": {
            "frequency": ScheduleFrequency.DAILY,
            "time_of_day": "07:00",
        },
    },
    "weekly_brand_audit": {
        "name": "Weekly Brand Audit",
        "description": "Audit brand consistency across content",
        "category": TaskCategory.BRAND,
        "owner_executive": "CMO",
        "handler_name": "brand_service.run_autonomous_analysis",
        "schedule": {
            "frequency": ScheduleFrequency.WEEKLY,
            "day_of_week": 0,  # Monday
            "time_of_day": "09:00",
        },
    },
    "daily_executive_report": {
        "name": "Daily Executive Report",
        "description": "Generate and distribute daily executive reports",
        "category": TaskCategory.REPORTING,
        "owner_executive": "CEO",
        "handler_name": "generate_executive_reports",
        "schedule": {
            "frequency": ScheduleFrequency.DAILY,
            "time_of_day": "06:00",
        },
    },
}
