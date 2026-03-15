"""Schedule optimization for task workloads.

Assigns tasks to time slots while respecting constraints: deadlines,
dependencies, resource limits, and working-hours windows.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class TimeSlot:
    """A scheduled time window for a task."""
    task_id: str
    start: datetime
    end: datetime
    resource_id: Optional[str] = None

    @property
    def duration_minutes(self) -> float:
        return (self.end - self.start).total_seconds() / 60.0

    def overlaps(self, other: "TimeSlot") -> bool:
        return self.start < other.end and other.start < self.end

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "duration_minutes": round(self.duration_minutes, 1),
            "resource_id": self.resource_id,
        }


@dataclass
class ScheduleConflict:
    """A detected scheduling conflict."""
    task_a: str
    task_b: str
    conflict_type: str
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_a": self.task_a,
            "task_b": self.task_b,
            "conflict_type": self.conflict_type,
            "message": self.message,
        }


@dataclass
class SchedulePlan:
    """Complete schedule with assignments and conflicts."""
    slots: List[TimeSlot] = field(default_factory=list)
    conflicts: List[ScheduleConflict] = field(default_factory=list)
    unscheduled: List[str] = field(default_factory=list)

    @property
    def scheduled_count(self) -> int:
        return len(self.slots)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scheduled_count": self.scheduled_count,
            "conflict_count": len(self.conflicts),
            "unscheduled_count": len(self.unscheduled),
            "slots": [s.to_dict() for s in self.slots],
            "conflicts": [c.to_dict() for c in self.conflicts],
            "unscheduled": self.unscheduled,
        }


class ScheduleOptimizer:
    """Greedy earliest-deadline-first scheduler.

    Assigns tasks to the earliest available slot that respects:
      - task duration
      - working hours (configurable start/end hour)
      - deadlines
      - dependency ordering
    """

    def __init__(self, work_start_hour: int = 9, work_end_hour: int = 17,
                 slot_granularity_minutes: int = 15):
        self._work_start = work_start_hour
        self._work_end = work_end_hour
        self._granularity = slot_granularity_minutes

    def _next_work_start(self, dt: datetime) -> datetime:
        """Advance dt to the next working-hours start if outside window."""
        if dt.hour >= self._work_end:
            dt = dt.replace(hour=self._work_start, minute=0, second=0, microsecond=0)
            dt += timedelta(days=1)
        elif dt.hour < self._work_start:
            dt = dt.replace(hour=self._work_start, minute=0, second=0, microsecond=0)
        return dt

    def _fits_in_day(self, start: datetime, duration_min: float) -> bool:
        end = start + timedelta(minutes=duration_min)
        return end.hour <= self._work_end or (end.hour == self._work_end and end.minute == 0)

    def optimize(self, tasks: List[Dict[str, Any]],
                 start_from: Optional[datetime] = None) -> SchedulePlan:
        """Schedule tasks using earliest-deadline-first.

        Each task dict should have:
          - task_id: str
          - duration_minutes: float
          - deadline: datetime (optional)
          - depends_on: list of task_id (optional)
          - priority: int (optional, tiebreaker)
        """
        if start_from is None:
            start_from = datetime.now(timezone.utc)

        # Sort by deadline (earliest first), then priority (highest first)
        def sort_key(t: Dict[str, Any]) -> Tuple:
            dl = t.get("deadline")
            if dl is None:
                dl = datetime.max.replace(tzinfo=timezone.utc)
            elif dl.tzinfo is None:
                dl = dl.replace(tzinfo=timezone.utc)
            return (dl, -(t.get("priority", 5)))

        sorted_tasks = sorted(tasks, key=sort_key)

        plan = SchedulePlan()
        scheduled_ends: Dict[str, datetime] = {}
        cursor = self._next_work_start(start_from)

        for task in sorted_tasks:
            tid = task["task_id"]
            duration = task.get("duration_minutes", 30)

            # Respect dependencies
            dep_end = cursor
            for dep_id in task.get("depends_on", []):
                if dep_id in scheduled_ends:
                    dep_end = max(dep_end, scheduled_ends[dep_id])

            slot_start = self._next_work_start(max(cursor, dep_end))

            # Find a slot that fits within working hours
            attempts = 0
            while not self._fits_in_day(slot_start, duration) and attempts < 30:
                slot_start = self._next_work_start(
                    slot_start + timedelta(days=1)
                )
                slot_start = slot_start.replace(
                    hour=self._work_start, minute=0, second=0, microsecond=0
                )
                attempts += 1

            if attempts >= 30:
                plan.unscheduled.append(tid)
                continue

            slot_end = slot_start + timedelta(minutes=duration)

            # Check deadline
            deadline = task.get("deadline")
            if deadline is not None:
                if deadline.tzinfo is None:
                    deadline = deadline.replace(tzinfo=timezone.utc)
                if slot_end > deadline:
                    plan.conflicts.append(ScheduleConflict(
                        task_a=tid, task_b="",
                        conflict_type="deadline_miss",
                        message=f"Task {tid} cannot complete before deadline",
                    ))

            slot = TimeSlot(task_id=tid, start=slot_start, end=slot_end)
            plan.slots.append(slot)
            scheduled_ends[tid] = slot_end
            cursor = slot_end

        return plan
