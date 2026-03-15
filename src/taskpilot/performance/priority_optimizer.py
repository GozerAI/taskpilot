"""Task priority optimization using multi-factor scoring.

Scores tasks based on deadline urgency, dependency depth, resource cost,
and business priority to produce an optimal execution order.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ScoredTask:
    """A task with computed priority score."""
    task_id: str
    original_priority: int
    computed_score: float
    urgency_score: float = 0.0
    dependency_score: float = 0.0
    resource_score: float = 0.0
    business_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "original_priority": self.original_priority,
            "computed_score": round(self.computed_score, 4),
            "urgency_score": round(self.urgency_score, 4),
            "dependency_score": round(self.dependency_score, 4),
            "resource_score": round(self.resource_score, 4),
            "business_score": round(self.business_score, 4),
        }


@dataclass
class OptimizationResult:
    """Result of priority optimization."""
    ordered_tasks: List[ScoredTask]
    reordered_count: int
    optimization_time_ms: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_count": len(self.ordered_tasks),
            "reordered_count": self.reordered_count,
            "optimization_time_ms": round(self.optimization_time_ms, 2),
            "top_5": [t.to_dict() for t in self.ordered_tasks[:5]],
        }


class PriorityOptimizer:
    """Multi-factor task priority optimizer.

    Scoring weights (configurable):
      - urgency: how close the deadline is (0.35)
      - dependency: tasks blocking others score higher (0.20)
      - resource: lower resource cost preferred (0.15)
      - business: explicit business priority value (0.30)
    """

    DEFAULT_WEIGHTS = {
        "urgency": 0.35,
        "dependency": 0.20,
        "resource": 0.15,
        "business": 0.30,
    }

    def __init__(self, weights: Optional[Dict[str, float]] = None):
        self._weights = weights or dict(self.DEFAULT_WEIGHTS)

    def _compute_urgency(self, task: Dict[str, Any], now: float) -> float:
        """Score 0-1 based on deadline proximity. No deadline = 0.5."""
        deadline = task.get("deadline")
        if deadline is None:
            return 0.5
        if isinstance(deadline, (int, float)):
            remaining = deadline - now
        else:
            remaining = deadline.timestamp() - now
        if remaining <= 0:
            return 1.0
        # Normalize: within 1 hour = 1.0, beyond 7 days = 0.0
        hours = remaining / 3600.0
        if hours >= 168:
            return 0.0
        return max(0.0, 1.0 - (hours / 168.0))

    def _compute_dependency(self, task: Dict[str, Any],
                            dependents_map: Dict[str, int]) -> float:
        """Score 0-1 based on how many other tasks depend on this one."""
        task_id = task.get("task_id", task.get("id", ""))
        count = dependents_map.get(str(task_id), 0)
        if count == 0:
            return 0.0
        return min(1.0, count / 10.0)

    def _compute_resource(self, task: Dict[str, Any]) -> float:
        """Score 0-1 where lower resource cost is better (higher score)."""
        cost = task.get("resource_cost", 0.5)
        return max(0.0, 1.0 - min(1.0, cost))

    def _compute_business(self, task: Dict[str, Any]) -> float:
        """Score 0-1 from explicit priority (1=lowest, 10=highest)."""
        priority = task.get("priority", 5)
        return min(1.0, max(0.0, priority / 10.0))

    def _build_dependents_map(self, tasks: List[Dict[str, Any]]) -> Dict[str, int]:
        """Count how many tasks depend on each task."""
        counts: Dict[str, int] = {}
        for task in tasks:
            deps = task.get("depends_on", [])
            for dep_id in deps:
                dep_str = str(dep_id)
                counts[dep_str] = counts.get(dep_str, 0) + 1
        return counts

    def optimize(self, tasks: List[Dict[str, Any]]) -> OptimizationResult:
        """Score and reorder tasks by computed priority."""
        start = time.monotonic()
        now = time.time()
        dependents_map = self._build_dependents_map(tasks)

        scored: List[ScoredTask] = []
        for task in tasks:
            task_id = str(task.get("task_id", task.get("id", "")))
            orig_priority = task.get("priority", 5)

            urgency = self._compute_urgency(task, now)
            dependency = self._compute_dependency(task, dependents_map)
            resource = self._compute_resource(task)
            business = self._compute_business(task)

            total = (
                self._weights["urgency"] * urgency +
                self._weights["dependency"] * dependency +
                self._weights["resource"] * resource +
                self._weights["business"] * business
            )

            scored.append(ScoredTask(
                task_id=task_id,
                original_priority=orig_priority,
                computed_score=total,
                urgency_score=urgency,
                dependency_score=dependency,
                resource_score=resource,
                business_score=business,
            ))

        # Sort descending by computed score
        original_order = [s.task_id for s in scored]
        scored.sort(key=lambda s: s.computed_score, reverse=True)
        new_order = [s.task_id for s in scored]

        reordered = sum(1 for a, b in zip(original_order, new_order) if a != b)
        duration = (time.monotonic() - start) * 1000

        return OptimizationResult(
            ordered_tasks=scored,
            reordered_count=reordered,
            optimization_time_ms=duration,
        )
