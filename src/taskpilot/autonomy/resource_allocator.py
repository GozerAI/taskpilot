"""Resource allocation for task workloads.

Allocates limited resources (workers, memory, CPU slots) to tasks using
a best-fit strategy that maximizes utilization while respecting capacity.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class Resource:
    """A pooled resource with capacity."""
    resource_id: str
    name: str
    total_capacity: float
    used_capacity: float = 0.0
    resource_type: str = "generic"

    @property
    def available(self) -> float:
        return max(0.0, self.total_capacity - self.used_capacity)

    @property
    def utilization_pct(self) -> float:
        if self.total_capacity <= 0:
            return 0.0
        return (self.used_capacity / self.total_capacity) * 100

    def can_allocate(self, amount: float) -> bool:
        return amount <= self.available

    def allocate(self, amount: float) -> bool:
        if not self.can_allocate(amount):
            return False
        self.used_capacity += amount
        return True

    def release(self, amount: float) -> None:
        self.used_capacity = max(0.0, self.used_capacity - amount)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "resource_id": self.resource_id,
            "name": self.name,
            "total_capacity": self.total_capacity,
            "used_capacity": round(self.used_capacity, 2),
            "available": round(self.available, 2),
            "utilization_pct": round(self.utilization_pct, 1),
            "resource_type": self.resource_type,
        }


@dataclass
class AllocationRequest:
    """A request to allocate resources for a task."""
    task_id: str
    requirements: Dict[str, float] = field(default_factory=dict)
    priority: int = 5

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "requirements": self.requirements,
            "priority": self.priority,
        }


@dataclass
class AllocationResult:
    """Result of an allocation attempt."""
    task_id: str
    allocated: bool
    assignments: Dict[str, float] = field(default_factory=dict)
    insufficient: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "allocated": self.allocated,
            "assignments": self.assignments,
            "insufficient": self.insufficient,
        }


class ResourceAllocator:
    """Best-fit resource allocator for task workloads.

    Manages a pool of resources and allocates them to tasks based on
    requirements. Supports allocation, release, and utilization tracking.
    """

    def __init__(self):
        self._resources: Dict[str, Resource] = {}
        self._allocations: Dict[str, Dict[str, float]] = {}

    def add_resource(self, resource: Resource) -> None:
        self._resources[resource.resource_id] = resource

    def remove_resource(self, resource_id: str) -> bool:
        return self._resources.pop(resource_id, None) is not None

    @property
    def resources(self) -> List[Resource]:
        return list(self._resources.values())

    def get_resource(self, resource_id: str) -> Optional[Resource]:
        return self._resources.get(resource_id)

    def allocate(self, request: AllocationRequest) -> AllocationResult:
        """Attempt to allocate all required resources for a task."""
        insufficient: List[str] = []
        assignments: Dict[str, float] = {}

        for res_id, amount in request.requirements.items():
            resource = self._resources.get(res_id)
            if resource is None:
                insufficient.append(f"{res_id} (not found)")
                continue
            if not resource.can_allocate(amount):
                insufficient.append(
                    f"{res_id} (need {amount}, have {resource.available:.2f})"
                )
                continue
            assignments[res_id] = amount

        if insufficient:
            return AllocationResult(
                task_id=request.task_id,
                allocated=False,
                insufficient=insufficient,
            )

        # Commit allocations
        for res_id, amount in assignments.items():
            self._resources[res_id].allocate(amount)

        self._allocations[request.task_id] = assignments

        return AllocationResult(
            task_id=request.task_id,
            allocated=True,
            assignments=assignments,
        )

    def release(self, task_id: str) -> bool:
        """Release all resources allocated to a task."""
        assignments = self._allocations.pop(task_id, None)
        if assignments is None:
            return False
        for res_id, amount in assignments.items():
            resource = self._resources.get(res_id)
            if resource:
                resource.release(amount)
        return True

    def batch_allocate(self, requests: List[AllocationRequest]) -> List[AllocationResult]:
        """Allocate resources for multiple tasks, ordered by priority."""
        sorted_reqs = sorted(requests, key=lambda r: r.priority, reverse=True)
        return [self.allocate(req) for req in sorted_reqs]

    def utilization_summary(self) -> Dict[str, Any]:
        """Return overall utilization metrics."""
        if not self._resources:
            return {"total_resources": 0, "avg_utilization_pct": 0.0}
        utils = [r.utilization_pct for r in self._resources.values()]
        return {
            "total_resources": len(self._resources),
            "active_allocations": len(self._allocations),
            "avg_utilization_pct": round(sum(utils) / len(utils), 1),
            "resources": {r.resource_id: r.to_dict() for r in self._resources.values()},
        }
