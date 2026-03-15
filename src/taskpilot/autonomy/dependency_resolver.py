"""Task dependency resolution with topological sorting.

Builds a DAG of task dependencies and produces an execution plan
with parallel tiers. Detects cycles and missing dependencies.
"""

import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class CycleError(Exception):
    """Raised when a dependency cycle is detected."""
    def __init__(self, cycle: List[str]):
        self.cycle = cycle
        super().__init__(f"Dependency cycle detected: {' -> '.join(cycle)}")


@dataclass
class DependencyNode:
    """A node in the dependency graph."""
    task_id: str
    depends_on: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "depends_on": self.depends_on,
            "metadata": self.metadata,
        }


@dataclass
class ExecutionPlan:
    """Ordered execution plan with parallel tiers."""
    tiers: List[List[str]] = field(default_factory=list)
    total_tasks: int = 0
    max_parallelism: int = 0
    missing_deps: List[str] = field(default_factory=list)

    @property
    def tier_count(self) -> int:
        return len(self.tiers)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tiers": self.tiers,
            "tier_count": self.tier_count,
            "total_tasks": self.total_tasks,
            "max_parallelism": self.max_parallelism,
            "missing_deps": self.missing_deps,
        }


class DependencyResolver:
    """Resolves task dependencies into an execution plan using topological sort.

    Features:
      - Kahn's algorithm for topological ordering
      - Tier-based parallel execution groups
      - Cycle detection
      - Missing dependency reporting
    """

    def __init__(self):
        self._nodes: Dict[str, DependencyNode] = {}

    def add_task(self, task_id: str, depends_on: Optional[List[str]] = None,
                 metadata: Optional[Dict[str, Any]] = None) -> DependencyNode:
        """Register a task with its dependencies."""
        node = DependencyNode(
            task_id=task_id,
            depends_on=depends_on or [],
            metadata=metadata or {},
        )
        self._nodes[task_id] = node
        return node

    def add_tasks(self, tasks: List[Dict[str, Any]]) -> int:
        """Bulk-add tasks. Each dict: task_id, depends_on (opt), metadata (opt)."""
        count = 0
        for t in tasks:
            self.add_task(
                task_id=t["task_id"],
                depends_on=t.get("depends_on"),
                metadata=t.get("metadata"),
            )
            count += 1
        return count

    def remove_task(self, task_id: str) -> bool:
        return self._nodes.pop(task_id, None) is not None

    @property
    def task_count(self) -> int:
        return len(self._nodes)

    def get_dependents(self, task_id: str) -> List[str]:
        """Return tasks that depend on the given task."""
        return [
            nid for nid, node in self._nodes.items()
            if task_id in node.depends_on
        ]

    def _detect_cycle(self) -> Optional[List[str]]:
        """DFS-based cycle detection. Returns cycle path or None."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[str, int] = {nid: WHITE for nid in self._nodes}
        parent: Dict[str, Optional[str]] = {nid: None for nid in self._nodes}

        def dfs(node_id: str) -> Optional[List[str]]:
            color[node_id] = GRAY
            for dep in self._nodes[node_id].depends_on:
                if dep not in self._nodes:
                    continue
                if color[dep] == GRAY:
                    # Found cycle — reconstruct
                    cycle = [dep, node_id]
                    curr = node_id
                    while parent.get(curr) and parent[curr] != dep:
                        curr = parent[curr]
                        cycle.append(curr)
                    cycle.append(dep)
                    cycle.reverse()
                    return cycle
                if color[dep] == WHITE:
                    parent[dep] = node_id
                    result = dfs(dep)
                    if result:
                        return result
            color[node_id] = BLACK
            return None

        for nid in self._nodes:
            if color[nid] == WHITE:
                result = dfs(nid)
                if result:
                    return result
        return None

    def resolve(self, detect_cycles: bool = True) -> ExecutionPlan:
        """Resolve dependencies into a tiered execution plan.

        Raises CycleError if detect_cycles=True and a cycle exists.
        """
        if detect_cycles:
            cycle = self._detect_cycle()
            if cycle:
                raise CycleError(cycle)

        all_ids = set(self._nodes.keys())
        missing: List[str] = []

        # Build in-degree map
        in_degree: Dict[str, int] = {nid: 0 for nid in all_ids}
        adj: Dict[str, List[str]] = {nid: [] for nid in all_ids}

        for nid, node in self._nodes.items():
            for dep in node.depends_on:
                if dep not in all_ids:
                    missing.append(dep)
                    continue
                adj[dep].append(nid)
                in_degree[nid] += 1

        # Kahn's algorithm with tier tracking
        tiers: List[List[str]] = []
        queue = deque([nid for nid, deg in in_degree.items() if deg == 0])

        while queue:
            tier: List[str] = []
            next_queue: deque = deque()
            while queue:
                nid = queue.popleft()
                tier.append(nid)
                for dependent in adj[nid]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        next_queue.append(dependent)
            if tier:
                tiers.append(sorted(tier))
            queue = next_queue

        scheduled = sum(len(t) for t in tiers)
        max_par = max((len(t) for t in tiers), default=0)

        return ExecutionPlan(
            tiers=tiers,
            total_tasks=scheduled,
            max_parallelism=max_par,
            missing_deps=sorted(set(missing)),
        )

    def clear(self) -> None:
        self._nodes.clear()
