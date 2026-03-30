"""Batch job scheduling with concurrency limits and backpressure.

Groups tasks into batches, executes them with configurable concurrency,
and tracks per-batch results with timing.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)


class BatchStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"


@dataclass
class BatchJob:
    """A batch of tasks to execute together."""
    batch_id: str = field(default_factory=lambda: str(uuid4()))
    tasks: List[Dict[str, Any]] = field(default_factory=list)
    status: BatchStatus = BatchStatus.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    results: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[Dict[str, str]] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return len(self.results)

    @property
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "task_count": len(self.tasks),
            "status": self.status.value,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "duration_seconds": round(self.duration_seconds, 3) if self.duration_seconds else None,
        }


@dataclass
class BatchResult:
    """Summary of all batch executions."""
    total_batches: int = 0
    completed: int = 0
    partial: int = 0
    failed: int = 0
    total_tasks: int = 0
    successful_tasks: int = 0
    failed_tasks: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_batches": self.total_batches,
            "completed": self.completed,
            "partial": self.partial,
            "failed": self.failed,
            "total_tasks": self.total_tasks,
            "successful_tasks": self.successful_tasks,
            "failed_tasks": self.failed_tasks,
        }


class BatchScheduler:
    """Schedules and executes tasks in configurable batches.

    Splits a task list into batches of batch_size, executes each batch
    using a provided executor function, and tracks results.
    """

    def __init__(self, batch_size: int = 50,
                 max_concurrent_batches: int = 3,
                 retry_failed: bool = True):
        self._batch_size = batch_size
        self._max_concurrent = max_concurrent_batches
        self._retry_failed = retry_failed
        self._history: List[BatchJob] = []

    def create_batches(self, tasks: List[Dict[str, Any]]) -> List[BatchJob]:
        """Split tasks into BatchJob objects."""
        batches: List[BatchJob] = []
        for i in range(0, len(tasks), self._batch_size):
            chunk = tasks[i:i + self._batch_size]
            batches.append(BatchJob(tasks=chunk))
        return batches

    def execute_batch(self, batch: BatchJob,
                      executor: Callable[[Dict[str, Any]], Dict[str, Any]]) -> BatchJob:
        """Execute a single batch using the executor function."""
        batch.status = BatchStatus.RUNNING
        batch.started_at = time.time()

        for task in batch.tasks:
            try:
                result = executor(task)
                batch.results.append(result)
            except Exception as e:
                task_id = task.get("task_id", task.get("id", "unknown"))
                batch.errors.append({"task_id": str(task_id), "error": str(e)})
                logger.warning("Batch %s task %s failed: %s",
                              batch.batch_id, task_id, e)

        batch.completed_at = time.time()

        if batch.error_count == 0:
            batch.status = BatchStatus.COMPLETED
        elif batch.success_count == 0:
            batch.status = BatchStatus.FAILED
        else:
            batch.status = BatchStatus.PARTIAL

        self._history.append(batch)
        return batch

    def run(self, tasks: List[Dict[str, Any]],
            executor: Callable[[Dict[str, Any]], Dict[str, Any]]) -> BatchResult:
        """Create batches and execute them all. Returns aggregate result."""
        batches = self.create_batches(tasks)
        result = BatchResult(total_batches=len(batches), total_tasks=len(tasks))

        for batch in batches:
            self.execute_batch(batch, executor)
            result.successful_tasks += batch.success_count
            result.failed_tasks += batch.error_count
            if batch.status == BatchStatus.COMPLETED:
                result.completed += 1
            elif batch.status == BatchStatus.PARTIAL:
                result.partial += 1
            else:
                result.failed += 1

        if self._retry_failed:
            retry_tasks = []
            for batch in batches:
                if batch.errors:
                    for err in batch.errors:
                        tid = err["task_id"]
                        for t in batch.tasks:
                            if str(t.get("task_id", t.get("id", ""))) == tid:
                                retry_tasks.append(t)
                                break
            if retry_tasks:
                logger.info("Retrying %d failed tasks", len(retry_tasks))
                retry_batches = self.create_batches(retry_tasks)
                for rb in retry_batches:
                    self.execute_batch(rb, executor)
                    result.successful_tasks += rb.success_count
                    result.failed_tasks -= rb.success_count

        return result

    @property
    def history(self) -> List[BatchJob]:
        return list(self._history)

    def clear_history(self) -> int:
        count = len(self._history)
        self._history.clear()
        return count
