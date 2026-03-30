"""
Scheduler Engine - Task execution and queue management.

Provides the core scheduling engine that manages task queues,
executes tasks, and handles retries.
"""

import asyncio
import inspect
import logging
import traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import uuid4

from taskpilot.core import (
    ScheduledTask,
    TaskExecution,
    ExecutionResult,
    TaskStatus,
    TaskPriority,
    TaskCategory,
    ScheduleConfig,
    ScheduleFrequency,
)

logger = logging.getLogger(__name__)


class TaskQueue:
    """Priority queue for scheduled tasks."""

    def __init__(self):
        self._tasks: Dict[str, ScheduledTask] = {}
        self._running: Set[str] = set()

    def add(self, task: ScheduledTask) -> None:
        """Add a task to the queue."""
        self._tasks[task.id] = task
        logger.info(f"Added task to queue: {task.name} ({task.id})")

    def remove(self, task_id: str) -> Optional[ScheduledTask]:
        """Remove a task from the queue."""
        task = self._tasks.pop(task_id, None)
        self._running.discard(task_id)
        if task:
            logger.info(f"Removed task from queue: {task.name}")
        return task

    def get(self, task_id: str) -> Optional[ScheduledTask]:
        """Get a task by ID."""
        return self._tasks.get(task_id)

    def get_due_tasks(self, now: Optional[datetime] = None) -> List[ScheduledTask]:
        """Get tasks that are due to run."""
        now = now or datetime.now(timezone.utc)
        due_tasks = []

        for task in self._tasks.values():
            if task.id not in self._running and task.should_run(now):
                due_tasks.append(task)

        # Sort by priority and next run time
        priority_order = {
            TaskPriority.CRITICAL: 0,
            TaskPriority.HIGH: 1,
            TaskPriority.NORMAL: 2,
            TaskPriority.LOW: 3,
        }
        due_tasks.sort(key=lambda t: (priority_order[t.priority], t.next_run or now))

        return due_tasks

    def mark_running(self, task_id: str) -> None:
        """Mark a task as running."""
        self._running.add(task_id)

    def mark_completed(self, task_id: str) -> None:
        """Mark a task as completed."""
        self._running.discard(task_id)

    def list_all(self) -> List[ScheduledTask]:
        """List all tasks."""
        return list(self._tasks.values())

    def list_by_category(self, category: TaskCategory) -> List[ScheduledTask]:
        """List tasks by category."""
        return [t for t in self._tasks.values() if t.category == category]

    def list_by_owner(self, executive_code: str) -> List[ScheduledTask]:
        """List tasks by owner executive."""
        return [t for t in self._tasks.values() if t.owner_executive == executive_code]

    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        tasks = self.list_all()
        return {
            "total_tasks": len(tasks),
            "running_tasks": len(self._running),
            "by_status": {
                status.value: len([t for t in tasks if t.status == status])
                for status in TaskStatus
            },
            "by_category": {
                cat.value: len([t for t in tasks if t.category == cat])
                for cat in TaskCategory
            },
            "by_priority": {
                pri.value: len([t for t in tasks if t.priority == pri])
                for pri in TaskPriority
            },
        }


class SchedulerEngine:
    """
    Core scheduling engine for autonomous task execution.

    Manages task scheduling, execution, retries, and monitoring.
    """

    def __init__(self):
        self._queue = TaskQueue()
        self._handlers: Dict[str, Callable] = {}
        self._running = False
        self._loop_task: Optional[asyncio.Task] = None

        # Configuration
        self._tick_interval = 60  # Check every 60 seconds
        self._max_concurrent = 5  # Max concurrent task executions
        self._current_concurrent = 0

        # Metrics
        self._metrics = {
            "total_executions": 0,
            "successful_executions": 0,
            "failed_executions": 0,
            "total_retries": 0,
        }

        self._started_at: Optional[datetime] = None

    def register_handler(
        self,
        name: str,
        handler: Callable,
    ) -> None:
        """Register a task handler."""
        self._handlers[name] = handler
        logger.info(f"Registered handler: {name}")

    def unregister_handler(self, name: str) -> None:
        """Unregister a task handler."""
        self._handlers.pop(name, None)

    def schedule_task(self, task: ScheduledTask) -> str:
        """Schedule a task."""
        # Validate handler exists
        if task.handler_name not in self._handlers:
            logger.warning(f"Handler not found for task: {task.handler_name}")

        # Calculate initial next run
        task.calculate_next_run()
        task.status = TaskStatus.SCHEDULED

        self._queue.add(task)
        return task.id

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a scheduled task."""
        task = self._queue.get(task_id)
        if not task:
            return False

        task.status = TaskStatus.CANCELLED
        self._queue.remove(task_id)
        logger.info(f"Cancelled task: {task.name}")
        return True

    def pause_task(self, task_id: str) -> bool:
        """Pause a scheduled task."""
        task = self._queue.get(task_id)
        if not task:
            return False

        task.status = TaskStatus.PAUSED
        logger.info(f"Paused task: {task.name}")
        return True

    def resume_task(self, task_id: str) -> bool:
        """Resume a paused task."""
        task = self._queue.get(task_id)
        if not task or task.status != TaskStatus.PAUSED:
            return False

        task.status = TaskStatus.SCHEDULED
        task.calculate_next_run()
        logger.info(f"Resumed task: {task.name}")
        return True

    async def execute_task(self, task: ScheduledTask) -> ExecutionResult:
        """Execute a single task."""
        result = ExecutionResult()
        execution = TaskExecution(
            task_id=task.id,
            execution_number=task.execution_count + 1,
            status=TaskStatus.RUNNING,
            scheduled_for=task.next_run,
            started_at=datetime.now(timezone.utc),
        )

        self._queue.mark_running(task.id)
        task.status = TaskStatus.RUNNING
        self._current_concurrent += 1

        try:
            handler = self._handlers.get(task.handler_name)
            if not handler:
                raise ValueError(f"Handler not found: {task.handler_name}")

            # Execute with timeout
            if inspect.iscoroutinefunction(handler):
                output = await asyncio.wait_for(
                    handler(**task.handler_params),
                    timeout=task.timeout_seconds,
                )
            else:
                output = await asyncio.wait_for(
                    asyncio.to_thread(handler, **task.handler_params),
                    timeout=task.timeout_seconds,
                )

            result.success = True
            result.output = output
            execution.status = TaskStatus.COMPLETED
            self._metrics["successful_executions"] += 1

            logger.info(f"Task completed successfully: {task.name}")

        except asyncio.TimeoutError:
            result.success = False
            result.error = f"Task timed out after {task.timeout_seconds} seconds"
            execution.status = TaskStatus.FAILED
            self._metrics["failed_executions"] += 1
            logger.error(f"Task timed out: {task.name}")

        except Exception as e:
            result.success = False
            result.error = str(e)
            execution.status = TaskStatus.FAILED
            self._metrics["failed_executions"] += 1
            logger.error(f"Task failed: {task.name} - {e}")
            logger.debug(traceback.format_exc())

        finally:
            result.completed_at = datetime.now(timezone.utc)
            result.duration_seconds = (
                result.completed_at - result.started_at
            ).total_seconds()

            execution.result = result
            execution.completed_at = result.completed_at

            task.record_execution(execution)
            task.status = TaskStatus.SCHEDULED if task.schedule.frequency != ScheduleFrequency.ONCE else TaskStatus.COMPLETED

            self._queue.mark_completed(task.id)
            self._current_concurrent -= 1
            self._metrics["total_executions"] += 1

        # Handle retries
        if not result.success and execution.retry_count < task.max_retries:
            await self._schedule_retry(task, execution)

        return result

    async def _schedule_retry(
        self,
        task: ScheduledTask,
        execution: TaskExecution,
    ) -> None:
        """Schedule a task retry."""
        execution.retry_count += 1
        self._metrics["total_retries"] += 1

        retry_time = datetime.now(timezone.utc) + timedelta(seconds=task.retry_delay_seconds)
        task.next_run = retry_time
        task.status = TaskStatus.PENDING

        logger.info(f"Scheduled retry {execution.retry_count} for task: {task.name} at {retry_time}")

    async def run_due_tasks(self) -> List[ExecutionResult]:
        """Run all due tasks, respecting concurrency limits."""
        due_tasks = self._queue.get_due_tasks()
        results = []

        for task in due_tasks:
            if self._current_concurrent >= self._max_concurrent:
                skipped = len(due_tasks) - len(results)
                self._metrics["skipped_due_to_backpressure"] = (
                    self._metrics.get("skipped_due_to_backpressure", 0) + skipped
                )
                logger.warning(
                    f"Backpressure: {skipped} due tasks deferred "
                    f"({self._current_concurrent}/{self._max_concurrent} slots used)"
                )
                break

            result = await self.execute_task(task)
            results.append(result)

        return results

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop with exponential backoff on repeated failures."""
        logger.info("Scheduler loop started")

        consecutive_failures = 0
        max_backoff = 60.0  # Max 60 second backoff

        while self._running:
            try:
                await self.run_due_tasks()
                consecutive_failures = 0  # Reset on success
            except Exception as e:
                consecutive_failures += 1
                logger.error(f"Scheduler loop error (attempt {consecutive_failures}): {e}")

                # Exponential backoff on repeated failures
                if consecutive_failures >= 3:
                    backoff = min(self._tick_interval * (2 ** (consecutive_failures - 3)), max_backoff)
                    logger.warning(f"Scheduler backing off for {backoff:.1f}s after {consecutive_failures} failures")
                    await asyncio.sleep(backoff)
                    continue

            await asyncio.sleep(self._tick_interval)

    async def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            return

        self._running = True
        self._started_at = datetime.now(timezone.utc)
        self._loop_task = asyncio.create_task(self._scheduler_loop())
        logger.info("Scheduler started")

    async def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False

        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass

        logger.info("Scheduler stopped")

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get a task by ID."""
        return self._queue.get(task_id)

    def get_due_tasks(self) -> List[ScheduledTask]:
        """Get tasks that are due to run."""
        return self._queue.get_due_tasks()

    def list_tasks(
        self,
        category: Optional[TaskCategory] = None,
        owner: Optional[str] = None,
    ) -> List[ScheduledTask]:
        """List tasks with optional filters."""
        if category:
            return self._queue.list_by_category(category)
        elif owner:
            return self._queue.list_by_owner(owner)
        else:
            return self._queue.list_all()

    def get_metrics(self) -> Dict[str, Any]:
        """Get scheduler metrics."""
        uptime = None
        if self._started_at:
            uptime = (datetime.now(timezone.utc) - self._started_at).total_seconds()

        return {
            "running": self._running,
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "uptime_seconds": uptime,
            "current_concurrent": self._current_concurrent,
            "max_concurrent": self._max_concurrent,
            **self._metrics,
            "queue_stats": self._queue.get_stats(),
        }

    def get_upcoming_tasks(self, count: int = 10) -> List[Dict[str, Any]]:
        """Get upcoming scheduled tasks."""
        tasks = self._queue.list_all()
        scheduled = [t for t in tasks if t.next_run and t.status == TaskStatus.SCHEDULED]
        scheduled.sort(key=lambda t: t.next_run)

        return [
            {
                "id": t.id,
                "name": t.name,
                "next_run": t.next_run.isoformat() if t.next_run else None,
                "category": t.category.value,
                "owner": t.owner_executive,
            }
            for t in scheduled[:count]
        ]

    def get_recent_executions(self, count: int = 10) -> List[Dict[str, Any]]:
        """Get recent task executions."""
        all_executions = []
        for task in self._queue.list_all():
            for execution in task.executions:
                all_executions.append({
                    "task_id": task.id,
                    "task_name": task.name,
                    **execution.to_dict(),
                })

        all_executions.sort(
            key=lambda e: e.get("completed_at") or e.get("started_at") or "",
            reverse=True,
        )

        return all_executions[:count]
