"""
Scheduler Persistence - SQLite-based storage for scheduled tasks.

Provides durable storage for scheduled tasks, workflows, and execution history.
"""

import asyncio
import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from taskpilot.core import (
    ExecutionResult,
    ScheduleConfig,
    ScheduleFrequency,
    ScheduledTask,
    TaskCategory,
    TaskExecution,
    TaskPriority,
    TaskStatus,
)
from taskpilot.workflows import (
    StepStatus,
    WorkflowDefinition,
    WorkflowExecution,
    WorkflowStatus,
    WorkflowStep,
)

logger = logging.getLogger(__name__)


class SchedulerPersistence:
    """
    SQLite-based persistence for the scheduler module.

    Provides durable storage for:
    - Scheduled tasks
    - Task execution history
    - Workflow definitions
    - Workflow execution history

    Example:
        ```python
        persistence = SchedulerPersistence("scheduler.db")
        await persistence.initialize()

        # Save a task
        await persistence.save_task(task)

        # Load all tasks
        tasks = await persistence.load_tasks()
        ```
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize persistence layer.

        Args:
            db_path: Path to SQLite database file. Defaults to in-memory.
        """
        if db_path:
            self._db_path = Path(db_path)
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._connection_string = str(self._db_path)
        else:
            self._connection_string = ":memory:"

        self._initialized = False
        self._lock = asyncio.Lock()

    @contextmanager
    def _get_connection(self):
        """Get a database connection."""
        conn = sqlite3.connect(self._connection_string)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    async def initialize(self) -> None:
        """Initialize the database schema."""
        async with self._lock:
            if self._initialized:
                return

            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Scheduled Tasks table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS scheduled_tasks (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        description TEXT,
                        handler_name TEXT NOT NULL,
                        handler_params TEXT,
                        schedule_frequency TEXT NOT NULL,
                        schedule_cron TEXT,
                        schedule_hour INTEGER DEFAULT 0,
                        schedule_minute INTEGER DEFAULT 0,
                        schedule_day_of_week INTEGER,
                        priority TEXT DEFAULT 'normal',
                        category TEXT DEFAULT 'operations',
                        owner_executive TEXT DEFAULT 'COO',
                        enabled INTEGER DEFAULT 1,
                        status TEXT DEFAULT 'active',
                        run_count INTEGER DEFAULT 0,
                        last_run TEXT,
                        next_run TEXT,
                        tags TEXT,
                        metadata TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                """)

                # Task Executions table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS task_executions (
                        id TEXT PRIMARY KEY,
                        task_id TEXT NOT NULL,
                        started_at TEXT NOT NULL,
                        completed_at TEXT,
                        duration_seconds REAL,
                        success INTEGER DEFAULT 0,
                        output TEXT,
                        error TEXT,
                        metrics TEXT,
                        FOREIGN KEY (task_id) REFERENCES scheduled_tasks(id)
                    )
                """)

                # Workflow Definitions table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS workflow_definitions (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        description TEXT,
                        version TEXT DEFAULT '1.0',
                        owner_executive TEXT DEFAULT 'COO',
                        parallel_execution INTEGER DEFAULT 0,
                        stop_on_failure INTEGER DEFAULT 1,
                        steps TEXT NOT NULL,
                        tags TEXT,
                        metadata TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                """)

                # Workflow Executions table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS workflow_executions (
                        id TEXT PRIMARY KEY,
                        workflow_id TEXT NOT NULL,
                        workflow_name TEXT,
                        status TEXT DEFAULT 'pending',
                        total_steps INTEGER DEFAULT 0,
                        completed_steps INTEGER DEFAULT 0,
                        failed_steps INTEGER DEFAULT 0,
                        started_at TEXT,
                        completed_at TEXT,
                        step_results TEXT,
                        final_output TEXT,
                        error TEXT,
                        metadata TEXT,
                        FOREIGN KEY (workflow_id) REFERENCES workflow_definitions(id)
                    )
                """)

                # Create indexes
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_tasks_owner
                    ON scheduled_tasks(owner_executive)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_tasks_status
                    ON scheduled_tasks(status)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_executions_task
                    ON task_executions(task_id)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_workflow_executions_workflow
                    ON workflow_executions(workflow_id)
                """)

                conn.commit()
                self._initialized = True
                logger.info("Scheduler persistence initialized")

    async def close(self) -> None:
        """Close the persistence layer."""
        self._initialized = False

    # =========================================================================
    # Task Operations
    # =========================================================================

    async def save_task(self, task: ScheduledTask) -> None:
        """Save a scheduled task."""
        async with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                now = datetime.now(timezone.utc).isoformat()

                cursor.execute("""
                    INSERT OR REPLACE INTO scheduled_tasks (
                        id, name, description, handler_name, handler_params,
                        schedule_frequency, schedule_cron, schedule_hour,
                        schedule_minute, schedule_day_of_week,
                        priority, category, owner_executive,
                        enabled, status, run_count, last_run, next_run,
                        tags, metadata, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    task.id,
                    task.name,
                    task.description,
                    task.handler_name,
                    json.dumps(task.handler_params),
                    task.schedule.frequency.value,
                    task.schedule.cron_expression,
                    task.schedule.hour,
                    task.schedule.minute,
                    task.schedule.day_of_week,
                    task.priority.value,
                    task.category.value,
                    task.owner_executive,
                    1 if task.enabled else 0,
                    task.status.value,
                    task.execution_count,
                    (task.last_execution.completed_at.isoformat()
                     if task.last_execution and task.last_execution.completed_at
                     else None),
                    task.next_run.isoformat() if task.next_run else None,
                    json.dumps(task.tags),
                    json.dumps(task.metadata),
                    task.created_at.isoformat(),
                    now,
                ))

                conn.commit()

    async def load_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Load a scheduled task by ID."""
        async with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM scheduled_tasks WHERE id = ?",
                    (task_id,)
                )
                row = cursor.fetchone()

                if row:
                    return self._row_to_task(row)
                return None

    async def load_tasks(
        self,
        owner_executive: Optional[str] = None,
        status: Optional[str] = None,
        enabled_only: bool = True,
    ) -> List[ScheduledTask]:
        """Load scheduled tasks with optional filters."""
        async with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                query = "SELECT * FROM scheduled_tasks WHERE 1=1"
                params = []

                if owner_executive:
                    query += " AND owner_executive = ?"
                    params.append(owner_executive)

                if status:
                    query += " AND status = ?"
                    params.append(status)

                if enabled_only:
                    query += " AND enabled = 1"

                query += " ORDER BY created_at DESC"

                cursor.execute(query, params)
                rows = cursor.fetchall()

                return [self._row_to_task(row) for row in rows]

    async def delete_task(self, task_id: str) -> bool:
        """Delete a scheduled task."""
        async with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM scheduled_tasks WHERE id = ?",
                    (task_id,)
                )
                conn.commit()
                return cursor.rowcount > 0

    def _row_to_task(self, row: sqlite3.Row) -> ScheduledTask:
        """Convert a database row to a ScheduledTask."""
        frequency_map = {
            "once": ScheduleFrequency.ONCE,
            "hourly": ScheduleFrequency.HOURLY,
            "daily": ScheduleFrequency.DAILY,
            "weekly": ScheduleFrequency.WEEKLY,
            "monthly": ScheduleFrequency.MONTHLY,
            "custom": ScheduleFrequency.CUSTOM,
            "on_demand": ScheduleFrequency.ON_DEMAND,
        }

        priority_map = {
            "low": TaskPriority.LOW,
            "normal": TaskPriority.NORMAL,
            "high": TaskPriority.HIGH,
            "critical": TaskPriority.CRITICAL,
        }

        category_map = {
            "operations": TaskCategory.OPERATIONS,
            "analytics": TaskCategory.ANALYTICS,
            "maintenance": TaskCategory.MAINTENANCE,
            "reporting": TaskCategory.REPORTING,
            "integration": TaskCategory.INTEGRATION,
        }

        status_map = {
            "scheduled": TaskStatus.SCHEDULED,
            "pending": TaskStatus.PENDING,
            "running": TaskStatus.RUNNING,
            "paused": TaskStatus.PAUSED,
            "completed": TaskStatus.COMPLETED,
            "failed": TaskStatus.FAILED,
            "cancelled": TaskStatus.CANCELLED,
            # Legacy DB value migration
            "active": TaskStatus.SCHEDULED,
        }

        schedule = ScheduleConfig(
            frequency=frequency_map.get(row["schedule_frequency"], ScheduleFrequency.DAILY),
            cron_expression=row["schedule_cron"],
            hour=row["schedule_hour"],
            minute=row["schedule_minute"],
            day_of_week=row["schedule_day_of_week"],
        )

        return ScheduledTask(
            id=row["id"],
            name=row["name"],
            description=row["description"] or "",
            handler_name=row["handler_name"],
            handler_params=json.loads(row["handler_params"] or "{}"),
            schedule=schedule,
            priority=priority_map.get(row["priority"], TaskPriority.NORMAL),
            category=category_map.get(row["category"], TaskCategory.OPERATIONS),
            owner_executive=row["owner_executive"],
            enabled=bool(row["enabled"]),
            status=status_map.get(row["status"], TaskStatus.SCHEDULED),
            execution_count=row["run_count"],
            next_run=(
                datetime.fromisoformat(row["next_run"])
                if row["next_run"] else None
            ),
            tags=json.loads(row["tags"] or "[]"),
            metadata=json.loads(row["metadata"] or "{}"),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    # =========================================================================
    # Task Execution Operations
    # =========================================================================

    async def save_execution(
        self,
        task_id: str,
        execution: TaskExecution,
    ) -> None:
        """Save a task execution record."""
        async with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT INTO task_executions (
                        id, task_id, started_at, completed_at,
                        duration_seconds, success, output, error, metrics
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    execution.id,
                    task_id,
                    execution.started_at.isoformat(),
                    execution.completed_at.isoformat() if execution.completed_at else None,
                    execution.duration_seconds,
                    1 if execution.result and execution.result.success else 0,
                    json.dumps(execution.result.output) if execution.result else None,
                    execution.result.error if execution.result else None,
                    json.dumps(execution.metrics),
                ))

                conn.commit()

    async def load_executions(
        self,
        task_id: str,
        limit: int = 10,
    ) -> List[TaskExecution]:
        """Load execution history for a task."""
        async with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT * FROM task_executions
                    WHERE task_id = ?
                    ORDER BY started_at DESC
                    LIMIT ?
                """, (task_id, limit))

                rows = cursor.fetchall()
                return [self._row_to_execution(row) for row in rows]

    def _row_to_execution(self, row: sqlite3.Row) -> TaskExecution:
        """Convert a database row to a TaskExecution."""
        result = None
        if row["output"] or row["error"]:
            result = ExecutionResult(
                success=bool(row["success"]),
                output=json.loads(row["output"]) if row["output"] else None,
                error=row["error"],
            )

        return TaskExecution(
            id=row["id"],
            started_at=datetime.fromisoformat(row["started_at"]),
            completed_at=(
                datetime.fromisoformat(row["completed_at"])
                if row["completed_at"] else None
            ),
            duration_seconds=row["duration_seconds"],
            result=result,
            metrics=json.loads(row["metrics"] or "{}"),
        )

    # =========================================================================
    # Workflow Operations
    # =========================================================================

    async def save_workflow(self, workflow: WorkflowDefinition) -> None:
        """Save a workflow definition."""
        async with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                now = datetime.now(timezone.utc).isoformat()

                # Serialize steps
                steps_data = [
                    {
                        "id": step.id,
                        "name": step.name,
                        "description": step.description,
                        "handler_name": step.handler_name,
                        "handler_params": step.handler_params,
                        "depends_on": step.depends_on,
                        "continue_on_failure": step.continue_on_failure,
                        "timeout_seconds": step.timeout_seconds,
                    }
                    for step in workflow.steps
                ]

                cursor.execute("""
                    INSERT OR REPLACE INTO workflow_definitions (
                        id, name, description, version,
                        owner_executive, parallel_execution, stop_on_failure,
                        steps, tags, metadata, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    workflow.id,
                    workflow.name,
                    workflow.description,
                    workflow.version,
                    workflow.owner_executive,
                    1 if workflow.parallel_execution else 0,
                    1 if workflow.stop_on_failure else 0,
                    json.dumps(steps_data),
                    json.dumps(workflow.tags),
                    json.dumps(workflow.metadata),
                    workflow.created_at.isoformat(),
                    now,
                ))

                conn.commit()

    async def load_workflow(self, workflow_id: str) -> Optional[WorkflowDefinition]:
        """Load a workflow definition by ID."""
        async with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM workflow_definitions WHERE id = ?",
                    (workflow_id,)
                )
                row = cursor.fetchone()

                if row:
                    return self._row_to_workflow(row)
                return None

    async def load_workflows(
        self,
        owner_executive: Optional[str] = None,
    ) -> List[WorkflowDefinition]:
        """Load workflow definitions."""
        async with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                if owner_executive:
                    cursor.execute(
                        "SELECT * FROM workflow_definitions WHERE owner_executive = ?",
                        (owner_executive,)
                    )
                else:
                    cursor.execute("SELECT * FROM workflow_definitions")

                rows = cursor.fetchall()
                return [self._row_to_workflow(row) for row in rows]

    def _row_to_workflow(self, row: sqlite3.Row) -> WorkflowDefinition:
        """Convert a database row to a WorkflowDefinition."""
        steps_data = json.loads(row["steps"] or "[]")
        steps = [
            WorkflowStep(
                id=s["id"],
                name=s["name"],
                description=s.get("description", ""),
                handler_name=s["handler_name"],
                handler_params=s.get("handler_params", {}),
                depends_on=s.get("depends_on", []),
                continue_on_failure=s.get("continue_on_failure", False),
                timeout_seconds=s.get("timeout_seconds", 300),
            )
            for s in steps_data
        ]

        return WorkflowDefinition(
            id=row["id"],
            name=row["name"],
            description=row["description"] or "",
            version=row["version"],
            steps=steps,
            owner_executive=row["owner_executive"],
            parallel_execution=bool(row["parallel_execution"]),
            stop_on_failure=bool(row["stop_on_failure"]),
            tags=json.loads(row["tags"] or "[]"),
            metadata=json.loads(row["metadata"] or "{}"),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    # =========================================================================
    # Workflow Execution Operations
    # =========================================================================

    async def save_workflow_execution(
        self,
        execution: WorkflowExecution,
    ) -> None:
        """Save a workflow execution record."""
        async with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT OR REPLACE INTO workflow_executions (
                        id, workflow_id, workflow_name, status,
                        total_steps, completed_steps, failed_steps,
                        started_at, completed_at,
                        step_results, final_output, error, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    execution.id,
                    execution.workflow_id,
                    execution.workflow_name,
                    execution.status.value,
                    execution.total_steps,
                    execution.completed_steps,
                    execution.failed_steps,
                    execution.started_at.isoformat() if execution.started_at else None,
                    execution.completed_at.isoformat() if execution.completed_at else None,
                    json.dumps({
                        k: v.to_dict() for k, v in execution.step_results.items()
                    }),
                    json.dumps(execution.final_output) if execution.final_output else None,
                    execution.error,
                    json.dumps(execution.metadata),
                ))

                conn.commit()

    async def load_workflow_executions(
        self,
        workflow_id: Optional[str] = None,
        limit: int = 10,
    ) -> List[WorkflowExecution]:
        """Load workflow execution history."""
        async with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                if workflow_id:
                    cursor.execute("""
                        SELECT * FROM workflow_executions
                        WHERE workflow_id = ?
                        ORDER BY started_at DESC
                        LIMIT ?
                    """, (workflow_id, limit))
                else:
                    cursor.execute("""
                        SELECT * FROM workflow_executions
                        ORDER BY started_at DESC
                        LIMIT ?
                    """, (limit,))

                rows = cursor.fetchall()
                return [self._row_to_workflow_execution(row) for row in rows]

    def _row_to_workflow_execution(self, row: sqlite3.Row) -> WorkflowExecution:
        """Convert a database row to a WorkflowExecution."""
        status_map = {
            "pending": WorkflowStatus.PENDING,
            "running": WorkflowStatus.RUNNING,
            "completed": WorkflowStatus.COMPLETED,
            "failed": WorkflowStatus.FAILED,
            "partial": WorkflowStatus.PARTIAL,
            "cancelled": WorkflowStatus.CANCELLED,
        }

        # Parse step results
        step_results = {}
        step_data = json.loads(row["step_results"] or "{}")
        for step_id, result_data in step_data.items():
            step_results[step_id] = ExecutionResult(
                success=result_data.get("success", False),
                output=result_data.get("output"),
                error=result_data.get("error"),
            )

        return WorkflowExecution(
            id=row["id"],
            workflow_id=row["workflow_id"],
            workflow_name=row["workflow_name"] or "",
            status=status_map.get(row["status"], WorkflowStatus.PENDING),
            total_steps=row["total_steps"],
            completed_steps=row["completed_steps"],
            failed_steps=row["failed_steps"],
            started_at=(
                datetime.fromisoformat(row["started_at"])
                if row["started_at"] else None
            ),
            completed_at=(
                datetime.fromisoformat(row["completed_at"])
                if row["completed_at"] else None
            ),
            step_results=step_results,
            final_output=json.loads(row["final_output"]) if row["final_output"] else None,
            error=row["error"],
            metadata=json.loads(row["metadata"] or "{}"),
        )

    # =========================================================================
    # Utility Methods
    # =========================================================================

    async def get_stats(self) -> Dict[str, Any]:
        """Get persistence statistics."""
        async with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Count tasks
                cursor.execute("SELECT COUNT(*) FROM scheduled_tasks")
                total_tasks = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM scheduled_tasks WHERE enabled = 1")
                active_tasks = cursor.fetchone()[0]

                # Count executions
                cursor.execute("SELECT COUNT(*) FROM task_executions")
                total_executions = cursor.fetchone()[0]

                # Count workflows
                cursor.execute("SELECT COUNT(*) FROM workflow_definitions")
                total_workflows = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM workflow_executions")
                total_workflow_executions = cursor.fetchone()[0]

                return {
                    "initialized": self._initialized,
                    "db_path": self._connection_string,
                    "total_tasks": total_tasks,
                    "active_tasks": active_tasks,
                    "total_task_executions": total_executions,
                    "total_workflows": total_workflows,
                    "total_workflow_executions": total_workflow_executions,
                }

    async def clear_all(self) -> None:
        """Clear all data (for testing)."""
        async with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM workflow_executions")
                cursor.execute("DELETE FROM workflow_definitions")
                cursor.execute("DELETE FROM task_executions")
                cursor.execute("DELETE FROM scheduled_tasks")
                conn.commit()
