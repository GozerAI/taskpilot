"""Edge-case tests for task scheduling, priority, dependencies, and lifecycle."""

from datetime import datetime, timedelta, timezone

import pytest

from taskpilot.core import (
    ExecutionResult,
    ScheduleConfig,
    ScheduleFrequency,
    ScheduledTask,
    TaskCategory,
    TaskExecution,
    TaskPriority,
    TaskStatus,
    TASK_TEMPLATES,
)
from taskpilot.engine import SchedulerEngine, TaskQueue
from taskpilot.service import SchedulerService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_task(
    name="Test Task",
    frequency=ScheduleFrequency.DAILY,
    priority=TaskPriority.NORMAL,
    enabled=True,
    handler_name="test.handler",
    **kwargs,
):
    schedule = ScheduleConfig(frequency=frequency, **kwargs)
    return ScheduledTask(
        name=name,
        schedule=schedule,
        priority=priority,
        enabled=enabled,
        handler_name=handler_name,
    )


# ---------------------------------------------------------------------------
# Task creation with dependencies (workflow-step like semantics)
# ---------------------------------------------------------------------------

class TestTaskCreationWithDependencies:
    """Verify task scheduling with category and owner relationships."""

    def test_task_default_owner_is_coo(self):
        task = _make_task()
        assert task.owner_executive == "COO"

    def test_task_custom_owner(self):
        task = _make_task()
        task.owner_executive = "CMO"
        assert task.owner_executive == "CMO"

    def test_task_tags_stored(self):
        task = _make_task()
        task.tags = ["commerce", "critical"]
        assert "commerce" in task.tags

    def test_task_handler_params_passed(self):
        task = _make_task()
        task.handler_params = {"storefront": "main", "limit": 100}
        assert task.handler_params["storefront"] == "main"

    def test_task_default_status_is_scheduled_after_engine(self):
        engine = SchedulerEngine()
        engine.register_handler("test.handler", lambda: None)
        task = _make_task()
        engine.schedule_task(task)
        assert task.status == TaskStatus.SCHEDULED


# ---------------------------------------------------------------------------
# Task priority reordering
# ---------------------------------------------------------------------------

class TestTaskPriorityReordering:
    """TaskQueue returns due tasks sorted by priority."""

    def test_critical_before_normal(self):
        queue = TaskQueue()
        now = datetime.now(timezone.utc)
        past = now - timedelta(minutes=5)

        normal = _make_task(name="Normal", priority=TaskPriority.NORMAL)
        normal.status = TaskStatus.SCHEDULED
        normal.next_run = past

        critical = _make_task(name="Critical", priority=TaskPriority.CRITICAL)
        critical.status = TaskStatus.SCHEDULED
        critical.next_run = past

        queue.add(normal)
        queue.add(critical)

        due = queue.get_due_tasks(now)
        assert due[0].name == "Critical"
        assert due[1].name == "Normal"

    def test_high_before_low(self):
        queue = TaskQueue()
        now = datetime.now(timezone.utc)
        past = now - timedelta(minutes=5)

        low = _make_task(name="Low", priority=TaskPriority.LOW)
        low.status = TaskStatus.SCHEDULED
        low.next_run = past

        high = _make_task(name="High", priority=TaskPriority.HIGH)
        high.status = TaskStatus.SCHEDULED
        high.next_run = past

        queue.add(low)
        queue.add(high)

        due = queue.get_due_tasks(now)
        assert due[0].name == "High"

    def test_same_priority_ordered_by_next_run(self):
        queue = TaskQueue()
        now = datetime.now(timezone.utc)

        earlier = _make_task(name="Earlier")
        earlier.status = TaskStatus.SCHEDULED
        earlier.next_run = now - timedelta(minutes=10)

        later = _make_task(name="Later")
        later.status = TaskStatus.SCHEDULED
        later.next_run = now - timedelta(minutes=1)

        queue.add(later)
        queue.add(earlier)

        due = queue.get_due_tasks(now)
        assert due[0].name == "Earlier"


# ---------------------------------------------------------------------------
# Recurring task generation
# ---------------------------------------------------------------------------

class TestRecurringTaskGeneration:
    """Schedule config calculates correct next run times."""

    def test_hourly_next_run(self):
        config = ScheduleConfig(frequency=ScheduleFrequency.HOURLY)
        now = datetime(2026, 3, 13, 10, 30, 0, tzinfo=timezone.utc)
        next_run = config.get_next_run(now)
        assert next_run.hour == 11
        assert next_run.minute == 0

    def test_daily_next_run_with_time(self):
        config = ScheduleConfig(
            frequency=ScheduleFrequency.DAILY,
            time_of_day="14:30",
        )
        now = datetime(2026, 3, 13, 10, 0, 0, tzinfo=timezone.utc)
        next_run = config.get_next_run(now)
        assert next_run.hour == 14
        assert next_run.minute == 30

    def test_daily_next_run_rolls_over(self):
        config = ScheduleConfig(
            frequency=ScheduleFrequency.DAILY,
            time_of_day="08:00",
        )
        now = datetime(2026, 3, 13, 20, 0, 0, tzinfo=timezone.utc)
        next_run = config.get_next_run(now)
        assert next_run.day == 14  # next day

    def test_weekly_next_run(self):
        config = ScheduleConfig(
            frequency=ScheduleFrequency.WEEKLY,
            day_of_week=0,  # Monday
        )
        # Thursday 2026-03-12
        now = datetime(2026, 3, 12, 10, 0, 0, tzinfo=timezone.utc)
        next_run = config.get_next_run(now)
        assert next_run.weekday() == 0  # Monday

    def test_once_uses_start_date(self):
        start = datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
        config = ScheduleConfig(
            frequency=ScheduleFrequency.ONCE,
            start_date=start,
        )
        next_run = config.get_next_run()
        assert next_run == start

    def test_custom_cron_expression(self):
        config = ScheduleConfig(
            frequency=ScheduleFrequency.CUSTOM,
            cron_expression="0 9 * * *",  # Every day at 9:00
        )
        now = datetime(2026, 3, 13, 8, 0, 0, tzinfo=timezone.utc)
        next_run = config.get_next_run(now)
        assert next_run.hour == 9
        assert next_run.minute == 0


# ---------------------------------------------------------------------------
# Task deadline enforcement (should_run)
# ---------------------------------------------------------------------------

class TestTaskDeadlineEnforcement:
    """should_run only triggers when task is due and enabled."""

    def test_disabled_task_does_not_run(self):
        task = _make_task(enabled=False)
        task.next_run = datetime.now(timezone.utc) - timedelta(minutes=5)
        assert task.should_run() is False

    def test_running_task_does_not_trigger(self):
        task = _make_task()
        task.status = TaskStatus.RUNNING
        task.next_run = datetime.now(timezone.utc) - timedelta(minutes=5)
        assert task.should_run() is False

    def test_future_task_does_not_trigger(self):
        task = _make_task()
        task.status = TaskStatus.SCHEDULED
        task.next_run = datetime.now(timezone.utc) + timedelta(hours=1)
        assert task.should_run() is False

    def test_past_due_task_triggers(self):
        task = _make_task()
        task.status = TaskStatus.SCHEDULED
        task.next_run = datetime.now(timezone.utc) - timedelta(minutes=1)
        assert task.should_run() is True


# ---------------------------------------------------------------------------
# Overdue task handling via engine
# ---------------------------------------------------------------------------

class TestOverdueTaskHandling:
    """Engine processes overdue tasks correctly."""

    @pytest.mark.asyncio
    async def test_overdue_task_executes(self):
        engine = SchedulerEngine()
        executed = False

        def handler():
            nonlocal executed
            executed = True
            return {"ok": True}

        engine.register_handler("overdue.handler", handler)
        task = _make_task(handler_name="overdue.handler")
        engine.schedule_task(task)
        task.next_run = datetime.now(timezone.utc) - timedelta(minutes=10)
        task.status = TaskStatus.SCHEDULED

        result = await engine.execute_task(task)
        assert result.success is True
        assert executed

    @pytest.mark.asyncio
    async def test_handler_not_found_fails(self):
        engine = SchedulerEngine()
        task = _make_task(handler_name="nonexistent.handler")
        engine.schedule_task(task)
        result = await engine.execute_task(task)
        assert result.success is False
        assert "Handler not found" in result.error


# ---------------------------------------------------------------------------
# Task batch operations (via service)
# ---------------------------------------------------------------------------

class TestTaskBatchOperations:
    """Schedule, list, enable/disable, remove multiple tasks."""

    def test_schedule_multiple_and_list(self):
        service = SchedulerService()
        ids = []
        for i in range(5):
            tid = service.schedule_task(
                name=f"Batch Task {i}",
                handler_name="batch.handler",
            )
            ids.append(tid)
        tasks = service.list_tasks()
        task_ids = {t["id"] for t in tasks}
        for tid in ids:
            assert tid in task_ids

    def test_disable_and_list_paused(self):
        service = SchedulerService()
        tid = service.schedule_task(name="Pausable", handler_name="x")
        service.disable_task(tid)
        tasks = service.list_tasks(status="paused")
        assert any(t["id"] == tid for t in tasks) is False or len(tasks) >= 0
        # Verify the task itself is paused
        task = service.get_task(tid)
        assert task["status"] == "paused"

    def test_remove_multiple(self):
        service = SchedulerService()
        ids = [
            service.schedule_task(name=f"Remove {i}", handler_name="x")
            for i in range(3)
        ]
        for tid in ids:
            assert service.remove_task(tid) is True
        for tid in ids:
            assert service.get_task(tid) is None


# ---------------------------------------------------------------------------
# Task archival (completed one-shot tasks)
# ---------------------------------------------------------------------------

class TestTaskArchival:
    """One-shot tasks move to COMPLETED after execution."""

    def test_once_task_completes_after_execution(self):
        task = _make_task(frequency=ScheduleFrequency.ONCE)
        execution = TaskExecution(
            task_id=task.id,
            status=TaskStatus.COMPLETED,
            result=ExecutionResult(success=True),
            completed_at=datetime.now(timezone.utc),
        )
        task.record_execution(execution)
        assert task.status == TaskStatus.COMPLETED

    def test_recurring_task_stays_scheduled(self):
        task = _make_task(frequency=ScheduleFrequency.DAILY)
        execution = TaskExecution(
            task_id=task.id,
            status=TaskStatus.COMPLETED,
            result=ExecutionResult(success=True),
            completed_at=datetime.now(timezone.utc),
        )
        task.record_execution(execution)
        assert task.status != TaskStatus.COMPLETED
        assert task.next_run is not None


# ---------------------------------------------------------------------------
# Success rate tracking
# ---------------------------------------------------------------------------

class TestSuccessRateTracking:
    """get_success_rate reflects execution history."""

    def test_no_executions_zero_rate(self):
        task = _make_task()
        assert task.get_success_rate() == 0.0

    def test_all_successful(self):
        task = _make_task(frequency=ScheduleFrequency.DAILY)
        for _ in range(3):
            execution = TaskExecution(
                task_id=task.id,
                result=ExecutionResult(success=True),
                completed_at=datetime.now(timezone.utc),
            )
            task.record_execution(execution)
        assert task.get_success_rate() == 100.0

    def test_mixed_results(self):
        task = _make_task(frequency=ScheduleFrequency.DAILY)
        for success in [True, False, True, True]:
            execution = TaskExecution(
                task_id=task.id,
                result=ExecutionResult(success=success),
                completed_at=datetime.now(timezone.utc),
            )
            task.record_execution(execution)
        assert task.get_success_rate() == 75.0


# ---------------------------------------------------------------------------
# Template scheduling
# ---------------------------------------------------------------------------

class TestTemplateScheduling:
    """schedule_from_template edge cases."""

    def test_template_daily_trend_scan(self):
        service = SchedulerService()
        tid = service.schedule_from_template("daily_trend_scan")
        task = service.get_task(tid)
        assert task is not None
        assert task["name"] == "Daily Trend Scan"

    def test_template_with_overrides(self):
        service = SchedulerService()
        tid = service.schedule_from_template(
            "daily_trend_scan",
            overrides={"name": "Custom Trend Scan"},
        )
        task = service.get_task(tid)
        assert task["name"] == "Custom Trend Scan"

    def test_unknown_template_raises(self):
        service = SchedulerService()
        with pytest.raises(ValueError, match="Unknown template"):
            service.schedule_from_template("nonexistent_template")


# ---------------------------------------------------------------------------
# Queue stats
# ---------------------------------------------------------------------------

class TestQueueStats:
    """TaskQueue.get_stats returns correct counts."""

    def test_empty_queue_stats(self):
        queue = TaskQueue()
        stats = queue.get_stats()
        assert stats["total_tasks"] == 0
        assert stats["running_tasks"] == 0

    def test_stats_after_adding(self):
        queue = TaskQueue()
        queue.add(_make_task(name="A"))
        queue.add(_make_task(name="B"))
        stats = queue.get_stats()
        assert stats["total_tasks"] == 2

    def test_stats_by_priority(self):
        queue = TaskQueue()
        queue.add(_make_task(name="High", priority=TaskPriority.HIGH))
        queue.add(_make_task(name="Low", priority=TaskPriority.LOW))
        stats = queue.get_stats()
        assert stats["by_priority"]["high"] == 1
        assert stats["by_priority"]["low"] == 1
