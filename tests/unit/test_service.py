"""Tests for SchedulerService."""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from taskpilot.service import SchedulerService


class TestSchedulerService:
    """Tests for the SchedulerService."""

    @pytest.fixture
    def scheduler_service(self):
        """Create a SchedulerService instance."""
        from taskpilot import SchedulerService
        return SchedulerService()

    def test_service_initialization(self, scheduler_service):
        """Verify service initializes correctly."""
        assert scheduler_service is not None
        assert hasattr(scheduler_service, "_engine")
        assert hasattr(scheduler_service, "_workflow_executor")

    def test_schedule_task(self, scheduler_service):
        """Schedule a task."""
        task_id = scheduler_service.schedule_task(
            name="Test Task",
            handler_name="test.handler",
            description="Test description",
            frequency="daily",
        )
        assert task_id is not None
        assert isinstance(task_id, str)

    def test_list_tasks(self, scheduler_service):
        """List scheduled tasks."""
        scheduler_service.schedule_task(
            name="Test Task List",
            handler_name="test.handler",
        )
        tasks = scheduler_service.list_tasks()
        assert isinstance(tasks, list)
        assert len(tasks) >= 1

    def test_list_tasks_by_owner(self, scheduler_service):
        """List tasks filtered by owner."""
        scheduler_service.schedule_task(
            name="COO Filtered Task",
            handler_name="test.handler",
            owner_executive="COO",
        )
        tasks = scheduler_service.list_tasks(owner_executive="COO")
        # Should have at least the task we just created
        assert len(tasks) >= 1

    def test_enable_disable_task(self, scheduler_service):
        """Enable and disable a task."""
        task_id = scheduler_service.schedule_task(
            name="Enable Disable Test Task",
            handler_name="test.handler",
        )

        # Disable
        result = scheduler_service.disable_task(task_id)
        assert result is True

        # Enable
        result = scheduler_service.enable_task(task_id)
        assert result is True

    def test_list_workflows(self, scheduler_service):
        """List available workflows."""
        workflows = scheduler_service.list_workflows()
        assert isinstance(workflows, list)
        # Should have pre-registered workflows
        assert len(workflows) >= 1

    def test_get_executive_report_coo(self, scheduler_service):
        """COO report has operations focus."""
        report = scheduler_service.get_executive_report("COO")
        assert report["executive"] == "COO"
        # Check that it has expected structure
        assert "generated_at" in report

    def test_get_executive_report_ceo(self, scheduler_service):
        """CEO report has strategic focus."""
        report = scheduler_service.get_executive_report("CEO")
        assert report["executive"] == "CEO"
        # Check that it has expected structure
        assert "generated_at" in report

    def test_get_stats(self, scheduler_service):
        """Get service stats."""
        stats = scheduler_service.get_stats()
        # Just verify it returns a dict
        assert isinstance(stats, dict)

    def test_remove_task(self, scheduler_service):
        """Remove a task from the schedule."""
        task_id = scheduler_service.schedule_task(
            name="Remove Me",
            handler_name="test.handler",
        )
        assert scheduler_service.remove_task(task_id) is True
        assert scheduler_service.get_task(task_id) is None

    def test_remove_nonexistent_task(self, scheduler_service):
        """Removing nonexistent task returns False."""
        assert scheduler_service.remove_task("nonexistent-id") is False

    @pytest.mark.asyncio
    async def test_run_task_now(self, scheduler_service):
        """Run a task immediately."""
        handler_called = False

        def handler():
            nonlocal handler_called
            handler_called = True
            return {"status": "done"}

        scheduler_service.register_handler("immediate.handler", handler)
        task_id = scheduler_service.schedule_task(
            name="Immediate Task",
            handler_name="immediate.handler",
        )
        result = await scheduler_service.run_task_now(task_id)
        assert result["success"] is True
        assert handler_called

    @pytest.mark.asyncio
    async def test_run_task_now_not_found(self, scheduler_service):
        """Running nonexistent task raises ValueError."""
        with pytest.raises(ValueError, match="Task not found"):
            await scheduler_service.run_task_now("nonexistent-id")

    def test_schedule_from_template(self, scheduler_service):
        """Schedule a task from a pre-defined template."""
        task_id = scheduler_service.schedule_from_template("daily_trend_scan")
        assert task_id is not None
        task = scheduler_service.get_task(task_id)
        assert task["name"] == "Daily Trend Scan"

    def test_schedule_from_template_unknown(self, scheduler_service):
        """Scheduling from unknown template raises ValueError."""
        with pytest.raises(ValueError, match="Unknown template"):
            scheduler_service.schedule_from_template("nonexistent_template")

    def test_list_tasks_by_status(self, scheduler_service):
        """List tasks filtered by status."""
        task_id = scheduler_service.schedule_task(
            name="Status Filter Test",
            handler_name="test.handler",
        )
        tasks = scheduler_service.list_tasks(status="active")
        assert isinstance(tasks, list)

    def test_count_tasks_run_today(self, scheduler_service):
        """Count tasks run today returns an integer."""
        count = scheduler_service._count_tasks_run_today()
        assert isinstance(count, int)
        assert count >= 0

    def test_get_telemetry_returns_dict(self, scheduler_service):
        """get_telemetry always returns a dict (empty if lib not installed)."""
        result = scheduler_service.get_telemetry()
        assert isinstance(result, dict)

    def test_get_telemetry_graceful_without_lib(self, scheduler_service):
        """Without gozerai-telemetry installed, get_telemetry returns {}."""
        import taskpilot.service as svc
        original = svc._HAS_TELEMETRY
        try:
            svc._HAS_TELEMETRY = False
            result = scheduler_service.get_telemetry()
            assert result == {}
        finally:
            svc._HAS_TELEMETRY = original

    @pytest.mark.asyncio
    async def test_workflow_execution_persistence(self, tmp_path):
        """Workflow executions persist to disk and load on init."""
        storage = str(tmp_path)
        svc = SchedulerService(storage_path=storage)

        def step_handler():
            return {"ok": True}

        svc.register_handler("persist_test.handler", step_handler)
        workflow_id = svc.create_workflow(
            name="Persist Test", owner_executive="COO",
        )
        svc.add_workflow_step(
            workflow_id=workflow_id,
            name="Step 1",
            handler_name="persist_test.handler",
        )
        await svc.execute_workflow(workflow_id)

        # Verify file exists
        executions_file = tmp_path / "executions.json"
        assert executions_file.exists()

        # New service instance should load the execution
        svc2 = SchedulerService(storage_path=storage)
        history = svc2.get_workflow_executions()
        assert len(history) >= 1
        assert history[0]["workflow_name"] == "Persist Test"

    def test_no_storage_path_no_persistence(self):
        """Without storage_path, no files created."""
        svc = SchedulerService()
        assert svc._workflow_executor._executions_file is None
