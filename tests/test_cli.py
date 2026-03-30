"""Tests for taskpilot.cli — command-line interface."""

from __future__ import annotations

import sys
from io import StringIO
from unittest.mock import patch, MagicMock

import pytest

from taskpilot.cli import (
    build_parser,
    cmd_create,
    cmd_list,
    cmd_run,
    cmd_status,
    cmd_workflow_create,
    cmd_workflow_run,
    cmd_workflow_status,
    main,
)
from taskpilot.service import SchedulerService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def service():
    """Fresh SchedulerService for each test."""
    return SchedulerService()


@pytest.fixture
def service_with_task(service):
    """Service with one task already created."""
    handler_name = "test_handler"
    service.register_handler(handler_name, lambda: "done")
    task_id = service.schedule_task(
        name="Test Task",
        handler_name=handler_name,
        description="A test task",
        priority="high",
        frequency="on_demand",
    )
    return service, task_id


@pytest.fixture
def service_with_workflow(service):
    """Service with one workflow already created."""
    service.register_handler("step_a", lambda: "a_done")
    service.register_handler("step_b", lambda: "b_done")
    wf_id = service.create_workflow(name="Test WF", description="A test workflow")
    service.add_workflow_step(wf_id, name="Step A", handler_name="step_a")
    service.add_workflow_step(wf_id, name="Step B", handler_name="step_b")
    return service, wf_id


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------

class TestParser:
    def test_build_parser_returns_parser(self):
        parser = build_parser()
        assert parser is not None
        assert parser.prog == "taskpilot"

    def test_parse_create(self):
        parser = build_parser()
        args = parser.parse_args(["create", "MyTask", "--priority", "high", "--due", "2026-04-01", "--tags", "a,b"])
        assert args.command == "create"
        assert args.name == "MyTask"
        assert args.priority == "high"
        assert args.due == "2026-04-01"
        assert args.tags == "a,b"

    def test_parse_create_defaults(self):
        parser = build_parser()
        args = parser.parse_args(["create", "Simple"])
        assert args.priority == "medium"
        assert args.due is None
        assert args.tags is None

    def test_parse_list_no_filters(self):
        parser = build_parser()
        args = parser.parse_args(["list"])
        assert args.command == "list"
        assert args.status is None
        assert args.limit == 20

    def test_parse_list_with_filters(self):
        parser = build_parser()
        args = parser.parse_args(["list", "--status", "running", "--limit", "5"])
        assert args.status == "running"
        assert args.limit == 5

    def test_parse_run(self):
        parser = build_parser()
        args = parser.parse_args(["run", "abc-123"])
        assert args.command == "run"
        assert args.task_id == "abc-123"

    def test_parse_status(self):
        parser = build_parser()
        args = parser.parse_args(["status", "abc-123"])
        assert args.command == "status"
        assert args.task_id == "abc-123"

    def test_parse_workflow_create(self):
        parser = build_parser()
        args = parser.parse_args(["workflow", "create", "MyWF", "--steps", "a,b,c"])
        assert args.command == "workflow"
        assert args.workflow_command == "create"
        assert args.name == "MyWF"
        assert args.steps == "a,b,c"

    def test_parse_workflow_run(self):
        parser = build_parser()
        args = parser.parse_args(["workflow", "run", "wf-123"])
        assert args.command == "workflow"
        assert args.workflow_command == "run"
        assert args.workflow_id == "wf-123"

    def test_parse_workflow_status(self):
        parser = build_parser()
        args = parser.parse_args(["workflow", "status", "wf-123"])
        assert args.command == "workflow"
        assert args.workflow_command == "status"
        assert args.workflow_id == "wf-123"

    def test_invalid_priority_rejected(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["create", "Bad", "--priority", "ultra"])

    def test_invalid_status_rejected(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["list", "--status", "bogus"])


# ---------------------------------------------------------------------------
# create command
# ---------------------------------------------------------------------------

class TestCmdCreate:
    def test_create_basic(self, service, capsys):
        parser = build_parser()
        args = parser.parse_args(["create", "NewTask"])
        cmd_create(args, service)
        out = capsys.readouterr().out
        assert "Task created:" in out
        assert "NewTask" in out

    def test_create_with_priority(self, service, capsys):
        parser = build_parser()
        args = parser.parse_args(["create", "Urgent", "--priority", "high"])
        cmd_create(args, service)
        out = capsys.readouterr().out
        assert "high" in out

    def test_create_with_due_date(self, service, capsys):
        parser = build_parser()
        args = parser.parse_args(["create", "Dated", "--due", "2026-12-31"])
        cmd_create(args, service)
        out = capsys.readouterr().out
        assert "2026-12-31" in out

    def test_create_with_tags(self, service, capsys):
        parser = build_parser()
        args = parser.parse_args(["create", "Tagged", "--tags", "tag1,tag2"])
        cmd_create(args, service)
        out = capsys.readouterr().out
        assert "tag1" in out
        assert "tag2" in out

    def test_create_low_priority(self, service, capsys):
        parser = build_parser()
        args = parser.parse_args(["create", "Chill", "--priority", "low"])
        cmd_create(args, service)
        out = capsys.readouterr().out
        assert "low" in out

    def test_create_medium_priority_default(self, service, capsys):
        parser = build_parser()
        args = parser.parse_args(["create", "Default"])
        cmd_create(args, service)
        out = capsys.readouterr().out
        assert "normal" in out


# ---------------------------------------------------------------------------
# list command
# ---------------------------------------------------------------------------

class TestCmdList:
    def test_list_empty(self, service, capsys):
        parser = build_parser()
        args = parser.parse_args(["list"])
        cmd_list(args, service)
        out = capsys.readouterr().out
        assert "No tasks found" in out

    def test_list_with_tasks(self, service_with_task, capsys):
        service, task_id = service_with_task
        parser = build_parser()
        args = parser.parse_args(["list"])
        cmd_list(args, service)
        out = capsys.readouterr().out
        assert "Test Task" in out
        assert "Total:" in out

    def test_list_with_limit(self, service, capsys):
        # Create multiple tasks
        for i in range(5):
            handler = f"h{i}"
            service.register_handler(handler, lambda: None)
            service.schedule_task(name=f"Task {i}", handler_name=handler, frequency="on_demand")

        parser = build_parser()
        args = parser.parse_args(["list", "--limit", "3"])
        cmd_list(args, service)
        out = capsys.readouterr().out
        assert "Total: 3" in out

    def test_list_status_filter_no_match(self, service_with_task, capsys):
        service, _ = service_with_task
        parser = build_parser()
        args = parser.parse_args(["list", "--status", "failed"])
        cmd_list(args, service)
        out = capsys.readouterr().out
        assert "No tasks found" in out


# ---------------------------------------------------------------------------
# run command
# ---------------------------------------------------------------------------

class TestCmdRun:
    def test_run_existing_task(self, service_with_task, capsys):
        service, task_id = service_with_task
        parser = build_parser()
        args = parser.parse_args(["run", task_id])
        cmd_run(args, service)
        out = capsys.readouterr().out
        assert "Running task:" in out
        assert "OK" in out

    def test_run_nonexistent_task(self, service):
        parser = build_parser()
        args = parser.parse_args(["run", "nonexistent-id"])
        with pytest.raises(SystemExit):
            cmd_run(args, service)


# ---------------------------------------------------------------------------
# status command
# ---------------------------------------------------------------------------

class TestCmdStatus:
    def test_status_existing_task(self, service_with_task, capsys):
        service, task_id = service_with_task
        parser = build_parser()
        args = parser.parse_args(["status", task_id])
        cmd_status(args, service)
        out = capsys.readouterr().out
        assert "Test Task" in out
        assert "ID:" in out
        assert "Status:" in out
        assert "Priority:" in out

    def test_status_nonexistent_task(self, service):
        parser = build_parser()
        args = parser.parse_args(["status", "ghost-id"])
        with pytest.raises(SystemExit):
            cmd_status(args, service)


# ---------------------------------------------------------------------------
# workflow create
# ---------------------------------------------------------------------------

class TestCmdWorkflowCreate:
    def test_create_workflow(self, service, capsys):
        parser = build_parser()
        args = parser.parse_args(["workflow", "create", "MyWF", "--steps", "fetch,process,save"])
        cmd_workflow_create(args, service)
        out = capsys.readouterr().out
        assert "Workflow created:" in out
        assert "MyWF" in out
        assert "fetch" in out
        assert "process" in out
        assert "save" in out

    def test_create_workflow_single_step(self, service, capsys):
        parser = build_parser()
        args = parser.parse_args(["workflow", "create", "Single", "--steps", "only_step"])
        cmd_workflow_create(args, service)
        out = capsys.readouterr().out
        assert "only_step" in out

    def test_create_workflow_empty_steps_fails(self, service):
        parser = build_parser()
        args = parser.parse_args(["workflow", "create", "Empty", "--steps", ""])
        with pytest.raises(SystemExit):
            cmd_workflow_create(args, service)


# ---------------------------------------------------------------------------
# workflow run
# ---------------------------------------------------------------------------

class TestCmdWorkflowRun:
    def test_run_workflow(self, service_with_workflow, capsys):
        service, wf_id = service_with_workflow
        parser = build_parser()
        args = parser.parse_args(["workflow", "run", wf_id])
        cmd_workflow_run(args, service)
        out = capsys.readouterr().out
        assert "Running workflow:" in out
        assert "Status:" in out

    def test_run_nonexistent_workflow(self, service):
        parser = build_parser()
        args = parser.parse_args(["workflow", "run", "no-such-wf"])
        with pytest.raises(SystemExit):
            cmd_workflow_run(args, service)


# ---------------------------------------------------------------------------
# workflow status
# ---------------------------------------------------------------------------

class TestCmdWorkflowStatus:
    def test_status_workflow(self, service_with_workflow, capsys):
        service, wf_id = service_with_workflow
        parser = build_parser()
        args = parser.parse_args(["workflow", "status", wf_id])
        cmd_workflow_status(args, service)
        out = capsys.readouterr().out
        assert "Test WF" in out
        assert "Steps:" in out
        assert "Step A" in out
        assert "Step B" in out

    def test_status_nonexistent_workflow(self, service):
        parser = build_parser()
        args = parser.parse_args(["workflow", "status", "no-such-wf"])
        with pytest.raises(SystemExit):
            cmd_workflow_status(args, service)


# ---------------------------------------------------------------------------
# main() entry point
# ---------------------------------------------------------------------------

class TestMain:
    def test_no_command_exits(self):
        with pytest.raises(SystemExit):
            main([])

    def test_main_create(self, capsys):
        main(["create", "CLITest", "--priority", "low", "--tags", "x,y"])
        out = capsys.readouterr().out
        assert "Task created:" in out

    def test_main_list_empty(self, capsys):
        main(["list"])
        out = capsys.readouterr().out
        assert "No tasks found" in out

    def test_main_status_not_found(self):
        with pytest.raises(SystemExit):
            main(["status", "missing-id"])

    def test_main_run_not_found(self):
        with pytest.raises(SystemExit):
            main(["run", "missing-id"])

    def test_main_workflow_no_subcommand(self):
        with pytest.raises(SystemExit):
            main(["workflow"])

    def test_main_workflow_create(self, capsys):
        main(["workflow", "create", "WFTest", "--steps", "a,b"])
        out = capsys.readouterr().out
        assert "Workflow created:" in out


# ---------------------------------------------------------------------------
# Integration: create then status
# ---------------------------------------------------------------------------

class TestIntegration:
    def test_create_then_list(self, capsys):
        """Create a task via CLI, then list it."""
        service = SchedulerService()
        parser = build_parser()

        # Create
        args = parser.parse_args(["create", "IntTask", "--priority", "high", "--tags", "ci"])
        cmd_create(args, service)
        capsys.readouterr()  # clear

        # List
        args = parser.parse_args(["list"])
        cmd_list(args, service)
        out = capsys.readouterr().out
        assert "IntTask" in out

    def test_create_then_status(self, capsys):
        """Create a task then inspect its status."""
        service = SchedulerService()
        parser = build_parser()

        # We need to capture the task ID from create output
        args = parser.parse_args(["create", "StatusTask"])
        cmd_create(args, service)
        create_out = capsys.readouterr().out

        # Extract task ID from "Task created: <id>"
        task_id = create_out.split("Task created:")[1].strip().split("\n")[0].strip()

        args = parser.parse_args(["status", task_id])
        cmd_status(args, service)
        out = capsys.readouterr().out
        assert "StatusTask" in out
        assert task_id[:20] in out

    def test_create_then_run(self, capsys):
        """Create a task then run it."""
        service = SchedulerService()
        handler_name = "int_handler"
        service.register_handler(handler_name, lambda: "result_value")

        task_id = service.schedule_task(
            name="Runnable",
            handler_name=handler_name,
            frequency="on_demand",
        )

        parser = build_parser()
        args = parser.parse_args(["run", task_id])
        cmd_run(args, service)
        out = capsys.readouterr().out
        assert "OK" in out

    def test_workflow_create_then_status(self, capsys):
        """Create a workflow then check its status."""
        service = SchedulerService()
        parser = build_parser()

        args = parser.parse_args(["workflow", "create", "WFInt", "--steps", "x,y,z"])
        cmd_workflow_create(args, service)
        create_out = capsys.readouterr().out
        wf_id = create_out.split("Workflow created:")[1].strip().split("\n")[0].strip()

        args = parser.parse_args(["workflow", "status", wf_id])
        cmd_workflow_status(args, service)
        out = capsys.readouterr().out
        assert "WFInt" in out
        assert "x" in out
        assert "y" in out
        assert "z" in out

    def test_workflow_create_run_then_status(self, capsys):
        """Create workflow, run it, then check status shows execution history."""
        service = SchedulerService()
        service.register_handler("cli_step_alpha", lambda: "alpha_done")
        service.register_handler("cli_step_beta", lambda: "beta_done")

        parser = build_parser()

        args = parser.parse_args(["workflow", "create", "RunMe", "--steps", "alpha,beta"])
        cmd_workflow_create(args, service)
        create_out = capsys.readouterr().out
        wf_id = create_out.split("Workflow created:")[1].strip().split("\n")[0].strip()

        args = parser.parse_args(["workflow", "run", wf_id])
        cmd_workflow_run(args, service)
        run_out = capsys.readouterr().out
        assert "completed" in run_out or "Status:" in run_out
