"""Taskpilot CLI — command-line interface for task and workflow management.

Provides argparse-based commands for creating, listing, running, and
inspecting tasks and workflows without external dependencies.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from typing import Optional, Sequence

from taskpilot.core import (
    ScheduleConfig,
    ScheduleFrequency,
    ScheduledTask,
    TaskPriority,
    TaskStatus,
)
from taskpilot.engine import SchedulerEngine
from taskpilot.service import SchedulerService
from taskpilot.workflows import WorkflowDefinition, WorkflowStep


def _get_service() -> SchedulerService:
    """Return a SchedulerService instance."""
    return SchedulerService()


def _run_async(coro):
    """Run an async coroutine synchronously."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # Already inside an event loop (e.g. pytest-asyncio) -- create a new one
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()
    else:
        return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_create(args: argparse.Namespace, service: SchedulerService) -> None:
    """Create a new task."""
    priority_map = {"high": "high", "medium": "normal", "low": "low"}
    priority = priority_map.get(args.priority, "normal")

    tags = []
    if args.tags:
        tags = [t.strip() for t in args.tags.split(",") if t.strip()]

    # Register a no-op handler so the engine accepts the task
    handler_name = f"cli_task_{args.name}"
    service.register_handler(handler_name, lambda: None)

    task_id = service.schedule_task(
        name=args.name,
        handler_name=handler_name,
        description=f"CLI task: {args.name}",
        priority=priority,
        frequency="on_demand",
    )

    # Attach metadata that the CLI cares about
    task_obj = service._engine.get_task(task_id)
    if task_obj:
        task_obj.tags = tags
        if args.due:
            task_obj.metadata["due"] = args.due

    print(f"Task created: {task_id}")
    print(f"  Name:     {args.name}")
    print(f"  Priority: {priority}")
    if args.due:
        print(f"  Due:      {args.due}")
    if tags:
        print(f"  Tags:     {', '.join(tags)}")


def cmd_list(args: argparse.Namespace, service: SchedulerService) -> None:
    """List tasks."""
    status_filter = getattr(args, "status", None)
    limit = getattr(args, "limit", 20)

    # Get all tasks, then filter by raw status value (the service only maps
    # a subset of statuses, so we do our own filtering here).
    tasks = service.list_tasks()
    if status_filter:
        tasks = [t for t in tasks if t.get("status") == status_filter]

    if limit and limit > 0:
        tasks = tasks[:limit]

    if not tasks:
        print("No tasks found.")
        return

    print(f"{'ID':<40} {'Name':<25} {'Status':<12} {'Priority':<10}")
    print("-" * 87)
    for t in tasks:
        tid = t.get("id", "")[:38]
        name = t.get("name", "")[:23]
        status = t.get("status", "")
        priority = t.get("priority", "")
        print(f"{tid:<40} {name:<25} {status:<12} {priority:<10}")

    print(f"\nTotal: {len(tasks)} task(s)")


def cmd_run(args: argparse.Namespace, service: SchedulerService) -> None:
    """Execute a task immediately."""
    task_id = args.task_id
    task_dict = service.get_task(task_id)
    if not task_dict:
        print(f"Error: Task '{task_id}' not found.", file=sys.stderr)
        sys.exit(1)

    print(f"Running task: {task_dict.get('name', task_id)}")
    try:
        result = _run_async(service.run_task_now(task_id))
        success = result.get("success", False)
        duration = result.get("duration_seconds", 0)
        print(f"  Status:   {'OK' if success else 'FAILED'}")
        print(f"  Duration: {duration:.2f}s")
        if result.get("error"):
            print(f"  Error:    {result['error']}")
        if result.get("output"):
            print(f"  Output:   {result['output']}")
    except Exception as e:
        print(f"  Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_status(args: argparse.Namespace, service: SchedulerService) -> None:
    """Show task details."""
    task_id = args.task_id
    task_dict = service.get_task(task_id)
    if not task_dict:
        print(f"Error: Task '{task_id}' not found.", file=sys.stderr)
        sys.exit(1)

    print(f"Task: {task_dict.get('name', 'N/A')}")
    print(f"  ID:             {task_dict.get('id', 'N/A')}")
    print(f"  Status:         {task_dict.get('status', 'N/A')}")
    print(f"  Priority:       {task_dict.get('priority', 'N/A')}")
    print(f"  Category:       {task_dict.get('category', 'N/A')}")
    print(f"  Handler:        {task_dict.get('handler_name', 'N/A')}")
    print(f"  Executions:     {task_dict.get('execution_count', 0)}")
    print(f"  Enabled:        {task_dict.get('enabled', False)}")
    print(f"  Tags:           {', '.join(task_dict.get('tags', []))}")
    print(f"  Created:        {task_dict.get('created_at', 'N/A')}")
    next_run = task_dict.get("next_run")
    if next_run:
        print(f"  Next Run:       {next_run}")
    last = task_dict.get("last_execution")
    if last:
        print(f"  Last Execution: {last.get('status', 'N/A')}")


def cmd_workflow_create(args: argparse.Namespace, service: SchedulerService) -> None:
    """Create a new workflow."""
    steps_raw = [s.strip() for s in args.steps.split(",") if s.strip()]
    if not steps_raw:
        print("Error: At least one step is required.", file=sys.stderr)
        sys.exit(1)

    # Register no-op handlers for each step
    for step_name in steps_raw:
        handler = f"cli_step_{step_name}"
        service.register_handler(handler, lambda: None)

    workflow_id = service.create_workflow(
        name=args.name,
        description=f"CLI workflow: {args.name}",
    )

    for step_name in steps_raw:
        handler = f"cli_step_{step_name}"
        service.add_workflow_step(
            workflow_id=workflow_id,
            name=step_name,
            handler_name=handler,
            description=f"Step: {step_name}",
        )

    print(f"Workflow created: {workflow_id}")
    print(f"  Name:  {args.name}")
    print(f"  Steps: {', '.join(steps_raw)}")


def cmd_workflow_run(args: argparse.Namespace, service: SchedulerService) -> None:
    """Execute a workflow."""
    workflow_id = args.workflow_id
    wf = service.get_workflow(workflow_id)
    if not wf:
        print(f"Error: Workflow '{workflow_id}' not found.", file=sys.stderr)
        sys.exit(1)

    print(f"Running workflow: {wf.get('name', workflow_id)}")
    try:
        result = _run_async(service.execute_workflow(workflow_id))
        status = result.get("status", "unknown")
        completed = result.get("completed_steps", 0)
        total = result.get("total_steps", 0)
        failed = result.get("failed_steps", 0)
        print(f"  Status:    {status}")
        print(f"  Completed: {completed}/{total} steps")
        if failed:
            print(f"  Failed:    {failed} step(s)")
        if result.get("error"):
            print(f"  Error:     {result['error']}")
    except Exception as e:
        print(f"  Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_workflow_status(args: argparse.Namespace, service: SchedulerService) -> None:
    """Show workflow details."""
    workflow_id = args.workflow_id
    wf = service.get_workflow(workflow_id)
    if not wf:
        print(f"Error: Workflow '{workflow_id}' not found.", file=sys.stderr)
        sys.exit(1)

    print(f"Workflow: {wf.get('name', 'N/A')}")
    print(f"  ID:       {wf.get('id', 'N/A')}")
    print(f"  Version:  {wf.get('version', 'N/A')}")
    print(f"  Steps:    {wf.get('steps_count', 0)}")
    print(f"  Owner:    {wf.get('owner_executive', 'N/A')}")
    print(f"  Parallel: {wf.get('parallel_execution', False)}")

    steps = wf.get("steps", [])
    if steps:
        print("\n  Steps:")
        for i, step in enumerate(steps, 1):
            name = step.get("name", "unnamed")
            status = step.get("status", "pending")
            handler = step.get("handler_name", "")
            print(f"    {i}. {name} [{status}] -> {handler}")

    # Show recent executions
    executions = service.get_workflow_executions(workflow_id=workflow_id, limit=5)
    if executions:
        print(f"\n  Recent Executions:")
        for ex in executions:
            ex_status = ex.get("status", "N/A")
            started = ex.get("started_at", "N/A")
            completed = ex.get("completed_steps", 0)
            total = ex.get("total_steps", 0)
            print(f"    - {started}: {ex_status} ({completed}/{total} steps)")


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="taskpilot",
        description="Taskpilot - Autonomous task scheduling and workflow orchestration",
    )
    sub = parser.add_subparsers(dest="command")

    # create
    create_p = sub.add_parser("create", help="Create a new task")
    create_p.add_argument("name", help="Task name")
    create_p.add_argument(
        "--priority", choices=["high", "medium", "low"], default="medium",
        help="Task priority (default: medium)",
    )
    create_p.add_argument("--due", help="Due date in YYYY-MM-DD format")
    create_p.add_argument("--tags", help="Comma-separated tags")

    # list
    list_p = sub.add_parser("list", help="List tasks")
    list_p.add_argument(
        "--status",
        choices=["pending", "running", "completed", "failed"],
        default=None,
        help="Filter by status",
    )
    list_p.add_argument(
        "--limit", type=int, default=20,
        help="Maximum number of tasks to show (default: 20)",
    )

    # run
    run_p = sub.add_parser("run", help="Execute a task")
    run_p.add_argument("task_id", help="Task ID to execute")

    # status
    status_p = sub.add_parser("status", help="Show task details")
    status_p.add_argument("task_id", help="Task ID")

    # workflow
    wf_p = sub.add_parser("workflow", help="Workflow management")
    wf_sub = wf_p.add_subparsers(dest="workflow_command")

    # workflow create
    wf_create = wf_sub.add_parser("create", help="Create a workflow")
    wf_create.add_argument("name", help="Workflow name")
    wf_create.add_argument(
        "--steps", required=True,
        help="Comma-separated step names",
    )

    # workflow run
    wf_run = wf_sub.add_parser("run", help="Execute a workflow")
    wf_run.add_argument("workflow_id", help="Workflow ID to execute")

    # workflow status
    wf_status = wf_sub.add_parser("status", help="Show workflow details")
    wf_status.add_argument("workflow_id", help="Workflow ID")

    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        sys.exit(1)

    service = _get_service()

    if args.command == "create":
        cmd_create(args, service)
    elif args.command == "list":
        cmd_list(args, service)
    elif args.command == "run":
        cmd_run(args, service)
    elif args.command == "status":
        cmd_status(args, service)
    elif args.command == "workflow":
        if not hasattr(args, "workflow_command") or not args.workflow_command:
            parser.parse_args(["workflow", "--help"])
            sys.exit(1)
        if args.workflow_command == "create":
            cmd_workflow_create(args, service)
        elif args.workflow_command == "run":
            cmd_workflow_run(args, service)
        elif args.workflow_command == "status":
            cmd_workflow_status(args, service)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
