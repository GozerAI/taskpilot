# Taskpilot

Autonomous task scheduling, workflow orchestration, and priority-based execution engine for Python.

## Overview

Taskpilot provides a lightweight, dependency-free framework for scheduling recurring tasks, orchestrating multi-step workflows with step dependencies, and persisting state to SQLite. Designed for applications that need reliable, autonomous operational cycles without external dependencies.

## Features

- **Priority-based task queue** -- Tasks sorted by priority (critical, high, normal, low) and next-run time
- **Recurring schedules** -- Frequencies: once, hourly, daily, weekly, monthly, custom (cron), and on-demand
- **Workflow orchestration** -- Multi-step workflows with explicit step dependencies and topological execution order
- **SQLite persistence** -- Durable storage for tasks, execution history, workflow definitions, and records
- **Task templates** -- Pre-defined templates for common patterns (trend scans, pricing checks, brand audits)
- **Async execution** -- Configurable timeouts and automatic retries with delay
- **Concurrency control** -- Maximum concurrent task limit with backpressure
- **Execution metrics** -- Track total executions, success/failure counts, retries, uptime, per-task success rates

## Installation

```
pip install taskpilot
```

For development:

```
pip install -e ".[dev]"
```

## Quick Start

```python
import asyncio
from taskpilot import SchedulerService

service = SchedulerService()

# Register a handler
def my_analysis():
    return {"status": "complete", "items_processed": 42}

service.register_handler("run_analysis", my_analysis)

# Schedule a daily task
task_id = service.schedule_task(
    name="Daily Analysis",
    handler_name="run_analysis",
    frequency="daily",
    hour=8,
    priority="high",
)

# Run the scheduler
async def main():
    await service.start()

asyncio.run(main())
```

### Workflows

```python
# Create a workflow with dependent steps
workflow_id = service.create_workflow(
    name="ETL Pipeline",
    description="Collect, transform, and report",
    parallel_execution=True,
)

step1 = service.add_workflow_step(workflow_id, name="Collect", handler_name="collect_data")
step2 = service.add_workflow_step(workflow_id, name="Transform", handler_name="transform_data",
                                  depends_on=[step1])
step3 = service.add_workflow_step(workflow_id, name="Report", handler_name="generate_report",
                                  depends_on=[step2])
```

## Architecture

```
src/taskpilot/
    core.py         Data models, enums, schedule config, task templates
    engine.py       SchedulerEngine, TaskQueue, async execution loop
    workflows.py    WorkflowDefinition, WorkflowStep, WorkflowExecutor
    persistence.py  SQLite-backed storage (async-safe)
    service.py      SchedulerService -- unified high-level facade
```

## Running Tests

```
pytest tests/ -v
```

## Requirements

- Python >= 3.10
- No external dependencies (stdlib only)

## License

MIT License. See [LICENSE](LICENSE) for details.

## Author

Chris Arseno
