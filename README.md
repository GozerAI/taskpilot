# Taskpilot

**Task scheduling, workflow orchestration, and priority-based execution engine.**

Part of the [GozerAI](https://gozerai.com) ecosystem.

## Overview

Taskpilot provides a lightweight framework for scheduling recurring tasks, orchestrating multi-step workflows with step dependencies, and persisting state to SQLite. Designed for applications that need reliable, autonomous operational cycles. Zero external dependencies at the library layer.

## Installation

```bash
pip install taskpilot
```

For development:

```bash
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

# List scheduled tasks
tasks = service.list_tasks()

# Execute a task manually
result = asyncio.run(service.execute_task(task_id))

# Get scheduler statistics
stats = service.get_stats()
```

### Workflows

```python
# Create a workflow with dependent steps
workflow_id = service.create_workflow(
    name="ETL Pipeline",
    description="Collect, transform, and report",
)

step1 = service.add_workflow_step(workflow_id, name="Collect", handler_name="collect_data")
step2 = service.add_workflow_step(workflow_id, name="Transform", handler_name="transform_data",
                                  depends_on=[step1])
step3 = service.add_workflow_step(workflow_id, name="Report", handler_name="generate_report",
                                  depends_on=[step2])

# Execute the workflow
result = asyncio.run(service.execute_workflow(workflow_id))
```

## Feature Tiers

| Feature | Community | Pro | Enterprise |
|---|:---:|:---:|:---:|
| Task scheduling (once, hourly, daily, weekly, monthly) | x | x | x |
| Priority-based execution (critical, high, normal, low) | x | x | x |
| Handler registration | x | x | x |
| Manual task execution | x | x | x |
| Task listing and detail | x | x | x |
| Basic statistics | x | x | x |
| Task deletion | x | x | x |
| Workflow creation and execution | | x | x |
| Step dependencies (topological ordering) | | x | x |
| Workflow execution history | | x | x |
| Autonomous operational cycles | | x | x |
| Executive reports | | x | x |
| SQLite persistence layer | | x | x |
| Enterprise task orchestration engine | | | x |

### Gated Features

Pro and Enterprise features require a license key. Set the `VINZY_LICENSE_KEY` environment variable or visit [gozerai.com/pricing](https://gozerai.com/pricing) to upgrade.

## API Endpoints

Start the API server:

```bash
uvicorn taskpilot.app:app --host 0.0.0.0 --port 8004
```

### Community (taskpilot:basic)

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Health check |
| POST | `/v1/tasks` | Create a task |
| GET | `/v1/tasks` | List tasks |
| GET | `/v1/tasks/{task_id}` | Task detail |
| POST | `/v1/tasks/{task_id}/execute` | Execute a task |
| DELETE | `/v1/tasks/{task_id}` | Delete a task |
| GET | `/v1/stats` | Scheduler statistics |

### Pro (taskpilot:full)

| Method | Path | Description |
|---|---|---|
| GET | `/v1/workflows` | List workflows |
| POST | `/v1/workflows` | Create a workflow |
| POST | `/v1/workflows/{id}/steps` | Add workflow step |
| POST | `/v1/workflows/{id}/execute` | Execute a workflow |
| GET | `/v1/workflows/{id}/executions` | Workflow execution history |
| POST | `/v1/autonomous/cycle` | Run autonomous cycle |
| GET | `/v1/executive/{code}` | Executive report |

## Configuration

| Variable | Default | Description |
|---|---|---|
| `ZUULTIMATE_BASE_URL` | `http://localhost:8000` | Auth service URL |
| `CORS_ORIGINS` | `http://localhost:3000` | Comma-separated allowed origins |
| `VINZY_LICENSE_KEY` | (empty) | License key for Pro/Enterprise features |
| `VINZY_SERVER` | `http://localhost:8080` | License validation server |

## Requirements

- Python >= 3.10
- No external dependencies for the library (stdlib only)
- FastAPI + httpx for the API server

## License

MIT License. See [LICENSE](LICENSE) for details.
