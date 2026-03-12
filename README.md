# Taskpilot

AI task automation and workflow engine — Part of the GozerAI ecosystem.

## Overview

Taskpilot provides a lightweight, dependency-free framework for scheduling recurring tasks, orchestrating multi-step workflows, and persisting state. Community tier includes core data models, the service layer, and licensing integration.

## Features (Community Tier)

- **Core data models** — Task, schedule configuration, task templates, priority enums
- **Service layer** — SchedulerService unified high-level facade
- **License integration** — Vinzy license gate for feature tiers

Pro and Enterprise tiers unlock advanced workflow orchestration, SQLite persistence, and the full async execution engine with concurrency control.

Visit [gozerai.com/pricing](https://gozerai.com/pricing) for tier details.

## Installation

```
pip install taskpilot
```

For development:

```
pip install -e ".[dev]"
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

## Links

- **Pricing & Licensing**: https://gozerai.com/pricing
- **Documentation**: https://gozerai.com/docs/taskpilot
