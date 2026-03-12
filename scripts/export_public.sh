#!/usr/bin/env bash
# export_public.sh — Creates a clean public export of Taskpilot for GozerAI/taskpilot.
# Usage: bash scripts/export_public.sh [target_dir]
#
# Strips proprietary Pro/Enterprise modules and internal infrastructure,
# leaving only community-tier code + the license gate (so users see the upgrade path).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${1:-${REPO_ROOT}/../taskpilot-public-export}"

echo "=== Taskpilot Public Export ==="
echo "Source: ${REPO_ROOT}"
echo "Target: ${TARGET}"

# Clean target
rm -rf "${TARGET}"
mkdir -p "${TARGET}"

# Use git archive to get a clean copy (respects .gitignore, excludes .git)
cd "${REPO_ROOT}"
git archive HEAD | tar -x -C "${TARGET}"

# ===== STRIP PROPRIETARY MODULES =====

# Pro tier — advanced workflow automation
rm -f "${TARGET}/src/taskpilot/workflows.py"
rm -f "${TARGET}/src/taskpilot/persistence.py"

# Enterprise tier — enterprise task orchestration engine
rm -f "${TARGET}/src/taskpilot/engine.py"

# ===== STRIP TESTS FOR PROPRIETARY MODULES =====
rm -f "${TARGET}/tests/unit/test_persistence.py"

# ===== CREATE STUB FILES FOR STRIPPED MODULES =====

for mod in workflows persistence engine; do
    cat > "${TARGET}/src/taskpilot/${mod}.py" << 'PYEOF'
"""This module requires a commercial license.

Visit https://gozerai.com/pricing for Pro and Enterprise tier details.
Set VINZY_LICENSE_KEY to unlock licensed features.
"""

raise ImportError(
    f"{__name__} requires a commercial Taskpilot license. "
    "Visit https://gozerai.com/pricing for details."
)
PYEOF
done

# ===== SANITIZE README =====
cat > "${TARGET}/README.md" << 'MDEOF'
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
MDEOF

echo ""
echo "=== Export complete: ${TARGET} ==="
echo ""
echo "Community-tier modules included:"
echo "  __init__, app, core, licensing, service"
echo ""
echo "Stripped (Pro/Enterprise):"
echo "  workflows, persistence, engine"
