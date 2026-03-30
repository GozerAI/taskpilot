"""
Workflow Definitions - Multi-step autonomous workflows.

Provides workflow orchestration for complex multi-step
autonomous operations.
"""

import asyncio
import inspect
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

from taskpilot.core import (
    ExecutionResult,
    TaskStatus,
)

logger = logging.getLogger(__name__)


class StepStatus(Enum):
    """Status of a workflow step."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class WorkflowStatus(Enum):
    """Status of a workflow execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"  # Some steps failed
    CANCELLED = "cancelled"


@dataclass
class WorkflowStep:
    """A single step in a workflow."""
    id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    description: str = ""
    handler_name: str = ""
    handler_params: Dict[str, Any] = field(default_factory=dict)

    # Flow control
    depends_on: List[str] = field(default_factory=list)  # Step IDs this depends on
    continue_on_failure: bool = False
    timeout_seconds: int = 300

    # Execution
    status: StepStatus = StepStatus.PENDING
    result: Optional[ExecutionResult] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "handler_name": self.handler_name,
            "depends_on": self.depends_on,
            "status": self.status.value,
            "result": self.result.to_dict() if self.result else None,
        }


@dataclass
class WorkflowDefinition:
    """Definition of a multi-step workflow."""
    id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    description: str = ""
    version: str = "1.0"

    # Steps
    steps: List[WorkflowStep] = field(default_factory=list)

    # Configuration
    owner_executive: str = ""
    parallel_execution: bool = False  # Run independent steps in parallel
    stop_on_failure: bool = True

    # Metadata
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "steps_count": len(self.steps),
            "steps": [s.to_dict() for s in self.steps],
            "owner_executive": self.owner_executive,
            "parallel_execution": self.parallel_execution,
        }

    def add_step(self, step: WorkflowStep) -> None:
        """Add a step to the workflow."""
        self.steps.append(step)

    def get_step(self, step_id: str) -> Optional[WorkflowStep]:
        """Get a step by ID."""
        for step in self.steps:
            if step.id == step_id:
                return step
        return None

    def get_execution_order(self) -> List[List[WorkflowStep]]:
        """
        Get steps in execution order, grouped by parallel batches.

        Returns a list of step groups. Steps in the same group
        can run in parallel.
        """
        remaining = set(s.id for s in self.steps)
        completed = set()
        batches = []

        while remaining:
            # Find steps with all dependencies met
            ready = []
            for step in self.steps:
                if step.id in remaining:
                    deps_met = all(d in completed for d in step.depends_on)
                    if deps_met:
                        ready.append(step)

            if not ready:
                # Circular dependency or error
                logger.warning("Could not resolve remaining dependencies")
                break

            batches.append(ready)
            for step in ready:
                remaining.remove(step.id)
                completed.add(step.id)

        return batches


@dataclass
class WorkflowExecution:
    """Record of a workflow execution."""
    id: str = field(default_factory=lambda: str(uuid4()))
    workflow_id: str = ""
    workflow_name: str = ""
    status: WorkflowStatus = WorkflowStatus.PENDING

    # Progress
    total_steps: int = 0
    completed_steps: int = 0
    failed_steps: int = 0

    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Results
    step_results: Dict[str, ExecutionResult] = field(default_factory=dict)
    final_output: Any = None
    error: Optional[str] = None

    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "workflow_name": self.workflow_name,
            "status": self.status.value,
            "total_steps": self.total_steps,
            "completed_steps": self.completed_steps,
            "failed_steps": self.failed_steps,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "step_results": {k: v.to_dict() for k, v in self.step_results.items()},
            "error": self.error,
        }


class WorkflowExecutor:
    """
    Executes workflows with step orchestration.

    Handles sequential and parallel step execution,
    dependency resolution, and result aggregation.
    """

    def __init__(self, storage_path: Optional[str] = None):
        self._handlers: Dict[str, Callable] = {}
        self._workflows: Dict[str, WorkflowDefinition] = {}
        self._executions: List[WorkflowExecution] = []
        self._storage_path = Path(storage_path) if storage_path else None
        self._executions_file = (
            self._storage_path / "executions.json" if self._storage_path else None
        )
        if self._executions_file and self._executions_file.exists():
            self._load_executions()

    def register_handler(self, name: str, handler: Callable) -> None:
        """Register a step handler."""
        self._handlers[name] = handler

    def register_workflow(self, workflow: WorkflowDefinition) -> None:
        """Register a workflow definition."""
        self._workflows[workflow.id] = workflow
        logger.info(f"Registered workflow: {workflow.name}")

    def get_workflow(self, workflow_id: str) -> Optional[WorkflowDefinition]:
        """Get a workflow by ID."""
        return self._workflows.get(workflow_id)

    def list_workflows(self) -> List[WorkflowDefinition]:
        """List all registered workflows."""
        return list(self._workflows.values())

    async def execute_step(
        self,
        step: WorkflowStep,
        context: Dict[str, Any],
    ) -> ExecutionResult:
        """Execute a single workflow step."""
        result = ExecutionResult()
        step.status = StepStatus.RUNNING
        step.started_at = datetime.now(timezone.utc)

        try:
            handler = self._handlers.get(step.handler_name)
            if not handler:
                raise ValueError(f"Handler not found: {step.handler_name}")

            # Merge context with step params
            params = {**context, **step.handler_params}

            if inspect.iscoroutinefunction(handler):
                output = await asyncio.wait_for(
                    handler(**params),
                    timeout=step.timeout_seconds,
                )
            else:
                output = await asyncio.wait_for(
                    asyncio.to_thread(handler, **params),
                    timeout=step.timeout_seconds,
                )

            result.success = True
            result.output = output
            step.status = StepStatus.COMPLETED
            logger.info(f"Step completed: {step.name}")

        except Exception as e:
            result.success = False
            result.error = str(e)
            step.status = StepStatus.FAILED
            logger.error(f"Step failed: {step.name} - {e}")

        finally:
            result.completed_at = datetime.now(timezone.utc)
            result.duration_seconds = (
                result.completed_at - result.started_at
            ).total_seconds()
            step.result = result
            step.completed_at = datetime.now(timezone.utc)

        return result

    async def execute_workflow(
        self,
        workflow_id: str,
        initial_context: Optional[Dict[str, Any]] = None,
    ) -> WorkflowExecution:
        """
        Execute a workflow.

        Args:
            workflow_id: ID of the workflow to execute
            initial_context: Initial context data for steps

        Returns:
            WorkflowExecution record
        """
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow not found: {workflow_id}")

        execution = WorkflowExecution(
            workflow_id=workflow.id,
            workflow_name=workflow.name,
            status=WorkflowStatus.RUNNING,
            total_steps=len(workflow.steps),
            started_at=datetime.now(timezone.utc),
        )

        context = initial_context or {}

        try:
            batches = workflow.get_execution_order()

            for batch in batches:
                if workflow.parallel_execution:
                    # Run batch in parallel
                    tasks = [
                        self.execute_step(step, context)
                        for step in batch
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for step, result in zip(batch, results):
                        if isinstance(result, Exception):
                            execution.failed_steps += 1
                            step.status = StepStatus.FAILED
                            step.result = ExecutionResult(
                                success=False,
                                error=str(result),
                            )
                        else:
                            execution.step_results[step.id] = result
                            if result.success:
                                execution.completed_steps += 1
                                # Add output to context
                                if result.output:
                                    context[f"step_{step.id}_output"] = result.output
                            else:
                                execution.failed_steps += 1

                else:
                    # Run sequentially
                    for step in batch:
                        result = await self.execute_step(step, context)
                        execution.step_results[step.id] = result

                        if result.success:
                            execution.completed_steps += 1
                            if result.output:
                                context[f"step_{step.id}_output"] = result.output
                        else:
                            execution.failed_steps += 1

                            if workflow.stop_on_failure and not step.continue_on_failure:
                                raise Exception(f"Step failed: {step.name}")

            # Determine final status
            if execution.failed_steps == 0:
                execution.status = WorkflowStatus.COMPLETED
            elif execution.completed_steps > 0:
                execution.status = WorkflowStatus.PARTIAL
            else:
                execution.status = WorkflowStatus.FAILED

            # Get final output from last successful step
            if workflow.steps:
                last_step = workflow.steps[-1]
                if last_step.result and last_step.result.success:
                    execution.final_output = last_step.result.output

        except Exception as e:
            execution.status = WorkflowStatus.FAILED
            execution.error = str(e)
            logger.error(f"Workflow failed: {workflow.name} - {e}")

        finally:
            execution.completed_at = datetime.now(timezone.utc)
            self._executions.append(execution)
            self._save_executions()

        return execution

    def get_executions(
        self,
        workflow_id: Optional[str] = None,
        limit: int = 10,
    ) -> List[WorkflowExecution]:
        """Get workflow executions."""
        executions = self._executions
        if workflow_id:
            executions = [e for e in executions if e.workflow_id == workflow_id]

        return sorted(
            executions,
            key=lambda e: e.started_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )[:limit]

    def get_stats(self) -> Dict[str, Any]:
        """Get executor statistics."""
        return {
            "registered_workflows": len(self._workflows),
            "registered_handlers": len(self._handlers),
            "total_executions": len(self._executions),
            "by_status": {
                status.value: len([e for e in self._executions if e.status == status])
                for status in WorkflowStatus
            },
        }

    def _save_executions(self) -> None:
        """Persist execution history to disk."""
        if not self._executions_file:
            return
        try:
            self._executions_file.parent.mkdir(parents=True, exist_ok=True)
            data = [e.to_dict() for e in self._executions]
            with open(self._executions_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save executions: {e}")

    def _load_executions(self) -> None:
        """Load execution history from disk."""
        if not self._executions_file or not self._executions_file.exists():
            return
        try:
            with open(self._executions_file) as f:
                data = json.load(f)
            for entry in data:
                execution = WorkflowExecution(
                    id=entry["id"],
                    workflow_id=entry["workflow_id"],
                    workflow_name=entry["workflow_name"],
                    status=WorkflowStatus(entry["status"]),
                    total_steps=entry["total_steps"],
                    completed_steps=entry["completed_steps"],
                    failed_steps=entry["failed_steps"],
                    started_at=(
                        datetime.fromisoformat(entry["started_at"])
                        if entry.get("started_at")
                        else None
                    ),
                    completed_at=(
                        datetime.fromisoformat(entry["completed_at"])
                        if entry.get("completed_at")
                        else None
                    ),
                    error=entry.get("error"),
                )
                self._executions.append(execution)
            logger.info(f"Loaded {len(data)} execution records from disk")
        except Exception as e:
            logger.error(f"Failed to load executions: {e}")


# Pre-defined workflow templates
def create_daily_operations_workflow() -> WorkflowDefinition:
    """Create the daily operations workflow."""
    workflow = WorkflowDefinition(
        name="Daily Operations Workflow",
        description="Complete daily operational analysis and reporting",
        owner_executive="COO",
        parallel_execution=True,
    )

    # Step 1: Collect trends
    workflow.add_step(WorkflowStep(
        id="trend_scan",
        name="Trend Scan",
        description="Collect and analyze trends",
        handler_name="trend_service.run_autonomous_analysis",
    ))

    # Step 2: Commerce analysis (parallel with trends)
    workflow.add_step(WorkflowStep(
        id="commerce_analysis",
        name="Commerce Analysis",
        description="Analyze commerce operations",
        handler_name="commerce_service.run_autonomous_analysis",
    ))

    # Step 3: Generate reports (depends on both above)
    workflow.add_step(WorkflowStep(
        id="generate_reports",
        name="Generate Executive Reports",
        description="Generate reports for all executives",
        handler_name="generate_executive_reports",
        depends_on=["trend_scan", "commerce_analysis"],
    ))

    return workflow


def create_pricing_optimization_workflow() -> WorkflowDefinition:
    """Create pricing optimization workflow."""
    workflow = WorkflowDefinition(
        name="Pricing Optimization Workflow",
        description="Analyze and optimize product pricing",
        owner_executive="CRO",
    )

    workflow.add_step(WorkflowStep(
        id="margin_analysis",
        name="Margin Analysis",
        description="Analyze current margins",
        handler_name="commerce_service.get_margin_analysis",
    ))

    workflow.add_step(WorkflowStep(
        id="pricing_recommendations",
        name="Generate Pricing Recommendations",
        description="Generate pricing optimization recommendations",
        handler_name="commerce_service.optimize_pricing",
        depends_on=["margin_analysis"],
        handler_params={"target_margin": 40.0},
    ))

    return workflow


def create_brand_audit_workflow() -> WorkflowDefinition:
    """Create brand audit workflow."""
    workflow = WorkflowDefinition(
        name="Brand Audit Workflow",
        description="Comprehensive brand consistency audit",
        owner_executive="CMO",
    )

    workflow.add_step(WorkflowStep(
        id="brand_analysis",
        name="Brand Analysis",
        description="Analyze brand consistency",
        handler_name="brand_service.run_autonomous_analysis",
    ))

    return workflow
