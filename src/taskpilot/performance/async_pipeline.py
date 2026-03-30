"""Multi-stage async task processing pipeline.

Provides a configurable pipeline of stages that tasks flow through
sequentially, with retry support and statistics tracking.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)


class StageStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class PipelineStage:
    """Definition of a pipeline stage."""
    name: str
    handler: Callable[[Dict[str, Any]], Dict[str, Any]]
    max_retries: int = 2
    timeout_seconds: float = 30.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "max_retries": self.max_retries,
            "timeout_seconds": self.timeout_seconds,
        }


@dataclass
class PipelineResult:
    """Result of processing a single item through the pipeline."""
    item_id: str
    status: str = "completed"
    stages_completed: int = 0
    total_stages: int = 0
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    duration_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_id": self.item_id,
            "status": self.status,
            "stages_completed": self.stages_completed,
            "total_stages": self.total_stages,
            "output": self.output,
            "error": self.error,
            "duration_ms": round(self.duration_ms, 2),
        }


@dataclass
class PipelineStats:
    """Aggregated pipeline statistics."""
    total_processed: int = 0
    successful: int = 0
    failed: int = 0
    retries: int = 0
    avg_duration_ms: float = 0.0
    _durations: List[float] = field(default_factory=list, repr=False)

    def record(self, duration_ms: float, success: bool) -> None:
        self.total_processed += 1
        if success:
            self.successful += 1
        else:
            self.failed += 1
        self._durations.append(duration_ms)
        self.avg_duration_ms = sum(self._durations) / len(self._durations)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_processed": self.total_processed,
            "successful": self.successful,
            "failed": self.failed,
            "retries": self.retries,
            "avg_duration_ms": round(self.avg_duration_ms, 2),
        }


class AsyncPipeline:
    """Configurable multi-stage processing pipeline for tasks.

    Stages execute sequentially per item. Each stage receives the output
    of the previous stage. Failed stages are retried up to max_retries.
    """

    def __init__(self, name: str = "default"):
        self._name = name
        self._stages: List[PipelineStage] = []
        self._stats = PipelineStats()
        self._dead_letter: List[PipelineResult] = []

    @property
    def name(self) -> str:
        return self._name

    def add_stage(self, name: str, handler: Callable[[Dict[str, Any]], Dict[str, Any]],
                  max_retries: int = 2, timeout_seconds: float = 30.0) -> None:
        self._stages.append(PipelineStage(
            name=name, handler=handler,
            max_retries=max_retries, timeout_seconds=timeout_seconds,
        ))

    @property
    def stage_count(self) -> int:
        return len(self._stages)

    def process(self, item: Dict[str, Any],
                item_id: Optional[str] = None) -> PipelineResult:
        """Process a single item through all pipeline stages."""
        item_id = item_id or str(uuid4())
        start = time.monotonic()
        data = dict(item)
        stages_done = 0

        for stage in self._stages:
            attempts = 0
            while attempts <= stage.max_retries:
                try:
                    data = stage.handler(data)
                    stages_done += 1
                    break
                except Exception as e:
                    attempts += 1
                    if attempts <= stage.max_retries:
                        self._stats.retries += 1
                        logger.warning("Stage %s attempt %d failed: %s",
                                      stage.name, attempts, e)
                    else:
                        duration = (time.monotonic() - start) * 1000
                        result = PipelineResult(
                            item_id=item_id, status="failed",
                            stages_completed=stages_done,
                            total_stages=len(self._stages),
                            error=f"Stage {stage.name}: {e}",
                            duration_ms=duration,
                        )
                        self._stats.record(duration, False)
                        self._dead_letter.append(result)
                        return result

        duration = (time.monotonic() - start) * 1000
        result = PipelineResult(
            item_id=item_id, status="completed",
            stages_completed=stages_done,
            total_stages=len(self._stages),
            output=data, duration_ms=duration,
        )
        self._stats.record(duration, True)
        return result

    def process_batch(self, items: List[Dict[str, Any]]) -> List[PipelineResult]:
        """Process multiple items through the pipeline."""
        return [self.process(item) for item in items]

    def get_stats(self) -> PipelineStats:
        return self._stats

    @property
    def dead_letter_count(self) -> int:
        return len(self._dead_letter)

    def drain_dead_letter(self) -> List[PipelineResult]:
        """Return and clear dead-letter items."""
        items = list(self._dead_letter)
        self._dead_letter.clear()
        return items
