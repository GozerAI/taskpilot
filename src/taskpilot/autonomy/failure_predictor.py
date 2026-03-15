"""Task failure prediction using historical execution patterns.

Analyzes past execution outcomes to predict which tasks are likely to fail,
based on error rate history, resource contention, and timing patterns.
"""

import logging
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class FailureRisk:
    """Failure risk assessment for a single task."""
    task_id: str
    risk_score: float
    risk_level: str  # "low", "medium", "high", "critical"
    factors: Dict[str, float] = field(default_factory=dict)
    recommendation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "risk_score": round(self.risk_score, 4),
            "risk_level": self.risk_level,
            "factors": {k: round(v, 4) for k, v in self.factors.items()},
            "recommendation": self.recommendation,
        }


@dataclass
class PredictionResult:
    """Aggregated prediction results."""
    predictions: List[FailureRisk]
    high_risk_count: int = 0
    avg_risk_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_tasks": len(self.predictions),
            "high_risk_count": self.high_risk_count,
            "avg_risk_score": round(self.avg_risk_score, 4),
            "predictions": [p.to_dict() for p in self.predictions],
        }


_RISK_THRESHOLDS = {
    "low": (0.0, 0.25),
    "medium": (0.25, 0.50),
    "high": (0.50, 0.75),
    "critical": (0.75, 1.0),
}


class FailurePredictor:
    """Predicts task failure probability from historical patterns.

    Factors:
      - historical_error_rate: fraction of past failures for this task type
      - retry_ratio: how often retries were needed
      - resource_pressure: current resource utilization level
      - recency_weight: more recent failures weighted higher

    Weights are configurable.
    """

    DEFAULT_WEIGHTS = {
        "error_rate": 0.40,
        "retry_ratio": 0.20,
        "resource_pressure": 0.25,
        "recency": 0.15,
    }

    def __init__(self, weights: Optional[Dict[str, float]] = None):
        self._weights = weights or dict(self.DEFAULT_WEIGHTS)
        self._history: List[Dict[str, Any]] = []

    def record_outcome(self, task_id: str, task_type: str,
                       success: bool, retries: int = 0,
                       duration_ms: float = 0.0) -> None:
        """Record a task execution outcome for future predictions."""
        self._history.append({
            "task_id": task_id,
            "task_type": task_type,
            "success": success,
            "retries": retries,
            "duration_ms": duration_ms,
            "index": len(self._history),
        })

    def _error_rate(self, task_type: str) -> float:
        matching = [h for h in self._history if h["task_type"] == task_type]
        if not matching:
            return 0.0
        failures = sum(1 for h in matching if not h["success"])
        return failures / len(matching)

    def _retry_ratio(self, task_type: str) -> float:
        matching = [h for h in self._history if h["task_type"] == task_type]
        if not matching:
            return 0.0
        retried = sum(1 for h in matching if h["retries"] > 0)
        return retried / len(matching)

    def _recency_score(self, task_type: str) -> float:
        matching = [h for h in self._history if h["task_type"] == task_type]
        if not matching:
            return 0.0
        total = len(self._history)
        if total <= 1:
            return 0.0
        recent_failures = 0
        recent_window = matching[-min(10, len(matching)):]
        for h in recent_window:
            if not h["success"]:
                recency_factor = (h["index"] + 1) / total
                recent_failures += recency_factor
        return min(1.0, recent_failures / len(recent_window))

    def _classify_risk(self, score: float) -> str:
        for level, (lo, hi) in _RISK_THRESHOLDS.items():
            if lo <= score < hi:
                return level
        return "critical"

    def _recommend(self, risk_level: str, factors: Dict[str, float]) -> str:
        if risk_level == "low":
            return "No action needed"
        if risk_level == "medium":
            if factors.get("resource_pressure", 0) > 0.3:
                return "Consider reducing concurrent load"
            return "Monitor closely; consider adding retry capacity"
        if risk_level == "high":
            if factors.get("error_rate", 0) > 0.4:
                return "Investigate root cause of frequent failures"
            return "Add circuit breaker or reduce batch size"
        return "Critical risk: pause and investigate before proceeding"

    def predict(self, tasks: List[Dict[str, Any]],
                resource_utilization: float = 0.0) -> PredictionResult:
        """Predict failure risk for a list of tasks.

        Each task dict should have:
          - task_id: str
          - task_type: str
          - resource_pressure: float (optional, 0-1, overrides global)
        """
        predictions: List[FailureRisk] = []

        for task in tasks:
            task_id = task["task_id"]
            task_type = task.get("task_type", "unknown")
            pressure = task.get("resource_pressure", resource_utilization)

            error = self._error_rate(task_type)
            retry = self._retry_ratio(task_type)
            recency = self._recency_score(task_type)

            factors = {
                "error_rate": error,
                "retry_ratio": retry,
                "resource_pressure": pressure,
                "recency": recency,
            }

            score = sum(
                self._weights[k] * v for k, v in factors.items()
            )
            score = min(1.0, max(0.0, score))
            level = self._classify_risk(score)

            predictions.append(FailureRisk(
                task_id=task_id,
                risk_score=score,
                risk_level=level,
                factors=factors,
                recommendation=self._recommend(level, factors),
            ))

        high_risk = sum(1 for p in predictions if p.risk_level in ("high", "critical"))
        avg = sum(p.risk_score for p in predictions) / len(predictions) if predictions else 0.0

        return PredictionResult(
            predictions=predictions,
            high_risk_count=high_risk,
            avg_risk_score=avg,
        )
