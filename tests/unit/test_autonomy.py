"""Tests for taskpilot.autonomy modules."""

from datetime import datetime, timedelta, timezone

import pytest

from taskpilot.autonomy.schedule_optimizer import (
    ScheduleOptimizer, TimeSlot, SchedulePlan, ScheduleConflict,
)
from taskpilot.autonomy.resource_allocator import (
    ResourceAllocator, Resource, AllocationRequest, AllocationResult,
)
from taskpilot.autonomy.failure_predictor import (
    FailurePredictor, FailureRisk, PredictionResult,
)
from taskpilot.autonomy.dependency_resolver import (
    DependencyResolver, DependencyNode, ExecutionPlan, CycleError,
)


class TestScheduleOptimizer:
    def setup_method(self):
        self.opt = ScheduleOptimizer(work_start_hour=9, work_end_hour=17)

    def test_basic_scheduling(self):
        start = datetime(2026, 3, 13, 9, 0, tzinfo=timezone.utc)
        tasks = [
            {"task_id": "T1", "duration_minutes": 60},
            {"task_id": "T2", "duration_minutes": 30},
        ]
        plan = self.opt.optimize(tasks, start_from=start)
        assert plan.scheduled_count == 2
        assert len(plan.unscheduled) == 0

    def test_deadline_conflict(self):
        start = datetime(2026, 3, 13, 16, 0, tzinfo=timezone.utc)
        tasks = [
            {"task_id": "T1", "duration_minutes": 120,
             "deadline": datetime(2026, 3, 13, 17, 0, tzinfo=timezone.utc)},
        ]
        plan = self.opt.optimize(tasks, start_from=start)
        assert len(plan.conflicts) > 0

    def test_dependency_ordering(self):
        start = datetime(2026, 3, 13, 9, 0, tzinfo=timezone.utc)
        tasks = [
            {"task_id": "T1", "duration_minutes": 30},
            {"task_id": "T2", "duration_minutes": 30, "depends_on": ["T1"]},
        ]
        plan = self.opt.optimize(tasks, start_from=start)
        t1_slot = next(s for s in plan.slots if s.task_id == "T1")
        t2_slot = next(s for s in plan.slots if s.task_id == "T2")
        assert t2_slot.start >= t1_slot.end

    def test_to_dict(self):
        start = datetime(2026, 3, 13, 9, 0, tzinfo=timezone.utc)
        plan = self.opt.optimize([{"task_id": "T1", "duration_minutes": 30}], start)
        d = plan.to_dict()
        assert "scheduled_count" in d

    def test_time_slot_overlaps(self):
        s1 = TimeSlot("A", datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc),
                      datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc))
        s2 = TimeSlot("B", datetime(2026, 1, 1, 9, 30, tzinfo=timezone.utc),
                      datetime(2026, 1, 1, 10, 30, tzinfo=timezone.utc))
        assert s1.overlaps(s2)


class TestResourceAllocator:
    def setup_method(self):
        self.alloc = ResourceAllocator()
        self.alloc.add_resource(Resource("cpu", "CPU Slots", 100.0))
        self.alloc.add_resource(Resource("mem", "Memory GB", 64.0))

    def test_allocate_success(self):
        req = AllocationRequest("task1", {"cpu": 10.0, "mem": 8.0})
        result = self.alloc.allocate(req)
        assert result.allocated
        assert result.assignments == {"cpu": 10.0, "mem": 8.0}

    def test_allocate_insufficient(self):
        req = AllocationRequest("task1", {"cpu": 200.0})
        result = self.alloc.allocate(req)
        assert not result.allocated
        assert len(result.insufficient) > 0

    def test_release(self):
        req = AllocationRequest("task1", {"cpu": 50.0})
        self.alloc.allocate(req)
        cpu = self.alloc.get_resource("cpu")
        assert cpu.available == 50.0
        self.alloc.release("task1")
        assert cpu.available == 100.0

    def test_batch_allocate(self):
        reqs = [
            AllocationRequest("t1", {"cpu": 10.0}, priority=10),
            AllocationRequest("t2", {"cpu": 10.0}, priority=1),
        ]
        results = self.alloc.batch_allocate(reqs)
        assert len(results) == 2
        assert all(r.allocated for r in results)

    def test_utilization_summary(self):
        self.alloc.allocate(AllocationRequest("t1", {"cpu": 50.0}))
        summary = self.alloc.utilization_summary()
        assert summary["total_resources"] == 2
        assert summary["active_allocations"] == 1

    def test_resource_not_found(self):
        req = AllocationRequest("t1", {"gpu": 1.0})
        result = self.alloc.allocate(req)
        assert not result.allocated


class TestFailurePredictor:
    def setup_method(self):
        self.predictor = FailurePredictor()

    def test_predict_no_history(self):
        tasks = [{"task_id": "t1", "task_type": "etl"}]
        result = self.predictor.predict(tasks)
        assert len(result.predictions) == 1
        assert result.predictions[0].risk_level == "low"

    def test_predict_with_failures(self):
        for i in range(10):
            self.predictor.record_outcome(f"t{i}", "etl",
                                          success=(i % 2 == 0), retries=1 if i % 2 else 0)
        result = self.predictor.predict([{"task_id": "new", "task_type": "etl"}])
        assert result.predictions[0].risk_score > 0

    def test_predict_high_risk(self):
        for i in range(10):
            self.predictor.record_outcome(f"t{i}", "bad_type", success=False, retries=2)
        result = self.predictor.predict(
            [{"task_id": "x", "task_type": "bad_type"}],
            resource_utilization=0.9,
        )
        assert result.high_risk_count >= 1

    def test_prediction_result_to_dict(self):
        result = self.predictor.predict([{"task_id": "t1", "task_type": "a"}])
        d = result.to_dict()
        assert "total_tasks" in d
        assert "predictions" in d


class TestDependencyResolver:
    def setup_method(self):
        self.resolver = DependencyResolver()

    def test_linear_chain(self):
        self.resolver.add_task("A")
        self.resolver.add_task("B", depends_on=["A"])
        self.resolver.add_task("C", depends_on=["B"])
        plan = self.resolver.resolve()
        assert plan.tier_count == 3
        assert plan.tiers[0] == ["A"]
        assert plan.tiers[1] == ["B"]
        assert plan.tiers[2] == ["C"]

    def test_parallel_tasks(self):
        self.resolver.add_task("A")
        self.resolver.add_task("B")
        self.resolver.add_task("C")
        plan = self.resolver.resolve()
        assert plan.tier_count == 1
        assert plan.max_parallelism == 3

    def test_diamond_dependency(self):
        self.resolver.add_task("A")
        self.resolver.add_task("B", depends_on=["A"])
        self.resolver.add_task("C", depends_on=["A"])
        self.resolver.add_task("D", depends_on=["B", "C"])
        plan = self.resolver.resolve()
        assert plan.tier_count == 3
        assert "D" in plan.tiers[2]

    def test_cycle_detection(self):
        self.resolver.add_task("A", depends_on=["C"])
        self.resolver.add_task("B", depends_on=["A"])
        self.resolver.add_task("C", depends_on=["B"])
        with pytest.raises(CycleError):
            self.resolver.resolve()

    def test_missing_dependency(self):
        self.resolver.add_task("A", depends_on=["MISSING"])
        plan = self.resolver.resolve()
        assert "MISSING" in plan.missing_deps

    def test_add_tasks_bulk(self):
        count = self.resolver.add_tasks([
            {"task_id": "X"}, {"task_id": "Y", "depends_on": ["X"]},
        ])
        assert count == 2
        assert self.resolver.task_count == 2

    def test_get_dependents(self):
        self.resolver.add_task("A")
        self.resolver.add_task("B", depends_on=["A"])
        self.resolver.add_task("C", depends_on=["A"])
        deps = self.resolver.get_dependents("A")
        assert set(deps) == {"B", "C"}

    def test_clear(self):
        self.resolver.add_task("A")
        self.resolver.clear()
        assert self.resolver.task_count == 0

    def test_execution_plan_to_dict(self):
        self.resolver.add_task("A")
        plan = self.resolver.resolve()
        d = plan.to_dict()
        assert "tiers" in d
        assert "total_tasks" in d
