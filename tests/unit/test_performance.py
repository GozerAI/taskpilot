"""Tests for taskpilot.performance modules."""

import time

import pytest

from taskpilot.performance.cache import TaskCache, CacheEntry, CacheStats
from taskpilot.performance.async_pipeline import (
    AsyncPipeline, PipelineStage, PipelineResult, PipelineStats,
)
from taskpilot.performance.batch_scheduler import (
    BatchScheduler, BatchJob, BatchResult, BatchStatus,
)
from taskpilot.performance.priority_optimizer import (
    PriorityOptimizer, ScoredTask, OptimizationResult,
)


class TestTaskCache:
    def test_put_and_get(self):
        cache = TaskCache()
        cache.put("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_miss_returns_none(self):
        cache = TaskCache()
        assert cache.get("missing") is None

    def test_expired_entry(self):
        cache = TaskCache(default_ttl=0.01)
        cache.put("key1", "value1")
        time.sleep(0.02)
        assert cache.get("key1") is None

    def test_invalidate(self):
        cache = TaskCache()
        cache.put("key1", "value1")
        assert cache.invalidate("key1")
        assert cache.get("key1") is None

    def test_invalidate_by_prefix(self):
        cache = TaskCache()
        cache.put("task:1", "a")
        cache.put("task:2", "b")
        cache.put("other:1", "c")
        count = cache.invalidate_by_prefix("task:")
        assert count == 2
        assert cache.get("other:1") == "c"

    def test_get_or_compute(self):
        cache = TaskCache()
        val = cache.get_or_compute("computed", lambda: 42)
        assert val == 42
        assert cache.get("computed") == 42

    def test_lru_eviction(self):
        cache = TaskCache(max_size=3)
        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("c", 3)
        cache.put("d", 4)
        assert cache.get("a") is None
        assert cache.get("d") == 4

    def test_clear(self):
        cache = TaskCache()
        cache.put("a", 1)
        cache.put("b", 2)
        count = cache.clear()
        assert count == 2
        assert cache.get("a") is None

    def test_stats(self):
        cache = TaskCache()
        cache.put("k", "v")
        cache.get("k")
        cache.get("miss")
        stats = cache.get_stats()
        assert stats.hits == 1
        assert stats.misses == 1
        assert stats.hit_rate == 50.0


class TestAsyncPipeline:
    def test_process_single_item(self):
        pipe = AsyncPipeline(name="test")
        pipe.add_stage("double", lambda d: {"val": d["val"] * 2})
        result = pipe.process({"val": 5})
        assert result.status == "completed"
        assert result.output["val"] == 10

    def test_multi_stage(self):
        pipe = AsyncPipeline()
        pipe.add_stage("add1", lambda d: {"v": d["v"] + 1})
        pipe.add_stage("mul2", lambda d: {"v": d["v"] * 2})
        result = pipe.process({"v": 3})
        assert result.output["v"] == 8

    def test_stage_failure(self):
        pipe = AsyncPipeline()
        pipe.add_stage("fail", lambda d: 1/0, max_retries=0)
        result = pipe.process({"val": 1})
        assert result.status == "failed"
        assert result.error is not None

    def test_retry_then_succeed(self):
        attempts = [0]
        def flaky(d):
            attempts[0] += 1
            if attempts[0] < 2:
                raise RuntimeError("flaky")
            return d
        pipe = AsyncPipeline()
        pipe.add_stage("flaky", flaky, max_retries=2)
        result = pipe.process({"v": 1})
        assert result.status == "completed"

    def test_process_batch(self):
        pipe = AsyncPipeline()
        pipe.add_stage("pass", lambda d: d)
        results = pipe.process_batch([{"i": 1}, {"i": 2}])
        assert len(results) == 2
        assert all(r.status == "completed" for r in results)

    def test_dead_letter(self):
        pipe = AsyncPipeline()
        pipe.add_stage("fail", lambda d: 1/0, max_retries=0)
        pipe.process({"v": 1})
        assert pipe.dead_letter_count == 1
        items = pipe.drain_dead_letter()
        assert len(items) == 1
        assert pipe.dead_letter_count == 0

    def test_stats_tracking(self):
        pipe = AsyncPipeline()
        pipe.add_stage("ok", lambda d: d)
        pipe.process({})
        stats = pipe.get_stats()
        assert stats.total_processed == 1
        assert stats.successful == 1


class TestBatchScheduler:
    def test_create_batches(self):
        sched = BatchScheduler(batch_size=2)
        tasks = [{"task_id": str(i)} for i in range(5)]
        batches = sched.create_batches(tasks)
        assert len(batches) == 3
        assert len(batches[0].tasks) == 2
        assert len(batches[2].tasks) == 1

    def test_execute_batch_success(self):
        sched = BatchScheduler()
        batch = BatchJob(tasks=[{"id": "1"}, {"id": "2"}])
        result = sched.execute_batch(batch, lambda t: {"done": True})
        assert result.status == BatchStatus.COMPLETED
        assert result.success_count == 2

    def test_execute_batch_partial(self):
        count = [0]
        def half_fail(t):
            count[0] += 1
            if count[0] % 2 == 0:
                raise RuntimeError("fail")
            return {"ok": True}
        sched = BatchScheduler()
        batch = BatchJob(tasks=[{"task_id": "1"}, {"task_id": "2"}])
        result = sched.execute_batch(batch, half_fail)
        assert result.status == BatchStatus.PARTIAL

    def test_run_with_retry(self):
        fail_set = set(["2"])
        attempt = [0]
        def executor(t):
            tid = t.get("task_id", "")
            if tid in fail_set:
                fail_set.discard(tid)
                raise RuntimeError("first fail")
            return {"done": True}
        sched = BatchScheduler(batch_size=5, retry_failed=True)
        tasks = [{"task_id": str(i)} for i in range(3)]
        result = sched.run(tasks, executor)
        assert result.total_tasks == 3

    def test_history(self):
        sched = BatchScheduler(batch_size=10)
        sched.run([{"task_id": "1"}], lambda t: t)
        assert len(sched.history) >= 1
        sched.clear_history()
        assert len(sched.history) == 0


class TestPriorityOptimizer:
    def test_optimize_basic(self):
        opt = PriorityOptimizer()
        tasks = [
            {"task_id": "low", "priority": 1},
            {"task_id": "high", "priority": 10},
            {"task_id": "mid", "priority": 5},
        ]
        result = opt.optimize(tasks)
        assert isinstance(result, OptimizationResult)
        assert result.ordered_tasks[0].task_id == "high"

    def test_urgency_scoring(self):
        opt = PriorityOptimizer()
        import time as t
        now = t.time()
        tasks = [
            {"task_id": "urgent", "priority": 5, "deadline": now + 60},
            {"task_id": "relaxed", "priority": 5, "deadline": now + 86400 * 30},
        ]
        result = opt.optimize(tasks)
        urgent = next(s for s in result.ordered_tasks if s.task_id == "urgent")
        relaxed = next(s for s in result.ordered_tasks if s.task_id == "relaxed")
        assert urgent.urgency_score > relaxed.urgency_score

    def test_dependency_scoring(self):
        opt = PriorityOptimizer()
        tasks = [
            {"task_id": "base", "priority": 5},
            {"task_id": "dep1", "priority": 5, "depends_on": ["base"]},
            {"task_id": "dep2", "priority": 5, "depends_on": ["base"]},
        ]
        result = opt.optimize(tasks)
        base = next(s for s in result.ordered_tasks if s.task_id == "base")
        assert base.dependency_score > 0

    def test_optimization_result_to_dict(self):
        opt = PriorityOptimizer()
        result = opt.optimize([{"task_id": "a", "priority": 5}])
        d = result.to_dict()
        assert "task_count" in d
        assert "optimization_time_ms" in d
