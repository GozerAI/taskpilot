"""Tests for SchedulerPersistence — enum mapping and field correctness."""

import pytest
from datetime import datetime, timezone

from taskpilot.core import (
    ScheduleConfig,
    ScheduleFrequency,
    ScheduledTask,
    TaskCategory,
    TaskPriority,
    TaskStatus,
)
from taskpilot.persistence import SchedulerPersistence


class TestPersistenceRoundTrip:
    """Verify tasks survive save → load without data loss."""

    @pytest.fixture
    async def db(self, tmp_path):
        p = SchedulerPersistence(db_path=str(tmp_path / "test.db"))
        await p.initialize()
        return p

    @pytest.mark.asyncio
    async def test_save_load_task_status_scheduled(self, db):
        """SCHEDULED status round-trips correctly."""
        task = ScheduledTask(
            name="Status Test",
            handler_name="test.handler",
            status=TaskStatus.SCHEDULED,
        )
        await db.save_task(task)
        loaded = await db.load_task(task.id)
        assert loaded is not None
        assert loaded.status == TaskStatus.SCHEDULED

    @pytest.mark.asyncio
    async def test_save_load_task_status_paused(self, db):
        """PAUSED status round-trips correctly."""
        task = ScheduledTask(
            name="Paused Test",
            handler_name="test.handler",
            status=TaskStatus.PAUSED,
            enabled=False,
        )
        await db.save_task(task)
        loaded = await db.load_task(task.id)
        assert loaded.status == TaskStatus.PAUSED

    @pytest.mark.asyncio
    async def test_execution_count_round_trip(self, db):
        """execution_count persists and loads correctly."""
        task = ScheduledTask(
            name="Count Test",
            handler_name="test.handler",
            execution_count=42,
        )
        await db.save_task(task)
        loaded = await db.load_task(task.id)
        assert loaded.execution_count == 42

    @pytest.mark.asyncio
    async def test_all_frequencies_round_trip(self, db):
        """All schedule frequencies survive persistence."""
        for freq in ScheduleFrequency:
            task = ScheduledTask(
                name=f"Freq {freq.value}",
                handler_name="test.handler",
                schedule=ScheduleConfig(frequency=freq),
            )
            await db.save_task(task)
            loaded = await db.load_task(task.id)
            assert loaded.schedule.frequency == freq, f"Failed for {freq}"

    @pytest.mark.asyncio
    async def test_all_statuses_round_trip(self, db):
        """All task statuses survive persistence."""
        for status in TaskStatus:
            task = ScheduledTask(
                name=f"Status {status.value}",
                handler_name="test.handler",
                status=status,
            )
            await db.save_task(task)
            loaded = await db.load_task(task.id)
            assert loaded.status == status, f"Failed for {status}"


class TestCronParsing:
    """Tests for cron expression scheduling."""

    def test_cron_every_15_minutes(self):
        """*/15 * * * * matches every 15 minutes."""
        config = ScheduleConfig(
            frequency=ScheduleFrequency.CUSTOM,
            cron_expression="*/15 * * * *",
        )
        base = datetime(2026, 3, 11, 10, 0, 0, tzinfo=timezone.utc)
        next_run = config.get_next_run(base)
        assert next_run.minute == 15
        assert next_run.hour == 10

    def test_cron_daily_at_3am(self):
        """0 3 * * * runs at 3:00 AM."""
        config = ScheduleConfig(
            frequency=ScheduleFrequency.CUSTOM,
            cron_expression="0 3 * * *",
        )
        base = datetime(2026, 3, 11, 10, 0, 0, tzinfo=timezone.utc)
        next_run = config.get_next_run(base)
        assert next_run.hour == 3
        assert next_run.minute == 0
        assert next_run.day == 12  # Next day since we're past 3am

    def test_cron_specific_hours(self):
        """0 9,17 * * * runs at 9am and 5pm."""
        config = ScheduleConfig(
            frequency=ScheduleFrequency.CUSTOM,
            cron_expression="0 9,17 * * *",
        )
        base = datetime(2026, 3, 11, 10, 0, 0, tzinfo=timezone.utc)
        next_run = config.get_next_run(base)
        assert next_run.hour == 17
        assert next_run.minute == 0

    def test_cron_fallback_on_invalid(self):
        """Invalid cron expression falls back to +1 hour."""
        config = ScheduleConfig(
            frequency=ScheduleFrequency.CUSTOM,
            cron_expression="bad",
        )
        base = datetime(2026, 3, 11, 10, 0, 0, tzinfo=timezone.utc)
        next_run = config.get_next_run(base)
        assert next_run == base + __import__("datetime").timedelta(hours=1)

    def test_custom_without_cron_falls_back(self):
        """CUSTOM frequency without cron_expression falls back to +1 hour."""
        config = ScheduleConfig(frequency=ScheduleFrequency.CUSTOM)
        base = datetime(2026, 3, 11, 10, 0, 0, tzinfo=timezone.utc)
        next_run = config.get_next_run(base)
        assert next_run == base + __import__("datetime").timedelta(hours=1)

    def test_cron_range(self):
        """30 9-17 * * * runs at :30 during business hours."""
        config = ScheduleConfig(
            frequency=ScheduleFrequency.CUSTOM,
            cron_expression="30 9-17 * * *",
        )
        base = datetime(2026, 3, 11, 8, 0, 0, tzinfo=timezone.utc)
        next_run = config.get_next_run(base)
        assert next_run.hour == 9
        assert next_run.minute == 30
