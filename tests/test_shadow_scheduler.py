from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import app.shadow.scheduler as scheduler
from app.shadow.scheduler import next_run_at


def engine_with(row):
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.mappings.return_value.first.return_value = row
    return engine


@pytest.mark.parametrize(
    "status,hours", [("succeeded", 72), ("failed", 6), ("running", 6)]
)
def test_restart_recovers_deadline_from_history(tmp_path, status, hours):
    started = datetime(2026, 9, 9, tzinfo=timezone.utc)
    finished = None if status == "running" else started + timedelta(minutes=1)
    engine = engine_with(dict(status=status, started_at=started, finished_at=finished))
    state = tmp_path / "due.txt"
    first = next_run_at(engine, 72 * 3600, 6 * 3600, 168 * 3600, state)
    second = next_run_at(engine, 72 * 3600, 6 * 3600, 168 * 3600, state)
    assert first == second == (finished or started) + timedelta(hours=hours)
    assert not state.exists()


def test_initial_deadline_survives_restart_without_run_history(tmp_path):
    engine = engine_with(None)
    state = tmp_path / "data" / "due.txt"
    first = next_run_at(engine, 72 * 3600, 6 * 3600, 3600, state)
    assert next_run_at(engine, 72 * 3600, 6 * 3600, 3600, state) == first


def test_interval_change_uses_last_completion(tmp_path):
    finished = datetime(2026, 9, 9, tzinfo=timezone.utc)
    engine = engine_with(
        dict(status="succeeded", started_at=finished, finished_at=finished)
    )
    state = tmp_path / "due.txt"
    assert next_run_at(engine, 72 * 3600, 21600, 0, state) == finished + timedelta(
        days=3
    )


def test_sleep_until_rechecks_wall_time_after_short_intervals():
    start = datetime(2026, 9, 18, tzinfo=timezone.utc)
    clock = iter(
        [
            start,
            start + timedelta(seconds=60),
            start + timedelta(seconds=120),
            start + timedelta(seconds=125),
        ]
    )
    sleep = MagicMock()

    scheduler.sleep_until(
        start + timedelta(seconds=125),
        now=lambda: next(clock),
        sleep=sleep,
        maximum_sleep_seconds=60,
    )

    assert [call.args[0] for call in sleep.call_args_list] == [60, 60, 5]


def test_scheduled_run_disables_rentcast_calls_by_default(monkeypatch):
    monkeypatch.delenv("SHADOW_ENABLE_RENTCAST_API_CALLS", raising=False)
    run_pipeline = MagicMock(return_value={"status": "succeeded"})
    monkeypatch.setattr(scheduler, "run_shadow_pipeline", run_pipeline)

    assert scheduler.run_scheduled_shadow_pipeline() == {"status": "succeeded"}
    run_pipeline.assert_called_once_with(skip_rentcast=True)


def test_scheduled_run_requires_explicit_rentcast_opt_in(monkeypatch):
    monkeypatch.setenv("SHADOW_ENABLE_RENTCAST_API_CALLS", "true")
    run_pipeline = MagicMock(return_value={"status": "succeeded"})
    monkeypatch.setattr(scheduler, "run_shadow_pipeline", run_pipeline)

    scheduler.run_scheduled_shadow_pipeline()
    run_pipeline.assert_called_once_with(skip_rentcast=False)
