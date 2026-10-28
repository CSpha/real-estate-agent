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


class _StopWatching(Exception):
    pass


def _run_watchdog(health_results):
    results = iter(health_results)

    def is_healthy(engine):
        result = next(results)
        if isinstance(result, Exception):
            raise result
        return result

    exit_process = MagicMock()
    sleep_calls = MagicMock(side_effect=[None] * len(health_results) + [_StopWatching()])
    try:
        scheduler.watch_deadline(
            engine_factory=MagicMock(),
            is_healthy=is_healthy,
            sleep=sleep_calls,
            exit_process=exit_process,
            misses_before_exit=2,
        )
    except _StopWatching:
        pass
    return exit_process


def test_watchdog_exits_after_consecutive_missed_deadlines():
    exit_process = _run_watchdog([True, False, False])
    exit_process.assert_called_once_with(1)


def test_watchdog_resets_after_a_healthy_check():
    exit_process = _run_watchdog([False, True, False, True])
    exit_process.assert_not_called()


def test_watchdog_does_not_exit_when_history_is_unreadable():
    exit_process = _run_watchdog([RuntimeError("db down"), False, RuntimeError("db down")])
    exit_process.assert_not_called()


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
    monkeypatch.setattr(scheduler, "get_engine", MagicMock())
    monkeypatch.setattr(
        scheduler, "rentcast_budget_allows", MagicMock(return_value=(True, "ok"))
    )

    scheduler.run_scheduled_shadow_pipeline()
    run_pipeline.assert_called_once_with(skip_rentcast=False)


def test_scheduled_run_skips_rentcast_when_budget_denies(monkeypatch):
    monkeypatch.setenv("SHADOW_ENABLE_RENTCAST_API_CALLS", "true")
    run_pipeline = MagicMock(return_value={"status": "succeeded"})
    monkeypatch.setattr(scheduler, "run_shadow_pipeline", run_pipeline)
    monkeypatch.setattr(scheduler, "get_engine", MagicMock())
    monkeypatch.setattr(
        scheduler,
        "rentcast_budget_allows",
        MagicMock(return_value=(False, "monthly budget reached")),
    )

    scheduler.run_scheduled_shadow_pipeline()
    run_pipeline.assert_called_once_with(skip_rentcast=True)


def _usage_engine(attempts, last_attempt):
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.mappings.return_value.one.return_value = {
        "attempts_this_month": attempts,
        "last_attempt_at": last_attempt,
    }
    return engine


NOW = datetime(2026, 10, 31, 18, 0, tzinfo=timezone.utc)


def test_rentcast_budget_allows_a_spaced_run_under_the_cap(monkeypatch):
    monkeypatch.delenv("SHADOW_RENTCAST_MONTHLY_REQUEST_LIMIT", raising=False)
    allowed, _ = scheduler.rentcast_budget_allows(
        engine=_usage_engine(10, NOW - timedelta(hours=72)), now=NOW
    )
    assert allowed


def test_rentcast_budget_blocks_retries_within_minimum_spacing():
    allowed, reason = scheduler.rentcast_budget_allows(
        engine=_usage_engine(1, NOW - timedelta(hours=6)), now=NOW
    )
    assert not allowed
    assert "within 60 hours" in reason


def test_rentcast_budget_blocks_a_run_that_would_exceed_the_cap(monkeypatch):
    monkeypatch.setenv("SHADOW_RENTCAST_MONTHLY_REQUEST_LIMIT", "40")
    allowed, reason = scheduler.rentcast_budget_allows(
        engine=_usage_engine(13, NOW - timedelta(days=5)), now=NOW
    )
    assert not allowed
    assert "39 of 40" in reason


def test_rentcast_budget_counts_from_the_utc_month_start():
    engine = _usage_engine(0, None)
    scheduler.rentcast_budget_allows(engine=engine, now=NOW)
    params = engine.connect.return_value.__enter__.return_value.execute.call_args.args[1]
    assert params == {"month_start": datetime(2026, 10, 1, tzinfo=timezone.utc)}
