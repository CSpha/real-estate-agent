from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from sqlalchemy import text

from app.db import get_engine

from app.providers.rentcast import DEFAULT_MAX_RETRIES
from app.providers.sync_rentcast_shadow import PILOT_CITIES
from app.shadow.health import scheduler_is_healthy
from app.shadow.run_pipeline import run_shadow_pipeline


# One page per pilot city at PILOT_PAGE_SIZE. Runs record their actual request
# count; runs from before that was recorded used one request per city, and a
# run that failed before recording assumes every allowed retry was spent.
RENTCAST_REQUESTS_PER_RUN = len(PILOT_CITIES)
RENTCAST_MAX_REQUESTS_PER_RUN = len(PILOT_CITIES) * (1 + DEFAULT_MAX_RETRIES)
RENTCAST_USAGE_SQL = text(
    """
    SELECT
        COALESCE(
            SUM(
                CASE
                    WHEN details_json -> 'rentcast' ? 'request_count'
                        THEN (details_json -> 'rentcast' ->> 'request_count')::int
                    WHEN details_json ? 'rentcast' THEN :requests_per_run
                    ELSE :max_requests_per_run
                END
            ) FILTER (WHERE started_at >= :month_start),
            0
        ) AS requests_this_month,
        MAX(started_at) AS last_attempt_at
    FROM shadow_pipeline_runs
    WHERE source = 'rentcast'
      AND (details_json ? 'rentcast_requested' OR details_json ? 'rentcast')
    """
)


def rentcast_api_calls_enabled() -> bool:
    """Require an explicit opt-in before a scheduled run can call RentCast."""

    return os.getenv("SHADOW_ENABLE_RENTCAST_API_CALLS", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def rentcast_budget_allows(*, engine, now: datetime | None = None) -> tuple[bool, str]:
    """Keep scheduled RentCast calls inside the plan's monthly request allowance.

    The minimum spacing stops failure retries from re-fetching listings, and the
    calendar-month cap leaves headroom for manual audits and syncs.
    """
    now = now or datetime.now(timezone.utc)
    limit = _nonnegative_int("SHADOW_RENTCAST_MONTHLY_REQUEST_LIMIT", 40)
    min_hours = _positive_hours("SHADOW_RENTCAST_MIN_HOURS", 60)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    with engine.connect() as connection:
        row = (
            connection.execute(
                RENTCAST_USAGE_SQL,
                {
                    "month_start": month_start,
                    "requests_per_run": RENTCAST_REQUESTS_PER_RUN,
                    "max_requests_per_run": RENTCAST_MAX_REQUESTS_PER_RUN,
                },
            )
            .mappings()
            .one()
        )
    used = int(row["requests_this_month"] or 0)
    if used + RENTCAST_MAX_REQUESTS_PER_RUN > limit:
        return False, f"monthly budget reached ({used} of {limit} requests used)"
    last_attempt = row["last_attempt_at"]
    if last_attempt is not None and now - last_attempt < timedelta(hours=min_hours):
        return False, (
            f"listings were last requested at {last_attempt.isoformat()}, "
            f"within {min_hours} hours"
        )
    return True, f"{used} of {limit} monthly requests used before this run"


def run_scheduled_shadow_pipeline():
    rentcast_enabled = rentcast_api_calls_enabled()
    reason = "SHADOW_ENABLE_RENTCAST_API_CALLS is not set"
    if rentcast_enabled:
        rentcast_enabled, reason = rentcast_budget_allows(engine=get_engine())
    print(
        "Scheduled RentCast API calls are "
        f"{'enabled' if rentcast_enabled else 'disabled'}: {reason}.",
        flush=True,
    )
    return run_shadow_pipeline(skip_rentcast=not rentcast_enabled)


def _positive_hours(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    return value


def _nonnegative_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 0:
        raise ValueError(f"{name} cannot be negative")
    return value


def _nonnegative_hours(name: str, default: int) -> int:
    return _nonnegative_int(name, default)


def next_run_at(
    engine,
    interval_seconds: int,
    retry_seconds: int,
    initial_delay_seconds: int,
    state_path: Path,
) -> datetime:
    """Recover the deadline from durable run history, including interrupted runs."""
    with engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    "SELECT status, started_at, finished_at FROM shadow_pipeline_runs "
                    "WHERE source = 'rentcast' ORDER BY started_at DESC, id DESC LIMIT 1"
                )
            )
            .mappings()
            .first()
        )
    if row is not None:
        delay = interval_seconds if row["status"] == "succeeded" else retry_seconds
        return (row["finished_at"] or row["started_at"]) + timedelta(seconds=delay)
    if state_path.exists():
        return datetime.fromisoformat(state_path.read_text().strip())
    deadline = datetime.now(timezone.utc) + timedelta(seconds=initial_delay_seconds)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = state_path.with_suffix(".tmp")
    temporary.write_text(deadline.isoformat())
    temporary.replace(state_path)
    return deadline


def sleep_until(
    deadline: datetime,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], None] = time.sleep,
    maximum_sleep_seconds: int = 60,
) -> None:
    """Recheck wall time frequently so host suspend cannot leave a stale sleep."""
    while True:
        remaining = (deadline - now()).total_seconds()
        if remaining <= 0:
            return
        sleep(min(remaining, maximum_sleep_seconds))


def watch_deadline(
    *,
    engine_factory=get_engine,
    is_healthy=scheduler_is_healthy,
    sleep: Callable[[float], None] = time.sleep,
    exit_process: Callable[[int], None] = os._exit,
    check_seconds: int = 300,
    misses_before_exit: int = 2,
) -> None:
    """Exit after repeated missed deadlines so Docker's restart policy recovers us.

    Docker Compose restarts containers when the process exits, not when a
    healthcheck fails, so a hung scheduler must end its own process.
    """
    misses = 0
    while True:
        sleep(check_seconds)
        try:
            healthy = is_healthy(engine=engine_factory())
        except Exception as exc:
            print(
                f"Deadline watchdog could not read run history: {type(exc).__name__}: {exc}",
                flush=True,
            )
            continue
        misses = 0 if healthy else misses + 1
        if misses:
            print(f"Shadow scheduler is overdue (check {misses} of {misses_before_exit}).", flush=True)
        if misses >= misses_before_exit:
            print("Exiting so Docker restarts the shadow scheduler.", flush=True)
            exit_process(1)
            return


def main() -> None:
    interval_seconds = _positive_hours("SHADOW_INTERVAL_HOURS", 72) * 3600
    retry_seconds = _positive_hours("SHADOW_RETRY_HOURS", 6) * 3600
    initial_delay_seconds = _nonnegative_hours("SHADOW_INITIAL_DELAY_HOURS", 0) * 3600
    print("Shadow scheduler started. Alerts are not imported or configured.")
    threading.Thread(target=watch_deadline, name="shadow-deadline-watchdog", daemon=True).start()
    while True:
        try:
            deadline = next_run_at(
                get_engine(),
                interval_seconds,
                retry_seconds,
                initial_delay_seconds,
                Path("data/shadow_scheduler_initial_due.txt"),
            )
        except Exception as exc:
            print(f"Unable to recover schedule: {type(exc).__name__}: {exc}")
            time.sleep(60)
            continue
        print(f"Next shadow attempt due at {deadline.isoformat()}", flush=True)
        sleep_until(deadline)
        print(f"Starting shadow run at {datetime.now(timezone.utc).isoformat()}")
        try:
            result = run_scheduled_shadow_pipeline()
        except Exception as exc:
            print(f"Shadow run failed: {type(exc).__name__}: {exc}")
            delay = retry_seconds
        else:
            print(
                "Shadow run succeeded: "
                f"run_id={result['run_id']}, ready={result['readiness']['ready']}"
            )
            delay = 0
        # Also back off when a failure happens before a run can be recorded.
        if delay:
            sleep_until(datetime.now(timezone.utc) + timedelta(seconds=delay))


if __name__ == "__main__":
    main()
