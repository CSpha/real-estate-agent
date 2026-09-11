from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

from app.db import get_engine

from app.shadow.run_pipeline import run_shadow_pipeline


def _positive_hours(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    return value


def _nonnegative_hours(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 0:
        raise ValueError(f"{name} cannot be negative")
    return value


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


def main() -> None:
    interval_seconds = _positive_hours("SHADOW_INTERVAL_HOURS", 72) * 3600
    retry_seconds = _positive_hours("SHADOW_RETRY_HOURS", 6) * 3600
    initial_delay_seconds = _nonnegative_hours("SHADOW_INITIAL_DELAY_HOURS", 0) * 3600
    print("Shadow scheduler started. Alerts are not imported or configured.")
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
        time.sleep(max(0, (deadline - datetime.now(timezone.utc)).total_seconds()))
        print(f"Starting shadow run at {datetime.now(timezone.utc).isoformat()}")
        try:
            result = run_shadow_pipeline()
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
            time.sleep(delay)


if __name__ == "__main__":
    main()
