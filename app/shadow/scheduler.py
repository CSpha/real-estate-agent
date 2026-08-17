from __future__ import annotations

import os
import time
from datetime import datetime, timezone

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


def main() -> None:
    interval_seconds = _positive_hours("SHADOW_INTERVAL_HOURS", 168) * 3600
    retry_seconds = _positive_hours("SHADOW_RETRY_HOURS", 6) * 3600
    initial_delay_seconds = (
        _nonnegative_hours("SHADOW_INITIAL_DELAY_HOURS", 0) * 3600
    )
    print("Shadow scheduler started. Alerts are not imported or configured.")
    if initial_delay_seconds:
        print(
            "Initial shadow run delayed by "
            f"{initial_delay_seconds // 3600} hour(s)."
        )
        time.sleep(initial_delay_seconds)
    while True:
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
            delay = interval_seconds
        print(f"Next attempt in {delay // 3600} hour(s).")
        time.sleep(delay)


if __name__ == "__main__":
    main()
