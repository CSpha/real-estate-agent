"""Health check for the deadline-driven shadow scheduler."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.db import get_engine


def scheduler_is_healthy(*, engine: Engine, now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    interval = int(os.getenv("SHADOW_INTERVAL_HOURS", "72"))
    retry = int(os.getenv("SHADOW_RETRY_HOURS", "6"))
    grace = int(os.getenv("SHADOW_OVERDUE_GRACE_MINUTES", "15"))
    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT status, started_at, finished_at FROM shadow_pipeline_runs "
                "WHERE source = 'rentcast' ORDER BY started_at DESC, id DESC LIMIT 1"
            )
        ).mappings().first()
    if row is None:
        return True
    base = row["finished_at"] or row["started_at"]
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    delay_hours = interval if row["status"] == "succeeded" else retry
    return now <= base + timedelta(hours=delay_hours, minutes=grace)


def main() -> None:
    if not scheduler_is_healthy(engine=get_engine()):
        raise SystemExit("Shadow scheduler is overdue")


if __name__ == "__main__":
    main()
