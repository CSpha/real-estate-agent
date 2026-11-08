"""Post-run monitoring for repeated provider failures and database capacity.

Everything here is read-only against Postgres and reports only through
`OpsNotifier` (ops channel). It never imports `app.alerts`. Each problem state
has its own notifier/state file, so a problem is announced once and a recovery
message is sent once when the check is healthy again.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import text

from app.ops.notify import OpsNotifier


DEFAULT_STATE_DIR = Path("data")
DEFAULT_FAILURE_THRESHOLD = 2
DEFAULT_BUDGET_WARN_PERCENT = 80
DEFAULT_DB_SIZE_WARN_MB = 5000
TOP_TABLE_COUNT = 5

KEY_TABLES = (
    "listings_current",
    "listing_history",
    "listings_raw",
    "alert_outbox",
    "ingest_errors",
    "shadow_pipeline_runs",
    "listing_comparable_valuations",
)

RECENT_RENTCAST_RUNS_SQL = text(
    """
    SELECT id, status, error_message, started_at,
           (details_json ? 'rentcast_requested') AS requested,
           (details_json ? 'rentcast') AS synced
    FROM shadow_pipeline_runs
    WHERE source = 'rentcast' AND status <> 'running'
    ORDER BY started_at DESC, id DESC
    LIMIT :limit
    """
)
DATABASE_SIZE_SQL = text("SELECT pg_database_size(current_database())")
LARGEST_TABLES_SQL = text(
    """
    SELECT c.relname AS table_name,
           pg_total_relation_size(c.oid) AS total_bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r' AND n.nspname = current_schema()
    ORDER BY total_bytes DESC, c.relname
    LIMIT :limit
    """
)


def env_int(name: str, default: int, minimum: int = 0) -> int:
    """Read an integer setting; a bad value falls back so monitoring never raises."""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        print(f"{name} must be an integer; using {default}.", flush=True)
        return default
    if value < minimum:
        print(f"{name} must be at least {minimum}; using {default}.", flush=True)
        return default
    return value


# --- Pure evaluation -------------------------------------------------------

def repeated_failure_detail(runs: list[dict[str, Any]], threshold: int) -> str | None:
    """Warn when the latest `threshold` RentCast attempts all failed.

    `runs` are newest first. Runs that never requested RentCast are not
    attempts and neither reset nor extend the streak.
    """
    attempts = [r for r in runs if r["requested"]][:threshold]
    if len(attempts) < threshold or any(r["status"] != "failed" for r in attempts):
        return None
    error = " ".join(str(attempts[0]["error_message"] or "unknown error").split())
    return (
        f"The last {threshold} RentCast syncs failed (latest run "
        f"{attempts[0]['id']}: {error[:150]})."
    )


def unexpected_skip_detail(
    runs: list[dict[str, Any]],
    threshold: int,
    *,
    calls_enabled: bool,
    budget_blocked: bool,
    last_attempt_at: datetime | None,
    min_hours: int,
    now: datetime,
) -> str | None:
    """Warn when RentCast keeps being skipped although nothing explains it.

    Expected reasons (calls disabled, monthly budget reached, recent attempt
    inside the minimum spacing) never warn.
    """
    if not calls_enabled or budget_blocked or len(runs) < threshold:
        return None
    if any(r["requested"] for r in runs[:threshold]):
        return None
    if last_attempt_at is not None and (now - last_attempt_at).total_seconds() < min_hours * 3600:
        return None
    return (
        f"The last {threshold} runs skipped RentCast although API calls are "
        "enabled, the monthly budget has room, and no recent attempt explains it."
    )


def budget_detail(used: int, limit: int, warn_percent: int) -> str | None:
    if limit <= 0 or used * 100 < limit * warn_percent:
        return None
    return (
        f"{used} of {limit} monthly RentCast requests used "
        f"({used * 100 // limit}%); warning at {warn_percent}%."
    )


def database_size_detail(
    size_bytes: int, warn_mb: int, largest: list[tuple[str, int]]
) -> str | None:
    if warn_mb <= 0 or size_bytes < warn_mb * 1024 * 1024:
        return None
    tables = ", ".join(f"{name} {_mb(b)} MB" for name, b in largest[:3])
    return (
        f"Database is {_mb(size_bytes)} MB, over the {warn_mb} MB warning "
        f"threshold. Largest tables: {tables or 'unknown'}."
    )


def _mb(size_bytes: int) -> int:
    return int(size_bytes) // (1024 * 1024)


# --- Read-only queries -----------------------------------------------------

def fetch_recent_rentcast_runs(engine, limit: int) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        rows = connection.execute(RECENT_RENTCAST_RUNS_SQL, {"limit": limit}).mappings().all()
    return [dict(row) for row in rows]


def fetch_database_size(engine) -> int:
    with engine.connect() as connection:
        return int(connection.execute(DATABASE_SIZE_SQL).scalar_one())


def fetch_largest_tables(engine, limit: int = TOP_TABLE_COUNT) -> list[tuple[str, int]]:
    with engine.connect() as connection:
        rows = connection.execute(LARGEST_TABLES_SQL, {"limit": limit}).all()
    return [(row[0], int(row[1])) for row in rows]


def fetch_key_table_counts(engine, tables: tuple[str, ...] = KEY_TABLES) -> dict[str, int | None]:
    """Exact row counts for a fixed allowlist of tables; None when absent."""
    counts: dict[str, int | None] = {}
    with engine.connect() as connection:
        for table in tables:
            if connection.execute(text("SELECT to_regclass(:name)"), {"name": table}).scalar() is None:
                counts[table] = None
            else:
                counts[table] = connection.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar_one()
    return counts


# --- Reporting -------------------------------------------------------------

class MonitorNotifiers:
    """One OpsNotifier per problem state so each is announced and cleared alone."""

    NAMES = ("provider_failures", "provider_skips", "provider_budget", "database_size")

    def __init__(
        self,
        state_dir: Path = DEFAULT_STATE_DIR,
        factory: Callable[..., OpsNotifier] = OpsNotifier,
    ):
        self._by_name = {
            name: factory(state_path=state_dir / f"ops_monitor_{name}.json")
            for name in self.NAMES
        }

    def report(self, name: str, kind: str, detail: str | None, ok_detail: str) -> None:
        notifier = self._by_name[name]
        if detail is None:
            notifier.resolved(ok_detail)
        else:
            notifier.problem(kind, detail)


def check_provider_health(
    engine,
    notifiers: MonitorNotifiers,
    *,
    requests_used: int,
    request_limit: int,
    last_attempt_at: datetime | None,
    calls_enabled: bool,
    budget_blocked: bool,
    min_hours: int,
    now: datetime | None = None,
) -> None:
    now = now or datetime.now(timezone.utc)
    threshold = env_int("PROVIDER_FAILURE_THRESHOLD", DEFAULT_FAILURE_THRESHOLD, 1)
    warn_percent = env_int("RENTCAST_BUDGET_WARN_PERCENT", DEFAULT_BUDGET_WARN_PERCENT, 1)
    runs = fetch_recent_rentcast_runs(engine, max(threshold, 10))
    notifiers.report(
        "provider_failures",
        "repeated RentCast failures",
        repeated_failure_detail(runs, threshold),
        "RentCast sync succeeded again.",
    )
    notifiers.report(
        "provider_skips",
        "RentCast repeatedly skipped",
        unexpected_skip_detail(
            runs,
            threshold,
            calls_enabled=calls_enabled,
            budget_blocked=budget_blocked,
            last_attempt_at=last_attempt_at,
            min_hours=min_hours,
            now=now,
        ),
        "RentCast is no longer being skipped unexpectedly.",
    )
    notifiers.report(
        "provider_budget",
        "RentCast request budget nearly exhausted",
        budget_detail(requests_used, request_limit, warn_percent),
        "RentCast request usage is below the warning level.",
    )


def check_database_capacity(engine, notifiers: MonitorNotifiers) -> None:
    warn_mb = env_int("DB_SIZE_WARN_MB", DEFAULT_DB_SIZE_WARN_MB, 0)
    size = fetch_database_size(engine)
    over = bool(warn_mb) and size >= warn_mb * 1024 * 1024
    largest = fetch_largest_tables(engine) if over else []
    notifiers.report(
        "database_size",
        "database size over threshold",
        database_size_detail(size, warn_mb, largest),
        f"Database is {_mb(size)} MB, under the {warn_mb} MB threshold.",
    )
