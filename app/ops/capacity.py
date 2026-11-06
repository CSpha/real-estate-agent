"""Read-only database capacity report: `python -m app.ops.capacity`."""
from __future__ import annotations

import os
from pathlib import Path

from app.db import get_engine
from app.ops.monitoring import (
    DEFAULT_DB_SIZE_WARN_MB,
    env_int,
    fetch_database_size,
    fetch_key_table_counts,
    fetch_largest_tables,
)


def directory_usage(path: Path) -> tuple[int, int] | None:
    """Return (file_count, total_bytes) for a directory, or None if missing."""
    if not path.is_dir():
        return None
    count = total = 0
    for entry in path.rglob("*"):
        try:
            if entry.is_file():
                count += 1
                total += entry.stat().st_size
        except OSError:
            continue
    return count, total


def _mb(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):,.1f} MB"


def main() -> None:
    engine = get_engine()
    warn_mb = env_int("DB_SIZE_WARN_MB", DEFAULT_DB_SIZE_WARN_MB, 0)
    size = fetch_database_size(engine)
    state = "OVER" if warn_mb and size >= warn_mb * 1024 * 1024 else "ok"
    print(f"Database size: {_mb(size)} (warning threshold {warn_mb} MB, {state})")
    print("Largest tables:")
    for name, total in fetch_largest_tables(engine, 10):
        print(f"  {name:<40} {_mb(total):>12}")
    print("Key table row counts:")
    for name, count in fetch_key_table_counts(engine).items():
        print(f"  {name:<40} {'missing' if count is None else f'{count:,}':>12}")
    backup_dir = Path(os.getenv("BACKUP_DIR", "data/backups"))
    usage = directory_usage(backup_dir)
    if usage is None:
        print(f"Backup folder {backup_dir}: not found")
    else:
        print(f"Backup folder {backup_dir}: {usage[0]} files, {_mb(usage[1])}")


if __name__ == "__main__":
    main()
