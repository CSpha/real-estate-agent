"""Pagination bounds and per-request statement timeouts for the API."""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Sequence

from fastapi import Query, Response
from sqlalchemy import Engine, create_engine

from app.config import get_settings

DEFAULT_PAGE_LIMIT = 50
MAX_PAGE_LIMIT = 200
DEFAULT_STATEMENT_TIMEOUT_MS = 5000


def page_limit(default: int = DEFAULT_PAGE_LIMIT, maximum: int = MAX_PAGE_LIMIT):
    """Validated `limit` query parameter (422 when below 1 or above the max)."""
    return Query(default, ge=1, le=maximum, description=f"Page size (1-{maximum})")


def page_offset():
    """Validated `offset` query parameter (422 when negative)."""
    return Query(0, ge=0, le=1_000_000, description="Rows to skip")


def statement_timeout_ms() -> int:
    """API_STATEMENT_TIMEOUT_MS, falling back to the default when malformed."""
    try:
        value = int(os.getenv("API_STATEMENT_TIMEOUT_MS", ""))
    except ValueError:
        return DEFAULT_STATEMENT_TIMEOUT_MS
    return value if value >= 0 else DEFAULT_STATEMENT_TIMEOUT_MS


@lru_cache(maxsize=1)
def get_api_engine() -> Engine:
    """Engine whose sessions cancel any statement running past the timeout.

    Kept separate from the pipeline engine so batch jobs are not time limited.
    A value of 0 disables the timeout (Postgres semantics).
    """
    return create_engine(
        get_settings().sqlalchemy_database_url,
        pool_pre_ping=True,
        connect_args={"options": f"-c statement_timeout={statement_timeout_ms()}"},
    )


def set_local_timeout(cursor: Any) -> None:
    """Apply the timeout to the current transaction of a raw psycopg2 cursor."""
    cursor.execute(
        "SELECT set_config('statement_timeout', %s, true)",
        (str(statement_timeout_ms()),),
    )


def paginate(rows: Sequence[Any], limit: int) -> tuple[list[Any], bool]:
    """Trim a `limit + 1` fetch to one page and report whether more rows exist."""
    return list(rows[:limit]), len(rows) > limit


def set_page_headers(
    response: Response, *, limit: int, offset: int, has_more: bool
) -> None:
    """Expose paging metadata as headers so list bodies stay plain arrays."""
    response.headers["X-Page-Limit"] = str(limit)
    response.headers["X-Page-Offset"] = str(offset)
    response.headers["X-Has-More"] = "true" if has_more else "false"
    if has_more:
        response.headers["X-Next-Offset"] = str(offset + limit)
