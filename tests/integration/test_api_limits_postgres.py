from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

import app.api_limits as api_limits

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")

pytestmark = pytest.mark.integration


@pytest.fixture
def api_engine(monkeypatch):
    if not TEST_DATABASE_URL:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")
    if not (make_url(TEST_DATABASE_URL).database or "").endswith("_test"):
        pytest.fail("TEST_DATABASE_URL database name must end with '_test'")

    class Settings:
        sqlalchemy_database_url = TEST_DATABASE_URL

    monkeypatch.setattr(api_limits, "get_settings", lambda: Settings())
    monkeypatch.setenv("API_STATEMENT_TIMEOUT_MS", "200")
    api_limits.get_api_engine.cache_clear()
    engine = api_limits.get_api_engine()
    yield engine
    engine.dispose()
    api_limits.get_api_engine.cache_clear()


def test_api_engine_cancels_statements_past_the_timeout(api_engine):
    with pytest.raises(OperationalError, match="statement timeout"):
        with api_engine.connect() as conn:
            conn.execute(text("SELECT pg_sleep(2)"))


def test_api_engine_allows_fast_statements(api_engine):
    with api_engine.connect() as conn:
        assert conn.execute(text("SELECT 1")).scalar() == 1


def test_limit_plus_one_offset_paging_is_stable():
    engine = create_engine(TEST_DATABASE_URL) if TEST_DATABASE_URL else None
    if engine is None:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")
    query = text(
        "SELECT n FROM generate_series(1, 5) AS n ORDER BY n LIMIT :limit OFFSET :offset"
    )
    with engine.connect() as conn:
        first = conn.execute(query, {"limit": 3, "offset": 0}).scalars().all()
        second = conn.execute(query, {"limit": 3, "offset": 2}).scalars().all()
    engine.dispose()

    page, has_more = api_limits.paginate(first, 2)
    assert (page, has_more) == ([1, 2], True)
    page, has_more = api_limits.paginate(second, 2)
    assert (page, has_more) == ([3, 4], True)
