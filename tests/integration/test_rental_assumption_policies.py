from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

from app.market.rental_assumptions import DEFAULT_ASSUMPTIONS, load_active_assumptions
from app.transforms.manage_rental_assumptions import activate_version, create_version

pytestmark = pytest.mark.integration
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture()
def alembic_env():
    if not TEST_DATABASE_URL:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")
    if not (make_url(TEST_DATABASE_URL).database or "").endswith("_test"):
        pytest.fail("TEST_DATABASE_URL database name must end with '_test'")
    schema = f"rental_{uuid4().hex}"
    admin = create_engine(TEST_DATABASE_URL)
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(TEST_DATABASE_URL)

    @event.listens_for(engine, "connect")
    def set_search_path(dbapi_connection, _record):
        with dbapi_connection.cursor() as cursor:
            cursor.execute(f'SET search_path TO "{schema}"')

    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    try:
        yield engine, config
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin.dispose()


def _run(engine, config, fn, target):
    with engine.connect() as connection:
        config.attributes["connection"] = connection
        fn(config, target)


def _table_exists(engine, name):
    with engine.connect() as conn:
        return conn.execute(text("SELECT to_regclass(:n)"), {"n": name}).scalar() is not None


def test_migration_seeds_v1_and_round_trips(alembic_env):
    engine, config = alembic_env
    _run(engine, config, command.upgrade, "0021_listing_feed_presence")
    assert not _table_exists(engine, "rental_assumption_policies")
    _run(engine, config, command.upgrade, "head")
    with engine.connect() as conn:
        active = load_active_assumptions(conn)
        assert active.version == "rental-assumptions-v1"
        assert active.assumptions == DEFAULT_ASSUMPTIONS
        cols = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = 'rental_forecast_scenarios'"
        )).scalars().all()
        assert "assumption_version" in cols
    _run(engine, config, command.downgrade, "0021_listing_feed_presence")
    assert not _table_exists(engine, "rental_assumption_policies")
    _run(engine, config, command.upgrade, "head")
    assert _table_exists(engine, "rental_assumption_policies")


def test_create_and_activate_is_transactional_and_single_active(alembic_env):
    engine, config = alembic_env
    _run(engine, config, command.upgrade, "head")
    candidate = {"scenario_growth_pct": {"conservative": "0", "base": "3", "upside": "5"},
                 "rent_to_cash_flow_pass_through": "0.70"}
    with engine.begin() as conn:
        create_version(conn, "rental-assumptions-v2", candidate)
    with engine.connect() as conn:
        assert load_active_assumptions(conn).version == "rental-assumptions-v1"
    with pytest.raises(ValueError):
        with engine.begin() as conn:
            activate_version(conn, "missing")
    with engine.begin() as conn:
        activate_version(conn, "rental-assumptions-v2")
    with engine.connect() as conn:
        active = load_active_assumptions(conn)
        assert active.version == "rental-assumptions-v2"
        assert active.pass_through == Decimal("0.70")
        assert conn.execute(text(
            "SELECT count(*) FROM rental_assumption_policies WHERE is_active"
        )).scalar() == 1
    with pytest.raises(ValueError):
        with engine.begin() as conn:
            create_version(conn, "bad", {"scenario_growth_pct": {}})
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO rental_assumption_policies (policy_version, assumptions, is_active) "
                "VALUES ('second-active', '{}'::jsonb, TRUE)"
            ))
