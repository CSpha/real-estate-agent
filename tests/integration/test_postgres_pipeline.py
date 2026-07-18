from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine, make_url

from app.alerts.send_price_drop_alerts import (
    get_price_drop_events,
    record_alert,
)
from app.ingest.load_sample_listings import load_sample_csv
from app.transforms.score_listings_against_market import (
    score_listings_against_market,
)
from app.transforms.snapshot_listings import snapshot_current_listings


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_engine() -> Engine:
    if not TEST_DATABASE_URL:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")

    url = make_url(TEST_DATABASE_URL)
    if not (url.database or "").endswith("_test"):
        pytest.fail("TEST_DATABASE_URL database name must end with '_test'")

    schema = f"phase1_{uuid4().hex}"
    admin_engine = create_engine(TEST_DATABASE_URL)
    with admin_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))

    engine = create_engine(TEST_DATABASE_URL)

    @event.listens_for(engine, "connect")
    def set_search_path(dbapi_connection, _connection_record):
        with dbapi_connection.cursor() as cursor:
            cursor.execute(f'SET search_path TO "{schema}"')

    try:
        alembic_config = Config(str(PROJECT_ROOT / "alembic.ini"))
        with engine.connect() as connection:
            alembic_config.attributes["connection"] = connection
            command.upgrade(alembic_config, "head")
        yield engine
    finally:
        engine.dispose()
        with admin_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin_engine.dispose()


def test_sample_ingestion_and_history_are_idempotent(postgres_engine):
    sample_path = PROJECT_ROOT / "data" / "sample_listings.csv"

    first_load = load_sample_csv(str(sample_path), postgres_engine)
    first_snapshot = snapshot_current_listings(postgres_engine)
    second_load = load_sample_csv(str(sample_path), postgres_engine)
    second_snapshot = snapshot_current_listings(postgres_engine)

    assert first_load == {
        "received": 3,
        "raw_inserted": 3,
        "current_changed": 3,
    }
    assert first_snapshot == 3
    assert second_load == {
        "received": 3,
        "raw_inserted": 0,
        "current_changed": 0,
    }
    assert second_snapshot == 0

    with postgres_engine.connect() as conn:
        assert conn.scalar(text("SELECT COUNT(*) FROM listings_raw")) == 3
        assert conn.scalar(text("SELECT COUNT(*) FROM listings_current")) == 3
        assert conn.scalar(text("SELECT COUNT(*) FROM listing_history")) == 3
        normalized = conn.execute(
            text(
                """
                SELECT status, property_type
                FROM listings_current
                WHERE source_listing_id = '1001'
                """
            )
        ).one()
        assert normalized.status == "Active"
        assert normalized.property_type == "SingleFamilyResidence"


def test_material_price_change_creates_one_event(postgres_engine):
    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE listings_current
                SET list_price = 70000, updated_at = CURRENT_TIMESTAMP
                WHERE source = 'sample_feed'
                  AND source_listing_id = '1001'
                """
            )
        )

    assert snapshot_current_listings(postgres_engine) == 1
    assert snapshot_current_listings(postgres_engine) == 0

    events = get_price_drop_events(postgres_engine)
    matching = [event for event in events if event["source_listing_id"] == "1001"]
    assert len(matching) == 1
    assert matching[0]["previous_price"] == 78500
    assert matching[0]["current_price"] == 70000


def test_alert_ledger_recording_is_idempotent(postgres_engine):
    event = {
        "source": "sample_feed",
        "source_listing_id": "ledger-test",
        "address": "1 Test St",
        "city": "Wooster",
        "state": "OH",
        "previous_price": 100000,
        "current_price": 95000,
        "price_change": -5000,
        "price_change_pct": -5,
        "event_timestamp": datetime(2026, 7, 18, tzinfo=timezone.utc),
    }

    assert record_alert(event, postgres_engine) is True
    assert record_alert(event, postgres_engine) is False

    with postgres_engine.connect() as conn:
        count = conn.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM alerts_sent
                WHERE source_listing_id = 'ledger-test'
                """
            )
        )
    assert count == 1


def test_market_scoring_changes_only_when_inputs_change(postgres_engine):
    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO county_sales (
                    county_name,
                    state,
                    period_date,
                    median_sale_price
                )
                VALUES
                    ('Wayne', 'OH', DATE '2026-06-01', 300000),
                    ('Ashland', 'OH', DATE '2026-06-01', 300000)
                """
            )
        )

    assert score_listings_against_market(postgres_engine) == 2

    with postgres_engine.connect() as conn:
        first_scored_at = conn.execute(
            text(
                """
                SELECT source_listing_id, scored_at
                FROM listing_market_scores
                ORDER BY source_listing_id
                """
            )
        ).all()

    assert score_listings_against_market(postgres_engine) == 0

    with postgres_engine.connect() as conn:
        second_scored_at = conn.execute(
            text(
                """
                SELECT source_listing_id, scored_at
                FROM listing_market_scores
                ORDER BY source_listing_id
                """
            )
        ).all()
        potential_deal_count = conn.scalar(text("SELECT COUNT(*) FROM potential_deals"))

    assert second_scored_at == first_scored_at
    assert potential_deal_count == 2
