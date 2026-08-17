from __future__ import annotations

import io
import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import requests
import pandas as pd
from alembic import command
from alembic.config import Config
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine, make_url

from app.alerts.send_price_drop_alerts import (
    enqueue_price_drop_events,
    format_slack_message,
    get_price_drop_events,
    record_alert,
)
from app.alerts.outbox import deliver_pending_alerts
from app.ingest.load_comparable_sales import load_comparable_sales
from app.ingest.load_sample_listings import load_sample_csv
from app.market.macro_rate_signals import (
    load_fred_series,
    load_sme_expectations,
)
from app.market.backtest_mortgage_rate_outlook import run_backtest
from app.market.comparable_selection import select_comparables
from app.market.comparable_valuation import value_listing
from app.market.mortgage_rate_outlook import generate_and_store_outlooks
from app.market.mortgage_rate_outlook_v2 import (
    generate_multi_signal_outlooks,
)
from app.market.mortgage_rates import get_rate_trend, load_pmms_csv
from app.searches.models import (
    SavedSearchCreate,
    SavedSearchUpdate,
    SearchCriteria,
)
from app.searches.service import (
    create_saved_search,
    delete_saved_search,
    evaluate_saved_search,
    list_search_evaluations,
    update_saved_search,
)
from app.transforms.score_listings_against_market import (
    score_listings_against_market,
)
from app.transforms.persist_comparable_valuation import (
    persist_comparable_valuation,
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
        "scoring_eligible": 3,
        "scoring_excluded": 0,
        "current_changed": 3,
    }
    assert first_snapshot == 3
    assert second_load == {
        "received": 3,
        "raw_inserted": 0,
        "scoring_eligible": 3,
        "scoring_excluded": 0,
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


def test_comparable_sales_ingestion_is_idempotent(postgres_engine):
    sample_path = PROJECT_ROOT / "data" / "sample_comparable_sales.csv"

    first_load = load_comparable_sales(sample_path, postgres_engine)
    second_load = load_comparable_sales(sample_path, postgres_engine)

    assert first_load == {
        "received": 3,
        "raw_inserted": 3,
        "current_changed": 3,
    }
    assert second_load == {
        "received": 3,
        "raw_inserted": 0,
        "current_changed": 0,
    }

    with postgres_engine.connect() as conn:
        assert conn.scalar(text("SELECT COUNT(*) FROM comparable_sales_raw")) == 3
        assert conn.scalar(text("SELECT COUNT(*) FROM comparable_sales")) == 3
        normalized = conn.execute(
            text(
                """
                SELECT state, zip, property_type, arms_length
                FROM comparable_sales
                WHERE source_sale_id = 'wayne-2026-002'
                """
            )
        ).one()
        assert normalized.state == "OH"
        assert normalized.zip == "44691"
        assert normalized.property_type == "SingleFamilyResidence"
        assert normalized.arms_length is True


def test_comparable_selection_is_deterministic_and_exposes_fallback(
    postgres_engine,
):
    strict_result = select_comparables(
        "sample_feed",
        "1001",
        as_of_date=date(2026, 7, 27),
        engine=postgres_engine,
    )
    repeated_result = select_comparables(
        "sample_feed",
        "1001",
        as_of_date=date(2026, 7, 27),
        engine=postgres_engine,
    )

    assert strict_result == repeated_result
    assert strict_result["selected_tier"]["key"] == "strict_zip"
    assert strict_result["meets_minimum"] is True
    assert strict_result["available_comparable_count"] == 3
    assert strict_result["returned_comparable_count"] == 3
    assert strict_result["attempted_tiers"] == [
        {"tier": "strict_zip", "candidate_count": 3}
    ]
    assert "lot_size_acres" in strict_result["criteria_not_applied"]
    assert "year_built" in strict_result["criteria_not_applied"]
    assert all(
        comparable["property_type"] == "SingleFamilyResidence"
        for comparable in strict_result["comparables"]
    )

    try:
        with postgres_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE comparable_sales
                    SET zip = '44690'
                    WHERE source_sale_id = 'wayne-2026-003'
                    """
                )
            )

        fallback_result = select_comparables(
            "sample_feed",
            "1001",
            as_of_date=date(2026, 7, 27),
            engine=postgres_engine,
        )
        assert fallback_result["selected_tier"]["key"] == "city_fallback"
        assert fallback_result["attempted_tiers"] == [
            {"tier": "strict_zip", "candidate_count": 2},
            {"tier": "broad_zip", "candidate_count": 2},
            {"tier": "city_fallback", "candidate_count": 3},
        ]
        assert fallback_result["meets_minimum"] is True
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE comparable_sales
                    SET zip = '44691'
                    WHERE source_sale_id = 'wayne-2026-003'
                    """
                )
            )


def test_comparable_valuation_is_explainable(postgres_engine):
    result = value_listing(
        "sample_feed",
        "1001",
        as_of_date=date(2026, 7, 27),
        engine=postgres_engine,
    )

    assert result["status"] == "valued"
    assert result["valuation_version"] == "comparable-valuation-v1"
    assert result["selected_tier"] == "strict_zip"
    assert result["included_comparable_count"] == 3
    assert result["outlier_filter_applied"] is False
    assert result["weighted_median_price_per_sqft"] == Decimal("66.30")
    assert result["estimated_value"] == Decimal("82207.41")
    assert result["estimated_value_low"] == Decimal("79437.50")
    assert result["estimated_value_high"] == Decimal("86357.14")
    assert result["list_price_discount_amount"] == Decimal("3707.41")
    assert result["list_price_discount_pct"] == Decimal("0.0451")
    assert result["confidence_label"] == "medium"
    assert set(result["confidence_components"]) == {
        "sample_size_points",
        "geography_points",
        "recency_points",
        "similarity_points",
        "completeness_points",
        "dispersion_penalty",
    }
    assert all(
        comparable["included_in_valuation"]
        for comparable in result["comparables"]
    )


def test_comparable_valuation_persistence_is_idempotent_and_auditable(
    postgres_engine,
):
    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO listing_history (
                    source,
                    source_listing_id,
                    list_price,
                    days_on_market,
                    snapshot_timestamp
                )
                VALUES
                    ('sample_feed', '1001', 90000, 10, '2026-06-01 12:00:00+00'),
                    ('sample_feed', '1001', 85000, 30, '2026-06-20 12:00:00+00'),
                    ('sample_feed', '1001', 78500, 45, '2026-07-15 12:00:00+00')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO county_sales (
                    county_name,
                    state,
                    period_date,
                    median_sale_price,
                    homes_sold,
                    new_listings,
                    active_listings,
                    median_days_on_market
                )
                VALUES
                    (
                        'Wayne', 'OH', DATE '2025-07-01', 100000,
                        NULL, NULL, NULL, NULL
                    ),
                    (
                        'Wayne', 'OH', DATE '2026-04-01', 108000,
                        NULL, NULL, NULL, NULL
                    ),
                    (
                        'Wayne', 'OH', DATE '2026-07-01', 110000,
                        50, 50, 200, 20
                    )
                """
            )
        )
    try:
        first = persist_comparable_valuation(
            "sample_feed",
            "1001",
            as_of_date=date(2026, 7, 27),
            engine=postgres_engine,
        )
        repeated = persist_comparable_valuation(
            "sample_feed",
            "1001",
            as_of_date=date(2026, 7, 27),
            engine=postgres_engine,
        )

        assert first["valuation_created"] is True
        assert first["component_created"] is True
        assert repeated["valuation_created"] is False
        assert repeated["component_created"] is False
        assert repeated["valuation_id"] == first["valuation_id"]
        assert repeated["component_id"] == first["component_id"]
        assert repeated["valuation_created_at"] == first["valuation_created_at"]
        assert repeated["component_created_at"] == first["component_created_at"]
        assert first["deal_score_v2_created"] is True
        assert repeated["deal_score_v2_created"] is False
        assert repeated["deal_score_v2_id"] == first["deal_score_v2_id"]
        assert repeated["deal_score_v2_created_at"] == first[
            "deal_score_v2_created_at"
        ]
        assert first["deal_score_v2_component"]["status"] == "available"
        assert first["deal_score_v2_component"]["points"] == Decimal("6.01")

        first_components = {
            item["component"]["component_key"]: item
            for item in first["deal_score_v2_components"]
        }
        repeated_components = {
            item["component"]["component_key"]: item
            for item in repeated["deal_score_v2_components"]
        }
        assert set(first_components) == {
            "comparable_discount",
            "listing_opportunity",
            "days_on_market",
            "market_momentum",
            "liquidity_inventory",
            "data_confidence",
        }
        assert first_components["listing_opportunity"]["component"][
            "points"
        ] == Decimal("12.34")
        assert first_components["days_on_market"]["component"][
            "points"
        ] == Decimal("15.00")
        assert first_components["market_momentum"]["component"][
            "points"
        ] == Decimal("12.56")
        assert first_components["liquidity_inventory"]["component"][
            "points"
        ] == Decimal("8.00")
        assert first_components["data_confidence"]["component"][
            "points"
        ] == Decimal("3.91")
        assert first["deal_score_v2"]["status"] == "complete"
        assert first["deal_score_v2"]["total_points"] == Decimal("57.82")
        assert first["deal_score_v2"]["coverage_pct"] == Decimal("100.00")
        assert all(
            not item["component_created"]
            for item in repeated_components.values()
        )
        assert {
            key: item["component_id"] for key, item in first_components.items()
        } == {
            key: item["component_id"]
            for key, item in repeated_components.items()
        }

        with postgres_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE listings_current
                    SET list_price = 77000
                    WHERE source = 'sample_feed'
                      AND source_listing_id = '1001'
                    """
                )
            )
        changed = persist_comparable_valuation(
            "sample_feed",
            "1001",
            as_of_date=date(2026, 7, 27),
            engine=postgres_engine,
        )
        assert changed["valuation_created"] is True
        assert changed["component_created"] is True
        assert changed["deal_score_v2_created"] is True
        assert changed["valuation_id"] != first["valuation_id"]
        assert changed["deal_score_v2_id"] != first["deal_score_v2_id"]
        assert changed["input_fingerprint"] != first["input_fingerprint"]

        with postgres_engine.connect() as conn:
            assert (
                conn.scalar(
                    text("SELECT COUNT(*) FROM listing_comparable_valuations")
                )
                == 2
            )
            assert (
                conn.scalar(
                    text("SELECT COUNT(*) FROM deal_score_v2_components")
                )
                == 12
            )
            assert (
                conn.scalar(text("SELECT COUNT(*) FROM deal_score_v2_scores"))
                == 2
            )
            stored = conn.execute(
                text(
                    """
                    SELECT
                        component.status,
                        component.points,
                        component.max_points,
                        component.input_fingerprint
                    FROM deal_score_v2_components component
                    WHERE component.id = :component_id
                    """
                ),
                {"component_id": first["component_id"]},
            ).one()
            assert stored.status == "available"
            assert stored.points == Decimal("6.01")
            assert stored.max_points == Decimal("40.00")
            assert len(stored.input_fingerprint) == 64
            stored_score = conn.execute(
                text(
                    """
                    SELECT
                        status,
                        total_points,
                        coverage_pct,
                        input_fingerprint,
                        calculation_json
                    FROM deal_score_v2_scores
                    WHERE id = :score_id
                    """
                ),
                {"score_id": first["deal_score_v2_id"]},
            ).one()
            assert stored_score.status == "complete"
            assert stored_score.total_points == Decimal("57.82")
            assert stored_score.coverage_pct == Decimal("100.00")
            assert len(stored_score.input_fingerprint) == 64
            assert set(
                stored_score.calculation_json["inputs"][
                    "component_fingerprints"
                ]
            ) == set(first_components)
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE listings_current
                    SET list_price = 78500
                    WHERE source = 'sample_feed'
                      AND source_listing_id = '1001'
                    """
                )
            )
            conn.execute(
                text(
                    """
                    DELETE FROM listing_history
                    WHERE source = 'sample_feed'
                      AND source_listing_id = '1001'
                      AND snapshot_timestamp IN (
                          '2026-06-01 12:00:00+00',
                          '2026-06-20 12:00:00+00',
                          '2026-07-15 12:00:00+00'
                      )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    DELETE FROM county_sales
                    WHERE county_name = 'Wayne'
                      AND state = 'OH'
                      AND period_date IN (
                          DATE '2025-07-01',
                          DATE '2026-04-01',
                          DATE '2026-07-01'
                      )
                    """
                )
            )


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


def test_shadow_history_cannot_create_a_price_drop_event(postgres_engine):
    try:
        with postgres_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO listing_history (
                        source,
                        source_listing_id,
                        address,
                        city,
                        state,
                        list_price,
                        status,
                        alert_eligible,
                        snapshot_timestamp
                    )
                    VALUES
                        (
                            'rentcast', 'shadow-test', '1 Shadow St',
                            'Wooster', 'OH', 200000, 'Active', FALSE,
                            '2026-08-15 12:00:00+00'
                        ),
                        (
                            'rentcast', 'shadow-test', '1 Shadow St',
                            'Wooster', 'OH', 190000, 'Active', FALSE,
                            '2026-08-16 12:00:00+00'
                        )
                    """
                )
            )

        events = get_price_drop_events(postgres_engine)
        assert not any(
            event["source"] == "rentcast"
            and event["source_listing_id"] == "shadow-test"
            for event in events
        )
    finally:
        with postgres_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    DELETE FROM listing_history
                    WHERE source = 'rentcast'
                      AND source_listing_id = 'shadow-test'
                    """
                )
            )


def test_price_drop_outbox_queues_and_delivers_once(postgres_engine):
    assert enqueue_price_drop_events(postgres_engine) == 1
    assert enqueue_price_drop_events(postgres_engine) == 0

    sent_payloads = []

    def fake_send(webhook_url, payload):
        sent_payloads.append((webhook_url, payload))

    first_run = deliver_pending_alerts(
        "https://example.invalid/test-webhook",
        {"price_drop": format_slack_message},
        send=fake_send,
        engine=postgres_engine,
    )
    second_run = deliver_pending_alerts(
        "https://example.invalid/test-webhook",
        {"price_drop": format_slack_message},
        send=fake_send,
        engine=postgres_engine,
    )

    assert first_run == {"sent": 1, "failed": 0}
    assert second_run == {"sent": 0, "failed": 0}
    assert len(sent_payloads) == 1

    with postgres_engine.connect() as conn:
        outbox_row = conn.execute(
            text(
                """
                SELECT status, attempt_count, sent_at
                FROM alert_outbox
                WHERE source_listing_id = '1001'
                """
            )
        ).one()
        receipt_count = conn.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM alerts_sent
                WHERE alert_type = 'price_drop'
                  AND source_listing_id = '1001'
                """
            )
        )

    assert outbox_row.status == "sent"
    assert outbox_row.attempt_count == 1
    assert outbox_row.sent_at is not None
    assert receipt_count == 1


def test_outbox_records_retryable_and_permanent_failures(postgres_engine):
    with postgres_engine.begin() as conn:
        outbox_id = conn.scalar(
            text(
                """
                INSERT INTO alert_outbox (
                    alert_type,
                    source,
                    source_listing_id,
                    event_timestamp,
                    payload_json
                )
                VALUES (
                    'price_drop',
                    'sample_feed',
                    'retry-test',
                    :event_timestamp,
                    CAST(:payload_json AS JSONB)
                )
                RETURNING id
                """
            ),
            {
                "event_timestamp": datetime(
                    2026, 7, 19, tzinfo=timezone.utc
                ),
                "payload_json": json.dumps(
                    {
                        "address": "2 Retry St",
                        "city": "Wooster",
                        "state": "OH",
                        "previous_price": "100000",
                        "current_price": "90000",
                        "price_change": "-10000",
                        "price_change_pct": "-10",
                        "source_listing_id": "retry-test",
                    }
                ),
            },
        )

    def timeout_send(_webhook_url, _payload):
        raise requests.Timeout("temporary timeout")

    retry_run = deliver_pending_alerts(
        "https://example.invalid/test-webhook",
        {"price_drop": format_slack_message},
        send=timeout_send,
        engine=postgres_engine,
    )
    assert retry_run == {"sent": 0, "failed": 1}

    with postgres_engine.begin() as conn:
        retry_row = conn.execute(
            text(
                """
                SELECT status, attempt_count, available_at, last_error
                FROM alert_outbox
                WHERE id = :outbox_id
                """
            ),
            {"outbox_id": outbox_id},
        ).one()
        conn.execute(
            text(
                """
                UPDATE alert_outbox
                SET available_at = CURRENT_TIMESTAMP
                WHERE id = :outbox_id
                """
            ),
            {"outbox_id": outbox_id},
        )

    assert retry_row.status == "retryable_failed"
    assert retry_row.attempt_count == 1
    assert "temporary timeout" in retry_row.last_error

    response = requests.Response()
    response.status_code = 400

    def bad_request_send(_webhook_url, _payload):
        raise requests.HTTPError("bad webhook", response=response)

    permanent_run = deliver_pending_alerts(
        "https://example.invalid/test-webhook",
        {"price_drop": format_slack_message},
        send=bad_request_send,
        engine=postgres_engine,
    )
    assert permanent_run == {"sent": 0, "failed": 1}

    with postgres_engine.connect() as conn:
        permanent_row = conn.execute(
            text(
                """
                SELECT status, attempt_count, last_error
                FROM alert_outbox
                WHERE id = :outbox_id
                """
            ),
            {"outbox_id": outbox_id},
        ).one()

    assert permanent_row.status == "permanent_failed"
    assert permanent_row.attempt_count == 2
    assert "bad webhook" in permanent_row.last_error


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

    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE listings_current
                SET scoring_eligible = FALSE
                WHERE source = 'sample_feed'
                  AND source_listing_id = '1001'
                """
            )
        )
    assert score_listings_against_market(postgres_engine) == 1
    with postgres_engine.connect() as conn:
        assert conn.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM listing_market_scores
                WHERE source = 'sample_feed'
                  AND source_listing_id = '1001'
                """
            )
        ) == 0

    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE listings_current
                SET scoring_eligible = TRUE
                WHERE source = 'sample_feed'
                  AND source_listing_id = '1001'
                """
            )
        )
    assert score_listings_against_market(postgres_engine) == 1


def test_mortgage_rate_history_is_idempotent(postgres_engine):
    csv_text = """date,pmms30,pmms30p,pmms15,pmms15p
6/25/2026,6.50,,5.75,
7/2/2026,6.43,,5.79,
7/9/2026,6.49,,5.82,
7/16/2026,6.55,,5.93,
7/23/2026,6.58,,5.96,
"""

    first_load = load_pmms_csv(csv_text, postgres_engine)
    second_load = load_pmms_csv(csv_text, postgres_engine)
    trend = get_rate_trend("30_year_fixed", postgres_engine)

    assert first_load == {"received": 10, "inserted_or_changed": 10}
    assert second_load == {"received": 10, "inserted_or_changed": 0}
    assert trend.current_rate == Decimal("6.58")
    assert trend.change_4_weeks == Decimal("0.08")
    assert trend.direction == "roughly_stable"


def test_mortgage_rate_outlooks_are_versioned_and_idempotent(postgres_engine):
    latest = datetime(2026, 7, 23)
    history_rows = ["date,pmms30,pmms30p,pmms15,pmms15p"]
    for week in range(53):
        observation_date = latest - timedelta(weeks=week)
        history_rows.append(
            f"{observation_date.month}/{observation_date.day}/"
            f"{observation_date.year},"
            f"{Decimal('6.58') - Decimal(week) / 100},,"
            f"{Decimal('5.96') - Decimal(week) / 100},"
        )
    load_pmms_csv("\n".join(history_rows), postgres_engine)

    first = generate_and_store_outlooks(engine=postgres_engine)
    second = generate_and_store_outlooks(engine=postgres_engine)

    assert len(first) == 2
    assert first == second
    assert {outlook.horizon_months for outlook in first} == {1, 3}
    assert all(
        outlook.probability_lower
        + outlook.probability_stable
        + outlook.probability_higher
        == Decimal("1")
        for outlook in first
    )

    with postgres_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    horizon_months,
                    model_version,
                    input_snapshot,
                    drivers
                FROM mortgage_rate_outlooks
                WHERE product_type = '30_year_fixed'
                ORDER BY horizon_months
                """
            )
        ).all()

    assert len(rows) == 2
    assert all(row.model_version == "pmms_momentum_v1" for row in rows)
    assert all(
        Decimal(row.input_snapshot["current_rate"]) == Decimal("6.58")
        for row in rows
    )
    assert all(row.drivers for row in rows)


def test_fred_loader_enforces_rolling_retention(postgres_engine):
    counts = load_fred_series(
        "DGS2",
        """observation_date,DGS2
2022-07-22,3.00
2023-07-23,4.00
2026-07-23,4.37
""",
        postgres_engine,
    )

    with postgres_engine.connect() as conn:
        dates = conn.scalars(
            text(
                """
                SELECT observation_date
                FROM macro_rate_observations
                WHERE series_id = 'DGS2'
                ORDER BY observation_date
                """
            )
        ).all()

    assert counts == {
        "received": 2,
        "inserted_or_changed": 2,
        "expired_deleted": 0,
    }
    assert dates == [date(2023, 7, 23), date(2026, 7, 23)]


def test_multi_signal_outlook_uses_macro_and_fed_inputs(postgres_engine):
    latest = datetime(2026, 7, 23)
    for series_id, start_value, count in (
        ("DGS2", Decimal("4.37"), 70),
        ("DGS10", Decimal("4.45"), 70),
        ("DFF", Decimal("3.63"), 5),
    ):
        lines = [f"observation_date,{series_id}"]
        for day_offset in range(count):
            observation_date = latest - timedelta(days=day_offset)
            value = start_value - Decimal(day_offset) / 1000
            lines.append(f"{observation_date.date().isoformat()},{value}")
        load_fred_series(series_id, "\n".join(lines), postgres_engine)

    cpi_lines = ["observation_date,CPIAUCSL"]
    for month_offset in range(16):
        observation_date = (
            latest.date() - relativedelta(months=month_offset)
        ).replace(day=1)
        value = Decimal("325") - Decimal(month_offset) / 2
        cpi_lines.append(f"{observation_date.isoformat()},{value}")
    load_fred_series("CPIAUCSL", "\n".join(cpi_lines), postgres_engine)

    expectation_frame = pd.DataFrame(
        [
            {
                "survey_release_date": "2026-06-03",
                "panel_type": "Combined",
                "subject": "fed_funds_target_range",
                "value_tag": f"fftr_pathofmodes_{horizon:%Y%m%d}",
                "horizon_date": horizon.date().isoformat(),
                "aggregation": aggregation,
                "aggregation_value": value,
            }
            for horizon in (
                datetime(2026, 6, 17),
                datetime(2026, 8, 31),
                datetime(2026, 10, 31),
            )
            for aggregation, value in (
                ("count", 61),
                ("pctl25", 0.0350),
                ("pctl50", 0.0363),
                ("pctl75", 0.0375),
            )
        ]
    )
    workbook = io.BytesIO()
    expectation_frame.to_excel(workbook, index=False)
    load_sme_expectations(
        workbook.getvalue(),
        source_url="https://example.test/sme.xlsx",
        engine=postgres_engine,
    )

    outlooks = generate_multi_signal_outlooks(engine=postgres_engine)

    assert len(outlooks) == 2
    assert all(item.model_version == "multi_signal_v2" for item in outlooks)
    assert all(item.confidence == "medium" for item in outlooks)
    assert all(
        item.input_snapshot["treasury_10y_change"] is not None
        and item.input_snapshot["inflation_yoy_change_3_months"] is not None
        and item.input_snapshot["expected_fed_funds_rate"] is not None
        for item in outlooks
    )


def test_mortgage_rate_backtest_uses_chronological_holdout(postgres_engine):
    latest = datetime(2026, 7, 23)
    pmms_rows = ["date,pmms30,pmms30p,pmms15,pmms15p"]
    for week in range(160):
        observation_date = latest - timedelta(weeks=week)
        cycle = Decimal((week % 26) - 13) / 100
        pmms_rows.append(
            f"{observation_date.month}/{observation_date.day}/"
            f"{observation_date.year},{Decimal('6.50') + cycle},,"
            f"{Decimal('5.90') + cycle},"
        )
    load_pmms_csv("\n".join(pmms_rows), postgres_engine)

    for series_id, base in (
        ("DGS10", Decimal("4.40")),
        ("DFF", Decimal("3.63")),
    ):
        lines = [f"observation_date,{series_id}"]
        for day_offset in range(1096):
            observation_date = latest - timedelta(days=day_offset)
            cycle = Decimal((day_offset % 120) - 60) / 1000
            lines.append(
                f"{observation_date.date().isoformat()},{base + cycle}"
            )
        load_fred_series(series_id, "\n".join(lines), postgres_engine)

    cpi_lines = ["observation_date,CPIAUCSL"]
    for month_offset in range(48):
        observation_date = (
            latest.date() - relativedelta(months=month_offset)
        ).replace(day=1)
        cpi_lines.append(
            f"{observation_date.isoformat()},"
            f"{Decimal('325') - Decimal(month_offset) * Decimal('0.6')}"
        )
    load_fred_series("CPIAUCSL", "\n".join(cpi_lines), postgres_engine)

    expectation_records = []
    release_date = date(2023, 8, 1)
    while release_date <= date(2026, 4, 1):
        for months in (1, 3):
            horizon = release_date + relativedelta(months=months)
            tag = f"fftr_{release_date:%Y%m%d}_{horizon:%Y%m%d}"
            expectation_records.extend(
                [
                    {
                        "survey_release_date": release_date.isoformat(),
                        "panel_type": "Combined",
                        "subject": "fed_funds_target_range",
                        "value_tag": tag,
                        "horizon_date": horizon.isoformat(),
                        "aggregation": "count",
                        "aggregation_value": 50,
                    },
                    {
                        "survey_release_date": release_date.isoformat(),
                        "panel_type": "Combined",
                        "subject": "fed_funds_target_range",
                        "value_tag": tag,
                        "horizon_date": horizon.isoformat(),
                        "aggregation": "pctl50",
                        "aggregation_value": 0.0363,
                    },
                ]
            )
        release_date += relativedelta(months=2)
    workbook = io.BytesIO()
    pd.DataFrame(expectation_records).to_excel(workbook, index=False)
    load_sme_expectations(
        workbook.getvalue(),
        source_url="https://example.test/archive.xlsx",
        engine=postgres_engine,
    )

    predictions, calibrations = run_backtest(engine=postgres_engine)

    assert len(predictions) >= 40
    assert {row.split for row in predictions} == {"training", "holdout"}
    assert len(calibrations) == 2
    assert all(result.training_count >= 20 for result in calibrations)
    assert all(result.holdout_count >= 8 for result in calibrations)
    assert all(result.approval_reason for result in calibrations)

    with postgres_engine.connect() as conn:
        assert conn.scalar(
            text("SELECT COUNT(*) FROM mortgage_rate_backtest_runs")
        ) == 1
        assert conn.scalar(
            text("SELECT COUNT(*) FROM mortgage_rate_calibrations")
        ) == 2


def test_saved_search_versioning_and_evaluation_are_idempotent(postgres_engine):
    search = create_saved_search(
        SavedSearchCreate(
            name="Wayne active homes",
            description="Phase 2 integration search",
            criteria=SearchCriteria(
                locations=[{"state": "OH", "counties": ["Wayne"]}],
                price={"max": 80000},
                beds={"min": 3},
                property_types=["single family"],
                statuses=["active"],
            ),
            notification_mode="immediate",
            cooldown_minutes=60,
        ),
        postgres_engine,
    )

    assert search.criteria_version == 1
    first_run = evaluate_saved_search(search.id, engine=postgres_engine)
    second_run = evaluate_saved_search(search.id, engine=postgres_engine)

    assert first_run.evaluated == 3
    assert first_run.matched == 1
    assert first_run.new_matches == 1
    assert first_run.recorded == 3
    assert second_run.evaluated == 3
    assert second_run.matched == 1
    assert second_run.new_matches == 0
    assert second_run.recorded == 0

    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE listings_current
                SET beds = 2, updated_at = CURRENT_TIMESTAMP
                WHERE source = 'sample_feed'
                  AND source_listing_id = '1001'
                """
            )
        )
    nonmatching_change = evaluate_saved_search(search.id, engine=postgres_engine)
    assert nonmatching_change.recorded == 1
    assert nonmatching_change.matched == 0
    assert nonmatching_change.new_matches == 0

    with postgres_engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE listings_current
                SET beds = 3, updated_at = CURRENT_TIMESTAMP
                WHERE source = 'sample_feed'
                  AND source_listing_id = '1001'
                """
            )
        )
    matching_change = evaluate_saved_search(search.id, engine=postgres_engine)
    assert matching_change.recorded == 1
    assert matching_change.matched == 1
    assert matching_change.new_matches == 1

    second_search = create_saved_search(
        SavedSearchCreate(
            name="All active sample homes",
            criteria=SearchCriteria(statuses=["Active"]),
        ),
        postgres_engine,
    )
    independent_run = evaluate_saved_search(second_search.id, engine=postgres_engine)
    assert independent_run.matched == 2
    assert independent_run.recorded == 3

    notification_update = update_saved_search(
        search.id,
        SavedSearchUpdate(notification_mode="digest", cooldown_minutes=1440),
        postgres_engine,
    )
    assert notification_update.criteria_version == 1

    criteria_update = update_saved_search(
        search.id,
        SavedSearchUpdate(
            criteria=SearchCriteria(
                locations=[{"state": "OH", "counties": ["Wayne"]}],
                price={"max": 60000},
                statuses=["active"],
            )
        ),
        postgres_engine,
    )
    assert criteria_update.criteria_version == 2

    third_run = evaluate_saved_search(search.id, engine=postgres_engine)
    assert third_run.recorded == 3
    assert third_run.matched == 0
    assert third_run.new_matches == 0

    evaluations = list_search_evaluations(search.id, engine=postgres_engine)
    assert len(evaluations) == 8
    assert any(item.became_match for item in evaluations)
    assert all(item.match_reasons or item.failed_reasons for item in evaluations)

    delete_saved_search(second_search.id, postgres_engine)
    delete_saved_search(search.id, postgres_engine)
    with postgres_engine.connect() as conn:
        remaining = conn.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM listing_search_evaluations
                WHERE saved_search_id = :saved_search_id
                """
            ),
            {"saved_search_id": search.id},
        )
    assert remaining == 0
