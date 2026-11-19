from __future__ import annotations

import json
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

from app.ingest.load_sample_listings import ingest_records
from app.providers.rentcast import RentCastProvider
from app.searches.models import SavedSearchCreate, SearchCriteria
from app.searches.service import create_saved_search, evaluate_saved_search
from app.transforms.backfill_listing_coordinates import backfill_listing_coordinates
from app.transforms.snapshot_listings import snapshot_current_listings

pytestmark = pytest.mark.integration
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture()
def engine():
    if not TEST_DATABASE_URL:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")
    if not (make_url(TEST_DATABASE_URL).database or "").endswith("_test"):
        pytest.fail("TEST_DATABASE_URL database name must end with '_test'")
    schema = f"coords_{uuid4().hex}"
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
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
            connection.commit()
        engine.alembic_config = config
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin.dispose()


def _listing(listing_id: str, **overrides) -> dict:
    record = {
        "source": "rentcast",
        "source_listing_id": listing_id,
        "address": f"{listing_id} Main St",
        "city": "Wooster",
        "state": "OH",
        "zip": "44691",
        "list_price": 200000,
        "beds": 3,
        "baths": 2,
        "sqft": 1500,
        "property_type": "SingleFamilyResidence",
        "status": "Active",
        "days_on_market": 10,
        "first_seen_date": "2026-10-01",
        "last_seen_date": "2026-10-20",
        "price_per_sqft": Decimal("133.33"),
    }
    record.update(overrides)
    return record


def _coordinates(engine, listing_id: str):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT latitude, longitude FROM listings_current "
                "WHERE source = 'rentcast' AND source_listing_id = :id"
            ),
            {"id": listing_id},
        ).one()


def _add_raw(engine, listing_id: str, payload: dict, *, hours_ago: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO listings_raw (
                    source, source_listing_id, payload_hash, raw_record_json, ingested_at
                )
                VALUES (
                    'rentcast', :id, :hash, CAST(:payload AS JSONB),
                    CURRENT_TIMESTAMP - make_interval(hours => :hours_ago)
                )
                """
            ),
            {
                "id": listing_id,
                "hash": uuid4().hex,
                "payload": json.dumps(payload),
                "hours_ago": hours_ago,
            },
        )


def test_migration_round_trip_and_paired_constraint(engine):
    config = engine.alembic_config
    with engine.connect() as connection:
        config.attributes["connection"] = connection
        command.downgrade(config, "0022_rental_assumption_policies")
        connection.commit()
        columns = connection.scalars(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND table_name = 'listings_current'"
            )
        ).all()
        assert "latitude" not in columns
        command.upgrade(config, "head")
        connection.commit()

    ingest_records([_listing("pair")], engine=engine)
    with pytest.raises(IntegrityError, match="ck_listings_current_coordinates_paired"):
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE listings_current SET latitude = 40.8 WHERE source_listing_id = 'pair'")
            )


def test_rentcast_coordinates_are_stored_without_new_history(engine, capsys):
    provider = RentCastProvider(api_key="test-key", zip_code="44691", session=object())
    raw = {
        "id": "1-Main-St",
        "addressLine1": "1 Main St",
        "city": "Wooster",
        "state": "OH",
        "zipCode": "44691",
        "price": 200000,
        "squareFootage": 1500,
        "status": "Active",
        "mlsNumber": "rc-1",
    }
    ingest_records([provider.normalize(raw)], engine=engine)
    assert snapshot_current_listings(engine) == 1
    assert _coordinates(engine, "rc-1") == (None, None)

    raw |= {"latitude": 40.79860149, "longitude": -81.9375}
    counts = ingest_records([provider.normalize(raw)], engine=engine)
    assert counts["current_changed"] == 1
    assert _coordinates(engine, "rc-1") == (Decimal("40.798601"), Decimal("-81.937500"))
    # Coordinates are not tracked in listing_history.
    assert snapshot_current_listings(engine) == 0

    again = ingest_records([provider.normalize(raw)], engine=engine)
    assert again["current_changed"] == 0
    assert again["raw_inserted"] == 0
    with engine.connect() as conn:
        stored = conn.scalar(
            text(
                "SELECT raw_record_json FROM listings_raw "
                "WHERE source_listing_id = 'rc-1' AND raw_record_json ? 'latitude' "
                "AND raw_record_json ->> 'latitude' IS NOT NULL"
            )
        )
    assert stored["latitude"] == 40.79860149


def test_backfill_uses_latest_usable_payload_and_is_idempotent(engine, capsys):
    ids = ["latest", "strings", "placeholder", "none", "already", "out-of-range"]
    ingest_records([_listing(listing_id) for listing_id in ids], engine=engine)
    ingest_records(
        [_listing("already", latitude=41.0, longitude=-82.0)], engine=engine
    )

    _add_raw(engine, "latest", {"latitude": 40.1, "longitude": -81.1}, hours_ago=48)
    _add_raw(engine, "latest", {"latitude": 40.2, "longitude": -81.2}, hours_ago=24)
    # The newest payload has no coordinates, so the newest usable one wins.
    _add_raw(engine, "latest", {"latitude": None, "longitude": None}, hours_ago=1)
    _add_raw(engine, "strings", {"latitude": "40.7986001", "longitude": "-81.9375"}, hours_ago=1)
    _add_raw(engine, "placeholder", {"latitude": 0, "longitude": 0}, hours_ago=1)
    _add_raw(engine, "out-of-range", {"latitude": 95, "longitude": -81.9}, hours_ago=1)
    _add_raw(engine, "already", {"latitude": 10, "longitude": 10}, hours_ago=0)

    dry = backfill_listing_coordinates(engine, dry_run=True)
    assert (dry.missing_before, dry.fillable, dry.updated) == (5, 2, 0)
    assert _coordinates(engine, "latest") == (None, None)

    result = backfill_listing_coordinates(engine, source="rentcast")
    assert (result.missing_before, result.fillable, result.updated) == (5, 2, 2)
    assert result.still_missing == 3
    assert _coordinates(engine, "latest") == (Decimal("40.200000"), Decimal("-81.200000"))
    assert _coordinates(engine, "strings") == (Decimal("40.798600"), Decimal("-81.937500"))
    assert _coordinates(engine, "placeholder") == (None, None)
    assert _coordinates(engine, "none") == (None, None)
    assert _coordinates(engine, "out-of-range") == (None, None)
    assert _coordinates(engine, "already") == (Decimal("41.000000"), Decimal("-82.000000"))

    rerun = backfill_listing_coordinates(engine)
    assert (rerun.missing_before, rerun.updated) == (3, 0)
    assert backfill_listing_coordinates(engine, source="other").missing_before == 0
    assert "Coordinate backfill complete" in capsys.readouterr().out


def test_saved_search_bounding_box_on_postgres(engine):
    ingest_records(
        [
            _listing("inside", latitude=40.80, longitude=-81.94),
            _listing("outside", latitude=40.95, longitude=-81.94),
            _listing("unknown"),
        ],
        engine=engine,
    )
    search = create_saved_search(
        SavedSearchCreate(
            name="Wooster box",
            criteria=SearchCriteria(
                bounding_box={
                    "min_latitude": 40.75,
                    "max_latitude": 40.85,
                    "min_longitude": -82.0,
                    "max_longitude": -81.9,
                }
            ),
        ),
        engine,
    )

    run = evaluate_saved_search(search.id, engine=engine)

    by_id = {result.source_listing_id: result for result in run.results}
    assert run.evaluated == 3
    assert run.matched == 1
    assert by_id["inside"].is_match is True
    assert by_id["outside"].failed_reasons[0].message == "Listing is outside the bounding box."
    assert by_id["unknown"].failed_reasons[0].message == "Listing coordinates are missing."
    assert evaluate_saved_search(search.id, engine=engine).recorded == 0
