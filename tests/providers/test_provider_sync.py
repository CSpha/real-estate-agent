import unittest

from sqlalchemy import create_engine, text

from app.providers.fixture import FixtureProvider
from app.providers.sync import sync_provider


class ProviderSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:")
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE listings_raw (
                        source TEXT NOT NULL,
                        source_listing_id TEXT NOT NULL,
                        payload_hash TEXT NOT NULL,
                        raw_record_json TEXT NOT NULL,
                        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (source, source_listing_id, payload_hash)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE listings_current (
                        source TEXT NOT NULL,
                        source_listing_id TEXT NOT NULL,
                        address TEXT,
                        city TEXT,
                        state TEXT,
                        zip TEXT,
                        list_price REAL,
                        beds REAL,
                        baths REAL,
                        sqft REAL,
                        lot_size_acres REAL,
                        property_type TEXT,
                        status TEXT,
                        days_on_market INTEGER,
                        first_seen_date TEXT,
                        last_seen_date TEXT,
                        price_per_sqft REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (source, source_listing_id)
                    )
                    """
                )
            )

    def test_sync_provider_is_idempotent_and_records_state(self) -> None:
        provider = FixtureProvider()

        first_result = sync_provider(provider, engine=self.engine)
        second_result = sync_provider(provider, engine=self.engine)

        self.assertEqual(first_result.synced_count, 1)
        self.assertEqual(first_result.updated_count, 1)
        self.assertEqual(first_result.skipped_count, 0)

        self.assertEqual(second_result.synced_count, 1)
        self.assertEqual(second_result.updated_count, 0)
        self.assertEqual(second_result.skipped_count, 1)

        with self.engine.connect() as conn:
            state_row = conn.execute(
                text("SELECT provider_name, last_status FROM provider_sync_state WHERE provider_name = :provider_name"),
                {"provider_name": provider.name},
            ).one()

        self.assertEqual(state_row.provider_name, provider.name)
        self.assertEqual(state_row.last_status, "completed")


if __name__ == "__main__":
    unittest.main()
