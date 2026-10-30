import unittest
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text

from app.providers.fixture import FixtureProvider
from app.providers.sync import ProviderSyncState, resume_cursor, sync_provider


def _listing(listing_id: str, **overrides):
    record = {
        "source": "fixture",
        "source_listing_id": listing_id,
        "address": f"{listing_id} Sample Street",
        "city": "Ann Arbor",
        "state": "MI",
        "zip": "48104",
        "list_price": 245000,
        "beds": 3,
        "baths": 2,
        "sqft": 1800,
        "property_type": "SingleFamilyResidence",
        "status": "Active",
        "days_on_market": 14,
        "first_seen_date": "2026-01-01",
        "last_seen_date": "2026-01-01",
        "price_per_sqft": 136.11,
    }
    record.update(overrides)
    return record


class FailingProvider(FixtureProvider):
    """Fixture provider that raises once when asked for one cursor."""

    def __init__(self, records, *, page_size, fail_at):
        super().__init__(records, page_size=page_size)
        self.fail_at = fail_at
        self.requested = []

    def fetch_updated_since(self, cursor=None):
        self.requested.append(cursor)
        if cursor == self.fail_at:
            self.fail_at = None
            raise ConnectionError("provider went away")
        return super().fetch_updated_since(cursor)


class RepeatingProvider(FixtureProvider):
    def fetch_updated_since(self, cursor=None):
        records, _ = super().fetch_updated_since(None)
        return records, "1"


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
                        alert_eligible BOOLEAN NOT NULL DEFAULT TRUE,
                        scoring_eligible BOOLEAN NOT NULL DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (source, source_listing_id)
                    )
                    """
                )
            )
            # Mirrors migration 0020_provider_sync_state.
            conn.execute(
                text(
                    """
                    CREATE TABLE provider_sync_state (
                        provider_name TEXT PRIMARY KEY,
                        last_status TEXT NOT NULL,
                        last_cursor TEXT,
                        last_error TEXT,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        started_at TIMESTAMP,
                        completed_at TIMESTAMP,
                        pages_committed INTEGER NOT NULL DEFAULT 0,
                        records_committed INTEGER NOT NULL DEFAULT 0
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE ingest_errors (
                        source TEXT NOT NULL,
                        stage TEXT NOT NULL,
                        source_record_id TEXT,
                        payload_hash TEXT NOT NULL,
                        error_code TEXT NOT NULL,
                        error_message TEXT NOT NULL,
                        raw_record_json TEXT NOT NULL,
                        UNIQUE (source, stage, payload_hash, error_code)
                    )
                    """
                )
            )

    def _state(self, provider_name):
        with self.engine.connect() as conn:
            return conn.execute(
                text("SELECT * FROM provider_sync_state WHERE provider_name = :name"),
                {"name": provider_name},
            ).one()

    def _listing_count(self):
        with self.engine.connect() as conn:
            return conn.scalar(text("SELECT COUNT(*) FROM listings_current"))

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

        state_row = self._state(provider.name)
        self.assertEqual(state_row.provider_name, provider.name)
        self.assertEqual(state_row.last_status, "completed")
        self.assertIsNone(state_row.last_cursor)
        self.assertIsNotNone(state_row.completed_at)

    def test_sync_reads_every_page_and_counts_them(self) -> None:
        records = [_listing(f"id-{index}") for index in range(5)]
        provider = FixtureProvider(records, page_size=2)

        result = sync_provider(provider, engine=self.engine)

        self.assertEqual(result.pages, 3)
        self.assertEqual(result.synced_count, 5)
        self.assertEqual(self._listing_count(), 5)
        state_row = self._state(provider.name)
        self.assertEqual(state_row.pages_committed, 3)
        self.assertEqual(state_row.records_committed, 5)

    def test_interrupted_sync_resumes_from_last_committed_page(self) -> None:
        records = [_listing(f"id-{index}") for index in range(5)]
        provider = FailingProvider(records, page_size=2, fail_at="4")

        with self.assertRaises(ConnectionError):
            sync_provider(provider, engine=self.engine)

        failed = self._state(provider.name)
        self.assertEqual(failed.last_status, "failed")
        self.assertEqual(failed.last_cursor, "4")
        self.assertIn("provider went away", failed.last_error)
        self.assertEqual(failed.records_committed, 4)
        self.assertEqual(self._listing_count(), 4)

        result = sync_provider(provider, engine=self.engine)

        self.assertEqual(result.resumed_from, "4")
        self.assertEqual(result.pages, 1)
        self.assertEqual(provider.requested, [None, "2", "4", "4"])
        self.assertEqual(self._listing_count(), 5)
        completed = self._state(provider.name)
        self.assertEqual(completed.last_status, "completed")
        self.assertEqual(completed.records_committed, 5)
        self.assertIsNone(completed.last_error)

    def test_repeated_cursor_fails_instead_of_looping(self) -> None:
        provider = RepeatingProvider([_listing("id-0")])

        with self.assertRaisesRegex(RuntimeError, "repeated cursor"):
            sync_provider(provider, engine=self.engine)

        self.assertEqual(self._state(provider.name).last_status, "failed")

    def test_page_limit_stops_runaway_pagination(self) -> None:
        records = [_listing(f"id-{index}") for index in range(5)]
        provider = FixtureProvider(records, page_size=1)

        with self.assertRaisesRegex(RuntimeError, "exceeded 3 pages"):
            sync_provider(provider, engine=self.engine, max_pages=3)

    def test_bad_records_are_quarantined_without_losing_the_page(self) -> None:
        records = [
            _listing("good"),
            _listing("", address="No Identifier Rd"),
            _listing("bad-date", first_seen_date="not-a-date"),
        ]
        provider = FixtureProvider(records)

        result = sync_provider(provider, engine=self.engine)
        sync_provider(provider, engine=self.engine)

        self.assertEqual(result.synced_count, 1)
        self.assertEqual(result.quarantined_count, 2)
        self.assertEqual(self._listing_count(), 1)
        with self.engine.connect() as conn:
            codes = conn.execute(
                text("SELECT error_code FROM ingest_errors ORDER BY error_code")
            ).scalars().all()
        self.assertEqual(codes, ["invalid_date", "missing_identifier"])


class ResumeCursorTests(unittest.TestCase):
    NOW = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)

    def _state(self, status, cursor="200", age=timedelta(minutes=5)):
        return ProviderSyncState(
            last_status=status, last_cursor=cursor, updated_at=self.NOW - age
        )

    def test_recent_failed_or_interrupted_runs_resume(self) -> None:
        self.assertEqual(resume_cursor(self._state("failed"), now=self.NOW), "200")
        self.assertEqual(
            resume_cursor(self._state("in_progress"), now=self.NOW), "200"
        )

    def test_completed_or_stale_runs_start_over(self) -> None:
        self.assertIsNone(resume_cursor(self._state("completed"), now=self.NOW))
        self.assertIsNone(
            resume_cursor(
                self._state("failed", age=timedelta(hours=7)), now=self.NOW
            )
        )
        self.assertIsNone(resume_cursor(self._state("failed", cursor=None), now=self.NOW))
        self.assertIsNone(resume_cursor(None, now=self.NOW))


if __name__ == "__main__":
    unittest.main()
