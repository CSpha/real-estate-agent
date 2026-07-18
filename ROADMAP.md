# Real Estate Agent Development Roadmap

Last reviewed: July 15, 2026

## Goal

Build a reliable application that retrieves authorized real-estate listings, stores current and historical listing data, evaluates configurable search criteria, and sends deduplicated Slack notifications when meaningful events occur.

## Current state

The repository contains the main pieces of a prototype:

- PostgreSQL tables for current listings, history, market scores, and sent alerts
- Sample CSV ingestion and current-listing upserts
- Listing snapshots and price-change detection
- County-level market scoring
- Slack alerts for price drops and potential deals
- A small FastAPI API
- Docker Compose configuration for PostgreSQL

The application is not yet connected to a live listing provider. The primary pipeline still loads `data/sample_listings.csv`.

## Known issues

These should be resolved before adding a live listing feed:

1. `load_sample_listings.py` inserts normalized listing columns into `listings_raw`, but `sql/005_create_listings_raw.sql` defines `listings_raw` as a raw JSON table.
2. SQL migrations `007_create_listing_market_scores.sql` and `009_create_listing_market_scores.sql` define incompatible versions of the same table.
3. Database configuration is split between `POSTGRES_*` and `DB_*` environment-variable conventions.
4. The main pipeline does not run county data loading, market scoring, or potential-deal alerts.
5. Every run snapshots every listing, including unchanged listings.
6. Potential-deal alerts use `scored_at` as their event identity and can be repeated after an unchanged listing is rescored.
7. The API `/score` SQL references `l.status`, although the CTE exposes `current_status`.
8. Criteria and score thresholds are hard-coded in multiple places.
9. County-wide median price alone is too coarse to identify a genuine deal.
10. There are no automated tests, production scheduler, worker, structured logs, or pipeline-run records.
11. The API `/health` endpoint does not verify the database or listing-sync freshness.
12. The README is empty.

## Design principles

- Use only an authorized listing feed and follow its licensing, display, and retention requirements.
- Prefer a RESO Web API/Data Dictionary-compatible source when available.
- Keep provider-specific fields behind a provider adapter and normalize them into one internal listing model.
- Separate objective search filters from optional deal-ranking scores.
- Store raw provider payloads so normalization issues can be investigated.
- Insert history and generate events only for meaningful changes.
- Give every alert event a stable identity so retries do not create duplicates.
- Keep alert thresholds configurable and versioned.
- Avoid changing notification behavior without testing it against a Slack test channel.

## Phase 1: Stabilize the prototype

### Work

- Choose one authoritative database schema.
- Replace ambiguous numbered SQL execution with a migration system, preferably Alembic.
- Resolve the `listings_raw` schema/loader mismatch.
- Consolidate database configuration into one settings module and one connection provider.
- Standardize on one environment-variable convention.
- Add `.env.example` with safe placeholders and no secrets.
- Fix the API `/score` SQL bug.
- Normalize status and property-type values during ingestion.
- Decide whether the API score or county-market score is authoritative; remove the duplicate implementation.
- Add a PostgreSQL health check to Docker Compose.
- Add application and PostgreSQL services to Compose if local containerized operation is desired.
- Write README setup, migration, pipeline, API, and test instructions.
- Add unit and PostgreSQL integration tests.

### Acceptance criteria

- A new developer can start PostgreSQL and initialize the schema from documented commands.
- All modules use the same database settings.
- Sample ingestion runs successfully against a fresh database.
- Re-running ingestion is safe and produces the documented results.
- API imports and affected endpoints run without SQL/runtime errors.
- Tests cover ingestion, normalization, price-change detection, and alert deduplication.
- No credentials are committed.

## Phase 2: Build configurable saved searches

### Work

- Add a `saved_searches` table.
- Store validated, versioned criteria, initially as JSONB.
- Build one shared criteria evaluator used by the pipeline and API.
- Support initial filters such as:
  - State, county, city, ZIP, and optional geographic bounds
  - Minimum and maximum price
  - Minimum beds and baths
  - Property types
  - Listing statuses
  - Minimum/maximum square feet and acreage when available
  - Maximum days on market
  - Minimum price-drop amount and percentage
  - Optional minimum deal score
- Separate binary search matching from deal ranking.
- Store the exact reasons a listing matched.
- Version criteria so changing a search has explicit notification semantics.
- Add notification controls such as immediate, digest, cooldown, and enabled/disabled.
- Create stable event fingerprints based on listing, saved search, criteria version, event type, and material listing state.

### Example criteria

```json
{
  "locations": [{"state": "OH", "counties": ["Wayne"]}],
  "price": {"min": 50000, "max": 175000},
  "beds": {"min": 3},
  "baths": {"min": 1.5},
  "property_types": ["SingleFamilyResidence"],
  "statuses": ["Active"],
  "max_days_on_market": 60,
  "price_drop": {"min_amount": 5000, "min_percent": 3},
  "deal_score": {"min": 75}
}
```

### Acceptance criteria

- Multiple saved searches can evaluate the same listing independently.
- A listing produces a clear list of matching and non-matching reasons.
- Re-running an unchanged listing and unchanged search produces no new event.
- A meaningful listing change can produce exactly one new event.
- Editing criteria creates predictable, documented notification behavior.

## Phase 3: Connect an authorized live listing provider

### Provider decision

Before implementation, select the target geography and obtain authorized access from an MLS, broker, or listing-data vendor. Prefer a RESO Web API-compatible provider. Do not scrape consumer listing sites unless their terms and an explicit agreement authorize it.

### Work

- Define a provider interface with operations such as:

```python
fetch_updated_since(cursor)
normalize(raw_listing)
next_cursor(response)
```

- Implement authentication and token refresh.
- Read provider metadata and map provider/RESO fields into the internal model.
- Support pagination and incremental retrieval by provider modification timestamp or cursor.
- Store raw payloads before normalization.
- Add rate limiting, bounded retries, exponential backoff, and request timeouts.
- Persist synchronization cursors only after a page/batch commits successfully.
- Track provider listing ID, provider modification timestamp, listing URL, photos, and attribution fields.
- Handle active, coming soon, pending, sold, expired, canceled, withdrawn, and deleted/missing listings as permitted by the feed.
- Detect listings that disappear from the feed without immediately deleting history.
- Record every sync run with start/end time, cursor, counts, warnings, and failure details.
- Document licensing, attribution, display, photo, and retention requirements.

### Acceptance criteria

- An interrupted sync resumes without skipping or duplicating listings.
- Pagination is verified with automated tests or provider fixtures.
- Provider throttling and transient errors are retried safely.
- Bad individual records are quarantined without losing the whole batch.
- Current listing state and meaningful history are correct after repeated syncs.
- Feed attribution and retention rules are documented and enforced.

## Phase 4: Make notifications reliable

### Work

- Introduce an alert outbox instead of sending directly from detection logic.
- Use states such as `pending`, `sending`, `sent`, `retryable_failed`, and `permanent_failed`.
- Add attempt counts, next-attempt timestamps, and error summaries.
- Claim pending alerts safely so multiple workers cannot send the same event.
- Make the event fingerprint uniquely constrained in PostgreSQL.
- Retry temporary Slack failures with backoff and jitter.
- Honor Slack HTTP 429 `Retry-After` responses.
- Pace incoming-webhook messages at no more than approximately one per second.
- Send malformed payloads and invalid/revoked webhook failures to permanent-failure handling.
- Add Slack Block Kit formatting with:
  - Address and listing URL
  - Primary photo when permitted
  - Current price and previous price
  - Match reasons
  - Relevant listing attributes
  - Data source/attribution
- Add digest mode to avoid flooding a channel.
- Add a dry-run/test-channel mode.

### Acceptance criteria

- Retrying a failed job never creates a duplicate Slack alert.
- Slack rate limiting is handled automatically.
- Permanent failures are visible and do not block later alerts.
- Alerts link to the listing and explain why it matched.
- A test mode can validate messages without notifying the production channel.

## Phase 5: Operate it as an application

### Work

- Containerize the API and background worker.
- Add a scheduler for incremental provider synchronization and alert processing.
- Use structured logging with run IDs, provider IDs, and event IDs.
- Add pipeline-run tables and operational metrics.
- Change `/health` to report database connectivity and last successful sync freshness.
- Add readiness and liveness endpoints if deployed behind an orchestrator.
- Add authentication and authorization before publicly exposing searches, listings, or alerts.
- Validate and limit API pagination and expensive queries.
- Add PostgreSQL backups and a restore test.
- Add CI for formatting, linting, tests, migrations, and secret scanning.
- Add monitoring for stale syncs, repeated provider failures, alert backlog, and database capacity.
- Document deployment, rollback, backup, restore, credential rotation, and incident response.

### Acceptance criteria

- The application starts from documented deployment commands.
- Database readiness is checked before dependent services start.
- A failed or stale listing sync generates an operational alert.
- Database backup restoration has been tested.
- CI blocks broken migrations and failing tests.
- Secrets can be rotated without code changes.

## Recommended data model additions

Likely additions include:

- `saved_searches`
- `listing_changes` or `listing_events`
- `listing_search_matches`
- `alert_outbox`
- `provider_sync_state`
- `pipeline_runs`
- `ingest_errors` or a dead-letter/quarantine table

Important stable identifiers:

- Listing: `(provider/source, provider listing ID)`
- Search version: `(saved search ID, criteria version)`
- Material state: normalized fingerprint of tracked listing fields
- Alert event: `(listing, search version, event type, material state)`

## Scoring recommendations

County median price should remain contextual information rather than the sole definition of a deal. Improve ranking by comparing similar properties using available fields such as:

- Property type
- ZIP, city, or smaller market area
- Beds and baths
- Square-foot band
- Acreage
- Age/year built
- Listing status
- Days on market
- Price per square foot
- Recent comparable sale period

Keep the score explainable. Store each component and its contribution instead of only a final number.

## Validation strategy

The test suite should eventually include:

- Normalization unit tests with provider fixtures
- Criteria boundary tests
- Status-enumeration tests
- Price-change and fingerprint tests
- Alert idempotency tests
- Migration tests against fresh PostgreSQL
- Repeated-ingestion integration tests
- Provider pagination, throttling, and retry tests
- Slack 200, 429, temporary 5xx, and permanent 4xx tests
- End-to-end dry-run pipeline test

## Next session: start here

Begin with Phase 1 and keep the first change small:

1. Inspect the actual development database schema, if a database is available.
2. Decide which `listing_market_scores` definition is authoritative.
3. Decide that `listings_raw` will store immutable raw JSON payloads plus ingestion metadata.
4. Introduce an initial migration baseline or Alembic.
5. Consolidate database settings and add `.env.example`.
6. Fix sample ingestion so it writes raw JSON and normalized current listings consistently.
7. Run sample ingestion twice and verify current/history/idempotency behavior.
8. Add tests for the repaired flow before moving to saved searches.

Do not change price-drop or potential-deal Slack behavior during this stabilization work without first using a dry-run or test channel, because an accidental change could spam the production Slack channel.

## External references

- RESO specifications: https://www.reso.org/specs/
- RESO Web API transition guide and provider examples: https://www.reso.org/api-guide/
- Slack incoming webhooks: https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks
- Slack rate limits: https://docs.slack.dev/apis/web-api/rate-limits/
- Docker Compose startup/readiness guidance: https://docs.docker.com/compose/how-tos/startup-order/

