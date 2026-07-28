# Real Estate Agent Development Roadmap

Last reviewed: July 18, 2026

## Goal

Build a reliable application that retrieves authorized real-estate listings, stores current and historical listing data, evaluates configurable search criteria, and sends deduplicated Slack notifications when meaningful events occur.

## Current state

Phase 1 stabilization is implemented in commit `8b892c7`. Phase 2 configurable
saved searches are implemented in commit `17cee39`. Both commits are pushed to
`origin/main`. The repository now contains:

- One authoritative Alembic-managed PostgreSQL schema
- Immutable raw JSON payloads and normalized current listings
- Idempotent sample ingestion and change-aware listing history
- One authoritative county-level market scoring implementation
- Slack alerts for price drops and potential deals
- A FastAPI API with database-aware health checking
- Docker Compose services for PostgreSQL and the API, including readiness checks
- Unit tests and opt-in isolated PostgreSQL integration tests
- Setup, migration, pipeline, API, testing, and rollback documentation
- Versioned saved searches with validated, explainable criteria evaluation
- Stable listing/search event fingerprints and persisted match transitions
- A PostgreSQL-backed price-drop alert outbox with atomic worker claims,
  retries, and visible permanent failures

The application is not yet connected to a live listing provider. The primary pipeline still loads `data/sample_listings.csv`.

## Remaining issues

The original Phase 1 schema, configuration, ingestion, scoring, API, and
documentation issues are resolved. Remaining work includes:

1. Continue expanding PostgreSQL integration coverage as later phases are
   implemented; the Phase 1 and Phase 2 integration suite now passes against
   the isolated `realestate_test` Docker database.
2. An existing prototype database is not automatically reconciled with the new
   clean Alembic baseline. Existing data must be exported and inspected before
   rebuilding or writing a one-time migration.
3. The main pipeline does not run county data loading, market scoring, or
   potential-deal Slack alerts.
4. The legacy `potential_deals` score threshold remains hard-coded, although
   saved-search criteria are now configurable.
5. Geographic bounding-box criteria are deferred until a listing provider
   supplies reliable latitude/longitude fields.
6. County-wide median price alone is too coarse to identify a genuine deal.
7. There is no authorized live listing provider.
8. There is no production scheduler or long-running worker process. The
   queue and worker commands are separate, but must still be scheduled.
   Structured logging, pipeline-run history, authentication, monitoring, and
   backup automation are also absent.
9. `/health` verifies database connectivity but cannot report listing freshness
   until live synchronization exists.

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

## Phase 1: Stabilize the prototype — Implemented

Implementation commit: `8b892c7`

Validation completed:

- 19 unit/API tests passed
- Ruff lint and formatting checks passed
- Python compilation and dependency checks passed
- Alembic upgrade and downgrade SQL generation passed
- Four PostgreSQL integration tests were added and safely skipped because no
  `_test` database was available
- No Slack messages were sent during implementation or validation

### Completed work

- [x] Chose the source-aware `listing_market_scores` schema as authoritative.
- [x] Replaced ambiguous numbered SQL schema files with Alembic.
- [x] Made `listings_raw` an immutable JSONB payload store with content hashes.
- [x] Consolidated configuration into one settings module and SQLAlchemy engine.
- [x] Standardized database settings on `DB_*`, with optional `DATABASE_URL`.
- [x] Added `.env.example` with safe placeholders and no secrets.
- [x] Replaced the broken duplicate API scoring query with the authoritative
      county-market scorer.
- [x] Normalized status, property type, state, ZIP, dates, and integer fields.
- [x] Added PostgreSQL readiness checks and API startup to Docker Compose.
- [x] Added comprehensive README documentation.
- [x] Added unit and opt-in isolated PostgreSQL integration tests.
- [x] Added `--skip-alerts` for safe pipeline validation.
- [x] Made current upserts, raw ingestion, history snapshots, scoring timestamps,
      and alert-ledger recording idempotent where possible.

### Acceptance status

- [x] PostgreSQL startup and schema initialization are documented.
- [x] All modules use the same database settings and engine.
- [ ] Execute sample ingestion against a fresh live PostgreSQL database.
- [ ] Execute sample ingestion twice and confirm database-level idempotency.
- [x] API imports and non-database endpoint tests pass.
- [x] Tests cover ingestion, normalization, price changes, scoring stability,
      alert-ledger idempotency, and dry-run alert suppression.
- [x] No credentials are committed.

The two unchecked criteria have automated integration tests ready; they only
require a configured PostgreSQL `_test` database.

## Phase 2: Build configurable saved searches — Implemented

Implementation commit: `17cee39`

The implementation is committed and pushed. Live database integration
validation remains pending because PostgreSQL/Docker was not available in the
implementation environment.

### Completed work

- [x] Added `saved_searches` and `listing_search_evaluations` in Alembic
      revision `0002_saved_searches`.
- [x] Stored strict, validated, versioned criteria as JSONB.
- [x] Built one pure criteria evaluator shared by the pipeline, API, and CLI.
- [x] Added state, county, city, ZIP, price, beds, baths, property type,
      listing status, square feet, acreage, days-on-market, price-drop, and
      deal-score filters.
- [x] Kept binary saved-search matching separate from county-market ranking.
- [x] Stored structured matching and failure reasons.
- [x] Incremented `criteria_version` only when criteria change.
- [x] Added immediate/digest mode, cooldown, and enabled/disabled controls.
- [x] Added stable listing and event fingerprints with database uniqueness.
- [x] Recorded explicit nonmatch-to-match transitions without sending Slack.
- [x] Added CRUD, evaluation, and evaluation-history API endpoints.
- [x] Added a safe evaluation CLI and enabled-search pipeline step.
- [ ] Add optional geographic bounding-box criteria after provider coordinates
      are available.

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

### Acceptance status

- [x] Unit tests verify clear matching and non-matching reasons.
- [x] Unit tests verify validation, normalization, inclusive boundaries, missing
      values, location OR semantics, and price-drop thresholds.
- [x] Integration tests cover multiple searches evaluating the same listing.
- [x] Integration tests cover unchanged reruns producing no new event.
- [x] Integration tests cover one new event per meaningful listing transition.
- [x] Integration tests cover criteria versioning and non-versioning notification
      setting edits.
- [x] Notification semantics and the absence of Phase 2 Slack delivery are
      documented.
- [ ] Execute the Phase 2 integration test against a PostgreSQL `_test` database.

Local validation completed:

- 31 non-database tests passed
- Five PostgreSQL integration tests collected and safely skipped
- Ruff lint and format checks passed
- Python compilation and dependency checks passed
- Alembic upgrade and downgrade SQL generation passed
- No Slack messages were sent

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

- [x] Introduce an alert outbox for price drops instead of sending directly
  from detection logic.
- [x] Use `pending`, `processing`, `sent`, `retryable_failed`, and
  `permanent_failed` states.
- [x] Add attempt counts, next-attempt timestamps, and error summaries.
- [x] Claim pending alerts safely so multiple workers cannot send the same event.
- [x] Uniquely constrain the price-drop event identity in PostgreSQL.
- [x] Retry temporary Slack failures with exponential backoff.
- [x] Separate price-drop detection/queueing from the delivery worker command.
- Add jitter to retry delays.
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

1. If Docker/PostgreSQL is available, complete the two remaining Phase 1
   acceptance checks:
   - Start a fresh test database whose name ends in `_test`.
   - Run `python -m pytest -m integration`.
   - Run `python -m app.run_pipeline --skip-alerts` twice against the development
     database and confirm that the second unchanged run adds no raw payloads,
     current changes, or history snapshots.
2. Validate Phase 2 against PostgreSQL:
   - Apply `python -m alembic upgrade head`.
   - Create at least two saved searches through the API.
   - Run the pipeline twice with `--skip-alerts`.
   - Confirm the second unchanged run records no new search evaluations.
3. Begin Phase 3 by choosing the target geography and authorized RESO/API data
   provider before writing provider-specific ingestion code.
4. Do not connect saved-search match transitions to production Slack until the
   Phase 4 outbox/retry design is implemented.

## External references

- RESO specifications: https://www.reso.org/specs/
- RESO Web API transition guide and provider examples: https://www.reso.org/api-guide/
- Slack incoming webhooks: https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks
- Slack rate limits: https://docs.slack.dev/apis/web-api/rate-limits/
- Docker Compose startup/readiness guidance: https://docs.docker.com/compose/how-tos/startup-order/
