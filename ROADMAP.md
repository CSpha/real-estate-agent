# Real Estate Agent Development Roadmap

Last reviewed: July 29, 2026

## Goal

Build a reliable application that retrieves authorized real-estate listings, stores current and historical listing data, evaluates configurable search criteria, scores opportunities with transparent market context, and sends deduplicated Slack notifications when meaningful events occur.

## Current state

The repository has moved beyond the original Phase 1/Phase 2 milestone notes and now includes:

- One authoritative Alembic-managed PostgreSQL schema
- Immutable raw JSON payloads and normalized current listings
- An auditable, idempotent property-level comparable-sales schema and CSV importer
- Versioned deterministic comparable selection with visible fallback tiers
- Explainable comparable valuation using weighted-median price-per-square-foot logic, outlier safeguards, value ranges, and confidence components
- Immutable valuation snapshots and a versioned 40-point comparable-discount
  component for the in-progress Deal Score v2
- A versioned 15-point listing-opportunity component using cumulative price
  reductions, reduction frequency, and recency without future-data leakage
- A versioned 15-point days-on-market component using fresh point-in-time
  listing history and county median context
- Idempotent sample ingestion and change-aware listing history
- One authoritative county-level market scoring implementation
- Mortgage-rate outlook and backtest analysis modules with signal-based scoring
- Slack alerts for price drops and potential deals
- A FastAPI API with database-aware health checking
- Docker Compose services for PostgreSQL and the API, including readiness checks
- Unit tests and opt-in isolated PostgreSQL integration tests
- Setup, migration, pipeline, API, testing, and rollback documentation
- Versioned saved searches with validated, explainable criteria evaluation
- Stable listing/search event fingerprints and persisted match transitions
- A PostgreSQL-backed price-drop alert outbox with atomic worker claims, retries, and visible permanent failures

The application is still not connected to a live listing provider. The primary pipeline still loads sample data from `data/sample_listings.csv`.

## Near-term priorities

The core prototype is now substantially more capable than the original roadmap suggested. The remaining work is mostly about hardening the system and connecting it to a real data source.

1. Add a live provider adapter and replace the sample-data-only pipeline path.
2. Add a documented migration path for existing prototype databases so they can be reconciled with the Alembic baseline.
3. Finish wiring the pipeline so county-market data and potential-deal alert queueing run consistently in the same flow.
4. Replace the remaining hard-coded deal-score behavior with versioned, configurable thresholds.
5. Add geographic bounding-box criteria once provider coordinates are available.
6. Continue improving the deal-scoring model so it combines comparable valuation, local market context, price-change history, and confidence more transparently.
7. Add an operational scheduler and long-running worker process for sync and alert processing.
8. Add structured logging, pipeline-run history, authentication, monitoring, and backup automation.
9. Extend `/health` to report sync freshness and backlog health, not just database connectivity.

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

The current county-median score remains implemented as coarse market context.
The property-level comparable-sales schema, CSV importer, deterministic
selection tiers, and comparable valuation with confidence scoring are
implemented. Immutable valuation persistence, the comparable-discount
component, listing-opportunity component, and days-on-market component are also
implemented. Market momentum, liquidity, confidence aggregation, backfill, and
activation remain planned. Deal Score v2 must not replace the current score
until those remaining inputs and tests are complete.

### Proposed Deal Score v2

Use a transparent 100-point score:

- 40 points: discount to an estimated comparable value
- 15 points: listing opportunity, including cumulative price reductions and
  reduction frequency
- 15 points: days on market relative to the local median
- 15 points: local price trend or market momentum
- 10 points: local liquidity and inventory conditions
- 5 points: data confidence and comparable-sample quality

County median sale price should contribute contextual information but should
not independently define a deal. Price per square foot remains an input to
comparable valuation rather than a separately scored component, avoiding
double-counting the same evidence.

### Comparable selection

Prefer recent sold properties with:

- The same property type
- The same ZIP or Census tract when sample size permits
- Similar beds and baths
- A bounded square-foot range
- Similar lot size
- Similar age or year-built range
- A documented maximum sale age

Widen geography or similarity bands only when the initial comparable set is too
small. Record which fallback level was used.

### Separate subscores

Do not hide every investment concern inside one number. Store and expose:

- `deal_score`: purchase price relative to supported market value
- `rental_score`: rent yield, operating costs, and estimated cash flow
- `market_momentum_score`: price trend, inventory, and market velocity
- `risk_score`: hazard, insurance, tax, condition, and data risks
- `confidence_score`: comparable count, recency, similarity, and missing data

The initial Deal Score v2 implementation should not require rental or risk data;
those subscores can remain unavailable until reliable sources are connected.

### Explainability and safeguards

Persist each input, normalized component score, weight, contribution, final
score, scoring version, and human-readable reason. Missing data must reduce
confidence rather than silently receiving a favorable score.

Do not use protected-class data or demographic proxies to value or rank
properties. Test scoring results for geographic bias before using the score in
consumer-facing or credit-related decisions.

### Acceptance criteria

- Re-running unchanged inputs produces the same score and timestamp.
- A listing is never compared across incompatible property types.
- Comparable fallback behavior is deterministic and visible.
- Missing or sparse comparable data produces a lower confidence score.
- Every final score can be reconstructed from persisted component values.
- Score weights and thresholds are versioned.
- Tests cover boundary values, missing data, outliers, and small comparable
  samples.

## Mortgage-rate intelligence

Add financing context alongside Deal Score v2. Mortgage-rate information should
remain a separate, timestamped analysis rather than being hidden inside the
property deal score because an individual borrower's actual rate depends on
credit, loan structure, down payment, points, lender, and location.

### Rate data

Store:

- Current national average 30-year and 15-year fixed mortgage rates
- Weekly historical observations
- Observation date, publication date, source, product type, and methodology
  version
- Changes over 1, 4, 13, 26, and 52 weeks
- Rolling averages, recent range, volatility, and distance from recent highs
  and lows

Use Freddie Mac's Primary Mortgage Market Survey as the initial authoritative
mortgage-rate source. Its downloadable history extends back to 1971. Preserve
source attribution and account for documented methodology changes when
comparing long periods.

### Market drivers and sentiment

Track explanatory indicators separately from observed mortgage rates:

- Federal funds target range and recent FOMC decisions
- FOMC Summary of Economic Projections
- New York Fed Survey of Market Expectations
- Treasury yields, especially the 2-year and 10-year
- Inflation measures and recent inflation trend
- Employment and unemployment releases
- Mortgage-rate spread to the 10-year Treasury
- Mortgage-backed-securities indicators when a licensed, reliable source is
  available

Prefer structured surveys and market-implied measures over unsourced news
sentiment. If narrative news analysis is added later, retain source links,
publication timestamps, extracted claims, and model/version provenance.

### One-to-three-month directional outlook

Produce separate 1-month and 3-month outlooks with:

- Probabilities that mortgage rates will be `lower`, `roughly_stable`, or
  `higher`
- A defined stable band, initially within 0.25 percentage points
- Confidence level and data-freshness status
- Primary upward and downward drivers
- Forecast creation time, target date, input snapshot, and model version
- Subsequent actual outcome for backtesting

Begin with a transparent rules-based model using recent mortgage-rate momentum,
Treasury-yield momentum, inflation trend, and structured policy expectations.
Do not present the result as certainty or personalized financial advice.

Baseline implementation status:

- [x] Add versioned 1-month and 3-month `lower`, `roughly_stable`, and `higher`
  probabilities using PMMS momentum and recent volatility.
- [x] Persist the input snapshot, explanatory drivers, target date, confidence,
  and model version for reproducibility and later backtesting.
- [x] Cap confidence because the baseline does not yet include explanatory
  market signals.
- [x] Add 2-year and 10-year Treasury histories and use 10-year yield momentum
  in `multi_signal_v2`.
- [x] Add CPI history and the recent change in year-over-year inflation.
- [x] Add structured combined-panel federal-funds expectations from the New
  York Fed Survey of Market Expectations.
- [x] Bound FRED storage to rolling three-year daily-rate windows and a rolling
  ten-year monthly CPI window.
- [x] Add completed-outcome backtesting, chronological train/holdout splits,
  Brier scores, and persisted calibration artifacts.
- [x] Reject calibration automatically when it fails to improve on both the raw
  model and a stable-rate holdout baseline.
- [ ] Add point-in-time CPI and PMMS vintages to eliminate remaining revision
  bias in historical tests.
- [ ] Improve signal design until a later chronological holdout beats the
  stable-rate baseline; do not deploy the current rejected calibration scales.

### Affordability scenarios

For each listing, show estimated principal-and-interest payments at:

- The current benchmark mortgage rate
- Current rate minus 0.50 percentage points
- Current rate plus 0.50 percentage points

Make loan amount, down payment, term, taxes, insurance, HOA, and other cost
assumptions explicit. Keep these scenarios separate from lender quotes and
borrower-specific qualification.

### Acceptance criteria

- Rate ingestion is idempotent and retains immutable historical observations.
- Every displayed rate and outlook includes an as-of time and source.
- Trend calculations cannot mix incompatible products or methodologies without
  an explicit adjustment or warning.
- Outlook probabilities sum to 100 percent and are reproducible from a stored
  input snapshot.
- Forecast accuracy and calibration are measured by horizon and model version.
- Stale or missing upstream data lowers confidence and is visible to users.
- Affordability calculations have boundary tests and match independently
  verified amortization examples.
- No forecast language implies guaranteed rate direction.

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
- Freddie Mac Primary Mortgage Market Survey:
  https://www.freddiemac.com/pmms
- Freddie Mac PMMS historical archive:
  https://www.freddiemac.com/pmms/pmms_archives
- Federal Reserve Summary of Economic Projections:
  https://www.federalreserve.gov/faqs/summary-economic-projections-sep.htm
- New York Fed Survey of Market Expectations:
  https://www.newyorkfed.org/markets/market-intelligence/survey-of-market-expectations
