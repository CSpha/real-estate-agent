# Real Estate Agent Development Roadmap

Last reviewed: October 28, 2026 (code through commit `23f0f10`, September 18)

## Goal

Build a reliable application that retrieves authorized real-estate listings, stores current and historical listing data, evaluates configurable search criteria, scores opportunities with transparent market context, and sends deduplicated Slack notifications when meaningful events occur.

## Status at a glance

| Area | Status |
| --- | --- |
| Alert pipeline (`app.run_pipeline`) | Working end to end, but still loads only `data/sample_listings.csv` |
| Shadow pipeline (`app.shadow.run_pipeline`) | Runs real Wayne County data; alert-isolated; three-day Docker scheduler |
| Deal Score v2 | All six components and the 100-point aggregate implemented; thresholds versioned in `deal_score_policies` |
| Rental underwriting | RentCast rent AVM, cash flow, cap rate, DSCR sizing, fixed-rate growth scenarios |
| Mortgage-rate outlook | Multi-signal model and backtesting implemented; calibration rejected, not deployed |
| Operations | Health with freshness and backlog, opt-in API auth and rate limiting, daily backups with restore verification, CI |
| Alert promotion | **Blocked**: readiness passes, but listing freshness, transaction validity, and flagged-listing evidence are outstanding |

Latest schema revision: `0019_deal_score_policies`.

## Current capabilities

Data and ingestion

- One authoritative Alembic-managed PostgreSQL schema (19 revisions)
- Immutable raw JSON payloads with content hashes and normalized current listings
- Idempotent sample ingestion and change-aware listing history
- RentCast active-sale-listing adapter with bounded pagination, secret-header
  authentication, and a read-only coverage audit
- Wayne County RentCast shadow sync with persistent alert isolation, explicit
  single-family/condo/townhouse automation rules, review-only manufactured and
  multifamily records, land exclusion, and neutral handling of missing square footage
- Streaming, revision-safe Redfin county-market importer (price, sales,
  inventory, new listings, market velocity)
- 24-month Wayne County Auditor ArcGIS comparable-sales importer with a
  conservative arm's-length proxy, parcel-bundle suppression, and provisional-source labeling
- `ingest_errors` quarantine for supported-class auditor records that cannot be
  normalized, keyed by payload fingerprint and error code
- Source-tracked property-attribute evidence (`listing_property_attribute_evidence`);
  reviewed exact-match auditor living area can fill missing listing square footage
  without overwriting provider history

Scoring and analysis

- One authoritative county-level market score (coarse context only)
- Versioned deterministic comparable selection with visible fallback tiers
- Explainable comparable valuation: weighted-median price per square foot,
  outlier safeguards, value ranges, and confidence components
- Deal Score v2 with immutable valuation snapshots, six versioned components,
  and an aggregate that distinguishes complete, partial, and unavailable results
- Versioned threshold policies (`deal-score-thresholds-v1` active) with a
  transactional admin command; policy changes produce new fingerprints and leave
  earlier results reproducible
- RentCast long-term rent AVM with request fingerprints and 30-day reuse;
  cash flow, gross yield, cap rate, and DSCR loan sizing
- Five-year rental scenarios at fixed assumed growth rates (-1%, 2%, 4%)
- Mortgage-rate outlooks (PMMS, Treasury, CPI, NY Fed SME) with backtesting
  and automatic calibration rejection
- Optional AI analyst endpoints (OpenAI, disabled by default) with evidence
  snapshots and human review records

Searches, alerts, and API

- Versioned saved searches with validated, explainable criteria evaluation
- Stable listing/search event fingerprints and persisted match transitions
- PostgreSQL alert outbox with atomic claims, retries, and visible permanent failures
- Slack alerts for price drops and potential deals (sample pipeline only)
- FastAPI API and a local shadow-review dashboard (`/dashboard`)
- Opt-in bearer-token auth (`API_AUTH_REQUIRED`), per-client rate limiting,
  and structured JSON request logs that omit tokens and query strings

Operations

- Docker Compose services: Postgres, API, `shadow` profile (scheduler), and
  `backup` profile (daily dumps plus weekly isolated restore verification)
- `/health` reports database connectivity, shadow-run freshness, alert backlog,
  and permanent alert failures (`degraded` when stale or failing)
- Shadow scheduler recovers deadlines from run history across restarts and host
  sleep; scheduled RentCast calls are off unless
  `SHADOW_ENABLE_RENTCAST_API_CALLS=true`
- CI on every push: Ruff, unit tests, and Compose config validation
- Unit tests plus opt-in isolated PostgreSQL integration tests

## Known issues found in this review

1. **Resolved October 28: shadow deadline health check was on the wrong
   service.** Commit `23f0f10` attached it to `backup`; it now runs on
   `shadow-runner`.
2. **Resolved October 28: unhealthy containers were not restarted.** Compose
   restart policies act on process exit, not health status. The scheduler now
   runs an in-process watchdog that exits after two consecutive missed-deadline
   checks so `restart: unless-stopped` recovers it. The six-hour failure backoff
   also uses the suspend-safe `sleep_until`. The API healthcheck still only
   reports unhealthy status.
3. **Readiness passes on stale listing data.** Docker Desktop was not running
   from about September 18 to October 28, so no runs occurred in that gap. Run 4
   (October 28) made readiness pass, but runs 3 and 4 skipped RentCast (scheduled
   calls are off by default), so the newest listing data is from September 16.
   The observations re-score the same listings rather than observe new market
   activity. **Addressed October 28:** scheduled RentCast refreshes are enabled
   locally with a request budget (60-hour minimum spacing, 40 requests per UTC
   month against the plan's 50), and readiness now requires each of the latest
   three run dates to include a successful RentCast sync. Readiness is
   correctly not ready again until three fresh observations accumulate.
4. **`provider_sync_state` is created at runtime.** `app/providers/sync.py`
   issues its own `CREATE TABLE` instead of using an Alembic revision, and
   `sync_provider` processes one page per call. Both must be resolved before the
   generic sync path is used for live ingestion.
5. **Rental scenario assumptions are hard-coded.** Growth rates and the 0.74
   rent-to-cash-flow pass-through in `app/market/rental_forecast.py` are not
   derived from market rent history and are not stored in a policy table.

## Alert promotion gates

Promotion of RentCast listings to alert eligibility requires all of the following.
Passing `python -m app.shadow.readiness` alone does not satisfy them.

- [x] Three successful shadow run dates spanning at least 14 days
      (four runs over 49 days as of October 28; see known issue 3)
- [x] At least 70% supported comparable valuation coverage
      (97.78% minimum across recent runs)
- [x] Zero alert-eligible shadow listings
- [ ] Latest three run dates each synced RentCast listings (readiness check
      `recent_run_dates_synced_listings`; earliest pass about November 6 at
      the three-day cadence)
- [ ] Comparable transaction validity confirmed by an authoritative export or
      licensed feed (see [Wayne County valid sales](docs/wayne-county-valid-sales.md);
      the request to the auditor has been drafted but not sent)
- [ ] Evidence obtained for the 21 flagged listings; none has been cleared
- [ ] Slack notification hardening and test-channel validation (Phase 4)
- [ ] Separate, explicit approval for production alert activation

## Near-term priorities

1. Confirm the October 31 run (18:18 UTC) fetches RentCast listings and
   records `rentcast_requested`; watch monthly usage in the scheduler logs.
2. Keep Docker Desktop running (start at sign-in) and review each new report in
   `data/shadow-runner/`.
3. Obtain a transaction-level valid-sales export: download a sample from the
   auditor's Sales Report page or send the drafted request. Do not build a parser
   around an assumed format.
4. Obtain evidence for flagged listings: the 18 low-confidence valuations
   ([review](docs/low-confidence-review.md)) and the no-comparables home at
   254 W Wayne Ave ([scoring gaps](docs/scoring-gap-review.md)).
5. Decide whether to add a Fredericksburg abbreviation mapping to the auditor
   parser. Keep multi-value property records quarantined until a deterministic
   parcel/dwelling rule is documented.
6. Finish Phase 4 Slack hardening before any alert activation.
7. Extend CI with an Alembic upgrade/downgrade check against Postgres, the
   integration suite, and secret scanning.
8. Move the generic provider sync to Alembic-managed state with multi-page
   resumable cursors (known issue 4).
9. Add geographic bounding-box criteria once provider coordinates are available.

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

## Phase 1: Stabilize the prototype — Complete

Implementation commit: `8b892c7`. Alembic replaced numbered SQL schema files;
raw payloads became immutable JSONB with content hashes; configuration was
consolidated into one settings module and engine (`DB_*` with `POSTGRES_*`
aliases and optional `DATABASE_URL`); normalization, Docker readiness checks,
`--skip-alerts`, and idempotent upserts, snapshots, scoring, and alert-ledger
recording were added. Database-level idempotency was confirmed against a fresh
PostgreSQL test database on September 8, 2026.

## Phase 2: Configurable saved searches — Complete

Implementation commit: `17cee39`. Adds `saved_searches` and
`listing_search_evaluations` (revision `0002`), strict versioned JSONB criteria,
one shared pure evaluator, structured match/failure reasons, immediate/digest
mode with cooldown, stable event fingerprints, CRUD and evaluation endpoints,
and a pipeline step. Integration validation passed on September 8, 2026.

- [ ] Add optional geographic bounding-box criteria after provider coordinates
      are available.

Example criteria:

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

## Phase 3: Connect an authorized live listing provider — In progress

### Provider decision

Wayne County, Ohio is the pilot geography. RentCast is the initial non-MLS
route; the coverage audit found 98–105 unique Wayne County listings across
Wooster, Orrville, and Rittman with full price, days-on-market, and history
coverage. A direct MLS Now/Trestle feed remains the preferred long-term option
if a broker relationship becomes available. Do not scrape consumer listing sites
unless their terms and an explicit agreement authorize it.

### Work

- [x] Provider interface (`fetch_updated_since`, `normalize`) with RentCast and fixture adapters
- [x] Bounded pagination with request timeouts
- [x] Store raw payloads before normalization
- [x] Track provider listing ID and source
- [x] Quarantine bad individual records (`ingest_errors`, auditor importer)
- [x] Record shadow sync runs with start/end time, counts, and failure details (`shadow_pipeline_runs`)
- [ ] Manage `provider_sync_state` in Alembic and persist cursors only after each page commits
- [ ] Rate limiting, bounded retries, and exponential backoff for provider requests
- [ ] Incremental retrieval by provider modification timestamp where supported
- [ ] Track provider modification timestamp, listing URL, photos, and attribution fields
- [ ] Handle active, coming soon, pending, sold, expired, canceled, withdrawn, and deleted/missing statuses
- [ ] Detect listings that disappear from the feed without deleting history
- [ ] Extend quarantine to RentCast listing normalization failures
- [ ] Document licensing, attribution, display, photo, and retention requirements

### Acceptance criteria

- An interrupted sync resumes without skipping or duplicating listings.
- Pagination is verified with automated tests or provider fixtures.
- Provider throttling and transient errors are retried safely.
- Bad individual records are quarantined without losing the whole batch.
- Current listing state and meaningful history are correct after repeated syncs.
- Feed attribution and retention rules are documented and enforced.

## Phase 4: Make notifications reliable — In progress

### Work

- [x] Alert outbox for price drops, separate from detection logic
- [x] `pending`, `processing`, `sent`, `retryable_failed`, and `permanent_failed` states
- [x] Attempt counts, next-attempt timestamps, and error summaries
- [x] Safe claims so multiple workers cannot send the same event
- [x] Unique price-drop event identity in PostgreSQL
- [x] Exponential backoff for temporary failures (429 and 5xx are retryable)
- [x] Separate detection/queueing from the delivery worker command
- [x] Permanent failures visible in `/health`
- [ ] Add jitter to retry delays
- [ ] Honor Slack HTTP 429 `Retry-After` responses
- [ ] Pace incoming-webhook messages at no more than about one per second
- [ ] Route malformed payloads and invalid/revoked webhooks to permanent failure
- [ ] Route potential-deal alerts through the outbox
- [ ] Slack Block Kit formatting: address and listing URL, permitted primary
      photo, current and previous price, match reasons, key attributes, and
      data source/attribution
- [ ] Digest delivery to avoid flooding a channel
- [ ] Dry-run/test-channel mode

### Acceptance criteria

- Retrying a failed job never creates a duplicate Slack alert.
- Slack rate limiting is handled automatically.
- Permanent failures are visible and do not block later alerts.
- Alerts link to the listing and explain why it matched.
- A test mode can validate messages without notifying the production channel.

## Phase 5: Operate it as an application — In progress

### Work

- [x] Containerize the API and alert-isolated shadow runner
- [x] Three-day shadow scheduler with persisted deadlines and host-sleep recovery
- [x] Shadow pipeline-run table and promotion-readiness calculation
- [x] `/health` reports database connectivity, last successful sync freshness,
      alert backlog, and permanent failures
- [x] Opt-in bearer-token authentication and per-client rate limiting
- [x] Structured JSON API request logs
- [x] Daily PostgreSQL backups and weekly isolated restore verification
      (first restore verified September 11: 34 tables, 6,123 rows)
- [x] CI for linting, unit tests, and Compose configuration
- [x] Shadow deadline healthcheck on `shadow-runner` with a self-exiting
      watchdog so the restart policy recovers a missed deadline
- [ ] Separately deployed production alert worker and alert-processing schedule
      (after promotion)
- [ ] Structured logging with run, provider, and event IDs in pipelines and workers
- [ ] Operational alert for stale or failed syncs (health is pull-only today)
- [ ] Monitoring for repeated provider failures and database capacity
- [ ] Off-machine backup copies and a retention policy
- [ ] Readiness and liveness endpoints if deployed behind an orchestrator
- [ ] Validate and limit API pagination and expensive queries
- [ ] CI for migrations, integration tests, and secret scanning
- [ ] Document deployment, credential rotation, and incident response

### Acceptance criteria

- The application starts from documented deployment commands.
- Database readiness is checked before dependent services start.
- A failed or stale listing sync generates an operational alert.
- Database backup restoration has been tested.
- CI blocks broken migrations and failing tests.
- Secrets can be rotated without code changes.

## Data model

Implemented: `saved_searches`, `listing_search_evaluations`, `alert_outbox`,
`shadow_pipeline_runs`, `ingest_errors`, `listing_property_attribute_evidence`,
`listing_rent_estimates`, `rental_underwriting_analyses`,
`rental_forecast_scenarios`, and `deal_score_policies`.

Still needed:

- `provider_sync_state` under Alembic (currently created at runtime)
- `listing_events` for disappearance and status transitions
- General `pipeline_runs` covering the sample pipeline and alert worker

Stable identifiers:

- Listing: `(provider/source, provider listing ID)`
- Search version: `(saved search ID, criteria version)`
- Material state: normalized fingerprint of tracked listing fields
- Alert event: `(listing, search version, event type, material state)`

## Scoring

The county-median score remains as coarse market context. Deal Score v2 is fully
implemented but stays in shadow review until the promotion gates are met.

### Deal Score v2

A transparent 100-point score, with thresholds held in the active
`deal_score_policies` row:

- 40 points: discount to estimated comparable value
- 15 points: listing opportunity (cumulative price reductions, frequency, recency)
- 15 points: days on market relative to the county median
- 15 points: market momentum (3-month change at 40%, 12-month at 60%; zero at
  -10%, neutral at 0%, full at +10%)
- 10 points: liquidity and inventory (6 points for inventory months, full at ≤2
  and zero at ≥8; 4 points for sold-to-new ratio, full at 1.0 and zero at 0.5)
- 5 points: data confidence (3 for comparable quality, 2 for coverage of the other 95)

Missing submetrics receive neutral weighted credit; stale or wholly
insufficient context makes a component unavailable. A final score is published
only when all six components are available. Partial results keep available
points, coverage, and a normalized available-evidence score, which is not a final
total. County median sale price provides context but never defines a deal on its
own, and price per square foot feeds valuation only, to avoid double-counting.

Latest reviewed results (September 15 shadow run): 18 valued listings at `low`
confidence. Earlier September 9 review: 73 of 74 comparable-ready listings
complete, one unavailable with no suitable comparables. Manufactured and
multifamily homes stay review-only pending type-specific valuation.

### Comparable selection

Prefer recent sold properties with the same property type, the same ZIP or
Census tract when sample size permits, similar beds, baths, square footage, lot
size, and age, and a documented maximum sale age. Widen geography or similarity
bands only when the initial set is too small, and record the fallback level.

### Separate subscores

Store and expose rather than hide in one number:

- `deal_score`: purchase price relative to supported market value — implemented
- `rental_score`: rent yield, operating costs, and cash flow — inputs implemented
  (`rental_underwriting_analyses`); no normalized score yet
- `market_momentum_score`: price trend, inventory, and velocity — implemented as
  Deal Score v2 components
- `risk_score`: hazard, insurance, tax, condition, and data risks — not started
- `confidence_score`: comparable count, recency, similarity, and missing data —
  implemented as the data-confidence component

Rental follow-up:

- [x] Bounded RentCast rent AVM with request fingerprints, ranges, comparables,
      immutable snapshots, and 30-day reuse
- [x] Explicit cash flow, gross yield, cap rate, and expense assumptions
- [x] DSCR loan sized at the lower of 1.25 DSCR support and 75% LTV, using a
      stored PMMS-plus-spread rate and 30-year amortization
- [x] Rent and DSCR results in the shadow-review dashboard
- [x] Five-year conservative/base/upside scenarios (`rental-growth-scenarios-v1`)
- [ ] Derive growth rates from market rent history and version the assumptions
      before presenting scenarios as a forecast

### Explainability and safeguards

Persist each input, normalized component score, weight, contribution, final
score, scoring version, policy version, and human-readable reason. Missing data
must reduce confidence rather than silently receive a favorable score.

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
- Tests cover boundary values, missing data, outliers, and small comparable samples.

## Mortgage-rate intelligence

Mortgage-rate information stays a separate, timestamped analysis rather than
part of the property deal score, because a borrower's actual rate depends on
credit, loan structure, down payment, points, lender, and location. Freddie Mac's
Primary Mortgage Market Survey is the authoritative rate source.

### Status

- [x] Versioned 1-month and 3-month `lower`, `roughly_stable`, and `higher`
      probabilities with a 0.25-point stable band
- [x] Persisted input snapshots, drivers, target dates, confidence, and model version
- [x] 2-year and 10-year Treasury histories; 10-year momentum in `multi_signal_v2`
- [x] CPI history and year-over-year inflation change
- [x] NY Fed Survey of Market Expectations federal-funds expectations
- [x] Bounded FRED storage (three-year daily, ten-year monthly CPI windows)
- [x] Completed-outcome backtesting, chronological splits, Brier scores, and
      persisted calibration artifacts
- [x] Automatic calibration rejection when it fails to beat both the raw model
      and a stable-rate baseline
- [ ] Point-in-time CPI and PMMS vintages to remove revision bias
- [ ] Improve signals until a chronological holdout beats the stable-rate
      baseline; do not deploy the current rejected calibration scales
- [ ] Affordability scenarios per listing at the benchmark rate and ±0.50
      points, with explicit loan, tax, insurance, and HOA assumptions

### Acceptance criteria

- Rate ingestion is idempotent and retains immutable historical observations.
- Every displayed rate and outlook includes an as-of time and source.
- Trend calculations cannot mix incompatible products or methodologies without
  an explicit adjustment or warning.
- Outlook probabilities sum to 100 percent and are reproducible from a stored snapshot.
- Forecast accuracy and calibration are measured by horizon and model version.
- Stale or missing upstream data lowers confidence and is visible to users.
- Affordability calculations have boundary tests and match verified amortization examples.
- No forecast language implies guaranteed rate direction.

## Validation strategy

Covered today: normalization with provider fixtures, criteria boundaries,
price-change and fingerprint logic, alert idempotency, repeated-ingestion
integration, bounded single-page provider fetches, scheduler deadlines, health,
backups, and dry-run alert suppression.

Still needed:

- Multi-page provider pagination and interrupted-sync resume tests
- Status-enumeration tests for the full listing lifecycle
- Migration tests against fresh PostgreSQL in CI
- Provider throttling and retry tests
- Slack 200, 429 with `Retry-After`, temporary 5xx, and permanent 4xx tests
- End-to-end dry-run test of the shadow pipeline

## History

- **September 8, 2026:** Integration suite passed (18) on an isolated test
  database. Legacy database backed up, rehearsed, and cut over to
  `realestate_dev_20260908`, with originals retained in `legacy_archive` and the
  untouched `realestate` database. See [legacy migration](migrations/LEGACY_MIGRATION.md).
  Full suite: 166 passed.
- **September 9:** First real shadow run (run ID 1): 98 listings, 175 months of
  Redfin history, 2,038 auditor sales. A local-vs-UTC date bug excluded history
  evidence; after the UTC fix and recalculation, 73 of 74 aggregates were complete.
  21 flagged listings reviewed and retained in shadow. Auditor ingest errors
  persisted (12 records: 11 ambiguous multi-dwelling values, 1 abbreviated city).
- **September 11:** Daily backups and first verified restore. Scoring-gap and
  valid-sales source investigations recorded in `docs/`.
- **September 15:** Second observation date; 97.78% minimum valuation coverage.
  Low-confidence and missing-square-footage audits; local review dashboard;
  rent and DSCR underwriting.
- **September 16–18:** Scheduled RentCast calls disabled by default; verified
  auditor square footage applied through the attribute-evidence model; rental
  scenarios; versioned deal-score thresholds; API auth, rate limiting, and JSON
  request logs; backup verification worker; CI; host-sleep recovery; scheduler
  deadline healthcheck (attached to the wrong service, see known issues).
- **October 28:** Roadmap review. Docker Desktop had not been running since
  about September 18. Moved the deadline healthcheck to `shadow-runner` and
  added the self-exiting watchdog; 186 unit tests passed. On restart, run 4
  succeeded (119 listings, 99 comparable-ready, 97 valued, 0 alert-eligible)
  using September 16 listing data. Enabled budgeted scheduled RentCast
  refreshes and added the listing-sync readiness check; 191 unit tests passed.

## External references

- RESO specifications: https://www.reso.org/specs/
- RESO Web API transition guide and provider examples: https://www.reso.org/api-guide/
- Slack incoming webhooks: https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks
- Slack rate limits: https://docs.slack.dev/apis/web-api/rate-limits/
- Docker Compose startup/readiness guidance: https://docs.docker.com/compose/how-tos/startup-order/
- Freddie Mac Primary Mortgage Market Survey: https://www.freddiemac.com/pmms
- Freddie Mac PMMS historical archive: https://www.freddiemac.com/pmms/pmms_archives
- Federal Reserve Summary of Economic Projections:
  https://www.federalreserve.gov/faqs/summary-economic-projections-sep.htm
- New York Fed Survey of Market Expectations:
  https://www.newyorkfed.org/markets/market-intelligence/survey-of-market-expectations
