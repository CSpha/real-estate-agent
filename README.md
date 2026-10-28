# Real Estate Agent

Monitors residential listings, stores current and historical data in Postgres,
detects price drops, scores potential deals, and sends Slack alerts.

## Stack

- Python
- Postgres
- Docker
- FastAPI
- Slack webhooks

## Setup

Create a `.env` file locally. Do not commit it.

Supported database variable names:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=realestate
DB_USER=realestate
DB_PASSWORD=changeme
```

The app also accepts the Docker-style names `POSTGRES_HOST`, `POSTGRES_PORT`,
`POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD`.

`DB_*` takes precedence when both conventions are set. `DATABASE_URL` overrides
the central Python engine's connection URL. Docker Compose accepts either
variable convention for database credentials, database name, and published port;
containers connect to the internal `postgres:5432` address. Compose does not
derive its service configuration from `DATABASE_URL`.

For Slack alerts, add:

```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

For the optional RentCast listing provider, first activate a RentCast API plan
(the Developer plan includes a small free request allowance), then add:

```env
RENTCAST_API_KEY=...
```

For AI investment analyses, add an OpenAI API key. The model is configurable:

```env
OPENAI_API_KEY=...
OPENAI_MODEL=gpt-5.4-mini
```

The analyst endpoints are optional and disabled by default. To enable them, add
either of the following to your environment:

```env
ANALYST_FEATURES_ENABLED=true
# or
ENABLE_ANALYST_FEATURES=true
```

## Database

Start Postgres:

```powershell
docker compose up -d
```

Apply the authoritative Alembic schema, including comparable valuations:

```powershell
python -m alembic upgrade head
```

For an existing prototype database without `alembic_version`, follow the
[legacy migration rehearsal](migrations/LEGACY_MIGRATION.md) first. The initial
migration creates tables and must not be applied over the old schema directly.

## AI deal analyst

After loading listings, county sales, snapshots, and market scores, create an
analysis with `POST /listings/{source}/{source_listing_id}/analyses`. The saved
record includes the exact evidence snapshot, prompt version, model, structured
conclusions, citations, risks, and due-diligence questions.

Submit human feedback with `POST /analyses/{analysis_id}/reviews` using a
decision of `approved`, `rejected`, or `corrected`. Corrected decisions require
a `corrections` JSON object, making reviewed cases reusable as regression data.

## Workflows

Run a bounded, read-only RentCast coverage audit for the Wayne County pilot:

```powershell
python -m app.providers.audit_rentcast
```

The default audit makes three requests for Wooster, Orrville, and Rittman. It
prints only aggregate coverage and field-completeness information; it neither
prints addresses or agent contacts nor stores listing records.

After reviewing coverage, run the Wayne County shadow sync:

```powershell
python -m alembic upgrade head
python -m app.providers.sync_rentcast_shadow
```

The shadow sync reads all active listings for Wooster, Orrville, and Rittman,
keeps only Wayne County records, and is idempotent. Automated scoring is limited
to active single-family homes, condos, and townhouses with the required identity
and price fields. Manufactured and multifamily listings remain visible but need
type-specific review; land is excluded. Missing square footage is left as
unavailable rather than zero, so an otherwise eligible home can retain county
context while remaining unavailable for comparable valuation.

Shadow listings have `alert_eligible = false` in both current and historical
records. They can be snapshotted and scored, but neither price-drop nor
potential-deal alert queries can select them. The command also deliberately
skips saved-search evaluation and alert queueing.

Import the most recent 24 months of property-level Wayne County sales and run
the all-listing review:

```powershell
python -m app.ingest.load_wayne_county_comparable_sales --months 24
python -m app.transforms.backfill_comparable_valuations
python -m app.reports.shadow_scoring_review
```

The importer reads the Wayne County Auditor's public ArcGIS `Recent Sales`
layer, maps Ohio residential class codes, and retains sale amount, date,
address, ZIP, living area, acreage, and year built. The automated layer omits
the auditor site's `Valid` sale flag, so `arms_length` is explicitly a
conservative proxy based on residential class, living area, price, appraised
value, and bundled-parcel suppression. Keep these valuations in shadow review
until a machine-readable valid-sales export or licensed closed-sale feed can
confirm transactions.

Supported-class auditor records that cannot be normalized are stored once in
`ingest_errors`, keyed by their payload fingerprint and stable error code. The
raw JSON remains available for internal diagnosis while routine output exposes
only aggregate reasons. Review them with:

```powershell
python -m app.reports.ingest_errors `
  --source wayne_county_auditor_arcgis_provisional
```

The review writes `data/shadow_scoring_review.csv` (ignored by Git) with every
listing's market context, comparable result, review score, property type, price,
square footage, days on market, missing fields, and policy decision.

Run the dedicated pipeline once, or start its three-day Docker scheduler:

```powershell
python -m app.shadow.run_pipeline
docker compose --profile shadow up -d shadow-runner
python -m app.shadow.readiness
```

The shadow runner has no Slack webhook in its container environment and imports
no alert module. Scheduled runs do not call RentCast by default, so they reuse
the listings and rent estimates already stored in Postgres while refreshing
Redfin context, auditor sales, valuations, the review report, and
`shadow_pipeline_runs`. Scheduled RentCast listing refreshes require an
explicit `SHADOW_ENABLE_RENTCAST_API_CALLS=true` opt-in. Each refresh makes one
request per pilot city (three in total). A request budget then applies: RentCast is
skipped when listings were requested within `SHADOW_RENTCAST_MIN_HOURS`
(default 60), so failure retries do not re-fetch, or when the run would exceed
`SHADOW_RENTCAST_MONTHLY_REQUEST_LIMIT` (default 40) in the current UTC calendar
month. The budget counts only scheduled and manual shadow-pipeline runs; leave
headroom for audits and standalone syncs. A skipped run still refreshes the other
sources. Promotion remains
blocked until three successful run dates span at least 14 days, each of the
latest three run dates synced RentCast listings, no shadow listing has become
alert-eligible, and at least 70% of comparable-ready homes have supported
valuations. The default cadence is three days and can be changed
with `SHADOW_INTERVAL_HOURS`. The next deadline is recovered from persisted run history, including manual runs.
`SHADOW_INITIAL_DELAY_HOURS` applies only before any run history exists; its
initial deadline is persisted under the mounted data directory.

The shadow service persists its generated CSV under
`data/shadow-runner/shadow_scoring_review.csv` on the host. Manual review commands
continue to write `data/shadow_scoring_review.csv`. Both locations are ignored
by Git. View scheduler activity with
`docker compose --profile shadow logs --tail 50 shadow-runner`, and stop it with
`docker compose --profile shadow stop shadow-runner`.

The scheduler runs 72 hours after each successful completion and retries failures
after 6 hours by default. Restarts preserve the deadline; an overdue attempt
runs on startup. An interrupted run is retried six hours after its start.
The 14-day readiness window starts at the first successful run; valuation
coverage is checked across the latest three successful run dates. Docker Desktop and the host must stay
running for scheduled work to execute.

Load real monthly Wayne County market context from Redfin, then refresh the
county-level market scores:

```powershell
python -m app.ingest.load_redfin_county_sales
python -m app.transforms.score_listings_against_market
```

The importer streams Redfin's nationwide county CSV without saving it locally,
validates the published columns, retains only monthly `Wayne County, OH` rows,
and upserts revisions by county, state, and period. It maps homes sold, median
sale price, median days on market, new listings, and active listings into
`county_sales`. Re-running an unchanged file does not change `loaded_at`.

Data is provided by Redfin, a national real estate brokerage. Redfin publishes
the source through its [Data Center download hub](https://www.redfin.com/news/data-center/downloads/)
and documents metric definitions in its [methodology](https://www.redfin.com/news/data-center/methodology/).
The importer does not evaluate saved searches, queue alerts, or send Slack
messages.

RentCast is a commercial aggregation API, not a direct MLS feed. Confirm its
current usage and retention terms before production use. Its public API terms
currently permit internal research, analytics, derivative work, and storage in
internal systems, subject to their restrictions and any applicable third-party
data terms. Review the [RentCast API terms](https://www.rentcast.io/terms-api)
before enabling recurring ingestion.

Load sample listings:

```powershell
python -m app.ingest.load_sample_listings
```

Snapshot listing history:

```powershell
python -m app.transforms.snapshot_listings
```

Detect and send price-drop alerts:

```powershell
python -m app.alerts.send_price_drop_alerts
```

Run the full pipeline:

```powershell
python -m app.run_pipeline
```

Run the API:

```powershell
python -m app.api
```

Reviewed exact-match Auditor living-area evidence is stored separately before it
updates a missing current-listing field. This preserves the provider history and
records the parcel, matching method, review basis, and source payload fingerprint:

```powershell
python -m app.transforms.apply_auditor_sqft_candidates
```

With the API running, open `http://127.0.0.1:8000/dashboard` to use the local
Property Signal dashboard. It shows pipeline health, shadow-promotion readiness,
recent runs, and the current listing scores from Postgres. Listings can be
searched, filtered, sorted, and opened to inspect their valuation evidence and
score components. The dashboard is read-only and clearly marks the data as
provisional; it does not enable alerts or change listing records.

## Rental estimates and DSCR underwriting

Apply the rental-underwriting schema, then run a bounded RentCast long-term rent
estimate refresh:

```powershell
python -m alembic upgrade head
python -m app.transforms.sync_rent_estimates --max-requests 25
```

The command skips unchanged estimates that are less than 30 days old and caps
new successful RentCast requests at 25 per invocation. This cap matters because
RentCast counts each successful property AVM request against the account's
monthly allowance and may charge overages. It prints aggregate results without
printing addresses or API response bodies.

Each immutable rent estimate stores the request inputs, estimate range, rental
comparables, provider response, analysis date, and input fingerprint. Its linked
rental analysis stores a reproducible acquisition scenario with monthly cash
flow, gross yield, cap rate, and loan sizing.

The initial DSCR target is 1.25 with a 75% maximum LTV and 30-year amortization.
The assumed DSCR interest rate is the latest stored Freddie Mac 30-year PMMS
benchmark plus 1.50 percentage points, falling back to 8.00% when no benchmark
is available. This is a screening assumption rather than a lender quote.
Property tax is estimated at 1.50% of purchase price annually, insurance at
0.50%, vacancy at 5%, management at 8%, maintenance at 8%, capital reserves at
5%, and HOA at zero. The analysis sizes the target loan to the lower of the LTV
cap or the loan supported by gross monthly rent divided by monthly PITIA at the
target DSCR. The dashboard exposes the constraint, loan amount, down payment,
PITIA, minimum qualifying rent, and all assumptions for review.

## Comparable valuation and Deal Score v2

Valuation analysis dates are UTC calendar days, including the default date for
shadow runs and batch backfills. An explicit `--as-of-date` includes history
strictly before midnight UTC at the start of the next day. Snapshot recency
also uses UTC, independent of the machine or PostgreSQL session timezone.
Backfills choose one analysis date for the whole batch. Historical snapshots
and prior calculations remain immutable when later evidence is recalculated.

Load property-level comparable sales, then persist a listing valuation:

```powershell
python -m app.ingest.load_comparable_sales
python -m app.transforms.persist_comparable_valuation sample_feed 1001 `
  --as-of-date 2026-07-27
```

`listing_comparable_valuations` stores an immutable calculation snapshot keyed
by its input fingerprint. An unchanged rerun reuses the existing row and
timestamp; changed listing or comparable inputs create a new auditable
snapshot. `deal_score_v2_components` stores the versioned
component results linked to that valuation. `deal_score_v2_scores` stores the
corresponding immutable aggregate and the fingerprints of the exact component
rows used to calculate it.

The comparable-discount component contributes zero points at or above supported
value and scales to its full 40 points at a 30% discount. It remains
`unavailable`, rather than receiving zero, when no supported valuation exists
or confidence is below 40.

The `listing_opportunity` component contributes up to 15 points from listing
history available on or before the valuation date:

- Up to 9 points for cumulative reduction from the historical peak, with full
  credit at 15%
- Up to 4 points for distinct price reductions, with full credit at three
- 2 points when the latest reduction is within 30 days, 1 point within 90 days

The `days_on_market` component contributes up to 15 points by comparing the
listing with the latest county median available by the valuation date. It
scores zero at or below 75% of the county median and scales to full credit at
twice the median. Listing evidence older than 30 days or county context older
than 180 days makes the component unavailable instead of silently using stale
data.

The `market_momentum` component contributes up to 15 points from county median
sale-price changes. It blends a 3-month trend at 40% and a 12-month trend at
60%. Each horizon scores from zero at a 10% decline, through 7.5 points when
flat, to 15 points at a 10% increase. When one horizon is unavailable, its
weight receives neutral credit rather than treating missing data as positive
momentum. County history older than 180 days, or history with neither usable
horizon, makes the component unavailable.

The `liquidity_inventory` component contributes up to 10 points from the
latest county market period. Six points come from an inventory-months proxy
(`active listings / homes sold`), with full credit at two months or less and
zero at eight months or more. Four points come from the sold-to-new-listings
ratio, with zero credit at 0.5 and full credit at 1.0. If one submetric is
missing, that portion receives neutral credit; missing homes-sold data, both
inventory inputs missing, or context older than 180 days makes the component
unavailable.

The `data_confidence` component contributes the final 5 points: up to 3 points
from comparable-valuation confidence and up to 2 points from coverage of the
other 95 score points.

The aggregate publishes `total_points` only when all six components are
available. A missing non-core component produces a `partial` result with
available points, coverage, and a normalized available-evidence score kept
separate from the final total. Missing comparable-discount evidence makes the
aggregate `unavailable`.

The opportunity component is also `unavailable` when no valid price history
exists. Each score component has its own input fingerprint, so unchanged
evidence is reused while backfilled or changed history produces a new auditable
component. The Deal Score v2 calculation and persistence model is used by the
dedicated shadow pipeline and review report. It is still isolated from
saved-search and alert logic; promotion remains a separate, gated decision.

### Deal Score threshold policies

`deal_score_policies` holds versioned threshold sets. The initial active policy,
`deal-score-thresholds-v1`, preserves the thresholds documented above. Each
component records its policy version and thresholds in its immutable calculation
JSON, so a future policy produces new component and aggregate fingerprints while
leaving earlier results reproducible. There can be only one active policy.
Create and review a new policy row before switching it active; after a change,
run the valuation backfill to publish scores calculated under that policy.

Use `python -m app.transforms.manage_deal_score_policy --show-active` to inspect
the live policy. To make a reviewed JSON threshold file available, run
`--create policy-name --thresholds-file policy.json`; activate it separately with
`--activate policy-name`. Creation validates the complete threshold schema and
activation is transactional, so there is always one active policy.

## Database availability

Postgres uses `restart: unless-stopped`, also applied to the existing container
without interrupting it. Docker restarts it after a crash or daemon restart
unless it was explicitly stopped. Docker Desktop must still be running; an
intentional `docker stop` requires starting the container again. This is not a
host startup or power-management setting.

## Operational health and backups

Before exposing the API outside the local machine, set `API_AUTH_REQUIRED=true`
and a long random `API_AUTH_TOKEN`. Requests then require
`Authorization: Bearer <token>`; `/health` remains available to a monitoring
system. `API_RATE_LIMIT_PER_MINUTE` caps requests per process and client IP,
with a default of 120. API request logs are structured JSON and omit tokens and
query strings.
Docker also monitors the API with a local `/health` check and marks the
container unhealthy after repeated failures. Compose restart policies act only
when a process exits, so an unhealthy API is reported but not restarted.
The shadow-runner has its own deadline check: if its latest run passes the
scheduled interval (or failure-retry interval) by 15 minutes, Docker marks it
unhealthy. An in-process watchdog runs the same check every five minutes and,
after two consecutive misses, exits the scheduler so `restart: unless-stopped`
starts a fresh one that recovers its deadline from run history. A run that
lasts longer than the retry interval plus grace is treated as hung.

`GET /health` reports database connectivity and shadow-pipeline history: last
successful completion, its age in seconds, expected next attempt, latest run
status, and up to five failure timestamps from the last seven days. It reports
`degraded` for missing or stale history; freshness allows the configured
72-hour interval plus a 6-hour retry grace period. Database failures return 503.
A successful run timestamp is a freshness proxy, not a scheduler heartbeat or
proof that every provider record is current. The next run is unknown until
history exists. Local Docker API access is at `http://127.0.0.1:8000/health`.
The endpoint also reports pending/retryable alert backlog and permanent alert
failures; permanent failures make health `degraded`.

The `backup` Compose profile now includes `backup-verify`. It restores the newest
completed dump into a disposable `*_test` database, compares schema metadata and
row counts with the source, then removes that database. It runs weekly by default
and retries failures hourly. Start the profile with `docker compose --profile backup up -d`.

Daily backups and isolated restore verification are described in
[Postgres backups](docs/postgres-backups.md). The first backup is immediate;
subsequent backups preserve their 24-hour deadline across restarts.
