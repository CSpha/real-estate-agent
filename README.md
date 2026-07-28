# Real Estate Agent

A Python/PostgreSQL prototype for ingesting residential listings, retaining
meaningful listing history, scoring listings against market context, and
sending deduplicated Slack alerts.

The application currently uses sample CSV listings. It is not yet connected to
an MLS or another live listing provider. See [ROADMAP.md](ROADMAP.md) for the
planned live-provider and saved-search phases.

## What Phase 1 provides

- One Alembic-managed PostgreSQL schema
- Immutable raw JSON payload storage
- Idempotent current-listing upserts
- Auditable, idempotent property-level comparable-sale ingestion
- History snapshots only when tracked listing data changes
- Normalized listing statuses, property types, state values, ZIP codes, and dates
- One authoritative county-market scoring implementation
- Versioned, configurable saved searches with explainable matching
- Stable, idempotent listing/search evaluation fingerprints
- Alert-ledger uniqueness and safe repeated alert recording
- A FastAPI API with database-aware health checking
- Docker Compose readiness checks
- Unit tests and optional isolated PostgreSQL integration tests

## Requirements

Choose either:

- Docker Desktop with Docker Compose, or
- Python 3.13 and PostgreSQL 16

## Quick start with Docker

1. Create the local environment file:

   ```powershell
   Copy-Item .env.example .env
   ```

2. Edit `.env` and set a local `DB_PASSWORD`.

3. Build and start PostgreSQL and the API:

   ```powershell
   docker compose up --build
   ```

The API starts after PostgreSQL passes its readiness check. Alembic migrations
run before Uvicorn starts.

Useful URLs:

- API root: http://localhost:8000/
- Database-aware health check: http://localhost:8000/health
- OpenAPI UI: http://localhost:8000/docs

Stop the services without deleting database data:

```powershell
docker compose down
```

## Existing prototype databases

The new Alembic revision is a clean schema baseline. It intentionally does not
guess how to transform an existing prototype database because the old SQL files
defined incompatible versions of `listings_raw` and
`listing_market_scores`.

Before pointing this version at an existing database:

1. Export any data that must be retained.
2. Inspect the actual table definitions.
3. Either write a one-time reconciliation migration or create a new database.

For disposable local sample data only, removing the Compose volume and starting
fresh is the simplest route:

```powershell
docker compose down --volumes
docker compose up --build
```

The first command permanently deletes the local Compose database volume. Do not
run it against data you need.

## Local Python setup

Create and activate a virtual environment:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
Copy-Item .env.example .env
```

Edit `.env`, then apply the schema:

```powershell
python -m alembic upgrade head
python -m alembic current
```

Run the API:

```powershell
python -m uvicorn app.api:app --reload
```

## Configuration

The application uses one database convention:

| Variable | Required | Default |
| --- | --- | --- |
| `DB_HOST` | No | `localhost` |
| `DB_PORT` | No | `5432` |
| `DB_NAME` | No | `realestate` |
| `DB_USER` | No | `realestate` |
| `DB_PASSWORD` | Required by a password-protected database | None |
| `DATABASE_URL` | No | Built from the `DB_*` values |
| `SLACK_WEBHOOK_URL` | Only for sending alerts | None |

`DATABASE_URL`, when set, takes precedence over individual `DB_*` values.

Never commit `.env` or a Slack webhook URL.

## Database migrations

Alembic under `migrations/` is the only authoritative schema path.

```powershell
python -m alembic upgrade head
python -m alembic current
python -m alembic history
```

The `sql/` directory is reserved for standalone analysis/reporting SQL, not
schema definitions.

## Sample pipeline

Safe validation without Slack:

```powershell
python -m app.run_pipeline --skip-alerts
```

That command:

1. Loads `data/sample_listings.csv`
2. Stores previously unseen raw payloads
3. Upserts changed current listings
4. Adds history only for listings whose tracked state changed
5. Refreshes market scores from the latest available county sales data
6. Evaluates enabled saved searches using those refreshed scores
7. Does not queue or send Slack messages

To load, snapshot, evaluate searches, and queue price-drop events:

```powershell
python -m app.run_pipeline
```

The pipeline does not contact Slack and does not require `SLACK_WEBHOOK_URL`.
Deliver queued alerts separately:

```powershell
python -m app.alerts.process_alert_outbox
```

The worker requires `SLACK_WEBHOOK_URL`. Use a Slack test channel first. Limit
the amount of work in one invocation with `--limit`, for example:

```powershell
python -m app.alerts.process_alert_outbox --limit 25
```

The full pipeline does not yet run potential-deal alerts automatically.

Price-drop delivery uses an `alert_outbox` table:

1. Detection inserts each event once using its listing and event timestamp.
2. A worker atomically claims one due event with `FOR UPDATE SKIP LOCKED`.
3. A successful Slack request marks the outbox row `sent` and writes the
   `alerts_sent` receipt in one database transaction.
4. Temporary failures are retried with exponential backoff; permanent failures
   remain in the outbox for inspection.

This prevents two workers from normally sending the same pending event. Slack
webhooks do not support idempotency keys, so a process crash after Slack accepts
a message but before PostgreSQL records success can still produce a duplicate
on retry. Delivery is therefore at-least-once, not mathematically exactly-once.

To validate only ingestion and history without evaluating saved searches:

```powershell
python -m app.run_pipeline --skip-alerts --skip-search-evaluation
```

Individual workflows:

```powershell
python -m app.ingest.load_sample_listings
python -m app.transforms.snapshot_listings
python -m app.transforms.detect_price_changes
python -m app.ingest.load_county_sales
python -m app.ingest.load_comparable_sales
python -m app.transforms.score_listings_against_market
python -m app.alerts.queue_price_drop_alerts
python -m app.alerts.process_alert_outbox
python -m app.alerts.send_potential_deal_alerts
```

`load_county_sales` expects `data/county_sales.csv`. A `state` column is
recommended. For backward compatibility, files without it default to Ohio.

## Comparable sales

Apply migrations and load the included sample:

```powershell
python -m app.ingest.load_comparable_sales
```

Pass another CSV path to load an actual authorized export:

```powershell
python -m app.ingest.load_comparable_sales data\my_comparable_sales.csv
```

Required columns are `source`, `source_sale_id`, `sale_date`, `sale_price`,
`address`, `city`, `state`, `zip`, `property_type`, and `arms_length`. Optional
columns are `parcel_number`, `county_name`, `beds`, `baths`, `sqft`,
`lot_size_acres`, `year_built`, `latitude`, and `longitude`.

Each distinct source payload is retained in `comparable_sales_raw` for audit
history. `comparable_sales` contains the latest normalized version of each
`(source, source_sale_id)` and only changes when normalized values change.
The included rows are synthetic workflow fixtures, not appraisal evidence.

Select comparables for a current listing with a fixed analysis date:

```powershell
python -m app.market.comparable_selection sample_feed 1001 `
  --as-of-date 2026-07-27
```

Selection uses versioned, deterministic tiers:

1. `strict_zip`: same property type and ZIP, sold within 12 months, within 20%
   of square footage, one bed, one bath, and 50% of lot size when available.
2. `broad_zip`: expands to 18 months, 30% square footage, 1.5 baths, and 100%
   lot-size tolerance.
3. `city_fallback`: expands to the same city and state, 24 months, 40% square
   footage, two beds, and two baths.

Only arm's-length sales are eligible. Exact subject-address matches are
excluded. The selector stops at the first tier containing the requested
minimum number of comps, then ranks by physical similarity, recency, and stable
source identifiers. Its output includes every attempted tier and criteria that
could not be applied because subject data was unavailable.

## Market scoring

There is one Phase 1 scoring definition:

- 90: at least 30% below the latest county median
- 80: at least 20% below
- 70: at least 10% below
- 60: below the median
- 50: no more than 10% above
- 30: more than 10% above

The `potential_deals` view selects scores of 80 or higher.

This score is only coarse market context. It is not an appraisal or a robust
comparable-sale analysis. Saved-search matching is implemented separately from
deal ranking.

## Mortgage rates

Apply migrations, then load the official Freddie Mac Primary Mortgage Market
Survey history:

```powershell
python -m app.market.mortgage_rates
```

For reproducible or offline loading, download the official consolidated CSV and
provide it explicitly:

```powershell
python -m app.market.mortgage_rates --file data\PMMS_history.csv
```

The importer currently stores the 30-year and 15-year fixed-rate series. Loads
are idempotent by source, product, and observation date. Each observation keeps
its source URL and is tagged as either the legacy lender-survey methodology or
the application-based methodology introduced on November 17, 2022.

Trend calculations report the latest rate, 4/13/52-week changes, four-week
average, observation count, and a deterministic recent direction. This trend is
historical context.

Generate and persist versioned 1- and 3-month directional outlooks:

```powershell
python -m app.market.mortgage_rate_outlook
python -m app.market.mortgage_rate_outlook --product 15_year_fixed
```

The `pmms_momentum_v1` model reports probabilities for rates being lower,
roughly stable, or higher, where stable means within 0.25 percentage points.
It stores its input snapshot and explanatory drivers so forecasts can be
reproduced and backtested.

Version 1 uses only recent PMMS momentum and volatility, so confidence is capped
at medium and may be low with sparse or volatile data. It is a transparent
baseline, not a validated forecast, lender quote, or personalized financial
recommendation. Treasury, inflation, and policy-expectation signals remain
available through the version 2 workflow.

Load the current multi-signal inputs:

```powershell
python -m app.market.macro_rate_signals
```

This imports the FRED `DGS2`, `DGS10`, `DFF`, and `CPIAUCSL` series plus
combined-panel federal-funds expectations from the New York Fed Survey of
Market Expectations. When a newer survey workbook is released, supply it with:

```powershell
python -m app.market.macro_rate_signals --sme-url <official-xlsx-url>
```

FRED retention is intentionally bounded: Treasury yields and the effective
federal funds rate keep a rolling three years of daily observations, while CPI
keeps ten years of monthly observations. The loader requests only those
windows and deletes older rows for each series. New York Fed survey releases
remain retained because the structured history is small and useful for
forecast backtesting.

Generate the version 2 outlook:

```powershell
python -m app.market.mortgage_rate_outlook_v2
python -m app.market.mortgage_rate_outlook_v2 --product 15_year_fixed
```

`multi_signal_v2` weights mortgage-rate momentum at 40%, 10-year Treasury
momentum at 30%, the change in year-over-year CPI inflation at 15%, and the
nearest combined-panel federal-funds expectation at 15%. The 2-year Treasury is
stored for analysis but is not yet weighted. Missing signals are neutral rather
than favorable and lower forecast confidence.

Version 2 confidence remains capped at medium until enough target dates have
passed to measure calibration. The model stores each source observation,
selected expectation horizon, component strength, weight, and final combined
strength.

Load the historical structured survey archive and run the chronological
backtest:

```powershell
python -m app.market.macro_rate_signals --sme-archive
python -m app.market.backtest_mortgage_rate_outlook
```

The backtest samples forecast origins every four weeks, uses only observations
dated on or before each origin, and reserves the latest 30% of completed
outcomes as a chronological holdout. Calibration selects a strength multiplier
using the earlier 70% only. It is approved only when the holdout sample is large
enough and calibrated Brier score beats both the raw model and a stable-rate
baseline.

New York Fed expectation results are treated as available no earlier than 21
days after the first associated FOMC meeting date. This avoids using the
workbook's question-distribution date as though aggregated results were already
public.

The first live backtest did not approve calibration:

- 1 month: scale `0.8`; raw holdout Brier `0.4958`; calibrated `0.4839`;
  stable baseline `0.4482`
- 3 months: scale `0.5`; raw holdout Brier `0.6226`; calibrated `0.5624`;
  stable baseline `0.5204`

Lower Brier score is better. Calibration improved the raw model but did not
beat the simple baseline, so these scales are stored for analysis and are not
used by live forecasts. CPI and PMMS are currently consolidated histories
rather than point-in-time data vintages, so the result is not yet a fully
vintage-correct research backtest.

## Saved searches

Phase 2 saved searches are separate from market scoring. A listing must satisfy
every configured criterion to match; multiple location entries use OR
semantics. Match and failure reasons are stored as structured JSON for every
new listing state.

Supported criteria:

- State with optional county, city, and ZIP restrictions
- Price range
- Minimum/maximum beds and baths
- Minimum/maximum square feet
- Minimum/maximum lot acreage, when available
- Property types
- Listing statuses
- Maximum days on market
- Minimum price-drop amount and/or percentage
- Minimum county-market score

Example request:

```json
{
  "name": "Wayne active homes",
  "description": "Primary home search",
  "criteria": {
    "locations": [
      {
        "state": "OH",
        "counties": ["Wayne"]
      }
    ],
    "price": {
      "min": 50000,
      "max": 175000
    },
    "beds": {
      "min": 3
    },
    "baths": {
      "min": 1.5
    },
    "property_types": ["SingleFamilyResidence"],
    "statuses": ["Active"],
    "max_days_on_market": 60,
    "price_drop": {
      "min_amount": 5000,
      "min_percent": 3
    },
    "deal_score": {
      "min": 75
    }
  },
  "enabled": true,
  "notification_mode": "immediate",
  "cooldown_minutes": 60
}
```

Criteria edits increment `criteria_version`. Changes to the name, description,
enabled flag, notification mode, or cooldown do not. Re-evaluating an unchanged
listing under the same criteria version does not add another evaluation.

Saved-search evaluation does not send Slack messages. Run it explicitly through
the API or CLI:

```powershell
python -m app.searches.evaluate 1
```

The `enabled`, `notification_mode`, and `cooldown_minutes` fields are stored for
future notification processing. They do not trigger delivery in Phase 2.

## API endpoints

- `GET /` — process check
- `GET /health` — verifies database connectivity
- `GET /listings?limit=100&offset=0` — current listings
- `GET /listings/{id}` — one current listing
- `POST /score?limit=100` — runs authoritative market scoring and returns scores
- `GET /alerts?limit=100&offset=0` — sent-alert ledger
- `POST /saved-searches` — create a validated saved search
- `GET /saved-searches` — list saved searches
- `GET /saved-searches/{id}` — retrieve one saved search
- `PATCH /saved-searches/{id}` — update settings or versioned criteria
- `DELETE /saved-searches/{id}` — delete a search and its evaluations
- `POST /saved-searches/{id}/evaluate` — evaluate current listings without Slack
- `GET /saved-searches/{id}/evaluations` — inspect stored match decisions

The API is not authenticated. Do not expose it publicly until authentication
and authorization are implemented.

## Tests

Run unit tests:

```powershell
python -m pytest -m "not integration"
ruff check app migrations tests
ruff format --check app migrations tests
```

PostgreSQL integration tests are opt-in. They create and remove an isolated
schema and require a database whose name ends in `_test`:

```powershell
$env:TEST_DATABASE_URL = "postgresql+psycopg2://user:password@localhost:5432/realestate_test"
python -m pytest -m integration
```

The database-name guard is intentional. Never point integration tests at the
development or production database.

## Project layout

```text
app/
  alerts/       Slack message detection, delivery, and sent ledger
  ingest/       Sample and county-data loaders
  transforms/   History, price-change, and market-score transforms
  searches/     Saved-search models, evaluator, persistence, CLI, and routes
  api.py        FastAPI endpoints
  config.py     Environment settings
  db.py         Shared SQLAlchemy engine
migrations/     Authoritative Alembic schema history
sql/            Non-schema analysis/reporting SQL only
tests/          Unit and optional PostgreSQL integration tests
```

## Current limitations

- No authorized live listing provider
- County-wide median scoring is too coarse for investment decisions
- Alert delivery does not yet use a transactional outbox/retry worker
- No production scheduler, authentication, metrics, or backup automation

Those items are planned in later phases in [ROADMAP.md](ROADMAP.md).
