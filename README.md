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

For Slack alerts, add:

```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
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

Run schema files as needed:

```powershell
python -m app.utils.run_sql sql/001_create_listings_current.sql
python -m app.utils.run_sql sql/002_create_listing_history.sql
python -m app.utils.run_sql sql/003_create_alerts_sent.sql
python -m app.utils.run_sql sql/011_create_investment_analyses.sql
```

Apply the authoritative Alembic schema, including comparable valuations:

```powershell
python -m alembic upgrade head
```

## AI deal analyst

After loading listings, county sales, snapshots, and market scores, create an
analysis with `POST /listings/{source}/{source_listing_id}/analyses`. The saved
record includes the exact evidence snapshot, prompt version, model, structured
conclusions, citations, risks, and due-diligence questions.

Submit human feedback with `POST /analyses/{analysis_id}/reviews` using a
decision of `approved`, `rejected`, or `corrected`. Corrected decisions require
a `corrections` JSON object, making reviewed cases reusable as regression data.

## Workflows

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

## Comparable valuation and Deal Score v2

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
component results linked to that valuation.

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

The opportunity component is also `unavailable` when no valid price history
exists. Each score component has its own input fingerprint, so unchanged
evidence is reused while backfilled or changed history produces a new auditable
component. Deal Score v2 is still incomplete and is not used by the pipeline or
alert logic.
