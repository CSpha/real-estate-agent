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
keeps only Wayne County records, and is idempotent. It retains land records in
the raw and current tables for auditability but marks them
`scoring_eligible = false`. Missing square footage is left as unavailable
rather than zero; those homes remain scoring-eligible for components that do
not need price per square foot.

Shadow listings have `alert_eligible = false` in both current and historical
records. They can be snapshotted and scored, but neither price-drop nor
potential-deal alert queries can select them. The command also deliberately
skips saved-search evaluation and alert queueing.

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
component. The Deal Score v2 calculation and persistence model is complete but
is not yet used by the pipeline or alert logic; backfill and activation review
remain separate steps.
