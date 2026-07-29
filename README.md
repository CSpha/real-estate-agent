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
