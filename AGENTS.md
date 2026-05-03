# Real Estate Agent Project Instructions

## Project purpose
This project monitors residential listings, stores current and historical data in Postgres, detects price drops, and sends Slack alerts.

## Current stack
- Python
- Postgres
- Docker
- VS Code
- Slack webhooks

## Key workflows
- Load listings: `python -m app.ingest.load_sample_listings`
- Snapshot history: `python -m app.transforms.snapshot_listings`
- Detect/send alerts: `python -m app.alerts.send_price_drop_alerts`
- Run full pipeline: `python -m app.run_pipeline`

## Rules
- Do not commit `.env` or secrets.
- Prefer small, reversible changes.
- Keep imports package-style: `from app...`
- Update SQL files in `sql/` for schema changes.
- Preserve idempotent behavior where possible.
- Ask before changing alert logic in ways that could spam Slack.

## Coding preferences
- Keep scripts simple and readable.
- Add clear print/log output for each step.
- Prefer explicit SQL over magic abstractions.
- Use Git-friendly incremental changes.

## Validation
Before declaring work complete:
1. Run the affected Python module.
2. Check for import/runtime errors.
3. Confirm DB changes against Postgres when relevant.
4. Summarize what changed and any follow-up steps.