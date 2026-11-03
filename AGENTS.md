# Real Estate Agent Project Instructions

## Project purpose
This project retrieves residential listings (sample CSV and the RentCast API),
stores current and historical data in Postgres, values listings against
comparable sales and market context, evaluates saved searches, and sends
deduplicated Slack alerts for price drops and potential deals.

`README.md` documents every workflow in detail. `ROADMAP.md` tracks status,
known issues, and the gates for alert promotion.

## Current stack
- Python, FastAPI (`app/api.py`, dashboard at `/dashboard`)
- Postgres, with schema managed by Alembic (`migrations/`)
- Docker Compose (`postgres`, `api`, plus the `shadow` and `backup` profiles)
- Slack incoming webhooks (alerts, test channel, ops warnings)
- GitHub Actions CI (`.github/workflows/ci.yml`)

## Two pipelines
- **Sample alert pipeline**: `python -m app.run_pipeline`. Loads
  `data/sample_listings.csv`, snapshots history, scores listings against
  market context, evaluates saved searches, and
  queues price-drop alerts in the outbox. It does not send to Slack.
- **Shadow pipeline**: `python -m app.shadow.run_pipeline`. Uses real Wayne
  County data (RentCast, Redfin, county auditor sales). Shadow listings are
  `alert_eligible = false` and the runner has no alert webhook. Keep it that
  way: promotion to alerts needs the gates in `ROADMAP.md` and explicit
  approval.

## Key workflows
- Apply schema: `python -m alembic upgrade head`
- Load sample listings: `python -m app.ingest.load_sample_listings`
- Snapshot history: `python -m app.transforms.snapshot_listings`
- Queue alerts: `python -m app.alerts.queue_price_drop_alerts` and
  `python -m app.alerts.send_potential_deal_alerts`
- Deliver alerts: `python -m app.alerts.process_alert_outbox` (use `--dry-run`
  or `--test-channel` first; both leave the outbox unchanged)
- Shadow readiness: `python -m app.shadow.readiness`
- Shadow review report: `python -m app.reports.shadow_scoring_review`
- API: `python -m app.api`

## Rules
- Do not commit `.env` or secrets. CI runs gitleaks over the full history.
- Prefer small, reversible changes.
- Keep imports package-style: `from app...`
- Make schema changes as Alembic revisions in `migrations/versions/`
  (`python -m alembic revision -m "..."`) with a working downgrade. `sql/` is
  only for standalone reporting and reconciliation queries, never schema.
- Preserve idempotent behavior where possible.
- Ask before changing alert logic in ways that could spam Slack, and before
  anything that could make shadow listings alert-eligible.
- RentCast requests count against a small monthly plan. Do not add calls to
  scheduled or test paths without a request budget.

## Coding preferences
- Keep scripts simple and readable.
- Add clear print/log output for each step.
- Prefer explicit SQL over magic abstractions.
- Use Git-friendly incremental changes.
- Update `README.md` and `ROADMAP.md` when behavior or status changes.

## Validation
Before declaring work complete:
1. Lint with the pinned version, as CI does:
   `python -m ruff check app tests scripts migrations` (ruff 0.8.6 from
   `requirements-dev.txt`; newer ruff versions report unrelated errors).
2. Run unit tests: `python -m pytest -q tests --ignore=tests/integration`
3. Run the affected Python module and check for import or runtime errors.
4. For DB changes, run the integration suite against a disposable database:
   set `TEST_DATABASE_URL` and run `python -m pytest -q -m integration tests/integration`.
   CI also checks an Alembic upgrade/downgrade/upgrade round trip.
5. Summarize what changed and any follow-up steps.
