# Deployment

How to set up, check, update, and switch live alerts on and off for the stack
on one Windows PC with Docker Desktop. Commands are PowerShell, run from the
repository root (the folder that contains `docker-compose.yml` and `.env`).

Related: [credential rotation](credential-rotation.md),
[incident response](incident-response.md), [Postgres backups](postgres-backups.md).

## What runs

| Service | Profile | Purpose |
| --- | --- | --- |
| `postgres` | none | Database. Data lives in the `postgres_data` volume. |
| `api` | none | FastAPI on `127.0.0.1:8000`, plus the `/dashboard`. |
| `shadow-runner` | `shadow` | Runs the shadow pipeline every 72 hours. Never sends listing alerts. |
| `alert-worker` | `alerts` | The only service that holds `SLACK_WEBHOOK_URL`. Queues and sends live alerts. |
| `backup` | `backup` | Daily `pg_dump` into `data/backups/`. |
| `backup-verify` | `backup` | Weekly restore test of the newest dump into a throwaway database. |

`docker compose up -d` with no profile starts only `postgres` and `api`.
Profiled services start only when you name the profile (or the service).

## First-time setup

1. Install Docker Desktop and Git. Start Docker Desktop and wait until it says
   it is running.
2. Clone and enter the repository:

   ```powershell
   git clone <repo-url> real-estate-agent
   cd real-estate-agent
   ```

3. Create `.env` from the template. `.env` is ignored by Git and Docker; never
   commit it.

   ```powershell
   Copy-Item .env.example .env
   notepad .env
   ```

4. Fill in the variables below. Anything not listed is optional tuning; the
   comments in `.env.example` describe each.

   Required for Compose to run at all:

   | Variable | Notes |
   | --- | --- |
   | `DB_PASSWORD` | Or `POSTGRES_PASSWORD`. `DB_PASSWORD` wins if both are set. Compose refuses to start without one. Use a long random value. |
   | `RENTCAST_API_KEY` | Commented out in `.env.example`; uncomment it. Compose reads the whole file, so **every** `docker compose` command fails without it, even if you only start `postgres` and `api`. If you do not have a key yet, put a placeholder such as `not-set-yet`. Scheduled runs make no RentCast calls unless `SHADOW_ENABLE_RENTCAST_API_CALLS=true`. |

   Keep the defaults for `DB_HOST=localhost`, `DB_PORT`, `DB_NAME` and
   `DB_USER`. The containers override the host to `postgres` themselves; the
   `localhost` values are for commands you run directly on the PC.

   Optional, recommended:

   | Variable | Used by | Purpose |
   | --- | --- | --- |
   | `SLACK_OPS_WEBHOOK_URL` | `shadow-runner`, `alert-worker` | Ops warnings channel (failed or overdue runs, budget, capacity). |
   | `SHADOW_HEARTBEAT_URL` | `shadow-runner` | External dead-man's-switch, for example a Healthchecks.io check. |
   | `SLACK_WEBHOOK_URL` | `alert-worker` | Live alert channel. Only needed to send alerts. |
   | `SLACK_TEST_WEBHOOK_URL` | host commands only | Test channel for `--test-channel` and `send_test_alerts`. Must differ from `SLACK_WEBHOOK_URL`. |
   | `LIVE_ALERTS_ENABLED` | `shadow-runner`, `alert-worker` | Defaults to `false`. See [Live alerts](#live-alerts-on-and-off). |
   | `SHADOW_ENABLE_RENTCAST_API_CALLS` | `shadow-runner` | Defaults to `false`. When `true`, each run may use up to 9 RentCast requests, within the monthly budget. |
   | `BACKUP_OFFSITE_DIR` | `backup` | Host folder for an off-machine copy. Use forward slashes. |

   `API_AUTH_REQUIRED`, `API_AUTH_TOKEN` and `API_RATE_LIMIT_PER_MINUTE` are in
   `.env.example` and are passed to the `api` container (auth off and 120
   requests per minute unless you set them). See
   [credential rotation](credential-rotation.md#api_auth_token).

5. Check that Compose accepts the file. `--quiet` prints nothing on success.
   Do not run `docker compose config` without `--quiet` and paste the output:
   it prints your secrets.

   ```powershell
   docker compose --profile shadow --profile backup --profile alerts config --quiet
   ```

6. Start the stack. This builds the images the first time.

   ```powershell
   docker compose --profile shadow --profile backup up -d --build
   ```

   Leave out `--profile alerts` until you want live alerts.

### How migrations run

- `api` and `shadow-runner` each run `python -m alembic upgrade head` before
  their main process starts, so a normal start applies pending migrations.
- `alert-worker`, `backup` and `backup-verify` do not run migrations.
- The migrations are copied into the images, so a new migration needs a
  rebuild (see [Updating](#updating-after-a-merged-pr)).
- To apply or inspect migrations by hand, run them inside the API container:

  ```powershell
  docker compose exec api python -m alembic current
  docker compose exec api python -m alembic upgrade head
  ```

- The first start of `shadow-runner` begins a run immediately
  (`SHADOW_INITIAL_DELAY_HOURS` defaults to 0).

## Checking that it is healthy

1. Container status. `api`, `postgres` and `shadow-runner` should say
   `healthy`. `alert-worker`, `backup` and `backup-verify` have no health check;
   `Up` is all Docker can tell you, so read their logs.

   ```powershell
   docker compose --profile shadow --profile backup --profile alerts ps
   ```

2. API health. `status` should be `ok`.

   ```powershell
   Invoke-RestMethod http://127.0.0.1:8000/health | ConvertTo-Json -Depth 5
   ```

   - `degraded` means shadow history is missing or stale, or at least one
     `alert_outbox` row is `permanent_failed`.
   - HTTP 503 means the API cannot reach the database.
   - `/health` is always public, even when API auth is on.

3. Shadow scheduler. Look for `Next shadow attempt due at ...` and
   `Shadow run succeeded`:

   ```powershell
   docker compose --profile shadow logs --tail 50 shadow-runner
   ```

4. Promotion readiness. This reports whether the shadow data meets the gates
   for promotion (three run dates over 14 days, and so on). It is not a
   liveness check, so `ready: false` early on is normal.

   ```powershell
   docker compose --profile shadow exec shadow-runner python -m app.shadow.readiness
   ```

5. Capacity, read-only. Run it on the PC (with the project virtualenv active
   and `.env` pointing at `localhost`) to include `data/backups` usage:

   ```powershell
   python -m app.ops.capacity
   ```

6. Backups. A new `.dump` file should appear in `data\backups` about every
   24 hours:

   ```powershell
   Get-ChildItem data\backups\*.dump | Sort-Object LastWriteTime -Descending | Select-Object -First 3 Name, Length, LastWriteTime
   ```

## Updating after a merged PR

1. Pull:

   ```powershell
   git pull
   ```

2. Rebuild only what the change touches. Each of `api`, `shadow-runner` and
   `alert-worker` builds its own image from the same `Dockerfile`; `backup` and
   `backup-verify` build from `scripts/backup.Dockerfile`.

   | What changed | Run |
   | --- | --- |
   | `app/`, `migrations/`, `alembic.ini`, `requirements.txt`, `Dockerfile` | Rebuild `api`, `shadow-runner` and (if running) `alert-worker`. |
   | `scripts/` or `app/ops/backup_retention.py` | Rebuild `backup` and `backup-verify`. |
   | `docker-compose.yml` or `.env` only | `up -d` without `--build`. Compose recreates only services whose configuration changed. |
   | `docs/`, `tests/`, `README.md`, `ROADMAP.md` | Nothing. |

   Not sure? Rebuild everything you run. Compose recreates only containers
   whose image or configuration changed:

   ```powershell
   docker compose --profile shadow --profile backup up -d --build
   ```

   Add `--profile alerts` if the alert worker is running. Note that
   `docker compose restart` does **not** re-read `.env` or pick up a new image;
   use `up -d`.

   To limit the interruption, do it in this order when a migration is included:

   ```powershell
   docker compose up -d --build api
   docker compose ps api            # wait for (healthy)
   docker compose --profile shadow up -d --build shadow-runner
   docker compose --profile alerts up -d --build alert-worker   # only if live alerts are on
   ```

   With `LIVE_ALERTS_ENABLED=false`, `alert-worker` logs that live alerts are
   off and idles without sending; set the flag to `true` and recreate it to
   start sending.

3. Before recreating `shadow-runner`, check that it is not mid-run. A recreated
   container kills a run in progress, and that run is retried six hours after
   it started. A run is in progress if the last log line is `Starting shadow run`
   with no `Shadow run succeeded` or `Shadow run failed` after it:

   ```powershell
   docker compose --profile shadow logs --tail 20 shadow-runner
   ```

4. Confirm: `docker compose ps`, the `/health` call above, and a look at the
   logs of each service you rebuilt. A migration problem shows up in the `api`
   or `shadow-runner` logs as an Alembic error, and that container keeps
   restarting.

5. Rolling back code is `git switch`/`git revert` plus the same rebuild.
   Migrations are not undone automatically. Check you have a recent backup
   (the `Get-ChildItem` call under "Checking that it is healthy") before you merge a PR that includes a migration.

## Live alerts on and off

Live alerts are off unless `LIVE_ALERTS_ENABLED=true`. Two services read the
flag: `shadow-runner` (marks synced RentCast listings alert-eligible) and
`alert-worker` (queues and sends). The shadow runner never sends alerts.

### Turn on

1. In `.env` set `SLACK_WEBHOOK_URL=<live-alerts-webhook>` and
   `LIVE_ALERTS_ENABLED=true`. Optionally set `ALERT_WORKER_INTERVAL_MINUTES`
   (default 15).
2. Set `SLACK_OPS_WEBHOOK_URL` too, so worker failures reach you.
3. Preview what would be sent without touching the outbox:

   ```powershell
   python -m app.alerts.process_alert_outbox --dry-run
   ```

4. Recreate the runner so the next sync marks listings eligible, then start the
   worker:

   ```powershell
   docker compose --profile shadow up -d shadow-runner
   docker compose --profile alerts up -d --build alert-worker
   docker compose --profile alerts logs --tail 20 alert-worker
   ```

   Expect `Live alert worker started; checking every 15 minute(s).` followed by
   `Live alert cycle complete: {...}`.

Price drops need two alert-eligible snapshots, so they start from the second
run after enabling. See the README's [Live alerts](../README.md#live-alerts).

### Turn off

For an emergency, use the fast path in
[incident response](incident-response.md#stop-all-alerts-now). The routine
version:

1. Stop the worker first. A running worker keeps the old flag value until it is
   stopped or recreated, so it would keep sending.

   ```powershell
   docker compose --profile alerts stop alert-worker
   ```

2. Set `LIVE_ALERTS_ENABLED=false` in `.env`.
3. Recreate the runner so the next sync makes listings shadow-only again:

   ```powershell
   docker compose --profile shadow up -d shadow-runner
   ```

Alerts already in `alert_outbox` stay there. They are sent when the worker next
runs, which may be much later.

## Keeping Docker Desktop running

- Nothing runs unless Docker Desktop is running. Turn on Docker Desktop's
  setting to start when you sign in to Windows (Settings > General). It starts
  at sign-in, so after a reboot nothing runs until you sign in.
- Containers use `restart: unless-stopped`. When Docker Desktop starts, they
  come back by themselves, except any you stopped on purpose (for example
  `alert-worker` after a kill switch).
- After a Docker restart, a service may log one failed connection to Postgres
  and retry. That is harmless; the backup worker is a known example.
- Windows Update restarts and Docker Desktop updates stop everything briefly.
  Check `docker compose ps` afterwards.

### When the PC sleeps

- The shadow scheduler re-reads wall-clock time at least every 60 seconds. A run
  that came due during sleep starts on wake, and the deadline survives restarts
  because it is rebuilt from run history.
- After a long sleep you may get an "overdue" ops warning, followed by a
  recovery message after the next successful run.
- Backups, restore checks and the alert worker are timer loops. They continue
  after wake but can run late. Check the newest dump's `LastWriteTime` after a
  long sleep.
- If the PC is asleep or off for longer than your heartbeat period plus grace
  (3 days + 7 hours in the suggested Healthchecks.io setup), the external
  heartbeat alert fires even though nothing is broken.
- To avoid all of this, set Windows power options to never sleep while plugged
  in (Settings > System > Power).
