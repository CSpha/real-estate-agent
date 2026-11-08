# Incident response

Symptom, check, fix for the failures this system reports. Commands are
PowerShell, run from the repository root. If you need to stop Slack alerts
immediately, jump to [Stop all alerts now](#stop-all-alerts-now).

Related: [deployment](deployment.md), [credential rotation](credential-rotation.md),
[Postgres backups](postgres-backups.md).

## First look (30 seconds)

```powershell
docker info > $null; $LASTEXITCODE      # 0 = Docker Desktop is running
docker compose --profile shadow --profile alerts --profile backup ps
Invoke-RestMethod http://127.0.0.1:8000/health | ConvertTo-Json -Depth 5
docker compose --profile shadow logs --tail 50 shadow-runner
```

- `docker info` fails: Docker Desktop is not running. Start it; containers
  marked `restart: unless-stopped` come back by themselves.
- `/health` returns HTTP 503: see [Database is down](#database-is-down).

### Read-only database queries

Use this pattern for every query below. `SET default_transaction_read_only = on`
makes the session refuse any write, and the shell variables are filled in by the
container, so no password or user name is typed:

```powershell
"SET default_transaction_read_only = on; SELECT 1;" | docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB'
```

Replace `SELECT 1;` with the query. Use single quotes for SQL strings inside the
PowerShell double quotes.

## Where warnings come from

| Message (ops Slack channel) | Section |
| --- | --- |
| `Shadow pipeline run failed` | [Shadow run failed or overdue](#shadow-run-failed-or-overdue) |
| `Shadow pipeline overdue` | [Shadow run failed or overdue](#shadow-run-failed-or-overdue) |
| `Shadow pipeline cannot read run history` | [Database is down](#database-is-down) |
| `Live alert worker delivery failed` / `cycle failed` | [Alert delivery failure](#alert-delivery-failure) |
| `Shadow pipeline repeated RentCast failures` / `RentCast repeatedly skipped` / `RentCast request budget nearly exhausted` | [RentCast budget or provider failure warning](#rentcast-budget-or-provider-failure-warning) |
| `Shadow pipeline database size over threshold` | [Database capacity warning](#database-capacity-warning) |
| Email or message from your heartbeat service | [Heartbeat missing](#heartbeat-missing) |

Each warning is posted once when the problem starts and once when it clears
(`:white_check_mark: ... recovered from ...`). If you never get a recovery
message, the problem is still there. The notifier remembers state in
`data/shadow-runner/ops_notification_state.json`,
`data/shadow-runner/ops_monitor_*.json` and
`data/alert-worker/alert_worker_notification_state.json`.

Backups and backup verification send **no** Slack message. See
[Backup or backup verification failure](#backup-or-backup-verification-failure).

## Shadow run failed or overdue

**Symptom:** ops Slack says `Shadow pipeline run failed (since ...)` or
`Shadow pipeline overdue (since ...)`.

**Check:**

```powershell
docker compose --profile shadow logs --tail 100 shadow-runner
"SET default_transaction_read_only = on; SELECT id, status, started_at, finished_at, left(error_message, 300) AS error FROM shadow_pipeline_runs WHERE source = 'rentcast' ORDER BY started_at DESC LIMIT 5;" | docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB'
```

Look for `Shadow run failed: <ExceptionType>: <message>` in the logs; the same text
is stored in `error_message`.

**Fix by cause:**

- **Database connection errors:** go to [Database is down](#database-is-down).
- **RentCast errors** (HTTP 401, 403, 429, 5xx): go to
  [RentCast budget or provider failure warning](#rentcast-budget-or-provider-failure-warning).
- **A code or migration error right after an update:** read the traceback in the
  logs, then fix the code or roll back (`git switch`, rebuild). See
  [deployment](deployment.md#updating-after-a-merged-pr).
- **`overdue`:** the run did not finish by its deadline plus 15 minutes. The
  scheduler checks every 5 minutes and, after two misses, exits so Docker
  restarts it (`Exiting so Docker restarts the shadow scheduler.` in the logs).
  After a PC sleep or wake it usually clears itself with the next run. If
  `shadow-runner` is not `Up`, start it:

  ```powershell
  docker compose --profile shadow up -d shadow-runner
  ```

  If it is `Up` but stuck on one run for hours:

  ```powershell
  docker compose --profile shadow restart shadow-runner
  ```

  An interrupted run is retried six hours after it started.

**Retrying:** failed runs retry on their own after `SHADOW_RETRY_HOURS` (default
6). Restarting the container does not make the run happen sooner, because the
deadline is rebuilt from history. To run once now without touching RentCast:

```powershell
docker compose --profile shadow exec shadow-runner python -m app.shadow.run_pipeline --skip-rentcast
```

Without `--skip-rentcast`, a manual run calls RentCast and bypasses the
scheduler's monthly budget. The recovery message in Slack only follows the
scheduler's own next success.

## Heartbeat missing

**Symptom:** your heartbeat service (for example Healthchecks.io) reports the
check as late or down. The scheduler pings only after a **successful** run, every
72 hours by default, so you may hear about a problem up to a few days late.

**Check, in this order:**

1. Is the PC on and awake, and is Docker Desktop running (`docker info`)?
2. Is `shadow-runner` `Up`/`healthy` (`docker compose --profile shadow ps`)?
3. Did a run succeed recently? `/health` shows `last_successful_run_at` and
   `latest_status`; the run-history query above shows the same.
4. Did the ping itself fail?

   ```powershell
   docker compose --profile shadow logs shadow-runner | Select-String Heartbeat
   ```

   - `Heartbeat sent.`: it worked; check the monitoring service's check settings.
   - `Heartbeat failed: ... (HTTP 404)`: the URL is wrong or the check was deleted.
   - `Heartbeat not sent: SHADOW_HEARTBEAT_URL is not set.`: the container does
     not have the variable; set it in `.env` and run
     `docker compose --profile shadow up -d shadow-runner`.
5. Test the URL from the PC: `python -m app.ops.heartbeat --test`.

**Fix:** a run that failed or never ran: see
[Shadow run failed or overdue](#shadow-run-failed-or-overdue). PC or Docker was
off or asleep: start it, and the overdue run starts by itself. A failed ping does
not fail the run, so a broken URL only shows here and in the logs. If you paused
the stack on purpose, pause the check in the monitoring service too.

## Alert delivery failure

**Symptom:** ops Slack says `Live alert worker delivery failed` ("N alert(s) could
not be delivered; check the alert_outbox last_error column") or
`Live alert worker cycle failed`. `/health` may report `degraded` with
`permanent_alert_failure_count` above zero.

**Check (all read-only):**

```powershell
docker compose --profile alerts logs --tail 50 alert-worker
"SET default_transaction_read_only = on; SELECT status, count(*) FROM alert_outbox GROUP BY status ORDER BY status;" | docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB'
"SET default_transaction_read_only = on; SELECT id, alert_type, source_listing_id, status, attempt_count, available_at, left(last_error, 200) AS last_error FROM alert_outbox WHERE last_error IS NOT NULL OR status IN ('retryable_failed', 'permanent_failed') ORDER BY id DESC LIMIT 20;" | docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB'
```

`last_error` is cleared when a row is claimed for a new attempt, so a row with
an error is one that has just failed. It can contain connection-error text, so
read it before you share it.

Statuses: `pending` (waiting), `processing` (claimed; reclaimed after 10
minutes), `retryable_failed` (will retry at `available_at`), `permanent_failed`
(gave up), `sent`.

| `last_error` starts with | Meaning | Fix |
| --- | --- | --- |
| `Slack returned HTTP 404`, `410`, `403`, `401`, or a body such as `no_service`, `invalid_token`, `channel_not_found`, `channel_is_archived` | Webhook revoked, deleted or channel archived. The run stops at the first one. | Make a new webhook and rotate `SLACK_WEBHOOK_URL`: [credential rotation](credential-rotation.md#slack_webhook_url-slack_test_webhook_url-slack_ops_webhook_url). |
| `Slack returned HTTP 429` | Slack rate limit. Retried after `Retry-After`, does not use up attempts. | Wait. |
| `Slack returned HTTP 5xx`, `Slack request failed: ...` | Slack or network problem. Retried up to 5 attempts with backoff (about 30 seconds up to an hour). | Check the PC's internet. If attempts run out the row becomes `permanent_failed`. |
| `Slack payload ...` or `Unsupported alert type` | The message itself is invalid. Permanent. | Not fixable by retrying. Report it as a bug with the row `id`. |

**Requeue after fixing the cause** (this is a write, and each requeued alert is
sent to the live channel on the next cycle; they may be stale by now, so decide
first whether you want them):

```powershell
"UPDATE alert_outbox SET status = 'pending', attempt_count = 0, available_at = CURRENT_TIMESTAMP, claimed_at = NULL, last_error = NULL WHERE status = 'permanent_failed' AND id IN (<id>, <id>);" | docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB'
```

Notes:

- `permanent_failed` rows keep `/health` at `degraded` for as long as they exist.
  The system has no "dismissed" status. Requeue them, or accept the `degraded`.
- Do not delete outbox rows. The unique event key is how queueing avoids sending
  the same event twice, so a deleted row can be queued and sent again.
- If the worker container is gone or the cycle itself failed, read its logs, fix
  (often the database or `.env`), and recreate:
  `docker compose --profile alerts up -d alert-worker`.

## Backup or backup verification failure

**Symptom:** nothing tells you. The `backup` and `backup-verify` workers have no
Slack or heartbeat connection. Look weekly, and after any outage.

**Check:**

```powershell
docker compose --profile backup ps
docker compose --profile backup logs --tail 30 backup
docker compose --profile backup logs --tail 30 backup-verify
Get-ChildItem data\backups\*.dump | Sort-Object LastWriteTime -Descending | Select-Object -First 3 Name, Length, LastWriteTime
```

A healthy `backup` log ends in `Backup complete: ...` and
`Next PostgreSQL backup in 86400 seconds`. A dump older than about 25 hours
(longer if the PC slept) means a problem.

| Log line | Cause | Fix |
| --- | --- | --- |
| `Backup failed; retrying in 300 seconds` with `password authentication failed` | `.env` password does not match the database | [Database password](credential-rotation.md#db_password-and-postgres_password) "If it goes wrong" |
| `Backup failed; ...` with `could not connect` / `Connection refused` | Postgres is down or still starting. It retries by itself. | [Database is down](#database-is-down) |
| `No space left on device` | Disk full | Free space; see [capacity](#database-capacity-warning). Retention keeps 14 daily and 8 weekly dumps by default. |
| `Off-machine copy FAILED` | The local backup worked, the copy to `BACKUP_OFFSITE_DIR` did not (drive missing, sync folder not running) | Fix the path or drive; the next backup copies again. |
| `Post-backup steps had problems; backup itself succeeded` | Pruning or copy had an error; look above this line | As above. |
| `Restore verification failed: restored database metadata or table row counts differ from source` | Data was written between the dump and the check | Re-run in a quiet window (below). |
| `Restore verification failed: ...` (other) | The dump cannot be restored. Treat as serious. | Read the message, take a fresh backup, re-verify. If it fails again, the dump format or database is the problem. |
| `No PostgreSQL dumps found in /backups` | No backup has completed yet | Look at the `backup` log. |

Backups retry every 5 minutes and verification every hour (`BACKUP_VERIFY_RETRY_SECONDS`)
without help. To re-run a verification now:

```powershell
docker compose --profile backup exec -T backup python3 /app/scripts/verify_postgres_backup.py
```

A successful verification prints `Restore verification passed: ...` and removes
its test database. It never touches the application database.

If the services themselves are not running: `docker compose --profile backup up -d`.

## RentCast budget or provider failure warning

**Symptom:** ops Slack says one of:

- `repeated RentCast failures`: the last 2 RentCast syncs
  (`PROVIDER_FAILURE_THRESHOLD`) failed. The text includes the latest error.
- `RentCast request budget nearly exhausted`: `N of M monthly RentCast requests
  used`, warning at 80% (`RENTCAST_BUDGET_WARN_PERCENT`).
- `RentCast repeatedly skipped`: runs skip RentCast although calls are enabled,
  the budget has room and no recent attempt explains it.

These only exist when `SHADOW_ENABLE_RENTCAST_API_CALLS=true`; with the default
`false` the scheduler never calls RentCast.

**Check:**

```powershell
"SET default_transaction_read_only = on; SELECT id, status, started_at, details_json -> 'rentcast' ->> 'request_count' AS requests, left(error_message, 150) AS error FROM shadow_pipeline_runs WHERE source = 'rentcast' AND started_at >= date_trunc('month', now()) ORDER BY started_at;" | docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB'
docker compose --profile shadow logs --tail 100 shadow-runner | Select-String -Pattern "RentCast"
```

The query lists this month's runs (the database clock is UTC, matching the
budget's calendar month) and the requests each used.

**Fix:**

- **Budget:** informational. When used requests plus the nine-request worst case
  would exceed `SHADOW_RENTCAST_MONTHLY_REQUEST_LIMIT` (default 40), scheduled
  runs skip RentCast and still refresh the other sources. The count resets at
  the start of the UTC month. The budget does not count manual audits, standalone
  syncs or `sync_rent_estimates`, which also use your plan. Raise the limit in
  `.env` only if your RentCast plan allows it, then
  `docker compose --profile shadow up -d shadow-runner`.
- **Failures with 401 or 403:** bad or revoked key. Rotate it:
  [credential rotation](credential-rotation.md#rentcast_api_key). These errors are
  not retried.
- **429:** plan or rate limit. Retries respect `Retry-After` up to 60 seconds;
  beyond that the sync gives up. Wait, and check your RentCast usage page.
- **5xx or timeouts:** provider trouble. Retried twice per request, then the run
  fails and retries after 6 hours. Wait.
- **Repeatedly skipped:** check `SHADOW_RENTCAST_MIN_HOURS` (default 60) and
  whether the key is set; read the `Scheduled RentCast API calls are ...` line in
  the runner logs, which gives the reason.

## Database capacity warning

**Symptom:** ops Slack says `database size over threshold`: `Database is N MB,
over the M MB warning threshold. Largest tables: ...` (`DB_SIZE_WARN_MB`, default
5000). It is checked after each scheduled run.

**Check** (read-only):

```powershell
python -m app.ops.capacity
```

Run it on the PC with the project virtualenv active. It shows the size, the ten
largest tables, key row counts and the size of `data/backups`. Without the
virtualenv, `docker compose --profile shadow exec shadow-runner python -m app.ops.capacity`
works too, but reports the backup folder as not found.

Also check free space on the Windows drive and Docker Desktop's disk image
(Settings > Resources).

**Fix:**

- If the threshold is simply too low for your data, raise `DB_SIZE_WARN_MB` in
  `.env` (0 turns the check off) and run
  `docker compose --profile shadow up -d shadow-runner`.
- If growth is the problem, look at the largest tables. Nothing is deleted
  automatically, and there is no pruning command. Take a backup, and decide
  deliberately before removing any data.
- If the backup folder is the big part, lower `BACKUP_KEEP_DAILY` or
  `BACKUP_KEEP_WEEKLY` and recreate the backup services (see
  [Postgres backups](postgres-backups.md)).
- If the disk is nearly full, fix that first: a full disk stops Postgres and
  backups.

## Database is down

**Symptom:** `/health` returns HTTP 503 (`Database is unavailable`); ops Slack
says `cannot read run history`; logs show `connection refused`, `could not
connect` or `password authentication failed`; the dashboard shows no data.

**Check:**

```powershell
docker info > $null; $LASTEXITCODE
docker compose ps postgres
docker compose logs --tail 50 postgres
```

**Fix by cause:**

- **Docker Desktop is not running:** start it. Containers return on their own.
- **`postgres` exited or was stopped:** `docker compose up -d postgres`. Read
  the end of its log for the reason.
- **`No space left on device` in the logs:** free disk space, then start it.
- **Port 5432 already in use** (another PostgreSQL on the PC): set `DB_PORT` in
  `.env` to a free port and run `docker compose up -d postgres`. Containers still
  use `postgres:5432` internally.
- **Postgres is `healthy` but services log `password authentication failed`:**
  the `.env` password and the database disagree. See
  [credential rotation](credential-rotation.md#if-it-goes-wrong).
- **Data corruption or lost volume:** restore, below.

Never run `docker compose down -v` or `docker volume rm` on `postgres_data`:
that deletes the database.

After Postgres is back, the API reconnects by itself, the scheduler retries its
run-history read every minute, and the backup worker retries. Recreate any
service that stays unhealthy. The recovery messages arrive in the ops channel.

## API returns 401 or 429

Look at the request log; every line is JSON with `status_code`, `path` and
`client`, and tokens are never logged:

```powershell
docker compose logs --tail 50 api
```

### 401 Authentication required

The API only checks tokens when `API_AUTH_REQUIRED=true` is in its environment
(see the Compose caveat in [credential rotation](credential-rotation.md#api_auth_token)).
Then every path except `/health` needs `Authorization: Bearer <token>`.

- The header is missing or misspelled, or the value does not start with
  exactly `Bearer ` (capital B, one space).
- The token is stale after a rotation. Update the client.
- `API_AUTH_REQUIRED=true` but `API_AUTH_TOKEN` is empty or unset: every request
  gets 401. Set the token and recreate the API.
- The browser `/dashboard` and `/dashboard-assets/...` also need the header, so
  they return 401 in a browser while auth is on. `/health` always works.

Check: `Invoke-WebRequest http://127.0.0.1:8000/listings?limit=1 -SkipHttpErrorCheck -Headers @{ Authorization = "Bearer <token>" }`.

### 429 Rate limit exceeded

Each client IP may make `API_RATE_LIMIT_PER_MINUTE` requests per rolling minute
(default 120), counted in memory per API process. `/health` is exempt.

- Wait a minute, or slow the client.
- Restarting the API clears the counters: `docker compose restart api`.
- A 429 here is the API's own. RentCast or Slack 429s appear in other logs and
  have their own sections above.
- `docker-compose.yml` does not pass `API_RATE_LIMIT_PER_MINUTE` to the `api`
  container, so the default applies there unless Compose is changed.

## Stop all alerts now

The `alert-worker` container is the only service that has the live-alert webhook
and the only one that sends listing alerts. Stopping it stops them.

1. **Stop the worker** (takes seconds):

   ```powershell
   docker compose --profile alerts stop alert-worker
   ```

   A stopped container stays stopped across Docker Desktop restarts.

2. **Make it stick.** In `.env` set `LIVE_ALERTS_ENABLED=false`, so that nobody
   starts the worker again by accident (with the flag off it refuses to start)
   and so new RentCast listings stop being marked alert-eligible, then:

   ```powershell
   docker compose --profile shadow up -d shadow-runner
   ```

   Check first that the runner is not mid-run
   ([deployment](deployment.md#updating-after-a-merged-pr)).

3. **Block manual sends too.** `python -m app.alerts.process_alert_outbox` on the
   PC sends pending alerts to the live channel whenever `SLACK_WEBHOOK_URL` is in
   `.env`, regardless of `LIVE_ALERTS_ENABLED`. To rule that out, comment out
   `SLACK_WEBHOOK_URL` in `.env`.

4. **Confirm:**

   ```powershell
   docker compose --profile alerts ps alert-worker       # no running container
   "SET default_transaction_read_only = on; SELECT status, count(*) FROM alert_outbox GROUP BY status;" | docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB'
   ```

   `sent` should stop growing.

Last resort: delete the webhook in Slack. Do it only after the worker has stopped:
a revoked webhook marks the alert in hand `permanent_failed` on each cycle.

**What is left behind.** Alerts already queued (`pending`, `retryable_failed`)
stay in `alert_outbox` and are sent when the worker next runs, possibly long
after they stopped being true. Before you turn alerts back on, preview them with
`python -m app.alerts.process_alert_outbox --dry-run`. To discard them, mark them
as permanently failed (a write; it does not delete, so the events are not queued
again, but it leaves `/health` `degraded`):

```powershell
"UPDATE alert_outbox SET status = 'permanent_failed', last_error = 'discarded manually' WHERE status IN ('pending', 'retryable_failed');" | docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB'
```

The shadow pipeline and the ops warnings keep running while alerts are off. To
turn alerts back on, see [deployment](deployment.md#turn-on).

## Restore from backup

Read [Postgres backups](postgres-backups.md) first. It covers where dumps live
(`data/backups/`, custom format, named `<database>_<timestamp>.dump`), retention,
the off-machine copy, and the isolated restore check the system runs weekly.

That document describes verification, not a step-by-step production restore, and
this runbook has not been rehearsed against production either. Before restoring:

1. Stop everything that writes, and keep Postgres up:

   ```powershell
   docker compose --profile shadow --profile alerts --profile backup stop api shadow-runner alert-worker backup backup-verify
   ```

2. Copy the dump you intend to use somewhere safe. If `data\backups` is damaged,
   use the copy in `BACKUP_OFFSITE_DIR`.
3. Choose the dump. The newest verified one is named in `data\backups\.last_verified`
   (`Get-Content data\backups\.last_verified`).
4. Restore into a new, empty database first and inspect it, instead of over the
   live one. `scripts/verify_postgres_backup.py` shows the working recipe:
   `CREATE DATABASE <name> TEMPLATE template0`, then
   `pg_restore --exit-on-error --no-owner --dbname <name> <dump>`.
5. Only when the restored copy looks right, plan the switch-over (renaming
   databases or restoring into the live one) deliberately. Dumps cover the
   application database only, not other databases or role passwords.
6. Restart the services and run the health checks in
   [deployment](deployment.md#checking-that-it-is-healthy).

Consider writing down the exact procedure the first time you do this and adding
it to `docs/postgres-backups.md`.

## After an incident

- [ ] Services are healthy: `docker compose ps`, `/health` is `ok`, and the ops
      channel has its recovery messages.
- [ ] The cause is known and written down: what happened, when it started, how it
      was found, what fixed it.
- [ ] Any secret that was exposed has been rotated:
      [credential rotation](credential-rotation.md#if-a-secret-leaked).
- [ ] Data is complete: the next shadow run succeeded, and `alert_outbox` has no
      unexplained `pending` or `permanent_failed` rows.
- [ ] A recent backup exists after the fix, and the next verification passes.
- [ ] `.env` changes are noted, and `LIVE_ALERTS_ENABLED` is in the state you
      want.
- [ ] Monitoring gaps are closed (for example, backups send no alert; add a
      calendar reminder to check them).
- [ ] If the fix needed a code or docs change, make it on a branch.
