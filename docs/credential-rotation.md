# Credential rotation

How to replace each secret without breaking the stack. Commands are PowerShell,
run from the repository root. Values are written as `<placeholder>`; never paste
a real secret into a document, chat, issue, or commit.

Related: [deployment](deployment.md), [incident response](incident-response.md).

## Where each secret is used

| Secret | Read by (containers) | Also read by commands run on the PC | Recreate after changing |
| --- | --- | --- | --- |
| `DB_PASSWORD` / `POSTGRES_PASSWORD` | `postgres` (first start only), `api`, `shadow-runner`, `alert-worker`, `backup`, `backup-verify` | Everything that connects to the database | `api`, `shadow-runner`, `alert-worker`, `backup`, `backup-verify` |
| `SLACK_WEBHOOK_URL` | `alert-worker` | `process_alert_outbox` (real send) | `alert-worker` |
| `SLACK_TEST_WEBHOOK_URL` | none | `process_alert_outbox --test-channel`, `send_test_alerts` | nothing |
| `SLACK_OPS_WEBHOOK_URL` | `shadow-runner`, `alert-worker` | `python -m app.ops.notify --test` | `shadow-runner`, `alert-worker` |
| `RENTCAST_API_KEY` | `shadow-runner` | `audit_rentcast`, `sync_rentcast_shadow`, `providers.sync`, `sync_rent_estimates` | `shadow-runner` |
| `SHADOW_HEARTBEAT_URL` | `shadow-runner` | `python -m app.ops.heartbeat --test` | `shadow-runner` |
| `API_AUTH_TOKEN` | `api` (see the caveat below) | none | `api` |

All of them live in `.env`. Compose reads `.env` when you run `docker compose`
and passes only the variables listed in `docker-compose.yml` into each
container; `.env` itself is not copied into any image.

Two rules apply to every section below:

- **Use `up -d`, not `restart`.** `docker compose restart` keeps the old
  environment. `up -d` notices the changed value and recreates the container.
- **Add the new secret before you revoke the old one** wherever the provider
  allows it. That keeps downtime near zero.

Only list services you actually run. `docker compose --profile shadow --profile alerts --profile backup ps`
shows them. Do not start `alert-worker` while `LIVE_ALERTS_ENABLED=false`.

## Generate a random value

PowerShell 7 (hex, so it is safe in `.env` and in URLs):

```powershell
-join ([Security.Cryptography.RandomNumberGenerator]::GetBytes(24) | ForEach-Object { $_.ToString('x2') })
```

## DB_PASSWORD and POSTGRES_PASSWORD

### How Compose wires it

For every service, Compose uses `${DB_PASSWORD-${POSTGRES_PASSWORD:?...}}`:
`DB_PASSWORD` if it is set at all, otherwise `POSTGRES_PASSWORD`. Set the same
value in both if you keep both, or keep only one. Host-side Python
(`app/config.py`) also prefers `DB_PASSWORD`. If you set `DATABASE_URL`, it
embeds the password too and must be updated (URL-encode special characters;
hex needs none).

### The trap

The Postgres image applies `POSTGRES_PASSWORD` only when it creates a **new**
data directory. The password already stored in the `postgres_data` volume does
not change when you edit `.env`. If you change only `.env`, the database keeps
the old password and every service that gets the new one fails to log in.
Change the password inside Postgres first, then update `.env`.

You never need the old password for this. Connections from inside the
`postgres` container use the local socket, which is set to `trust` in the
image's `pg_hba.conf`.

### Steps

Expect about a minute in which the API, the runner and the backups cannot open
new database connections. Do steps 2 to 4 back to back, and avoid doing it
while `shadow-runner` is mid-run (see [deployment](deployment.md#updating-after-a-merged-pr)).

1. Generate the new password (above). Keep it in your password manager.
2. Change it inside Postgres. `\password` prompts twice, hides what you type
   and sends only a hash, so the password is not in shell history or in the
   Postgres log:

   ```powershell
   docker compose exec postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB'
   ```

   then, at the `psql` prompt:

   ```text
   \password
   \q
   ```

3. Edit `.env`: set `DB_PASSWORD=<new-password>`, and `POSTGRES_PASSWORD` and
   `DATABASE_URL` too if you have them.
4. Recreate the services that connect, without touching `postgres`:

   ```powershell
   docker compose --profile shadow --profile alerts --profile backup up -d --no-deps --force-recreate api shadow-runner alert-worker backup backup-verify
   ```

   Drop `alert-worker` from the list if live alerts are off.
5. Verify:

   ```powershell
   docker compose --profile shadow --profile alerts --profile backup ps
   Invoke-RestMethod http://127.0.0.1:8000/health | ConvertTo-Json -Depth 5
   docker compose --profile backup logs --tail 20 backup
   python -m app.ops.capacity
   ```

   - `api`, `shadow-runner` and `postgres` are `healthy`; `/health` is not 503.
   - No `password authentication failed` lines in any service's logs.
   - `python -m app.ops.capacity` connects through the published port with a
     password (the socket shortcut does not apply there), so it proves the new
     value works from the PC. It reads `.env`, so run it with your project
     virtualenv active.

### Afterwards

- The `postgres` container still has the old `POSTGRES_PASSWORD` in its
  environment. Compose sees that its configuration changed and recreates it on
  your next plain `docker compose up -d`. That is harmless (the image ignores
  the variable once the volume exists) but briefly restarts the database, so do
  it at a quiet time.
- Backups (`data/backups/*.dump`) do not contain role passwords, so old dumps
  restore fine under the new password.
- The database port is published on all of the PC's network interfaces
  (`5432:5432`, not `127.0.0.1`), so this password is the only barrier on your
  network. Keep it long.

### If it goes wrong

Services log `password authentication failed`: `.env` and the database
disagree. Run the `\password` step again with the value that is in `.env`, then
the recreate command. Never run `docker compose down -v` to "fix" this: it
deletes the data volume.

## SLACK_WEBHOOK_URL, SLACK_TEST_WEBHOOK_URL, SLACK_OPS_WEBHOOK_URL

Each is a Slack incoming-webhook URL, and the URL is the secret. Anyone who has
it can post to that channel.

To create a replacement, open your Slack app's Incoming Webhooks page, add a
new webhook for the same channel, and copy the URL. Do not delete the old one
yet.

### SLACK_WEBHOOK_URL (live alerts)

- Used by `alert-worker`. Nothing else in Compose receives it.
- Rotate:
  1. Create the new webhook for the live alerts channel in Slack.
  2. Set `SLACK_WEBHOOK_URL=<new-webhook-url>` in `.env`.
  3. Recreate the worker:

     ```powershell
     docker compose --profile alerts up -d alert-worker
     ```

  4. Watch one cycle succeed (default every 15 minutes):

     ```powershell
     docker compose --profile alerts logs --tail 20 alert-worker
     ```

     Look for `Live alert cycle complete`. If the outbox is empty, the cycle
     reports `0 sent, 0 failed`; that confirms the worker starts with the new
     value but not that Slack accepts it. For a real delivery test, use the
     test webhook below.
  5. Delete the old webhook in Slack.
- Downtime: seconds. The outbox is durable. An alert being sent at the moment of
  recreation is reclaimed after 10 minutes.
- Do not delete the old webhook before step 3. A revoked webhook makes the
  current alert `permanent_failed` and stops the delivery run, and
  `permanent_failed` rows keep `/health` at `degraded`.

### SLACK_TEST_WEBHOOK_URL (test channel)

- Read only by commands you run on the PC: `python -m app.alerts.process_alert_outbox --test-channel`
  and `python -m app.alerts.send_test_alerts`. No container gets it.
- Rotate: create the new webhook, set `SLACK_TEST_WEBHOOK_URL=<new-test-webhook-url>`
  in `.env`, delete the old one. Nothing to recreate.
- It must differ from `SLACK_WEBHOOK_URL`; `send_test_alerts` refuses to run
  otherwise.
- Verify by sending one real event to the test channel (reads the database,
  writes nothing):

  ```powershell
  python -m app.alerts.send_test_alerts --limit 1
  ```

  `--dry-run` only prints and sends nothing, so it does not test the webhook.

### SLACK_OPS_WEBHOOK_URL (ops warnings)

- Used by `shadow-runner` and `alert-worker`, both to post operational
  warnings only. Without it, warnings are only logged.
- Rotate:
  1. Create the new webhook in Slack.
  2. Set `SLACK_OPS_WEBHOOK_URL=<new-ops-webhook-url>` in `.env`.
  3. Recreate the services that read it:

     ```powershell
     docker compose --profile shadow --profile alerts up -d shadow-runner alert-worker
     ```

     Leave out `alert-worker` if it is not running.
  4. Test from the PC and from inside a container (each sends one message to
     the ops channel):

     ```powershell
     python -m app.ops.notify --test
     docker compose --profile shadow exec shadow-runner python -m app.ops.notify --test
     ```

     Output is `Ops notification sent.` (exit code 0) or
     `Ops notification failed: ...`.
  5. Delete the old webhook in Slack.
- Downtime: none for alerts. A recreated `shadow-runner` interrupts a run in
  progress; check first, as in [deployment](deployment.md#updating-after-a-merged-pr).

## RENTCAST_API_KEY

- Used by `shadow-runner` (sent as the `X-Api-Key` header) and by the
  RentCast commands you run on the PC. Compose also requires the variable to be
  non-empty for **every** command, so `.env` must never lack it.
- Scheduled runs do not call RentCast unless
  `SHADOW_ENABLE_RENTCAST_API_CALLS=true`, so a bad key can go unnoticed until
  the next run that does.
- Rotate:
  1. In your RentCast account, create the new key. Check there whether the old
     key keeps working or is revoked immediately, and keep the old key until
     step 4 if you can.
  2. Set `RENTCAST_API_KEY=<new-rentcast-key>` in `.env`.
  3. Recreate the runner:

     ```powershell
     docker compose --profile shadow up -d shadow-runner
     ```

  4. Verify with the bounded read-only audit. It makes three requests (they use
     plan allowance, and they are **not** counted in the shadow runner's monthly
     budget) and prints only aggregate numbers:

     ```powershell
     python -m app.providers.audit_rentcast
     ```

     An invalid key fails with an HTTP 401/403 error; authentication errors are
     not retried.
  5. Revoke the old key.
- Downtime: none. If you have opted in to scheduled calls, check that the next
  run's `shadow_pipeline_runs` row succeeded (see
  [incident response](incident-response.md#rentcast-budget-or-provider-failure-warning)).

## SHADOW_HEARTBEAT_URL

- Used by `shadow-runner`, which pings it after each successful run. A failed
  ping is logged and does not fail the run.
- The URL is the secret: anyone with it can mark the check healthy.
- Rotate (the exact screens depend on your monitoring service; this describes a
  Healthchecks.io-style check):
  1. Create a new check with the same period and grace as the old one (3 days
     and 7 hours is the suggestion in `.env.example`). Copy its ping URL.
  2. Set `SHADOW_HEARTBEAT_URL=<new-ping-url>` in `.env`.
  3. Send a test ping, which also turns the new check green:

     ```powershell
     python -m app.ops.heartbeat --test
     ```

     Output is `Heartbeat sent.` (exit code 0) or `Heartbeat failed: ...`.
  4. Recreate the runner:

     ```powershell
     docker compose --profile shadow up -d shadow-runner
     ```

  5. Pause or delete the old check. Do this last, or it starts alerting
     because nothing pings it any more.
- Downtime: none. The test ping starts the new check's clock, and the next
  successful run pings it again.

## API_AUTH_TOKEN

- Checked by the API when `API_AUTH_REQUIRED=true`. Clients send
  `Authorization: Bearer <token>`. `/health` is exempt. There is one token and
  no overlap period, so clients using the old token get HTTP 401 the moment the
  API restarts.
- **Caveat:** `docker-compose.yml` does not pass `API_AUTH_REQUIRED`,
  `API_AUTH_TOKEN` or `API_RATE_LIMIT_PER_MINUTE` to the `api` service, and `.env`
  is not baked into the image. As shipped, the containerised API does not
  enforce a token, and changing this variable there has no effect. Run the
  check below before relying on it.
- Rotate (once the variables reach the API process):
  1. Generate a token (see the generator above).
  2. Set `API_AUTH_TOKEN=<new-token>` in `.env` (and `API_AUTH_REQUIRED=true`).
  3. Recreate the API:

     ```powershell
     docker compose up -d api
     ```

  4. Update every client that calls the API, then verify. Without a token the
     second call must return 401; with the token it must return 200:

     ```powershell
     Invoke-WebRequest http://127.0.0.1:8000/listings?limit=1 -SkipHttpErrorCheck | Select-Object StatusCode
     Invoke-WebRequest http://127.0.0.1:8000/listings?limit=1 -SkipHttpErrorCheck -Headers @{ Authorization = "Bearer <new-token>" } | Select-Object StatusCode
     ```

     If the first call returns 200, authentication is not enforced.
- Downtime: seconds for the API restart. With auth on, the browser dashboard
  (`/dashboard`) is also blocked, since a browser cannot add the header.

## General rules

- **Never commit `.env`.** It is in `.gitignore` and `.dockerignore`. Stage files
  by name, not with `git add .`. CI runs gitleaks over the full history.
- Keep `.env.example` to placeholders only.
- Do not paste `docker compose config` output, `docker inspect` output or
  `.env` into chats or tickets: they show real values. Use
  `docker compose config --quiet` to validate.
- The project avoids logging webhook, heartbeat and API-key URLs, but
  connection-error text (for example in `alert_outbox.last_error` or a worker
  log line) can still contain part of one. Read log and query output before you
  paste it anywhere.
- Store secrets in a password manager, not only in `.env`.

## If a secret leaked

1. **Rotate first, clean up second.** Follow the section above for the leaked
   secret. Rewriting Git history does not un-leak anything that was pushed.
2. Revoke the old value at its source (Slack, RentCast, your heartbeat service);
   for the database, `\password` as above.
3. Check for misuse: the Slack channel for messages you did not send; the
   RentCast dashboard for unexpected requests; the API request logs
   (`docker compose logs api`) for unknown `client` addresses.
4. If it was committed or pushed, rotate, then decide with the repo owner
   whether to rewrite history. If it reached a public repository, assume it was
   copied.
5. If you rotate the database password because of a leak, note that port 5432 is
   reachable from the local network; check who could have connected.
6. Add a line to your post-incident notes (see
   [incident response](incident-response.md#after-an-incident)).
