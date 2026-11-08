from __future__ import annotations

import json
import math
import random
import time
from collections.abc import Callable, Mapping
from typing import Any

import requests
from sqlalchemy import Engine, text

from app.alerts.formatting import build_digest_message
from app.alerts.slack import SlackDeliveryError
from app.db import get_engine
from app.ops.logging import get_logger


log = get_logger(__name__)

MAX_ATTEMPTS = 5
CLAIM_TIMEOUT_MINUTES = 10
MAX_RETRY_DELAY_SECONDS = 3600
# Slack incoming webhooks allow roughly one message per second.
MIN_SEND_INTERVAL_SECONDS = 1.0
# More than DIGEST_THRESHOLD due alerts are sent as evenly sized digests of at
# most DIGEST_SIZE (see digest_sizes).
DIGEST_THRESHOLD = 3
DIGEST_SIZE = 10

COUNT_DUE_SQL = text(
    """
    SELECT COUNT(*)
    FROM alert_outbox
    WHERE status IN ('pending', 'retryable_failed')
      AND available_at <= CURRENT_TIMESTAMP
    """
)

DUE_ALERTS_SQL = text(
    """
    SELECT *
    FROM alert_outbox
    WHERE status IN ('pending', 'retryable_failed')
      AND available_at <= CURRENT_TIMESTAMP
    ORDER BY available_at, id
    LIMIT :limit
    """
)

CLAIM_NEXT_SQL = text(
    """
    WITH candidate AS (
        SELECT id
        FROM alert_outbox
        WHERE (
            status IN ('pending', 'retryable_failed')
            AND available_at <= CURRENT_TIMESTAMP
        ) OR (
            status = 'processing'
            AND claimed_at < CURRENT_TIMESTAMP
                - make_interval(mins => :claim_timeout_minutes)
        )
        ORDER BY available_at, id
        FOR UPDATE SKIP LOCKED
        LIMIT :batch_size
    )
    UPDATE alert_outbox outbox
    SET
        status = 'processing',
        claimed_at = CURRENT_TIMESTAMP,
        attempt_count = attempt_count + 1,
        last_error = NULL
    FROM candidate
    WHERE outbox.id = candidate.id
    RETURNING outbox.*
    """
)


RELEASE_SQL = text(
    """
    UPDATE alert_outbox
    SET
        status = 'pending',
        claimed_at = NULL,
        attempt_count = GREATEST(attempt_count - 1, 0)
    WHERE id = ANY(:alert_ids)
      AND status = 'processing'
    """
)


def claim_alerts(engine: Engine | None, batch_size: int) -> list[dict[str, Any]]:
    engine = engine or get_engine()
    with engine.begin() as conn:
        rows = conn.execute(
            CLAIM_NEXT_SQL,
            {
                "claim_timeout_minutes": CLAIM_TIMEOUT_MINUTES,
                "batch_size": batch_size,
            },
        ).mappings().all()
    return sorted((dict(row) for row in rows), key=lambda row: (row["available_at"], row["id"]))


def claim_next_alert(engine: Engine | None = None) -> dict[str, Any] | None:
    claimed = claim_alerts(engine, 1)
    return claimed[0] if claimed else None


def release_alerts(alerts: list[Mapping[str, Any]], engine: Engine | None = None) -> None:
    """Return claimed alerts to pending without counting the attempt."""
    if not alerts:
        return
    engine = engine or get_engine()
    with engine.begin() as conn:
        conn.execute(RELEASE_SQL, {"alert_ids": [alert["id"] for alert in alerts]})


def digest_sizes(due: int, limit: int | None = None) -> list[int]:
    """Split due alerts into the fewest digests of at most DIGEST_SIZE, evenly.

    11 due alerts become [6, 5], not [10, 1]. A limit caps how many alerts are
    sent, and the digest count follows the capped total, so this never plans
    more messages than ceil(min(due, limit) / DIGEST_SIZE).
    """
    total = due if limit is None else min(due, limit)
    if total <= 0:
        return []
    digests = math.ceil(total / DIGEST_SIZE)
    size, extra = divmod(total, digests)
    return [size + 1] * extra + [size] * (digests - extra)


def count_due_alerts(engine: Engine | None = None) -> int:
    engine = engine or get_engine()
    with engine.connect() as conn:
        return int(conn.execute(COUNT_DUE_SQL).scalar_one())


def mark_alert_sent(
    alert: Mapping[str, Any],
    engine: Engine | None = None,
) -> None:
    engine = engine or get_engine()
    payload_json = alert["payload_json"]
    if not isinstance(payload_json, str):
        payload_json = json.dumps(payload_json)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO alerts_sent (
                    alert_type,
                    source,
                    source_listing_id,
                    event_timestamp,
                    payload_json
                )
                VALUES (
                    :alert_type,
                    :source,
                    :source_listing_id,
                    :event_timestamp,
                    CAST(:payload_json AS JSONB)
                )
                ON CONFLICT (
                    alert_type,
                    source,
                    source_listing_id,
                    event_timestamp
                )
                DO NOTHING
                """
            ),
            {
                "alert_type": alert["alert_type"],
                "source": alert["source"],
                "source_listing_id": alert["source_listing_id"],
                "event_timestamp": alert["event_timestamp"],
                "payload_json": payload_json,
            },
        )
        conn.execute(
            text(
                """
                UPDATE alert_outbox
                SET
                    status = 'sent',
                    sent_at = CURRENT_TIMESTAMP,
                    claimed_at = NULL,
                    last_error = NULL
                WHERE id = :alert_id
                  AND status = 'processing'
                """
            ),
            {"alert_id": alert["id"]},
        )


def retry_delay_seconds(
    attempts: int,
    *,
    retry_after_seconds: float | None = None,
    jitter: Callable[[], float] = random.random,
) -> float:
    """Exponential backoff with equal jitter, never sooner than Retry-After."""
    base = min(60 * (2 ** max(attempts - 1, 0)), MAX_RETRY_DELAY_SECONDS)
    delay = base / 2 + jitter() * base / 2
    if retry_after_seconds is not None:
        delay = max(delay, retry_after_seconds)
    return round(delay, 3)


def mark_alert_failed(
    alert: Mapping[str, Any],
    error: Exception,
    *,
    retryable: bool,
    engine: Engine | None = None,
    retry_after_seconds: float | None = None,
    rate_limited: bool = False,
) -> None:
    engine = engine or get_engine()
    attempts = int(alert["attempt_count"])
    # Throttling is not the alert's fault, so it never exhausts the attempts.
    should_retry = retryable and (attempts < MAX_ATTEMPTS or rate_limited)
    status = "retryable_failed" if should_retry else "permanent_failed"
    delay_seconds = retry_delay_seconds(
        attempts, retry_after_seconds=retry_after_seconds
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE alert_outbox
                SET
                    status = :status,
                    available_at = CASE
                        WHEN :should_retry
                            THEN CURRENT_TIMESTAMP
                                + make_interval(secs => :delay_seconds)
                        ELSE available_at
                    END,
                    claimed_at = NULL,
                    last_error = :last_error
                WHERE id = :alert_id
                  AND status = 'processing'
                """
            ),
            {
                "alert_id": alert["id"],
                "status": status,
                "should_retry": should_retry,
                "delay_seconds": delay_seconds,
                "last_error": str(error)[:2000],
            },
        )
    # Log the error type only: Slack and requests messages can echo the webhook.
    log.warning(
        "Alert delivery failed.",
        extra={
            "alert_id": alert["id"],
            "event_type": alert.get("alert_type"),
            "outcome": status,
            "attempt": attempts,
            "error_type": type(error).__name__,
        },
    )


def is_retryable_slack_error(error: Exception) -> bool:
    if isinstance(error, SlackDeliveryError):
        return error.retryable
    if isinstance(error, requests.HTTPError) and error.response is not None:
        status_code = error.response.status_code
        return status_code == 429 or status_code >= 500
    return isinstance(error, requests.RequestException)


def _payload(alert: Mapping[str, Any]) -> dict[str, Any]:
    payload = alert["payload_json"]
    return json.loads(payload) if isinstance(payload, str) else payload


def preview_due_alerts(
    formatters: Mapping[str, Callable[[Mapping[str, Any]], dict[str, Any]]],
    *,
    engine: Engine | None = None,
    limit: int = 100,
) -> list[tuple[dict[str, Any], dict[str, Any] | None, str | None]]:
    """Format due alerts without claiming them or changing outbox state.

    Returns (alert, message or None, error or None) for each due row.
    """
    engine = engine or get_engine()
    with engine.connect() as conn:
        alerts = [
            dict(row)
            for row in conn.execute(DUE_ALERTS_SQL, {"limit": limit}).mappings()
        ]
    previews = []
    for alert in alerts:
        formatter = formatters.get(alert["alert_type"])
        if formatter is None:
            previews.append((alert, None, f"Unsupported alert type: {alert['alert_type']}"))
            continue
        try:
            previews.append((alert, formatter(_payload(alert)), None))
        except Exception as exc:
            previews.append((alert, None, f"{type(exc).__name__}: {exc}"))
    return previews


def deliver_pending_alerts(
    webhook_url: str,
    formatters: Mapping[str, Callable[[Mapping[str, Any]], dict[str, Any]]],
    *,
    send: Callable[[str, dict[str, Any]], None],
    engine: Engine | None = None,
    limit: int = 100,
    min_send_interval_seconds: float = MIN_SEND_INTERVAL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    summarizers: Mapping[str, Callable[[Mapping[str, Any]], tuple[str, str]]]
    | None = None,
) -> dict[str, int]:
    """Deliver due alerts, pacing sends and stopping when Slack says to.

    A rate limit leaves remaining alerts pending for a later run. A webhook-level
    failure (revoked, archived, or missing channel) marks only the current alert
    as permanently failed and stops, so the rest are not burned on a bad webhook.
    With summarizers, more than DIGEST_THRESHOLD due alerts go out as digests.
    """
    engine = engine or get_engine()
    due = count_due_alerts(engine) if summarizers is not None else 0
    if summarizers is not None and due > DIGEST_THRESHOLD:
        log.info("Several alerts are due; sending them as digests.", extra={"due": due})
        return _deliver_digests(
            webhook_url,
            summarizers,
            send=send,
            engine=engine,
            due=due,
            limit=limit,
            min_send_interval_seconds=min_send_interval_seconds,
            sleep=sleep,
            monotonic=monotonic,
        )
    counts = {"sent": 0, "failed": 0}
    last_send_at: float | None = None

    for _ in range(limit):
        alert = claim_next_alert(engine)
        if alert is None:
            break

        formatter = formatters.get(alert["alert_type"])
        if formatter is None:
            mark_alert_failed(
                alert,
                ValueError(f"Unsupported alert type: {alert['alert_type']}"),
                retryable=False,
                engine=engine,
            )
            counts["failed"] += 1
            continue

        try:
            message = formatter(_payload(alert))
        except Exception as exc:
            mark_alert_failed(alert, exc, retryable=False, engine=engine)
            counts["failed"] += 1
            continue

        if last_send_at is not None:
            wait = min_send_interval_seconds - (monotonic() - last_send_at)
            if wait > 0:
                sleep(wait)
        last_send_at = monotonic()

        try:
            send(webhook_url, message)
        except Exception as exc:
            mark_alert_failed(
                alert,
                exc,
                retryable=is_retryable_slack_error(exc),
                engine=engine,
                retry_after_seconds=getattr(exc, "retry_after_seconds", None),
                rate_limited=getattr(exc, "rate_limited", False),
            )
            counts["failed"] += 1
            if getattr(exc, "webhook_unusable", False):
                log.error(
                    "Stopping delivery: Slack webhook is unusable.",
                    extra={"error_type": type(exc).__name__},
                )
                break
            if getattr(exc, "rate_limited", False):
                log.warning(
                    "Stopping delivery: Slack rate limited this webhook.",
                    extra={"error_type": type(exc).__name__},
                )
                break
        else:
            mark_alert_sent(alert, engine)
            counts["sent"] += 1
            log.info(
                "Alert sent.",
                extra={
                    "alert_id": alert["id"],
                    "event_type": alert["alert_type"],
                    "outcome": "sent",
                },
            )

    return counts


def _deliver_digests(
    webhook_url: str,
    summarizers: Mapping[str, Callable[[Mapping[str, Any]], tuple[str, str]]],
    *,
    send: Callable[[str, dict[str, Any]], None],
    engine: Engine,
    due: int,
    limit: int,
    min_send_interval_seconds: float,
    sleep: Callable[[float], None],
    monotonic: Callable[[], float],
) -> dict[str, int]:
    """Send due alerts in evenly sized digests, one Slack message each.

    Sizes come from digest_sizes(due, limit). Alerts that become due mid-run
    wait for the next cycle, so a run never sends more digests than planned.
    Every alert in a digest shares its outcome. A webhook-level failure marks
    only the first alert permanently failed and returns the rest to pending.
    """
    counts = {"sent": 0, "failed": 0}
    last_send_at: float | None = None

    for size in digest_sizes(due, limit):
        batch = claim_alerts(engine, size)
        if not batch:
            break

        items: list[tuple[str, dict[str, Any]]] = []
        ready: list[dict[str, Any]] = []
        for alert in batch:
            summarizer = summarizers.get(alert["alert_type"])
            try:
                if summarizer is None:
                    raise ValueError(f"Unsupported alert type: {alert['alert_type']}")
                payload = _payload(alert)
                summarizer(payload)
            except Exception as exc:
                mark_alert_failed(alert, exc, retryable=False, engine=engine)
                counts["failed"] += 1
                continue
            items.append((alert["alert_type"], payload))
            ready.append(alert)
        if not ready:
            continue

        message = build_digest_message(items, summarizers)
        if last_send_at is not None:
            wait = min_send_interval_seconds - (monotonic() - last_send_at)
            if wait > 0:
                sleep(wait)
        last_send_at = monotonic()

        try:
            send(webhook_url, message)
        except Exception as exc:
            if getattr(exc, "webhook_unusable", False):
                mark_alert_failed(ready[0], exc, retryable=False, engine=engine)
                release_alerts(ready[1:], engine)
                counts["failed"] += 1
                log.error(
                    "Stopping delivery: Slack webhook is unusable.",
                    extra={"error_type": type(exc).__name__, "released": len(ready) - 1},
                )
                break
            for alert in ready:
                mark_alert_failed(
                    alert,
                    exc,
                    retryable=is_retryable_slack_error(exc),
                    engine=engine,
                    retry_after_seconds=getattr(exc, "retry_after_seconds", None),
                    rate_limited=getattr(exc, "rate_limited", False),
                )
            counts["failed"] += len(ready)
            outcome = "failed"
            if getattr(exc, "rate_limited", False):
                log.warning(
                    "Stopping delivery: Slack rate limited this webhook.",
                    extra={"error_type": type(exc).__name__},
                )
                break
        else:
            for alert in ready:
                mark_alert_sent(alert, engine)
            counts["sent"] += len(ready)
            outcome = "sent"
        log.info(
            f"Digest of {len(ready)} alert(s) processed.",
            extra={"outcome": outcome, "alert_ids": [alert["id"] for alert in ready]},
        )

    return counts
