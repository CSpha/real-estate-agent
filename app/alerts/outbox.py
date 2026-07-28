from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

import requests
from sqlalchemy import Engine, text

from app.db import get_engine


MAX_ATTEMPTS = 5
CLAIM_TIMEOUT_MINUTES = 10

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
        LIMIT 1
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


def claim_next_alert(engine: Engine | None = None) -> dict[str, Any] | None:
    engine = engine or get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            CLAIM_NEXT_SQL,
            {"claim_timeout_minutes": CLAIM_TIMEOUT_MINUTES},
        ).mappings().first()
    return dict(row) if row is not None else None


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


def mark_alert_failed(
    alert: Mapping[str, Any],
    error: Exception,
    *,
    retryable: bool,
    engine: Engine | None = None,
) -> None:
    engine = engine or get_engine()
    attempts = int(alert["attempt_count"])
    should_retry = retryable and attempts < MAX_ATTEMPTS
    status = "retryable_failed" if should_retry else "permanent_failed"
    delay_seconds = min(60 * (2 ** max(attempts - 1, 0)), 3600)

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


def is_retryable_slack_error(error: Exception) -> bool:
    if isinstance(error, requests.HTTPError) and error.response is not None:
        status_code = error.response.status_code
        return status_code == 429 or status_code >= 500
    return isinstance(error, requests.RequestException)


def deliver_pending_alerts(
    webhook_url: str,
    formatters: Mapping[str, Callable[[Mapping[str, Any]], dict[str, Any]]],
    *,
    send: Callable[[str, dict[str, Any]], None],
    engine: Engine | None = None,
    limit: int = 100,
) -> dict[str, int]:
    engine = engine or get_engine()
    counts = {"sent": 0, "failed": 0}

    for _ in range(limit):
        alert = claim_next_alert(engine)
        if alert is None:
            break

        payload = alert["payload_json"]
        if isinstance(payload, str):
            payload = json.loads(payload)
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
            send(webhook_url, formatter(payload))
        except Exception as exc:
            mark_alert_failed(
                alert,
                exc,
                retryable=is_retryable_slack_error(exc),
                engine=engine,
            )
            counts["failed"] += 1
        else:
            mark_alert_sent(alert, engine)
            counts["sent"] += 1

    return counts
