"""Send real listing events to the Slack test channel, formatted as alerts.

Shadow listings are not alert-eligible, so the normal alert queries return
nothing for them. This command selects recent RentCast price drops and
potential deals regardless of eligibility, formats them with the production
formatters, and posts them only to SLACK_TEST_WEBHOOK_URL. It never reads the
outbox or alerts_sent, never writes to the database, and refuses to run if the
test webhook is the production webhook.
"""
from __future__ import annotations

import argparse
import json
import time
from typing import Any, Callable

from sqlalchemy import Engine, text

from app.alerts import send_potential_deal_alerts, send_price_drop_alerts
from app.alerts.formatting import build_digest_message, context_block
from app.alerts.outbox import MIN_SEND_INTERVAL_SECONDS
from app.alerts.process_alert_outbox import SUMMARIZERS
from app.alerts.slack import send_to_slack
from app.config import get_settings, require_setting
from app.db import get_engine


MAX_PER_TYPE = 10
TEST_BANNER = ":test_tube: TEST MESSAGE - not a live alert (shadow data)\n"

PRICE_DROP_SAMPLE_SQL = text(
    """
    WITH ranked AS (
        SELECT
            source,
            source_listing_id,
            address,
            city,
            state,
            list_price,
            snapshot_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY source, source_listing_id
                ORDER BY snapshot_timestamp DESC, id DESC
            ) AS rn
        FROM listing_history
        WHERE source = :source
    )
    SELECT
        cur.source,
        cur.source_listing_id,
        cur.address,
        cur.city,
        cur.state,
        prev.list_price AS previous_price,
        cur.list_price AS current_price,
        (cur.list_price - prev.list_price) AS price_change,
        ROUND(
            ((cur.list_price - prev.list_price) / NULLIF(prev.list_price, 0)) * 100,
            2
        ) AS price_change_pct,
        cur.snapshot_timestamp AS event_timestamp
    FROM ranked cur
    JOIN ranked prev
      ON prev.source = cur.source
     AND prev.source_listing_id = cur.source_listing_id
     AND prev.rn = 2
    WHERE cur.rn = 1
      AND cur.list_price < prev.list_price
    ORDER BY cur.snapshot_timestamp DESC, price_change ASC
    LIMIT :limit
    """
)
POTENTIAL_DEAL_SAMPLE_SQL = text(
    """
    SELECT
        source,
        source_listing_id,
        address,
        city,
        state,
        county_name,
        list_price,
        county_median_sale_price,
        price_vs_county_median,
        pct_below_or_above_median,
        market_score,
        score_reason,
        scored_at AS event_timestamp
    FROM potential_deals
    WHERE source = :source
    ORDER BY market_score DESC, pct_below_or_above_median ASC
    LIMIT :limit
    """
)

SAMPLES = (
    ("price_drop", PRICE_DROP_SAMPLE_SQL, send_price_drop_alerts.format_slack_message),
    (
        "potential_deal",
        POTENTIAL_DEAL_SAMPLE_SQL,
        send_potential_deal_alerts.format_slack_message,
    ),
)


def resolve_test_webhook_url() -> str:
    settings = get_settings()
    url = require_setting(settings.slack_test_webhook_url, "SLACK_TEST_WEBHOOK_URL")
    production = (settings.slack_webhook_url or "").strip()
    if production and url.strip() == production:
        raise ValueError(
            "SLACK_TEST_WEBHOOK_URL is the same as SLACK_WEBHOOK_URL; "
            "use a separate test channel."
        )
    return url


def build_test_messages(
    *,
    source: str = "rentcast",
    limit: int = 3,
    digest: bool = False,
    engine: Engine | None = None,
) -> list[tuple[str, str, dict[str, Any]]]:
    engine = engine or get_engine()
    messages = []
    with engine.connect() as conn:
        for alert_type, query, formatter in SAMPLES:
            events = conn.execute(
                query, {"source": source, "limit": limit}
            ).mappings().all()
            print(f"Found {len(events)} {alert_type} event(s) for {source}.")
            for event in events:
                messages.append(
                    (alert_type, str(event["source_listing_id"]), formatter(event), event)
                )
    if digest and messages:
        message = build_digest_message(
            [(alert_type, event) for alert_type, _id, _message, event in messages],
            SUMMARIZERS,
        )
        messages = [("digest", f"{len(messages)} events", message, None)]
    return [
        (alert_type, label, _mark_as_test(message))
        for alert_type, label, message, _event in messages
    ]


def _mark_as_test(message: dict[str, Any]) -> dict[str, Any]:
    marked = {**message, "text": TEST_BANNER + message["text"]}
    if "blocks" in message:
        marked["blocks"] = [context_block(TEST_BANNER.strip()), *message["blocks"]]
    return marked


def send_test_alerts(
    *,
    source: str = "rentcast",
    limit: int = 3,
    dry_run: bool = False,
    digest: bool = False,
    engine: Engine | None = None,
    send: Callable[[str, dict[str, Any]], None] = send_to_slack,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    if not 1 <= limit <= MAX_PER_TYPE:
        raise ValueError(f"limit must be between 1 and {MAX_PER_TYPE}")
    url = None if dry_run else resolve_test_webhook_url()
    sent = 0
    for index, (alert_type, listing_id, message) in enumerate(
        build_test_messages(source=source, limit=limit, digest=digest, engine=engine)
    ):
        label = f"{alert_type} {listing_id}"
        if url is None:
            print(f"PREVIEW {label}\n{json.dumps(message, indent=2)}")
            continue
        if index:
            sleep(MIN_SEND_INTERVAL_SECONDS)
        send(url, message)
        sent += 1
        print(f"TEST SENT {label}")
    print(
        f"Test alerts complete: {sent} sent to the test channel. "
        "Database unchanged."
    )
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Send real listing events, formatted as alerts, to "
            "SLACK_TEST_WEBHOOK_URL only."
        )
    )
    parser.add_argument("--source", default="rentcast")
    parser.add_argument(
        "--limit",
        type=int,
        default=3,
        choices=range(1, MAX_PER_TYPE + 1),
        metavar=f"1-{MAX_PER_TYPE}",
        help="Maximum events per alert type (default: 3).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the messages without sending them.",
    )
    parser.add_argument(
        "--digest",
        action="store_true",
        help="Combine the events into one digest message, as busy runs do.",
    )
    args = parser.parse_args()
    send_test_alerts(
        source=args.source,
        limit=args.limit,
        dry_run=args.dry_run,
        digest=args.digest,
    )


if __name__ == "__main__":
    main()
