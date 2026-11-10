import json

from sqlalchemy import Engine, text

from app.alerts.affordability import attach_affordability
from app.alerts.formatting import (
    address_link,
    affordability_block,
    affordability_brief,
    affordability_lines,
    context_block,
    escape,
    fields_block,
    full_address,
    header_block,
    money,
    percent,
    section_block,
    to_decimal,
)
from app.db import get_engine


def get_price_drop_events(engine: Engine | None = None):
    query = text(
        """
        WITH ranked_history AS (
            SELECT
                source,
                source_listing_id,
                address,
                city,
                state,
                list_price,
                status,
                alert_eligible,
                snapshot_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY source, source_listing_id
                    ORDER BY snapshot_timestamp DESC, id DESC
                ) AS rn
            FROM listing_history
        ),
        latest AS (
            SELECT
                source,
                source_listing_id,
                address,
                city,
                state,
                list_price AS current_price,
                status AS current_status,
                alert_eligible AS current_alert_eligible,
                snapshot_timestamp AS current_snapshot
            FROM ranked_history
            WHERE rn = 1
        ),
        previous AS (
            SELECT
                source,
                source_listing_id,
                list_price AS previous_price,
                status AS previous_status,
                alert_eligible AS previous_alert_eligible,
                snapshot_timestamp AS previous_snapshot
            FROM ranked_history
            WHERE rn = 2
        )
        SELECT
            l.source,
            l.source_listing_id,
            l.address,
            l.city,
            l.state,
            p.previous_price,
            l.current_price,
            (l.current_price - p.previous_price) AS price_change,
            ROUND(
                ((l.current_price - p.previous_price) / NULLIF(p.previous_price, 0)) * 100,
                2
            ) AS price_change_pct,
            l.current_snapshot AS event_timestamp
        FROM latest l
        JOIN previous p
            ON l.source = p.source
           AND l.source_listing_id = p.source_listing_id
        LEFT JOIN alerts_sent a
            ON a.alert_type = 'price_drop'
           AND a.source = l.source
           AND a.source_listing_id = l.source_listing_id
           AND a.event_timestamp = l.current_snapshot
        LEFT JOIN alert_outbox outbox
            ON outbox.alert_type = 'price_drop'
           AND outbox.source = l.source
           AND outbox.source_listing_id = l.source_listing_id
           AND outbox.event_timestamp = l.current_snapshot
        WHERE l.current_price < p.previous_price
          AND l.current_alert_eligible
          AND p.previous_alert_eligible
          AND a.id IS NULL
          AND outbox.id IS NULL
        ORDER BY price_change ASC, l.source_listing_id;
        """
    )

    engine = engine or get_engine()

    with engine.connect() as conn:
        return conn.execute(query).mappings().all()


def format_slack_message(event):
    change = f"{money(event['price_change'])} ({percent(event['price_change_pct'])})"
    affordability = affordability_block(event)
    return {
        "text": "\n".join(
            [
                (
                    f"Price drop: {full_address(event)}\n"
                    f"Previous price: {money(event['previous_price'])}\n"
                    f"Current price: {money(event['current_price'])}\n"
                    f"Change: {change}\n"
                    f"Listing ID: {event['source_listing_id']}"
                ),
                *affordability_lines(event),
            ]
        ),
        "blocks": [
            header_block(f":chart_with_downwards_trend: Price drop: {event['address']}"),
            section_block(f"*{address_link(event)}*"),
            fields_block(
                [
                    ("Now", money(event["current_price"])),
                    ("Was", money(event["previous_price"])),
                    ("Change", change),
                ]
            ),
            *([affordability] if affordability else []),
            context_block(f"Listing ID {escape(event['source_listing_id'])}"),
        ],
    }


def summarize_event(event):
    """One digest line: (mrkdwn, plain text)."""
    detail = (
        f"{money(event['current_price'])}, down "
        f"{money(abs(to_decimal(event['price_change']) or 0))} "
        f"({percent(event['price_change_pct'])}) from {money(event['previous_price'])}"
        f"{affordability_brief(event)}"
    )
    return (
        f":chart_with_downwards_trend: *{address_link(event)}*  {detail}",
        f"Price drop: {full_address(event)}, {detail}",
    )


def enqueue_price_drop_events(engine: Engine | None = None) -> int:
    engine = engine or get_engine()
    events = get_price_drop_events(engine)
    if not events:
        return 0
    events = attach_affordability(events, "current_price", engine)

    insert_sql = text(
        """
        INSERT INTO alert_outbox (
            alert_type,
            source,
            source_listing_id,
            event_timestamp,
            payload_json
        )
        VALUES (
            'price_drop',
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
    )
    values = [
        {
            "source": event["source"],
            "source_listing_id": event["source_listing_id"],
            "event_timestamp": event["event_timestamp"],
            "payload_json": json.dumps(
                {
                    "source": event["source"],
                    "source_listing_id": event["source_listing_id"],
                    "address": event["address"],
                    "city": event["city"],
                    "state": event["state"],
                    "previous_price": str(event["previous_price"]),
                    "current_price": str(event["current_price"]),
                    "price_change": str(event["price_change"]),
                    "price_change_pct": str(event["price_change_pct"]),
                    "affordability": event["affordability"],
                    "event_timestamp": event["event_timestamp"].isoformat(),
                }
            ),
        }
        for event in events
    ]
    with engine.begin() as conn:
        result = conn.execute(insert_sql, values)
    return max(result.rowcount, 0)


def record_alert(event, engine: Engine | None = None) -> bool:
    insert_sql = text(
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
    )

    payload_json = json.dumps(
        {
            "address": event["address"],
            "city": event["city"],
            "state": event["state"],
            "previous_price": str(event["previous_price"]),
            "current_price": str(event["current_price"]),
            "price_change": str(event["price_change"]),
            "price_change_pct": str(event["price_change_pct"]),
        }
    )

    engine = engine or get_engine()

    with engine.begin() as conn:
        result = conn.execute(
            insert_sql,
            {
                "alert_type": "price_drop",
                "source": event["source"],
                "source_listing_id": event["source_listing_id"],
                "event_timestamp": event["event_timestamp"],
                "payload_json": payload_json,
            },
        )
    return result.rowcount == 1
