import json
import os

import requests
from dotenv import load_dotenv
from sqlalchemy import text

from app.utils.db import get_engine


def get_price_drop_events():
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
                snapshot_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY source, source_listing_id
                    ORDER BY snapshot_timestamp DESC
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
        WHERE l.current_price < p.previous_price
          AND a.id IS NULL
        ORDER BY price_change ASC, l.source_listing_id;
        """
    )

    engine = get_engine()

    with engine.connect() as conn:
        return conn.execute(query).mappings().all()


def format_slack_message(event):
    return {
        "text": (
            "Price drop detected\n"
            f"{event['address']}, {event['city']}, {event['state']}\n"
            f"Previous price: ${event['previous_price']}\n"
            f"Current price: ${event['current_price']}\n"
            f"Change: ${event['price_change']} ({event['price_change_pct']}%)\n"
            f"Listing ID: {event['source_listing_id']}"
        )
    }


def send_to_slack(webhook_url: str, message_payload: dict):
    response = requests.post(webhook_url, json=message_payload, timeout=30)
    response.raise_for_status()


def record_alert(event):
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
            :payload_json
        )
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

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            insert_sql,
            {
                "alert_type": "price_drop",
                "source": event["source"],
                "source_listing_id": event["source_listing_id"],
                "event_timestamp": event["event_timestamp"],
                "payload_json": payload_json,
            },
        )


def main():
    load_dotenv()
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        raise ValueError("SLACK_WEBHOOK_URL is not set in .env")

    events = get_price_drop_events()

    if not events:
        print("No new price drop alerts to send.")
        return

    for event in events:
        message_payload = format_slack_message(event)
        send_to_slack(webhook_url, message_payload)
        record_alert(event)

        print(
            f"Sent alert for listing {event['source_listing_id']} "
            f"at event time {event['event_timestamp']}"
        )

    print(f"Finished sending {len(events)} alert(s).")


if __name__ == "__main__":
    main()