import json

import requests
from sqlalchemy import Engine, text

from app.config import get_settings, require_setting
from app.db import get_engine


def get_potential_deal_events(engine: Engine | None = None):
    query = text(
        """
        SELECT
            pd.source,
            pd.source_listing_id,
            pd.address,
            pd.city,
            pd.state,
            pd.county_name,
            pd.list_price,
            pd.county_median_sale_price,
            pd.price_vs_county_median,
            pd.pct_below_or_above_median,
            pd.market_score,
            pd.score_reason,
            pd.scored_at AS event_timestamp
        FROM potential_deals pd
        LEFT JOIN alerts_sent a
            ON a.alert_type = 'potential_deal'
           AND a.source = pd.source
           AND a.source_listing_id = pd.source_listing_id
           AND a.event_timestamp = pd.scored_at
        WHERE a.id IS NULL
        ORDER BY pd.market_score DESC, pd.pct_below_or_above_median ASC;
        """
    )

    engine = engine or get_engine()

    with engine.connect() as conn:
        return conn.execute(query).mappings().all()


def format_currency(value):
    if value is None:
        return "N/A"

    return f"${value:,.0f}"


def format_percent(value):
    if value is None:
        return "N/A"

    return f"{value * 100:.2f}%"


def format_slack_message(event):
    return {
        "text": (
            "Potential deal detected\n"
            f"{event['address']}, {event['city']}, {event['state']}\n"
            f"County: {event['county_name']}\n"
            f"List price: {format_currency(event['list_price'])}\n"
            f"County median sale price: {format_currency(event['county_median_sale_price'])}\n"
            f"Difference from median: {format_currency(event['price_vs_county_median'])}\n"
            f"Percent vs median: {format_percent(event['pct_below_or_above_median'])}\n"
            f"Market score: {event['market_score']}\n"
            f"Reason: {event['score_reason']}\n"
            f"Listing ID: {event['source_listing_id']}"
        )
    }


def send_to_slack(webhook_url: str, message_payload: dict):
    response = requests.post(webhook_url, json=message_payload, timeout=30)
    response.raise_for_status()


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
            "county_name": event["county_name"],
            "list_price": str(event["list_price"]),
            "county_median_sale_price": str(event["county_median_sale_price"]),
            "price_vs_county_median": str(event["price_vs_county_median"]),
            "pct_below_or_above_median": str(event["pct_below_or_above_median"]),
            "market_score": event["market_score"],
            "score_reason": event["score_reason"],
        }
    )

    engine = engine or get_engine()

    with engine.begin() as conn:
        result = conn.execute(
            insert_sql,
            {
                "alert_type": "potential_deal",
                "source": event["source"],
                "source_listing_id": event["source_listing_id"],
                "event_timestamp": event["event_timestamp"],
                "payload_json": payload_json,
            },
        )
    return result.rowcount == 1


def main():
    webhook_url = require_setting(
        get_settings().slack_webhook_url,
        "SLACK_WEBHOOK_URL",
    )

    events = get_potential_deal_events()

    if not events:
        print("No new potential deal alerts to send.")
        return

    for event in events:
        message_payload = format_slack_message(event)
        send_to_slack(webhook_url, message_payload)
        record_alert(event)

        print(
            f"Sent potential deal alert for listing {event['source_listing_id']} "
            f"at event time {event['event_timestamp']}"
        )

    print(f"Finished sending {len(events)} potential deal alert(s).")


if __name__ == "__main__":
    main()
