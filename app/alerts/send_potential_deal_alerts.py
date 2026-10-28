import json
from decimal import Decimal

from sqlalchemy import Engine, text

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
        JOIN listings_current listing
          ON listing.source = pd.source
         AND listing.source_listing_id = pd.source_listing_id
        LEFT JOIN alerts_sent a
            ON a.alert_type = 'potential_deal'
           AND a.source = pd.source
           AND a.source_listing_id = pd.source_listing_id
           AND a.event_timestamp = pd.scored_at
        LEFT JOIN alert_outbox outbox
            ON outbox.alert_type = 'potential_deal'
           AND outbox.source = pd.source
           AND outbox.source_listing_id = pd.source_listing_id
           AND outbox.event_timestamp = pd.scored_at
        WHERE listing.alert_eligible
          AND a.id IS NULL
          AND outbox.id IS NULL
        ORDER BY pd.market_score DESC, pd.pct_below_or_above_median ASC;
        """
    )

    engine = engine or get_engine()

    with engine.connect() as conn:
        return conn.execute(query).mappings().all()


def _decimal(value):
    # Outbox payloads store numbers as strings; direct events use Decimal.
    return None if value is None or value == "None" else Decimal(str(value))


def format_currency(value):
    value = _decimal(value)
    if value is None:
        return "N/A"

    return f"${value:,.0f}"


def format_percent(value):
    value = _decimal(value)
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


def _optional_str(value):
    return None if value is None else str(value)


def enqueue_potential_deal_events(engine: Engine | None = None) -> int:
    engine = engine or get_engine()
    events = get_potential_deal_events(engine)
    if not events:
        return 0

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
            'potential_deal',
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
                    "county_name": event["county_name"],
                    "list_price": _optional_str(event["list_price"]),
                    "county_median_sale_price": _optional_str(
                        event["county_median_sale_price"]
                    ),
                    "price_vs_county_median": _optional_str(
                        event["price_vs_county_median"]
                    ),
                    "pct_below_or_above_median": _optional_str(
                        event["pct_below_or_above_median"]
                    ),
                    "market_score": _optional_str(event["market_score"]),
                    "score_reason": event["score_reason"],
                    "event_timestamp": event["event_timestamp"].isoformat(),
                }
            ),
        }
        for event in events
    ]
    with engine.begin() as conn:
        result = conn.execute(insert_sql, values)
    return max(result.rowcount, 0)


def main():
    queued = enqueue_potential_deal_events()
    print(f"Queued {queued} new potential deal alert(s).")
    print("Run python -m app.alerts.process_alert_outbox to deliver queued alerts.")


if __name__ == "__main__":
    main()
