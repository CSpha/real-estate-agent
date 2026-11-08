import json

from sqlalchemy import Engine, text

from app.alerts.formatting import (
    address_link,
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


# Sources whose deals must also have a usable comparable valuation. Low-confidence
# or missing valuations are the flagged listings, so they never alert.
VALUATION_REQUIRED_SOURCES = ("rentcast",)


def get_potential_deal_events(engine: Engine | None = None):
    """Deals to alert: each listing once, and again only at a lower list price.

    A listing is re-scored on every run, so its scoring time is not a new event.
    """
    query = text(
        """
        WITH latest_valuation AS (
            SELECT DISTINCT ON (source, source_listing_id)
                source,
                source_listing_id,
                status,
                confidence_label
            FROM listing_comparable_valuations
            ORDER BY source, source_listing_id, as_of_date DESC, id DESC
        ),
        previous_deal_alerts AS (
            SELECT source, source_listing_id, payload_json
            FROM alerts_sent
            WHERE alert_type = 'potential_deal'
            UNION ALL
            SELECT source, source_listing_id, payload_json
            FROM alert_outbox
            WHERE alert_type = 'potential_deal'
        )
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
        LEFT JOIN latest_valuation valuation
          ON valuation.source = pd.source
         AND valuation.source_listing_id = pd.source_listing_id
        WHERE listing.alert_eligible
          AND listing.scoring_eligible
          AND listing.missing_from_feed_since IS NULL
          AND (
              pd.source <> ALL(:valuation_sources)
              OR (
                  valuation.status = 'valued'
                  AND valuation.confidence_label <> 'low'
              )
          )
          AND NOT EXISTS (
              SELECT 1
              FROM previous_deal_alerts previous
              WHERE previous.source = pd.source
                AND previous.source_listing_id = pd.source_listing_id
                AND (
                    previous.payload_json ->> 'list_price' IS NULL
                    OR (previous.payload_json ->> 'list_price')::numeric
                        <= pd.list_price
                )
          )
        ORDER BY pd.market_score DESC, pd.pct_below_or_above_median ASC;
        """
    )

    engine = engine or get_engine()

    with engine.connect() as conn:
        return (
            conn.execute(
                query, {"valuation_sources": list(VALUATION_REQUIRED_SOURCES)}
            )
            .mappings()
            .all()
        )


def format_currency(value):
    return money(value)


def format_percent(value):
    return percent(value, fraction=True)


def _vs_median(event):
    return (
        f"{format_currency(event['price_vs_county_median'])} "
        f"({format_percent(event['pct_below_or_above_median'])})"
    )


def format_slack_message(event):
    return {
        "text": (
            f"Potential deal: {full_address(event)}\n"
            f"County: {event['county_name']}\n"
            f"List price: {format_currency(event['list_price'])}\n"
            f"County median sale price: {format_currency(event['county_median_sale_price'])}\n"
            f"Difference from median: {format_currency(event['price_vs_county_median'])}\n"
            f"Percent vs median: {format_percent(event['pct_below_or_above_median'])}\n"
            f"Market score: {event['market_score']}\n"
            f"Reason: {event['score_reason']}\n"
            f"Listing ID: {event['source_listing_id']}"
        ),
        "blocks": [
            header_block(f":moneybag: Potential deal: {event['address']}"),
            section_block(f"*{address_link(event)}*"),
            fields_block(
                [
                    ("List price", format_currency(event["list_price"])),
                    (
                        f"{event['county_name']} County median",
                        format_currency(event["county_median_sale_price"]),
                    ),
                    ("vs. median", _vs_median(event)),
                    ("Market score", escape(event["market_score"])),
                ]
            ),
            section_block(f"_{escape(event['score_reason'])}_"),
            context_block(f"Listing ID {escape(event['source_listing_id'])}"),
        ],
    }


def summarize_event(event):
    """One digest line: (mrkdwn, plain text)."""
    pct = to_decimal(event["pct_below_or_above_median"])
    position = "below" if pct is not None and pct < 0 else "above"
    magnitude = format_percent(abs(pct)) if pct is not None else "N/A"
    detail = (
        f"{format_currency(event['list_price'])}, {magnitude} {position} the "
        f"county median, score {event['market_score']}"
    )
    return (
        f":moneybag: *{address_link(event)}*  {escape(detail)}",
        f"Potential deal: {full_address(event)}, {detail}",
    )


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
