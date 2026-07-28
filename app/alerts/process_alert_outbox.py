from __future__ import annotations

import argparse

from sqlalchemy import Engine

from app.alerts.outbox import deliver_pending_alerts
from app.alerts.send_price_drop_alerts import (
    format_slack_message,
    send_to_slack,
)
from app.config import get_settings, require_setting


def process_alert_outbox(
    *,
    limit: int = 100,
    engine: Engine | None = None,
) -> dict[str, int]:
    webhook_url = require_setting(
        get_settings().slack_webhook_url,
        "SLACK_WEBHOOK_URL",
    )
    counts = deliver_pending_alerts(
        webhook_url,
        {"price_drop": format_slack_message},
        send=send_to_slack,
        engine=engine,
        limit=limit,
    )
    print(
        "Alert delivery complete: "
        f"{counts['sent']} sent, {counts['failed']} failed."
    )
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deliver due alerts from the PostgreSQL outbox.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        choices=range(1, 1001),
        metavar="1-1000",
        help="Maximum number of outbox rows to process (default: 100).",
    )
    args = parser.parse_args()
    process_alert_outbox(limit=args.limit)


if __name__ == "__main__":
    main()
