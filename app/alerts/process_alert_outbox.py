from __future__ import annotations

import argparse
import json
import time

from sqlalchemy import Engine

from app.alerts import send_potential_deal_alerts, send_price_drop_alerts
from app.alerts.outbox import (
    MIN_SEND_INTERVAL_SECONDS,
    deliver_pending_alerts,
    preview_due_alerts,
)
from app.alerts.slack import send_to_slack
from app.config import get_settings, require_setting


FORMATTERS = {
    "price_drop": send_price_drop_alerts.format_slack_message,
    "potential_deal": send_potential_deal_alerts.format_slack_message,
}


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
        FORMATTERS,
        send=send_to_slack,
        engine=engine,
        limit=limit,
    )
    print(
        "Alert delivery complete: "
        f"{counts['sent']} sent, {counts['failed']} failed."
    )
    return counts


def preview_alert_outbox(
    *,
    limit: int = 100,
    test_channel: bool = False,
    engine: Engine | None = None,
    send=send_to_slack,
    sleep=time.sleep,
) -> dict[str, int]:
    """Show or test-send due alerts without changing outbox state.

    With test_channel, messages go only to SLACK_TEST_WEBHOOK_URL; the production
    webhook is never read, and the alerts remain pending for real delivery.
    """
    test_webhook_url = None
    if test_channel:
        test_webhook_url = require_setting(
            get_settings().slack_test_webhook_url,
            "SLACK_TEST_WEBHOOK_URL",
        )
    counts = {"previewed": 0, "invalid": 0, "test_sent": 0}
    for index, (alert, message, error) in enumerate(
        preview_due_alerts(FORMATTERS, engine=engine, limit=limit)
    ):
        label = f"#{alert['id']} {alert['alert_type']} {alert['source_listing_id']}"
        if error is not None:
            print(f"INVALID {label}: {error}")
            counts["invalid"] += 1
            continue
        counts["previewed"] += 1
        if test_webhook_url is None:
            print(f"PREVIEW {label}\n{json.dumps(message, indent=2)}")
            continue
        if index:
            sleep(MIN_SEND_INTERVAL_SECONDS)
        send(test_webhook_url, message)
        counts["test_sent"] += 1
        print(f"TEST SENT {label}")
    print(
        "Alert preview complete: "
        f"{counts['previewed']} valid, {counts['invalid']} invalid, "
        f"{counts['test_sent']} sent to the test channel. Outbox unchanged."
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
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Print formatted due alerts without sending or changing the outbox.",
    )
    mode.add_argument(
        "--test-channel",
        action="store_true",
        help=(
            "Send due alerts to SLACK_TEST_WEBHOOK_URL only, leaving them "
            "pending for production delivery."
        ),
    )
    args = parser.parse_args()
    if args.dry_run or args.test_channel:
        preview_alert_outbox(limit=args.limit, test_channel=args.test_channel)
    else:
        process_alert_outbox(limit=args.limit)


if __name__ == "__main__":
    main()
