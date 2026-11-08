"""Queue and send live listing alerts after successful shadow runs.

This runs as its own container (compose profile `alerts`) so the shadow runner
never holds SLACK_WEBHOOK_URL. Each cycle is idempotent: queueing skips events
already queued or sent, and delivery only sends due outbox rows.
"""
from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import Engine, text

from app.alerts.process_alert_outbox import process_alert_outbox
from app.alerts.send_potential_deal_alerts import enqueue_potential_deal_events
from app.alerts.send_price_drop_alerts import enqueue_price_drop_events
from app.config import get_settings, require_setting
from app.db import get_engine
from app.ops.logging import get_logger, log_context
from app.ops.notify import OpsNotifier


log = get_logger(__name__)

DEFAULT_INTERVAL_MINUTES = 15
NOTIFIER_STATE_PATH = Path("data/alert_worker_notification_state.json")
LATEST_RUN_SQL = text(
    """
    SELECT id, status, finished_at
    FROM shadow_pipeline_runs
    WHERE source = :source
    ORDER BY started_at DESC, id DESC
    LIMIT 1
    """
)


def latest_shadow_run(engine: Engine, source: str = "rentcast") -> dict[str, Any] | None:
    with engine.connect() as connection:
        row = connection.execute(LATEST_RUN_SQL, {"source": source}).mappings().first()
    return dict(row) if row is not None else None


def run_live_alert_cycle(
    *,
    engine: Engine | None = None,
    deliver: Callable[..., dict[str, int]] = process_alert_outbox,
) -> dict[str, Any]:
    """Queue new events after a successful shadow run, then deliver due alerts.

    Queueing waits while a run is in progress or after a failed run, so alerts
    never come from half-refreshed data. Already-queued alerts are still sent.
    """
    engine = engine or get_engine()
    result: dict[str, Any] = {
        "run_id": None,
        "queued_price_drops": 0,
        "queued_potential_deals": 0,
        "sent": 0,
        "failed": 0,
    }
    with log_context(component="alert_worker"):
        run = latest_shadow_run(engine)
        if run is None:
            log.info("Step 1/2: No shadow run yet; nothing to queue.")
        elif run["status"] != "succeeded":
            log.info(
                "Step 1/2: Latest shadow run %s is %s; not queueing new alerts.",
                run["id"],
                run["status"],
            )
        else:
            result["run_id"] = run["id"]
            result["queued_price_drops"] = enqueue_price_drop_events(engine)
            result["queued_potential_deals"] = enqueue_potential_deal_events(engine)
            with log_context(run_id=run["id"]):
                log.info(
                    "Step 1/2: After shadow run %s, queued %s price drop(s) and "
                    "%s potential deal(s).",
                    run["id"],
                    result["queued_price_drops"],
                    result["queued_potential_deals"],
                    extra={
                        "queued_price_drops": result["queued_price_drops"],
                        "queued_potential_deals": result["queued_potential_deals"],
                    },
                )
        log.info("Step 2/2: Delivering due alerts to SLACK_WEBHOOK_URL.")
        counts = deliver(engine=engine)
        result["sent"] = counts.get("sent", 0)
        result["failed"] = counts.get("failed", 0)
    return result


def interval_minutes() -> int:
    raw = os.getenv("ALERT_WORKER_INTERVAL_MINUTES", str(DEFAULT_INTERVAL_MINUTES))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("ALERT_WORKER_INTERVAL_MINUTES must be an integer") from exc
    if value < 1:
        raise ValueError("ALERT_WORKER_INTERVAL_MINUTES must be at least 1")
    return value


def require_live_alerts() -> None:
    settings = get_settings()
    if not settings.live_alerts_enabled:
        raise SystemExit(
            "Live alerts are off: set LIVE_ALERTS_ENABLED=true to send to "
            "#listing-alerts."
        )
    require_setting(settings.slack_webhook_url, "SLACK_WEBHOOK_URL")


def run_forever(
    *,
    interval_seconds: int,
    cycle: Callable[[], dict[str, Any]] = run_live_alert_cycle,
    notifier: OpsNotifier | None = None,
    sleep: Callable[[float], None] = time.sleep,
    max_cycles: int | None = None,
) -> None:
    """Run cycles forever; failures warn the ops channel once and retry."""
    cycles = 0
    while max_cycles is None or cycles < max_cycles:
        cycles += 1
        with log_context(component="alert_worker", cycle=cycles):
            log.info("Live alert cycle at %s", datetime.now(timezone.utc).isoformat())
            try:
                result = cycle()
            except Exception as exc:
                log.error("Live alert cycle failed: %s: %s", type(exc).__name__, exc)
                if notifier is not None:
                    notifier.problem("cycle failed", f"{type(exc).__name__}: {exc}")
            else:
                log.info(
                    "Live alert cycle complete: %s",
                    result,
                    extra={"sent": result.get("sent"), "failed": result.get("failed")},
                )
                if notifier is not None:
                    if result["failed"]:
                        notifier.problem(
                            "delivery failed",
                            f"{result['failed']} alert(s) could not be delivered; "
                            "check the alert_outbox last_error column.",
                        )
                    else:
                        notifier.resolved("Alert delivery is working again.")
        if max_cycles is None or cycles < max_cycles:
            sleep(interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Queue and send live listing alerts after successful shadow runs.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one queue-and-deliver cycle, then exit.",
    )
    args = parser.parse_args()
    require_live_alerts()
    if args.once:
        result = run_live_alert_cycle()
        with log_context(component="alert_worker"):
            log.info("Live alert cycle complete: %s", result)
        return
    minutes = interval_minutes()
    with log_context(component="alert_worker"):
        log.info("Live alert worker started; checking every %s minute(s).", minutes)
    run_forever(
        interval_seconds=minutes * 60,
        notifier=OpsNotifier(state_path=NOTIFIER_STATE_PATH, label="Live alert worker"),
    )


if __name__ == "__main__":
    main()
