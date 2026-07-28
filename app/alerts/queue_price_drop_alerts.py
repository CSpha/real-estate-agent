from __future__ import annotations

from sqlalchemy import Engine

from app.alerts.send_price_drop_alerts import enqueue_price_drop_events


def queue_price_drop_alerts(engine: Engine | None = None) -> int:
    queued = enqueue_price_drop_events(engine)
    print(f"Queued {queued} new price drop alert(s).")
    return queued


def main() -> None:
    queue_price_drop_alerts()


if __name__ == "__main__":
    main()
