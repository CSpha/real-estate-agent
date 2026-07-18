from __future__ import annotations

import argparse
from pathlib import Path

from app.alerts.send_price_drop_alerts import main as send_price_drop_alerts
from app.ingest.load_sample_listings import load_sample_csv
from app.transforms.snapshot_listings import snapshot_current_listings


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_LISTINGS_PATH = PROJECT_ROOT / "data" / "sample_listings.csv"


def run_pipeline(*, send_alerts: bool = True) -> None:
    print("Starting pipeline...")

    try:
        print("Step 1: Loading listings...")
        load_sample_csv(str(SAMPLE_LISTINGS_PATH))

        print("Step 2: Snapshotting current listings...")
        snapshot_current_listings()

        if send_alerts:
            print("Step 3: Sending price drop alerts...")
            send_price_drop_alerts()
        else:
            print("Step 3: Skipping Slack alerts (dry-run mode).")

        print("Pipeline completed successfully.")

    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the sample listing pipeline.")
    parser.add_argument(
        "--skip-alerts",
        action="store_true",
        help="Load and snapshot listings without sending Slack messages.",
    )
    args = parser.parse_args()
    run_pipeline(send_alerts=not args.skip_alerts)
