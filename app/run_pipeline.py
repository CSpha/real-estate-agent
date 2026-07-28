from __future__ import annotations

import argparse
from pathlib import Path

from app.alerts.queue_price_drop_alerts import queue_price_drop_alerts
from app.ingest.load_sample_listings import load_sample_csv
from app.searches.service import evaluate_enabled_searches
from app.transforms.score_listings_against_market import (
    score_listings_against_market,
)
from app.transforms.snapshot_listings import snapshot_current_listings


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_LISTINGS_PATH = PROJECT_ROOT / "data" / "sample_listings.csv"


def run_pipeline(
    *,
    queue_alerts: bool = True,
    evaluate_searches: bool = True,
) -> None:
    print("Starting pipeline...")

    try:
        print("Step 1: Loading listings...")
        load_sample_csv(str(SAMPLE_LISTINGS_PATH))

        print("Step 2: Snapshotting current listings...")
        snapshot_current_listings()

        print("Step 3: Refreshing market scores...")
        score_listings_against_market()

        if evaluate_searches:
            print("Step 4: Evaluating enabled saved searches...")
            search_runs = evaluate_enabled_searches()
            print(
                f"Evaluated {len(search_runs)} enabled saved search(es); "
                f"recorded {sum(run.recorded for run in search_runs)} "
                "new listing evaluation(s)."
            )
        else:
            print("Step 4: Skipping saved-search evaluation.")

        if queue_alerts:
            print("Step 5: Queueing price drop alerts...")
            queue_price_drop_alerts()
            print("Run the outbox worker separately to deliver queued alerts.")
        else:
            print("Step 5: Skipping price-drop alert queueing.")

        print("Pipeline completed successfully.")

    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the sample listing pipeline.")
    parser.add_argument(
        "--skip-alerts",
        action="store_true",
        help="Load and snapshot listings without queueing price-drop alerts.",
    )
    parser.add_argument(
        "--skip-search-evaluation",
        action="store_true",
        help="Load and snapshot listings without evaluating saved searches.",
    )
    args = parser.parse_args()
    run_pipeline(
        queue_alerts=not args.skip_alerts,
        evaluate_searches=not args.skip_search_evaluation,
    )
