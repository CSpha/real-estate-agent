from app.alerts.send_price_drop_alerts import main as send_price_drop_alerts
from app.ingest.load_sample_listings import load_sample_csv
from app.transforms.snapshot_listings import snapshot_current_listings


def run_pipeline():
    print("Starting pipeline...")

    try:
        print("Step 1: Loading listings...")
        load_sample_csv("data/sample_listings.csv")

        print("Step 2: Snapshotting current listings...")
        snapshot_current_listings()

        print("Step 3: Sending price drop alerts...")
        send_price_drop_alerts()

        print("Pipeline completed successfully.")

    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()