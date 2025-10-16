# assets/ingest/klaviyo/test_purchase_events.py

# --- Standard Library Imports ---
import os
import sys
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

# --- Third-Party Imports ---
import pandas as pd
from dotenv import load_dotenv




# --------------------------------------------------------------------
# Import Project Dependencies
# --------------------------------------------------------------------
try:
    # Compute project root dynamically (top-level "db3" directory)
    project_root = Path(__file__).resolve()
    # Step up 7 levels from this file to reach "db3/"
    for _ in range(7):
        project_root = project_root.parent

    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    # Now imports will resolve correctly from project root
    from orchestration.lotus_dagster.assets.ingest.klaviyo.email_open import extract_email_open_query_fn
    from orchestration.lotus_dagster.resources.connectors.klaviyo_resource import KlaviyoClient


except ImportError as e:
    print(f"❌ ERROR: Failed to import project modules.\n"
          f"Please run from project root: "
          f"`python -m orchestration.lotus_dagster.assets.ingest.klaviyo.test_klaviyo.test_campaigns`\n"
          f"Details: {e}")
    sys.exit(1)

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    """
    A simple test harness to run the Klaviyo purchase event extraction 
    logic without needing a full orchestration engine.
    """
    # --- Load Environment & Initialize Client ---
    env_path = project_root / "infra" / ".env"
    if not env_path.exists():
        logging.error(f"❌ .env file not found at expected path: {env_path}")
        return
        
    load_dotenv(dotenv_path=env_path)
    api_key = os.getenv("KLAVIYO_API_KEY")

    if not api_key:
        logging.error("❌ KLAVIYO_API_KEY not found in .env file. Please check your configuration.")
        return
        
    logging.info("Initializing KlaviyoClient...")
    client = KlaviyoClient(api_key=api_key) 
    
    # --- Set Parameters and Run ---
    # Simulate a run asking for data from the last 24 hours.
    last_sync_time = datetime(2025, 10, 1, 14, 0, 0, tzinfo=timezone.utc)
    
    logging.info(f"🚀 Starting test extraction for data since: {last_sync_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 80)

    try:
        # --- Call Your Extraction Function ---
        result = extract_email_open_query_fn(client, last_sync_time)
        
        logging.info("✅ Extraction function executed successfully!")
        logging.info(f"Total purchase events found: {result.get('row_count', 0)}")
        
        # --- Save Output for Inspection ---
        if result.get('records'):
            output_dir = Path(__file__).parent / "test_output"
            output_dir.mkdir(exist_ok=True)

            # Save raw JSON
            json_path = output_dir / "open_events_raw.json"
            with open(json_path, "w") as f:
                json.dump(result['records'], f, indent=2, default=str)
            logging.info(f"Saved raw JSON to: {json_path}")

            # Save flattened CSV
            df = pd.json_normalize(result['records'])
            csv_path = output_dir / "open_events.csv"
            df.to_csv(csv_path, index=False)
            logging.info(f"Saved all {len(df)} events to: {csv_path}")
            
            print("-" * 80)
            logging.info("Sample of flattened data:")
            print(df.head(3))

    except Exception as e:
        logging.error(f"❌ An error occurred during extraction: {e}", exc_info=True)

if __name__ == "__main__":
    main()