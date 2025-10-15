# assets/ingest/klaviyo/test_email_clicked.py
import os
import sys
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    project_root = Path(__file__).resolve().parent.parent.parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))
    
    from orchestration.lotus_dagster.assets.ingest.klaviyo.email_clicked import extract_email_clicked_query_fn
    from orchestration.lotus_dagster.resources.connectors.klaviyo_resource import KlaviyoClient

except ImportError as e:
    print(f"❌ ERROR: Failed to import project modules. \n"
          f"Please ensure you run this script as a module from your project's root directory (`db3`). \n"
          f"Details: {e}")
    sys.exit(1)

def main():
    """Test harness for the Klaviyo Email Clicked event extractor."""
    env_path = project_root / "infra" / ".env"
    load_dotenv(dotenv_path=env_path)
    api_key = os.getenv("KLAVIYO_API_KEY")

    if not api_key:
        logging.error("❌ KLAVIYO_API_KEY not found in .env file.")
        return

    logging.info("Initializing KlaviyoClient...")
    client = KlaviyoClient(api_key=api_key)
    
    last_sync_time = datetime.now(timezone.utc) - timedelta(hours=24)
    logging.info(f"🚀 Starting test extraction for data since: {last_sync_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 80)

    try:
        result = extract_email_clicked_query_fn(client, last_sync_time)
        
        logging.info("✅ Extraction function executed successfully!")
        logging.info(f"Total records processed: {result.get('row_count', 0)}")
        
        if result.get('records'):
            output_dir = Path(__file__).parent / "test_output"
            output_dir.mkdir(exist_ok=True)
            
            json_path = output_dir / "email_clicked_raw.json"
            with open(json_path, "w") as f:
                json.dump(result['records'], f, indent=2, default=str)
            logging.info(f"Saved raw JSON data to: {json_path}")

            df = pd.DataFrame(result['records'])
            csv_path = output_dir / "email_clicked.csv"
            df.to_csv(csv_path, index=False)
            logging.info(f"Saved transformed data to: {csv_path}")
            
            print("-" * 80)
            logging.info("Sample of transformed data:")
            print(df.head(3))

    except Exception as e:
        logging.error(f"❌ An error occurred during extraction: {e}", exc_info=True)

if __name__ == "__main__":
    main()