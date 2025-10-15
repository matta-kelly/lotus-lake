import logging
import os
from datetime import datetime, timezone
from typing import List, Dict
import pandas as pd
from dagster import resource
from shared.config import settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class LocalLoader:
    """Loads extracted data to local CSV files for testing."""
    
    def load(self, source: str, entity: str, data: List[Dict]) -> str:
        """
        Load data to local CSV file.
        """
        print("=" * 80, flush=True)
        print(">>> LOCAL LOADER: Starting load process", flush=True)
        print("=" * 80, flush=True)
        
        print(f">>> ENV check: {settings.ENV.lower()}", flush=True)
        if settings.ENV.lower() == "production":
            raise RuntimeError("LocalLoader is test-only and cannot run in production.")
        
        print(f">>> Source: {source}", flush=True)
        print(f">>> Entity: {entity}", flush=True)
        print(f">>> Data records: {len(data)}", flush=True)
        
        if not data:
            print(">>> WARNING: No data to load, creating empty file anyway", flush=True)
        
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_dir = "test_output"
        
        print(f">>> Creating output directory: {output_dir}", flush=True)
        os.makedirs(output_dir, exist_ok=True)
        
        output_path = f"{output_dir}/{source}_{entity}_{timestamp}.csv"
        print(f">>> Output path: {output_path}", flush=True)
        print(f">>> Absolute path: {os.path.abspath(output_path)}", flush=True)
        
        print(f">>> Converting to DataFrame...", flush=True)
        df = pd.DataFrame(data) if data else pd.DataFrame()
        print(f">>> DataFrame shape: {df.shape}", flush=True)
        print(f">>> DataFrame columns: {list(df.columns) if not df.empty else 'None (empty)'}", flush=True)
        
        print(f">>> Writing to CSV...", flush=True)
        df.to_csv(output_path, index=False)
        
        print(f">>> Verifying file exists...", flush=True)
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            print(f">>> SUCCESS: File written successfully", flush=True)
            print(f">>> File size: {file_size} bytes", flush=True)
            print(f">>> Location: {os.path.abspath(output_path)}", flush=True)
        else:
            print(f">>> ERROR: File does not exist after write!", flush=True)
        
        print("=" * 80, flush=True)
        logger.info("Wrote %d rows to %s", len(df), output_path)
        
        return output_path


@resource
def local_loader_resource(_):
    """Dagster resource for loading data to local files."""
    print(">>> Creating LocalLoader instance", flush=True)
    return LocalLoader()