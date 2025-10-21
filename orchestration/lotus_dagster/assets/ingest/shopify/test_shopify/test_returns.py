import json
import logging
from datetime import datetime, timedelta, timezone
import sys
from pathlib import Path

# ----------------------------------------------------------------------
# Ensure project root (e.g., ~/bode/lotusandluna/db3) is on sys.path
# ----------------------------------------------------------------------
current_file = Path(__file__).resolve()

# ----------------------------------------------------------------------
# Imports (now aligned with your live structure)
# ----------------------------------------------------------------------
try:
    # Compute project root dynamically (top-level "db3" directory)
    project_root = Path(__file__).resolve()
    # Step up 7 levels from this file to reach "db3/"
    for _ in range(7):
        project_root = project_root.parent

    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))
    from orchestration.lotus_dagster.resources.connectors.shop_resource import ShopifyClient
    from orchestration.lotus_dagster.assets.ingest.shopify.returns import extract_refunds_query_fn
except ImportError as e:
    print(f"❌ ERROR: Failed to import project modules.\nDetails: {e}")
    sys.exit(1)

# ----------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ----------------------------------------------------------------------
# Main function
# ----------------------------------------------------------------------
def main():
    logging.info("Initializing ShopifyClient...")
    client = ShopifyClient()

    # Fetch refunds from last 7 days (refunds are less frequent than products)
    last_sync_time = datetime.now(timezone.utc) - timedelta(days=7)
    logging.info(f"🚀 Fetching refunds updated since: {last_sync_time.isoformat()}")

    result = extract_refunds_query_fn(client, last_sync_time)

    logging.info(f"✅ Extraction complete: {result['row_count']} refunds fetched")

    output_path = current_file.parent / "shopify_refunds_output.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)

    # Optional: quick summary
    refunds = result.get("records", [])
    logging.info(f"Summary → Total refunds: {len(refunds)}")

    if refunds:
        # Show first refund as example
        logging.info(f"Example refund: {json.dumps(refunds[0], indent=2, default=str)}")
        
        # Count refund line items across all refunds
        total_line_items = sum(
            len(r.get("refundLineItems", {}).get("edges", []))
            for r in refunds
        )
        logging.info(f"Total refund line items across all refunds: {total_line_items}")

    logging.info(f"🗂 Output saved to {output_path}")

# ----------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------
if __name__ == "__main__":
    main()