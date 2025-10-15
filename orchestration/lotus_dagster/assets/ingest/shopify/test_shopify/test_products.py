import json
import logging
from datetime import datetime, timedelta, timezone
import sys
from pathlib import Path

# ----------------------------------------------------------------------
# Ensure project root (e.g., ~/bode/lotusandluna/db3) is on sys.path
# ----------------------------------------------------------------------
current_file = Path(__file__).resolve()
project_root = current_file.parents[5]  # Goes up to db3/
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# ----------------------------------------------------------------------
# Imports (now aligned with your live structure)
# ----------------------------------------------------------------------
try:
    from orchestration.lotus_dagster.resources.connectors.shop_resource import ShopifyClient
    from orchestration.lotus_dagster.assets.ingest.shopify.products import extract_products_query_fn
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
    client = ShopifyClient()  # ✅ No api_key argument — uses internal credentials

    last_sync_time = datetime.now(timezone.utc) - timedelta(hours=24)
    logging.info(f"🚀 Fetching products updated since: {last_sync_time.isoformat()}")

    result = extract_products_query_fn(client, last_sync_time)

    logging.info(f"✅ Extraction complete: {result['row_count']} products fetched")

    output_path = current_file.parent / "shopify_products_output.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)

    # Optional: quick summary
    products = result.get("records", {}).get("products", [])
    variants = result.get("records", {}).get("variants", [])
    logging.info(f"Summary → Products: {len(products)}, Variants: {len(variants)}")

    if products:
        logging.info(f"Example product: {json.dumps(products[0], indent=2)}")

    logging.info(f"🗂 Output saved to {output_path}")

# ----------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------
if __name__ == "__main__":
    main()
