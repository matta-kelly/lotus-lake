import logging
import time
from datetime import datetime
from typing import Dict, List, Any
import pyarrow as pa

from ..factories.make_flow import make_flow_assets
from ....resources.load import schema_converters

# --------------------------------------------------------------------
# Flow-specific config
# --------------------------------------------------------------------
NAMESPACE = "shopify"
TABLE = "products"
UPDATED_FIELD = "updatedAt"  
MAX_PAGES_PER_RUN = 10

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------
# SCHEMA DEFINITION (Structure Contract)
# Matches the Iceberg DDL and GraphQL query structure.
# --------------------------------------------------------------------
PRODUCTS_SCHEMA = pa.schema([
    pa.field('id', pa.string()),
    pa.field('handle', pa.string()),
    pa.field('title', pa.string()),
    pa.field('tags', pa.list_(pa.string())),
    pa.field('productType', pa.string()),
    pa.field('status', pa.string()),
    pa.field('createdAt', pa.timestamp('us', tz='UTC')),
    pa.field('updatedAt', pa.timestamp('us', tz='UTC')),
    pa.field('publishedAt', pa.timestamp('us', tz='UTC')),
    pa.field('variants', pa.struct([
        pa.field('edges', pa.list_(pa.struct([
            pa.field('node', pa.struct([
                pa.field('id', pa.string()),
                pa.field('sku', pa.string()),
                pa.field('price', pa.string()),
                pa.field('compareAtPrice', pa.string()),
                pa.field('createdAt', pa.timestamp('us', tz='UTC')),
                pa.field('updatedAt', pa.timestamp('us', tz='UTC')),
            ]))
        ])))
    ])),
    pa.field('ingestion_date', pa.string()),
])

# --------------------------------------------------------------------
# FIELD CONVERTERS (Value Preprocessing)
# --------------------------------------------------------------------
FIELD_CONVERTERS = {
    # This now ONLY handles top-level fields. The nested variant
    # timestamps are handled explicitly during extraction.
    'createdAt': schema_converters.convert_timestamp,
    'updatedAt': schema_converters.convert_timestamp,
    'publishedAt': schema_converters.convert_timestamp,
}

# --------------------------------------------------------------------
# Asset builder (ties into factories)
# --------------------------------------------------------------------
def build_products_assets():
    """Build the Dagster asset pipeline for Shopify Products."""
    return make_flow_assets(NAMESPACE, TABLE, UPDATED_FIELD, extract_products_query_fn)


def extract_products_query_fn(client, last_sync: datetime) -> Dict[str, Any]:
    """
    Extract Shopify products and explicitly pre-process nested timestamps
    before returning the data for loading.
    """
    start_iso = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info(f"Extracting Shopify products updated since {start_iso}")

    query = f"""
    query($cursor: String) {{
      products(
        first: 100
        after: $cursor
        query: "status:active OR status:draft OR status:archived updated_at:>={start_iso}"
        sortKey: UPDATED_AT
      ) {{
        edges {{
          node {{
            id
            handle
            title
            tags
            productType
            status
            createdAt
            updatedAt
            publishedAt
            variants(first: 250) {{
              edges {{
                node {{
                  id
                  sku
                  price
                  compareAtPrice
                  createdAt
                  updatedAt
                }}
              }}
            }}
          }}
        }}
        pageInfo {{
          hasNextPage
          endCursor
        }}
      }}
    }}
    """

    all_products: List[Dict[str, Any]] = []
    cursor = None
    page_count = 0
    start_time = time.time()

    while page_count < MAX_PAGES_PER_RUN:
        page_count += 1
        logger.info(f"Fetching page {page_count}...")
        
        variables = {"cursor": cursor}
        response = client.execute(query, variables)

        if "errors" in response:
            error_msg = f"GraphQL errors on page {page_count}: {response['errors']}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        products_data = response.get("data", {}).get("products", {})
        edges = products_data.get("edges", [])
        
        if not edges:
            logger.info("No more edges, ending pagination")
            break

        for edge in edges:
            all_products.append(edge["node"])

        logger.info(f"Page {page_count} complete: {len(edges)} products (total so far: {len(all_products)})")

        page_info = products_data.get("pageInfo", {})
        has_next = page_info.get("hasNextPage", False)
        
        if not has_next:
            logger.info("No more pages available, ending pagination")
            break
        
        cursor = page_info.get("endCursor")

    if has_next and page_count >= MAX_PAGES_PER_RUN:
        logger.warning(
            f"Hit pagination limit ({MAX_PAGES_PER_RUN} pages, {len(all_products)} products). "
            f"More data exists - will continue in next run."
        )
    
    # --- THE CORRECT FIX: Explicitly process nested timestamps ---
    # This loop targets ONLY the specific fields inside the variants list that
    # need conversion, preventing errors from a generic recursive function.
    logger.info(f"Pre-processing {len(all_products)} products to convert nested variant timestamps...")
    for product in all_products:
        if product.get("variants") and product["variants"].get("edges"):
            for edge in product["variants"]["edges"]:
                node = edge.get("node")
                if node:
                    if node.get("createdAt"):
                        node["createdAt"] = schema_converters.convert_timestamp(node["createdAt"])
                    if node.get("updatedAt"):
                        node["updatedAt"] = schema_converters.convert_timestamp(node["updatedAt"])
    logger.info("Nested timestamp pre-processing complete.")

    elapsed = time.time() - start_time
    logger.info("EXTRACTION SUMMARY:")
    logger.info(f"  - Total products: {len(all_products)}")
    logger.info(f"  - Pages fetched: {page_count}")
    logger.info(f"  - Time elapsed: {elapsed:.2f}s")

    return {
        "records": all_products,
        "row_count": len(all_products),
        "schema": PRODUCTS_SCHEMA,
        "field_converters": FIELD_CONVERTERS,
    }