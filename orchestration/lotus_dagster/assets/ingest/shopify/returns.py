import logging
import json
import time
from datetime import datetime
from typing import Dict
import pyarrow as pa

from ..factories.make_flow import make_flow_assets
from ....resources.load import schema_converters

# --------------------------------------------------------------------
# Flow-specific config
# --------------------------------------------------------------------
NAMESPACE = "shopify"
TABLE = "refunds"
UPDATED_FIELD = "updatedAt"
PAGE_SIZE = 100
MAX_PAGES_PER_RUN = 50

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------
# SCHEMA DEFINITION (Structure Contract)
# --------------------------------------------------------------------
REFUNDS_SCHEMA = pa.schema([
    # Root-level fields
    pa.field('id', pa.string()),
    pa.field('order_id', pa.string()),
    pa.field('order_name', pa.string()),
    pa.field('createdAt', pa.timestamp('us', tz='UTC')),
    pa.field('updatedAt', pa.timestamp('us', tz='UTC')),
    
    # Total refunded amount
    pa.field('totalRefundedSet', pa.struct([
        pa.field('shopMoney', pa.struct([
            pa.field('amount', pa.string()),
            pa.field('currencyCode', pa.string()),
        ])),
    ])),
    
    # Refund line items (edges wrapper)
    pa.field('refundLineItems', pa.struct([
        pa.field('edges', pa.list_(pa.struct([
            pa.field('node', pa.struct([
                pa.field('quantity', pa.int32()),
                pa.field('lineItem', pa.struct([
                    pa.field('id', pa.string()),
                    pa.field('title', pa.string()),
                    pa.field('sku', pa.string()),
                    pa.field('product', pa.struct([
                        pa.field('id', pa.string()),
                        pa.field('productType', pa.string()),
                    ])),
                    pa.field('variant', pa.struct([
                        pa.field('id', pa.string()),
                        pa.field('sku', pa.string()),
                    ])),
                ])),
                pa.field('subtotalSet', pa.struct([
                    pa.field('shopMoney', pa.struct([
                        pa.field('amount', pa.string()),
                        pa.field('currencyCode', pa.string()),
                    ])),
                ])),
                pa.field('totalTaxSet', pa.struct([
                    pa.field('shopMoney', pa.struct([
                        pa.field('amount', pa.string()),
                        pa.field('currencyCode', pa.string()),
                    ])),
                ])),
            ])),
        ]))),
    ])),
    
    # Transactions for shipping refunds
    pa.field('transactions', pa.struct([
        pa.field('edges', pa.list_(pa.struct([
            pa.field('node', pa.struct([
                pa.field('id', pa.string()),
                pa.field('amount', pa.string()),
                pa.field('kind', pa.string()),
                pa.field('status', pa.string()),
                pa.field('createdAt', pa.timestamp('us', tz='UTC')),
            ])),
        ]))),
    ])),
    
    # Partition column (added by loader)
    pa.field('ingestion_date', pa.string()),
])

# --------------------------------------------------------------------
# FIELD CONVERTERS (Value Preprocessing)
# --------------------------------------------------------------------
FIELD_CONVERTERS = {
    'createdAt': schema_converters.convert_timestamp,
    'updatedAt': schema_converters.convert_timestamp,
}

# --------------------------------------------------------------------
# Asset builder (ties into factories)
# --------------------------------------------------------------------
def build_refunds_assets():
    return make_flow_assets(NAMESPACE, TABLE, UPDATED_FIELD, extract_refunds_query_fn)

def extract_refunds_query_fn(client, last_sync: datetime) -> Dict:
    """
    Extract refunds from Shopify with pagination.
    
    Returns:
        dict with:
            - records: List of refund dicts from GraphQL (raw nested structure)
            - row_count: Number of records
            - schema: PyArrow schema defining structure
            - field_converters: Dict mapping fields to conversion functions
    """
    all_refunds = []
    cursor = None
    page_count = 0
    start_time = time.time()
    
    since_date = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info(f"Building query with filter: updated_at >= {since_date}")
    
    # Single paginated query - fetch orders with full refund details
    query = f"""
      query($cursor: String) {{
        orders(
          first: {PAGE_SIZE}
          after: $cursor
          query: "updated_at:>={since_date}"
          sortKey: UPDATED_AT
        ) {{
          edges {{
            node {{
              id
              name
              refunds(first: 100) {{
                id
                createdAt
                updatedAt
                totalRefundedSet {{
                  shopMoney {{
                    amount
                    currencyCode
                  }}
                }}
                refundLineItems(first: 100) {{
                  edges {{
                    node {{
                      quantity
                      lineItem {{
                        id
                        title
                        sku
                        product {{
                          id
                          productType
                        }}
                        variant {{
                          id
                          sku
                        }}
                      }}
                      subtotalSet {{
                        shopMoney {{
                          amount
                          currencyCode
                        }}
                      }}
                      totalTaxSet {{
                        shopMoney {{
                          amount
                          currencyCode
                        }}
                      }}
                    }}
                  }}
                }}
                transactions(first: 250) {{
                  edges {{
                    node {{
                      id
                      amount
                      kind
                      status
                      createdAt
                    }}
                  }}
                }}
              }}
            }}
          }}
          pageInfo {{ hasNextPage endCursor }}
        }}
      }}
    """
    
    logger.info(f"Starting pagination loop (max {MAX_PAGES_PER_RUN} pages)...")
    
    while page_count < MAX_PAGES_PER_RUN:
        logger.info(f"Fetching page {page_count + 1}...")
        variables = {"cursor": cursor}
        
        response = client.execute(query, variables)
        data = response if isinstance(response, dict) else json.loads(response)
        
        if 'errors' in data:
            error_msg = f"GraphQL errors on page {page_count + 1}: {data['errors']}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        orders_data = data.get("data", {}).get("orders", {})
        edges = orders_data.get("edges", [])
        
        logger.info(f"Page {page_count + 1}: Received {len(edges)} edges")
        
        if not edges:
            logger.info("No more edges, ending pagination")
            break
        
        # Extract refunds from orders
        for edge in edges:
            order = edge["node"]
            order_id = order.get("id")
            order_name = order.get("name")
            refunds = order.get("refunds", [])
            
            # Each refund becomes a separate record
            for refund in refunds:
                refund_record = {
                    'id': refund.get('id'),
                    'order_id': order_id,
                    'order_name': order_name,
                    'createdAt': refund.get('createdAt'),
                    'updatedAt': refund.get('updatedAt'),
                    'totalRefundedSet': refund.get('totalRefundedSet'),
                    'refundLineItems': refund.get('refundLineItems'),
                    'transactions': refund.get('transactions'),
                }
                all_refunds.append(refund_record)
        
        page_count += 1
        logger.info(f"Page {page_count} complete: extracted {len([r for e in edges for r in e['node'].get('refunds', [])])} refunds (total so far: {len(all_refunds)})")
        
        page_info = orders_data.get("pageInfo", {})
        has_next = page_info.get("hasNextPage", False)
        logger.info(f"Has next page: {has_next}")
        
        if not has_next:
            logger.info("No more pages available, ending pagination")
            break
        
        cursor = page_info.get("endCursor")
        if cursor:
            logger.debug(f"Next cursor: {cursor[:20]}...")
    
    if has_next and page_count >= MAX_PAGES_PER_RUN:
        logger.warning(
            f"Hit pagination limit ({MAX_PAGES_PER_RUN} pages, {len(all_refunds)} refunds). "
            f"More data exists - will continue in next run."
        )
    
    logger.info(f"Pre-processing {len(all_refunds)} refunds to convert nested transaction timestamps...")
    for refund in all_refunds:
        if refund.get("transactions") and refund["transactions"].get("edges"):
            for edge in refund["transactions"]["edges"]:
                node = edge.get("node")
                if node and node.get("createdAt"):
                    node["createdAt"] = schema_converters.convert_timestamp(node["createdAt"])
    logger.info("Nested timestamp pre-processing complete.")
    
    elapsed = time.time() - start_time
    logger.info(f"EXTRACTION SUMMARY:")
    logger.info(f"  - Total refunds: {len(all_refunds)}")
    logger.info(f"  - Pages fetched: {page_count}")
    logger.info(f"  - Time elapsed: {elapsed:.2f}s")
    
    return {
        "records": all_refunds,
        "row_count": len(all_refunds),
        "schema": REFUNDS_SCHEMA,
        "field_converters": FIELD_CONVERTERS,
    }