import logging
import json
import time
from datetime import datetime
from typing import List, Dict
import pyarrow as pa

from ..factories.make_flow import make_flow_assets
from ....resources.load import schema_converters  # ← NEW: Import converter library

# --------------------------------------------------------------------
# Flow-specific config
# --------------------------------------------------------------------
NAMESPACE = "shopify"
TABLE = "orders"
UPDATED_FIELD = "updatedAt"
PAGE_SIZE = 100
MAX_PAGES_PER_RUN = 50
has_next = False

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------
# SCHEMA DEFINITION (Structure Contract)
# Matches Iceberg DDL exactly - defines what the data should look like
# --------------------------------------------------------------------
ORDERS_SCHEMA = pa.schema([
    # Root-level fields
    pa.field('id', pa.string()),
    pa.field('name', pa.string()),
    pa.field('createdAt', pa.timestamp('us', tz='UTC')),
    pa.field('updatedAt', pa.timestamp('us', tz='UTC')),
    pa.field('displayFinancialStatus', pa.string()),
    
    # Customer struct
    pa.field('customer', pa.struct([
        pa.field('id', pa.string()),
        pa.field('email', pa.string()),
        pa.field('firstName', pa.string()),
        pa.field('lastName', pa.string()),
    ])),
    
    # Channel information struct
    pa.field('channelInformation', pa.struct([
        pa.field('channelDefinition', pa.struct([
            pa.field('channelName', pa.string()),
            pa.field('subChannelName', pa.string()),
        ])),
    ])),
    
    # Money structs
    pa.field('totalTaxSet', pa.struct([
        pa.field('shopMoney', pa.struct([
            pa.field('amount', pa.string()),
            pa.field('currencyCode', pa.string()),
        ])),
    ])),
    
    pa.field('currentSubtotalPriceSet', pa.struct([
        pa.field('shopMoney', pa.struct([
            pa.field('amount', pa.string()),
            pa.field('currencyCode', pa.string()),
        ])),
    ])),
    
    pa.field('currentTotalDiscountsSet', pa.struct([
        pa.field('shopMoney', pa.struct([
            pa.field('amount', pa.string()),
            pa.field('currencyCode', pa.string()),
        ])),
    ])),
    
    pa.field('currentTotalPriceSet', pa.struct([
        pa.field('shopMoney', pa.struct([
            pa.field('amount', pa.string()),
            pa.field('currencyCode', pa.string()),
        ])),
    ])),
    
    # Shipping address struct
    pa.field('shippingAddress', pa.struct([
        pa.field('address1', pa.string()),
        pa.field('city', pa.string()),
        pa.field('zip', pa.string()),
        pa.field('countryCode', pa.string()),
    ])),
    
    # Order-level discount applications (edges wrapper)
    pa.field('discountApplications', pa.struct([
        pa.field('edges', pa.list_(pa.struct([
            pa.field('node', pa.struct([
                pa.field('__typename', pa.string()),
                pa.field('index', pa.int32()),
                pa.field('allocationMethod', pa.string()),
                pa.field('targetSelection', pa.string()),
                pa.field('targetType', pa.string()),
                pa.field('code', pa.string()),
                pa.field('value', pa.struct([
                    pa.field('__typename', pa.string()),
                    pa.field('percentage', pa.float64()),
                    pa.field('amount', pa.string()),
                    pa.field('currencyCode', pa.string()),
                ])),
            ])),
        ]))),
    ])),
    
    # Shipping line with discount allocations (handles empty arrays correctly)
    pa.field('shippingLine', pa.struct([
        pa.field('originalPriceSet', pa.struct([
            pa.field('shopMoney', pa.struct([
                pa.field('amount', pa.string()),
                pa.field('currencyCode', pa.string()),
            ])),
        ])),
        pa.field('discountAllocations', pa.list_(pa.struct([
            pa.field('allocatedAmountSet', pa.struct([
                pa.field('shopMoney', pa.struct([
                    pa.field('amount', pa.string()),
                    pa.field('currencyCode', pa.string()),
                ])),
            ])),
            pa.field('discountApplication', pa.struct([
                pa.field('__typename', pa.string()),
                pa.field('index', pa.int32()),
                pa.field('allocationMethod', pa.string()),
                pa.field('code', pa.string()),
            ])),
        ]))),
        pa.field('taxLines', pa.list_(pa.struct([
            pa.field('price', pa.string()),
        ]))),
        pa.field('carrierIdentifier', pa.string()),
    ])),
    
    # Order-level tax lines
    pa.field('taxLines', pa.list_(pa.struct([
        pa.field('price', pa.string()),
        pa.field('title', pa.string()),
    ]))),
    
    # Line items (edges wrapper)
    pa.field('lineItems', pa.struct([
        pa.field('edges', pa.list_(pa.struct([
            pa.field('node', pa.struct([
                pa.field('id', pa.string()),
                pa.field('title', pa.string()),
                pa.field('quantity', pa.int32()),
                pa.field('sku', pa.string()),
                pa.field('product', pa.struct([
                    pa.field('id', pa.string()),
                    pa.field('productType', pa.string()),
                ])),
                pa.field('variant', pa.struct([
                    pa.field('id', pa.string()),
                    pa.field('sku', pa.string()),
                    pa.field('price', pa.string()),
                ])),
                pa.field('originalTotalSet', pa.struct([
                    pa.field('shopMoney', pa.struct([
                        pa.field('amount', pa.string()),
                        pa.field('currencyCode', pa.string()),
                    ])),
                ])),
                pa.field('discountedTotalSet', pa.struct([
                    pa.field('shopMoney', pa.struct([
                        pa.field('amount', pa.string()),
                        pa.field('currencyCode', pa.string()),
                    ])),
                ])),
                pa.field('discountAllocations', pa.list_(pa.struct([
                    pa.field('allocatedAmountSet', pa.struct([
                        pa.field('shopMoney', pa.struct([
                            pa.field('amount', pa.string()),
                            pa.field('currencyCode', pa.string()),
                        ])),
                    ])),
                    pa.field('discountApplication', pa.struct([
                        pa.field('__typename', pa.string()),
                        pa.field('index', pa.int32()),
                        pa.field('allocationMethod', pa.string()),
                        pa.field('code', pa.string()),
                    ])),
                ]))),
            ])),
        ]))),
    ])),
    
    # Partition column (added by loader)
    pa.field('ingestion_date', pa.string()),
])

# --------------------------------------------------------------------
# FIELD CONVERTERS (Value Preprocessing)
# Maps field names to converter functions from schema_converters module
# Tells load_s3.py HOW to prepare values before applying schema
# --------------------------------------------------------------------
FIELD_CONVERTERS = {
    # Root-level timestamp fields (ISO strings → datetime objects)
    'createdAt': schema_converters.convert_timestamp,
    'updatedAt': schema_converters.convert_timestamp,
}

# --------------------------------------------------------------------
# Asset builder (ties into factories)
# --------------------------------------------------------------------
def build_orders_assets():
    return make_flow_assets(NAMESPACE, TABLE, UPDATED_FIELD, extract_orders_query_fn)

def extract_orders_query_fn(client, last_sync: datetime) -> Dict:
    """
    Extract orders from Shopify with pagination.
    
    Returns:
        dict with:
            - records: List of order dicts from GraphQL
            - row_count: Number of records
            - schema: PyArrow schema defining structure
            - field_converters: Dict mapping fields to conversion functions
    """
    all_orders = []
    cursor = None
    page_count = 0
    start_time = time.time()
    
    since_date = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info(f"Building query with filter: updated_at >= {since_date}")
    
    query = f"""
      query($cursor: String) {{
        orders(
          first: {PAGE_SIZE}
          after: $cursor
          query: "NOT (financial_status:voided OR financial_status:pending OR financial_status:authorized) updated_at:>={since_date}"
          sortKey: UPDATED_AT
        ) {{
          edges {{
            node {{
              id
              name
              createdAt
              updatedAt
              displayFinancialStatus
              customer {{
                id
                email
                firstName
                lastName
              }}
              channelInformation {{
                channelDefinition {{
                  channelName
                  subChannelName
                }}
              }}
              totalTaxSet {{ shopMoney {{ amount currencyCode }} }}
              currentSubtotalPriceSet {{ shopMoney {{ amount currencyCode }} }}
              currentTotalDiscountsSet {{ shopMoney {{ amount currencyCode }} }}
              currentTotalPriceSet {{ shopMoney {{ amount currencyCode }} }}
              shippingAddress {{ address1 city zip countryCode }}

              # --- Order-level discounts ---
              discountApplications(first: 25) {{
                edges {{
                  node {{
                    __typename
                    index
                    allocationMethod
                    targetSelection
                    targetType
                    ... on DiscountCodeApplication {{ code }}
                    value {{
                      __typename
                      ... on MoneyV2 {{ amount currencyCode }}
                      ... on PricingPercentageValue {{ percentage }}
                    }}
                  }}
                }}
              }}

              # --- Shipping line ---
              shippingLine {{
                originalPriceSet {{ shopMoney {{ amount currencyCode }} }}
                discountAllocations {{
                  allocatedAmountSet {{ shopMoney {{ amount currencyCode }} }}
                  discountApplication {{
                    __typename
                    index
                    allocationMethod
                    ... on DiscountCodeApplication {{ code }}
                  }}
                }}
                taxLines {{ price }}
                carrierIdentifier
              }}

              taxLines {{ price title }}

              # --- Line items ---
              lineItems(first: 250) {{
                edges {{
                  node {{
                    id
                    title
                    quantity
                    sku
                    product {{ id productType }}
                    variant {{ id sku price }}
                    originalTotalSet {{ shopMoney {{ amount currencyCode }} }}
                    discountedTotalSet {{ shopMoney {{ amount currencyCode }} }}
                    discountAllocations {{
                      allocatedAmountSet {{ shopMoney {{ amount currencyCode }} }}
                      discountApplication {{
                        __typename
                        index
                        allocationMethod
                        ... on DiscountCodeApplication {{ code }}
                      }}
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

        for edge in edges:
            all_orders.append(edge["node"])

        page_count += 1
        logger.info(f"Page {page_count} complete: {len(edges)} orders (total so far: {len(all_orders)})")

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
            f"Hit pagination limit ({MAX_PAGES_PER_RUN} pages, {len(all_orders)} orders). "
            f"More data exists - will continue in next run."
        )

    elapsed = time.time() - start_time
    logger.info(f"EXTRACTION SUMMARY:")
    logger.info(f"  - Total orders: {len(all_orders)}")
    logger.info(f"  - Pages fetched: {page_count}")
    logger.info(f"  - Time elapsed: {elapsed:.2f}s")
    
    # Return complete configuration for downstream processing
    return {
        "records": all_orders,
        "row_count": len(all_orders),
        "schema": ORDERS_SCHEMA,
        "field_converters": FIELD_CONVERTERS,
    }