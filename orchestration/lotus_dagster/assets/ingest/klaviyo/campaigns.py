import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List
import pyarrow as pa

from ..factories.make_flow import make_flow_assets
from ....resources.load import schema_converters

# --------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------
NAMESPACE = "klaviyo"
TABLE = "campaign"
UPDATED_FIELD = "updated_at"
MAX_PAGES_PER_RUN = 20
DEFAULT_TIME_WINDOW_HOURS = 120

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------
# SIMPLIFIED SCHEMA - Just campaign data, no nested messages
# --------------------------------------------------------------------
CAMPAIGNS_SCHEMA = pa.schema([
    pa.field('id', pa.string()),
    pa.field('name', pa.string()),
    pa.field('status', pa.string()),
    pa.field('send_time', pa.timestamp('us', tz='UTC')),
    pa.field('created_at', pa.timestamp('us', tz='UTC')),
    pa.field('updated_at', pa.timestamp('us', tz='UTC')),
    pa.field('_load_timestamp', pa.timestamp('us', tz='UTC')),
    pa.field('ingestion_date', pa.string()),
])

# --------------------------------------------------------------------
# FIELD CONVERTERS
# --------------------------------------------------------------------
FIELD_CONVERTERS = {
    'send_time': schema_converters.convert_timestamp,
    'created_at': schema_converters.convert_timestamp,
    'updated_at': schema_converters.convert_timestamp,
    '_load_timestamp': schema_converters.convert_timestamp,
}

# --------------------------------------------------------------------
# Asset builder
# --------------------------------------------------------------------
def build_campaign_assets():
    return make_flow_assets(NAMESPACE, TABLE, UPDATED_FIELD, extract_campaigns_query_fn)

# --------------------------------------------------------------------
# Extraction function - SIMPLIFIED
# --------------------------------------------------------------------
def extract_campaigns_query_fn(client, last_sync: datetime) -> Dict:
    """
    Extract Klaviyo Campaigns - simplified version without nested messages.
    Campaign ID is the reliable link to events.
    """
    all_campaigns: List[Dict] = []
    page_count = 0
    start_time = time.time()

    start_iso = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_dt = last_sync + timedelta(hours=DEFAULT_TIME_WINDOW_HOURS)
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(f"Extracting Klaviyo Campaigns from {start_iso} to {end_iso}")

    params = {
        "filter": (
            f"and("
            f"equals(messages.channel,'email'),"
            f"greater-or-equal(updated_at,{start_iso}),"
            f"less-than(updated_at,{end_iso})"
            f")"
        ),
        "sort": "-updated_at",
    }

    def flatten_campaign(c: Dict) -> Dict:
        attrs = c.get("attributes", {}) or {}
        return {
            "id": c.get("id"),
            "name": attrs.get("name"),
            "status": attrs.get("status"),
            "send_time": attrs.get("send_time"),
            "created_at": attrs.get("created_at"),
            "updated_at": attrs.get("updated_at"),
            "_load_timestamp": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        }

    try:
        response = client.get_campaigns(**params)
        page_count += 1
        logger.info(f"Fetched campaigns page {page_count}")

        while True:
            if hasattr(response, "data") and response.data:
                logger.info(f"Page {page_count}: {len(response.data)} campaigns")
                for c in response.data:
                    record = c if isinstance(c, dict) else c.dict()
                    campaign_row = flatten_campaign(record)
                    all_campaigns.append(campaign_row)

            # Pagination
            if (
                hasattr(response, "links")
                and getattr(response.links, "next", None)
                and page_count < MAX_PAGES_PER_RUN
            ):
                params["page_cursor"] = response.links.next
                response = client.get_campaigns(**params)
                page_count += 1
                logger.info(f"Fetched campaigns page {page_count}")
            else:
                break

    except Exception as e:
        logger.error(f"Failed to fetch Klaviyo campaigns: {e}")
        raise RuntimeError(f"Klaviyo API error: {e}")

    elapsed = time.time() - start_time
    logger.info(f"EXTRACTION SUMMARY:")
    logger.info(f"  - Total campaigns: {len(all_campaigns)}")
    logger.info(f"  - Pages fetched: {page_count}")
    logger.info(f"  - Time window: {start_iso} to {end_iso}")
    logger.info(f"  - Time elapsed: {elapsed:.2f}s")

    return {
        "records": all_campaigns,
        "row_count": len(all_campaigns),
        "schema": CAMPAIGNS_SCHEMA,
        "field_converters": FIELD_CONVERTERS,
    }