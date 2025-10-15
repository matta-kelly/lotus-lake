import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict
import pyarrow as pa

from ..factories.make_flow import make_flow_assets
from ....resources.load import schema_converters

# --------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------
NAMESPACE = "klaviyo"
TABLE = "campaign"
UPDATED_FIELD = "updated_at"
MAX_PAGES_PER_RUN = 5
DEFAULT_TIME_WINDOW_HOURS = 24

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------
# SCHEMA DEFINITION
# --------------------------------------------------------------------
CAMPAIGNS_SCHEMA = pa.schema([
    pa.field('id', pa.string()),
    pa.field('name', pa.string()),
    pa.field('send_time', pa.timestamp('us', tz='UTC')),
    pa.field('updated_at', pa.timestamp('us', tz='UTC')),
    pa.field('_load_timestamp', pa.timestamp('us', tz='UTC')),
    pa.field('ingestion_date', pa.string()),
])

# --------------------------------------------------------------------
# FIELD CONVERTERS
# --------------------------------------------------------------------
FIELD_CONVERTERS = {
    'send_time': schema_converters.convert_timestamp,
    'updated_at': schema_converters.convert_timestamp,
    '_load_timestamp': schema_converters.convert_timestamp,
}

# --------------------------------------------------------------------
# Asset builder
# --------------------------------------------------------------------
def build_campaign_assets():
    return make_flow_assets(NAMESPACE, TABLE, UPDATED_FIELD, extract_campaigns_query_fn)

# --------------------------------------------------------------------
# Extraction function
# --------------------------------------------------------------------
def extract_campaigns_query_fn(client, last_sync: datetime) -> Dict:
    """
    Extract Klaviyo Campaigns between two timestamps.
    Works with both old SDK objects and new dict responses.
    """
    all_campaigns = []
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
        """Flatten campaign, keeping timestamps as RAW ISO STRINGS."""
        attrs = c.get("attributes", {}) or {}
        return {
            "id": c.get("id"),
            "name": attrs.get("name"),
            "send_time": attrs.get("send_time"),
            "updated_at": attrs.get("updated_at"),
            "_load_timestamp": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        }

    try:
        response = client.get_campaigns(**params)
        page_count += 1

        if hasattr(response, "data") and response.data:
            for c in response.data:
                record = c if isinstance(c, dict) else c.dict()
                all_campaigns.append(flatten_campaign(record))
            logger.info(f"Page {page_count}: fetched {len(response.data)} campaigns")

        while (
            hasattr(response, "links")
            and getattr(response.links, "next", None)
            and page_count < MAX_PAGES_PER_RUN
        ):
            params["page_cursor"] = response.links.next
            response = client.get_campaigns(**params)
            page_count += 1

            if hasattr(response, "data") and response.data:
                for c in response.data:
                    record = c if isinstance(c, dict) else c.dict()
                    all_campaigns.append(flatten_campaign(record))
                logger.info(f"Page {page_count}: fetched {len(response.data)} campaigns")
            else:
                break

    except Exception as e:
        logger.error(f"Failed to fetch Klaviyo campaigns: {e}")
        raise RuntimeError(f"Klaviyo API error: {e}")

    elapsed = time.time() - start_time
    logger.info(f"SUMMARY: {len(all_campaigns)} campaigns fetched across {page_count} pages in {elapsed:.2f}s")

    return {
        "records": all_campaigns,
        "row_count": len(all_campaigns),
        "schema": CAMPAIGNS_SCHEMA,
        "field_converters": FIELD_CONVERTERS,
    }