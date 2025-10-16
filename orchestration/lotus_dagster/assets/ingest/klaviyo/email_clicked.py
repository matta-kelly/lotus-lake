# assets/ingest/klaviyo/email_clicked.py
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict
import pyarrow as pa

from ..factories.make_flow import make_flow_assets
from ....resources.load import schema_converters

# --------------------------------------------------------------------
# Flow-specific config for EMAIL CLICKED EVENTS
# --------------------------------------------------------------------
NAMESPACE = "klaviyo"
TABLE = "email_clicked"
UPDATED_FIELD = "datetime"
MAX_PAGES_PER_RUN = 20
DEFAULT_TIME_WINDOW_HOURS = 120

METRIC_ID_EMAIL_CLICKED = "TteUhh"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------
# SCHEMA DEFINITION (Structure Contract)
# --------------------------------------------------------------------
EMAIL_CLICKED_SCHEMA = pa.schema([
    pa.field('event_id', pa.string()),
    pa.field('timestamp_utc', pa.timestamp('us', tz='UTC')),
    pa.field('profile_id', pa.string()),
    pa.field('email', pa.string()),
    pa.field('campaign_id', pa.string()),  # This will actually contain the campaign ID
    pa.field('flow_id', pa.string()),       # For flow emails if present
    pa.field('_load_timestamp', pa.timestamp('us', tz='UTC')),
    pa.field('datetime', pa.timestamp('us', tz='UTC')),
    pa.field('ingestion_date', pa.string()),
])

# --------------------------------------------------------------------
# FIELD CONVERTERS (Value Preprocessing)
# --------------------------------------------------------------------
FIELD_CONVERTERS = {
    'datetime': schema_converters.convert_timestamp,
    'timestamp_utc': schema_converters.convert_timestamp,
    '_load_timestamp': schema_converters.convert_timestamp,
}

# --------------------------------------------------------------------
# Asset builder
# --------------------------------------------------------------------
def build_email_clicked_assets():
    """Builds the asset pipeline for Klaviyo Email Clicked Events."""
    return make_flow_assets(NAMESPACE, TABLE, UPDATED_FIELD, extract_email_clicked_query_fn)

# --------------------------------------------------------------------
# Extraction function
# --------------------------------------------------------------------
def extract_email_clicked_query_fn(client, last_sync: datetime) -> Dict:
    """
    Extracts 'Email Clicked' events from Klaviyo within a specific time window.
    Note: Klaviyo's $message field contains the campaign_id for campaigns.
    """
    all_events = []
    page_count = 0
    start_time = time.time()

    start_iso = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_dt = last_sync + timedelta(hours=DEFAULT_TIME_WINDOW_HOURS)
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(f"Extracting Klaviyo EMAIL CLICKED events from {start_iso} to {end_iso}")

    combined_filter = (
        f"and("
        f"equals(metric_id,'{METRIC_ID_EMAIL_CLICKED}'),"
        f"greater-than(datetime,{start_iso}),"
        f"less-than(datetime,{end_iso})"
        f")"
    )

    params = {"filter": combined_filter, "sort": "datetime"}

    def flatten_event(event: Dict) -> Dict:
        """
        Flattens the nested API response.
        IMPORTANT: Klaviyo's $message field contains campaign_id for campaigns.
        """
        data = event.get("attributes", {}) or {}
        relationships = event.get("relationships", {}) or {}
        event_props = data.get("event_properties", {}) or {}
        profile_data = relationships.get("profile", {}).get("data", {}) or {}

        event_datetime_str = data.get("datetime") or data.get("datetime_")
        
        # Klaviyo's $message contains the campaign_id for campaigns
        # $flow contains flow ID for flow emails
        campaign_or_flow_id = event_props.get("$message")
        flow_id = event_props.get("$flow")

        return {
            "event_id": event.get("id"),
            "timestamp_utc": event_datetime_str,
            "profile_id": profile_data.get("id"),
            "email": event_props.get("Recipient Email Address"),
            "campaign_id": campaign_or_flow_id,  # This is actually the campaign ID
            "flow_id": flow_id,  # Store flow ID separately if present
            "_load_timestamp": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
            "datetime": event_datetime_str,
        }

    try:
        # Fetch first page
        response = client.get_events(**params)
        page_count += 1

        if hasattr(response, "data") and response.data:
            for e in response.data:
                record = e if isinstance(e, dict) else e.dict()
                all_events.append(flatten_event(record))
            logger.info(f"Page {page_count}: Fetched {len(response.data)} events")

        # Pagination loop
        while (
            hasattr(response, "links")
            and getattr(response.links, "next", None)
            and page_count < MAX_PAGES_PER_RUN
        ):
            next_url = response.links.next
            if not next_url:
                break

            response = client.get_events(page_cursor=next_url)
            page_count += 1

            if hasattr(response, "data") and response.data:
                for e in response.data:
                    record = e if isinstance(e, dict) else e.dict()
                    all_events.append(flatten_event(record))
                logger.info(f"Page {page_count}: Fetched {len(response.data)} events")
            else:
                logger.info(f"Page {page_count}: No data returned, stopping pagination.")
                break

    except Exception as e:
        logger.error(f"Failed to fetch Klaviyo email clicked events: {e}")
        raise RuntimeError(f"Klaviyo API error: {e}") from e

    elapsed = time.time() - start_time
    logger.info("EXTRACTION SUMMARY:")
    logger.info(f"  - Total clicked events: {len(all_events)}")
    logger.info(f"  - Pages fetched: {page_count}")
    logger.info(f"  - Time window: {start_iso} to {end_iso}")
    logger.info(f"  - Time elapsed: {elapsed:.2f}s")

    return {
        "records": all_events,
        "row_count": len(all_events),
        "schema": EMAIL_CLICKED_SCHEMA,
        "field_converters": FIELD_CONVERTERS,
    }