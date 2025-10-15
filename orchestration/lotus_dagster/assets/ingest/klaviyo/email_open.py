import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict
import pyarrow as pa

from ..factories.make_flow import make_flow_assets
from ....resources.load import schema_converters

# --------------------------------------------------------------------
# Flow-specific config for EMAIL OPEN EVENTS
# --------------------------------------------------------------------
NAMESPACE = "klaviyo"
TABLE = "email_open"
UPDATED_FIELD = "datetime"
MAX_PAGES_PER_RUN = 20
DEFAULT_TIME_WINDOW_HOURS = 24

METRIC_ID_EMAIL_OPEN = "UCDKb9"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------
# SCHEMA DEFINITION (Structure Contract)
# --------------------------------------------------------------------
EMAIL_OPEN_SCHEMA = pa.schema([
    pa.field('event_id', pa.string()),
    pa.field('timestamp_utc', pa.timestamp('us', tz='UTC')),
    pa.field('profile_id', pa.string()),
    pa.field('email', pa.string()),
    pa.field('campaign_id', pa.string()),
    pa.field('message_id', pa.string()),
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
}


# --------------------------------------------------------------------
# Asset builder
# --------------------------------------------------------------------
def build_email_open_assets():
    """Builds the asset pipeline for Klaviyo Email Open Events."""
    return make_flow_assets(NAMESPACE, TABLE, UPDATED_FIELD, extract_email_open_query_fn)


# --------------------------------------------------------------------
# Extraction function
# --------------------------------------------------------------------
def extract_email_open_query_fn(client, last_sync: datetime) -> Dict:
    """
    Extracts 'Email Open' events from Klaviyo, passing raw data
    with schema and converters for robust downstream processing.
    """
    all_events = []
    page_count = 0
    start_time = time.time()

    start_iso = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_dt = last_sync + timedelta(hours=DEFAULT_TIME_WINDOW_HOURS)
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(f"Extracting Klaviyo EMAIL OPEN events from {start_iso} to {end_iso}")

    combined_filter = (
        f"and("
        f"equals(metric_id,'{METRIC_ID_EMAIL_OPEN}'),"
        f"greater-than(datetime,{start_iso}),"
        f"less-than(datetime,{end_iso})"
        f")"
    )

    params = {"filter": combined_filter, "sort": "datetime"}

    def flatten_event(event: Dict) -> Dict:
        """
        Flattens the nested API response, preserving the raw timestamp string
        by using the correct 'datetime_' field name from the SDK.
        """
        data = event.get("attributes", {}) or {}
        relationships = event.get("relationships", {}) or {}
        event_props = data.get("event_properties", {}) or {}
        profile_data = relationships.get("profile", {}).get("data", {}) or {}

        # The raw ISO string from the API, accessed with the correct field name.
        # Conversion is handled by the loader via FIELD_CONVERTERS.
        event_datetime_str = data.get("datetime_")

        return {
            "event_id": event.get("id"),
            "timestamp_utc": event_datetime_str, # Pass raw string
            "profile_id": profile_data.get("id"),
            "email": event_props.get("Recipient Email Address"),
            "campaign_id": event_props.get("$campaign"),
            "message_id": event_props.get("$message"),
            "_load_timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
            "datetime": event_datetime_str, # Pass raw string
        }

    try:
        response = client.get_events(**params)
        page_count += 1

        if hasattr(response, "data") and response.data:
            for e in response.data:
                all_events.append(flatten_event(e.dict()))
            logger.info(f"Page {page_count}: Fetched {len(response.data)} events")

        while (
            hasattr(response, "links")
            and getattr(response.links, "next", None)
            and page_count < MAX_PAGES_PER_RUN
        ):
            # The Klaviyo SDK handles pagination internally, but this is a safeguard.
            break

    except Exception as e:
        logger.error(f"Failed to fetch Klaviyo email open events: {e}")
        raise RuntimeError(f"Klaviyo API error: {e}") from e

    elapsed = time.time() - start_time
    logger.info("EXTRACTION SUMMARY:")
    logger.info(f"  - Total open events: {len(all_events)}")
    logger.info(f"  - Pages fetched: {page_count}")
    logger.info(f"  - Time window: {start_iso} to {end_iso}")
    logger.info(f"  - Time elapsed: {elapsed:.2f}s")

    # Return the full payload for the S3 loader
    return {
        "records": all_events,
        "row_count": len(all_events),
        "schema": EMAIL_OPEN_SCHEMA,
        "field_converters": FIELD_CONVERTERS,
    }