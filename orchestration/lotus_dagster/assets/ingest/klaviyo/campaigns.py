import logging
import time
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
import pyarrow as pa
import pandas as pd

from ..factories.make_flow import make_flow_assets
from ....resources.load import schema_converters

# --------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------
NAMESPACE = "klaviyo"
TABLE = "campaign"
UPDATED_FIELD = "updated_at"
MAX_PAGES_PER_RUN = 1
DEFAULT_TIME_WINDOW_HOURS = 120

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------
# SCHEMA DEFINITION
# --------------------------------------------------------------------
CAMPAIGNS_SCHEMA = pa.schema([
    # Campaign-level fields
    pa.field('id', pa.string()),
    pa.field('name', pa.string()),
    pa.field('send_time', pa.timestamp('us', tz='UTC')),
    pa.field('updated_at', pa.timestamp('us', tz='UTC')),
    pa.field('_load_timestamp', pa.timestamp('us', tz='UTC')),
    pa.field('ingestion_date', pa.string()),

    # Nested messages array
    pa.field('messages', pa.list_(
        pa.struct([
            pa.field('id', pa.string()),
            pa.field('subject', pa.string()),
            pa.field('preview_text', pa.string()),
            pa.field('from_email', pa.string()),
            pa.field('from_label', pa.string()),
            pa.field('send_time', pa.timestamp('us', tz='UTC')),
            pa.field('created_at', pa.timestamp('us', tz='UTC')),
            pa.field('updated_at', pa.timestamp('us', tz='UTC')),
        ])
    )),
])

# --------------------------------------------------------------------
# FIELD CONVERTERS - Applied AFTER ensuring proper list structure
# --------------------------------------------------------------------
def convert_messages_field(messages_value: Any) -> List[Dict]:
    """
    Custom converter for the messages field that ensures it's a proper list of dicts
    and converts nested timestamps.
    """
    # First ensure it's a list of dicts
    messages = _ensure_list_of_dicts(messages_value)
    
    # Then convert timestamps within each message
    for msg in messages:
        if isinstance(msg, dict):
            # Convert timestamp fields in each message
            for field in ['send_time', 'created_at', 'updated_at']:
                if field in msg:
                    msg[field] = schema_converters.convert_timestamp(msg[field])
    
    return messages

FIELD_CONVERTERS = {
    'send_time': schema_converters.convert_timestamp,
    'updated_at': schema_converters.convert_timestamp,
    '_load_timestamp': schema_converters.convert_timestamp,
    'messages': convert_messages_field,  # Custom converter for the entire messages field
}

# --------------------------------------------------------------------
# Asset builder
# --------------------------------------------------------------------
def build_campaign_assets():
    return make_flow_assets(NAMESPACE, TABLE, UPDATED_FIELD, extract_campaigns_query_fn)

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def _ensure_list_of_dicts(x: Any) -> List[Dict]:
    """
    Guarantee a list[dict] (Arrow-compatible).
    Handles None, strings (JSON or repr), lists, and other types.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return []
    
    # If it's already a proper list, validate its contents
    if isinstance(x, list):
        result = []
        for item in x:
            if item is None:
                continue
            if isinstance(item, dict):
                result.append(item)
            elif hasattr(item, 'dict'):
                result.append(item.dict())
            elif isinstance(item, str):
                # Try to parse string items as JSON
                try:
                    parsed = json.loads(item)
                    if isinstance(parsed, dict):
                        result.append(parsed)
                except json.JSONDecodeError:
                    logger.warning(f"Skipping non-JSON string item in list: {item[:50]}...")
        return result
    
    # If it's a string, could be JSON or Python repr
    if isinstance(x, str):
        # First try JSON parsing
        try:
            parsed = json.loads(x)
            if isinstance(parsed, list):
                return _ensure_list_of_dicts(parsed)
            elif isinstance(parsed, dict):
                return [parsed]
            else:
                return []
        except json.JSONDecodeError:
            # Not JSON, might be Python repr like "[{'id': '...'}]"
            try:
                import ast
                parsed = ast.literal_eval(x)
                if isinstance(parsed, list):
                    return _ensure_list_of_dicts(parsed)
                elif isinstance(parsed, dict):
                    return [parsed]
            except (ValueError, SyntaxError):
                logger.warning(f"Could not parse string as list: {x[:100]}...")
                return []
    
    # For any other type, return empty list
    logger.warning(f"Unexpected type for messages field: {type(x)}")
    return []

# --------------------------------------------------------------------
# Extraction function
# --------------------------------------------------------------------
def extract_campaigns_query_fn(client, last_sync: datetime) -> Dict:
    """
    Extract Klaviyo Campaigns between two timestamps.
    For each campaign, also fetch campaign-messages and attach them as a nested list.
    Ensures `messages` is always a list[dict] matching Arrow schema.
    """
    all_campaigns: List[Dict] = []
    page_count = 0
    start_time = time.time()

    start_iso = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_dt = last_sync + timedelta(hours=DEFAULT_TIME_WINDOW_HOURS)
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(f"Extracting Klaviyo Campaigns (with messages) from {start_iso} to {end_iso}")

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
            "send_time": attrs.get("send_time"),
            "updated_at": attrs.get("updated_at"),
            "_load_timestamp": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        }

    def flatten_message(m: Dict) -> Dict:
        attrs = m.get("attributes", {}) or {}
        definition = attrs.get("definition", {}) or {}
        content = definition.get("content", {}) or {}
        send_times = attrs.get("send_times", []) or []
        send_time_val = None
        if send_times and isinstance(send_times, list):
            first = send_times[0] or {}
            if isinstance(first, dict):
                send_time_val = first.get("datetime")

        return {
            "id": m.get("id"),
            "subject": content.get("subject"),
            "preview_text": content.get("preview_text"),
            "from_email": content.get("from_email"),
            "from_label": content.get("from_label"),
            "send_time": send_time_val,
            "created_at": attrs.get("created_at"),
            "updated_at": attrs.get("updated_at"),
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

                    # Fetch and normalize messages
                    cid = campaign_row["id"]
                    messages_list = []
                    
                    try:
                        logger.info(f"Fetching messages for campaign {cid}")
                        msg_response = client.get_campaign_messages(campaign_id=cid)
                        raw_msgs = getattr(msg_response, "data", []) or []
                        
                        for m in raw_msgs:
                            msg_dict = flatten_message(m if isinstance(m, dict) else m.dict())
                            messages_list.append(msg_dict)
                        
                        logger.info(f"  → {len(messages_list)} messages fetched for campaign {cid}")
                    except Exception as me:
                        logger.warning(f"Failed to fetch messages for campaign {cid}: {me}")
                        # Keep messages_list as empty list
                    
                    # Assign the messages list directly (not through _ensure_list_of_dicts yet)
                    # The converter will handle it
                    campaign_row["messages"] = messages_list
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
        logger.error(f"Failed to fetch Klaviyo campaigns (with messages): {e}")
        raise RuntimeError(f"Klaviyo API error: {e}")

    elapsed = time.time() - start_time
    logger.info(f"EXTRACTION SUMMARY:")
    logger.info(f"  - Total campaigns: {len(all_campaigns)}")
    logger.info(f"  - Pages fetched: {page_count}")
    logger.info(f"  - Time window: {start_iso} to {end_iso}")
    logger.info(f"  - Time elapsed: {elapsed:.2f}s")
    
    # Validate messages field is properly structured before returning
    for campaign in all_campaigns:
        if "messages" in campaign and not isinstance(campaign["messages"], list):
            logger.error(f"Campaign {campaign['id']} has invalid messages type: {type(campaign['messages'])}")

    return {
        "records": all_campaigns,
        "row_count": len(all_campaigns),
        "schema": CAMPAIGNS_SCHEMA,
        "field_converters": FIELD_CONVERTERS,
    }