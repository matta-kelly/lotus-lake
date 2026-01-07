"""
S3 Sensors

Poll SeaweedFS for new Parquet files from Airbyte.
When new data lands, trigger the specific dbt model for that stream.

Auto-discovers source+stream combos from orchestration/assets/streams/{source}/{stream}.json
Each stream gets its own sensor that triggers only that stream's model.
"""
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    AssetSelection,
    DefaultSensorStatus,
)

# =============================================================================
# Configuration
# =============================================================================

STREAMS_DIR = Path(__file__).parent / "assets" / "streams"
S3_BUCKET = "landing"
S3_RAW_PREFIX = "raw"  # Airbyte writes to s3://landing/raw/{source}/{stream}/


# =============================================================================
# S3 Helpers
# =============================================================================

def get_s3_client():
    """Create S3 client for SeaweedFS."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:8333"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID", "minio"),
        aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY", "minio123"),
    )


def get_latest_modified(s3_client, bucket: str, prefix: str) -> datetime | None:
    """Get the most recent LastModified timestamp under a prefix."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" not in response:
            return None
        timestamps = [obj["LastModified"] for obj in response["Contents"]]
        return max(timestamps) if timestamps else None
    except Exception:
        return None


# =============================================================================
# Stream Discovery
# =============================================================================

def discover_streams() -> list[tuple[str, str]]:
    """
    Auto-discover source+stream combos from streams/ directory.

    Each .json file (except _catalog.json) is a stream.
    Example: streams/shopify/orders.json → ("shopify", "orders")

    Returns list of (source, stream) tuples.
    """
    if not STREAMS_DIR.exists():
        return []

    streams = []
    for source_dir in STREAMS_DIR.iterdir():
        if not source_dir.is_dir() or source_dir.name.startswith("_"):
            continue
        for stream_file in source_dir.glob("*.json"):
            if stream_file.name.startswith("_"):
                continue  # Skip _catalog.json
            stream_name = stream_file.stem
            streams.append((source_dir.name, stream_name))
    return streams


# =============================================================================
# Sensor Factory
# =============================================================================

def make_stream_sensor(source: str, stream: str):
    """
    Factory for stream-specific S3 sensors.

    - S3 path: raw/{source}/{stream}/ (matches Airbyte destination)
    - Asset selection: tag("{source}__{stream}") + downstream (marts via ref())

    dbt models must be tagged with source__stream to be triggered:
        {{ config(tags=['core', 'shopify__orders']) }}

    Downstream models (marts) auto-trigger via ref() dependencies.
    """
    tag = f"{source}__{stream}"
    s3_prefix = f"{S3_RAW_PREFIX}/{source}/{stream}/"

    # Select the tagged model AND anything downstream of it (marts via ref())
    selection = AssetSelection.tag(tag) | AssetSelection.tag(tag).downstream()

    @sensor(
        name=f"{source}_{stream}_sensor",
        asset_selection=selection,
        minimum_interval_seconds=300,  # 5 min
        default_status=DefaultSensorStatus.RUNNING,
    )
    def _sensor(context: SensorEvaluationContext):
        s3 = get_s3_client()
        latest = get_latest_modified(s3, S3_BUCKET, s3_prefix)

        if latest is None:
            context.log.info(f"No files found at s3://{S3_BUCKET}/{s3_prefix}")
            return

        # Cursor stores last seen timestamp as ISO string
        last_seen_str = context.cursor
        last_seen = (
            datetime.fromisoformat(last_seen_str)
            if last_seen_str
            else datetime.min.replace(tzinfo=timezone.utc)
        )

        if latest > last_seen:
            context.log.info(f"New {source}/{stream} data: {latest} > {last_seen}")
            context.update_cursor(latest.isoformat())
            yield RunRequest(run_key=f"{tag}_{latest.isoformat()}")
        else:
            context.log.info(f"No new {source}/{stream} data since {last_seen}")

    return _sensor


# =============================================================================
# Auto-generated Sensors
# =============================================================================

# Discover streams and create sensors automatically
_streams = discover_streams()
_sensors = {f"{src}__{strm}": make_stream_sensor(src, strm) for src, strm in _streams}

# Export all sensors
def get_all_sensors():
    """Get all auto-discovered sensors."""
    return list(_sensors.values())

__all__ = ["get_all_sensors"]
