# make_extractor_asset.py
from dagster import asset, AssetIn
from datetime import datetime
from typing import List, Dict, Callable
import time
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def make_extractor_asset(
    namespace: str,
    table: str,
    extractor_fn: Callable,
):
    upstream_asset_key = f"get_last_sync_{namespace}_{table}"

    @asset(
        required_resource_keys={namespace},
        name=f"extract_{namespace}_{table}",
        group_name=f"{namespace}_{table}",
        ins={"last_sync": AssetIn(upstream_asset_key)},
    )
    def _extract(context, last_sync: datetime):
        """
        Extract records from source API starting from last_sync timestamp.
        
        The last_sync timestamp already includes buffer logic from get_last_sync asset.
        Returns dict with records and row count for validation chain.
        """
        
        # Validate input
        if last_sync is None:
            raise ValueError(f"last_sync is None for {namespace}/{table}")
        if not isinstance(last_sync, datetime):
            raise TypeError(f"last_sync must be datetime for {namespace}/{table}, got {type(last_sync)}")
        if last_sync.tzinfo is None:
            raise ValueError(f"last_sync must be timezone-aware for {namespace}/{table}")

        # Get client resource
        client = getattr(context.resources, namespace, None)
        if client is None:
            raise RuntimeError(f"Failed to get {namespace} client from resources for {namespace}/{table}")

        # Execute extraction
        context.log.info(f"Starting extraction for {namespace}.{table} from {last_sync.isoformat()}")
        start = time.time()
        result = extractor_fn(client, last_sync)
        duration = time.time() - start

        # Validate output structure
        if result is None:
            raise ValueError(f"Extractor returned None for {namespace}/{table}")
        if not isinstance(result, dict):
            raise TypeError(f"Extractor must return dict for {namespace}/{table}, got {type(result)}")
        
        if "records" not in result or "row_count" not in result:
            raise ValueError(f"Extractor result missing required keys for {namespace}/{table}")
        
        records = result["records"]
        row_count = result["row_count"]
        
        # Validate records
        if not isinstance(records, list):
            raise TypeError(f"records must be list for {namespace}/{table}, got {type(records)}")
        
        # Validate count matches
        if len(records) != row_count:
            raise ValueError(
                f"Row count mismatch in extractor for {namespace}/{table}: "
                f"claimed={row_count}, actual={len(records)}"
            )
        
        # Empty records with buffer is an ERROR (buffer guarantees at least 1 record)
        if len(records) == 0:
            raise ValueError(
                f"No records extracted for {namespace}/{table} since {last_sync.isoformat()}. "
                f"Buffer strategy guarantees at least 1 record - this indicates API issue or data deletion."
            )

        context.log.info(
            f"Successfully extracted {row_count} records for {namespace}.{table} in {duration:.2f}s"
        )
        
        # Return the dict as-is for validation chain
        return result

    return _extract