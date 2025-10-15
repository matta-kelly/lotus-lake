# make_update_last_sync.py
from dagster import asset, AssetIn
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def make_update_sync_asset(namespace: str, table: str):
    # Depend directly on iceberg_loader (decoupled from dbt)
    upstream_asset_key = f"iceberg_loader_{namespace}_{table}"
    
    @asset(
        required_resource_keys={"sync_state"},
        name=f"update_sync_{namespace}_{table}",
        group_name=f"{namespace}_{table}",
        ins={"iceberg_result": AssetIn(upstream_asset_key)},
    )
    def _update_sync(context, iceberg_result: dict) -> dict:
        """
        Update sync state after successful iceberg load.
        
        This marks the extraction as complete and advances the sync cursor.
        dbt transformations run separately and are not part of the ingestion flow.
        """
        # Validate input
        if iceberg_result is None:
            raise ValueError(f"Received None result for {namespace}/{table}")
        if not isinstance(iceberg_result, dict):
            raise TypeError(f"Result must be dict for {namespace}/{table}, got {type(iceberg_result)}")
        
        # Extract timestamp
        if "last_updated_at" not in iceberg_result:
            raise ValueError(f"Result missing 'last_updated_at' for {namespace}/{table}")
        
        last_updated_at = iceberg_result["last_updated_at"]
        
        # With buffer strategy, this should never be None
        if last_updated_at is None:
            raise ValueError(
                f"Cannot update sync state: last_updated_at is None for {namespace}/{table}. "
                f"Buffer strategy guarantees data - this should never happen."
            )
        
        if not isinstance(last_updated_at, datetime):
            raise TypeError(f"last_updated_at must be datetime for {namespace}/{table}, got {type(last_updated_at)}")
        
        if last_updated_at.tzinfo is None:
            raise ValueError(f"last_updated_at must be timezone-aware for {namespace}/{table}")
        
        # Sanity check: timestamp shouldn't be in the future
        now = datetime.now(timezone.utc)
        if last_updated_at > now:
            raise ValueError(
                f"last_updated_at is in the future for {namespace}/{table}: "
                f"{last_updated_at.isoformat()} > {now.isoformat()}. "
                f"This indicates clock skew or bad source data."
            )
        
        # Get old timestamp for validation
        old_last_sync = context.resources.sync_state.get_last_sync(namespace, table)
        
        # Warn if timestamp didn't advance (duplicate processing)
        if last_updated_at <= old_last_sync:
            logger.warning(
                f"New last_updated_at ({last_updated_at.isoformat()}) is not after "
                f"old last_sync ({old_last_sync.isoformat()}) for {namespace}/{table}. "
                f"This may indicate duplicate processing or clock issues."
            )
        
        # Update sync state after successful iceberg load
        context.log.info(
            f"Iceberg load complete - updating sync state for {namespace}/{table}: "
            f"{old_last_sync.isoformat()} → {last_updated_at.isoformat()}"
        )
        
        result = context.resources.sync_state.update_last_sync(namespace, table, last_updated_at)
        
        if result is None:
            raise RuntimeError(f"sync_state.update_last_sync returned None for {namespace}/{table}")
        
        context.log.info(f"Successfully updated last_sync for {namespace}/{table} to {result.isoformat()}")
        context.log.info(f"Ingestion complete for {namespace}/{table}. dbt transformations will run separately.")
        
        return {
            "namespace": namespace,
            "table": table,
            "sync_timestamp": result,
            "status": "updated"
        }

    return _update_sync