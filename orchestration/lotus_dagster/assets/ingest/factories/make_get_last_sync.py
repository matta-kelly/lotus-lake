# make_get_last_sync.py
from dagster import asset
from datetime import datetime, timedelta, timezone
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Buffer to prevent boundary gaps - guarantees overlap with previous run
SYNC_BUFFER_SECONDS = 10

def make_get_last_sync_asset(namespace: str, table: str):
    @asset(
        required_resource_keys={"sync_state"},
        name=f"get_last_sync_{namespace}_{table}",
        group_name=f"{namespace}_{table}",
    )
    def _get_last_sync(context) -> datetime:
        """
        Fetch the last sync timestamp for extraction, with buffer applied.
        
        Applies a 1-minute buffer to prevent boundary gaps. This ensures we always
        re-pull the most recent record from the previous run, guaranteeing no data loss.
        MERGE operations handle the duplicate idempotently.
        
        Returns:
            datetime: Buffered sync point for extraction (actual last_sync - 1 minute)
        """
        # Check if we're in development mode
        from shared.config import settings
        is_dev = settings.ENV.lower() in ["development", "dev", "local"]
        
        if is_dev:
            # DEV MODE: Use default date, return early - NEVER touch DB
            default_sync = datetime.now(timezone.utc) - timedelta(days=7)
            buffered_sync = default_sync - timedelta(seconds=SYNC_BUFFER_SECONDS)
            
            context.log.info(
                f"[DEV MODE] Using default last_sync for {namespace}/{table}: "
                f"default={default_sync.isoformat()}, "
                f"buffered={buffered_sync.isoformat()} "
                f"(buffer={SYNC_BUFFER_SECONDS}min)"
            )
            return buffered_sync
        
        # Get stored last_sync from DB
        stored_last_sync = context.resources.sync_state.get_last_sync(namespace, table)
        
        # Validate
        if stored_last_sync is None:
            raise ValueError(f"sync_state.get_last_sync returned None for {namespace}/{table}")
        if not isinstance(stored_last_sync, datetime):
            raise TypeError(f"last_sync must be datetime for {namespace}/{table}, got {type(stored_last_sync)}")
        if stored_last_sync.tzinfo is None:
            raise ValueError(f"last_sync must be timezone-aware for {namespace}/{table}")
        
        # Sanity check: last_sync shouldn't be in the future
        now = datetime.now(timezone.utc)
        if stored_last_sync > now:
            raise ValueError(
                f"last_sync is in the future for {namespace}/{table}: "
                f"{stored_last_sync.isoformat()} > {now.isoformat()}"
            )
        
        # Apply buffer to prevent boundary gaps
        buffered_sync = stored_last_sync - timedelta(seconds=SYNC_BUFFER_SECONDS)
        
        context.log.info(
            f"Last sync for {namespace}/{table}: "
            f"stored={stored_last_sync.isoformat()}, "
            f"buffered={buffered_sync.isoformat()} "
            f"(buffer={SYNC_BUFFER_SECONDS}min)"
        )
        
        return buffered_sync

    return _get_last_sync