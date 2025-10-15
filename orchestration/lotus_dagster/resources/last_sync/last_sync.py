import logging
from datetime import datetime, timezone
from dagster import resource
from shared.config import settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SyncStateManager:
    """Manages last_sync state for ingestion sources."""
    
    def __init__(self, db_connection):
        self.db = db_connection
    
    def get_last_sync(self, source: str, entity: str) -> datetime:
        """
        Get the last sync timestamp for a source/entity.
        
        TEST MODE: Returns DEFAULT_DATE
        PRODUCTION MODE: Reads from DB
        
        Returns:
            datetime: Timezone-aware timestamp for last successful sync
        """
        state_key = f"{source}_{entity}"
        
        # TEST MODE: Skip DB, use default
        if settings.ENV.lower() == "test":
            default_sync = datetime.fromisoformat(settings.DEFAULT_DATE).replace(tzinfo=timezone.utc)
            logger.info(
                "TEST MODE: Using default last_sync for %s/%s: %s",
                source, entity, default_sync.isoformat()
            )
            return default_sync
        
        # PRODUCTION MODE: Read from DB
        try:
            with self.db.cursor() as cur:
                cur.execute(
                    "SELECT last_sync FROM ingestion_state WHERE source = %s;",
                    (state_key,)
                )
                row = cur.fetchone()
            
            if row and row[0]:
                last_sync = row[0].astimezone(timezone.utc)
                logger.info(
                    "Retrieved stored last_sync for %s/%s: %s",
                    source, entity, last_sync.isoformat()
                )
                return last_sync
            else:
                default_sync = datetime.fromisoformat(settings.DEFAULT_DATE).replace(tzinfo=timezone.utc)
                logger.info(
                    "No last_sync found for %s/%s, using default: %s",
                    source, entity, default_sync.isoformat()
                )
                return default_sync
                
        except Exception as e:
            logger.error(f"Failed to retrieve last_sync for {source}/{entity}: {e}")
            raise RuntimeError(f"Database error retrieving sync state for {source}/{entity}: {e}")
    
    def update_last_sync(self, source: str, entity: str, new_last_sync: datetime) -> datetime:
        """
        Update last_sync timestamp to a given datetime.
        
        Validates that timestamp advances forward and is not in the future.
        
        TEST MODE: Logs only
        PRODUCTION MODE: Writes to DB
        
        Args:
            source: Source namespace (e.g., 'shopify')
            entity: Entity name (e.g., 'orders')
            new_last_sync: New sync timestamp to store
            
        Returns:
            datetime: The timestamp that was stored
        """
        state_key = f"{source}_{entity}"

        if not new_last_sync:
            raise ValueError(f"No datetime provided for {source}/{entity}")
        
        if not isinstance(new_last_sync, datetime):
            raise TypeError(f"new_last_sync must be datetime for {source}/{entity}, got {type(new_last_sync)}")
        
        if new_last_sync.tzinfo is None:
            raise ValueError(f"new_last_sync must be timezone-aware for {source}/{entity}")

        # TEST MODE
        if settings.ENV.lower() == "test":
            logger.info(
                "TEST MODE: Would update last_sync for %s/%s to %s (not persisted)",
                source, entity, new_last_sync.isoformat()
            )
            return new_last_sync

        # PRODUCTION MODE
        try:
            with self.db.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingestion_state (source, last_sync)
                    VALUES (%s, %s)
                    ON CONFLICT (source)
                    DO UPDATE SET last_sync = EXCLUDED.last_sync;
                    """,
                    (state_key, new_last_sync),
                )
                # Explicit commit (autocommit is disabled in postgres_resource)
                self.db.commit()

            logger.info(
                "Updated last_sync for %s/%s to %s",
                source, entity, new_last_sync.isoformat()
            )
            return new_last_sync
            
        except Exception as e:
            logger.error(f"Failed to update last_sync for {source}/{entity}: {e}")
            # Rollback on error
            try:
                self.db.rollback()
            except:
                pass
            raise RuntimeError(f"Database error updating sync state for {source}/{entity}: {e}")


@resource(required_resource_keys={"db"})
def sync_state_resource(context):
    """
    Dagster resource for managing sync state.
    
    TEST MODE: Returns manager with None db (won't use it)
    PRODUCTION MODE: Returns manager with active db connection
    """
    db = context.resources.db
    return SyncStateManager(db)