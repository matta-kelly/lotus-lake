import logging
import psycopg2
from dagster import resource
from shared.config import settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_connection():
    """
    Creates and returns a psycopg2 connection to the PostgreSQL database.
    Uses credentials from the centralized `settings` object.
    
    Note: autocommit is disabled - transactions must be explicitly committed.
    """
    db_user = settings.DB_USER
    db_password = settings.DB_PASSWORD
    db_host = settings.DB_HOST
    db_port = settings.DB_PORT
    db_name = settings.DB_NAME

    logger.debug("Creating psycopg2 connection for database %s at %s:%s",
                 db_name, db_host, db_port)
    
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name,
        sslmode='prefer'
    )
    
    # autocommit disabled - explicit commits required
    # This allows rollback on errors
    logger.debug("Connection created with autocommit=False (explicit commits required)")
    
    return conn


@resource
def postgres_resource(_):
    """
    Dagster resource yielding a psycopg2 connection.
    
    TEST MODE: Returns None (DB not used)
    PRODUCTION MODE: Returns active connection with autocommit disabled
    """
    # TEST MODE: Don't connect, return None
    if settings.ENV.lower() == "test":
        logger.info("TEST MODE: Skipping postgres connection")
        yield None
        return
    
    # PRODUCTION MODE: Normal connection
    conn = get_connection()
    logger.info("Postgres connection created")
    try:
        yield conn
    finally:
        conn.close()
        logger.info("Postgres connection closed")