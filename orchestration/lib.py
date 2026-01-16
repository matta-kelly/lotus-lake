"""
Feeder Library

Core functions for the feeder system. No Dagster dependencies.

Cursor-based processing:
- get_cursor / set_cursor: track processing progress
- get_files_after_cursor: only fetch new files (partition-aware)
"""
import os
import re

import duckdb


def _natural_sort_key(s: str) -> list:
    """Sort key that handles numbers naturally (part_9 < part_10)."""
    return [int(c) if c.isdigit() else c for c in re.split(r'(\d+)', s)]


def get_ducklake_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with DuckLake attached."""
    conn = duckdb.connect(":memory:")

    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    s3_key = os.environ.get("S3_ACCESS_KEY_ID", "")
    s3_secret = os.environ.get("S3_SECRET_ACCESS_KEY", "")
    s3_endpoint = os.environ.get("S3_ENDPOINT", "")

    # DuckLake uses CREATE SECRET, not SET s3_* commands
    conn.execute(f"""
        CREATE SECRET s3_secret (
            TYPE S3,
            KEY_ID '{s3_key}',
            SECRET '{s3_secret}',
            ENDPOINT '{s3_endpoint}',
            USE_SSL false,
            URL_STYLE 'path'
        )
    """)

    pg_host = os.environ.get("DUCKLAKE_DB_HOST", "ducklake-db-rw.lotus-lake.svc.cluster.local")
    pg_pass = os.environ.get("DUCKLAKE_DB_PASSWORD", "")

    conn.execute(f"""
        ATTACH 'ducklake:postgres:host={pg_host} port=5432 dbname=ducklake user=ducklake password={pg_pass}'
        AS lakehouse (DATA_PATH 's3://landing/raw/')
    """)

    return conn


# =============================================================================
# Cursor Management
# =============================================================================

def ensure_cursor_table(conn: duckdb.DuckDBPyConnection):
    """Create cursor tracking table if it doesn't exist."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS lakehouse.meta;")
    # Note: DuckLake doesn't support PRIMARY KEY constraints
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lakehouse.meta.feeder_cursors (
            source VARCHAR,
            stream VARCHAR,
            cursor_path VARCHAR,
            updated_at TIMESTAMP
        )
    """)


def get_cursor(conn: duckdb.DuckDBPyConnection, source: str, stream: str) -> str | None:
    """Get the last processed file path for a stream."""
    ensure_cursor_table(conn)
    result = conn.execute(f"""
        SELECT cursor_path FROM lakehouse.meta.feeder_cursors
        WHERE source = '{source}' AND stream = '{stream}'
    """).fetchone()
    return result[0] if result else None


def set_cursor(conn: duckdb.DuckDBPyConnection, source: str, stream: str, cursor_path: str):
    """Update the cursor to the last processed file path."""
    ensure_cursor_table(conn)
    # DuckLake doesn't support ON CONFLICT, so delete then insert
    conn.execute(f"""
        DELETE FROM lakehouse.meta.feeder_cursors
        WHERE source = '{source}' AND stream = '{stream}'
    """)
    conn.execute(f"""
        INSERT INTO lakehouse.meta.feeder_cursors (source, stream, cursor_path, updated_at)
        VALUES ('{source}', '{stream}', '{cursor_path}', now())
    """)


# =============================================================================
# File Discovery (Cursor-Based, Partition-Aware)
# =============================================================================

def _parse_partition_date(path: str) -> tuple[int, int, int] | None:
    """
    Extract year, month, day from hive-partitioned path.

    Path format: .../year=2024/month=06/day=15/...
    Returns (2024, 6, 15) or None if parsing fails.
    """
    import re
    match = re.search(r'year=(\d+)/month=(\d+)/day=(\d+)', path)
    if match:
        return int(match.group(1)), int(match.group(2)), int(match.group(3))
    return None


def _generate_partition_paths(
    source: str,
    stream: str,
    start_date: tuple[int, int, int],
    end_date: tuple[int, int, int] | None = None,
) -> list[str]:
    """
    Generate partition glob paths from start_date to end_date (or today).

    Returns list of paths like:
    s3://landing/raw/shopify/orders/year=2024/month=06/day=15/*.parquet
    """
    from datetime import date, timedelta

    start = date(start_date[0], start_date[1], start_date[2])

    if end_date:
        end = date(end_date[0], end_date[1], end_date[2])
    else:
        end = date.today()

    paths = []
    current = start

    while current <= end:
        path = (
            f"s3://landing/raw/{source}/{stream}/"
            f"year={current.year}/month={current.month:02d}/day={current.day:02d}/*.parquet"
        )
        paths.append(path)
        current += timedelta(days=1)

    return paths


def get_files_after_cursor(
    conn: duckdb.DuckDBPyConnection,
    source: str,
    stream: str,
) -> list[str]:
    """
    Get S3 files newer than the cursor.

    Partition-aware: only globs partitions from cursor date forward,
    avoiding full scan of historical data.
    """
    cursor = get_cursor(conn, source, stream)

    if cursor:
        # Parse date from cursor path
        cursor_date = _parse_partition_date(cursor)

        if cursor_date:
            # Only glob from cursor date forward
            partition_paths = _generate_partition_paths(source, stream, cursor_date)

            files = []
            for path in partition_paths:
                try:
                    result = conn.execute(f"SELECT file FROM glob('{path}')").fetchall()
                    files.extend(row[0] for row in result)
                except Exception:
                    # Partition might not exist yet
                    pass

            # Filter to files strictly after cursor (natural sort for part_9 < part_10)
            cursor_key = _natural_sort_key(cursor)
            files = sorted([f for f in files if _natural_sort_key(f) > cursor_key], key=_natural_sort_key)
            return files

    # No cursor or couldn't parse - fall back to full glob
    s3_path = f"s3://landing/raw/{source}/{stream}/**/*.parquet"
    s3_files = conn.execute(f"SELECT file FROM glob('{s3_path}')").fetchall()
    return sorted((row[0] for row in s3_files), key=_natural_sort_key)


