"""
Duck Feeder Library

Core functions for the feeder system. No Dagster dependencies.

Cursor-based processing:
- get_cursor / set_cursor: track processing progress
- get_files_after_cursor: only fetch new files (partition-aware)

Batching:
- batch_by_size: chunk files into ~500MB batches
- register_batch: call ducklake_add_data_files for a batch
"""
import os
from typing import Iterator

import duckdb


def get_ducklake_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with DuckLake attached."""
    conn = duckdb.connect(":memory:")

    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    s3_key = os.environ.get("S3_ACCESS_KEY_ID", "")
    s3_secret = os.environ.get("S3_SECRET_ACCESS_KEY", "")
    s3_endpoint = os.environ.get("S3_ENDPOINT", "")

    conn.execute(f"SET s3_access_key_id='{s3_key}';")
    conn.execute(f"SET s3_secret_access_key='{s3_secret}';")
    conn.execute(f"SET s3_endpoint='{s3_endpoint}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")

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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lakehouse.meta.feeder_cursors (
            source VARCHAR,
            stream VARCHAR,
            cursor_path VARCHAR,
            updated_at TIMESTAMP,
            PRIMARY KEY (source, stream)
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
    conn.execute(f"""
        INSERT INTO lakehouse.meta.feeder_cursors (source, stream, cursor_path, updated_at)
        VALUES ('{source}', '{stream}', '{cursor_path}', now())
        ON CONFLICT (source, stream) DO UPDATE SET
            cursor_path = EXCLUDED.cursor_path,
            updated_at = EXCLUDED.updated_at
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

            # Filter to files strictly after cursor (same-day precision)
            files = sorted(f for f in files if f > cursor)
            return files

    # No cursor or couldn't parse - fall back to full glob
    s3_path = f"s3://landing/raw/{source}/{stream}/**/*.parquet"
    s3_files = conn.execute(f"SELECT file FROM glob('{s3_path}')").fetchall()
    return sorted(row[0] for row in s3_files)


# =============================================================================
# Batching
# =============================================================================

def get_file_size(conn: duckdb.DuckDBPyConnection, file_path: str) -> int:
    """Get file size in bytes from parquet metadata."""
    try:
        result = conn.execute(f"""
            SELECT file_size_bytes FROM parquet_file_metadata('{file_path}')
        """).fetchone()
        return result[0] if result else 0
    except Exception:
        return 50_000_000  # 50MB default estimate


def batch_by_size(
    conn: duckdb.DuckDBPyConnection,
    files: list[str],
    max_bytes: int = 500_000_000,  # 500MB
) -> Iterator[list[str]]:
    """
    Group files into batches by cumulative size.

    Iterates through files, accumulating into a batch until
    adding another file would exceed max_bytes. Yields batch,
    starts fresh.
    """
    if not files:
        return

    current_batch = []
    current_size = 0

    for file_path in files:
        file_size = get_file_size(conn, file_path)

        if current_batch and (current_size + file_size) > max_bytes:
            yield current_batch
            current_batch = []
            current_size = 0

        current_batch.append(file_path)
        current_size += file_size

    if current_batch:
        yield current_batch


# =============================================================================
# Registration
# =============================================================================

def register_batch(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    files: list[str],
) -> int:
    """
    Register a batch of files into a DuckLake staging table.

    Calls ducklake_add_data_files for each file. The file metadata
    is added to DuckLake's catalog - the staging table can now
    see those rows. Files are not copied, just registered.

    Returns count of files registered.
    """
    count = 0

    for file_path in files:
        try:
            conn.execute(f"""
                CALL ducklake_add_data_files(
                    'lakehouse',
                    '{table_name}',
                    '{file_path}',
                    schema => 'staging',
                    hive_partitioning => true
                )
            """)
            count += 1
        except Exception as e:
            print(f"Failed to register {file_path}: {e}")

    return count
