"""
Unified Asset Definitions

Three asset types:
1. Landing - DDL reconciliation (creates/updates DuckLake tables from SQL files)
2. Feeder - one per stream, registers files + runs processed dbt inline
3. Enriched - single asset for all enriched models, auto-materializes on upstream changes

Plus sensors that trigger feeders when new S3 files arrive.
"""
import glob
import re
from pathlib import Path
from typing import Any, Mapping, Optional

import duckdb
from dagster import (
    asset,
    sensor,
    AssetExecutionContext,
    AssetKey,
    AutomationCondition,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    RunsFilter,
    SensorEvaluationContext,
    get_dagster_logger,
)
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

from .resources import DBT_MANIFEST
from .dag.landing.lib import (
    get_ducklake_connection,
    get_cursor,
    get_files_after_cursor,
    set_cursor,
    batch_by_size,
    register_batch,
)


# =============================================================================
# Paths
# =============================================================================

STREAMS_DIR = Path(__file__).parent / "dag" / "streams"
LANDING_DIR = Path(__file__).parent / "dag" / "landing"


# =============================================================================
# Stream Discovery - Single source of truth
# =============================================================================

def discover_streams() -> list[tuple[str, str]]:
    """
    Scan streams/ directory, return [(source, stream), ...]

    streams/shopify/orders.json → ("shopify", "orders")
    """
    if not STREAMS_DIR.exists():
        return []

    streams = []
    for source_dir in STREAMS_DIR.iterdir():
        if not source_dir.is_dir() or source_dir.name.startswith("_"):
            continue
        for stream_file in source_dir.glob("*.json"):
            if stream_file.name.startswith("_"):
                continue
            streams.append((source_dir.name, stream_file.stem))
    return sorted(streams)


# =============================================================================
# Landing Tables - DDL Reconciliation
# =============================================================================

def _parse_table_name(ddl: str) -> str:
    """Extract table name from CREATE TABLE statement."""
    match = re.search(r"CREATE TABLE IF NOT EXISTS\s+(\S+)", ddl, re.IGNORECASE)
    if match:
        return match.group(1)
    raise ValueError(f"Could not parse table name from DDL: {ddl[:100]}...")


def _parse_columns_from_ddl(ddl: str) -> dict[str, str]:
    """Extract column names and types from DDL."""
    match = re.search(r"\(\s*\n?(.*)\s*\);?\s*$", ddl, re.DOTALL)
    if not match:
        return {}

    columns = {}
    content = match.group(1)

    depth = 0
    current = ""
    for char in content:
        if char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
        elif char == "," and depth == 0:
            col_def = current.strip()
            if col_def and not col_def.startswith("--"):
                parts = col_def.split(None, 2)
                if len(parts) >= 2:
                    col_name = parts[0]
                    col_type = parts[1] if len(parts) == 2 else " ".join(parts[1:])
                    if "STRUCT" in col_type.upper() or "VARCHAR" in col_type.upper():
                        col_type = " ".join(parts[1:]) if len(parts) > 2 else parts[1]
                    columns[col_name] = col_type
            current = ""
            continue
        current += char

    col_def = current.strip()
    if col_def and not col_def.startswith("--"):
        parts = col_def.split(None, 2)
        if len(parts) >= 2:
            columns[parts[0]] = " ".join(parts[1:]) if len(parts) > 2 else parts[1]

    return columns


def _get_existing_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> dict[str, str]:
    """Get existing columns from a DuckLake table."""
    try:
        result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        return {row[0]: row[1] for row in result}
    except Exception:
        return {}


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Check if a table exists in DuckLake."""
    try:
        conn.execute(f"DESCRIBE {table_name}")
        return True
    except Exception:
        return False


@asset(group_name="landing")
def landing_tables():
    """
    Reconcile landing DDL files to DuckLake tables.

    - Creates missing tables
    - Adds missing columns to existing tables
    - Idempotent: safe to run on every deploy
    """
    logger = get_dagster_logger()
    conn = get_ducklake_connection()

    conn.execute("CREATE SCHEMA IF NOT EXISTS lakehouse.staging;")
    logger.info("Ensured lakehouse.staging schema exists")

    ddl_files = glob.glob(str(LANDING_DIR / "**/*.sql"), recursive=True)
    logger.info(f"Found {len(ddl_files)} DDL files")

    created = []
    altered = []
    unchanged = []

    for ddl_file in ddl_files:
        with open(ddl_file) as f:
            ddl = f.read()

        table_name = _parse_table_name(ddl)
        logger.info(f"Processing {table_name}")

        if not _table_exists(conn, table_name):
            conn.execute(ddl)
            created.append(table_name)
            logger.info(f"Created {table_name}")
        else:
            existing_cols = _get_existing_columns(conn, table_name)
            ddl_cols = _parse_columns_from_ddl(ddl)
            new_cols = set(ddl_cols.keys()) - set(existing_cols.keys())

            if new_cols:
                for col_name in new_cols:
                    col_type = ddl_cols[col_name]
                    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type};")
                    logger.info(f"Added column {col_name} to {table_name}")
                altered.append(table_name)
            else:
                unchanged.append(table_name)
                logger.info(f"{table_name} unchanged")

    conn.close()

    return {
        "created": created,
        "altered": altered,
        "unchanged": unchanged,
    }


# =============================================================================
# Feeder Factory - One asset per stream
# =============================================================================

def _extract_date_from_path(path: str) -> str:
    """Extract date from hive partition path for logging."""
    import re
    match = re.search(r'year=(\d+)/month=(\d+)/day=(\d+)', path)
    if match:
        return f"{match.group(1)}-{match.group(2).zfill(2)}-{match.group(3).zfill(2)}"
    return path.split('/')[-1]  # fallback to filename


def make_feeder_asset(source: str, stream: str):
    """
    Factory to create a feeder asset for a stream.

    The feeder:
    1. Gets files after cursor (only new files)
    2. Processes in batches until done
    3. Each batch: register files → run processed dbt → update cursor
    """
    asset_name = f"feeder_{source}_{stream}"
    landing_table = f"stg_{source}__{stream}"
    dbt_tag = f"{source}__{stream}"

    @asset(
        name=asset_name,
        group_name="feeders",
        deps=[AssetKey("landing_tables")],
    )
    def _feeder(context: AssetExecutionContext, dbt: DbtCliResource) -> dict:
        conn = get_ducklake_connection()

        # Log current cursor position
        current_cursor = get_cursor(conn, source, stream)
        if current_cursor:
            cursor_date = _extract_date_from_path(current_cursor)
            context.log.info(f"[{source}/{stream}] Current cursor: {cursor_date}")
        else:
            context.log.info(f"[{source}/{stream}] No cursor - starting fresh")

        files = get_files_after_cursor(conn, source, stream)

        if not files:
            conn.close()
            context.log.info(f"[{source}/{stream}] No new files to process")
            return {"status": "no_files", "processed": 0}

        # Log overview of work to do
        first_date = _extract_date_from_path(files[0])
        last_date = _extract_date_from_path(files[-1])
        context.log.info(f"[{source}/{stream}] Found {len(files)} files spanning {first_date} to {last_date}")

        # Pre-calculate batches for progress tracking
        batches = list(batch_by_size(conn, files))
        total_batches = len(batches)
        context.log.info(f"[{source}/{stream}] Will process in {total_batches} batch(es)")

        total_registered = 0
        last_file = None

        for batch_num, batch in enumerate(batches, 1):
            batch_first = _extract_date_from_path(batch[0])
            batch_last = _extract_date_from_path(batch[-1])

            context.log.info(
                f"[{source}/{stream}] Batch {batch_num}/{total_batches}: "
                f"{len(batch)} files ({batch_first} to {batch_last})"
            )

            # Register batch into landing
            registered = register_batch(conn, landing_table, batch)
            total_registered += registered
            context.log.info(
                f"[{source}/{stream}] Registered {registered} files into {landing_table}"
            )

            # Run processed dbt model(s) with this tag
            context.log.info(f"[{source}/{stream}] Running dbt models (tag:{dbt_tag})...")
            dbt_result = dbt.cli(["run", "--select", f"tag:{dbt_tag}"], context=context)

            models_run = []
            for event in dbt_result.stream():
                yield event
                # Try to capture model names from dbt events
                if hasattr(event, 'raw_event'):
                    raw = event.raw_event
                    if isinstance(raw, dict) and raw.get('info', {}).get('name') == 'LogModelResult':
                        model_name = raw.get('data', {}).get('node_info', {}).get('node_name', '')
                        status = raw.get('data', {}).get('status', '')
                        if model_name:
                            models_run.append(f"{model_name}:{status}")

            if models_run:
                context.log.info(f"[{source}/{stream}] dbt results: {', '.join(models_run)}")
            else:
                context.log.info(f"[{source}/{stream}] dbt run complete")

            # Update cursor to last file in batch
            last_file = batch[-1]
            set_cursor(conn, source, stream, last_file)
            new_cursor_date = _extract_date_from_path(last_file)
            context.log.info(
                f"[{source}/{stream}] Cursor updated to {new_cursor_date} "
                f"(batch {batch_num}/{total_batches} complete)"
            )

        conn.close()

        context.log.info(
            f"[{source}/{stream}] COMPLETE: {total_registered} files processed "
            f"in {total_batches} batches, cursor now at {_extract_date_from_path(last_file)}"
        )
        return {
            "status": "complete",
            "processed": total_registered,
            "batches": total_batches,
            "cursor": last_file,
        }

    return _feeder


def make_feeder_sensor(source: str, stream: str):
    """
    Factory to create a sensor that triggers feeder when new files arrive.
    Uses cursor-based check - only looks for files newer than cursor.
    Skips if feeder is already running (no queue buildup).
    """
    sensor_name = f"{source}_{stream}_sensor"
    feeder_key = AssetKey(f"feeder_{source}_{stream}")

    @sensor(
        name=sensor_name,
        asset_selection=[feeder_key],
        default_status=DefaultSensorStatus.RUNNING,
    )
    def _sensor(context: SensorEvaluationContext):
        try:
            # Skip if feeder is already running (check by tag)
            in_progress_runs = context.instance.get_runs(
                filters=RunsFilter(
                    statuses=[
                        DagsterRunStatus.STARTED,
                        DagsterRunStatus.QUEUED,
                    ],
                    tags={"feeder": f"{source}/{stream}"},
                ),
                limit=1,
            )

            if in_progress_runs:
                context.log.info(f"Feeder {source}/{stream} already running, skipping")
                return

            conn = get_ducklake_connection()
            files = get_files_after_cursor(conn, source, stream)
            conn.close()

            if files:
                context.log.info(f"Detected {len(files)} new files for {source}/{stream}")
                yield RunRequest(
                    run_key=f"{source}_{stream}_{context.cursor}",
                    tags={"feeder": f"{source}/{stream}"},
                )

        except Exception as e:
            context.log.error(f"Sensor error for {source}/{stream}: {e}")

    return _sensor


# =============================================================================
# Enriched Factory - Single asset for all enriched models
# =============================================================================

class EnrichedDbtTranslator(DagsterDbtTranslator):
    """Translator for enriched layer models with auto-materialize."""

    def get_group_name(self, dbt_resource_props):
        return "enriched"

    def get_automation_condition(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> Optional[AutomationCondition]:
        return AutomationCondition.eager()


@dbt_assets(
    manifest=DBT_MANIFEST,
    select="tag:enriched",
    dagster_dbt_translator=EnrichedDbtTranslator(),
)
def enriched_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """All enriched layer dbt models - auto-materialize when upstream updates."""
    yield from dbt.cli(["run"], context=context).stream()


# =============================================================================
# Generate all assets and sensors
# =============================================================================

_streams = discover_streams()

feeder_assets = [make_feeder_asset(src, strm) for src, strm in _streams]
feeder_sensors = [make_feeder_sensor(src, strm) for src, strm in _streams]
