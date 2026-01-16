"""
Asset Definitions

Two asset types:
1. Feeder - one per stream, reads parquet directly, runs dbt, emits AssetMaterialization
2. Enriched - dbt models (fct_*) auto-materialize when processed updates

Feeders:
- Loop through files one at a time
- Pass file path to dbt as a var
- dbt reads parquet directly via read_parquet()
- Advance cursor after each file

Plus sensors that trigger feeders when new S3 files arrive.
"""
import json
import re
from pathlib import Path
from typing import Any, Mapping, Optional

from dagster import (
    asset,
    sensor,
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetSpec,
    AutomationCondition,
    DagsterRunStatus,
    DefaultSensorStatus,
    external_assets_from_specs,
    Output,
    RunRequest,
    RunsFilter,
    SensorEvaluationContext,
)
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

from .resources import DBT_MANIFEST
from .lib import (
    get_ducklake_connection,
    get_cursor,
    get_files_after_cursor,
    set_cursor,
)


# =============================================================================
# Paths
# =============================================================================

STREAMS_DIR = Path(__file__).parent / "dag" / "streams"


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
    2. Loops through files one at a time
    3. Each file: pass to dbt as var → dbt reads parquet directly → update cursor
    """
    asset_name = f"{source}_{stream}"
    dbt_tag = f"{source}__{stream}"

    @asset(
        name=asset_name,
        group_name="feeders",
    )
    def _feeder(context: AssetExecutionContext, dbt: DbtCliResource):
        conn = get_ducklake_connection()

        # Log current cursor position
        current_cursor = get_cursor(conn, source, stream)
        if current_cursor:
            cursor_file = current_cursor.split("/")[-1]
            cursor_date = _extract_date_from_path(current_cursor)
            context.log.info(f"[{source}/{stream}] Current cursor: {cursor_date} ({cursor_file})")
        else:
            context.log.info(f"[{source}/{stream}] No cursor - will process all files")

        files = get_files_after_cursor(conn, source, stream)

        if not files:
            conn.close()
            context.log.info(f"[{source}/{stream}] No new files to process")
            yield Output({"status": "no_files", "processed": 0})
            return

        # Log overview of work to do
        first_date = _extract_date_from_path(files[0])
        last_date = _extract_date_from_path(files[-1])
        context.log.info(f"[{source}/{stream}] Found {len(files)} files spanning {first_date} to {last_date}")

        # Batch files to reduce dbt startup overhead
        # DuckDB streams through files one at a time - memory stays bounded
        BATCH_SIZE = 10
        total_files = len(files)
        processed_count = 0
        last_file = None

        for batch_start in range(0, total_files, BATCH_SIZE):
            batch = files[batch_start:batch_start + BATCH_SIZE]
            batch_num = (batch_start // BATCH_SIZE) + 1
            total_batches = (total_files + BATCH_SIZE - 1) // BATCH_SIZE

            first_date = _extract_date_from_path(batch[0])
            last_date = _extract_date_from_path(batch[-1])
            context.log.info(f"[{source}/{stream}] Batch {batch_num}/{total_batches}: {len(batch)} files ({first_date} to {last_date})")

            # Run dbt with file list - read_parquet streams through one at a time
            vars_json = json.dumps({"files": batch})
            dbt_result = dbt.cli(["run", "--select", f"tag:{dbt_tag}", "--vars", vars_json])

            models_succeeded = []
            models_failed = []
            for event in dbt_result.stream_raw_events():
                raw = event.raw_event
                if raw.get('info', {}).get('name') == 'LogModelResult':
                    model_name = raw.get('data', {}).get('node_info', {}).get('node_name', '')
                    status = raw.get('data', {}).get('status', '')
                    if model_name and status.lower() in ('success', 'ok'):
                        models_succeeded.append(model_name)
                    elif model_name:
                        models_failed.append(f"{model_name}:{status}")

            if models_succeeded:
                context.log.info(f"[{source}/{stream}] dbt succeeded: {', '.join(models_succeeded)}")
            if models_failed:
                context.log.error(f"[{source}/{stream}] dbt failed: {', '.join(models_failed)}")
                raise Exception(f"dbt failed for {source}/{stream}: {', '.join(models_failed)}")

            # Emit AssetMaterialization for each successful model
            # Key must match dagster-dbt's asset key format (just model name, no schema prefix)
            for model_name in models_succeeded:
                asset_key = AssetKey([model_name])
                yield AssetMaterialization(
                    asset_key=asset_key,
                    metadata={
                        "source": source,
                        "stream": stream,
                        "batch_num": batch_num,
                        "total_batches": total_batches,
                        "files_in_batch": len(batch),
                    }
                )

            # Update cursor to last file in batch
            last_file = batch[-1]
            set_cursor(conn, source, stream, last_file)
            processed_count += len(batch)
            context.log.info(f"[{source}/{stream}] Batch {batch_num}/{total_batches} complete, cursor at {last_file.split('/')[-1]}")

        conn.close()

        final_cursor_file = last_file.split("/")[-1]
        context.log.info(f"[{source}/{stream}] COMPLETE: {processed_count} files, cursor now at {final_cursor_file}")
        yield Output({
            "status": "complete",
            "processed": processed_count,
            "cursor": last_file,
        })

    return _feeder


def make_feeder_sensor(source: str, stream: str):
    """
    Factory to create a sensor that triggers feeder when new files arrive.
    Uses cursor-based check - only looks for files newer than cursor.
    Skips if feeder is already running (no queue buildup).
    """
    sensor_name = f"{source}_{stream}_sensor"
    feeder_key = AssetKey(f"{source}_{stream}")

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
                latest_file = files[-1]
                context.log.info(f"Detected {len(files)} new files for {source}/{stream}, latest: {latest_file}")
                yield RunRequest(
                    run_key=f"{source}/{stream}/{latest_file}",
                    tags={"feeder": f"{source}/{stream}"},
                )

        except Exception as e:
            context.log.error(f"Sensor error for {source}/{stream}: {e}")

    return _sensor


# =============================================================================
# Enriched dbt Assets - Auto-materializes when processed updates
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
# Processed External Assets - Required for enriched auto-materialize
# =============================================================================

def get_processed_model_specs() -> list[AssetSpec]:
    """
    Create AssetSpecs for processed models (int_*) from dbt manifest.
    These external assets let enriched models depend on them via AutomationCondition.
    Feeders emit AssetMaterialization events that Dagster recognizes as materializations
    of these external assets.
    """
    manifest_path = DBT_MANIFEST
    if not manifest_path.exists():
        return []

    with open(manifest_path) as f:
        manifest = json.load(f)

    specs = []
    for key, node in manifest["nodes"].items():
        if node["resource_type"] == "model" and node["name"].startswith("int_"):
            specs.append(
                AssetSpec(
                    key=AssetKey([node["name"]]),
                    group_name="processed",
                )
            )
    return specs


processed_external_assets = external_assets_from_specs(get_processed_model_specs())


# =============================================================================
# Generate all assets and sensors
# =============================================================================

_streams = discover_streams()

feeder_assets = [make_feeder_asset(src, strm) for src, strm in _streams]
feeder_sensors = [make_feeder_sensor(src, strm) for src, strm in _streams]
