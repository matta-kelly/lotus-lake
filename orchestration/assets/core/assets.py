"""
Core Layer Assets - Factory Pattern

Each dbt model gets its own @dbt_assets decorator.
Sensors trigger ONLY the specific model that has new data.

Note: Airbyte syncs run independently on their own schedule.
Dagster only handles dbt transforms.
"""
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

from ...resources import DBT_MANIFEST


# =============================================================================
# Stream Discovery (same as sensors.py)
# =============================================================================

STREAMS_DIR = Path(__file__).parent.parent / "streams"


def discover_streams() -> list[tuple[str, str]]:
    """Discover source+stream combos from streams/ directory."""
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
    return streams


# =============================================================================
# dbt Asset Factory - One @dbt_assets per model
# =============================================================================

def make_dbt_asset(source: str, stream: str):
    """Factory to create a @dbt_assets for a single model."""
    tag = f"{source}__{stream}"
    model_name = f"int_{source}__{stream}"

    class ModelTranslator(DagsterDbtTranslator):
        def get_group_name(self, dbt_resource_props):
            return source

    @dbt_assets(
        manifest=DBT_MANIFEST,
        select=f"tag:{tag}",  # Select ONLY this model's tag
        dagster_dbt_translator=ModelTranslator(),
        name=f"{source}_{stream}_dbt",  # Unique name per model
    )
    def _dbt_asset(context: AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["run"], context=context).stream()

    return _dbt_asset


# =============================================================================
# Auto-generated Assets - One per stream
# =============================================================================

_streams = discover_streams()
_assets = [make_dbt_asset(src, strm) for src, strm in _streams]


def get_all_core_assets():
    """Get all auto-discovered dbt assets."""
    return _assets


# For Dagster to discover
__all__ = ["get_all_core_assets"]
