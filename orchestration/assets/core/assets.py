"""
Core Layer Assets

Syncs: shopify_sync, klaviyo_sync (Airbyte)
Models: int_shopify__*, int_klaviyo__* (dbt factory)
"""
from dagster import asset, AssetExecutionContext, Output
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from dagster_airbyte import AirbyteResource

from ...resources import DBT_MANIFEST, SHOPIFY_CONNECTION_ID, KLAVIYO_CONNECTION_ID


# =============================================================================
# Airbyte Sync Assets
# =============================================================================

@asset(group_name="shopify", compute_kind="airbyte")
def shopify_sync(airbyte: AirbyteResource) -> Output[None]:
    """Syncs Shopify data to S3 Parquet via Airbyte."""
    result = airbyte.sync_and_poll(connection_id=SHOPIFY_CONNECTION_ID)
    return Output(None, metadata={"records_synced": result.records_synced})


@asset(group_name="klaviyo", compute_kind="airbyte")
def klaviyo_sync(airbyte: AirbyteResource) -> Output[None]:
    """Syncs Klaviyo data to S3 Parquet via Airbyte."""
    result = airbyte.sync_and_poll(connection_id=KLAVIYO_CONNECTION_ID)
    return Output(None, metadata={"records_synced": result.records_synced})


# =============================================================================
# dbt Core Models Factory
# =============================================================================

class CoreDbtTranslator(DagsterDbtTranslator):
    """Translator for core layer models."""

    def get_group_name(self, dbt_resource_props):
        # Group by source: shopify or klaviyo
        fqn = dbt_resource_props.get("fqn", [])
        if "shopify" in fqn:
            return "shopify"
        elif "klaviyo" in fqn:
            return "klaviyo"
        return "core"


@dbt_assets(
    manifest=DBT_MANIFEST,
    select="tag:core",
    dagster_dbt_translator=CoreDbtTranslator(),
)
def core_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """All core layer dbt models - auto-generated from manifest.

    Includes:
    - int_shopify__orders, int_shopify__refunds, int_shopify__customers
    - int_klaviyo__profiles, int_klaviyo__events
    """
    yield from dbt.cli(["run"], context=context).stream()
