"""
Core Layer Assets

Models: int_shopify__*, int_klaviyo__* (dbt factory)

Note: Airbyte syncs run independently on their own schedule.
Dagster only handles dbt transforms.
"""
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

from ...resources import DBT_MANIFEST


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
