"""
Marts Layer Assets

Models: fct_*, dim_* (dbt factory)
"""
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

from ...resources import DBT_MANIFEST


# =============================================================================
# dbt Mart Models Factory
# =============================================================================

class MartsDbtTranslator(DagsterDbtTranslator):
    """Translator for mart layer models."""

    def get_group_name(self, dbt_resource_props):
        return "marts"


@dbt_assets(
    manifest=DBT_MANIFEST,
    select="tag:mart",
    dagster_dbt_translator=MartsDbtTranslator(),
)
def mart_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """All mart layer dbt models - auto-generated from manifest.

    Includes:
    - fct_sales

    Dependencies auto-wired via dbt ref().
    """
    yield from dbt.cli(["run"], context=context).stream()
