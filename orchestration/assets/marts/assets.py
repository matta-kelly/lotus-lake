"""
Marts Layer Assets

Models: fct_*, dim_* (dbt factory)

Auto-materializes when upstream core models are updated.
"""
from typing import Any, Mapping, Optional

from dagster import AssetExecutionContext, AutomationCondition
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

from ...resources import DBT_MANIFEST


# =============================================================================
# dbt Mart Models Factory
# =============================================================================

class MartsDbtTranslator(DagsterDbtTranslator):
    """Translator for mart layer models with auto-materialize."""

    def get_group_name(self, dbt_resource_props):
        return "marts"

    def get_automation_condition(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> Optional[AutomationCondition]:
        """Auto-materialize marts when upstream core models update."""
        return AutomationCondition.eager()


@dbt_assets(
    manifest=DBT_MANIFEST,
    select="tag:mart",
    dagster_dbt_translator=MartsDbtTranslator(),
)
def mart_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """All mart layer dbt models - auto-generated from manifest.

    Dependencies auto-wired via dbt ref().
    When any upstream core model materializes, these marts auto-run.
    """
    yield from dbt.cli(["run"], context=context).stream()
