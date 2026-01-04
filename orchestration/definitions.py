"""
Lotus Lake Dagster Definitions

Asset layers:
- Core: shopify_sync, klaviyo_sync → core_dbt_models (int_shopify__*, int_klaviyo__*)
- Marts: mart_dbt_models (fct_*) - depends on core models via dbt ref()

Dependencies are auto-wired by dbt manifest. Each @dbt_assets factory
generates assets from the manifest filtered by tag.
"""
from dagster import Definitions, load_assets_from_modules

from . import assets
from .resources import airbyte_resource, dbt_resource

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "airbyte": airbyte_resource,
        "dbt": dbt_resource,
    },
)
