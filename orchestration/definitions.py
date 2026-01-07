"""
Lotus Lake Dagster Definitions

Asset layers:
- Core: core_dbt_models (int_shopify__*, int_klaviyo__*)
- Marts: mart_dbt_models (fct_*) - depends on core models via dbt ref()

Sensors:
- Auto-discovered from orchestration/assets/streams/
- Each source folder gets a sensor that polls S3 for new data

Airbyte syncs run independently. Sensors detect new data and trigger dbt.
"""
from dagster import Definitions, load_assets_from_modules

from . import assets
from .resources import dbt_resource
from .sensors import get_all_sensors

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    sensors=get_all_sensors(),
    resources={
        "dbt": dbt_resource,
    },
)
