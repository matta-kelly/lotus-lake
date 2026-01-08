"""
Lotus Lake Dagster Definitions

Asset layers:
- Core: One dbt asset per stream (int_shopify__orders, int_klaviyo__profiles, etc.)
- Marts: mart_dbt_models (fct_*) - depends on core models via dbt ref()

Sensors:
- Auto-discovered from orchestration/assets/streams/
- Each stream gets a sensor that triggers ONLY that stream's model

Airbyte syncs run independently. Sensors detect new data and trigger dbt.
"""
from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
)

from .assets import core_dbt_assets, mart_dbt_models
from .resources import dbt_resource
from .sensors import get_all_sensors

# Combine all assets: individual core assets + mart asset
all_assets = [*core_dbt_assets, mart_dbt_models]

# Automation sensor for auto-materializing marts when upstreams update
# default_status=RUNNING means it starts enabled without manual UI toggle
automation_sensor = AutomationConditionSensorDefinition(
    name="mart_automation_sensor",
    target=AssetSelection.groups("marts"),
    default_status=DefaultSensorStatus.RUNNING,
)

defs = Definitions(
    assets=all_assets,
    sensors=[*get_all_sensors(), automation_sensor],
    resources={
        "dbt": dbt_resource,
    },
)
