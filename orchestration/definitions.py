"""
Lotus Lake Dagster Definitions

Asset layers:
- Feeders: One per stream - reads parquet directly, runs dbt, emits AssetMaterialization
- Enriched: dbt models (fct_*) - auto-materializes when processed updates

Sensors:
- One per stream - triggers feeder when new S3 files arrive
- Enriched automation - auto-materializes enriched when processed updates
"""
from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
)

from .assets import (
    feeder_assets,
    feeder_sensors,
    enriched_dbt_models,
    processed_external_assets,
)
from .resources import dbt_resource

# All assets: feeders + processed (external) + enriched
all_assets = [*feeder_assets, *processed_external_assets, enriched_dbt_models]

# Automation sensor for auto-materializing enriched models
automation_sensor = AutomationConditionSensorDefinition(
    name="enriched_automation_sensor",
    target=AssetSelection.groups("enriched"),
    default_status=DefaultSensorStatus.RUNNING,
)

defs = Definitions(
    assets=all_assets,
    sensors=[*feeder_sensors, automation_sensor],
    resources={
        "dbt": dbt_resource,
    },
)
