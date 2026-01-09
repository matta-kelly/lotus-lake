"""
Lotus Lake Dagster Definitions

Asset layers:
- Landing: DDL reconciliation (landing_tables)
- Feeders: One per stream - registers files, runs dbt, emits AssetMaterialization
- Processed: dbt models (int_*) - defined for Dagster awareness, executed by feeders
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
    landing_tables,
    feeder_assets,
    feeder_sensors,
    processed_dbt_models,
    enriched_dbt_models,
)
from .resources import dbt_resource

# All assets: landing + feeders + processed + enriched
all_assets = [landing_tables, *feeder_assets, processed_dbt_models, enriched_dbt_models]

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
