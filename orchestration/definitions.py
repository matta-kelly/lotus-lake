"""
Lotus Lake Dagster Definitions

Asset layers:
- DLT Ingestion: Scheduled extraction from custom APIs → S3 parquet
- Feeders: One per stream - reads parquet directly, runs dbt, emits AssetMaterialization
- Enriched: dbt models (fct_*) - auto-materializes when processed updates

Sensors:
- DLT automation - runs dlt extraction on cron schedule
- Feeder sensors - triggers feeder when new S3 files arrive
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
    dlt_assets,
    enriched_dbt_models,
    processed_asset_specs,
)
from .resources import dbt_resource

# All assets: dlt extraction + feeders + processed (external) + enriched
all_assets = [*dlt_assets, *feeder_assets, *processed_asset_specs, enriched_dbt_models]

# Automation sensor for dlt extraction (cron-scheduled)
dlt_automation_sensor = AutomationConditionSensorDefinition(
    name="dlt_automation_sensor",
    target=AssetSelection.groups("dlt_ingestion"),
    default_status=DefaultSensorStatus.RUNNING,
)

# Automation sensor for auto-materializing enriched models
enriched_automation_sensor = AutomationConditionSensorDefinition(
    name="enriched_automation_sensor",
    target=AssetSelection.groups("enriched"),
    default_status=DefaultSensorStatus.RUNNING,
)

defs = Definitions(
    assets=all_assets,
    sensors=[*feeder_sensors, dlt_automation_sensor, enriched_automation_sensor],
    resources={
        "dbt": dbt_resource,
    },
)
