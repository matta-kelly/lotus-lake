from dagster import Definitions
from shared.config import settings

# ============================================================================
# ASSETS
# ============================================================================

# Ingestion flows
from .assets.ingest.shopify.orders import build_orders_assets
from .assets.ingest.shopify.products import build_products_assets
from .assets.ingest.shopify.returns import build_refunds_assets
from .assets.ingest.klaviyo.email_open import build_email_open_assets
from .assets.ingest.klaviyo.received_email import build_received_email_assets
from .assets.ingest.klaviyo.campaigns import build_campaign_assets
from .assets.ingest.klaviyo.email_clicked import build_email_clicked_assets



# dbt transformations
from .assets.model.dbt_assets import (
    shopify_orders_core_dbt_models,
    shopify_products_core_dbt_models,
    shopify_orders_mart_dbt_models,
    klaviyo_email_open_core_dbt_models,
    klaviyo_received_email_core_dbt_models,
    klaviyo_campaign_core_dbt_models,
    klaviyo_email_clicked_core_dbt_models,
    klaviyo_campaign_performance_mart_dbt_models,
    shopify_tag_performance_summary_mart_dbt_models,
    shopify_product_performance_mart_dbt_models, 
    shopify_refunds_core_dbt_models,
)

# Resources
from .resources.last_sync.postgres_resource import postgres_resource
from .resources.last_sync.last_sync import sync_state_resource
from .resources.load.load_s3 import s3_client_resource, s3_loader_resource
from .resources.load.iceberg_loader import iceberg_loader_resource
from .resources.connectors.shop_resource import shopify_client_resource
from .resources.connectors.klaviyo_resource import klaviyo_client_resource

# Jobs and schedules
from .schedules import (
    shopify_orders_ingestion_schedule,
    shopify_orders_core_dbt_schedule,
    shopify_orders_mart_dbt_schedule,
    klaviyo_email_open_ingestion_schedule,
    klaviyo_email_open_dbt_schedule,
    klaviyo_received_email_ingestion_schedule,
    klaviyo_received_email_dbt_schedule,
    klaviyo_campaign_ingestion_schedule,
    klaviyo_campaign_dbt_schedule,
    shopify_products_ingestion_schedule,
    shopify_products_core_dbt_schedule,
    klaviyo_email_clicked_ingestion_schedule,
    klaviyo_email_clicked_dbt_schedule,
    klaviyo_campaign_performance_mart_dbt_schedule,
    shopify_product_performance_mart_dbt_schedule,
    shopify_tag_performance_summary_mart_dbt_schedule,
    shopify_refunds_ingestion_schedule,
    shopify_refunds_core_dbt_schedule,  
)

# ============================================================================
# DEFINITIONS
# ============================================================================

# Build all assets from their respective factory functions
all_ingestion_assets = [
    *build_orders_assets(),
    *build_products_assets(),
    *build_email_open_assets(),
    *build_received_email_assets(),
    *build_campaign_assets(),
    *build_email_clicked_assets(),
    *build_refunds_assets(),
]
all_dbt_assets = [
    shopify_orders_core_dbt_models,
    shopify_products_core_dbt_models,
    shopify_orders_mart_dbt_models,
    klaviyo_email_open_core_dbt_models,
    klaviyo_received_email_core_dbt_models,
    klaviyo_campaign_core_dbt_models,
    klaviyo_email_clicked_core_dbt_models,
    klaviyo_campaign_performance_mart_dbt_models,
    shopify_tag_performance_summary_mart_dbt_models,
    shopify_product_performance_mart_dbt_models,
    shopify_refunds_core_dbt_models, 
]

defs = Definitions(
    assets=[
        *all_ingestion_assets,
        *all_dbt_assets,
    ],
    schedules=[
        shopify_orders_ingestion_schedule,
        shopify_orders_core_dbt_schedule,
        shopify_orders_mart_dbt_schedule,
        shopify_products_ingestion_schedule,
        shopify_products_core_dbt_schedule,
        klaviyo_email_open_ingestion_schedule,
        klaviyo_email_open_dbt_schedule,
        klaviyo_received_email_ingestion_schedule,
        klaviyo_received_email_dbt_schedule,
        klaviyo_campaign_ingestion_schedule,
        klaviyo_campaign_dbt_schedule,
        klaviyo_email_clicked_ingestion_schedule,
        klaviyo_email_clicked_dbt_schedule,
        klaviyo_campaign_performance_mart_dbt_schedule,
        shopify_product_performance_mart_dbt_schedule,
        shopify_tag_performance_summary_mart_dbt_schedule,
        shopify_refunds_ingestion_schedule,
        shopify_refunds_core_dbt_schedule,  
    ],
    resources={
        "db": postgres_resource,
        "sync_state": sync_state_resource,
        "s3_client": s3_client_resource,
        "s3_loader": s3_loader_resource,
        "loader": iceberg_loader_resource,
        "shopify": shopify_client_resource,
        "klaviyo": klaviyo_client_resource.configured(
            {"api_key": settings.KLAVIYO_API_KEY}
        ),
    },
)