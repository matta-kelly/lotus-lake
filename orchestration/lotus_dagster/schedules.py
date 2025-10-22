from dagster import ScheduleDefinition
from .jobs import (
    shopify_orders_ingestion_job,
    shopify_orders_core_dbt_job,
    shopify_orders_mart_dbt_job,
    klaviyo_email_open_ingestion_job,
    klaviyo_email_open_dbt_job,
    klaviyo_received_email_ingestion_job,
    klaviyo_received_email_dbt_job,
    klaviyo_campaign_ingestion_job,
    klaviyo_campaign_dbt_job,
    shopify_products_core_dbt_job,
    shopify_products_ingestion_job,
    klaviyo_email_clicked_ingestion_job,
    klaviyo_email_clicked_dbt_job,
    klaviyo_campaign_performance_mart_dbt_job,
    shopify_product_performance_mart_dbt_job,
    shopify_tag_performance_summary_mart_dbt_job,
    shopify_refunds_ingestion_job,
    shopify_refunds_core_dbt_job,
)

# ============================================================================
# INGESTION SCHEDULES - Events every 7 min, staggered by 2-3 min
# Pattern: Ingestion runs every 7 min, dbt starts 7 min later
# ============================================================================

# === HIGH FREQUENCY - Event Ingestions (Every 7 min, offset by 2-3 min) ===
klaviyo_email_open_ingestion_schedule = ScheduleDefinition(
    job=klaviyo_email_open_ingestion_job,
    cron_schedule="0,7,14,21,28,35,42,49,56 * * * *",  # Opens: every 7 min starting :00
)

klaviyo_email_clicked_ingestion_schedule = ScheduleDefinition(
    job=klaviyo_email_clicked_ingestion_job,
    cron_schedule="2,9,16,23,30,37,44,51,58 * * * *",  # Clicks: every 7 min starting :02
)

klaviyo_received_email_ingestion_schedule = ScheduleDefinition(
    job=klaviyo_received_email_ingestion_job,
    cron_schedule="4,11,18,25,32,39,46,53 * * * *",  # Receives: every 7 min starting :04
)

# === MEDIUM-HIGH FREQUENCY - Orders (Every 10 min) ===
shopify_orders_ingestion_schedule = ScheduleDefinition(
    job=shopify_orders_ingestion_job,
    cron_schedule="1,11,21,31,41,51 * * * *",  # Orders: every 10 min at :01
)

# === MEDIUM FREQUENCY - Campaigns (Every 30 min) ===
klaviyo_campaign_ingestion_schedule = ScheduleDefinition(
    job=klaviyo_campaign_ingestion_job,
    cron_schedule="5,35 * * * *",  # Campaigns: :05, :35
)

# === LOW FREQUENCY - Products (Hourly) ===
shopify_products_ingestion_schedule = ScheduleDefinition(
    job=shopify_products_ingestion_job,
    cron_schedule="3 * * * *",  # Products: :03 every hour
)

# === MEDIUM FREQUENCY - Refunds (Every 15 min) ===
shopify_refunds_ingestion_schedule = ScheduleDefinition(
    job=shopify_refunds_ingestion_job,
    cron_schedule="6,21,36,51 * * * *",  # Refunds: every 15 min at :06
)
# ============================================================================
# CORE DBT SCHEDULES - 7 min after corresponding ingestion
# ============================================================================

# === HIGH FREQUENCY - Event DBT (7 min after ingestion) ===
klaviyo_email_open_dbt_schedule = ScheduleDefinition(
    job=klaviyo_email_open_dbt_job,
    cron_schedule="7,14,21,28,35,42,49,56,3 * * * *",  # 7 min after opens
)

klaviyo_email_clicked_dbt_schedule = ScheduleDefinition(
    job=klaviyo_email_clicked_dbt_job,
    cron_schedule="9,16,23,30,37,44,51,58,5 * * * *",  # 7 min after clicks
)

klaviyo_received_email_dbt_schedule = ScheduleDefinition(
    job=klaviyo_received_email_dbt_job,
    cron_schedule="11,18,25,32,39,46,53,0 * * * *",  # 7 min after receives
)

# === MEDIUM-HIGH FREQUENCY - Orders DBT (7 min after ingestion) ===
shopify_orders_core_dbt_schedule = ScheduleDefinition(
    job=shopify_orders_core_dbt_job,
    cron_schedule="8,18,28,38,48,58 * * * *",  # 7 min after orders
)

# === MEDIUM FREQUENCY - Campaigns DBT (7 min after ingestion) ===
klaviyo_campaign_dbt_schedule = ScheduleDefinition(
    job=klaviyo_campaign_dbt_job,
    cron_schedule="12,42 * * * *",  # 7 min after campaigns
)

# === LOW FREQUENCY - Products DBT (7 min after ingestion) ===
shopify_products_core_dbt_schedule = ScheduleDefinition(
    job=shopify_products_core_dbt_job,
    cron_schedule="10 * * * *",  # 7 min after products
)

# === MEDIUM FREQUENCY - Refunds DBT (7 min after ingestion) ===
shopify_refunds_core_dbt_schedule = ScheduleDefinition(
    job=shopify_refunds_core_dbt_job,
    cron_schedule="13,28,43,58 * * * *",  # 7 min after refunds
)

# ============================================================================
# MART DBT SCHEDULES - Hourly after all dependencies complete
# ============================================================================

shopify_orders_mart_dbt_schedule = ScheduleDefinition(
    job=shopify_orders_mart_dbt_job,
    cron_schedule="15 * * * *",  # :15 - depends on orders core
)

klaviyo_campaign_performance_mart_dbt_schedule = ScheduleDefinition(
    job=klaviyo_campaign_performance_mart_dbt_job,
    cron_schedule="25 * * * *",  # :25 - depends on campaign + all 3 event cores
)

shopify_product_performance_mart_dbt_schedule = ScheduleDefinition(
    job=shopify_product_performance_mart_dbt_job,
    cron_schedule="35 * * * *",  # :35 - depends on orders core + products core
)

shopify_tag_performance_summary_mart_dbt_schedule = ScheduleDefinition(
    job=shopify_tag_performance_summary_mart_dbt_job,
    cron_schedule="45 * * * *",  # :45 - depends on product_performance mart
)

