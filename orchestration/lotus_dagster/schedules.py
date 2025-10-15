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
)

# ============================================================================
# INGESTION SCHEDULES
# ============================================================================

# === HIGH FREQUENCY (Every 5 min) - Events & Orders ===
shopify_orders_ingestion_schedule = ScheduleDefinition(
    job=shopify_orders_ingestion_job,
    cron_schedule="0,5,10,15,20,25,30,35,40,45,50,55 * * * *",  # SALES - Every 5 min at :00
)

klaviyo_email_open_ingestion_schedule = ScheduleDefinition(
    job=klaviyo_email_open_ingestion_job,
    cron_schedule="1,6,11,16,21,26,31,36,41,46,51,56 * * * *",  # Every 5 min at :01
)

klaviyo_email_clicked_ingestion_schedule = ScheduleDefinition(
    job=klaviyo_email_clicked_ingestion_job,
    cron_schedule="2,7,12,17,22,27,32,37,42,47,52,57 * * * *",  # Every 5 min at :02
)

klaviyo_received_email_ingestion_schedule = ScheduleDefinition(
    job=klaviyo_received_email_ingestion_job,
    cron_schedule="3,8,13,18,23,28,33,38,43,48,53,58 * * * *",  # Every 5 min at :03
)

# === MEDIUM FREQUENCY (Every 30 min) - Campaigns ===
klaviyo_campaign_ingestion_schedule = ScheduleDefinition(
    job=klaviyo_campaign_ingestion_job,
    cron_schedule="0,30 * * * *",  # Every 30 min - campaigns change slowly
)

# === LOW FREQUENCY (Hourly) - Products ===
shopify_products_ingestion_schedule = ScheduleDefinition(
    job=shopify_products_ingestion_job,
    cron_schedule="0 * * * *",  # Hourly - catalog changes slowly
)

# ============================================================================
# CORE DBT SCHEDULES - 2-3 min after ingestion, staggered
# ============================================================================

# === HIGH FREQUENCY CORE (Every 5 min) ===
shopify_orders_core_dbt_schedule = ScheduleDefinition(
    job=shopify_orders_core_dbt_job,
    cron_schedule="2,7,12,17,22,27,32,37,42,47,52,57 * * * *",  # 2 min after orders
)

klaviyo_email_open_dbt_schedule = ScheduleDefinition(
    job=klaviyo_email_open_dbt_job,
    cron_schedule="3,8,13,18,23,28,33,38,43,48,53,58 * * * *",  # 2 min after email_open
)

klaviyo_email_clicked_dbt_schedule = ScheduleDefinition(
    job=klaviyo_email_clicked_dbt_job,
    cron_schedule="4,9,14,19,24,29,34,39,44,49,54,59 * * * *",  # 2 min after email_clicked
)

klaviyo_received_email_dbt_schedule = ScheduleDefinition(
    job=klaviyo_received_email_dbt_job,
    cron_schedule="5,10,15,20,25,30,35,40,45,50,55,0 * * * *",  # 2 min after received_email
)

# === MEDIUM FREQUENCY CORE (Every 30 min) ===
klaviyo_campaign_dbt_schedule = ScheduleDefinition(
    job=klaviyo_campaign_dbt_job,
    cron_schedule="3,33 * * * *",  # 3 min after campaign ingestion
)

# === LOW FREQUENCY CORE (Hourly) ===
shopify_products_core_dbt_schedule = ScheduleDefinition(
    job=shopify_products_core_dbt_job,
    cron_schedule="5 * * * *",  # 5 min after products ingestion
)

# ============================================================================
# MART DBT SCHEDULES - Hourly after dependencies
# ============================================================================

shopify_orders_mart_dbt_schedule = ScheduleDefinition(
    job=shopify_orders_mart_dbt_job,
    cron_schedule="15 * * * *",  # Depends on orders core
)

klaviyo_campaign_performance_mart_dbt_schedule = ScheduleDefinition(
    job=klaviyo_campaign_performance_mart_dbt_job,
    cron_schedule="20 * * * *",  # Depends on campaign, email_open, received, clicked core
)

shopify_product_performance_mart_dbt_schedule = ScheduleDefinition(
    job=shopify_product_performance_mart_dbt_job,
    cron_schedule="25 * * * *",  # Depends on orders core + products core
)

shopify_tag_performance_summary_mart_dbt_schedule = ScheduleDefinition(
    job=shopify_tag_performance_summary_mart_dbt_job,
    cron_schedule="30 * * * *",  # Depends on product_performance mart
)