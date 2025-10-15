from dagster import AssetSelection, define_asset_job

# ============================================================================
# SHOPIFY JOBS
# ============================================================================

# Shopify Orders Ingestion
shopify_orders_ingestion_job = define_asset_job(
    name="shopify_orders_ingestion_job",
    selection=AssetSelection.groups("shopify_orders"),
)

# Shopify Core dbt
shopify_orders_core_dbt_job = define_asset_job(
    name="shopify_orders_core_dbt_job",
    selection=AssetSelection.groups("shopify_order_core_dbt"),
)

# Shopify Mart dbt
shopify_orders_mart_dbt_job = define_asset_job(
    name="shopify_orders_mart_dbt_job",
    selection=AssetSelection.groups("shopify_sales_summary_mart_dbt"),
)

shopify_products_ingestion_job = define_asset_job(
    name="shopify_products_ingestion_job",
    selection=AssetSelection.groups("shopify_products"),
)

# Shopify Products Core dbt
shopify_products_core_dbt_job = define_asset_job(
    name="shopify_products_core_dbt_job",
    selection=AssetSelection.groups("shopify_products_core_dbt"),
)

# ============================================================================
# KLAVIYO JOBS
# ============================================================================

# Klaviyo Email Open Ingestion
klaviyo_email_open_ingestion_job = define_asset_job(
    name="klaviyo_email_open_ingestion_job",
    selection=AssetSelection.groups("klaviyo_email_open"),
)

# Klaviyo Email Open Core dbt
klaviyo_email_open_dbt_job = define_asset_job(
    name="klaviyo_email_open_dbt_job",
    selection=AssetSelection.groups("klaviyo_email_open_dbt"),
)

# Klaviyo Received Email Ingestion
klaviyo_received_email_ingestion_job = define_asset_job(
    name="klaviyo_received_email_ingestion_job",
    selection=AssetSelection.groups("klaviyo_received_email"),
)

# Klaviyo Received Email Core dbt
klaviyo_received_email_dbt_job = define_asset_job(
    name="klaviyo_received_email_dbt_job",
    selection=AssetSelection.groups("klaviyo_received_email_dbt"),
)

# Klaviyo Campaign Ingestion
klaviyo_campaign_ingestion_job = define_asset_job(
    name="klaviyo_campaign_ingestion_job",
    selection=AssetSelection.groups("klaviyo_campaign"),
)

# Klaviyo Campaign dbt
klaviyo_campaign_dbt_job = define_asset_job(
    name="klaviyo_campaign_dbt_job",
    selection=AssetSelection.groups("klaviyo_campaign_dbt"),
)

# Klaviyo Email Clicked Ingestion
klaviyo_email_clicked_ingestion_job = define_asset_job(
    name="klaviyo_email_clicked_ingestion_job",
    selection=AssetSelection.groups("klaviyo_email_clicked"),
)

# Klaviyo Email Clicked Core dbt
klaviyo_email_clicked_dbt_job = define_asset_job(
    name="klaviyo_email_clicked_dbt_job",
    selection=AssetSelection.groups("klaviyo_email_clicked_dbt"),
)

# Klaviyo Campaign Performance Mart dbt
klaviyo_campaign_performance_mart_dbt_job = define_asset_job(
    name="klaviyo_campaign_performance_mart_dbt_job",
    selection=AssetSelection.groups("klaviyo_campaign_performance_mart_dbt"),
)

# Shopify Product Performance Mart dbt
shopify_product_performance_mart_dbt_job = define_asset_job(
    name="shopify_product_performance_mart_dbt_job",
    selection=AssetSelection.groups("shopify_product_performance_mart_dbt"),
)

# Shopify Tag Performance Summary Mart dbt
shopify_tag_performance_summary_mart_dbt_job = define_asset_job(
    name="shopify_tag_performance_summary_mart_dbt_job",
    selection=AssetSelection.groups("shopify_tag_performance_summary_mart_dbt"),
)
