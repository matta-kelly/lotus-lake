resource "airbyte_connection" "shopify_to_minio" {
  source_id                       = airbyte_source.shopify.source_id
  destination_id                  = airbyte_destination.minio_shopify.destination_id
  name                            = "Shopify → MinIO - Shopify"
  status                          = "active"
  namespace_definition            = "destination"
  non_breaking_changes_preference = "propagate_columns"
  notify_schema_changes           = true
  notify_schema_changes_by_email  = false
  prefix                          = ""
  schedule_type                   = "basic"

  schedule_data = {
    basic_schedule = {
      time_unit = "hours"
      units     = 24
    }
  }
}