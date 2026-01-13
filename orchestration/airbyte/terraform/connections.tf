resource "airbyte_connection" "shopify_to_lake" {
  source_id                       = airbyte_source.shopify.source_id
  destination_id                  = airbyte_destination.s3.destination_id
  name                            = "Shopify → S3"
  status                          = "active"
  namespace_definition            = "customformat"
  namespace_format                = "shopify"
  non_breaking_changes_preference = "propagate_columns"
  notify_schema_changes           = true
  notify_schema_changes_by_email  = false
  prefix                          = ""
  schedule_type                   = "cron"

  schedule_data = {
    cron_schedule = {
      cron_expression = "0,20,40 * * * *"
      cron_timezone   = "UTC"
    }
  }

  sync_catalog = jsondecode(file("${path.module}/../../dag/streams/shopify/_catalog.json"))
}

resource "airbyte_connection" "klaviyo_to_lake" {
  source_id                       = airbyte_source.klaviyo.source_id
  destination_id                  = airbyte_destination.s3.destination_id
  name                            = "Klaviyo → S3"
  status                          = "active"
  namespace_definition            = "customformat"
  namespace_format                = "klaviyo"
  non_breaking_changes_preference = "propagate_columns"
  notify_schema_changes           = true
  notify_schema_changes_by_email  = false
  prefix                          = ""
  schedule_type                   = "cron"

  schedule_data = {
    cron_schedule = {
      cron_expression = "5,15,25,35,45,55 * * * *"
      cron_timezone   = "UTC"
    }
  }

  sync_catalog = jsondecode(file("${path.module}/../../dag/streams/klaviyo/_catalog.json"))
}