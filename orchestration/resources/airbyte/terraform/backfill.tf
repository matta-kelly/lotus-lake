# Backfill support - triggers connection reset when backfill: true in stream configs

locals {
  # Read all stream configs and check for backfill flags
  shopify_stream_files = fileset("${path.module}/../../../assets/streams/shopify", "*.json")
  klaviyo_stream_files = fileset("${path.module}/../../../assets/streams/klaviyo", "*.json")

  shopify_needs_backfill = anytrue([
    for f in local.shopify_stream_files :
    f != "_catalog.json" ? try(jsondecode(file("${path.module}/../../../assets/streams/shopify/${f}")).backfill, false) : false
  ])

  klaviyo_needs_backfill = anytrue([
    for f in local.klaviyo_stream_files :
    f != "_catalog.json" ? try(jsondecode(file("${path.module}/../../../assets/streams/klaviyo/${f}")).backfill, false) : false
  ])
}

# Reset Shopify connection when any stream has backfill: true
resource "null_resource" "shopify_backfill" {
  count = local.shopify_needs_backfill ? 1 : 0

  triggers = {
    # Re-trigger on every apply when backfill is true
    timestamp = timestamp()
  }

  provisioner "local-exec" {
    command = <<-EOT
      curl -s -X POST "http://localhost:8080/api/v1/connections/reset" \
        -u "${var.airbyte_username}:${var.airbyte_password}" \
        -H "Content-Type: application/json" \
        -d '{"connectionId": "${airbyte_connection.shopify_to_lake.connection_id}"}'
      echo "Triggered backfill for Shopify connection"
    EOT
  }

  depends_on = [airbyte_connection.shopify_to_lake]
}

# Reset Klaviyo connection when any stream has backfill: true
resource "null_resource" "klaviyo_backfill" {
  count = local.klaviyo_needs_backfill ? 1 : 0

  triggers = {
    timestamp = timestamp()
  }

  provisioner "local-exec" {
    command = <<-EOT
      curl -s -X POST "http://localhost:8080/api/v1/connections/reset" \
        -u "${var.airbyte_username}:${var.airbyte_password}" \
        -H "Content-Type: application/json" \
        -d '{"connectionId": "${airbyte_connection.klaviyo_to_lake.connection_id}"}'
      echo "Triggered backfill for Klaviyo connection"
    EOT
  }

  depends_on = [airbyte_connection.klaviyo_to_lake]
}

# Output to show backfill status
output "backfill_status" {
  value = {
    shopify_backfill_triggered = local.shopify_needs_backfill
    klaviyo_backfill_triggered = local.klaviyo_needs_backfill
  }
}
