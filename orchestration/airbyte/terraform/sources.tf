resource "airbyte_source" "shopify" {
  name                 = "Shopify"
  workspace_id         = var.workspace_id
  source_definition_id = "9da77001-af33-4bcd-be46-6252bf9342b9"
  connection_configuration = jsonencode({
    shop       = var.shopify_store
    start_date = "2026-01-01"
    credentials = {
      auth_method  = "api_password"
      api_password = var.shopify_api_password
    }
    bulk_window_in_days                      = 30
    job_checkpoint_interval                  = 100000
    job_termination_threshold                = 7200
    fetch_transactions_user_id               = false
    job_product_variants_include_pres_prices = true
  })
}

resource "airbyte_source" "klaviyo" {
  name                 = "Klaviyo"
  workspace_id         = var.workspace_id
  source_definition_id = "95e8cffd-b8c4-4039-968e-d32fb4a69bde"
  connection_configuration = jsonencode({
    api_key     = var.klaviyo_api_key
    start_date  = "2026-01-01T00:00:00Z"
    num_workers = 10
  })
}