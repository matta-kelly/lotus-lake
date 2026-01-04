resource "airbyte_destination" "s3_data_lake" {
  name                      = "S3 Data Lake"
  workspace_id              = var.workspace_id
  destination_definition_id = "716ca874-520b-4902-9f80-9fad66754b89"
  connection_configuration  = jsonencode({
    access_key_id      = var.minio_user
    secret_access_key  = var.minio_password
    s3_bucket_name     = "landing"
    s3_bucket_region   = "us-east-1"
    s3_endpoint        = var.minio_endpoint
    warehouse_location = "s3://landing/iceberg/"
    main_branch_name   = "main"
    catalog_type = {
      catalog_type = "NESSIE"
      server_uri   = var.nessie_endpoint
      namespace    = "raw"
    }
  })
}