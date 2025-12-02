resource "airbyte_destination" "minio_landing" {
  name                        = "MinIO Landing"
  workspace_id                = var.workspace_id
  destination_definition_id   = "4816b78f-1489-44c1-9060-4b19d5fa9362"
  connection_configuration = jsonencode({
    s3_bucket_region  = "us-east-1"
    s3_endpoint       = var.minio_endpoint
    s3_bucket_name    = "landing"
    s3_bucket_path    = "shopify"
    access_key_id     = var.minio_user
    secret_access_key = var.minio_password
    format = {
      format_type            = "Parquet"
      compression_codec      = "UNCOMPRESSED"
      block_size_mb          = 128
      page_size_kb           = 1024
      dictionary_page_size_kb = 1024
      max_padding_size_mb    = 8
    }
  })
}