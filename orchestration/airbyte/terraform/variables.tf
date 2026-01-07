# Airbyte
variable "workspace_id" {}
variable "airbyte_server_url" {
  default = "http://localhost:8080/api/"
}
variable "airbyte_username" {}
variable "airbyte_password" {}

# Shopify
variable "shopify_store" {}
variable "shopify_api_password" {}

# Klaviyo
variable "klaviyo_api_key" {}

# MinIO / S3
variable "minio_user" {}
variable "minio_password" {}
variable "minio_endpoint" {}