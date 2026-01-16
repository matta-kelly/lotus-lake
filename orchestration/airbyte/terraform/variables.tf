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

# S3 (SeaweedFS)
variable "s3_access_key_id" {}
variable "s3_secret_access_key" {}
variable "s3_endpoint" {}