variable "workspace_id" {}
variable "airbyte_username" {}
variable "airbyte_password" {}
variable "shopify_api_password" {}
variable "shopify_store" {}
variable "klaviyo_api_key" {}
variable "minio_user" {}
variable "minio_password" {}
variable "minio_endpoint" {}
variable "nessie_endpoint" {
  type        = string
  description = "Nessie catalog endpoint"
}