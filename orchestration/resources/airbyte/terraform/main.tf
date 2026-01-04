terraform {
  required_providers {
    airbyte = {
      source  = "aballiet/airbyte-oss"
      version = "1.2.3"
    }
  }
}

provider "airbyte" {
  server_url = "http://localhost:8080/api/"
  username   = var.airbyte_username
  password   = var.airbyte_password
}