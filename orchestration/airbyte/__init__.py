"""
Airbyte resource for Dagster.

Includes:
- discover-source.py: Discover available streams from Airbyte sources
- generate-catalog.py: Generate Airbyte catalogs from stream configs
- terraform/: IaC for Airbyte sources, destinations, connections
"""
import os
from pathlib import Path

from dagster_airbyte import AirbyteResource

# Connection IDs (set after terraform apply)
SHOPIFY_CONNECTION_ID = os.getenv("AIRBYTE_SHOPIFY_CONNECTION_ID", "")
KLAVIYO_CONNECTION_ID = os.getenv("AIRBYTE_KLAVIYO_CONNECTION_ID", "")

# Resource
airbyte_resource = AirbyteResource(
    host=os.getenv("AIRBYTE_HOST", "localhost"),
    port=os.getenv("AIRBYTE_PORT", "8001"),
)
