import os
from pathlib import Path

from dagster_dbt import DbtCliResource

from .airbyte import airbyte_resource, SHOPIFY_CONNECTION_ID, KLAVIYO_CONNECTION_ID

# Paths
DBT_PROJECT_DIR = Path(__file__).parent.parent  # orchestration/
DBT_MANIFEST = DBT_PROJECT_DIR / "target" / "manifest.json"

# Find dbt executable - check venv first, then system
VENV_DBT = DBT_PROJECT_DIR.parent / ".venv" / "bin" / "dbt"
DBT_EXECUTABLE = str(VENV_DBT) if VENV_DBT.exists() else "dbt"

# Resources
dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
    dbt_executable=DBT_EXECUTABLE,
)

__all__ = [
    "airbyte_resource",
    "dbt_resource",
    "SHOPIFY_CONNECTION_ID",
    "KLAVIYO_CONNECTION_ID",
    "DBT_PROJECT_DIR",
    "DBT_MANIFEST",
]
