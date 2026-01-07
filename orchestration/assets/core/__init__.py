from .assets import get_all_core_assets

# Get all factory-generated dbt assets (one per stream)
core_dbt_assets = get_all_core_assets()

__all__ = [
    "core_dbt_assets",
]
