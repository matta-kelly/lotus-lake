# make_flow_asset.py
from .make_get_last_sync import make_get_last_sync_asset
from .make_s3_loader import make_s3_loader_asset
from .make_iceberg_loader import make_iceberg_loader_asset
from .make_update_last_sync import make_update_sync_asset
from .make_extractor import make_extractor_asset  

def make_flow_assets(namespace: str, table: str, updated_field: str, extractor_fn):
    """
    Build a full DAG of assets for a given flow (namespace + table).
    """
    return [
        make_get_last_sync_asset(namespace, table),
        make_extractor_asset(namespace, table, extractor_fn),
        make_s3_loader_asset(namespace, table, updated_field), 
        make_iceberg_loader_asset(namespace, table),
        make_update_sync_asset(namespace, table),
    ]