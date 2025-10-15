# make_iceberg_loader.py
from dagster import asset, AssetIn
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def make_iceberg_loader_asset(namespace: str, table: str):
    upstream_asset_key = f"s3_loader_{namespace}_{table}"
    
    @asset(
        required_resource_keys={"loader"},         
        name=f"iceberg_loader_{namespace}_{table}",            
        group_name=f"{namespace}_{table}",
        ins={"s3_loader_result": AssetIn(upstream_asset_key)},
    )
    def _iceberg_loader(context, s3_loader_result: dict) -> dict:
        """
        Load data from S3 staging to Iceberg table.
        
        Validates input count before loading.
        """
        # Validate input
        if not isinstance(s3_loader_result, dict):
            raise TypeError(f"s3_loader must return dict for {namespace}/{table}, got {type(s3_loader_result)}")
        
        required_keys = ["s3_uri", "last_updated_at", "row_count"]
        missing_keys = [k for k in required_keys if k not in s3_loader_result]
        if missing_keys:
            raise ValueError(f"s3_loader_result missing keys {missing_keys} for {namespace}/{table}")
        
        s3_uri = s3_loader_result["s3_uri"]
        last_updated_at = s3_loader_result["last_updated_at"]
        claimed_count = s3_loader_result["row_count"]
        
        # Empty should never happen - fail if it does
        if not s3_uri or claimed_count == 0:
            raise ValueError(
                f"Received empty data for {namespace}.{table}. "
                f"Buffer strategy guarantees data - s3_loader should have failed upstream."
            )

        # Load to Iceberg - IcebergLoader handles retries and errors
        context.log.info(
            f"✓ Validated input: {claimed_count} rows claimed by S3 for {namespace}.{table}"
        )
        context.log.info(
            f"Loading {claimed_count} rows for {namespace}.{table} from {s3_uri} to Iceberg"
        )
        
        result = context.resources.loader.load(namespace, table, s3_uri)
        
        # Validate output
        if not isinstance(result, dict):
            raise TypeError(f"Iceberg loader must return dict for {namespace}/{table}, got {type(result)}")
        
        if "status" not in result:
            raise ValueError(f"Iceberg loader result missing 'status' key for {namespace}/{table}")
        
        if result["status"] != "success":
            raise ValueError(
                f"Iceberg load failed for {namespace}/{table}: "
                f"status={result.get('status')}, message={result.get('message')}"
            )
        
        context.log.info(
            f"✓ Iceberg load complete for {namespace}.{table}: "
            f"status={result['status']}, {claimed_count} rows loaded"
        )

        # Return combined result for downstream assets with row count for validation chain
        return {
            "status": result["status"],
            "last_updated_at": last_updated_at,
            "row_count": claimed_count,
            "message": result.get("message", "Success")
        }

    return _iceberg_loader