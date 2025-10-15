# make_s3_loader.py
from dagster import asset, AssetIn
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def make_s3_loader_asset(namespace: str, table: str, updated_field: str):
    upstream_asset_key = f"extract_{namespace}_{table}"
    
    @asset(
        required_resource_keys={"s3_loader"},
        name=f"s3_loader_{namespace}_{table}", 
        group_name=f"{namespace}_{table}",
        ins={"extract_result": AssetIn(upstream_asset_key)},
    )
    def _s3_loader(context, extract_result: dict) -> dict:
        """
        Load extracted records to S3 staging area.
        
        Validates input count and passes schema + field converters to loader.
        """
        # Validate input structure
        if extract_result is None:
            raise ValueError(f"Received None extract_result for {namespace}/{table}")
        if not isinstance(extract_result, dict):
            raise TypeError(f"extract_result must be dict for {namespace}/{table}, got {type(extract_result)}")
        
        if "records" not in extract_result or "row_count" not in extract_result:
            raise ValueError(f"extract_result missing required keys for {namespace}/{table}")
        
        records = extract_result["records"]
        claimed_count = extract_result["row_count"]
        schema = extract_result.get("schema")
        field_converters = extract_result.get("field_converters")  # NEW: Extract converters
        
        # Validate records
        if records is None:
            raise ValueError(f"Received None records for {namespace}/{table}")
        if not isinstance(records, list):
            raise TypeError(f"records must be list for {namespace}/{table}, got {type(records)}")
        
        # ROW COUNT VALIDATION: Input count must match claimed count
        actual_count = len(records)
        if actual_count != claimed_count:
            raise ValueError(
                f"Row count mismatch for {namespace}/{table}: "
                f"claimed={claimed_count}, actual={actual_count}. "
                f"Data loss detected between extraction and S3 load."
            )
        
        # Empty should never happen due to buffer - fail if it does
        if actual_count == 0:
            raise ValueError(
                f"Received empty records for {namespace}/{table}. "
                f"Buffer strategy guarantees at least 1 record - extraction should have failed upstream."
            )
        
        context.log.info(f"✓ Validated input: {actual_count} records match claimed count for {namespace}/{table}")
        
        # Log configuration
        if schema is not None and field_converters is not None:
            context.log.info(
                f"Using professional pipeline: {len(field_converters)} converters + explicit schema for {namespace}/{table}"
            )
        elif schema is not None:
            context.log.info(f"Using explicit schema (no converters) for {namespace}/{table}")
        elif field_converters is not None:
            context.log.info(f"Using {len(field_converters)} converters (no schema) for {namespace}/{table}")
        else:
            context.log.info(f"Using type inference (no schema/converters) for {namespace}/{table}")
        
        context.log.info(f"Loading {actual_count} records for {namespace}/{table} to S3 staging")
        
        # Load to S3 with schema + field converters
        result = context.resources.s3_loader.load(
            namespace, 
            table, 
            records, 
            updated_field,
            schema=schema,
            field_converters=field_converters  # NEW: Pass converters
        )
        
        # Validate output
        if not isinstance(result, dict):
            raise TypeError(f"s3_loader.load must return dict for {namespace}/{table}, got {type(result)}")
        
        required_keys = ["s3_uri", "last_updated_at", "row_count"]
        missing_keys = [k for k in required_keys if k not in result]
        if missing_keys:
            raise ValueError(f"s3_loader result missing keys {missing_keys} for {namespace}/{table}")
        
        s3_uri = result["s3_uri"]
        last_updated_at = result["last_updated_at"]
        row_count = result["row_count"]
        
        # Validate non-empty results have proper data
        if not s3_uri:
            raise ValueError(f"s3_uri is empty for {namespace}/{table}")
        if last_updated_at is None:
            raise ValueError(f"last_updated_at is None for {namespace}/{table}")
        
        # ROW COUNT VALIDATION: Uploaded count must match input count
        if row_count != actual_count:
            raise ValueError(
                f"Row count mismatch for {namespace}/{table}: "
                f"input={actual_count}, uploaded={row_count}. "
                f"Data loss detected during S3 upload."
            )

        context.log.info(f"✓ Validated output: {row_count} rows uploaded to S3 for {namespace}/{table}")
        context.log.info(
            f"S3 load complete: {row_count} rows → {s3_uri}, "
            f"last_{updated_field}={last_updated_at.isoformat()}"
        )
        
        return result

    return _s3_loader
