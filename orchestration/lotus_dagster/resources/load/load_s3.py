import logging
import os
from datetime import datetime, timezone
from typing import List, Dict, Optional, Callable
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.exceptions import ClientError
from dagster import resource
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class S3Loader:
    """Loads extracted data to S3/MinIO in Parquet format with type-safe conversion."""

    def __init__(self, s3_client):
        self.s3 = s3_client

    def load(
        self, 
        source: str, 
        entity: str, 
        data: List[Dict], 
        updated_field: str = "updatedAt",
        schema: Optional[pa.Schema] = None,
        field_converters: Optional[Dict[str, Callable]] = None
    ) -> dict:
        """
        Load data to S3 in Parquet format with explicit type conversion.

        Professional two-stage processing:
        1. Apply field converters (value preprocessing)
        2. Apply PyArrow schema (structure enforcement)

        Path pattern:
          staging/{source}/{entity}/ingestion_date=YYYY-MM-DD/{entity}_{timestamp}.parquet

        Args:
            source: Source namespace (e.g., 'shopify')
            entity: Entity name (e.g., 'orders')
            data: List of record dicts
            updated_field: Field name for tracking sync state
            schema: PyArrow schema defining structure
            field_converters: Dict mapping field names to converter functions

        Returns:
            dict with s3_uri, last_updated_at, row_count
        """
        if not data:
            raise ValueError(
                f"No data provided for {source}/{entity}. "
                f"Buffer strategy guarantees at least 1 record - this should never happen."
            )

        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        tmp_path = f"/tmp/{entity}_{timestamp}.parquet"

        try:
            # Create DataFrame
            df = pd.DataFrame(data)
            df["ingestion_date"] = ingestion_date
            
            logger.info(f"Created DataFrame for {source}/{entity} with {len(df)} rows")

            # ===================================================================
            # STAGE 1: VALUE PREPROCESSING (Apply Field Converters)
            # ===================================================================
            if field_converters:
                logger.info(f"Applying {len(field_converters)} field converters for {source}/{entity}")
                for field_name, converter_fn in field_converters.items():
                    if field_name in df.columns:
                        try:
                            df[field_name] = df[field_name].apply(converter_fn)
                            logger.debug(f"Applied converter to field '{field_name}'")
                        except Exception as e:
                            logger.error(f"Failed to apply converter to field '{field_name}': {e}")
                            raise RuntimeError(
                                f"Field conversion failed for '{field_name}' in {source}/{entity}: {e}"
                            ) from e
                    else:
                        logger.debug(f"Field '{field_name}' not found in data, skipping converter")

            # ===================================================================
            # STAGE 2: STRUCTURE ENFORCEMENT (Apply Schema)
            # ===================================================================
            if schema is not None:
                logger.info(f"Applying explicit PyArrow schema for {source}/{entity}")
                try:
                    # Convert DataFrame to PyArrow Table with explicit schema
                    # Schema enforces:
                    # - Correct types for all fields (including converted ones)
                    # - Proper nested structure (structs, arrays)
                    # - Empty array typing (the critical fix)
                    table = pa.Table.from_pandas(df, schema=schema)
                    
                    logger.debug(f"Successfully created PyArrow Table with {len(table)} rows")
                    
                    # Write using PyArrow directly
                    pq.write_table(
                        table,
                        tmp_path,
                        coerce_timestamps="us",
                        allow_truncated_timestamps=True,
                    )
                    
                    logger.info(f"Wrote {len(df)} rows with explicit schema to: {tmp_path}")
                    
                except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
                    logger.error(
                        f"Schema validation failed for {source}/{entity}: {e}\n"
                        f"Data structure does not match expected schema."
                    )
                    raise ValueError(
                        f"Data does not match expected schema for {source}/{entity}: {e}"
                    ) from e
                    
                except Exception as e:
                    logger.error(f"Failed to create PyArrow Table for {source}/{entity}: {e}")
                    raise RuntimeError(
                        f"PyArrow conversion failed for {source}/{entity}: {e}"
                    ) from e
            
            else:
                # Schema not provided - write with pandas (type inference)
                logger.info(f"No schema provided for {source}/{entity} - using type inference")
                df.to_parquet(
                    tmp_path,
                    index=False,
                    engine="pyarrow",
                    coerce_timestamps="us",
                    allow_truncated_timestamps=True,
                )
                logger.info(f"Wrote {len(df)} rows (inference mode) to: {tmp_path}")

            # ===================================================================
            # STAGE 3: S3 UPLOAD
            # ===================================================================
            key_prefix = f"staging/{source}/{entity}/ingestion_date={ingestion_date}/"
            key = key_prefix + os.path.basename(tmp_path)
            bucket = os.getenv("S3_BUCKET", "default-bucket")
            
            self._upload_with_retry(tmp_path, bucket, key, source, entity)

            s3_uri = f"s3://{bucket}/{key}"
            logger.info(f"Uploaded {len(df)} rows to {s3_uri}")

            # ===================================================================
            # STAGE 4: CALCULATE LAST_UPDATED_AT
            # ===================================================================
            last_updated_at: Optional[datetime] = None
            if updated_field in df.columns and not df[updated_field].isna().all():
                last_updated_at = df[updated_field].max()
                if pd.notna(last_updated_at):
                    # Handle both pandas Timestamp and datetime objects
                    if isinstance(last_updated_at, pd.Timestamp):
                        last_updated_at = last_updated_at.to_pydatetime()
                    if last_updated_at.tzinfo is None:
                        last_updated_at = last_updated_at.replace(tzinfo=timezone.utc)
                    else:
                        last_updated_at = last_updated_at.astimezone(timezone.utc)
                    
                    logger.info(f"last_updated_at={last_updated_at.isoformat()} from '{updated_field}'")
                else:
                    logger.warning(f"Field '{updated_field}' exists but all values are NaT")
            else:
                logger.warning(f"Field '{updated_field}' not found or empty")

            return {
                "s3_uri": s3_uri,
                "last_updated_at": last_updated_at,
                "row_count": len(df)
            }

        except Exception as e:
            logger.error(f"S3 load failed for {source}/{entity}: {e}")
            raise
        finally:
            # Always cleanup tmp file
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                    logger.debug(f"Cleaned up temporary file: {tmp_path}")
                except Exception as cleanup_error:
                    logger.warning(f"Failed to cleanup {tmp_path}: {cleanup_error}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(ClientError),
    )
    def _upload_with_retry(self, tmp_path: str, bucket: str, key: str, source: str, entity: str):
        """Upload file to S3 with retry logic for transient failures."""
        try:
            self.s3.upload_file(tmp_path, bucket, key)
            logger.info(f"S3 upload successful: s3://{bucket}/{key}")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"S3 upload failed: {error_code}")
            raise RuntimeError(f"S3 upload failed for {source}/{entity}: {error_code}")
        except Exception as e:
            logger.error(f"Unexpected S3 error: {e}")
            raise


@resource
def s3_client_resource(_):
    """Dagster resource yielding a boto3 S3 client."""
    session = boto3.session.Session()
    client = session.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT") or None,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )
    logger.info(f"S3 client created (endpoint={os.getenv('S3_ENDPOINT') or 'AWS'})")
    return client


@resource(required_resource_keys={"s3_client"})
def s3_loader_resource(context):
    """Dagster resource for loading data to S3."""
    s3_client = context.resources.s3_client
    return S3Loader(s3_client)