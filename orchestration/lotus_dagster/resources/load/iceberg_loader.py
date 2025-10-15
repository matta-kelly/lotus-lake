import logging
import os
import subprocess
from dagster import resource
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class IcebergLoader:
    """
    Submits Spark jobs via spark-submit to append Parquet files into Iceberg.
    Uses client mode with Python for simplicity and reliability.
    """

    def load(self, namespace: str, table: str, s3_uri: str) -> dict:
        """
        Load data from S3 staging to Iceberg table via Spark.
        
        With buffer strategy, empty s3_uri should never occur.
        
        Returns:
            dict with:
              - status (str): "success"
              - message (str): Human-readable result
        """
        if not s3_uri:
            raise ValueError(
                f"No s3_uri provided for {namespace}.{table}. "
                f"Buffer strategy guarantees data - this should never happen."
            )

        # Python script is local to Dagster container
        job_script = "/opt/spark/jobs/run-append.py"
        
        cmd = [
            "spark-submit",
            "--master", "spark://spark-master:7077",
            "--deploy-mode", "client",
            "--name", f"IcebergLoader-{namespace}-{table}",
            "--conf", f"spark.executorEnv.ICEBERG_CATALOG_URI={os.getenv('ICEBERG_CATALOG_URI', 'http://nessie:19120/api/v1')}",
            "--conf", f"spark.executorEnv.ICEBERG_CATALOG_WAREHOUSE={os.getenv('ICEBERG_CATALOG_WAREHOUSE', 's3a://lotus-lakehouse')}",
            "--conf", f"spark.executorEnv.S3_ENDPOINT={os.getenv('S3_ENDPOINT', 'http://datalake-storage:9000')}",
            "--conf", f"spark.executorEnv.AWS_ACCESS_KEY_ID={os.getenv('AWS_ACCESS_KEY_ID')}",
            "--conf", f"spark.executorEnv.AWS_SECRET_ACCESS_KEY={os.getenv('AWS_SECRET_ACCESS_KEY')}",
            "--conf", f"spark.executorEnv.ICEBERG_CATALOG_REF={os.getenv('ICEBERG_CATALOG_REF', 'main')}",
            "--conf", f"spark.executorEnv.ICEBERG_CATALOG_AUTH={os.getenv('ICEBERG_CATALOG_AUTH', 'NONE')}",
            job_script,
            "--namespace", namespace,
            "--table", table,
            "--input", s3_uri,
        ]

        logger.info(f"Submitting Spark job for {namespace}.{table}: {' '.join(cmd)}")

        try:
            self._run_spark_job_with_retry(cmd, namespace, table, s3_uri)
            return {
                "status": "success",
                "message": f"Successfully loaded {namespace}.{table} from {s3_uri}"
            }
        except Exception as e:
            logger.error(f"Iceberg load failed for {namespace}.{table}: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        retry=retry_if_exception_type((subprocess.CalledProcessError, subprocess.TimeoutExpired)),
    )
    def _run_spark_job_with_retry(self, cmd: list, namespace: str, table: str, s3_uri: str):
        """
        Execute Spark job with retry logic for transient failures.
        
        Retries on:
        - CalledProcessError (non-zero exit code)
        - TimeoutExpired (job took too long)
        
        Does NOT retry on:
        - Other exceptions (configuration errors, etc.)
        """
        try:
            # Increased timeout to 20 minutes for large data volumes
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=1200,
                check=True
            )
            
            # Log success with truncated output
            logger.info(f"Spark job completed successfully for {namespace}.{table}")
            
            # Log full stdout at debug level, truncated at info level
            if result.stdout:
                stdout_lines = result.stdout.strip().split('\n')
                if len(stdout_lines) > 20:
                    logger.info(f"Spark stdout (first 10 lines):\n" + '\n'.join(stdout_lines[:10]))
                    logger.info(f"Spark stdout (last 10 lines):\n" + '\n'.join(stdout_lines[-10:]))
                    logger.debug(f"Spark full stdout:\n{result.stdout}")
                else:
                    logger.info(f"Spark stdout:\n{result.stdout}")
            
            return result
            
        except subprocess.CalledProcessError as e:
            # Parse stderr for common Spark errors
            error_context = self._parse_spark_error(e.stderr)
            
            logger.error(
                f"Spark job failed for {namespace}.{table} with exit code {e.returncode}\n"
                f"S3 URI: {s3_uri}\n"
                f"Error: {error_context}"
            )
            
            # Log stderr for debugging
            if e.stderr:
                stderr_lines = e.stderr.strip().split('\n')
                logger.error(f"Spark stderr (FULL):\n" + '\n'.join(stderr_lines))
            
            raise RuntimeError(
                f"Spark job failed for {namespace}.{table}: {error_context}\n"
                f"Exit code: {e.returncode}"
            )
            
        except subprocess.TimeoutExpired as e:
            logger.error(
                f"Spark job timed out after 1200s for {namespace}.{table}\n"
                f"S3 URI: {s3_uri}\n"
                f"This may indicate large data volume or resource constraints"
            )
            raise TimeoutError(
                f"Spark job for {namespace}.{table} did not complete in 20 minutes. "
                f"Consider increasing worker resources or splitting the load."
            )

    def _parse_spark_error(self, stderr: str) -> str:
        """Extract meaningful error messages from Spark stderr."""
        if not stderr:
            return "Unknown error (no stderr)"
        
        # Common Spark error patterns
        error_patterns = [
            "java.lang.OutOfMemoryError",
            "org.apache.spark.SparkException",
            "Connection refused",
            "Table not found",
            "Access denied",
            "NoSuchBucket",
            "File does not exist",
        ]
        
        # Find first line containing an error pattern
        for line in stderr.split('\n'):
            for pattern in error_patterns:
                if pattern in line:
                    return line.strip()
        
        # Fallback: return last non-empty line
        lines = [l.strip() for l in stderr.split('\n') if l.strip()]
        return lines[-1] if lines else stderr[:200]


@resource
def iceberg_loader_resource(_):
    return IcebergLoader()