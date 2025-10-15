import argparse
import os
import sys
import logging
import traceback
from pyspark.sql import SparkSession

# === DEBUG ENV + ARGV ===
print("=== DEBUG ENV ===", flush=True)
for k, v in os.environ.items():
    if "ICEBERG" in k or "S3" in k or "AWS" in k:
        print(f"{k}={v}", flush=True)
print("=== ARGV ===", sys.argv, flush=True)
print("=" * 80, flush=True)

# === Logging setup ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def main(namespace: str, table: str, input_path: str):
    if input_path.startswith("s3://"):
        input_path = input_path.replace("s3://", "s3a://", 1)

    log.info("=" * 80)
    log.info(">>> STARTING ICEBERG APPEND (SELF-CONFIGURED)")
    log.info(">>> Namespace : %s", namespace)
    log.info(">>> Table     : %s", table)
    log.info(">>> Input     : %s", input_path)
    log.info("=" * 80)

    try:
        log.info("Building Spark session...")
        spark = (
            SparkSession.builder.appName(f"IcebergLoader-{namespace}-{table}")
            # Iceberg/Nessie
            .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
            .config("spark.sql.catalog.nessie.uri", os.getenv("ICEBERG_CATALOG_URI"))
            .config("spark.sql.catalog.nessie.ref", os.getenv("ICEBERG_CATALOG_REF", "main"))
            .config("spark.sql.catalog.nessie.authentication.type", os.getenv("ICEBERG_CATALOG_AUTH", "NONE"))
            .config("spark.sql.catalog.nessie.warehouse", os.getenv("ICEBERG_CATALOG_WAREHOUSE"))
            # S3/MinIO
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .getOrCreate()
        )
        log.info("Spark session created successfully.")

        log.info("Reading Parquet file...")
        df = spark.read.parquet(input_path)
        log.info("Rows read: %d", df.count())

        # Append raw to write ot raw table
        iceberg_table = f"nessie.{namespace}.{table}_raw"
        log.info("Writing rows into Iceberg table: %s", iceberg_table)
        df.writeTo(iceberg_table).append()

        spark.stop()
        log.info("Spark session stopped.")

    except Exception:
        log.error("=" * 80)
        log.error(">>> AN UNEXPECTED ERROR OCCURRED IN RUN-APPEND <<<")
        log.error(traceback.format_exc())
        log.error("=" * 80)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Append Parquet data to an Iceberg table")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--input", required=True)
    args, unknown = parser.parse_known_args()

    # Spark’s REST launcher can leak an extra arg like "org.apache.spark.deploy.PythonRunner".
    # Ignore anything we don't recognize.
    if unknown:
        logging.getLogger(__name__).info("Ignoring unknown args from launcher: %s", unknown)

    main(args.namespace, args.table, args.input)
