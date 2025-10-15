#!/usr/bin/env bash
set -euo pipefail

#
# Apply Iceberg DDLs (CREATE/ALTER) from /sql (recursively) into Nessie-backed catalog.
# Separate from merge; run this when schemas change.
#
# Required env vars:
#   NESSIE_URI          e.g. http://nessie:19120/api/v1
#   WAREHOUSE           e.g. s3a://lotus-lakehouse
#   S3_ENDPOINT         e.g. http://datalake-storage:9000
#   AWS_ACCESS_KEY_ID   MinIO/AWS key
#   AWS_SECRET_ACCESS_KEY MinIO/AWS secret
#
# Optional:
#   NESSIE_REF       default: main
#   S3_PATH_STYLE    default: true
#   S3_SSL_ENABLED   default: false
#   INIT_SQL_GLOB    default: /sql/{01_raw,02_core,03_mart}/**/*.sql
#   SPARK_EXTRA_OPTS extra conf switches
#

: "${NESSIE_URI:?set NESSIE_URI (e.g. http://nessie:19120/api/v1)}"
: "${WAREHOUSE:?set WAREHOUSE (e.g. s3a://lotus-lakehouse)}"
: "${S3_ENDPOINT:?set S3_ENDPOINT (e.g. http://datalake-storage:9000)}"
: "${AWS_ACCESS_KEY_ID:?set AWS_ACCESS_KEY_ID}"
: "${AWS_SECRET_ACCESS_KEY:?set AWS_SECRET_ACCESS_KEY}"

: "${NESSIE_REF:=main}"
: "${S3_PATH_STYLE:=true}"
: "${S3_SSL_ENABLED:=false}"
: "${INIT_SQL_GLOB:=/sql/*.sql}"
: "${SPARK_EXTRA_OPTS:=}"

# Enable recursive globbing (**)
shopt -s nullglob globstar
files=( ${INIT_SQL_GLOB} )
shopt -u globstar
shopt -u nullglob

if (( ${#files[@]} == 0 )); then
  echo "ERROR: no schema files matched: ${INIT_SQL_GLOB}" >&2
  exit 2
fi

# Wait for Nessie trees endpoint to be ready
echo "Waiting for Nessie catalog to be ready..."
until curl -fsSL "${NESSIE_URI}/trees/tree/${NESSIE_REF}?fetch=MINIMAL" >/dev/null; do
  echo "Nessie not ready yet, retrying in 3s..."
  sleep 3
done
echo "Nessie is ready!"

echo "=== Spark INIT starting ==="
echo "Schema files  : ${files[*]}"
echo "Nessie URI    : $NESSIE_URI"
echo "Nessie ref    : $NESSIE_REF"
echo "Warehouse     : $WAREHOUSE"
echo "S3 endpoint   : $S3_ENDPOINT"

for f in "${files[@]}"; do
  echo "--- applying: $f"
  /opt/spark/bin/spark-sql \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions \
    --conf spark.sql.defaultCatalog=nessie \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.nessie.uri="${NESSIE_URI}" \
    --conf spark.sql.catalog.nessie.ref="${NESSIE_REF}" \
    --conf spark.sql.catalog.nessie.warehouse="${WAREHOUSE}" \
    --conf spark.hadoop.fs.s3a.endpoint="${S3_ENDPOINT}" \
    --conf spark.hadoop.fs.s3a.access.key="${AWS_ACCESS_KEY_ID}" \
    --conf spark.hadoop.fs.s3a.secret.key="${AWS_SECRET_ACCESS_KEY}" \
    --conf spark.hadoop.fs.s3a.path.style.access="${S3_PATH_STYLE}" \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled="${S3_SSL_ENABLED}" \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    --conf spark.sql.session.timeZone=UTC \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.jars.packages="" \
    ${SPARK_EXTRA_OPTS} \
    -f "$f"
done

echo "=== Spark INIT complete ==="
