# DuckLake Metadata Catalog Reference

DuckLake stores all metadata in a PostgreSQL database (our `ducklake-db` cluster). Unlike Iceberg/Delta Lake which store metadata as files, DuckLake uses relational tables.

## Connection

```sql
-- From DuckDB
ATTACH 'ducklake:postgres:host=ducklake-db-rw.lotus-lake.svc.cluster.local port=5432 dbname=ducklake user=ducklake password=XXX' AS lakehouse;

-- Direct PostgreSQL access
psql -h ducklake-db-rw.lotus-lake.svc.cluster.local -U ducklake -d ducklake
```

## Catalog Tables (22 total)

All tables are in the `public` schema with `ducklake_` prefix.

### Core Schema Tables

| Table | Purpose |
|-------|---------|
| `ducklake_schema` | Schema definitions (staging, main, etc.) |
| `ducklake_table` | Table metadata - names, paths, validity snapshots |
| `ducklake_column` | Column definitions, types, defaults |
| `ducklake_view` | View definitions |

### Snapshot/Versioning

| Table | Purpose |
|-------|---------|
| `ducklake_snapshot` | Version history - each write creates a snapshot |
| `ducklake_snapshot_changes` | Change tracking (author, commit messages) |
| `ducklake_schema_versions` | Schema evolution history |

### Data Files

| Table | Purpose |
|-------|---------|
| `ducklake_data_file` | Parquet file locations, sizes, row counts |
| `ducklake_delete_file` | Deletion markers |
| `ducklake_files_scheduled_for_deletion` | Files pending cleanup |
| `ducklake_inlined_data_tables` | Small data staged for inlining |

### Column Mapping (Critical for our error)

| Table | Purpose |
|-------|---------|
| `ducklake_column_mapping` | Maps parquet fields to columns when no field-id exists |
| `ducklake_name_mapping` | Field name-to-column-id mappings |

**Our "Unknown name map id 16550" error**: The parquet files reference a `mapping_id` (16550) in `ducklake_name_mapping` that no longer exists in the catalog.

### Statistics

| Table | Purpose |
|-------|---------|
| `ducklake_table_stats` | Row counts, file sizes per table |
| `ducklake_table_column_stats` | Min/max values per column |
| `ducklake_file_column_stats` | Stats per file |

### Partitioning

| Table | Purpose |
|-------|---------|
| `ducklake_partition_info` | Partition configurations |
| `ducklake_partition_column` | Partition key columns |
| `ducklake_file_partition_value` | Partition values per file |

### Auxiliary

| Table | Purpose |
|-------|---------|
| `ducklake_metadata` | Key-value config store |
| `ducklake_tag` | Object tags |
| `ducklake_column_tag` | Column-specific tags |

## Key Relationships

```
ducklake_schema (schema_id)
    └── ducklake_table (table_id, schema_id)
            ├── ducklake_column (column_id, table_id)
            ├── ducklake_data_file (data_file_id, table_id, mapping_id)
            │       └── ducklake_column_mapping (mapping_id)
            │               └── ducklake_name_mapping (mapping_id, column_id)
            └── ducklake_table_stats
```

## Snapshot System

- Every write creates a new snapshot (`snapshot_id` in `ducklake_snapshot`)
- Tables track validity via `begin_snapshot` and `end_snapshot`
- `end_snapshot = NULL` means the entry is currently valid
- Time travel: `SELECT * FROM my_table AT (VERSION => 5)`

## Maintenance Commands (via DuckDB)

```sql
-- Expire old snapshots (required before files can be deleted)
CALL ducklake_expire_snapshots('lakehouse', older_than => now() - INTERVAL '1 week');

-- Delete files scheduled for deletion
CALL ducklake_cleanup_old_files('lakehouse', older_than => now() - INTERVAL '1 day');

-- Delete orphaned files (not tracked in catalog)
CALL ducklake_delete_orphaned_files('lakehouse', dry_run => true);  -- preview first
CALL ducklake_delete_orphaned_files('lakehouse', cleanup_all => true);

-- All-in-one maintenance
CHECKPOINT lakehouse;
```

## Inspecting Current State

```sql
-- Via PostgreSQL directly:

-- List all schemas
SELECT schema_id, schema_name, begin_snapshot, end_snapshot
FROM ducklake_schema WHERE end_snapshot IS NULL;

-- List all current tables
SELECT t.table_id, s.schema_name, t.table_name, t.path
FROM ducklake_table t
JOIN ducklake_schema s ON t.schema_id = s.schema_id
WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL;

-- List data files for a table
SELECT df.data_file_id, df.path, df.record_count, df.mapping_id
FROM ducklake_data_file df
JOIN ducklake_table t ON df.table_id = t.table_id
WHERE t.table_name = 'stg_shopify__order_refunds'
  AND df.end_snapshot IS NULL;

-- Check for orphaned name mappings
SELECT nm.* FROM ducklake_name_mapping nm
LEFT JOIN ducklake_column_mapping cm ON nm.mapping_id = cm.mapping_id
WHERE cm.mapping_id IS NULL;
```

## Common Issues

### "Unknown name map id X"
Parquet files reference a `mapping_id` that was deleted from catalog. Fix:
1. Drop the table via DuckDB: `DROP TABLE lakehouse.schema.table_name;`
2. This properly cascades deletes across all related catalog tables
3. Re-sync data from source

### Orphaned Files
Files exist in S3 but not in catalog. Fix:
```sql
CALL ducklake_delete_orphaned_files('lakehouse', cleanup_all => true);
```

### Manual Catalog Edits - DON'T
Never manually DELETE/UPDATE rows in catalog tables. Always use DuckDB's DDL commands which handle cascading properly.

## Runbook

Practical commands for common operations. Run from the Dagster pod which has all credentials configured.

### Check Current State

```bash
# Get into Dagster pod
export KUBECONFIG=/path/to/kubeconfig.yaml
kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake -- python3 -c "
from orchestration.dag.landing.lib import get_ducklake_connection

conn = get_ducklake_connection()

# List schemas
schemas = conn.execute('SELECT schema_id, schema_name FROM lakehouse.information_schema.schemata').fetchall()
print('Schemas:', schemas)

# List tables
tables = conn.execute('''
    SELECT table_schema, table_name
    FROM lakehouse.information_schema.tables
    WHERE table_schema IN (\"staging\", \"main\", \"meta\")
''').fetchall()
for t in tables:
    print(f'  {t[0]}.{t[1]}')

conn.close()
"
```

### Check for Orphaned Files

```bash
kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake -- python3 -c "
from orchestration.dag.landing.lib import get_ducklake_connection

conn = get_ducklake_connection()
orphans = conn.execute(\"CALL ducklake_delete_orphaned_files('lakehouse', dry_run => true)\").fetchall()
print(f'Orphaned files: {len(orphans)}')
for o in orphans[:10]:
    print(f'  {o[0]}')
if len(orphans) > 10:
    print(f'  ... and {len(orphans) - 10} more')
conn.close()
"
```

### Delete Orphaned Files (Fast Method)

The built-in `ducklake_delete_orphaned_files` can be slow. Use direct S3 deletion:

```bash
kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake -- python3 -c "
from orchestration.dag.landing.lib import get_ducklake_connection
import boto3
import os

conn = get_ducklake_connection()
orphans = conn.execute(\"CALL ducklake_delete_orphaned_files('lakehouse', dry_run => true)\").fetchall()
print(f'Deleting {len(orphans)} orphaned files...')

s3 = boto3.client('s3',
    endpoint_url='http://' + os.environ.get('S3_ENDPOINT', ''),
    aws_access_key_id=os.environ.get('S3_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('S3_SECRET_ACCESS_KEY')
)

for path in [o[0] for o in orphans]:
    key = path.replace('s3://landing/', '')
    s3.delete_object(Bucket='landing', Key=key)

conn.close()
print('Done')
"
```

### Drop a Table

Always use DuckDB's DROP TABLE (not manual catalog deletes):

```bash
kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake -- python3 -c "
from orchestration.dag.landing.lib import get_ducklake_connection

conn = get_ducklake_connection()
conn.execute('DROP TABLE IF EXISTS lakehouse.staging.stg_shopify__order_refunds')
conn.execute('DROP TABLE IF EXISTS lakehouse.main.int_shopify__order_refunds')
print('Dropped')
conn.close()
"
```

### Drop All Tables (Nuclear Reset)

```bash
kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake -- python3 -c "
from orchestration.dag.landing.lib import get_ducklake_connection

conn = get_ducklake_connection()

tables = [
    'lakehouse.staging.stg_klaviyo__events',
    'lakehouse.staging.stg_klaviyo__profiles',
    'lakehouse.staging.stg_shopify__customers',
    'lakehouse.staging.stg_shopify__orders',
    'lakehouse.staging.stg_shopify__order_refunds',
    'lakehouse.main.int_klaviyo__events',
    'lakehouse.main.int_klaviyo__profiles',
    'lakehouse.main.int_shopify__customers',
    'lakehouse.main.int_shopify__orders',
    'lakehouse.main.int_shopify__order_refunds',
    'lakehouse.meta.feeder_cursors',
]

for t in tables:
    try:
        conn.execute(f'DROP TABLE IF EXISTS {t}')
        print(f'Dropped {t}')
    except Exception as e:
        print(f'Failed {t}: {e}')

conn.close()
"
```

### Query Catalog via PostgreSQL

For direct catalog inspection (read-only):

```bash
PGPASSWORD=$(kubectl get secret -n lotus-lake ducklake-db-app -o jsonpath='{.data.password}' | base64 -d)

kubectl exec -n lotus-lake ducklake-db-1 -c postgres -- bash -c "
PGPASSWORD='$PGPASSWORD' psql -h localhost -U ducklake -d ducklake -c \"
SELECT s.schema_name, t.table_name, t.table_id
FROM ducklake_table t
JOIN ducklake_schema s ON t.schema_id = s.schema_id
WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL
ORDER BY s.schema_name, t.table_name;
\""
```

## Sources

- [DuckLake Specification](https://ducklake.select/docs/stable/specification/tables/overview)
- [DuckLake Maintenance](https://ducklake.select/docs/stable/duckdb/maintenance/cleanup_of_files)
- [DuckDB Extension Docs](https://duckdb.org/docs/stable/core_extensions/ducklake)
