# Adding a Source

How to add a new data source (Shopify, Klaviyo, etc.) to Airbyte.

> **Custom APIs:** For sources not in the Airbyte catalog (custom APIs, complex auth, rate limiting), see [Custom Ingestion with dlt](custom-ingestion-dlt.md).

## Overview

```
1. Discover available streams (optional)
2. Define source in Terraform
3. Configure streams to sync
4. Generate Airbyte catalog
5. Add connection in Terraform
6. Push → tofu-controller applies
```

## Steps

### 1. Discover Available Streams (Optional)

If exploring a new source, discover what's available:

```bash
make discover
```

Select your source. This saves schemas to `orchestration/dag/sources/{source}/`:

```
dag/sources/shopify/
├── orders.json       # Available fields, types, sync modes
├── customers.json
└── ...
```

### 2. Define Source in Terraform

Add to `orchestration/airbyte/terraform/sources.tf`:

```hcl
resource "airbyte_source" "my_source" {
  workspace_id = var.workspace_id
  name         = "My Source"

  configuration = {
    source_type = "my-source"
    api_key     = var.my_source_api_key
  }
}
```

Add variables to `variables.tf`:

```hcl
variable "my_source_api_key" {
  type      = string
  sensitive = true
}
```

### 3. Configure Streams

Create stream configs in `orchestration/dag/streams/{source}/`:

```bash
mkdir -p orchestration/dag/streams/my_source
```

Create `{stream}.json` for each stream to sync:

```json
{
  "stream": "orders",
  "sync_mode": "incremental",
  "destination_sync_mode": "append",
  "primary_key": [["id"]],
  "cursor_field": ["updated_at"],
  "fields": {
    "id": {"type": "integer"},
    "created_at": {"type": "string", "format": "date-time"},
    "total_price": {"type": "number"}
  }
}
```

| Field | Options | Purpose |
|-------|---------|---------|
| `sync_mode` | `incremental`, `full_refresh` | How to read from source |
| `destination_sync_mode` | `append`, `overwrite` | How to write to S3 |
| `primary_key` | `[["id"]]` | Dedup key |
| `cursor_field` | `["updated_at"]` | Incremental cursor |
| `fields` | `{...}` | Only sync these fields |

### 4. Generate Catalog

Run the catalog generator locally (CI will validate it's up to date):

```bash
python orchestration/airbyte/generate-catalog.py
```

Creates `dag/streams/{source}/_catalog.json` for Airbyte.

**Note:** CI will fail if you forget this step, ensuring catalogs are always current.

### 5. Add Connection

Add to `orchestration/airbyte/terraform/connections.tf`:

```hcl
resource "airbyte_connection" "my_source_to_lake" {
  source_id      = airbyte_source.my_source.source_id
  destination_id = airbyte_destination.s3_data_lake.destination_id
  name           = "My Source → S3 Data Lake"

  schedule = {
    schedule_type   = "cron"
    cron_expression = "0 0 * * * ?"  # Every hour
  }

  sync_catalog = jsondecode(file("${path.module}/../../dag/streams/my_source/_catalog.json"))
}
```

### 6. Add Secrets

In h-kube repo:

```bash
sops cluster/namespaces/lotus-lake/lotus-lake-secrets.yaml
```

Add `TF_VAR_my_source_api_key`, commit, push h-kube.

### 7. Push

```bash
git add orchestration/
git commit -m "Add My Source"
git push
```

tofu-controller applies within 5 minutes.

## Adding a Stream to Existing Source

Simpler workflow:

1. Create `dag/streams/{source}/{stream}.json`
2. Run `python orchestration/airbyte/generate-catalog.py`
3. Push

The feeder asset and sensor are auto-generated from stream configs.

## Triggering a Backfill

To re-sync all historical data:

1. Set `"backfill": true` in stream config
2. Push, wait for sync
3. Set `"backfill": false`, push again

## Verification

```bash
# Check terraform status
kubectl get terraform -n lotus-lake

# Force reconcile (if impatient)
kubectl annotate terraform -n lotus-lake lotus-lake-airbyte \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite

# List connections
kubectl exec -n airbyte deploy/airbyte-server -- curl -s \
  "http://localhost:8001/api/v1/connections/list" \
  -H "Content-Type: application/json" \
  -d '{"workspaceId":"b93dc139-15d7-4729-9cdc-5c754b9d9401"}' | jq '.connections[].name'
```

## Quick Reference

| Task | Command |
|------|---------|
| Discover streams | `cd orchestration/airbyte && python discover-source.py` |
| Generate catalog | `python orchestration/airbyte/generate-catalog.py` |
| Check terraform | `kubectl get terraform -n lotus-lake` |
| Force reconcile | See above |

## What Happens After

Once Airbyte is syncing:

1. Parquet files land in `s3://landing/raw/{source}/{stream}/year=.../`
2. Sensor detects new files
3. Feeder registers files into landing table
4. Processed dbt model transforms data
5. Enriched auto-materializes

See [Adding a Flow](adding-a-flow.md) to set up the landing DDL and dbt models.
