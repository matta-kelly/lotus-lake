# Adding a New Source

## Overview

```
1. Discover available streams (optional - if source exists in Airbyte)
2. Define source in Terraform
3. Select & configure streams
4. Generate catalog
5. Push to git → tofu-controller applies automatically
```

## Steps

### 1. Discover Available Streams (Optional)

If the source already exists in Airbyte, discover what streams are available:

```bash
make discover
```

Select your source. This fetches all available streams and saves to:
```
orchestration/assets/sources/my_source/
├── stream_a.json
├── stream_b.json
└── stream_c.json
```

Each file shows:
- Available sync modes
- Default primary key / cursor
- Full field schema with types and descriptions

### 2. Define Source in Terraform

Add source definition to `orchestration/airbyte/terraform/sources.tf`:

```hcl
resource "airbyte_source" "my_source" {
  workspace_id = var.workspace_id
  name         = "My Source"

  configuration = {
    source_type = "my-source"
    api_key     = var.my_source_api_key
    # ... other config
  }
}
```

Add any new variables to `orchestration/airbyte/terraform/variables.tf`:

```hcl
variable "my_source_api_key" {
  description = "API key for My Source"
  type        = string
  sensitive   = true
}
```

> **Tip:** Sometimes it's easier to configure the source in the Airbyte UI first, then copy the JSON configuration over.

### 3. Select & Configure Streams

Create stream configs in `orchestration/assets/streams/my_source/`:

```bash
mkdir -p orchestration/assets/streams/my_source
```

Create a JSON file for each stream you want to sync (e.g., `stream_a.json`):

```json
{
  "stream": "stream_a",
  "sync_mode": "incremental",
  "destination_sync_mode": "append_dedup",
  "backfill": false,
  "primary_key": [
    [
      "id"
    ]
  ],
  "cursor_field": [
    "updated_at"
  ],
  "fields": {
    "id": {
      "type": "integer",
      "description": "Unique identifier"
    },
    "name": {
      "type": "string",
      "description": "Name of the record"
    },
    "updated_at": {
      "type": "string",
      "description": "Last update timestamp",
      "format": "date-time"
    }
  }
}
```

**Configuration options:**

| Field | Options | Description |
|-------|---------|-------------|
| `sync_mode` | `full_refresh`, `incremental` | How to read from source |
| `destination_sync_mode` | `overwrite`, `append`, `append_dedup` | How to write to destination |
| `backfill` | `true`, `false` | Set `true` to trigger a full re-sync |
| `primary_key` | `[["field"]]` | Dedup key for append_dedup mode |
| `cursor_field` | `["field"]` | Field for incremental sync |
| `fields` | `{...}` | Only these fields will be synced |

**Field selection:** Only include fields you actually need. Reference `orchestration/assets/sources/my_source/` for available fields with types and descriptions.

### 4. Generate Catalog

```bash
python orchestration/airbyte/generate-catalog.py
```

This creates `orchestration/assets/streams/my_source/_catalog.json` from your stream configs.

### 5. Add Connection to Terraform

Add connection to `orchestration/airbyte/terraform/connections.tf`:

```hcl
resource "airbyte_connection" "my_source_to_lake" {
  source_id      = airbyte_source.my_source.source_id
  destination_id = airbyte_destination.s3_data_lake.destination_id
  name           = "My Source → S3 Data Lake"

  schedule = {
    schedule_type = "cron"
    cron_expression = "0 0 * * * ?"  # Every hour
  }

  sync_catalog = jsondecode(file("${path.module}/../../assets/streams/my_source/_catalog.json"))
}
```

### 6. Push to Git

```bash
git add orchestration/
git commit -m "Add My Source to Airbyte"
git push
```

tofu-controller will automatically:
1. Detect the new commit (within 5 minutes)
2. Run `terraform plan`
3. Auto-approve and apply
4. Create source and connection in Airbyte

### 7. Verify

Check tofu-controller status:
```bash
kubectl get terraform -n lotus-lake
```

Expected output:
```
NAME                 READY   STATUS                              AGE
lotus-lake-airbyte   True    Applied successfully: main@sha1:...  5m
```

Check Airbyte connections:
```bash
kubectl exec -n airbyte deploy/airbyte-server -- curl -s \
  "http://localhost:8001/api/v1/connections/list" \
  -H "Content-Type: application/json" \
  -d '{"workspaceId":"YOUR_WORKSPACE_ID"}' | jq '.connections[].name'
```

## Adding a Stream to an Existing Source

Simpler workflow when the source already exists:

1. Create stream config in `orchestration/assets/streams/SOURCE_NAME/STREAM.json`
2. Run `python orchestration/airbyte/generate-catalog.py`
3. Push to git

That's it. tofu-controller handles the rest.

## Triggering a Backfill

To re-sync all historical data for a stream:

1. Edit the stream config, set `"backfill": true`
2. Push to git
3. Wait for sync to complete
4. Set `"backfill": false` and push again

## Quick Reference

| Task | Command |
|------|---------|
| Discover streams | `make discover` |
| Generate catalog | `python orchestration/airbyte/generate-catalog.py` |
| Check terraform status | `kubectl get terraform -n lotus-lake` |
| Check GitRepository revision | `kubectl get gitrepository -n lotus-lake lotus-lake -o jsonpath='{.status.artifact.revision}'` |
| Force GitRepository fetch | `kubectl annotate gitrepository -n lotus-lake lotus-lake reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite` |
| Force terraform reconcile | `kubectl annotate terraform -n lotus-lake lotus-lake-airbyte reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite` |

> **Note:** When forcing reconcile after a push, always force GitRepository first (to fetch new commit), then Terraform.

## Secrets

New source credentials must be added to the terraform vars secret in h-kube:

```bash
# In h-kube repo
sops cluster/namespaces/lotus-lake/lotus-lake-secrets.yaml
```

Add your variable (e.g., `TF_VAR_my_source_api_key`), then commit and push h-kube.
