# Adding a New Source

## Overview

```
1. Define source in Terraform
2. Deploy source to Airbyte
3. Discover available streams
4. Select & configure streams
5. Generate catalog
6. Apply connection
```

## Steps

### 1. Define Source in Terraform

Add source definition to `infrastructure/airbyte/terraform/sources.tf`:

Note: perhaps easier to config in UI & Copy json over

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

Add connection to `infrastructure/airbyte/terraform/connections.tf`:

```hcl
resource "airbyte_connection" "my_source_to_lake" {
  source_id      = airbyte_source.my_source.source_id
  destination_id = airbyte_destination.s3_data_lake.destination_id
  name           = "My Source → S3 Data Lake"
  # ... config

  sync_catalog = jsondecode(file("${path.module}/../streams/my_source.json"))
}
```

### 2. Deploy Source to Airbyte

```bash
cd infrastructure/airbyte/terraform
terraform apply
```

This creates the source in Airbyte (connection will fail until we have streams).

### 3. Discover Available Streams

```bash
make discover
```

Select your new source. This fetches all available streams and saves to:
```
orchestration/assets/sources/my_source/
├── stream_a.json
├── stream_b.json
└── stream_c.json
```

Each file shows:
- Available sync modes
- Default primary key / cursor
- Full field schema

### 4. Select & Configure Streams

Copy the streams you want:

```bash
mkdir -p orchestration/assets/streams/my_source
cp orchestration/assets/sources/my_source/stream_a.json \
   orchestration/assets/streams/my_source/
```

Edit the stream file to configure:

```json
{
  "stream": "stream_a",
  "sync_mode": "incremental",
  "destination_sync_mode": "append_dedup",
  "primary_key": [["id"]],
  "cursor_field": ["updated_at"],
  "fields": {
    "id": { "type": "integer" },
    "name": { "type": "string" },
    "updated_at": { "type": "string", "format": "date-time" }
  }
}
```

- Set `sync_mode` from available options
- Set `destination_sync_mode`: `overwrite`, `append`, or `append_dedup`
- Keep or override `primary_key` and `cursor_field`
- Trim `fields` to only what you need

### 5. Generate Catalog

```bash
make generate
```

Select your source. This creates:
```
infrastructure/airbyte/streams/my_source.json
```

### 6. Apply Connection

```bash
make apply
```

This updates Terraform with the new catalog and applies to Airbyte.

## Verify

Check Airbyte UI or trigger a sync:
```bash
# List connections
curl -s http://localhost:8080/api/public/v1/connections | jq '.data[].name'
```

## Quick Reference

| Command | What it does |
|---------|--------------|
| `make discover` | Fetch available streams from Airbyte source |
| `make generate` | Build Airbyte catalog from stream configs |
| `make plan` | Generate all + terraform plan |
| `make apply` | Generate all + terraform apply |
