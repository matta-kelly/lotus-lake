# Adding or Changing a Destination

## Overview

```
1. Configure destination in Airbyte UI (get JSON config)
2. Define destination in Terraform
3. Update connections to reference new destination
4. Push to git → tofu-controller applies
5. Verify destination connection and sync
```

## Steps

### 1. Configure in Airbyte UI

Create the destination in the Airbyte UI first to get the correct JSON configuration:

1. Go to Destinations → New Destination
2. Select destination type (e.g., S3, BigQuery, etc.)
3. Fill in configuration
4. Click "Copy JSON" before saving (or save and retrieve later)

### 2. Define Destination in Terraform

Add destination to `orchestration/airbyte/terraform/destinations.tf`:

```hcl
resource "airbyte_destination" "s3" {
  name                      = "S3"
  workspace_id              = var.workspace_id
  destination_definition_id = "4816b78f-1489-44c1-9060-4b19d5fa9362"
  connection_configuration  = jsonencode({
    access_key_id     = var.minio_user
    secret_access_key = var.minio_password
    s3_bucket_name    = "landing"
    s3_bucket_path    = "raw"
    s3_bucket_region  = "us-west-1"
    s3_endpoint       = var.minio_endpoint
    # Date-partitioned paths: raw/{namespace}/{stream}/YYYY/MM/DD/part_0.parquet
    s3_path_format    = "$${NAMESPACE}/$${STREAM_NAME}/$${YEAR}/$${MONTH}/$${DAY}"
    file_name_pattern = "part_{part_number}"
    format = {
      format_type       = "Parquet"
      compression_codec = "UNCOMPRESSED"
    }
  })
}
```

> **Finding destination_definition_id:** Query Airbyte API:
> ```bash
> kubectl exec -n airbyte deploy/airbyte-server -- curl -s \
>   "http://localhost:8001/api/v1/destination_definitions/list" \
>   -H "Content-Type: application/json" -d '{}' | jq '.destinationDefinitions[] | select(.name == "S3")'
> ```

### 3. Update Connections

Update connections in `orchestration/airbyte/terraform/connections.tf` to reference the new destination:

```hcl
resource "airbyte_connection" "shopify_to_s3" {
  source_id      = airbyte_source.shopify.source_id
  destination_id = airbyte_destination.s3.destination_id  # Updated reference
  name           = "Shopify → S3"
  # ... rest of config
}
```

### 4. Push to Git

```bash
git add orchestration/airbyte/terraform/
git commit -m "Update destination to S3 Parquet"
git push
```

### 5. Force Reconcile

Force GitRepository to fetch new commit, then Terraform to apply:

```bash
# 1. Force GitRepository fetch
kubectl annotate gitrepository -n lotus-lake lotus-lake \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite

# 2. Check it picked up new commit
kubectl get gitrepository -n lotus-lake lotus-lake \
  -o jsonpath='{.status.artifact.revision}'

# 3. Force Terraform reconcile
kubectl annotate terraform -n lotus-lake lotus-lake-airbyte \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
```

### 6. Verify

Check Terraform applied:
```bash
kubectl get terraform -n lotus-lake
```

Test destination connection:
```bash
kubectl exec -n airbyte deploy/airbyte-server -- curl -s -X POST \
  "http://localhost:8001/api/v1/destinations/check_connection" \
  -H "Content-Type: application/json" \
  -d '{"destinationId":"YOUR_DESTINATION_ID"}' | jq '{status: .status}'
```

Trigger a sync and verify:
```bash
kubectl exec -n airbyte deploy/airbyte-server -- curl -s -X POST \
  "http://localhost:8001/api/v1/connections/sync" \
  -H "Content-Type: application/json" \
  -d '{"connectionId":"YOUR_CONNECTION_ID"}' | jq '{jobId: .job.id, status: .job.status}'
```

## Quick Reference

| Task | Command |
|------|---------|
| List destination types | `kubectl exec -n airbyte deploy/airbyte-server -- curl -s "http://localhost:8001/api/v1/destination_definitions/list" -H "Content-Type: application/json" -d '{}' \| jq '[.destinationDefinitions[] \| {name, id: .destinationDefinitionId}]'` |
| List destinations | `kubectl exec -n airbyte deploy/airbyte-server -- curl -s "http://localhost:8001/api/v1/destinations/list" -H "Content-Type: application/json" -d '{"workspaceId":"WORKSPACE_ID"}' \| jq '.destinations[].name'` |
| Test destination | `kubectl exec -n airbyte deploy/airbyte-server -- curl -s -X POST "http://localhost:8001/api/v1/destinations/check_connection" -H "Content-Type: application/json" -d '{"destinationId":"DEST_ID"}' \| jq '.status'` |
| Force GitRepository | `kubectl annotate gitrepository -n lotus-lake lotus-lake reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite` |
| Force Terraform | `kubectl annotate terraform -n lotus-lake lotus-lake-airbyte reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite` |

## Secrets

Destination credentials must be in the terraform vars secret in h-kube:

```bash
# In h-kube repo
sops cluster/namespaces/lotus-lake/lotus-lake-secrets.yaml
```

Add variables (e.g., `minio_user`, `minio_password`), then commit and push h-kube.

## S3 Path Format Variables

When using S3 destination, these variables are available for `s3_path_format`:

| Variable | Value |
|----------|-------|
| `${NAMESPACE}` | Connection namespace (e.g., `shopify`) |
| `${STREAM_NAME}` | Stream name (e.g., `orders`) |
| `${YEAR}`, `${MONTH}`, `${DAY}` | Upload date |
| `${EPOCH}` | Timestamp |

Example: `${NAMESPACE}/${STREAM_NAME}/${YEAR}/${MONTH}/${DAY}` → `shopify/orders/2026/01/07/`
