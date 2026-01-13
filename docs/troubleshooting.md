# Troubleshooting

Common issues, debugging workflows, and health checks.

---

## Health Checks

### Is the System Running?

```bash
export KUBECONFIG=/home/mkultra/bode/h-kube/generated/kubeconfig.yaml

# Check all lotus-lake pods
kubectl get pods -n lotus-lake

# Expected: All Running, no restarts
# - dagster-webserver-*
# - dagster-daemon-*
# - dagster-user-deployments-*
# - dagster-db-1
# - ducklake-db-1
```

### Is Data Flowing?

```bash
# Check recent Dagster runs
kubectl logs -n lotus-lake -l component=daemon --tail=50 | grep -i "run\|complete\|fail"

# Check sensor activity
kubectl logs -n lotus-lake -l component=user-deployments --tail=100 | grep -i sensor
```

### Check Feeder Cursors

Connect to DuckLake and query cursor positions:

```sql
SELECT source, stream, cursor_value, updated_at
FROM lakehouse.meta.feeder_cursors
ORDER BY updated_at DESC;
```

If cursors aren't advancing, feeders aren't processing new files.

### Check S3 Connectivity

```bash
# From a Dagster pod
kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake -- \
  python -c "
import boto3
s3 = boto3.client('s3',
    endpoint_url='http://seaweedfs-s3.seaweedfs.svc.cluster.local:8333',
    aws_access_key_id='$S3_ACCESS_KEY_ID',
    aws_secret_access_key='$S3_SECRET_ACCESS_KEY')
print(s3.list_buckets())
"
```

---

## Common Issues

### Terraform Stuck or Failing

**Symptoms**: Airbyte sources/connections not updating after push

**Check status**:
```bash
kubectl get terraform -n lotus-lake -o yaml | grep -A20 status
```

**Force reconciliation**:
```bash
kubectl annotate terraform -n lotus-lake lotus-lake-airbyte \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
```

**Check terraform runner logs**:
```bash
kubectl logs -n lotus-lake -l app.kubernetes.io/instance=lotus-lake-airbyte --tail=100
```

**Common causes**:
- Local `.tfstate` files committed (see TICKET-003)
- Airbyte API unreachable
- Invalid HCL syntax

---

### Feeder Not Processing Files

**Symptoms**: Files in S3 but not appearing in DuckLake tables

**Check sensor is running**:
```bash
kubectl logs -n lotus-lake -l component=user-deployments --tail=200 | grep sensor
```

**Check for files after cursor**:
```sql
-- Get current cursor
SELECT * FROM lakehouse.meta.feeder_cursors
WHERE source = 'shopify' AND stream = 'orders';
```

**Force re-run by deleting cursor**:
```sql
DELETE FROM lakehouse.meta.feeder_cursors
WHERE source = 'shopify' AND stream = 'orders';
```

Then materialize the feeder asset in Dagster UI.

**Common causes**:
- Cursor ahead of actual files (time sync issue)
- S3 permissions
- Files in unexpected path format

---

### dbt Model Failing

**Symptoms**: Feeder runs but processed tables not updating

**Check dbt logs in run output**:
```bash
# In Dagster UI, check the run logs for the feeder asset
# Or check daemon logs
kubectl logs -n lotus-lake -l component=daemon --tail=200
```

**Test dbt locally**:
```bash
cd orchestration
dbt parse --profiles-dir . --project-dir .
dbt run --select int_shopify__orders --profiles-dir . --project-dir .
```

**Common causes**:
- Missing source in `_sources.yml`
- Typo in `ref()` or `source()`
- Invalid SQL syntax
- Column type mismatch

---

### Image Not Updating in Cluster

**Symptoms**: Pushed code but pods still running old version

**Check GitRepository status**:
```bash
kubectl get gitrepository -n lotus-lake lotus-lake
```

**Force git fetch**:
```bash
kubectl annotate gitrepository -n lotus-lake lotus-lake \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
```

**Check current pod image**:
```bash
kubectl get pods -n lotus-lake -l component=user-deployments \
  -o jsonpath='{.items[*].spec.containers[*].image}'
```

**Force pod restart**:
```bash
kubectl rollout restart deployment -n lotus-lake -l component=user-deployments
```

---

### Dagster Won't Start

**Symptoms**: User deployment pod in CrashLoopBackOff

**Check pod logs**:
```bash
kubectl logs -n lotus-lake -l component=user-deployments --previous
```

**Common causes**:
- Missing environment variables (secrets not mounted)
- Python import errors
- dbt manifest not found (`target/manifest.json`)

**Verify definitions load**:
```bash
kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake -- \
  python -c "from orchestration.definitions import defs; print(defs)"
```

---

### S3 Permission Errors (403 Forbidden)

**Symptoms**: Feeder can't read/write S3 files

**Check credentials match**:
1. SeaweedFS config has the identity
2. Service secret matches SeaweedFS exactly
3. Pods were restarted after secret update

**Verify S3 access**:
```bash
kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake -- \
  env | grep S3
```

See [secrets.md](secrets.md) for credential sync requirements.

---

### CNPG Backup Failing

**Symptoms**: Scheduled backup jobs failing

**Check backup status**:
```bash
kubectl get scheduledbackup -n lotus-lake
kubectl logs -n lotus-lake -l cnpg.io/cluster=dagster-db --tail=50
```

**Common causes**:
- Credential mismatch with SeaweedFS
- Bucket doesn't exist
- S3 endpoint unreachable

---

## Debugging Commands

### View Logs

```bash
# Dagster webserver
kubectl logs -n lotus-lake -l component=webserver -f

# Dagster daemon (runs sensors, schedules)
kubectl logs -n lotus-lake -l component=daemon -f

# User deployments (asset definitions)
kubectl logs -n lotus-lake -l component=user-deployments -f

# Specific run
kubectl logs -n lotus-lake <run-pod-name>
```

### Check Deployed Code

```bash
# Verify the code in the running pod
kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake \
  -- head -50 /app/orchestration/assets.py
```

### Database Queries

```bash
# Connect to Dagster DB
kubectl exec -it -n lotus-lake dagster-db-1 -- psql -U dagster dagster

# Connect to DuckLake DB
kubectl exec -it -n lotus-lake ducklake-db-1 -- psql -U ducklake ducklake
```

### Airbyte API

```bash
# List connections
kubectl exec -n airbyte deploy/airbyte-server -- curl -s \
  "http://localhost:8001/api/v1/connections/list" \
  -H "Content-Type: application/json" \
  -d '{"workspaceId":"YOUR_WORKSPACE_ID"}' | jq '.connections[].name'

# Trigger sync
kubectl exec -n airbyte deploy/airbyte-server -- curl -s -X POST \
  "http://localhost:8001/api/v1/connections/sync" \
  -H "Content-Type: application/json" \
  -d '{"connectionId":"CONNECTION_ID"}'
```

---

## Reference

For known issues and limitations, see [tickets.md](tickets.md).
