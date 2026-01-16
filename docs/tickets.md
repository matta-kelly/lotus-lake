# Tickets & Known Limitations

Tracking issues, limitations, and potential solutions for the lotus-lake platform.

---

# TODOs: Intended GitOps Functionality

These are the workflows the platform should support. Use these as acceptance criteria.

---

## TODO-001: Add a New Source

**Status:** Implemented
**Workflow:**

1. Edit `orchestration/airbyte/terraform/sources.tf`
   - Add new `airbyte_source` resource (e.g., Facebook Ads)
2. `git add . && git commit -m "Add Facebook Ads source" && git push`
3. Within 5-15 minutes:
   - Flux GitRepository detects new commit
   - tofu-controller runs `terraform plan`
   - Plan shows: "1 to add"
   - Auto-approves and applies
   - New source appears in Airbyte UI
4. No manual intervention required

---

## TODO-002: Modify Connection Schema

**Status:** Implemented
**Workflow:**

1. Edit stream config (e.g., `orchestration/dag/streams/shopify/orders.json`)
   - Add new field selection or change sync_mode
2. Regenerate catalog:
   ```bash
   python orchestration/airbyte/generate-catalog.py
   ```
3. `git add . && git commit -m "Add field to orders" && git push`
4. tofu-controller detects catalog change
   - Updates `airbyte_connection` resource
   - Airbyte connection schema updates automatically

---

## TODO-003: Remove a Source

**Status:** Implemented
**Workflow:**

1. Delete the source resource from `sources.tf`
2. Delete associated connection from `connections.tf`
3. `git add . && git commit -m "Remove old source" && git push`
4. tofu-controller runs plan:
   - Plan shows: "2 to destroy" (connection first, then source)
   - Auto-approves and applies
   - Resources removed from Airbyte cleanly
5. No orphaned resources left behind (`destroyResourcesOnDeletion: true`)

---

## TODO-004: Trigger a Backfill

**Status:** Implemented
**Workflow:**

1. Edit stream config (e.g., `orchestration/dag/streams/shopify/orders.json`)
   - Set `"backfill": true`
2. `git add . && git commit -m "Trigger orders backfill" && git push`
3. tofu-controller applies:
   - Detects backfill flag
   - Calls Airbyte API to reset connection
   - Backfill starts
4. After backfill completes:
   - Set `"backfill": false`
   - Commit and push
   - System returns to stable state (no perpetual drift)

---

## What "Stable" Looks Like

```bash
$ kubectl get terraform -n lotus-lake
NAME                 READY   STATUS                                    AGE
lotus-lake-airbyte   True    Applied successfully: main@sha1:abc123   5m

# Events should be quiet between actual changes - no perpetual drift
```

---

# Known Issues & Limitations

---

## TICKET-001: Tofu-Controller Source Refresh on Retry

**Status:** Open
**Priority:** Medium
**Discovered:** 2026-01-06
**Component:** tofu-controller / Flux GitOps

### Problem Statement

When a Terraform resource fails (e.g., `terraform plan` error), the tofu-controller enters a **retry loop that uses cached source artifacts**. Even if you push a fix to the Git repository, the Terraform will NOT pick up the new code until:

1. The next full `interval` cycle completes, OR
2. The Terraform spec itself changes (triggers new generation), OR
3. Manual intervention (`kubectl annotate`)

### Observed Behavior

```
Timeline:
22:06 - Terraform applies successfully (commit A)
22:08 - Push commit B (broken code) -> GitRepository picks it up
22:09 - Terraform tries commit B -> FAILS
22:10 - Push commit C (fix) -> GitRepository picks it up
22:11 - Terraform retries -> Still uses commit B (cached) -> FAILS
22:12 - Terraform retries -> Still uses commit B -> FAILS
...continues until interval expires...
22:18 - New interval cycle -> Finally picks up commit C -> SUCCESS
```

### Why This Breaks GitOps

The GitOps promise is: **push code -> system converges automatically**

This limitation breaks that promise because:
- Fast iteration during development is blocked
- You can't quickly fix a broken push
- The feedback loop extends from seconds to minutes (interval duration)

### Current Workarounds

| Workaround | Pros | Cons |
|------------|------|------|
| Reduce `interval` (10m -> 5m) | Faster recovery | More cluster load, still not instant |
| Manual `kubectl annotate` | Immediate | Not GitOps, requires cluster access |
| Change Terraform spec in h-kube | Triggers new generation | Requires commit to h-kube for every lotus-lake fix |

### Ideal Solution

A `spec.refreshSourceOnRetry: true` option that tells tofu-controller:
> "On each retry, re-fetch the source artifact before attempting plan"

### Potential Custom Solutions

1. **Notification Controller + Webhook**
   - Flux Notification Controller watches GitRepository
   - On new artifact, webhook triggers Terraform annotation
   - Complexity: Medium, requires additional infra

2. **Custom Controller/Operator**
   - Watch GitRepository artifacts
   - When artifact changes, patch dependent Terraform resources
   - Complexity: High, custom code to maintain

3. **Pre-commit Validation**
   - Run `terraform validate` locally before push
   - Catch errors before they hit the cluster
   - Limitation: Can't catch runtime issues (API connectivity, etc.)

4. **Feature Request to tofu-controller**
   - Open issue on https://github.com/flux-iac/tofu-controller
   - Request `refreshSourceOnRetry` or similar
   - Timeline: Unknown, depends on maintainers

### Related Configuration

Current lotus-lake-terraform.yaml settings (h-kube):
```yaml
spec:
  interval: 5m          # Reduced from 10m for faster recovery
  retryInterval: 1m     # Explicit retry timing
  approvePlan: auto
```

### Action Items

- [ ] Open GitHub issue on flux-iac/tofu-controller requesting this feature
- [ ] Consider implementing Notification Controller webhook as interim solution
- [x] Add pre-commit terraform validate to lotus-lake CI (`.github/workflows/build.yaml`)
- [ ] Document manual recovery procedure for stuck Terraform resources

### References

- [tofu-controller docs](https://flux-iac.github.io/tofu-controller/)
- [Flux GitRepository](https://fluxcd.io/flux/components/source/gitrepositories/)

---

---

## TICKET-002: Orphaned Airbyte Connections from State Mismatch

**Status:** Resolved (documented for future reference)
**Priority:** Medium
**Discovered:** 2026-01-06
**Component:** Airbyte / Terraform State

### Problem Statement

Duplicate Airbyte connections appear and persist even after deletion attempts. This happens when Terraform state gets out of sync with Airbyte reality.

### Root Cause

Terraform state (stored in K8s secret `tfstate-default-lotus-lake-airbyte`) only knows about connections it created. If connections are created by:
- Manual UI actions
- A different Terraform run
- State loss/reset

...then Terraform will create NEW connections alongside existing ones, resulting in duplicates.

### How to Diagnose

1. **List all connections in Airbyte:**
```bash
kubectl exec -n airbyte deploy/airbyte-server -- curl -s \
  "http://localhost:8001/api/v1/connections/list" \
  -H "Content-Type: application/json" \
  -d '{"workspaceId":"YOUR_WORKSPACE_ID"}' | jq '[.connections[] | {id: .connectionId, name: .name, status: .status, created: .created_at}]'
```

2. **List connections in Terraform state:**
```bash
kubectl get secret tfstate-default-lotus-lake-airbyte -n lotus-lake \
  -o jsonpath='{.data.tfstate}' | base64 -d | gunzip | \
  jq '[.resources[] | select(.type == "airbyte_connection") | {name: .instances[0].attributes.name, connection_id: .instances[0].attributes.connection_id}]'
```

3. **Compare:** Connections in Airbyte but NOT in Terraform state are orphans.

### How to Fix

Delete orphaned connections via API:
```bash
kubectl exec -n airbyte deploy/airbyte-server -- curl -s -X POST \
  "http://localhost:8001/api/v1/connections/delete" \
  -H "Content-Type: application/json" \
  -d '{"connectionId":"ORPHAN_CONNECTION_ID"}'
```

### Prevention

1. **Never create connections manually** if Terraform manages them
2. **Import existing resources** before running Terraform on an existing workspace
3. **Protect Terraform state** - don't delete the tfstate secret
4. **Use `destroyResourcesOnDeletion: true`** in tofu-controller spec

### Takeaway

When debugging duplicate Airbyte resources:
1. Always compare Airbyte reality vs Terraform state
2. Terraform will recreate resources it owns if you delete them manually
3. Orphans (not in state) must be deleted via API, not Terraform

---

## TICKET-003: State Drift from Local Terraform Runs

**Status:** Open (Critical)
**Priority:** Critical
**Discovered:** 2026-01-07
**Component:** Terraform State / tofu-controller

### Problem Statement

Running `terraform apply` locally while tofu-controller manages the same resources creates **two separate states** that drift apart, causing:
- Syncs fail with "secret not found" errors
- HTTP 409 conflicts when triggering syncs
- Resources point to deleted/orphaned IDs
- "Applied successfully" lies - Terraform trusts stale state

### Root Cause

```
tofu-controller state: stored in K8s secret (tfstate-default-lotus-lake-airbyte)
Local terraform state: stored in terraform.tfstate file

These are COMPLETELY INDEPENDENT. Changes in one don't affect the other.
```

**What happens when you run terraform locally:**

```
1. Local terraform sees no state (or old state)
2. Creates NEW sources/connections with NEW IDs
3. Saves to LOCAL terraform.tfstate
4. tofu-controller's K8s state still has OLD IDs
5. tofu-controller "applies successfully" (trusts its stale state)
6. Connections in K8s state point to DELETED sources
7. Syncs fail: "secret not found for source <old-id>"
```

### How to Diagnose

**Check for local state file:**
```bash
ls -la orchestration/airbyte/terraform/*.tfstate*
# If these exist, you have a problem
```

**Compare states:**
```bash
# Local state
cat orchestration/airbyte/terraform/terraform.tfstate | \
  jq '[.resources[] | select(.type == "airbyte_source") | {name: .name, id: .instances[0].attributes.source_id}]'

# K8s state
kubectl get secret tfstate-default-lotus-lake-airbyte -n lotus-lake \
  -o jsonpath='{.data.tfstate}' | base64 -d | gunzip | \
  jq '[.resources[] | select(.type == "airbyte_source") | {name: .name, id: .instances[0].attributes.source_id}]'

# If IDs don't match, states are drifted
```

**Check Airbyte reality:**
```bash
kubectl exec -n airbyte deploy/airbyte-server -- curl -s \
  "http://localhost:8001/api/v1/sources/list" \
  -H "Content-Type: application/json" \
  -d '{"workspaceId":"YOUR_WORKSPACE_ID"}' | jq '[.sources[] | {name: .name, id: .sourceId}]'
```

### How to Fix (Nuclear Option)

When states are drifted, the cleanest fix is to delete everything and let Terraform recreate:

**1. Delete all Airbyte resources:**
```bash
# Get workspace ID
WORKSPACE_ID="b93dc139-15d7-4729-9cdc-5c754b9d9401"

# Delete connections first (they reference sources)
kubectl exec -n airbyte deploy/airbyte-server -- curl -s \
  "http://localhost:8001/api/v1/connections/list" \
  -H "Content-Type: application/json" \
  -d "{\"workspaceId\":\"$WORKSPACE_ID\"}" | \
  jq -r '.connections[].connectionId' | while read id; do
    kubectl exec -n airbyte deploy/airbyte-server -- curl -s -X POST \
      "http://localhost:8001/api/v1/connections/delete" \
      -H "Content-Type: application/json" \
      -d "{\"connectionId\":\"$id\"}"
    echo "Deleted connection $id"
  done

# Delete sources
kubectl exec -n airbyte deploy/airbyte-server -- curl -s \
  "http://localhost:8001/api/v1/sources/list" \
  -H "Content-Type: application/json" \
  -d "{\"workspaceId\":\"$WORKSPACE_ID\"}" | \
  jq -r '.sources[].sourceId' | while read id; do
    kubectl exec -n airbyte deploy/airbyte-server -- curl -s -X POST \
      "http://localhost:8001/api/v1/sources/delete" \
      -H "Content-Type: application/json" \
      -d "{\"sourceId\":\"$id\"}"
    echo "Deleted source $id"
  done

# Delete destination (if needed)
kubectl exec -n airbyte deploy/airbyte-server -- curl -s \
  "http://localhost:8001/api/v1/destinations/list" \
  -H "Content-Type: application/json" \
  -d "{\"workspaceId\":\"$WORKSPACE_ID\"}" | \
  jq -r '.destinations[].destinationId' | while read id; do
    kubectl exec -n airbyte deploy/airbyte-server -- curl -s -X POST \
      "http://localhost:8001/api/v1/destinations/delete" \
      -H "Content-Type: application/json" \
      -d "{\"destinationId\":\"$id\"}"
    echo "Deleted destination $id"
  done
```

**2. Delete Terraform state:**
```bash
# Delete K8s state
kubectl delete secret tfstate-default-lotus-lake-airbyte -n lotus-lake

# Delete local state
rm -f orchestration/airbyte/terraform/terraform.tfstate*
rm -rf orchestration/airbyte/terraform/.terraform
```

**3. Force tofu-controller to recreate:**
```bash
kubectl annotate terraform -n lotus-lake lotus-lake-airbyte \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
```

**4. Verify:**
```bash
# Wait for apply
kubectl get terraform -n lotus-lake -w

# Check sources/connections created
kubectl exec -n airbyte deploy/airbyte-server -- curl -s \
  "http://localhost:8001/api/v1/connections/list" \
  -H "Content-Type: application/json" \
  -d '{"workspaceId":"YOUR_WORKSPACE_ID"}' | jq '.connections[].name'
```

### Prevention (CRITICAL RULES)

1. **NEVER run `terraform apply` locally** - tofu-controller owns this
2. **NEVER run `terraform plan` locally** - it can create state files
3. **Delete local .tfstate files** - add to .gitignore (already done)
4. **All Terraform changes go through git push** - that's the GitOps contract
5. **CI blocks commits with state files** - `.github/workflows/build.yaml` checks for `.tfstate*` and `.terraform/`

If you need to debug Terraform:
- Read the tofu-controller logs
- Check the stored plan: `kubectl get configmap tfplan-default-lotus-lake-airbyte -n lotus-lake`
- Use `terraform validate` (doesn't touch state) if needed

### Related

- TICKET-002: Orphaned connections (same root cause - state mismatch)
- TICKET-001: Retry caching (compounds the problem)

---

## TICKET-004: Terraform Doesn't Detect Secret Value Changes

**Status:** Open
**Priority:** Medium
**Discovered:** 2026-01-07
**Component:** tofu-controller / Terraform

### Problem Statement

When you update values in the `lotus-lake-terraform-vars` secret (e.g., changing credentials), **Terraform does not detect the change** and won't update the affected resources.

### Root Cause

Terraform stores the **computed output** in state, not variable references:

```
# What's in state:
connection_configuration = {"access_key_id": "old_value", ...}

# What Terraform compares:
- State: {"access_key_id": "old_value"}
- Plan:  {"access_key_id": "new_value"}  ← Uses new secret value
- Result: Detects change, plans update
```

**But here's the catch:** tofu-controller caches the runner pod environment. Secret changes may not propagate immediately to the next terraform run.

### Observed Behavior

1. Update `lotus-lake-secrets.yaml` with new credentials
2. Commit, push, Flux applies new secret to cluster
3. Force tofu-controller reconcile
4. Terraform says "Applied successfully" but **doesn't update resources**
5. Resources still have old credentials, connections fail

### Workaround

Update the resource directly via Airbyte API, then let terraform sync state:

```bash
# 1. Update destination via API
kubectl exec -n airbyte deploy/airbyte-server -- curl -s -X POST \
  "http://localhost:8001/api/v1/destinations/update" \
  -H "Content-Type: application/json" \
  -d '{
    "destinationId": "YOUR_DESTINATION_ID",
    "name": "S3 Data Lake",
    "connectionConfiguration": {
      "access_key_id": "NEW_VALUE",
      "secret_access_key": "NEW_SECRET",
      ...rest of config...
    }
  }'

# 2. Force terraform reconcile to sync state
kubectl annotate terraform -n lotus-lake lotus-lake-airbyte \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
```

### Prevention

When changing credentials in terraform vars:
1. Update the h-kube secret
2. Update the resource via API with same values
3. Force terraform reconcile
4. Verify state matches reality

### Related

- TICKET-003: State drift (similar symptom, different cause)

---

## TICKET-005: SeaweedFS S3 Gateway Doesn't Hot-Reload Credentials

**Status:** Open
**Priority:** Medium
**Discovered:** 2026-01-07
**Component:** SeaweedFS / S3 Gateway

### Problem Statement

When adding or updating S3 identities in the `seaweedfs-s3-config` secret, the SeaweedFS S3 gateway **does not automatically reload** the credentials. Connections using new credentials will fail with:

```
The access key ID you provided does not exist in our records.
```

### Observed Behavior

1. Add new identity to `seaweedfs-s3-config` secret (via h-kube)
2. Flux applies the secret to the cluster
3. Airbyte destination uses the new credentials
4. Connection test fails: "access key ID does not exist"
5. SeaweedFS S3 gateway logs show: `could not find accessKey lotus-lake-s3`

### Root Cause

The SeaweedFS S3 gateway reads the config secret at startup and doesn't watch for changes. New identities aren't available until the pod restarts.

### How to Fix

Restart the S3 gateway after adding/updating credentials:

```bash
kubectl rollout restart deployment/seaweedfs-s3 -n seaweedfs
```

Wait for the new pod to be ready, then retry the connection.

### Prevention

When adding new S3 identities to h-kube:
1. Update the `seaweedfs-s3-config` secret
2. Commit and push to h-kube
3. **Restart the S3 gateway** (or add to the h-kube workflow)
4. Then configure Airbyte/other services to use the new credentials

### Potential Solutions

1. **Add annotation to trigger restart** - Use Flux/Kustomize to add a hash annotation that changes when the secret changes, triggering a rollout
2. **Use Reloader** - Install [stakater/Reloader](https://github.com/stakater/Reloader) to auto-restart deployments when secrets change
3. **SeaweedFS config watch** - Check if SeaweedFS supports hot-reloading (unlikely based on current behavior)

### Action Items

- [ ] Consider adding Reloader to h-kube infrastructure
- [ ] Or add secret hash annotation to seaweedfs-s3 deployment
- [x] Document manual restart procedure (this ticket)

### Related

- TICKET-004: Terraform doesn't detect secret changes (similar pattern)

---

## TICKET-006: Cube.js Health DOWN - DuckLake S3 Access

**Status:** In Progress (fix pushed, awaiting verification)
**Priority:** Critical
**Discovered:** 2026-01-15
**Component:** Cube.js / DuckDB / DuckLake

### Problem Statement

Cube.js pods start but immediately fail health checks, returning `{"health":"DOWN"}`. This causes:
- Restart loops on the Cube.js deployment
- Flux dependency chain blocked (infrastructure-services → lotus-lake-deploy → pods)
- Entire lotus-lake namespace stuck waiting for Cube to be healthy

### Root Cause

**DuckLake requires `CREATE SECRET` for S3 access** - environment variables alone don't work.

When DuckDB attaches a DuckLake catalog with `DATA_PATH 's3://...'`, it needs S3 credentials configured via SQL `CREATE SECRET`, not just env vars. The Cube.js config was missing this.

**Working pattern (from Dagster's `lib.py`):**
```python
conn.execute(f"""
    CREATE SECRET s3_secret (
        TYPE S3,
        KEY_ID '{s3_key}',
        SECRET '{s3_secret}',
        ENDPOINT '{s3_endpoint}',
        USE_SSL false,
        URL_STYLE 'path'
    )
""")
```

**Broken Cube.js config (before fix):**
```javascript
// Relied on CUBEJS_DB_DUCKDB_S3_* env vars
// These work for direct DuckDB S3 access but NOT for DuckLake attach
```

### The Fix

**Commit:** `eed6ca8` (pushed to main, awaiting CI build)
**File:** `orchestration/cube/cube.js`

Added `CREATE SECRET` and `LOAD httpfs` to the `initSql`:

```javascript
return new DuckDBDriver({
  database: ':memory:',
  initSql: `
    INSTALL ducklake; LOAD ducklake;
    INSTALL httpfs; LOAD httpfs;

    CREATE SECRET s3_secret (
      TYPE S3,
      KEY_ID '${s3Key}',
      SECRET '${s3Secret}',
      ENDPOINT '${s3Endpoint}',
      USE_SSL false,
      URL_STYLE 'path'
    );

    ATTACH 'ducklake:postgres:host=${pgHost} port=5432 dbname=ducklake user=ducklake password=${pgPass}'
    AS lakehouse (DATA_PATH 's3://landing/raw/');
  `,
});
```

### Current Status

- ✅ Fix committed and pushed (commit `eed6ca8`)
- ⏳ Waiting for CI to build new `lotus-lake-cube` image
- ⏳ Once image is built, Flux will deploy automatically
- ⏳ Need to verify health endpoint returns `{"health":"UP"}`

### Verification Steps

Once CI builds the new image:

```bash
# 1. Check if new image is deployed
kubectl get pods -n lotus-lake -l app=cube -o jsonpath='{.items[0].spec.containers[0].image}'

# 2. Check pod status (should be Running, not CrashLoopBackOff)
kubectl get pods -n lotus-lake -l app=cube

# 3. Check health endpoint
kubectl exec -n lotus-lake deploy/cube -- curl -s http://localhost:4000/readiness

# 4. Check logs for successful DuckLake attach
kubectl logs -n lotus-lake -l app=cube --tail=100 | grep -i "lakehouse\|ducklake\|s3"

# 5. Verify Flux dependency chain unblocks
kubectl get kustomization -n flux-system infrastructure-services
kubectl get helmrelease -n lotus-lake
```

### Key Cube.js Configuration Facts

Learned during investigation - document for future reference:

| Aspect | Value/Notes |
|--------|-------------|
| **Version** | 1.6.3 (DuckLake support added in 1.3.26) |
| **REST API Port** | 4000 |
| **PostgreSQL Protocol** | 5432 (for BI tools like Power BI) |
| **Health Endpoints** | `/livez`, `/readiness` |
| **Config File** | `orchestration/cube/cube.js` |
| **Models** | Auto-discovered from `model/` folder |
| **DuckLake Catalog** | Attached as `lakehouse` alias |

**Environment Variables:**
| Variable | Purpose |
|----------|---------|
| `CUBEJS_DB_DUCKDB_S3_ACCESS_KEY_ID` | S3 access key (for CREATE SECRET) |
| `CUBEJS_DB_DUCKDB_S3_SECRET_ACCESS_KEY` | S3 secret key (for CREATE SECRET) |
| `CUBEJS_DB_DUCKDB_S3_ENDPOINT` | SeaweedFS endpoint |
| `DUCKLAKE_DB_HOST` | PostgreSQL catalog host |
| `DUCKLAKE_DB_PASSWORD` | PostgreSQL catalog password |
| `CUBEJS_API_SECRET` | API authentication |
| `CUBEJS_SQL_USER` | PostgreSQL protocol username (default: `cube`) |
| `CUBEJS_SQL_PASSWORD` | PostgreSQL protocol password (defaults to API_SECRET) |

### Related Changes (Same Session)

Also completed in this session:
- Renamed `minio_*` secrets to `s3_*` across both repos
- Added separate `cubejs_api_secret` (was sharing `minio_password`)
- Updated 9 files total (h-kube + lotus-lake)

### Action Items

- [ ] Verify Cube.js health goes UP after new image deploys
- [ ] Test PostgreSQL protocol connection from laptop
- [ ] Test REST API via HTTPS (Traefik ingress)
- [ ] Document connection instructions once verified working

### References

- Cube.js DuckDB Driver: https://cube.dev/docs/product/configuration/data-sources/duckdb
- DuckLake Extension: https://duckdb.org/docs/extensions/ducklake
- Working example: `orchestration/dag/landing/lib.py` (Dagster's DuckLake access)

---

## Template for New Tickets

```markdown
## TICKET-XXX: Title

**Status:** Open | In Progress | Resolved
**Priority:** Low | Medium | High | Critical
**Discovered:** YYYY-MM-DD
**Component:** affected component

### Problem Statement
What's broken or limiting?

### Observed Behavior
What actually happens?

### Expected Behavior
What should happen?

### Workarounds
Current ways to deal with it.

### Potential Solutions
Ideas for fixing properly.

### Action Items
- [ ] Concrete next steps

### References
- Links to relevant docs/issues
```
