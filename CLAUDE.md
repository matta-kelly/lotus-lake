# Claude Code Workflow

Quick reference for working on Lotus Lake.

---

## After Context Compaction: Re-read the Docs

**If this session was resumed from a compacted/summarized conversation, immediately read the full docs directory to restore context:**

```bash
# Read all docs to restore full context
cat docs/*.md
```

| Doc | Purpose |
|-----|---------|
| `docs/architecture.md` | System architecture and data flow |
| `docs/adding-a-source.md` | Adding Airbyte sources and streams |
| `docs/adding-a-flow.md` | Adding dbt core/mart models |
| `docs/github-actions.md` | CI/CD pipeline |
| `docs/tickets.md` | Known issues |

**Compaction loses details. The docs have the full picture.**

---

## CRITICAL: Never Run Terraform Locally

```
╔═══════════════════════════════════════════════════════════════════════════╗
║  DO NOT RUN `terraform apply` OR `terraform plan` LOCALLY                ║
║                                                                           ║
║  tofu-controller manages Terraform state in K8s.                         ║
║  Running locally creates a SEPARATE state file that drifts.              ║
║  This causes syncs to fail with "secret not found" errors.               ║
║                                                                           ║
║  See TICKET-003 in docs/tickets.md for full explanation.                 ║
╚═══════════════════════════════════════════════════════════════════════════╝
```

**If you see `terraform.tfstate` files locally, DELETE THEM:**
```bash
rm -f orchestration/airbyte/terraform/terraform.tfstate*
rm -rf orchestration/airbyte/terraform/.terraform
```

**The ONLY way to apply Terraform changes:**
1. Edit files
2. `git commit && git push`
3. tofu-controller applies automatically

---

## Before You Execute: Stop and Assess

**Always follow this framework before making changes:**

### 1. Check Tickets and Identify Procedure Doc

**Before creating any plan of action:**

1. Review `docs/tickets.md` for open issues that may affect your change
2. Identify which procedure doc applies:
   - Adding/changing sources → `docs/adding-a-source.md`
   - Adding/changing destinations → `docs/adding-a-destination.md`
   - Adding dbt models (core layer) → `docs/adding-a-flow.md`
   - Deploying/updating Dagster → `docs/deploying-dagster.md`
   - Architecture questions → `docs/architecture.md`
3. **State the doc you're following** when presenting the plan (e.g., "Following `docs/adding-a-destination.md`")

If no procedure doc exists for your task, create one as part of the work.

### 2. Understand the Change

- What files are being modified?
- What is the expected outcome?
- What systems are affected? (Airbyte, Terraform, dbt, Dagster)

### 4. Know the Execution Path

```
Push to git
  ↓
Flux GitRepository detects (≤5 min interval)
  ↓
tofu-controller runs terraform plan
  ↓
Auto-approve + apply
  ↓
Airbyte/infrastructure updates
```

### 5. Identify Risks

| Question | Why It Matters |
|----------|----------------|
| What could fail? | Prepare mitigation |
| How will I know it failed? | Define verification steps |
| What's the rollback? | Know before you need it |

### 6. Define Verification Steps

Before committing, know exactly how you'll verify success:
- What commands will you run?
- What output do you expect?
- What's the timeline? (immediate vs. wait for interval)

### 7. Post-Execution Review (Required)

After every significant change, review what happened:

**If everything worked as expected:**
- ✅ Confirm docs are still accurate
- ✅ No action needed

**If something unexpected happened:**
1. **STOP** - Don't just work around it
2. **Understand** - Why did this happen? What assumption was wrong?
3. **Document** - Add to `docs/tickets.md` as new ticket
4. **Update** - Fix relevant docs/CLAUDE.md if process was unclear

**This is not optional.** Every bump becomes a ticket. Every lesson updates the docs. This keeps the system learnable and maintainable.

---

## GitOps Workflow

This repo uses **tofu-controller** for GitOps. You don't run terraform locally.

### Making Changes

```bash
# 1. Make changes to orchestration/airbyte/terraform/*
# 2. If stream changes, regenerate catalog:
python orchestration/airbyte/generate-catalog.py

# 3. Commit and push
git add . && git commit -m "description" && git push

# 4. Verify (wait up to 5 min for reconciliation)
kubectl get terraform -n lotus-lake
```

### Force Reconciliation

If you can't wait for the interval:
```bash
kubectl annotate terraform -n lotus-lake lotus-lake-airbyte \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
```

### Check Status

```bash
# Terraform status
kubectl get terraform -n lotus-lake

# Airbyte connections
kubectl exec -n airbyte deploy/airbyte-server -- curl -s \
  "http://localhost:8001/api/v1/connections/list" \
  -H "Content-Type: application/json" \
  -d '{"workspaceId":"b93dc139-15d7-4729-9cdc-5c754b9d9401"}' | jq '.connections[].name'
```

---

## Key Paths

| What | Where |
|------|-------|
| Airbyte terraform | `orchestration/airbyte/terraform/` |
| Stream configs | `orchestration/assets/streams/<source>/*.json` |
| Source schemas | `orchestration/assets/sources/<source>/*.json` |
| dbt core models | `orchestration/assets/core/<source>/int_*.sql` |
| dbt sources | `orchestration/assets/core/<source>/_<source>__sources.yml` |
| Dagster assets | `orchestration/assets/core/assets.py` |
| Tickets/issues | `docs/tickets.md` |

---

## Naming Conventions

### dbt Models

Double underscore separates layer from entity:

```
int_shopify__orders
│   │        │
│   │        └── entity
│   └── source
└── layer (int = intermediate)
```

| Prefix | Layer |
|--------|-------|
| `stg_` | Staging - light cleaning |
| `int_` | Intermediate - business logic |
| `fct_` | Fact tables |
| `dim_` | Dimension tables |

### Stream Configs

```
orchestration/assets/streams/shopify/
├── orders.json        # Stream config (what we sync)
├── customers.json     # Stream config
└── _catalog.json      # Generated - don't edit manually
```

---

## Common Tasks

### Add a Stream to Existing Source

```bash
# 1. Check available fields
cat orchestration/assets/sources/SOURCE/STREAM.json

# 2. Create stream config
vim orchestration/assets/streams/SOURCE/STREAM.json

# 3. Regenerate catalog
python orchestration/airbyte/generate-catalog.py

# 4. Add dbt source (if new table)
vim orchestration/assets/core/SOURCE/_SOURCE__sources.yml

# 5. Create dbt model
vim orchestration/assets/core/SOURCE/int_SOURCE__STREAM.sql

# 6. Commit and push
```

### Trigger Backfill

```bash
# 1. Set backfill flag
vim orchestration/assets/streams/SOURCE/STREAM.json
# Set "backfill": true

# 2. Push, wait for sync to complete

# 3. Set "backfill": false and push again
```

---

## CRITICAL: Dagster Sensor & Asset Wiring

### Stream Config is Source of Truth

The stream config filename drives ALL naming:

```
streams/{source}/{stream}.json
       ↓
┌──────┴──────────────────────────────────────────────────┐
│                                                          │
▼                                                          ▼
S3 Path (Hive format)                            Code Generation
raw/{source}/{stream}/year=YYYY/month=MM/day=DD/          │
       └── enables partition pruning      ┌───────────────┼───────────────┐
                                          ▼               ▼               ▼
                                       Sensor          Asset           dbt Model
                                  {source}_{stream}  {source}_{stream}  int_{source}__{stream}
                                     _sensor            _dbt              .sql (incremental)
```

### Asset Key Format

dagster-dbt creates asset keys as `["main", "model_name"]`:
```python
asset_key = AssetKey(["main", f"int_{source}__{stream}"])
```

### Sensor Requirements

**Sensors MUST have `asset_selection` on the decorator:**
```python
@sensor(
    name=f"{source}_{stream}_sensor",
    asset_selection=[asset_key],  # REQUIRED - defines target
    ...
)
def _sensor(context):
    yield RunRequest(run_key="...")  # Uses decorator's target
```

Without `asset_selection`, you get:
> "Sensor evaluation function returned a RunRequest for a sensor lacking a specified target"

### Factory Pattern for Assets

Each stream needs its OWN `@dbt_assets` decorator (not one bundled decorator):

```python
# CORRECT - one asset per stream
def make_dbt_asset(source: str, stream: str):
    @dbt_assets(
        manifest=DBT_MANIFEST,
        select=f"tag:{source}__{stream}",  # Select ONLY this model
        name=f"{source}_{stream}_dbt",
    )
    def _asset(context, dbt):
        yield from dbt.cli(["run"], context=context).stream()
    return _asset

# WRONG - bundles all models together
@dbt_assets(select="tag:core")  # Triggers ALL core models at once
def core_dbt_models(...):
```

### Verification After Code Push

**ALWAYS verify deployed code matches repo before testing:**
```bash
# Check sensor code
kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake \
  -- grep -A3 "asset_selection" /app/orchestration/sensors.py

# Check asset factory
kubectl exec -n lotus-lake deploy/dagster-dagster-user-deployments-lotus-lake \
  -- head -50 /app/orchestration/assets/core/assets.py

# Force image pull if stale
kubectl delete pod -n lotus-lake -l component=user-deployments
```

**CI takes ~2 minutes. Don't test until new image is deployed.**

---

## Troubleshooting

### Terraform Stuck/Failing

```bash
# Check status
kubectl get terraform -n lotus-lake -o yaml | grep -A20 status

# Force new reconciliation
kubectl annotate terraform -n lotus-lake lotus-lake-airbyte \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
```

### Known Limitation: TICKET-001

When terraform fails, retries use **cached source artifacts**. Your fix won't be picked up until:
- Next interval cycle (5 min), OR
- Manual annotation (above), OR
- Change to h-kube terraform spec

See `docs/tickets.md` for details.

---

## Reference Docs

| Doc | Purpose |
|-----|---------|
| `docs/architecture.md` | System architecture and data flow |
| `docs/adding-a-source.md` | Adding Airbyte sources and streams |
| `docs/adding-a-destination.md` | Adding Airbyte destinations |
| `docs/adding-a-flow.md` | Adding dbt core/mart models |
| `docs/deploying-dagster.md` | Dagster deployment and configuration |
| `docs/github-actions.md` | CI/CD pipeline and image builds |
| `docs/tickets.md` | Known issues and TODOs |
