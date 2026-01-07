# Claude Code Workflow

Quick reference for working on Lotus Lake.

---

## Before You Execute: Stop and Assess

**Always follow this framework before making changes:**

### 1. Understand the Change

- What files are being modified?
- What is the expected outcome?
- What systems are affected? (Airbyte, Terraform, dbt, Dagster)

### 2. Know the Execution Path

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

### 3. Identify Risks

| Question | Why It Matters |
|----------|----------------|
| What could fail? | Prepare mitigation |
| How will I know it failed? | Define verification steps |
| What's the rollback? | Know before you need it |

### 4. Define Verification Steps

Before committing, know exactly how you'll verify success:
- What commands will you run?
- What output do you expect?
- What's the timeline? (immediate vs. wait for interval)

### 5. Check Open Tickets

Review `docs/tickets.md` for known limitations that may affect your change.

### 6. Post-Execution Review (Required)

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

### 7. Reference Tickets Before Execution

Before executing, always check `docs/tickets.md` for known issues that may affect your change. If you hit a known issue, that's expected - not a new problem

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

- `docs/adding-a-source.md` - Full guide for adding sources
- `docs/tickets.md` - Known issues and TODOs
