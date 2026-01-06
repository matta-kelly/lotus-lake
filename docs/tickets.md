# Tickets & Known Limitations

Tracking issues, limitations, and potential solutions for the lotus-lake platform.

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
- [ ] Add pre-commit terraform validate to lotus-lake CI
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
