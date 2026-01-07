# Tickets & Known Limitations

Tracking issues and limitations for the lotus-lake platform.

---

## TICKET-001: Tofu-Controller Source Refresh on Retry

**Status:** Open
**Priority:** Medium
**Component:** tofu-controller / Flux GitOps

### Problem Statement

When a Terraform resource fails, the tofu-controller enters a retry loop that uses cached source artifacts. Even if you push a fix, Terraform won't pick up the new code until:

1. The next full `interval` cycle completes, OR
2. The Terraform spec itself changes, OR
3. Manual intervention (`kubectl annotate`)

### Workarounds

| Workaround | Pros | Cons |
|------------|------|------|
| Reduce `interval` (10m -> 5m) | Faster recovery | More cluster load |
| Manual `kubectl annotate` | Immediate | Requires cluster access |

### Action Items

- [ ] Open GitHub issue on flux-iac/tofu-controller
- [ ] Consider Notification Controller webhook as interim solution

---

## TICKET-002: State Drift from Local Terraform Runs

**Status:** Open
**Priority:** Critical
**Component:** Terraform State / tofu-controller

### Problem Statement

Running `terraform apply` locally while tofu-controller manages the same resources creates two separate states that drift apart, causing:
- Syncs fail with "secret not found" errors
- HTTP 409 conflicts when triggering syncs
- Resources point to deleted/orphaned IDs

### Prevention (CRITICAL)

1. **NEVER run `terraform apply` locally**
2. **NEVER run `terraform plan` locally**
3. **Delete local .tfstate files** if they exist
4. **All Terraform changes go through git push**

### How to Fix (Nuclear Option)

See `CLAUDE.md` for full recovery procedure. Summary:
1. Delete all Airbyte resources via API
2. Delete Terraform state secret
3. Force tofu-controller to recreate

---

## TICKET-003: Terraform Doesn't Detect Secret Value Changes

**Status:** Open
**Priority:** Medium
**Component:** tofu-controller / Terraform

### Problem Statement

When you update values in the `lotus-lake-terraform-vars` secret, Terraform may not detect the change due to runner pod caching.

### Workaround

Update the resource directly via Airbyte API, then force terraform reconcile to sync state.

---

## TICKET-004: SeaweedFS S3 Gateway Doesn't Hot-Reload Credentials

**Status:** Open
**Priority:** Medium
**Component:** SeaweedFS / S3 Gateway

### Problem Statement

When adding/updating S3 identities in the `seaweedfs-s3-config` secret, the S3 gateway doesn't automatically reload. New credentials fail with "access key ID does not exist".

### How to Fix

Restart the S3 gateway after adding/updating credentials:

```bash
kubectl rollout restart deployment/seaweedfs-s3 -n seaweedfs
```

---

## Template for New Tickets

```markdown
## TICKET-XXX: Title

**Status:** Open | In Progress | Resolved
**Priority:** Low | Medium | High | Critical
**Component:** affected component

### Problem Statement
What's broken or limiting?

### Workarounds
Current ways to deal with it.

### Action Items
- [ ] Concrete next steps
```
