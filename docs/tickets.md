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
