package gateway

// Only an actual reduction of authoritative privileges can consume emergency
// journal space. A new generation/version by itself is not a reduction. Repeats
// keep existing CAS/audit semantics but must use ordinary capacity.
func restrictivePrincipalChange(old, next *principalState, system string, action AuditAction) bool {
	if old == nil || next == nil {
		return false
	}
	switch action {
	case AuditActionBindingRevoke:
		before, after := old.bindings[system], next.bindings[system]
		return before != nil && after != nil && before.info.State == BindingActive && after.info.State == BindingRevoked && after.credential == ""
	case AuditActionPrincipalDisable:
		return !old.disabled && next.disabled
	case AuditActionPolicyUpdate:
		before, after := map[string]bool{}, map[string]bool{}
		for _, name := range old.allowed {
			before[name] = true
		}
		for _, name := range next.allowed {
			if !before[name] {
				return false
			}
			after[name] = true
		}
		return len(after) < len(before)
	}
	return false
}
