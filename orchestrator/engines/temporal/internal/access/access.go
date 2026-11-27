// Package access is the one statement of who may see a catalog resource:
// RBAC (allowedRoles, Aktis ADR 0004) ANDed with private scoping
// (allowedPrincipals, ADR 0060).
//
// Every place that decides visibility — the vector store's query filter and
// its by-id re-check, knowledge-base member gating, corpus reads — calls into
// here, so the two axes cannot drift apart between retrieval and execution.
package access

// Viewer is who is asking, reduced to what visibility depends on.
type Viewer struct {
	// Roles are the caller's RBAC roles.
	Roles []string

	// Principals are the subjects this caller may be matched against a
	// resource's allowedPrincipals as. Empty for a caller who does not
	// identify one person (a shared or service subject), which makes every
	// private resource invisible to them — the fail-closed default.
	Principals []string
}

// Audience is who a resource admits.
type Audience struct {
	Roles []string

	// Unrestricted admits any role (a skill with no tool/agent refs,
	// ADR 0011). It waives the role check only: a private audience still
	// requires a principal match.
	Unrestricted bool

	// Private marks a resource scoped to Principals. It is a separate flag
	// rather than len(Principals) > 0 because an empty private set is a real
	// state — a skill whose own list and a referenced tool's list do not
	// intersect — and it must mean "no one", not "public".
	Private    bool
	Principals []string
}

// Admits reports whether v may see a resource with audience a.
func (v Viewer) Admits(a Audience) bool {
	if !a.Unrestricted && !overlaps(v.Roles, a.Roles) {
		return false
	}
	return !a.Private || overlaps(v.Principals, a.Principals)
}

// AdmitsPrincipal is the principal half of Admits alone, for call sites that
// already checked roles against something other than an Audience.
func (v Viewer) AdmitsPrincipal(allowedPrincipals []string) bool {
	return len(allowedPrincipals) == 0 || overlaps(v.Principals, allowedPrincipals)
}

// Of is the audience of a resource that declares its own allowedRoles and
// allowedPrincipals directly (Tool, LocalTool, MCPTool, Agent, Corpus).
func Of(roles, principals []string) Audience {
	return Audience{Roles: roles, Private: len(principals) > 0, Principals: principals}
}

// Narrow intersects a private audience into an existing principal scope.
// Public inputs leave the scope unchanged, so a skill referencing only public
// tools stays public and a private tool can only ever shrink it.
func Narrow(private bool, principals []string, nextPrivate bool, next []string) (bool, []string) {
	if !nextPrivate {
		return private, principals
	}
	if !private {
		return true, dedupe(next)
	}
	return true, intersect(principals, next)
}

// Widen unions principal scopes for a resource usable by anyone who can use
// ANY of its parts (a knowledge base's generated tools, ADR 0039 §4). One
// public part makes the whole public.
func Widen(scopes [][]string, private []bool) (bool, []string) {
	if len(scopes) == 0 {
		return false, nil
	}
	seen := map[string]bool{}
	out := []string{}
	for i, scope := range scopes {
		if !private[i] {
			return false, nil
		}
		for _, p := range scope {
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	return true, out
}

func overlaps(held, allowed []string) bool {
	if len(held) == 0 || len(allowed) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(held))
	for _, h := range held {
		set[h] = struct{}{}
	}
	for _, a := range allowed {
		if _, ok := set[a]; ok {
			return true
		}
	}
	return false
}

func intersect(a, b []string) []string {
	in := make(map[string]bool, len(b))
	for _, x := range b {
		in[x] = true
	}
	out := []string{}
	for _, x := range dedupe(a) {
		if in[x] {
			out = append(out, x)
		}
	}
	return out
}

func dedupe(xs []string) []string {
	seen := make(map[string]bool, len(xs))
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}
