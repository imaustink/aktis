package catalog

import "github.com/aktisai/orchestrator/engines/temporal/internal/access"

// DeriveSkillAccess computes a skill's retrieval audience (Aktis
// ADR 0011: skills carry no RBAC of their own — "skills aren't dangerous,
// tools are"):
//
//   - no tool/agent refs        → Unrestricted (any resolved identity)
//   - any dangling ref          → EffectiveRoles [] (visible to no one)
//   - otherwise                 → intersection of every ref's allowedRoles
//     (empty intersection also fails closed)
//
// and its private scope (ADR 0060), independently of roles:
//
//   - the skill's own AllowedPrincipals, if any, start the scope
//   - each referenced tool/agent that is itself private narrows it to the
//     intersection; public refs leave it alone
//   - no private input anywhere → public on this axis
//
// So a skill can narrow its tools' audience but never widen a private tool's,
// and a disjoint intersection fails closed (private, no one listed).
func DeriveSkillAccess(skill SkillDescriptor, tools map[string]ToolDescriptor, agents map[string]AgentDescriptor) SkillDescriptor {
	skill.Unrestricted = false
	skill.EffectiveRoles = nil
	skill.EffectivePrivate, skill.EffectivePrincipals = access.Narrow(false, nil,
		len(skill.AllowedPrincipals) > 0, skill.AllowedPrincipals)

	if len(skill.ToolIDs) == 0 && len(skill.AgentIDs) == 0 {
		skill.Unrestricted = true
		return skill
	}

	roleSets := make([][]string, 0, len(skill.ToolIDs)+len(skill.AgentIDs))
	narrow := func(aud access.Audience) {
		roleSets = append(roleSets, aud.Roles)
		skill.EffectivePrivate, skill.EffectivePrincipals = access.Narrow(
			skill.EffectivePrivate, skill.EffectivePrincipals, aud.Private, aud.Principals)
	}
	for _, id := range skill.ToolIDs {
		tool, ok := tools[id]
		if !ok {
			skill.EffectiveRoles = []string{}
			return skill
		}
		narrow(tool.Audience())
	}
	for _, id := range skill.AgentIDs {
		agent, ok := agents[id]
		if !ok {
			skill.EffectiveRoles = []string{}
			return skill
		}
		narrow(agent.Audience())
	}

	skill.EffectiveRoles = intersect(roleSets)
	return skill
}

func intersect(sets [][]string) []string {
	counts := map[string]int{}
	for _, set := range sets {
		seen := map[string]bool{}
		for _, role := range set {
			if !seen[role] {
				seen[role] = true
				counts[role]++
			}
		}
	}
	out := []string{}
	for _, role := range sets[0] {
		if counts[role] == len(sets) {
			counts[role] = 0 // dedupe repeats in sets[0]
			out = append(out, role)
		}
	}
	return out
}
