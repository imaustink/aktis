package catalog_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/aktisai/orchestrator/engines/temporal/internal/access"
	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
	"github.com/aktisai/orchestrator/engines/temporal/internal/vectorstore"
)

// ADR 0060: user-scoped private resources via allowedPrincipals.

const (
	alice = "openwebui:alice"
	bob   = "openwebui:bob"
)

func cr(kind, name string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "core.aktis.ai/v1alpha1",
		"kind":       kind,
		"metadata":   map[string]any{"name": name},
		"spec":       spec,
	}}
}

func TestDecodeAllowedPrincipals(t *testing.T) {
	private := map[string]any{
		"description":       "d",
		"allowedRoles":      []any{"eng"},
		"allowedPrincipals": []any{alice},
	}

	tool, err := catalog.DecodeTool(cr("Tool", "t", private))
	require.NoError(t, err)
	require.Equal(t, []string{alice}, tool.AllowedPrincipals)

	local, err := catalog.DecodeLocalTool(cr("LocalTool", "l", map[string]any{
		"description": "d", "allowedRoles": []any{"eng"}, "allowedPrincipals": []any{alice},
		"runtime": "node", "package": "p", "version": "1",
	}))
	require.NoError(t, err)
	require.Equal(t, []string{alice}, local.AllowedPrincipals)

	mcp, err := catalog.DecodeMCPTool(cr("MCPTool", "m", map[string]any{
		"serverRef": "s", "remoteToolName": "r", "description": "d",
		"allowedRoles": []any{"eng"}, "allowedPrincipals": []any{alice},
	}))
	require.NoError(t, err)
	require.Equal(t, []string{alice}, mcp.AllowedPrincipals)

	agent, err := catalog.DecodeAgent(cr("Agent", "a", private))
	require.NoError(t, err)
	require.Equal(t, []string{alice}, agent.AllowedPrincipals)

	skill, err := catalog.DecodeSkill(cr("Skill", "s", map[string]any{
		"description": "d", "markdown": "m", "allowedPrincipals": []any{alice},
	}))
	require.NoError(t, err)
	require.Equal(t, []string{alice}, skill.AllowedPrincipals)
	require.False(t, skill.EffectivePrivate, "effective scope is derived at index time, not decoded")

	corpus, err := catalog.DecodeCorpus(cr("Corpus", "c", map[string]any{
		"connectionRef": "x", "description": "d",
		"allowedRoles": []any{"eng"}, "allowedPrincipals": []any{alice},
	}))
	require.NoError(t, err)
	require.Equal(t, []string{alice}, corpus.AllowedPrincipals)

	public, err := catalog.DecodeTool(cr("Tool", "p", map[string]any{"description": "d", "allowedRoles": []any{"eng"}}))
	require.NoError(t, err)
	require.Empty(t, public.AllowedPrincipals)
	require.False(t, public.Audience().Private, "unset means public")
}

func TestDeriveSkillAccessPrincipals(t *testing.T) {
	tools := map[string]catalog.ToolDescriptor{
		"public":    {ID: "public", AllowedRoles: []string{"eng"}},
		"alice-bob": {ID: "alice-bob", AllowedRoles: []string{"eng"}, AllowedPrincipals: []string{alice, bob}},
		"bob-only":  {ID: "bob-only", AllowedRoles: []string{"eng"}, AllowedPrincipals: []string{bob}},
	}
	agents := map[string]catalog.AgentDescriptor{
		"alice-agent": {ID: "alice-agent", AllowedRoles: []string{"eng"}, AllowedPrincipals: []string{alice}},
	}
	derive := func(s catalog.SkillDescriptor) catalog.SkillDescriptor {
		return catalog.DeriveSkillAccess(s, tools, agents)
	}

	t.Run("only public refs and no own list stays public", func(t *testing.T) {
		s := derive(catalog.SkillDescriptor{ID: "s", ToolIDs: []string{"public"}})
		require.False(t, s.EffectivePrivate)
		require.Equal(t, []string{"eng"}, s.EffectiveRoles)
	})

	t.Run("a private tool makes the skill private to its principals", func(t *testing.T) {
		s := derive(catalog.SkillDescriptor{ID: "s", ToolIDs: []string{"public", "alice-bob"}})
		require.True(t, s.EffectivePrivate)
		require.ElementsMatch(t, []string{alice, bob}, s.EffectivePrincipals)
	})

	t.Run("several private refs intersect", func(t *testing.T) {
		s := derive(catalog.SkillDescriptor{ID: "s", ToolIDs: []string{"alice-bob"}, AgentIDs: []string{"alice-agent"}})
		require.True(t, s.EffectivePrivate)
		require.Equal(t, []string{alice}, s.EffectivePrincipals)
	})

	t.Run("the skill's own list narrows a private tool", func(t *testing.T) {
		s := derive(catalog.SkillDescriptor{ID: "s", ToolIDs: []string{"alice-bob"}, AllowedPrincipals: []string{bob}})
		require.Equal(t, []string{bob}, s.EffectivePrincipals)
	})

	t.Run("the skill's own list can never widen a private tool", func(t *testing.T) {
		s := derive(catalog.SkillDescriptor{ID: "s", ToolIDs: []string{"bob-only"}, AllowedPrincipals: []string{alice}})
		require.True(t, s.EffectivePrivate)
		require.Empty(t, s.EffectivePrincipals, "disjoint scopes fail closed: private, no one listed")
		require.False(t, (access.Viewer{Roles: []string{"eng"}, Principals: []string{alice}}).Admits(s.Audience()))
		require.False(t, (access.Viewer{Roles: []string{"eng"}, Principals: []string{bob}}).Admits(s.Audience()))
	})

	t.Run("a private respond-only skill (no refs) stays unrestricted on roles but private", func(t *testing.T) {
		s := derive(catalog.SkillDescriptor{ID: "s", AllowedPrincipals: []string{alice}})
		require.True(t, s.Unrestricted)
		require.True(t, s.EffectivePrivate)
		require.True(t, (access.Viewer{Principals: []string{alice}}).Admits(s.Audience()))
		require.False(t, (access.Viewer{Principals: []string{bob}}).Admits(s.Audience()))
	})

	t.Run("a dangling ref still fails closed", func(t *testing.T) {
		s := derive(catalog.SkillDescriptor{ID: "s", ToolIDs: []string{"alice-bob", "missing"}})
		require.Empty(t, s.EffectiveRoles)
	})
}

func privateCorpus(id string, roles, principals []string) catalog.CorpusDescriptor {
	return catalog.CorpusDescriptor{
		ID: id, Provider: "confluence", Description: id, Collection: "c-" + id,
		AllowedRoles: roles, AllowedPrincipals: principals,
		APIEnabled: true, IdentityProviders: []string{"atlassian"},
	}
}

func TestKnowledgeBasePrincipalsAreTheUnionOfItsMembers(t *testing.T) {
	kb := catalog.KnowledgeBaseDescriptor{ID: "kb", Description: "d", CorpusRefs: []string{"a", "b"}}

	t.Run("one public member keeps the knowledge base public", func(t *testing.T) {
		conns := map[string]catalog.CorpusDescriptor{
			"a": privateCorpus("a", []string{"eng"}, []string{alice}),
			"b": privateCorpus("b", []string{"eng"}, nil),
		}
		s := catalog.DeriveKnowledgeBaseSkill(kb, conns)
		require.False(t, s.EffectivePrivate)
	})

	t.Run("all-private members union their principals", func(t *testing.T) {
		conns := map[string]catalog.CorpusDescriptor{
			"a": privateCorpus("a", []string{"eng"}, []string{alice}),
			"b": privateCorpus("b", []string{"eng"}, []string{bob}),
		}
		s := catalog.DeriveKnowledgeBaseSkill(kb, conns)
		require.True(t, s.EffectivePrivate)
		require.Equal(t, []string{alice, bob}, s.EffectivePrincipals)
	})
}

func TestVisibleConnectionsWithholdsPrivateMembers(t *testing.T) {
	kb := catalog.KnowledgeBaseDescriptor{ID: "kb", CorpusRefs: []string{"shared", "mine"}}
	conns := map[string]catalog.CorpusDescriptor{
		"shared": privateCorpus("shared", []string{"eng"}, nil),
		"mine":   privateCorpus("mine", []string{"eng"}, []string{alice}),
	}

	visible, withheld := catalog.VisibleConnections(kb, conns, access.Viewer{Roles: []string{"eng"}, Principals: []string{alice}})
	require.Len(t, visible, 2)
	require.Zero(t, withheld)

	visible, withheld = catalog.VisibleConnections(kb, conns, access.Viewer{Roles: []string{"eng"}, Principals: []string{bob}})
	require.Len(t, visible, 1)
	require.Equal(t, "shared", visible[0].ID)
	require.Equal(t, 1, withheld, "a private member is withheld and counted, like a role-scoped one")
}

func TestIndexerWritesPrincipalScopeToTheStore(t *testing.T) {
	tools, skills, agents := newRecordingStore(), newRecordingStore(), newRecordingStore()
	ix := catalog.NewIndexer(vectorstore.Collections{Tools: tools, Skills: skills, Agents: agents})
	ctx := context.Background()

	require.NoError(t, ix.UpsertTool(ctx, catalog.ToolDescriptor{
		ID: "notes", AllowedRoles: []string{"eng"}, AllowedPrincipals: []string{alice},
	}))
	require.NoError(t, ix.UpsertAgent(ctx, catalog.AgentDescriptor{ID: "public-agent", AllowedRoles: []string{"eng"}}))
	require.NoError(t, ix.UpsertSkill(ctx, catalog.SkillDescriptor{ID: "notes-skill", ToolIDs: []string{"notes"}}))

	rec, ok := tools.get("notes")
	require.True(t, ok)
	require.True(t, rec.Private)
	require.Equal(t, []string{alice}, rec.Principals)

	rec, ok = agents.get("public-agent")
	require.True(t, ok)
	require.False(t, rec.Private)

	rec, ok = skills.get("notes-skill")
	require.True(t, ok)
	require.True(t, rec.Private, "a skill over a private tool is indexed private")
	require.Equal(t, []string{alice}, rec.Principals)

	// A knowledge base made only of private members writes private tools too.
	require.NoError(t, ix.UpsertCorpus(ctx, privateCorpus("mine", []string{"eng"}, []string{alice})))
	require.NoError(t, ix.UpsertKnowledgeBase(ctx, catalog.KnowledgeBaseDescriptor{
		ID: "personal", Description: "d", CorpusRefs: []string{"mine"},
	}))
	for _, id := range []string{
		catalog.KnowledgeBaseSearchToolID("personal"),
		catalog.KnowledgeBaseReadToolID("personal"),
		catalog.KnowledgeBaseQueryToolID("personal"),
	} {
		rec, ok := tools.get(id)
		require.True(t, ok, id)
		require.True(t, rec.Private, id)
		require.Equal(t, []string{alice}, rec.Principals, id)
	}
	rec, ok = skills.get(catalog.KnowledgeBaseSkillID("personal"))
	require.True(t, ok)
	require.True(t, rec.Private)
}

func TestSharedKnowledgeBaseTextNeverNamesAPrivateMemberToOutsiders(t *testing.T) {
	secret := privateCorpus("alice-notes", []string{"eng"}, []string{alice})
	secret.DisplayName = "Alice's 1:1 notes"
	secret.Description = "Alice's private one-on-one notes"
	shared := privateCorpus("team-wiki", []string{"eng"}, nil)
	shared.DisplayName = "Team wiki"
	shared.Description = "The team's shared wiki"

	leaked := func(t *testing.T, text string) {
		t.Helper()
		require.NotContains(t, text, "alice-notes")
		require.NotContains(t, text, secret.DisplayName)
		require.NotContains(t, text, secret.Description)
	}

	t.Run("a knowledge base with a public member counts the private one instead of naming it", func(t *testing.T) {
		tools := newRecordingStore()
		skills := newRecordingStore()
		ix := catalog.NewIndexer(vectorstore.Collections{Tools: tools, Skills: skills, Agents: newRecordingStore()})
		ctx := context.Background()
		require.NoError(t, ix.UpsertCorpus(ctx, secret))
		require.NoError(t, ix.UpsertCorpus(ctx, shared))
		require.NoError(t, ix.UpsertKnowledgeBase(ctx, catalog.KnowledgeBaseDescriptor{
			ID: "team", Description: "d", CorpusRefs: []string{"alice-notes", "team-wiki"},
		}))

		skill := catalog.DeriveKnowledgeBaseSkill(
			catalog.KnowledgeBaseDescriptor{ID: "team", Description: "d", CorpusRefs: []string{"alice-notes", "team-wiki"}},
			map[string]catalog.CorpusDescriptor{"alice-notes": secret, "team-wiki": shared},
		)
		require.False(t, skill.EffectivePrivate)
		leaked(t, skill.Markdown)
		require.Contains(t, skill.Markdown, shared.Description)
		require.Contains(t, skill.Markdown, "plus 1 private source")

		for _, id := range []string{catalog.KnowledgeBaseReadToolID("team"), catalog.KnowledgeBaseQueryToolID("team")} {
			rec, ok := tools.get(id)
			require.True(t, ok, id)
			tool := decodeTool(t, rec)
			leaked(t, tool.Description+tool.Input+tool.Output)
			require.Contains(t, tool.Input, "team-wiki (Team wiki)", id)
			require.Contains(t, tool.Input, "plus 1 private source", id)
		}
		rec, ok := skills.get(catalog.KnowledgeBaseSkillID("team"))
		require.True(t, ok)
		leaked(t, rec.Text)
	})

	t.Run("a private member is still named when everyone who can see the text is on its list", func(t *testing.T) {
		skill := catalog.DeriveKnowledgeBaseSkill(
			catalog.KnowledgeBaseDescriptor{ID: "mine", Description: "d", CorpusRefs: []string{"alice-notes"}},
			map[string]catalog.CorpusDescriptor{"alice-notes": secret},
		)
		require.Contains(t, skill.Markdown, secret.Description)
		require.NotContains(t, skill.Markdown, "private source")
	})

	t.Run("a private member is not named when the union also admits someone it excludes", func(t *testing.T) {
		bobs := privateCorpus("bob-notes", []string{"eng"}, []string{bob})
		skill := catalog.DeriveKnowledgeBaseSkill(
			catalog.KnowledgeBaseDescriptor{ID: "pair", Description: "d", CorpusRefs: []string{"alice-notes", "bob-notes"}},
			map[string]catalog.CorpusDescriptor{"alice-notes": secret, "bob-notes": bobs},
		)
		require.True(t, skill.EffectivePrivate)
		leaked(t, skill.Markdown)
		require.NotContains(t, skill.Markdown, "bob-notes")
		require.Contains(t, skill.Markdown, "plus 2 private sources")
	})
}
