package activities_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
	"github.com/aktisai/orchestrator/engines/temporal/internal/temporal/activities"
	"github.com/aktisai/orchestrator/engines/temporal/internal/vectorstore"
)

// ADR 0060: user-scoped private resources via allowedPrincipals.

const (
	aliceSubject = "openwebui:alice"
	bobSubject   = "openwebui:bob"
)

func TestCallerViewerPrincipals(t *testing.T) {
	t.Run("a per-user subject is a principal", func(t *testing.T) {
		v := activities.Caller{Subject: aliceSubject, Roles: []string{"eng"}, PerUser: true}.Viewer()
		require.Equal(t, []string{aliceSubject}, v.Principals)
		require.Equal(t, []string{"eng"}, v.Roles)
	})

	t.Run("a shared subject is never a principal", func(t *testing.T) {
		// The gateway's own service subject must not match a resource that
		// happens to list it: everyone behind that gateway would see it.
		v := activities.Caller{Subject: "svc:integration-gateway", Roles: []string{"agent"}}.Viewer()
		require.Empty(t, v.Principals)
	})

	t.Run("a verified linked sender is a principal on a shared-subject turn", func(t *testing.T) {
		v := activities.Caller{Subject: "svc:integration-gateway", SenderSubject: aliceSubject}.Viewer()
		require.Equal(t, []string{aliceSubject}, v.Principals)
	})

	t.Run("the same subject twice is listed once", func(t *testing.T) {
		v := activities.Caller{Subject: aliceSubject, PerUser: true, SenderSubject: aliceSubject}.Viewer()
		require.Equal(t, []string{aliceSubject}, v.Principals)
	})
}

func privateRec(id string, roles, principals []string, descriptor any) vectorstore.Record {
	r := rec(id, roles, false, descriptor)
	r.Private, r.Principals = true, principals
	return r
}

func privateActivities() *activities.RetrievalActivities {
	return &activities.RetrievalActivities{Collections: vectorstore.Collections{
		Skills: &fakeStore{records: map[string]vectorstore.Record{
			"journal": privateRec("journal", []string{"eng"}, []string{aliceSubject}, map[string]any{
				"id": "journal", "toolIds": []string{"diary", "shared"},
			}),
			"team": rec("team", []string{"eng"}, false, map[string]any{
				"id": "team", "toolIds": []string{"diary", "shared"},
			}),
		}},
		Tools: &fakeStore{records: map[string]vectorstore.Record{
			"diary":  privateRec("diary", []string{"eng"}, []string{aliceSubject}, map[string]any{"id": "diary"}),
			"shared": rec("shared", []string{"eng"}, false, map[string]any{"id": "shared"}),
		}},
		Agents: &fakeStore{records: map[string]vectorstore.Record{
			"aide": privateRec("aide", []string{"eng"}, []string{aliceSubject}, map[string]any{"id": "aide"}),
		}},
	}}
}

func TestPrivateResourcesReachOnlyTheirPrincipals(t *testing.T) {
	ctx := context.Background()
	acts := privateActivities()
	alice := activities.Caller{Subject: aliceSubject, Roles: []string{"eng"}, PerUser: true}
	bob := activities.Caller{Subject: bobSubject, Roles: []string{"eng"}, PerUser: true}
	shared := activities.Caller{Subject: aliceSubject, Roles: []string{"eng"}} // same string, not PerUser

	skillIDs := func(c activities.Caller) []string {
		skills, err := acts.RetrieveSkills(ctx, activities.RetrieveInput{Caller: c, Request: "r", TopK: 10})
		require.NoError(t, err)
		ids := make([]string, len(skills))
		for i, s := range skills {
			ids[i] = s.ID
		}
		return ids
	}

	t.Run("retrieval", func(t *testing.T) {
		require.ElementsMatch(t, []string{"journal", "team"}, skillIDs(alice))
		require.Equal(t, []string{"team"}, skillIDs(bob))
		require.Equal(t, []string{"team"}, skillIDs(shared),
			"a subject that is not asserted per-user must not match, even when it is the same string")
	})

	t.Run("a public skill resolves only the private tools the caller may see", func(t *testing.T) {
		result, err := acts.ResolveSkillTools(ctx, activities.ResolveSkillToolsInput{Caller: bob, SkillID: "team"})
		require.NoError(t, err)
		require.Len(t, result.Tools, 1)
		require.Equal(t, "shared", result.Tools[0].ID)

		result, err = acts.ResolveSkillTools(ctx, activities.ResolveSkillToolsInput{Caller: alice, SkillID: "team"})
		require.NoError(t, err)
		require.Len(t, result.Tools, 2)
	})

	t.Run("a forced or re-checked private skill is refused by id", func(t *testing.T) {
		result, err := acts.ResolveSkillTools(ctx, activities.ResolveSkillToolsInput{Caller: bob, SkillID: "journal"})
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("a private agent and tool are refused by id", func(t *testing.T) {
		agent, err := acts.ResolveAgent(ctx, activities.ResolveAgentInput{Caller: bob, AgentID: "aide"})
		require.NoError(t, err)
		require.Nil(t, agent)
		agent, err = acts.ResolveAgent(ctx, activities.ResolveAgentInput{Caller: alice, AgentID: "aide"})
		require.NoError(t, err)
		require.NotNil(t, agent)
	})

	t.Run("a linked webhook sender reaches their own private skill", func(t *testing.T) {
		sender := activities.Caller{Subject: "svc:integration-gateway", Roles: []string{"eng"}, SenderSubject: aliceSubject}
		require.Contains(t, skillIDs(sender), "journal")
	})
}

func privateMember(id string, roles, principals []string) catalog.KnowledgeBaseExecMember {
	m := member(id, roles, "coll-"+id)
	m.AllowedPrincipals = principals
	return m
}

func TestSearchWithholdsAPrivateMemberFromOthers(t *testing.T) {
	resolver := &fakeResolver{token: "t"}
	out, err := activitiesWith(resolver).SearchKnowledgeBase(context.Background(),
		activities.SearchKnowledgeBaseInput{
			Caller: activities.Caller{Subject: bobSubject, Roles: []string{"reader"}, PerUser: true},
			Tool:   searchTool(privateMember("alices", []string{"reader"}, []string{aliceSubject})),
			Query:  "q",
		})

	require.NoError(t, err)
	require.Contains(t, out.Result, "outside your access")
	require.Zero(t, resolver.callCount, "nothing visible, so nothing searched and no credential requested")
}

func TestReadCorpusRefusesAPrivateMemberToOthers(t *testing.T) {
	tool := catalog.ToolDescriptor{
		ID: "kb:globex/read",
		KnowledgeBaseExec: &catalog.KnowledgeBaseExecSpec{
			KnowledgeBaseID: "globex", DisplayName: "GLOBEX", Operation: "read",
			Members: []catalog.KnowledgeBaseExecMember{
				privateMember("alices", []string{"reader"}, []string{aliceSubject}),
			},
		},
	}
	resolver := &fakeResolver{token: "t"}
	out, err := activitiesWith(resolver).ReadCorpus(context.Background(), activities.ReadCorpusInput{
		Caller:   activities.Caller{Subject: bobSubject, Roles: []string{"reader"}, PerUser: true},
		Tool:     tool,
		SourceID: "alices/123",
	})

	require.NoError(t, err)
	require.Contains(t, out.Result, "do not have access")
	require.Zero(t, resolver.callCount)
}
