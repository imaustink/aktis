package facade

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
	"github.com/aktisai/orchestrator/engines/temporal/internal/rbac"
)

// Factory coordination (docs/adr/0055) as the facade sees it: a trigger's
// target.outcome reaches the engine only inside the signed dispatch, the 202
// echoes onComplete's holds for integration-gateway, and a conflict-kind
// event is an event like any other.

func coordinateTrigger() *catalog.EventTriggerDescriptor {
	spec := reviewTrigger().Spec
	spec.Target.Outcome = catalog.OutcomeCoordination
	return trigger("factory-coordinate", spec)
}

// Which outcome the run reports is a trigger decision, so it rides only
// inside the signed dispatch.
func TestEngineSignsATriggersOutcomeIntoTheDispatchNeverTheBody(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	e := g.engine()
	e.SenderAssertionSecret = engineAssertionSecret
	_, err := e.Invoke(context.Background(), TurnInput{
		Request: "coordinate", SessionID: "s", ForcedAgentID: "swe", TriggerName: "t", ReadOnly: true, Outcome: "coordination",
	})
	require.NoError(t, err)
	post := g.posts()[0]
	require.NotContains(t, post.Body, "outcome")
	require.Equal(t, "coordination", payloadOf(t, post.Header.Get(rbac.DispatchAssertionHeader))["outcome"])
}

func TestATriggersOutcomeIsForwardedOnTheTurnAndOnlyOnTriggerTurns(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Sessions: memSessions(), Triggers: triggerRegistry(t, coordinateTrigger())})

	postInvoke(t, base, map[string]any{"event": reviewEvent()}, nil)
	first := engine.waitInvokes(t, 1)[0]
	require.Equal(t, "claude-code-swe-agent", first.ForcedAgentID)
	require.True(t, first.ReadOnly)
	require.Equal(t, "coordination", first.Outcome)

	// A follow-up in the same session inherits read-only, not the outcome.
	postInvoke(t, base, map[string]any{"request": "and then?", "session_id": first.SessionID}, nil)
	second := engine.waitInvokes(t, 2)[1]
	require.True(t, second.ReadOnly)
	require.Empty(t, second.Outcome)
}

func TestRunEvent202EchoesOnCompleteHolds(t *testing.T) {
	spec := factoryReviewSpec()
	spec.OnComplete = &catalog.EventTriggerOnComplete{
		EmitTo:          "runs",
		WaitUntilClosed: &catalog.OnCompleteWaitUntilClosed{RepositoryParam: "repository", NumberParam: "number"},
		Serialize:       &catalog.OnCompleteSerialize{Group: "coord", KeyParam: "repository"},
	}
	base := serve(t, &Server{Engine: newFakeEngine(), Triggers: triggerRegistry(t, trigger("factory-review", spec))})
	trig := decode[map[string]any](t, postEvent(t, base, runEnvelope(nil), nil))["trigger"].(map[string]any)
	require.Equal(t, map[string]any{
		"emitTo":          "runs",
		"waitUntilClosed": map[string]any{"repositoryParam": "repository", "numberParam": "number"},
		"serialize":       map[string]any{"group": "coord", "keyParam": "repository"},
	}, trig["onComplete"])
}

func TestAConflictKindEventIsAcceptedAndMatched(t *testing.T) {
	resync := trigger("factory-resync", catalog.EventTriggerSpec{
		SourceRef: "pr-conflicts",
		Match:     catalog.EventTriggerMatch{CEL: `kind == "conflict" && body.pullRequest.number == 12`},
		Target:    catalog.EventTriggerTarget{None: &struct{}{}},
	})
	base := serve(t, &Server{Engine: newFakeEngine(), Triggers: triggerRegistry(t, resync)})
	res := postEvent(t, base, envelope(map[string]any{
		"source": "pr-conflicts", "kind": "conflict", "headers": map[string]any{},
		"body": map[string]any{"pullRequest": map[string]any{"number": 12}},
	}), nil)
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	require.Equal(t, "factory-resync", decode[map[string]any](t, res)["trigger"].(map[string]any)["name"])

	// An unknown kind is still refused.
	res = postEvent(t, base, envelope(map[string]any{"kind": "merge"}), nil)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
}
