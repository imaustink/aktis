package facade

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
)

// POST /events/prefilter (docs/event-triggers.md §8): integration-gateway's
// question about a webhook delivery before it resolves the sender.

func prelimEnvelope(body map[string]any) map[string]any {
	return map[string]any{
		"source": "github", "kind": "webhook", "id": "d1", "time": "2026-01-01T00:00:00Z",
		"headers": map[string]any{"x-github-event": "issues"}, "body": body, "sender": map[string]any{}, "repository": "",
	}
}

func labeledTrigger() *catalog.EventTriggerDescriptor {
	return trigger("review", catalog.EventTriggerSpec{
		SourceRef: "github",
		Match:     catalog.EventTriggerMatch{CEL: `body.action == "labeled"`},
		Target:    catalog.EventTriggerTarget{AgentRef: "swe", PromptTemplate: "go"},
	})
}

func prefilterServer(t *testing.T, triggers *catalog.EventTriggerRegistry) string {
	return serve(t, &Server{Engine: newFakeEngine(), Triggers: triggers, CallerAuth: authenticator("enforce")})
}

func postPrefilter(t *testing.T, base string, body any, headers map[string]string) *http.Response {
	return do(t, http.MethodPost, base+"/events/prefilter", body, headers)
}

var senderExprs = map[string]any{
	"loginFrom":      "body.sender.login",
	"isBotFrom":      `body.sender.type == "Bot"`,
	"repositoryFrom": "body.repository.full_name",
	"accountIdFrom":  "body.sender.id",
}

func TestPrefilterCouldMatchFalseDropsWithoutEvaluatingSender(t *testing.T) {
	base := prefilterServer(t, triggerRegistry(t, labeledTrigger()))
	res := postPrefilter(t, base, map[string]any{"envelope": prelimEnvelope(map[string]any{"action": "opened"}), "sender": senderExprs}, bearer(gatewayToken))
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, map[string]any{"couldMatch": false}, decode[map[string]any](t, res))
}

func TestPrefilterEvaluatesSenderExpressionsWhenATriggerCouldMatch(t *testing.T) {
	base := prefilterServer(t, triggerRegistry(t, labeledTrigger()))
	body := map[string]any{
		"action":     "labeled",
		"sender":     map[string]any{"login": "alice", "type": "User", "id": 583231},
		"repository": map[string]any{"full_name": "acme/widgets"},
	}
	res := postPrefilter(t, base, map[string]any{"envelope": prelimEnvelope(body), "sender": senderExprs}, bearer(gatewayToken))
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, map[string]any{
		"couldMatch": true,
		"sender": map[string]any{
			"loginFrom":      map[string]any{"ok": true, "type": "string", "value": "alice"},
			"isBotFrom":      map[string]any{"ok": true, "type": "bool", "value": false},
			"repositoryFrom": map[string]any{"ok": true, "type": "string", "value": "acme/widgets"},
			// An int travels as a decimal string, never a JSON number.
			"accountIdFrom": map[string]any{"ok": true, "type": "int", "value": "583231"},
		},
	}, decode[map[string]any](t, res))
}

func TestPrefilterReportsCompileAndEvaluationErrorsAsNotOK(t *testing.T) {
	base := prefilterServer(t, triggerRegistry(t, labeledTrigger()))
	res := postPrefilter(t, base, map[string]any{
		"envelope": prelimEnvelope(map[string]any{"action": "labeled"}),
		"sender":   map[string]any{"loginFrom": "body.(", "accountIdFrom": "body.sender.id"},
	}, bearer(gatewayToken))
	require.Equal(t, http.StatusOK, res.StatusCode)
	sender := decode[map[string]any](t, res)["sender"].(map[string]any)
	// Only the requested fields: isBotFrom/repositoryFrom were not asked for.
	require.Len(t, sender, 2)
	for _, field := range []string{"loginFrom", "accountIdFrom"} {
		result := sender[field].(map[string]any)
		require.Equal(t, false, result["ok"], field)
		require.NotEmpty(t, result["error"], field)
		require.NotContains(t, result, "value", field)
	}
}

func TestPrefilterWithoutATriggerTableCannotMatch(t *testing.T) {
	base := prefilterServer(t, nil)
	res := postPrefilter(t, base, map[string]any{"envelope": prelimEnvelope(map[string]any{"action": "labeled"})}, bearer(gatewayToken))
	require.Equal(t, map[string]any{"couldMatch": false}, decode[map[string]any](t, res))
}

func TestPrefilterRejectsABadRequestWith400(t *testing.T) {
	base := prefilterServer(t, triggerRegistry(t, labeledTrigger()))
	noKind := prelimEnvelope(map[string]any{})
	delete(noKind, "kind")
	for name, body := range map[string]any{
		"not JSON":            "{nope",
		"no envelope":         map[string]any{"sender": senderExprs},
		"malformed envelope":  map[string]any{"envelope": noKind},
		"envelope not object": map[string]any{"envelope": "github"},
	} {
		t.Run(name, func(t *testing.T) {
			res := postPrefilter(t, base, body, bearer(gatewayToken))
			require.Equal(t, http.StatusBadRequest, res.StatusCode)
			require.NotEmpty(t, decode[map[string]any](t, res)["error"])
		})
	}
	res := postPrefilter(t, base, map[string]any{"envelope": noKind}, bearer(gatewayToken))
	require.Contains(t, decode[map[string]any](t, res)["error"], "event.kind")
}

func TestPrefilterRequiresAuthentication(t *testing.T) {
	base := prefilterServer(t, triggerRegistry(t, labeledTrigger()))
	body := map[string]any{"envelope": prelimEnvelope(map[string]any{"action": "labeled"})}
	for _, headers := range []map[string]string{nil, bearer("forged"), {forwardedUserJWTHeader: goodJWT}} {
		res := postPrefilter(t, base, body, headers)
		require.Equal(t, http.StatusUnauthorized, res.StatusCode)
		require.Equal(t, map[string]any{"error": "unauthorized: could not authenticate the caller"}, decode[map[string]any](t, res))
	}
	require.Equal(t, http.StatusOK, postPrefilter(t, base, body, bearer(gatewayToken)).StatusCode)
}
