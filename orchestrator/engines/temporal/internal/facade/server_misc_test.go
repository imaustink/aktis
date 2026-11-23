package facade

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
	"github.com/aktisai/orchestrator/engines/temporal/internal/rbac"
)

// Ported from server.test.ts: live-session routes, signed sender assertion,
// accept-phase identity check and trace context.

// ── /sessions/live after docs/adr/0053 ─────────────────────────────────────

func TestSessionsLiveAnswersAConfirmedNotLive(t *testing.T) {
	base := serve(t, &Server{Engine: newFakeEngine()})
	res := do(t, http.MethodGet, base+"/sessions/live?sessionId="+url.QueryEscape("github:acme/widgets#7"), nil, withAuth(nil))
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, map[string]any{"live": false}, decode[map[string]any](t, res))
}

func TestSessionsLive401sWithoutABearerToken(t *testing.T) {
	base := serve(t, &Server{Engine: newFakeEngine()})
	require.Equal(t, http.StatusUnauthorized, do(t, http.MethodGet, base+"/sessions/live?sessionId=s1", nil, nil).StatusCode)
	require.Equal(t, http.StatusUnauthorized, do(t, http.MethodGet, base+"/sessions/live?sessionId=s1", nil, map[string]string{"authorization": "Basic x"}).StatusCode)
}

func TestSessionsLive400sWithoutASessionID(t *testing.T) {
	base := serve(t, &Server{Engine: newFakeEngine()})
	require.Equal(t, http.StatusBadRequest, do(t, http.MethodGet, base+"/sessions/live", nil, withAuth(nil)).StatusCode)
}

func TestTheAgentRunTunnelRoutesAreGone(t *testing.T) {
	base := serve(t, &Server{Engine: newFakeEngine()})
	require.Equal(t, http.StatusNotFound, do(t, http.MethodGet, base+"/agent-runs/run-1/events?sessionId=s1", nil, withAuth(nil)).StatusCode)
	require.Equal(t, http.StatusNotFound, do(t, http.MethodPost, base+"/agent-runs/run-1/opencode?sessionId=s1",
		map[string]any{"method": "GET", "path": "/global/health"}, withAuth(nil)).StatusCode)
}

func TestHealthz(t *testing.T) {
	base := serve(t, &Server{Engine: newFakeEngine()})
	require.Equal(t, http.StatusOK, do(t, http.MethodGet, base+"/healthz", nil, nil).StatusCode)
}

// ── signed sender assertion (docs/adr/0030 §6) ─────────────────────────────

const assertionSecret = "assertion-secret"

func senderServer(t *testing.T, engine Engine, secret string) string {
	return serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, catchAllTrigger()), SenderAssertionSecret: secret})
}

func TestSenderLoginFromTheEnvelopeWithoutASecret(t *testing.T) {
	engine := newFakeEngine()
	base := senderServer(t, engine, "")
	postInvoke(t, base, map[string]any{"event": envelope(map[string]any{"sender": map[string]any{"login": "imaustink", "isBot": false}})}, nil)
	in := engine.waitInvokes(t, 1)[0]
	require.Equal(t, "imaustink", in.SenderLogin)
	require.Nil(t, in.SenderClaims)
}

func TestSenderLoginOnlyFromAVerifiedAssertionWithASecret(t *testing.T) {
	engine := newFakeEngine()
	base := senderServer(t, engine, assertionSecret)
	// A DIFFERENT login in the body must be ignored outright.
	postInvoke(t, base, map[string]any{"event": envelope(map[string]any{"sender": map[string]any{"login": "victim"}})},
		map[string]string{rbac.SenderAssertionHeader: rbac.MintSenderAssertion(assertionSecret, "imaustink", time.Minute, time.Now())})
	in := engine.waitInvokes(t, 1)[0]
	require.Equal(t, "imaustink", in.SenderLogin)
	require.Equal(t, &rbac.SenderClaims{Login: "imaustink"}, in.SenderClaims)
}

func TestSenderLoginUnsignedIsDroppedWithASecret(t *testing.T) {
	for name, header := range map[string]string{
		"no assertion": "",
		"wrong secret": rbac.MintSenderAssertion("other-secret", "imaustink", time.Minute, time.Now()),
		"expired":      rbac.MintSenderAssertion(assertionSecret, "imaustink", time.Minute, time.Now().Add(-time.Hour)),
	} {
		t.Run(name, func(t *testing.T) {
			engine := newFakeEngine()
			base := senderServer(t, engine, assertionSecret)
			headers := map[string]string{}
			if header != "" {
				headers[rbac.SenderAssertionHeader] = header
			}
			postInvoke(t, base, map[string]any{"event": envelope(map[string]any{"sender": map[string]any{"login": "victim"}})}, headers)
			// No principal rather than the WRONG principal.
			require.Empty(t, engine.waitInvokes(t, 1)[0].SenderLogin)
		})
	}
}

// ── accept-phase identity check (docs/adr/0049 §6) ─────────────────────────

func agentTrigger() *catalog.EventTriggerDescriptor {
	return trigger("triage-as-sender", catalog.EventTriggerSpec{
		Target: catalog.EventTriggerTarget{AgentRef: "swe", PromptTemplate: "triage", Identity: "delegated"},
		Reply:  &catalog.EventTriggerReply{GitHub: &catalog.EventGitHubReply{Repository: "acme/widgets", IssueNumber: "7", OnIdentityError: true}},
	})
}

var aliceClaims = rbac.SenderClaims{Login: "alice", AccountID: "583231", SubjectStatus: rbac.SenderSubjectUnlinked}

func postAsAlice(t *testing.T, engine Engine, d *catalog.EventTriggerDescriptor) *http.Response {
	base := serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, d), SenderAssertionSecret: assertionSecret})
	return postInvoke(t, base, map[string]any{"event": envelope(map[string]any{"sender": map[string]any{"login": "alice"}})},
		map[string]string{rbac.SenderAssertionHeader: rbac.MintSenderClaims(assertionSecret, aliceClaims, time.Minute, time.Now())})
}

func identityRefusal() *IdentityRefusal {
	errObj := map[string]any{"code": "identity_link_required", "message": "alice: link your GitHub account so swe can act as you: https://gw/x"}
	return &IdentityRefusal{
		Body:  map[string]any{"status": "failed", "error": errObj, "hint": "kept"},
		Error: errObj,
	}
}

func TestIdentityCheck403sWithTheEnginesRefusalAndTheTriggerReply(t *testing.T) {
	engine := newFakeEngine()
	engine.checkFn = func(TurnInput) *IdentityRefusal { return identityRefusal() }
	res := postAsAlice(t, engine, agentTrigger())

	require.Equal(t, http.StatusForbidden, res.StatusCode)
	body := decode[map[string]any](t, res)
	// The engine's whole body survives, top-level fields included.
	require.Equal(t, "failed", body["status"])
	require.Equal(t, "kept", body["hint"])
	errBody := body["error"].(map[string]any)
	require.Equal(t, "identity_link_required", errBody["code"])
	require.Equal(t, "alice: link your GitHub account so swe can act as you: https://gw/x", errBody["message"])
	trig := errBody["trigger"].(map[string]any)
	require.Equal(t, "triage-as-sender", trig["name"])
	require.Equal(t, true, trig["reply"].(map[string]any)["github"].(map[string]any)["onIdentityError"])
	stillFalse(t, func() bool { return len(engine.invoked()) > 0 })

	// The check saw the turn as the engine would.
	checks := engine.checked()
	require.Len(t, checks, 1)
	require.Equal(t, "swe", checks[0].ForcedAgentID)
	require.Equal(t, "delegated", checks[0].IdentityOverride)
	require.Equal(t, "alice", checks[0].SenderLogin)
	require.Equal(t, &aliceClaims, checks[0].SenderClaims)
	require.Equal(t, "triage", checks[0].Request)
	require.Equal(t, "triage-as-sender", checks[0].TriggerName)
	require.Regexp(t, anyTraceparent, checks[0].Traceparent)
}

func TestIdentityCheckNoObjectionAcceptsAndCarriesTheClaims(t *testing.T) {
	engine := newFakeEngine()
	res := postAsAlice(t, engine, agentTrigger())
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	require.Equal(t, &aliceClaims, engine.waitInvokes(t, 1)[0].SenderClaims)
}

func TestIdentityCheckSkipsATriggerThatNamesNoAgent(t *testing.T) {
	engine := newFakeEngine()
	engine.checkFn = func(TurnInput) *IdentityRefusal { return identityRefusal() }
	res := postAsAlice(t, engine, catchAllTrigger())
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	require.Empty(t, engine.checked())
}

// ── trace context (docs/adr/0050 §4) ───────────────────────────────────────

func TestInvokeContinuesAValidInboundTraceparentOrRootsANewTrace(t *testing.T) {
	const inbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	for _, header := range []string{inbound, "not-a-traceparent"} {
		engine := newFakeEngine()
		base := serve(t, &Server{Engine: engine})
		postInvoke(t, base, map[string]any{"request": "hi"}, map[string]string{"traceparent": header})
		got := engine.waitInvokes(t, 1)[0].Traceparent
		require.Regexp(t, anyTraceparent, got)
		require.NotEqual(t, header, got, "this hop is a span of its own")
		if header == inbound {
			require.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", got[3:35])
		} else {
			require.NotEqual(t, "4bf92f3577b34da6a3ce929d0e0e4736", got[3:35])
		}
	}
}

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return rec
}

func endedSpan(t *testing.T, rec *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found sdktrace.ReadOnlySpan
	waitFor(t, func() bool {
		for _, s := range rec.Ended() {
			if s.Name() == name {
				found = s
				return true
			}
		}
		return false
	})
	return found
}

func attrs(s sdktrace.ReadOnlySpan) map[string]any {
	out := map[string]any{}
	for _, kv := range s.Attributes() {
		out[string(kv.Key)] = kv.Value.AsInterface()
	}
	return out
}

func TestInvokeWithExportOnHandsTheTurnAnExportedServerSpansContext(t *testing.T) {
	rec := recordSpans(t)
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine})
	id := acceptedID(t, postInvoke(t, base, map[string]any{"request": "hi"},
		map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}))

	span := endedSpan(t, rec, "POST /invoke")
	require.Equal(t, "00f067aa0ba902b7", span.Parent().SpanID().String())
	require.Equal(t, "00-4bf92f3577b34da6a3ce929d0e0e4736-"+span.SpanContext().SpanID().String()+"-01", engine.waitInvokes(t, 1)[0].Traceparent)
	a := attrs(span)
	require.Equal(t, int64(202), a["http.response.status_code"])
	require.Equal(t, id, a["aktis.invocation_id"])
}

// ── the record bookkeeping never reaches a caller ──────────────────────────

func TestInvocationRecordVisibleStripsBookkeeping(t *testing.T) {
	r := InvocationRecord{ID: "i", Status: "pending", EngineInvocationID: "engine-1", ForcedAgent: true}
	v := r.visible()
	require.Equal(t, InvocationRecord{ID: "i", Status: "pending"}, v)
	// visible() is a copy: the stored record keeps its bookkeeping.
	require.Equal(t, "engine-1", r.EngineInvocationID)
}
