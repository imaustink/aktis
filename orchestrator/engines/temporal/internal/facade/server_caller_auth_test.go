package facade

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
)

// Ported from server.caller-auth.test.ts: nothing about a turn runs until
// whoever started it is authenticated (docs/adr/0053 follow-up 1).

const (
	gatewayToken = "gw-token"
	goodJWT      = "good-user-jwt"
)

// mapResolver resolves exactly the listed tokens and counts its calls.
type mapResolver struct {
	tokens map[string]string
	mu     sync.Mutex
	calls  []string
}

func (r *mapResolver) callCount() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.calls) }

func (r *mapResolver) Resolve(_ context.Context, token string) *Identity {
	r.mu.Lock()
	r.calls = append(r.calls, token)
	r.mu.Unlock()
	subject, ok := r.tokens[token]
	if !ok {
		return nil
	}
	return &Identity{Subject: subject, Roles: []string{"reader"}}
}

func authenticator(mode string) *CallerAuthenticator {
	return &CallerAuthenticator{
		Bearer:        &mapResolver{tokens: map[string]string{gatewayToken: "client-integration-gateway"}},
		ForwardedUser: &mapResolver{tokens: map[string]string{goodJWT: "openwebui:alice"}},
		Mode:          mode,
	}
}

func authServer(t *testing.T, engine Engine, auth *CallerAuthenticator, extra func(*Server)) string {
	s := &Server{Engine: engine, CallerAuth: auth}
	if extra != nil {
		extra(s)
	}
	return serve(t, s)
}

func bearer(token string) map[string]string {
	return map[string]string{"authorization": "Bearer " + token}
}

func TestInvokeCallerAuthRefusesWith401BeforeTheEngineRuns(t *testing.T) {
	for name, headers := range map[string]map[string]string{
		"no Authorization header": {},
		"an unknown bearer token": bearer("not-a-known-token"),
		"a non-bearer scheme":     {"authorization": "Basic " + gatewayToken},
	} {
		t.Run(name, func(t *testing.T) {
			engine := newFakeEngine()
			base := authServer(t, engine, authenticator("enforce"), nil)
			res := do(t, http.MethodPost, base+"/invoke", map[string]any{"request": "do something"}, headers)
			require.Equal(t, http.StatusUnauthorized, res.StatusCode)
			require.Regexp(t, `^Bearer`, res.Header.Get("www-authenticate"))
			require.Equal(t, map[string]any{"error": "unauthorized: could not authenticate the caller"}, decode[map[string]any](t, res))
			require.Empty(t, engine.checked())
			stillFalse(t, func() bool { return len(engine.invoked()) > 0 })
		})
	}
}

func TestInvokeCallerAuthAcceptsAResolvedBearer(t *testing.T) {
	engine := newFakeEngine()
	base := authServer(t, engine, authenticator("enforce"), nil)
	res := do(t, http.MethodPost, base+"/invoke", map[string]any{"request": "do something"}, bearer(gatewayToken))
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	engine.waitInvokes(t, 1)
}

// The orchestrator signs a matched trigger's dispatch, so an envelope from an
// unauthenticated caller must never reach selection. The trigger's params
// cannot resolve, so selection running at all would answer 422, not 401.
func TestInvokeCallerAuthRefusesAnEnvelopeBeforeAnyTriggerIsMatched(t *testing.T) {
	broken := trigger("needs-a-pr", catalog.EventTriggerSpec{
		Params: map[string]string{"number": "body.pull_request.number"},
		Target: catalog.EventTriggerTarget{Orchestrator: &struct{}{}, PromptTemplate: "PR {{number}}"},
	})
	engine := newFakeEngine()
	base := authServer(t, engine, authenticator("enforce"), func(s *Server) { s.Triggers = triggerRegistry(t, broken) })
	event := envelope(nil)

	res := do(t, http.MethodPost, base+"/invoke", map[string]any{"event": event}, bearer("forged"))
	require.Equal(t, http.StatusUnauthorized, res.StatusCode)

	// The control: authenticated, the same envelope does reach selection.
	ok := do(t, http.MethodPost, base+"/invoke", map[string]any{"event": event}, bearer(gatewayToken))
	require.Equal(t, http.StatusUnprocessableEntity, ok.StatusCode)
	require.Empty(t, engine.invoked())
}

func TestInvokeCallerAuthDoesNotTakeAForwardedUserJWT(t *testing.T) {
	base := authServer(t, newFakeEngine(), authenticator("enforce"), nil)
	res := do(t, http.MethodPost, base+"/invoke", map[string]any{"request": "x"}, map[string]string{forwardedUserJWTHeader: goodJWT})
	require.Equal(t, http.StatusUnauthorized, res.StatusCode)
}

func TestInvokeCallerAuthWarnModeLogsAndLetsTheTurnRun(t *testing.T) {
	logs := captureLog(t)
	engine := newFakeEngine()
	base := authServer(t, engine, authenticator("warn"), nil)
	res := do(t, http.MethodPost, base+"/invoke", map[string]any{"request": "x"}, bearer("not-a-known-token"))
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	engine.waitInvokes(t, 1)
	require.Contains(t, logs(), "would reject POST /invoke: bearer token not recognized")
	// The credential itself is never logged.
	require.NotContains(t, logs(), "not-a-known-token")
}

// TS: "fails closed when a resolver throws". A Go resolver cannot throw: it
// fails closed by returning nil (an unreachable JWKS is a failed verify).
func TestInvokeCallerAuthFailsClosedWhenTheResolverCannotVerify(t *testing.T) {
	down := &CallerAuthenticator{Bearer: resolverFunc(func(string) *Identity { return nil }), Mode: "enforce"}
	engine := newFakeEngine()
	base := authServer(t, engine, down, nil)
	res := do(t, http.MethodPost, base+"/invoke", map[string]any{"request": "x"}, bearer(gatewayToken))
	require.Equal(t, http.StatusUnauthorized, res.StatusCode)
	stillFalse(t, func() bool { return len(engine.invoked()) > 0 })
}

type resolverFunc func(token string) *Identity

func (f resolverFunc) Resolve(_ context.Context, token string) *Identity { return f(token) }

func TestChatCallerAuthRefusesAnUnknownBearerInOpenAIShape(t *testing.T) {
	engine := newFakeEngine()
	base := authServer(t, engine, authenticator("enforce"), nil)
	res := do(t, http.MethodPost, base+"/v1/chat/completions", userMessages("hello"), bearer("not-a-known-token"))
	require.Equal(t, http.StatusUnauthorized, res.StatusCode)
	require.Equal(t, map[string]any{"error": map[string]any{
		"message": "unauthorized: could not authenticate the caller", "type": "invalid_request_error", "code": "invalid_api_key",
	}}, decode[map[string]any](t, res))
	require.Empty(t, engine.invoked())
}

func TestChatCallerAuthAcceptsAVerifiedUserJWTWithoutABearer(t *testing.T) {
	engine := newFakeEngine()
	base := authServer(t, engine, authenticator("enforce"), nil)
	res := do(t, http.MethodPost, base+"/v1/chat/completions", userMessages("hello"), map[string]string{forwardedUserJWTHeader: goodJWT})
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Len(t, engine.invoked(), 1)
}

func TestChatCallerAuthRefusesAForgedUserJWTEvenWithAValidBearer(t *testing.T) {
	engine := newFakeEngine()
	base := authServer(t, engine, authenticator("enforce"), nil)
	res := do(t, http.MethodPost, base+"/v1/chat/completions", userMessages("hello"),
		map[string]string{"authorization": "Bearer " + gatewayToken, forwardedUserJWTHeader: "forged"})
	require.Equal(t, http.StatusUnauthorized, res.StatusCode)
	require.Empty(t, engine.invoked())
}

func TestChatCallerAuthGatesHousekeepingCompletionsToo(t *testing.T) {
	completer := &stubTaskCompleter{reply: "A title"}
	base := authServer(t, newFakeEngine(), authenticator("enforce"), func(s *Server) { s.TaskCompleter = completer })
	res := do(t, http.MethodPost, base+"/v1/chat/completions", userMessages("### Task:\nGenerate a concise title"), bearer("nope"))
	require.Equal(t, http.StatusUnauthorized, res.StatusCode)
	require.Zero(t, completer.count())
}
