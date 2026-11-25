package facade

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"

	"github.com/aktisai/orchestrator/engines/temporal/internal/callertools"
	"github.com/aktisai/orchestrator/engines/temporal/internal/rbac"
)

// Ported from engine/temporal-engine.test.ts, against an httptest stand-in
// for the engine gateway's accept-then-poll /invoke.

const (
	engineAssertionSecret = "sender-assertion-secret"
	jwtSecret             = "openwebui-forward-jwt-secret"
	acceptedEngineID      = "conversation-abc.upd-1"
)

type gatewayCall struct {
	Method string
	Path   string
	Header http.Header
	Body   map[string]any
}

// fakeGateway scripts the accept/poll pair: every POST /invoke is accepted
// with acceptedEngineID, and each GET answers the next record (the last one
// repeats).
type fakeGateway struct {
	t       *testing.T
	srv     *httptest.Server
	mu      sync.Mutex
	calls   []gatewayCall
	records []map[string]any
	polls   int
	// onPost, when set, answers POST /invoke instead.
	onPost func(w http.ResponseWriter)
	// onCheck answers POST /invoke/check.
	onCheck func(w http.ResponseWriter)
}

func newFakeGateway(t *testing.T, records ...map[string]any) *fakeGateway {
	g := &fakeGateway{t: t, records: records}
	g.srv = httptest.NewServer(http.HandlerFunc(g.serve))
	t.Cleanup(g.srv.Close)
	return g
}

func (g *fakeGateway) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &body)
	}
	g.mu.Lock()
	g.calls = append(g.calls, gatewayCall{Method: r.Method, Path: r.URL.Path, Header: r.Header.Clone(), Body: body})
	g.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/invoke":
		if g.onPost != nil {
			g.onPost(w)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"id": acceptedEngineID, "status": "pending"})
	case r.Method == http.MethodPost && r.URL.Path == "/invoke/check":
		if g.onCheck != nil {
			g.onCheck(w)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/invoke/"):
		g.mu.Lock()
		record := g.records[min(g.polls, len(g.records)-1)]
		g.polls++
		g.mu.Unlock()
		writeJSON(w, http.StatusOK, record)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (g *fakeGateway) all() []gatewayCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]gatewayCall(nil), g.calls...)
}

func (g *fakeGateway) posts() []gatewayCall {
	var out []gatewayCall
	for _, c := range g.all() {
		if c.Method == http.MethodPost && c.Path == "/invoke" {
			out = append(out, c)
		}
	}
	return out
}

func (g *fakeGateway) engine() *TemporalEngine {
	return &TemporalEngine{BaseURL: g.srv.URL + "/", PollInterval: time.Millisecond, AutoResumeEvery: time.Millisecond}
}

func rec(status string, fields map[string]any) map[string]any {
	r := map[string]any{"id": "x", "status": status}
	for k, v := range fields {
		r[k] = v
	}
	return r
}

func ok(result string) map[string]any { return rec("succeeded", map[string]any{"result": result}) }

func turn(request string) TurnInput { return TurnInput{Request: request} }

func payloadOf(t *testing.T, assertion string) map[string]any {
	t.Helper()
	b64, _, found := strings.Cut(assertion, ".")
	require.True(t, found)
	raw, err := base64.RawURLEncoding.DecodeString(b64)
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(raw, &payload))
	return payload
}

// assertedSubjects is the subject each POST /invoke asserted, "" for none.
func assertedSubjects(t *testing.T, g *fakeGateway) []string {
	var out []string
	for _, c := range g.posts() {
		header := c.Header.Get(rbac.CallerIdentityHeader)
		if header == "" {
			out = append(out, "")
			continue
		}
		subject, _, valid := rbac.VerifyCallerIdentityAssertion(engineAssertionSecret, header, time.Now())
		require.True(t, valid)
		out = append(out, subject)
	}
	return out
}

func TestEngineStartsATurnAndReturnsTheSettledResult(t *testing.T) {
	g := newFakeGateway(t, rec("pending", nil), ok("three pods, all Running"))
	e := g.engine()
	e.Token = "engine-token"
	out, err := e.Invoke(context.Background(), turn("what pods are running?"))
	require.NoError(t, err)
	require.Equal(t, "three pods, all Running", *out.Result)
	require.Empty(t, out.Error)
	calls := g.all()
	require.Equal(t, "/invoke", calls[0].Path)
	require.Equal(t, "/invoke/"+acceptedEngineID, calls[1].Path)
	require.Equal(t, "Bearer engine-token", calls[0].Header.Get("authorization"))
	require.Equal(t, "Bearer engine-token", calls[1].Header.Get("authorization"))
	require.Equal(t, map[string]any{"request": "what pods are running?"}, calls[0].Body)
}

func TestEnginePassesASucceededRecordsOutcomeThroughAsRunOutcome(t *testing.T) {
	outcome := map[string]any{"pullRequest": map[string]any{"number": float64(12), "url": "u"}, "madeNewCommits": true}
	g := newFakeGateway(t, rec("succeeded", map[string]any{"result": "reviewed", "outcome": outcome}))
	out, err := g.engine().Invoke(context.Background(), turn("x"))
	require.NoError(t, err)
	require.Equal(t, "reviewed", *out.Result)
	var got map[string]any
	require.NoError(t, json.Unmarshal(out.RunOutcome, &got))
	require.Equal(t, outcome, got)

	bare := newFakeGateway(t, ok("reviewed"))
	out, err = bare.engine().Invoke(context.Background(), turn("x"))
	require.NoError(t, err)
	require.Empty(t, out.RunOutcome)
}

// Start is the accept and nothing after it: one POST, no poll.
func TestEngineStartReturnsTheEngineIDWithoutPolling(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	id, err := g.engine().Start(context.Background(), turn("x"))
	require.NoError(t, err)
	require.Equal(t, acceptedEngineID, id)
	calls := g.all()
	require.Len(t, calls, 1)
	require.Equal(t, http.MethodPost, calls[0].Method)
}

func TestEngineStartRefusesWhatInvokeWouldFailBeforeTheEngine(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	trig := turn("x")
	trig.TriggerName = "review"
	_, err := g.engine().Start(context.Background(), trig)
	require.ErrorContains(t, err, "AGENT_SENDER_ASSERTION_SECRET")

	g.onPost = func(w http.ResponseWriter) { w.WriteHeader(http.StatusBadGateway) }
	_, err = g.engine().Start(context.Background(), turn("x"))
	require.ErrorContains(t, err, "502")
	require.Len(t, g.posts(), 1, "the trigger turn never reached the engine")
}

func TestEngineStatusMapsEachEngineAnswer(t *testing.T) {
	rc := map[string]any{"remoteControlUrl": "https://rc"}
	pending, err := newFakeGateway(t, rec("pending", rc)).engine().Status(context.Background(), "conversation-old.upd-9")
	require.NoError(t, err)
	require.Equal(t, EngineStatus{Pending: true, RemoteControlURL: "https://rc"}, pending)

	done, err := newFakeGateway(t, rec("succeeded", map[string]any{
		"result": "reviewed", "outcome": map[string]any{"verdict": "changes-requested"}, "remoteControlUrl": "https://rc",
	})).engine().Status(context.Background(), "conversation-old.upd-9")
	require.NoError(t, err)
	require.False(t, done.Pending)
	require.Equal(t, "reviewed", *done.Outcome.Result)
	require.JSONEq(t, `{"verdict":"changes-requested"}`, string(done.Outcome.RunOutcome))
	require.Equal(t, "https://rc", done.RemoteControlURL)

	failed, err := newFakeGateway(t, rec("failed", map[string]any{"error": "boom"})).engine().Status(context.Background(), "x.y")
	require.NoError(t, err)
	require.Equal(t, "boom", failed.Outcome.Error)
}

func TestEngineStatusTellsUnknownFromUnreachable(t *testing.T) {
	missing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }))
	t.Cleanup(missing.Close)
	unknown, err := (&TemporalEngine{BaseURL: missing.URL}).Status(context.Background(), "x.y")
	require.NoError(t, err)
	require.True(t, unknown.Unknown)

	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	t.Cleanup(broken.Close)
	_, err = (&TemporalEngine{BaseURL: broken.URL}).Status(context.Background(), "x.y")
	require.ErrorContains(t, err, "503")

	broken.Close()
	_, err = (&TemporalEngine{BaseURL: broken.URL}).Status(context.Background(), "x.y")
	require.Error(t, err)
}

// Only a failure to get an answer polls as pending. An id the engine rejects
// outright (a forged "t.foo") is unknown, and a 4xx that would repeat on every
// poll (a wrong engine token) fails the turn instead of leaving it pending
// until the caller's budget runs out.
func TestEngineStatusEnds4xxInsteadOfPollingForever(t *testing.T) {
	statusOf := func(code int) (EngineStatus, error) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }))
		t.Cleanup(server.Close)
		return (&TemporalEngine{BaseURL: server.URL}).Status(context.Background(), "foo")
	}

	malformed, err := statusOf(http.StatusBadRequest)
	require.NoError(t, err)
	require.True(t, malformed.Unknown)

	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		refused, err := statusOf(code)
		require.NoError(t, err)
		require.False(t, refused.Unknown)
		require.False(t, refused.Pending)
		require.Contains(t, refused.Outcome.Error, strconv.Itoa(code))
	}
}

func TestEngineReportsAFailedTurnAsAnError(t *testing.T) {
	g := newFakeGateway(t, rec("failed", map[string]any{"error": "the planner exploded"}))
	out, err := g.engine().Invoke(context.Background(), turn("x"))
	require.NoError(t, err)
	require.Equal(t, "the planner exploded", out.Error)
	require.Nil(t, out.Result)

	blank := newFakeGateway(t, rec("failed", nil))
	out, err = blank.engine().Invoke(context.Background(), turn("x"))
	require.NoError(t, err)
	require.Equal(t, "the turn failed", out.Error)
}

func TestEngineCarriesPendingCallerToolCallsThrough(t *testing.T) {
	g := newFakeGateway(t, rec("succeeded", map[string]any{"toolCalls": []any{map[string]any{"id": "call_1", "name": "web_search", "arguments": `{"query":"x"}`}}}))
	out, err := g.engine().Invoke(context.Background(), turn("x"))
	require.NoError(t, err)
	require.Equal(t, []callertools.PendingCall{{ID: "call_1", Name: "web_search", Arguments: `{"query":"x"}`}}, out.PendingToolCalls)
	require.Nil(t, out.Result)
}

func TestEngineForwardsCallerToolsAndPriorCallsInTheBody(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	tool := callertools.Descriptor{Name: "get_weather", Description: "Look up the weather", ParametersJSON: `{"type":"object"}`, Hash: "h1"}
	in := turn("x")
	in.CallerTools = []callertools.Descriptor{tool}
	in.CallerToolRequired = true
	in.PriorCallerToolCalls = []callertools.PriorCall{{ID: "call_1", Name: "caller:get_weather", Arguments: `{"city":"Oslo"}`, Result: "rain"}}
	_, err := g.engine().Invoke(context.Background(), in)
	require.NoError(t, err)

	body := g.posts()[0].Body
	require.Equal(t, []any{map[string]any{"name": "get_weather", "description": "Look up the weather", "parametersJson": `{"type":"object"}`, "hash": "h1"}}, body["callerTools"])
	require.Equal(t, true, body["callerToolRequired"])
	// The engine re-adds the `caller:` namespace itself, so it is stripped here.
	require.Equal(t, []any{map[string]any{"name": "get_weather", "arguments": `{"city":"Oslo"}`, "result": "rain"}}, body["priorCallerToolCalls"])
}

func TestEngineForwardsImagesOnlyWhenThereAreSome(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	with := turn("x")
	with.Images = []ChatImage{{URL: "data:image/png;base64,AAAA"}}
	_, err := g.engine().Invoke(context.Background(), with)
	require.NoError(t, err)
	_, err = g.engine().Invoke(context.Background(), turn("x"))
	require.NoError(t, err)
	posts := g.posts()
	require.Equal(t, []any{map[string]any{"url": "data:image/png;base64,AAAA"}}, posts[0].Body["images"])
	require.NotContains(t, posts[1].Body, "images")
}

func TestEngineSurfacesTheReasonForA400OnlyFor400(t *testing.T) {
	g := newFakeGateway(t, ok("unused"))
	g.onPost = func(w http.ResponseWriter) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"message": "image attachments are not enabled on this deployment", "type": "x"}})
	}
	_, err := g.engine().Invoke(context.Background(), turn("x"))
	require.EqualError(t, err, "temporal engine /invoke failed: 400 (image attachments are not enabled on this deployment)")

	g.onPost = func(w http.ResponseWriter) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "  plain reason "})
	}
	_, err = g.engine().Invoke(context.Background(), turn("x"))
	require.EqualError(t, err, "temporal engine /invoke failed: 400 (plain reason)")

	g.onPost = func(w http.ResponseWriter) {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "secret internals"})
	}
	_, err = g.engine().Invoke(context.Background(), turn("x"))
	require.EqualError(t, err, "temporal engine /invoke failed: 500")
}

func TestEngineSendsNoCallerToolFieldsForATurnWithoutAny(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	_, err := g.engine().Invoke(context.Background(), turn("x"))
	require.NoError(t, err)
	body := g.posts()[0].Body
	for _, field := range []string{"callerTools", "callerToolRequired", "priorCallerToolCalls"} {
		require.NotContains(t, body, field)
	}
}

func TestEngineForwardsATriggerTurnAsASignedDispatchNotBodyFields(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	e := g.engine()
	e.SenderAssertionSecret = engineAssertionSecret
	_, err := e.Invoke(context.Background(), TurnInput{
		Request: "Run tool slack-notify", SessionID: "event:ci:1",
		ForcedToolID: "slack-notify", ToolInput: `{"channel":"ops"}`, ReadOnly: true,
		Params: map[string]string{"channel": "ops"}, TriggerName: "ci-notify", TargetRepository: "acme/widgets",
	})
	require.NoError(t, err)

	post := g.posts()[0]
	require.Equal(t, map[string]any{"request": "Run tool slack-notify", "sessionId": "event:ci:1", "readOnly": true, "targetRepository": "acme/widgets"}, post.Body)

	header := post.Header.Get(rbac.DispatchAssertionHeader)
	d := rbac.VerifyDispatchAssertion(engineAssertionSecret, header, "event:ci:1", "Run tool slack-notify", time.Now())
	require.NotNil(t, d, "signed with the shared secret and bound to session + request")
	require.Equal(t, rbac.Dispatch{TriggerName: "ci-notify", ForcedToolID: "slack-notify", ToolInput: `{"channel":"ops"}`, ReadOnly: true, Params: map[string]string{"channel": "ops"}}, *d)
	payload := payloadOf(t, header)
	require.Equal(t, "dispatch", payload["typ"])
	require.Equal(t, rbac.RequestDigest("Run tool slack-notify"), payload["requestSha256"])
	// Bound: the same header does not verify for another prompt or session.
	require.Nil(t, rbac.VerifyDispatchAssertion(engineAssertionSecret, header, "event:ci:1", "something else", time.Now()))
	require.Nil(t, rbac.VerifyDispatchAssertion(engineAssertionSecret, header, "event:ci:2", "Run tool slack-notify", time.Now()))
}

func TestEngineDropsToolInputFromADispatchWithoutAToolRef(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	e := g.engine()
	e.SenderAssertionSecret = engineAssertionSecret
	_, err := e.Invoke(context.Background(), TurnInput{Request: "r", SessionID: "s", ForcedAgentID: "swe", TriggerName: "t", ToolInput: "stray"})
	require.NoError(t, err)
	payload := payloadOf(t, g.posts()[0].Header.Get(rbac.DispatchAssertionHeader))
	require.NotContains(t, payload, "toolInput")
	require.NotContains(t, payload, "params")
	require.Equal(t, "swe", payload["forcedAgentId"])
}

func TestEngineSignsAnIdentityOverrideIntoTheDispatchNeverTheBody(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	e := g.engine()
	e.SenderAssertionSecret = engineAssertionSecret
	_, err := e.Invoke(context.Background(), TurnInput{Request: "triage", SessionID: "s", ForcedAgentID: "swe", TriggerName: "t", IdentityOverride: "delegated"})
	require.NoError(t, err)
	post := g.posts()[0]
	require.NotContains(t, post.Body, "identity")
	require.NotContains(t, post.Body, "identityOverride")
	require.Equal(t, "delegated", payloadOf(t, post.Header.Get(rbac.DispatchAssertionHeader))["identity"])
}

func TestEngineRefusesATriggerTurnWithoutASecretToSignIt(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	out, err := g.engine().Invoke(context.Background(), TurnInput{Request: "x", ForcedAgentID: "claude-code-swe-agent", TriggerName: "github-issue-triage"})
	require.NoError(t, err)
	require.Contains(t, out.Error, "AGENT_SENDER_ASSERTION_SECRET")
	require.Empty(t, g.all())
}

func TestEngineSendsReadOnlyWheneverSetAndNoDispatchOnAnOrdinaryTurn(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	e := g.engine()
	e.SenderAssertionSecret = engineAssertionSecret
	_, err := e.Invoke(context.Background(), TurnInput{Request: "follow-up", ReadOnly: true})
	require.NoError(t, err)
	_, err = e.Invoke(context.Background(), turn("hello"))
	require.NoError(t, err)
	posts := g.posts()
	require.Equal(t, true, posts[0].Body["readOnly"])
	require.NotContains(t, posts[1].Body, "readOnly")
	for _, p := range posts {
		require.Empty(t, p.Header.Get(rbac.DispatchAssertionHeader))
	}
}

func TestEngineSurfacesAForcedTargetMissAsAFailedTurn(t *testing.T) {
	g := newFakeGateway(t, rec("failed", map[string]any{"error": `event trigger pr-review: agent "gone" is not available`}))
	e := g.engine()
	e.SenderAssertionSecret = engineAssertionSecret
	out, err := e.Invoke(context.Background(), TurnInput{Request: "x", ForcedAgentID: "gone", TriggerName: "pr-review"})
	require.NoError(t, err)
	require.Equal(t, `event trigger pr-review: agent "gone" is not available`, out.Error)
	require.Nil(t, out.Result)
}

func TestEngineForwardsTheIdentityLinkFlowOnlyWhenNamed(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	_, err := g.engine().Invoke(context.Background(), TurnInput{Request: "x", IdentityLinkFlow: "device"})
	require.NoError(t, err)
	_, err = g.engine().Invoke(context.Background(), turn("x"))
	require.NoError(t, err)
	require.Equal(t, "device", g.posts()[0].Body["identityLinkFlow"])
	require.NotContains(t, g.posts()[1].Body, "identityLinkFlow")
}

func TestEngineForwardsTheTargetRepositoryOnlyWhenSet(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	_, err := g.engine().Invoke(context.Background(), TurnInput{Request: "x", TargetRepository: "e2e-org/e2e-repo"})
	require.NoError(t, err)
	_, err = g.engine().Invoke(context.Background(), turn("x"))
	require.NoError(t, err)
	require.Equal(t, "e2e-org/e2e-repo", g.posts()[0].Body["targetRepository"])
	require.NotContains(t, g.posts()[1].Body, "targetRepository")
}

func TestEngineSignsTheSenderLoginRatherThanSendingItAsABodyField(t *testing.T) {
	secret := "shared-with-integration-gateway"
	g := newFakeGateway(t, ok("ok"))
	e := g.engine()
	e.SenderAssertionSecret = secret
	_, err := e.Invoke(context.Background(), TurnInput{Request: "x", SenderLogin: "imaustink"})
	require.NoError(t, err)
	post := g.posts()[0]
	require.Equal(t, "imaustink", rbac.VerifySenderAssertion(secret, post.Header.Get(rbac.SenderAssertionHeader), time.Now()))
	require.NotContains(t, post.Body, "senderLogin")
}

func TestEngineOmitsTheSenderAssertionWithoutASecret(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	_, err := g.engine().Invoke(context.Background(), TurnInput{Request: "x", SenderLogin: "imaustink"})
	require.NoError(t, err)
	require.Empty(t, g.posts()[0].Header.Get(rbac.SenderAssertionHeader))
}

func TestEngineReSignsTheGatewaysFullSenderClaims(t *testing.T) {
	claims := rbac.SenderClaims{Login: "alice", AccountID: "583231", Subject: "openwebui:a", SubjectStatus: rbac.SenderSubjectLinked}
	g := newFakeGateway(t, ok("ok"))
	e := g.engine()
	e.SenderAssertionSecret = engineAssertionSecret
	_, err := e.Invoke(context.Background(), TurnInput{Request: "x", SenderLogin: "alice", SenderClaims: &claims})
	require.NoError(t, err)
	require.Equal(t, &claims, rbac.VerifySenderClaims(engineAssertionSecret, g.posts()[0].Header.Get(rbac.SenderAssertionHeader), time.Now()))
}

// Claims for a DIFFERENT login than the verified one are not trusted: only
// the bare login is signed.
func TestEngineSignsOnlyTheLoginWhenClaimsDisagreeWithIt(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	e := g.engine()
	e.SenderAssertionSecret = engineAssertionSecret
	_, err := e.Invoke(context.Background(), TurnInput{Request: "x", SenderLogin: "alice", SenderClaims: &rbac.SenderClaims{Login: "mallory", AccountID: "1"}})
	require.NoError(t, err)
	require.Equal(t, &rbac.SenderClaims{Login: "alice"}, rbac.VerifySenderClaims(engineAssertionSecret, g.posts()[0].Header.Get(rbac.SenderAssertionHeader), time.Now()))
}

func TestEngineReportsAResumablePauseWhenItStopsWaiting(t *testing.T) {
	g := newFakeGateway(t, rec("pending", nil))
	e := g.engine()
	e.Timeout = time.Nanosecond
	out, err := e.Invoke(context.Background(), turn("x"))
	require.NoError(t, err)
	require.Empty(t, out.Error)
	require.Contains(t, *out.Result, "still running")
}

// flakyTransport fails the first `fail` GET polls at the transport level.
type flakyTransport struct {
	mu    sync.Mutex
	fail  int
	gets  int
	inner http.RoundTripper
}

func (f *flakyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet {
		f.mu.Lock()
		f.gets++
		failing := f.gets <= f.fail
		f.mu.Unlock()
		if failing {
			return nil, errors.New("UND_ERR_CONNECT_TIMEOUT")
		}
	}
	return f.inner.RoundTrip(r)
}

func TestEngineRetriesAPollRejectedMidRollout(t *testing.T) {
	g := newFakeGateway(t, ok("survived the roll"))
	e := g.engine()
	e.HTTP = &http.Client{Transport: &flakyTransport{fail: 2, inner: http.DefaultTransport}}
	out, err := e.Invoke(context.Background(), turn("x"))
	require.NoError(t, err)
	require.Empty(t, out.Error)
	require.Equal(t, "survived the roll", *out.Result)
}

func TestEngineGivesUpOnAnUnreachableGatewayOnlyOnceThePollBudgetIsSpent(t *testing.T) {
	g := newFakeGateway(t, ok("unused"))
	e := g.engine()
	transport := &flakyTransport{fail: 1 << 30, inner: http.DefaultTransport}
	e.HTTP = &http.Client{Transport: transport}
	e.Timeout = 30 * time.Millisecond
	_, err := e.Invoke(context.Background(), turn("x"))
	require.ErrorContains(t, err, "unreachable for the poll budget")
	require.Greater(t, transport.gets, 1, "retried before giving up")
}

func TestEngineFailsHonestlyOnANon2xxAnswer(t *testing.T) {
	g := newFakeGateway(t, ok("unused"))
	g.onPost = func(w http.ResponseWriter) { w.WriteHeader(http.StatusBadGateway); _, _ = io.WriteString(w, "nope") }
	_, err := g.engine().Invoke(context.Background(), turn("x"))
	require.ErrorContains(t, err, "502")

	// A non-2xx poll is fatal too, not retried.
	poll := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			writeJSON(w, http.StatusAccepted, map[string]any{"id": "c"})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(poll.Close)
	_, err = (&TemporalEngine{BaseURL: poll.URL}).Invoke(context.Background(), turn("x"))
	require.EqualError(t, err, "temporal engine /invoke/c failed: 404")
}

func TestEngineRelaysNewProgressLinesWithoutRepeatingThem(t *testing.T) {
	g := newFakeGateway(t,
		rec("pending", map[string]any{"progress": []any{"clone: cloning the repo"}}),
		rec("pending", map[string]any{"progress": []any{"clone: cloning the repo", "edit: adding the README"}}),
		ok("Opened PR #1"),
	)
	var got [][2]string
	in := turn("x")
	in.ProgressListener = func(stage, message string) { got = append(got, [2]string{stage, message}) }
	out, err := g.engine().Invoke(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, "Opened PR #1", *out.Result)
	require.Equal(t, [][2]string{{"", "clone: cloning the repo"}, {"", "edit: adding the README"}}, got)
}

func TestEngineUnflattensContentNarrationLinesBackIntoTheirStage(t *testing.T) {
	g := newFakeGateway(t,
		rec("pending", map[string]any{"progress": []any{
			"agent-text: Sure, here's the ",
			"agent-text: Sure, here's the fix",
			"identity-link: https://example.com/link",
			"remote-control-url: https://example.com/rc",
			"clone: cloning the repo",
		}}),
		ok("done"),
	)
	var got [][2]string
	in := turn("x")
	in.ProgressListener = func(stage, message string) { got = append(got, [2]string{stage, message}) }
	_, err := g.engine().Invoke(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, [][2]string{
		{"agent-text", "Sure, here's the "},
		{"agent-text", "Sure, here's the fix"},
		{"identity-link", "https://example.com/link"},
		{"remote-control-url", "https://example.com/rc"},
		{"", "clone: cloning the repo"},
	}, got)
}

func TestEngineNeedsNoProgressListener(t *testing.T) {
	g := newFakeGateway(t, rec("pending", map[string]any{"progress": []any{"clone: cloning the repo"}}), ok("Opened PR #1"))
	out, err := g.engine().Invoke(context.Background(), turn("x"))
	require.NoError(t, err)
	require.Equal(t, "Opened PR #1", *out.Result)
}

// ── auto-resuming a parked identity link ───────────────────────────────────

const linkPrompt = "To continue, please link your GitHub account..."

func linkRequired() map[string]any {
	return rec("succeeded", map[string]any{"result": linkPrompt, "path": "link-required"})
}

func TestAutoResumeResubmitsAndSurfacesTheResultWithoutRepeatingThePrompt(t *testing.T) {
	g := newFakeGateway(t, linkRequired(), ok("Opened PR #42."))
	var got [][2]string
	in := TurnInput{Request: "x", SessionID: "chat-1", ProgressListener: func(s, m string) { got = append(got, [2]string{s, m}) }}
	out, err := g.engine().Invoke(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, "Opened PR #42.", *out.Result)
	require.Equal(t, [][2]string{{"identity-link", linkPrompt}}, got)
	require.Len(t, g.posts(), 2)
	// The resume goes to the SAME session.
	require.Equal(t, "chat-1", g.posts()[1].Body["sessionId"])
}

func TestAutoResumeNeverHappensWithoutALiveChannel(t *testing.T) {
	g := newFakeGateway(t, linkRequired(), ok("never reached"))
	out, err := g.engine().Invoke(context.Background(), TurnInput{Request: "x", SessionID: "chat-1"})
	require.NoError(t, err)
	require.Equal(t, linkPrompt, *out.Result)
	require.Len(t, g.posts(), 1)
}

func TestAutoResumeNeverHappensWithoutASessionID(t *testing.T) {
	g := newFakeGateway(t, linkRequired(), ok("never reached"))
	calls := 0
	out, err := g.engine().Invoke(context.Background(), TurnInput{Request: "x", ProgressListener: func(string, string) { calls++ }})
	require.NoError(t, err)
	require.Equal(t, linkPrompt, *out.Result)
	require.Zero(t, calls)
	require.Len(t, g.posts(), 1)
}

func TestAutoResumeGivesUpAfterItsDeadlineWithoutRepeatingThePrompt(t *testing.T) {
	g := newFakeGateway(t, linkRequired())
	e := g.engine()
	e.AutoResumeMax = time.Nanosecond
	calls := 0
	out, err := e.Invoke(context.Background(), TurnInput{Request: "x", SessionID: "chat-1", ProgressListener: func(string, string) { calls++ }})
	require.NoError(t, err)
	require.Contains(t, *out.Result, "Still waiting for you to finish linking")
	require.NotContains(t, *out.Result, "please link your GitHub account")
	require.Equal(t, 1, calls)
}

func TestAutoResumeStopsWhenTheCallerGoesAway(t *testing.T) {
	g := newFakeGateway(t, linkRequired())
	e := g.engine()
	e.AutoResumeEvery = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	_, err := e.Invoke(ctx, TurnInput{Request: "x", SessionID: "chat-1", ProgressListener: func(string, string) {}})
	require.ErrorIs(t, err, context.Canceled)
}

func forwardedJWT(t *testing.T, userID string, ttl time.Duration) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"id": userID, "role": "user", "exp": time.Now().Add(ttl).Unix()}).SignedString([]byte(jwtSecret))
	require.NoError(t, err)
	return token
}

// countingResolver wraps a resolver and counts its calls.
type countingResolver struct {
	inner IdentityResolver
	mu    sync.Mutex
	n     int
}

func (c *countingResolver) Resolve(ctx context.Context, token string) *Identity {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return c.inner.Resolve(ctx, token)
}

// sequenceResolver answers its identities in order, then nil.
type sequenceResolver struct {
	mu   sync.Mutex
	ids  []*Identity
	seen int
}

func (s *sequenceResolver) Resolve(context.Context, string) *Identity {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen++
	if len(s.ids) == 0 {
		return nil
	}
	id := s.ids[0]
	s.ids = s.ids[1:]
	return id
}

// The bitovi incident: Open WebUI's JWT lives 300s and the resume loop runs
// for up to 10 minutes. Re-verifying it on each resume made the resume after
// expiry run as the gateway's shared subject. Here the forwarded-user
// resolver verifies the token exactly once and refuses it on any later call
// (as an expired JWT would be): every resume must still carry the subject.
func TestAutoResumeKeepsTheCallersIdentityAfterTheForwardedJWTWouldExpire(t *testing.T) {
	g := newFakeGateway(t, linkRequired(), linkRequired(), ok("Opened PR #42."))
	e := g.engine()
	e.SenderAssertionSecret = engineAssertionSecret
	resolver := &sequenceResolver{ids: []*Identity{{Subject: "openwebui:alice", Roles: []string{"writer"}, PerUser: true}}}
	e.ForwardedUserResolver = resolver
	out, err := e.Invoke(context.Background(), TurnInput{Request: "x", SessionID: "chat-1", ForwardedUserToken: "jwt", ProgressListener: func(string, string) {}})
	require.NoError(t, err)
	require.Equal(t, "Opened PR #42.", *out.Result)
	require.Equal(t, []string{"openwebui:alice", "openwebui:alice", "openwebui:alice"}, assertedSubjects(t, g))
	require.Equal(t, 1, resolver.seen)
}

func TestForwardedIdentityIsAssertedOnTheHop(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	e := g.engine()
	e.SenderAssertionSecret = engineAssertionSecret
	e.ForwardedUserResolver = &OpenWebUIForwardedUserResolver{Secret: []byte(jwtSecret), Roles: []string{"writer"}}
	_, err := e.Invoke(context.Background(), TurnInput{Request: "x", ForwardedUserToken: forwardedJWT(t, "alice", time.Minute)})
	require.NoError(t, err)
	require.Equal(t, []string{"openwebui:alice"}, assertedSubjects(t, g))
	_, roles, perUser := rbac.VerifyCallerIdentityAssertion(engineAssertionSecret, g.posts()[0].Header.Get(rbac.CallerIdentityHeader), time.Now())
	require.Equal(t, []string{"writer"}, roles)
	require.True(t, perUser)
}

func TestForwardedJWTIsResolvedOncePerTurnNotPerResume(t *testing.T) {
	g := newFakeGateway(t, rec("succeeded", map[string]any{"result": "link please", "path": "link-required"}), ok("done"))
	e := g.engine()
	e.SenderAssertionSecret = engineAssertionSecret
	counter := &countingResolver{inner: &OpenWebUIForwardedUserResolver{Secret: []byte(jwtSecret), Roles: []string{"writer"}}}
	e.ForwardedUserResolver = counter
	_, err := e.Invoke(context.Background(), TurnInput{Request: "x", SessionID: "chat-1", ForwardedUserToken: forwardedJWT(t, "alice", time.Minute), ProgressListener: func(string, string) {}})
	require.NoError(t, err)
	require.Len(t, g.posts(), 2)
	require.Equal(t, 1, counter.n)
}

func TestForwardedJWTThatDoesNotVerifyRefusesTheTurn(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	e := g.engine()
	e.SenderAssertionSecret = engineAssertionSecret
	e.ForwardedUserResolver = &OpenWebUIForwardedUserResolver{Secret: []byte(jwtSecret), Roles: []string{"writer"}}
	out, err := e.Invoke(context.Background(), TurnInput{Request: "x", ForwardedUserToken: forwardedJWT(t, "alice", -time.Minute)})
	require.NoError(t, err)
	require.Equal(t, "unauthorized: could not resolve caller identity", out.Error)
	require.Empty(t, g.all())
}

func TestForwardedIdentityThatCannotBeSignedRefusesTheTurn(t *testing.T) {
	captureLog(t)
	g := newFakeGateway(t, ok("ok"))
	e := g.engine()
	e.ForwardedUserResolver = &sequenceResolver{ids: []*Identity{{Subject: "openwebui:alice"}}}
	out, err := e.Invoke(context.Background(), TurnInput{Request: "x", ForwardedUserToken: "jwt"})
	require.NoError(t, err)
	require.Equal(t, "unauthorized: could not resolve caller identity", out.Error)
	require.Empty(t, g.all())
}

func TestNoCallerAssertionForATurnWithoutAForwardedJWT(t *testing.T) {
	g := newFakeGateway(t, ok("ok"))
	e := g.engine()
	e.SenderAssertionSecret = engineAssertionSecret
	resolver := &sequenceResolver{}
	e.ForwardedUserResolver = resolver
	out, err := e.Invoke(context.Background(), turn("x"))
	require.NoError(t, err)
	require.Equal(t, "ok", *out.Result)
	require.Zero(t, resolver.seen)
	require.Equal(t, []string{""}, assertedSubjects(t, g))
}

// ── the accept-phase check (docs/adr/0049 §6) ──────────────────────────────

func TestCheckIdentityReturnsOnlyAWellFormed403Refusal(t *testing.T) {
	refusal := map[string]any{"status": "failed", "error": map[string]any{"code": "identity_link_required", "message": "link it"}}
	cases := map[string]struct {
		answer func(w http.ResponseWriter)
		want   *IdentityRefusal
	}{
		"403 refusal": {func(w http.ResponseWriter) { writeJSON(w, http.StatusForbidden, refusal) },
			&IdentityRefusal{Body: refusal, Error: refusal["error"].(map[string]any)}},
		"204":           {func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }, nil},
		"500":           {func(w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) }, nil},
		"unrelated 403": {func(w http.ResponseWriter) { writeJSON(w, http.StatusForbidden, map[string]any{"unrelated": true}) }, nil},
		"403 without message": {func(w http.ResponseWriter) {
			writeJSON(w, http.StatusForbidden, map[string]any{"error": map[string]any{"code": "x"}})
		}, nil},
		"403 not JSON": {func(w http.ResponseWriter) { w.WriteHeader(http.StatusForbidden); _, _ = io.WriteString(w, "nope") }, nil},
	}
	claims := rbac.SenderClaims{Login: "alice", AccountID: "583231", Subject: "openwebui:a", SubjectStatus: rbac.SenderSubjectLinked}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			g := newFakeGateway(t, ok("unused"))
			g.onCheck = tc.answer
			e := g.engine()
			e.SenderAssertionSecret = engineAssertionSecret
			got := e.CheckIdentity(context.Background(), TurnInput{Request: "r", SessionID: "s", TriggerName: "t", ForcedAgentID: "swe", SenderLogin: "alice", SenderClaims: &claims, IdentityOverride: "delegated"})
			require.Equal(t, tc.want, got)
			calls := g.all()
			require.Len(t, calls, 1)
			require.Equal(t, "/invoke/check", calls[0].Path)
			// The check carries exactly what the turn would: claims and dispatch.
			require.Equal(t, &claims, rbac.VerifySenderClaims(engineAssertionSecret, calls[0].Header.Get(rbac.SenderAssertionHeader), time.Now()))
			require.Equal(t, "delegated", rbac.VerifyDispatchAssertion(engineAssertionSecret, calls[0].Header.Get(rbac.DispatchAssertionHeader), "s", "r", time.Now()).Identity)
		})
	}
	t.Run("unreachable", func(t *testing.T) {
		e := &TemporalEngine{BaseURL: "http://127.0.0.1:1", SenderAssertionSecret: engineAssertionSecret}
		require.Nil(t, e.CheckIdentity(context.Background(), TurnInput{Request: "r", TriggerName: "t", ForcedAgentID: "swe"}))
	})
	t.Run("no secret to sign the dispatch", func(t *testing.T) {
		g := newFakeGateway(t, ok("unused"))
		require.Nil(t, g.engine().CheckIdentity(context.Background(), TurnInput{Request: "r", TriggerName: "t", ForcedAgentID: "swe"}))
		require.Empty(t, g.all())
	})
}

// ── trace context (docs/adr/0050 §4) ───────────────────────────────────────

func TestEngineSendsAChildOfTheInboundTraceparentOrAFreshRoot(t *testing.T) {
	const inbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	for _, parent := range []string{inbound, ""} {
		g := newFakeGateway(t, ok("ok"))
		_, err := g.engine().Invoke(context.Background(), TurnInput{Request: "x", Traceparent: parent})
		require.NoError(t, err)
		sent := g.posts()[0].Header.Get("traceparent")
		require.Regexp(t, `^00-[0-9a-f]{32}-[0-9a-f]{16}-01$`, sent)
		if parent != "" {
			require.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", sent[3:35])
			require.NotEqual(t, inbound, sent)
		}
	}
}

func TestEngineWithExportOnSendsAnExportedClientSpansContext(t *testing.T) {
	spans := recordSpans(t)
	g := newFakeGateway(t, ok("ok"))
	_, err := g.engine().Invoke(context.Background(), TurnInput{Request: "x", Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"})
	require.NoError(t, err)
	hop := endedSpan(t, spans, "engine POST /invoke")
	require.Equal(t, "00f067aa0ba902b7", hop.Parent().SpanID().String())
	require.Equal(t, "00-4bf92f3577b34da6a3ce929d0e0e4736-"+hop.SpanContext().SpanID().String()+"-01", g.posts()[0].Header.Get("traceparent"))
	a := attrs(hop)
	require.Equal(t, int64(202), a["http.response.status_code"])
	require.Equal(t, acceptedEngineID, a["aktis.invocation_id"])
}

func TestEngineMarksAFailedEngineCallsSpanWithTheStatusCode(t *testing.T) {
	spans := recordSpans(t)
	g := newFakeGateway(t, ok("unused"))
	g.onPost = func(w http.ResponseWriter) { w.WriteHeader(http.StatusServiceUnavailable) }
	_, err := g.engine().Invoke(context.Background(), turn("x"))
	require.ErrorContains(t, err, "503")
	hop := endedSpan(t, spans, "engine POST /invoke")
	require.Equal(t, codes.Error, hop.Status().Code)
	require.Equal(t, "http_503", attrs(hop)["error.type"])
}
