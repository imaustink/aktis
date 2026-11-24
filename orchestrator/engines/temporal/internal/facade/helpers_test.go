package facade

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
)

// anyTraceparent is every version-00 traceparent: every /invoke turn carries
// its server span's (docs/adr/0050 §4).
var anyTraceparent = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$`)

// fakeEngine is an Engine whose calls can be inspected.
type fakeEngine struct {
	mu      sync.Mutex
	invokes []TurnInput
	checks  []TurnInput
	adopts  []string

	invokeFn func(ctx context.Context, in TurnInput) (TurnOutcome, error)
	checkFn  func(in TurnInput) *IdentityRefusal
	adoptFn  func(ctx context.Context, id string, in TurnInput) (TurnOutcome, error)
}

func resultOutcome(s string) TurnOutcome { return TurnOutcome{Result: &s} }

// engineReturning ends every turn with outcome.
func engineReturning(outcome TurnOutcome) *fakeEngine {
	return &fakeEngine{invokeFn: func(context.Context, TurnInput) (TurnOutcome, error) { return outcome, nil }}
}

func newFakeEngine() *fakeEngine { return engineReturning(resultOutcome("done")) }

func (f *fakeEngine) Invoke(ctx context.Context, in TurnInput) (TurnOutcome, error) {
	f.mu.Lock()
	f.invokes = append(f.invokes, in)
	fn := f.invokeFn
	f.mu.Unlock()
	return fn(ctx, in)
}

func (f *fakeEngine) Adopt(ctx context.Context, id string, in TurnInput) (TurnOutcome, error) {
	f.mu.Lock()
	f.adopts = append(f.adopts, id)
	fn := f.adoptFn
	f.mu.Unlock()
	if fn == nil {
		<-ctx.Done()
		return TurnOutcome{}, ctx.Err()
	}
	return fn(ctx, id, in)
}

func (f *fakeEngine) CheckIdentity(_ context.Context, in TurnInput) *IdentityRefusal {
	f.mu.Lock()
	f.checks = append(f.checks, in)
	fn := f.checkFn
	f.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(in)
}

func (f *fakeEngine) invoked() []TurnInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]TurnInput(nil), f.invokes...)
}

func (f *fakeEngine) checked() []TurnInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]TurnInput(nil), f.checks...)
}

func (f *fakeEngine) adopted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.adopts...)
}

// waitInvokes waits until the engine has seen n turns and returns them.
func (f *fakeEngine) waitInvokes(t *testing.T, n int) []TurnInput {
	t.Helper()
	waitFor(t, func() bool { return len(f.invoked()) >= n })
	return f.invoked()
}

// deferredEngine's turns settle only when the test says so.
type deferred struct {
	ch   chan TurnOutcome
	once sync.Once
}

func deferredEngine(t *testing.T) (*fakeEngine, func(TurnOutcome)) {
	d := &deferred{ch: make(chan TurnOutcome, 1)}
	engine := &fakeEngine{invokeFn: func(ctx context.Context, _ TurnInput) (TurnOutcome, error) {
		select {
		case o := <-d.ch:
			return o, nil
		case <-ctx.Done():
			return TurnOutcome{}, ctx.Err()
		}
	}}
	settle := func(o TurnOutcome) { d.once.Do(func() { d.ch <- o }) }
	t.Cleanup(func() { settle(TurnOutcome{}) })
	return engine, settle
}

// hangingInvoke never settles until the test ends.
func hangingInvoke(t *testing.T) func(context.Context, TurnInput) (TurnOutcome, error) {
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	return func(ctx context.Context, _ TurnInput) (TurnOutcome, error) {
		<-stop
		return TurnOutcome{}, context.Canceled
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// stillFalse asserts cond stays false for a short while (something that must
// NOT happen asynchronously).
func stillFalse(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(deadline) {
		require.False(t, cond())
		time.Sleep(2 * time.Millisecond)
	}
}

// plain is a TurnInput with the callbacks and traceparent cleared, for exact
// comparison. Callers assert those separately.
func plain(in TurnInput) TurnInput {
	in.ProgressListener = nil
	in.RemoteControlURLListener = nil
	in.ReportEngineInvocation = nil
	in.Traceparent = ""
	return in
}

// requireInvokeTurn is the TS expectation for every /invoke turn: the two
// bookkeeping callbacks, a traceparent, NO progress listener, and otherwise
// exactly want.
func requireInvokeTurn(t *testing.T, want, got TurnInput) {
	t.Helper()
	require.NotNil(t, got.RemoteControlURLListener)
	require.NotNil(t, got.ReportEngineInvocation)
	require.Nil(t, got.ProgressListener)
	require.Regexp(t, anyTraceparent, got.Traceparent)
	require.Equal(t, want, plain(got))
}

func serve(t *testing.T, s *Server) string {
	t.Helper()
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)
	return ts.URL
}

func do(t *testing.T, method, url string, body any, headers map[string]string) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != nil {
		if raw, ok := body.(string); ok {
			reader = strings.NewReader(raw)
		} else {
			payload, err := json.Marshal(body)
			require.NoError(t, err)
			reader = bytes.NewReader(payload)
		}
	}
	req, err := http.NewRequest(method, url, reader)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { res.Body.Close() })
	return res
}

func withAuth(headers map[string]string) map[string]string {
	out := map[string]string{"authorization": "Bearer tok-1"}
	for k, v := range headers {
		out[k] = v
	}
	return out
}

func postInvoke(t *testing.T, base string, body any, headers map[string]string) *http.Response {
	return do(t, http.MethodPost, base+"/invoke", body, withAuth(headers))
}

func postChat(t *testing.T, base string, body any, headers map[string]string) *http.Response {
	return do(t, http.MethodPost, base+"/v1/chat/completions", body, withAuth(headers))
}

func decode[T any](t *testing.T, res *http.Response) T {
	t.Helper()
	var v T
	require.NoError(t, json.NewDecoder(res.Body).Decode(&v))
	return v
}

func getInvocation(t *testing.T, base, id string) map[string]any {
	t.Helper()
	res := do(t, http.MethodGet, base+"/invoke/"+id, nil, nil)
	require.Equal(t, http.StatusOK, res.StatusCode)
	return decode[map[string]any](t, res)
}

// settled polls until the record leaves "pending".
func settled(t *testing.T, base, id string) map[string]any {
	t.Helper()
	var record map[string]any
	waitFor(t, func() bool {
		record = getInvocation(t, base, id)
		return record["status"] != "pending"
	})
	return record
}

func acceptedID(t *testing.T, res *http.Response) string {
	t.Helper()
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	body := decode[map[string]any](t, res)
	id, _ := body["id"].(string)
	require.NotEmpty(t, id)
	return id
}

// ── SSE ────────────────────────────────────────────────────────────────────

type sseChunk struct {
	Event *struct {
		Type string `json:"type"`
		Data *struct {
			Description string `json:"description"`
			Done        bool   `json:"done"`
		} `json:"data"`
	} `json:"event"`
	Choices []struct {
		Delta struct {
			Content   *string          `json:"content"`
			ToolCalls []map[string]any `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
}

// readSSE returns the data chunks (without [DONE]) and the raw text.
func readSSE(t *testing.T, res *http.Response) ([]sseChunk, string) {
	t.Helper()
	raw, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	var chunks []sseChunk
	for _, block := range strings.Split(string(raw), "\n\n") {
		block = strings.TrimSpace(block)
		if !strings.HasPrefix(block, "data: ") || block == "data: [DONE]" {
			continue
		}
		var c sseChunk
		require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(block, "data: ")), &c), block)
		chunks = append(chunks, c)
	}
	return chunks, string(raw)
}

func contentOf(chunks []sseChunk) string {
	var b strings.Builder
	for _, c := range chunks {
		if len(c.Choices) > 0 && c.Choices[0].Delta.Content != nil {
			b.WriteString(*c.Choices[0].Delta.Content)
		}
	}
	return b.String()
}

func statusesOf(chunks []sseChunk) []string {
	var out []string
	for _, c := range chunks {
		if c.Event != nil && c.Event.Type == "status" && c.Event.Data != nil {
			out = append(out, c.Event.Data.Description)
		}
	}
	return out
}

func withChoices(chunks []sseChunk) []sseChunk {
	var out []sseChunk
	for _, c := range chunks {
		if len(c.Choices) > 0 {
			out = append(out, c)
		}
	}
	return out
}

// ── triggers ───────────────────────────────────────────────────────────────

// triggerRegistry is a real trigger table over the given specs, each
// controller-Ready, each required to compile (or the test would be vacuous).
func triggerRegistry(t *testing.T, triggers ...*catalog.EventTriggerDescriptor) *catalog.EventTriggerRegistry {
	t.Helper()
	r := catalog.NewEventTriggerRegistry()
	for _, d := range triggers {
		require.NoError(t, d.Invalid(), d.Name)
		r.Upsert(d)
	}
	return r
}

func trigger(name string, spec catalog.EventTriggerSpec) *catalog.EventTriggerDescriptor {
	return catalog.NewEventTriggerDescriptor(name, spec, true)
}

// catchAllTrigger matches every event from any source: an ordinary
// conversational turn on "triage".
func catchAllTrigger() *catalog.EventTriggerDescriptor {
	return trigger("catch-all", catalog.EventTriggerSpec{Target: catalog.EventTriggerTarget{Orchestrator: &struct{}{}, PromptTemplate: "triage"}})
}

func reviewTrigger() *catalog.EventTriggerDescriptor {
	return trigger("github-pr-review", catalog.EventTriggerSpec{
		SourceRef:  "github",
		Match:      catalog.EventTriggerMatch{CEL: `body.action == "labeled" && body.label.name == "ai-review"`},
		Params:     map[string]string{"repo": "body.repository.full_name", "number": "body.issue.number"},
		SessionKey: "github:{{repo}}#{{number}}",
		Target:     catalog.EventTriggerTarget{AgentRef: "claude-code-swe-agent", ReadOnly: true, PromptTemplate: "Review {{repo}}#{{number}}"},
		Reply: &catalog.EventTriggerReply{GitHub: &catalog.EventGitHubReply{
			Repository: "{{repo}}", IssueNumber: "{{number}}", Ack: "Reviewing…", Result: true,
		}},
	})
}

// envelope is a well-formed §2 envelope, overridable per test.
func envelope(overrides map[string]any) map[string]any {
	e := map[string]any{
		"source":  "github",
		"kind":    "webhook",
		"id":      "delivery-1",
		"time":    "2026-10-13T00:00:00Z",
		"headers": map[string]any{"x-github-event": "issues"},
		"body": map[string]any{
			"action": "labeled", "label": map[string]any{"name": "ai-triage"},
			"repository": map[string]any{"full_name": "acme/widgets"},
			"issue":      map[string]any{"number": 7, "title": "Add dark mode"},
		},
		"sender":     map[string]any{"login": "imaustink", "isBot": false},
		"repository": "acme/widgets",
	}
	for k, v := range overrides {
		if v == nil {
			delete(e, k)
		} else {
			e[k] = v
		}
	}
	return e
}

func reviewEvent() map[string]any {
	return envelope(map[string]any{"body": map[string]any{
		"action": "labeled", "label": map[string]any{"name": "ai-review"},
		"repository": map[string]any{"full_name": "acme/widgets"}, "issue": map[string]any{"number": 7},
	}})
}

// captureLog redirects the standard logger for the test.
func captureLog(t *testing.T) func() string {
	var mu sync.Mutex
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}))
	t.Cleanup(func() { log.SetOutput(prev) })
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		return buf.String()
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
