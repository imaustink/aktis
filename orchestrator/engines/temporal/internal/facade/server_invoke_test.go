package facade

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
)

// Ported from apps/agent-orchestrator/src/server.test.ts ("InvokeServer").

// The id is all a poll needs (docs/adr/0058 §1): a replica that never saw
// the POST answers it, with no store in between.
func TestInvokeAnyReplicaAnswersAPollFromTheIDAlone(t *testing.T) {
	engine, settle := deferredEngine(t)
	accepting := serve(t, &Server{Engine: engine})
	polling := serve(t, &Server{Engine: engine})

	id := acceptedID(t, postInvoke(t, accepting, map[string]any{"request": "do the thing"}, nil))
	require.True(t, strings.HasPrefix(id, turnInvocationPrefix), id)
	require.Equal(t, "pending", getInvocation(t, polling, id)["status"])
	settle(resultOutcome("done"))

	require.Equal(t, map[string]any{"id": id, "status": "succeeded", "result": "done"}, settled(t, polling, id))
	require.Len(t, engine.invoked(), 1)
}

// Failing to ASK the engine is not the turn failing: the poller is told to
// ask again, not that its turn is gone.
func TestInvokePollAnswersPendingWhenTheEngineCannotBeAsked(t *testing.T) {
	logs := captureLog(t)
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine})
	id := acceptedID(t, postInvoke(t, base, map[string]any{"request": "do the thing"}, nil))

	engine.mu.Lock()
	engine.statusFn = func(string) (EngineStatus, error) { return EngineStatus{}, errors.New("dial tcp: connection refused") }
	engine.mu.Unlock()
	require.Equal(t, map[string]any{"id": id, "status": "pending"}, getInvocation(t, base, id))
	require.Contains(t, logs(), "connection refused")
}

func TestInvokePollIs404WhenTheEngineDoesNotKnowTheTurn(t *testing.T) {
	base := serve(t, &Server{Engine: newFakeEngine()})
	res := do(t, http.MethodGet, base+"/invoke/"+turnInvocationPrefix+"conversation-gone.u-1", nil, nil)
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}

// A turn the engine would not accept is still a 202: the caller hears of it
// from its poll, the way it always has, and runs its failure path from there.
func TestInvokeAnswers202WithAFailedIDWhenTheEngineRefusesToStart(t *testing.T) {
	engine := newFakeEngine()
	engine.startErr = errors.New("temporal engine /invoke failed: 400 (image too large)")
	accepting := serve(t, &Server{Engine: engine})
	other := serve(t, &Server{Engine: newFakeEngine()})

	id := acceptedID(t, postInvoke(t, accepting, map[string]any{"request": "do a thing"}, nil))
	require.True(t, strings.HasPrefix(id, failedInvocationPrefix), id)
	want := map[string]any{"id": id, "status": "failed", "error": "temporal engine /invoke failed: 400 (image too large)"}
	require.Equal(t, want, getInvocation(t, accepting, id))
	require.Equal(t, want, getInvocation(t, other, id), "stateless: any replica decodes it")
}

func TestInvokeFailedIDBoundsTheMessageAndRejectsGarbage(t *testing.T) {
	id := failedInvocationID(strings.Repeat("x", 5000))
	message, ok := decodeFailedInvocationID(id)
	require.True(t, ok)
	require.Len(t, message, maxFailedMessageBytes)

	base := serve(t, &Server{Engine: newFakeEngine()})
	res := do(t, http.MethodGet, base+"/invoke/"+failedInvocationPrefix+"!!not-base64", nil, nil)
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}

func TestInvokeRefusesNewTurnsOnceDraining(t *testing.T) {
	engine := newFakeEngine()
	s := &Server{Engine: engine}
	base := serve(t, s)
	s.BeginDraining()

	res := postInvoke(t, base, map[string]any{"request": "do the thing"}, nil)
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	require.Equal(t, "1", res.Header.Get("retry-after"))
	stillFalse(t, func() bool { return len(engine.invoked()) > 0 })
}

func TestInvokeAcceptsReturns202AndTheResultOnceTheTurnResolves(t *testing.T) {
	engine, settle := deferredEngine(t)
	base := serve(t, &Server{Engine: engine})

	res := postInvoke(t, base, map[string]any{"request": "scrape https://example.com/recipe"}, nil)
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	body := decode[map[string]any](t, res)
	require.Equal(t, "pending", body["status"])
	id := body["id"].(string)
	require.Equal(t, "/invoke/"+id, res.Header.Get("location"))

	got := engine.waitInvokes(t, 1)[0]
	requireInvokeTurn(t, TurnInput{Request: "scrape https://example.com/recipe"}, got)

	require.Equal(t, map[string]any{"id": id, "status": "pending"}, getInvocation(t, base, id))

	settle(resultOutcome("Pancakes"))
	record := settled(t, base, id)
	require.Equal(t, map[string]any{"id": id, "status": "succeeded", "result": "Pancakes"}, record)
}

func TestInvokeSurfacesRemoteControlURLWhilePendingAndOnTheTerminalRecord(t *testing.T) {
	release := make(chan TurnOutcome, 1)
	var engine *fakeEngine
	engine = &fakeEngine{invokeFn: func(_ context.Context, in TurnInput) (TurnOutcome, error) {
		engine.setRemoteControlURL("https://claude.ai/code/session_abc123")
		return <-release, nil
	}}
	t.Cleanup(func() {
		select {
		case release <- TurnOutcome{}:
		default:
		}
	})
	base := serve(t, &Server{Engine: engine})

	id := acceptedID(t, postInvoke(t, base, map[string]any{"request": "triage this"}, nil))
	var pending map[string]any
	waitFor(t, func() bool {
		pending = getInvocation(t, base, id)
		return pending["remoteControlUrl"] != nil
	})
	require.Equal(t, "pending", pending["status"])
	require.Equal(t, "https://claude.ai/code/session_abc123", pending["remoteControlUrl"])
	// This fire-and-forget path never sets a progress listener.
	require.Nil(t, engine.invoked()[0].ProgressListener)

	release <- resultOutcome("opened PR #7")
	done := settled(t, base, id)
	require.Equal(t, "succeeded", done["status"])
	require.Equal(t, "https://claude.ai/code/session_abc123", done["remoteControlUrl"])
}

func TestInvokeOmitsRemoteControlURLWhenTheEngineNeverReportsOne(t *testing.T) {
	base := serve(t, &Server{Engine: newFakeEngine()})
	id := acceptedID(t, postInvoke(t, base, map[string]any{"request": "open a PR"}, nil))
	record := settled(t, base, id)
	require.Equal(t, "succeeded", record["status"])
	require.NotContains(t, record, "remoteControlUrl")
}

func TestInvokeMarksTheInvocationFailedWhenTheTurnSettlesWithAnError(t *testing.T) {
	base := serve(t, &Server{Engine: engineReturning(TurnOutcome{Error: "no matching skill for this request"})})
	// No authorization header: without a CallerAuth gate nothing requires one.
	res := do(t, http.MethodPost, base+"/invoke", map[string]any{"request": "do a thing"}, nil)
	id := acceptedID(t, res)
	record := settled(t, base, id)
	require.Equal(t, map[string]any{"id": id, "status": "failed", "error": "no matching skill for this request"}, record)
}

func TestInvokeMarksTheInvocationFailedWhenTheEngineCallItselfFails(t *testing.T) {
	engine := &fakeEngine{invokeFn: func(context.Context, TurnInput) (TurnOutcome, error) {
		return TurnOutcome{}, errors.New("temporal engine /invoke failed: 502")
	}}
	base := serve(t, &Server{Engine: engine})
	id := acceptedID(t, postInvoke(t, base, map[string]any{"request": "do a thing"}, nil))
	record := settled(t, base, id)
	require.Equal(t, "failed", record["status"])
	require.Equal(t, "temporal engine /invoke failed: 502", record["error"])
}

func TestInvokeRejectsAMissingOrInvalidBodyWith400(t *testing.T) {
	for name, body := range map[string]any{
		"empty object":   map[string]any{},
		"blank request":  map[string]any{"request": "   "},
		"non-string":     map[string]any{"request": 7},
		"not JSON":       "{nope",
		"not an object":  `["request"]`,
		"no body at all": nil,
	} {
		t.Run(name, func(t *testing.T) {
			engine := newFakeEngine()
			base := serve(t, &Server{Engine: engine})
			res := postInvoke(t, base, body, nil)
			require.Equal(t, http.StatusBadRequest, res.StatusCode)
			require.Equal(t, map[string]any{"error": invalidInvokeBody}, decode[map[string]any](t, res))
			stillFalse(t, func() bool { return len(engine.invoked()) > 0 })
		})
	}
}

func TestInvokeReturns404ForAnUnknownInvocationID(t *testing.T) {
	base := serve(t, &Server{Engine: newFakeEngine()})
	res := do(t, http.MethodGet, base+"/invoke/does-not-exist", nil, nil)
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}

// failingSetSessionStore reads fine and fails every write.
type failingSetSessionStore struct{}

func (failingSetSessionStore) Get(context.Context, string) *SessionRecord { return nil }
func (failingSetSessionStore) Set(context.Context, string, bool) error {
	return errors.New("redis: connection reset")
}

// TS: "marks the invocation failed (not an unhandled rejection) when
// buildTurnInput itself rejects". The Go SessionStore.Get is soft-failing by
// contract and cannot error, so the remaining fallible step before the engine
// is recording the turn's read-only mode: a failure there fails the record
// and launches nothing.
func TestInvokeMarksTheInvocationFailedWhenRecordingTheSessionModeFails(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Sessions: failingSetSessionStore{}, Triggers: triggerRegistry(t, reviewTrigger())})

	id := acceptedID(t, postInvoke(t, base, map[string]any{"event": reviewEvent()}, nil))
	record := settled(t, base, id)
	require.Equal(t, "failed", record["status"])
	require.Equal(t, "redis: connection reset", record["error"])
	require.Empty(t, engine.invoked())
}

func TestInvokeCarriesAnOptionalSessionIDIntoTheTurn(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Sessions: &MemorySessionStore{TTL: time.Minute, MaxEntries: 10}})
	postInvoke(t, base, map[string]any{"request": "extract https://example.com", "session_id": "cli-7"}, nil)
	requireInvokeTurn(t, TurnInput{Request: "extract https://example.com", SessionID: "cli-7"}, engine.waitInvokes(t, 1)[0])
}

// ── event envelopes (ADR 0048, docs/event-triggers.md §5) ──────────────────

func TestInvokeEventAnswers200IgnoredAndStartsNoTurnWhenNoTriggerMatches(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, reviewTrigger())})

	// Even with a perfectly good `request`, which must NOT be run as a fallback.
	res := postInvoke(t, base, map[string]any{"request": "please do something", "event": envelope(nil)}, nil)
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, map[string]any{"status": "ignored"}, decode[map[string]any](t, res))
	stillFalse(t, func() bool { return len(engine.invoked()) > 0 })
}

func TestInvokeEventIsIgnoredWhenNoTriggerTableIsConfigured(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine})
	res := postInvoke(t, base, map[string]any{"event": reviewEvent()}, nil)
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, map[string]any{"status": "ignored"}, decode[map[string]any](t, res))
	stillFalse(t, func() bool { return len(engine.invoked()) > 0 })
}

func TestInvokeEventAnswers422WhenTheMatchedTriggersParamsCannotBeResolved(t *testing.T) {
	broken := trigger("needs-a-pr", catalog.EventTriggerSpec{
		Params: map[string]string{"number": "body.pull_request.number"},
		Target: catalog.EventTriggerTarget{Orchestrator: &struct{}{}, PromptTemplate: "PR {{number}}"},
	})
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, broken)})

	res := postInvoke(t, base, map[string]any{"event": envelope(nil)}, nil)
	require.Equal(t, http.StatusUnprocessableEntity, res.StatusCode)
	body := decode[map[string]any](t, res)
	require.Equal(t, "failed", body["status"])
	require.Contains(t, body["error"], "needs-a-pr")
	stillFalse(t, func() bool { return len(engine.invoked()) > 0 })
}

func TestInvokeEventAnswers400ForAMalformedEnvelope(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, catchAllTrigger())})
	res := postInvoke(t, base, map[string]any{"request": "triage", "event": envelope(map[string]any{"kind": nil})}, nil)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
	require.Contains(t, decode[map[string]any](t, res)["error"], "event.kind")
	stillFalse(t, func() bool { return len(engine.invoked()) > 0 })
}

func TestInvokeEventStartsTheResolvedTurnAndAnswers202WithTheTrigger(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, reviewTrigger())})

	res := postInvoke(t, base, map[string]any{"request": "ignored when an event matches", "event": reviewEvent()}, nil)
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	body := decode[map[string]any](t, res)
	require.NotEmpty(t, body["id"])
	delete(body, "id")
	require.Equal(t, map[string]any{
		"status": "pending",
		"trigger": map[string]any{
			"name":      "github-pr-review",
			"sessionId": "github:acme/widgets#7",
			"reply":     map[string]any{"github": map[string]any{"repository": "acme/widgets", "issueNumber": float64(7), "ack": "Reviewing…", "result": true}},
			"params":    map[string]any{"repo": "acme/widgets", "number": "7"},
		},
	}, body)

	in := engine.waitInvokes(t, 1)[0]
	requireInvokeTurn(t, TurnInput{
		Request:          "Review acme/widgets#7",
		SessionID:        "github:acme/widgets#7",
		ForcedAgentID:    "claude-code-swe-agent",
		ReadOnly:         true,
		Params:           map[string]string{"repo": "acme/widgets", "number": "7"},
		TriggerName:      "github-pr-review",
		TargetRepository: "acme/widgets",
		// No secret configured: the unsigned envelope login is trusted.
		SenderLogin: "imaustink",
	}, in)
}

func TestInvokeEventForcesASkillForASkillRefTarget(t *testing.T) {
	skill := trigger("ci-failed", catalog.EventTriggerSpec{Target: catalog.EventTriggerTarget{SkillRef: "ci-triage-skill", PromptTemplate: "CI failed"}})
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, skill)})
	postInvoke(t, base, map[string]any{"event": envelope(nil)}, nil)

	in := engine.waitInvokes(t, 1)[0]
	require.Equal(t, "CI failed", in.Request)
	require.Equal(t, "ci-triage-skill", in.ForcedSkillID)
	require.Equal(t, "ci-failed", in.TriggerName)
	require.Empty(t, in.ForcedAgentID)
	// Not read-only: the trigger didn't ask for it.
	require.False(t, in.ReadOnly)
}

func TestInvokeEventForcesAToolRunWithTheToolInputAndADescriptiveRequest(t *testing.T) {
	tool := trigger("notify", catalog.EventTriggerSpec{
		Params: map[string]string{"input": "body.issue.title"},
		Target: catalog.EventTriggerTarget{ToolRef: "slack-notify"},
	})
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, tool)})
	postInvoke(t, base, map[string]any{"event": envelope(nil)}, nil)

	in := engine.waitInvokes(t, 1)[0]
	require.Equal(t, "Run tool slack-notify", in.Request)
	require.Equal(t, "slack-notify", in.ForcedToolID)
	require.Equal(t, "Add dark mode", in.ToolInput)
}

func TestInvokeEventRunsAnOrchestratorTargetAsAConversationalTurn(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, catchAllTrigger())})
	postInvoke(t, base, map[string]any{"event": envelope(map[string]any{"repository": ""}), "session_id": "caller-session"}, nil)

	in := engine.waitInvokes(t, 1)[0]
	require.Equal(t, "triage", in.Request)
	require.Equal(t, "caller-session", in.SessionID)
	require.Equal(t, "catch-all", in.TriggerName)
	require.Empty(t, in.ForcedSkillID)
	require.Empty(t, in.ForcedAgentID)
	require.Empty(t, in.ForcedToolID)
	require.Empty(t, in.TargetRepository, "an empty envelope.repository names no repository")
}

func TestInvokeLeavesARequestWithoutAnEventConversational(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, catchAllTrigger())})
	res := postInvoke(t, base, map[string]any{"request": "what's for dinner?"}, nil)
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	require.NotContains(t, decode[map[string]any](t, res), "trigger")

	in := engine.waitInvokes(t, 1)[0]
	require.Equal(t, "what's for dinner?", in.Request)
	require.Empty(t, in.TriggerName)
	require.False(t, in.ReadOnly)
}

func TestInvokeEventCarriesTheWebhookRepositoryAsTheTargetRepository(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, catchAllTrigger())})

	postInvoke(t, base, map[string]any{"event": envelope(map[string]any{"repository": "e2e-org/e2e-repo"})}, nil)
	require.Equal(t, "e2e-org/e2e-repo", engine.waitInvokes(t, 1)[0].TargetRepository)

	postInvoke(t, base, map[string]any{"event": envelope(map[string]any{"repository": ""})}, nil)
	require.Empty(t, engine.waitInvokes(t, 2)[1].TargetRepository)
}

// ── sticky readOnly per session ────────────────────────────────────────────

func memSessions() *MemorySessionStore { return &MemorySessionStore{TTL: time.Minute, MaxEntries: 10} }

func readOnlyOf(s SessionStore, id string) bool {
	r := s.Get(context.Background(), id)
	return r != nil && r.ReadOnly
}

func TestStickyReadOnlyAppliesToLaterTurnsInTheSessionOnly(t *testing.T) {
	sessions := memSessions()
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Sessions: sessions, Triggers: triggerRegistry(t, reviewTrigger())})

	postInvoke(t, base, map[string]any{"event": reviewEvent()}, nil)
	engine.waitInvokes(t, 1)
	require.True(t, readOnlyOf(sessions, "github:acme/widgets#7"))

	// A session-page follow-up: no event, just a prompt on the same session.
	postInvoke(t, base, map[string]any{"request": "now push a fix", "session_id": "github:acme/widgets#7"}, nil)
	require.True(t, engine.waitInvokes(t, 2)[1].ReadOnly)
	require.True(t, readOnlyOf(sessions, "github:acme/widgets#7"))

	// A different conversation is untouched.
	postInvoke(t, base, map[string]any{"request": "now push a fix", "session_id": "github:acme/widgets#8"}, nil)
	require.False(t, engine.waitInvokes(t, 3)[2].ReadOnly)
	require.Nil(t, sessions.Get(context.Background(), "github:acme/widgets#8"))
}

func TestStickyReadOnlyAppliesToAChatFollowUpOnTheSameSession(t *testing.T) {
	sessions := memSessions()
	require.NoError(t, sessions.Set(context.Background(), "chat-ro", true))
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Sessions: sessions})

	postChat(t, base, map[string]any{"messages": []any{map[string]any{"role": "user", "content": "push it"}}}, map[string]string{"x-openwebui-chat-id": "chat-ro"})
	in := engine.invoked()[0]
	require.True(t, in.ReadOnly)
	require.Equal(t, "chat-ro", in.SessionID)
}

func TestStickyReadOnlyIsClearedByAWritableTriggerOnTheSameSession(t *testing.T) {
	triage := trigger("github-pr-triage", catalog.EventTriggerSpec{
		SourceRef:  "github",
		Match:      catalog.EventTriggerMatch{CEL: `body.action == "labeled" && body.label.name == "ai-triage"`},
		Params:     map[string]string{"repo": "body.repository.full_name", "number": "body.issue.number"},
		SessionKey: "github:{{repo}}#{{number}}",
		Target:     catalog.EventTriggerTarget{AgentRef: "claude-code-swe-agent", PromptTemplate: "Triage {{repo}}#{{number}}"},
	})
	triageEvent := envelope(map[string]any{"body": map[string]any{
		"action": "labeled", "label": map[string]any{"name": "ai-triage"},
		"repository": map[string]any{"full_name": "acme/widgets"}, "issue": map[string]any{"number": 7},
	}})
	sessions := memSessions()
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Sessions: sessions, Triggers: triggerRegistry(t, reviewTrigger(), triage)})
	const sid = "github:acme/widgets#7"

	postInvoke(t, base, map[string]any{"event": reviewEvent()}, nil)
	engine.waitInvokes(t, 1)
	require.True(t, readOnlyOf(sessions, sid))

	postInvoke(t, base, map[string]any{"event": triageEvent}, nil)
	in := engine.waitInvokes(t, 2)[1]
	require.Equal(t, "github-pr-triage", in.TriggerName)
	require.False(t, in.ReadOnly)
	require.False(t, readOnlyOf(sessions, sid))

	// Its follow-ups inherit the writable mode...
	postInvoke(t, base, map[string]any{"request": "also fix the typo", "session_id": sid}, nil)
	require.False(t, engine.waitInvokes(t, 3)[2].ReadOnly)

	// ...and a later review makes it read-only again.
	postInvoke(t, base, map[string]any{"event": reviewEvent()}, nil)
	require.True(t, engine.waitInvokes(t, 4)[3].ReadOnly)
	require.True(t, readOnlyOf(sessions, sid))
}

func TestStickyReadOnlyIsRecordedBeforeTheTurnRuns(t *testing.T) {
	sessions := memSessions()
	saw := make(chan bool, 1)
	stop := hangingInvoke(t)
	engine := &fakeEngine{invokeFn: func(ctx context.Context, in TurnInput) (TurnOutcome, error) {
		saw <- readOnlyOf(sessions, "github:acme/widgets#7")
		return stop(ctx, in) // the pod "dies" mid-turn
	}}
	base := serve(t, &Server{Engine: engine, Sessions: sessions, Triggers: triggerRegistry(t, reviewTrigger())})
	postInvoke(t, base, map[string]any{"event": reviewEvent()}, nil)

	select {
	case v := <-saw:
		require.True(t, v)
	case <-time.After(3 * time.Second):
		t.Fatal("turn never started")
	}
}
