package facade

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
	"github.com/aktisai/orchestrator/engines/temporal/internal/rbac"
)

// Ported from server.run-events.test.ts (docs/adr/0052): the 202's trigger
// params/onComplete, target.none, the run outcome on the poll, CheckOutcome,
// and adopting a turn whose process died.

func runEnvelope(overrides map[string]any) map[string]any {
	e := map[string]any{
		"source":  "runs",
		"kind":    "run",
		"id":      "run-evt-1",
		"time":    "2026-10-13T00:00:00Z",
		"headers": map[string]any{},
		"body": map[string]any{
			"trigger": "github-issue-triage",
			"status":  "succeeded",
			"params":  map[string]any{"repository": "acme/widgets", "number": "7"},
			"outcome": map[string]any{"pullRequest": map[string]any{"number": 12, "url": "https://github.com/acme/widgets/pull/12"}, "madeNewCommits": true},
			"chain":   map[string]any{"id": "d-1", "depth": 1, "counts": map[string]any{"github-issue-triage": 1}, "origin": map[string]any{"login": "alice", "repository": "acme/widgets"}},
		},
		"sender":     map[string]any{"login": "alice", "isBot": false},
		"repository": "acme/widgets",
	}
	for k, v := range overrides {
		e[k] = v
	}
	return e
}

func factoryReviewSpec() catalog.EventTriggerSpec {
	return catalog.EventTriggerSpec{
		SourceRef: "runs",
		Match:     catalog.EventTriggerMatch{CEL: `kind == "run" && has(body.outcome.pullRequest)`},
		Params:    map[string]string{"number": "body.outcome.pullRequest.number", "repository": "body.params.repository"},
		Target:    catalog.EventTriggerTarget{AgentRef: "claude-code-swe-agent", ReadOnly: true, PromptTemplate: "Review {{repository}}#{{number}}"},
		Reply: &catalog.EventTriggerReply{GitHub: &catalog.EventGitHubReply{
			Repository: "{{repository}}", IssueNumber: "{{number}}", AddLabels: []string{"factory:reviewing"},
		}},
		OnComplete: &catalog.EventTriggerOnComplete{EmitTo: "runs"},
	}
}

func factoryDone() *catalog.EventTriggerDescriptor {
	return trigger("factory-done", catalog.EventTriggerSpec{
		SourceRef: "runs",
		Params:    map[string]string{"number": "body.outcome.pullRequest.number", "repository": "body.params.repository", "assignee": "sender.login"},
		Target:    catalog.EventTriggerTarget{None: &struct{}{}},
		Reply: &catalog.EventTriggerReply{GitHub: &catalog.EventGitHubReply{
			Repository: "{{repository}}", IssueNumber: "{{number}}", AddLabels: []string{"factory:ready"}, Assignees: []string{"{{assignee}}"},
		}},
	})
}

func postEvent(t *testing.T, base string, event map[string]any, headers map[string]string) *http.Response {
	return postInvoke(t, base, map[string]any{"event": event}, headers)
}

func TestRunEvent202CarriesStringParamsOnCompleteAndRenderedAddLabels(t *testing.T) {
	base := serve(t, &Server{Engine: newFakeEngine(), Triggers: triggerRegistry(t, trigger("factory-review", factoryReviewSpec()))})
	res := postEvent(t, base, runEnvelope(nil), nil)
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	require.Equal(t, map[string]any{
		"name":       "factory-review",
		"sessionId":  "event:runs:run-evt-1",
		"reply":      map[string]any{"github": map[string]any{"repository": "acme/widgets", "issueNumber": float64(12), "addLabels": []any{"factory:reviewing"}}},
		"params":     map[string]any{"number": "12", "repository": "acme/widgets"},
		"onComplete": map[string]any{"emitTo": "runs"},
	}, decode[map[string]any](t, res)["trigger"])
}

func TestRunEvent202OmitsOnCompleteWhenTheTriggerDoesNotSetIt(t *testing.T) {
	spec := factoryReviewSpec()
	spec.OnComplete = nil
	base := serve(t, &Server{Engine: newFakeEngine(), Triggers: triggerRegistry(t, trigger("factory-review", spec))})
	trig := decode[map[string]any](t, postEvent(t, base, runEnvelope(nil), nil))["trigger"].(map[string]any)
	require.NotContains(t, trig, "onComplete")
	require.Equal(t, map[string]any{"number": "12", "repository": "acme/widgets"}, trig["params"])
}

// A trigger with no params still reports `params: {}` (never null).
func TestRunEvent202ParamsIsAnEmptyObjectWithoutParams(t *testing.T) {
	base := serve(t, &Server{Engine: newFakeEngine(), Triggers: triggerRegistry(t, catchAllTrigger())})
	trig := decode[map[string]any](t, postEvent(t, base, envelope(nil), nil))["trigger"].(map[string]any)
	require.Equal(t, map[string]any{}, trig["params"])
	require.Contains(t, trig, "reply")
	require.Nil(t, trig["reply"])
}

func TestRunEventRejectsAnUnknownKindButAcceptsRun(t *testing.T) {
	base := serve(t, &Server{Engine: newFakeEngine(), Triggers: triggerRegistry(t, trigger("factory-review", factoryReviewSpec()))})
	bad := postEvent(t, base, runEnvelope(map[string]any{"kind": "bogus"}), nil)
	require.Equal(t, http.StatusBadRequest, bad.StatusCode)
	require.Contains(t, decode[map[string]any](t, bad)["error"], `"run"`)
	require.Equal(t, http.StatusAccepted, postEvent(t, base, runEnvelope(nil), nil).StatusCode)
}

func TestTargetNoneAcceptsAndRunsNothingWithAnAlreadySucceededRecord(t *testing.T) {
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, factoryDone())})
	res := postEvent(t, base, runEnvelope(nil), nil)
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	body := decode[map[string]any](t, res)
	require.Equal(t, "pending", body["status"])
	id := body["id"].(string)
	require.Equal(t, "/invoke/"+id, res.Header.Get("location"))
	require.Equal(t, map[string]any{
		"name":      "factory-done",
		"sessionId": "event:runs:run-evt-1",
		"reply": map[string]any{"github": map[string]any{
			"repository": "acme/widgets", "issueNumber": float64(12), "addLabels": []any{"factory:ready"}, "assignees": []any{"alice"},
		}},
		"params": map[string]any{"assignee": "alice", "number": "12", "repository": "acme/widgets"},
	}, body["trigger"])

	// Immediately terminal: no wait, no engine turn.
	require.Equal(t, map[string]any{"id": id, "status": "succeeded", "result": ""}, getInvocation(t, base, id))
	stillFalse(t, func() bool { return len(engine.invoked()) > 0 })
}

func TestTargetNoneNeverRunsTheIdentityPreflight(t *testing.T) {
	engine := newFakeEngine()
	engine.checkFn = func(TurnInput) *IdentityRefusal { return identityRefusal() }
	base := serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, factoryDone()), SenderAssertionSecret: assertionSecret})
	res := postEvent(t, base, runEnvelope(nil), map[string]string{
		rbac.SenderAssertionHeader: rbac.MintSenderClaims(assertionSecret, rbac.SenderClaims{Login: "alice", AccountID: "1", SubjectStatus: "unlinked"}, time.Minute, time.Now()),
	})
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	require.Empty(t, engine.checked())
	stillFalse(t, func() bool { return len(engine.invoked()) > 0 })
}

func TestTargetNoneIsRefusedWhileDraining(t *testing.T) {
	s := &Server{Engine: newFakeEngine(), Triggers: triggerRegistry(t, factoryDone())}
	base := serve(t, s)
	s.BeginDraining()
	res := postEvent(t, base, runEnvelope(nil), nil)
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	require.Equal(t, "1", res.Header.Get("retry-after"))
}

// ── GET /invoke/:id outcome (docs/adr/0052 §2) ─────────────────────────────

var runOutcome = json.RawMessage(`{"pullRequest":{"number":12,"url":"https://github.com/acme/widgets/pull/12"},"madeNewCommits":true}`)

func runTurn(t *testing.T, d *catalog.EventTriggerDescriptor, outcome TurnOutcome) map[string]any {
	t.Helper()
	base := serve(t, &Server{Engine: engineReturning(outcome), Triggers: triggerRegistry(t, d), Invocations: &MemoryInvocationStore{}})
	return settled(t, base, acceptedID(t, postEvent(t, base, runEnvelope(nil), nil)))
}

func reviewed(outcome json.RawMessage) TurnOutcome {
	o := resultOutcome("Reviewed.")
	o.RunOutcome = outcome
	return o
}

func TestRunOutcomeSurfacesForAForcedAgentRun(t *testing.T) {
	record := runTurn(t, trigger("factory-review", factoryReviewSpec()), reviewed(runOutcome))
	require.Equal(t, "succeeded", record["status"])
	require.Equal(t, "Reviewed.", record["result"])
	var want map[string]any
	require.NoError(t, json.Unmarshal(runOutcome, &want))
	require.Equal(t, want, record["outcome"])
}

func TestRunOutcomeIsOmitted(t *testing.T) {
	skill := trigger("skill", catalog.EventTriggerSpec{Target: catalog.EventTriggerTarget{SkillRef: "s", PromptTemplate: "p"}})
	cases := map[string]struct {
		d       *catalog.EventTriggerDescriptor
		outcome TurnOutcome
		status  string
	}{
		"when the run reported none":       {trigger("factory-review", factoryReviewSpec()), reviewed(nil), "succeeded"},
		"on a failed turn":                 {trigger("factory-review", factoryReviewSpec()), TurnOutcome{Error: "boom", RunOutcome: runOutcome}, "failed"},
		"when the trigger forced no agent": {skill, TurnOutcome{RunOutcome: runOutcome}, "succeeded"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			record := runTurn(t, tc.d, tc.outcome)
			require.Equal(t, tc.status, record["status"])
			require.NotContains(t, record, "outcome")
		})
	}
}

func TestRunOutcomeThatIsNotAnAcceptableObjectIsDroppedAndLogged(t *testing.T) {
	cases := map[string]json.RawMessage{
		"an array":  json.RawMessage(`[1,2]`),
		"a string":  json.RawMessage(`"pr 12"`),
		"oversized": json.RawMessage(`{"blob":"` + strings.Repeat("x", MaxOutcomeBytes) + `"}`),
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			logs := captureLog(t)
			record := runTurn(t, trigger("factory-review", factoryReviewSpec()), reviewed(bad))
			require.Equal(t, "succeeded", record["status"])
			require.Equal(t, "Reviewed.", record["result"])
			require.NotContains(t, record, "outcome")
			require.Contains(t, logs(), "dropping run outcome")
		})
	}
	// TS logged a null outcome too; Go treats a JSON null as "no outcome"
	// (the engine's omitempty field), dropped without a log line.
	t.Run("null", func(t *testing.T) {
		record := runTurn(t, trigger("factory-review", factoryReviewSpec()), reviewed(json.RawMessage(`null`)))
		require.Equal(t, "succeeded", record["status"])
		require.NotContains(t, record, "outcome")
	})
}

// ── CheckOutcome ───────────────────────────────────────────────────────────

func TestCheckOutcomeAcceptsExactlyTheByteLimit(t *testing.T) {
	// {"k":"<pad>"} is 8 bytes of framing.
	at := `{"k":"` + strings.Repeat("x", MaxOutcomeBytes-8) + `"}`
	require.Len(t, at, MaxOutcomeBytes)
	got, err := CheckOutcome(json.RawMessage(at))
	require.NoError(t, err)
	require.Equal(t, map[string]any{"k": strings.Repeat("x", MaxOutcomeBytes-8)}, got)

	_, err = CheckOutcome(json.RawMessage(`{"k":"` + strings.Repeat("x", MaxOutcomeBytes-7) + `"}`))
	require.ErrorContains(t, err, "over the 8192-byte limit")
}

// Measured compactly: insignificant whitespace in the raw bytes doesn't count.
func TestCheckOutcomeMeasuresCompactJSON(t *testing.T) {
	padded := `{  "k" :  "` + strings.Repeat("x", MaxOutcomeBytes-8) + `"  }`
	_, err := CheckOutcome(json.RawMessage(padded))
	require.NoError(t, err)
}

func TestCheckOutcomeMeasuresUTF8Bytes(t *testing.T) {
	// "é" is one UTF-16 unit but two bytes: under the limit in .length, over it in bytes.
	outcome := `{"k":"` + strings.Repeat("é", (MaxOutcomeBytes-8)/2+1) + `"}`
	_, err := CheckOutcome(json.RawMessage(outcome))
	require.Error(t, err)
}

func TestCheckOutcomeRefusesNonObjects(t *testing.T) {
	for raw, kind := range map[string]string{`null`: "null", `1`: "number", `"s"`: "string", `true`: "boolean", `[]`: "array"} {
		_, err := CheckOutcome(json.RawMessage(raw))
		require.ErrorContains(t, err, "got "+kind, raw)
	}
	_, err := CheckOutcome(json.RawMessage(`{nope`))
	require.ErrorContains(t, err, "not serializable")
	_, err = CheckOutcome(nil)
	require.Error(t, err)
}

// ── adoption ───────────────────────────────────────────────────────────────

// hangingReporter reports its engine id then never settles: the process
// "dies" mid-turn.
func hangingReporter(t *testing.T) *fakeEngine {
	hang := hangingInvoke(t)
	return &fakeEngine{invokeFn: func(ctx context.Context, in TurnInput) (TurnOutcome, error) {
		if err := in.ReportEngineInvocation("engine-1"); err != nil {
			return TurnOutcome{}, err
		}
		return hang(ctx, in)
	}}
}

func adoptionServer(t *testing.T, engine Engine, store InvocationStore) string {
	return serve(t, &Server{Engine: engine, Triggers: triggerRegistry(t, trigger("factory-review", factoryReviewSpec())), Invocations: store})
}

func acceptOn(t *testing.T, base string, store InvocationStore) string {
	t.Helper()
	id := acceptedID(t, postEvent(t, base, runEnvelope(nil), nil))
	waitFor(t, func() bool {
		r := store.Get(context.Background(), id)
		return r != nil && r.EngineInvocationID != ""
	})
	r := store.Get(context.Background(), id)
	require.Equal(t, "engine-1", r.EngineInvocationID)
	require.True(t, r.ForcedAgent)
	return id
}

func TestAdoptionANewProcessFinishesTheTurnFromTheEngineID(t *testing.T) {
	store := &MemoryInvocationStore{}
	dead := adoptionServer(t, hangingReporter(t), store)
	id := acceptOn(t, dead, store)

	replacement := hangingReporter(t)
	replacement.adoptFn = func(_ context.Context, engineID string, _ TurnInput) (TurnOutcome, error) {
		o := resultOutcome("Reviewed.")
		o.RunOutcome = json.RawMessage(`{"verdict":"changes-requested"}`)
		return o, nil
	}
	base := adoptionServer(t, replacement, store)
	record := settled(t, base, id)

	require.Equal(t, []string{"engine-1"}, replacement.adopted())
	require.Equal(t, map[string]any{"id": id, "status": "succeeded", "result": "Reviewed.", "outcome": map[string]any{"verdict": "changes-requested"}}, record)
}

func TestAdoptionNeverAdoptsATurnThisProcessIsStillRunning(t *testing.T) {
	store := &MemoryInvocationStore{}
	engine := hangingReporter(t)
	engine.adoptFn = func(context.Context, string, TurnInput) (TurnOutcome, error) { return resultOutcome("x"), nil }
	base := adoptionServer(t, engine, store)
	id := acceptOn(t, base, store)

	// And the bookkeeping is hidden from the caller.
	require.Equal(t, map[string]any{"id": id, "status": "pending"}, getInvocation(t, base, id))
	stillFalse(t, func() bool { return len(engine.adopted()) > 0 })
}

// A pending record without an engine id has nothing to adopt by.
func TestAdoptionSkipsARecordWithoutAnEngineID(t *testing.T) {
	store := &MemoryInvocationStore{}
	require.NoError(t, store.Set(context.Background(), InvocationRecord{ID: "orphan", Status: "pending"}))
	engine := newFakeEngine()
	base := serve(t, &Server{Engine: engine, Invocations: store})
	require.Equal(t, "pending", getInvocation(t, base, "orphan")["status"])
	stillFalse(t, func() bool { return len(engine.adopted()) > 0 })
}

// A draining process does not start adopting turns it could not finish.
func TestAdoptionDoesNotStartWhileDraining(t *testing.T) {
	store := &MemoryInvocationStore{}
	require.NoError(t, store.Set(context.Background(), InvocationRecord{ID: "orphan", Status: "pending", EngineInvocationID: "engine-9"}))
	engine := newFakeEngine()
	s := &Server{Engine: engine, Invocations: store}
	base := serve(t, s)
	s.BeginDraining()
	require.Equal(t, map[string]any{"id": "orphan", "status": "pending"}, getInvocation(t, base, "orphan"))
	stillFalse(t, func() bool { return len(engine.adopted()) > 0 })
}

func TestAdoptionRecordsAnAdoptedTurnsEngineFailure(t *testing.T) {
	store := &MemoryInvocationStore{}
	dead := adoptionServer(t, hangingReporter(t), store)
	id := acceptOn(t, dead, store)

	replacement := hangingReporter(t)
	replacement.adoptFn = func(context.Context, string, TurnInput) (TurnOutcome, error) {
		return TurnOutcome{}, errors.New("engine 404")
	}
	record := settled(t, adoptionServer(t, replacement, store), id)
	require.Equal(t, "failed", record["status"])
	require.Equal(t, "engine 404", record["error"])
}
