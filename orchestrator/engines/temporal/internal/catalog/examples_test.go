package catalog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

// The example EventTriggers (examples/event-triggers) compile, and the
// software factory's routing (docs/adr/0052, docs/adr/0055) sends every run
// completion to exactly the step it should. The production copies in
// orchestrator/charts/aktis/values-production.yaml mirror these.
//
// Ported from @aktis/event-triggers' examples.test.ts: this is the one CEL
// evaluator (docs/adr/0056), so this is where the routing is proven.

func exampleTriggers(t *testing.T) []*EventTriggerDescriptor {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "..", "examples", "event-triggers")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var out []*EventTriggerDescriptor
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "eventtrigger-") || !strings.HasSuffix(name, ".yaml") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		obj := &unstructured.Unstructured{}
		require.NoError(t, yaml.Unmarshal(raw, &obj.Object), name)
		var spec EventTriggerSpec
		require.NoError(t, decodeSpec(obj, &spec), name)
		// Ready: these are the specs as written; readiness is core-controller's
		// call and not what this proves.
		out = append(out, NewEventTriggerDescriptor(obj.GetName(), spec, true))
	}
	return out
}

// routed is the winning trigger and its string params, or "ignored".
type routed struct {
	trigger string
	params  map[string]string
}

func route(t *testing.T, triggers []*EventTriggerDescriptor, envelopeJSON string) routed {
	t.Helper()
	var e Envelope
	require.NoError(t, json.Unmarshal([]byte(envelopeJSON), &e))
	m, err := MatchEventTriggers(triggers, e, "s")
	require.NoError(t, err)
	if m == nil {
		return routed{trigger: "ignored"}
	}
	return routed{trigger: m.Trigger.Name, params: m.StringParams}
}

func runEvent(t *testing.T, trigger string, outcome map[string]any, status string, counts map[string]int, params map[string]string) string {
	t.Helper()
	if status == "" {
		status = "succeeded"
	}
	if counts == nil {
		counts = map[string]int{}
	}
	if params == nil {
		params = map[string]string{"repository": "acme/widgets", "number": "7", "title": "Add widgets"}
	}
	b, err := json.Marshal(map[string]any{
		"source": "runs", "kind": "run", "id": "e", "time": "2026-11-23T00:00:00Z", "headers": map[string]any{},
		"sender": map[string]any{"login": "alice", "isBot": false}, "repository": "acme/widgets",
		"body": map[string]any{
			"trigger": trigger, "status": status, "params": params, "outcome": outcome,
			"chain": map[string]any{"id": "c", "depth": 1, "counts": counts},
		},
	})
	require.NoError(t, err)
	return string(b)
}

func TestExampleEventTriggersAllCompile(t *testing.T) {
	triggers := exampleTriggers(t)
	require.Greater(t, len(triggers), 10)
	for _, d := range triggers {
		require.NoError(t, d.Invalid(), d.Name)
	}
}

func TestFactoryCoordinationRouting(t *testing.T) {
	triggers := exampleTriggers(t)

	t.Run("ai-triage on an issue goes to factory-intake, not straight to triage", func(t *testing.T) {
		got := route(t, triggers, `{"source":"github","kind":"webhook","id":"d","time":"t","headers":{"x-github-event":"issues"},
			"sender":{"login":"alice","isBot":false},"repository":"acme/widgets",
			"body":{"action":"labeled","label":{"name":"ai-triage"},"issue":{"number":7,"title":"Add widgets","body":""},"repository":{"full_name":"acme/widgets"}}}`)
		require.Equal(t, routed{trigger: "factory-intake", params: map[string]string{"repository": "acme/widgets", "number": "7", "title": "Add widgets"}}, got)
	})

	for _, from := range []string{"factory-intake", "factory-park"} {
		t.Run("a "+from+" completion is coordinated", func(t *testing.T) {
			require.Equal(t, "factory-coordinate", route(t, triggers, runEvent(t, from, map[string]any{}, "", nil, nil)).trigger)
		})
	}

	toTriage := []struct {
		name    string
		outcome map[string]any
		counts  map[string]int
		status  string
	}{
		{"proceed", map[string]any{"coordination": map[string]any{"decision": "proceed"}}, nil, "succeeded"},
		{"no decision", map[string]any{"coordination": map[string]any{"decision": "none"}}, nil, "succeeded"},
		{"no outcome at all", map[string]any{}, nil, "succeeded"},
		{"a failed coordinator", map[string]any{"coordination": map[string]any{"decision": "wait", "number": 5}}, nil, "failed"},
		{"a wait on itself", map[string]any{"coordination": map[string]any{"decision": "wait", "number": 7}}, nil, "succeeded"},
		{"a wait past 5 waits", map[string]any{"coordination": map[string]any{"decision": "wait", "number": 5}}, map[string]int{"factory-park": 5}, "succeeded"},
		{"a stack on itself", map[string]any{"coordination": map[string]any{"decision": "stack", "number": 7}}, nil, "succeeded"},
	}
	for _, c := range toTriage {
		t.Run(c.name+" -> factory-triage", func(t *testing.T) {
			require.Equal(t, "factory-triage", route(t, triggers, runEvent(t, "factory-coordinate", c.outcome, c.status, c.counts, nil)).trigger)
		})
	}

	t.Run("wait <N> -> factory-park, carrying N as waitFor", func(t *testing.T) {
		got := route(t, triggers, runEvent(t, "factory-coordinate",
			map[string]any{"coordination": map[string]any{"decision": "wait", "number": 5}}, "", map[string]int{"factory-park": 4}, nil))
		require.Equal(t, "factory-park", got.trigger)
		require.Equal(t, "5", got.params["waitFor"])
		require.Equal(t, "7", got.params["number"])
		require.Equal(t, "acme/widgets", got.params["repository"])
	})

	t.Run("stack <N> -> factory-triage-stacked, carrying N as stackOn", func(t *testing.T) {
		got := route(t, triggers, runEvent(t, "factory-coordinate",
			map[string]any{"coordination": map[string]any{"decision": "stack", "number": 12}}, "", nil, nil))
		require.Equal(t, "factory-triage-stacked", got.trigger)
		require.Equal(t, "12", got.params["stackOn"])
		require.Equal(t, "7", got.params["closesIssue"])
	})

	for _, from := range []string{"factory-triage", "factory-triage-stacked"} {
		t.Run("a "+from+" run that opened a PR is reviewed", func(t *testing.T) {
			got := route(t, triggers, runEvent(t, from,
				map[string]any{"pullRequest": map[string]any{"number": 12, "url": "u"}, "madeNewCommits": true}, "", nil, nil))
			require.Equal(t, "factory-review", got.trigger)
			require.Equal(t, "12", got.params["number"])
		})
	}
}

func TestConflictResyncRouting(t *testing.T) {
	triggers := exampleTriggers(t)

	t.Run("a conflict event goes to factory-resync on that PR", func(t *testing.T) {
		got := route(t, triggers, `{"source":"pr-conflicts","kind":"conflict","id":"e","time":"t","headers":{},
			"sender":{"login":"alice","isBot":false},"repository":"acme/widgets",
			"body":{"pullRequest":{"number":12,"url":"u","title":"Add widgets","baseRef":"main","baseSha":"b","headRef":"f","headSha":"h"}}}`)
		require.Equal(t, routed{trigger: "factory-resync", params: map[string]string{
			"repository": "acme/widgets", "number": "12", "title": "Add widgets", "baseRef": "main",
		}}, got)
	})

	resynced := func(outcome map[string]any, status string) string {
		return runEvent(t, "factory-resync", outcome, status, nil,
			map[string]string{"repository": "acme/widgets", "number": "12", "title": "T", "baseRef": "main"})
	}

	t.Run("a resync that pushed is re-reviewed", func(t *testing.T) {
		got := route(t, triggers, resynced(map[string]any{"madeNewCommits": true}, ""))
		require.Equal(t, "factory-review", got.trigger)
		require.Equal(t, "12", got.params["number"])
	})

	for _, c := range []struct {
		name, status, why string
		outcome           map[string]any
	}{
		{"pushed nothing", "succeeded", "pushed no changes", map[string]any{"madeNewCommits": false}},
		{"failed", "failed", "did not succeed", map[string]any{}},
	} {
		t.Run("a resync that "+c.name+" is handed off", func(t *testing.T) {
			got := route(t, triggers, resynced(c.outcome, c.status))
			require.Equal(t, "factory-handoff", got.trigger)
			require.Equal(t, "12", got.params["number"])
			require.Equal(t, "alice", got.params["assignee"])
			require.Contains(t, got.params["reason"], c.why)
		})
	}
}
