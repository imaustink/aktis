package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Ported from integration-gateway's readsResolvedFields cases
// (apps/integration-gateway/src/event-triggers.test.ts). The Go version reads
// CEL's checked reference map rather than scanning the source, so it answers
// for the VARIABLE `sender`/`repository`, never a field or a string that
// happens to share the name.
func TestCompiledExprReadsResolved(t *testing.T) {
	cases := []struct {
		expr string
		want bool
	}{
		{`sender.login == 'alice'`, true},
		{`sender.login == "x"`, true},
		{`repository == 'acme/widgets'`, true},
		{`has(sender.isBot) && body.x`, true},
		{`body . x || repository.startsWith('acme/')`, true},
		{`body.repository.full_name == 'acme/widgets'`, false},
		{`body.repository == "x"`, false},
		{`body . sender . login == 'x'`, false},
		{`body.label.name == "sender" || headers["repository"] == 'repository'`, false},
		{`body.x == """sender"""`, false},
		{"body.x == r'sender' // sender in a comment", false},
		// TS's case used an undeclared `senders` variable, which the Go
		// compiler rejects outright; the field-select form keeps its intent
		// (a near-miss identifier is not the variable).
		{`body.senders.size() > 0 || body.repositoryName == ''`, false},
		// A comprehension variable that shadows `sender` lands in the
		// reference map too. Erring toward true is the safe side: a false
		// positive only costs a sender lookup, never a dropped delivery.
		{`body.items.exists(sender, sender == 1)`, true},
	}
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			compiled, err := CompileEventExpr(tc.expr)
			require.NoError(t, err)
			require.Equal(t, tc.want, compiled.readsResolved)
		})
	}
}

func prelimEnvelope(body map[string]any, headers map[string]string) Envelope {
	if headers == nil {
		headers = map[string]string{}
	}
	return Envelope{Source: "github", Kind: "webhook", ID: "d1", Time: "2026-01-01T00:00:00Z",
		Headers: headers, Body: body, Sender: map[string]any{}, Repository: ""}
}

func prefilterTrigger(t *testing.T, name string, spec EventTriggerSpec, ready bool) *EventTriggerDescriptor {
	t.Helper()
	if spec.Target == (EventTriggerTarget{}) {
		spec.Target = EventTriggerTarget{AgentRef: "swe", PromptTemplate: "go"}
	}
	return NewEventTriggerDescriptor(name, spec, ready)
}

func TestCouldMatchBeforeSender(t *testing.T) {
	labeled := func(t *testing.T) *EventTriggerDescriptor {
		d := prefilterTrigger(t, "review", EventTriggerSpec{
			SourceRef: "github",
			Match:     EventTriggerMatch{CEL: `headers["x-github-event"] == "pull_request" && body.action == "labeled"`},
		}, true)
		require.NoError(t, d.Invalid())
		return d
	}
	pr := map[string]string{"x-github-event": "pull_request"}

	t.Run("false when every trigger for the source evaluates false", func(t *testing.T) {
		require.False(t, CouldMatchBeforeSender([]*EventTriggerDescriptor{labeled(t)}, prelimEnvelope(map[string]any{"action": "opened"}, pr)))
	})
	t.Run("true when one evaluates true", func(t *testing.T) {
		require.True(t, CouldMatchBeforeSender([]*EventTriggerDescriptor{labeled(t)}, prelimEnvelope(map[string]any{"action": "labeled"}, pr)))
	})
	t.Run("true for a trigger with no match, or no sourceRef", func(t *testing.T) {
		any1 := prefilterTrigger(t, "any", EventTriggerSpec{SourceRef: "github"}, true)
		any2 := prefilterTrigger(t, "any-source", EventTriggerSpec{}, true)
		require.True(t, CouldMatchBeforeSender([]*EventTriggerDescriptor{any1}, prelimEnvelope(map[string]any{}, nil)))
		require.True(t, CouldMatchBeforeSender([]*EventTriggerDescriptor{any2}, prelimEnvelope(map[string]any{}, nil)))
	})
	t.Run("true for a trigger reading sender/repository, whatever the rest says", func(t *testing.T) {
		for _, cel := range []string{`sender.login == "x"`, `sender.login == "x" && body.action == "never"`, `repository == "acme/widgets"`} {
			d := prefilterTrigger(t, "s", EventTriggerSpec{SourceRef: "github", Match: EventTriggerMatch{CEL: cel}}, true)
			require.NoError(t, d.Invalid())
			require.True(t, CouldMatchBeforeSender([]*EventTriggerDescriptor{d}, prelimEnvelope(map[string]any{}, nil)), cel)
		}
	})
	t.Run("a body field named repository is evaluated, not assumed", func(t *testing.T) {
		d := prefilterTrigger(t, "b", EventTriggerSpec{SourceRef: "github", Match: EventTriggerMatch{CEL: `body.repository == "x"`}}, true)
		require.NoError(t, d.Invalid())
		require.False(t, CouldMatchBeforeSender([]*EventTriggerDescriptor{d}, prelimEnvelope(map[string]any{"repository": "y"}, nil)))
		require.True(t, CouldMatchBeforeSender([]*EventTriggerDescriptor{d}, prelimEnvelope(map[string]any{"repository": "x"}, nil)))
	})
	t.Run("ignores triggers for other sources, and ones that don't compile", func(t *testing.T) {
		slack := prefilterTrigger(t, "slack", EventTriggerSpec{SourceRef: "slack"}, true)
		broken := prefilterTrigger(t, "broken", EventTriggerSpec{SourceRef: "github", Match: EventTriggerMatch{CEL: "body.("}}, true)
		require.Error(t, broken.Invalid())
		require.False(t, CouldMatchBeforeSender([]*EventTriggerDescriptor{slack}, prelimEnvelope(map[string]any{}, nil)))
		require.False(t, CouldMatchBeforeSender([]*EventTriggerDescriptor{broken}, prelimEnvelope(map[string]any{}, nil)))
	})
	t.Run("an invalid target or a not-Ready trigger never counts", func(t *testing.T) {
		twoTargets := prefilterTrigger(t, "two", EventTriggerSpec{SourceRef: "github",
			Target: EventTriggerTarget{AgentRef: "a", SkillRef: "s", PromptTemplate: "x"}}, true)
		require.Error(t, twoTargets.Invalid())
		notReady := prefilterTrigger(t, "nr", EventTriggerSpec{SourceRef: "github"}, false)
		require.False(t, CouldMatchBeforeSender([]*EventTriggerDescriptor{twoTargets, notReady}, prelimEnvelope(map[string]any{}, nil)))
	})
	t.Run("an evaluation error is a non-match", func(t *testing.T) {
		d := prefilterTrigger(t, "e", EventTriggerSpec{SourceRef: "github", Match: EventTriggerMatch{CEL: "body.missing.deeper == 1"}}, true)
		require.NoError(t, d.Invalid())
		require.False(t, CouldMatchBeforeSender([]*EventTriggerDescriptor{d}, prelimEnvelope(map[string]any{}, nil)))
	})
	t.Run("the registry method reads the live table", func(t *testing.T) {
		r := NewEventTriggerRegistry()
		e := prelimEnvelope(map[string]any{"action": "labeled"}, pr)
		require.False(t, r.CouldMatchBeforeSender(e))
		r.Upsert(labeled(t))
		require.True(t, r.CouldMatchBeforeSender(e))
		r.Delete("review")
		require.False(t, r.CouldMatchBeforeSender(e))
	})
}
