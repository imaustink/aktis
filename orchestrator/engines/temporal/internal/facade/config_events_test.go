package facade

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
)

// config.test.ts, plus the chart contract's defaults.

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestConfigKeepsAGENT_ENGINEAsSet(t *testing.T) {
	require.Equal(t, "langgraph", ConfigFromEnv(env(map[string]string{"AGENT_ENGINE": "langgraph"})).RequestedEngine)
	require.Equal(t, "temporal", ConfigFromEnv(env(map[string]string{"AGENT_ENGINE": "temporal"})).RequestedEngine)
}

func TestConfigTreatsEmptyEngineVariablesAsUnset(t *testing.T) {
	c := ConfigFromEnv(env(map[string]string{"AGENT_ENGINE": "", "AGENT_TEMPORAL_ENGINE_URL": ""}))
	require.Empty(t, c.RequestedEngine)
	require.Empty(t, c.TemporalEngineURL)
}

func TestConfigDefaults(t *testing.T) {
	c := ConfigFromEnv(env(nil))
	require.Equal(t, Config{
		Namespace: "default", CRDGroup: "core.aktis.ai", CRDVersion: "v1alpha1",
		QdrantURL: "http://localhost:6333", QdrantGRPCPort: 6334, QdrantVectorSize: 1536,
		CallerToolsCollection: "caller_tools", CallerToolTopK: 5,
		CallerToolTTL: 7 * 24 * time.Hour, CallerToolPruneInterval: time.Hour,
		EmbeddingModel: "text-embedding-3-small", OpenAIBaseURL: "https://api.openai.com/v1",
		TurnTimeout: 8 * time.Hour, ShutdownDrain: 25 * time.Second,
		SessionTTL: 30 * time.Minute, InvocationTTL: time.Hour, SessionMaxEntries: 1000, HTTPPort: 8081,
		DefaultIdentityLinkFlow: "authcode", OpenWebUIUserRoles: []string{"reader", "writer"},
		IdentityResolverKind: "static", OIDCRolesClaim: "roles",
	}, c)
}

func TestConfigParsesOverrides(t *testing.T) {
	c := ConfigFromEnv(env(map[string]string{
		"OPENAI_BASE_URL":                  "https://proxy.example/v1///",
		"AGENT_RUN_TIMEOUT_SECONDS":        "1.5",
		"AGENT_SHUTDOWN_DRAIN_MS":          "100",
		"AGENT_HTTP_PORT":                  " 9000 ",
		"AGENT_OPENWEBUI_USER_ROLES":       " reader , ,admin ",
		"AGENT_DEFAULT_IDENTITY_LINK_FLOW": "device",
		"AGENT_IDENTITY_RESOLVER":          "oidc",
		"AGENT_CALLER_AUTH":                "warn",
	}))
	require.Equal(t, "https://proxy.example/v1", c.OpenAIBaseURL)
	require.Equal(t, 1500*time.Millisecond, c.TurnTimeout)
	require.Equal(t, 100*time.Millisecond, c.ShutdownDrain)
	require.Equal(t, 9000, c.HTTPPort)
	require.Equal(t, []string{"reader", "admin"}, c.OpenWebUIUserRoles)
	require.Equal(t, "device", c.DefaultIdentityLinkFlow)
	require.Equal(t, "oidc", c.IdentityResolverKind)
	require.Equal(t, "warn", c.CallerAuthMode)
}

// A missing, unparsable or non-positive number keeps the default; a typo'd
// flow or resolver kind keeps the safe default.
func TestConfigFallsBackOnBadValues(t *testing.T) {
	for _, raw := range []string{"abc", "0", "-5"} {
		c := ConfigFromEnv(env(map[string]string{"AGENT_HTTP_PORT": raw, "AGENT_SESSION_TTL_SECONDS": raw}))
		require.Equal(t, 8081, c.HTTPPort, raw)
		require.Equal(t, 30*time.Minute, c.SessionTTL, raw)
	}
	c := ConfigFromEnv(env(map[string]string{"AGENT_DEFAULT_IDENTITY_LINK_FLOW": "Device", "AGENT_IDENTITY_RESOLVER": "OIDC"}))
	require.Equal(t, "authcode", c.DefaultIdentityLinkFlow)
	require.Equal(t, "static", c.IdentityResolverKind)
}

// ── ParseEnvelope (routing/crd-event-trigger-registry.test.ts) ─────────────

func TestParseEnvelopeAcceptsAWellFormedEnvelopeAndDefaultsOptionalFields(t *testing.T) {
	got, err := ParseEnvelope(map[string]any{"source": "nightly", "kind": "cron", "id": "nightly-1", "time": "2026-10-13T07:00:00Z"})
	require.NoError(t, err)
	require.Equal(t, catalog.Envelope{
		Source: "nightly", Kind: "cron", ID: "nightly-1", Time: "2026-10-13T07:00:00Z",
		Headers: map[string]string{}, Body: map[string]any{}, Sender: map[string]any{}, Repository: "",
	}, got)
}

func TestParseEnvelopeKeepsEveryField(t *testing.T) {
	got, err := ParseEnvelope(map[string]any{
		"source": "github", "kind": "webhook", "id": "1", "time": "t",
		"headers": map[string]any{"x-github-event": "issues"}, "body": []any{1.0},
		"sender": map[string]any{"login": "a"}, "repository": "acme/widgets",
	})
	require.NoError(t, err)
	require.Equal(t, catalog.Envelope{
		Source: "github", Kind: "webhook", ID: "1", Time: "t",
		Headers: map[string]string{"x-github-event": "issues"}, Body: []any{1.0},
		Sender: map[string]any{"login": "a"}, Repository: "acme/widgets",
	}, got)
	// An explicit null body is kept as null (any JSON is a body).
	got, err = ParseEnvelope(map[string]any{"source": "s", "kind": "run", "id": "1", "time": "t", "body": nil})
	require.NoError(t, err)
	require.Nil(t, got.Body)
}

func TestParseEnvelopeRejects(t *testing.T) {
	base := func(extra map[string]any) map[string]any {
		m := map[string]any{"source": "github", "kind": "webhook", "id": "1", "time": "t"}
		for k, v := range extra {
			if v == nil {
				delete(m, k)
			} else {
				m[k] = v
			}
		}
		return m
	}
	cases := map[string]struct {
		raw  any
		want string
	}{
		"not an object":         {"github", "event must be an object"},
		"no kind":               {base(map[string]any{"kind": nil}), "event.kind"},
		"unknown kind":          {base(map[string]any{"kind": "push"}), `event.kind must be "webhook", "cron", "run" or "conflict"`},
		"no source":             {base(map[string]any{"source": nil}), "event.source"},
		"empty source":          {base(map[string]any{"source": ""}), "event.source"},
		"no id":                 {base(map[string]any{"id": nil}), "event.id"},
		"no time":               {base(map[string]any{"time": nil}), "event.time"},
		"non-string header":     {base(map[string]any{"headers": map[string]any{"a": 1.0}}), `event.headers["a"] must be a string`},
		"non-object headers":    {base(map[string]any{"headers": "x"}), "event.headers must be an object"},
		"non-object sender":     {base(map[string]any{"sender": "x"}), "event.sender must be an object"},
		"non-string repository": {base(map[string]any{"repository": 3.0}), "event.repository must be a string"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseEnvelope(tc.raw)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
