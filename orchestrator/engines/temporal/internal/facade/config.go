// Package facade is agent-orchestrator: the HTTP front door in front of the
// Temporal engine's gateway (docs/adr/0006, 0007, 0036, 0053).
//
// It serves, on AGENT_HTTP_PORT:
//
//   - POST /invoke, GET /invoke/:id — accept-then-poll turns, including
//     EventTrigger envelopes (ADR 0048), matched here and forwarded resolved.
//   - GET /v1/models, POST /v1/chat/completions — the OpenAI-compatible facade
//     Open WebUI talks to.
//   - POST /events/prefilter — the CEL integration-gateway needs before a
//     webhook's sender is resolved (docs/event-triggers.md §8).
//   - GET /sessions/live — always {live:false} (see handleSessionLive).
//
// Every turn runs on the engine. What stays here is request parsing, trigger
// selection, consumer-supplied tools (ADR 0035), the sticky read-only session
// flag, and the per-user identity this process verifies and signs across the
// hop.
package facade

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is everything agent-orchestrator reads from its environment. The
// variable names and defaults are the chart's contract
// (orchestrator/charts/aktis/charts/agent-orchestrator) and must not drift.
type Config struct {
	// Namespace the EventTrigger CRs are read from (ADR 0048).
	Namespace  string
	CRDGroup   string
	CRDVersion string

	QdrantURL        string
	QdrantAPIKey     string
	QdrantGRPCPort   int
	QdrantVectorSize int
	// CallerToolsCollection is the Qdrant collection for consumer-supplied
	// tools (ADR 0035) — its own, so a caller's ephemeral definitions never
	// mix with anything else.
	CallerToolsCollection string
	// CallerToolTopK is the max caller tools forwarded to the engine, and the
	// threshold below which the index is skipped entirely.
	CallerToolTopK          int
	CallerToolTTL           time.Duration
	CallerToolPruneInterval time.Duration
	EmbeddingModel          string
	OpenAIBaseURL           string
	OpenAIAPIKey            string

	// TemporalEngineURL is the engine gateway's base URL. Required.
	TemporalEngineURL   string
	TemporalEngineToken string
	// RequestedEngine is AGENT_ENGINE as set, kept only so startup can refuse
	// a deployment still asking for the removed LangGraph engine.
	RequestedEngine string

	// TurnTimeout bounds one turn's poll of the engine before the caller gets
	// a resumable "still running" pause instead of an answer.
	TurnTimeout time.Duration
	// ShutdownDrain is how long in-flight requests get on SIGTERM. Must stay
	// under the pod's terminationGracePeriodSeconds.
	ShutdownDrain time.Duration

	SessionTTL        time.Duration
	InvocationTTL     time.Duration
	SessionMaxEntries int
	RedisURL          string
	HTTPPort          int

	// SenderAssertionSecret is shared with integration-gateway (which signs
	// sender assertions with it) and the engine gateway (which verifies what
	// this process signs).
	SenderAssertionSecret string

	IdentityLinkGatewayURL   string
	IdentityLinkGatewayToken string
	// DefaultIdentityLinkFlow is "device" or "authcode".
	DefaultIdentityLinkFlow string
	ConnectionsURL          string

	OpenWebUIUserJWTSecret string
	OpenWebUIUserRoles     []string

	// IdentityResolverKind is "static" or "oidc".
	IdentityResolverKind string
	StaticIdentities     string
	OIDCIssuer           string
	OIDCJWKSURI          string
	OIDCAudience         string
	OIDCRolesClaim       string
	// CallerAuthMode is AGENT_CALLER_AUTH raw; validated at startup.
	CallerAuthMode string
}

// positive mirrors the TS config's num(): a missing, unparsable or
// non-positive value falls back to the default rather than failing.
func positive(raw string, fallback float64) float64 {
	if raw == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

// ConfigFromEnv reads Config with the same defaults the TS process used.
func ConfigFromEnv(getenv func(string) string) Config {
	if getenv == nil {
		getenv = os.Getenv
	}
	orDefault := func(key, fallback string) string {
		if v, ok := lookup(getenv, key); ok {
			return v
		}
		return fallback
	}
	num := func(key string, fallback float64) float64 { return positive(getenv(key), fallback) }
	seconds := func(key string, fallback float64) time.Duration {
		return time.Duration(num(key, fallback) * float64(time.Second))
	}

	roles := []string{}
	for _, role := range strings.Split(orDefault("AGENT_OPENWEBUI_USER_ROLES", "reader,writer"), ",") {
		if role = strings.TrimSpace(role); role != "" {
			roles = append(roles, role)
		}
	}

	// Anything other than an exact "device" keeps authcode: a typo must not
	// silently change which flow every chat user gets.
	flow := "authcode"
	if getenv("AGENT_DEFAULT_IDENTITY_LINK_FLOW") == "device" {
		flow = "device"
	}
	resolverKind := "static"
	if getenv("AGENT_IDENTITY_RESOLVER") == "oidc" {
		resolverKind = "oidc"
	}

	return Config{
		Namespace:               orDefault("AGENT_NAMESPACE", "default"),
		CRDGroup:                orDefault("AGENT_CRD_GROUP", "core.aktis.ai"),
		CRDVersion:              orDefault("AGENT_CRD_VERSION", "v1alpha1"),
		QdrantURL:               orDefault("AGENT_QDRANT_URL", "http://localhost:6333"),
		QdrantAPIKey:            getenv("AGENT_QDRANT_API_KEY"),
		QdrantGRPCPort:          int(num("AGENT_QDRANT_GRPC_PORT", 6334)),
		QdrantVectorSize:        int(num("AGENT_QDRANT_VECTOR_SIZE", 1536)),
		CallerToolsCollection:   orDefault("AGENT_QDRANT_CALLER_TOOLS_COLLECTION", "caller_tools"),
		CallerToolTopK:          int(num("AGENT_CALLER_TOOL_TOP_K", 5)),
		CallerToolTTL:           seconds("AGENT_CALLER_TOOL_TTL_SECONDS", 604_800), // 7 days
		CallerToolPruneInterval: seconds("AGENT_CALLER_TOOL_PRUNE_INTERVAL_SECONDS", 3_600),
		EmbeddingModel:          orDefault("AGENT_EMBEDDING_MODEL", "text-embedding-3-small"),
		OpenAIBaseURL:           strings.TrimRight(orDefault("OPENAI_BASE_URL", "https://api.openai.com/v1"), "/"),
		OpenAIAPIKey:            getenv("OPENAI_API_KEY"),
		TemporalEngineURL:       getenv("AGENT_TEMPORAL_ENGINE_URL"),
		TemporalEngineToken:     getenv("AGENT_TEMPORAL_ENGINE_TOKEN"),
		RequestedEngine:         getenv("AGENT_ENGINE"),
		// Same env var as before 0053, when it also bounded the in-process
		// engine's own wait on an agent run.
		TurnTimeout:              seconds("AGENT_RUN_TIMEOUT_SECONDS", 28_800),
		ShutdownDrain:            time.Duration(num("AGENT_SHUTDOWN_DRAIN_MS", 25_000)) * time.Millisecond,
		SessionTTL:               seconds("AGENT_SESSION_TTL_SECONDS", 1_800),
		InvocationTTL:            seconds("AGENT_INVOCATION_TTL_SECONDS", 3_600),
		SessionMaxEntries:        int(num("AGENT_SESSION_MAX_ENTRIES", 1_000)),
		RedisURL:                 getenv("AGENT_REDIS_URL"),
		HTTPPort:                 int(num("AGENT_HTTP_PORT", 8081)),
		SenderAssertionSecret:    getenv("AGENT_SENDER_ASSERTION_SECRET"),
		IdentityLinkGatewayURL:   getenv("IDENTITY_LINK_GATEWAY_URL"),
		IdentityLinkGatewayToken: getenv("IDENTITY_LINK_GATEWAY_TOKEN"),
		DefaultIdentityLinkFlow:  flow,
		ConnectionsURL:           getenv("AGENT_CONNECTIONS_URL"),
		OpenWebUIUserJWTSecret:   getenv("AGENT_OPENWEBUI_USER_JWT_SECRET"),
		OpenWebUIUserRoles:       roles,
		IdentityResolverKind:     resolverKind,
		StaticIdentities:         getenv("AGENT_STATIC_IDENTITIES"),
		OIDCIssuer:               getenv("AGENT_OIDC_ISSUER"),
		OIDCJWKSURI:              getenv("AGENT_OIDC_JWKS_URI"),
		OIDCAudience:             getenv("AGENT_OIDC_AUDIENCE"),
		OIDCRolesClaim:           orDefault("AGENT_OIDC_ROLES_CLAIM", "roles"),
		CallerAuthMode:           getenv("AGENT_CALLER_AUTH"),
	}
}

// lookup distinguishes unset from empty for the few variables whose TS
// default applied only when unset (`??`, not `||`). A getenv that cannot
// tell the two apart (tests) treats "" as unset.
func lookup(getenv func(string) string, key string) (string, bool) {
	if v := getenv(key); v != "" {
		return v, true
	}
	return "", false
}
