// Command agent-orchestrator is the facade in front of the Temporal engine
// (docs/adr/0006, 0007, 0036, 0053): the OpenAI-compatible chat API Open
// WebUI talks to, accept-then-poll /invoke for integration-gateway and
// programmatic callers, and EventTrigger matching. See package facade.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/qdrant/go-client/qdrant"
	"github.com/redis/go-redis/v9"
	"k8s.io/client-go/dynamic"

	"github.com/aktisai/orchestrator/engines/temporal/internal/callertools"
	"github.com/aktisai/orchestrator/engines/temporal/internal/catalog"
	"github.com/aktisai/orchestrator/engines/temporal/internal/facade"
	"github.com/aktisai/orchestrator/engines/temporal/internal/kubeconfig"
	"github.com/aktisai/orchestrator/engines/temporal/internal/llm"
	"github.com/aktisai/orchestrator/engines/temporal/internal/telemetry"
)

// startupRetry covers a dependency that routinely comes up after this pod
// (~2 minutes worst case).
const (
	startupAttempts     = 12
	startupInitialDelay = time.Second
	startupMaxDelay     = 15 * time.Second
	// triggerSyncTimeout bounds the wait for the initial EventTrigger list.
	triggerSyncTimeout = 60 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Printf("agent-orchestrator: %v", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := facade.ConfigFromEnv(os.Getenv)

	// The process lives until a signal; turns run under turnsCtx, which is
	// never cancelled before exit — an in-flight turn's poll is not cut short
	// by the drain, only by the process ending.
	turnsCtx := context.Background()
	watchCtx, stopWatch := context.WithCancel(context.Background())
	defer stopWatch()

	shutdownTracing := telemetry.Setup(watchCtx, "agent-orchestrator")

	if cfg.RequestedEngine != "" && cfg.RequestedEngine != "temporal" {
		// A stale AGENT_ENGINE=langgraph is a deployment that believes it is
		// running code that no longer exists (docs/adr/0053).
		return fmt.Errorf("AGENT_ENGINE=%s is not supported: the Temporal engine is the only engine (docs/adr/0053). "+
			"Unset AGENT_ENGINE and set AGENT_TEMPORAL_ENGINE_URL", cfg.RequestedEngine)
	}
	if cfg.TemporalEngineURL == "" {
		return errors.New("AGENT_TEMPORAL_ENGINE_URL is required: every turn runs on the Temporal engine (docs/adr/0036)")
	}

	// EventTrigger table (ADR 0048): an /invoke event is matched here and the
	// RESOLVED turn, not the envelope, goes to the engine.
	triggers, err := startEventTriggerWatch(watchCtx, cfg)
	if err != nil {
		return err
	}

	// Without this, every chat turn runs as the engine gateway's one shared
	// subject, so an account linked by one Open WebUI user is usable by all.
	var openWebUI facade.IdentityResolver
	if cfg.OpenWebUIUserJWTSecret != "" {
		openWebUI = &facade.OpenWebUIForwardedUserResolver{Secret: []byte(cfg.OpenWebUIUserJWTSecret), Roles: cfg.OpenWebUIUserRoles}
	}

	// Handed the BARE JWT verifier, so a refused turn records nothing.
	callerAuth, warnings, err := facade.BuildCallerAuthenticator(cfg, openWebUI, func(uri string) (jwt.Keyfunc, error) {
		k, err := keyfunc.NewDefaultCtx(watchCtx, []string{uri})
		if err != nil {
			return nil, err
		}
		return k.Keyfunc, nil
	})
	if err != nil {
		return err
	}
	for _, warning := range warnings {
		log.Printf("WARNING: %s", warning)
	}

	var identityLink *facade.IdentityLinkGatewayClient
	if cfg.IdentityLinkGatewayURL != "" && cfg.IdentityLinkGatewayToken != "" {
		identityLink = &facade.IdentityLinkGatewayClient{BaseURL: cfg.IdentityLinkGatewayURL, Token: cfg.IdentityLinkGatewayToken}
	}
	// The Connections page (docs/adr/0046) finds a chat user's credentials
	// from an IdP sign-in by email; this records each verified caller's.
	forwardedUser := openWebUI
	if openWebUI != nil && identityLink != nil && cfg.ConnectionsURL != "" {
		forwardedUser = &facade.PrincipalRecordingResolver{Inner: openWebUI, Sink: identityLink}
	}
	if cfg.ConnectionsURL != "" && (openWebUI == nil || identityLink == nil) {
		log.Printf("AGENT_CONNECTIONS_URL is set but the Open WebUI JWT secret or identity-link gateway is not, " +
			"so no chat user can be found from the Connections page")
	}

	// Said out loud because the failure it guards against is otherwise
	// SILENT: an authcode link dies at GitHub's consent screen before any
	// request reaches this process.
	if cfg.DefaultIdentityLinkFlow == "authcode" {
		log.Printf("identity-link default flow: authcode -- requires the GitHub App's registered Callback URL to exactly match " +
			"integration-gateway's GITHUB_OAUTH_REDIRECT_URI; set AGENT_DEFAULT_IDENTITY_LINK_FLOW=device to use the redirect-free device flow instead")
	} else {
		log.Printf("identity-link default flow: %s -- user-code flow, no redirect URI involved", cfg.DefaultIdentityLinkFlow)
	}

	// Session (sticky read-only flag) and /invoke record stores. On Redis,
	// any replica can answer any poll, so a rollout no longer loses the turns
	// accepted around it.
	var sessions facade.SessionStore
	var invocations facade.InvocationStore
	var redisClient *redis.Client
	if cfg.RedisURL != "" {
		opts, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			return fmt.Errorf("AGENT_REDIS_URL: %w", err)
		}
		redisClient = redis.NewClient(opts)
		if err := retry("redis startup check", func() error { return redisClient.Ping(watchCtx).Err() }); err != nil {
			return err
		}
		log.Printf("Using Redis session + invocation stores (invocation ttl %s)", cfg.InvocationTTL)
		sessions = &facade.RedisSessionStore{Client: redisClient, TTL: cfg.SessionTTL}
		invocations = &facade.RedisInvocationStore{Client: redisClient, TTL: cfg.InvocationTTL}
	} else {
		log.Printf("No AGENT_REDIS_URL: /invoke records are in-process, so a poll must reach the SAME pod that accepted it. " +
			"A rollout loses turns accepted around it, and more than one replica will 404 polls.")
		sessions = &facade.MemorySessionStore{TTL: cfg.SessionTTL, MaxEntries: cfg.SessionMaxEntries}
		invocations = &facade.MemoryInvocationStore{}
	}

	// Just-in-time index for consumer-supplied tools (ADR 0035), in its own
	// collection. Retried: after a full-cluster restart this pod routinely
	// comes up before Qdrant.
	callerTools, err := startCallerToolStore(watchCtx, cfg)
	if err != nil {
		return err
	}

	engine := &facade.TemporalEngine{
		BaseURL:               cfg.TemporalEngineURL,
		Token:                 cfg.TemporalEngineToken,
		SenderAssertionSecret: cfg.SenderAssertionSecret,
		ForwardedUserResolver: forwardedUser,
		Timeout:               cfg.TurnTimeout,
	}
	log.Printf("agent engine: temporal (%s)", cfg.TemporalEngineURL)

	server := &facade.Server{
		Engine:                  engine,
		Sessions:                sessions,
		TaskCompleter:           &facade.OpenAITaskCompleter{BaseURL: cfg.OpenAIBaseURL, APIKey: cfg.OpenAIAPIKey},
		Triggers:                triggers,
		SenderAssertionSecret:   cfg.SenderAssertionSecret,
		CallerTools:             callerTools,
		CallerToolTopK:          cfg.CallerToolTopK,
		DefaultIdentityLinkFlow: cfg.DefaultIdentityLinkFlow,
		Invocations:             invocations,
		CallerAuth:              callerAuth,
	}
	if cfg.SenderAssertionSecret == "" {
		log.Printf("WARNING: AGENT_SENDER_ASSERTION_SECRET is not set -- a webhook turn's sender login is trusted from the request " +
			"body without verification, and neither a chat user's identity nor an EventTrigger dispatch can be signed " +
			"to the engine (docs/adr/0030, 0049). Set it, and integration-gateway's and the engine gateway's matching " +
			"secrets, to require signed assertions.")
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	served := make(chan error, 1)
	go func() { served <- server.Listen(turnsCtx, cfg.HTTPPort) }()
	log.Printf("agent-orchestrator listening: invoke API on :%d", cfg.HTTPPort)

	select {
	case err := <-served:
		return err
	case sig := <-signals:
		log.Printf("%s received, shutting down", sig)
	}

	// Stop the watch first, so a reconnect racing exit logs nothing spurious.
	stopWatch()
	// Refuse NEW turns before draining: a durable record makes an answer
	// retrievable, not this process's poll of the engine survivable.
	server.BeginDraining()
	drainCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownDrain)
	if err := server.Shutdown(drainCtx); err != nil {
		log.Printf("in-flight requests still running after %s; closing stores anyway", cfg.ShutdownDrain)
	}
	cancel()
	if redisClient != nil {
		_ = redisClient.Close()
	}
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 2*time.Second)
	_ = shutdownTracing(flushCtx)
	cancelFlush()
	return nil
}

func retry(label string, fn func() error) error {
	delay := startupInitialDelay
	var err error
	for attempt := 1; attempt <= startupAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt == startupAttempts {
			break
		}
		log.Printf("%s failed (attempt %d/%d): %v -- retrying in %s", label, attempt, startupAttempts, err, delay)
		time.Sleep(delay)
		delay = min(delay*2, startupMaxDelay)
	}
	return fmt.Errorf("%s: %w", label, err)
}

func startEventTriggerWatch(ctx context.Context, cfg facade.Config) (*catalog.EventTriggerRegistry, error) {
	if group := catalog.EventTriggerGVR.Group; cfg.CRDGroup != group || cfg.CRDVersion != catalog.EventTriggerGVR.Version {
		log.Printf("WARNING: AGENT_CRD_GROUP/VERSION=%s/%s, but EventTriggers are watched as %s/%s",
			cfg.CRDGroup, cfg.CRDVersion, group, catalog.EventTriggerGVR.Version)
	}
	restConfig, err := kubeconfig.Load()
	if err != nil {
		return nil, fmt.Errorf("load kube config: %w", err)
	}
	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("build dynamic client: %w", err)
	}
	triggers := catalog.NewEventTriggerRegistry()
	synced := make(chan struct{})
	failed := make(chan error, 1)
	go func() {
		err := catalog.RunEventTriggerWatchNotify(ctx, client, cfg.Namespace, triggers, func() { close(synced) })
		if err != nil {
			failed <- err
		}
	}()
	select {
	case <-synced:
		return triggers, nil
	case err := <-failed:
		return nil, fmt.Errorf("EventTrigger watch: %w", err)
	case <-time.After(triggerSyncTimeout):
		return nil, fmt.Errorf("EventTrigger watch: initial list not synced after %s", triggerSyncTimeout)
	}
}

// startCallerToolStore opens the caller-tool collection over Qdrant's gRPC
// port (AGENT_QDRANT_GRPC_PORT, default 6334) on AGENT_QDRANT_URL's host,
// and starts its prune loop. Qdrant has no TTL, so definitions that stopped
// being sent are swept on a timer.
func startCallerToolStore(ctx context.Context, cfg facade.Config) (callertools.Store, error) {
	parsed, err := url.Parse(cfg.QdrantURL)
	if err != nil || parsed.Hostname() == "" {
		return nil, fmt.Errorf("AGENT_QDRANT_URL %q is not a URL", cfg.QdrantURL)
	}
	client, err := qdrant.NewClient(&qdrant.Config{
		Host:   parsed.Hostname(),
		Port:   cfg.QdrantGRPCPort,
		APIKey: cfg.QdrantAPIKey,
		UseTLS: parsed.Scheme == "https",
	})
	if err != nil {
		return nil, fmt.Errorf("dial qdrant at %s:%s: %w", parsed.Hostname(), strconv.Itoa(cfg.QdrantGRPCPort), err)
	}
	embedder := llm.NewEmbedder(cfg.OpenAIBaseURL, cfg.OpenAIAPIKey, cfg.EmbeddingModel)
	store := callertools.NewQdrantStore(client, cfg.CallerToolsCollection, embedder, uint64(cfg.QdrantVectorSize))
	if err := retry("qdrant startup check", func() error { return store.EnsureCollection(ctx) }); err != nil {
		return nil, err
	}
	go func() {
		ticker := time.NewTicker(cfg.CallerToolPruneInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := store.Prune(ctx, int64(cfg.CallerToolTTL.Seconds())); err != nil {
					// Stale definitions just live until the next sweep.
					log.Printf("caller-tool prune failed: %v", err)
				}
			}
		}
	}()
	return store, nil
}
