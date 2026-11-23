import * as k8s from "@kubernetes/client-node";
import { config } from "./config.js";
import { setupTelemetry } from "./telemetry.js";
import { OpenWebUiForwardedUserResolver } from "./rbac/openwebui-forwarded-user-resolver.js";
import { PrincipalRecordingResolver } from "./identity-link/principal-recording-resolver.js";
import { IdentityLinkGatewayClient } from "./identity-link/gateway-client.js";
import { CrdEventTriggerRegistry } from "./routing/crd-event-trigger-registry.js";
import { OpenAiEmbedder } from "./vector-store/openai-embedder.js";
import { QdrantCallerToolStore } from "./caller-tools/qdrant-caller-tool-store.js";
import { TemporalEngine } from "./engine/temporal-engine.js";
import { OpenAiTaskCompleter } from "./openai/task-completer.js";
import { InMemorySessionStore } from "./session/in-memory-session-store.js";
import { RedisSessionStore } from "./session/redis-session-store.js";
import type { SessionStore } from "./session/types.js";
import { RedisInvocationStore } from "./invocation/redis-invocation-store.js";
import { InMemoryInvocationStore, type InvocationStore } from "./invocation/types.js";
import { InvokeServer } from "./server.js";
import { buildCallerAuthenticator } from "./rbac/caller-auth-config.js";
import { retryWithBackoff } from "./retry.js";

/** Process exit code for startup failures — this is a long-lived service, not a one-shot CLI (ADR 0006). */
const EXIT_STARTUP_FAILURE = 1;

/** Startup retry for a dependency that routinely comes up after this pod (~2 min worst case). */
const STARTUP_RETRY = { attempts: 12, initialDelayMs: 1_000, maxDelayMs: 15_000 };

/**
 * Long-lived service entry point (ADR 0006): the facade in front of the
 * Temporal engine (docs/adr/0036, 0053). Serves, on `AGENT_HTTP_PORT`:
 *
 * - `POST /invoke` / `GET /invoke/:id` — accept-then-poll turns, including
 *   EventTrigger envelopes (ADR 0048), matched here and forwarded resolved.
 * - `GET /v1/models`, `POST /v1/chat/completions` — the OpenAI-compatible
 *   facade (ADR 0007) Open WebUI talks to.
 *
 * Every turn runs on the engine. What stays here is request parsing, trigger
 * selection, consumer-supplied tools (ADR 0035), the sticky read-only session
 * flag, and the per-user identity this process verifies and signs across the
 * hop.
 */
async function main(): Promise<void> {
  // docs/adr/0050 §4: a span per /invoke and per engine call, exported only
  // when the chart sets OTEL_EXPORTER_OTLP_ENDPOINT.
  const shutdownTelemetry = setupTelemetry("agent-orchestrator");

  if (config.requestedEngine && config.requestedEngine !== "temporal") {
    // Fail loudly rather than run a deployment that asked for something else:
    // the in-process LangGraph engine is gone (docs/adr/0053), and a stale
    // `AGENT_ENGINE=langgraph` left over from a rollback is a deployment that
    // believes it is running code that no longer exists.
    console.error(
      `AGENT_ENGINE=${config.requestedEngine} is not supported: the Temporal engine is the only engine ` +
        "(docs/adr/0053). Unset AGENT_ENGINE and set AGENT_TEMPORAL_ENGINE_URL.",
    );
    process.exit(EXIT_STARTUP_FAILURE);
  }
  if (!config.temporalEngineUrl) {
    console.error("AGENT_TEMPORAL_ENGINE_URL is required: every turn runs on the Temporal engine (docs/adr/0036).");
    process.exit(EXIT_STARTUP_FAILURE);
  }

  const kubeConfig = new k8s.KubeConfig();
  kubeConfig.loadFromDefault();

  // EventTrigger table (ADR 0048, docs/event-triggers.md): `/invoke` with an
  // `event` envelope is matched here (CEL over the envelope) and the RESOLVED
  // turn, not the envelope, is forwarded to the engine. No triggers installed
  // -> every event is a clean `200 ignored`.
  const eventTriggerRegistry = CrdEventTriggerRegistry.fromKubeConfig(
    config.namespace,
    config.crdGroup,
    config.crdVersion,
    kubeConfig,
  );
  await eventTriggerRegistry.listAll();
  const eventTriggerWatch = eventTriggerRegistry.watch((err) => console.error("EventTrigger watch error:", err));

  // Resolves identity from Open WebUI's per-request signed user JWT rather
  // than its shared static bearer token -- without this, every chat turn runs
  // as the engine gateway's one shared subject, so an OAuth identity linked
  // by one Open WebUI user is usable by all of them.
  const openWebUiResolver = config.openWebUiUserJwtSecret
    ? new OpenWebUiForwardedUserResolver({ secret: config.openWebUiUserJwtSecret, roles: config.openWebUiUserRoles })
    : undefined;

  // Authenticates whoever starts a turn (docs/adr/0053 follow-up 1): a
  // verified Open WebUI user JWT on a chat turn, otherwise a bearer token the
  // static/OIDC map resolves. Handed the BARE JWT verifier, not the
  // principal-recording one below, so a refused turn records nothing.
  const callerAuth = buildCallerAuthenticator(config, openWebUiResolver);
  if ("error" in callerAuth) {
    console.error(callerAuth.error);
    process.exit(EXIT_STARTUP_FAILURE);
  }
  for (const warning of callerAuth.warnings) console.error(`WARNING: ${warning}`);

  const identityLinkGateway =
    config.identityLinkGatewayUrl && config.identityLinkGatewayToken
      ? new IdentityLinkGatewayClient({
          baseUrl: config.identityLinkGatewayUrl,
          token: config.identityLinkGatewayToken,
        })
      : undefined;

  // The Connections page (docs/adr/0046) finds a chat user's credentials from
  // an IdP sign-in by email; this records each verified caller's email for it.
  const forwardedUserIdentityResolver =
    openWebUiResolver && identityLinkGateway && config.connectionsUrl
      ? new PrincipalRecordingResolver(openWebUiResolver, identityLinkGateway)
      : openWebUiResolver;
  if (config.connectionsUrl && !(openWebUiResolver && identityLinkGateway)) {
    console.warn(
      "AGENT_CONNECTIONS_URL is set but the Open WebUI JWT secret or identity-link gateway is not, " +
        "so no chat user can be found from the Connections page",
    );
  }

  // Said out loud at startup because the failure it guards against is
  // otherwise SILENT: an authcode link dies at GitHub's own consent screen
  // ("The redirect_uri is not associated with this application") before any
  // request reaches this process or the engine.
  console.log(
    `identity-link default flow: ${config.defaultIdentityLinkFlow}` +
      (config.defaultIdentityLinkFlow === "authcode"
        ? " -- requires the GitHub App's registered Callback URL to exactly match integration-gateway's " +
          "GITHUB_OAUTH_REDIRECT_URI; set AGENT_DEFAULT_IDENTITY_LINK_FLOW=device to use the redirect-free device flow instead"
        : " -- user-code flow, no redirect URI involved"),
  );

  // Conversation-session store: holds only the sticky read-only flag (see
  // session/types.ts). Redis-backed when AGENT_REDIS_URL is set, so the flag
  // survives restarts and is shared across replicas.
  let redisSessionStore: RedisSessionStore | undefined;
  let sessionStore: SessionStore;
  if (config.redisUrl) {
    const store = new RedisSessionStore(config.redisUrl, { ttlSeconds: config.sessionTtlSeconds });
    await retryWithBackoff("redis startup check", () => store.connect(), STARTUP_RETRY);
    console.error(`Using Redis session store: ${config.redisUrl}`);
    redisSessionStore = store;
    sessionStore = store;
  } else {
    sessionStore = new InMemorySessionStore({
      ttlMs: config.sessionTtlSeconds * 1000,
      maxEntries: config.sessionMaxEntries,
    });
  }

  // Where `/invoke`'s accept-then-poll records live. On Redis, ANY replica can
  // answer a poll -- which is what stops a rollout losing every turn accepted
  // in the seconds around it, and is the precondition for running more than one
  // replica at all.
  //
  // TTL has to outlast the longest poll budget a caller uses, not the session
  // TTL: integration-gateway polls for up to `pollTimeoutMs` (15 minutes by
  // default) on one record.
  let redisInvocationStore: RedisInvocationStore | undefined;
  let invocationStore: InvocationStore;
  if (config.redisUrl) {
    const store = new RedisInvocationStore(config.redisUrl, { ttlSeconds: config.invocationTtlSeconds });
    await retryWithBackoff("redis invocation store startup check", () => store.connect(), STARTUP_RETRY);
    console.error(`Using Redis invocation store: ${config.redisUrl} (ttl ${config.invocationTtlSeconds}s)`);
    redisInvocationStore = store;
    invocationStore = store;
  } else {
    console.error(
      "No REDIS_URL: /invoke records are in-process, so a poll must reach the SAME pod that accepted it. " +
        "A rollout loses turns accepted around it, and more than one replica will 404 polls.",
    );
    invocationStore = new InMemoryInvocationStore();
  }

  // Just-in-time index for consumer-supplied tools (docs/adr/0035), in its own
  // collection. Nothing is upserted at startup (definitions arrive in request
  // bodies), so this only ensures the collection exists -- retried, because
  // after a full-cluster restart this pod routinely comes up before Qdrant.
  const callerToolStore = new QdrantCallerToolStore(
    {
      url: config.qdrantUrl,
      apiKey: config.qdrantApiKey,
      collection: config.callerToolsQdrantCollection,
      vectorSize: config.qdrantVectorSize,
    },
    new OpenAiEmbedder({ model: config.embeddingModel }),
  );
  await retryWithBackoff("qdrant startup check", () => callerToolStore.ensureCollection(), STARTUP_RETRY);
  // Qdrant has no native TTL, so definitions that stopped being sent are swept
  // on a timer. `unref()` so an idle sweep timer never holds the process open
  // during shutdown; it is also cleared explicitly below.
  const callerToolPruneTimer = setInterval(
    () => {
      void callerToolStore
        .prune(config.callerToolTtlSeconds * 1_000)
        // A failed sweep is not worth failing anything else over: the only
        // consequence is that stale definitions live until the next sweep.
        .catch((err: unknown) => console.warn("caller-tool prune failed:", err));
    },
    config.callerToolPruneIntervalSeconds * 1_000,
  );
  callerToolPruneTimer.unref();

  const engine = new TemporalEngine({
    baseUrl: config.temporalEngineUrl,
    ...(config.temporalEngineToken ? { token: config.temporalEngineToken } : {}),
    // Reuses the assertion contract rather than trusting a login on an
    // internal hop -- see TemporalEngine's own note.
    ...(config.senderAssertionSecret ? { senderAssertionSecret: config.senderAssertionSecret } : {}),
    // Without it, every CHAT turn collapses onto the gateway's shared
    // default/bearer identity regardless of which human is chatting. Scoped to
    // this one resolver deliberately -- see
    // TemporalEngineOptions.forwardedUserIdentityResolver's doc comment.
    ...(forwardedUserIdentityResolver ? { forwardedUserIdentityResolver } : {}),
    timeoutMs: config.turnTimeoutSeconds * 1_000,
  });
  console.log(`agent engine: temporal (${config.temporalEngineUrl})`);

  const invokeServer = new InvokeServer(
    engine,
    sessionStore,
    // Answers Open WebUI's internal housekeeping completions (title/tags/query
    // generation) directly, bypassing the engine -- see server.ts's
    // handleInternalUiTask and isInternalUiTaskRequest.
    new OpenAiTaskCompleter(),
    eventTriggerRegistry,
    config.senderAssertionSecret,
    callerToolStore,
    config.callerToolTopK,
    config.defaultIdentityLinkFlow,
    invocationStore,
    callerAuth.authenticator,
  );
  if (!config.senderAssertionSecret) {
    console.error(
      "WARNING: AGENT_SENDER_ASSERTION_SECRET is not set -- a webhook turn's sender login is trusted from the request " +
        "body without verification, and neither a chat user's identity nor an EventTrigger dispatch can be signed " +
        "to the engine (docs/adr/0030, 0049). Set it, and integration-gateway's and the engine gateway's matching " +
        "secrets, to require signed assertions.",
    );
  }

  await invokeServer.listen(config.httpPort);
  console.error(`agent-orchestrator listening: invoke API on :${config.httpPort}`);

  const shutdown = async (signal: string): Promise<void> => {
    console.error(`${signal} received, shutting down`);
    // Stop the CRD watch before closing everything else -- otherwise a
    // reconnect racing the process exit would log a spurious watch error.
    eventTriggerWatch.stop();
    clearInterval(callerToolPruneTimer);

    // Refuse NEW turns first. Durable invocation records make an answer
    // retrievable from any replica; they do not keep this process's poll of
    // the engine alive. So a caller arriving now is told to retry (503) and
    // reaches the replacement, rather than being handed a 202 for a record
    // nobody will complete.
    invokeServer.beginDraining();

    // Phase 1 -- let in-flight requests finish, bounded, because k8s SIGKILLs
    // at terminationGracePeriodSeconds (30s) regardless. The turns themselves
    // carry on in the engine; only this process's wait on them ends.
    const httpDrained = invokeServer.close().then(
      () => "drained" as const,
      (err: unknown) => {
        console.error("error draining HTTP server during shutdown:", err);
        return "drained" as const;
      },
    );
    const drainOutcome = await Promise.race([
      httpDrained,
      new Promise<"deadline">((resolve) => setTimeout(() => resolve("deadline"), config.shutdownDrainMs)),
    ]);
    if (drainOutcome === "deadline") {
      console.error(`in-flight requests still running after ${config.shutdownDrainMs}ms; closing stores anyway`);
    }

    // Phase 2 -- nothing should still be waiting on these.
    const closers: Promise<void>[] = [];
    if (redisSessionStore) closers.push(redisSessionStore.close());
    if (redisInvocationStore) closers.push(redisInvocationStore.close());
    await Promise.all(closers);
    await shutdownTelemetry();
    process.exit(0);
  };
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  process.on("SIGINT", () => void shutdown("SIGINT"));
}

main().catch((err) => {
  // Log the full error (stack + any extra fields the underlying client
  // attached, e.g. Qdrant/OpenAI clients often put the response body on a
  // non-standard property) -- a bare `.message` here was hiding the actual
  // cause of startup failures (just "Bad Request" with nothing else).
  console.error(err instanceof Error ? (err.stack ?? err.message) : String(err));
  if (err && typeof err === "object") {
    const { message: _m, stack: _s, ...rest } = err as Record<string, unknown>;
    if (Object.keys(rest).length > 0) console.error("additional error fields:", rest);
  }
  process.exit(EXIT_STARTUP_FAILURE);
});
