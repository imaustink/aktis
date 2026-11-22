/** Central configuration for the orchestrator container. */
export interface AppConfig {
  /** k8s namespace the EventTrigger CRs are read from (ADR 0048). */
  namespace: string;
  /** API group for the CRDs, e.g. `core.aktis.ai`. */
  crdGroup: string;
  /** API version for the CRDs, e.g. `v1alpha1`. */
  crdVersion: string;
  qdrantUrl: string;
  qdrantApiKey: string | undefined;
  qdrantVectorSize: number;
  /**
   * Collection name for consumer-supplied tools (docs/adr/0035) — its OWN
   * collection so a caller's ephemeral function definitions never mix with
   * anything else. Points are keyed by content hash, which makes the
   * collection an embedding cache: an identical definition is embedded once,
   * ever.
   */
  callerToolsQdrantCollection: string;
  /**
   * Max consumer-supplied tools forwarded to the engine (docs/adr/0035 §3).
   * Doubles as the threshold below which the caller-tool index is skipped
   * ENTIRELY: with this many tools or fewer there is nothing to prune, so a
   * caller sending a handful pays no embedding or Qdrant cost at all.
   */
  callerToolTopK: number;
  /**
   * How long an unused caller-tool definition survives before being swept
   * (Qdrant has no native TTL, so this is a periodic `prune`). Any turn that
   * references a definition refreshes it, so this only ages out tool sets that
   * genuinely stopped being sent.
   */
  callerToolTtlSeconds: number;
  /** How often to run that sweep. */
  callerToolPruneIntervalSeconds: number;
  embeddingModel: string;
  /** Base URL of the Temporal engine's gateway Service (docs/adr/0036). Required. */
  temporalEngineUrl: string | undefined;
  /** Bearer token presented to that gateway. */
  temporalEngineToken: string | undefined;
  /**
   * `AGENT_ENGINE` as set, kept only so startup can refuse a deployment still
   * asking for the removed LangGraph engine (docs/adr/0053) rather than
   * silently running it on Temporal.
   */
  requestedEngine: string | undefined;
  /**
   * How long one turn's poll of the engine may run before this process reports
   * a resumable pause instead of an answer. The turn itself keeps running in
   * the engine; its answer arrives on the conversation's next message.
   */
  turnTimeoutSeconds: number;
  /**
   * On SIGTERM, how long to let in-flight HTTP requests finish before tearing
   * down the Redis connections they depend on. Must stay under the pod's
   * `terminationGracePeriodSeconds` (30s today) or k8s SIGKILLs mid-drain and
   * the wait buys nothing.
   */
  shutdownDrainMs: number;
  /** Sliding idle TTL (seconds) for conversation-session records. */
  sessionTtlSeconds: number;
  /**
   * TTL for a `/invoke` accept-then-poll record. Must outlast the longest
   * poll budget any caller uses -- integration-gateway's `pollTimeoutMs`
   * defaults to 15 minutes -- or a slow turn's answer expires before the
   * caller reads it, which reads as the 404 this store exists to prevent.
   */
  invocationTtlSeconds: number;
  /** Hard cap on stored conversation sessions; least-recently-updated evicted first. */
  sessionMaxEntries: number;
  /**
   * Redis URL for the session and invocation stores (docs/adr/0016). When set,
   * both survive restarts and are shared across replicas. Leave unset for the
   * single-replica in-memory default.
   */
  redisUrl: string | undefined;
  /** HTTP port the consumer-facing invoke API listens on (ADR 0006). */
  httpPort: number;
  /**
   * Shared secret integration-gateway signs its sender assertions with
   * (docs/adr/0030 §6), and that this process signs its own assertions to the
   * engine with. When set, a webhook turn's sender login is accepted ONLY from
   * a verified assertion; when unset, the unsigned request-body field is
   * trusted (pre-assertion behaviour, warned about at startup).
   */
  senderAssertionSecret: string;
  /**
   * Base URL of orchestrator/apps/integration-gateway's internal identity-link
   * API. Used here only to record each verified chat user's email for the
   * Connections page (docs/adr/0046).
   */
  identityLinkGatewayUrl: string | undefined;
  /** Bearer token this orchestrator authenticates to the identity-link API with. */
  identityLinkGatewayToken: string | undefined;
  /**
   * Which identity-link flow a caller gets when it doesn't ask for one.
   *
   * Only `/invoke` can name a flow per request (`identity_link_flow`); the
   * chat-completions facade has no such field, so every Open WebUI turn takes
   * this default. `"authcode"` (the default) redirects the browser to GitHub
   * and therefore requires the App's registered Callback URL to match
   * integration-gateway's `GITHUB_OAUTH_REDIRECT_URI` exactly; `"device"`
   * shows a user code instead and involves no redirect URI at all, which is
   * the only flow that works when that Callback URL cannot be set.
   */
  defaultIdentityLinkFlow: "device" | "authcode";
  /**
   * Public URL of integration-gateway's Connections page
   * (`https://<gateway>/connections`, docs/adr/0046). When set, each verified
   * Open WebUI caller's email is recorded with the gateway so the page can
   * find their credentials. Absent leaves the page unable to map anyone.
   */
  connectionsUrl: string | undefined;
  /**
   * Shared HS256 secret matching Open WebUI's `FORWARD_USER_INFO_HEADER_JWT_SECRET`,
   * used to verify its per-request `X-OpenWebUI-User-Jwt` header
   * (`OpenWebUiForwardedUserResolver`). Open WebUI's `Authorization` bearer
   * token is a single static value shared by every one of its users, so
   * without this every human collapses into the engine's one shared subject.
   */
  openWebUiUserJwtSecret: string | undefined;
  /**
   * RBAC roles granted to every caller `OpenWebUiForwardedUserResolver`
   * resolves. Comma-separated. Defaults to `reader,writer`.
   */
  openWebUiUserRoles: string[];
  /**
   * Bearer-token identity configuration: `"static"` resolves tokens from
   * `staticIdentities`; `"oidc"` verifies them as JWTs against `oidcIssuer`'s
   * JWKS, falling back to `staticIdentities` when that is also set.
   *
   * Authenticates whoever starts a turn (rbac/caller-authenticator.ts); a
   * chat turn with a verified forwarded-user JWT needs no bearer. Decides
   * whether the turn runs at all, not who it runs as.
   */
  identityResolverKind: "static" | "oidc";
  staticIdentities: string | undefined;
  oidcIssuer: string | undefined;
  oidcJwksUri: string | undefined;
  oidcAudience: string | undefined;
  oidcRolesClaim: string;
  /**
   * Raw `AGENT_CALLER_AUTH`: `enforce` (the default when unset) or `warn`.
   * Validated at startup (rbac/caller-auth-config.ts), so a typo stops the pod
   * instead of quietly turning enforcement off.
   */
  callerAuthMode: string | undefined;
}

function num(raw: string | undefined, fallback: number): number {
  if (raw === undefined) return fallback;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

export const config: AppConfig = {
  namespace: process.env.AGENT_NAMESPACE ?? "default",
  crdGroup: process.env.AGENT_CRD_GROUP ?? "core.aktis.ai",
  crdVersion: process.env.AGENT_CRD_VERSION ?? "v1alpha1",
  qdrantUrl: process.env.AGENT_QDRANT_URL ?? "http://localhost:6333",
  qdrantApiKey: process.env.AGENT_QDRANT_API_KEY,
  qdrantVectorSize: num(process.env.AGENT_QDRANT_VECTOR_SIZE, 1536),
  callerToolsQdrantCollection: process.env.AGENT_QDRANT_CALLER_TOOLS_COLLECTION ?? "caller_tools",
  callerToolTopK: num(process.env.AGENT_CALLER_TOOL_TOP_K, 5),
  callerToolTtlSeconds: num(process.env.AGENT_CALLER_TOOL_TTL_SECONDS, 604_800), // 7 days
  callerToolPruneIntervalSeconds: num(process.env.AGENT_CALLER_TOOL_PRUNE_INTERVAL_SECONDS, 3_600),
  embeddingModel: process.env.AGENT_EMBEDDING_MODEL ?? "text-embedding-3-small",
  temporalEngineUrl: process.env.AGENT_TEMPORAL_ENGINE_URL || undefined,
  temporalEngineToken: process.env.AGENT_TEMPORAL_ENGINE_TOKEN,
  requestedEngine: process.env.AGENT_ENGINE || undefined,
  // Same env var as before 0053, when it also bounded the in-process engine's
  // own wait on an agent run.
  turnTimeoutSeconds: num(process.env.AGENT_RUN_TIMEOUT_SECONDS, 28800),
  shutdownDrainMs: num(process.env.AGENT_SHUTDOWN_DRAIN_MS, 25_000), // under the 30s grace period
  sessionTtlSeconds: num(process.env.AGENT_SESSION_TTL_SECONDS, 1800),
  invocationTtlSeconds: num(process.env.AGENT_INVOCATION_TTL_SECONDS, 3600),
  sessionMaxEntries: num(process.env.AGENT_SESSION_MAX_ENTRIES, 1000),
  redisUrl: process.env.AGENT_REDIS_URL,
  httpPort: num(process.env.AGENT_HTTP_PORT, 8081),
  senderAssertionSecret: process.env.AGENT_SENDER_ASSERTION_SECRET ?? "",
  identityLinkGatewayUrl: process.env.IDENTITY_LINK_GATEWAY_URL,
  identityLinkGatewayToken: process.env.IDENTITY_LINK_GATEWAY_TOKEN,
  // Anything other than an exact "device" keeps the pre-existing authcode
  // behaviour -- a typo must not silently change which flow every chat user
  // gets.
  defaultIdentityLinkFlow: process.env.AGENT_DEFAULT_IDENTITY_LINK_FLOW === "device" ? "device" : "authcode",
  connectionsUrl: process.env.AGENT_CONNECTIONS_URL || undefined,
  openWebUiUserJwtSecret: process.env.AGENT_OPENWEBUI_USER_JWT_SECRET,
  openWebUiUserRoles: (process.env.AGENT_OPENWEBUI_USER_ROLES ?? "reader,writer")
    .split(",")
    .map((role) => role.trim())
    .filter((role) => role.length > 0),
  identityResolverKind: process.env.AGENT_IDENTITY_RESOLVER === "oidc" ? "oidc" : "static",
  staticIdentities: process.env.AGENT_STATIC_IDENTITIES,
  oidcIssuer: process.env.AGENT_OIDC_ISSUER,
  oidcJwksUri: process.env.AGENT_OIDC_JWKS_URI,
  oidcAudience: process.env.AGENT_OIDC_AUDIENCE,
  oidcRolesClaim: process.env.AGENT_OIDC_ROLES_CLAIM ?? "roles",
  callerAuthMode: process.env.AGENT_CALLER_AUTH || undefined,
};
