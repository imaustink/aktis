import { randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import {
  buildCronEnvelope,
  buildWebhookEnvelope,
  normalizeHeaders,
  parseBody,
  type Envelope,
  type JsonValue,
} from "@aktis/event-triggers";
import type { GithubReplyClient } from "./github-client.js";
import type { GithubDeviceFlowLinker } from "./identity-link/device-flow-linker.js";
import { IdentityLinkApi } from "./identity-link/api.js";
import type { OAuthAuthCodeLinker } from "./identity-link/oauth-authcode-linker.js";
import { ClaudeAuthApi } from "./claude-auth/api.js";
import { ConnectionsApi } from "./connections/api.js";
import type { OidcLoginClient } from "./connections/oidc-login.js";
import type { PrincipalDirectory } from "./connections/principal-directory.js";
import { buildConnectionProviders } from "./connections/providers.js";
import type { IdentityLinkStore } from "./identity-link/store.js";
import type { ClaudeSetupTokenFlows } from "./claude-auth/pty-setup-token.js";
import type { ClaudeLoginFlows } from "./claude-auth/pty-login.js";
import type { ClaudeTokenStore } from "./claude-auth/store.js";
import type { IdentityResolver, RepoContext } from "./identity.js";
import type { OrchestratorClient, OrchestratorInvokeResult, CelResult, SenderExpressions } from "./orchestrator-client.js";
import type { SenderClaims } from "./sender-assertion.js";
import { startSpan, type TraceSpan } from "./telemetry.js";
import { RunEffects, type RunStarter, type WebhookOutcome } from "./runs/effects.js";
import { REPOSITORY } from "./runs/model.js";

export { EVENT_SIGNATURE_HEADER, DEFAULT_RUN_EVENT_MAX_DEPTH } from "./runs/effects.js";

import { ClaudeCredentialRefresher, ClaudeCredentialSweeper } from "./claude-auth/credential-refresher.js";
import { renderSessionPage } from "./session-page.js";
import type { SessionPageStore } from "./session-page-store.js";
import { tickUsername, type TokenAuthenticator } from "./tick-auth.js";
import {
  StaticEventSourceRegistry,
  type CompiledEventSource,
  type CompiledSender,
  type EventSourceLookup,
  type SecretLookup,
} from "./event-sources.js";
import {
  slackUrlVerificationChallenge,
  verifyHeaderName,
  verifyWebhookRequest,
} from "./webhooks/verify.js";

const SESSION_PAGE_PATH = /^\/sessions\/([^/]+)$/;
const SESSION_PROMPT_PATH = /^\/sessions\/([^/]+)\/prompts$/;
const SESSION_LIVE_EVENTS_PATH = /^\/sessions\/([^/]+)\/live-events$/;
const SESSION_LIVE_PROMPT_PATH = /^\/sessions\/([^/]+)\/live-prompt$/;
const WEBHOOK_PATH = /^\/webhooks\/([^/]+)$/;
const TICK_PATH = /^\/events\/([^/]+)\/tick$/;
/** A Kubernetes object name (DNS subdomain) -- anything else cannot name an EventSource, so it is a 404 without a lookup. */
const SOURCE_NAME = /^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$/;
/**
 * Upper bound on a webhook body, buffered in full BEFORE verification (the
 * signature covers the raw bytes). Matches GitHub's own 25 MB payload cap; an
 * unauthenticated sender can't make the gateway buffer more than this.
 */
const MAX_WEBHOOK_BODY_BYTES = 25 * 1024 * 1024;
/** Bound on the sender-CEL compile cache; real deployments have a handful of distinct expressions. */
const MAX_CACHED_SENDER_EXPRESSIONS = 1000;
/**
 * How long a "does not compile" verdict is trusted before the orchestrator is
 * asked again. A verdict depends on the orchestrator's CEL environment, not
 * only on the expression, so during a rollout across versions with different
 * functions or variables a negative answer may stop being true.
 */
const SENDER_COMPILE_NEGATIVE_TTL_MS = 60_000;
/**
 * After `/events/compile` could not be answered, how long the source is served
 * without asking again. Bounds the orchestrator round trips (and their retry
 * sleeps) that unauthenticated POSTs can cause before signature verification.
 */
const SENDER_COMPILE_FAILURE_BACKOFF_MS = 10_000;
/** A bearer that could be a Kubernetes JWT; anything else is refused without being verified. */
const BEARER_JWT = /^Bearer ([A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)$/;
/** Header carrying `sha256=<hex hmac(body)>` on a signed `reply.http` POST. */

export interface GatewayServerOptions {
  /**
   * The `EventSource` table (ADR 0048): which `/webhooks/:source` and
   * `/events/:source/tick` routes exist and how each is verified. Absent ->
   * no sources, every event route 404s.
   */
  eventSources?: EventSourceLookup;
  /**
   * Reads the Secrets that `verify.secretRef` and a trigger's
   * `reply.http.secretRef` name (in the CRD namespace). Absent -> every lookup is "missing", so verification fails
   * closed (401) and signed HTTP replies are not sent.
   */
  secrets?: SecretLookup;
  /**
   * Reviews a cron tick's ServiceAccount token (docs/event-triggers.md §8).
   * Absent (or no `tickNamespace`) -> every tick is 401.
   */
  tickAuthenticator?: TokenAuthenticator;
  /** Namespace the cron sources' ServiceAccounts live in -- the CRD namespace. */
  tickNamespace?: string;
  /** Maps a source's sender login to an identity; only a login it resolves is ever forwarded. */
  identityResolver: IdentityResolver;
  /**
   * Resolves a verified sender's immutable account id to the subject(s) their
   * links live under (docs/adr/0049 §5) -- the identity-link store. Absent:
   * no sender ever resolves to a subject ("unavailable"), which fails closed.
   */
  senderSubjects?: { findSubjectsByAccount?(provider: string, accountId: string): Promise<string[]> };
  orchestratorClient: OrchestratorClient;
  githubReplyClient: GithubReplyClient;
  /** `fetch` used for `reply.http` deliveries; injectable for tests. */
  replyFetch?: typeof fetch;
  /** Operational log lines (drops, ignored events, rejected verifications). Defaults to console.error. Never given secrets. */
  log?: (line: string) => void;
  /** Injectable clock (Slack timestamp window, envelope time). */
  now?: () => Date;
  /** Called with any error from the background invoke-and-reply step; defaults to console.error. */
  onBackgroundError?: (error: unknown) => void;
  /**
   * The identity-link credential-broker API (docs on `IdentityLinkApi`) --
   * optional so deployments/tests that don't use it needn't wire it up. Both
   * fields must be set together to enable `/identity-link/*`.
   */
  identityLinkLinker?: GithubDeviceFlowLinker;
  identityLinkToken?: string;
  /**
   * Authorization-code providers beyond GitHub, keyed by name (docs/adr/0040
   * needs Atlassian). Absent in a deployment that links only GitHub, where
   * behaviour is exactly what it was.
   */
  identityLinkAuthCodeLinkers?: ReadonlyMap<string, OAuthAuthCodeLinker>;
  /**
   * Per-user Claude Code OAuth `setup-token` flow (docs/adr/0027) -- a
   * sibling to identity-link above, but PTY-driven rather than an HTTP
   * device flow. All three must be set together (mirroring
   * `identityLinkLinker`/`identityLinkToken`'s pairing) to enable
   * `/claude-auth/*`; reuses `identityLinkToken` as its own bearer secret
   * rather than minting a new one. Requires `publicBaseUrl` (below) to be
   * set too, since the page link handed back needs a reachable host.
   */
  claudeAuthFlows?: ClaudeSetupTokenFlows;
  claudeAuthStore?: ClaudeTokenStore;
  /**
   * How long a PARKED turn is held open waiting for the caller to finish
   * linking, before giving up. Absent -> {@link
   * GatewayServer.DEFAULT_RESUME_WAIT_MS}. Short values are appropriate wherever
   * turns park routinely rather than exceptionally (see that constant).
   */
  resumeWaitMs?: number;
  /**
   * Full-login (`claude auth login --claudeai`) flows for Remote Control
   * (docs/adr/0027 follow-up) -- optional and additive alongside
   * `claudeAuthFlows` above: a subject's `mode=login` request 501s until this
   * is wired up, rather than falling back to `setup-token`.
   */
  claudeLoginFlows?: ClaudeLoginFlows;
  /**
   * Off switch for refreshing stored `login` credentials before serving them
   * (credential-refresher.ts). Defaults to on. Turning it off restores the
   * behaviour where only a run's own CLI ever refreshes -- which is what made
   * a link depend on every pod reporting back.
   */
  claudeCredentialRefreshEnabled?: boolean;
  /** How close to expiry a stored credential must be before it is refreshed on read. */
  claudeCredentialRefreshMarginMs?: number;
  /** How often the background sweep renews stored credentials nothing is reading. */
  claudeCredentialSweepIntervalMs?: number;
  /**
   * How close to expiry a credential must be for the SWEEP to renew it --
   * deliberately wider than the on-read margin, since the sweep's job is that a
   * link is never found dead rather than that one blob is usable right now.
   */
  claudeCredentialSweepMarginMs?: number;
  /**
   * Session-page feature (issue #81) -- both fields must be set together to
   * enable it: a trigger whose `reply.github.sessionPage` is set gets a link
   * to a minimal `GET /sessions/:token` page appended to its ack comment,
   * showing that session's turn history and accepting follow-up prompts.
   * Omitted entirely, no page routes are reachable and no link is posted.
   */
  sessionPageStore?: SessionPageStore;
  publicBaseUrl?: string;
  /**
   * The Connections page (docs/adr/0046). Requires identity-link
   * (`identityLinkLinker`/`identityLinkToken`) and `publicBaseUrl`; ignored
   * without them. Absent keeps every route exactly as before.
   */
  connections?: {
    store: IdentityLinkStore;
    directory: PrincipalDirectory;
    oidc: OidcLoginClient;
    cookieKey: Buffer;
    sessionTtlMs: number;
    /** Whether GitHub's authcode flow is configured -- the page cannot run the device flow. */
    githubAuthCode: boolean;
  };
  /**
   * The global run-event chain depth cap (docs/adr/0052 §1,
   * `RUN_EVENT_MAX_DEPTH`). Absent -> 12.
   */
  runEventMaxDepth?: number;
  /**
   * Starts the workflow each accepted event's run lives in (docs/adr/0057):
   * runs/temporal.ts in production.
   */
  runs: RunStarter;
}

class BodyTooLargeError extends Error {}

/**
 * The integration gateway's HTTP surface. Its event half (ADR 0048,
 * docs/event-triggers.md §7) is generic: `POST /webhooks/:source` verifies a
 * request against its `EventSource` BEFORE parsing it, normalizes it into the
 * envelope, optionally verifies who sent it, and relays it to the
 * orchestrator's `/invoke`; `POST /events/:source/tick` does the same for a
 * cron source's CronJob. The gateway does no filtering of its own -- whether an
 * event does anything is decided by the orchestrator's EventTriggers -- and
 * the only side effects it runs are the ones the matched trigger's `reply`
 * declares (GitHub ack/result/label removal, an HTTP callback).
 */
export class GatewayServer {
  private server: Server | undefined;
  private readonly identityLinkApi: IdentityLinkApi | undefined;
  private readonly claudeAuthApi: ClaudeAuthApi | undefined;
  private readonly connectionsApi: ConnectionsApi | undefined;
  private readonly claudeCredentialSweeper: ClaudeCredentialSweeper | undefined;
  /** Every side effect of a run, shared with the run activities (runs/activities.ts). */
  readonly effects: RunEffects;
  private readonly eventSources: EventSourceLookup;
  /**
   * Sender CEL the orchestrator has compiled: `error` null when it compiles,
   * else the compiler's message; `at` (epoch ms) is when a negative verdict
   * was recorded, so it can expire.
   */
  private readonly compiledSenderExpressions = new Map<string, { error: string | null; at: number }>();
  /** Expression sets whose `/events/compile` call failed, to the epoch ms until which they are not asked again. */
  private readonly senderCompileBackoff = new Map<string, number>();
  private readonly log: (line: string) => void;
  private readonly now: () => Date;
  private readonly report: (error: unknown) => void;

  constructor(private readonly options: GatewayServerOptions) {
    this.eventSources = options.eventSources ?? new StaticEventSourceRegistry();
    this.log = options.log ?? ((line) => console.error(line));
    this.now = options.now ?? (() => new Date());
    this.report = options.onBackgroundError ?? ((error: unknown) => console.error(error));
    this.effects = new RunEffects({
      eventSources: this.eventSources,
      ...(options.secrets ? { secrets: options.secrets } : {}),
      identityResolver: options.identityResolver,
      ...(options.senderSubjects ? { senderSubjects: options.senderSubjects } : {}),
      orchestratorClient: options.orchestratorClient,
      githubReplyClient: options.githubReplyClient,
      ...(options.replyFetch ? { replyFetch: options.replyFetch } : {}),
      ...(options.sessionPageStore ? { sessionPageStore: options.sessionPageStore } : {}),
      ...(options.publicBaseUrl ? { publicBaseUrl: options.publicBaseUrl } : {}),
      ...(options.claudeAuthStore ? { claudeAuthStore: options.claudeAuthStore } : {}),
      ...(options.resumeWaitMs !== undefined ? { resumeWaitMs: options.resumeWaitMs } : {}),
      ...(options.runEventMaxDepth !== undefined ? { runEventMaxDepth: options.runEventMaxDepth } : {}),
      runs: options.runs,
      log: this.log,
      report: this.report,
      now: this.now,
    });
    this.identityLinkApi =
      options.identityLinkLinker && options.identityLinkToken
        ? new IdentityLinkApi(
            options.identityLinkLinker,
            options.identityLinkToken,
            options.identityLinkAuthCodeLinkers,
          )
        : undefined;
    const refresher = options.claudeAuthStore
      ? new ClaudeCredentialRefresher({
          store: options.claudeAuthStore,
          enabled: options.claudeCredentialRefreshEnabled ?? true,
          ...(options.claudeCredentialRefreshMarginMs ? { marginMs: options.claudeCredentialRefreshMarginMs } : {}),
        })
      : undefined;
    this.claudeAuthApi =
      options.claudeAuthFlows && options.claudeAuthStore && options.identityLinkToken && options.publicBaseUrl
        ? new ClaudeAuthApi(
            options.claudeAuthFlows,
            options.claudeAuthStore,
            options.identityLinkToken,
            options.publicBaseUrl,
            options.claudeLoginFlows,
            // Refresh a near-expiry `login` credential before handing it to a
            // run, in ONE serialized place. Previously the only thing that ever
            // refreshed was a run's own in-pod CLI, which made the stored copy
            // dependent on that pod reporting back, and left an unexercised
            // link to age out on its own.
            refresher,
          )
        : undefined;
    const connections = options.connections;
    if (connections && this.identityLinkApi && options.identityLinkToken && options.publicBaseUrl) {
      const claudeAuthApi = this.claudeAuthApi;
      const connectionsApi = new ConnectionsApi({
        providers: buildConnectionProviders({
          store: connections.store,
          ...(connections.githubAuthCode && options.identityLinkLinker ? { githubLinker: options.identityLinkLinker } : {}),
          ...(options.identityLinkAuthCodeLinkers ? { authCodeLinkers: options.identityLinkAuthCodeLinkers } : {}),
          ...(claudeAuthApi && options.claudeAuthStore
            ? {
                claude: {
                  store: options.claudeAuthStore,
                  start: (subject, kind) => claudeAuthApi.startPageFlow(subject, kind),
                  loginEnabled: Boolean(options.claudeLoginFlows),
                },
              }
            : {}),
        }),
        directory: connections.directory,
        oidc: connections.oidc,
        cookieKey: connections.cookieKey,
        internalToken: options.identityLinkToken,
        publicBaseUrl: options.publicBaseUrl,
        sessionTtlMs: connections.sessionTtlMs,
      });
      const back = connectionsApi.completionRedirect.bind(connectionsApi);
      this.identityLinkApi.completionRedirect = back;
      this.identityLinkApi.cancelRedirect = connectionsApi.cancelRedirect.bind(connectionsApi);
      if (claudeAuthApi) claudeAuthApi.completionRedirect = back;
      this.connectionsApi = connectionsApi;
    }
    // The sweep renews credentials nothing is currently asking for, which is
    // the only thing that keeps an UNUSED link alive -- refresh-on-read above
    // fires only when something reads. Shares the refresher, so a sweep and a
    // concurrent launch collapse onto one refresh per subject rather than
    // rotating each other's token out.
    this.claudeCredentialSweeper =
      refresher && options.claudeAuthStore && (options.claudeCredentialRefreshEnabled ?? true)
        ? new ClaudeCredentialSweeper({
            store: options.claudeAuthStore,
            refresher,
            ...(options.claudeCredentialSweepIntervalMs ? { intervalMs: options.claudeCredentialSweepIntervalMs } : {}),
            ...(options.claudeCredentialSweepMarginMs ? { marginMs: options.claudeCredentialSweepMarginMs } : {}),
          })
        : undefined;
  }

  listen(port: number): Promise<void> {
    return new Promise((resolve) => {
      this.server = createServer((req, res) => {
        this.handleRequest(req, res).catch((error: unknown) => {
          console.error(error);
          if (!res.headersSent) res.writeHead(500);
          if (!res.writableEnded) res.end();
        });
      });
      this.claudeCredentialSweeper?.start();
      this.server.listen(port, resolve);
    });
  }

  close(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.claudeCredentialSweeper?.stop();
      if (!this.server) return resolve();
      this.server.close((err) => (err ? reject(err) : resolve()));
    });
  }

  private async handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (req.method === "GET" && url.pathname === "/healthz") {
      res.writeHead(200).end("ok");
      return;
    }
    const webhookMatch = req.method === "POST" ? WEBHOOK_PATH.exec(url.pathname) : null;
    if (webhookMatch) {
      await this.handleWebhook(req, res, webhookMatch[1] as string);
      return;
    }
    const tickMatch = req.method === "POST" ? TICK_PATH.exec(url.pathname) : null;
    if (tickMatch) {
      await this.handleTick(req, res, tickMatch[1] as string);
      return;
    }
    if (this.options.sessionPageStore) {
      const liveEventsMatch = req.method === "GET" ? SESSION_LIVE_EVENTS_PATH.exec(url.pathname) : null;
      if (liveEventsMatch) {
        await this.handleSessionLiveEvents(res, liveEventsMatch[1] as string);
        return;
      }
      const livePromptMatch = req.method === "POST" ? SESSION_LIVE_PROMPT_PATH.exec(url.pathname) : null;
      if (livePromptMatch) {
        await this.handleSessionLivePrompt(req, res, livePromptMatch[1] as string);
        return;
      }
      const pageMatch = req.method === "GET" ? SESSION_PAGE_PATH.exec(url.pathname) : null;
      if (pageMatch) {
        await this.handleSessionPage(res, pageMatch[1] as string);
        return;
      }
      const promptMatch = req.method === "POST" ? SESSION_PROMPT_PATH.exec(url.pathname) : null;
      if (promptMatch) {
        await this.handleSessionPrompt(req, res, promptMatch[1] as string);
        return;
      }
    }
    if (this.connectionsApi && (await this.connectionsApi.handle(req, res, url))) {
      return;
    }
    // The authcode callback route is intercepted BEFORE the bearer-gated
    // dispatch below -- it's hit directly by the end user's browser (via
    // GitHub's redirect), which cannot carry our internal bearer token, and
    // `identityLinkApi.handle`'s own bearer check would otherwise 401 it.
    if (this.identityLinkApi && (await this.identityLinkApi.handleCallback(req, res, url))) {
      return;
    }
    // Same reasoning as the identity-link callback above: the claude-auth
    // page/submit routes are hit directly by the human's browser (capability
    // via `flowId` in the URL, not a bearer token), so they must be
    // dispatched before `claudeAuthApi.handle`'s bearer check.
    if (this.claudeAuthApi && (await this.claudeAuthApi.handlePage(req, res, url))) {
      return;
    }
    if (this.claudeAuthApi && (await this.claudeAuthApi.handle(req, res, url))) {
      return;
    }
    if (this.identityLinkApi && (await this.identityLinkApi.handle(req, res, url))) {
      return;
    }
    res.writeHead(404).end();
  }

  /**
   * Whether every sender expression of `sourceName` compiles, asked of the
   * orchestrator (the one CEL compiler) once per distinct expression and then
   * cached. A positive verdict is kept (an edited expression is a new cache
   * key); a negative one is re-asked after {@link SENDER_COMPILE_NEGATIVE_TTL_MS},
   * because it depends on the orchestrator's CEL environment too, which can
   * differ across versions mid-rollout. A source with a broken expression 404s,
   * as it did when this process compiled CEL itself.
   *
   * When the orchestrator can't answer, the source is served: each delivery's
   * `/events/prefilter` then evaluates (and fails closed on) the same
   * expressions, so serving it never lets an unevaluated sender through. That
   * failure is remembered for {@link SENDER_COMPILE_FAILURE_BACKOFF_MS}, so
   * repeated (possibly unauthenticated) POSTs during an outage don't each make
   * orchestrator round trips before signature verification.
   */
  private async senderCompiles(sourceName: string, expressions: SenderExpressions): Promise<boolean> {
    const all = [expressions.loginFrom, expressions.isBotFrom, expressions.repositoryFrom, expressions.accountIdFrom].filter(
      (e): e is string => typeof e === "string" && e !== "",
    );
    const now = this.now().getTime();
    const unknown = all.filter((e) => {
      const verdict = this.compiledSenderExpressions.get(e);
      return verdict === undefined || (verdict.error !== null && now - verdict.at >= SENDER_COMPILE_NEGATIVE_TTL_MS);
    });
    if (unknown.length > 0) {
      const backoffKey = unknown.join("\n");
      const backoffUntil = this.senderCompileBackoff.get(backoffKey);
      if (backoffUntil !== undefined && now < backoffUntil) return true;
      const result = await this.options.orchestratorClient.compile(unknown);
      if (result.kind === "failed") {
        this.log(`webhook ${sourceName}: could not check its sender CEL (${result.error}); serving it, each delivery is still evaluated`);
        if (this.senderCompileBackoff.size >= MAX_CACHED_SENDER_EXPRESSIONS) this.senderCompileBackoff.clear();
        this.senderCompileBackoff.set(backoffKey, now + SENDER_COMPILE_FAILURE_BACKOFF_MS);
        return true;
      }
      this.senderCompileBackoff.delete(backoffKey);
      if (this.compiledSenderExpressions.size + unknown.length > MAX_CACHED_SENDER_EXPRESSIONS) {
        this.compiledSenderExpressions.clear();
      }
      for (const expr of unknown) {
        const error = result.errors[expr];
        this.compiledSenderExpressions.set(expr, { error: error ?? null, at: now });
        if (error !== undefined) this.log(`webhook ${sourceName}: sender expression does not compile, routes 404 (${error})`);
      }
    }
    // A stale negative verdict was re-asked above; when that call failed the
    // source is served, so only a fresh or just-confirmed negative 404s here.
    return all.every((e) => this.compiledSenderExpressions.get(e)?.error == null);
  }

  /** The source a route names, or undefined (-> 404) for an unknown, unusable or wrongly-kinded one. */
  private lookupSource(rawName: string, kind: CompiledEventSource["kind"]): CompiledEventSource | undefined {
    let name: string;
    try {
      name = decodeURIComponent(rawName);
    } catch {
      return undefined;
    }
    if (!SOURCE_NAME.test(name)) return undefined;
    const source = this.eventSources.get(name);
    return source && source.kind === kind ? source : undefined;
  }

  /**
   * `POST /webhooks/:source` (docs/event-triggers.md §7). Order matters and
   * is the security argument for this handler:
   *
   *  1. 404 for an unknown or non-webhook source, before reading anything.
   *  2. Buffer the raw bytes (bounded) and VERIFY them -- 401 on failure.
   *     Nothing has been parsed, no CEL has run, nothing has been relayed.
   *  3. Only then parse, answer Slack's url_verification handshake inline,
   *     and ack 202 -- senders have short delivery timeouts, so identity
   *     resolution and the relay run after the response.
   */
  private async handleWebhook(req: IncomingMessage, res: ServerResponse, rawName: string): Promise<void> {
    const source = this.lookupSource(rawName, "webhook");
    if (!source || source.kind !== "webhook") {
      res.writeHead(404).end();
      return;
    }
    if (source.sender && !(await this.senderCompiles(source.name, source.sender.expressions))) {
      // An unusable source, exactly like an unknown one: its sender CEL can
      // never be evaluated, so no delivery to it could ever be relayed.
      res.writeHead(404).end();
      return;
    }

    let rawBody: Buffer;
    try {
      rawBody = await readBodyBuffer(req, MAX_WEBHOOK_BODY_BYTES);
    } catch (error) {
      if (error instanceof BodyTooLargeError) {
        res.writeHead(413, { connection: "close" }).end();
        return;
      }
      throw error;
    }

    const verify = source.webhook.verify;
    const receivedAt = this.now();
    let verdict = verifyWebhookRequest(
      verify,
      await this.readSecret(verify.secretRef.name, verify.secretRef.key),
      req.headers,
      rawBody,
      receivedAt,
    );
    if (!verdict.ok) {
      // One forced re-read before rejecting, so a just-rotated Secret is
      // honoured without waiting out the cache TTL. The reader rate-limits
      // forced re-reads, so a flood of bad signatures can't become a flood of
      // API calls.
      const fresh = await this.readSecret(verify.secretRef.name, verify.secretRef.key, true);
      verdict = verifyWebhookRequest(verify, fresh, req.headers, rawBody, receivedAt);
    }
    if (!verdict.ok) {
      this.log(`webhook ${source.name}: rejected, verification failed (${verdict.reason})`);
      res.writeHead(401).end();
      return;
    }

    const contentType = typeof req.headers["content-type"] === "string" ? req.headers["content-type"] : undefined;
    const body = parseBody(rawBody, contentType);

    if (verify.scheme === "slackV0") {
      // Slack's endpoint handshake is answered synchronously and is not an
      // event -- nothing is relayed. It is still verified first: Slack signs
      // it like any other request.
      const challenge = slackUrlVerificationChallenge(body);
      if (challenge !== undefined) {
        res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ challenge }));
        return;
      }
    }

    // The verify header never reaches the envelope (normalizeHeaders also
    // always drops authorization and cookie): a signature or shared secret has
    // no business in CEL, the orchestrator, or an agent's prompt.
    const headers = normalizeHeaders(req.headers, [verifyHeaderName(verify)]);
    const deliveryHeader = source.webhook.deliveryIdHeader?.toLowerCase();
    const id = (deliveryHeader ? headers[deliveryHeader] : undefined) || randomUUID();

    // The answer waits for the relay's ACCEPT (never for the run): a sender
    // whose agent must act as them but who has no usable linked account gets
    // a synchronous 403 they can act on (docs/adr/0049 §6). Everything else is
    // the same 202 as before -- a dropped or unmatched event is not the
    // sender's error, and a non-2xx would only make them retry the same drop.
    // One trace per delivery (docs/adr/0050 §4), continuing the sender's own
    // traceparent when it sent a valid one. The span covers verification-to-
    // answer; what it decided is `aktis.event.outcome`.
    const span = startSpan("webhook delivery", typeof req.headers.traceparent === "string" ? req.headers.traceparent : undefined, {
      kind: "server",
      attributes: {
        "http.request.method": "POST",
        "http.route": "/webhooks/:source",
        "aktis.event_source": source.name,
        "aktis.event_id": id,
      },
    });
    let outcome: WebhookOutcome;
    try {
      outcome = await this.ingestWebhook(source, { id, headers, body, receivedAt }, span);
    } catch (error) {
      this.report(error);
      span.fail("internal_error");
      outcome = { status: 202 };
    }
    span.set({ "http.response.status_code": outcome.status });
    span.end();
    if (outcome.body !== undefined) {
      res.writeHead(outcome.status, { "content-type": "application/json" }).end(JSON.stringify(outcome.body));
    } else {
      res.writeHead(outcome.status).end();
    }
    outcome.after?.().catch(this.report);
  }

  private async ingestWebhook(
    source: Extract<CompiledEventSource, { kind: "webhook" }>,
    input: { id: string; headers: Record<string, string>; body: JsonValue; receivedAt: Date },
    span: TraceSpan,
  ): Promise<WebhookOutcome> {
    let senderClaims: SenderClaims | undefined;
    let sender: Envelope["sender"] | undefined;
    let repository: string | undefined;
    if (source.sender) {
      const preliminary = buildWebhookEnvelope({ source: source.name, ...input, now: input.receivedAt });
      // One orchestrator round trip answers both pre-sender questions in CEL
      // (docs/event-triggers.md §8): could any trigger fire, and what do this
      // source's sender expressions say. Resolving a sender costs GitHub API
      // calls; most deliveries (push, check_run, ...) can't fire any trigger,
      // so they stop here, before spending any. Only ever drops -- an event
      // that survives is still verified below and matched by the orchestrator.
      const prefilter = await this.options.orchestratorClient.prefilter(preliminary, source.sender.expressions);
      if (prefilter.kind === "failed") {
        this.log(`webhook ${source.name} event ${input.id}: dropped (${prefilter.error})`);
        span.set({ "aktis.event.outcome": "dropped" });
        return { status: 202 };
      }
      if (!prefilter.couldMatch) {
        this.log(`webhook ${source.name} event ${input.id}: no trigger could match (ignored before sender lookup)`);
        span.set({ "aktis.event.outcome": "ignored" });
        return { status: 202 };
      }
      const resolved = await this.resolveSender(source.sender, prefilter.sender);
      if (!resolved.ok) {
        // 202 already sent: a dropped event is not the sender's error, and a
        // non-2xx would only make it retry the same drop.
        this.log(`webhook ${source.name} event ${input.id}: dropped (${resolved.reason})`);
        span.set({ "aktis.event.outcome": "dropped" });
        return { status: 202 };
      }
      senderClaims = await this.effects.senderClaims(source.name, resolved);
      sender = { login: resolved.login, isBot: resolved.isBot };
      repository = resolved.repository;
    }
    // A source without `sender` forwards NO login: the run has no per-user
    // principal and acts as the gateway's own service identity.
    const envelope = buildWebhookEnvelope({
      source: source.name,
      ...input,
      ...(sender ? { sender } : {}),
      ...(repository ? { repository } : {}),
      now: input.receivedAt,
    });
    return this.effects.relayEvent(envelope, senderClaims, span);
  }

  /**
   * Reads the source's sender expressions, as the orchestrator evaluated
   * them over the preliminary envelope (sender {} / repository ""), and has
   * the provider VERIFY the login. Every failure -- an expression error, a
   * wrong type, a bot, a login the provider doesn't recognize -- drops the
   * event: what comes out of here selects whose credentials a run gets, so
   * "couldn't tell" never means "let it through".
   */
  private async resolveSender(
    sender: CompiledSender,
    results: Record<string, CelResult>,
  ): Promise<
    { ok: true; login: string; isBot: boolean; repository: string; accountId?: string } | { ok: false; reason: string }
  > {
    const result = (field: string): CelResult => results[field] ?? { ok: false, error: "not evaluated" };

    const loginResult = result("loginFrom");
    if (!loginResult.ok) return { ok: false, reason: `sender.loginFrom failed: ${loginResult.error}` };
    if (loginResult.type !== "string" || typeof loginResult.value !== "string" || !loginResult.value) {
      return { ok: false, reason: "sender.loginFrom is not a non-empty string" };
    }
    const login = { value: loginResult.value };

    let isBot = false;
    if (sender.expressions.isBotFrom) {
      const r = result("isBotFrom");
      if (!r.ok) return { ok: false, reason: `sender.isBotFrom failed: ${r.error}` };
      if (r.type !== "bool" || typeof r.value !== "boolean") return { ok: false, reason: "sender.isBotFrom is not a bool" };
      isBot = r.value;
    }

    let repository = "";
    if (sender.expressions.repositoryFrom) {
      const r = result("repositoryFrom");
      if (!r.ok) return { ok: false, reason: `sender.repositoryFrom failed: ${r.error}` };
      if (r.type !== "string" || typeof r.value !== "string") return { ok: false, reason: "sender.repositoryFrom is not a string" };
      repository = r.value;
    }

    // The immutable account id (docs/adr/0049 §5). A number in GitHub's
    // payload, so a bare `body.sender.id` is accepted as well as string(...);
    // an int arrives as its decimal string.
    let accountId: string | undefined;
    if (sender.expressions.accountIdFrom) {
      const r = result("accountIdFrom");
      if (!r.ok) return { ok: false, reason: `sender.accountIdFrom failed: ${r.error}` };
      if ((r.type === "string" || r.type === "int") && typeof r.value === "string" && r.value) accountId = r.value;
      else return { ok: false, reason: "sender.accountIdFrom is not a non-empty string or integer" };
    }

    // Self-trigger guard: the agent's own comments/labels come from a bot.
    if (isBot && sender.dropBots) return { ok: false, reason: `sender ${login.value} is a bot` };

    const repoMatch = REPOSITORY.exec(repository);
    const repoContext: RepoContext | undefined = repoMatch ? { owner: repoMatch[1] as string, repo: repoMatch[2] as string } : undefined;
    const identity = await this.options.identityResolver.resolve(login.value, isBot, repoContext);
    if (!identity) return { ok: false, reason: `sender ${login.value} not verified by provider ${sender.provider}` };
    return { ok: true, login: login.value, isBot, repository, ...(accountId ? { accountId } : {}) };
  }

  /**
   * `POST /events/:source/tick` -- a cron source's CronJob (docs/event-triggers.md
   * §8). Authenticated FIRST (so an unauthenticated caller can't probe which
   * sources exist): the bearer must review as THIS source's ServiceAccount, so
   * one source's identity can never tick another. Then 404 unless the source
   * is cron. The payload comes from the CR, never from the tick: the token
   * proves "it's time", not "here is what to do".
   */
  private async handleTick(req: IncomingMessage, res: ServerResponse, rawName: string): Promise<void> {
    req.resume(); // the tick carries no body we use
    if (!(await this.tickAuthorized(req.headers.authorization, rawName))) {
      this.log("event tick rejected: bearer is not this source's ServiceAccount");
      res.writeHead(401).end();
      return;
    }
    const source = this.lookupSource(rawName, "cron");
    if (!source || source.kind !== "cron") {
      res.writeHead(404).end();
      return;
    }
    const envelope = buildCronEnvelope({ source: source.name, ...(source.cron.payload !== undefined ? { payload: source.cron.payload } : {}), now: this.now() });
    res.writeHead(202).end();
    const span = startSpan("cron tick", undefined, {
      kind: "server",
      attributes: { "http.request.method": "POST", "http.route": "/events/:source/tick", "aktis.event_source": source.name },
    });
    this.effects.relayEvent(envelope, undefined, span)
      .catch((error: unknown) => {
        span.fail("internal_error");
        throw error;
      })
      .finally(() => span.end())
      .then((outcome) => outcome.after?.())
      .catch(this.report);
  }

  private async tickAuthorized(authorization: string | undefined, rawName: string): Promise<boolean> {
    const { tickAuthenticator, tickNamespace } = this.options;
    if (!tickAuthenticator || !tickNamespace || typeof authorization !== "string") return false;
    let name: string;
    try {
      name = decodeURIComponent(rawName);
    } catch {
      return false;
    }
    // Shape checks first, so junk never costs an API round trip.
    const bearer = BEARER_JWT.exec(authorization);
    if (!bearer || !SOURCE_NAME.test(name)) return false;
    const username = await tickAuthenticator.authenticate(bearer[1] as string);
    return username === tickUsername(tickNamespace, name);
  }

  private readSecret(name: string, key: string, refresh = false): Promise<string | undefined> {
    return this.effects.readSecret(name, key, refresh);
  }

  /**
   * A follow-up prompt from the session page: an ordinary conversational turn
   * on the SAME session id (no event), whose result is posted to the issue
   * stored on the page entry.
   */
  private async runSessionTurn(owner: string, repo: string, issueNumber: number, sessionId: string, request: string): Promise<void> {
    // This relay has no browser -- always force device flow explicitly rather
    // than relying on agent-orchestrator's own default ("authcode", intended
    // for browser-based callers).
    const invokeOnce = (): Promise<OrchestratorInvokeResult> =>
      this.options.orchestratorClient.invoke(request, sessionId, "device");

    const finishTurn = async (tracked: { turnIndex: number } | undefined, outcome: OrchestratorInvokeResult): Promise<void> => {
      await this.effects.completeTurn(sessionId, tracked, outcome);
      const reply =
        outcome.status === "succeeded"
          ? (outcome.result ?? "")
          : `Something went wrong processing this: ${outcome.error ?? "unknown error"}`;
      await this.options.githubReplyClient.postIssueComment(owner, repo, issueNumber, reply);
    };

    const tracked = await this.effects.trackTurn(sessionId, request);
    const outcome = await invokeOnce();

    // Unauthenticated turn: post the link prompt, then -- for a provider whose
    // token store this gateway owns -- wait for the link and resume the SAME
    // request automatically.
    if (outcome.status === "succeeded" && outcome.identityLinkPending && outcome.identityLink) {
      await finishTurn(tracked, outcome);
      if (await this.effects.waitForLink(outcome.identityLink)) {
        const resumed = await invokeOnce();
        await finishTurn(await this.effects.trackTurn(sessionId, request), resumed);
      }
      return;
    }

    await finishTurn(tracked, outcome);
  }

  private async handleSessionPage(res: ServerResponse, token: string): Promise<void> {
    const entry = await this.options.sessionPageStore!.getByToken(token);
    if (!entry) {
      res.writeHead(404).end("Not found");
      return;
    }
    // Real-time liveness check (ADR 0026) -- refreshed on every page load
    // rather than trusted from a stale cache, since the underlying Pod could
    // have exited since the last check.
    const liveStatus = await this.options.orchestratorClient.checkLive(entry.sessionId);
    if (liveStatus.live && liveStatus.agentRunId) {
      if (entry.live?.agentRunId !== liveStatus.agentRunId) {
        await this.options.sessionPageStore!.setLive(entry.sessionId, { agentRunId: liveStatus.agentRunId });
        entry.live = { agentRunId: liveStatus.agentRunId };
      }
    } else if (entry.live) {
      await this.options.sessionPageStore!.setLive(entry.sessionId, undefined);
      entry.live = undefined;
    }
    res.writeHead(200, { "content-type": "text/html; charset=utf-8" }).end(renderSessionPage(entry, { live: liveStatus.live }));
  }

  /**
   * `GET /sessions/:token/live-events` (ADR 0026): proxies
   * agent-orchestrator's `GET /agent-runs/:runId/events` SSE stream straight
   * through to the browser's `EventSource` -- piped, not buffered.
   */
  private async handleSessionLiveEvents(res: ServerResponse, token: string): Promise<void> {
    const entry = await this.options.sessionPageStore!.getByToken(token);
    if (!entry?.live?.agentRunId) {
      res.writeHead(404).end("Not found");
      return;
    }
    const upstream = await this.options.orchestratorClient.openEventStream(entry.live.agentRunId, entry.sessionId);
    if (!upstream.ok || !upstream.body) {
      res.writeHead(502).end("Live session unavailable");
      return;
    }
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
    res.flushHeaders?.();
    const reader = upstream.body.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        res.write(value);
      }
    } catch {
      // Client disconnected or upstream dropped -- nothing to recover.
    } finally {
      res.end();
    }
  }

  /**
   * `POST /sessions/:token/live-prompt` (ADR 0026): submits a prompt
   * directly into the live opencode session (fire-and-forget via
   * `prompt_async` -- the response streams back through the SSE view the
   * caller is presumably already watching), discovering and caching the
   * underlying opencode session id on first use.
   */
  private async handleSessionLivePrompt(req: IncomingMessage, res: ServerResponse, token: string): Promise<void> {
    const entry = await this.options.sessionPageStore!.getByToken(token);
    if (!entry?.live?.agentRunId) {
      res.writeHead(404).end("Not found");
      return;
    }
    const rawBody = await readBody(req);
    const prompt = new URLSearchParams(rawBody).get("prompt")?.trim() ?? "";
    if (prompt) {
      this.submitLivePrompt(entry.sessionId, entry.live.agentRunId, entry.live.opencodeSessionId, prompt).catch(this.report);
    }
    res.writeHead(303, { location: `/sessions/${token}` }).end();
  }

  private async submitLivePrompt(
    sessionId: string,
    agentRunId: string,
    opencodeSessionId: string | undefined,
    prompt: string,
  ): Promise<void> {
    let sessionIdToUse = opencodeSessionId;
    if (!sessionIdToUse) {
      const listed = await this.options.orchestratorClient.forwardOpencode(agentRunId, sessionId, {
        method: "GET",
        path: "/session",
      });
      const sessions = Array.isArray(listed.body) ? (listed.body as Array<{ id?: unknown }>) : [];
      const discovered = sessions.find((s) => typeof s.id === "string")?.id as string | undefined;
      if (!discovered) throw new Error("could not discover the live opencode session id");
      sessionIdToUse = discovered;
      await this.options.sessionPageStore!.setLive(sessionId, { agentRunId, opencodeSessionId: discovered });
    }
    await this.options.orchestratorClient.forwardOpencode(agentRunId, sessionId, {
      method: "POST",
      path: `/session/${sessionIdToUse}/prompt_async`,
      body: { parts: [{ type: "text", text: prompt }] },
    });
  }

  private async handleSessionPrompt(req: IncomingMessage, res: ServerResponse, token: string): Promise<void> {
    const entry = await this.options.sessionPageStore!.getByToken(token);
    if (!entry) {
      res.writeHead(404).end("Not found");
      return;
    }
    const rawBody = await readBody(req);
    const prompt = new URLSearchParams(rawBody).get("prompt")?.trim() ?? "";
    if (prompt) {
      this.runSessionTurn(entry.owner, entry.repo, entry.issueNumber, entry.sessionId, prompt).catch(this.report);
    }
    // Redirect back to the page immediately -- the new turn shows as
    // "pending" there (meta-refreshing) rather than blocking this response
    // on the orchestrator's full turn, same async posture as the event path.
    res.writeHead(303, { location: `/sessions/${token}` }).end();
  }
}

/** Buffers a request body, rejecting with {@link BodyTooLargeError} past `limit` bytes. */
function readBodyBuffer(req: IncomingMessage, limit: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let done = false;
    req.on("data", (chunk: Buffer) => {
      if (done) return;
      size += chunk.length;
      if (size > limit) {
        done = true;
        reject(new BodyTooLargeError(`request body exceeds ${limit} bytes`));
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      if (!done) resolve(Buffer.concat(chunks));
    });
    req.on("error", (err) => {
      if (!done) reject(err);
    });
  });
}

function readBody(req: IncomingMessage): Promise<string> {
  return readBodyBuffer(req, MAX_WEBHOOK_BODY_BYTES).then((b) => b.toString("utf8"));
}
