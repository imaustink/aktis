import { randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import {
  callerToolId,
  type CallerToolDescriptor,
  type CallerToolStore,
  type PendingToolCall,
  type PriorCallerToolCall,
} from "./caller-tools/types.js";
import { parseCallerTools } from "./caller-tools/parse.js";
import { resolveCallerTools } from "./caller-tools/resolve.js";
import type { SessionStore } from "./session/types.js";
import { InMemoryInvocationStore, type InvocationStore } from "./invocation/types.js";
import { checkOutcome } from "./invocation/outcome.js";
import { parseEnvelope, type EventTriggerSelector } from "./routing/crd-event-trigger-registry.js";
import type { Envelope, RenderedReply } from "@aktis/event-triggers";
import {
  buildAgentRequest,
  chatCompletionChunk,
  chatCompletionId,
  chatCompletionResponse,
  chatCompletionToolCallResponse,
  toolCallDeltaChunk,
  errorStatusAndCode,
  isInternalUiTaskRequest,
  listModelsResponse,
  MODEL_ID,
  openAiError,
  renderResult,
  writeSseChunk,
  writeSseComment,
  writeSseDone,
  writeSseStatus,
} from "./openai/chat-completions.js";
import { SENDER_ASSERTION_HEADER, verifySenderClaims, type SenderClaims } from "./rbac/sender-assertion.js";
import type { CallerAuthResult, CallerAuthenticator } from "./rbac/caller-authenticator.js";
import type { PriorToolCall, TurnEngine, TurnInput, TurnOutcome } from "./engine/types.js";
import { TRACEPARENT_HEADER } from "./traceparent.js";
import { startSpan, type TraceSpan } from "./telemetry.js";
import type { TaskCompleter } from "./openai/task-completer.js";
import { withHeartbeat } from "./openai/with-heartbeat.js";
import { renderTurnEvent, type TurnEvent, type TurnEventKind } from "@aktis/messaging";

/**
 * Builds a {@link TurnEvent} for a lifecycle point the orchestrator narrates
 * (ADR 0004). The sequence number is per-process-monotonic — enough for the
 * ordering/dedupe the envelope documents; B1 does not persist the stream.
 */
let turnEventSeq = 0;
function turnEvent(kind: TurnEventKind, fields: Partial<Omit<TurnEvent, "seq" | "ts" | "kind">> = {}): TurnEvent {
  return { seq: turnEventSeq++, ts: new Date().toISOString(), kind, ...fields };
}

export type InvocationStatus = "pending" | "succeeded" | "failed";

export interface InvocationRecord {
  id: string;
  status: InvocationStatus;
  result?: unknown;
  error?: string;
  /**
   * The most recent progress message seen with `stage: "remote-control-url"`
   * (a Remote Control session URL, `https://claude.ai/code/session_...`) --
   * only ever set when the delegated agent actually emits one (today: a
   * later phase of `claude-code-swe-agent`; `opencode-swe-agent` and every
   * other run never sets it). A polling caller (integration-gateway's triage
   * relay) uses this to prefer linking a live Remote Control session over its
   * own session page once available. Omitted/undefined for every run that
   * never emits this progress event -- fully backward compatible.
   */
  remoteControlUrl?: string;
  /**
   * Tool calls the turn is asking the CALLER to execute (docs/adr/0035), present
   * only when the planner chose a consumer-supplied tool. A `succeeded` record
   * carrying these has produced no `result`: the answer depends on the caller
   * running these functions.
   *
   * Note `/invoke` can only OFFER tools, not resume from their results — it takes
   * a single `request` string with nowhere to put a `role: "tool"` message. The
   * full round trip is chat-facade-only.
   */
  pendingToolCalls?: PendingToolCall[];
  /**
   * The structured outcome (docs/adr/0052 §2) of an event turn's forced
   * `agentRef` run, from the run's FINAL reply. integration-gateway copies it
   * into the run-completion event's `body.outcome`, where the next trigger's
   * CEL reads it (`has(body.outcome.pullRequest)`). Present only on a
   * `succeeded` record, only when the run reported one, and only after
   * `checkOutcome` accepted it (a JSON object, <= 8 KiB compact). Unrelated to
   * `result`, which stays the turn's text answer.
   */
  outcome?: Record<string, unknown>;
  /**
   * The engine's own id for this turn (Temporal: workflow id + update id),
   * recorded on a still-pending record the moment the engine accepts it.
   * The engine's record IS the workflow, so with this id ANY orchestrator
   * process can finish the turn -- not just the one whose in-memory poll
   * started it. A rollout kills that poll; without this the record stayed
   * `pending` forever and the caller (integration-gateway) never heard back,
   * which is how a software-factory chain silently stopped mid-review.
   * Internal: never returned to a caller.
   */
  engineInvocationId?: string;
  /** Whether the turn was a forced `agentRef` dispatch -- the only kind whose `outcome` is reported -- so an adopting process finishes it the same way. Internal. */
  forcedAgent?: boolean;
}

/**
 * The run outcome to put on an invocation record, or undefined. An outcome
 * that fails `checkOutcome` is dropped and logged, never truncated or passed
 * through: it feeds CEL predicates that decide what runs next.
 */
function acceptedOutcome(invocationId: string, raw: unknown): Record<string, unknown> | undefined {
  if (raw === undefined) return undefined;
  const checked = checkOutcome(raw);
  if (checked.ok) return checked.outcome;
  console.warn(`agent-orchestrator: dropping run outcome for invocation ${invocationId}: ${checked.reason}`);
  return undefined;
}

/** The 202's `trigger` object for an event turn (docs/event-triggers.md §5, docs/adr/0052 §1/§3). */
interface TriggerResponse {
  name: string;
  sessionId: string;
  reply: RenderedReply | null;
  /** Every rendered param in §4 string form; the gateway threads these into the run-completion event. */
  params: Record<string, string>;
  /** Present only when the trigger sets `onComplete.emitTo`. */
  onComplete?: { emitTo: string };
}

/** How often to emit an SSE keep-alive comment while a turn is running on the engine. */
const HEARTBEAT_MS = 15_000;

/**
 * Conversation-id header forwarded by Open WebUI on every upstream request
 * when its deployment sets `ENABLE_FORWARD_USER_INFO_HEADERS=true`
 * (docs/adr/0012). Absent header -> stateless per-turn skill selection,
 * exactly the pre-0012 behavior.
 */
const CHAT_ID_HEADER = "x-openwebui-chat-id";

/**
 * Per-request signed user JWT forwarded by Open WebUI on every upstream
 * request when its deployment sets `ENABLE_FORWARD_USER_INFO_HEADERS=true`.
 * Unlike the `Authorization` bearer token (a single static value shared by
 * every Open WebUI user), this identifies the specific human sending the
 * request -- see `OpenWebUiForwardedUserResolver`. Absent header -> identity
 * resolution falls back to the shared static/OIDC path, same as before this
 * header was read.
 */
const FORWARDED_USER_JWT_HEADER = "x-openwebui-user-jwt";

/**
 * Consumer-facing HTTP interface (ADR 0006). Deliberately asynchronous:
 * `POST /invoke` returns immediately with an id; the caller polls
 * `GET /invoke/:id` for the result. A synchronous request/response would
 * hold the HTTP connection open for as long as the launched tool Job takes
 * (e.g. video transcription can take minutes), risking proxy/client
 * timeouts — see ADR 0006 for the alternatives considered.
 *
 * Runs on its own port (`AGENT_HTTP_PORT`), separate from the
 * Job-callback port (`CallbackReceiver`, `AGENT_CALLBACK_PORT`) so the two
 * can be exposed differently at the network level: this one to whoever is
 * allowed to call the agent, the callback port only to Job pods in-cluster.
 *
 * Every turn runs on the Temporal engine (docs/adr/0036, 0053); this process is
 * the facade in front of it: request parsing, EventTrigger selection, caller
 * tools, and the per-user identity it signs across the hop.
 *
 * Also serves an OpenAI Chat Completions-compatible facade on the same port
 * (`GET /v1/models`, `POST /v1/chat/completions`) so chat UIs that speak the
 * OpenAI API (e.g. Open WebUI) can call the agent directly (ADR 0007). This
 * is a thin translation layer over the same engine — it doesn't change how
 * `/invoke` behaves.
 */
/**
 * Everything a single turn needs to know about consumer-supplied tools
 * (docs/adr/0035), bundled so it can ride along as one trailing
 * `buildTurnInput` argument instead of three more positional params.
 */
interface CallerToolTurn {
  /** Resolved, top-K-pruned tools to offer the planner. */
  tools: CallerToolDescriptor[];
  /** `tool_choice: "required"` — a directive, not a guarantee. */
  required: boolean;
  /** Prior client-executed calls, mapped into the planner's own history shape. */
  actionHistory: PriorToolCall[];
}

/**
 * The turn an EventTrigger resolved (docs/event-triggers.md §5), bundled so it
 * rides into `buildTurnInput` as one argument. Everything here was derived by
 * the shared `selectTrigger` -- the engine never sees the raw envelope.
 */
interface TriggerTurn {
  triggerName: string;
  forcedSkillId?: string;
  forcedAgentId?: string;
  forcedToolId?: string;
  toolInput?: string;
  readOnly: boolean;
  params: Record<string, string>;
  /** `target.identity` (docs/adr/0049); see `TurnInput.identityOverride`. */
  identity?: string;
}

export class InvokeServer {
  private server: Server | undefined;
  /**
   * Where accept-then-poll records live. Defaults to an in-process Map (the
   * previous behaviour, single-replica); a Redis-backed store lets ANY replica
   * answer any poll, which is what stops a rollout losing every turn accepted
   * around it. See invocation/types.ts.
   */
  private readonly invocations: InvocationStore;
  /**
   * Set the moment shutdown begins. A pod that is going away must stop
   * ACCEPTING new turns even while it finishes the ones it has: `/invoke`
   * answers 202 and polls the engine from this process, so a turn accepted now
   * is a turn whose poll dies with the pod. Durable records make the answer
   * *retrievable*, not the poll survivable -- only refusing keeps a caller
   * from being handed a record nobody will complete.
   */
  private draining = false;
  /** Invocations a poll in THIS process is driving to completion (see adoptInvocation). */
  private readonly driving = new Set<string>();

  constructor(
    private readonly engine: TurnEngine,
    /**
     * Conversation-session store. Holds only the sticky read-only flag
     * (`SessionRecord.readOnly`); the engine keeps everything else about a
     * conversation itself. Absent -> no turn inherits read-only.
     */
    private readonly sessionStore?: SessionStore,
    /**
     * Answers Open WebUI's internal housekeeping completions (title/tags/
     * query generation, `isInternalUiTaskRequest`) directly, bypassing the
     * engine so these can never be misrouted into skill/agent
     * delegation. Absent -> such requests still bypass the engine but get a
     * generic static reply (safety takes priority over title quality).
     */
    private readonly taskCompleter?: TaskCompleter,
    /**
     * EventTrigger table (ADR 0048, docs/event-triggers.md). `/invoke` with an
     * `event` envelope is matched against it: no match is a clean no-op
     * (`200 ignored`), never a fall-through to RAG over the rendered event.
     * Absent -> no trigger can match, so every event is ignored.
     */
    private readonly eventTriggerRegistry?: EventTriggerSelector,
    /**
     * Shared secret integration-gateway signs its sender assertions with
     * (docs/adr/0030 §6).
     *
     * When set, a webhook turn's sender login is accepted ONLY from a verified
     * assertion and the unsigned `event.sender.login` envelope field is ignored.
     * When unset, the body field is trusted -- the pre-assertion behaviour,
     * kept so an existing deployment is not broken by upgrading, and warned
     * about at startup (see index.ts).
     */
    private readonly senderAssertionSecret?: string,
    /**
     * Just-in-time index for consumer-supplied tools (docs/adr/0035), in its own
     * Qdrant collection. Only consulted when a caller sends MORE tools than
     * `callerToolTopK` — below that there is nothing to prune, so the store is
     * skipped and the feature costs nothing. Absent -> a caller sending a large
     * tool array gets an unranked truncation instead of relevance ranking, never
     * an error.
     */
    private readonly callerToolStore?: CallerToolStore,
    /** Max consumer-supplied tools forwarded to the engine (docs/adr/0035 §3). */
    private readonly callerToolTopK: number = 5,
    /**
     * Identity-link flow used for any turn that doesn't name one
     * (`AGENT_DEFAULT_IDENTITY_LINK_FLOW`).
     *
     * Applied in `buildTurnInput`, so an explicit `/invoke`
     * `identity_link_flow` still wins. Defaults to `"authcode"`, which is also
     * the engine's own default, so it is only forwarded when different.
     */
    private readonly defaultIdentityLinkFlow: "device" | "authcode" = "authcode",
    /**
     * Where accept-then-poll records live. Defaults to the in-process Map this
     * used to hard-code; pass a Redis-backed store so a poll can be answered
     * by a replica other than the one that accepted it.
     */
    invocationStore?: InvocationStore,
    /**
     * Authenticates whoever starts a turn (docs/adr/0053 follow-up 1).
     * index.ts always passes one. Absent -> no check, which only unit tests
     * that are not about authentication rely on.
     */
    private readonly callerAuth?: CallerAuthenticator,
  ) {
    this.invocations = invocationStore ?? new InMemoryInvocationStore();
  }

  /**
   * The authentication gate in front of every turn-starting route. Returns
   * true to proceed. On a refusal it has already written the 401, before
   * anything about the turn (trigger matching, caller tools, the engine) runs.
   * Logs the reason, never the credential.
   */
  private async admitCaller(
    req: IncomingMessage,
    res: ServerResponse,
    route: "/invoke" | "/v1/chat/completions",
    forwardedUserToken?: string,
  ): Promise<boolean> {
    if (!this.callerAuth) return true;
    let result: CallerAuthResult;
    try {
      result = await this.callerAuth.authenticate({
        bearer: bearerToken(req.headers.authorization),
        ...(forwardedUserToken ? { forwardedUserToken } : {}),
      });
    } catch (err) {
      // The resolvers fail closed by contract; this is the backstop if one
      // ever throws instead.
      result = { ok: false, reason: `authentication error: ${err instanceof Error ? err.message : String(err)}` };
    }
    if (result.ok) return true;
    if (this.callerAuth.mode === "warn") {
      console.warn(`agent-orchestrator: AGENT_CALLER_AUTH=warn: would reject POST ${route}: ${result.reason}`);
      return true;
    }
    console.warn(`agent-orchestrator: rejected POST ${route}: ${result.reason}`);
    const message = "unauthorized: could not authenticate the caller";
    res
      .writeHead(401, { "content-type": "application/json", "www-authenticate": 'Bearer realm="agent-orchestrator"' })
      .end(JSON.stringify(route === "/v1/chat/completions" ? openAiError(message, "invalid_api_key") : { error: message }));
    return false;
  }

  /**
   * Stops accepting new turns. Called at the top of shutdown, BEFORE the
   * drain: everything already in flight finishes, but a caller asking for new
   * work is told to go elsewhere rather than handed a turn whose poll is
   * about to be killed with this process.
   */
  beginDraining(): void {
    this.draining = true;
  }

  /** Builds the engine input for one turn, folding in the session's sticky read-only flag. */
  private async buildTurnInput(
    request: string,
    sessionId: string | undefined,
    progressListener?: (stage: string, message: string | undefined) => void,
    identityLinkFlow?: "device" | "authcode",
    forwardedUserToken?: string,
    // Deliberately a separate param, not folded into `progressListener` --
    // see `TurnInput.remoteControlUrlListener`.
    remoteControlUrlListener?: (url: string) => void,
    senderLogin?: string,
    /**
     * Consumer-supplied tools for this turn (docs/adr/0035), already resolved by
     * `resolveCallerTools`. Only the chat-completions/invoke facades pass this;
     * every other caller (webhook triage, session-page follow-ups) has no client
     * to execute a tool call, so it stays absent.
     */
    callerTools?: CallerToolTurn,
    /** The EventTrigger-resolved turn, when `/invoke` carried a matching event. */
    trigger?: TriggerTurn,
  ): Promise<TurnInput> {
    const input: TurnInput = { request };
    if (senderLogin) input.senderLogin = senderLogin;
    if (callerTools && callerTools.tools.length > 0) input.callerTools = callerTools.tools;
    if (callerTools?.required) input.callerToolChoiceRequired = true;
    if (callerTools && callerTools.actionHistory.length > 0) input.actionHistory = callerTools.actionHistory;
    if (progressListener) input.progressListener = progressListener;
    if (remoteControlUrlListener) input.remoteControlUrlListener = remoteControlUrlListener;
    const resolvedFlow =
      identityLinkFlow ?? (this.defaultIdentityLinkFlow === "authcode" ? undefined : this.defaultIdentityLinkFlow);
    if (resolvedFlow) input.identityLinkFlow = resolvedFlow;
    if (forwardedUserToken) input.forwardedUserToken = forwardedUserToken;
    if (trigger) {
      input.triggerName = trigger.triggerName;
      if (trigger.forcedSkillId) input.forcedSkillId = trigger.forcedSkillId;
      if (trigger.forcedAgentId) input.forcedAgentId = trigger.forcedAgentId;
      if (trigger.forcedToolId) {
        input.forcedToolId = trigger.forcedToolId;
        input.toolInput = trigger.toolInput ?? "";
      }
      if (trigger.readOnly) input.readOnly = true;
      if (Object.keys(trigger.params).length > 0) input.params = trigger.params;
      if (trigger.identity) input.identityOverride = trigger.identity;
    }
    if (sessionId) input.sessionId = sessionId;
    if (!sessionId || !this.sessionStore) return input;
    const record = await this.sessionStore.get(sessionId);
    // Sticky read-only (docs/event-triggers.md §1): a turn no trigger produced
    // inherits the mode the session's last trigger turn set, so a follow-up
    // can only make a turn MORE restricted. A trigger turn instead brings its
    // own mode, writable included (see `setSessionReadOnly`): triggers with
    // different modes share session keys, and inheriting here would leave a
    // writable triage read-only for good after one review on the same issue.
    // Honored regardless of who wrote the record: honoring a restriction from
    // a record some other caller wrote leaks nothing, while ignoring it would
    // let a follow-up from a different identity -- a session-page user rather
    // than the webhook relay -- run with write access in a conversation that
    // was opened read-only.
    if (record?.readOnly && !trigger) input.readOnly = true;
    return input;
  }

  /**
   * Records the turn's read-only mode on its session BEFORE the turn runs
   * (see `SessionRecord.readOnly`), so the conversation's next turn inherits
   * it. Called for every read-only turn, and for a writable trigger turn,
   * which sets the session writable again.
   */
  private async setSessionReadOnly(sessionId: string, readOnly: boolean): Promise<void> {
    if (!this.sessionStore) return;
    const existing = await this.sessionStore.get(sessionId);
    if ((existing?.readOnly === true) === readOnly) return;
    await this.sessionStore.set(sessionId, readOnly ? { readOnly: true } : {});
  }

  listen(port: number): Promise<void> {
    return new Promise((resolve) => {
      this.server = createServer((req, res) => {
        this.handleRequest(req, res).catch(() => {
          if (!res.writableEnded) res.writeHead(500).end();
        });
      });
      this.server.listen(port, resolve);
    });
  }

  /**
   * Stops accepting new connections and resolves once in-flight requests have
   * finished. Idle keep-alive connections are closed explicitly — without
   * that, `server.close()` waits on sockets that are holding no request at
   * all, and a shutdown that should take milliseconds instead runs out the
   * pod's termination grace period.
   */
  close(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.server) return resolve();
      this.server.close((err) => (err ? reject(err) : resolve()));
      this.server.closeIdleConnections();
    });
  }

  private async handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? "/", "http://localhost");

    if (req.method === "POST" && url.pathname === "/invoke") {
      await this.handleInvoke(req, res);
      return;
    }

    const match = /^\/invoke\/([^/]+)$/.exec(url.pathname);
    if (req.method === "GET" && match) {
      await this.handleGetInvocation(res, match[1] as string);
      return;
    }

    if (req.method === "GET" && url.pathname === "/v1/models") {
      res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(listModelsResponse()));
      return;
    }

    if (req.method === "POST" && url.pathname === "/v1/chat/completions") {
      await this.handleChatCompletions(req, res);
      return;
    }

    if (req.method === "GET" && url.pathname === "/sessions/live") {
      this.handleSessionLive(req, res, url);
      return;
    }

    res.writeHead(404).end();
  }

  /**
   * `GET /sessions/live?sessionId=...` -> always `{ live: false }`.
   *
   * The live-session tunnel (ADR 0026) proxied into an agent Pod the
   * in-process engine had launched; the Temporal engine launches and owns its
   * runs, so this process never has one to tunnel into. It says nothing about
   * whether a turn is still running -- integration-gateway's label reconciler
   * must not use it as a terminal signal (it once did, and stripped labels
   * from runs still in progress); it uses the gateway's own in-flight records.
   */
  private handleSessionLive(req: IncomingMessage, res: ServerResponse, url: URL): void {
    if (!bearerToken(req.headers.authorization)) {
      res.writeHead(401).end();
      return;
    }
    if (!url.searchParams.get("sessionId")) {
      res.writeHead(400, { "content-type": "application/json" }).end(JSON.stringify({ error: "sessionId query param is required" }));
      return;
    }
    res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ live: false }));
  }

  /**
   * Persist a terminal invocation record from the fire-and-forget turn chain
   * without ever rejecting.
   *
   * These writes happen on the `void`-ed chain in `handleInvoke`, and
   * {@link RedisInvocationStore.set} deliberately PROPAGATES its error (a lost
   * terminal record is the failure the durable store exists to remove, so it
   * surfaces rather than swallows). But a rejection escaping the void-ed chain
   * is an unhandled promise rejection, and Node >=20 terminates the process by
   * default -- which would wipe every OTHER in-flight record on this pod and
   * reintroduce the `poll failed: 404` by a worse route than the one durability
   * closed. So the terminal writes here log-and-continue: a Redis blip at
   * terminal-write time costs THIS record its answer, not the whole pod. The
   * initial `pending` write (in the request path, not the void-ed chain) still
   * surfaces its error as a 500 to the caller, who simply retries.
   */
  /**
   * Writes a settled turn's terminal record -- one mapping, shared by the
   * process that ran the turn and one that adopted it. Carries forward the
   * remoteControlUrl the listener recorded, since this write replaces the
   * whole record.
   */
  private async persistTerminalOutcome(id: string, outcome: TurnOutcome, forcedAgent: boolean): Promise<void> {
    const remoteControlUrl = (await this.invocations.get(id))?.remoteControlUrl;
    const runOutcome = !outcome.error && forcedAgent ? acceptedOutcome(id, outcome.runOutcome) : undefined;
    await this.persistTerminalInvocation(id, {
      id,
      status: outcome.error ? "failed" : "succeeded",
      result: outcome.result,
      error: outcome.error,
      ...(remoteControlUrl ? { remoteControlUrl } : {}),
      ...(runOutcome ? { outcome: runOutcome } : {}),
      ...((outcome.pendingToolCalls ?? []).length > 0 ? { pendingToolCalls: outcome.pendingToolCalls } : {}),
    });
  }

  /**
   * Finishes a pending turn whose process died (a rollout, an OOM): the
   * engine still holds the turn, so polling it by the engine id the dead
   * process recorded gets the same answer. Started by a poll that finds the
   * record pending with nobody here driving it. Two processes adopting the
   * same turn while a rollout overlaps them is harmless: both poll the same
   * engine record and write the same terminal record, and nothing else
   * happens here -- every side effect of a finished turn is the caller's.
   */
  private adoptInvocation(record: InvocationRecord): void {
    const { id, engineInvocationId } = record;
    if (!engineInvocationId || !this.engine.adopt || this.driving.has(id)) return;
    this.driving.add(id);
    console.log(`agent-orchestrator: adopting orphaned invocation ${id} (engine ${engineInvocationId})`);
    const input: TurnInput = {
      request: "",
      // As on the original turn: records a Remote Control link that surfaces
      // after the takeover.
      remoteControlUrlListener: (url: string) => {
        void (async () => {
          const current = await this.invocations.get(id);
          if (current && current.status === "pending") await this.invocations.set(id, { ...current, remoteControlUrl: url });
        })();
      },
    };
    void this.engine
      .adopt(engineInvocationId, input)
      .then((outcome) => this.persistTerminalOutcome(id, outcome, record.forcedAgent === true))
      .catch((err: unknown) =>
        this.persistTerminalInvocation(id, { id, status: "failed", error: err instanceof Error ? err.message : String(err) }),
      )
      .finally(() => this.driving.delete(id));
  }

  private async persistTerminalInvocation(id: string, record: InvocationRecord): Promise<void> {
    try {
      await this.invocations.set(id, record);
    } catch (err) {
      console.error(
        `agent-orchestrator: failed to persist terminal invocation record ${id}:`,
        err instanceof Error ? err.message : String(err),
      );
    }
  }

  /**
   * docs/adr/0050 §4: one server span per `/invoke`, continuing the caller's
   * traceparent (integration-gateway's delivery span) when valid. The turn --
   * and the engine calls it makes -- carry THIS span's context, so the
   * engine's spans nest under it. The span ends with the answer (202/403/...),
   * not with the turn, which outlives it.
   */
  private async handleInvoke(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const span = startSpan("POST /invoke", headerValue(req.headers[TRACEPARENT_HEADER]), {
      kind: "server",
      attributes: { "http.request.method": "POST", "http.route": "/invoke" },
    });
    try {
      await this.handleInvokeTraced(req, res, span);
    } catch (error) {
      span.fail("internal_error");
      throw error;
    } finally {
      span.set({ "http.response.status_code": res.statusCode });
      span.end();
    }
  }

  private async handleInvokeTraced(req: IncomingMessage, res: ServerResponse, span: TraceSpan): Promise<void> {
    const rawBody = await readBody(req);
    if (!(await this.admitCaller(req, res, "/invoke"))) {
      span.fail("unauthorized");
      return;
    }

    let request: string;
    let sessionId: string | undefined;
    let identityLinkFlow: "device" | "authcode" | undefined;
    let senderLogin: string | undefined;
    let senderClaims: SenderClaims | undefined;
    const traceparent = span.traceparent;
    let envelope: Envelope | undefined;
    // Consumer-supplied tools (docs/adr/0035), accepted here too so a
    // programmatic `/invoke` caller has parity with the chat facade. The raw
    // fields are captured inside the parse block and resolved after it, since
    // resolution is async (it may hit the caller-tool index) while this block is
    // deliberately synchronous.
    let rawTools: unknown;
    let rawToolChoice: unknown;
    try {
      const parsed: unknown = rawBody ? JSON.parse(rawBody) : {};
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) throw new Error("invalid body");
      const body = parsed as Record<string, unknown>;

      // An event envelope (ADR 0048, docs/event-triggers.md §2/§5). Present ->
      // this is an event turn: `request` is optional (the matched trigger's
      // rendered prompt replaces it) and a malformed envelope is a 400, never
      // quietly run as a conversational turn over whatever `request` says.
      if (body.event !== undefined) {
        const parsedEnvelope = parseEnvelope(body.event);
        if ("error" in parsedEnvelope) {
          res.writeHead(400, { "content-type": "application/json" }).end(JSON.stringify({ error: parsedEnvelope.error }));
          return;
        }
        envelope = parsedEnvelope;
      }
      if (!envelope && (typeof body.request !== "string" || body.request.trim() === "")) throw new Error("invalid body");
      request = typeof body.request === "string" ? body.request : "";
      const rawSessionId = body.session_id;
      sessionId = typeof rawSessionId === "string" && rawSessionId.trim() !== "" ? rawSessionId : undefined;
      // Power-user/headless-caller convenience field, not required -- an
      // absent or invalid value is silently ignored (the engine's own
      // "authcode" default applies) rather than 400ing the whole request.
      const rawFlow = body.identity_link_flow;
      identityLinkFlow = rawFlow === "device" || rawFlow === "authcode" ? rawFlow : undefined;
      rawTools = body.tools;
      rawToolChoice = body.tool_choice;

      // WHERE the sender login is trusted from depends on configuration
      // (docs/adr/0030 §6). It selects the caller's principal, and therefore
      // which stored credentials the run receives, so with an assertion secret
      // configured ONLY a signed assertion is accepted and the envelope's
      // `sender.login` is ignored entirely -- otherwise anything holding the
      // gateway's /invoke token could name an arbitrary login and be handed
      // that person's credentials.
      if (this.senderAssertionSecret) {
        senderClaims = verifySenderClaims(this.senderAssertionSecret, headerValue(req.headers[SENDER_ASSERTION_HEADER]));
        senderLogin = senderClaims?.login;
      } else if (envelope) {
        const login = envelope.sender.login;
        if (typeof login === "string" && login.trim() !== "") senderLogin = login;
      }
    } catch {
      res.writeHead(400, { "content-type": "application/json" }).end(
        JSON.stringify({ error: 'body must be JSON: { "request": "<non-empty string>" } or { "event": <envelope> }' }),
      );
      return;
    }

    // ── EventTrigger selection (docs/event-triggers.md §4/§5) ──────────────
    // Resolved here, once, by the shared contract package: the engine is
    // handed the resolved target, never the envelope.
    let trigger: TriggerTurn | undefined;
    let triggerResponse: TriggerResponse | undefined;
    let targetRepository: string | undefined;
    if (envelope) {
      const selection = this.eventTriggerRegistry?.select(envelope, sessionId ? { callerSessionId: sessionId } : {}) ?? {
        status: "ignored" as const,
      };
      span.set({ "aktis.event.outcome": selection.status });
      if (selection.status === "ignored") {
        // A clean no-op: no turn, no invocation record, and deliberately no
        // RAG fallback -- a Stripe or CI webhook that matches nothing must not
        // be interpreted as free text (ADR 0048).
        res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ status: "ignored" }));
        return;
      }
      if (selection.status === "failed") {
        res
          .writeHead(422, { "content-type": "application/json" })
          .end(JSON.stringify({ status: "failed", error: `event trigger ${selection.trigger}: ${selection.error}` }));
        return;
      }
      const { target } = selection;
      triggerResponse = {
        name: selection.trigger,
        sessionId: selection.sessionId,
        reply: selection.reply,
        params: selection.stringParams,
        ...(selection.onComplete ? { onComplete: selection.onComplete } : {}),
      };
      if (target.kind === "none") {
        await this.acceptNoneTurn(res, span, triggerResponse);
        return;
      }
      // A toolRef trigger needs no promptTemplate, but the turn still needs a
      // request (it is what the session, run annotations and logs show), so it
      // gets a short descriptive one -- never text lifted from the event body.
      request = selection.prompt !== "" ? selection.prompt : `Run tool ${target.ref ?? ""}`.trim();
      sessionId = selection.sessionId;
      targetRepository = envelope.repository !== "" ? envelope.repository : undefined;
      trigger = {
        triggerName: selection.trigger,
        ...(target.kind === "skill" ? { forcedSkillId: target.ref } : {}),
        ...(target.kind === "agent" ? { forcedAgentId: target.ref } : {}),
        ...(target.kind === "tool" ? { forcedToolId: target.ref, toolInput: selection.toolInput } : {}),
        readOnly: target.readOnly,
        params: selection.stringParams,
        ...(target.identity ? { identity: target.identity } : {}),
      };
    }

    // `/invoke` has no prior-tool-call history to read: it takes a single
    // `request` string, not a message array, so a caller resuming a tool call
    // here has nowhere to have put the result. Tool offering works; the
    // round-trip resume is chat-facade-only (see the app README's known gaps).
    const callerTools = await this.resolveCallerToolTurn(request, rawTools, rawToolChoice, []);
    if ("error" in callerTools) {
      res.writeHead(400, { "content-type": "application/json" }).end(JSON.stringify({ error: callerTools.error }));
      return;
    }

    if (this.draining) {
      // 503 + Retry-After, not a queued 202: this process is going away and the
      // poll for anything accepted now would die with it. A caller that
      // retries reaches the replacement pod; one handed a 202 would poll a
      // record nobody is ever going to complete.
      res
        .writeHead(503, { "content-type": "application/json", "retry-after": "1" })
        .end(JSON.stringify({ error: "orchestrator is shutting down; retry" }));
      return;
    }

    // Accept-phase identity check (docs/adr/0049 §6): a trigger turn whose
    // agent must act as its webhook sender, who cannot be acted as, is refused
    // NOW, synchronously, so the webhook caller can resolve it -- not accepted
    // and then failed where nobody is listening.
    if (trigger?.forcedAgentId && senderLogin) {
      const refusal = await this.engine.checkIdentity({
        request,
        ...(sessionId ? { sessionId } : {}),
        senderLogin,
        ...(senderClaims ? { senderClaims } : {}),
        ...(targetRepository ? { targetRepository } : {}),
        triggerName: trigger.triggerName,
        forcedAgentId: trigger.forcedAgentId,
        ...(trigger.readOnly ? { readOnly: true } : {}),
        ...(Object.keys(trigger.params).length > 0 ? { params: trigger.params } : {}),
        ...(trigger.identity ? { identityOverride: trigger.identity } : {}),
        traceparent,
      });
      if (refusal) {
        span.set({ "aktis.identity_error": refusal.error.code });
        span.fail(refusal.error.code);
        res.writeHead(403, { "content-type": "application/json" }).end(
          JSON.stringify({ ...refusal, error: { ...refusal.error, ...(triggerResponse ? { trigger: triggerResponse } : {}) } }),
        );
        return;
      }
    }

    const id = randomUUID();
    await this.invocations.set(id, { id, status: "pending", ...(trigger?.forcedAgentId ? { forcedAgent: true } : {}) });
    // This process is driving the turn; until it settles, a poll must not
    // adopt it (see handleGetInvocation).
    this.driving.add(id);

    // Fire-and-forget: the HTTP response returns immediately; the engine poll
    // updates the record when the turn eventually settles.
    void this.buildTurnInput(
      request,
      sessionId,
      // Deliberately `undefined` -- this is a fire-and-forget caller with no
      // live channel, so `progressListener` must stay unset (see
      // `TurnInput.progressListener`): setting it makes the engine adapter
      // hold the turn open auto-resuming an identity link for minutes.
      undefined,
      identityLinkFlow,
      undefined,
      // Tracks the latest Remote Control URL onto this invocation's record
      // (see `InvocationRecord.remoteControlUrl`). Only mutate while still
      // pending -- never clobber a terminal record.
      async (url) => {
        const current = await this.invocations.get(id);
        if (current && current.status === "pending") {
          await this.invocations.set(id, { ...current, remoteControlUrl: url });
        }
      },
      senderLogin,
      callerTools,
      trigger,
    ).then(async (turnInput) => {
      if (targetRepository) turnInput.targetRepository = targetRepository;
      if (senderClaims) turnInput.senderClaims = senderClaims;
      turnInput.traceparent = traceparent;
      // Persist the turn's read-only mode BEFORE anything launches (see
      // `setSessionReadOnly`): a read-only turn's, and a trigger turn's
      // either way. Awaited, and a failure fails the turn: the flag's job is
      // to make later turns read-only, and a turn that can't record that must
      // not go on to launch runs the next turn won't know were read-only.
      // (The Redis adapter swallows its own errors by contract, so this only
      // bites a store that actually throws.)
      if (sessionId && (turnInput.readOnly || trigger)) await this.setSessionReadOnly(sessionId, turnInput.readOnly === true);
      // Recorded so another process can finish the turn if this one dies
      // (see InvocationRecord.engineInvocationId). Only while still pending.
      turnInput.reportEngineInvocation = async (engineInvocationId) => {
        const current = await this.invocations.get(id);
        if (current && current.status === "pending") {
          await this.invocations.set(id, { ...current, engineInvocationId });
        }
      };
      return this.engine
        .invoke(turnInput)
        .then((outcome) => this.persistTerminalOutcome(id, outcome, Boolean(trigger?.forcedAgentId)))
        .catch(async (err: unknown) => {
          const remoteControlUrl = (await this.invocations.get(id))?.remoteControlUrl;
          await this.persistTerminalInvocation(id, {
            id,
            status: "failed",
            error: err instanceof Error ? err.message : String(err),
            ...(remoteControlUrl ? { remoteControlUrl } : {}),
          });
        });
    }).catch(async (err: unknown) => {
      // `buildTurnInput` itself can reject (e.g. `sessionStore.get` hitting
      // Redis) before the turn ever starts. Without this, that rejection is
      // unhandled -- Node terminates the process, wiping the in-memory
      // `invocations` Map and turning every in-flight poll (not just this
      // one) into a 404 the caller reports as "poll failed: 404".
      await this.persistTerminalInvocation(id, {
        id,
        status: "failed",
        error: err instanceof Error ? err.message : String(err),
      });
    }).finally(() => this.driving.delete(id));

    // An event turn also tells the caller WHICH trigger it started and with
    // what already-rendered reply side-effects (docs/event-triggers.md §5) --
    // integration-gateway's reply adapters act on exactly this, never on
    // anything they re-derive from the event.
    span.set({ "aktis.invocation_id": id, ...(triggerResponse ? { "aktis.trigger": triggerResponse.name } : {}) });
    res.writeHead(202, { "content-type": "application/json", location: `/invoke/${id}` }).end(
      JSON.stringify({ id, status: "pending", ...(triggerResponse ? { trigger: triggerResponse } : {}) }),
    );
  }

  /**
   * A `target.none` trigger (docs/adr/0052 §3): matched and accepted like any
   * event turn, so integration-gateway applies its reply side effects (labels,
   * assignees, ack) and, with `onComplete`, emits the completion event -- but
   * NOTHING runs. No engine turn, LLM, AgentRun or tool, and so also no identity
   * pre-flight: there is no agent to act as anyone, so a none trigger can never
   * 403. The record is written already `succeeded` with an empty result, so
   * the gateway's first poll finishes the turn.
   */
  private async acceptNoneTurn(res: ServerResponse, span: TraceSpan, trigger: TriggerResponse): Promise<void> {
    if (this.draining) {
      // Same reason as a real turn's 503: the record may live only in this
      // process's memory, and the replacement pod would 404 its poll.
      res
        .writeHead(503, { "content-type": "application/json", "retry-after": "1" })
        .end(JSON.stringify({ error: "orchestrator is shutting down; retry" }));
      return;
    }
    const id = randomUUID();
    await this.invocations.set(id, { id, status: "succeeded", result: "" });
    span.set({ "aktis.invocation_id": id, "aktis.trigger": trigger.name, "aktis.trigger.target": "none" });
    res
      .writeHead(202, { "content-type": "application/json", location: `/invoke/${id}` })
      .end(JSON.stringify({ id, status: "pending", trigger }));
  }

  private async handleGetInvocation(res: ServerResponse, id: string): Promise<void> {
    const record = await this.invocations.get(id);
    if (!record) {
      res.writeHead(404).end();
      return;
    }
    if (record.status === "pending" && !this.draining) this.adoptInvocation(record);
    // The engine id and dispatch kind are this process's bookkeeping, not
    // part of the contract.
    const { engineInvocationId: _engine, forcedAgent: _forced, ...visible } = record;
    res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(visible));
  }

  private async handleChatCompletions(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const rawBody = await readBody(req);
    // Before everything else, including Open WebUI's internal housekeeping
    // completions below: those spend model tokens too.
    if (!(await this.admitCaller(req, res, "/v1/chat/completions", headerValue(req.headers[FORWARDED_USER_JWT_HEADER])))) {
      return;
    }
    let parsed: {
      messages?: unknown;
      model?: unknown;
      stream?: unknown;
      tools?: unknown;
      tool_choice?: unknown;
      identity_link_flow?: unknown;
    };
    try {
      parsed = rawBody ? (JSON.parse(rawBody) as typeof parsed) : {};
    } catch {
      res.writeHead(400, { "content-type": "application/json" }).end(
        JSON.stringify(openAiError("body must be valid JSON", "invalid_request")),
      );
      return;
    }

    const built = buildAgentRequest(parsed.messages);
    if (!built) {
      res.writeHead(400, { "content-type": "application/json" }).end(
        JSON.stringify(openAiError('messages must include a non-empty "user" message', "invalid_request")),
      );
      return;
    }
    const { request, priorToolCalls } = built;

    const model = typeof parsed.model === "string" && parsed.model ? parsed.model : MODEL_ID;
    const sessionId = headerValue(req.headers[CHAT_ID_HEADER]);
    const forwardedUserToken = headerValue(req.headers[FORWARDED_USER_JWT_HEADER]);
    const stream = parsed.stream === true;
    // Same optional field, and the same silently-ignored-if-invalid handling,
    // as `/invoke`'s. Open WebUI never sends it; it is for an OpenAI-SDK
    // client that has no browser to finish an authcode redirect in, and it
    // arrives via the SDK's `extra_body`.
    const identityLinkFlow =
      parsed.identity_link_flow === "device" || parsed.identity_link_flow === "authcode"
        ? parsed.identity_link_flow
        : undefined;

    // Open WebUI's own housekeeping completions (title/tags/query/follow-up
    // generation) must NEVER reach the engine — see
    // isInternalUiTaskRequest. Answered directly, with no delegation, no
    // identity resolution, and no session mutation, regardless of `stream`.
    //
    // This check stays AHEAD of caller-tool parsing on purpose (docs/adr/0035
    // §5): Open WebUI sends these to the same endpoint with the same body, so a
    // title-generation request that happens to carry the chat's tool array must
    // come back as prose. Emitting a tool call here would have the client
    // execute a real function as a side effect of rendering a chat title.
    if (isInternalUiTaskRequest(request)) {
      await this.handleInternalUiTask(res, parsed.messages, model, stream);
      return;
    }

    // Consumer-supplied tools (docs/adr/0035). Rejected loudly rather than
    // dropped: a client that offers tools and silently never gets a tool call
    // has no way to tell "the agent chose not to" from "the agent never saw
    // them".
    const callerTools = await this.resolveCallerToolTurn(request, parsed.tools, parsed.tool_choice, priorToolCalls);
    if ("error" in callerTools) {
      res.writeHead(400, { "content-type": "application/json" }).end(
        JSON.stringify(openAiError(callerTools.error, "invalid_request")),
      );
      return;
    }

    if (!stream) {
      await this.handleChatCompletionsBlocking(
        res,
        request,
        model,
        sessionId,
        forwardedUserToken,
        callerTools,
        identityLinkFlow,
      );
      return;
    }
    await this.handleChatCompletionsStreaming(
      res,
      request,
      model,
      sessionId,
      forwardedUserToken,
      callerTools,
      identityLinkFlow,
    );
  }

  /**
   * Validates the request's `tools`/`tool_choice` and resolves which of them
   * reach the engine (docs/adr/0035) — the just-in-time vectorization step,
   * skipped entirely when the caller sent few enough tools that there is nothing
   * to prune.
   *
   * Also maps prior client-executed calls into {@link PriorToolCall}s under the
   * same `caller:`-namespaced ids they were originally offered under.
   */
  private async resolveCallerToolTurn(
    request: string,
    rawTools: unknown,
    rawToolChoice: unknown,
    priorToolCalls: PriorCallerToolCall[],
  ): Promise<CallerToolTurn | { error: string }> {
    const parsed = parseCallerTools(rawTools, rawToolChoice);
    if ("error" in parsed) return parsed;
    const actionHistory: PriorToolCall[] = priorToolCalls.map((call) => ({
      toolId: callerToolId(call.name),
      toolArgs: call.arguments,
      result: call.result,
    }));
    if (parsed.tools.length === 0) return { tools: [], required: false, actionHistory };

    const resolved = await resolveCallerTools(
      request,
      parsed.tools,
      parsed.choice,
      this.callerToolTopK,
      this.callerToolStore,
      (message, err) => console.warn(`[caller-tools] ${message}`, err ?? ""),
    );
    return {
      tools: resolved,
      required: parsed.choice.kind === "auto" && parsed.choice.required === true,
      actionHistory,
    };
  }

  /**
   * Answers an Open WebUI internal housekeeping request (see
   * `isInternalUiTaskRequest`) with a direct, non-agentic completion —
   * bypassing the engine entirely. Falls back to a generic static reply
   * when no `taskCompleter` is configured; safety (never delegating) matters
   * more here than title quality.
   */
  private async handleInternalUiTask(
    res: ServerResponse,
    messages: unknown,
    model: string,
    stream: boolean,
  ): Promise<void> {
    let content: string;
    try {
      content = this.taskCompleter ? await this.taskCompleter.complete(messages, model) : "";
    } catch {
      content = "";
    }
    const id = chatCompletionId();
    if (!stream) {
      res.writeHead(200, { "content-type": "application/json" }).end(
        JSON.stringify(chatCompletionResponse(id, model, content, "stop")),
      );
      return;
    }
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
    });
    res.flushHeaders?.();
    writeSseChunk(res, chatCompletionChunk(id, model, { content }, null));
    writeSseChunk(res, chatCompletionChunk(id, model, {}, "stop"));
    writeSseDone(res);
    res.end();
  }

  private async handleChatCompletionsBlocking(
    res: ServerResponse,
    request: string,
    model: string,
    sessionId: string | undefined,
    forwardedUserToken?: string,
    callerTools?: CallerToolTurn,
    identityLinkFlow?: "device" | "authcode",
  ): Promise<void> {
    // A Remote Control session URL is deliberately NOT surfaced on the
    // non-streaming path: RC's whole value is a LIVE session to watch/steer,
    // and a blocking response only returns once the turn is already complete
    // (nothing live left to join). The streaming path -- the actual Open WebUI
    // chat surface -- streams it inline as it arrives (see
    // handleChatCompletionsStreaming).
    const turnInput = await this.buildTurnInput(
      request,
      sessionId,
      undefined,
      identityLinkFlow,
      forwardedUserToken,
      undefined,
      undefined,
      callerTools,
    );
    const outcome = await this.engine.invoke(turnInput);
    if (outcome.error) {
      const { status, code } = errorStatusAndCode(outcome.error);
      res.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(openAiError(outcome.error, code)));
      return;
    }
    const id = chatCompletionId();
    // The turn is asking the CALLER to run a tool (docs/adr/0035): hand back
    // `tool_calls` + `finish_reason: "tool_calls"` instead of an answer. Checked
    // before `result` because this turn deliberately produced no assistant text —
    // rendering it as a normal completion would show an empty message and the
    // client would never execute anything.
    if (outcome.pendingToolCalls && outcome.pendingToolCalls.length > 0) {
      res.writeHead(200, { "content-type": "application/json" }).end(
        JSON.stringify(chatCompletionToolCallResponse(id, model, outcome.pendingToolCalls)),
      );
      return;
    }
    const content = renderResult(outcome.result);
    res.writeHead(200, { "content-type": "application/json" }).end(
      JSON.stringify(chatCompletionResponse(id, model, content, "stop")),
    );
  }

  private async handleChatCompletionsStreaming(
    res: ServerResponse,
    request: string,
    model: string,
    sessionId: string | undefined,
    forwardedUserToken?: string,
    callerTools?: CallerToolTurn,
    identityLinkFlow?: "device" | "authcode",
  ): Promise<void> {
    const id = chatCompletionId();
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
    });
    res.flushHeaders?.();

    // Tracks the most recent in-progress ("done: false") status step so it
    // can be closed out before the stream ends -- otherwise Open WebUI's
    // StatusHistory spinner is left stuck on that step forever, since a
    // "done: false" event is never followed by a matching "done: true" one
    // for the same step once the agent turn completes.
    let openStatusLabel: string | undefined;
    // Tracks whether the last piece of real content written to the stream
    // ended in whitespace. Each "agent-text" progress call is one COMPLETE,
    // independent text block straight from claude-runner.ts's stream-json
    // parsing (one per Claude message content block) -- the engine never
    // joins successive blocks with a separator anywhere upstream of here, so
    // writing them verbatim back-to-back fuses "...account." and "Found it"
    // into one run-on "...account.Found it" with nothing between. The
    // remote-control-url branch below already guards against exactly this
    // ("an earlier streamed chunk may not end in a newline"); this extends
    // the same guard to agent-text/identity-link and the final result.
    let lastContentEndedWithSpace = true; // nothing streamed yet -- no separator needed before the first chunk
    const withSeparator = (content: string): string => {
      const needsSeparator = !lastContentEndedWithSpace && !/^\s/.test(content);
      lastContentEndedWithSpace = content === "" ? lastContentEndedWithSpace : /\s$/.test(content);
      return needsSeparator ? `\n\n${content}` : content;
    };
    const finish = (content: string): void => {
      if (openStatusLabel !== undefined) {
        writeSseStatus(res, openStatusLabel, true);
        openStatusLabel = undefined;
      }
      writeSseChunk(res, chatCompletionChunk(id, model, { content: withSeparator(content) }, null));
      writeSseChunk(res, chatCompletionChunk(id, model, {}, "stop"));
      writeSseDone(res);
      res.end();
    };

    /**
     * Terminal variant for a turn that ends by asking the CALLER to run a tool
     * (docs/adr/0035). Separate from `finish` because the two differ in more than
     * content: this emits a `tool_calls` delta and finishes with
     * `finish_reason: "tool_calls"`, which is what tells the client to execute
     * something rather than render an answer. Any open status spinner is still
     * closed out the same way.
     */
    const finishWithToolCalls = (calls: PendingToolCall[]): void => {
      if (openStatusLabel !== undefined) {
        writeSseStatus(res, openStatusLabel, true);
        openStatusLabel = undefined;
      }
      writeSseChunk(res, toolCallDeltaChunk(id, model, calls));
      writeSseChunk(res, chatCompletionChunk(id, model, {}, "tool_calls"));
      writeSseDone(res);
      res.end();
    };

    try {
      const turnInput = await this.buildTurnInput(request, sessionId, (stage, message) => {
        // "agent-text" (opencode-swe-agent/src/index.ts) is the delegated
        // agent's own narrative — its actual reasoning/commentary as it
        // works. Stream that live as real chat content so the user reads it
        // like normal assistant prose, rather than burying it in the
        // collapsible status spinner with the mechanical tool-call noise.
        // "identity-link" (TemporalEngine.autoResumeLink) is the one-time
        // "please link your GitHub account" prompt -- also real content, not
        // a status step, since the status label is truncated to 120 chars
        // and would mangle the markdown link/URL.
        // "remote-control-url" (claude-code-swe-agent's live Remote Control
        // session link) is likewise streamed as real content -- a clickable
        // "watch live / take over" link -- for the same reason (a truncated
        // status label would mangle the URL). This gives a chat turn the same
        // live Remote Control surface the GitHub triage path already posts
        // (integration-gateway's relayAndReply), replacing the deprecated
        // session-page link for Claude-backed sessions.
        // Any of these three content branches means the agent has moved past
        // whatever mechanical step last set `openStatusLabel` (e.g. "running
        // Bash") into producing real prose -- close that step out now rather
        // than leaving it open until `finish()`. Otherwise a long bridged
        // agent run that does most of its remaining work as agent-text (no
        // further tool-status callbacks) leaves Open WebUI's StatusHistory
        // spinner showing a stale, superseded step for the rest of the turn,
        // even once real content is visibly well past it.
        if (stage === "remote-control-url" && message) {
          if (openStatusLabel !== undefined) {
            writeSseStatus(res, openStatusLabel, true);
            openStatusLabel = undefined;
          }
          // Trailing blank line so this link stands as its own paragraph;
          // withSeparator supplies the leading one only if actually needed.
          writeSseChunk(
            res,
            chatCompletionChunk(id, model, { content: withSeparator(`🤖 Watch live or take over this session here: ${message}\n\n`) }, null),
          );
          return;
        }
        if ((stage === "agent-text" || stage === "identity-link") && message) {
          if (openStatusLabel !== undefined) {
            writeSseStatus(res, openStatusLabel, true);
            openStatusLabel = undefined;
          }
          writeSseChunk(res, chatCompletionChunk(id, model, { content: withSeparator(message) }, null));
          return;
        }
        // Everything else (tool invocations, warnings, pipeline stage
        // transitions) is mechanical bookkeeping -- an Open WebUI status step
        // (collapsible StatusHistory spinner) while the Job runs. Routed through
        // the typed lifecycle envelope (ADR 0004): `renderTurnEvent` reproduces
        // the exact same label this branch used to build inline, so the status
        // string the client sees is unchanged — the envelope is just the single
        // typed source it is projected from.
        const label = renderTurnEvent(turnEvent("tool-progress", { stage, message }));
        openStatusLabel = label;
        writeSseStatus(res, label, false);
      }, identityLinkFlow, forwardedUserToken, undefined, undefined, callerTools);
      // Started now and awaited inside the iterable, so `withHeartbeat` is
      // already racing it when the first keep-alive is due: a turn can run for
      // many minutes with no progress line, and an idle SSE connection would
      // otherwise be cut by whatever sits upstream (ingress, load balancer,
      // browser) while the turn itself carried on.
      const turn = this.engine.invoke(turnInput);
      const source: AsyncIterable<Awaited<typeof turn>> = {
        async *[Symbol.asyncIterator]() {
          yield await turn;
        },
      };
      for await (const item of withHeartbeat(source, HEARTBEAT_MS)) {
        if (item.type === "heartbeat") {
          writeSseComment(res, "keep-alive");
          continue;
        }
        const outcome = item.value;
        if (outcome.error) {
          finish(`❌ ${outcome.error}`);
          return;
        }
        // The turn asked the CALLER to execute a tool (docs/adr/0035): the
        // answer is with the client now, and there is no `result` to render.
        if (outcome.pendingToolCalls && outcome.pendingToolCalls.length > 0) {
          finishWithToolCalls(outcome.pendingToolCalls);
          return;
        }
        // Neither an answer, an error nor tool calls: fall through to the
        // explicit failure below rather than render `undefined` as an answer.
        if (outcome.result !== undefined) {
          finish(renderResult(outcome.result));
          return;
        }
      }
      finish("❌ agent stream ended unexpectedly");
    } catch (err) {
      finish(`❌ ${err instanceof Error ? err.message : String(err)}`);
    }
  }
}

function bearerToken(header: string | undefined): string {
  if (!header) return "";
  const [scheme, token] = header.split(" ");
  return scheme?.toLowerCase() === "bearer" && token ? token : "";
}

/** Normalizes a possibly-repeated header to its first non-empty value. */
function headerValue(raw: string | string[] | undefined): string | undefined {
  const value = Array.isArray(raw) ? raw[0] : raw;
  return value && value.trim() !== "" ? value : undefined;
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}
