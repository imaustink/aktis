import type { CallerToolDescriptor, PendingToolCall } from "../caller-tools/types.js";
import type { SenderClaims } from "../rbac/sender-assertion.js";

/**
 * A tool call the CLIENT already executed for the exchange in flight
 * (docs/adr/0035 §1), in the `caller:`-namespaced id form it was offered under.
 */
export interface PriorToolCall {
  toolId: string;
  toolArgs: string;
  result: string;
}

/**
 * Everything the facade hands the engine for one turn. The engine holds the
 * conversation's own state (docs/adr/0036, 0053), so this is only what the
 * facade resolved from the request itself.
 */
export interface TurnInput {
  request: string;
  /**
   * Open WebUI's per-request signed user JWT (`X-OpenWebUI-User-Jwt`), if the
   * caller sent one -- see `OpenWebUiForwardedUserResolver`.
   */
  forwardedUserToken?: string;
  /** Conversation id: the Open WebUI chat-id header, `/invoke`'s `session_id`, or a trigger's session key. */
  sessionId?: string;
  /**
   * Tools the CONSUMER supplied in this request (docs/adr/0035), already parsed,
   * validated and relevance-pruned to top-K. Executed by the caller's own client,
   * never here.
   */
  callerTools?: CallerToolDescriptor[];
  /** The caller sent `tool_choice: "required"` — a directive, not a guarantee (docs/adr/0035 §5). */
  callerToolChoiceRequired?: boolean;
  /** Tool calls the client already executed for this exchange (docs/adr/0035 §1). */
  actionHistory?: PriorToolCall[];
  /**
   * Which OAuth flow an identity-link prompt starts. Absent -> the engine's
   * own default ("authcode").
   */
  identityLinkFlow?: "device" | "authcode";
  /**
   * Live progress relay, set only by the SSE streaming handler. Its presence is
   * also the "this caller has somewhere to wait" signal (see
   * `TemporalEngine.autoResumeLink`), so a fire-and-forget caller must never set it.
   */
  progressListener?: (stage: string, message: string | undefined) => void;
  /**
   * Receives the delegated agent's Remote Control session URL. Separate from
   * `progressListener` because a fire-and-forget `/invoke` caller wants this
   * URL without being treated as a live channel.
   */
  remoteControlUrlListener?: (url: string) => void;
  /** Id of a Skill CR an EventTrigger dispatches to directly (ADR 0048). At most one forced target is set. */
  forcedSkillId?: string;
  /** Id of an Agent CR an EventTrigger dispatches to directly — see `forcedSkillId`. */
  forcedAgentId?: string;
  /** Id of a Tool CR to run exactly once with `toolInput` — see `forcedSkillId`. */
  forcedToolId?: string;
  /** The forced tool's single string input (docs/event-triggers.md §1). */
  toolInput?: string;
  /** Read-only turn: the trigger's `target.readOnly`, else the session's sticky flag. */
  readOnly?: boolean;
  /** The trigger's `target.identity` (docs/adr/0049), forwarded inside the signed dispatch assertion. */
  identityOverride?: string;
  /** Everything integration-gateway vouched for about a webhook sender (docs/adr/0049 §5). */
  senderClaims?: SenderClaims;
  /** W3C trace context this turn joins (docs/adr/0050 §4). */
  traceparent?: string;
  /** The trigger's params in §4 string form -> `AgentRun.spec.params` of the targeted agent. */
  params?: Record<string, string>;
  /** Name of the EventTrigger that started this turn. */
  triggerName?: string;
  /**
   * GitHub login of the human who triggered this turn: the verified sender
   * assertion, or (no assertion secret configured) the envelope's
   * `sender.login`.
   */
  senderLogin?: string;
  /** The `owner/repo` a relayed webhook event fired on — the engine's read gate uses it (docs/adr/0042). */
  targetRepository?: string;
}

/** How a turn ended. Exactly one of the three is meaningful. */
export interface TurnOutcome {
  result?: string;
  error?: string;
  /** Tool calls the turn asks the CALLER to execute (docs/adr/0035); no `result` accompanies them. */
  pendingToolCalls?: PendingToolCall[];
  /**
   * The forced agentRef run's structured outcome (docs/adr/0052 §2), raw as
   * the engine recorded it. server.ts validates it (shape + size) before it
   * reaches an invocation record.
   */
  runOutcome?: unknown;
}

/** The engine's 403 body for a sender who cannot be acted as (docs/adr/0049 §6). */
export interface IdentityRefusal {
  status: "failed";
  error: { code: string; message: string; [key: string]: unknown };
}

/** The slice of the engine the facade needs — kept small and mockable for tests. */
export interface TurnEngine {
  invoke(input: TurnInput): Promise<TurnOutcome>;
  /**
   * The engine's accept-phase identity check (docs/adr/0049 §6): the 403 body
   * for a sender who cannot be acted as, or undefined for no objection.
   */
  checkIdentity(input: TurnInput): Promise<IdentityRefusal | undefined>;
}
