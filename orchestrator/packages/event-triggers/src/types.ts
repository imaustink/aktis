// Shapes from docs/event-triggers.md §1-§2 and §5. The CR types mirror
// orchestrator/controllers/core-controller/api/v1alpha1/eventsource_types.go and
// eventtrigger_types.go (JSON field names).

import type { JsonValue } from "./values.js";

// ---------------------------------------------------------------------------
// §2 envelope
// ---------------------------------------------------------------------------

/**
 * `run`: a trigger-dispatched run finished (docs/adr/0052 §1).
 * `conflict`: a watched pull request conflicts with its base (docs/adr/0055 §1).
 */
export type EnvelopeKind = "webhook" | "cron" | "run" | "conflict";

export interface EnvelopeSender {
  login?: string;
  isBot?: boolean;
  [key: string]: JsonValue | undefined;
}

export interface Envelope {
  source: string;
  kind: EnvelopeKind;
  id: string;
  /** RFC 3339, UTC. */
  time: string;
  /** Lower-cased names; authorization, cookie and the verify header removed. */
  headers: Record<string, string>;
  body: JsonValue;
  /** `{login, isBot}` when the source's sender resolved, else `{}`. */
  sender: EnvelopeSender;
  /** `repositoryFrom` result, else "". */
  repository: string;
}

// ---------------------------------------------------------------------------
// §1 CRDs
// ---------------------------------------------------------------------------

export interface ObjectMeta {
  name: string;
  namespace?: string;
  uid?: string;
  resourceVersion?: string;
  generation?: number;
  labels?: Record<string, string>;
  annotations?: Record<string, string>;
}

export interface Condition {
  type: string;
  status: "True" | "False" | "Unknown";
  reason?: string;
  message?: string;
  lastTransitionTime?: string;
  observedGeneration?: number;
}

export interface SecretKeySelector {
  name: string;
  key: string;
}

export type WebhookVerifyScheme = "hmacSha256" | "slackV0" | "bearer";

export interface WebhookVerify {
  scheme: WebhookVerifyScheme;
  header?: string;
  prefix?: string;
  secretRef: SecretKeySelector;
}

export interface WebhookSource {
  verify: WebhookVerify;
  deliveryIdHeader?: string;
}

export interface CronSource {
  schedule: string;
  timeZone?: string;
  suspend?: boolean;
  payload?: JsonValue;
}

export interface EventSender {
  provider: "github";
  loginFrom: string;
  isBotFrom?: string;
  repositoryFrom?: string;
  /** CEL -> the sender's immutable account id (docs/adr/0049 §5). */
  accountIdFrom?: string;
  dropBots?: boolean;
}

/**
 * A `run` source (docs/adr/0052 §1) receives the gateway's synthetic
 * run-completion events for triggers that set `onComplete.emitTo` to it. It
 * has no configuration of its own.
 */
export type RunSource = Record<string, never>;

/**
 * A `conflicts` source (docs/adr/0055 §1): the gateway sweeps the open pull
 * requests `author` opened and emits one `conflict` event per PR that
 * conflicts with its base, once per base commit.
 */
export interface ConflictsSource {
  /** GitHub search `author:` qualifier, e.g. "app/k5s-bot". */
  author: string;
  /** Seconds between sweeps; min 60, default 300. */
  intervalSeconds?: number;
  /** PRs carrying any of these labels are skipped (work already in flight). */
  skipLabels?: string[];
}

export interface EventSourceSpec {
  webhook?: WebhookSource;
  cron?: CronSource;
  run?: RunSource;
  conflicts?: ConflictsSource;
  sender?: EventSender;
}

export interface EventSourceStatus {
  cronJobName?: string;
  conditions?: Condition[];
}

export interface EventSourceResource {
  apiVersion?: string;
  kind?: "EventSource";
  metadata: ObjectMeta;
  spec: EventSourceSpec;
  status?: EventSourceStatus;
}

export interface EventTriggerMatch {
  cel?: string;
}

export interface EventTriggerTargetSpec {
  orchestrator?: Record<string, unknown>;
  /** Run nothing; only the trigger's reply side effects happen (docs/adr/0052 §3). */
  none?: Record<string, unknown>;
  skillRef?: string;
  agentRef?: string;
  toolRef?: string;
  promptTemplate?: string;
  readOnly?: boolean;
  /** Structured outcome a read-only agent run reports (docs/adr/0055). */
  outcome?: string;
  /** Identity-mode override for the agents this turn launches (docs/adr/0049). */
  identity?: string;
}

export interface GitHubReplySpec {
  repository: string;
  issueNumber: string;
  ack?: string;
  sessionPage?: boolean;
  result?: boolean;
  removeLabel?: string;
  /** Labels added when the run is accepted, alongside the ack (docs/adr/0052 §3). */
  addLabels?: string[];
  /** Logins assigned when the run is accepted (docs/adr/0052 §3). */
  assignees?: string[];
  /** Post an identity error's message as a comment (docs/adr/0049 §6). */
  onIdentityError?: boolean;
}

export interface HttpReplySpec {
  url: string;
  secretRef?: SecretKeySelector;
}

export interface EventTriggerReplySpec {
  github?: GitHubReplySpec;
  http?: HttpReplySpec;
}

export interface EventTriggerSpec {
  sourceRef?: string;
  priority?: number;
  match?: EventTriggerMatch;
  params?: Record<string, string>;
  sessionKey?: string;
  target: EventTriggerTargetSpec;
  reply?: EventTriggerReplySpec;
  onComplete?: EventTriggerOnComplete;
}

/** docs/adr/0052 §1, docs/adr/0055 §2. */
export interface EventTriggerOnComplete {
  /** A `run` EventSource (same namespace) that receives this trigger's run-completion events. */
  emitTo?: string;
  /** Hold the completion event until the GitHub issue/PR named by these params is closed. */
  waitUntilClosed?: OnCompleteWaitUntilClosed;
  /** Emit at most one completion event at a time per (group, key): held until the run the previous one started finishes. */
  serialize?: OnCompleteSerialize;
}

export interface OnCompleteWaitUntilClosed {
  /** Name of the param holding `owner/repo`. */
  repositoryParam: string;
  /** Name of the param holding the issue/PR number. */
  numberParam: string;
}

export interface OnCompleteSerialize {
  group: string;
  /** Name of the param whose value keys the queue (e.g. `repository`). */
  keyParam: string;
}

/** What an accepted trigger echoes back for the gateway (only when emitTo is set). */
export interface AcceptedOnComplete {
  emitTo: string;
  waitUntilClosed?: OnCompleteWaitUntilClosed;
  serialize?: OnCompleteSerialize;
}

export interface EventTriggerStatus {
  conditions?: Condition[];
}

export interface EventTriggerResource {
  apiVersion?: string;
  kind?: "EventTrigger";
  metadata: ObjectMeta;
  spec: EventTriggerSpec;
  status?: EventTriggerStatus;
}

// ---------------------------------------------------------------------------
// Resolved dispatch (§4-§5)
// ---------------------------------------------------------------------------

/** Structured outcomes a read-only agent run can report (docs/adr/0055). */
export const RUN_OUTCOMES = ["verdict", "coordination"] as const;
export type RunOutcome = (typeof RUN_OUTCOMES)[number];

export type TriggerTargetKind = "orchestrator" | "skill" | "agent" | "tool" | "none";

export interface TriggerTarget {
  kind: TriggerTargetKind;
  /** Skill / Agent / Tool id; absent for "orchestrator". */
  ref?: string;
  readOnly: boolean;
  promptTemplate?: string;
  /**
   * `target.outcome` (docs/adr/0055): the structured outcome the forced
   * read-only agent run reports. Forwarded inside the signed dispatch
   * assertion and handed to the run as `AgentRun.spec.outcome`.
   */
  outcome?: RunOutcome;
  /**
   * `target.identity` (docs/adr/0049): the identity mode every agent the turn
   * authorizes runs in. Can widen what a run reaches, so it is forwarded only
   * inside a signed dispatch assertion.
   */
  identity?: string;
}

/** `reply` with every template rendered; `issueNumber` parsed to a number. */
export interface RenderedReply {
  github?: {
    repository: string;
    issueNumber: number;
    ack?: string;
    sessionPage?: boolean;
    result?: boolean;
    removeLabel?: string;
    addLabels?: string[];
    assignees?: string[];
    onIdentityError?: boolean;
  };
  http?: {
    url: string;
    secretRef?: SecretKeySelector;
  };
}
