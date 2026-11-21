// Shapes from docs/event-triggers.md §1-§2 and §5. The CR types mirror
// orchestrator/controllers/core-controller/api/v1alpha1/eventsource_types.go and
// eventtrigger_types.go (JSON field names).

import type { JsonValue } from "./values.js";

// ---------------------------------------------------------------------------
// §2 envelope
// ---------------------------------------------------------------------------

/** `run`: a trigger-dispatched run finished (docs/adr/0052 §1). */
export type EnvelopeKind = "webhook" | "cron" | "run";

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

export interface EventSourceSpec {
  webhook?: WebhookSource;
  cron?: CronSource;
  run?: RunSource;
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

/** docs/adr/0052 §1. */
export interface EventTriggerOnComplete {
  /** A `run` EventSource (same namespace) that receives this trigger's run-completion events. */
  emitTo?: string;
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

export type TriggerTargetKind = "orchestrator" | "skill" | "agent" | "tool" | "none";

export interface TriggerTarget {
  kind: TriggerTargetKind;
  /** Skill / Agent / Tool id; absent for "orchestrator". */
  ref?: string;
  readOnly: boolean;
  promptTemplate?: string;
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
