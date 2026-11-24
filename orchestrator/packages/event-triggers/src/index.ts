// @aktis/event-triggers — the EventSource/EventTrigger contract
// (ADR 0048, docs/event-triggers.md): the normalized event envelope and the
// CR types. Pure logic; no Kubernetes client.
//
// CEL (matching, params, templates, sender extraction) is evaluated in one
// place only: the orchestrator's Go evaluator, shared with the Temporal
// engine and core-controller. integration-gateway asks it over
// `POST /events/prefilter` (docs/event-triggers.md §8).

export type {
  AcceptedOnComplete,
  Condition,
  ConflictsSource,
  OnCompleteSerialize,
  OnCompleteWaitUntilClosed,
  CronSource,
  Envelope,
  EnvelopeKind,
  EnvelopeSender,
  EventSender,
  EventSourceResource,
  EventSourceSpec,
  EventSourceStatus,
  EventTriggerMatch,
  EventTriggerOnComplete,
  EventTriggerReplySpec,
  EventTriggerResource,
  EventTriggerSpec,
  EventTriggerStatus,
  EventTriggerTargetSpec,
  GitHubReplySpec,
  HttpReplySpec,
  ObjectMeta,
  RenderedReply,
  RunOutcome,
  RunSource,
  SecretKeySelector,
  TriggerTarget,
  TriggerTargetKind,
  WebhookSource,
  WebhookVerify,
  WebhookVerifyScheme,
} from "./types.js";

export { RUN_OUTCOMES } from "./types.js";

export type { JsonValue } from "./values.js";

export {
  ALWAYS_DROPPED_HEADERS,
  buildCronEnvelope,
  buildWebhookEnvelope,
  formatEventTime,
  normalizeHeaders,
  parseBody,
} from "./envelope.js";
export type { CronEnvelopeInput, RawHeaders, WebhookEnvelopeInput } from "./envelope.js";
