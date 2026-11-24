import type { Envelope, JsonValue } from "@aktis/event-triggers";
import type { PullRequestState } from "../github-client.js";
import type { AcceptedTrigger, OrchestratorInvokeResult } from "../orchestrator-client.js";
import type { SenderClaims } from "../sender-assertion.js";

/**
 * The data the event-run workflows (docs/adr/0057) carry, and the pure rules
 * they apply. Everything here is plain JSON and pure functions: the workflow
 * sandbox imports this module, so it must not touch Node, the network or the
 * clock.
 */

/** A GitHub issue or pull request. */
export interface IssueRef {
  owner: string;
  repo: string;
  number: number;
}

export const refKey = (r: IssueRef): string => `${r.owner}/${r.repo}#${r.number}`.toLowerCase();

/** `owner/name`, each a GitHub-legal segment -- what lands in a REST path. */
export const REPOSITORY = /^([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+)$/;

/** The login a `conflicts.author` search qualifier acts as on GitHub: `app/<slug>` is `<slug>[bot]`. */
export function actorLogin(author: string): string {
  return author.startsWith("app/") ? `${author.slice(4)}[bot]` : author;
}

/** A positive integer from a string param (params arrive in their §4 string form). */
export function positiveInt(value: string | undefined): number | undefined {
  if (typeof value !== "string" || !/^[1-9][0-9]*$/.test(value.trim())) return undefined;
  const n = Number(value.trim());
  return Number.isSafeInteger(n) ? n : undefined;
}

/**
 * The chain a run belongs to (docs/adr/0052 §1), carried in every run event's
 * `body.chain`. A webhook/cron delivery starts one at depth 0; each run event
 * is one deeper.
 */
export interface RunChain {
  /** The id of the envelope that started the chain. */
  id: string;
  depth: number;
  /** Completed runs per trigger name in this chain. */
  counts: Record<string, number>;
  /** The verified human who started it. Absent for a sourceless sender (cron, a webhook without `sender`), which cannot emit. */
  origin?: { login: string; accountId?: string; repository: string };
}

const isCount = (v: unknown): v is number => typeof v === "number" && Number.isSafeInteger(v) && v >= 0;

/**
 * Reads back a run event's `body.chain`. The gateway wrote it, and a run
 * source has no HTTP route, but the engine round-trips it, so it is checked
 * rather than trusted: an unreadable chain stops emission (fail closed) rather
 * than restarting at depth 0, which would defeat the depth cap.
 */
export function parseChain(raw: unknown): RunChain | undefined {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return undefined;
  const c = raw as Record<string, unknown>;
  if (typeof c.id !== "string" || !c.id || !isCount(c.depth)) return undefined;
  if (typeof c.counts !== "object" || c.counts === null || Array.isArray(c.counts)) return undefined;
  const counts: Record<string, number> = {};
  for (const [name, n] of Object.entries(c.counts)) {
    if (!isCount(n)) return undefined;
    counts[name] = n;
  }
  let origin: RunChain["origin"];
  if (c.origin !== undefined) {
    const o = c.origin as Record<string, unknown> | null;
    if (typeof o !== "object" || o === null || typeof o.login !== "string" || !o.login || typeof o.repository !== "string") return undefined;
    if (o.accountId !== undefined && typeof o.accountId !== "string") return undefined;
    origin = { login: o.login, ...(o.accountId ? { accountId: o.accountId } : {}), repository: o.repository };
  }
  return { id: c.id, depth: c.depth, counts, ...(origin ? { origin } : {}) };
}

/**
 * The chain an accepted envelope belongs to. A run event carries it in its
 * body; anything else starts a new one, whose origin is the delivery's
 * VERIFIED sender -- `sender` is only ever set for a login the source's
 * provider resolved.
 */
export function chainOf(envelope: Envelope, sender: SenderClaims | undefined): RunChain | undefined {
  if (envelope.kind === "run") return parseChain((envelope.body as Record<string, JsonValue> | null)?.chain);
  return {
    id: envelope.id,
    depth: 0,
    counts: {},
    ...(sender?.login
      ? { origin: { login: sender.login, ...(sender.accountId ? { accountId: sender.accountId } : {}), repository: envelope.repository } }
      : {}),
  };
}

/** The validated `reply.github` of an accepted trigger, with its repository split. */
export interface GithubReplyTarget {
  owner: string;
  repo: string;
  issueNumber: number;
  ack?: string;
  sessionPage?: boolean;
  result?: boolean;
  removeLabel?: string;
  /** Applied at acceptance (docs/adr/0052 §3); only non-empty strings survive validation. */
  addLabels?: string[];
  assignees?: string[];
}

/** The non-empty strings of a rendered list; anything else would only earn a 422 from GitHub. */
function nonEmptyStrings(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((v): v is string => typeof v === "string" && v.trim() !== "") : [];
}

/**
 * `reply.github` from the orchestrator, validated. An unusable one is treated
 * as absent (it would only produce 404s against a garbage path); `problem`
 * says why, for the caller to log.
 */
export function githubReplyTarget(trigger: AcceptedTrigger): { target?: GithubReplyTarget; problem?: string } {
  const gh = trigger.reply?.github;
  if (!gh) return {};
  const repoMatch = typeof gh.repository === "string" ? REPOSITORY.exec(gh.repository) : null;
  const issueNumber = gh.issueNumber;
  if (!repoMatch || typeof issueNumber !== "number" || !Number.isSafeInteger(issueNumber) || issueNumber <= 0) {
    return {
      problem: `trigger ${trigger.name}: reply.github ignored (repository ${JSON.stringify(gh.repository)} / issueNumber ${JSON.stringify(issueNumber)} unusable)`,
    };
  }
  const addLabels = nonEmptyStrings(gh.addLabels);
  const assignees = nonEmptyStrings(gh.assignees);
  return {
    target: {
      owner: repoMatch[1] as string,
      repo: repoMatch[2] as string,
      issueNumber,
      ...(gh.ack ? { ack: gh.ack } : {}),
      ...(gh.sessionPage ? { sessionPage: true } : {}),
      ...(gh.result ? { result: true } : {}),
      ...(gh.removeLabel ? { removeLabel: gh.removeLabel } : {}),
      ...(addLabels.length ? { addLabels } : {}),
      ...(assignees.length ? { assignees } : {}),
    },
  };
}

/** How a run ended, for its completion event. */
export interface FinishedRun {
  status: OrchestratorInvokeResult["status"];
  outcome?: Record<string, unknown>;
}

/**
 * How publishing a run event ended: accepted; delivered but not accepted (or
 * stopped by a gate) -- final; or "failed", a transport failure worth retrying.
 */
export type EmitRunEventResult = "accepted" | "not-accepted" | "failed";

/** A serialize queue (docs/adr/0055 §2) and who holds it. */
export interface QueueHold {
  queue: string;
  /** The held-event workflow that took the queue; the run it started releases it under the same name. */
  holder: string;
}

/** Workflow settings, fixed when a run starts so a replay never reads config. */
export interface RunSettings {
  /** How often a held `waitUntilClosed` event re-reads its issues (docs/adr/0055 §2). */
  heldEventCheckMs: number;
  /** The first wait before re-publishing a completion the orchestrator could not take; it doubles, to 15 minutes. */
  publishRetryMs: number;
  /**
   * How long a completion keeps being retried after its first failed
   * publish. "failed" covers an unreachable orchestrator, but also permanent
   * errors, so retrying must end: long enough to ride out an outage, short
   * enough that a poison event stops calling the orchestrator.
   */
  publishRetryWindowMs: number;
  /** How long polling one turn may take: the gateway's poll budget (GATEWAY_POLL_TIMEOUT_MS), plus slack. */
  turnTimeoutMs: number;
  /** How long an identity-link wait may take: GATEWAY_RESUME_WAIT_MS, plus slack. */
  linkWaitTimeoutMs: number;
}

export const DEFAULT_RUN_SETTINGS: RunSettings = {
  heldEventCheckMs: 5 * 60 * 1000,
  turnTimeoutMs: 15 * 60 * 1000 + 10 * 60 * 1000,
  linkWaitTimeoutMs: 10 * 60 * 1000 + 10 * 60 * 1000,
  publishRetryMs: 30_000,
  publishRetryWindowMs: 24 * 60 * 60 * 1000,
};

/** One accepted event, as `eventRun` receives it. */
export interface EventRunInput {
  /** The invocation the orchestrator accepted the event as; the workflow id derives from it. */
  invocationId: string;
  envelope: Envelope;
  sender?: SenderClaims;
  trigger: AcceptedTrigger;
  traceparent?: string;
  /** The serialize queue this run holds (docs/adr/0055 §2), released once it has finished. */
  holdsQueue?: QueueHold;
  /**
   * A run started before this gateway ran runs on Temporal, handed over from
   * the old in-flight store (legacy-drain.ts): its acceptance side effects
   * already happened, and these say which finishing steps did too.
   */
  resumed?: {
    ackComment?: { id: number; body: string };
    remoteControlPosted?: boolean;
    resultPosted?: boolean;
    emitted?: boolean;
  };
  settings: RunSettings;
}

/** A finished run whose completion event is still to be published. */
export interface CompletedRun {
  envelope: Envelope;
  sender?: SenderClaims;
  trigger: AcceptedTrigger;
  finished: FinishedRun;
  traceparent?: string;
}

/**
 * Which holds a completion waits on (docs/adr/0055 §2). Neither set: publish
 * now (every hold was unusable, or already released).
 */
export interface HoldPlan {
  waitFor?: IssueRef;
  /** The held run's own issue: closing it abandons the wait. */
  parkedOn?: IssueRef;
  /** `serialize`: `<group>:<key>`. */
  queue?: string;
  /** `waitFor` was open when held: the chain's depth restarts when it is published. */
  blocked?: boolean;
}

/** A held completion, as `heldRunEvent` receives it. */
export interface HeldRunEventInput extends HoldPlan {
  completed: CompletedRun;
  settings: RunSettings;
}

/** What `heldRunEvent` records in its memo, so a new hold can see what is already waiting on what. */
export interface HeldMemo {
  parkedOn?: string;
  waitFor?: string;
}

/** One pull request the conflict sweep found conflicting with its base (docs/adr/0055 §1). */
export interface ConflictCandidate {
  owner: string;
  repo: string;
  pr: PullRequestState;
  assignee: { login: string; accountId?: string };
  /** Once per (source, PR, base commit). */
  markKey: string;
}

/**
 * Workflow ids: one per invocation, held event, queue and conflicts source,
 * each under its task queue -- so two releases sharing a Temporal namespace
 * never share a queue or a sweep.
 */
export const workflowIds = {
  eventRun: (taskQueue: string, invocationId: string) => `${taskQueue}/event-run:${invocationId}`,
  heldRunEvent: (taskQueue: string, id: string) => `${taskQueue}/held-run-event:${id}`,
  runQueue: (taskQueue: string, queue: string) => `${taskQueue}/run-queue:${queue}`,
  conflictSweep: (taskQueue: string, source: string) => `${taskQueue}/conflict-sweep:${source}`,
};
