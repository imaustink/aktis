import type { CompiledConflicts } from "../event-sources.js";
import type { AcceptedTrigger, OrchestratorInvokeResult } from "../orchestrator-client.js";
import type {
  CompletedRun,
  ConflictCandidate,
  EmitRunEventResult,
  EventRunInput,
  HoldPlan,
  IssueRef,
  QueueHold,
} from "./model.js";

/**
 * Every side effect the event-run workflows perform (docs/adr/0057), each
 * one a Temporal activity implemented in activities.ts. Unless a method says
 * otherwise it is best effort: it reports a failure and returns, so a failed
 * comment never stops a run from finishing.
 */
export interface RunActivities {
  /** One operational log line. */
  log(line: string): Promise<void>;

  /** Acceptance (docs/event-triggers.md §7, docs/adr/0052 §3): the ack (+ session page link), status labels, assignees. Returns the posted ack, so a Remote Control link can be edited into it. */
  acknowledge(input: EventRunInput): Promise<{ ackComment?: { id: number; body: string } }>;
  /** Adds a turn to the session page, when the session has one. */
  trackTurn(sessionId: string, label: string): Promise<{ turnIndex: number } | undefined>;
  completeTurn(sessionId: string, tracked: { turnIndex: number } | undefined, outcome: OrchestratorInvokeResult): Promise<void>;
  /**
   * Polls the turn to its end (heartbeating), surfacing a Remote Control link
   * at most once. Retried on another worker if this one dies: the
   * orchestrator answers the same invocation whichever process polls it.
   */
  awaitTurn(args: {
    invocationId: string;
    trigger: AcceptedTrigger;
    ackComment?: { id: number; body: string };
    remoteControlPosted?: boolean;
  }): Promise<OrchestratorInvokeResult>;
  /** Posts an identity-link prompt where the trigger replies. */
  postLinkPrompt(trigger: AcceptedTrigger, text: string): Promise<void>;
  /** Waits (heartbeating) for a link this gateway's own store can see. False when it can't wait on that provider, or the link never landed. */
  waitForLink(identityLink: { provider: string; subject: string }): Promise<boolean>;
  /** Re-submits the event after a completed link. */
  resubmit(args: Pick<EventRunInput, "envelope" | "sender" | "traceparent">): Promise<{ kind: "accepted"; id: string } | { kind: "failed"; error: string }>;
  /** The result comment (when `reply.github.result`) and the `reply.http` callback. */
  reportResult(trigger: AcceptedTrigger, outcome: OrchestratorInvokeResult, sendHttp: boolean): Promise<void>;
  /** The last line of defence: a run must not end in silence where its trigger declared somewhere to report. */
  reportFailure(trigger: AcceptedTrigger, message: string, sendHttp: boolean): Promise<void>;
  /** The identity-link wait ended without a link, so no run started: tells the `reply.http` callback (the issue already shows the prompt). */
  reportLinkAbandoned(trigger: AcceptedTrigger): Promise<void>;
  /** Removes `reply.github.removeLabel`. THROWS on failure, so Temporal retries it. */
  removeLabel(trigger: AcceptedTrigger): Promise<void>;

  /** docs/adr/0055 §2: which holds a completion really waits on, with the self- and cycle-guards applied. */
  planHold(completed: CompletedRun): Promise<HoldPlan>;
  /** A held event's issues: "released" once `waitFor` is closed or gone, "abandoned" once `parkedOn` is. */
  holdState(waitFor: IssueRef, parkedOn: IssueRef | undefined): Promise<"waiting" | "released" | "abandoned">;
  /** docs/adr/0052 §1: publishes a completion to its `onComplete.emitTo`, behind the depth cap and per-hop re-verification. */
  emitRunEvent(completed: CompletedRun, opts: { resetDepth?: boolean; holdsQueue?: QueueHold }): Promise<EmitRunEventResult>;
  /** Tells the held run's issue that its chain stopped: the next step could not be delivered. */
  commentChainDropped(completed: CompletedRun, parkedOn: IssueRef | undefined): Promise<void>;
  /** Joins (starting it if needed) a serialize queue's workflow. */
  enqueue(hold: QueueHold): Promise<void>;
  /** Frees a queue `hold.holder` holds; a no-op otherwise. */
  releaseQueue(hold: QueueHold): Promise<void>;

  /** A `conflicts` source's current spec, or undefined once it is gone (or no longer that kind). */
  conflictsSource(name: string): Promise<CompiledConflicts | undefined>;
  /** docs/adr/0055 §1: the source's open PRs that conflict and have an App-assigned assignee who verifies. */
  findConflicts(name: string, conflicts: CompiledConflicts): Promise<ConflictCandidate[]>;
  /** Relays one conflict event; true unless the orchestrator could not be reached. */
  emitConflict(name: string, candidate: ConflictCandidate): Promise<boolean>;
}
