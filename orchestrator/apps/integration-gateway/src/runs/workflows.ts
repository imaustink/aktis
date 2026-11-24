import {
  ActivityFailure,
  condition,
  continueAsNew,
  defineSignal,
  getExternalWorkflowHandle,
  makeContinueAsNewFunc,
  ParentClosePolicy,
  proxyActivities,
  setHandler,
  sleep,
  startChild,
  workflowInfo,
} from "@temporalio/workflow";
import type { RunActivities } from "./contract.js";
import {
  githubReplyTarget,
  EMIT_TIMEOUT_MS,
  LABEL_RETRY_MS,
  LINK_ATTEMPTS,
  PUBLISH_RETRY_MAX_MS,
  queueHoldTimeoutMs,
  refKey,
  TURN_ATTEMPTS,
  workflowIds,
  DEFAULT_RUN_SETTINGS,
  type CompletedRun,
  type EventRunInput,
  type FinishedRun,
  type HeldMemo,
  type HeldRunEventInput,
  type IssueRef,
  type QueueHold,
  type RunSettings,
} from "./model.js";

/**
 * The event-run workflows (docs/adr/0057). What used to be an in-process
 * promise backed by three Redis stores -- the in-flight run with its lease,
 * the owed label removal, the held run event with its queue -- is a workflow
 * here: Temporal keeps its place, so a gateway that dies mid-run costs
 * nothing but a retried activity on another replica.
 *
 * Workflow code is deterministic: every side effect is an activity
 * (activities.ts), and time is workflow time.
 *
 * CHANGING A WORKFLOW (docs/adr/0057 §7): these workflows outlive a release
 * (heldRunEvent for weeks), and a new pod replays each from its history. Any
 * change to the commands one issues -- an activity, timer, signal or child
 * added, removed or reordered -- must keep the old path for running
 * workflows, behind `patched()`:
 *
 *   if (patched("claim-queue-on-start")) await signalQueue(hold, claimSignal);
 *
 * then `deprecatePatch()` a release later. src/runs/replay.test.ts replays
 * committed histories (src/runs/__histories__) against this file and fails
 * on a change that is not gated; fix the change, don't re-capture them.
 */

/** Short side effects: comments, labels, relays. Each is best effort inside, so a retry only covers a crashed worker. */
const effects = proxyActivities<RunActivities>({
  startToCloseTimeout: "2 minutes",
  retry: { maximumAttempts: 3 },
});

/**
 * Publishing a run or conflict event: never retried. Each attempt submits its
 * own envelope, and the orchestrator accepts every envelope it is sent as a
 * new run -- so an attempt that dies after `/invoke` accepted (a worker crash,
 * this timeout) is not repeated: a second attempt would start the next hop
 * twice. An attempt that fails this way has an unknown outcome; the workflow
 * logs it and does not publish again. An orchestrator that answers "failed"
 * (unreachable, a 5xx) is still retried, by `publish`'s own loop.
 */
const emits = proxyActivities<Pick<RunActivities, "emitRunEvent" | "emitConflict">>({
  startToCloseTimeout: EMIT_TIMEOUT_MS,
  retry: { maximumAttempts: 1 },
});

/** Polls a turn for as long as it may run: the gateway's poll budget bounds it (settings.turnTimeoutMs). */
function turnsFor(settings: RunSettings) {
  return proxyActivities<Pick<RunActivities, "awaitTurn">>({
    startToCloseTimeout: settings.turnTimeoutMs,
    heartbeatTimeout: "1 minute",
    retry: { maximumAttempts: TURN_ATTEMPTS },
  });
}

/** The identity-link wait (settings.linkWaitTimeoutMs). */
function linksFor(settings: RunSettings) {
  return proxyActivities<Pick<RunActivities, "waitForLink">>({
    startToCloseTimeout: settings.linkWaitTimeoutMs,
    heartbeatTimeout: "1 minute",
    retry: { maximumAttempts: LINK_ATTEMPTS },
  });
}

/** Label removal is retried for an hour: a stranded label means a human can never re-trigger by hand. */
const labels = proxyActivities<Pick<RunActivities, "removeLabel">>({
  startToCloseTimeout: "1 minute",
  scheduleToCloseTimeout: LABEL_RETRY_MS,
  retry: { initialInterval: "2 seconds", backoffCoefficient: 2, maximumInterval: "5 minutes" },
});

/** A queue with nobody waiting closes after this long; the next enqueue starts it again. */
const QUEUE_IDLE = "1 hour";

/** How many checks / queue turns / sweeps a workflow runs before continuing as new, bounding its history. */
const ITERATIONS_PER_RUN = 200;

export const enqueueSignal = defineSignal<[holder: string]>("enqueue");
export const releaseSignal = defineSignal<[holder: string]>("release");
export const turnSignal = defineSignal("turn");
/** A run started by a holder's publish: it now holds the queue (sent when the run starts). */
export const claimSignal = defineSignal<[holder: string]>("claim");
/** A holder whose publish ended with an unknown outcome: the queue is the run's, if one claims it. */
export const handoffSignal = defineSignal<[holder: string]>("handoff");

/** Signals a serialize queue's workflow; a queue that has closed has nobody to tell. */
async function signalQueue(hold: QueueHold, signal: typeof claimSignal | typeof handoffSignal): Promise<void> {
  try {
    await getExternalWorkflowHandle(workflowIds.runQueue(workflowInfo().taskQueue, hold.queue)).signal(signal, hold.holder);
  } catch {
    // Not running: nothing holds it to claim or hand off.
  }
}

function messageOf(error: unknown): string {
  if (error instanceof ActivityFailure && error.cause) return error.cause.message;
  return error instanceof Error ? error.message : String(error);
}

/**
 * One accepted event, start to finish (docs/event-triggers.md §7):
 *
 *   ack -> poll -> result comment -> http reply -> remove label (always)
 *   -> publish the completion (docs/adr/0052 §1), held if its trigger says so
 *
 * The completion goes out LAST, so the next hop's acceptance (say, re-adding
 * a label this run just removed) lands after this run's cleanup rather than
 * being undone by it.
 */
export async function eventRun(input: EventRunInput): Promise<void> {
  const { envelope, sender, trigger, traceparent, resumed } = input;
  const sessionId = trigger.sessionId;
  const turns = turnsFor(input.settings);
  const links = linksFor(input.settings);
  try {
    // Tells the queue this run holds it now, so a publish that ended without
    // an answer does not let the queue move on under a live run.
    if (input.holdsQueue) await signalQueue(input.holdsQueue, claimSignal);
    // Set once the run has a final outcome to publish.
    let finished: FinishedRun | undefined;
    let httpSent = false;
    try {
      let ackComment = resumed?.ackComment;
      if (!resumed) ackComment = (await effects.acknowledge(input)).ackComment;

      const turnLabel = `${envelope.source} event ${envelope.id} → ${trigger.name || "trigger"}`;
      let tracked = await effects.trackTurn(sessionId, turnLabel);
      let outcome = await turns.awaitTurn({
        invocationId: input.invocationId,
        trigger,
        ...(ackComment ? { ackComment } : {}),
        ...(resumed?.remoteControlPosted ? { remoteControlPosted: true } : {}),
      });

      // Unauthenticated turn: `result` is a "link your account" prompt, not
      // finished work. It is posted whenever there is an issue to post to,
      // then -- for a provider whose token store this gateway owns -- the
      // same event is re-submitted once the link lands (the ack is not
      // repeated, and a Remote Control link is posted as its own comment:
      // the ack now sits above the prompt the human is acting on).
      if (outcome.status === "succeeded" && outcome.identityLinkPending && outcome.identityLink) {
        await effects.completeTurn(sessionId, tracked, outcome);
        await effects.postLinkPrompt(trigger, outcome.result ?? "");
        if (!(await links.waitForLink(outcome.identityLink))) {
          httpSent = true;
          await effects.reportLinkAbandoned(trigger);
          return;
        }
        const again = await effects.resubmit({ envelope, ...(sender ? { sender } : {}), ...(traceparent ? { traceparent } : {}) });
        tracked = await effects.trackTurn(sessionId, turnLabel);
        outcome =
          again.kind === "accepted"
            ? await turns.awaitTurn({ invocationId: again.id, trigger })
            : { status: "failed", error: `re-submitting the event after linking: ${again.error}` };
      }

      // A turn that is still a "link your account" prompt did no work, so it
      // has no completion to publish: its `succeeded` is the prompt's.
      if (!outcome.identityLinkPending) {
        finished = { status: outcome.status, ...(outcome.outcome ? { outcome: outcome.outcome } : {}) };
      }
      await effects.completeTurn(sessionId, tracked, outcome);
      if (!resumed?.resultPosted) {
        httpSent = true;
        await effects.reportResult(trigger, outcome, true);
      }
    } catch (error) {
      // Last line of defence: the run must not end in silence where the
      // trigger declared somewhere to report to. No completion is published
      // ("the gateway failed mid-run" is not a status a trigger routes on).
      finished = undefined;
      await effects.reportFailure(trigger, messageOf(error), !httpSent);
    } finally {
      // Every outcome: a failed run is exactly when someone wants to
      // re-trigger by hand.
      if (githubReplyTarget(trigger).target?.removeLabel) {
        try {
          await labels.removeLabel(trigger);
        } catch (error) {
          await effects.log(`trigger ${trigger.name}: could not remove its label after retrying for an hour: ${messageOf(error)}`);
        }
      }
    }

    if (finished && trigger.onComplete?.emitTo && !resumed?.emitted) {
      await complete({ envelope, ...(sender ? { sender } : {}), trigger, finished, ...(traceparent ? { traceparent } : {}) }, input);
    }
  } finally {
    // docs/adr/0055 §2: the queue lives exactly as long as the run that
    // holds it -- released after this run's own completion went out, so the
    // next run in the queue starts only once this one's next hop exists.
    if (input.holdsQueue) await effects.releaseQueue(input.holdsQueue);
  }
}

/** Publishes a completion now, or hands it to a held-event workflow when its trigger holds it. */
async function complete(completed: CompletedRun, input: EventRunInput): Promise<void> {
  const oc = completed.trigger.onComplete!;
  if (oc.waitUntilClosed || oc.serialize) {
    const plan = await effects.planHold(completed);
    if (plan.waitFor || plan.queue) {
      const memo: HeldMemo = {
        ...(plan.parkedOn ? { parkedOn: refKey(plan.parkedOn) } : {}),
        ...(plan.waitFor ? { waitFor: refKey(plan.waitFor) } : {}),
      };
      // Its own workflow: a hold can last weeks, and this run is done.
      await startChild(heldRunEvent, {
        workflowId: workflowIds.heldRunEvent(workflowInfo().taskQueue, input.invocationId),
        parentClosePolicy: ParentClosePolicy.ABANDON,
        memo: memo as Record<string, unknown>,
        args: [{ ...plan, completed, settings: input.settings }],
      });
      return;
    }
  }
  await publish(completed, input.settings, {});
}

/**
 * Publishes a completion, retrying a transport failure ("failed") with
 * backoff for `settings.publishRetryWindowMs`. A delivered-but-not-accepted
 * outcome (ignored, rejected, a gate) is final. Releases the queue unless the
 * publish started a run, which then holds it.
 */
async function publish(
  completed: CompletedRun,
  settings: RunSettings,
  opts: { resetDepth?: boolean; holdsQueue?: QueueHold; parkedOn?: IssueRef },
): Promise<void> {
  const name = completed.trigger.name || "trigger";
  let firstFailure: number | undefined;
  for (let attempt = 1; ; attempt++) {
    let result: Awaited<ReturnType<RunActivities["emitRunEvent"]>>;
    try {
      result = await emits.emitRunEvent(completed, {
        ...(opts.resetDepth ? { resetDepth: true } : {}),
        ...(opts.holdsQueue ? { holdsQueue: opts.holdsQueue } : {}),
      });
    } catch (error) {
      // Unknown outcome (see `emits`): the next hop may already be running,
      // so it is not published again. Nor is a held queue released: it is
      // handed off. If that run started, it claims the queue and releases it
      // when it finishes; if no run claims it within the claim grace, the
      // queue moves on.
      await effects.log(`run event ${name}: publish attempt ${attempt} ended without an answer (${messageOf(error)}); not published again, so it cannot start the next hop twice`);
      if (opts.holdsQueue) await signalQueue(opts.holdsQueue, handoffSignal);
      return;
    }
    if (result !== "failed") {
      // "accepted-not-started" too: no run exists to release it.
      if (opts.holdsQueue && result !== "accepted") await effects.releaseQueue(opts.holdsQueue);
      return;
    }
    firstFailure ??= Date.now();
    if (Date.now() - firstFailure >= settings.publishRetryWindowMs) {
      await effects.log(`run event ${name}: publish failed ${attempt} times since ${new Date(firstFailure).toISOString()}; dropped`);
      // Say so where the run replied: a parked ticket would otherwise keep
      // looking like it is still waiting.
      await effects.commentChainDropped(completed, opts.parkedOn);
      if (opts.holdsQueue) await effects.releaseQueue(opts.holdsQueue);
      return;
    }
    await effects.log(`run event ${name}: publish failed (attempt ${attempt}); retrying`);
    await sleep(Math.min(PUBLISH_RETRY_MAX_MS, settings.publishRetryMs * 2 ** (attempt - 1)));
  }
}

/**
 * docs/adr/0055 §2: a completion held until its `waitFor` issue/PR is closed
 * (abandoned if its own issue closes first) and/or it has its turn in a
 * serialize queue, then published.
 */
export async function heldRunEvent(input: HeldRunEventInput): Promise<void> {
  let myTurn = false;
  setHandler(turnSignal, () => {
    myTurn = true;
  });
  const name = input.completed.trigger.name || "trigger";

  if (input.waitFor) {
    for (let i = 0; i < ITERATIONS_PER_RUN; i++) {
      await sleep(input.settings.heldEventCheckMs);
      const state = await effects.holdState(input.waitFor, input.parkedOn);
      if (state === "abandoned") {
        await effects.log(`held run event ${name}: abandoned (its issue ${input.parkedOn ? refKey(input.parkedOn) : "?"} was closed while waiting)`);
        return;
      }
      if (state === "released") break;
      if (i === ITERATIONS_PER_RUN - 1 || workflowInfo().continueAsNewSuggested) {
        await makeContinueAsNewFunc<typeof heldRunEvent>({ memo: workflowInfo().memo ?? {} })(input);
      }
    }
  }

  let holdsQueue: QueueHold | undefined;
  if (input.queue) {
    holdsQueue = { queue: input.queue, holder: workflowInfo().workflowId };
    await effects.enqueue(holdsQueue);
    await condition(() => myTurn);
  }
  await publish(input.completed, input.settings, {
    ...(input.blocked ? { resetDepth: true } : {}),
    ...(holdsQueue ? { holdsQueue } : {}),
    ...(input.parkedOn ? { parkedOn: input.parkedOn } : {}),
  });
}

/**
 * One serialize queue (docs/adr/0055 §2): hands turns to held events in the
 * order they joined, one at a time. A turn ends when its holder releases it --
 * the held event itself when its publish started nothing, else the run it
 * started, once that run has finished. A held event whose publish ended
 * without an answer hands its turn off instead: the turn then lasts only
 * until the claim grace unless a run claims it.
 */
export async function runQueue(input: { queue: string; waiting?: string[]; holdTimeoutMs?: number; claimGraceMs?: number }): Promise<void> {
  // A holder that never releases (its workflow was terminated) is passed over after this long -- longer than any live holder.
  const holdTimeoutMs = input.holdTimeoutMs ?? queueHoldTimeoutMs(DEFAULT_RUN_SETTINGS);
  // A handed-off turn that no run claims is passed over after this long.
  const claimGraceMs = input.claimGraceMs ?? DEFAULT_RUN_SETTINGS.queueClaimGraceMs;
  const waiting = [...(input.waiting ?? [])];
  let current: string | undefined;
  let released = false;
  let claimed = false;
  let handedOff = false;
  setHandler(enqueueSignal, (holder) => {
    if (holder !== current && !waiting.includes(holder)) waiting.push(holder);
  });
  setHandler(releaseSignal, (holder) => {
    if (holder === current) released = true;
    else {
      const at = waiting.indexOf(holder);
      if (at >= 0) waiting.splice(at, 1);
    }
  });
  setHandler(claimSignal, (holder) => {
    if (holder === current) claimed = true;
  });
  setHandler(handoffSignal, (holder) => {
    if (holder === current) handedOff = true;
  });

  for (let served = 0; ; served++) {
    if (waiting.length === 0 && !(await condition(() => waiting.length > 0, QUEUE_IDLE))) return;
    if (served >= ITERATIONS_PER_RUN || workflowInfo().continueAsNewSuggested) {
      await continueAsNew<typeof runQueue>({ queue: input.queue, waiting, holdTimeoutMs, claimGraceMs });
    }
    current = waiting.shift()!;
    released = false;
    claimed = false;
    handedOff = false;
    try {
      await getExternalWorkflowHandle(current).signal(turnSignal);
    } catch {
      // The holder is gone (terminated, say): nothing to wait for.
      current = undefined;
      continue;
    }
    const turnStarted = Date.now();
    const left = () => holdTimeoutMs - (Date.now() - turnStarted);
    let passedOver = !(await condition(() => released || (handedOff && !claimed), holdTimeoutMs));
    if (!released && !passedOver) {
      // Handed off, and no run has claimed the turn yet.
      const graceMs = Math.min(claimGraceMs, left());
      if (graceMs <= 0 || !(await condition(() => released || claimed, graceMs))) {
        await effects.log(`run queue ${input.queue}: ${current}'s publish ended without an answer and no run claimed the queue within ${claimGraceMs}ms; passing it over`);
      } else if (!released) {
        passedOver = left() <= 0 || !(await condition(() => released, left()));
      }
    }
    if (passedOver) await effects.log(`run queue ${input.queue}: ${current} did not release the queue within ${holdTimeoutMs}ms; passing it over`);
    current = undefined;
  }
}

/** A conflict mark outlives any plausible gap between a conflict and the next base move. */
const MARK_TTL_MS = 30 * 24 * 60 * 60 * 1000;

/**
 * The `conflicts` EventSource kind (docs/adr/0055 §1): every `intervalMs`,
 * emit one `conflict` event per open PR that conflicts with its base -- once
 * per (PR, base commit), remembered in this workflow's own state. One
 * workflow per source, so replicas never sweep twice; it ends when its
 * source is gone.
 */
export async function conflictSweep(input: { source: string; marks?: Record<string, number> }): Promise<void> {
  const marks: Record<string, number> = { ...(input.marks ?? {}) };
  for (let i = 0; i < ITERATIONS_PER_RUN; i++) {
    const conflicts = await effects.conflictsSource(input.source);
    if (!conflicts) return;
    for (const [key, at] of Object.entries(marks)) if (Date.now() - at > MARK_TTL_MS) delete marks[key];

    for (const candidate of await effects.findConflicts(input.source, conflicts)) {
      if (marks[candidate.markKey] !== undefined) continue;
      let delivered: boolean;
      try {
        delivered = await emits.emitConflict(input.source, candidate);
      } catch (error) {
        // Unknown outcome (see `emits`): the event may have started a run, so
        // it is marked rather than sent again. The PR's next base move emits anew.
        await effects.log(`conflicts ${input.source}: emitting ${candidate.markKey} ended without an answer (${messageOf(error)}); not emitted again`);
        delivered = true;
      }
      // Undelivered (the orchestrator was unreachable): try again next sweep.
      if (delivered) marks[candidate.markKey] = Date.now();
    }
    await sleep(conflicts.intervalMs);
    if (workflowInfo().continueAsNewSuggested) break;
  }
  await continueAsNew<typeof conflictSweep>({ source: input.source, marks });
}

