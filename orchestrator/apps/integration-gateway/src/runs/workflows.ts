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
  refKey,
  workflowIds,
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
 */

/** Short side effects: comments, labels, relays. Each is best effort inside, so a retry only covers a crashed worker. */
const effects = proxyActivities<RunActivities>({
  startToCloseTimeout: "2 minutes",
  retry: { maximumAttempts: 3 },
});

/** Polls a turn for as long as it may run: the gateway's poll budget bounds it (settings.turnTimeoutMs). */
function turnsFor(settings: RunSettings) {
  return proxyActivities<Pick<RunActivities, "awaitTurn">>({
    startToCloseTimeout: settings.turnTimeoutMs,
    heartbeatTimeout: "1 minute",
    retry: { maximumAttempts: 10 },
  });
}

/** The identity-link wait (settings.linkWaitTimeoutMs). */
function linksFor(settings: RunSettings) {
  return proxyActivities<Pick<RunActivities, "waitForLink">>({
    startToCloseTimeout: settings.linkWaitTimeoutMs,
    heartbeatTimeout: "1 minute",
    retry: { maximumAttempts: 3 },
  });
}

/** Label removal is retried for an hour: a stranded label means a human can never re-trigger by hand. */
const labels = proxyActivities<Pick<RunActivities, "removeLabel">>({
  startToCloseTimeout: "1 minute",
  scheduleToCloseTimeout: "1 hour",
  retry: { initialInterval: "2 seconds", backoffCoefficient: 2, maximumInterval: "5 minutes" },
});

const PUBLISH_RETRY_MAX_MS = 15 * 60 * 1000;

/** A queue's holder that never releases (it was terminated) is passed over after this long -- longer than any run. */
export const QUEUE_HOLD_TIMEOUT = "9 hours";
/** A queue with nobody waiting closes after this long; the next enqueue starts it again. */
const QUEUE_IDLE = "1 hour";

/** How many checks / queue turns / sweeps a workflow runs before continuing as new, bounding its history. */
const ITERATIONS_PER_RUN = 200;

export const enqueueSignal = defineSignal<[holder: string]>("enqueue");
export const releaseSignal = defineSignal<[holder: string]>("release");
export const turnSignal = defineSignal("turn");

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
    const result = await effects.emitRunEvent(completed, {
      ...(opts.resetDepth ? { resetDepth: true } : {}),
      ...(opts.holdsQueue ? { holdsQueue: opts.holdsQueue } : {}),
    });
    if (result !== "failed") {
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
 * started, once that run has finished.
 */
export async function runQueue(input: { queue: string; waiting?: string[] }): Promise<void> {
  const waiting = [...(input.waiting ?? [])];
  let current: string | undefined;
  let released = false;
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

  for (let served = 0; ; served++) {
    if (waiting.length === 0 && !(await condition(() => waiting.length > 0, QUEUE_IDLE))) return;
    if (served >= ITERATIONS_PER_RUN || workflowInfo().continueAsNewSuggested) {
      await continueAsNew<typeof runQueue>({ queue: input.queue, waiting });
    }
    current = waiting.shift()!;
    released = false;
    try {
      await getExternalWorkflowHandle(current).signal(turnSignal);
    } catch {
      // The holder is gone (terminated, say): nothing to wait for.
      current = undefined;
      continue;
    }
    await condition(() => released, QUEUE_HOLD_TIMEOUT);
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
      // Undelivered (the orchestrator was unreachable): try again next sweep.
      if (await effects.emitConflict(input.source, candidate)) marks[candidate.markKey] = Date.now();
    }
    await sleep(conflicts.intervalMs);
    if (workflowInfo().continueAsNewSuggested) break;
  }
  await continueAsNew<typeof conflictSweep>({ source: input.source, marks });
}

