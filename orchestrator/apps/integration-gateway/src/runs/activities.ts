import { Context } from "@temporalio/activity";
import type { RunActivities } from "./contract.js";
import type { RunEffects } from "./effects.js";
import type { TemporalRuns } from "./temporal.js";

/** How often a long activity tells Temporal it is alive; well inside the workflows' 1-minute heartbeat timeout. */
const HEARTBEAT_MS = 10_000;

/**
 * Heartbeats every {@link HEARTBEAT_MS} while `work` runs, with whatever
 * `details` say at the time, and gives up as soon as Temporal cancels the
 * activity -- a worker shutting down (a gateway rollout) cancels its
 * activities, so the retry starts on another replica at once instead of
 * waiting out the heartbeat timeout. The abandoned `work` is only a poll or a
 * wait, safe to drop.
 */
async function heartbeating<T>(work: Promise<T>, details: () => unknown = () => undefined): Promise<T> {
  const ctx = Context.current();
  ctx.heartbeat(details());
  const timer = setInterval(() => ctx.heartbeat(details()), HEARTBEAT_MS);
  try {
    return await Promise.race([work, ctx.cancelled]);
  } finally {
    clearInterval(timer);
  }
}

/** The activities of the event-run workflows (docs/adr/0057), over the gateway's {@link RunEffects}. */
export function createRunActivities(effects: RunEffects, runs: TemporalRuns, log: (line: string) => void): RunActivities {
  return {
    async log(line) {
      log(line);
    },
    acknowledge: (input) => effects.acknowledge(input),
    trackTurn: (sessionId, label) => effects.trackTurn(sessionId, label),
    completeTurn: (sessionId, tracked, outcome) => effects.completeTurn(sessionId, tracked, outcome),

    async awaitTurn({ invocationId, trigger, ackComment, remoteControlPosted }) {
      // A retry (this activity's worker died mid-poll) learns from the last
      // heartbeat whether the Remote Control link already went out.
      const previous = Context.current().info.heartbeatDetails as { remoteControlPosted?: boolean } | undefined;
      let posted = Boolean(remoteControlPosted || previous?.remoteControlPosted);
      const gh = effects.replyTarget(trigger);
      const hooks = gh && !posted ? effects.remoteControlHooks(gh, ackComment, () => (posted = true)) : {};
      return heartbeating(effects.pollTurn(invocationId, hooks), () => ({ remoteControlPosted: posted }));
    },

    postLinkPrompt: (trigger, text) => effects.comment(trigger, text),
    waitForLink: (identityLink) => heartbeating(effects.waitForLink(identityLink)),
    resubmit: ({ envelope, sender, traceparent }) => effects.resubmit(envelope, sender, traceparent),
    reportResult: (trigger, outcome, sendHttp) => effects.reportResult(trigger, outcome, sendHttp),
    reportFailure: (trigger, message, sendHttp) => effects.reportFailure(trigger, message, sendHttp),
    reportLinkAbandoned: (trigger) => effects.reportLinkAbandoned(trigger),
    removeLabel: (trigger) => effects.removeLabel(trigger),

    planHold: (completed) => effects.planHold(completed),
    holdState: (waitFor, parkedOn) => effects.holdState(waitFor, parkedOn),
    emitRunEvent: (completed, opts) => effects.emitRunEvent(completed, opts),
    commentChainDropped: (completed, parkedOn) => effects.commentChainDropped(completed, parkedOn),
    enqueue: (hold) => runs.enqueue(hold),
    releaseQueue: (hold) => runs.releaseQueue(hold),

    conflictsSource: async (name) => effects.conflictsSource(name),
    findConflicts: (name, conflicts) => effects.findConflicts(name, conflicts),
    emitConflict: (name, candidate) => effects.emitConflictEvent(name, candidate),
  };
}
