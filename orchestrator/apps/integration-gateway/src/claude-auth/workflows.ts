import { WorkflowExecutionAlreadyStartedError } from "@temporalio/common";
import {
  continueAsNew,
  isCancellation,
  ParentClosePolicy,
  proxyActivities,
  sleep,
  startChild,
  workflowInfo,
} from "@temporalio/workflow";
import type { RunActivities } from "../runs/contract.js";
import { workflowIds } from "../runs/model.js";
import type {
  ClaudeCredentialActivities,
  ClaudeCredentialRefreshInput,
  ClaudeCredentialRefreshResult,
  ClaudeCredentialSweepInput,
} from "./refresh-contract.js";

/**
 * The Claude credential workflows (credential-refresher.ts explains why they
 * exist). Bundled with the event-run workflows -- src/runs/workflows.ts
 * re-exports them -- so the same worker runs both, and the same rules hold:
 * deterministic code only, and any change to the commands one issues is
 * gated with `patched()` (docs/adr/0057 §7; src/runs/replay.test.ts replays
 * their committed histories).
 */

/**
 * The refresh. Never retried: Anthropic rotates the refresh token on every
 * refresh, so a retry after an attempt that got as far as the token endpoint
 * would spend a dead token. Failures the activity can see (a 5xx, a
 * rejection, a store that would not take the result) are its result, not an
 * error; only a crashed worker or this timeout fails it, and then the stored
 * credential is served unchanged and the next read or sweep tries again.
 */
const refreshes = proxyActivities<Pick<ClaudeCredentialActivities, "refreshClaudeCredential">>({
  startToCloseTimeout: "5 minutes",
  retry: { maximumAttempts: 1 },
});

/** The sweep's listing (a read) and its log lines: safe to retry. */
const sweeps = proxyActivities<Pick<ClaudeCredentialActivities, "claudeCredentialsDue"> & Pick<RunActivities, "log">>({
  startToCloseTimeout: "5 minutes",
  retry: { maximumAttempts: 3 },
});

/** Sweeps per run before continuing as new: a day at the default hourly interval. */
const SWEEPS_PER_RUN = 24;

/**
 * Refreshes one subject's `login` credential. Its id is the subject's
 * (workflowIds.claudeCredentialRefresh), and callers start it with
 * USE_EXISTING, so every replica asking for the same subject at once shares
 * this one run. The activity re-reads the store, so a run started just after
 * another finished finds the credential fresh and does nothing.
 */
export async function claudeCredentialRefresh(input: ClaudeCredentialRefreshInput): Promise<ClaudeCredentialRefreshResult> {
  try {
    return await refreshes.refreshClaudeCredential(input);
  } catch (error) {
    if (isCancellation(error)) throw error;
    return { status: "failed", detail: messageOf(error) };
  }
}

/**
 * Renews every stored `login` credential inside the sweep margin, on a
 * timer, whether or not anything is reading it -- refresh-on-read only fires
 * when something reads, and a refresh token is not valid forever, so an idle
 * link would otherwise age out. One per task queue
 * (workflowIds.claudeCredentialSweep), started if not running by every
 * replica, so replicas never sweep twice.
 *
 * Each due subject is refreshed by a child `claudeCredentialRefresh` with the
 * subject's own id, one at a time -- the same workflow a read starts, so a
 * sweep and a launch never refresh one subject twice. A subject whose refresh
 * is already running (a launch got there first) is skipped: that refresh is
 * renewing it. Children are abandoned rather than terminated with the sweep,
 * since a launch may be attached to one.
 *
 * Ends when the listing says there is nothing it may do (refresh disabled, a
 * store that cannot enumerate): a replica starts it again on its next check.
 */
export async function claudeCredentialSweep(input: ClaudeCredentialSweepInput): Promise<void> {
  let intervalMs = input.intervalMs;
  for (let i = 0; i < SWEEPS_PER_RUN; i++) {
    let plan: Awaited<ReturnType<ClaudeCredentialActivities["claudeCredentialsDue"]>> | undefined;
    try {
      plan = await sweeps.claudeCredentialsDue();
    } catch (error) {
      if (isCancellation(error)) throw error;
      await sweeps.log(`[claude-refresh] sweep could not list stored credentials: ${messageOf(error)}`);
    }
    if (plan) {
      intervalMs = plan.intervalMs;
      if (plan.kind === "stop") {
        await sweeps.log(`[claude-refresh] sweep disabled: ${plan.reason}`);
        return;
      }
      if (plan.kind === "failed") {
        // Deliberately loud: a list that failed is not an empty store.
        await sweeps.log(`[claude-refresh] sweep could not list stored credentials: ${plan.detail}`);
      } else {
        let refreshed = 0;
        /** Refreshed already by the time its child ran (a read, or another replica's sweep). */
        let alreadyFresh = 0;
        let stillDue = plan.unrefreshable;
        let alreadyRunning = 0;
        for (const subject of plan.due) {
          try {
            const child = await startChild(claudeCredentialRefresh, {
              workflowId: workflowIds.claudeCredentialRefresh(workflowInfo().taskQueue, subject),
              args: [{ subject, marginMs: plan.marginMs }],
              parentClosePolicy: ParentClosePolicy.ABANDON,
            });
            const result = await child.result();
            if (result.status === "refreshed") refreshed += 1;
            else if (result.status === "not-needed") alreadyFresh += 1;
            else if (result.status !== "missing") stillDue += 1;
          } catch (error) {
            if (isCancellation(error)) throw error;
            if (error instanceof WorkflowExecutionAlreadyStartedError) alreadyRunning += 1;
            else stillDue += 1;
          }
        }
        // Same condition as before alreadyFresh was split out of refreshed, so
        // the log activity is scheduled exactly when it was (replay).
        if (refreshed > 0 || alreadyFresh > 0 || stillDue > 0 || alreadyRunning > 0) {
          await sweeps.log(
            `[claude-refresh] sweep: ${plan.examined} examined, ${refreshed} refreshed, ${alreadyFresh} already fresh, ` +
              `${alreadyRunning} already refreshing, ${stillDue} still due`,
          );
        }
      }
    }
    await sleep(intervalMs);
    if (workflowInfo().continueAsNewSuggested) break;
  }
  await continueAsNew<typeof claudeCredentialSweep>({ intervalMs });
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
