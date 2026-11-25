import {
  allHandlersFinished,
  ApplicationFailure,
  condition,
  continueAsNew,
  defineSignal,
  defineUpdate,
  isCancellation,
  proxyActivities,
  setHandler,
  sleep,
  workflowInfo,
} from "@temporalio/workflow";
import {
  SOURCES_CHANGED_SIGNAL,
  SYNC_NOW_UPDATE,
  type CorpusSyncInput,
  type PassResult,
  type SyncActivities,
} from "./model.js";

/**
 * One Corpus's sync loop (docs/adr/0058): a full reconcile pass every
 * `reconcileInterval`, a partial pass for what webhooks name in between, and
 * a full pass on demand. One workflow per Corpus, so its passes never overlap
 * however many broker replicas there are: two concurrent full passes can each
 * conclude the other's freshly written chunks are absent, and a full pass
 * deletes what it believes absent.
 *
 * The reconcile is the source of truth (ADR 0038 §4); a webhook only adds
 * work. Resources a webhook names while a pass runs are kept for the next
 * one, never dropped.
 *
 * Workflow code is deterministic: every side effect is an activity
 * (activities.ts), and time is workflow time.
 *
 * CHANGING THIS WORKFLOW (docs/adr/0057 §7): it runs for as long as its
 * Corpus exists, and a new pod replays it from its history. Any change to the
 * commands it issues -- an activity, timer, signal or update handler added,
 * removed or reordered -- must keep the old path behind `patched()`, then
 * `deprecatePatch()` a release later. src/sync/replay.test.ts replays the
 * committed histories (src/sync/__histories__) against this file and fails on
 * a change that is not gated; fix the change, don't re-capture them.
 */

/** Reading a Corpus's settings from the broker's registry: cheap, and retried while a pod restarts. */
const settings = proxyActivities<Pick<SyncActivities, "syncTarget" | "log">>({
  startToCloseTimeout: "1 minute",
  retry: { maximumAttempts: 5 },
});

/**
 * A pass: as long as a large corpus needs, heartbeating, so a broker that dies
 * mid-pass is noticed within the heartbeat timeout and the pass is retried --
 * from the start -- on another replica. A pass is idempotent (point ids are
 * content hashes), so repeating one costs only time.
 */
const passes = proxyActivities<Pick<SyncActivities, "runPass">>({
  startToCloseTimeout: "6 hours",
  heartbeatTimeout: "2 minutes",
  retry: { maximumAttempts: 3, initialInterval: "10 seconds", backoffCoefficient: 3 },
});

export const sourcesChanged = defineSignal<[string[]]>(SOURCES_CHANGED_SIGNAL);
export const syncNow = defineUpdate<PassResult, []>(SYNC_NOW_UPDATE);

/** How often a waiting loop re-reads its Corpus, so an edited interval or a deleted Corpus is seen without waiting out the old interval. */
export const RECHECK_MS = 5 * 60_000;
/** A webhook burst is gathered into one partial pass rather than one pass per delivery. */
export const WEBHOOK_SETTLE_MS = 5_000;
/** Loop turns before continuing as new, so a corpus that lives for years keeps a bounded history. */
const CYCLES_PER_RUN = 200;

export async function corpusSync(input: CorpusSyncInput): Promise<void> {
  const corpus = input.corpus;
  const pending = new Set(input.pendingSourceIds ?? []);
  let fullRequested = input.fullRequested ?? false;
  let lastFullPassAt = input.lastFullPassAt;
  /** Passes started in this run; a run-now waits for the next one. */
  let started = 0;
  let finished: { n: number; result?: PassResult; error?: string } | undefined;
  /** Why the loop ended, once the Corpus is no longer synced. */
  let ended: string | undefined;

  setHandler(sourcesChanged, (sourceIds) => {
    // Empty means "something changed, we do not know what": escalate to a
    // full pass rather than do nothing, or a deletion that sent no event
    // would never be noticed.
    if (sourceIds.length === 0) fullRequested = true;
    else for (const id of sourceIds) pending.add(id);
  });

  // Waits for a full pass that STARTS after the request: one already running
  // may have listed the source before whatever prompted the request.
  setHandler(syncNow, async () => {
    const n = started + 1;
    fullRequested = true;
    await condition(() => ended !== undefined || (finished !== undefined && finished.n >= n));
    if (finished && finished.n >= n) {
      if (finished.result) return finished.result;
      throw ApplicationFailure.nonRetryable(finished.error ?? "the pass failed", "SyncFailed");
    }
    throw ApplicationFailure.nonRetryable(ended ?? "the corpus is no longer synced", "CorpusNotSynced");
  });

  let target = await settings.syncTarget(corpus);
  for (let cycle = 0; target; cycle++) {
    if ((cycle >= CYCLES_PER_RUN || workflowInfo().continueAsNewSuggested) && allHandlersFinished()) {
      await continueAsNew<typeof corpusSync>({
        corpus,
        ...(lastFullPassAt !== undefined ? { lastFullPassAt } : {}),
        ...(pending.size > 0 ? { pendingSourceIds: [...pending] } : {}),
        ...(fullRequested ? { fullRequested } : {}),
      });
    }

    const dueAt = lastFullPassAt === undefined ? Date.now() : lastFullPassAt + target.intervalMs;
    const due = Date.now() >= dueAt;
    if (!due && !fullRequested && pending.size === 0) {
      const signalled = await condition(() => fullRequested || pending.size > 0, Math.min(dueAt - Date.now(), RECHECK_MS));
      // Woken by the clock before the pass is due: re-read the Corpus, so an
      // edited interval applies and a deleted Corpus ends its loop.
      if (!signalled && Date.now() < dueAt) target = await settings.syncTarget(corpus);
      continue;
    }

    const full = due || fullRequested;
    // A partial pass waits a moment, so a burst of deliveries becomes one pass.
    if (!full) await sleep(WEBHOOK_SETTLE_MS);
    // A full pass examines everything, the named resources included; anything
    // named from here on is for the next pass.
    const sourceIds = full || fullRequested ? undefined : [...pending];
    const isFull = sourceIds === undefined;
    if (isFull) fullRequested = false;
    pending.clear();

    const n = ++started;
    let result: PassResult | null = null;
    try {
      result = await passes.runPass(corpus, sourceIds);
      if (!result) break;
      finished = { n, result };
    } catch (error) {
      if (isCancellation(error)) throw error;
      // Retried already (see `passes`). A failed partial pass's resources are
      // the next full pass's: the reconcile is the backstop for exactly this.
      const message = messageOf(error);
      finished = { n, error: message };
      await settings.log(`sync ${corpus}: ${isFull ? "full" : "partial"} pass failed: ${message}`);
    }
    // A failed full pass also waits out the interval: retrying at once would
    // hammer a source that is down.
    if (isFull) lastFullPassAt = Date.now();
    target = await settings.syncTarget(corpus);
  }

  ended = `corpus ${corpus} is no longer synced by this broker`;
  await condition(allHandlersFinished);
}

function messageOf(error: unknown): string {
  // An activity's failure wraps the error it threw; that one says what went wrong.
  const cause = (error as { cause?: unknown })?.cause;
  if (cause instanceof Error) return cause.message;
  return error instanceof Error ? error.message : String(error);
}
