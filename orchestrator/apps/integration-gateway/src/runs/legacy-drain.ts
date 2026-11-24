import { hostname } from "node:os";
import { randomUUID } from "node:crypto";
import type { DeferredEventStore } from "../deferred-events.js";
import { InflightRunRecovery, type InflightRunStore } from "../inflight-runs.js";
import { gatewayOwedRunTerminal, LabelReconciler, type PendingLabelStore } from "../label-reconciler.js";
import type { TemporalRuns } from "./temporal.js";

/**
 * Hands what a pre-Temporal gateway left in Redis to the run workflows
 * (docs/adr/0057, "Migration"). Runs no new code path writes to: the old
 * stores only drain. Remove this module, inflight-runs.ts, deferred-events.ts
 * and label-reconciler.ts one release after 0057 ships.
 *
 *  - An in-flight run whose lease lapsed (its old pod is gone) becomes an
 *    `eventRun` that starts at the poll, skipping the steps the record says
 *    are done. The lease is what keeps this from racing an old pod that is
 *    still finishing it during the rolling update.
 *  - A held run event is claimed (deleted) and becomes a `heldRunEvent`; it
 *    is put back if the workflow can't be started. Claiming is what the old
 *    pods' sweep does before publishing, so exactly one side takes it.
 *  - Owed label removals keep the old reconciler: a record whose run is in
 *    neither store is finished, and its label comes off.
 */
export interface LegacyRunDrainOptions {
  runs: TemporalRuns;
  inflightRunStore?: InflightRunStore;
  deferredEventStore?: DeferredEventStore;
  pendingLabelStore?: PendingLabelStore;
  removeLabel: (owner: string, repo: string, issueNumber: number, label: string) => Promise<void>;
  log?: (line: string) => void;
  onError?: (error: unknown) => void;
  intervalMs?: number;
}

const DEFAULT_INTERVAL_MS = 60_000;

export class LegacyRunDrain {
  private readonly recovery: InflightRunRecovery | undefined;
  private readonly labels: LabelReconciler | undefined;
  private readonly log: (line: string) => void;
  private readonly onError: (error: unknown) => void;
  private timer: ReturnType<typeof setInterval> | undefined;
  private draining = false;

  constructor(private readonly o: LegacyRunDrainOptions) {
    this.log = o.log ?? ((l) => console.error(l));
    this.onError = o.onError ?? ((e) => console.error(e));
    const store = o.inflightRunStore;
    this.recovery = store
      ? new InflightRunRecovery({
          store,
          owner: `${hostname()}-${randomUUID().slice(0, 8)}`,
          isRunning: () => false,
          intervalMs: o.intervalMs ?? DEFAULT_INTERVAL_MS,
          onError: this.onError,
          resume: async (run) => {
            await o.runs.startEventRun({
              invocationId: run.invocationId,
              envelope: run.envelope,
              ...(run.sender ? { sender: run.sender } : {}),
              trigger: run.trigger,
              ...(run.traceparent ? { traceparent: run.traceparent } : {}),
              resumed: {
                ...(run.ackComment ? { ackComment: run.ackComment } : {}),
                ...(run.remoteControlPosted ? { remoteControlPosted: true } : {}),
                ...(run.resultPosted ? { resultPosted: true } : {}),
                ...(run.emitted ? { emitted: true } : {}),
              },
            });
            await store.clear(run.key);
            this.log(`legacy drain: run ${run.key} (trigger ${run.trigger.name}) handed to its workflow`);
          },
        })
      : undefined;
    this.labels = o.pendingLabelStore
      ? new LabelReconciler({
          store: o.pendingLabelStore,
          isRunTerminal: gatewayOwedRunTerminal({ isRunningHere: () => false, ...(store ? { inflightRunStore: store } : {}) }),
          removeLabel: o.removeLabel,
          onError: this.onError,
          intervalMs: o.intervalMs ?? DEFAULT_INTERVAL_MS,
        })
      : undefined;
  }

  /** One pass over the held events. Never throws. */
  async drainHeldOnce(): Promise<void> {
    const store = this.o.deferredEventStore;
    if (!store || this.draining) return;
    this.draining = true;
    try {
      for (const ev of (await store.list()).sort((a, b) => a.createdAt - b.createdAt)) {
        if (!(await store.claim(ev.id))) continue;
        try {
          await this.o.runs.startHeld(`legacy-${ev.id}`, {
            completed: {
              envelope: ev.envelope,
              ...(ev.sender ? { sender: ev.sender } : {}),
              trigger: ev.trigger,
              finished: ev.finished,
              ...(ev.traceparent ? { traceparent: ev.traceparent } : {}),
            },
            ...(ev.waitFor ? { waitFor: ev.waitFor } : {}),
            ...(ev.parkedOn ? { parkedOn: ev.parkedOn } : {}),
            ...(ev.queue ? { queue: ev.queue } : {}),
            ...(ev.blocked ? { blocked: true } : {}),
          });
          this.log(`legacy drain: held run event ${ev.id} (trigger ${ev.trigger.name}) handed to its workflow`);
        } catch (error) {
          // Put it back for the next pass.
          await store.save(ev);
          this.onError(error);
        }
      }
    } catch (error) {
      this.onError(error);
    } finally {
      this.draining = false;
    }
  }

  start(): void {
    this.recovery?.start();
    this.labels?.start();
    if (this.o.deferredEventStore && !this.timer) {
      void this.drainHeldOnce();
      this.timer = setInterval(() => void this.drainHeldOnce(), this.o.intervalMs ?? DEFAULT_INTERVAL_MS);
      this.timer.unref?.();
    }
  }

  stop(): void {
    this.recovery?.stop();
    this.labels?.stop();
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }
}
