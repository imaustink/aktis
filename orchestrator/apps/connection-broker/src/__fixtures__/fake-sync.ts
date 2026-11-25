import { ApplicationFailure } from "@temporalio/common";
import type { PassResult, SyncActivities } from "../sync/model.js";

/**
 * Stand-in activities for one or more corpora: records every pass, can hold
 * a pass open (to deliver a webhook or a run-now while it runs), fail one, or
 * make a corpus disappear.
 */
export class FakeSyncSource {
  intervalMs = 60_000;
  /** What the Corpus's status says about its last full pass, if anything. */
  lastReconcileAt: number | undefined;
  /** Corpora that are synced; one removed here is "gone". */
  readonly present = new Set<string>();
  readonly passes: { corpus: string; sourceIds?: string[] }[] = [];
  readonly logs: string[] = [];
  private active = 0;
  /** The most passes ever running at once, across every corpus. */
  maxActive = 0;
  private held: (() => void) | undefined;
  private holding: Promise<void> | undefined;
  /** The next pass fails, without retries, with this message. */
  failNext: string | undefined;

  constructor(...corpora: string[]) {
    for (const c of corpora) this.present.add(c);
  }

  /** Holds every pass that starts from now until `release()`. */
  hold(): void {
    this.holding = new Promise((resolve) => (this.held = resolve));
  }

  release(): void {
    this.held?.();
    this.holding = undefined;
  }

  passesOf(corpus: string): { sourceIds?: string[] }[] {
    return this.passes.filter((p) => p.corpus === corpus).map(({ corpus: _, ...pass }) => pass);
  }

  activities(): SyncActivities {
    return {
      syncTarget: async (corpus) =>
        this.present.has(corpus)
          ? { intervalMs: this.intervalMs, ...(this.lastReconcileAt !== undefined ? { lastReconcileAt: this.lastReconcileAt } : {}) }
          : null,
      runPass: async (corpus, sourceIds) => {
        if (!this.present.has(corpus)) return null;
        this.active += 1;
        this.maxActive = Math.max(this.maxActive, this.active);
        try {
          this.passes.push({ corpus, ...(sourceIds ? { sourceIds } : {}) });
          if (this.holding) await this.holding;
          if (this.failNext) {
            const message = this.failNext;
            this.failNext = undefined;
            throw ApplicationFailure.nonRetryable(message);
          }
          const result: PassResult = {
            indexed: sourceIds?.length ?? 3,
            removed: 0,
            unchanged: 1,
            failed: [],
            failedCount: 0,
            full: sourceIds === undefined,
          };
          return result;
        } finally {
          this.active -= 1;
        }
      },
      log: async (line) => {
        this.logs.push(line);
      },
    };
  }
}

export async function until(check: () => boolean | Promise<boolean>, what: string, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}
