/**
 * What the Corpus sync workflow and the broker's host side agree on
 * (docs/adr/0058). Imported by workflows.ts, so nothing here may touch Node,
 * the network or the clock.
 */

/** Workflow ids live under the task queue (docs/adr/0057 §5), so two releases sharing a namespace never share a corpus's loop. */
export const workflowIds = {
  corpusSync: (taskQueue: string, corpus: string) => `${taskQueue}/corpus-sync:${corpus}`,
};

export const DEFAULT_TASK_QUEUE = "connection-broker";

/** Signal: a provider webhook named resources that changed. Empty means "something changed; we do not know what" — a full pass. */
export const SOURCES_CHANGED_SIGNAL = "sourcesChanged";
/** Update: run a full pass that starts after this request, and return its result (POST /corpora/:name/sync). */
export const SYNC_NOW_UPDATE = "syncNow";

/** Input to `corpusSync`; everything past `corpus` is state carried across continue-as-new. */
export interface CorpusSyncInput {
  corpus: string;
  /** Workflow time (ms) the last full pass ended; the next is due an interval later. Absent: run one at once. */
  lastFullPassAt?: number;
  /** Resources a webhook named that no pass has examined since. */
  pendingSourceIds?: string[];
  /** A full pass was asked for (an empty webhook, a run-now) and has not started. */
  fullRequested?: boolean;
}

/** What one Corpus's pass needs to know about it right now; `null` when it is no longer synced here. */
export interface SyncTargetInfo {
  intervalMs: number;
}

/** A pass's outcome, as the workflow keeps it. `failed` is capped so a broken source cannot bloat the history. */
export interface PassResult {
  indexed: number;
  removed: number;
  unchanged: number;
  failed: string[];
  /** How many resources failed in all, of which `failed` lists the first few. */
  failedCount: number;
  full: boolean;
}

export const FAILED_IDS_KEPT = 20;

/** The activities the workflow calls (activities.ts). */
export interface SyncActivities {
  /** The Corpus's current sync settings, or `null` once it is gone or no longer syncable. */
  syncTarget(corpus: string): Promise<SyncTargetInfo | null>;
  /**
   * One pass, full when `sourceIds` is absent. Records the Corpus's status
   * when it is done. `null` when the Corpus is no longer syncable.
   */
  runPass(corpus: string, sourceIds?: string[]): Promise<PassResult | null>;
  log(line: string): Promise<void>;
}
