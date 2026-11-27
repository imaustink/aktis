/**
 * What this broker and corpus-sync, the Go worker that runs each Corpus's
 * sync workflow, agree on (docs/adr/0059 §4). The broker only starts, signals
 * and updates those workflows; it runs none of them.
 *
 * PARITY: orchestrator/engines/temporal/internal/corpussync/contract.go.
 * Every name and JSON shape here is mirrored there; change both together.
 */

/** The workflow type corpus-sync registers. */
export const WORKFLOW_NAME = "CorpusSync";

/** corpus-sync's queue when TEMPORAL_TASK_QUEUE is unset. */
export const DEFAULT_TASK_QUEUE = "corpus-sync";

/** Workflow ids live under the task queue (docs/adr/0057 §5). */
export function workflowId(taskQueue: string, corpus: string): string {
  return `${taskQueue}/corpus-sync:${corpus}`;
}

/** Signal: a provider webhook named resources that changed. Empty means "something changed; we do not know what" -- a full pass. */
export const SOURCES_CHANGED_SIGNAL = "sourcesChanged";

/** Update: run a full pass that starts after this request, and answer its result (POST /corpora/:name/sync). */
export const SYNC_NOW_UPDATE = "syncNow";

/** The update's failure type when its Corpus stopped being synced while it waited. */
export const CORPUS_NOT_SYNCED = "CorpusNotSynced";

/** The workflow's input, as the broker starts it. */
export interface CorpusSyncInput {
  corpus: string;
}

/** A pass's outcome: the run-now's answer. */
export interface PassResult {
  indexed: number;
  removed: number;
  unchanged: number;
  /** The first few resources that could not be fetched. */
  failed: string[];
  failedCount: number;
  full: boolean;
}
