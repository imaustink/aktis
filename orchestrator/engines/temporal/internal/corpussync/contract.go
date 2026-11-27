// Package corpussync indexes each Corpus into its Qdrant collection from one
// long-lived Temporal workflow per Corpus (docs/adr/0058 §2, moved to Go by
// docs/adr/0059). A pass lists and fetches through connection-broker's HTTP
// API with the Corpus's own sync token, so the third-party credential never
// leaves the broker (docs/adr/0038 §3); it chunks, embeds, reconciles against
// what is indexed and records the outcome on the Corpus's status.
package corpussync

import "fmt"

// What connection-broker and this worker agree on (docs/adr/0059 §4). The
// broker starts, signals and updates these workflows from TypeScript.
//
// PARITY: orchestrator/apps/connection-broker/src/sync/contract.ts. Every
// name and JSON shape in this file is mirrored there; change both together.
const (
	// WorkflowName is the workflow type the broker starts by name.
	WorkflowName = "CorpusSync"
	// DefaultTaskQueue is the queue the worker polls when
	// CORPUS_SYNC_TASK_QUEUE is unset.
	DefaultTaskQueue = "corpus-sync"
	// SourcesChangedSignal carries a webhook's resource ids ([]string). Empty
	// means "something changed; we do not know what" -- a full pass.
	SourcesChangedSignal = "sourcesChanged"
	// SyncNowUpdate runs a full pass that starts after the request and
	// answers its PassResult (POST /corpora/:name/sync).
	SyncNowUpdate = "syncNow"

	// ErrTypeSyncFailed fails a SyncNowUpdate whose pass failed after its retries.
	ErrTypeSyncFailed = "SyncFailed"
	// ErrTypeCorpusNotSynced fails a SyncNowUpdate whose Corpus stopped being synced.
	ErrTypeCorpusNotSynced = "CorpusNotSynced"
)

// WorkflowID is one Corpus's loop. Ids live under the task queue
// (docs/adr/0057 §5), so two releases sharing a namespace never share a loop.
func WorkflowID(taskQueue, corpus string) string {
	return fmt.Sprintf("%s/corpus-sync:%s", taskQueue, corpus)
}

// Input starts a loop; everything past Corpus is state carried across
// continue-as-new.
type Input struct {
	Corpus string `json:"corpus"`
	// LastFullPassAt is the workflow time (ms since the epoch) the last full
	// pass ended; the next is due an interval later. Zero: none yet.
	LastFullPassAt int64 `json:"lastFullPassAt,omitempty"`
	// PendingSourceIDs are resources a webhook named that no pass has examined.
	PendingSourceIDs []string `json:"pendingSourceIds,omitempty"`
	// FullRequested is a full pass asked for (an empty webhook, a run-now)
	// that has not started.
	FullRequested bool `json:"fullRequested,omitempty"`
}

// PassResult is a pass's outcome as the workflow keeps it, and the run-now's answer.
type PassResult struct {
	Indexed   int `json:"indexed"`
	Removed   int `json:"removed"`
	Unchanged int `json:"unchanged"`
	// Failed lists the first FailedIDsKept resources that could not be
	// fetched, so a broken source cannot bloat the history.
	Failed []string `json:"failed"`
	// FailedCount is how many failed in all.
	FailedCount int  `json:"failedCount"`
	Full        bool `json:"full"`
}

// FailedIDsKept caps PassResult.Failed.
const FailedIDsKept = 20

// TargetInfo is what the workflow needs to know about its Corpus right now.
// The activity answers nil when the Corpus is no longer synced.
type TargetInfo struct {
	IntervalMs int64 `json:"intervalMs"`
	// LastReconcileAt is the status's lastReconcileTime (ms since the epoch),
	// zero when it has none. A loop started afresh schedules from it.
	LastReconcileAt int64 `json:"lastReconcileAt,omitempty"`
}
