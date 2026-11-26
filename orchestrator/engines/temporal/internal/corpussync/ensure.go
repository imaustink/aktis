package corpussync

import (
	"context"
	"errors"
	"log"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// Ensurer keeps one loop running per synced Corpus. Every replica calls it
// every minute; the workflow id makes that one loop however many ask. A loop
// whose Corpus stopped being synced ends on its own, and a new Corpus gets
// its loop within a minute.
type Ensurer struct {
	Client    client.Client
	TaskQueue string
	// LegacyTaskQueue is where a docs/adr/0058 broker ran these loops in
	// TypeScript. Nothing polls it any more, so a loop left there would
	// hang forever -- or, during the rollout, still be run by an old broker
	// pod alongside the new loop. Each tick terminates them. Empty: off.
	// Remove one release after docs/adr/0059 ships.
	LegacyTaskQueue string
	// Synced lists the Corpora to keep loops for.
	Synced func(ctx context.Context) ([]string, error)
	// All lists every Corpus name, synced or not, for the legacy drain.
	All  func(ctx context.Context) ([]string, error)
	Logf func(format string, args ...any)
}

func (e *Ensurer) logf(format string, args ...any) {
	if e.Logf != nil {
		e.Logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// Run ticks every interval until ctx ends, starting with one at once.
func (e *Ensurer) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		e.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Tick drains legacy loops, then starts any loop that is not running.
// Failures are logged per Corpus: one bad Corpus must not stop the rest.
func (e *Ensurer) Tick(ctx context.Context) {
	if e.LegacyTaskQueue != "" && e.LegacyTaskQueue != e.TaskQueue && e.All != nil {
		names, err := e.All(ctx)
		if err != nil {
			e.logf("corpus-sync: could not list corpora for the legacy drain: %v", err)
		}
		for _, name := range names {
			e.terminateLegacy(ctx, name)
		}
	}

	names, err := e.Synced(ctx)
	if err != nil {
		e.logf("corpus-sync: could not list corpora: %v", err)
		return
	}
	for _, name := range names {
		if err := e.Start(ctx, name); err != nil {
			e.logf("sync %s: could not start its workflow: %v", name, err)
		}
	}
}

// Start makes sure the Corpus's loop is running; one already running is left alone.
func (e *Ensurer) Start(ctx context.Context, corpus string) error {
	_, err := e.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       WorkflowID(e.TaskQueue, corpus),
		TaskQueue:                e.TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, WorkflowName, Input{Corpus: corpus})
	return err
}

func (e *Ensurer) terminateLegacy(ctx context.Context, corpus string) {
	id := WorkflowID(e.LegacyTaskQueue, corpus)
	err := e.Client.TerminateWorkflow(ctx, id, "", "corpus sync moved to the corpus-sync worker (docs/adr/0059)")
	var notFound *serviceerror.NotFound
	switch {
	case err == nil:
		e.logf("sync %s: terminated its loop on the legacy task queue %s", corpus, e.LegacyTaskQueue)
	case errors.As(err, &notFound):
		// Never existed, or already closed: nothing to drain.
	default:
		e.logf("sync %s: could not terminate legacy loop %s: %v", corpus, id, err)
	}
}
