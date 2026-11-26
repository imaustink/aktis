package corpussync

import (
	"context"
	"log"

	"go.temporal.io/sdk/activity"
)

// Activity names, registered by name so the workflow never depends on a Go
// method value.
const (
	SyncTargetActivityName = "CorpusSyncTarget"
	RunPassActivityName    = "CorpusSyncRunPass"
)

// RunPassInput names one pass. Full is explicit rather than "no source ids",
// because a nil and an empty list look the same once encoded.
type RunPassInput struct {
	Corpus    string   `json:"corpus"`
	Full      bool     `json:"full"`
	SourceIDs []string `json:"sourceIds,omitempty"`
}

// CorpusReader is the Corpus side the activities need (*Corpora).
type CorpusReader interface {
	Get(ctx context.Context, name string) (*Synced, error)
	RecordPass(ctx context.Context, name string, report Report) error
}

// Activities are the workflow's side effects.
type Activities struct {
	Corpora CorpusReader
	// SourceFor builds a source carrying the Corpus's own sync token; a
	// source built with another Corpus's token would 403 on every request.
	SourceFor func(*Synced) Source
	// WriterFor builds a writer stamping the Corpus's own allowedRoles.
	WriterFor func(*Synced) Writer
	Logf      func(format string, args ...any)
}

func (a *Activities) logf(format string, args ...any) {
	if a.Logf != nil {
		a.Logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// SyncTarget is the Corpus's current settings, or nil once it is gone or no
// longer synced here -- which ends its loop.
func (a *Activities) SyncTarget(ctx context.Context, corpus string) (*TargetInfo, error) {
	s, err := a.Corpora.Get(ctx, corpus)
	if err != nil || s == nil {
		return nil, err
	}
	return &TargetInfo{IntervalMs: s.IntervalMs, LastReconcileAt: s.LastReconcileAt}, nil
}

// RunPass runs one pass and records it on the Corpus's status. Nil when the
// Corpus is no longer synced. A failing pass returns its error, so Temporal
// retries it -- from the start, which is safe because point ids are content
// hashes and a repeated pass re-embeds nothing it already wrote.
func (a *Activities) RunPass(ctx context.Context, in RunPassInput) (*PassResult, error) {
	s, err := a.Corpora.Get(ctx, in.Corpus)
	if err != nil || s == nil {
		return nil, err
	}

	opts := PassOptions{}
	if !in.Full {
		opts.OnlySourceIDs = append([]string{}, in.SourceIDs...)
	}
	// Only inside an activity: a direct call (a unit test) has no heartbeat.
	// A cancelled pass (worker shutting down, heartbeat timed out) stops at
	// its next step through ctx, so its retry elsewhere does not overlap it
	// for long.
	if activity.IsActivity(ctx) {
		activity.RecordHeartbeat(ctx, Progress{Phase: "start"})
		opts.OnProgress = func(p Progress) { activity.RecordHeartbeat(ctx, p) }
	}

	report, err := RunPass(ctx, a.SourceFor(s), a.WriterFor(s), Target{
		Corpus:     s.Name,
		Label:      s.Name,
		Collection: s.Collection,
	}, opts)
	if err != nil {
		return nil, err
	}

	a.logf("sync %s: indexed=%d removed=%d unchanged=%d failed=%d full=%t",
		in.Corpus, report.Indexed, report.Removed, report.Unchanged, len(report.Failed), report.Full)
	// Reported, never fatal: the chunks are already indexed, and failing the
	// pass to protect a timestamp would throw away real work.
	if err := a.Corpora.RecordPass(ctx, in.Corpus, report); err != nil {
		a.logf("could not publish sync status for %s: %v", in.Corpus, err)
	}

	failed := report.Failed
	if len(failed) > FailedIDsKept {
		failed = failed[:FailedIDsKept]
	}
	if failed == nil {
		failed = []string{}
	}
	return &PassResult{
		Indexed:     report.Indexed,
		Removed:     report.Removed,
		Unchanged:   report.Unchanged,
		Failed:      failed,
		FailedCount: len(report.Failed),
		Full:        report.Full,
	}, nil
}
