package corpussync

import (
	"context"
	"fmt"
)

// ResourceRef is one resource the broker lists, before its body is fetched.
type ResourceRef struct {
	ID string `json:"id"`
}

// Source reads one Corpus's resources. Implemented over the broker's HTTP API
// (HTTPSource) so the third-party credential stays in the broker.
type Source interface {
	// List returns one page; an empty next cursor ends the listing.
	List(ctx context.Context, corpus, cursor string) (refs []ResourceRef, next string, err error)
	Fetch(ctx context.Context, corpus, id string) (Document, error)
}

// Writer is the corpus side: what is already indexed, and where points go.
type Writer interface {
	// Indexed is every chunk in the collection; onPage is called after each
	// page read, so a long read still heartbeats.
	Indexed(ctx context.Context, collection string, onPage func(pages int)) ([]IndexedChunk, error)
	// Upsert embeds and writes chunks; onBatch is called with the running
	// total after each batch written.
	Upsert(ctx context.Context, collection string, chunks []Chunk, onBatch func(done int)) error
	Remove(ctx context.Context, collection string, contentHashes []string) error
}

// Progress is reported after every listed page, fetched resource, page of the
// index read and batch written, and once before removal. A pass run as an
// activity heartbeats from it, so a worker that dies mid-pass is noticed
// within the heartbeat timeout and a live pass is never taken for a dead one.
type Progress struct {
	Phase string `json:"phase"` // list | fetch | indexed | write | remove
	Done  int    `json:"done"`
	Total int    `json:"total,omitempty"`
}

// Target is one pass's Corpus.
type Target struct {
	Corpus     string
	Label      string
	Collection string
	Chunking   ChunkOptions
}

// Report is what a pass did.
type Report struct {
	Indexed   int
	Removed   int
	Unchanged int
	// Failed are resources that could not be fetched: reported, never silently skipped.
	Failed []string
	Full   bool
}

// DefaultMaxPages guards against a driver whose cursor never terminates.
const DefaultMaxPages = 1000

// PassOptions shape one pass.
type PassOptions struct {
	// OnlySourceIDs restricts the pass to these resources (a webhook's). Nil
	// means a full reconcile, the only kind allowed to delete freely.
	OnlySourceIDs []string
	MaxPages      int
	OnProgress    func(Progress)
}

// RunPass runs one sync pass over one Corpus.
//
// A resource that cannot be fetched is reported rather than dropped, and it
// does not count as examined: a read failure says nothing about whether the
// resource still exists, and counting it would let reconciliation read the
// failure as a deletion. For the same reason a full pass that could not read
// everything is downgraded to a partial one over what it did read.
func RunPass(ctx context.Context, source Source, writer Writer, target Target, opts PassOptions) (Report, error) {
	progress := opts.OnProgress
	if progress == nil {
		progress = func(Progress) {}
	}
	full := opts.OnlySourceIDs == nil

	var refs []ResourceRef
	if full {
		maxPages := opts.MaxPages
		if maxPages <= 0 {
			maxPages = DefaultMaxPages
		}
		walked, err := walk(ctx, source, target.Corpus, maxPages, progress)
		if err != nil {
			return Report{}, err
		}
		refs = walked
	} else {
		for _, id := range opts.OnlySourceIDs {
			refs = append(refs, ResourceRef{ID: id})
		}
	}

	var chunks []Chunk
	var fetched, failed []string
	for i, ref := range refs {
		progress(Progress{Phase: "fetch", Done: i, Total: len(refs)})
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}
		doc, err := source.Fetch(ctx, target.Corpus, ref.ID)
		if err != nil {
			// A cancelled pass stops; it did not learn the resource is unreadable.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return Report{}, ctxErr
			}
			failed = append(failed, ref.ID)
			continue
		}
		fetched = append(fetched, ref.ID)
		chunks = append(chunks, ChunkDocument(target.Corpus, target.Label, doc, target.Chunking)...)
	}

	scope := Scope{Full: full && len(failed) == 0}
	if !scope.Full {
		scope.SourceIDs = fetched
	}

	indexed, err := writer.Indexed(ctx, target.Collection, func(pages int) {
		progress(Progress{Phase: "indexed", Done: pages})
	})
	if err != nil {
		return Report{}, err
	}
	plan := Reconcile(chunks, indexed, scope)

	if len(plan.Upsert) > 0 {
		total := len(plan.Upsert)
		if err := writer.Upsert(ctx, target.Collection, plan.Upsert, func(done int) {
			progress(Progress{Phase: "write", Done: done, Total: total})
		}); err != nil {
			return Report{}, err
		}
	}
	if len(plan.Remove) > 0 {
		progress(Progress{Phase: "remove", Done: 0, Total: len(plan.Remove)})
		if err := writer.Remove(ctx, target.Collection, plan.Remove); err != nil {
			return Report{}, err
		}
	}

	return Report{
		Indexed:   len(plan.Upsert),
		Removed:   len(plan.Remove),
		Unchanged: plan.Unchanged,
		Failed:    failed,
		Full:      scope.Full,
	}, nil
}

// walk follows the listing to its end. maxPages is a guard, not a tuning
// knob: a cursor that never terminates would otherwise spin forever holding a
// credential, and look like a hung pass rather than a bug.
func walk(ctx context.Context, source Source, corpus string, maxPages int, progress func(Progress)) ([]ResourceRef, error) {
	var refs []ResourceRef
	cursor := ""
	for page := 0; page < maxPages; page++ {
		got, next, err := source.List(ctx, corpus, cursor)
		if err != nil {
			return nil, err
		}
		refs = append(refs, got...)
		progress(Progress{Phase: "list", Done: page + 1})
		if next == "" {
			return refs, nil
		}
		cursor = next
	}
	return nil, fmt.Errorf("%s did not finish listing within %d pages", corpus, maxPages)
}
