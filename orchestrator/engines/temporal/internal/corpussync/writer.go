package corpussync

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aktisai/orchestrator/engines/temporal/internal/vectorstore"
)

// PointStore is one collection's slice of vectorstore.Qdrant the writer
// needs. vectorstore owns the point id derivation and the payload layout the
// retrieval side reads, so the writer cannot drift from them.
type PointStore interface {
	Exists(ctx context.Context) (bool, error)
	EnsureCollection(ctx context.Context) error
	Upsert(ctx context.Context, records []vectorstore.Record) error
	Delete(ctx context.Context, ids []string) error
	Scroll(ctx context.Context, pageSize uint32, page func([]vectorstore.PointRef) error) error
}

// upsertBatch is how many chunks are embedded and written per request. It
// mirrors the scroll page size and stays well under the embeddings
// endpoint's input cap (BatchingEmbedder enforces the token cap).
const upsertBatch = 256

// QdrantWriter writes one Corpus's chunks.
//
// Per Corpus, not shared: every point carries its Corpus's allowedRoles, and
// a shared writer would stamp one client's roles onto another's chunks. A
// corpus point is never `unrestricted`; broad access is expressed by granting
// roles widely, not by opting out of the filter (ADR 0039 §4).
type QdrantWriter struct {
	// Store returns the store for a collection.
	Store        func(collection string) PointStore
	AllowedRoles []string
}

// Indexed reads the whole collection: reconcile compares it against what the
// source listed to decide what to delete, and a partial read would present
// surviving chunks as absent. A collection that does not exist yet has
// nothing indexed -- the state every Corpus is in before its first pass.
func (w *QdrantWriter) Indexed(ctx context.Context, collection string, onPage func(int)) ([]IndexedChunk, error) {
	store := w.Store(collection)
	exists, err := store.Exists(ctx)
	if err != nil || !exists {
		return nil, err
	}
	var out []IndexedChunk
	pages := 0
	err = store.Scroll(ctx, upsertBatch, func(refs []vectorstore.PointRef) error {
		for _, ref := range refs {
			// A point without a readable id is skipped rather than guessed
			// at: absent would make a full reconcile delete it, and a wrong
			// id would strand it.
			if ref.ID == "" {
				continue
			}
			var descriptor struct {
				SourceID string `json:"sourceId"`
			}
			// An unparseable descriptor still names a real point, keyed by
			// its hash; only its source is unknown.
			_ = json.Unmarshal([]byte(ref.Descriptor), &descriptor)
			out = append(out, IndexedChunk{ContentHash: ref.ID, SourceID: descriptor.SourceID})
		}
		pages++
		if onPage != nil {
			onPage(pages)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (w *QdrantWriter) Upsert(ctx context.Context, collection string, chunks []Chunk, onBatch func(int)) error {
	if len(chunks) == 0 {
		return nil
	}
	store := w.Store(collection)
	if err := store.EnsureCollection(ctx); err != nil {
		// Another replica may have created it between the check and the
		// create; that is success.
		if exists, existsErr := store.Exists(ctx); existsErr != nil || !exists {
			return err
		}
	}
	roles := w.AllowedRoles
	if roles == nil {
		roles = []string{}
	}
	for start := 0; start < len(chunks); start += upsertBatch {
		batch := chunks[start:min(start+upsertBatch, len(chunks))]
		records := make([]vectorstore.Record, len(batch))
		for i, c := range batch {
			descriptor, err := json.Marshal(toDescriptor(c))
			if err != nil {
				return fmt.Errorf("encode descriptor: %w", err)
			}
			records[i] = vectorstore.Record{
				ID:           c.ContentHash,
				Text:         c.Text,
				Roles:        roles,
				Unrestricted: false,
				Hidden:       false,
				Descriptor:   descriptor,
			}
		}
		if err := store.Upsert(ctx, records); err != nil {
			return err
		}
		if onBatch != nil {
			onBatch(start + len(batch))
		}
	}
	return nil
}

func (w *QdrantWriter) Remove(ctx context.Context, collection string, contentHashes []string) error {
	if len(contentHashes) == 0 {
		return nil
	}
	return w.Store(collection).Delete(ctx, contentHashes)
}

// descriptor is the payload's `descriptor`, decoded by corpus.Chunk on the
// retrieval side. Field for field what the TS writer wrote, ACL mirror
// included (nothing reads it yet, but recovering it later would mean
// re-embedding every corpus).
type descriptor struct {
	ConnectionID    string   `json:"connectionId"`
	ConnectionLabel string   `json:"connectionLabel"`
	SourceURL       string   `json:"sourceUrl"`
	SourceID        string   `json:"sourceId"`
	Title           string   `json:"title"`
	UpdatedAt       string   `json:"updatedAt,omitempty"`
	ContentHash     string   `json:"contentHash"`
	Version         string   `json:"version,omitempty"`
	Text            string   `json:"text"`
	ACLPrincipals   []string `json:"aclPrincipals,omitempty"`
	ACLPermissive   *bool    `json:"aclPermissive,omitempty"`
}

func toDescriptor(c Chunk) descriptor {
	return descriptor{
		ConnectionID:    c.ConnectionID,
		ConnectionLabel: c.ConnectionLabel,
		SourceURL:       c.SourceURL,
		SourceID:        c.SourceID,
		Title:           c.Title,
		UpdatedAt:       c.UpdatedAt,
		ContentHash:     c.ContentHash,
		Version:         c.Version,
		Text:            c.Text,
		ACLPrincipals:   c.ACLPrincipals,
		ACLPermissive:   c.ACLPermissive,
	}
}
