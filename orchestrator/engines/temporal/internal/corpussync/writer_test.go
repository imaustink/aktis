package corpussync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/corpus"
	"github.com/aktisai/orchestrator/engines/temporal/internal/vectorstore"
)

type fakeStore struct {
	exists    bool
	createErr error
	upserts   [][]vectorstore.Record
	deletes   [][]string
	pages     [][]vectorstore.PointRef
	pageSize  uint32
}

func (s *fakeStore) Exists(context.Context) (bool, error) { return s.exists, nil }
func (s *fakeStore) EnsureCollection(context.Context) error {
	if s.createErr != nil {
		return s.createErr
	}
	s.exists = true
	return nil
}
func (s *fakeStore) Upsert(_ context.Context, records []vectorstore.Record) error {
	s.upserts = append(s.upserts, records)
	return nil
}
func (s *fakeStore) Delete(_ context.Context, ids []string) error {
	s.deletes = append(s.deletes, ids)
	return nil
}
func (s *fakeStore) Scroll(_ context.Context, pageSize uint32, page func([]vectorstore.PointRef) error) error {
	s.pageSize = pageSize
	for _, p := range s.pages {
		if err := page(p); err != nil {
			return err
		}
	}
	return nil
}

func writerOver(store *fakeStore, roles []string) *QdrantWriter {
	return &QdrantWriter{Store: func(string) PointStore { return store }, AllowedRoles: roles}
}

func TestWriterWritesThePayloadTheRetrievalSideReads(t *testing.T) {
	store := &fakeStore{}
	permissive := false
	c := Chunk{
		ConnectionID: "eng", ConnectionLabel: "eng", SourceID: "p1", SourceURL: "https://x/p1", Title: "P1",
		UpdatedAt: "2026-01-01T00:00:00Z", Version: "3", ContentHash: "h1", Text: "hello",
		ACLPrincipals: []string{"user:1"}, ACLPermissive: &permissive,
	}
	require.NoError(t, writerOver(store, []string{"eng-readers"}).Upsert(context.Background(), "corpus-eng", []Chunk{c}, nil))

	require.Len(t, store.upserts, 1)
	rec := store.upserts[0][0]
	require.Equal(t, "h1", rec.ID)
	require.Equal(t, "hello", rec.Text)
	require.Equal(t, []string{"eng-readers"}, rec.Roles)
	require.False(t, rec.Unrestricted, "a corpus point is always role-gated")
	require.False(t, rec.Hidden)

	// The descriptor decodes as the engine's corpus.Chunk.
	var decoded corpus.Chunk
	require.NoError(t, json.Unmarshal(rec.Descriptor, &decoded))
	require.Equal(t, corpus.Chunk{
		CorpusID: "eng", CorpusLabel: "eng", SourceURL: "https://x/p1", SourceID: "p1", Title: "P1",
		UpdatedAt: "2026-01-01T00:00:00Z", ContentHash: "h1", Version: "3", Text: "hello",
		ACLPrincipals: []string{"user:1"},
	}, decoded)

	// And carries exactly the TS writer's keys, the ACL mirror included.
	var keys map[string]any
	require.NoError(t, json.Unmarshal(rec.Descriptor, &keys))
	require.Equal(t, false, keys["aclPermissive"])
	require.Len(t, keys, 11)
}

func TestWriterWritesEmptyRolesRatherThanNull(t *testing.T) {
	store := &fakeStore{}
	require.NoError(t, writerOver(store, nil).Upsert(context.Background(), "c", []Chunk{{ContentHash: "h"}}, nil))
	require.Equal(t, []string{}, store.upserts[0][0].Roles)
}

func TestWriterSplitsALargeWriteIntoBatches(t *testing.T) {
	store := &fakeStore{}
	var chunks []Chunk
	for i := range 600 {
		chunks = append(chunks, Chunk{ContentHash: fmt.Sprintf("h%d", i), Text: fmt.Sprintf("t%d", i)})
	}
	var progress []int
	require.NoError(t, writerOver(store, nil).Upsert(context.Background(), "c", chunks, func(done int) { progress = append(progress, done) }))
	require.Len(t, store.upserts, 3)
	require.Len(t, store.upserts[0], 256)
	require.Len(t, store.upserts[2], 88)
	require.Equal(t, "h599", store.upserts[2][87].ID)
	require.Equal(t, []int{256, 512, 600}, progress)
}

func TestWriterDoesNothingForAnEmptyWrite(t *testing.T) {
	store := &fakeStore{}
	require.NoError(t, writerOver(store, nil).Upsert(context.Background(), "c", nil, nil))
	require.NoError(t, writerOver(store, nil).Remove(context.Background(), "c", nil))
	require.Empty(t, store.upserts)
	require.Empty(t, store.deletes)
	require.False(t, store.exists, "no collection is created for nothing")
}

func TestWriterToleratesAnotherReplicaCreatingTheCollection(t *testing.T) {
	store := &fakeStore{createErr: errors.New("already exists")}
	w := &QdrantWriter{Store: func(string) PointStore { return store }}
	// Exists answers false, so the create error surfaces.
	require.Error(t, w.Upsert(context.Background(), "c", []Chunk{{ContentHash: "h"}}, nil))
	// Exists answers true (the other replica won the race): success.
	store.exists = true
	require.NoError(t, w.Upsert(context.Background(), "c", []Chunk{{ContentHash: "h"}}, nil))
}

func TestWriterReadsEveryPageOfWhatIsIndexed(t *testing.T) {
	store := &fakeStore{exists: true, pages: [][]vectorstore.PointRef{
		{{ID: "a", Descriptor: `{"sourceId":"p1"}`}, {ID: "", Descriptor: `{"sourceId":"p1"}`}},
		{{ID: "b", Descriptor: `not json`}},
		{},
	}}
	var pages []int
	got, err := writerOver(store, nil).Indexed(context.Background(), "c", func(n int) { pages = append(pages, n) })
	require.NoError(t, err)
	// The point without an id is skipped; the unparseable descriptor still counts.
	require.Equal(t, []IndexedChunk{{ContentHash: "a", SourceID: "p1"}, {ContentHash: "b"}}, got)
	require.Equal(t, []int{1, 2, 3}, pages)
	require.Equal(t, uint32(256), store.pageSize)
}

func TestWriterReportsNothingIndexedForAMissingCollection(t *testing.T) {
	got, err := writerOver(&fakeStore{}, nil).Indexed(context.Background(), "c", nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestWriterRemovesByContentHash(t *testing.T) {
	store := &fakeStore{exists: true}
	require.NoError(t, writerOver(store, nil).Remove(context.Background(), "c", []string{"a", "b"}))
	require.Equal(t, [][]string{{"a", "b"}}, store.deletes)
}
