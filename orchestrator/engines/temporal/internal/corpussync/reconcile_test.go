package corpussync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func chunk(source, hash string) Chunk {
	return Chunk{SourceID: source, ContentHash: hash, Text: hash}
}

func indexedChunk(source, hash string) IndexedChunk {
	return IndexedChunk{SourceID: source, ContentHash: hash}
}

func hashes(chunks []Chunk) []string {
	out := []string{}
	for _, c := range chunks {
		out = append(out, c.ContentHash)
	}
	return out
}

func TestReconcileEmbedsOnlyWhatChanged(t *testing.T) {
	plan := Reconcile(
		[]Chunk{chunk("p1", "a"), chunk("p1", "b2"), chunk("p2", "c")},
		[]IndexedChunk{indexedChunk("p1", "a"), indexedChunk("p1", "b"), indexedChunk("p2", "c")},
		Scope{Full: true},
	)
	require.Equal(t, []string{"b2"}, hashes(plan.Upsert))
	require.Equal(t, []string{"b"}, plan.Remove)
	require.Equal(t, 2, plan.Unchanged)
}

func TestReconcileCostsNothingWhenNothingMoved(t *testing.T) {
	plan := Reconcile([]Chunk{chunk("p1", "a")}, []IndexedChunk{indexedChunk("p1", "a")}, Scope{Full: true})
	require.Empty(t, plan.Upsert)
	require.Empty(t, plan.Remove)
	require.Equal(t, 1, plan.Unchanged)
}

func TestReconcileRemovesWhatAFullPassNoLongerFinds(t *testing.T) {
	plan := Reconcile([]Chunk{chunk("p1", "a")}, []IndexedChunk{indexedChunk("p1", "a"), indexedChunk("gone", "z")}, Scope{Full: true})
	require.Equal(t, []string{"z"}, plan.Remove)
}

func TestReconcileNeverDeletesOutsideThePartialPassScope(t *testing.T) {
	// A webhook pass looked at p1 only; p2 and p3 are absent from `current`
	// because nobody fetched them, not because they are gone.
	plan := Reconcile(
		[]Chunk{chunk("p1", "a")},
		[]IndexedChunk{indexedChunk("p1", "a"), indexedChunk("p2", "b"), indexedChunk("p3", "c")},
		Scope{SourceIDs: []string{"p1"}},
	)
	require.Empty(t, plan.Remove)
}

func TestReconcileDeletesAPartiallySyncedPagesLostContent(t *testing.T) {
	plan := Reconcile(
		[]Chunk{chunk("p1", "a")},
		[]IndexedChunk{indexedChunk("p1", "a"), indexedChunk("p1", "old"), indexedChunk("p2", "b")},
		Scope{SourceIDs: []string{"p1"}},
	)
	require.Equal(t, []string{"old"}, plan.Remove)
}

func TestReconcileRemovesEveryChunkOfAPageAPartialPassFoundEmpty(t *testing.T) {
	plan := Reconcile(nil, []IndexedChunk{indexedChunk("p1", "a"), indexedChunk("p1", "b"), indexedChunk("p2", "c")}, Scope{SourceIDs: []string{"p1"}})
	require.Equal(t, []string{"a", "b"}, plan.Remove)
}

func TestReconcileWritesARepeatedPassageOnce(t *testing.T) {
	plan := Reconcile([]Chunk{chunk("p1", "a"), chunk("p1", "a")}, nil, Scope{Full: true})
	require.Equal(t, []string{"a"}, hashes(plan.Upsert))
}

func TestReconcileTreatsAnEmptyCorpusAsAllNew(t *testing.T) {
	plan := Reconcile([]Chunk{chunk("p1", "a"), chunk("p2", "b")}, nil, Scope{Full: true})
	require.Equal(t, []string{"a", "b"}, hashes(plan.Upsert))
	require.Empty(t, plan.Remove)
}

func TestReconcileDoesNothingForAPartialPassThatTouchedNothing(t *testing.T) {
	plan := Reconcile(nil, []IndexedChunk{indexedChunk("p1", "a")}, Scope{SourceIDs: nil})
	require.Empty(t, plan.Upsert)
	require.Empty(t, plan.Remove)
}
