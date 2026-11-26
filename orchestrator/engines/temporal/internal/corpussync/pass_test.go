package corpussync

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeSource serves pages of refs and documents by id.
type fakeSource struct {
	mu       sync.Mutex
	pages    [][]string
	docs     map[string]string // id -> markdown
	failing  map[string]bool
	endless  bool
	fetched  []string
	listings int
}

func (s *fakeSource) List(_ context.Context, _ string, cursor string) ([]ResourceRef, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listings++
	if s.endless {
		return nil, "more", nil
	}
	page := 0
	if cursor != "" {
		page = int(cursor[0] - '0')
	}
	var refs []ResourceRef
	for _, id := range s.pages[page] {
		refs = append(refs, ResourceRef{ID: id})
	}
	next := ""
	if page+1 < len(s.pages) {
		next = string(rune('0' + page + 1))
	}
	return refs, next, nil
}

func (s *fakeSource) Fetch(_ context.Context, _ string, id string) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fetched = append(s.fetched, id)
	if s.failing[id] {
		return Document{}, &SourceError{Kind: SourceTransient, Message: "503"}
	}
	md, ok := s.docs[id]
	if !ok {
		return Document{}, &SourceError{Kind: SourceDenied, Message: "403"}
	}
	return Document{ID: id, Title: id, URL: "https://x/" + id, Markdown: md}, nil
}

// fakeWriter keeps points in memory, keyed by content hash.
type fakeWriter struct {
	mu      sync.Mutex
	points  map[string]IndexedChunk
	upserts [][]Chunk
	removes [][]string
}

func newFakeWriter() *fakeWriter { return &fakeWriter{points: map[string]IndexedChunk{}} }

func (w *fakeWriter) Indexed(_ context.Context, _ string, onPage func(int)) ([]IndexedChunk, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []IndexedChunk
	for _, p := range w.points {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ContentHash < out[j].ContentHash })
	if onPage != nil {
		onPage(1)
	}
	return out, nil
}

func (w *fakeWriter) Upsert(_ context.Context, _ string, chunks []Chunk, onBatch func(int)) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.upserts = append(w.upserts, chunks)
	for _, c := range chunks {
		w.points[c.ContentHash] = IndexedChunk{ContentHash: c.ContentHash, SourceID: c.SourceID}
	}
	if onBatch != nil {
		onBatch(len(chunks))
	}
	return nil
}

func (w *fakeWriter) Remove(_ context.Context, _ string, contentHashes []string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.removes = append(w.removes, contentHashes)
	for _, h := range contentHashes {
		delete(w.points, h)
	}
	return nil
}

func (w *fakeWriter) sources() []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range w.points {
		if !seen[p.SourceID] {
			seen[p.SourceID] = true
			out = append(out, p.SourceID)
		}
	}
	sort.Strings(out)
	return out
}

var passTarget = Target{Corpus: "eng", Label: "eng", Collection: "corpus-eng"}

func TestRunPassReportsProgressInEveryPhase(t *testing.T) {
	source := &fakeSource{pages: [][]string{{"p1"}}, docs: map[string]string{"p1": "one"}}
	writer := newFakeWriter()
	writer.points["stale"] = IndexedChunk{ContentHash: "stale", SourceID: "p9"}
	phases := map[string]bool{}
	_, err := RunPass(context.Background(), source, writer, passTarget, PassOptions{OnProgress: func(p Progress) { phases[p.Phase] = true }})
	require.NoError(t, err)
	require.Equal(t, map[string]bool{"list": true, "fetch": true, "indexed": true, "write": true, "remove": true}, phases)
}

func TestRunPassWalksEveryPageAndIndexesWhatItFinds(t *testing.T) {
	source := &fakeSource{pages: [][]string{{"p1", "p2"}, {"p3"}}, docs: map[string]string{"p1": "one", "p2": "two", "p3": "three"}}
	writer := newFakeWriter()
	report, err := RunPass(context.Background(), source, writer, passTarget, PassOptions{})
	require.NoError(t, err)
	require.Equal(t, 3, report.Indexed)
	require.True(t, report.Full)
	require.Equal(t, []string{"p1", "p2", "p3"}, writer.sources())
}

func TestRunPassReEmbedsNothingWhenNothingChanged(t *testing.T) {
	source := &fakeSource{pages: [][]string{{"p1"}}, docs: map[string]string{"p1": "one"}}
	writer := newFakeWriter()
	_, err := RunPass(context.Background(), source, writer, passTarget, PassOptions{})
	require.NoError(t, err)
	report, err := RunPass(context.Background(), source, writer, passTarget, PassOptions{})
	require.NoError(t, err)
	require.Equal(t, 0, report.Indexed)
	require.Equal(t, 1, report.Unchanged)
	require.Len(t, writer.upserts, 1)
}

func TestRunPassRemovesAPageAFullPassNoLongerFinds(t *testing.T) {
	source := &fakeSource{pages: [][]string{{"p1", "p2"}}, docs: map[string]string{"p1": "one", "p2": "two"}}
	writer := newFakeWriter()
	_, err := RunPass(context.Background(), source, writer, passTarget, PassOptions{})
	require.NoError(t, err)
	source.pages = [][]string{{"p1"}}
	report, err := RunPass(context.Background(), source, writer, passTarget, PassOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, report.Removed)
	require.Equal(t, []string{"p1"}, writer.sources())
}

func TestRunPassDoesNotDeleteOutsideAWebhookPass(t *testing.T) {
	source := &fakeSource{pages: [][]string{{"p1", "p2"}}, docs: map[string]string{"p1": "one", "p2": "two"}}
	writer := newFakeWriter()
	_, err := RunPass(context.Background(), source, writer, passTarget, PassOptions{})
	require.NoError(t, err)
	source.docs["p1"] = "one, edited"
	report, err := RunPass(context.Background(), source, writer, passTarget, PassOptions{OnlySourceIDs: []string{"p1"}})
	require.NoError(t, err)
	require.False(t, report.Full)
	require.Equal(t, []string{"p1"}, source.fetched[len(source.fetched)-1:])
	require.Equal(t, []string{"p1", "p2"}, writer.sources())
	require.Equal(t, 0, source.listings-1, "a partial pass lists nothing")
}

func TestRunPassDowngradesAFullPassThatCouldNotReadEverything(t *testing.T) {
	source := &fakeSource{pages: [][]string{{"p1", "p2"}}, docs: map[string]string{"p1": "one", "p2": "two"}}
	writer := newFakeWriter()
	_, err := RunPass(context.Background(), source, writer, passTarget, PassOptions{})
	require.NoError(t, err)
	// p2 is down, not gone: a full pass must not read the outage as a deletion.
	source.failing = map[string]bool{"p2": true}
	report, err := RunPass(context.Background(), source, writer, passTarget, PassOptions{})
	require.NoError(t, err)
	require.False(t, report.Full)
	require.Equal(t, []string{"p2"}, report.Failed)
	require.Equal(t, []string{"p1", "p2"}, writer.sources())
}

func TestRunPassReportsAnUnreadableResource(t *testing.T) {
	source := &fakeSource{pages: [][]string{{"p1", "secret"}}, docs: map[string]string{"p1": "one"}}
	report, err := RunPass(context.Background(), source, newFakeWriter(), passTarget, PassOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"secret"}, report.Failed)
}

func TestRunPassRefusesToWalkForever(t *testing.T) {
	_, err := RunPass(context.Background(), &fakeSource{endless: true}, newFakeWriter(), passTarget, PassOptions{MaxPages: 3})
	require.ErrorContains(t, err, "did not finish listing within 3 pages")
}

func TestRunPassWritesNothingWhenThereIsNothingToDo(t *testing.T) {
	writer := newFakeWriter()
	_, err := RunPass(context.Background(), &fakeSource{pages: [][]string{{}}}, writer, passTarget, PassOptions{})
	require.NoError(t, err)
	require.Empty(t, writer.upserts)
	require.Empty(t, writer.removes)
}

func TestRunPassStopsWhenCancelledRatherThanCountingFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	source := &fakeSource{pages: [][]string{{"p1", "p2"}}, docs: map[string]string{"p1": "one", "p2": "two"}}
	writer := newFakeWriter()
	_, err := RunPass(ctx, source, writer, passTarget, PassOptions{OnProgress: func(p Progress) {
		if p.Phase == "fetch" && p.Done == 1 {
			cancel()
		}
	}})
	require.True(t, errors.Is(err, context.Canceled))
	require.Empty(t, writer.upserts)
}
