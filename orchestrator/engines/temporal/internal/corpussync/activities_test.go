package corpussync

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeCorpusReader struct {
	synced   map[string]*Synced
	getErr   error
	recorded map[string][]Report
	recErr   error
}

func (f *fakeCorpusReader) Get(_ context.Context, name string) (*Synced, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.synced[name], nil
}

func (f *fakeCorpusReader) RecordPass(_ context.Context, name string, r Report) error {
	if f.recorded == nil {
		f.recorded = map[string][]Report{}
	}
	f.recorded[name] = append(f.recorded[name], r)
	return f.recErr
}

func activitiesOver(reader *fakeCorpusReader, sources map[string]*fakeSource, writers map[string]*fakeWriter) *Activities {
	return &Activities{
		Corpora:   reader,
		SourceFor: func(s *Synced) Source { return sources[s.Token] },
		WriterFor: func(s *Synced) Writer { return writers[s.Name] },
		Logf:      func(string, ...any) {},
	}
}

func TestRunPassActivityUsesEachCorpussOwnTokenAndWriter(t *testing.T) {
	reader := &fakeCorpusReader{synced: map[string]*Synced{
		"eng":   {Name: "eng", Collection: "c-eng", Token: "tok-eng", IntervalMs: 1},
		"sales": {Name: "sales", Collection: "c-sales", Token: "tok-sales", IntervalMs: 1},
	}}
	sources := map[string]*fakeSource{
		"tok-eng":   {pages: [][]string{{"e1"}}, docs: map[string]string{"e1": "eng doc"}},
		"tok-sales": {pages: [][]string{{"s1"}}, docs: map[string]string{"s1": "sales doc"}},
	}
	writers := map[string]*fakeWriter{"eng": newFakeWriter(), "sales": newFakeWriter()}
	acts := activitiesOver(reader, sources, writers)

	for _, corpus := range []string{"eng", "sales"} {
		res, err := acts.RunPass(context.Background(), RunPassInput{Corpus: corpus, Full: true})
		require.NoError(t, err)
		require.Equal(t, &PassResult{Indexed: 1, Failed: []string{}, Full: true}, res)
	}
	require.Equal(t, []string{"e1"}, writers["eng"].sources())
	require.Equal(t, []string{"s1"}, writers["sales"].sources())
	require.Len(t, reader.recorded["eng"], 1, "each pass is recorded on its Corpus")
}

func TestRunPassActivityRunsAPartialPassForExactlyTheNamedResources(t *testing.T) {
	reader := &fakeCorpusReader{synced: map[string]*Synced{"eng": {Name: "eng", Token: "t"}}}
	source := &fakeSource{pages: [][]string{{"p1", "p2", "p3"}}, docs: map[string]string{"p1": "1", "p2": "2", "p3": "3"}}
	acts := activitiesOver(reader, map[string]*fakeSource{"t": source}, map[string]*fakeWriter{"eng": newFakeWriter()})
	res, err := acts.RunPass(context.Background(), RunPassInput{Corpus: "eng", SourceIDs: []string{"p2"}})
	require.NoError(t, err)
	require.False(t, res.Full)
	require.Equal(t, []string{"p2"}, source.fetched)
	require.Zero(t, source.listings)
}

func TestRunPassActivityLetsAFailingPassFailSoTemporalRetriesIt(t *testing.T) {
	reader := &fakeCorpusReader{synced: map[string]*Synced{"eng": {Name: "eng", Token: "t"}}}
	acts := activitiesOver(reader, map[string]*fakeSource{"t": {endless: true}}, map[string]*fakeWriter{"eng": newFakeWriter()})
	_, err := acts.RunPass(context.Background(), RunPassInput{Corpus: "eng", Full: true})
	require.Error(t, err)
	require.Empty(t, reader.recorded)
}

func TestRunPassActivityKeepsOnlyTheFirstFewFailedIDs(t *testing.T) {
	var ids []string
	for i := range 30 {
		ids = append(ids, fmt.Sprintf("p%d", i))
	}
	reader := &fakeCorpusReader{synced: map[string]*Synced{"eng": {Name: "eng", Token: "t"}}}
	acts := activitiesOver(reader, map[string]*fakeSource{"t": {pages: [][]string{ids}}}, map[string]*fakeWriter{"eng": newFakeWriter()})
	res, err := acts.RunPass(context.Background(), RunPassInput{Corpus: "eng", Full: true})
	require.NoError(t, err)
	require.Len(t, res.Failed, FailedIDsKept)
	require.Equal(t, 30, res.FailedCount)
}

func TestRunPassActivitySurvivesAStatusWriteFailure(t *testing.T) {
	reader := &fakeCorpusReader{synced: map[string]*Synced{"eng": {Name: "eng", Token: "t"}}, recErr: errors.New("403")}
	acts := activitiesOver(reader, map[string]*fakeSource{"t": {pages: [][]string{{}}}}, map[string]*fakeWriter{"eng": newFakeWriter()})
	_, err := acts.RunPass(context.Background(), RunPassInput{Corpus: "eng", Full: true})
	require.NoError(t, err)
}

func TestActivitiesAnswerNilForACorpusNotSyncedHere(t *testing.T) {
	acts := activitiesOver(&fakeCorpusReader{}, nil, nil)
	res, err := acts.RunPass(context.Background(), RunPassInput{Corpus: "gone", Full: true})
	require.NoError(t, err)
	require.Nil(t, res)
	info, err := acts.SyncTarget(context.Background(), "gone")
	require.NoError(t, err)
	require.Nil(t, info)
}

func TestSyncTargetFailsRetryablyWhenTheAPIServerCannotAnswer(t *testing.T) {
	acts := activitiesOver(&fakeCorpusReader{getErr: errors.New("connection refused")}, nil, nil)
	_, err := acts.SyncTarget(context.Background(), "eng")
	require.ErrorContains(t, err, "connection refused")
}

func TestSyncTargetReadsTheCorpusFresh(t *testing.T) {
	reader := &fakeCorpusReader{synced: map[string]*Synced{"eng": {Name: "eng", IntervalMs: 60_000, LastReconcileAt: 42}}}
	acts := activitiesOver(reader, nil, nil)
	info, err := acts.SyncTarget(context.Background(), "eng")
	require.NoError(t, err)
	require.Equal(t, &TargetInfo{IntervalMs: 60_000, LastReconcileAt: 42}, info)
	reader.synced["eng"].IntervalMs = 120_000
	info, err = acts.SyncTarget(context.Background(), "eng")
	require.NoError(t, err)
	require.Equal(t, int64(120_000), info.IntervalMs)
}
