package corpussync

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/llm"
)

// scriptedEmbedder answers each text with a one-element vector of its length,
// refusing any request over `limit` inputs with a 400.
type scriptedEmbedder struct {
	limit    int
	calls    [][]string
	failWith error
	short    bool
}

func (e *scriptedEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	e.calls = append(e.calls, texts)
	if e.failWith != nil {
		return nil, e.failWith
	}
	if e.limit > 0 && len(texts) > e.limit {
		return nil, &llm.StatusError{Status: 400, Message: "too many tokens"}
	}
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = []float32{float32(len(t))}
	}
	if e.short {
		out = out[1:]
	}
	return out, nil
}

func TestBatchingEmbedderSplitsAndRetriesARefusedBatchInOrder(t *testing.T) {
	inner := &scriptedEmbedder{limit: 2}
	got, err := BatchingEmbedder{Inner: inner}.Embed(context.Background(), []string{"a", "bb", "ccc", "dddd", "eeeee"})
	require.NoError(t, err)
	require.Equal(t, [][]float32{{1}, {2}, {3}, {4}, {5}}, got)
	require.Greater(t, len(inner.calls), 1)
}

func TestBatchingEmbedderGivesUpOnASingleRefusedInput(t *testing.T) {
	inner := &scriptedEmbedder{failWith: &llm.StatusError{Status: 400, Message: "input too long"}}
	_, err := BatchingEmbedder{Inner: inner}.Embed(context.Background(), []string{"a", "b"})
	require.ErrorContains(t, err, "input too long")
	require.Equal(t, [][]string{{"a", "b"}, {"a"}}, inner.calls, "the pair, then the first half alone, then stop")
}

func TestBatchingEmbedderDoesNotSplitOnOtherFailures(t *testing.T) {
	inner := &scriptedEmbedder{failWith: errors.New("connection reset")}
	_, err := BatchingEmbedder{Inner: inner}.Embed(context.Background(), []string{"a", "b"})
	require.ErrorContains(t, err, "connection reset")
	require.Len(t, inner.calls, 1)
}

func TestBatchingEmbedderRefusesTheWrongNumberOfVectors(t *testing.T) {
	_, err := BatchingEmbedder{Inner: &scriptedEmbedder{short: true}}.Embed(context.Background(), []string{"a", "b"})
	require.ErrorContains(t, err, "1 vectors for 2 inputs")
}

func TestBatchingEmbedderDoesNotCallOutForNothing(t *testing.T) {
	inner := &scriptedEmbedder{}
	got, err := BatchingEmbedder{Inner: inner}.Embed(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Empty(t, inner.calls)
}

func TestBatchInputsStartsANewBatchBeforeTheTokenBudget(t *testing.T) {
	big := strings.Repeat("x", 400_000) // ~133k estimated tokens
	sizes := []int{}
	for _, b := range batchInputs([]string{big, big, big}) {
		sizes = append(sizes, len(b))
	}
	require.Equal(t, []int{1, 1, 1}, sizes)
}
