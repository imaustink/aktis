package corpussync

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/aktisai/orchestrator/engines/temporal/internal/llm"
)

// Embedder turns texts into vectors, one per input in input order. Satisfied
// by *llm.Embedder.
type Embedder interface {
	Embed(ctx context.Context, inputs []string) ([][]float32, error)
}

// OpenAI's embeddings endpoint caps one request at 2,048 inputs and 300,000
// tokens. A large corpus sent at once 400s on the token cap, so inputs are
// batched under both. The token figure is estimated from length, slightly
// over (about 3 characters a token), and an estimate that is still too low is
// caught by the split-and-retry below.
const (
	maxInputsPerRequest = 2048
	maxTokensPerRequest = 250_000
)

func estimateTokens(text string) int {
	return (utf16Len(text)+2)/3 + 1
}

// BatchingEmbedder keeps each request under the provider's per-request caps,
// and splits and retries a batch the provider still refuses with a 400.
type BatchingEmbedder struct {
	Inner Embedder
}

func (b BatchingEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	vectors := make([][]float32, 0, len(texts))
	for _, batch := range batchInputs(texts) {
		got, err := b.embedBatch(ctx, batch)
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, got...)
	}
	return vectors, nil
}

func (b BatchingEmbedder) embedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	vectors, err := b.Inner.Embed(ctx, texts)
	if err != nil {
		// Recursion ends at one input, where a genuine refusal surfaces
		// instead of looping.
		var status *llm.StatusError
		if errors.As(err, &status) && status.Status == http.StatusBadRequest && len(texts) > 1 {
			mid := (len(texts) + 1) / 2
			head, err := b.embedBatch(ctx, texts[:mid])
			if err != nil {
				return nil, err
			}
			rest, err := b.embedBatch(ctx, texts[mid:])
			if err != nil {
				return nil, err
			}
			return append(head, rest...), nil
		}
		return nil, err
	}
	if len(vectors) != len(texts) {
		return nil, fmt.Errorf("embeddings returned %d vectors for %d inputs", len(vectors), len(texts))
	}
	return vectors, nil
}

// batchInputs splits inputs greedily, in order, under both caps. An input
// that alone exceeds the token budget still gets a batch of its own: an
// over-long chunk is the chunker's concern, not a reason to drop it.
func batchInputs(texts []string) [][]string {
	var batches [][]string
	var current []string
	tokens := 0
	for _, text := range texts {
		estimate := estimateTokens(text)
		if len(current) > 0 && (len(current) >= maxInputsPerRequest || tokens+estimate > maxTokensPerRequest) {
			batches = append(batches, current)
			current, tokens = nil, 0
		}
		current = append(current, text)
		tokens += estimate
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches
}
