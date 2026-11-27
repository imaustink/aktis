package corpussync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// testdata/ts-parity.json was written by the TypeScript chunker, hasher and
// batcher this package replaces (docs/adr/0059 §5), before they were removed.
// A difference here is not a style choice: a chunk cut or hashed differently
// is a new point id, so every corpus would re-embed on upgrade and the old
// points would be deleted. Fix the Go side; never regenerate the fixture.

type parityFixture struct {
	Chunks []struct {
		Name     string       `json:"name"`
		Options  ChunkOptions `json:"options"`
		Document Document     `json:"document"`
		Chunks   []struct {
			Chunk
			TextSha256 string `json:"textSha256"`
			TextLength int    `json:"textLength"`
		} `json:"chunks"`
	} `json:"chunks"`
	Hashes []struct {
		Connection string `json:"connection"`
		Source     string `json:"source"`
		Text       string `json:"text"`
		Hash       string `json:"hash"`
	} `json:"hashes"`
	Batches []struct {
		Runs []struct {
			Char   string `json:"char"`
			Repeat int    `json:"repeat"`
			Count  int    `json:"count"`
		} `json:"runs"`
		Sizes []int `json:"sizes"`
	} `json:"batches"`
}

func loadParity(t *testing.T) parityFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/ts-parity.json")
	require.NoError(t, err)
	var f parityFixture
	require.NoError(t, json.Unmarshal(raw, &f))
	require.NotEmpty(t, f.Chunks)
	return f
}

func TestChunkDocumentMatchesTheTypeScriptChunker(t *testing.T) {
	for _, c := range loadParity(t).Chunks {
		t.Run(c.Name, func(t *testing.T) {
			got := ChunkDocument("conn-1", "Conn One", c.Document, c.Options)
			require.Len(t, got, len(c.Chunks))
			for i, want := range c.Chunks {
				g := got[i]
				if want.TextSha256 != "" {
					sum := sha256.Sum256([]byte(g.Text))
					require.Equal(t, want.TextSha256, hex.EncodeToString(sum[:]), "chunk %d text", i)
					require.Equal(t, want.TextLength, utf16Len(g.Text), "chunk %d length", i)
					g.Text, want.Text = "", ""
				}
				require.Equal(t, want.Chunk, g, "chunk %d", i)
			}
		})
	}
}

func TestHashChunkMatchesTheTypeScriptHash(t *testing.T) {
	for _, h := range loadParity(t).Hashes {
		require.Equal(t, h.Hash, HashChunk(h.Connection, h.Source, h.Text), "%q", h.Text)
	}
}

func TestBatchInputsMatchesTheTypeScriptBatcher(t *testing.T) {
	for _, b := range loadParity(t).Batches {
		var texts []string
		for _, run := range b.Runs {
			for range run.Count {
				texts = append(texts, strings.Repeat(run.Char, run.Repeat))
			}
		}
		var sizes []int
		for _, batch := range batchInputs(texts) {
			sizes = append(sizes, len(batch))
		}
		if len(b.Sizes) == 0 {
			require.Empty(t, sizes)
			continue
		}
		require.Equal(t, b.Sizes, sizes)
	}
}
