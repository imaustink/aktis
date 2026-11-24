package callertools

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

// liveQdrantEnv names a REAL Qdrant's gRPC address (host:port). Unit tests
// cannot show that Qdrant accepts this store's filters — a hash keyword
// match, a payload-only SetPayload, a delete-by-range prune — only that the
// code meant to send them. e2e/specs/caller-tools.e2e.ts port-forwards the
// cluster's Qdrant and runs TestLiveQdrant* with this set; without it they
// skip.
const liveQdrantEnv = "CALLERTOOLS_QDRANT_ADDR"

type countingEmbedder struct{ calls atomic.Int64 }

// Embed is deterministic and offline: a character histogram, enough for
// search to rank and for "re-embeds nothing" to be observable.
func (e *countingEmbedder) Embed(_ context.Context, inputs []string) ([][]float32, error) {
	out := make([][]float32, len(inputs))
	for i, text := range inputs {
		e.calls.Add(1)
		v := make([]float32, 64)
		for j, r := range text {
			v[(int(r)*31+j)%64]++
		}
		out[i] = v
	}
	return out, nil
}

func liveStore(t *testing.T) (*QdrantStore, *qdrant.Client, *countingEmbedder) {
	t.Helper()
	addr := os.Getenv(liveQdrantEnv)
	if addr == "" {
		t.Skipf("%s not set: no live Qdrant", liveQdrantEnv)
	}
	host, rawPort, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	port, err := strconv.Atoi(rawPort)
	require.NoError(t, err)
	client, err := qdrant.NewClient(&qdrant.Config{Host: host, Port: port})
	require.NoError(t, err)
	collection := fmt.Sprintf("e2e_caller_tools_live_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_ = client.DeleteCollection(context.Background(), collection)
		_ = client.Close()
	})
	embedder := &countingEmbedder{}
	store := NewQdrantStore(client, collection, embedder, 64)
	require.NoError(t, store.EnsureCollection(context.Background()))
	return store, client, embedder
}

func mustTool(t *testing.T, name, description string) Descriptor {
	t.Helper()
	d, err := New(name, description, []byte(`{"type":"object"}`))
	require.NoError(t, err)
	return d
}

func storedNames(t *testing.T, client *qdrant.Client, collection string) []string {
	t.Helper()
	limit := uint32(200)
	points, err := client.Scroll(context.Background(), &qdrant.ScrollPoints{
		CollectionName: collection, Limit: &limit, WithPayload: qdrant.NewWithPayload(true),
	})
	require.NoError(t, err)
	var names []string
	for _, p := range points {
		names = append(names, p.GetPayload()["name"].GetStringValue())
	}
	sort.Strings(names)
	return names
}

func names(tools []Descriptor) []string {
	out := make([]string, len(tools))
	for i, tool := range tools {
		out[i] = tool.Name
	}
	sort.Strings(out)
	return out
}

func TestLiveQdrantIndexSearchAndCache(t *testing.T) {
	store, client, embedder := liveStore(t)
	ctx := context.Background()
	alpha := mustTool(t, "alpha_search", "Search the alpha corpus")
	beta := mustTool(t, "beta_lookup", "Look up a beta record")
	gamma := mustTool(t, "gamma_report", "Generate a gamma report")

	ok, err := client.CollectionExists(ctx, store.collection)
	require.NoError(t, err)
	require.True(t, ok, "EnsureCollection created the collection")

	require.NoError(t, store.Index(ctx, []Descriptor{alpha, beta}))
	require.Equal(t, []string{"alpha_search", "beta_lookup"}, storedNames(t, client, store.collection))

	// The content-hash cache against a real store: re-indexing adds no
	// points and embeds nothing, which is what makes just-in-time indexing
	// free in the steady state.
	before := embedder.calls.Load()
	require.NoError(t, store.Index(ctx, []Descriptor{alpha, beta}))
	require.Equal(t, []string{"alpha_search", "beta_lookup"}, storedNames(t, client, store.collection))
	require.Equal(t, before, embedder.calls.Load(), "a cache hit re-embedded")

	// The hash filter IS the isolation boundary: gamma is indexed but not
	// supplied, so it must never come back.
	require.NoError(t, store.Index(ctx, []Descriptor{gamma}))
	found, err := store.Search(ctx, "look up a beta record", []Descriptor{alpha, beta}, 5)
	require.NoError(t, err)
	require.Equal(t, []string{"alpha_search", "beta_lookup"}, names(found))

	one, err := store.Search(ctx, "anything at all", []Descriptor{alpha, beta, gamma}, 1)
	require.NoError(t, err)
	require.Len(t, one, 1)
}

// The TypeScript store's toQdrantPointId("a"*64), computed by that function.
func TestLegacyPointIDMatchesTheTypeScriptStore(t *testing.T) {
	require.Equal(t, "d554f00d-7ae5-544b-9061-f5b186ebffac", legacyPointID(strings.Repeat("a", 64)))
}

func TestLiveQdrantReusesAPre0055VectorInsteadOfEmbedding(t *testing.T) {
	store, client, embedder := liveStore(t)
	ctx := context.Background()
	migrated := mustTool(t, "migrated_tool", "Embedded by the TypeScript store")
	brandNew := mustTool(t, "brand_new_tool", "Never seen before")

	// The point the TS store left: its id derivation, ms lastSeenAt, a vector
	// the fake embedder could never produce (so reuse is observable).
	tsVector := make([]float32, 64)
	tsVector[7] = 42
	wait := true
	_, err := client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: store.collection,
		Wait:           &wait,
		Points: []*qdrant.PointStruct{{
			Id:      qdrant.NewIDUUID(legacyPointID(migrated.Hash)),
			Vectors: qdrant.NewVectors(tsVector...),
			Payload: qdrant.NewValueMap(map[string]any{"hash": migrated.Hash, "name": migrated.Name, "lastSeenAt": time.Now().UnixMilli()}),
		}},
	})
	require.NoError(t, err)

	require.NoError(t, store.Index(ctx, []Descriptor{migrated, brandNew}))
	require.Equal(t, int64(1), embedder.calls.Load(), "only the definition the TS store never saw is embedded")
	require.Equal(t, []string{"brand_new_tool", "migrated_tool"}, storedNames(t, client, store.collection), "the TS point is replaced, not duplicated")

	points, err := client.Get(ctx, &qdrant.GetPoints{
		CollectionName: store.collection,
		Ids:            []*qdrant.PointId{qdrant.NewIDUUID(store.pointID(migrated.Hash))},
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, 1)
	got := points[0].GetVectors().GetVector()
	data := got.GetDense().GetData()
	if len(data) == 0 {
		data = got.GetData() //nolint:staticcheck
	}
	// Cosine collections normalize on write, so compare direction, not values.
	require.InDelta(t, 1.0, data[7], 1e-6, "the TS vector was carried over")
}

func TestLiveQdrantPruneDropsOnlyStaleDefinitionsIncludingLegacyMillis(t *testing.T) {
	store, client, _ := liveStore(t)
	ctx := context.Background()
	old := mustTool(t, "old_tool", "Indexed before the cutoff")
	require.NoError(t, store.Index(ctx, []Descriptor{old}))

	// A point the TypeScript store wrote: its own id and lastSeenAt in ms.
	stale := time.Now().Add(-time.Hour).UnixMilli()
	fresh := time.Now().Add(time.Hour).UnixMilli() // still inside any TTL after the sleep below
	wait := true
	_, err := client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: store.collection,
		Wait:           &wait,
		Points: []*qdrant.PointStruct{
			{Id: qdrant.NewIDUUID(uuid.NewString()), Vectors: qdrant.NewVectors(make([]float32, 64)...),
				Payload: qdrant.NewValueMap(map[string]any{"hash": "legacy-stale", "name": "legacy_stale", "lastSeenAt": stale})},
			{Id: qdrant.NewIDUUID(uuid.NewString()), Vectors: qdrant.NewVectors(make([]float32, 64)...),
				Payload: qdrant.NewValueMap(map[string]any{"hash": "legacy-fresh", "name": "legacy_fresh", "lastSeenAt": fresh})},
		},
	})
	require.NoError(t, err)

	// Second-resolution timestamps: let the cutoff pass old_tool, then index
	// a new one after it.
	time.Sleep(2100 * time.Millisecond)
	recent := mustTool(t, "recent_tool", "Indexed after the cutoff")
	require.NoError(t, store.Index(ctx, []Descriptor{recent}))

	removed, err := store.Prune(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 2, removed, "old_tool and the stale legacy point")
	require.Equal(t, []string{"legacy_fresh", "recent_tool"}, storedNames(t, client, store.collection))
}
