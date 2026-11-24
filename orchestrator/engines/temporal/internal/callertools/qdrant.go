package callertools

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
)

// Embedder is satisfied by *llm.Embedder.
type Embedder interface {
	Embed(ctx context.Context, inputs []string) ([][]float32, error)
}

// QdrantStore indexes caller tools in their own collection, keyed by content
// hash.
//
// The keying is what makes this affordable. Identical definitions embed once,
// ever, across all callers and all turns — and since a given client sends a
// near-identical tool array every single turn, the steady-state embedding cost
// of the whole feature is zero. That is what makes "vectorize just in time"
// viable: the JIT cost is paid on first sight of a definition, not per request.
//
// Content-hash keying does mean a shared cache is a shared NAMESPACE. A caller
// learns nothing from it — they can only retrieve by hashes they computed from
// definitions they already hold — but two callers using the same definition do
// share one point. This is the design's least conventional decision and is
// called out as such upstream.
type QdrantStore struct {
	client     *qdrant.Client
	collection string
	embedder   Embedder
	dims       uint64
}

func NewQdrantStore(client *qdrant.Client, collection string, embedder Embedder, dims uint64) *QdrantStore {
	return &QdrantStore{client: client, collection: collection, embedder: embedder, dims: dims}
}

func (s *QdrantStore) EnsureCollection(ctx context.Context) error {
	exists, err := s.client.CollectionExists(ctx, s.collection)
	if err != nil {
		return fmt.Errorf("check collection %s: %w", s.collection, err)
	}
	if exists {
		return nil
	}
	if err := s.client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: s.collection,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     s.dims,
			Distance: qdrant.Distance_Cosine,
		}),
	}); err != nil {
		return fmt.Errorf("create collection %s: %w", s.collection, err)
	}
	return nil
}

// pointID derives a stable UUID from the content hash. Qdrant point ids must
// be UUIDs or integers; the hash itself also rides the payload. The seed is
// this module's pre-aktis path and must never change: it is part of every
// point id already on disk.
func (s *QdrantStore) pointID(hash string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("github.com/controller-agent/temporal-engine/callertools/"+hash)).String()
}

// embeddingText is what gets vectorized. Name and description only: a JSON
// Schema is structure, not meaning, and embedding it would let a large schema's
// field names dominate the similarity of a tool whose actual purpose is one
// line of prose.
func embeddingText(t Descriptor) string {
	if t.Description == "" {
		return t.Name
	}
	return t.Name + ": " + t.Description
}

func (s *QdrantStore) Index(ctx context.Context, tools []Descriptor) error {
	if len(tools) == 0 {
		return nil
	}
	now := time.Now().Unix()

	// Which are already present? Only misses need embedding — the round trip
	// that this lookup saves is the entire point of the content-hash key.
	present, err := s.existing(ctx, tools)
	if err != nil {
		return err
	}

	var misses []Descriptor
	var touch []*qdrant.PointId
	for _, tool := range tools {
		if present[tool.Hash] {
			touch = append(touch, qdrant.NewIDUUID(s.pointID(tool.Hash)))
			continue
		}
		misses = append(misses, tool)
	}

	// Refresh lastSeenAt on the hits so Prune does not reclaim a definition
	// that is still in active use.
	if len(touch) > 0 {
		wait := true
		if _, err := s.client.SetPayload(ctx, &qdrant.SetPayloadPoints{
			CollectionName: s.collection,
			Payload:        qdrant.NewValueMap(map[string]any{"lastSeenAt": now}),
			PointsSelector: qdrant.NewPointsSelector(touch...),
			Wait:           &wait,
		}); err != nil {
			// Not fatal: a missed touch only risks an early prune of a
			// definition that will simply be re-indexed on its next use.
			return fmt.Errorf("refresh lastSeenAt on %d caller tools: %w", len(touch), err)
		}
	}

	if len(misses) == 0 {
		return nil
	}

	// A definition the TypeScript store already embedded is reused, not
	// re-embedded: same hash, same embedding text, only the point id differs.
	legacy, err := s.legacyVectors(ctx, misses)
	if err != nil {
		return err
	}
	vectors := make([][]float32, len(misses))
	var toEmbed []int
	for i, tool := range misses {
		if v, ok := legacy[tool.Hash]; ok {
			vectors[i] = v
			continue
		}
		toEmbed = append(toEmbed, i)
	}
	if len(toEmbed) > 0 {
		texts := make([]string, len(toEmbed))
		for j, i := range toEmbed {
			texts[j] = embeddingText(misses[i])
		}
		embedded, err := s.embedder.Embed(ctx, texts)
		if err != nil {
			return fmt.Errorf("embed %d caller tools: %w", len(toEmbed), err)
		}
		for j, i := range toEmbed {
			vectors[i] = embedded[j]
		}
	}

	points := make([]*qdrant.PointStruct, len(misses))
	for i, tool := range misses {
		descriptor, err := json.Marshal(tool)
		if err != nil {
			return fmt.Errorf("marshal caller tool %s: %w", tool.Name, err)
		}
		points[i] = &qdrant.PointStruct{
			Id:      qdrant.NewIDUUID(s.pointID(tool.Hash)),
			Vectors: qdrant.NewVectors(vectors[i]...),
			Payload: qdrant.NewValueMap(map[string]any{
				"hash": tool.Hash,
				// Observability only (never read back): which function a
				// point is, for an operator or the e2e suite scrolling the
				// collection. The TS store wrote the same field.
				"name":       tool.Name,
				"descriptor": string(descriptor),
				"lastSeenAt": now,
			}),
		}
	}
	wait := true
	if _, err := s.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: s.collection,
		Points:         points,
		Wait:           &wait,
	}); err != nil {
		return fmt.Errorf("upsert %d caller tools: %w", len(points), err)
	}
	if len(legacy) > 0 {
		// Migrated: the TS point is now a duplicate. Best effort -- one left
		// behind is harmless and Prune ages it out on the normal TTL.
		ids := make([]*qdrant.PointId, 0, len(legacy))
		for hash := range legacy {
			ids = append(ids, qdrant.NewIDUUID(legacyPointID(hash)))
		}
		_, _ = s.client.Delete(ctx, &qdrant.DeletePoints{
			CollectionName: s.collection,
			Points:         qdrant.NewPointsSelector(ids...),
		})
	}
	return nil
}

// legacyVectors returns, by hash, the vectors the TypeScript agent-orchestrator
// (before docs/adr/0056) stored for any of tools. Its points live in the same
// collection under legacyPointID; a payload whose hash disagrees is ignored.
func (s *QdrantStore) legacyVectors(ctx context.Context, tools []Descriptor) (map[string][]float32, error) {
	ids := make([]*qdrant.PointId, len(tools))
	for i, tool := range tools {
		ids[i] = qdrant.NewIDUUID(legacyPointID(tool.Hash))
	}
	points, err := s.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: s.collection,
		Ids:            ids,
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	if err != nil {
		return nil, fmt.Errorf("look up %d pre-0056 caller tools: %w", len(ids), err)
	}
	out := map[string][]float32{}
	for _, p := range points {
		hash := p.GetPayload()["hash"].GetStringValue()
		vector := p.GetVectors().GetVector()
		data := vector.GetDense().GetData()
		if len(data) == 0 {
			data = vector.GetData() //nolint:staticcheck // older servers fill only the deprecated field
		}
		if hash != "" && legacyPointID(hash) == p.GetId().GetUuid() && uint64(len(data)) == s.dims {
			out[hash] = data
		}
	}
	return out, nil
}

// legacyNamespace is the UUIDv5 namespace the TypeScript store derived point
// ids in (vector-store/qdrant-id.ts: toQdrantPointId).
var legacyNamespace = uuid.Must(uuid.FromBytes([]byte{
	0x7b, 0x6f, 0x1e, 0x1a, 0x6e, 0x6a, 0x4f, 0x0e, 0x9b, 0x7a, 0x9f, 0x2e, 0x6b, 0x8a, 0x2f, 0x1a,
}))

// legacyPointID is the point id the TypeScript store wrote a definition under.
func legacyPointID(hash string) string {
	return uuid.NewSHA1(legacyNamespace, []byte(hash)).String()
}

func (s *QdrantStore) existing(ctx context.Context, tools []Descriptor) (map[string]bool, error) {
	ids := make([]*qdrant.PointId, len(tools))
	for i, tool := range tools {
		ids[i] = qdrant.NewIDUUID(s.pointID(tool.Hash))
	}
	points, err := s.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: s.collection,
		Ids:            ids,
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("look up %d caller tools: %w", len(ids), err)
	}
	present := make(map[string]bool, len(points))
	for _, p := range points {
		if hash := p.GetPayload()["hash"].GetStringValue(); hash != "" {
			present[hash] = true
		}
	}
	return present, nil
}

func (s *QdrantStore) Search(ctx context.Context, text string, tools []Descriptor, k int) ([]Descriptor, error) {
	if len(tools) == 0 || k <= 0 {
		return nil, nil
	}
	vectors, err := s.embedder.Embed(ctx, []string{text})
	if err != nil {
		return nil, fmt.Errorf("embed caller-tool query: %w", err)
	}

	// The filter is restricted to hashes taken from THIS request's body. That
	// is what makes cross-caller leakage structurally impossible, and it is why
	// this collection needs no RBAC payload filter — retrieval can never range
	// over definitions the request did not itself supply.
	// A hash can be held by two points while pre-0056 TypeScript points exist
	// (this store's pointID and the TS store's legacyPointID): adoption deletes
	// the legacy point best effort, and a TS pod still running mid-rollout can
	// write it again. Fetch twice k so that, after de-duplicating by hash
	// below, a duplicate never costs a distinct tool its top-k slot.
	limit := uint64(2 * k)
	points, err := s.client.Query(ctx, &qdrant.QueryPoints{
		CollectionName: s.collection,
		Query:          qdrant.NewQuery(vectors[0]...),
		Filter:         &qdrant.Filter{Must: []*qdrant.Condition{qdrant.NewMatchKeywords("hash", Hashes(tools)...)}},
		Limit:          &limit,
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("query caller tools: %w", err)
	}

	// Resolve back to the REQUEST's own descriptors rather than trusting the
	// stored payload. Belt and braces on the filter above: a point that somehow
	// matched without being in this request cannot make it into the result.
	hits := make([]string, len(points))
	for i, p := range points {
		hits[i] = p.GetPayload()["hash"].GetStringValue()
	}
	return resolveHits(hits, tools, k), nil
}

// resolveHits maps ranked hit hashes back to the request's own descriptors,
// dropping any hash not in the request and every repeat of a hash already
// returned (the first, highest-scoring hit wins), and keeps at most k. A
// repeated tool would offer the model two tools with the same name, which LLM
// APIs reject.
func resolveHits(hits []string, tools []Descriptor, k int) []Descriptor {
	byHash := make(map[string]Descriptor, len(tools))
	for _, tool := range tools {
		byHash[tool.Hash] = tool
	}
	seen := make(map[string]bool, len(hits))
	out := make([]Descriptor, 0, min(len(hits), k))
	for _, hash := range hits {
		if len(out) == k {
			break
		}
		tool, ok := byHash[hash]
		if !ok || seen[hash] {
			continue
		}
		seen[hash] = true
		out = append(out, tool)
	}
	return out
}

// Prune reclaims abandoned definitions. Qdrant has no native TTL, so without
// this the collection grows forever — every definition any caller ever sent,
// including one-off experiments and every intermediate edit of a schema.
func (s *QdrantStore) Prune(ctx context.Context, olderThanSeconds int64) (int, error) {
	now := time.Now()
	cutoff := float64(now.Unix() - olderThanSeconds)
	// Points written by the TypeScript agent-orchestrator (before docs/adr/
	// 0056) carry lastSeenAt in MILLISECONDS and a different point id, so this
	// store never touches them again. A seconds-only range would never reach
	// them either; this second range ages them out on the same TTL. Any value
	// at or above legacyMillisFloor is a millisecond timestamp (in seconds it
	// would be the year ~5000).
	legacyFloor := float64(legacyMillisFloor)
	legacyCutoff := float64(now.UnixMilli() - olderThanSeconds*1000)
	stale := &qdrant.Filter{
		Should: []*qdrant.Condition{
			qdrant.NewRange("lastSeenAt", &qdrant.Range{Lt: &cutoff}),
			qdrant.NewRange("lastSeenAt", &qdrant.Range{Gte: &legacyFloor, Lt: &legacyCutoff}),
		},
	}

	// Counted before deleting: Delete reports an operation status, not how many
	// points it removed, and a prune that silently reclaims nothing looks
	// identical to one that reclaimed everything.
	count, err := s.client.Count(ctx, &qdrant.CountPoints{
		CollectionName: s.collection,
		Filter:         stale,
	})
	if err != nil {
		return 0, fmt.Errorf("count stale caller tools: %w", err)
	}
	if count == 0 {
		return 0, nil
	}

	wait := true
	if _, err := s.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: s.collection,
		Points:         qdrant.NewPointsSelectorFilter(stale),
		Wait:           &wait,
	}); err != nil {
		return 0, fmt.Errorf("prune caller tools: %w", err)
	}
	return int(count), nil
}

// legacyMillisFloor separates millisecond lastSeenAt values (the TS store's)
// from second ones: 10^11 seconds is ~year 5138, 10^11 ms is 1973.
const legacyMillisFloor = int64(100_000_000_000)
