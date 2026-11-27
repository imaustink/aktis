package vectorstore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"

	"github.com/aktisai/orchestrator/engines/temporal/internal/access"
)

// Qdrant implements Store over one Qdrant collection. Nothing outside this
// file touches the Qdrant client (Aktis ADR 0003's port rule).
type Qdrant struct {
	client     *qdrant.Client
	collection string
	embedder   Embedder
	dims       uint64
}

func NewQdrant(client *qdrant.Client, collection string, embedder Embedder, dims uint64) *Qdrant {
	return &Qdrant{client: client, collection: collection, embedder: embedder, dims: dims}
}

// EnsureCollection creates the collection if it doesn't exist (cosine
// distance, matching the embedding model's semantics).
func (s *Qdrant) EnsureCollection(ctx context.Context) error {
	exists, err := s.client.CollectionExists(ctx, s.collection)
	if err != nil {
		return fmt.Errorf("check collection %s: %w", s.collection, err)
	}
	if exists {
		return nil
	}
	err = s.client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: s.collection,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     s.dims,
			Distance: qdrant.Distance_Cosine,
		}),
	})
	if err != nil {
		return fmt.Errorf("create collection %s: %w", s.collection, err)
	}
	return nil
}

// Exists reports whether the collection exists. A Corpus collection that does
// not exist yet has nothing indexed, which is a true answer, not an error.
func (s *Qdrant) Exists(ctx context.Context) (bool, error) {
	exists, err := s.client.CollectionExists(ctx, s.collection)
	if err != nil {
		return false, fmt.Errorf("check collection %s: %w", s.collection, err)
	}
	return exists, nil
}

// PointRef is what a scroll reads back from one point: the record id and its
// descriptor JSON, both from the payload. ID is empty when the payload has
// none.
type PointRef struct {
	ID         string
	Descriptor string
}

// Scroll walks every point in the collection, `pageSize` at a time, reading
// only the `id` and `descriptor` payload fields (vectors are the bulk of a
// point and are not wanted). `page` is called once per page read, including
// an empty last one.
func (s *Qdrant) Scroll(ctx context.Context, pageSize uint32, page func([]PointRef) error) error {
	var offset *qdrant.PointId
	for {
		points, next, err := s.client.ScrollAndOffset(ctx, &qdrant.ScrollPoints{
			CollectionName: s.collection,
			Limit:          &pageSize,
			Offset:         offset,
			WithPayload:    qdrant.NewWithPayloadInclude("id", "descriptor"),
			WithVectors:    qdrant.NewWithVectors(false),
		})
		if err != nil {
			return fmt.Errorf("scroll %s: %w", s.collection, err)
		}
		refs := make([]PointRef, 0, len(points))
		for _, p := range points {
			payload := p.GetPayload()
			refs = append(refs, PointRef{
				ID:         payload["id"].GetStringValue(),
				Descriptor: payload["descriptor"].GetStringValue(),
			})
		}
		if err := page(refs); err != nil {
			return err
		}
		if next == nil {
			return nil
		}
		offset = next
	}
}

// pointID derives a stable UUID for a record id (Qdrant point ids must be
// UUIDs or integers; the real id lives in the payload). The seed is this
// module's pre-aktis path and must never change: it is part of every point
// id already on disk, and the connection broker derives the same ids
// (point-id.ts).
func (s *Qdrant) pointID(id string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("github.com/controller-agent/temporal-engine/"+s.collection+"/"+id)).String()
}

func (s *Qdrant) Upsert(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	texts := make([]string, len(records))
	for i, r := range records {
		texts[i] = r.Text
	}
	vectors, err := s.embedder.Embed(ctx, texts)
	if err != nil {
		return fmt.Errorf("embed %d records: %w", len(records), err)
	}

	points := make([]*qdrant.PointStruct, len(records))
	for i, r := range records {
		points[i] = &qdrant.PointStruct{
			Id:      qdrant.NewIDUUID(s.pointID(r.ID)),
			Vectors: qdrant.NewVectors(vectors[i]...),
			Payload: qdrant.NewValueMap(map[string]any{
				"id":           r.ID,
				"roles":        anyList(r.Roles),
				"unrestricted": r.Unrestricted,
				"private":      r.Private,
				"principals":   anyList(r.Principals),
				"hidden":       r.Hidden,
				"descriptor":   string(r.Descriptor),
			}),
		}
	}

	wait := true
	_, err = s.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: s.collection,
		Points:         points,
		Wait:           &wait,
	})
	if err != nil {
		return fmt.Errorf("upsert %d points into %s: %w", len(points), s.collection, err)
	}
	return nil
}

func (s *Qdrant) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	pointIDs := make([]*qdrant.PointId, len(ids))
	for i, id := range ids {
		pointIDs[i] = qdrant.NewIDUUID(s.pointID(id))
	}
	wait := true
	_, err := s.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: s.collection,
		Points:         qdrant.NewPointsSelector(pointIDs...),
		Wait:           &wait,
	})
	if err != nil {
		return fmt.Errorf("delete %d points from %s: %w", len(ids), s.collection, err)
	}
	return nil
}

func anyList(xs []string) []any {
	out := make([]any, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}

// visibilityFilter is access.Viewer.Admits as a Qdrant filter: the record's
// roles intersect the viewer's (or it is unrestricted), AND it is either
// public or names one of the viewer's principals (ADR 0060). With no viewer
// roles only unrestricted records pass the first clause, and with no viewer
// principals only public records pass the second — both fail closed.
//
// Records written before `private` existed carry no such field. A must-not
// match against an absent field passes, so they read as public, which is what
// they were: nothing could make a record private before this field.
func visibilityFilter(viewer access.Viewer) *qdrant.Filter {
	roles := []*qdrant.Condition{qdrant.NewMatchBool("unrestricted", true)}
	if len(viewer.Roles) > 0 {
		roles = append(roles, qdrant.NewMatchKeywords("roles", viewer.Roles...))
	}
	principals := []*qdrant.Condition{
		qdrant.NewFilterAsCondition(&qdrant.Filter{
			MustNot: []*qdrant.Condition{qdrant.NewMatchBool("private", true)},
		}),
	}
	if len(viewer.Principals) > 0 {
		principals = append(principals, qdrant.NewMatchKeywords("principals", viewer.Principals...))
	}
	return &qdrant.Filter{Must: []*qdrant.Condition{
		qdrant.NewFilterAsCondition(&qdrant.Filter{Should: roles}),
		qdrant.NewFilterAsCondition(&qdrant.Filter{Should: principals}),
	}}
}

// queryFilter is visibilityFilter plus the hidden exclusion, applied to
// semantic search only — GetByIDs deliberately keeps using visibilityFilter, so
// a hidden record stays reachable by the thing that declared it.
//
// Records written before `hidden` existed carry no such field, and a must-not
// match against an absent field passes, so this is backward compatible with an
// already-populated collection.
func queryFilter(viewer access.Viewer) *qdrant.Filter {
	filter := visibilityFilter(viewer)
	filter.MustNot = []*qdrant.Condition{qdrant.NewMatchBool("hidden", true)}
	return filter
}

func (s *Qdrant) Query(ctx context.Context, text string, viewer access.Viewer, limit int) ([]Hit, error) {
	vectors, err := s.embedder.Embed(ctx, []string{text})
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}

	limit64 := uint64(limit)
	points, err := s.client.Query(ctx, &qdrant.QueryPoints{
		CollectionName: s.collection,
		Query:          qdrant.NewQuery(vectors[0]...),
		Filter:         queryFilter(viewer),
		Limit:          &limit64,
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", s.collection, err)
	}

	hits := make([]Hit, 0, len(points))
	for _, p := range points {
		hit, err := hitFromPayload(p.GetPayload())
		if err != nil {
			return nil, err
		}
		hit.Score = p.GetScore()
		hits = append(hits, hit)
	}
	return hits, nil
}

func (s *Qdrant) GetByIDs(ctx context.Context, ids []string, viewer access.Viewer) ([]Hit, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	pointIDs := make([]*qdrant.PointId, len(ids))
	for i, id := range ids {
		pointIDs[i] = qdrant.NewIDUUID(s.pointID(id))
	}
	points, err := s.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: s.collection,
		Ids:            pointIDs,
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("get %d points from %s: %w", len(ids), s.collection, err)
	}

	// Visibility re-check in code: direct lookups bypass the query filter, so
	// this is the defense-in-depth backstop (ADR 0008).
	hits := make([]Hit, 0, len(points))
	for _, p := range points {
		payload := p.GetPayload()
		if !viewer.Admits(audienceOf(payload)) {
			continue
		}
		hit, err := hitFromPayload(payload)
		if err != nil {
			return nil, err
		}
		hits = append(hits, hit)
	}
	return hits, nil
}

// GetByIDsUnfiltered resolves records by id with no role check. See the Store
// interface for why this exists and the one question it may answer.
func (s *Qdrant) GetByIDsUnfiltered(ctx context.Context, ids []string) ([]Hit, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	pointIDs := make([]*qdrant.PointId, len(ids))
	for i, id := range ids {
		pointIDs[i] = qdrant.NewIDUUID(s.pointID(id))
	}
	points, err := s.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: s.collection,
		Ids:            pointIDs,
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("get %d points from %s: %w", len(ids), s.collection, err)
	}
	hits := make([]Hit, 0, len(points))
	for _, p := range points {
		hit, err := hitFromPayload(p.GetPayload())
		if err != nil {
			return nil, err
		}
		hits = append(hits, hit)
	}
	return hits, nil
}

// audienceOf reads a point's access fields back into an access.Audience, so
// the by-id re-check is the very rule the query filter encodes.
func audienceOf(payload map[string]*qdrant.Value) access.Audience {
	return access.Audience{
		Roles:        stringList(payload["roles"]),
		Unrestricted: payload["unrestricted"].GetBoolValue(),
		Private:      payload["private"].GetBoolValue(),
		Principals:   stringList(payload["principals"]),
	}
}

func stringList(v *qdrant.Value) []string {
	values := v.GetListValue().GetValues()
	out := make([]string, 0, len(values))
	for _, item := range values {
		out = append(out, item.GetStringValue())
	}
	return out
}

func hitFromPayload(payload map[string]*qdrant.Value) (Hit, error) {
	id := payload["id"].GetStringValue()
	descriptor := payload["descriptor"].GetStringValue()
	if id == "" || descriptor == "" {
		return Hit{}, fmt.Errorf("point payload missing id/descriptor")
	}
	return Hit{ID: id, Descriptor: json.RawMessage(descriptor)}, nil
}
