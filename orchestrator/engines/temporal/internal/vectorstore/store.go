// Package vectorstore is the retrieval port over the catalog collections
// (tools / skills / agents), with RBAC baked into every read: queries are
// role-filtered at the store (an unauthorized record is never a candidate),
// and lookups by id re-check roles as defense in depth — mirroring
// Aktis ADRs 0003/0004/0008.
package vectorstore

import (
	"context"
	"encoding/json"

	"github.com/aktisai/orchestrator/engines/temporal/internal/access"
)

// Embedder is satisfied by *llm.Embedder.
type Embedder interface {
	Embed(ctx context.Context, inputs []string) ([][]float32, error)
}

// Record is one indexed catalog entry.
type Record struct {
	ID   string
	Text string // what gets embedded

	// Roles gates retrieval (match-any against the caller's roles).
	// Unrestricted marks records visible to any resolved identity (derived
	// tool-less skills); Roles is ignored when set.
	Roles        []string
	Unrestricted bool

	// Private scopes the record to Principals on top of Roles (Aktis
	// ADR 0060): a viewer must ALSO hold one of them. A separate flag because
	// a private record with no principals left is a real state (a skill
	// whose own list and a private tool's do not intersect) and must mean
	// "no one", not "public".
	Private    bool
	Principals []string

	// Hidden keeps a record REFERENCEABLE but not RETRIEVABLE: GetByIDs finds
	// it, semantic Query never returns it.
	//
	// Needed by records that exist only to be named by something else. A
	// knowledge base's generated search tool, and a Connection's scoped GET
	// tool, are reachable precisely because the knowledge base that owns them
	// was selected (Aktis ADR 0039 §2). Letting them compete in open
	// retrieval would put every client's scoped tooling in front of every
	// caller, which is the outcome that design exists to prevent.
	Hidden bool

	// Descriptor is the full descriptor JSON, returned verbatim on hits.
	Descriptor json.RawMessage
}

type Hit struct {
	ID         string
	Score      float32
	Descriptor json.RawMessage
}

type Store interface {
	Upsert(ctx context.Context, records []Record) error
	Delete(ctx context.Context, ids []string) error

	// Query returns the top-k records visible to viewer for the request text.
	// No viewer roles matches only Unrestricted records, and no viewer
	// principals matches no Private record (both fail closed).
	Query(ctx context.Context, text string, viewer access.Viewer, limit int) ([]Hit, error)

	// GetByIDs resolves records directly (no ranking), re-applying the same
	// visibility check. Missing ids are silently absent from the result.
	GetByIDs(ctx context.Context, ids []string, viewer access.Viewer) ([]Hit, error)

	// GetByIDsUnfiltered resolves records by id with NO role check.
	//
	// The one deliberate exception to this package's RBAC discipline, and it
	// answers a different question. Every other read here asks "which records
	// may this CALLER reach?", because the system is deciding on that caller's
	// behalf. An Agent's own `toolRefs` (upstream ADR 0028) asks "which tools
	// did the OPERATOR declare this agent may call?" — a property of deployed
	// configuration, independent of whoever's turn happened to launch the
	// agent, and the same question the upstream reconciler's own validation
	// asks.
	//
	// Routing that through the role-filtered read would need a synthetic
	// caller-roles filter that either coincidentally works or requires
	// threading the launching caller's roles across the whole life of a
	// possibly long-running agent, for no benefit. Callers MUST NOT use this
	// to answer a caller-scoped question.
	GetByIDsUnfiltered(ctx context.Context, ids []string) ([]Hit, error)
}
